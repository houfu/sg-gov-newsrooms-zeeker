"""
Accounting and Corporate Regulatory Authority (ACRA) Singapore newsroom resource.

Cadence: Daily (Tier 1)
Source: https://www.acra.gov.sg/news-events/news-announcements/
Strategy: Incremental — sitemap.xml discovery, filtered to /news-events/news-announcements/* URLs
  not already in DB. lastmod dates in the sitemap may not reflect publication dates, so we scrape
  each article page for its date and filter by published_date >= START_DATE post-scrape.

Licensing: © Government of Singapore, all rights reserved.
Content stored but NOT shown by default in Datasette UI. See zeeker.toml for column config.
"""

import asyncio
import hashlib
import os
import random
import re
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree

import click
import httpx
from bs4 import BeautifulSoup
from openai import AsyncOpenAI
from sqlite_utils.db import Table
from tenacity import retry, stop_after_attempt, wait_exponential

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://www.acra.gov.sg"
SITEMAP_URL = "https://www.acra.gov.sg/sitemap.xml"
NEWS_PREFIX = "/news-events/news-announcements/"

# Only keep articles published from this date onwards
START_DATE = date(2026, 1, 1)

# Scraping rate limits (Tier 1 — daily incremental, be polite)
REQUEST_DELAY_BASE = 1.5
REQUEST_DELAY_JITTER = 0.5
REQUEST_TIMEOUT = 30.0
MAX_CONSECUTIVE_FAILURES = 5
MAX_RETRIES = 3

# LLM concurrency
_LLM_SEMAPHORE = asyncio.Semaphore(3)

# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SUMMARY_SYSTEM_PROMPT = """
As an expert in Singapore corporate regulation and business compliance, provide
concise summaries of ACRA news announcements, press releases, and speeches for
business owners, compliance professionals, and corporate service providers.
Highlight key regulatory changes, enforcement actions, filing requirements,
corporate governance updates, or industry developments. Write 1 narrative paragraph,
no longer than 100 words. Focus on what changed, who is affected, and why it matters
for businesses and corporate entities in Singapore.
"""

# =============================================================================
# HELPERS
# =============================================================================


def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def polite_sleep():
    delay = REQUEST_DELAY_BASE + random.uniform(-REQUEST_DELAY_JITTER, REQUEST_DELAY_JITTER)
    time.sleep(max(0.5, delay))


def infer_category(url: str, title: str) -> str:
    """
    Infer article category from URL slug and title.

    ACRA publishes: announcements, press releases, speeches, and ACRAConnect newsletters.
    """
    slug = url.lower()
    title_lower = title.lower()

    if "press-release" in slug or "press release" in title_lower:
        return "press-release"
    if any(x in slug or x in title_lower for x in ["speech", "address", "remarks"]):
        return "speech"
    if "newsletter" in slug or "acraconnect" in title_lower or "newsletter" in title_lower:
        return "newsletter"
    if "announcement" in slug or "announcement" in title_lower:
        return "announcement"
    return "other"


def parse_date_string(date_str: str) -> Optional[str]:
    """Parse 'DD Month YYYY' or 'DD Mon YYYY' to ISO date string."""
    date_str = date_str.strip()
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(20\d{2})", date_str)
    if m:
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt).date().isoformat()
            except ValueError:
                continue
    return None


# =============================================================================
# SITEMAP DISCOVERY
# =============================================================================


def discover_urls_from_sitemap(client: httpx.Client, existing_urls: set) -> List[str]:
    """
    Fetch sitemap.xml and return news announcement URLs not already in the database.

    ACRA's sitemap contains ~480 news-announcement URLs. We filter to the
    /news-events/news-announcements/ prefix, excluding the index page itself
    and any URL already in the DB.
    """
    click.echo(f"Fetching sitemap: {SITEMAP_URL}")
    try:
        response = client.get(SITEMAP_URL)
        response.raise_for_status()
    except httpx.HTTPError as e:
        click.echo(f"Failed to fetch sitemap: {e}", err=True)
        return []

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    try:
        root = ElementTree.fromstring(response.content)
    except ElementTree.ParseError as e:
        click.echo(f"Failed to parse sitemap: {e}", err=True)
        return []

    all_urls = [
        url_el.findtext("sm:loc", namespaces=ns) or ""
        for url_el in root.findall("sm:url", ns)
    ]

    news_urls = [
        url for url in all_urls
        if NEWS_PREFIX in url
        and url.rstrip("/") != f"{BASE_URL}{NEWS_PREFIX}".rstrip("/")
        and url not in existing_urls
    ]

    click.echo(
        f"Sitemap: {len(all_urls)} total URLs, "
        f"{len(news_urls)} new news announcement URLs to scrape."
    )
    return news_urls


# =============================================================================
# ARTICLE SCRAPING
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article(url: str, client: httpx.Client) -> Dict[str, Any]:
    """
    Fetch and extract content from an acra.gov.sg news announcement page.

    The site is built on Isomer (Next.js). Key extraction notes:
    - Title: <h1> element
    - Date: footer text pattern "last updated DD Month YYYY"; also searched in full page text
    - Content: <main> element, stripping nav/footer/aside noise
    """
    response = client.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "lxml")

    # Title
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True).split("|")[0].strip()

    # Date — look for "last updated DD Month YYYY" pattern first (Isomer footer)
    published_date = None
    page_text = soup.get_text(" ", strip=True)

    last_updated = re.search(
        r"last updated\s+(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|"
        r"Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2})",
        page_text,
        re.I,
    )
    if last_updated:
        published_date = parse_date_string(last_updated.group(1))

    # Fallback: any date pattern in the page
    if not published_date:
        m = re.search(
            r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|"
            r"Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2})\b",
            page_text,
        )
        if m:
            published_date = parse_date_string(m.group(1))

    # Content — extract from <main>
    content_text = ""
    main = soup.find("main")
    container = main or soup.find("body")
    if container:
        for unwanted in container.find_all(
            ["nav", "header", "footer", "script", "style", "aside", "form"]
        ):
            unwanted.decompose()
        parts = [
            el.get_text(strip=True)
            for el in container.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"])
            if len(el.get_text(strip=True)) > 15
        ]
        content_text = "\n\n".join(parts)

    return {
        "title": title,
        "published_date": published_date,
        "content_text": content_text,
    }


# =============================================================================
# AI SUMMARY
# =============================================================================


async def get_summary(text: str, title: str) -> str:
    base_url = os.environ.get("LLM_BASE_URL", "")
    api_key = os.environ.get("LLM_API_KEY", "")
    model = os.environ.get("LLM_MODEL", "")

    if not base_url:
        click.echo("  LLM_BASE_URL not set — skipping summary", err=True)
        return ""

    client = AsyncOpenAI(
        base_url=base_url,
        api_key=api_key or "not-needed",
        max_retries=2,
        timeout=120.0,
    )
    content_snippet = text[:4000] if text else title

    async with _LLM_SEMAPHORE:
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Summarise this ACRA item:\n\n{content_snippet}"},
                ],
            )
            return (response.choices[0].message.content or "").strip()
        except Exception as e:
            click.echo(f"  Summary failed: {e}", err=True)
            return ""


async def generate_summaries(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tasks = [get_summary(item.get("content_text", ""), item.get("title", "")) for item in items]
    summaries = await asyncio.gather(*tasks, return_exceptions=True)
    for item, summary in zip(items, summaries):
        item["summary"] = "" if isinstance(summary, Exception) else summary
    return items


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================


def fetch_data(existing_table: Optional[Table]) -> List[Dict[str, Any]]:
    """
    Fetch new ACRA news announcement articles via sitemap discovery.

    Incremental: skips URLs already in DB and articles before START_DATE.
    """
    existing_urls: set = set()
    if existing_table:
        existing_urls = {row["source_url"] for row in existing_table.rows}
        click.echo(f"Existing records: {len(existing_urls)}")

    results: List[Dict[str, Any]] = []
    consecutive_failures = 0

    with httpx.Client(
        timeout=REQUEST_TIMEOUT,
        follow_redirects=True,
        headers={
            "User-Agent": "ZeekerBot/1.0 (+https://data.zeeker.sg; sg-gov-newsrooms research bot)"
        },
        limits=httpx.Limits(max_connections=5, max_keepalive_connections=3),
    ) as client:
        new_urls = discover_urls_from_sitemap(client, existing_urls)

        if not new_urls:
            click.echo("No new URLs to process.")
            return []

        click.echo(f"\nScraping {len(new_urls)} articles...")
        for i, url in enumerate(new_urls, 1):
            click.echo(f"[{i}/{len(new_urls)}] {url}")
            polite_sleep()

            try:
                article = fetch_article(url, client)
                consecutive_failures = 0
            except Exception as e:
                click.echo(f"  Failed: {e}", err=True)
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    click.echo("Circuit breaker triggered — too many consecutive failures.", err=True)
                    break
                continue

            # Filter by date
            pub_date = article.get("published_date")
            if pub_date:
                try:
                    if date.fromisoformat(pub_date) < START_DATE:
                        click.echo(f"  Skipping (before {START_DATE}): {pub_date}")
                        continue
                except ValueError:
                    pass
            elif pub_date is None:
                click.echo("  Warning: no date found, including anyway")

            title = article.get("title", "")
            category = infer_category(url, title)

            result = {
                "id": make_id(url),
                "source_url": url,
                "category": category,
                "title": title,
                "published_date": pub_date,
                "content_text": article.get("content_text", ""),
                "summary": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            results.append(result)
            click.echo(
                f"  → {title[:60]} "
                f"({pub_date}, {category}, {len(article.get('content_text',''))} chars)"
            )

    if not results:
        click.echo("No articles scraped.")
        return []

    click.echo(f"\nGenerating summaries for {len(results)} articles...")
    results = asyncio.run(generate_summaries(results))

    summaries_ok = sum(1 for r in results if r.get("summary"))
    click.echo(f"\nDone: {len(results)} new articles, {summaries_ok} with summaries.")
    return results


def transform_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return raw_data
