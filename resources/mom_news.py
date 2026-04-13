"""
Ministry of Manpower Singapore press releases resource.

Cadence: Daily (Tier 1)
Source: https://www.mom.gov.sg/newsroom/press-releases
Strategy: Incremental — sitemap discovery via newsroom.xml, filtered to
  /newsroom/press-releases/ URLs from 2026 onwards. The URL path contains
  the year (/press-releases/2026/), so we pre-filter before scraping.

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

BASE_URL = "https://www.mom.gov.sg"
SITEMAP_URL = "https://www.mom.gov.sg/newsroom.xml"
PRESS_RELEASES_PREFIX = "/newsroom/press-releases/"

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
As an expert in Singapore employment law and workforce policy, provide concise summaries of
Ministry of Manpower press releases for employers, workers, HR professionals, and policy
researchers. Highlight key enforcement actions, workplace safety incidents, employment
statistics, foreign workforce updates, labour policy changes, or regulatory developments.
Write 1 narrative paragraph, no longer than 100 words. Focus on what happened, who is
affected, and why it matters for workplaces in Singapore.
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
    Infer press release category from URL slug and title.

    MOM publishes across: workplace safety, employment practices, foreign workforce,
    statistics, and general announcements.
    """
    slug = url.lower()
    title_lower = title.lower()

    if any(x in slug or x in title_lower for x in [
        "workplace-safety", "wsh", "safety", "fatal", "accident", "injured",
        "arrested", "fined", "jailed", "prosecuted",
    ]):
        return "workplace-safety"
    if any(x in slug or x in title_lower for x in [
        "foreign-worker", "foreign-workforce", "work-permit", "work-pass",
        "migrant", "dormitor",
    ]):
        return "foreign-workforce"
    if any(x in slug or x in title_lower for x in [
        "statistic", "survey", "report", "labour-market", "employment-rate",
    ]):
        return "statistics"
    if any(x in slug or x in title_lower for x in [
        "wage", "salary", "cpf", "retirement", "progressive-wage",
    ]):
        return "employment-standards"
    return "general"


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


def extract_date_from_url(url: str) -> Optional[str]:
    """
    Extract date from MOM press release URL pattern.

    URL pattern: /newsroom/press-releases/YYYY/MMDD-slug
    e.g., /newsroom/press-releases/2026/0401-10-arrested → 2026-04-01
    """
    m = re.search(r"/press-releases/(\d{4})/(\d{2})(\d{2})-", url)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except ValueError:
            pass
    return None


# =============================================================================
# SITEMAP DISCOVERY
# =============================================================================


def discover_urls_from_sitemap(client: httpx.Client, existing_urls: set) -> List[str]:
    """
    Fetch newsroom.xml sitemap and return press release URLs not already in the database.

    Pre-filters by year in URL path to only fetch 2026+ articles.
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

    # Filter to press release detail pages (not category/year index pages)
    # Detail URLs have the pattern: /press-releases/YYYY/MMDD-slug
    press_urls = []
    for url in all_urls:
        if PRESS_RELEASES_PREFIX not in url:
            continue
        # Must match detail page pattern (year/slug), not just index pages
        if not re.search(r"/press-releases/\d{4}/\d{4}-", url):
            continue
        # Pre-filter by year in URL
        year_match = re.search(r"/press-releases/(\d{4})/", url)
        if year_match and int(year_match.group(1)) < START_DATE.year:
            continue
        if url in existing_urls:
            continue
        press_urls.append(url)

    click.echo(
        f"Sitemap: {len(all_urls)} total URLs, "
        f"{len(press_urls)} new press release URLs (>= {START_DATE.year}) to scrape."
    )
    return press_urls


# =============================================================================
# ARTICLE SCRAPING
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article(url: str, client: httpx.Client) -> Dict[str, Any]:
    """
    Fetch and extract content from a mom.gov.sg press release page.

    MOM uses ASP.NET/Telerik. Key extraction:
    - Title: <h1> element or page <title>
    - Date: extracted from URL pattern (/YYYY/MMDD-) or page text
    - Content: <article>, <main>, or content container
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
            title = title_tag.get_text(strip=True).split("|")[0].split("-")[0].strip()

    # Date — prefer URL-based extraction (most reliable for MOM)
    published_date = extract_date_from_url(url)

    # Fallback to page text
    if not published_date:
        page_text = soup.get_text(" ", strip=True)
        m = re.search(
            r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|"
            r"Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2})\b",
            page_text,
        )
        if m:
            published_date = parse_date_string(m.group(1))

    # Content — try multiple selectors for ASP.NET layout
    content_text = ""
    for selector in ["article", "main", "[class*='content']", "body"]:
        container = soup.select_one(selector)
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
            if len(content_text) > 100:
                break

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
                    {"role": "user", "content": f"Summarise this MOM press release:\n\n{content_snippet}"},
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
    Fetch new MOM press releases via sitemap discovery.

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
                f"({pub_date}, {category}, {len(article.get('content_text', ''))} chars)"
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
