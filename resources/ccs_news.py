"""
Competition and Consumer Commission of Singapore (CCCS) newsroom resource.

Cadence: Daily (Tier 1)
Source: https://www.ccs.gov.sg/media-and-events/newsroom/announcements-and-media-releases/
Strategy: Incremental — paginated listing page discovery, filtered to 2026+ articles.
  CCCS news URLs are NOT in the sitemap, so we paginate through the listing pages
  and scrape each detail page for content.

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

import click
import httpx
from bs4 import BeautifulSoup
from openai import AsyncOpenAI
from sqlite_utils.db import Table
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from ._isomer import parse_isomer_listing_dates, parse_isomer_listing_items, normalize_url
except ImportError:
    from pathlib import Path as _P
    import sys as _sys
    _sys.path.insert(0, str(_P(__file__).resolve().parent))
    from _isomer import parse_isomer_listing_dates, parse_isomer_listing_items, normalize_url

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://www.ccs.gov.sg"
LISTING_URL = "https://www.ccs.gov.sg/media-and-events/newsroom/announcements-and-media-releases/"

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
As an expert in competition law and consumer protection, provide concise summaries of CCCS
announcements, media releases, and advisories for legal practitioners, businesses, and policy
researchers. Highlight key competition or consumer protection issues, merger decisions,
enforcement actions, penalties, and market studies. Write 1 narrative paragraph, no longer
than 100 words. Focus on what happened, who is affected, and the regulatory implications.
"""

# =============================================================================
# HELPERS
# =============================================================================


def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def polite_sleep():
    delay = REQUEST_DELAY_BASE + random.uniform(-REQUEST_DELAY_JITTER, REQUEST_DELAY_JITTER)
    time.sleep(max(0.5, delay))


def infer_category(url: str, title: str, listing_category: str = "") -> str:
    """Infer article category from listing label, URL, or title."""
    cat = listing_category.strip().lower()
    if "media release" in cat:
        return "media-release"
    if "announcement" in cat:
        return "announcement"
    if "forum" in cat or "letter" in cat:
        return "forum-letter-reply"

    slug = url.lower()
    title_lower = title.lower()
    if "media-release" in slug or "media release" in title_lower:
        return "media-release"
    if "forum" in slug or "forum letter" in title_lower:
        return "forum-letter-reply"
    return "announcement"


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
# LISTING PAGE DISCOVERY
# =============================================================================


def discover_urls_from_listing(client: httpx.Client, existing_urls: set) -> List[Dict[str, Any]]:
    """
    Extract article metadata from Isomer RSC payload on the listing page.

    Isomer embeds all article data (path, date, title, category) in the React
    Server Component payload. No pagination needed — all items are on page 1.
    """
    click.echo(f"Fetching listing page: {LISTING_URL}")
    try:
        response = client.get(LISTING_URL)
        response.raise_for_status()
    except httpx.HTTPError as e:
        click.echo(f"Failed to fetch listing page: {e}", err=True)
        return []

    items = parse_isomer_listing_items(
        response.text,
        "/media-and-events/newsroom/announcements-and-media-releases/",
    )
    click.echo(f"RSC payload: {len(items)} articles found.")

    all_items: List[Dict[str, Any]] = []
    skipped_old = 0
    for item in items:
        url = f"{BASE_URL}{item['path']}"
        if normalize_url(url) in existing_urls:
            continue
        # Filter by START_DATE before scraping detail pages
        try:
            if date.fromisoformat(item["date"]) < START_DATE:
                skipped_old += 1
                continue
        except (ValueError, KeyError):
            pass
        all_items.append({
            "source_url": url,
            "title": item["title"],
            "published_date": item["date"],
            "category": item["category"],
        })

    click.echo(
        f"Discovered {len(all_items)} new article URLs"
        f" ({skipped_old} skipped, before {START_DATE})."
    )
    return all_items


# =============================================================================
# ARTICLE SCRAPING
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article(url: str, client: httpx.Client) -> Dict[str, Any]:
    """
    Fetch and extract content from a CCCS newsroom article page.

    Isomer/Next.js site. Key extraction:
    - Title: <h1> element
    - Date: "last updated DD Month YYYY" or date pattern in page text
    - Content: <main> element
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

    # Date
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

    if not published_date:
        m = re.search(
            r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
            r"September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|"
            r"Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2})\b",
            page_text,
        )
        if m:
            published_date = parse_date_string(m.group(1))

    # Content
    content_text = ""
    container = soup.find("main") or soup.find("body")
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
                    {"role": "user", "content": f"Summarise this CCCS item:\n\n{content_snippet}"},
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
    Fetch new CCCS newsroom articles via paginated listing page discovery.

    Incremental: skips URLs already in DB and articles before START_DATE.
    """
    existing_urls: set = set()
    if existing_table:
        existing_urls = {normalize_url(row["source_url"]) for row in existing_table.rows}
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
        discovered = discover_urls_from_listing(client, existing_urls)

        if not discovered:
            click.echo("No new URLs to process.")
            return []

        click.echo(f"\nScraping {len(discovered)} articles...")
        for i, item in enumerate(discovered, 1):
            url = item["source_url"]
            click.echo(f"[{i}/{len(discovered)}] {url}")
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

            pub_date = item.get("published_date") or article.get("published_date")
            if pub_date:
                try:
                    if date.fromisoformat(pub_date) < START_DATE:
                        click.echo(f"  Skipping (before {START_DATE}): {pub_date}")
                        continue
                except ValueError:
                    pass
            elif pub_date is None:
                click.echo("  Warning: no date found, including anyway")

            title = article.get("title") or item.get("title", "")
            category = infer_category(url, title, item.get("category", ""))

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
