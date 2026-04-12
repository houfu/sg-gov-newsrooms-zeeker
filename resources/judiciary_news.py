"""
Singapore Judiciary newsroom resource.

Cadence: Daily (Tier 1)
Source: https://www.judiciary.gov.sg/news-and-resources/news
Strategy: Incremental — Sitefinity GetFilteredList AJAX endpoint filtered to 2026+.
          Pagination via POST with CurrentPage (0-indexed). Fetches detail pages for
          full content text.

Discovery mechanism: POST /news-and-resources/news/GetFilteredList/ with JSON body
    {"model": {"CurrentPage": N, "SelectedYear": "YYYY", ...}}
Returns JSON with `listPartialView` (HTML fragment). Stops when 0 items returned.
Typically 3 pages (~30 items) for 2026 content.

Licensing note: © Government of Singapore, all rights reserved. Content is stored
but NOT shown by default in the Datasette UI. Accessible only via direct SQL or FTS.
Same licensing approach as mlaw_news.
"""

import asyncio
import hashlib
import json
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

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://www.judiciary.gov.sg"
LISTING_URL = "https://www.judiciary.gov.sg/news-and-resources/news"
FILTERED_LIST_URL = "https://www.judiciary.gov.sg/news-and-resources/news/GetFilteredList/"

# Only import articles published from this date onwards
START_DATE = date(2026, 1, 1)

# Scraping rate limits (Tier 1 — daily incremental, be polite)
REQUEST_DELAY_BASE = 1.5  # seconds between requests
REQUEST_DELAY_JITTER = 0.5
REQUEST_TIMEOUT = 30.0
MAX_CONSECUTIVE_FAILURES = 5
MAX_RETRIES = 3

# LLM concurrency
_LLM_SEMAPHORE = asyncio.Semaphore(3)

# CADENCE: Daily (Tier 1)
# judiciary.gov.sg publishes ~3–10 news items per week
# Recommended cron: 0 3 * * * (3 AM UTC = 11 AM SGT)
# Strategy: Incremental — deduplicate on source_url

# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SUMMARY_SYSTEM_PROMPT = """
As an expert in Singapore law and the judiciary, provide concise summaries of Singapore Courts
news releases, speeches, and media advisories for legal practitioners and researchers.
Highlight key judicial developments, appointments, procedural changes, or court initiatives.
Write 1 narrative paragraph, no longer than 100 words.
Focus on what changed or was announced, which courts are affected, and why it matters.
"""

# =============================================================================
# HELPERS
# =============================================================================


def make_id(url: str) -> str:
    """Generate a stable 12-char ID from a URL."""
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def polite_sleep():
    """Sleep with random jitter to avoid predictable request patterns."""
    delay = REQUEST_DELAY_BASE + random.uniform(-REQUEST_DELAY_JITTER, REQUEST_DELAY_JITTER)
    time.sleep(max(0.5, delay))


def parse_date_string(date_str: str) -> Optional[str]:
    """
    Parse date strings in judiciary.gov.sg listing format.
    Examples: '10 Apr 2026', '08 Jan 2026'
    Returns ISO format YYYY-MM-DD or None if unparseable.
    """
    date_str = date_str.strip()
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    return None


# =============================================================================
# LISTING DISCOVERY via Sitefinity GetFilteredList endpoint
# =============================================================================


def fetch_listing_page(
    client: httpx.Client,
    page: int,
    year: str,
) -> List[Dict[str, Any]]:
    """
    POST to GetFilteredList endpoint for a single page of results.

    Returns list of dicts with: source_url, title, published_date, content_type, courts.
    Returns empty list when no items are found (pagination exhausted).
    """
    payload = {
        "model": {
            "CurrentPage": page,
            "SearchKeywords": "",
            "SelectedCourts": [],
            "SelectedTopics": [],
            "SelectedContentTypes": [],
            "SelectedYear": year,
        }
    }

    try:
        response = client.post(
            FILTERED_LIST_URL,
            content=json.dumps(payload),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": LISTING_URL,
            },
        )
        response.raise_for_status()
        data = response.json()
    except (httpx.HTTPError, ValueError) as e:
        click.echo(f"  Listing page {page} failed: {e}", err=True)
        return []

    list_html = data.get("listPartialView", "")
    soup = BeautifulSoup(list_html, "lxml")

    items = []
    for link in soup.select("a.list-item"):
        href = link.get("href", "").strip()
        if not href:
            continue

        # Resolve relative URL
        source_url = f"{BASE_URL}{href}" if href.startswith("/") else href

        # Title from <h4>
        h4 = link.find("h4")
        title = h4.get_text(strip=True) if h4 else link.get("title", "").strip()

        # Courts from <strong class="metadata courts">
        court_tags = link.select("strong.metadata.courts")
        courts = "|".join(c.get_text(strip=True) for c in court_tags if c.get_text(strip=True))

        # Date and content type from <span> in metadata
        spans = link.select(".metadata-wrapper span") or link.find_all("span")
        published_date = None
        content_type = ""
        for span in spans:
            text = span.get_text(strip=True)
            if re.match(r"\d{1,2}\s+\w+\s+\d{4}", text):
                published_date = parse_date_string(text)
            elif text and not re.search(r"\d{4}", text):
                content_type = text

        items.append({
            "source_url": source_url,
            "title": title,
            "published_date": published_date,
            "content_type": content_type,
            "courts": courts,
        })

    return items


def discover_news(
    client: httpx.Client,
    existing_urls: set,
    start_year: int = START_DATE.year,
) -> List[Dict[str, Any]]:
    """
    Discover all news items from start_year onwards, skipping existing URLs.

    Paginates through all available pages for each year.
    For daily incremental use, stops as soon as a full page of items are all
    already in existing_urls (avoids unnecessary HTTP requests on stable history).
    """
    all_new = []
    current_year = datetime.now().year

    for year in range(start_year, current_year + 1):
        year_str = str(year)
        click.echo(f"\nDiscovering {year_str} content...")
        page = 0

        while True:
            click.echo(f"  Fetching page {page + 1} of {year_str}...")
            items = fetch_listing_page(client, page, year_str)

            if not items:
                click.echo(f"  No more items on page {page + 1} — done with {year_str}.")
                break

            new_items = [i for i in items if i["source_url"] not in existing_urls]
            all_new.extend(new_items)

            click.echo(
                f"  Page {page + 1}: {len(items)} items, "
                f"{len(new_items)} new, {len(items) - len(new_items)} already in DB."
            )

            # If all items on this page are already in DB, stop paginating this year
            # (listing is newest-first; all older items will also be in DB)
            if not new_items:
                click.echo(f"  All items on page already in DB — stopping pagination for {year_str}.")
                break

            page += 1
            polite_sleep()

    return all_new


# =============================================================================
# CONTENT EXTRACTION
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article_content(url: str, client: httpx.Client) -> str:
    """
    Fetch full content text from a judiciary.gov.sg news detail page.

    Content is in .detail-wrapper .col-md-8 on the page.
    Returns clean extracted text.
    """
    response = client.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "lxml")

    # Primary: .detail-wrapper .col-md-8
    container = soup.select_one(".detail-wrapper .col-md-8")

    # Fallback: .sf_colsIn.primary-content
    if not container:
        container = soup.select_one(".sf_colsIn.primary-content")

    # Last resort: main content area
    if not container:
        container = soup.select_one("main") or soup.select_one("[class*='content']")

    if not container:
        return ""

    # Remove unwanted elements
    for unwanted in container.find_all(["nav", "header", "footer", "script", "style", "aside"]):
        unwanted.decompose()

    # Extract text from meaningful elements
    parts = []
    for el in container.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"]):
        text = el.get_text(strip=True)
        if text and len(text) > 10:
            parts.append(text)

    return "\n\n".join(parts)


# =============================================================================
# AI SUMMARY
# =============================================================================


async def get_summary(text: str, title: str) -> str:
    """Generate a search-optimized summary using any OpenAI-compatible LLM."""
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
                    {"role": "user", "content": f"Summarise this Singapore judiciary news item:\n\n{content_snippet}"},
                ],
            )
            summary = response.choices[0].message.content or ""
            return summary.strip()
        except Exception as e:
            click.echo(f"  Summary failed: {e}", err=True)
            return ""


async def generate_summaries(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate AI summaries for all items concurrently (semaphore-limited)."""
    tasks = [get_summary(item.get("content_text", ""), item.get("title", "")) for item in items]
    summaries = await asyncio.gather(*tasks, return_exceptions=True)

    for item, summary in zip(items, summaries):
        if isinstance(summary, Exception):
            click.echo(f"  Summary error for '{item['title'][:50]}': {summary}", err=True)
            item["summary"] = ""
        else:
            item["summary"] = summary

    return items


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================


def fetch_data(existing_table: Optional[Table]) -> List[Dict[str, Any]]:
    """
    Fetch new Singapore judiciary news items.

    Incremental: skips URLs already present in the database.
    Discovery: Sitefinity GetFilteredList endpoint, filtered by year.
    Content: BeautifulSoup extraction from .detail-wrapper .col-md-8.
    AI summary: Ollama-compatible LLM via LLM_BASE_URL env var.
    """
    # Build set of already-imported URLs for dedup
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
            "User-Agent": (
                "ZeekerBot/1.0 (+https://data.zeeker.sg; sg-gov-newsrooms research bot)"
            )
        },
        limits=httpx.Limits(max_connections=5, max_keepalive_connections=3),
    ) as client:

        # Phase 1: Discover new articles from listing endpoint
        new_items = discover_news(client, existing_urls, start_year=START_DATE.year)

        if not new_items:
            click.echo("No new items to process.")
            return []

        # Phase 2: Fetch full content from detail pages
        click.echo(f"\nFetching content for {len(new_items)} articles...")
        for i, item in enumerate(new_items, 1):
            url = item["source_url"]
            click.echo(f"[{i}/{len(new_items)}] {url}")

            try:
                content_text = fetch_article_content(url, client)
                consecutive_failures = 0
            except Exception as e:
                click.echo(f"  Content fetch failed: {e}", err=True)
                content_text = ""
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    click.echo(
                        f"  {consecutive_failures} consecutive failures — stopping.", err=True
                    )
                    break

            result = {
                "id": make_id(url),
                "source_url": url,
                "title": item["title"],
                "published_date": item["published_date"],
                "content_type": item["content_type"],
                "courts": item["courts"],
                "content_text": content_text,
                "summary": "",  # Filled in phase 3
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            results.append(result)
            click.echo(
                f"  → {item['title'][:60]} "
                f"({item['published_date']}, {len(content_text)} chars)"
            )

            if i < len(new_items):
                polite_sleep()

    if not results:
        click.echo("No articles successfully scraped.")
        return []

    # Phase 3: Generate AI summaries (async, semaphore-bounded)
    click.echo(f"\nGenerating summaries for {len(results)} articles...")
    results = asyncio.run(generate_summaries(results))

    summaries_ok = sum(1 for r in results if r.get("summary"))
    click.echo(f"\nDone: {len(results)} new articles, {summaries_ok} with summaries.")
    return results


def transform_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pass-through — no post-processing needed."""
    return raw_data
