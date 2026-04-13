"""
Personal Data Protection Commission Singapore press room resource.

Cadence: Daily (Tier 1)
Source: https://www.pdpc.gov.sg/news-and-events/press-room
Strategy: Incremental -- JSON API discovery via CWP platform, filtered to 2026+ articles.
  The PDPC press room uses a form-encoded POST API with CSRF token authentication.
  We fetch the token from the page, then paginate the API for the current year.

Licensing: Government of Singapore, all rights reserved.
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

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://www.pdpc.gov.sg"
PRESS_ROOM_URL = f"{BASE_URL}/news-and-events/press-room"
API_URL = f"{BASE_URL}/api/pdpcpressroom/getpressroomlisting"

# Only keep articles published from this date onwards
START_DATE = date(2026, 1, 1)

# Scraping rate limits (Tier 1 -- daily incremental, be polite)
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
As an expert in Singapore data protection law and privacy regulation, provide concise
summaries of PDPC press releases, enforcement decisions, speeches, and advisories for
legal practitioners and privacy professionals. Highlight key PDPA enforcement outcomes,
financial penalties, regulatory guidance, policy developments, or advisory updates.
Write 1 narrative paragraph, no longer than 100 words. Focus on what happened, who is
affected, and why it matters for data protection compliance in Singapore.
"""

# =============================================================================
# HELPERS
# =============================================================================


def make_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def polite_sleep():
    delay = REQUEST_DELAY_BASE + random.uniform(-REQUEST_DELAY_JITTER, REQUEST_DELAY_JITTER)
    time.sleep(max(0.5, delay))


def slugify_category(type_str: str) -> str:
    """Slugify API type field to category value (e.g., 'Media Release' -> 'media-release')."""
    return re.sub(r"[^a-z0-9]+", "-", type_str.strip().lower()).strip("-")


def parse_date_string(date_str: str) -> Optional[str]:
    """Parse 'DD Mon YYYY' or 'DD Month YYYY' to ISO date string."""
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
# API DISCOVERY
# =============================================================================


def get_csrf_token(client: httpx.Client) -> str:
    """Fetch the press room page and extract the CSRF token."""
    click.echo(f"Fetching CSRF token from: {PRESS_ROOM_URL}")
    response = client.get(PRESS_ROOM_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "lxml")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    token = token_input["value"] if token_input else ""
    if not token:
        click.echo("Warning: CSRF token not found in page", err=True)
    return token


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_api_page(client: httpx.Client, token: str, year: str, page: int) -> Dict[str, Any]:
    """Fetch a single page of results from the PDPC press room API."""
    response = client.post(
        API_URL,
        data={"page": page, "year": year, "type": "all", "keyword": ""},
        headers={
            "RequestVerificationToken": token,
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    response.raise_for_status()
    return response.json()


def discover_items_from_api(client: httpx.Client, token: str, existing_urls: set) -> List[Dict[str, Any]]:
    """
    Discover press room items from the PDPC API for 2026 onwards.

    Paginates through all pages for each year from START_DATE.year to current year.
    Returns items not already in the database.
    """
    current_year = date.today().year
    all_items: List[Dict[str, Any]] = []

    for year in range(START_DATE.year, current_year + 1):
        year_str = str(year)
        page = 1

        while True:
            click.echo(f"Fetching API: year={year_str}, page={page}")
            polite_sleep()

            try:
                data = fetch_api_page(client, token, year_str, page)
            except Exception as e:
                click.echo(f"API request failed for year={year_str}, page={page}: {e}", err=True)
                break

            if data.get("ResponseCode") != "OK":
                click.echo(f"API error: {data.get('Message', 'unknown')}", err=True)
                break

            items = data.get("items", [])
            if not items:
                break

            total_pages = data.get("totalPages", 1)

            for item in items:
                rel_url = item.get("url", "")
                full_url = f"{BASE_URL}{rel_url}" if rel_url else ""
                if full_url and full_url not in existing_urls:
                    all_items.append(item)

            click.echo(f"  Found {len(items)} items (page {page}/{total_pages})")

            if page >= total_pages:
                break
            page += 1

    click.echo(f"Total new items discovered: {len(all_items)}")
    return all_items


# =============================================================================
# ARTICLE SCRAPING
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article_content(url: str, client: httpx.Client) -> str:
    """
    Fetch and extract content text from a PDPC detail page.

    The content is in the main content area, typically under #mainContent.
    """
    response = client.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "lxml")

    # Try #mainContent first, then fall back to <main> or <body>
    container = soup.find(id="mainContent") or soup.find("main") or soup.find("body")
    if not container:
        return ""

    for unwanted in container.find_all(
        ["nav", "header", "footer", "script", "style", "aside", "form"]
    ):
        unwanted.decompose()

    parts = [
        el.get_text(strip=True)
        for el in container.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"])
        if len(el.get_text(strip=True)) > 15
    ]
    return "\n\n".join(parts)


# =============================================================================
# AI SUMMARY
# =============================================================================


async def get_summary(text: str, title: str) -> str:
    base_url = os.environ.get("LLM_BASE_URL", "")
    api_key = os.environ.get("LLM_API_KEY", "")
    model = os.environ.get("LLM_MODEL", "")

    if not base_url:
        click.echo("  LLM_BASE_URL not set -- skipping summary", err=True)
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
                    {"role": "user", "content": f"Summarise this PDPC item:\n\n{content_snippet}"},
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
    Fetch new PDPC press room articles via JSON API discovery.

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
        # Step 1: Get CSRF token
        try:
            token = get_csrf_token(client)
        except Exception as e:
            click.echo(f"Failed to get CSRF token: {e}", err=True)
            return []

        # Step 2: Discover items from API
        api_items = discover_items_from_api(client, token, existing_urls)

        if not api_items:
            click.echo("No new items to process.")
            return []

        # Step 3: Scrape detail pages for content
        click.echo(f"\nScraping {len(api_items)} articles...")
        for i, item in enumerate(api_items, 1):
            rel_url = item.get("url", "")
            full_url = f"{BASE_URL}{rel_url}"
            title = item.get("title", "")
            click.echo(f"[{i}/{len(api_items)}] {full_url}")
            polite_sleep()

            # Parse date from API response
            pub_date = parse_date_string(item.get("date", ""))

            # Filter by date
            if pub_date:
                try:
                    if date.fromisoformat(pub_date) < START_DATE:
                        click.echo(f"  Skipping (before {START_DATE}): {pub_date}")
                        continue
                except ValueError:
                    pass
            elif pub_date is None:
                click.echo("  Warning: no date found, including anyway")

            # Fetch detail page content
            content_text = ""
            try:
                content_text = fetch_article_content(full_url, client)
                consecutive_failures = 0
            except Exception as e:
                click.echo(f"  Failed to fetch content: {e}", err=True)
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    click.echo("Circuit breaker triggered -- too many consecutive failures.", err=True)
                    break
                # Still include the item with description as fallback
                content_text = item.get("description", "")

            category = slugify_category(item.get("type", "other"))

            result = {
                "id": make_id(full_url),
                "source_url": full_url,
                "category": category,
                "title": title,
                "published_date": pub_date,
                "content_text": content_text,
                "summary": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            results.append(result)
            click.echo(
                f"  -> {title[:60]} "
                f"({pub_date}, {category}, {len(content_text)} chars)"
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
