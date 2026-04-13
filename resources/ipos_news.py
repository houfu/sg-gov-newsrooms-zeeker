"""
Intellectual Property Office of Singapore (IPOS) newsroom resource.

Cadence: Daily (Tier 1)
Source: https://www.ipos.gov.sg/news/news-collection/
Strategy: Incremental — paginated listing page discovery, filtered to 2026+ articles.
  IPOS news URLs are NOT in the sitemap, so we paginate through the listing pages
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

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_URL = "https://www.ipos.gov.sg"
LISTING_URL = "https://www.ipos.gov.sg/news/news-collection/"

# Only keep articles published from this date onwards
START_DATE = date(2026, 1, 1)

# Scraping rate limits (Tier 1 — daily incremental, be polite)
REQUEST_DELAY_BASE = 1.5
REQUEST_DELAY_JITTER = 0.5
REQUEST_TIMEOUT = 30.0
MAX_CONSECUTIVE_FAILURES = 5
MAX_RETRIES = 3

# Pagination
INCREMENTAL_STOP_THRESHOLD = 5
MAX_PAGES = 50  # Safety limit

# LLM concurrency
_LLM_SEMAPHORE = asyncio.Semaphore(3)

# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SUMMARY_SYSTEM_PROMPT = """
As an expert in intellectual property law and innovation policy, provide concise summaries of
IPOS news, media releases, and speeches for IP practitioners, inventors, and policy researchers.
Highlight key IP policy developments, new programmes or initiatives, regulatory changes,
international rankings, and appointments. Write 1 narrative paragraph, no longer than 100 words.
Focus on what changed, who is affected, and why it matters for the IP ecosystem.
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
    if "speech" in cat:
        return "speech"
    if "insight" in cat:
        return "insights"
    if "update" in cat:
        return "update"

    slug = url.lower()
    title_lower = title.lower()
    if "media-release" in slug or "media release" in title_lower:
        return "media-release"
    if "speech" in slug or "address" in slug or "speech" in title_lower:
        return "speech"
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
# LISTING PAGE DISCOVERY
# =============================================================================


def discover_urls_from_listing(client: httpx.Client, existing_urls: set) -> List[Dict[str, Any]]:
    """
    Paginate through IPOS news listing pages and extract article metadata.

    Returns list of dicts with: source_url, title, published_date, category
    """
    all_items: List[Dict[str, Any]] = []
    consecutive_known = 0
    page = 1

    while page <= MAX_PAGES:
        url = f"{LISTING_URL}?page={page}" if page > 1 else LISTING_URL
        click.echo(f"Fetching listing page {page}: {url}")

        try:
            response = client.get(url)
            if response.status_code == 404:
                break
            response.raise_for_status()
        except httpx.HTTPError as e:
            click.echo(f"Failed to fetch listing page {page}: {e}", err=True)
            break

        soup = BeautifulSoup(response.content, "lxml")

        # Find article items — Isomer sites use various card/link patterns
        items_found = 0
        for link in soup.select("a[href*='/news/news-collection/']"):
            href = link.get("href", "")
            if not href or href.rstrip("/") == "/news/news-collection":
                continue

            # Resolve relative URLs
            if href.startswith("/"):
                href = f"{BASE_URL}{href}"

            # Skip non-article links
            if href == f"{BASE_URL}/news/news-collection/" or href == LISTING_URL:
                continue

            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                continue

            items_found += 1

            if href in existing_urls:
                consecutive_known += 1
                if consecutive_known >= INCREMENTAL_STOP_THRESHOLD:
                    click.echo(
                        f"Stopping: {consecutive_known} consecutive known items on page {page}"
                    )
                    return all_items
                continue
            else:
                consecutive_known = 0

            # Try to extract date and category from surrounding context
            parent = link.find_parent(["li", "div", "article"])
            listing_date = None
            listing_category = ""
            if parent:
                text = parent.get_text(" ", strip=True)
                m = re.search(
                    r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
                    r"September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|"
                    r"Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2})",
                    text,
                )
                if m:
                    listing_date = parse_date_string(m.group(1))

                # Look for category labels
                for label in ["Media releases", "Speeches", "Insights", "Updates"]:
                    if label in text:
                        listing_category = label
                        break

            all_items.append({
                "source_url": href,
                "title": title,
                "published_date": listing_date,
                "category": listing_category,
            })

        if items_found == 0:
            click.echo(f"No items found on page {page} — stopping.")
            break

        page += 1
        polite_sleep()

    click.echo(f"Discovered {len(all_items)} new article URLs across {page - 1} pages.")
    return all_items


# =============================================================================
# ARTICLE SCRAPING
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article(url: str, client: httpx.Client) -> Dict[str, Any]:
    """
    Fetch and extract content from an IPOS newsroom article page.

    Isomer/Next.js site. Key extraction:
    - Title: <h1> element
    - Date: "last updated DD Month YYYY" or date pattern in page text
    - Content: <main id="main-content"> or <section class="component-content">
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

    # Date — look for "last updated DD Month YYYY" pattern (Isomer footer)
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

    # Content — extract from <main> or component-content
    content_text = ""
    container = soup.find("main", id="main-content") or soup.find("main") or soup.find("body")
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
                    {"role": "user", "content": f"Summarise this IPOS item:\n\n{content_snippet}"},
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
    Fetch new IPOS newsroom articles via paginated listing page discovery.

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

            # Use scraped date, fall back to listing date
            pub_date = article.get("published_date") or item.get("published_date")
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
