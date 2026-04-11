"""
Ministry of Law Singapore newsroom resource.

Cadence: Daily (Tier 1)
Source: https://www.mlaw.gov.sg/news/
Strategy: Incremental — sitemap.xml discovery, filtered to /news/* URLs from 2026 onwards.

Discovery mechanism: The site uses infinite scroll (no stable pagination), but publishes
a full sitemap.xml with <lastmod> dates. We parse the sitemap to discover all news URLs,
filter to those with lastmod >= START_DATE and not already in the DB, then scrape each
detail page for content.

Licensing note: mlaw.gov.sg content is copyright protected (all rights reserved).
Content is stored but NOT shown by default in the Datasette UI — accessible only
via direct SQL or FTS query. See zeeker.toml for column config.
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

BASE_URL = "https://www.mlaw.gov.sg"
SITEMAP_URL = "https://www.mlaw.gov.sg/sitemap.xml"
NEWS_PREFIX = "/news/"

# Only import articles published from this date onwards
START_DATE = date(2026, 1, 1)

# Scraping rate limits (Tier 1 — daily incremental, be polite)
REQUEST_DELAY_BASE = 1.5  # seconds between requests
REQUEST_DELAY_JITTER = 0.5
REQUEST_TIMEOUT = 30.0
MAX_CONSECUTIVE_FAILURES = 5
MAX_RETRIES = 3

# LLM concurrency — local Ollama handles one inference at a time
_LLM_SEMAPHORE = asyncio.Semaphore(3)

# Known news categories (URL path segments under /news/)
NEWS_CATEGORIES = {
    "press-releases",
    "speeches",
    "parliamentary-speeches",
    "announcements",
    "visits",
    "replies",
}

# CADENCE: Daily (Tier 1)
# mlaw.gov.sg publishes ~2-5 news items per week
# Recommended cron: 0 3 * * * (3 AM UTC = 11 AM SGT)
# Strategy: Incremental — sitemap filtered by lastmod and existing URLs

# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SUMMARY_SYSTEM_PROMPT = """
As an expert in Singapore law and public policy, provide concise summaries of Ministry of Law
announcements, speeches, and press releases for legal practitioners and policy researchers.
Highlight the key legal or policy developments, relevant legislation or regulations mentioned,
and the practical implications. Write 1 narrative paragraph, no longer than 100 words.
Focus on what changed, who is affected, and why it matters legally or administratively.
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


def extract_category_from_url(url: str) -> str:
    """
    Extract news category from URL path.

    e.g. https://www.mlaw.gov.sg/news/press-releases/xyz/ → press-releases
    """
    path = url.replace(BASE_URL, "").strip("/")
    parts = path.split("/")
    # parts = ['news', 'press-releases', 'item-slug']
    if len(parts) >= 2 and parts[0] == "news":
        return parts[1] if parts[1] in NEWS_CATEGORIES else "other"
    return "other"


def parse_date_string(date_str: str) -> Optional[str]:
    """
    Parse date strings in formats found on mlaw.gov.sg.

    Examples: '8 April 2026', '08 APR 2026', '08 Apr 2026'
    Returns ISO format YYYY-MM-DD or None if unparseable.
    """
    date_str = date_str.strip()
    for fmt in ("%d %B %Y", "%d %b %Y", "%-d %B %Y", "%-d %b %Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    # Try regex fallback: any "DD Month YYYY" or "DD MON YYYY"
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(20\d{2})", date_str)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %B %Y").date().isoformat()
        except ValueError:
            try:
                return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%d %b %Y").date().isoformat()
            except ValueError:
                pass
    return None


# =============================================================================
# SITEMAP DISCOVERY
# =============================================================================


def discover_news_urls(client: httpx.Client, existing_urls: set) -> List[Dict[str, Any]]:
    """
    Parse sitemap.xml and return new /news/ items with lastmod >= START_DATE.

    Returns list of dicts with: source_url, category, lastmod_date
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
        click.echo(f"Failed to parse sitemap XML: {e}", err=True)
        return []

    items = []
    total_news = 0
    skipped_date = 0
    skipped_existing = 0

    for url_el in root.findall("sm:url", ns):
        loc = (url_el.findtext("sm:loc", namespaces=ns) or "").strip()
        lastmod_str = (url_el.findtext("sm:lastmod", namespaces=ns) or "").strip()

        # Only /news/ pages (not sub-pages like /about-us/, /e-services/, etc.)
        if NEWS_PREFIX not in loc:
            continue

        # Must have at least one more path segment after /news/ (the category)
        path_after_news = loc.replace(BASE_URL + NEWS_PREFIX, "").strip("/")
        if not path_after_news or "/" not in path_after_news:
            # This is a category index page like /news/press-releases/ — skip
            continue

        total_news += 1

        # Filter by date
        if lastmod_str:
            try:
                lastmod_date = datetime.fromisoformat(lastmod_str).date()
                if lastmod_date < START_DATE:
                    skipped_date += 1
                    continue
            except ValueError:
                pass  # If we can't parse the date, include it anyway

        # Skip already-imported URLs
        if loc in existing_urls:
            skipped_existing += 1
            continue

        items.append({
            "source_url": loc,
            "category": extract_category_from_url(loc),
        })

    click.echo(
        f"Sitemap: {total_news} news URLs found. "
        f"{skipped_date} before {START_DATE}, {skipped_existing} already in DB, "
        f"{len(items)} new to process."
    )
    return items


# =============================================================================
# CONTENT EXTRACTION
# =============================================================================


@retry(stop=stop_after_attempt(MAX_RETRIES), wait=wait_exponential(multiplier=2, min=1, max=10))
def fetch_article(url: str, client: httpx.Client) -> Dict[str, Any]:
    """
    Fetch and extract content from an mlaw.gov.sg news article page.

    Returns dict with: title, published_date, content_text
    """
    response = client.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "lxml")

    # Title — prefer <h1>, fall back to <title>
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True).split("|")[0].strip()

    # Published date — look for common date patterns in the page
    published_date = None

    # Try meta tags first (og:article:published_time, article:published_time)
    for meta_name in ["article:published_time", "og:article:published_time", "date"]:
        meta = soup.find("meta", attrs={"property": meta_name}) or soup.find(
            "meta", attrs={"name": meta_name}
        )
        if meta and meta.get("content"):
            try:
                published_date = datetime.fromisoformat(meta["content"]).date().isoformat()
                break
            except ValueError:
                pass

    # Fall back to text pattern search in the page
    if not published_date:
        page_text = soup.get_text(" ", strip=True)
        # Look for "8 April 2026" or "08 Apr 2026" patterns
        m = re.search(r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|"
                      r"September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|"
                      r"Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2})\b", page_text)
        if m:
            published_date = parse_date_string(m.group(1))

    # Content — extract from <main>, fall back to <article>, then body
    content_text = ""
    for selector in ["main", "article", "[class*='content']", "body"]:
        container = soup.select_one(selector)
        if container:
            # Remove nav, header, footer, script, style
            for unwanted in container.find_all(
                ["nav", "header", "footer", "script", "style", "aside", "form"]
            ):
                unwanted.decompose()

            # Extract text from meaningful elements
            parts = []
            for el in container.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "td"]):
                text = el.get_text(strip=True)
                if text and len(text) > 15:  # Skip very short fragments
                    parts.append(text)

            content_text = "\n\n".join(parts)
            if len(content_text) > 100:  # Found meaningful content
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

    # Truncate to avoid hitting context limits
    content_snippet = text[:4000] if text else title

    async with _LLM_SEMAPHORE:
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Summarise this Ministry of Law item:\n\n{content_snippet}"},
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
    Fetch new Ministry of Law news items via sitemap discovery.

    Incremental: skips URLs already present in the database.
    Content extraction: BeautifulSoup from <main> element.
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

        # Phase 1: Discover new URLs from sitemap
        new_items = discover_news_urls(client, existing_urls)

        if not new_items:
            click.echo("No new items to process.")
            return []

        # Phase 2: Scrape each article
        click.echo(f"\nScraping {len(new_items)} articles...")
        for i, item in enumerate(new_items, 1):
            url = item["source_url"]
            click.echo(f"[{i}/{len(new_items)}] {url}")

            # Circuit breaker
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                click.echo(
                    f"Circuit breaker: {consecutive_failures} consecutive failures. Stopping.",
                    err=True,
                )
                break

            try:
                article = fetch_article(url, client)
                consecutive_failures = 0

                result = {
                    "id": make_id(url),
                    "source_url": url,
                    "category": item["category"],
                    "title": article["title"],
                    "published_date": article["published_date"],
                    "content_text": article["content_text"],
                    "summary": "",  # Filled in phase 3
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                results.append(result)
                click.echo(
                    f"  → {article['title'][:60]} "
                    f"({article['published_date']}, {len(article['content_text'])} chars)"
                )

            except Exception as e:
                consecutive_failures += 1
                click.echo(f"  → Failed: {e}", err=True)
                continue

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
