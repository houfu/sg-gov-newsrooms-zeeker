"""Shared helpers for Isomer CMS (Next.js/RSC) government sites."""

import re
from typing import Dict, List
from urllib.parse import urljoin

import click
import httpx


def parse_isomer_listing_dates(html: str) -> Dict[str, str]:
    """Extract {path: ISO date} from Isomer RSC payload in listing page HTML."""
    dates: Dict[str, str] = {}
    text = html.replace('\\"', '"')
    for m in re.finditer(
        r'"id":"([^"]+)","date":"\$D(\d{4}-\d{2}-\d{2})T[^"]*"',
        text,
    ):
        dates[m.group(1)] = m.group(2)
    return dates


def parse_isomer_listing_items(html: str, path_prefix: str) -> List[Dict[str, str]]:
    """Extract article items from Isomer RSC payload.

    Returns list of dicts with: path, date, title, category.
    """
    text = html.replace('\\"', '"')
    items = []
    for m in re.finditer(
        r'"id":"([^"]+)","date":"\$D(\d{4}-\d{2}-\d{2})T[^"]*","category":"([^"]*)","title":"([^"]*)"',
        text,
    ):
        path = m.group(1)
        if path.startswith(path_prefix):
            items.append({
                "path": path,
                "date": m.group(2),
                "category": m.group(3),
                "title": m.group(4),
            })
    return items


def fetch_isomer_listing_dates(
    client: httpx.Client,
    listing_url: str,
    path_prefix: str,
) -> Dict[str, str]:
    """Fetch an Isomer listing page and extract publication dates.

    Returns dict mapping full URLs to ISO date strings (YYYY-MM-DD).
    """
    try:
        response = client.get(listing_url)
        response.raise_for_status()
    except httpx.HTTPError as e:
        click.echo(f"Failed to fetch Isomer listing dates from {listing_url}: {e}", err=True)
        return {}

    path_dates = parse_isomer_listing_dates(response.text)

    result: Dict[str, str] = {}
    for path, date_str in path_dates.items():
        if path.startswith(path_prefix):
            full_url = urljoin(listing_url, path)
            result[full_url] = date_str

    click.echo(f"Isomer listing dates: {len(result)} articles with dates from {listing_url}")
    return result
