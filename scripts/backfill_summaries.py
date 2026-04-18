#!/usr/bin/env python3
"""
Backfill empty summaries in sg-gov-newsrooms.db using the LLM.

Resumable: tracks progress in backfill_progress.json (crash-safe, flushes
after each row). Safe to Ctrl+C and restart.

Usage:
    cd ~/nanoclaw/groups/discord_main/sg-gov-newsrooms-zeeker
    set -a; source ~/.config/zeeker/.env; set +a
    uv run python scripts/backfill_summaries.py
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

from openai import AsyncOpenAI

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "sg-gov-newsrooms.db"
PROGRESS_PATH = PROJECT_DIR / "backfill_progress.json"

# ---------------------------------------------------------------------------
# LLM config (same env vars as the resource modules)
# ---------------------------------------------------------------------------

LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "")

# ---------------------------------------------------------------------------
# Per-table system prompts (copied from each resource module)
# ---------------------------------------------------------------------------

SYSTEM_PROMPTS: dict[str, str] = {
    "agc_news": (
        "As an expert in Singapore law and public policy, provide concise summaries of "
        "Attorney-General's Chambers press releases, media statements, and speeches for "
        "legal practitioners and policy researchers. Highlight the key legal developments, "
        "prosecution outcomes, policy positions, or appointments. Write 1 narrative paragraph, "
        "no longer than 100 words. Focus on what happened, who is affected, and why it matters "
        "legally or institutionally."
    ),
    "acra_news": (
        "As an expert in Singapore corporate regulation and business compliance, provide "
        "concise summaries of ACRA news announcements, press releases, and speeches for "
        "business owners, compliance professionals, and corporate service providers. "
        "Highlight key regulatory changes, enforcement actions, filing requirements, "
        "corporate governance updates, or industry developments. Write 1 narrative paragraph, "
        "no longer than 100 words. Focus on what changed, who is affected, and why it matters "
        "for businesses and corporate entities in Singapore."
    ),
    "ccs_news": (
        "As an expert in competition law and consumer protection, provide concise summaries of CCCS "
        "announcements, media releases, and advisories for legal practitioners, businesses, and policy "
        "researchers. Highlight key competition or consumer protection issues, merger decisions, "
        "enforcement actions, penalties, and market studies. Write 1 narrative paragraph, no longer "
        "than 100 words. Focus on what happened, who is affected, and the regulatory implications."
    ),
    "ipos_news": (
        "As an expert in intellectual property law and innovation policy, provide concise summaries of "
        "IPOS news, media releases, and speeches for IP practitioners, inventors, and policy researchers. "
        "Highlight key IP policy developments, new programmes or initiatives, regulatory changes, "
        "international rankings, and appointments. Write 1 narrative paragraph, no longer than 100 words. "
        "Focus on what changed, who is affected, and why it matters for the IP ecosystem."
    ),
    "mom_news": (
        "As an expert in Singapore employment law and workforce policy, provide concise summaries of "
        "Ministry of Manpower press releases for employers, workers, HR professionals, and policy "
        "researchers. Highlight key enforcement actions, workplace safety incidents, employment "
        "statistics, foreign workforce updates, labour policy changes, or regulatory developments. "
        "Write 1 narrative paragraph, no longer than 100 words. Focus on what happened, who is "
        "affected, and why it matters for workplaces in Singapore."
    ),
    "pdpc_news": (
        "As an expert in Singapore data protection law and privacy regulation, provide concise "
        "summaries of PDPC press releases, enforcement decisions, speeches, and advisories for "
        "legal practitioners and privacy professionals. Highlight key PDPA enforcement outcomes, "
        "financial penalties, regulatory guidance, policy developments, or advisory updates. "
        "Write 1 narrative paragraph, no longer than 100 words. Focus on what happened, who is "
        "affected, and why it matters for data protection compliance in Singapore."
    ),
}

# Per-table user prompt prefixes (matching resource modules)
USER_PROMPT_PREFIX: dict[str, str] = {
    "agc_news": "Summarise this AGC item:",
    "acra_news": "Summarise this ACRA item:",
    "ccs_news": "Summarise this CCCS item:",
    "ipos_news": "Summarise this IPOS item:",
    "mom_news": "Summarise this MOM press release:",
    "pdpc_news": "Summarise this PDPC item:",
}

# Tables to process (order: largest first for best progress visibility)
TABLES = ["acra_news", "mom_news", "agc_news", "ccs_news", "ipos_news", "pdpc_news"]

# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

SEMAPHORE = asyncio.Semaphore(3)

# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------


def load_progress() -> dict[str, bool]:
    if PROGRESS_PATH.exists():
        try:
            return json.loads(PROGRESS_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            print(f"WARNING: could not read {PROGRESS_PATH}, starting fresh")
    return {}


def save_progress(progress: dict[str, bool]) -> None:
    tmp = PROGRESS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress))
    tmp.rename(PROGRESS_PATH)  # atomic on POSIX


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def fetch_rows(table: str) -> list[tuple[str, str, str]]:
    """Return (id, title, content_text) for rows needing summaries."""
    conn = get_db()
    try:
        rows = conn.execute(
            f"SELECT id, title, content_text FROM [{table}] "
            "WHERE summary IS NULL OR summary = '' OR summary = 'None'",
        ).fetchall()
        return rows
    finally:
        conn.close()


def update_summary(table: str, row_id: str, summary: str) -> None:
    """Write a summary back to the DB, retrying on lock."""
    for attempt in range(5):
        try:
            conn = get_db()
            try:
                conn.execute(
                    f"UPDATE [{table}] SET summary = ? WHERE id = ?",
                    (summary, row_id),
                )
                conn.commit()
                return
            finally:
                conn.close()
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and attempt < 4:
                time.sleep(2 ** attempt)
            else:
                raise


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------


async def summarise(
    client: AsyncOpenAI, table: str, title: str, content_text: str
) -> str:
    content_snippet = content_text[:4000] if content_text else title
    user_msg = f"{USER_PROMPT_PREFIX[table]}\n\n{content_snippet}"

    async with SEMAPHORE:
        response = await client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPTS[table]},
                {"role": "user", "content": user_msg},
            ],
        )
        return (response.choices[0].message.content or "").strip()


# ---------------------------------------------------------------------------
# Per-row worker
# ---------------------------------------------------------------------------


async def process_row(
    client: AsyncOpenAI,
    table: str,
    row_id: str,
    title: str,
    content_text: str,
    progress: dict[str, bool],
    counter: dict[str, int],
) -> bool:
    """Returns True on success, False on failure."""
    key = f"{table}:{row_id}"
    idx = counter["done"] + counter["fail"] + 1
    total = counter["total"]
    short_title = (title[:60] + "...") if len(title) > 63 else title

    t0 = time.monotonic()
    try:
        summary = await summarise(client, table, title, content_text)
    except Exception as e:
        counter["fail"] += 1
        print(f"[{idx}/{total}] {table}: \"{short_title}\" -- FAILED ({e})")
        return False

    if not summary:
        counter["fail"] += 1
        print(f"[{idx}/{total}] {table}: \"{short_title}\" -- FAILED (empty response)")
        return False

    # Write to DB (sync, but fast)
    try:
        update_summary(table, row_id, summary)
    except Exception as e:
        counter["fail"] += 1
        print(f"[{idx}/{total}] {table}: \"{short_title}\" -- DB FAILED ({e})")
        return False

    elapsed = time.monotonic() - t0
    counter["done"] += 1
    progress[key] = True
    save_progress(progress)
    print(f"[{idx}/{total}] {table}: \"{short_title}\" -- done ({elapsed:.1f}s)")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    if not LLM_BASE_URL:
        print("ERROR: LLM_BASE_URL not set. Export it before running.", file=sys.stderr)
        sys.exit(1)
    if not LLM_MODEL:
        print("ERROR: LLM_MODEL not set. Export it before running.", file=sys.stderr)
        sys.exit(1)

    print(f"LLM: {LLM_MODEL} @ {LLM_BASE_URL}")
    print(f"DB:  {DB_PATH}")
    print(f"Progress: {PROGRESS_PATH}\n")

    client = AsyncOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY or "not-needed",
        max_retries=2,
        timeout=120.0,
    )

    progress = load_progress()

    # Collect all work across tables
    all_work: list[tuple[str, str, str, str]] = []  # (table, id, title, content_text)
    for table in TABLES:
        rows = fetch_rows(table)
        pending = [
            (table, r[0], r[1], r[2])
            for r in rows
            if f"{table}:{r[0]}" not in progress
        ]
        if pending:
            print(f"=== {table}: {len(pending)} articles to process ===")
            all_work.extend(pending)
        else:
            print(f"=== {table}: nothing to do (all done or already in progress file) ===")

    if not all_work:
        print("\nAll summaries are already filled. Nothing to do.")
        return

    counter = {"done": 0, "fail": 0, "total": len(all_work)}
    print(f"\nTotal: {counter['total']} articles to summarise\n")

    # Process tables sequentially, rows within a table concurrently (up to 3)
    current_table = None
    batch: list[tuple[str, str, str, str]] = []

    async def run_batch(batch: list[tuple[str, str, str, str]]) -> None:
        tasks = [
            process_row(client, t, rid, title, ct, progress, counter)
            for t, rid, title, ct in batch
        ]
        await asyncio.gather(*tasks)

    for item in all_work:
        table = item[0]
        if table != current_table:
            # Flush previous batch
            if batch:
                await run_batch(batch)
                batch = []
            current_table = table
            if counter["done"] + counter["fail"] > 0:
                print()  # visual separator between tables
        batch.append(item)

    # Flush final batch
    if batch:
        await run_batch(batch)

    print(f"\nDone: {counter['done']}/{counter['total']} succeeded, {counter['fail']} failed")


if __name__ == "__main__":
    # Graceful Ctrl+C
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    asyncio.run(main())
