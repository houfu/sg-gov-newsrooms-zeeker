# sg-gov-newsrooms-zeeker

Zeeker database project collecting news and announcements from Singapore government ministries
and judiciary websites.

## Project overview

- **Database**: `sg-gov-newsrooms.db`
- **Build trigger**: `bash /workspace/group/bin/build-zeeker sg-gov-newsrooms-zeeker [resource]`
- **Cadence**: Daily at 11:00 AM SGT (host-side trigger)
- **LLM**: Gemma4 26B via Ollama (via Tailscale — host configured in env)
- **GitHub Actions**: Disabled — using host-side build trigger

## Resources

### `mlaw_news` — Ministry of Law Singapore

- **Source**: https://www.mlaw.gov.sg/news/
- **Cadence**: Daily (Tier 1) — ~2–5 new items per week
- **Discovery**: Sitemap-based (`sitemap.xml` filtered to `/news/*` with `lastmod >= 2026-01-01`)
- **Coverage**: Press releases, speeches, parliamentary speeches, announcements, from 2026 onwards
- **Archive size**: ~450–500 items from 1999–2026; only 2026+ imported here
- **Content**: Full text scraped from `<main>` element via BeautifulSoup
- **Licensing**: All rights reserved (mlaw.gov.sg Terms of Use). Content stored but hidden
  from Datasette default view — accessible via direct SQL/FTS only.
- **UI approach**: `content_text` column intentionally not surfaced in default table view.
  `summary` (AI-generated, ~100 words) is the primary search/display field.

**Environment variables needed**:
- `LLM_BASE_URL` — Ollama base URL (e.g. `http://your-ollama-host:11434/v1`)
- `LLM_API_KEY` — placeholder (e.g. `not-needed` for Ollama)
- `LLM_MODEL` — model name (e.g. `gemma4:26b`)
- `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — deployment

**Scraping notes**:
- User-Agent: `ZeekerBot/1.0 (+https://data.zeeker.sg; sg-gov-newsrooms research bot)`
- Delay: 1.5s ± 0.5s between requests
- Circuit breaker: stops after 5 consecutive failures
- robots.txt: `/news/` pages are not disallowed (site is public government content)
- Retry: 3 attempts with exponential backoff (tenacity)

**Adding more agencies**: Add a new resource module in `resources/` following the same
sitemap-based pattern. If the agency has an RSS feed, prefer that over sitemap scraping.
Suggested next additions:
- Singapore Courts (judiciary.gov.sg) — has RSS
- Attorney-General's Chambers (agc.gov.sg)
- Ministry of Finance (mof.gov.sg)
- Monetary Authority of Singapore (mas.gov.sg)

## Scraping principles

All resources in this collection follow these principles:

1. **robots.txt compliance** — always check before adding a new source
2. **Polite delays** — minimum 1s between requests, with jitter
3. **User-Agent identification** — always set a descriptive bot User-Agent with contact URL
4. **Incremental only** — never re-scrape content already in the database
5. **Circuit breaker** — stop on consecutive failures, don't hammer a struggling server
6. **No personal data** — scrape institutional communications only, not individual profiles
7. **Copyright awareness** — note licensing on each resource; hide full content when unclear
8. **Per-domain rate limits** — each agency's site gets its own polite_sleep budget
