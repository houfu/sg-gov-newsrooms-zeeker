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
pattern. If the agency has an RSS feed, prefer that over scraping.

### `judiciary_news` — Singapore Judiciary

- **Source**: https://www.judiciary.gov.sg/news-and-resources/news
- **Cadence**: Daily (Tier 1) — ~3–10 new items per week
- **Discovery**: Sitefinity CMS GetFilteredList AJAX endpoint (`POST /news-and-resources/news/GetFilteredList/`),
  filtered by year, paginated via `CurrentPage` (0-indexed)
- **Coverage**: Speeches, media releases, media advisories, appointments — from 2026 onwards.
  ~30 articles per year (3 pages of 10).
- **Content**: Full text scraped from `.detail-wrapper .col-md-8` on each detail page
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden
  from Datasette default view — accessible via direct SQL/FTS only.
- **UI approach**: `content_text` hidden; `summary` (AI-generated) is primary search field.

**Discovery endpoint**:
```
POST https://www.judiciary.gov.sg/news-and-resources/news/GetFilteredList/
Content-Type: application/json
{"model": {"CurrentPage": 0, "SelectedYear": "2026", "SearchKeywords": "", "SelectedCourts": [], "SelectedTopics": [], "SelectedContentTypes": []}}
```
Returns JSON `{listPartialView: "<html>"}`. Pagination: increment `CurrentPage` until 0 items returned.

**Scraping notes**:
- Same User-Agent and delay policy as `mlaw_news`
- robots.txt: /news/ pages are public government content; no disallow rules for this path

### `agc_news` — Attorney-General's Chambers Singapore

- **Source**: https://www.agc.gov.sg/newsroom/
- **Cadence**: Daily (Tier 1)
- **Discovery**: Sitemap-based (`sitemap.xml` filtered to `/newsroom/*` URLs)
- **Coverage**: Press releases, media statements, speeches, prosecution updates — from 2026 onwards
- **Content**: Full text scraped from `<main>` element (Isomer/Next.js site)
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden.
- **Date extraction**: "last updated DD Month YYYY" Isomer footer pattern, with fallback

### `ipos_news` — Intellectual Property Office of Singapore

- **Source**: https://www.ipos.gov.sg/news/news-collection/
- **Cadence**: Daily (Tier 1) — ~1–3 new items per week
- **Discovery**: Paginated listing pages (`?page=N`). News URLs are NOT in the sitemap.
- **Coverage**: Media releases, speeches, insights, updates — from 2026 onwards
- **Archive size**: ~100 items total; only 2026+ imported
- **Content**: Full text scraped from `<main id="main-content">` (Isomer/Next.js site)
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden.
- **Incremental stop**: After 5 consecutive known URLs (listing sorted newest-first)

### `ccs_news` — Competition and Consumer Commission of Singapore

- **Source**: https://www.ccs.gov.sg/media-and-events/newsroom/announcements-and-media-releases/
- **Cadence**: Daily (Tier 1) — ~1–3 new items per week
- **Discovery**: Paginated listing pages. News URLs are NOT in the sitemap.
- **Coverage**: Announcements, media releases, forum letter replies — from 2026 onwards
- **Archive size**: ~473 items total across 48 pages; only 2026+ imported
- **Content**: Full text scraped from `<main>` element (Isomer/Next.js site)
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden.

### `acra_news` — Accounting and Corporate Regulatory Authority

- **Source**: https://www.acra.gov.sg/news-events/news-announcements/
- **Cadence**: Daily (Tier 1) — ~2–5 new items per week
- **Discovery**: Sitemap-based (`sitemap.xml` filtered to `/news-events/news-announcements/*`)
- **Coverage**: Announcements, press releases, speeches, newsletters — from 2026 onwards
- **Archive size**: ~480 URLs in sitemap; only 2026+ imported
- **Content**: Full text scraped from `<main>` element (Isomer/Next.js site)
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden.

### `mom_news` — Ministry of Manpower Singapore

- **Source**: https://www.mom.gov.sg/newsroom/press-releases
- **Cadence**: Daily (Tier 1) — ~2–5 new items per week
- **Discovery**: Sitemap-based (`newsroom.xml` filtered to `/newsroom/press-releases/YYYY/` URLs)
- **Coverage**: Press releases on workplace safety, employment, foreign workforce — from 2026 onwards
- **Archive size**: ~763 press release URLs in sitemap; only 2026+ imported
- **Content**: Full text scraped from `<article>` or `<main>` element (ASP.NET/Telerik site)
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden.
- **Date extraction**: Parsed from URL pattern `/press-releases/YYYY/MMDD-slug`

### `pdpc_news` — Personal Data Protection Commission

- **Source**: https://www.pdpc.gov.sg/news-and-events/press-room
- **Cadence**: Daily (Tier 1) — ~1–2 new items per month
- **Discovery**: CWP JSON API (`POST /api/pdpcpressroom/getpressroomlisting`).
  Requires CSRF token from `__RequestVerificationToken` hidden input on press room page.
  Form-encoded POST with `RequestVerificationToken` header.
- **API parameters**: `page`, `year`, `type`, `keyword`
- **API response**: `{"ResponseCode":"OK","totalPages":N,"items":[{title,date,description,type,url}]}`
- **Coverage**: Media releases, speeches, forum replies, advisories — from 2026 onwards
- **Content**: Full text scraped from `#mainContent` on each detail page
- **Licensing**: © Government of Singapore, all rights reserved. Content stored but hidden.
- **Categories**: media-release, speech, forum-reply, article-clarification, advertorial

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
