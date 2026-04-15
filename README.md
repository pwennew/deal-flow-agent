# Deal Flow Agent

Automated monitoring of private equity firm announcements to surface **carve-out deals** (a corporate parent or PE sponsor selling a division/subsidiary to another PE buyer) and qualify them against Larkhill & Company's buyer profile.

The agent runs daily, reads ~250 PE and law firm websites, classifies new deal announcements with Claude, writes qualified alerts to Notion, and feeds a downstream HubSpot deal-brief generator.

---

## What it does

**Input:** a curated list of ~250 PE firms and ~12 M&A law firms (`targets.yml`) plus the BusinessWire RSS feed.

**Output:**
- Notion database rows for every qualified deal (`pursue` or `monitor`)
- Cost-tracked daily run log (Opus classifier + Opus qualifier API spend)
- A flagged-firm list for manual URL review (firms returning 404s consistently)
- State that persists between runs so a deal is never surfaced twice

**Not in this repo:**
- **Deal brief generation + HubSpot deal creation** — handled by a separate scheduled Claude Code task (`anthropic-skills:deal-brief`) that reads `Queued` rows from Notion at 08:00 local. This repo stops at the Notion write.

---

## Pipeline (daily scan)

Entry point: `python -m carveout_monitor scan` (see `src/carveout_monitor/__main__.py::cmd_scan`). Scheduled daily at 06:00 UTC via `.github/workflows/daily_scan.yml`.

### 1. Load firms and state
- Parse `targets.yml` into `Firm` objects (`models.py::load_firms`).
- Load `state.json` via `StateManager` (`state.py`). This file tracks:
  - **`seen`** — URLs already processed (14-day TTL)
  - **`seen_deals`** — `(target, seller, stage)` triples already written to Notion (14-day TTL, fuzzy-matched)
  - **`firm_errors`** — per-firm error tracking for scraper resilience (see §Scraper resilience)

### 2. Fetch articles (three parallel sources)

**2a. Firm-specific RSS feeds** (`feeds.py::fetch_articles`)
- For each firm with a `feed_url`, fetch their RSS/Atom feed (24h lookback).
- ThreadPoolExecutor, 10 workers.

**2b. Core feeds** (`feeds.py::fetch_core_feeds`)
- BusinessWire RSS — catches deal press releases not on any specific firm's feed.
- Reuters M&A (via Google News RSS, scoped to `site:reuters.com` with M&A keywords) — catches reporter-written deal coverage with deal value / context that press releases often omit.
- 7-day lookback by default.

**2c. Press page scraping** (`scraper.py::scrape_articles`)
- For firms without RSS, scrape the HTML press page and parse article cards.
- Date-filter to 24h lookback (articles without a parseable date are dropped — press archives go back years).
- Auto-discover press pages for firms with only a `domain` (probes 41 common paths like `/news`, `/press`, `/insights`).
- JS-heavy pages fall back to a Playwright headless Chromium render.
- Also scrapes 12 M&A law firm newsrooms (`LAW_FIRM_SOURCES` in `feeds.py`) — law firms announce signed/closed deals 24–48h after signing and often name buyer + seller + value.

All three sources merge into one article list.

### 3. Dedup

**3a. URL dedup** (within this run)
Exact URL match — removes duplicates where the same article hits two sources.

**3b. State dedup** (cross-run)
Drop articles whose URL is in `state.seen` from a prior run.

### 4. Fetch full article body text
`fetcher.py::fetch_article_bodies` — for each surviving article, fetch the full page HTML, strip boilerplate (nav/footer/ads), and extract article body (capped at 6000 chars / 1500 words). This replaces the short RSS summary with the full article text to dramatically improve classification accuracy.

### 5. Classify (Claude Opus)
`classifier.py::classify_articles`

Two-pass classification:
- First pass: Opus reads each article's title + body and decides:
  - `is_carveout` (boolean)
  - `deal_type` — `corporate_carveout` (PE buys division from corporate) or `portco_carveout` (PE buys division from PE portco)
  - `stage` — `signing` or `closing`
  - `target_company`, `seller`, `buyer`, `confidence` (0–100), `reasoning`
- Articles are batched for cost efficiency.
- Filter: keep only `confidence >= 50` and drop any alert where `seller` is unknown/N/A — those are almost always regular PE acquisitions misclassified as carve-outs.

### 6. Within-run dedup (fuzzy)
The same deal often appears in multiple articles (law firm announcement + PE firm press release + BusinessWire wire). `state.py::deals_match` applies fuzzy matching on `(target, seller, stage)` using:
- Token overlap (≥2 shared tokens OR Jaccard ≥ 0.5)
- Substring containment after seller-name stripping
- Seller-prefix matching ("ContiTech" and "Continental Industrial Unit" both start with "conti")
- Acronym matching ("SWC" ↔ "Smart World Communication")
- 50+ stop words excluded (`division`, `unit`, `holdings`, `capital`, etc.)

Higher-confidence article wins; loser's title is appended to winner's reasoning so the Opus qualifier sees all headlines.

### 7. Cross-run deal dedup
Drop deals whose `(target, seller, stage)` has already been written to Notion (checked against `state.seen_deals` with the same fuzzy matching). Signing and closing have different stages so both pass.

### 8. Qualify (Claude Opus)
`qualifier.py::qualify_alerts`

Opus 4.6 reads the full article body + reasoning and scores each deal against Larkhill's buyer profile:
- **`larkhill_fit`** — 0–100 score based on deal size, sector, geography, PE firm, complexity
- **`recommended_action`** — `pursue` (high-fit, actionable), `monitor` (relevant but lower priority), or discard
- **`pe_firm`** — canonicalised PE buyer name
- **`buyer_track_record`** — prior carve-outs this PE firm has executed
- Extracted fields for the deal brief (rationale, next-step questions, etc.)

Larkhill focuses on deals ≥$300M EV; below that is noise and gets discarded (memory: `feedback_deal_size_threshold`). Opus is used for both classifier and qualifier passes — the classifier was upgraded from Sonnet after observing recall issues where legitimate carve-outs were scored below the 50-confidence threshold or had the seller extracted as "Unknown" (memory: `feedback_opus_for_critical`).

### 9. Write to Notion
`notion.py::NotionClient.write_alerts`

All `pursue` + `monitor` alerts are written to the Notion database as `Queued` rows. Each row gets a page ID tracked back to the alert. If all writes fail, the run raises (prevents silent data loss).

### 10. Persist state
Mark all processed article URLs and written deals as seen, then `state.save()`. The GitHub Actions workflow commits `state.json` back to the repo so the next run remembers what it saw.

### 11. Cost report
Log total Opus spend for the run, split by classifier vs qualifier pass (Opus: $15/$75 per M input/output tokens).

---

## Downstream (separate project)

At 08:00 local a scheduled Claude Code task runs the `anthropic-skills:deal-brief` skill:
1. Reads `Queued` rows from Notion
2. Generates a HubSpot CRM deal
3. Writes a .docx deal brief attached to the HubSpot deal
4. Updates the Notion row to `Brief Generated`

This is not in this repo — Notion is the integration point.

---

## Scraper resilience

Every HTTP call in `feeds.py`, `scraper.py`, and `fetcher.py` goes through `http_client.py::resilient_get`. The helper centralises:

| Concern | Behaviour |
|---|---|
| Headers | Realistic Chrome UA + `Accept` / `Accept-Language` (avoids bot-UA 403s from Carlyle, Ares, PSG, Ropes & Gray) |
| SSL | `verify=certifi.where()` first; on `SSLError`, retry once with `verify=False` and log a warning |
| DNS / connection errors | 2 retries with 1s/2s exponential backoff |
| Timeout | 30s default (matches Playwright) |
| 403 | If `playwright_fallback=True`, escalate to Playwright headless render; mark firm `prefer_playwright=True` so future runs route straight to Playwright |
| 404 | Increment `consecutive_404s`; at 3 consecutive 404s set `needs_url_update=True` and skip the firm on future runs until the URL is manually fixed |

The `StateManager` exposes per-firm error methods: `record_firm_error`, `record_firm_success`, `mark_prefer_playwright`, `should_skip_firm`, `prefers_playwright`, `get_flagged_firms`.

On global scrape timeout, the run logs the **names** of unfinished firms so it's obvious which ones to investigate.

---

## Other commands

```bash
python -m carveout_monitor scan                            # daily pipeline (default)
python -m carveout_monitor discover                        # probe all firms for RSS feeds + press pages
python -m carveout_monitor discover --firm "<name>"        # re-probe a single firm (e.g. one flagged needs_url_update)
python -m carveout_monitor backtest                        # classify all articles (no date filter, no Notion write)
python -m carveout_monitor lookback                        # extended window fetch + classify → CSV (for tuning)
python -m carveout_monitor feedback                        # read Verdict labels from Notion, report precision/recall
python -m carveout_monitor reset-state                     # back up and wipe state.json
```

All commands accept `--targets <path>` and `--state <path>`. `scan` accepts `--hours`, `--skip-notion`, `--skip-scraper`, `--skip-fetch`. `discover` accepts `--update` (persist to `targets.yml`) and `--firm <name>` (single-firm re-probe; with `--update`, also clears `needs_url_update` in state on success so the firm is picked back up on the next scan).

---

## Repo layout

```
src/carveout_monitor/
├── __main__.py       # CLI entry point and pipeline orchestration
├── models.py         # Pydantic: Firm, Article, DealAlert, QualifiedAlert
├── feeds.py          # RSS fetch + discovery (firm feeds + BusinessWire)
├── scraper.py        # HTML press page scraping + Playwright fallback
├── fetcher.py        # Full article body fetch for classifier context
├── http_client.py    # Shared resilient_get (SSL/headers/retry/403→Playwright)
├── classifier.py     # First-pass Opus classification
├── qualifier.py      # Second-pass Opus qualification against Larkhill profile
├── notion.py         # Notion DB write client
├── state.py          # StateManager: seen URLs, seen deals, firm_errors (fuzzy dedup)
├── feedback.py       # Pull Verdict labels from Notion to measure accuracy
└── utils.py          # Shared helpers

tests/                # pytest suite (mocked HTTP; no live network)
targets.yml           # ~250 firm configs: name, domain, feed_url, press_url, sectors
state.json            # persistent state (committed back by workflow)
.github/workflows/
├── daily_scan.yml    # 06:00 UTC daily scan
└── lookback.yml      # manual workflow_dispatch for historical exercises
```

---

## Configuration

**Secrets (GitHub Actions):**
- `ANTHROPIC_API_KEY` — Claude API
- `NOTION_API` — Notion integration token
- `NOTION_DB_ID` — target Notion database

**Firm config (`targets.yml`):**
Each firm has `name`, `domain` (required for auto-discovery), optional `feed_url` / `press_url`, plus `hq`, `sectors`, `source_category` (`pe_firm` or `law_firm`). Law firm press pages are hardcoded in `feeds.py::LAW_FIRM_SOURCES` rather than `targets.yml`.

**State schema (`state.json`):**
```json
{
  "version": 2,
  "last_run": "2026-04-10T06:15:00",
  "seen": { "<url>": {"hash": "...", "first_seen": "..."} },
  "seen_deals": { "<target|seller|stage>": {"first_seen": "..."} },
  "firm_errors": {
    "<firm_name>": {
      "last_error": "ssl|dns|timeout|403|404|http_error",
      "last_error_date": "...",
      "consecutive_404s": 0,
      "needs_url_update": false,
      "prefer_playwright": false
    }
  }
}
```

---

## Tests

```bash
pip install -r requirements.txt
pip install pytest
PYTHONPATH=src python -m pytest tests/
```

HTTP is fully mocked — no live network calls. Covers: feed discovery/fetch, body extraction, HTTP client resilience (SSL fallback, DNS retry, 403 escalation, 404 tracking), scraper routing (prefer_playwright, needs_url_update, global timeout logging), state dedup (exact + fuzzy), and classifier/qualifier wiring.

---

## Known areas for improvement

1. **Press page auto-discovery** is best-effort and sometimes picks the wrong page (portfolio news vs press releases). `discover` command + manual `targets.yml` review is currently the safety net.
2. **Fuzzy deal matching** (`state.py::deals_match`) is tuned for English PE deal names. Cross-language targets (e.g. European firms) may need additional stop words or transliteration.
3. **Additional core feeds** — BusinessWire + Reuters M&A cover most deal announcements; adding PR Newswire and Law360 would further improve recall.
