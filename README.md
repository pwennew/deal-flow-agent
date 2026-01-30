# Deal Flow Agent

RSS-based deal intelligence scanner that monitors PE/M&A news and writes to HubSpot.

## How it works

1. Fetches articles from PE-focused RSS feeds (PEHub, Reuters PE, etc.)
2. Searches Google News RSS for target PE firm mentions
3. Filters for deal-related content (acquisitions, completions, etc.)
4. Deduplicates by title similarity
5. Writes matching articles as Notes on HubSpot Company records

## Setup

1. Add `HUBSPOT_API_KEY` secret to GitHub repo settings
2. Run workflow manually or wait for daily 6 AM UTC schedule

## Manual run

Actions tab → Deal Flow Agent → Run workflow

Options:
- `no_hubspot`: Skip HubSpot integration (dry run)
- `hours`: Lookback period (default: 24)

## Local run

```bash
pip install -r requirements.txt
python rss_monitor_v2.py --no-hubspot  # dry run
python rss_monitor_v2.py               # with HubSpot
```
