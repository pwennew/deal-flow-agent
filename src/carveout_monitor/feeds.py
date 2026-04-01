"""RSS feed discovery and fetching for PE firm websites."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import re
from urllib.parse import urljoin, urlparse

import feedparser
import requests

from .models import Article, Firm

logger = logging.getLogger(__name__)

# =============================================================================
# CORE RSS FEEDS — Google News, press wire services, PE news sites
# These are NOT firm-specific — they surface deal announcements from all sources.
# The classifier determines which are carve-outs.
# =============================================================================

CORE_FEEDS: list[dict[str, str]] = [
    # --- Direct PE / M&A news ---
    {"url": "https://www.pehub.com/feed/", "source": "PEHub"},
    {"url": "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
     "source": "PRNewswire M&A"},
    {"url": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",
     "source": "BusinessWire"},
    {"url": "https://www.globenewswire.com/RssFeed/subjectcode/27-Mergers%20And%20Acquisitions/feedTitle/GlobeNewswire%20-%20Mergers%20And%20Acquisitions",
     "source": "GlobeNewswire M&A"},

    # --- Google News: Premium journalism ---
    {"url": "https://news.google.com/rss/search?q=site:ft.com+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: FT + PE"},
    {"url": "https://news.google.com/rss/search?q=site:wsj.com+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: WSJ + PE"},
    {"url": "https://news.google.com/rss/search?q=site:bloomberg.com+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: Bloomberg + PE"},
    {"url": "https://news.google.com/rss/search?q=site:reuters.com+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: Reuters + PE"},

    # --- Google News: Deal-focused (US) ---
    {"url": "https://news.google.com/rss/search?q=private+equity+acquisition+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: PE acquisition (US)"},
    {"url": "https://news.google.com/rss/search?q=private+equity+buyout+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: PE buyout (US)"},
    {"url": "https://news.google.com/rss/search?q=leveraged+buyout+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: LBO (US)"},

    # --- Google News: Carve-out / divestiture specific ---
    {"url": "https://news.google.com/rss/search?q=%22carve-out%22+OR+%22carveout%22+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: carve-out (US)"},
    {"url": "https://news.google.com/rss/search?q=%22divestiture%22+OR+%22divests%22+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: divestiture (US)"},
    {"url": "https://news.google.com/rss/search?q=%22sells+division%22+OR+%22sells+business+unit%22+OR+%22sells+subsidiary%22+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: sells division (US)"},
    {"url": "https://news.google.com/rss/search?q=%22spin-off%22+OR+%22spinoff%22+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: spin-off (US)"},

    # --- Google News: Broad separation deals (no PE filter — catches corporate divestitures) ---
    {"url": "https://news.google.com/rss/search?q=%22carve-out%22+OR+%22carveout%22+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: carve-out broad (US)"},
    {"url": "https://news.google.com/rss/search?q=%22spin-off%22+OR+%22spinoff%22+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: spin-off broad (US)"},
    {"url": "https://news.google.com/rss/search?q=%22business+unit+sale%22+OR+%22corporate+separation%22+when:7d&hl=en-US&gl=US&ceid=US:en",
     "source": "Google News: business unit sale (US)"},

    # --- Google News: UK / Europe ---
    {"url": "https://news.google.com/rss/search?q=private+equity+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
     "source": "Google News: PE (UK)"},
    {"url": "https://news.google.com/rss/search?q=buyout+acquisition+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
     "source": "Google News: buyout acquisition (UK)"},
    {"url": "https://news.google.com/rss/search?q=%22carve-out%22+OR+%22divestiture%22+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
     "source": "Google News: carve-out / divestiture (UK)"},
]


_FEED_PATHS = [
    "/feed", "/rss", "/feed/", "/rss/",
    "/news/feed", "/news/rss", "/press/feed", "/press/rss",
    "/press-releases/feed", "/press-releases/rss",
    "/media/feed", "/media/rss",
    "/newsroom/feed", "/insights/feed",
    "/news-and-insights/feed", "/news-views/feed",
    "/blog/feed",
]

_TIMEOUT = 15
_MAX_WORKERS = 10
_USER_AGENT = "CarveOutMonitor/1.0 (+https://github.com/pwennew/Deal-Flow-Agent)"

# Regex to find <link rel="alternate" type="application/rss+xml" href="..."> in HTML
_LINK_RE = re.compile(
    r'<link[^>]+type=["\']application/(rss|atom)\+xml["\'][^>]+href=["\']([^"\']+)["\']',
    re.IGNORECASE,
)


def _try_feed_url(url: str) -> tuple[str, int] | None:
    """Try a URL as an RSS feed. Returns (url, entry_count) or None."""
    try:
        resp = requests.get(url, timeout=_TIMEOUT, headers={"User-Agent": _USER_AGENT},
                            allow_redirects=True)
        if resp.status_code != 200:
            return None
        ct = resp.headers.get("Content-Type", "").lower()
        if any(t in ct for t in ["xml", "rss", "atom"]):
            parsed = feedparser.parse(resp.text)
            if parsed.entries:
                return (url, len(parsed.entries))
    except requests.RequestException:
        pass
    return None


def _discover_from_html(page_url: str) -> str | None:
    """Fetch a page and look for RSS <link> tags in the HTML."""
    try:
        resp = requests.get(page_url, timeout=_TIMEOUT,
                            headers={"User-Agent": _USER_AGENT}, allow_redirects=True)
        if resp.status_code != 200:
            return None
        for match in _LINK_RE.finditer(resp.text[:50_000]):  # Only scan head area
            href = match.group(2)
            feed_url = urljoin(page_url, href)
            result = _try_feed_url(feed_url)
            if result:
                return result[0]
    except requests.RequestException:
        pass
    return None


def discover_feed(firm: Firm) -> str | None:
    """Discover RSS feed for a firm using multiple strategies."""
    if not firm.domain:
        return None

    # Strategy 1: Check HTML <link> tags on homepage
    homepage = f"https://{firm.domain}"
    found = _discover_from_html(homepage)
    if found:
        logger.info("Discovered feed for %s via HTML link: %s", firm.name, found)
        return found

    # Strategy 2: Check HTML <link> tags on press page
    if firm.press_url:
        found = _discover_from_html(firm.press_url)
        if found:
            logger.info("Discovered feed for %s via press page link: %s", firm.name, found)
            return found

        # Strategy 3: Try press_url + /feed (WordPress convention)
        press_feed = firm.press_url.rstrip("/") + "/feed"
        result = _try_feed_url(press_feed)
        if result:
            logger.info("Discovered feed for %s via press URL + /feed: %s (%d entries)",
                        firm.name, result[0], result[1])
            return result[0]

    # Strategy 4: Brute-force common feed paths on domain
    for path in _FEED_PATHS:
        url = f"https://{firm.domain}{path}"
        result = _try_feed_url(url)
        if result:
            logger.info("Discovered feed for %s via path probe: %s (%d entries)",
                        firm.name, result[0], result[1])
            return result[0]

    return None


def discover_feeds(firms: list[Firm]) -> dict[str, str | None]:
    """Discover RSS feeds for all firms without a known feed_url.

    Returns mapping of firm name → discovered feed URL (or None).
    """
    results: dict[str, str | None] = {}
    to_discover = [f for f in firms if not f.feed_url and f.domain]

    logger.info("Discovering feeds for %d firms (skipping %d with known feeds)",
                len(to_discover), len(firms) - len(to_discover))

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {executor.submit(discover_feed, firm): firm for firm in to_discover}
        for future in as_completed(futures):
            firm = futures[future]
            try:
                url = future.result()
                results[firm.name] = url
                if not url:
                    logger.debug("No feed found for %s", firm.name)
            except Exception as e:
                logger.warning("Error discovering feed for %s: %s", firm.name, e)
                results[firm.name] = None

    found = sum(1 for v in results.values() if v)
    logger.info("Feed discovery complete: %d found, %d not found", found, len(results) - found)
    return results


def _fetch_single_feed(firm: Firm, lookback_hours: int | None = None) -> list[Article]:
    """Fetch articles from a single firm's RSS feed."""
    if not firm.feed_url:
        return []

    try:
        resp = requests.get(firm.feed_url, timeout=_TIMEOUT,
                            headers={"User-Agent": _USER_AGENT})
        if resp.status_code != 200:
            logger.warning("Feed fetch failed for %s (status %d)", firm.name, resp.status_code)
            return []
    except requests.RequestException as e:
        logger.warning("Feed fetch error for %s: %s", firm.name, e)
        return []

    parsed = feedparser.parse(resp.text)
    cutoff = None
    if lookback_hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    articles = []
    for entry in parsed.entries:
        published = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

        if cutoff and published and published < cutoff:
            continue

        title = entry.get("title", "").strip()
        url = entry.get("link", "").strip()
        summary = entry.get("summary", "").strip()[:500]

        if not title or not url:
            continue

        articles.append(Article(
            title=title,
            url=url,
            summary=summary,
            published=published,
            firm_name=firm.name,
        ))

    logger.debug("Fetched %d articles from %s feed", len(articles), firm.name)
    return articles


def fetch_articles(firms: list[Firm], lookback_hours: int = 24) -> list[Article]:
    """Fetch articles from all firms with RSS feeds, filtered by lookback window."""
    firms_with_feeds = [f for f in firms if f.feed_url]
    logger.info("Fetching RSS feeds from %d firms (lookback=%dh)", len(firms_with_feeds), lookback_hours)

    all_articles: list[Article] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_single_feed, firm, lookback_hours): firm
            for firm in firms_with_feeds
        }
        for future in as_completed(futures):
            firm = futures[future]
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as e:
                logger.warning("Error fetching feed for %s: %s", firm.name, e)

    logger.info("Fetched %d articles from RSS feeds", len(all_articles))
    return all_articles


def fetch_all_articles(firms: list[Firm]) -> list[Article]:
    """Fetch ALL available articles from feeds (no date filter). For backtest."""
    firms_with_feeds = [f for f in firms if f.feed_url]
    logger.info("Backtest: fetching ALL articles from %d feeds", len(firms_with_feeds))

    all_articles: list[Article] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_single_feed, firm, None): firm
            for firm in firms_with_feeds
        }
        for future in as_completed(futures):
            firm = futures[future]
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as e:
                logger.warning("Error fetching feed for %s: %s", firm.name, e)

    logger.info("Backtest: fetched %d total articles from feeds", len(all_articles))
    return all_articles


def _fetch_single_core_feed(feed: dict[str, str], lookback_hours: int | None = None) -> list[Article]:
    """Fetch articles from a single core RSS feed (not firm-specific)."""
    url = feed["url"]
    source = feed["source"]

    try:
        resp = requests.get(url, timeout=_TIMEOUT,
                            headers={"User-Agent": _USER_AGENT})
        if resp.status_code != 200:
            logger.warning("Core feed fetch failed for %s (status %d)", source, resp.status_code)
            return []
    except requests.RequestException as e:
        logger.warning("Core feed fetch error for %s: %s", source, e)
        return []

    parsed = feedparser.parse(resp.text)
    cutoff = None
    if lookback_hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    articles = []
    for entry in parsed.entries:
        published = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

        if cutoff and published and published < cutoff:
            continue

        title = entry.get("title", "").strip()
        entry_url = entry.get("link", "").strip()
        summary = entry.get("summary", "").strip()[:500]

        if not title or not entry_url:
            continue

        articles.append(Article(
            title=title,
            url=entry_url,
            summary=summary,
            published=published,
            firm_name=source,  # Use feed source name as firm_name for core feeds
        ))

    logger.debug("Fetched %d articles from core feed: %s", len(articles), source)
    return articles


def fetch_core_feeds(lookback_hours: int = 168) -> list[Article]:
    """Fetch articles from all core RSS feeds (Google News, press wires, etc.).

    Uses a longer default lookback (168h = 7 days) since Google News feeds
    already filter to 7 days via the when:7d parameter.
    """
    logger.info("Fetching %d core feeds (lookback=%dh)", len(CORE_FEEDS), lookback_hours)

    all_articles: list[Article] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_single_core_feed, feed, lookback_hours): feed
            for feed in CORE_FEEDS
        }
        for future in as_completed(futures):
            feed = futures[future]
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as e:
                logger.warning("Error fetching core feed %s: %s", feed["source"], e)

    logger.info("Fetched %d articles from %d core feeds", len(all_articles), len(CORE_FEEDS))
    return all_articles


def fetch_core_feeds_lookback(days: int = 30) -> list[Article]:
    """Fetch core feeds with an extended Google News lookback window.

    Replaces 'when:7d' in Google News URLs with 'when:{days}d' and
    removes the lookback_hours date filter so all articles are returned.
    """
    extended_feeds = []
    for feed in CORE_FEEDS:
        url = feed["url"]
        if "when:7d" in url:
            url = url.replace("when:7d", f"when:{days}d")
        extended_feeds.append({"url": url, "source": feed["source"]})

    logger.info("Fetching %d core feeds with %d-day lookback", len(extended_feeds), days)

    all_articles: list[Article] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_single_core_feed, feed, None): feed
            for feed in extended_feeds
        }
        for future in as_completed(futures):
            feed = futures[future]
            try:
                articles = future.result()
                all_articles.extend(articles)
            except Exception as e:
                logger.warning("Error fetching core feed %s: %s", feed["source"], e)

    logger.info("Fetched %d articles from %d core feeds (lookback %dd)",
                len(all_articles), len(extended_feeds), days)
    return all_articles
