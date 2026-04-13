"""RSS feed discovery and fetching for PE firm websites."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import re
from urllib.parse import urljoin, urlparse

import feedparser

from .http_client import resilient_get
from .models import Article, Firm

logger = logging.getLogger(__name__)

# =============================================================================
# CORE RSS FEEDS — Non-firm-specific deal announcement sources.
# Kept to BusinessWire only: most PE deal press releases are distributed
# through BusinessWire first. Firm-specific RSS/press pages provide the rest.
# =============================================================================

CORE_FEEDS: list[dict[str, str]] = [
    {"url": "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",
     "source": "BusinessWire"},
]


# =============================================================================
# LAW FIRM PRESS PAGES — Buy-side M&A counsel for top PE firms
# Law firm press releases announce signed/closed deals 24–48 hours after signing,
# naming the PE buyer, target, seller, and often deal value.
# =============================================================================

LAW_FIRM_SOURCES: list[dict[str, str]] = [
    {
        "name": "Kirkland & Ellis",
        "domain": "kirkland.com",
        "press_url": "https://www.kirkland.com/insights",
    },
    {
        "name": "Simpson Thacher & Bartlett",
        "domain": "stblaw.com",
        "press_url": "https://www.stblaw.com/about-us/news",
    },
    {
        "name": "Latham & Watkins",
        "domain": "lw.com",
        "press_url": "https://www.lw.com/en/news",
    },
    {
        "name": "Paul, Weiss",
        "domain": "paulweiss.com",
        "press_url": "https://www.paulweiss.com/insights/client-news",
    },
    {
        "name": "Ropes & Gray",
        "domain": "ropesgray.com",
        "press_url": "https://www.ropesgray.com/en/newsroom",
    },
    {
        "name": "Debevoise & Plimpton",
        "domain": "debevoise.com",
        "press_url": "https://www.debevoise.com/news",
    },
    {
        "name": "Willkie Farr & Gallagher",
        "domain": "willkie.com",
        "press_url": "https://www.willkie.com/news",
    },
    {
        "name": "Gibson, Dunn & Crutcher",
        "domain": "gibsondunn.com",
        "press_url": "https://www.gibsondunn.com/category/firm-news/",
    },
    {
        "name": "Paul Hastings",
        "domain": "paulhastings.com",
        "press_url": "https://www.paulhastings.com/news",
    },
    {
        "name": "Freshfields Bruckhaus Deringer",
        "domain": "freshfields.com",
        "press_url": "https://www.freshfields.com/en/our-thinking/news",
    },
    {
        "name": "Clifford Chance",
        "domain": "cliffordchance.com",
        "press_url": "https://www.cliffordchance.com/news.html",
    },
]


def get_law_firm_sources() -> list[Firm]:
    """Return law firm press page sources as Firm objects."""
    return [
        Firm(
            name=src["name"],
            domain=src["domain"],
            press_url=src["press_url"],
            source_category="law_firm",
        )
        for src in LAW_FIRM_SOURCES
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

_MAX_WORKERS = 10

# Regex to find <link rel="alternate" type="application/rss+xml" href="..."> in HTML
_LINK_RE = re.compile(
    r'<link[^>]+type=["\']application/(rss|atom)\+xml["\'][^>]+href=["\']([^"\']+)["\']',
    re.IGNORECASE,
)


def _try_feed_url(url: str) -> tuple[str, int] | None:
    """Try a URL as an RSS feed. Returns (url, entry_count) or None."""
    resp = resilient_get(url, playwright_fallback=False)
    if not resp.ok:
        return None
    # feedparser handles content-type detection itself; try to parse regardless.
    parsed = feedparser.parse(resp.text)
    if parsed.entries:
        return (url, len(parsed.entries))
    return None


def _discover_from_html(page_url: str) -> str | None:
    """Fetch a page and look for RSS <link> tags in the HTML."""
    resp = resilient_get(page_url, playwright_fallback=False)
    if not resp.ok:
        return None
    for match in _LINK_RE.finditer(resp.text[:50_000]):  # Only scan head area
        href = match.group(2)
        feed_url = urljoin(page_url, href)
        result = _try_feed_url(feed_url)
        if result:
            return result[0]
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


def _fetch_single_feed(firm: Firm, lookback_hours: int | None = None,
                       state=None) -> list[Article]:
    """Fetch articles from a single firm's RSS feed."""
    if not firm.feed_url:
        return []

    resp = resilient_get(firm.feed_url, playwright_fallback=False,
                         firm_name=firm.name, state=state)
    if not resp.ok:
        logger.warning("Feed fetch failed for %s (status=%d, error=%s)",
                       firm.name, resp.status_code, resp.error_type)
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


def fetch_articles(firms: list[Firm], lookback_hours: int = 24, state=None) -> list[Article]:
    """Fetch articles from all firms with RSS feeds, filtered by lookback window."""
    firms_with_feeds = [f for f in firms if f.feed_url]
    # Skip firms flagged needs_url_update
    if state is not None:
        skipped = [f for f in firms_with_feeds if state.should_skip_firm(f.name)]
        if skipped:
            logger.info("Skipping %d firms flagged needs_url_update: %s",
                        len(skipped), ", ".join(f.name for f in skipped))
            firms_with_feeds = [f for f in firms_with_feeds if not state.should_skip_firm(f.name)]
    logger.info("Fetching RSS feeds from %d firms (lookback=%dh)", len(firms_with_feeds), lookback_hours)

    all_articles: list[Article] = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_single_feed, firm, lookback_hours, state): firm
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

    resp = resilient_get(url, playwright_fallback=False)
    if not resp.ok:
        logger.warning("Core feed fetch failed for %s (status=%d, error=%s)",
                       source, resp.status_code, resp.error_type)
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
    """Fetch articles from core RSS feeds (BusinessWire)."""
    logger.info("Fetching %d core feeds (lookback=%dh)", len(CORE_FEEDS), lookback_hours)

    all_articles: list[Article] = []
    for feed in CORE_FEEDS:
        try:
            articles = _fetch_single_core_feed(feed, lookback_hours)
            all_articles.extend(articles)
        except Exception as e:
            logger.warning("Error fetching core feed %s: %s", feed["source"], e)

    logger.info("Fetched %d articles from %d core feeds", len(all_articles), len(CORE_FEEDS))
    return all_articles


def fetch_core_feeds_lookback(days: int = 30) -> list[Article]:
    """Fetch core feeds with no date filter (for lookback exercises)."""
    logger.info("Fetching %d core feeds with %d-day lookback", len(CORE_FEEDS), days)

    all_articles: list[Article] = []
    for feed in CORE_FEEDS:
        try:
            all_articles.extend(_fetch_single_core_feed(feed, None))
        except Exception as e:
            logger.warning("Error fetching core feed %s: %s", feed["source"], e)

    logger.info("Fetched %d articles from %d core feeds (lookback %dd)",
                len(all_articles), len(CORE_FEEDS), days)
    return all_articles
