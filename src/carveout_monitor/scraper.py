"""HTML and headless browser scraping for PE firm press pages."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .http_client import _BROWSER_UA, resilient_get
from .models import Article, Firm
from .utils import scale_workers

logger = logging.getLogger(__name__)

# Upper cap on parallel workers. Scrape is I/O-bound; actual worker count is
# scaled to firm list size via `scale_workers()` so growing targets.yml doesn't
# push us past the 600s global timeout.
_MAX_WORKERS = 32

# Common press/news page paths to probe
_PRESS_PATHS = [
    "/news", "/press", "/press-releases", "/media", "/newsroom",
    "/news-and-insights", "/insights", "/news-media",
    "/press-room", "/media-centre", "/media-center",
    "/about/news", "/about/press", "/about/media",
    "/who-we-are/news", "/our-news",
    "/updates", "/announcements", "/portfolio-news",
    "/news-views", "/news-releases", "/currents",           # Nordic Capital, Onex, Riverside
    "/about-us/news-and-insights",                          # Bridgepoint
    "/en/news-and-views",                                   # Partners Group
    "/en/news",                                             # American Securities
    "/news-insights", "/news-insights/press-releases",      # Ardian
]


def discover_press_page(firm: Firm) -> str | None:
    """Try common press page paths on a firm's domain. Returns URL or None."""
    if not firm.domain:
        return None

    for path in _PRESS_PATHS:
        url = f"https://{firm.domain}{path}"
        resp = resilient_get(url, playwright_fallback=False)
        if not resp.ok:
            # Domain-level errors mean the host itself is unreachable —
            # no point trying the remaining paths.
            if resp.error_type in ("dns", "timeout"):
                logger.warning(
                    "Domain unreachable for %s (%s on %s) — skipping remaining paths",
                    firm.name, resp.error_type, firm.domain,
                )
                return None
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        # Check if page has article-like links (not just a redirect to homepage)
        links = soup.find_all("a", href=True)
        article_links = [
            a for a in links
            if any(kw in a.get("href", "").lower()
                   for kw in ["/news/", "/press/", "/media/", "/insight",
                              "release", "announce", "article"])
        ]
        if len(article_links) >= 3:
            logger.info("Discovered press page for %s: %s (%d article links)",
                        firm.name, url, len(article_links))
            return url
    return None


def _scrape_press_page_html(firm: Firm, state=None) -> tuple[list[Article], str | None]:
    """Scrape articles from a firm's press page using plain HTML parsing.

    Returns (articles, error_type). error_type is None on success, or one of the
    ResilientResponse.error_type values. Caller uses error_type to decide whether
    to escalate to Playwright (403) or skip (dns/404/etc.).
    """
    url = firm.press_url
    if not url:
        return [], None

    resp = resilient_get(url, playwright_fallback=False,
                         firm_name=firm.name, state=state)
    if not resp.ok:
        logger.warning("Press page fetch failed for %s (status=%d, error=%s)",
                       firm.name, resp.status_code, resp.error_type)
        return [], resp.error_type

    soup = BeautifulSoup(resp.text, "html.parser")
    base_url = f"https://{firm.domain}"
    articles = []

    # Look for article-like elements: <a> tags within containers
    # Common patterns: article tags, li within news lists, divs with press release classes
    for tag_name in ["article", "li", "div"]:
        for container in soup.find_all(tag_name):
            # Must have a link
            link = container.find("a", href=True)
            if not link:
                continue

            href = link.get("href", "")
            if not href or href == "#":
                continue

            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            if parsed.netloc and firm.domain not in parsed.netloc:
                continue  # Skip external links

            # Get title from link text or heading
            title = ""
            heading = container.find(["h1", "h2", "h3", "h4"])
            if heading:
                title = heading.get_text(strip=True)
            if not title:
                title = link.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            # Try to find a date
            published = _extract_date(container)

            # Try to find a summary
            summary = ""
            p_tag = container.find("p")
            if p_tag:
                summary = p_tag.get_text(strip=True)[:300]

            articles.append(Article(
                title=title,
                url=full_url,
                summary=summary,
                published=published,
                firm_name=firm.name,
            ))

    # Deduplicate by URL
    seen_urls: set[str] = set()
    unique: list[Article] = []
    for a in articles:
        if a.url not in seen_urls:
            seen_urls.add(a.url)
            unique.append(a)

    logger.debug("Scraped %d articles from %s press page", len(unique), firm.name)
    return unique, None


def _extract_date(element) -> datetime | None:
    """Try to extract a date from an HTML element."""
    # Look for <time> tag
    time_tag = element.find("time")
    if time_tag:
        dt = time_tag.get("datetime", "")
        if dt:
            try:
                parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                pass

    # Look for common date patterns in text
    text = element.get_text()
    date_patterns = [
        r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        r"(\d{4})-(\d{2})-(\d{2})",
        r"(\d{2})\.(\d{2})\.(\d{2,4})",  # MM.DD.YY or MM.DD.YYYY (Simpson Thacher)
    ]

    months = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    }

    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            groups = match.groups()
            try:
                if len(groups) == 3 and groups[1].lower() in months:
                    # "15 March 2024"
                    return datetime(int(groups[2]), months[groups[1].lower()], int(groups[0]),
                                    tzinfo=timezone.utc)
                elif len(groups) == 3 and groups[0].lower() in months:
                    # "March 15, 2024"
                    return datetime(int(groups[2]), months[groups[0].lower()], int(groups[1]),
                                    tzinfo=timezone.utc)
                elif len(groups) == 3 and groups[0].isdigit() and len(groups[0]) == 4:
                    # "2024-03-15"
                    return datetime(int(groups[0]), int(groups[1]), int(groups[2]),
                                    tzinfo=timezone.utc)
                elif len(groups) == 3 and groups[2].isdigit() and len(groups[2]) <= 4:
                    # "04.02.26" or "04.02.2026" (MM.DD.YY)
                    year = int(groups[2])
                    if year < 100:
                        year += 2000
                    return datetime(year, int(groups[0]), int(groups[1]),
                                    tzinfo=timezone.utc)
            except (ValueError, KeyError):
                continue

    # Try to extract date from URL paths (e.g. /news/2026/03/15/ — Debevoise style)
    link = element.find("a", href=True)
    if link:
        href = link.get("href", "")
        url_date = re.search(r"/(\d{4})/(\d{2})(?:/(\d{2}))?(?:/|$)", href)
        if url_date:
            try:
                year, month = int(url_date.group(1)), int(url_date.group(2))
                day = int(url_date.group(3)) if url_date.group(3) else 1
                if 2000 <= year <= 2100 and 1 <= month <= 12:
                    return datetime(year, month, day, tzinfo=timezone.utc)
            except ValueError:
                pass

    return None


def _scrape_press_page_playwright(firm: Firm) -> list[Article]:
    """Scrape articles from a JS-heavy press page using Playwright."""
    url = firm.press_url
    if not url:
        return []

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not installed — skipping headless scrape for %s", firm.name)
        return []

    articles = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=_BROWSER_UA)
            page.goto(url, timeout=30000, wait_until="networkidle")

            # Wait for article content to render (SPAs may need extra time)
            for selector in ["article a", "h2 a", "h3 a", ".news a", "a[href*='/news/']"]:
                try:
                    page.wait_for_selector(selector, timeout=5000)
                    break
                except Exception:
                    continue

            # Get rendered HTML
            html = page.content()
            browser.close()

        # Parse the rendered HTML with same logic as static scraping
        soup = BeautifulSoup(html, "html.parser")
        base_url = f"https://{firm.domain}"

        for tag_name in ["article", "li", "div"]:
            for container in soup.find_all(tag_name):
                link = container.find("a", href=True)
                if not link:
                    continue
                href = link.get("href", "")
                if not href or href == "#":
                    continue

                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc and firm.domain not in parsed.netloc:
                    continue

                title = ""
                heading = container.find(["h1", "h2", "h3", "h4"])
                if heading:
                    title = heading.get_text(strip=True)
                if not title:
                    title = link.get_text(strip=True)
                if not title or len(title) < 10:
                    continue

                published = _extract_date(container)
                summary = ""
                p_tag = container.find("p")
                if p_tag:
                    summary = p_tag.get_text(strip=True)[:300]

                articles.append(Article(
                    title=title, url=full_url, summary=summary,
                    published=published, firm_name=firm.name,
                ))

        # Deduplicate
        seen: set[str] = set()
        unique = []
        for a in articles:
            if a.url not in seen:
                seen.add(a.url)
                unique.append(a)
        articles = unique

    except Exception as e:
        logger.warning("Playwright scrape failed for %s: %s", firm.name, e)
        return []

    logger.debug("Playwright scraped %d articles from %s", len(articles), firm.name)
    return articles


def scrape_firm(firm: Firm, state=None) -> list[Article]:
    """Scrape a firm's press page. Auto-discovers if needed, tries HTML then Playwright.

    If `state` is provided:
      - Skip firms flagged `needs_url_update` (3+ consecutive 404s).
      - Route `prefer_playwright` firms straight to Playwright (persistent 403).
      - On 403 from HTML, escalate to Playwright immediately and mark prefer_playwright.
    """
    # Persistent 404: URL is dead, skip until manually updated
    if state is not None and state.should_skip_firm(firm.name):
        logger.debug("Skipping %s (needs_url_update)", firm.name)
        return []

    # If no press_url, try to discover one from domain
    if not firm.press_url and firm.domain:
        discovered = discover_press_page(firm)
        if discovered:
            firm.press_url = discovered
        else:
            logger.debug("No press page found for %s (%s)", firm.name, firm.domain)
            return []

    if not firm.press_url:
        return []

    # Persistent 403: go directly to Playwright
    if state is not None and state.prefers_playwright(firm.name):
        logger.debug("Routing %s directly to Playwright (prefer_playwright)", firm.name)
        articles = _scrape_press_page_playwright(firm)
        if articles and state is not None:
            state.record_firm_success(firm.name)
        return articles

    # Try HTML scraping first
    articles, error_type = _scrape_press_page_html(firm, state=state)
    if articles:
        return articles

    # On 403, escalate to Playwright and mark firm for future Playwright routing
    if error_type == "403":
        logger.warning("HTML 403 for %s — falling back to Playwright", firm.name)
        pw_articles = _scrape_press_page_playwright(firm)
        if pw_articles:
            if state is not None:
                state.mark_prefer_playwright(firm.name)
                state.record_firm_success(firm.name)
            return pw_articles
        return []

    # Hard errors (dns/404/timeout) — don't bother with Playwright
    if error_type in ("dns", "404", "timeout"):
        return []

    # HTML returned 0 articles but not an error: might be JS-rendered; try Playwright
    logger.debug("HTML scrape returned 0 articles for %s, trying Playwright", firm.name)
    return _scrape_press_page_playwright(firm)


def scrape_articles(firms: list[Firm], lookback_hours: int = 24, state=None) -> list[Article]:
    """Scrape articles from all firms with a press page or domain.

    Runs independently of RSS — both sources are always used. Duplicate URLs
    between RSS and press page are handled by the URL dedup step in the pipeline.
    """
    to_scrape = [f for f in firms if f.press_url or f.domain]
    if not to_scrape:
        return []

    # Filter out firms flagged needs_url_update up-front (don't waste a worker slot)
    if state is not None:
        skipped = [f for f in to_scrape if state.should_skip_firm(f.name)]
        if skipped:
            logger.info("Skipping %d firms flagged needs_url_update: %s",
                        len(skipped), ", ".join(f.name for f in skipped))
            to_scrape = [f for f in to_scrape if not state.should_skip_firm(f.name)]

    workers = scale_workers(len(to_scrape), cap=_MAX_WORKERS)
    logger.info("Scraping press pages for %d firms (%d with known press_url, %d to discover) — %d workers",
                len(to_scrape),
                sum(1 for f in to_scrape if f.press_url),
                sum(1 for f in to_scrape if not f.press_url),
                workers)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    all_articles: list[Article] = []

    # Counters for the summary log — exposes how many undated articles we're
    # admitting, which was previously lost to DEBUG-level logging.
    kept_dated = 0
    kept_undated = 0
    dropped_too_old = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_firm, firm, state): firm for firm in to_scrape}
        try:
            for future in as_completed(futures, timeout=600):
                firm = futures[future]
                try:
                    articles = future.result(timeout=60)
                    # Date filter strategy:
                    # - Dated, within cutoff → keep (normal case)
                    # - Dated, outside cutoff → drop (definitely old)
                    # - Undated → KEEP and let state.seen_urls dedup downstream
                    #   handle re-processing. Previously we dropped all undated
                    #   articles on the assumption they came from multi-year press
                    #   archives, but that silently lost legitimate recent articles
                    #   whenever the press page's date format wasn't one of the
                    #   few _extract_date() understood. First scan of a firm may
                    #   admit archive articles, but they'll be in state.seen after
                    #   one classification pass and filtered on subsequent runs.
                    for a in articles:
                        if a.published is None:
                            kept_undated += 1
                            all_articles.append(a)
                        elif a.published >= cutoff:
                            kept_dated += 1
                            all_articles.append(a)
                        else:
                            dropped_too_old += 1
                except TimeoutError:
                    logger.warning("Scrape timed out for %s — skipping", firm.name)
                except Exception as e:
                    logger.warning("Error scraping %s: %s", firm.name, e)
        except TimeoutError:
            unfinished_names = [futures[f].name for f in futures if not f.done()]
            logger.warning(
                "Global scrape timeout — %d of %d firms unfinished: %s (continuing with %d articles)",
                len(unfinished_names), len(futures),
                ", ".join(unfinished_names), len(all_articles),
            )
            for f in futures:
                if not f.done():
                    f.cancel()

    logger.info("Scraped %d articles from press pages (%d dated-in-window, "
                "%d undated kept for state dedup, %d dropped as too old)",
                len(all_articles), kept_dated, kept_undated, dropped_too_old)
    return all_articles
