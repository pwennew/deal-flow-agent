"""HTML and headless browser scraping for PE firm press pages."""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .models import Article, Firm

logger = logging.getLogger(__name__)

_TIMEOUT = 15
_MAX_WORKERS = 10
_USER_AGENT = "CarveOutMonitor/1.0 (+https://github.com/pwennew/Deal-Flow-Agent)"

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
        try:
            resp = requests.get(url, timeout=_TIMEOUT,
                                headers={"User-Agent": _USER_AGENT},
                                allow_redirects=True)
            if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
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
        except requests.RequestException:
            continue
    return None


def _scrape_press_page_html(firm: Firm) -> list[Article]:
    """Scrape articles from a firm's press page using plain HTML parsing."""
    url = firm.press_url
    if not url:
        return []

    try:
        resp = requests.get(url, timeout=_TIMEOUT,
                            headers={"User-Agent": _USER_AGENT},
                            allow_redirects=True)
        if resp.status_code != 200:
            logger.warning("Press page fetch failed for %s (status %d)", firm.name, resp.status_code)
            return []
    except requests.RequestException as e:
        logger.warning("Press page fetch error for %s: %s", firm.name, e)
        return []

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
    return unique


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
            except (ValueError, KeyError):
                continue
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
            page = browser.new_page(user_agent=_USER_AGENT)
            page.goto(url, timeout=30000, wait_until="networkidle")

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


def scrape_firm(firm: Firm) -> list[Article]:
    """Scrape a firm's press page. Auto-discovers if needed, tries HTML then Playwright."""
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

    # Try HTML scraping first
    articles = _scrape_press_page_html(firm)
    if articles:
        return articles

    # Fallback to Playwright for JS-heavy pages
    logger.debug("HTML scrape returned 0 articles for %s, trying Playwright", firm.name)
    return _scrape_press_page_playwright(firm)


def scrape_articles(firms: list[Firm], lookback_hours: int = 24) -> list[Article]:
    """Scrape articles from all firms with a press page or domain.

    Runs independently of RSS — both sources are always used. Duplicate URLs
    between RSS and press page are handled by the URL dedup step in the pipeline.
    """
    to_scrape = [f for f in firms if f.press_url or f.domain]
    if not to_scrape:
        return []

    logger.info("Scraping press pages for %d firms (%d with known press_url, %d to discover)",
                len(to_scrape),
                sum(1 for f in to_scrape if f.press_url),
                sum(1 for f in to_scrape if not f.press_url))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    all_articles: list[Article] = []

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_firm, firm): firm for firm in to_scrape}
        try:
            for future in as_completed(futures, timeout=300):
                firm = futures[future]
                try:
                    articles = future.result(timeout=60)
                    # Date filter — require a parseable date within the lookback window.
                    # Articles without dates are dropped because press page archives
                    # go back years/decades and we can't tell if they're recent.
                    dated = [a for a in articles if a.published]
                    undated = len(articles) - len(dated)
                    if undated:
                        logger.debug("Dropped %d undated articles from %s", undated, firm.name)
                    filtered = [a for a in dated if a.published >= cutoff]
                    all_articles.extend(filtered)
                except TimeoutError:
                    logger.warning("Scrape timed out for %s — skipping", firm.name)
                except Exception as e:
                    logger.warning("Error scraping %s: %s", firm.name, e)
        except TimeoutError:
            unfinished = [f for f in futures if not f.done()]
            logger.warning(
                "Global scrape timeout — %d of %d firms unfinished, continuing with %d articles",
                len(unfinished), len(futures), len(all_articles),
            )
            for f in unfinished:
                f.cancel()

    logger.info("Scraped %d articles from press pages", len(all_articles))
    return all_articles
