"""
Investment Bank Mandate Monitor
Monitors press releases from top M&A advisers for "appointed as financial adviser" announcements.
Captures Adviser Appointed signals - earliest reliable public signal.

Uses investment_banks.py as single source of truth for bank reference list.
"""

import re
import requests
from datetime import datetime, timedelta
from typing import Optional
from bs4 import BeautifulSoup
import random
import time

# Import from investment_banks.py - single source of truth
from investment_banks import (
    INVESTMENT_BANKS,
    BANK_ALIASES,
    BANK_NEWS_RSS,
    match_bank,
    extract_bank_from_text,
    get_bank_tier,
)

# User agent rotation to avoid bot detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

def get_headers():
    """Get randomized headers to avoid bot detection"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

HEADERS = get_headers()

# ==========================================================
# INVESTMENT BANK PRESS RELEASE PAGES
# ==========================================================
BANK_PRESS_PAGES = {
    # Bulge bracket
    "Goldman Sachs": {
        "url": "https://www.goldmansachs.com/media-relations/press-releases/",
        "type": "html",
        "article_selector": ".press-release, article, .news-item",
        "title_selector": "h2, h3, .title, a",
        "date_selector": ".date, time, .press-release-date",
        "link_selector": "a",
    },
    "Morgan Stanley": {
        "url": "https://www.morganstanley.com/press-releases",
        "type": "html",
        "article_selector": ".press-release, article, .news-item",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "JPMorgan": {
        "url": "https://www.jpmorgan.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    
    # Elite boutiques
    "Lazard": {
        "url": "https://www.lazard.com/news/",
        "type": "html",
        "article_selector": ".news-item, article, .press-release",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Evercore": {
        "url": "https://www.evercore.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Centerview Partners": {
        "url": "https://www.centerviewpartners.com/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Moelis & Company": {
        "url": "https://www.moelis.com/about/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "PJT Partners": {
        "url": "https://www.pjtpartners.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Perella Weinberg Partners": {
        "url": "https://www.pwpartners.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    
    # European
    "Rothschild & Co": {
        "url": "https://www.rothschildandco.com/en/newsroom/press-releases/",
        "type": "html",
        "article_selector": ".news-item, article, .press-release",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    
    # Upper middle market
    "Jefferies": {
        "url": "https://www.jefferies.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Houlihan Lokey": {
        "url": "https://www.hl.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
}

# Keywords indicating adviser appointment (vs general bank news)
MANDATE_KEYWORDS = [
    # Appointment language
    "appointed as",
    "has been appointed",
    "acting as",
    "is serving as",
    "will serve as",
    "retained as",
    "engaged as",
    "selected as",
    "hired",
    "mandated",
    
    # Role descriptions
    "financial adviser",
    "financial advisor",
    "exclusive financial",
    "lead financial",
    "sole financial",
    "m&a adviser",
    "m&a advisor",
    "strategic adviser",
    "strategic advisor",
    
    # Transaction types
    "sale of",
    "sale process",
    "divestiture",
    "spin-off",
    "spinoff",
    "separation",
    "strategic review",
    "strategic alternatives",
    "exploration of alternatives",
]

# Keywords indicating carve-out specifically
CARVEOUT_INDICATORS = [
    "carve-out",
    "carve out",
    "divestiture",
    "spin-off",
    "spinoff",
    "separation",
    "division",
    "unit",
    "segment",
    "business unit",
    "subsidiary",
]


def fetch_page(url: str, timeout: int = 15) -> Optional[str]:
    """Fetch HTML content from URL"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        if response.status_code == 200:
            return response.text
        return None
    except Exception as e:
        print(f"  Warning: Failed to fetch {url}: {e}")
        return None


def extract_articles_generic(html: str, config: dict, base_url: str) -> list:
    """Extract articles using generic selectors"""
    soup = BeautifulSoup(html, 'html.parser')
    articles = []
    
    # Try each article selector
    article_selectors = config.get("article_selector", "article").split(", ")
    elements = []
    for selector in article_selectors:
        elements.extend(soup.select(selector))
    
    if not elements:
        # Fallback: look for common patterns
        elements = soup.find_all(['article', 'div'], class_=lambda x: x and any(
            term in str(x).lower() for term in ['news', 'press', 'release', 'item']
        ))
    
    for elem in elements[:20]:  # Limit to recent 20
        article = {}
        
        # Extract title
        title_selectors = config.get("title_selector", "h2, h3").split(", ")
        for sel in title_selectors:
            title_elem = elem.select_one(sel)
            if title_elem:
                article['title'] = title_elem.get_text(strip=True)
                break
        
        if not article.get('title'):
            link = elem.find('a')
            if link:
                article['title'] = link.get_text(strip=True)
        
        # Extract link
        link_elem = elem.select_one(config.get("link_selector", "a"))
        if link_elem and link_elem.get('href'):
            href = link_elem['href']
            if href.startswith('/'):
                from urllib.parse import urlparse
                parsed = urlparse(base_url)
                href = f"{parsed.scheme}://{parsed.netloc}{href}"
            article['link'] = href
        
        # Extract date
        date_selectors = config.get("date_selector", ".date, time").split(", ")
        for sel in date_selectors:
            date_elem = elem.select_one(sel)
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                if date_elem.get('datetime'):
                    date_text = date_elem['datetime']
                article['date'] = date_text
                break
        
        if article.get('title'):
            articles.append(article)
    
    return articles


def is_mandate_announcement(title: str) -> bool:
    """Check if article title indicates adviser appointment"""
    title_lower = title.lower()
    return any(kw in title_lower for kw in MANDATE_KEYWORDS)


def is_carveout_related(title: str) -> bool:
    """Check if mandate appears to be carve-out related"""
    title_lower = title.lower()
    return any(kw in title_lower for kw in CARVEOUT_INDICATORS)


def extract_mandate_info(title: str, bank_name: str) -> dict:
    """Extract basic mandate information from title"""
    
    # Get bank tier from investment_banks.py
    tier = get_bank_tier(bank_name)
    
    info = {
        "adviser": bank_name,
        "adviser_tier": tier,
        "raw_title": title,
        "signal_type": "Adviser Appointed",
        "source": f"{bank_name} Press Release",
        "is_carveout": is_carveout_related(title),
    }
    
    return info


def scrape_bank(bank_name: str, config: dict) -> list:
    """Scrape press releases from a single bank"""
    signals = []
    
    url = config.get("url")
    if not url:
        return signals
    
    html = fetch_page(url)
    if not html:
        return signals
    
    articles = extract_articles_generic(html, config, url)
    
    for article in articles:
        title = article.get('title', '')
        if not title:
            continue
        
        # Filter for mandate announcements
        if not is_mandate_announcement(title):
            continue
        
        # Extract mandate info
        mandate_info = extract_mandate_info(title, bank_name)
        mandate_info['link'] = article.get('link', url)
        mandate_info['date'] = article.get('date', '')
        
        signals.append(mandate_info)
    
    return signals


def fetch_rss_mandates() -> list:
    """Fetch mandate signals from Google News RSS feeds"""
    import feedparser
    
    # Date parsing helper
    def parse_date(date_str):
        if not date_str:
            return None
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo:
                    dt = dt.replace(tzinfo=None)
                return dt
            except ValueError:
                continue
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            return dt.replace(tzinfo=None)
        except:
            pass
        return None
    
    def is_within_48h(date_str):
        """Extended to 48h for better coverage"""
        if not date_str:
            return True
        pub_date = parse_date(date_str)
        if not pub_date:
            return True
        age = datetime.now() - pub_date
        return age.total_seconds() < 48 * 3600
    
    signals = []
    seen_urls = set()
    skipped_old = 0
    
    print(f"\nFetching mandate signals from {len(BANK_NEWS_RSS)} RSS feeds...")
    
    for feed_url in BANK_NEWS_RSS:
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:15]:
                link = entry.get("link", "")
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                
                published = entry.get("published", "")
                
                # Skip articles older than 48h
                if not is_within_48h(published):
                    skipped_old += 1
                    continue
                
                title = entry.get("title", "")
                summary = entry.get("summary", entry.get("description", ""))
                
                # Check if it's a mandate announcement
                if not is_mandate_announcement(title):
                    continue
                
                # Extract bank names from title using investment_banks.py
                bank_matches = extract_bank_from_text(f"{title} {summary}")
                
                if bank_matches:
                    # Use first (highest confidence) match
                    bank_name = bank_matches[0]
                    tier = get_bank_tier(bank_name)
                    
                    signal = {
                        "adviser": bank_name,
                        "adviser_tier": tier,
                        "raw_title": title,
                        "signal_type": "Adviser Appointed",
                        "source": feed.feed.get("title", "Google News"),
                        "link": link,
                        "date": published,
                        "summary": summary,
                        "is_carveout": is_carveout_related(title),
                    }
                    signals.append(signal)
                    
        except Exception as e:
            print(f"  Warning: Failed to fetch RSS feed: {e}")
            continue
    
    print(f"  Found {len(signals)} mandate signals (skipped {skipped_old} older than 48h)")
    return signals


def fetch_bank_mandate_signals() -> list:
    """
    Main function: Fetch all bank mandate signals.
    
    Combines:
    1. Direct press release scraping from bank websites
    2. Google News RSS feeds for "financial adviser" announcements
    
    Returns list of mandate signal dicts.
    """
    all_signals = []
    seen_titles = set()
    
    # ==================================================
    # SOURCE 1: Direct bank press release pages
    # ==================================================
    print("\nScraping bank press release pages...")
    for bank_name, config in BANK_PRESS_PAGES.items():
        print(f"  Checking {bank_name}...")
        
        # Rate limiting
        time.sleep(random.uniform(0.5, 1.5))
        
        try:
            signals = scrape_bank(bank_name, config)
            for signal in signals:
                title_key = signal.get('raw_title', '').lower()[:50]
                if title_key not in seen_titles:
                    seen_titles.add(title_key)
                    all_signals.append(signal)
                    print(f"    ✓ Found: {signal['raw_title'][:60]}...")
        except Exception as e:
            print(f"    Warning: Error scraping {bank_name}: {e}")
    
    # ==================================================
    # SOURCE 2: Google News RSS feeds
    # ==================================================
    rss_signals = fetch_rss_mandates()
    for signal in rss_signals:
        title_key = signal.get('raw_title', '').lower()[:50]
        if title_key not in seen_titles:
            seen_titles.add(title_key)
            all_signals.append(signal)
    
    print(f"\nTotal bank mandate signals: {len(all_signals)}")
    return all_signals


def format_for_claude_analysis(signals: list) -> list:
    """
    Format mandate signals as articles for Claude analysis.
    
    Converts mandate signal format to article format expected by agent.py.
    """
    articles = []
    
    for signal in signals:
        article = {
            "title": signal.get("raw_title", ""),
            "summary": signal.get("summary", f"Investment bank {signal.get('adviser', 'Unknown')} has been appointed as financial adviser."),
            "link": signal.get("link", ""),
            "published": signal.get("date", ""),
            "source": f"Bank Mandate: {signal.get('adviser', 'Unknown')}",
            
            # Metadata for filtering/enrichment
            "_source_type": "bank_mandate",
            "_adviser": signal.get("adviser"),
            "_adviser_tier": signal.get("adviser_tier"),
            "_is_carveout": signal.get("is_carveout", False),
        }
        articles.append(article)
    
    return articles


# ==========================================================
# TESTS
# ==========================================================

if __name__ == "__main__":
    print("Bank Mandate Monitor - Test Run")
    print("=" * 50)
    
    # Test bank matching using investment_banks.py
    print("\n1. Testing bank extraction from text:")
    test_texts = [
        "Goldman Sachs appointed as financial adviser for Siemens divestiture",
        "Company hires Lazard and Evercore for strategic review",
        "JP Morgan to advise on sale of industrial division",
        "Rothschild acting as sole financial adviser",
    ]
    
    for text in test_texts:
        banks = extract_bank_from_text(text)
        print(f"  '{text[:50]}...'")
        print(f"    -> Banks found: {banks}")
    
    # Test RSS feeds
    print("\n2. Testing RSS mandate fetching:")
    print("  (This makes live API calls)")
    
    try:
        import feedparser
        
        # Test just first 3 RSS feeds
        test_feeds = BANK_NEWS_RSS[:3]
        for feed_url in test_feeds:
            print(f"\n  Testing: {feed_url[:60]}...")
            feed = feedparser.parse(feed_url)
            print(f"    Entries found: {len(feed.entries)}")
            if feed.entries:
                print(f"    First entry: {feed.entries[0].get('title', 'No title')[:60]}...")
    except ImportError:
        print("  feedparser not installed, skipping RSS test")
    
    print("\n" + "=" * 50)
    print("Test complete.")
