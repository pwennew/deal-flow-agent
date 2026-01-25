"""
Investment Bank Mandate Monitor
Monitors press releases from top M&A advisers for "appointed as financial adviser" announcements.
Captures Adviser Appointed signals - earliest reliable public signal.

Target banks:
- Bulge bracket: Goldman Sachs, Morgan Stanley, JPMorgan
- Elite boutiques: Lazard, Evercore, Centerview, Moelis, PJT Partners
- Tech specialists: Qatalyst Partners
- European: Rothschild
"""

import re
import requests
from datetime import datetime, timedelta
from typing import Optional
from bs4 import BeautifulSoup
import random
import time

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

HEADERS = get_headers()  # For backward compatibility

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
        "notes": "Centerview rarely posts press releases - may need alternative source",
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
    
    # Tech specialists
    "Qatalyst Partners": {
        "url": "https://www.qatalyst.com/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
        "notes": "Qatalyst rarely posts press releases - may need alternative source",
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

# Google News RSS for bank mandate announcements (primary source - more reliable than bank websites)
# Using when:1d for 24-hour lookback (aligned to daily scan cycle)
BANK_NEWS_RSS = [
    # Wire service searches (most reliable for mandate announcements)
    'https://news.google.com/rss/search?q=("appointed"+OR+"retained"+OR+"engaged")+"financial+adviser"+(divestiture+OR+"strategic+review"+OR+sale+OR+"spin-off")+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="financial+advisor"+appointed+(divestiture+OR+carve-out+OR+"strategic+alternatives")+when:1d&hl=en-US&gl=US&ceid=US:en',
    
    # Bank-specific searches
    'https://news.google.com/rss/search?q="Goldman+Sachs"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Morgan+Stanley"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Lazard"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Evercore"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Centerview"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Moelis"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="PJT+Partners"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Rothschild"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="JPMorgan"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    
    # PR Newswire / Business Wire direct searches
    'https://news.google.com/rss/search?q=site:prnewswire.com+"financial+adviser"+appointed+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:businesswire.com+"financial+adviser"+appointed+when:1d&hl=en-US&gl=US&ceid=US:en',
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
    info = {
        "adviser": bank_name,
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
    
    # Extended bank name patterns
    bank_patterns = {
        "goldman sachs": "Goldman Sachs",
        "goldman": "Goldman Sachs",
        "morgan stanley": "Morgan Stanley",
        "jpmorgan": "JPMorgan",
        "jp morgan": "JPMorgan",
        "lazard": "Lazard",
        "evercore": "Evercore",
        "centerview": "Centerview Partners",
        "moelis": "Moelis & Company",
        "pjt partners": "PJT Partners",
        "pjt": "PJT Partners",
        "qatalyst": "Qatalyst Partners",
        "rothschild": "Rothschild & Co",
        "barclays": "Barclays",
        "citi": "Citi",
        "citigroup": "Citi",
        "bofa": "Bank of America",
        "bank of america": "Bank of America",
        "ubs": "UBS",
        "credit suisse": "Credit Suisse",
        "deutsche bank": "Deutsche Bank",
        "hsbc": "HSBC",
        "nomura": "Nomura",
        "jefferies": "Jefferies",
        "guggenheim": "Guggenheim",
        "perella weinberg": "Perella Weinberg",
        "houlihan lokey": "Houlihan Lokey",
        "william blair": "William Blair",
        "raymond james": "Raymond James",
        "piper sandler": "Piper Sandler",
        "dc advisory": "DC Advisory",
        "lincoln international": "Lincoln International",
        "baird": "Baird",
        "stifel": "Stifel",
        "ernst & young": "EY",
        "ey ": "EY",  # space to avoid matching "they"
        "deloitte": "Deloitte",
        "kpmg": "KPMG",
        "pwc": "PwC",
    }
    
    signals = []
    seen_urls = set()
    
    print(f"\nFetching mandate signals from {len(BANK_NEWS_RSS)} RSS feeds...")
    
    for feed_url in BANK_NEWS_RSS:
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:15]:
                link = entry.get("link", "")
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                
                title = entry.get("title", "")
                
                # Check if it's a mandate announcement
                if not is_mandate_announcement(title):
                    continue
                
                # Extract bank name from title using extended patterns
                title_lower = title.lower()
                bank_name = "Unknown Adviser"
                for pattern, name in bank_patterns.items():
                    if pattern in title_lower:
                        bank_name = name
                        break
                
                signal = {
                    "adviser": bank_name,
                    "raw_title": title,
                    "signal_type": "Adviser Appointed",
                    "source": "News (Bank Mandate)",
                    "link": link,
                    "date": entry.get("published", ""),
                    "is_carveout": is_carveout_related(title),
                }
                signals.append(signal)
                
        except Exception as e:
            print(f"  Warning: Failed to fetch RSS feed: {e}")
    
    print(f"  Found {len(signals)} mandate signals from RSS")
    
    return signals


def fetch_bank_mandate_signals(banks: dict = None) -> list:
    """
    Fetch adviser appointment signals from multiple sources.
    Primary: RSS feeds (wire services, news)
    Secondary: Bank press release pages (often blocked)
    
    Args:
        banks: Dict of banks to scrape (defaults to BANK_PRESS_PAGES)
    
    Returns:
        List of mandate signal dicts
    """
    if banks is None:
        banks = BANK_PRESS_PAGES
    
    all_signals = []
    
    # PRIMARY SOURCE: RSS feeds (more reliable than bank websites)
    rss_signals = fetch_rss_mandates()
    all_signals.extend(rss_signals)
    
    # SECONDARY SOURCE: Bank press release pages (often blocked but worth trying)
    print(f"\nScraping {len(banks)} investment bank press release pages...")
    
    for i, (bank_name, config) in enumerate(banks.items()):
        print(f"  Checking {bank_name}...")
        
        # Add delay between requests
        if i > 0:
            time.sleep(1.5)
        
        try:
            signals = scrape_bank(bank_name, config)
            
            if signals:
                print(f"    Found {len(signals)} mandate announcements")
                all_signals.extend(signals)
            
        except Exception as e:
            print(f"    Error scraping {bank_name}: {e}")
    
    print(f"\nTotal bank mandate signals: {len(all_signals)}")
    
    # Deduplicate by title similarity
    seen_titles = set()
    unique_signals = []
    for sig in all_signals:
        title_key = sig.get('raw_title', '').lower()[:50]
        if title_key not in seen_titles:
            seen_titles.add(title_key)
            unique_signals.append(sig)
    
    print(f"After dedup: {len(unique_signals)} unique signals")
    
    return unique_signals


def format_for_claude_analysis(signals: list) -> list:
    """
    Format bank mandate signals for Claude analysis.
    Returns list of article-like dicts compatible with agent.py
    """
    articles = []
    
    for sig in signals:
        article = {
            "title": sig.get("raw_title", ""),
            "link": sig.get("link", ""),
            "summary": f"Investment bank mandate: {sig.get('adviser', 'Unknown')} appointed. {sig.get('raw_title', '')}",
            "published": sig.get("date", ""),
            "source": sig.get("source", "Bank Press Release"),
            # Pre-populated hints for Claude
            "_signal_type_hint": "Adviser Appointed",
            "_adviser_hint": sig.get("adviser"),
            "_is_carveout_hint": sig.get("is_carveout", False),
        }
        articles.append(article)
    
    return articles


if __name__ == "__main__":
    # Test run
    print("Investment Bank Mandate Monitor - Test Run")
    print("=" * 50)
    
    signals = fetch_bank_mandate_signals()
    
    print("\n" + "=" * 50)
    print("Sample signals:")
    for sig in signals[:5]:
        print(f"\n{sig.get('adviser')}: {sig.get('raw_title', '')[:80]}")
        print(f"  Carve-out related: {sig.get('is_carveout')}")
