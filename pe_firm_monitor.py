"""
PE Firm Press Release Monitor
Scrapes press release pages of target PE firms for deal announcements.
Focuses on Tier 1 industrial carve-out specialists.

Captures: Deal Completed, Definitive Agreement signals from source
"""

import re
import requests
from datetime import datetime, timedelta
from typing import Optional
from bs4 import BeautifulSoup

# Tier 1 PE firms with known press release page structures
# Format: firm_name -> {url, selector config}
PE_FIRM_PRESS_PAGES = {
    # ==========================================================
    # TIER 1: Industrial carve-out specialists (highest priority)
    # ==========================================================
    "KPS Capital Partners": {
        "url": "https://www.kpsfund.com/news/",
        "type": "html",
        "article_selector": "article, .news-item, .press-release",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time, .post-date",
        "link_selector": "a",
    },
    "AURELIUS Group": {
        "url": "https://aurelius-group.com/en/news/",
        "type": "html",
        "article_selector": ".news-item, article, .post",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Platinum Equity": {
        "url": "https://www.platinumequity.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "One Rock Capital Partners": {
        "url": "https://onerockcapital.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "American Industrial Partners": {
        "url": "https://www.americanindustrial.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Atlas Holdings": {
        "url": "https://www.atlasholdingsllc.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "SK Capital Partners": {
        "url": "https://www.skcapitalpartners.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "OpenGate Capital": {
        "url": "https://opengatecapital.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Sterling Group": {
        "url": "https://www.sterling-group.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    
    # ==========================================================
    # TIER 1: Large-cap with carve-out activity
    # ==========================================================
    "H.I.G. Capital": {
        "url": "https://higgrowth.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Carlyle Group": {
        "url": "https://www.carlyle.com/media-room/news-release-archive",
        "type": "html",
        "article_selector": ".news-item, .views-row, article",
        "title_selector": "h2, h3, .title",
        "date_selector": ".date, time, .field-date",
        "link_selector": "a",
    },
    "Clayton Dubilier & Rice": {
        "url": "https://www.cdr-inc.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Advent International": {
        "url": "https://www.adventinternational.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Bain Capital": {
        "url": "https://www.baincapital.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "KKR": {
        "url": "https://www.kkr.com/news",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Apollo Global Management": {
        "url": "https://www.apollo.com/media/press-releases",
        "type": "html",
        "article_selector": ".news-item, article, .press-release",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Blackstone": {
        "url": "https://www.blackstone.com/news/press/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    
    # ==========================================================
    # TIER 1: European carve-out specialists
    # ==========================================================
    "Triton Partners": {
        "url": "https://www.triton-partners.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Cinven": {
        "url": "https://www.cinven.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "CVC Capital Partners": {
        "url": "https://www.cvc.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "PAI Partners": {
        "url": "https://www.paipartners.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Inflexion": {
        "url": "https://www.inflexion.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "EQT": {
        "url": "https://eqtgroup.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
    "Montagu Private Equity": {
        "url": "https://www.montagu.com/news/",
        "type": "html",
        "article_selector": ".news-item, article",
        "title_selector": "h2, h3",
        "date_selector": ".date, time",
        "link_selector": "a",
    },
}

# Keywords indicating deal announcements (vs general news)
DEAL_KEYWORDS = [
    "acquires", "acquisition", "acquired",
    "completes", "completed", "completion",
    "closes", "closed", "closing",
    "signs", "signed", "signing",
    "definitive agreement",
    "purchase", "purchased",
    "buys", "bought",
    "carve-out", "carve out",
    "divestiture",
    "spin-off", "spinoff",
    "agreement to acquire",
    "portfolio company",
    "announces investment",
    "announces acquisition",
]

# Keywords indicating carve-out specifically (vs platform acquisition)
CARVEOUT_INDICATORS = [
    "carve-out", "carve out", "carveout",
    "divestiture", "divest",
    "spin-off", "spinoff", "spin off",
    "division", "unit", "segment",
    "business unit",
    "subsidiary",
    "from", "sells", "sold by",
]

# User agent rotation to avoid bot detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

import random
import time

def get_headers():
    """Get randomized headers to avoid bot detection"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def fetch_page(url: str, timeout: int = 20, retries: int = 2) -> Optional[str]:
    """Fetch HTML content from URL with retry logic"""
    for attempt in range(retries + 1):
        try:
            # Add delay between requests to avoid rate limiting
            if attempt > 0:
                time.sleep(2 * attempt)
            
            response = requests.get(url, headers=get_headers(), timeout=timeout, allow_redirects=True)
            
            if response.status_code == 200:
                return response.text
            elif response.status_code in [429, 503] and attempt < retries:
                print(f"    Rate limited, retrying in {2 * (attempt + 1)}s...")
                continue
            else:
                print(f"  Warning: {url} returned {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            if attempt < retries:
                print(f"    Timeout, retrying...")
                continue
            print(f"  Warning: Timeout fetching {url}")
            return None
        except Exception as e:
            print(f"  Warning: Failed to fetch {url}: {e}")
            return None
    return None


def extract_articles_generic(html: str, config: dict) -> list:
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
            term in str(x).lower() for term in ['news', 'press', 'release', 'post', 'item']
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
            # Try link text
            link = elem.find('a')
            if link:
                article['title'] = link.get_text(strip=True)
        
        # Extract link
        link_elem = elem.select_one(config.get("link_selector", "a"))
        if link_elem and link_elem.get('href'):
            href = link_elem['href']
            # Handle relative URLs
            if href.startswith('/'):
                # Extract base URL from config
                base_url = config.get('url', '')
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


def is_deal_announcement(title: str) -> bool:
    """Check if article title indicates a deal announcement"""
    title_lower = title.lower()
    return any(kw in title_lower for kw in DEAL_KEYWORDS)


def is_carveout_related(title: str) -> bool:
    """Check if deal appears to be carve-out related"""
    title_lower = title.lower()
    return any(kw in title_lower for kw in CARVEOUT_INDICATORS)


def extract_deal_info(title: str, pe_firm: str) -> dict:
    """Extract basic deal information from title"""
    info = {
        "pe_buyer": pe_firm,
        "raw_title": title,
        "signal_type": "Deal Completed",  # Default, may be overridden
        "source": f"{pe_firm} Press Release",
    }
    
    title_lower = title.lower()
    
    # Determine signal type
    if any(kw in title_lower for kw in ["completes", "completed", "closes", "closed", "finalizes"]):
        info["signal_type"] = "Deal Completed"
    elif any(kw in title_lower for kw in ["signs", "signed", "definitive agreement", "agrees to"]):
        info["signal_type"] = "Definitive Agreement"
    elif any(kw in title_lower for kw in ["announces", "to acquire", "acquisition of"]):
        info["signal_type"] = "Definitive Agreement"
    
    # Check if carve-out
    info["is_carveout"] = is_carveout_related(title)
    
    return info


def scrape_pe_firm(firm_name: str, config: dict) -> list:
    """Scrape press releases from a single PE firm"""
    signals = []
    
    url = config.get("url")
    if not url:
        return signals
    
    html = fetch_page(url)
    if not html:
        return signals
    
    articles = extract_articles_generic(html, config)
    
    for article in articles:
        title = article.get('title', '')
        if not title:
            continue
        
        # Filter for deal announcements
        if not is_deal_announcement(title):
            continue
        
        # Extract deal info
        deal_info = extract_deal_info(title, firm_name)
        deal_info['link'] = article.get('link', url)
        deal_info['date'] = article.get('date', '')
        
        signals.append(deal_info)
    
    return signals


def fetch_pe_firm_signals(firms: dict = None, days_back: int = 30) -> list:
    """
    Fetch deal signals from PE firm press release pages.
    
    Args:
        firms: Dict of PE firms to scrape (defaults to PE_FIRM_PRESS_PAGES)
        days_back: Only include signals from last N days (if date parseable)
    
    Returns:
        List of deal signal dicts
    """
    if firms is None:
        firms = PE_FIRM_PRESS_PAGES
    
    all_signals = []
    
    print(f"\nScraping {len(firms)} PE firm press release pages...")
    
    for i, (firm_name, config) in enumerate(firms.items()):
        print(f"  Checking {firm_name}...")
        
        # Add delay between requests (except first)
        if i > 0:
            time.sleep(1.5)
        
        try:
            signals = scrape_pe_firm(firm_name, config)
            
            if signals:
                print(f"    Found {len(signals)} deal announcements")
                all_signals.extend(signals)
            
        except Exception as e:
            print(f"    Error scraping {firm_name}: {e}")
    
    print(f"\nTotal PE firm signals: {len(all_signals)}")
    
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
    Format PE firm signals for Claude analysis.
    Returns list of article-like dicts compatible with agent.py
    """
    articles = []
    
    for sig in signals:
        article = {
            "title": sig.get("raw_title", ""),
            "link": sig.get("link", ""),
            "summary": f"{sig.get('pe_buyer', '')} deal announcement: {sig.get('raw_title', '')}",
            "published": sig.get("date", ""),
            "source": sig.get("source", "PE Firm Press Release"),
            # Pre-populated hints for Claude
            "_pe_buyer_hint": sig.get("pe_buyer"),
            "_signal_type_hint": sig.get("signal_type"),
            "_is_carveout_hint": sig.get("is_carveout", False),
        }
        articles.append(article)
    
    return articles


if __name__ == "__main__":
    # Test run
    print("PE Firm Press Release Monitor - Test Run")
    print("=" * 50)
    
    signals = fetch_pe_firm_signals()
    
    print("\n" + "=" * 50)
    print("Sample signals:")
    for sig in signals[:5]:
        print(f"\n{sig.get('pe_buyer')}: {sig.get('raw_title', '')[:80]}")
        print(f"  Type: {sig.get('signal_type')}, Carve-out: {sig.get('is_carveout')}")
