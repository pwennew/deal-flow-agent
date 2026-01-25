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

# ==========================================================
# ALL TARGET PE FIRMS (135 total)
# ==========================================================
ALL_TARGET_PE_FIRMS = [
    "ABRY Partners", "ACON Investments", "Actis", "Advent International",
    "Affinity Equity Partners", "Altaris Capital Partners", "American Industrial Partners",
    "American Securities", "Antin Infrastructure Partners", "Apax Partners",
    "Apollo Global Management", "Aquiline Capital Partners", "Arcline Investment Management",
    "Arcmont Asset Management", "Ardian", "Arsenal Capital Partners", "Astorg",
    "AURELIUS Group", "Bain Capital", "BC Partners", "Berkshire Partners", "BGH Capital",
    "Blackstone", "Blue Wolf Capital Partners", "Bridgepoint", "Bruckmann Rosser Sherrill",
    "Carlyle Group", "Charterhouse Capital Partners", "Cinven", "Clayton Dubilier & Rice",
    "Clearlake Capital Group", "Court Square", "CVC Capital Partners", "EagleTree Capital",
    "EQT", "Eurazeo", "Flexpoint Ford", "Forbion", "Francisco Partners", "FSN Capital",
    "General Atlantic", "Genstar Capital", "GI Partners", "Golden Gate Capital",
    "Great Hill Partners", "Gryphon Investors", "GTCR", "H.I.G. Capital", "Harvest Partners",
    "HayFin Capital Management", "Hellman & Friedman", "HGGC", "Housatonic Partners", "Hg",
    "ICG", "IK Partners", "Incline Equity Partners", "Inflexion", "Insight Partners",
    "Intermediate Capital Group", "JMI Equity", "K1 Investment Management", "Kelso & Company",
    "Kohlberg & Company", "KKR", "KPS Capital Partners", "L Catterton", "Lee Equity Partners",
    "Leonard Green & Partners", "Levine Leichtman Capital Partners", "Livingbridge",
    "LLR Partners", "Madison Dearborn Partners", "Main Post Partners", "MBK Partners",
    "Montagu Private Equity", "New Mountain Capital", "NewQuest Capital Partners",
    "Nordic Capital", "Norvestor", "Oak Hill Capital Partners", "Odyssey Investment Partners",
    "One Rock Capital Partners", "Onex", "Owl Rock Capital", "PAI Partners",
    "Pamplona Capital Management", "Parthenon Capital", "Peak Rock Capital", "Permira Advisers",
    "Platinum Equity", "Providence Equity Partners", "Quadrant Private Equity",
    "RBC Capital Partners", "Resurgens Technology Partners", "Reverence Capital Partners",
    "Rhône Group", "Ridgemont Equity Partners", "Rivean Capital", "Riverside Company",
    "Roark Capital Group", "SDC Capital Partners", "Silver Lake", "SK Capital Partners",
    "Snow Phipps Group", "Sole Source Capital", "Solis Capital Partners", "Spectrum Equity",
    "Stone Point Capital", "Summit Partners", "Sun Capital Partners", "Sycamore Partners",
    "TA Associates", "TCV", "TDR Capital", "TH Lee", "The Carlyle Group",
    "The Riverside Company", "The Sterling Group", "Thoma Bravo", "Thomas H. Lee Partners",
    "TPG", "Trilantic Capital Partners", "Triton Partners", "Ufenau Capital Partners",
    "Veritas Capital", "Victory Park Capital", "Vista Equity Partners", "Vitruvian Partners",
    "Warburg Pincus", "Water Street Healthcare Partners", "Webster Equity Partners",
    "Welsh Carson Anderson & Stowe", "WindRose Health Investors", "Wynnchurch Capital",
    # Additional carve-out specialists not in main list
    "Atlas Holdings", "OpenGate Capital", "Sterling Group", "Stellex Capital",
    "American Securities", "Olympus Partners",
]

def build_pe_firm_rss_url(firm_name: str) -> str:
    """Build Google News RSS URL for a PE firm's deal announcements (24h lookback)"""
    # URL encode the firm name
    import urllib.parse
    firm_encoded = urllib.parse.quote(f'"{firm_name}"')
    
    # Search for acquisitions, completions, closings - 1d lookback
    return f'https://news.google.com/rss/search?q={firm_encoded}+(acquires+OR+completes+OR+closes+OR+acquisition+OR+"has+acquired"+OR+"announces+acquisition")+when:1d&hl=en-US&gl=US&ceid=US:en'


def build_pe_firm_rss_urls_batch(firms: list, batch_size: int = 5) -> list:
    """
    Build RSS URLs that search for multiple firms at once (24h lookback).
    More efficient than individual firm searches.
    """
    import urllib.parse
    
    urls = []
    
    # Batch firms together (Google allows OR queries)
    for i in range(0, len(firms), batch_size):
        batch = firms[i:i + batch_size]
        
        # Build OR query for firm names
        firm_queries = [f'"{firm}"' for firm in batch]
        firms_part = "+OR+".join(urllib.parse.quote(f) for f in firm_queries)
        
        # Deal keywords + 1d lookback
        deal_keywords = "(acquires+OR+completes+OR+closes+OR+acquisition+OR+acquired)+when:1d"
        
        url = f'https://news.google.com/rss/search?q=({firms_part})+{deal_keywords}&hl=en-US&gl=US&ceid=US:en'
        urls.append(url)
    
    return urls


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


def fetch_pe_rss_signals(firms: list = None) -> list:
    """
    Fetch deal signals from Google News RSS for all target PE firms.
    Primary source - more reliable than scraping PE firm websites.
    
    Args:
        firms: List of PE firm names (defaults to ALL_TARGET_PE_FIRMS)
    
    Returns:
        List of deal signal dicts
    """
    import feedparser
    
    if firms is None:
        firms = ALL_TARGET_PE_FIRMS
    
    all_signals = []
    seen_urls = set()
    
    # Build firm name lookup for extraction
    firm_lookup = {}
    for firm in firms:
        # Create variations for matching
        firm_lower = firm.lower()
        firm_lookup[firm_lower] = firm
        # Also add first word for partial matches
        first_word = firm_lower.split()[0]
        if first_word not in firm_lookup:
            firm_lookup[first_word] = firm
    
    print(f"\nFetching RSS signals for {len(firms)} PE firms...")
    
    # Build batch RSS URLs (5 firms per URL for efficiency)
    rss_urls = build_pe_firm_rss_urls_batch(firms, batch_size=5)
    print(f"  Querying {len(rss_urls)} RSS feeds...")
    
    for i, feed_url in enumerate(rss_urls):
        try:
            feed = feedparser.parse(feed_url)
            
            for entry in feed.entries[:20]:  # Limit per feed
                link = entry.get("link", "")
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                
                title = entry.get("title", "")
                if not title:
                    continue
                
                # Check if it's a deal announcement
                if not is_deal_announcement(title):
                    continue
                
                # Extract PE firm name from title
                title_lower = title.lower()
                pe_firm = None
                for pattern, firm_name in firm_lookup.items():
                    if pattern in title_lower:
                        pe_firm = firm_name
                        break
                
                if not pe_firm:
                    continue  # Skip if we can't identify the PE firm
                
                # Extract deal info
                signal = extract_deal_info(title, pe_firm)
                signal['link'] = link
                signal['date'] = entry.get("published", "")
                signal['source'] = "Google News"
                
                all_signals.append(signal)
                
        except Exception as e:
            print(f"    Warning: RSS feed error: {e}")
        
        # Small delay between feeds
        if i > 0 and i % 10 == 0:
            time.sleep(0.5)
    
    print(f"  Found {len(all_signals)} signals from RSS")
    
    return all_signals


def fetch_pe_firm_signals(firms: dict = None, days_back: int = 30, include_rss: bool = True) -> list:
    """
    Fetch deal signals from PE firms.
    Primary: Google News RSS for all 135 target firms
    Secondary: Direct website scraping (for sites that allow it)
    
    Args:
        firms: Dict of PE firms to scrape (defaults to PE_FIRM_PRESS_PAGES)
        days_back: Only include signals from last N days (if date parseable)
        include_rss: Whether to include RSS signals (default True)
    
    Returns:
        List of deal signal dicts
    """
    if firms is None:
        firms = PE_FIRM_PRESS_PAGES
    
    all_signals = []
    
    # ==========================================================
    # PRIMARY: RSS feeds for all 135 target PE firms
    # ==========================================================
    if include_rss:
        rss_signals = fetch_pe_rss_signals()
        all_signals.extend(rss_signals)
    
    # ==========================================================
    # SECONDARY: Direct website scraping (for accessible sites)
    # ==========================================================
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
