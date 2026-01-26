"""
RSS Feed Monitor for Deal Flow Agent

Fetches carve-out/divestiture news from RSS feeds:
- Direct PE/M&A news sources (PE Hub, PR Newswire, Business Wire)
- Premium journalism via Google News (FT, WSJ, Bloomberg, Reuters)
- Sell-side signals (strategic review, adviser appointments)
- PE buyer activity (firms circling, bidding)
- UK/Europe coverage

Provides deduplication via DedupManager integration.
"""

import feedparser
from datetime import datetime
from typing import Optional
from dedup import DedupManager


# ==========================================================================
# RSS FEED CONFIGURATION
# ==========================================================================

# Extended to when:2d for 48-hour lookback with overlap detection
RSS_FEEDS = [
    # ========== DIRECT PE/M&A NEWS SOURCES ==========
    "https://www.pehub.com/feed/",
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",
    
    # ========== PREMIUM JOURNALISM (via Google News) ==========
    "https://news.google.com/rss/search?q=site:ft.com+%22private+equity%22+acquisition+OR+divestiture+OR+spin-off+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:wsj.com+spin-off+OR+divestiture+OR+carve-out+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:bloomberg.com+carve-out+OR+divestiture+OR+spin-off+%22private+equity%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:reuters.com+divestiture+OR+spin-off+%22private+equity%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    
    # ========== SELL-SIDE SIGNALS ==========
    "https://news.google.com/rss/search?q=corporate+spin-off+OR+divestiture+OR+%22strategic+review%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=company+%22exploring+sale%22+OR+%22weighing+sale%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22strategic+alternatives%22+division+OR+unit+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22hired+Goldman%22+OR+%22hired+Morgan+Stanley%22+OR+%22hired+JPMorgan%22+sale+OR+divestiture+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22hiring+advisers%22+OR+%22appointed+advisers%22+sale+OR+strategic+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22non-core%22+sale+OR+divestiture+OR+%22portfolio+review%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22activist+investor%22+spin-off+OR+divestiture+OR+%22break+up%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    
    # ========== PE BUYER ACTIVITY ==========
    "https://news.google.com/rss/search?q=%22private+equity%22+%22in+talks%22+OR+%22circling%22+OR+%22bidding%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22carve-out%22+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=KKR+OR+Blackstone+OR+Carlyle+OR+Apollo+%22acquisition%22+OR+%22buy%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=EQT+OR+CVC+OR+TPG+OR+%22Bain+Capital%22+%22acquisition%22+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Thoma+Bravo%22+OR+%22Vista+Equity%22+OR+%22Silver+Lake%22+acquisition+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22H.I.G.%22+OR+%22HIG+Capital%22+OR+%22KPS+Capital%22+OR+Aurelius+acquisition+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22KPS+Capital%22+OR+%22One+Rock%22+OR+%22American+Industrial+Partners%22+OR+%22Atlas+Holdings%22+acquisition+OR+carve-out+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Platinum+Equity%22+OR+%22OpenGate+Capital%22+OR+%22Sterling+Group%22+acquisition+OR+carve-out+when:2d&hl=en-US&gl=US&ceid=US:en",
    
    # ========== UK/EUROPE ==========
    "https://news.google.com/rss/search?q=divestiture+OR+spin-off+UK+OR+Europe+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=%22private+equity%22+acquisition+UK+OR+Europe+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=Cinven+OR+Permira+OR+%22BC+Partners%22+OR+%22PAI+Partners%22+acquisition+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=Inflexion+OR+%22Triton+Partners%22+OR+%22Nordic+Capital%22+acquisition+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
]


# ==========================================================================
# DATE PARSING
# ==========================================================================

def parse_published_date(date_str: str) -> Optional[datetime]:
    """Parse various date formats from RSS feeds"""
    if not date_str:
        return None
    
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d %b %Y",
        "%B %d, %Y",
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


def is_within_48h(date_str: str) -> bool:
    """Check if published date is within last 48 hours"""
    if not date_str:
        return True  # Assume recent if no date
    
    pub_date = parse_published_date(date_str)
    if not pub_date:
        return True
    
    now = datetime.now()
    age = now - pub_date
    
    return age.total_seconds() < 48 * 3600


# ==========================================================================
# MAIN FETCH FUNCTION
# ==========================================================================

def fetch_rss_articles(dedup: DedupManager) -> list:
    """
    Fetch articles from all RSS feeds with deduplication.
    
    Args:
        dedup: DedupManager instance for URL/content deduplication
        
    Returns:
        List of article dicts with keys: title, link, summary, published, source
    """
    articles = []
    skipped_old = 0
    skipped_dedup = 0
    
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:20]:
                link = entry.get("link", "")
                
                # Check URL dedup first (fast)
                if dedup.is_url_duplicate(link):
                    skipped_dedup += 1
                    continue
                
                published = entry.get("published", "")
                
                # HARD FILTER: Skip articles older than 48h
                if not is_within_48h(published):
                    skipped_old += 1
                    continue
                
                title = entry.get("title", "")
                summary = entry.get("summary", entry.get("description", ""))
                
                # Content dedup (catches syndicated articles)
                if dedup.is_content_duplicate(title, summary):
                    skipped_dedup += 1
                    continue
                
                article = {
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "published": published,
                    "source": feed.feed.get("title", feed_url),
                }
                
                # Mark as seen
                dedup.mark_processed(article)
                articles.append(article)
                
        except Exception as e:
            print(f"Warning: Failed to fetch {feed_url}: {e}")
    
    print(f"Fetched {len(articles)} articles from {len(RSS_FEEDS)} feeds")
    print(f"  Skipped: {skipped_old} old, {skipped_dedup} duplicates")
    return articles


def format_for_claude_analysis(articles: list) -> list:
    """
    Format RSS articles for Claude analysis.
    RSS articles are already in the correct format.
    
    This function exists for API consistency with other monitors.
    """
    return articles


# ==========================================================================
# STATS
# ==========================================================================

def get_feed_count() -> int:
    """Return number of configured RSS feeds"""
    return len(RSS_FEEDS)


# ==========================================================================
# TESTS
# ==========================================================================

if __name__ == "__main__":
    print("RSS Monitor - Test Run")
    print("=" * 50)
    print(f"Configured feeds: {len(RSS_FEEDS)}")
    
    # Test without dedup (standalone)
    class MockDedup:
        def is_url_duplicate(self, url): return False
        def is_content_duplicate(self, title, summary): return False
        def mark_processed(self, article): pass
    
    articles = fetch_rss_articles(MockDedup())
    
    print(f"\nSample articles:")
    for article in articles[:5]:
        print(f"  - {article['title'][:60]}...")
        print(f"    Source: {article['source']}")
    
    print("\n" + "=" * 50)
    print("Test complete.")
