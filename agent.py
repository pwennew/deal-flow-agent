"""
Deal Flow Agent v3 - Daily Carve-Out/Spin-Off Intelligence
Scans public sources for:
1. Divestiture signals (companies selling divisions)
2. PE buyer activity (firms circling targets)
Writes results to Notion with improved deduplication
"""

import os
import re
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional
import feedparser
import requests
from notion_client import Client
from anthropic import Anthropic
from pe_firms import PE_FIRMS

# Configuration
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# Signal keywords that indicate potential carve-outs/spin-offs
SIGNAL_KEYWORDS = [
    "strategic review",
    "strategic alternatives",
    "exploring options",
    "evaluating alternatives",
    "spin-off",
    "spinoff",
    "carve-out",
    "carve out",
    "divestiture",
    "divest",
    "non-core assets",
    "portfolio optimization",
    "separation",
    "standalone basis",
    "sale process",
    "formal sale process",
    "exploring sale",
    "weighing sale",
    "considering sale",
    "hired advisers",
    "working with advisers",
    "gauging interest",
    "potential suitors",
    "private equity interest",
    # UK-specific terminology
    "disposal",
    "hive off",
    "demerger",
    # PE buyer activity signals
    "in talks to acquire",
    "in talks to buy",
    "circling",
    "among bidders",
    "exploring acquisition",
    "said to be interested",
    "weighing bid",
    "considering bid",
    "eyeing",
    "approached",
    "makes offer",
    "submits bid",
    "enters race",
    "joins bidding",
]

# Build PE firm name patterns for matching
PE_FIRM_PATTERNS = [firm.lower() for firm in PE_FIRMS]

# News sources to monitor
# NOTE: when:7d added for one-time backfill - REVERT after manual run
RSS_FEEDS = [
    # Google News searches for key terms - Sell-side signals
    "https://news.google.com/rss/search?q=corporate+spin-off+OR+divestiture+OR+%22strategic+review%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=company+%22exploring+sale%22+OR+%22weighing+sale%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22carve-out%22+private+equity+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22strategic+alternatives%22+division+OR+unit+when:7d&hl=en-US&gl=US&ceid=US:en",
    # UK/Europe focused
    "https://news.google.com/rss/search?q=divestiture+OR+spin-off+UK+OR+Europe+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
    # PR Newswire M&A
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    # Business Wire
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",
    # PE buyer activity - major firms
    "https://news.google.com/rss/search?q=%22private+equity%22+%22in+talks%22+OR+%22circling%22+OR+%22bidding%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=KKR+OR+Blackstone+OR+Carlyle+OR+Apollo+%22acquisition%22+OR+%22buy%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=EQT+OR+CVC+OR+TPG+OR+%22Bain+Capital%22+%22acquisition%22+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22H.I.G.%22+OR+%22HIG+Capital%22+OR+%22KPS+Capital%22+OR+Aurelius+acquisition+when:7d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Thoma+Bravo%22+OR+%22Vista+Equity%22+OR+%22Silver+Lake%22+acquisition+when:7d&hl=en-US&gl=US&ceid=US:en",
    # European PE activity
    "https://news.google.com/rss/search?q=%22private+equity%22+acquisition+UK+OR+Europe+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=Cinven+OR+Permira+OR+%22BC+Partners%22+OR+%22PAI+Partners%22+acquisition+when:7d&hl=en-GB&gl=GB&ceid=GB:en",
    # Top industrial carve-out specialists (Tier 1 targets)
    "https://news.google.com/rss/search?q=%22KPS+Capital%22+OR+%22One+Rock%22+OR+%22American+Industrial+Partners%22+OR+%22Atlas+Holdings%22+acquisition+OR+carve-out&hl=en-US&gl=US&ceid=US:en",
]


def get_existing_entries_from_db() -> set:
    """Fetch all existing company titles from Notion database for deduplication"""
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    existing = set()
    has_more = True
    start_cursor = None
    
    while has_more:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        
        try:
            response = requests.post(
                f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                headers=headers,
                json=body,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"Warning: Failed to fetch existing entries: {response.status_code}")
                break
            
            results = response.json()
            for entry in results.get("results", []):
                props = entry.get("properties", {})
                company_prop = props.get("Company", {})
                title_list = company_prop.get("title", [])
                if title_list:
                    title = title_list[0].get("plain_text", "")
                    # Normalize for comparison
                    normalized = normalize_title(title)
                    if normalized:
                        existing.add(normalized)
            
            has_more = results.get("has_more", False)
            start_cursor = results.get("next_cursor")
            
        except Exception as e:
            print(f"Warning: Error fetching existing entries: {e}")
            break
    
    return existing


def normalize_title(title: str) -> str:
    """Normalize title for deduplication comparison"""
    # Lowercase
    t = title.lower()
    # Remove PE firm names in parentheses at end
    t = re.sub(r'\s*\([^)]*\)\s*$', '', t)
    # Normalize whitespace
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def init_notion_database(notion: Client, database_id: str):
    """Verify connection to the Notion database"""
    try:
        result = notion.databases.retrieve(database_id=database_id)
        print(f"✓ Connected to Notion database")
    except Exception as e:
        print(f"Warning: Could not connect to database: {e}")


def fetch_rss_articles() -> list:
    """Fetch articles from all RSS feeds"""
    articles = []
    
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:20]:  # Limit per feed
                article = {
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "summary": entry.get("summary", entry.get("description", "")),
                    "published": entry.get("published", ""),
                    "source": feed.feed.get("title", feed_url),
                }
                articles.append(article)
        except Exception as e:
            print(f"Warning: Failed to fetch {feed_url}: {e}")
    
    return articles


def has_signal_keywords(text: str) -> bool:
    """Check if text contains any signal keywords"""
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in SIGNAL_KEYWORDS)


def filter_relevant_articles(articles: list) -> list:
    """Pre-filter articles that contain signal keywords"""
    relevant = []
    for article in articles:
        combined_text = f"{article['title']} {article['summary']}"
        if has_signal_keywords(combined_text):
            relevant.append(article)
    return relevant


def analyze_article_with_claude(client: Anthropic, article: dict) -> Optional[dict]:
    """Use Claude to extract structured deal information from article"""
    
    prompt = f"""Analyze this news article for potential M&A opportunities relevant to a carve-out/integration consultant.

ARTICLE:
Title: {article['title']}
Summary: {article['summary']}
Source: {article['source']}

TASK:
Determine if this article describes EITHER:
A) A corporate divestiture, spin-off, carve-out, or strategic review (SELL-SIDE)
B) A private equity firm circling, bidding on, or acquiring a target (BUY-SIDE)

If YES, extract the following information in JSON format:
{{
    "is_relevant": true,
    "company": "Target company or parent company name",
    "division": "Division or unit being sold/spun off (if specified, otherwise 'Whole Company')",
    "signal_type": "Strategic Review" | "Exploring Sale" | "Adviser Appointed" | "PE Interest Reported" | "Spin-off Announced" | "Divestiture" | "PE Circling" | "PE In Talks" | "PE Bid Submitted",
    "pe_buyer": "Name of PE firm involved (if any, otherwise null)",
    "size_estimate": "Revenue or deal value if mentioned (e.g., '$500M revenue', '$1.2B EV')",
    "sector": "TMT" | "Financial Services" | "Healthcare" | "Consumer" | "Industrials" | "Retail" | "Technology" | "Other",
    "geography": ["US", "UK", "Europe"] (list all that apply),
    "key_quote": "The most important sentence from the article that signals the deal",
    "confidence": "high" | "medium" | "low"
}}

If NO (article is not relevant), return:
{{
    "is_relevant": false,
    "reason": "Brief reason why not relevant"
}}

SIGNAL TYPE GUIDANCE:
- "Strategic Review" = Company evaluating options, no specific buyer
- "Exploring Sale" = Company actively seeking buyers
- "Adviser Appointed" = Investment bank hired to run process
- "PE Interest Reported" = PE firms reported as interested (general)
- "Spin-off Announced" = Company announced spin-off
- "Divestiture" = Sale process underway
- "PE Circling" = Specific PE firm(s) reported as circling target
- "PE In Talks" = PE firm in active discussions with target
- "PE Bid Submitted" = PE firm has made formal offer

INCLUDE:
- Corporate divisions being sold or spun off
- Companies hiring advisers to explore strategic options
- PE firms circling, bidding on, or acquiring businesses
- Strategic reviews that may lead to divestitures
- Rumored PE interest with named firms

EXCLUDE:
- Completed deals (already closed)
- Venture capital / growth equity investments
- Public company M&A without PE involvement
- Opinion pieces without news
- Very early speculation without substance

Return ONLY valid JSON, no other text."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        # Parse the JSON response
        response_text = response.content[0].text.strip()
        # Clean up potential markdown code blocks
        if response_text.startswith("```"):
            response_text = re.sub(r'^```json?\n?', '', response_text)
            response_text = re.sub(r'\n?```$', '', response_text)
        
        result = json.loads(response_text)
        
        if result.get("is_relevant") and result.get("confidence") in ["high", "medium"]:
            return result
        return None
        
    except Exception as e:
        print(f"Warning: Claude analysis failed: {e}")
        return None


def safe_str(value, default=""):
    """Safely convert value to string, handling None"""
    if value is None:
        return default
    return str(value)


def create_notion_entry(database_id: str, article: dict, analysis: dict):
    """Create a new entry in the Notion database using requests"""
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Build geography multi-select
    geography = analysis.get("geography", [])
    if isinstance(geography, str):
        geography = [geography]
    geo_options = [{"name": g} for g in geography if g in ["US", "UK", "Europe", "Global"]]
    if not geo_options:
        geo_options = [{"name": "US"}]  # Default
    
    # Safely extract values with defaults
    company = safe_str(analysis.get("company"), "Unknown")[:100]
    division = safe_str(analysis.get("division"), "Not specified")[:100]
    signal_type = safe_str(analysis.get("signal_type"), "Strategic Review")
    pe_buyer = safe_str(analysis.get("pe_buyer"), "")[:100]
    size_estimate = safe_str(analysis.get("size_estimate"), "Not disclosed")[:100]
    sector = safe_str(analysis.get("sector"), "Other")
    key_quote = safe_str(analysis.get("key_quote"), "")[:2000]
    source_url = safe_str(article.get("link"), "")
    
    # Validate signal type against allowed values
    valid_signal_types = ["Strategic Review", "Exploring Sale", "Adviser Appointed", "PE Interest Reported", "Spin-off Announced", "Divestiture", "PE Circling", "PE In Talks", "PE Bid Submitted"]
    if signal_type not in valid_signal_types:
        signal_type = "Strategic Review"
    
    # Validate sector against allowed values
    valid_sectors = ["TMT", "Financial Services", "Healthcare", "Consumer", "Industrials", "Retail", "Technology", "Other"]
    if sector not in valid_sectors:
        sector = "Other"
    
    # Build title - include PE buyer if present
    if pe_buyer:
        title = f"{company} - {division} ({pe_buyer})"
    else:
        title = f"{company} - {division}"
    
    # Build the page properties
    properties = {
        "Company": {
            "title": [{"text": {"content": title[:100]}}]
        },
        "Division": {
            "rich_text": [{"text": {"content": division}}]
        },
        "Signal Type": {
            "select": {"name": signal_type}
        },
        "Sector": {
            "select": {"name": sector}
        },
        "Geography": {
            "multi_select": geo_options
        },
        "Status": {
            "select": {"name": "New"}
        },
    }
    
    # Add PE Buyer if present
    if pe_buyer:
        properties["PE Buyer"] = {"rich_text": [{"text": {"content": pe_buyer}}]}
    
    # Add Link if URL is valid
    if source_url.startswith("http"):
        properties["Link"] = {"url": source_url}
    
    try:
        response = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json={
                "parent": {"database_id": database_id},
                "properties": properties
            },
            timeout=30
        )
        
        if response.status_code == 200:
            if pe_buyer:
                print(f"✓ Added: {company} - {division} (PE: {pe_buyer})")
            else:
                print(f"✓ Added: {company} - {division}")
            return True
        else:
            print(f"✗ Failed to add entry: {response.status_code} - {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"✗ Failed to add entry: {e}")
        return False


def run_agent():
    """Main agent execution"""
    print(f"\n{'='*60}")
    print(f"Deal Flow Agent v3 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")
    
    # Validate configuration
    if not all([NOTION_API_KEY, NOTION_DATABASE_ID, ANTHROPIC_API_KEY]):
        print("ERROR: Missing required environment variables:")
        print(f"  NOTION_API_KEY: {'✓' if NOTION_API_KEY else '✗'}")
        print(f"  NOTION_DATABASE_ID: {'✓' if NOTION_DATABASE_ID else '✗'}")
        print(f"  ANTHROPIC_API_KEY: {'✓' if ANTHROPIC_API_KEY else '✗'}")
        return
    
    # Initialize Claude client
    anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # CRITICAL: Fetch existing entries from database FIRST
    print("Fetching existing entries from Notion...")
    existing_titles = get_existing_entries_from_db()
    print(f"Found {len(existing_titles)} existing entries\n")
    
    # Fetch articles from RSS feeds
    print("Fetching articles from RSS feeds...")
    articles = fetch_rss_articles()
    print(f"  Found {len(articles)} total articles")
    
    # Pre-filter for signal keywords
    relevant_articles = filter_relevant_articles(articles)
    print(f"  {len(relevant_articles)} articles contain signal keywords\n")
    
    # Analyze each relevant article with Claude
    new_entries = 0
    skipped_duplicates = 0
    
    for article in relevant_articles:
        print(f"Analyzing: {article['title'][:70]}...")
        analysis = analyze_article_with_claude(anthropic, article)
        
        if analysis:
            # Create title for deduplication check
            company = safe_str(analysis.get("company"), "Unknown")
            division = safe_str(analysis.get("division"), "Division")
            pe_buyer = safe_str(analysis.get("pe_buyer"), "")
            
            if pe_buyer:
                full_title = f"{company} - {division} ({pe_buyer})"
            else:
                full_title = f"{company} - {division}"
            
            # Normalize for comparison (strip PE buyer suffix)
            normalized = normalize_title(full_title)
            
            # Skip if we've already got this one
            if normalized in existing_titles:
                skipped_duplicates += 1
                print(f"  Skipped (duplicate): {company}")
                continue
            
            success = create_notion_entry(NOTION_DATABASE_ID, article, analysis)
            if success:
                new_entries += 1
                existing_titles.add(normalized)
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  New entries added: {new_entries}")
    print(f"  Duplicates skipped: {skipped_duplicates}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_agent()
