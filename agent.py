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
# NOTE: when:14d added for 2-week backfill to test dedup - REVERT to 24h after validation
RSS_FEEDS = [
    # Google News searches for key terms - Sell-side signals
    "https://news.google.com/rss/search?q=corporate+spin-off+OR+divestiture+OR+%22strategic+review%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=company+%22exploring+sale%22+OR+%22weighing+sale%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22carve-out%22+private+equity+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22strategic+alternatives%22+division+OR+unit+when:14d&hl=en-US&gl=US&ceid=US:en",
    # UK/Europe focused
    "https://news.google.com/rss/search?q=divestiture+OR+spin-off+UK+OR+Europe+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
    # PR Newswire M&A
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    # Business Wire
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",
    # PE buyer activity - major firms
    "https://news.google.com/rss/search?q=%22private+equity%22+%22in+talks%22+OR+%22circling%22+OR+%22bidding%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=KKR+OR+Blackstone+OR+Carlyle+OR+Apollo+%22acquisition%22+OR+%22buy%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=EQT+OR+CVC+OR+TPG+OR+%22Bain+Capital%22+%22acquisition%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22H.I.G.%22+OR+%22HIG+Capital%22+OR+%22KPS+Capital%22+OR+Aurelius+acquisition+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Thoma+Bravo%22+OR+%22Vista+Equity%22+OR+%22Silver+Lake%22+acquisition+when:14d&hl=en-US&gl=US&ceid=US:en",
    # European PE activity
    "https://news.google.com/rss/search?q=%22private+equity%22+acquisition+UK+OR+Europe+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=Cinven+OR+Permira+OR+%22BC+Partners%22+OR+%22PAI+Partners%22+acquisition+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
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
    # Remove corporate suffixes
    t = re.sub(r'\b(corporation|corp|inc|incorporated|ltd|limited|plc|llc|llp|co|company|group|holdings)\b\.?', '', t)
    # Remove "the " at start
    t = re.sub(r'^the\s+', '', t)
    # Normalize whitespace and punctuation
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def extract_company_key(title: str) -> str:
    """Extract just the company name for fuzzy matching"""
    # Split on " - " to get company part
    parts = title.split(' - ')
    if parts:
        return normalize_title(parts[0])
    return normalize_title(title)


def is_duplicate(new_title: str, existing_titles: set) -> bool:
    """Check if title is a duplicate using normalized comparison"""
    normalized_new = normalize_title(new_title)
    company_key = extract_company_key(new_title)
    
    for existing in existing_titles:
        # Exact normalized match
        if normalize_title(existing) == normalized_new:
            return True
        # Company + division match (handles "LKQ" vs "LKQ Corporation")
        if extract_company_key(existing) == company_key:
            # Check if divisions also match
            new_div = new_title.split(' - ')[-1].lower() if ' - ' in new_title else ''
            existing_div = existing.split(' - ')[-1].lower() if ' - ' in existing else ''
            if new_div and existing_div and (new_div in existing_div or existing_div in new_div):
                return True
    return False


def init_notion_database(notion: Client, database_id: str):
    """Verify connection to the Notion database"""
    try:
        result = notion.databases.retrieve(database_id=database_id)
        print(f"✓ Connected to Notion database")
    except Exception as e:
        print(f"Warning: Could not connect to database: {e}")


def fetch_rss_articles() -> list:
    """Fetch articles from all RSS feeds with URL deduplication"""
    articles = []
    seen_urls = set()
    
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:20]:  # Limit per feed
                link = entry.get("link", "")
                # Skip if we've already seen this URL
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                
                article = {
                    "title": entry.get("title", ""),
                    "link": link,
                    "summary": entry.get("summary", entry.get("description", "")),
                    "published": entry.get("published", ""),
                    "source": feed.feed.get("title", feed_url),
                }
                articles.append(article)
        except Exception as e:
            print(f"Warning: Failed to fetch {feed_url}: {e}")
    
    print(f"Fetched {len(articles)} unique articles from {len(RSS_FEEDS)} feeds")
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
    
    prompt = f"""Analyze this news article for potential M&A opportunities relevant to a carve-out/integration consultant targeting PE buyers in the US, UK and Europe.

ARTICLE:
Title: {article['title']}
Summary: {article['summary']}
Source: {article['source']}

TASK:
Determine if this article describes a GENUINE CARVE-OUT OPPORTUNITY:
A) A corporate division, business unit, or subsidiary being sold or spun off (SELL-SIDE)
B) A private equity firm circling, bidding on, or acquiring a specific division/business (BUY-SIDE)

CRITICAL: A carve-out is the separation of a DIVISION or UNIT from a larger company. It is NOT:
- A whole company being sold (unless PE is acquiring to then carve out parts)
- An IPO or public offering
- A minority stake sale
- A merger of equals

If YES (genuine carve-out opportunity), extract:
{{
    "is_relevant": true,
    "company": "Parent company name",
    "division": "Division or unit being carved out",
    "signal_type": "Strategic Review" | "Exploring Sale" | "Adviser Appointed" | "PE Interest Reported" | "Spin-off Announced" | "Divestiture" | "PE Circling" | "PE In Talks" | "PE Bid Submitted",
    "pe_buyer": "Name of PE firm involved (if any, otherwise null)",
    "size_estimate": "Revenue or deal value if mentioned",
    "sector": "TMT" | "Financial Services" | "Healthcare" | "Consumer" | "Industrials" | "Retail" | "Technology" | "Other",
    "geography": "US" | "UK" | "Europe" (primary geography of the TARGET ASSET, not the parent),
    "key_quote": "Most important sentence signaling the deal",
    "confidence": "high" | "medium" | "low"
}}

If NO, return:
{{
    "is_relevant": false,
    "reason": "Brief reason"
}}

SIGNAL TYPE GUIDANCE:
- "Strategic Review" = Company evaluating options
- "Exploring Sale" = Company actively seeking buyers
- "Adviser Appointed" = Investment bank hired
- "PE Interest Reported" = PE firms reported as interested
- "Spin-off Announced" = Formal spin-off announcement
- "Divestiture" = Sale process underway
- "PE Circling" = Specific PE firm(s) circling
- "PE In Talks" = Active discussions
- "PE Bid Submitted" = Formal offer made

INCLUDE:
- Corporate divisions being sold or spun off
- Business units being divested
- PE firms acquiring divisions from corporates
- Strategic reviews of specific business units

EXCLUDE (return is_relevant: false):
- Whole company sales without PE involvement
- Completed deals (already closed)
- Venture capital / growth equity investments
- Public M&A without PE involvement
- Academic/university spin-offs
- Government/public sector divestitures
- Geographies outside US/UK/Europe (China, LatAm, Middle East, Asia, Africa, Australia)
- Real estate transactions
- Very early speculation without substance
- IPOs or public offerings
- Minority stake sales

GEOGRAPHY RULE: Only include if the TARGET ASSET (the thing being sold) is primarily in US, UK or Europe. Reject China, LatAm, APAC, Middle East, Africa.

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


def passes_post_filters(analysis: dict) -> tuple[bool, str]:
    """Apply post-extraction filters to reject low-quality entries"""
    
    # Geography filter - must be US, UK or Europe
    geo = analysis.get("geography", "")
    if isinstance(geo, list):
        geo = geo[0] if geo else ""
    geo_lower = geo.lower()
    
    invalid_geos = ["china", "asia", "apac", "latam", "latin america", "middle east", 
                    "africa", "australia", "india", "japan", "korea", "brazil", 
                    "mexico", "central america", "south america"]
    for invalid in invalid_geos:
        if invalid in geo_lower:
            return False, f"Geography filter: {geo}"
    
    # Only allow US, UK, Europe, Global
    if geo and geo not in ["US", "UK", "Europe", "Global", "US, UK", "UK, US", "Europe, UK", "UK, Europe"]:
        # Check for comma-separated valid geos
        geo_parts = [g.strip() for g in geo.split(',')]
        valid_geos = {"US", "UK", "Europe", "Global"}
        if not all(g in valid_geos for g in geo_parts):
            return False, f"Geography filter: {geo}"
    
    # Division filter - reject "Whole Company" without PE buyer
    division = analysis.get("division", "").lower()
    pe_buyer = analysis.get("pe_buyer")
    
    whole_company_indicators = ["whole company", "entire company", "full company", 
                                "not specified", "company-wide", "whole business"]
    is_whole_company = any(ind in division for ind in whole_company_indicators)
    
    if is_whole_company and not pe_buyer:
        return False, "Whole company sale without PE buyer"
    
    # Sector filter - reject irrelevant sectors
    sector = analysis.get("sector", "")
    company = analysis.get("company", "").lower()
    
    # Academic/university spin-offs
    academic_indicators = ["university", "college", "institute", "research center", 
                          "academic", "professor", "laboratory"]
    if any(ind in company for ind in academic_indicators):
        return False, "Academic/university spin-off"
    
    # Government/public sector
    govt_indicators = ["government", "ministry", "department of", "federal", 
                       "state of", "county", "municipal", "public sector"]
    if any(ind in company for ind in govt_indicators):
        return False, "Government/public sector"
    
    # Signal type filter - reject weak signals
    signal = analysis.get("signal_type", "")
    if signal == "Strategic Review" and not pe_buyer:
        # Strategic review without PE interest is too early
        confidence = analysis.get("confidence", "")
        if confidence != "high":
            return False, "Strategic review without PE interest (low confidence)"
    
    return True, ""


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
        # Handle comma-separated like "US, UK"
        geography = [g.strip() for g in geography.split(',')]
    valid_geos = {"US", "UK", "Europe", "Global"}
    geo_options = [{"name": g} for g in geography if g in valid_geos]
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
    
    # Add Size Estimate if present and not default
    if size_estimate and size_estimate != "Not disclosed":
        properties["Size Estimate"] = {"rich_text": [{"text": {"content": size_estimate}}]}
    
    # Add Key Quote if present
    if key_quote:
        properties["Key Quote"] = {"rich_text": [{"text": {"content": key_quote}}]}
    
    # Add Date Spotted
    properties["Date Spotted"] = {"date": {"start": datetime.now().strftime("%Y-%m-%d")}}
    
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
    skipped_filtered = 0
    
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
            
            # Deduplication check using improved function
            if is_duplicate(full_title, existing_titles):
                skipped_duplicates += 1
                print(f"  ⊘ Skipped (duplicate): {company} - {division}")
                continue
            
            # Post-extraction filter check
            passes, reason = passes_post_filters(analysis)
            if not passes:
                skipped_filtered += 1
                print(f"  ⊘ Filtered: {company} - {division} ({reason})")
                continue
            
            success = create_notion_entry(NOTION_DATABASE_ID, article, analysis)
            if success:
                new_entries += 1
                existing_titles.add(full_title)
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  New entries added: {new_entries}")
    print(f"  Duplicates skipped: {skipped_duplicates}")
    print(f"  Filtered out: {skipped_filtered}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_agent()
