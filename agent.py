"""
Deal Flow Agent v3.11 - Daily Carve-Out/Spin-Off Intelligence
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
from target_accounts import TARGET_PE_FIRMS, passes_tier2_filter, FILTERED_SIGNAL_TYPES

# Configuration
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# Signal keywords that indicate potential carve-outs/spin-offs
SIGNAL_KEYWORDS = [
    # Sell-side signals
    "strategic review",
    "strategic alternatives",
    "exploring options",
    "evaluating alternatives",
    "spin-off",
    "spinoff",
    "spin off",  # Critical: two words, no hyphen
    "carve-out",
    "carve out",
    "divestiture",
    "divest",
    "divesting",
    "non-core assets",
    "non-core business",
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
    "sells division",
    "sells unit",
    "sells business",
    "selling its",
    "to sell its",
    "offload",
    "offloading",
    "shed",
    "shedding",
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
    "acquires division",
    "acquires unit",
    "acquires business",
    "acquiring",
    "to acquire",
    "snaps up",
    "scoops up",
    "take private",
    "going private",
    "buys division",
    "buys unit",
    "buys business",
    # Business unit indicators (combined with other signals)
    "business unit",
    "division sale",
    "unit sale",
    "segment sale",
    # Gemini research: Pre-deal rumor language (Tier 1)
    "potential suitors",
    "drum up interest",
    "preliminary talks",
    "gauge buyer interest",
    "gauge potential buyer interest",
    "early stages",
    "financial sponsor interest",
    "exploring its options",
    "potential buyers",
    "deliberations remain",
    "suitors circling",
    "auction block",
    "bidding process",
    "non-binding offer",
    "competing to acquire",
    "final bidders",
    "teaser letters",
    "received a buyout offer",
    "buyout offer",
    "open to selling",
    "takeover speculation",
    "buyer interest",
    "indication of interest",
    "exclusive talks",
    "advanced negotiations",
    "preferred bidder",
    "leading the race",
    "late-stage talks",
    "lead suitor",
    # Gemini research: Completed deal language (Tier 4)
    "completes sale",
    "completes acquisition",
    "completes divestiture",
    "definitive agreement",
    "signs definitive agreement",
    "transfer of shares",
    "transfer of subsidiary",
    "ownership position",
    "agreement to divest",
    "completes carve-out",
]

# Build PE firm name patterns for matching
# PE firm patterns built from target accounts
PE_FIRM_PATTERNS = [firm.lower() for firm in TARGET_PE_FIRMS]

# News sources to monitor
# NOTE: when:14d added for 2-week backfill to test dedup - REVERT to 24h after validation
RSS_FEEDS = [
    # ========== DIRECT PE/M&A NEWS SOURCES ==========
    # PE Hub - Direct PE deal announcements (highest signal quality)
    "https://www.pehub.com/feed/",
    # PR Newswire M&A
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    # Business Wire
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",
    
    # ========== PREMIUM JOURNALISM (via Google News) ==========
    # FT - Strong on UK/Europe deals, often breaks PE news first
    "https://news.google.com/rss/search?q=site:ft.com+%22private+equity%22+acquisition+OR+divestiture+OR+spin-off+when:14d&hl=en-US&gl=US&ceid=US:en",
    # WSJ - Strong on US deals
    "https://news.google.com/rss/search?q=site:wsj.com+spin-off+OR+divestiture+OR+carve-out+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Bloomberg - Often first to report deal rumors
    "https://news.google.com/rss/search?q=site:bloomberg.com+carve-out+OR+divestiture+OR+spin-off+%22private+equity%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Reuters - Global deal coverage
    "https://news.google.com/rss/search?q=site:reuters.com+divestiture+OR+spin-off+%22private+equity%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    
    # ========== SELL-SIDE SIGNALS ==========
    # Strategic review / exploring options
    "https://news.google.com/rss/search?q=corporate+spin-off+OR+divestiture+OR+%22strategic+review%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=company+%22exploring+sale%22+OR+%22weighing+sale%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22strategic+alternatives%22+division+OR+unit+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Adviser appointments (strong signal - sale process starting)
    "https://news.google.com/rss/search?q=%22hired+Goldman%22+OR+%22hired+Morgan+Stanley%22+OR+%22hired+JPMorgan%22+sale+OR+divestiture+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22hiring+advisers%22+OR+%22appointed+advisers%22+sale+OR+strategic+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Non-core asset sales
    "https://news.google.com/rss/search?q=%22non-core%22+sale+OR+divestiture+OR+%22portfolio+review%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Activist pressure (often triggers spin-offs)
    "https://news.google.com/rss/search?q=%22activist+investor%22+spin-off+OR+divestiture+OR+%22break+up%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    
    # ========== PE BUYER ACTIVITY ==========
    # General PE activity
    "https://news.google.com/rss/search?q=%22private+equity%22+%22in+talks%22+OR+%22circling%22+OR+%22bidding%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22carve-out%22+private+equity+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Major PE firms - US
    "https://news.google.com/rss/search?q=KKR+OR+Blackstone+OR+Carlyle+OR+Apollo+%22acquisition%22+OR+%22buy%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=EQT+OR+CVC+OR+TPG+OR+%22Bain+Capital%22+%22acquisition%22+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Thoma+Bravo%22+OR+%22Vista+Equity%22+OR+%22Silver+Lake%22+acquisition+when:14d&hl=en-US&gl=US&ceid=US:en",
    # Tier 1 industrial carve-out specialists
    "https://news.google.com/rss/search?q=%22H.I.G.%22+OR+%22HIG+Capital%22+OR+%22KPS+Capital%22+OR+Aurelius+acquisition+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22KPS+Capital%22+OR+%22One+Rock%22+OR+%22American+Industrial+Partners%22+OR+%22Atlas+Holdings%22+acquisition+OR+carve-out+when:14d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=%22Platinum+Equity%22+OR+%22OpenGate+Capital%22+OR+%22Sterling+Group%22+acquisition+OR+carve-out+when:14d&hl=en-US&gl=US&ceid=US:en",
    
    # ========== UK/EUROPE ==========
    "https://news.google.com/rss/search?q=divestiture+OR+spin-off+UK+OR+Europe+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=%22private+equity%22+acquisition+UK+OR+Europe+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=Cinven+OR+Permira+OR+%22BC+Partners%22+OR+%22PAI+Partners%22+acquisition+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=Inflexion+OR+%22Triton+Partners%22+OR+%22Nordic+Capital%22+acquisition+when:14d&hl=en-GB&gl=GB&ceid=GB:en",
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
    """Check if title is a duplicate using normalized comparison with word-level division matching"""
    normalized_new = normalize_title(new_title)
    company_key = extract_company_key(new_title)
    
    for existing in existing_titles:
        # Exact normalized match
        if normalize_title(existing) == normalized_new:
            return True
        # Company + division match (handles "LKQ" vs "LKQ Corporation")
        if extract_company_key(existing) == company_key:
            # Extract divisions and check for word overlap
            new_div = new_title.split(' - ')[-1].lower() if ' - ' in new_title else ''
            existing_div = existing.split(' - ')[-1].lower() if ' - ' in existing else ''
            
            # Get significant words (>3 chars, excluding company name)
            company_words = set(company_key.lower().split())
            new_words = set(w for w in re.sub(r'[^a-z0-9 ]', '', new_div).split() if len(w) > 3 and w not in company_words)
            existing_words = set(w for w in re.sub(r'[^a-z0-9 ]', '', existing_div).split() if len(w) > 3 and w not in company_words)
            
            # If any significant word overlaps, it's a duplicate
            # e.g., "Freight Unit" and "FedEx Freight" both have "freight"
            if new_words and existing_words and new_words & existing_words:
                return True
            
            # Also check substring match for backwards compatibility
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
    "signal_type": "Strategic Review" | "Adviser Appointed" | "PE Interest" | "PE In Talks" | "PE Bid Submitted" | "Definitive Agreement" | "Deal Completed",
    "pe_buyer": "Name of PE firm involved (if any, otherwise null)",
    "likely_buyers": "Comma-separated list of PE firms likely to be interested based on their sector focus and deal size (e.g. 'KPS Capital, Platinum Equity, One Rock'). Use your knowledge of PE firm strategies. Null if unknown.",
    "size_estimate": "Revenue or deal value if mentioned (e.g. '$500M revenue' or '$1.2B EV')",
    "ev_low": "Low end of estimated enterprise value in millions as integer (e.g. 500 for $500M). Null if cannot estimate.",
    "ev_high": "High end of estimated enterprise value in millions as integer (e.g. 800 for $800M). Null if cannot estimate.",
    "sector": "TMT" | "Financial Services" | "Healthcare" | "Consumer" | "Industrials" | "Retail" | "Technology" | "Other",
    "geography": "US" | "UK" | "Europe" (primary geography of the TARGET ASSET, not the parent),
    "complexity": "Low" | "Medium" | "High" | "Very High" (based on: cross-border operations, IT/ERP entanglement, manufacturing footprint, TSA likely duration),
    "key_quote": "Most important sentence signaling the deal",
    "buyer_intelligence": "Brief note on why this might interest PE buyers and what the value creation angle could be",
    "notes": "Any other relevant context from the article",
    "confidence": "high" | "medium" | "low"
}}

If NO, return:
{{
    "is_relevant": false,
    "reason": "Brief reason"
}}

SIGNAL TYPE GUIDANCE:
- "Strategic Review" = Company evaluating options or actively exploring sale
- "Adviser Appointed" = Investment bank hired to run process
- "PE Interest" = PE firms reported as interested or circling the asset
- "PE In Talks" = Active negotiations between parties
- "PE Bid Submitted" = Formal offer made
- "Definitive Agreement" = Deal signed, awaiting regulatory approval or close
- "Deal Completed" = Transaction has closed

INCLUDE:
- Corporate divisions being sold or spun off
- Business units being divested
- PE firms acquiring divisions from corporates
- Strategic reviews of specific business units

EXCLUDE (return is_relevant: false):
- Whole company sales without PE involvement
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
    """Apply post-extraction filters to reject low-quality entries.
    
    SIGNAL TYPE LOGIC:
    
    Late-stage (Definitive Agreement, Deal Completed):
        - MUST have named PE buyer on target list
        - MUST have valid geography
        - No PE buyer = strategic buyer = filter out
    
    Early-stage (Strategic Review, Adviser Appointed, PE Interest, PE Bid Submitted, PE In Talks):
        - PE buyer NOT required (process may be pre-buyer selection)
        - For PE Interest/PE Bid Submitted: must have likely_buyers OR pe_buyer
        - Geography filter still applies
        - Sector/academic/govt filters still apply
    """
    signal = analysis.get("signal_type", "")
    pe_buyer = analysis.get("pe_buyer", "")
    likely_buyers = analysis.get("likely_buyers", "")
    geo = analysis.get("geography", "")
    
    # Handle list geography
    if isinstance(geo, list):
        geo = geo[0] if geo else ""
    
    # ==========================================================
    # LATE-STAGE SIGNALS: Must have target PE buyer
    # ==========================================================
    if signal in FILTERED_SIGNAL_TYPES:  # Definitive Agreement, Deal Completed
        passes, reason = passes_tier2_filter(signal, pe_buyer, geo)
        return passes, reason
    
    # ==========================================================
    # EARLY-STAGE SIGNALS: More permissive filtering
    # ==========================================================
    early_stage_signals = {"Strategic Review", "Adviser Appointed", "PE Interest", 
                          "PE Bid Submitted", "PE In Talks"}
    
    geo_lower = geo.lower() if geo else ""
    
    # Geography filter - must be US, UK or Europe (applies to all signals)
    invalid_geos = ["china", "asia", "apac", "latam", "latin america", "middle east", 
                    "africa", "australia", "india", "japan", "korea", "brazil", 
                    "mexico", "central america", "south america"]
    for invalid in invalid_geos:
        if invalid in geo_lower:
            return False, f"Geography filter: {geo}"
    
    # Only allow US, UK, Europe, Global
    if geo and geo not in ["US", "UK", "Europe", "Global", "US, UK", "UK, US", "Europe, UK", "UK, Europe"]:
        geo_parts = [g.strip() for g in geo.split(',')]
        valid_geos = {"US", "UK", "Europe", "Global"}
        if not all(g in valid_geos for g in geo_parts):
            return False, f"Geography filter: {geo}"
    
    # Sector filter - reject irrelevant sectors (applies to all signals)
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
    
    # ==========================================================
    # EARLY-STAGE SPECIFIC LOGIC
    # ==========================================================
    if signal in early_stage_signals:
        # PE Interest and PE Bid Submitted: must have EITHER pe_buyer OR likely_buyers
        # (bidders are often disclosed in likely_buyers field before winner selected)
        if signal in {"PE Interest", "PE Bid Submitted"}:
            if not pe_buyer and not likely_buyers:
                return False, f"{signal} without any PE firms identified"
        
        # Strategic Review and Adviser Appointed: allow without PE buyer
        # These signals occur BEFORE buyers are involved
        # No additional filter - geography and sector already checked
        
        # PE In Talks: allow - negotiations underway, buyer may or may not be named
        
        return True, f"Early-stage signal: {signal}"
    
    # ==========================================================
    # FALLBACK: Unknown signal types get standard filtering
    # ==========================================================
    division = analysis.get("division", "").lower()
    
    whole_company_indicators = ["whole company", "entire company", "full company", 
                                "not specified", "company-wide", "whole business"]
    is_whole_company = any(ind in division for ind in whole_company_indicators)
    
    if is_whole_company and not pe_buyer:
        return False, "Whole company sale without PE buyer"
    
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
    valid_signal_types = ["Strategic Review", "Adviser Appointed", "PE Interest", "PE In Talks", "PE Bid Submitted", "Definitive Agreement", "Deal Completed"]
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
    
    # Add Research Status - always Raw for new entries
    properties["Research Status"] = {"select": {"name": "Raw"}}
    
    # Add Source from RSS feed
    source_name = safe_str(article.get("source"), "")[:100]
    if source_name:
        properties["Source"] = {"rich_text": [{"text": {"content": source_name}}]}
    
    # Add Likely Buyers if present
    likely_buyers = safe_str(analysis.get("likely_buyers"), "")[:500]
    if likely_buyers:
        properties["Likely Buyers"] = {"rich_text": [{"text": {"content": likely_buyers}}]}
    
    # Add EV estimates if present
    ev_low = analysis.get("ev_low")
    ev_high = analysis.get("ev_high")
    if ev_low and isinstance(ev_low, (int, float)):
        properties["Est EV Low"] = {"number": ev_low}
    if ev_high and isinstance(ev_high, (int, float)):
        properties["Est EV High"] = {"number": ev_high}
    
    # Add Complexity if present
    complexity = safe_str(analysis.get("complexity"), "")[:100]
    if complexity:
        properties["Complexity"] = {"rich_text": [{"text": {"content": complexity}}]}
    
    # Add Buyer Intelligence if present
    buyer_intel = safe_str(analysis.get("buyer_intelligence"), "")[:2000]
    if buyer_intel:
        properties["Buyer Intelligence"] = {"rich_text": [{"text": {"content": buyer_intel}}]}
    
    # Add Notes if present
    notes = safe_str(analysis.get("notes"), "")[:2000]
    if notes:
        properties["Notes"] = {"rich_text": [{"text": {"content": notes}}]}
    
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


def run_agent(include_pe_firms: bool = True, include_sec: bool = True, include_bank_mandates: bool = True):
    """Main agent execution
    
    Args:
        include_pe_firms: Scrape PE firm press releases (P2.1)
        include_sec: Monitor SEC filings (P2.2)
        include_bank_mandates: Monitor bank mandate announcements (P3)
    """
    print(f"\n{'='*60}")
    print(f"Deal Flow Agent v4.0 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    print(f"Sources: RSS{'+ PE Firms' if include_pe_firms else ''}{'+ SEC' if include_sec else ''}{'+ Bank Mandates' if include_bank_mandates else ''}")
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
    print(f"Found {len(existing_titles)} existing entries")
    
    # Safety check: if we get 0 entries, verify this is expected
    if len(existing_titles) == 0:
        print("WARNING: No existing entries found. This could mean:")
        print("  - Database is genuinely empty (first run)")
        print("  - Database fetch failed silently")
        print("  - API rate limit or network issue")
        user_input = input("Continue anyway? [y/N]: ").strip().lower()
        if user_input != 'y':
            print("Aborting to prevent duplicate creation.")
            return
    print()
    
    # ==========================================================
    # SOURCE 1: RSS Feeds (existing)
    # ==========================================================
    print("=" * 40)
    print("SOURCE 1: RSS Feeds")
    print("=" * 40)
    articles = fetch_rss_articles()
    print(f"  Found {len(articles)} total articles")
    
    # Pre-filter for signal keywords
    relevant_articles = filter_relevant_articles(articles)
    print(f"  {len(relevant_articles)} articles contain signal keywords\n")
    
    # ==========================================================
    # SOURCE 2: PE Firm Press Releases (P2.1)
    # ==========================================================
    pe_firm_articles = []
    if include_pe_firms:
        print("=" * 40)
        print("SOURCE 2: PE Firm Press Releases")
        print("=" * 40)
        try:
            from pe_firm_monitor import fetch_pe_firm_signals, format_for_claude_analysis as format_pe
            pe_signals = fetch_pe_firm_signals()
            pe_firm_articles = format_pe(pe_signals)
            print(f"  {len(pe_firm_articles)} PE firm articles to analyze\n")
        except ImportError as e:
            print(f"  Warning: PE firm monitor not available: {e}\n")
        except Exception as e:
            print(f"  Warning: PE firm monitor error: {e}\n")
    
    # ==========================================================
    # SOURCE 3: SEC Filings (P2.2)
    # ==========================================================
    sec_articles = []
    if include_sec:
        print("=" * 40)
        print("SOURCE 3: SEC Filings")
        print("=" * 40)
        try:
            from sec_monitor import fetch_all_sec_signals, format_for_claude_analysis as format_sec
            sec_signals = fetch_all_sec_signals(days_back=14)
            sec_articles = format_sec(sec_signals)
            print(f"  {len(sec_articles)} SEC filings to analyze\n")
        except ImportError as e:
            print(f"  Warning: SEC monitor not available: {e}\n")
        except Exception as e:
            print(f"  Warning: SEC monitor error: {e}\n")
    
    # ==========================================================
    # SOURCE 4: Bank Mandate Announcements (P3)
    # ==========================================================
    bank_articles = []
    if include_bank_mandates:
        print("=" * 40)
        print("SOURCE 4: Bank Mandate Announcements")
        print("=" * 40)
        try:
            from bank_mandate_monitor import fetch_bank_mandate_signals, format_for_claude_analysis as format_bank
            bank_signals = fetch_bank_mandate_signals()
            bank_articles = format_bank(bank_signals)
            print(f"  {len(bank_articles)} bank mandate articles to analyze\n")
        except ImportError as e:
            print(f"  Warning: Bank mandate monitor not available: {e}\n")
        except Exception as e:
            print(f"  Warning: Bank mandate monitor error: {e}\n")
    
    # ==========================================================
    # COMBINE ALL SOURCES
    # ==========================================================
    all_articles = relevant_articles + pe_firm_articles + sec_articles + bank_articles
    
    print("=" * 40)
    print(f"TOTAL: {len(all_articles)} articles to analyze")
    print(f"  - RSS: {len(relevant_articles)}")
    print(f"  - PE Firms: {len(pe_firm_articles)}")
    print(f"  - SEC: {len(sec_articles)}")
    print(f"  - Bank Mandates: {len(bank_articles)}")
    print("=" * 40 + "\n")
    
    # Analyze each article with Claude
    new_entries = 0
    skipped_duplicates = 0
    skipped_filtered = 0
    
    for article in all_articles:
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
                # Add NORMALIZED title to maintain consistency with DB fetch
                existing_titles.add(normalize_title(full_title))
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  New entries added: {new_entries}")
    print(f"  Duplicates skipped: {skipped_duplicates}")
    print(f"  Filtered out: {skipped_filtered}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Deal Flow Agent v4.0")
    parser.add_argument("--no-pe-firms", action="store_true", help="Skip PE firm press releases")
    parser.add_argument("--no-sec", action="store_true", help="Skip SEC filings")
    parser.add_argument("--no-bank-mandates", action="store_true", help="Skip bank mandate announcements")
    parser.add_argument("--rss-only", action="store_true", help="Only use RSS feeds (original behavior)")
    
    args = parser.parse_args()
    
    if args.rss_only:
        run_agent(include_pe_firms=False, include_sec=False, include_bank_mandates=False)
    else:
        run_agent(
            include_pe_firms=not args.no_pe_firms,
            include_sec=not args.no_sec,
            include_bank_mandates=not args.no_bank_mandates
        )


