"""
Deal Flow Agent v5.0 - Daily Carve-Out/Spin-Off Intelligence

Improvements over v4.0:
- Two-stage classification (regex pre-filter, then Claude)
- Multi-layer deduplication (URL, content similarity, deal hash)
- Parallel Claude API calls (5x speedup)
- Extended lookback (48h with overlap detection)
- Company grouping to reduce duplicate analysis
- Response caching to avoid re-analysis on restart
- Run state persistence for resume capability

Scans public sources for:
1. Divestiture signals (companies selling divisions)
2. PE buyer activity (firms circling targets)

Writes results to Notion with improved deduplication.
"""

import os
import re
import json
import hashlib
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from notion_client import Client
from anthropic import Anthropic
from target_accounts import TARGET_PE_FIRMS, passes_tier2_filter, FILTERED_SIGNAL_TYPES

# Import modular components
from dedup import (
    DedupManager,
    compute_deal_hash,
    compute_url_hash,
    extract_company_from_title,
    get_content_signature,
    content_similarity,
)
from classifier import (
    classify_article,
    classify_batch,
    get_classification_stats,
)
from cache import (
    CacheManager,
    get_cache_key,
    get_cache_paths,
)
from rss_monitor import (
    fetch_rss_articles,
    get_feed_count,
)

# Configuration
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# Parallelization settings
MAX_CONCURRENT_CLAUDE_CALLS = 5
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# State file for caching and persistence
STATE_FILE = os.environ.get("STATE_FILE", "/tmp/deal_flow_state.json")
CACHE_TTL_HOURS = 48  # How long to cache Claude responses


class RunState:
    """
    Manages run state for caching and crash recovery.
    
    Stores:
    - Processed URL hashes (cross-run dedup)
    - Claude response cache (avoid re-analysis)
    - Run metadata (for debugging)
    """
    
    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = state_file
        self.state = {
            "processed_urls": {},  # url_hash -> timestamp
            "claude_cache": {},    # url_hash -> {response, timestamp}
            "last_run": None,
            "runs_count": 0,
        }
        self.load()
    
    def load(self):
        """Load state from file if exists"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    loaded = json.load(f)
                    self.state.update(loaded)
                    self._cleanup_old_entries()
        except Exception as e:
            print(f"Warning: Could not load state file: {e}")
    
    def save(self):
        """Save state to file"""
        try:
            self.state["last_run"] = datetime.now().isoformat()
            self.state["runs_count"] = self.state.get("runs_count", 0) + 1
            
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save state file: {e}")
    
    def _cleanup_old_entries(self):
        """Remove entries older than CACHE_TTL_HOURS"""
        cutoff = datetime.now() - timedelta(hours=CACHE_TTL_HOURS)
        cutoff_str = cutoff.isoformat()
        
        # Clean processed URLs
        self.state["processed_urls"] = {
            k: v for k, v in self.state.get("processed_urls", {}).items()
            if v > cutoff_str
        }
        
        # Clean Claude cache
        self.state["claude_cache"] = {
            k: v for k, v in self.state.get("claude_cache", {}).items()
            if v.get("timestamp", "") > cutoff_str
        }
    
    def is_url_processed(self, url_hash: str) -> bool:
        """Check if URL was processed in recent runs"""
        return url_hash in self.state.get("processed_urls", {})
    
    def mark_url_processed(self, url_hash: str):
        """Mark URL as processed"""
        self.state.setdefault("processed_urls", {})[url_hash] = datetime.now().isoformat()
    
    def get_cached_response(self, url_hash: str) -> Optional[dict]:
        """Get cached Claude response if exists and not expired"""
        cache = self.state.get("claude_cache", {}).get(url_hash)
        if cache:
            return cache.get("response")
        return None
    
    def cache_response(self, url_hash: str, response: dict):
        """Cache Claude response"""
        self.state.setdefault("claude_cache", {})[url_hash] = {
            "response": response,
            "timestamp": datetime.now().isoformat(),
        }
    
    def get_stats(self) -> dict:
        """Get state statistics"""
        return {
            "processed_urls": len(self.state.get("processed_urls", {})),
            "cached_responses": len(self.state.get("claude_cache", {})),
            "runs_count": self.state.get("runs_count", 0),
            "last_run": self.state.get("last_run"),
        }

# Build PE firm name patterns for matching
PE_FIRM_PATTERNS = [firm.lower() for firm in TARGET_PE_FIRMS]



def get_existing_entries_from_db() -> tuple[set, list]:
    """
    Fetch existing entries from Notion for deduplication.
    
    Returns:
        - Set of normalized titles (for backward compat)
        - List of dicts with deal_hash and url_hash (for new dedup)
    """
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    existing_titles = set()
    existing_entries = []
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
                
                # Extract title (backward compat)
                company_prop = props.get("Company", {})
                title_list = company_prop.get("title", [])
                if title_list:
                    title = title_list[0].get("plain_text", "")
                    normalized = normalize_title(title)
                    if normalized:
                        existing_titles.add(normalized)
                
                # Extract new hash fields if present
                entry_data = {}
                
                deal_hash_prop = props.get("Deal Hash", {})
                if deal_hash_prop.get("rich_text"):
                    entry_data['deal_hash'] = deal_hash_prop["rich_text"][0].get("plain_text", "")
                
                url_hash_prop = props.get("Source URL Hash", {})
                if url_hash_prop.get("rich_text"):
                    entry_data['url_hash'] = url_hash_prop["rich_text"][0].get("plain_text", "")
                
                if entry_data:
                    existing_entries.append(entry_data)
            
            has_more = results.get("has_more", False)
            start_cursor = results.get("next_cursor")
            
        except Exception as e:
            print(f"Warning: Error fetching existing entries: {e}")
            break
    
    return existing_titles, existing_entries


def normalize_title(title: str) -> str:
    """Normalize title for deduplication comparison"""
    t = title.lower()
    t = re.sub(r'\s*\([^)]*\)\s*$', '', t)
    t = re.sub(r'\b(corporation|corp|inc|incorporated|ltd|limited|plc|llc|llp|co|company|group|holdings)\b\.?', '', t)
    t = re.sub(r'^the\s+', '', t)
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def extract_company_key(title: str) -> str:
    """Extract just the company name for fuzzy matching"""
    parts = title.split(' - ')
    if parts:
        return normalize_title(parts[0])
    return normalize_title(title)


def analyze_article_with_claude(client: Anthropic, article: dict, run_state: Optional['RunState'] = None) -> Optional[dict]:
    """Use Claude to extract structured deal information from article"""
    
    # Check cache first
    url_hash = compute_url_hash(article.get('link', ''))
    if run_state:
        cached = run_state.get_cached_response(url_hash)
        if cached:
            return cached if cached.get("is_relevant") else None
    
    prompt = f"""Analyze this news article for carve-out opportunities relevant to a separation consultant.

ARTICLE:
Title: {article['title']}
Summary: {article['summary']}
Source: {article['source']}

WHAT IS A CARVE-OUT?
A carve-out is when a DIVISION, UNIT or SEGMENT is separated from its PARENT COMPANY. The key test:
- Does the asset share IT systems, back-office, manufacturing, or operations with the parent?
- Will there be a Transition Services Agreement (TSA) needed post-sale?
- Is there operational "entanglement" that needs to be unwound?

If YES to any of these → it's a carve-out (RELEVANT)
If NO to all → it's just an asset sale (NOT RELEVANT)

EXPLICITLY EXCLUDE (return is_relevant: false):
1. SECONDARY BUYOUTS: PE firm selling portfolio company to another PE firm. These are clean transfers - the company already operates independently.
   Example: "Blackstone acquires X from Morgan Stanley Capital Partners" → NOT a carve-out
   
2. STANDALONE BUSINESS SALES: A company selling an independent subsidiary/business that already operates on its own systems.
   Example: "NatWest sells fintech Cushon" → If Cushon runs independently, NOT a carve-out
   
3. WHOLE COMPANY ACQUISITIONS: PE buying an entire independent company (not a division of something larger).
   Example: "KKR acquires ABC Corp" → NOT a carve-out unless ABC is being carved from a parent

4. IPOs, minority stakes, venture capital, growth equity

5. Geographies outside US/UK/Europe

EXPLICITLY INCLUDE (return is_relevant: true):
1. Corporate parent selling/spinning off a DIVISION that shares infrastructure
   Example: "Siemens carving out industrial motors division" → RELEVANT
   
2. Conglomerate divesting a business unit that uses shared services
   Example: "3M separating healthcare business" → RELEVANT
   
3. Strategic review of a division within a larger corporate
   Example: "Honeywell exploring sale of aerospace segment" → RELEVANT

If RELEVANT, extract:
{{
    "is_relevant": true,
    "company": "Parent company name (the seller)",
    "division": "Division or unit being carved out",
    "signal_type": "Strategic Review" | "Adviser Appointed" | "PE Interest" | "PE In Talks" | "PE Bid Submitted" | "Definitive Agreement" | "Deal Completed",
    "pe_buyer": "Name of PE firm involved (if any, otherwise null)",
    "likely_buyers": "PE firms likely interested based on sector focus (e.g. 'KPS Capital, Platinum Equity'). Null if unknown.",
    "size_estimate": "Revenue or deal value if mentioned",
    "ev_low": "Low EV estimate in millions as integer. Null if unknown.",
    "ev_high": "High EV estimate in millions as integer. Null if unknown.",
    "sector": "TMT" | "Financial Services" | "Healthcare" | "Consumer" | "Industrials" | "Retail" | "Technology" | "Other",
    "geography": "US" | "UK" | "Europe",
    "complexity": "Low" | "Medium" | "High" | "Very High",
    "key_quote": "Key sentence from article",
    "buyer_intelligence": "Why this interests PE buyers",
    "notes": "Other context",
    "confidence": "high" | "medium" | "low"
}}

If NOT RELEVANT, return:
{{
    "is_relevant": false,
    "reason": "Brief reason (e.g. 'Secondary buyout - PE to PE sale' or 'Standalone business, no separation needed')"
}}

Return ONLY valid JSON, no other text."""

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = response.content[0].text.strip()
        if response_text.startswith("```"):
            response_text = re.sub(r'^```json?\n?', '', response_text)
            response_text = re.sub(r'\n?```$', '', response_text)
        
        result = json.loads(response_text)
        
        # Cache the result
        if run_state:
            run_state.cache_response(url_hash, result)
        
        if result.get("is_relevant") and result.get("confidence") in ["high", "medium"]:
            return result
        return None
        
    except Exception as e:
        print(f"Warning: Claude analysis failed: {e}")
        return None


def analyze_articles_parallel(client: Anthropic, articles: list, dedup: DedupManager, run_state: Optional['RunState'] = None) -> list:
    """
    Analyze multiple articles in parallel using ThreadPoolExecutor.
    
    First groups articles by company to reduce duplicate analysis.
    Uses cache to avoid re-analyzing previously seen articles.
    Returns list of (article, analysis) tuples for relevant articles.
    """
    # Group by company first to reduce duplicates
    print(f"  Grouping {len(articles)} articles by company...")
    representatives = dedup.get_representative_articles(articles)
    grouped_count = len(articles) - len(representatives)
    if grouped_count > 0:
        print(f"  → Reduced to {len(representatives)} representatives ({grouped_count} grouped)")
    
    # Check cache for already-analyzed articles
    to_analyze = []
    cached_results = []
    
    if run_state:
        for article in representatives:
            url_hash = compute_url_hash(article.get('link', ''))
            cached = run_state.get_cached_response(url_hash)
            if cached and cached.get("is_relevant"):
                cached_results.append((article, cached))
            else:
                to_analyze.append(article)
        
        if cached_results:
            print(f"  → {len(cached_results)} from cache, {len(to_analyze)} need analysis")
    else:
        to_analyze = representatives
    
    results = cached_results.copy()
    
    if to_analyze:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CLAUDE_CALLS) as executor:
            future_to_article = {
                executor.submit(analyze_article_with_claude, client, article, run_state): article
                for article in to_analyze
            }
            
            for future in as_completed(future_to_article):
                article = future_to_article[future]
                try:
                    analysis = future.result()
                    if analysis:
                        results.append((article, analysis))
                except Exception as e:
                    print(f"Warning: Analysis failed for {article.get('title', '')[:50]}: {e}")
    
    return results


def passes_post_filters(analysis: dict) -> tuple[bool, str]:
    """Apply post-extraction filters to reject low-quality entries."""
    signal = analysis.get("signal_type", "")
    pe_buyer = analysis.get("pe_buyer", "")
    likely_buyers = analysis.get("likely_buyers", "")
    geo = analysis.get("geography", "")
    
    if isinstance(geo, list):
        geo = geo[0] if geo else ""
    
    # Late-stage signals must have target PE buyer
    if signal in FILTERED_SIGNAL_TYPES:
        passes, reason = passes_tier2_filter(signal, pe_buyer, geo)
        return passes, reason
    
    # Early-stage signals: more permissive
    early_stage_signals = {"Strategic Review", "Adviser Appointed", "PE Interest", 
                          "PE Bid Submitted", "PE In Talks"}
    
    geo_lower = geo.lower() if geo else ""
    
    # Geography filter
    invalid_geos = ["china", "asia", "apac", "latam", "latin america", "middle east", 
                    "africa", "australia", "india", "japan", "korea", "brazil", 
                    "mexico", "central america", "south america"]
    for invalid in invalid_geos:
        if invalid in geo_lower:
            return False, f"Geography filter: {geo}"
    
    if geo and geo not in ["US", "UK", "Europe", "Global", "US, UK", "UK, US", "Europe, UK", "UK, Europe"]:
        geo_parts = [g.strip() for g in geo.split(',')]
        valid_geos = {"US", "UK", "Europe", "Global"}
        if not all(g in valid_geos for g in geo_parts):
            return False, f"Geography filter: {geo}"
    
    # Sector filter
    company = analysis.get("company", "").lower()
    
    academic_indicators = ["university", "college", "institute", "research center", 
                          "academic", "professor", "laboratory"]
    if any(ind in company for ind in academic_indicators):
        return False, "Academic/university spin-off"
    
    govt_indicators = ["government", "ministry", "department of", "federal", 
                       "state of", "county", "municipal", "public sector"]
    if any(ind in company for ind in govt_indicators):
        return False, "Government/public sector"
    
    # Early-stage specific logic
    if signal in early_stage_signals:
        if signal in {"PE Interest", "PE Bid Submitted"}:
            if not pe_buyer and not likely_buyers:
                return False, f"{signal} without any PE firms identified"
        return True, f"Early-stage signal: {signal}"
    
    # Fallback
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


def create_notion_entry(database_id: str, article: dict, analysis: dict) -> bool:
    """Create a new entry in the Notion database"""
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Build geography multi-select
    geography = analysis.get("geography", [])
    if isinstance(geography, str):
        geography = [g.strip() for g in geography.split(',')]
    valid_geos = {"US", "UK", "Europe", "Global"}
    geo_options = [{"name": g} for g in geography if g in valid_geos]
    if not geo_options:
        geo_options = [{"name": "US"}]
    
    # Safely extract values
    company = safe_str(analysis.get("company"), "Unknown")[:100]
    division = safe_str(analysis.get("division"), "Not specified")[:100]
    signal_type = safe_str(analysis.get("signal_type"), "Strategic Review")
    pe_buyer = safe_str(analysis.get("pe_buyer"), "")[:100]
    size_estimate = safe_str(analysis.get("size_estimate"), "Not disclosed")[:100]
    sector = safe_str(analysis.get("sector"), "Other")
    key_quote = safe_str(analysis.get("key_quote"), "")[:2000]
    source_url = safe_str(article.get("link"), "")
    
    # Validate against allowed values
    valid_signal_types = ["Strategic Review", "Adviser Appointed", "PE Interest", "PE In Talks", "PE Bid Submitted", "Definitive Agreement", "Deal Completed"]
    if signal_type not in valid_signal_types:
        signal_type = "Strategic Review"
    
    valid_sectors = ["TMT", "Financial Services", "Healthcare", "Consumer", "Industrials", "Retail", "Technology", "Other"]
    if sector not in valid_sectors:
        sector = "Other"
    
    # Build title
    if pe_buyer:
        title = f"{company} - {division} ({pe_buyer})"
    else:
        title = f"{company} - {division}"
    
    # Compute hashes for dedup
    ev_low = analysis.get("ev_low")
    deal_hash = compute_deal_hash(company, division, ev_low)
    url_hash = compute_url_hash(source_url)
    
    # Build properties
    properties = {
        "Company": {"title": [{"text": {"content": title[:100]}}]},
        "Division": {"rich_text": [{"text": {"content": division}}]},
        "Signal Type": {"select": {"name": signal_type}},
        "Sector": {"select": {"name": sector}},
        "Geography": {"multi_select": geo_options},
        "Status": {"select": {"name": "New"}},
        "Date Spotted": {"date": {"start": datetime.now().strftime("%Y-%m-%d")}},
        "Research Status": {"select": {"name": "Raw"}},
    }
    
    # Add optional fields
    if pe_buyer:
        properties["PE Buyer"] = {"rich_text": [{"text": {"content": pe_buyer}}]}
    
    if source_url.startswith("http"):
        properties["Link"] = {"url": source_url}
    
    if size_estimate and size_estimate != "Not disclosed":
        properties["Size Estimate"] = {"rich_text": [{"text": {"content": size_estimate}}]}
    
    if key_quote:
        properties["Key Quote"] = {"rich_text": [{"text": {"content": key_quote}}]}
    
    source_name = safe_str(article.get("source"), "")[:100]
    if source_name:
        properties["Source"] = {"rich_text": [{"text": {"content": source_name}}]}
    
    likely_buyers = safe_str(analysis.get("likely_buyers"), "")[:500]
    if likely_buyers:
        properties["Likely Buyers"] = {"rich_text": [{"text": {"content": likely_buyers}}]}
    
    if ev_low and isinstance(ev_low, (int, float)):
        properties["Est EV Low"] = {"number": ev_low}
    
    ev_high = analysis.get("ev_high")
    if ev_high and isinstance(ev_high, (int, float)):
        properties["Est EV High"] = {"number": ev_high}
    
    complexity = safe_str(analysis.get("complexity"), "")[:100]
    if complexity:
        properties["Complexity"] = {"rich_text": [{"text": {"content": complexity}}]}
    
    buyer_intel = safe_str(analysis.get("buyer_intelligence"), "")[:2000]
    if buyer_intel:
        properties["Buyer Intelligence"] = {"rich_text": [{"text": {"content": buyer_intel}}]}
    
    notes = safe_str(analysis.get("notes"), "")[:2000]
    if notes:
        properties["Notes"] = {"rich_text": [{"text": {"content": notes}}]}
    
    # Add hash fields for future dedup
    properties["Deal Hash"] = {"rich_text": [{"text": {"content": deal_hash}}]}
    properties["Source URL Hash"] = {"rich_text": [{"text": {"content": url_hash}}]}
    
    try:
        response = requests.post(
            "https://api.notion.com/v1/pages",
            headers=headers,
            json={"parent": {"database_id": database_id}, "properties": properties},
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


def run_agent(include_pe_firms: bool = True, include_sec: bool = True, 
              include_bank_mandates: bool = True, auto_confirm: bool = False):
    """Main agent execution with all improvements"""
    
    print(f"\n{'='*60}")
    print(f"Deal Flow Agent v5.0 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    print(f"Sources: RSS{'+ PE Firms' if include_pe_firms else ''}{'+ SEC' if include_sec else ''}{'+ Bank Mandates' if include_bank_mandates else ''}")
    print(f"Improvements: Two-stage classifier, Content dedup, Parallel analysis, Caching")
    print(f"{'='*60}\n")
    
    # Validate configuration
    if not all([NOTION_API_KEY, NOTION_DATABASE_ID, ANTHROPIC_API_KEY]):
        print("ERROR: Missing required environment variables")
        return
    
    # Initialize
    anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)
    dedup = DedupManager()
    run_state = RunState()
    
    # Show state info
    state_stats = run_state.get_stats()
    if state_stats["runs_count"] > 0:
        print(f"State loaded: {state_stats['cached_responses']} cached responses, {state_stats['processed_urls']} processed URLs")
        print(f"Last run: {state_stats['last_run']}")
        print()
    
    # Fetch existing entries
    print("Fetching existing entries from Notion...")
    existing_titles, existing_entries = get_existing_entries_from_db()
    dedup.load_existing_from_notion(existing_entries)
    print(f"Loaded {len(existing_titles)} existing titles, {len(existing_entries)} with hashes")
    
    if len(existing_titles) == 0 and not auto_confirm:
        print("WARNING: No existing entries found.")
        user_input = input("Continue anyway? [y/N]: ").strip().lower()
        if user_input != 'y':
            return
    print()
    
    # ==================================================
    # COLLECT ALL ARTICLES
    # ==================================================
    all_articles = []
    
    # SOURCE 1: RSS Feeds
    print("=" * 40)
    print("SOURCE 1: RSS Feeds")
    print("=" * 40)
    rss_articles = fetch_rss_articles(dedup)
    all_articles.extend(rss_articles)
    
    # SOURCE 2: PE Firm Press Releases
    if include_pe_firms:
        print("\n" + "=" * 40)
        print("SOURCE 2: PE Firm Press Releases")
        print("=" * 40)
        try:
            from pe_firm_monitor import fetch_pe_firm_signals, format_for_claude_analysis as format_pe
            pe_signals = fetch_pe_firm_signals()
            pe_articles = format_pe(pe_signals)
            # Dedup these too
            for article in pe_articles:
                if not dedup.is_url_duplicate(article.get('link', '')):
                    if not dedup.is_content_duplicate(article.get('title', ''), article.get('summary', '')):
                        dedup.mark_processed(article)
                        all_articles.append(article)
            print(f"  Added {len(pe_articles)} PE firm articles")
        except Exception as e:
            print(f"  Warning: PE firm monitor error: {e}")
    
    # SOURCE 3: SEC Filings
    if include_sec:
        print("\n" + "=" * 40)
        print("SOURCE 3: SEC Filings")
        print("=" * 40)
        try:
            from sec_monitor import fetch_all_sec_signals, format_for_claude_analysis as format_sec
            sec_signals = fetch_all_sec_signals(days_back=14)
            sec_articles = format_sec(sec_signals)
            for article in sec_articles:
                if not dedup.is_url_duplicate(article.get('link', '')):
                    dedup.mark_processed(article)
                    all_articles.append(article)
            print(f"  Added {len(sec_articles)} SEC filings")
        except Exception as e:
            print(f"  Warning: SEC monitor error: {e}")
    
    # SOURCE 4: Bank Mandates
    if include_bank_mandates:
        print("\n" + "=" * 40)
        print("SOURCE 4: Bank Mandate Announcements")
        print("=" * 40)
        try:
            from bank_mandate_monitor import fetch_bank_mandate_signals, format_for_claude_analysis as format_bank
            bank_signals = fetch_bank_mandate_signals()
            bank_articles = format_bank(bank_signals)
            for article in bank_articles:
                if not dedup.is_url_duplicate(article.get('link', '')):
                    dedup.mark_processed(article)
                    all_articles.append(article)
            print(f"  Added {len(bank_articles)} bank mandate articles")
        except Exception as e:
            print(f"  Warning: Bank mandate monitor error: {e}")
    
    print(f"\n{'='*40}")
    print(f"TOTAL COLLECTED: {len(all_articles)} articles")
    print(f"{'='*40}\n")
    
    # ==================================================
    # STAGE 1: CLASSIFY ARTICLES
    # ==================================================
    print("=" * 40)
    print("STAGE 1: Classification (regex pre-filter)")
    print("=" * 40)
    
    to_analyze, to_skip = classify_batch(all_articles)
    stats = get_classification_stats(all_articles)
    
    print(f"  High signal (>=6): {stats['high_signal']}")
    print(f"  Medium signal (3-5): {stats['medium_signal']}")
    print(f"  Low signal (1-2): {stats['low_signal']}")
    print(f"  No signal (0): {stats['no_signal']}")
    print(f"  → Sending to Claude: {len(to_analyze)}")
    print(f"  → Skipping: {len(to_skip)}")
    
    api_savings = len(to_skip) * 0.015  # Estimated $0.015 per Claude call
    print(f"  → Estimated API savings: ${api_savings:.2f}")
    
    # ==================================================
    # STAGE 1.5: COMPANY GROUPING (reduce duplicate analysis)
    # ==================================================
    print(f"\n{'='*40}")
    print("STAGE 1.5: Company Grouping")
    print("=" * 40)
    
    # Group articles by extracted company name
    company_groups: dict[str, list] = {}
    ungrouped = []
    
    for article in to_analyze:
        title = article.get('title', '')
        company = extract_company_from_title(title)
        
        if company and company.lower() not in ['unknown', 'none', '']:
            # Normalize for grouping
            company_key = company.lower().strip()
            if company_key not in company_groups:
                company_groups[company_key] = []
            company_groups[company_key].append(article)
        else:
            ungrouped.append(article)
    
    # Select best article per company (longest summary = most info)
    representative_articles = []
    articles_grouped = 0
    
    for company_key, articles in company_groups.items():
        if len(articles) > 1:
            # Pick the one with longest summary (most info for Claude)
            best = max(articles, key=lambda a: len(a.get('summary', '')))
            best['_company_group'] = company_key
            best['_group_size'] = len(articles)
            best['_related_urls'] = [a.get('link', '') for a in articles if a != best]
            representative_articles.append(best)
            articles_grouped += len(articles) - 1
        else:
            representative_articles.append(articles[0])
    
    # Add ungrouped articles
    representative_articles.extend(ungrouped)
    
    print(f"  Companies detected: {len(company_groups)}")
    print(f"  Articles grouped (skipped): {articles_grouped}")
    print(f"  → Analyzing: {len(representative_articles)} (was {len(to_analyze)})")
    
    grouping_savings = articles_grouped * 0.015
    print(f"  → Additional API savings: ${grouping_savings:.2f}")
    
    # ==================================================
    # STAGE 2: CLAUDE ANALYSIS (parallel)
    # ==================================================
    print(f"\n{'='*40}")
    print(f"STAGE 2: Claude Analysis ({len(representative_articles)} articles)")
    print(f"{'='*40}")
    
    if not representative_articles:
        print("  No articles to analyze")
        return
    
    print(f"  Analyzing with {MAX_CONCURRENT_CLAUDE_CALLS} parallel workers...")
    
    results = analyze_articles_parallel(anthropic, representative_articles, dedup, run_state)
    print(f"  Relevant results: {len(results)}")
    
    # ==================================================
    # STAGE 3: POST-FILTER AND WRITE
    # ==================================================
    print(f"\n{'='*40}")
    print("STAGE 3: Post-filter and Write to Notion")
    print(f"{'='*40}")
    
    new_entries = 0
    skipped_filtered = 0
    skipped_deal_dupe = 0
    
    for article, analysis in results:
        company = safe_str(analysis.get("company"), "Unknown")
        division = safe_str(analysis.get("division"), "Division")
        ev_low = analysis.get("ev_low")
        
        # Deal-level dedup
        if dedup.is_deal_duplicate(company, division, ev_low):
            skipped_deal_dupe += 1
            print(f"  ⊘ Deal duplicate: {company} - {division}")
            continue
        
        # Post-extraction filter
        passes, reason = passes_post_filters(analysis)
        if not passes:
            skipped_filtered += 1
            print(f"  ⊘ Filtered: {company} - {division} ({reason})")
            continue
        
        # Write to Notion
        success = create_notion_entry(NOTION_DATABASE_ID, article, analysis)
        if success:
            new_entries += 1
            dedup.mark_processed(article, analysis)
    
    # ==================================================
    # SUMMARY
    # ==================================================
    
    # Save state for next run
    run_state.save()
    
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Articles collected: {len(all_articles)}")
    print(f"  Passed classification: {len(to_analyze)}")
    print(f"  Relevant (Claude): {len(results)}")
    print(f"  New entries added: {new_entries}")
    print(f"  Deal duplicates skipped: {skipped_deal_dupe}")
    print(f"  Filtered out: {skipped_filtered}")
    print(f"  Classification skipped: {len(to_skip)}")
    
    dedup_stats = dedup.get_stats()
    print(f"\nDeduplication stats:")
    print(f"  URL duplicates: {dedup_stats['url_dupes']}")
    print(f"  Content duplicates: {dedup_stats['content_dupes']}")
    print(f"  Deal duplicates: {dedup_stats['deal_dupes']}")
    
    state_stats = run_state.get_stats()
    print(f"\nCache stats:")
    print(f"  Cached responses: {state_stats['cached_responses']}")
    print(f"  Processed URLs: {state_stats['processed_urls']}")
    print(f"  Total runs: {state_stats['runs_count']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Deal Flow Agent v5.0")
    parser.add_argument("--no-pe-firms", action="store_true", help="Skip PE firm press releases")
    parser.add_argument("--no-sec", action="store_true", help="Skip SEC filings")
    parser.add_argument("--no-bank-mandates", action="store_true", help="Skip bank mandate announcements")
    parser.add_argument("--rss-only", action="store_true", help="Only use RSS feeds")
    parser.add_argument("--auto-confirm", "-y", action="store_true", help="Skip confirmation prompts")
    
    args = parser.parse_args()
    
    if args.rss_only:
        run_agent(include_pe_firms=False, include_sec=False, include_bank_mandates=False, 
                  auto_confirm=args.auto_confirm)
    else:
        run_agent(
            include_pe_firms=not args.no_pe_firms,
            include_sec=not args.no_sec,
            include_bank_mandates=not args.no_bank_mandates,
            auto_confirm=args.auto_confirm
        )
