"""
Deal Flow Agent v6.0 - Daily Carve-Out/Spin-Off Intelligence

Improvements over v5.0:
- Consolidated caching system (using cache.py CacheManager)
- Structured logging with metrics tracking
- Retry logic with exponential backoff for API calls
- Rate limiting for Notion and SEC APIs
- Improved Claude prompt with few-shot examples
- Removed redundant company grouping (using dedup module)
- Input sanitization for prompt injection prevention
- Response validation for Claude outputs
- Better error handling throughout

Scans public sources for:
1. Divestiture signals (companies selling divisions)
2. PE buyer activity (firms circling targets)

Writes results to Notion with improved deduplication.
"""

import os
import re
import json
import time
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from anthropic import Anthropic
from target_accounts import TARGET_PE_FIRMS, passes_tier2_filter, FILTERED_SIGNAL_TYPES

# Import modular components
from dedup import (
    DedupManager,
    compute_deal_hash,
    compute_url_hash,
)
from classifier import classify_batch
from cache import CacheManager
from rss_monitor import fetch_rss_articles
from utils import (
    logger,
    RunMetrics,
    RateLimiter,
    make_request_with_retry,
    sanitize_for_prompt,
    validate_claude_response,
    validate_environment,
    notion_rate_limiter,
)

# Configuration
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# Parallelization settings
MAX_CONCURRENT_CLAUDE_CALLS = 5
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Build PE firm name patterns for matching
PE_FIRM_PATTERNS = [firm.lower() for firm in TARGET_PE_FIRMS]


# ==========================================================================
# IMPROVED CLAUDE PROMPT WITH FEW-SHOT EXAMPLES
# ==========================================================================

CLAUDE_SYSTEM_PROMPT = """You are an expert M&A analyst specializing in corporate carve-outs and divestitures.
Your task is to analyze news articles and identify genuine carve-out opportunities for separation consultants.

A carve-out is when a DIVISION, UNIT, or SEGMENT is separated from its PARENT COMPANY and requires
operational separation work (IT systems, back-office, manufacturing, TSA agreements).

ALWAYS EXCLUDE:
- Secondary buyouts (PE to PE sales)
- Standalone business sales (independent subsidiaries)
- Whole company acquisitions
- IPOs, minority stakes, venture capital
- Geographies outside US/UK/Europe

ALWAYS INCLUDE:
- Corporate parent selling/spinning off a division that shares infrastructure
- Conglomerate divesting a business unit using shared services
- Strategic review of a division within a larger corporate"""


def build_claude_prompt(article: dict) -> str:
    """Build the Claude analysis prompt with few-shot examples"""

    title = sanitize_for_prompt(article.get('title', ''), max_length=500)
    summary = sanitize_for_prompt(article.get('summary', ''), max_length=2000)
    source = sanitize_for_prompt(article.get('source', ''), max_length=100)

    return f"""Analyze this news article for carve-out opportunities.

ARTICLE:
Title: {title}
Summary: {summary}
Source: {source}

EXAMPLES:

Example 1 - RELEVANT (True Carve-Out):
Title: "Siemens to carve out industrial motors division, hires Goldman Sachs"
Analysis: {{"is_relevant": true, "company": "Siemens", "division": "Industrial Motors", "signal_type": "Adviser Appointed", "pe_buyer": null, "likely_buyers": "KPS Capital, Platinum Equity, American Industrial Partners", "ev_low": 800, "ev_high": 1200, "sector": "Industrials", "geography": "Europe", "complexity": "High", "confidence": "high"}}

Example 2 - NOT RELEVANT (Secondary Buyout):
Title: "Blackstone acquires software company from Vista Equity Partners"
Analysis: {{"is_relevant": false, "reason": "Secondary buyout - PE to PE sale, no separation work needed"}}

Example 3 - NOT RELEVANT (Standalone Business):
Title: "NatWest sells fintech subsidiary Cushon to WTW"
Analysis: {{"is_relevant": false, "reason": "Standalone business that operates independently, no operational separation required"}}

Example 4 - RELEVANT (Strategic Review):
Title: "3M exploring strategic alternatives for healthcare business unit"
Analysis: {{"is_relevant": true, "company": "3M", "division": "Healthcare", "signal_type": "Strategic Review", "pe_buyer": null, "likely_buyers": "Carlyle, CD&R, Bain Capital", "ev_low": 15000, "ev_high": 20000, "sector": "Healthcare", "geography": "US", "complexity": "Very High", "confidence": "high"}}

NOW ANALYZE THE ARTICLE ABOVE.

If RELEVANT, return JSON with these fields:
- is_relevant: true
- company: Parent company name (seller)
- division: Division/unit being carved out
- signal_type: "Strategic Review" | "Adviser Appointed" | "PE Interest" | "PE In Talks" | "PE Bid Submitted" | "Definitive Agreement" | "Deal Completed"
- pe_buyer: PE firm involved (null if none)
- likely_buyers: PE firms likely interested (null if unknown)
- size_estimate: Revenue or deal value if mentioned
- ev_low: Low EV estimate in millions (null if unknown). Use 8-12x EBITDA if mentioned.
- ev_high: High EV estimate in millions (null if unknown)
- sector: "TMT" | "Financial Services" | "Healthcare" | "Consumer" | "Industrials" | "Retail" | "Technology" | "Other"
- geography: "US" | "UK" | "Europe"
- complexity: "Low" | "Medium" | "High" | "Very High"
- key_quote: Key sentence from article
- buyer_intelligence: Why this interests PE buyers
- notes: Other context
- confidence: "high" | "medium" | "low"

If NOT RELEVANT, return:
{{"is_relevant": false, "reason": "Brief explanation"}}

Return ONLY valid JSON, no other text."""


# ==========================================================================
# NOTION HELPERS
# ==========================================================================

def get_existing_entries_from_db(metrics: RunMetrics) -> tuple[set, list]:
    """
    Fetch existing entries from Notion for deduplication.
    Uses rate limiting and retry logic.
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
            notion_rate_limiter.wait()

            def make_request():
                return requests.post(
                    f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                    headers=headers,
                    json=body,
                    timeout=30
                )

            response = make_request_with_retry(make_request)
            metrics.increment('api_calls_notion')

            if response.status_code != 200:
                logger.warning(f"Failed to fetch existing entries", status=response.status_code)
                break

            results = response.json()
            for entry in results.get("results", []):
                props = entry.get("properties", {})

                # Extract title
                company_prop = props.get("Company", {})
                title_list = company_prop.get("title", [])
                if title_list:
                    title = title_list[0].get("plain_text", "")
                    normalized = normalize_title(title)
                    if normalized:
                        existing_titles.add(normalized)

                # Extract hash fields and company/division for fuzzy dedup
                entry_data = {}
                deal_hash_prop = props.get("Deal Hash", {})
                if deal_hash_prop.get("rich_text"):
                    entry_data['deal_hash'] = deal_hash_prop["rich_text"][0].get("plain_text", "")

                url_hash_prop = props.get("Source URL Hash", {})
                if url_hash_prop.get("rich_text"):
                    entry_data['url_hash'] = url_hash_prop["rich_text"][0].get("plain_text", "")

                # Extract company and division for fuzzy matching
                division_prop = props.get("Division", {})
                if division_prop.get("rich_text"):
                    entry_data['division'] = division_prop["rich_text"][0].get("plain_text", "")
                
                # Company is in title format "Company - Division", extract just company
                if title_list:
                    full_title = title_list[0].get("plain_text", "")
                    if " - " in full_title:
                        entry_data['company'] = full_title.split(" - ")[0].strip()
                    else:
                        entry_data['company'] = full_title

                if entry_data:
                    existing_entries.append(entry_data)

            has_more = results.get("has_more", False)
            start_cursor = results.get("next_cursor")

        except Exception as e:
            logger.error("Error fetching existing entries", exc=e)
            metrics.increment('api_errors')
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


# ==========================================================================
# CLAUDE ANALYSIS
# ==========================================================================

def analyze_article_with_claude(
    client: Anthropic,
    article: dict,
    cache: CacheManager,
    metrics: RunMetrics
) -> Optional[dict]:
    """
    Use Claude to extract structured deal information from article.
    Includes caching, retry logic, and response validation.
    """
    url = article.get('link', '')
    title = article.get('title', '')

    # Check cache first
    cached = cache.get_response(url, title)
    if cached:
        metrics.increment('cache_hits')
        return cached if cached.get("is_relevant") else None

    metrics.increment('cache_misses')
    prompt = build_claude_prompt(article)

    # Retry logic for Claude API
    max_retries = 3
    last_error = None

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1000,
                system=CLAUDE_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}]
            )

            metrics.increment('api_calls_claude')

            response_text = response.content[0].text.strip()

            # Clean up markdown code blocks
            if response_text.startswith("```"):
                response_text = re.sub(r'^```json?\n?', '', response_text)
                response_text = re.sub(r'\n?```$', '', response_text)

            result = json.loads(response_text)

            # Validate response structure
            is_valid, error_msg = validate_claude_response(result)
            if not is_valid:
                logger.warning(f"Invalid Claude response", error=error_msg, title=title[:50])
                # Return None but still cache to avoid retrying
                cache.set_response(url, title, {"is_relevant": False, "reason": f"Invalid response: {error_msg}"})
                return None

            # Cache the result
            cache.set_response(url, title, result)

            if result.get("is_relevant") and result.get("confidence") in ["high", "medium"]:
                return result
            return None

        except json.JSONDecodeError as e:
            last_error = e
            logger.warning(f"JSON parse error (attempt {attempt + 1})", error=str(e)[:50])
            if attempt < max_retries - 1:
                time.sleep(1)

        except Exception as e:
            last_error = e
            metrics.increment('api_errors')
            if attempt < max_retries - 1:
                delay = 2 ** attempt
                logger.warning(f"Claude API error (attempt {attempt + 1})", delay=delay, error=str(e)[:50])
                time.sleep(delay)
                metrics.increment('api_retries')
            else:
                logger.error("Claude analysis failed after retries", exc=e, title=title[:50])

    return None


def analyze_articles_parallel(
    client: Anthropic,
    articles: list,
    dedup: DedupManager,
    cache: CacheManager,
    metrics: RunMetrics
) -> list:
    """
    Analyze multiple articles in parallel using ThreadPoolExecutor.
    Uses dedup module for company grouping (removed redundant implementation).
    """
    # Use dedup's get_representative_articles (removes redundant grouping)
    logger.info(f"Grouping {len(articles)} articles by company")
    representatives = dedup.get_representative_articles(articles)
    grouped_count = len(articles) - len(representatives)

    if grouped_count > 0:
        logger.info(f"Reduced to {len(representatives)} representatives", grouped=grouped_count)

    # Check cache for already-analyzed articles
    to_analyze = []
    cached_results = []

    for article in representatives:
        url = article.get('link', '')
        title = article.get('title', '')
        cached = cache.get_response(url, title)

        if cached and cached.get("is_relevant"):
            cached_results.append((article, cached))
            metrics.increment('cache_hits')
        else:
            to_analyze.append(article)

    if cached_results:
        logger.info(f"Cache results", from_cache=len(cached_results), need_analysis=len(to_analyze))

    results = cached_results.copy()

    if to_analyze:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_CLAUDE_CALLS) as executor:
            future_to_article = {
                executor.submit(analyze_article_with_claude, client, article, cache, metrics): article
                for article in to_analyze
            }

            for future in as_completed(future_to_article):
                article = future_to_article[future]
                try:
                    analysis = future.result()
                    if analysis:
                        results.append((article, analysis))
                        metrics.increment('articles_relevant')
                except Exception as e:
                    logger.error(f"Analysis failed", exc=e, title=article.get('title', '')[:50])

    return results


# ==========================================================================
# POST-FILTERING
# ==========================================================================

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


# ==========================================================================
# NOTION WRITE
# ==========================================================================

def safe_str(value, default=""):
    """Safely convert value to string, handling None"""
    if value is None:
        return default
    return str(value)


def create_notion_entry(
    database_id: str,
    article: dict,
    analysis: dict,
    metrics: RunMetrics
) -> bool:
    """Create a new entry in the Notion database with rate limiting and retry."""
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
    valid_signal_types = ["Strategic Review", "Adviser Appointed", "PE Interest", "PE In Talks",
                          "PE Bid Submitted", "Definitive Agreement", "Deal Completed"]
    if signal_type not in valid_signal_types:
        signal_type = "Strategic Review"

    valid_sectors = ["TMT", "Financial Services", "Healthcare", "Consumer", "Industrials",
                     "Retail", "Technology", "Other"]
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
        "Status": {"select": {"name": "Lead"}},
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
        # Rate limit Notion requests
        notion_rate_limiter.wait()

        def make_request():
            return requests.post(
                "https://api.notion.com/v1/pages",
                headers=headers,
                json={"parent": {"database_id": database_id}, "properties": properties},
                timeout=30
            )

        response = make_request_with_retry(make_request)
        metrics.increment('api_calls_notion')

        if response.status_code == 200:
            if pe_buyer:
                logger.info(f"Added: {company} - {division} (PE: {pe_buyer})")
            else:
                logger.info(f"Added: {company} - {division}")
            return True
        else:
            logger.error(f"Failed to add entry", status=response.status_code, error=response.text[:100])
            metrics.increment('api_errors')
            return False

    except Exception as e:
        logger.error("Failed to add entry", exc=e)
        metrics.increment('api_errors')
        return False


# ==========================================================================
# MAIN AGENT
# ==========================================================================

def run_agent(
    include_pe_firms: bool = True,
    include_bank_mandates: bool = True,
    auto_confirm: bool = False
):
    """Main agent execution with all improvements"""

    print(f"\n{'='*60}")
    print(f"Deal Flow Agent v6.0 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    print(f"Sources: RSS{'+ PE Firms' if include_pe_firms else ''}{'+ Bank Mandates' if include_bank_mandates else ''}")
    print(f"Improvements: Consolidated cache, Structured logging, Rate limiting, Retries")
    print(f"{'='*60}\n")

    # Validate environment
    is_valid, missing = validate_environment()
    if not is_valid:
        logger.error(f"Missing required environment variables: {missing}")
        return

    # Initialize
    metrics = RunMetrics()
    anthropic = Anthropic(api_key=ANTHROPIC_API_KEY)
    dedup = DedupManager()
    cache = CacheManager()

    # Check for incomplete run to resume
    if cache.has_incomplete_run():
        run_state = cache.get_run_state()
        logger.info(
            "Found incomplete run to resume",
            phase=run_state.phase,
            articles_collected=run_state.articles_collected,
            pending=len(run_state.pending_articles)
        )
        if not auto_confirm:
            user_input = input("Resume previous run? [Y/n]: ").strip().lower()
            if user_input == 'n':
                cache.start_run()
                logger.info("Starting fresh run")
    else:
        cache.start_run()

    # Show cache stats
    cache_stats = cache.get_stats()
    if cache_stats['response_cache']['size'] > 0:
        logger.info(
            "Cache loaded",
            responses=cache_stats['response_cache']['size'],
            processed_urls=cache_stats['processed_urls']['total']
        )

    # Fetch existing entries
    logger.info("Fetching existing entries from Notion...")
    existing_titles, existing_entries = get_existing_entries_from_db(metrics)
    dedup.load_existing_from_notion(existing_entries)
    logger.info(f"Loaded {len(existing_titles)} existing titles, {len(existing_entries)} with hashes")

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
    metrics.set_source_count('rss', len(rss_articles))

    # SOURCE 2: PE Firm Press Releases
    if include_pe_firms:
        print("\n" + "=" * 40)
        print("SOURCE 2: PE Firm Press Releases")
        print("=" * 40)
        try:
            from pe_firm_monitor import fetch_pe_firm_signals, format_for_claude_analysis as format_pe
            pe_signals = fetch_pe_firm_signals()
            pe_articles = format_pe(pe_signals)
            pe_count = 0
            for article in pe_articles:
                if not dedup.is_url_duplicate(article.get('link', '')):
                    if not dedup.is_content_duplicate(article.get('title', ''), article.get('summary', '')):
                        dedup.mark_processed(article)
                        all_articles.append(article)
                        pe_count += 1
            logger.info(f"Added {pe_count} PE firm articles")
            metrics.set_source_count('pe_firms', pe_count)
        except Exception as e:
            logger.error("PE firm monitor error", exc=e)

    # SOURCE 3: Bank Mandates
    if include_bank_mandates:
        print("\n" + "=" * 40)
        print("SOURCE 3: Bank Mandate Announcements")
        print("=" * 40)
        try:
            from bank_mandate_monitor import fetch_bank_mandate_signals, format_for_claude_analysis as format_bank
            bank_signals = fetch_bank_mandate_signals()
            bank_articles = format_bank(bank_signals)
            bank_count = 0
            for article in bank_articles:
                if not dedup.is_url_duplicate(article.get('link', '')):
                    dedup.mark_processed(article)
                    all_articles.append(article)
                    bank_count += 1
            logger.info(f"Added {bank_count} bank mandate articles")
            metrics.set_source_count('bank_mandates', bank_count)
        except Exception as e:
            logger.error("Bank mandate monitor error", exc=e)

    metrics.increment('articles_collected', len(all_articles))
    cache.update_run("collecting", articles_collected=len(all_articles))
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
    metrics.increment('articles_classified', len(all_articles))

    # Count by score level
    high_signal = sum(1 for a in to_analyze if a.get('_classification', {}).get('score', 0) >= 6)
    medium_signal = len(to_analyze) - high_signal

    print(f"  High signal (>=6): {high_signal}")
    print(f"  Medium signal (3-5): {medium_signal}")
    print(f"  Low/No signal: {len(to_skip)}")
    print(f"  -> Sending to Claude: {len(to_analyze)}")
    print(f"  -> Skipping: {len(to_skip)}")

    api_savings = len(to_skip) * 0.015
    print(f"  -> Estimated API savings: ${api_savings:.2f}")

    # Update run state with pending articles for resume capability
    cache.update_run(
        "classifying",
        articles_skipped=len(to_skip),
        pending_articles=[a.get('link', '') for a in to_analyze if a.get('link')]
    )

    # ==================================================
    # STAGE 2: CLAUDE ANALYSIS (parallel)
    # ==================================================
    print(f"\n{'='*40}")
    print(f"STAGE 2: Claude Analysis ({len(to_analyze)} articles)")
    print(f"{'='*40}")

    if not to_analyze:
        print("  No articles to analyze")
        cache.complete_run()
        metrics.complete()
        metrics.print_summary()
        return

    print(f"  Analyzing with {MAX_CONCURRENT_CLAUDE_CALLS} parallel workers...")

    cache.update_run("analyzing")
    results = analyze_articles_parallel(anthropic, to_analyze, dedup, cache, metrics)
    metrics.increment('articles_analyzed', len(to_analyze))
    cache.update_run("analyzing", articles_analyzed=len(to_analyze), articles_relevant=len(results))
    print(f"  Relevant results: {len(results)}")

    # ==================================================
    # STAGE 3: POST-FILTER AND WRITE
    # ==================================================
    print(f"\n{'='*40}")
    print("STAGE 3: Post-filter and Write to Notion")
    print(f"{'='*40}")

    cache.update_run("writing")
    new_entries = 0
    skipped_filtered = 0
    skipped_deal_dupe = 0

    for article, analysis in results:
        company = safe_str(analysis.get("company"), "Unknown")
        division = safe_str(analysis.get("division"), "Division")
        ev_low = analysis.get("ev_low")

        # Deal-level dedup (exact hash match)
        if dedup.is_deal_duplicate(company, division, ev_low):
            skipped_deal_dupe += 1
            metrics.increment('entries_duplicate')
            logger.info(f"Deal duplicate: {company} - {division}")
            continue

        # Fuzzy dedup (catches "North American" vs "North America" etc)
        if dedup.is_fuzzy_duplicate(company, division):
            skipped_deal_dupe += 1
            metrics.increment('entries_duplicate')
            logger.info(f"Fuzzy duplicate: {company} - {division}")
            continue

        # Post-extraction filter
        passes, reason = passes_post_filters(analysis)
        if not passes:
            skipped_filtered += 1
            metrics.increment('entries_filtered')
            logger.info(f"Filtered: {company} - {division} ({reason})")
            continue

        # Write to Notion
        success = create_notion_entry(NOTION_DATABASE_ID, article, analysis, metrics)
        if success:
            # Track for fuzzy dedup within this run
            dedup.add_company_division(company, division)
            new_entries += 1
            metrics.increment('entries_written')
            dedup.mark_processed(article, analysis)
            cache.mark_processed(article.get('link', ''), "added")

    # ==================================================
    # SUMMARY
    # ==================================================

    # Mark run as complete
    cache.update_run(
        "complete",
        entries_written=new_entries,
        entries_filtered=skipped_filtered,
        entries_duplicate=skipped_deal_dupe
    )
    cache.complete_run()
    metrics.complete()

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

    cache_stats = cache.get_stats()
    print(f"\nCache stats:")
    print(f"  Response cache size: {cache_stats['response_cache']['size']}")
    print(f"  Cache hit rate: {cache_stats['response_cache']['hit_rate']:.1%}")
    print(f"  Processed URLs: {cache_stats['processed_urls']['total']}")

    metrics.print_summary()
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Deal Flow Agent v6.0")
    parser.add_argument("--no-pe-firms", action="store_true", help="Skip PE firm press releases")
    parser.add_argument("--no-bank-mandates", action="store_true", help="Skip bank mandate announcements")
    parser.add_argument("--rss-only", action="store_true", help="Only use RSS feeds")
    parser.add_argument("--auto-confirm", "-y", action="store_true", help="Skip confirmation prompts")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logger.set_level("DEBUG")

    if args.rss_only:
        run_agent(include_pe_firms=False, include_bank_mandates=False,
                  auto_confirm=args.auto_confirm)
    else:
        run_agent(
            include_pe_firms=not args.no_pe_firms,
            include_bank_mandates=not args.no_bank_mandates,
            auto_confirm=args.auto_confirm
        )
