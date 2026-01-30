"""
RSS Monitor - Simplified Deal Flow Agent
=========================================

Pipeline:
1. Fetch from RSS feeds (parallel)
2. URL deduplication
3. Filter: Target account mentions only (from target_accounts.py)
4. Filter: Exclude Asia/Oceania
5. Filter: Exclude out-of-scope (real estate, credit, minority, non-deal news)
6. Content deduplication (title similarity)
7. Output final list

"""

import feedparser
import csv
import re
import os
import time
import urllib.parse
import urllib3
import requests
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from target_accounts import TARGET_PE_FIRMS, FIRM_ALIASES, match_pe_firm, get_company_id, get_target_firms

# Disable SSL warnings
urllib3.disable_warnings()

# HubSpot API
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY")


# =============================================================================
# CONFIGURATION
# =============================================================================

# RSS feeds - PE-focused sources + target firm searches
RSS_FEEDS_PE_SOURCES = [
    # Direct PE/M&A news
    "https://www.pehub.com/feed/",
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",

    # Premium journalism via Google News
    "https://news.google.com/rss/search?q=site:ft.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:wsj.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:bloomberg.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:reuters.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",

    # Deal-focused queries
    "https://news.google.com/rss/search?q=private+equity+acquisition+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=private+equity+buyout+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=leveraged+buyout+when:2d&hl=en-US&gl=US&ceid=US:en",

    # UK/Europe
    "https://news.google.com/rss/search?q=private+equity+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=buyout+acquisition+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
]


def generate_firm_search_feeds() -> list[str]:
    """Generate Google News RSS feeds for batches of target firms"""
    feeds = []
    firms_list = list(get_target_firms())
    batch_size = 5

    for i in range(0, len(firms_list), batch_size):
        batch = firms_list[i:i+batch_size]
        query_parts = [f'"{firm}"' for firm in batch]
        query = " OR ".join(query_parts)
        encoded = urllib.parse.quote(query)

        # US and UK/Europe feeds
        feeds.append(f"https://news.google.com/rss/search?q={encoded}+when:2d&hl=en-US&gl=US&ceid=US:en")
        feeds.append(f"https://news.google.com/rss/search?q={encoded}+when:2d&hl=en-GB&gl=GB&ceid=GB:en")

    return feeds


# =============================================================================
# EXCLUSION FILTERS
# =============================================================================

ASIA_OCEANIA_TERMS = [
    'china', 'chinese', 'japan', 'japanese', 'korea', 'korean', 'south korea',
    'india', 'indian', 'singapore', 'hong kong', 'taiwan', 'taiwanese',
    'thailand', 'thai', 'vietnam', 'vietnamese', 'malaysia', 'malaysian',
    'indonesia', 'indonesian', 'philippines', 'filipino', 'pakistan',
    'australia', 'australian', 'new zealand', 'nz', 'oceania',
    'beijing', 'shanghai', 'shenzhen', 'tokyo', 'osaka', 'seoul', 'mumbai',
    'delhi', 'bangalore', 'sydney', 'melbourne', 'auckland',
    'asia', 'asian', 'apac', 'asia-pacific', 'asia pacific',
]

# =============================================================================
# DEAL KEYWORD FILTER (must match at least one)
# =============================================================================

DEAL_KEYWORDS = [
    # Acquisition/completion terms
    'acquires', 'acquired', 'acquisition', 'acquiring',
    'completes', 'completed', 'completion',
    'closes', 'closed', 'closing',
    'finalizes', 'finalized',
    # Agreement terms
    'signs', 'signed', 'signing',
    'agrees', 'agreed', 'agreement',
    'announces', 'announced', 'announcement',
    # Deal types
    'buyout', 'buy-out', 'buys',
    'purchase', 'purchased', 'purchasing',
    'merger', 'merges', 'merged',
    'deal', 'transaction',
    # Carve-out specific
    'carve-out', 'carveout', 'carve out',
    'divestiture', 'divests', 'divested',
    'spin-off', 'spinoff', 'spins off',
    'sells', 'sold', 'sale of',
]


def has_deal_keywords(text: str) -> tuple[bool, list[str]]:
    """
    Check if text contains deal-related keywords.
    Returns (has_keywords, list_of_matched_keywords)
    """
    if not text:
        return False, []

    text_lower = text.lower()
    matched = []

    for keyword in DEAL_KEYWORDS:
        if keyword in text_lower:
            matched.append(keyword)

    return len(matched) > 0, matched


# =============================================================================
# PE FIRM DIRECT RSS FEEDS (from scan of HubSpot companies)
# =============================================================================

PE_FIRM_RSS_FEEDS = {
    # Company Name -> RSS Feed URL
    "Montagu": "https://montagu.com/feed",
    "H.I.G. Capital": "https://higcapital.com/feed",
    "Altor Equity Partners": "https://altor.com/feed",
    "Flexpoint Ford": "https://flexpointford.com/feed",
    "Levine Leichtman Capital Partners": "https://llcp.com/feed",
    "Kohlberg & Company": "https://kohlberg.com/feed",
    "IK Partners": "https://ikpartners.com/feed",
    "Berkshire Partners": "https://berkshirepartners.com/feed",
    "Alvarez & Marsal Capital Partners": "https://amcapitalpartners.com/feed",
    "Vestar Capital Partners": "https://vestarcapital.com/feed",
    "Warburg Pincus": "https://warburgpincus.com/feed",
    "Summa Equity": "https://summaequity.com/feed",
    "Rivean Capital": "https://riveancapital.com/feed",
    "SK Capital Partners": "https://skcapitalpartners.com/feed",
    "Nautic Partners": "https://nautic.com/feed",
    "Arcline Investment Management": "https://arcline.com/feed",
    "Waterland Private Equity Investments": "https://waterland.nl/feed",
    "Leonard Green & Partners": "https://leonardgreen.com/feed",
    "Vistria Group": "https://vistria.com/feed",
    "Oak Hill Capital": "https://oakhillcapital.com/feed",
    "Alpine Investors": "https://alpineinvestors.com/feed",
    "GTCR": "https://gtcr.com/feed",
    "Genstar Capital": "https://genstarcapital.com/feed",
    "Hellman & Friedman": "https://hf.com/feed",
    "The Jordan Company": "https://thejordancompany.com/feed",
    "Brightstar Capital Partners": "https://brightstarcapitalpartners.com/feed",
    "Sagard": "https://sagard.com/feed",
    "Incline Equity Partners": "https://inclineequity.com/feed",
    "New Mountain Capital": "https://newmountaincapital.com/feed",
    "Lindsay Goldberg": "https://lindsaygoldberg.com/feed",
    "Wind Point Partners": "https://windpointpartners.com/feed",
    "Gemspring Capital": "https://gemspring.com/feed",
    "Cortec Group": "https://cortecgroup.com/feed",
    "SDC Capital Partners": "https://sdccapital.com/feed",
    "Cinven": "https://cinven.com/feed",
    "GHO Capital Partners": "https://ghocapital.com/feed",
    "AEA Investors": "https://aeainvestors.com/feed",
    "Blackstone": "https://blackstone.com/feed",
    "Trivest Partners": "https://trivest.com/feed",
    "Gridiron Capital": "https://gridironcapital.com/feed",
    "Platinum Equity": "https://platinumequity.com/feed",
    "Frazier Healthcare Partners": "https://frazierhealthcare.com/feed",
    "LLR Partners": "https://llrpartners.com/rss",
    "Pacific Equity Partners": "https://pep.com.au/feed/",
    "TDR Capital": "https://tdrcapital.com/feed/",
    "Greenbriar Equity Group": "https://greenbriarequity.com/feed/",
    "The Sterling Group": "https://sterling-group.com/feed",
    "Lovell Minnick Partners": "https://lmp.com/feed",
    "Kinderhook Industries": "https://kinderhook.com/feed/",
    "Tate & Lyle": "https://www.tateandlyle.com/rss.xml",
}


def fetch_pe_firm_rss_articles(lookback_hours: int = 24, verbose: bool = True) -> list[dict]:
    """
    Fetch articles from PE firm direct RSS feeds.
    Filters for deal keywords before returning.

    Returns list of articles that contain deal announcements.
    """
    if verbose:
        print(f"\nFetching from {len(PE_FIRM_RSS_FEEDS)} PE firm RSS feeds...")

    all_articles = []

    for firm_name, feed_url in PE_FIRM_RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)

            for entry in feed.entries[:10]:  # Latest 10 per firm
                published = entry.get("published", "")

                if not is_within_hours(published, lookback_hours):
                    continue

                title = entry.get("title", "")
                summary = entry.get("summary", entry.get("description", ""))[:500]
                text = f"{title} {summary}"

                # Filter for deal keywords
                has_deal, keywords = has_deal_keywords(text)
                if not has_deal:
                    continue

                all_articles.append({
                    "title": title,
                    "link": entry.get("link", ""),
                    "summary": summary,
                    "published": published,
                    "source": f"{firm_name} (Direct)",
                    "target_accounts": firm_name,
                    "deal_keywords": ", ".join(keywords[:3]),
                })

        except Exception as e:
            if verbose:
                print(f"  Warning: Failed to fetch {firm_name}: {e}")

        time.sleep(0.2)  # Rate limiting

    if verbose:
        print(f"  Found {len(all_articles)} deal announcements from PE firm feeds")

    return all_articles


SCOPE_EXCLUSIONS = {
    'Specialty Finance': [
        'specialty finance', 'senior unsecured notes', 'supplemental indenture'
    ],
    'Private Wealth': [
        'private wealth', 'wealth push', 'wealth ranks', 'wealth product', 'wealth hire'
    ],
    'PE Firm Stock News': [
        'share price pullback', 'stock position', 'stock holdings in',
        'acquires shares of', 'bullish or bearish', 'options begin trading',
        'core holding', 'shares up', 'buys back', 'share price', 'derivative'
    ],
    'Dividend/NAV News': [
        'dividend', 'nav total return', 'share buybacks', 'buybacks'
    ],
    'Credit/Debt': [
        'credit income fund', 'credit income', 'debt deal', 'bonds', 'private credit'
    ],
    'Profit/Results News': [
        'profit jump', 'beats estimates', 'surprise profit'
    ],
    'Real Estate': [
        'real estate', 'reit', 'property fund', 'warehouse', 'logistics facility',
        'office building', 'commercial real estate', 'data center', 'data centre',
        'residential', 'rent', 'industrial property', 'retail property',
        'cmbs market', 'mortgage trust',
    ],
    'Minority/Non-buyout': [
        'minority stake', 'minority investment', 'takes stake', 'took stake',
        'growth investment', 'growth equity', 'invests in', 'buys stake', 'secondaries'
    ],
    'Non-deal News': [
        'fundraising', 'raises fund', 'closes fund', 'closed fund',
        'earnings', 'quarterly results', 'q1 results', 'q2 results', 'q3 results', 'q4 results',
        'stock price', 'analyst', 'rating', 'upgraded', 'downgraded',
        'ipo', 'goes public', 'valuation', 'venture fund',
        'fourth quarter for its', 'infrastructure fund', 'wealth channel',
    ],
    'Legal/Regulatory': [
        'investigating', 'investigation', 'investor challenge'
    ],
    'False Positives': [
        'adidas', 'boots leaked', 'gas producer', 'the points guy', 'tpg awards',
        'silver lake eagles',
        # KKR = Kolkata Knight Riders (cricket)
        'kolkata knight riders', 'ipl 2026', 'csk vs kkr', 'kkr vs', 'vs kkr', 'bbl 2025',
        'win over kkr', 'csk end', 'dhoni',
        # TPG false positives
        'tpg online daily', 'the platform group', 'tpg re finance', 'harwood district',
        # Kantar = market research, not PE
        'kantar', 'winter olympics',
        # Blackstone grill brand
        'blackstone grill', 'blackstone tailgater', 'blackstone griddle',
        # EQT Corporation = US gas company (NYSE: EQT), not EQT Partners
        'eqt ties', 'eqt gas', 'gas demand', 'natural gas', 'eqt corporation',
        'eqt projects', 'eqt foundation', 'derivatives gain',
        # Tate & Lyle = food company
        'tate & lyle', 'tate and lyle',
        # Stock holdings (institutional, not deals)
        'mellon corp acquires', 'million stake in',
    ],
    'Price Targets': [
        'price target', 'target at', 'target cut', 'target raised',
    ],
    'Fund/CLO News': [
        'clo 10', 'clo ltd', 're-up rate', 'fund iv', 'fund v', 'fund vi',
        'sophomore vehicle', 'bond offering', 'secured lending',
    ],
}


# =============================================================================
# DATE PARSING
# =============================================================================

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


def is_within_hours(date_str: str, hours: int = 24) -> bool:
    """Check if published date is within last N hours"""
    if not date_str:
        return True  # Assume recent if no date

    pub_date = parse_published_date(date_str)
    if not pub_date:
        return True

    now = datetime.now()
    age = now - pub_date
    return age.total_seconds() < hours * 3600


# =============================================================================
# FILTERING FUNCTIONS
# =============================================================================

def find_target_accounts(text: str) -> list[tuple[str, str, int]]:
    """
    Find target account mentions in text.
    Returns list of (matched_text, canonical_name, confidence)
    """
    if not text:
        return []

    matches = []
    text_lower = text.lower()

    # Check aliases (exact match)
    for alias, canonical in FIRM_ALIASES.items():
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower):
            if not any(m[1] == canonical for m in matches):
                matches.append((alias, canonical, 100))

    # Check canonical names from HubSpot
    for firm in get_target_firms():
        firm_lower = firm.lower()
        pattern = r'\b' + re.escape(firm_lower) + r'\b'
        if re.search(pattern, text_lower):
            if not any(m[1] == firm for m in matches):
                matches.append((firm, firm, 100))

    # Fuzzy match potential firm names
    potential_names = re.findall(
        r'\b([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)\s+(?:Capital|Partners|Group|Equity|Management|Advisers|Investments?)\b',
        text
    )
    for potential in potential_names:
        is_match, canonical, confidence = match_pe_firm(potential)
        if is_match and confidence >= 85:
            if not any(m[1] == canonical for m in matches):
                matches.append((potential, canonical, confidence))

    return matches


def is_asia_oceania(text: str) -> tuple[bool, Optional[str]]:
    """Check if text mentions Asia/Oceania"""
    text_lower = text.lower()
    for term in ASIA_OCEANIA_TERMS:
        pattern = r'\b' + re.escape(term) + r'\b'
        if re.search(pattern, text_lower):
            return True, term
    return False, None


def is_out_of_scope(text: str) -> tuple[bool, Optional[str], Optional[str]]:
    """Check if text matches out-of-scope exclusions"""
    text_lower = text.lower()
    for category, terms in SCOPE_EXCLUSIONS.items():
        for term in terms:
            pattern = r'\b' + re.escape(term) + r'\b'
            if re.search(pattern, text_lower):
                return True, category, term
    return False, None, None


def dedupe_by_content(articles: list[dict]) -> list[dict]:
    """
    Deduplicate articles by title similarity.
    Keeps first occurrence of each story.
    """
    unique = []
    seen_signatures = []

    for article in articles:
        title = article.get('title', '')
        accounts = article.get('target_accounts', '')

        # Create signature: significant words from title + accounts
        words = set(w.lower() for w in title.split() if len(w) > 5)
        signature = (frozenset(words), accounts)

        # Check if similar to any seen article
        is_dupe = False
        for seen_words, seen_accounts in seen_signatures:
            if seen_accounts == accounts:
                overlap = len(words & seen_words)
                if overlap >= 3:  # At least 3 significant words in common
                    is_dupe = True
                    break

        if not is_dupe:
            unique.append(article)
            seen_signatures.append(signature)

    return unique


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def fetch_single_feed(url: str, lookback_hours: int = 24) -> list[dict]:
    """Fetch articles from a single RSS feed"""
    articles = []
    try:
        feed = feedparser.parse(url)
        feed_title = feed.feed.get("title", url[:50])

        for entry in feed.entries[:30]:
            published = entry.get("published", "")

            if not is_within_hours(published, lookback_hours):
                continue

            articles.append({
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "summary": entry.get("summary", entry.get("description", ""))[:500],
                "published": published,
                "source": feed_title,
            })
    except Exception:
        pass

    return articles


def run_pipeline(
    use_firm_searches: bool = True,
    use_pe_sources: bool = True,
    lookback_hours: int = 24,
    max_workers: int = 15,
    verbose: bool = True
) -> list[dict]:
    """
    Run the full RSS Monitor pipeline.

    Args:
        use_firm_searches: Include Google News searches for target firm names
        use_pe_sources: Include PE-focused news sources
        lookback_hours: How far back to look for articles
        max_workers: Max parallel feed fetches
        verbose: Print progress

    Returns:
        List of filtered, deduplicated articles
    """

    # Build feed list
    feeds = []
    if use_pe_sources:
        feeds.extend(RSS_FEEDS_PE_SOURCES)
    if use_firm_searches:
        feeds.extend(generate_firm_search_feeds())

    if verbose:
        print(f"Fetching from {len(feeds)} RSS feeds...")

    # Step 1: Fetch all feeds in parallel
    all_articles = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_single_feed, url, lookback_hours) for url in feeds]
        for future in as_completed(futures):
            all_articles.extend(future.result())

    if verbose:
        print(f"  Fetched {len(all_articles)} articles")

    # Step 2: URL deduplication
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        url = article["link"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)

    if verbose:
        print(f"  After URL dedup: {len(unique_articles)}")

    # Step 3: Filter to target account mentions
    target_matched = []
    for article in unique_articles:
        text = f"{article['title']} {article['summary']}"
        matches = find_target_accounts(text)
        if matches:
            article['target_accounts'] = ', '.join(set(m[1] for m in matches))
            target_matched.append(article)

    if verbose:
        print(f"  After target account filter: {len(target_matched)}")

    # Step 4: Filter out Asia/Oceania
    geo_filtered = []
    for article in target_matched:
        is_excluded, _ = is_asia_oceania(article['title'])
        if not is_excluded:
            geo_filtered.append(article)

    if verbose:
        print(f"  After Asia/Oceania filter: {len(geo_filtered)}")

    # Step 5: Filter out-of-scope
    scope_filtered = []
    for article in geo_filtered:
        is_excluded, _, _ = is_out_of_scope(article['title'])
        if not is_excluded:
            scope_filtered.append(article)

    if verbose:
        print(f"  After scope filter: {len(scope_filtered)}")

    # Step 6: Content deduplication
    final_articles = dedupe_by_content(scope_filtered)

    if verbose:
        print(f"  After content dedup: {len(final_articles)}")

    return final_articles


def export_to_csv(articles: list[dict], filename: str):
    """Export articles to CSV"""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Source', 'Published', 'Target Accounts', 'Link'])
        for a in articles:
            writer.writerow([
                a.get('title', ''),
                a.get('source', ''),
                a.get('published', ''),
                a.get('target_accounts', ''),
                a.get('link', '')
            ])


# =============================================================================
# HUBSPOT INTEGRATION
# =============================================================================



def hubspot_create_note(company_id: str, article: dict) -> bool:
    """
    Create a Note on a HubSpot Company with article details.
    Returns True if successful.
    """
    if not HUBSPOT_API_KEY:
        return False

    url = "https://api.hubapi.com/crm/v3/objects/notes"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    # Format note body with HTML for clickable link
    title = article.get('title', 'Untitled')
    source = article.get('source', 'Unknown')
    published = article.get('published', '')
    link = article.get('link', '')
    all_firms = article.get('target_accounts', '')

    # Use HTML formatting with clickable link
    note_body = f"""📰 DEAL INTELLIGENCE<br><br>
<strong>{title}</strong><br><br>
Source: {source}<br>
Date: {published}<br>
Firms mentioned: {all_firms}<br><br>
<a href="{link}">Read Article</a>"""

    payload = {
        "properties": {
            "hs_timestamp": datetime.now().isoformat() + "Z",
            "hs_note_body": note_body
        },
        "associations": [{
            "to": {"id": company_id},
            "types": [{
                "associationCategory": "HUBSPOT_DEFINED",
                "associationTypeId": 190  # Note to Company
            }]
        }]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10, verify=False)
        return response.status_code == 201
    except Exception:
        return False


def write_to_hubspot(articles: list[dict], verbose: bool = True) -> dict:
    """
    Write articles to HubSpot as Notes on Company records.
    Uses pre-cached company IDs from target_accounts.py.

    Returns dict with counts: {written, skipped_no_match, errors}
    """
    if not HUBSPOT_API_KEY:
        if verbose:
            print("  HubSpot API key not configured - skipping")
        return {"written": 0, "skipped_no_match": 0, "errors": 0}

    stats = {"written": 0, "skipped_no_match": 0, "errors": 0}

    for article in articles:
        firms = article.get('target_accounts', '').split(', ')

        for firm in firms:
            firm = firm.strip()
            if not firm:
                continue

            # Get company ID from cached lookup
            company_id = get_company_id(firm)

            if not company_id:
                stats["skipped_no_match"] += 1
                if verbose:
                    print(f"    No HubSpot match for: {firm}")
                continue

            # Create note
            success = hubspot_create_note(company_id, article)
            time.sleep(0.1)  # Rate limiting

            if success:
                stats["written"] += 1
                if verbose:
                    print(f"    ✓ Added note to {firm}")
            else:
                stats["errors"] += 1
                if verbose:
                    print(f"    ✗ Failed to add note to {firm}")

    return stats


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RSS Monitor - Deal Flow Agent")
    parser.add_argument("--no-hubspot", action="store_true", help="Skip HubSpot integration")
    parser.add_argument("--hours", type=int, default=24, help="Lookback hours (default: 24)")
    parser.add_argument("--pe-feeds-only", action="store_true", help="Only fetch PE firm direct RSS feeds")
    args = parser.parse_args()

    print("=" * 70)
    print("RSS Monitor - Deal Flow Agent")
    print("=" * 70)
    print()

    all_articles = []

    # 1. General RSS pipeline (news sources + Google News)
    if not args.pe_feeds_only:
        articles = run_pipeline(
            use_firm_searches=True,
            use_pe_sources=True,
            lookback_hours=args.hours,
            verbose=True
        )
        all_articles.extend(articles)
        print(f"\nGeneral RSS: {len(articles)} articles")

    # 2. PE Firm direct RSS feeds (filtered for deals)
    pe_articles = fetch_pe_firm_rss_articles(
        lookback_hours=args.hours,
        verbose=True
    )
    all_articles.extend(pe_articles)

    print()
    print(f"FINAL: {len(all_articles)} total articles")
    print(f"  - General RSS: {len(all_articles) - len(pe_articles)}")
    print(f"  - PE firm feeds (deal-filtered): {len(pe_articles)}")
    print()

    # Export to CSV
    export_to_csv(all_articles, 'rss_monitor_output.csv')
    print(f"Exported to: rss_monitor_output.csv")

    # Write to HubSpot
    if not args.no_hubspot:
        print()
        print("Writing to HubSpot...")
        stats = write_to_hubspot(all_articles, verbose=True)
        print()
        print(f"HubSpot: {stats['written']} notes created, "
              f"{stats['skipped_no_match']} no match, "
              f"{stats['errors']} errors")

    # Show sample
    print()
    print("Sample articles:")
    for i, a in enumerate(all_articles[:10], 1):
        print(f"  {i}. [{a['target_accounts']}] {a['title'][:60]}")
