"""
SEC Filing Monitor
Monitors SEC EDGAR for carve-out signals:
- 8-K filings: strategic review, sale process, divestiture announcements
- 13D/13G filings: activist PE accumulation
- S-4 filings: spin-off registration statements

Uses SEC EDGAR API (free, no authentication required)
Filters to companies with market cap >$400M
"""

import re
import requests
from datetime import datetime, timedelta
from typing import Optional
import time

# SEC EDGAR API endpoints
EDGAR_BASE = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FILINGS = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_FULL_TEXT = "https://efts.sec.gov/LATEST/search-index"

# User agent required by SEC (they block generic requests)
SEC_HEADERS = {
    "User-Agent": "Larkhill & Company paul.ennew@larkhill.co",
    "Accept": "application/json",
}

# Minimum market cap filter (in millions)
MIN_MARKET_CAP_M = 400

# Cache for market cap lookups (avoid repeated API calls)
_market_cap_cache = {}


def get_market_cap(ticker: str) -> Optional[float]:
    """
    Get market cap for a ticker.
    Uses yfinance library if available, otherwise returns None.
    Returns market cap in millions, or None if not found.
    """
    if not ticker:
        return None
    
    # Clean ticker
    ticker = ticker.strip().upper()
    
    # Check cache
    if ticker in _market_cap_cache:
        return _market_cap_cache[ticker]
    
    try:
        # Try using yfinance if available
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info
        market_cap = info.get('marketCap')
        
        if market_cap:
            market_cap_m = market_cap / 1_000_000
            _market_cap_cache[ticker] = market_cap_m
            return market_cap_m
            
    except ImportError:
        # yfinance not installed - skip market cap filtering
        pass
    except Exception as e:
        pass
    
    _market_cap_cache[ticker] = None
    return None


def get_market_cap_batch(tickers: list) -> dict:
    """
    Get market caps for multiple tickers in batch.
    More efficient than individual lookups.
    Returns dict of ticker -> market_cap_m
    """
    results = {}
    
    # Check cache first
    uncached = []
    for ticker in tickers:
        ticker = ticker.strip().upper() if ticker else None
        if not ticker:
            continue
        if ticker in _market_cap_cache:
            results[ticker] = _market_cap_cache[ticker]
        else:
            uncached.append(ticker)
    
    if not uncached:
        return results
    
    try:
        import yfinance as yf
        
        # Batch download
        tickers_str = " ".join(uncached)
        data = yf.download(tickers_str, period="1d", progress=False, show_errors=False)
        
        for ticker in uncached:
            try:
                stock = yf.Ticker(ticker)
                info = stock.fast_info
                market_cap = getattr(info, 'market_cap', None)
                
                if market_cap:
                    market_cap_m = market_cap / 1_000_000
                    _market_cap_cache[ticker] = market_cap_m
                    results[ticker] = market_cap_m
                else:
                    _market_cap_cache[ticker] = None
                    results[ticker] = None
            except:
                _market_cap_cache[ticker] = None
                results[ticker] = None
                
    except ImportError:
        # yfinance not installed
        for ticker in uncached:
            _market_cap_cache[ticker] = None
            results[ticker] = None
    except Exception as e:
        for ticker in uncached:
            _market_cap_cache[ticker] = None
            results[ticker] = None
    
    return results


def extract_ticker_from_company(company_name: str) -> Optional[str]:
    """
    Extract ticker symbol from SEC company name.
    SEC format often includes ticker in parentheses: "Company Name (TICK)"
    """
    if not company_name:
        return None
    
    # Look for ticker in parentheses
    import re
    match = re.search(r'\(([A-Z]{1,5})\)', company_name)
    if match:
        return match.group(1)
    
    # Look for ticker pattern at end: "Company Name  (TICK, TICKW)"
    match = re.search(r'\(([A-Z]{1,5})(?:,|\))', company_name)
    if match:
        return match.group(1)
    
    return None


def passes_market_cap_filter(company_name: str, min_cap_m: float = MIN_MARKET_CAP_M) -> tuple[bool, Optional[float]]:
    """
    Check if company passes market cap filter.
    
    Returns:
        (passes: bool, market_cap_m: Optional[float])
    """
    ticker = extract_ticker_from_company(company_name)
    
    if not ticker:
        # Can't determine market cap without ticker - allow through with warning
        return True, None
    
    market_cap_m = get_market_cap(ticker)
    
    if market_cap_m is None:
        # Can't get market cap - allow through with warning
        return True, None
    
    return market_cap_m >= min_cap_m, market_cap_m

# ==========================================================
# 8-K FILING KEYWORDS (Strategic Review / Divestiture)
# ==========================================================
FILING_8K_KEYWORDS = [
    # Strategic review signals
    "strategic review",
    "strategic alternatives",
    "exploring alternatives",
    "evaluating strategic",
    "exploring options",
    "exploring a sale",
    "formal sale process",
    
    # Divestiture signals
    "divestiture",
    "divest",
    "spin-off",
    "spinoff",
    "separation",
    "carve-out",
    "carve out",
    
    # Sale process signals
    "sale of",
    "sale process",
    "agreement to sell",
    "definitive agreement",
    "binding agreement",
    
    # Adviser appointment
    "retained",
    "engaged",
    "financial adviser",
    "financial advisor",
    "investment bank",
]

# ==========================================================
# 13D/13G KEYWORDS (Activist Involvement)
# ==========================================================
FILING_13D_KEYWORDS = [
    # Activist intent signals
    "strategic alternatives",
    "strategic review",
    "unlock value",
    "unlocking value",
    "maximize shareholder value",
    "operational improvements",
    "board representation",
    "change in control",
    "sale of the company",
    "spin-off",
    "divestiture",
    "break-up",
    "break up",
]

# Target PE firms to watch for 13D filings
ACTIVIST_PE_FIRMS = {
    "Elliott Management",
    "Elliott Investment Management",
    "Starboard Value",
    "ValueAct",
    "Third Point",
    "Icahn",
    "Carl Icahn",
    "Trian Partners",
    "Trian Fund",
    "JANA Partners",
    "Sachem Head",
    "Engine No. 1",
    "Engaged Capital",
    "Land & Buildings",
    "Legion Partners",
    "Ancora",
    "Barington Capital",
}


def search_edgar_filings(
    form_types: list,
    keywords: list,
    days_back: int = 14,
    max_results: int = 100
) -> list:
    """
    Search SEC EDGAR for filings matching criteria.
    
    Args:
        form_types: List of form types (e.g., ["8-K", "8-K/A"])
        keywords: List of keywords to search for
        days_back: Look back N days
        max_results: Maximum results to return
    
    Returns:
        List of filing dicts with metadata
    """
    filings = []
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    # Build search query
    keyword_query = " OR ".join(f'"{kw}"' for kw in keywords[:10])  # Limit keywords
    form_query = " OR ".join(f'formType:"{ft}"' for ft in form_types)
    
    # Use SEC full-text search API
    search_url = "https://efts.sec.gov/LATEST/search-index"
    
    params = {
        "q": keyword_query,
        "dateRange": "custom",
        "startdt": start_date.strftime("%Y-%m-%d"),
        "enddt": end_date.strftime("%Y-%m-%d"),
        "forms": ",".join(form_types),
        "from": 0,
        "size": max_results,
    }
    
    try:
        # SEC requires specific endpoint for full-text search
        api_url = f"https://efts.sec.gov/LATEST/search-index?q={keyword_query}&dateRange=custom&startdt={start_date.strftime('%Y-%m-%d')}&enddt={end_date.strftime('%Y-%m-%d')}&forms={','.join(form_types)}"
        
        response = requests.get(
            api_url,
            headers=SEC_HEADERS,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            hits = data.get("hits", {}).get("hits", [])
            
            for hit in hits:
                source = hit.get("_source", {})
                filing = {
                    "cik": source.get("cik"),
                    "company": source.get("display_names", ["Unknown"])[0] if source.get("display_names") else "Unknown",
                    "form_type": source.get("form"),
                    "filed_date": source.get("file_date"),
                    "accession_number": source.get("adsh"),
                    "description": source.get("file_description", ""),
                }
                
                # Build filing URL
                if filing["cik"] and filing["accession_number"]:
                    cik_padded = str(filing["cik"]).zfill(10)
                    accession_clean = filing["accession_number"].replace("-", "")
                    filing["url"] = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_clean}/{filing['accession_number']}.txt"
                    filing["html_url"] = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={filing['cik']}&type={filing['form_type']}&dateb=&owner=include&count=40"
                
                filings.append(filing)
        else:
            print(f"  SEC API returned {response.status_code}")
            
    except Exception as e:
        print(f"  Error searching SEC EDGAR: {e}")
    
    return filings


def fetch_8k_signals(days_back: int = 14) -> list:
    """
    Fetch 8-K filings indicating strategic review or divestiture.
    Filters to companies with market cap >= $400M.
    
    Returns list of signal dicts.
    """
    print(f"\nSearching SEC 8-K filings (last {days_back} days)...")
    
    signals = []
    
    # Search for relevant 8-K filings
    filings = search_edgar_filings(
        form_types=["8-K", "8-K/A"],
        keywords=FILING_8K_KEYWORDS,
        days_back=days_back,
        max_results=100
    )
    
    print(f"  Found {len(filings)} potentially relevant 8-K filings")
    
    filtered_count = 0
    for filing in filings:
        company = filing.get("company", "Unknown")
        
        # Apply market cap filter
        passes, market_cap_m = passes_market_cap_filter(company, MIN_MARKET_CAP_M)
        
        if not passes:
            filtered_count += 1
            continue
        
        signal = {
            "company": company,
            "signal_type": determine_8k_signal_type(filing),
            "source": f"SEC 8-K ({filing.get('filed_date', 'Unknown date')})",
            "link": filing.get("html_url", ""),
            "filing_date": filing.get("filed_date"),
            "form_type": filing.get("form_type"),
            "description": filing.get("description", ""),
            "market_cap_m": market_cap_m,
        }
        signals.append(signal)
    
    if filtered_count > 0:
        print(f"  Filtered {filtered_count} companies below ${MIN_MARKET_CAP_M}M market cap")
    
    return signals


def determine_8k_signal_type(filing: dict) -> str:
    """Determine signal type from 8-K filing content"""
    desc = filing.get("description", "").lower()
    
    if any(kw in desc for kw in ["definitive agreement", "binding agreement", "agreement to sell"]):
        return "Definitive Agreement"
    elif any(kw in desc for kw in ["strategic review", "strategic alternatives", "exploring"]):
        return "Strategic Review"
    elif any(kw in desc for kw in ["retained", "engaged", "financial adviser", "financial advisor"]):
        return "Adviser Appointed"
    elif any(kw in desc for kw in ["divestiture", "spin-off", "separation"]):
        return "Strategic Review"
    
    return "Strategic Review"  # Default


def fetch_13d_signals(days_back: int = 30) -> list:
    """
    Fetch 13D/13G filings from activist PE firms.
    
    Returns list of signal dicts indicating activist involvement.
    """
    print(f"\nSearching SEC 13D/13G filings (last {days_back} days)...")
    
    signals = []
    
    # Search for 13D filings with activist keywords
    filings = search_edgar_filings(
        form_types=["SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"],
        keywords=list(ACTIVIST_PE_FIRMS)[:10],  # Search by firm name
        days_back=days_back,
        max_results=50
    )
    
    print(f"  Found {len(filings)} potentially relevant 13D/13G filings")
    
    for filing in filings:
        # Check if filer is an activist PE firm
        company = filing.get("company", "")
        is_activist = any(firm.lower() in company.lower() for firm in ACTIVIST_PE_FIRMS)
        
        if is_activist:
            signal = {
                "company": company,
                "signal_type": "PE Interest",
                "source": f"SEC {filing.get('form_type', '13D')} ({filing.get('filed_date', 'Unknown')})",
                "link": filing.get("html_url", ""),
                "filing_date": filing.get("filed_date"),
                "form_type": filing.get("form_type"),
                "activist_firm": company,
                "notes": "Activist PE position - potential catalyst for strategic review",
            }
            signals.append(signal)
    
    return signals


def fetch_s4_signals(days_back: int = 30) -> list:
    """
    Fetch S-4 filings indicating spin-offs.
    Filters to companies with market cap >= $400M.
    
    Returns list of signal dicts.
    """
    print(f"\nSearching SEC S-4 filings (last {days_back} days)...")
    
    signals = []
    
    # S-4 is used for spin-off registrations
    filings = search_edgar_filings(
        form_types=["S-4", "S-4/A"],
        keywords=["spin-off", "spinoff", "separation", "distribution"],
        days_back=days_back,
        max_results=30
    )
    
    print(f"  Found {len(filings)} potentially relevant S-4 filings")
    
    filtered_count = 0
    for filing in filings:
        company = filing.get("company", "Unknown")
        
        # Apply market cap filter
        passes, market_cap_m = passes_market_cap_filter(company, MIN_MARKET_CAP_M)
        
        if not passes:
            filtered_count += 1
            continue
        
        signal = {
            "company": company,
            "signal_type": "Strategic Review",  # S-4 indicates spin-off in progress
            "source": f"SEC S-4 ({filing.get('filed_date', 'Unknown')})",
            "link": filing.get("html_url", ""),
            "filing_date": filing.get("filed_date"),
            "form_type": filing.get("form_type"),
            "notes": "Spin-off registration statement filed",
            "market_cap_m": market_cap_m,
        }
        signals.append(signal)
    
    if filtered_count > 0:
        print(f"  Filtered {filtered_count} companies below ${MIN_MARKET_CAP_M}M market cap")
    
    return signals


def fetch_all_sec_signals(days_back: int = 14) -> list:
    """
    Fetch all SEC filing signals.
    
    Args:
        days_back: Look back N days
    
    Returns:
        Combined list of all SEC-derived signals
    """
    all_signals = []
    
    # 8-K filings (strategic review, divestiture)
    signals_8k = fetch_8k_signals(days_back=days_back)
    all_signals.extend(signals_8k)
    
    # Add delay to avoid rate limiting
    time.sleep(1)
    
    # 13D/13G filings (activist involvement)
    signals_13d = fetch_13d_signals(days_back=days_back * 2)  # Look back further for activist
    all_signals.extend(signals_13d)
    
    time.sleep(1)
    
    # S-4 filings (spin-offs)
    signals_s4 = fetch_s4_signals(days_back=days_back * 2)
    all_signals.extend(signals_s4)
    
    print(f"\nTotal SEC signals: {len(all_signals)}")
    
    return all_signals


def format_for_claude_analysis(signals: list) -> list:
    """
    Format SEC signals for Claude analysis.
    Returns list of article-like dicts compatible with agent.py
    """
    articles = []
    
    for sig in signals:
        article = {
            "title": f"{sig.get('company', 'Unknown')} - {sig.get('form_type', 'SEC Filing')}",
            "link": sig.get("link", ""),
            "summary": f"{sig.get('company', '')} {sig.get('form_type', '')} filing: {sig.get('notes', sig.get('description', ''))}",
            "published": sig.get("filing_date", ""),
            "source": sig.get("source", "SEC EDGAR"),
            # Pre-populated hints for Claude
            "_signal_type_hint": sig.get("signal_type"),
            "_company_hint": sig.get("company"),
        }
        articles.append(article)
    
    return articles


if __name__ == "__main__":
    # Test run
    print("SEC Filing Monitor - Test Run")
    print("=" * 50)
    
    signals = fetch_all_sec_signals(days_back=14)
    
    print("\n" + "=" * 50)
    print("Sample signals:")
    for sig in signals[:10]:
        print(f"\n{sig.get('company')}: {sig.get('signal_type')}")
        print(f"  Source: {sig.get('source')}")
        if sig.get('notes'):
            print(f"  Notes: {sig.get('notes')}")
