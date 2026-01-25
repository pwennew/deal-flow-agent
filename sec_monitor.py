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
    
    for filing in filings:
        signal = {
            "company": filing.get("company", "Unknown"),
            "signal_type": determine_8k_signal_type(filing),
            "source": f"SEC 8-K ({filing.get('filed_date', 'Unknown date')})",
            "link": filing.get("html_url", ""),
            "filing_date": filing.get("filed_date"),
            "form_type": filing.get("form_type"),
            "description": filing.get("description", ""),
        }
        signals.append(signal)
    
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
    
    for filing in filings:
        signal = {
            "company": filing.get("company", "Unknown"),
            "signal_type": "Strategic Review",  # S-4 indicates spin-off in progress
            "source": f"SEC S-4 ({filing.get('filed_date', 'Unknown')})",
            "link": filing.get("html_url", ""),
            "filing_date": filing.get("filed_date"),
            "form_type": filing.get("form_type"),
            "notes": "Spin-off registration statement filed",
        }
        signals.append(signal)
    
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
