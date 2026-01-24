#!/usr/bin/env python3
"""
Deal Flow Agent v3.11 - Web Search Results Processor
Processes web search results to extract PE carve-out/divestiture signals
"""

import csv
import json
from datetime import datetime
from typing import List, Dict, Optional

# Target PE Firms List (130 firms)
TARGET_PE_FIRMS = [
    # Tier 1 - Carve-out Specialists
    "KPS Capital Partners", "KPS Capital", "KPS",
    "AURELIUS Group", "AURELIUS", "Aurelius",
    "Platinum Equity",
    "Clayton Dubilier & Rice", "Clayton, Dubilier & Rice", "CD&R", "CDR",
    "Cinven",
    "One Rock Capital Partners", "One Rock Capital", "One Rock",
    "American Industrial Partners", "AIP",
    "Atlas Holdings",
    "SK Capital Partners", "SK Capital",
    "Advent International", "Advent",
    "OpenGate Capital",
    "Sterling Group", "The Sterling Group",
    "Stellex Capital", "Stellex Capital Management",
    "H.I.G. Capital", "HIG Capital", "H.I.G.", "HIG",
    "Inflexion", "Inflexion Private Equity",

    # Tier 2 - Large Cap with Carve-out Activity
    "Carlyle Group", "Carlyle", "The Carlyle Group",
    "CVC Capital Partners", "CVC Capital", "CVC",
    "PAI Partners", "PAI",
    "Bain Capital",
    "KKR", "Kohlberg Kravis Roberts",
    "American Securities",
    "Olympus Partners",
    "Triton Partners", "Triton",
    "Blackstone", "The Blackstone Group",
    "Apollo Global Management", "Apollo Global", "Apollo",

    # Tier 3 - Active PE Buyers
    "TPG", "TPG Capital", "Texas Pacific Group",
    "Warburg Pincus",
    "Vista Equity Partners", "Vista Equity",
    "Thoma Bravo",
    "Silver Lake", "Silver Lake Partners",
    "EQT", "EQT Partners",
    "Permira", "Permira Advisers",
    "BC Partners",
    "Apax Partners", "Apax",
    "Bridgepoint", "Bridgepoint Group",
    "TDR Capital",
    "Nordic Capital",
    "Hg", "Hg Capital", "HgCapital",
    "Montagu Private Equity", "Montagu",
    "Intermediate Capital Group", "ICG",
    "Investcorp",
    "General Atlantic",
    "Leonard Green & Partners", "Leonard Green",
    "Hellman & Friedman", "H&F",
    "Providence Equity Partners", "Providence Equity",
    "Welsh Carson Anderson & Stowe", "WCAS", "Welsh Carson",
    "Summit Partners",
    "TA Associates",
    "GTCR",
    "Madison Dearborn Partners", "Madison Dearborn",
    "Roark Capital", "Roark Capital Group",
    "Insight Partners",
    "Francisco Partners",
    "Clearlake Capital", "Clearlake Capital Group",
    "Genstar Capital",
    "Veritas Capital",
    "Kelso & Company", "Kelso",
    "Court Square Capital Partners", "Court Square",
    "Charterhouse Capital Partners", "Charterhouse",
    "Searchlight Capital Partners", "Searchlight Capital",
    "Sun Capital Partners", "Sun Capital",
    "Cerberus Capital Management", "Cerberus",
    "Centerbridge Partners", "Centerbridge",
    "Golden Gate Capital",
    "New Mountain Capital",
    "Symphony Technology Group", "STG",
    "Thomas H. Lee Partners", "THL Partners", "THL",
    "Berkshire Partners",
    "Harvest Partners",
    "Gryphon Investors",
    "Arsenal Capital Partners", "Arsenal Capital",
    "The Jordan Company", "Jordan Company",
    "Littlejohn & Co", "Littlejohn",
    "MidOcean Partners", "MidOcean",
    "Blue Point Capital Partners", "Blue Point Capital",
    "Wind Point Partners", "Wind Point",
    "Mason Wells",
    "Arbor Investments",
    "Frontenac Company", "Frontenac",
    "Chicago Pacific Founders",
    "ONCAP",
    "Sentinel Capital Partners", "Sentinel Capital",
    "Kohlberg & Company", "Kohlberg",
    "The Riverside Company", "Riverside Company", "Riverside",
    "Graham Partners",
    "Huron Capital",
    "Gauge Capital",
    "Ridgemont Equity Partners", "Ridgemont",
    "Shore Capital Partners", "Shore Capital",
    "LaSalle Capital", "LaSalle Capital Group",
    "Norwest Equity Partners",
    "Pamlico Capital",
    "Spell Capital Partners", "Spell Capital",
    "Svoboda Capital Partners", "Svoboda Capital",
    "Incline Equity Partners", "Incline Equity",
    "Chicago Growth Partners",
    "BV Investment Partners",
    "Saw Mill Capital",
    "LLR Partners",
    "NewSpring Capital",
    "Signal Hill Equity Partners", "Signal Hill",
    "Water Street Healthcare Partners", "Water Street",
    "Frazier Healthcare Partners", "Frazier Healthcare",
    "Enhanced Healthcare Partners",
    "Trinity Hunt Partners", "Trinity Hunt",
    "Pharos Capital Group", "Pharos Capital",
    "Sheridan Capital Partners", "Sheridan Capital",
    "MBF Healthcare Partners",
    "Webster Capital",
    "WindRose Health Investors", "WindRose",
    "Revelstoke Capital Partners", "Revelstoke Capital",
    "Waud Capital Partners", "Waud Capital",
    "PPC Enterprises",
    "Chicago Pacific", "Chicago Pacific Founders",
    "Audax Private Equity", "Audax",
    "Lion Equity Partners", "Lion Equity",
    "Mill Point Capital",
    "Hypax",
    "Brookfield", "Brookfield Asset Management",
    "Corvex Private Equity", "Corvex",

    # European Specialists
    "3i Group", "3i",
    "Oakley Capital",
    "Equistone Partners Europe", "Equistone",
    "IK Partners", "IK Investment Partners",
    "Astorg",
    "Ergon Capital", "Ergon Capital Partners",
    "Bregal", "Bregal Investments",
    "Investindustrial",
    "Exponent Private Equity", "Exponent",
    "Bowmark Capital", "Bowmark",
    "LDC", "Lloyds Development Capital",
    "NorthEdge Capital", "NorthEdge",
    "August Equity",
    "Foresight Group", "Foresight",
    "BGF", "Business Growth Fund",
    "Gresham House",
    "Agilitas Private Equity", "Agilitas",
    "Herkules Capital",
    "Summa Equity",
    "Verdane",
    "Procuritas", "Procuritas Partners"
]


def normalize_firm_name(name: Optional[str]) -> str:
    """Normalize PE firm name for matching."""
    if not name:
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    suffixes = [' llc', ' lp', ' inc', ' ltd', ' limited', ' partners',
                ' capital', ' group', ' management', ' equity', ' fund',
                ' private equity', ' holdings']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    # Remove punctuation
    name = name.replace(',', '').replace('&', 'and').replace('.', '')
    return name


def is_target_pe_firm(pe_buyer: Optional[str]) -> bool:
    """Check if PE buyer is on the target list."""
    if not pe_buyer:
        return False

    buyer_normalized = normalize_firm_name(pe_buyer)

    for target in TARGET_PE_FIRMS:
        target_normalized = normalize_firm_name(target)
        # Exact match or substring match
        if buyer_normalized == target_normalized:
            return True
        if buyer_normalized in target_normalized or target_normalized in buyer_normalized:
            return True

    return False


def passes_post_filters(signal: Dict) -> bool:
    """
    Returns True if signal should be included, False if filtered out.
    Replicates the post-filter logic from the agent.
    """

    # Rule 1: Tier 2-4 signals (Definitive Agreement, Deal Completed)
    # MUST have pe_buyer from TARGET_PE_FIRMS list + valid geography
    if signal['signal_type'] in ['Definitive Agreement', 'Deal Completed']:
        if not signal.get('pe_buyer'):
            return False  # Must have named PE buyer
        if not is_target_pe_firm(signal['pe_buyer']):
            return False  # PE buyer must be on target list
        if signal.get('geography') not in ['US', 'UK', 'Europe', 'Global']:
            return False  # Must be target geography
        return True

    # Rule 2: Geography filter for all other signals
    excluded_geos = ['China', 'Asia', 'Latin America', 'LatAm',
                     'Middle East', 'Africa', 'Australia', 'India']
    if signal.get('geography') in excluded_geos:
        return False

    # Rule 3: Exclude whole company sales without PE buyer
    if not signal.get('division') and not signal.get('pe_buyer'):
        # Whole company with no PE buyer = likely strategic M&A
        return False

    # Rule 4: Exclude academic/government entities
    academic_keywords = ['university', 'college', 'institute', 'research center',
                        'government', 'ministry', 'department of', 'federal',
                        'state of', 'county', 'municipal']
    company_lower = signal.get('company', '').lower()
    if any(kw in company_lower for kw in academic_keywords):
        return False

    # Rule 5: Strategic Review with no PE buyer requires high confidence
    if signal.get('signal_type') == 'Strategic Review':
        if not signal.get('pe_buyer') and not signal.get('likely_buyers'):
            if signal.get('confidence') != 'High':
                return False

    return True


def normalize_title(title: Optional[str]) -> str:
    """Normalize company/division name for deduplication."""
    if not title:
        return ""
    title = title.lower().strip()
    # Remove common suffixes
    suffixes = [' inc', ' inc.', ' corp', ' corp.', ' corporation',
                ' llc', ' ltd', ' limited', ' plc', ' se', ' ag', ' gmbh',
                ' sa', ' nv', ' bv', ' holdings', ' group']
    for suffix in suffixes:
        if title.endswith(suffix):
            title = title[:-len(suffix)].strip()
    # Remove PE firm names from title
    for firm in TARGET_PE_FIRMS:
        firm_lower = firm.lower()
        title = title.replace(firm_lower, '').strip()
    return title


def extract_company_key(signal: Dict) -> str:
    """Extract key identifier for deduplication."""
    company = normalize_title(signal.get('company', ''))
    division = normalize_title(signal.get('division', ''))
    return f"{company}|{division}"


def is_duplicate(new_signal: Dict, existing_signals: List[Dict]) -> bool:
    """Check if signal is duplicate of existing."""
    new_key = extract_company_key(new_signal)
    new_company = normalize_title(new_signal.get('company', ''))
    new_division = normalize_title(new_signal.get('division', ''))

    for existing in existing_signals:
        existing_key = extract_company_key(existing)

        # Exact normalized title match
        if new_key == existing_key:
            return True

        # Same company, overlapping division words
        existing_company = normalize_title(existing.get('company', ''))
        existing_division = normalize_title(existing.get('division', ''))

        if new_company == existing_company:
            # Check division word overlap
            new_words = set(new_division.split())
            existing_words = set(existing_division.split())
            if new_words and existing_words:
                overlap = new_words & existing_words
                if len(overlap) >= min(len(new_words), len(existing_words)) * 0.5:
                    return True

    return False


def write_signals_to_csv(signals: List[Dict], output_file: str):
    """Write filtered and deduplicated signals to CSV."""

    # Sort by signal_type priority
    signal_priority = {
        'Deal Completed': 1,
        'Definitive Agreement': 2,
        'PE Bid Submitted': 3,
        'PE In Talks': 4,
        'PE Interest': 5,
        'Adviser Appointed': 6,
        'Strategic Review': 7
    }

    signals_sorted = sorted(signals, key=lambda x: signal_priority.get(x.get('signal_type'), 99))

    fieldnames = [
        'date_identified', 'company', 'division', 'signal_type', 'pe_buyer',
        'likely_buyers', 'size_estimate', 'ev_low_usd_m', 'ev_high_usd_m',
        'sector', 'geography', 'complexity', 'key_quote', 'buyer_intelligence',
        'notes', 'confidence', 'source_url'
    ]

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for signal in signals_sorted:
            # Ensure all fields exist
            row = {field: signal.get(field, '') for field in fieldnames}
            writer.writerow(row)

    print(f"✓ Wrote {len(signals_sorted)} signals to {output_file}")


if __name__ == "__main__":
    # Test the functions
    test_signal = {
        'company': 'Albemarle Corporation',
        'division': 'Ketjen',
        'signal_type': 'Definitive Agreement',
        'pe_buyer': 'KPS Capital Partners',
        'geography': 'US',
        'confidence': 'High'
    }

    print(f"Is target PE firm: {is_target_pe_firm('KPS Capital Partners')}")
    print(f"Passes post filters: {passes_post_filters(test_signal)}")
