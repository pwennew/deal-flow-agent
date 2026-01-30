"""
Target PE Accounts for Deal Flow Agent filtering
Fetches company list from HubSpot, then applies fuzzy matching.
"""

import os
import time
import requests
from rapidfuzz import fuzz, process

# HubSpot API
HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY")

# Fuzzy matching threshold (0-100). 85+ = high confidence match
FUZZY_MATCH_THRESHOLD = 85

# Cache for HubSpot companies: {name: hubspot_id}
_HUBSPOT_COMPANIES = None
_NORMALIZED_TARGETS = None
_COMPANY_IDS = None  # {normalized_name: hubspot_id}


def fetch_hubspot_companies() -> dict[str, str]:
    """
    Fetch all company names and IDs from HubSpot.
    Returns dict of {company_name: hubspot_id}
    """
    if not HUBSPOT_API_KEY:
        print("  Warning: HUBSPOT_API_KEY not set, using empty company list")
        return {}

    companies = {}
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }
    params = {
        "limit": 100,
        "properties": "name"
    }

    after = None
    page = 0

    while True:
        if after:
            params["after"] = after

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30, verify=False)

            if response.status_code != 200:
                print(f"  Warning: HubSpot API returned {response.status_code}")
                break

            data = response.json()
            results = data.get("results", [])

            for company in results:
                name = company.get("properties", {}).get("name")
                company_id = company.get("id")
                if name and company_id:
                    companies[name] = company_id

            # Check for next page
            paging = data.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")

            if not after:
                break

            page += 1
            time.sleep(0.1)  # Rate limiting

        except Exception as e:
            print(f"  Warning: HubSpot API error: {e}")
            break

    return companies


def get_target_firms() -> set[str]:
    """Get target PE firm names from HubSpot (cached)"""
    global _HUBSPOT_COMPANIES, _COMPANY_IDS
    if _HUBSPOT_COMPANIES is None:
        print("  Fetching target accounts from HubSpot...")
        companies_dict = fetch_hubspot_companies()
        _HUBSPOT_COMPANIES = set(companies_dict.keys())
        # Build ID lookup by normalized name
        _COMPANY_IDS = {}
        for name, company_id in companies_dict.items():
            normalized = normalize_firm_name(name)
            _COMPANY_IDS[normalized] = company_id
            _COMPANY_IDS[name] = company_id  # Also store exact name
        print(f"  Loaded {len(_HUBSPOT_COMPANIES)} companies from HubSpot")
    return _HUBSPOT_COMPANIES


def get_company_id(firm_name: str) -> str | None:
    """Get HubSpot company ID for a firm name"""
    global _COMPANY_IDS
    if _COMPANY_IDS is None:
        get_target_firms()  # Initialize cache
    if _COMPANY_IDS is None:
        return None

    # Try exact match first
    if firm_name in _COMPANY_IDS:
        return _COMPANY_IDS[firm_name]

    # Try normalized match
    normalized = normalize_firm_name(firm_name)
    return _COMPANY_IDS.get(normalized)


def refresh_target_firms():
    """Force refresh of HubSpot companies cache"""
    global _HUBSPOT_COMPANIES, _NORMALIZED_TARGETS, _COMPANY_IDS
    _HUBSPOT_COMPANIES = None
    _NORMALIZED_TARGETS = None
    _COMPANY_IDS = None
    return get_target_firms()


# For backwards compatibility
TARGET_PE_FIRMS = set()  # Will be populated dynamically


# Common variations for matching (checked before fuzzy)
# These map common shorthand to likely HubSpot company names
FIRM_ALIASES = {
    "blackstone group": "Blackstone",
    "the blackstone group": "Blackstone",
    "kkr & co": "KKR",
    "carlyle": "Carlyle Group",
    "carlyle group": "Carlyle Group",
    "the carlyle group": "Carlyle Group",
    "apollo": "Apollo Global Management",
    "apollo management": "Apollo Global Management",
    "bain": "Bain Capital",
    "cd&r": "Clayton Dubilier & Rice",
    "cdr": "Clayton Dubilier & Rice",
    "hellman friedman": "Hellman & Friedman",
    "h&f": "Hellman & Friedman",
    "leonard green": "Leonard Green & Partners",
    "lgp": "Leonard Green & Partners",
    "permira": "Permira Advisers",
    "eqt partners": "EQT",
    "cvc": "CVC Capital Partners",
    "tpg capital": "TPG",
    "warburg": "Warburg Pincus",
    "advent": "Advent International",
    "clearlake": "Clearlake Capital Group",
    "platinum": "Platinum Equity",
    "kps": "KPS Capital Partners",
    "kps capital": "KPS Capital Partners",
    "genstar": "Genstar Capital",
    "new mountain": "New Mountain Capital",
    "l catterton": "L Catterton",
    "stone point": "Stone Point Capital",
    "veritas": "Veritas Capital",
    "hig": "H.I.G. Capital",
    "h.i.g.": "H.I.G. Capital",
    "hig capital": "H.I.G. Capital",
    "thoma bravo": "Thoma Bravo",
    "vista equity": "Vista Equity Partners",
    "vista": "Vista Equity Partners",
    "silver lake": "Silver Lake",
    "francisco partners": "Francisco Partners",
    "insight partners": "Insight Partners",
    "american industrial partners": "American Industrial Partners",
    "aip": "American Industrial Partners",
    "one rock": "One Rock Capital Partners",
    "one rock capital": "One Rock Capital Partners",
    "aurelius": "AURELIUS Group",
    "inflexion": "Inflexion",
    "montagu": "Montagu Private Equity",
    "pai": "PAI Partners",
    "pacific equity": "Pacific Equity Partners",
    "pep": "Pacific Equity Partners",
    "audax": "Audax Private Equity",
    "audax pe": "Audax Private Equity",
    "triton": "Triton Partners",
    "apax": "Apax Partners",
    "bc partners": "BC Partners",
    "gtcr": "GTCR",
    "oak hill": "Oak Hill Capital Partners",
    "madison dearborn": "Madison Dearborn Partners",
    "welsh carson": "Welsh Carson Anderson & Stowe",
    "wcas": "Welsh Carson Anderson & Stowe",
    "sun capital": "Sun Capital Partners",
    "sycamore": "Sycamore Partners",
    "roark": "Roark Capital Group",
    "providence": "Providence Equity Partners",
    "golden gate": "Golden Gate Capital",
    "riverside": "Riverside Company",
    "ta": "TA Associates",
    "berkshire": "Berkshire Partners",
    "tcv": "TCV",
    "spectrum": "Spectrum Equity",
    "great hill": "Great Hill Partners",
    "gryphon": "Gryphon Investors",
    "harvest": "Harvest Partners",
    "kelso": "Kelso & Company",
    "kohlberg": "Kohlberg & Company",
    "onex": "Onex",
    "rhone": "Rhône Group",
    "trilantic": "Trilantic Capital Partners",
    "icg": "ICG",
    "intermediate capital": "Intermediate Capital Group",
    "hg capital": "Hg",
    "vitruvian": "Vitruvian Partners",
    "antin": "Antin Infrastructure Partners",
    "eurazeo": "Eurazeo",
    "ardian": "Ardian",
    "cinven": "Cinven",
    "bridgepoint": "Bridgepoint",
    "charterhouse": "Charterhouse Capital Partners",
    "nordic": "Nordic Capital",
    "ik": "IK Partners",
    "ik partners": "IK Partners",
    "tdr": "TDR Capital",
    "sk capital": "SK Capital Partners",
    "arsenal": "Arsenal Capital Partners",
    "water street": "Water Street Healthcare Partners",
    "gi partners": "GI Partners",
    "flexpoint": "Flexpoint Ford",
    "parthenon": "Parthenon Capital",
}


def normalize_firm_name(name: str) -> str:
    """Normalize firm name for comparison"""
    if not name:
        return ""
    n = name.lower().strip()
    # Remove punctuation that causes matching issues
    n = n.replace(".", "").replace("&", "and").replace("-", " ")
    # Strip common suffixes
    for suffix in [" llc", " lp", " ltd", " inc", " partners", " capital", " group",
                   " management", " advisers", " advisors", " private equity", " equity"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def _get_normalized_targets() -> dict[str, str]:
    """Build normalized name -> canonical name mapping (cached)"""
    global _NORMALIZED_TARGETS
    if _NORMALIZED_TARGETS is None:
        _NORMALIZED_TARGETS = {}
        for firm in get_target_firms():
            normalized = normalize_firm_name(firm)
            _NORMALIZED_TARGETS[normalized] = firm
    return _NORMALIZED_TARGETS


def match_pe_firm(pe_buyer: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> tuple[bool, str | None, int]:
    """
    Match PE buyer to target list (from HubSpot) using fuzzy matching.

    Matching hierarchy:
    1. Exact alias match (fastest)
    2. Exact normalized match
    3. Fuzzy match against all targets (handles typos)

    Args:
        pe_buyer: Name of PE firm to check
        threshold: Minimum fuzzy match score (0-100)

    Returns:
        Tuple of (is_match, matched_firm_name, confidence_score)
        If no match: (False, None, 0)
    """
    if not pe_buyer:
        return False, None, 0

    pe_lower = pe_buyer.lower().strip()

    # 1. Check aliases - map to canonical name, then verify in HubSpot
    if pe_lower in FIRM_ALIASES:
        canonical = FIRM_ALIASES[pe_lower]
        # Verify the canonical name exists in HubSpot
        normalized_targets = _get_normalized_targets()
        canonical_normalized = normalize_firm_name(canonical)
        if canonical_normalized in normalized_targets:
            return True, normalized_targets[canonical_normalized], 100
        # If alias target not in HubSpot, try fuzzy match on alias target
        match = process.extractOne(
            canonical_normalized,
            normalized_targets.keys(),
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold
        )
        if match:
            matched_normalized, score, _ = match
            return True, normalized_targets[matched_normalized], int(score)

    # 2. Exact normalized match
    pe_normalized = normalize_firm_name(pe_buyer)
    normalized_targets = _get_normalized_targets()

    if not normalized_targets:
        # No HubSpot companies loaded
        return False, None, 0

    if pe_normalized in normalized_targets:
        return True, normalized_targets[pe_normalized], 100

    # 3. Fuzzy match - try token_set_ratio first
    match = process.extractOne(
        pe_normalized,
        normalized_targets.keys(),
        scorer=fuzz.token_set_ratio,
        score_cutoff=threshold
    )

    if match:
        matched_normalized, score, _ = match
        return True, normalized_targets[matched_normalized], int(score)

    # 4. Try WRatio as fallback
    match = process.extractOne(
        pe_normalized,
        normalized_targets.keys(),
        scorer=fuzz.WRatio,
        score_cutoff=threshold
    )

    if match:
        matched_normalized, score, _ = match
        return True, normalized_targets[matched_normalized], int(score)

    return False, None, 0


def is_target_pe_firm(pe_buyer: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> bool:
    """Check if PE buyer is in target accounts list using fuzzy matching."""
    is_match, _, _ = match_pe_firm(pe_buyer, threshold)
    return is_match
