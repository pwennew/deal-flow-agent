"""
Target PE Accounts for Deal Flow Agent filtering
Filters Tier 2-4 deals to target PE firms only (no strategic buyers)

Uses fuzzy matching (rapidfuzz) for typo tolerance and name variations.
"""

from rapidfuzz import fuzz, process

# Fuzzy matching threshold (0-100). 85+ = high confidence match
FUZZY_MATCH_THRESHOLD = 85

TARGET_PE_FIRMS = {
    "ABRY Partners",
    "ACON Investments",
    "Actis",
    "Advent International",
    "Audax Private Equity",
    "Affinity Equity Partners",
    "Altaris Capital Partners",
    "American Industrial Partners",
    "American Securities",
    "Antin Infrastructure Partners",
    "Apax Partners",
    "Apollo Global Management",
    "Aquiline Capital Partners",
    "Arcline Investment Management",
    "Arcmont Asset Management",
    "Ardian",
    "Arsenal Capital Partners",
    "Astorg",
    "AURELIUS Group",
    "Bain Capital",
    "BC Partners",
    "Berkshire Partners",
    "BGH Capital",
    "Blackstone",
    "Blue Wolf Capital Partners",
    "Bridgepoint",
    "Bruckmann Rosser Sherrill",
    "Carlyle Group",
    "Charterhouse Capital Partners",
    "Cinven",
    "Clayton Dubilier & Rice",
    "Clearlake Capital Group",
    "Court Square",
    "CVC Capital Partners",
    "EagleTree Capital",
    "EQT",
    "Eurazeo",
    "Flexpoint Ford",
    "Forbion",
    "Francisco Partners",
    "FSN Capital",
    "General Atlantic",
    "Genstar Capital",
    "GI Partners",
    "Golden Gate Capital",
    "Great Hill Partners",
    "Gryphon Investors",
    "GTCR",
    "H.I.G. Capital",
    "Harvest Partners",
    "HayFin Capital Management",
    "Hellman & Friedman",
    "HGGC",
    "Housatonic Partners",
    "Hg",
    "ICG",
    "IK Partners",
    "Incline Equity Partners",
    "Inflexion",
    "Insight Partners",
    "Intermediate Capital Group",
    "JMI Equity",
    "K1 Investment Management",
    "Kelso & Company",
    "Kohlberg & Company",
    "KKR",
    "KPS Capital Partners",
    "L Catterton",
    "Lee Equity Partners",
    "Leonard Green & Partners",
    "Levine Leichtman Capital Partners",
    "Livingbridge",
    "LLR Partners",
    "Madison Dearborn Partners",
    "Main Post Partners",
    "MBK Partners",
    "Montagu Private Equity",
    "New Mountain Capital",
    "NewQuest Capital Partners",
    "Nordic Capital",
    "Norvestor",
    "Oak Hill Capital Partners",
    "Odyssey Investment Partners",
    "One Rock Capital Partners",
    "Onex",
    "Owl Rock Capital",
    "PAI Partners",
    "Pacific Equity Partners",
    "Pamplona Capital Management",
    "Parthenon Capital",
    "Peak Rock Capital",
    "Permira Advisers",
    "Platinum Equity",
    "Providence Equity Partners",
    "Quadrant Private Equity",
    "RBC Capital Partners",
    "Resurgens Technology Partners",
    "Reverence Capital Partners",
    "Rhône Group",
    "Ridgemont Equity Partners",
    "Rivean Capital",
    "Riverside Company",
    "Roark Capital Group",
    "SDC Capital Partners",
    "Silver Lake",
    "SK Capital Partners",
    "Snow Phipps Group",
    "Sole Source Capital",
    "Solis Capital Partners",
    "Spectrum Equity",
    "Stone Point Capital",
    "Summit Partners",
    "Sun Capital Partners",
    "Sycamore Partners",
    "TA Associates",
    "TCV",
    "TDR Capital",
    "TH Lee",
    "The Carlyle Group",
    "The Riverside Company",
    "The Sterling Group",
    "Thoma Bravo",
    "Thomas H. Lee Partners",
    "TPG",
    "Trilantic Capital Partners",
    "Triton Partners",
    "Ufenau Capital Partners",
    "Veritas Capital",
    "Victory Park Capital",
    "Vista Equity Partners",
    "Vitruvian Partners",
    "Warburg Pincus",
    "Water Street Healthcare Partners",
    "Webster Equity Partners",
    "Welsh Carson Anderson & Stowe",
    "WindRose Health Investors",
    "Wynnchurch Capital",
}

# Common variations for matching (checked before fuzzy)
FIRM_ALIASES = {
    "blackstone group": "Blackstone",
    "the blackstone group": "Blackstone",
    "kkr & co": "KKR",
    "carlyle": "The Carlyle Group",
    "carlyle group": "The Carlyle Group",
    "the carlyle group": "The Carlyle Group",
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
    "pai partners": "PAI Partners",
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
    "riverside": "The Riverside Company",
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

# Signal types requiring target account filtering
FILTERED_SIGNAL_TYPES = {"Definitive Agreement", "Deal Completed"}

# Valid geographies
VALID_GEOGRAPHIES = {"US", "UK", "Europe", "Global"}

# Pre-build normalized lookup for fuzzy matching
_NORMALIZED_TARGETS = None


def _get_normalized_targets() -> dict[str, str]:
    """Build normalized name -> canonical name mapping (cached)"""
    global _NORMALIZED_TARGETS
    if _NORMALIZED_TARGETS is None:
        _NORMALIZED_TARGETS = {}
        for firm in TARGET_PE_FIRMS:
            normalized = normalize_firm_name(firm)
            _NORMALIZED_TARGETS[normalized] = firm
    return _NORMALIZED_TARGETS


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


def is_target_pe_firm(pe_buyer: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> bool:
    """
    Check if PE buyer is in target accounts list using fuzzy matching.
    
    Matching hierarchy:
    1. Exact alias match (fastest)
    2. Exact normalized match
    3. Fuzzy match against all targets (handles typos)
    
    Args:
        pe_buyer: Name of PE firm to check
        threshold: Minimum fuzzy match score (0-100). Default 85.
    
    Returns:
        True if match found, False otherwise
    """
    if not pe_buyer:
        return False
    
    pe_lower = pe_buyer.lower().strip()
    
    # 1. Check aliases first (exact match on known variations)
    if pe_lower in FIRM_ALIASES:
        return True
    
    # 2. Check exact normalized match
    pe_normalized = normalize_firm_name(pe_buyer)
    normalized_targets = _get_normalized_targets()
    
    if pe_normalized in normalized_targets:
        return True
    
    # 3. Fuzzy match against normalized targets
    # Use token_set_ratio: handles word reordering and partial matches
    # e.g. "KPS Capital Partners" matches "Partners Capital KPS"
    match = process.extractOne(
        pe_normalized,
        normalized_targets.keys(),
        scorer=fuzz.token_set_ratio,
        score_cutoff=threshold
    )
    
    if match:
        return True
    
    # 4. Also try WRatio (weighted ratio) for cases where token_set fails
    # Handles substring scenarios better
    match = process.extractOne(
        pe_normalized,
        normalized_targets.keys(),
        scorer=fuzz.WRatio,
        score_cutoff=threshold
    )
    
    return match is not None


def match_pe_firm(pe_buyer: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> tuple[bool, str | None, int]:
    """
    Match PE buyer to target list and return match details.
    
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
    
    # 1. Check aliases
    if pe_lower in FIRM_ALIASES:
        canonical = FIRM_ALIASES[pe_lower]
        return True, canonical, 100
    
    # 2. Exact normalized match
    pe_normalized = normalize_firm_name(pe_buyer)
    normalized_targets = _get_normalized_targets()
    
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


def is_valid_geography(geography: str) -> bool:
    """Check if geography is US, UK, Europe or Global"""
    if not geography:
        return False
    
    # Handle list
    if isinstance(geography, list):
        geography = geography[0] if geography else ""
    
    # Check each part of comma-separated geography
    geo_parts = [g.strip() for g in geography.split(',')]
    return all(g in VALID_GEOGRAPHIES for g in geo_parts)


def passes_tier2_filter(signal_type: str, pe_buyer: str, geography: str) -> tuple[bool, str]:
    """
    Filter for Tier 2-4 signals (Definitive Agreement, Deal Completed).
    ONLY checks: target PE firm + valid geography.
    No PE buyer = strategic buyer = filter out.
    """
    if signal_type not in FILTERED_SIGNAL_TYPES:
        return True, "Tier 1 signal - standard filters apply"
    
    # Must have PE buyer (no strategic buyers)
    if not pe_buyer or pe_buyer.lower() in ["unknown", "undisclosed", "n/a", "none", "tbd", ""]:
        return False, "No PE buyer (strategic buyer)"
    
    # Must be target PE firm (now with fuzzy matching)
    is_match, matched_firm, confidence = match_pe_firm(pe_buyer)
    if not is_match:
        return False, f"Non-target PE: {pe_buyer}"
    
    # Must be valid geography
    if not is_valid_geography(geography):
        return False, f"Invalid geography: {geography}"
    
    # Include confidence in success message for debugging
    if confidence < 100:
        return True, f"Target PE (fuzzy {confidence}%): {pe_buyer} -> {matched_firm}"
    return True, f"Target PE + valid geo: {matched_firm}"
