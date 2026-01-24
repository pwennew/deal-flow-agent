"""
Target PE Accounts for Deal Flow Agent filtering
Filters Tier 2-4 deals to target PE firms only (no strategic buyers)
"""

TARGET_PE_FIRMS = {
    "ABRY Partners",
    "ACON Investments",
    "Actis",
    "Advent International",
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

# Common variations for matching
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
    "summit": "Summit Partners",
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


def normalize_firm_name(name: str) -> str:
    """Normalize firm name for comparison"""
    if not name:
        return ""
    n = name.lower().strip()
    for suffix in [" llc", " lp", " ltd", " inc", " partners", " capital", " group", 
                   " management", " advisers", " advisors", " private equity", " equity"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def is_target_pe_firm(pe_buyer: str) -> bool:
    """Check if PE buyer is in target accounts list"""
    if not pe_buyer:
        return False
    
    pe_lower = pe_buyer.lower().strip()
    
    # Check aliases
    if pe_lower in FIRM_ALIASES:
        return True
    
    pe_normalized = normalize_firm_name(pe_buyer)
    
    for target in TARGET_PE_FIRMS:
        target_normalized = normalize_firm_name(target)
        if pe_normalized == target_normalized:
            return True
        if len(pe_normalized) > 3 and len(target_normalized) > 3:
            if pe_normalized in target_normalized or target_normalized in pe_normalized:
                return True
    
    return False


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
    
    # Must be target PE firm
    if not is_target_pe_firm(pe_buyer):
        return False, f"Non-target PE: {pe_buyer}"
    
    # Must be valid geography
    if not is_valid_geography(geography):
        return False, f"Invalid geography: {geography}"
    
    return True, f"Target PE + valid geo: {pe_buyer}"
