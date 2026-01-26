"""
Investment Banks Reference List for Deal Flow Agent
Fortune 1000 Sell-Side M&A Advisory ($400M+ divestitures)

Uses fuzzy matching (rapidfuzz) for maximum signal capture from news articles.
Banks are categorized by tier for prioritization in alerts.

Usage:
    from investment_banks import is_target_bank, match_bank, get_bank_tier
"""

from rapidfuzz import fuzz, process

# Fuzzy matching threshold (0-100). 85+ = high confidence match
FUZZY_MATCH_THRESHOLD = 85

# ==========================================================
# INVESTMENT BANKS BY TIER
# ==========================================================

# Tier 1: Bulge Bracket - Global full-service banks
BULGE_BRACKET = {
    "Goldman Sachs",
    "Morgan Stanley",
    "JPMorgan",
    "Bank of America",
    "Citigroup",
    "Barclays",
    "UBS",
    "Deutsche Bank",
}

# Tier 2: Elite Boutique - M&A advisory focused
ELITE_BOUTIQUE = {
    "Evercore",
    "Lazard",
    "Centerview Partners",
    "Moelis & Company",
    "PJT Partners",
    "Perella Weinberg Partners",
    "Rothschild & Co",
    "Greenhill & Co",
    "Guggenheim Partners",
    "Qatalyst Partners",
    "LionTree",
    "Gordon Dyal & Co",
}

# Tier 3: Upper Middle Market - Strong M&A practices
UPPER_MIDDLE_MARKET = {
    "Jefferies",
    "Houlihan Lokey",
    "William Blair",
    "Harris Williams",
    "Robert W. Baird",
    "Piper Sandler",
    "Raymond James",
    "Lincoln International",
    "Stifel",
    "RBC Capital Markets",
}

# Tier 4: Regional/Specialist - Sector or regional focus
REGIONAL_SPECIALIST = {
    "TD Cowen",
    "KeyBanc Capital Markets",
    "Truist Securities",
    "Oppenheimer",
    "Wells Fargo Securities",
}

# Combined set for quick lookup
INVESTMENT_BANKS = (
    BULGE_BRACKET | 
    ELITE_BOUTIQUE | 
    UPPER_MIDDLE_MARKET | 
    REGIONAL_SPECIALIST
)


# ==========================================================
# ALIASES FOR FUZZY MATCHING
# Common variations, abbreviations, and informal names
# ==========================================================

BANK_ALIASES = {
    # Goldman Sachs
    "goldman": "Goldman Sachs",
    "goldman sachs & co": "Goldman Sachs",
    "gs": "Goldman Sachs",
    
    # Morgan Stanley
    "morgan stanley & co": "Morgan Stanley",
    "ms": "Morgan Stanley",
    
    # JPMorgan
    "jp morgan": "JPMorgan",
    "j.p. morgan": "JPMorgan",
    "jpmorgan chase": "JPMorgan",
    "jpm": "JPMorgan",
    
    # Bank of America
    "bofa": "Bank of America",
    "bofa securities": "Bank of America",
    "bank of america merrill lynch": "Bank of America",
    "baml": "Bank of America",
    "merrill lynch": "Bank of America",
    
    # Citigroup
    "citi": "Citigroup",
    "citibank": "Citigroup",
    "citi global markets": "Citigroup",
    
    # Barclays
    "barclays capital": "Barclays",
    "barclays bank": "Barclays",
    
    # UBS
    "ubs investment bank": "UBS",
    "ubs securities": "UBS",
    
    # Deutsche Bank
    "deutsche": "Deutsche Bank",
    "db": "Deutsche Bank",
    
    # Evercore
    "evercore partners": "Evercore",
    "evercore isi": "Evercore",
    
    # Lazard
    "lazard freres": "Lazard",
    "lazard ltd": "Lazard",
    
    # Centerview
    "centerview": "Centerview Partners",
    
    # Moelis
    "moelis": "Moelis & Company",
    "moelis and company": "Moelis & Company",
    
    # PJT Partners
    "pjt": "PJT Partners",
    "pjt park hill": "PJT Partners",
    
    # Perella Weinberg
    "perella weinberg": "Perella Weinberg Partners",
    "pwp": "Perella Weinberg Partners",
    "perella": "Perella Weinberg Partners",
    
    # Rothschild
    "rothschild": "Rothschild & Co",
    "n m rothschild": "Rothschild & Co",
    "rothschild and co": "Rothschild & Co",
    
    # Greenhill
    "greenhill": "Greenhill & Co",
    
    # Guggenheim
    "guggenheim securities": "Guggenheim Partners",
    "guggenheim": "Guggenheim Partners",
    
    # Qatalyst
    "qatalyst": "Qatalyst Partners",
    
    # LionTree
    "liontree advisors": "LionTree",
    "liontree llc": "LionTree",
    
    # Gordon Dyal
    "gordon dyal": "Gordon Dyal & Co",
    "dyal co": "Gordon Dyal & Co",
    
    # Jefferies
    "jefferies llc": "Jefferies",
    "jefferies group": "Jefferies",
    
    # Houlihan Lokey
    "houlihan": "Houlihan Lokey",
    "hl": "Houlihan Lokey",
    
    # William Blair
    "blair": "William Blair",
    "wm blair": "William Blair",
    
    # Harris Williams
    "harris williams & co": "Harris Williams",
    "hw": "Harris Williams",
    
    # Baird
    "baird": "Robert W. Baird",
    "rw baird": "Robert W. Baird",
    "r.w. baird": "Robert W. Baird",
    
    # Piper Sandler
    "piper": "Piper Sandler",
    "piper jaffray": "Piper Sandler",
    
    # Raymond James
    "rj": "Raymond James",
    "raymond james financial": "Raymond James",
    
    # Lincoln International
    "lincoln": "Lincoln International",
    
    # Stifel
    "stifel nicolaus": "Stifel",
    "stifel financial": "Stifel",
    
    # RBC
    "rbc": "RBC Capital Markets",
    "royal bank of canada": "RBC Capital Markets",
    "rbc cm": "RBC Capital Markets",
    
    # TD Cowen
    "cowen": "TD Cowen",
    "td securities": "TD Cowen",
    "cowen and company": "TD Cowen",
    
    # KeyBanc
    "keybanc": "KeyBanc Capital Markets",
    "key banc": "KeyBanc Capital Markets",
    "keybank capital": "KeyBanc Capital Markets",
    
    # Truist
    "truist": "Truist Securities",
    "suntrust robinson humphrey": "Truist Securities",
    
    # Oppenheimer
    "oppenheimer holdings": "Oppenheimer",
    "oppenheimer & co": "Oppenheimer",
    
    # Wells Fargo
    "wells fargo": "Wells Fargo Securities",
    "wells": "Wells Fargo Securities",
    "wf securities": "Wells Fargo Securities",
}


# ==========================================================
# TIER LOOKUP
# ==========================================================

BANK_TIERS = {}
for bank in BULGE_BRACKET:
    BANK_TIERS[bank] = 1
for bank in ELITE_BOUTIQUE:
    BANK_TIERS[bank] = 2
for bank in UPPER_MIDDLE_MARKET:
    BANK_TIERS[bank] = 3
for bank in REGIONAL_SPECIALIST:
    BANK_TIERS[bank] = 4


# ==========================================================
# MATCHING FUNCTIONS
# ==========================================================

# Pre-build normalized lookup for fuzzy matching (cached)
_NORMALIZED_BANKS = None


def _get_normalized_banks() -> dict[str, str]:
    """Build normalized name -> canonical name mapping (cached)"""
    global _NORMALIZED_BANKS
    if _NORMALIZED_BANKS is None:
        _NORMALIZED_BANKS = {}
        for bank in INVESTMENT_BANKS:
            normalized = normalize_bank_name(bank)
            _NORMALIZED_BANKS[normalized] = bank
    return _NORMALIZED_BANKS


def normalize_bank_name(name: str) -> str:
    """Normalize bank name for comparison"""
    if not name:
        return ""
    n = name.lower().strip()
    # Remove punctuation that causes matching issues
    n = n.replace(".", "").replace("&", "and").replace("-", " ").replace(",", "")
    # Strip common suffixes
    for suffix in [" llc", " lp", " ltd", " inc", " plc", " sa", " ag",
                   " partners", " capital", " group", " securities", 
                   " investment bank", " advisory", " advisors", " advisers",
                   " and co", " co"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def is_target_bank(bank_name: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> bool:
    """
    Check if bank is in target investment banks list using fuzzy matching.
    
    Matching hierarchy:
    1. Exact alias match (fastest)
    2. Exact normalized match
    3. Fuzzy match against all targets (handles typos)
    
    Args:
        bank_name: Name of bank to check
        threshold: Minimum fuzzy match score (0-100). Default 85.
    
    Returns:
        True if match found, False otherwise
    """
    if not bank_name:
        return False
    
    bank_lower = bank_name.lower().strip()
    
    # 1. Check aliases first (exact match on known variations)
    if bank_lower in BANK_ALIASES:
        return True
    
    # 2. Check exact normalized match
    bank_normalized = normalize_bank_name(bank_name)
    normalized_banks = _get_normalized_banks()
    
    if bank_normalized in normalized_banks:
        return True
    
    # 3. Fuzzy match against normalized targets
    # Use token_set_ratio: handles word reordering and partial matches
    match = process.extractOne(
        bank_normalized,
        normalized_banks.keys(),
        scorer=fuzz.token_set_ratio,
        score_cutoff=threshold
    )
    
    if match:
        return True
    
    # 4. Also try WRatio (weighted ratio) for cases where token_set fails
    match = process.extractOne(
        bank_normalized,
        normalized_banks.keys(),
        scorer=fuzz.WRatio,
        score_cutoff=threshold
    )
    
    return match is not None


def match_bank(bank_name: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> tuple[bool, str | None, int, int | None]:
    """
    Match bank to target list and return match details.
    
    Args:
        bank_name: Name of bank to check
        threshold: Minimum fuzzy match score (0-100)
    
    Returns:
        Tuple of (is_match, matched_bank_name, confidence_score, tier)
        If no match: (False, None, 0, None)
    """
    if not bank_name:
        return False, None, 0, None
    
    bank_lower = bank_name.lower().strip()
    
    # 1. Check aliases
    if bank_lower in BANK_ALIASES:
        canonical = BANK_ALIASES[bank_lower]
        tier = BANK_TIERS.get(canonical)
        return True, canonical, 100, tier
    
    # 2. Exact normalized match
    bank_normalized = normalize_bank_name(bank_name)
    normalized_banks = _get_normalized_banks()
    
    if bank_normalized in normalized_banks:
        canonical = normalized_banks[bank_normalized]
        tier = BANK_TIERS.get(canonical)
        return True, canonical, 100, tier
    
    # 3. Fuzzy match - try token_set_ratio first
    match = process.extractOne(
        bank_normalized,
        normalized_banks.keys(),
        scorer=fuzz.token_set_ratio,
        score_cutoff=threshold
    )
    
    if match:
        matched_normalized, score, _ = match
        canonical = normalized_banks[matched_normalized]
        tier = BANK_TIERS.get(canonical)
        return True, canonical, int(score), tier
    
    # 4. Try WRatio as fallback
    match = process.extractOne(
        bank_normalized,
        normalized_banks.keys(),
        scorer=fuzz.WRatio,
        score_cutoff=threshold
    )
    
    if match:
        matched_normalized, score, _ = match
        canonical = normalized_banks[matched_normalized]
        tier = BANK_TIERS.get(canonical)
        return True, canonical, int(score), tier
    
    return False, None, 0, None


def get_bank_tier(bank_name: str) -> int | None:
    """
    Get tier for a bank (1=Bulge Bracket, 2=Elite Boutique, 3=Upper MM, 4=Regional)
    
    Args:
        bank_name: Name of bank
    
    Returns:
        Tier number (1-4) or None if not found
    """
    is_match, canonical, _, tier = match_bank(bank_name)
    return tier if is_match else None


def extract_bank_from_text(text: str, threshold: int = FUZZY_MATCH_THRESHOLD) -> list[tuple[str, str, int, int]]:
    """
    Extract all investment bank mentions from text.
    
    Useful for parsing news article titles/content to identify advisers.
    
    Args:
        text: Text to search for bank mentions
        threshold: Minimum fuzzy match score
    
    Returns:
        List of tuples: (matched_text, canonical_name, confidence, tier)
    """
    if not text:
        return []
    
    matches = []
    text_lower = text.lower()
    
    # Check each alias/bank name for presence in text
    # Start with aliases (more specific)
    for alias, canonical in BANK_ALIASES.items():
        if alias in text_lower:
            tier = BANK_TIERS.get(canonical)
            matches.append((alias, canonical, 100, tier))
    
    # Then check canonical names
    for bank in INVESTMENT_BANKS:
        bank_lower = bank.lower()
        if bank_lower in text_lower and not any(m[1] == bank for m in matches):
            tier = BANK_TIERS.get(bank)
            matches.append((bank_lower, bank, 100, tier))
    
    # Dedupe by canonical name, keeping highest confidence
    seen = {}
    for match in matches:
        canonical = match[1]
        if canonical not in seen or match[2] > seen[canonical][2]:
            seen[canonical] = match
    
    return list(seen.values())


# ==========================================================
# RSS FEED BANK PATTERNS
# For use in bank_mandate_monitor.py
# ==========================================================

def get_bank_patterns_for_rss() -> dict[str, str]:
    """
    Return dict of lowercase patterns -> canonical names for RSS parsing.
    Combines aliases with key terms from bank names.
    """
    patterns = dict(BANK_ALIASES)  # Start with aliases
    
    # Add additional patterns for RSS matching
    for bank in INVESTMENT_BANKS:
        # Add lowercase canonical
        patterns[bank.lower()] = bank
        # Add key term (first significant word)
        key_term = normalize_bank_name(bank).split()[0]
        if key_term not in patterns and len(key_term) > 3:
            patterns[key_term] = bank
    
    return patterns


# ==========================================================
# GOOGLE NEWS RSS FEED URLS
# For bank mandate monitoring
# ==========================================================

BANK_NEWS_RSS = [
    # Wire service searches (most reliable for mandate announcements)
    'https://news.google.com/rss/search?q=("appointed"+OR+"retained"+OR+"engaged")+"financial+adviser"+(divestiture+OR+"strategic+review"+OR+sale+OR+"spin-off")+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="financial+advisor"+appointed+(divestiture+OR+carve-out+OR+"strategic+alternatives")+when:1d&hl=en-US&gl=US&ceid=US:en',
    
    # Bulge bracket specific
    'https://news.google.com/rss/search?q="Goldman+Sachs"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Morgan+Stanley"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="JPMorgan"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Bank+of+America"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Citigroup"+OR+"Citi"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Barclays"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="UBS"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Deutsche+Bank"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    
    # Elite boutique specific
    'https://news.google.com/rss/search?q="Evercore"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Lazard"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Centerview"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Moelis"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="PJT+Partners"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Perella+Weinberg"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Rothschild"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Greenhill"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Guggenheim"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Qatalyst"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="LionTree"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    
    # Upper middle market specific
    'https://news.google.com/rss/search?q="Jefferies"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Houlihan+Lokey"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="William+Blair"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Harris+Williams"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Baird"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Piper+Sandler"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Raymond+James"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Lincoln+International"+adviser+appointed+sale+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="Stifel"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q="RBC+Capital"+adviser+appointed+(sale+OR+divestiture)+when:1d&hl=en-US&gl=US&ceid=US:en',
    
    # Wire service direct searches
    'https://news.google.com/rss/search?q=site:prnewswire.com+"financial+adviser"+appointed+when:1d&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=site:businesswire.com+"financial+adviser"+appointed+when:1d&hl=en-US&gl=US&ceid=US:en',
]


if __name__ == "__main__":
    # Test the matching functions
    print("Investment Banks Reference - Test Run")
    print("=" * 50)
    
    print(f"\nTotal banks in list: {len(INVESTMENT_BANKS)}")
    print(f"  Bulge Bracket: {len(BULGE_BRACKET)}")
    print(f"  Elite Boutique: {len(ELITE_BOUTIQUE)}")
    print(f"  Upper Middle Market: {len(UPPER_MIDDLE_MARKET)}")
    print(f"  Regional/Specialist: {len(REGIONAL_SPECIALIST)}")
    print(f"  Aliases defined: {len(BANK_ALIASES)}")
    
    print("\n" + "=" * 50)
    print("Testing fuzzy matching:")
    
    test_cases = [
        "Goldman Sachs",           # Exact
        "goldman",                 # Alias
        "GS",                      # Abbreviation
        "Goldmann Sachs",          # Typo
        "Morgan Stanley & Co",     # With suffix
        "Lazard Freres",           # Alternative name
        "Centerview",              # Partial
        "Houlihan",                # Partial
        "Random Bank LLC",         # Not in list
        "Baird",                   # Common name
        "RBC Capital Markets",     # Full name
    ]
    
    for test in test_cases:
        is_match, canonical, confidence, tier = match_bank(test)
        if is_match:
            print(f"  '{test}' -> {canonical} (Tier {tier}, {confidence}% confidence)")
        else:
            print(f"  '{test}' -> NOT MATCHED")
    
    print("\n" + "=" * 50)
    print("Testing text extraction:")
    
    sample_text = "Goldman Sachs and Lazard have been appointed as financial advisers to XYZ Corp for the sale of its industrial division."
    matches = extract_bank_from_text(sample_text)
    print(f"  Text: {sample_text[:80]}...")
    print(f"  Found banks: {[m[1] for m in matches]}")
