"""
Classification Keywords for Deal Flow Agent

Single source of truth for all carve-out/divestiture signal detection.
Used by classifier.py for Stage 1 filtering before Claude analysis.

Scoring:
- PRIMARY: 3 points (strong carve-out signal)
- SECONDARY: 2 points (moderate signal)
- TERTIARY: 1 point (weak signal, needs combination)
- PE_INDICATORS: 1 point each, capped at 3
- NEGATIVE: -2 points (wrong deal type)

Threshold: Score >= 3 sends to Claude
"""

# ==========================================================================
# PRIMARY KEYWORDS (3 points each)
# Strong indicators of carve-out/divestiture activity
# ==========================================================================

PRIMARY_KEYWORDS = [
    # ----- Carve-out / Separation terminology -----
    "carve-out",
    "carve out",
    "carveout",
    "spin-off",
    "spin off",
    "spinoff",
    "divestiture",
    "divestment",  # British English
    "divesting",
    "divest",
    "demerger",
    "hive off",
    "hive-off",
    "separation agreement",
    "separation plan",
    
    # ----- Sale process signals -----
    "strategic review",
    "strategic alternatives",
    "exploring sale",
    "explore sale",
    "explores sale",
    "weighing sale",
    "weighing a sale",
    "considering sale",
    "considering a sale",
    "evaluating sale",
    "evaluating a sale",
    "formal sale process",
    "sale process",
    "exploring options",
    "evaluating options",
    "exploring a sale",
    "mulling sale",
    "mulling a sale",
    "planning to sell",
    "plans to sell",
    "looking to sell",
    "seeks to sell",
    "seeking to sell",
    "seeking buyer",
    "seeks buyer",
    "put up for sale",
    "on the block",
    "up for sale",
    
    # ----- Adviser appointments (strong signal) -----
    "hired advisers",
    "hired adviser",
    "hired advisor",
    "hired advisors",
    "appointed advisers",
    "appointed adviser",
    "appointed advisor",
    "appointed advisors",
    "working with advisers",
    "working with adviser",
    "working with advisor",
    "working with advisors",
    "engaged advisers",
    "engaged adviser",
    "engaged advisor",
    "tapped advisers",
    "tapped adviser",
    "retained advisers",
    "retained adviser",
    "mandated advisers",
    "mandated adviser",
    "financial adviser",
    "financial advisor",
    
    # Named banks as advisers
    "engaged goldman",
    "engaged morgan stanley",
    "engaged jpmorgan",
    "engaged lazard",
    "engaged evercore",
    "engaged rothschild",
    "engaged centerview",
    "hired goldman",
    "hired morgan stanley",
    "hired jpmorgan",
    "hired lazard",
    "hired evercore",
    "hired rothschild",
    "hired centerview",
    
    # ----- PE buyer signals -----
    "private equity interest",
    "pe interest",
    "private equity circling",
    "pe firms circling",
    "private equity eyeing",
    "pe firms eyeing",
    "private equity firms eyeing",
    "buyout firms circling",
    "buyout firms eyeing",
    "bidding war",
    "auction process",
    "definitive agreement",
    "draw interest from private equity",
    "attract interest from private equity",
    "attracting interest from private equity",
    "attracting pe interest",
]


# ==========================================================================
# SECONDARY KEYWORDS (2 points each)
# Moderate indicators, often need context
# ==========================================================================

SECONDARY_KEYWORDS = [
    # ----- Division/unit sale language -----
    "business unit",
    "division sale",
    "unit sale",
    "sells division",
    "sells unit",
    "sells business",
    "selling division",
    "selling unit",
    "selling its",
    "to sell its",
    "sale of its",
    "disposal of",
    "disposing of",
    
    # ----- Non-core / Portfolio rationalization -----
    "non-core",
    "non core",
    "noncore",
    "portfolio review",
    "portfolio rationalization",
    "portfolio simplification",
    "streamlining portfolio",
    "refocusing on core",
    
    # ----- Exit language -----
    "exits business",
    "exiting business",
    "exits division",
    "exiting division",
    "offload",
    "offloading",
    "shedding",
    "sheds",
    "winding down",
    
    # ----- PE activity signals -----
    "in talks to acquire",
    "in talks to buy",
    "circling",
    "among bidders",
    "weighing bid",
    "considering bid",
    "submits bid",
    "submitted bid",
    "makes offer",
    "made offer",
    "approached by",
    "received interest",
    "receiving interest",
    
    # ----- Deal progression -----
    "preliminary talks",
    "advanced talks",
    "exclusive talks",
    "exclusive negotiations",
    "preferred bidder",
    "leading bidder",
    "final round",
    "second round",
    "shortlist",
    "short list",
    
    # ----- SEC / Legal signals -----
    "form 10",
    "transition services",
    "tsa agreement",
    "standalone basis",
    "pro forma",
]


# ==========================================================================
# TERTIARY KEYWORDS (1 point each)
# Weak indicators, need combination with others
# ==========================================================================

TERTIARY_KEYWORDS = [
    # ----- Generic M&A -----
    "acquisition",
    "acquires",
    "acquire",
    "buys",
    "buying",
    "purchase",
    "purchasing",
    "takeover",
    "take over",
    "merger",
    "deal",
    "transaction",
    
    # ----- Interest language -----
    "interest in",
    "interested in",
    "eyes",
    "eyeing",
    "targets",
    "targeting",
    
    # ----- Valuation signals -----
    "valued at",
    "valuation",
    "enterprise value",
    "price tag",
    "asking price",
    
    # ----- Generic sale -----
    "for sale",
    "sells",
    "sold",
    "selling",
    "buyer",
    "bidder",
]


# ==========================================================================
# PE FIRM INDICATORS (1 point each, capped at 3 total)
# Presence of PE firms suggests relevant deal activity
# ==========================================================================

# Generic PE terms (always included)
PE_GENERIC_TERMS = [
    "private equity",
    "pe firm",
    "pe fund",
    "buyout",
    "leveraged buyout",
    "lbo",
    "sponsor",
    "financial sponsor",
]

# PE firm names imported from target_accounts.py (single source of truth)
def _load_pe_firms():
    """Load PE firm names from target_accounts.py and convert to lowercase for matching"""
    try:
        from target_accounts import TARGET_PE_FIRMS
        return [firm.lower() for firm in TARGET_PE_FIRMS]
    except ImportError:
        # Fallback if target_accounts.py not available
        return []

PE_INDICATORS = PE_GENERIC_TERMS + _load_pe_firms()


# ==========================================================================
# HARD REJECT KEYWORDS (automatic skip, no scoring)
# If any of these appear, article is rejected regardless of positive signals
# ==========================================================================

HARD_REJECT_KEYWORDS = [
    # ----- VC / Growth funding (never carve-outs) -----
    "growth equity",
    "growth funding",
    "growth investment",
    "series a",
    "series b",
    "series c",
    "seed round",
    "venture capital",
    "funding round",
    
    # ----- Bolt-on / Tuck-in (clean acquisitions, no separation) -----
    "bolt-on",
    "bolt on",
    "tuck-in",
    "tuck in",
    "add-on acquisition",
    
    # ----- IPO / Public offerings -----
    "ipo",
    "initial public offering",
    "goes public",
    "going public",
]


# ==========================================================================
# NEGATIVE KEYWORDS (-2 points each)
# Indicators this is probably NOT a relevant carve-out opportunity
# ==========================================================================

NEGATIVE_KEYWORDS = [
    # ----- IPO / Public offerings (not hard reject) -----
    "public offering",
    "stock offering",
    "secondary offering",
    
    # ----- Real estate -----
    "real estate",
    "property sale",
    "property portfolio",
    "office building",
    "warehouse",
    "reit",
    
    # ----- Academic / Government -----
    "university",
    "college",
    "government",
    "federal",
    "ministry",
    "municipality",
    
    # ----- Wrong geography (deprioritize) -----
    "china",
    "chinese",
    "india",
    "indian",
    "brazil",
    "brazilian",
    "latin america",
    "middle east",
    "africa",
    "australia",
    "australian",
    
    # ----- Minority / Financial transactions (not operational) -----
    "minority stake",
    "minority shareholding",
    "minority interest",
    "stake sale",
    "sells stake",
    "sold stake",
    "selling stake",
    "exits stake",
    "exiting stake",
    
    # ----- Standalone / Non-integrated (no separation work) -----
    "standalone fintech",
    "standalone platform",
    "operated independently",
    "operates independently",
    "remained independent",
    "remains independent",
]


# ==========================================================================
# PREMIUM SOURCES
# Bonus +2 for high-quality sources (more likely relevant even with lower keyword score)
# ==========================================================================

PREMIUM_SOURCES = [
    # Tier 1 financial press
    "ft.com",
    "financial times",
    "wsj.com",
    "wall street journal",
    "bloomberg",
    "reuters",
    
    # PE-focused
    "pe hub",
    "pehub",
    "pitchbook",
    "privateequitywire",
    "buyouts",
    "mergermarket",
    
    # Quality business press
    "dealbook",
    "barrons",
    "fortune",
    "business insider",
]


# ==========================================================================
# HELPER FUNCTIONS
# ==========================================================================

def get_all_positive_keywords() -> list:
    """Return all positive keywords (primary + secondary + tertiary)"""
    return PRIMARY_KEYWORDS + SECONDARY_KEYWORDS + TERTIARY_KEYWORDS


def get_keyword_weight(keyword: str) -> int:
    """Return the point weight for a keyword"""
    if keyword in PRIMARY_KEYWORDS:
        return 3
    elif keyword in SECONDARY_KEYWORDS:
        return 2
    elif keyword in TERTIARY_KEYWORDS:
        return 1
    elif keyword in PE_INDICATORS:
        return 1
    elif keyword in NEGATIVE_KEYWORDS:
        return -2
    return 0


# ==========================================================================
# TESTS
# ==========================================================================

if __name__ == "__main__":
    print("Keywords Module - Statistics")
    print("=" * 50)
    print(f"Primary keywords:   {len(PRIMARY_KEYWORDS):3d} (3 pts each)")
    print(f"Secondary keywords: {len(SECONDARY_KEYWORDS):3d} (2 pts each)")
    print(f"Tertiary keywords:  {len(TERTIARY_KEYWORDS):3d} (1 pt each)")
    print(f"PE indicators:      {len(PE_INDICATORS):3d} (1 pt, max 3)")
    print(f"Negative keywords:  {len(NEGATIVE_KEYWORDS):3d} (-2 pts each)")
    print(f"Premium sources:    {len(PREMIUM_SOURCES):3d} (+2 bonus)")
    print("=" * 50)
    print(f"Total positive:     {len(get_all_positive_keywords())}")
    
    # Check for duplicates
    all_positive = get_all_positive_keywords()
    dupes = [k for k in all_positive if all_positive.count(k) > 1]
    if dupes:
        print(f"\nWARNING: Duplicate keywords: {set(dupes)}")
    else:
        print("\nNo duplicates found.")
