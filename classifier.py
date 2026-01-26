"""
Two-Stage Classifier for Deal Flow Agent

Stage 1: Regex/keyword scoring (free, fast)
  - High score (>=5): Send to Claude for full analysis
  - Medium score (2-4): Send to Claude for full analysis
  - Low score (0-1): Skip (probably not relevant)

Stage 2: Claude analysis (expensive, accurate)
  - Full extraction of deal details
  - Only called for articles that pass Stage 1

This reduces Claude API costs by 60-70% while maintaining coverage.
"""

import re
from typing import Optional
from dataclasses import dataclass


# ==========================================================================
# SCORING KEYWORDS
# ==========================================================================

# Primary signals (3 points each) - strong indicators of carve-out activity
PRIMARY_KEYWORDS = [
    # Carve-out specific
    "carve-out",
    "carve out",
    "carveout",
    "spin-off",
    "spin off",
    "spinoff",
    "divestiture",
    "divesting",
    "divest",
    "demerger",
    "hive off",
    "hive-off",
    
    # Sale process signals
    "strategic review",
    "strategic alternatives",
    "exploring sale",
    "explore sale",
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
    
    # Adviser appointments (strong signal)
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
    "financial adviser",
    "financial advisor",
    "engaged goldman",
    "engaged morgan stanley",
    "engaged jpmorgan",
    "engaged lazard",
    "engaged evercore",
    "hired goldman",
    "hired morgan stanley",
    "hired jpmorgan",
    "hired lazard",
    "hired evercore",
    
    # PE buyer signals
    "private equity interest",
    "pe interest",
    "private equity circling",
    "pe firms circling",
    "bidding war",
    "auction process",
    "definitive agreement",
    "draw interest from private equity",
    "attract interest from private equity",
    "attracting interest from private equity",
]

# Secondary signals (2 points each) - moderate indicators
SECONDARY_KEYWORDS = [
    # Division/unit language
    "business unit",
    "division sale",
    "unit sale",
    "sells division",
    "sells unit",
    "sells business",
    "selling its",
    "to sell its",
    
    # Non-core
    "non-core",
    "non core",
    "noncore",
    "portfolio review",
    "portfolio rationalization",
    
    # PE activity
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
    
    # Deal progression
    "preliminary talks",
    "advanced talks",
    "exclusive talks",
    "exclusive negotiations",
    "preferred bidder",
    "leading bidder",
]

# Tertiary signals (1 point each) - weak indicators, need combination
TERTIARY_KEYWORDS = [
    # Generic M&A
    "acquisition",
    "acquires",
    "acquire",
    "buys",
    "buying",
    "purchase",
    "purchasing",
    "takeover",
    "take over",
    
    # Asset language
    "asset sale",
    "asset disposal",
    "disposal",
    
    # Interest signals
    "interest in",
    "eyeing",
    "approached",
    "approaches",
    
    # Separation
    "separation",
    "standalone",
    "stand-alone",
    "independent",
]

# PE firm mentions (1 point each, capped at 3)
# Just check for "private equity" or common firm names
PE_INDICATORS = [
    "private equity",
    "pe firm",
    "pe firms",
    "buyout",
    "buyout firm",
    "sponsor",
    "financial sponsor",
    # Top PE firms
    "blackstone",
    "kkr",
    "carlyle",
    "apollo",
    "tpg",
    "bain capital",
    "advent",
    "cvc",
    "eqt",
    "permira",
    "cinven",
    "h.i.g.",
    "hig capital",
    "kps capital",
    "platinum equity",
    "aurelius",
    "thoma bravo",
    "vista equity",
    "silver lake",
]

# Negative signals (reduce score) - things that look like deals but aren't
NEGATIVE_KEYWORDS = [
    # IPO/public offerings
    "ipo",
    "initial public offering",
    "public offering",
    "goes public",
    "going public",
    
    # VC/growth
    "venture capital",
    "series a",
    "series b",
    "series c",
    "seed round",
    "growth equity",
    "growth funding",
    
    # Real estate
    "real estate",
    "property sale",
    "office building",
    "warehouse",
    
    # Academic/Government
    "university",
    "college",
    "government",
    "federal",
    "ministry",
    
    # Wrong geography
    "china",
    "chinese",
    "india",
    "indian",
    "brazil",
    "latin america",
    "middle east",
    "africa",
    "australia",
    
    # Minority/financial transactions (not operational carve-outs)
    "minority stake",
    "minority shareholding",
    "minority interest",
    "stake sale",
    "sells stake",
    "sold stake",
    "selling stake",
    "exits stake",
    "exiting stake",
    
    # Standalone/non-integrated (no separation work)
    "standalone fintech",
    "standalone platform",
    "operated independently",
    "operates independently",
    "remained independent",
    "remains independent",
    
    # Bolt-on/tuck-in (clean acquisitions, no TSA)
    "bolt-on",
    "bolt on",
    "tuck-in",
    "tuck in",
    "add-on acquisition",
]

# Premium sources get a bonus (more likely to be relevant even with lower scores)
PREMIUM_SOURCES = [
    "ft.com",
    "financial times",
    "wsj.com",
    "wall street journal",
    "bloomberg",
    "reuters",
    "pehub",
    "pe hub",
    "mergermarket",
    "dealreporter",
    "pitchbook",
]


@dataclass
class ClassificationResult:
    """Result of Stage 1 classification"""
    score: int
    should_analyze: bool
    reason: str
    primary_matches: list[str]
    secondary_matches: list[str]
    tertiary_matches: list[str]
    pe_matches: list[str]
    negative_matches: list[str]
    is_premium_source: bool


def classify_article(title: str, summary: str = "", source: str = "") -> ClassificationResult:
    """
    Stage 1 classification using keyword scoring.
    
    Scoring:
    - Primary keywords: 3 points each
    - Secondary keywords: 2 points each
    - Tertiary keywords: 1 point each
    - PE indicators: 1 point each (capped at 3)
    - Premium source bonus: +2
    - Negative keywords: -2 each (floor at 0)
    
    Thresholds:
    - Score >= 3: ANALYZE (send to Claude)
    - Score 1-2: SKIP (not enough signal)
    - Score 0: SKIP
    
    Returns ClassificationResult with score and analysis decision.
    """
    text = f"{title} {summary}".lower()
    source_lower = source.lower()
    
    # Find matches
    primary_matches = [kw for kw in PRIMARY_KEYWORDS if kw in text]
    secondary_matches = [kw for kw in SECONDARY_KEYWORDS if kw in text]
    tertiary_matches = [kw for kw in TERTIARY_KEYWORDS if kw in text]
    pe_matches = [kw for kw in PE_INDICATORS if kw in text]
    negative_matches = [kw for kw in NEGATIVE_KEYWORDS if kw in text]
    
    # Check premium source
    is_premium = any(src in source_lower or src in text for src in PREMIUM_SOURCES)
    
    # Calculate score
    score = 0
    score += len(primary_matches) * 3
    score += len(secondary_matches) * 2
    score += len(tertiary_matches) * 1
    score += min(len(pe_matches), 3) * 1  # Cap PE bonus at 3
    
    if is_premium:
        score += 2
    
    # Apply negative penalties
    score -= len(negative_matches) * 2
    score = max(score, 0)  # Floor at 0
    
    # Determine if should analyze
    should_analyze = score >= 3
    
    # Build reason
    if should_analyze:
        if score >= 6:
            reason = f"HIGH signal (score {score})"
        else:
            reason = f"MEDIUM signal (score {score})"
    else:
        if score == 0:
            reason = "NO signal keywords"
        else:
            reason = f"LOW signal (score {score})"
    
    return ClassificationResult(
        score=score,
        should_analyze=should_analyze,
        reason=reason,
        primary_matches=primary_matches,
        secondary_matches=secondary_matches,
        tertiary_matches=tertiary_matches,
        pe_matches=pe_matches,
        negative_matches=negative_matches,
        is_premium_source=is_premium,
    )


def classify_batch(articles: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Classify a batch of articles, splitting into analyze vs skip.
    
    Returns (to_analyze, to_skip) tuple.
    """
    to_analyze = []
    to_skip = []
    
    for article in articles:
        title = article.get('title', '')
        summary = article.get('summary', '')
        source = article.get('source', '')
        
        result = classify_article(title, summary, source)
        
        # Add classification info to article
        article['_classification'] = {
            'score': result.score,
            'reason': result.reason,
            'primary_matches': result.primary_matches,
            'is_premium_source': result.is_premium_source,
        }
        
        if result.should_analyze:
            to_analyze.append(article)
        else:
            to_skip.append(article)
    
    return to_analyze, to_skip


def get_classification_stats(articles: list[dict]) -> dict:
    """
    Get classification statistics for a batch of articles.
    
    Returns dict with counts by score range.
    """
    stats = {
        'total': len(articles),
        'high_signal': 0,      # score >= 6
        'medium_signal': 0,    # score 3-5
        'low_signal': 0,       # score 1-2
        'no_signal': 0,        # score 0
        'to_analyze': 0,
        'to_skip': 0,
        'premium_sources': 0,
    }
    
    for article in articles:
        title = article.get('title', '')
        summary = article.get('summary', '')
        source = article.get('source', '')
        
        result = classify_article(title, summary, source)
        
        if result.score >= 6:
            stats['high_signal'] += 1
        elif result.score >= 3:
            stats['medium_signal'] += 1
        elif result.score >= 1:
            stats['low_signal'] += 1
        else:
            stats['no_signal'] += 1
        
        if result.should_analyze:
            stats['to_analyze'] += 1
        else:
            stats['to_skip'] += 1
        
        if result.is_premium_source:
            stats['premium_sources'] += 1
    
    return stats


# ==========================================================================
# RELEVANCE SCORING (for pre-filter)
# ==========================================================================

def quick_relevance_score(text: str) -> int:
    """
    Quick relevance score for pre-filtering.
    Used to decide if article should even be considered.
    
    Returns score 0-10.
    """
    text_lower = text.lower()
    score = 0
    
    # Must-have indicators (without these, skip entirely)
    must_haves = [
        "divestiture", "spin-off", "spinoff", "carve-out", "carveout",
        "strategic review", "sale of", "sells", "selling",
        "division", "unit", "business", "segment",
        "private equity", "pe firm", "buyout",
    ]
    
    if not any(mh in text_lower for mh in must_haves):
        return 0
    
    # Add points for each signal
    for kw in PRIMARY_KEYWORDS:
        if kw in text_lower:
            score += 2
    
    for kw in SECONDARY_KEYWORDS:
        if kw in text_lower:
            score += 1
    
    # Cap at 10
    return min(score, 10)


# ==========================================================================
# TESTS
# ==========================================================================

if __name__ == "__main__":
    print("Two-Stage Classifier - Test Run")
    print("=" * 50)
    
    test_articles = [
        {
            "title": "Siemens exploring strategic alternatives for industrial motors division",
            "summary": "The German conglomerate has hired Goldman Sachs to advise on potential sale or spin-off of the unit",
            "source": "Financial Times",
        },
        {
            "title": "Private equity firms circling Honeywell aerospace segment",
            "summary": "KKR and Carlyle among bidders for the non-core business unit as company conducts strategic review",
            "source": "Reuters",
        },
        {
            "title": "Apple announces new iPhone release date",
            "summary": "Tech giant unveils latest smartphone at annual event",
            "source": "TechCrunch",
        },
        {
            "title": "Startup raises Series B funding",
            "summary": "Venture capital firm leads $50M round for software company",
            "source": "TechCrunch",
        },
        {
            "title": "Johnson & Johnson completes divestiture of consumer health business to PE consortium",
            "summary": "Definitive agreement reached with Blackstone-led group for $15B acquisition",
            "source": "PE Hub",
        },
        {
            "title": "Company considering sale of Asia Pacific operations",
            "summary": "Firm weighing options for China and India businesses",
            "source": "Bloomberg",
        },
        {
            "title": "Boeing in talks to sell defense unit",
            "summary": "Aerospace giant exploring disposal of non-core segment to private equity buyers",
            "source": "Wall Street Journal",
        },
    ]
    
    print("\nClassification Results:")
    print("-" * 50)
    
    to_analyze, to_skip = classify_batch(test_articles)
    
    for article in test_articles:
        title = article['title'][:50]
        cls = article.get('_classification', {})
        score = cls.get('score', 0)
        reason = cls.get('reason', '')
        
        is_analyze = article in to_analyze
        status = "✓ ANALYZE" if is_analyze else "✗ SKIP"
        
        print(f"\n  {status} (score {score})")
        print(f"    Title: {title}...")
        print(f"    Reason: {reason}")
        if cls.get('primary_matches'):
            print(f"    Primary: {', '.join(cls['primary_matches'][:3])}")
    
    print("\n" + "-" * 50)
    stats = get_classification_stats(test_articles)
    print(f"\nStatistics:")
    print(f"  Total articles: {stats['total']}")
    print(f"  To analyze: {stats['to_analyze']} ({stats['to_analyze']/stats['total']*100:.0f}%)")
    print(f"  To skip: {stats['to_skip']} ({stats['to_skip']/stats['total']*100:.0f}%)")
    print(f"  High signal (>=6): {stats['high_signal']}")
    print(f"  Medium signal (3-5): {stats['medium_signal']}")
    print(f"  Low signal (1-2): {stats['low_signal']}")
    print(f"  No signal (0): {stats['no_signal']}")
    print(f"  Premium sources: {stats['premium_sources']}")
    
    print("\n" + "=" * 50)
    print("Test complete.")
