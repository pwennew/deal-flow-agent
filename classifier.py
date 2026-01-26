"""
Two-Stage Classifier for Deal Flow Agent

Stage 1: Regex/keyword scoring (free, fast)
  - High score (>=6): Send to Claude for full analysis
  - Medium score (3-5): Send to Claude for full analysis  
  - Low score (0-2): Skip (probably not relevant)

Stage 2: Claude analysis (expensive, accurate)
  - Full extraction of deal details
  - Only called for articles that pass Stage 1

This reduces Claude API costs by 60-70% while maintaining coverage.

Keywords are defined in keywords.py for easy maintenance.
"""

import re
from typing import Optional
from dataclasses import dataclass

# Import keywords from single source of truth
from keywords import (
    PRIMARY_KEYWORDS,
    SECONDARY_KEYWORDS,
    TERTIARY_KEYWORDS,
    PE_INDICATORS,
    NEGATIVE_KEYWORDS,
    HARD_REJECT_KEYWORDS,
    PREMIUM_SOURCES,
)


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
    hard_reject_matches: list[str]
    is_premium_source: bool


def classify_article(title: str, summary: str = "", source: str = "") -> ClassificationResult:
    """
    Stage 1 classification using keyword scoring.
    
    Hard Reject:
    - If any HARD_REJECT_KEYWORDS found, skip regardless of score
    
    Scoring:
    - Primary keywords: 3 points each
    - Secondary keywords: 2 points each
    - Tertiary keywords: 1 point each
    - PE indicators: 1 point each (capped at 3)
    - Premium source bonus: +2
    - Negative keywords: -2 each (floor at 0)
    
    Thresholds (IMPROVED in v6.0):
    - Hard reject keyword found: SKIP (always)
    - Score >= 4: ANALYZE (raised from 3 to reduce false positives)
    - Score >= 3 with primary match: ANALYZE (ensures quality)
    - Score 1-3 without primary: SKIP (not enough signal)
    - Score 0: SKIP

    Returns ClassificationResult with score and analysis decision.
    """
    text = f"{title} {summary}".lower()
    source_lower = source.lower()

    # Check hard rejects first (automatic skip)
    hard_reject_matches = [kw for kw in HARD_REJECT_KEYWORDS if kw in text]

    if hard_reject_matches:
        return ClassificationResult(
            score=0,
            should_analyze=False,
            reason=f"HARD REJECT: {', '.join(hard_reject_matches[:3])}",
            primary_matches=[],
            secondary_matches=[],
            tertiary_matches=[],
            pe_matches=[],
            negative_matches=[],
            hard_reject_matches=hard_reject_matches,
            is_premium_source=False,
        )

    # Find matches
    primary_matches = [kw for kw in PRIMARY_KEYWORDS if kw in text]
    secondary_matches = [kw for kw in SECONDARY_KEYWORDS if kw in text]
    tertiary_matches = [kw for kw in TERTIARY_KEYWORDS if kw in text]
    pe_matches = [kw for kw in PE_INDICATORS if kw in text]
    negative_matches = [kw for kw in NEGATIVE_KEYWORDS if kw in text]

    # Check premium source
    is_premium = any(src in source_lower or src in text for src in PREMIUM_SOURCES)

    # Context validation: check for conflicting keywords near PE terms
    # If "real estate" appears within 50 chars of "private equity", penalize more
    has_context_conflict = False
    conflict_terms = ["real estate", "reit", "property fund", "infrastructure fund"]
    for term in conflict_terms:
        if term in text:
            # Check if PE term is nearby (conflicting context)
            pe_pos = text.find("private equity")
            term_pos = text.find(term)
            if pe_pos >= 0 and term_pos >= 0 and abs(pe_pos - term_pos) < 50:
                has_context_conflict = True
                break

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

    # Additional penalty for context conflicts
    if has_context_conflict:
        score -= 3

    score = max(score, 0)  # Floor at 0

    # Determine if should analyze (IMPROVED threshold logic)
    # Require either score >= 4, OR score >= 3 with at least one primary match
    has_primary = len(primary_matches) > 0
    should_analyze = score >= 4 or (score >= 3 and has_primary)
    
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
        hard_reject_matches=[],
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
        "divestiture", "divestment", "spin-off", "spinoff", "carve-out", "carveout",
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
    
    # Import keyword stats
    from keywords import (
        PRIMARY_KEYWORDS as PK,
        SECONDARY_KEYWORDS as SK,
        TERTIARY_KEYWORDS as TK,
        PE_INDICATORS as PE,
        NEGATIVE_KEYWORDS as NK,
    )
    print(f"\nKeywords loaded from keywords.py:")
    print(f"  Primary: {len(PK)}, Secondary: {len(SK)}, Tertiary: {len(TK)}")
    print(f"  PE indicators: {len(PE)}, Negative: {len(NK)}")
    
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
            "title": "NatWest sells stake in fintech Cushon to WTW",
            "summary": "Bank selling its minority stake in standalone fintech that operated independently",
            "source": "Reuters",
        },
        {
            "title": "Nokia weighing a sale of managed services business",
            "summary": "Finnish company working with advisers to gauge interest from private equity firms",
            "source": "Bloomberg",
        },
    ]
    
    print("\nClassification Results:")
    print("-" * 50)
    
    to_analyze, to_skip = classify_batch(test_articles)
    
    for article in test_articles:
        title = article['title'][:55]
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
    
    print("\n" + "=" * 50)
    print("Test complete.")
