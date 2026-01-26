"""
Deduplication Module for Deal Flow Agent

Provides multi-layer deduplication:
1. URL hash - exact URL dedup (fastest)
2. Content hash - near-duplicate detection via title+summary hash
3. Company extraction - group articles by company before Claude analysis
4. Deal hash - entity-level dedup for Notion (same deal, different articles)

Usage:
    from dedup import (
        DedupManager,
        extract_company_from_title,
        compute_deal_hash,
    )
"""

import re
import hashlib
from typing import Optional
from dataclasses import dataclass, field
from rapidfuzz import fuzz


# ==========================================================================
# COMPANY NAME EXTRACTION
# ==========================================================================

# Patterns to extract company names from article titles
# Order matters - more specific patterns first
COMPANY_EXTRACTION_PATTERNS = [
    # "Company to sell/divest/spin off..." - capture just the company name before verb
    r'^([A-Z][A-Za-z&\.\-]+(?:\s+[A-Z][a-z&\.\-]+)*)\s+to\s+(?:sell|divest|spin|separate|explore|weigh|consider|evaluate)',
    
    # "Company exploring/weighing/considering..."
    r'^([A-Z][A-Za-z&\.\-]+(?:\s+[A-Z][a-z&\.\-]+)*)\s+(?:exploring|weighing|considering|evaluating|announces|said)',
    
    # "Company's division/unit..."
    r"^([A-Z][A-Za-z&\.\-]+(?:\s+[A-Z][a-z&\.\-]+)*)'s\s+",
    
    # "PE Firm acquires/buys [from] Company..." - capture the seller
    r'(?:acquires?|buys?|to\s+buy|to\s+acquire)\s+(?:[\w\s]+\s+)?(?:from|of)\s+([A-Z][A-Za-z&\.\-]+(?:\s+[A-Z][a-z&\.\-]+)*)',
    
    # "... circle/approach Company"
    r'(?:circle|circles|circling|approach|approaches|approaching)\s+([A-Z][A-Za-z&\.\-]+(?:\s+[A-Z][a-z&\.\-]+)*)',
    
    # "Sale of Company..."
    r'[Ss]ale\s+of\s+([A-Z][A-Za-z&\.\-]+(?:\s+[A-Z][a-z&\.\-]+)*)',
    
    # "Company division/unit attracts..." 
    r'^([A-Z][A-Za-z&\.\-]+)\s+(?:division|unit|business|segment|arm)',
]

# Common words that aren't company names
NON_COMPANY_WORDS = {
    'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or',
    'private', 'equity', 'firm', 'firms', 'pe', 'group', 'capital',
    'report', 'reports', 'sources', 'source', 'people', 'person',
    'deal', 'deals', 'bid', 'bids', 'offer', 'offers',
    'exclusive', 'breaking', 'update', 'analysis',
    'reuters', 'bloomberg', 'wsj', 'ft', 'financial', 'times',
    'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december',
}

# Known company aliases for normalization
COMPANY_ALIASES = {
    # Tech
    "alphabet": "Google",
    "google": "Google",
    "meta": "Meta",
    "facebook": "Meta",
    "microsoft": "Microsoft",
    "msft": "Microsoft",
    "amazon": "Amazon",
    "amzn": "Amazon",
    "apple": "Apple",
    "aapl": "Apple",
    
    # Industrial
    "ge": "General Electric",
    "general electric": "General Electric",
    "siemens ag": "Siemens",
    "siemens": "Siemens",
    "honeywell": "Honeywell",
    "3m": "3M",
    "mmm": "3M",
    "caterpillar": "Caterpillar",
    "cat": "Caterpillar",
    "deere": "John Deere",
    "john deere": "John Deere",
    
    # Healthcare
    "j&j": "Johnson & Johnson",
    "johnson & johnson": "Johnson & Johnson",
    "jnj": "Johnson & Johnson",
    "pfizer": "Pfizer",
    "pfe": "Pfizer",
    "abbvie": "AbbVie",
    "abbv": "AbbVie",
    "merck": "Merck",
    "mrk": "Merck",
    
    # Consumer
    "p&g": "Procter & Gamble",
    "procter & gamble": "Procter & Gamble",
    "pg": "Procter & Gamble",
    "unilever": "Unilever",
    "nestle": "Nestle",
    "coca-cola": "Coca-Cola",
    "coke": "Coca-Cola",
    "ko": "Coca-Cola",
    "pepsi": "PepsiCo",
    "pepsico": "PepsiCo",
    
    # Financial
    "jpmorgan": "JPMorgan",
    "jp morgan": "JPMorgan",
    "jpm": "JPMorgan",
    "goldman": "Goldman Sachs",
    "goldman sachs": "Goldman Sachs",
    "gs": "Goldman Sachs",
    "morgan stanley": "Morgan Stanley",
    "ms": "Morgan Stanley",
    "bofa": "Bank of America",
    "bank of america": "Bank of America",
    "bac": "Bank of America",
    
    # UK/Europe
    "bp": "BP",
    "british petroleum": "BP",
    "shell": "Shell",
    "royal dutch shell": "Shell",
    "hsbc": "HSBC",
    "barclays": "Barclays",
    "glaxosmithkline": "GSK",
    "gsk": "GSK",
    "astrazeneca": "AstraZeneca",
    "azn": "AstraZeneca",
    "vodafone": "Vodafone",
    "bt group": "BT",
    "bt": "BT",
    "tesco": "Tesco",
    "sainsbury": "Sainsbury's",
    "sainsbury's": "Sainsbury's",
}


def normalize_company_name(name: str) -> str:
    """Normalize company name for comparison"""
    if not name:
        return ""
    
    n = name.lower().strip()
    
    # Check aliases first
    if n in COMPANY_ALIASES:
        return COMPANY_ALIASES[n]
    
    # Remove common suffixes
    suffixes = [
        ' plc', ' ltd', ' limited', ' inc', ' incorporated', ' corp', ' corporation',
        ' co', ' company', ' group', ' holdings', ' ag', ' sa', ' nv', ' se',
        ' llc', ' llp', ' lp', ' international', ' intl',
    ]
    for suffix in suffixes:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    
    # Remove leading "the"
    if n.startswith('the '):
        n = n[4:]
    
    # Standardize common variations
    n = n.replace(' north american ', ' north america ')
    n = n.replace(' north american', ' north america')
    n = n.replace('north american ', 'north america ')
    
    # Check aliases again after normalization
    if n in COMPANY_ALIASES:
        return COMPANY_ALIASES[n]
    
    # Title case the result
    return n.title()


def extract_company_from_title(title: str) -> Optional[str]:
    """
    Extract company name from article title using pattern matching.
    
    Returns normalized company name or None if extraction fails.
    """
    if not title:
        return None
    
    for pattern in COMPANY_EXTRACTION_PATTERNS:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            
            # Validate: not too short, not a common word
            words = candidate.lower().split()
            if len(words) == 0:
                continue
            if all(w in NON_COMPANY_WORDS for w in words):
                continue
            if len(candidate) < 3:
                continue
            
            # Normalize and return
            return normalize_company_name(candidate)
    
    return None


# ==========================================================================
# CONTENT HASHING
# ==========================================================================

def compute_content_hash(title: str, summary: str = "") -> str:
    """
    Compute hash of article content for near-duplicate detection.
    
    Uses sorted significant words to catch:
    - Syndicated content with minor edits
    - Same story from different outlets
    - Title variations of same news
    
    Returns 16-char hex hash.
    """
    # Combine and normalize
    text = f"{title} {summary}".lower()
    
    # Remove punctuation and extra whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Get words, filter out very common/short ones
    words = text.split()
    
    # Remove stop words and short words for better matching
    stop_words = {
        'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or', 'is', 'are',
        'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
        'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall',
        'that', 'this', 'these', 'those', 'it', 'its', 'with', 'from', 'by', 'as',
        'said', 'says', 'according', 'sources', 'people', 'person', 'report', 'reports',
    }
    significant_words = [w for w in words if len(w) > 3 and w not in stop_words]
    
    # Sort and take top 20 unique words
    unique_sorted = sorted(set(significant_words))[:20]
    
    # Hash
    content = ' '.join(unique_sorted)
    return hashlib.md5(content.encode()).hexdigest()[:16]


def get_content_signature(title: str, summary: str = "") -> set[str]:
    """
    Get set of significant words for Jaccard similarity comparison.
    More flexible than hash for near-duplicate detection.
    """
    text = f"{title} {summary}".lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    
    stop_words = {
        'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or', 'is', 'are',
        'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
        'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall',
        'that', 'this', 'these', 'those', 'it', 'its', 'with', 'from', 'by', 'as',
        'said', 'says', 'according', 'sources', 'people', 'person', 'report', 'reports',
    }
    
    words = text.split()
    return {w for w in words if len(w) > 3 and w not in stop_words}


def content_similarity(sig1: set[str], sig2: set[str]) -> float:
    """
    Compute Jaccard similarity between two content signatures.
    Returns 0.0 to 1.0 (1.0 = identical).
    """
    if not sig1 or not sig2:
        return 0.0
    
    intersection = len(sig1 & sig2)
    union = len(sig1 | sig2)
    
    return intersection / union if union > 0 else 0.0


def compute_url_hash(url: str) -> str:
    """
    Compute hash of URL for exact deduplication.
    
    Normalizes URL before hashing:
    - Removes tracking parameters
    - Lowercases
    - Removes trailing slashes
    
    Returns 16-char hex hash.
    """
    if not url:
        return ""
    
    # Lowercase
    url = url.lower().strip()
    
    # Remove common tracking parameters
    url = re.sub(r'[?&](utm_\w+|ref|source|campaign|fbclid|gclid)=[^&]*', '', url)
    
    # Remove trailing ? or & if we stripped all params
    url = re.sub(r'[?&]$', '', url)
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    return hashlib.md5(url.encode()).hexdigest()[:16]


# ==========================================================================
# DEAL HASHING
# ==========================================================================

def compute_deal_hash(company: str, division: str, ev_estimate: Optional[int] = None) -> str:
    """
    Compute hash for deal-level deduplication in Notion.
    
    Combines:
    - Normalized company name
    - Normalized division name
    - EV bucket (optional, for disambiguation)
    
    Same deal reported by multiple sources should produce same hash.
    
    Returns 12-char hex hash.
    """
    # Normalize company
    company_norm = normalize_company_name(company) if company else "unknown"
    
    # Normalize division
    division_norm = ""
    if division:
        division_norm = division.lower().strip()
        # Remove generic terms
        for term in ['division', 'unit', 'business', 'segment', 'arm', 'subsidiary', 'the']:
            division_norm = division_norm.replace(term, '')
        division_norm = re.sub(r'\s+', ' ', division_norm).strip()
    
    # EV bucket (order of magnitude)
    ev_bucket = ""
    if ev_estimate:
        if ev_estimate < 100:
            ev_bucket = "sub100m"
        elif ev_estimate < 500:
            ev_bucket = "100-500m"
        elif ev_estimate < 1000:
            ev_bucket = "500m-1b"
        elif ev_estimate < 5000:
            ev_bucket = "1-5b"
        else:
            ev_bucket = "5b+"
    
    # Combine and hash
    content = f"{company_norm}|{division_norm}|{ev_bucket}".lower()
    return hashlib.md5(content.encode()).hexdigest()[:12]


# ==========================================================================
# DEDUP MANAGER
# ==========================================================================

@dataclass
class ArticleGroup:
    """Group of articles about the same company"""
    company: str
    articles: list = field(default_factory=list)
    
    def add(self, article: dict):
        self.articles.append(article)
    
    def get_best_article(self) -> dict:
        """Return the most detailed article (longest summary)"""
        if not self.articles:
            return {}
        return max(self.articles, key=lambda a: len(a.get('summary', '')))
    
    def get_all_urls(self) -> list[str]:
        """Return all URLs in this group"""
        return [a.get('link', '') for a in self.articles]


class DedupManager:
    """
    Manages deduplication across multiple stages of the pipeline.
    
    Usage:
        dedup = DedupManager()
        
        # Load existing hashes from Notion
        dedup.load_existing_from_notion(existing_entries)
        
        # Process articles
        for article in articles:
            if dedup.is_duplicate(article):
                continue
            # ... process article ...
            dedup.mark_processed(article)
    """
    
    CONTENT_SIMILARITY_THRESHOLD = 0.6  # 60% word overlap = duplicate
    
    def __init__(self):
        # URL hashes seen this run
        self.seen_url_hashes: set[str] = set()
        
        # Content signatures seen this run (for Jaccard similarity)
        self.seen_content_signatures: list[set[str]] = []
        
        # Deal hashes from Notion (cross-run dedup)
        self.existing_deal_hashes: set[str] = set()
        
        # URL hashes from Notion (cross-run dedup)
        self.existing_url_hashes: set[str] = set()
        
        # Company+division pairs from Notion (fuzzy match dedup)
        self.existing_company_divisions: list[tuple[str, str]] = []
        
        # Company groups for batching
        self.company_groups: dict[str, ArticleGroup] = {}
        
        # Stats
        self.stats = {
            'url_dupes': 0,
            'content_dupes': 0,
            'deal_dupes': 0,
            'fuzzy_dupes': 0,
            'total_processed': 0,
        }
    
    def load_existing_from_notion(self, entries: list[dict]):
        """
        Load existing hashes from Notion database entries.
        
        Expects entries with:
        - 'deal_hash': str (Deal Hash column)
        - 'url_hash': str (Source URL Hash column)
        - 'company': str (Company name, optional)
        - 'division': str (Division name, optional)
        """
        for entry in entries:
            if deal_hash := entry.get('deal_hash'):
                self.existing_deal_hashes.add(deal_hash)
            if url_hash := entry.get('url_hash'):
                self.existing_url_hashes.add(url_hash)
            # Load company+division for fuzzy matching
            company = entry.get('company', '')
            division = entry.get('division', '')
            if company or division:
                self.existing_company_divisions.append((
                    normalize_company_name(company),
                    normalize_company_name(division)
                ))
    
    def is_fuzzy_duplicate(self, company: str, division: str) -> bool:
        """
        Check if company+division fuzzy-matches an existing entry.
        Catches cases where Claude returns slightly different names.
        """
        if not company and not division:
            return False
        
        company_norm = normalize_company_name(company)
        division_norm = normalize_company_name(division)
        
        for existing_company, existing_division in self.existing_company_divisions:
            # Check if company names are similar
            company_match = (
                company_norm == existing_company or
                company_norm in existing_company or
                existing_company in company_norm or
                self._fuzzy_match(company_norm, existing_company)
            )
            
            # Check if division names are similar
            division_match = (
                division_norm == existing_division or
                division_norm in existing_division or
                existing_division in division_norm or
                self._fuzzy_match(division_norm, existing_division)
            )
            
            if company_match and division_match:
                self.stats['fuzzy_dupes'] += 1
                return True
        
        return False
    
    def _fuzzy_match(self, s1: str, s2: str, threshold: float = 0.85) -> bool:
        """Simple fuzzy match using character-level Jaccard similarity"""
        if not s1 or not s2:
            return False
        
        # Use word-level comparison
        words1 = set(s1.lower().split())
        words2 = set(s2.lower().split())
        
        if not words1 or not words2:
            return False
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return (intersection / union) >= threshold if union > 0 else False
    
    def add_company_division(self, company: str, division: str):
        """Add a company+division pair to the tracking list (after successful write)"""
        self.existing_company_divisions.append((
            normalize_company_name(company),
            normalize_company_name(division)
        ))
    
    def is_url_duplicate(self, url: str) -> bool:
        """Check if URL has been seen (this run or in Notion)"""
        url_hash = compute_url_hash(url)
        
        if url_hash in self.seen_url_hashes:
            self.stats['url_dupes'] += 1
            return True
        
        if url_hash in self.existing_url_hashes:
            self.stats['url_dupes'] += 1
            return True
        
        return False
    
    def is_content_duplicate(self, title: str, summary: str = "") -> bool:
        """Check if content is a near-duplicate of seen article using Jaccard similarity"""
        new_sig = get_content_signature(title, summary)
        
        if not new_sig:
            return False
        
        for existing_sig in self.seen_content_signatures:
            similarity = content_similarity(new_sig, existing_sig)
            if similarity >= self.CONTENT_SIMILARITY_THRESHOLD:
                self.stats['content_dupes'] += 1
                return True
        
        return False
    
    def is_deal_duplicate(self, company: str, division: str, ev_estimate: Optional[int] = None) -> bool:
        """Check if deal already exists in Notion"""
        deal_hash = compute_deal_hash(company, division, ev_estimate)
        
        if deal_hash in self.existing_deal_hashes:
            self.stats['deal_dupes'] += 1
            return True
        
        return False
    
    def is_duplicate(self, article: dict) -> tuple[bool, str]:
        """
        Full deduplication check for an article.
        
        Returns (is_duplicate, reason)
        """
        url = article.get('link', '')
        title = article.get('title', '')
        summary = article.get('summary', '')
        
        # Level 1: URL hash
        if self.is_url_duplicate(url):
            return True, "URL duplicate"
        
        # Level 2: Content similarity
        if self.is_content_duplicate(title, summary):
            return True, "Content duplicate"
        
        return False, ""
    
    def mark_processed(self, article: dict, analysis: Optional[dict] = None):
        """
        Mark article as processed, adding to seen sets.
        
        If analysis provided, also track deal hash.
        """
        url = article.get('link', '')
        title = article.get('title', '')
        summary = article.get('summary', '')
        
        # Add URL hash
        self.seen_url_hashes.add(compute_url_hash(url))
        
        # Add content signature
        sig = get_content_signature(title, summary)
        if sig:
            self.seen_content_signatures.append(sig)
        
        # Add deal hash if analysis provided
        if analysis:
            company = analysis.get('company', '')
            division = analysis.get('division', '')
            ev_low = analysis.get('ev_low')
            deal_hash = compute_deal_hash(company, division, ev_low)
            self.existing_deal_hashes.add(deal_hash)
        
        self.stats['total_processed'] += 1
    
    def group_by_company(self, articles: list[dict]) -> dict[str, ArticleGroup]:
        """
        Group articles by extracted company name.
        
        Returns dict of company -> ArticleGroup
        """
        groups: dict[str, ArticleGroup] = {}
        ungrouped: list[dict] = []
        
        for article in articles:
            title = article.get('title', '')
            company = extract_company_from_title(title)
            
            if company:
                if company not in groups:
                    groups[company] = ArticleGroup(company=company)
                groups[company].add(article)
            else:
                ungrouped.append(article)
        
        # Add ungrouped as individual "companies"
        for i, article in enumerate(ungrouped):
            key = f"_ungrouped_{i}"
            groups[key] = ArticleGroup(company=key)
            groups[key].add(article)
        
        return groups
    
    def get_representative_articles(self, articles: list[dict]) -> list[dict]:
        """
        From a list of articles, return representative set for Claude analysis.
        
        - Groups by company
        - Returns best (longest) article per company
        - Preserves ungrouped articles
        
        This reduces Claude API calls while maintaining coverage.
        """
        groups = self.group_by_company(articles)
        
        representatives = []
        for group in groups.values():
            best = group.get_best_article()
            if best:
                # Tag with group info for potential batch processing
                best['_company_group'] = group.company
                best['_group_size'] = len(group.articles)
                best['_related_urls'] = group.get_all_urls()
                representatives.append(best)
        
        return representatives
    
    def get_stats(self) -> dict:
        """Return deduplication statistics"""
        return {
            **self.stats,
            'existing_deals': len(self.existing_deal_hashes),
            'existing_urls': len(self.existing_url_hashes),
            'seen_urls_this_run': len(self.seen_url_hashes),
            'seen_content_this_run': len(self.seen_content_signatures),
        }


# ==========================================================================
# FUZZY TITLE MATCHING (for Notion dedup)
# ==========================================================================

def titles_match_fuzzy(title1: str, title2: str, threshold: int = 85) -> bool:
    """
    Check if two titles refer to the same deal using fuzzy matching.
    
    Handles variations like:
    - "Siemens - Industrial Motors" vs "Siemens - Innomotics"
    - "LKQ" vs "LKQ Corporation"
    """
    # Normalize both
    t1 = normalize_title_for_comparison(title1)
    t2 = normalize_title_for_comparison(title2)
    
    # Exact match
    if t1 == t2:
        return True
    
    # Fuzzy match
    score = fuzz.token_set_ratio(t1, t2)
    return score >= threshold


def normalize_title_for_comparison(title: str) -> str:
    """Normalize title for fuzzy comparison"""
    t = title.lower().strip()
    
    # Remove PE firm names in parentheses
    t = re.sub(r'\s*\([^)]*\)\s*$', '', t)
    
    # Remove corporate suffixes
    t = re.sub(r'\b(corporation|corp|inc|incorporated|ltd|limited|plc|llc|llp|co|company|group|holdings)\b\.?', '', t)
    
    # Remove "the" at start
    t = re.sub(r'^the\s+', '', t)
    
    # Normalize whitespace and punctuation
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    
    return t


# ==========================================================================
# TESTS
# ==========================================================================

if __name__ == "__main__":
    print("Deduplication Module - Test Run")
    print("=" * 50)
    
    # Test company extraction
    print("\n1. Company Extraction:")
    test_titles = [
        "Siemens to sell industrial motors division",
        "General Electric exploring sale of healthcare unit",
        "KKR acquires consumer health business from Johnson & Johnson",
        "PE firms circle Honeywell aerospace segment",
        "Sale of Boeing defense unit attracts bidders",
        "Breaking: Sources say deal imminent",  # Should fail
    ]
    
    for title in test_titles:
        company = extract_company_from_title(title)
        status = "✓" if company else "✗"
        print(f"  {status} '{title[:45]}...' -> {company}")
    
    # Test content similarity
    print("\n2. Content Similarity (syndication detection):")
    article1 = "Siemens to sell industrial motors division to private equity"
    article2 = "Private equity to acquire Siemens industrial motors division"
    article3 = "Apple announces new iPhone release"
    
    sig1 = get_content_signature(article1)
    sig2 = get_content_signature(article2)
    sig3 = get_content_signature(article3)
    
    sim_1_2 = content_similarity(sig1, sig2)
    sim_1_3 = content_similarity(sig1, sig3)
    
    print(f"  Article 1: '{article1[:50]}...'")
    print(f"  Article 2: '{article2[:50]}...'")
    print(f"  Article 3: '{article3[:50]}...'")
    print(f"  Similarity 1 vs 2: {sim_1_2:.2%} {'(DUPLICATE)' if sim_1_2 >= 0.6 else ''}")
    print(f"  Similarity 1 vs 3: {sim_1_3:.2%} {'(DUPLICATE)' if sim_1_3 >= 0.6 else ''}")
    
    # Test deal hashing
    print("\n3. Deal Hashing:")
    deal1 = compute_deal_hash("Siemens", "Industrial Motors", 500)
    deal2 = compute_deal_hash("Siemens AG", "industrial motors division", 600)
    deal3 = compute_deal_hash("Siemens", "Healthcare", 1000)
    
    print(f"  Siemens Motors (500M): {deal1}")
    print(f"  Siemens AG Motors Div (600M): {deal2}")
    print(f"  Siemens Healthcare (1B): {deal3}")
    print(f"  Motors deals match: {deal1 == deal2} {'✓' if deal1 == deal2 else '✗'}")
    print(f"  Motors vs Healthcare: {deal1 == deal3} {'✓' if deal1 != deal3 else '✗'}")
    
    # Test DedupManager
    print("\n4. DedupManager:")
    dedup = DedupManager()
    
    articles = [
        {"title": "Siemens to sell motors unit to PE buyers", "link": "https://ft.com/123", "summary": "Siemens exploring sale of industrial motors division to private equity firms"},
        {"title": "PE firms interested in Siemens motors division", "link": "https://reuters.com/456", "summary": "Private equity interested in Siemens industrial motors unit"},
        {"title": "Apple launches new product", "link": "https://techcrunch.com/789", "summary": "Apple iPhone announcement"},
    ]
    
    print(f"  Processing {len(articles)} articles:")
    for article in articles:
        is_dup, reason = dedup.is_duplicate(article)
        if is_dup:
            print(f"    ⊘ DUPLICATE ({reason}): {article['title'][:40]}...")
        else:
            print(f"    ✓ NEW: {article['title'][:40]}...")
            dedup.mark_processed(article)
    
    print(f"\n  Stats: {dedup.get_stats()}")
    
    # Test company grouping
    print("\n5. Company Grouping:")
    more_articles = [
        {"title": "Siemens to sell motors unit", "link": "https://a.com/1", "summary": "..."},
        {"title": "Siemens motors attracts PE interest", "link": "https://a.com/2", "summary": "..."},
        {"title": "Siemens exploring healthcare sale", "link": "https://a.com/3", "summary": "..."},
        {"title": "GE weighs industrial division sale", "link": "https://a.com/4", "summary": "..."},
        {"title": "Random news about nothing", "link": "https://a.com/5", "summary": "..."},
    ]
    
    dedup2 = DedupManager()
    reps = dedup2.get_representative_articles(more_articles)
    print(f"  Input: {len(more_articles)} articles")
    print(f"  Representatives: {len(reps)} articles")
    for rep in reps:
        group = rep.get('_company_group', 'unknown')
        size = rep.get('_group_size', 1)
        print(f"    - {group} (group size {size}): {rep['title'][:35]}...")
    
    print("\n" + "=" * 50)
    print("All tests complete.")
