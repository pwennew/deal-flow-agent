"""
Cache Module for Deal Flow Agent

Provides:
1. Response caching - skip re-analysis of already-processed articles
2. Run state persistence - resume interrupted runs
3. Processed URL tracking - cross-run deduplication

Storage options:
- Local JSON files (for GitHub Actions cache)
- Environment-based paths for flexibility
"""

import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Any
from dataclasses import dataclass, asdict, field


# ==========================================================================
# CONFIGURATION
# ==========================================================================

# Cache directory - defaults to .cache in current dir, can be overridden
CACHE_DIR = os.environ.get("DEAL_FLOW_CACHE_DIR", ".cache")

# Cache TTLs
RESPONSE_CACHE_TTL_HOURS = 72  # Keep Claude responses for 72h
PROCESSED_URL_TTL_DAYS = 7     # Keep processed URLs for 7 days
RUN_STATE_TTL_HOURS = 24       # Run state expires after 24h


# ==========================================================================
# UTILITIES
# ==========================================================================

def ensure_cache_dir():
    """Create cache directory if it doesn't exist"""
    os.makedirs(CACHE_DIR, exist_ok=True)


def get_cache_path(filename: str) -> str:
    """Get full path for a cache file"""
    ensure_cache_dir()
    return os.path.join(CACHE_DIR, filename)


def compute_article_hash(url: str, title: str) -> str:
    """
    Compute unique hash for an article.
    Uses URL + title to handle edge cases where same URL has different content.
    """
    content = f"{url}|{title}".lower().strip()
    return hashlib.md5(content.encode()).hexdigest()[:16]


# ==========================================================================
# RESPONSE CACHE
# ==========================================================================

class ResponseCache:
    """
    Caches Claude API responses to avoid re-analyzing the same articles.
    
    Structure:
    {
        "article_hash": {
            "response": {...},  # Claude's analysis result
            "timestamp": "2026-01-26T09:00:00",
            "url": "https://...",
            "title": "..."
        }
    }
    """
    
    CACHE_FILE = "response_cache.json"
    
    def __init__(self):
        self.cache: dict = {}
        self.hits = 0
        self.misses = 0
        self._load()
    
    def _load(self):
        """Load cache from disk"""
        path = get_cache_path(self.CACHE_FILE)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                
                # Filter out expired entries
                now = datetime.now()
                cutoff = now - timedelta(hours=RESPONSE_CACHE_TTL_HOURS)
                
                for key, entry in data.items():
                    ts = datetime.fromisoformat(entry.get('timestamp', '2000-01-01'))
                    if ts > cutoff:
                        self.cache[key] = entry
                
                print(f"  Loaded {len(self.cache)} cached responses (expired {len(data) - len(self.cache)})")
            except Exception as e:
                print(f"  Warning: Failed to load response cache: {e}")
                self.cache = {}
    
    def _save(self):
        """Save cache to disk"""
        path = get_cache_path(self.CACHE_FILE)
        try:
            with open(path, 'w') as f:
                json.dump(self.cache, f, indent=2, default=str)
        except Exception as e:
            print(f"  Warning: Failed to save response cache: {e}")
    
    def get(self, url: str, title: str) -> Optional[dict]:
        """
        Get cached response for an article.
        Returns None if not cached or expired.
        """
        key = compute_article_hash(url, title)
        
        if key in self.cache:
            entry = self.cache[key]
            
            # Check TTL
            ts = datetime.fromisoformat(entry.get('timestamp', '2000-01-01'))
            if datetime.now() - ts < timedelta(hours=RESPONSE_CACHE_TTL_HOURS):
                self.hits += 1
                return entry.get('response')
            else:
                # Expired
                del self.cache[key]
        
        self.misses += 1
        return None
    
    def set(self, url: str, title: str, response: dict):
        """Cache a Claude response"""
        key = compute_article_hash(url, title)
        
        self.cache[key] = {
            'response': response,
            'timestamp': datetime.now().isoformat(),
            'url': url,
            'title': title[:100],
        }
        
        self._save()
    
    def get_stats(self) -> dict:
        """Return cache statistics"""
        return {
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0,
        }


# ==========================================================================
# PROCESSED URL TRACKER
# ==========================================================================

class ProcessedURLTracker:
    """
    Tracks URLs that have been fully processed (analyzed + written to Notion).
    Used for cross-run deduplication to avoid re-processing.
    
    Structure:
    {
        "url_hash": {
            "url": "https://...",
            "timestamp": "2026-01-26T09:00:00",
            "result": "added" | "filtered" | "duplicate" | "irrelevant"
        }
    }
    """
    
    CACHE_FILE = "processed_urls.json"
    
    def __init__(self):
        self.processed: dict = {}
        self._load()
    
    def _load(self):
        """Load processed URLs from disk"""
        path = get_cache_path(self.CACHE_FILE)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                
                # Filter out expired entries
                now = datetime.now()
                cutoff = now - timedelta(days=PROCESSED_URL_TTL_DAYS)
                
                for key, entry in data.items():
                    ts = datetime.fromisoformat(entry.get('timestamp', '2000-01-01'))
                    if ts > cutoff:
                        self.processed[key] = entry
                
                print(f"  Loaded {len(self.processed)} processed URLs (expired {len(data) - len(self.processed)})")
            except Exception as e:
                print(f"  Warning: Failed to load processed URLs: {e}")
                self.processed = {}
    
    def _save(self):
        """Save to disk"""
        path = get_cache_path(self.CACHE_FILE)
        try:
            with open(path, 'w') as f:
                json.dump(self.processed, f, indent=2, default=str)
        except Exception as e:
            print(f"  Warning: Failed to save processed URLs: {e}")
    
    def is_processed(self, url: str) -> bool:
        """Check if URL has been processed"""
        key = hashlib.md5(url.lower().encode()).hexdigest()[:16]
        return key in self.processed
    
    def mark_processed(self, url: str, result: str = "added"):
        """Mark URL as processed"""
        key = hashlib.md5(url.lower().encode()).hexdigest()[:16]
        
        self.processed[key] = {
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'result': result,
        }
        
        self._save()
    
    def get_stats(self) -> dict:
        """Return statistics"""
        results = {}
        for entry in self.processed.values():
            result = entry.get('result', 'unknown')
            results[result] = results.get(result, 0) + 1
        
        return {
            'total': len(self.processed),
            'by_result': results,
        }


# ==========================================================================
# RUN STATE
# ==========================================================================

@dataclass
class RunState:
    """
    Tracks state of an in-progress run for resume capability.
    
    Allows resuming interrupted runs without re-processing.
    """
    run_id: str = ""
    started_at: str = ""
    phase: str = "init"  # init, collecting, classifying, analyzing, writing, complete
    
    # Collection phase
    articles_collected: int = 0
    articles_by_source: dict = field(default_factory=dict)
    
    # Classification phase
    articles_to_analyze: list = field(default_factory=list)
    articles_skipped: int = 0
    
    # Analysis phase
    articles_analyzed: int = 0
    articles_relevant: int = 0
    pending_articles: list = field(default_factory=list)  # URLs not yet analyzed
    
    # Writing phase
    entries_written: int = 0
    entries_filtered: int = 0
    entries_duplicate: int = 0
    
    # Completion
    completed_at: str = ""
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'RunState':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class RunStateManager:
    """
    Manages run state persistence for resume capability.
    """
    
    STATE_FILE = "run_state.json"
    
    def __init__(self):
        self.state: Optional[RunState] = None
        self._load()
    
    def _load(self):
        """Load existing run state if present and not expired"""
        path = get_cache_path(self.STATE_FILE)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                
                # Check if expired
                started = datetime.fromisoformat(data.get('started_at', '2000-01-01'))
                if datetime.now() - started < timedelta(hours=RUN_STATE_TTL_HOURS):
                    if data.get('phase') != 'complete':
                        self.state = RunState.from_dict(data)
                        print(f"  Found incomplete run from {started.strftime('%H:%M')}, phase: {self.state.phase}")
                        return
                
                # Expired or complete - clear it
                os.remove(path)
                
            except Exception as e:
                print(f"  Warning: Failed to load run state: {e}")
        
        self.state = None
    
    def _save(self):
        """Save current state to disk"""
        if not self.state:
            return
        
        path = get_cache_path(self.STATE_FILE)
        try:
            with open(path, 'w') as f:
                json.dump(self.state.to_dict(), f, indent=2, default=str)
        except Exception as e:
            print(f"  Warning: Failed to save run state: {e}")
    
    def start_new_run(self) -> RunState:
        """Start a new run"""
        self.state = RunState(
            run_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
            started_at=datetime.now().isoformat(),
            phase="init",
        )
        self._save()
        return self.state
    
    def get_current_run(self) -> Optional[RunState]:
        """Get current run state (if resuming)"""
        return self.state
    
    def has_incomplete_run(self) -> bool:
        """Check if there's an incomplete run to resume"""
        return self.state is not None and self.state.phase != 'complete'
    
    def update_phase(self, phase: str, **kwargs):
        """Update run phase and any additional fields"""
        if self.state:
            self.state.phase = phase
            for key, value in kwargs.items():
                if hasattr(self.state, key):
                    setattr(self.state, key, value)
            self._save()
    
    def complete_run(self):
        """Mark run as complete"""
        if self.state:
            self.state.phase = "complete"
            self.state.completed_at = datetime.now().isoformat()
            self._save()
            
            # Clear state file after completion
            path = get_cache_path(self.STATE_FILE)
            if os.path.exists(path):
                os.remove(path)
            
            self.state = None
    
    def add_pending_article(self, url: str):
        """Add article to pending analysis list"""
        if self.state and url not in self.state.pending_articles:
            self.state.pending_articles.append(url)
            self._save()
    
    def remove_pending_article(self, url: str):
        """Remove article from pending list (after analysis)"""
        if self.state and url in self.state.pending_articles:
            self.state.pending_articles.remove(url)
            self._save()
    
    def get_pending_articles(self) -> list:
        """Get list of articles still pending analysis"""
        return self.state.pending_articles if self.state else []


# ==========================================================================
# COMBINED CACHE MANAGER
# ==========================================================================

class CacheManager:
    """
    Combined interface for all caching functionality.
    
    Usage:
        cache = CacheManager()
        
        # Check if article already processed
        if cache.is_url_processed(url):
            continue
        
        # Check for cached Claude response
        cached = cache.get_response(url, title)
        if cached:
            analysis = cached
        else:
            analysis = call_claude(article)
            cache.set_response(url, title, analysis)
        
        # Mark as processed
        cache.mark_processed(url, "added")
    """
    
    def __init__(self):
        print("Initializing cache...")
        self.responses = ResponseCache()
        self.processed = ProcessedURLTracker()
        self.run_state = RunStateManager()
    
    # Response cache shortcuts
    def get_response(self, url: str, title: str) -> Optional[dict]:
        return self.responses.get(url, title)
    
    def set_response(self, url: str, title: str, response: dict):
        self.responses.set(url, title, response)
    
    # Processed URL shortcuts
    def is_url_processed(self, url: str) -> bool:
        return self.processed.is_processed(url)
    
    def mark_processed(self, url: str, result: str = "added"):
        self.processed.mark_processed(url, result)
    
    # Run state shortcuts
    def start_run(self) -> RunState:
        return self.run_state.start_new_run()
    
    def has_incomplete_run(self) -> bool:
        return self.run_state.has_incomplete_run()
    
    def get_run_state(self) -> Optional[RunState]:
        return self.run_state.get_current_run()
    
    def update_run(self, phase: str, **kwargs):
        self.run_state.update_phase(phase, **kwargs)
    
    def complete_run(self):
        self.run_state.complete_run()
    
    def get_stats(self) -> dict:
        return {
            'response_cache': self.responses.get_stats(),
            'processed_urls': self.processed.get_stats(),
            'has_incomplete_run': self.has_incomplete_run(),
        }


# ==========================================================================
# GITHUB ACTIONS CACHE HELPERS
# ==========================================================================

def get_cache_key() -> str:
    """
    Generate cache key for GitHub Actions.
    
    Format: deal-flow-cache-YYYYMMDD
    Changes daily to ensure fresh data while maintaining some persistence.
    """
    return f"deal-flow-cache-{datetime.now().strftime('%Y%m%d')}"


def get_cache_paths() -> list:
    """
    Get list of paths to cache in GitHub Actions.
    
    Usage in workflow:
        - uses: actions/cache@v3
          with:
            path: |
              .cache/response_cache.json
              .cache/processed_urls.json
            key: ${{ steps.cache-key.outputs.key }}
    """
    return [
        f"{CACHE_DIR}/response_cache.json",
        f"{CACHE_DIR}/processed_urls.json",
    ]


# ==========================================================================
# TESTS
# ==========================================================================

if __name__ == "__main__":
    print("Cache Module - Test Run")
    print("=" * 50)
    
    # Test response cache
    print("\n1. Response Cache:")
    cache = ResponseCache()
    
    test_url = "https://example.com/article1"
    test_title = "Test Article Title"
    test_response = {"is_relevant": True, "company": "Test Corp"}
    
    # Set
    cache.set(test_url, test_title, test_response)
    print(f"  Set response for: {test_url}")
    
    # Get
    retrieved = cache.get(test_url, test_title)
    print(f"  Retrieved: {retrieved}")
    print(f"  Match: {retrieved == test_response}")
    
    # Miss
    miss = cache.get("https://other.com", "Other")
    print(f"  Miss test: {miss is None}")
    
    print(f"  Stats: {cache.get_stats()}")
    
    # Test processed URL tracker
    print("\n2. Processed URL Tracker:")
    tracker = ProcessedURLTracker()
    
    tracker.mark_processed("https://example.com/1", "added")
    tracker.mark_processed("https://example.com/2", "filtered")
    tracker.mark_processed("https://example.com/3", "duplicate")
    
    print(f"  Is processed (1): {tracker.is_processed('https://example.com/1')}")
    print(f"  Is processed (new): {tracker.is_processed('https://example.com/999')}")
    print(f"  Stats: {tracker.get_stats()}")
    
    # Test run state
    print("\n3. Run State Manager:")
    rsm = RunStateManager()
    
    state = rsm.start_new_run()
    print(f"  Started run: {state.run_id}")
    
    rsm.update_phase("collecting", articles_collected=50)
    print(f"  Updated to collecting phase")
    
    rsm.update_phase("analyzing", articles_analyzed=10, articles_relevant=5)
    print(f"  Updated to analyzing phase")
    
    rsm.complete_run()
    print(f"  Completed run")
    
    # Test combined manager
    print("\n4. Combined Cache Manager:")
    cm = CacheManager()
    print(f"  Stats: {cm.get_stats()}")
    
    print("\n" + "=" * 50)
    print("Test complete.")
