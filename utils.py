"""
Utility functions for Deal Flow Agent

Provides:
- Structured logging with severity levels
- Retry logic with exponential backoff
- Rate limiting helpers
- Input sanitization
- Response validation
"""

import os
import sys
import time
import logging
import re
import functools
from datetime import datetime
from typing import Optional, Callable, Any, TypeVar
from dataclasses import dataclass

# Type variable for generic retry function
T = TypeVar('T')


# ==========================================================================
# LOGGING CONFIGURATION
# ==========================================================================

class AgentLogger:
    """
    Structured logger for Deal Flow Agent.
    Provides consistent formatting and severity levels.
    """

    def __init__(self, name: str = "deal-flow-agent"):
        self.logger = logging.getLogger(name)

        # Set up handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

        # Metrics tracking
        self.metrics = {
            'errors': 0,
            'warnings': 0,
            'api_calls': 0,
            'api_retries': 0,
            'api_failures': 0,
        }

    def info(self, message: str, **kwargs):
        """Log info message with optional context"""
        if kwargs:
            context = ' | '.join(f'{k}={v}' for k, v in kwargs.items())
            message = f"{message} | {context}"
        self.logger.info(message)

    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.metrics['warnings'] += 1
        if kwargs:
            context = ' | '.join(f'{k}={v}' for k, v in kwargs.items())
            message = f"{message} | {context}"
        self.logger.warning(message)

    def error(self, message: str, exc: Optional[Exception] = None, **kwargs):
        """Log error message with optional exception"""
        self.metrics['errors'] += 1
        if kwargs:
            context = ' | '.join(f'{k}={v}' for k, v in kwargs.items())
            message = f"{message} | {context}"
        if exc:
            message = f"{message} | exception={type(exc).__name__}: {str(exc)[:100]}"
        self.logger.error(message)

    def debug(self, message: str, **kwargs):
        """Log debug message"""
        if kwargs:
            context = ' | '.join(f'{k}={v}' for k, v in kwargs.items())
            message = f"{message} | {context}"
        self.logger.debug(message)

    def api_call(self, api_name: str, success: bool, retries: int = 0):
        """Track API call metrics"""
        self.metrics['api_calls'] += 1
        if retries > 0:
            self.metrics['api_retries'] += retries
        if not success:
            self.metrics['api_failures'] += 1

    def get_metrics(self) -> dict:
        """Return current metrics"""
        return dict(self.metrics)

    def set_level(self, level: str):
        """Set logging level (DEBUG, INFO, WARNING, ERROR)"""
        levels = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
        }
        self.logger.setLevel(levels.get(level.upper(), logging.INFO))


# Global logger instance
logger = AgentLogger()


# ==========================================================================
# RETRY LOGIC
# ==========================================================================

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0
    retryable_exceptions: tuple = (Exception,)
    retryable_status_codes: tuple = (429, 500, 502, 503, 504)


def retry_with_backoff(
    func: Callable[..., T],
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable[[int, Exception], None]] = None
) -> T:
    """
    Execute function with retry and exponential backoff.

    Args:
        func: Function to execute (takes no arguments, use functools.partial for args)
        config: Retry configuration
        on_retry: Optional callback on retry (receives attempt number and exception)

    Returns:
        Result of successful function call

    Raises:
        Last exception if all retries exhausted
    """
    if config is None:
        config = RetryConfig()

    last_exception = None

    for attempt in range(config.max_retries + 1):
        try:
            result = func()
            if attempt > 0:
                logger.api_call("retry", success=True, retries=attempt)
            return result

        except config.retryable_exceptions as e:
            last_exception = e

            if attempt < config.max_retries:
                delay = min(
                    config.base_delay * (config.exponential_base ** attempt),
                    config.max_delay
                )

                logger.warning(
                    f"Retry {attempt + 1}/{config.max_retries}",
                    delay=f"{delay:.1f}s",
                    error=str(e)[:50]
                )

                if on_retry:
                    on_retry(attempt + 1, e)

                time.sleep(delay)
            else:
                logger.api_call("retry", success=False, retries=attempt)

    raise last_exception


def make_request_with_retry(
    request_func: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0
) -> Any:
    """
    Wrapper for HTTP requests with retry logic.

    Args:
        request_func: Function that makes the request (returns response object)
        max_retries: Maximum retry attempts
        base_delay: Base delay in seconds for exponential backoff

    Returns:
        Response object from successful request

    Raises:
        Last exception if all retries fail
    """
    import requests

    config = RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        retryable_exceptions=(
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        )
    )

    def wrapped():
        response = request_func()
        # Check for retryable status codes
        if response.status_code in config.retryable_status_codes:
            raise requests.exceptions.HTTPError(
                f"Retryable status code: {response.status_code}"
            )
        return response

    return retry_with_backoff(wrapped, config)


# ==========================================================================
# RATE LIMITING
# ==========================================================================

class RateLimiter:
    """
    Simple rate limiter using token bucket algorithm.

    Usage:
        limiter = RateLimiter(requests_per_second=3)
        limiter.wait()  # Blocks if needed to respect rate limit
        make_api_call()
    """

    def __init__(self, requests_per_second: float):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait(self):
        """Wait if needed to respect rate limit"""
        now = time.time()
        elapsed = now - self.last_request_time

        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def __enter__(self):
        self.wait()
        return self

    def __exit__(self, *args):
        pass


# Pre-configured rate limiters
notion_rate_limiter = RateLimiter(requests_per_second=2.5)  # Notion limit is 3/sec


# ==========================================================================
# INPUT SANITIZATION
# ==========================================================================

def sanitize_for_prompt(text: str, max_length: int = 5000) -> str:
    """
    Sanitize text before including in Claude prompt.

    - Removes potential prompt injection patterns
    - Truncates to max length
    - Removes control characters
    - Normalizes whitespace
    """
    if not text:
        return ""

    # Remove control characters except newlines and tabs
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)

    # Remove potential prompt injection patterns
    injection_patterns = [
        r'<\|.*?\|>',  # Special tokens
        r'\[INST\].*?\[/INST\]',  # Instruction markers
        r'###\s*(System|User|Assistant)',  # Role markers
        r'Human:.*?Assistant:',  # Claude-style markers
    ]
    for pattern in injection_patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    # Truncate
    if len(text) > max_length:
        text = text[:max_length] + "..."

    return text


def sanitize_company_name(name: str) -> str:
    """Sanitize company name for database storage"""
    if not name:
        return "Unknown"

    # Remove quotes and special characters
    name = re.sub(r'["\'\`]', '', name)

    # Truncate
    return name[:100].strip()


# ==========================================================================
# RESPONSE VALIDATION
# ==========================================================================

def validate_claude_response(response: dict) -> tuple[bool, str]:
    """
    Validate Claude's JSON response against expected schema.

    Returns:
        (is_valid, error_message)
    """
    if not isinstance(response, dict):
        return False, "Response is not a dictionary"

    # Check is_relevant field
    if "is_relevant" not in response:
        return False, "Missing 'is_relevant' field"

    if not isinstance(response["is_relevant"], bool):
        return False, "'is_relevant' must be boolean"

    # If not relevant, just need a reason
    if not response["is_relevant"]:
        if "reason" not in response:
            return False, "Non-relevant response missing 'reason'"
        return True, ""

    # Required fields for relevant responses
    required_fields = ["company", "signal_type", "confidence"]
    for field in required_fields:
        if field not in response:
            return False, f"Missing required field: {field}"

    # Validate signal_type
    valid_signal_types = {
        "Strategic Review", "Adviser Appointed", "PE Interest",
        "PE In Talks", "PE Bid Submitted", "Definitive Agreement", "Deal Completed"
    }
    if response.get("signal_type") not in valid_signal_types:
        return False, f"Invalid signal_type: {response.get('signal_type')}"

    # Validate confidence
    valid_confidence = {"high", "medium", "low"}
    if response.get("confidence") not in valid_confidence:
        return False, f"Invalid confidence: {response.get('confidence')}"

    # Validate EV estimates if present
    for ev_field in ["ev_low", "ev_high"]:
        if ev_field in response and response[ev_field] is not None:
            if not isinstance(response[ev_field], (int, float)):
                return False, f"{ev_field} must be a number"
            if response[ev_field] < 0:
                return False, f"{ev_field} cannot be negative"

    return True, ""


# ==========================================================================
# METRICS & MONITORING
# ==========================================================================

class RunMetrics:
    """
    Tracks metrics for a single agent run.
    Can be exported for monitoring/alerting.
    """

    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None

        self.counters = {
            'articles_collected': 0,
            'articles_classified': 0,
            'articles_analyzed': 0,
            'articles_relevant': 0,
            'entries_written': 0,
            'entries_filtered': 0,
            'entries_duplicate': 0,
            'api_calls_claude': 0,
            'api_calls_notion': 0,
            'api_retries': 0,
            'api_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
        }

        self.sources = {}

    def increment(self, metric: str, value: int = 1):
        """Increment a counter"""
        if metric in self.counters:
            self.counters[metric] += value

    def set_source_count(self, source: str, count: int):
        """Record count for a data source"""
        self.sources[source] = count

    def complete(self):
        """Mark run as complete"""
        self.end_time = datetime.now()

    def get_duration_seconds(self) -> float:
        """Get run duration in seconds"""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    def to_dict(self) -> dict:
        """Export metrics as dictionary"""
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.get_duration_seconds(),
            'counters': dict(self.counters),
            'sources': dict(self.sources),
        }

    def print_summary(self):
        """Print human-readable summary"""
        print(f"\n{'='*60}")
        print("RUN METRICS")
        print(f"{'='*60}")
        print(f"  Duration: {self.get_duration_seconds():.1f}s")
        print(f"  Articles: {self.counters['articles_collected']} collected → "
              f"{self.counters['articles_analyzed']} analyzed → "
              f"{self.counters['entries_written']} written")
        print(f"  API calls: Claude={self.counters['api_calls_claude']}, "
              f"Notion={self.counters['api_calls_notion']}, "
              f"Retries={self.counters['api_retries']}")
        print(f"  Cache: {self.counters['cache_hits']} hits, "
              f"{self.counters['cache_misses']} misses")
        if self.counters['api_errors'] > 0:
            print(f"  ⚠ API errors: {self.counters['api_errors']}")
        print(f"{'='*60}")


# ==========================================================================
# ENVIRONMENT VALIDATION
# ==========================================================================

def validate_environment() -> tuple[bool, list[str]]:
    """
    Validate required environment variables are set.

    Returns:
        (is_valid, list of missing variables)
    """
    required = [
        "NOTION_API_KEY",
        "NOTION_DATABASE_ID",
        "ANTHROPIC_API_KEY",
    ]

    missing = [var for var in required if not os.environ.get(var)]

    return len(missing) == 0, missing


# ==========================================================================
# TESTS
# ==========================================================================

if __name__ == "__main__":
    print("Utils Module - Test Run")
    print("=" * 50)

    # Test logger
    print("\n1. Testing Logger:")
    logger.info("Test info message", key="value")
    logger.warning("Test warning", count=42)
    logger.error("Test error", exc=ValueError("test"))
    print(f"   Metrics: {logger.get_metrics()}")

    # Test rate limiter
    print("\n2. Testing Rate Limiter:")
    limiter = RateLimiter(requests_per_second=10)
    start = time.time()
    for i in range(5):
        limiter.wait()
        print(f"   Request {i+1} at {time.time() - start:.3f}s")

    # Test sanitization
    print("\n3. Testing Sanitization:")
    dirty_text = "Hello [INST]inject[/INST] world\x00\x01"
    clean_text = sanitize_for_prompt(dirty_text)
    print(f"   Input:  {repr(dirty_text)}")
    print(f"   Output: {repr(clean_text)}")

    # Test response validation
    print("\n4. Testing Response Validation:")
    valid_response = {
        "is_relevant": True,
        "company": "Test Corp",
        "signal_type": "Strategic Review",
        "confidence": "high"
    }
    is_valid, error = validate_claude_response(valid_response)
    print(f"   Valid response: {is_valid}")

    invalid_response = {"is_relevant": True}
    is_valid, error = validate_claude_response(invalid_response)
    print(f"   Invalid response: {is_valid}, error: {error}")

    # Test environment validation
    print("\n5. Testing Environment Validation:")
    is_valid, missing = validate_environment()
    print(f"   Valid: {is_valid}, Missing: {missing}")

    print("\n" + "=" * 50)
    print("Tests complete.")
