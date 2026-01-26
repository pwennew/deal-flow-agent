"""
Tests for utils.py - logging, retries, rate limiting, and validation
"""

import pytest
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    AgentLogger,
    RateLimiter,
    RetryConfig,
    retry_with_backoff,
    sanitize_for_prompt,
    sanitize_company_name,
    validate_claude_response,
    validate_environment,
    RunMetrics,
)


class TestAgentLogger:
    """Tests for the AgentLogger class"""

    def test_logger_creation(self):
        """Logger should be created successfully"""
        logger = AgentLogger("test-logger")
        assert logger is not None

    def test_log_levels(self):
        """Should support different log levels"""
        logger = AgentLogger("test-logger")
        # These should not raise exceptions
        logger.info("Test info")
        logger.warning("Test warning")
        logger.error("Test error")
        logger.debug("Test debug")

    def test_metrics_tracking(self):
        """Should track metrics"""
        logger = AgentLogger("test-logger")
        initial_warnings = logger.metrics["warnings"]
        logger.warning("Test")
        assert logger.metrics["warnings"] == initial_warnings + 1

    def test_api_call_tracking(self):
        """Should track API call metrics"""
        logger = AgentLogger("test-logger")
        logger.api_call("test", success=True, retries=2)
        assert logger.metrics["api_calls"] == 1
        assert logger.metrics["api_retries"] == 2


class TestRateLimiter:
    """Tests for the RateLimiter class"""

    def test_rate_limiting(self):
        """Should enforce rate limit"""
        limiter = RateLimiter(requests_per_second=10)

        start = time.time()
        for _ in range(5):
            limiter.wait()
        elapsed = time.time() - start

        # 5 requests at 10/sec should take at least 0.4 seconds
        assert elapsed >= 0.35  # Some tolerance for timing

    def test_context_manager(self):
        """Should work as context manager"""
        limiter = RateLimiter(requests_per_second=100)

        with limiter:
            pass  # Should not raise

    def test_first_request_immediate(self):
        """First request should not wait"""
        limiter = RateLimiter(requests_per_second=1)

        start = time.time()
        limiter.wait()
        elapsed = time.time() - start

        # First request should be immediate
        assert elapsed < 0.1


class TestRetryWithBackoff:
    """Tests for retry logic"""

    def test_success_first_try(self):
        """Should return immediately on success"""
        call_count = 0

        def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = retry_with_backoff(successful_func)
        assert result == "success"
        assert call_count == 1

    def test_retry_on_failure(self):
        """Should retry on failure"""
        call_count = 0

        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        config = RetryConfig(max_retries=5, base_delay=0.01)
        result = retry_with_backoff(fail_then_succeed, config)

        assert result == "success"
        assert call_count == 3

    def test_max_retries_exceeded(self):
        """Should raise after max retries"""

        def always_fail():
            raise ValueError("Always fails")

        config = RetryConfig(max_retries=2, base_delay=0.01)

        with pytest.raises(ValueError):
            retry_with_backoff(always_fail, config)


class TestSanitization:
    """Tests for input sanitization functions"""

    def test_sanitize_removes_control_chars(self):
        """Should remove control characters"""
        dirty = "Hello\x00World\x1f"
        clean = sanitize_for_prompt(dirty)
        assert "\x00" not in clean
        assert "\x1f" not in clean
        assert "Hello" in clean

    def test_sanitize_truncates_long_text(self):
        """Should truncate very long text"""
        long_text = "x" * 10000
        clean = sanitize_for_prompt(long_text, max_length=100)
        assert len(clean) <= 103  # 100 + "..."

    def test_sanitize_empty_string(self):
        """Should handle empty string"""
        result = sanitize_for_prompt("")
        assert result == ""

    def test_sanitize_company_name(self):
        """Should sanitize company names"""
        result = sanitize_company_name('Test "Company"')
        assert '"' not in result

    def test_sanitize_company_none(self):
        """Should handle None company name"""
        result = sanitize_company_name("")
        assert result == "Unknown"


class TestClaudeResponseValidation:
    """Tests for Claude response validation"""

    def test_valid_relevant_response(self):
        """Should accept valid relevant response"""
        response = {
            "is_relevant": True,
            "company": "Siemens",
            "signal_type": "Strategic Review",
            "confidence": "high",
        }
        is_valid, error = validate_claude_response(response)
        assert is_valid is True
        assert error == ""

    def test_valid_not_relevant_response(self):
        """Should accept valid not-relevant response"""
        response = {
            "is_relevant": False,
            "reason": "Not a carve-out"
        }
        is_valid, error = validate_claude_response(response)
        assert is_valid is True

    def test_missing_is_relevant(self):
        """Should reject response without is_relevant"""
        response = {"company": "Siemens"}
        is_valid, error = validate_claude_response(response)
        assert is_valid is False
        assert "is_relevant" in error

    def test_invalid_signal_type(self):
        """Should reject invalid signal type"""
        response = {
            "is_relevant": True,
            "company": "Siemens",
            "signal_type": "Invalid Type",
            "confidence": "high",
        }
        is_valid, error = validate_claude_response(response)
        assert is_valid is False
        assert "signal_type" in error

    def test_invalid_confidence(self):
        """Should reject invalid confidence"""
        response = {
            "is_relevant": True,
            "company": "Siemens",
            "signal_type": "Strategic Review",
            "confidence": "very high",  # Invalid
        }
        is_valid, error = validate_claude_response(response)
        assert is_valid is False
        assert "confidence" in error

    def test_negative_ev(self):
        """Should reject negative EV"""
        response = {
            "is_relevant": True,
            "company": "Siemens",
            "signal_type": "Strategic Review",
            "confidence": "high",
            "ev_low": -100,
        }
        is_valid, error = validate_claude_response(response)
        assert is_valid is False
        assert "ev_low" in error

    def test_non_dict_response(self):
        """Should reject non-dict response"""
        is_valid, error = validate_claude_response("not a dict")
        assert is_valid is False


class TestRunMetrics:
    """Tests for RunMetrics class"""

    def test_increment(self):
        """Should increment counters"""
        metrics = RunMetrics()
        metrics.increment("articles_collected", 5)
        assert metrics.counters["articles_collected"] == 5

    def test_set_source_count(self):
        """Should track source counts"""
        metrics = RunMetrics()
        metrics.set_source_count("rss", 100)
        assert metrics.sources["rss"] == 100

    def test_duration(self):
        """Should calculate duration"""
        metrics = RunMetrics()
        time.sleep(0.1)
        duration = metrics.get_duration_seconds()
        assert duration >= 0.1

    def test_to_dict(self):
        """Should export to dict"""
        metrics = RunMetrics()
        metrics.increment("entries_written", 10)
        data = metrics.to_dict()

        assert "counters" in data
        assert "sources" in data
        assert data["counters"]["entries_written"] == 10


class TestEnvironmentValidation:
    """Tests for environment validation"""

    def test_missing_variables(self):
        """Should detect missing variables"""
        # Save current env
        saved = {}
        for var in ["NOTION_API_KEY", "NOTION_DATABASE_ID", "ANTHROPIC_API_KEY"]:
            saved[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

        try:
            is_valid, missing = validate_environment()
            assert is_valid is False
            assert len(missing) == 3
        finally:
            # Restore env
            for var, val in saved.items():
                if val is not None:
                    os.environ[var] = val


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
