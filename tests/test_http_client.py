"""Tests for the shared resilient HTTP client."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor import http_client
from carveout_monitor.http_client import resilient_get, browser_headers


def _mock_resp(status_code: int = 200, text: str = ""):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    return resp


def test_sends_realistic_browser_headers():
    captured = {}

    def fake_get(url, **kwargs):
        captured.update(kwargs)
        return _mock_resp(200, "<html></html>")

    with patch("carveout_monitor.http_client.requests.get", side_effect=fake_get):
        resilient_get("https://example.com")

    ua = captured["headers"]["User-Agent"]
    assert "Chrome" in ua
    assert "Mozilla" in ua
    assert "CarveOutMonitor" not in ua  # old bot UA must be gone
    assert "Accept-Language" in captured["headers"]
    assert "Accept" in captured["headers"]


def test_timeout_default_30s():
    captured = {}

    def fake_get(url, **kwargs):
        captured.update(kwargs)
        return _mock_resp(200, "ok")

    with patch("carveout_monitor.http_client.requests.get", side_effect=fake_get):
        resilient_get("https://example.com")

    assert captured["timeout"] == 30


def test_ssl_fallback_on_ssl_error():
    """On SSLError, retry once with verify=False and succeed."""
    call_kwargs = []

    def fake_get(url, **kwargs):
        call_kwargs.append(kwargs)
        if len(call_kwargs) == 1:
            raise requests.exceptions.SSLError("cert verify failed")
        return _mock_resp(200, "fallback ok")

    with patch("carveout_monitor.http_client.requests.get", side_effect=fake_get):
        resp = resilient_get("https://badssl.example.com")

    assert resp.ok
    assert resp.ssl_fallback_used
    # First call used certifi bundle path (a string), second used verify=False
    assert call_kwargs[0]["verify"] != False
    assert call_kwargs[1]["verify"] is False


def test_dns_retry_with_backoff():
    """ConnectionError retries 2× with backoff, then succeeds."""
    attempts = []

    def fake_get(url, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            raise requests.exceptions.ConnectionError("DNS resolution failed")
        return _mock_resp(200, "finally ok")

    with patch("carveout_monitor.http_client.requests.get", side_effect=fake_get), \
         patch("carveout_monitor.http_client.time.sleep") as mock_sleep:
        resp = resilient_get("https://missing.example.com")

    assert resp.ok
    assert len(attempts) == 3
    # Two backoff sleeps (1s, 2s)
    assert mock_sleep.call_count == 2


def test_dns_retry_exhausted():
    """After 3 ConnectionErrors, returns error_type='dns'."""
    def fake_get(url, **kwargs):
        raise requests.exceptions.ConnectionError("DNS failed")

    with patch("carveout_monitor.http_client.requests.get", side_effect=fake_get), \
         patch("carveout_monitor.http_client.time.sleep"):
        resp = resilient_get("https://missing.example.com")

    assert not resp.ok
    assert resp.error_type == "dns"


def test_404_records_state_counter():
    """404 responses should increment consecutive_404s in state."""
    state = MagicMock()

    with patch("carveout_monitor.http_client.requests.get",
               return_value=_mock_resp(404, "Not Found")):
        resp = resilient_get("https://example.com/gone",
                             firm_name="TestFirm", state=state)

    assert resp.error_type == "404"
    state.record_firm_error.assert_called_once_with("TestFirm", "404")


def test_403_triggers_playwright_fallback():
    """403 + playwright_fallback=True invokes Playwright and marks prefer_playwright."""
    state = MagicMock()

    with patch("carveout_monitor.http_client.requests.get",
               return_value=_mock_resp(403, "Forbidden")), \
         patch("carveout_monitor.http_client._fetch_with_playwright",
               return_value="<html>rendered by playwright</html>") as mock_pw:
        resp = resilient_get("https://blocked.example.com",
                             playwright_fallback=True,
                             firm_name="TestFirm", state=state)

    assert resp.ok
    assert resp.used_playwright
    assert "rendered" in resp.text
    mock_pw.assert_called_once()
    state.mark_prefer_playwright.assert_called_once_with("TestFirm")


def test_403_without_playwright_fallback_returns_error():
    """403 without playwright_fallback returns error_type='403' and records error."""
    state = MagicMock()

    with patch("carveout_monitor.http_client.requests.get",
               return_value=_mock_resp(403, "Forbidden")):
        resp = resilient_get("https://blocked.example.com",
                             playwright_fallback=False,
                             firm_name="TestFirm", state=state)

    assert resp.error_type == "403"
    assert not resp.used_playwright
    state.record_firm_error.assert_called_once_with("TestFirm", "403")


def test_success_records_firm_success():
    state = MagicMock()

    with patch("carveout_monitor.http_client.requests.get",
               return_value=_mock_resp(200, "ok")):
        resilient_get("https://example.com", firm_name="TestFirm", state=state)

    state.record_firm_success.assert_called_once_with("TestFirm")


def test_browser_headers_copy_is_independent():
    h = browser_headers()
    h["X-Custom"] = "y"
    assert "X-Custom" not in browser_headers()
