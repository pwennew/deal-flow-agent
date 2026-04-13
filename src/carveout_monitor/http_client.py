"""Shared HTTP client with resilience: SSL fallback, retries, 403→Playwright escalation.

All network calls in feeds.py, scraper.py, and fetcher.py should go through
`resilient_get()` rather than calling `requests.get()` directly. This centralises:

- Realistic browser headers (avoids bot-UA 403 blocks)
- SSL: verify with certifi CA bundle, per-call fallback to verify=False on SSLError
- DNS/connection retries with exponential backoff
- 403 → Playwright headless fetch (optional)
- Per-firm error tracking via StateManager (404 counter, prefer_playwright)
"""

from __future__ import annotations

import logging
import time
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING

import certifi
import requests
import urllib3

if TYPE_CHECKING:
    from .state import StateManager

logger = logging.getLogger(__name__)

# Realistic Chrome headers. The old "CarveOutMonitor/1.0" UA was getting 403s
# from Carlyle, Ares, PSG, and Ropes & Gray — those sites WAF on bot-looking UAs.
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

_DEFAULT_HEADERS = {
    "User-Agent": _BROWSER_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

_DEFAULT_TIMEOUT = 30
_MAX_CONNECTION_RETRIES = 2
_BACKOFF_SECONDS = (1.0, 2.0)


@dataclass
class ResilientResponse:
    """Wraps a requests.Response plus metadata about how it was obtained.

    `error_type` is None on success, or one of: "ssl", "dns", "timeout",
    "403", "404", "http_error", "playwright_failed".
    """

    status_code: int
    text: str
    url: str
    error_type: str | None = None
    used_playwright: bool = False
    ssl_fallback_used: bool = False

    @property
    def ok(self) -> bool:
        return self.status_code == 200 and self.error_type is None


def browser_headers() -> dict[str, str]:
    """Return a copy of the realistic browser headers dict."""
    return dict(_DEFAULT_HEADERS)


def resilient_get(
    url: str,
    *,
    timeout: int = _DEFAULT_TIMEOUT,
    playwright_fallback: bool = False,
    firm_name: str | None = None,
    state: "StateManager | None" = None,
    extra_headers: dict[str, str] | None = None,
) -> ResilientResponse:
    """Fetch a URL with SSL fallback, connection retries, and optional Playwright escalation.

    Parameters:
        url: the URL to fetch.
        timeout: request timeout in seconds (default 30, matches Playwright).
        playwright_fallback: if True, 403 responses trigger a Playwright headless fetch
            and the firm gets flagged `prefer_playwright=True` in state for future runs.
        firm_name: used for state tracking of 404s and prefer_playwright.
        state: StateManager to record per-firm errors/successes.
        extra_headers: merged on top of the default browser headers.
    """
    headers = dict(_DEFAULT_HEADERS)
    if extra_headers:
        headers.update(extra_headers)

    # --- Connection/DNS retry loop (covers ConnectionError incl. DNS failures) ---
    last_exc: Exception | None = None
    for attempt in range(_MAX_CONNECTION_RETRIES + 1):
        try:
            resp = _attempt_with_ssl_fallback(url, headers, timeout)
            return _handle_response(resp, url, firm_name, state, playwright_fallback)
        except requests.exceptions.ConnectionError as e:
            last_exc = e
            if attempt < _MAX_CONNECTION_RETRIES:
                backoff = _BACKOFF_SECONDS[attempt]
                logger.warning(
                    "Connection/DNS error for %s (attempt %d/%d): %s — retrying in %.1fs",
                    url, attempt + 1, _MAX_CONNECTION_RETRIES + 1, e, backoff,
                )
                time.sleep(backoff)
                continue
            logger.warning(
                "Connection/DNS failed for %s after %d attempts — may be stale URL or transient: %s",
                url, _MAX_CONNECTION_RETRIES + 1, e,
            )
            if state and firm_name:
                state.record_firm_error(firm_name, "dns")
            return ResilientResponse(status_code=0, text="", url=url, error_type="dns")
        except requests.exceptions.Timeout as e:
            logger.warning("Timeout fetching %s: %s", url, e)
            if state and firm_name:
                state.record_firm_error(firm_name, "timeout")
            return ResilientResponse(status_code=0, text="", url=url, error_type="timeout")
        except requests.RequestException as e:
            last_exc = e
            logger.warning("Request failed for %s: %s", url, e)
            return ResilientResponse(status_code=0, text="", url=url, error_type="http_error")

    # Unreachable, but keeps type checker happy
    return ResilientResponse(status_code=0, text="", url=url, error_type="http_error")


def _attempt_with_ssl_fallback(
    url: str, headers: dict[str, str], timeout: int,
) -> requests.Response:
    """Try with verify=certifi.where() first; on SSLError, retry once with verify=False."""
    try:
        return requests.get(
            url, headers=headers, timeout=timeout,
            verify=certifi.where(), allow_redirects=True,
        )
    except requests.exceptions.SSLError as e:
        logger.warning(
            "SSL verification failed for %s (%s) — retrying with verify=False",
            url, e,
        )
        # Suppress the InsecureRequestWarning only for this single retry
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
            resp = requests.get(
                url, headers=headers, timeout=timeout,
                verify=False, allow_redirects=True,
            )
        # Mark it so the caller knows SSL was bypassed
        resp._ssl_fallback_used = True  # type: ignore[attr-defined]
        return resp


def _handle_response(
    resp: requests.Response,
    url: str,
    firm_name: str | None,
    state: "StateManager | None",
    playwright_fallback: bool,
) -> ResilientResponse:
    """Map a raw response to a ResilientResponse, handling 403/404/state tracking."""
    ssl_fallback = getattr(resp, "_ssl_fallback_used", False)

    if resp.status_code == 403 and playwright_fallback:
        logger.warning("403 blocked on %s — escalating to Playwright", url)
        pw_html = _fetch_with_playwright(url)
        if pw_html is not None:
            if state and firm_name:
                state.record_firm_success(firm_name)
                state.mark_prefer_playwright(firm_name)
            return ResilientResponse(
                status_code=200, text=pw_html, url=url,
                error_type=None, used_playwright=True,
                ssl_fallback_used=ssl_fallback,
            )
        if state and firm_name:
            state.record_firm_error(firm_name, "403")
        return ResilientResponse(
            status_code=403, text="", url=url,
            error_type="playwright_failed", ssl_fallback_used=ssl_fallback,
        )

    if resp.status_code == 403:
        if state and firm_name:
            state.record_firm_error(firm_name, "403")
        return ResilientResponse(
            status_code=403, text=resp.text, url=url,
            error_type="403", ssl_fallback_used=ssl_fallback,
        )

    if resp.status_code == 404:
        if state and firm_name:
            state.record_firm_error(firm_name, "404")
        return ResilientResponse(
            status_code=404, text=resp.text, url=url,
            error_type="404", ssl_fallback_used=ssl_fallback,
        )

    if resp.status_code != 200:
        return ResilientResponse(
            status_code=resp.status_code, text=resp.text, url=url,
            error_type="http_error", ssl_fallback_used=ssl_fallback,
        )

    # Success
    if state and firm_name:
        state.record_firm_success(firm_name)
    return ResilientResponse(
        status_code=200, text=resp.text, url=url,
        error_type=None, ssl_fallback_used=ssl_fallback,
    )


def _fetch_with_playwright(url: str) -> str | None:
    """Fetch a URL using Playwright headless Chromium. Returns rendered HTML or None."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not installed — cannot escalate 403 for %s", url)
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=_BROWSER_UA)
            page.goto(url, timeout=30000, wait_until="networkidle")
            html = page.content()
            browser.close()
        return html
    except Exception as e:
        logger.warning("Playwright fetch failed for %s: %s", url, e)
        return None
