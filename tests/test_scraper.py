"""Tests for scraper routing behaviour (per-firm error tracking, Playwright escalation)."""

import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.models import Firm
from carveout_monitor.scraper import scrape_firm, scrape_articles


def test_scrape_firm_skips_needs_url_update():
    """Firms flagged needs_url_update should return [] without fetching."""
    firm = Firm(name="Triton", domain="triton-partners.com",
                press_url="https://triton-partners.com/news")
    state = MagicMock()
    state.should_skip_firm.return_value = True

    with patch("carveout_monitor.scraper._scrape_press_page_html") as html_mock, \
         patch("carveout_monitor.scraper._scrape_press_page_playwright") as pw_mock:
        result = scrape_firm(firm, state=state)

    assert result == []
    html_mock.assert_not_called()
    pw_mock.assert_not_called()


def test_scrape_firm_prefers_playwright_when_flagged():
    """Firms with prefer_playwright=True skip HTML and go straight to Playwright."""
    firm = Firm(name="Carlyle", domain="carlyle.com",
                press_url="https://carlyle.com/news")
    state = MagicMock()
    state.should_skip_firm.return_value = False
    state.prefers_playwright.return_value = True

    fake_articles = [MagicMock()]
    with patch("carveout_monitor.scraper._scrape_press_page_html") as html_mock, \
         patch("carveout_monitor.scraper._scrape_press_page_playwright",
               return_value=fake_articles) as pw_mock:
        result = scrape_firm(firm, state=state)

    html_mock.assert_not_called()
    pw_mock.assert_called_once_with(firm)
    assert result == fake_articles


def test_scrape_firm_403_escalates_to_playwright():
    """HTML returning 403 triggers immediate Playwright fallback and marks prefer_playwright."""
    firm = Firm(name="Ropes", domain="ropesgray.com",
                press_url="https://ropesgray.com/news")
    state = MagicMock()
    state.should_skip_firm.return_value = False
    state.prefers_playwright.return_value = False

    fake_articles = [MagicMock()]
    with patch("carveout_monitor.scraper._scrape_press_page_html",
               return_value=([], "403")) as html_mock, \
         patch("carveout_monitor.scraper._scrape_press_page_playwright",
               return_value=fake_articles) as pw_mock:
        result = scrape_firm(firm, state=state)

    html_mock.assert_called_once()
    pw_mock.assert_called_once_with(firm)
    state.mark_prefer_playwright.assert_called_once_with("Ropes")
    assert result == fake_articles


def test_scrape_firm_dns_error_does_not_escalate():
    """DNS errors shouldn't trigger Playwright (Playwright would also fail)."""
    firm = Firm(name="LindsayG", domain="lindsaygoldberg.com",
                press_url="https://lindsaygoldberg.com/news")
    state = MagicMock()
    state.should_skip_firm.return_value = False
    state.prefers_playwright.return_value = False

    with patch("carveout_monitor.scraper._scrape_press_page_html",
               return_value=([], "dns")), \
         patch("carveout_monitor.scraper._scrape_press_page_playwright") as pw_mock:
        result = scrape_firm(firm, state=state)

    assert result == []
    pw_mock.assert_not_called()


def test_scrape_articles_keeps_undated():
    """Undated articles must be kept so state dedup can handle re-processing.

    Previously we dropped all undated articles on the assumption they came
    from multi-year press archives. This silently lost legitimate recent
    articles whenever the press page date format wasn't understood. The new
    behaviour admits undated articles; the state.seen URL dedup downstream
    ensures each archive article is classified at most once.
    """
    from datetime import datetime, timedelta, timezone
    from carveout_monitor.models import Article

    firms = [Firm(name="F1", domain="f1.com", press_url="https://f1.com/news")]
    now = datetime.now(timezone.utc)

    fake_articles = [
        # Dated, within window — kept
        Article(title="Recent deal", url="https://f1.com/a",
                summary="", published=now - timedelta(hours=2), firm_name="F1"),
        # Dated, outside window — dropped
        Article(title="Old deal", url="https://f1.com/b",
                summary="", published=now - timedelta(days=30), firm_name="F1"),
        # Undated — kept (was previously dropped)
        Article(title="Undated deal", url="https://f1.com/c",
                summary="", published=None, firm_name="F1"),
    ]

    with patch("carveout_monitor.scraper.scrape_firm", return_value=fake_articles):
        result = scrape_articles(firms, lookback_hours=24)

    urls = {a.url for a in result}
    assert "https://f1.com/a" in urls      # dated, in window
    assert "https://f1.com/b" not in urls  # dated, outside window
    assert "https://f1.com/c" in urls      # undated → kept now


def test_global_timeout_logs_firm_names(caplog):
    """On global scrape timeout, log firm names, not just counts."""
    import threading
    firms = [
        Firm(name="SlowFirmA", domain="a.com", press_url="https://a.com/news"),
        Firm(name="SlowFirmB", domain="b.com", press_url="https://b.com/news"),
    ]

    # Block scrape_firm so futures never complete before the global timeout fires
    never = threading.Event()

    def hang(*args, **kwargs):
        never.wait(timeout=10)
        return []

    def fake_as_completed(futures, timeout=None):
        raise TimeoutError("global timeout")

    with patch("carveout_monitor.scraper.as_completed", side_effect=fake_as_completed), \
         patch("carveout_monitor.scraper.scrape_firm", side_effect=hang):
        with caplog.at_level(logging.WARNING, logger="carveout_monitor.scraper"):
            scrape_articles(firms, lookback_hours=24)
    never.set()  # release the hanging workers

    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "Global scrape timeout" in messages
    # Both firm names must appear in the log (neither completed)
    assert "SlowFirmA" in messages
    assert "SlowFirmB" in messages
