"""Tests for feed discovery and fetching (mocked HTTP)."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.models import Firm
from carveout_monitor.feeds import discover_feed, _fetch_single_feed

_SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
<title>Test Feed</title>
<item>
  <title>Blackstone acquires division from Acme</title>
  <link>https://blackstone.com/news/1</link>
  <description>Blackstone agreed to acquire...</description>
  <pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate>
</item>
<item>
  <title>Another press release</title>
  <link>https://blackstone.com/news/2</link>
  <pubDate>Sun, 31 Dec 2023 12:00:00 GMT</pubDate>
</item>
</channel>
</rss>"""


def test_discover_feed_found():
    firm = Firm(name="TestFirm", domain="testfirm.com")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = _SAMPLE_RSS
    mock_resp.headers = {"Content-Type": "application/rss+xml"}

    with patch("carveout_monitor.feeds.requests.get", return_value=mock_resp):
        with patch("carveout_monitor.feeds.feedparser") as mock_fp:
            mock_fp.parse.return_value = MagicMock(entries=[{"title": "test"}])
            url = discover_feed(firm)

    assert url is not None
    assert "testfirm.com" in url


def test_discover_feed_no_domain():
    firm = Firm(name="TestFirm")
    url = discover_feed(firm)
    assert url is None


def test_fetch_single_feed():
    firm = Firm(name="TestFirm", domain="testfirm.com",
                feed_url="https://testfirm.com/feed")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = _SAMPLE_RSS

    with patch("carveout_monitor.feeds.requests.get", return_value=mock_resp):
        articles = _fetch_single_feed(firm)

    assert len(articles) == 2
    assert articles[0].firm_name == "TestFirm"


def test_fetch_no_feed_url():
    firm = Firm(name="TestFirm")
    articles = _fetch_single_feed(firm)
    assert articles == []
