"""Tests for fetcher module (mocked HTTP calls)."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.models import Article
from carveout_monitor.fetcher import fetch_article_bodies, _extract_body, _MAX_CHARS


def _make_article(title="Test", url="https://example.com/1", summary=""):
    return Article(title=title, url=url, summary=summary)


# --- _extract_body tests ---

def test_extract_body_article_tag():
    html = """
    <html><body>
    <nav>Menu stuff</nav>
    <article><p>First paragraph.</p><p>Second paragraph with enough words to pass the threshold.
    This is a longer sentence to make sure we have at least fifty words in total for this test.
    We need to keep adding words so the extraction logic considers this valid body text content.
    More words here to pad it out sufficiently for the fifty word minimum check.</p></article>
    <footer>Footer stuff</footer>
    </body></html>
    """
    result = _extract_body(html)
    assert "First paragraph" in result
    assert "Menu stuff" not in result
    assert "Footer stuff" not in result


def test_extract_body_div_with_p_children():
    paragraphs = " ".join(f"<p>Paragraph {i} with some extra words to pad it out.</p>" for i in range(10))
    html = f"""
    <html><body>
    <div class="sidebar">Sidebar</div>
    <div class="content">{paragraphs}</div>
    </body></html>
    """
    result = _extract_body(html)
    assert "Paragraph 0" in result
    assert "Sidebar" not in result


def test_extract_body_bare_p_tags():
    html = """
    <html><body>
    <p>Only paragraph tags here with some content.</p>
    <p>Another paragraph with more text.</p>
    </body></html>
    """
    result = _extract_body(html)
    assert "Only paragraph" in result


def test_extract_body_strips_ads():
    html = """
    <html><body>
    <div class="advertisement">Buy now!</div>
    <article><p>""" + " ".join(["word"] * 60) + """</p></article>
    </body></html>
    """
    result = _extract_body(html)
    assert "Buy now" not in result


def test_extract_body_caps_at_max_chars():
    long_text = "word " * 5000
    html = f"<html><body><article><p>{long_text}</p></article></body></html>"
    result = _extract_body(html)
    assert len(result) <= _MAX_CHARS


# --- fetch_article_bodies tests ---

def test_fetch_bodies_success():
    articles = [_make_article(url="https://example.com/1")]
    body_html = "<html><body><article><p>" + " ".join(["content"] * 60) + "</p></article></body></html>"

    mock_resp = MagicMock()
    mock_resp.text = body_html
    mock_resp.raise_for_status = MagicMock()

    with patch("carveout_monitor.fetcher.requests.get", return_value=mock_resp):
        result = fetch_article_bodies(articles)

    assert len(result) == 1
    assert "content" in result[0].summary


def test_fetch_bodies_failure_keeps_original_summary():
    articles = [_make_article(url="https://example.com/1", summary="Original summary")]

    with patch("carveout_monitor.fetcher.requests.get", side_effect=Exception("Connection error")):
        result = fetch_article_bodies(articles)

    assert len(result) == 1
    assert result[0].summary == "Original summary"


def test_fetch_bodies_timeout_keeps_original():
    import requests as req
    articles = [_make_article(url="https://example.com/1", summary="Existing")]

    with patch("carveout_monitor.fetcher.requests.get", side_effect=req.Timeout("timed out")):
        result = fetch_article_bodies(articles)

    assert result[0].summary == "Existing"


def test_fetch_bodies_empty_list():
    result = fetch_article_bodies([])
    assert result == []
