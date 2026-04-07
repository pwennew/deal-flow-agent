"""Tests for TAM account monitoring — firm name matching in article titles."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.models import Article, Firm, TamAlert
from carveout_monitor.tam_monitor import scan_for_tam_mentions


def _make_article(title: str, url: str = "https://example.com/article", summary: str = "") -> Article:
    return Article(title=title, url=url, summary=summary)


def _make_firm(name: str, domain: str = "") -> Firm:
    return Firm(name=name, domain=domain)


FIRMS = [
    _make_firm("One Rock Capital Partners", "onerockcapital.com"),
    _make_firm("Insight Partners", "insightpartners.com"),
    _make_firm("Francisco Partners", "franciscopartners.com"),
]


def test_exact_match_in_title():
    articles = [_make_article("One Rock Capital Partners Acquires Widget Corp")]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 1
    assert alerts[0].matched_firms == ["One Rock Capital Partners"]
    assert alerts[0].match_locations == ["title"]


def test_case_insensitive_match():
    articles = [_make_article("one rock capital partners announces new fund")]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 1
    assert alerts[0].matched_firms == ["One Rock Capital Partners"]


def test_no_partial_match():
    """'Rock Capital' alone should not match 'One Rock Capital Partners'."""
    articles = [_make_article("Rock Capital Acquires Business Unit")]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 0


def test_no_substring_match():
    """'Rockwell Automation' should not match any firm."""
    articles = [_make_article("Rockwell Automation Reports Q4 Earnings")]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 0


def test_multiple_firms_in_title():
    articles = [_make_article("One Rock Capital Partners and Insight Partners Announce Joint Venture")]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 1
    assert set(alerts[0].matched_firms) == {"One Rock Capital Partners", "Insight Partners"}
    assert len(alerts[0].match_locations) == 2


def test_title_only_not_summary():
    """Firm name in summary but not title should NOT match (title-only mode)."""
    articles = [_make_article(
        title="Major PE Deal Announced in Industrial Sector",
        summary="One Rock Capital Partners is acquiring a division of Acme Corp.",
    )]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 0


def test_no_match_returns_empty():
    articles = [_make_article("Blackstone Announces New Fund")]
    alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(alerts) == 0


def test_firm_subset_filtering():
    """Only match against firms in the subset."""
    articles = [
        _make_article("One Rock Capital Partners Acquires Widget Corp"),
        _make_article("Insight Partners Leads Series B Round"),
    ]
    alerts = scan_for_tam_mentions(articles, FIRMS, firm_subset=["Insight Partners"])
    assert len(alerts) == 1
    assert alerts[0].matched_firms == ["Insight Partners"]


def test_empty_articles():
    alerts = scan_for_tam_mentions([], FIRMS)
    assert alerts == []


def test_empty_firms():
    articles = [_make_article("One Rock Capital Partners Acquires Widget Corp")]
    alerts = scan_for_tam_mentions(articles, [])
    assert alerts == []


def test_dedup_against_classifier_results():
    """Verify that TAM alerts can be filtered against classifier URLs."""
    articles = [
        _make_article("One Rock Capital Partners Acquires Widget Corp", url="https://example.com/1"),
        _make_article("Insight Partners Leads Series B Round", url="https://example.com/2"),
    ]
    tam_alerts = scan_for_tam_mentions(articles, FIRMS)
    assert len(tam_alerts) == 2

    # Simulate classifier catching the One Rock article
    classifier_urls = {"https://example.com/1"}
    tam_only = [t for t in tam_alerts if t.article.url not in classifier_urls]
    assert len(tam_only) == 1
    assert tam_only[0].matched_firms == ["Insight Partners"]


def test_word_boundary_prevents_embedded_match():
    """Firm name embedded in a larger word should not match."""
    firms = [_make_firm("Atlas Partners")]
    articles = [_make_article("AtlasPartners.com Launches New Platform")]
    alerts = scan_for_tam_mentions(articles, firms)
    # "AtlasPartners" has no word boundary between Atlas and Partners — no match
    assert len(alerts) == 0


def test_firm_name_with_special_chars():
    """Firm names with special regex characters should be escaped properly."""
    firms = [_make_firm("H.I.G. Capital")]
    articles = [_make_article("H.I.G. Capital Acquires Manufacturing Division")]
    alerts = scan_for_tam_mentions(articles, firms)
    assert len(alerts) == 1
    assert alerts[0].matched_firms == ["H.I.G. Capital"]
