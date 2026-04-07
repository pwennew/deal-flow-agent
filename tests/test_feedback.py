"""Tests for feedback module (accuracy computation, no Notion calls)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.feedback import compute_accuracy, format_report, format_slack_report


def _row(verdict="Pending", action="pursue", deal_type="corporate_carveout",
         title="Test Deal", target="Target Co", seller="Seller Co", score="75"):
    return {
        "page_id": "abc",
        "title": title,
        "verdict": verdict,
        "action": action,
        "deal_type": deal_type,
        "target": target,
        "seller": seller,
        "buyer": "Buyer PE",
        "pe_firm": "Buyer PE",
        "score": score,
        "url": "https://example.com",
    }


def test_all_pending():
    rows = [_row(), _row(), _row()]
    stats = compute_accuracy(rows)
    assert stats["total"] == 3
    assert stats["reviewed"] == 0
    assert stats["pending"] == 3
    assert stats["confirmed"] == 0
    assert stats["false_positives"] == 0
    assert stats["precision"] == 0.0


def test_all_confirmed():
    rows = [_row(verdict="Confirmed"), _row(verdict="Confirmed")]
    stats = compute_accuracy(rows)
    assert stats["reviewed"] == 2
    assert stats["confirmed"] == 2
    assert stats["false_positives"] == 0
    assert stats["precision"] == 1.0


def test_mixed_verdicts():
    rows = [
        _row(verdict="Confirmed"),
        _row(verdict="Confirmed"),
        _row(verdict="Not a Carve-Out"),
        _row(verdict="Wrong Geography"),
        _row(verdict="Pending"),
    ]
    stats = compute_accuracy(rows)
    assert stats["total"] == 5
    assert stats["reviewed"] == 4
    assert stats["pending"] == 1
    assert stats["confirmed"] == 2
    assert stats["false_positives"] == 2
    assert stats["precision"] == 0.5


def test_fp_by_reason_breakdown():
    rows = [
        _row(verdict="Not a Carve-Out"),
        _row(verdict="Not a Carve-Out"),
        _row(verdict="Low EV"),
        _row(verdict="Wrong Geography"),
    ]
    stats = compute_accuracy(rows)
    assert stats["fp_by_reason"]["Not a Carve-Out"] == 2
    assert stats["fp_by_reason"]["Low EV"] == 1
    assert stats["fp_by_reason"]["Wrong Geography"] == 1


def test_fp_by_deal_type():
    rows = [
        _row(verdict="Not a Carve-Out", deal_type="corporate_carveout"),
        _row(verdict="Low EV", deal_type="corporate_carveout"),
        _row(verdict="Wrong Geography", deal_type="portco_carveout"),
    ]
    stats = compute_accuracy(rows)
    assert stats["fp_by_deal_type"]["corporate_carveout"] == 2
    assert stats["fp_by_deal_type"]["portco_carveout"] == 1


def test_fp_by_action():
    rows = [
        _row(verdict="Not a Carve-Out", action="pursue"),
        _row(verdict="Low EV", action="monitor"),
        _row(verdict="Wrong Geography", action="pursue"),
    ]
    stats = compute_accuracy(rows)
    assert stats["fp_by_action"]["pursue"] == 2
    assert stats["fp_by_action"]["monitor"] == 1


def test_examples_capped_at_10():
    rows = [_row(verdict="Not a Carve-Out", title=f"Deal {i}") for i in range(15)]
    stats = compute_accuracy(rows)
    assert len(stats["examples"]) == 15  # all stored
    report = format_report(stats)
    # Report should only show 10 examples
    assert report.count("[Not a Carve-Out]") == 10


def test_format_report_runs():
    rows = [_row(verdict="Confirmed"), _row(verdict="Not a Carve-Out")]
    stats = compute_accuracy(rows)
    report = format_report(stats)
    assert "Precision:" in report
    assert "50%" in report


def test_format_slack_report_runs():
    rows = [_row(verdict="Confirmed"), _row(verdict="Low EV")]
    stats = compute_accuracy(rows)
    msg = format_slack_report(stats)
    assert "Precision" in msg
    assert "50%" in msg


def test_empty_rows():
    stats = compute_accuracy([])
    assert stats["total"] == 0
    assert stats["precision"] == 0.0
    report = format_report(stats)
    assert "Total alerts" in report
