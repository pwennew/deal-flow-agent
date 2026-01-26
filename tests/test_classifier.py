"""
Tests for classifier.py - keyword-based article classification
"""

import pytest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from classifier import classify_article, classify_batch, ClassificationResult


class TestClassifyArticle:
    """Tests for the classify_article function"""

    def test_high_signal_carveout(self):
        """Articles about carve-outs should score high"""
        result = classify_article(
            title="Siemens exploring strategic alternatives for industrial motors division",
            summary="The German conglomerate has hired Goldman Sachs to advise on potential sale or spin-off",
            source="Financial Times"
        )
        assert result.score >= 6, f"Expected high score, got {result.score}"
        assert result.should_analyze is True
        assert "carve" in result.reason.lower() or "high" in result.reason.lower()

    def test_hard_reject_ipo(self):
        """IPO articles should be rejected"""
        result = classify_article(
            title="Company announces IPO plans",
            summary="Tech company plans initial public offering",
            source="TechCrunch"
        )
        assert result.should_analyze is False
        assert "ipo" in str(result.hard_reject_matches).lower() or result.score == 0

    def test_hard_reject_series_a(self):
        """Venture funding should be rejected"""
        result = classify_article(
            title="Startup raises Series A funding",
            summary="Venture capital firm leads $50M round",
            source="TechCrunch"
        )
        assert result.should_analyze is False
        assert len(result.hard_reject_matches) > 0 or result.score < 3

    def test_hard_reject_bolt_on(self):
        """Bolt-on acquisitions should be rejected"""
        result = classify_article(
            title="PE firm makes bolt-on acquisition for portfolio company",
            summary="The deal represents another tuck-in acquisition",
            source="PE Hub"
        )
        assert result.should_analyze is False

    def test_irrelevant_tech_news(self):
        """General tech news should not be analyzed"""
        result = classify_article(
            title="Apple announces new iPhone",
            summary="Tech giant unveils latest smartphone",
            source="TechCrunch"
        )
        assert result.should_analyze is False
        assert result.score < 3

    def test_premium_source_bonus(self):
        """Premium sources should get score bonus"""
        result = classify_article(
            title="Company exploring sale of division",
            summary="Strategic review underway",
            source="Financial Times"
        )
        assert result.is_premium_source is True

        result_non_premium = classify_article(
            title="Company exploring sale of division",
            summary="Strategic review underway",
            source="Random Blog"
        )
        # Premium source should score higher
        assert result.score >= result_non_premium.score

    def test_negative_keywords_reduce_score(self):
        """Negative keywords should reduce score"""
        result_with_negative = classify_article(
            title="Real estate private equity firm announces acquisition",
            summary="The REIT focused fund is buying properties",
            source="Reuters"
        )

        result_without_negative = classify_article(
            title="Private equity firm announces acquisition of industrial division",
            summary="The buyout fund is acquiring manufacturing assets",
            source="Reuters"
        )

        assert result_without_negative.score > result_with_negative.score

    def test_divestiture_signals(self):
        """Core divestiture signals should be detected"""
        test_cases = [
            ("Strategic review announced for healthcare unit", "Healthcare division under review", True),
            ("Company to spin off technology division", "Spin-off expected next quarter", True),
            ("Conglomerate divesting non-core assets", "Divestiture of consumer business", True),
            ("Adviser appointed for sale process", "Goldman Sachs retained as adviser", True),
        ]

        for title, summary, should_analyze in test_cases:
            result = classify_article(title, summary, "Bloomberg")
            assert result.should_analyze == should_analyze, \
                f"Expected should_analyze={should_analyze} for: {title}"

    def test_pe_buyer_activity(self):
        """PE buyer activity should be detected"""
        result = classify_article(
            title="KKR and Carlyle circling Honeywell division",
            summary="Private equity firms bidding for aerospace unit",
            source="Reuters"
        )
        assert len(result.pe_matches) > 0 or result.score >= 3

    def test_context_conflict_penalty(self):
        """Context conflicts should be penalized"""
        # Real estate near private equity should be penalized
        result = classify_article(
            title="Private equity real estate fund launches",
            summary="The private equity firm is focusing on real estate investments",
            source="Bloomberg"
        )
        # Should be lower score due to context conflict
        assert result.should_analyze is False or result.score < 5


class TestClassifyBatch:
    """Tests for batch classification"""

    def test_batch_splits_correctly(self):
        """Batch classification should correctly split articles"""
        articles = [
            {"title": "Carve-out announced", "summary": "Spin-off of division", "source": "FT"},
            {"title": "New iPhone released", "summary": "Tech product launch", "source": "TC"},
            {"title": "Series A funding", "summary": "Startup raises capital", "source": "TC"},
        ]

        to_analyze, to_skip = classify_batch(articles)

        assert len(to_analyze) + len(to_skip) == len(articles)
        assert len(to_analyze) >= 1  # Carve-out should pass
        assert len(to_skip) >= 1  # iPhone should fail

    def test_batch_adds_classification_info(self):
        """Batch should add _classification field to articles"""
        articles = [
            {"title": "Strategic review announced", "summary": "Division for sale", "source": "Bloomberg"},
        ]

        to_analyze, _ = classify_batch(articles)

        for article in to_analyze:
            assert "_classification" in article
            assert "score" in article["_classification"]
            assert "reason" in article["_classification"]


class TestEdgeCases:
    """Edge case tests"""

    def test_empty_strings(self):
        """Should handle empty strings gracefully"""
        result = classify_article("", "", "")
        assert result.score == 0
        assert result.should_analyze is False

    def test_none_handling(self):
        """Should handle None-like inputs"""
        # Passing empty dict values (simulating missing keys)
        result = classify_article(
            title="",
            summary="",
            source=""
        )
        assert isinstance(result, ClassificationResult)

    def test_unicode_content(self):
        """Should handle unicode content"""
        result = classify_article(
            title="Siemens untersucht strategische Alternativen",
            summary="Der Konzern prüft einen Verkauf der Sparte",
            source="Handelsblatt"
        )
        assert isinstance(result, ClassificationResult)

    def test_very_long_content(self):
        """Should handle very long content"""
        long_summary = "Strategic review " * 1000
        result = classify_article(
            title="Company announces review",
            summary=long_summary,
            source="Bloomberg"
        )
        assert isinstance(result, ClassificationResult)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
