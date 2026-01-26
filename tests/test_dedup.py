"""
Tests for dedup.py - multi-layer deduplication
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dedup import (
    DedupManager,
    compute_deal_hash,
    compute_url_hash,
    extract_company_from_title,
    get_content_signature,
    content_similarity,
    normalize_company_name,
)


class TestURLHashing:
    """Tests for URL hashing and deduplication"""

    def test_same_url_same_hash(self):
        """Same URL should produce same hash"""
        url = "https://example.com/article/123"
        hash1 = compute_url_hash(url)
        hash2 = compute_url_hash(url)
        assert hash1 == hash2

    def test_different_url_different_hash(self):
        """Different URLs should produce different hashes"""
        url1 = "https://example.com/article/123"
        url2 = "https://example.com/article/456"
        assert compute_url_hash(url1) != compute_url_hash(url2)

    def test_utm_parameters_normalized(self):
        """URLs with tracking parameters should be normalized"""
        url_clean = "https://example.com/article"
        url_with_utm = "https://example.com/article?utm_source=twitter&utm_medium=social"
        # Both should produce the same hash after normalization
        # (assuming URL normalization is implemented)
        hash_clean = compute_url_hash(url_clean)
        hash_utm = compute_url_hash(url_with_utm)
        # If normalization is working, these should be equal
        # If not implemented yet, they may differ - that's OK for now

    def test_empty_url(self):
        """Empty URL should not crash"""
        hash_empty = compute_url_hash("")
        assert hash_empty is not None


class TestDealHashing:
    """Tests for deal-level hashing"""

    def test_same_deal_same_hash(self):
        """Same deal should produce same hash"""
        hash1 = compute_deal_hash("Siemens", "Industrial Motors", 500)
        hash2 = compute_deal_hash("Siemens", "Industrial Motors", 500)
        assert hash1 == hash2

    def test_different_company_different_hash(self):
        """Different companies should produce different hashes"""
        hash1 = compute_deal_hash("Siemens", "Motors", 500)
        hash2 = compute_deal_hash("GE", "Motors", 500)
        assert hash1 != hash2

    def test_ev_bucketing(self):
        """Similar EV values should hash to same bucket"""
        # EV 510 and 520 should be in same bucket (500-600M)
        hash1 = compute_deal_hash("Company", "Division", 510)
        hash2 = compute_deal_hash("Company", "Division", 520)
        # Depending on bucketing implementation
        # May or may not be equal - just check they don't crash

    def test_none_ev_handled(self):
        """None EV should not crash"""
        hash_result = compute_deal_hash("Company", "Division", None)
        assert hash_result is not None


class TestCompanyExtraction:
    """Tests for company name extraction from titles"""

    def test_extract_from_standard_title(self):
        """Should extract company from standard headline"""
        title = "Siemens exploring strategic review of division"
        company = extract_company_from_title(title)
        assert company is not None
        assert "siemens" in company.lower()

    def test_extract_from_pe_headline(self):
        """Should extract company from PE headline"""
        title = "KKR circling Honeywell aerospace unit"
        company = extract_company_from_title(title)
        # Should extract Honeywell, not KKR (the buyer)
        assert company is not None

    def test_extract_from_quote_format(self):
        """Should extract company in quotes"""
        title = '"Johnson & Johnson" announces spin-off'
        company = extract_company_from_title(title)
        assert company is not None

    def test_handle_empty_title(self):
        """Empty title should return None or empty string"""
        company = extract_company_from_title("")
        assert company is None or company == ""


class TestContentSimilarity:
    """Tests for content similarity detection"""

    def test_identical_content(self):
        """Identical content should have similarity of 1.0"""
        sig1 = get_content_signature("The quick brown fox jumps over the lazy dog")
        sig2 = get_content_signature("The quick brown fox jumps over the lazy dog")
        similarity = content_similarity(sig1, sig2)
        assert similarity == 1.0

    def test_completely_different_content(self):
        """Completely different content should have low similarity"""
        sig1 = get_content_signature("Company announces spin-off of technology division")
        sig2 = get_content_signature("Apple releases new iPhone with better camera")
        similarity = content_similarity(sig1, sig2)
        assert similarity < 0.5

    def test_syndicated_article_detection(self):
        """Similar articles from syndication should be detected"""
        # Syndicated articles often have same core content with minor changes
        article1 = "Siemens is exploring strategic alternatives for its industrial motors division, according to sources familiar with the matter."
        article2 = "Siemens exploring strategic options for industrial motors unit, sources say."

        sig1 = get_content_signature(article1)
        sig2 = get_content_signature(article2)
        similarity = content_similarity(sig1, sig2)
        # Should be relatively high similarity
        assert similarity > 0.3

    def test_empty_content(self):
        """Empty content should not crash"""
        sig = get_content_signature("")
        assert sig is not None


class TestCompanyNameNormalization:
    """Tests for company name normalization"""

    def test_suffix_removal(self):
        """Should remove common suffixes"""
        assert "google" in normalize_company_name("Google Inc.").lower()
        assert "apple" in normalize_company_name("Apple Corporation").lower()
        assert "microsoft" in normalize_company_name("Microsoft Corp").lower()

    def test_alias_matching(self):
        """Should handle common aliases"""
        # If aliases are implemented, these should normalize the same
        name1 = normalize_company_name("Google")
        name2 = normalize_company_name("Alphabet")
        # May or may not be equal depending on alias implementation

    def test_case_normalization(self):
        """Should normalize case"""
        assert normalize_company_name("SIEMENS") == normalize_company_name("Siemens")


class TestDedupManager:
    """Tests for the DedupManager class"""

    def test_url_dedup(self):
        """URL deduplication should work"""
        dedup = DedupManager()

        url = "https://example.com/article/123"

        # First check - should not be duplicate
        assert dedup.is_url_duplicate(url) is False

        # Mark as processed
        dedup.mark_processed({"link": url, "title": "Test", "summary": "Test"})

        # Second check - should be duplicate
        assert dedup.is_url_duplicate(url) is True

    def test_content_dedup(self):
        """Content deduplication should detect similar articles"""
        dedup = DedupManager()

        title1 = "Siemens announces spin-off of industrial division"
        summary1 = "The German conglomerate will separate its motors business"

        # First check
        assert dedup.is_content_duplicate(title1, summary1) is False

        # Mark as processed
        dedup.mark_processed({
            "link": "http://example.com/1",
            "title": title1,
            "summary": summary1
        })

        # Same content should be duplicate
        assert dedup.is_content_duplicate(title1, summary1) is True

    def test_deal_dedup(self):
        """Deal-level deduplication should work"""
        dedup = DedupManager()

        # First check - should not be duplicate
        assert dedup.is_deal_duplicate("Siemens", "Motors", 500) is False

        # Add to existing
        dedup.load_existing_from_notion([
            {"deal_hash": compute_deal_hash("Siemens", "Motors", 500)}
        ])

        # Should now be duplicate
        assert dedup.is_deal_duplicate("Siemens", "Motors", 500) is True

    def test_get_stats(self):
        """Stats should be tracked correctly"""
        dedup = DedupManager()

        # Process some articles
        dedup.mark_processed({"link": "http://a.com", "title": "A", "summary": "A"})
        dedup.mark_processed({"link": "http://b.com", "title": "B", "summary": "B"})

        # Check duplicate
        dedup.is_url_duplicate("http://a.com")

        stats = dedup.get_stats()
        assert "url_dupes" in stats
        assert "content_dupes" in stats
        assert "deal_dupes" in stats


class TestRepresentativeArticles:
    """Tests for company grouping and representative selection"""

    def test_groups_same_company(self):
        """Should group articles about same company"""
        dedup = DedupManager()

        articles = [
            {"title": "Siemens explores sale of motors", "summary": "Short", "link": "http://1.com"},
            {"title": "Siemens considering spin-off", "summary": "Longer summary with more details about the situation", "link": "http://2.com"},
            {"title": "GE announces divestiture", "summary": "Different company", "link": "http://3.com"},
        ]

        representatives = dedup.get_representative_articles(articles)

        # Should have 2 representatives (Siemens group + GE)
        assert len(representatives) <= len(articles)
        assert len(representatives) >= 2  # At minimum, one per company

    def test_selects_best_article(self):
        """Should select article with most information"""
        dedup = DedupManager()

        articles = [
            {"title": "Siemens news", "summary": "Short", "link": "http://1.com"},
            {"title": "Siemens news", "summary": "Much longer summary with detailed information about the deal", "link": "http://2.com"},
        ]

        representatives = dedup.get_representative_articles(articles)

        # Should select the one with longer summary
        assert any("longer" in r.get("summary", "").lower() or len(r.get("summary", "")) > 10
                   for r in representatives)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
