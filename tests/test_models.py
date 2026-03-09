"""Tests for data models."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.models import Firm, Article, DealAlert, DealStage, load_firms


def test_firm_creation():
    firm = Firm(name="Blackstone", domain="blackstone.com", feed_url="https://blackstone.com/feed")
    assert firm.name == "Blackstone"
    assert firm.domain == "blackstone.com"
    assert firm.feed_url == "https://blackstone.com/feed"


def test_firm_defaults():
    firm = Firm(name="KKR")
    assert firm.domain == ""
    assert firm.feed_url is None
    assert firm.press_url is None
    assert firm.sectors == []


def test_article_creation():
    article = Article(title="Test Article", url="https://example.com/article", firm_name="Blackstone")
    assert article.title == "Test Article"
    assert article.summary == ""
    assert article.published is None


def test_deal_alert_defaults():
    article = Article(title="Test", url="https://example.com")
    alert = DealAlert(article=article)
    assert alert.is_carveout is False
    assert alert.stage is None
    assert alert.confidence == 0


def test_deal_alert_carveout():
    article = Article(title="Deal signed", url="https://example.com")
    alert = DealAlert(
        article=article,
        is_carveout=True,
        stage=DealStage.SIGNING,
        target_company="Widget Division",
        seller="Acme Corp",
        confidence=90,
    )
    assert alert.is_carveout is True
    assert alert.stage == DealStage.SIGNING
    assert alert.target_company == "Widget Division"
    assert alert.seller == "Acme Corp"


def test_deal_stage_values():
    assert DealStage.SIGNING.value == "signing"
    assert DealStage.CLOSING.value == "closing"


def test_load_firms(tmp_path):
    targets = tmp_path / "targets.yml"
    targets.write_text("""
firms:
  - name: TestFirm
    domain: testfirm.com
    feed_url: https://testfirm.com/feed
    hq: London
    sectors: [TMT]
  - name: AnotherFirm
    domain: another.com
""")
    firms = load_firms(targets)
    assert len(firms) == 2
    assert firms[0].name == "TestFirm"
    assert firms[0].feed_url == "https://testfirm.com/feed"
    assert firms[1].feed_url is None
