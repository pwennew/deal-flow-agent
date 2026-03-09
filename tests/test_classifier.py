"""Tests for classifier module (mocked LLM calls)."""

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.models import Article, DealStage
from carveout_monitor.classifier import classify_batch, classify_articles


def _mock_response(results: list[dict]):
    """Create a mock Anthropic API response."""
    mock = MagicMock()
    mock.content = [MagicMock(text=json.dumps(results))]
    mock.usage = MagicMock(input_tokens=100, output_tokens=50)
    return mock


def test_classify_batch_carveout():
    articles = [
        Article(title="Blackstone agrees to acquire Safety Products from 3M",
                url="https://example.com/1", firm_name="Blackstone"),
    ]

    mock_results = [{
        "is_carveout": True,
        "stage": "signing",
        "target_company": "Safety Products",
        "seller": "3M",
        "confidence": 95,
        "reasoning": "Classic carve-out signing",
    }]

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        with patch("carveout_monitor.classifier.anthropic") as mock_anthropic:
            mock_client = MagicMock()
            mock_anthropic.Anthropic.return_value = mock_client
            mock_client.messages.create.return_value = _mock_response(mock_results)

            alerts = classify_batch(articles)

    assert len(alerts) == 1
    assert alerts[0].is_carveout is True
    assert alerts[0].stage == DealStage.SIGNING
    assert alerts[0].target_company == "Safety Products"
    assert alerts[0].seller == "3M"
    assert alerts[0].confidence == 95


def test_classify_batch_not_carveout():
    articles = [
        Article(title="Apollo raises $30B fund",
                url="https://example.com/2", firm_name="Apollo"),
    ]

    mock_results = [{
        "is_carveout": False,
        "stage": None,
        "target_company": "",
        "seller": "",
        "confidence": 98,
        "reasoning": "Fund news, not a deal",
    }]

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        with patch("carveout_monitor.classifier.anthropic") as mock_anthropic:
            mock_client = MagicMock()
            mock_anthropic.Anthropic.return_value = mock_client
            mock_client.messages.create.return_value = _mock_response(mock_results)

            alerts = classify_batch(articles)

    assert len(alerts) == 1
    assert alerts[0].is_carveout is False


def test_classify_articles_empty():
    alerts = classify_articles([])
    assert alerts == []


def test_classify_no_api_key():
    articles = [Article(title="Test", url="https://example.com")]

    with patch.dict("os.environ", {}, clear=True):
        alerts = classify_batch(articles)

    assert len(alerts) == 1
    assert alerts[0].is_carveout is False
