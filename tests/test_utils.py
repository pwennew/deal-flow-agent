"""Tests for shared utility functions."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.utils import extract_json_array


def test_clean_json_array():
    result = extract_json_array('[{"a": 1}, {"b": 2}]')
    assert result == [{"a": 1}, {"b": 2}]


def test_json_with_markdown_fences():
    text = '```json\n[{"deal": "test"}]\n```'
    result = extract_json_array(text)
    assert result == [{"deal": "test"}]


def test_json_with_preamble():
    text = 'Here are the results:\n\n[{"deal": "test"}]'
    result = extract_json_array(text)
    assert result == [{"deal": "test"}]


def test_json_with_trailing_text():
    text = '[{"deal": "test"}]\n\nLet me know if you need more.'
    result = extract_json_array(text)
    assert result == [{"deal": "test"}]


def test_nested_arrays_and_objects():
    text = '[{"tags": ["a", "b"], "nested": {"x": [1, 2]}}]'
    result = extract_json_array(text)
    assert result == [{"tags": ["a", "b"], "nested": {"x": [1, 2]}}]


def test_no_array_raises():
    with pytest.raises(ValueError, match="No JSON array found"):
        extract_json_array("Just some text with no array")


def test_unbalanced_brackets_raises():
    with pytest.raises(ValueError, match="Unbalanced JSON array"):
        extract_json_array("[{incomplete")


def test_markdown_fences_with_preamble():
    text = 'I analysed the articles:\n\n```json\n[{"x": 1}]\n```\n\nDone.'
    result = extract_json_array(text)
    assert result == [{"x": 1}]
