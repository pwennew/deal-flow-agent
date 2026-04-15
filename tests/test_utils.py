"""Tests for shared utility functions."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.utils import extract_json_array, scale_workers


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


# --- scale_workers ---

def test_scale_workers_below_floor_returns_floor():
    # Empty / trivial workloads still get the floor worth of workers
    assert scale_workers(0) == 10
    assert scale_workers(5) == 10
    assert scale_workers(50) == 10


def test_scale_workers_grows_with_item_count():
    # 10 items per worker → 150 items should give 15 workers
    assert scale_workers(150) == 15
    assert scale_workers(250) == 25


def test_scale_workers_capped():
    assert scale_workers(500) == 32
    assert scale_workers(10_000) == 32


def test_scale_workers_respects_custom_bounds():
    # cap=20 takes precedence over the default cap
    assert scale_workers(1000, cap=20) == 20
    # floor=4 for very small workloads
    assert scale_workers(1, floor=4, cap=20) == 4
    # items_per_worker knob
    assert scale_workers(100, floor=1, cap=100, items_per_worker=5) == 20
