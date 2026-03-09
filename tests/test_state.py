"""Tests for state management."""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.state import StateManager


def test_fresh_state(tmp_path):
    state = StateManager(tmp_path / "state.json")
    assert state.seen_count == 0
    assert not state.is_seen("https://example.com")


def test_mark_and_check(tmp_path):
    state = StateManager(tmp_path / "state.json")
    state.mark_seen("https://example.com/article1")
    assert state.is_seen("https://example.com/article1")
    assert not state.is_seen("https://example.com/article2")


def test_save_and_reload(tmp_path):
    path = tmp_path / "state.json"

    state1 = StateManager(path)
    state1.mark_seen("https://example.com/a")
    state1.mark_seen("https://example.com/b")
    state1.save()

    state2 = StateManager(path)
    assert state2.seen_count == 2
    assert state2.is_seen("https://example.com/a")
    assert state2.is_seen("https://example.com/b")


def test_prune(tmp_path):
    path = tmp_path / "state.json"

    # Write state with old entries
    old_date = (datetime.now() - timedelta(days=60)).isoformat()
    data = {
        "version": 1,
        "last_run": None,
        "seen": {
            "https://old.com": {"hash": "abc", "first_seen": old_date},
            "https://new.com": {"hash": "def", "first_seen": datetime.now().isoformat()},
        },
    }
    with open(path, "w") as f:
        json.dump(data, f)

    state = StateManager(path)
    pruned = state.prune(max_age_days=30)
    assert pruned == 1
    assert not state.is_seen("https://old.com")
    assert state.is_seen("https://new.com")


def test_corrupt_state_file(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("not valid json")
    state = StateManager(path)
    assert state.seen_count == 0
