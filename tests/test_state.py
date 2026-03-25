"""Tests for state management."""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from carveout_monitor.state import StateManager, _deal_key


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


# --- Deal deduplication tests ---

def test_deal_key_normalisation():
    # Punctuation stripped, case folded, stage appended
    assert _deal_key("Safety Products (Division)", "3M Co.", "signing") == \
        "safety products division|3m co|signing"
    # Parentheses stripped — EN and FR of same announcement produce identical key
    k1 = _deal_key("SPIE / SGS Industrial Services", "SPIE SA", "signing")
    k2 = _deal_key("SPIE  SGS Industrial Services", "SPIE SA", "signing")
    assert k1 == k2


def test_deal_key_stage_separates_signing_closing():
    # Signing and closing of the same deal must NOT share a key
    k_sign = _deal_key("Lantiq", "Infineon AG", "signing")
    k_close = _deal_key("Lantiq", "Infineon AG", "closing")
    assert k_sign != k_close


def test_is_deal_seen_not_seen(tmp_path):
    state = StateManager(tmp_path / "state.json")
    assert not state.is_deal_seen("Safety Products", "3M", "signing")


def test_mark_and_check_deal_seen(tmp_path):
    state = StateManager(tmp_path / "state.json")
    state.mark_deal_seen("Safety Products", "3M", "signing")
    assert state.is_deal_seen("Safety Products", "3M", "signing")
    # Different stage is NOT seen
    assert not state.is_deal_seen("Safety Products", "3M", "closing")


def test_deal_seen_empty_seller_not_deduped(tmp_path):
    state = StateManager(tmp_path / "state.json")
    state.mark_deal_seen("Some Division", "N/A", "signing")
    # N/A seller should never block a write
    assert not state.is_deal_seen("Some Division", "N/A", "signing")


def test_deal_seen_persists_across_loads(tmp_path):
    path = tmp_path / "state.json"
    s1 = StateManager(path)
    s1.mark_deal_seen("Red Lobster", "Darden", "signing")
    s1.save()

    s2 = StateManager(path)
    assert s2.is_deal_seen("Red Lobster", "Darden", "signing")
    assert not s2.is_deal_seen("Red Lobster", "Darden", "closing")


def test_deal_seen_pruned_after_30_days(tmp_path):
    path = tmp_path / "state.json"
    s = StateManager(path)
    s.mark_deal_seen("Old Deal", "Old Seller", "signing")
    # Backdate the entry
    key = _deal_key("Old Deal", "Old Seller", "signing")
    s._data["seen_deals"][key]["first_seen"] = (
        datetime.now() - timedelta(days=31)
    ).isoformat()
    s.prune()
    assert not s.is_deal_seen("Old Deal", "Old Seller", "signing")
