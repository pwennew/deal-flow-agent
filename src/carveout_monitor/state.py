"""Cross-run state management for article deduplication."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def _deal_key(target: str, seller: str, stage: str) -> str:
    """Normalised dedup key for a (target, seller, stage) triple.

    Stage is included so signing and closing of the same deal are treated as
    distinct entries — only multilingual/identical duplicates (same stage) are
    collapsed.
    """
    def norm(s: str) -> str:
        s = s.lower().strip()
        s = re.sub(r"[^\w\s]", "", s)   # strip punctuation
        s = re.sub(r"\s+", " ", s)       # collapse whitespace
        return s
    return f"{norm(target)}|{norm(seller)}|{stage.lower()}"


class StateManager:
    """Tracks previously seen article URLs across pipeline runs.

    State is persisted as a JSON file that gets committed back to the repo
    by the GitHub Actions workflow after each run.
    """

    def __init__(self, path: str | Path = "state.json"):
        self._path = Path(path)
        self._data: dict = {"version": 1, "last_run": None, "seen": {}, "seen_deals": {}}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            logger.info("No state file found at %s, starting fresh", self._path)
            return

        try:
            with open(self._path) as f:
                raw = json.load(f)
            if not isinstance(raw, dict) or "seen" not in raw:
                logger.warning("State file has unexpected format, starting fresh")
                return
            self._data = raw
            logger.info("Loaded state with %d seen URLs", len(self._data["seen"]))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load state file: %s — starting fresh", e)

    def is_seen(self, url: str) -> bool:
        return url in self._data["seen"]

    def mark_seen(self, url: str) -> None:
        self._data["seen"][url] = {
            "hash": hashlib.sha256(url.encode()).hexdigest()[:16],
            "first_seen": datetime.now().isoformat(),
        }

    def is_deal_seen(self, target: str, seller: str, stage: str) -> bool:
        """True if this (target, seller, stage) deal has been written to Notion before."""
        if not target or not seller or seller.upper() in ("N/A", "UNKNOWN", ""):
            return False  # don't dedup on empty/unknown sellers
        key = _deal_key(target, seller, stage)
        return key in self._data.get("seen_deals", {})

    def mark_deal_seen(self, target: str, seller: str, stage: str) -> None:
        """Record that this (target, seller, stage) deal has been written to Notion."""
        key = _deal_key(target, seller, stage)
        self._data.setdefault("seen_deals", {})[key] = {
            "first_seen": datetime.now().isoformat(),
        }

    def prune(self, max_age_days: int = 30) -> int:
        cutoff = datetime.now() - timedelta(days=max_age_days)

        # Prune seen URLs
        to_remove = []
        for url, entry in self._data["seen"].items():
            try:
                first_seen = datetime.fromisoformat(entry["first_seen"])
                if first_seen < cutoff:
                    to_remove.append(url)
            except (KeyError, ValueError):
                to_remove.append(url)
        for url in to_remove:
            del self._data["seen"][url]

        # Prune seen deals
        deals_to_remove = []
        for key, entry in self._data.get("seen_deals", {}).items():
            try:
                if datetime.fromisoformat(entry["first_seen"]) < cutoff:
                    deals_to_remove.append(key)
            except (KeyError, ValueError):
                deals_to_remove.append(key)
        for key in deals_to_remove:
            del self._data["seen_deals"][key]

        pruned = len(to_remove) + len(deals_to_remove)
        if pruned:
            logger.info("Pruned %d stale URL(s) and %d stale deal(s) from state",
                        len(to_remove), len(deals_to_remove))
        return pruned

    def save(self) -> None:
        self._data["last_run"] = datetime.now().isoformat()
        with open(self._path, "w") as f:
            json.dump(self._data, f, indent=2)
        logger.info("Saved state (%d seen URLs, %d seen deals) to %s",
                    len(self._data["seen"]), len(self._data.get("seen_deals", {})), self._path)

    @property
    def seen_count(self) -> int:
        return len(self._data["seen"])
