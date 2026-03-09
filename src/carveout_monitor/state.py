"""Cross-run state management for article deduplication."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class StateManager:
    """Tracks previously seen article URLs across pipeline runs.

    State is persisted as a JSON file that gets committed back to the repo
    by the GitHub Actions workflow after each run.
    """

    def __init__(self, path: str | Path = "state.json"):
        self._path = Path(path)
        self._data: dict = {"version": 1, "last_run": None, "seen": {}}
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

    def prune(self, max_age_days: int = 30) -> int:
        cutoff = datetime.now() - timedelta(days=max_age_days)
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
        if to_remove:
            logger.info("Pruned %d stale entries from state", len(to_remove))
        return len(to_remove)

    def save(self) -> None:
        self._data["last_run"] = datetime.now().isoformat()
        with open(self._path, "w") as f:
            json.dump(self._data, f, indent=2)
        logger.info("Saved state (%d seen URLs) to %s", len(self._data["seen"]), self._path)

    @property
    def seen_count(self) -> int:
        return len(self._data["seen"])
