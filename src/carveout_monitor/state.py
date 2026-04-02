"""Cross-run state management for article deduplication."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


_STOP_WORDS = frozenset({
    # Articles / prepositions
    "the", "a", "an", "of", "in", "for", "and", "to", "from", "its", "s",
    # Corporate structure words
    "division", "unit", "business", "arm", "segment", "operations", "group",
    "industrial", "services", "company", "corp", "inc", "ltd", "plc", "ag",
    "se", "sa", "nv", "gmbh", "co", "llc", "lp", "holdings", "holding",
    # Generic business descriptors — too common to be meaningful in isolation
    "solutions", "products", "systems", "technologies", "technology", "tech",
    "global", "international", "worldwide", "americas", "europe", "asia",
    "capital", "partners", "advisors", "advisory", "management", "investments",
    "portfolio", "enterprises", "resources", "consulting", "associates",
    "financial", "ventures", "fund", "funds", "equity",
    # Industry verticals (too broad as single tokens)
    "energy", "health", "healthcare", "medical", "pharma", "pharmaceutical",
    "bio", "life", "sciences", "materials", "chemicals", "digital", "data",
    "analytics", "media", "communications", "network", "networks", "logistics",
    "supply", "manufacturing", "engineering", "automotive", "aerospace",
    "defense", "food", "beverage",
    # Common descriptors
    "north", "south", "east", "west", "new", "advanced", "specialty",
    "premium", "strategic", "integrated", "national",
})


def _norm(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^\w\s]", "", s)   # strip punctuation
    s = re.sub(r"\s+", " ", s)       # collapse whitespace
    return s


def _deal_key(target: str, seller: str, stage: str) -> str:
    """Normalised dedup key for a (target, seller, stage) triple.

    Stage is included so signing and closing of the same deal are treated as
    distinct entries — only multilingual/identical duplicates (same stage) are
    collapsed.
    """
    return f"{_norm(target)}|{_norm(seller)}|{stage.lower()}"


def _deal_tokens(s: str) -> set[str]:
    """Extract meaningful tokens from a company/target name for fuzzy matching."""
    return {t for t in _norm(s).split() if t not in _STOP_WORDS and len(t) > 1}


def _acronym(s: str) -> str:
    """Build acronym from a string: 'Global Elite Group' -> 'geg'.

    Does NOT strip stop words — acronyms include all initials
    (GEG = Global Elite Group, IBM = International Business Machines).
    """
    tokens = [t for t in _norm(s).split() if len(t) > 1]
    return "".join(t[0] for t in tokens) if len(tokens) >= 2 else ""


def _targets_match(target_a: str, target_b: str, seller_a: str = "", seller_b: str = "") -> bool:
    """Check if two target names likely refer to the same entity.

    This is the core matching logic, separated from seller checking so it
    can be reused when one seller is missing.
    """
    na, nb = _norm(target_a), _norm(target_b)

    # Exact match (normalised)
    if na == nb:
        return True

    seller_tokens = _deal_tokens(seller_a) | _deal_tokens(seller_b)
    target_tokens_a = _deal_tokens(target_a) - seller_tokens
    target_tokens_b = _deal_tokens(target_b) - seller_tokens

    # 1. Token overlap (after removing seller name tokens)
    # Require either 2+ shared tokens OR high Jaccard similarity (>= 0.5)
    # to avoid false matches on single generic words like "color" or "solutions"
    overlap = target_tokens_a & target_tokens_b
    if target_tokens_a and target_tokens_b and overlap:
        union = target_tokens_a | target_tokens_b
        jaccard = len(overlap) / len(union) if union else 0
        if len(overlap) >= 2 or jaccard >= 0.5:
            return True

    # 2. Substring containment — strip both seller names
    na_clean = na
    nb_clean = nb
    for s in (seller_a, seller_b):
        sn = _norm(s)
        if sn:
            na_clean = na_clean.replace(sn, "").strip()
            nb_clean = nb_clean.replace(sn, "").strip()
    if na_clean and nb_clean and (na_clean in nb_clean or nb_clean in na_clean):
        return True

    # 3. Seller prefix matching — both targets start with seller's first 4 chars
    # e.g. "ContiTech" and "Continental Industrial Unit" both start with "conti"
    for s in (seller_a, seller_b):
        sn = _norm(s)
        if len(sn) >= 5:
            prefix = sn[:4]
            if na.startswith(prefix) and nb.startswith(prefix):
                return True

    # 4. Generic target fallback — if one or both targets reduce to empty tokens
    # after stop word removal (e.g. "industrial division", "business unit"),
    # treat as same deal if sellers share tokens. This prevents generic
    # descriptions from creating duplicate deals for the same seller.
    # Both sellers must be present and share tokens — otherwise an empty seller
    # would match anything generic (e.g. "contact centre business" ≠ "industrial division").
    seller_tokens_a = _deal_tokens(seller_a)
    seller_tokens_b = _deal_tokens(seller_b)
    if (seller_tokens_a and seller_tokens_b
            and (seller_tokens_a & seller_tokens_b)
            and (not target_tokens_a or not target_tokens_b)):
        return True

    # 4. Acronym matching: "SWC" matches "Smart World Communication"
    acro_a = _acronym(target_a)
    acro_b = _acronym(target_b)
    if acro_a and acro_b and acro_a == acro_b:
        return True
    na_stripped = re.sub(r"[^\w]", "", na)
    nb_stripped = re.sub(r"[^\w]", "", nb)
    if acro_a and (nb_stripped.startswith(acro_a) or acro_a in _deal_tokens(target_b)):
        return True
    if acro_b and (na_stripped.startswith(acro_b) or acro_b in _deal_tokens(target_a)):
        return True

    return False


def deals_match(target_a: str, seller_a: str, target_b: str, seller_b: str) -> bool:
    """Check if two deals are likely the same based on fuzzy seller + target matching.

    Returns True if sellers overlap AND targets are likely the same entity.
    Also matches when one seller is empty/unknown if targets are strong matches.
    Handles brand names (ContiTech = Continental's tech division),
    acronyms (SWC = Smart World & Communication), and descriptive variants.
    """
    seller_tokens_a = _deal_tokens(seller_a)
    seller_tokens_b = _deal_tokens(seller_b)

    # If both sellers are present, they must share at least one token
    if seller_tokens_a and seller_tokens_b:
        if not seller_tokens_a & seller_tokens_b:
            return False
        return _targets_match(target_a, target_b, seller_a, seller_b)

    # If one or both sellers are empty/unknown, match on targets alone
    # (more permissive — avoids missing duplicates when Haiku extracts
    # different seller names or fails to extract one)
    return _targets_match(target_a, target_b, seller_a, seller_b)


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
        """True if this (target, seller, stage) deal has been written to Notion before.

        Uses fuzzy matching so 'ContiTech / Continental' matches
        'Continental Industrial Unit / Continental'.
        """
        if not target or not seller or seller.upper() in ("N/A", "UNKNOWN", ""):
            return False  # don't dedup on empty/unknown sellers
        # Exact match first (fast path)
        key = _deal_key(target, seller, stage)
        if key in self._data.get("seen_deals", {}):
            return True
        # Fuzzy match against all seen deals with same stage
        for seen_key in self._data.get("seen_deals", {}):
            parts = seen_key.split("|")
            if len(parts) != 3:
                continue
            seen_target, seen_seller, seen_stage = parts
            if seen_stage != stage.lower():
                continue
            if deals_match(target, seller, seen_target, seen_seller):
                return True
        return False

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
