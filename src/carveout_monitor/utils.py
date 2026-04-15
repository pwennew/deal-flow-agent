"""Shared utilities for the carve-out monitor."""

from __future__ import annotations

import json
import re


def scale_workers(num_items: int, *, floor: int = 10, cap: int = 32,
                  items_per_worker: int = 10) -> int:
    """Scale ThreadPoolExecutor worker count to item count.

    Fetching RSS feeds / scraping press pages is I/O-bound, so we can safely
    run many more workers than CPU cores. With ~10 items per worker at a few
    seconds each, even 250+ firms complete within the 600s global timeout.

    Returns a value clamped to [floor, cap].
    """
    if num_items <= 0:
        return floor
    return min(cap, max(floor, (num_items + items_per_worker - 1) // items_per_worker))


def extract_json_array(text: str) -> list:
    """Extract a JSON array from LLM response text, handling markdown fences and preamble."""
    # Strip markdown code fences if present
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    # Find the outermost JSON array
    start = text.find("[")
    if start == -1:
        raise ValueError("No JSON array found in response")

    # Find matching closing bracket (handle nested arrays/objects)
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "[":
            depth += 1
        elif text[i] == "]":
            depth -= 1
            if depth == 0:
                return json.loads(text[start:i + 1])

    raise ValueError("Unbalanced JSON array in response")
