"""Shared utilities for the carve-out monitor."""

from __future__ import annotations

import json
import re


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
