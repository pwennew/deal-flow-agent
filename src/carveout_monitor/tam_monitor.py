"""TAM account monitoring — flag articles that mention firms from targets.yml."""

from __future__ import annotations

import logging
import re

from .models import Article, Firm, TamAlert

logger = logging.getLogger(__name__)


def scan_for_tam_mentions(
    articles: list[Article],
    firms: list[Firm],
    firm_subset: list[str] | None = None,
) -> list[TamAlert]:
    """Scan article titles for mentions of TAM firm names.

    Matches full firm names using word-boundary regex (case-insensitive).
    Title-only matching keeps false-positive volume low — firm names in
    titles are almost always deal principals, not passing references.

    Args:
        articles: Articles to scan.
        firms: Full list of firms from targets.yml.
        firm_subset: If provided, only match against these firm names.

    Returns:
        List of TamAlert for articles with at least one firm match.
    """
    target_firms = firms
    if firm_subset:
        subset_lower = {n.lower() for n in firm_subset}
        target_firms = [f for f in firms if f.name.lower() in subset_lower]

    if not target_firms:
        return []

    # Pre-compile regex patterns for each firm name
    patterns: list[tuple[str, re.Pattern[str]]] = []
    for firm in target_firms:
        try:
            pattern = re.compile(r"\b" + re.escape(firm.name) + r"\b", re.IGNORECASE)
            patterns.append((firm.name, pattern))
        except re.error:
            logger.warning("Invalid regex for firm name: %s", firm.name)

    alerts: list[TamAlert] = []
    for article in articles:
        matched_firms: list[str] = []
        match_locations: list[str] = []

        for firm_name, pattern in patterns:
            if pattern.search(article.title):
                matched_firms.append(firm_name)
                match_locations.append("title")

        if matched_firms:
            alerts.append(TamAlert(
                article=article,
                matched_firms=matched_firms,
                match_locations=match_locations,
            ))

    return alerts
