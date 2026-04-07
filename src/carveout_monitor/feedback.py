"""Read Verdict feedback from Notion and compute pipeline accuracy metrics."""

from __future__ import annotations

import logging
import os
from collections import Counter

import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.notion.com/v1"
_NOTION_VERSION = "2022-06-28"

# Verdict values that count as "false positive" (classifier or qualifier got it wrong)
FALSE_POSITIVE_VERDICTS = frozenset({
    "Not a Carve-Out",
    "Wrong Geography",
    "Low EV",
    "No Separation Work",
    "Duplicate",
    "Not Relevant",
})

# Verdict values that count as "true positive" (pipeline got it right)
TRUE_POSITIVE_VERDICTS = frozenset({
    "Confirmed",
})


def _query_database(api_key: str, database_id: str, start_cursor: str | None = None) -> dict:
    """Query the Notion database, paginating through all rows."""
    url = f"{_BASE_URL}/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": _NOTION_VERSION,
    }
    payload: dict = {"page_size": 100}
    if start_cursor:
        payload["start_cursor"] = start_cursor

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _extract_text(prop: dict) -> str:
    """Extract plain text from a Notion property value."""
    prop_type = prop.get("type", "")
    if prop_type == "title":
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    if prop_type == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
    if prop_type == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    if prop_type == "number":
        val = prop.get("number")
        return str(val) if val is not None else ""
    return ""


def fetch_verdicts() -> list[dict]:
    """Fetch all rows from the Carve-Out Alerts database with their verdict and metadata.

    Returns list of dicts with keys: title, verdict, action, deal_type, target,
    seller, buyer, pe_firm, score, url.
    """
    api_key = os.environ.get("NOTION_API", "")
    database_id = os.environ.get("NOTION_DB_ID", "")
    if not api_key or not database_id:
        logger.error("NOTION_API or NOTION_DB_ID not set")
        return []

    rows: list[dict] = []
    cursor = None

    while True:
        data = _query_database(api_key, database_id, start_cursor=cursor)
        for page in data.get("results", []):
            props = page.get("properties", {})
            rows.append({
                "page_id": page.get("id", ""),
                "title": _extract_text(props.get("Alert", {})),
                "verdict": _extract_text(props.get("Verdict", {})),
                "action": _extract_text(props.get("Action", {})),
                "deal_type": _extract_text(props.get("Deal Type", {})),
                "target": _extract_text(props.get("Target", {})),
                "seller": _extract_text(props.get("Seller", {})),
                "buyer": _extract_text(props.get("Buyer", {})),
                "pe_firm": _extract_text(props.get("PE Firm", {})),
                "score": _extract_text(props.get("%", {})),
                "url": (props.get("URL", {}).get("url") or ""),
            })

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    logger.info("Fetched %d rows from Notion", len(rows))
    return rows


def compute_accuracy(rows: list[dict]) -> dict:
    """Compute accuracy metrics from verdict-labelled rows.

    Returns dict with:
      - total: total rows
      - reviewed: rows with non-Pending verdict
      - pending: rows still pending review
      - confirmed: true positive count
      - false_positives: total false positives
      - precision: confirmed / (confirmed + false_positives)
      - verdict_breakdown: Counter of each verdict value
      - fp_by_deal_type: false positives broken down by deal_type
      - fp_by_reason: false positives broken down by verdict reason
      - fp_by_action: false positives broken down by action (pursue/monitor)
      - examples: list of false positive examples (title, verdict, deal_type, score)
    """
    total = len(rows)
    reviewed = [r for r in rows if r["verdict"] and r["verdict"] != "Pending"]
    pending = [r for r in rows if not r["verdict"] or r["verdict"] == "Pending"]

    confirmed = [r for r in reviewed if r["verdict"] in TRUE_POSITIVE_VERDICTS]
    false_positives = [r for r in reviewed if r["verdict"] in FALSE_POSITIVE_VERDICTS]

    reviewed_count = len(reviewed)
    confirmed_count = len(confirmed)
    fp_count = len(false_positives)

    precision = confirmed_count / (confirmed_count + fp_count) if (confirmed_count + fp_count) > 0 else 0.0

    verdict_breakdown = Counter(r["verdict"] for r in rows if r["verdict"])
    fp_by_deal_type = Counter(r["deal_type"] for r in false_positives if r["deal_type"])
    fp_by_reason = Counter(r["verdict"] for r in false_positives)
    fp_by_action = Counter(r["action"] for r in false_positives if r["action"])

    examples = [
        {
            "title": r["title"][:80],
            "verdict": r["verdict"],
            "deal_type": r["deal_type"],
            "score": r["score"],
            "action": r["action"],
            "target": r["target"],
            "seller": r["seller"],
        }
        for r in false_positives
    ]

    return {
        "total": total,
        "reviewed": reviewed_count,
        "pending": len(pending),
        "confirmed": confirmed_count,
        "false_positives": fp_count,
        "precision": precision,
        "verdict_breakdown": verdict_breakdown,
        "fp_by_deal_type": fp_by_deal_type,
        "fp_by_reason": fp_by_reason,
        "fp_by_action": fp_by_action,
        "examples": examples,
    }


def format_report(stats: dict) -> str:
    """Format accuracy stats into a human-readable report."""
    lines = [
        "=== Deal Flow Agent — Accuracy Report ===",
        "",
        f"Total alerts:     {stats['total']}",
        f"Reviewed:         {stats['reviewed']}",
        f"Pending review:   {stats['pending']}",
        "",
        f"Confirmed (TP):   {stats['confirmed']}",
        f"False positives:  {stats['false_positives']}",
        f"Precision:        {stats['precision']:.0%}",
    ]

    if stats["verdict_breakdown"]:
        lines += ["", "Verdict breakdown:"]
        for verdict, count in stats["verdict_breakdown"].most_common():
            lines.append(f"  {verdict}: {count}")

    if stats["fp_by_reason"]:
        lines += ["", "False positive reasons:"]
        for reason, count in stats["fp_by_reason"].most_common():
            pct = count / stats["false_positives"] * 100 if stats["false_positives"] else 0
            lines.append(f"  {reason}: {count} ({pct:.0f}%)")

    if stats["fp_by_deal_type"]:
        lines += ["", "False positives by deal type:"]
        for dtype, count in stats["fp_by_deal_type"].most_common():
            lines.append(f"  {dtype}: {count}")

    if stats["fp_by_action"]:
        lines += ["", "False positives by action:"]
        for action, count in stats["fp_by_action"].most_common():
            lines.append(f"  {action}: {count}")

    if stats["examples"]:
        lines += ["", "False positive examples:"]
        for ex in stats["examples"][:10]:
            lines.append(f"  [{ex['verdict']}] {ex['title']}")
            lines.append(f"    deal_type={ex['deal_type']}, score={ex['score']}, action={ex['action']}")
            lines.append(f"    target={ex['target']}, seller={ex['seller']}")

    return "\n".join(lines)


