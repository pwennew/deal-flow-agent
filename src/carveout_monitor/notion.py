"""Notion output — creates database rows for carve-out alerts."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from .models import DealAlert, QualifiedAlert

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.notion.com/v1"
_NOTION_VERSION = "2022-06-28"


def _create_page(api_key: str, database_id: str, alert: DealAlert) -> str | None:
    """Create a page (row) in the Notion database for a carve-out alert.

    Returns the page ID on success, or None on failure.
    """
    url = f"{_BASE_URL}/pages"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": _NOTION_VERSION,
    }

    a = alert.article
    # Guard against corrupt/missing titles
    title = (a.title or "").strip()
    if not title or len(title) <= 2:
        logger.warning("Skipping Notion write — corrupt/missing title: %r (target: %s)",
                       title, alert.target_company)
        return None
    stage_label = alert.stage.value.capitalize() if alert.stage else None

    # Use QualifiedAlert fields when available
    is_qualified = isinstance(alert, QualifiedAlert)
    pe_firm_value = alert.pe_firm if is_qualified and alert.pe_firm else ""
    score_value = alert.larkhill_fit if is_qualified else alert.confidence

    properties: dict = {
        "Alert": {
            "title": [{"text": {"content": title[:2000]}}],
        },
        "PE Firm": {
            "rich_text": [{"text": {"content": pe_firm_value[:2000]}}],
        },
        "Target": {
            "rich_text": [{"text": {"content": (alert.target_company or "N/A")[:2000]}}],
        },
        "Seller": {
            "rich_text": [{"text": {"content": (alert.seller or "N/A")[:2000]}}],
        },
        "%": {
            "number": score_value,
        },
        "URL": {
            "url": a.url or None,
        },
        "Reasoning": {
            "rich_text": [{"text": {"content": (alert.reasoning or "")[:2000]}}],
        },
    }

    # Add deal type if available
    deal_type_label = alert.deal_type.value if alert.deal_type else ""
    if deal_type_label:
        properties["Deal Type"] = {"select": {"name": deal_type_label}}

    # Add buyer if available
    buyer_value = alert.buyer if hasattr(alert, "buyer") and alert.buyer else ""
    if buyer_value:
        properties["Buyer"] = {
            "rich_text": [{"text": {"content": buyer_value[:2000]}}],
        }

    if stage_label:
        properties["Stage"] = {"select": {"name": stage_label}}

    if a.published:
        properties["Date"] = {"date": {"start": a.published.strftime("%Y-%m-%d")}}

    # Queue pursue deals for the Brief Generator
    if is_qualified:
        action = alert.recommended_action
        if action in ("pursue", "monitor"):
            properties["Action"] = {"select": {"name": action}}
        if action == "pursue":
            properties["Brief Status"] = {"select": {"name": "Queued"}}

    payload = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            page_id = resp.json().get("id")
            return page_id
        logger.warning("Notion page creation failed (status %d): %s",
                       resp.status_code, resp.text[:200])
        return None
    except requests.RequestException as e:
        logger.error("Notion API error: %s", e)
        return None


def _append_page_content(api_key: str, page_id: str, text: str) -> bool:
    """Append text content as paragraph blocks to a Notion page."""
    url = f"{_BASE_URL}/blocks/{page_id}/children"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": _NOTION_VERSION,
    }

    # Split text into paragraphs and create block children
    # Notion rich_text content limit is 2000 chars per block
    paragraphs = text.split("\n\n")
    children = []
    for para in paragraphs:
        if not para.strip():
            continue
        # Chunk long paragraphs into 2000-char blocks
        for i in range(0, len(para), 2000):
            chunk = para[i:i + 2000]
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}],
                },
            })

    if not children:
        return True

    # Notion allows max 100 blocks per request
    for i in range(0, len(children), 100):
        batch = children[i:i + 100]
        payload = {"children": batch}
        try:
            resp = requests.patch(url, headers=headers, json=payload, timeout=15)
            if resp.status_code != 200:
                logger.warning("Notion block append failed (status %d): %s",
                               resp.status_code, resp.text[:200])
                return False
        except requests.RequestException as e:
            logger.error("Notion block append error: %s", e)
            return False

    return True


class NotionClient:
    """Creates rows in a Notion database for carve-out alerts."""

    def __init__(self):
        self._api_key = os.environ.get("NOTION_API", "")
        self._database_id = os.environ.get("NOTION_DB_ID", "")

    @property
    def configured(self) -> bool:
        return bool(self._api_key and self._database_id)

    def write_alerts(self, alerts: list[DealAlert]) -> dict:
        """Write carve-out alerts as Notion database rows.

        Returns stats dict with 'written', 'skipped', 'errors' counts
        and 'page_ids' mapping alert index to Notion page ID.
        """
        stats: dict = {"written": 0, "skipped": 0, "errors": 0, "page_ids": {}}

        if not self._api_key or not self._database_id:
            logger.warning("NOTION_API_KEY or NOTION_DATABASE_ID not set — skipping Notion output")
            return stats

        for idx, alert in enumerate(alerts):
            # Skip discard alerts for QualifiedAlerts
            if isinstance(alert, QualifiedAlert) and alert.recommended_action == "discard":
                stats["skipped"] += 1
                continue

            page_id = _create_page(self._api_key, self._database_id, alert)
            time.sleep(0.35)

            if page_id:
                stats["written"] += 1
                stats["page_ids"][idx] = page_id
                logger.info("Created Notion row: %s — %s",
                            alert.article.firm_name, alert.article.title[:60])
            else:
                stats["errors"] += 1

        logger.info("Notion: %d rows created, %d skipped, %d errors",
                    stats["written"], stats["skipped"], stats["errors"])
        return stats
