"""Notion output — creates database rows for carve-out alerts."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from .models import DealAlert

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.notion.com/v1"
_NOTION_VERSION = "2022-06-28"


def _create_page(api_key: str, database_id: str, alert: DealAlert) -> bool:
    """Create a page (row) in the Notion database for a carve-out alert."""
    url = f"{_BASE_URL}/pages"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": _NOTION_VERSION,
    }

    a = alert.article
    stage_label = alert.stage.value.capitalize() if alert.stage else None

    properties: dict = {
        "Alert": {
            "title": [{"text": {"content": a.title[:2000]}}],
        },
        "PE Firm": {
            "rich_text": [{"text": {"content": a.firm_name[:2000]}}],
        },
        "Target": {
            "rich_text": [{"text": {"content": (alert.target_company or "N/A")[:2000]}}],
        },
        "Seller": {
            "rich_text": [{"text": {"content": (alert.seller or "N/A")[:2000]}}],
        },
        "Confidence": {
            "number": alert.confidence,
        },
        "URL": {
            "url": a.url or None,
        },
        "Reasoning": {
            "rich_text": [{"text": {"content": (alert.reasoning or "")[:2000]}}],
        },
    }

    if stage_label:
        properties["Stage"] = {"select": {"name": stage_label}}

    if a.published:
        properties["Date"] = {"date": {"start": a.published.strftime("%Y-%m-%d")}}

    payload = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            return True
        logger.warning("Notion page creation failed (status %d): %s",
                       resp.status_code, resp.text[:200])
        return False
    except requests.RequestException as e:
        logger.error("Notion API error: %s", e)
        return False


class NotionClient:
    """Creates rows in a Notion database for carve-out alerts."""

    def __init__(self):
        self._api_key = os.environ.get("NOTION_API", "")
        self._database_id = os.environ.get("NOTION_DB_ID", "")

    @property
    def configured(self) -> bool:
        return bool(self._api_key and self._database_id)

    def write_alerts(self, alerts: list[DealAlert]) -> dict:
        """Write carve-out alerts as Notion database rows. Returns stats dict."""
        stats = {"written": 0, "skipped": 0, "errors": 0}

        if not self._api_key or not self._database_id:
            logger.warning("NOTION_API_KEY or NOTION_DATABASE_ID not set — skipping Notion output")
            return stats

        for alert in alerts:
            success = _create_page(self._api_key, self._database_id, alert)
            time.sleep(0.35)

            if success:
                stats["written"] += 1
                logger.info("Created Notion row: %s — %s",
                            alert.article.firm_name, alert.article.title[:60])
            else:
                stats["errors"] += 1

        logger.info("Notion: %d rows created, %d skipped, %d errors",
                    stats["written"], stats["skipped"], stats["errors"])
        return stats
