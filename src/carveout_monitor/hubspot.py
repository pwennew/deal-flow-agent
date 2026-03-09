"""HubSpot output — creates Notes on Company records for carve-out alerts."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import requests

from .models import DealAlert

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.hubapi.com/crm/v3/objects"


def _search_company(api_key: str, name: str) -> str | None:
    """Search HubSpot for a Company by name. Returns company ID or None."""
    url = f"{_BASE_URL}/companies/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "name",
                "operator": "CONTAINS_TOKEN",
                "value": name,
            }]
        }],
        "properties": ["name"],
        "limit": 5,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code != 200:
            logger.warning("HubSpot company search failed (status %d): %s",
                           resp.status_code, resp.text[:200])
            return None

        results = resp.json().get("results", [])
        if not results:
            return None

        # Prefer exact name match
        name_lower = name.lower()
        for r in results:
            if r.get("properties", {}).get("name", "").lower() == name_lower:
                return r["id"]
        return results[0]["id"]

    except requests.RequestException as e:
        logger.warning("HubSpot company search error for '%s': %s", name, e)
        return None


def _create_company(api_key: str, name: str) -> str | None:
    """Create a new Company in HubSpot. Returns company ID or None."""
    url = f"{_BASE_URL}/companies"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"properties": {"name": name}}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 201:
            company_id = resp.json().get("id")
            logger.info("Created HubSpot company: %s (ID: %s)", name, company_id)
            return company_id
        logger.warning("HubSpot company creation failed (status %d): %s",
                       resp.status_code, resp.text[:200])
        return None
    except requests.RequestException as e:
        logger.error("HubSpot company creation error for '%s': %s", name, e)
        return None


def _create_note(api_key: str, company_id: str, alert: DealAlert) -> bool:
    """Create a Note on a HubSpot Company record. Returns True on success."""
    url = f"{_BASE_URL}/notes"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    a = alert.article
    stage_label = alert.stage.value.upper() if alert.stage else "UNKNOWN"

    note_body = (
        f"<strong>CARVE-OUT ALERT: {stage_label}</strong><br><br>"
        f"<strong>{a.title}</strong><br><br>"
        f"Target: {alert.target_company or 'N/A'}<br>"
        f"Seller: {alert.seller or 'N/A'}<br>"
        f"Stage: {stage_label}<br>"
        f"Confidence: {alert.confidence}/100<br>"
        f"Source: {a.firm_name}<br>"
        f"Date: {a.published.strftime('%Y-%m-%d') if a.published else 'N/A'}<br><br>"
        f"<a href=\"{a.url}\">Read Article</a>"
    )

    payload = {
        "properties": {
            "hs_timestamp": datetime.now().isoformat() + "Z",
            "hs_note_body": note_body,
        },
        "associations": [{
            "to": {"id": company_id},
            "types": [{
                "associationCategory": "HUBSPOT_DEFINED",
                "associationTypeId": 190,  # Note to Company
            }],
        }],
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 201:
            return True
        logger.warning("HubSpot note creation failed (status %d): %s",
                       resp.status_code, resp.text[:200])
        return False
    except requests.RequestException as e:
        logger.error("HubSpot API error: %s", e)
        return False


class HubSpotClient:
    """Creates Notes on HubSpot Company records for carve-out alerts."""

    def __init__(self):
        self._api_key = os.environ.get("HUBSPOT_API_KEY", "")
        self._company_cache: dict[str, str | None] = {}

    @property
    def configured(self) -> bool:
        return bool(self._api_key)

    def _resolve_company_id(self, name: str) -> str | None:
        """Resolve a firm name to a HubSpot Company ID (cache → search → create)."""
        if name in self._company_cache:
            return self._company_cache[name]

        company_id = _search_company(self._api_key, name)
        time.sleep(0.2)

        if company_id:
            logger.info("Found HubSpot company: %s (ID: %s)", name, company_id)
        else:
            company_id = _create_company(self._api_key, name)
            time.sleep(0.2)

        self._company_cache[name] = company_id
        return company_id

    def write_alerts(self, alerts: list[DealAlert]) -> dict:
        """Write carve-out alerts as HubSpot Notes. Returns stats dict."""
        stats = {"written": 0, "skipped": 0, "errors": 0}

        if not self._api_key:
            logger.warning("HUBSPOT_API_KEY not set — skipping HubSpot output")
            return stats

        for alert in alerts:
            # Create note on the PE firm's company record
            company_id = self._resolve_company_id(alert.article.firm_name)
            if not company_id:
                stats["skipped"] += 1
                logger.warning("Could not resolve HubSpot company for: %s",
                               alert.article.firm_name)
                continue

            success = _create_note(self._api_key, company_id, alert)
            time.sleep(0.1)

            if success:
                stats["written"] += 1
                logger.info("Created note on %s: %s",
                            alert.article.firm_name, alert.article.title[:60])
            else:
                stats["errors"] += 1

        logger.info("HubSpot: %d notes created, %d skipped, %d errors",
                    stats["written"], stats["skipped"], stats["errors"])
        return stats
