#!/usr/bin/env python3
"""
Delete recent HubSpot notes to test filters.

Usage:
  export HUBSPOT_API_KEY="your-key"
  python delete_recent_notes.py [--hours 24] [--dry-run]
"""
import os
import sys
import argparse
import requests
from datetime import datetime, timedelta

HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY")


def fetch_recent_notes(hours=24):
    """Fetch notes from the last N hours"""
    if not HUBSPOT_API_KEY:
        print("ERROR: HUBSPOT_API_KEY environment variable not set")
        print("Run: export HUBSPOT_API_KEY='your-key'")
        sys.exit(1)

    url = "https://api.hubapi.com/crm/v3/objects/notes"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    cutoff = datetime.utcnow() - timedelta(hours=hours)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    params = {
        "limit": 100,
        "properties": "hs_note_body,hs_createdate",
        "sorts": "-hs_createdate"
    }

    notes = []
    after = None

    while True:
        if after:
            params["after"] = after

        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code != 200:
            print(f"Error fetching notes: {response.status_code} - {response.text}")
            break

        data = response.json()
        for note in data.get("results", []):
            props = note.get("properties", {})
            created = props.get("hs_createdate")
            if created:
                created_ts = int(datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp() * 1000)
                if created_ts < cutoff_ms:
                    return notes

            body = props.get("hs_note_body", "")
            # Extract title from HTML body
            title = ""
            if "<strong>" in body:
                start = body.find("<strong>") + len("<strong>")
                end = body.find("</strong>", start)
                if end > start:
                    title = body[start:end]

            notes.append({
                "id": note.get("id"),
                "created": created,
                "title": title[:80] if title else body[:80]
            })

        paging = data.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after or len(notes) >= 500:
            break

    return notes


def delete_note(note_id):
    """Delete a single note"""
    url = f"https://api.hubapi.com/crm/v3/objects/notes/{note_id}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    }

    response = requests.delete(url, headers=headers, timeout=30)
    return response.status_code == 204


def main():
    parser = argparse.ArgumentParser(description="Delete recent HubSpot notes")
    parser.add_argument("--hours", type=int, default=24, help="Delete notes from last N hours (default: 24)")
    parser.add_argument("--dry-run", action="store_true", help="List notes without deleting")
    args = parser.parse_args()

    print(f"Fetching HubSpot notes from last {args.hours} hours...")
    notes = fetch_recent_notes(hours=args.hours)

    if not notes:
        print("No notes found in the specified time range.")
        return

    print(f"Found {len(notes)} notes.\n")

    # Show all notes
    print("Notes to be deleted:")
    for i, note in enumerate(notes, 1):
        print(f"  {i}. [{note['created'][:16]}] {note['title']}...")

    if args.dry_run:
        print(f"\n[DRY RUN] Would delete {len(notes)} notes.")
        return

    # Confirm deletion
    print(f"\nAbout to delete {len(notes)} notes. Continue? [y/N] ", end="")
    confirm = input().strip().lower()
    if confirm != 'y':
        print("Aborted.")
        return

    # Delete all notes
    print(f"\nDeleting {len(notes)} notes...")
    deleted = 0
    errors = 0

    for i, note in enumerate(notes, 1):
        if delete_note(note['id']):
            deleted += 1
            print(f"  [{i}/{len(notes)}] Deleted: {note['title'][:50]}...")
        else:
            errors += 1
            print(f"  [{i}/{len(notes)}] FAILED: {note['title'][:50]}...")

    print(f"\nDone! Deleted {deleted} notes, {errors} errors.")


if __name__ == "__main__":
    main()
