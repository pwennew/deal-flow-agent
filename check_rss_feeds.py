"""
Check which HubSpot companies have RSS feeds available.
Fetches company list + websites from HubSpot, tests common RSS patterns.
"""

import os
import requests
import feedparser
import time
from urllib.parse import urlparse

HUBSPOT_API_KEY = os.environ.get("HUBSPOT_API_KEY")

# Common RSS feed URL patterns to try
RSS_PATTERNS = [
    "/feed",
    "/feed/",
    "/rss",
    "/rss/",
    "/news/feed",
    "/news/feed/",
    "/press/feed",
    "/media/feed",
    "/blog/feed",
    "/feed.xml",
    "/rss.xml",
    "/atom.xml",
]


def fetch_hubspot_companies_with_websites():
    """Fetch companies from HubSpot with their website URLs"""
    if not HUBSPOT_API_KEY:
        print("ERROR: HUBSPOT_API_KEY not set")
        return []

    companies = []
    url = "https://api.hubapi.com/crm/v3/objects/companies"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }
    params = {
        "limit": 100,
        "properties": "name,website,domain"
    }

    after = None
    while True:
        if after:
            params["after"] = after

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                print(f"HubSpot API error: {response.status_code}")
                break

            data = response.json()
            for company in data.get("results", []):
                props = company.get("properties", {})
                name = props.get("name")
                website = props.get("website") or props.get("domain")
                if name:
                    companies.append({
                        "id": company.get("id"),
                        "name": name,
                        "website": website
                    })

            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after:
                break
            time.sleep(0.1)

        except Exception as e:
            print(f"Error: {e}")
            break

    return companies


def normalize_website(website):
    """Ensure website has https:// prefix"""
    if not website:
        return None
    website = website.strip()
    if not website.startswith(("http://", "https://")):
        website = "https://" + website
    # Remove trailing slash
    return website.rstrip("/")


def test_rss_feed(url):
    """Test if URL is a valid RSS feed. Returns (is_valid, entry_count, sample_title)"""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; DealFlowBot/1.0)"}
        response = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        
        if response.status_code != 200:
            return False, 0, None

        content_type = response.headers.get("content-type", "").lower()
        text = response.text[:1000]

        # Check if it looks like RSS/Atom
        if not any(x in content_type for x in ["xml", "rss", "atom"]):
            if not any(x in text for x in ["<?xml", "<rss", "<feed", "<channel"]):
                return False, 0, None

        feed = feedparser.parse(response.text)
        if feed.entries:
            sample = feed.entries[0].get("title", "")[:50]
            return True, len(feed.entries), sample

        return False, 0, None

    except Exception:
        return False, 0, None


def check_company_rss(company):
    """Check all RSS patterns for a company. Returns list of working feeds."""
    website = normalize_website(company.get("website"))
    if not website:
        return []

    working_feeds = []
    
    for pattern in RSS_PATTERNS:
        url = website + pattern
        is_valid, count, sample = test_rss_feed(url)
        
        if is_valid:
            working_feeds.append({
                "url": url,
                "entries": count,
                "sample": sample
            })
            break  # Found one, don't need to test more patterns
        
        time.sleep(0.2)  # Be polite

    return working_feeds


def main():
    print("Fetching companies from HubSpot...")
    companies = fetch_hubspot_companies_with_websites()
    print(f"Found {len(companies)} companies")
    print()

    results = {
        "with_rss": [],
        "no_website": [],
        "no_rss": []
    }

    for i, company in enumerate(companies):
        name = company["name"]
        website = company.get("website")
        
        print(f"[{i+1}/{len(companies)}] {name}...", end=" ", flush=True)

        if not website:
            print("no website")
            results["no_website"].append(name)
            continue

        feeds = check_company_rss(company)
        
        if feeds:
            feed = feeds[0]
            print(f"✓ RSS found: {feed['entries']} entries")
            results["with_rss"].append({
                "name": name,
                "website": website,
                "feed_url": feed["url"],
                "entries": feed["entries"],
                "sample": feed["sample"]
            })
        else:
            print("no RSS")
            results["no_rss"].append({"name": name, "website": website})

    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Companies with RSS feeds: {len(results['with_rss'])}")
    print(f"Companies without RSS:    {len(results['no_rss'])}")
    print(f"Companies without website: {len(results['no_website'])}")
    print()

    if results["with_rss"]:
        print("Companies with working RSS feeds:")
        for r in results["with_rss"]:
            print(f"  {r['name']}")
            print(f"    {r['feed_url']}")
            print(f"    {r['entries']} entries, sample: {r['sample']}")
            print()

    # Output to CSV
    import csv
    with open("hubspot_rss_feeds.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Company", "Website", "RSS Feed URL", "Entries", "Sample Title"])
        for r in results["with_rss"]:
            writer.writerow([r["name"], r["website"], r["feed_url"], r["entries"], r["sample"]])
    
    print(f"Results saved to: hubspot_rss_feeds.csv")


if __name__ == "__main__":
    main()
