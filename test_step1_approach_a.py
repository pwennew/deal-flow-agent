"""
Approach A: Target firm names embedded in RSS queries
Generate Google News searches for each target firm
"""

import feedparser
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from target_accounts import TARGET_PE_FIRMS, FIRM_ALIASES, match_pe_firm
import re
import urllib.parse

from rss_monitor import parse_published_date


def is_within_24h(date_str: str) -> bool:
    if not date_str:
        return True
    pub_date = parse_published_date(date_str)
    if not pub_date:
        return True
    now = datetime.now()
    age = now - pub_date
    return age.total_seconds() < 24 * 3600


def find_target_accounts_in_text(text: str) -> list[tuple[str, str, int]]:
    if not text:
        return []
    matches = []
    text_lower = text.lower()

    for alias, canonical in FIRM_ALIASES.items():
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower):
            matches.append((alias, canonical, 100))

    for firm in TARGET_PE_FIRMS:
        firm_lower = firm.lower()
        pattern = r'\b' + re.escape(firm_lower) + r'\b'
        if re.search(pattern, text_lower):
            if not any(m[1] == firm for m in matches):
                matches.append((firm, firm, 100))

    return matches


def generate_firm_feeds():
    """Generate Google News RSS feeds for groups of target firms"""
    feeds = []

    # Group firms into batches of 5 for OR queries (Google has query length limits)
    firms_list = list(TARGET_PE_FIRMS)
    batch_size = 5

    for i in range(0, len(firms_list), batch_size):
        batch = firms_list[i:i+batch_size]
        # Build OR query: "Firm1" OR "Firm2" OR ...
        query_parts = [f'"{firm}"' for firm in batch]
        query = " OR ".join(query_parts)
        encoded = urllib.parse.quote(query)

        # US feed
        feeds.append(f"https://news.google.com/rss/search?q={encoded}+when:2d&hl=en-US&gl=US&ceid=US:en")
        # UK/Europe feed
        feeds.append(f"https://news.google.com/rss/search?q={encoded}+when:2d&hl=en-GB&gl=GB&ceid=GB:en")

    return feeds


def fetch_and_filter():
    all_articles = []
    feeds = generate_firm_feeds()

    def fetch_feed(url):
        try:
            feed = feedparser.parse(url)
            feed_title = feed.feed.get("title", "Google News")
            articles = []
            for entry in feed.entries[:20]:
                published = entry.get("published", "")
                if not is_within_24h(published):
                    continue
                articles.append({
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "summary": entry.get("summary", entry.get("description", ""))[:500],
                    "published": published,
                    "source": feed_title,
                })
            return articles
        except:
            return []

    print(f"APPROACH A: Firm names in queries")
    print(f"Generated {len(feeds)} RSS feeds from {len(TARGET_PE_FIRMS)} target firms")
    print("Fetching...")

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(fetch_feed, url) for url in feeds]
        for future in as_completed(futures):
            all_articles.extend(future.result())

    print(f"Found {len(all_articles)} articles in last 24h")

    # Deduplicate by URL
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        url = article["link"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)

    print(f"After URL dedup: {len(unique_articles)} unique articles")

    # Post-filter for target account mentions (validation)
    filtered = []
    for article in unique_articles:
        text = f"{article['title']} {article['summary']}"
        matches = find_target_accounts_in_text(text)
        if matches:
            article["target_accounts_mentioned"] = matches
            filtered.append(article)

    return filtered, unique_articles


def main():
    print("=" * 70)
    print("APPROACH A: Target firm names in RSS queries")
    print("=" * 70)
    print()

    filtered, all_unique = fetch_and_filter()

    print()
    print(f"RESULTS:")
    print(f"  Total unique articles (24h): {len(all_unique)}")
    print(f"  Articles mentioning target accounts: {len(filtered)}")
    print(f"  Hit rate: {(len(filtered)/len(all_unique)*100):.1f}%" if all_unique else "N/A")
    print()

    # Export to CSV
    csv_file = "step1_approach_a_matched.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Source', 'Published', 'Target Accounts', 'Link'])
        for a in filtered:
            accounts = ', '.join(set(m[1] for m in a.get('target_accounts_mentioned', [])))
            writer.writerow([a['title'], a['source'], a['published'], accounts, a['link']])

    print(f"Exported to: {csv_file}")


if __name__ == "__main__":
    main()
