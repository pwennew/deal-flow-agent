"""
Approach B: PE-focused sources + post-filter
Better quality sources that naturally cover PE/M&A, filter after fetching
"""

import feedparser
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from target_accounts import TARGET_PE_FIRMS, FIRM_ALIASES, match_pe_firm
import re

from rss_monitor import parse_published_date

# PE-focused sources (no firm names in queries)
RSS_FEEDS_PE_FOCUSED = [
    # ========== DIRECT PE/M&A NEWS ==========
    "https://www.pehub.com/feed/",
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",

    # ========== PREMIUM JOURNALISM (via Google News) ==========
    # FT - private equity coverage
    "https://news.google.com/rss/search?q=site:ft.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    # WSJ - deals coverage
    "https://news.google.com/rss/search?q=site:wsj.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    # Bloomberg - PE coverage
    "https://news.google.com/rss/search?q=site:bloomberg.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    # Reuters - PE coverage
    "https://news.google.com/rss/search?q=site:reuters.com+private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",

    # ========== DEAL-FOCUSED QUERIES ==========
    "https://news.google.com/rss/search?q=private+equity+acquisition+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=private+equity+buyout+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=private+equity+investment+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=leveraged+buyout+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=PE+firm+acquisition+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=buyout+firm+when:2d&hl=en-US&gl=US&ceid=US:en",

    # ========== UK/EUROPE ==========
    "https://news.google.com/rss/search?q=private+equity+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=site:ft.com+private+equity+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=buyout+acquisition+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
]


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

    potential_names = re.findall(
        r'\b([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)\s+(?:Capital|Partners|Group|Equity|Management|Advisers|Investments?)\b',
        text
    )
    for potential in potential_names:
        is_match, canonical, confidence = match_pe_firm(potential)
        if is_match and confidence >= 85:
            if not any(m[1] == canonical for m in matches):
                matches.append((potential, canonical, confidence))

    return matches


def fetch_and_filter():
    all_articles = []

    def fetch_feed(url):
        try:
            feed = feedparser.parse(url)
            feed_title = feed.feed.get("title", url[:50])
            articles = []
            for entry in feed.entries[:30]:
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

    print(f"APPROACH B: PE-focused sources + post-filter")
    print(f"Using {len(RSS_FEEDS_PE_FOCUSED)} PE-focused RSS feeds (no firm names in queries)")
    print("Fetching...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_feed, url) for url in RSS_FEEDS_PE_FOCUSED]
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

    # Post-filter for target account mentions
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
    print("APPROACH B: PE-focused sources + post-filter")
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
    csv_file = "step1_approach_b_matched.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Source', 'Published', 'Target Accounts', 'Link'])
        for a in filtered:
            accounts = ', '.join(set(m[1] for m in a.get('target_accounts_mentioned', [])))
            writer.writerow([a['title'], a['source'], a['published'], accounts, a['link']])

    print(f"Exported to: {csv_file}")


if __name__ == "__main__":
    main()
