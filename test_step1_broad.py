"""
Test Step 1 with BROAD RSS feeds - no keyword filtering
Only filter: target account mention
"""

import feedparser
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from target_accounts import TARGET_PE_FIRMS, FIRM_ALIASES, match_pe_firm
import re

from rss_monitor import parse_published_date

# BROAD RSS FEEDS - no keyword filtering, just general business/PE news sources
RSS_FEEDS_BROAD = [
    # ========== DIRECT PE/M&A NEWS SOURCES ==========
    "https://www.pehub.com/feed/",
    "https://www.prnewswire.com/rss/financial-services-latest-news/mergers-and-acquisitions-list.rss",
    "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWw==",

    # ========== GENERAL BUSINESS NEWS ==========
    "https://news.google.com/rss/search?q=private+equity+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=acquisition+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=merger+when:2d&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=buyout+when:2d&hl=en-US&gl=US&ceid=US:en",

    # ========== UK/EUROPE ==========
    "https://news.google.com/rss/search?q=private+equity+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
    "https://news.google.com/rss/search?q=acquisition+when:2d&hl=en-GB&gl=GB&ceid=GB:en",
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

    # 1. Check aliases first (exact matches)
    for alias, canonical in FIRM_ALIASES.items():
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower):
            matches.append((alias, canonical, 100))

    # 2. Check canonical names directly
    for firm in TARGET_PE_FIRMS:
        firm_lower = firm.lower()
        pattern = r'\b' + re.escape(firm_lower) + r'\b'
        if re.search(pattern, text_lower):
            if not any(m[1] == firm for m in matches):
                matches.append((firm, firm, 100))

    # 3. Fuzzy matching on potential firm name patterns
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
            for entry in feed.entries[:30]:  # Get more entries per feed
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

    print(f"Fetching from {len(RSS_FEEDS_BROAD)} BROAD RSS feeds (no keyword filtering)...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_feed, url) for url in RSS_FEEDS_BROAD]
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

    # ONLY FILTER: target account mention
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
    print("STEP 1 TEST (BROAD FEEDS): Target Account Mention Only")
    print("=" * 70)
    print()

    filtered, all_unique = fetch_and_filter()

    print()
    print(f"RESULTS:")
    print(f"  Total unique articles (24h): {len(all_unique)}")
    print(f"  Articles mentioning target accounts: {len(filtered)}")
    print(f"  Filter rate: {100 - (len(filtered)/len(all_unique)*100):.1f}% filtered out" if all_unique else "N/A")
    print()

    # Export to CSV
    csv_file = "step1_broad_matched.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Source', 'Published', 'Target Accounts', 'Link'])
        for a in filtered:
            accounts = ', '.join(set(m[1] for m in a.get('target_accounts_mentioned', [])))
            writer.writerow([a['title'], a['source'], a['published'], accounts, a['link']])

    print(f"Exported to: {csv_file}")
    print()

    print("=" * 70)
    print("MATCHED ARTICLES:")
    print("=" * 70)
    for i, article in enumerate(filtered, 1):
        accounts = list(set(m[1] for m in article.get("target_accounts_mentioned", [])))
        print(f"{i}. {article['title'][:70]}")
        print(f"   Accounts: {', '.join(accounts)}")
        print()


if __name__ == "__main__":
    main()
