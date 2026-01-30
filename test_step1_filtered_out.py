"""
Test Step 1: Show what was FILTERED OUT (no target account mention)
"""

import feedparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from target_accounts import TARGET_PE_FIRMS, FIRM_ALIASES, match_pe_firm
import re

from rss_monitor import RSS_FEEDS, parse_published_date

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


def fetch_and_analyze():
    all_articles = []

    def fetch_feed(url):
        try:
            feed = feedparser.parse(url)
            feed_title = feed.feed.get("title", url[:50])
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

    print(f"Fetching from {len(RSS_FEEDS)} RSS feeds...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_feed, url) for url in RSS_FEEDS]
        for future in as_completed(futures):
            all_articles.extend(future.result())

    # Deduplicate by URL
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        url = article["link"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)

    # Separate into matched and filtered out
    matched = []
    filtered_out = []

    for article in unique_articles:
        text = f"{article['title']} {article['summary']}"
        matches = find_target_accounts_in_text(text)

        if matches:
            article["target_accounts_mentioned"] = matches
            matched.append(article)
        else:
            filtered_out.append(article)

    return matched, filtered_out


def main():
    print("=" * 70)
    print("STEP 1 ANALYSIS: What was FILTERED OUT?")
    print("=" * 70)
    print()

    matched, filtered_out = fetch_and_analyze()

    print(f"\nSUMMARY:")
    print(f"  Matched (kept): {len(matched)}")
    print(f"  Filtered out: {len(filtered_out)}")
    print()

    print("=" * 70)
    print("FILTERED OUT ARTICLES (no target account mention):")
    print("=" * 70)

    for i, article in enumerate(filtered_out, 1):
        print(f"\n{i}. {article['title'][:85]}")
        print(f"   Source: {article['source'][:60]}")

    print()
    print("=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
