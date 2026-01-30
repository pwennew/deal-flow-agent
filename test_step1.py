"""
Test Step 1: RSS Monitor filtering to Target Account mentions only
"""

import feedparser
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from target_accounts import TARGET_PE_FIRMS, FIRM_ALIASES, match_pe_firm, normalize_firm_name
import re

# Use the existing RSS feeds
from rss_monitor import RSS_FEEDS, parse_published_date

def is_within_24h(date_str: str) -> bool:
    """Check if published date is within last 24 hours"""
    if not date_str:
        return True

    pub_date = parse_published_date(date_str)
    if not pub_date:
        return True

    now = datetime.now()
    age = now - pub_date
    return age.total_seconds() < 24 * 3600


def find_target_accounts_in_text(text: str) -> list[tuple[str, str, int]]:
    """
    Find all target account mentions in text.

    Returns list of (matched_text, canonical_name, confidence)
    """
    if not text:
        return []

    matches = []
    text_lower = text.lower()

    # 1. Check aliases first (exact matches)
    for alias, canonical in FIRM_ALIASES.items():
        # Use word boundary matching to avoid partial matches
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower):
            matches.append((alias, canonical, 100))

    # 2. Check canonical names directly
    for firm in TARGET_PE_FIRMS:
        firm_lower = firm.lower()
        pattern = r'\b' + re.escape(firm_lower) + r'\b'
        if re.search(pattern, text_lower):
            # Avoid duplicates from aliases
            if not any(m[1] == firm for m in matches):
                matches.append((firm, firm, 100))

    # 3. Try fuzzy matching on potential firm name patterns
    # Look for patterns like "X Capital", "X Partners", "X Group", etc.
    potential_names = re.findall(
        r'\b([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)\s+(?:Capital|Partners|Group|Equity|Management|Advisers|Investments?)\b',
        text
    )

    for potential in potential_names:
        full_name = potential + " Capital"  # Try common suffixes
        is_match, canonical, confidence = match_pe_firm(potential)
        if is_match and confidence >= 85:
            if not any(m[1] == canonical for m in matches):
                matches.append((potential, canonical, confidence))

    return matches


def fetch_and_filter_articles():
    """Fetch RSS articles and filter to those mentioning target accounts"""

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
        except Exception as e:
            return []

    # Fetch all feeds in parallel
    print(f"Fetching from {len(RSS_FEEDS)} RSS feeds...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_feed, url) for url in RSS_FEEDS]
        for future in as_completed(futures):
            all_articles.extend(future.result())

    print(f"Found {len(all_articles)} articles in last 24h (before filtering)")

    # Deduplicate by URL
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        url = article["link"]
        if url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)

    print(f"After URL dedup: {len(unique_articles)} unique articles")

    # STEP 1: Filter to articles mentioning target accounts
    filtered_articles = []

    for article in unique_articles:
        text = f"{article['title']} {article['summary']}"
        matches = find_target_accounts_in_text(text)

        if matches:
            article["target_accounts_mentioned"] = matches
            filtered_articles.append(article)

    return filtered_articles, unique_articles


def main():
    print("=" * 70)
    print("STEP 1 TEST: RSS Monitor - Target Account Mention Filter")
    print("=" * 70)
    print()

    filtered, all_unique = fetch_and_filter_articles()

    print()
    print(f"RESULTS:")
    print(f"  Total unique articles (24h): {len(all_unique)}")
    print(f"  Articles mentioning target accounts: {len(filtered)}")
    print(f"  Filter rate: {100 - (len(filtered)/len(all_unique)*100):.1f}% filtered out" if all_unique else "N/A")
    print()

    print("=" * 70)
    print("FILTERED ARTICLES (mentioning target PE accounts):")
    print("=" * 70)

    for i, article in enumerate(filtered, 1):
        print(f"\n{i}. {article['title'][:80]}")
        print(f"   Source: {article['source']}")
        print(f"   Published: {article['published']}")
        accounts = article.get("target_accounts_mentioned", [])
        account_names = list(set(m[1] for m in accounts))
        print(f"   Target Accounts: {', '.join(account_names)}")
        print(f"   Link: {article['link'][:80]}...")

    print()
    print("=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
