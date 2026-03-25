"""CLI entry point: python -m carveout_monitor [scan|discover|backtest]"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import yaml
from pathlib import Path

from .models import load_firms, DealAlert, DealStage
from .feeds import fetch_articles, fetch_all_articles, fetch_core_feeds, discover_feeds
from .scraper import scrape_articles, discover_press_page
from .classifier import classify_articles
from .notion import NotionClient
from .state import StateManager, _deal_key

logger = logging.getLogger("carveout_monitor")


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_scan(args):
    """Daily scan: fetch → dedup → classify → Notion."""
    start = time.time()
    firms = load_firms(args.targets)
    logger.info("Loaded %d target firms", len(firms))

    # Step 1a: Fetch articles from firm-specific RSS feeds
    t0 = time.time()
    articles = fetch_articles(firms, lookback_hours=args.hours)
    logger.info("[%.1fs] RSS fetch: %d articles", time.time() - t0, len(articles))

    # Step 1b: Fetch articles from core feeds (Google News, press wires)
    t0 = time.time()
    core_articles = fetch_core_feeds(lookback_hours=args.hours)
    articles.extend(core_articles)
    logger.info("[%.1fs] Core feeds: %d articles", time.time() - t0, len(core_articles))

    # Step 2: Scrape press pages for firms without RSS
    t0 = time.time()
    # Snapshot which firms had no press_url before scraping (scrape_firm auto-discovers)
    firms_before = {f.name: f.press_url for f in firms}
    scraped = scrape_articles(firms, lookback_hours=args.hours)
    articles.extend(scraped)
    logger.info("[%.1fs] Scrape: %d additional articles", time.time() - t0, len(scraped))

    # Persist any newly-discovered press URLs to targets.yml
    new_press = {f.name: f.press_url for f in firms
                 if f.press_url and not firms_before.get(f.name)}
    if new_press:
        logger.info("Persisting %d newly-discovered press URLs to %s", len(new_press), args.targets)
        _update_targets(args.targets, {}, new_press)

    if not articles:
        logger.info("No articles found — nothing to do")
        return

    # Step 3: URL dedup
    seen_urls: set[str] = set()
    unique = []
    for a in articles:
        if a.url not in seen_urls:
            seen_urls.add(a.url)
            unique.append(a)
    logger.info("After URL dedup: %d articles (removed %d duplicates)",
                len(unique), len(articles) - len(unique))
    articles = unique

    # Step 4: Cross-run dedup via state
    state = StateManager(args.state)
    state.prune()
    new_articles = [a for a in articles if not state.is_seen(a.url)]
    logger.info("After state dedup: %d new articles (skipped %d seen)",
                len(new_articles), len(articles) - len(new_articles))

    if not new_articles:
        logger.info("No new articles — nothing to classify")
        state.save()
        return

    # Step 5: Classify with Haiku
    t0 = time.time()
    all_alerts = classify_articles(new_articles)
    logger.info("[%.1fs] Classification complete", time.time() - t0)

    # Step 6: Filter to carve-outs
    carveouts = [a for a in all_alerts if a.is_carveout and a.confidence >= 70]
    logger.info("Carve-outs found: %d (confidence >= 70)", len(carveouts))

    for alert in carveouts:
        stage = alert.stage.value if alert.stage else "unknown"
        logger.info("  [%s] %s — target: %s, seller: %s (confidence: %d)",
                     stage, alert.article.title[:80],
                     alert.target_company, alert.seller, alert.confidence)

    # Step 6b: Within-run deal dedup — same (target, seller, stage) from multiple articles
    # Signing and closing of the same deal have different stages so both pass through.
    # Multilingual duplicates (EN + FR same announcement, same stage) are collapsed.
    seen_this_run: dict[str, DealAlert] = {}
    for alert in carveouts:
        stage_val = alert.stage.value if alert.stage else "unknown"
        key = _deal_key(alert.target_company, alert.seller, stage_val)
        if key in seen_this_run:
            existing = seen_this_run[key]
            if alert.confidence > existing.confidence:
                logger.info("  Within-run dedup: replacing lower-confidence duplicate for %s / %s [%s]",
                            alert.target_company, alert.seller, stage_val)
                seen_this_run[key] = alert
            else:
                logger.info("  Within-run dedup: dropping duplicate for %s / %s [%s]",
                            alert.target_company, alert.seller, stage_val)
        else:
            seen_this_run[key] = alert
    deduped = list(seen_this_run.values())
    if len(deduped) < len(carveouts):
        logger.info("After within-run dedup: %d carve-outs remain (dropped %d)",
                    len(deduped), len(carveouts) - len(deduped))
    carveouts = deduped

    # Step 6c: Cross-run deal dedup — skip deals already written to Notion
    new_carveouts = [a for a in carveouts
                     if not state.is_deal_seen(
                         a.target_company, a.seller,
                         a.stage.value if a.stage else "unknown"
                     )]
    skipped = len(carveouts) - len(new_carveouts)
    if skipped:
        logger.info("Cross-run dedup: skipped %d deal(s) already in Notion", skipped)
    carveouts = new_carveouts

    # Step 7: Write to Notion
    if not args.skip_notion and carveouts:
        t0 = time.time()
        client = NotionClient()
        if client.configured:
            stats = client.write_alerts(carveouts)
            logger.info("[%.1fs] Notion: %s", time.time() - t0, stats)
            # Mark written deals as seen
            for alert in carveouts:
                state.mark_deal_seen(
                    alert.target_company, alert.seller,
                    alert.stage.value if alert.stage else "unknown"
                )
        else:
            logger.warning("Notion not configured — skipping")
    elif args.skip_notion:
        logger.info("Notion output skipped (--skip-notion flag)")

    # Step 8: Mark all processed articles as seen
    for a in new_articles:
        state.mark_seen(a.url)
    state.save()

    elapsed = time.time() - start
    logger.info("Pipeline complete in %.1fs: %d articles processed, %d carve-outs found",
                elapsed, len(new_articles), len(carveouts))


def cmd_discover(args):
    """Discover RSS feeds and press pages for all firms."""
    firms = load_firms(args.targets)
    logger.info("Loaded %d target firms", len(firms))

    # Discover RSS feeds
    logger.info("=== Discovering RSS feeds ===")
    new_feeds = discover_feeds(firms)

    # Discover press pages for firms without feeds (existing or newly-discovered)
    logger.info("=== Discovering press pages ===")
    press_pages: dict[str, str | None] = {}
    firms_needing_press = [
        f for f in firms
        if not f.feed_url and not new_feeds.get(f.name) and not f.press_url
    ]
    logger.info("Probing press pages for %d firms", len(firms_needing_press))
    for firm in firms_needing_press:
        url = discover_press_page(firm)
        if url:
            press_pages[firm.name] = url

    # Summary
    existing_feeds = sum(1 for f in firms if f.feed_url)
    discovered_feeds = sum(1 for v in new_feeds.values() if v)
    discovered_press = len(press_pages)

    logger.info("=== Discovery Summary ===")
    logger.info("  Existing RSS feeds: %d", existing_feeds)
    logger.info("  Newly discovered feeds: %d", discovered_feeds)
    logger.info("  Press pages found: %d", discovered_press)
    logger.info("  No source found: %d",
                len(firms) - existing_feeds - discovered_feeds - discovered_press)

    if discovered_feeds or discovered_press:
        logger.info("\nDiscovered feeds:")
        for name, url in sorted(new_feeds.items()):
            if url:
                logger.info("  %s: %s", name, url)
        if press_pages:
            logger.info("\nDiscovered press pages:")
            for name, url in sorted(press_pages.items()):
                logger.info("  %s: %s", name, url)

        # Offer to update targets.yml
        if args.update:
            _update_targets(args.targets, new_feeds, press_pages)
            logger.info("Updated %s with discovered URLs", args.targets)


def _update_targets(path: str, feeds: dict[str, str | None], press_pages: dict[str, str | None]):
    """Update targets.yml with discovered feed URLs and press pages."""
    with open(path) as f:
        data = yaml.safe_load(f)

    for firm in data.get("firms", []):
        name = firm["name"]
        if name in feeds and feeds[name] and not firm.get("feed_url"):
            firm["feed_url"] = feeds[name]
        if name in press_pages and press_pages[name] and not firm.get("press_url"):
            firm["press_url"] = press_pages[name]

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def cmd_backtest(args):
    """Fetch ALL articles from feeds + press pages, classify, report (no HubSpot)."""
    firms = load_firms(args.targets)
    logger.info("Loaded %d target firms", len(firms))

    # Fetch all articles from RSS feeds (no date filter)
    t0 = time.time()
    articles = fetch_all_articles(firms)
    logger.info("[%.1fs] Fetched %d articles from RSS feeds (no date filter)",
                time.time() - t0, len(articles))

    # URL dedup
    seen: set[str] = set()
    unique = []
    for a in articles:
        if a.url not in seen:
            seen.add(a.url)
            unique.append(a)
    articles = unique
    logger.info("After dedup: %d unique articles", len(articles))

    if not articles:
        logger.info("No articles to classify")
        return

    # Classify
    t0 = time.time()
    all_alerts = classify_articles(articles)
    logger.info("[%.1fs] Classification complete", time.time() - t0)

    # Report
    carveouts = [a for a in all_alerts if a.is_carveout]
    logger.info("\n=== BACKTEST RESULTS ===")
    logger.info("Total articles: %d", len(articles))
    logger.info("Carve-outs found: %d", len(carveouts))

    if carveouts:
        # Group by firm
        by_firm: dict[str, list[DealAlert]] = {}
        for alert in carveouts:
            firm = alert.article.firm_name
            by_firm.setdefault(firm, []).append(alert)

        for firm_name, alerts in sorted(by_firm.items()):
            logger.info("\n  %s (%d alerts):", firm_name, len(alerts))
            for alert in alerts:
                stage = alert.stage.value if alert.stage else "?"
                date = alert.article.published.strftime("%Y-%m-%d") if alert.article.published else "no date"
                logger.info("    [%s] %s — %s (confidence: %d)",
                            stage, date, alert.article.title[:70], alert.confidence)
    else:
        logger.info("No carve-outs found in backtest data")


def main():
    parser = argparse.ArgumentParser(
        prog="carveout_monitor",
        description="Monitor PE firm websites for carve-out deal announcements",
    )
    parser.add_argument("--targets", default="targets.yml", help="Path to targets YAML file")
    parser.add_argument("--state", default="state.json", help="Path to state file")

    sub = parser.add_subparsers(dest="command")

    # scan
    scan_p = sub.add_parser("scan", help="Daily scan for new carve-out announcements")
    scan_p.add_argument("--hours", type=int, default=24, help="Lookback hours (default: 24)")
    scan_p.add_argument("--skip-notion", action="store_true", help="Skip Notion output")
    scan_p.add_argument("--skip-hubspot", action="store_true", dest="skip_notion",
                        help=argparse.SUPPRESS)  # backwards compat alias

    # discover
    disc_p = sub.add_parser("discover", help="Discover RSS feeds and press pages for all firms")
    disc_p.add_argument("--update", action="store_true",
                        help="Update targets.yml with discovered URLs")

    # backtest
    sub.add_parser("backtest", help="Backtest: classify all available articles (no Notion)")

    args = parser.parse_args()
    _setup_logging()

    if args.command == "scan":
        cmd_scan(args)
    elif args.command == "discover":
        cmd_discover(args)
    elif args.command == "backtest":
        cmd_backtest(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
