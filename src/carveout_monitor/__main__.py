"""CLI entry point: python -m carveout_monitor [scan|discover|backtest|reset-state|lookback]"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import shutil
import sys
import time
import yaml
from pathlib import Path

from .models import load_firms, DealAlert, DealStage, DealType, QualifiedAlert
from .feeds import fetch_articles, fetch_all_articles, fetch_core_feeds, fetch_core_feeds_lookback, discover_feeds, get_law_firm_sources
from .scraper import scrape_articles, discover_press_page
from .classifier import classify_articles, token_usage as haiku_tokens
from .qualifier import qualify_alerts, token_usage as opus_tokens
from .notion import NotionClient, _append_page_content
from .state import StateManager, _deal_key, deals_match

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
    if getattr(args, "skip_scraper", False):
        logger.info("Skipping press page scraper (--skip-scraper)")
    else:
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

        # Step 2b: Scrape law firm press pages
        t0 = time.time()
        law_firms = get_law_firm_sources()
        law_firm_articles = scrape_articles(law_firms, lookback_hours=args.hours)
        articles.extend(law_firm_articles)
        logger.info("[%.1fs] Law firm scrape: %d articles from %d firms",
                    time.time() - t0, len(law_firm_articles), len(law_firms))

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

    # Step 6: Filter to separation deals (all three deal types)
    carveouts = [a for a in all_alerts if a.deal_type is not None and a.confidence >= 50]
    logger.info("Haiku positives (raw): %d (confidence >= 50)", len(carveouts))

    # Step 6a: Reject deals where seller is unknown/unidentifiable — these are almost
    # always regular PE acquisitions that Haiku misclassified as carve-outs.
    pre_filter = len(carveouts)
    carveouts = [a for a in carveouts
                 if a.seller.strip()
                 and not a.seller.strip().lower().startswith("unknown")
                 and a.seller.strip().upper() != "N/A"]
    unknown_dropped = pre_filter - len(carveouts)
    if unknown_dropped:
        logger.info("Dropped %d alerts with unknown/unidentifiable sellers", unknown_dropped)
    logger.info("Haiku positives: %d (after seller filter)", len(carveouts))

    for alert in carveouts:
        stage = alert.stage.value if alert.stage else "unknown"
        dtype = alert.deal_type.value if alert.deal_type else "unknown"
        logger.info("  [%s][%s] %s — target: %s, seller: %s, buyer: %s (confidence: %d)",
                     stage, dtype, alert.article.title[:70],
                     alert.target_company, alert.seller, alert.buyer, alert.confidence)

    # Step 6b: Within-run deal dedup — fuzzy match on (target, seller, stage).
    # Signing and closing of the same deal have different stages so both pass.
    # Articles about the same deal with different wording (e.g. "ContiTech" vs
    # "Continental Industrial Unit") are collapsed via token overlap matching.
    deduped: list[DealAlert] = []
    for alert in carveouts:
        stage_val = alert.stage.value if alert.stage else "unknown"
        merged = False
        for i, existing in enumerate(deduped):
            existing_stage = existing.stage.value if existing.stage else "unknown"
            if existing_stage != stage_val:
                continue
            if deals_match(alert.target_company, alert.seller,
                           existing.target_company, existing.seller):
                # Merge context: append loser's title to winner's reasoning
                # so Opus qualifier sees all article headlines for PE firm extraction
                if alert.confidence > existing.confidence:
                    logger.info("  Within-run dedup: replacing '%s / %s' with higher-confidence '%s / %s' [%s]",
                                existing.target_company, existing.seller,
                                alert.target_company, alert.seller, stage_val)
                    alert.reasoning = (alert.reasoning or "") + f"\n[Also: {existing.article.title}]"
                    deduped[i] = alert
                else:
                    logger.info("  Within-run dedup: dropping '%s / %s' (duplicate of '%s / %s') [%s]",
                                alert.target_company, alert.seller,
                                existing.target_company, existing.seller, stage_val)
                    existing.reasoning = (existing.reasoning or "") + f"\n[Also: {alert.article.title}]"
                merged = True
                break
        if not merged:
            deduped.append(alert)
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

    # Step 7: Qualify with Opus
    t0 = time.time()
    qualified = qualify_alerts(carveouts)
    pursue = [a for a in qualified if a.recommended_action == "pursue"]
    monitor = [a for a in qualified if a.recommended_action == "monitor"]
    discard_count = len(qualified) - len(pursue) - len(monitor)
    logger.info("[%.1fs] Qualification: %d pursue, %d monitor, %d discard",
                time.time() - t0, len(pursue), len(monitor), discard_count)

    for alert in pursue:
        logger.info("  PURSUE [%d%%] %s — PE: %s, target: %s",
                     alert.larkhill_fit, alert.article.title[:60],
                     alert.pe_firm, alert.target_company)

    # Step 8: Write pursue + monitor to Notion
    actionable = pursue + monitor
    notion_page_ids: dict[int, str] = {}
    if not args.skip_notion and actionable:
        t0 = time.time()
        notion_client = NotionClient()
        if notion_client.configured:
            stats = notion_client.write_alerts(actionable)
            notion_page_ids = stats.get("page_ids", {})
            logger.info("[%.1fs] Notion: %d written, %d skipped, %d errors",
                        time.time() - t0, stats["written"], stats["skipped"], stats["errors"])
            # Mark written deals as seen
            for alert in actionable:
                state.mark_deal_seen(
                    alert.target_company, alert.seller,
                    alert.stage.value if alert.stage else "unknown"
                )
        else:
            logger.warning("Notion not configured — skipping")
    elif args.skip_notion:
        logger.info("Notion output skipped (--skip-notion flag)")

    # HubSpot deal creation is handled by the deal-brief-generator
    # scheduled task (8am local via Claude Code). It reads Queued rows
    # from Notion and creates fully-populated deals with .docx briefs.

    # Step 9: Mark all processed articles as seen
    for a in new_articles:
        state.mark_seen(a.url)
    state.save()

    elapsed = time.time() - start
    logger.info("Pipeline complete in %.1fs: %d articles processed, %d qualified (%d pursue, %d monitor)",
                elapsed, len(new_articles), len(qualified), len(pursue), len(monitor))

    # Cost reporting
    # Haiku: $0.80/M input, $4.00/M output
    # Opus: $15.00/M input, $75.00/M output
    haiku_cost = (haiku_tokens["input"] * 0.80 + haiku_tokens["output"] * 4.00) / 1_000_000
    opus_cost = (opus_tokens["input"] * 15.00 + opus_tokens["output"] * 75.00) / 1_000_000
    total_cost = haiku_cost + opus_cost
    logger.info("API cost estimate: Haiku $%.4f (%dk in, %dk out) + Opus $%.4f (%dk in, %dk out) = $%.4f total",
                haiku_cost, haiku_tokens["input"] // 1000, haiku_tokens["output"] // 1000,
                opus_cost, opus_tokens["input"] // 1000, opus_tokens["output"] // 1000,
                total_cost)


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
    deals = [a for a in all_alerts if a.deal_type is not None]
    logger.info("\n=== BACKTEST RESULTS ===")
    logger.info("Total articles: %d", len(articles))
    logger.info("Separation deals found: %d", len(deals))

    if deals:
        # Group by deal type
        by_type: dict[str, list[DealAlert]] = {}
        for alert in deals:
            dtype = alert.deal_type.value if alert.deal_type else "unknown"
            by_type.setdefault(dtype, []).append(alert)

        for dtype, type_alerts in sorted(by_type.items()):
            logger.info("\n  %s (%d alerts):", dtype, len(type_alerts))
            for alert in type_alerts:
                stage = alert.stage.value if alert.stage else "?"
                date = alert.article.published.strftime("%Y-%m-%d") if alert.article.published else "no date"
                logger.info("    [%s] %s — %s → %s (confidence: %d)",
                            stage, date, alert.article.title[:60],
                            alert.buyer or "unknown buyer", alert.confidence)
    else:
        logger.info("No separation deals found in backtest data")


def cmd_reset_state(args):
    """Back up and reset state.json to a fresh empty state."""
    state_path = Path(args.state)
    if state_path.exists():
        backup_path = state_path.with_suffix(".json.bak")
        shutil.copy2(state_path, backup_path)
        logger.info("Backed up %s → %s", state_path, backup_path)
    else:
        logger.info("No existing state file at %s — nothing to back up", state_path)

    fresh_state = {"version": 1, "last_run": None, "seen": {}, "seen_deals": {}}
    with open(state_path, "w") as f:
        json.dump(fresh_state, f, indent=2)
    logger.info("Wrote fresh empty state to %s", state_path)


def cmd_lookback(args):
    """Lookback exercise: fetch extended window, classify all, export CSV."""
    start = time.time()
    firms = load_firms(args.targets)
    logger.info("Loaded %d target firms", len(firms))
    logger.info("Lookback: %d days, output → %s", args.days, args.output)

    # Step 1: Fetch all articles from firm RSS feeds (no date filter)
    t0 = time.time()
    articles = fetch_all_articles(firms)
    logger.info("[%.1fs] RSS fetch (no date filter): %d articles", time.time() - t0, len(articles))

    # Step 2: Fetch core feeds with extended Google News window
    t0 = time.time()
    core_articles = fetch_core_feeds_lookback(days=args.days)
    articles.extend(core_articles)
    logger.info("[%.1fs] Core feeds (lookback %dd): %d articles",
                time.time() - t0, args.days, len(core_articles))

    # Step 3: Scrape press pages (no date filter)
    t0 = time.time()
    scraped = scrape_articles(firms, lookback_hours=args.days * 24)
    articles.extend(scraped)
    logger.info("[%.1fs] Scrape: %d additional articles", time.time() - t0, len(scraped))

    # Step 3b: Scrape law firm press pages
    t0 = time.time()
    law_firms = get_law_firm_sources()
    law_firm_articles = scrape_articles(law_firms, lookback_hours=args.days * 24)
    articles.extend(law_firm_articles)
    logger.info("[%.1fs] Law firm scrape: %d articles", time.time() - t0, len(law_firm_articles))

    if not articles:
        logger.info("No articles found — nothing to classify")
        return

    # Step 4: URL dedup
    seen_urls: set[str] = set()
    unique = []
    for a in articles:
        if a.url not in seen_urls:
            seen_urls.add(a.url)
            unique.append(a)
    logger.info("After URL dedup: %d articles (removed %d duplicates)",
                len(unique), len(articles) - len(unique))
    articles = unique

    # Step 5: Classify ALL articles
    t0 = time.time()
    all_alerts = classify_articles(articles)
    logger.info("[%.1fs] Classification complete: %d articles", time.time() - t0, len(all_alerts))

    # Step 6: Write ALL results to CSV (positives AND negatives)
    output_path = Path(args.output)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "title", "url", "source", "published",
            "deal_type", "stage", "target_company", "seller", "buyer",
            "confidence", "reasoning",
        ])
        for alert in all_alerts:
            a = alert.article
            writer.writerow([
                a.title,
                a.url,
                a.firm_name,
                a.published.strftime("%Y-%m-%d %H:%M") if a.published else "",
                alert.deal_type.value if alert.deal_type else "none",
                alert.stage.value if alert.stage else "",
                alert.target_company,
                alert.seller,
                alert.buyer,
                alert.confidence,
                alert.reasoning,
            ])

    carveouts = [a for a in all_alerts if a.deal_type is not None]
    elapsed = time.time() - start
    logger.info("Lookback complete in %.1fs: %d articles classified, %d carve-outs, CSV → %s",
                elapsed, len(all_alerts), len(carveouts), output_path)


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
    # HubSpot deal creation moved to deal-brief-generator scheduled task
    scan_p.add_argument("--skip-scraper", action="store_true", help="Skip press page scraping")

    # discover
    disc_p = sub.add_parser("discover", help="Discover RSS feeds and press pages for all firms")
    disc_p.add_argument("--update", action="store_true",
                        help="Update targets.yml with discovered URLs")

    # backtest
    sub.add_parser("backtest", help="Backtest: classify all available articles (no Notion)")

    # reset-state
    sub.add_parser("reset-state", help="Back up and reset state.json to fresh empty state")

    # lookback
    lookback_p = sub.add_parser("lookback", help="Lookback exercise: extended fetch + classify all to CSV")
    lookback_p.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    lookback_p.add_argument("--output", default="lookback_results.csv", help="Output CSV path (default: lookback_results.csv)")

    args = parser.parse_args()
    _setup_logging()

    if args.command == "scan":
        cmd_scan(args)
    elif args.command == "discover":
        cmd_discover(args)
    elif args.command == "backtest":
        cmd_backtest(args)
    elif args.command == "reset-state":
        cmd_reset_state(args)
    elif args.command == "lookback":
        cmd_lookback(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
