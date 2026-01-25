#!/usr/bin/env python3
"""
Comprehensive merge of all web search signals (late-stage + all early-stage batches)
"""

import json
from datetime import datetime
from web_search_deal_processor import (
    passes_post_filters,
    is_duplicate,
    write_signals_to_csv,
    is_target_pe_firm
)


def merge_all_signals():
    """Load ALL signal batches and merge."""

    # Load all three signal files
    with open('web_search_extracted_signals.json', 'r') as f:
        late_stage = json.load(f)

    with open('web_search_early_stage_signals.json', 'r') as f:
        early_stage_batch1 = json.load(f)

    with open('web_search_additional_early_stage_signals.json', 'r') as f:
        early_stage_batch2 = json.load(f)

    # Merge all
    all_signals = late_stage + early_stage_batch1 + early_stage_batch2

    print(f"=== LOADING ALL SIGNAL BATCHES ===")
    print(f"Late-stage signals (completed/definitive): {len(late_stage)}")
    print(f"Early-stage batch 1 (PE In Talks, Strategic Review): {len(early_stage_batch1)}")
    print(f"Early-stage batch 2 (Adviser Appointed, PE Interest, PE Bid Submitted): {len(early_stage_batch2)}")
    print(f"TOTAL SIGNALS TO PROCESS: {len(all_signals)}")

    # Add date_identified to all signals
    today = datetime.now().strftime('%Y-%m-%d')
    for signal in all_signals:
        signal['date_identified'] = today

    # Apply post-filters
    print("\n=== APPLYING POST-FILTERS ===")
    filtered_signals = []
    excluded_signals = []

    for signal in all_signals:
        # Check if PE buyer is on target list for completed/definitive deals
        if signal['signal_type'] in ['Definitive Agreement', 'Deal Completed']:
            if signal.get('pe_buyer'):
                pe_buyers = signal.get('pe_buyer', '').split(',')
                found_target = False
                for buyer in pe_buyers:
                    buyer = buyer.strip()
                    if is_target_pe_firm(buyer):
                        found_target = True
                        print(f"  ✓ Found target PE firm: {buyer}")
                        break

                if not found_target:
                    print(f"  ✗ Excluded {signal['company']} - {signal.get('division', 'N/A')}: PE buyer '{signal.get('pe_buyer')}' not on target list")
                    excluded_signals.append((signal, 'PE buyer not on target list'))
                    continue

        # Apply all post-filters
        if passes_post_filters(signal):
            filtered_signals.append(signal)
            print(f"  ✓ Passed: {signal['company']} - {signal.get('division', 'Whole company')} ({signal['signal_type']})")
        else:
            reason = "Failed post-filter checks"

            # Determine specific reason
            if signal['signal_type'] in ['Definitive Agreement', 'Deal Completed']:
                if not signal.get('pe_buyer'):
                    reason = "No PE buyer specified for completed/definitive deal"
                elif signal.get('geography') not in ['US', 'UK', 'Europe', 'Global']:
                    reason = f"Geography {signal.get('geography')} not in target list for completed/definitive deal"
            elif signal.get('geography') in ['China', 'Asia', 'Latin America', 'LatAm', 'Middle East', 'Africa', 'Australia', 'India']:
                reason = f"Excluded geography: {signal.get('geography')}"
            elif not signal.get('division') and not signal.get('pe_buyer'):
                reason = "Whole company sale without PE buyer (likely strategic M&A)"

            print(f"  ✗ Excluded {signal['company']} - {signal.get('division', 'N/A')}: {reason}")
            excluded_signals.append((signal, reason))

    print(f"\nPost-filter results: {len(filtered_signals)} passed, {len(excluded_signals)} excluded")

    # Deduplicate
    print("\n=== DEDUPLICATING SIGNALS ===")
    deduplicated_signals = []
    duplicate_count = 0

    for signal in filtered_signals:
        if is_duplicate(signal, deduplicated_signals):
            print(f"  ✗ Duplicate: {signal['company']} - {signal.get('division', 'N/A')}")
            duplicate_count += 1
        else:
            deduplicated_signals.append(signal)
            print(f"  ✓ Unique: {signal['company']} - {signal.get('division', 'N/A')} ({signal['signal_type']})")

    print(f"\nDeduplication results: {len(deduplicated_signals)} unique signals, {duplicate_count} duplicates removed")

    # Write to CSV
    print("\n=== WRITING TO CSV ===")
    output_file = 'web_search_deal_signals_2025_comprehensive.csv'
    write_signals_to_csv(deduplicated_signals, output_file)

    # Print summary statistics
    print("\n" + "="*80)
    print("COMPREHENSIVE WEB SEARCH RESULTS - FINAL SUMMARY")
    print("="*80)
    print(f"Total signals extracted: {len(all_signals)}")
    print(f"Passed post-filters: {len(filtered_signals)}")
    print(f"Excluded by filters: {len(excluded_signals)}")
    print(f"Duplicates removed: {duplicate_count}")
    print(f"FINAL UNIQUE SIGNALS: {len(deduplicated_signals)}")

    # Signal type breakdown
    signal_types = {}
    for signal in deduplicated_signals:
        st = signal.get('signal_type', 'Unknown')
        signal_types[st] = signal_types.get(st, 0) + 1

    print("\n=== SIGNAL TYPE BREAKDOWN ===")
    signal_order = [
        'Deal Completed',
        'Definitive Agreement',
        'PE Bid Submitted',
        'PE In Talks',
        'PE Interest',
        'Adviser Appointed',
        'Strategic Review'
    ]

    for st in signal_order:
        count = signal_types.get(st, 0)
        pct = (count / len(deduplicated_signals) * 100) if deduplicated_signals else 0
        status = "✓" if count > 0 else "❌"
        print(f"  {status} {st}: {count} ({pct:.1f}%)")

    # Calculate coverage by stage
    late_stage_count = signal_types.get('Deal Completed', 0) + signal_types.get('Definitive Agreement', 0)
    early_stage_count = len(deduplicated_signals) - late_stage_count

    print(f"\n=== DEAL STAGE COVERAGE ===")
    print(f"  Late-Stage (Agreement/Closed): {late_stage_count} ({late_stage_count/len(deduplicated_signals)*100:.1f}%)")
    print(f"  Early-Stage (Review→Talks): {early_stage_count} ({early_stage_count/len(deduplicated_signals)*100:.1f}%)")

    # Geography breakdown
    geographies = {}
    for signal in deduplicated_signals:
        geo = signal.get('geography', 'Unknown')
        geographies[geo] = geographies.get(geo, 0) + 1

    print("\n=== GEOGRAPHY BREAKDOWN ===")
    for geo, count in sorted(geographies.items(), key=lambda x: x[1], reverse=True):
        print(f"  {geo}: {count}")

    # Sector breakdown
    sectors = {}
    for signal in deduplicated_signals:
        sector = signal.get('sector', 'Unknown')
        sectors[sector] = sectors.get(sector, 0) + 1

    print("\n=== SECTOR BREAKDOWN ===")
    for sector, count in sorted(sectors.items(), key=lambda x: x[1], reverse=True):
        print(f"  {sector}: {count}")

    # Exclusion reasons
    print("\n=== EXCLUSION REASONS ===")
    exclusion_reasons = {}
    for signal, reason in excluded_signals:
        exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1

    for reason, count in sorted(exclusion_reasons.items(), key=lambda x: x[1], reverse=True):
        print(f"  {reason}: {count}")

    # Market coverage estimate
    print("\n=== MARKET COVERAGE ESTIMATE ===")
    print(f"Industry benchmark (S&P Global): ~290-350 PE carve-outs in 2025")
    print(f"Our capture: {len(deduplicated_signals)} unique signals")
    print(f"Coverage rate: {len(deduplicated_signals)/320*100:.1f}% (assuming 320 deals)")
    print(f"Late-stage coverage: {late_stage_count}/320 = {late_stage_count/320*100:.1f}%")
    print(f"Early-stage coverage: {early_stage_count}/~200 = {early_stage_count/200*100:.1f}% (est)")

    return deduplicated_signals


if __name__ == "__main__":
    signals = merge_all_signals()
