#!/usr/bin/env python3
"""
Process web search extracted signals through filtering and deduplication pipeline
"""

import json
from datetime import datetime
from web_search_deal_processor import (
    passes_post_filters,
    is_duplicate,
    write_signals_to_csv,
    is_target_pe_firm
)


def process_signals():
    """Load, filter, deduplicate and output signals."""

    # Load extracted signals
    with open('web_search_extracted_signals.json', 'r') as f:
        signals = json.load(f)

    print(f"Loaded {len(signals)} extracted signals")

    # Add date_identified to all signals
    today = datetime.now().strftime('%Y-%m-%d')
    for signal in signals:
        signal['date_identified'] = today

    # Apply post-filters
    print("\n=== Applying Post-Filters ===")
    filtered_signals = []
    excluded_signals = []

    for signal in signals:
        # Check if PE buyer is on target list for completed/definitive deals
        if signal['signal_type'] in ['Definitive Agreement', 'Deal Completed']:
            pe_buyers = signal.get('pe_buyer', '').split(',')
            found_target = False
            for buyer in pe_buyers:
                buyer = buyer.strip()
                if is_target_pe_firm(buyer):
                    found_target = True
                    print(f"  ✓ Found target PE firm: {buyer}")
                    break

            if not found_target and signal.get('pe_buyer'):
                print(f"  ✗ Excluded {signal['company']} - {signal.get('division', 'N/A')}: PE buyer '{signal.get('pe_buyer')}' not on target list")
                excluded_signals.append((signal, 'PE buyer not on target list'))
                continue

        # Apply all post-filters
        if passes_post_filters(signal):
            filtered_signals.append(signal)
            print(f"  ✓ Passed: {signal['company']} - {signal.get('division', 'Whole company')}")
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
    print("\n=== Deduplicating Signals ===")
    deduplicated_signals = []
    duplicate_count = 0

    for signal in filtered_signals:
        if is_duplicate(signal, deduplicated_signals):
            print(f"  ✗ Duplicate: {signal['company']} - {signal.get('division', 'N/A')}")
            duplicate_count += 1
        else:
            deduplicated_signals.append(signal)
            print(f"  ✓ Unique: {signal['company']} - {signal.get('division', 'N/A')}")

    print(f"\nDeduplication results: {len(deduplicated_signals)} unique signals, {duplicate_count} duplicates removed")

    # Write to CSV
    print("\n=== Writing to CSV ===")
    output_file = 'web_search_deal_signals_2025.csv'
    write_signals_to_csv(deduplicated_signals, output_file)

    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total signals extracted: {len(signals)}")
    print(f"Passed post-filters: {len(filtered_signals)}")
    print(f"Excluded by filters: {len(excluded_signals)}")
    print(f"Duplicates removed: {duplicate_count}")
    print(f"Final unique signals: {len(deduplicated_signals)}")

    # Signal type breakdown
    signal_types = {}
    for signal in deduplicated_signals:
        st = signal.get('signal_type', 'Unknown')
        signal_types[st] = signal_types.get(st, 0) + 1

    print("\n=== Signal Type Breakdown ===")
    for st, count in sorted(signal_types.items(), key=lambda x: x[1], reverse=True):
        print(f"  {st}: {count}")

    # Geography breakdown
    geographies = {}
    for signal in deduplicated_signals:
        geo = signal.get('geography', 'Unknown')
        geographies[geo] = geographies.get(geo, 0) + 1

    print("\n=== Geography Breakdown ===")
    for geo, count in sorted(geographies.items(), key=lambda x: x[1], reverse=True):
        print(f"  {geo}: {count}")

    # Sector breakdown
    sectors = {}
    for signal in deduplicated_signals:
        sector = signal.get('sector', 'Unknown')
        sectors[sector] = sectors.get(sector, 0) + 1

    print("\n=== Sector Breakdown ===")
    for sector, count in sorted(sectors.items(), key=lambda x: x[1], reverse=True):
        print(f"  {sector}: {count}")

    # Top PE buyers
    pe_buyers = {}
    for signal in deduplicated_signals:
        buyer = signal.get('pe_buyer', 'Not disclosed')
        if buyer:
            # Handle multiple buyers
            for b in buyer.split(','):
                b = b.strip()
                if b and b != 'Not disclosed':
                    pe_buyers[b] = pe_buyers.get(b, 0) + 1

    print("\n=== Top PE Buyers ===")
    for buyer, count in sorted(pe_buyers.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {buyer}: {count}")

    # Exclusion reasons
    print("\n=== Exclusion Reasons ===")
    exclusion_reasons = {}
    for signal, reason in excluded_signals:
        exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1

    for reason, count in sorted(exclusion_reasons.items(), key=lambda x: x[1], reverse=True):
        print(f"  {reason}: {count}")


if __name__ == "__main__":
    process_signals()
