#!/usr/bin/env python3
"""
Aggregate `Encounter.class.code` (and per-class status) counts from a
FHIR server's Encounter feed, following pagination across the whole
window. Used during stedi onboarding to decide
`--encounter-filter-class-codes` when the server doesn't support
server-side `class`/`status` filtering and we have to filter
client-side.

PHI safety: this script never prints patient demographics, references,
or raw resources. It only emits aggregated counts keyed by class code
and status. The bundle is held in memory only long enough to extract
those two fields per entry.

Usage:

    python scripts/multi-org/fhir_encounter_class_histogram.py \\
        --base-url https://coc.fhir.cbh4.crediblebh.com/R4 \\
        --token "$BEARER" \\
        --since 2026-06-08T00:00:00Z

    # Optional: cap pages while spot-checking a large feed.
    python scripts/multi-org/fhir_encounter_class_histogram.py \\
        --base-url https://coc.fhir.cbh4.crediblebh.com/R4 \\
        --token "$BEARER" \\
        --since 2026-06-08T00:00:00Z \\
        --max-pages 5
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict


def fetch_bundle(url, token):
    req = urllib.request.Request(
        url,
        headers={
            'Accept': 'application/fhir+json',
            'Authorization': f'Bearer {token}',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f'HTTP {e.code} on {url}', file=sys.stderr)
        print(body, file=sys.stderr)
        raise


def next_link(bundle):
    for link in bundle.get('link') or []:
        if link.get('relation') == 'next':
            return link.get('url')
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True,
                        help='FHIR base, e.g. https://.../R4')
    parser.add_argument('--token', required=True,
                        help='OAuth bearer token')
    parser.add_argument('--since', default=None,
                        help='Date filter value, e.g. 2026-06-08T00:00:00Z. '
                             'Omit to pull with no date filter (may be huge).')
    parser.add_argument('--date-param', default='_lastUpdated',
                        choices=['_lastUpdated', 'date'],
                        help='Which date param to filter on. Firely on '
                             'Credible only supports these two on Encounter.')
    parser.add_argument('--date-prefix', default='ge',
                        choices=['ge', 'gt', 'eq', ''],
                        help='Date prefix. Some Firely builds reject gt; '
                             'empty string = no prefix (exact-match semantics).')
    parser.add_argument('--page-size', type=int, default=200,
                        help='_count per page (default 200)')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Stop after N pages (default: walk all)')
    parser.add_argument('--max-encounters', type=int, default=1000,
                        help='Stop after this many encounters (default 1000)')
    args = parser.parse_args()

    params = {'_count': args.page_size}
    if args.since:
        params[args.date_param] = f'{args.date_prefix}{args.since}'
    url = f"{args.base_url.rstrip('/')}/Encounter?{urllib.parse.urlencode(params)}"

    class_counts = Counter()
    class_status = defaultdict(Counter)
    total = 0
    pages = 0

    while url:
        bundle = fetch_bundle(url, args.token)
        pages += 1
        for entry in bundle.get('entry') or []:
            resource = entry.get('resource') or {}
            if resource.get('resourceType') != 'Encounter':
                continue
            total += 1
            cls = (resource.get('class') or {}).get('code') or '(none)'
            status = resource.get('status') or '(none)'
            class_counts[cls] += 1
            class_status[cls][status] += 1
            if total >= args.max_encounters:
                break

        print(f'  page {pages}: {total} encounters so far', file=sys.stderr)
        if total >= args.max_encounters:
            print(f'  stopping at --max-encounters={args.max_encounters}',
                  file=sys.stderr)
            break
        if args.max_pages and pages >= args.max_pages:
            print(f'  stopping at --max-pages={args.max_pages}', file=sys.stderr)
            break
        url = next_link(bundle)

    print()
    print(f'Total encounters sampled: {total} across {pages} page(s)')
    print()
    print('Class code histogram:')
    for cls, count in class_counts.most_common():
        pct = (count / total * 100) if total else 0
        print(f'  {cls:20s} {count:6d}  ({pct:5.1f}%)')

    print()
    print('Class x status breakdown:')
    for cls, _ in class_counts.most_common():
        statuses = class_status[cls]
        parts = ', '.join(f'{s}={n}' for s, n in statuses.most_common())
        print(f'  {cls:20s} {parts}')


if __name__ == '__main__':
    main()
