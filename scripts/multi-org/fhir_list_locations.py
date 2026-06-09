#!/usr/bin/env python3
"""
List every Location resource on a FHIR server, printing id, name, and
description. Used during stedi onboarding to figure out which site/unit
ids exist (e.g. to scope an Encounter histogram to one facility) when
the server doesn't expose Encounter.location as a search param and we
have to filter client-side.

Location is low-cardinality and not PHI on its own (facility names,
addresses), so this script prints record fields directly rather than
aggregating. Don't extend it to also dump Patient or Encounter — those
are PHI and need the aggregate-only treatment in
fhir_encounter_class_histogram.py.

Usage:

    python scripts/multi-org/fhir_list_locations.py \\
        --base-url https://coc.fhir.cbh4.crediblebh.com/R4 \\
        --token "$BEARER"
"""

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request


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
    parser.add_argument('--page-size', type=int, default=50,
                        help='_count per page (default 50, the Firely cap)')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Stop after N pages (default: walk all)')
    args = parser.parse_args()

    params = {'_count': args.page_size}
    url = f"{args.base_url.rstrip('/')}/Location?{urllib.parse.urlencode(params)}"

    locations = []
    pages = 0

    while url:
        bundle = fetch_bundle(url, args.token)
        pages += 1
        for entry in bundle.get('entry') or []:
            resource = entry.get('resource') or {}
            if resource.get('resourceType') != 'Location':
                continue
            addr = resource.get('address') or {}
            addr_parts = [
                ', '.join(addr.get('line') or []),
                addr.get('city'),
                addr.get('state'),
                addr.get('postalCode'),
            ]
            address = ', '.join(p for p in addr_parts if p)
            locations.append({
                'id': resource.get('id'),
                'name': resource.get('name'),
                'description': resource.get('description'),
                'status': resource.get('status'),
                'address': address,
                'type': [
                    c.get('code')
                    for t in (resource.get('type') or [])
                    for c in (t.get('coding') or [])
                ],
            })

        print(f'  page {pages}: {len(locations)} locations so far',
              file=sys.stderr)
        if args.max_pages and pages >= args.max_pages:
            print(f'  stopping at --max-pages={args.max_pages}',
                  file=sys.stderr)
            break
        url = next_link(bundle)

    print()
    print(f'Total locations: {len(locations)} across {pages} page(s)')
    print()
    for loc in locations:
        print(f'[{loc["id"]}] {loc["name"] or "(no name)"}'
              f'  status={loc["status"] or "-"}')
        if loc['description']:
            print(f'    description: {loc["description"]}')
        if loc['address']:
            print(f'    address:     {loc["address"]}')
        if loc['type']:
            print(f'    type:        {", ".join(loc["type"])}')
        print()


if __name__ == '__main__':
    main()
