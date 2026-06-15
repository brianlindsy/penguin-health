#!/usr/bin/env python3
"""Upsert a per-vendor (or per-org override) RPA playbook into
penguin-health-org-config.

A playbook is a declarative list of read-only ops the Fargate runner's
playbook engine executes against a Playwright BrowserContext. The op
vocabulary is the strict allowlist in `lambda/multi-org/rpa/playbook_engine.py`:

    navigate, click, wait_for_selector, extract, loop_over_list,
    if_exists, emit_note, log, stop

`fill` is intentionally NOT in the allowlist — RPA is read-only on the
portal. This script validates that the playbook JSON only uses the
allowed ops and that selectors are plain CSS (no `text=`, `xpath=`,
`role=` prefixes).

Usage:

    # Seed a shared playbook (multiple orgs on the same vendor can use it):
    python scripts/multi-org/seed_rpa_playbook.py \\
        --playbook-id cr-notes-v1 \\
        --vendor centralreach \\
        --version 1 \\
        --json playbooks/centralreach/notes-v1.json

    # Seed an org-specific override (overlay shared playbook for one org):
    python scripts/multi-org/seed_rpa_playbook.py \\
        --playbook-id cr-notes-v1 \\
        --vendor centralreach \\
        --version 1 \\
        --org-id demo \\
        --json playbooks/centralreach/notes-v1-demo-override.json

The DDB key is:
    pk = ORG#shared (default) | ORG#{org_id}
    sk = RPA_PLAYBOOK#{playbook_id}

The runtime resolver (`load_playbook`) checks ORG#{org_id} first, then
ORG#shared, so an org-specific override takes precedence automatically.

Re-running this script overwrites the existing playbook item.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3


TABLE_NAME = 'penguin-health-org-config'

# Mirror of rpa.playbook_engine._ALLOWED_OPS. Kept duplicated rather than
# imported because this script runs from the repo root and shouldn't drag
# Lambda runtime modules onto sys.path. If you change one, change both —
# the rpa unit test pinning `_ALLOWED_OPS` will catch the runtime drift.
ALLOWED_OPS = frozenset({
    'navigate', 'click', 'wait_for_selector', 'extract',
    'loop_over_list', 'if_exists', 'emit_note', 'log', 'stop',
})

# Same as playbook_engine_playwright._css: any non-CSS prefix is rejected
# at the adapter, so we reject them at seed time too.
_NON_CSS_PREFIXES = ('text=', 'xpath=', 'role=', 'css=', '/', '//')


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--playbook-id', required=True,
                   help='Stable id used by RPA_CONFIG.playbook_id.')
    p.add_argument('--vendor', required=True,
                   help='Vendor key (e.g. centralreach).')
    p.add_argument('--version', type=int, required=True,
                   help='Monotonic version number. Bump on every change so '
                        'the runner audit event records the right playbook '
                        'revision.')
    p.add_argument('--json', dest='json_path', required=True,
                   help='Path to a JSON file with the playbook body. '
                        'See docs/rpa-integration-onboarding.md for the shape.')
    p.add_argument('--org-id', default=None,
                   help='When set, write as an org-specific override under '
                        'pk=ORG#{org_id}. When omitted, write as a shared '
                        'playbook under pk=ORG#shared.')
    p.add_argument('--region', default='us-east-1')
    p.add_argument('--dry-run', action='store_true')
    return p.parse_args(argv)


def _walk_ops(steps, errors, path='$.steps'):
    """Depth-first walk validating every op against ALLOWED_OPS and that
    selectors don't smuggle a non-CSS prefix.
    """
    if not isinstance(steps, list):
        errors.append(f"{path}: expected list, got {type(steps).__name__}")
        return
    for i, step in enumerate(steps):
        sp = f"{path}[{i}]"
        if not isinstance(step, dict):
            errors.append(f"{sp}: expected object, got {type(step).__name__}")
            continue
        op = step.get('op')
        if op not in ALLOWED_OPS:
            errors.append(
                f"{sp}.op={op!r} not in allowlist: {sorted(ALLOWED_OPS)}"
            )
            continue
        # Selector vetting — every op that has a selector key gets it
        # checked. extract has nested `fields[name].selector`.
        sel = step.get('selector')
        if sel is not None:
            _check_selector(sel, f"{sp}.selector", errors)
        if op == 'extract':
            for name, spec in (step.get('fields') or {}).items():
                if not isinstance(spec, dict):
                    errors.append(
                        f"{sp}.fields.{name}: expected object, got "
                        f"{type(spec).__name__}"
                    )
                    continue
                if 'selector' in spec:
                    _check_selector(
                        spec['selector'],
                        f"{sp}.fields.{name}.selector",
                        errors,
                    )
        if op == 'loop_over_list':
            body = step.get('body') or []
            _walk_ops(body, errors, path=f"{sp}.body")
        if op == 'if_exists':
            then = step.get('then') or []
            _walk_ops(then, errors, path=f"{sp}.then")
            else_ = step.get('else')
            if else_ is not None:
                _walk_ops(else_, errors, path=f"{sp}.else")


def _check_selector(selector, path, errors):
    if not isinstance(selector, str):
        errors.append(f"{path}: expected string, got {type(selector).__name__}")
        return
    stripped = selector.lstrip()
    for prefix in _NON_CSS_PREFIXES:
        if stripped.startswith(prefix):
            errors.append(
                f"{path}={selector!r} uses non-CSS prefix; v1 playbooks "
                "must use plain CSS selectors only"
            )
            return


def validate_playbook(doc):
    errors = []
    if not isinstance(doc, dict):
        return [f"$: expected object, got {type(doc).__name__}"]
    _walk_ops(doc.get('steps'), errors)
    return errors


def build_item(args, doc):
    now = datetime.now(timezone.utc).isoformat()
    pk = f'ORG#{args.org_id}' if args.org_id else 'ORG#shared'
    sk = f'RPA_PLAYBOOK#{args.playbook_id}'
    item = {
        'pk': pk,
        'sk': sk,
        'playbook_id': args.playbook_id,
        'vendor': args.vendor,
        'version': args.version,
        'auth': doc.get('auth') or {},
        'default_timeouts': doc.get('default_timeouts') or {},
        'steps': doc.get('steps') or [],
        'description': doc.get('description'),
        'created_at': now,
        'updated_at': now,
    }
    return item


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])

    with open(args.json_path, 'r', encoding='utf-8') as f:
        try:
            doc = json.load(f)
        except json.JSONDecodeError as e:
            raise SystemExit(f"failed to parse {args.json_path}: {e}")

    errors = validate_playbook(doc)
    if errors:
        print("Playbook validation failed:", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 2

    item = build_item(args, doc)

    if args.dry_run:
        print(json.dumps(item, indent=2, default=str))
        return 0

    dynamodb = boto3.resource('dynamodb', region_name=args.region)
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)
    print(
        f"Wrote RPA_PLAYBOOK#{args.playbook_id} "
        f"(pk={item['pk']}, vendor={args.vendor}, version={args.version}, "
        f"steps={len(item['steps'])})"
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
