#!/usr/bin/env python3
"""
One-time backfill that grants every existing non-super-admin Cognito user
org_admin permissions in their current organization.

Without this, the new RBAC code path defaults to deny for any user that
doesn't have a USER#<email> / ORG#<org_id> record in penguin-health-org-config,
which means non-super-admins lose access on cutover. Running this preserves
today's behavior — every existing user keeps full access in their org —
and granular tightening is opt-in per user from there.

Behavior, per Cognito user:
  - In the 'Admins' group         -> skip (super-admin path is unchanged)
  - Has custom:organization_id    -> write {role: org_admin} for that org
  - Missing custom:organization_id -> log a warning and skip (orphan)

Idempotent: PutItem uses attribute_not_exists(pk) so a re-run won't overwrite
records that have been edited (e.g. someone demoted to member after the first
run).

Usage:
    python scripts/backfill_user_permissions.py \\
        --user-pool-id us-east-1_XXXXXXXXX \\
        [--table-name penguin-health-org-config] \\
        [--region us-east-1] \\
        [--profile my-aws-profile] \\
        [--dry-run]
"""

import argparse
import sys
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


DEFAULT_TABLE = 'penguin-health-org-config'
DEFAULT_REGION = 'us-east-1'
SUPER_ADMIN_GROUP = 'Admins'


def list_all_cognito_users(cognito, user_pool_id):
    """Yield every user in the pool, paginating through ListUsers."""
    pagination_token = None
    while True:
        kwargs = {'UserPoolId': user_pool_id, 'Limit': 60}
        if pagination_token:
            kwargs['PaginationToken'] = pagination_token
        resp = cognito.list_users(**kwargs)
        for user in resp.get('Users', []):
            yield user
        pagination_token = resp.get('PaginationToken')
        if not pagination_token:
            break


def get_user_groups(cognito, user_pool_id, username):
    """Return the list of group names the user belongs to."""
    resp = cognito.admin_list_groups_for_user(
        UserPoolId=user_pool_id,
        Username=username,
    )
    return [g['GroupName'] for g in resp.get('Groups', [])]


def extract_attr(user, name):
    """Pull a named attribute off a Cognito user record."""
    for attr in user.get('Attributes', []):
        if attr.get('Name') == name:
            return attr.get('Value')
    return None


def build_org_admin_item(email, org_id):
    """The DDB item we write for one (email, org) pair."""
    now = datetime.utcnow().isoformat() + 'Z'
    return {
        'pk': f'USER#{email}',
        'sk': f'ORG#{org_id}',
        'gsi1pk': 'USER_PERM',
        'gsi1sk': f'ORG#{org_id}#USER#{email}',
        'email': email,
        'organization_id': org_id,
        'role': 'org_admin',
        'report_permissions': {},
        'analytics_permissions': [],
        'created_at': now,
        'updated_at': now,
    }


def backfill(cognito, table, user_pool_id, *, dry_run):
    """Walk the user pool and write org_admin records for eligible users."""
    counts = {
        'granted': 0,
        'already_existed': 0,
        'skipped_super_admin': 0,
        'skipped_no_org': 0,
        'skipped_no_email': 0,
    }

    for user in list_all_cognito_users(cognito, user_pool_id):
        username = user.get('Username')
        email = extract_attr(user, 'email')
        org_id = extract_attr(user, 'custom:organization_id')

        if not email:
            counts['skipped_no_email'] += 1
            print(f"  skip {username}: no email attribute")
            continue

        groups = get_user_groups(cognito, user_pool_id, username)
        if SUPER_ADMIN_GROUP in groups:
            counts['skipped_super_admin'] += 1
            print(f"  skip {email}: super admin")
            continue

        if not org_id:
            counts['skipped_no_org'] += 1
            print(f"  skip {email}: no custom:organization_id")
            continue

        if dry_run:
            counts['granted'] += 1
            print(f"  [dry-run] would grant org_admin: {email} -> {org_id}")
            continue

        item = build_org_admin_item(email, org_id)
        try:
            table.put_item(
                Item=item,
                ConditionExpression='attribute_not_exists(pk)',
            )
            counts['granted'] += 1
            print(f"  granted org_admin: {email} -> {org_id}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                counts['already_existed'] += 1
                print(f"  already exists, leaving alone: {email} -> {org_id}")
            else:
                raise

    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--user-pool-id', required=True,
                        help='Cognito user pool ID')
    parser.add_argument('--table-name', default=DEFAULT_TABLE,
                        help=f'DynamoDB table (default: {DEFAULT_TABLE})')
    parser.add_argument('--region', default=DEFAULT_REGION,
                        help=f'AWS region (default: {DEFAULT_REGION})')
    parser.add_argument('--profile', default=None,
                        help='Optional AWS profile name')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print what would change without writing')
    args = parser.parse_args(argv)

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cognito = session.client('cognito-idp')
    table = session.resource('dynamodb').Table(args.table_name)

    mode = 'DRY-RUN' if args.dry_run else 'WRITE'
    print(f"=== Backfill user permissions ({mode}) ===")
    print(f"  user pool : {args.user_pool_id}")
    print(f"  table     : {args.table_name}")
    print(f"  region    : {args.region}")
    print()

    counts = backfill(cognito, table, args.user_pool_id, dry_run=args.dry_run)

    print()
    print("=== Summary ===")
    for label, value in counts.items():
        print(f"  {label.replace('_', ' '):>22}: {value}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
