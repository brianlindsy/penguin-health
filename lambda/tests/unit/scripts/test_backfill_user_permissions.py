"""
Tests for scripts/backfill_user_permissions.py.

DynamoDB is mocked via moto. The Cognito surface used by the script is small
(list_users + admin_list_groups_for_user), so we use a tiny in-memory fake
instead of pulling in moto's cognito-idp extra (joserfc).
"""

import os
import sys

import boto3
import pytest
from moto import mock_aws

# Put the scripts/ dir on sys.path so we can `import backfill_user_permissions`.
SCRIPTS_DIR = os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..', 'scripts'
)
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))


class FakeCognito:
    """Minimal fake covering the two methods the script calls."""

    def __init__(self, page_size=60):
        self._page_size = page_size
        self._users = []         # list of dicts {Username, Attributes: [...]}
        self._groups = {}        # username -> set of group names

    def add_user(self, email, *, org_id=None, groups=()):
        attrs = [{'Name': 'email', 'Value': email}]
        if org_id:
            attrs.append({'Name': 'custom:organization_id', 'Value': org_id})
        self._users.append({'Username': email, 'Attributes': attrs})
        if groups:
            self._groups[email] = set(groups)

    # boto3-shaped API ----------------------------------------------------

    def list_users(self, *, UserPoolId, Limit=60, PaginationToken=None):
        start = int(PaginationToken) if PaginationToken else 0
        end = start + min(Limit, self._page_size)
        page = self._users[start:end]
        out = {'Users': page}
        if end < len(self._users):
            out['PaginationToken'] = str(end)
        return out

    def admin_list_groups_for_user(self, *, UserPoolId, Username):
        groups = self._groups.get(Username, set())
        return {'Groups': [{'GroupName': g} for g in groups]}


@pytest.fixture
def perm_table():
    """Fresh DynamoDB table per test, set up to mirror prod schema."""
    with mock_aws():
        ddb = boto3.resource('dynamodb', region_name='us-east-1')
        ddb.create_table(
            TableName='penguin-health-org-config',
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'},
                {'AttributeName': 'gsi1pk', 'AttributeType': 'S'},
                {'AttributeName': 'gsi1sk', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[{
                'IndexName': 'gsi1',
                'KeySchema': [
                    {'AttributeName': 'gsi1pk', 'KeyType': 'HASH'},
                    {'AttributeName': 'gsi1sk', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'},
            }],
            BillingMode='PAY_PER_REQUEST',
        )
        yield ddb.Table('penguin-health-org-config')


@pytest.fixture
def cognito():
    return FakeCognito()


class TestBackfill:
    def test_grants_org_admin_to_normal_users(self, cognito, perm_table):
        from backfill_user_permissions import backfill

        cognito.add_user('alice@clinic.com', org_id='org-A')
        cognito.add_user('bob@clinic.com', org_id='org-B')

        counts = backfill(cognito, perm_table, 'pool-id', dry_run=False)

        assert counts['granted'] == 2
        assert counts['skipped_super_admin'] == 0

        alice = perm_table.get_item(
            Key={'pk': 'USER#alice@clinic.com', 'sk': 'ORG#org-A'}
        )
        assert alice['Item']['role'] == 'org_admin'
        assert alice['Item']['gsi1pk'] == 'USER_PERM'
        assert alice['Item']['gsi1sk'] == 'ORG#org-A#USER#alice@clinic.com'

    def test_skips_super_admins(self, cognito, perm_table):
        from backfill_user_permissions import backfill

        cognito.add_user('admin@clinic.com', org_id='org-A', groups=['Admins'])

        counts = backfill(cognito, perm_table, 'pool-id', dry_run=False)

        assert counts['skipped_super_admin'] == 1
        assert counts['granted'] == 0
        result = perm_table.get_item(
            Key={'pk': 'USER#admin@clinic.com', 'sk': 'ORG#org-A'}
        )
        assert 'Item' not in result

    def test_skips_users_without_org(self, cognito, perm_table):
        from backfill_user_permissions import backfill

        cognito.add_user('orphan@clinic.com')

        counts = backfill(cognito, perm_table, 'pool-id', dry_run=False)

        assert counts['skipped_no_org'] == 1
        assert counts['granted'] == 0

    def test_dry_run_writes_nothing(self, cognito, perm_table):
        from backfill_user_permissions import backfill

        cognito.add_user('alice@clinic.com', org_id='org-A')

        counts = backfill(cognito, perm_table, 'pool-id', dry_run=True)

        assert counts['granted'] == 1  # would-be granted, but...
        result = perm_table.get_item(
            Key={'pk': 'USER#alice@clinic.com', 'sk': 'ORG#org-A'}
        )
        assert 'Item' not in result

    def test_idempotent_does_not_overwrite_existing_record(self, cognito, perm_table):
        """Re-running must not clobber a record edited after the first run."""
        from backfill_user_permissions import backfill

        cognito.add_user('alice@clinic.com', org_id='org-A')

        # Pre-existing record with role demoted to member.
        perm_table.put_item(Item={
            'pk': 'USER#alice@clinic.com',
            'sk': 'ORG#org-A',
            'gsi1pk': 'USER_PERM',
            'gsi1sk': 'ORG#org-A#USER#alice@clinic.com',
            'email': 'alice@clinic.com',
            'organization_id': 'org-A',
            'role': 'member',
            'report_permissions': {'Billing': ['view']},
            'analytics_permissions': [],
        })

        counts = backfill(cognito, perm_table, 'pool-id', dry_run=False)

        assert counts['already_existed'] == 1
        assert counts['granted'] == 0
        item = perm_table.get_item(
            Key={'pk': 'USER#alice@clinic.com', 'sk': 'ORG#org-A'}
        )['Item']
        assert item['role'] == 'member'
        assert item['report_permissions'] == {'Billing': ['view']}

    def test_paginates_through_many_users(self, perm_table):
        """ListUsers caps at Limit=60; ensure we walk past page boundaries."""
        from backfill_user_permissions import backfill

        cognito = FakeCognito(page_size=10)  # force pagination
        for i in range(25):
            cognito.add_user(f'user{i}@clinic.com', org_id='org-A')

        counts = backfill(cognito, perm_table, 'pool-id', dry_run=False)

        assert counts['granted'] == 25


class TestBuildItem:
    def test_item_shape(self):
        from backfill_user_permissions import build_org_admin_item

        item = build_org_admin_item('a@b.com', 'org-X')

        assert item['pk'] == 'USER#a@b.com'
        assert item['sk'] == 'ORG#org-X'
        assert item['gsi1pk'] == 'USER_PERM'
        assert item['gsi1sk'] == 'ORG#org-X#USER#a@b.com'
        assert item['role'] == 'org_admin'
        assert item['report_permissions'] == {}
        assert item['analytics_permissions'] == []
        assert item['email'] == 'a@b.com'
        assert item['organization_id'] == 'org-X'
        assert 'created_at' in item
        assert 'updated_at' in item
