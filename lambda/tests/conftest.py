"""
Shared pytest fixtures for Penguin Health Lambda tests.

Uses moto to mock AWS services (DynamoDB, S3) and pytest-mock for Bedrock.
All tests run entirely in-memory with zero network calls.
"""

import json
import os
import sys

# Set AWS environment variables BEFORE importing boto3 or any modules that use it
# This is critical because admin_api.py creates boto3 clients at module level
os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
os.environ['AWS_SECURITY_TOKEN'] = 'testing'
os.environ['AWS_SESSION_TOKEN'] = 'testing'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

# admin_api.py and permissions.py are bundled flat at the Lambda asset root,
# so admin_api.py uses `import permissions`. Make that work in tests too by
# putting lambda/api on sys.path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))
# The Stedi package is bundled into the admin_api Lambda asset from
# lambda/multi-org/stedi; expose it under the same `import stedi` name in
# tests by putting lambda/multi-org on sys.path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'multi-org'))
# bedrock_client, claude_cost, and rate_limiter live in lambda/multi-org/
# rules-engine and are bundled flat into both the admin_api and rules-engine
# Lambda assets. Tests reach them the same way the Lambda runtime does.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'multi-org', 'rules-engine'))

# Audit emitter / decorator / actor live in lambda/multi-org/audit and are
# bundled as `audit/` into every emitting Lambda. lambda/multi-org is already
# on sys.path above, which makes `from audit import emit` resolve.

import pytest
import boto3
from moto import mock_aws


# -----------------------------------------------------------------------------
# AWS Credentials Fixture (for documentation, env vars already set above)
# -----------------------------------------------------------------------------

@pytest.fixture(scope="function")
def aws_credentials():
    """
    AWS credentials are set at module level above.
    This fixture exists for compatibility and documentation.
    """
    yield


@pytest.fixture(autouse=True)
def _reset_permission_cache():
    """The permission loader caches per (email, org_id); flush between tests.
    Same for the org PROGRAMS cache, which otherwise leaks the previous test's
    seeded list into a suite that expects a fresh empty allowlist."""
    try:
        import permissions as _perms
        _perms.invalidate_cache()
        _perms.invalidate_org_programs_cache()
    except ImportError:
        pass
    yield
    try:
        import permissions as _perms
        _perms.invalidate_cache()
        _perms.invalidate_org_programs_cache()
    except ImportError:
        pass


@pytest.fixture(autouse=True)
def _reset_audit_clients():
    """Drop the audit emitter's cached boto3 handles so each test's moto
    context gets fresh, properly-mocked clients. Without this, the first
    test to call audit.emit caches a client that subsequent moto contexts
    can't replace."""
    try:
        from audit import emitter as _audit_emitter
        _audit_emitter._reset_for_tests()
    except ImportError:
        pass
    yield
    try:
        from audit import emitter as _audit_emitter
        _audit_emitter._reset_for_tests()
    except ImportError:
        pass


# -----------------------------------------------------------------------------
# DynamoDB Mock Fixture
# -----------------------------------------------------------------------------

@pytest.fixture(scope="function")
def mock_dynamodb(aws_credentials):
    """
    Create mocked DynamoDB tables matching production schema.

    Tables created:
    - penguin-health-org-config (pk, sk, gsi1)
    - penguin-health-validation-results (pk, sk, gsi1, gsi2)
    """
    with mock_aws():
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

        # penguin-health-org-config table
        # Used by: admin_api.py (line 20), multi_org_config.py
        dynamodb.create_table(
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

        # penguin-health-validation-results table
        # Used by: admin_api.py (line 21), results_handler.py
        dynamodb.create_table(
            TableName='penguin-health-validation-results',
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'},
                {'AttributeName': 'gsi1pk', 'AttributeType': 'S'},
                {'AttributeName': 'gsi1sk', 'AttributeType': 'S'},
                {'AttributeName': 'gsi2pk', 'AttributeType': 'S'},
                {'AttributeName': 'gsi2sk', 'AttributeType': 'S'},
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'gsi1',
                    'KeySchema': [
                        {'AttributeName': 'gsi1pk', 'KeyType': 'HASH'},
                        {'AttributeName': 'gsi1sk', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                },
                {
                    'IndexName': 'gsi2',
                    'KeySchema': [
                        {'AttributeName': 'gsi2pk', 'KeyType': 'HASH'},
                        {'AttributeName': 'gsi2sk', 'KeyType': 'RANGE'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                },
            ],
            BillingMode='PAY_PER_REQUEST',
        )

        # penguin-health-analytics-reports table
        # Used by: admin_api.py for saved NL-analytics snapshots.
        dynamodb.create_table(
            TableName='penguin-health-analytics-reports',
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'},
            ],
            BillingMode='PAY_PER_REQUEST',
        )

        # penguin-health-audit table
        # Used by: audit.emit (HIPAA audit hot mirror, 90d TTL).
        # Schema mirrors penguin-health-stedi's AUDIT# row layout so
        # post-cutover the stedi dedup queries keep working.
        dynamodb.create_table(
            TableName='penguin-health-audit',
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

        # penguin-health-narrative-hashes table
        # Used by: deterministic_evaluator.op_narrative_hash_unique (rule 1
        # of the supportive-care org's compliance rules). TTL attribute is
        # `ttl` but moto doesn't enforce eviction — tests must rely on
        # explicit deletions / fresh fixtures rather than TTL aging.
        dynamodb.create_table(
            TableName='penguin-health-narrative-hashes',
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'},
            ],
            BillingMode='PAY_PER_REQUEST',
        )

        # penguin-health-stedi table
        # Used by: stedi.audit (eligibility audit log + daily counter).
        dynamodb.create_table(
            TableName='penguin-health-stedi',
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

        yield dynamodb


# -----------------------------------------------------------------------------
# S3 Mock Fixture
# -----------------------------------------------------------------------------

@pytest.fixture(scope="function")
def mock_s3(aws_credentials):
    """Create mocked S3 bucket for file operations."""
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='penguin-health-test-org')
        yield s3


# -----------------------------------------------------------------------------
# Sample Organization Config Fixture
# -----------------------------------------------------------------------------

@pytest.fixture(scope="function")
def sample_org_config(mock_dynamodb):
    """
    Seed DynamoDB with sample organization data for testing.

    Creates:
    - Organization metadata (ORG#test-org, METADATA)
    - Sample rule (ORG#test-org, RULE#rule-001)
    - Rules config (ORG#test-org, RULES_CONFIG)
    """
    table = mock_dynamodb.Table('penguin-health-org-config')

    # Organization metadata
    table.put_item(Item={
        'pk': 'ORG#test-org',
        'sk': 'METADATA',
        'gsi1pk': 'ORG_METADATA',
        'gsi1sk': 'test-org',
        'organization_id': 'test-org',
        'organization_name': 'Test Organization',
        'enabled': True,
        's3_bucket_name': 'penguin-health-test-org',
        'created_at': '2024-01-01T00:00:00Z',
    })

    # Sample rule
    table.put_item(Item={
        'pk': 'ORG#test-org',
        'sk': 'RULE#rule-001',
        'gsi1pk': 'RULE',
        'gsi1sk': 'ORG#test-org#RULE#rule-001',
        'rule_id': 'rule-001',
        'name': 'Service Date Documentation',
        'category': 'Compliance Audit',
        'enabled': True,
        'type': 'llm',
        'rule_text': 'Verify the service date is documented in the chart.',
        'fields_to_extract': [
            {'name': 'service_date', 'type': 'datetime', 'description': 'Date of service'}
        ],
    })

    # Rules config (field mappings)
    table.put_item(Item={
        'pk': 'ORG#test-org',
        'sk': 'RULES_CONFIG',
        'gsi1pk': 'RULES_CONFIG',
        'gsi1sk': 'ORG#test-org',
        'organization_id': 'test-org',
        'field_mappings': {'document_id': 'Consumer Service ID:'},
        'csv_column_mappings': {'service_id': '1_Service_ID'},
    })

    return table


# -----------------------------------------------------------------------------
# Sample Validation Result Fixture
# -----------------------------------------------------------------------------

@pytest.fixture(scope="function")
def sample_validation_result(mock_dynamodb, sample_org_config):
    """
    Seed a validation result for testing confirm/resolve/incorrect flows.

    Creates:
    - Document validation result (DOC#12345, VALIDATION#...)
    - Validation run summary (ORG#test-org, RUN#...)
    """
    table = mock_dynamodb.Table('penguin-health-validation-results')

    # Document validation result
    table.put_item(Item={
        'pk': 'DOC#12345',
        'sk': 'VALIDATION#2024-01-15T10:00:00',
        'gsi1pk': 'ORG#test-org',
        'gsi1sk': 'DOC#12345',
        'gsi2pk': 'RUN#20240115-100000',
        'gsi2sk': 'DOC#12345',
        'document_id': '12345',
        'validation_run_id': '20240115-100000',
        'organization_id': 'test-org',
        'rules': [
            {'rule_id': 'rule-001', 'category': 'Compliance Audit',
             'status': 'FAIL', 'message': 'Service date not found'}
        ],
        'extracted_fields': {'service_id': '12345'},
    })

    # Validation run summary
    table.put_item(Item={
        'pk': 'ORG#test-org',
        'sk': 'RUN#20240115-100000',
        'gsi1pk': 'VALIDATION_RUN',
        'gsi1sk': 'ORG#test-org#20240115-100000',
        'validation_run_id': '20240115-100000',
        'organization_id': 'test-org',
        'timestamp': '2024-01-15T10:00:00Z',
        'total_documents': 10,
        'passed': 8,
        'failed': 2,
        'skipped': 0,
    })

    return table


# -----------------------------------------------------------------------------
# JWT Event Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def super_admin_event():
    """
    API Gateway event simulating a super admin user.

    Super admins have 'Admins' in cognito:groups claim.
    They can access any organization's data.
    """
    return {
        'requestContext': {
            'authorizer': {
                'jwt': {
                    'claims': {
                        'email': 'admin@example.com',
                        'sub': 'admin-user-id-123',
                        'cognito:groups': '[Admins]',
                    }
                }
            }
        },
        'pathParameters': {},
        'body': None,
    }


@pytest.fixture
def org_user_event():
    """
    API Gateway event simulating an org-scoped user.

    Org users have custom:organization_id but NOT in Admins group.
    They can only access their own organization's data.
    """
    return {
        'requestContext': {
            'authorizer': {
                'jwt': {
                    'claims': {
                        'email': 'user@example.com',
                        'sub': 'org-user-id-456',
                        'cognito:groups': '[]',
                        'custom:organization_id': 'test-org',
                    }
                }
            }
        },
        'pathParameters': {},
        'body': None,
    }


@pytest.fixture
def member_event():
    """API Gateway event simulating a non-admin org member (no Cognito groups)."""
    return {
        'requestContext': {
            'authorizer': {
                'jwt': {
                    'claims': {
                        'email': 'member@example.com',
                        'sub': 'member-id-789',
                        'cognito:groups': '[]',
                        'custom:organization_id': 'test-org',
                    }
                }
            }
        },
        'pathParameters': {},
        'body': None,
    }


@pytest.fixture
def seed_user_perms(mock_dynamodb):
    """Helper fixture: write a USER#<email> / ORG#<org> perm record."""
    table = mock_dynamodb.Table('penguin-health-org-config')

    def _seed(email, org_id, *, role='member', report_permissions=None,
              analytics_permissions=None, program_permissions=None):
        from datetime import datetime
        report_permissions = report_permissions or {}
        analytics_permissions = analytics_permissions or []
        program_permissions = program_permissions or []
        now = datetime.utcnow().isoformat() + 'Z'
        item = {
            'pk': f'USER#{email}',
            'sk': f'ORG#{org_id}',
            'gsi1pk': 'USER_PERM',
            'gsi1sk': f'ORG#{org_id}#USER#{email}',
            'email': email,
            'organization_id': org_id,
            'role': role,
            'report_permissions': report_permissions,
            'analytics_permissions': analytics_permissions,
            'program_permissions': program_permissions,
            'created_at': now,
            'updated_at': now,
        }
        table.put_item(Item=item)
        return item

    return _seed


@pytest.fixture
def seed_org_programs(mock_dynamodb):
    """Helper fixture: write the org's canonical PROGRAMS list."""
    table = mock_dynamodb.Table('penguin-health-org-config')

    def _seed(org_id, programs):
        from datetime import datetime
        table.put_item(Item={
            'pk': f'ORG#{org_id}',
            'sk': 'PROGRAMS',
            'organization_id': org_id,
            'programs': list(programs),
            'updated_at': datetime.utcnow().isoformat() + 'Z',
        })
        # Clear the module-level cache so a subsequent load sees the write.
        try:
            import permissions as _perms
            _perms.invalidate_org_programs_cache(org_id)
        except ImportError:
            pass

    return _seed


@pytest.fixture
def unauthorized_event():
    """API Gateway event with no valid JWT claims."""
    return {
        'requestContext': {
            'authorizer': {
                'jwt': {
                    'claims': {}
                }
            }
        },
        'pathParameters': {},
        'body': None,
    }


# -----------------------------------------------------------------------------
# Bedrock Mock Fixture
# -----------------------------------------------------------------------------

@pytest.fixture
def mock_bedrock_response():
    """Default mock Bedrock Claude response for LLM validation."""
    return {
        'content': [{
            'type': 'text',
            'text': '```json\n{"status": "PASS", "reasoning": "Service date documented as 01/15/2024."}\n```'
        }]
    }


@pytest.fixture
def mock_bedrock_client(mocker, mock_bedrock_response):
    """
    Mock boto3 Bedrock client for LLM-based rule evaluation.

    Returns deterministic responses for:
    - Field extraction
    - Rule validation (PASS/FAIL/SKIP)
    """
    mock_client = mocker.MagicMock()
    mock_client.invoke_model.return_value = {
        'body': mocker.MagicMock(read=lambda: json.dumps(mock_bedrock_response).encode())
    }

    # Patch boto3.client to return our mock when 'bedrock-runtime' is requested
    original_client = boto3.client

    def patched_client(service_name, *args, **kwargs):
        if service_name == 'bedrock-runtime':
            return mock_client
        return original_client(service_name, *args, **kwargs)

    mocker.patch('boto3.client', side_effect=patched_client)
    return mock_client
