#!/usr/bin/env bash

# Create a new organization with complete infrastructure setup.
#
# Bootstraps a per-org S3 bucket, seeds DynamoDB config records, grants
# multi-org Lambdas access to the new bucket, and optionally chains the
# RPA / FHIR / Stedi / CSV-splitter integration setup scripts.
#
# Re-runnable: each step checks for existing state before acting, so
# adding an integration to an already-bootstrapped org is just a re-run
# with the new flag.

set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${AWS_REGION:-us-east-1}"

usage() {
    cat <<EOF
Usage: ./create-organization.sh <org-id> <org-name> [options]

Required:
  <org-id>                  Lower-kebab org identifier, e.g. community-health
  <org-name>                Human-readable name, e.g. "Community Health Services"

Optional integrations (each takes a JSON config file; see examples below):
  --with-rpa <file.json>    Seed RPA_CONFIG. Requires Secrets Manager entry at
                            penguin-health/rpa/<org-id>/credentials and an existing
                            RPA_PLAYBOOK row (seed_rpa_playbook.py).
  --with-fhir <file.json>   Provision KMS keypair + JWKS, then seed FHIR_CONFIG and
                            FHIR_POLL_CURSOR. Requires a JwksHosting bucket/domain
                            from the CDK stack.
  --with-stedi <file.json>  Seed STEDI_CONFIG. Requires the shared Stedi API key at
                            penguin-health/stedi/api-key.
  --with-sftp               Enable SFTP CSV ingestion: wires the per-bucket S3
                            trigger that fires the CSV splitter Lambda when files
                            land in uploaded-data-sftp/. Only needed when the org
                            delivers data via SFTP.

Pre-flight validation:
  All integration prerequisites (secrets, KMS hosting bucket, helper scripts) are
  checked BEFORE any AWS writes. The script exits non-zero with a clear list if
  anything is missing, so you never end up with a half-provisioned org.

JSON config schemas:

  rpa.json:
    {
      "vendor": "centralreach",
      "display_name": "Community Health",
      "base_url": "https://members.centralreach.com",
      "bot_username": "ph-bot@community-health",
      "playbook_id": "credible-eligibility-v1",
      "timezone": "America/Chicago",
      "allowed_hours_start": "06:00",
      "allowed_hours_end": "20:00",
      "rate_limit_ms": 1500,
      "blackout_dates": "2026-12-25,2027-01-01",
      "cr_scope": null,
      "cr_sandbox": false,
      "disabled": false
    }

  fhir.json:
    {
      "vendor": "credible",
      "base_url": "https://fhir.example.com",
      "token_url": "https://login.example.com/oauth2/token",
      "client_id": "abc123",
      "source_column": "service_id_1",
      "jwks_bucket": "penguin-health-fhir-jwks",
      "jwks_domain": "jwks.penguin-health.example.com",
      "scopes": "",
      "page_size": 100,
      "concurrency": 4,
      "disabled": false
    }

  stedi.json:
    {
      "npi": "1234567890",
      "organization_name": "Community Health Services",
      "daily_cap": 500,
      "preferred_payers": "00001,00002",
      "demo_mode": false,
      "census_enabled": false,
      "encounter_filter_class_codes": "",
      "encounter_filter_type_codes": "",
      "encounter_filter_statuses": "",
      "cob_enabled": false,
      "disabled": false
    }

Examples:
  # Bare org, no integrations
  ./create-organization.sh community-health "Community Health Services"

  # Org with FHIR + Stedi
  ./create-organization.sh community-health "Community Health Services" \\
      --with-fhir ./fhir-community-health.json \\
      --with-stedi ./stedi-community-health.json

  # Add RPA later (re-run; bare-org steps are idempotent)
  ./create-organization.sh community-health "Community Health Services" \\
      --with-rpa ./rpa-community-health.json
EOF
}

if [ $# -lt 2 ]; then
    usage
    exit 1
fi

ORG_ID="$1"
ORG_NAME="$2"
shift 2

RPA_JSON=""
FHIR_JSON=""
STEDI_JSON=""
WITH_SFTP=0

while [ $# -gt 0 ]; do
    case "$1" in
        --with-rpa)
            RPA_JSON="${2:-}"
            [ -z "$RPA_JSON" ] && { echo "--with-rpa requires a JSON file path"; exit 1; }
            shift 2
            ;;
        --with-fhir)
            FHIR_JSON="${2:-}"
            [ -z "$FHIR_JSON" ] && { echo "--with-fhir requires a JSON file path"; exit 1; }
            shift 2
            ;;
        --with-stedi)
            STEDI_JSON="${2:-}"
            [ -z "$STEDI_JSON" ] && { echo "--with-stedi requires a JSON file path"; exit 1; }
            shift 2
            ;;
        --with-sftp)
            WITH_SFTP=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

BUCKET_NAME="penguin-health-${ORG_ID}"

# Track completed steps so a failure message can tell the operator what to resume.
COMPLETED_STEPS=()
record_done() { COMPLETED_STEPS+=("$1"); }
on_error() {
    local exit_code=$?
    echo ""
    echo -e "${RED}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║   FAILED                                               ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""
    if [ ${#COMPLETED_STEPS[@]} -gt 0 ]; then
        echo "The following steps DID succeed before the failure:"
        for step in "${COMPLETED_STEPS[@]}"; do
            echo "  ✓ $step"
        done
        echo ""
        echo "Re-run the script with the same args to resume — completed steps are idempotent."
    else
        echo "No steps completed before the failure."
    fi
    exit $exit_code
}
trap on_error ERR

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Creating Organization: ${ORG_NAME}${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Organization ID:${NC}   $ORG_ID"
echo -e "${BLUE}Organization Name:${NC} $ORG_NAME"
echo -e "${BLUE}S3 Bucket:${NC}         $BUCKET_NAME"
echo -e "${BLUE}Region:${NC}            $REGION"
echo -e "${BLUE}Integrations:${NC}      \
RPA=$([ -n "$RPA_JSON" ] && echo yes || echo no) \
FHIR=$([ -n "$FHIR_JSON" ] && echo yes || echo no) \
Stedi=$([ -n "$STEDI_JSON" ] && echo yes || echo no) \
SFTP=$([ "$WITH_SFTP" = 1 ] && echo yes || echo no)"
echo ""

# ----------------------------------------------------------------------
# Pre-flight: validate every prerequisite before any AWS write.
# ----------------------------------------------------------------------

echo -e "${BLUE}Pre-flight checks${NC}"

PREFLIGHT_ERRORS=()

require_file() {
    local path="$1"
    local label="$2"
    if [ ! -f "$path" ]; then
        PREFLIGHT_ERRORS+=("Missing $label: $path")
    fi
}

require_jq() {
    if ! command -v jq >/dev/null 2>&1; then
        PREFLIGHT_ERRORS+=("'jq' is required for parsing integration JSON files but not installed")
    fi
}

require_secret() {
    local secret_id="$1"
    local label="$2"
    if ! aws secretsmanager describe-secret --secret-id "$secret_id" --region "$REGION" >/dev/null 2>&1; then
        PREFLIGHT_ERRORS+=("Missing Secrets Manager entry for $label: $secret_id")
    fi
}

require_bucket_exists() {
    local bucket="$1"
    local label="$2"
    if ! aws s3api head-bucket --bucket "$bucket" --region "$REGION" >/dev/null 2>&1; then
        PREFLIGHT_ERRORS+=("Missing bucket for $label: s3://$bucket (provisioned by JwksHosting CDK construct)")
    fi
}

if [ "$WITH_SFTP" = 1 ]; then
    require_file "$SCRIPT_DIR/add-csv-splitter-trigger.sh" "add-csv-splitter-trigger.sh"
fi

if [ -n "$RPA_JSON" ]; then
    require_jq
    require_file "$RPA_JSON" "RPA config JSON"
    require_file "$SCRIPT_DIR/add_rpa_config.py" "add_rpa_config.py"
    require_secret "penguin-health/rpa/${ORG_ID}/credentials" "RPA credentials"
fi

if [ -n "$FHIR_JSON" ]; then
    require_jq
    require_file "$FHIR_JSON" "FHIR config JSON"
    require_file "$SCRIPT_DIR/add_fhir_config.py" "add_fhir_config.py"
    require_file "$SCRIPT_DIR/provision_fhir_keypair.py" "provision_fhir_keypair.py"
    if [ -f "$FHIR_JSON" ] && command -v jq >/dev/null 2>&1; then
        JWKS_BUCKET=$(jq -r '.jwks_bucket // empty' "$FHIR_JSON")
        [ -z "$JWKS_BUCKET" ] && PREFLIGHT_ERRORS+=("FHIR config missing required field: jwks_bucket")
        [ -n "$JWKS_BUCKET" ] && require_bucket_exists "$JWKS_BUCKET" "FHIR JWKS hosting"
        jq -r '.jwks_domain // empty' "$FHIR_JSON" | grep -q . \
            || PREFLIGHT_ERRORS+=("FHIR config missing required field: jwks_domain")
        for field in vendor base_url token_url client_id source_column; do
            jq -r ".${field} // empty" "$FHIR_JSON" | grep -q . \
                || PREFLIGHT_ERRORS+=("FHIR config missing required field: $field")
        done
    fi
fi

if [ -n "$STEDI_JSON" ]; then
    require_jq
    require_file "$STEDI_JSON" "Stedi config JSON"
    require_file "$SCRIPT_DIR/add_stedi_config.py" "add_stedi_config.py"
    require_secret "penguin-health/stedi/api-key" "shared Stedi API key"
    if [ -f "$STEDI_JSON" ] && command -v jq >/dev/null 2>&1; then
        for field in npi organization_name daily_cap; do
            jq -r ".${field} // empty" "$STEDI_JSON" | grep -q . \
                || PREFLIGHT_ERRORS+=("Stedi config missing required field: $field")
        done
    fi
fi

# CDK-provisioned DynamoDB table must already exist (otherwise this is a fresh
# stack and the operator should run cdk deploy first).
if ! aws dynamodb describe-table --table-name penguin-health-org-config --region "$REGION" >/dev/null 2>&1; then
    PREFLIGHT_ERRORS+=("DynamoDB table penguin-health-org-config not found — run 'cdk deploy' first")
fi

if [ ${#PREFLIGHT_ERRORS[@]} -gt 0 ]; then
    echo -e "${RED}Pre-flight failed:${NC}"
    for err in "${PREFLIGHT_ERRORS[@]}"; do
        echo -e "  ${RED}✗${NC} $err"
    done
    echo ""
    echo "No AWS writes performed. Fix the above and re-run."
    trap - ERR
    exit 1
fi
echo -e "${GREEN}✓${NC} All prerequisites satisfied"
echo ""

# ----------------------------------------------------------------------
# Step 1: S3 bucket
# ----------------------------------------------------------------------
echo -e "${BLUE}Step 1: S3 bucket${NC}"

if aws s3api head-bucket --bucket "$BUCKET_NAME" --region "$REGION" 2>/dev/null; then
    echo -e "  ${YELLOW}⚠${NC} Bucket already exists: $BUCKET_NAME (skipping create)"
else
    # us-east-1 doesn't accept LocationConstraint; other regions require it.
    if [ "$REGION" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" >/dev/null
    else
        aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" \
            --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
    fi
    echo -e "  ${GREEN}✓${NC} Created s3://$BUCKET_NAME"
fi
record_done "S3 bucket exists"

# ----------------------------------------------------------------------
# Step 2: Block Public Access (PHI bucket must never be public).
# ----------------------------------------------------------------------
echo -e "${BLUE}Step 2: Block Public Access${NC}"
aws s3api put-public-access-block --bucket "$BUCKET_NAME" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
echo -e "  ${GREEN}✓${NC} All four public-access flags set to true"
record_done "Block Public Access enforced"

# ----------------------------------------------------------------------
# Step 3: Folder structure
# ----------------------------------------------------------------------
echo -e "${BLUE}Step 3: Folder structure${NC}"

# Folders added unconditionally — cheap, and avoids a flag matrix.
# `data/`, `data/fhir/encounter/`, `analytics/fhir/encounter/` were added
# alongside RPA and FHIR encounter work but weren't in the original script.
FOLDERS=(
    "textract-to-be-processed/"
    "textract-processed/"
    "textract-processing/"
    "textract-raw/"
    "uploaded-data-sftp/"
    "csv-staging/"
    "archived/"
    "archived/textract/"
    "archived/validation/"
    "archived/csv/"
    "archived/sftp/"
    "archived/irp/"
    "validation-reports/"
    "data/"
    "data/fhir/encounter/"
    "analytics/fhir/encounter/"
)

for folder in "${FOLDERS[@]}"; do
    aws s3api put-object --bucket "$BUCKET_NAME" --key "$folder" >/dev/null
done
echo -e "  ${GREEN}✓${NC} Ensured ${#FOLDERS[@]} prefixes exist"
record_done "S3 folder structure"

# ----------------------------------------------------------------------
# Step 4: Versioning + encryption
# ----------------------------------------------------------------------
echo -e "${BLUE}Step 4: Versioning + encryption${NC}"

aws s3api put-bucket-versioning --bucket "$BUCKET_NAME" \
    --versioning-configuration Status=Enabled
echo -e "  ${GREEN}✓${NC} Versioning enabled"

# TODO: evaluate switching to SSE-KMS with a per-account CMK once a key
# management plan is in place. AES256 satisfies "encrypted at rest" today
# but KMS gives us per-object audit + per-bucket key isolation.
aws s3api put-bucket-encryption --bucket "$BUCKET_NAME" \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
            "BucketKeyEnabled": false
        }]
    }'
echo -e "  ${GREEN}✓${NC} AES256 default encryption"
record_done "Bucket versioning + encryption"

# ----------------------------------------------------------------------
# Step 5: METADATA + CHART_CONFIG DynamoDB records
# ----------------------------------------------------------------------
echo -e "${BLUE}Step 5: DynamoDB METADATA + CHART_CONFIG${NC}"

python3 - <<EOF
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb', region_name='${REGION}')
table = dynamodb.Table('penguin-health-org-config')
now = datetime.now(timezone.utc).isoformat()

# METADATA — notifications_enabled is read by email_sender.py and defaults
# True; setting it explicitly makes the contract visible in the DDB row.
table.put_item(Item={
    'pk': 'ORG#${ORG_ID}',
    'sk': 'METADATA',
    'gsi1pk': 'ORG_METADATA',
    'gsi1sk': 'ORG#${ORG_ID}',
    'organization_id': '${ORG_ID}',
    'organization_name': '${ORG_NAME}',
    'display_name': '${ORG_NAME}',
    'enabled': True,
    'notifications_enabled': True,
    's3_bucket_name': '${BUCKET_NAME}',
    'created_at': now,
    'updated_at': now,
})
print("  ✓ METADATA")

table.put_item(Item={
    'pk': 'ORG#${ORG_ID}',
    'sk': 'CHART_CONFIG',
    'gsi1pk': 'CHART_CONFIG',
    'gsi1sk': 'ORG#${ORG_ID}',
    'organization_id': '${ORG_ID}',
    'encounter_delimiter': 'Consumer Service ID:',
    'encounter_id_field': 'Consumer Service ID:',
    'irp_folder_pattern': 'irp/',
    'folders': {
        'raw_charts': 'textract-raw/',
        'raw_irp': 'textract-raw/irp/',
        'archive_charts': 'archived/textract/',
        'archive_irp': 'archived/irp/textract/',
    },
    'version': '1.0.0',
})
print("  ✓ CHART_CONFIG")
EOF
record_done "DynamoDB METADATA + CHART_CONFIG"

# ----------------------------------------------------------------------
# Step 6: Grant S3 access to multi-org Lambdas
# ----------------------------------------------------------------------
echo -e "${BLUE}Step 6: Grant S3 access to multi-org Lambdas${NC}"

# Base set — always need access to the org bucket. Integration-specific
# Lambdas (fhir-eligibility-poller) are appended conditionally below.
# The rules-engine runs as a Fargate task and already has a wildcard
# `penguin-health-*` grant on its task role, so it doesn't need per-org
# policy stitching here.
MULTI_ORG_LAMBDAS=(
    "penguin-health-process-raw-charts-multi-org"
    "penguin-health-textract-result-handler-multi-org"
    "penguin-health-csv-splitter-multi-org"
    "penguin-health-encounter-materializer"
)

if [ -n "$FHIR_JSON" ]; then
    MULTI_ORG_LAMBDAS+=("penguin-health-fhir-eligibility-poller")
fi

for LAMBDA_NAME in "${MULTI_ORG_LAMBDAS[@]}"; do
    LAMBDA_ARN=$(aws lambda get-function --function-name "$LAMBDA_NAME" \
        --query 'Configuration.FunctionArn' --output text --region "$REGION" 2>/dev/null || echo "")

    if [ -z "$LAMBDA_ARN" ]; then
        echo -e "  ${YELLOW}⚠${NC} $LAMBDA_NAME not found (skip)"
        continue
    fi

    ROLE_NAME=$(aws lambda get-function --function-name "$LAMBDA_NAME" \
        --query 'Configuration.Role' --output text --region "$REGION" | awk -F'/' '{print $NF}')

    python3 - <<EOF
import boto3, json

iam = boto3.client('iam')
role_name = '${ROLE_NAME}'
bucket_name = '${BUCKET_NAME}'

try:
    response = iam.get_role_policy(RoleName=role_name, PolicyName='lambda-s3')
    policy = response['PolicyDocument']
except iam.exceptions.NoSuchEntityException:
    policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'OrgBucketAccess',
            'Effect': 'Allow',
            'Action': [
                's3:PutObject', 's3:GetObject', 's3:GetObjectTagging',
                's3:ListBucket', 's3:PutObjectTagging', 's3:DeleteObject',
            ],
            'Resource': [],
        }],
    }

new_resources = [
    f'arn:aws:s3:::{bucket_name}',
    f'arn:aws:s3:::{bucket_name}/*',
]
for r in new_resources:
    if r not in policy['Statement'][0]['Resource']:
        policy['Statement'][0]['Resource'].append(r)

iam.put_role_policy(
    RoleName=role_name,
    PolicyName='lambda-s3',
    PolicyDocument=json.dumps(policy),
)
EOF
    echo -e "  ${GREEN}✓${NC} $LAMBDA_NAME"
done
record_done "Lambda S3 permissions"

# ----------------------------------------------------------------------
# Optional: SFTP ingestion (wires the CSV-splitter S3 trigger)
# ----------------------------------------------------------------------
if [ "$WITH_SFTP" = 1 ]; then
    echo -e "${BLUE}Step 7: SFTP CSV trigger${NC}"
    "$SCRIPT_DIR/add-csv-splitter-trigger.sh" "$ORG_ID"
    record_done "SFTP CSV trigger"
fi

# ----------------------------------------------------------------------
# Optional: RPA
# ----------------------------------------------------------------------
if [ -n "$RPA_JSON" ]; then
    echo -e "${BLUE}Step 8: RPA_CONFIG${NC}"

    # Translate JSON → flag list. jq null/empty → omit the flag entirely so the
    # helper's default kicks in.
    RPA_ARGS=(--org-id "$ORG_ID")
    add_flag() { local v; v=$(jq -r ".$2 // empty" "$RPA_JSON"); [ -n "$v" ] && RPA_ARGS+=("--$1" "$v"); }
    add_bool() { [ "$(jq -r ".$2 // false" "$RPA_JSON")" = "true" ] && RPA_ARGS+=("--$1"); }

    add_flag vendor                vendor
    add_flag display-name          display_name
    add_flag base-url              base_url
    add_flag bot-username          bot_username
    add_flag playbook-id           playbook_id
    add_flag timezone              timezone
    add_flag allowed-hours-start   allowed_hours_start
    add_flag allowed-hours-end     allowed_hours_end
    add_flag rate-limit-ms         rate_limit_ms
    add_flag blackout-dates        blackout_dates
    add_flag cr-scope              cr_scope
    add_bool cr-sandbox            cr_sandbox
    add_bool disabled              disabled
    RPA_ARGS+=(--region "$REGION")

    python3 "$SCRIPT_DIR/add_rpa_config.py" "${RPA_ARGS[@]}"
    record_done "RPA_CONFIG"
fi

# ----------------------------------------------------------------------
# Optional: FHIR
# ----------------------------------------------------------------------
if [ -n "$FHIR_JSON" ]; then
    echo -e "${BLUE}Step 9a: FHIR KMS keypair + JWKS${NC}"

    JWKS_BUCKET=$(jq -r '.jwks_bucket' "$FHIR_JSON")
    JWKS_DOMAIN=$(jq -r '.jwks_domain' "$FHIR_JSON")
    JWKS_URL="https://${JWKS_DOMAIN}/${ORG_ID}/jwks.json"

    # provision_fhir_keypair.py always creates a new key. Re-running rotates;
    # only execute if the alias doesn't yet exist.
    if aws kms describe-key --key-id "alias/penguin-health-fhir-${ORG_ID}" --region "$REGION" >/dev/null 2>&1; then
        echo -e "  ${YELLOW}⚠${NC} KMS alias alias/penguin-health-fhir-${ORG_ID} already exists (skip)"
    else
        python3 "$SCRIPT_DIR/provision_fhir_keypair.py" \
            --org-id "$ORG_ID" \
            --jwks-bucket "$JWKS_BUCKET" \
            --jwks-domain "$JWKS_DOMAIN" \
            --region "$REGION"
    fi
    record_done "FHIR KMS keypair + JWKS"

    echo -e "${BLUE}Step 9b: FHIR_CONFIG${NC}"

    FHIR_ARGS=(--org-id "$ORG_ID")
    add_flag() { local v; v=$(jq -r ".$2 // empty" "$FHIR_JSON"); [ -n "$v" ] && FHIR_ARGS+=("--$1" "$v"); }
    add_bool() { [ "$(jq -r ".$2 // false" "$FHIR_JSON")" = "true" ] && FHIR_ARGS+=("--$1"); }

    add_flag vendor          vendor
    add_flag base-url        base_url
    add_flag token-url       token_url
    add_flag client-id       client_id
    add_flag source-column   source_column
    add_flag scopes          scopes
    add_flag page-size       page_size
    add_flag concurrency     concurrency
    add_bool disabled        disabled
    FHIR_ARGS+=(--jwks-url "$JWKS_URL")
    FHIR_ARGS+=(--region "$REGION")

    python3 "$SCRIPT_DIR/add_fhir_config.py" "${FHIR_ARGS[@]}"
    record_done "FHIR_CONFIG"

    echo -e "${BLUE}Step 9c: FHIR_POLL_CURSOR seed${NC}"
    # Empty cursor row so the poller's first run doesn't hit a missing-item code path.
    python3 - <<EOF
import boto3
from datetime import datetime, timezone
table = boto3.resource('dynamodb', region_name='${REGION}').Table('penguin-health-org-config')
table.put_item(
    Item={
        'pk': 'ORG#${ORG_ID}',
        'sk': 'FHIR_POLL_CURSOR',
        'organization_id': '${ORG_ID}',
        'last_polled_at': None,
        'created_at': datetime.now(timezone.utc).isoformat(),
    },
    ConditionExpression='attribute_not_exists(pk)',
)
print("  ✓ FHIR_POLL_CURSOR seeded")
EOF
    record_done "FHIR_POLL_CURSOR"
fi

# ----------------------------------------------------------------------
# Optional: Stedi
# ----------------------------------------------------------------------
if [ -n "$STEDI_JSON" ]; then
    echo -e "${BLUE}Step 10: STEDI_CONFIG${NC}"

    STEDI_ARGS=(--org-id "$ORG_ID")
    add_flag() { local v; v=$(jq -r ".$2 // empty" "$STEDI_JSON"); [ -n "$v" ] && STEDI_ARGS+=("--$1" "$v"); }
    add_bool() { [ "$(jq -r ".$2 // false" "$STEDI_JSON")" = "true" ] && STEDI_ARGS+=("--$1"); }

    add_flag npi                            npi
    add_flag organization-name              organization_name
    add_flag daily-cap                      daily_cap
    add_flag preferred-payers               preferred_payers
    add_flag encounter-filter-class-codes   encounter_filter_class_codes
    add_flag encounter-filter-type-codes    encounter_filter_type_codes
    add_flag encounter-filter-statuses      encounter_filter_statuses
    add_bool disabled         disabled
    add_bool demo-mode        demo_mode
    add_bool census-enabled   census_enabled
    add_bool cob-enabled      cob_enabled
    STEDI_ARGS+=(--region "$REGION")

    python3 "$SCRIPT_DIR/add_stedi_config.py" "${STEDI_ARGS[@]}"
    record_done "STEDI_CONFIG"
fi

trap - ERR

# ----------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Organization Created Successfully                    ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Completed steps:${NC}"
for step in "${COMPLETED_STEPS[@]}"; do
    echo "  ✓ $step"
done
echo ""
echo -e "${BLUE}Organization:${NC}"
echo "  ID:        $ORG_ID"
echo "  Name:      $ORG_NAME"
echo "  S3:        s3://$BUCKET_NAME"
echo "  DynamoDB:  penguin-health-org-config (pk=ORG#${ORG_ID})"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Add validation rules:"
echo "     ./scripts/multi-org/import-rules-from-json.py $ORG_ID config/rules/your-org.json"
echo ""
echo "2. Smoke-test by uploading a chart:"
echo "     aws s3 cp test-chart.pdf s3://$BUCKET_NAME/textract-to-be-processed/"
echo "     aws lambda invoke --function-name penguin-health-process-raw-charts-multi-org \\"
echo "       --payload '{\"organization_id\":\"$ORG_ID\"}' response.json"
echo ""
if [ -n "$RPA_JSON" ]; then
    echo "3. RPA: enable the EventBridge schedule by adding $ORG_ID to _PER_ORG_SCHEDULES"
    echo "   in infra/components/rpa.py and redeploying."
    echo ""
fi
if [ -n "$FHIR_JSON" ]; then
    echo "4. FHIR: give the vendor this JWKS URL:"
    echo "     https://$(jq -r '.jwks_domain' "$FHIR_JSON")/${ORG_ID}/jwks.json"
    echo ""
fi
