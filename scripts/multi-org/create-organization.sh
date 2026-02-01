#!/usr/bin/env bash

# Create new organization with complete infrastructure setup
# This script creates S3 bucket, DynamoDB records, and Lambda triggers

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Usage check
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./create-organization.sh <org-id> <org-name>"
    echo ""
    echo "Example: ./create-organization.sh community-health \"Community Health Services\""
    exit 1
fi

ORG_ID=$1
ORG_NAME=$2
BUCKET_NAME="penguin-health-${ORG_ID}"
REGION="us-east-1"

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Creating Organization: ${ORG_NAME}${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Organization ID:${NC} $ORG_ID"
echo -e "${BLUE}Organization Name:${NC} $ORG_NAME"
echo -e "${BLUE}S3 Bucket:${NC} $BUCKET_NAME"
echo ""

# Step 1: Create S3 bucket
echo -e "${BLUE}Step 1: Creating S3 Bucket${NC}"
echo ""

if aws s3 ls "s3://$BUCKET_NAME" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  Bucket already exists: $BUCKET_NAME${NC}"
    read -p "Continue with existing bucket? (y/N): " USE_EXISTING
    if [ "$USE_EXISTING" != "y" ]; then
        echo "Exiting."
        exit 1
    fi
else
    echo "Creating bucket: $BUCKET_NAME"
    aws s3 mb "s3://$BUCKET_NAME" --region "$REGION"
    echo -e "${GREEN}✓${NC} Created S3 bucket"
fi

# Step 2: Create folder structure
echo ""
echo -e "${BLUE}Step 2: Creating Folder Structure${NC}"
echo ""

FOLDERS=(
    "rag-documents/"
    "textract-to-be-processed/"
    "textract-processed/"
    "textract-processing/"
    "textract-raw/"
    "uploaded-data-sftp/"
    "archived/"
    "archived/textract/"
    "archived/validation/"
    "archived/irp/"
    "irp-config/"
    "validation-reports/"
)

for folder in "${FOLDERS[@]}"; do
    aws s3api put-object --bucket "$BUCKET_NAME" --key "$folder" > /dev/null
    echo "  ✓ Created $folder"
done

# Step 3: Enable versioning
echo ""
echo -e "${BLUE}Step 3: Enabling Bucket Versioning${NC}"
echo ""

aws s3api put-bucket-versioning \
  --bucket "$BUCKET_NAME" \
  --versioning-configuration Status=Enabled

echo -e "${GREEN}✓${NC} Enabled versioning"

# Step 4: Enable encryption
echo ""
echo -e "${BLUE}Step 4: Enabling Bucket Encryption${NC}"
echo ""

aws s3api put-bucket-encryption \
  --bucket "$BUCKET_NAME" \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      },
      "BucketKeyEnabled": false
    }]
  }'

echo -e "${GREEN}✓${NC} Enabled encryption (AES256)"

# Step 5: Create organization record in DynamoDB
echo ""
echo -e "${BLUE}Step 5: Creating Organization Record in DynamoDB${NC}"
echo ""

python3 - <<EOF
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

item = {
    'pk': 'ORG#${ORG_ID}',
    'sk': 'METADATA',
    'gsi1pk': 'ORG_METADATA',
    'gsi1sk': 'ORG#${ORG_ID}',
    'organization_id': '${ORG_ID}',
    'organization_name': '${ORG_NAME}',
    'display_name': '${ORG_NAME}',
    'enabled': True,
    's3_bucket_name': '${BUCKET_NAME}',
    'created_at': datetime.utcnow().isoformat() + 'Z',
    'updated_at': datetime.utcnow().isoformat() + 'Z'
}

table.put_item(Item=item)
print("✓ Organization record created")
EOF

# Step 6: Create default IRP config
echo ""
echo -e "${BLUE}Step 6: Creating Default IRP Configuration${NC}"
echo ""

python3 - <<EOF
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

table.put_item(Item={
    'pk': 'ORG#${ORG_ID}',
    'sk': 'IRP_CONFIG',
    'gsi1pk': 'IRP_CONFIG',
    'gsi1sk': 'ORG#${ORG_ID}',
    'organization_id': '${ORG_ID}',
    'field_mappings': {
        'consumer_name': 'Consumer Name:',
        'document_id': 'Consumer Service ID:'
    },
    'text_patterns': {},
    'version': '1.0.0'
})
print("✓ Default IRP config created")
EOF

# Step 6.5: Create default chart configuration
echo ""
echo -e "${BLUE}Step 6.5: Creating Default Chart Configuration${NC}"
echo ""

python3 - <<EOF
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

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
        'archive_irp': 'archived/irp/textract/'
    },
    'version': '1.0.0'
})
print("✓ Default chart config created")
EOF

# Step 7: Grant S3 permissions to multi-org Lambda functions
echo ""
echo -e "${BLUE}Step 7: Granting S3 Permissions to Multi-Org Lambda Functions${NC}"
echo ""

# List of Lambda functions that need S3 access to org buckets
MULTI_ORG_LAMBDAS=(
    "penguin-health-process-raw-charts-multi-org"
    "penguin-health-textract-result-handler-multi-org"
    "penguin-health-rules-engine-rag"
)

for LAMBDA_NAME in "${MULTI_ORG_LAMBDAS[@]}"; do
    # Get Lambda ARN
    LAMBDA_ARN=$(aws lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.FunctionArn' --output text --region "$REGION" 2>/dev/null || echo "")

    if [ -n "$LAMBDA_ARN" ]; then
        # Get Lambda execution role
        # Extract role name from ARN - get last segment after final '/'
        # ARN format can be: arn:aws:iam::ACCOUNT:role/ROLE_NAME or arn:aws:iam::ACCOUNT:role/service-role/ROLE_NAME
        ROLE_NAME=$(aws lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.Role' --output text --region "$REGION" | awk -F'/' '{print $NF}')

        echo "Updating IAM policy for $LAMBDA_NAME (role: $ROLE_NAME)"

        # Add new bucket to policy resources using Python
        python3 - <<EOF
import boto3
import json

iam = boto3.client('iam')
role_name = '${ROLE_NAME}'
bucket_name = '${BUCKET_NAME}'

# Get current policy
try:
    response = iam.get_role_policy(RoleName=role_name, PolicyName='lambda-s3')
    policy = response['PolicyDocument']
except:
    # Create new policy if doesn't exist
    policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'VisualEditor0',
            'Effect': 'Allow',
            'Action': ['s3:PutObject', 's3:GetObject', 's3:GetObjectTagging', 's3:ListBucket', 's3:PutObjectTagging', 's3:DeleteObject'],
            'Resource': []
        }]
    }

# Add new bucket ARNs if not already present
new_resources = [
    f'arn:aws:s3:::{bucket_name}',
    f'arn:aws:s3:::{bucket_name}/*'
]

for resource in new_resources:
    if resource not in policy['Statement'][0]['Resource']:
        policy['Statement'][0]['Resource'].append(resource)

# Update policy
iam.put_role_policy(
    RoleName=role_name,
    PolicyName='lambda-s3',
    PolicyDocument=json.dumps(policy)
)
print(f"  ✓ Granted S3 permissions for {bucket_name}")
EOF

        echo -e "  ${GREEN}✓${NC} $LAMBDA_NAME can now access ${BUCKET_NAME}"
    else
        echo -e "  ${YELLOW}⚠${NC} $LAMBDA_NAME not found (skip)"
    fi
done

# Summary
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Organization Created Successfully!                   ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Organization Details:${NC}"
echo "  ID: $ORG_ID"
echo "  Name: $ORG_NAME"
echo "  S3 Bucket: s3://$BUCKET_NAME"
echo "  DynamoDB: penguin-health-org-config"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Add validation rules:"
echo "   ./scripts/multi-org/add-rule.py $ORG_ID rule-config.json"
echo ""
echo "   Or import from existing config:"
echo "   ./scripts/multi-org/import-rules-from-json.py $ORG_ID config/rules/your-org.json"
echo ""
echo "2. List current rules:"
echo "   ./scripts/multi-org/list-rules.py $ORG_ID"
echo ""
echo "3. Upload PDFs to S3 and invoke Lambda manually:"
echo "   aws s3 cp test-chart.pdf s3://$BUCKET_NAME/textract-to-be-processed/"
echo ""
echo "   Then invoke the processing Lambda:"
echo "   aws lambda invoke --function-name penguin-health-process-raw-charts-multi-org \\"
echo "     --payload '{\"organization_id\":\"$ORG_ID\"}' response.json"
echo ""
echo "   See LAMBDA_INVOCATION.md for complete invocation examples including:"
echo "   - EventBridge scheduled triggers"
echo "   - Step Functions workflows"
echo "   - Multi-organization batch processing"
echo ""
echo "4. Monitor Lambda processing:"
echo "   aws logs tail /aws/lambda/penguin-health-process-raw-charts-multi-org --follow"
echo "   aws logs tail /aws/lambda/penguin-health-textract-result-handler-multi-org --follow"
echo "   aws logs tail /aws/lambda/penguin-health-rules-engine-rag --follow"
echo ""
