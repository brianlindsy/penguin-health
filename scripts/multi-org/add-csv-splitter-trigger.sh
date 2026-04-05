#!/usr/bin/env bash

# Configure S3 trigger for CSV splitter Lambda on an organization's bucket
# This script adds an S3 event notification to trigger the CSV splitter
# when CSV files are uploaded to the uploaded-data-sftp/ folder

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Usage check
if [ -z "$1" ]; then
    echo "Usage: ./add-csv-splitter-trigger.sh <org-id>"
    echo ""
    echo "Example: ./add-csv-splitter-trigger.sh catholic-charities-multi-org"
    exit 1
fi

ORG_ID=$1
BUCKET_NAME="penguin-health-${ORG_ID}"
LAMBDA_NAME="penguin-health-csv-splitter-multi-org"
REGION="us-east-1"

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Adding CSV Splitter Trigger                          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Organization ID:${NC} $ORG_ID"
echo -e "${BLUE}S3 Bucket:${NC} $BUCKET_NAME"
echo -e "${BLUE}Lambda Function:${NC} $LAMBDA_NAME"
echo ""

# Check if Lambda exists
LAMBDA_ARN=$(aws lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.FunctionArn' --output text --region "$REGION" 2>/dev/null || echo "")

if [ -z "$LAMBDA_ARN" ]; then
    echo -e "${RED}Error: Lambda function $LAMBDA_NAME not found${NC}"
    echo "Please deploy the CSV splitter Lambda first."
    exit 1
fi

echo -e "${GREEN}✓${NC} Found Lambda: $LAMBDA_ARN"

# Check if bucket exists
if ! aws s3 ls "s3://$BUCKET_NAME" 2>/dev/null; then
    echo -e "${RED}Error: Bucket $BUCKET_NAME not found${NC}"
    echo "Please create the organization first using create-organization.sh"
    exit 1
fi

echo -e "${GREEN}✓${NC} Found bucket: $BUCKET_NAME"

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)

# Step 1: Add Lambda permission for S3 to invoke
echo ""
echo -e "${BLUE}Step 1: Adding Lambda permission for S3${NC}"

# Remove existing permission if present (ignore errors)
aws lambda remove-permission \
    --function-name "$LAMBDA_NAME" \
    --statement-id "s3-invoke-${ORG_ID}" \
    --region "$REGION" 2>/dev/null || true

# Add new permission
aws lambda add-permission \
    --function-name "$LAMBDA_NAME" \
    --statement-id "s3-invoke-${ORG_ID}" \
    --action "lambda:InvokeFunction" \
    --principal s3.amazonaws.com \
    --source-arn "arn:aws:s3:::${BUCKET_NAME}" \
    --source-account "$ACCOUNT_ID" \
    --region "$REGION"

echo -e "${GREEN}✓${NC} Lambda permission added"

# Step 2: Get existing notification configuration
echo ""
echo -e "${BLUE}Step 2: Configuring S3 event notification${NC}"

EXISTING_CONFIG=$(aws s3api get-bucket-notification-configuration --bucket "$BUCKET_NAME" 2>/dev/null || echo "{}")

# Create notification configuration using Python to handle JSON properly
python3 - <<EOF
import boto3
import json

bucket_name = '${BUCKET_NAME}'
lambda_arn = '${LAMBDA_ARN}'
org_id = '${ORG_ID}'

s3 = boto3.client('s3')

# Get existing config
try:
    existing = s3.get_bucket_notification_configuration(Bucket=bucket_name)
    # Remove ResponseMetadata if present
    existing.pop('ResponseMetadata', None)
except:
    existing = {}

# Get existing Lambda configurations
lambda_configs = existing.get('LambdaFunctionConfigurations', [])

# Remove any existing CSV splitter trigger for this bucket
lambda_configs = [c for c in lambda_configs if 'CsvSplitterTrigger' not in c.get('Id', '')]

# Add new CSV splitter trigger
lambda_configs.append({
    'Id': f'CsvSplitterTrigger-{org_id}',
    'LambdaFunctionArn': lambda_arn,
    'Events': ['s3:ObjectCreated:*'],
    'Filter': {
        'Key': {
            'FilterRules': [
                {'Name': 'prefix', 'Value': 'uploaded-data-sftp/'},
                {'Name': 'suffix', 'Value': '.csv'}
            ]
        }
    }
})

existing['LambdaFunctionConfigurations'] = lambda_configs

# Apply notification configuration
s3.put_bucket_notification_configuration(
    Bucket=bucket_name,
    NotificationConfiguration=existing
)

print(f"✓ S3 trigger configured: uploaded-data-sftp/*.csv -> {lambda_arn}")
EOF

# Step 3: Grant Lambda S3 permissions for this bucket
echo ""
echo -e "${BLUE}Step 3: Granting Lambda S3 permissions${NC}"

python3 - <<EOF
import boto3
import json

iam = boto3.client('iam')
lambda_client = boto3.client('lambda')
bucket_name = '${BUCKET_NAME}'
lambda_name = '${LAMBDA_NAME}'

# Get Lambda execution role
func = lambda_client.get_function(FunctionName=lambda_name)
role_arn = func['Configuration']['Role']
role_name = role_arn.split('/')[-1]

# Get current policy or create new one
try:
    response = iam.get_role_policy(RoleName=role_name, PolicyName='lambda-s3')
    policy = response['PolicyDocument']
except:
    policy = {
        'Version': '2012-10-17',
        'Statement': [{
            'Sid': 'S3Access',
            'Effect': 'Allow',
            'Action': ['s3:PutObject', 's3:GetObject', 's3:ListBucket', 's3:DeleteObject'],
            'Resource': []
        }]
    }

# Add new bucket ARNs if not present
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
print(f"✓ Lambda can now access s3://{bucket_name}")
EOF

# Summary
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   CSV Splitter Trigger Configured!                     ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Trigger Configuration:${NC}"
echo "  Bucket: s3://$BUCKET_NAME"
echo "  Prefix: uploaded-data-sftp/"
echo "  Suffix: .csv"
echo "  Lambda: $LAMBDA_NAME"
echo ""
echo -e "${BLUE}How it works:${NC}"
echo "1. Upload bulk CSV via SFTP to: s3://$BUCKET_NAME/uploaded-data-sftp/"
echo "2. Lambda automatically splits CSV into individual charts"
echo "3. Charts staged at: s3://$BUCKET_NAME/csv-staging/"
echo "4. Original CSV archived to: s3://$BUCKET_NAME/archived/sftp/"
echo ""
echo -e "${BLUE}Test the trigger:${NC}"
echo "  aws s3 cp test-bulk.csv s3://$BUCKET_NAME/uploaded-data-sftp/"
echo "  aws logs tail /aws/lambda/$LAMBDA_NAME --follow"
echo ""
