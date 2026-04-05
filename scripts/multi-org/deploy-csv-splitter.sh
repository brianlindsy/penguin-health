#!/usr/bin/env bash

# Deploy CSV Splitter Lambda
# Creates the Lambda function if it doesn't exist, or updates if it does

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

LAMBDA_NAME="penguin-health-csv-splitter-multi-org"
REGION="us-east-1"
RUNTIME="python3.12"
HANDLER="csv_splitter_multi_org.lambda_handler"
TIMEOUT=60
MEMORY=256

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Deploying CSV Splitter Lambda                        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 1: Package the Lambda
echo -e "${BLUE}Step 1: Packaging Lambda code...${NC}"

BUILD_DIR=$(mktemp -d)
LAMBDA_DIR="$PROJECT_ROOT/lambda/multi-org/csv-splitter"
RULES_ENGINE_DIR="$PROJECT_ROOT/lambda/multi-org/rules-engine"

# Copy main handler
cp "$LAMBDA_DIR/csv_splitter_multi_org.py" "$BUILD_DIR/"

# Copy splitters module
mkdir -p "$BUILD_DIR/splitters"
cp "$LAMBDA_DIR/splitters/__init__.py" "$BUILD_DIR/splitters/"
cp "$LAMBDA_DIR/splitters/base_splitter.py" "$BUILD_DIR/splitters/"
cp "$LAMBDA_DIR/splitters/catholic_charities.py" "$BUILD_DIR/splitters/"
cp "$LAMBDA_DIR/splitters/circles_of_care.py" "$BUILD_DIR/splitters/"

# Copy shared config module
cp "$RULES_ENGINE_DIR/multi_org_config.py" "$BUILD_DIR/"

# Create zip
cd "$BUILD_DIR"
zip -r lambda-package.zip . -q
cd "$PROJECT_ROOT"
mv "$BUILD_DIR/lambda-package.zip" .

echo -e "${GREEN}✓${NC} Package created: lambda-package.zip ($(du -h lambda-package.zip | cut -f1))"

# Step 2: Check if Lambda exists
echo ""
echo -e "${BLUE}Step 2: Checking Lambda function...${NC}"

LAMBDA_EXISTS=$(aws lambda get-function --function-name "$LAMBDA_NAME" --region "$REGION" 2>/dev/null && echo "yes" || echo "no")

if [ "$LAMBDA_EXISTS" == "yes" ]; then
    echo -e "${GREEN}✓${NC} Lambda exists, updating code..."

    aws lambda update-function-code \
        --function-name "$LAMBDA_NAME" \
        --zip-file fileb://lambda-package.zip \
        --region "$REGION" \
        --query 'FunctionArn' \
        --output text > /dev/null

    echo -e "${GREEN}✓${NC} Lambda code updated"
else
    echo -e "${YELLOW}⚠${NC} Lambda does not exist, creating..."

    # Get or create IAM role
    ROLE_NAME="penguin-health-csv-splitter-role"
    ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || echo "")

    if [ -z "$ROLE_ARN" ]; then
        echo "Creating IAM role..."

        # Create trust policy
        cat > /tmp/trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF

        aws iam create-role \
            --role-name "$ROLE_NAME" \
            --assume-role-policy-document file:///tmp/trust-policy.json \
            --region "$REGION" > /dev/null

        # Attach basic Lambda execution policy
        aws iam attach-role-policy \
            --role-name "$ROLE_NAME" \
            --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

        # Create and attach S3 policy
        cat > /tmp/s3-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:CopyObject"
            ],
            "Resource": [
                "arn:aws:s3:::penguin-health-*",
                "arn:aws:s3:::penguin-health-*/*"
            ]
        }
    ]
}
EOF

        aws iam put-role-policy \
            --role-name "$ROLE_NAME" \
            --policy-name "lambda-s3" \
            --policy-document file:///tmp/s3-policy.json

        # Create and attach DynamoDB read policy
        cat > /tmp/dynamodb-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DynamoDBRead",
            "Effect": "Allow",
            "Action": [
                "dynamodb:GetItem",
                "dynamodb:Query"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/penguin-health-org-config",
                "arn:aws:dynamodb:*:*:table/penguin-health-org-config/index/*"
            ]
        }
    ]
}
EOF

        aws iam put-role-policy \
            --role-name "$ROLE_NAME" \
            --policy-name "lambda-dynamodb" \
            --policy-document file:///tmp/dynamodb-policy.json

        ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)

        echo -e "${GREEN}✓${NC} IAM role created: $ROLE_ARN"

        # Wait for role to propagate
        echo "Waiting for IAM role to propagate..."
        sleep 10
    fi

    # Create Lambda function
    aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime "$RUNTIME" \
        --role "$ROLE_ARN" \
        --handler "$HANDLER" \
        --zip-file fileb://lambda-package.zip \
        --timeout "$TIMEOUT" \
        --memory-size "$MEMORY" \
        --region "$REGION" \
        --query 'FunctionArn' \
        --output text > /dev/null

    echo -e "${GREEN}✓${NC} Lambda function created"
fi

# Clean up
rm -f lambda-package.zip
rm -rf "$BUILD_DIR"

# Step 3: Wait for Lambda to be ready
echo ""
echo -e "${BLUE}Step 3: Waiting for Lambda to be ready...${NC}"
aws lambda wait function-updated --function-name "$LAMBDA_NAME" --region "$REGION" 2>/dev/null || true
echo -e "${GREEN}✓${NC} Lambda is ready"

# Summary
LAMBDA_ARN=$(aws lambda get-function --function-name "$LAMBDA_NAME" --query 'Configuration.FunctionArn' --output text --region "$REGION")

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   CSV Splitter Lambda Deployed!                        ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Lambda Details:${NC}"
echo "  Name: $LAMBDA_NAME"
echo "  ARN: $LAMBDA_ARN"
echo "  Runtime: $RUNTIME"
echo "  Handler: $HANDLER"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Configure S3 trigger for an organization:"
echo "   ./scripts/multi-org/add-csv-splitter-trigger.sh <org-id>"
echo ""
echo "2. Configure CSV column mappings in Admin UI:"
echo "   Organization > Field Mappings > CSV Column Mappings"
echo ""
echo "3. Test by uploading a CSV file:"
echo "   aws s3 cp test.csv s3://penguin-health-<org-id>/uploaded-data-sftp/"
echo ""
