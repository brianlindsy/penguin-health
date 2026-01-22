#!/usr/bin/env bash

# Create DynamoDB table for multi-organization configuration
# This single table stores all org metadata, rules, and IRP configs

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Creating Multi-Org Configuration Table              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

TABLE_NAME="penguin-health-org-config"
REGION="us-east-1"

# Check if table already exists
if aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION" &> /dev/null; then
    echo -e "${YELLOW}⚠️  Table '$TABLE_NAME' already exists${NC}"
    read -p "Do you want to delete and recreate it? (y/N): " RECREATE

    if [ "$RECREATE" = "y" ]; then
        echo "Deleting existing table..."
        aws dynamodb delete-table --table-name "$TABLE_NAME" --region "$REGION"

        echo "Waiting for table to be deleted..."
        aws dynamodb wait table-not-exists --table-name "$TABLE_NAME" --region "$REGION"
        echo -e "${GREEN}✓${NC} Table deleted"
    else
        echo "Keeping existing table. Exiting."
        exit 0
    fi
fi

echo "Creating DynamoDB table: $TABLE_NAME"
echo ""

# Create table with GSIs
aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=gsi1pk,AttributeType=S \
    AttributeName=gsi1sk,AttributeType=S \
    AttributeName=gsi2pk,AttributeType=S \
    AttributeName=gsi2sk,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes \
    "[
      {
        \"IndexName\": \"GSI1\",
        \"KeySchema\": [
          {\"AttributeName\": \"gsi1pk\", \"KeyType\": \"HASH\"},
          {\"AttributeName\": \"gsi1sk\", \"KeyType\": \"RANGE\"}
        ],
        \"Projection\": {\"ProjectionType\": \"ALL\"}
      },
      {
        \"IndexName\": \"GSI2\",
        \"KeySchema\": [
          {\"AttributeName\": \"gsi2pk\", \"KeyType\": \"HASH\"},
          {\"AttributeName\": \"gsi2sk\", \"KeyType\": \"RANGE\"}
        ],
        \"Projection\": {\"ProjectionType\": \"ALL\"}
      }
    ]" \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION"

echo ""
echo "Waiting for table to be created..."
aws dynamodb wait table-exists --table-name "$TABLE_NAME" --region "$REGION"

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Table Created Successfully!                          ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Table Details:${NC}"
echo "  Name: $TABLE_NAME"
echo "  Region: $REGION"
echo "  Billing: PAY_PER_REQUEST"
echo ""
echo -e "${BLUE}Indexes:${NC}"
echo "  Primary: pk (HASH), sk (RANGE)"
echo "  GSI1: gsi1pk (HASH), gsi1sk (RANGE) - For cross-org queries"
echo "  GSI2: gsi2pk (HASH), gsi2sk (RANGE) - For version queries"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Run migration script to populate with existing data:"
echo "   python3 scripts/migrate-config-to-dynamodb.py"
echo ""
echo "2. Deploy updated rules-engine-rag Lambda"
echo ""
