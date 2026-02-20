#!/bin/bash
set -euo pipefail

# Deploy admin UI frontend to S3 and invalidate CloudFront cache
#
# Usage:
#   ./scripts/admin-ui/deploy-frontend.sh
#
# Prerequisites:
#   - CDK stack must be deployed (cdk deploy from infra/)
#   - npm dependencies installed in admin-ui/

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FRONTEND_DIR="$PROJECT_ROOT/admin-ui"

BUCKET_NAME="penguin-health-admin-ui"
REGION="us-east-1"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}Building admin UI...${NC}"
cd "$FRONTEND_DIR"
npm run build

echo ""
echo -e "${BLUE}Syncing to S3...${NC}"
aws s3 sync dist/ "s3://$BUCKET_NAME/" \
  --delete \
  --region "$REGION"

echo ""
echo -e "${BLUE}Invalidating CloudFront cache...${NC}"

# Get distribution ID from CDK stack outputs
DIST_ID=$(aws cloudformation describe-stacks \
  --stack-name PenguinHealthAdminUi \
  --query "Stacks[0].Outputs[?OutputKey=='DistributionId'].OutputValue" \
  --output text \
  --region "$REGION" 2>/dev/null || echo "")

if [ -n "$DIST_ID" ]; then
  aws cloudfront create-invalidation \
    --distribution-id "$DIST_ID" \
    --paths "/*" \
    --region "$REGION" > /dev/null
  echo -e "${GREEN}CloudFront invalidation created${NC}"
else
  echo -e "${RED}Could not find CloudFront distribution ID. Skipping invalidation.${NC}"
fi

echo ""
echo -e "${GREEN}Deploy complete!${NC}"

# Print CloudFront URL
CF_URL=$(aws cloudformation describe-stacks \
  --stack-name PenguinHealthAdminUi \
  --query "Stacks[0].Outputs[?OutputKey=='CloudFrontUrl'].OutputValue" \
  --output text \
  --region "$REGION" 2>/dev/null || echo "")

if [ -n "$CF_URL" ]; then
  echo -e "URL: ${BLUE}${CF_URL}${NC}"
fi
