#!/usr/bin/env bash

# Script to set environment variables for all Lambda functions
# This loads variables from .env.local and sets them in AWS Lambda

set -e

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if .env.local exists
if [ ! -f ".env.local" ]; then
    print_error ".env.local file not found!"
    echo "Please create .env.local from .env.local.example"
    exit 1
fi

# Load environment variables from .env.local
print_info "Loading configuration from .env.local..."
source .env.local

# Lambda functions
LAMBDA_FUNCTIONS=(
    "process-raw-charts"
    "textract-result-handler"
    "rules-engine"
    "rules-engine-rag"
    "irp-processor"
)

# Function to set env vars for a specific function
set_env_vars() {
    local function_name=$1

    print_info "Setting environment variables for: $function_name"

    # Create a temporary file for the JSON
    local temp_file=$(mktemp)

    # Build the environment variables JSON based on function
    # Note: Must be wrapped in "Variables" key for AWS Lambda
    case "$function_name" in
        "process-raw-charts")
            cat > "$temp_file" <<EOF
{
  "Variables": {
    "BUCKET_NAME": "$BUCKET_NAME",
    "SNS_TOPIC_ARN": "$SNS_TOPIC_ARN",
    "SNS_ROLE_ARN": "$SNS_ROLE_ARN",
    "TEXTRACT_TO_BE_PROCESSED": "$TEXTRACT_TO_BE_PROCESSED",
    "TEXTRACT_IRP_FOLDER": "$TEXTRACT_IRP_FOLDER"
  }
}
EOF
            ;;
        "textract-result-handler")
            cat > "$temp_file" <<EOF
{
  "Variables": {
    "BUCKET_NAME": "$BUCKET_NAME",
    "TEXTRACT_PROCESSED": "$TEXTRACT_PROCESSED",
    "TEXTRACT_IRP_PROCESSED": "$TEXTRACT_IRP_PROCESSED"
  }
}
EOF
            ;;
        "rules-engine")
            cat > "$temp_file" <<EOF
{
  "Variables": {
    "BUCKET_NAME": "$BUCKET_NAME",
    "DYNAMODB_TABLE": "$DYNAMODB_TABLE",
    "DYNAMODB_IRP_TABLE": "$DYNAMODB_IRP_TABLE",
    "ORGANIZATION_ID": "$ORGANIZATION_ID",
    "TEXTRACT_PROCESSED": "$TEXTRACT_PROCESSED"
  }
}
EOF
            ;;
        "rules-engine-rag")
            cat > "$temp_file" <<EOF
{
  "Variables": {
    "BUCKET_NAME": "$BUCKET_NAME",
    "DYNAMODB_TABLE": "$DYNAMODB_TABLE",
    "DYNAMODB_IRP_TABLE": "$DYNAMODB_IRP_TABLE",
    "ORGANIZATION_ID": "$ORGANIZATION_ID",
    "TEXTRACT_PROCESSED": "$TEXTRACT_PROCESSED",
    "KNOWLEDGE_BASE_ID": "$KNOWLEDGE_BASE_ID"
  }
}
EOF
            ;;
        "irp-processor")
            cat > "$temp_file" <<EOF
{
  "Variables": {
    "BUCKET_NAME": "$BUCKET_NAME",
    "DYNAMODB_IRP_TABLE": "$DYNAMODB_IRP_TABLE",
    "ORGANIZATION_ID": "$ORGANIZATION_ID",
    "TEXTRACT_PROCESSED_IRP": "$TEXTRACT_PROCESSED_IRP"
  }
}
EOF
            ;;
        *)
            print_error "Unknown function: $function_name"
            rm -f "$temp_file"
            return 1
            ;;
    esac

    # Update Lambda environment variables using file input
    aws lambda update-function-configuration \
        --function-name "$function_name" \
        --environment file://"$temp_file" \
        --query 'FunctionArn' \
        --output text > /dev/null

    # Clean up temp file
    rm -f "$temp_file"

    print_success "Environment variables set for $function_name"
}

# Main script
echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║   Setting Lambda Environment Variables                ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

# Check if specific function was requested
if [ "$1" == "--function" ] || [ "$1" == "-f" ]; then
    if [ -z "$2" ]; then
        print_error "Please specify a function name"
        exit 1
    fi
    set_env_vars "$2"
else
    # Set env vars for all functions
    for function_name in "${LAMBDA_FUNCTIONS[@]}"; do
        set_env_vars "$function_name"
        echo ""
    done
fi

echo ""
print_success "✨ All environment variables updated successfully!"
echo ""
echo "Note: The Lambda functions will now use these environment variables as defaults."
echo "You can still override them at invoke-time using the event payload."
