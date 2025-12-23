#!/usr/bin/env bash

# Penguin Health Lambda Deployment Script
# Usage:
#   ./deploy.sh                  # Deploy all Lambda functions to dev
#   ./deploy.sh --function process-raw-charts
#   ./deploy.sh --function textract-result-handler
#   ./deploy.sh --function rules-engine
#   ./deploy.sh --function irp-processor

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get git info
GIT_TAG=$(git describe --tags --always --dirty 2>/dev/null || echo "untagged")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DESCRIPTION="$GIT_TAG ($GIT_COMMIT) - $(git log -1 --pretty=%s 2>/dev/null | head -c 80)"

# Lambda functions list
LAMBDA_FUNCTIONS=(
    "process-raw-charts"
    "textract-result-handler"
    "rules-engine"
    "irp-processor"
)

# Get handler for a function name
get_handler() {
    local function_name=$1
    case "$function_name" in
        "process-raw-charts")
            echo "process_raw_charts.lambda_handler"
            ;;
        "textract-result-handler")
            echo "textract_result_handler.lambda_handler"
            ;;
        "rules-engine")
            echo "rules_engine.lambda_handler"
            ;;
        "irp-processor")
            echo "irp_processor.lambda_handler"
            ;;
        *)
            echo "unknown.lambda_handler"
            ;;
    esac
}

# Print colored message
print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Check if required tools are installed
check_prerequisites() {
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI not found. Please install it first."
        exit 1
    fi

    if ! command -v zip &> /dev/null; then
        print_error "zip command not found. Please install it first."
        exit 1
    fi
}

# Package Lambda function
package_lambda() {
    local function_name=$1
    local handler=$(get_handler "$function_name")
    local handler_file=$(echo "$handler" | cut -d'.' -f1)

    print_info "Packaging $function_name..."

    # Create temporary build directory
    BUILD_DIR=$(mktemp -d)

    # Copy only the specific handler file for this function
    if [ -f "lambda/${handler_file}.py" ]; then
        cp "lambda/${handler_file}.py" "$BUILD_DIR/"
        print_info "Packaged: ${handler_file}.py"
    else
        print_error "Handler file not found: lambda/${handler_file}.py"
        rm -rf "$BUILD_DIR"
        exit 1
    fi

    # Create deployment package
    cd "$BUILD_DIR"
    zip -r lambda-package.zip . -q
    cd - > /dev/null

    # Move package to project root
    mv "$BUILD_DIR/lambda-package.zip" .

    # Clean up
    rm -rf "$BUILD_DIR"

    print_success "Package created: lambda-package.zip ($(du -h lambda-package.zip | cut -f1))"
}

# Package all Lambda functions
package_all_lambdas() {
    print_info "Packaging all Lambda functions..."

    # Create temporary build directory
    BUILD_DIR=$(mktemp -d)

    # Copy all lambda code
    cp -r lambda/* "$BUILD_DIR/"

    # Create deployment package
    cd "$BUILD_DIR"
    zip -r lambda-package.zip . -q
    cd - > /dev/null

    # Move package to project root
    mv "$BUILD_DIR/lambda-package.zip" .

    # Clean up
    rm -rf "$BUILD_DIR"

    print_success "Package created: lambda-package.zip ($(du -h lambda-package.zip | cut -f1))"
}

# Deploy specific function using AWS CLI
deploy_function() {
    local function_name=$1
    local handler=$(get_handler "$function_name")

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_info "Deploying: $function_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Package the function
    package_lambda "$function_name"

    # Update function code
    print_info "Updating function code..."
    aws lambda update-function-code \
        --function-name "$function_name" \
        --zip-file fileb://lambda-package.zip \
        --query 'FunctionArn' \
        --output text > /dev/null

    # Wait for update to complete
    print_info "Waiting for update to complete..."
    aws lambda wait function-updated \
        --function-name "$function_name"

    # Update description
    print_info "Updating function description..."
    aws lambda update-function-configuration \
        --function-name "$function_name" \
        --description "$DESCRIPTION" \
        --query 'FunctionArn' \
        --output text > /dev/null

    # Tag the deployed function
    tag_function "$function_name"

    # Clean up package
    rm -f lambda-package.zip

    print_success "✨ Successfully deployed $function_name"
}

# Tag deployed function with git info
tag_function() {
    local function_name=$1

    print_info "Tagging $function_name with version info..."

    FUNCTION_ARN=$(aws lambda get-function \
        --function-name "$function_name" \
        --query 'Configuration.FunctionArn' \
        --output text 2>/dev/null)

    if [ -n "$FUNCTION_ARN" ]; then
        aws lambda tag-resource \
            --resource "$FUNCTION_ARN" \
            --tags GitTag="$GIT_TAG" GitCommit="$GIT_COMMIT" DeployedAt="$TIMESTAMP" \
            &> /dev/null || true

        print_success "Tagged with git version info"
    fi
}

# Deploy all functions
deploy_all() {
    echo ""
    echo "╔════════════════════════════════════════════════════════╗"
    echo "║      Deploying All Penguin Health Lambda Functions    ║"
    echo "╚════════════════════════════════════════════════════════╝"
    echo ""
    print_info "Git Tag: $GIT_TAG"
    print_info "Commit: $GIT_COMMIT"
    print_info "Timestamp: $TIMESTAMP"
    echo ""

    # Package all functions once
    package_all_lambdas

    # Deploy each function with the same package
    for function_name in "${LAMBDA_FUNCTIONS[@]}"; do
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        print_info "Deploying: $function_name"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Update function code
        print_info "Updating function code..."
        aws lambda update-function-code \
            --function-name "$function_name" \
            --zip-file fileb://lambda-package.zip \
            --query 'FunctionArn' \
            --output text > /dev/null

        # Wait for update to complete
        print_info "Waiting for update to complete..."
        aws lambda wait function-updated \
            --function-name "$function_name"

        # Update description
        print_info "Updating function description..."
        aws lambda update-function-configuration \
            --function-name "$function_name" \
            --description "$DESCRIPTION" \
            --query 'FunctionArn' \
            --output text > /dev/null

        # Tag the deployed function
        tag_function "$function_name"

        print_success "✨ Successfully deployed $function_name"
        echo ""
    done

    # Clean up package
    rm -f lambda-package.zip

    # Show deployment summary
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Deployment Summary"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""

    print_info "Deployed Functions:"
    for function_name in "${LAMBDA_FUNCTIONS[@]}"; do
        aws lambda get-function \
            --function-name "$function_name" \
            --query 'Configuration.{Name:FunctionName,Runtime:Runtime,Updated:LastModified}' \
            --output table 2>/dev/null || print_warning "  - $function_name (not found)"
    done

    echo ""
    print_success "🎉 All functions deployed successfully!"
}

# Main script logic
main() {
    # Check prerequisites
    check_prerequisites

    # Parse arguments
    DEPLOY_TARGET=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            --function|-f)
                DEPLOY_TARGET="$2"
                shift 2
                ;;
            --help|-h)
                echo "Penguin Health Lambda Deployment Script"
                echo ""
                echo "Usage:"
                echo "  $0                      # Deploy all functions"
                echo "  $0 --function <name>    # Deploy specific function"
                echo ""
                echo "Available functions:"
                for func in "${LAMBDA_FUNCTIONS[@]}"; do
                    echo "  - $func"
                done
                echo ""
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done

    # Display banner
    echo ""
    echo "╔════════════════════════════════════════════════════════╗"
    echo "║         Penguin Health Deployment Tool                ║"
    echo "╚════════════════════════════════════════════════════════╝"
    echo ""

    # Deploy
    if [ -z "$DEPLOY_TARGET" ]; then
        # Deploy all functions
        deploy_all
    else
        # Check if function is valid
        valid=false
        for func in "${LAMBDA_FUNCTIONS[@]}"; do
            if [ "$func" == "$DEPLOY_TARGET" ]; then
                valid=true
                break
            fi
        done

        if [ "$valid" == "true" ]; then
            deploy_function "$DEPLOY_TARGET"
        else
            print_error "Unknown function: $DEPLOY_TARGET"
            echo ""
            echo "Available functions:"
            for func in "${LAMBDA_FUNCTIONS[@]}"; do
                echo "  - $func"
            done
            exit 1
        fi
    fi
}

# Run main function
main "$@"
