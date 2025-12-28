# Penguin Health Lambda Deployment Guide

## Quick Start

### Deploy All Functions
```bash
# Tag your changes
git add .
git commit -m "Update rules engine to use forms only"
git tag -a v1.3.0 -m "Forms-only extraction with LLM rules"
git push origin v1.3.0

# Deploy all Lambda functions
./scripts/deploy.sh
```

### Deploy Single Function
```bash
./scripts/deploy.sh --function rules-engine
# or use short form
./scripts/deploy.sh -f rules-engine
```

## Available Functions

- `process-raw-charts` - Initiates Textract analysis
- `textract-result-handler` - Processes Textract results
- `rules-engine` - Validates documents using LLM rules
- `irp-processor` - Processes IRP documents

## Rollback

If you need to rollback to a previous version:

```bash
# Checkout previous tag
git checkout v1.2.0

# Deploy all functions
./scripts/deploy.sh

# Return to main branch
git checkout main
```

## Viewing Deployment History

### Git tags
```bash
# List all tags
git tag -l

# Show tag details
git show v1.3.0
```

### AWS Lambda versions
```bash
# View function info with tags
aws lambda get-function --function-name rules-engine

# List function tags
aws lambda list-tags --resource <function-arn>
```