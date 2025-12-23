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
./deploy.sh
```

### Deploy Single Function
```bash
./deploy.sh --function rules-engine
# or use short form
./deploy.sh -f rules-engine
```

## Available Functions

- `process-raw-charts` - Initiates Textract analysis
- `textract-result-handler` - Processes Textract results
- `rules-engine` - Validates documents using LLM rules
- `irp-processor` - Processes IRP documents

## Deployment Script Features

✨ **Auto-versioning**: Uses git tags and commits for tracking
📦 **Smart packaging**: Creates ZIP packages with lambda code and config files
🏷️ **AWS tagging**: Tags functions with git version info
📊 **Progress tracking**: Shows deployment status for each function
🎯 **Selective deploy**: Deploy single function or all at once
⚙️ **Environment variables**: Automatically sets ENVIRONMENT, ORGANIZATION_ID, BUCKET_NAME, and DynamoDB table names

## Typical Workflow

### 1. Make Changes
```bash
# Edit your Lambda code
vim lambda/rules_engine.py

# Edit configuration
vim config/rules/catholic-charities.json
```

### 2. Test Locally (optional)
```bash
# Run your tests
python -m pytest tests/
```

### 3. Commit and Tag
```bash
# Stage changes
git add lambda/ config/

# Commit with descriptive message
git commit -m "Add debugging to LLM evaluation"

# Create semantic version tag
git tag -a v1.3.1 -m "Add LLM debugging logs"
```

### 4. Deploy
```bash
# Deploy all functions
./deploy.sh

# Or deploy specific function
./deploy.sh --function rules-engine
```

### 5. Verify
Check CloudWatch Logs or test with sample data to verify deployment.

## Version Tagging Convention

Use semantic versioning: `vMAJOR.MINOR.PATCH`

- **MAJOR**: Breaking changes (v2.0.0)
- **MINOR**: New features, backwards compatible (v1.3.0)
- **PATCH**: Bug fixes, backwards compatible (v1.3.1)

### Examples

```bash
# Bug fix
git tag -a v1.3.1 -m "Fix field extraction for empty values"

# New feature
git tag -a v1.4.0 -m "Add support for multi-page documents"

# Breaking change
git tag -a v2.0.0 -m "Remove text pattern fallback, forms only"
```

## Rollback

If you need to rollback to a previous version:

```bash
# Checkout previous tag
git checkout v1.2.0

# Deploy all functions
./deploy.sh

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

## Troubleshooting

### Deployment fails with "Function not found"
The Lambda function doesn't exist in AWS. Create it first:
```bash
# Create function using AWS CLI
aws lambda create-function \
  --function-name rules-engine \
  --runtime python3.12 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/lambda-execution-role \
  --handler rules_engine.lambda_handler \
  --zip-file fileb://lambda-package.zip
```

### Permission denied on deploy.sh
Make the script executable:
```bash
chmod +x deploy.sh
```

### Package too large
Lambda has a 50MB direct upload limit. The script automatically creates a ZIP package. If you need to reduce size:
- Remove unused dependencies
- Use Lambda layers for large libraries
- Or deploy via S3 (for packages > 50MB)

## Best Practices

1. **Always tag before deploying** - Makes version tracking easier
2. **Test in dev first** - Deploy to dev environment before prod
3. **Use descriptive commit messages** - Shows up in function description
4. **Monitor after deployment** - Check CloudWatch Logs for errors
5. **Keep tags in sync** - Push tags to remote: `git push --tags`
6. **Package cleanup** - The script automatically removes lambda-package.zip after deployment

## How It Works

The deployment script performs these steps:

1. **Packaging** - Creates a temporary build directory, copies lambda code and config files, creates ZIP package
2. **Code Update** - Uses `aws lambda update-function-code` to deploy the ZIP file
3. **Configuration** - Uses `aws lambda update-function-configuration` to set environment variables:
   - `ENVIRONMENT=dev`
   - `ORGANIZATION_ID=catholic-charities`
   - `BUCKET_NAME=penguin-health-catholic-charities`
   - `DYNAMODB_TABLE=penguin-health-validation-results-dev`
   - `DYNAMODB_IRP_TABLE=penguin-health-irp-dev`
4. **Tagging** - Adds git version tags to the Lambda function
5. **Cleanup** - Removes temporary files and packages

## CI/CD Integration (Future)

For automated deployments, consider:
- GitHub Actions on tag push
- AWS CodePipeline
- GitLab CI/CD

Example GitHub Action trigger:
```yaml
on:
  push:
    tags:
      - 'v*.*.*'
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to Lambda
        run: ./deploy.sh
```
