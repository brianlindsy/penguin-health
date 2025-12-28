# Lambda Configuration Guide

This document explains how to configure the Penguin Health Lambda functions with environment variables and runtime configuration.

## Configuration Files

### .env.local (Git-Ignored)

Contains your actual configuration values. **Never commit this file.**

```bash
# Copy the example to get started
cp .env.local.example .env.local

# Edit with your values
vim .env.local
```

### .env.local.example (Git-Tracked)

Template file showing all required configuration keys. Safe to commit.

## Setup Methods

### Method 1: Set AWS Lambda Environment Variables (Recommended)

Use the provided script to load configuration from `.env.local` into AWS Lambda:

```bash
# Set env vars for all functions
./set-lambda-env-vars.sh

# Set env vars for specific function
./set-lambda-env-vars.sh --function process-raw-charts
```

### Method 2: Use Event Payloads for Each Invocation

Pass configuration in the event payload when invoking:

```bash
aws lambda invoke \
  --function-name process-raw-charts \
  --payload file://test-events/process-raw-charts-event.json \
  response.json
```

## Configuration Pass-Through Architecture

### How It Works

1. **process-raw-charts** starts Textract job
2. Stores config in S3 metadata file: `textract-processing/{filename}-metadata.json`
3. Textract completes and sends SNS notification
4. **textract-result-handler** receives SNS event
5. Reads metadata file to get both:
   - Source file path
   - Original config from process-raw-charts
6. Merges configs: `metadata config + event config = final config`
7. Uses final config for processing

### Why This Matters

SNS notifications don't support custom payloads, so we can't pass config directly through SNS. Instead:

- Config is stored as metadata alongside the job info
- Retrieved when processing results
- Ensures consistency throughout the pipeline

## Per-Function Configuration

### process-raw-charts

**Required:**
- `BUCKET_NAME` - S3 bucket for PDFs
- `SNS_TOPIC_ARN` - SNS topic for Textract notifications
- `SNS_ROLE_ARN` - IAM role for Textract
- `TEXTRACT_TO_BE_PROCESSED` - S3 prefix for input PDFs
- `TEXTRACT_IRP_FOLDER` - S3 prefix for IRP PDFs

**Example Event:**
```json
{
  "config": {
    "BUCKET_NAME": "my-bucket",
    "SNS_TOPIC_ARN": "arn:aws:sns:...",
    "SNS_ROLE_ARN": "arn:aws:iam::...",
    "TEXTRACT_TO_BE_PROCESSED": "textract-to-be-processed/",
    "TEXTRACT_IRP_FOLDER": "textract-to-be-processed/irp/"
  }
}
```

### textract-result-handler

**Required:**
- `BUCKET_NAME` - S3 bucket for results
- `TEXTRACT_PROCESSED` - S3 prefix for processed JSONs
- `TEXTRACT_IRP_PROCESSED` - S3 prefix for IRP results

**Note:** Config is automatically retrieved from metadata stored by process-raw-charts. You rarely need to pass config in the event.

### rules-engine

**Required:**
- `BUCKET_NAME` - S3 bucket for validation rules and results
- `DYNAMODB_TABLE` - Table for validation results
- `DYNAMODB_IRP_TABLE` - Table for IRP data
- `ORGANIZATION_ID` - Organization identifier
- `TEXTRACT_PROCESSED` - S3 prefix for input JSONs

### irp-processor

**Required:**
- `BUCKET_NAME` - S3 bucket for IRP documents
- `DYNAMODB_IRP_TABLE` - Table for IRP data
- `ORGANIZATION_ID` - Organization identifier
- `TEXTRACT_PROCESSED_IRP` - S3 prefix for IRP JSONs

## Testing

### Test Events Directory

All test events are in [test-events/](test-events/):
- `process-raw-charts-event.json`
- `textract-result-handler-event.json`
- `rules-engine-event.json`
- `irp-processor-event.json`

See [test-events/README.md](test-events/README.md) for detailed usage instructions.