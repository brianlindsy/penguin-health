# Lambda Invocation Guide

This document explains how to invoke the multi-organization Lambda functions after removing S3 event triggers.

## Overview

The Lambda functions have been simplified to accept `organization_id` as a required parameter in the event, removing dependency on S3 event notifications. This allows for more flexible invocation patterns (manual, scheduled, or via orchestration services like Step Functions or EventBridge).

---

## process-raw-charts-multi-org

Processes PDF files from the `textract-to-be-processed/` folder for a specific organization and starts Textract analysis.

### Event Structure

```json
{
  "organization_id": "community-health"
}
```

### Optional Configuration

```json
{
  "organization_id": "community-health",
  "config": {
    "textract_folder": "textract-to-be-processed/",
    "irp_folder": "textract-to-be-processed/irp/"
  }
}
```

### Response

```json
{
  "statusCode": 200,
  "organization_id": "community-health",
  "message": "Started processing 3 files",
  "processed_count": 3,
  "job_ids": [
    {
      "job_id": "abc123",
      "file_key": "textract-to-be-processed/chart1.pdf"
    },
    {
      "job_id": "def456",
      "file_key": "textract-to-be-processed/chart2.pdf"
    }
  ]
}
```

### AWS CLI Invocation

```bash
aws lambda invoke \
  --function-name process-raw-charts-multi-org \
  --payload '{"organization_id":"community-health"}' \
  response.json
```

### Python boto3 Invocation

```python
import boto3
import json

lambda_client = boto3.client('lambda')

response = lambda_client.invoke(
    FunctionName='process-raw-charts-multi-org',
    InvocationType='RequestResponse',
    Payload=json.dumps({
        'organization_id': 'community-health'
    })
)

result = json.loads(response['Payload'].read())
print(f"Processed {result['processed_count']} files")
```

---

## rules-engine-rag

Validates processed JSON files against organization-specific rules and generates CSV reports.

### Event Structure

```json
{
  "organization_id": "community-health"
}
```

### Optional Configuration

```json
{
  "organization_id": "community-health",
  "config": {
    "textract_processed": "textract-processed/"
  }
}
```

### Response

```json
{
  "statusCode": 200,
  "organization_id": "community-health",
  "message": "Validation completed successfully",
  "validation_run_id": "20260124-153022",
  "files_processed": 15
}
```

### AWS CLI Invocation

```bash
aws lambda invoke \
  --function-name rules-engine-rag \
  --payload '{"organization_id":"community-health"}' \
  response.json
```

### Python boto3 Invocation

```python
import boto3
import json

lambda_client = boto3.client('lambda')

response = lambda_client.invoke(
    FunctionName='rules-engine-rag',
    InvocationType='RequestResponse',
    Payload=json.dumps({
        'organization_id': 'community-health'
    })
)

result = json.loads(response['Payload'].read())
print(f"Validation run ID: {result['validation_run_id']}")
print(f"Files processed: {result['files_processed']}")
```

---

## Orchestration Patterns

### Option 1: EventBridge Scheduled Trigger

Process PDFs for an organization on a schedule (e.g., every hour):

```bash
# Create EventBridge rule
aws events put-rule \
  --name process-community-health-hourly \
  --schedule-expression "rate(1 hour)"

# Add Lambda target with constant JSON
aws events put-targets \
  --rule process-community-health-hourly \
  --targets '[{
    "Id": "1",
    "Arn": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:process-raw-charts-multi-org",
    "Input": "{\"organization_id\":\"community-health\"}"
  }]'
```

### Option 2: Step Functions Workflow

Create a workflow that processes PDFs, waits for Textract, then runs validation:

```json
{
  "Comment": "Multi-org document processing workflow",
  "StartAt": "ProcessPDFs",
  "States": {
    "ProcessPDFs": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:process-raw-charts-multi-org",
      "Parameters": {
        "organization_id.$": "$.organization_id"
      },
      "Next": "WaitForTextract"
    },
    "WaitForTextract": {
      "Type": "Wait",
      "Seconds": 300,
      "Next": "RunValidation"
    },
    "RunValidation": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:rules-engine-rag",
      "Parameters": {
        "organization_id.$": "$.organization_id"
      },
      "End": true
    }
  }
}
```

Start execution:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:DocumentProcessing \
  --input '{"organization_id":"community-health"}'
```

### Option 3: API Gateway Trigger

Create an API endpoint to trigger processing on demand:

```bash
# Create REST API
aws apigateway create-rest-api \
  --name document-processing-api

# Add Lambda integration for process-raw-charts-multi-org
# (Full API Gateway setup omitted for brevity)
```

Then trigger via HTTP:

```bash
curl -X POST https://api-id.execute-api.us-east-1.amazonaws.com/prod/process \
  -H "Content-Type: application/json" \
  -d '{"organization_id":"community-health"}'
```

---

## Multi-Organization Processing

### Process Multiple Organizations Sequentially

```python
import boto3
import json

lambda_client = boto3.client('lambda')

organizations = ['community-health', 'example-org', 'another-org']

for org_id in organizations:
    print(f"Processing {org_id}...")

    # Process PDFs
    response = lambda_client.invoke(
        FunctionName='process-raw-charts-multi-org',
        InvocationType='RequestResponse',
        Payload=json.dumps({'organization_id': org_id})
    )

    result = json.loads(response['Payload'].read())
    print(f"  - Started processing {result['processed_count']} files")
```

### Process Multiple Organizations in Parallel

```python
import boto3
import json
from concurrent.futures import ThreadPoolExecutor

lambda_client = boto3.client('lambda')

def process_organization(org_id):
    response = lambda_client.invoke(
        FunctionName='process-raw-charts-multi-org',
        InvocationType='Event',  # Async invocation
        Payload=json.dumps({'organization_id': org_id})
    )
    return org_id

organizations = ['community-health', 'example-org', 'another-org']

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(process_organization, org) for org in organizations]

    for future in futures:
        org_id = future.result()
        print(f"Triggered processing for {org_id}")
```

---

## Error Handling

Both Lambda functions will raise exceptions if required parameters are missing:

```python
# Missing organization_id
{
  "errorMessage": "Missing required parameter: organization_id",
  "errorType": "ValueError"
}
```

Handle errors in your invocation code:

```python
import boto3
import json

lambda_client = boto3.client('lambda')

try:
    response = lambda_client.invoke(
        FunctionName='process-raw-charts-multi-org',
        Payload=json.dumps({'organization_id': 'community-health'})
    )

    payload = json.loads(response['Payload'].read())

    # Check for function errors
    if 'FunctionError' in response:
        error_message = payload.get('errorMessage', 'Unknown error')
        print(f"Lambda error: {error_message}")
    else:
        print(f"Success: {payload['message']}")

except Exception as e:
    print(f"Invocation error: {str(e)}")
```

---

## Monitoring

### CloudWatch Logs

View logs for each Lambda:

```bash
# Process raw charts logs
aws logs tail /aws/lambda/process-raw-charts-multi-org --follow

# Rules engine logs
aws logs tail /aws/lambda/rules-engine-rag --follow
```

### CloudWatch Metrics

Monitor invocations and errors:

```bash
# Get invocation count
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=process-raw-charts-multi-org \
  --start-time 2026-01-24T00:00:00Z \
  --end-time 2026-01-24T23:59:59Z \
  --period 3600 \
  --statistics Sum

# Get error count
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Errors \
  --dimensions Name=FunctionName,Value=rules-engine-rag \
  --start-time 2026-01-24T00:00:00Z \
  --end-time 2026-01-24T23:59:59Z \
  --period 3600 \
  --statistics Sum
```

---

## Migration Notes

### Removed Features

The following features have been removed from both Lambda functions:

1. **S3 Event Triggers**: Lambdas no longer automatically trigger on S3 uploads
2. **Bucket Name Extraction**: No longer extracts org_id from S3 bucket names in events
3. **Batch Manifest Processing**: Removed from rules-engine-rag (simplified to process all files)
4. **Legacy Mode**: Removed multiple invocation paths for simplicity

### Benefits of New Approach

1. **Explicit Control**: You decide when to process each organization
2. **Flexible Scheduling**: Use EventBridge, Step Functions, or custom triggers
3. **Better Error Handling**: Easier to retry specific organizations
4. **Simplified Testing**: Invoke Lambdas directly with test data
5. **Cost Control**: Process only when needed, not on every S3 upload

### Required Changes

1. **Remove S3 Event Notifications**: Delete S3 bucket notification configurations
2. **Remove Lambda Permissions**: Remove S3 invoke permissions from Lambda functions
3. **Deploy Updated Code**: Deploy the new Lambda code
4. **Setup New Triggers**: Configure EventBridge rules or Step Functions as needed
