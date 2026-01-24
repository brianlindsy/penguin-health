# Multi-Organization Setup Guide

This guide covers the setup and management of the multi-organization validation system.

## Overview

The system supports unlimited organizations, each with:
- Dedicated S3 bucket for complete data isolation
- Organization-specific validation rules stored in DynamoDB
- Independent IRP (Individual Recovery Plan) configurations
- On-demand Lambda invocation (EventBridge, Step Functions, or manual)

## Architecture

### S3 Structure
```
s3://penguin-health-{org-id}/
├── charts/                     # Input documents
├── textract-to-be-processed/   # Queued for Textract
├── textract-processed/         # Textract JSON output
├── validation-reports/         # CSV validation reports
├── archived/                   # Archived processed files
└── irp/
    └── textract-processed/
```

### DynamoDB Table: `penguin-health-org-config`

Single table storing all organization configuration:

**Organization Metadata:**
- pk: `ORG#{org_id}`
- sk: `METADATA`

**Validation Rules:**
- pk: `ORG#{org_id}`
- sk: `RULE#{rule_id}`

**IRP Configuration:**
- pk: `ORG#{org_id}`
- sk: `IRP_CONFIG`

---

## Initial Setup

### Step 1: Create DynamoDB Table

```bash
./scripts/multi-org/create-org-config-table.sh
```

This creates the `penguin-health-org-config` table with:
- Primary key: pk (HASH), sk (RANGE)
- GSI1: For cross-org queries
- GSI2: For version-specific queries
- Billing mode: PAY_PER_REQUEST

### Step 2: Migrate Existing Configuration

```bash
python3 scripts/multi-org/migrate-config-to-dynamodb.py
```

This migrates existing organization configuration from JSON files to DynamoDB. Edit the script to specify your organization ID and name.

### Step 3: Deploy Updated Lambda

```bash
./scripts/deploy.sh --function rules-engine-rag
```

The Lambdas now accept organization_id as a parameter:
- Requires `organization_id` in event payload
- Loads configuration from DynamoDB
- Processes charts with org-specific rules
- See [LAMBDA_INVOCATION.md](LAMBDA_INVOCATION.md) for invocation examples

---

## Creating a New Organization

### Quick Start

```bash
./scripts/multi-org/create-organization.sh community-health "Community Health Services" admin@community-health.org
```

This single command:
1. ✓ Creates S3 bucket: `s3://penguin-health-community-health/`
2. ✓ Creates folder structure
3. ✓ Enables versioning and encryption
4. ✓ Creates organization record in DynamoDB
5. ✓ Creates default IRP configuration
6. ✓ Grants S3 write permissions to textract-result-handler-multi-org

### Manual Steps (if needed)

If you need to set up components individually:

**1. Create S3 Bucket:**
```bash
aws s3 mb s3://penguin-health-{org-id} --region us-east-1
```

**2. Create Organization Record:**
```python
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

table.put_item(Item={
    'pk': 'ORG#community-health',
    'sk': 'METADATA',
    'gsi1pk': 'ORG_METADATA',
    'gsi1sk': 'ORG#community-health',
    'organization_id': 'community-health',
    'organization_name': 'Community Health Services',
    'enabled': True,
    's3_bucket_name': 'penguin-health-community-health',
    'created_at': datetime.utcnow().isoformat() + 'Z'
})
```

---

## Managing Validation Rules

### Add a New Rule

```bash
./scripts/multi-org/add-rule.py community-health rule-config.json
```

**Example rule config** (`rule-config.json`):
```json
{
  "id": "101",
  "name": "Required Field Check",
  "category": "Compliance",
  "description": "Validates required fields are present",
  "enabled": true,
  "type": "llm",
  "version": "1.0.0",
  "llm_config": {
    "model_id": "openai.gpt-oss-120b-1:0",
    "use_rag": false,
    "system_prompt": "You are a compliance auditor...",
    "question": "Are all required fields present?"
  },
  "messages": {
    "pass": "PASS - All fields present",
    "fail": "FAIL - Missing fields: {llm_reasoning}",
    "skip": "SKIP - {llm_reasoning}"
  }
}
```

### List All Rules for an Organization

```bash
./scripts/multi-org/list-rules.py community-health
```

Output:
```
Found 5 rule(s) for community-health:

ID     Name                                               Category             Type       Enabled    RAG
------------------------------------------------------------------------------------------------------------------------
101    Required Field Check                               Compliance           llm        ✓ Yes
102    Treatment Protocol Validation                      Clinical Review      llm        ✓ Yes      ✓
103    Modality Check                                     Compliance           llm        ✓ Yes
```

### Update an Existing Rule

```bash
./scripts/multi-org/update-rule.py community-health 101 updated-rule-101.json
```

### Enable/Disable a Rule

Edit the rule JSON and set `"enabled": false`, then:

```bash
./scripts/multi-org/update-rule.py community-health 101 disabled-rule-101.json
```

---

## Managing Organizations

### List All Organizations

```bash
./scripts/multi-org/list-organizations.py
```

Output:
```
Found 2 organization(s):

ID                             Name                                     Enabled    S3 Bucket
------------------------------------------------------------------------------------------------------------------------
community-health               Community Health Services                ✓ Yes      penguin-health-community-health
example-org                    Example Organization                     ✓ Yes      penguin-health-example-org
```

### Disable an Organization

```python
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

table.update_item(
    Key={'pk': 'ORG#community-health', 'sk': 'METADATA'},
    UpdateExpression='SET enabled = :val',
    ExpressionAttributeValues={':val': False}
)
```

---

## Testing

### Upload a Test Document and Invoke Processing

```bash
# Upload PDF to S3
aws s3 cp test-chart.pdf s3://penguin-health-community-health/textract-to-be-processed/

# Invoke processing Lambda
aws lambda invoke \
  --function-name process-raw-charts-multi-org \
  --payload '{"organization_id":"community-health"}' \
  response.json

# Check response
cat response.json
```

For complete invocation examples including EventBridge, Step Functions, and multi-org batch processing, see [LAMBDA_INVOCATION.md](LAMBDA_INVOCATION.md).

### Monitor Lambda Processing

```bash
aws logs tail /aws/lambda/process-raw-charts-multi-org --follow
aws logs tail /aws/lambda/textract-result-handler-multi-org --follow
aws logs tail /aws/lambda/rules-engine-rag --follow
```

### Check Validation Results

```bash
aws dynamodb query \
  --table-name penguin-health-validation-results \
  --key-condition-expression "organization_id = :org" \
  --expression-attribute-values '{":org":{"S":"community-health"}}'
```

---

## RAG Configuration

### Enable RAG for a Rule

In your rule configuration, add:

```json
{
  "llm_config": {
    "use_rag": true,
    "knowledge_base_id": "OIX5H8GGRQ",
    "model_id": "anthropic.claude-3-sonnet-20240229-v1:0",
    "system_prompt": "Use clinical guidelines to validate...",
    "question": "Does treatment align with guidelines?"
  }
}
```

### Create Organization-Specific Knowledge Base

```bash
# Create Knowledge Base in AWS Console
# Then update org metadata:

import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

table.put_item(Item={
    'pk': 'ORG#community-health',
    'sk': 'KB#NEW_KB_ID_HERE',
    'gsi1pk': 'KNOWLEDGE_BASE',
    'gsi1sk': 'ORG#community-health#KB#NEW_KB_ID_HERE',
    'knowledge_base_id': 'NEW_KB_ID_HERE',
    'knowledge_base_name': 'Community Health Clinical Guidelines',
    'enabled': True
})
```

---

## Troubleshooting

### Organization Not Found

**Error:** `Organization 'xyz' not found in registry`

**Solution:**
```bash
./scripts/multi-org/list-organizations.py  # Verify org exists
```

If missing, recreate:
```bash
./scripts/multi-org/create-organization.sh xyz "XYZ Organization"
```

### No Rules Found

**Error:** `No rules found for organization: xyz`

**Solution:**
```bash
./scripts/multi-org/add-rule.py xyz rule-config.json
```

### Lambda Not Processing Files

**Check if Lambda was invoked:**
```bash
aws logs tail /aws/lambda/process-raw-charts-multi-org --follow
```

**Manually invoke Lambda:**
```bash
aws lambda invoke \
  --function-name process-raw-charts-multi-org \
  --payload '{"organization_id":"xyz"}' \
  response.json
```

See [LAMBDA_INVOCATION.md](LAMBDA_INVOCATION.md) for complete troubleshooting and invocation examples.

---

## Migration from Single-Org

If you have an existing single-org setup:

1. **Create table and migrate:**
```bash
./scripts/multi-org/create-org-config-table.sh
python3 scripts/multi-org/migrate-config-to-dynamodb.py
```

2. **Deploy updated Lambda:**
```bash
./scripts/deploy.sh --function rules-engine-rag
```

3. **Test existing org:**
```bash
# Upload test file (replace {org-id} with your organization ID)
aws s3 cp test.pdf s3://penguin-health-{org-id}/textract-to-be-processed/

# Invoke processing
aws lambda invoke \
  --function-name process-raw-charts-multi-org \
  --payload '{"organization_id":"{org-id}"}' \
  response.json

# Monitor
aws logs tail /aws/lambda/process-raw-charts-multi-org --follow
aws logs tail /aws/lambda/rules-engine-rag --follow
```

4. **Onboard new orgs:**
```bash
./scripts/multi-org/create-organization.sh new-org "New Organization"
```

---

## Best Practices

### Naming Conventions
- **Organization IDs:** lowercase-with-hyphens (e.g., `community-health`)
- **Rule IDs:** numeric (e.g., `101`, `102`)
- **S3 Buckets:** `penguin-health-{org-id}`

### Rule Versioning
- Update `version` field when changing rules
- Use semantic versioning: `1.0.0`, `1.1.0`, `2.0.0`
- Query specific versions via GSI2

### Security
- Each org has dedicated S3 bucket
- DynamoDB partition keys include org_id
- No cross-org data leakage possible
- Enable bucket encryption (done automatically)

### Cost Optimization
- Use PAY_PER_REQUEST billing for DynamoDB
- Archive old validation results to S3 Glacier
- Use S3 lifecycle policies to transition to cheaper storage

---

## Managing Chart Processing Configuration

### Overview

Chart processing configuration controls how Textract results are processed for each organization, including:
- **Encounter delimiter**: The field that marks the start of a new encounter (e.g., "Consumer Service ID:")
- **Encounter ID field**: The field to extract for filename generation
- **IRP folder pattern**: Pattern to detect IRP documents (default: "irp/")
- **Folder paths**: Customizable paths for raw data, archives, etc.

### Add Chart Config to Existing Organizations

```bash
# Add default config to a specific organization
./scripts/multi-org/add-chart-config.py community-health

# Add default config to all organizations
./scripts/multi-org/add-chart-config.py --all
```

### Update Chart Configuration

Create a JSON file with your custom configuration:

**Example: `custom-chart-config.json`**
```json
{
  "encounter_delimiter": "Client ID:",
  "encounter_id_field": "Client ID:",
  "version": "1.1.0"
}
```

Apply the configuration:
```bash
./scripts/multi-org/update-chart-config.py community-health custom-chart-config.json
```

### Chart Config Schema

**DynamoDB Structure**:
```json
{
  "pk": "ORG#example-org",
  "sk": "CHART_CONFIG",
  "organization_id": "example-org",
  "encounter_delimiter": "Consumer Service ID:",
  "encounter_id_field": "Consumer Service ID:",
  "irp_folder_pattern": "irp/",
  "folders": {
    "raw_charts": "textract-raw/",
    "raw_irp": "textract-raw/irp/",
    "archive_charts": "archived/textract/",
    "archive_irp": "archived/irp/textract/"
  },
  "version": "1.0.0"
}
```

### Example Configurations

**Default Configuration**:
```json
{
  "encounter_delimiter": "Consumer Service ID:",
  "encounter_id_field": "Consumer Service ID:"
}
```

**Organization with Different Field Names**:
```json
{
  "encounter_delimiter": "Patient Record ID:",
  "encounter_id_field": "Patient Record ID:"
}
```

**Organization with Custom Folder Structure**:
```json
{
  "encounter_delimiter": "Client ID:",
  "encounter_id_field": "Client ID:",
  "folders": {
    "raw_charts": "raw-documents/",
    "raw_irp": "raw-documents/recovery-plans/",
    "archive_charts": "archive/charts/",
    "archive_irp": "archive/recovery-plans/"
  }
}
```

### Multi-Org Processing Pipeline

The multi-org processing pipeline consists of three Lambda functions working together:

#### 1. process-raw-charts-multi-org
Invoked manually with organization_id parameter:
- Accepts `organization_id` in event payload
- Scans S3 bucket `penguin-health-{org-id}` for PDFs in `textract-to-be-processed/`
- Starts Textract async analysis with FORMS feature for each PDF
- Stores job metadata with organization_id for downstream processing

**Event Structure**:
```json
{
  "organization_id": "community-health"
}
```

**Deploy**:
```bash
./scripts/deploy.sh --function process-raw-charts-multi-org
```

#### 2. textract-result-handler-multi-org
Triggered by SNS when Textract completes analysis:
- Extracts organization ID from S3 bucket name
- Loads org-specific chart configuration from DynamoDB
- Uses configured delimiter to split multi-encounter documents
- Extracts encounter IDs using configured field names
- Stores processed files in configured folder paths

**Deploy**:
```bash
./scripts/deploy.sh --function textract-result-handler-multi-org
```

#### 3. rules-engine-rag
Invoked manually with organization_id parameter:
- Accepts `organization_id` in event payload
- Processes all JSON files in `textract-processed/` folder for that organization
- Loads org-specific validation rules from DynamoDB
- Executes LLM-based validation with optional RAG
- Generates consolidated CSV report with unique validation_run_id

**Event Structure**:
```json
{
  "organization_id": "community-health"
}
```

**Deploy**:
```bash
./scripts/deploy.sh --function rules-engine-rag
```

**Complete Pipeline Flow**:
```
1. Upload PDF → s3://penguin-health-{org-id}/textract-to-be-processed/chart.pdf

2. Invoke Lambda → process-raw-charts-multi-org
   aws lambda invoke --function-name process-raw-charts-multi-org \
     --payload '{"organization_id":"{org-id}"}' response.json

3. Textract → Analyzes document with FORMS feature (30s - 5min)

4. SNS → textract-result-handler-multi-org (automatic, triggered by Textract)
   - Splits encounters using org config
   - Uploads individual encounter JSON files to textract-processed/

5. Invoke Lambda → rules-engine-rag (after Textract completes)
   aws lambda invoke --function-name rules-engine-rag \
     --payload '{"organization_id":"{org-id}"}' response.json
   - Processes all encounters with same validation_run_id
   - Generates single consolidated CSV report

6. Results → s3://penguin-health-{org-id}/validation-reports/validation-{run_id}.csv
```

**For Automated Processing**:
See [LAMBDA_INVOCATION.md](LAMBDA_INVOCATION.md) for:
- EventBridge scheduled triggers
- Step Functions workflows
- Multi-organization batch processing
- API Gateway integration

**Note**: Original single-org Lambda functions (`process-raw-charts`, `textract-result-handler`) remain unchanged for backward compatibility.

---

## Scripts Reference

All multi-org scripts are located in `scripts/multi-org/`:

| Script | Purpose |
|--------|---------|
| `create-org-config-table.sh` | Create DynamoDB table |
| `migrate-config-to-dynamodb.py` | Migrate existing configs |
| `create-organization.sh` | Complete org setup (S3 bucket, DynamoDB, permissions) |
| `add-rule.py` | Add validation rule |
| `update-rule.py` | Update existing rule |
| `list-organizations.py` | List all organizations |
| `list-rules.py` | List rules for an org |
| `add-chart-config.py` | Add chart config to existing orgs |
| `update-chart-config.py` | Update chart processing settings |
| `import-rules-from-json.py` | Import rules from JSON file |

---

## Support

For issues or questions:
1. Check CloudWatch logs: `/aws/lambda/rules-engine-rag`
2. Verify DynamoDB records: `penguin-health-org-config`
3. Check S3 bucket permissions
4. Review Lambda IAM role permissions
