"""
Results Handler for validation results storage and reporting.

Handles:
- Storing validation results in DynamoDB
- Generating CSV reports from DynamoDB
- Saving CSV reports to S3
"""

import json
import csv
import io
from decimal import Decimal

import boto3

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')


def store_results(results, env_config):
    """
    Store validation results in DynamoDB.

    Args:
        results: Validation results dict from validate_document
        env_config: Environment config with DYNAMODB_TABLE
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        item = json.loads(json.dumps(results), parse_float=Decimal)

        item['pk'] = f"DOC#{results['document_id']}"
        item['sk'] = f"VALIDATION#{results['validation_timestamp']}"
        item['gsi1pk'] = f"DATE#{results['validation_timestamp'][:10]}"
        item['gsi1sk'] = f"DOC#{results['document_id']}"
        item['gsi2pk'] = f"RUN#{results['validation_run_id']}"
        item['gsi2sk'] = f"DOC#{results['document_id']}"
        item['organization_id'] = results.get('organization_id', 'unknown')

        table.put_item(Item=item)

        print(f"Stored results for document {results['document_id']} in DynamoDB (run: {results['validation_run_id']})")

    except Exception as e:
        print(f"Error storing results in DynamoDB: {str(e)}")


def generate_csv_from_dynamodb(validation_run_id, env_config):
    """
    Query all validation results for this run from DynamoDB and generate CSV.

    CSV format: One row per service_id with separate columns for each rule's status.

    Args:
        validation_run_id: ID of the validation run to query
        env_config: Environment config with DYNAMODB_TABLE

    Returns:
        str: CSV content as string
    """
    try:
        table = dynamodb.Table(env_config['DYNAMODB_TABLE'])

        response = table.query(
            IndexName='gsi2',
            KeyConditionExpression='gsi2pk = :run_key',
            ExpressionAttributeValues={
                ':run_key': f"RUN#{validation_run_id}"
            }
        )

        items = response.get('Items', [])
        print(f"Found {len(items)} documents for validation run {validation_run_id}")

        all_rule_names = set()
        for item in items:
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                all_rule_names.add(rule_name)

        sorted_rule_names = sorted(all_rule_names)

        output = io.StringIO()
        writer = csv.writer(output)
        header = ['Service ID', 'Consumer Name'] + sorted_rule_names
        writer.writerow(header)

        for item in items:
            field_values = item.get('field_values', {})
            service_id = field_values.get('document_id', 'N/A') if field_values else 'N/A'
            consumer_name = field_values.get('consumer_name', 'N/A') if field_values else 'N/A'

            rule_statuses = {}
            for rule in item.get('rules', []):
                rule_name = rule.get('rule_name', 'Unknown')
                status = rule.get('status', 'N/A')
                message = rule.get('message', '')

                if status == 'PASS':
                    rule_statuses[rule_name] = 'PASS'
                elif message and message != status:
                    if message.upper().startswith(status.upper()):
                        rule_statuses[rule_name] = message
                    else:
                        rule_statuses[rule_name] = f"{status}: {message}"
                else:
                    rule_statuses[rule_name] = status

            row = [service_id, consumer_name]
            for rule_name in sorted_rule_names:
                row.append(rule_statuses.get(rule_name, 'N/A'))

            writer.writerow(row)

        csv_content = output.getvalue()
        print(f"Generated CSV with {len(items)} rows (one per service_id) and {len(sorted_rule_names)} rule columns")
        return csv_content

    except Exception as e:
        print(f"Error generating CSV from DynamoDB: {str(e)}")
        raise e


def save_csv_to_s3(csv_content, validation_run_id, env_config):
    """
    Save CSV report to S3.

    Args:
        csv_content: CSV content as string
        validation_run_id: ID of the validation run
        env_config: Environment config with BUCKET_NAME
    """
    try:
        csv_key = f"validation-reports/{validation_run_id}-validation-report.csv"

        s3_client.put_object(
            Bucket=env_config['BUCKET_NAME'],
            Key=csv_key,
            Body=csv_content,
            ContentType='text/csv'
        )

        print(f"Saved CSV report to s3://{env_config['BUCKET_NAME']}/{csv_key}")

    except Exception as e:
        print(f"Error saving CSV to S3: {str(e)}")
        raise e
