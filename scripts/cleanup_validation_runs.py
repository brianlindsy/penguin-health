#!/usr/bin/env python3
"""
Script to delete all validation runs and associated document validations
for an organization, except for a specific validation run to keep.

Usage:
    python scripts/cleanup_validation_runs.py

Configuration is set in the script below.
"""

import boto3
from botocore.config import Config

# Configuration
ORG_ID = "catholic-charities-multi-org"
KEEP_RUN_ID = "20260422-145927"
TABLE_NAME = "penguin-health-validation-results"
DRY_RUN = False  # Set to False to actually delete

# Use us-east-1 region
config = Config(region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', config=config)
table = dynamodb.Table(TABLE_NAME)


def get_all_run_ids_for_org(org_id):
    """Get all validation run IDs for an organization."""
    run_ids = []

    response = table.query(
        KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
        ExpressionAttributeValues={
            ':pk': f"ORG#{org_id}",
            ':sk_prefix': 'RUN#'
        }
    )

    for item in response.get('Items', []):
        run_id = item.get('validation_run_id')
        if run_id:
            run_ids.append(run_id)

    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
            ExpressionAttributeValues={
                ':pk': f"ORG#{org_id}",
                ':sk_prefix': 'RUN#'
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        for item in response.get('Items', []):
            run_id = item.get('validation_run_id')
            if run_id:
                run_ids.append(run_id)

    return run_ids


def get_documents_for_run(run_id):
    """Get all document validation items for a validation run using GSI2."""
    items = []

    response = table.query(
        IndexName='gsi2',
        KeyConditionExpression='gsi2pk = :run_key',
        ExpressionAttributeValues={
            ':run_key': f"RUN#{run_id}"
        }
    )

    items.extend(response.get('Items', []))

    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='gsi2',
            KeyConditionExpression='gsi2pk = :run_key',
            ExpressionAttributeValues={
                ':run_key': f"RUN#{run_id}"
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))

    return items


def delete_items(items_to_delete):
    """Delete items using batch write."""
    if not items_to_delete:
        return 0

    deleted_count = 0

    # DynamoDB batch_write_item supports max 25 items per batch
    batch_size = 25
    for i in range(0, len(items_to_delete), batch_size):
        batch = items_to_delete[i:i + batch_size]

        delete_requests = [
            {'DeleteRequest': {'Key': {'pk': item['pk'], 'sk': item['sk']}}}
            for item in batch
        ]

        if DRY_RUN:
            print(f"  [DRY RUN] Would delete {len(batch)} items")
            deleted_count += len(batch)
        else:
            response = dynamodb.meta.client.batch_write_item(
                RequestItems={TABLE_NAME: delete_requests}
            )
            deleted_count += len(batch)

            # Handle unprocessed items
            unprocessed = response.get('UnprocessedItems', {}).get(TABLE_NAME, [])
            if unprocessed:
                print(f"  Warning: {len(unprocessed)} items were not processed")

    return deleted_count


def main():
    print(f"Cleanup Validation Runs for {ORG_ID}")
    print(f"Keeping run: {KEEP_RUN_ID}")
    print(f"Table: {TABLE_NAME}")
    print(f"DRY RUN: {DRY_RUN}")
    print("-" * 60)

    # Get all run IDs
    all_run_ids = get_all_run_ids_for_org(ORG_ID)
    print(f"Found {len(all_run_ids)} validation runs for {ORG_ID}")

    # Filter out the run to keep
    runs_to_delete = [r for r in all_run_ids if r != KEEP_RUN_ID]
    print(f"Runs to delete: {len(runs_to_delete)}")
    print(f"Run to keep: {KEEP_RUN_ID}")

    if KEEP_RUN_ID not in all_run_ids:
        print(f"\nWARNING: Run {KEEP_RUN_ID} not found in the list of runs!")
        print("Available runs:")
        for run_id in sorted(all_run_ids):
            print(f"  - {run_id}")

    print("-" * 60)

    total_docs_deleted = 0
    total_runs_deleted = 0

    for run_id in runs_to_delete:
        print(f"\nProcessing run: {run_id}")

        # Get all document validations for this run
        doc_items = get_documents_for_run(run_id)
        print(f"  Found {len(doc_items)} document validations")

        # Delete document validations
        if doc_items:
            deleted = delete_items(doc_items)
            total_docs_deleted += deleted
            print(f"  Deleted {deleted} document validations")

        # Delete the run summary item
        run_summary_item = {
            'pk': f"ORG#{ORG_ID}",
            'sk': f"RUN#{run_id}"
        }

        if DRY_RUN:
            print(f"  [DRY RUN] Would delete run summary: pk={run_summary_item['pk']}, sk={run_summary_item['sk']}")
        else:
            table.delete_item(Key=run_summary_item)
            print(f"  Deleted run summary")

        total_runs_deleted += 1

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Runs deleted: {total_runs_deleted}")
    print(f"  Document validations deleted: {total_docs_deleted}")

    if DRY_RUN:
        print("\n*** DRY RUN - No actual deletions were made ***")
        print("Set DRY_RUN = False to perform actual deletions")


if __name__ == "__main__":
    main()
