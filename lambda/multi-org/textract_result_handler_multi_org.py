import json
import boto3
from datetime import datetime
from multi_org_config import extract_org_id_from_bucket, load_chart_config

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

def lambda_handler(event, context):
    """
    Lambda function triggered by SNS when Textract job completes
    Retrieves results and stores them in the processed folder

    Multi-org version: Extracts bucket/org from Textract job results (not env vars)
    This allows a single Lambda to process jobs from multiple organizations
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            job_id = message.get('JobId')
            status = message.get('Status')
            api = message.get('API')

            print(f"Received notification for Job ID: {job_id}, Status: {status}, API: {api}")

            if status == 'SUCCEEDED':
                # Extract document location from SNS message
                # Textract includes DocumentLocation in the completion notification
                document_location = message.get('DocumentLocation', {})
                bucket_name = document_location.get('S3Bucket')
                source_file_key = document_location.get('S3ObjectName')

                if not bucket_name or not source_file_key:
                    print(f"Could not extract bucket/key from SNS message for job {job_id}")
                    print(f"Message contents: {json.dumps(message)}")
                    continue

                print(f"Processing job from bucket: {bucket_name}, file: {source_file_key}")

                # Extract organization ID from bucket name
                org_id = extract_org_id_from_bucket(bucket_name)
                print(f"Detected organization: {org_id}")

                # Build configuration for this organization
                config = {
                    'BUCKET_NAME': bucket_name,
                    'ORGANIZATION_ID': org_id
                }

                # Try to get additional config from stored metadata
                metadata_result = get_job_metadata(job_id, bucket_name)
                if metadata_result:
                    config.update(metadata_result.get('config', {}))

                # Get Textract results
                textract_results = get_textract_results(job_id)

                # Process and store results
                process_and_store_results(job_id, source_file_key, config, textract_results)

                # Clean up metadata file after successful processing
                if metadata_result and metadata_result.get('metadata_key'):
                    try:
                        s3_client.delete_object(Bucket=bucket_name, Key=metadata_result['metadata_key'])
                        print(f"Cleaned up metadata file: {metadata_result['metadata_key']}")
                    except Exception as e:
                        print(f"Warning: Could not delete metadata file: {str(e)}")

            elif status == 'FAILED':
                print(f"Textract job {job_id} failed")
                # Optionally handle failed jobs (e.g., move to error folder, send alert)

        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed Textract results')
        }

    except Exception as e:
        print(f"Error in result handler: {str(e)}")
        raise e


def get_job_metadata(job_id, bucket_name):
    """
    Retrieve job metadata from S3 (does not delete - caller handles cleanup)
    Returns dict with config and metadata_key, or None if not found
    """
    try:
        # List metadata files
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='textract-processing/'
        )

        if 'Contents' not in response:
            return None

        # Search for metadata file with matching job_id
        for obj in response['Contents']:
            if obj['Key'].endswith('-metadata.json'):
                metadata_obj = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=obj['Key']
                )
                metadata = json.loads(metadata_obj['Body'].read().decode('utf-8'))

                if metadata.get('job_id') == job_id:
                    print(f"Found metadata for job {job_id}: {obj['Key']}")
                    return {
                        'config': metadata.get('config', {}),
                        'metadata_key': obj['Key'],
                        'source_file': metadata.get('source_file'),
                        'organization_id': metadata.get('organization_id')
                    }

        print(f"No metadata found for job {job_id} (this is OK)")
        return None

    except Exception as e:
        print(f"Error retrieving metadata: {str(e)}")
        return None


def get_textract_results(job_id):
    """
    Retrieve results from completed Textract document analysis job
    Handles pagination for multi-page documents
    """
    try:
        # Get the document analysis results
        response = textract_client.get_document_analysis(JobId=job_id)

        # Handle pagination if document has multiple pages
        pages = [response]
        next_token = response.get('NextToken')

        while next_token:
            response = textract_client.get_document_analysis(
                JobId=job_id,
                NextToken=next_token
            )
            pages.append(response)
            next_token = response.get('NextToken')

        # Combine all blocks from all pages
        all_blocks = []
        for page in pages:
            all_blocks.extend(page.get('Blocks', []))

        return {
            'Blocks': all_blocks,
            'JobStatus': pages[0]['JobStatus'],
            'DocumentMetadata': pages[0].get('DocumentMetadata', {})
        }

    except Exception as e:
        print(f"Error retrieving Textract results: {str(e)}")
        raise e


def process_and_store_results(job_id, source_file_key, config, textract_response=None):
    """
    Process Textract results and store in processed folder
    Handles multi-encounter documents by splitting them into separate files
    Multi-org version: Loads organization-specific chart processing config
    """
    try:
        # Extract organization ID from bucket name and load chart config
        org_id = extract_org_id_from_bucket(config['BUCKET_NAME'])
        chart_config = load_chart_config(org_id)
        print(f"Processing for organization: {org_id}")

        # Get Textract results if not already provided
        if not textract_response:
            textract_response = get_textract_results(job_id)

        if textract_response['JobStatus'] != 'SUCCEEDED':
            print(f"Textract job {job_id} status: {textract_response['JobStatus']}")
            return False

        # Determine if this is an IRP document based on source folder
        # Use configurable pattern from chart_config
        irp_pattern = chart_config.get('irp_folder_pattern', 'irp/')
        is_irp = irp_pattern in source_file_key

        # Use configurable folder paths from chart_config
        folders = chart_config.get('folders', {})

        # Get processed folder for triggering validation Lambda
        processed_folder = config.get('TEXTRACT_IRP_PROCESSED', 'textract-processed/irp/') if is_irp else config.get('TEXTRACT_PROCESSED', 'textract-processed/')

        # Archive folder for storing original PDFs
        archive_folder = folders.get('archive_irp', 'archived/irp/textract/') if is_irp else folders.get('archive_charts', 'archived/textract/')

        # Generate base file name
        filename = source_file_key.split('/')[-1].replace('.pdf', '')
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

        # Save raw Textract response to textract-raw folder
        raw_folder = folders.get('raw_irp', 'textract-raw/irp/') if is_irp else folders.get('raw_charts', 'textract-raw/')
        raw_response_key = f"{raw_folder}{filename}-{timestamp}.json"
        s3_client.put_object(
            Bucket=config['BUCKET_NAME'],
            Key=raw_response_key,
            Body=json.dumps(textract_response, indent=2),
            ContentType='application/json'
        )
        print(f"Saved raw Textract response to: {raw_response_key}")

        # Extract structured data
        extracted_data = process_textract_response(textract_response)

        # Split into encounters if this is a multi-encounter document
        # Pass chart_config for org-specific delimiter configuration
        encounters = split_into_encounters(extracted_data, is_irp, chart_config)

        # Save each encounter as a separate file
        encounter_files = []  # Track all uploaded encounter files for batch manifest
        batch_id = f"{filename}-{timestamp}"

        if len(encounters) > 1:
            print(f"Split document into {len(encounters)} encounters")
            # Get configurable encounter ID field
            encounter_id_field = chart_config.get('encounter_id_field', 'Consumer Service ID:')

            for idx, encounter in enumerate(encounters):
                # Extract encounter ID from the encounter text using org-specific field
                encounter_id = f'encounter-{idx+1}'
                encounter_text = encounter.get('text', '')
                for line in encounter_text.split('\n'):
                    if encounter_id_field in line:
                        # Extract the ID value after the delimiter
                        parts = line.split(encounter_id_field, 1)
                        if len(parts) > 1:
                            encounter_id = parts[1].strip().split()[0]  # Get first word after delimiter
                        break

                # Sanitize encounter_id for filename
                safe_encounter_id = encounter_id.replace('/', '-').replace(' ', '-')
                output_key = f"{processed_folder}{filename}-{safe_encounter_id}-{timestamp}.json"

                s3_client.put_object(
                    Bucket=config['BUCKET_NAME'],
                    Key=output_key,
                    Body=json.dumps(encounter, indent=2),
                    ContentType='application/json'
                )
                encounter_files.append(output_key)
                print(f"Successfully processed and saved encounter {idx+1} to: {output_key}")
        else:
            # Single encounter, save as before
            output_key = f"{processed_folder}{filename}-{timestamp}.json"
            s3_client.put_object(
                Bucket=config['BUCKET_NAME'],
                Key=output_key,
                Body=json.dumps(encounters[0] if encounters else extracted_data, indent=2),
                ContentType='application/json'
            )
            encounter_files.append(output_key)
            print(f"Successfully processed and saved to: {output_key}")

        # Create batch manifest file to trigger validation
        # This ensures all encounters are validated together with a single validation_run_id
        manifest = {
            'batch_id': batch_id,
            'source_pdf': source_file_key,
            'organization_id': org_id,
            'encounter_count': len(encounters),
            'encounter_files': encounter_files,
            'created_at': datetime.utcnow().isoformat(),
            'is_batch_manifest': True
        }

        manifest_key = f"{processed_folder}{filename}-{timestamp}-batch-complete.json"
        s3_client.put_object(
            Bucket=config['BUCKET_NAME'],
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2),
            ContentType='application/json'
        )
        print(f"Created batch manifest: {manifest_key} ({len(encounters)} encounters) - triggers validation")

        # Move the original file to an archive folder
        archive_key = f"{archive_folder}{filename}-{timestamp}.pdf"
        s3_client.copy_object(
            Bucket=config['BUCKET_NAME'],
            CopySource={'Bucket': config['BUCKET_NAME'], 'Key': source_file_key},
            Key=archive_key
        )

        # Delete from to-be-processed folder
        s3_client.delete_object(Bucket=config['BUCKET_NAME'], Key=source_file_key)

        print(f"Archived original file to: {archive_key}")

        return True

    except Exception as e:
        print(f"Error processing results for job {job_id}: {str(e)}")
        raise e


def split_into_encounters_text_based(extracted_data, delimiter_field='Consumer Service ID:'):
    """
    Split encounters by finding delimiters in LINE blocks, use FORMS for content.
    - LINE blocks (lines_text): Used ONLY for delimiter detection (finds all 63)
    - FORMS (forms_text): Used for actual encounter content (proper formatting)
    """
    lines_text = extracted_data.get('lines_text', '')
    forms_text = extracted_data.get('text', '')

    # ALWAYS use lines_text to find ALL delimiter positions
    line_text_lines = lines_text.split('\n')
    line_delimiter_indices = [idx for idx, line in enumerate(line_text_lines)
                              if delimiter_field in line]

    print(f"Found {len(line_delimiter_indices)} delimiters in lines_text (LINE blocks)")

    # Use forms_text for content if available, otherwise fallback to lines_text
    if forms_text:
        content_text = forms_text
        content_lines = forms_text.split('\n')

        # Find delimiter positions in forms_text for splitting
        forms_delimiter_indices = [idx for idx, line in enumerate(content_lines)
                                   if delimiter_field in line]

        print(f"Found {len(forms_delimiter_indices)} delimiters in forms_text (FORMS)")

        # Use forms delimiters for splitting (should match line count)
        delimiter_indices = forms_delimiter_indices
    else:
        # No FORMS available, use LINE blocks for everything
        print("No FORMS detected, using LINE blocks for content")
        content_text = lines_text
        content_lines = line_text_lines
        delimiter_indices = line_delimiter_indices

    # If no delimiters or only one, return whole document as single encounter
    if len(delimiter_indices) <= 1:
        return [{
            'text': content_text,
            'metadata': {
                **extracted_data.get('metadata', {}),
                'encounter_index': 0,
                'is_split_encounter': False,
                'line_count': len(content_lines)
            }
        }]

    # Create encounters based on delimiter positions in content_text
    encounters = []
    for i in range(len(delimiter_indices)):
        start_idx = delimiter_indices[i]
        end_idx = delimiter_indices[i + 1] if i + 1 < len(delimiter_indices) else len(content_lines)

        # Get text lines for this encounter from properly formatted content
        encounter_lines = content_lines[start_idx:end_idx]
        encounter_text = '\n'.join(encounter_lines)

        encounter_data = {
            'text': encounter_text,
            'metadata': {
                **extracted_data.get('metadata', {}),
                'encounter_index': i,
                'is_split_encounter': True,
                'line_count': len(encounter_lines),
                'start_line': start_idx,
                'end_line': end_idx,
                'total_delimiters_in_line_blocks': len(line_delimiter_indices)
            }
        }

        encounters.append(encounter_data)
        print(f"Encounter {i}: {len(encounter_lines)} lines")

    return encounters


def split_into_encounters(extracted_data, is_irp=False, chart_config=None):
    """
    Split a multi-encounter document into separate encounters

    For regular charts: Split by org-specific delimiter from chart_config
    For IRP documents: Return as single encounter

    Args:
        extracted_data: The extracted Textract data
        is_irp: Boolean indicating if this is an IRP document
        chart_config: Organization-specific chart processing configuration
    """
    if is_irp:
        # IRP documents are typically single encounter
        return [{
            'text': extracted_data['text'],
            'metadata': {
                **extracted_data.get('metadata', {}),
                'encounter_index': 0,
                'is_split_encounter': False
            }
        }]

    # Get delimiter from chart_config, with fallback to default
    if chart_config:
        delimiter = chart_config.get('encounter_delimiter', 'Consumer Service ID:')
    else:
        delimiter = 'Consumer Service ID:'

    print(f"Using encounter delimiter: '{delimiter}'")
    return split_into_encounters_text_based(extracted_data, delimiter)


def process_textract_response(response):
    """
    Extract text content from both LINE blocks and FORMS
    - LINE blocks: Complete text for encounter splitting
    - FORMS: Structured key-value pairs for better formatting
    """
    extracted_data = {
        'text': '',
        'lines': [],
        'metadata': {
            'document_pages': response.get('DocumentMetadata', {}).get('Pages', 0),
            'extraction_timestamp': datetime.utcnow().isoformat(),
            'job_status': response.get('JobStatus')
        }
    }

    blocks_map = {}
    for block in response['Blocks']:
        blocks_map[block['Id']] = block

    # Extract key-value pairs from FORMS for structured data
    form_pairs = []
    for block in response['Blocks']:
        if block['BlockType'] == 'KEY_VALUE_SET' and block.get('EntityTypes') and 'KEY' in block['EntityTypes']:
            key_text = get_text_from_block(block, blocks_map)
            value_block = get_value_block(block, blocks_map)
            value_text = get_text_from_block(value_block, blocks_map) if value_block else ''

            if key_text:
                page = block.get('Page', 1)
                y_pos = block.get('Geometry', {}).get('BoundingBox', {}).get('Top', 0)
                form_pairs.append({
                    'key': key_text,
                    'value': value_text,
                    'page': page,
                    'y_position': y_pos
                })

    # Sort by page and Y-position
    form_pairs.sort(key=lambda x: (x['page'], x['y_position']))

    # Build formatted text from forms (key: value format) if available
    if form_pairs:
        text_lines = []
        for pair in form_pairs:
            if pair['value']:
                text_lines.append(f"{pair['key']} {pair['value']}")
            else:
                text_lines.append(pair['key'])
        forms_text = '\n'.join(text_lines)
        print(f"Extracted {len(form_pairs)} form key-value pairs")
    else:
        forms_text = ''

    # ALSO extract from LINE blocks (for encounter delimiter detection)
    lines = []
    for block in response['Blocks']:
        if block['BlockType'] == 'LINE':
            page = block.get('Page', 1)
            y_pos = block.get('Geometry', {}).get('BoundingBox', {}).get('Top', 0)
            lines.append({
                'text': block['Text'],
                'page': page,
                'y_position': y_pos
            })

    # Sort by page and Y-position to maintain reading order
    lines.sort(key=lambda x: (x['page'], x['y_position']))
    lines_text = '\n'.join([line['text'] for line in lines])
    extracted_data['lines'] = lines
    print(f"Extracted {len(lines)} text lines from LINE blocks")

    # Use FORMS text if available (better formatting), otherwise use LINE text
    extracted_data['text'] = forms_text if forms_text else lines_text

    # Store raw LINE text separately for delimiter detection
    extracted_data['lines_text'] = lines_text

    return extracted_data


def get_text_from_block(block, blocks_map):
    """Get text from a block by following CHILD relationships"""
    if not block:
        return ''

    text = ''
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child = blocks_map.get(child_id)
                    if child and child['BlockType'] == 'WORD':
                        text += child['Text'] + ' '
    return text.strip()


def get_value_block(key_block, blocks_map):
    """Get the VALUE block associated with a KEY block"""
    if 'Relationships' in key_block:
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    return blocks_map.get(value_id)
    return None


