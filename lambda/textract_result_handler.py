import json
import boto3
import os
from datetime import datetime

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

def lambda_handler(event, context):
    """
    Lambda function triggered by SNS when Textract job completes
    Retrieves results and stores them in the processed folder

    Configuration can be passed via environment variables or event parameters.
    Event parameters take precedence over environment variables.
    """
    # Load configuration from environment variables
    config = {
        'BUCKET_NAME': os.environ.get('BUCKET_NAME'),
        'TEXTRACT_PROCESSED': os.environ.get('TEXTRACT_PROCESSED'),
        'TEXTRACT_IRP_PROCESSED': os.environ.get('TEXTRACT_IRP_PROCESSED')
    }

    # Override with event-level config if provided
    if 'config' in event:
        config.update(event['config'])

    try:
        # Parse SNS message
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            job_id = message.get('JobId')
            status = message.get('Status')
            api = message.get('API')

            print(f"Received notification for Job ID: {job_id}, Status: {status}, API: {api}")

            if status == 'SUCCEEDED':
                # Retrieve the source file and config from job metadata
                source_file_key, metadata_config = get_source_file_from_metadata(job_id, config)

                # Merge metadata config with event config (event config takes precedence)
                if metadata_config:
                    merged_config = {**metadata_config, **config}
                    config = merged_config

                if source_file_key:
                    # Process and store results
                    process_and_store_results(job_id, source_file_key, config)
                else:
                    print(f"Could not find source file for job {job_id}")

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


def get_source_file_from_metadata(job_id, config):
    """
    Retrieve source file path and config from stored metadata
    Returns tuple: (source_file_key, metadata_config)
    """
    try:
        # List metadata files
        response = s3_client.list_objects_v2(
            Bucket=config['BUCKET_NAME'],
            Prefix='textract-processing/'
        )

        if 'Contents' not in response:
            return None, None

        # Search for metadata file with matching job_id
        for obj in response['Contents']:
            if obj['Key'].endswith('-metadata.json'):
                metadata_obj = s3_client.get_object(
                    Bucket=config['BUCKET_NAME'],
                    Key=obj['Key']
                )
                metadata = json.loads(metadata_obj['Body'].read().decode('utf-8'))

                if metadata.get('job_id') == job_id:
                    # Delete metadata file after use
                    s3_client.delete_object(Bucket=config['BUCKET_NAME'], Key=obj['Key'])
                    return metadata.get('source_file'), metadata.get('config', {})

        return None, None

    except Exception as e:
        print(f"Error retrieving metadata: {str(e)}")
        return None, None


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


def process_and_store_results(job_id, source_file_key, config):
    """
    Process Textract results and store in processed folder
    Handles multi-encounter documents by splitting them into separate files
    """
    try:
        # Get Textract results
        textract_response = get_textract_results(job_id)

        if textract_response['JobStatus'] != 'SUCCEEDED':
            print(f"Textract job {job_id} status: {textract_response['JobStatus']}")
            return False

        # Determine if this is an IRP document based on source folder
        is_irp = 'irp/' in source_file_key
        processed_folder = config['TEXTRACT_IRP_PROCESSED'] if is_irp else config['TEXTRACT_PROCESSED']
        archive_folder = "archived/irp/textract/" if is_irp else "archived/textract/"

        # Generate base file name
        filename = source_file_key.split('/')[-1].replace('.pdf', '')
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

        # Save raw Textract response to textract-raw folder
        raw_folder = "textract-raw/irp/" if is_irp else "textract-raw/"
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
        encounters = split_into_encounters(extracted_data, is_irp)

        # Save each encounter as a separate file
        if len(encounters) > 1:
            print(f"Split document into {len(encounters)} encounters")
            for idx, encounter in enumerate(encounters):
                encounter_id = encounter.get('forms', {}).get('Consumer Service ID:', {}).get('value', f'encounter-{idx+1}')
                # Sanitize encounter_id for filename
                safe_encounter_id = encounter_id.replace('/', '-').replace(' ', '-')
                output_key = f"{processed_folder}{filename}-{safe_encounter_id}-{timestamp}.json"

                s3_client.put_object(
                    Bucket=config['BUCKET_NAME'],
                    Key=output_key,
                    Body=json.dumps(encounter, indent=2),
                    ContentType='application/json'
                )
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
            print(f"Successfully processed and saved to: {output_key}")

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
    Split encounters by finding the delimiter in the text.
    Each encounter gets all text from one delimiter to the next.
    """
    full_text = extracted_data.get('text', '')

    # Split by lines and find delimiter
    text_lines = full_text.split('\n')
    delimiter_indices = []

    for idx, line in enumerate(text_lines):
        if delimiter_field in line:
            delimiter_indices.append(idx)

    print(f"Found {len(delimiter_indices)} encounters (delimiter: '{delimiter_field}')")

    # If no delimiters or only one, return whole document as single encounter
    if len(delimiter_indices) <= 1:
        return [{
            'text': full_text,
            'metadata': {
                **extracted_data.get('metadata', {}),
                'encounter_index': 0,
                'is_split_encounter': False,
                'line_count': len(text_lines)
            }
        }]

    # Create encounters based on delimiter positions
    encounters = []
    for i in range(len(delimiter_indices)):
        start_idx = delimiter_indices[i]
        end_idx = delimiter_indices[i + 1] if i + 1 < len(delimiter_indices) else len(text_lines)

        # Get text lines for this encounter
        encounter_lines = text_lines[start_idx:end_idx]
        encounter_text = '\n'.join(encounter_lines)

        encounter_data = {
            'text': encounter_text,
            'metadata': {
                **extracted_data.get('metadata', {}),
                'encounter_index': i,
                'is_split_encounter': True,
                'line_count': len(encounter_lines),
                'start_line': start_idx,
                'end_line': end_idx
            }
        }

        encounters.append(encounter_data)
        print(f"Encounter {i}: {len(encounter_lines)} lines")

    return encounters


def split_into_encounters(extracted_data, is_irp=False):
    """
    Split a multi-encounter document into separate encounters

    For regular charts: Split by 'Consumer Service ID:' delimiter
    For IRP documents: Return as single encounter
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

    return split_into_encounters_text_based(extracted_data, 'Consumer Service ID:')


def process_textract_response(response):
    """
    Extract text content from LINE blocks
    LINE blocks capture all text including delimiters that may not be in FORMS
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

    # Extract from LINE blocks (captures all text in reading order)
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
    extracted_data['text'] = '\n'.join([line['text'] for line in lines])
    extracted_data['lines'] = lines
    print(f"Extracted {len(lines)} text lines from LINE blocks")

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


