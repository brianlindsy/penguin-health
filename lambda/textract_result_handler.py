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

        # Extract structured data
        extracted_data = process_textract_response(textract_response)

        # Determine if this is an IRP document based on source folder
        is_irp = 'irp/' in source_file_key
        processed_folder = config['TEXTRACT_IRP_PROCESSED'] if is_irp else config['TEXTRACT_PROCESSED']
        archive_folder = "archived/irp/textract/" if is_irp else "archived/textract/"

        # Generate base file name
        filename = source_file_key.split('/')[-1].replace('.pdf', '')
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')

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
    Split encounters using text-based line matching as fallback.
    More reliable when Textract fails to detect all delimiter fields as key-value pairs.
    """
    raw_lines = extracted_data.get('raw_lines', [])
    forms = extracted_data.get('forms', {})

    # Find all lines containing the delimiter
    delimiter_lines = []
    for idx, line in enumerate(raw_lines):
        if delimiter_field in line.get('text', ''):
            delimiter_lines.append({
                'line_index': idx,
                'page': line.get('page', 1),
                'y_position': line.get('geometry', {}).get('BoundingBox', {}).get('Top', 0),
                'line': line
            })

    print(f"DEBUG: Text-based splitting found {len(delimiter_lines)} delimiter lines")

    if len(delimiter_lines) <= 1:
        return [extracted_data]

    # Sort by page and Y-position
    delimiter_lines.sort(key=lambda x: (x['page'], x['y_position']))

    # Create encounters based on line ranges
    encounters = []
    for i in range(len(delimiter_lines)):
        start_idx = delimiter_lines[i]['line_index']
        end_idx = delimiter_lines[i + 1]['line_index'] if i + 1 < len(delimiter_lines) else len(raw_lines)

        # Get lines for this encounter
        encounter_lines = raw_lines[start_idx:end_idx]

        # Determine page boundaries
        start_page = delimiter_lines[i]['page']
        start_y = delimiter_lines[i]['y_position']
        end_page = delimiter_lines[i + 1]['page'] if i + 1 < len(delimiter_lines) else start_page + 10
        end_y = delimiter_lines[i + 1]['y_position'] if i + 1 < len(delimiter_lines) else 1.0

        boundary = {
            'encounter_index': i,
            'page': start_page,
            'y_start': start_y,
            'y_end': end_y,
            'next_page': end_page
        }

        # Assign forms based on geometry
        encounter_forms = {}
        for form_key, form_data in forms.items():
            form_page = form_data.get('page', 1)
            form_y = form_data.get('geometry', {}).get('BoundingBox', {}).get('Top', 0)
            if belongs_to_encounter(form_page, form_y, boundary):
                encounter_forms[form_key] = form_data

        encounter_data = create_encounter_from_forms_and_lines(
            encounter_forms,
            encounter_lines,
            extracted_data,
            i
        )
        encounters.append(encounter_data)

    return encounters


def split_into_encounters(extracted_data, is_irp=False):
    """
    Split a multi-encounter document into separate encounters

    For regular charts: attempts form-based splitting first, falls back to text-based
    For IRP documents: typically single encounter, returns as-is
    """
    if is_irp:
        # IRP documents are typically single encounter
        return [extracted_data]

    # Try text-based splitting as fallback when forms don't capture all delimiters
    return split_into_encounters_text_based(extracted_data, 'Consumer Service ID:')


def belongs_to_encounter(item_page, item_y, boundary):
    """
    Determine if an item (form field or text line) belongs to an encounter
    based on its page and Y-position
    """
    # Item is on the same page as encounter start
    if item_page == boundary['page']:
        # Check if Y position is after encounter start
        if item_y >= boundary['y_start']:
            # If there's a next encounter on the same page, check we're before it
            if boundary['next_page'] == boundary['page']:
                return item_y < boundary['y_end']
            else:
                # Next encounter is on a different page, so include everything after start
                return True
        else:
            return False
    # Item is on a page between encounter start and next encounter
    elif item_page > boundary['page'] and item_page < boundary['next_page']:
        return True
    # Item is on the same page as next encounter
    elif item_page == boundary['next_page'] and boundary['next_page'] != boundary['page']:
        # Include items before the next encounter starts
        return item_y < boundary['y_end']
    else:
        return False


def create_encounter_from_forms_and_lines(encounter_forms, encounter_lines, original_data, encounter_index):
    """
    Create an encounter data structure from a subset of forms and text lines
    This properly splits the text field per encounter based on geometry
    """
    # Build the text field from only the lines that belong to this encounter
    encounter_text = '\n'.join([line['text'] for line in encounter_lines])

    # Remove geometry data from forms before storing (keep only value, confidence, and original_key)
    clean_forms = {}
    for key, data in encounter_forms.items():
        clean_forms[key] = {
            'value': data.get('value', ''),
            'confidence': data.get('confidence', 0),
            'original_key': data.get('original_key', key)  # Preserve original_key for rules engine
        }

    # Clean up raw_lines to remove geometry (optional - can keep for debugging)
    clean_lines = []
    for line in encounter_lines:
        clean_lines.append({
            'text': line['text'],
            'confidence': line['confidence']
        })

    encounter_data = {
        'text': encounter_text,
        'forms': clean_forms,
        'raw_lines': clean_lines,
        'metadata': {
            **original_data.get('metadata', {}),
            'encounter_index': encounter_index,
            'is_split_encounter': True,
            'line_count': len(encounter_lines),
            'form_count': len(encounter_forms)
        }
    }

    return encounter_data


def process_textract_response(response):
    """
    Extract forms and text from Textract document analysis response
    """
    extracted_data = {
        'text': '',
        'forms': {},
        'raw_lines': [],
        'metadata': {
            'document_pages': response.get('DocumentMetadata', {}).get('Pages', 0),
            'extraction_timestamp': datetime.utcnow().isoformat(),
            'job_status': response.get('JobStatus')
        }
    }

    # Create a map of block IDs to blocks for easy lookup
    blocks_map = {}
    for block in response['Blocks']:
        blocks_map[block['Id']] = block

    # Extract forms (key-value pairs)
    print(f"DEBUG TEXTRACT: Processing {len(response['Blocks'])} blocks")
    form_count = 0

    for block in response['Blocks']:
        if block['BlockType'] == 'KEY_VALUE_SET':
            if block.get('EntityTypes') and 'KEY' in block['EntityTypes']:
                key_text = get_text(block, blocks_map)
                value_block = get_value_block(block, blocks_map)
                value_text = get_text(value_block, blocks_map) if value_block else ''

                # Debug log for Consumer Service ID or similar fields
                if key_text and ('consumer' in key_text.lower() or 'service' in key_text.lower() or 'id' in key_text.lower()):
                    print(f"DEBUG TEXTRACT: Found potential ID field - Key: '{key_text}', Value: '{value_text}'")

                if key_text:
                    form_count += 1
                    # Store with geometry for splitting
                    extracted_data['forms'][key_text] = {
                        'value': value_text,
                        'confidence': block.get('Confidence', 0),
                        'geometry': block.get('Geometry', {}),
                        'page': block.get('Page', 1),
                        'original_key': key_text
                    }

    print(f"DEBUG TEXTRACT: Extracted {form_count} form key-value pairs")

    # Extract text lines with geometry information
    for block in response['Blocks']:
        if block['BlockType'] == 'LINE':
            line_data = {
                'text': block['Text'],
                'confidence': block['Confidence'],
                'geometry': block.get('Geometry', {}),
                'page': block.get('Page', 1)
            }
            extracted_data['raw_lines'].append(line_data)
            extracted_data['text'] += block['Text'] + '\n'

    print(f"Extracted {len(extracted_data['forms'])} forms, {len(extracted_data['raw_lines'])} text lines")

    return extracted_data


def get_text(block, blocks_map):
    """Get text from a block"""
    text = ''
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = blocks_map.get(child_id)
                    if child_block and child_block['BlockType'] == 'WORD':
                        text += child_block['Text'] + ' '
    return text.strip()


def get_value_block(key_block, blocks_map):
    """Get the value block associated with a key block"""
    if 'Relationships' in key_block:
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    return blocks_map.get(value_id)
    return None
