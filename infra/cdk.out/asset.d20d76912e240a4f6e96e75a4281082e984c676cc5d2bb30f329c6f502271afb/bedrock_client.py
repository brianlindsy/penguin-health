"""
Bedrock Client for Claude model invocations.

Handles Claude model invocation via AWS Bedrock with:
- Rate limiting
- Throttling exception handling with retry
- JSON extraction from responses
"""

import json
import re
import time
import random
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from rate_limiter import RateLimiter

# Module-level rate limiter (100 requests per minute)
bedrock_rate_limiter = RateLimiter(max_requests_per_minute=1000)

# Default model ID for Claude Sonnet 4.5
MODEL_ID = 'global.anthropic.claude-sonnet-4-5-20250929-v1:0'


def _extract_complete_json(text: str, start_char: str = '{', end_char: str = '}') -> Optional[str]:
    """
    Extract a complete JSON object or array by properly matching brackets.
    Handles nested structures and strings containing brackets.

    Args:
        text: The text to search
        start_char: Opening bracket ('{' for objects, '[' for arrays)
        end_char: Closing bracket ('}' for objects, ']' for arrays)
    """
    start = text.find(start_char)
    if start == -1:
        return None

    bracket_count = 0
    in_string = False
    escape_next = False

    for i in range(start, len(text)):
        char = text[i]

        if escape_next:
            escape_next = False
            continue

        if char == '\\':
            escape_next = True
            continue

        if char == '"':
            in_string = not in_string
            continue

        if not in_string:
            if char == start_char:
                bracket_count += 1
            elif char == end_char:
                bracket_count -= 1
                if bracket_count == 0:
                    return text[start:i+1]

    return None


def extract_json_from_claude_response(response_body: dict):
    """Extract JSON (object or array) from a Bedrock Claude model response."""
    content_list = response_body.get("content", [])
    if not content_list:
        print("No 'content' field found or it's empty in the model response.")
        return None

    all_text = " ".join(
        block.get("text", "") for block in content_list if block.get("type") == "text"
    )

    # Try ```json``` code block first (handles both objects and arrays)
    match = re.search(r"```json\s*([\[\{][\s\S]*?[\]\}])\s*```", all_text)
    if match:
        json_str = match.group(1)
        try:
            parsed = json.loads(json_str)
            print(f"Extracted JSON from code block: {type(parsed).__name__}")
            return parsed
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from code block: {e}")

    # Try to find array first (for validation results)
    if '[' in all_text:
        array_start = all_text.find('[')
        obj_start = all_text.find('{')
        # If array comes before object (or no object), try array first
        if obj_start == -1 or array_start < obj_start:
            json_str = _extract_complete_json(all_text, '[', ']')
            if json_str:
                try:
                    parsed = json.loads(json_str)
                    print(f"Extracted JSON array with {len(parsed)} items")
                    return parsed
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON array: {e}")

    # Fall back to object extraction
    json_str = _extract_complete_json(all_text, '{', '}')
    if not json_str:
        print("No valid JSON found in response.")
        return None

    try:
        parsed = json.loads(json_str)
        print(f"Extracted JSON object")
        return parsed
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON object: {e}")
        print(f"Raw JSON string: {json_str[:500]}...")
        return None


def invoke_claude_model(
    inference_profile_id: str,
    body: dict,
    return_json_only: bool,
    bedrock_client=None,
    retries: int = 1,
    raise_on_error: bool = True,
    region_name: str = 'us-east-1',
    throttle_retries: int = 3,
):
    """
    Invoke Claude model via Bedrock with rate limiting, throttle handling, and JSON extraction.

    Args:
        inference_profile_id: Bedrock model ID or inference profile
        body: Request body for the model
        return_json_only: If True, extract and return JSON from response
        bedrock_client: Optional boto3 bedrock-runtime client
        retries: Number of retries for JSON extraction failures
        raise_on_error: If True, raise exception on failure
        region_name: AWS region for bedrock client
        throttle_retries: Number of retries on ThrottlingException

    Returns:
        dict: Extracted JSON or full response body
    """
    if bedrock_client is None:
        bedrock_client = boto3.client('bedrock-runtime', region_name=region_name)

    # Wait for rate limit before making request
    bedrock_rate_limiter.wait_if_needed()

    # Attempt request with throttle retry logic
    model_response = None
    for attempt in range(throttle_retries + 1):
        try:
            model_response = bedrock_client.invoke_model(
                modelId=inference_profile_id,
                body=json.dumps(body),
                contentType='application/json',
                accept='application/json',
            )
            break  # Success, exit retry loop
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ThrottlingException':
                if attempt < throttle_retries:
                    wait_time = random.uniform(60, 70)
                    print(f"ThrottlingException: Waiting {wait_time:.1f}s before retry {attempt + 1}/{throttle_retries}")
                    time.sleep(wait_time)
                else:
                    print(f"ThrottlingException: All {throttle_retries} retries exhausted")
                    raise
            else:
                raise  # Re-raise non-throttling errors

    response_body = json.loads(model_response['body'].read())

    if not return_json_only:
        return response_body

    extracted_json = extract_json_from_claude_response(response_body)

    if extracted_json is None:
        if retries > 0:
            return invoke_claude_model(
                inference_profile_id=inference_profile_id,
                body=body,
                return_json_only=return_json_only,
                bedrock_client=bedrock_client,
                retries=retries - 1,
                raise_on_error=raise_on_error,
                region_name=region_name,
                throttle_retries=throttle_retries,
            )
        else:
            if raise_on_error:
                raise ValueError("No JSON found in Claude response")
            return None

    return extracted_json
