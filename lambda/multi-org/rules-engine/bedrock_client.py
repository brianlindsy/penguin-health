"""
Bedrock Client for Claude model invocations.

Canonical home for `invoke_claude_model`. Other lambdas import this
module — the admin_api lambda used to carry a near-identical copy; that
duplicate was removed during the cost-attribution consolidation.

Handles:
- Tuned boto3 config (long read_timeout for tool-use turns, retries
  disabled so we drive retries explicitly).
- Rate limiting to stay within Bedrock RPM limits.
- JSON extraction from Claude responses (fenced code blocks + brace-
  matched raw JSON fallback).
- Per-call cost emission to CloudWatch via claude_cost.record_cost.
"""

import json
import re
import time
from typing import Optional

import boto3
from botocore.config import Config as BotoConfig

import claude_cost
from rate_limiter import RateLimiter

# Module-level rate limiter (10000 requests per minute)
bedrock_rate_limiter = RateLimiter(max_requests_per_minute=10000)

# Default model ID for Claude Sonnet 4.5
MODEL_ID = 'global.anthropic.claude-sonnet-4-5-20250929-v1:0'

# Bedrock client tuning for tool-use turns: read_timeout above the boto3
# default of 60s because Claude can take longer to reason over chunky
# tool_result content (large run_sql output, narrative extraction). The
# worker Lambda has a 10-min ceiling; this just stops boto3 from giving
# up prematurely inside a single turn. retries={} disables botocore's
# legacy retry mode (we drive retries ourselves where wanted).
_BEDROCK_BOTO_CONFIG = BotoConfig(
    read_timeout=300,
    connect_timeout=10,
    retries={'max_attempts': 1, 'mode': 'standard'},
)


def _extract_complete_json(text: str) -> Optional[str]:
    """
    Extract a complete JSON object by properly matching braces.
    Handles nested objects and strings containing braces.
    """
    start = text.find('{')
    if start == -1:
        return None

    brace_count = 0
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
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    return text[start:i+1]

    return None


def extract_json_from_claude_response(response_body: dict) -> Optional[dict]:
    """Extract JSON from a Bedrock Claude model response."""
    content_list = response_body.get("content", [])
    if not content_list:
        print("No 'content' field found or it's empty in the model response.")
        return None

    all_text = " ".join(
        block.get("text", "") for block in content_list if block.get("type") == "text"
    )

    # Try ```json``` code block first
    match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", all_text)
    if not match:
        print("No JSON code block found. Trying raw extraction...")
        json_str = _extract_complete_json(all_text)
        if not json_str:
            print("No valid JSON object found.")
            return None

        try:
            parsed = json.loads(json_str)
            print(f"Extracted JSON data: {len(json_str)} chars, keys={list(parsed.keys()) if isinstance(parsed, dict) else type(parsed).__name__}")
            return parsed
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e} (raw length {len(json_str)} chars)")
        return None

    json_str = match.group(1)
    try:
        parsed = json.loads(json_str)
        print(f"Extracted JSON data: {len(json_str)} chars, keys={list(parsed.keys()) if isinstance(parsed, dict) else type(parsed).__name__}")
        return parsed
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e} (raw length {len(json_str)} chars)")
        return None


def invoke_claude_model(
    inference_profile_id: str,
    body: dict,
    return_json_only: bool,
    bedrock_client=None,
    retries: int = 1,
    raise_on_error: bool = True,
    region_name: str = 'us-east-1',
    *,
    org_id: Optional[str] = None,
    user_email: Optional[str] = None,
    call_type: Optional[str] = None,
    parent_request_id: Optional[str] = None,
):
    """Invoke Claude model via Bedrock with rate limiting, cost emission,
    JSON extraction, and retry logic.

    The body dict is forwarded to Bedrock as-is, so callers can include
    tool-use fields (`tools`, `tool_choice`) and multi-turn `messages`
    arrays. Tool-use loops live in nl_agent.run_agent_loop; this function
    is single-turn.

    Cost-attribution kwargs (`org_id`, `user_email`, `call_type`,
    `parent_request_id`) are accepted as keyword-only so the legacy
    positional signature is preserved for any tests that still pass
    positionally. They are forwarded to claude_cost.record_cost on EVERY
    successful Bedrock invocation, including retries — Bedrock bills per
    invocation regardless of whether downstream JSON parsing succeeds.
    """
    if bedrock_client is None:
        bedrock_client = boto3.client(
            'bedrock-runtime',
            region_name=region_name,
            config=_BEDROCK_BOTO_CONFIG,
        )

    # Wait for rate limit before making request
    bedrock_rate_limiter.wait_if_needed()

    start = time.time()
    model_response = bedrock_client.invoke_model(
        modelId=inference_profile_id,
        body=json.dumps(body),
        contentType='application/json',
        accept='application/json',
    )

    response_body = json.loads(model_response['body'].read())
    duration_ms = int((time.time() - start) * 1000)

    # Cost emission is best-effort; record_cost swallows any exception.
    # We only attempt it when org_id is present (i.e. caller opted in).
    if org_id is not None:
        claude_cost.record_cost(
            org_id=org_id,
            user_email=user_email,
            call_type=call_type,
            model_id=inference_profile_id,
            response_body=response_body,
            duration_ms=duration_ms,
            parent_request_id=parent_request_id,
        )

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
                org_id=org_id,
                user_email=user_email,
                call_type=call_type,
                parent_request_id=parent_request_id,
            )
        else:
            if raise_on_error:
                raise ValueError("No JSON found in Claude response")
            return None

    return extracted_json
