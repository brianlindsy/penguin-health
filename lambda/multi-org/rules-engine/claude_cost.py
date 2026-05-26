"""
Per-org Claude cost attribution via CloudWatch metrics.

Emits one PutMetricData call per Bedrock invocation under namespace
`PenguinHealth/LLMCost`, dimensioned by (org_id, model_id, call_type) and
optionally (parent_request_id). No prompt or response text is stored —
token counts and computed USD only — so PHI/PII never crosses into the
metric stream.

USD is stored as integer micros (USD * 1_000_000) to avoid float drift
across aggregations. Divide by 1e6 at read time.

Cost emission is best-effort: a CloudWatch failure must NEVER fail a
customer Bedrock call, so the public entry point swallows exceptions
and logs them.
"""

import logging
import os

import boto3


logger = logging.getLogger(__name__)

NAMESPACE = 'PenguinHealth/LLMCost'

# USD per 1,000,000 tokens. Update when AWS publishes new Bedrock pricing
# or a new model is added. Historical metric data is NOT retroactively
# repriced — datapoints reflect the rate active at the time of the call.
MODEL_PRICING = {
    'global.anthropic.claude-sonnet-4-5-20250929-v1:0': {
        'input': 3.0,
        'output': 15.0,
        'cache_read': 0.30,
        'cache_write': 3.75,
    },
}


_cloudwatch_client = None


def _get_cloudwatch():
    global _cloudwatch_client
    if _cloudwatch_client is None:
        region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION') or 'us-east-1'
        _cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    return _cloudwatch_client


def compute_cost_micros(model_id, usage):
    """Return integer USD-micros for a Bedrock usage block.

    Unknown model_id -> 0 (logged once per call by the caller). Missing
    keys in `usage` are treated as 0 tokens."""
    pricing = MODEL_PRICING.get(model_id)
    if not pricing:
        return 0

    input_tokens = int(usage.get('input_tokens') or 0)
    output_tokens = int(usage.get('output_tokens') or 0)
    cache_read = int(usage.get('cache_read_input_tokens') or 0)
    cache_creation = int(usage.get('cache_creation_input_tokens') or 0)

    # USD = (tokens / 1e6) * price_per_million. Multiply by 1e6 again for
    # micros => the divisions cancel, so micros = tokens * price.
    cost_micros = (
        input_tokens * pricing['input']
        + output_tokens * pricing['output']
        + cache_read * pricing['cache_read']
        + cache_creation * pricing['cache_write']
    )
    return int(round(cost_micros))


def record_cost(*, org_id, user_email, call_type, model_id, response_body,
                duration_ms=None, parent_request_id=None):
    """Emit one CloudWatch metric set for a single Bedrock invocation.

    Never raises. A CloudWatch failure or malformed response is logged
    and swallowed so the customer request continues unaffected."""
    try:
        usage = (response_body or {}).get('usage') or {}
        if not usage:
            logger.warning(
                "claude_cost: missing usage block (org=%s call=%s model=%s) — emitting zeros",
                org_id, call_type, model_id,
            )

        if model_id not in MODEL_PRICING:
            logger.warning(
                "claude_cost: unknown model_id %s — emitting zero cost (org=%s call=%s)",
                model_id, org_id, call_type,
            )

        input_tokens = int(usage.get('input_tokens') or 0)
        output_tokens = int(usage.get('output_tokens') or 0)
        cache_read = int(usage.get('cache_read_input_tokens') or 0)
        cache_creation = int(usage.get('cache_creation_input_tokens') or 0)
        cost_micros = compute_cost_micros(model_id, usage)

        base_dims = [
            {'Name': 'org_id', 'Value': str(org_id or 'unknown')},
            {'Name': 'model_id', 'Value': str(model_id or 'unknown')},
            {'Name': 'call_type', 'Value': str(call_type or 'unknown')},
        ]

        def _metric(name, value, unit='Count'):
            return {
                'MetricName': name,
                'Dimensions': base_dims,
                'Value': value,
                'Unit': unit,
            }

        metric_data = [
            _metric('CostMicros', cost_micros),
            _metric('InputTokens', input_tokens),
            _metric('OutputTokens', output_tokens),
            _metric('CacheReadTokens', cache_read),
            _metric('CacheCreationTokens', cache_creation),
            _metric('CallCount', 1),
        ]
        if duration_ms is not None:
            metric_data.append({
                'MetricName': 'DurationMs',
                'Dimensions': base_dims,
                'Value': float(duration_ms),
                'Unit': 'Milliseconds',
            })

        # When the call is part of an agent loop, emit a second copy
        # carrying the parent_request_id dimension so the entire loop's
        # spend is queryable in one shot. Skipping when unset keeps
        # cardinality manageable for non-agent calls.
        if parent_request_id:
            parent_dims = base_dims + [
                {'Name': 'parent_request_id', 'Value': str(parent_request_id)},
            ]
            metric_data.append({
                'MetricName': 'CostMicros',
                'Dimensions': parent_dims,
                'Value': cost_micros,
                'Unit': 'Count',
            })
            metric_data.append({
                'MetricName': 'CallCount',
                'Dimensions': parent_dims,
                'Value': 1,
                'Unit': 'Count',
            })

        _get_cloudwatch().put_metric_data(
            Namespace=NAMESPACE,
            MetricData=metric_data,
        )
    except Exception as exc:
        # NEVER propagate. Cost emission is fire-and-forget; a CloudWatch
        # outage or perms regression must not break a customer Claude call.
        logger.warning(
            "claude_cost: record_cost failed (org=%s call=%s): %s",
            org_id, call_type, exc,
        )
