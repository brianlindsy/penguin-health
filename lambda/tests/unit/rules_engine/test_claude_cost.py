"""
Tests for per-org Claude cost attribution.

Covers:
- USD-micros calculation across all four token categories
- Unknown model_id → cost=0, metric still emitted
- Missing `usage` block → tolerated, metric emitted with zeros
- PHI guard: no field in any emitted metric carries response prose
- record_cost NEVER raises (CloudWatch failure is swallowed)
- parent_request_id is added as a dimension only when set
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine'))


SONNET_45 = 'global.anthropic.claude-sonnet-4-5-20250929-v1:0'


@pytest.fixture
def fake_cloudwatch(monkeypatch):
    """Replace the lazily-cached cloudwatch client with a mock and
    return it so individual tests can assert on put_metric_data calls."""
    import claude_cost

    mock = MagicMock()
    monkeypatch.setattr(claude_cost, '_cloudwatch_client', mock)
    yield mock


class TestComputeCostMicros:
    def test_input_tokens_only(self):
        from claude_cost import compute_cost_micros
        # 1,000,000 input tokens * $3/M = $3 = 3_000_000 micros
        assert compute_cost_micros(SONNET_45, {'input_tokens': 1_000_000}) == 3_000_000

    def test_output_tokens_only(self):
        from claude_cost import compute_cost_micros
        # 1,000,000 output tokens * $15/M = $15 = 15_000_000 micros
        assert compute_cost_micros(SONNET_45, {'output_tokens': 1_000_000}) == 15_000_000

    def test_cache_read(self):
        from claude_cost import compute_cost_micros
        # 1,000,000 cache_read * $0.30/M = $0.30 = 300_000 micros
        assert compute_cost_micros(SONNET_45, {'cache_read_input_tokens': 1_000_000}) == 300_000

    def test_cache_creation(self):
        from claude_cost import compute_cost_micros
        # 1,000,000 cache_creation * $3.75/M = $3.75 = 3_750_000 micros
        assert compute_cost_micros(SONNET_45, {'cache_creation_input_tokens': 1_000_000}) == 3_750_000

    def test_all_four_combined(self):
        from claude_cost import compute_cost_micros
        usage = {
            'input_tokens': 100_000,        # 100k * 3 = 300_000 micros
            'output_tokens': 50_000,        # 50k * 15 = 750_000 micros
            'cache_read_input_tokens': 200_000,      # 200k * 0.30 = 60_000 micros
            'cache_creation_input_tokens': 30_000,   # 30k * 3.75 = 112_500 micros
        }
        # Total: 300_000 + 750_000 + 60_000 + 112_500 = 1_222_500 micros
        assert compute_cost_micros(SONNET_45, usage) == 1_222_500

    def test_unknown_model_returns_zero(self):
        from claude_cost import compute_cost_micros
        assert compute_cost_micros('made-up-model', {'input_tokens': 1_000_000}) == 0

    def test_missing_usage_keys_treated_as_zero(self):
        from claude_cost import compute_cost_micros
        assert compute_cost_micros(SONNET_45, {}) == 0

    def test_none_values_in_usage_treated_as_zero(self):
        from claude_cost import compute_cost_micros
        # Defensive: some Bedrock responses may carry null for a token field
        usage = {'input_tokens': None, 'output_tokens': 100}
        # 100 output * 15 = 1500 micros
        assert compute_cost_micros(SONNET_45, usage) == 1500


class TestRecordCost:
    def test_happy_path_emits_one_putmetricdata_call(self, fake_cloudwatch):
        from claude_cost import record_cost, NAMESPACE

        record_cost(
            org_id='catholic-charities-multi-org',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body={
                'usage': {'input_tokens': 1000, 'output_tokens': 200},
                'content': [{'type': 'text', 'text': 'PATIENT_SECRET_123 PASS'}],
            },
            duration_ms=350,
        )

        fake_cloudwatch.put_metric_data.assert_called_once()
        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        assert kwargs['Namespace'] == NAMESPACE

        metric_names = {m['MetricName'] for m in kwargs['MetricData']}
        assert {'CostMicros', 'InputTokens', 'OutputTokens',
                'CacheReadTokens', 'CacheCreationTokens',
                'CallCount', 'DurationMs'} <= metric_names

    def test_dimensions_carry_org_model_calltype(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='rule_fields_enhance',
            model_id=SONNET_45,
            response_body={'usage': {'input_tokens': 10, 'output_tokens': 5}},
        )

        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        cost_metric = next(m for m in kwargs['MetricData'] if m['MetricName'] == 'CostMicros')
        dim_map = {d['Name']: d['Value'] for d in cost_metric['Dimensions']}
        assert dim_map['org_id'] == 'org-xyz'
        assert dim_map['model_id'] == SONNET_45
        assert dim_map['call_type'] == 'rule_fields_enhance'
        # parent_request_id MUST NOT appear in the base dims when unset.
        assert 'parent_request_id' not in dim_map

    def test_parent_request_id_adds_extra_dimension(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='nl_agent_step',
            model_id=SONNET_45,
            response_body={'usage': {'input_tokens': 10, 'output_tokens': 5}},
            parent_request_id='job-abc-123',
        )

        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        # Two CostMicros datapoints: one without parent dim, one with.
        cost_metrics = [m for m in kwargs['MetricData'] if m['MetricName'] == 'CostMicros']
        assert len(cost_metrics) == 2
        has_parent = [
            m for m in cost_metrics
            if any(d['Name'] == 'parent_request_id' for d in m['Dimensions'])
        ]
        assert len(has_parent) == 1
        parent_dim = next(d for d in has_parent[0]['Dimensions'] if d['Name'] == 'parent_request_id')
        assert parent_dim['Value'] == 'job-abc-123'

    def test_unknown_model_still_emits_with_zero_cost(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id='made-up-model',
            response_body={'usage': {'input_tokens': 1000, 'output_tokens': 200}},
        )

        fake_cloudwatch.put_metric_data.assert_called_once()
        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        cost = next(m for m in kwargs['MetricData'] if m['MetricName'] == 'CostMicros')
        assert cost['Value'] == 0
        # Token counts still emit so we can detect "unknown model with traffic".
        in_tokens = next(m for m in kwargs['MetricData'] if m['MetricName'] == 'InputTokens')
        assert in_tokens['Value'] == 1000

    def test_missing_usage_block_emits_zeros(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body={'content': []},
        )

        fake_cloudwatch.put_metric_data.assert_called_once()
        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        for name in ('CostMicros', 'InputTokens', 'OutputTokens',
                     'CacheReadTokens', 'CacheCreationTokens'):
            m = next(x for x in kwargs['MetricData'] if x['MetricName'] == name)
            assert m['Value'] == 0

    def test_none_response_body_tolerated(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body=None,
        )

        fake_cloudwatch.put_metric_data.assert_called_once()

    def test_phi_never_appears_in_metric_data(self, fake_cloudwatch):
        """CLAUDE.md: no PHI/PII in any observability stream. The
        response body intentionally contains a marker that must NOT
        appear anywhere in the PutMetricData payload."""
        from claude_cost import record_cost

        marker = 'PATIENT_SECRET_123_DO_NOT_LEAK'
        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body={
                'usage': {'input_tokens': 10, 'output_tokens': 5},
                'content': [{'type': 'text', 'text': f'Diagnosis for {marker}'}],
            },
        )

        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        # Stringify the whole MetricData payload + namespace and confirm
        # the marker is nowhere in it.
        payload = repr(kwargs)
        assert marker not in payload

    def test_putmetricdata_failure_is_swallowed(self, fake_cloudwatch):
        """A boto3 / CloudWatch failure MUST NOT break a customer call."""
        from claude_cost import record_cost

        fake_cloudwatch.put_metric_data.side_effect = RuntimeError("cloudwatch is down")

        # Must not raise.
        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body={'usage': {'input_tokens': 10, 'output_tokens': 5}},
        )

        fake_cloudwatch.put_metric_data.assert_called_once()

    def test_duration_ms_omitted_when_not_provided(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body={'usage': {'input_tokens': 10, 'output_tokens': 5}},
            # No duration_ms
        )

        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        names = {m['MetricName'] for m in kwargs['MetricData']}
        assert 'DurationMs' not in names

    def test_cost_calculation_passes_through_to_metric(self, fake_cloudwatch):
        from claude_cost import record_cost

        record_cost(
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
            model_id=SONNET_45,
            response_body={
                'usage': {
                    'input_tokens': 1_000_000,    # $3
                    'output_tokens': 500_000,     # $7.50
                }
            },
        )

        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        cost = next(m for m in kwargs['MetricData'] if m['MetricName'] == 'CostMicros')
        # $10.50 == 10_500_000 micros
        assert cost['Value'] == 10_500_000


class TestBedrockClientIntegration:
    """End-to-end: invoke_claude_model -> record_cost -> CloudWatch."""

    def test_invoke_records_cost_when_org_id_provided(self, fake_cloudwatch):
        import json as _json
        from unittest.mock import MagicMock

        from bedrock_client import invoke_claude_model

        stream = MagicMock()
        stream.read.return_value = _json.dumps({
            'usage': {'input_tokens': 100, 'output_tokens': 50},
            'content': [{'type': 'text', 'text': 'PASS'}],
        }).encode('utf-8')

        fake_bedrock = MagicMock()
        fake_bedrock.invoke_model.return_value = {'body': stream}

        invoke_claude_model(
            inference_profile_id=SONNET_45,
            body={'messages': []},
            return_json_only=False,
            bedrock_client=fake_bedrock,
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
        )

        fake_cloudwatch.put_metric_data.assert_called_once()
        kwargs = fake_cloudwatch.put_metric_data.call_args.kwargs
        cost = next(m for m in kwargs['MetricData'] if m['MetricName'] == 'CostMicros')
        # 100 input * 3 + 50 output * 15 = 300 + 750 = 1050 micros
        assert cost['Value'] == 1050

    def test_invoke_skips_cost_recording_when_org_id_missing(self, fake_cloudwatch):
        """Back-compat: callers that haven't been migrated don't crash
        and don't emit anonymous cost data."""
        import json as _json
        from unittest.mock import MagicMock

        from bedrock_client import invoke_claude_model

        stream = MagicMock()
        stream.read.return_value = _json.dumps({
            'usage': {'input_tokens': 100, 'output_tokens': 50},
            'content': [{'type': 'text', 'text': 'PASS'}],
        }).encode('utf-8')

        fake_bedrock = MagicMock()
        fake_bedrock.invoke_model.return_value = {'body': stream}

        invoke_claude_model(
            inference_profile_id=SONNET_45,
            body={'messages': []},
            return_json_only=False,
            bedrock_client=fake_bedrock,
            # No org_id
        )

        fake_cloudwatch.put_metric_data.assert_not_called()

    def test_invoke_records_cost_on_every_retry(self, fake_cloudwatch):
        """Bedrock bills per invocation; every retry must show up as
        its own cost metric."""
        import json as _json
        from unittest.mock import MagicMock

        from bedrock_client import invoke_claude_model

        no_json_stream = MagicMock()
        no_json_stream.read.return_value = _json.dumps({
            'usage': {'input_tokens': 10, 'output_tokens': 5},
            'content': [{'type': 'text', 'text': 'no json here'}],
        }).encode('utf-8')

        good_stream = MagicMock()
        good_stream.read.return_value = _json.dumps({
            'usage': {'input_tokens': 20, 'output_tokens': 10},
            'content': [{'type': 'text', 'text': '```json\n{"ok": true}\n```'}],
        }).encode('utf-8')

        fake_bedrock = MagicMock()
        fake_bedrock.invoke_model.side_effect = [
            {'body': no_json_stream},
            {'body': good_stream},
        ]

        invoke_claude_model(
            inference_profile_id=SONNET_45,
            body={'messages': []},
            return_json_only=True,
            bedrock_client=fake_bedrock,
            retries=1,
            org_id='org-xyz',
            user_email='alice@example.com',
            call_type='chart_rule_validate',
        )

        # Two Bedrock calls -> two CloudWatch emissions.
        assert fake_bedrock.invoke_model.call_count == 2
        assert fake_cloudwatch.put_metric_data.call_count == 2
