"""
Characterization tests for invoke_claude_model.

Pinned BEFORE the consolidation refactor to prove that:
  - the body dict passed to bedrock_client.invoke_model is forwarded byte-
    for-byte (Bedrock is strict about message shape)
  - return_json_only=False returns the raw response
  - JSON extraction handles both the ```json fenced and raw-braces paths
  - retry-on-no-JSON works
  - raise_on_error / raise_on_error=False behaviour is preserved
  - the rate limiter is consulted exactly once per call
  - dependency-injected bedrock_client is used as-is

These tests are the regression net for the consolidation. After the
refactor moves the canonical implementation into a single module, they
must continue to pass unchanged.
"""

import json
import os
import sys
from unittest.mock import MagicMock

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', 'multi-org', 'rules-engine'))


def _make_bedrock_response(body_dict):
    """Build a fake bedrock-runtime invoke_model response shape."""
    stream = MagicMock()
    stream.read.return_value = json.dumps(body_dict).encode('utf-8')
    return {'body': stream}


def _claude_text_body(text):
    """Build a Claude messages-API response body containing a single text block."""
    return {'content': [{'type': 'text', 'text': text}]}


class TestInvokeClaudeModel:
    def test_passes_body_unchanged_to_bedrock(self):
        from bedrock_client import invoke_claude_model

        body = {
            'anthropic_version': 'bedrock-2023-05-31',
            'max_tokens': 256,
            'system': 'SYS',
            'messages': [{'role': 'user', 'content': 'hello'}],
        }
        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(_claude_text_body('hi'))

        invoke_claude_model(
            inference_profile_id='test-model',
            body=body,
            return_json_only=False,
            bedrock_client=fake_client,
        )

        assert fake_client.invoke_model.call_count == 1
        call = fake_client.invoke_model.call_args
        assert call.kwargs['modelId'] == 'test-model'
        assert call.kwargs['contentType'] == 'application/json'
        assert call.kwargs['accept'] == 'application/json'
        # Body is forwarded as exact-JSON of the input dict.
        assert json.loads(call.kwargs['body']) == body

    def test_returns_raw_body_when_return_json_only_false(self):
        from bedrock_client import invoke_claude_model

        response_body = _claude_text_body('not json at all, just narrative')
        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(response_body)

        result = invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=False,
            bedrock_client=fake_client,
        )

        assert result == response_body

    def test_extracts_json_from_fenced_code_block(self):
        from bedrock_client import invoke_claude_model

        body = _claude_text_body(
            'Here is the result:\n```json\n{"status": "PASS", "score": 0.9}\n```\nDone.'
        )
        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(body)

        result = invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=True,
            bedrock_client=fake_client,
        )

        assert result == {'status': 'PASS', 'score': 0.9}

    def test_extracts_raw_json_no_fence(self):
        from bedrock_client import invoke_claude_model

        # No ```json fence — brace-matching fallback must find this.
        body = _claude_text_body('Result: {"status": "FAIL", "nested": {"k": "v"}}')
        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(body)

        result = invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=True,
            bedrock_client=fake_client,
        )

        assert result == {'status': 'FAIL', 'nested': {'k': 'v'}}

    def test_retries_when_first_response_has_no_json(self):
        from bedrock_client import invoke_claude_model

        no_json = _claude_text_body('I refuse to answer in JSON.')
        good_json = _claude_text_body('```json\n{"ok": true}\n```')
        fake_client = MagicMock()
        fake_client.invoke_model.side_effect = [
            _make_bedrock_response(no_json),
            _make_bedrock_response(good_json),
        ]

        result = invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=True,
            bedrock_client=fake_client,
            retries=1,
        )

        assert result == {'ok': True}
        assert fake_client.invoke_model.call_count == 2

    def test_raises_when_retries_exhausted_and_raise_on_error_true(self):
        from bedrock_client import invoke_claude_model

        no_json = _claude_text_body('still not json')
        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(no_json)

        with pytest.raises(ValueError, match='No JSON found'):
            invoke_claude_model(
                inference_profile_id='test-model',
                body={'messages': []},
                return_json_only=True,
                bedrock_client=fake_client,
                retries=0,
                raise_on_error=True,
            )

    def test_returns_none_when_retries_exhausted_and_raise_on_error_false(self):
        from bedrock_client import invoke_claude_model

        no_json = _claude_text_body('still not json')
        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(no_json)

        result = invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=True,
            bedrock_client=fake_client,
            retries=0,
            raise_on_error=False,
        )

        assert result is None

    def test_uses_provided_bedrock_client_does_not_construct_new_one(self):
        """Several call sites inject a pre-configured client (e.g. with
        tuned BotoConfig). The wrapper must not silently replace it."""
        from bedrock_client import invoke_claude_model

        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(_claude_text_body('hi'))

        invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=False,
            bedrock_client=fake_client,
        )

        # The injected client was used (single call), and no other boto3
        # client construction happened on it.
        assert fake_client.invoke_model.call_count == 1

    def test_rate_limiter_consulted_once_per_invoke(self, monkeypatch):
        """The rules-engine wrapper currently calls
        bedrock_rate_limiter.wait_if_needed() before each Bedrock call.
        Lock that in so the refactor doesn't drop it."""
        import bedrock_client

        calls = []
        monkeypatch.setattr(
            bedrock_client.bedrock_rate_limiter,
            'wait_if_needed',
            lambda: calls.append(1),
        )

        fake_client = MagicMock()
        fake_client.invoke_model.return_value = _make_bedrock_response(_claude_text_body('hi'))

        bedrock_client.invoke_claude_model(
            inference_profile_id='test-model',
            body={'messages': []},
            return_json_only=False,
            bedrock_client=fake_client,
        )

        assert calls == [1]


class TestExtractJsonFromClaudeResponse:
    """The JSON extraction helper is currently duplicated across
    admin_api.py and bedrock_client.py. Pin its behaviour so the
    consolidation can swap one for the other safely."""

    def test_empty_content_returns_none(self):
        from bedrock_client import extract_json_from_claude_response
        assert extract_json_from_claude_response({'content': []}) is None
        assert extract_json_from_claude_response({}) is None

    def test_fenced_json_block(self):
        from bedrock_client import extract_json_from_claude_response
        body = _claude_text_body('prefix ```json\n{"a": 1}\n``` suffix')
        assert extract_json_from_claude_response(body) == {'a': 1}

    def test_raw_braces_with_nested_object(self):
        from bedrock_client import extract_json_from_claude_response
        body = _claude_text_body('Here: {"a": 1, "b": {"c": 2}} trailing text')
        assert extract_json_from_claude_response(body) == {'a': 1, 'b': {'c': 2}}

    def test_braces_inside_strings_ignored(self):
        from bedrock_client import extract_json_from_claude_response
        # The brace-counter must skip braces inside string literals so the
        # closing } of the JSON object is found correctly.
        body = _claude_text_body('{"text": "has } a brace", "ok": true}')
        assert extract_json_from_claude_response(body) == {'text': 'has } a brace', 'ok': True}

    def test_no_json_anywhere_returns_none(self):
        from bedrock_client import extract_json_from_claude_response
        body = _claude_text_body('completely free-form prose, no braces')
        assert extract_json_from_claude_response(body) is None

    def test_skips_non_text_content_blocks(self):
        from bedrock_client import extract_json_from_claude_response
        # Tool-use blocks should be ignored — only text blocks contribute.
        body = {
            'content': [
                {'type': 'tool_use', 'id': 'x', 'name': 'run_sql', 'input': {}},
                {'type': 'text', 'text': '{"a": 1}'},
            ]
        }
        assert extract_json_from_claude_response(body) == {'a': 1}
