"""Stedi API key fetcher.

One Stedi account serves all penguin-health orgs, so the bearer key is a
single Secrets Manager secret. Cached for the Lambda lifetime — rotation
will cycle Lambda containers naturally; force a refresh by redeploying.
"""

import json
import os

import boto3

_SECRET_NAME = os.environ.get('STEDI_API_KEY_SECRET', 'penguin-health/stedi/api-key')
_secrets_client = boto3.client('secretsmanager')
_cached_key = None


def get_stedi_api_key():
    global _cached_key
    if _cached_key is not None:
        return _cached_key
    response = _secrets_client.get_secret_value(SecretId=_SECRET_NAME)
    payload = json.loads(response['SecretString'])
    _cached_key = payload['api_key']
    return _cached_key


def _reset_for_tests():
    global _cached_key
    _cached_key = None
