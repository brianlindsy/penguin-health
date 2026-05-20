"""
Resolve KMS aliases to key metadata at runtime.

The KMS alias is the single source of truth for "which key signs FHIR
requests for this org." From the alias we derive everything else we need:
  - the key ARN (via kms:DescribeKey)
  - the public key in DER form (via kms:GetPublicKey)
  - the `kid` for our JWT header (RFC 7638 thumbprint of the public key)

Caching: results are cached per-process. KMS keys behind an alias do
change during rotation, but rotation is a coordinated operation that
expects readers to re-resolve eventually. Within a single Lambda
invocation we trust the cache; cold starts re-resolve.
"""

import base64
import hashlib
import json
import threading

import boto3
from cryptography.hazmat.primitives import serialization

from .exceptions import FhirAuthError


_kms = boto3.client('kms')

_cache_lock = threading.Lock()
_cache: dict[str, dict] = {}


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def _int_to_b64url(value: int) -> str:
    byte_length = (value.bit_length() + 7) // 8
    return _b64url(value.to_bytes(byte_length, 'big'))


def _rfc7638_thumbprint(public_key_der: bytes) -> str:
    """Build the RFC 7638 JWK thumbprint (the canonical kid) from a DER public key."""
    public_key = serialization.load_der_public_key(public_key_der)
    numbers = public_key.public_numbers()
    canonical = json.dumps(
        {
            'e': _int_to_b64url(numbers.e),
            'kty': 'RSA',
            'n': _int_to_b64url(numbers.n),
        },
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8')
    return _b64url(hashlib.sha256(canonical).digest())


def resolve_alias(alias: str) -> dict:
    """
    Return `{kms_key_arn, kid}` for the given KMS alias.

    Raises FhirAuthError if the alias doesn't exist, the key isn't usable
    for signing, or we don't have permission.
    """
    with _cache_lock:
        cached = _cache.get(alias)
        if cached is not None:
            return cached

    try:
        described = _kms.describe_key(KeyId=alias)
        public = _kms.get_public_key(KeyId=alias)
    except _kms.exceptions.NotFoundException as e:
        raise FhirAuthError(f"KMS alias {alias} not found") from e
    except Exception as e:
        raise FhirAuthError(f"failed to resolve KMS alias {alias}: {e}") from e

    meta = described['KeyMetadata']
    if meta.get('KeyUsage') != 'SIGN_VERIFY':
        raise FhirAuthError(
            f"KMS key behind {alias} has KeyUsage={meta.get('KeyUsage')}; expected SIGN_VERIFY"
        )

    resolved = {
        'kms_key_arn': meta['Arn'],
        'kid': _rfc7638_thumbprint(public['PublicKey']),
    }
    with _cache_lock:
        _cache[alias] = resolved
    return resolved


def reset_cache_for_tests():
    with _cache_lock:
        _cache.clear()
