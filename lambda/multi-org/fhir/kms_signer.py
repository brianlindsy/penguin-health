"""
KMS-backed JWT signer.

The private key never leaves AWS. We hand KMS the JWT signing input
(`base64url(header) + "." + base64url(payload)`) and KMS returns the
RS384 signature bytes. We base64url-encode those bytes and append them
to form the final JWT.

This signer matches `jwt.encode(algorithm="RS384")` byte-for-byte from a
verifier's perspective — PyJWT (and any other JWT library) verifies the
output against the corresponding public key exactly the same way.

Production keys live in KMS; tests use moto, which implements the
relevant `Sign` / `GetPublicKey` API with real cryptography.
"""

import base64
import json

import boto3


_KMS_SIGNING_ALGORITHM = 'RSASSA_PKCS1_V1_5_SHA_384'  # RS384 in JWT terms

_kms = boto3.client('kms')


def _b64url(value: bytes) -> bytes:
    return base64.urlsafe_b64encode(value).rstrip(b'=')


def sign_rs384(*, kms_key_arn: str, kid: str, claims: dict) -> str:
    """
    Build an RS384 JWT signed by the KMS-managed private key behind kms_key_arn.

    Args:
        kms_key_arn: KMS key ARN with KeyUsage=SIGN_VERIFY and KeySpec=RSA_4096.
        kid: Key ID stamped in the JWT header. Must match the kid published
             in our JWKS so the verifier can locate the right public key.
        claims: JWT body. Caller owns iss/sub/aud/iat/exp/jti and any other
                claims; this function does not add or remove anything.

    Returns:
        The compact JWT string.

    Raises:
        botocore.exceptions.ClientError if KMS rejects the Sign call
        (e.g. missing kms:Sign permission, disabled key, etc.).
    """
    header = {'alg': 'RS384', 'typ': 'JWT', 'kid': kid}
    header_b64 = _b64url(json.dumps(header, separators=(',', ':')).encode('utf-8'))
    payload_b64 = _b64url(json.dumps(claims, separators=(',', ':')).encode('utf-8'))
    signing_input = header_b64 + b'.' + payload_b64

    response = _kms.sign(
        KeyId=kms_key_arn,
        Message=signing_input,
        MessageType='RAW',
        SigningAlgorithm=_KMS_SIGNING_ALGORITHM,
    )
    signature_b64 = _b64url(response['Signature'])
    return (signing_input + b'.' + signature_b64).decode('ascii')


def get_public_key_der(kms_key_arn: str) -> bytes:
    """Return the SubjectPublicKeyInfo DER bytes for the KMS key's public half."""
    response = _kms.get_public_key(KeyId=kms_key_arn)
    return response['PublicKey']
