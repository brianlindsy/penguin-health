import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from .exceptions import FhirAuthError
from .fhir_client import FhirClient
from .kms_signer import sign_rs384


_TOKEN_REQUEST_TIMEOUT = 30
_CLIENT_ASSERTION_TYPE = (
    'urn:ietf:params:oauth:client-assertion-type:jwt-bearer'
)
_ASSERTION_LIFETIME_SECONDS = 300  # 5 minutes — well under common server caps


class CredibleFhirClient(FhirClient):
    """
    Credible FHIR R4 client. Authenticates with the token endpoint using
    OAuth2 client_credentials + private_key_jwt client assertion (RS384),
    with the private key held in AWS KMS — `kms:Sign` is the only access
    path. The key bytes never enter this process.
    """

    def authenticate(self):
        token_url = self.config['token_url']
        assertion = self._build_client_assertion(token_url)

        form = {
            'grant_type': 'client_credentials',
            'client_assertion_type': _CLIENT_ASSERTION_TYPE,
            'client_assertion': assertion,
        }
        scopes = self.config.get('scopes') or []
        if scopes:
            form['scope'] = ' '.join(scopes)

        body = urllib.parse.urlencode(form).encode('utf-8')
        request = urllib.request.Request(
            token_url,
            data=body,
            method='POST',
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=_TOKEN_REQUEST_TIMEOUT) as response:
                payload = json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            body = ''
            try:
                body = e.read().decode('utf-8', errors='replace')
            except Exception:
                pass
            raise FhirAuthError(
                f"token request to {token_url} -> {e.code}: {body}"
            ) from e
        except urllib.error.URLError as e:
            raise FhirAuthError(f"token request to {token_url} -> {e.reason}") from e

        access_token = payload.get('access_token')
        expires_in = int(payload.get('expires_in', 3600))
        if not access_token:
            raise FhirAuthError(f"token response missing access_token: {payload}")
        return access_token, expires_in

    def _build_client_assertion(self, token_url):
        client_id = self.credentials['client_id']
        kid = self.credentials['kid']
        kms_key_arn = self.credentials['kms_key_arn']

        now = int(time.time())
        claims = {
            'iss': client_id,
            'sub': client_id,
            'aud': token_url,
            'iat': now,
            'exp': now + _ASSERTION_LIFETIME_SECONDS,
            'jti': str(uuid.uuid4()),
        }
        try:
            return sign_rs384(kms_key_arn=kms_key_arn, kid=kid, claims=claims)
        except Exception as e:
            raise FhirAuthError(f"failed to sign client_assertion via KMS: {e}") from e
