"""Build the right Stedi client for an org.

Both the synchronous verify-patient endpoint and the scheduled census
runner need the same client-selection logic: if demo_mode is on, wrap
the real client (or no client at all) in DemoStediClient; otherwise
hand back a plain StediClient with the shared API key.

Extracted from eligibility_api.py to keep the choice in one place.
"""

from . import secrets
from .demo_client import DemoStediClient
from .stedi_client import StediClient


def build_client(org_config, *, client_ip=None):
    """Return an object with the StediClient surface area
    (check_eligibility, check_insurance_discovery).

    `org_config` is the STEDI_CONFIG item from DynamoDB. In demo mode we
    skip the Secrets Manager fetch entirely so a misconfigured key can't
    fail a demo run.
    """
    if org_config.get('demo_mode'):
        return DemoStediClient(real_client=None)
    api_key = secrets.get_stedi_api_key()
    return StediClient(api_key, client_ip=client_ip)
