"""Demo-mode Stedi client.

Wraps the real StediClient and short-circuits requests to canned
fixtures (see demo_fixtures.py) when the patient demographics match a
known scenario. Falls through to the real client for everything else,
so an org running in demo mode can still hit Stedi for unrecognized
patients if needed.

This is opt-in per org via STEDI_CONFIG.demo_mode=true; not gated by an
environment variable so it's visible in the audit log + DynamoDB
without touching infra config.
"""

from . import demo_fixtures


class DemoStediClient:
    """Same interface as StediClient (check_eligibility / check_insurance_discovery).
    Inspects the outbound payload's subscriber demographics; if it matches
    a fixture, returns the canned response. Otherwise delegates to `real`."""

    def __init__(self, real_client):
        self.real_client = real_client

    def check_insurance_discovery(self, payload):
        sub = payload.get('subscriber') or {}
        scenario = demo_fixtures.lookup(
            sub.get('firstName'), sub.get('lastName'), sub.get('dateOfBirth'),
        )
        if scenario is not None and scenario.get('discovery') is not None:
            return scenario['discovery']
        # No fixture matches. If a real client was wired (production-like
        # demo where some patients hit real Stedi), defer to it. Otherwise
        # return an empty discovery so the demo stays self-contained — UR
        # sees "no coverage found" and can rerun with corrected demographics.
        if self.real_client is not None:
            return self.real_client.check_insurance_discovery(payload)
        return _empty_discovery_response()

    def check_eligibility(self, payload):
        sub = payload.get('subscriber') or {}
        payer_id = payload.get('tradingPartnerServiceId')
        member_id = sub.get('memberId')

        # Primary lookup: by patient name+dob (covers both discovery-first
        # callers and direct-path callers for patients in SCENARIOS).
        scenario = demo_fixtures.lookup(
            sub.get('firstName'), sub.get('lastName'), sub.get('dateOfBirth'),
        )
        if scenario is not None:
            builder = (scenario.get('eligibility_by_payer') or {}).get(payer_id)
            if builder is not None:
                return builder(member_id)

        # Fallback: direct-path lookup by (member_id, payer_id). Lets UR
        # type a member ID into the verify form without knowing the patient
        # was in SCENARIOS, or hit eligibility for a discovery-returned
        # member ID that doesn't match the form's typed name.
        direct = demo_fixtures.lookup_direct_eligibility(member_id, payer_id)
        if direct is not None:
            return direct

        # Both lookups missed.
        if scenario is not None:
            # The patient name is known but we have no fixture for this
            # (member_id, payer_id) tuple — return empty rather than calling
            # real Stedi so demo mode stays self-contained.
            return _empty_eligibility_response(payer_id, sub)
        if self.real_client is not None:
            return self.real_client.check_eligibility(payload)
        return _empty_eligibility_response(payer_id, sub)


def _empty_discovery_response():
    return {
        "discoveryId": "demo-empty",
        "coveragesFound": 0,
        "items": [],
        "errors": [],
        "status": "COMPLETE",
        "meta": {"applicationMode": "demo"},
    }


def _empty_eligibility_response(payer_id, sub):
    return {
        "controlNumber": "demo-ctrl-empty",
        "tradingPartnerServiceId": payer_id,
        "subscriber": {
            "firstName": sub.get('firstName'),
            "lastName": sub.get('lastName'),
            "memberId": sub.get('memberId'),
        },
        "planInformation": {},
        "planDateInformation": {},
        "benefitsInformation": [],
    }
