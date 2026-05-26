"""Hand-built Stedi response fixtures for the demo workflow.

These are the canned responses the orchestrator returns when an org's
STEDI_CONFIG has `demo_mode: true`. Lets us exercise the full UI path
(form -> discovery -> eligibility -> result card -> copy block) without
spending real Stedi transactions on patients who don't exist.

Shapes match what Stedi actually returns; see
  https://www.stedi.com/docs/healthcare/insurance-discovery

All patient names use obviously-synthetic last names so it's visually
clear in any demo/screenshot that no real patient is involved.

Two lookup paths:
- SCENARIOS, keyed by (first_name_lower, last_name_lower) — for the
  discovery-first workflow (UR types name/DOB/SSN only). Each scenario
  carries a canned `discovery` response and per-payer `eligibility_by_payer`
  responses for any HIGH-confidence hit the discovery returned.
- ELIGIBILITY_DIRECT_SCENARIOS, keyed by (member_id, payer_id) — for the
  direct-path workflow (UR types member_id + payer). DemoStediClient
  falls back to this lookup when the name-based scenario lookup misses
  on `check_eligibility`.
"""

from datetime import date, timedelta


# ---- Eligibility response builders -------------------------------------
# Each builder returns a Stedi 271-shaped dict for one (member, payer)
# pair. All builders here use the same shape; only benefit codes and
# plan-date semantics differ.

def _eligibility_aetna_active(member_id, first, last, dob, *, plan_name="Aetna Choice POS II"):
    return {
        "controlNumber": "demo-ctrl-aetna-active",
        "tradingPartnerServiceId": "60054",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
            "groupNumber": "012345607890008",
        },
        "planInformation": {"planName": plan_name},
        "planDateInformation": {"planBegin": "20260101", "planEnd": "20261231"},
        "benefitsInformation": [
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"]},
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["45", "MH"]},
            {"code": "B", "name": "Co-Payment", "benefitAmount": "50.00",
             "serviceTypeCodes": ["45"], "authOrCertIndicator": "Y"},
            {"code": "B", "name": "Co-Payment", "benefitAmount": "30.00",
             "serviceTypeCodes": ["MH"], "authOrCertIndicator": "Y"},
            {"code": "C", "name": "Deductible", "benefitAmount": "2500.00",
             "inPlanNetworkIndicatorCode": "Y", "timeQualifierCode": "23"},
            {"code": "G", "name": "Out of Pocket (Stop Loss)", "benefitAmount": "8000.00",
             "inPlanNetworkIndicatorCode": "Y"},
        ],
    }


def _eligibility_aetna_inactive(member_id, first, last, dob, *, terminated_days_ago):
    """Plan was active up until N days ago. Used for the 'Inovalon missed
    a recent termination' workflow case."""
    term_date = (date.today() - timedelta(days=terminated_days_ago)).strftime("%Y%m%d")
    return {
        "controlNumber": "demo-ctrl-aetna-inactive",
        "tradingPartnerServiceId": "60054",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
        },
        "planInformation": {"planName": "Aetna Choice POS II"},
        "planDateInformation": {"planBegin": "20240101", "planEnd": term_date},
        "benefitsInformation": [
            {"code": "6", "name": "Inactive", "serviceTypeCodes": ["30"]},
            {"code": "6", "name": "Inactive", "serviceTypeCodes": ["45", "MH"]},
        ],
    }


def _eligibility_humana_active(member_id, first, last, dob):
    return {
        "controlNumber": "demo-ctrl-humana-active",
        "tradingPartnerServiceId": "61101",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
            "groupNumber": "HUM-GRP-77",
        },
        "planInformation": {"planName": "Humana Choice POS"},
        "planDateInformation": {"planBegin": "20260101"},
        "benefitsInformation": [
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"]},
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["45", "MH"]},
            {"code": "B", "name": "Co-Payment", "benefitAmount": "75.00",
             "serviceTypeCodes": ["45"], "authOrCertIndicator": "Y"},
            {"code": "C", "name": "Deductible", "benefitAmount": "3000.00",
             "inPlanNetworkIndicatorCode": "Y", "timeQualifierCode": "23"},
        ],
    }


def _eligibility_medicaid_active(member_id, first, last, dob):
    return {
        "controlNumber": "demo-ctrl-medicaid-active",
        "tradingPartnerServiceId": "68068",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
        },
        "planInformation": {"planName": "Sunshine Health Medicaid Managed Care"},
        "planDateInformation": {"planBegin": "20250701"},
        "benefitsInformation": [
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"]},
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["45", "MH", "AI"]},
            {"code": "B", "name": "Co-Payment", "benefitAmount": "0.00",
             "serviceTypeCodes": ["MH"], "authOrCertIndicator": "Y"},
        ],
    }


def _eligibility_medicaid_auth_required(member_id, first, last, dob):
    """Sunshine Medicaid with prior-auth required for inpatient BH —
    UR has to call to precert before admission."""
    return {
        "controlNumber": "demo-ctrl-medicaid-auth",
        "tradingPartnerServiceId": "68068",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
        },
        "planInformation": {"planName": "Cenpatico Sunshine BH Managed Care"},
        "planDateInformation": {"planBegin": "20250101"},
        "benefitsInformation": [
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"]},
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["45", "MH", "AI"],
             "authOrCertIndicator": "Y"},
            {"code": "B", "name": "Co-Payment", "benefitAmount": "0.00",
             "serviceTypeCodes": ["MH"], "authOrCertIndicator": "Y"},
        ],
    }


def _eligibility_medicare_active(member_id, first, last, dob):
    return {
        "controlNumber": "demo-ctrl-medicare-active",
        "tradingPartnerServiceId": "09101",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
        },
        "planInformation": {"planName": "Medicare Part A & B"},
        "planDateInformation": {"planBegin": "20200101"},
        "benefitsInformation": [
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"]},
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["45"]},
            {"code": "C", "name": "Deductible", "benefitAmount": "1632.00",
             "inPlanNetworkIndicatorCode": "Y", "timeQualifierCode": "27"},
        ],
    }


def _eligibility_cigna_no_bh(member_id, first, last, dob):
    """Cigna plan is active overall but explicitly does NOT cover inpatient
    behavioral health — code I (Non-Covered) on service-types 45/MH/AI."""
    return {
        "controlNumber": "demo-ctrl-cigna-no-bh",
        "tradingPartnerServiceId": "62308",
        "subscriber": {
            "firstName": first,
            "lastName": last,
            "memberId": member_id,
            "dateOfBirth": dob,
            "groupNumber": "CIGNA-RES-12",
        },
        "planInformation": {"planName": "Cigna OAP Limited"},
        "planDateInformation": {"planBegin": "20260101", "planEnd": "20261231"},
        "benefitsInformation": [
            {"code": "1", "name": "Active Coverage", "serviceTypeCodes": ["30"]},
            {"code": "I", "name": "Non-Covered", "serviceTypeCodes": ["45"]},
            {"code": "I", "name": "Non-Covered", "serviceTypeCodes": ["MH"]},
            {"code": "I", "name": "Non-Covered", "serviceTypeCodes": ["AI"]},
        ],
    }


# ---- Discovery responses -----------------------------------------------

_DISCOVERY_JANE_SAMPLE_REVIEW_NEEDED = {
    "coveragesFound": 1,
    "discoveryId": "demo-disc-jane-sample-001",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-001"},
    "items": [
        {
            "provider": {
                "providerName": "THE DOCTORS OFFICE",
                "entityType": "Non-Person Entity",
                "npi": "1999999984",
            },
            "tradingPartnerServiceId": "60054",
            "payer": {
                "entityIdentifier": "Payer",
                "entityType": "Non-Person Entity",
                "name": "Aetna",
                "payorIdentification": "60054",
            },
            "subscriber": {
                "memberId": "J9606211996",
                "firstName": "JOHN",
                "lastName": "SAMPLE",
                "groupNumber": "012345607890008",
                "groupDescription": "SAMPLE HEALTH GROUP",
            },
            "dependent": {
                "firstName": "JANE",
                "lastName": "SMITH SAMPLE",
                "gender": "F",
                "dateOfBirth": "20010925",
                "relationToSubscriber": "Child",
            },
            "confidence": {
                "level": "REVIEW_NEEDED",
                "reason": "Last name mismatch (request: SAMPLE; payer: SMITH SAMPLE) — likely the same patient under a hyphenated/full surname.",
            },
        },
    ],
    "errors": [],
}


_DISCOVERY_ROBERT_TESTPATIENT_TWO_HIGH = {
    "coveragesFound": 2,
    "discoveryId": "demo-disc-robert-testpatient-002",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-002"},
    "items": [
        {
            "tradingPartnerServiceId": "60054",
            "payer": {"name": "Aetna", "payorIdentification": "60054"},
            "subscriber": {
                "memberId": "AETNA12345",
                "firstName": "ROBERT",
                "lastName": "TESTPATIENT",
                "groupNumber": "GRP-AET-91",
            },
            "confidence": {
                "level": "HIGH",
                "reason": "Full demographic match (name + DOB + SSN last 4 + ZIP).",
            },
        },
        {
            "tradingPartnerServiceId": "68068",
            "payer": {"name": "Cenpatico Sunshine State"},
            "subscriber": {
                "memberId": "FLM-3344556677",
                "firstName": "ROBERT",
                "lastName": "TESTPATIENT",
                "groupNumber": "MEDICAID-FL",
            },
            "confidence": {
                "level": "HIGH",
                "reason": "Full demographic match against state Medicaid roster.",
            },
        },
    ],
    "errors": [],
}


_DISCOVERY_NO_COVERAGE_NORA = {
    "coveragesFound": 0,
    "discoveryId": "demo-disc-nora-empty",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-nora"},
    "items": [],
    "errors": [],
}


_DISCOVERY_DANIEL_DEMOSON_INACTIVE = {
    "coveragesFound": 1,
    "discoveryId": "demo-disc-daniel-demoson",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-daniel"},
    "items": [
        {
            "tradingPartnerServiceId": "60054",
            "payer": {"name": "Aetna", "payorIdentification": "60054"},
            "subscriber": {
                "memberId": "AETNA-INACTIVE-001",
                "firstName": "DANIEL",
                "lastName": "DEMOSON",
                "groupNumber": "GRP-AET-LAID-OFF",
            },
            "confidence": {
                "level": "HIGH",
                "reason": "Member found in Aetna roster. Note: eligibility check will reveal recent termination.",
            },
        },
    ],
    "errors": [],
}


_DISCOVERY_LINDA_SANDBOX_HUMANA = {
    "coveragesFound": 1,
    "discoveryId": "demo-disc-linda-sandbox",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-linda"},
    "items": [
        {
            "tradingPartnerServiceId": "61101",
            "payer": {"name": "Humana", "payorIdentification": "61101"},
            "subscriber": {
                "memberId": "HUMANA-FRESH-002",
                "firstName": "LINDA",
                "lastName": "SANDBOX",
                "groupNumber": "HUM-GRP-77",
            },
            "confidence": {
                "level": "HIGH",
                "reason": "Full demographic match. Patient previously on Cigna per prior admission.",
            },
        },
    ],
    "errors": [],
}


_DISCOVERY_NO_COVERAGE_TYLER = {
    "coveragesFound": 0,
    "discoveryId": "demo-disc-tyler-empty",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-tyler"},
    "items": [],
    "errors": [],
}


_DISCOVERY_JAMES_EXAMPLE_CIGNA = {
    "coveragesFound": 1,
    "discoveryId": "demo-disc-james-example",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-james"},
    "items": [
        {
            "tradingPartnerServiceId": "62308",
            "payer": {"name": "Cigna", "payorIdentification": "62308"},
            "subscriber": {
                "memberId": "CIGNA-NO-BH-004",
                "firstName": "JAMES",
                "lastName": "EXAMPLE",
                "groupNumber": "CIGNA-RES-12",
            },
            "confidence": {
                "level": "HIGH",
                "reason": "Demographic match. Note: plan is active but inpatient mental health is non-covered.",
            },
        },
    ],
    "errors": [],
}


_DISCOVERY_SARAH_PLACEHOLDER_AGED_OUT = {
    "coveragesFound": 1,
    "discoveryId": "demo-disc-sarah-placeholder",
    "status": "COMPLETE",
    "meta": {"applicationMode": "demo", "traceId": "demo-trace-sarah"},
    "items": [
        {
            "tradingPartnerServiceId": "60054",
            "payer": {"name": "Aetna", "payorIdentification": "60054"},
            "subscriber": {
                "memberId": "AETNA-AGED-005",
                "firstName": "SARAH",
                "lastName": "PLACEHOLDER",
                "groupNumber": "GRP-AET-PARENT",
            },
            "confidence": {
                "level": "HIGH",
                "reason": "Member found on dependent roster. Note: aged out 2 days ago — eligibility will show inactive.",
            },
        },
    ],
    "errors": [],
}


# ---- Scenario registry (discovery-first lookup) ------------------------
# Keyed by (first_name lower, last_name lower). DOB must also match the
# fixture's expected DOB or the orchestrator falls through to "no_coverage".

SCENARIOS = {
    ("jane", "sample"): {
        "expected_dob": "20010925",
        "summary": "Jane Sample — dependent on father's Aetna; REVIEW_NEEDED (last-name mismatch SMITH SAMPLE vs SAMPLE)",
        "discovery": _DISCOVERY_JANE_SAMPLE_REVIEW_NEEDED,
        # REVIEW_NEEDED hits don't trigger follow-up eligibility calls.
        "eligibility_by_payer": {},
    },
    ("robert", "testpatient"): {
        "expected_dob": "19780214",
        "summary": "Robert Testpatient — Aetna commercial primary + FL Medicaid secondary (the messy multi-payer case)",
        "discovery": _DISCOVERY_ROBERT_TESTPATIENT_TWO_HIGH,
        "eligibility_by_payer": {
            "60054": lambda m: _eligibility_aetna_active(m, "ROBERT", "TESTPATIENT", "19780214"),
            "68068": lambda m: _eligibility_medicaid_active(m, "ROBERT", "TESTPATIENT", "19780214"),
        },
    },
    ("maria", "mockerson"): {
        "expected_dob": "19550630",
        "summary": "Maria Mockerson — Medicare Part A/B FFS via MBI lookup (direct-path: known from prior admit)",
        # Direct-path patient: no discovery block needed.
        "discovery": None,
        "eligibility_by_payer": {
            "09101": lambda m: _eligibility_medicare_active(m, "MARIA", "MOCKERSON", "19550630"),
        },
    },
    ("nora", "faker"): {
        "expected_dob": "19900101",
        "summary": "Nora Faker — Stedi returned coveragesFound=0 (use to test the empty-result UI path)",
        "discovery": _DISCOVERY_NO_COVERAGE_NORA,
        "eligibility_by_payer": {},
    },
    ("daniel", "demoson"): {
        "expected_dob": "19850712",
        "summary": "Daniel Demoson — Aetna terminated 12 days ago (recent-inactivation discrepancy)",
        "discovery": _DISCOVERY_DANIEL_DEMOSON_INACTIVE,
        "eligibility_by_payer": {
            "60054": lambda m: _eligibility_aetna_inactive(m, "DANIEL", "DEMOSON", "19850712",
                                                            terminated_days_ago=12),
        },
    },
    ("linda", "sandbox"): {
        "expected_dob": "19620818",
        "summary": "Linda Sandbox — primary changed from Cigna to Humana (driven by seeded audit history)",
        "discovery": _DISCOVERY_LINDA_SANDBOX_HUMANA,
        "eligibility_by_payer": {
            "61101": lambda m: _eligibility_humana_active(m, "LINDA", "SANDBOX", "19620818"),
        },
    },
    ("tyler", "fixture"): {
        "expected_dob": "20140315",
        "summary": "Tyler Fixture — pediatric, no insurance info (Dawn calls parent)",
        "discovery": _DISCOVERY_NO_COVERAGE_TYLER,
        "eligibility_by_payer": {},
    },
    ("patricia", "stub"): {
        "expected_dob": "19710505",
        "summary": "Patricia Stub — Sunshine FL Medicaid with auth required (direct-path: returning patient)",
        # Direct-path patient.
        "discovery": None,
        "eligibility_by_payer": {
            "68068": lambda m: _eligibility_medicaid_auth_required(m, "PATRICIA", "STUB", "19710505"),
        },
    },
    ("james", "example"): {
        "expected_dob": "19831120",
        "summary": "James Example — Cigna active overall but inpatient BH is non-covered (service-type denied)",
        "discovery": _DISCOVERY_JAMES_EXAMPLE_CIGNA,
        "eligibility_by_payer": {
            "62308": lambda m: _eligibility_cigna_no_bh(m, "JAMES", "EXAMPLE", "19831120"),
        },
    },
    ("sarah", "placeholder"): {
        "expected_dob": "20030414",
        "summary": "Sarah Placeholder — aged out at 26, Aetna inactive 2 days ago",
        "discovery": _DISCOVERY_SARAH_PLACEHOLDER_AGED_OUT,
        "eligibility_by_payer": {
            "60054": lambda m: _eligibility_aetna_inactive(m, "SARAH", "PLACEHOLDER", "20030414",
                                                            terminated_days_ago=2),
        },
    },
}


# ---- Direct-path eligibility lookup ------------------------------------
# Keyed by (member_id, payer_id). DemoStediClient.check_eligibility falls
# back to this when the patient-name lookup misses, so the synchronous
# verify-patient form returns canned data when the user types in a member
# ID + payer directly (rather than discovering them).
#
# Tyler Fixture and Nora Faker are intentionally absent — both are "no
# info / no coverage" cases with no member_id to type into the form.

ELIGIBILITY_DIRECT_SCENARIOS = {
    # Jane Sample's father's Aetna plan (the subscriber discovery returned).
    ("J9606211996", "60054"): lambda: _eligibility_aetna_active(
        "J9606211996", "JOHN", "SAMPLE", "19700401"),
    # Robert Testpatient — Aetna primary.
    ("AETNA12345", "60054"): lambda: _eligibility_aetna_active(
        "AETNA12345", "ROBERT", "TESTPATIENT", "19780214"),
    # Robert Testpatient — FL Medicaid secondary (Cenpatico).
    ("FLM-3344556677", "68068"): lambda: _eligibility_medicaid_active(
        "FLM-3344556677", "ROBERT", "TESTPATIENT", "19780214"),
    # Maria Mockerson — Medicare MBI.
    ("1AB2-CD3-EF45", "09101"): lambda: _eligibility_medicare_active(
        "1AB2-CD3-EF45", "MARIA", "MOCKERSON", "19550630"),
    # Daniel Demoson — recently inactivated Aetna.
    ("AETNA-INACTIVE-001", "60054"): lambda: _eligibility_aetna_inactive(
        "AETNA-INACTIVE-001", "DANIEL", "DEMOSON", "19850712", terminated_days_ago=12),
    # Linda Sandbox — current Humana (primary-changed-from-Cigna case).
    ("HUMANA-FRESH-002", "61101"): lambda: _eligibility_humana_active(
        "HUMANA-FRESH-002", "LINDA", "SANDBOX", "19620818"),
    # Patricia Stub — Sunshine Medicaid with auth required.
    ("SUNSHINE-003", "68068"): lambda: _eligibility_medicaid_auth_required(
        "SUNSHINE-003", "PATRICIA", "STUB", "19710505"),
    # James Example — Cigna active but no BH coverage.
    ("CIGNA-NO-BH-004", "62308"): lambda: _eligibility_cigna_no_bh(
        "CIGNA-NO-BH-004", "JAMES", "EXAMPLE", "19831120"),
    # Sarah Placeholder — aged out of Aetna.
    ("AETNA-AGED-005", "60054"): lambda: _eligibility_aetna_inactive(
        "AETNA-AGED-005", "SARAH", "PLACEHOLDER", "20030414", terminated_days_ago=2),
}


# ---- Census roster: ordered list of patients the scheduled run hits ----
# Each entry mirrors what would later arrive via FHIR encounter feed or
# SFTP census upload. The census_runner loops over this list and calls
# orchestrator.verify() for each. Patients with `member_id` + `payer_id`
# take the direct path; others go through discovery first.
#
# Order is stable so the demo run's worklist always renders the same.

CENSUS_ROSTER = [
    # Discovery-first patients (intake captured demographics but no member ID).
    # Each entry mirrors what intake would actually have collected: name,
    # DOB, gender, last 4 of SSN, address. This is the input UR can edit
    # and rerun if discovery misses or returns REVIEW_NEEDED.
    {
        "first_name": "Jane", "middle_name": "A", "last_name": "Sample", "suffix": None,
        "dob": "20010925", "gender": "F", "ssn_last4": "4421",
        "address1": "812 Palm Ave", "address2": None,
        "city": "Tallahassee", "state": "FL", "postal_code": "32301",
    },
    {
        "first_name": "Robert", "middle_name": "J", "last_name": "Testpatient", "suffix": "Jr",
        "dob": "19780214", "gender": "M", "ssn_last4": "8812",
        "address1": "44 Oak Ridge Dr", "address2": "Apt 3B",
        "city": "Pensacola", "state": "FL", "postal_code": "32503",
    },
    {
        "first_name": "Nora", "middle_name": None, "last_name": "Faker", "suffix": None,
        "dob": "19900101", "gender": "F", "ssn_last4": None,  # intake couldn't get SSN
        "address1": None, "address2": None,                    # no address on file either
        "city": None, "state": "FL", "postal_code": None,
    },
    {
        "first_name": "Daniel", "middle_name": "P", "last_name": "Demoson", "suffix": None,
        "dob": "19850712", "gender": "M", "ssn_last4": "2255",
        "address1": "910 Magnolia St", "address2": None,
        "city": "Jacksonville", "state": "FL", "postal_code": "32202",
    },
    {
        "first_name": "Linda", "middle_name": "M", "last_name": "Sandbox", "suffix": None,
        "dob": "19620818", "gender": "F", "ssn_last4": "6677",
        "address1": "27 Bayshore Ct", "address2": None,
        "city": "Tampa", "state": "FL", "postal_code": "33606",
    },
    {
        "first_name": "Tyler", "middle_name": None, "last_name": "Fixture", "suffix": None,
        "dob": "20140315", "gender": "M", "ssn_last4": None,   # minor — parent has SSN
        "address1": None, "address2": None,
        "city": None, "state": None, "postal_code": None,
    },
    {
        "first_name": "James", "middle_name": "K", "last_name": "Example", "suffix": None,
        "dob": "19831120", "gender": "M", "ssn_last4": "3344",
        "address1": "1200 Beachside Blvd", "address2": "Unit 12",
        "city": "Miami", "state": "FL", "postal_code": "33139",
    },
    {
        "first_name": "Sarah", "middle_name": "E", "last_name": "Placeholder", "suffix": None,
        "dob": "20030414", "gender": "F", "ssn_last4": "9988",
        "address1": "55 College Way", "address2": None,
        "city": "Gainesville", "state": "FL", "postal_code": "32601",
    },
    # Direct-path patients (returning, member ID + payer already on file).
    {
        "first_name": "Maria", "middle_name": "T", "last_name": "Mockerson", "suffix": None,
        "dob": "19550630", "gender": "F", "ssn_last4": "1133",
        "address1": "7 Sunset Pl", "address2": None,
        "city": "Naples", "state": "FL", "postal_code": "34102",
        "member_id": "1AB2-CD3-EF45", "payer_id": "09101",
    },
    {
        "first_name": "Patricia", "middle_name": None, "last_name": "Stub", "suffix": None,
        "dob": "19710505", "gender": "F", "ssn_last4": "5566",
        "address1": "303 River Rd", "address2": None,
        "city": "Orlando", "state": "FL", "postal_code": "32801",
        "member_id": "SUNSHINE-003", "payer_id": "68068",
    },
]


# Demographic fields that may be present on a roster entry / census item.
# Used to enforce schema consistency on rerun and on the UI.
DEMOGRAPHIC_FIELDS = (
    "first_name", "middle_name", "last_name", "suffix",
    "dob", "gender", "ssn_last4",
    "address1", "address2", "city", "state", "postal_code",
)


# Linda Sandbox's "primary changed" discrepancy needs a prior audit row
# showing Cigna so the orchestrator's _derive_discrepancies fires.
# Seeded once by census_runner._ensure_demo_history_seeds.
DEMO_HISTORY_SEEDS = [
    {
        "first_name": "Linda", "last_name": "Sandbox", "dob": "19620818",
        "payer_name": "Cigna", "payer_id": "62308",
        # Must be within the orchestrator's _DISCREPANCY_LOOKBACK_DAYS (30)
        # so the "primary changed" discrepancy actually fires.
        "days_ago": 25,
        "call_type": "eligibility",
        "result_status": "active",
    },
]


def lookup(first_name, last_name, dob):
    """Return the matching scenario dict, or None if no fixture matches.

    Match is case-insensitive on name and exact on DOB. None means "fall
    through and call real Stedi" — useful so an operator can still hit
    Stedi for a real patient even with demo_mode enabled.
    """
    key = ((first_name or "").strip().lower(), (last_name or "").strip().lower())
    scenario = SCENARIOS.get(key)
    if not scenario:
        return None
    if dob and scenario.get("expected_dob") and dob != scenario["expected_dob"]:
        return None
    return scenario


def lookup_direct_eligibility(member_id, payer_id):
    """Return a canned eligibility response for the given (member_id, payer_id),
    or None if no fixture matches."""
    builder = ELIGIBILITY_DIRECT_SCENARIOS.get((member_id, payer_id))
    return builder() if builder else None


def list_scenarios():
    """Returns a UI-facing list (used by /eligibility/config when demo_mode
    is on). Order is stable for screenshot reproducibility."""
    return [
        {
            "first_name": k[0].title(),
            "last_name": k[1].title(),
            "dob": v["expected_dob"],
            "summary": v["summary"],
        }
        for k, v in SCENARIOS.items()
    ]
