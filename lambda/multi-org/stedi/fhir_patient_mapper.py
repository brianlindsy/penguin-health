"""Map a FHIR R4 Patient resource into the dict shape orchestrator.verify
expects. Pure functions only — no boto, no network, easy to unit-test.

The output keys match the `input_` dict documented on
orchestrator.verify (first_name, last_name, dob YYYYMMDD, gender 'M'/'F',
ssn_last4, address1/address2/city/state/postal_code, member_id, payer_id).

The fhir_encounter argument is reserved for future Coverage extraction via
Encounter.account or Encounter.insurance — today it's unused and returns
None for payer_id. Kept on the signature so callers don't have to refactor
when that wires up.
"""

import re


_SSN_SYSTEM = 'http://hl7.org/fhir/sid/us-ssn'
_GENDER_MAP = {'male': 'M', 'female': 'F'}
# Identifier type codes used by source EMRs to mark insurance member ids
# and medical record numbers. 'MB' (Member Number) is what most payer
# integrations use; 'MR' (Medical Record Number) is a fallback for EMRs
# that don't distinguish — we prefer MB when both are present.
_MEMBER_ID_CODES = ('MB', 'MR')


def to_verify_input(fhir_patient, fhir_encounter=None):
    """Return the orchestrator.verify input dict for the given FHIR Patient.

    Missing fields are returned as None — orchestrator.verify validates
    first_name/last_name/dob at the boundary and rejects the request, so
    we don't gate here.
    """
    patient = fhir_patient or {}
    name = _official_or_first(patient.get('name') or [])
    given = name.get('given') or []
    suffix = name.get('suffix') or []

    address = _preferred_address(patient.get('address') or [])
    lines = address.get('line') or []

    identifiers = patient.get('identifier') or []

    return {
        'first_name': given[0] if given else None,
        'middle_name': given[1] if len(given) > 1 else None,
        'last_name': name.get('family'),
        'suffix': suffix[0] if suffix else None,
        'dob': _normalize_birth_date(patient.get('birthDate')),
        'gender': _GENDER_MAP.get((patient.get('gender') or '').lower()),
        'ssn': None,
        'ssn_last4': _extract_ssn_last4(identifiers),
        'address1': lines[0] if lines else None,
        'address2': lines[1] if len(lines) > 1 else None,
        'city': address.get('city'),
        'state': address.get('state'),
        'postal_code': address.get('postalCode'),
        'member_id': _extract_member_id(identifiers),
        'payer_id': None,
    }


def _official_or_first(names):
    """Pick HumanName with use='official', else first entry, else {}."""
    for n in names:
        if n.get('use') == 'official':
            return n
    return names[0] if names else {}


def _preferred_address(addresses):
    """Pick Address with use='home', then 'billing', skipping 'old' if a
    non-old entry exists. Falls back to the first entry."""
    if not addresses:
        return {}
    non_old = [a for a in addresses if a.get('use') != 'old'] or addresses
    for use in ('home', 'billing'):
        for a in non_old:
            if a.get('use') == use:
                return a
    return non_old[0]


def _normalize_birth_date(birth_date):
    """FHIR birthDate is YYYY-MM-DD; orchestrator.verify expects YYYYMMDD.
    Returns None on missing/malformed input rather than raising."""
    if not birth_date or not isinstance(birth_date, str):
        return None
    digits = birth_date.replace('-', '')
    if len(digits) != 8 or not digits.isdigit():
        return None
    return digits


def _extract_ssn_last4(identifiers):
    for ident in identifiers:
        if ident.get('system') == _SSN_SYSTEM:
            value = ident.get('value') or ''
            digits = re.sub(r'\D', '', value)
            if len(digits) >= 4:
                return digits[-4:]
    return None


def _extract_member_id(identifiers):
    """Prefer the first identifier with type.coding[].code == 'MB' (member
    number); fall back to 'MR' (medical record number) only if no MB
    exists. Returns None if no typed identifier is present."""
    by_code = {}
    for ident in identifiers:
        type_codings = ((ident.get('type') or {}).get('coding')) or []
        for coding in type_codings:
            code = coding.get('code')
            if code in _MEMBER_ID_CODES and code not in by_code:
                value = ident.get('value')
                if value:
                    by_code[code] = value
    for code in _MEMBER_ID_CODES:
        if code in by_code:
            return by_code[code]
    return None
