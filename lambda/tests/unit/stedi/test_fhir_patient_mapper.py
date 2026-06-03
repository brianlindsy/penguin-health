"""Tests for the FHIR Patient -> orchestrator.verify input mapper.

Pure-function tests; no AWS, no fixtures.
"""

import pytest

from stedi import fhir_patient_mapper


def _patient(**overrides):
    base = {
        "resourceType": "Patient",
        "id": "p-1",
        "name": [{"use": "official", "family": "Doe", "given": ["Jane"]}],
        "birthDate": "1985-07-12",
        "gender": "female",
        "address": [{"use": "home", "line": ["1 Main St"], "city": "Tampa",
                     "state": "FL", "postalCode": "33606"}],
    }
    base.update(overrides)
    return base


def test_basic_patient_maps_required_fields():
    result = fhir_patient_mapper.to_verify_input(_patient())
    assert result['first_name'] == 'Jane'
    assert result['last_name'] == 'Doe'
    assert result['dob'] == '19850712'
    assert result['gender'] == 'F'
    assert result['address1'] == '1 Main St'
    assert result['city'] == 'Tampa'
    assert result['state'] == 'FL'
    assert result['postal_code'] == '33606'
    # Defaults for missing optional fields
    assert result['middle_name'] is None
    assert result['suffix'] is None
    assert result['ssn'] is None
    assert result['ssn_last4'] is None
    assert result['address2'] is None
    assert result['member_id'] is None
    assert result['payer_id'] is None


@pytest.mark.parametrize("birth_date,expected", [
    ("1985-07-12", "19850712"),
    ("2001-01-01", "20010101"),
    ("19850712", "19850712"),
    ("1985-7-12", None),    # malformed (missing zero-pad)
    ("not-a-date", None),
    ("", None),
    (None, None),
])
def test_birth_date_normalization(birth_date, expected):
    patient = _patient(birthDate=birth_date)
    assert fhir_patient_mapper.to_verify_input(patient)['dob'] == expected


@pytest.mark.parametrize("fhir_gender,expected", [
    ("male", "M"),
    ("female", "F"),
    ("MALE", "M"),    # case-insensitive
    ("other", None),
    ("unknown", None),
    ("", None),
    (None, None),
])
def test_gender_normalization(fhir_gender, expected):
    patient = _patient(gender=fhir_gender)
    assert fhir_patient_mapper.to_verify_input(patient)['gender'] == expected


def test_prefers_official_name_when_multiple_uses_present():
    patient = _patient(name=[
        {"use": "nickname", "family": "Smith", "given": ["Janie"]},
        {"use": "official", "family": "Smith-Jones", "given": ["Jane", "Marie"]},
        {"use": "old", "family": "Old", "given": ["X"]},
    ])
    result = fhir_patient_mapper.to_verify_input(patient)
    assert result['first_name'] == 'Jane'
    assert result['middle_name'] == 'Marie'
    assert result['last_name'] == 'Smith-Jones'


def test_falls_back_to_first_name_when_no_official():
    patient = _patient(name=[
        {"use": "nickname", "family": "Smith", "given": ["Janie"]},
    ])
    result = fhir_patient_mapper.to_verify_input(patient)
    assert result['first_name'] == 'Janie'
    assert result['last_name'] == 'Smith'


def test_suffix_extracted():
    patient = _patient(name=[
        {"use": "official", "family": "Doe", "given": ["John"], "suffix": ["Jr"]},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['suffix'] == 'Jr'


def test_address_line_split_into_address1_and_address2():
    patient = _patient(address=[
        {"use": "home", "line": ["44 Oak Ridge Dr", "Apt 3B"], "city": "Pensacola",
         "state": "FL", "postalCode": "32503"},
    ])
    result = fhir_patient_mapper.to_verify_input(patient)
    assert result['address1'] == '44 Oak Ridge Dr'
    assert result['address2'] == 'Apt 3B'


def test_address_prefers_home_over_billing_over_first():
    patient = _patient(address=[
        {"use": "billing", "line": ["BIL"], "city": "Bil"},
        {"use": "home", "line": ["HOME"], "city": "Hom"},
        {"use": "work", "line": ["WRK"], "city": "Wrk"},
    ])
    result = fhir_patient_mapper.to_verify_input(patient)
    assert result['address1'] == 'HOME'
    assert result['city'] == 'Hom'


def test_address_skips_old_when_alternative_exists():
    patient = _patient(address=[
        {"use": "old", "line": ["OLD"], "city": "OldCity"},
        {"use": "home", "line": ["NEW"], "city": "NewCity"},
    ])
    result = fhir_patient_mapper.to_verify_input(patient)
    assert result['address1'] == 'NEW'


def test_address_only_old_used_when_only_option():
    patient = _patient(address=[
        {"use": "old", "line": ["OLD"], "city": "OldCity"},
    ])
    result = fhir_patient_mapper.to_verify_input(patient)
    assert result['address1'] == 'OLD'


def test_ssn_last4_from_us_ssn_identifier():
    patient = _patient(identifier=[
        {"system": "http://hl7.org/fhir/sid/us-ssn", "value": "123-45-6789"},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['ssn_last4'] == '6789'


def test_ssn_last4_handles_no_dashes():
    patient = _patient(identifier=[
        {"system": "http://hl7.org/fhir/sid/us-ssn", "value": "123456789"},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['ssn_last4'] == '6789'


def test_ssn_absent_returns_none():
    patient = _patient(identifier=[
        {"system": "http://hospital.example.org/mrn", "value": "MRN-12"},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['ssn_last4'] is None


def test_member_id_from_mb_typed_identifier():
    patient = _patient(identifier=[
        {"type": {"coding": [{"code": "MB"}]}, "value": "MEM-1234"},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['member_id'] == 'MEM-1234'


def test_member_id_prefers_mb_over_mr():
    patient = _patient(identifier=[
        {"type": {"coding": [{"code": "MR"}]}, "value": "MRN-99"},
        {"type": {"coding": [{"code": "MB"}]}, "value": "MEM-1234"},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['member_id'] == 'MEM-1234'


def test_member_id_falls_back_to_mr():
    patient = _patient(identifier=[
        {"type": {"coding": [{"code": "MR"}]}, "value": "MRN-99"},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['member_id'] == 'MRN-99'


def test_handles_minimal_patient_without_keyerror():
    result = fhir_patient_mapper.to_verify_input({"resourceType": "Patient", "id": "x"})
    assert result['first_name'] is None
    assert result['last_name'] is None
    assert result['dob'] is None
    assert result['address1'] is None
    assert result['payer_id'] is None


def test_handles_none_input():
    result = fhir_patient_mapper.to_verify_input(None)
    assert result['first_name'] is None
    assert result['payer_id'] is None


def test_encounter_argument_does_not_affect_output_today():
    """payer_id wiring via Encounter is reserved for a follow-up; callers
    should get the same dict whether or not they pass an encounter."""
    patient = _patient()
    encounter = {"resourceType": "Encounter", "id": "e-1",
                 "subject": {"reference": "Patient/p-1"}}
    assert fhir_patient_mapper.to_verify_input(patient) == \
        fhir_patient_mapper.to_verify_input(patient, encounter)


def test_payer_id_always_none_for_v1():
    """Explicit assertion so a future implementer who flips this on has a
    test that breaks (forcing them to update callers and add coverage)."""
    patient = _patient(extension=[
        {"url": "http://hl7.org/fhir/StructureDefinition/patient-coverage",
         "valueReference": {"reference": "Coverage/c-1"}},
    ])
    assert fhir_patient_mapper.to_verify_input(patient)['payer_id'] is None
