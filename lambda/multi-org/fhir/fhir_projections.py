import json
from datetime import datetime, timezone


def _iso_now():
    return datetime.now(timezone.utc).isoformat()


def project_encounter(resource, *, ndjson_s3_key, ndjson_line_no, status='ok'):
    resource = resource or {}
    cls = resource.get('class') or {}
    period = resource.get('period') or {}
    subject = resource.get('subject') or {}
    service_provider = resource.get('serviceProvider') or {}
    return {
        'encounter_id': resource.get('id'),
        'status': resource.get('status'),
        'class_code': cls.get('code'),
        'class_system': cls.get('system'),
        'period_start': period.get('start'),
        'period_end': period.get('end'),
        'subject_reference': subject.get('reference'),
        'service_provider_reference': service_provider.get('reference'),
        'reason_codes_json': json.dumps(resource.get('reasonCode') or []),
        'type_codes_json': json.dumps(resource.get('type') or []),
        'participant_refs_json': json.dumps([
            ((p.get('individual') or {}).get('reference'))
            for p in (resource.get('participant') or [])
        ]),
        'ndjson_s3_key': ndjson_s3_key,
        'ndjson_line_no': ndjson_line_no,
        'fetched_at': _iso_now(),
        'fhir_lookup_status': status,
    }


def empty_encounter_row(encounter_id, *, status):
    return {
        'encounter_id': encounter_id,
        'status': None,
        'class_code': None,
        'class_system': None,
        'period_start': None,
        'period_end': None,
        'subject_reference': None,
        'service_provider_reference': None,
        'reason_codes_json': json.dumps([]),
        'type_codes_json': json.dumps([]),
        'participant_refs_json': json.dumps([]),
        'ndjson_s3_key': None,
        'ndjson_line_no': None,
        'fetched_at': _iso_now(),
        'fhir_lookup_status': status,
    }
