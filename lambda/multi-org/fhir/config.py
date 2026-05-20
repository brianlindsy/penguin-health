import boto3
from functools import lru_cache

from .exceptions import FhirOrgNotConfigured

dynamodb = boto3.resource('dynamodb')
_table = dynamodb.Table('penguin-health-org-config')


@lru_cache(maxsize=100)
def load_fhir_config(org_id):
    response = _table.get_item(Key={'pk': f'ORG#{org_id}', 'sk': 'FHIR_CONFIG'})
    item = response.get('Item')
    if not item:
        raise FhirOrgNotConfigured(f"no FHIR_CONFIG for org={org_id}")
    if not item.get('enabled', False):
        raise FhirOrgNotConfigured(f"FHIR disabled for org={org_id}")
    return item


def has_encounter_mapping(fhir_config):
    return bool((fhir_config.get('fhir_mappings') or {}).get('encounter'))
