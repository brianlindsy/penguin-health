"""Load per-org Stedi config from the existing penguin-health-org-config table.

The STEDI_CONFIG item is co-located with FHIR_CONFIG so turning Stedi on/off
for a customer is a config edit alongside their other integration settings.
The high-volume audit log lives on a separate table (penguin-health-stedi).
"""

import boto3
from functools import lru_cache

from .exceptions import StediOrgNotConfigured

dynamodb = boto3.resource('dynamodb')
_table = dynamodb.Table('penguin-health-org-config')


@lru_cache(maxsize=100)
def load_stedi_config(org_id):
    response = _table.get_item(Key={'pk': f'ORG#{org_id}', 'sk': 'STEDI_CONFIG'})
    item = response.get('Item')
    if not item:
        raise StediOrgNotConfigured(f"no STEDI_CONFIG for org={org_id}")
    if not item.get('enabled', False):
        raise StediOrgNotConfigured(f"Stedi disabled for org={org_id}")
    return item


def invalidate_cache():
    load_stedi_config.cache_clear()
