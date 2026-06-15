"""Load per-org RPA config and shared playbooks from penguin-health-org-config.

The RPA_CONFIG item lives alongside FHIR_CONFIG and STEDI_CONFIG so turning
the integration on/off for a customer is a config edit alongside their
other integration settings.

Playbooks are stored separately under sk=RPA_PLAYBOOK#{id} so multiple orgs
on the same vendor can share a playbook by referencing the same id from
their RPA_CONFIG.playbook_id. Playbooks are usually under pk=ORG#shared;
an org-specific override would use pk=ORG#{org_id}, sk=RPA_PLAYBOOK#{id}.
"""

import boto3
from functools import lru_cache

from .exceptions import RpaOrgNotConfigured, RpaPlaybookNotFound

dynamodb = boto3.resource("dynamodb")
_table = dynamodb.Table("penguin-health-org-config")


@lru_cache(maxsize=100)
def load_rpa_config(org_id: str) -> dict:
    response = _table.get_item(Key={"pk": f"ORG#{org_id}", "sk": "RPA_CONFIG"})
    item = response.get("Item")
    if not item:
        raise RpaOrgNotConfigured(f"no RPA_CONFIG for org={org_id}")
    if not item.get("enabled", False):
        raise RpaOrgNotConfigured(f"RPA disabled for org={org_id}")
    return item


@lru_cache(maxsize=100)
def load_playbook(org_id: str, playbook_id: str) -> dict:
    """Resolve a playbook, preferring org-specific override over shared.

    Lookup order:
      1. pk=ORG#{org_id}, sk=RPA_PLAYBOOK#{playbook_id}  (per-org override)
      2. pk=ORG#shared,   sk=RPA_PLAYBOOK#{playbook_id}  (shared)
    """
    sk = f"RPA_PLAYBOOK#{playbook_id}"
    for pk in (f"ORG#{org_id}", "ORG#shared"):
        resp = _table.get_item(Key={"pk": pk, "sk": sk})
        item = resp.get("Item")
        if item:
            return item
    raise RpaPlaybookNotFound(
        f"no RPA_PLAYBOOK#{playbook_id} for org={org_id} or ORG#shared"
    )


def invalidate_cache() -> None:
    load_rpa_config.cache_clear()
    load_playbook.cache_clear()
