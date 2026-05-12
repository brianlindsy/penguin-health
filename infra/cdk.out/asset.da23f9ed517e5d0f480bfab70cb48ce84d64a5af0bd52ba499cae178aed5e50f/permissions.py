"""
Granular RBAC for the admin API.

Identity comes from the JWT (email + cognito:groups + custom:organization_id).
Permissions are stored in penguin-health-org-config under
  pk = USER#<email>
  sk = ORG#<org_id>
and loaded per-request via load_permissions(). Super admins (Cognito group
'Admins') bypass all checks. Members default-deny when no record exists.
"""

import time
from datetime import datetime

import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('penguin-health-org-config')

CATEGORIES = ["Intake", "Billing", "Compliance Audit", "Quality Assurance"]
ANALYTICS_PAGES = ["staff_performance", "revenue_analysis"]
VERBS = ("view", "run")

_CACHE_TTL_SECONDS = 60
_perm_cache: dict[tuple[str, str], tuple[float, dict | None]] = {}


def _now() -> float:
    return time.monotonic()


def load_permissions(email: str | None, org_id: str | None) -> dict | None:
    """Return the user's permission record for an org, or None if no record exists."""
    if not email or not org_id:
        return None

    cache_key = (email, org_id)
    cached = _perm_cache.get(cache_key)
    if cached is not None:
        expires_at, value = cached
        if _now() < expires_at:
            return value

    result = table.get_item(Key={'pk': f'USER#{email}', 'sk': f'ORG#{org_id}'})
    item = result.get('Item')
    perms = _normalize(item) if item else None

    _perm_cache[cache_key] = (_now() + _CACHE_TTL_SECONDS, perms)
    return perms


def invalidate_cache(email: str | None = None, org_id: str | None = None) -> None:
    """Drop cache entries. Call after upsert/delete so new perms take effect immediately."""
    if email is None and org_id is None:
        _perm_cache.clear()
        return
    for key in [k for k in _perm_cache if (email is None or k[0] == email)
                and (org_id is None or k[1] == org_id)]:
        _perm_cache.pop(key, None)


def _normalize(item: dict) -> dict:
    """Coerce a DDB item into a clean perms dict with predictable shape."""
    report_perms_raw = item.get('report_permissions') or {}
    report_perms = {
        cat: [v for v in (report_perms_raw.get(cat) or []) if v in VERBS]
        for cat in CATEGORIES
    }
    analytics = [
        page for page in (item.get('analytics_permissions') or [])
        if page in ANALYTICS_PAGES
    ]
    return {
        'email': item.get('email'),
        'organization_id': item.get('organization_id'),
        'role': item.get('role') or 'member',
        'report_permissions': report_perms,
        'analytics_permissions': analytics,
    }


def is_super_admin(claims: dict) -> bool:
    return 'Admins' in (claims.get('groups') or [])


def is_org_admin(claims: dict, org_id: str) -> bool:
    """Super-admin OR member with role=org_admin in the given org."""
    if is_super_admin(claims):
        return True
    perms = load_permissions(claims.get('email'), org_id)
    return bool(perms and perms.get('role') == 'org_admin')


def can_view_category(claims: dict, org_id: str, category: str) -> bool:
    if is_org_admin(claims, org_id):
        return True
    perms = load_permissions(claims.get('email'), org_id)
    if not perms:
        return False
    return 'view' in (perms['report_permissions'].get(category) or [])


def can_run_category(claims: dict, org_id: str, category: str) -> bool:
    if is_org_admin(claims, org_id):
        return True
    perms = load_permissions(claims.get('email'), org_id)
    if not perms:
        return False
    return 'run' in (perms['report_permissions'].get(category) or [])


def viewable_categories(claims: dict, org_id: str) -> set[str]:
    if is_org_admin(claims, org_id):
        return set(CATEGORIES)
    perms = load_permissions(claims.get('email'), org_id)
    if not perms:
        return set()
    return {c for c, verbs in perms['report_permissions'].items() if 'view' in verbs}


def runnable_categories(claims: dict, org_id: str) -> set[str]:
    if is_org_admin(claims, org_id):
        return set(CATEGORIES)
    perms = load_permissions(claims.get('email'), org_id)
    if not perms:
        return set()
    return {c for c, verbs in perms['report_permissions'].items() if 'run' in verbs}


def can_view_analytics(claims: dict, org_id: str, page: str) -> bool:
    if is_org_admin(claims, org_id):
        return True
    perms = load_permissions(claims.get('email'), org_id)
    if not perms:
        return False
    return page in perms['analytics_permissions']


def serialize_for_me_endpoint(claims: dict) -> dict:
    """Shape returned by GET /api/me/permissions for the frontend."""
    if is_super_admin(claims):
        return {
            'is_super_admin': True,
            'role': None,
            'organization_id': None,
            'report_permissions': {cat: list(VERBS) for cat in CATEGORIES},
            'analytics_permissions': list(ANALYTICS_PAGES),
        }
    org_id = claims.get('organization_id')
    perms = load_permissions(claims.get('email'), org_id) if org_id else None
    if not perms:
        return {
            'is_super_admin': False,
            'role': 'member',
            'organization_id': org_id,
            'report_permissions': {cat: [] for cat in CATEGORIES},
            'analytics_permissions': [],
        }
    return {
        'is_super_admin': False,
        'role': perms['role'],
        'organization_id': perms['organization_id'],
        'report_permissions': perms['report_permissions'],
        'analytics_permissions': perms['analytics_permissions'],
    }


def build_user_perm_item(email: str, org_id: str, body: dict, *, existing: dict | None = None) -> dict:
    """Build a USER#<email> / ORG#<org_id> item for PutItem from a request body."""
    role = body.get('role', existing.get('role') if existing else 'member')
    if role not in ('member', 'org_admin'):
        raise ValueError(f"Invalid role: {role!r}")

    raw_report = body.get('report_permissions',
                          existing.get('report_permissions') if existing else {}) or {}
    report_perms = {}
    for cat, verbs in raw_report.items():
        if cat not in CATEGORIES:
            raise ValueError(f"Unknown category: {cat!r}")
        cleaned = [v for v in (verbs or []) if v in VERBS]
        report_perms[cat] = cleaned
    for cat in CATEGORIES:
        report_perms.setdefault(cat, [])

    raw_analytics = body.get('analytics_permissions',
                             existing.get('analytics_permissions') if existing else []) or []
    for page in raw_analytics:
        if page not in ANALYTICS_PAGES:
            raise ValueError(f"Unknown analytics page: {page!r}")

    now = datetime.utcnow().isoformat() + 'Z'
    return {
        'pk': f'USER#{email}',
        'sk': f'ORG#{org_id}',
        'gsi1pk': 'USER_PERM',
        'gsi1sk': f'ORG#{org_id}#USER#{email}',
        'email': email,
        'organization_id': org_id,
        'role': role,
        'report_permissions': report_perms,
        'analytics_permissions': list(raw_analytics),
        'created_at': existing.get('created_at') if existing else now,
        'updated_at': now,
    }
