"""Static mapping between Stedi tradingPartnerServiceId values and the
friendly payer names the Catholic Charities team uses day-to-day.

Trading-partner IDs are sourced from Stedi's payer catalog. Seeded with
the payers the UR team actually hits; extend as new orgs come online.
"""

# Canonical name (what we display + what we accept from user input) →
# Stedi trading partner ID.
PAYERS = [
    {"id": "68069",  "name": "AmBetter - Centene / Children's Medical Services MCD", "aliases": ["ambetter - centene", "children's medical services mcd"]},
    {"id": "87726",  "name": "UBH Comm & MCR / UBH Medicaid",                        "aliases": ["ubh comm & mcr", "ubh medicaid"]},
    {"id": "68068",  "name": "Cenpatico Sunshine State",                             "aliases": ["cenpatico sunshine state"]},
    {"id": "BCBSF",  "name": "Blue Cross Blue Shield",                               "aliases": ["blue cross blue shield"]},
    {"id": "95092",  "name": "Careplus",                                             "aliases": ["careplus"]},
    {"id": "09101",  "name": "Medicare A and B / Medicare AB No psych days / Medicare A Only", "aliases": ["medicare a and b", "medicare ab no psych days", "medicare a only"]},
    {"id": "SMPLY",  "name": "Simply Medicaid",                                      "aliases": ["simply medicaid"]},
    {"id": "61101",  "name": "Humana MCD / Humana MCR",                              "aliases": ["humana mcd", "humana mcr"]},
    {"id": "ABH01",  "name": "Aetna Better Hlth MCD",                                "aliases": ["aetna better hlth mcd"]},
    {"id": "77027",  "name": "Medicaid Full / Medicaid Medically Needy",             "aliases": ["medicaid full", "medicaid medically needy"]},
    {"id": "01260",  "name": "Magellan Devoted Health",                              "aliases": ["magellan devoted health"]},
    {"id": "59064",  "name": "Community Care Plan S FL MCD",                         "aliases": ["community care plan s fl mcd"]},
    {"id": "99727",  "name": "Tricare East",                                         "aliases": ["tricare east"]},
    {"id": "12115",  "name": "VA fee basis",                                         "aliases": ["va fee basis"]},
    {"id": "60054",  "name": "Aetna Comm & MCR",                                     "aliases": ["aetna comm & mcr"]},
    {"id": "59322",  "name": "Florida Health Care Plans",                            "aliases": ["florida health care plans"]},
    {"id": "62308",  "name": "Cigna Behavioral Health",                              "aliases": ["cigna behavioral health"]},
    {"id": "41212",  "name": "Freedom Medicare",                                     "aliases": ["freedom medicare"]},
    {"id": "81040",  "name": "Allegiance Benefit Plan",                              "aliases": ["allegiance benefit plan"]},
    {"id": "14163",  "name": "Wellcare MCR & COM / Wellcare Dual Eligible",          "aliases": ["wellcare mcr & com", "wellcare dual eligible"]},
    {"id": "128FL",  "name": "Aetna Better Health Healthy Kids",                     "aliases": ["aetna better health healthy kids"]},
    {"id": "25463",  "name": "Surest United Healthcare",                             "aliases": ["surest united healthcare"]},
]


_BY_ID = {p["id"]: p for p in PAYERS}
_BY_ALIAS = {}
for p in PAYERS:
    _BY_ALIAS[p["name"].lower()] = p
    for alias in p["aliases"]:
        _BY_ALIAS[alias.lower()] = p


def lookup_by_id(trading_partner_id):
    """Return the canonical payer dict for a Stedi ID, or a stub with
    payer_name_unknown=True if we have no mapping. Never raises."""
    p = _BY_ID.get(trading_partner_id)
    if p:
        return {"id": p["id"], "name": p["name"], "payer_name_unknown": False}
    return {"id": trading_partner_id, "name": trading_partner_id, "payer_name_unknown": True}


def lookup_by_user_input(text):
    """Resolve a user-typed payer name to a canonical entry. Case-insensitive
    exact match against name or any alias. Returns None if unresolved so the
    caller can fall back to discovery."""
    if not text:
        return None
    return _BY_ALIAS.get(text.strip().lower())


def list_all():
    return [{"id": p["id"], "name": p["name"]} for p in PAYERS]
