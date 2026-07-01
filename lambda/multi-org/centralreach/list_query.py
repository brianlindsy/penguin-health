"""Billing list endpoint — `POST /crxapi/internal/billing/query`.

Wraps the validator-required body shape (see the design doc's "List
query — required body shape" subsection) and parses the response into
a typed `BillingListResponse`. The runner iterates pages via
`paginate_billing_entries()` until the list returns no more items.

The full UI request body has ~200 fields, most empty-string UI state.
We send only the eight fields the server-side validator requires.
Operators discover additional required fields if CR's validator names
them via `responseStatus.errors[].fieldName` — the client raises
`CentralReachValidationError` carrying the name in that case.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterator

from .client import CentralReachClient


_LIST_QUERY_PATH = "/crxapi/internal/billing/query"
_DEFAULT_PAGE_SIZE = 500  # the max the UI dropdown exposes


@dataclass(frozen=True)
class BillingEntry:
    """One row from the billing list response.

    Named fields cover every column on the response the ingest cares
    about — control-flow fields (the ids, void/deleted booleans) plus
    every column the design doc names as feeding a rule. Fields sourced
    from this endpoint land on the record with a `billing_list_`
    prefix so they're distinguishable from `note_*` fields
    (Bedrock-extracted from the PDF) at a glance.
    """

    # ---- ids + control flow (used directly by the pipeline) -----------------
    id: int                       # CR's billing entry id; the source_record_id
    client_id: int
    client_first_name: str
    client_last_name: str
    provider_id: int
    provider_first_name: str
    provider_last_name: str
    is_void: bool
    is_deleted: bool
    is_locked: bool

    # ---- fields that also flow onto the record ------------------------------
    # Both used by the pipeline AND surfaced on `extracted_fields` under
    # the canonical short names (`billed_start`, `billed_end`,
    # `billed_minutes`, ...) AND re-surfaced verbatim under the
    # `billing_list_` prefix.
    date_of_service: str          # ISO-8601 from CR
    procedure_code_string: str    # e.g. "97155: Treatment Planning - BCBA"
    procedure_code_id: int
    location: str                 # e.g. "10: Telehealth Provided in Patient's Home"
    time_worked_in_mins: int
    units_of_service: int
    date_time_from: str
    date_time_to: str
    creation_date: str
    timezone: str                 # e.g. "America/Chicago"

    # ---- remainder of the list-item columns ---------------------------------
    # These aren't in the pipeline's control flow but the design doc
    # names them as inputs to compliance rules or ops reporting, so we
    # capture them for the `billing_list_` prefix dump. Keep the list
    # ordered the same way the CR endpoint returns them so the mapping
    # is easy to audit against a captured payload.
    voided_date: str
    deleted_date: str
    last_paid: str
    last_billed: str
    first_billed_date: str
    modified_date: str
    schedule_date: str
    authorization_id: int
    authorization_resource_id: int
    service_location_id: int
    calc_type: int
    drive_time_minutes: int
    mileage: float
    labels: str
    code_labels: str
    resource_count: int
    payor_id: int
    payor_insurance_id: int
    payor_name: str
    rate_client: float
    rate_client_agreed: float
    rate_client_drive_hourly: float
    rate_client_drive_mileage: float
    invoiced: int
    payments_made: int
    exported: int
    claims: int
    claims_exported: int
    group_count: int
    group_id: int
    client_charges: float
    client_charges_agreed: float
    drive_time_charges: float
    mileage_charges: float
    client_charges_total: float
    client_charges_total_agreed: float
    amount_owed: float
    amount_owed_agreed: float
    amount_paid: float
    amount_adjustment: float
    copay_owed: float
    copay_amount: float
    tasks: int
    tasks_completed: int
    show_agreed: bool
    schedule_course: int
    schedule_auth: int
    schedule_code: int
    schedule_ordinal: int
    time_worked_from_utc_offset: str
    time_zone_abbr: str

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> "BillingEntry":
        """Project a CR list item into our typed dataclass.

        Missing fields default to type-appropriate empties — CR has
        been observed to omit fields when they're empty rather than
        sending null, so defensive defaulting is reasonable here.
        Ordering in this projection mirrors the sample payload in
        `docs/centralreach-api-integration.md` so a future field
        addition is a single-line edit next to the existing shape.
        """
        return cls(
            id=int(raw["Id"]),
            date_of_service=str(raw.get("DateOfService") or ""),
            client_id=int(raw.get("ClientId") or 0),
            client_first_name=str(raw.get("ClientFirstName") or ""),
            client_last_name=str(raw.get("ClientLastName") or ""),
            provider_id=int(raw.get("ProviderId") or 0),
            provider_first_name=str(raw.get("ProviderFirstName") or ""),
            provider_last_name=str(raw.get("ProviderLastName") or ""),
            procedure_code_string=str(raw.get("ProcedureCodeString") or ""),
            procedure_code_id=int(raw.get("ProcedureCodeId") or 0),
            location=str(raw.get("Location") or ""),
            time_worked_in_mins=int(raw.get("TimeWorkedInMins") or 0),
            units_of_service=int(raw.get("UnitsOfService") or 0),
            date_time_from=str(raw.get("DateTimeFrom") or ""),
            date_time_to=str(raw.get("DateTimeTo") or ""),
            creation_date=str(raw.get("CreationDate") or ""),
            is_void=bool(raw.get("IsVoid", False)),
            is_deleted=bool(raw.get("IsDeleted", False)),
            is_locked=bool(raw.get("IsLocked", False)),
            timezone=str(raw.get("Timezone") or ""),
            voided_date=str(raw.get("VoidedDate") or ""),
            deleted_date=str(raw.get("DeletedDate") or ""),
            last_paid=str(raw.get("lastPaid") or ""),
            last_billed=str(raw.get("lastBilled") or ""),
            first_billed_date=str(raw.get("firstBilledDate") or ""),
            modified_date=str(raw.get("ModifiedDate") or ""),
            schedule_date=str(raw.get("ScheduleDate") or ""),
            authorization_id=int(raw.get("AuthorizationId") or 0),
            authorization_resource_id=int(
                raw.get("AuthorizationResourceId") or 0,
            ),
            service_location_id=int(raw.get("ServiceLocationId") or 0),
            calc_type=int(raw.get("CalcType") or 0),
            drive_time_minutes=int(raw.get("DriveTimeMinutes") or 0),
            mileage=float(raw.get("Mileage") or 0),
            labels=str(raw.get("Labels") or ""),
            code_labels=str(raw.get("CodeLabels") or ""),
            resource_count=int(raw.get("ResourceCount") or 0),
            payor_id=int(raw.get("PayorId") or 0),
            payor_insurance_id=int(raw.get("PayorInsuranceId") or 0),
            payor_name=str(raw.get("PayorName") or ""),
            rate_client=float(raw.get("RateClient") or 0),
            rate_client_agreed=float(raw.get("RateClientAgreed") or 0),
            rate_client_drive_hourly=float(
                raw.get("RateClientDriveHourly") or 0,
            ),
            rate_client_drive_mileage=float(
                raw.get("RateClientDriveMileage") or 0,
            ),
            invoiced=int(raw.get("Invoiced") or 0),
            payments_made=int(raw.get("PaymentsMade") or 0),
            exported=int(raw.get("Exported") or 0),
            claims=int(raw.get("Claims") or 0),
            claims_exported=int(raw.get("ClaimsExported") or 0),
            group_count=int(raw.get("GroupCount") or 0),
            group_id=int(raw.get("GroupId") or 0),
            client_charges=float(raw.get("ClientCharges") or 0),
            client_charges_agreed=float(raw.get("ClientChargesAgreed") or 0),
            drive_time_charges=float(raw.get("DriveTimeCharges") or 0),
            mileage_charges=float(raw.get("MileageCharges") or 0),
            client_charges_total=float(raw.get("ClientChargesTotal") or 0),
            client_charges_total_agreed=float(
                raw.get("ClientChargesTotalAgreed") or 0,
            ),
            amount_owed=float(raw.get("AmountOwed") or 0),
            amount_owed_agreed=float(raw.get("AmountOwedAgreed") or 0),
            amount_paid=float(raw.get("AmountPaid") or 0),
            amount_adjustment=float(raw.get("AmountAdjustment") or 0),
            copay_owed=float(raw.get("CopayOwed") or 0),
            copay_amount=float(raw.get("CopayAmount") or 0),
            tasks=int(raw.get("Tasks") or 0),
            tasks_completed=int(raw.get("TasksCompleted") or 0),
            show_agreed=bool(raw.get("ShowAgreed", False)),
            schedule_course=int(raw.get("ScheduleCourse") or 0),
            schedule_auth=int(raw.get("ScheduleAuth") or 0),
            schedule_code=int(raw.get("ScheduleCode") or 0),
            schedule_ordinal=int(raw.get("ScheduleOrdinal") or 0),
            time_worked_from_utc_offset=str(
                raw.get("TimeWorkedFromUtcOffset") or "",
            ),
            time_zone_abbr=str(raw.get("TimeZoneAbbr") or ""),
        )


@dataclass(frozen=True)
class BillingListResponse:
    """One page of the billing list."""

    items: tuple[BillingEntry, ...]
    page: int
    page_size: int

    @property
    def is_empty(self) -> bool:
        return len(self.items) == 0


# ----- request body builder ------------------------------------------------


def _format_date_range(start: str, end: str) -> str:
    """Build the `dateRange` display string CR's validator requires
    to be non-empty.

    The validator's only enforcement is non-empty; the UI builds
    `"Jun 29"` for single-day and `"Jun 22 - Jun 28"` for multi-day.
    We match the UI shape for the human-readability bonus but the
    server doesn't parse it.
    """
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if start_d == end_d:
        return start_d.strftime("%b %-d")
    return f"{start_d.strftime('%b %-d')} - {end_d.strftime('%b %-d')}"


def _build_body(
    start_date: str, end_date: str, *,
    page: int, page_size: int, utc_offset_minutes: int,
) -> dict[str, Any]:
    """Build the eight-field validator-required body.

    `_utcOffsetMinutes` must match the org's configured timezone — the
    same value used for the `tzoffset` cookie. The runner passes both
    in from a single source (`RPA_CONFIG.guardrails.timezone` resolved
    via `zoneinfo`) so they stay in sync.
    """
    return {
        "startDate": start_date,
        "endDate": end_date,
        "startDateDisplay": start_date,
        "endDateDisplay": end_date,
        "dateRange": _format_date_range(start_date, end_date),
        "page": page,
        "pageSize": page_size,
        "_utcOffsetMinutes": utc_offset_minutes,
    }


# ----- public API ----------------------------------------------------------


def query_billing_page(
    client: CentralReachClient, *,
    start_date: str, end_date: str,
    page: int = 1, page_size: int = _DEFAULT_PAGE_SIZE,
    utc_offset_minutes: int,
) -> BillingListResponse:
    """Query one page of billing entries for the date range.

    Raises `CentralReachValidationError` if the validator rejects the
    body; the `field_name` on the exception names the offending field
    so the operator can add it to the required-fields table in the
    design doc.
    """
    body = _build_body(
        start_date, end_date,
        page=page, page_size=page_size,
        utc_offset_minutes=utc_offset_minutes,
    )
    raw = client.post_json(_LIST_QUERY_PATH, body=body)
    items_raw = raw.get("items") or []
    items = tuple(BillingEntry.from_json(it) for it in items_raw)
    return BillingListResponse(items=items, page=page, page_size=page_size)


def paginate_billing_entries(
    client: CentralReachClient, *,
    start_date: str, end_date: str,
    page_size: int = _DEFAULT_PAGE_SIZE,
    utc_offset_minutes: int,
) -> Iterator[BillingEntry]:
    """Iterate every billing entry across all pages.

    Yields one entry at a time; the runner pipes the iterator straight
    into per-entry processing without buffering an entire run's worth
    of entries in memory. Stops on the first empty page.
    """
    page = 1
    while True:
        response = query_billing_page(
            client,
            start_date=start_date, end_date=end_date,
            page=page, page_size=page_size,
            utc_offset_minutes=utc_offset_minutes,
        )
        if response.is_empty:
            return
        yield from response.items
        page += 1
