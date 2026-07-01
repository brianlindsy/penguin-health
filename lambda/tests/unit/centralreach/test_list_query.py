"""Tests for centralreach.list_query.

Pins three contracts:
  1. The body shape matches what CR's validator requires (verified
     against the captured request in conversation history)
  2. Response parsing matches the captured response shape
  3. Pagination stops on the first empty page
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import patch

import pytest

from centralreach.auth import Session
from centralreach.client import CentralReachClient
from centralreach.list_query import (
    BillingEntry,
    BillingListResponse,
    _format_date_range,
    paginate_billing_entries,
    query_billing_page,
)
from centralreach.rate_limiter import RateLimiter


# ----- fixtures -------------------------------------------------------------


# Redacted version of the captured response. Field names match what we
# observed in `centralreach-api-billing-list-response.txt`; values are
# safe placeholders.
_REDACTED_LIST_ITEM = {
    "Id": 1234,
    "DateOfService": "2026-06-28T17:00:00.0000000",
    "ClientId": 5678,
    "ClientFirstName": "[FIRST]",
    "ClientLastName": "[LAST]",
    "ProviderId": 9012,
    "ProviderFirstName": "[PFIRST]",
    "ProviderLastName": "[PLAST]",
    "ProcedureCodeString": "97156: Family Treatment Guidance – BCBA",
    "ProcedureCodeId": 3456,
    "Location": "10: Telehealth Provided in Patient's Home",
    "TimeWorkedInMins": 30,
    "UnitsOfService": 2,
    "DateTimeFrom": "2026-06-28T17:00:00.0000000",
    "DateTimeTo": "2026-06-28T17:30:00.0000000",
    "CreationDate": "2026-06-28T16:55:00.0000000",
    "IsVoid": False,
    "IsDeleted": False,
    "IsLocked": False,
    "Timezone": "America/Chicago",
    # Remaining list-item columns — full sample of every column
    # `list_query.BillingEntry` now parses, taken from the redacted
    # capture in `docs/centralreach-api-integration.md`. Numeric
    # values are representative, not the real ones.
    "VoidedDate": "",
    "DeletedDate": "",
    "lastPaid": "",
    "lastBilled": "",
    "firstBilledDate": "",
    "ModifiedDate": "2026-06-28T22:32:03",
    "ScheduleDate": "2026-06-28T00:00:00",
    "AuthorizationId": 1111,
    "AuthorizationResourceId": 2222,
    "ServiceLocationId": 50,
    "CalcType": 1,
    "DriveTimeMinutes": 0,
    "Mileage": 0.0,
    "Labels": "",
    "CodeLabels": "code labels",
    "ResourceCount": 1,
    "PayorId": 3333,
    "PayorInsuranceId": 4444,
    "PayorName": "payor name",
    "RateClient": 115.0,
    "RateClientAgreed": 47.21,
    "RateClientDriveHourly": 0.0,
    "RateClientDriveMileage": 0.0,
    "Invoiced": 0,
    "PaymentsMade": 0,
    "Exported": 0,
    "Claims": 0,
    "ClaimsExported": 0,
    "GroupCount": 1,
    "GroupId": 5555,
    "ClientCharges": 230.0,
    "ClientChargesAgreed": 94.42,
    "DriveTimeCharges": 0.0,
    "MileageCharges": 0.0,
    "ClientChargesTotal": 230.0,
    "ClientChargesTotalAgreed": 94.42,
    "AmountOwed": 230.0,
    "AmountOwedAgreed": 94.42,
    "AmountPaid": 0.0,
    "AmountAdjustment": 0.0,
    "CopayOwed": 0.0,
    "CopayAmount": 0.0,
    "Tasks": 0,
    "TasksCompleted": 0,
    "ShowAgreed": True,
    "ScheduleCourse": 6666,
    "ScheduleAuth": 7777,
    "ScheduleCode": 8888,
    "ScheduleOrdinal": 1,
    "TimeWorkedFromUtcOffset": "-300",
    "TimeZoneAbbr": "CDT",
}


_REDACTED_LIST_RESPONSE = {
    "items": [_REDACTED_LIST_ITEM],
    "result": "OK",
    "failed": False,
    "cachedTime": 0,
}


_EMPTY_LIST_RESPONSE = {
    "items": [],
    "result": "OK",
    "failed": False,
    "cachedTime": 0,
}


class _NoSleepLimiter(RateLimiter):
    def __init__(self):
        super().__init__(0)


def _make_client_with_captured_request(
    response_payloads: list[dict],
) -> tuple[CentralReachClient, list[dict[str, Any]]]:
    """Build a client whose `post_json` returns canned response
    payloads in order, and records each request's path + body."""
    captured: list[dict[str, Any]] = []
    payloads = iter(response_payloads)

    class StubClient:
        def post_json(self, path, body=None):
            captured.append({"path": path, "body": body})
            return next(payloads)

        # Other client methods unused but referenced via type hint
        def get_json(self, path):
            return next(payloads)

        def get_bytes(self, url):
            raise NotImplementedError

    return StubClient(), captured  # type: ignore[return-value]


# ----- _format_date_range ---------------------------------------------------


def test_format_date_range_single_day():
    assert _format_date_range("2026-06-29", "2026-06-29") == "Jun 29"


def test_format_date_range_multi_day():
    assert _format_date_range("2026-06-22", "2026-06-28") == "Jun 22 - Jun 28"


def test_format_date_range_across_months():
    assert _format_date_range("2026-06-28", "2026-07-02") == "Jun 28 - Jul 2"


# ----- body shape -----------------------------------------------------------


def test_request_body_has_all_eight_required_fields():
    """The validator-required body fields must all be populated. Names
    and types match what CR's validator accepts."""
    client, captured = _make_client_with_captured_request([_REDACTED_LIST_RESPONSE])
    query_billing_page(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    )
    body = captured[0]["body"]
    assert set(body.keys()) == {
        "startDate", "endDate",
        "startDateDisplay", "endDateDisplay",
        "dateRange",
        "page", "pageSize",
        "_utcOffsetMinutes",
    }
    assert body["startDate"] == "2026-06-29"
    assert body["endDate"] == "2026-06-29"
    assert body["startDateDisplay"] == "2026-06-29"
    assert body["endDateDisplay"] == "2026-06-29"
    assert body["dateRange"] == "Jun 29"
    assert body["page"] == 1
    assert body["pageSize"] == 500
    assert body["_utcOffsetMinutes"] == 300


def test_request_uses_correct_path():
    client, captured = _make_client_with_captured_request([_REDACTED_LIST_RESPONSE])
    query_billing_page(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    )
    assert captured[0]["path"] == "/crxapi/internal/billing/query"


# ----- response parsing -----------------------------------------------------


def test_parses_billing_entry_from_redacted_response():
    client, _ = _make_client_with_captured_request([_REDACTED_LIST_RESPONSE])
    response = query_billing_page(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    )
    assert isinstance(response, BillingListResponse)
    assert len(response.items) == 1
    entry = response.items[0]
    assert isinstance(entry, BillingEntry)
    assert entry.id == 1234
    assert entry.client_id == 5678
    assert entry.provider_id == 9012
    assert entry.procedure_code_string.startswith("97156:")
    assert entry.time_worked_in_mins == 30
    assert entry.date_time_from == "2026-06-28T17:00:00.0000000"
    assert entry.date_time_to == "2026-06-28T17:30:00.0000000"
    assert entry.creation_date == "2026-06-28T16:55:00.0000000"
    assert entry.is_void is False
    assert entry.timezone == "America/Chicago"


def test_parses_full_list_item_columns_verbatim():
    """Every column CR sends on the list endpoint lands on
    `BillingEntry`. Rules downstream read the full column set
    through the `billing_list_*` record fields; a regression here
    (a new column silently dropped) would break those rules
    silently, so pin a representative subset covering each
    parsed-type variant (str, int, float, bool)."""
    client, _ = _make_client_with_captured_request([_REDACTED_LIST_RESPONSE])
    response = query_billing_page(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    )
    entry = response.items[0]
    # Timestamps (str)
    assert entry.modified_date == "2026-06-28T22:32:03"
    assert entry.schedule_date == "2026-06-28T00:00:00"
    # Ids / counters (int)
    assert entry.authorization_id == 1111
    assert entry.authorization_resource_id == 2222
    assert entry.service_location_id == 50
    assert entry.payor_id == 3333
    assert entry.group_id == 5555
    assert entry.resource_count == 1
    # Money (float)
    assert entry.rate_client == 115.0
    assert entry.rate_client_agreed == 47.21
    assert entry.client_charges_total == 230.0
    assert entry.amount_owed_agreed == 94.42
    # Bool
    assert entry.show_agreed is True
    # Labels + timezone-abbr (str)
    assert entry.code_labels == "code labels"
    assert entry.time_worked_from_utc_offset == "-300"
    assert entry.time_zone_abbr == "CDT"


def test_empty_response_is_handled():
    client, _ = _make_client_with_captured_request([_EMPTY_LIST_RESPONSE])
    response = query_billing_page(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    )
    assert response.is_empty
    assert response.items == ()


def test_billing_entry_from_json_uses_defaults_for_missing_fields():
    """CR has been observed to omit fields when they're empty; the
    parser must not raise on missing optional fields. The record
    downstream still emits the `billing_list_*` keys with these
    defaults so a rule can distinguish "field present but zero"
    from "record was never ingested"."""
    minimal = {"Id": 42}
    entry = BillingEntry.from_json(minimal)
    assert entry.id == 42
    assert entry.client_first_name == ""
    assert entry.time_worked_in_mins == 0
    assert entry.date_time_from == ""
    assert entry.date_time_to == ""
    assert entry.creation_date == ""
    assert entry.is_void is False
    # New columns default to type-appropriate empties.
    assert entry.modified_date == ""
    assert entry.authorization_id == 0
    assert entry.service_location_id == 0
    assert entry.rate_client == 0.0
    assert entry.show_agreed is False
    assert entry.time_zone_abbr == ""


# ----- pagination -----------------------------------------------------------


def test_paginate_yields_all_entries_then_stops_on_empty():
    """Page 1 returns one entry, page 2 returns empty → iterator
    stops after yielding the one entry."""
    page1 = {**_REDACTED_LIST_RESPONSE,
             "items": [{**_REDACTED_LIST_ITEM, "Id": 1},
                       {**_REDACTED_LIST_ITEM, "Id": 2}]}
    page2 = _EMPTY_LIST_RESPONSE
    client, captured = _make_client_with_captured_request([page1, page2])

    entries = list(paginate_billing_entries(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    ))

    assert [e.id for e in entries] == [1, 2]
    # Two API calls — page 1 yielded entries, page 2 stopped iteration
    assert len(captured) == 2
    assert captured[0]["body"]["page"] == 1
    assert captured[1]["body"]["page"] == 2


def test_paginate_stops_immediately_on_first_empty_page():
    client, captured = _make_client_with_captured_request([_EMPTY_LIST_RESPONSE])
    entries = list(paginate_billing_entries(
        client,
        start_date="2026-06-29", end_date="2026-06-29",
        utc_offset_minutes=300,
    ))
    assert entries == []
    assert len(captured) == 1
