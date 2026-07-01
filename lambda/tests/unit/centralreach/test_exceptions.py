"""Tests for centralreach.exceptions.

Light tests — verifying the hierarchy and carry-over fields. Most of
the exception behavior surfaces in the client tests where exceptions
are actually raised in realistic flows.
"""

import pytest

from centralreach.exceptions import (
    CentralReachAPIError,
    CentralReachAuthError,
    CentralReachContentTypeError,
    CentralReachError,
    CentralReachRateLimitError,
    CentralReachUnsupportedVendor,
    CentralReachValidationError,
)


def test_all_subclass_centralreach_error():
    """Pinned: every exception in the module must inherit from
    CentralReachError so the per-entry pipeline can catch a single
    base class."""
    for cls in (
        CentralReachAuthError,
        CentralReachUnsupportedVendor,
        CentralReachContentTypeError,
        CentralReachValidationError,
        CentralReachAPIError,
        CentralReachRateLimitError,
    ):
        assert issubclass(cls, CentralReachError), cls


def test_validation_error_carries_field_name():
    e = CentralReachValidationError("bad", field_name="DateRange")
    assert e.field_name == "DateRange"
    assert "bad" in str(e)


def test_validation_error_field_name_optional():
    e = CentralReachValidationError("bad")
    assert e.field_name is None


def test_api_error_carries_status_code():
    e = CentralReachAPIError("transport failed", status_code=503)
    assert e.status_code == 503


def test_api_error_status_code_optional():
    e = CentralReachAPIError("transport failed")
    assert e.status_code is None
