"""Shared email-notification helpers.

Imported by the rules engine (validation-run completion), the FHIR
eligibility poller (per-encounter problem detection), and the admin API
(subscription CRUD). Bundled as a `notifications` package into each
Lambda asset alongside `stedi/`, `fhir/`, etc.
"""

from .email_sender import send_email, EVENT_VALIDATION_RUN_COMPLETE, EVENT_ELIGIBILITY_ISSUE
from .subscriptions import get_subscribers, set_subscription, list_my_subscriptions

__all__ = [
    "send_email",
    "EVENT_VALIDATION_RUN_COMPLETE",
    "EVENT_ELIGIBILITY_ISSUE",
    "get_subscribers",
    "set_subscription",
    "list_my_subscriptions",
]
