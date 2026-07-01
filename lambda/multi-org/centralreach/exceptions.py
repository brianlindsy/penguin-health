"""Exception hierarchy for the centralreach module.

Catchable at the per-entry boundary (see pipeline.py once it lands) so
one entry's failure does not abort the run. The pipeline converts each
into a structured failure event with the exception type as the
`error_type` discriminator.
"""


class CentralReachError(Exception):
    """Base for everything raised in the centralreach module."""


class CentralReachAuthError(CentralReachError):
    """Authentication or session establishment failed.

    Distinct from per-request errors because auth failure aborts the
    whole run — there is no recovery within a single Fargate task.
    """


class CentralReachUnsupportedVendor(CentralReachError):
    """Reserved for future multi-vendor expansion.

    Currently the module hard-codes the centralreach vendor; this
    exception lets the rest of the codebase grep for vendor dispatch
    failures consistently if a second vendor is ever added.
    """


class CentralReachContentTypeError(CentralReachError):
    """A response's Content-Type does not match what the endpoint
    documents.

    Most commonly raised when ServiceStack returns its HTML metadata
    snapshot page (content-type: text/html) instead of the JSON the
    caller wanted. Indicates the Accept header or request shape did
    not unambiguously select JSON. See the design doc's "ServiceStack
    content negotiation" subsection for the root cause.
    """


class CentralReachValidationError(CentralReachError):
    """CR returned HTTP 200 with a populated `responseStatus.errors`.

    CR is a ServiceStack service; validation failures arrive as
    HTTP-200 bodies with a `responseStatus` object naming the offending
    field. Carries the first error's fieldName so audit and dashboard
    rollups can aggregate by field without parsing the message string.
    """

    def __init__(self, message: str, *, field_name: str | None = None) -> None:
        super().__init__(message)
        self.field_name = field_name


class CentralReachAPIError(CentralReachError):
    """CR returned a 4xx or 5xx, or a 200 with a documented-failure
    signal that isn't a validation error.

    Catches the remaining error class: transport-level failures, and
    the "200 + `success: false` with no diagnostic info" responses we
    have observed on per-resource endpoints when the session isn't
    authorized to access the resource.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class CentralReachRateLimitError(CentralReachError):
    """CR returned a 429 (rate-limited) or equivalent throttle signal.

    Catchable separately so the client's retry path can distinguish
    "back off and retry" from real per-entry failures.
    """
