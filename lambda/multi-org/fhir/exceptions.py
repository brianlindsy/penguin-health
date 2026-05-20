class FhirError(Exception):
    pass


class FhirOrgNotConfigured(FhirError):
    pass


class FhirAuthError(FhirError):
    pass


class FhirNotFound(FhirError):
    pass


class FhirRateLimited(FhirError):
    pass


class FhirQueryTooLarge(FhirError):
    pass


class FhirUpstreamError(FhirError):
    pass
