class RpaError(Exception):
    pass


class RpaOrgNotConfigured(RpaError):
    pass


class RpaPlaybookNotFound(RpaError):
    pass


class RpaOutsideWindow(RpaError):
    """Raised when usage_guard rejects the run (outside hours / blackout date).

    The runner catches this, emits a `minor-failure` audit, and exits 0.
    """


class RpaAuthError(RpaError):
    """SSO authentication or session-cookie exchange failed."""


class RpaUnsupportedVendor(RpaError):
    """No authenticator strategy is registered for this RPA_CONFIG.vendor."""


class RpaPlaybookError(RpaError):
    """Playbook step failed (selector not found, navigation timeout, etc.)."""
