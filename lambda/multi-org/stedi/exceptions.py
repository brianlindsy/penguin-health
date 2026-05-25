class StediError(Exception):
    pass


class StediOrgNotConfigured(StediError):
    pass


class StediAuthError(StediError):
    pass


class StediRateLimited(StediError):
    pass


class StediUpstreamError(StediError):
    pass


class StediDailyCapExceeded(StediError):
    pass


class StediBadRequest(StediError):
    pass
