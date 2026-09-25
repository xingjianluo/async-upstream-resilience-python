"""可区分的拒绝异常。"""

from __future__ import annotations

from app.models import RejectReason


class ResilienceError(Exception):
    """本层所有拒绝的基类。"""

    reason: RejectReason


class QuotaRejected(ResilienceError):
    def __init__(self, reason: RejectReason, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")


class NoUsableData(ResilienceError):
    def __init__(self, detail: str = ""):
        self.reason = RejectReason.NO_DATA
        self.detail = detail
        super().__init__(f"{RejectReason.NO_DATA.value}: {detail}")


class StaleNotAllowed(ResilienceError):
    def __init__(self, detail: str = ""):
        self.reason = RejectReason.STALE_NOT_ALLOWED
        self.detail = detail
        super().__init__(f"{RejectReason.STALE_NOT_ALLOWED.value}: {detail}")
