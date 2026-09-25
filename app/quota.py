"""调用方配额、共享容量与优先级保留。

规则：
- caller_quotas: 每个调用方在途请求上限（未配置的调用方使用默认 default_caller_quota）；
- capacity: 全局共享在途容量，申请即占、释放即还；
- priority_reserved: 为优先级 >= p 的请求保留的共享容量。
  低优先级请求只有在 剩余容量 > 所有高于其优先级的保留槽总和 时才准入；
  即高优先级永远至少有 reserved 个槽不会被低优先级占满。
- 拒绝原因可区分：
  * CALLER_QUOTA —— 调用方自身配额已满（优先检查，便于解释）；
  * CAPACITY     —— 共享容量不足或被高优先级保留拦截。
- async context manager：正常返回、异常、调用方取消都会释放槽位，
  在途计数不得残留或串号（按调用方分桶计数）。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from app.errors import QuotaRejected
from app.models import RejectReason
from app.observability import MetricsRegistry, log_decision


@dataclass
class _State:
    total: int = 0
    per_caller: dict[str, int] = field(default_factory=dict)


class QuotaManager:
    def __init__(
        self,
        *,
        capacity: int = 10,
        caller_quotas: dict[str, int] | None = None,
        default_caller_quota: int = 100,
        priority_reserved: dict[int, int] | None = None,
        registry: MetricsRegistry | None = None,
    ) -> None:
        self.capacity = capacity
        self.caller_quotas = caller_quotas or {}
        self.default_caller_quota = default_caller_quota
        # priority_reserved: {优先级: 保留槽数}，语义为"优先级 >= p 保留 n"
        self.priority_reserved = dict(sorted((priority_reserved or {}).items()))
        self.registry = registry
        self._lock = asyncio.Lock()
        self._state = _State()

    def _caller_limit(self, caller: str) -> int:
        return self.caller_quotas.get(caller, self.default_caller_quota)

    def _reserve_above(self, priority: int) -> int:
        """严格高于 priority 的请求所占用的保留槽总和。"""
        return sum(n for p, n in self.priority_reserved.items() if p > priority)

    def in_flight(self) -> int:
        return self._state.total

    def caller_in_flight(self, caller: str) -> int:
        return self._state.per_caller.get(caller, 0)

    @asynccontextmanager
    async def acquire(self, caller: str, priority: int):
        async with self._lock:
            cur_caller = self._state.per_caller.get(caller, 0)
            limit = self._caller_limit(caller)
            if cur_caller >= limit:
                if self.registry:
                    self.registry.incr("caller_quota_rejected")
                log_decision(
                    "quota_check",
                    input={"caller": caller, "priority": priority},
                    decision="reject",
                    reason=RejectReason.CALLER_QUOTA.value,
                    in_flight=cur_caller, limit=limit,
                )
                raise QuotaRejected(
                    RejectReason.CALLER_QUOTA,
                    f"caller={caller} 在途 {cur_caller} 已达配额 {limit}",
                )

            free = self.capacity - self._state.total
            reserved = self._reserve_above(priority)
            if free <= reserved:
                if self.registry:
                    self.registry.incr("capacity_rejected")
                log_decision(
                    "quota_check",
                    input={"caller": caller, "priority": priority},
                    decision="reject",
                    reason=RejectReason.CAPACITY.value,
                    free=free, reserved_for_higher=reserved,
                    capacity=self.capacity,
                )
                raise QuotaRejected(
                    RejectReason.CAPACITY,
                    f"共享容量不足：空闲 {free} <= 高优先级保留 {reserved}",
                )

            self._state.total += 1
            self._state.per_caller[caller] = cur_caller + 1
            if self.registry:
                self.registry.incr("in_flight")
            log_decision(
                "quota_check",
                input={"caller": caller, "priority": priority},
                decision="admit",
                total_in_flight=self._state.total,
                caller_in_flight=cur_caller + 1,
            )

        try:
            yield
        finally:
            async with self._lock:
                self._state.total -= 1
                left = self._state.per_caller.get(caller, 0) - 1
                if left <= 0:
                    self._state.per_caller.pop(caller, None)
                else:
                    self._state.per_caller[caller] = left
                if self.registry:
                    self.registry.incr("in_flight", -1)
