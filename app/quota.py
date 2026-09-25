"""调用方配额 + 全局共享容量 + 优先级门。

可解释的规则：
1. 每个请求需要同时占用“调用方配额槽”和“全局容量槽”，离开时必须归还；
2. 调用方自身在途数已达配额 -> 立即拒绝 QUOTA_EXCEEDED（不排队）；
3. 全局容量不足时排队等待：HIGH 优先级调用方排在所有 LOW 之前，
   因此低优先级流量无法把高优先级完全挤占（同优先级 FIFO）；
4. 等待中的请求在被取消（超时/调用方放弃）时从队列摘除，不残留、不串号；
5. 槽位的所有变更都在同一把 asyncio.Lock 内完成，并发计数自洽。

两类拒绝可区分：
- QuotaExceededError    -> 调用方自己的并发配额用尽（始终立即拒绝）；
- CapacityExceededError -> 全局容量不足（acquire(wait=False) 时立即抛出；
                           默认 wait=True 时排队等待，不产生该拒绝）。
"""
from __future__ import annotations

import asyncio
import itertools
import logging
from types import TracebackType

from app.config import QuotaPolicy
from app.metrics import Metrics
from app.models import CapacityExceededError, Priority, QuotaExceededError

logger = logging.getLogger("app.quota")


class _Waiter:
    __slots__ = ("caller", "future", "priority", "seq")

    def __init__(self, caller: str, priority: Priority, seq: int) -> None:
        self.caller = caller
        self.priority = priority
        self.seq = seq
        self.future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    # 排序键：优先级（HIGH=2 取负后在前）-> 到达序号 FIFO
    def sort_key(self) -> tuple[int, int]:
        return (-self.priority.value, self.seq)


class PriorityGate:
    def __init__(
        self,
        global_capacity: int,
        policy: QuotaPolicy,
        metrics: Metrics,
    ) -> None:
        self._capacity = global_capacity
        self._policy = policy
        self._metrics = metrics
        self._lock = asyncio.Lock()
        self._in_flight = 0
        self._per_caller: dict[str, int] = {}
        self._waiters: list[_Waiter] = []
        self._seq = itertools.count()

    # ---- 状态查询（锁内一致快照）----
    async def active_callers(self) -> dict[str, int]:
        async with self._lock:
            return dict(self._per_caller)

    async def in_flight_total(self) -> int:
        async with self._lock:
            return self._in_flight

    async def waiting(self) -> int:
        async with self._lock:
            return len(self._waiters)

    # ---- 接纳 ----
    async def acquire(self, caller: str, *, wait: bool = True) -> None:
        """占用一个调用方配额槽与一个全局容量槽。"""
        async with self._lock:
            quota = self._policy.quota_of(caller)
            current = self._per_caller.get(caller, 0)
            if current >= quota:
                self._metrics.record_reject_quota(caller)
                logger.info(
                    "reject: caller=%s reason=quota_exceeded in_flight=%d quota=%d global=%d/%d",
                    caller, current, quota, self._in_flight, self._capacity,
                )
                raise QuotaExceededError(
                    f"caller {caller!r} quota {quota} exceeded"
                )

            if self._in_flight < self._capacity:
                self._admit(caller)
                logger.info(
                    "admit immediate: caller=%s global=%d/%d",
                    caller, self._in_flight, self._capacity,
                )
                return

            if not wait:
                self._metrics.record_reject_capacity(caller)
                logger.info(
                    "reject: caller=%s reason=capacity_exceeded global=%d/%d",
                    caller, self._in_flight, self._capacity,
                )
                raise CapacityExceededError("global capacity exhausted")

            waiter = _Waiter(caller, self._policy.priority_of(caller), next(self._seq))
            self._waiters.append(waiter)
            self._waiters.sort(key=lambda w: w.sort_key())
            logger.info(
                "queued: caller=%s priority=%s queue_pos=%d global=%d/%d",
                caller, waiter.priority.name,
                self._waiters.index(waiter), self._in_flight, self._capacity,
            )

        # 锁外等待 future；被唤醒即代表已在 _pump 内占用槽位。
        try:
            await waiter.future
        except BaseException:
            # 等待中取消/异常：摘除自己，并在锁内重新调度后来者。
            async with self._lock:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
                self._pump()
            logger.info("wait cancelled and cleaned: caller=%s", caller)
            raise
        logger.info("admitted from queue: caller=%s", caller)

    async def release(self, caller: str) -> None:
        """归还一个调用方槽与一个全局容量槽，并在锁内重新调度排队者。"""
        async with self._lock:
            self._in_flight -= 1
            left = self._per_caller.get(caller, 0) - 1
            if left <= 0:
                self._per_caller.pop(caller, None)
            else:
                self._per_caller[caller] = left
            self._metrics.record_release(caller)
            logger.info(
                "release: caller=%s global=%d/%d per_caller=%s waiting=%d",
                caller, self._in_flight, self._capacity,
                dict(self._per_caller), len(self._waiters),
            )
            self._pump()

    def _admit(self, caller: str) -> None:
        """调用时必须持锁：实际占用槽位。"""
        self._in_flight += 1
        self._per_caller[caller] = self._per_caller.get(caller, 0) + 1
        self._metrics.record_admission(caller)

    def _pump(self) -> None:
        """调用时必须持锁：按优先级顺序尽量多地放行队首请求。"""
        admitted: list[str] = []
        while self._waiters:
            head = self._waiters[0]
            if self._in_flight >= self._capacity:
                break
            # 双保险：队首调用方自身配额已满则跳过（正常 acquire 已前置拦截）。
            if self._per_caller.get(head.caller, 0) >= self._policy.quota_of(
                head.caller
            ):
                self._waiters.pop(0)
                if not head.future.done():
                    head.future.set_exception(
                        QuotaExceededError(
                            f"caller {head.caller!r} quota exceeded while queued"
                        )
                    )
                continue
            self._waiters.pop(0)
            self._admit(head.caller)
            admitted.append(head.caller)
            if not head.future.done():
                head.future.set_result(None)
        if admitted:
            logger.info("pump admitted: callers=%s", admitted)


class Admission:
    """acquire 成功后的上下文句柄，退出时自动归还（异常/取消也安全）。"""

    def __init__(self, gate: PriorityGate, caller: str) -> None:
        self._gate = gate
        self._caller = caller
        self._held = False

    async def __aenter__(self) -> Admission:  # noqa: PYI034
        await self._gate.acquire(self._caller)
        self._held = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._held:
            await self._gate.release(self._caller)
            self._held = False
