import asyncio
import logging

import pytest

from app.config import QuotaPolicy
from app.metrics import Metrics
from app.models import (
    CapacityExceededError,
    Priority,
    QuotaExceededError,
)
from app.quota import Admission, PriorityGate

logger = logging.getLogger("tests.test_quota")


def _gate(capacity: int, policy: QuotaPolicy) -> tuple[PriorityGate, Metrics]:
    metrics = Metrics()
    return PriorityGate(capacity, policy, metrics), metrics


async def test_quota_exceeded_is_distinct_from_capacity() -> None:
    # 输入：caller low1 配额 1，容量充足；判定：第 2 个请求是 QUOTA 而非 CAPACITY
    policy = QuotaPolicy(quotas={"low1": 1})
    gate, metrics = _gate(capacity=10, policy=policy)

    async with Admission(gate, "low1"):
        with pytest.raises(QuotaExceededError):
            await gate.acquire("low1")
        # 另一调用方仍能获得容量 -> 证明拒绝原因是配额
        async with Admission(gate, "low2"):
            assert await gate.in_flight_total() == 2

    snap = metrics.snapshot()
    logger.info("assert reject: snapshot=%s", snap)
    assert snap["rejected_quota"] == 1
    assert snap["rejected_capacity"] == 0


async def test_capacity_exceeded_nonblocking_mode() -> None:
    policy = QuotaPolicy(quotas={"a": 5, "b": 5})
    gate, metrics = _gate(capacity=1, policy=policy)
    await gate.acquire("a")
    with pytest.raises(CapacityExceededError):
        await gate.acquire("b", wait=False)
    logger.info("assert capacity reject: snapshot=%s", metrics.snapshot())
    assert metrics.snapshot()["rejected_capacity"] == 1
    await gate.release("a")


async def test_high_priority_jumps_queue_ahead_of_low() -> None:
    # 输入：容量 1 被占用；排队顺序 low-waiter 先到、high-waiter 后到。
    # 判定依据：释放后 HIGH 先被接纳；随后 LOW 接纳。
    policy = QuotaPolicy(
        quotas={"holder": 1, "low": 1, "high": 1},
        priorities={"high": Priority.HIGH},
    )
    gate, _ = _gate(capacity=1, policy=policy)

    order: list[str] = []
    holder_released = asyncio.Event()

    async def hold() -> None:
        await gate.acquire("holder")
        await holder_released.wait()
        await gate.release("holder")

    async def waiter(name: str, entered: asyncio.Event) -> None:
        await gate.acquire(name)
        order.append(name)
        entered.set()
        await gate.release(name)

    holder_task = asyncio.create_task(hold())
    await asyncio.sleep(0.01)

    low_entered = asyncio.Event()
    high_entered = asyncio.Event()
    low_task = asyncio.create_task(waiter("low", low_entered))
    await asyncio.sleep(0.01)  # 确保 low 先排队
    high_task = asyncio.create_task(waiter("high", high_entered))
    await asyncio.sleep(0.01)

    assert await gate.waiting() == 2
    holder_released.set()

    await asyncio.wait_for(high_entered.wait(), timeout=1.0)
    assert order == ["high"], f"high must be admitted first, got {order}"
    await asyncio.wait_for(low_entered.wait(), timeout=1.0)
    assert order == ["high", "low"]

    await asyncio.gather(holder_task, low_task, high_task)
    assert await gate.in_flight_total() == 0
    assert await gate.active_callers() == {}
    logger.info("assert priority order=%s, no residue after release", order)


async def test_cancelled_waiter_leaves_no_residue() -> None:
    # 输入：容量 1 被占，低优先级请求排队后被取消。
    # 判定：队列摘除、计数归零、后来者可正常获取容量。
    policy = QuotaPolicy(quotas={"a": 1, "b": 1})
    gate, metrics = _gate(capacity=1, policy=policy)
    await gate.acquire("a")

    async def waiting() -> None:
        await gate.acquire("b")  # pragma: no cover - 应被取消

    task = asyncio.create_task(waiting())
    await asyncio.sleep(0.02)
    assert await gate.waiting() == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert await gate.waiting() == 0
    await gate.release("a")
    assert await gate.in_flight_total() == 0

    async with Admission(gate, "b"):
        assert await gate.in_flight_total() == 1
    snap = metrics.snapshot()
    logger.info("assert cancellation cleanup: snapshot=%s", snap)
    assert snap["admitted"] == snap["released"]
