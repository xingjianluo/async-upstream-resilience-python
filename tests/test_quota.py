"""配额与优先级测试。"""

import asyncio
import logging

import pytest

from app.errors import QuotaRejected
from app.models import RejectReason
from app.observability import MetricsRegistry
from app.quota import QuotaManager


@pytest.mark.asyncio
async def test_caller_quota_rejection_distinct(caplog):
    caplog.set_level(logging.INFO, logger="resilience")
    reg = MetricsRegistry()
    qm = QuotaManager(capacity=100, caller_quotas={"c1": 1}, registry=reg)
    hold = asyncio.Event()

    async def worker():
        async with qm.acquire("c1", 0):
            await hold.wait()

    t = asyncio.create_task(worker())
    await asyncio.sleep(0.01)
    with pytest.raises(QuotaRejected) as ei:
        async with qm.acquire("c1", 0):
            pass
    assert ei.value.reason is RejectReason.CALLER_QUOTA
    assert "caller_quota_exceeded" in caplog.text
    hold.set()
    await t
    assert qm.in_flight() == 0
    assert reg.snapshot()["caller_quota_rejected"] == 1


@pytest.mark.asyncio
async def test_capacity_rejection_distinct():
    reg = MetricsRegistry()
    qm = QuotaManager(capacity=2, default_caller_quota=100, registry=reg)
    hold = asyncio.Event()

    async def worker(caller):
        async with qm.acquire(caller, 0):
            await hold.wait()

    ts = [asyncio.create_task(worker(f"c{i}")) for i in range(2)]
    await asyncio.sleep(0.02)
    with pytest.raises(QuotaRejected) as ei:
        async with qm.acquire("c3", 0):
            pass
    assert ei.value.reason is RejectReason.CAPACITY
    hold.set()
    await asyncio.gather(*ts)
    assert qm.in_flight() == 0
    assert reg.snapshot()["capacity_rejected"] == 1


@pytest.mark.asyncio
async def test_high_priority_slots_protected():
    # 容量 3，为优先级 >=1 保留 1 槽：低优先级最多占 2
    qm = QuotaManager(
        capacity=3, default_caller_quota=100,
        priority_reserved={1: 1},
    )
    hold = asyncio.Event()

    async def worker(caller):
        async with qm.acquire(caller, 0):
            await hold.wait()

    ts = [asyncio.create_task(worker(f"low{i}")) for i in range(2)]
    await asyncio.sleep(0.02)
    # 第三个低优先级请求应被保留槽拦截
    with pytest.raises(QuotaRejected) as ei:
        async with qm.acquire("low2", 0):
            pass
    assert ei.value.reason is RejectReason.CAPACITY

    # 高优先级仍可准入
    async with qm.acquire("vip", 1):
        assert qm.in_flight() == 3
    hold.set()
    await asyncio.gather(*ts)


@pytest.mark.asyncio
async def test_cancel_releases_slot():
    qm = QuotaManager(capacity=1, default_caller_quota=100)
    entered = asyncio.Event()

    async def worker():
        async with qm.acquire("c", 0):
            entered.set()
            await asyncio.sleep(5)

    t = asyncio.create_task(worker())
    await entered.wait()
    assert qm.in_flight() == 1
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert qm.in_flight() == 0
    # 槽位已释放，可再次获取
    async with qm.acquire("c", 0):
        assert qm.in_flight() == 1


@pytest.mark.asyncio
async def test_exception_releases_slot():
    qm = QuotaManager(capacity=1, default_caller_quota=100)
    with pytest.raises(ValueError):
        async with qm.acquire("c", 0):
            raise ValueError("boom")
    assert qm.in_flight() == 0


@pytest.mark.asyncio
async def test_caller_buckets_no_crosstalk():
    qm = QuotaManager(capacity=10, caller_quotas={"a": 1, "b": 1})
    hold = asyncio.Event()

    async def worker(caller):
        async with qm.acquire(caller, 0):
            await hold.wait()

    ta = asyncio.create_task(worker("a"))
    tb = asyncio.create_task(worker("b"))
    await asyncio.sleep(0.02)
    assert qm.caller_in_flight("a") == 1
    assert qm.caller_in_flight("b") == 1
    hold.set()
    await asyncio.gather(ta, tb)
    assert qm.caller_in_flight("a") == 0
    assert qm.caller_in_flight("b") == 0
