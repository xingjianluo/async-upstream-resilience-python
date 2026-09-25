"""聚合器测试：部分失败降级、超时、并发上限、整体取消。"""

import asyncio
import logging

import pytest

from app.aggregator import Aggregator
from app.models import SourceStatus
from app.observability import MetricsRegistry
from app.sources import MockSource


@pytest.mark.asyncio
async def test_partial_failure_degrades_without_failing(caplog):
    caplog.set_level(logging.INFO, logger="resilience")
    a = MockSource("a")
    b = MockSource("b", modes={"k1": "fail"})
    c = MockSource("c", modes={"k1": "timeout"})
    agg = Aggregator([a, b, c], per_source_timeout=0.1, fresh_ttl=1.0,
                     degraded_ttl=0.2)

    res = await agg.aggregate("k1")

    statuses = {s.source: s.status for s in res.sources}
    assert statuses["a"] is SourceStatus.OK
    assert statuses["b"] is SourceStatus.FAILED
    assert statuses["c"] is SourceStatus.TIMEOUT
    assert res.degraded is True
    assert set(res.merged) == {"a"}  # 缺失数据不得混入正常结果
    assert res.ttl_seconds == 0.2
    assert "source_degrade" in caplog.text  # 判定依据已打印


@pytest.mark.asyncio
async def test_all_fail_uncacheable():
    srcs = [MockSource("a", modes={"k": "fail"}),
            MockSource("b", modes={"k": "timeout"})]
    agg = Aggregator(srcs, per_source_timeout=0.05)
    res = await agg.aggregate("k")
    assert res.merged == {}
    assert res.ttl_seconds is None


@pytest.mark.asyncio
async def test_concurrency_cap_is_respected():
    active = {"n": 0}
    max_seen = {"n": 0}

    class SlowSource:
        def __init__(self, name):
            self.name = name

        async def fetch(self, key):
            active["n"] += 1
            max_seen["n"] = max(max_seen["n"], active["n"])
            await asyncio.sleep(0.05)
            active["n"] -= 1
            return self.name

    agg = Aggregator([SlowSource(str(i)) for i in range(6)],
                     per_source_timeout=1, concurrency=2)
    res = await agg.aggregate("k")
    assert len(res.merged) == 6
    assert max_seen["n"] <= 2


@pytest.mark.asyncio
async def test_parent_cancellation_cleans_inflight():
    entered = asyncio.Event()

    class StuckSource:
        name = "stuck"

        async def fetch(self, key):
            entered.set()
            await asyncio.sleep(5)

    agg = Aggregator([StuckSource()], per_source_timeout=5)
    task = asyncio.create_task(agg.aggregate("k"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_metrics_counters(monkeypatch):
    registry = MetricsRegistry()
    a = MockSource("a")
    b = MockSource("b", modes={"k": "fail"})
    agg = Aggregator([a, b], registry=registry)
    await agg.aggregate("k")
    snap = registry.snapshot()
    assert snap["source_ok"] == 1
    assert snap["source_failed"] == 1
