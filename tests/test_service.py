"""端到端服务测试：本地可控模拟来源复现并发/降级/配额场景。"""

import asyncio
import logging

import pytest

from app.aggregator import Aggregator
from app.cache import SingleFlightCache
from app.models import RejectReason
from app.observability import MetricsRegistry
from app.quota import QuotaManager
from app.service import QueryService
from app.sources import MockSource


def build_service(*, modes=None, capacity=10, caller_quotas=None,
                  priority_reserved=None, fresh_ttl=1.0, stale_ttl=10.0,
                  degraded_ttl=0.2, timeout=0.2):
    reg = MetricsRegistry()
    sources = [
        MockSource("inventory"),
        MockSource("price"),
        MockSource("review"),
    ]
    for src in sources:
        if modes and src.name in modes:
            m = modes[src.name]
            src.modes = dict(m.get("modes", {}))
            src.default_mode = m.get("default_mode", "ok")
    agg = Aggregator(sources, per_source_timeout=timeout, concurrency=4,
                     fresh_ttl=fresh_ttl, degraded_ttl=degraded_ttl,
                     registry=reg)
    cache = SingleFlightCache(fresh_ttl=fresh_ttl, stale_ttl=stale_ttl,
                              registry=reg)
    quota = QuotaManager(capacity=capacity, caller_quotas=caller_quotas,
                         default_caller_quota=100,
                         priority_reserved=priority_reserved, registry=reg)
    svc = QueryService(agg, cache, quota, reg)
    return svc, sources, reg


@pytest.mark.asyncio
async def test_happy_path_full_aggregate(caplog):
    caplog.set_level(logging.INFO, logger="resilience")
    svc, sources, reg = build_service()
    resp = await svc.query("p1", "caller-a")
    assert resp.accepted
    assert set(resp.data) == {"inventory", "price", "review"}
    assert resp.freshness == "fresh"
    assert resp.degraded is False
    assert resp.cache.cache_hit is False
    assert sum(s.call_count for s in sources) == 3
    assert "query_begin" in caplog.text and "query_done" in caplog.text


@pytest.mark.asyncio
async def test_concurrent_same_key_single_upstream_call():
    svc, sources, reg = build_service(capacity=20)
    # 给来源加延迟，制造并发窗口
    for s in sources:
        s.default_delay = 0.05
    resps = await asyncio.gather(*[
        svc.query("hot", f"c{i}") for i in range(20)
    ])
    assert all(r.accepted for r in resps)
    # 每个来源只被真正调用一次（single-flight 合并回源）
    assert {s.call_count for s in sources} == {1}
    snap = reg.snapshot()
    assert snap["upstream_calls"] == 1
    assert snap["upstream_dedup"] == 19


@pytest.mark.asyncio
async def test_partial_source_failure_degrades_and_explains():
    svc, sources, reg = build_service(modes={
        "review": {"modes": {"p": "fail"}},
        "price": {"modes": {"p": "timeout"}},
    }, timeout=0.1, degraded_ttl=0.2)
    resp = await svc.query("p", "c")
    assert resp.accepted
    assert set(resp.data) == {"inventory"}
    assert resp.source_statuses["review"] == "failed"
    assert resp.source_statuses["price"] == "timeout"
    assert resp.source_statuses["inventory"] == "ok"
    assert resp.degraded is True
    # 部分失败结果按短 TTL 缓存
    resp2 = await svc.query("p", "c")
    assert resp2.accepted and resp2.degraded is True
    # 第二次命中缓存，来源不再被调用
    assert sources[0].call_count == 1


@pytest.mark.asyncio
async def test_all_sources_fail_rejected_no_data():
    svc, _, reg = build_service(modes={
        "inventory": {"default_mode": "fail"},
        "price": {"default_mode": "fail"},
        "review": {"default_mode": "timeout"},
    }, timeout=0.05)
    resp = await svc.query("p", "c")
    assert resp.accepted is False
    assert resp.reason == RejectReason.NO_DATA.value
    assert resp.data is None
    assert reg.snapshot()["no_data_rejected"] == 1
    # 全失败结果不入缓存：再次查询仍触发回源
    resp2 = await svc.query("p", "c")
    assert resp2.reason == RejectReason.NO_DATA.value


@pytest.mark.asyncio
async def test_quota_rejection_reasons_distinguishable():
    svc, _, reg = build_service(capacity=1, caller_quotas={"tight": 1})
    hold = asyncio.Event()

    async def hold_query(caller):
        for s in ["inventory", "price", "review"]:
            pass
        return await svc.query("p", caller)

    # 用一个被 loader 卡住的查询占住容量更复杂；直接配额拒绝用独立 caller
    async def blocker():
        async with svc.quota.acquire("tight", 0):
            await hold.wait()

    tb = asyncio.create_task(blocker())
    await asyncio.sleep(0.02)
    r1 = await svc.query("p", "tight")
    assert r1.accepted is False
    assert r1.reason == RejectReason.CALLER_QUOTA.value

    r2 = await svc.query("p", "other")
    assert r2.accepted is False
    assert r2.reason == RejectReason.CAPACITY.value
    hold.set()
    await tb


@pytest.mark.asyncio
async def test_high_priority_admitted_under_pressure():
    svc, _, _ = build_service(
        capacity=3, priority_reserved={1: 1},
    )
    hold = asyncio.Event()

    async def blocker(caller):
        async with svc.quota.acquire(caller, 0):
            await hold.wait()

    ts = [asyncio.create_task(blocker(f"low{i}")) for i in range(2)]
    await asyncio.sleep(0.02)
    # 第三个低优先级被拒（保留槽）
    low = await svc.query("x", "low3", priority=0)
    assert low.reason == RejectReason.CAPACITY.value
    # 高优先级正常服务
    vip = await svc.query("x", "vip", priority=1)
    assert vip.accepted is True
    hold.set()
    await asyncio.gather(*ts)


@pytest.mark.asyncio
async def test_cancelled_waiters_no_residue():
    svc, _, reg = build_service()
    for s in ["inventory", "price", "review"]:
        pass
    # 让回源慢一点，制造等待窗口
    for src in svc.aggregator.sources:
        src.default_delay = 0.1

    async def wait_then_cancel():
        t = asyncio.create_task(svc.query("k", "c"))
        await asyncio.sleep(0.02)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            return "cancelled"

    results = await asyncio.gather(*[wait_then_cancel() for _ in range(5)])
    assert set(results) == {"cancelled"}
    # 等待者取消后：在途配额归零，且共享回源不受影响
    await asyncio.sleep(0.2)
    assert svc.quota.in_flight() == 0
    resp = await svc.query("k", "c")
    assert resp.accepted


@pytest.mark.asyncio
async def test_stale_served_after_refresh_failure():
    svc, _, reg = build_service(fresh_ttl=0.1, stale_ttl=5.0)
    r1 = await svc.query("p", "c")
    assert r1.freshness == "fresh"
    # 让所有来源失败
    for src in svc.aggregator.sources:
        src.default_mode = "fail"
    await asyncio.sleep(0.15)  # 过期进入宽限窗口
    r2 = await svc.query("p", "c", wait_refresh=True)
    assert r2.accepted
    assert r2.freshness == "stale"
    assert r2.cache.refresh_failures >= 1
    assert r2.data  # 旧值仍可用
