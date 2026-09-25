import asyncio
import logging

import pytest

from app.cache import TTLCache
from app.config import ResilienceConfig
from app.metrics import Metrics
from app.models import (
    AggregatedResult,
    CacheState,
    SourceResult,
    SourceState,
    UpstreamUnavailableError,
)

logger = logging.getLogger("tests.test_cache")


def _agg(key: str = "k", *, ok: bool = True, tag: str = "v1") -> AggregatedResult:
    if ok:
        return AggregatedResult(
            key=key,
            sources={"s": SourceResult("s", SourceState.OK, value=tag)},
            merged={"s": tag},
            degraded=False,
            total_elapsed=0.0,
        )
    return AggregatedResult(
        key=key,
        sources={"s": SourceResult("s", SourceState.ERROR, error="boom")},
        merged={},
        degraded=True,
        total_elapsed=0.0,
    )


def _cache(**cfg) -> tuple[TTLCache, Metrics]:
    metrics = Metrics()
    return TTLCache(ResilienceConfig(**cfg), metrics), metrics


async def test_concurrent_miss_triggers_single_fetch() -> None:
    # 输入：10 个同键并发请求，回源被事件门阻塞 50ms。
    # 判定：真实回源仅 1 次；其余 9 个共享；所有响应数据一致。
    cache, metrics = _cache(fresh_ttl=10, stale_ttl=10, refresh_min_interval=0.0)
    started = asyncio.Event()
    calls = 0

    async def fetch(key: str) -> AggregatedResult:
        nonlocal calls
        calls += 1
        logger.info("real fetch invoked: key=%s call#=%d", key, calls)
        started.set()
        await asyncio.sleep(0.05)
        return _agg(key, tag="shared")

    results = await asyncio.gather(
        *[cache.get_or_fetch("k1", fetch, caller=f"c{i}") for i in range(10)]
    )
    states = {r.cache_state for r in results}
    logger.info(
        "assert single-flight: calls=%d states=%s shared_flags=%d",
        calls, {s.value for s in states}, sum(r.in_flight_shared for r in results),
    )
    assert calls == 1
    assert metrics.snapshot()["fetch_by_key"]["k1"] == 1
    assert metrics.snapshot()["fetch_shared"] == 9
    assert all(r.data == {"s": "shared"} for r in results)
    # 一个 MISS（领导），九个共享者标注 IN_FLIGHT
    assert sum(1 for r in results if r.cache_state is CacheState.MISS) == 1
    assert sum(1 for r in results if r.cache_state is CacheState.IN_FLIGHT) == 9
    assert sum(1 for r in results if r.in_flight_shared) == 9


async def test_fresh_hit_no_refetch() -> None:
    cache, metrics = _cache(fresh_ttl=1.0, stale_ttl=1.0)
    calls = 0

    async def fetch(key: str) -> AggregatedResult:
        nonlocal calls
        calls += 1
        return _agg(tag="once")

    first = await cache.get_or_fetch("k2", fetch, caller="a")
    second = await cache.get_or_fetch("k2", fetch, caller="b")
    logger.info("assert fresh: states=%s calls=%d", [first.cache_state.value, second.cache_state.value], calls)
    assert first.cache_state is CacheState.MISS
    assert second.cache_state is CacheState.FRESH
    assert calls == 1
    assert metrics.snapshot()["cache_hits_fresh"] == 1


async def test_stale_refresh_success_and_background_update() -> None:
    # 输入：fresh_ttl 0.03s；第一次取到 v1，等待过期后第二次拿到旧值 STALE，
    # 后台刷新（0.02s）完成后立刻第三次查询，应拿到 FRESH 新值 v2。
    cache, _metrics = _cache(fresh_ttl=0.03, stale_ttl=5.0, refresh_min_interval=0.0)
    version = {"v": "v1"}

    async def fetch(key: str) -> AggregatedResult:
        await asyncio.sleep(0.02)
        return _agg(tag=version["v"])

    first = await cache.get_or_fetch("k3", fetch, caller="a")
    assert first.data == {"s": "v1"}
    await asyncio.sleep(0.05)  # 变成 stale（> fresh_ttl）

    version["v"] = "v2"
    second = await cache.get_or_fetch("k3", fetch, caller="b")  # SWR：先给旧值
    assert second.cache_state is CacheState.STALE
    assert second.data == {"s": "v1"}
    await asyncio.sleep(0.04)  # 后台刷新 0.02s 落盘，新值仍在 fresh_ttl=0.03 内
    third = await cache.get_or_fetch("k3", fetch, caller="c")
    logger.info("assert SWR: states=%s data=%s", [x.cache_state.value for x in (first, second, third)], third.data)
    assert third.cache_state is CacheState.FRESH
    assert third.data == {"s": "v2"}


async def test_fetch_failure_serves_stale_with_explanation() -> None:
    cache, metrics = _cache(fresh_ttl=0.01, stale_ttl=5.0, refresh_min_interval=0.0)

    async def good(key: str) -> AggregatedResult:
        return _agg(tag="old")

    await cache.get_or_fetch("k4", good, caller="a")
    await asyncio.sleep(0.03)  # stale

    async def bad(key: str) -> AggregatedResult:
        await asyncio.sleep(0.01)
        return _agg(ok=False)

    resp = await cache.get_or_fetch("k4", bad, caller="b")
    await asyncio.sleep(0.05)  # 等失败结算
    logger.info("assert stale-on-error: state=%s data=%s detail=%s",
                resp.cache_state.value, resp.data, resp.detail)
    assert resp.cache_state is CacheState.STALE
    assert resp.data == {"s": "old"}
    assert "stale" in resp.detail
    assert metrics.snapshot()["fetch_failures"] >= 1


async def test_expired_entry_fetch_failure_is_rejected() -> None:
    # fresh+stale 已过：旧值不可用；回源失败必须明确拒绝。
    cache, metrics = _cache(fresh_ttl=0.01, stale_ttl=0.02, refresh_min_interval=0.0)

    async def good(key: str) -> AggregatedResult:
        return _agg(tag="gone")

    await cache.get_or_fetch("k5", good, caller="a")
    await asyncio.sleep(0.06)  # 彻底过期

    async def bad(key: str) -> AggregatedResult:
        return _agg(ok=False)

    with pytest.raises(UpstreamUnavailableError):
        await cache.get_or_fetch("k5", bad, caller="b")
    logger.info("assert expired+failure rejection: snapshot=%s", metrics.snapshot())
    assert metrics.snapshot()["fetch_failures"] == 1


async def test_leader_cancel_does_not_kill_fetch_and_no_residue() -> None:
    # 输入：领导请求在等待回源时被取消；判定：回源任务继续完成并写缓存，
    # 后续请求直接 FRESH 命中，无二次回源、无在途残留（通过指标自洽体现）。
    cache, metrics = _cache(fresh_ttl=10, stale_ttl=10, refresh_min_interval=0.0)
    released = asyncio.Event()

    async def slow_fetch(key: str) -> AggregatedResult:
        await asyncio.sleep(0.05)
        released.set()
        return _agg(tag="persisted")

    task = asyncio.create_task(cache.get_or_fetch("k6", slow_fetch, caller="leader"))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.wait_for(released.wait(), timeout=1.0)
    await asyncio.sleep(0.05)  # 让回源落盘

    resp = await cache.get_or_fetch("k6", slow_fetch, caller="latecomer")
    snap = metrics.snapshot()
    logger.info("assert cancel isolation: state=%s data=%s fetch_by_key=%s",
                resp.cache_state.value, resp.data, snap["fetch_by_key"])
    assert resp.cache_state is CacheState.FRESH
    assert resp.data == {"s": "persisted"}
    assert snap["fetch_by_key"]["k6"] == 1
