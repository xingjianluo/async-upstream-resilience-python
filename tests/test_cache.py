"""防击穿缓存测试：并发回源合并、旧值取舍、刷新失败、超窗拒绝、取消。"""

import asyncio
import logging

import pytest

from app.cache import SingleFlightCache
from app.errors import NoUsableData, StaleNotAllowed
from app.models import CacheEntryValue, Freshness
from app.observability import MetricsRegistry


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def make_value(ttl=1.0, marker="v1"):
    return CacheEntryValue(merged={"m": marker}, source_statuses={},
                           ttl_seconds=ttl)


@pytest.mark.asyncio
async def test_concurrent_cold_load_single_flight(caplog):
    caplog.set_level(logging.INFO, logger="resilience")
    clock = FakeClock()
    reg = MetricsRegistry()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10, registry=reg, clock=clock)
    started = asyncio.Event()
    calls = {"n": 0}

    async def loader():
        calls["n"] += 1
        started.set()
        await asyncio.sleep(0.05)
        return make_value(marker="fresh")

    res = await asyncio.gather(*[cache.get("k", loader) for _ in range(10)])
    assert calls["n"] == 1                      # 只有一次真实回源
    assert all(v.merged["m"] == "fresh" for v, _ in res)
    snap = reg.snapshot()
    assert snap["upstream_calls"] == 1
    assert snap["upstream_dedup"] == 9
    assert snap["cache_miss"] == 10
    assert "await_fresh_from_upstream" in caplog.text


@pytest.mark.asyncio
async def test_fresh_hit_no_loader():
    clock = FakeClock()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10, clock=clock)
    await cache.get("k", lambda: _v("v1"))
    clock.advance(0.5)
    called = False

    async def loader():
        nonlocal called
        called = True
        return make_value()

    value, snap = await cache.get("k", loader)
    assert called is False
    assert snap.state is Freshness.FRESH
    assert snap.cache_hit is True


@pytest.mark.asyncio
async def test_stale_served_with_background_refresh():
    clock = FakeClock()
    reg = MetricsRegistry()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10, registry=reg, clock=clock)
    await cache.get("k", lambda: _v("old"))
    clock.advance(2)  # 过期但在宽限窗口内

    refreshed = asyncio.Event()

    async def loader():
        refreshed.set()
        return await _v("new")

    value, snap = await cache.get("k", loader)
    assert value.merged["m"] == "old"          # 立即拿到旧值
    assert snap.state is Freshness.STALE
    assert snap.cache_hit is True
    await refreshed.wait()
    await asyncio.sleep(0.01)                  # 等后台刷新落库
    value2, snap2 = await cache.get("k", lambda: _v("new2"))
    assert value2.merged["m"] == "new"         # 已刷新
    assert snap2.state is Freshness.FRESH
    assert reg.snapshot()["cache_stale_served"] == 1


async def _v(marker):
    return make_value(marker=marker)


@pytest.mark.asyncio
async def test_refresh_failure_keeps_stale_and_counts():
    clock = FakeClock()
    reg = MetricsRegistry()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10, registry=reg, clock=clock)
    await cache.get("k", lambda: _v("old"))
    clock.advance(2)

    async def bad_loader():
        raise RuntimeError("upstream down")

    value, snap = await cache.get("k", bad_loader, wait_refresh=True)
    assert value.merged["m"] == "old"
    assert snap.state is Freshness.STALE
    assert snap.refresh_failures == 1
    assert reg.snapshot()["refresh_failures"] == 1


@pytest.mark.asyncio
async def test_beyond_window_failure_rejects():
    clock = FakeClock()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=1, clock=clock)
    await cache.get("k", lambda: _v("old"))
    clock.advance(5)  # 超出宽限窗口

    async def bad_loader():
        return None

    with pytest.raises(NoUsableData):
        await cache.get("k", bad_loader)


@pytest.mark.asyncio
async def test_beyond_window_allow_stale_false_rejects_without_call():
    clock = FakeClock()
    calls = {"n": 0}
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=1, clock=clock)
    await cache.get("k", lambda: _v("old"))
    clock.advance(5)

    async def loader():
        calls["n"] += 1
        return make_value()

    with pytest.raises(StaleNotAllowed):
        await cache.get("k", loader, allow_stale=False)
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_cold_loader_failure_rejects():
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=1)

    async def bad():
        raise RuntimeError("boom")

    with pytest.raises(NoUsableData):
        await cache.get("absent", bad)


@pytest.mark.asyncio
async def test_awaiter_cancel_does_not_kill_flight():
    started = asyncio.Event()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10)

    async def slow_loader():
        started.set()
        await asyncio.sleep(0.1)
        return make_value(marker="survived")

    t1 = asyncio.create_task(cache.get("k", slow_loader))
    await started.wait()
    t2 = asyncio.create_task(cache.get("k", slow_loader))
    await asyncio.sleep(0.01)
    t2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t2
    value, _ = await t1  # 共享回源仍完成
    assert value.merged["m"] == "survived"


@pytest.mark.asyncio
async def test_keys_dont_crosstalk():
    calls = {"n": 0}
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10)

    async def loader_factory(marker):
        async def loader():
            calls["n"] += 1
            return make_value(marker=marker)
        return loader

    v1, _ = await cache.get("k1", await loader_factory("a"))
    v2, _ = await cache.get("k2", await loader_factory("b"))
    assert v1.merged["m"] == "a"
    assert v2.merged["m"] == "b"


@pytest.mark.asyncio
async def test_concurrent_stale_requests_share_background_refresh():
    clock = FakeClock()
    reg = MetricsRegistry()
    cache = SingleFlightCache(fresh_ttl=1, stale_ttl=10, registry=reg, clock=clock)
    await cache.get("k", lambda: _v("old"))
    clock.advance(2)

    calls = {"n": 0}

    async def loader():
        calls["n"] += 1
        await asyncio.sleep(0.05)
        return make_value(marker="new")

    # 5 个并发请求：都拿旧值，但后台刷新只发生一次，其余 4 次计入 dedup
    res = await asyncio.gather(*[
        cache.get("k", loader) for _ in range(5)
    ])
    assert all(v.merged["m"] == "old" for v, _ in res)
    assert calls["n"] == 1
    snap = reg.snapshot()
    assert snap["cache_stale_served"] == 5
    assert snap["upstream_dedup"] == 4
    await asyncio.sleep(0.06)
    value, _ = await cache.get("k", loader)
    assert value.merged["m"] == "new"
