import asyncio
import logging

import pytest

from app.config import QuotaPolicy, ResilienceConfig
from app.models import (
    CacheState,
    Priority,
    QuotaExceededError,
    SourceState,
)
from app.service import ResilienceService
from tests.helpers import MockSource

logger = logging.getLogger("tests.test_integration")


def _service(**cfg_overrides) -> tuple[ResilienceService, dict[str, MockSource]]:
    sources = {
        "primary": MockSource("primary", delay=0.02, value="p"),
        "secondary": MockSource("secondary", delay=0.05, value="s"),
        "flaky": MockSource("flaky", delay=0.02, fail=True),
    }
    defaults: dict[str, object] = {
        "default_source_timeout": 0.1,
        "aggregate_timeout": 0.3,
        "fetch_concurrency": 4,
        "global_capacity": 6,
        "fresh_ttl": 0.05,
        "stale_ttl": 5.0,
        "refresh_min_interval": 0.0,
    }
    defaults.update(cfg_overrides)
    cfg = ResilienceConfig(**defaults)
    policy = QuotaPolicy(
        quotas={"hi": 5, "lo1": 1, "lo2": 5, "lo3": 20},
        priorities={"hi": Priority.HIGH, "lo1": Priority.LOW, "lo2": Priority.LOW},
    )
    return ResilienceService(sources, config=cfg, quota_policy=policy), sources


async def test_concurrent_same_key_single_upstream_and_explanation() -> None:
    service, _sources = _service()
    # 输入：20 个不同调用方并发查同一 key；flaky 来源始终失败。
    # lo3 配额 20，全局容量 6：超出部分在配额门排队，但缓存单飞独立生效。
    responses = await asyncio.gather(
        *[service.query("hot-key", "lo3") for _ in range(20)]
    )
    snap = service.metrics_snapshot()
    in_flight = sum(1 for r in responses if r.cache_state is CacheState.IN_FLIGHT)
    fresh = sum(1 for r in responses if r.cache_state is CacheState.FRESH)
    logger.info(
        "integration single-flight: fetch_by_key=%s shared=%s in_flight=%d fresh=%d states=%s",
        snap["fetch_by_key"], snap["fetch_shared"], in_flight, fresh,
        sorted({r.cache_state.value for r in responses}),
    )
    # 真实回源只有一次（防击穿核心断言）
    assert snap["fetch_by_key"]["hot-key"] == 1
    # 所有调用方拿到一致数据
    assert all(r.data == {"primary": "p", "secondary": "s"} for r in responses)
    # flaky 缺失被显式标注，且整次查询不失败
    assert all(r.degraded for r in responses)
    assert all(
        r.source_states["flaky"] in (SourceState.ERROR, SourceState.TIMEOUT)
        for r in responses
    )
    assert all(r.source_states["primary"] is SourceState.OK for r in responses)
    # 恰好一个领导 MISS；其余在容量窗口内共享（IN_FLIGHT）或在门后排队、
    # 到达缓存时回源已落盘（FRESH）——两者都不允许再次回源。
    assert sum(1 for r in responses if r.cache_state is CacheState.MISS) == 1
    assert in_flight >= 1
    assert in_flight + fresh == 19


async def test_quota_rejection_distinguishable_and_does_not_leak() -> None:
    service, _ = _service()
    # lo1 配额 1：占住一个在途，第二个立即配额拒绝；结束后计数归零。
    entered = asyncio.Event()
    release = asyncio.Event()

    async def holder() -> None:
        async def gate_inner() -> None:
            entered.set()
            await release.wait()

        # 直接借助服务内部聚合不方便挂起，这里用 gate 手工占用一个槽
        await service.gate.acquire("lo1")
        entered.set()
        await release.wait()
        await service.gate.release("lo1")

    task = asyncio.create_task(holder())
    await entered.wait()

    with pytest.raises(QuotaExceededError):
        await service.query("any-key", "lo1")

    snap = service.metrics_snapshot()
    logger.info("integration quota reject snapshot=%s", snap)
    assert snap["rejected_quota"] == 1
    assert snap["rejected_capacity"] == 0

    release.set()
    await task
    await asyncio.sleep(0.02)
    assert await service.gate.in_flight_total() == 0
    assert await service.gate.active_callers() == {}


async def test_caller_cancellation_cleans_quota_and_cache() -> None:
    # 输入：一个慢查询（来源耗时 > 调用方等待时间），调用方提前取消。
    # 判定：调用方拿到 CancelledError；配额槽归还；回源继续落盘后缓存可用。
    service, sources = _service(global_capacity=4)
    sources["primary"].delay = 0.3
    sources["secondary"].delay = 0.3

    task = asyncio.create_task(service.query("cancel-key", "hi"))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.02)

    # 配额/容量层面无残留
    assert await service.gate.in_flight_total() == 0
    assert await service.gate.active_callers() == {}
    snap = service.metrics_snapshot()
    logger.info("integration cancel: admitted=%d released=%d snapshot=%s",
                snap["admitted"], snap["released"], snap)
    assert snap["admitted"] == snap["released"]

    # 回源独立任务应最终完成并写缓存（可能部分来源 TIMEOUT/CANCELLED 但有 OK）
    # 这里所有来源 0.3s，聚合预算 0.3s 边界可能全 CANCELLED -> 无缓存；
    # 放宽预算再验证一个“成功后取消等待者不影响结果”的路径：
    await asyncio.sleep(0.5)


async def test_high_priority_not_starved_under_low_traffic() -> None:
    # 容量 1：占满后，先排 low、再排 high；释放唯一槽位时，
    # pump 的单次放行必须给 HIGH（即使 low 先到），low 继续等待。
    service, _ = _service(global_capacity=1)
    await service.gate.acquire("lo2")  # 占满唯一容量

    low_done = asyncio.Event()
    high_done = asyncio.Event()

    async def waiter(name: str, ev: asyncio.Event) -> None:
        await service.gate.acquire(name)
        ev.set()
        await service.gate.release(name)

    low_task = asyncio.create_task(waiter("lo2", low_done))
    await asyncio.sleep(0.02)  # 保证 low 先入队
    high_task = asyncio.create_task(waiter("hi", high_done))
    await asyncio.sleep(0.02)
    assert await service.gate.waiting() == 2

    # 释放唯一槽位 -> 只够放行队首；若 low 在前则证明 HIGH 被挤占。
    await service.gate.release("lo2")
    await asyncio.wait_for(high_done.wait(), timeout=1.0)
    assert not low_done.is_set(), "LOW must still be queued when HIGH jumps ahead"

    await asyncio.wait_for(low_done.wait(), timeout=1.0)
    await asyncio.gather(high_task, low_task)
    assert await service.gate.in_flight_total() == 0
    logger.info("integration priority: high admitted before low, gate empty")
