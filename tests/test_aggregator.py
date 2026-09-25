import asyncio
import logging

from app.aggregator import SourceAggregator
from app.config import ResilienceConfig
from app.models import SourceState
from tests.helpers import MockSource

logger = logging.getLogger("tests.test_aggregator")


def _cfg(**overrides) -> ResilienceConfig:
    return ResilienceConfig(**overrides)


async def test_partial_failure_keeps_good_sources() -> None:
    # 输入：3 来源，1 个抛错；判定依据：其余 2 个 OK、整体 degraded=True
    sources = {
        "a": MockSource("a"),
        "b": MockSource("b", fail=True),
        "c": MockSource("c"),
    }
    agg = SourceAggregator(sources, _cfg(default_source_timeout=0.5))
    res = await agg.fetch("k1")

    logger.info("assert input=3 sources(1 fail) states=%s", {n: r.state.value for n, r in res.sources.items()})
    assert res.sources["a"].state is SourceState.OK
    assert res.sources["b"].state is SourceState.ERROR
    assert res.sources["c"].state is SourceState.OK
    assert res.degraded is True
    assert res.any_ok is True
    assert res.merged == {"a": "a:k1", "c": "c:k1"}


async def test_source_timeout_marked_not_failed_whole_query() -> None:
    # 输入：慢来源 0.3s，单来源超时 0.05s；判定：该来源 TIMEOUT，其他仍 OK
    sources = {
        "fast": MockSource("fast"),
        "slow": MockSource("slow", delay=0.3),
    }
    agg = SourceAggregator(sources, _cfg(default_source_timeout=0.05, aggregate_timeout=1.0))
    res = await agg.fetch("k2")

    logger.info("assert slow timeout: states=%s", {n: r.state.value for n, r in res.sources.items()})
    assert res.sources["slow"].state is SourceState.TIMEOUT
    assert res.sources["fast"].state is SourceState.OK
    assert res.degraded is True


async def test_aggregate_budget_cancels_slow_sources() -> None:
    # 输入：两个 0.3s 慢来源，整体预算 0.05s；判定：未完成来源 CANCELLED
    sources = {"s1": MockSource("s1", delay=0.3), "s2": MockSource("s2", delay=0.3)}
    agg = SourceAggregator(sources, _cfg(default_source_timeout=2.0, aggregate_timeout=0.05))
    res = await agg.fetch("k3")

    logger.info("assert budget cancel: states=%s", {n: r.state.value for n, r in res.sources.items()})
    assert res.total_elapsed < 0.5
    assert all(r.state is SourceState.CANCELLED for r in res.sources.values())
    assert res.any_ok is False
    # 取消后被聚合的 mock 应已退出（sleep 被取消）
    await asyncio.sleep(0)
    assert sources["s1"].active == 0
    assert sources["s2"].active == 0


async def test_all_sources_unavailable() -> None:
    sources = {"x": MockSource("x", fail=True), "y": MockSource("y", delay=1.0)}
    agg = SourceAggregator(
        sources,
        _cfg(default_source_timeout=0.02, aggregate_timeout=0.5),
    )
    res = await agg.fetch("k4")
    logger.info("assert all unavailable: states=%s", {n: r.state.value for n, r in res.sources.items()})
    assert res.any_ok is False
    assert res.sources["x"].state is SourceState.ERROR
    assert res.sources["y"].state is SourceState.TIMEOUT
