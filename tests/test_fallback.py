"""按来源短期回退（SourceFallbackStore + 聚合层替换）的专项测试。

每个用例打印输入（来源行为/时序/配置）与判定依据（状态、回退年龄、指标）。
"""
import asyncio
import logging

import pytest

from app.aggregator import SourceAggregator
from app.config import QuotaPolicy, ResilienceConfig
from app.metrics import Metrics
from app.models import (
    CacheState,
    CapacityExceededError,
    QuotaExceededError,
    SourceState,
    UpstreamUnavailableError,
)
from app.service import ResilienceService
from tests.helpers import MockSource

logger = logging.getLogger("tests.test_fallback")


def _agg(sources, metrics=None, **cfg) -> SourceAggregator:
    return SourceAggregator(sources, ResilienceConfig(**cfg), metrics=metrics)


# ---------- 聚合层：按来源回退 ----------

async def test_fallback_substitutes_flaky_source_and_marks_age() -> None:
    # 输入：a 恒正常；b 第 1 轮正常（建立基准）、第 2 轮抛错；fallback_ttl=5s。
    # 判定：b 第 2 轮状态 FALLBACK，值为第 1 轮基准值，fallback_age 有标注；
    # 真实失败计入 source_error，回退命中计入 fallback_hits，两者分开。
    a = MockSource("a", value="a-v1")
    b = MockSource("b", value="b-v1")
    metrics = Metrics()
    agg = _agg({"a": a, "b": b}, metrics, fallback_ttl=5.0)

    first = await agg.fetch("k")
    assert first.sources["b"].state is SourceState.OK

    b.fail = True
    second = await agg.fetch("k")
    logger.info(
        "assert fallback: input=b fails round2; states=%s fallback_age=%s merged=%s",
        {n: r.state.value for n, r in second.sources.items()},
        second.sources["b"].fallback_age, second.merged,
    )
    assert second.sources["a"].state is SourceState.OK
    assert second.sources["b"].state is SourceState.FALLBACK
    assert second.sources["b"].value == "b-v1"
    assert second.sources["b"].fallback_age is not None
    assert 0 <= second.sources["b"].fallback_age < 5.0
    assert second.sources["b"].error is not None  # 保留真实失败原因
    assert second.merged == {"a": "a-v1", "b": "b-v1"}
    assert second.degraded is True
    assert second.any_usable is True
    snap = metrics.snapshot()
    logger.info("assert metrics split: %s", snap)
    assert snap["source_error"] == 1       # 来源真实失败
    assert snap["fallback_hits"] == 1      # 回退命中，单独计数
    assert snap["fetch_failures"] == 0     # 未走到最终拒绝


async def test_fallback_ttl_boundary() -> None:
    # 输入：单来源，fallback_ttl=0.06s；第 1 轮成功建立基准。
    # 第 2 轮（基准年龄 ~0.02s < ttl）失败 -> FALLBACK；
    # 第 3 轮（基准年龄 ~0.10s > ttl）失败 -> 回到 ERROR，不兜底。
    s = MockSource("s", value="s-v1")
    agg = _agg({"s": s}, fallback_ttl=0.06)

    await agg.fetch("k")
    s.fail = True

    await asyncio.sleep(0.02)
    within = await agg.fetch("k")
    logger.info(
        "assert within ttl: age=%.3f ttl=0.06 state=%s",
        within.sources["s"].fallback_age or -1, within.sources["s"].state.value,
    )
    assert within.sources["s"].state is SourceState.FALLBACK
    assert within.any_usable is True

    await asyncio.sleep(0.08)  # 基准年龄超过 ttl
    beyond = await agg.fetch("k")
    logger.info(
        "assert beyond ttl: state=%s any_usable=%s",
        beyond.sources["s"].state.value, beyond.any_usable,
    )
    assert beyond.sources["s"].state is SourceState.ERROR
    assert beyond.sources["s"].fallback_age is None
    assert beyond.any_usable is False


async def test_partial_success_does_not_wipe_other_baselines() -> None:
    # 输入：a/b 两来源交替成败。第 1 轮双 OK；第 2 轮 a OK(新值)、b 失败；
    # 第 3 轮 a 失败、b OK。
    # 判定：第 2 轮 b 用第 1 轮基准；第 3 轮 a 用第 2 轮刷新的基准——
    # 部分成功与失败都不会冲掉其他来源的基准值。
    a = MockSource("a", value="a-v1")
    b = MockSource("b", value="b-v1")
    agg = _agg({"a": a, "b": b}, fallback_ttl=5.0)

    await agg.fetch("k")

    a.value = "a-v2"
    b.fail = True
    second = await agg.fetch("k")
    logger.info(
        "assert round2: states=%s merged=%s",
        {n: r.state.value for n, r in second.sources.items()}, second.merged,
    )
    assert second.sources["a"].state is SourceState.OK
    assert second.sources["b"].state is SourceState.FALLBACK
    assert second.sources["b"].value == "b-v1"

    a.fail = True
    b.fail = False
    b.value = "b-v3"
    third = await agg.fetch("k")
    logger.info(
        "assert round3: states=%s merged=%s",
        {n: r.state.value for n, r in third.sources.items()}, third.merged,
    )
    assert third.sources["a"].state is SourceState.FALLBACK
    assert third.sources["a"].value == "a-v2"  # 第 2 轮部分成功刷新的基准仍在
    assert third.sources["b"].state is SourceState.OK
    assert third.sources["b"].value == "b-v3"


async def test_all_sources_fail_with_fallback_still_usable() -> None:
    # 输入：两来源第 1 轮双 OK，第 2 轮双双失败。
    # 判定：全部 FALLBACK，any_usable=True（查询可成功但降级），
    # 与“全失败且无回退 -> any_usable=False”区分。
    a = MockSource("a", value="a-v1")
    b = MockSource("b", value="b-v1")
    metrics = Metrics()
    agg = _agg({"a": a, "b": b}, metrics, fallback_ttl=5.0)

    await agg.fetch("k")
    a.fail = True
    b.fail = True
    res = await agg.fetch("k")
    logger.info(
        "assert all-fallback: states=%s any_usable=%s",
        {n: r.state.value for n, r in res.sources.items()}, res.any_usable,
    )
    assert all(r.state is SourceState.FALLBACK for r in res.sources.values())
    assert res.any_ok is False        # 无实时值
    assert res.any_usable is True     # 但回退可用 -> 查询可成功
    assert res.degraded is True
    assert metrics.snapshot()["fallback_hits"] == 2


# ---------- 服务层：端到端组合 ----------

def _service(sources, *, fallback_ttl=5.0, quotas=None, capacity=8, **cfg):
    defaults: dict[str, object] = {
        "default_source_timeout": 0.2,
        "aggregate_timeout": 0.5,
        "fetch_concurrency": 4,
        "global_capacity": capacity,
        "fresh_ttl": 0.2,
        "stale_ttl": 5.0,
        "refresh_min_interval": 0.0,
        "fallback_ttl": fallback_ttl,
    }
    defaults.update(cfg)
    policy = QuotaPolicy(quotas=quotas or {"c": 20})
    return ResilienceService(
        sources, config=ResilienceConfig(**defaults), quota_policy=policy
    )


async def test_service_marks_fallback_in_response_and_metrics() -> None:
    # 输入：flaky 第 1 次查询正常，之后持续抛错；fresh_ttl=0.05s。
    # 判定：过期后触发刷新 -> 回退顶上；再查时响应明确标出
    # source_states[flaky]=fallback、fallback_ages 有值、degraded=True，
    # 且实时来源仍是 ok；指标中回退/真实失败/最终拒绝分开计数。
    primary = MockSource("primary", delay=0.01, value="p1")
    flaky = MockSource("flaky", delay=0.01, value="f1")
    service = _service({"primary": primary, "flaky": flaky})

    first = await service.query("k", "c")
    assert first.source_states["flaky"] is SourceState.OK

    flaky.fail = True
    await asyncio.sleep(0.25)  # 过期进入 stale 窗口
    second = await service.query("k", "c")  # SWR：先回旧值，后台刷新
    assert second.cache_state is CacheState.STALE
    await asyncio.sleep(0.05)  # 等后台刷新（含回退替换）落盘，仍在 fresh 窗口内

    third = await service.query("k", "c")
    logger.info(
        "assert service fallback: states=%s fallback_ages=%s data=%s detail=%s",
        {k: s.value for k, s in third.source_states.items()},
        third.fallback_ages, third.data, third.detail,
    )
    assert third.cache_state is CacheState.FRESH
    assert third.source_states["primary"] is SourceState.OK       # 实时值
    assert third.source_states["flaky"] is SourceState.FALLBACK   # 回退值
    assert "flaky" in third.fallback_ages
    assert third.data == {"primary": "p1", "flaky": "f1"}
    assert third.degraded is True

    snap = service.metrics_snapshot()
    logger.info("assert metrics correspondence: %s", snap)
    assert snap["fallback_hits"] == 1
    assert snap["source_error"] == 1
    assert snap["fetch_failures"] == 0  # 回退顶上 -> 未走到最终拒绝


async def test_service_all_fail_with_fallback_succeeds_degraded() -> None:
    # 输入：两来源首轮正常，之后双双失败。
    # 判定：刷新后查询仍成功（非 502），全部来源标注 fallback，degraded=True。
    a = MockSource("a", delay=0.01, value="a1")
    b = MockSource("b", delay=0.01, value="b1")
    service = _service({"a": a, "b": b})

    await service.query("k", "c")
    a.fail = True
    b.fail = True
    await asyncio.sleep(0.25)
    await service.query("k", "c")  # 触发后台刷新
    await asyncio.sleep(0.05)

    resp = await service.query("k", "c")
    logger.info(
        "assert all-fallback success: states=%s data=%s",
        {k: s.value for k, s in resp.source_states.items()}, resp.data,
    )
    assert resp.source_states["a"] is SourceState.FALLBACK
    assert resp.source_states["b"] is SourceState.FALLBACK
    assert resp.data == {"a": "a1", "b": "b1"}
    assert resp.degraded is True
    snap = service.metrics_snapshot()
    assert snap["fetch_failures"] == 0
    assert snap["fallback_hits"] == 2


async def test_service_all_fail_without_fallback_rejects() -> None:
    # 输入：fallback_ttl=0（关闭回退），来源全部失败且无缓存。
    # 判定：明确拒绝 UpstreamUnavailableError；fetch_failures 计数，fallback_hits=0。
    a = MockSource("a", delay=0.01, fail=True)
    b = MockSource("b", delay=0.01, fail=True)
    service = _service({"a": a, "b": b}, fallback_ttl=0.0)

    with pytest.raises(UpstreamUnavailableError):
        await service.query("k", "c")
    snap = service.metrics_snapshot()
    logger.info("assert reject without fallback: %s", snap)
    assert snap["fetch_failures"] == 1
    assert snap["fallback_hits"] == 0


async def test_concurrent_same_key_fallback_not_amplified() -> None:
    # 输入：先正常查询建立基准与缓存；flaky 转为失败、缓存过期后，
    # 10 个并发请求打同一 key。
    # 判定：真实回源总共只有 2 次（首轮 + 一次刷新），回退命中仅 1 次，
    # 所有响应数据一致且 flaky 为回退标注。
    primary = MockSource("primary", delay=0.02, value="p1")
    flaky = MockSource("flaky", delay=0.02, value="f1")
    service = _service({"primary": primary, "flaky": flaky})

    await service.query("hot", "c")
    flaky.fail = True
    await asyncio.sleep(0.25)  # 缓存进入 stale 窗口

    responses = await asyncio.gather(
        *[service.query("hot", "c") for _ in range(10)]
    )
    await asyncio.sleep(0.05)  # 等唯一一次后台刷新落盘（仍在 fresh 窗口内）
    final = await service.query("hot", "c")

    snap = service.metrics_snapshot()
    logger.info(
        "assert no amplification: fetch_by_key=%s fallback_hits=%d states=%s",
        snap["fetch_by_key"], snap["fallback_hits"],
        sorted({r.cache_state.value for r in responses}),
    )
    assert snap["fetch_by_key"]["hot"] == 2  # 首轮 + 唯一一次刷新
    assert snap["fallback_hits"] == 1        # 只有那一次刷新发生回退替换
    assert all(r.data == {"primary": "p1", "flaky": "f1"} for r in responses)
    assert final.source_states["flaky"] is SourceState.FALLBACK
    assert final.source_states["primary"] is SourceState.OK


async def test_caller_cancel_does_not_affect_waiters_or_fallback_store() -> None:
    # 输入：两调用方并发查同一 key（来源 0.15s 慢），caller1 中途取消。
    # 判定：caller2 正常拿到结果；配额无残留；之后来源失败时
    # 同 key 回退仍正确（取消没有污染回退基准）。
    a = MockSource("a", delay=0.15, value="a1")
    b = MockSource("b", delay=0.15, value="b1")
    service = _service({"a": a, "b": b}, fresh_ttl=0.4)

    t1 = asyncio.create_task(service.query("ck", "c1"))
    t2 = asyncio.create_task(service.query("ck", "c2"))
    await asyncio.sleep(0.03)
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    resp2 = await t2
    logger.info(
        "assert cancel isolation: caller2 states=%s data=%s",
        {k: s.value for k, s in resp2.source_states.items()}, resp2.data,
    )
    assert resp2.data == {"a": "a1", "b": "b1"}
    assert await service.gate.in_flight_total() == 0
    snap = service.metrics_snapshot()
    assert snap["admitted"] == snap["released"]

    # 取消未污染回退基准：来源失败后同 key 仍能回退
    a.fail = True
    await asyncio.sleep(0.45)  # 过 fresh 窗口
    await service.query("ck", "c2")
    await asyncio.sleep(0.3)  # 等慢来源刷新落盘（仍在 fresh 窗口内）
    resp3 = await service.query("ck", "c2")
    logger.info(
        "assert fallback intact after cancel: states=%s",
        {k: s.value for k, s in resp3.source_states.items()},
    )
    assert resp3.source_states["a"] is SourceState.FALLBACK
    assert resp3.source_states["b"] is SourceState.OK


async def test_quota_and_capacity_rejection_unchanged_with_fallback() -> None:
    # 输入：回退基准已建立、来源已全部失败；lo1 配额 1、全局容量 2。
    # 判定：占住 lo1 配额后查询仍立即 QuotaExceededError（回退不绕过准入）；
    # 占满全局容量后 wait=False 仍 CapacityExceededError；两类拒绝分开计数，
    # 且全程未发生回退命中（没进门就不会取数/回退）。
    a = MockSource("a", delay=0.01, value="a1")
    service = _service(
        {"a": a}, quotas={"lo1": 1, "x1": 4, "x2": 4}, capacity=2,
    )

    await service.query("k", "lo1")  # 建立基准与缓存
    a.fail = True

    await service.gate.acquire("lo1")  # 占满 lo1 自身配额
    with pytest.raises(QuotaExceededError):
        await service.query("k", "lo1")
    await service.gate.release("lo1")

    await service.gate.acquire("x1")
    await service.gate.acquire("x2")  # 占满全局容量 2
    with pytest.raises(CapacityExceededError):
        await service.gate.acquire("lo1", wait=False)
    await service.gate.release("x1")
    await service.gate.release("x2")

    snap = service.metrics_snapshot()
    logger.info("assert reject split with fallback enabled: %s", snap)
    assert snap["rejected_quota"] == 1
    assert snap["rejected_capacity"] == 1
    assert snap["fallback_hits"] == 0
    assert snap["fetch_failures"] == 0
