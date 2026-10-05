"""按来源短期回退的专项测试。

每个用例都打印输入（来源行为、时序、配置）与判定依据
（来源状态、回退标注、指标计数），便于失败时定位。
"""
import asyncio
import logging

import pytest

from app.aggregator import SourceAggregator
from app.config import QuotaPolicy, ResilienceConfig
from app.models import (
    CapacityExceededError,
    Priority,
    QuotaExceededError,
    SourceState,
    UpstreamUnavailableError,
)
from app.service import ResilienceService
from tests.helpers import MockSource

logger = logging.getLogger("tests.test_fallback")


def _cfg(**overrides) -> ResilienceConfig:
    defaults: dict[str, object] = {
        "default_source_timeout": 0.1,
        "aggregate_timeout": 0.5,
        "fresh_ttl": 0.03,
        "stale_ttl": 0.03,
        "refresh_min_interval": 0.0,
        "fallback_ttl": 2.0,
    }
    defaults.update(overrides)
    return ResilienceConfig(**defaults)


# ---------- 聚合层：按来源回退 ----------

async def test_source_flap_served_from_fallback_and_marked() -> None:
    # 输入：a/b 先都成功（建立基准），随后 b 抛错。
    # 判定：b 本次 state=ERROR（真实失败保留），fallback=True 且顶出旧值，
    #       a 不受影响；merged 含 b 的回退值；degraded=True。
    sources = {"a": MockSource("a", value="va"), "b": MockSource("b", value="vb")}
    agg = SourceAggregator(sources, _cfg())

    first = await agg.fetch("k")
    logger.info("baseline states=%s merged=%s",
                {n: r.state.value for n, r in first.sources.items()}, first.merged)
    assert first.merged == {"a": "va", "b": "vb"}

    sources["b"].fail = True
    res = await agg.fetch("k")
    logger.info(
        "flap result: states=%s fallback=%s ages=%s merged=%s degraded=%s",
        {n: r.state.value for n, r in res.sources.items()},
        {n: r.fallback for n, r in res.sources.items()},
        {n: r.fallback_age for n, r in res.sources.items()},
        res.merged, res.degraded,
    )
    b = res.sources["b"]
    assert b.state is SourceState.ERROR  # 真实失败原因保留
    assert b.fallback is True
    assert b.fallback_age is not None and b.fallback_age >= 0.0
    assert b.value == "vb"  # 顶出的是最近一次成功值
    assert res.sources["a"].state is SourceState.OK
    assert res.sources["a"].fallback is False
    assert res.merged == {"a": "va", "b": "vb"}
    assert res.degraded is True
    assert res.any_usable is True


async def test_fallback_ttl_boundary() -> None:
    # 输入：fallback_ttl=0.12；基准建立后 b 持续失败。
    # 判定：age<ttl 时回退顶上；age>ttl 后回到缺失（无回退、value=None）。
    sources = {"b": MockSource("b", value="vb")}
    agg = SourceAggregator(sources, _cfg(fallback_ttl=0.12))
    await agg.fetch("k")
    sources["b"].fail = True

    await asyncio.sleep(0.05)  # age≈0.05 < 0.12，有效期内
    within = await agg.fetch("k")
    logger.info("within ttl: fallback=%s age=%s", within.sources["b"].fallback,
                within.sources["b"].fallback_age)
    assert within.sources["b"].fallback is True
    assert within.merged == {"b": "vb"}

    await asyncio.sleep(0.15)  # age≈0.20 > 0.12，已过期
    expired = await agg.fetch("k")
    logger.info("beyond ttl: fallback=%s value=%s state=%s",
                expired.sources["b"].fallback, expired.sources["b"].value,
                expired.sources["b"].state.value)
    assert expired.sources["b"].fallback is False
    assert expired.sources["b"].value is None
    assert expired.any_usable is False


async def test_partial_success_does_not_clobber_other_baselines() -> None:
    # 输入：t0 a/b 都成功；t1 a 失败（回退）、b 成功拿到新值；
    #       t2 a/b 都失败。
    # 判定：t2 时 a 顶出 t0 的旧值、b 顶出 t1 的新值——
    #       t1 的部分成功只刷新 b 的基准，没有冲掉 a 的基准。
    sources = {"a": MockSource("a", value="a0"), "b": MockSource("b", value="b0")}
    agg = SourceAggregator(sources, _cfg())
    await agg.fetch("k")

    sources["a"].fail = True
    sources["b"].value = "b1"
    t1 = await agg.fetch("k")
    logger.info("t1: merged=%s fallback=%s", t1.merged,
                {n: r.fallback for n, r in t1.sources.items()})
    assert t1.merged == {"a": "a0", "b": "b1"}

    sources["b"].fail = True
    t2 = await agg.fetch("k")
    logger.info("t2: merged=%s ages=%s", t2.merged,
                {n: r.fallback_age for n, r in t2.sources.items()})
    assert t2.merged == {"a": "a0", "b": "b1"}
    assert t2.sources["a"].fallback and t2.sources["b"].fallback
    # b 的基准更新（t1），其回退年龄应小于 a 的（t0）
    assert t2.sources["b"].fallback_age < t2.sources["a"].fallback_age


async def test_all_sources_fail_without_fallback_stays_unusable() -> None:
    # 输入：无基准且所有来源失败。判定：any_usable=False，无回退标注。
    sources = {"x": MockSource("x", fail=True), "y": MockSource("y", fail=True)}
    agg = SourceAggregator(sources, _cfg())
    res = await agg.fetch("k")
    logger.info("all fail no baseline: states=%s any_usable=%s",
                {n: r.state.value for n, r in res.sources.items()}, res.any_usable)
    assert res.any_ok is False
    assert res.any_usable is False
    assert all(not r.fallback for r in res.sources.values())


# ---------- 服务层：组合结果与指标 ----------

def _service(**cfg_overrides) -> tuple[ResilienceService, dict[str, MockSource]]:
    sources = {
        "primary": MockSource("primary", value="p", delay=0.01),
        "secondary": MockSource("secondary", value="s", delay=0.01),
    }
    cfg = _cfg(**cfg_overrides)
    policy = QuotaPolicy(
        quotas={"hi": 5, "lo1": 1, "filler": 10, "bulk": 20},
        priorities={"hi": Priority.HIGH},
    )
    return ResilienceService(sources, config=cfg, quota_policy=policy), sources


async def test_service_partial_fallback_success_marked_degraded() -> None:
    # 输入：先全成功建立基准与缓存；缓存过期后 secondary 失败。
    # 判定：查询仍成功，data 含回退值，degraded=True，
    #       fallback_sources 标出 secondary 及其年龄，primary 是实时值。
    service, sources = _service()
    await service.query("k", "hi")
    await asyncio.sleep(0.08)  # 缓存彻底过期（fresh+stale=0.06）
    sources["secondary"].fail = True

    resp = await service.query("k", "hi")
    logger.info(
        "partial fallback resp: cache=%s data=%s degraded=%s states=%s fallback=%s detail=%s",
        resp.cache_state.value, resp.data, resp.degraded,
        {k: v.value for k, v in resp.source_states.items()},
        resp.fallback_sources, resp.detail,
    )
    assert resp.data == {"primary": "p", "secondary": "s"}
    assert resp.degraded is True
    assert resp.source_states["primary"] is SourceState.OK
    assert resp.source_states["secondary"] is SourceState.ERROR
    assert set(resp.fallback_sources) == {"secondary"}
    assert resp.fallback_sources["secondary"] >= 0.0

    snap = service.metrics_snapshot()
    logger.info("metrics: fallback_hits=%s source_error=%s fetch_failures=%s",
                snap["fallback_hits"], snap["source_error"], snap["fetch_failures"])
    # 回退命中与来源真实失败分开计数，且本次不算回源失败
    assert snap["fallback_hits"] == 1
    assert snap["fallback_hits_by_source"] == {"secondary": 1}
    assert snap["source_error"] == 1
    assert snap["fetch_failures"] == 0


async def test_service_all_fail_with_fallback_succeeds_degraded() -> None:
    # 输入：基准建立后全部来源失败，缓存过期。
    # 判定：查询仍成功（全部回退顶上），degraded=True，两个来源都标注回退。
    service, sources = _service()
    await service.query("k", "hi")
    await asyncio.sleep(0.08)
    sources["primary"].fail = True
    sources["secondary"].fail = True

    resp = await service.query("k", "hi")
    logger.info("all-fallback resp: data=%s degraded=%s fallback=%s",
                resp.data, resp.degraded, resp.fallback_sources)
    assert resp.data == {"primary": "p", "secondary": "s"}
    assert resp.degraded is True
    assert set(resp.fallback_sources) == {"primary", "secondary"}
    snap = service.metrics_snapshot()
    assert snap["fallback_hits"] == 2
    assert snap["fetch_failures"] == 0


async def test_service_all_fail_no_fallback_rejects() -> None:
    # 输入：回退关闭（fallback_ttl=0）且所有来源失败、无缓存。
    # 判定：明确拒绝 UpstreamUnavailableError；fetch_failures 计数 +1，
    #       且不计入 fallback_hits。
    service, sources = _service(fallback_ttl=0.0)
    sources["primary"].fail = True
    sources["secondary"].fail = True

    with pytest.raises(UpstreamUnavailableError):
        await service.query("k", "hi")
    snap = service.metrics_snapshot()
    logger.info("reject snapshot: fetch_failures=%s fallback_hits=%s",
                snap["fetch_failures"], snap["fallback_hits"])
    assert snap["fetch_failures"] == 1
    assert snap["fallback_hits"] == 0


async def test_fallback_expired_falls_back_to_reject() -> None:
    # 输入：fallback_ttl=0.06，基准建立后等待超过回退有效期，来源仍失败。
    # 判定：回退不再兜底，回到现有拒绝行为。
    service, sources = _service(fallback_ttl=0.06)
    await service.query("k", "hi")
    sources["primary"].fail = True
    sources["secondary"].fail = True
    await asyncio.sleep(0.12)  # 超过 fallback_ttl，也超过缓存 fresh+stale

    with pytest.raises(UpstreamUnavailableError):
        await service.query("k", "hi")
    snap = service.metrics_snapshot()
    logger.info("expired fallback reject: fetch_failures=%s fallback_hits=%s",
                snap["fetch_failures"], snap["fallback_hits"])
    assert snap["fallback_hits"] == 0
    assert snap["fetch_failures"] == 1


async def test_concurrent_same_key_fallback_not_amplified() -> None:
    # 输入：基准建立、缓存过期后来源部分失败，10 个并发请求同键到达。
    # 判定：真实回源只有 1 次；所有调用方拿到一致的回退标注结果。
    service, sources = _service()
    await service.query("hot", "hi")
    await asyncio.sleep(0.08)
    sources["secondary"].fail = True
    before = service.metrics_snapshot()["fetch_by_key"].get("hot", 0)

    responses = await asyncio.gather(*[service.query("hot", "bulk") for _ in range(10)])
    snap = service.metrics_snapshot()
    logger.info(
        "concurrent fallback: fetch_by_key=%s shared=%s states=%s",
        snap["fetch_by_key"], snap["fetch_shared"],
        sorted({r.cache_state.value for r in responses}),
    )
    # 10 个并发只放大出 1 次真实回源
    assert snap["fetch_by_key"]["hot"] - before == 1
    assert all(r.data == {"primary": "p", "secondary": "s"} for r in responses)
    assert all(r.degraded for r in responses)
    assert all(set(r.fallback_sources) == {"secondary"} for r in responses)
    # 回退只发生在这唯一一次真实回源里
    assert snap["fallback_hits"] == 1


async def test_caller_cancellation_does_not_affect_waiters_or_fallback() -> None:
    # 输入：慢来源下两个调用方并发同键查询，其中一个中途取消。
    # 判定：被取消者拿到 CancelledError；另一等待者正常拿到结果；
    #       回源完成落盘，后续查询（来源转失败）仍能正确回退。
    service, sources = _service(default_source_timeout=0.4)
    sources["primary"].delay = 0.15
    sources["secondary"].delay = 0.15

    victim = asyncio.create_task(service.query("k", "hi"))
    survivor = asyncio.create_task(service.query("k", "hi"))
    await asyncio.sleep(0.03)
    victim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await victim
    resp = await survivor
    logger.info("survivor resp: cache=%s data=%s", resp.cache_state.value, resp.data)
    assert resp.data == {"primary": "p", "secondary": "s"}

    # 无状态残留：门与在途计数归零
    assert await service.gate.in_flight_total() == 0
    snap = service.metrics_snapshot()
    assert snap["admitted"] == snap["released"]

    # 基准已建立：缓存过期后来源失败，回退照常工作（未串数据）
    await asyncio.sleep(0.08)
    sources["primary"].fail = True
    resp2 = await service.query("k", "hi")
    logger.info("post-cancel fallback: data=%s fallback=%s",
                resp2.data, resp2.fallback_sources)
    assert resp2.data == {"primary": "p", "secondary": "s"}
    assert set(resp2.fallback_sources) == {"primary"}


async def test_quota_and_capacity_rejections_still_distinguishable() -> None:
    # 输入：lo1 配额 1 被占满 -> 配额拒绝；全局容量被 filler 占满 ->
    #       非阻塞获取容量拒绝。回退功能开启不影响两类拒绝的区分。
    service, _sources = _service(global_capacity=2)
    await service.gate.acquire("lo1")  # 占掉 lo1 唯一配额
    with pytest.raises(QuotaExceededError):
        await service.query("any", "lo1")

    await service.gate.acquire("filler")  # 全局容量 2/2 占满
    with pytest.raises(CapacityExceededError):
        await service.gate.acquire("hi", wait=False)

    snap = service.metrics_snapshot()
    logger.info("rejections: quota=%s capacity=%s",
                snap["rejected_quota"], snap["rejected_capacity"])
    assert snap["rejected_quota"] == 1
    assert snap["rejected_capacity"] == 1

    await service.gate.release("lo1")
    await service.gate.release("filler")
    assert await service.gate.in_flight_total() == 0
