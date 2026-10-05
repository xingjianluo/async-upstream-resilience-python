"""多来源并行聚合。

规则（对调用方可解释）：
- 所有来源并行发起，同一次聚合内受 fetch_concurrency 信号量约束；
- 每个来源受各自单次超时（source_timeouts，缺省 default_source_timeout）约束，
  超时标注 TIMEOUT，不拖垮其他来源；
- 整次聚合受 aggregate_timeout 预算约束：预算耗尽时已完成来源保留，
  未完成来源标注 CANCELLED；
- 来源抛错标注 ERROR。TIMEOUT/ERROR/CANCELLED 均为“降级缺失”而非整次失败；
- 按来源短期回退：来源本次失败时，若该来源在 fallback_ttl 内有最近成功值，
  则临时顶上（fallback=True、保留真实失败 state、标注大致年龄），
  只有真实成功的值才刷新基准，部分成功不会冲掉其他来源的基准；
- 全部来源都不可用（含无法回退）时，返回结果 any_usable=False，
  由上层决定返回旧值或明确拒绝。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from app.config import ResilienceConfig
from app.fallback import SourceFallbackStore
from app.metrics import Metrics
from app.models import AggregatedResult, SourceResult, SourceState

logger = logging.getLogger("app.aggregator")

# 来源调用签名：(key) -> 任意可 JSON 序列化对象
SourceFn = Callable[[str], Awaitable[object]]


class SourceAggregator:
    def __init__(
        self,
        sources: dict[str, SourceFn],
        config: ResilienceConfig,
        source_timeouts: dict[str, float] | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self._sources = dict(sources)
        self._config = config
        self._source_timeouts = dict(source_timeouts or {})
        self._sem = asyncio.Semaphore(config.fetch_concurrency)
        self._metrics = metrics
        self._fallback = SourceFallbackStore(config.fallback_ttl)

    def _timeout_of(self, name: str) -> float:
        return self._source_timeouts.get(name, self._config.default_source_timeout)

    async def _fetch_one(self, name: str, fn: SourceFn, key: str) -> SourceResult:
        start = time.monotonic()
        try:
            async with self._sem:
                value = await asyncio.wait_for(fn(key), timeout=self._timeout_of(name))
        except TimeoutError:
            elapsed = time.monotonic() - start
            logger.info(
                "source degraded: key=%s source=%s state=timeout timeout=%.3fs elapsed=%.3fs",
                key, name, self._timeout_of(name), elapsed,
            )
            return SourceResult(name, SourceState.TIMEOUT, error="timeout", elapsed=elapsed)
        except asyncio.CancelledError:
            # 被聚合预算或调用方取消：透传，状态由 fetch 统一收尾标注。
            logger.info("source cancelled while fetch: key=%s source=%s", key, name)
            raise
        except Exception as exc:  # noqa: BLE001 - 来源任意异常都需降级标注
            elapsed = time.monotonic() - start
            logger.info(
                "source degraded: key=%s source=%s state=error error=%r elapsed=%.3fs",
                key, name, exc, elapsed,
            )
            return SourceResult(name, SourceState.ERROR, error=repr(exc), elapsed=elapsed)
        elapsed = time.monotonic() - start
        logger.info("source ok: key=%s source=%s elapsed=%.3fs", key, name, elapsed)
        return SourceResult(name, SourceState.OK, value=value, elapsed=elapsed)

    async def fetch(self, key: str) -> AggregatedResult:
        start = time.monotonic()
        logger.info("aggregate start: key=%s sources=%s", key, list(self._sources))
        tasks: dict[str, asyncio.Task[SourceResult]] = {
            name: asyncio.ensure_future(self._fetch_one(name, fn, key))
            for name, fn in self._sources.items()
        }

        budget_timed_out = False
        try:
            # 整体预算：超时后 wait_for 取消内部 gather，未完成子任务随之被取消。
            await asyncio.wait_for(
                asyncio.gather(*tasks.values(), return_exceptions=True),
                timeout=self._config.aggregate_timeout,
            )
        except TimeoutError:
            budget_timed_out = True
            logger.info(
                "aggregate budget exhausted: key=%s budget=%.3fs",
                key, self._config.aggregate_timeout,
            )
        # 调用方取消产生的 CancelledError 故意不捕获：必须继续向上传播。

        if budget_timed_out:
            # wait_for 已传播取消；对仍未结束任务补一次 cancel 并统一 await 收尾，
            # 避免“任务悬挂销毁”告警。
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)

        results: dict[str, SourceResult] = {}
        for name, task in tasks.items():
            result = self._resolve_task(name, key, task)
            results[name] = result
            if self._metrics is not None:
                self._metrics.record_source(result.state.value)

        self._apply_fallback(key, results)

        merged = {
            name: r.value
            for name, r in results.items()
            if r.state is SourceState.OK or r.fallback
        }
        degraded = any(r.state is not SourceState.OK for r in results.values())
        total = time.monotonic() - start
        logger.info(
            "aggregate done: key=%s degraded=%s ok_sources=%s states=%s elapsed=%.3fs",
            key,
            degraded,
            [n for n, r in results.items() if r.state is SourceState.OK],
            {n: r.state.value for n, r in results.items()},
            total,
        )
        return AggregatedResult(
            key=key,
            sources=results,
            merged=merged,
            degraded=degraded,
            total_elapsed=total,
        )

    def _apply_fallback(self, key: str, results: dict[str, SourceResult]) -> None:
        """按来源回退收尾：成功值刷新基准；失败来源尝试用近期基准顶上。

        回退只改 SourceResult 的 value/fallback 标注，不改 state——
        真实失败原因保留，指标上失败与回退命中分开计数。
        """
        for name, result in results.items():
            if result.state is SourceState.OK:
                self._fallback.record_success(key, name, result.value)
                continue
            record = self._fallback.lookup(key, name)
            if record is None:
                continue
            result.value = record.value
            result.fallback = True
            result.fallback_age = record.age(time.monotonic())
            if self._metrics is not None:
                self._metrics.record_fallback_hit(name)
            logger.info(
                "source served from fallback: key=%s source=%s state=%s age=%.3fs",
                key, name, result.state.value, result.fallback_age,
            )

    def _resolve_task(
        self, name: str, key: str, task: asyncio.Task[SourceResult]
    ) -> SourceResult:
        if task.cancelled():
            logger.info(
                "source cancelled by aggregate budget: key=%s source=%s", key, name
            )
            return SourceResult(name, SourceState.CANCELLED, error="aggregate timeout")
        if task.done():
            exc = task.exception()
            if exc is None:
                return task.result()
            logger.info(
                "source resolved with exception: key=%s source=%s exc=%r",
                key, name, exc,
            )
            return SourceResult(name, SourceState.ERROR, error=repr(exc))
        # 理论不可达：预算超时路径已收尾；保守标注为取消。
        logger.info(
            "source unexpectedly pending, marked cancelled: key=%s source=%s",
            key, name,
        )
        return SourceResult(name, SourceState.CANCELLED, error="aggregate timeout")
