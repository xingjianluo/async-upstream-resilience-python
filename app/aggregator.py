"""多来源并行聚合。

规则：
- 所有来源并行发起，asyncio 信号量约束同时在途的来源请求数（并发上限）；
- 每个来源有独立单次超时 per_source_timeout，超时记 SourceStatus.TIMEOUT；
- 来源抛错记 SourceStatus.FAILED；个别失败/超时只降级对应键位，
  merged 中不包含失败来源，degraded=True，但整次聚合不失败；
- 全部来源失败 => ttl_seconds=None（不可缓存）；
  部分成功 => 使用较短 degraded_ttl；全部成功 => fresh_ttl；
- 调用方整体取消时，TaskGroup 负责取消全部子任务，无在途残留。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from app.models import AggregatedResult, SourceResult, SourceStatus
from app.observability import MetricsRegistry, log_decision
from app.sources import Source


class Aggregator:
    def __init__(
        self,
        sources: list[Source],
        *,
        per_source_timeout: float = 0.3,
        concurrency: int = 8,
        fresh_ttl: float = 1.0,
        degraded_ttl: float = 0.2,
        registry: MetricsRegistry | None = None,
    ) -> None:
        if not sources:
            raise ValueError("at least one source required")
        self.sources = sources
        self.per_source_timeout = per_source_timeout
        self._sem = asyncio.Semaphore(concurrency)
        self.fresh_ttl = fresh_ttl
        self.degraded_ttl = degraded_ttl
        self.registry = registry

    async def aggregate(self, key: str) -> AggregatedResult:
        start = time.monotonic()
        results: list[SourceResult] = []

        async def run_one(source: Source) -> SourceResult:
            t0 = time.monotonic()
            try:
                async with self._sem:
                    value = await asyncio.wait_for(
                        source.fetch(key), timeout=self.per_source_timeout
                    )
                sr = SourceResult(
                    source=source.name,
                    status=SourceStatus.OK,
                    value=value,
                    elapsed_ms=(time.monotonic() - t0) * 1000,
                )
                if self.registry:
                    self.registry.incr("source_ok")
                return sr
            except TimeoutError:
                sr = SourceResult(
                    source=source.name,
                    status=SourceStatus.TIMEOUT,
                    elapsed_ms=(time.monotonic() - t0) * 1000,
                    detail=f"exceeded {self.per_source_timeout}s",
                )
                if self.registry:
                    self.registry.incr("source_timeout")
                log_decision(
                    "source_degrade", input={"key": key, "source": source.name},
                    decision="timeout", timeout_s=self.per_source_timeout,
                )
                return sr
            except asyncio.CancelledError:
                # 整体取消：交给 TaskGroup 传播，不吞掉
                raise
            except Exception as exc:  # noqa: BLE001 - 任何上游异常都降级为 FAILED
                sr = SourceResult(
                    source=source.name,
                    status=SourceStatus.FAILED,
                    elapsed_ms=(time.monotonic() - t0) * 1000,
                    detail=f"{type(exc).__name__}: {exc}",
                )
                if self.registry:
                    self.registry.incr("source_failed")
                log_decision(
                    "source_degrade", input={"key": key, "source": source.name},
                    decision="failed", error=sr.detail,
                )
                return sr

        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(run_one(s)) for s in self.sources]
        except Exception as exc:  # TaskGroup 只有在我们未捕获的错误时才到这里
            log_decision("aggregate_abort", input={"key": key}, error=str(exc))
            raise
        results = [t.result() for t in tasks]

        merged: dict[str, Any] = {
            r.source: r.value for r in results if r.status is SourceStatus.OK
        }
        degraded = any(r.status is not SourceStatus.OK for r in results)
        if not merged:
            ttl: float | None = None
        else:
            ttl = self.degraded_ttl if degraded else self.fresh_ttl

        elapsed_ms = (time.monotonic() - start) * 1000
        log_decision(
            "aggregate_done",
            input={"key": key, "sources": [s.name for s in self.sources]},
            merged_keys=sorted(merged),
            degraded=degraded,
            ttl_seconds=ttl,
            elapsed_ms=round(elapsed_ms, 3),
        )
        return AggregatedResult(
            merged=merged,
            sources=results,
            degraded=degraded,
            elapsed_ms=elapsed_ms,
            ttl_seconds=ttl,
        )
