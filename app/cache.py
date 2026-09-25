"""防击穿缓存：单飞（single-flight）合并回源 + 明确的新旧值取舍。

取舍规则（时间全部基于 time.monotonic()，不受系统时钟影响）：
- age < fresh_ttl                 -> FRESH：直接返回，不回源；
- fresh_ttl <= age < fresh+stale  -> STALE 宽限（stale-while-revalidate）：
    * 距上次尝试 >= refresh_min_interval 时，由一个“领导请求”发起回源，
      其他同键并发请求共享同一个在途回源，绝不二次回源；
    * 宽限内命中时旧值立即返回（不阻塞等待新值），新值成功后供后续请求；
    * 回源成功 -> 写入新值；无旧值的等待者拿到 MISS/新值；
    * 回源失败 -> 仍在宽限内的旧值照返，状态 STALE 并说明旧值年龄与失败原因；
- age >= fresh_ttl + stale_ttl    -> 旧值失效先驱逐：
    * 回源成功 -> 返回新值（MISS）；
    * 回源失败 -> 明确拒绝 UpstreamUnavailableError，绝不把缺失当正常结果；
- 无缓存（MISS）                  -> 同键仅一个真实回源，其余并发共享并等待。

取消语义：领导/等待请求被取消不会取消真实回源任务——回源在独立任务中继续，
完成后照常写缓存；在途表在 settle 时清理，计数自洽、不串号。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.config import ResilienceConfig
from app.metrics import Metrics
from app.models import (
    AggregatedResult,
    CacheState,
    QueryResponse,
    UpstreamUnavailableError,
)

logger = logging.getLogger("app.cache")

FetchFn = Callable[[str], Awaitable[AggregatedResult]]


@dataclass
class _Entry:
    stored_at: float
    result: AggregatedResult


@dataclass
class _Inflight:
    future: asyncio.Future[QueryResponse]


class TTLCache:
    def __init__(self, config: ResilienceConfig, metrics: Metrics) -> None:
        self._config = config
        self._metrics = metrics
        self._entries: dict[str, _Entry] = {}
        self._last_attempt_at: dict[str, float] = {}
        self._inflight: dict[str, _Inflight] = {}
        self._lock = asyncio.Lock()

    # ---- 条目判定（调用方持锁）----
    def _take_entry(self, key: str, now: float) -> _Entry | None:
        """返回当前可用条目（FRESH/STALE）；彻底过期则驱逐并返回 None。"""
        entry = self._entries.get(key)
        if entry is None:
            return None
        age = now - entry.stored_at
        if age >= self._config.fresh_ttl + self._config.stale_ttl:
            self._entries.pop(key, None)
            self._last_attempt_at.pop(key, None)
            logger.info("cache entry expired, evicted: key=%s age=%.3fs", key, age)
            return None
        return entry

    @staticmethod
    def _is_fresh(entry: _Entry, now: float, fresh_ttl: float) -> bool:
        return now - entry.stored_at < fresh_ttl

    async def get_or_fetch(
        self,
        key: str,
        fetch_fn: FetchFn,
        *,
        caller: str,
    ) -> QueryResponse:
        now = time.monotonic()
        future: asyncio.Future[QueryResponse] | None = None
        is_joiner = False
        async with self._lock:
            entry = self._take_entry(key, now)

            if entry is not None and self._is_fresh(
                entry, now, self._config.fresh_ttl
            ):
                self._metrics.record_cache_hit_fresh(key)
                logger.info(
                    "cache FRESH hit: caller=%s key=%s age=%.3fs",
                    caller, key, now - entry.stored_at,
                )
                return self._response(entry, CacheState.FRESH, False)

            had_stale = entry is not None

            inflight = self._inflight.get(key)
            if inflight is not None:
                if had_stale:
                    # 宽限旧值 + 在途刷新：旧值立即返回，无需等待。
                    self._metrics.record_cache_hit_stale(key)
                    self._metrics.record_fetch_share(key)
                    logger.info(
                        "cache STALE hit + share in-flight: caller=%s key=%s age=%.3fs",
                        caller, key, now - entry.stored_at,
                    )
                    return self._response(entry, CacheState.STALE, True)
                # MISS 路径：必须等待唯一回源的结果（加入者）。
                self._metrics.record_fetch_share(key)
                future = inflight.future
                is_joiner = True
                logger.info(
                    "cache MISS share in-flight fetch: caller=%s key=%s", caller, key
                )
            else:
                # 节流：宽限内刚尝试过刷新则直接返回旧值，不重复回源。
                if had_stale and (
                    now - self._last_attempt_at.get(key, 0.0)
                ) < self._config.refresh_min_interval:
                    self._metrics.record_cache_hit_stale(key)
                    logger.info(
                        "cache STALE hit (refresh throttled): caller=%s key=%s age=%.3fs",
                        caller, key, now - entry.stored_at,
                    )
                    return self._response(entry, CacheState.STALE, False)

                if had_stale:
                    self._metrics.record_cache_hit_stale(key)
                else:
                    self._metrics.record_cache_miss(key)

                future = asyncio.get_running_loop().create_future()
                self._inflight[key] = _Inflight(future=future)
                self._last_attempt_at[key] = now
                self._metrics.record_fetch(key)
                logger.info(
                    "cache fetch leader: caller=%s key=%s had_stale=%s",
                    caller, key, had_stale,
                )
                # ensure_future 只登记任务，首步要等当前协程让出锁后才执行，
                # 因此持锁内调度是安全的。
                asyncio.ensure_future(self._run_fetch(key, fetch_fn, had_stale))

                if had_stale:
                    # stale-while-revalidate：先返回旧值，后台继续刷新。
                    return self._response(entry, CacheState.STALE, False)

        # 无可用旧值：领导保留底层 MISS 状态；加入者标注 IN_FLIGHT。
        assert future is not None
        return await self._await_future(caller, key, future, shared=is_joiner)

    async def _await_future(
        self,
        caller: str,
        key: str,
        future: asyncio.Future[QueryResponse],
        *,
        shared: bool,
    ) -> QueryResponse:
        response = await future  # 失败时抛 UpstreamUnavailableError，语义即拒绝
        if not shared:
            # 领导请求：future 完成的就是它触发的回源结果，保留底层真实状态。
            logger.info(
                "leader fetch resolved: caller=%s key=%s state=%s",
                caller, key, response.cache_state.value,
            )
            return response
        logger.info(
            "shared fetch resolved: caller=%s key=%s state=%s",
            caller, key, response.cache_state.value,
        )
        # 加入者并未自己回源：无论底层结果是新值还是旧值，对其而言都应标注
        # “复用了在途回源”，IN_FLIGHT 与领导的 MISS 明确区分。
        detail = response.detail + " (merged with an in-flight upstream fetch)"
        return QueryResponse(
            key=response.key,
            data=response.data,
            cache_state=CacheState.IN_FLIGHT,
            degraded=response.degraded,
            source_states=dict(response.source_states),
            in_flight_shared=True,
            age=response.age,
            detail=detail,
        )

    async def _run_fetch(
        self, key: str, fetch_fn: FetchFn, had_stale: bool
    ) -> None:
        """回源任务体：拿到结果后在缓存锁内统一 settle。"""
        try:
            result = await fetch_fn(key)
        except asyncio.CancelledError:
            logger.info("fetch task cancelled: key=%s", key)
            await self._settle_failure(key, had_stale, "fetch cancelled")
            raise
        except Exception as exc:  # noqa: BLE001 - 回源任意异常都要进入失败结算
            logger.info("fetch task raised: key=%s exc=%r", key, exc)
            await self._settle_failure(key, had_stale, repr(exc))
            return

        if result.any_ok:
            await self._settle_success(key, result, had_stale)
        else:
            await self._settle_failure(
                key,
                had_stale,
                "all sources unavailable: "
                + ",".join(f"{n}:{r.state.value}" for n, r in result.sources.items()),
            )

    async def _settle_success(
        self, key: str, result: AggregatedResult, refreshed: bool
    ) -> None:
        async with self._lock:
            stored_at = time.monotonic()
            existing = self._entries.get(key)
            if existing is None or existing.stored_at <= stored_at:
                self._entries[key] = _Entry(stored_at, result)
            inflight = self._inflight.pop(key, None)
            entry = self._entries[key]
            state = CacheState.STALE_FRESH if refreshed else CacheState.MISS
            response = self._response(entry, state, False)
            logger.info(
                "cache fetch success: key=%s state=%s degraded=%s sources=%s",
                key, state.value, result.degraded,
                {n: r.state.value for n, r in result.sources.items()},
            )
            if inflight is not None and not inflight.future.done():
                inflight.future.set_result(response)

    async def _settle_failure(
        self, key: str, had_stale: bool, reason: str
    ) -> None:
        self._metrics.record_fetch_failure(key)
        async with self._lock:
            inflight = self._inflight.pop(key, None)
            # settle 时再确认旧值仍在宽限内（回源期间条目可能过期被驱逐）。
            now = time.monotonic()
            stale_entry = self._entries.get(key) if had_stale else None
            if stale_entry is not None and (
                now - stale_entry.stored_at
                >= self._config.fresh_ttl + self._config.stale_ttl
            ):
                stale_entry = None

            if stale_entry is not None and self._config.allow_stale_on_error:
                age = now - stale_entry.stored_at
                response = self._response(
                    stale_entry,
                    CacheState.STALE,
                    False,
                    override_detail=(
                        f"upstream refresh failed ({reason}); "
                        f"serving stale value, age={age:.3f}s"
                    ),
                )
                logger.info(
                    "cache fetch failed, serve stale: key=%s reason=%s age=%.3fs",
                    key, reason, age,
                )
                if inflight is not None and not inflight.future.done():
                    inflight.future.set_result(response)
                return

            logger.info("cache fetch failed, reject: key=%s reason=%s", key, reason)
            if inflight is not None and not inflight.future.done():
                inflight.future.set_exception(
                    UpstreamUnavailableError(
                        f"key={key}: upstream failed ({reason}); no usable cache"
                    )
                )

    # ---- 响应构造 ----
    def _response(
        self,
        entry: _Entry,
        state: CacheState,
        in_flight_shared: bool,
        *,
        override_detail: str | None = None,
    ) -> QueryResponse:
        result = entry.result
        age = time.monotonic() - entry.stored_at
        source_states = {n: r.state for n, r in result.sources.items()}
        detail = override_detail or self._explain(state, result, age)
        return QueryResponse(
            key=result.key,
            data=result.merged,
            cache_state=state,
            degraded=result.degraded,
            source_states=source_states,
            in_flight_shared=in_flight_shared,
            age=age,
            detail=detail,
        )

    @staticmethod
    def _explain(state: CacheState, result: AggregatedResult, age: float) -> str:
        missing = [
            f"{n}:{r.state.value}"
            for n, r in result.sources.items()
            if r.state is not None
            and r.state.value != "ok"
        ]
        missing_part = f" missing=[{', '.join(missing)}]" if missing else ""
        if state is CacheState.FRESH:
            return f"fresh cache hit, age={age:.3f}s.{missing_part}"
        if state is CacheState.STALE:
            return f"serving stale value, age={age:.3f}s.{missing_part}"
        if state is CacheState.STALE_FRESH:
            return f"stale entry triggered refresh; new value stored, age={age:.3f}s.{missing_part}"
        return f"cache miss, fetched from upstream, age={age:.3f}s.{missing_part}"
