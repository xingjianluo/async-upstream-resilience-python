"""防击穿缓存与合并回源（single-flight）。

取舍规则（明确可解释）：
1. 无缓存（cold）：所有并发调用方合并等待同一个 loader，只触发一次真实回源；
   loader 成功 => FRESH 新值；loader 返回 None 或抛错 => 抛 NoUsableData。
2. 缓存处于新鲜期（age <= fresh_ttl）=> 直接 FRESH 命中，不回源。
3. 已过新鲜期但在宽限窗口（fresh_ttl < age <= fresh_ttl+stale_ttl）：
   - 默认：立即返回 STALE 旧值并说明 age，同时后台触发一次刷新（single-flight）；
   - wait_refresh=True：同步等待刷新；成功 => FRESH 新值；
     失败/None => 仍返回窗口内 STALE 旧值，refresh_failures 累加。
4. 超出宽限窗口：
   - allow_stale=False => 抛 StaleNotAllowed（不触发回源）；
   - allow_stale=True  => 同步回源；成功 => FRESH；失败 => 抛 NoUsableData
     （窗口外的旧值不再视为可用，宁可明确拒绝）。

指标口径（并发自洽）：
- cache_hit：进入时存在缓存条目且最终用缓存条目应答（FRESH 或 STALE）；
- cache_miss：进入时无条目，必须回源；
- cache_stale_served：实际应答值为 STALE（cache_hit 的子集）；
- upstream_calls：真正发起的 loader 次数（合并回源下每波只计一次）；
- upstream_dedup：并发请求挂到已有在途回源、未再次发起的次数；
- refresh_failresh_failures：回源失败但保留旧值的累计次数（按 key 留存于快照）。

取消安全：等待共享 future 的调用方自身被取消时，不取消共享回源；
后台刷新任务独立持有，不随原请求取消。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.errors import NoUsableData, StaleNotAllowed
from app.models import CacheEntryValue, CacheSnapshot, Freshness
from app.observability import MetricsRegistry, log_decision

Loader = Callable[[], Awaitable[CacheEntryValue | None]]


@dataclass
class _Entry:
    value: CacheEntryValue
    stored_at: float  # monotonic 写入时刻


class SingleFlightCache:
    def __init__(
        self,
        *,
        fresh_ttl: float = 1.0,
        stale_ttl: float = 5.0,
        registry: MetricsRegistry | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.fresh_ttl = fresh_ttl
        self.stale_ttl = stale_ttl
        self.registry = registry
        self._clock = clock
        self._store: dict[str, _Entry] = {}
        self._inflight: dict[str, asyncio.Future[CacheEntryValue | None]] = {}
        self._failures: dict[str, int] = {}

    # ---------- 内部工具 ----------

    def _snap(
        self, key: str, state: Freshness, age: float, *,
        cache_hit: bool, upstream_called: bool,
    ) -> CacheSnapshot:
        return CacheSnapshot(
            state=state,
            age_seconds=round(age, 6),
            cache_hit=cache_hit,
            upstream_called=upstream_called,
            refresh_failures=self._failures.get(key, 0),
        )

    def _put(self, key: str, value: CacheEntryValue) -> None:
        self._store[key] = _Entry(value=value, stored_at=self._clock())

    def _ensure_flight(
        self, key: str, loader: Loader
    ) -> tuple[asyncio.Future, bool]:
        """若已有在途回源则复用（joined=True，计 dedup），否则发起真实回源。"""
        existing = self._inflight.get(key)
        if existing is not None and not existing.done():
            if self.registry:
                self.registry.incr("upstream_dedup")
            log_decision("cache_get", input={"key": key},
                         decision="join_inflight_background_refresh")
            return existing, True
        if self.registry:
            self.registry.incr("upstream_calls")
        fut: asyncio.Future[CacheEntryValue | None] = (
            asyncio.get_running_loop().create_future()
        )
        self._inflight[key] = fut

        async def run() -> None:
            try:
                value = await loader()
            except Exception as exc:  # noqa: BLE001 - 回源失败不炸共享 future 以外的东西
                self._failures[key] = self._failures.get(key, 0) + 1
                if self.registry:
                    self.registry.incr("refresh_failures")
                log_decision(
                    "loader_failed", input={"key": key},
                    decision="retain_stale_if_available",
                    error=f"{type(exc).__name__}: {exc}",
                    refresh_failures=self._failures[key],
                )
                if not fut.done():
                    fut.set_exception(exc)
            else:
                if value is None:
                    self._failures[key] = self._failures.get(key, 0) + 1
                    if self.registry:
                        self.registry.incr("refresh_failures")
                    log_decision(
                        "loader_returned_none", input={"key": key},
                        decision="retain_stale_if_available",
                        refresh_failures=self._failures[key],
                    )
                else:
                    self._put(key, value)
                    self._failures.pop(key, None)
                if not fut.done():
                    fut.set_result(value)
            finally:
                self._inflight.pop(key, None)

        asyncio.create_task(run())
        return fut, False

    async def _await_flight(
        self, fut: asyncio.Future[CacheEntryValue | None]
    ) -> CacheEntryValue | None:
        """等待共享回源；调用方自身取消不影响回源任务。"""
        return await asyncio.shield(fut)

    def _stale_answer(
        self, key: str, entry: _Entry, upstream_called: bool
    ) -> tuple[CacheEntryValue, CacheSnapshot]:
        if self.registry:
            self.registry.incr("cache_hit")
            self.registry.incr("cache_stale_served")
        age = self._clock() - entry.stored_at
        log_decision("cache_get", input={"key": key},
                     decision="serve_stale", age=round(age, 6))
        return entry.value, self._snap(
            key, Freshness.STALE, age,
            cache_hit=True, upstream_called=upstream_called,
        )

    # ---------- 对外 API ----------

    async def get(
        self,
        key: str,
        loader: Loader,
        *,
        allow_stale: bool = True,
        wait_refresh: bool = False,
    ) -> tuple[CacheEntryValue, CacheSnapshot]:
        now = self._clock()
        entry = self._store.get(key)
        age = (now - entry.stored_at) if entry is not None else float("inf")

        # 2. 新鲜命中
        if entry is not None and age <= self.fresh_ttl:
            if self.registry:
                self.registry.incr("cache_hit")
            log_decision("cache_get", input={"key": key},
                         decision="fresh_hit", age=round(age, 6))
            return entry.value, self._snap(
                key, Freshness.FRESH, age,
                cache_hit=True, upstream_called=False,
            )

        # stale_entry：处于过期宽限窗口内的旧条目（非 None 时可安全返回旧值）
        stale_entry: _Entry | None = (
            entry
            if entry is not None and age <= self.fresh_ttl + self.stale_ttl
            else None
        )

        # 4b. 超出宽限窗口且不接受旧值：明确拒绝，不回源
        if stale_entry is None and entry is not None and not allow_stale:
            log_decision("cache_get", input={"key": key},
                         decision="reject_stale_not_allowed", age=round(age, 6))
            raise StaleNotAllowed(
                f"key={key} age={age:.3f}s 超出宽限窗口且 allow_stale=False"
            )

        # 3a. 宽限窗口内 + 允许旧值 + 不等待：返回 STALE，后台刷新
        if stale_entry is not None and allow_stale and not wait_refresh:
            # 已在途则挂接同一刷新并计 dedup；否则发起新刷新
            self._ensure_flight(key, loader)
            return self._stale_answer(key, stale_entry, upstream_called=True)

        # 1 / 3b / 4a：需要同步等待回源
        if entry is None and self.registry:
            self.registry.incr("cache_miss")
        fut, joining = self._ensure_flight(key, loader)
        log_decision(
            "cache_get", input={"key": key},
            decision="join_inflight" if joining
            else "await_fresh_from_upstream",
            joined=joining, had_entry=entry is not None,
        )

        try:
            value = await self._await_flight(fut)
        except Exception:
            # 回源失败：窗口内旧值仍可服务；否则拒绝
            if stale_entry is not None and allow_stale:
                return self._stale_answer(
                    key, stale_entry, upstream_called=True)
            raise NoUsableData(
                f"key={key} 回源失败且无窗口内可用旧值"
            )

        if value is None:
            if stale_entry is not None and allow_stale:
                return self._stale_answer(
                    key, stale_entry, upstream_called=True)
            raise NoUsableData(f"key={key} 全部来源失败且无缓存")

        if entry is not None and self.registry:
            # 刷新成功：进入时持有旧条目，按缓存命中口径（非 stale 应答）
            self.registry.incr("cache_hit")
        age_new = self._clock() - self._store[key].stored_at
        log_decision("cache_get", input={"key": key},
                     decision="fresh_from_upstream", joined=joining)
        return value, self._snap(
            key, Freshness.FRESH, age_new,
            cache_hit=entry is not None, upstream_called=True,
        )
