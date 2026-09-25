"""对外查询服务：配额 -> 缓存(single-flight) -> 多来源聚合。

响应 QueryResponse 完整自解释：
- accepted=False 时 reason 为可区分拒绝原因，data=None；
- accepted=True 时 freshness/degraded/source_statuses/cache 说明数据成色；
- 回源结果只有 ttl_seconds 非 None（至少一个来源成功）才写入缓存；
  全失败聚合结果不缓存，但首次/并发调用方能拿到状态标注（或拒绝）。
"""

from __future__ import annotations

from app.aggregator import Aggregator
from app.cache import SingleFlightCache
from app.errors import NoUsableData, QuotaRejected, StaleNotAllowed
from app.models import QueryResponse, RejectReason
from app.observability import MetricsRegistry, log_decision
from app.quota import QuotaManager


class QueryService:
    def __init__(
        self,
        aggregator: Aggregator,
        cache: SingleFlightCache,
        quota: QuotaManager,
        registry: MetricsRegistry,
    ) -> None:
        self.aggregator = aggregator
        self.cache = cache
        self.quota = quota
        self.registry = registry

    async def query(
        self,
        key: str,
        caller: str,
        priority: int = 0,
        *,
        allow_stale: bool = True,
        wait_refresh: bool = False,
    ) -> QueryResponse:
        log_decision(
            "query_begin",
            input={"key": key, "caller": caller, "priority": priority,
                   "allow_stale": allow_stale, "wait_refresh": wait_refresh},
        )
        try:
            async with self.quota.acquire(caller, priority):
                return await self._serve(
                    key, caller, priority,
                    allow_stale=allow_stale, wait_refresh=wait_refresh,
                )
        except QuotaRejected as exc:
            reason = exc.reason
            return self._reject(key, caller, priority, reason, exc.detail)
        except NoUsableData as exc:
            self.registry.incr("no_data_rejected")
            return self._reject(key, caller, priority,
                                RejectReason.NO_DATA, exc.detail)
        except StaleNotAllowed as exc:
            self.registry.incr("stale_rejected")
            return self._reject(key, caller, priority,
                                RejectReason.STALE_NOT_ALLOWED, exc.detail)

    async def _serve(
        self, key: str, caller: str, priority: int,
        *, allow_stale: bool, wait_refresh: bool,
    ) -> QueryResponse:
        async def loader():
            result = await self.aggregator.aggregate(key)
            if result.ttl_seconds is None:
                # 全部来源失败：不可作为缓存值
                return None
            return result.to_cache_value()

        value, snap = await self.cache.get(
            key, loader,
            allow_stale=allow_stale, wait_refresh=wait_refresh,
        )

        resp = QueryResponse(
            key=key,
            caller=caller,
            priority=priority,
            accepted=True,
            data=value.merged,
            source_statuses=dict(value.source_statuses),
            freshness=snap.state.value,
            degraded=any(
                s != "ok" for s in value.source_statuses.values()
            ),
            cache=snap,
        )
        log_decision(
            "query_done", input={"key": key, "caller": caller},
            decision="accept",
            freshness=resp.freshness, degraded=resp.degraded,
            cache_hit=snap.cache_hit, upstream_called=snap.upstream_called,
        )
        return resp

    @staticmethod
    def _reject(key, caller, priority, reason: RejectReason, detail: str):
        log_decision(
            "query_done", input={"key": key, "caller": caller},
            decision="reject", reason=reason.value, detail=detail,
        )
        from app.models import CacheSnapshot, Freshness
        return QueryResponse(
            key=key, caller=caller, priority=priority, accepted=False,
            data=None, source_statuses={}, freshness=Freshness.UNCACHED.value,
            degraded=False,
            cache=CacheSnapshot(state=Freshness.UNCACHED, age_seconds=0.0,
                                cache_hit=False, upstream_called=False,
                                refresh_failures=0),
            reason=reason.value,
        )

