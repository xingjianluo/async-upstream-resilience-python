"""门面服务：配额门 -> 单飞缓存 -> 多来源聚合。

一次 query 的判定顺序（全部在日志中可追踪）：
1. PriorityGate 接纳：配额不足立即 QuotaExceededError；容量不足按优先级排队；
2. TTLCache.get_or_fetch：fresh 命中直接返回；stale 走 SWR；miss/过期单飞回源；
3. 回源即 SourceAggregator.fetch：部分来源失败只降级，全失败交由缓存决定旧值/拒绝；
4. 无论成功、失败还是调用方取消，Admission 都会归还槽位，配额与在途计数不残留。
"""
from __future__ import annotations

import logging

from app.aggregator import SourceAggregator, SourceFn
from app.cache import TTLCache
from app.config import QuotaPolicy, ResilienceConfig
from app.metrics import Metrics
from app.models import QueryResponse
from app.quota import Admission, PriorityGate

logger = logging.getLogger("app.service")


class ResilienceService:
    def __init__(
        self,
        sources: dict[str, SourceFn],
        config: ResilienceConfig | None = None,
        quota_policy: QuotaPolicy | None = None,
        source_timeouts: dict[str, float] | None = None,
    ) -> None:
        self._config = config or ResilienceConfig()
        self._quota_policy = quota_policy or QuotaPolicy()
        self._metrics = Metrics()
        self._aggregator = SourceAggregator(
            sources,
            self._config,
            source_timeouts=source_timeouts,
            metrics=self._metrics,
        )
        self._cache = TTLCache(self._config, self._metrics)
        self._gate = PriorityGate(
            self._config.global_capacity, self._quota_policy, self._metrics
        )

    async def query(self, key: str, caller: str) -> QueryResponse:
        logger.info("query accepted at service entry: caller=%s key=%s", caller, key)
        async with Admission(self._gate, caller):
            response = await self._cache.get_or_fetch(
                key, self._aggregator.fetch, caller=caller
            )
        logger.info(
            "query done: caller=%s key=%s cache=%s degraded=%s detail=%s",
            caller, key, response.cache_state.value, response.degraded, response.detail,
        )
        return response

    def metrics_snapshot(self) -> dict[str, object]:
        return self._metrics.snapshot()

    @property
    def gate(self) -> PriorityGate:
        return self._gate

    @property
    def metrics(self) -> Metrics:
        return self._metrics
