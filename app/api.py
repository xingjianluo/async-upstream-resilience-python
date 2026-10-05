"""FastAPI 装配层。

路由：
- GET /query?key=...&caller=... 执行受保护查询；
- GET /metrics                   返回一致性指标快照；
- GET /health                    存活探针。

拒绝状态码刻意区分：
- 402 QUOTA_EXCEEDED    调用方自身并发配额用尽；
- 429 CAPACITY_EXCEEDED 全局共享容量不足（非阻塞模式）；
- 502 UPSTREAM_FAILED   无缓存且回源全部失败。
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from app.models import (
    CapacityExceededError,
    QuotaExceededError,
    UpstreamUnavailableError,
)
from app.service import ResilienceService

logger = logging.getLogger("app.api")


def create_app(service: ResilienceService) -> FastAPI:
    app = FastAPI(title="async-upstream-resilience", version="0.1.0")

    @app.get("/query")
    async def query(
        key: str = Query(..., min_length=1),
        caller: str = Query(..., min_length=1),
    ) -> dict[str, object]:
        try:
            resp = await service.query(key, caller)
        except QuotaExceededError as exc:
            logger.info("http reject 402: caller=%s key=%s reason=%s", caller, key, exc)
            raise HTTPException(status_code=402, detail={
                "reason": "quota_exceeded",
                "caller": caller,
                "key": key,
                "message": str(exc),
            }) from exc
        except CapacityExceededError as exc:
            logger.info("http reject 429: caller=%s key=%s reason=%s", caller, key, exc)
            raise HTTPException(status_code=429, detail={
                "reason": "capacity_exceeded",
                "caller": caller,
                "key": key,
                "message": str(exc),
            }) from exc
        except UpstreamUnavailableError as exc:
            logger.info("http reject 502: caller=%s key=%s reason=%s", caller, key, exc)
            raise HTTPException(status_code=502, detail={
                "reason": "upstream_failed",
                "caller": caller,
                "key": key,
                "message": str(exc),
            }) from exc

        return {
            "key": resp.key,
            "data": resp.data,
            "cache_state": resp.cache_state.value,
            "degraded": resp.degraded,
            "source_states": {k: s.value for k, s in resp.source_states.items()},
            "fallback_sources": resp.fallback_sources,
            "in_flight_shared": resp.in_flight_shared,
            "age": round(resp.age, 6),
            "detail": resp.detail,
        }

    @app.get("/metrics")
    async def metrics() -> JSONResponse:
        return JSONResponse(service.metrics_snapshot())

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
