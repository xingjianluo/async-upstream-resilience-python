"""FastAPI 入口：组装并发保护的上游聚合与缓存层。

配置（环境变量，均有默认值）：
- RES_CAPACITY          共享在途容量
- RES_FRESH_TTL         缓存新鲜期秒数
- RES_STALE_TTL         过期宽限窗口秒数
- RES_SOURCE_TIMEOUT    单来源超时秒数
- RES_CONCURRENCY       来源并发上限
- RES_CALLER_QUOTAS     JSON，如 {"free":2}
- RES_PRIORITY_RESERVED JSON，如 {"1":1} 表示优先级>=1 保留 1 槽
- RES_MOCK_MODES        JSON，如 {"review":{"k":"fail"}}（演示/本地验证用）

示例：
  curl 'http://127.0.0.1:8000/query?key=p1&caller=vip&priority=1'
  curl 'http://127.0.0.1:8000/metrics'
"""

from __future__ import annotations

import json
import os

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from app.aggregator import Aggregator
from app.cache import SingleFlightCache
from app.observability import MetricsRegistry
from app.quota import QuotaManager
from app.service import QueryService
from app.sources import MockSource


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_json(name: str, default):
    raw = os.environ.get(name)
    return json.loads(raw) if raw else default


def build_service() -> QueryService:
    registry = MetricsRegistry()
    mock_modes = _env_json("RES_MOCK_MODES", {})
    source_names = ["inventory", "price", "review"]
    sources: list[MockSource] = []
    for name in source_names:
        cfg = mock_modes.get(name, {})
        sources.append(MockSource(
            name=name,
            default_mode=cfg.get("default_mode", "ok"),
            default_delay=float(cfg.get("default_delay", 0.0)),
            modes={k: v if isinstance(v, str) else v.get("mode", "ok")
                   for k, v in cfg.get("modes", {}).items()},
            delays={k: v.get("delay", 0.0) for k, v in cfg.get("modes", {}).items()
                    if isinstance(v, dict) and "delay" in v},
        ))

    aggregator = Aggregator(
        sources,
        per_source_timeout=_env_float("RES_SOURCE_TIMEOUT", 0.3),
        concurrency=_env_int("RES_CONCURRENCY", 8),
        fresh_ttl=_env_float("RES_FRESH_TTL", 1.0),
        degraded_ttl=_env_float("RES_DEGRADED_TTL", 0.2),
        registry=registry,
    )
    cache = SingleFlightCache(
        fresh_ttl=_env_float("RES_FRESH_TTL", 1.0),
        stale_ttl=_env_float("RES_STALE_TTL", 5.0),
        registry=registry,
    )
    quota = QuotaManager(
        capacity=_env_int("RES_CAPACITY", 10),
        caller_quotas=_env_json("RES_CALLER_QUOTAS", None),
        default_caller_quota=_env_int("RES_DEFAULT_CALLER_QUOTA", 100),
        priority_reserved={int(k): v for k, v in
                           _env_json("RES_PRIORITY_RESERVED", {}).items()},
        registry=registry,
    )
    return QueryService(aggregator, cache, quota, registry)


app = FastAPI(title="async-upstream-resilience")
service = build_service()


class QueryParams(BaseModel):
    key: str
    caller: str = "anonymous"
    priority: int = 0
    allow_stale: bool = True
    wait_refresh: bool = False


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "async-upstream-resilience", "docs": "/docs"}


@app.get("/query")
async def query(
    key: str,
    caller: str = "anonymous",
    priority: int = 0,
    allow_stale: bool = True,
    wait_refresh: bool = False,
):
    resp = await service.query(
        key, caller, priority,
        allow_stale=allow_stale, wait_refresh=wait_refresh,
    )
    return resp.__dict__ | {"cache": resp.cache.__dict__ | {
        "state": resp.cache.state.value
    }}


@app.get("/metrics")
def metrics() -> dict[str, int]:
    return service.registry.snapshot()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1",
                port=int(os.environ.get("RES_PORT", "8000")))
