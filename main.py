"""本地演示入口：装配三个模拟来源（含一个可制造超时/失败的来源）。

运行：uv run uvicorn main:app --port 8000
示例：
  curl 'http://127.0.0.1:8000/query?key=demo&caller=alpha'
  curl 'http://127.0.0.1:8000/metrics'
"""
from __future__ import annotations

import asyncio
import os

import uvicorn
from fastapi import FastAPI

from app.api import create_app
from app.config import QuotaPolicy, ResilienceConfig
from app.models import Priority
from app.service import ResilienceService


async def source_primary(key: str) -> dict[str, str]:
    await asyncio.sleep(0.02)
    return {"payload": f"primary:{key}"}


async def source_secondary(key: str) -> dict[str, str]:
    await asyncio.sleep(0.05)
    return {"payload": f"secondary:{key}"}


async def source_flaky(key: str) -> dict[str, str]:
    # 环境变量 FLAKY=timeout/error 时用于手工演示降级；默认正常。
    mode = os.environ.get("FLAKY", "")
    if mode == "timeout":
        await asyncio.sleep(2.0)
    if mode == "error":
        raise RuntimeError("flaky source forced error")
    await asyncio.sleep(0.03)
    return {"payload": f"flaky:{key}"}


def build_service() -> ResilienceService:
    config = ResilienceConfig(
        default_source_timeout=0.2,
        aggregate_timeout=0.5,
        fetch_concurrency=8,
        global_capacity=8,
        fresh_ttl=1.0,
        stale_ttl=5.0,
        refresh_min_interval=0.1,
    )
    policy = QuotaPolicy(
        quotas={"alpha": 4, "beta": 2, "gamma": 2},
        priorities={"alpha": Priority.HIGH, "beta": Priority.LOW, "gamma": Priority.LOW},
    )
    return ResilienceService(
        sources={
            "primary": source_primary,
            "secondary": source_secondary,
            "flaky": source_flaky,
        },
        config=config,
        quota_policy=policy,
    )


service = build_service()
app: FastAPI = create_app(service)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
