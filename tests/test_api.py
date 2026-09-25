import logging

from httpx2 import ASGITransport, AsyncClient

from app.api import create_app
from app.config import QuotaPolicy, ResilienceConfig
from app.models import Priority
from app.service import ResilienceService
from tests.helpers import MockSource

logger = logging.getLogger("tests.test_api")


def _service() -> ResilienceService:
    sources = {
        "primary": MockSource("primary", delay=0.01, value="p"),
        "broken": MockSource("broken", fail=True),
    }
    cfg = ResilienceConfig(
        default_source_timeout=0.1,
        aggregate_timeout=0.3,
        global_capacity=3,
        fresh_ttl=1.0,
        stale_ttl=5.0,
    )
    policy = QuotaPolicy(quotas={"tight": 1}, priorities={"tight": Priority.LOW})
    return ResilienceService(sources, config=cfg, quota_policy=policy)


async def test_query_endpoint_shape_and_partial_degradation() -> None:
    service = _service()
    transport = ASGITransport(app=create_app(service))
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/query", params={"key": "abc", "caller": "anyone"})
        assert resp.status_code == 200
        body = resp.json()
        logger.info("api ok input=abc/anyone body=%s", body)
        assert body["key"] == "abc"
        assert body["data"] == {"primary": "p"}
        assert body["degraded"] is True
        assert body["source_states"]["broken"] == "error"
        assert body["source_states"]["primary"] == "ok"
        assert body["cache_state"] == "miss"

        second = (
            await client.get("/query", params={"key": "abc", "caller": "anyone"})
        ).json()
        logger.info("api second call cache_state=%s", second["cache_state"])
        assert second["cache_state"] == "fresh"


async def test_quota_rejection_returns_distinct_402() -> None:
    # 输入：tight 配额 1，先手工占住其唯一槽位；HTTP 查询必须得到 402 而非 429。
    service = _service()
    await service.gate.acquire("tight")
    transport = ASGITransport(app=create_app(service))
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/query", params={"key": "q", "caller": "tight"})
        logger.info("api reject input=tight status=%d body=%s", resp.status_code, resp.json())
        assert resp.status_code == 402
        assert resp.json()["detail"]["reason"] == "quota_exceeded"
    await service.gate.release("tight")


async def test_validation_and_observability_endpoints() -> None:
    service = _service()
    transport = ASGITransport(app=create_app(service))
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        missing = await client.get("/query", params={"key": "x"})
        assert missing.status_code == 422
        assert (await client.get("/health")).json() == {"status": "ok"}
        metrics = (await client.get("/metrics")).json()
        logger.info("api metrics keys=%s", sorted(metrics.keys()))
        assert {"fetch_started", "rejected_quota", "rejected_capacity"} <= set(metrics)


async def test_all_sources_down_returns_explicit_502() -> None:
    # 输入：唯一来源始终失败、无缓存；判定：HTTP 502 + upstream_failed 原因，
    # 而不是返回空数据冒充成功。
    sources = {"only": MockSource("only", fail=True)}
    cfg = ResilienceConfig(
        default_source_timeout=0.1,
        aggregate_timeout=0.2,
        global_capacity=2,
        fresh_ttl=0.01,
        stale_ttl=0.01,
    )
    service = ResilienceService(sources, config=cfg)
    transport = ASGITransport(app=create_app(service))
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.get("/query", params={"key": "doomed", "caller": "c"})
        logger.info("api upstream-fail status=%d body=%s", resp.status_code, resp.json())
        assert resp.status_code == 502
        assert resp.json()["detail"]["reason"] == "upstream_failed"
        assert service.metrics_snapshot()["fetch_failures"] == 1
