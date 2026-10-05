"""关键配置项：集中定义，便于文档与调参。

时间单位均为秒；TTL 计时基于 time.monotonic()，不受系统时钟回拨影响。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.models import Priority

DEFAULT_QUOTA = 4


@dataclass(frozen=True)
class QuotaPolicy:
    """调用方并发配额策略：按 caller 授权并发上限与优先级。

    - quotas: 每个调用方在途请求数上限；未登记调用方使用 DEFAULT_QUOTA。
    - priorities: 每个调用方的优先级；未登记调用方默认 LOW。
    """

    quotas: dict[str, int] = field(default_factory=dict)
    priorities: dict[str, Priority] = field(default_factory=dict)

    def quota_of(self, caller: str) -> int:
        return self.quotas.get(caller, DEFAULT_QUOTA)

    def priority_of(self, caller: str) -> Priority:
        return self.priorities.get(caller, Priority.LOW)


@dataclass(frozen=True)
class ResilienceConfig:
    """聚合/缓存层总配置。

    关键项：
    - default_source_timeout: 单个来源的单次超时（来源可用 source_timeouts 覆盖）。
    - aggregate_timeout: 一次聚合的整体预算，超时后未完成来源标注 CANCELLED。
    - fetch_concurrency: 同一次聚合内并行抓取的并发上限（信号量）。
    - global_capacity: 全服务共享在途容量，所有调用方竞争。
    - fresh_ttl: 缓存新值新鲜期，期内命中不回源。
    - stale_ttl: 新鲜期之后旧值仍可接受的宽限期；超宽限期旧值不再返回。
    - allow_stale_on_error: 回源失败时是否允许返回宽限期内旧值。
    - refresh_min_interval: 每键回源节流间隔，防止 stale 期并发风暴。
    - fallback_ttl: 按来源短期回退的有效期：来源本次失败时，最近一次
      成功值在此期限内可临时顶上（显式标注为回退值）；<=0 关闭回退。
    """

    default_source_timeout: float = 0.5
    aggregate_timeout: float = 1.0
    fetch_concurrency: int = 8
    global_capacity: int = 16
    fresh_ttl: float = 1.0
    stale_ttl: float = 5.0
    allow_stale_on_error: bool = True
    refresh_min_interval: float = 0.05
    fallback_ttl: float = 2.0
