"""领域类型定义：来源状态、聚合结果、缓存快照、查询响应与指标。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SourceStatus(str, Enum):
    """单个来源在一次聚合中的状态（可解释降级标注）。"""

    OK = "ok"                    # 正常返回
    TIMEOUT = "timeout"          # 单次超时被取消
    FAILED = "failed"            # 上游显式失败
    DEGRADED = "degraded"        # 其他可预期降级（预留）


class Freshness(str, Enum):
    """返回数据的新鲜度标注。"""

    FRESH = "fresh"              # 新鲜期内
    STALE = "stale"              # 已过期但在宽限窗口内（回源失败或后台刷新中）
    UNCACHED = "uncached"        # 本次直接来自回源（首次获取）


class RejectReason(str, Enum):
    """可区分的拒绝原因。"""

    CALLER_QUOTA = "caller_quota_exceeded"  # 超出调用方自身配额
    CAPACITY = "capacity_full"              # 共享容量不足（含高优先级保留槽检查）
    NO_DATA = "no_usable_data"              # 全部来源失败且无可用旧值
    STALE_NOT_ALLOWED = "stale_not_allowed"  # 数据过期且调用方不接受旧值


@dataclass(frozen=True)
class SourceResult:
    """单个来源的结果或失败标注。"""

    source: str
    status: SourceStatus
    value: Any | None = None
    elapsed_ms: float = 0.0
    detail: str = ""


@dataclass(frozen=True)
class CacheEntryValue:
    """可缓存的数据载荷（聚合器产出）。"""

    merged: dict[str, Any]
    source_statuses: dict[str, str]
    ttl_seconds: float
    fetched_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class AggregatedResult:
    """聚合器一次聚合的完整对外可解释结果。"""

    merged: dict[str, Any]
    sources: list[SourceResult]
    degraded: bool
    elapsed_ms: float
    ttl_seconds: float | None = None  # None => 不可缓存（全部来源失败等）

    def to_cache_value(self) -> CacheEntryValue:
        return CacheEntryValue(
            merged=self.merged,
            source_statuses={s.source: s.status.value for s in self.sources},
            ttl_seconds=self.ttl_seconds or 0.0,
        )


@dataclass(frozen=True)
class CacheSnapshot:
    """缓存判定快照，随响应返回以便解释。"""

    state: Freshness
    age_seconds: float
    cache_hit: bool
    upstream_called: bool
    refresh_failures: int


@dataclass(frozen=True)
class QueryResponse:
    """/query 的对外响应。"""

    key: str
    caller: str
    priority: int
    accepted: bool
    data: dict[str, Any] | None
    source_statuses: dict[str, str]
    freshness: str
    degraded: bool
    cache: CacheSnapshot
    reason: str | None = None


@dataclass
class Metrics:
    """可观测计数器（并发下自洽，全部通过方法更新）。"""

    cache_hit: int = 0
    cache_miss: int = 0
    cache_stale_served: int = 0
    upstream_calls: int = 0
    upstream_dedup: int = 0
    refresh_failures: int = 0
    source_ok: int = 0
    source_timeout: int = 0
    source_failed: int = 0
    caller_quota_rejected: int = 0
    capacity_rejected: int = 0
    no_data_rejected: int = 0
    stale_rejected: int = 0
    in_flight: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            k: v
            for k, v in self.__dict__.items()
            if isinstance(v, int)
        }
