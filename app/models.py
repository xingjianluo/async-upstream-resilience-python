"""领域模型：跨层共享的数据类型与枚举。

所有枚举/数据类只描述“是什么”，不含业务逻辑，
聚合、缓存、配额各层之间通过这些类型交互。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class Priority(Enum):
    """调用方优先级：HIGH 在共享容量内优先被接纳。"""

    LOW = 1
    HIGH = 2


class SourceState(Enum):
    """单个来源在一次聚合中的最终状态。"""

    OK = "ok"              # 成功返回并参与合并
    TIMEOUT = "timeout"    # 超过单次来源超时
    ERROR = "error"        # 来源主动失败
    CANCELLED = "cancelled"  # 因整体超时/调用方取消而未完成


class CacheState(Enum):
    """对外结果相对缓存的解释。"""

    MISS = "miss"          # 无缓存，走了一次真实回源
    FRESH = "fresh"        # 命中未过期值，未回源
    STALE = "stale"        # 回源失败，返回在 stale_ttl 宽限内的旧值
    STALE_FRESH = "stale_fresh"  # 命中已过 fresh_ttl 但仍在 stale_ttl 内的旧值（同步回源成功）
    EXPIRED_REJECT = "expired_reject"  # 无可用旧值且回源失败，明确拒绝
    IN_FLIGHT = "in_flight"  # 复用了同键在途回源


class RejectReason(Enum):
    """配额/容量拒绝原因，两类必须可区分。"""

    QUOTA_EXCEEDED = "quota_exceeded"      # 调用方自身并发配额用尽
    CAPACITY_EXCEEDED = "capacity_exceeded"  # 全局共享容量用尽
    UPSTREAM_FAILED = "upstream_failed"    # 无缓存且全部来源不可用


@dataclass
class SourceResult:
    """单个来源的结果或失败标注。失败时 value=None。

    回退语义：本次实时取数失败（state 非 OK）但命中按来源短期回退时，
    fallback=True、value 为最近一次成功值、fallback_age 为其大致年龄；
    state 保留真实失败原因，绝不把回退值伪装成本次实时值。
    """

    source: str
    state: SourceState
    value: object | None = None
    error: str | None = None
    elapsed: float = 0.0
    fallback: bool = False
    fallback_age: float | None = None


@dataclass
class AggregatedResult:
    """聚合对外结果：缺失来源以状态显式标注，绝不冒充正常数据。"""

    key: str
    sources: dict[str, SourceResult]
    merged: object | None
    degraded: bool
    total_elapsed: float
    fetched_at: float = field(default_factory=time.monotonic)

    @property
    def any_ok(self) -> bool:
        return any(s.state is SourceState.OK for s in self.sources.values())

    @property
    def any_usable(self) -> bool:
        """是否存在可用数据：实时 OK 或命中回退的来源都算。"""
        return any(
            s.state is SourceState.OK or s.fallback for s in self.sources.values()
        )


@dataclass
class CacheEntry:
    """缓存条目：monotonic 时间基准，fresh_ttl 内直接命中。"""

    key: str
    result: AggregatedResult
    stored_at: float
    fresh_ttl: float
    stale_ttl: float

    def fresh_until(self) -> float:
        return self.stored_at + self.fresh_ttl

    def expire_at(self) -> float:
        return self.stored_at + self.fresh_ttl + self.stale_ttl


@dataclass
class QueryResponse:
    """对外查询响应：数据 + 完整可解释性标注。

    fallback_sources: 本次以回退值顶上的来源及其大致年龄（秒）；
    与 source_states 对照即可区分实时值与回退值。
    """

    key: str
    data: object | None
    cache_state: CacheState
    degraded: bool
    source_states: dict[str, SourceState]
    in_flight_shared: bool
    age: float
    detail: str
    fallback_sources: dict[str, float] = field(default_factory=dict)


class ResilienceError(Exception):
    """本服务对外可解释错误的基类。"""


class QuotaExceededError(ResilienceError):
    """调用方并发配额超出。"""


class CapacityExceededError(ResilienceError):
    """全局共享容量不足（被更高优先级或存量占用）。"""


class UpstreamUnavailableError(ResilienceError):
    """无任何可用数据：无缓存/旧值不可用且回源失败。"""
