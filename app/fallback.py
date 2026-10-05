"""按来源的短期回退存储。

同一查询键下，记录每个来源最近一次成功值及其写入时间（monotonic 基准）：
- 只有真实成功的值才写入——失败/回退值都不会覆盖基准，
  因此一次部分成功不会冲掉其他来源此前可用的基准数据；
- 来源本次超时/失败时，聚合层查询 lookup：age <= ttl 的值可临时顶上，
  超过有效期返回 None，回到既有的缺失/拒绝行为，不会无限兜底；
- ttl <= 0 时整体关闭回退。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger("app.fallback")


@dataclass
class FallbackValue:
    """一条回退记录：值 + 写入时的 monotonic 时间。"""

    value: object
    stored_at: float

    def age(self, now: float) -> float:
        return now - self.stored_at


class SourceFallbackStore:
    """(key, source) 粒度的最近成功值存储。

    只在事件循环线程内被聚合层访问（fetch 收尾阶段同步调用），
    无跨线程写入，故用普通 dict 即可。
    """

    def __init__(self, ttl: float) -> None:
        self._ttl = ttl
        self._values: dict[str, dict[str, FallbackValue]] = {}

    @property
    def enabled(self) -> bool:
        return self._ttl > 0

    def record_success(self, key: str, source: str, value: object) -> None:
        """来源真实成功后刷新基准值。失败与回退值一律不写入。"""
        if not self.enabled:
            return
        self._values.setdefault(key, {})[source] = FallbackValue(
            value=value, stored_at=time.monotonic()
        )
        logger.info("fallback baseline stored: key=%s source=%s", key, source)

    def lookup(self, key: str, source: str) -> FallbackValue | None:
        """返回有效期内的回退值；无记录、已过期或已关闭时返回 None。"""
        if not self.enabled:
            return None
        record = self._values.get(key, {}).get(source)
        if record is None:
            return None
        age = record.age(time.monotonic())
        if age > self._ttl:
            logger.info(
                "fallback expired: key=%s source=%s age=%.3fs ttl=%.3fs",
                key, source, age, self._ttl,
            )
            return None
        logger.info(
            "fallback usable: key=%s source=%s age=%.3fs ttl=%.3fs",
            key, source, age, self._ttl,
        )
        return record
