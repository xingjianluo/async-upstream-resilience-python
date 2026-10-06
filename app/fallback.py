"""按来源的短期回退存储：记录每个 (查询键, 来源) 最近一次成功值。

定位：只兜“偶发抖动”——来源本次超时/失败时，若该来源在本键下的
历史成功值年龄 < fallback_ttl，就临时顶上；超过有效期一律不兜底，
回到既有的缺失/拒绝行为，绝不无限期拿历史值充数。

不变量：
- 只在来源真实成功（OK）时按 (key, source) 写入；部分成功不会清掉
  其他来源的基准值，失败也绝不写入/覆盖；
- 查询键之间完全隔离（键即 (key, source) 元组），无串数据；
- 过期条目在查询时惰性驱逐，不依赖后台任务；
- 全部方法为同步字典操作，运行在单事件循环内，无需额外锁。
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger("app.fallback")


@dataclass
class FallbackEntry:
    """一条回退基准值：monotonic 时间基准。"""

    value: object
    stored_at: float

    def age(self, now: float) -> float:
        return now - self.stored_at


class SourceFallbackStore:
    def __init__(self, ttl: float) -> None:
        self._ttl = ttl
        self._entries: dict[tuple[str, str], FallbackEntry] = {}

    @property
    def ttl(self) -> float:
        return self._ttl

    def record(self, key: str, source: str, value: object) -> None:
        """来源真实成功后登记基准值（只写成功值，失败永不写入）。"""
        if self._ttl <= 0:
            return
        self._entries[(key, source)] = FallbackEntry(value, time.monotonic())
        logger.info("fallback baseline stored: key=%s source=%s", key, source)

    def lookup(self, key: str, source: str) -> FallbackEntry | None:
        """取有效期内的基准值；缺失或超过 fallback_ttl 返回 None 并惰性驱逐。"""
        if self._ttl <= 0:
            return None
        ident = (key, source)
        entry = self._entries.get(ident)
        if entry is None:
            return None
        age = entry.age(time.monotonic())
        if age >= self._ttl:
            self._entries.pop(ident, None)
            logger.info(
                "fallback baseline expired, evicted: key=%s source=%s age=%.3fs ttl=%.3fs",
                key, source, age, self._ttl,
            )
            return None
        logger.info(
            "fallback baseline usable: key=%s source=%s age=%.3fs ttl=%.3fs",
            key, source, age, self._ttl,
        )
        return entry

    def size(self) -> int:
        """当前基准值条数（含未惰性清理的过期项），仅用于测试/观测。"""
        return len(self._entries)
