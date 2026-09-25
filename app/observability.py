"""指标注册表与判定日志。

- MetricsRegistry: 进程内共享计数器，锁保护，快照为一致性拷贝；
- log_decision: 以 JSON 行打印每次判定的输入与依据，测试中可通过 caplog 核对。
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
from typing import Any

from app.models import Metrics

logger = logging.getLogger("resilience")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(name)s %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False


class MetricsRegistry:
    """单进程共享指标。in_flight 之外的计数器单调递增。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.metrics = Metrics()

    def incr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            cur = getattr(self.metrics, name)
            setattr(self.metrics, name, cur + amount)

    def decr(self, name: str, amount: int = 1) -> None:
        with self._lock:
            cur = getattr(self.metrics, name)
            setattr(self.metrics, name, cur - amount)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dataclasses.asdict(self.metrics)

    def reset(self) -> None:
        with self._lock:
            self.metrics = Metrics()


def log_decision(event: str, **fields: Any) -> None:
    """打印一次判定的输入与依据（JSON 行）。"""
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))
