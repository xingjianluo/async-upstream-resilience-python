"""可观测指标：计数器在锁内更新，保证并发下自洽、可断言。"""
from __future__ import annotations

import threading


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.cache_hits_fresh = 0
        self.cache_hits_stale = 0
        self.cache_misses = 0
        # fetch_started：真实发起回源次数；fetch_shared：复用在途次数
        self.fetch_started = 0
        self.fetch_shared = 0
        self.fetch_failures = 0  # 回源后无任何来源 OK
        # 每键真实回源次数，用于断言“同键只回源一次”
        self.fetch_by_key: dict[str, int] = {}
        # 按来源统计结果状态
        self.source_ok = 0
        self.source_timeout = 0
        self.source_error = 0
        self.source_cancelled = 0
        # 配额门
        self.rejected_quota = 0
        self.rejected_capacity = 0
        self.admitted = 0
        self.released = 0
        self.rejected_quota_by_caller: dict[str, int] = {}
        self.rejected_capacity_by_caller: dict[str, int] = {}

    def record_cache_hit_fresh(self, key: str) -> None:
        with self._lock:
            self.cache_hits_fresh += 1

    def record_cache_hit_stale(self, key: str) -> None:
        with self._lock:
            self.cache_hits_stale += 1

    def record_cache_miss(self, key: str) -> None:
        with self._lock:
            self.cache_misses += 1

    def record_fetch(self, key: str) -> None:
        with self._lock:
            self.fetch_started += 1
            self.fetch_by_key[key] = self.fetch_by_key.get(key, 0) + 1

    def record_fetch_share(self, key: str) -> None:
        with self._lock:
            self.fetch_shared += 1

    def record_fetch_failure(self, key: str) -> None:
        with self._lock:
            self.fetch_failures += 1

    def record_source(self, state: str) -> None:
        with self._lock:
            attr = f"source_{state}"
            setattr(self, attr, getattr(self, attr) + 1)

    def record_reject_quota(self, caller: str) -> None:
        with self._lock:
            self.rejected_quota += 1
            self.rejected_quota_by_caller[caller] = (
                self.rejected_quota_by_caller.get(caller, 0) + 1
            )

    def record_reject_capacity(self, caller: str) -> None:
        with self._lock:
            self.rejected_capacity += 1
            self.rejected_capacity_by_caller[caller] = (
                self.rejected_capacity_by_caller.get(caller, 0) + 1
            )

    def record_admission(self, caller: str) -> None:
        with self._lock:
            self.admitted += 1

    def record_release(self, caller: str) -> None:
        with self._lock:
            self.released += 1

    def snapshot(self) -> dict[str, object]:
        """一致性快照：同一把锁内拷贝全部计数。"""
        with self._lock:
            return {
                "cache_hits_fresh": self.cache_hits_fresh,
                "cache_hits_stale": self.cache_hits_stale,
                "cache_misses": self.cache_misses,
                "fetch_started": self.fetch_started,
                "fetch_shared": self.fetch_shared,
                "fetch_failures": self.fetch_failures,
                "fetch_by_key": dict(self.fetch_by_key),
                "source_ok": self.source_ok,
                "source_timeout": self.source_timeout,
                "source_error": self.source_error,
                "source_cancelled": self.source_cancelled,
                "rejected_quota": self.rejected_quota,
                "rejected_capacity": self.rejected_capacity,
                "admitted": self.admitted,
                "released": self.released,
                "rejected_quota_by_caller": dict(self.rejected_quota_by_caller),
                "rejected_capacity_by_caller": dict(
                    self.rejected_capacity_by_caller
                ),
            }
