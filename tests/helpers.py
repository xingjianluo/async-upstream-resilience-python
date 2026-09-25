"""本地可控模拟来源：可制造延迟、超时、异常与并发计数。"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger("tests.helpers")


class MockSource:
    def __init__(
        self,
        name: str,
        *,
        delay: float = 0.0,
        fail: bool = False,
        value: object = None,
    ) -> None:
        self.name = name
        self.delay = delay
        self.fail = fail
        self.value = value
        self.calls = 0
        self.active = 0
        self.max_active = 0

    async def __call__(self, key: str) -> object:
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        logger.info(
            "mock input: source=%s key=%s delay=%.3f fail=%s (call #%d)",
            self.name, key, self.delay, self.fail, self.calls,
        )
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if self.fail:
                raise RuntimeError(f"source {self.name} forced failure")
            return self.value if self.value is not None else f"{self.name}:{key}"
        finally:
            self.active -= 1
