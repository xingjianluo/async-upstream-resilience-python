"""来源协议与本地可控的模拟来源。

MockSource 按 key 指定行为，便于在单测中复现：
- "ok":      返回 {"source": name, "key": key}
- "fail":    抛出 RuntimeError（对应 SourceStatus.FAILED）
- "timeout": 睡眠远超聚合器单次超时，等待被取消（对应 SourceStatus.TIMEOUT）
- "delay":   自定义延迟秒数后成功

mode/delay 支持按 key 覆盖，未命中的 key 使用默认模式。
call_count 字段用于断言"真实回源次数"。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol


class Source(Protocol):
    """上游来源协议。"""

    name: str

    async def fetch(self, key: str) -> Any:
        """成功返回任意 JSON 值；失败抛出异常；超时由聚合器取消。"""
        ...


@dataclass
class MockSource:
    """本地可控模拟来源。"""

    name: str
    default_mode: str = "ok"
    default_delay: float = 0.0
    modes: dict[str, str] | None = None      # key -> ok|fail|timeout|delay
    delays: dict[str, float] | None = None   # key -> delay seconds
    fail_messages: dict[str, str] | None = None
    timeout_sleep: float = 5.0
    call_count: int = 0

    async def fetch(self, key: str) -> Any:
        self.call_count += 1
        mode = (self.modes or {}).get(key, self.default_mode)
        delay = (self.delays or {}).get(key, self.default_delay)
        if delay:
            await asyncio.sleep(delay)
        if mode == "fail":
            msg = (self.fail_messages or {}).get(key, f"{self.name} forced failure")
            raise RuntimeError(msg)
        if mode == "timeout":
            # 远大于聚合器 per_source_timeout，正常路径下应被取消
            await asyncio.sleep(self.timeout_sleep)
        return {"source": self.name, "key": key, "payload": f"{self.name}:{key}"}
