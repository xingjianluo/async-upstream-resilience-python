# async-upstream-resilience-python

受并发保护的上游聚合与缓存层：多来源并行聚合与部分失败降级、防击穿合并回源缓存、
调用方配额与优先级保护。

详见 [docs/design.md](docs/design.md)。

```bash
uv sync
uv run pytest -q
uv run python main.py
```
