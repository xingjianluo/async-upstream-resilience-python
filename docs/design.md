# 并发保护的上游聚合与缓存层

本服务在对外查询与多个上游来源之间增加三层保护：**多来源聚合**、**防击穿缓存（single-flight）**、
**调用方配额与优先级**。目标：并发、上游超时或部分失败时，对外结果仍然稳定且每一次取舍都可解释。

## 1. 多来源聚合与降级（`app/aggregator.py`）

- 一次查询的所有来源**并行**发起，`asyncio.Semaphore` 约束同时在途的来源请求数（`concurrency`）。
- 每个来源有独立单次超时 `per_source_timeout`，由 `asyncio.wait_for` 实施。
- 每个来源的结果都带明确状态（`source_statuses`）：
  - `ok`：正常返回，数据进入 `merged`；
  - `timeout`：超过单次超时被取消；
  - `failed`：来源抛出异常（附带异常类型与信息）。
- **部分失败只降级对应键位**：`merged` 中不会出现失败来源，响应 `degraded=true`；
  整次聚合不抛错。缺失数据永远不会被当成正常值返回。
- 全部来源失败时聚合结果 `ttl_seconds=null`，标记为**不可缓存**，由服务层拒绝为 `no_usable_data`。
- 缓存 TTL 规则：全部成功用 `fresh_ttl`；部分成功（降级）用较短的 `degraded_ttl`。
- 调用方整体取消时 `TaskGroup` 取消全部子任务，不在事件循环里残留在途来源调用。

## 2. 防击穿缓存与合并回源（`app/cache.py`）

同一数据键的并发请求只触发一次真实回源（single-flight）：后来的请求挂接在共享 future 上等待，
计入 `upstream_dedup`；真实回源次数计入 `upstream_calls`。

| 进入时缓存状态 | 默认行为 | `wait_refresh=true` | `allow_stale=false` |
|---|---|---|---|
| 无缓存（cold） | 合并回源，成功返回 `fresh`；失败拒绝 `no_usable_data` | 同左 | 同左 |
| 新鲜期内（age ≤ `fresh_ttl`） | 直接命中 `fresh`，不回源 | 同左 | 同左 |
| 宽限窗口内（`fresh_ttl` < age ≤ `fresh_ttl+stale_ttl`） | 立即返回 `stale` 旧值（带 `age_seconds`），后台 single-flight 刷新 | 同步等刷新：成功 `fresh` 新值；失败仍返回 `stale` 旧值 | 仍可返回窗口内旧值 |
| 超出宽限窗口 | 同步回源；成功 `fresh`，失败明确拒绝 `no_usable_data` | 同左 | 直接拒绝 `stale_not_allowed`，**不触发回源** |

取舍可解释性：

- 返回旧值时 `freshness=stale` 且 `cache.age_seconds` 给出旧值年龄；
- 回源失败但保留旧值时 `cache.refresh_failures` 累加并打印 `loader_failed` 日志；
- 窗口外的旧值**不**再静默使用——宁可明确拒绝，也不返回无法说明时效的数据。

取消安全：等待者自身被取消时用 `asyncio.shield` 保护共享回源，回源任务与后台刷新不受影响；
请求结束（含异常/取消）后不残留任何在途状态。不同 key 的回源与缓存条目互不计较、不串号。

## 3. 调用方配额、共享容量与优先级（`app/quota.py`）

准入检查顺序与拒绝原因（可区分）：

1. **调用方配额** `caller_quotas`（按调用方分桶的在途上限，未配置用 `default_caller_quota`）：
   超限拒绝 `caller_quota_exceeded`；
2. **共享容量** `capacity`：空闲槽位必须大于"严格高于当前优先级的保留槽总和"，
   否则拒绝 `capacity_full`。

优先级保留 `priority_reserved`：例如 `{"1": 1}` 表示容量中永远为优先级 ≥ 1 的请求保留 1 个槽，
低优先级流量最多占满其余槽位，无法把高优先级完全挤掉。

槽位通过 `async with quota.acquire(...)` 获取，`finally` 中释放；正常返回、异常、取消都会归还，
`in_flight` 与每个调用方的分桶计数在压测与取消后均归零。

## 4. 对外响应（`/query`）

`QueryResponse` 字段：

- `accepted` / `reason`：是否准入与可区分拒绝原因
  （`caller_quota_exceeded` / `capacity_full` / `no_usable_data` / `stale_not_allowed`）；
- `data`：按来源名聚合的数据（仅含 `ok` 来源）；
- `source_statuses`：每个来源的 `ok/timeout/failed`；
- `freshness`：`fresh/stale/uncached`；
- `degraded`：是否有来源降级；
- `cache`：`state/age_seconds/cache_hit/upstream_called/refresh_failures`。

## 5. 可观测性（`app/observability.py`）

- 所有关键判定以 JSON 行打印到 `resilience` logger：`query_begin / quota_check /
  cache_get / source_degrade / loader_failed / aggregate_done / query_done`，
  日志同时包含**输入**与**判定依据**（空闲槽、保留槽、age、ttl、异常信息等）。
- `GET /metrics` 输出一致性快照：缓存命中/未命中/旧值服务、回源/合并/刷新失败、
  来源 ok/timeout/failed、两类配额拒绝、无数据/旧值拒绝、当前在途数。

## 6. 关键配置项（环境变量）

| 变量 | 默认 | 含义 |
|---|---|---|
| `RES_CAPACITY` | 10 | 全局共享在途容量 |
| `RES_DEFAULT_CALLER_QUOTA` | 100 | 未显式配置调用方的在途配额 |
| `RES_CALLER_QUOTAS` | 空 | JSON，如 `{"free":2}` |
| `RES_PRIORITY_RESERVED` | 空 | JSON，如 `{"1":1}`，优先级 ≥1 保留 1 槽 |
| `RES_FRESH_TTL` | 1.0 | 缓存新鲜期（秒） |
| `RES_STALE_TTL` | 5.0 | 过期后可接受旧值的宽限窗口（秒） |
| `RES_DEGRADED_TTL` | 0.2 | 降级聚合结果的缓存 TTL（秒） |
| `RES_SOURCE_TIMEOUT` | 0.3 | 单来源单次超时（秒） |
| `RES_CONCURRENCY` | 8 | 来源请求并发上限 |
| `RES_MOCK_MODES` | 空 | JSON，注入模拟来源行为（演示/本地验证） |

## 7. 本地验证

```bash
uv sync

# 单元测试（含并发回源、部分失败、超时取消、超配额场景；日志打印输入与判定依据）
uv run pytest -q

# 启动（注入一个失败来源和一个超时来源，小容量便于观察拒绝）
RES_MOCK_MODES='{"review":{"modes":{"p1":"fail"}},"price":{"modes":{"p1":"timeout"}}}' \
RES_SOURCE_TIMEOUT=0.1 RES_CAPACITY=2 RES_CALLER_QUOTAS='{"free":1}' \
uv run python main.py

# 查询（部分失败降级，但整体成功、可解释）
curl 'http://127.0.0.1:8000/query?key=p1&caller=free'
# 高优先级
curl 'http://127.0.0.1:8000/query?key=p1&caller=vip&priority=1'
# 不接受旧值
curl 'http://127.0.0.1:8000/query?key=p1&caller=vip&allow_stale=false'
# 指标快照
curl 'http://127.0.0.1:8000/metrics'
```

测试文件索引：

- `tests/test_aggregator.py`：部分失败/超时/并发上限/整体取消/来源计数；
- `tests/test_cache.py`：并发 single-flight、新鲜命中、宽限旧值、刷新失败计数、
  超窗拒绝、等待者取消不影响共享回源、key 隔离；
- `tests/test_quota.py`：两类拒绝可区分、高优先级保留、取消/异常释放、分桶不串号；
- `tests/test_service.py`：端到端 20 并发合并回源、部分失败、全失败拒绝、
  优先级准入、取消无残留、旧值服务。
