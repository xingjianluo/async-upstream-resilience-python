
面向“多个调用方并发查询多个上游”的韧性层：并行聚合、防击穿缓存、调用方配额与
优先级调度。任何部分失败都**降级并显式标注**，不会把缺失数据当正常结果返回。

## 1. 架构与请求路径

```
HTTP /query ──> ResilienceService.query(key, caller)
                   │
                   ├─ 1. PriorityGate     配额 + 全局容量 + 优先级排队
                   ├─ 2. TTLCache         单飞合并、fresh/stale 取舍
                   └─ 3. SourceAggregator 多来源并行抓取（信号量 + 超时）
```

模块职责：

| 模块 | 职责 |
| --- | --- |
| `app/models.py` | 枚举与数据结构：来源状态、缓存状态、拒绝原因、对外响应 |
| `app/config.py` | 全部关键配置项（见第 4 节） |
| `app/aggregator.py` | 多来源并行、单来源超时、整体预算、部分失败降级 |
| `app/cache.py` | 单飞回源合并、TTL、stale-while-revalidate、失败取舍 |
| `app/quota.py` | 调用方并发配额、全局容量、HIGH/LOW 优先级队列 |
| `app/service.py` | 串联三层的门面，统一取消清理 |
| `app/api.py` | FastAPI 装配与可区分的拒绝状态码 |
| `app/metrics.py` | 锁内一致更新的计数器，`/metrics` 暴露 |

## 2. 降级与缓存策略

### 2.1 多来源聚合（SourceAggregator）

- 所有来源**并行**发起，受 `fetch_concurrency` 信号量约束；
- 每个来源受单次超时约束（`source_timeouts` 可按来源覆盖，缺省
  `default_source_timeout`），超时标注 `timeout`；
- 整次查询受 `aggregate_timeout` 预算约束：预算耗尽时已完成的结果保留，
  未完成来源标注 `cancelled`，查询不因此失败；
- 来源抛异常标注 `error`。三种缺失都出现在响应的 `source_states` 中，
  且 `degraded=true`；合并结果只包含 `ok` 来源的数据。
- 全部来源都不可用时 `AggregatedResult.any_ok=false`，交由缓存层决定
  “返回旧值”还是“明确拒绝”。

### 2.2 缓存新旧值取舍（TTLCache，时间基于 monotonic）

设条目年龄 `age = now - stored_at`：

| 条件 | 对外状态 | 行为 |
| --- | --- | --- |
| `age < fresh_ttl` | `fresh` | 直接命中，不回源 |
| `fresh_ttl ≤ age < fresh_ttl + stale_ttl`，有在途刷新 | `stale`（`in_flight_shared=true`） | 旧值立即返回，共享在途回源 |
| 同上，且距上次尝试 ≥ `refresh_min_interval` | `stale` | 旧值先返回，后台单飞刷新（SWR） |
| 同上，但刷新节流窗口内 | `stale` | 只返回旧值，不重复回源 |
| 刷新成功（有任意来源 OK） | 后续请求 `fresh` | 新值落盘，年龄清零 |
| 刷新失败，旧值仍在宽限内且 `allow_stale_on_error=true` | `stale` | 返回旧值，`detail` 注明“回源失败 + 旧值年龄” |
| `age ≥ fresh_ttl + stale_ttl` 后回源失败 | HTTP 502 | 抛 `UpstreamUnavailableError`，明确拒绝 |
| 无缓存（miss），回源成功 | `miss`（领导）/ `in_flight`（加入者） | 同键仅一次真实回源 |
| 无缓存且回源全失败 | HTTP 502 | 明确拒绝，不返回空数据 |

**单飞（防击穿）**：同键并发下，第一个请求成为“领导”登记在途 future 并发起
**独立回源任务**；其余请求共享同一 future，绝不二次回源。加入者拿到结果时
状态标注为 `in_flight`，与领导的 `miss` 可区分。

**取消隔离**：领导/加入请求被调用方取消，不会取消真实回源任务——回源继续
完成并落盘，后续请求可直接命中；在途表在回源 settle 时必定清理，不残留。

## 3. 配额与优先级规则（PriorityGate）

每个请求需同时持有 **调用方配额槽** 与 **全局容量槽**，退出时（含异常/取消）
由 `Admission` 异步上下文管理器保证归还。

- **配额拒绝（HTTP 402 `quota_exceeded`）**：调用方在途数已达自身配额，
  立即拒绝、不排队；
- **容量拒绝（HTTP 429 `capacity_exceeded`）**：全局容量不足。默认
  `acquire()` 会排队（不返回该错误），仅 `acquire(wait=False)` 立即拒绝；
- **优先级**：等待队列按 `(-priority, 到达序号)` 排序，`HIGH` 永远排在所有
  `LOW` 之前，同优先级 FIFO——低优先级流量无法把高优先级完全挤占；
- **取消清理**：等待中的请求被取消时从队列摘除并触发一次重新调度，
  `admitted == released` 始终成立，`active_callers`/在途计数不串号。

拒绝原因在异常类型（`QuotaExceededError` / `CapacityExceededError` /
`UpstreamUnavailableError`）、指标计数与 HTTP 状态码三处都可区分。

## 4. 关键配置项（`app/config.py`）

| 配置 | 默认 | 含义 |
| --- | --- | --- |
| `default_source_timeout` | 0.5s | 单来源单次超时 |
| `aggregate_timeout` | 1.0s | 一次聚合的整体预算 |
| `fetch_concurrency` | 8 | 单次聚合内并行抓取信号量 |
| `global_capacity` | 16 | 全服务共享在途容量 |
| `fresh_ttl` | 1.0s | 新值新鲜期，期内不回源 |
| `stale_ttl` | 5.0s | 旧值可接受宽限期 |
| `allow_stale_on_error` | True | 回源失败时是否允许返回宽限内旧值 |
| `refresh_min_interval` | 0.05s | 每键刷新节流，防 stale 期回源风暴 |
| `QuotaPolicy.quotas` | 未登记=4 | 每调用方并发上限 |
| `QuotaPolicy.priorities` | 未登记=LOW | 每调用方优先级 |

所有 TTL 计时使用 `time.monotonic()`，不受系统时钟回拨影响。

## 5. 可观测性

`GET /metrics` 返回一致性快照，关键计数：

- 缓存：`cache_hits_fresh` / `cache_hits_stale` / `cache_misses`
- 回源：`fetch_started`、`fetch_shared`、`fetch_failures`、
  `fetch_by_key`（断言“同键仅一次回源”）
- 来源：`source_ok` / `source_timeout` / `source_error` / `source_cancelled`
- 配额门：`admitted`、`released`、`rejected_quota`、`rejected_capacity`
  及按调用方细分的拒绝计数

单测与运行日志对每个判定打印**输入**（key/caller/来源/延迟）与**判定依据**
（状态、年龄、缺失来源、拒绝原因）。

## 6. 本地验证

依赖使用 uv 管理（需要 Python ≥ 3.14）：

```bash
uv sync                                  # 安装含 pytest 的依赖
uv run pytest -q                         # 全部单测（23 个，告警即失败）
uv run pytest tests/test_cache.py -s -q  # 只跑缓存并发用例
```

启动演示服务（3 个模拟来源，flaky 可由环境变量制造故障）：

```bash
uv run uvicorn main:app --port 8000
curl 'http://127.0.0.1:8000/query?key=demo&caller=alpha'
curl 'http://127.0.0.1:8000/metrics'
FLAKY=timeout uv run uvicorn main:app --port 8000   # 演示来源超时降级
```

### 测试覆盖场景

| 文件 | 覆盖 |
| --- | --- |
| `tests/test_aggregator.py` | 部分失败、单来源超时、整体预算取消并清理、全失败 |
| `tests/test_cache.py` | 10 并发单飞、fresh 命中、SWR 刷新、失败返旧值、过期拒绝、领导取消不杀回源 |
| `tests/test_quota.py` | 配额 vs 容量拒绝可区分、HIGH 插队、等待取消无残留 |
| `tests/test_service_integration.py` | 20 并发同键单回源、配额拒绝不泄漏、调用方取消清理、高优先级不被饿死 |
| `tests/test_api.py` | 响应结构、402/422/502 状态码、健康与指标端点 |

模拟来源见 `tests/helpers.py`：可用 `delay` / `fail` 精确制造超时与异常，
并记录最大并发数，便于本地复现。
