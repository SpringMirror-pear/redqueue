# Changelog / 版本变更记录

All notable public release changes are documented here.

所有公开发布版本的重要变更都会记录在此文件中。

Development versions are tracked separately from formal release versions.
开发版本与正式版本分开管理。

## [0.13.1] - 2026-06-27

### Fixed

- Fixed a compatibility regression where legacy messages with blank
  `headers["trace_id"]` values could fail to construct or consume after
  upgrading to `0.13.0`.
- Fixed sync and async `publish(..., delay=..., message_id=...)` so delayed
  publishing preserves caller-provided message ids.

### 修复

- 修复升级到 `0.13.0` 后，历史消息中空白 `headers["trace_id"]` 可能导致
  消息构造或消费失败的兼容性回归。
- 修复同步和异步 `publish(..., delay=..., message_id=...)`，现在延迟发布会
  保留调用方传入的消息 ID。

## [0.13.0] - 2026-06-27

### Added

- Added first-class `trace_id` support to `Message`, sync and async
  `publish()`, sync and async `delay()`, List, Streams, and delayed task
  backends.
- Added trace propagation through message envelopes, retry, dead-letter,
  delayed release, and requeue flows while remaining compatible with existing
  `headers["trace_id"]` messages.
- Added `trace_id` to monitoring events and CLI message output for lifecycle
  correlation.
- Added CLI `--trace-id` support for `publish` and `delay`.
- Added `new_trace_id()` helper for callers that want RedQueue-generated trace
  identifiers.

### 新增

- `Message`、同步和异步 `publish()`、同步和异步 `delay()`、List、Streams
  和延迟任务后端新增一等 `trace_id` 支持。
- trace 会穿过消息 envelope、重试、死信、延迟释放和重放流程，并兼容已有的
  `headers["trace_id"]` 消息。
- 监控事件和 CLI 消息输出新增 `trace_id`，方便按链路聚合生命周期事件。
- CLI 的 `publish` 和 `delay` 命令新增 `--trace-id`。
- 新增 `new_trace_id()` helper，方便调用方生成 RedQueue trace 标识。

## [0.12.0] - 2026-06-23

### Added

- Added the `redqueue` CLI and `python -m redqueue` module entry point for
  developer debugging.
- Added CLI commands for Redis compatibility checks, queue statistics, message
  publish, consume with optional ack/nack/retry, delayed scheduling, due
  release, and dead-letter inspection.
- Added deterministic JSON CLI output and user-facing JSON validation errors.
- Added CLI unit tests with injected fake Redis and queue clients.

### Fixed

- Normalized `BRPOPLPUSH` timeout values to integer seconds for Redis versions
  older than 6.2, improving Redis 5.x List consume compatibility.

### 新增

- 新增 `redqueue` CLI 和 `python -m redqueue` 模块入口，方便开发者调试。
- 新增 Redis 兼容性检查、队列统计、消息发布、消费并可选 ack/nack/retry、
  延迟调度、到期释放和死信查看命令。
- CLI 输出稳定 JSON，并提供面向用户的 JSON 参数校验错误。
- 新增基于 fake Redis 和 fake queue client 的 CLI 单元测试。

### 修复

- 对 Redis 6.2 以下版本使用 `BRPOPLPUSH` 时，将 timeout 规范为整数秒，
  提升 Redis 5.x List 消费兼容性。

## [0.11.2] - 2026-06-21

### Fixed

- Fixed cleanup for directly constructed sync clients when owned Redis backend
  initialization fails.
- Fixed cleanup for directly constructed async clients when lazy backend
  initialization fails.
- Made sync and async client `close()` idempotent for owned Redis clients.

### 修复

- 修复直接构造同步客户端时，如果 owned Redis 的后端初始化失败，Redis client
  未释放的问题。
- 修复直接构造异步客户端时，如果懒加载后端初始化失败，Redis client 未释放的问题。
- 同步和异步客户端的 `close()` 对 owned Redis client 变为幂等。

## [0.11.1] - 2026-06-21

### Fixed

- Fixed resource cleanup in sync and async `from_url()` when Redis capability
  detection, configuration validation, or backend initialization fails after the
  client created an owned Redis connection.
- Added explicit `owns_redis` override support to sync and async `from_url()`
  for advanced ownership control.

### 修复

- 修复同步和异步 `from_url()` 在自动创建 Redis 连接后，如果 Redis 能力探测、
  配置校验或后端初始化失败，已创建连接未释放的问题。
- 同步和异步 `from_url()` 新增显式 `owns_redis` 覆盖支持，用于高级资源所有权
  控制。

## [0.11.0] - 2026-06-21

### Added

- Added synchronous `RedisConnectionManager` and asynchronous
  `AsyncRedisConnectionManager` for shared Redis connection pool ownership.
- Added client context manager support for explicit sync and async resource
  cleanup.
- Added `connection_manager` and `pool_options` support to sync and async
  `from_url` constructors.
- Added `README-zh-CN.md` and converted `README.md` to English-only content.
- Added `CONTRIBUTING.md` with the project branch model and contribution
  workflow.
- Added `CODE_OF_CONDUCT.md`.

### Changed

- Updated formal release version to `0.11.0`.
- Documented the branch model: `main`, `develop`, `feature/*`, `release/*`,
  and `hotfix/*`.

### 新增

- 新增同步 `RedisConnectionManager` 和异步 `AsyncRedisConnectionManager`，
  用于共享 Redis 连接池所有权管理。
- 新增客户端上下文管理器支持，用于显式释放同步和异步资源。
- 同步和异步 `from_url` 构造器新增 `connection_manager` 和 `pool_options`
  支持。
- 新增 `README-zh-CN.md`，并将 `README.md` 调整为纯英文文档。
- 新增 `CONTRIBUTING.md`，记录项目分支模型和贡献流程。
- 新增 `CODE_OF_CONDUCT.md`。

### 变更

- 正式版本更新为 `0.11.0`。
- 记录分支模型：`main`、`develop`、`feature/*`、`release/*` 和 `hotfix/*`。

## [0.10.1] - 2026-06-20

### Fixed

- Fixed Redis List `ack`, `nack`, `retry`, and dead-letter requeue when custom
  serializers do not produce deterministic bytes.
- Fixed delayed task scheduling cleanup so payload keys are removed if `ZADD`
  fails after `SET`.
- Fixed Streams dead-letter reads and moves by ensuring the consumer group also
  exists on the dead-letter stream.
- Fixed async client creation monitoring so `AsyncQueueClient` emits the same
  `client.created` event as the synchronous client.
- Removed duplicate delay scheduling monitoring events from the synchronous
  client facade.
- Normalized internal Redis Streams entry ids returned as bytes.

### 修复

- 修复自定义序列化器输出字节不稳定时，Redis List `ack`、`nack`、`retry`
  和死信重放无法精确删除原始消息的问题。
- 修复延迟任务在 `SET` 成功但 `ZADD` 失败时残留 payload key 的问题。
- 修复 Streams 死信读取和搬移前未确保死信 stream 消费组存在的问题。
- 修复异步客户端创建时未与同步客户端一致发出 `client.created` 监控事件的问题。
- 移除同步客户端门面层重复发出的延迟调度监控事件。
- 统一规范化 Redis Streams 返回的 bytes 类型 entry id。

### Validation

- `python -m ruff check .`
- `PYTHONPATH=src python -m mypy`
- `PYTHONPATH=src python -m pytest`
- `REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration`

### 验证

- `python -m ruff check .`
- `PYTHONPATH=src python -m mypy`
- `PYTHONPATH=src python -m pytest`
- `REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration`

## [0.10.0] - 2026-06-20

### Added

- Initial public release of RedQueue.
- Synchronous `QueueClient` and asynchronous `AsyncQueueClient`.
- Redis List backend with reliable processing, ack, nack, retry, recovery, and
  dead-letter support.
- Redis Streams backend with consumer groups, ack, retry, dead-letter support,
  and pending recovery.
- Delayed tasks based on Redis Sorted Set.
- Redis version and capability detection through `INFO server`.
- Unified exception hierarchy with structured context.
- JSON serializer and custom serializer protocol.
- Monitoring hook system with safe wrapper, in-memory hook, and composite hook.
- Unit, contract, asynchronous, and opt-in Redis integration tests.
- GitHub Actions CI and local quality check script.

### 新增

- RedQueue 首个公开发布版本。
- 同步 `QueueClient` 与异步 `AsyncQueueClient`。
- Redis List 后端，支持可靠处理、ack、nack、retry、恢复和死信。
- Redis Streams 后端，支持消费组、ack、retry、死信和 pending 恢复。
- 基于 Redis Sorted Set 的延迟任务。
- 通过 `INFO server` 进行 Redis 版本与能力探测。
- 带结构化上下文的统一异常体系。
- JSON 序列化器与自定义序列化协议。
- 监控 hook 系统，包含安全包装、内存 hook 和组合 hook。
- 单元测试、契约测试、异步测试和可选 Redis 集成测试。
- GitHub Actions CI 与本地质量检查脚本。

### Compatibility

- Python `>=3.9`.
- Runtime dependency `redis==6.4.0`.
- Redis Streams require Redis `>=5.0`.
- Redis Streams `XAUTOCLAIM` and List `BLMOVE` are used when Redis `>=6.2`.
- Delayed tasks use Redis Sorted Set.

### 兼容性

- Python `>=3.9`。
- 运行依赖 `redis==6.4.0`。
- Redis Streams 要求 Redis `>=5.0`。
- Redis `>=6.2` 时使用 Streams `XAUTOCLAIM` 与 List `BLMOVE`。
- 延迟任务使用 Redis Sorted Set。

### Validation

- `python -m ruff check .`
- `PYTHONPATH=src python -m mypy`
- `PYTHONPATH=src python -m pytest`
- `REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration`

### 验证

- `python -m ruff check .`
- `PYTHONPATH=src python -m mypy`
- `PYTHONPATH=src python -m pytest`
- `REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration`
