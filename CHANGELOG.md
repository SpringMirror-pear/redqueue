# Changelog / 版本变更记录

All notable public release changes are documented here.

所有公开发布版本的重要变更都会记录在此文件中。

Development versions are tracked separately from formal release versions.
开发版本与正式版本分开管理。

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
