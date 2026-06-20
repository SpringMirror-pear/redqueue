# RedQueue API / RedQueue API 文档

This document describes the public API available in RedQueue `0.11.0`.

本文档描述 RedQueue `0.11.0` 的公开 API。

## Clients / 客户端

### `QueueClient`

Synchronous queue client.

同步队列客户端。

```python
from redqueue import QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="emails",
    backend="list",
)
```

Methods / 方法：

- `from_url(url, *, queue, backend="list", connection_manager=None, **options) -> QueueClient`
- `publish(payload, *, delay=None, headers=None, message_id=None) -> str`
- `consume(*, timeout=None, batch_size=1) -> Message | list[Message] | None`
- `ack(message) -> None`
- `nack(message, *, requeue=True) -> None`
- `retry(message, *, delay=None, reason=None) -> None`
- `delay(payload, *, delay_seconds=None, run_at=None, headers=None) -> str`
- `schedule_due(*, limit=100, now=None) -> int`
- `recover_stale(*, min_idle_ms=None, limit=100) -> int`
- `dead_letters(*, limit=100) -> list[Message]`
- `requeue_dead(message) -> None`
- `close() -> None`
- `with QueueClient.from_url(...): ...`

### `AsyncQueueClient`

Asynchronous queue client with API parity to `QueueClient`.

与 `QueueClient` 对齐的异步队列客户端。

```python
from redqueue import AsyncQueueClient

client = await AsyncQueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="jobs",
    backend="list",
)
```

Methods / 方法：

- `await from_url(url, *, queue, backend="list", connection_manager=None, **options) -> AsyncQueueClient`
- `await publish(payload, *, delay=None, headers=None, message_id=None) -> str`
- `await consume(*, timeout=None, batch_size=1) -> Message | list[Message] | None`
- `await ack(message) -> None`
- `await nack(message, *, requeue=True) -> None`
- `await retry(message, *, delay=None, reason=None) -> None`
- `await delay(payload, *, delay_seconds=None, run_at=None, headers=None) -> str`
- `await schedule_due(*, limit=100, now=None) -> int`
- `await recover_stale(*, min_idle_ms=None, limit=100) -> int`
- `await dead_letters(*, limit=100) -> list[Message]`
- `await requeue_dead(message) -> None`
- `await close() -> None`
- `async with await AsyncQueueClient.from_url(...): ...`

## Connection Management / 连接管理

### `RedisConnectionManager`

Synchronous Redis connection pool owner.

同步 Redis 连接池所有者。

```python
from redqueue import QueueClient, RedisConnectionManager

with RedisConnectionManager(
    "redis://127.0.0.1:6379/0",
    max_connections=20,
) as manager:
    client = QueueClient.from_url(
        manager.url,
        queue="emails",
        connection_manager=manager,
    )
```

Methods / 方法：

- `redis() -> redis.Redis`
- `close() -> None`
- `with RedisConnectionManager(...): ...`

### `AsyncRedisConnectionManager`

Asynchronous Redis connection pool owner.

异步 Redis 连接池所有者。

```python
from redqueue import AsyncQueueClient, AsyncRedisConnectionManager

async with AsyncRedisConnectionManager(
    "redis://127.0.0.1:6379/0",
    max_connections=20,
) as manager:
    client = await AsyncQueueClient.from_url(
        manager.url,
        queue="jobs",
        connection_manager=manager,
    )
```

Methods / 方法：

- `redis() -> redis.asyncio.Redis`
- `await close() -> None`
- `async with AsyncRedisConnectionManager(...): ...`

Clients created from a connection manager do not close the shared pool when the
client is closed. Close the manager to release shared connections.

通过连接管理器创建的客户端在关闭时不会关闭共享连接池。需要释放共享连接时，
请关闭连接管理器。

## Configuration / 配置

### `QueueConfig`

Queue configuration shared by sync and async clients.

同步和异步客户端共享的队列配置。

Important fields / 重要字段：

- `queue: str`
- `backend: BackendType | str = BackendType.LIST`
- `enable_delay: bool = False`
- `namespace: str = "rq"`
- `retry: RetryConfig`
- `monitoring: MonitoringHook`
- `serializer: Serializer`
- `visibility_timeout_seconds: float = 300.0`
- `consumer_group: str = "redqueue"`
- `consumer_name: str | None = None`
- `metadata: dict[str, Any]`

### `RetryConfig`

Retry policy for failed messages.

失败消息的重试策略。

- `max_retries: int = 3`
- `base_delay_seconds: float = 0.0`
- `max_delay_seconds: float | None = None`
- `next_delay(attempts: int) -> float`

### `BackendType`

Supported backends.

支持的后端。

- `BackendType.LIST`
- `BackendType.STREAM`

## Message / 消息

### `Message`

Normalized message returned by consumers.

消费者返回的标准化消息。

Fields / 字段：

- `id: str`
- `queue: str`
- `payload: Any`
- `headers: dict[str, Any]`
- `attempts: int`
- `created_at: float`
- `available_at: float | None`
- `backend: str | None`
- `raw_id: str | None`
- `raw_payload: bytes | None`

Helpers / 辅助方法：

- `with_attempt() -> Message`
- `with_backend(backend, *, raw_id=None, raw_payload=None) -> Message`
- `new_message_id() -> str`

## Backends / 后端

### List

The List backend is the baseline reliable queue backend. It stores ready,
processing, and dead-letter messages in namespaced Redis keys.

List 后端是基础可靠队列后端，使用带 namespace 的 Redis key 存储 ready、
processing 和 dead-letter 消息。

Redis `>=6.2` uses `BLMOVE`; Redis `>=2.2` fallback uses `BRPOPLPUSH`.

Redis `>=6.2` 使用 `BLMOVE`；Redis `>=2.2` 回退使用 `BRPOPLPUSH`。

### Streams

The Streams backend uses Redis consumer groups and requires Redis `>=5.0`.

Streams 后端使用 Redis 消费组，要求 Redis `>=5.0`。

Redis `>=6.2` uses `XAUTOCLAIM` for pending recovery. Redis 5.x uses
`XPENDING` and `XCLAIM` fallback.

Redis `>=6.2` 使用 `XAUTOCLAIM` 恢复 pending 消息。Redis 5.x 回退使用
`XPENDING` 和 `XCLAIM`。

### Delay

Delayed tasks are stored in Redis Sorted Set with Unix timestamp scores.
`schedule_due()` moves due messages to the selected backend.

延迟任务存储在 Redis Sorted Set 中，score 为 Unix 时间戳。`schedule_due()`
将到期消息转移到所选后端。

## Serialization / 序列化

### `Serializer`

Protocol for custom payload serializers.

自定义 payload 序列化器协议。

- `content_type: str`
- `encode(payload, *, queue=None) -> bytes`
- `decode(payload, *, queue=None) -> Any`

### `JsonSerializer`

Default serializer. JSON-compatible objects are encoded as UTF-8 bytes. Bytes,
bytearray, and memoryview payloads are passed through as bytes.

默认序列化器。JSON 兼容对象会编码为 UTF-8 bytes。bytes、bytearray 和
memoryview payload 会按 bytes 透传。

## Exceptions / 异常

All RedQueue custom exceptions inherit from `RedQueueError`.

所有 RedQueue 自定义异常都继承 `RedQueueError`。

- `RedQueueError`
- `RedisCompatibilityError`
- `QueueConfigError`
- `MessageEncodeError`
- `MessageDecodeError`
- `BackendUnavailableError`
- `AckError`
- `RetryExceededError`

`RedQueueError.to_dict()` returns structured context for logs and APIs.

`RedQueueError.to_dict()` 返回适合日志和 API 的结构化上下文。

## Monitoring / 监控

### Events / 事件

- `client.created`
- `message.published`
- `message.consumed`
- `message.acked`
- `message.nacked`
- `message.retried`
- `message.dead_lettered`
- `delay.scheduled`
- `delay.released`
- `backend.error`

### Hooks / Hook

- `MonitoringHook`
- `NoopMonitoringHook`
- `InMemoryMonitoringHook`
- `CompositeMonitoringHook`
- `SafeMonitoringHook`

Monitoring events do not include business payload by default.

监控事件默认不包含业务 payload。

## Redis Capability Detection / Redis 能力探测

- `RedisVersion`
- `RedisCapabilities`
- `extract_redis_version(info)`
- `detect_capabilities(client)`
- `detect_capabilities_async(client)`

Streams are rejected with `RedisCompatibilityError` when Redis is below `5.0`.

当 Redis 低于 `5.0` 时，启用 Streams 会抛出 `RedisCompatibilityError`。
