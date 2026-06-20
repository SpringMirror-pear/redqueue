# RedQueue

RedQueue is a Redis-backed Python message queue library with List, Streams,
delayed tasks, synchronous APIs, asynchronous APIs, compatibility checks, and
monitoring hooks.

RedQueue 是一个基于 Redis 的 Python 消息队列库，支持 List、Streams、延迟任务、
同步 API、异步 API、兼容性检查和监控 hook。

Repository / 仓库：
https://github.com/SpringMirror-pear/redqueue.git

## Features / 功能

- Redis List reliable queue with `BLMOVE` on Redis `>=6.2` and `BRPOPLPUSH`
  fallback on older compatible Redis versions.
- Redis Streams backend with consumer groups. Streams require Redis `>=5.0`.
- Delayed tasks based on Redis Sorted Set.
- Sync client `QueueClient` and async client `AsyncQueueClient`.
- Unified exception hierarchy with structured context.
- Monitoring events for publish, consume, ack, nack, retry, dead letter, delay,
  and backend errors.
- Redis capability detection from `INFO server`.
- Apache License 2.0.

- 基于 Redis List 的可靠队列：Redis `>=6.2` 使用 `BLMOVE`，低版本兼容时回退
  `BRPOPLPUSH`。
- 基于 Redis Streams 的消费组后端，Streams 要求 Redis `>=5.0`。
- 基于 Redis Sorted Set 的延迟任务。
- 同步客户端 `QueueClient` 与异步客户端 `AsyncQueueClient`。
- 带结构化上下文的统一异常体系。
- 针对发布、消费、确认、拒绝、重试、死信、延迟和后端错误的监控事件。
- 通过 `INFO server` 探测 Redis 能力。
- Apache License 2.0。

## Compatibility / 兼容性

Runtime:

- Python `>=3.9`
- redis-py `6.4.0`
- Target development environment: Python `3.14.5`

Redis:

| Feature | Redis requirement | Notes |
| --- | --- | --- |
| List blocking consume | `>=2.0` | Uses `BLPOP` family compatibility baseline |
| List reliable move | `>=2.2` | Uses `BRPOPLPUSH`; `BLMOVE` preferred on `>=6.2` |
| Streams | `>=5.0` | Uses `XADD`, `XGROUP CREATE`, `XREADGROUP` |
| Streams auto claim | `>=6.2` | Uses `XAUTOCLAIM`; Redis 5.x uses `XPENDING`/`XCLAIM` fallback |
| Delayed tasks | `>=1.2` | Uses `ZADD` and timestamp scores |

运行环境：

- Python `>=3.9`
- redis-py `6.4.0`
- 目标开发环境：Python `3.14.5`

Redis：

| 功能 | Redis 要求 | 说明 |
| --- | --- | --- |
| List 阻塞消费 | `>=2.0` | 以 `BLPOP` 系列能力为基础 |
| List 可靠搬移 | `>=2.2` | 使用 `BRPOPLPUSH`；Redis `>=6.2` 优先使用 `BLMOVE` |
| Streams | `>=5.0` | 使用 `XADD`、`XGROUP CREATE`、`XREADGROUP` |
| Streams 自动认领 | `>=6.2` | 使用 `XAUTOCLAIM`；Redis 5.x 回退 `XPENDING`/`XCLAIM` |
| 延迟任务 | `>=1.2` | 使用 `ZADD` 和时间戳 score |

## Installation / 安装

```bash
pip install redqueue
```

For local development:

```bash
python -m pip install -r requirements.txt
```

本地开发：

```bash
python -m pip install -r requirements.txt
```

## Quick Start / 快速开始

Synchronous List queue:

```python
from redqueue import QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="emails",
    backend="list",
)

message_id = client.publish({"to": "user@example.com"})
message = client.consume(timeout=1)

if message is not None:
    try:
        print(message.payload)
        client.ack(message)
    except Exception:
        client.retry(message, reason="handler failed")
```

同步 List 队列：

```python
from redqueue import QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="emails",
    backend="list",
)

message_id = client.publish({"to": "user@example.com"})
message = client.consume(timeout=1)

if message is not None:
    try:
        print(message.payload)
        client.ack(message)
    except Exception:
        client.retry(message, reason="handler failed")
```

Streams backend:

```python
from redqueue import QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="events",
    backend="stream",
    consumer_group="redqueue",
    consumer_name="worker-1",
)

client.publish({"event": "created"})
message = client.consume(timeout=1)
```

Streams 后端：

```python
from redqueue import QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="events",
    backend="stream",
    consumer_group="redqueue",
    consumer_name="worker-1",
)

client.publish({"event": "created"})
message = client.consume(timeout=1)
```

Asynchronous client:

```python
import asyncio

from redqueue import AsyncQueueClient


async def main() -> None:
    client = await AsyncQueueClient.from_url(
        "redis://127.0.0.1:6379/0",
        queue="jobs",
        backend="list",
    )
    await client.publish({"task": "sync"})
    message = await client.consume(timeout=1)
    if message is not None:
        await client.ack(message)
    await client.close()


asyncio.run(main())
```

异步客户端：

```python
import asyncio

from redqueue import AsyncQueueClient


async def main() -> None:
    client = await AsyncQueueClient.from_url(
        "redis://127.0.0.1:6379/0",
        queue="jobs",
        backend="list",
    )
    await client.publish({"task": "sync"})
    message = await client.consume(timeout=1)
    if message is not None:
        await client.ack(message)
    await client.close()


asyncio.run(main())
```

Delayed task:

```python
from redqueue import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0", queue="emails")
client.delay({"to": "later@example.com"}, delay_seconds=60)
released = client.schedule_due(limit=100)
```

延迟任务：

```python
from redqueue import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0", queue="emails")
client.delay({"to": "later@example.com"}, delay_seconds=60)
released = client.schedule_due(limit=100)
```

## Documentation / 文档

- API: [docs/API.md](docs/API.md)
- Examples: [examples/README.md](examples/README.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Release process: [docs/RELEASE.md](docs/RELEASE.md)
- Test guide: [tests/README.md](tests/README.md)

- API 文档：[docs/API.md](docs/API.md)
- 示例代码：[examples/README.md](examples/README.md)
- 版本变更记录：[CHANGELOG.md](CHANGELOG.md)
- 发布流程：[docs/RELEASE.md](docs/RELEASE.md)
- 测试指南：[tests/README.md](tests/README.md)

## Examples / 示例

The `examples/` directory contains runnable scripts for synchronous List queues,
asynchronous List queues, Streams, delayed tasks, monitoring hooks, custom
serializers, and Redis compatibility checks.

`examples/` 目录包含可运行脚本，覆盖同步 List 队列、异步 List 队列、Streams、
延迟任务、监控 hook、自定义序列化器和 Redis 兼容性检查。

```bash
PYTHONPATH=src python examples/sync_list_queue.py
PYTHONPATH=src python examples/async_list_queue.py
PYTHONPATH=src python examples/stream_queue.py
PYTHONPATH=src python examples/delayed_tasks.py
PYTHONPATH=src python examples/monitoring_hooks.py
PYTHONPATH=src python examples/custom_serializer.py
PYTHONPATH=src python examples/compatibility_check.py
```

## Testing / 测试

```bash
PYTHONPATH=src python -m pytest
```

Run integration tests with a local Redis server:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```

使用本地 Redis 运行集成测试：

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```


## License / 许可证

Apache License 2.0. See [LICENSE](LICENSE).

Apache License 2.0。详见 [LICENSE](LICENSE)。
