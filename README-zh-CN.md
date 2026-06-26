# RedQueue

RedQueue 是一个基于 Redis 的 Python 消息队列库，支持 List、Streams、延迟任务、
同步 API、异步 API、兼容性检查、监控 hook，以及 Redis 连接池和资源管理。

英文文档：[README.md](README.md)

仓库：
https://github.com/SpringMirror-pear/redqueue.git

## 功能

- 基于 Redis List 的可靠队列：Redis `>=6.2` 使用 `BLMOVE`，低版本兼容时回退
  `BRPOPLPUSH`。
- 基于 Redis Streams 的消费组后端，Streams 要求 Redis `>=5.0`。
- 基于 Redis Sorted Set 的延迟任务。
- 同步客户端 `QueueClient` 与异步客户端 `AsyncQueueClient`。
- 支持同步和异步 Redis 连接池管理器，方便多个客户端共享连接池。
- 提供 `redqueue` CLI，用于本地调试和运行时检查。
- 一等 `trace_id` 链路追踪能力，贯穿消息生命周期。
- 带结构化上下文的统一异常体系。
- 针对发布、消费、确认、拒绝、重试、死信、延迟和后端错误的监控事件。
- 通过 `INFO server` 探测 Redis 能力。
- Apache License 2.0。

## 兼容性

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

## 安装

```bash
pip install redqueue
```

安装后会提供 `redqueue` 命令。从源码目录调试时也可以使用
`python -m redqueue`。

本地开发：

```bash
python -m pip install -r requirements.txt
```

## CLI

检查 Redis 兼容性：

```bash
redqueue check --url redis://127.0.0.1:6379/0
```

查看队列统计：

```bash
redqueue stats --url redis://127.0.0.1:6379/0 --queue emails
```

发布和消费消息：

```bash
redqueue publish --queue emails --payload '{"to":"user@example.com"}'
redqueue consume --queue emails --timeout 1 --ack
```

发布时指定 trace：

```bash
redqueue publish --queue emails --payload '{"to":"user@example.com"}' --trace-id trace-123
```

调试延迟任务：

```bash
redqueue delay --queue emails --payload '{"to":"later@example.com"}' --delay-seconds 60
redqueue schedule-due --queue emails --limit 100
```

查看死信：

```bash
redqueue dead-letters --queue emails --limit 20
```

所有命令都输出 JSON，方便接入脚本和日志处理流程。

## 快速开始

同步 List 队列：

```python
from redqueue import QueueClient, new_trace_id

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="emails",
    backend="list",
)

trace_id = new_trace_id()
message_id = client.publish({"to": "user@example.com"}, trace_id=trace_id)
message = client.consume(timeout=1)

if message is not None:
    try:
        print(message.trace_id)
        print(message.payload)
        client.ack(message)
    except Exception:
        client.retry(message, reason="handler failed")
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

延迟任务：

```python
from redqueue import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0", queue="emails")
client.delay({"to": "later@example.com"}, delay_seconds=60, trace_id="trace-123")
released = client.schedule_due(limit=100)
```

链路追踪：

```python
from redqueue import InMemoryMonitoringHook, QueueClient, new_trace_id

hook = InMemoryMonitoringHook()
client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="emails",
    monitoring=hook,
)

trace_id = new_trace_id()
client.publish({"to": "user@example.com"}, trace_id=trace_id)
message = client.consume(timeout=1)

assert message.trace_id == trace_id
assert hook.events[-1].trace_id == trace_id
```

连接池管理：

```python
from redqueue import QueueClient, RedisConnectionManager

with RedisConnectionManager(
    "redis://127.0.0.1:6379/0",
    max_connections=20,
    health_check_interval=30,
) as manager:
    producer = QueueClient.from_url(
        manager.url,
        queue="emails",
        connection_manager=manager,
    )
    consumer = QueueClient.from_url(
        manager.url,
        queue="emails",
        connection_manager=manager,
    )

    producer.publish({"to": "user@example.com"})
    message = consumer.consume(timeout=1)
    if message is not None:
        consumer.ack(message)
```

异步连接池管理：

```python
import asyncio

from redqueue import AsyncQueueClient, AsyncRedisConnectionManager


async def main() -> None:
    async with AsyncRedisConnectionManager(
        "redis://127.0.0.1:6379/0",
        max_connections=20,
    ) as manager:
        client = await AsyncQueueClient.from_url(
            manager.url,
            queue="jobs",
            connection_manager=manager,
        )
        await client.publish({"task": "sync"})


asyncio.run(main())
```

## 分支模型

RedQueue 使用轻量 Git Flow：

- `main`：稳定发布分支，只接受发布合并和紧急 hotfix。
- `develop`：下一个小版本的集成分支。
- `feature/<name>`：功能开发分支，从 `develop` 创建并合回 `develop`。
- `release/<minor>`：版本稳定分支，例如 `release/0.11`。
- `hotfix/<version>`：紧急补丁分支，从 `main` 创建，并合回 `main` 与
  `develop`。

## 文档

- 英文 README：[README.md](README.md)
- API 文档：[docs/API.md](docs/API.md)
- 示例代码：[examples/README.md](examples/README.md)
- 版本变更记录：[CHANGELOG.md](CHANGELOG.md)
- 发布流程：[docs/RELEASE.md](docs/RELEASE.md)
- 测试指南：[tests/README.md](tests/README.md)
- 贡献指南：[CONTRIBUTING.md](CONTRIBUTING.md)
- 行为准则：[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## 示例

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

## 测试

```bash
PYTHONPATH=src python -m pytest
```

使用本地 Redis 运行集成测试：

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```

运行可用性测试：

```bash
PYTHONPATH=src python -m pytest -m availability
```

运行确定性的内存性能测试：

```bash
PYTHONPATH=src python -m pytest -m performance
```

运行真实 Redis 可用性测试：

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and availability"
```

运行真实 Redis 性能测试：

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and performance"
```

运行真实 Redis 高并发测试：

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and concurrency"
```

## 可用性测试结果

Python `3.14.5` 最新本地运行结果：

- 未设置 `REDQUEUE_REDIS_URL` 时的完整测试套件：`77 passed, 8 skipped`。
- 真实 Redis 可用性套件：使用 `redis://127.0.0.1:6379/0` 时 `3 passed`。
- 真实 Redis 服务端：Redis for Windows `5.0.14.1`。
- 可用性套件覆盖：List processing 恢复、List 死信重放、Streams Redis
  `<5.0` 兼容性拒绝、Streams Redis 5.x pending 恢复回退、Streams 死信
  重放、延迟任务发布失败回滚、延迟 payload 缺失错误、异步 List 恢复、异步
  Streams 死信重放和异步延迟任务回滚。
- 真实 Redis 可用性套件额外验证了运行中 Redis 服务上的 List 恢复、Streams
  死信重放和延迟任务回滚。

## 性能测试结果

性能套件使用确定性的内存 Redis fake。它衡量 RedQueue 自身开销，不包含网络延迟，
用于回归基线，不代表 Redis 服务端性能。

真实 Redis 性能测试使用 Redis for Windows `5.0.14.1`。Redis for Windows
存在额外的平台和兼容层开销，因此这些数字只作为本地参考，不代表 Linux 线上环境
的预期性能。

Python `3.14.5` 最新本地基线：

| 场景 | 操作数 | 耗时 | 吞吐量 |
| --- | ---: | ---: | ---: |
| JSON 编码 + 解码 | 10,000 | 0.064742s | 154,459 ops/s |
| 同步 List 发布 + 消费 + ack | 2,000 | 0.042705s | 46,833 ops/s |
| 同步 Streams 发布 + 消费 + ack | 1,000 | 0.091318s | 10,951 ops/s |
| 延迟任务调度 + 释放 | 1,000 | 0.033192s | 30,127 ops/s |
| 异步 List 发布 + 消费 + ack | 1,000 | 0.023531s | 42,497 ops/s |

Python `3.14.5` 和 Redis for Windows `5.0.14.1` 最新真实 Redis 基线：

| 场景 | 操作数 | 耗时 | 吞吐量 |
| --- | ---: | ---: | ---: |
| 真实 Redis List 发布 + 消费 + ack | 200 | 0.045230s | 4,422 ops/s |
| 真实 Redis Streams 发布 + 消费 + ack | 100 | 0.027108s | 3,689 ops/s |
| 真实 Redis 延迟任务调度 + 释放 | 100 | 0.056498s | 1,770 ops/s |
| 真实 Redis 并发 List 发布 + 消费 + ack | 200 | 1.059509s | 189 ops/s |

## 许可证

Apache License 2.0。详见 [LICENSE](LICENSE)。
