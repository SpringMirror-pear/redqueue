# RedQueue Examples / RedQueue 示例

These examples cover the main RedQueue features. They expect Redis to be
available at `redis://127.0.0.1:6379/0` unless `REDQUEUE_REDIS_URL` is set.

这些示例覆盖 RedQueue 的主要功能。默认使用
`redis://127.0.0.1:6379/0`，也可以通过 `REDQUEUE_REDIS_URL` 指定 Redis。

## Run / 运行

From the repository root:

```bash
PYTHONPATH=src python examples/sync_list_queue.py
```

在仓库根目录执行：

```bash
PYTHONPATH=src python examples/sync_list_queue.py
```

## Files / 文件

- `sync_list_queue.py`: synchronous List publish, consume, ack, nack, retry,
  dead letter, and recovery.
- `async_list_queue.py`: asynchronous List publish, consume, ack, retry, and
  close.
- `stream_queue.py`: Streams consumer group publish, consume, ack, pending
  recovery, and compatibility failure handling.
- `delayed_tasks.py`: delayed task scheduling with Sorted Set and
  `schedule_due`.
- `monitoring_hooks.py`: in-memory, composite, and safe monitoring hooks.
- `custom_serializer.py`: custom serializer protocol.
- `compatibility_check.py`: explicit Redis capability detection and Streams
  version guard.

- `sync_list_queue.py`：同步 List 发布、消费、ack、nack、retry、死信和恢复。
- `async_list_queue.py`：异步 List 发布、消费、ack、retry 和关闭连接。
- `stream_queue.py`：Streams 消费组发布、消费、ack、pending 恢复和兼容性失败处理。
- `delayed_tasks.py`：基于 Sorted Set 的延迟任务和 `schedule_due`。
- `monitoring_hooks.py`：内存 hook、组合 hook 和安全 hook。
- `custom_serializer.py`：自定义序列化协议。
- `compatibility_check.py`：显式 Redis 能力探测和 Streams 版本保护。

## Notes / 注意

Examples use queue names prefixed with `redqueue-example-`.

示例使用 `redqueue-example-` 前缀的队列名。
