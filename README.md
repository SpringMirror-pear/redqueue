# RedQueue

RedQueue is a Redis-backed Python message queue library with List, Streams,
delayed tasks, synchronous APIs, asynchronous APIs, compatibility checks,
monitoring hooks, and connection-pool resource management.

Chinese documentation: [README-zh-CN.md](README-zh-CN.md)

Repository:
https://github.com/SpringMirror-pear/redqueue.git

## Features

- Redis List reliable queue with `BLMOVE` on Redis `>=6.2` and `BRPOPLPUSH`
  fallback on older compatible Redis versions.
- Redis Streams backend with consumer groups. Streams require Redis `>=5.0`.
- Delayed tasks based on Redis Sorted Set.
- Sync client `QueueClient` and async client `AsyncQueueClient`.
- Redis connection pool managers for shared sync and async resources.
- `redqueue` CLI for local debugging and operational checks.
- First-class `trace_id` propagation for lifecycle tracing.
- Opt-in message deduplication with Redis `SET NX` and TTL windows.
- Unified exception hierarchy with structured context.
- Monitoring events for publish, consume, ack, nack, retry, dead letter, delay,
  and backend errors.
- Redis capability detection from `INFO server`.
- Apache License 2.0.

## Compatibility

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
| Message deduplication | `>=2.6.12` | Uses `SET` with `NX` and `EX`/`PX` |

## Installation

```bash
pip install redqueue
```

The package installs a `redqueue` command. You can also run it with
`python -m redqueue` from a source checkout.

For local development:

```bash
python -m pip install -r requirements.txt
```

## CLI

Check Redis compatibility:

```bash
redqueue check --url redis://127.0.0.1:6379/0
```

Inspect queue counts:

```bash
redqueue stats --url redis://127.0.0.1:6379/0 --queue emails
```

Publish and consume messages:

```bash
redqueue publish --queue emails --payload '{"to":"user@example.com"}'
redqueue consume --queue emails --timeout 1 --ack
```

Publish with trace correlation:

```bash
redqueue publish --queue emails --payload '{"to":"user@example.com"}' --trace-id trace-123
```

Delayed task debugging:

```bash
redqueue delay --queue emails --payload '{"to":"later@example.com"}' --delay-seconds 60
redqueue schedule-due --queue emails --limit 100
```

Dead-letter inspection:

```bash
redqueue dead-letters --queue emails --limit 20
```

All command output is JSON so it can be piped into scripts or log processors.

## Quick Start

Synchronous List queue:

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

Delayed task:

```python
from redqueue import QueueClient

client = QueueClient.from_url("redis://127.0.0.1:6379/0", queue="emails")
client.delay({"to": "later@example.com"}, delay_seconds=60, trace_id="trace-123")
released = client.schedule_due(limit=100)
```

Message deduplication:

```python
from redqueue import DeduplicationConfig, QueueClient

client = QueueClient.from_url(
    "redis://127.0.0.1:6379/0",
    queue="emails",
    deduplication=DeduplicationConfig(enabled=True, ttl_seconds=3600),
)

first_id = client.publish({"order": 1}, dedup_key="order-1")
second_id = client.publish({"order": 1}, dedup_key="order-1")

assert second_id == first_id
```

Trace IDs:

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

Connection pool management:

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

Async connection pool management:

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

## Branch Model

RedQueue uses a lightweight Git Flow model:

- `main`: stable release branch. Only release merges and urgent hotfixes land
  here.
- `develop`: integration branch for the next minor release.
- `feature/<name>`: feature work branched from `develop`, merged back into
  `develop`.
- `release/<minor>`: release stabilization branch, such as `release/0.11`.
- `hotfix/<version>`: urgent patch branch from `main`, merged back to both
  `main` and `develop`.

## Documentation

- Chinese README: [README-zh-CN.md](README-zh-CN.md)
- API: [docs/API.md](docs/API.md)
- Examples: [examples/README.md](examples/README.md)
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Release process: [docs/RELEASE.md](docs/RELEASE.md)
- Test guide: [tests/README.md](tests/README.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## Examples

The `examples/` directory contains runnable scripts for synchronous List queues,
asynchronous List queues, Streams, delayed tasks, monitoring hooks, custom
serializers, and Redis compatibility checks.

```bash
PYTHONPATH=src python examples/sync_list_queue.py
PYTHONPATH=src python examples/async_list_queue.py
PYTHONPATH=src python examples/stream_queue.py
PYTHONPATH=src python examples/delayed_tasks.py
PYTHONPATH=src python examples/monitoring_hooks.py
PYTHONPATH=src python examples/custom_serializer.py
PYTHONPATH=src python examples/compatibility_check.py
```

## Testing

```bash
PYTHONPATH=src python -m pytest
```

Run integration tests with a local Redis server:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```

Run availability tests:

```bash
PYTHONPATH=src python -m pytest -m availability
```

Run deterministic in-memory performance tests:

```bash
PYTHONPATH=src python -m pytest -m performance
```

Run real Redis availability tests:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and availability"
```

Run real Redis performance tests:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and performance"
```

Run real Redis concurrency tests:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and concurrency"
```

## Availability Results

Latest local run on Python `3.14.5`:

- Full test suite without `REDQUEUE_REDIS_URL`: `114 passed, 10 skipped`.
- Real Redis integration suite: `10 passed` with
  `redis://127.0.0.1:6379/0`.
- Real Redis availability suite: `5 passed` with
  `redis://127.0.0.1:6379/0`.
- Real Redis server: Redis for Windows `5.0.14.1`.
- Availability suite: covers List processing recovery, List dead-letter
  requeue, Streams Redis `<5.0` compatibility rejection, Streams Redis 5.x
  pending recovery fallback, Streams dead-letter requeue, delayed task publish
  failure rollback, missing delayed payload errors, publish-time deduplication,
  delayed task deduplication, async List recovery, async Streams dead-letter
  requeue, and async delayed task rollback.
- Real Redis availability suite additionally validates List recovery, Streams
  dead-letter requeue, delayed task rollback, List deduplication, and delayed
  task deduplication against a running Redis server.

## Performance Results

The performance suite uses deterministic in-memory Redis fakes. It measures
RedQueue overhead without network latency and is intended as a regression
baseline, not as a Redis server benchmark.

Real Redis performance tests were also run against Redis for Windows
`5.0.14.1`. Redis for Windows has extra platform and compatibility overhead, so
these numbers are only a local reference and do not represent expected Linux
production performance.

Latest local baseline on Python `3.14.5`:

| Scenario | Operations | Elapsed | Throughput |
| --- | ---: | ---: | ---: |
| JSON encode + decode | 10,000 | 0.064742s | 154,459 ops/s |
| Sync List publish + consume + ack | 2,000 | 0.042705s | 46,833 ops/s |
| Sync Streams publish + consume + ack | 1,000 | 0.091318s | 10,951 ops/s |
| Delay schedule + release | 1,000 | 0.033192s | 30,127 ops/s |
| Async List publish + consume + ack | 1,000 | 0.023531s | 42,497 ops/s |

Latest real Redis baseline on Python `3.14.5` and Redis for Windows
`5.0.14.1`:

| Scenario | Operations | Elapsed | Throughput |
| --- | ---: | ---: | ---: |
| Real Redis List publish + consume + ack | 200 | 0.045230s | 4,422 ops/s |
| Real Redis Streams publish + consume + ack | 100 | 0.027108s | 3,689 ops/s |
| Real Redis delay schedule + release | 100 | 0.056498s | 1,770 ops/s |
| Real Redis concurrent List publish + consume + ack | 200 | 1.059509s | 189 ops/s |

## License

Apache License 2.0. See [LICENSE](LICENSE).
