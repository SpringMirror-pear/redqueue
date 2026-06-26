# Message Deduplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in Redis-backed message deduplication for RedQueue `0.14.0`.

**Architecture:** Add a small client-side deduplication layer that wraps publish and delay operations before they reach List, Streams, or delayed backends. The layer reserves `rq:{queue}:dedup:<dedup_key>` with Redis `SET NX` and a TTL, returns the original message id on duplicate hits, and rolls back the reservation if publishing fails.

**Tech Stack:** Python `>=3.9`, redis-py `6.4.0`, Redis string `SET NX`/`GET`/`DELETE`, pytest, ruff, mypy.

## Global Constraints

- Apache-2.0 Python file headers are required: `# SPDX-License-Identifier: Apache-2.0` and `# Author: SpringMirror-pear`.
- Deduplication is opt-in and disabled by default.
- Deduplication keys are explicit; RedQueue does not infer keys from payloads.
- Deduplication TTL defaults to `86400.0` seconds and must be greater than zero.
- Duplicate publishes return the first stored message id and do not enqueue or schedule a duplicate.
- Deduplication keys are not deleted on `ack()`; the TTL controls the window.
- Business payloads must not be emitted in monitoring events.

---

### Task 1: Configuration And Monitoring Surface

**Files:**
- Modify: `src/redqueue/config.py`
- Modify: `src/redqueue/monitoring.py`
- Modify: `src/redqueue/__init__.py`
- Test: `tests/test_project_skeleton.py`

**Interfaces:**
- Produces: `DeduplicationConfig(enabled: bool = False, ttl_seconds: float = 86400.0)`
- Produces: `QueueConfig.deduplication: DeduplicationConfig`
- Produces: `MonitoringEventType.MESSAGE_DEDUPLICATED`

- [ ] **Step 1: Write failing configuration tests**

Add tests in `ProjectSkeletonTests`:

```python
def test_deduplication_config_validates_ttl(self) -> None:
    config = DeduplicationConfig(enabled=True, ttl_seconds=60)

    self.assertTrue(config.enabled)
    self.assertEqual(config.ttl_seconds, 60)

    for value in [0, -1]:
        with self.subTest(value=value):
            with self.assertRaises(QueueConfigError):
                DeduplicationConfig(enabled=True, ttl_seconds=value)

def test_queue_config_accepts_deduplication_config(self) -> None:
    deduplication = DeduplicationConfig(enabled=True, ttl_seconds=30)
    config = QueueConfig(queue="emails", deduplication=deduplication)

    self.assertIs(config.deduplication, deduplication)
    self.assertEqual(config.key("dedup:order-1"), "rq:{emails}:dedup:order-1")

def test_monitoring_event_type_includes_deduplicated(self) -> None:
    self.assertEqual(
        MonitoringEventType.MESSAGE_DEDUPLICATED.value,
        "message.deduplicated",
    )
```

- [ ] **Step 2: Run failing tests**

Run:

```powershell
$env:PYTHONPATH='src'; python -m pytest tests/test_project_skeleton.py -k "deduplication_config or monitoring_event_type_includes_deduplicated"
```

Expected: FAIL because `DeduplicationConfig` and `MESSAGE_DEDUPLICATED` do not exist.

- [ ] **Step 3: Implement configuration and monitoring**

Add `DeduplicationConfig` beside `RetryConfig`, add `deduplication` to
`QueueConfig`, validate its type in `QueueConfig.__post_init__`, export it from
`src/redqueue/__init__.py`, and add `MESSAGE_DEDUPLICATED = "message.deduplicated"`.

- [ ] **Step 4: Run passing tests**

Run the same pytest command. Expected: PASS.

### Task 2: Synchronous Deduplication Layer

**Files:**
- Create: `src/redqueue/deduplication.py`
- Modify: `src/redqueue/client.py`
- Modify: `tests/fakes.py`
- Test: `tests/test_project_skeleton.py`

**Interfaces:**
- Consumes: `QueueConfig.deduplication`
- Produces: `DeduplicationBackend.reserve_or_get(dedup_key: str, message_id: str, trace_id: str | None = None) -> DeduplicationResult`
- Produces: `DeduplicationBackend.rollback(dedup_key: str) -> None`
- Produces: `QueueClient.publish(..., dedup_key: str | None = None) -> str`
- Produces: `QueueClient.delay(..., dedup_key: str | None = None) -> str`

- [ ] **Step 1: Write failing sync tests**

Add tests:

```python
def test_sync_list_deduplicates_publish_by_key(self) -> None:
    redis = FakeListRedis()
    hook = InMemoryMonitoringHook()
    client = QueueClient(
        QueueConfig(
            queue="emails",
            deduplication=DeduplicationConfig(enabled=True, ttl_seconds=60),
            monitoring=hook,
        ),
        redis=redis,
        capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
    )

    first_id = client.publish({"order": 1}, dedup_key=" order-1 ")
    second_id = client.publish({"order": 1}, dedup_key="order-1")

    self.assertEqual(second_id, first_id)
    self.assertEqual(len(redis.lists[client.config.key("ready")]), 1)
    self.assertIn("set", redis.commands)
    self.assertIn("get", redis.commands)
    self.assertEqual(hook.events[-1].type, MonitoringEventType.MESSAGE_DEDUPLICATED)
    self.assertEqual(hook.events[-1].message_id, first_id)

def test_sync_publish_without_dedup_key_is_unchanged(self) -> None:
    redis = FakeListRedis()
    client = QueueClient(
        QueueConfig(
            queue="emails",
            deduplication=DeduplicationConfig(enabled=True, ttl_seconds=60),
        ),
        redis=redis,
        capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
    )

    client.publish({"order": 1})
    client.publish({"order": 1})

    self.assertEqual(len(redis.lists[client.config.key("ready")]), 2)

def test_sync_deduplication_rejects_blank_key(self) -> None:
    client = QueueClient(
        QueueConfig(
            queue="emails",
            deduplication=DeduplicationConfig(enabled=True, ttl_seconds=60),
        ),
        redis=FakeListRedis(),
        capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
    )

    with self.assertRaises(QueueConfigError):
        client.publish({"order": 1}, dedup_key=" ")
```

- [ ] **Step 2: Run failing sync tests**

Run:

```powershell
$env:PYTHONPATH='src'; python -m pytest tests/test_project_skeleton.py -k "sync_list_deduplicates_publish_by_key or sync_publish_without_dedup_key_is_unchanged or sync_deduplication_rejects_blank_key"
```

Expected: FAIL because `dedup_key` is not accepted.

- [ ] **Step 3: Implement sync deduplication**

Create `src/redqueue/deduplication.py` with:

- `DeduplicationResult(message_id: str, duplicate: bool)`
- `DeduplicationBackend`
- TTL normalization to `ex` for integral seconds and `px` for fractional seconds.

Update `QueueClient.publish()` and `QueueClient.delay()` to call a helper that:

1. Generates or preserves `message_id`.
2. Reserves the deduplication key.
3. Delegates to the target operation on reservation success.
4. Emits `message.deduplicated` and returns existing id on duplicate.
5. Deletes the dedup key if the target operation raises.

Add `set(..., nx=False, ex=None, px=None)` and `get()` support to `FakeListRedis`.

- [ ] **Step 4: Run passing sync tests**

Run the same pytest command. Expected: PASS.

### Task 3: Async Deduplication Layer

**Files:**
- Modify: `src/redqueue/deduplication.py`
- Modify: `src/redqueue/async_client.py`
- Modify: `tests/fakes.py`
- Test: `tests/test_project_skeleton.py`

**Interfaces:**
- Consumes: `DeduplicationBackend`
- Produces: `AsyncDeduplicationBackend.reserve_or_get(...) -> DeduplicationResult`
- Produces: `AsyncQueueClient.publish(..., dedup_key: str | None = None) -> str`
- Produces: `AsyncQueueClient.delay(..., dedup_key: str | None = None) -> str`

- [ ] **Step 1: Write failing async tests**

Add tests:

```python
def test_async_list_deduplicates_publish_by_key(self) -> None:
    async def run() -> tuple[str, str, int]:
        redis = FakeAsyncListRedis()
        client = AsyncQueueClient(
            QueueConfig(
                queue="jobs",
                deduplication=DeduplicationConfig(enabled=True, ttl_seconds=60),
            ),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        first_id = await client.publish({"order": 1}, dedup_key="order-1")
        second_id = await client.publish({"order": 1}, dedup_key="order-1")
        return first_id, second_id, len(redis.lists[client.config.key("ready")])

    first_id, second_id, ready_count = asyncio.run(run())

    self.assertEqual(second_id, first_id)
    self.assertEqual(ready_count, 1)

def test_async_deduplication_rejects_blank_key(self) -> None:
    async def run() -> None:
        client = AsyncQueueClient(
            QueueConfig(
                queue="jobs",
                deduplication=DeduplicationConfig(enabled=True, ttl_seconds=60),
            ),
            redis=FakeAsyncListRedis(),
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        with self.assertRaises(QueueConfigError):
            await client.publish({"order": 1}, dedup_key=" ")

    asyncio.run(run())
```

- [ ] **Step 2: Run failing async tests**

Run:

```powershell
$env:PYTHONPATH='src'; python -m pytest tests/test_project_skeleton.py -k "async_list_deduplicates_publish_by_key or async_deduplication_rejects_blank_key"
```

Expected: FAIL because async deduplication is not implemented.

- [ ] **Step 3: Implement async deduplication**

Add `AsyncDeduplicationBackend` and update `AsyncQueueClient` with async helper
methods mirroring the synchronous flow. Add async `set()` signature support to
`FakeAsyncListRedis`.

- [ ] **Step 4: Run passing async tests**

Run the same pytest command. Expected: PASS.

### Task 4: Streams, Delay, Rollback, And Integration Coverage

**Files:**
- Modify: `tests/test_project_skeleton.py`
- Modify: `tests/test_real_redis_availability.py`
- Modify: `tests/fakes.py`

**Interfaces:**
- Consumes: sync and async client deduplication APIs.

- [ ] **Step 1: Write failing coverage tests**

Add tests for:

- Sync Streams duplicate publish appends one stream entry.
- Sync delay duplicate scheduling stores one delayed entry and one payload.
- `publish(delay=..., dedup_key=...)` deduplicates delayed scheduling.
- Reservation rollback removes dedup key when backend publish fails.
- Real Redis List deduplication smoke test.
- Real Redis delayed deduplication smoke test.

- [ ] **Step 2: Run failing coverage tests**

Run:

```powershell
$env:PYTHONPATH='src'; python -m pytest tests/test_project_skeleton.py -k "stream_deduplicates or delay_deduplicates or deduplication_rolls_back"
```

Expected: FAIL until implementation covers all paths.

- [ ] **Step 3: Complete implementation gaps**

Adjust `QueueClient` and `AsyncQueueClient` helper routing until List, Streams,
Delay, and `publish(delay=...)` share the same deduplication flow.

- [ ] **Step 4: Run coverage tests**

Run the same pytest command. Expected: PASS.

### Task 5: Documentation, Version, Build, And Release

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/redqueue/_version.py`
- Modify: `tests/test_project_skeleton.py`
- Modify: `CHANGELOG.md`
- Modify: `README.md`
- Modify: `README-zh-CN.md`
- Modify: `docs/API.md`

**Interfaces:**
- Produces: RedQueue `0.14.0` public release.

- [ ] **Step 1: Update version and docs**

Set version to `0.14.0`, document `DeduplicationConfig`, `dedup_key`, duplicate
return behavior, TTL semantics, and `message.deduplicated`.

- [ ] **Step 2: Run full verification**

Run:

```powershell
$env:PYTHONPATH='src'; python -m ruff check .
$env:PYTHONPATH='src'; python -m mypy
$env:PYTHONPATH='src'; python -m pytest
$env:REDQUEUE_REDIS_URL='redis://127.0.0.1:6379/0'; $env:PYTHONPATH='src'; python -m pytest -m integration
python -m build --no-isolation
python -m twine check dist\redqueue-0.14.0*
```

Expected: all commands exit 0.

- [ ] **Step 3: Commit, tag, push, publish**

Use the project branch model:

```powershell
git switch -c feature/message-deduplication
git add .
git commit -m "Add message deduplication"
git push -u origin feature/message-deduplication
git switch main
git merge --ff-only feature/message-deduplication
git push origin main
git tag v0.14.0
git push origin v0.14.0
python -m twine upload --disable-progress-bar --skip-existing dist\redqueue-0.14.0*
```

Expected: GitHub `main` and PyPI both contain `0.14.0`.
