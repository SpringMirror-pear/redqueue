# RedQueue Message Deduplication Design

## Context

RedQueue currently supports synchronous and asynchronous publishing, List and
Streams backends, delayed tasks, monitoring events, trace ids, and Redis
connection management. Publishing is routed through `QueueClient` and
`AsyncQueueClient`, then delegated to backend implementations.

Message deduplication should work consistently across List, Streams, and delayed
tasks without changing existing behavior for users who do not opt in.

## Goals

- Add opt-in message deduplication in RedQueue `0.14.0`.
- Support synchronous and asynchronous clients.
- Support immediate publish and delayed publish paths.
- Keep backend APIs focused on queue storage; put shared deduplication behavior
  in a reusable client-side layer.
- Provide deterministic duplicate handling: repeated publishes with the same
  deduplication key return the original message id and do not enqueue another
  message.
- Expose duplicate hits through monitoring.

## Non-Goals

- RedQueue will not infer deduplication keys from payloads in `0.14.0`.
- RedQueue will not delete deduplication keys on `ack()`.
- RedQueue will not provide exactly-once processing guarantees. This feature is
  a publish-time deduplication window, not a processing guarantee.
- RedQueue will not make deduplication mandatory for all messages.

## Public API

### Configuration

Add `DeduplicationConfig`:

- `enabled: bool = False`
- `ttl_seconds: float = 86400.0`

Add `QueueConfig.deduplication: DeduplicationConfig`.

For ergonomic construction, `QueueConfig` should accept a
`DeduplicationConfig` instance. If needed, direct helper arguments can be added
later, but `0.14.0` should keep the public configuration surface small.

### Client Methods

Extend synchronous and asynchronous APIs:

- `publish(payload, *, delay=None, headers=None, message_id=None, trace_id=None, dedup_key=None) -> str`
- `delay(payload, *, delay_seconds=None, run_at=None, headers=None, message_id=None, trace_id=None, dedup_key=None) -> str`

When deduplication is disabled or `dedup_key` is `None`, behavior is unchanged.

When deduplication is enabled and `dedup_key` is present:

1. Normalize and validate `dedup_key` as a non-empty string after trimming.
2. Determine the message id before reserving the deduplication key. Use the
   caller-provided `message_id` when present, otherwise generate one.
3. Reserve Redis key `rq:{queue}:dedup:<dedup_key>` with the message id using
   `SET ... NX` and TTL.
4. If reservation succeeds, publish or schedule the message using that message
   id.
5. If reservation fails, read the existing deduplication key and return the
   stored message id without publishing or scheduling a duplicate.

## Redis Design

Use Redis string keys:

- Key: `config.key("dedup:<dedup_key>")`
- Value: RedQueue message id
- TTL: `DeduplicationConfig.ttl_seconds`

Synchronous Redis command shape:

- `set(name, value, nx=True, ex=<seconds>)`
- `get(name)`
- `delete(name)` for rollback only

Asynchronous behavior mirrors the synchronous command shape.

TTL values should be normalized for redis-py compatibility:

- integral values may use `ex`
- fractional positive values should use `px` with a ceiling millisecond value

## Failure Handling

If deduplication reservation succeeds but the actual publish or delay scheduling
fails, RedQueue should delete the reserved deduplication key before re-raising.
This prevents a failed publish attempt from blocking future retries for the
entire TTL window.

If duplicate detection succeeds but reading the existing message id fails,
RedQueue should raise the existing unified Redis backend error type.

If `dedup_key` is blank while deduplication is enabled, RedQueue should raise
`QueueConfigError`.

## Monitoring

Add monitoring event type:

- `message.deduplicated`

Emit this event when a duplicate publish or delayed publish is skipped.

Attributes:

- `dedup_key`
- `existing_message_id`

The event should include `trace_id` only if the duplicate call supplied one.
Business payloads should not be included.

## Data Flow

Immediate publish:

1. Client receives `publish(..., dedup_key=...)`.
2. Deduplication layer reserves or detects the key.
3. Reservation success delegates to List or Streams backend publish.
4. Duplicate hit returns stored message id.

Delayed publish:

1. Client receives `delay(..., dedup_key=...)` or
   `publish(..., delay=..., dedup_key=...)`.
2. Deduplication layer reserves or detects the key.
3. Reservation success delegates to delayed task backend.
4. Duplicate hit returns stored message id and does not create another delayed
   payload or Sorted Set entry.

## Test Plan

Unit tests:

- `DeduplicationConfig` validates `ttl_seconds > 0`.
- `QueueConfig` accepts `DeduplicationConfig`.
- Sync List duplicate publish returns the first message id and enqueues once.
- Async List duplicate publish returns the first message id and enqueues once.
- Sync Streams duplicate publish returns the first message id and appends once.
- Sync delay duplicate scheduling returns the first message id and stores one
  delayed entry.
- `publish(delay=..., dedup_key=...)` passes deduplication through to delayed
  publishing.
- Duplicate hits emit `message.deduplicated`.
- Blank deduplication keys raise `QueueConfigError` when deduplication is
  enabled.
- Reservation rollback removes the deduplication key when publishing fails.

Integration tests:

- Real Redis List deduplication smoke test with two identical `dedup_key`
  publishes.
- Real Redis delayed deduplication smoke test if Redis is available.

Documentation:

- Update README and README-zh-CN.
- Update docs/API.md.
- Update CHANGELOG.md for `0.14.0`.

## Release

Release as `0.14.0` because this adds new public API and behavior. Development
and formal release versions remain separate.
