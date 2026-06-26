# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Tests for the RedQueue project skeleton and core models."""

import asyncio
import json
import unittest

from redqueue import (
    AckError,
    AsyncQueueClient,
    AsyncRedisConnectionManager,
    BackendType,
    BackendUnavailableError,
    CompositeMonitoringHook,
    ErrorContext,
    InMemoryMonitoringHook,
    JsonSerializer,
    Message,
    MessageDecodeError,
    MessageEncodeError,
    MonitoringEvent,
    MonitoringEventType,
    QueueClient,
    QueueConfig,
    QueueConfigError,
    RedisCapabilities,
    RedisCompatibilityError,
    RedisConnectionManager,
    RedisVersion,
    RedQueueError,
    RetryConfig,
    RetryExceededError,
    SafeMonitoringHook,
    Serializer,
    __version__,
    detect_capabilities,
    detect_capabilities_async,
    extract_redis_version,
    new_message_id,
    new_trace_id,
)
from tests.fakes import (
    FakeAsyncListRedis,
    FakeAsyncStreamRedis,
    FakeListRedis,
    FakeStreamRedis,
)


class ProjectSkeletonTests(unittest.TestCase):
    def test_version_is_current_dev_version(self) -> None:
        self.assertEqual(__version__, "0.13.1")

    def test_queue_config_accepts_and_normalizes_backend(self) -> None:
        config = QueueConfig(queue=" emails ", backend="stream")

        self.assertEqual(config.queue, "emails")
        self.assertIs(config.backend, BackendType.STREAM)
        self.assertEqual(config.key("ready"), "rq:{emails}:ready")
        self.assertIsInstance(config.serializer, JsonSerializer)

    def test_queue_config_rejects_invalid_values(self) -> None:
        invalid_configs = [
            {"queue": ""},
            {"queue": "bad queue"},
            {"queue": "emails", "backend": "unknown"},
            {"queue": "emails", "namespace": ""},
            {"queue": "emails", "visibility_timeout_seconds": 0},
            {"queue": "emails", "consumer_group": " "},
            {"queue": "emails", "consumer_name": " "},
            {"queue": "emails", "serializer": object()},
        ]

        for kwargs in invalid_configs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(QueueConfigError):
                    QueueConfig(**kwargs)

    def test_retry_config_validates_and_calculates_delay(self) -> None:
        retry = RetryConfig(
            max_retries=5,
            base_delay_seconds=2,
            max_delay_seconds=5,
        )

        self.assertEqual(retry.next_delay(0), 0)
        self.assertEqual(retry.next_delay(2), 4)
        self.assertEqual(retry.next_delay(3), 5)

        invalid_configs = [
            {"max_retries": -1},
            {"base_delay_seconds": -1},
            {"max_delay_seconds": -1},
            {"base_delay_seconds": 3, "max_delay_seconds": 1},
        ]

        for kwargs in invalid_configs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(QueueConfigError):
                    RetryConfig(**kwargs)

    def test_message_id_is_stable_and_message_validates(self) -> None:
        message_id = new_message_id()
        message = Message(
            id=f" {message_id} ",
            queue=" emails ",
            payload={"to": "user@example.com"},
            headers={"trace_id": "abc"},
        )

        self.assertEqual(message.id, message_id)
        self.assertEqual(message.queue, "emails")
        self.assertEqual(message.trace_id, "abc")
        self.assertEqual(message.headers, {"trace_id": "abc"})
        self.assertEqual(message.with_attempt().attempts, 1)
        self.assertEqual(message.with_backend("list").backend, "list")

        traced = Message(
            id="msg-trace",
            queue="emails",
            payload={"to": "user@example.com"},
            trace_id=" trace-123 ",
        )
        self.assertEqual(traced.trace_id, "trace-123")
        self.assertEqual(traced.headers["trace_id"], "trace-123")
        self.assertTrue(new_trace_id())

    def test_message_accepts_blank_legacy_trace_header(self) -> None:
        message = Message(
            id="legacy-msg",
            queue="emails",
            payload={"to": "user@example.com"},
            headers={"trace_id": " "},
        )

        self.assertIsNone(message.trace_id)
        self.assertEqual(message.headers, {"trace_id": " "})

        traced = Message(
            id="override-msg",
            queue="emails",
            payload={"to": "user@example.com"},
            headers={"trace_id": "old-trace"},
            trace_id=" new-trace ",
        )

        self.assertEqual(traced.trace_id, "new-trace")
        self.assertEqual(traced.headers["trace_id"], "new-trace")

    def test_message_rejects_invalid_values(self) -> None:
        invalid_messages = [
            {"id": "", "queue": "emails", "payload": None},
            {"id": "msg", "queue": "", "payload": None},
            {"id": "msg", "queue": "emails", "payload": None, "attempts": -1},
            {"id": "msg", "queue": "emails", "payload": None, "created_at": -1},
            {"id": "msg", "queue": "emails", "payload": None, "available_at": -1},
            {"id": "msg", "queue": "emails", "payload": None, "trace_id": " "},
        ]

        for kwargs in invalid_messages:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(QueueConfigError):
                    Message(**kwargs)

    def test_redis_streams_capability_requires_redis_5(self) -> None:
        capabilities = RedisCapabilities(RedisVersion.parse("4.0.14"))

        with self.assertRaises(RedisCompatibilityError) as ctx:
            capabilities.require_streams()

        self.assertIn("Redis >= 5.0.0", str(ctx.exception))
        self.assertEqual(
            ctx.exception.to_dict()["details"]["required_version"],
            "5.0.0",
        )

    def test_redqueue_error_carries_structured_context(self) -> None:
        error = RedQueueError(
            "operation failed",
            action="publish",
            queue="emails",
            details={"backend": "list"},
        )

        self.assertIsInstance(error.context, ErrorContext)
        self.assertIn("action='publish'", str(error))
        self.assertEqual(
            error.to_dict(),
            {
                "type": "RedQueueError",
                "message": "operation failed",
                "action": "publish",
                "queue": "emails",
                "details": {"backend": "list"},
            },
        )

    def test_all_custom_errors_inherit_redqueue_error(self) -> None:
        error_types = [
            QueueConfigError,
            RedisCompatibilityError,
            MessageEncodeError,
            MessageDecodeError,
            BackendUnavailableError,
            AckError,
            RetryExceededError,
        ]

        for error_type in error_types:
            with self.subTest(error_type=error_type.__name__):
                self.assertTrue(issubclass(error_type, RedQueueError))

    def test_message_encode_decode_errors_preserve_cause(self) -> None:
        encode_cause = ValueError("not json")
        decode_cause = ValueError("bad payload")

        encode_error = MessageEncodeError.from_exception(
            encode_cause,
            queue="emails",
        )
        decode_error = MessageDecodeError.from_exception(
            decode_cause,
            queue="emails",
        )

        self.assertIs(encode_error.__cause__, encode_cause)
        self.assertIs(decode_error.__cause__, decode_cause)
        self.assertEqual(encode_error.to_dict()["queue"], "emails")
        self.assertEqual(decode_error.to_dict()["action"], "message.decode")

    def test_json_serializer_encodes_and_decodes_payloads(self) -> None:
        serializer = JsonSerializer()

        encoded = serializer.encode({"b": 2, "a": 1}, queue="emails")
        decoded = serializer.decode(encoded, queue="emails")

        self.assertIsInstance(encoded, bytes)
        self.assertEqual(encoded, b'{"a":1,"b":2}')
        self.assertEqual(decoded, {"a": 1, "b": 2})

    def test_json_serializer_passes_bytes_through(self) -> None:
        serializer = JsonSerializer()

        self.assertEqual(serializer.encode(b"raw", queue="emails"), b"raw")
        self.assertEqual(serializer.encode(bytearray(b"raw"), queue="emails"), b"raw")
        self.assertEqual(serializer.encode(memoryview(b"raw"), queue="emails"), b"raw")

    def test_json_serializer_wraps_encode_and_decode_failures(self) -> None:
        serializer = JsonSerializer()

        with self.assertRaises(MessageEncodeError) as encode_ctx:
            serializer.encode({"bad": {1, 2, 3}}, queue="emails")

        with self.assertRaises(MessageDecodeError) as decode_ctx:
            serializer.decode(b"{bad-json", queue="emails")

        self.assertIsNotNone(encode_ctx.exception.__cause__)
        self.assertIsNotNone(decode_ctx.exception.__cause__)
        self.assertEqual(encode_ctx.exception.to_dict()["queue"], "emails")
        self.assertEqual(decode_ctx.exception.to_dict()["queue"], "emails")

    def test_queue_config_accepts_custom_serializer(self) -> None:
        class ReverseSerializer:
            content_type = "text/reverse"

            def encode(self, payload: object, *, queue: str | None = None) -> bytes:
                return str(payload)[::-1].encode()

            def decode(self, payload: bytes, *, queue: str | None = None) -> object:
                return payload.decode()[::-1]

        serializer = ReverseSerializer()
        config = QueueConfig(queue="emails", serializer=serializer)

        self.assertIsInstance(config.serializer, Serializer)
        self.assertEqual(config.serializer.decode(serializer.encode("abc")), "abc")

    def test_redis_62_supports_modern_recovery_commands(self) -> None:
        capabilities = RedisCapabilities(RedisVersion.parse("6.2.0"))

        self.assertTrue(capabilities.supports_streams)
        self.assertTrue(capabilities.supports_streams_auto_claim)
        self.assertTrue(capabilities.supports_list_reliable_blmove)

    def test_redis_version_parses_release_suffixes(self) -> None:
        self.assertEqual(RedisVersion.parse("7.2.4").patch, 4)
        self.assertEqual(RedisVersion.parse("7.2.4-rc1").patch, 4)
        self.assertEqual(str(RedisVersion.parse("5.0")), "5.0.0")

        with self.assertRaises(ValueError):
            RedisVersion.parse("bad.version")

    def test_redis_capability_boundaries(self) -> None:
        redis_4 = RedisCapabilities(RedisVersion.parse("4.0.14"))
        redis_5 = RedisCapabilities(RedisVersion.parse("5.0.0"))
        redis_62 = RedisCapabilities(RedisVersion.parse("6.2.0"))

        self.assertFalse(redis_4.supports_streams)
        self.assertTrue(redis_5.supports_streams)
        self.assertFalse(redis_5.supports_streams_auto_claim)
        self.assertTrue(redis_62.supports_streams_auto_claim)
        self.assertTrue(redis_62.supports_list_reliable_blmove)

    def test_extract_redis_version_from_info(self) -> None:
        self.assertEqual(
            extract_redis_version({"redis_version": b"6.2.13"}),
            RedisVersion(6, 2, 13),
        )

        with self.assertRaises(BackendUnavailableError):
            extract_redis_version({})

        with self.assertRaises(BackendUnavailableError):
            extract_redis_version({"redis_version": "invalid"})

    def test_detect_capabilities_from_sync_client(self) -> None:
        class FakeRedis:
            def info(self, section: str | None = None) -> dict[str, str]:
                self.section = section
                return {"redis_version": "7.4.0"}

        client = FakeRedis()
        capabilities = detect_capabilities(client)

        self.assertEqual(client.section, "server")
        self.assertEqual(capabilities.version, RedisVersion(7, 4, 0))

    def test_detect_capabilities_wraps_sync_info_failure(self) -> None:
        class BrokenRedis:
            def info(self, section: str | None = None) -> dict[str, str]:
                raise TimeoutError("redis unavailable")

        with self.assertRaises(BackendUnavailableError) as ctx:
            detect_capabilities(BrokenRedis())

        self.assertIsInstance(ctx.exception.__cause__, TimeoutError)

    def test_detect_capabilities_from_async_client(self) -> None:
        class FakeAsyncRedis:
            async def info(self, section: str | None = None) -> dict[str, str]:
                self.section = section
                return {"redis_version": "5.0.14"}

        async def run() -> RedisCapabilities:
            client = FakeAsyncRedis()
            capabilities = await detect_capabilities_async(client)
            self.assertEqual(client.section, "server")
            return capabilities

        capabilities = asyncio.run(run())

        self.assertEqual(capabilities.version, RedisVersion(5, 0, 14))
        self.assertTrue(capabilities.supports_streams)

    def test_sync_client_emits_monitoring_event(self) -> None:
        events: list[MonitoringEvent] = []

        class Hook:
            def emit(self, event: MonitoringEvent) -> None:
                events.append(event)

        client = QueueClient(
            QueueConfig(queue="emails", monitoring=Hook()),
            redis=FakeListRedis(),
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        message_id = client.publish({"to": "user@example.com"})

        self.assertTrue(message_id)
        self.assertIs(events[0].type, MonitoringEventType.CLIENT_CREATED)
        self.assertIs(events[-1].type, MonitoringEventType.MESSAGE_PUBLISHED)

    def test_sync_list_backend_propagates_trace_id(self) -> None:
        hook = InMemoryMonitoringHook()
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails", monitoring=hook),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        message_id = client.publish(
            {"to": "user@example.com"},
            trace_id="trace-sync",
        )
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.trace_id, "trace-sync")
        self.assertEqual(message.headers["trace_id"], "trace-sync")
        self.assertEqual(hook.events[-1].trace_id, "trace-sync")

    def test_async_client_emits_monitoring_event(self) -> None:
        async def run() -> InMemoryMonitoringHook:
            hook = InMemoryMonitoringHook()
            AsyncQueueClient(
                QueueConfig(queue="jobs", monitoring=hook),
                redis=FakeAsyncListRedis(),
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            return hook

        hook = asyncio.run(run())

        self.assertEqual(len(hook.events), 1)
        self.assertIs(hook.events[0].type, MonitoringEventType.CLIENT_CREATED)

    def test_async_list_backend_propagates_trace_id(self) -> None:
        async def run() -> Message:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            await client.publish({"task": "sync"}, trace_id="trace-async")
            message = await client.consume(timeout=1)
            self.assertIsInstance(message, Message)
            return message

        message = asyncio.run(run())

        self.assertEqual(message.trace_id, "trace-async")
        self.assertEqual(message.headers["trace_id"], "trace-async")

    def test_monitoring_event_to_dict_excludes_payload(self) -> None:
        event = MonitoringEvent(
            type=MonitoringEventType.MESSAGE_PUBLISHED,
            queue="emails",
            message_id="msg-1",
            trace_id="trace-1",
            backend="list",
            duration_ms=1.5,
            attributes={"key": "rq:{emails}:ready"},
        )

        data = event.to_dict()

        self.assertEqual(data["type"], "message.published")
        self.assertEqual(data["message_id"], "msg-1")
        self.assertEqual(data["trace_id"], "trace-1")
        self.assertNotIn("payload", data)

    def test_monitoring_hooks_store_fanout_and_isolate_errors(self) -> None:
        first = InMemoryMonitoringHook()
        second = InMemoryMonitoringHook()

        class BrokenHook:
            def emit(self, event: MonitoringEvent) -> None:
                raise RuntimeError("monitoring failed")

        event = MonitoringEvent(
            type=MonitoringEventType.CLIENT_CREATED,
            queue="emails",
        )
        composite = CompositeMonitoringHook(first, second)
        composite.emit(event)

        self.assertEqual(len(first.events), 1)
        self.assertEqual(len(second.events), 1)

        safe = SafeMonitoringHook(BrokenHook())
        safe.emit(event)

        self.assertEqual(len(safe.errors), 1)

    def test_queue_config_wraps_monitoring_hook_safely(self) -> None:
        hook = InMemoryMonitoringHook()
        config = QueueConfig(queue="emails", monitoring=hook)

        self.assertIsInstance(config.monitoring, SafeMonitoringHook)
        config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.CLIENT_CREATED,
                queue="emails",
            )
        )

        self.assertEqual(len(hook.events), 1)

    def test_sync_list_backend_publish_consume_ack_uses_blmove(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(6, 2, 0)),
        )

        message_id = client.publish({"to": "user@example.com"})
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.payload, {"to": "user@example.com"})
        self.assertIn("blmove", redis.commands)

        client.ack(message)

        self.assertEqual(redis.lists[client.config.key("processing")], [])

    def test_sync_list_backend_ack_uses_original_serialized_payload(self) -> None:
        class NonDeterministicSerializer:
            content_type = "application/x-redqueue-test"

            def __init__(self) -> None:
                self.counter = 0

            def encode(self, payload: object, *, queue: str | None = None) -> bytes:
                self.counter += 1
                return json.dumps(
                    {"envelope": payload, "nonce": self.counter},
                    separators=(",", ":"),
                ).encode()

            def decode(self, payload: bytes, *, queue: str | None = None) -> object:
                return json.loads(payload.decode())["envelope"]

        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails", serializer=NonDeterministicSerializer()),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        client.ack(message)

        self.assertEqual(redis.lists[client.config.key("processing")], [])

    def test_sync_list_backend_consumes_with_brpoplpush_on_old_redis(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(5, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        self.assertIn("brpoplpush", redis.commands)

    def test_sync_list_backend_normalizes_legacy_timeout(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(5, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        client.consume(timeout=0.25)

        self.assertEqual(redis.timeouts[-1], 1)

    def test_sync_list_backend_nack_requeues_or_dead_letters(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        message = client.consume(timeout=1)
        client.nack(message, requeue=True)

        self.assertEqual(len(redis.lists[client.config.key("ready")]), 1)

        message = client.consume(timeout=1)
        client.nack(message, requeue=False)

        self.assertEqual(len(redis.lists[client.config.key("dead")]), 1)

    def test_sync_list_backend_nack_uses_original_serialized_payload(self) -> None:
        class NonDeterministicSerializer:
            content_type = "application/x-redqueue-test"

            def __init__(self) -> None:
                self.counter = 0

            def encode(self, payload: object, *, queue: str | None = None) -> bytes:
                self.counter += 1
                return json.dumps(
                    {"envelope": payload, "nonce": self.counter},
                    separators=(",", ":"),
                ).encode()

            def decode(self, payload: bytes, *, queue: str | None = None) -> object:
                return json.loads(payload.decode())["envelope"]

        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails", serializer=NonDeterministicSerializer()),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        client.nack(message, requeue=False)

        self.assertEqual(redis.lists[client.config.key("processing")], [])
        self.assertEqual(len(redis.lists[client.config.key("dead")]), 1)

    def test_sync_list_backend_retry_increments_attempts_and_dead_letters(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails", retry=RetryConfig(max_retries=1)),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        message = client.consume(timeout=1)
        client.retry(message, reason="temporary")
        retried = client.consume(timeout=1)

        self.assertEqual(retried.attempts, 1)

        with self.assertRaises(RetryExceededError):
            client.retry(retried, reason="permanent")

        self.assertEqual(len(redis.lists[client.config.key("dead")]), 1)

    def test_async_client_publish_shape(self) -> None:
        async def run() -> str:
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=FakeAsyncListRedis(),
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            return await client.publish({"task": "sync"})

        self.assertTrue(asyncio.run(run()))

    def test_async_list_backend_publish_consume_ack_and_close(self) -> None:
        async def run() -> tuple[FakeAsyncListRedis, str]:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            message_id = await client.publish({"task": "sync"})
            message = await client.consume(timeout=1)

            self.assertIsInstance(message, Message)
            self.assertEqual(message.id, message_id)
            self.assertEqual(message.payload, {"task": "sync"})
            self.assertIn("blmove", redis.commands)

            await client.ack(message)
            await client.close()
            return redis, message_id

        redis, message_id = asyncio.run(run())

        self.assertTrue(message_id)
        self.assertIn("aclose", redis.commands)

    def test_sync_client_can_leave_injected_redis_open(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            owns_redis=False,
        )

        client.close()

        self.assertNotIn("close", redis.commands)

    def test_sync_client_context_closes_owned_redis(self) -> None:
        redis = FakeListRedis()

        with QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        ):
            pass

        self.assertIn("close", redis.commands)

    def test_sync_client_close_is_idempotent(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.close()
        client.close()

        self.assertEqual(redis.commands.count("close"), 1)

    def test_sync_client_closes_owned_redis_when_backend_init_fails(self) -> None:
        class ClosableStreamRedis(FakeStreamRedis):
            def close(self) -> None:
                self.commands.append("close")

        redis = ClosableStreamRedis()

        with self.assertRaises(RedisCompatibilityError):
            QueueClient(
                QueueConfig(queue="events", backend="stream"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(4, 0, 14)),
            )

        self.assertIn("close", redis.commands)

    def test_async_client_can_leave_injected_redis_open(self) -> None:
        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
                owns_redis=False,
            )
            await client.close()
            return redis

        redis = asyncio.run(run())

        self.assertNotIn("aclose", redis.commands)

    def test_async_client_context_closes_owned_redis(self) -> None:
        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            async with AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            ):
                pass
            return redis

        redis = asyncio.run(run())

        self.assertIn("aclose", redis.commands)

    def test_async_client_close_is_idempotent(self) -> None:
        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            await client.close()
            await client.close()
            return redis

        redis = asyncio.run(run())

        self.assertEqual(redis.commands.count("aclose"), 1)

    def test_async_client_closes_owned_redis_when_backend_init_fails(self) -> None:
        async def run() -> FakeAsyncStreamRedis:
            class ClosableAsyncStreamRedis(FakeAsyncStreamRedis):
                async def aclose(self) -> None:
                    self.commands.append("aclose")

            redis = ClosableAsyncStreamRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="events", backend="stream"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(4, 0, 14)),
            )
            with self.assertRaises(RedisCompatibilityError):
                await client.consume(timeout=1)
            return redis

        redis = asyncio.run(run())

        self.assertIn("aclose", redis.commands)

    def test_connection_managers_create_pooled_clients(self) -> None:
        manager = RedisConnectionManager(
            "redis://127.0.0.1:6379/0",
            max_connections=3,
        )

        try:
            first = manager.redis()
            second = manager.redis()

            self.assertIs(first.connection_pool, manager.pool)
            self.assertIs(second.connection_pool, manager.pool)
        finally:
            manager.close()

        with self.assertRaises(RuntimeError):
            manager.redis()

    def test_sync_from_url_accepts_connection_manager_and_pool_options(self) -> None:
        redis = FakeListRedis()
        client = QueueClient.from_url(
            "redis://127.0.0.1:6379/0",
            queue="emails",
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            pool_options={"max_connections": 3},
        )

        client.close()

        self.assertNotIn("close", redis.commands)

        manager = RedisConnectionManager(
            "redis://127.0.0.1:6379/0",
            max_connections=3,
        )
        try:
            managed_client = QueueClient.from_url(
                manager.url,
                queue="emails",
                connection_manager=manager,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )

            self.assertIs(managed_client.redis.connection_pool, manager.pool)
            managed_client.close()
            self.assertIs(manager.redis().connection_pool, manager.pool)
        finally:
            manager.close()

    def test_sync_from_url_closes_owned_redis_when_initialization_fails(self) -> None:
        class OwnedRedis(FakeListRedis):
            def info(self, section: str | None = None) -> dict[str, str]:
                return {"redis_version": "7.0.0"}

        redis = OwnedRedis()

        with self.assertRaises(QueueConfigError):
            QueueClient.from_url(
                "redis://127.0.0.1:6379/0",
                queue="bad queue",
                redis=redis,
                owns_redis=True,
            )

        self.assertIn("close", redis.commands)

    def test_sync_from_url_leaves_injected_redis_open_when_init_fails(self) -> None:
        class InjectedRedis(FakeListRedis):
            def info(self, section: str | None = None) -> dict[str, str]:
                return {"redis_version": "7.0.0"}

        redis = InjectedRedis()

        with self.assertRaises(QueueConfigError):
            QueueClient.from_url(
                "redis://127.0.0.1:6379/0",
                queue="bad queue",
                redis=redis,
            )

        self.assertNotIn("close", redis.commands)

    def test_sync_from_url_closes_owned_redis_when_capability_probe_fails(self) -> None:
        class BrokenInfoRedis(FakeListRedis):
            def info(self, section: str | None = None) -> dict[str, str]:
                raise TimeoutError("redis unavailable")

        redis = BrokenInfoRedis()

        with self.assertRaises(BackendUnavailableError):
            QueueClient.from_url(
                "redis://127.0.0.1:6379/0",
                queue="emails",
                redis=redis,
                owns_redis=True,
            )

        self.assertIn("close", redis.commands)

    def test_async_connection_manager_creates_pooled_clients(self) -> None:
        async def run() -> None:
            manager = AsyncRedisConnectionManager(
                "redis://127.0.0.1:6379/0",
                max_connections=3,
            )
            try:
                first = manager.redis()
                second = manager.redis()

                self.assertIs(first.connection_pool, manager.pool)
                self.assertIs(second.connection_pool, manager.pool)
            finally:
                await manager.close()

            with self.assertRaises(RuntimeError):
                manager.redis()

        asyncio.run(run())

    def test_async_from_url_accepts_connection_manager_and_pool_options(self) -> None:
        async def run() -> None:
            redis = FakeAsyncListRedis()
            client = await AsyncQueueClient.from_url(
                "redis://127.0.0.1:6379/0",
                queue="jobs",
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
                pool_options={"max_connections": 3},
            )

            await client.close()

            self.assertNotIn("aclose", redis.commands)

            manager = AsyncRedisConnectionManager(
                "redis://127.0.0.1:6379/0",
                max_connections=3,
            )
            try:
                managed_client = await AsyncQueueClient.from_url(
                    manager.url,
                    queue="jobs",
                    connection_manager=manager,
                    capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
                )

                self.assertIs(managed_client.redis.connection_pool, manager.pool)
                await managed_client.close()
                self.assertIs(manager.redis().connection_pool, manager.pool)
            finally:
                await manager.close()

        asyncio.run(run())

    def test_async_from_url_closes_owned_redis_when_initialization_fails(self) -> None:
        class OwnedAsyncRedis(FakeAsyncListRedis):
            async def info(self, section: str | None = None) -> dict[str, str]:
                return {"redis_version": "7.0.0"}

        async def run() -> FakeAsyncListRedis:
            redis = OwnedAsyncRedis()
            with self.assertRaises(QueueConfigError):
                await AsyncQueueClient.from_url(
                    "redis://127.0.0.1:6379/0",
                    queue="bad queue",
                    redis=redis,
                    owns_redis=True,
                )
            return redis

        redis = asyncio.run(run())

        self.assertIn("aclose", redis.commands)

    def test_async_from_url_leaves_injected_redis_open_when_init_fails(self) -> None:
        class InjectedAsyncRedis(FakeAsyncListRedis):
            async def info(self, section: str | None = None) -> dict[str, str]:
                return {"redis_version": "7.0.0"}

        async def run() -> FakeAsyncListRedis:
            redis = InjectedAsyncRedis()
            with self.assertRaises(QueueConfigError):
                await AsyncQueueClient.from_url(
                    "redis://127.0.0.1:6379/0",
                    queue="bad queue",
                    redis=redis,
                )
            return redis

        redis = asyncio.run(run())

        self.assertNotIn("aclose", redis.commands)

    def test_async_from_url_closes_owned_redis_on_probe_failure(self) -> None:
        class BrokenInfoAsyncRedis(FakeAsyncListRedis):
            async def info(self, section: str | None = None) -> dict[str, str]:
                raise TimeoutError("redis unavailable")

        async def run() -> FakeAsyncListRedis:
            redis = BrokenInfoAsyncRedis()
            with self.assertRaises(BackendUnavailableError):
                await AsyncQueueClient.from_url(
                    "redis://127.0.0.1:6379/0",
                    queue="jobs",
                    redis=redis,
                    owns_redis=True,
                )
            return redis

        redis = asyncio.run(run())

        self.assertIn("aclose", redis.commands)

    def test_async_list_backend_ack_uses_original_serialized_payload(self) -> None:
        class NonDeterministicSerializer:
            content_type = "application/x-redqueue-test"

            def __init__(self) -> None:
                self.counter = 0

            def encode(self, payload: object, *, queue: str | None = None) -> bytes:
                self.counter += 1
                return json.dumps(
                    {"envelope": payload, "nonce": self.counter},
                    separators=(",", ":"),
                ).encode()

            def decode(self, payload: bytes, *, queue: str | None = None) -> object:
                return json.loads(payload.decode())["envelope"]

        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs", serializer=NonDeterministicSerializer()),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            await client.publish({"task": "sync"})
            message = await client.consume(timeout=1)

            self.assertIsInstance(message, Message)
            await client.ack(message)
            return redis

        redis = asyncio.run(run())

        self.assertEqual(redis.lists["rq:{jobs}:processing"], [])

    def test_async_list_backend_uses_brpoplpush_on_old_redis(self) -> None:
        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(5, 0, 0)),
            )
            await client.publish({"task": "sync"})
            message = await client.consume(timeout=1)

            self.assertIsInstance(message, Message)
            return redis

        redis = asyncio.run(run())

        self.assertIn("brpoplpush", redis.commands)

    def test_async_list_backend_normalizes_legacy_timeout(self) -> None:
        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(5, 0, 0)),
            )
            await client.publish({"task": "sync"})
            await client.consume(timeout=0.25)
            return redis

        redis = asyncio.run(run())

        self.assertEqual(redis.timeouts[-1], 1)

    def test_async_list_backend_retry_dead_letters(self) -> None:
        async def run() -> FakeAsyncListRedis:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs", retry=RetryConfig(max_retries=1)),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            await client.publish({"task": "sync"})
            message = await client.consume(timeout=1)
            await client.retry(message, reason="temporary")
            retried = await client.consume(timeout=1)

            self.assertEqual(retried.attempts, 1)
            with self.assertRaises(RetryExceededError):
                await client.retry(retried, reason="permanent")
            return redis

        redis = asyncio.run(run())

        self.assertEqual(len(redis.lists["rq:{jobs}:dead"]), 1)

    def test_stream_backend_requires_redis_5(self) -> None:
        with self.assertRaises(RedisCompatibilityError):
            QueueClient(
                QueueConfig(queue="events", backend="stream"),
                redis=FakeStreamRedis(),
                capabilities=RedisCapabilities(RedisVersion(4, 0, 14)),
            )

    def test_stream_backend_publish_consume_ack_and_group_init(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(queue="events", backend="stream", consumer_name="worker-1"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        message_id = client.publish({"event": "created"})
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.payload, {"event": "created"})
        self.assertIn("xgroup_create", redis.commands)
        self.assertIn("xreadgroup", redis.commands)

        client.ack(message)

        self.assertIn("xack", redis.commands)

    def test_stream_backend_propagates_trace_id(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(queue="events", backend="stream"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"event": "created"}, trace_id="trace-stream")
        message = client.consume(timeout=1)

        self.assertIsInstance(message, Message)
        self.assertEqual(message.trace_id, "trace-stream")

    def test_stream_backend_retry_dead_letters_and_autoclaims(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(
                queue="events",
                backend="stream",
                retry=RetryConfig(max_retries=1),
            ),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"event": "created"})
        message = client.consume(timeout=1)
        recovered = client.backend.recover_pending(min_idle_ms=1)

        self.assertEqual(recovered[0].id, message.id)
        self.assertIn("xautoclaim", redis.commands)

        client.retry(message, reason="temporary")
        retried = client.consume(timeout=1)

        self.assertEqual(retried.attempts, 1)
        with self.assertRaises(RetryExceededError):
            client.retry(retried, reason="permanent")

        self.assertTrue(redis.streams["rq:{events}:dead"])
        self.assertIn(
            (client.config.key("dead"), client.config.consumer_group),
            redis.groups,
        )
        self.assertEqual(client.dead_letters()[0].id, retried.id)

    def test_async_stream_backend_publish_consume_ack(self) -> None:
        async def run() -> tuple[FakeAsyncStreamRedis, str]:
            redis = FakeAsyncStreamRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="events", backend="stream"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            message_id = await client.publish({"event": "created"})
            message = await client.consume(timeout=1)

            self.assertIsInstance(message, Message)
            self.assertEqual(message.id, message_id)
            await client.ack(message)
            return redis, message_id

        redis, message_id = asyncio.run(run())

        self.assertTrue(message_id)
        self.assertIn("xgroup_create", redis.commands)
        self.assertIn("xack", redis.commands)

    def test_async_stream_backend_dead_letters_ensure_group(self) -> None:
        async def run() -> tuple[FakeAsyncStreamRedis, str, str]:
            redis = FakeAsyncStreamRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="events", backend="stream"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            await client.publish({"event": "created"})
            message = await client.consume(timeout=1)

            self.assertIsInstance(message, Message)
            await client.nack(message, requeue=False)
            dead = await client.dead_letters()
            return redis, client.config.key("dead"), dead[0].id

        redis, dead_key, dead_id = asyncio.run(run())

        self.assertTrue(dead_id)
        self.assertIn((dead_key, "redqueue"), redis.groups)

    def test_delay_backend_releases_only_due_messages_once(self) -> None:
        redis = FakeListRedis()
        hook = InMemoryMonitoringHook()
        client = QueueClient(
            QueueConfig(queue="emails", monitoring=hook),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        future_id = client.delay({"to": "future@example.com"}, run_at=200)
        due_id = client.delay(
            {"to": "due@example.com"},
            run_at=100,
            trace_id="trace-delay",
        )

        self.assertEqual(client.schedule_due(now=150), 1)
        self.assertEqual(client.schedule_due(now=150), 0)
        self.assertIn(future_id, redis.sorted_sets[client.config.key("delayed")])
        self.assertNotIn(due_id, redis.sorted_sets[client.config.key("delayed")])

        message = client.consume(timeout=1)

        self.assertEqual(message.id, due_id)
        self.assertEqual(message.payload, {"to": "due@example.com"})
        self.assertEqual(message.trace_id, "trace-delay")
        event_types = [event.type for event in hook.events]
        self.assertIn(MonitoringEventType.DELAY_SCHEDULED, event_types)
        self.assertIn(MonitoringEventType.DELAY_RELEASED, event_types)
        self.assertEqual(event_types.count(MonitoringEventType.DELAY_SCHEDULED), 2)

    def test_sync_publish_delay_preserves_message_id(self) -> None:
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=FakeListRedis(),
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        message_id = client.publish(
            {"to": "later@example.com"},
            delay=60,
            message_id="fixed-delay-id",
        )
        client.schedule_due(now=9_999_999_999)
        message = client.consume(timeout=1)

        self.assertEqual(message_id, "fixed-delay-id")
        self.assertIsInstance(message, Message)
        self.assertEqual(message.id, "fixed-delay-id")

    def test_delay_backend_cleans_payload_when_zadd_fails(self) -> None:
        class BrokenZaddRedis(FakeListRedis):
            def zadd(self, name: str, mapping: dict[str, float]) -> int:
                super().zadd(name, mapping)
                raise RuntimeError("zadd failed")

        redis = BrokenZaddRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        with self.assertRaises(BackendUnavailableError):
            client.delay({"to": "due@example.com"}, run_at=100)

        self.assertFalse(redis.values)

    def test_delay_backend_restores_zset_when_publish_fails(self) -> None:
        class BrokenPublisher:
            def publish(
                self,
                payload: object,
                *,
                headers: dict[str, object] | None = None,
                message_id: str | None = None,
                trace_id: str | None = None,
            ) -> str:
                raise RuntimeError("publish failed")

        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        message_id = client.delay({"to": "due@example.com"}, run_at=100)
        client.delay_backend.publisher = BrokenPublisher()

        with self.assertRaises(RuntimeError):
            client.schedule_due(now=150)

        self.assertIn(message_id, redis.sorted_sets[client.config.key("delayed")])
        self.assertIn(client.config.key(f"payload:{message_id}"), redis.values)

    def test_async_delay_backend_releases_due_message(self) -> None:
        async def run() -> tuple[FakeAsyncListRedis, str]:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            message_id = await client.delay(
                {"task": "later"},
                run_at=100,
                trace_id="trace-async-delay",
            )
            released = await client.schedule_due(now=150)
            message = await client.consume(timeout=1)

            self.assertEqual(released, 1)
            self.assertEqual(message.id, message_id)
            self.assertEqual(message.payload, {"task": "later"})
            self.assertEqual(message.trace_id, "trace-async-delay")
            return redis, message_id

        redis, message_id = asyncio.run(run())

        self.assertNotIn(message_id, redis.sorted_sets["rq:{jobs}:delayed"])

    def test_async_publish_delay_preserves_message_id(self) -> None:
        async def run() -> Message:
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=FakeAsyncListRedis(),
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            message_id = await client.publish(
                {"task": "later"},
                delay=60,
                message_id="fixed-async-delay-id",
            )
            await client.schedule_due(now=9_999_999_999)
            message = await client.consume(timeout=1)

            self.assertEqual(message_id, "fixed-async-delay-id")
            self.assertIsInstance(message, Message)
            return message

        message = asyncio.run(run())

        self.assertEqual(message.id, "fixed-async-delay-id")

    def test_async_delay_backend_cleans_payload_when_zadd_fails(self) -> None:
        class BrokenAsyncZaddRedis(FakeAsyncListRedis):
            async def zadd(self, name: str, mapping: dict[str, float]) -> int:
                await super().zadd(name, mapping)
                raise RuntimeError("zadd failed")

        async def run() -> FakeAsyncListRedis:
            redis = BrokenAsyncZaddRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            with self.assertRaises(BackendUnavailableError):
                await client.delay({"task": "later"}, run_at=100)
            return redis

        redis = asyncio.run(run())

        self.assertFalse(redis.values)

    def test_list_backend_recovers_processing_and_requeues_dead_letter(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="emails"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        client.publish({"to": "user@example.com"})
        processing = client.consume(timeout=1)

        self.assertEqual(client.recover_stale(limit=10), 1)
        self.assertEqual(redis.lists[client.config.key("processing")], [])
        recovered = client.consume(timeout=1)

        self.assertEqual(recovered.id, processing.id)
        client.nack(recovered, requeue=False)

        dead = client.dead_letters()
        self.assertEqual(dead[0].id, recovered.id)

        client.requeue_dead(dead[0])

        self.assertEqual(client.consume(timeout=1).id, recovered.id)

    def test_stream_backend_recovers_pending_with_xclaim_on_redis_5(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(queue="events", backend="stream"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(5, 0, 0)),
        )

        client.publish({"event": "created"})
        message = client.consume(timeout=1)
        recovered_count = client.recover_stale(min_idle_ms=1, limit=10)

        self.assertEqual(recovered_count, 1)
        self.assertIn("xpending_range", redis.commands)
        self.assertIn("xclaim", redis.commands)
        self.assertEqual(
            client.backend.recover_pending(min_idle_ms=1)[0].id,
            message.id,
        )

    def test_stream_backend_normalizes_bytes_stream_ids(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(queue="events", backend="stream"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(5, 0, 0)),
        )

        client.publish({"event": "created"})
        message = client.consume(timeout=1)
        recovered = client.backend.recover_pending(min_idle_ms=1, limit=10)

        self.assertIsInstance(message.raw_id, str)
        self.assertIsInstance(recovered[0].raw_id, str)
        client.ack(recovered[0])

    def test_async_list_backend_dead_letter_requeue(self) -> None:
        async def run() -> str:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="jobs"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            await client.publish({"task": "fail"})
            message = await client.consume(timeout=1)
            await client.nack(message, requeue=False)
            dead = await client.dead_letters()
            await client.requeue_dead(dead[0])
            requeued = await client.consume(timeout=1)
            return requeued.id

        self.assertTrue(asyncio.run(run()))


if __name__ == "__main__":
    unittest.main()
