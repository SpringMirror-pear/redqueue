# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Availability and failure-recovery tests for RedQueue."""

import asyncio
import unittest

import pytest

from redqueue import (
    AsyncQueueClient,
    BackendUnavailableError,
    QueueClient,
    QueueConfig,
    RedisCapabilities,
    RedisCompatibilityError,
    RedisVersion,
    RetryConfig,
    RetryExceededError,
)
from tests.fakes import (
    FakeAsyncListRedis,
    FakeAsyncStreamRedis,
    FakeListRedis,
    FakeStreamRedis,
)


@pytest.mark.availability
class AvailabilityTests(unittest.TestCase):
    def test_list_recovers_multiple_processing_messages_without_loss(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="availability-list"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        expected_ids = {
            client.publish({"index": index})
            for index in range(10)
        }

        consumed = client.consume(timeout=1, batch_size=10)

        self.assertIsInstance(consumed, list)
        self.assertEqual({message.id for message in consumed}, expected_ids)
        self.assertEqual(client.recover_stale(limit=10), 10)

        recovered = client.consume(timeout=1, batch_size=10)

        self.assertIsInstance(recovered, list)
        self.assertEqual({message.id for message in recovered}, expected_ids)
        for message in recovered:
            client.ack(message)
        self.assertEqual(redis.lists[client.config.key("processing")], [])

    def test_list_dead_letter_requeue_preserves_message_identity(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="availability-list-dead"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        message_id = client.publish({"work": "dead-letter"})
        message = client.consume(timeout=1)

        client.nack(message, requeue=False)
        dead = client.dead_letters(limit=10)
        client.requeue_dead(dead[0])
        requeued = client.consume(timeout=1)

        self.assertEqual(requeued.id, message_id)
        self.assertEqual(requeued.payload, {"work": "dead-letter"})
        client.ack(requeued)

    def test_streams_reject_unsupported_redis_before_runtime_use(self) -> None:
        with self.assertRaises(RedisCompatibilityError):
            QueueClient(
                QueueConfig(queue="availability-stream-old", backend="stream"),
                redis=FakeStreamRedis(),
                capabilities=RedisCapabilities(RedisVersion(4, 0, 14)),
            )

    def test_stream_recovery_uses_xclaim_fallback_on_redis_5(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(queue="availability-stream-recovery", backend="stream"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(5, 0, 14)),
        )
        message_id = client.publish({"event": "recover"})
        client.consume(timeout=1)

        recovered = client.backend.recover_pending(min_idle_ms=1, limit=10)

        self.assertEqual(recovered[0].id, message_id)
        self.assertIn("xpending_range", redis.commands)
        self.assertIn("xclaim", redis.commands)
        client.ack(recovered[0])

    def test_stream_dead_letter_can_be_read_and_requeued(self) -> None:
        redis = FakeStreamRedis()
        client = QueueClient(
            QueueConfig(
                queue="availability-stream-dead",
                backend="stream",
                retry=RetryConfig(max_retries=0),
            ),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        message_id = client.publish({"event": "dead"})
        message = client.consume(timeout=1)

        with self.assertRaises(RetryExceededError):
            client.retry(message, reason="availability")

        dead = client.dead_letters(limit=10)
        client.requeue_dead(dead[0])
        requeued = client.consume(timeout=1)

        self.assertEqual(requeued.id, message_id)
        self.assertEqual(requeued.payload, {"event": "dead"})
        client.ack(requeued)

    def test_delay_publish_failure_restores_due_entry_and_payload(self) -> None:
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
            QueueConfig(queue="availability-delay"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        message_id = client.delay({"task": "later"}, run_at=100)
        client.delay_backend.publisher = BrokenPublisher()

        with self.assertRaises(RuntimeError):
            client.schedule_due(now=150)

        self.assertIn(message_id, redis.sorted_sets[client.config.key("delayed")])
        self.assertIn(client.config.key(f"payload:{message_id}"), redis.values)

    def test_delay_missing_payload_fails_explicitly(self) -> None:
        redis = FakeListRedis()
        client = QueueClient(
            QueueConfig(queue="availability-delay-missing"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )
        message_id = client.delay({"task": "later"}, run_at=100)
        redis.values.pop(client.config.key(f"payload:{message_id}"))

        with self.assertRaises(BackendUnavailableError):
            client.schedule_due(now=150)

    def test_async_list_recovers_processing_messages_without_loss(self) -> None:
        async def run() -> set[str]:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="availability-async-list"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            expected_ids = {
                await client.publish({"index": index})
                for index in range(5)
            }
            consumed = await client.consume(timeout=1, batch_size=5)

            self.assertIsInstance(consumed, list)
            self.assertEqual({message.id for message in consumed}, expected_ids)
            self.assertEqual(await client.recover_stale(limit=5), 5)

            recovered = await client.consume(timeout=1, batch_size=5)
            self.assertIsInstance(recovered, list)
            for message in recovered:
                await client.ack(message)
            return {message.id for message in recovered}

        self.assertEqual(len(asyncio.run(run())), 5)

    def test_async_stream_dead_letter_can_be_requeued(self) -> None:
        async def run() -> tuple[str, str]:
            redis = FakeAsyncStreamRedis()
            client = AsyncQueueClient(
                QueueConfig(
                    queue="availability-async-stream",
                    backend="stream",
                    retry=RetryConfig(max_retries=0),
                ),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            message_id = await client.publish({"event": "dead"})
            message = await client.consume(timeout=1)

            with self.assertRaises(RetryExceededError):
                await client.retry(message, reason="availability")

            dead = await client.dead_letters(limit=10)
            await client.requeue_dead(dead[0])
            requeued = await client.consume(timeout=1)
            await client.ack(requeued)
            return message_id, requeued.id

        self.assertEqual(*asyncio.run(run()))

    def test_async_delay_publish_failure_restores_due_entry_and_payload(self) -> None:
        class BrokenAsyncPublisher:
            async def publish(
                self,
                payload: object,
                *,
                headers: dict[str, object] | None = None,
                message_id: str | None = None,
                trace_id: str | None = None,
            ) -> str:
                raise RuntimeError("publish failed")

        async def run() -> tuple[FakeAsyncListRedis, str]:
            redis = FakeAsyncListRedis()
            client = AsyncQueueClient(
                QueueConfig(queue="availability-async-delay"),
                redis=redis,
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )
            message_id = await client.delay({"task": "later"}, run_at=100)
            delay_backend = await client._ensure_delay_backend()
            delay_backend.publisher = BrokenAsyncPublisher()

            with self.assertRaises(RuntimeError):
                await client.schedule_due(now=150)
            return redis, message_id

        redis, message_id = asyncio.run(run())

        delayed_key = "rq:{availability-async-delay}:delayed"
        payload_key = f"rq:{{availability-async-delay}}:payload:{message_id}"
        self.assertIn(message_id, redis.sorted_sets[delayed_key])
        self.assertIn(payload_key, redis.values)


if __name__ == "__main__":
    unittest.main()
