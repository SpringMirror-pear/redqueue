# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Real Redis availability tests."""

from __future__ import annotations

import os
import unittest
from uuid import uuid4

import pytest
from redis import Redis

from redqueue import QueueClient, RetryConfig, RetryExceededError

REDIS_URL = os.getenv("REDQUEUE_REDIS_URL")


def _queue_name(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _redis() -> Redis:
    return Redis.from_url(REDIS_URL)


def _cleanup(redis: Redis, queue: str) -> None:
    keys = list(redis.scan_iter(match=f"rq:{{{queue}}}:*", count=1000))
    if keys:
        redis.delete(*keys)


@pytest.mark.integration
@pytest.mark.availability
@unittest.skipUnless(REDIS_URL, "set REDQUEUE_REDIS_URL to run real Redis tests")
class RealRedisAvailabilityTests(unittest.TestCase):
    def test_list_recovery_against_real_redis(self) -> None:
        queue = _queue_name("real-availability-list")
        redis = _redis()
        _cleanup(redis, queue)
        client = QueueClient.from_url(REDIS_URL, queue=queue, backend="list")
        try:
            expected_ids = {client.publish({"index": index}) for index in range(20)}
            consumed = client.consume(timeout=1, batch_size=20)

            self.assertIsInstance(consumed, list)
            self.assertEqual({message.id for message in consumed}, expected_ids)
            self.assertEqual(client.recover_stale(limit=20), 20)

            recovered = client.consume(timeout=1, batch_size=20)
            self.assertIsInstance(recovered, list)
            self.assertEqual({message.id for message in recovered}, expected_ids)
            for message in recovered:
                client.ack(message)
        finally:
            client.close()
            _cleanup(redis, queue)
            redis.close()

    def test_stream_dead_letter_requeue_against_real_redis(self) -> None:
        queue = _queue_name("real-availability-stream")
        redis = _redis()
        _cleanup(redis, queue)
        client = QueueClient.from_url(
            REDIS_URL,
            queue=queue,
            backend="stream",
            retry=RetryConfig(max_retries=0),
        )
        try:
            message_id = client.publish({"event": "dead"})
            message = client.consume(timeout=1)

            with self.assertRaises(RetryExceededError):
                client.retry(message, reason="real-redis-availability")

            dead = client.dead_letters(limit=10)
            client.requeue_dead(dead[0])
            requeued = client.consume(timeout=1)

            self.assertEqual(requeued.id, message_id)
            self.assertEqual(requeued.payload, {"event": "dead"})
            client.ack(requeued)
        finally:
            client.close()
            _cleanup(redis, queue)
            redis.close()

    def test_delay_rollback_against_real_redis(self) -> None:
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

        queue = _queue_name("real-availability-delay")
        redis = _redis()
        _cleanup(redis, queue)
        client = QueueClient.from_url(REDIS_URL, queue=queue, backend="list")
        try:
            message_id = client.delay({"task": "later"}, run_at=100)
            client.delay_backend.publisher = BrokenPublisher()

            with self.assertRaises(RuntimeError):
                client.schedule_due(now=150)

            delayed_key = client.config.key("delayed")
            payload_key = client.config.key(f"payload:{message_id}")
            self.assertEqual(redis.zscore(delayed_key, message_id), 100)
            self.assertIsNotNone(redis.get(payload_key))
        finally:
            client.close()
            _cleanup(redis, queue)
            redis.close()


if __name__ == "__main__":
    unittest.main()
