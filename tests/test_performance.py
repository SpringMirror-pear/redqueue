# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Deterministic in-memory performance baseline tests."""

import asyncio
import time
import unittest

import pytest

from redqueue import (
    AsyncQueueClient,
    JsonSerializer,
    QueueClient,
    QueueConfig,
    RedisCapabilities,
    RedisVersion,
)
from tests.fakes import FakeAsyncListRedis, FakeListRedis, FakeStreamRedis


def _rate(operations: int, elapsed_seconds: float) -> float:
    return operations / max(elapsed_seconds, 1e-9)


@pytest.mark.performance
class PerformanceTests(unittest.TestCase):
    def test_json_serializer_encode_decode_baseline(self) -> None:
        serializer = JsonSerializer()
        operations = 10_000
        payload = {
            "id": "message",
            "queue": "performance",
            "payload": {"value": 1},
            "headers": {"trace_id": "abc"},
            "attempts": 0,
            "created_at": 1.0,
            "available_at": None,
            "backend": "list",
            "raw_id": None,
        }

        start = time.perf_counter()
        for _ in range(operations):
            encoded = serializer.encode(payload, queue="performance")
            serializer.decode(encoded, queue="performance")
        elapsed = time.perf_counter() - start

        self.assertGreaterEqual(_rate(operations, elapsed), 1_000)

    def test_sync_list_roundtrip_baseline(self) -> None:
        operations = 2_000
        client = QueueClient(
            QueueConfig(queue="performance-list"),
            redis=FakeListRedis(),
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        start = time.perf_counter()
        for index in range(operations):
            client.publish({"index": index})
        for _ in range(operations):
            message = client.consume(timeout=0)
            client.ack(message)
        elapsed = time.perf_counter() - start

        self.assertGreaterEqual(_rate(operations, elapsed), 500)

    def test_sync_stream_roundtrip_baseline(self) -> None:
        operations = 1_000
        client = QueueClient(
            QueueConfig(queue="performance-stream", backend="stream"),
            redis=FakeStreamRedis(),
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        start = time.perf_counter()
        for index in range(operations):
            client.publish({"index": index})
        for _ in range(operations):
            message = client.consume(timeout=0)
            client.ack(message)
        elapsed = time.perf_counter() - start

        self.assertGreaterEqual(_rate(operations, elapsed), 300)

    def test_delay_schedule_and_release_baseline(self) -> None:
        operations = 1_000
        client = QueueClient(
            QueueConfig(queue="performance-delay"),
            redis=FakeListRedis(),
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

        start = time.perf_counter()
        for index in range(operations):
            client.delay({"index": index}, run_at=100)
        released = client.schedule_due(limit=operations, now=150)
        elapsed = time.perf_counter() - start

        self.assertEqual(released, operations)
        self.assertGreaterEqual(_rate(operations, elapsed), 300)

    def test_async_list_roundtrip_baseline(self) -> None:
        async def run() -> float:
            operations = 1_000
            client = AsyncQueueClient(
                QueueConfig(queue="performance-async-list"),
                redis=FakeAsyncListRedis(),
                capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
            )

            start = time.perf_counter()
            for index in range(operations):
                await client.publish({"index": index})
            for _ in range(operations):
                message = await client.consume(timeout=0)
                await client.ack(message)
            elapsed = time.perf_counter() - start
            return _rate(operations, elapsed)

        self.assertGreaterEqual(asyncio.run(run()), 300)


if __name__ == "__main__":
    unittest.main()
