# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Real Redis performance and concurrency tests."""

from __future__ import annotations

import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from redis import Redis

from redqueue import QueueClient

REDIS_URL = os.getenv("REDQUEUE_REDIS_URL")


def _queue_name(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _redis() -> Redis:
    return Redis.from_url(REDIS_URL)


def _cleanup(redis: Redis, queue: str) -> None:
    keys = list(redis.scan_iter(match=f"rq:{{{queue}}}:*", count=1000))
    if keys:
        redis.delete(*keys)


def _rate(operations: int, elapsed_seconds: float) -> float:
    return operations / max(elapsed_seconds, 1e-9)


@pytest.mark.integration
@pytest.mark.performance
@unittest.skipUnless(REDIS_URL, "set REDQUEUE_REDIS_URL to run real Redis tests")
class RealRedisPerformanceTests(unittest.TestCase):
    def test_real_redis_list_roundtrip_baseline(self) -> None:
        queue = _queue_name("real-performance-list")
        redis = _redis()
        _cleanup(redis, queue)
        client = QueueClient.from_url(REDIS_URL, queue=queue, backend="list")
        operations = 200
        try:
            start = time.perf_counter()
            for index in range(operations):
                client.publish({"index": index})
            for _ in range(operations):
                message = client.consume(timeout=1)
                client.ack(message)
            elapsed = time.perf_counter() - start

            self.assertGreaterEqual(_rate(operations, elapsed), 20)
        finally:
            client.close()
            _cleanup(redis, queue)
            redis.close()

    def test_real_redis_stream_roundtrip_baseline(self) -> None:
        queue = _queue_name("real-performance-stream")
        redis = _redis()
        _cleanup(redis, queue)
        client = QueueClient.from_url(REDIS_URL, queue=queue, backend="stream")
        operations = 100
        try:
            start = time.perf_counter()
            for index in range(operations):
                client.publish({"index": index})
            for _ in range(operations):
                message = client.consume(timeout=1)
                client.ack(message)
            elapsed = time.perf_counter() - start

            self.assertGreaterEqual(_rate(operations, elapsed), 10)
        finally:
            client.close()
            _cleanup(redis, queue)
            redis.close()

    def test_real_redis_delay_schedule_release_baseline(self) -> None:
        queue = _queue_name("real-performance-delay")
        redis = _redis()
        _cleanup(redis, queue)
        client = QueueClient.from_url(REDIS_URL, queue=queue, backend="list")
        operations = 100
        try:
            start = time.perf_counter()
            for index in range(operations):
                client.delay({"index": index}, run_at=100)
            released = client.schedule_due(limit=operations, now=150)
            elapsed = time.perf_counter() - start

            self.assertEqual(released, operations)
            self.assertGreaterEqual(_rate(operations, elapsed), 10)
        finally:
            client.close()
            _cleanup(redis, queue)
            redis.close()


@pytest.mark.integration
@pytest.mark.performance
@pytest.mark.concurrency
@unittest.skipUnless(REDIS_URL, "set REDQUEUE_REDIS_URL to run real Redis tests")
class RealRedisConcurrencyTests(unittest.TestCase):
    def test_real_redis_concurrent_list_publish_consume_ack(self) -> None:
        queue = _queue_name("real-concurrency-list")
        redis = _redis()
        _cleanup(redis, queue)
        total_messages = 200
        workers = 4
        per_worker = total_messages // workers
        processed_ids: set[str] = set()
        lock = threading.Lock()

        def publish_batch(worker: int) -> None:
            client = QueueClient.from_url(REDIS_URL, queue=queue, backend="list")
            try:
                for index in range(per_worker):
                    client.publish({"worker": worker, "index": index})
            finally:
                client.close()

        def consume_batch() -> None:
            client = QueueClient.from_url(REDIS_URL, queue=queue, backend="list")
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    with lock:
                        if len(processed_ids) >= total_messages:
                            return
                    message = client.consume(timeout=1)
                    if message is None:
                        continue
                    client.ack(message)
                    with lock:
                        processed_ids.add(message.id)
                        if len(processed_ids) >= total_messages:
                            return
            finally:
                client.close()

        try:
            start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=workers) as executor:
                list(executor.map(publish_batch, range(workers)))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                list(executor.map(lambda _worker: consume_batch(), range(workers)))
            elapsed = time.perf_counter() - start

            self.assertEqual(len(processed_ids), total_messages)
            self.assertGreaterEqual(_rate(total_messages, elapsed), 10)
        finally:
            _cleanup(redis, queue)
            redis.close()


if __name__ == "__main__":
    unittest.main()
