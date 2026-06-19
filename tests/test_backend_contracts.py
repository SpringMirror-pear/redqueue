# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Backend contract tests shared across supported backends."""

import unittest

import pytest

from redqueue import (
    QueueClient,
    QueueConfig,
    RedisCapabilities,
    RedisVersion,
    RetryConfig,
    RetryExceededError,
)
from tests.fakes import FakeListRedis, FakeStreamRedis


@pytest.mark.contract
class BackendContractTests(unittest.TestCase):
    def make_client(self, backend: str) -> QueueClient:
        redis = FakeStreamRedis() if backend == "stream" else FakeListRedis()
        return QueueClient(
            QueueConfig(
                queue=f"{backend}-contract",
                backend=backend,
                retry=RetryConfig(max_retries=1),
            ),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(7, 0, 0)),
        )

    def test_publish_consume_ack_contract(self) -> None:
        for backend in ("list", "stream"):
            with self.subTest(backend=backend):
                client = self.make_client(backend)
                message_id = client.publish({"backend": backend})
                message = client.consume(timeout=1)

                self.assertEqual(message.id, message_id)
                self.assertEqual(message.payload, {"backend": backend})

                client.ack(message)

    def test_nack_requeue_contract(self) -> None:
        for backend in ("list", "stream"):
            with self.subTest(backend=backend):
                client = self.make_client(backend)
                message_id = client.publish({"backend": backend})
                message = client.consume(timeout=1)
                client.nack(message, requeue=True)
                requeued = client.consume(timeout=1)

                self.assertEqual(requeued.id, message_id)

    def test_retry_dead_letter_contract(self) -> None:
        for backend in ("list", "stream"):
            with self.subTest(backend=backend):
                client = self.make_client(backend)
                client.publish({"backend": backend})
                message = client.consume(timeout=1)
                client.retry(message, reason="contract")
                retried = client.consume(timeout=1)

                self.assertEqual(retried.attempts, 1)

                with self.assertRaises(RetryExceededError):
                    client.retry(retried, reason="contract")

                self.assertTrue(client.dead_letters(limit=10))


if __name__ == "__main__":
    unittest.main()
