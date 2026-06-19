# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Opt-in Redis integration tests.

Run with REDQUEUE_REDIS_URL=redis://localhost:6379/0 to exercise a real Redis.
"""

import os
import unittest

import pytest

from redqueue import QueueClient

REDIS_URL = os.getenv("REDQUEUE_REDIS_URL")


@pytest.mark.integration
@unittest.skipUnless(REDIS_URL, "set REDQUEUE_REDIS_URL to run Redis integration tests")
class RedisIntegrationTests(unittest.TestCase):
    def test_list_publish_consume_ack_against_real_redis(self) -> None:
        client = QueueClient.from_url(
            REDIS_URL,
            queue="redqueue-integration-list",
            backend="list",
        )
        try:
            message_id = client.publish({"integration": True})
            message = client.consume(timeout=1)

            self.assertEqual(message.id, message_id)
            self.assertEqual(message.payload, {"integration": True})

            client.ack(message)
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
