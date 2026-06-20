# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Delayed tasks example based on Redis Sorted Set."""

from __future__ import annotations

from time import time
from uuid import uuid4

from common import REDIS_URL, example_queue

from redqueue import QueueClient


def main() -> None:
    client = QueueClient.from_url(
        REDIS_URL,
        queue=example_queue(f"delay-{uuid4().hex[:8]}"),
        backend="list",
    )
    try:
        future_id = client.delay({"task": "send-later"}, run_at=time() + 60)
        due_id = client.delay({"task": "send-now"}, delay_seconds=0)

        released = client.schedule_due(limit=100)
        print(f"future_id={future_id} due_id={due_id} released={released}")

        message = client.consume(timeout=1)
        if message is not None:
            print(f"scheduled_payload={message.payload}")
            client.ack(message)
    finally:
        client.close()


if __name__ == "__main__":
    main()
