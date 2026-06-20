# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Custom serializer example."""

from __future__ import annotations

import json
from typing import Any

from common import REDIS_URL, example_queue
from redis import Redis

from redqueue import QueueClient, QueueConfig


class TaggedJsonSerializer:
    """Example deterministic serializer with a custom content type."""

    content_type = "application/vnd.redqueue.example+json"

    def encode(self, payload: Any, *, queue: str | None = None) -> bytes:
        envelope = {"queue": queue, "payload": payload}
        return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()

    def decode(self, payload: bytes, *, queue: str | None = None) -> Any:
        return json.loads(payload.decode())["payload"]


def main() -> None:
    redis = Redis.from_url(REDIS_URL)
    client = QueueClient(
        QueueConfig(
            queue=example_queue("serializer"),
            serializer=TaggedJsonSerializer(),
        ),
        redis=redis,
    )
    try:
        client.publish({"payload": ["custom", "json", "serializer"]})
        message = client.consume(timeout=1)
        if message is not None:
            print(message.payload)
            client.ack(message)
    finally:
        client.close()


if __name__ == "__main__":
    main()
