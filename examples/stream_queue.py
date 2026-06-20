# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Redis Streams queue example."""

from __future__ import annotations

from common import REDIS_URL, example_queue

from redqueue import QueueClient, RedisCompatibilityError


def main() -> None:
    try:
        client = QueueClient.from_url(
            REDIS_URL,
            queue=example_queue("stream"),
            backend="stream",
            consumer_group="redqueue-examples",
            consumer_name="worker-1",
        )
    except RedisCompatibilityError as exc:
        print(f"streams unavailable: {exc}")
        return

    try:
        message_id = client.publish({"event": "user.created"})
        message = client.consume(timeout=1)
        if message is None:
            print("no stream message consumed")
            return

        print(f"stream_published={message_id} raw_id={message.raw_id}")
        recovered = client.backend.recover_pending(min_idle_ms=1, limit=10)
        print(f"pending_recovered={len(recovered)}")
        client.ack(recovered[0] if recovered else message)
    finally:
        client.close()


if __name__ == "__main__":
    main()
