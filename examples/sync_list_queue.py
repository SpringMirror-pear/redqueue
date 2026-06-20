# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Synchronous Redis List queue example."""

from __future__ import annotations

from common import REDIS_URL, example_queue

from redqueue import QueueClient, RetryConfig


def main() -> None:
    client = QueueClient.from_url(
        REDIS_URL,
        queue=example_queue("sync-list"),
        backend="list",
        retry=RetryConfig(max_retries=1),
    )
    try:
        message_id = client.publish(
            {"kind": "email", "to": "user@example.com"},
            headers={"trace_id": "sync-list-1"},
        )
        message = client.consume(timeout=1)
        if message is None:
            print("no message consumed")
            return

        print(f"published={message_id} consumed={message.id} payload={message.payload}")
        client.ack(message)

        client.publish({"kind": "temporary-failure"})
        failed = client.consume(timeout=1)
        if failed is not None:
            client.retry(failed, reason="temporary backend error")
            retried = client.consume(timeout=1)
            if retried is not None:
                client.nack(retried, requeue=False)

        dead = client.dead_letters(limit=10)
        print(f"dead_letters={len(dead)}")
        if dead:
            client.requeue_dead(dead[0])
            recovered = client.consume(timeout=1)
            if recovered is not None:
                client.ack(recovered)
                print(f"requeued_dead={recovered.id}")

        client.publish({"kind": "needs-recovery"})
        in_processing = client.consume(timeout=1)
        if in_processing is not None:
            recovered_count = client.recover_stale(limit=10)
            print(f"recovered_processing={recovered_count}")
            recovered_message = client.consume(timeout=1)
            if recovered_message is not None:
                client.ack(recovered_message)
    finally:
        client.close()


if __name__ == "__main__":
    main()
