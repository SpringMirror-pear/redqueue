# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Asynchronous Redis List queue example."""

from __future__ import annotations

import asyncio

from common import REDIS_URL, example_queue

from redqueue import AsyncQueueClient, RetryConfig


async def main() -> None:
    client = await AsyncQueueClient.from_url(
        REDIS_URL,
        queue=example_queue("async-list"),
        backend="list",
        retry=RetryConfig(max_retries=1),
    )
    try:
        message_id = await client.publish({"task": "render-report"})
        message = await client.consume(timeout=1)
        if message is not None:
            print(f"published={message_id} consumed={message.id}")
            await client.ack(message)

        await client.publish({"task": "retry-once"})
        failed = await client.consume(timeout=1)
        if failed is not None:
            await client.retry(failed, reason="transient failure")
            retried = await client.consume(timeout=1)
            if retried is not None:
                await client.nack(retried, requeue=False)
        print(f"dead_letters={len(await client.dead_letters())}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
