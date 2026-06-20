# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Monitoring hooks example."""

from __future__ import annotations

from common import REDIS_URL, example_queue
from redis import Redis

from redqueue import (
    CompositeMonitoringHook,
    InMemoryMonitoringHook,
    MonitoringEvent,
    QueueClient,
    QueueConfig,
    SafeMonitoringHook,
)


class PrintingHook:
    """Simple hook that prints each monitoring event."""

    def emit(self, event: MonitoringEvent) -> None:
        print(event.to_dict())


class FailingHook:
    """Hook used to demonstrate SafeMonitoringHook isolation."""

    def emit(self, event: MonitoringEvent) -> None:
        raise RuntimeError("monitoring sink is unavailable")


def main() -> None:
    memory = InMemoryMonitoringHook()
    safe_failing = SafeMonitoringHook(FailingHook())
    monitoring = CompositeMonitoringHook(memory, PrintingHook(), safe_failing)
    redis = Redis.from_url(REDIS_URL)
    client = QueueClient(
        QueueConfig(queue=example_queue("monitoring"), monitoring=monitoring),
        redis=redis,
    )
    try:
        message_id = client.publish({"metric": "example"})
        message = client.consume(timeout=1)
        if message is not None:
            client.ack(message)
        print(f"message_id={message_id}")
        print(f"events={len(memory.events)} isolated_errors={len(safe_failing.errors)}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
