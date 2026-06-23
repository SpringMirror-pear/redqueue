# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Tests for RedQueue command line diagnostics."""

from __future__ import annotations

import json
import unittest
from io import StringIO
from typing import Any

from redqueue.cli import run_cli
from redqueue.message import Message


class FakeCliRedis:
    """Redis fake used by CLI diagnostics tests."""

    def __init__(self) -> None:
        """Initialize deterministic Redis command responses."""

        self.closed = False
        self.counts = {
            "rq:{emails}:ready": 3,
            "rq:{emails}:processing": 1,
            "rq:{emails}:dead": 2,
            "rq:{emails}:delayed": 4,
            "rq:{emails}:stream": 5,
        }

    def info(self, section: str | None = None) -> dict[str, str]:
        """Return fake Redis server information."""

        self.section = section
        return {"redis_version": "7.2.1"}

    def llen(self, name: str) -> int:
        """Return fake List length."""

        return self.counts[name]

    def zcard(self, name: str) -> int:
        """Return fake Sorted Set cardinality."""

        return self.counts[name]

    def xlen(self, name: str) -> int:
        """Return fake Stream length."""

        return self.counts[name]

    def close(self) -> None:
        """Record resource cleanup."""

        self.closed = True


class FakeCliClient:
    """QueueClient fake used by CLI command tests."""

    def __init__(
        self,
        url: str,
        *,
        queue: str,
        backend: str,
        namespace: str,
        consumer_group: str,
        consumer_name: str | None,
    ) -> None:
        """Capture construction options."""

        self.url = url
        self.queue = queue
        self.backend = backend
        self.namespace = namespace
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self.closed = False
        self.messages = [
            Message(id="msg-1", queue=queue, payload={"task": "send"}),
            Message(id="msg-2", queue=queue, payload={"task": "audit"}),
        ]
        self.actions: list[tuple[str, str]] = []

    def publish(
        self,
        payload: Any,
        *,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> str:
        """Return a deterministic published message id."""

        self.payload = payload
        self.headers = headers
        return message_id or "published-1"

    def consume(
        self,
        *,
        timeout: float | None = None,
        batch_size: int = 1,
    ) -> Message | list[Message] | None:
        """Return fake consumed messages."""

        self.timeout = timeout
        if batch_size <= 1:
            return self.messages[0]
        return self.messages[:batch_size]

    def ack(self, message: Message) -> None:
        """Record an ack action."""

        self.actions.append(("ack", message.id))

    def nack(self, message: Message, *, requeue: bool = True) -> None:
        """Record a nack action."""

        self.actions.append(("nack", f"{message.id}:{requeue}"))

    def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        """Record a retry action."""

        self.actions.append(("retry", f"{message.id}:{reason}"))

    def delay(
        self,
        payload: Any,
        *,
        delay_seconds: float | None = None,
        run_at: float | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        """Return a deterministic delayed message id."""

        self.payload = payload
        self.delay_seconds = delay_seconds
        self.run_at = run_at
        self.headers = headers
        return "delayed-1"

    def schedule_due(self, *, limit: int = 100, now: float | None = None) -> int:
        """Return a deterministic due release count."""

        self.limit = limit
        self.now = now
        return 2

    def dead_letters(self, *, limit: int = 100) -> list[Message]:
        """Return fake dead letters."""

        self.limit = limit
        return [Message(id="dead-1", queue=self.queue, payload={"failed": True})]

    def close(self) -> None:
        """Record resource cleanup."""

        self.closed = True


class CliTests(unittest.TestCase):
    """CLI behavior tests."""

    def run_command(
        self,
        *argv: str,
        client: FakeCliClient | None = None,
        redis: FakeCliRedis | None = None,
    ) -> tuple[int, dict[str, Any] | None, str, FakeCliClient | None, FakeCliRedis]:
        """Run a CLI command with fake dependencies."""

        created_client = client
        created_redis = redis or FakeCliRedis()

        def client_factory(*args: Any, **kwargs: Any) -> FakeCliClient:
            nonlocal created_client
            created_client = created_client or FakeCliClient(*args, **kwargs)
            return created_client

        def redis_factory(_url: str) -> FakeCliRedis:
            return created_redis

        stdout = StringIO()
        stderr = StringIO()
        code = run_cli(
            argv,
            stdout=stdout,
            stderr=stderr,
            client_factory=client_factory,
            redis_factory=redis_factory,
        )
        output = stdout.getvalue().strip()
        data = json.loads(output) if output else None
        return code, data, stderr.getvalue(), created_client, created_redis

    def test_check_reports_redis_capabilities(self) -> None:
        code, data, stderr, _client, redis = self.run_command("check")

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["redis_version"], "7.2.1")
        self.assertTrue(data["supports"]["streams"])
        self.assertTrue(redis.closed)

    def test_stats_reports_queue_counts(self) -> None:
        code, data, stderr, _client, redis = self.run_command(
            "stats",
            "--queue",
            "emails",
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["counts"]["ready"], 3)
        self.assertEqual(data["counts"]["delayed"], 4)
        self.assertIsNone(data["counts"]["stream"])
        self.assertTrue(redis.closed)

    def test_publish_parses_payload_and_headers(self) -> None:
        code, data, stderr, client, _redis = self.run_command(
            "publish",
            "--queue",
            "emails",
            "--payload",
            '{"to":"user@example.com"}',
            "--headers",
            '{"trace_id":"abc"}',
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["message_id"], "published-1")
        self.assertEqual(client.payload, {"to": "user@example.com"})
        self.assertEqual(client.headers, {"trace_id": "abc"})
        self.assertTrue(client.closed)

    def test_consume_can_ack_batch(self) -> None:
        code, data, stderr, client, _redis = self.run_command(
            "consume",
            "--queue",
            "emails",
            "--batch-size",
            "2",
            "--ack",
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["count"], 2)
        self.assertEqual(data["action"], "ack")
        self.assertEqual(client.actions, [("ack", "msg-1"), ("ack", "msg-2")])
        self.assertTrue(client.closed)

    def test_delay_and_schedule_due_commands(self) -> None:
        code, data, stderr, client, _redis = self.run_command(
            "delay",
            "--queue",
            "emails",
            "--payload",
            '{"to":"later@example.com"}',
            "--delay-seconds",
            "30",
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["message_id"], "delayed-1")
        self.assertEqual(client.delay_seconds, 30)
        self.assertTrue(client.closed)

        next_client = FakeCliClient(
            "redis://127.0.0.1:6379/0",
            queue="emails",
            backend="list",
            namespace="rq",
            consumer_group="redqueue",
            consumer_name=None,
        )
        code, data, stderr, client, _redis = self.run_command(
            "schedule-due",
            "--queue",
            "emails",
            "--limit",
            "10",
            "--now",
            "100",
            client=next_client,
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["released"], 2)
        self.assertEqual(client.limit, 10)
        self.assertEqual(client.now, 100)

    def test_dead_letters_outputs_messages(self) -> None:
        code, data, stderr, client, _redis = self.run_command(
            "dead-letters",
            "--queue",
            "emails",
        )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["messages"][0]["id"], "dead-1")
        self.assertTrue(client.closed)

    def test_invalid_json_returns_user_error(self) -> None:
        code, data, stderr, _client, _redis = self.run_command(
            "publish",
            "--queue",
            "emails",
            "--payload",
            "{bad-json",
        )

        self.assertEqual(code, 2)
        self.assertIsNone(data)
        self.assertIn("payload must be valid JSON", stderr)


if __name__ == "__main__":
    unittest.main()
