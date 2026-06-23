# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Command line interface for RedQueue diagnostics and debugging."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from typing import Any, TextIO, cast

from redis import Redis

from redqueue._version import __version__
from redqueue.client import QueueClient
from redqueue.compat import RedisInfoClient, detect_capabilities
from redqueue.config import BackendType
from redqueue.exceptions import RedQueueError
from redqueue.message import Message

ClientFactory = Callable[..., QueueClient]
RedisFactory = Callable[[str], Any]


class CliError(Exception):
    """Raised for user-facing CLI validation failures."""


def main(argv: Sequence[str] | None = None) -> int:
    """Run the RedQueue CLI.

    Args:
        argv: Optional command arguments. ``None`` uses ``sys.argv``.

    Returns:
        Process exit code.
    """

    return run_cli(argv, stdout=sys.stdout, stderr=sys.stderr)


def run_cli(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO,
    stderr: TextIO,
    client_factory: ClientFactory = QueueClient.from_url,
    redis_factory: RedisFactory | None = None,
) -> int:
    """Execute a CLI command with injectable dependencies for tests.

    Args:
        argv: Command arguments without the executable name.
        stdout: Stream receiving command output.
        stderr: Stream receiving errors.
        client_factory: Factory compatible with ``QueueClient.from_url``.
        redis_factory: Redis factory used by direct diagnostic commands.

    Returns:
        Process exit code where ``0`` means success.
    """

    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "command", None) is None:
        parser.print_help(stdout)
        return 0

    redis_factory = redis_factory or Redis.from_url
    try:
        result = args.handler(args, client_factory, redis_factory)
    except (CliError, RedQueueError) as exc:
        print(str(exc), file=stderr)
        return 2
    except Exception as exc:
        print(f"RedQueue CLI command failed: {exc}", file=stderr)
        return 1

    if result is not None:
        write_json(stdout, result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI argument parser.

    Returns:
        Configured ``argparse.ArgumentParser`` instance.
    """

    parser = argparse.ArgumentParser(
        prog="redqueue",
        description="Debug Redis-backed RedQueue queues.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"redqueue {__version__}",
    )
    subcommands = parser.add_subparsers(dest="command")

    check = subcommands.add_parser("check", help="check Redis compatibility")
    add_redis_args(check)
    check.set_defaults(handler=handle_check)

    stats = subcommands.add_parser("stats", help="inspect queue key counts")
    add_queue_args(stats)
    stats.set_defaults(handler=handle_stats)

    publish = subcommands.add_parser("publish", help="publish a message")
    add_queue_args(publish)
    add_payload_args(publish)
    publish.add_argument("--message-id", default=None)
    publish.set_defaults(handler=handle_publish)

    consume = subcommands.add_parser("consume", help="consume messages")
    add_queue_args(consume)
    consume.add_argument("--timeout", type=float, default=1.0)
    consume.add_argument("--batch-size", type=int, default=1)
    action = consume.add_mutually_exclusive_group()
    action.add_argument("--ack", action="store_true", help="ack consumed messages")
    action.add_argument("--nack", action="store_true", help="nack consumed messages")
    action.add_argument("--retry", action="store_true", help="retry consumed messages")
    consume.add_argument(
        "--no-requeue",
        action="store_true",
        help="move nacked messages to dead letters",
    )
    consume.add_argument("--reason", default="redqueue-cli")
    consume.set_defaults(handler=handle_consume)

    delay = subcommands.add_parser("delay", help="schedule a delayed message")
    add_queue_args(delay)
    add_payload_args(delay)
    delay_time = delay.add_mutually_exclusive_group()
    delay_time.add_argument("--delay-seconds", type=float, default=None)
    delay_time.add_argument("--run-at", type=float, default=None)
    delay.set_defaults(handler=handle_delay)

    schedule_due = subcommands.add_parser(
        "schedule-due",
        help="release due delayed messages",
    )
    add_queue_args(schedule_due)
    schedule_due.add_argument("--limit", type=int, default=100)
    schedule_due.add_argument("--now", type=float, default=None)
    schedule_due.set_defaults(handler=handle_schedule_due)

    dead_letters = subcommands.add_parser(
        "dead-letters",
        help="read dead-lettered messages",
    )
    add_queue_args(dead_letters)
    dead_letters.add_argument("--limit", type=int, default=100)
    dead_letters.set_defaults(handler=handle_dead_letters)

    return parser


def add_redis_args(parser: argparse.ArgumentParser) -> None:
    """Add Redis connection arguments to a parser."""

    parser.add_argument(
        "--url",
        default="redis://127.0.0.1:6379/0",
        help="Redis URL",
    )


def add_queue_args(parser: argparse.ArgumentParser) -> None:
    """Add common queue arguments to a parser."""

    add_redis_args(parser)
    parser.add_argument("--queue", required=True, help="RedQueue queue name")
    parser.add_argument(
        "--backend",
        choices=[item.value for item in BackendType],
        default=BackendType.LIST.value,
    )
    parser.add_argument("--namespace", default="rq")
    parser.add_argument("--consumer-group", default="redqueue")
    parser.add_argument("--consumer-name", default=None)


def add_payload_args(parser: argparse.ArgumentParser) -> None:
    """Add JSON payload and header arguments to a parser."""

    parser.add_argument(
        "--payload",
        required=True,
        help="JSON payload, for example '{\"hello\":\"world\"}'",
    )
    parser.add_argument(
        "--headers",
        default=None,
        help="Optional JSON object with message headers",
    )


def handle_check(
    args: argparse.Namespace,
    _client_factory: ClientFactory,
    redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``check`` command.

    Args:
        args: Parsed command arguments.
        _client_factory: Unused client factory kept for handler parity.
        redis_factory: Redis factory used for capability detection.

    Returns:
        Redis version and feature capability flags.
    """

    redis = redis_factory(args.url)
    try:
        capabilities = detect_capabilities(cast(RedisInfoClient, redis))
        return {
            "redis_version": str(capabilities.version),
            "supports": {
                "list_blocking": capabilities.supports_list_blocking,
                "list_reliable": capabilities.supports_list_reliable_brpoplpush,
                "list_blmove": capabilities.supports_list_reliable_blmove,
                "streams": capabilities.supports_streams,
                "streams_auto_claim": capabilities.supports_streams_auto_claim,
                "delayed_tasks": capabilities.supports_delay_sorted_set,
            },
        }
    finally:
        close_redis(redis)


def handle_stats(
    args: argparse.Namespace,
    _client_factory: ClientFactory,
    redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``stats`` command.

    Args:
        args: Parsed command arguments.
        _client_factory: Unused client factory kept for handler parity.
        redis_factory: Redis factory used to read Redis key counts.

    Returns:
        Queue key counts for the selected backend and delay store.
    """

    redis = redis_factory(args.url)
    try:
        ready_key = key(args.namespace, args.queue, "ready")
        processing_key = key(args.namespace, args.queue, "processing")
        dead_key = key(args.namespace, args.queue, "dead")
        delayed_key = key(args.namespace, args.queue, "delayed")
        stream_key = key(args.namespace, args.queue, "stream")
        is_stream = args.backend == BackendType.STREAM.value
        return {
            "queue": args.queue,
            "namespace": args.namespace,
            "backend": args.backend,
            "keys": {
                "ready": ready_key,
                "processing": processing_key,
                "dead": dead_key,
                "delayed": delayed_key,
                "stream": stream_key,
            },
            "counts": {
                "ready": call_int(redis, "llen", ready_key),
                "processing": call_int(redis, "llen", processing_key),
                "dead": (
                    call_int(redis, "xlen", dead_key)
                    if is_stream
                    else call_int(redis, "llen", dead_key)
                ),
                "delayed": call_int(redis, "zcard", delayed_key),
                "stream": call_int(redis, "xlen", stream_key) if is_stream else None,
            },
        }
    finally:
        close_redis(redis)


def handle_publish(
    args: argparse.Namespace,
    client_factory: ClientFactory,
    _redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``publish`` command."""

    payload = parse_json(args.payload, field_name="payload")
    headers = parse_optional_headers(args.headers)
    client = create_client(args, client_factory)
    try:
        message_id = client.publish(
            payload,
            headers=headers,
            message_id=args.message_id,
        )
        return {"message_id": message_id, "queue": args.queue, "backend": args.backend}
    finally:
        client.close()


def handle_consume(
    args: argparse.Namespace,
    client_factory: ClientFactory,
    _redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``consume`` command."""

    client = create_client(args, client_factory)
    try:
        consumed = client.consume(timeout=args.timeout, batch_size=args.batch_size)
        messages = normalize_messages(consumed)
        for message in messages:
            if args.ack:
                client.ack(message)
            elif args.nack:
                client.nack(message, requeue=not args.no_requeue)
            elif args.retry:
                client.retry(message, reason=args.reason)
        return {
            "count": len(messages),
            "action": consume_action(args),
            "messages": [message_to_dict(message) for message in messages],
        }
    finally:
        client.close()


def handle_delay(
    args: argparse.Namespace,
    client_factory: ClientFactory,
    _redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``delay`` command."""

    payload = parse_json(args.payload, field_name="payload")
    headers = parse_optional_headers(args.headers)
    client = create_client(args, client_factory)
    try:
        message_id = client.delay(
            payload,
            delay_seconds=args.delay_seconds,
            run_at=args.run_at,
            headers=headers,
        )
        return {"message_id": message_id, "queue": args.queue, "backend": args.backend}
    finally:
        client.close()


def handle_schedule_due(
    args: argparse.Namespace,
    client_factory: ClientFactory,
    _redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``schedule-due`` command."""

    client = create_client(args, client_factory)
    try:
        released = client.schedule_due(limit=args.limit, now=args.now)
        return {"released": released, "queue": args.queue, "backend": args.backend}
    finally:
        client.close()


def handle_dead_letters(
    args: argparse.Namespace,
    client_factory: ClientFactory,
    _redis_factory: RedisFactory,
) -> dict[str, Any]:
    """Handle the ``dead-letters`` command."""

    client = create_client(args, client_factory)
    try:
        messages = client.dead_letters(limit=args.limit)
        return {
            "count": len(messages),
            "messages": [message_to_dict(message) for message in messages],
        }
    finally:
        client.close()


def create_client(
    args: argparse.Namespace,
    client_factory: ClientFactory,
) -> QueueClient:
    """Create a synchronous client from parsed queue arguments.

    Args:
        args: Parsed command arguments.
        client_factory: Factory compatible with ``QueueClient.from_url``.

    Returns:
        Configured ``QueueClient``.
    """

    return client_factory(
        args.url,
        queue=args.queue,
        backend=args.backend,
        namespace=args.namespace,
        consumer_group=args.consumer_group,
        consumer_name=args.consumer_name,
    )


def parse_json(value: str, *, field_name: str) -> Any:
    """Parse JSON CLI input.

    Args:
        value: Raw JSON text.
        field_name: Field name used in error messages.

    Returns:
        Parsed JSON value.

    Raises:
        CliError: If the text is not valid JSON.
    """

    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise CliError(f"{field_name} must be valid JSON: {exc.msg}") from exc


def parse_optional_headers(value: str | None) -> dict[str, Any] | None:
    """Parse optional JSON headers.

    Args:
        value: Optional JSON object text.

    Returns:
        Parsed headers or ``None``.

    Raises:
        CliError: If headers are not a JSON object.
    """

    if value is None:
        return None
    headers = parse_json(value, field_name="headers")
    if not isinstance(headers, dict):
        raise CliError("headers must be a JSON object")
    return headers


def normalize_messages(value: Message | list[Message] | None) -> list[Message]:
    """Normalize a consume result into a list."""

    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def message_to_dict(message: Message) -> dict[str, Any]:
    """Convert a message to CLI-safe JSON data."""

    return {
        "id": message.id,
        "queue": message.queue,
        "payload": message.payload,
        "headers": message.headers,
        "attempts": message.attempts,
        "created_at": message.created_at,
        "available_at": message.available_at,
        "backend": message.backend,
        "raw_id": message.raw_id,
    }


def consume_action(args: argparse.Namespace) -> str:
    """Return the post-consume action selected by CLI flags."""

    if args.ack:
        return "ack"
    if args.nack:
        return "nack_dead_letter" if args.no_requeue else "nack_requeue"
    if args.retry:
        return "retry"
    return "none"


def key(namespace: str, queue: str, suffix: str) -> str:
    """Build a RedQueue Redis key for diagnostics."""

    return f"{namespace}:{{{queue}}}:{suffix}"


def call_int(redis: Any, command: str, key_name: str) -> int | None:
    """Call an optional Redis integer command.

    Args:
        redis: Redis-like client.
        command: Redis command method name.
        key_name: Redis key passed to the command.

    Returns:
        Integer command result or ``None`` when the command is unavailable.
    """

    func = getattr(redis, command, None)
    if func is None:
        return None
    value = func(key_name)
    return int(value)


def close_redis(redis: Any) -> None:
    """Close a Redis-like object if it exposes ``close``."""

    close = getattr(redis, "close", None)
    if close is not None:
        close()


def write_json(stdout: TextIO, data: Any) -> None:
    """Write a stable JSON response to stdout."""

    stdout.write(json.dumps(data, ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
