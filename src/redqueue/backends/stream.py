# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Synchronous Redis Streams backend."""

from __future__ import annotations

from typing import Any, Protocol

from redqueue.backends.base import BaseMessageBackend
from redqueue.compat import RedisCapabilities, RedisVersion
from redqueue.config import QueueConfig
from redqueue.exceptions import AckError, BackendUnavailableError, RetryExceededError
from redqueue.message import Message, new_message_id
from redqueue.monitoring import MonitoringEventType


class SyncStreamRedis(Protocol):
    """Redis command subset required by the synchronous Streams backend."""

    def xadd(self, name: str, fields: dict[str, bytes | str], id: str = "*") -> str: ...

    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = True,
    ) -> bool: ...

    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> list[Any]: ...

    def xack(self, name: str, groupname: str, *ids: str) -> int: ...

    def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str,
        count: int | None = None,
    ) -> Any: ...

    def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
    ) -> list[Any]: ...

    def xclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        message_ids: list[str],
    ) -> list[Any]: ...


class StreamBackend(BaseMessageBackend):
    """Reliable queue backend implemented with Redis Streams.

    The backend stores messages in a Redis Stream and consumes through a
    consumer group. Redis 6.2+ uses ``XAUTOCLAIM`` for pending recovery, while
    Redis 5.x falls back to ``XPENDING`` and ``XCLAIM``.

    Attributes:
        redis: Redis client implementing ``SyncStreamRedis``.
        capabilities: Redis command capability set.
    """

    backend_name = "stream"

    def __init__(
        self,
        redis: SyncStreamRedis,
        config: QueueConfig,
        capabilities: RedisCapabilities,
    ) -> None:
        """Initialize a Streams backend and ensure the consumer group exists.

        Args:
            redis: Redis client implementing required Streams commands.
            config: Queue configuration.
            capabilities: Detected Redis capabilities.

        Raises:
            RedisCompatibilityError: If Redis Streams are unavailable.
            BackendUnavailableError: If consumer group initialization fails.
        """

        capabilities.require_streams()
        super().__init__(config)
        self.redis = redis
        self.capabilities = capabilities
        self._ensure_group(self.stream_key)

    @classmethod
    def for_modern_redis(
        cls,
        redis: SyncStreamRedis,
        config: QueueConfig,
    ) -> StreamBackend:
        """Create a Streams backend assuming a modern Redis server.

        Args:
            redis: Redis client implementing required Streams commands.
            config: Queue configuration.

        Returns:
            ``StreamBackend`` using Redis 7 capability assumptions.
        """

        return cls(redis, config, RedisCapabilities(RedisVersion(7, 0, 0)))

    @property
    def stream_key(self) -> str:
        """Redis Stream key containing active messages."""

        return self.config.key("stream")

    @property
    def dead_key(self) -> str:
        """Redis Stream key containing dead-lettered messages."""

        return self.config.key("dead")

    def publish(
        self,
        payload: Any,
        *,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
        trace_id: str | None = None,
    ) -> str:
        """Append a message to the active Redis Stream.

        Args:
            payload: Application payload.
            headers: Optional message metadata.
            message_id: Optional stable RedQueue message id.
            trace_id: Optional correlation id propagated with the message.

        Returns:
            RedQueue message id.
        """

        message = Message(
            id=message_id or new_message_id(),
            queue=self.config.queue,
            payload=payload,
            headers=headers or {},
            trace_id=trace_id,
            backend=self.backend_name,
        )
        self._publish_message(message)
        return message.id

    def consume(
        self,
        *,
        timeout: float | None = None,
        batch_size: int = 1,
    ) -> Message | list[Message] | None:
        """Read one or more messages through the consumer group.

        Args:
            timeout: Optional blocking timeout in seconds.
            batch_size: Maximum number of messages to read.

        Returns:
            ``None``, one ``Message``, or a list of messages.
        """

        block = int(timeout * 1000) if timeout is not None else None
        response = self._execute(
            "redis.xreadgroup",
            self.redis.xreadgroup,
            self.config.consumer_group,
            self._consumer_name(),
            {self.stream_key: ">"},
            batch_size,
            block,
        )
        messages = self._parse_read_response(response)
        if batch_size <= 1:
            if not messages:
                return None
            self._emit(MonitoringEventType.MESSAGE_CONSUMED, messages[0])
            return messages[0]
        for message in messages:
            self._emit(MonitoringEventType.MESSAGE_CONSUMED, message)
        return messages

    def ack(self, message: Message) -> None:
        """Acknowledge a Streams message with ``XACK``.

        Args:
            message: Message returned by ``consume`` or pending recovery.

        Raises:
            AckError: If the message has no raw stream id or Redis does not
                acknowledge it.
        """

        if not message.raw_id:
            raise AckError(
                "stream message is missing raw Redis stream id",
                action="message.ack",
                queue=self.config.queue,
                details={"message_id": message.id},
            )
        removed = self._execute(
            "redis.xack",
            self.redis.xack,
            self.stream_key,
            self.config.consumer_group,
            message.raw_id,
        )
        if removed < 1:
            raise AckError(
                "stream message was not acknowledged",
                action="message.ack",
                queue=self.config.queue,
                details={"message_id": message.id, "raw_id": message.raw_id},
            )
        self._emit(MonitoringEventType.MESSAGE_ACKED, message)

    def nack(self, message: Message, *, requeue: bool = True) -> None:
        """Reject a Streams message.

        Args:
            message: Message returned by ``consume``.
            requeue: When true, republish the payload and acknowledge the
                original. When false, move it to the dead-letter stream.
        """

        if requeue:
            self.publish(
                message.payload,
                headers=message.headers,
                message_id=message.id,
                trace_id=message.trace_id,
            )
            self.ack(message)
        else:
            self._move_to_dead(message)
        self._emit(
            MonitoringEventType.MESSAGE_NACKED,
            message,
            attributes={"requeue": requeue},
        )

    def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        """Retry a Streams message or dead-letter it.

        Args:
            message: Message returned by ``consume``.
            delay: Reserved delay hint for future retry scheduling.
            reason: Optional diagnostic reason.

        Raises:
            RetryExceededError: If retry attempts are exhausted.
        """

        if message.attempts >= self.config.retry.max_retries:
            self._move_to_dead(message)
            self._emit(
                MonitoringEventType.MESSAGE_DEAD_LETTERED,
                message,
                attributes={"reason": reason, "attempts": message.attempts},
            )
            raise RetryExceededError(
                "stream message exceeded max retries and was moved to dead letter",
                action="message.retry",
                queue=self.config.queue,
                details={
                    "message_id": message.id,
                    "attempts": message.attempts,
                    "max_retries": self.config.retry.max_retries,
                },
            )
        retried = message.with_attempt()
        self._publish_message(retried)
        self.ack(message)
        self._emit(
            MonitoringEventType.MESSAGE_RETRIED,
            retried,
            attributes={"delay": delay, "reason": reason},
        )

    def recover_pending(self, *, min_idle_ms: int, limit: int = 100) -> list[Message]:
        """Claim pending messages for the configured consumer.

        Args:
            min_idle_ms: Minimum idle time in milliseconds before a pending
                message can be claimed.
            limit: Maximum number of pending messages to recover.

        Returns:
            Claimed messages.
        """

        if not self.capabilities.supports_streams_auto_claim:
            pending = self._execute(
                "redis.xpending_range",
                self.redis.xpending_range,
                self.stream_key,
                self.config.consumer_group,
                "-",
                "+",
                limit,
            )
            message_ids = [self._pending_id(item) for item in pending]
            if not message_ids:
                return []
            claimed = self._execute(
                "redis.xclaim",
                self.redis.xclaim,
                self.stream_key,
                self.config.consumer_group,
                self._consumer_name(),
                min_idle_ms,
                message_ids,
            )
            return [
                self._decode_stream_entry(raw_id, fields)
                for raw_id, fields in claimed
            ]
        response = self._execute(
            "redis.xautoclaim",
            self.redis.xautoclaim,
            self.stream_key,
            self.config.consumer_group,
            self._consumer_name(),
            min_idle_ms,
            "0-0",
            limit,
        )
        return self._parse_autoclaim_response(response)

    def dead_letters(self, *, limit: int = 100) -> list[Message]:
        """Read messages from the dead-letter stream.

        Args:
            limit: Maximum number of dead letters to read.

        Returns:
            Decoded dead-letter messages.
        """

        self._ensure_group(self.dead_key)
        response = self._execute(
            "redis.xreadgroup",
            self.redis.xreadgroup,
            self.config.consumer_group,
            self._consumer_name(),
            {self.dead_key: ">"},
            limit,
            None,
        )
        return self._parse_read_response(response)

    def requeue_dead(self, message: Message) -> None:
        """Republish a dead-lettered message to the active stream.

        Args:
            message: Message returned by ``dead_letters``.
        """

        self.publish(
            message.payload,
            headers=message.headers,
            message_id=message.id,
            trace_id=message.trace_id,
        )
        if message.raw_id:
            self._execute(
                "redis.xack",
                self.redis.xack,
                self.dead_key,
                self.config.consumer_group,
                message.raw_id,
            )

    def _publish_message(self, message: Message) -> str:
        """Append an encoded RedQueue message envelope to the stream.

        Args:
            message: Message to publish.

        Returns:
            Redis stream entry id as text.
        """

        raw_id = self._execute(
            "redis.xadd",
            self.redis.xadd,
            self.stream_key,
            {"payload": self._encode(message)},
        )
        published = message.with_backend(
            self.backend_name,
            raw_id=self._to_text(raw_id),
        )
        self._emit(
            MonitoringEventType.MESSAGE_PUBLISHED,
            published,
            attributes={"key": self.stream_key},
        )
        return self._to_text(raw_id)

    def _ensure_group(self, stream_key: str) -> None:
        """Create the configured consumer group if it does not exist.

        Args:
            stream_key: Redis Stream key that should contain the group.

        Raises:
            BackendUnavailableError: If Redis rejects group creation for reasons
                other than ``BUSYGROUP``.
        """

        try:
            self.redis.xgroup_create(
                stream_key,
                self.config.consumer_group,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                return
            self._emit_backend_error("redis.xgroup_create", str(exc))
            raise BackendUnavailableError(
                "Redis Streams group initialization failed",
                action="redis.xgroup_create",
                queue=self.config.queue,
            ) from exc

    def _move_to_dead(self, message: Message) -> None:
        """Append a message to the dead-letter stream and ack the original."""

        self._ensure_group(self.dead_key)
        self._execute(
            "redis.xadd",
            self.redis.xadd,
            self.dead_key,
            {"payload": self._encode(message)},
        )
        self.ack(message)

    def _parse_read_response(self, response: list[Any]) -> list[Message]:
        """Parse an ``XREADGROUP`` response.

        Args:
            response: Redis-py stream response.

        Returns:
            Decoded messages.
        """

        messages: list[Message] = []
        for _stream, entries in response or []:
            for raw_id, fields in entries:
                messages.append(self._decode_stream_entry(raw_id, fields))
        return messages

    def _parse_autoclaim_response(self, response: Any) -> list[Message]:
        """Parse an ``XAUTOCLAIM`` response.

        Args:
            response: Redis-py autoclaim response.

        Returns:
            Decoded claimed messages.
        """

        if not response:
            return []
        entries = response[1] if len(response) > 1 else []
        return [
            self._decode_stream_entry(raw_id, fields)
            for raw_id, fields in entries
        ]

    def _decode_stream_entry(self, raw_id: Any, fields: dict[Any, Any]) -> Message:
        """Decode one Redis Stream entry into a ``Message``.

        Args:
            raw_id: Redis stream entry id, bytes or text.
            fields: Stream entry field mapping.

        Returns:
            Decoded message tagged with the raw stream id.

        Raises:
            BackendUnavailableError: If the entry has no ``payload`` field.
        """

        payload = fields.get("payload") or fields.get(b"payload")
        if payload is None:
            raise BackendUnavailableError(
                "stream entry is missing payload field",
                action="message.decode",
                queue=self.config.queue,
                details={"raw_id": self._to_text(raw_id)},
            )
        return self._decode(payload).with_backend(
            self.backend_name,
            raw_id=self._to_text(raw_id),
        )

    def _consumer_name(self) -> str:
        """Return the configured consumer name or the default name."""

        return self.config.consumer_name or "redqueue-consumer"

    @staticmethod
    def _pending_id(item: Any) -> str:
        """Extract a message id from an ``XPENDING`` entry."""

        if isinstance(item, dict):
            value = item.get("message_id") or item.get("message-id") or item.get("id")
            return value.decode() if isinstance(value, bytes) else str(value)
        value = item[0]
        return value.decode() if isinstance(value, bytes) else str(value)

    @staticmethod
    def _to_text(value: Any) -> str:
        """Normalize Redis bytes or text identifiers to ``str``."""

        return value.decode() if isinstance(value, bytes) else str(value)

    def _execute(self, action: str, func: Any, *args: Any) -> Any:
        """Execute a Redis Streams command and wrap failures.

        Args:
            action: Operation identifier.
            func: Redis command callable.
            *args: Arguments passed to ``func``.

        Returns:
            Redis command result.

        Raises:
            BackendUnavailableError: If the Redis command raises.
        """

        try:
            return func(*args)
        except Exception as exc:
            self._emit_backend_error(action, str(exc))
            raise BackendUnavailableError(
                "Redis Streams backend command failed",
                action=action,
                queue=self.config.queue,
            ) from exc
