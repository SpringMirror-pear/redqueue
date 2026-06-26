# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Asynchronous delayed task backend based on Redis Sorted Set."""

from __future__ import annotations

from time import time
from typing import Any, Protocol

from redqueue.config import QueueConfig
from redqueue.exceptions import BackendUnavailableError, QueueConfigError
from redqueue.message import Message, new_message_id
from redqueue.monitoring import MonitoringEvent, MonitoringEventType


class AsyncDelayRedis(Protocol):
    """Redis command subset required by the async delayed task backend."""

    async def set(self, name: str, value: bytes) -> bool: ...

    async def get(self, name: str) -> bytes | None: ...

    async def delete(self, *names: str) -> int: ...

    async def zadd(self, name: str, mapping: dict[str, float]) -> int: ...

    async def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> list[str | bytes]: ...

    async def zrem(self, name: str, *values: str) -> int: ...


class AsyncDelayBackend:
    """Async delayed task scheduler implemented with Redis Sorted Set.

    Attributes:
        redis: Async Redis client implementing delayed task commands.
        config: Queue configuration.
        publisher: Async backend that receives due messages.
    """

    backend_name = "delay"

    def __init__(
        self,
        redis: AsyncDelayRedis,
        config: QueueConfig,
        publisher: Any,
    ) -> None:
        """Initialize the async delay scheduler.

        Args:
            redis: Async Redis client implementing ``AsyncDelayRedis``.
            config: Queue configuration.
            publisher: Async backend exposing ``publish`` for released messages.
        """

        self.redis = redis
        self.config = config
        self.publisher = publisher

    @property
    def delayed_key(self) -> str:
        """Redis Sorted Set key containing delayed message ids."""

        return self.config.key("delayed")

    def payload_key(self, message_id: str) -> str:
        """Return the Redis key that stores a delayed message envelope.

        Args:
            message_id: RedQueue message id.

        Returns:
            Namespaced Redis string key.
        """

        return self.config.key(f"payload:{message_id}")

    async def delay(
        self,
        payload: Any,
        *,
        delay_seconds: float | None = None,
        run_at: float | None = None,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
        trace_id: str | None = None,
    ) -> str:
        """Schedule a payload for future async release.

        Args:
            payload: Application payload.
            delay_seconds: Relative delay in seconds.
            run_at: Absolute Unix timestamp when the message is due.
            headers: Optional message metadata.
            message_id: Optional stable message id.
            trace_id: Optional correlation id propagated with the message.

        Returns:
            Scheduled message id.
        """

        available_at = self._available_at(delay_seconds=delay_seconds, run_at=run_at)
        message = Message(
            id=message_id or new_message_id(),
            queue=self.config.queue,
            payload=payload,
            headers=headers or {},
            trace_id=trace_id,
            available_at=available_at,
            backend=self.backend_name,
        )
        payload_key = self.payload_key(message.id)
        await self._execute(
            "redis.set",
            self.redis.set,
            payload_key,
            self._encode(message),
        )
        try:
            await self._execute(
                "redis.zadd",
                self.redis.zadd,
                self.delayed_key,
                {message.id: available_at},
            )
        except Exception:
            await self._execute("redis.delete", self.redis.delete, payload_key)
            raise
        self._emit(MonitoringEventType.DELAY_SCHEDULED, message)
        return message.id

    async def schedule_due(self, *, limit: int = 100, now: float | None = None) -> int:
        """Release due delayed messages into the async publisher backend.

        Args:
            limit: Maximum number of due ids to scan.
            now: Optional Unix timestamp override.

        Returns:
            Number of released messages.
        """

        now_value = time() if now is None else now
        due_ids = await self._execute(
            "redis.zrangebyscore",
            self.redis.zrangebyscore,
            self.delayed_key,
            "-inf",
            now_value,
            0,
            limit,
        )
        released = 0
        for raw_message_id in due_ids:
            message_id = self._to_text(raw_message_id)
            removed = await self._execute(
                "redis.zrem",
                self.redis.zrem,
                self.delayed_key,
                message_id,
            )
            if removed < 1:
                continue
            message = await self._load_message(message_id)
            try:
                await self.publisher.publish(
                    message.payload,
                    headers=message.headers,
                    message_id=message.id,
                    trace_id=message.trace_id,
                )
            except Exception:
                await self._execute(
                    "redis.zadd",
                    self.redis.zadd,
                    self.delayed_key,
                    {message_id: message.available_at or now_value},
                )
                raise
            await self._execute(
                "redis.delete",
                self.redis.delete,
                self.payload_key(message_id),
            )
            self._emit(MonitoringEventType.DELAY_RELEASED, message)
            released += 1
        return released

    async def _load_message(self, message_id: str) -> Message:
        """Load a delayed message envelope by id.

        Args:
            message_id: RedQueue message id.

        Returns:
            Decoded delayed message.

        Raises:
            BackendUnavailableError: If the payload key is missing.
        """

        payload = await self._execute(
            "redis.get",
            self.redis.get,
            self.payload_key(message_id),
        )
        if payload is None:
            raise BackendUnavailableError(
                "delayed payload is missing",
                action="delay.load",
                queue=self.config.queue,
                details={"message_id": message_id},
            )
        return self._decode(payload)

    def _available_at(
        self,
        *,
        delay_seconds: float | None,
        run_at: float | None,
    ) -> float:
        """Calculate a delayed message availability timestamp.

        Args:
            delay_seconds: Relative delay in seconds.
            run_at: Absolute Unix timestamp.

        Returns:
            Unix timestamp when the message becomes due.

        Raises:
            QueueConfigError: If both values are set or either value is negative.
        """

        if delay_seconds is not None and run_at is not None:
            raise QueueConfigError("delay_seconds and run_at cannot both be set")
        if delay_seconds is not None:
            if delay_seconds < 0:
                raise QueueConfigError(
                    "delay_seconds must be greater than or equal to 0"
                )
            return time() + delay_seconds
        if run_at is not None:
            if run_at < 0:
                raise QueueConfigError("run_at must be greater than or equal to 0")
            return run_at
        return time()

    def _encode(self, message: Message) -> bytes:
        """Encode a delayed message envelope.

        Args:
            message: Message to encode.

        Returns:
            Serialized envelope bytes.
        """

        envelope = {
            "id": message.id,
            "queue": message.queue,
            "payload": message.payload,
            "headers": message.headers,
            "trace_id": message.trace_id,
            "attempts": message.attempts,
            "created_at": message.created_at,
            "available_at": message.available_at,
            "backend": self.backend_name,
            "raw_id": message.raw_id,
        }
        return self.config.serializer.encode(envelope, queue=self.config.queue)

    def _decode(self, payload: bytes) -> Message:
        """Decode a delayed message envelope.

        Args:
            payload: Serialized envelope bytes.

        Returns:
            Decoded delayed message.

        Raises:
            BackendUnavailableError: If the decoded envelope is not a mapping.
        """

        envelope = self.config.serializer.decode(payload, queue=self.config.queue)
        if not isinstance(envelope, dict):
            raise BackendUnavailableError(
                "decoded delayed message envelope must be a mapping",
                action="delay.decode",
                queue=self.config.queue,
                details={"payload_type": type(envelope).__name__},
            )
        return Message(
            id=str(envelope["id"]),
            queue=str(envelope["queue"]),
            payload=envelope["payload"],
            headers=dict(envelope.get("headers") or {}),
            trace_id=envelope.get("trace_id"),
            attempts=int(envelope.get("attempts") or 0),
            created_at=float(envelope["created_at"]),
            available_at=envelope.get("available_at"),
            backend=self.backend_name,
            raw_id=envelope.get("raw_id"),
            raw_payload=payload,
        )

    async def _execute(self, action: str, func: Any, *args: Any) -> Any:
        """Execute an async Redis delayed-task command and wrap failures.

        Args:
            action: Operation identifier.
            func: Async Redis command callable.
            *args: Arguments passed to ``func``.

        Returns:
            Redis command result.

        Raises:
            BackendUnavailableError: If the Redis command raises.
        """

        try:
            return await func(*args)
        except Exception as exc:
            self.config.monitoring.emit(
                MonitoringEvent(
                    type=MonitoringEventType.BACKEND_ERROR,
                    queue=self.config.queue,
                    backend=self.backend_name,
                    error=str(exc),
                    attributes={"action": action},
                )
            )
            raise BackendUnavailableError(
                "Redis async delayed task backend command failed",
                action=action,
                queue=self.config.queue,
            ) from exc

    def _emit(self, event_type: MonitoringEventType, message: Message) -> None:
        """Emit a delay monitoring event.

        Args:
            event_type: Delay event type.
            message: Message related to the event.
        """

        self.config.monitoring.emit(
            MonitoringEvent(
                type=event_type,
                queue=self.config.queue,
                message_id=message.id,
                trace_id=message.trace_id,
                backend=self.backend_name,
            )
        )

    @staticmethod
    def _to_text(value: str | bytes) -> str:
        """Normalize Redis bytes or text to ``str``."""

        return value.decode() if isinstance(value, bytes) else value
