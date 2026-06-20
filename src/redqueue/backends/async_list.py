# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Asynchronous Redis List backend."""

from __future__ import annotations

from typing import Any, Protocol

from redqueue.backends.base import BaseListBackend
from redqueue.compat import RedisCapabilities, RedisVersion
from redqueue.config import QueueConfig
from redqueue.exceptions import AckError, BackendUnavailableError, RetryExceededError
from redqueue.message import Message, new_message_id
from redqueue.monitoring import MonitoringEventType


class AsyncListRedis(Protocol):
    """Redis command subset required by the asynchronous List backend.

    Implemented by ``redis.asyncio.Redis`` and by async test fakes.
    """

    async def lpush(self, name: str, *values: bytes) -> int: ...

    async def lrem(self, name: str, count: int, value: bytes) -> int: ...

    async def lrange(self, name: str, start: int, end: int) -> list[bytes]: ...

    async def blmove(
        self,
        first_list: str,
        second_list: str,
        timeout: float,
        src: str = "RIGHT",
        dest: str = "LEFT",
    ) -> bytes | None: ...

    async def brpoplpush(
        self,
        src: str,
        dst: str,
        timeout: float,
    ) -> bytes | None: ...


class AsyncListBackend(BaseListBackend):
    """Reliable async queue backend implemented with Redis List commands.

    This backend mirrors ``ListBackend`` while awaiting Redis calls.

    Attributes:
        redis: Async Redis client implementing ``AsyncListRedis``.
        capabilities: Redis command capability set.
    """

    def __init__(
        self,
        redis: AsyncListRedis,
        config: QueueConfig,
        capabilities: RedisCapabilities,
    ) -> None:
        """Initialize an asynchronous List backend.

        Args:
            redis: Async Redis client implementing required List commands.
            config: Queue configuration.
            capabilities: Detected Redis capabilities.

        Raises:
            RedisCompatibilityError: If reliable List commands are unavailable.
        """

        capabilities.require_list_reliable()
        super().__init__(config)
        self.redis = redis
        self.capabilities = capabilities

    @classmethod
    def for_modern_redis(
        cls,
        redis: AsyncListRedis,
        config: QueueConfig,
    ) -> AsyncListBackend:
        """Create a backend for tests or callers that already know Redis is modern.

        Args:
            redis: Async Redis client implementing required List commands.
            config: Queue configuration.

        Returns:
            ``AsyncListBackend`` using Redis 7 capability assumptions.
        """

        return cls(redis, config, RedisCapabilities(RedisVersion(7, 0, 0)))

    async def publish(
        self,
        payload: Any,
        *,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> str:
        """Publish a payload to the ready list.

        Args:
            payload: Application payload.
            headers: Optional message metadata.
            message_id: Optional stable message id.

        Returns:
            Message id.
        """

        message = Message(
            id=message_id or new_message_id(),
            queue=self.config.queue,
            payload=payload,
            headers=headers or {},
            backend=self.backend_name,
        )
        await self._execute(
            "redis.lpush",
            self.redis.lpush,
            self.ready_key,
            self._encode(message),
        )
        self._emit(
            MonitoringEventType.MESSAGE_PUBLISHED,
            message,
            attributes={"key": self.ready_key},
        )
        return message.id

    async def consume(
        self,
        *,
        timeout: float | None = None,
        batch_size: int = 1,
    ) -> Message | list[Message] | None:
        """Consume one or more messages asynchronously.

        Args:
            timeout: Blocking timeout in seconds.
            batch_size: Maximum number of messages to consume.

        Returns:
            ``None``, one ``Message``, or a list of messages.
        """

        timeout_value = timeout or 0
        if batch_size <= 1:
            payload = await self._move_to_processing(timeout_value)
            if payload is None:
                return None
            message = self._decode(payload)
            self._emit(MonitoringEventType.MESSAGE_CONSUMED, message)
            return message

        messages: list[Message] = []
        for _ in range(batch_size):
            payload = await self._move_to_processing(timeout_value)
            if payload is None:
                break
            message = self._decode(payload)
            self._emit(MonitoringEventType.MESSAGE_CONSUMED, message)
            messages.append(message)
        return messages

    async def ack(self, message: Message) -> None:
        """Remove a processed message from the processing list.

        Args:
            message: Message previously returned by ``consume``.

        Raises:
            AckError: If the encoded message is not found in processing.
        """

        removed = await self._execute(
            "redis.lrem",
            self.redis.lrem,
            self.processing_key,
            1,
            self._encode(message),
        )
        if removed < 1:
            raise AckError(
                "message was not found in processing queue",
                action="message.ack",
                queue=self.config.queue,
                details={"message_id": message.id, "key": self.processing_key},
            )
        self._emit(MonitoringEventType.MESSAGE_ACKED, message)

    async def nack(self, message: Message, *, requeue: bool = True) -> None:
        """Reject a message and move it to ready or dead letters.

        Args:
            message: Message previously returned by ``consume``.
            requeue: When true, return to ready; otherwise move to dead letters.

        Raises:
            AckError: If the encoded message is not found in processing.
        """

        encoded = self._encode(message)
        removed = await self._execute(
            "redis.lrem",
            self.redis.lrem,
            self.processing_key,
            1,
            encoded,
        )
        if removed < 1:
            raise AckError(
                "message was not found in processing queue",
                action="message.nack",
                queue=self.config.queue,
                details={"message_id": message.id, "key": self.processing_key},
            )
        target_key = self.ready_key if requeue else self.dead_key
        await self._execute("redis.lpush", self.redis.lpush, target_key, encoded)
        self._emit(
            MonitoringEventType.MESSAGE_NACKED,
            message,
            attributes={"requeue": requeue, "target_key": target_key},
        )

    async def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        """Retry a message or dead-letter it when retries are exhausted.

        Args:
            message: Message previously returned by ``consume``.
            delay: Reserved delay hint for future retry scheduling.
            reason: Optional diagnostic reason.

        Raises:
            AckError: If the message is missing from processing.
            RetryExceededError: If ``max_retries`` has been reached.
        """

        if message.attempts >= self.config.retry.max_retries:
            await self.nack(message, requeue=False)
            self._emit(
                MonitoringEventType.MESSAGE_DEAD_LETTERED,
                message,
                attributes={"reason": reason, "attempts": message.attempts},
            )
            raise RetryExceededError(
                "message exceeded max retries and was moved to dead letter queue",
                action="message.retry",
                queue=self.config.queue,
                details={
                    "message_id": message.id,
                    "attempts": message.attempts,
                    "max_retries": self.config.retry.max_retries,
                },
            )

        retried = message.with_attempt()
        removed = await self._execute(
            "redis.lrem",
            self.redis.lrem,
            self.processing_key,
            1,
            self._encode(message),
        )
        if removed < 1:
            raise AckError(
                "message was not found in processing queue",
                action="message.retry",
                queue=self.config.queue,
                details={"message_id": message.id, "key": self.processing_key},
            )
        await self._execute(
            "redis.lpush",
            self.redis.lpush,
            self.ready_key,
            self._encode(retried),
        )
        self._emit(
            MonitoringEventType.MESSAGE_RETRIED,
            retried,
            attributes={"delay": delay, "reason": reason},
        )

    async def recover_stale(self, *, limit: int = 100) -> int:
        """Move messages from processing back to ready.

        Args:
            limit: Maximum number of processing entries to recover.

        Returns:
            Number of messages requeued.
        """

        recovered = 0
        entries = await self._execute(
            "redis.lrange",
            self.redis.lrange,
            self.processing_key,
            0,
            max(limit - 1, 0),
        )
        for payload in entries:
            message = self._decode(payload)
            removed = await self._execute(
                "redis.lrem",
                self.redis.lrem,
                self.processing_key,
                1,
                payload,
            )
            if removed < 1:
                continue
            await self._execute(
                "redis.lpush",
                self.redis.lpush,
                self.ready_key,
                payload,
            )
            self._emit(
                MonitoringEventType.MESSAGE_RETRIED,
                message,
                attributes={"reason": "stale_processing_recovered"},
            )
            recovered += 1
        return recovered

    async def dead_letters(self, *, limit: int = 100) -> list[Message]:
        """Read messages from the dead-letter list.

        Args:
            limit: Maximum number of dead letters to return.

        Returns:
            Decoded dead-letter messages.
        """

        entries = await self._execute(
            "redis.lrange",
            self.redis.lrange,
            self.dead_key,
            0,
            max(limit - 1, 0),
        )
        return [self._decode(payload) for payload in entries]

    async def requeue_dead(self, message: Message) -> None:
        """Move a dead-lettered message back to ready.

        Args:
            message: Message returned by ``dead_letters``.

        Raises:
            AckError: If the message is not present in the dead-letter list.
        """

        encoded = self._encode(message)
        removed = await self._execute(
            "redis.lrem",
            self.redis.lrem,
            self.dead_key,
            1,
            encoded,
        )
        if removed < 1:
            raise AckError(
                "message was not found in dead letter queue",
                action="message.requeue_dead",
                queue=self.config.queue,
                details={"message_id": message.id, "key": self.dead_key},
            )
        await self._execute("redis.lpush", self.redis.lpush, self.ready_key, encoded)
        self._emit(
            MonitoringEventType.MESSAGE_RETRIED,
            message,
            attributes={"reason": "dead_letter_requeued"},
        )

    async def _move_to_processing(self, timeout: float) -> bytes | None:
        """Atomically move one message from ready to processing.

        Args:
            timeout: Blocking timeout in seconds.

        Returns:
            Serialized payload or ``None`` when no message is available.
        """

        if self.capabilities.supports_list_reliable_blmove:
            return await self._execute(
                "redis.blmove",
                self.redis.blmove,
                self.ready_key,
                self.processing_key,
                timeout,
                "RIGHT",
                "LEFT",
            )
        return await self._execute(
            "redis.brpoplpush",
            self.redis.brpoplpush,
            self.ready_key,
            self.processing_key,
            timeout,
        )

    async def _execute(self, action: str, func: Any, *args: Any) -> Any:
        """Execute an async Redis command and wrap failures consistently.

        Args:
            action: Operation identifier for monitoring and errors.
            func: Async Redis command callable.
            *args: Positional arguments passed to ``func``.

        Returns:
            Redis command result.

        Raises:
            BackendUnavailableError: If the Redis command raises.
        """

        try:
            return await func(*args)
        except Exception as exc:
            self._emit_backend_error(action, str(exc))
            raise BackendUnavailableError(
                "Redis async List backend command failed",
                action=action,
                queue=self.config.queue,
            ) from exc
