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
    """Redis command subset required by the asynchronous List backend."""

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
    """Reliable async queue backend implemented with Redis List commands."""

    def __init__(
        self,
        redis: AsyncListRedis,
        config: QueueConfig,
        capabilities: RedisCapabilities,
    ) -> None:
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
        """Create a backend for tests or callers that already know Redis is modern."""

        return cls(redis, config, RedisCapabilities(RedisVersion(7, 0, 0)))

    async def publish(
        self,
        payload: Any,
        *,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> str:
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
        entries = await self._execute(
            "redis.lrange",
            self.redis.lrange,
            self.dead_key,
            0,
            max(limit - 1, 0),
        )
        return [self._decode(payload) for payload in entries]

    async def requeue_dead(self, message: Message) -> None:
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
        try:
            return await func(*args)
        except Exception as exc:
            self._emit_backend_error(action, str(exc))
            raise BackendUnavailableError(
                "Redis async List backend command failed",
                action=action,
                queue=self.config.queue,
            ) from exc
