# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Asynchronous RedQueue client."""

from __future__ import annotations

from typing import Any, cast

from redis.asyncio import Redis

from redqueue.backends import AsyncDelayBackend, AsyncListBackend, AsyncStreamBackend
from redqueue.compat import (
    AsyncRedisInfoClient,
    RedisCapabilities,
    detect_capabilities_async,
)
from redqueue.config import BackendType, QueueConfig
from redqueue.message import Message


class AsyncQueueClient:
    """Asynchronous queue client with API parity to QueueClient."""

    def __init__(
        self,
        config: QueueConfig,
        *,
        redis: Any | None = None,
        capabilities: RedisCapabilities | None = None,
    ) -> None:
        self.config = config
        self.redis = redis
        self.capabilities = capabilities
        self.backend: AsyncListBackend | AsyncStreamBackend | None = None
        self.delay_backend: AsyncDelayBackend | None = None

    @classmethod
    async def from_url(
        cls,
        url: str,
        *,
        queue: str,
        backend: str | BackendType = BackendType.LIST,
        **options: Any,
    ) -> AsyncQueueClient:
        redis = options.pop("redis", None) or Redis.from_url(url)
        capabilities = options.pop(
            "capabilities",
            None,
        ) or await detect_capabilities_async(cast(AsyncRedisInfoClient, redis))
        config = QueueConfig(queue=queue, backend=backend, **options)
        client = cls(config=config, redis=redis, capabilities=capabilities)
        await client._ensure_backend()
        return client

    async def publish(
        self,
        payload: Any,
        *,
        delay: float | None = None,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> str:
        if delay is not None:
            return await self.delay(payload, delay_seconds=delay, headers=headers)
        backend = await self._ensure_backend()
        return await backend.publish(
            payload,
            headers=headers,
            message_id=message_id,
        )

    async def consume(
        self,
        *,
        timeout: float | None = None,
        batch_size: int = 1,
    ) -> Message | list[Message] | None:
        backend = await self._ensure_backend()
        return await backend.consume(
            timeout=timeout,
            batch_size=batch_size,
        )

    async def ack(self, message: Message) -> None:
        await (await self._ensure_backend()).ack(message)

    async def nack(self, message: Message, *, requeue: bool = True) -> None:
        await (await self._ensure_backend()).nack(message, requeue=requeue)

    async def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        await (await self._ensure_backend()).retry(message, delay=delay, reason=reason)

    async def delay(
        self,
        payload: Any,
        *,
        delay_seconds: float | None = None,
        run_at: float | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        delay_backend = await self._ensure_delay_backend()
        message_id = await delay_backend.delay(
            payload,
            delay_seconds=delay_seconds,
            run_at=run_at,
            headers=headers,
        )
        return message_id

    async def schedule_due(self, *, limit: int = 100, now: float | None = None) -> int:
        return await (await self._ensure_delay_backend()).schedule_due(
            limit=limit,
            now=now,
        )

    async def recover_stale(
        self,
        *,
        min_idle_ms: int | None = None,
        limit: int = 100,
    ) -> int:
        backend = await self._ensure_backend()
        if isinstance(backend, AsyncStreamBackend):
            idle = min_idle_ms or int(self.config.visibility_timeout_seconds * 1000)
            return len(await backend.recover_pending(min_idle_ms=idle, limit=limit))
        return await backend.recover_stale(limit=limit)

    async def dead_letters(self, *, limit: int = 100) -> list[Message]:
        return await (await self._ensure_backend()).dead_letters(limit=limit)

    async def requeue_dead(self, message: Message) -> None:
        await (await self._ensure_backend()).requeue_dead(message)

    async def close(self) -> None:
        close = getattr(self.redis, "aclose", None) or getattr(
            self.redis,
            "close",
            None,
        )
        if close is not None:
            result = close()
            if hasattr(result, "__await__"):
                await result

    async def _ensure_backend(self) -> AsyncListBackend | AsyncStreamBackend:
        if self.backend is not None:
            return self.backend
        if self.config.backend_type is BackendType.LIST:
            if self.redis is None:
                raise TypeError("redis client is required for async List backend")
            capabilities = self.capabilities
            if capabilities is None:
                raise TypeError("Redis capabilities are required before backend use")
            self.backend = AsyncListBackend(self.redis, self.config, capabilities)
            return self.backend
        if self.config.backend_type is BackendType.STREAM:
            if self.redis is None:
                raise TypeError("redis client is required for async Streams backend")
            capabilities = self.capabilities
            if capabilities is None:
                raise TypeError("Redis capabilities are required before backend use")
            self.backend = await AsyncStreamBackend.create(
                self.redis,
                self.config,
                capabilities,
            )
            return self.backend
        raise NotImplementedError(
            f"backend {self.config.backend_type.value!r} is not implemented"
        )

    async def _ensure_delay_backend(self) -> AsyncDelayBackend:
        if self.delay_backend is not None:
            return self.delay_backend
        if self.redis is None:
            raise TypeError("redis client is required for async delayed tasks")
        capabilities = self.capabilities
        if capabilities is None:
            raise TypeError("Redis capabilities are required before delay backend use")
        capabilities.require_delay_sorted_set()
        self.delay_backend = AsyncDelayBackend(
            self.redis,
            self.config,
            await self._ensure_backend(),
        )
        return self.delay_backend
