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
from redqueue.monitoring import MonitoringEvent, MonitoringEventType


class AsyncQueueClient:
    """Asynchronous facade for RedQueue operations.

    The async client mirrors ``QueueClient`` while deferring backend creation
    until ``from_url`` or the first operation. It is intended for applications
    using ``redis.asyncio`` or compatible async Redis clients.

    Attributes:
        config: Normalized queue configuration.
        redis: Async Redis client or protocol-compatible fake.
        capabilities: Redis feature set detected from the server or injected by
            tests.
        backend: Lazily-created async List or Streams backend.
        delay_backend: Lazily-created async Sorted Set delay scheduler.
    """

    def __init__(
        self,
        config: QueueConfig,
        *,
        redis: Any | None = None,
        capabilities: RedisCapabilities | None = None,
    ) -> None:
        """Initialize an async client container.

        Args:
            config: Queue configuration shared by all async operations.
            redis: Async Redis client. Required before any backend operation.
            capabilities: Optional Redis capability set. ``from_url`` detects
                this automatically.
        """

        self.config = config
        self.redis = redis
        self.capabilities = capabilities
        self.backend: AsyncListBackend | AsyncStreamBackend | None = None
        self.delay_backend: AsyncDelayBackend | None = None
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.CLIENT_CREATED,
                queue=config.queue,
                backend=config.backend_type.value,
            )
        )

    @classmethod
    async def from_url(
        cls,
        url: str,
        *,
        queue: str,
        backend: str | BackendType = BackendType.LIST,
        **options: Any,
    ) -> AsyncQueueClient:
        """Create and initialize an async client from a Redis URL.

        Args:
            url: Redis connection URL accepted by ``redis.asyncio.Redis``.
            queue: Logical RedQueue queue name.
            backend: Backend name or ``BackendType`` value.
            **options: Additional ``QueueConfig`` options. Tests may also pass
                ``redis`` or ``capabilities``.

        Returns:
            An initialized ``AsyncQueueClient`` with its primary backend ready.

        Raises:
            BackendUnavailableError: If Redis ``INFO server`` cannot be read.
            RedisCompatibilityError: If the selected backend is not supported.
            QueueConfigError: If configuration values are invalid.
        """

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
        """Publish a message immediately or schedule it for later.

        Args:
            payload: Application payload to serialize.
            delay: Optional relative delay in seconds.
            headers: Optional metadata stored with the message.
            message_id: Optional stable message id.

        Returns:
            The RedQueue message id.
        """

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
        """Consume one or more messages asynchronously.

        Args:
            timeout: Backend-specific blocking timeout in seconds.
            batch_size: Maximum number of messages to return.

        Returns:
            ``None``, a single ``Message``, or a list of messages depending on
            availability and ``batch_size``.
        """

        backend = await self._ensure_backend()
        return await backend.consume(
            timeout=timeout,
            batch_size=batch_size,
        )

    async def ack(self, message: Message) -> None:
        """Acknowledge successful async message processing.

        Args:
            message: Message returned by ``consume``.
        """

        await (await self._ensure_backend()).ack(message)

    async def nack(self, message: Message, *, requeue: bool = True) -> None:
        """Reject a message and optionally requeue it.

        Args:
            message: Message returned by ``consume``.
            requeue: When true, return the message to the ready path. When
                false, move it to dead letters.
        """

        await (await self._ensure_backend()).nack(message, requeue=requeue)

    async def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        """Retry a message according to ``RetryConfig``.

        Args:
            message: Message returned by ``consume``.
            delay: Reserved delay hint for future delayed retry strategies.
            reason: Optional diagnostic reason emitted in monitoring events.
        """

        await (await self._ensure_backend()).retry(message, delay=delay, reason=reason)

    async def delay(
        self,
        payload: Any,
        *,
        delay_seconds: float | None = None,
        run_at: float | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        """Schedule a payload for future async delivery.

        Args:
            payload: Application payload to deliver later.
            delay_seconds: Relative delay in seconds.
            run_at: Absolute Unix timestamp when the message becomes due.
            headers: Optional metadata stored with the message.

        Returns:
            The scheduled message id.
        """

        delay_backend = await self._ensure_delay_backend()
        message_id = await delay_backend.delay(
            payload,
            delay_seconds=delay_seconds,
            run_at=run_at,
            headers=headers,
        )
        return message_id

    async def schedule_due(self, *, limit: int = 100, now: float | None = None) -> int:
        """Move due delayed messages into the active backend.

        Args:
            limit: Maximum number of due messages to release.
            now: Optional Unix timestamp override.

        Returns:
            Number of released messages.
        """

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
        """Recover stale async processing state.

        Args:
            min_idle_ms: Minimum idle time for Streams pending recovery.
            limit: Maximum number of messages to recover.

        Returns:
            Number of recovered messages.
        """

        backend = await self._ensure_backend()
        if isinstance(backend, AsyncStreamBackend):
            idle = min_idle_ms or int(self.config.visibility_timeout_seconds * 1000)
            return len(await backend.recover_pending(min_idle_ms=idle, limit=limit))
        return await backend.recover_stale(limit=limit)

    async def dead_letters(self, *, limit: int = 100) -> list[Message]:
        """Read dead-lettered messages from the selected backend.

        Args:
            limit: Maximum number of dead letters to return.

        Returns:
            Dead-letter messages decoded from Redis.
        """

        return await (await self._ensure_backend()).dead_letters(limit=limit)

    async def requeue_dead(self, message: Message) -> None:
        """Move a dead-lettered message back to the ready path.

        Args:
            message: Message previously returned by ``dead_letters``.
        """

        await (await self._ensure_backend()).requeue_dead(message)

    async def close(self) -> None:
        """Close the underlying async Redis client if it exposes close methods."""

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
        """Return the initialized primary backend, creating it when needed.

        Returns:
            Async List or Streams backend.

        Raises:
            TypeError: If Redis or capabilities are missing.
            RedisCompatibilityError: If Redis lacks required backend commands.
        """

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
        """Return the initialized async delay scheduler, creating it when needed."""

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
