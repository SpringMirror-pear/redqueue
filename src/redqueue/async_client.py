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
from redqueue.connection import AsyncRedisConnectionManager
from redqueue.deduplication import AsyncDeduplicationBackend
from redqueue.message import Message, new_message_id
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
        owns_redis: bool = True,
    ) -> None:
        """Initialize an async client container.

        Args:
            config: Queue configuration shared by all async operations.
            redis: Async Redis client. Required before any backend operation.
            capabilities: Optional Redis capability set. ``from_url`` detects
                this automatically.
            owns_redis: When true, ``close`` closes the async Redis client.
        """

        self.config = config
        self.redis = redis
        self.capabilities = capabilities
        self._owns_redis = owns_redis
        self._closed = False
        self.backend: AsyncListBackend | AsyncStreamBackend | None = None
        self.delay_backend: AsyncDelayBackend | None = None
        self.deduplication_backend: AsyncDeduplicationBackend | None = None
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
        connection_manager: AsyncRedisConnectionManager | None = None,
        **options: Any,
    ) -> AsyncQueueClient:
        """Create and initialize an async client from a Redis URL.

        Args:
            url: Redis connection URL accepted by ``redis.asyncio.Redis``.
            queue: Logical RedQueue queue name.
            backend: Backend name or ``BackendType`` value.
            connection_manager: Optional async connection manager used to create
                a client from a shared pool.
            **options: Additional ``QueueConfig`` options. Tests may also pass
                ``redis``, ``capabilities``, ``pool_options``, or
                ``owns_redis``.

        Returns:
            An initialized ``AsyncQueueClient`` with its primary backend ready.

        Raises:
            BackendUnavailableError: If Redis ``INFO server`` cannot be read.
            RedisCompatibilityError: If the selected backend is not supported.
            QueueConfigError: If configuration values are invalid.
        """

        redis = options.pop("redis", None)
        pool_options = options.pop("pool_options", None) or {}
        explicit_owns_redis = options.pop("owns_redis", None)
        owns_redis = (
            bool(explicit_owns_redis)
            if explicit_owns_redis is not None
            else False
        )
        if redis is None:
            if connection_manager is not None:
                redis = connection_manager.redis()
            else:
                redis = Redis.from_url(url, **pool_options)
                owns_redis = (
                    True
                    if explicit_owns_redis is None
                    else bool(explicit_owns_redis)
                )
        capabilities = options.pop("capabilities", None)
        if capabilities is None:
            try:
                capabilities = await detect_capabilities_async(
                    cast(AsyncRedisInfoClient, redis)
                )
            except Exception:
                if owns_redis:
                    await cls._close_redis(redis)
                raise
        try:
            config = QueueConfig(queue=queue, backend=backend, **options)
        except Exception:
            if owns_redis:
                await cls._close_redis(redis)
            raise
        client = cls(
            config=config,
            redis=redis,
            capabilities=capabilities,
            owns_redis=owns_redis,
        )
        await client._ensure_backend()
        return client

    async def publish(
        self,
        payload: Any,
        *,
        delay: float | None = None,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
        trace_id: str | None = None,
        dedup_key: str | None = None,
    ) -> str:
        """Publish a message immediately or schedule it for later.

        Args:
            payload: Application payload to serialize.
            delay: Optional relative delay in seconds.
            headers: Optional metadata stored with the message.
            message_id: Optional stable message id.
            trace_id: Optional correlation id propagated with message lifecycle
                events.
            dedup_key: Optional deduplication key used when deduplication is
                enabled.

        Returns:
            The RedQueue message id.
        """

        reserved_id = message_id or new_message_id()
        if delay is not None:
            return await self._publish_with_deduplication(
                dedup_key,
                reserved_id,
                trace_id=trace_id,
                publish=lambda msg_id: self.delay(
                    payload,
                    delay_seconds=delay,
                    headers=headers,
                    message_id=msg_id,
                    trace_id=trace_id,
                    dedup_key=None,
                ),
            )
        backend = await self._ensure_backend()
        return await self._publish_with_deduplication(
            dedup_key,
            reserved_id,
            trace_id=trace_id,
            publish=lambda msg_id: backend.publish(
                payload,
                headers=headers,
                message_id=msg_id,
                trace_id=trace_id,
            ),
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
        message_id: str | None = None,
        trace_id: str | None = None,
        dedup_key: str | None = None,
    ) -> str:
        """Schedule a payload for future async delivery.

        Args:
            payload: Application payload to deliver later.
            delay_seconds: Relative delay in seconds.
            run_at: Absolute Unix timestamp when the message becomes due.
            headers: Optional metadata stored with the message.
            message_id: Optional stable message id. When omitted, RedQueue
                generates one.
            trace_id: Optional correlation id propagated when the delayed
                message is released.
            dedup_key: Optional deduplication key used when deduplication is
                enabled.

        Returns:
            The scheduled message id.
        """

        delay_backend = await self._ensure_delay_backend()
        reserved_id = message_id or new_message_id()
        message_id = await self._publish_with_deduplication(
            dedup_key,
            reserved_id,
            trace_id=trace_id,
            publish=lambda msg_id: delay_backend.delay(
                payload,
                delay_seconds=delay_seconds,
                run_at=run_at,
                headers=headers,
                message_id=msg_id,
                trace_id=trace_id,
            ),
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
        """Close the async Redis client when this client owns it."""

        if self._closed or not self._owns_redis:
            return

        close = getattr(self.redis, "aclose", None) or getattr(
            self.redis,
            "close",
            None,
        )
        if close is not None:
            await self._call_close(close)
        self._closed = True

    async def __aenter__(self) -> AsyncQueueClient:
        """Enter an asynchronous resource-management context."""

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        """Close owned async resources when leaving an async context manager."""

        await self.close()

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
            try:
                self.backend = AsyncListBackend(self.redis, self.config, capabilities)
                return self.backend
            except Exception:
                await self.close()
                raise
        if self.config.backend_type is BackendType.STREAM:
            if self.redis is None:
                raise TypeError("redis client is required for async Streams backend")
            capabilities = self.capabilities
            if capabilities is None:
                raise TypeError("Redis capabilities are required before backend use")
            try:
                self.backend = await AsyncStreamBackend.create(
                    self.redis,
                    self.config,
                    capabilities,
                )
                return self.backend
            except Exception:
                await self.close()
                raise
        raise NotImplementedError(
            f"backend {self.config.backend_type.value!r} is not implemented"
        )

    @staticmethod
    async def _close_redis(redis: Any) -> None:
        """Close an async Redis-like object if it exposes a close method."""

        close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
        if close is not None:
            await AsyncQueueClient._call_close(close)

    @staticmethod
    async def _call_close(close: Any) -> None:
        """Call a sync or async close method."""

        result = close()
        if hasattr(result, "__await__"):
            await result

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

    async def _ensure_deduplication_backend(self) -> AsyncDeduplicationBackend:
        """Return the initialized async deduplication helper."""

        if self.deduplication_backend is not None:
            return self.deduplication_backend
        if self.redis is None:
            raise TypeError("redis client is required for async deduplication")
        self.deduplication_backend = AsyncDeduplicationBackend(
            self.redis,
            self.config,
        )
        return self.deduplication_backend

    async def _publish_with_deduplication(
        self,
        dedup_key: str | None,
        message_id: str,
        *,
        trace_id: str | None,
        publish: Any,
    ) -> str:
        """Run an async publish operation through optional deduplication."""

        if not self.config.deduplication.enabled or dedup_key is None:
            return await publish(message_id)
        deduplication = await self._ensure_deduplication_backend()
        result = await deduplication.reserve_or_get(
            dedup_key,
            message_id,
            trace_id=trace_id,
        )
        if result.duplicate:
            return result.message_id
        try:
            return await publish(result.message_id)
        except Exception:
            await deduplication.rollback(result.dedup_key)
            raise
