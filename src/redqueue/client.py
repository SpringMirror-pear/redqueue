# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Synchronous RedQueue client."""

from __future__ import annotations

from typing import Any, cast

from redis import Redis

from redqueue.backends import DelayBackend, ListBackend, StreamBackend
from redqueue.compat import RedisCapabilities, RedisInfoClient, detect_capabilities
from redqueue.config import BackendType, QueueConfig
from redqueue.connection import RedisConnectionManager
from redqueue.message import Message
from redqueue.monitoring import MonitoringEvent, MonitoringEventType


class QueueClient:
    """Synchronous facade for RedQueue operations.

    The client owns backend selection, Redis capability validation, delay
    scheduling, and monitoring events for synchronous applications. Callers can
    use either the List backend or the Streams backend through the same public
    methods.

    Attributes:
        config: Normalized queue configuration.
        redis: Redis client or Redis-compatible object used by backends.
        capabilities: Redis feature set detected from the server or provided by
            tests.
        backend: Concrete List or Streams backend selected from ``config``.
        delay_backend: Sorted Set scheduler used by ``delay`` and
            ``schedule_due``.
    """

    def __init__(
        self,
        config: QueueConfig,
        *,
        redis: Any | None = None,
        capabilities: RedisCapabilities | None = None,
        owns_redis: bool = True,
    ) -> None:
        """Initialize the synchronous client and selected backend.

        Args:
            config: Queue configuration that controls queue name, backend,
                retry policy, serializer, and monitoring hook.
            redis: Redis client or protocol-compatible fake. A client is
                required because backends are created eagerly.
            capabilities: Optional pre-detected Redis capabilities. When omitted,
                capabilities are read from Redis during backend creation.
            owns_redis: When true, ``close`` closes the Redis client. Direct
                construction defaults to true; ``from_url`` uses false for
                injected Redis clients and connection-manager clients.

        Raises:
            TypeError: If no Redis client is available for the selected backend.
            RedisCompatibilityError: If Redis lacks a required command family.
        """

        self.config = config
        self.redis = redis
        self.capabilities = capabilities
        self._owns_redis = owns_redis
        self.backend = self._create_backend()
        self.delay_backend = self._create_delay_backend()
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.CLIENT_CREATED,
                queue=config.queue,
                backend=config.backend_type.value,
            )
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        queue: str,
        backend: str | BackendType = BackendType.LIST,
        connection_manager: RedisConnectionManager | None = None,
        **options: Any,
    ) -> QueueClient:
        """Create a client from a Redis URL.

        Args:
            url: Redis connection URL accepted by ``redis.Redis.from_url``.
            queue: Logical RedQueue queue name.
            backend: Backend name or ``BackendType`` value.
            connection_manager: Optional connection manager used to create a
                client from a shared pool.
            **options: Additional ``QueueConfig`` options. Tests may also pass
                ``redis``, ``capabilities``, or ``pool_options`` to bypass or
                customize connection creation.

        Returns:
            A ready-to-use synchronous ``QueueClient``.

        Raises:
            BackendUnavailableError: If Redis ``INFO server`` cannot be read.
            RedisCompatibilityError: If the selected backend is unsupported by
                the connected Redis server.
            QueueConfigError: If configuration values are invalid.
        """

        redis = options.pop("redis", None)
        pool_options = options.pop("pool_options", None) or {}
        owns_redis = False
        if redis is None:
            if connection_manager is not None:
                redis = connection_manager.redis()
            else:
                redis = Redis.from_url(url, **pool_options)
                owns_redis = True
        capabilities = options.pop("capabilities", None) or detect_capabilities(
            cast(RedisInfoClient, redis)
        )
        config = QueueConfig(queue=queue, backend=backend, **options)
        return cls(
            config=config,
            redis=redis,
            capabilities=capabilities,
            owns_redis=owns_redis,
        )

    def publish(
        self,
        payload: Any,
        *,
        delay: float | None = None,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> str:
        """Publish a message immediately or schedule it for later.

        Args:
            payload: Application payload to serialize into the RedQueue envelope.
            delay: Optional relative delay in seconds. When set, the message is
                written to the delayed task store instead of the active backend.
            headers: Optional metadata stored with the message.
            message_id: Optional stable message id. When omitted, RedQueue
                generates one.

        Returns:
            The RedQueue message id.
        """

        if delay is not None:
            return self.delay(payload, delay_seconds=delay, headers=headers)
        return self.backend.publish(
            payload,
            headers=headers,
            message_id=message_id,
        )

    def consume(
        self,
        *,
        timeout: float | None = None,
        batch_size: int = 1,
    ) -> Message | list[Message] | None:
        """Consume one or more messages from the selected backend.

        Args:
            timeout: Backend-specific blocking timeout in seconds.
            batch_size: Maximum number of messages to return. Values greater
                than one return a list.

        Returns:
            ``None`` when no message is available, one ``Message`` when
            ``batch_size`` is one, or a list of messages for batch consumption.
        """

        return self.backend.consume(timeout=timeout, batch_size=batch_size)

    def ack(self, message: Message) -> None:
        """Acknowledge successful processing of a message.

        Args:
            message: Message returned by ``consume``.

        Raises:
            AckError: If the message is not present in the backend's processing
                state.
        """

        self.backend.ack(message)

    def nack(self, message: Message, *, requeue: bool = True) -> None:
        """Reject a message and optionally requeue it.

        Args:
            message: Message returned by ``consume``.
            requeue: When true, move the message back to the ready queue. When
                false, move it to dead letters.
        """

        self.backend.nack(message, requeue=requeue)

    def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        """Retry a message according to the configured retry policy.

        Args:
            message: Message returned by ``consume``.
            delay: Reserved delay hint for future delayed retry strategies.
            reason: Optional diagnostic reason emitted in monitoring events.

        Raises:
            RetryExceededError: If the message has reached ``max_retries`` and
                is moved to dead letters.
        """

        self.backend.retry(message, delay=delay, reason=reason)

    def delay(
        self,
        payload: Any,
        *,
        delay_seconds: float | None = None,
        run_at: float | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        """Schedule a payload for future delivery.

        Args:
            payload: Application payload to deliver later.
            delay_seconds: Relative delay in seconds.
            run_at: Absolute Unix timestamp when the message becomes due.
            headers: Optional metadata stored with the message.

        Returns:
            The scheduled message id.

        Raises:
            QueueConfigError: If both ``delay_seconds`` and ``run_at`` are set or
                if either value is negative.
        """

        message_id = self.delay_backend.delay(
            payload,
            delay_seconds=delay_seconds,
            run_at=run_at,
            headers=headers,
        )
        return message_id

    def schedule_due(self, *, limit: int = 100, now: float | None = None) -> int:
        """Move due delayed messages into the active backend.

        Args:
            limit: Maximum number of due messages to release.
            now: Optional Unix timestamp override for tests or custom schedulers.

        Returns:
            Number of released messages.
        """

        return self.delay_backend.schedule_due(limit=limit, now=now)

    def recover_stale(self, *, min_idle_ms: int | None = None, limit: int = 100) -> int:
        """Recover messages left in backend processing state.

        Args:
            min_idle_ms: Minimum idle time for Streams pending recovery. List
                recovery ignores this value and requeues processing entries.
            limit: Maximum number of messages to recover.

        Returns:
            Number of recovered messages.
        """

        if isinstance(self.backend, StreamBackend):
            idle = min_idle_ms or int(self.config.visibility_timeout_seconds * 1000)
            return len(self.backend.recover_pending(min_idle_ms=idle, limit=limit))
        return self.backend.recover_stale(limit=limit)

    def dead_letters(self, *, limit: int = 100) -> list[Message]:
        """Read dead-lettered messages.

        Args:
            limit: Maximum number of dead letters to return.

        Returns:
            Dead-letter messages decoded from the selected backend.
        """

        return self.backend.dead_letters(limit=limit)

    def requeue_dead(self, message: Message) -> None:
        """Move a dead-lettered message back to the ready path.

        Args:
            message: Message previously returned by ``dead_letters``.
        """

        self.backend.requeue_dead(message)

    def close(self) -> None:
        """Close the Redis client when this client owns it."""

        if not self._owns_redis:
            return

        close = getattr(self.redis, "close", None)
        if close is not None:
            close()

    def __enter__(self) -> QueueClient:
        """Enter a synchronous resource-management context."""

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        """Close owned resources when leaving a context manager."""

        self.close()

    def _create_backend(self) -> ListBackend | StreamBackend:
        """Instantiate the configured concrete backend.

        Returns:
            A synchronous List or Streams backend.

        Raises:
            TypeError: If the Redis client is missing.
            RedisCompatibilityError: If Redis lacks required backend commands.
        """

        if self.config.backend_type is BackendType.LIST:
            if self.redis is None:
                raise TypeError("redis client is required for List backend")
            capabilities = self.capabilities or detect_capabilities(
                cast(RedisInfoClient, self.redis)
            )
            return ListBackend(self.redis, self.config, capabilities)
        if self.config.backend_type is BackendType.STREAM:
            if self.redis is None:
                raise TypeError("redis client is required for Streams backend")
            capabilities = self.capabilities or detect_capabilities(
                cast(RedisInfoClient, self.redis)
            )
            return StreamBackend(self.redis, self.config, capabilities)
        raise NotImplementedError(
            f"backend {self.config.backend_type.value!r} is not implemented"
        )

    def _create_delay_backend(self) -> DelayBackend:
        """Create the Sorted Set delay scheduler for the current backend."""

        if self.redis is None:
            raise TypeError("redis client is required for delayed tasks")
        capabilities = self.capabilities or detect_capabilities(
            cast(RedisInfoClient, self.redis)
        )
        capabilities.require_delay_sorted_set()
        return DelayBackend(self.redis, self.config, self.backend)
