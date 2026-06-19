# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Synchronous RedQueue client."""

from __future__ import annotations

from typing import Any, cast

from redis import Redis

from redqueue.backends import DelayBackend, ListBackend, StreamBackend
from redqueue.compat import RedisCapabilities, RedisInfoClient, detect_capabilities
from redqueue.config import BackendType, QueueConfig
from redqueue.message import Message
from redqueue.monitoring import MonitoringEvent, MonitoringEventType


class QueueClient:
    """Synchronous queue client."""

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
        **options: Any,
    ) -> QueueClient:
        redis = options.pop("redis", None) or Redis.from_url(url)
        capabilities = options.pop("capabilities", None) or detect_capabilities(
            cast(RedisInfoClient, redis)
        )
        config = QueueConfig(queue=queue, backend=backend, **options)
        return cls(config=config, redis=redis, capabilities=capabilities)

    def publish(
        self,
        payload: Any,
        *,
        delay: float | None = None,
        headers: dict[str, Any] | None = None,
        message_id: str | None = None,
    ) -> str:
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
        return self.backend.consume(timeout=timeout, batch_size=batch_size)

    def ack(self, message: Message) -> None:
        self.backend.ack(message)

    def nack(self, message: Message, *, requeue: bool = True) -> None:
        self.backend.nack(message, requeue=requeue)

    def retry(
        self,
        message: Message,
        *,
        delay: float | None = None,
        reason: str | None = None,
    ) -> None:
        self.backend.retry(message, delay=delay, reason=reason)

    def delay(
        self,
        payload: Any,
        *,
        delay_seconds: float | None = None,
        run_at: float | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        message_id = self.delay_backend.delay(
            payload,
            delay_seconds=delay_seconds,
            run_at=run_at,
            headers=headers,
        )
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.DELAY_SCHEDULED,
                queue=self.config.queue,
                message_id=message_id,
                backend=self.config.backend_type.value,
                attributes={"delay_seconds": delay_seconds, "run_at": run_at},
            )
        )
        return message_id

    def schedule_due(self, *, limit: int = 100, now: float | None = None) -> int:
        return self.delay_backend.schedule_due(limit=limit, now=now)

    def recover_stale(self, *, min_idle_ms: int | None = None, limit: int = 100) -> int:
        if isinstance(self.backend, StreamBackend):
            idle = min_idle_ms or int(self.config.visibility_timeout_seconds * 1000)
            return len(self.backend.recover_pending(min_idle_ms=idle, limit=limit))
        return self.backend.recover_stale(limit=limit)

    def dead_letters(self, *, limit: int = 100) -> list[Message]:
        return self.backend.dead_letters(limit=limit)

    def requeue_dead(self, message: Message) -> None:
        self.backend.requeue_dead(message)

    def close(self) -> None:
        close = getattr(self.redis, "close", None)
        if close is not None:
            close()

    def _create_backend(self) -> ListBackend | StreamBackend:
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
        if self.redis is None:
            raise TypeError("redis client is required for delayed tasks")
        capabilities = self.capabilities or detect_capabilities(
            cast(RedisInfoClient, self.redis)
        )
        capabilities.require_delay_sorted_set()
        return DelayBackend(self.redis, self.config, self.backend)
