# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Redis-backed publish-time deduplication helpers."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Protocol

from redqueue.config import QueueConfig
from redqueue.exceptions import BackendUnavailableError, QueueConfigError
from redqueue.monitoring import MonitoringEvent, MonitoringEventType


@dataclass(frozen=True)
class DeduplicationResult:
    """Result of reserving or detecting a deduplication key.

    Attributes:
        dedup_key: Normalized deduplication key.
        message_id: New or existing RedQueue message id.
        duplicate: Whether the key already existed.
    """

    dedup_key: str
    message_id: str
    duplicate: bool


class SyncDedupRedis(Protocol):
    """Redis command subset required by sync deduplication."""

    def set(
        self,
        name: str,
        value: str,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None: ...

    def get(self, name: str) -> str | bytes | None: ...

    def delete(self, *names: str) -> int: ...


class AsyncDedupRedis(Protocol):
    """Redis command subset required by async deduplication."""

    async def set(
        self,
        name: str,
        value: str,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None: ...

    async def get(self, name: str) -> str | bytes | None: ...

    async def delete(self, *names: str) -> int: ...


class DeduplicationBackend:
    """Synchronous publish-time deduplication backed by Redis strings."""

    backend_name = "deduplication"

    def __init__(self, redis: SyncDedupRedis, config: QueueConfig) -> None:
        """Initialize the sync deduplication helper.

        Args:
            redis: Redis client exposing string commands.
            config: Queue configuration containing deduplication settings.
        """

        self.redis = redis
        self.config = config

    def reserve_or_get(
        self,
        dedup_key: str,
        message_id: str,
        *,
        trace_id: str | None = None,
    ) -> DeduplicationResult:
        """Reserve a deduplication key or return the existing message id.

        Args:
            dedup_key: User-provided deduplication key.
            message_id: Message id to store when the key is new.
            trace_id: Optional trace id for duplicate monitoring events.

        Returns:
            Deduplication result describing a new reservation or duplicate hit.

        Raises:
            QueueConfigError: If ``dedup_key`` is blank.
            BackendUnavailableError: If Redis string commands fail.
        """

        key = self.normalize_key(dedup_key)
        redis_key = self.redis_key(key)
        try:
            created = self.redis.set(
                redis_key,
                message_id,
                nx=True,
                **self._ttl_kwargs(),
            )
            if created:
                return DeduplicationResult(
                    dedup_key=key,
                    message_id=message_id,
                    duplicate=False,
                )
            existing = self.redis.get(redis_key)
        except Exception as exc:
            self._emit_backend_error("redis.dedup.reserve", str(exc))
            raise BackendUnavailableError(
                "Redis deduplication command failed",
                action="redis.dedup.reserve",
                queue=self.config.queue,
            ) from exc

        if existing is None:
            return DeduplicationResult(
                dedup_key=key,
                message_id=message_id,
                duplicate=False,
            )
        existing_id = (
            existing.decode() if isinstance(existing, bytes) else str(existing)
        )
        self._emit_duplicate(key, existing_id, trace_id=trace_id)
        return DeduplicationResult(
            dedup_key=key,
            message_id=existing_id,
            duplicate=True,
        )

    def rollback(self, dedup_key: str) -> None:
        """Delete a reserved deduplication key after publish failure."""

        key = self.normalize_key(dedup_key)
        try:
            self.redis.delete(self.redis_key(key))
        except Exception as exc:
            self._emit_backend_error("redis.dedup.rollback", str(exc))
            raise BackendUnavailableError(
                "Redis deduplication rollback failed",
                action="redis.dedup.rollback",
                queue=self.config.queue,
            ) from exc

    def redis_key(self, dedup_key: str) -> str:
        """Return the namespaced Redis key for a normalized deduplication key."""

        return self.config.key(f"dedup:{dedup_key}")

    @staticmethod
    def normalize_key(dedup_key: str) -> str:
        """Trim and validate a user-provided deduplication key."""

        if not isinstance(dedup_key, str):
            raise QueueConfigError("dedup_key must be a string")
        normalized = dedup_key.strip()
        if not normalized:
            raise QueueConfigError("dedup_key must not be empty")
        return normalized

    def _ttl_kwargs(self) -> dict[str, int]:
        ttl = self.config.deduplication.ttl_seconds
        if float(ttl).is_integer():
            return {"ex": int(ttl)}
        return {"px": max(1, ceil(ttl * 1000))}

    def _emit_duplicate(
        self,
        dedup_key: str,
        message_id: str,
        *,
        trace_id: str | None,
    ) -> None:
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.MESSAGE_DEDUPLICATED,
                queue=self.config.queue,
                message_id=message_id,
                trace_id=trace_id,
                backend=self.backend_name,
                attributes={
                    "dedup_key": dedup_key,
                    "existing_message_id": message_id,
                },
            )
        )

    def _emit_backend_error(self, action: str, error: str | None = None) -> None:
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.BACKEND_ERROR,
                queue=self.config.queue,
                backend=self.backend_name,
                error=error,
                attributes={"action": action},
            )
        )


class AsyncDeduplicationBackend:
    """Asynchronous publish-time deduplication backed by Redis strings."""

    backend_name = "deduplication"

    def __init__(self, redis: AsyncDedupRedis, config: QueueConfig) -> None:
        """Initialize the async deduplication helper."""

        self.redis = redis
        self.config = config

    async def reserve_or_get(
        self,
        dedup_key: str,
        message_id: str,
        *,
        trace_id: str | None = None,
    ) -> DeduplicationResult:
        """Reserve a deduplication key asynchronously or return existing id."""

        key = self.normalize_key(dedup_key)
        redis_key = self.redis_key(key)
        try:
            created = await self.redis.set(
                redis_key,
                message_id,
                nx=True,
                **self._ttl_kwargs(),
            )
            if created:
                return DeduplicationResult(
                    dedup_key=key,
                    message_id=message_id,
                    duplicate=False,
                )
            existing = await self.redis.get(redis_key)
        except Exception as exc:
            self._emit_backend_error("redis.dedup.reserve", str(exc))
            raise BackendUnavailableError(
                "Redis async deduplication command failed",
                action="redis.dedup.reserve",
                queue=self.config.queue,
            ) from exc

        if existing is None:
            return DeduplicationResult(
                dedup_key=key,
                message_id=message_id,
                duplicate=False,
            )
        existing_id = (
            existing.decode() if isinstance(existing, bytes) else str(existing)
        )
        self._emit_duplicate(key, existing_id, trace_id=trace_id)
        return DeduplicationResult(
            dedup_key=key,
            message_id=existing_id,
            duplicate=True,
        )

    async def rollback(self, dedup_key: str) -> None:
        """Delete a reserved deduplication key after async publish failure."""

        key = self.normalize_key(dedup_key)
        try:
            await self.redis.delete(self.redis_key(key))
        except Exception as exc:
            self._emit_backend_error("redis.dedup.rollback", str(exc))
            raise BackendUnavailableError(
                "Redis async deduplication rollback failed",
                action="redis.dedup.rollback",
                queue=self.config.queue,
            ) from exc

    def redis_key(self, dedup_key: str) -> str:
        """Return the namespaced Redis key for a normalized deduplication key."""

        return self.config.key(f"dedup:{dedup_key}")

    @staticmethod
    def normalize_key(dedup_key: str) -> str:
        """Trim and validate a user-provided deduplication key."""

        return DeduplicationBackend.normalize_key(dedup_key)

    def _ttl_kwargs(self) -> dict[str, int]:
        ttl = self.config.deduplication.ttl_seconds
        if float(ttl).is_integer():
            return {"ex": int(ttl)}
        return {"px": max(1, ceil(ttl * 1000))}

    def _emit_duplicate(
        self,
        dedup_key: str,
        message_id: str,
        *,
        trace_id: str | None,
    ) -> None:
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.MESSAGE_DEDUPLICATED,
                queue=self.config.queue,
                message_id=message_id,
                trace_id=trace_id,
                backend=self.backend_name,
                attributes={
                    "dedup_key": dedup_key,
                    "existing_message_id": message_id,
                },
            )
        )

    def _emit_backend_error(self, action: str, error: str | None = None) -> None:
        self.config.monitoring.emit(
            MonitoringEvent(
                type=MonitoringEventType.BACKEND_ERROR,
                queue=self.config.queue,
                backend=self.backend_name,
                error=error,
                attributes={"action": action},
            )
        )
