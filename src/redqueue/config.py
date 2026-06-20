# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Configuration models for RedQueue."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from redqueue.exceptions import QueueConfigError
from redqueue.monitoring import MonitoringHook, NoopMonitoringHook, SafeMonitoringHook
from redqueue.serialization import JsonSerializer, Serializer


class BackendType(str, Enum):
    """Supported Redis queue backends.

    Attributes:
        LIST: Reliable Redis List backend.
        STREAM: Redis Streams backend with consumer groups.
    """

    LIST = "list"
    STREAM = "stream"

    @classmethod
    def coerce(cls, value: str | BackendType) -> BackendType:
        """Convert user input into a backend enum.

        Args:
            value: String backend name or ``BackendType`` instance.

        Returns:
            Normalized backend enum.

        Raises:
            QueueConfigError: If ``value`` does not match a supported backend.
        """

        if isinstance(value, cls):
            return value
        try:
            return cls(value)
        except ValueError as exc:
            supported = ", ".join(item.value for item in cls)
            raise QueueConfigError(
                f"unsupported backend {value!r}; supported backends: {supported}"
            ) from exc


@dataclass(frozen=True)
class RetryConfig:
    """Retry behavior for failed messages.

    Attributes:
        max_retries: Maximum number of retry attempts before dead-lettering.
        base_delay_seconds: Linear delay factor used by ``next_delay``.
        max_delay_seconds: Optional upper bound for computed retry delay.
    """

    max_retries: int = 3
    base_delay_seconds: float = 0.0
    max_delay_seconds: float | None = None

    def __post_init__(self) -> None:
        """Validate retry limits after dataclass initialization.

        Raises:
            QueueConfigError: If retry counts or delay values are negative, or
                if ``max_delay_seconds`` is less than ``base_delay_seconds``.
        """

        if self.max_retries < 0:
            raise QueueConfigError("max_retries must be greater than or equal to 0")
        if self.base_delay_seconds < 0:
            raise QueueConfigError(
                "base_delay_seconds must be greater than or equal to 0"
            )
        if self.max_delay_seconds is not None and self.max_delay_seconds < 0:
            raise QueueConfigError(
                "max_delay_seconds must be greater than or equal to 0"
            )
        if (
            self.max_delay_seconds is not None
            and self.max_delay_seconds < self.base_delay_seconds
        ):
            raise QueueConfigError(
                "max_delay_seconds must be greater than or equal to base_delay_seconds"
            )

    def next_delay(self, attempts: int) -> float:
        """Return the retry delay for an attempt count.

        Args:
            attempts: Current attempt count.

        Returns:
            Delay in seconds, capped by ``max_delay_seconds`` when configured.

        Raises:
            QueueConfigError: If ``attempts`` is negative.
        """

        if attempts < 0:
            raise QueueConfigError("attempts must be greater than or equal to 0")
        delay = self.base_delay_seconds * attempts
        if self.max_delay_seconds is not None:
            return min(delay, self.max_delay_seconds)
        return delay


@dataclass(frozen=True)
class QueueConfig:
    """Queue configuration shared by sync and async clients.

    Attributes:
        queue: Logical queue name. Whitespace is trimmed and whitespace inside
            the name is rejected.
        backend: Backend type or backend name.
        enable_delay: Reserved feature flag for delay support.
        namespace: Redis key namespace prefix.
        retry: Retry policy used by backend ``retry`` operations.
        monitoring: Monitoring hook. Custom hooks are wrapped in
            ``SafeMonitoringHook``.
        serializer: Payload serializer used for message envelopes.
        visibility_timeout_seconds: Default stale/pending recovery window.
        consumer_group: Streams consumer group name.
        consumer_name: Optional Streams consumer name.
        metadata: User-defined configuration metadata.
    """

    queue: str
    backend: BackendType | str = BackendType.LIST
    enable_delay: bool = False
    namespace: str = "rq"
    retry: RetryConfig = field(default_factory=RetryConfig)
    monitoring: MonitoringHook = field(
        default_factory=lambda: SafeMonitoringHook(NoopMonitoringHook())
    )
    serializer: Serializer = field(default_factory=JsonSerializer)
    visibility_timeout_seconds: float = 300.0
    consumer_group: str = "redqueue"
    consumer_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize and validate queue configuration.

        Raises:
            QueueConfigError: If names, retry config, serializer, monitoring
                hook, or visibility timeout are invalid.
        """

        queue = self._normalize_name(self.queue, field_name="queue")
        namespace = self._normalize_name(self.namespace, field_name="namespace")
        object.__setattr__(self, "queue", queue)
        object.__setattr__(self, "namespace", namespace)
        object.__setattr__(self, "backend", BackendType.coerce(self.backend))

        if self.visibility_timeout_seconds <= 0:
            raise QueueConfigError("visibility_timeout_seconds must be greater than 0")
        if not isinstance(self.retry, RetryConfig):
            raise QueueConfigError("retry must be a RetryConfig instance")
        if not hasattr(self.monitoring, "emit"):
            raise QueueConfigError(
                "monitoring must implement the MonitoringHook protocol"
            )
        if not isinstance(self.monitoring, SafeMonitoringHook):
            object.__setattr__(
                self,
                "monitoring",
                SafeMonitoringHook(self.monitoring),
            )
        if not isinstance(self.serializer, Serializer):
            raise QueueConfigError("serializer must implement the Serializer protocol")
        if self.consumer_group is not None:
            consumer_group = self.consumer_group.strip()
            if not consumer_group:
                raise QueueConfigError("consumer_group must not be empty")
            object.__setattr__(self, "consumer_group", consumer_group)
        if self.consumer_name is not None:
            consumer_name = self.consumer_name.strip()
            if not consumer_name:
                raise QueueConfigError("consumer_name must not be empty")
            object.__setattr__(self, "consumer_name", consumer_name)

    @staticmethod
    def _normalize_name(value: str, *, field_name: str) -> str:
        """Validate and trim a Redis key name segment.

        Args:
            value: Raw name value.
            field_name: Human-readable field name used in error messages.

        Returns:
            Trimmed name.

        Raises:
            QueueConfigError: If the value is not a non-empty string or contains
                whitespace.
        """

        if not isinstance(value, str):
            raise QueueConfigError(f"{field_name} must be a string")
        normalized = value.strip()
        if not normalized:
            raise QueueConfigError(f"{field_name} must not be empty")
        if any(ch.isspace() for ch in normalized):
            raise QueueConfigError(f"{field_name} must not contain whitespace")
        return normalized

    @property
    def backend_type(self) -> BackendType:
        """Normalized backend type.

        Returns:
            ``BackendType`` value derived from the ``backend`` field.
        """

        return BackendType.coerce(self.backend)

    @property
    def key_prefix(self) -> str:
        """Redis key prefix for this queue.

        Returns:
            Hash-tagged Redis key prefix, for example ``rq:{emails}``.
        """

        return f"{self.namespace}:{{{self.queue}}}"

    def key(self, suffix: str) -> str:
        """Build a namespaced Redis key for a queue-owned data structure.

        Args:
            suffix: Key suffix such as ``ready``, ``processing``, or ``dead``.

        Returns:
            Fully namespaced Redis key.

        Raises:
            QueueConfigError: If ``suffix`` is invalid.
        """

        suffix = self._normalize_name(suffix, field_name="suffix")
        return f"{self.key_prefix}:{suffix}"
