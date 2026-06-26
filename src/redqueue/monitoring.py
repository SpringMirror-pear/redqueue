# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Monitoring hooks for RedQueue lifecycle events."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import Any, Protocol


class MonitoringEventType(str, Enum):
    """Event names emitted by RedQueue.

    The enum values are stable string identifiers intended for logs, metrics,
    and tracing integrations.
    """

    CLIENT_CREATED = "client.created"
    MESSAGE_PUBLISHED = "message.published"
    MESSAGE_CONSUMED = "message.consumed"
    MESSAGE_ACKED = "message.acked"
    MESSAGE_NACKED = "message.nacked"
    MESSAGE_RETRIED = "message.retried"
    MESSAGE_DEAD_LETTERED = "message.dead_lettered"
    DELAY_SCHEDULED = "delay.scheduled"
    DELAY_RELEASED = "delay.released"
    BACKEND_ERROR = "backend.error"


@dataclass(frozen=True)
class MonitoringEvent:
    """A structured monitoring event emitted by clients and backends.

    Attributes:
        type: Event type identifier.
        queue: Logical queue name.
        timestamp: Unix timestamp when the event was created.
        message_id: Optional RedQueue message id.
        trace_id: Optional correlation id shared by message lifecycle events.
        backend: Optional backend name.
        duration_ms: Optional operation duration in milliseconds.
        error: Optional error text for failure events.
        attributes: Additional structured event attributes. Business payload is
            intentionally not included by default.
    """

    type: MonitoringEventType
    queue: str
    timestamp: float = field(default_factory=time)
    message_id: str | None = None
    trace_id: str | None = None
    backend: str | None = None
    duration_ms: float | None = None
    error: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation safe for logs and metrics.

        Returns:
            Dictionary containing populated monitoring fields.
        """

        data: dict[str, Any] = {
            "type": self.type.value,
            "queue": self.queue,
            "timestamp": self.timestamp,
        }
        if self.message_id is not None:
            data["message_id"] = self.message_id
        if self.trace_id is not None:
            data["trace_id"] = self.trace_id
        if self.backend is not None:
            data["backend"] = self.backend
        if self.duration_ms is not None:
            data["duration_ms"] = self.duration_ms
        if self.error is not None:
            data["error"] = self.error
        if self.attributes:
            data["attributes"] = dict(self.attributes)
        return data


class MonitoringHook(Protocol):
    """Protocol for metrics, tracing, and logging integrations."""

    def emit(self, event: MonitoringEvent) -> None:
        """Handle a monitoring event.

        Args:
            event: Structured monitoring event emitted by RedQueue.
        """


class NoopMonitoringHook:
    """Default monitoring hook that drops events."""

    def emit(self, event: MonitoringEvent) -> None:
        """Drop a monitoring event.

        Args:
            event: Event to ignore.
        """

        return None


class InMemoryMonitoringHook:
    """Monitoring hook that stores events in memory for tests or diagnostics.

    Attributes:
        events: Events received by this hook in order.
    """

    def __init__(self) -> None:
        """Initialize an empty in-memory event store."""

        self.events: list[MonitoringEvent] = []

    def emit(self, event: MonitoringEvent) -> None:
        """Append an event to the in-memory store.

        Args:
            event: Event to store.
        """

        self.events.append(event)

    def clear(self) -> None:
        """Remove all stored events."""

        self.events.clear()


class CompositeMonitoringHook:
    """Monitoring hook that fans out events to multiple hooks."""

    def __init__(self, *hooks: MonitoringHook) -> None:
        """Initialize a fan-out hook.

        Args:
            *hooks: Hooks that should receive each event.
        """

        self.hooks = hooks

    def emit(self, event: MonitoringEvent) -> None:
        """Emit an event to each configured hook.

        Args:
            event: Event to fan out.
        """

        for hook in self.hooks:
            hook.emit(event)


class SafeMonitoringHook:
    """Monitoring hook wrapper that isolates hook failures from queue operations.

    Attributes:
        hook: Wrapped monitoring hook.
        errors: Exceptions raised by the wrapped hook.
    """

    def __init__(self, hook: MonitoringHook) -> None:
        """Wrap a monitoring hook.

        Args:
            hook: Hook whose failures should be isolated.
        """

        self.hook = hook
        self.errors: list[Exception] = []

    def emit(self, event: MonitoringEvent) -> None:
        """Emit an event while capturing hook exceptions.

        Args:
            event: Event to emit.
        """

        try:
            self.hook.emit(event)
        except Exception as exc:
            self.errors.append(exc)
