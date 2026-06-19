# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Monitoring hooks for RedQueue lifecycle events."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import Any, Protocol


class MonitoringEventType(str, Enum):
    """Event names emitted by RedQueue."""

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
    """A structured monitoring event emitted by clients and backends."""

    type: MonitoringEventType
    queue: str
    timestamp: float = field(default_factory=time)
    message_id: str | None = None
    backend: str | None = None
    duration_ms: float | None = None
    error: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary representation safe for logs and metrics."""

        data: dict[str, Any] = {
            "type": self.type.value,
            "queue": self.queue,
            "timestamp": self.timestamp,
        }
        if self.message_id is not None:
            data["message_id"] = self.message_id
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
        """Handle a monitoring event."""


class NoopMonitoringHook:
    """Default monitoring hook that drops events."""

    def emit(self, event: MonitoringEvent) -> None:
        return None


class InMemoryMonitoringHook:
    """Monitoring hook that stores events in memory for tests or diagnostics."""

    def __init__(self) -> None:
        self.events: list[MonitoringEvent] = []

    def emit(self, event: MonitoringEvent) -> None:
        self.events.append(event)

    def clear(self) -> None:
        self.events.clear()


class CompositeMonitoringHook:
    """Monitoring hook that fans out events to multiple hooks."""

    def __init__(self, *hooks: MonitoringHook) -> None:
        self.hooks = hooks

    def emit(self, event: MonitoringEvent) -> None:
        for hook in self.hooks:
            hook.emit(event)


class SafeMonitoringHook:
    """Monitoring hook wrapper that isolates hook failures from queue operations."""

    def __init__(self, hook: MonitoringHook) -> None:
        self.hook = hook
        self.errors: list[Exception] = []

    def emit(self, event: MonitoringEvent) -> None:
        try:
            self.hook.emit(event)
        except Exception as exc:
            self.errors.append(exc)
