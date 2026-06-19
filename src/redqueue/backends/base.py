# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Shared backend helpers."""

from __future__ import annotations

from typing import Any

from redqueue.config import QueueConfig
from redqueue.exceptions import BackendUnavailableError
from redqueue.message import Message
from redqueue.monitoring import MonitoringEvent, MonitoringEventType


class BaseMessageBackend:
    """Shared message envelope and monitoring behavior."""

    backend_name = "backend"

    def __init__(self, config: QueueConfig) -> None:
        self.config = config

    def _encode(self, message: Message) -> bytes:
        envelope = {
            "id": message.id,
            "queue": message.queue,
            "payload": message.payload,
            "headers": message.headers,
            "attempts": message.attempts,
            "created_at": message.created_at,
            "available_at": message.available_at,
            "backend": self.backend_name,
            "raw_id": message.raw_id,
        }
        return self.config.serializer.encode(envelope, queue=self.config.queue)

    def _decode(self, payload: bytes) -> Message:
        envelope = self.config.serializer.decode(payload, queue=self.config.queue)
        if not isinstance(envelope, dict):
            raise BackendUnavailableError(
                "decoded message envelope must be a mapping",
                action="message.decode",
                queue=self.config.queue,
                details={"payload_type": type(envelope).__name__},
            )
        return Message(
            id=str(envelope["id"]),
            queue=str(envelope["queue"]),
            payload=envelope["payload"],
            headers=dict(envelope.get("headers") or {}),
            attempts=int(envelope.get("attempts") or 0),
            created_at=float(envelope["created_at"]),
            available_at=envelope.get("available_at"),
            backend=self.backend_name,
            raw_id=envelope.get("raw_id"),
        )

    def _emit(
        self,
        event_type: MonitoringEventType,
        message: Message,
        *,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        self.config.monitoring.emit(
            MonitoringEvent(
                type=event_type,
                queue=self.config.queue,
                message_id=message.id,
                backend=self.backend_name,
                attributes=attributes or {},
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


class BaseListBackend(BaseMessageBackend):
    """Shared Redis List backend behavior."""

    backend_name = "list"

    @property
    def ready_key(self) -> str:
        return self.config.key("ready")

    @property
    def processing_key(self) -> str:
        return self.config.key("processing")

    @property
    def dead_key(self) -> str:
        return self.config.key("dead")
