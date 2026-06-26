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
    """Shared message envelope and monitoring behavior.

    Attributes:
        backend_name: Name written into monitoring events and message metadata.
        config: Queue configuration used for key generation and serialization.
    """

    backend_name = "backend"

    def __init__(self, config: QueueConfig) -> None:
        """Initialize shared backend state.

        Args:
            config: Queue configuration owned by the client.
        """

        self.config = config

    def _encode(self, message: Message) -> bytes:
        """Encode a message into a Redis-storable envelope.

        Args:
            message: Message to encode.

        Returns:
            Serialized bytes produced by the configured serializer.
        """

        envelope = {
            "id": message.id,
            "queue": message.queue,
            "payload": message.payload,
            "headers": message.headers,
            "trace_id": message.trace_id,
            "attempts": message.attempts,
            "created_at": message.created_at,
            "available_at": message.available_at,
            "backend": self.backend_name,
            "raw_id": message.raw_id,
        }
        return self.config.serializer.encode(envelope, queue=self.config.queue)

    def _decode(self, payload: bytes) -> Message:
        """Decode a Redis payload into a ``Message``.

        Args:
            payload: Serialized message envelope read from Redis.

        Returns:
            Decoded message tagged with this backend name.

        Raises:
            BackendUnavailableError: If the decoded envelope is not a mapping.
        """

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
            trace_id=envelope.get("trace_id"),
            attempts=int(envelope.get("attempts") or 0),
            created_at=float(envelope["created_at"]),
            available_at=envelope.get("available_at"),
            backend=self.backend_name,
            raw_id=envelope.get("raw_id"),
            raw_payload=payload,
        )

    def _emit(
        self,
        event_type: MonitoringEventType,
        message: Message,
        *,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        """Emit a monitoring event for a message operation.

        Args:
            event_type: Type of event to emit.
            message: Message related to the event.
            attributes: Optional structured event attributes.
        """

        self.config.monitoring.emit(
            MonitoringEvent(
                type=event_type,
                queue=self.config.queue,
                message_id=message.id,
                trace_id=message.trace_id,
                backend=self.backend_name,
                attributes=attributes or {},
            )
        )

    def _emit_backend_error(self, action: str, error: str | None = None) -> None:
        """Emit a backend error monitoring event.

        Args:
            action: Redis command or backend action that failed.
            error: Optional text representation of the underlying error.
        """

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
        """Redis List key holding messages ready for delivery."""

        return self.config.key("ready")

    @property
    def processing_key(self) -> str:
        """Redis List key holding consumed but unacknowledged messages."""

        return self.config.key("processing")

    @property
    def dead_key(self) -> str:
        """Redis List key holding dead-lettered messages."""

        return self.config.key("dead")
