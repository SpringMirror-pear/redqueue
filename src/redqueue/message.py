# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Message model used by RedQueue backends."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from time import time
from typing import Any
from uuid import uuid4

from redqueue.exceptions import QueueConfigError


def new_message_id() -> str:
    """Create a stable opaque message identifier.

    Returns:
        Hex-encoded UUID4 string suitable for Redis keys and message metadata.
    """

    return uuid4().hex


def new_trace_id() -> str:
    """Create a stable opaque trace identifier.

    Returns:
        Hex-encoded UUID4 string suitable for correlating message lifecycle
        events across producers, delayed scheduling, consumers, and retries.
    """

    return uuid4().hex


@dataclass(frozen=True)
class Message:
    """A normalized message returned by RedQueue consumers.

    Attributes:
        payload: Application payload after serializer decoding.
        queue: Logical queue name.
        id: Stable RedQueue message id.
        headers: User metadata copied into the message envelope.
        trace_id: Optional correlation id propagated through message lifecycle
            events and mirrored into ``headers["trace_id"]``.
        attempts: Number of retry attempts already applied.
        created_at: Unix timestamp when the message object was created.
        available_at: Optional Unix timestamp used by delayed messages.
        backend: Backend name that produced or owns the message.
        raw_id: Backend-specific id, such as a Redis Streams entry id.
        raw_payload: Original serialized Redis payload used for exact removal
            from Redis Lists when serializers are not byte-deterministic.
    """

    payload: Any
    queue: str
    id: str = field(default_factory=new_message_id)
    headers: dict[str, Any] = field(default_factory=dict)
    trace_id: str | None = None
    attempts: int = 0
    created_at: float = field(default_factory=time)
    available_at: float | None = None
    backend: str | None = None
    raw_id: str | None = None
    raw_payload: bytes | None = None

    def __post_init__(self) -> None:
        """Normalize immutable dataclass fields after initialization.

        Raises:
            QueueConfigError: If required string fields are empty or timestamps
                and counters are negative.
        """

        message_id = self._normalize_required(self.id, field_name="id")
        queue = self._normalize_required(self.queue, field_name="queue")
        headers = dict(self.headers)
        trace_id = self._normalize_trace_id(self.trace_id, headers=headers)
        object.__setattr__(self, "id", message_id)
        object.__setattr__(self, "queue", queue)
        object.__setattr__(self, "trace_id", trace_id)
        if trace_id is not None:
            headers["trace_id"] = trace_id
        object.__setattr__(self, "headers", headers)

        if self.attempts < 0:
            raise QueueConfigError("attempts must be greater than or equal to 0")
        if self.available_at is not None and self.available_at < 0:
            raise QueueConfigError("available_at must be greater than or equal to 0")
        if self.created_at < 0:
            raise QueueConfigError("created_at must be greater than or equal to 0")

    @staticmethod
    def _normalize_required(value: str, *, field_name: str) -> str:
        """Validate and trim a required message string field.

        Args:
            value: Raw field value.
            field_name: Name included in error messages.

        Returns:
            Trimmed field value.

        Raises:
            QueueConfigError: If the value is not a non-empty string.
        """

        if not isinstance(value, str):
            raise QueueConfigError(f"message {field_name} must be a string")
        normalized = value.strip()
        if not normalized:
            raise QueueConfigError(f"message {field_name} must not be empty")
        return normalized

    @staticmethod
    def _normalize_trace_id(
        trace_id: str | None,
        *,
        headers: dict[str, Any],
    ) -> str | None:
        """Normalize an optional trace id.

        Args:
            trace_id: Explicit trace id passed to the message.
            headers: Message headers that may contain a legacy ``trace_id``.

        Returns:
            Trimmed trace id or ``None``.

        Raises:
            QueueConfigError: If the trace id is empty after trimming.
        """

        value = trace_id if trace_id is not None else headers.get("trace_id")
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            raise QueueConfigError("message trace_id must not be empty")
        return normalized

    def with_attempt(self) -> Message:
        """Return a copy with attempts incremented by one.

        Returns:
            New immutable ``Message`` instance.
        """

        return replace(self, attempts=self.attempts + 1)

    def with_backend(
        self,
        backend: str,
        *,
        raw_id: str | None = None,
        raw_payload: bytes | None = None,
    ) -> Message:
        """Return a copy tagged with backend-specific metadata.

        Args:
            backend: Backend name such as ``list`` or ``stream``.
            raw_id: Optional backend-specific id.
            raw_payload: Optional original serialized Redis payload.

        Returns:
            New immutable ``Message`` instance.
        """

        return replace(
            self,
            backend=backend,
            raw_id=raw_id,
            raw_payload=raw_payload,
        )
