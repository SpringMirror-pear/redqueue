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
    """Create a stable opaque message identifier."""

    return uuid4().hex


@dataclass(frozen=True)
class Message:
    """A normalized message returned by RedQueue consumers."""

    payload: Any
    queue: str
    id: str = field(default_factory=new_message_id)
    headers: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    created_at: float = field(default_factory=time)
    available_at: float | None = None
    backend: str | None = None
    raw_id: str | None = None

    def __post_init__(self) -> None:
        message_id = self._normalize_required(self.id, field_name="id")
        queue = self._normalize_required(self.queue, field_name="queue")
        object.__setattr__(self, "id", message_id)
        object.__setattr__(self, "queue", queue)
        object.__setattr__(self, "headers", dict(self.headers))

        if self.attempts < 0:
            raise QueueConfigError("attempts must be greater than or equal to 0")
        if self.available_at is not None and self.available_at < 0:
            raise QueueConfigError("available_at must be greater than or equal to 0")
        if self.created_at < 0:
            raise QueueConfigError("created_at must be greater than or equal to 0")

    @staticmethod
    def _normalize_required(value: str, *, field_name: str) -> str:
        if not isinstance(value, str):
            raise QueueConfigError(f"message {field_name} must be a string")
        normalized = value.strip()
        if not normalized:
            raise QueueConfigError(f"message {field_name} must not be empty")
        return normalized

    def with_attempt(self) -> Message:
        """Return a copy with attempts incremented by one."""

        return replace(self, attempts=self.attempts + 1)

    def with_backend(self, backend: str, *, raw_id: str | None = None) -> Message:
        """Return a copy tagged with backend-specific metadata."""

        return replace(self, backend=backend, raw_id=raw_id)
