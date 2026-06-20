# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""RedQueue exception hierarchy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ErrorContext:
    """Structured context attached to RedQueue errors.

    Attributes:
        action: Operation name that failed, for example ``message.ack``.
        queue: Logical queue name related to the error.
        details: Additional structured diagnostic fields.
    """

    action: str | None = None
    queue: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return only populated context fields.

        Returns:
            Dictionary containing non-empty ``action``, ``queue``, and
            ``details`` values.
        """

        data: dict[str, Any] = {}
        if self.action:
            data["action"] = self.action
        if self.queue:
            data["queue"] = self.queue
        if self.details:
            data["details"] = dict(self.details)
        return data


class RedQueueError(Exception):
    """Base class for all RedQueue errors.

    ``RedQueueError`` keeps a human-readable message and machine-readable
    ``ErrorContext`` so callers can log or expose failures consistently.

    Attributes:
        message: Human-readable error message.
        context: Structured error context.
    """

    default_message = "RedQueue operation failed"

    def __init__(
        self,
        message: str | None = None,
        *,
        action: str | None = None,
        queue: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize a RedQueue error.

        Args:
            message: Optional error message. Defaults to ``default_message``.
            action: Optional operation identifier.
            queue: Optional logical queue name.
            details: Optional structured diagnostic fields.
        """

        self.message = message or self.default_message
        self.context = ErrorContext(
            action=action,
            queue=queue,
            details=dict(details or {}),
        )
        super().__init__(self.__str__())

    def __str__(self) -> str:
        """Render the message and structured context as text.

        Returns:
            Human-readable string suitable for logs and tracebacks.
        """

        context = self.context.as_dict()
        if not context:
            return self.message
        context_text = ", ".join(
            f"{key}={value!r}" for key, value in context.items()
        )
        return f"{self.message} ({context_text})"

    def to_dict(self) -> dict[str, Any]:
        """Return a structured representation suitable for logs or APIs.

        Returns:
            Dictionary containing exception type, message, and context fields.
        """

        return {
            "type": self.__class__.__name__,
            "message": self.message,
            **self.context.as_dict(),
        }


class RedisCompatibilityError(RedQueueError):
    """Raised when the connected Redis server lacks a required capability."""

    default_message = "Redis server does not meet RedQueue compatibility requirements"

    @classmethod
    def for_feature(
        cls,
        feature: str,
        *,
        current_version: Any,
        required_version: str,
        action: str | None = None,
        queue: str | None = None,
    ) -> RedisCompatibilityError:
        """Create a compatibility error with a consistent upgrade message.

        Args:
            feature: Human-readable feature name.
            current_version: Detected Redis version.
            required_version: Minimum Redis version required by the feature.
            action: Optional operation identifier.
            queue: Optional logical queue name.

        Returns:
            Configured ``RedisCompatibilityError`` instance.
        """

        return cls(
            f"{feature} requires Redis >= {required_version}, "
            f"current Redis is {current_version}. "
            f"Disable {feature.lower()} or upgrade Redis.",
            action=action,
            queue=queue,
            details={
                "feature": feature,
                "current_version": str(current_version),
                "required_version": required_version,
            },
        )


class QueueConfigError(RedQueueError):
    """Raised when queue configuration is invalid."""

    default_message = "Queue configuration is invalid"


class MessageEncodeError(RedQueueError):
    """Raised when a message cannot be encoded."""

    default_message = "Message encoding failed"

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        action: str = "message.encode",
        queue: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> MessageEncodeError:
        """Wrap a lower-level encoding exception while preserving cause.

        Args:
            exc: Original exception raised by a serializer.
            action: Operation identifier.
            queue: Optional logical queue name.
            details: Optional structured diagnostic fields.

        Returns:
            ``MessageEncodeError`` with ``__cause__`` set to ``exc``.
        """

        error = cls(str(exc), action=action, queue=queue, details=details)
        error.__cause__ = exc
        return error


class MessageDecodeError(RedQueueError):
    """Raised when a message cannot be decoded."""

    default_message = "Message decoding failed"

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        action: str = "message.decode",
        queue: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> MessageDecodeError:
        """Wrap a lower-level decoding exception while preserving cause.

        Args:
            exc: Original exception raised by a serializer.
            action: Operation identifier.
            queue: Optional logical queue name.
            details: Optional structured diagnostic fields.

        Returns:
            ``MessageDecodeError`` with ``__cause__`` set to ``exc``.
        """

        error = cls(str(exc), action=action, queue=queue, details=details)
        error.__cause__ = exc
        return error


class BackendUnavailableError(RedQueueError):
    """Raised when the Redis backend is unavailable or command execution fails."""

    default_message = "Redis backend is unavailable"


class AckError(RedQueueError):
    """Raised when a message cannot be acknowledged consistently."""

    default_message = "Message acknowledgement failed"


class RetryExceededError(RedQueueError):
    """Raised when a message exceeds the configured retry policy."""

    default_message = "Message retry limit exceeded"
