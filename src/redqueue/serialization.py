# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Serialization protocol and default codecs."""

from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable

from redqueue.exceptions import MessageDecodeError, MessageEncodeError


@runtime_checkable
class Serializer(Protocol):
    """Protocol implemented by RedQueue payload serializers.

    Custom serializers are responsible for encoding the complete RedQueue
    message envelope, not only the application payload.

    Attributes:
        content_type: Media type used for diagnostics and integrations.
    """

    content_type: str

    def encode(self, payload: Any, *, queue: str | None = None) -> bytes:
        """Encode a Python payload into Redis-safe bytes.

        Args:
            payload: Python object to encode.
            queue: Optional logical queue name for error context.

        Returns:
            Bytes safe to write to Redis.
        """

    def decode(self, payload: bytes, *, queue: str | None = None) -> Any:
        """Decode Redis bytes into a Python payload.

        Args:
            payload: Bytes read from Redis.
            queue: Optional logical queue name for error context.

        Returns:
            Decoded Python object.
        """


class JsonSerializer:
    """JSON serializer used by default.

    Bytes-like payloads are passed through as bytes. Other payloads are encoded
    as compact UTF-8 JSON.

    Attributes:
        ensure_ascii: Passed to ``json.dumps``.
        sort_keys: Passed to ``json.dumps`` for deterministic output.
    """

    content_type = "application/json"

    def __init__(
        self,
        *,
        ensure_ascii: bool = False,
        sort_keys: bool = True,
    ) -> None:
        """Initialize JSON serialization options.

        Args:
            ensure_ascii: Escape non-ASCII characters when true.
            sort_keys: Sort mapping keys for deterministic encoding.
        """

        self.ensure_ascii = ensure_ascii
        self.sort_keys = sort_keys

    def encode(self, payload: Any, *, queue: str | None = None) -> bytes:
        """Encode a Python object into JSON bytes.

        Args:
            payload: Object to encode. Bytes-like objects are passed through.
            queue: Optional logical queue name for error context.

        Returns:
            Encoded bytes.

        Raises:
            MessageEncodeError: If JSON encoding fails.
        """

        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, (bytearray, memoryview)):
            return bytes(payload)
        try:
            encoded = json.dumps(
                payload,
                ensure_ascii=self.ensure_ascii,
                separators=(",", ":"),
                sort_keys=self.sort_keys,
            )
            return encoded.encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise MessageEncodeError.from_exception(
                exc,
                queue=queue,
                details={"serializer": self.__class__.__name__},
            ) from exc

    def decode(self, payload: bytes, *, queue: str | None = None) -> Any:
        """Decode JSON bytes into a Python object.

        Args:
            payload: Bytes-like object containing UTF-8 JSON.
            queue: Optional logical queue name for error context.

        Returns:
            Decoded Python object.

        Raises:
            MessageDecodeError: If the payload is not bytes-like or cannot be
                decoded as JSON.
        """

        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise MessageDecodeError(
                "serialized payload must be bytes-like",
                action="message.decode",
                queue=queue,
                details={
                    "serializer": self.__class__.__name__,
                    "payload_type": type(payload).__name__,
                },
            )
        try:
            return json.loads(bytes(payload).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MessageDecodeError.from_exception(
                exc,
                queue=queue,
                details={"serializer": self.__class__.__name__},
            ) from exc
