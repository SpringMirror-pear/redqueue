# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Serialization protocol and default codecs."""

from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable

from redqueue.exceptions import MessageDecodeError, MessageEncodeError


@runtime_checkable
class Serializer(Protocol):
    """Protocol implemented by RedQueue payload serializers."""

    content_type: str

    def encode(self, payload: Any, *, queue: str | None = None) -> bytes:
        """Encode a Python payload into Redis-safe bytes."""

    def decode(self, payload: bytes, *, queue: str | None = None) -> Any:
        """Decode Redis bytes into a Python payload."""


class JsonSerializer:
    """JSON serializer used by default."""

    content_type = "application/json"

    def __init__(
        self,
        *,
        ensure_ascii: bool = False,
        sort_keys: bool = True,
    ) -> None:
        self.ensure_ascii = ensure_ascii
        self.sort_keys = sort_keys

    def encode(self, payload: Any, *, queue: str | None = None) -> bytes:
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
