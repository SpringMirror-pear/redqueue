# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Redis version and capability detection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from redqueue.exceptions import BackendUnavailableError, RedisCompatibilityError


@runtime_checkable
class RedisInfoClient(Protocol):
    """Protocol for sync Redis clients that expose INFO."""

    def info(self, section: str | None = None) -> Mapping[str, Any]:
        """Return Redis INFO fields."""


@runtime_checkable
class AsyncRedisInfoClient(Protocol):
    """Protocol for async Redis clients that expose INFO."""

    async def info(self, section: str | None = None) -> Mapping[str, Any]:
        """Return Redis INFO fields."""


@dataclass(frozen=True, order=True)
class RedisVersion:
    """Comparable Redis semantic version subset."""

    major: int
    minor: int
    patch: int = 0

    @classmethod
    def parse(cls, value: str) -> RedisVersion:
        version_text = value.strip()
        release_text = version_text.split("-", 1)[0]
        parts = release_text.split(".")
        if len(parts) < 2:
            raise ValueError(f"invalid Redis version: {value!r}")
        try:
            major = cls._parse_part(parts[0], value)
            minor = cls._parse_part(parts[1], value)
            patch = cls._parse_part(parts[2], value) if len(parts) > 2 else 0
        except ValueError as exc:
            raise ValueError(f"invalid Redis version: {value!r}") from exc
        return cls(major=major, minor=minor, patch=patch)

    @staticmethod
    def _parse_part(part: str, original: str) -> int:
        digits = ""
        for char in part:
            if char.isdigit():
                digits += char
                continue
            break
        if not digits:
            raise ValueError(f"invalid Redis version: {original!r}")
        return int(digits)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class RedisCapabilities:
    """Feature flags derived from Redis server version."""

    version: RedisVersion

    @property
    def supports_list_blocking(self) -> bool:
        return self.version >= RedisVersion(2, 0, 0)

    @property
    def supports_list_reliable_brpoplpush(self) -> bool:
        return self.version >= RedisVersion(2, 2, 0)

    @property
    def supports_list_reliable_blmove(self) -> bool:
        return self.version >= RedisVersion(6, 2, 0)

    @property
    def supports_streams(self) -> bool:
        return self.version >= RedisVersion(5, 0, 0)

    @property
    def supports_streams_auto_claim(self) -> bool:
        return self.version >= RedisVersion(6, 2, 0)

    @property
    def supports_delay_sorted_set(self) -> bool:
        return self.version >= RedisVersion(1, 2, 0)

    def require_streams(self) -> None:
        if not self.supports_streams:
            raise RedisCompatibilityError.for_feature(
                "Streams backend",
                current_version=self.version,
                required_version="5.0.0",
                action="redis.require_streams",
            )

    def require_list_reliable(self) -> None:
        if not self.supports_list_reliable_brpoplpush:
            raise RedisCompatibilityError.for_feature(
                "List reliable backend",
                current_version=self.version,
                required_version="2.2.0",
                action="redis.require_list_reliable",
            )

    def require_delay_sorted_set(self) -> None:
        if not self.supports_delay_sorted_set:
            raise RedisCompatibilityError.for_feature(
                "Delayed task backend",
                current_version=self.version,
                required_version="1.2.0",
                action="redis.require_delay_sorted_set",
            )

    @classmethod
    def from_info(cls, info: Mapping[str, Any]) -> RedisCapabilities:
        """Build capabilities from Redis INFO output."""

        version = extract_redis_version(info)
        return cls(version=version)


def extract_redis_version(info: Mapping[str, Any]) -> RedisVersion:
    """Extract and parse redis_version from INFO server output."""

    try:
        version_value = info["redis_version"]
    except KeyError as exc:
        raise BackendUnavailableError(
            "Redis INFO server response does not contain redis_version",
            action="redis.info",
        ) from exc
    if isinstance(version_value, bytes):
        version_text = version_value.decode("ascii")
    else:
        version_text = str(version_value)
    try:
        return RedisVersion.parse(version_text)
    except ValueError as exc:
        raise BackendUnavailableError(
            f"Redis INFO server returned invalid redis_version {version_text!r}",
            action="redis.info",
            details={"redis_version": version_text},
        ) from exc


def detect_capabilities(client: RedisInfoClient) -> RedisCapabilities:
    """Detect Redis capabilities using a synchronous Redis client."""

    try:
        info = client.info("server")
    except Exception as exc:
        raise BackendUnavailableError(
            "Failed to read Redis INFO server",
            action="redis.info",
        ) from exc
    return RedisCapabilities.from_info(info)


async def detect_capabilities_async(
    client: AsyncRedisInfoClient,
) -> RedisCapabilities:
    """Detect Redis capabilities using an asynchronous Redis client."""

    try:
        info = await client.info("server")
    except Exception as exc:
        raise BackendUnavailableError(
            "Failed to read Redis INFO server",
            action="redis.info",
        ) from exc
    return RedisCapabilities.from_info(info)
