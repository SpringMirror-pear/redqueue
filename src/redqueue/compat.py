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
        """Return Redis INFO fields.

        Args:
            section: Optional Redis INFO section name.

        Returns:
            Mapping containing INFO fields such as ``redis_version``.
        """


@runtime_checkable
class AsyncRedisInfoClient(Protocol):
    """Protocol for async Redis clients that expose INFO."""

    async def info(self, section: str | None = None) -> Mapping[str, Any]:
        """Return Redis INFO fields.

        Args:
            section: Optional Redis INFO section name.

        Returns:
            Mapping containing INFO fields such as ``redis_version``.
        """


@dataclass(frozen=True, order=True)
class RedisVersion:
    """Comparable Redis semantic version subset.

    Attributes:
        major: Redis major version.
        minor: Redis minor version.
        patch: Redis patch version, defaulting to zero when omitted.
    """

    major: int
    minor: int
    patch: int = 0

    @classmethod
    def parse(cls, value: str) -> RedisVersion:
        """Parse a Redis version string.

        Args:
            value: Redis version text, optionally with a release suffix such as
                ``7.2.4-rc1``.

        Returns:
            Parsed ``RedisVersion``.

        Raises:
            ValueError: If the value cannot be parsed as at least
                ``major.minor``.
        """

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
        """Parse one numeric version part.

        Args:
            part: Version segment to parse.
            original: Original version text used in error messages.

        Returns:
            Parsed integer segment.

        Raises:
            ValueError: If the segment does not start with digits.
        """

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
        """Return normalized ``major.minor.patch`` text."""

        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class RedisCapabilities:
    """Feature flags derived from Redis server version.

    Attributes:
        version: Parsed Redis server version.
    """

    version: RedisVersion

    @property
    def supports_list_blocking(self) -> bool:
        """Whether basic blocking List commands are available."""

        return self.version >= RedisVersion(2, 0, 0)

    @property
    def supports_list_reliable_brpoplpush(self) -> bool:
        """Whether reliable List move via ``BRPOPLPUSH`` is available."""

        return self.version >= RedisVersion(2, 2, 0)

    @property
    def supports_list_reliable_blmove(self) -> bool:
        """Whether modern reliable List move via ``BLMOVE`` is available."""

        return self.version >= RedisVersion(6, 2, 0)

    @property
    def supports_streams(self) -> bool:
        """Whether Redis Streams commands are available."""

        return self.version >= RedisVersion(5, 0, 0)

    @property
    def supports_streams_auto_claim(self) -> bool:
        """Whether Streams ``XAUTOCLAIM`` recovery is available."""

        return self.version >= RedisVersion(6, 2, 0)

    @property
    def supports_delay_sorted_set(self) -> bool:
        """Whether Sorted Set commands used by delayed tasks are available."""

        return self.version >= RedisVersion(1, 2, 0)

    def require_streams(self) -> None:
        """Require Redis Streams support.

        Raises:
            RedisCompatibilityError: If Redis is older than 5.0.0.
        """

        if not self.supports_streams:
            raise RedisCompatibilityError.for_feature(
                "Streams backend",
                current_version=self.version,
                required_version="5.0.0",
                action="redis.require_streams",
            )

    def require_list_reliable(self) -> None:
        """Require reliable Redis List move support.

        Raises:
            RedisCompatibilityError: If Redis is older than 2.2.0.
        """

        if not self.supports_list_reliable_brpoplpush:
            raise RedisCompatibilityError.for_feature(
                "List reliable backend",
                current_version=self.version,
                required_version="2.2.0",
                action="redis.require_list_reliable",
            )

    def require_delay_sorted_set(self) -> None:
        """Require Sorted Set support for delayed tasks.

        Raises:
            RedisCompatibilityError: If Redis is older than 1.2.0.
        """

        if not self.supports_delay_sorted_set:
            raise RedisCompatibilityError.for_feature(
                "Delayed task backend",
                current_version=self.version,
                required_version="1.2.0",
                action="redis.require_delay_sorted_set",
            )

    @classmethod
    def from_info(cls, info: Mapping[str, Any]) -> RedisCapabilities:
        """Build capabilities from Redis INFO output.

        Args:
            info: Mapping returned by ``INFO server``.

        Returns:
            Capability set derived from ``redis_version``.
        """

        version = extract_redis_version(info)
        return cls(version=version)


def extract_redis_version(info: Mapping[str, Any]) -> RedisVersion:
    """Extract and parse ``redis_version`` from INFO server output.

    Args:
        info: Mapping returned by Redis ``INFO server``.

    Returns:
        Parsed Redis version.

    Raises:
        BackendUnavailableError: If ``redis_version`` is missing or invalid.
    """

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
    """Detect Redis capabilities using a synchronous Redis client.

    Args:
        client: Redis client exposing ``info("server")``.

    Returns:
        Capability set for the connected Redis server.

    Raises:
        BackendUnavailableError: If Redis INFO cannot be read.
    """

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
    """Detect Redis capabilities using an asynchronous Redis client.

    Args:
        client: Async Redis client exposing ``info("server")``.

    Returns:
        Capability set for the connected Redis server.

    Raises:
        BackendUnavailableError: If Redis INFO cannot be read.
    """

    try:
        info = await client.info("server")
    except Exception as exc:
        raise BackendUnavailableError(
            "Failed to read Redis INFO server",
            action="redis.info",
        ) from exc
    return RedisCapabilities.from_info(info)
