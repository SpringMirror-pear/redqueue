# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Redis connection pool and resource management helpers."""

from __future__ import annotations

from types import TracebackType
from typing import Any

from redis import ConnectionPool, Redis
from redis.asyncio import ConnectionPool as AsyncConnectionPool
from redis.asyncio import Redis as AsyncRedis


class RedisConnectionManager:
    """Owns a synchronous Redis connection pool and creates Redis clients.

    The manager is useful when an application wants multiple ``QueueClient``
    instances to share one pool while keeping resource shutdown explicit.

    Attributes:
        url: Redis connection URL used to create the pool.
        pool: Underlying redis-py ``ConnectionPool``.
    """

    def __init__(self, url: str, **pool_options: Any) -> None:
        """Create a synchronous connection manager.

        Args:
            url: Redis URL accepted by ``redis.ConnectionPool.from_url``.
            **pool_options: Additional pool options such as ``max_connections``,
                ``socket_timeout``, or ``health_check_interval``.
        """

        self.url = url
        self.pool = ConnectionPool.from_url(url, **pool_options)
        self._closed = False

    def redis(self) -> Redis:
        """Create a Redis client backed by the managed pool.

        Returns:
            ``redis.Redis`` bound to this manager's connection pool.

        Raises:
            RuntimeError: If the manager has already been closed.
        """

        if self._closed:
            raise RuntimeError("RedisConnectionManager is closed")
        return Redis(connection_pool=self.pool)

    def close(self) -> None:
        """Disconnect all connections owned by this manager."""

        if self._closed:
            return
        self.pool.disconnect()
        self._closed = True

    def __enter__(self) -> RedisConnectionManager:
        """Enter a synchronous resource-management context."""

        if self._closed:
            raise RuntimeError("RedisConnectionManager is closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the pool when leaving a context manager."""

        self.close()


class AsyncRedisConnectionManager:
    """Owns an asynchronous Redis connection pool and creates async clients."""

    def __init__(self, url: str, **pool_options: Any) -> None:
        """Create an asynchronous connection manager.

        Args:
            url: Redis URL accepted by ``redis.asyncio.ConnectionPool.from_url``.
            **pool_options: Additional async pool options.
        """

        self.url = url
        self.pool = AsyncConnectionPool.from_url(url, **pool_options)
        self._closed = False

    def redis(self) -> AsyncRedis:
        """Create an async Redis client backed by the managed pool.

        Returns:
            ``redis.asyncio.Redis`` bound to this manager's connection pool.

        Raises:
            RuntimeError: If the manager has already been closed.
        """

        if self._closed:
            raise RuntimeError("AsyncRedisConnectionManager is closed")
        return AsyncRedis(connection_pool=self.pool)

    async def close(self) -> None:
        """Disconnect all async connections owned by this manager."""

        if self._closed:
            return
        await self.pool.disconnect()
        self._closed = True

    async def __aenter__(self) -> AsyncRedisConnectionManager:
        """Enter an asynchronous resource-management context."""

        if self._closed:
            raise RuntimeError("AsyncRedisConnectionManager is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the pool when leaving an async context manager."""

        await self.close()
