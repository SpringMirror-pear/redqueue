# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Redis compatibility detection example."""

from __future__ import annotations

from common import REDIS_URL, example_queue
from redis import Redis

from redqueue import (
    QueueClient,
    QueueConfig,
    RedisCapabilities,
    RedisCompatibilityError,
    RedisVersion,
    detect_capabilities,
)


def main() -> None:
    redis = Redis.from_url(REDIS_URL)
    capabilities = detect_capabilities(redis)
    print(f"redis_version={capabilities.version}")
    print(f"supports_streams={capabilities.supports_streams}")
    print(f"supports_blmove={capabilities.supports_list_reliable_blmove}")
    print(f"supports_xautoclaim={capabilities.supports_streams_auto_claim}")

    try:
        QueueClient(
            QueueConfig(queue=example_queue("compat"), backend="stream"),
            redis=redis,
            capabilities=RedisCapabilities(RedisVersion(4, 0, 14)),
        )
    except RedisCompatibilityError as exc:
        print(exc.to_dict())


if __name__ == "__main__":
    main()
