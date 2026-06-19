# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Redis-backed message queue library."""

from redqueue._version import __version__
from redqueue.async_client import AsyncQueueClient
from redqueue.client import QueueClient
from redqueue.compat import (
    RedisCapabilities,
    RedisVersion,
    detect_capabilities,
    detect_capabilities_async,
    extract_redis_version,
)
from redqueue.config import BackendType, QueueConfig, RetryConfig
from redqueue.exceptions import (
    AckError,
    BackendUnavailableError,
    ErrorContext,
    MessageDecodeError,
    MessageEncodeError,
    QueueConfigError,
    RedisCompatibilityError,
    RedQueueError,
    RetryExceededError,
)
from redqueue.message import Message, new_message_id
from redqueue.monitoring import (
    CompositeMonitoringHook,
    InMemoryMonitoringHook,
    MonitoringEvent,
    MonitoringEventType,
    MonitoringHook,
    NoopMonitoringHook,
    SafeMonitoringHook,
)
from redqueue.serialization import JsonSerializer, Serializer

__all__ = [
    "__version__",
    "AckError",
    "AsyncQueueClient",
    "BackendType",
    "BackendUnavailableError",
    "ErrorContext",
    "CompositeMonitoringHook",
    "InMemoryMonitoringHook",
    "JsonSerializer",
    "Message",
    "MessageDecodeError",
    "MessageEncodeError",
    "MonitoringEvent",
    "MonitoringEventType",
    "MonitoringHook",
    "NoopMonitoringHook",
    "QueueClient",
    "QueueConfig",
    "QueueConfigError",
    "RedisCapabilities",
    "RedQueueError",
    "RedisCompatibilityError",
    "RedisVersion",
    "RetryConfig",
    "RetryExceededError",
    "SafeMonitoringHook",
    "Serializer",
    "detect_capabilities",
    "detect_capabilities_async",
    "extract_redis_version",
    "new_message_id",
]
