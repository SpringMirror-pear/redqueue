# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Shared helpers for RedQueue examples."""

from __future__ import annotations

import os

REDIS_URL = os.getenv("REDQUEUE_REDIS_URL", "redis://127.0.0.1:6379/0")


def example_queue(name: str) -> str:
    """Return a namespaced example queue name."""

    return f"redqueue-example-{name}"
