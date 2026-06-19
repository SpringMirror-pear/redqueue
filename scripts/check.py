# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear

"""Run local quality checks for RedQueue."""

import os
import subprocess

COMMANDS = [
    ["python", "-m", "ruff", "check", "."],
    ["python", "-m", "mypy"],
    ["python", "-m", "pytest"],
]


def main() -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    for command in COMMANDS:
        result = subprocess.run(command, env=env, check=False)
        if result.returncode != 0:
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
