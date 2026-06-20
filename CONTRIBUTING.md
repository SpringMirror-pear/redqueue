# Contributing to RedQueue

Thank you for contributing to RedQueue. This project favors small, well-tested
changes that preserve Redis compatibility and clear resource ownership.

## Branch Model

RedQueue uses a lightweight Git Flow model:

- `main`: stable release branch. Release branches and urgent hotfixes merge
  here.
- `develop`: integration branch for the next minor release.
- `feature/<name>`: feature branches created from `develop` and merged back to
  `develop`.
- `release/<minor>`: release stabilization branches such as `release/0.11`.
- `hotfix/<version>`: urgent patch branches created from `main`, then merged
  back to both `main` and `develop`.

## Workflow

1. Create feature branches from `develop`.
2. Keep changes focused and avoid unrelated refactors.
3. Add or update tests for behavioral changes.
4. Update documentation when public APIs or workflows change.
5. Open a pull request into `develop` for regular work, or into the active
   `release/<minor>` branch for release stabilization fixes.

## Versioning

- Development versions use `0.1x.xdevN`.
- Formal releases use `0.1x.x`.
- Development and formal release version streams are independent.
- Patch releases fix defects.
- Minor releases may add compatible features.

## Quality Checks

Run the local quality gate before opening a pull request:

```bash
PYTHONPATH=src python scripts/check.py
```

Run Redis integration tests when Redis behavior changes:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```

Run real Redis availability and performance tests when queue reliability or
connection behavior changes:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and availability"
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m "integration and performance"
```

## Python Source Requirements

Every Python source file must include:

```python
# SPDX-License-Identifier: Apache-2.0
# Author: SpringMirror-pear
```

Use Google-style docstrings for public classes, functions, and non-trivial
internal helpers.

## Compatibility Expectations

- Python runtime support starts at `>=3.9`.
- Redis Streams require Redis `>=5.0`.
- Redis 5.x must continue to use `XPENDING`/`XCLAIM` fallback for pending
  recovery.
- Redis `>=6.2` may use `XAUTOCLAIM` and `BLMOVE`.
- Keep sync and async APIs aligned unless there is a documented reason not to.

## Pull Request Checklist

- Tests cover the changed behavior.
- `ruff`, `mypy`, and `pytest` pass.
- Public docs are updated when public behavior changes.
- Changelog entries are added for release branches.
- No credentials, tokens, local Redis data, or build artifacts are committed.
