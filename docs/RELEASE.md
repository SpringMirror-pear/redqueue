# Release Process / 发布流程

This document describes how RedQueue releases are prepared and published.

本文档描述 RedQueue 的发布准备与发布流程。

## Version Rules / 版本规则

- Development versions use `0.1x.xdevN`.
- Formal releases use `0.1x.x`.
- Development and formal release versions are independent streams.
- `0.10.0dev13` is the last development milestone before the first formal
  release.
- `0.10.0` is the first formal release.

- 开发版本使用 `0.1x.xdevN`。
- 正式版本使用 `0.1x.x`。
- 开发版本与正式版本是独立版本流。
- `0.10.0dev13` 是首个正式版本前的最后开发里程碑。
- `0.10.0` 是首个正式版本。

## Branch Model / 分支模型

- `main`: stable release branch.
- `develop`: integration branch for the next minor release.
- `feature/<name>`: feature work branched from `develop`.
- `release/<minor>`: release stabilization branch, such as `release/0.11`.
- `hotfix/<version>`: urgent patch branch from `main`, merged back to both
  `main` and `develop`.

- `main`：稳定发布分支。
- `develop`：下一个小版本的集成分支。
- `feature/<name>`：从 `develop` 创建的功能开发分支。
- `release/<minor>`：版本稳定分支，例如 `release/0.11`。
- `hotfix/<version>`：从 `main` 创建的紧急补丁分支，并合回 `main` 与
  `develop`。

## Pre-release Checklist / 发布前检查清单

1. Update `pyproject.toml` and `src/redqueue/_version.py`.
2. Update tests that assert `__version__`.
3. Update `README.md`, `CHANGELOG.md`, and `docs/API.md`.
4. Run quality checks:

```bash
PYTHONPATH=src python scripts/check.py
```

5. Run Redis integration tests:

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```

6. Build distributions:

```bash
python -m build
```

7. Validate distributions:

```bash
python -m twine check dist/*
```

发布前检查：

1. 更新 `pyproject.toml` 和 `src/redqueue/_version.py`。
2. 更新断言 `__version__` 的测试。
3. 更新 `README.md`、`CHANGELOG.md` 和 `docs/API.md`。
4. 运行质量检查：

```bash
PYTHONPATH=src python scripts/check.py
```

5. 运行 Redis 集成测试：

```bash
REDQUEUE_REDIS_URL=redis://127.0.0.1:6379/0 PYTHONPATH=src python -m pytest -m integration
```

6. 构建分发包：

```bash
python -m build
```

7. 校验分发包：

```bash
python -m twine check dist/*
```

## Publish to PyPI / 发布到 PyPI

Use a PyPI API token through an environment variable. Do not write tokens to
files or commit them.

通过环境变量使用 PyPI API token。不要将 token 写入文件或提交到仓库。

```bash
python -m twine upload dist/*
```

Required environment:

- `TWINE_USERNAME=__token__`
- `TWINE_PASSWORD=<pypi-api-token>`

所需环境变量：

- `TWINE_USERNAME=__token__`
- `TWINE_PASSWORD=<pypi-api-token>`

## GitHub Release / GitHub 发布

1. Commit release changes.
2. Tag the release:

```bash
git tag v0.10.0
```

3. Push commits and tags:

```bash
git push origin main
git push origin v0.10.0
```

GitHub 发布步骤：

1. 提交发布变更。
2. 创建发布标签：

```bash
git tag v0.10.0
```

3. 推送提交和标签：

```bash
git push origin main
git push origin v0.10.0
```
