# RedQueue Test Suite

## Layers

- Unit tests: core models, config, exceptions, serialization, compatibility logic.
- Backend contract tests: shared behavior across List and Streams backends.
- Integration tests: opt-in tests against a real Redis server.
- Async tests: async client and async backend behavior using fake Redis doubles.

## Commands

```powershell
$env:PYTHONPATH='src'; python -m pytest
python -m ruff check .
```

Run real Redis integration tests:

```powershell
$env:REDQUEUE_REDIS_URL='redis://localhost:6379/0'
$env:PYTHONPATH='src'; python -m pytest -m integration
```
