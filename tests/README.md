# RedQueue Test Suite

## Layers

- Unit tests: core models, config, exceptions, serialization, compatibility logic.
- Backend contract tests: shared behavior across List and Streams backends.
- Integration tests: opt-in tests against a real Redis server.
- Async tests: async client and async backend behavior using fake Redis doubles.
- Availability tests: recovery, dead-letter, compatibility, and rollback paths.
- Performance tests: deterministic in-memory regression baselines.
- Real Redis tests: opt-in availability, performance, and concurrency tests.

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

Run availability tests:

```powershell
$env:PYTHONPATH='src'; python -m pytest -m availability
```

Run performance tests:

```powershell
$env:PYTHONPATH='src'; python -m pytest -m performance
```

Run real Redis availability tests:

```powershell
$env:REDQUEUE_REDIS_URL='redis://127.0.0.1:6379/0'
$env:PYTHONPATH='src'; python -m pytest -m "integration and availability"
```

Run real Redis performance tests:

```powershell
$env:REDQUEUE_REDIS_URL='redis://127.0.0.1:6379/0'
$env:PYTHONPATH='src'; python -m pytest -m "integration and performance"
```

Run real Redis concurrency tests:

```powershell
$env:REDQUEUE_REDIS_URL='redis://127.0.0.1:6379/0'
$env:PYTHONPATH='src'; python -m pytest -m "integration and concurrency"
```
