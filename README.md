# tiny-worker

**Zero-dependency background task queue for Python.**

Like Celery, but in one file. No Redis, no broker, no setup — just `import` and run.

## Features

| Feature | Description |
|---------|-------------|
| **Thread Pool** | Configurable worker threads with automatic lifecycle |
| **Retry** | Exponential backoff with configurable max retries |
| **Rate Limiting** | Token bucket rate limiter for controlled throughput |
| **Timeout** | Per-task timeout with automatic cancellation |
| **Callbacks** | `on_complete` and `on_error` hooks |
| **Batch** | Submit multiple tasks with `.map()` |
| **Stats** | Track submitted, completed, failed, retried counts |
| **Context Manager** | `with Worker() as w:` auto-start/shutdown |
| **Zero Deps** | Pure Python stdlib only |

## Install

```bash
pip install tiny-worker
```

Or copy `tiny_worker.py` — it's a single file.

## Quick Start

```python
from tiny_worker import Worker

with Worker(max_workers=4) as w:
    # Submit individual tasks
    task = w.submit(sum, range(1000))
    result = task.wait()
    print(result)  # 499500

    # Batch submit
    tasks = w.map(lambda x: x * x, [1, 2, 3, 4, 5])
    for t in tasks:
        print(t.wait())
```

## Retry Policy

```python
from tiny_worker import Worker, RetryPolicy

retry = RetryPolicy(
    max_retries=5,
    delay=1.0,      # Start with 1 second
    backoff=2.0,    # Double each time: 1, 2, 4, 8...
    max_delay=60.0,
    exceptions=(ConnectionError, TimeoutError),
)

with Worker(default_retry=retry) as w:
    task = w.submit(fetch_data, "https://api.example.com")
    result = task.wait()
```

## Rate Limiting

```python
from tiny_worker import Worker, RateLimiter

limiter = RateLimiter(rate=10.0, burst=5)  # 10/sec

with Worker(rate_limiter=limiter) as w:
    for url in urls:
        w.submit(fetch, url)  # Auto-throttled
```

## Callbacks

```python
w = Worker()

@w.on_complete
def log_done(task):
    print(f"✅ {task.fn.__name__} done in {task.duration:.2f}s")

@w.on_error
def log_error(task, error):
    print(f"❌ {task.fn.__name__} failed: {error}")

w.start()
# ... submit tasks ...
w.shutdown()
```

## Context Manager

```python
from tiny_worker import Worker

with Worker(max_workers=8) as w:
    tasks = [w.submit(process, item) for item in items]
    results = [t.wait() for t in tasks]
# Auto-shutdown when exiting context
```

## API

| Method | Description |
|--------|-------------|
| `submit(fn, *args, **kwargs)` | Submit a task to the queue |
| `map(fn, items)` | Submit multiple tasks |
| `start()` | Start worker threads |
| `shutdown(wait=True)` | Stop accepting work, wait for completion |
| `on_complete(fn)` | Decorator for completion callbacks |
| `on_error(fn)` | Decorator for error callbacks |
| `stats()` | Return dict of worker statistics |

## License

MIT
