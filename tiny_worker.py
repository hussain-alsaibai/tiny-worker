"""tiny-worker — Zero-dependency background task queue for Python.

Single file. No dependencies. Production-ready.
"""

import threading
import queue
import time
import uuid
import functools
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Union

__version__ = "1.0.0"
__all__ = ["Worker", "Task", "RetryPolicy", "RateLimiter"]


class RetryPolicy:
    """Configure retry behavior for failed tasks."""

    def __init__(
        self,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        max_delay: float = 60.0,
        exceptions: tuple = (Exception,),
    ):
        self.max_retries = max_retries
        self.delay = delay
        self.backoff = backoff
        self.max_delay = max_delay
        self.exceptions = exceptions

    def should_retry(self, attempt: int, error: Exception) -> bool:
        if attempt >= self.max_retries:
            return False
        return isinstance(error, self.exceptions)

    def delay_for(self, attempt: int) -> float:
        d = self.delay * (self.backoff ** (attempt - 1))
        return min(d, self.max_delay)


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float = 10.0, burst: int = 5):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait(self, tokens: int = 1) -> None:
        while not self.acquire(tokens):
            time.sleep(0.01)


class Task:
    """Represents a unit of work."""

    _id_counter = 0
    _lock = threading.Lock()

    def __init__(
        self,
        fn: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        retry: Optional[RetryPolicy] = None,
        timeout: Optional[float] = None,
        metadata: Optional[dict] = None,
    ):
        with Task._lock:
            Task._id_counter += 1
            counter = Task._id_counter
        self.id = f"{uuid.uuid4().hex[:8]}-{counter}"
        self.fn = fn
        self.args = args
        self.kwargs = kwargs or {}
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata or {}
        self.status = "pending"  # pending, running, done, failed, cancelled
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.attempts = 0
        self.created_at = time.monotonic()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self._event = threading.Event()

    def wait(self, timeout: Optional[float] = None) -> Any:
        if not self._event.wait(timeout):
            raise TimeoutError(f"Task {self.id} timed out")
        if self.error and self.status == "failed":
            raise self.error
        return self.result

    @property
    def duration(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        if self.started_at:
            return time.monotonic() - self.started_at
        return None

    def __repr__(self) -> str:
        return f"<Task {self.id} status={self.status}>"


class Worker:
    """Background task queue with thread pool."""

    def __init__(
        self,
        max_workers: int = 4,
        max_queue: int = 1000,
        default_timeout: Optional[float] = None,
        default_retry: Optional[RetryPolicy] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.max_workers = max_workers
        self.max_queue = max_queue
        self.default_timeout = default_timeout
        self.default_retry = default_retry
        self.rate_limiter = rate_limiter

        self._queue: queue.Queue[Task] = queue.Queue(maxsize=max_queue)
        self._workers: List[threading.Thread] = []
        self._shutdown = False
        self._lock = threading.Lock()
        self._stats = defaultdict(int)

        # Task registry
        self._tasks: Dict[str, Task] = {}
        self._tasks_lock = threading.Lock()

        # Callbacks
        self._on_complete: List[Callable[[Task], None]] = []
        self._on_error: List[Callable[[Task, Exception], None]] = []

    def _worker_loop(self) -> None:
        while not self._shutdown:
            try:
                task = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if task.status == "cancelled":
                self._queue.task_done()
                continue

            self._execute_task(task)
            self._queue.task_done()

    def _execute_task(self, task: Task) -> None:
        task.status = "running"
        task.started_at = time.monotonic()
        task.attempts += 1

        try:
            if self.rate_limiter:
                self.rate_limiter.wait()

            if task.timeout:
                # Simple timeout via threading.Timer
                result = [None]
                error = [None]
                done = threading.Event()

                def run():
                    try:
                        result[0] = task.fn(*task.args, **task.kwargs)
                    except Exception as e:
                        error[0] = e
                    finally:
                        done.set()

                t = threading.Thread(target=run)
                t.start()
                if not done.wait(timeout=task.timeout):
                    raise TimeoutError(f"Task {task.id} exceeded {task.timeout}s")
                if error[0]:
                    raise error[0]
                task.result = result[0]
            else:
                task.result = task.fn(*task.args, **task.kwargs)

            task.status = "done"
            task.completed_at = time.monotonic()
            self._stats["completed"] += 1

            for cb in self._on_complete:
                try:
                    cb(task)
                except Exception:
                    pass

        except Exception as e:
            task.error = e
            retry = task.retry or self.default_retry

            if retry and retry.should_retry(task.attempts, e):
                task.status = "pending"
                delay = retry.delay_for(task.attempts)
                time.sleep(delay)
                self._queue.put(task)
                self._stats["retried"] += 1
                return

            task.status = "failed"
            task.completed_at = time.monotonic()
            self._stats["failed"] += 1

            for cb in self._on_error:
                try:
                    cb(task, e)
                except Exception:
                    pass

        finally:
            task._event.set()

    def submit(
        self,
        fn: Callable,
        *args,
        **kwargs,
    ) -> Task:
        """Submit a task to the queue."""
        task = Task(
            fn=fn,
            args=args,
            kwargs=kwargs,
            retry=kwargs.pop("retry", self.default_retry),
            timeout=kwargs.pop("timeout", self.default_timeout),
        )

        with self._tasks_lock:
            self._tasks[task.id] = task

        self._queue.put(task)
        self._stats["submitted"] += 1
        return task

    def map(
        self,
        fn: Callable,
        items: List[Any],
        **kwargs,
    ) -> List[Task]:
        """Submit multiple tasks and return them."""
        return [self.submit(fn, item, **kwargs) for item in items]

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """Stop accepting new work and optionally wait for completion."""
        self._shutdown = True
        if wait:
            self._queue.join()
        for w in self._workers:
            w.join(timeout=timeout)

    def start(self) -> "Worker":
        """Start worker threads. Returns self for chaining."""
        for _ in range(self.max_workers):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self._workers.append(t)
        return self

    def on_complete(self, fn: Callable[[Task], None]) -> Callable:
        """Decorator or direct registration for completion callbacks."""
        self._on_complete.append(fn)
        return fn

    def on_error(self, fn: Callable[[Task, Exception], None]) -> Callable:
        """Decorator or direct registration for error callbacks."""
        self._on_error.append(fn)
        return fn

    def stats(self) -> dict:
        """Return worker statistics."""
        return dict(self._stats)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.shutdown()

    def __repr__(self) -> str:
        return f"<Worker workers={len(self._workers)} queue={self._queue.qsize()}>"


# Convenience decorators
def task(fn: Callable) -> Callable:
    """Decorator to mark a function as a task factory."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)
    return wrapper
