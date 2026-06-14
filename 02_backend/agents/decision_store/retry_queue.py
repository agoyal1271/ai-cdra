"""
Simple in-memory retry queue for async tasks.

Tasks that fail (e.g., PagerDuty, Slack) are queued for retry.
Agents check the queue periodically and retry with exponential backoff.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class RetryTask:
    """Task queued for retry."""
    task_id: str
    func: Callable  # async function to retry
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    attempt: int = 0
    max_attempts: int = 3
    next_retry: datetime = field(default_factory=datetime.utcnow)
    error_log: list = field(default_factory=list)

    async def execute(self) -> bool:
        """Execute the task. Returns True if successful, False if should retry."""
        try:
            await self.func(*self.args, **self.kwargs)
            logger.info(f"[RETRY] Task {self.task_id} succeeded on attempt {self.attempt + 1}")
            return True
        except Exception as e:
            self.error_log.append(str(e))
            self.attempt += 1
            if self.attempt < self.max_attempts:
                # Exponential backoff: 2s, 4s, 8s
                backoff = 2 ** self.attempt
                self.next_retry = datetime.utcnow() + timedelta(seconds=backoff)
                logger.warning(f"[RETRY] Task {self.task_id} failed (attempt {self.attempt}/{self.max_attempts}). Retrying in {backoff}s. Error: {e}")
                return False
            else:
                logger.error(f"[RETRY] Task {self.task_id} failed after {self.max_attempts} attempts. Giving up.")
                return False


class RetryQueue:
    """In-memory queue for tasks that need retrying."""

    def __init__(self):
        self.tasks: dict[str, RetryTask] = {}
        self._lock = asyncio.Lock()

    async def enqueue(
        self,
        task_id: str,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
    ) -> None:
        """Add a task to the retry queue."""
        async with self._lock:
            task = RetryTask(
                task_id=task_id,
                func=func,
                args=args,
                kwargs=kwargs or {},
            )
            self.tasks[task_id] = task
            logger.info(f"[RETRY] Enqueued task: {task_id}")

    async def process_ready(self) -> None:
        """Process all tasks that are ready to retry."""
        async with self._lock:
            ready = [
                (tid, task) for tid, task in self.tasks.items()
                if task.next_retry <= datetime.utcnow() and task.attempt < task.max_attempts
            ]

        for task_id, task in ready:
            success = await task.execute()
            if success:
                async with self._lock:
                    del self.tasks[task_id]

    async def get_stats(self) -> dict:
        """Get stats about queued tasks."""
        async with self._lock:
            return {
                "total": len(self.tasks),
                "pending": sum(1 for t in self.tasks.values() if datetime.utcnow() < t.next_retry),
                "ready": sum(1 for t in self.tasks.values() if datetime.utcnow() >= t.next_retry),
            }

    async def cleanup_failed(self) -> int:
        """Remove tasks that have exhausted retries."""
        async with self._lock:
            failed = [tid for tid, task in self.tasks.items() if task.attempt >= task.max_attempts]
            for tid in failed:
                del self.tasks[tid]
            if failed:
                logger.info(f"[RETRY] Cleaned up {len(failed)} failed tasks")
            return len(failed)


# Global retry queue
retry_queue = RetryQueue()
