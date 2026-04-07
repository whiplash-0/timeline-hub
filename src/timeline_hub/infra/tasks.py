import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Hashable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from typing import Any, TypeVar

from loguru import logger


@dataclass(frozen=True, slots=True)
class TaskFailure:
    name: str
    exception: BaseException
    context: Mapping[str, Any]


type OnFailure = Callable[[TaskFailure], Awaitable[None]]
TaskResult = TypeVar('TaskResult')

# Function that returns a coroutine when called.
# Example: lambda: send_message(chat_id)
type Job = Callable[[], Awaitable[None]]


class TaskSupervisor:
    """Spawn detached asyncio tasks and fail fast on unhandled exceptions.

    All detached tasks should be created via `spawn()`. The supervisor tracks
    tasks, retrieves exceptions as soon as tasks finish, logs failures with
    context, and invokes `on_failure` once for the first non-cancellation error.
    """

    def __init__(self, on_failure: OnFailure | None = None) -> None:
        self._tasks: set[asyncio.Task[Any]] = set()
        self._on_failure = on_failure
        self._failure_triggered = False

    def spawn(
        self,
        coro: Coroutine[Any, Any, TaskResult],
        *,
        name: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> asyncio.Task[TaskResult]:
        task = asyncio.create_task(coro, name=name)
        task_name = name or task.get_name()
        task_context = dict(context or {})
        self._tasks.add(task)
        task.add_done_callback(partial(self._on_done, name=task_name, context=task_context))
        return task

    def cancel_all(self) -> None:
        for task in tuple(self._tasks):
            task.cancel()

    async def wait(self) -> None:
        """Wait for currently tracked tasks during shutdown.

        This does not disable fail-fast behavior: task failures are handled in
        `_on_done()` as soon as a task completes. `wait()` only collects
        completion to avoid pending tasks during teardown.
        """
        if not self._tasks:
            return
        await asyncio.gather(*tuple(self._tasks), return_exceptions=True)

    def _on_done(self, task: asyncio.Task[Any], *, name: str, context: Mapping[str, Any]) -> None:
        self._tasks.discard(task)

        if task.cancelled():
            return

        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return

        if exception is None:
            return

        logger.bind(task=name, **context).opt(exception=exception).error('Detached task failed')

        if self._failure_triggered or self._on_failure is None:
            return

        self._failure_triggered = True
        asyncio.create_task(self._run_failure_hook(TaskFailure(name=name, exception=exception, context=context)))

    async def _run_failure_hook(self, failure: TaskFailure) -> None:
        if self._on_failure is None:
            return
        try:
            await self._on_failure(failure)
        except Exception:
            logger.exception('Detached task failure hook failed')


class TaskScheduler:
    """Per-key delayed task scheduler with debounce semantics.

    Each key may have at most one pending timer. Calling `schedule()` cancels
    the previous timer and schedules `job()` to run after `delay`.

    If scheduling occurs again before the delay elapses, the previous timer is
    discarded and only the most recent job will run.

    Once the job starts executing it is shielded from cancellation and allowed
    to run to completion.
    """

    def __init__(self, task_supervisor: TaskSupervisor) -> None:
        self._tasks: dict[Hashable, asyncio.Task[None]] = {}
        self._generation: dict[Hashable, int] = {}
        self._task_supervisor = task_supervisor

    def schedule(self, job: Job, *, key: Hashable, delay: timedelta) -> None:
        self.cancel(key)
        self._generation[key] = self._generation.get(key, 0) + 1
        self._tasks[key] = self._task_supervisor.spawn(
            self._delayed(key, job, self._generation[key], delay),
        )

    def cancel(self, key: Hashable) -> None:
        if task := self._tasks.pop(key, None):
            task.cancel()

    async def _delayed(self, key: Hashable, job: Job, generation: int, delay: timedelta) -> None:
        try:
            await asyncio.sleep(delay.total_seconds())
        except asyncio.CancelledError:
            return
        if self._generation.get(key) != generation:
            return

        # Once real task started, it can't be canceled. So remove it from scheduler
        _ = self._tasks.pop(key, None)
        try:
            await asyncio.shield(job())
        except asyncio.CancelledError:
            return
