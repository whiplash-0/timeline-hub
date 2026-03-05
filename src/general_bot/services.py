import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta

from aiogram.types import Message, User

from general_bot.types import UserId

# Function that returns a coroutine when called.
# Example: lambda: send_message(user_id)
Job = Callable[[], Awaitable[None]]
Messages = list[Message]


class TaskScheduler:
    """
    Per-user delayed task scheduler with debounce semantics.

    Each user can have at most one pending timer. Calling `schedule()` cancels
    the previous timer and starts a new one that will run `job()` after `delay`.
    This is typically used to trigger work only after a period of inactivity
    (e.g. buffering incoming messages and acting once the user stops sending).

    A generation counter is used to prevent stale timers from executing.
    Cancellation in asyncio is cooperative and may race with the completion of
    `asyncio.sleep()`: a timer that was just canceled can still wake up and
    continue execution. To guard against this, each scheduled timer captures
    the current generation number for the user. When the timer wakes up, it
    compares the stored generation with the latest one; if they differ, the
    timer is obsolete and exits without running the job.

    Once the delay has elapsed and the timer is confirmed to be current, the
    job is executed under `asyncio.shield()`. This ensures that rescheduling or
    cancelling the timer does not interrupt the job after it has started. In
    other words:
        - while the delay is pending, the timer may be freely canceled
        - once the job starts, it is allowed to run to completion

    Only `CancelledError` is suppressed. Any other exception raised by `job()`
    propagates normally.
    """

    def __init__(self) -> None:
        self._tasks: dict[UserId, asyncio.Task[None]] = {}
        self._generation: dict[UserId, int] = {}

    def schedule(self, job: Job, *, user: User, delay: timedelta) -> None:
        self.cancel(user)
        self._generation[user.id] = self._generation.get(user.id, 0) + 1
        self._tasks[user.id] = asyncio.create_task(
            self._delayed(user.id, job, self._generation[user.id], delay)
        )

    def cancel(self, user: User) -> None:
        if task := self._tasks.pop(user.id, None):
            task.cancel()

    async def _delayed(self, user_id: UserId, job: Job, generation: int, delay: timedelta) -> None:
        try:
            await asyncio.sleep(delay.total_seconds())
        except asyncio.CancelledError:
            return
        if self._generation.get(user_id) != generation:
            return

        # noinspection PyAsyncCall
        self._tasks.pop(user_id, None)
        try:
            await asyncio.shield(job())
        except asyncio.CancelledError:
            return


class MessageBuffer:
    def __init__(self) -> None:
        self._messages: dict[UserId, Messages] = {}

    def append(self, message: Message, *, user: User) -> None:
        self._messages.setdefault(user.id, []).append(message)

    def get(self, user: User) -> Messages:
        return self._messages.get(user.id, [])

    def flush(self, user: User) -> Messages:
        return self._messages.pop(user.id, [])


@dataclass(frozen=True, slots=True)
class Services:
    task_scheduler: TaskScheduler = field(default_factory=TaskScheduler)
    message_buffer: MessageBuffer = field(default_factory=MessageBuffer)
