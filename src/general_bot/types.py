from collections.abc import Awaitable
from typing import Any, Callable

from aiogram.types import TelegramObject

MiddlewareData = dict[str, Any]
Handler = Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]]
UserId = int
