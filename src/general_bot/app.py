import asyncio
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.types import Update, User

from general_bot import handlers
from general_bot.config import config
from general_bot.services import Services
from general_bot.types import Handler, MiddlewareData


def run() -> None:
    asyncio.run(_main())


async def _main() -> None:
    dp = Dispatcher()
    dp['services'] = Services()
    dp.include_router(handlers.router)

    @dp.update.middleware()
    async def enforce_allowlist(handler: Handler, update: Update, data: MiddlewareData) -> Any:
        user: User | None = data.get('event_from_user')
        if user is None or user.id not in config.allowlist:
            return None
        return await handler(update, data)

    async with Bot(config.bot_token) as bot:
        await dp.start_polling(bot, polling_timeout=30)
