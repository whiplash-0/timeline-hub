import argparse
import asyncio
import logging
import sys
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.types import Update, User
from loguru import logger

from general_bot import handlers
from general_bot.services import Services
from general_bot.settings import Settings
from general_bot.types import Data, Handler


def run() -> None:
    args = _parse_args()
    settings = Settings.load(args.dev)
    _configure_logging()
    asyncio.run(_main(settings))


async def _main(settings: Settings) -> None:
    dp = Dispatcher()
    dp['services'] = Services()
    dp['settings'] = settings
    dp.include_router(handlers.router)

    @dp.update.middleware()
    async def enforce_allowlist(handler: Handler, update: Update, data: Data) -> Any:
        user: User | None = data.get('event_from_user')
        if user is None:
            return None
        if user.id not in settings.allowlist:
            logger.info('User {} (@{} {!r}) attempting to use bot', user.id, user.username or '', user.full_name)
            return None
        return await handler(update, data)

    async with Bot(settings.bot_token) as bot:
        logger.info('Starting bot polling')
        await dp.start_polling(bot, polling_timeout=30)
        logger.info('Bot polling stopped')


def _configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        format='{message}',
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )
    # Hide normal 'SIGINT` signal logs when shutting bot down
    logging.getLogger('aiogram').setLevel(logging.ERROR)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dev',
        action='store_true',
        help='Run bot in development mode',
    )
    return parser.parse_args()
