import argparse
import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable
from typing import Any

from aiogram import BaseMiddleware, Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import TelegramObject, User
from loguru import logger

from timeline_hub.handlers.router import router as handlers_router
from timeline_hub.infra.s3 import S3Client, S3Config
from timeline_hub.infra.tasks import TaskFailure, TaskScheduler, TaskSupervisor
from timeline_hub.services.clip_store import ClipStore
from timeline_hub.services.container import Services
from timeline_hub.services.message_buffer import ChatMessageBuffer
from timeline_hub.settings import Settings
from timeline_hub.types import UserId


class _AllowlistMiddleware(BaseMiddleware):
    def __init__(self, *, user_ids: set[UserId]) -> None:
        self._user_ids = user_ids

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user: User | None = data.get('event_from_user')
        if user is None:
            return None
        if user.id not in self._user_ids:
            logger.info(
                'User {} (@{} {!r}) attempting to use bot',
                user.id,
                user.username or '',
                user.full_name,
            )
            return None
        return await handler(event, data)


def run() -> None:
    args = _parse_args()
    settings = Settings.load(args.dev)
    _configure_logging()
    asyncio.run(_main(settings))


async def _main(settings: Settings) -> None:
    dp = Dispatcher(storage=MemoryStorage())

    async with (
        Bot(settings.bot_token.get_secret_value()) as bot,
        S3Client(
            S3Config(
                endpoint_url=settings.s3.endpoint_url,
                region=settings.s3.region,
                bucket=settings.s3.bucket,
                access_key_id=settings.s3.access_key_id,
                secret_access_key=settings.s3.secret_access_key.get_secret_value(),
            )
        ) as s3_client,
    ):

        async def on_failure_stop(_: TaskFailure | None = None) -> None:
            await _notify_superusers_and_stop_polling(
                bot=bot,
                dispatcher=dp,
                superuser_ids=settings.superuser_ids,
            )

        dp['services'] = Services(
            chat_message_buffer=ChatMessageBuffer(),
            task_scheduler=TaskScheduler(
                task_supervisor=TaskSupervisor(on_failure=on_failure_stop),
            ),
            clip_store=ClipStore(s3_client),
        )
        dp['settings'] = settings
        dp['on_failure'] = on_failure_stop
        dp.include_router(handlers_router)
        dp.update.middleware(_AllowlistMiddleware(user_ids=settings.user_ids))

        logger.info('Starting bot')
        await dp.start_polling(bot, polling_timeout=30)
        logger.info('Bot stopped')


async def _notify_superusers_and_stop_polling(
    *,
    bot: Bot,
    dispatcher: Dispatcher,
    superuser_ids: set[UserId],
) -> None:
    try:
        for superuser_id in superuser_ids:
            try:
                await bot.send_message(chat_id=superuser_id, text='Stopping bot due to error')
            except Exception:
                logger.exception('Failed to notify superuser {} about shutdown', superuser_id)
    finally:
        await dispatcher.stop_polling()


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
