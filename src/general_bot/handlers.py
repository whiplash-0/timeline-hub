from aiogram import Router
from aiogram.types import Message

from general_bot.config import config
from general_bot.services import Services

router = Router()


@router.message()
async def on_message_debounce(message: Message, services: Services) -> None:
    if message.from_user is None:
        return

    user = message.from_user
    services.message_buffer.append(message, user=user)

    async def check_and_reply() -> None:
        messages = services.message_buffer.get(user)
        video_messages = [m for m in messages if m.video is not None]

        if not video_messages:
            await message.answer('No videos found')
            return

        await message.answer(f'Found {len(video_messages)} videos')

    services.task_scheduler.schedule(
        check_and_reply,
        user=user,
        delay=config.forward_batch_timeout,
    )
