from enum import StrEnum, auto
from textwrap import dedent

from aiogram import Router
from aiogram.filters.callback_data import CallbackData
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from general_bot.config import config
from general_bot.services import Services

router = Router()


class VideoAction(StrEnum):
    NORMALIZE = auto()


class VideoCallback(CallbackData, prefix='video'):
    action: VideoAction


@router.message()
async def on_message_buffer_and_schedule_action_selection(message: Message, services: Services) -> None:
    # Channel or chat may also send a message
    if message.from_user is None:
        return

    user = message.from_user
    services.message_buffer.append(message, user=user)

    async def send_action_selection() -> None:
        messages = services.message_buffer.get(user)
        video_messages = [m for m in messages if m.video is not None]

        if not video_messages:
            await message.answer('No videos found')
            return

        normalize_button = InlineKeyboardButton(
            text=VideoAction.NORMALIZE.title(),
            callback_data=VideoCallback(action=VideoAction.NORMALIZE).pack(),
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [normalize_button],
            ]
        )
        await message.answer(f'Found {len(video_messages)} videos', reply_markup=keyboard)

    services.task_scheduler.schedule(
        send_action_selection,
        user=user,
        delay=config.forward_batch_timeout,
    )


@router.callback_query(VideoCallback.filter())
async def on_video_action(callback: CallbackQuery, callback_data: VideoCallback) -> None:
    # In inline-mode callbacks Telegram provides `inline_message_id` instead of `message`
    if callback.message is None:
        return

    text = dedent(
        f"""
        {callback.message.text}
    
        Selected: {callback_data.action.title()}
        """
    ).strip()
    await callback.message.edit_text(text, reply_markup=None)
