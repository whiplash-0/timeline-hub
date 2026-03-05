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
    CANCEL = auto()


class VideoCallback(CallbackData, prefix='video'):
    action: VideoAction


def _create_video_action_button(action: VideoAction) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=action.title(),
        callback_data=VideoCallback(action=action).pack(),
    )


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

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    _create_video_action_button(VideoAction.NORMALIZE),
                    _create_video_action_button(VideoAction.CANCEL),
                ],
            ]
        )
        await message.answer(f'Found {len(video_messages)} videos', reply_markup=keyboard)

    services.task_scheduler.schedule(
        send_action_selection,
        user=user,
        delay=config.forward_batch_timeout,
    )


@router.callback_query(VideoCallback.filter())
async def on_video_action(callback: CallbackQuery, callback_data: VideoCallback, services: Services) -> None:
    await callback.answer()
    # In inline-mode callbacks Telegram provides `inline_message_id` instead of `message`
    if callback.message is None:
        return
    if (user := callback.from_user) is None:
        return

    updated_text = dedent(
        f"""
        {callback.message.text}
    
        Selected: {callback_data.action.title()}
        """
    ).strip()
    await callback.message.edit_text(updated_text, reply_markup=None)

    match callback_data.action:
        case VideoAction.NORMALIZE:
            messages = services.message_buffer.flush(user)
            ...
        case VideoAction.CANCEL:
            services.message_buffer.flush(user)
            await callback.message.answer('Canceled')
