import asyncio
from collections.abc import Sequence
from enum import StrEnum, auto
from textwrap import dedent

from aiogram import Bot, Router
from aiogram.filters.callback_data import CallbackData
from aiogram.types import BufferedInputFile, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaVideo, Message

from general_bot.config import config
from general_bot.domain import normalize_video_volume
from general_bot.services import Services
from general_bot.types import ChatId

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


async def _resend_message_group(
    bot: Bot,
    chat_id: ChatId,
    message_group: Sequence[Message],
    replacement_videos: Sequence[bytes | None] | None = None,
) -> None:
    if not message_group:
        raise ValueError('`message_group` must not be empty')
    if replacement_videos is None:
        replacement_videos = [None] * len(message_group)
    if len(replacement_videos) != len(message_group):
        raise ValueError('`replacement_videos` must have the same length as `message_group`')

    # Case #1: Text message
    if len(message_group) == 1 and message_group[0].video is None:
        await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=message_group[0].chat.id,
            message_id=message_group[0].message_id,
        )
        return

    # Case #2: Video message
    if any(message.video is None for message in message_group):
        raise ValueError('Message group must contain only videos')

    media = []
    for message, replacement_video in zip(message_group, replacement_videos, strict=True):
        media.append(
            InputMediaVideo(
                media=(
                    message.video.file_id
                    if replacement_video is None else
                    BufferedInputFile(replacement_video, filename=message.video.file_name)
                ),
                caption=message.caption,
                caption_entities=message.caption_entities,
            )
        )

    # HACK: Allowed media length is 2-10, but it also works for 1. May break in the future
    await bot.send_media_group(chat_id=chat_id, media=media)


@router.message()
async def on_message_buffer_and_schedule_action_selection(message: Message, services: Services) -> None:
    # Channel or chat may also send a message
    if message.from_user is None:
        return

    user = message.from_user
    services.message_buffer.append(message, user=user)

    async def send_action_selection() -> None:
        messages = services.message_buffer.peek(user)
        video_messages = [m for m in messages if m.video is not None]

        if not video_messages:
            await message.answer('No videos received')
            return

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    _create_video_action_button(VideoAction.CANCEL),
                    _create_video_action_button(VideoAction.NORMALIZE),
                ],
            ]
        )
        await message.answer(f'Got {len(video_messages)} videos', reply_markup=keyboard)

    services.task_scheduler.schedule(
        send_action_selection,
        user=user,
        delay=config.forward_batch_timeout,
    )


@router.callback_query(VideoCallback.filter())
async def on_video_action(callback: CallbackQuery, callback_data: VideoCallback, bot: Bot, services: Services) -> None:
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
            message_groups = services.message_buffer.flush_grouped(user)
            cpu_semaphore = asyncio.Semaphore(1)

            async def normalize_message_video(message: Message) -> bytes | None:
                if message.video is None:
                    return None

                file = await bot.get_file(message.video.file_id)
                buffer = await bot.download_file(file.file_path)
                video_bytes = buffer.read()

                # Limit concurrent CPU-bound video processing to avoid overloading the constrained runtime
                async with cpu_semaphore:
                    return await normalize_video_volume(video_bytes, loudness=config.normalization_loudness)

            for message_group in message_groups:
                replacement_videos = await asyncio.wait_for(
                    asyncio.gather(*(normalize_message_video(m) for m in message_group)),
                    timeout=60,
                )
                await _resend_message_group(bot, user.id, message_group, replacement_videos)

        case VideoAction.CANCEL:
            services.message_buffer.flush(user)
            await callback.message.answer('Canceled')
