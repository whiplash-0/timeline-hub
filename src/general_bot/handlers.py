import asyncio
from collections.abc import Sequence
from enum import StrEnum, auto
from textwrap import dedent

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.types import BufferedInputFile, CallbackQuery, ErrorEvent, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaVideo, Message
from loguru import logger

from general_bot.domain import normalize_video_volume
from general_bot.services import Services
from general_bot.settings import Settings
from general_bot.types import ChatId

router = Router()


class ClipAction(StrEnum):
    NORMALIZE = auto()
    CANCEL = auto()


class ClipCallbackData(CallbackData, prefix='clip'):
    action: ClipAction


@router.error()
async def on_error_shutdown(_: ErrorEvent, dispatcher: Dispatcher) -> None:
    logger.exception('Handler exception')
    await dispatcher.stop_polling()


@router.message(F.chat.type == ChatType.PRIVATE)
async def on_message_buffer_and_schedule_clip_action_selection(
    message: Message,
    services: Services,
    settings: Settings,
) -> None:
    user = message.from_user
    chat_id = message.chat.id
    services.chat_message_buffer.append(message, chat_id=chat_id)

    async def send_action_selection() -> None:
        messages = services.chat_message_buffer.peek(chat_id)
        clip_messages = [m for m in messages if m.video is not None]

        if not clip_messages:
            services.chat_message_buffer.flush(chat_id)
            await message.answer('No clips received')
            return

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    _create_clip_action_button(ClipAction.CANCEL),
                    _create_clip_action_button(ClipAction.NORMALIZE),
                ],
            ]
        )
        await message.answer(
            f'Got {len(clip_messages)} clip{"" if len(clip_messages) == 1 else "s"}',
            reply_markup=keyboard,
        )

    services.task_scheduler.schedule(
        send_action_selection,
        user=user,
        delay=settings.forward_batch_timeout,
    )


@router.callback_query(
    ClipCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_clip_action(
    callback: CallbackQuery,
    callback_data: ClipCallbackData,
    bot: Bot,
    services: Services,
    settings: Settings,
) -> None:
    await callback.answer()
    chat_id = callback.message.chat.id

    updated_text = dedent(
        f"""
        {callback.message.text}
    
        Selected: {callback_data.action.title()}
        """
    ).strip()
    await callback.message.edit_text(updated_text, reply_markup=None)

    match callback_data.action:
        case ClipAction.NORMALIZE:
            message_groups = services.chat_message_buffer.flush_grouped(chat_id)
            cpu_semaphore = asyncio.Semaphore(1)

            async def normalize_message_clip_volume(message: Message) -> bytes | None:
                if message.video is None:
                    return None

                file = await bot.get_file(message.video.file_id)
                buffer = await bot.download_file(file.file_path)
                video_bytes = buffer.read()

                # Limit concurrent CPU-bound video processing to avoid overloading the constrained runtime
                async with cpu_semaphore:
                    return await normalize_video_volume(
                        video_bytes,
                        loudness=settings.normalization_loudness,
                        bitrate=settings.normalization_bitrate,
                    )

            for message_group in message_groups:
                replacement_videos = await asyncio.wait_for(
                    asyncio.gather(*(normalize_message_clip_volume(m) for m in message_group)),
                    timeout=60,
                )
                await _resend_message_group(bot, chat_id, message_group, replacement_videos)

        case ClipAction.CANCEL:
            services.chat_message_buffer.flush(chat_id)
            await callback.message.answer('Canceled')


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


def _create_clip_action_button(action: ClipAction) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=action.title(),
        callback_data=ClipCallbackData(action=action).pack(),
    )
