from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.types import InlineKeyboardButton, Message
from aiogram.utils.formatting import Bold, Text

from timeline_hub.handlers.clips.ingest import try_dispatch_clip_intake
from timeline_hub.handlers.menu import create_padding_line, single_button_keyboard
from timeline_hub.handlers.tracks.ingest import (
    TrackIntakeAction,
    TrackIntakeActionCallbackData,
    try_dispatch_track_intake,
)
from timeline_hub.services.container import Services
from timeline_hub.settings import Settings

router = Router()


@router.message(F.chat.type == ChatType.PRIVATE, F.text | F.photo | F.audio | F.video)
async def on_buffered_relevant_message(
    message: Message,
    services: Services,
    settings: Settings,
) -> None:
    chat_id = message.chat.id
    services.chat_message_buffer.append(message, chat_id=chat_id)

    async def send_action_selection() -> None:
        buffered_messages = services.chat_message_buffer.peek_raw(chat_id)
        ordered_buffered_messages = services.chat_message_buffer.peek_flat(chat_id)
        has_audio = any(buffered_message.audio is not None for buffered_message in buffered_messages)
        has_video = any(
            buffered_message.video is not None or getattr(buffered_message, 'animation', None) is not None
            for buffered_message in buffered_messages
        )

        if has_audio and not has_video:
            if not _is_valid_track_batch(ordered_buffered_messages):
                services.chat_message_buffer.flush(chat_id)
                await message.answer(text="Can't dispatch input")
                return

            handled = await try_dispatch_track_intake(
                message=message,
                services=services,
                settings=settings,
            )
        elif has_video and not has_audio:
            handled = await try_dispatch_clip_intake(
                message=message,
                services=services,
                settings=settings,
            )
        elif has_video and has_audio:
            services.chat_message_buffer.flush(chat_id)
            await message.answer(text="Can't dispatch mixed input")
            return
        else:
            handled = False

        if not handled:
            await _show_fallback_menu(
                message=message,
                message_count=len(buffered_messages),
                settings=settings,
                buffer_version=services.chat_message_buffer.version(chat_id),
            )

    services.task_scheduler.schedule(
        send_action_selection,
        key=chat_id,
        delay=settings.forward_batch_timeout,
    )


def _is_valid_track_batch(messages: list[Message]) -> bool:
    if len(messages) < 2 or len(messages) % 2 != 0:
        return False

    for index, buffered_message in enumerate(messages):
        if index % 2 == 0:
            if buffered_message.photo is None:
                return False
            if buffered_message.caption is None:
                return False
            lines = [line.strip() for line in buffered_message.caption.splitlines() if line.strip()]
            if len(lines) < 2:
                return False
            continue

        if buffered_message.audio is None:
            return False

    return True


async def _show_fallback_menu(
    *,
    message: Message,
    message_count: int,
    settings: Settings,
    buffer_version: int,
) -> None:
    await message.answer(
        **Text(
            'Messages: ',
            Bold(str(message_count)),
            '\n',
            create_padding_line(settings.message_width),
            '\n',
            'Select action:',
        ).as_kwargs(),
        reply_markup=single_button_keyboard(
            button=InlineKeyboardButton(
                text='Cancel',
                callback_data=TrackIntakeActionCallbackData(
                    action=TrackIntakeAction.CANCEL,
                    buffer_version=buffer_version,
                ).pack(),
            )
        ),
    )
