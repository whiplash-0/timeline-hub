from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.formatting import Bold, Text

from timeline_hub.handlers.clips.ingest import try_dispatch_clip_intake
from timeline_hub.handlers.menu import (
    callback_message,
    create_padding_line,
    handle_stale_selection,
    selected_text,
    single_button_keyboard,
)
from timeline_hub.handlers.tracks.ingest import try_dispatch_track_intake
from timeline_hub.services.container import Services
from timeline_hub.settings import Settings

router = Router()


class IntakeFallbackCallbackData(CallbackData, prefix='intake_fallback'):
    action: str
    buffer_version: int


@router.message(F.chat.type == ChatType.PRIVATE, F.text | F.photo | F.audio | F.video)
async def on_buffered_relevant_message(
    message: Message,
    services: Services,
    settings: Settings,
) -> None:
    chat_id = message.chat.id
    services.chat_message_buffer.append(message, chat_id=chat_id)

    async def send_action_selection() -> None:
        ordered_buffered_messages = services.chat_message_buffer.peek_flat(chat_id)
        message_count = len(ordered_buffered_messages)
        has_photo = any(buffered_message.photo is not None for buffered_message in ordered_buffered_messages)
        has_audio = any(buffered_message.audio is not None for buffered_message in ordered_buffered_messages)
        has_video = any(
            buffered_message.video is not None or getattr(buffered_message, 'animation', None) is not None
            for buffered_message in ordered_buffered_messages
        )

        if has_video and (has_photo or has_audio):
            services.chat_message_buffer.flush(chat_id)
            await message.answer(text="Can't dispatch")
            return

        if has_video:
            handled = await try_dispatch_clip_intake(
                message=message,
                services=services,
                settings=settings,
            )
            if handled:
                return
        elif has_photo:
            handled = await try_dispatch_track_intake(
                message=message,
                services=services,
                settings=settings,
            )
            if handled:
                return

        await message.answer(
            **Text(
                create_padding_line(settings.message_width),
                '\n',
                Text('Messages: ', Bold(str(message_count))),
            ).as_kwargs(),
            reply_markup=single_button_keyboard(
                button=InlineKeyboardButton(
                    text='Cancel',
                    callback_data=IntakeFallbackCallbackData(
                        action='cancel',
                        buffer_version=services.chat_message_buffer.version(chat_id),
                    ).pack(),
                )
            ),
        )

    services.task_scheduler.schedule(
        send_action_selection,
        key=chat_id,
        delay=settings.forward_batch_timeout,
    )


@router.callback_query(
    IntakeFallbackCallbackData.filter(F.action == 'cancel'),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_intake_fallback_cancel(
    callback: CallbackQuery,
    callback_data: IntakeFallbackCallbackData,
    services: Services,
    state: FSMContext,
) -> None:
    await callback.answer()
    message = callback_message(callback)
    if message is None:
        return
    if callback_data.buffer_version != services.chat_message_buffer.version(message.chat.id):
        await handle_stale_selection(message=message, state=state)
        return
    services.chat_message_buffer.flush(message.chat.id)
    await message.edit_text(
        **selected_text(selected=['Cancel']),
        reply_markup=None,
    )
