from enum import StrEnum, auto

from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message

from timeline_hub.handlers.clips.common import (
    callback_message,
    dummy_button,
    selected_text,
    stacked_keyboard,
    width_reserved_text,
)
from timeline_hub.settings import Settings

router = Router()


class RetrieveEntryAction(StrEnum):
    CANCEL = auto()


class RetrieveEntryCallbackData(CallbackData, prefix='track_retrieve_entry'):
    action: RetrieveEntryAction


@router.message(F.text == 'Tracks')
async def on_tracks(message: Message, state: FSMContext, settings: Settings) -> None:
    await state.clear()
    await message.answer(
        **width_reserved_text(
            text='Select action:',
            message_width=settings.message_width,
        ),
        reply_markup=_track_entry_reply_markup(),
    )


@router.callback_query(
    RetrieveEntryCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_retrieve_entry(
    callback: CallbackQuery,
    callback_data: RetrieveEntryCallbackData,
    state: FSMContext,
) -> None:
    await callback.answer()
    message = callback_message(callback)
    if message is None:
        await state.clear()
        return

    if callback_data.action is RetrieveEntryAction.CANCEL:
        await state.clear()
        await message.edit_text(
            **selected_text(selected='Cancel'),
            reply_markup=None,
        )


def _track_entry_reply_markup():
    return stacked_keyboard(
        buttons=[
            dummy_button(),
            dummy_button(),
            InlineKeyboardButton(
                text='Cancel',
                callback_data=RetrieveEntryCallbackData(action=RetrieveEntryAction.CANCEL).pack(),
            ),
        ]
    )
