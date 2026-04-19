from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.types import CallbackQuery, InaccessibleMessage, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.formatting import Bold, Text

DUMMY_BUTTON_TEXT = '—'
DUMMY_CALLBACK_VALUE = 'dummy'
T = TypeVar('T')


def create_padding_line(width: int) -> str:
    """Return a Telegram width stabilizer line.

    Uses two visible dot anchors and NBSP padding to reserve message width.
    Dots prevent Telegram from trimming whitespace-only lines; width counts
    total characters (2 dots + NBSPs).
    """
    if width < 2:
        raise ValueError('`width` must be >= 2')
    return '·' + '\u00a0' * (width - 2) + '·'


def selection_text(
    *,
    selected: Sequence[str],
    prompt: str | None = None,
    message_width: int | None = None,
) -> dict[str, Any]:
    selected_content = _selected_content(selected)
    if prompt is None:
        return selected_content.as_kwargs()
    if message_width is None:
        raise ValueError('`message_width` is required when `prompt` is provided with selected values')
    return _button_message_text(
        real_lines=[selected_content, prompt],
        message_width=message_width,
    )


def selected_text(
    *,
    selected: Sequence[str] | str,
    leading_text: str | None = None,
    message_width: int | None = None,
) -> dict[str, Any]:
    selected_content = _selected_content(_normalize_selected_values(selected))
    if leading_text is None:
        return selected_content.as_kwargs()
    return Text(leading_text, '\n', selected_content).as_kwargs()


def width_reserved_text(*, text: str, message_width: int) -> dict[str, Any]:
    padding_line = create_padding_line(message_width)
    return {'text': f'{padding_line}\n{padding_line}\n{text}'}


def fixed_option_keyboard(
    *,
    option_universe: Sequence[T],
    available_options: Sequence[T],
    build_button: Callable[[T], InlineKeyboardButton],
    back_button: InlineKeyboardButton,
) -> InlineKeyboardMarkup:
    available = tuple(available_options)
    return selection_keyboard(
        buttons=[build_button(option) if option in available else dummy_button() for option in option_universe],
        back_button=back_button,
    )


def selection_keyboard(
    *,
    buttons: Sequence[InlineKeyboardButton],
    back_button: InlineKeyboardButton,
) -> InlineKeyboardMarkup:
    regular_rows = _snake_rows(buttons)
    top_row, middle_row, bottom_row = ensure_three_rows(
        top_row=regular_rows.top_row,
        middle_row=regular_rows.bottom_row,
        bottom_row=[back_button],
    )
    return three_row_keyboard(
        top_row=top_row,
        middle_row=middle_row,
        bottom_row=bottom_row,
    )


def stacked_keyboard(*, buttons: Sequence[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    if len(buttons) != 3:
        raise ValueError('`buttons` must contain exactly 3 items')
    top_row, middle_row, bottom_row = ensure_three_rows(
        top_row=[buttons[0]],
        middle_row=[buttons[1]],
        bottom_row=[buttons[2]],
    )
    return three_row_keyboard(
        top_row=top_row,
        middle_row=middle_row,
        bottom_row=bottom_row,
    )


def single_button_keyboard(*, button: InlineKeyboardButton) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[button]],
    )


def ensure_three_rows(
    *,
    top_row: list[InlineKeyboardButton],
    middle_row: list[InlineKeyboardButton],
    bottom_row: list[InlineKeyboardButton],
) -> tuple[list[InlineKeyboardButton], list[InlineKeyboardButton], list[InlineKeyboardButton]]:
    total_real_buttons = len(top_row) + len(middle_row) + len(bottom_row)
    if total_real_buttons >= 3:
        return top_row, middle_row, bottom_row
    if total_real_buttons == 2:
        if bottom_row:
            if not middle_row:
                return top_row, [dummy_button()], bottom_row
            return [dummy_button()], middle_row, bottom_row
        if not top_row:
            return [dummy_button()], middle_row, bottom_row
        if not middle_row:
            return top_row, [dummy_button()], bottom_row
        return top_row, middle_row, [dummy_button()]
    if total_real_buttons == 1:
        return (
            top_row if top_row else [dummy_button()],
            middle_row if middle_row else [dummy_button()],
            bottom_row if bottom_row else [dummy_button()],
        )
    return [dummy_button()], [dummy_button()], [dummy_button()]


def three_row_keyboard(
    *,
    top_row: Sequence[InlineKeyboardButton] = (),
    middle_row: Sequence[InlineKeyboardButton] = (),
    bottom_row: Sequence[InlineKeyboardButton] = (),
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            list(top_row),
            list(middle_row),
            list(bottom_row),
        ]
    )


def back_button(*, callback_data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text='Back',
        callback_data=callback_data,
    )


def dummy_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=DUMMY_BUTTON_TEXT,
        callback_data=DUMMY_CALLBACK_VALUE,
    )


def callback_message(callback: CallbackQuery) -> Message | None:
    message = callback.message
    if message is None or isinstance(message, InaccessibleMessage):
        return None
    return message


async def terminate_menu(
    *,
    message: Message,
    state: FSMContext,
    text: str,
) -> None:
    await state.clear()
    await message.edit_text(text, reply_markup=None)


async def handle_stale_selection(*, message: Message, state: FSMContext) -> None:
    await terminate_menu(
        message=message,
        state=state,
        text='Selection is no longer available',
    )


async def validate_flow_state(
    *,
    message: Message,
    state: FSMContext,
    expected_mode: str,
    expected_state: State,
) -> bool:
    data = await state.get_data()
    if data.get('mode') != expected_mode or data.get('menu_message_id') != message.message_id:
        await handle_stale_selection(message=message, state=state)
        return False
    if await state.get_state() != expected_state.state:
        await handle_stale_selection(message=message, state=state)
        return False
    return True


def _normalize_selected_values(selected: Sequence[str] | str) -> list[str]:
    if isinstance(selected, str):
        return [selected]
    return list(selected)


def _selected_content(selected: Sequence[str]) -> Text:
    parts: list[Any] = ['Selected: ']
    for index, value in enumerate(selected):
        if index > 0:
            parts.append(' → ')
        parts.append(Bold(value))
    return Text(*parts)


class _TwoRowButtons:
    def __init__(
        self,
        *,
        top_row: Sequence[InlineKeyboardButton],
        bottom_row: Sequence[InlineKeyboardButton],
    ) -> None:
        self.top_row = list(top_row)
        self.bottom_row = list(bottom_row)


def _snake_rows(buttons: Sequence[InlineKeyboardButton]) -> _TwoRowButtons:
    button_list = list(buttons)
    top_row_size, bottom_row_size = _two_row_sizes(len(button_list))
    top_row: list[InlineKeyboardButton | None] = [None] * top_row_size
    bottom_row: list[InlineKeyboardButton | None] = [None] * bottom_row_size

    for button, (row_name, index) in zip(
        button_list,
        _snake_positions(top_row_size=top_row_size, bottom_row_size=bottom_row_size),
        strict=True,
    ):
        if row_name == 'top':
            top_row[index] = button
        else:
            bottom_row[index] = button

    return _TwoRowButtons(
        top_row=[button for button in top_row if button is not None],
        bottom_row=[button for button in bottom_row if button is not None],
    )


def _button_message_text(
    *,
    real_lines: Sequence[str | Text],
    message_width: int,
) -> dict[str, Any]:
    padding_line = create_padding_line(message_width)
    if len(real_lines) == 1:
        return Text(
            padding_line,
            '\n',
            padding_line,
            '\n',
            real_lines[0],
        ).as_kwargs()
    if len(real_lines) == 2:
        return Text(
            real_lines[0],
            '\n',
            padding_line,
            '\n',
            real_lines[1],
        ).as_kwargs()
    raise ValueError('button messages support exactly 1 or 2 real text lines')


def _two_row_sizes(button_count: int) -> tuple[int, int]:
    if button_count < 0:
        raise ValueError('`button_count` must be >= 0')
    if button_count == 1:
        return 0, 1
    top_row_size = button_count // 2
    return top_row_size, button_count - top_row_size


def _snake_positions(*, top_row_size: int, bottom_row_size: int) -> list[tuple[str, int]]:
    positions: list[tuple[str, int]] = []
    for offset in range(max(top_row_size, bottom_row_size)):
        top_index = top_row_size - 1 - offset
        bottom_index = bottom_row_size - 1 - offset
        if offset % 2 == 0:
            if top_index >= 0:
                positions.append(('top', top_index))
            if bottom_index >= 0:
                positions.append(('bottom', bottom_index))
        else:
            if bottom_index >= 0:
                positions.append(('bottom', bottom_index))
            if top_index >= 0:
                positions.append(('top', top_index))
    return positions
