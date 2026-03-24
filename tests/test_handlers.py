from datetime import date, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiogram.utils.formatting import Bold, Text

import general_bot.handlers.clips.fetch as fetch_module
import general_bot.handlers.clips.intake as intake_module
from general_bot.handlers.clips.common import (
    ALL_SCOPES_CALLBACK_VALUE,
    DUMMY_BUTTON_TEXT,
    FLOW_FETCH_RAW,
    FLOW_RECONCILE,
    FetchClipFlow,
    MenuAction,
    MenuStep,
    ReconcileClipFlow,
    StoreClipFlow,
    create_padding_line,
    format_store_summary,
    selected_text,
    selection_labels,
)
from general_bot.handlers.clips.fetch import (
    FetchCallbackData,
    FetchEntryAction,
    FetchEntryCallbackData,
    _send_fetch_scopes,
    _send_stored_clip_batch,
    _show_fetch_scope_menu,
    _show_fetch_season_menu,
    _show_fetch_sub_season_menu,
    _show_fetch_universe_menu,
    on_clips,
    on_fetch_entry,
    on_fetch_menu,
)
from general_bot.handlers.clips.intake import (
    IntakeAction,
    IntakeCallbackData,
    _reconcile_summary_kwargs,
    _show_store_scope_menu,
    _show_store_season_menu,
    _show_store_sub_season_menu,
    _show_store_universe_menu,
    _store_year_options,
    on_buffered_clip_message,
    on_intake_action,
    on_intake_menu,
)
from general_bot.handlers.router import on_dummy_button
from general_bot.services.clip_store import (
    Clip,
    ClipGroup,
    ClipSubGroup,
    DuplicateFilenamesError,
    InvalidFilenamesError,
    MixedClipGroupsError,
    ReconcileResult,
    Scope,
    Season,
    StoreResult,
    SubSeason,
    Universe,
    UnknownClipsError,
)
from general_bot.services.container import Services
from general_bot.services.message_buffer import ChatMessageBuffer


class _FakeState:
    def __init__(self) -> None:
        self.current_state: str | None = None
        self.data: dict[str, object] = {}
        self.clear_count = 0

    async def clear(self) -> None:
        self.current_state = None
        self.data = {}
        self.clear_count += 1

    async def set_state(self, state: object | None = None) -> None:
        if state is None:
            self.current_state = None
            return
        self.current_state = getattr(state, 'state', state)

    async def get_state(self) -> str | None:
        return self.current_state

    async def update_data(self, data: dict[str, object] | None = None, **kwargs: object) -> dict[str, object]:
        if data is not None:
            self.data.update(data)
        self.data.update(kwargs)
        return dict(self.data)

    async def get_data(self) -> dict[str, object]:
        return dict(self.data)


class _FakeScheduler:
    def __init__(self) -> None:
        self.job = None
        self.key = None
        self.delay = None

    def schedule(self, job, *, key, delay) -> None:
        self.job = job
        self.key = key
        self.delay = delay


class _RecordingBot:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    async def send_message(self, *, chat_id: int, text: str) -> None:
        self.events.append(('message', (chat_id, text)))

    async def send_video(self, *, chat_id: int, video) -> None:
        self.events.append(('video', (chat_id, video.filename)))

    async def send_media_group(self, *, chat_id: int, media) -> None:
        self.events.append(('media_group', (chat_id, [item.media.filename for item in media])))


class _FetchClipStore:
    def __init__(
        self,
        batches_by_scope: dict[Scope, list[list[Clip]]],
        *,
        sub_groups: list[ClipSubGroup] | None = None,
    ) -> None:
        self.batches_by_scope = batches_by_scope
        self.calls: list[tuple[ClipGroup, ClipSubGroup]] = []
        self.sub_groups = list(sub_groups or [])

    async def fetch(self, *, clip_group: ClipGroup, clip_sub_group: ClipSubGroup):
        self.calls.append((clip_group, clip_sub_group))
        for batch in self.batches_by_scope[clip_sub_group.scope]:
            yield batch

    async def list_sub_groups(self, clip_group: ClipGroup) -> list[ClipSubGroup]:
        return list(self.sub_groups)


class _NoListClipStore:
    def __init__(self) -> None:
        self.store = AsyncMock(return_value=StoreResult(stored_count=0, duplicate_count=0))
        self.list_groups = AsyncMock(side_effect=AssertionError('store flow must not call list_groups'))
        self.list_sub_groups = AsyncMock(side_effect=AssertionError('store flow must not call list_sub_groups'))


def _services(
    *,
    clip_store,
    scheduler: _FakeScheduler | None = None,
    buffer: ChatMessageBuffer | None = None,
) -> Services:
    return Services(
        task_scheduler=scheduler or _FakeScheduler(),
        chat_message_buffer=buffer or ChatMessageBuffer(),
        clip_store=clip_store,
    )


def _settings(**overrides: object) -> SimpleNamespace:
    values = {
        'forward_batch_timeout': timedelta(milliseconds=250),
        'min_clip_year': 2022,
        'normalization_loudness': -14,
        'normalization_bitrate': 128,
        'message_width': 35,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _fake_message(
    *,
    chat_id: int = 1,
    message_id: int = 1,
    text: str | None = None,
    video=None,
    media_group_id: str | None = None,
):
    message = SimpleNamespace(
        chat=SimpleNamespace(id=chat_id, type='private'),
        message_id=message_id,
        text=text,
        video=video,
        media_group_id=media_group_id,
        caption=None,
        caption_entities=None,
    )
    message.answer = AsyncMock()
    message.edit_text = AsyncMock()
    return message


def _fake_video(*, file_id: str, file_name: str | None) -> SimpleNamespace:
    return SimpleNamespace(file_id=file_id, file_name=file_name)


def _fake_callback(message) -> SimpleNamespace:
    callback = SimpleNamespace(message=message)
    callback.answer = AsyncMock()
    return callback


def _keyboard_rows(reply_markup) -> list[list[str]]:
    return [[button.text for button in row] for row in reply_markup.inline_keyboard]


def _assert_format_kwargs(actual: dict[str, object], expected: dict[str, object]) -> None:
    for key, value in expected.items():
        assert actual[key] == value


def _assert_one_line_button_message(*, text: str, real_line: str, message_width: int) -> None:
    padding_line = create_padding_line(message_width)
    assert text.split('\n') == [padding_line, padding_line, real_line]


def _assert_two_line_button_message(*, text: str, top_line: str, bottom_line: str, message_width: int) -> None:
    padding_line = create_padding_line(message_width)
    assert text.split('\n') == [top_line, padding_line, bottom_line]


def _assert_three_rows(reply_markup) -> None:
    assert len(reply_markup.inline_keyboard) == 3


def _assert_no_dummy_buttons(reply_markup) -> None:
    assert all(button.text != DUMMY_BUTTON_TEXT for row in reply_markup.inline_keyboard for button in row)


def _selected_kwargs(*values: str, prompt: str | None = None, message_width: int | None = None) -> dict[str, object]:
    parts: list[object] = ['Selected: ']
    for index, value in enumerate(values):
        if index > 0:
            parts.append(' → ')
        parts.append(Bold(value))

    selected = Text(*parts)
    if prompt is None:
        return selected.as_kwargs()
    if message_width is None:
        raise ValueError('`message_width` is required when `prompt` is provided')
    return Text(
        selected,
        '\n',
        create_padding_line(message_width),
        '\n',
        prompt,
    ).as_kwargs()


def test_create_padding_line_returns_two_visible_anchors_for_minimum_width() -> None:
    assert create_padding_line(2) == '··'


def test_create_padding_line_returns_nbsp_padding_between_anchors() -> None:
    assert create_padding_line(4) == '·\u00a0\u00a0·'


def test_create_padding_line_rejects_width_below_two() -> None:
    with pytest.raises(ValueError, match='`width` must be >= 2'):
        create_padding_line(1)


def test_selected_text_with_leading_text_keeps_plain_text_layout_and_segmented_bolding() -> None:
    expected = Text(
        'Got 1 clip',
        '\n',
        'Selected: ',
        Bold('Store'),
    ).as_kwargs()

    assert (
        selected_text(
            selected='Store',
            leading_text='Got 1 clip',
            message_width=6,
        )
        == expected
    )


def test_handlers_package_router_imports_cleanly() -> None:
    from general_bot.app import run
    from general_bot.handlers.router import router

    assert callable(run)
    assert router is not None


def test_selection_labels_omits_none_sub_season_from_visible_path() -> None:
    assert selection_labels(
        year=2026,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        scope=Scope.COLLECTION,
    ) == ['2026', '1', 'West', 'Collection']


@pytest.mark.parametrize(
    ('result', 'expected'),
    [
        (StoreResult(stored_count=3, duplicate_count=2), 'Stored: 3\nDeduplicated: 2'),
        (StoreResult(stored_count=3, duplicate_count=0), 'Stored: 3'),
        (StoreResult(stored_count=0, duplicate_count=2), 'Deduplicated: 2'),
        (StoreResult(stored_count=0, duplicate_count=0), 'Nothing changed'),
    ],
)
def test_format_store_summary_uses_conditional_multiline_output(
    result: StoreResult,
    expected: str,
) -> None:
    assert format_store_summary(result) == expected


@pytest.mark.parametrize(
    ('result', 'expected'),
    [
        (ReconcileResult(updated=3, removed=0), Text('Updated: ', Bold('3')).as_kwargs()),
        (ReconcileResult(updated=0, removed=2), Text('Removed: ', Bold('2')).as_kwargs()),
        (
            ReconcileResult(updated=3, removed=2),
            Text('Updated: ', Bold('3'), '\n', 'Removed: ', Bold('2')).as_kwargs(),
        ),
        (ReconcileResult(updated=0, removed=0), {'text': 'Nothing changed'}),
    ],
)
def test_reconcile_summary_omits_zero_value_lines(
    result: ReconcileResult,
    expected: dict[str, object],
) -> None:
    assert _reconcile_summary_kwargs(result) == expected


@pytest.mark.asyncio
async def test_on_clips_sends_fetch_entry_button() -> None:
    message = _fake_message(text='Clips')
    state = _FakeState()
    settings = _settings()

    await on_clips(message, state, settings)

    message.answer.assert_awaited_once()
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_one_line_button_message(
        text=message.answer.await_args.kwargs['text'],
        real_line='Select action:',
        message_width=settings.message_width,
    )
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Fetch'], ['Fetch raw'], ['Cancel']]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_on_fetch_entry_edits_to_no_clips_stored_when_empty() -> None:
    message = _fake_message(text='Clips', message_id=10)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace(list_groups=AsyncMock(return_value=[])))
    settings = _settings()

    await on_fetch_entry(
        callback,
        FetchEntryCallbackData(action=FetchEntryAction.FETCH),
        services,
        settings,
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('No clips stored', reply_markup=None)
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_on_fetch_entry_cancel_removes_buttons_and_shows_selected_text() -> None:
    message = _fake_message(text='Select action:', message_id=12)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace(list_groups=AsyncMock()))

    await on_fetch_entry(
        callback,
        FetchEntryCallbackData(action=FetchEntryAction.CANCEL),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with(
        **Text('Selected: ', Bold('Cancel')).as_kwargs(),
        reply_markup=None,
    )
    services.clip_store.list_groups.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_store_cancel_removes_buttons_and_shows_only_selected_cancel() -> None:
    message = _fake_message(text='Got 1 clip', message_id=13)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace(), buffer=ChatMessageBuffer())

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.CANCEL),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with(
        **Text('Selected: ', Bold('Cancel')).as_kwargs(),
        reply_markup=None,
    )
    message.answer.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_clip_action_selection_includes_store_button() -> None:
    scheduler = _FakeScheduler()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler)
    settings = _settings()
    message = _fake_message(
        chat_id=42,
        message_id=1,
        video=_fake_video(file_id='file-1', file_name='clip.mp4'),
    )

    await on_buffered_clip_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        'Clips: ',
        Bold('1'),
        '\n',
        create_padding_line(settings.message_width),
        '\n',
        'Select action:',
    ).as_kwargs()
    _assert_format_kwargs(
        message.answer.await_args.kwargs,
        expected,
    )
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Reconcile'], ['Cancel']]


@pytest.mark.asyncio
async def test_store_entry_places_newest_year_in_top_right_slot() -> None:
    message = _fake_message(text='Got 1 clip', message_id=30)
    callback = _fake_callback(message)
    state = _FakeState()
    bot = AsyncMock()
    services = _services(clip_store=_NoListClipStore())
    settings = _settings()
    callback_data = SimpleNamespace(action=IntakeAction.STORE)

    await on_intake_action(callback, callback_data, bot, services, settings, state)

    message.edit_text.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Store', prompt='Select year:', message_width=settings.message_width),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    _assert_no_dummy_buttons(reply_markup)
    assert _keyboard_rows(reply_markup) == [['2023', '2026'], ['2022', '2024', '2025'], ['Back']]
    services.clip_store.list_groups.assert_not_awaited()
    services.clip_store.list_sub_groups.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_entry_derives_group_and_opens_sub_season_menu() -> None:
    message = _fake_message(text='Got 1 clip', message_id=31)
    callback = _fake_callback(message)
    state = _FakeState()
    bot = AsyncMock()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=1, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=1,
    )
    buffer.append(
        _fake_message(
            chat_id=1,
            message_id=2,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
            media_group_id='g1',
        ),
        chat_id=1,
    )
    buffer.append(
        _fake_message(
            chat_id=1,
            message_id=3,
            video=_fake_video(file_id='f3', file_name='three.mp4'),
            media_group_id='g1',
        ),
        chat_id=1,
    )
    clip_store = SimpleNamespace(
        derive_group=AsyncMock(return_value=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST))
    )
    services = _services(clip_store=clip_store, buffer=buffer)
    settings = _settings()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        bot,
        services,
        settings,
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Reconcile', '2025', '1', 'West', prompt='Select sub-season:', message_width=35),
    )
    clip_store.derive_group.assert_awaited_once_with([['one.mp4'], ['two.mp4', 'three.mp4']])
    assert state.current_state == ReconcileClipFlow.sub_season.state
    assert state.data['clip_group'] == ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST)
    assert state.data['filename_batches'] == [['one.mp4'], ['two.mp4', 'three.mp4']]
    assert services.chat_message_buffer.peek(1) == []


@pytest.mark.asyncio
async def test_reconcile_entry_ignores_non_video_buffered_messages() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=34)
    callback = _fake_callback(message)
    state = _FakeState()
    bot = AsyncMock()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='note'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    buffer.append(_fake_message(chat_id=77, message_id=4, text='ignored', media_group_id='g1'), chat_id=77)
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=5,
            video=_fake_video(file_id='f3', file_name='three.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    clip_store = SimpleNamespace(
        derive_group=AsyncMock(return_value=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST))
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        bot,
        services,
        _settings(),
        state,
    )

    clip_store.derive_group.assert_awaited_once_with([['one.mp4'], ['two.mp4', 'three.mp4']])
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_back_from_scope_returns_to_sub_season_menu() -> None:
    message = _fake_message(text='Select scope:', message_id=32)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReconcileClipFlow.scope)
    await state.update_data(
        mode=FLOW_RECONCILE,
        menu_message_id=32,
        sub_season=SubSeason.NONE,
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        filename_batches=[['one.mp4'], ['two.mp4', 'three.mp4']],
    )
    services = _services(clip_store=SimpleNamespace(), buffer=ChatMessageBuffer())

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.BACK, step=MenuStep.SCOPE, value='back'),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Reconcile', '2025', '1', 'West', prompt='Select sub-season:', message_width=35),
    )
    assert state.current_state == ReconcileClipFlow.sub_season.state


@pytest.mark.asyncio
async def test_reconcile_back_from_sub_season_returns_to_clip_action_menu() -> None:
    message = _fake_message(text='Select sub-season:', chat_id=77, message_id=33)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReconcileClipFlow.sub_season)
    await state.update_data(
        mode=FLOW_RECONCILE,
        menu_message_id=33,
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        filename_batches=[['one.mp4'], ['two.mp4', 'three.mp4']],
    )
    services = _services(
        clip_store=SimpleNamespace(derive_group=AsyncMock(side_effect=AssertionError('must not re-derive'))),
        buffer=ChatMessageBuffer(),
    )

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.BACK, step=MenuStep.SUB_SEASON, value='back'),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Reconcile'], ['Cancel']]
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        Text(
            'Clips: ',
            Bold('3'),
            '\n',
            create_padding_line(35),
            '\n',
            'Select action:',
        ).as_kwargs(),
    )
    assert state.current_state is None
    assert state.clear_count == 0
    assert state.data['filename_batches'] == [['one.mp4'], ['two.mp4', 'three.mp4']]
    message.answer.assert_not_awaited()


def test_store_year_options_returns_ascending_range() -> None:
    assert _store_year_options(current_year=2026, min_year=2022) == [2022, 2023, 2024, 2025, 2026]


@pytest.mark.asyncio
async def test_fetch_back_from_season_keeps_year_slots_and_top_right_priority() -> None:
    message = _fake_message(message_id=11)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(FetchClipFlow.season)
    await state.update_data(mode='fetch', menu_message_id=11, year=2025)
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
                    ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
                ]
            )
        )
    )

    await on_fetch_menu(
        callback,
        FetchCallbackData(action=MenuAction.BACK, step=MenuStep.SEASON, value='back'),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Fetch', prompt='Select year:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    assert state.current_state == FetchClipFlow.year.state


@pytest.mark.asyncio
async def test_on_fetch_entry_opens_year_menu_with_fetch_selected() -> None:
    message = _fake_message(text='Select action:', message_id=111)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
                    ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
                ]
            )
        )
    )

    await on_fetch_entry(
        callback,
        FetchEntryCallbackData(action=FetchEntryAction.FETCH),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Fetch', prompt='Select year:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    assert state.current_state == FetchClipFlow.year.state


@pytest.mark.asyncio
async def test_on_fetch_entry_opens_year_menu_with_fetch_raw_selected() -> None:
    message = _fake_message(text='Select action:', message_id=112)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
                    ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
                ]
            )
        )
    )

    await on_fetch_entry(
        callback,
        FetchEntryCallbackData(action=FetchEntryAction.FETCH_RAW),
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Fetch raw', prompt='Select year:', message_width=35),
    )
    assert state.current_state == FetchClipFlow.year.state


@pytest.mark.asyncio
async def test_missing_fetch_state_is_treated_as_stale_selection() -> None:
    message = _fake_message(message_id=20)
    callback = _fake_callback(message)
    state = _FakeState()

    await on_fetch_menu(
        callback,
        FetchCallbackData(action=MenuAction.SELECT, step=MenuStep.YEAR, value='2025'),
        AsyncMock(),
        _services(clip_store=SimpleNamespace()),
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_stale_store_only_intake_step_in_reconcile_mode_is_treated_as_stale_selection() -> None:
    message = _fake_message(message_id=21)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReconcileClipFlow.sub_season)
    await state.update_data(mode=FLOW_RECONCILE, menu_message_id=21)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.YEAR, value='2025'),
        AsyncMock(),
        _services(clip_store=SimpleNamespace()),
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_store_season_menu_limits_current_year_and_uses_padding_line(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 6, 15)

    monkeypatch.setattr(intake_module, 'date', _FixedDate)

    message = _fake_message(message_id=70)
    state = _FakeState()
    settings = _settings(message_width=21)

    await _show_store_season_menu(
        message=message,
        state=state,
        settings=settings,
        year=2026,
    )

    expected = _selected_kwargs('Store', '2026', prompt='Select season:', message_width=21)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [[DUMMY_BUTTON_TEXT, '1'], [DUMMY_BUTTON_TEXT, '3', '2'], ['Back']]


@pytest.mark.asyncio
async def test_store_universe_menu_uses_fixed_west_east_layout() -> None:
    message = _fake_message(message_id=71)
    state = _FakeState()
    settings = _settings(message_width=18)

    await _show_store_universe_menu(
        message=message,
        state=state,
        settings=settings,
        year=2024,
        season=Season.S3,
    )

    expected = _selected_kwargs('Store', '2024', '3', prompt='Select universe:', message_width=18)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    _assert_no_dummy_buttons(reply_markup)
    assert _keyboard_rows(reply_markup) == [['West'], ['East'], ['Back']]


@pytest.mark.asyncio
async def test_store_sub_season_menu_uses_fixed_snake_layout() -> None:
    message = _fake_message(message_id=72)
    state = _FakeState()

    await _show_store_sub_season_menu(
        message=message,
        state=state,
        settings=_settings(),
        clip_group=ClipGroup(year=2024, season=Season.S2, universe=Universe.WEST),
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    _assert_no_dummy_buttons(reply_markup)
    assert _keyboard_rows(reply_markup) == [['C', 'None'], ['D', 'B', 'A'], ['Back']]


@pytest.mark.asyncio
async def test_fetch_scope_menu_uses_fixed_scope_grid_with_dummy_slots() -> None:
    message = _fake_message(message_id=73)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace())

    await _show_fetch_scope_menu(
        message=message,
        state=state,
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        sub_season=SubSeason.NONE,
        services=services,
        settings=_settings(),
        sub_groups=[
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ],
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [[DUMMY_BUTTON_TEXT, 'All'], ['Extra', 'Collection'], ['Back']]

    message_single = _fake_message(message_id=74)
    state_single = _FakeState()
    await _show_fetch_scope_menu(
        message=message_single,
        state=state_single,
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        sub_season=SubSeason.NONE,
        services=services,
        settings=_settings(),
        sub_groups=[
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
        ],
    )

    reply_markup_single = message_single.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup_single)
    assert _keyboard_rows(reply_markup_single) == [
        [DUMMY_BUTTON_TEXT, 'All'],
        [DUMMY_BUTTON_TEXT, 'Collection'],
        ['Back'],
    ]


@pytest.mark.asyncio
async def test_fetch_raw_scope_all_with_one_scope_sends_single_scope_normally() -> None:
    message = _fake_message(chat_id=9, message_id=741)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(FetchClipFlow.scope)
    await state.update_data(
        mode=FLOW_FETCH_RAW,
        menu_message_id=741,
        year=2024,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )
    bot = _RecordingBot()
    clip_store = _FetchClipStore(
        {Scope.COLLECTION: [[Clip(filename='one.mp4', bytes=b'1')]]},
        sub_groups=[ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)],
    )
    services = _services(clip_store=clip_store)

    await on_fetch_menu(
        callback,
        FetchCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=ALL_SCOPES_CALLBACK_VALUE),
        bot,
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Fetch raw', '2024', '1', 'West', 'All'),
    )
    assert clip_store.calls == [
        (
            ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
        )
    ]
    assert bot.events == [
        ('video', (9, 'one.mp4')),
        ('message', (9, 'Done')),
    ]
    assert state.current_state is None


@pytest.mark.asyncio
async def test_fetch_scope_all_normalizes_before_sending(monkeypatch: pytest.MonkeyPatch) -> None:
    message = _fake_message(chat_id=9, message_id=742)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(FetchClipFlow.scope)
    await state.update_data(
        mode='fetch',
        menu_message_id=742,
        year=2024,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )
    bot = AsyncMock()
    clip_store = _FetchClipStore(
        {Scope.COLLECTION: [[Clip(filename='one.mp4', bytes=b'one')]]},
        sub_groups=[ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)],
    )
    services = _services(clip_store=clip_store)

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        assert loudness == -14
        assert bitrate == 128
        return b'normalized:' + video_bytes

    monkeypatch.setattr(fetch_module, 'normalize_audio_loudness', _fake_normalize)

    await on_fetch_menu(
        callback,
        FetchCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=ALL_SCOPES_CALLBACK_VALUE),
        bot,
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Fetch', '2024', '1', 'West', 'All'),
    )
    assert bot.send_video.await_args.kwargs['video'].filename == 'one.mp4'
    assert bot.send_video.await_args.kwargs['video'].data == b'normalized:one'
    bot.send_message.assert_awaited_with(chat_id=9, text='Done')
    assert state.current_state is None


@pytest.mark.asyncio
async def test_fetch_sub_season_menu_skips_only_when_none_is_only_option() -> None:
    services = _services(clip_store=SimpleNamespace())

    message_none_only = _fake_message(message_id=75)
    state_none_only = _FakeState()
    await _show_fetch_sub_season_menu(
        message=message_none_only,
        state=state_none_only,
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        services=services,
        settings=_settings(),
        sub_groups=[ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)],
    )

    assert state_none_only.current_state == FetchClipFlow.scope.state
    reply_markup_none_only = message_none_only.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup_none_only)
    assert _keyboard_rows(reply_markup_none_only) == [
        [DUMMY_BUTTON_TEXT, 'All'],
        [DUMMY_BUTTON_TEXT, 'Collection'],
        ['Back'],
    ]

    message_with_extra = _fake_message(message_id=76)
    state_with_extra = _FakeState()
    await _show_fetch_sub_season_menu(
        message=message_with_extra,
        state=state_with_extra,
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        services=services,
        settings=_settings(),
        sub_groups=[
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ],
    )

    assert state_with_extra.current_state == FetchClipFlow.sub_season.state
    reply_markup_with_extra = message_with_extra.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup_with_extra)
    assert _keyboard_rows(reply_markup_with_extra) == [
        [DUMMY_BUTTON_TEXT, 'None'],
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT, 'A'],
        ['Back'],
    ]


@pytest.mark.asyncio
async def test_store_scope_menu_uses_fixed_scope_grid_with_dummy_all_slot() -> None:
    message = _fake_message(message_id=77)
    state = _FakeState()

    await _show_store_scope_menu(
        message=message,
        state=state,
        settings=_settings(),
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        sub_season=SubSeason.NONE,
    )

    expected = _selected_kwargs('Store', '2024', '1', 'West', prompt='Select scope:', message_width=35)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Source', DUMMY_BUTTON_TEXT], ['Extra', 'Collection'], ['Back']]


@pytest.mark.asyncio
async def test_fetch_season_menu_uses_store_slot_universe_with_dummy_substitution() -> None:
    message = _fake_message(message_id=78)
    state = _FakeState()
    settings = _settings(message_width=21)
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
                    ClipGroup(year=2024, season=Season.S3, universe=Universe.EAST),
                ]
            )
        )
    )

    await _show_fetch_season_menu(
        message=message,
        state=state,
        year=2024,
        services=services,
        settings=settings,
    )

    expected = _selected_kwargs('Fetch', '2024', prompt='Select season:', message_width=21)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, '1'],
        [DUMMY_BUTTON_TEXT, '3', DUMMY_BUTTON_TEXT],
        ['Back'],
    ]


@pytest.mark.asyncio
async def test_fetch_season_menu_limits_current_year_to_store_boundary_and_uses_dummy_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 6, 15)

    monkeypatch.setattr(fetch_module, 'date', _FixedDate)

    message = _fake_message(message_id=80)
    state = _FakeState()
    settings = _settings(message_width=21)
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(year=2026, season=Season.S1, universe=Universe.WEST),
                ]
            )
        )
    )

    await _show_fetch_season_menu(
        message=message,
        state=state,
        year=2026,
        services=services,
        settings=settings,
    )

    expected = _selected_kwargs('Fetch', '2026', prompt='Select season:', message_width=21)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, '1'],
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        ['Back'],
    ]


@pytest.mark.asyncio
async def test_fetch_universe_menu_uses_store_slot_universe_with_dummy_substitution() -> None:
    message = _fake_message(message_id=79)
    state = _FakeState()
    settings = _settings(message_width=18)
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(year=2024, season=Season.S3, universe=Universe.WEST),
                ]
            )
        )
    )

    await _show_fetch_universe_menu(
        message=message,
        state=state,
        year=2024,
        season=Season.S3,
        services=services,
        settings=settings,
    )

    expected = _selected_kwargs('Fetch', '2024', '3', prompt='Select universe:', message_width=18)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['West'], [DUMMY_BUTTON_TEXT], ['Back']]


@pytest.mark.asyncio
async def test_store_scope_selection_aggregates_results_and_sends_exact_summary() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=50)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    await state.update_data(
        mode='store',
        menu_message_id=50,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')), chat_id=77
    )
    buffer.append(
        _fake_message(
            chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name='two.mp4'), media_group_id='g1'
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77, message_id=3, video=_fake_video(file_id='f3', file_name='three.mp4'), media_group_id='g1'
        ),
        chat_id=77,
    )
    buffer.append(_fake_message(chat_id=77, message_id=4, text='note'), chat_id=77)

    clip_store = SimpleNamespace(
        store=AsyncMock(
            side_effect=[
                StoreResult(stored_count=1, duplicate_count=0),
                StoreResult(stored_count=2, duplicate_count=2),
            ]
        ),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.side_effect = [
        SimpleNamespace(file_path='path-1'),
        SimpleNamespace(file_path='path-2'),
        SimpleNamespace(file_path='path-3'),
    ]
    bot.download_file.side_effect = [BytesIO(b'one'), BytesIO(b'two'), BytesIO(b'three')]

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        bot,
        services,
        _settings(),
        state,
    )

    expected_selected = _selected_kwargs('Store', '2025', '1', 'West', 'Collection')
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected_selected)
    assert clip_store.store.await_count == 2
    first_group = clip_store.store.await_args_list[0].args[0]
    second_group = clip_store.store.await_args_list[1].args[0]
    assert [clip.filename for clip in first_group] == ['one.mp4']
    assert [clip.filename for clip in second_group] == ['two.mp4', 'three.mp4']
    expected_group = ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST)
    expected_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    assert clip_store.store.await_args_list[0].kwargs['clip_group'] == expected_group
    assert clip_store.store.await_args_list[0].kwargs['clip_sub_group'] == expected_sub_group
    assert clip_store.store.await_args_list[1].kwargs['clip_group'] == expected_group
    assert clip_store.store.await_args_list[1].kwargs['clip_sub_group'] == expected_sub_group
    clip_store.compact.assert_not_awaited()
    message.answer.assert_awaited_once_with(
        **Text(
            'Stored: ',
            Bold('3'),
            '\n',
            'Deduplicated: ',
            Bold('2'),
        ).as_kwargs()
    )
    assert state.current_state is None


@pytest.mark.asyncio
@pytest.mark.parametrize('scope', [Scope.EXTRA, Scope.SOURCE])
async def test_store_scope_selection_compacts_extra_and_source_batches(scope: Scope) -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=51)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    await state.update_data(
        mode='store',
        menu_message_id=51,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(
        store=AsyncMock(return_value=StoreResult(stored_count=1, duplicate_count=0)),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.return_value = SimpleNamespace(file_path='path-1')
    bot.download_file.return_value = BytesIO(b'one')

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=scope.value),
        bot,
        services,
        _settings(),
        state,
    )

    clip_store.compact.assert_awaited_once_with(
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.NONE, scope=scope),
        batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
    )


@pytest.mark.asyncio
async def test_store_scope_selection_does_not_compact_when_everything_is_duplicate() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=52)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    await state.update_data(
        mode='store',
        menu_message_id=52,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(
        store=AsyncMock(return_value=StoreResult(stored_count=0, duplicate_count=1)),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.return_value = SimpleNamespace(file_path='path-1')
    bot.download_file.return_value = BytesIO(b'one')

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.EXTRA.value),
        bot,
        services,
        _settings(),
        state,
    )

    clip_store.compact.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_scope_selection_uses_stored_filename_batches_without_downloading() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=53)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReconcileClipFlow.scope)
    await state.update_data(
        mode=FLOW_RECONCILE,
        menu_message_id=53,
        sub_season=SubSeason.NONE,
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        filename_batches=[['one.mp4'], ['two.mp4', 'three.mp4']],
    )

    clip_store = SimpleNamespace(reconcile=AsyncMock(return_value=ReconcileResult(updated=3, removed=1)))
    services = _services(clip_store=clip_store, buffer=ChatMessageBuffer())

    bot = AsyncMock()
    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        bot,
        services,
        _settings(),
        state,
    )

    clip_store.reconcile.assert_awaited_once_with(
        [['one.mp4'], ['two.mp4', 'three.mp4']],
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
    )
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()
    message.answer.assert_awaited_once_with(
        **Text(
            'Updated: ',
            Bold('3'),
            '\n',
            'Removed: ',
            Bold('1'),
        ).as_kwargs()
    )


@pytest.mark.asyncio
async def test_reconcile_entry_skips_text_only_buffered_groups() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=55)
    callback = _fake_callback(message)
    state = _FakeState()

    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='note'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, text='ignored', media_group_id='g1'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    clip_store = SimpleNamespace(
        derive_group=AsyncMock(return_value=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST))
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    clip_store.derive_group.assert_awaited_once_with([['one.mp4']])
    message.answer.assert_not_awaited()
    assert state.data['filename_batches'] == [['one.mp4']]


@pytest.mark.asyncio
@pytest.mark.parametrize('file_name', [None, ''])
async def test_reconcile_entry_raises_on_missing_video_filename(file_name: str | None) -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=56)
    callback = _fake_callback(message)
    state = _FakeState()

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name=file_name)),
        chat_id=77,
    )

    clip_store = SimpleNamespace(derive_group=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    with pytest.raises(ValueError, match='have a filename'):
        await on_intake_action(
            callback,
            SimpleNamespace(action=IntakeAction.RECONCILE),
            AsyncMock(),
            services,
            _settings(),
            state,
        )

    clip_store.derive_group.assert_not_awaited()
    message.answer.assert_not_awaited()
    assert services.chat_message_buffer.peek(77) != []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('error', 'expected_message'),
    [
        (DuplicateFilenamesError(), "Can't reconcile duplicates"),
        (InvalidFilenamesError(), "Can't reconcile not stored"),
        (UnknownClipsError(clip_ids=['abc']), "Can't reconcile not stored"),
        (
            MixedClipGroupsError(
                groups=[
                    ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
                    ClipGroup(year=2024, season=Season.S1, universe=Universe.EAST),
                ]
            ),
            "Can't reconcile mixed groups",
        ),
    ],
)
async def test_reconcile_entry_surfaces_derive_errors(
    error: Exception,
    expected_message: str,
) -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=54)
    callback = _fake_callback(message)
    state = _FakeState()

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(derive_group=AsyncMock(side_effect=error))
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.answer.assert_awaited_once_with(expected_message)
    assert state.current_state is None
    assert services.chat_message_buffer.peek(77) != []


@pytest.mark.asyncio
async def test_send_stored_clip_batch_preserves_filename() -> None:
    bot = AsyncMock()

    await _send_stored_clip_batch(
        bot=bot,
        chat_id=5,
        clips=[
            Clip(filename='clips/2025-1-west/a.mp4', bytes=b'a'),
            Clip(filename='clips/2025-1-west/b.mp4', bytes=b'b'),
        ],
    )

    media = bot.send_media_group.await_args.kwargs['media']
    assert media[0].media.filename == 'clips/2025-1-west/a.mp4'
    assert media[1].media.filename == 'clips/2025-1-west/b.mp4'


@pytest.mark.asyncio
async def test_send_stored_clip_batch_sends_single_clip_as_video() -> None:
    bot = AsyncMock()

    await _send_stored_clip_batch(
        bot=bot,
        chat_id=5,
        clips=[Clip(filename='clips/2025-1-west/a.mp4', bytes=b'a')],
    )

    assert bot.send_video.await_args.kwargs['video'].filename == 'clips/2025-1-west/a.mp4'
    bot.send_media_group.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_fetch_scopes_sends_separator_only_between_scope_blocks_and_done() -> None:
    bot = _RecordingBot()
    services = _services(
        clip_store=_FetchClipStore(
            {
                Scope.COLLECTION: [[Clip(filename='one.mp4', bytes=b'1')]],
                Scope.EXTRA: [[Clip(filename='two.mp4', bytes=b'2')]],
            }
        )
    )

    await _send_fetch_scopes(
        bot=bot,
        chat_id=9,
        services=services,
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        sub_season=SubSeason.NONE,
        scopes=[Scope.COLLECTION, Scope.EXTRA],
        settings=_settings(),
        normalize_audio=False,
    )

    assert services.clip_store.calls == [
        (
            ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
        ),
        (
            ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ),
    ]
    assert bot.events == [
        ('video', (9, 'one.mp4')),
        ('message', (9, '.')),
        ('video', (9, 'two.mp4')),
        ('message', (9, 'Done')),
    ]


@pytest.mark.asyncio
async def test_send_fetch_scopes_normalizes_clips_in_memory_before_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = AsyncMock()
    clip_store = _FetchClipStore(
        {
            Scope.COLLECTION: [
                [Clip(filename='one.mp4', bytes=b'one')],
                [
                    Clip(filename='two.mp4', bytes=b'two'),
                    Clip(filename='three.mp4', bytes=b'three'),
                ],
            ]
        }
    )
    services = _services(clip_store=clip_store)

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        assert loudness == -13
        assert bitrate == 160
        return video_bytes.upper()

    monkeypatch.setattr(fetch_module, 'normalize_audio_loudness', _fake_normalize)

    await _send_fetch_scopes(
        bot=bot,
        chat_id=9,
        services=services,
        clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
        sub_season=SubSeason.NONE,
        scopes=[Scope.COLLECTION],
        settings=_settings(normalization_loudness=-13, normalization_bitrate=160),
        normalize_audio=True,
    )

    assert bot.send_video.await_args.kwargs['video'].filename == 'one.mp4'
    assert bot.send_video.await_args.kwargs['video'].data == b'ONE'
    sent_media = bot.send_media_group.await_args.kwargs['media']
    assert [item.media.filename for item in sent_media] == ['two.mp4', 'three.mp4']
    assert [item.media.data for item in sent_media] == [b'TWO', b'THREE']
    assert clip_store.batches_by_scope[Scope.COLLECTION] == [
        [Clip(filename='one.mp4', bytes=b'one')],
        [
            Clip(filename='two.mp4', bytes=b'two'),
            Clip(filename='three.mp4', bytes=b'three'),
        ],
    ]


@pytest.mark.asyncio
async def test_send_fetch_scopes_propagates_normalization_failure_without_sending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bot = AsyncMock()
    services = _services(
        clip_store=_FetchClipStore(
            {
                Scope.COLLECTION: [
                    [Clip(filename='one.mp4', bytes=b'one')],
                ]
            }
        )
    )

    async def _failing_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        raise RuntimeError(f'boom: {video_bytes!r}')

    monkeypatch.setattr(fetch_module, 'normalize_audio_loudness', _failing_normalize)

    with pytest.raises(RuntimeError, match="boom: b'one'"):
        await _send_fetch_scopes(
            bot=bot,
            chat_id=9,
            services=services,
            clip_group=ClipGroup(year=2025, season=Season.S1, universe=Universe.WEST),
            sub_season=SubSeason.NONE,
            scopes=[Scope.COLLECTION],
            settings=_settings(),
            normalize_audio=True,
        )

    bot.send_video.assert_not_awaited()
    bot.send_media_group.assert_not_awaited()
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_dummy_button_handler_only_answers() -> None:
    message = _fake_message(message_id=90)
    callback = _fake_callback(message)

    await on_dummy_button(callback)

    callback.answer.assert_awaited_once_with()
    message.edit_text.assert_not_awaited()
    message.answer.assert_not_awaited()
