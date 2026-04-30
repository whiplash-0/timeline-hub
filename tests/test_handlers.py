from datetime import date, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from aiogram.types import InlineKeyboardButton, Message
from aiogram.utils.formatting import Bold, Text

import timeline_hub.handlers.clips.delivery as delivery_module
import timeline_hub.handlers.clips.ingest as intake_module
import timeline_hub.handlers.clips.retrieve as retrieve_module
import timeline_hub.handlers.clips.route_planning as route_planning_module
import timeline_hub.handlers.clips.store_execution as store_execution_module
import timeline_hub.handlers.tracks.ingest as track_ingest_module
import timeline_hub.handlers.tracks.retrieve as track_retrieve_module
import timeline_hub.handlers.tracks.store_execution as track_store_execution_module
import timeline_hub.services.track_store as track_store_module
from timeline_hub.handlers.clips.common import (
    ALL_SCOPES_CALLBACK_VALUE,
    FLOW_PULL,
    FLOW_RECONCILE,
    MenuAction,
    MenuStep,
    ReconcileClipFlow,
    RetrieveClipFlow,
    StoreClipFlow,
    format_store_summary,
    selection_labels,
)
from timeline_hub.handlers.clips.delivery import send_fetched_clip_batch
from timeline_hub.handlers.clips.ingest import (
    IntakeAction,
    IntakeCallbackData,
    _column_right_to_left_two_row_keyboard,
    _reconcile_summary_kwargs,
    _show_store_scope_menu,
    _show_store_season_menu,
    _show_store_sub_season_menu,
    _show_store_universe_menu,
    _store_year_options,
    on_intake_action,
    on_intake_menu,
    on_reorder_menu,
    try_dispatch_clip_intake,
)
from timeline_hub.handlers.clips.reorder_flow import ReorderCallbackData, ReorderClipFlow
from timeline_hub.handlers.clips.retrieve import (
    RetrieveCallbackData,
    RetrieveEntryAction,
    RetrieveEntryCallbackData,
    _send_retrieve_scopes,
    _show_retrieve_scope_menu,
    _show_retrieve_season_menu,
    _show_retrieve_sub_season_menu,
    _show_retrieve_universe_menu,
    on_clips,
    on_retrieve_entry,
    on_retrieve_menu,
)
from timeline_hub.handlers.clips.route_planning import parse_route_text
from timeline_hub.handlers.intake import (
    IntakeFallbackCallbackData,
    on_buffered_relevant_message,
    on_intake_fallback_cancel,
)
from timeline_hub.handlers.menu import (
    DUMMY_BUTTON_TEXT,
    create_padding_line,
    selected_text,
)
from timeline_hub.handlers.router import on_dummy_button, on_start_send_menu
from timeline_hub.handlers.tracks.ingest import (
    TrackIntakeAction,
    TrackIntakeActionCallbackData,
    TrackStoreAction,
    TrackStoreCallbackData,
    TrackStoreFlow,
    TrackStoreStep,
    on_track_intake_action,
    on_track_store_menu,
    try_dispatch_track_intake,
)
from timeline_hub.handlers.tracks.retrieve import (
    RetrieveEntryAction as TracksRetrieveEntryAction,
)
from timeline_hub.handlers.tracks.retrieve import (
    RetrieveEntryCallbackData as TracksRetrieveEntryCallbackData,
)
from timeline_hub.handlers.tracks.retrieve import (
    TrackRetrieveAction,
    TrackRetrieveCallbackData,
    TrackRetrieveFlow,
    TrackRetrieveStep,
    on_tracks,
)
from timeline_hub.handlers.tracks.retrieve import (
    on_retrieve_entry as on_tracks_retrieve_entry,
)
from timeline_hub.handlers.tracks.retrieve import (
    on_retrieve_menu as on_tracks_retrieve_menu,
)
from timeline_hub.services.clip_store import (
    AudioNormalization,
    ClipGroup,
    ClipInfo,
    ClipStore,
    ClipSubGroup,
    FetchedClip,
    ReconcileResult,
    Scope,
    Season,
    StoreResult,
    SubSeason,
    Universe,
)
from timeline_hub.services.container import Services
from timeline_hub.services.message_buffer import ChatMessageBuffer
from timeline_hub.types import Extension, FileBytes

_CLIP_ID_1 = '018f05c1f1a37b348d291f53a1c9d0e1'
_CLIP_ID_2 = '018f05c1f1a37b348d291f53a1c9d0e2'
_CLIP_ID_3 = '018f05c1f1a37b348d291f53a1c9d0e3'


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
        self.events.append(('video', (chat_id, getattr(video, 'filename', video))))

    async def send_media_group(self, *, chat_id: int, media) -> None:
        self.events.append(('media_group', (chat_id, [getattr(item.media, 'filename', item.media) for item in media])))


class _RetrieveClipStore:
    def __init__(
        self,
        batches_by_scope: dict[Scope, list[list[FetchedClip]]],
        *,
        sub_groups: list[ClipSubGroup] | None = None,
    ) -> None:
        self.batches_by_scope = batches_by_scope
        self.calls: list[tuple[ClipGroup, ClipSubGroup, tuple[str, ...] | None, AudioNormalization | None]] = []
        self.sub_groups = list(sub_groups or [])

    async def fetch(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        clip_ids=None,
        audio_normalization: AudioNormalization | None = None,
    ):
        self.calls.append((group, sub_group, None if clip_ids is None else tuple(clip_ids), audio_normalization))
        for batch in self.batches_by_scope[sub_group.scope]:
            if audio_normalization is None:
                yield batch
                continue

            yield [FetchedClip(id=clip.id, file=_mp4_file(b'normalized:' + clip.file.data)) for clip in batch]

    async def list_clips(self, group: ClipGroup) -> dict[ClipSubGroup, list[tuple[ClipInfo, ...]]]:
        return {sub_group: [] for sub_group in self.sub_groups}


class _NoListClipStore:
    def __init__(self) -> None:
        self.store = AsyncMock(return_value=StoreResult(stored_count=0, duplicate_count=0))
        self.list_groups = AsyncMock(side_effect=AssertionError('store flow must not call list_groups'))
        self.list_clips = AsyncMock(side_effect=AssertionError('store flow must not call list_clips'))


class _ProduceClipStore:
    def __init__(
        self,
        *,
        store_results: list[StoreResult],
        fetched_batches: list[list[FetchedClip]],
    ) -> None:
        self.events: list[tuple[str, object]] = []
        self._store_results = iter(store_results)
        self._fetched_batches = fetched_batches
        self.fetch_calls: list[tuple[ClipGroup, ClipSubGroup, tuple[str, ...] | None, AudioNormalization | None]] = []

    async def store(self, group: ClipGroup, sub_group: ClipSubGroup, *, clips) -> StoreResult:
        self.events.append(('store', (list(clips), group, sub_group)))
        return next(self._store_results)

    async def compact(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        batch_size: int,
    ) -> None:
        self.events.append(('compact', (group, sub_group, batch_size)))

    async def fetch(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        clip_ids=None,
        audio_normalization: AudioNormalization | None = None,
    ):
        self.fetch_calls.append((group, sub_group, None if clip_ids is None else tuple(clip_ids), audio_normalization))
        self.events.append(
            ('fetch', (group, sub_group, None if clip_ids is None else tuple(clip_ids), audio_normalization))
        )
        for batch in self._fetched_batches:
            if audio_normalization is None:
                yield batch
                continue

            yield [FetchedClip(id=clip.id, file=_mp4_file(b'normalized:' + clip.file.data)) for clip in batch]


class _RetrieveTrackStore:
    def __init__(
        self,
        *,
        groups: list[track_store_module.TrackGroup],
        tracks_by_group: dict[
            track_store_module.TrackGroup, dict[track_store_module.SubSeason, list[track_store_module.TrackInfo]]
        ],
        fetched_by_track_id: dict[str, track_store_module.FetchedVariants] | None = None,
        events: list[tuple[str, object]] | None = None,
    ) -> None:
        self.groups = groups
        self.tracks_by_group = tracks_by_group
        self.fetched_by_track_id = fetched_by_track_id or {}
        self.events = events if events is not None else []

    async def list_groups(self) -> list[track_store_module.TrackGroup]:
        self.events.append(('list_groups', None))
        return self.groups

    async def list_tracks(
        self,
        group: track_store_module.TrackGroup,
    ) -> dict[track_store_module.SubSeason, list[track_store_module.TrackInfo]]:
        self.events.append(('list_tracks', group))
        return self.tracks_by_group[group]

    async def fetch(
        self,
        group: track_store_module.TrackGroup,
        track_id: str,
    ) -> track_store_module.FetchedVariants:
        self.events.append(('fetch', (group, track_id)))
        return self.fetched_by_track_id[track_id]


class _RecordingTrackBot:
    def __init__(self, events: list[tuple[str, object]]) -> None:
        self.events = events

    async def send_photo(self, *, chat_id: int, photo, caption: str, caption_entities=None, parse_mode=None) -> None:
        self.events.append(
            (
                'photo',
                {
                    'chat_id': chat_id,
                    'filename': photo.filename,
                    'data': photo.data,
                    'caption': caption,
                    'caption_entities': None
                    if caption_entities is None
                    else [entity.model_dump(exclude_none=True) for entity in caption_entities],
                    'parse_mode': parse_mode,
                },
            )
        )

    async def send_audio(self, *, chat_id: int, audio, thumbnail=None, performer=None, title=None) -> None:
        self.events.append(
            (
                'audio',
                {
                    'chat_id': chat_id,
                    'filename': audio.filename,
                    'data': audio.data,
                    'thumbnail_filename': None if thumbnail is None else thumbnail.filename,
                    'thumbnail_data': None if thumbnail is None else thumbnail.data,
                    'performer': performer,
                    'title': title,
                },
            )
        )

    async def send_media_group(self, *, chat_id: int, media) -> None:
        self.events.append(
            (
                'media_group',
                {
                    'chat_id': chat_id,
                    'files': [
                        {
                            'filename': item.media.filename,
                            'data': item.media.data,
                            'thumbnail_filename': None if item.thumbnail is None else item.thumbnail.filename,
                            'thumbnail_data': None if item.thumbnail is None else item.thumbnail.data,
                            'performer': item.performer,
                            'title': item.title,
                        }
                        for item in media
                    ],
                },
            )
        )


def _services(
    *,
    clip_store,
    track_store=None,
    scheduler: _FakeScheduler | None = None,
    buffer: ChatMessageBuffer | None = None,
) -> Services:
    return Services(
        task_scheduler=scheduler or _FakeScheduler(),
        chat_message_buffer=buffer or ChatMessageBuffer(),
        clip_store=clip_store,
        track_store=track_store or SimpleNamespace(store=AsyncMock()),
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


def _stored_filename(group: ClipGroup, sub_group: ClipSubGroup, clip_id: str) -> str:
    return f'{ClipStore.clip_identity_to_string(group, clip_id)}{Extension.MP4.suffix}'


def _mp4_file(data: bytes) -> FileBytes:
    return FileBytes(data=data, extension=Extension.MP4)


def _jpg_file(data: bytes) -> FileBytes:
    return FileBytes(data=data, extension=Extension.JPG)


def _opus_file(data: bytes) -> FileBytes:
    return FileBytes(data=data, extension=Extension.OPUS)


def _fetched_variant(
    *,
    data: bytes,
    level: int = 1,
    speed: float = 1.0,
    reverb: float = 0.0,
) -> track_store_module.FetchedVariant:
    return track_store_module.FetchedVariant(
        level=level,
        speed=speed,
        reverb=reverb,
        audio=_opus_file(data),
    )


def _fetched_track(
    *,
    track_id: str,
    title: str,
    cover: bytes,
    variants: list[track_store_module.FetchedVariant],
    instrumental_variants: list[track_store_module.FetchedVariant] | None = None,
) -> track_store_module.FetchedVariants:
    return track_store_module.FetchedVariants(
        track_id=track_id,
        artists=('artist',),
        title=title,
        cover=_jpg_file(cover),
        variants=tuple(variants),
        instrumental_variants=None if instrumental_variants is None else tuple(instrumental_variants),
    )


def _fake_message(
    *,
    chat_id: int = 1,
    message_id: int = 1,
    text: str | None = None,
    caption: str | None = None,
    caption_entities=None,
    audio=None,
    photo=None,
    video=None,
    media_group_id: str | None = None,
):
    message = SimpleNamespace(
        chat=SimpleNamespace(id=chat_id, type='private'),
        message_id=message_id,
        text=text,
        video=video,
        audio=audio,
        photo=photo,
        media_group_id=media_group_id,
        caption=caption,
        caption_entities=caption_entities,
    )
    message.answer = AsyncMock()
    message.edit_text = AsyncMock()
    message.delete = AsyncMock()
    return message


def _fake_video(*, file_id: str, file_name: str | None) -> SimpleNamespace:
    return SimpleNamespace(file_id=file_id, file_name=file_name)


def _fake_audio(*, file_id: str, file_name: str | None) -> SimpleNamespace:
    return SimpleNamespace(file_id=file_id, file_name=file_name)


def _fake_photo(*, file_id: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(file_id=file_id)]


def _fake_callback(message, *, bot: object | None = None) -> SimpleNamespace:
    callback = SimpleNamespace(message=message)
    callback.answer = AsyncMock()
    if bot is not None:
        callback.bot = bot
    return callback


def _linked_dot_entity(url: str) -> SimpleNamespace:
    return SimpleNamespace(type='text_link', offset=0, length=1, url=url)


def _tracks_by_sub_season_with_id(
    track_id: str,
) -> dict[track_store_module.SubSeason, list[track_store_module.TrackInfo]]:
    return {
        track_store_module.SubSeason.A: [
            track_store_module.TrackInfo(
                id=track_id,
                artists=('Artist',),
                title='Title',
                has_instrumental=False,
            )
        ]
    }


def _tracks_by_sub_season_with_id_and_sub_season(
    track_id: str,
    sub_season: track_store_module.SubSeason,
) -> dict[track_store_module.SubSeason, list[track_store_module.TrackInfo]]:
    return {
        sub_season: [
            track_store_module.TrackInfo(
                id=track_id,
                artists=('Artist',),
                title='Title',
                has_instrumental=False,
            )
        ]
    }


def _keyboard_rows(reply_markup) -> list[list[str]]:
    return [[button.text for button in row] for row in reply_markup.inline_keyboard]


def _assert_format_kwargs(actual: dict[str, object], expected: dict[str, object]) -> None:
    for key, value in expected.items():
        assert actual[key] == value


def _assert_route_progress_edit(edit_call, *routes: tuple[str, ...]) -> None:
    assert edit_call.args == ()
    _assert_format_kwargs(edit_call.kwargs, {**_route_progress_kwargs(*routes), 'reply_markup': None})
    assert 'Route' not in edit_call.kwargs['text']
    assert 'Selected:' not in edit_call.kwargs['text']
    assert '·' not in edit_call.kwargs['text']
    assert '\n\n' not in edit_call.kwargs['text']
    assert '→' in edit_call.kwargs['text']


def _assert_one_line_button_message(*, text: str, message_width: int) -> None:
    padding_line = create_padding_line(message_width)
    assert text.split('\n') == [padding_line, '·']


def _assert_two_line_button_message(*, text: str, top_line: str, bottom_line: str, message_width: int) -> None:
    assert text.split('\n') == [top_line, bottom_line]


def _assert_three_rows(reply_markup) -> None:
    assert len(reply_markup.inline_keyboard) == 3


def _reply_keyboard_rows(reply_markup) -> list[list[str]]:
    return [[button.text for button in row] for row in reply_markup.keyboard]


def _assert_no_dummy_buttons(reply_markup) -> None:
    assert all(button.text != DUMMY_BUTTON_TEXT for row in reply_markup.inline_keyboard for button in row)


def _selected_kwargs(*values: str, prompt: str | None = None, message_width: int | None = None) -> dict[str, object]:
    parts: list[object] = ['Selected: ']
    for index, value in enumerate(values):
        if index > 0:
            parts.append(' / ')
        parts.append(Bold(value))

    selected = Text(*parts)
    if prompt is None:
        return selected.as_kwargs()
    if message_width is None:
        raise ValueError('`message_width` is required when `prompt` is provided')
    return Text(
        create_padding_line(message_width),
        '\n',
        selected,
    ).as_kwargs()


def _reorder_selected_kwargs(
    *values: str | int,
    prompt: str | None = None,
    message_width: int | None = None,
) -> dict[str, object]:
    parts: list[object] = ['Selected: ', Bold('Reorder')]
    if values:
        parts.append(' -> ')
        for index, value in enumerate(values):
            if index > 0:
                parts.append(' ')
            parts.append(Bold(str(value)))

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


def _route_progress_kwargs(*routes: tuple[str, ...]) -> dict[str, object]:
    parts: list[object] = ['Routing...']
    for route in routes:
        line_parts: list[object] = ['→ ']
        for index, value in enumerate(route):
            if index > 0:
                line_parts.append(' → ')
            line_parts.append(Bold(value))
        parts.extend(['\n', Text(*line_parts)])
    return Text(*parts).as_kwargs()


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


def test_route_progress_kwargs_preserves_arrow_path_rendering_without_route_label() -> None:
    actual = _route_progress_kwargs(
        ('West', '2025', '1', 'Source'),
        ('East', '2024', '2', 'Source'),
    )
    expected = Text(
        'Routing...',
        '\n',
        Text(
            '→ ',
            Bold('West'),
            ' → ',
            Bold('2025'),
            ' → ',
            Bold('1'),
            ' → ',
            Bold('Source'),
        ),
        '\n',
        Text(
            '→ ',
            Bold('East'),
            ' → ',
            Bold('2024'),
            ' → ',
            Bold('2'),
            ' → ',
            Bold('Source'),
        ),
    ).as_kwargs()

    assert actual == expected
    assert 'Route' not in actual['text']
    assert 'Selected:' not in actual['text']
    assert '·' not in actual['text']
    assert '\n\n' not in actual['text']


def test_handlers_package_router_imports_cleanly() -> None:
    from timeline_hub.app import run
    from timeline_hub.handlers.router import router

    assert callable(run)
    assert router is not None


@pytest.mark.asyncio
async def test_on_start_sends_only_clips_entry() -> None:
    message = _fake_message()

    await on_start_send_menu(message)

    message.answer.assert_awaited_once_with(
        text='Menu loaded',
        reply_markup=message.answer.await_args.kwargs['reply_markup'],
    )
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    assert _reply_keyboard_rows(reply_markup) == [['Clips', 'Tracks']]


def test_selection_labels_omits_none_sub_season_from_visible_path() -> None:
    assert selection_labels(
        universe=Universe.WEST,
        year=2026,
        season=Season.S1,
        sub_season=SubSeason.NONE,
        scope=Scope.COLLECTION,
    ) == ['West', '2026', '1', 'Collection']


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


@pytest.mark.parametrize(
    ('text', 'expected'),
    [
        ('W242', ClipGroup(universe=Universe.WEST, year=2024, season=Season.S2)),
        ('e215', ClipGroup(universe=Universe.EAST, year=2021, season=Season.S5)),
    ],
)
def test_parse_route_text_accepts_case_insensitive_universe(
    text: str,
    expected: ClipGroup,
) -> None:
    assert parse_route_text(text) == expected


@pytest.mark.asyncio
async def test_on_clips_sends_retrieve_entry_button() -> None:
    message = _fake_message(text='Clips')
    state = _FakeState()
    settings = _settings()

    await on_clips(message, state, settings)

    message.answer.assert_awaited_once()
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_one_line_button_message(
        text=message.answer.await_args.kwargs['text'],
        message_width=settings.message_width,
    )
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Get'], ['Pull'], ['Cancel']]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_on_tracks_sends_track_entry_button() -> None:
    message = _fake_message(text='Tracks')
    state = _FakeState()
    settings = _settings()

    await on_tracks(message, state, settings)

    message.answer.assert_awaited_once()
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_one_line_button_message(
        text=message.answer.await_args.kwargs['text'],
        message_width=settings.message_width,
    )
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Get'], [DUMMY_BUTTON_TEXT], ['Cancel']]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_track_retrieve_entry_get_opens_universe_menu_with_existing_values_only() -> None:
    message = _fake_message(text='Tracks', message_id=15)
    callback = _fake_callback(message)
    state = _FakeState()
    group_west = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    group_east = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.EAST,
        year=2025,
        season=track_store_module.Season.S2,
    )
    track_store = _RetrieveTrackStore(
        groups=[group_west, group_east],
        tracks_by_group={},
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store)
    settings = _settings()

    await on_tracks_retrieve_entry(
        callback,
        TracksRetrieveEntryCallbackData(action=TracksRetrieveEntryAction.GET),
        services,
        settings,
        state,
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', prompt='Select universe:', message_width=settings.message_width),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [[DUMMY_BUTTON_TEXT, 'West'], [DUMMY_BUTTON_TEXT, 'East'], ['Back']]
    assert state.current_state == TrackRetrieveFlow.universe.state


@pytest.mark.asyncio
async def test_track_retrieve_entry_auto_skips_single_universe_when_bot_is_available() -> None:
    message = _fake_message(text='Tracks', message_id=15)
    callback = _fake_callback(message)
    state = _FakeState()
    group_west_2024 = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    group_west_2025 = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2025,
        season=track_store_module.Season.S2,
    )
    track_store = _RetrieveTrackStore(
        groups=[group_west_2024, group_west_2025],
        tracks_by_group={},
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store)
    settings = _settings()

    await on_tracks_retrieve_entry(
        callback,
        TracksRetrieveEntryCallbackData(action=TracksRetrieveEntryAction.GET),
        services,
        settings,
        state,
        AsyncMock(),
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', prompt='Select year:', message_width=settings.message_width),
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    assert state.current_state == TrackRetrieveFlow.year.state


@pytest.mark.asyncio
async def test_on_retrieve_entry_edits_to_no_clips_stored_when_empty() -> None:
    message = _fake_message(text='Clips', message_id=10)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace(list_groups=AsyncMock(return_value=[])))
    settings = _settings()

    await on_retrieve_entry(
        callback,
        RetrieveEntryCallbackData(action=RetrieveEntryAction.GET),
        services,
        settings,
        state,
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', prompt='Select universe:', message_width=settings.message_width),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [[DUMMY_BUTTON_TEXT], [DUMMY_BUTTON_TEXT], ['Back']]
    services.clip_store.list_groups.assert_awaited_once()
    assert state.current_state == RetrieveClipFlow.universe.state
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_clip_retrieve_entry_auto_skips_single_universe_when_bot_is_available() -> None:
    message = _fake_message(text='Select action:', message_id=111)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ]
            )
        )
    )

    await on_retrieve_entry(
        callback,
        RetrieveEntryCallbackData(action=RetrieveEntryAction.GET),
        services,
        _settings(),
        state,
        AsyncMock(),
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', prompt='Select year:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    assert state.current_state == RetrieveClipFlow.year.state


@pytest.mark.asyncio
async def test_on_retrieve_entry_cancel_removes_buttons_and_shows_selected_text() -> None:
    message = _fake_message(text='Select action:', message_id=12)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace(list_groups=AsyncMock()))

    await on_retrieve_entry(
        callback,
        RetrieveEntryCallbackData(action=RetrieveEntryAction.CANCEL),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Canceled', reply_markup=None)
    message.delete.assert_not_awaited()
    services.clip_store.list_groups.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_on_track_retrieve_entry_cancel_removes_buttons_and_shows_selected_text() -> None:
    message = _fake_message(text='Select action:', message_id=14)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace(), track_store=SimpleNamespace(list_groups=AsyncMock()))

    await on_tracks_retrieve_entry(
        callback,
        TracksRetrieveEntryCallbackData(action=TracksRetrieveEntryAction.CANCEL),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Canceled', reply_markup=None)
    message.delete.assert_not_awaited()
    services.track_store.list_groups.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_track_retrieve_flow_shows_only_existing_year_season_and_sub_season_values() -> None:
    message = _fake_message(text='Select universe:', message_id=16)
    callback = _fake_callback(message)
    state = _FakeState()
    group_west_s1 = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    group_west_s3 = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S3,
    )
    group_east_s2 = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.EAST,
        year=2025,
        season=track_store_module.Season.S2,
    )
    track_store = _RetrieveTrackStore(
        groups=[group_west_s1, group_west_s3, group_east_s2],
        tracks_by_group={
            group_west_s1: {
                track_store_module.SubSeason.NONE: [
                    track_store_module.TrackInfo(
                        id='018f05c1f1a37b348d291f53a1c9d101',
                        artists=('artist',),
                        title='none track',
                        has_instrumental=False,
                    )
                ],
                track_store_module.SubSeason.B: [
                    track_store_module.TrackInfo(
                        id='018f05c1f1a37b348d291f53a1c9d102',
                        artists=('artist',),
                        title='b track',
                        has_instrumental=False,
                    )
                ],
            }
        },
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store)
    settings = _settings()
    await state.set_state(TrackRetrieveFlow.universe)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=16,
        groups=[group_west_s1, group_west_s3, group_east_s2],
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.SELECT,
            step=TrackRetrieveStep.UNIVERSE,
            value=track_store_module.TrackUniverse.WEST.value,
        ),
        AsyncMock(),
        services,
        settings,
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', '2024', prompt='Select season:', message_width=settings.message_width),
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        [DUMMY_BUTTON_TEXT, '1'],
        [DUMMY_BUTTON_TEXT, '3', DUMMY_BUTTON_TEXT],
        ['Back'],
    ]
    assert state.current_state == TrackRetrieveFlow.season.state

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.SELECT,
            step=TrackRetrieveStep.SEASON,
            value='1',
        ),
        AsyncMock(),
        services,
        settings,
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', '2024', '1', prompt='Select sub-season:', message_width=settings.message_width),
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        [DUMMY_BUTTON_TEXT, 'None'],
        [DUMMY_BUTTON_TEXT, 'B', DUMMY_BUTTON_TEXT],
        ['Back'],
    ]
    assert state.current_state == TrackRetrieveFlow.sub_season.state


@pytest.mark.asyncio
async def test_track_retrieve_back_from_universe_returns_to_root_menu() -> None:
    message = _fake_message(text='Select universe:', message_id=17)
    callback = _fake_callback(message)
    state = _FakeState()
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    await state.set_state(TrackRetrieveFlow.universe)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=17,
        groups=[group],
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.BACK,
            step=TrackRetrieveStep.UNIVERSE,
            value='back',
        ),
        AsyncMock(),
        _services(clip_store=SimpleNamespace(), track_store=SimpleNamespace()),
        _settings(),
        state,
    )

    _assert_one_line_button_message(
        text=message.edit_text.await_args.kwargs['text'],
        message_width=35,
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        ['Get'],
        [DUMMY_BUTTON_TEXT],
        ['Cancel'],
    ]
    assert state.current_state is None


@pytest.mark.asyncio
async def test_track_retrieve_back_from_season_returns_to_year_menu() -> None:
    message = _fake_message(text='Select season:', message_id=18)
    callback = _fake_callback(message)
    state = _FakeState()
    groups = [
        track_store_module.TrackGroup(
            universe=track_store_module.TrackUniverse.WEST,
            year=2024,
            season=track_store_module.Season.S1,
        ),
        track_store_module.TrackGroup(
            universe=track_store_module.TrackUniverse.WEST,
            year=2025,
            season=track_store_module.Season.S2,
        ),
    ]
    await state.set_state(TrackRetrieveFlow.season)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=18,
        groups=groups,
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.BACK,
            step=TrackRetrieveStep.SEASON,
            value='back',
        ),
        AsyncMock(),
        _services(clip_store=SimpleNamespace(), track_store=SimpleNamespace()),
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', prompt='Select year:', message_width=35),
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    assert state.current_state == TrackRetrieveFlow.year.state


@pytest.mark.asyncio
async def test_track_retrieve_back_from_season_skips_single_year_to_universe_menu() -> None:
    message = _fake_message(text='Select season:', message_id=18)
    callback = _fake_callback(message)
    state = _FakeState()
    groups = [
        track_store_module.TrackGroup(
            universe=track_store_module.TrackUniverse.WEST,
            year=2024,
            season=track_store_module.Season.S1,
        ),
        track_store_module.TrackGroup(
            universe=track_store_module.TrackUniverse.WEST,
            year=2024,
            season=track_store_module.Season.S2,
        ),
    ]
    await state.set_state(TrackRetrieveFlow.season)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=18,
        groups=groups,
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.BACK,
            step=TrackRetrieveStep.SEASON,
            value='back',
        ),
        AsyncMock(),
        _services(clip_store=SimpleNamespace(), track_store=SimpleNamespace()),
        _settings(),
        state,
    )

    _assert_one_line_button_message(
        text=message.edit_text.await_args.kwargs['text'],
        message_width=35,
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        ['Get'],
        [DUMMY_BUTTON_TEXT],
        ['Cancel'],
    ]
    assert state.current_state is None


@pytest.mark.asyncio
async def test_track_retrieve_removed_track_id_during_fetch_becomes_stale_selection() -> None:
    message = _fake_message(text='Select sub-season:', chat_id=9, message_id=22)
    callback = _fake_callback(message)
    state = _FakeState()
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    track = track_store_module.TrackInfo(
        id='018f05c1f1a37b348d291f53a1c9d141',
        artists=('artist',),
        title='Removed',
        has_instrumental=False,
    )

    async def _missing_fetch(
        fetch_group: track_store_module.TrackGroup,
        track_id: str,
    ) -> track_store_module.FetchedVariants:
        raise ValueError(
            f'Track id {track_id} does not exist in group '
            f'{fetch_group.universe.value}-{fetch_group.year}-{int(fetch_group.season)}'
        )

    services = _services(
        clip_store=SimpleNamespace(),
        track_store=SimpleNamespace(fetch=AsyncMock(side_effect=_missing_fetch)),
    )
    await state.set_state(TrackRetrieveFlow.sub_season)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=22,
        groups=[group],
        universe=group.universe,
        year=group.year,
        season=group.season,
        tracks_by_sub_season={track_store_module.SubSeason.A: [track]},
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.SELECT,
            step=TrackRetrieveStep.SUB_SEASON,
            value=track_store_module.SubSeason.A.value,
        ),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    assert message.edit_text.await_args_list[-1].args == ('Selection is no longer available',)
    assert message.edit_text.await_args_list[-1].kwargs == {'reply_markup': None}


@pytest.mark.asyncio
async def test_track_retrieve_sub_season_selection_fetches_all_before_sending_and_sends_reverse_track_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = _fake_message(text='Select sub-season:', chat_id=9, message_id=19)
    callback = _fake_callback(message)
    state = _FakeState()
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    track_1 = track_store_module.TrackInfo(
        id='018f05c1f1a37b348d291f53a1c9d111',
        artists=('artist',),
        title='First',
        has_instrumental=False,
    )
    track_2 = track_store_module.TrackInfo(
        id='018f05c1f1a37b348d291f53a1c9d112',
        artists=('artist',),
        title='Second',
        has_instrumental=True,
    )
    events: list[tuple[str, object]] = []
    track_store = _RetrieveTrackStore(
        groups=[group],
        tracks_by_group={group: {track_store_module.SubSeason.A: [track_1, track_2]}},
        fetched_by_track_id={
            track_1.id: _fetched_track(
                track_id=track_1.id,
                title='First',
                cover=b'cover-1',
                variants=[_fetched_variant(data=b'first-main', level=1, speed=0.95, reverb=0.06)],
            ),
            track_2.id: _fetched_track(
                track_id=track_2.id,
                title='Second',
                cover=b'cover-2',
                variants=[
                    _fetched_variant(data=b'second-main-1', level=2, speed=1.12, reverb=0.01),
                    _fetched_variant(data=b'second-main-2', level=1, speed=0.95, reverb=0.12),
                ],
                instrumental_variants=[_fetched_variant(data=b'second-inst', level=3, speed=1.08, reverb=0.01)],
            ),
        },
        events=events,
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store)
    bot = _RecordingTrackBot(events)
    pad_image_to_width_factor = Mock(
        side_effect=lambda image_bytes, *, width_factor=2.0, background='white', quality=95: image_bytes
    )
    monkeypatch.setattr(track_retrieve_module, 'pad_image_to_width_factor', pad_image_to_width_factor)
    await state.set_state(TrackRetrieveFlow.sub_season)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=19,
        groups=[group],
        universe=group.universe,
        year=group.year,
        season=group.season,
        tracks_by_sub_season={track_store_module.SubSeason.A: [track_1, track_2]},
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.SELECT,
            step=TrackRetrieveStep.SUB_SEASON,
            value=track_store_module.SubSeason.A.value,
        ),
        bot,
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', '2024', '1', 'A'),
    )
    assert events[:2] == [
        ('fetch', (group, track_1.id)),
        ('fetch', (group, track_2.id)),
    ]
    assert pad_image_to_width_factor.call_args_list == [
        call(b'cover-2', width_factor=2.0, background='blur'),
        call(b'cover-1', width_factor=2.0, background='blur'),
    ]
    assert events[2:] == [
        (
            'photo',
            {
                'chat_id': 9,
                'filename': f'{track_store_module.TrackStore.track_identity_to_string(group, track_2.id)}-cover.jpg',
                'data': b'cover-2',
                'caption': '·Second',
                'caption_entities': [
                    {
                        'type': 'text_link',
                        'offset': 0,
                        'length': 1,
                        'url': (
                            f'https://{track_store_module.TrackStore.track_identity_to_string(group, track_2.id)}.com'
                        ),
                    }
                ],
                'parse_mode': None,
            },
        ),
        (
            'media_group',
            {
                'chat_id': 9,
                'files': [
                    {
                        'filename': '++',
                        'data': b'second-main-1',
                        'thumbnail_filename': None,
                        'thumbnail_data': None,
                        'performer': None,
                        'title': None,
                    },
                    {
                        'filename': '--',
                        'data': b'second-main-2',
                        'thumbnail_filename': None,
                        'thumbnail_data': None,
                        'performer': None,
                        'title': None,
                    },
                ],
            },
        ),
        (
            'audio',
            {
                'chat_id': 9,
                'filename': '++',
                'data': b'second-inst',
                'thumbnail_filename': None,
                'thumbnail_data': None,
                'performer': None,
                'title': None,
            },
        ),
        (
            'photo',
            {
                'chat_id': 9,
                'filename': f'{track_store_module.TrackStore.track_identity_to_string(group, track_1.id)}-cover.jpg',
                'data': b'cover-1',
                'caption': '·First',
                'caption_entities': [
                    {
                        'type': 'text_link',
                        'offset': 0,
                        'length': 1,
                        'url': (
                            f'https://{track_store_module.TrackStore.track_identity_to_string(group, track_1.id)}.com'
                        ),
                    }
                ],
                'parse_mode': None,
            },
        ),
        (
            'audio',
            {
                'chat_id': 9,
                'filename': '--',
                'data': b'first-main',
                'thumbnail_filename': None,
                'thumbnail_data': None,
                'performer': None,
                'title': None,
            },
        ),
    ]
    assert state.current_state is None


def test_track_retrieve_variant_filename_rejects_unmodified_speed() -> None:
    variant = _fetched_variant(data=b'audio', level=1, speed=1.0, reverb=0.06)

    with pytest.raises(ValueError, match='speed must not be 1.0'):
        track_retrieve_module._variant_filename(variant)


def test_track_retrieve_variant_filename_uses_directional_tokens() -> None:
    fast_variant = _fetched_variant(data=b'audio', level=1, speed=1.12, reverb=0.06)
    slow_variant = _fetched_variant(data=b'audio', level=1, speed=0.95, reverb=0.06)

    assert track_retrieve_module._variant_filename(fast_variant) == '++'
    assert track_retrieve_module._variant_filename(slow_variant) == '--'


@pytest.mark.asyncio
async def test_track_retrieve_invalid_sub_season_callback_becomes_stale() -> None:
    message = _fake_message(text='Select sub-season:', message_id=20)
    callback = _fake_callback(message)
    state = _FakeState()
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    await state.set_state(TrackRetrieveFlow.sub_season)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=20,
        groups=[group],
        universe=group.universe,
        year=group.year,
        season=group.season,
        tracks_by_sub_season={
            track_store_module.SubSeason.A: [
                track_store_module.TrackInfo(
                    id='018f05c1f1a37b348d291f53a1c9d121',
                    artists=('artist',),
                    title='only',
                    has_instrumental=False,
                )
            ]
        },
    )

    await on_tracks_retrieve_menu(
        callback,
        TrackRetrieveCallbackData(
            action=TrackRetrieveAction.SELECT,
            step=TrackRetrieveStep.SUB_SEASON,
            value=track_store_module.SubSeason.B.value,
        ),
        AsyncMock(),
        _services(clip_store=SimpleNamespace(), track_store=SimpleNamespace(fetch=AsyncMock())),
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert state.current_state is None


@pytest.mark.asyncio
async def test_track_retrieve_raises_value_error_when_fetched_variant_list_exceeds_ten() -> None:
    message = _fake_message(text='Select sub-season:', chat_id=9, message_id=21)
    callback = _fake_callback(message)
    state = _FakeState()
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2024,
        season=track_store_module.Season.S1,
    )
    track = track_store_module.TrackInfo(
        id='018f05c1f1a37b348d291f53a1c9d131',
        artists=('artist',),
        title='Overflow',
        has_instrumental=False,
    )
    variants = [_fetched_variant(data=f'v{index}'.encode()) for index in range(11)]
    track_store = _RetrieveTrackStore(
        groups=[group],
        tracks_by_group={group: {track_store_module.SubSeason.A: [track]}},
        fetched_by_track_id={
            track.id: _fetched_track(
                track_id=track.id,
                title='Overflow',
                cover=b'cover',
                variants=variants,
            )
        },
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store)
    await state.set_state(TrackRetrieveFlow.sub_season)
    await state.update_data(
        mode=track_retrieve_module._TRACK_GET_MODE,
        menu_message_id=21,
        groups=[group],
        universe=group.universe,
        year=group.year,
        season=group.season,
        tracks_by_sub_season={track_store_module.SubSeason.A: [track]},
    )

    with pytest.raises(ValueError, match='at most 10 items'):
        await on_tracks_retrieve_menu(
            callback,
            TrackRetrieveCallbackData(
                action=TrackRetrieveAction.SELECT,
                step=TrackRetrieveStep.SUB_SEASON,
                value=track_store_module.SubSeason.A.value,
            ),
            AsyncMock(),
            services,
            _settings(),
            state,
        )


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
    message.edit_text.assert_awaited_once_with('Canceled', reply_markup=None)
    message.delete.assert_not_awaited()
    message.answer.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_video_and_text_dispatch_to_clip_action_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    text_message = _fake_message(chat_id=42, message_id=1, text='note')
    message = _fake_message(
        chat_id=42,
        message_id=2,
        video=_fake_video(file_id='file-1', file_name='clip.mp4'),
    )

    await on_buffered_relevant_message(text_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(
        message.answer.await_args.kwargs,
        expected,
    )
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        ['Route', 'Store', 'Reorder'],
        ['Reconcile', 'Produce', 'Compact'],
        ['Cancel'],
    ]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]


@pytest.mark.asyncio
async def test_valid_photo_audio_pairs_dispatch_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')
    second_message = _fake_message(
        chat_id=42,
        message_id=2,
        audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
    )
    third_message = _fake_message(chat_id=42, message_id=3, photo=[object()], caption='Another Artist\nAnother Title')
    message = _fake_message(
        chat_id=42,
        message_id=4,
        audio=_fake_audio(file_id='audio-2', file_name='two.mp3'),
    )

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(second_message, services, settings)
    await on_buffered_relevant_message(third_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('4')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [
        1,
        2,
        3,
        4,
    ]


@pytest.mark.asyncio
async def test_valid_out_of_order_appended_track_batch_dispatches_in_message_order() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_appended = _fake_message(
        chat_id=42,
        message_id=2,
        audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
    )
    second_appended = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')
    third_appended = _fake_message(
        chat_id=42,
        message_id=4,
        audio=_fake_audio(file_id='audio-2', file_name='two.mp3'),
    )
    message = _fake_message(chat_id=42, message_id=3, photo=[object()], caption='Another Artist\nAnother Title')

    await on_buffered_relevant_message(first_appended, services, settings)
    await on_buffered_relevant_message(second_appended, services, settings)
    await on_buffered_relevant_message(third_appended, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('4')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [
        2,
        1,
        4,
        3,
    ]


@pytest.mark.asyncio
async def test_video_and_audio_batch_is_rejected_and_flushed() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))
    message = _fake_message(chat_id=42, message_id=3, video=_fake_video(file_id='video-1', file_name='clip.mp4'))

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    message.answer.assert_awaited_once_with(text="Can't dispatch")
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_audio_before_photo_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    first_message = _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))
    message = _fake_message(chat_id=42, message_id=2, photo=[object()], caption='Artist\nTitle')

    await on_buffered_relevant_message(first_message, services, _settings())
    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(_settings().message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]


@pytest.mark.asyncio
async def test_photo_without_caption_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption=None)
    message = _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]


@pytest.mark.asyncio
async def test_single_line_caption_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Title only')
    message = _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]


@pytest.mark.asyncio
async def test_odd_number_of_track_candidate_messages_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')
    second_message = _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))
    message = _fake_message(chat_id=42, message_id=3, photo=[object()], caption='Another Artist\nAnother Title')

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(second_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('3')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]


@pytest.mark.asyncio
async def test_audio_only_batch_is_rejected_and_flushed() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    first_message = _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))
    message = _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-2', file_name='two.mp3'))

    await on_buffered_relevant_message(first_message, services, _settings())
    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(_settings().message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]


@pytest.mark.asyncio
async def test_photo_only_batch_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')
    message = _fake_message(chat_id=42, message_id=2, photo=[object()], caption='Another Artist\nAnother Title')

    await on_buffered_relevant_message(first_message, services, _settings())
    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(_settings().message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]


@pytest.mark.asyncio
async def test_photo_only_single_message_shows_fallback_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')

    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(_settings().message_width),
        '\n',
        Text('Messages: ', Bold('1')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1]


@pytest.mark.asyncio
async def test_video_and_photo_batch_is_rejected_and_flushed() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    first_message = _fake_message(chat_id=42, message_id=1, video=_fake_video(file_id='video-1', file_name='one.mp4'))
    message = _fake_message(chat_id=42, message_id=2, photo=[object()], caption='Artist\nTitle')

    await on_buffered_relevant_message(first_message, services, _settings())
    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    message.answer.assert_awaited_once_with(text="Can't dispatch")
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_video_photo_audio_batch_is_rejected_and_flushed() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    first_message = _fake_message(chat_id=42, message_id=1, video=_fake_video(file_id='video-1', file_name='one.mp4'))
    second_message = _fake_message(chat_id=42, message_id=2, photo=[object()], caption='Artist\nTitle')
    message = _fake_message(chat_id=42, message_id=3, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))

    await on_buffered_relevant_message(first_message, services, _settings())
    await on_buffered_relevant_message(second_message, services, _settings())
    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    message.answer.assert_awaited_once_with(text="Can't dispatch")
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_photo_text_audio_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')
    second_message = _fake_message(chat_id=42, message_id=2, text='ignore me')
    message = _fake_message(chat_id=42, message_id=3, audio=_fake_audio(file_id='audio-1', file_name='one.mp3'))

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(second_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('3')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]


@pytest.mark.asyncio
async def test_photo_and_text_batch_dispatches_to_track_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    settings = _settings()
    first_message = _fake_message(chat_id=42, message_id=1, photo=[object()], caption='Artist\nTitle')
    message = _fake_message(chat_id=42, message_id=2, text='note')

    await on_buffered_relevant_message(first_message, services, settings)
    await on_buffered_relevant_message(message, services, settings)
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(settings.message_width),
        '\n',
        Text('Messages: ', Bold('2')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['Store'], ['Replace'], ['Cancel']]


@pytest.mark.asyncio
async def test_text_only_buffered_batch_shows_fallback_menu() -> None:
    scheduler = _FakeScheduler()
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), scheduler=scheduler, buffer=buffer)
    message = _fake_message(
        chat_id=42,
        message_id=1,
        text='note',
    )

    await on_buffered_relevant_message(message, services, _settings())
    assert scheduler.job is not None

    await scheduler.job()

    expected = Text(
        create_padding_line(_settings().message_width),
        '\n',
        Text('Messages: ', Bold('1')),
    ).as_kwargs()
    _assert_format_kwargs(message.answer.await_args.kwargs, expected)
    reply_markup = message.answer.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['Cancel']]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1]


@pytest.mark.asyncio
async def test_try_dispatch_clip_intake_returns_false_without_videos() -> None:
    message = _fake_message(chat_id=42, message_id=1, text='note')
    buffer = ChatMessageBuffer()
    buffer.append(message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    handled = await try_dispatch_clip_intake(
        message=message,
        services=services,
        settings=_settings(),
    )

    assert handled is False
    message.answer.assert_not_awaited()
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1]


@pytest.mark.asyncio
async def test_try_dispatch_track_intake_shows_menu_without_audio() -> None:
    message = _fake_message(chat_id=42, message_id=1, text='note')
    buffer = ChatMessageBuffer()
    buffer.append(message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    handled = await try_dispatch_track_intake(
        message=message,
        services=services,
        settings=_settings(),
    )

    assert handled is True
    _assert_format_kwargs(
        message.answer.await_args.kwargs,
        {
            **Text(
                create_padding_line(_settings().message_width),
                '\n',
                Text('Messages: ', Bold('1')),
            ).as_kwargs(),
            'reply_markup': message.answer.await_args.kwargs['reply_markup'],
        },
    )
    assert _keyboard_rows(message.answer.await_args.kwargs['reply_markup']) == [
        ['Store'],
        ['Replace'],
        ['Cancel'],
    ]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1]


@pytest.mark.asyncio
async def test_fallback_cancel_flushes_buffer() -> None:
    buffer = ChatMessageBuffer()
    buffered_message = _fake_message(chat_id=42, message_id=1, text='note')
    buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(chat_id=42, message_id=10, text='Messages: 1')
    callback = _fake_callback(menu_message)
    state = _FakeState()

    await on_intake_fallback_cancel(
        callback,
        IntakeFallbackCallbackData(action='cancel', buffer_version=buffer.version(42)),
        services,
        state,
    )

    callback.answer.assert_awaited_once()
    menu_message.edit_text.assert_awaited_once_with('Canceled', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_fallback_cancel_stale_does_not_flush_newer_buffer() -> None:
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=42, message_id=1, text='note'), chat_id=42)
    rendered_version = buffer.version(42)
    buffer.append(_fake_message(chat_id=42, message_id=2, text='newer'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(chat_id=42, message_id=11, text='Messages: 1')
    callback = _fake_callback(menu_message)
    state = _FakeState()

    await on_intake_fallback_cancel(
        callback,
        IntakeFallbackCallbackData(action='cancel', buffer_version=rendered_version),
        services,
        state,
    )

    callback.answer.assert_awaited_once()
    menu_message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]


@pytest.mark.asyncio
async def test_try_dispatch_track_intake_shows_menu_with_multiple_pairs() -> None:
    message = _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-2', file_name='two.mp3'))
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=3,
            photo=_fake_photo(file_id='photo-2'),
            caption='Another Artist\nAnother Title',
        ),
        chat_id=42,
    )
    buffer.append(message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    handled = await try_dispatch_track_intake(
        message=message,
        services=services,
        settings=_settings(),
    )

    assert handled is True
    _assert_format_kwargs(
        message.answer.await_args.kwargs,
        {
            **Text(
                create_padding_line(_settings().message_width),
                '\n',
                Text('Messages: ', Bold('4')),
            ).as_kwargs(),
            'reply_markup': message.answer.await_args.kwargs['reply_markup'],
        },
    )
    assert _keyboard_rows(message.answer.await_args.kwargs['reply_markup']) == [
        ['Store'],
        ['Replace'],
        ['Cancel'],
    ]


@pytest.mark.asyncio
async def test_track_intake_replace_opens_replace_submenu_without_flushing() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=14)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=42, message_id=1, text='note'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.REPLACE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        {
            **Text(
                create_padding_line(_settings().message_width),
                '\n',
                Text('Messages: ', Bold('1')),
            ).as_kwargs(),
            'reply_markup': message.edit_text.await_args.kwargs['reply_markup'],
        },
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        ['Track'],
        ['Instrumental'],
        ['Back'],
    ]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1]
    assert state.clear_count == 0


@pytest.mark.asyncio
async def test_track_intake_back_returns_to_root_menu_without_flushing() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=14)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=42, message_id=1, text='note'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.BACK,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        {
            **Text(
                create_padding_line(_settings().message_width),
                '\n',
                Text('Messages: ', Bold('1')),
            ).as_kwargs(),
            'reply_markup': message.edit_text.await_args.kwargs['reply_markup'],
        },
    )
    assert _keyboard_rows(message.edit_text.await_args.kwargs['reply_markup']) == [
        ['Store'],
        ['Replace'],
        ['Cancel'],
    ]
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1]
    assert state.clear_count == 0


@pytest.mark.asyncio
async def test_track_replace_action_updates_track_audio_and_flushes_buffer(monkeypatch: pytest.MonkeyPatch) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_awaited_once_with(
        group,
        track_id,
        track=FileBytes(data=b'opus-1', extension=Extension.OPUS),
    )
    assert 'instrumental' not in track_store.update.await_args.kwargs
    _assert_format_kwargs(
        message.edit_text.await_args_list[0].kwargs,
        _selected_kwargs('Track', 'West', '2026', '1', 'A'),
    )
    assert message.edit_text.await_args_list[0].kwargs['reply_markup'] is None
    message.answer.assert_awaited_once_with('Done')
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_track_replace_action_updates_instrumental_and_flushes_buffer(monkeypatch: pytest.MonkeyPatch) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com/')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.INSTRUMENTAL, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_awaited_once_with(
        group,
        track_id,
        instrumental=FileBytes(data=b'opus-1', extension=Extension.OPUS),
    )
    assert 'track' not in track_store.update.await_args.kwargs
    _assert_format_kwargs(
        message.edit_text.await_args_list[0].kwargs,
        _selected_kwargs('Instrumental', 'West', '2026', '1', 'A'),
    )
    assert message.edit_text.await_args_list[0].kwargs['reply_markup'] is None
    message.answer.assert_awaited_once_with('Done')
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
@pytest.mark.parametrize('photo_first', [True, False])
async def test_track_replace_action_accepts_photo_audio_in_any_order(
    photo_first: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_2
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    photo_message = _fake_message(
        chat_id=42,
        message_id=1,
        photo=_fake_photo(file_id='photo-1'),
        caption='·Title',
        caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
    )
    audio_message = _fake_message(
        chat_id=42,
        message_id=2,
        audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
    )
    buffer = ChatMessageBuffer()
    for item in [photo_message, audio_message] if photo_first else [audio_message, photo_message]:
        buffer.append(item, chat_id=42)
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_awaited_once()
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_track_replace_action_hides_none_sub_season_in_selected_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(
            return_value=_tracks_by_sub_season_with_id_and_sub_season(track_id, track_store_module.SubSeason.NONE)
        ),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    _assert_format_kwargs(
        message.edit_text.await_args_list[0].kwargs,
        _selected_kwargs('Track', 'West', '2026', '1'),
    )
    assert ' / None' not in message.edit_text.await_args_list[0].kwargs['text']
    assert ' / none' not in message.edit_text.await_args_list[0].kwargs['text']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'messages',
    [
        [
            _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='·Title'),
        ],
        [
            _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        ],
        [
            _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='·Title'),
            _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
            _fake_message(chat_id=42, message_id=3, text='extra'),
        ],
        [
            _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='·Title'),
            _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
            _fake_message(chat_id=42, message_id=3, audio=_fake_audio(file_id='audio-2', file_name='two.mp3')),
        ],
        [
            _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='·Title'),
            _fake_message(chat_id=42, message_id=2, photo=_fake_photo(file_id='photo-2'), caption='·Title'),
            _fake_message(chat_id=42, message_id=3, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        ],
    ],
)
async def test_track_replace_action_invalid_exact_input_invalidates(messages: list[Message]) -> None:
    buffer = ChatMessageBuffer()
    for buffered_message in messages:
        buffer.append(buffered_message, chat_id=42)
    track_store = SimpleNamespace(update=AsyncMock())
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(message, bot=SimpleNamespace()),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_not_awaited()
    message.edit_text.assert_awaited_with('Invalid input', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('caption', 'caption_entities'),
    [
        (None, [_linked_dot_entity('https://west-2026-1--018f05c1f1a37b348d291f53a1c9d0e1.com')]),
        ('Title', [_linked_dot_entity('https://west-2026-1--018f05c1f1a37b348d291f53a1c9d0e1.com')]),
        ('·Title', None),
        ('·Title', [SimpleNamespace(type='text_link', offset=1, length=1, url='https://x.com')]),
        ('·Title', [SimpleNamespace(type='text_link', offset=0, length=1, url=None)]),
        ('·Title', [_linked_dot_entity('http://west-2026-1--018f05c1f1a37b348d291f53a1c9d0e1.com')]),
        ('·Title', [_linked_dot_entity('https://west-2026-1--018f05c1f1a37b348d291f53a1c9d0e1.net')]),
        ('·Title', [_linked_dot_entity('https://not-valid.com')]),
    ],
)
async def test_track_replace_action_invalid_photo_identity_invalidates(
    caption: str | None,
    caption_entities: list[SimpleNamespace] | None,
) -> None:
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption=caption,
            caption_entities=caption_entities,
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(update=AsyncMock())
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(message, bot=SimpleNamespace()),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_not_awaited()
    message.edit_text.assert_awaited_with('Invalid input', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'update_error',
    [
        track_store_module.TrackGroupNotFoundError(
            universe=track_store_module.TrackUniverse.WEST,
            year=2026,
            season=track_store_module.Season.S1,
            sub_season=None,
        ),
        ValueError('Track id 018f05c1f1a37b348d291f53a1c9d0e1 does not exist in group west-2026-1'),
        track_store_module.TrackInvalidAudioFormatError('Audio sample rate must be 48000 Hz, got 44100'),
    ],
)
async def test_track_replace_action_expected_update_failures_map_to_invalid_input(
    update_error: Exception,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(side_effect=update_error),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    message.edit_text.assert_awaited_with('Invalid input', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_track_replace_action_missing_target_invalidates_before_selected_state() -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(_CLIP_ID_2)),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(message, bot=SimpleNamespace()),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_not_awaited()
    message.edit_text.assert_awaited_once_with('Invalid input', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_track_replace_action_missing_group_invalidates_before_selected_state() -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(
            side_effect=track_store_module.TrackGroupNotFoundError(
                universe=track_store_module.TrackUniverse.WEST,
                year=2026,
                season=track_store_module.Season.S1,
                sub_season=None,
            )
        ),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(message, bot=SimpleNamespace()),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    track_store.update.assert_not_awaited()
    message.edit_text.assert_awaited_once_with('Invalid input', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_track_replace_action_flushes_early_and_preserves_new_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    original_messages = [
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
    ]
    for buffered_message in original_messages:
        buffer.append(buffered_message, chat_id=42)

    async def _update_and_append(*args, **kwargs) -> None:
        buffer.append(_fake_message(chat_id=42, message_id=99, text='new'), chat_id=42)

    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(side_effect=_update_and_append),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    remaining_ids = [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)]
    assert remaining_ids == [99]


@pytest.mark.asyncio
async def test_track_replace_action_operational_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    sync_error = track_store_module.TrackUpdateManifestSyncError(
        stage='manifest_write',
        track_id=track_id,
        touched_keys=['tracks/a'],
        manifest_key='tracks/manifest.json',
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(side_effect=sync_error),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'audio-1')),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    with pytest.raises(track_store_module.TrackUpdateManifestSyncError):
        await on_track_intake_action(
            _fake_callback(message, bot=bot),
            TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
            state,
            services,
            _settings(),
        )


@pytest.mark.asyncio
async def test_track_replace_action_opus_fast_path_skips_to_opus(monkeypatch: pytest.MonkeyPatch) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.opus'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'opus-input')),
    )
    to_opus = AsyncMock(return_value=b'opus-converted')
    monkeypatch.setattr(track_store_execution_module, 'to_opus', to_opus)

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    to_opus.assert_not_awaited()
    assert track_store.update.await_args.kwargs['track'] == FileBytes(data=b'opus-input', extension=Extension.OPUS)


@pytest.mark.asyncio
async def test_track_replace_action_non_opus_calls_to_opus(monkeypatch: pytest.MonkeyPatch) -> None:
    group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=2026,
        season=track_store_module.Season.S1,
    )
    track_id = _CLIP_ID_1
    identity = track_store_module.TrackStore.track_identity_to_string(group, track_id)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=_fake_photo(file_id='photo-1'),
            caption='·Title',
            caption_entities=[_linked_dot_entity(f'https://{identity}.com')],
        ),
        chat_id=42,
    )
    buffer.append(
        _fake_message(
            chat_id=42,
            message_id=2,
            audio=_fake_audio(file_id='audio-1', file_name='one.mp3'),
        ),
        chat_id=42,
    )
    track_store = SimpleNamespace(
        list_tracks=AsyncMock(return_value=_tracks_by_sub_season_with_id(track_id)),
        update=AsyncMock(),
    )
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_path='audio-path-1')),
        download_file=AsyncMock(return_value=BytesIO(b'mp3-input')),
    )
    to_opus = AsyncMock(return_value=b'opus-converted')
    monkeypatch.setattr(track_store_execution_module, 'to_opus', to_opus)

    await on_track_intake_action(
        _fake_callback(message, bot=bot),
        TrackIntakeActionCallbackData(action=TrackIntakeAction.TRACK, buffer_version=buffer.version(42)),
        state,
        services,
        _settings(),
    )

    to_opus.assert_awaited_once_with(b'mp3-input')
    assert track_store.update.await_args.kwargs['track'] == FileBytes(data=b'opus-converted', extension=Extension.OPUS)


@pytest.mark.asyncio
async def test_track_intake_cancel_flushes_buffer() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=15)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=42, message_id=1, text='note'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.CANCEL,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Canceled', reply_markup=None)
    message.delete.assert_not_awaited()
    assert services.chat_message_buffer.peek_raw(42) == []
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_track_intake_cancel_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=16)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='a.mp3')), chat_id=42
    )
    rendered_version = buffer.version(42)
    buffer.append(_fake_message(chat_id=42, message_id=2, text='newer'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.CANCEL,
            buffer_version=rendered_version,
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_track_intake_replace_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=16)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='a.mp3')), chat_id=42
    )
    rendered_version = buffer.version(42)
    buffer.append(_fake_message(chat_id=42, message_id=2, text='newer'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.REPLACE,
            buffer_version=rendered_version,
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_track_intake_back_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=16)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=42, message_id=1, audio=_fake_audio(file_id='audio-1', file_name='a.mp3')), chat_id=42
    )
    rendered_version = buffer.version(42)
    buffer.append(_fake_message(chat_id=42, message_id=2, text='newer'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.BACK,
            buffer_version=rendered_version,
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_fallback_cancel_flushes_buffer_when_current() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=17)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=42, message_id=1, text='note'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.CANCEL,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Canceled', reply_markup=None)
    message.delete.assert_not_awaited()
    assert services.chat_message_buffer.peek_raw(42) == []
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_fallback_cancel_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select action:', chat_id=42, message_id=18)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=42, message_id=1, text='note'), chat_id=42)
    rendered_version = buffer.version(42)
    buffer.append(_fake_message(chat_id=42, message_id=2, text='newer'), chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_track_intake_action(
        callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.CANCEL,
            buffer_version=rendered_version,
        ),
        state,
        services,
        _settings(),
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2]
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_track_store_happy_path_stores_all_prepared_tracks_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    track_store = SimpleNamespace(store=AsyncMock())
    buffer = ChatMessageBuffer()
    buffered_messages = [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist 1\nTitle 1'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        _fake_message(
            chat_id=42,
            message_id=3,
            photo=_fake_photo(file_id='photo-2'),
            caption='Artist 2\nArtist 3\nTitle 2',
        ),
        _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-2', file_name='two.mp3')),
    ]
    for buffered_message in buffered_messages:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=50)
    store_callback = _fake_callback(menu_message)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
                SimpleNamespace(file_path='photo-path-2'),
                SimpleNamespace(file_path='audio-path-2'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(b'cover-1'),
                BytesIO(b'audio-1'),
                BytesIO(b'cover-2'),
                BytesIO(b'audio-2'),
            ]
        ),
    )
    monkeypatch.setattr(
        track_store_execution_module,
        'normalize_cover_to_jpg',
        Mock(side_effect=[b'jpg-1', b'jpg-2']),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(side_effect=[b'opus-1', b'opus-2']))

    await on_track_intake_action(
        store_callback,
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    year = date.today().year - 1
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='A'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            bot,
        )

    assert track_store.store.await_count == 2
    first_call, second_call = track_store.store.await_args_list
    expected_group = track_store_module.TrackGroup(
        universe=track_store_module.TrackUniverse.WEST,
        year=year,
        season=track_store_module.Season.S1,
    )
    assert first_call.args == (expected_group, track_store_module.SubSeason.A)
    assert second_call.args == (expected_group, track_store_module.SubSeason.A)
    assert first_call.kwargs['track'].artists == ('Artist 1',)
    assert first_call.kwargs['track'].title == 'Title 1'
    assert first_call.kwargs['track'].cover == FileBytes(data=b'jpg-1', extension=Extension.JPG)
    assert first_call.kwargs['track'].audio == FileBytes(data=b'opus-1', extension=Extension.OPUS)
    assert second_call.kwargs['track'].artists == ('Artist 2', 'Artist 3')
    assert second_call.kwargs['track'].title == 'Title 2'
    assert second_call.kwargs['track'].cover == FileBytes(data=b'jpg-2', extension=Extension.JPG)
    assert second_call.kwargs['track'].audio == FileBytes(data=b'opus-2', extension=Extension.OPUS)
    _assert_format_kwargs(
        menu_message.edit_text.await_args_list[-1].kwargs,
        _selected_kwargs('Store', 'West', str(year), '1', 'A'),
    )
    assert menu_message.edit_text.await_args_list[-1].kwargs['reply_markup'] is None
    menu_message.answer.assert_awaited_once_with(**Text('Stored: ', Bold('2')).as_kwargs())
    assert services.chat_message_buffer.peek_raw(42) == []
    assert state.current_state is None


@pytest.mark.asyncio
async def test_prepare_tracks_from_buffer_skips_to_opus_for_opus_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='track.opus')),
    ]
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(b'cover-1'),
                BytesIO(b'already-opus'),
            ]
        ),
    )
    to_opus = AsyncMock()
    monkeypatch.setattr(track_store_execution_module, 'normalize_cover_to_jpg', Mock(return_value=b'jpg-1'))
    monkeypatch.setattr(track_store_execution_module, 'to_opus', to_opus)

    prepared_tracks = await track_store_execution_module.prepare_tracks_from_buffer(
        bot=bot,
        messages=messages,
    )

    assert prepared_tracks == [
        track_store_module.Track(
            artists=('Artist',),
            title='Title',
            cover=FileBytes(data=b'jpg-1', extension=Extension.JPG),
            audio=FileBytes(data=b'already-opus', extension=Extension.OPUS),
        )
    ]
    to_opus.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepare_tracks_from_buffer_normalizes_cover_via_image_infra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cover_bytes = b'cover-bytes'
    messages = [
        _fake_message(
            chat_id=42,
            message_id=1,
            photo=[SimpleNamespace(file_id='photo-1')],
            caption='Artist\nTitle',
        ),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='track.mp3')),
    ]
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(cover_bytes),
                BytesIO(b'source-audio'),
            ]
        ),
    )
    normalize_cover_to_jpg = Mock(return_value=b'normalized-cover')
    monkeypatch.setattr(track_store_execution_module, 'normalize_cover_to_jpg', normalize_cover_to_jpg)
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    prepared_tracks = await track_store_execution_module.prepare_tracks_from_buffer(
        bot=bot,
        messages=messages,
    )

    assert prepared_tracks == [
        track_store_module.Track(
            artists=('Artist',),
            title='Title',
            cover=FileBytes(data=b'normalized-cover', extension=Extension.JPG),
            audio=FileBytes(data=b'opus-1', extension=Extension.OPUS),
        )
    ]
    normalize_cover_to_jpg.assert_called_once_with(cover_bytes)


@pytest.mark.asyncio
async def test_prepare_tracks_from_buffer_ignores_interleaved_text_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, text='ignore'),
        _fake_message(chat_id=42, message_id=3, audio=_fake_audio(file_id='audio-1', file_name='track.mp3')),
    ]
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(b'cover-1'),
                BytesIO(b'audio-1'),
            ]
        ),
    )
    monkeypatch.setattr(track_store_execution_module, 'normalize_cover_to_jpg', Mock(return_value=b'jpg-1'))
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    prepared_tracks = await track_store_execution_module.prepare_tracks_from_buffer(
        bot=bot,
        messages=messages,
    )

    assert prepared_tracks == [
        track_store_module.Track(
            artists=('Artist',),
            title='Title',
            cover=FileBytes(data=b'jpg-1', extension=Extension.JPG),
            audio=FileBytes(data=b'opus-1', extension=Extension.OPUS),
        )
    ]


@pytest.mark.asyncio
async def test_prepare_tracks_from_buffer_ignores_random_text_around_valid_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [
        _fake_message(chat_id=42, message_id=1, text='ignore-start'),
        _fake_message(chat_id=42, message_id=2, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=3, text='ignore-middle'),
        _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-1', file_name='track.mp3')),
        _fake_message(chat_id=42, message_id=5, text='ignore-end'),
    ]
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(b'cover-1'),
                BytesIO(b'audio-1'),
            ]
        ),
    )
    monkeypatch.setattr(track_store_execution_module, 'normalize_cover_to_jpg', Mock(return_value=b'jpg-1'))
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    prepared_tracks = await track_store_execution_module.prepare_tracks_from_buffer(
        bot=bot,
        messages=messages,
    )

    assert prepared_tracks == [
        track_store_module.Track(
            artists=('Artist',),
            title='Title',
            cover=FileBytes(data=b'jpg-1', extension=Extension.JPG),
            audio=FileBytes(data=b'opus-1', extension=Extension.OPUS),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('messages', 'expected_error'),
    [
        (
            [
                _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption=None),
                _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
            ],
            "Can't dispatch input",
        ),
        (
            [
                _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Title only'),
                _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
            ],
            "Can't dispatch input",
        ),
        (
            [
                _fake_message(chat_id=42, message_id=1, text='ignore'),
                _fake_message(chat_id=42, message_id=2, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
                _fake_message(chat_id=42, message_id=3, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
                _fake_message(chat_id=42, message_id=4, photo=_fake_photo(file_id='photo-2'), caption='Artist\nTitle'),
            ],
            "Can't dispatch input",
        ),
        (
            [
                _fake_message(chat_id=42, message_id=1, text='ignore'),
                _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
                _fake_message(chat_id=42, message_id=3, text='ignore'),
                _fake_message(chat_id=42, message_id=4, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
            ],
            "Can't dispatch input",
        ),
    ],
)
async def test_prepare_tracks_from_buffer_rejects_invalid_extracted_store_sequence(
    messages: list[Message],
    expected_error: str,
) -> None:
    bot = SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock())

    with pytest.raises(track_store_execution_module.TrackInputError, match=expected_error):
        await track_store_execution_module.prepare_tracks_from_buffer(
            bot=bot,
            messages=messages,
        )

    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_track_store_preserves_message_id_order_even_if_buffer_out_of_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    track_store = SimpleNamespace(store=AsyncMock())
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist 1\nTitle 1'),
        _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-2', file_name='two.mp3')),
        _fake_message(chat_id=42, message_id=3, photo=_fake_photo(file_id='photo-2'), caption='Artist 2\nTitle 2'),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=55)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
                SimpleNamespace(file_path='photo-path-2'),
                SimpleNamespace(file_path='audio-path-2'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(b'cover-1'),
                BytesIO(b'audio-1'),
                BytesIO(b'cover-2'),
                BytesIO(b'audio-2'),
            ]
        ),
    )
    monkeypatch.setattr(
        track_store_execution_module,
        'normalize_cover_to_jpg',
        Mock(side_effect=[b'jpg-1', b'jpg-2']),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(side_effect=[b'opus-1', b'opus-2']))

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    year = date.today().year - 1
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='A'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            bot,
        )

    first_call, second_call = track_store.store.await_args_list
    assert first_call.kwargs['track'].artists == ('Artist 1',)
    assert first_call.kwargs['track'].title == 'Title 1'
    assert second_call.kwargs['track'].artists == ('Artist 2',)
    assert second_call.kwargs['track'].title == 'Title 2'


@pytest.mark.asyncio
async def test_track_store_caption_failure_invalidates_menu_and_skips_store() -> None:
    settings = _settings()
    track_store = SimpleNamespace(store=AsyncMock())
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Title only'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=60)
    state = _FakeState()
    bot = SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock())

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    year = date.today().year - 1
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='A'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            bot,
        )

    track_store.store.assert_not_awaited()
    menu_message.edit_text.assert_awaited_with("Can't dispatch input", reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []
    assert state.current_state is None


@pytest.mark.asyncio
async def test_track_store_validation_failure_does_not_download_files() -> None:
    settings = _settings()
    track_store = SimpleNamespace(store=AsyncMock())
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist 1\nTitle 1'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        _fake_message(chat_id=42, message_id=3, photo=_fake_photo(file_id='photo-2'), caption='Broken title'),
        _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-2', file_name='two.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=65)
    state = _FakeState()
    bot = SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock())

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    year = date.today().year - 1
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='A'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            bot,
        )

    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()
    track_store.store.assert_not_awaited()


@pytest.mark.asyncio
async def test_track_store_mid_batch_preprocessing_failure_prevents_partial_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    track_store = SimpleNamespace(store=AsyncMock())
    buffer = ChatMessageBuffer()
    buffered_messages = [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist 1\nTitle 1'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        _fake_message(chat_id=42, message_id=3, photo=_fake_photo(file_id='photo-2'), caption='Broken title'),
        _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-2', file_name='two.mp3')),
    ]
    for buffered_message in buffered_messages:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=70)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
            ]
        ),
        download_file=AsyncMock(side_effect=[BytesIO(b'cover-1'), BytesIO(b'audio-1')]),
    )
    monkeypatch.setattr(track_store_execution_module, 'normalize_cover_to_jpg', Mock(return_value=b'jpg-1'))
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(return_value=b'opus-1'))

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    year = date.today().year - 1
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='A'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            bot,
        )

    track_store.store.assert_not_awaited()
    menu_message.edit_text.assert_awaited_with("Can't dispatch input", reply_markup=None)
    assert services.chat_message_buffer.peek_raw(42) == []


@pytest.mark.asyncio
async def test_track_store_partial_failure_reports_stored_titles_and_flushes_buffer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()

    async def failing_store(*args, **kwargs) -> None:
        failing_store.calls.append((args, kwargs))
        if len(failing_store.calls) == 1:
            return None
        raise RuntimeError('boom')

    failing_store.calls = []
    track_store = SimpleNamespace(store=failing_store)
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist 1\nTitle 1'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
        _fake_message(chat_id=42, message_id=3, photo=_fake_photo(file_id='photo-2'), caption='Artist 2\nTitle 2'),
        _fake_message(chat_id=42, message_id=4, audio=_fake_audio(file_id='audio-2', file_name='two.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), track_store=track_store, buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=75)
    state = _FakeState()
    bot = SimpleNamespace(
        get_file=AsyncMock(
            side_effect=[
                SimpleNamespace(file_path='photo-path-1'),
                SimpleNamespace(file_path='audio-path-1'),
                SimpleNamespace(file_path='photo-path-2'),
                SimpleNamespace(file_path='audio-path-2'),
            ]
        ),
        download_file=AsyncMock(
            side_effect=[
                BytesIO(b'cover-1'),
                BytesIO(b'audio-1'),
                BytesIO(b'cover-2'),
                BytesIO(b'audio-2'),
            ]
        ),
    )
    monkeypatch.setattr(
        track_store_execution_module,
        'normalize_cover_to_jpg',
        Mock(side_effect=[b'jpg-1', b'jpg-2']),
    )
    monkeypatch.setattr(track_store_execution_module, 'to_opus', AsyncMock(side_effect=[b'opus-1', b'opus-2']))
    log_exception = Mock()
    monkeypatch.setattr(track_ingest_module.logger, 'exception', log_exception)

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    year = date.today().year - 1
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            bot,
        )

    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='A'),
        state,
        services,
        settings,
        bot,
    )

    assert len(failing_store.calls) == 2
    _assert_format_kwargs(
        menu_message.edit_text.await_args_list[-1].kwargs,
        _selected_kwargs('Store', 'West', str(year), '1', 'A'),
    )
    assert menu_message.edit_text.await_args_list[-1].kwargs['reply_markup'] is None
    menu_message.answer.assert_awaited_once_with(text='Storing failed\nStored titles:\nTitle 1')
    assert services.chat_message_buffer.peek_raw(42) == []
    assert state.current_state is None
    assert state.clear_count == 1
    log_exception.assert_called_once()


@pytest.mark.asyncio
async def test_track_store_selection_becomes_stale_when_buffer_changes() -> None:
    settings = _settings()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=80)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )
    assert state.current_state == TrackStoreFlow.universe.state

    buffer.append(_fake_message(chat_id=42, message_id=3, text='newer'), chat_id=42)

    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    menu_message.edit_text.assert_awaited_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(42)] == [1, 2, 3]


@pytest.mark.asyncio
async def test_track_store_invalid_universe_callback_value_becomes_stale() -> None:
    settings = _settings()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=81)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )

    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='bad'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    menu_message.edit_text.assert_awaited_with('Selection is no longer available', reply_markup=None)


@pytest.mark.asyncio
async def test_track_store_year_menu_shows_most_recent_years_first(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> date:
            return cls(2026, 1, 1)

    monkeypatch.setattr(track_ingest_module, 'date', _FixedDate)
    settings = _settings(min_clip_year=2022)
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=81)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )
    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    reply_markup = menu_message.edit_text.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['2023', '2026'], ['2022', '2024', '2025'], ['Back']]


@pytest.mark.asyncio
async def test_track_store_invalid_year_callback_value_becomes_stale() -> None:
    settings = _settings()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=82)
    state = _FakeState()

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )
    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value='bad'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    menu_message.edit_text.assert_awaited_with('Selection is no longer available', reply_markup=None)


@pytest.mark.asyncio
async def test_track_store_invalid_season_callback_value_becomes_stale() -> None:
    settings = _settings()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=83)
    state = _FakeState()
    year = date.today().year - 1

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
        )

    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='bad'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    menu_message.edit_text.assert_awaited_with('Selection is no longer available', reply_markup=None)


@pytest.mark.asyncio
async def test_track_store_invalid_sub_season_callback_value_becomes_stale() -> None:
    settings = _settings()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=42, message_id=1, photo=_fake_photo(file_id='photo-1'), caption='Artist\nTitle'),
        _fake_message(chat_id=42, message_id=2, audio=_fake_audio(file_id='audio-1', file_name='one.mp3')),
    ]:
        buffer.append(buffered_message, chat_id=42)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    menu_message = _fake_message(text='Select action:', chat_id=42, message_id=84)
    state = _FakeState()
    year = date.today().year - 1

    await on_track_intake_action(
        _fake_callback(menu_message),
        TrackIntakeActionCallbackData(
            action=TrackIntakeAction.STORE,
            buffer_version=buffer.version(42),
        ),
        state,
        services,
        settings,
    )
    for callback_data in [
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.UNIVERSE, value='west'),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.YEAR, value=str(year)),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SEASON, value='1'),
    ]:
        await on_track_store_menu(
            _fake_callback(menu_message),
            callback_data,
            state,
            services,
            settings,
            SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
        )

    await on_track_store_menu(
        _fake_callback(menu_message),
        TrackStoreCallbackData(action=TrackStoreAction.SELECT, step=TrackStoreStep.SUB_SEASON, value='bad'),
        state,
        services,
        settings,
        SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock()),
    )

    menu_message.edit_text.assert_awaited_with('Selection is no longer available', reply_markup=None)


def test_intake_action_keyboard_uses_right_to_left_columns() -> None:
    reply_markup = _column_right_to_left_two_row_keyboard(
        buttons=[InlineKeyboardButton(text=str(index), callback_data=str(index)) for index in range(1, 6)],
        back_button=InlineKeyboardButton(text='Cancel', callback_data='cancel'),
    )

    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        ['5', '3', '1'],
        ['4', '2'],
        ['Cancel'],
    ]


@pytest.mark.asyncio
async def test_reorder_action_opens_selection_menu_without_flushing_buffer() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=79)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        _fake_message(chat_id=77, message_id=2, text='ignore me'),
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f2', file_name='two.mp4')),
        _fake_message(chat_id=77, message_id=4, video=_fake_video(file_id='f3', file_name='three.mp4')),
        _fake_message(chat_id=77, message_id=5, video=_fake_video(file_id='f4', file_name='four.mp4')),
        _fake_message(chat_id=77, message_id=6, video=_fake_video(file_id='f5', file_name='five.mp4')),
    ]:
        buffer.append(buffered_message, chat_id=77)
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    settings = _settings()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.REORDER),
        AsyncMock(),
        services,
        settings,
        state,
    )

    callback.answer.assert_awaited_once()
    buffer.flush.assert_not_called()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _reorder_selected_kwargs(prompt='Select new order:', message_width=settings.message_width),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['5', '3', '1'], [DUMMY_BUTTON_TEXT, '4', '2'], ['Back']]
    assert state.current_state == ReorderClipFlow.selecting.state
    assert state.data['buffer_version'] == services.chat_message_buffer.version(77)
    assert state.data['selected_order'] == []
    assert state.data['total_clips'] == 5
    assert [message.message_id for message in services.chat_message_buffer.peek_raw(77)] == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_reorder_action_rejects_single_clip_and_flushes_buffer() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=80)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.REORDER),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    buffer.flush.assert_called_once_with(77)
    message.edit_text.assert_awaited_once_with('Unexpected number of clips', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_reorder_action_rejects_too_many_clips_and_flushes_buffer() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=81)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 18):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.REORDER),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    buffer.flush.assert_called_once_with(77)
    message.edit_text.assert_awaited_once_with('Too many clips', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_reorder_selection_duplicate_click_keeps_state_and_ui() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=82)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReorderClipFlow.selecting)
    await state.update_data(
        mode='reorder',
        menu_message_id=82,
        buffer_version=0,
        selected_order=[3],
        total_clips=5,
    )
    buffer = ChatMessageBuffer()
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.SELECT, value='3'),
        _RecordingBot(),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_not_awaited()
    assert state.data['selected_order'] == [3]


@pytest.mark.asyncio
async def test_reorder_selection_marks_clicked_button_as_primary() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=86)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 6):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    await state.set_state(ReorderClipFlow.selecting)
    await state.update_data(
        mode='reorder',
        menu_message_id=86,
        buffer_version=buffer.version(77),
        selected_order=[],
        total_clips=5,
    )
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.SELECT, value='3'),
        _RecordingBot(),
        services,
        _settings(),
        state,
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    button_by_text = {button.text: button for row in reply_markup.inline_keyboard for button in row}
    assert _keyboard_rows(reply_markup) == [['5', '3', '1'], [DUMMY_BUTTON_TEXT, '4', '2'], ['Reset']]
    assert button_by_text['3'].style == 'primary'
    assert button_by_text['5'].style is None


@pytest.mark.asyncio
async def test_reorder_reset_clears_selection_and_restores_back_button() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=87)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 6):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    await state.set_state(ReorderClipFlow.selecting)
    await state.update_data(
        mode='reorder',
        menu_message_id=87,
        buffer_version=buffer.version(77),
        selected_order=[3, 5],
        total_clips=5,
    )
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    settings = _settings()

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.BACK, value='reset'),
        _RecordingBot(),
        services,
        settings,
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _reorder_selected_kwargs(prompt='Select new order:', message_width=settings.message_width),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['5', '3', '1'], [DUMMY_BUTTON_TEXT, '4', '2'], ['Back']]
    assert all(button.style is None for row in reply_markup.inline_keyboard for button in row if button.text != 'Back')
    assert state.current_state == ReorderClipFlow.selecting.state
    assert state.data['selected_order'] == []
    assert state.data['buffer_version'] == buffer.version(77)
    assert state.data['total_clips'] == 5
    assert [message.message_id for message in services.chat_message_buffer.peek_raw(77)] == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_reorder_odd_layout_places_buttons_by_descending_columns() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=89)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 8):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.REORDER),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    assert _keyboard_rows(reply_markup) == [['7', '5', '3', '1'], [DUMMY_BUTTON_TEXT, '6', '4', '2'], ['Back']]


@pytest.mark.asyncio
async def test_reorder_back_from_empty_state_returns_to_intake_action_menu() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=88)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 6):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    await state.set_state(ReorderClipFlow.selecting)
    await state.update_data(
        mode='reorder',
        menu_message_id=88,
        buffer_version=buffer.version(77),
        selected_order=[],
        total_clips=5,
    )
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.BACK, value='back'),
        _RecordingBot(),
        services,
        _settings(),
        state,
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        ['Route', 'Store', 'Reorder'],
        ['Reconcile', 'Produce', 'Compact'],
        ['Cancel'],
    ]
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        Text(
            create_padding_line(35),
            '\n',
            Text('Messages: ', Bold('5')),
        ).as_kwargs(),
    )
    assert state.current_state is None


@pytest.mark.asyncio
async def test_reorder_back_with_empty_buffer_flushes_and_shows_no_clips_received() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=90)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.set_state(ReorderClipFlow.selecting)
    await state.update_data(
        mode='reorder',
        menu_message_id=90,
        buffer_version=buffer.version(77),
        selected_order=[],
        total_clips=1,
    )
    buffer.flush(77)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.BACK, value='back'),
        _RecordingBot(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Invalid input', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(77) == []
    assert state.current_state is None
    assert state.data == {}


@pytest.mark.asyncio
async def test_reorder_selection_becomes_stale_when_buffer_changes() -> None:
    message = _fake_message(text='Select new order:', chat_id=77, message_id=83)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name='two.mp4')),
        chat_id=77,
    )
    await state.set_state(ReorderClipFlow.selecting)
    await state.update_data(
        mode='reorder',
        menu_message_id=83,
        buffer_version=buffer.version(77),
        selected_order=[],
        total_clips=2,
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f3', file_name='three.mp4')),
        chat_id=77,
    )
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.SELECT, value='1'),
        _RecordingBot(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [message.message_id for message in services.chat_message_buffer.peek_raw(77)] == [1, 2, 3]
    assert state.current_state is None


@pytest.mark.asyncio
async def test_reorder_flow_resends_videos_by_file_id_in_selected_order() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=84)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        _fake_message(chat_id=77, message_id=2, text='ignore me'),
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f2', file_name='two.mp4')),
        _fake_message(chat_id=77, message_id=4, video=_fake_video(file_id='f3', file_name='three.mp4')),
        _fake_message(chat_id=77, message_id=5, video=_fake_video(file_id='f4', file_name='four.mp4')),
        _fake_message(chat_id=77, message_id=6, video=_fake_video(file_id='f5', file_name='five.mp4')),
    ]:
        buffer.append(buffered_message, chat_id=77)
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    bot = _RecordingBot()
    settings = _settings()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.REORDER),
        bot,
        services,
        settings,
        state,
    )

    first_edit = message.edit_text.await_args_list[-1].kwargs
    _assert_format_kwargs(
        first_edit,
        _reorder_selected_kwargs(prompt='Select new order:', message_width=settings.message_width),
    )
    buffer.flush.assert_not_called()

    for value in ['3', '5', '4', '1', '2']:
        await on_reorder_menu(
            callback,
            ReorderCallbackData(action=MenuAction.SELECT, value=value),
            bot,
            services,
            settings,
            state,
        )

    assert state.current_state is None
    assert state.data == {}
    buffer.flush.assert_called_once_with(77)
    assert message.edit_text.await_args_list[-1].kwargs == {
        **_reorder_selected_kwargs(3, 5, 4, 1, 2),
        'reply_markup': None,
    }
    assert bot.events == [('media_group', (77, ['f3', 'f5', 'f4', 'f1', 'f2']))]


@pytest.mark.asyncio
async def test_reorder_completion_flushes_only_at_completion_and_splits_large_resend() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=85)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 13):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    bot = _RecordingBot()
    settings = _settings()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.REORDER),
        bot,
        services,
        settings,
        state,
    )

    for value in ['12', '11', '10', '9', '8', '7', '6', '5', '4', '3', '2']:
        await on_reorder_menu(
            callback,
            ReorderCallbackData(action=MenuAction.SELECT, value=value),
            bot,
            services,
            settings,
            state,
        )
        assert buffer.flush.call_count == 0

    await on_reorder_menu(
        callback,
        ReorderCallbackData(action=MenuAction.SELECT, value='1'),
        bot,
        services,
        settings,
        state,
    )

    buffer.flush.assert_called_once_with(77)
    assert bot.events == [
        ('media_group', (77, ['f12', 'f11', 'f10', 'f9', 'f8', 'f7', 'f6', 'f5', 'f4', 'f3'])),
        ('media_group', (77, ['f2', 'f1'])),
    ]


@pytest.mark.asyncio
async def test_compact_action_resends_only_videos_by_file_id_in_original_order() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=91)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        _fake_message(chat_id=77, message_id=2, text='ignore me'),
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f2', file_name='two.mp4')),
        _fake_message(chat_id=77, message_id=4, caption='caption only'),
        _fake_message(chat_id=77, message_id=5, video=_fake_video(file_id='f3', file_name='three.mp4')),
        _fake_message(chat_id=77, message_id=6, text='still ignored'),
        _fake_message(chat_id=77, message_id=7, video=_fake_video(file_id='f4', file_name='four.mp4')),
    ]:
        buffer.append(buffered_message, chat_id=77)
    buffer.flush = Mock(wraps=buffer.flush)
    clip_store = SimpleNamespace(
        compact=AsyncMock(side_effect=AssertionError('Compact must not use ClipStore.compact')),
        list_groups=AsyncMock(side_effect=AssertionError('Compact must not use ClipStore.list_groups')),
        list_clips=AsyncMock(side_effect=AssertionError('Compact must not use ClipStore.list_clips')),
        reconcile=AsyncMock(side_effect=AssertionError('Compact must not use ClipStore.reconcile')),
        store=AsyncMock(side_effect=AssertionError('Compact must not use ClipStore.store')),
    )
    services = _services(clip_store=clip_store, buffer=buffer)
    bot = _RecordingBot()
    bot.get_file = AsyncMock(side_effect=AssertionError('Compact must not download files'))
    bot.download_file = AsyncMock(side_effect=AssertionError('Compact must not download files'))

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.COMPACT),
        bot,
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    message.edit_text.assert_awaited_once_with(
        **selected_text(selected='Compact'),
        reply_markup=None,
    )
    buffer.flush.assert_called_once_with(77)
    assert services.chat_message_buffer.peek_raw(77) == []
    assert state.current_state is None
    assert state.data == {}
    assert bot.events == [('media_group', (77, ['f1', 'f2', 'f3', 'f4']))]
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    clip_store.reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_compact_action_rejects_single_clip_and_flushes_buffer() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=92)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.append(_fake_message(chat_id=77, message_id=2, text='ignore me'), chat_id=77)
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    bot = _RecordingBot()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.COMPACT),
        bot,
        services,
        _settings(),
        state,
    )

    buffer.flush.assert_called_once_with(77)
    message.edit_text.assert_awaited_once_with('Unexpected number of clips', reply_markup=None)
    assert services.chat_message_buffer.peek_raw(77) == []
    assert bot.events == []


@pytest.mark.asyncio
async def test_compact_action_treats_zero_videos_as_stale_without_flushing() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=93)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='ignore me'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, caption='still not a video'), chat_id=77)
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    bot = _RecordingBot()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.COMPACT),
        bot,
        services,
        _settings(),
        state,
    )

    buffer.flush.assert_not_called()
    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(77)] == [1, 2]
    assert bot.events == []


@pytest.mark.asyncio
async def test_compact_action_allows_large_clip_counts_and_splits_batches_of_ten() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=94)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 18):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    buffer.flush = Mock(wraps=buffer.flush)
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    bot = _RecordingBot()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.COMPACT),
        bot,
        services,
        _settings(),
        state,
    )

    buffer.flush.assert_called_once_with(77)
    assert bot.events == [
        ('media_group', (77, ['f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10'])),
        ('media_group', (77, ['f11', 'f12', 'f13', 'f14', 'f15', 'f16', 'f17'])),
    ]


@pytest.mark.asyncio
async def test_compact_action_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=95)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for index in range(1, 3):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index,
                video=_fake_video(file_id=f'f{index}', file_name=f'{index}.mp4'),
            ),
            chat_id=77,
        )
    initial_version = buffer.version(77)
    buffer.flush = Mock(wraps=buffer.flush)
    buffer.version = Mock(side_effect=[initial_version, initial_version + 1])
    services = _services(clip_store=SimpleNamespace(), buffer=buffer)
    bot = _RecordingBot()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.COMPACT),
        bot,
        services,
        _settings(),
        state,
    )

    buffer.flush.assert_not_called()
    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(77)] == [1, 2]
    assert bot.events == []


@pytest.mark.asyncio
async def test_route_action_stores_clips_in_caption_route_order_across_message_groups() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=70)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='w251',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    buffer.append(_fake_message(chat_id=77, message_id=2, text='ignore me'), chat_id=77)
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
        ),
        chat_id=77,
    )
    buffer.append(_fake_message(chat_id=77, message_id=4, text='still ignored', media_group_id='g2'), chat_id=77)
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=5,
            video=_fake_video(file_id='f3', file_name='three.mp4'),
            media_group_id='g2',
        ),
        chat_id=77,
    )
    buffer.flush = Mock(wraps=buffer.flush)

    clip_store = SimpleNamespace(
        store=AsyncMock(return_value=StoreResult(stored_count=3, duplicate_count=0)),
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

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    buffer.flush.assert_called_once_with(77)
    assert services.chat_message_buffer.peek_raw(77) == []
    clip_store.store.assert_awaited_once()
    stored_clips = clip_store.store.await_args.kwargs['clips']
    assert stored_clips == [_mp4_file(b'one'), _mp4_file(b'two'), _mp4_file(b'three')]
    assert clip_store.store.await_args.args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S1,
    )
    assert clip_store.store.await_args.args[1] == ClipSubGroup(
        sub_season=SubSeason.NONE,
        scope=Scope.SOURCE,
    )
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('West', '2025', '1', 'Source'),
    )
    message.answer.assert_awaited_once_with(**Text('Stored: ', Bold('3')).as_kwargs())
    clip_store.compact.assert_awaited_once_with(
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
        batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
    )
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_route_action_splits_store_calls_when_caption_route_overrides() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=71)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='w251',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
            media_group_id='g1',
        ),
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
            media_group_id='g1',
        ),
        _fake_message(
            chat_id=77,
            message_id=3,
            caption='E242',
            video=_fake_video(file_id='f3', file_name='three.mp4'),
            media_group_id='g1',
        ),
        _fake_message(
            chat_id=77,
            message_id=4,
            video=_fake_video(file_id='f4', file_name='four.mp4'),
            media_group_id='g1',
        ),
    ]:
        buffer.append(buffered_message, chat_id=77)

    clip_store = SimpleNamespace(
        store=AsyncMock(
            side_effect=[
                StoreResult(stored_count=2, duplicate_count=1),
                StoreResult(stored_count=2, duplicate_count=0),
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
        SimpleNamespace(file_path='path-4'),
    ]
    bot.download_file.side_effect = [BytesIO(b'one'), BytesIO(b'two'), BytesIO(b'three'), BytesIO(b'four')]

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    assert clip_store.store.await_count == 2
    first_batch = clip_store.store.await_args_list[0].kwargs['clips']
    second_batch = clip_store.store.await_args_list[1].kwargs['clips']
    assert first_batch == [_mp4_file(b'one'), _mp4_file(b'two')]
    assert second_batch == [_mp4_file(b'three'), _mp4_file(b'four')]
    assert clip_store.store.await_args_list[0].args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S1,
    )
    assert clip_store.store.await_args_list[1].args[0] == ClipGroup(
        universe=Universe.EAST,
        year=2024,
        season=Season.S2,
    )
    assert all(
        call.args[1] == ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE)
        for call in clip_store.store.await_args_list
    )
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('West', '2025', '1', 'Source'),
    )
    _assert_route_progress_edit(
        message.edit_text.await_args_list[2],
        ('West', '2025', '1', 'Source'),
        ('East', '2024', '2', 'Source'),
    )
    message.answer.assert_awaited_once_with(
        **Text(
            'Stored: ',
            Bold('4'),
            '\n',
            'Deduplicated: ',
            Bold('1'),
        ).as_kwargs()
    )
    assert clip_store.compact.await_args_list == [
        call(
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
        call(
            ClipGroup(universe=Universe.EAST, year=2024, season=Season.S2),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
    ]


@pytest.mark.asyncio
async def test_route_action_chunks_long_single_route_group_without_duplicate_progress_updates() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=71)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()

    for index in range(10):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index + 1,
                caption='w251' if index == 0 else None,
                video=_fake_video(file_id=f'f{index + 1}', file_name=f'clip-{index + 1}.mp4'),
                media_group_id='g1',
            ),
            chat_id=77,
        )

    clip_store = SimpleNamespace(
        store=AsyncMock(
            side_effect=[
                StoreResult(stored_count=8, duplicate_count=0),
                StoreResult(stored_count=2, duplicate_count=0),
            ]
        ),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.side_effect = [SimpleNamespace(file_path=f'path-{index + 1}') for index in range(10)]
    bot.download_file.side_effect = [BytesIO(f'clip-{index + 1}'.encode()) for index in range(10)]

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    assert clip_store.store.await_count == 2
    assert clip_store.store.await_args_list[0].kwargs['clips'] == [
        _mp4_file(f'clip-{index}'.encode()) for index in range(1, 9)
    ]
    assert clip_store.store.await_args_list[1].kwargs['clips'] == [_mp4_file(b'clip-9'), _mp4_file(b'clip-10')]
    assert all(
        call.args[0] == ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
        for call in clip_store.store.await_args_list
    )
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('West', '2025', '1', 'Source'),
    )
    assert len(message.edit_text.await_args_list) == 2
    message.answer.assert_awaited_once_with(**Text('Stored: ', Bold('10')).as_kwargs())
    clip_store.compact.assert_awaited_once_with(
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
        batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
    )


@pytest.mark.asyncio
async def test_route_action_chunks_per_route_group_without_crossing_route_boundaries() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=71)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()

    for index in range(9):
        buffer.append(
            _fake_message(
                chat_id=77,
                message_id=index + 1,
                caption='w251' if index == 0 else None,
                video=_fake_video(file_id=f'w{index + 1}', file_name=f'west-{index + 1}.mp4'),
            ),
            chat_id=77,
        )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=10,
            caption='e242',
            video=_fake_video(file_id='e1', file_name='east-1.mp4'),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(
        store=AsyncMock(
            side_effect=[
                StoreResult(stored_count=8, duplicate_count=0),
                StoreResult(stored_count=1, duplicate_count=0),
                StoreResult(stored_count=1, duplicate_count=0),
            ]
        ),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.side_effect = [SimpleNamespace(file_path=f'path-{index + 1}') for index in range(10)]
    bot.download_file.side_effect = [BytesIO(f'clip-{index + 1}'.encode()) for index in range(10)]

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    assert clip_store.store.await_count == 3
    assert clip_store.store.await_args_list[0].kwargs['clips'] == [
        _mp4_file(f'clip-{index}'.encode()) for index in range(1, 9)
    ]
    assert clip_store.store.await_args_list[1].kwargs['clips'] == [_mp4_file(b'clip-9')]
    assert clip_store.store.await_args_list[2].kwargs['clips'] == [_mp4_file(b'clip-10')]
    assert clip_store.store.await_args_list[0].args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S1,
    )
    assert clip_store.store.await_args_list[1].args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S1,
    )
    assert clip_store.store.await_args_list[2].args[0] == ClipGroup(
        universe=Universe.EAST,
        year=2024,
        season=Season.S2,
    )
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('West', '2025', '1', 'Source'),
    )
    _assert_route_progress_edit(
        message.edit_text.await_args_list[2],
        ('West', '2025', '1', 'Source'),
        ('East', '2024', '2', 'Source'),
    )
    assert len(message.edit_text.await_args_list) == 3
    assert clip_store.compact.await_args_list == [
        call(
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
        call(
            ClipGroup(universe=Universe.EAST, year=2024, season=Season.S2),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
    ]


@pytest.mark.asyncio
async def test_route_action_updates_active_route_from_standalone_text_and_clip_captions() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=72)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    for buffered_message in [
        _fake_message(chat_id=77, message_id=1, text='random text'),
        _fake_message(chat_id=77, message_id=2, text='w251'),
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f1', file_name='one.mp4')),
        _fake_message(chat_id=77, message_id=4, text='still not a route'),
        _fake_message(chat_id=77, message_id=5, text='e242'),
        _fake_message(chat_id=77, message_id=6, video=_fake_video(file_id='f2', file_name='two.mp4')),
        _fake_message(
            chat_id=77,
            message_id=7,
            caption='w252',
            video=_fake_video(file_id='f3', file_name='three.mp4'),
        ),
        _fake_message(chat_id=77, message_id=8, video=_fake_video(file_id='f4', file_name='four.mp4')),
    ]:
        buffer.append(buffered_message, chat_id=77)

    clip_store = SimpleNamespace(
        store=AsyncMock(
            side_effect=[
                StoreResult(stored_count=1, duplicate_count=0),
                StoreResult(stored_count=1, duplicate_count=0),
                StoreResult(stored_count=2, duplicate_count=0),
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
        SimpleNamespace(file_path='path-4'),
    ]
    bot.download_file.side_effect = [BytesIO(b'one'), BytesIO(b'two'), BytesIO(b'three'), BytesIO(b'four')]

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    assert clip_store.store.await_count == 3
    assert clip_store.store.await_args_list[0].kwargs['clips'] == [_mp4_file(b'one')]
    assert clip_store.store.await_args_list[1].kwargs['clips'] == [_mp4_file(b'two')]
    assert clip_store.store.await_args_list[2].kwargs['clips'] == [_mp4_file(b'three'), _mp4_file(b'four')]
    assert clip_store.store.await_args_list[0].args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S1,
    )
    assert clip_store.store.await_args_list[1].args[0] == ClipGroup(
        universe=Universe.EAST,
        year=2024,
        season=Season.S2,
    )
    assert clip_store.store.await_args_list[2].args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S2,
    )
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('West', '2025', '1', 'Source'),
    )
    _assert_route_progress_edit(
        message.edit_text.await_args_list[2],
        ('West', '2025', '1', 'Source'),
        ('East', '2024', '2', 'Source'),
    )
    _assert_route_progress_edit(
        message.edit_text.await_args_list[3],
        ('West', '2025', '1', 'Source'),
        ('East', '2024', '2', 'Source'),
        ('West', '2025', '2', 'Source'),
    )
    message.answer.assert_awaited_once_with(**Text('Stored: ', Bold('4')).as_kwargs())
    assert clip_store.compact.await_args_list == [
        call(
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
        call(
            ClipGroup(universe=Universe.EAST, year=2024, season=Season.S2),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
        call(
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S2),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
            batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
        ),
    ]
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_route_action_edits_missing_route_when_first_video_has_no_caption() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=73)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Missing route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    # Intentional UX: Route flushes once at entry and does not restore buffered clips on failure.
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_route_action_edits_invalid_route_when_first_video_caption_is_invalid() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=74)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='bad',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Invalid route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_route_action_uses_latest_valid_pre_clip_text_before_first_video() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=74)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='random text'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, text='w251'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f1', file_name='one.mp4')),
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

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    assert clip_store.store.await_args.kwargs['clips'] == [_mp4_file(b'one')]
    assert clip_store.store.await_args.args[0] == ClipGroup(
        universe=Universe.WEST,
        year=2025,
        season=Season.S1,
    )
    message.answer.assert_awaited_once_with(**Text('Stored: ', Bold('1')).as_kwargs())
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('West', '2025', '1', 'Source'),
    )
    clip_store.compact.assert_awaited_once_with(
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
        batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
    )
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_route_action_ignores_invalid_pre_clip_text_until_valid_route_text() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=75)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='random text'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, text='still not a route'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=3, text='e242'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=4, video=_fake_video(file_id='f1', file_name='one.mp4')),
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

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    assert clip_store.store.await_args.args[0] == ClipGroup(
        universe=Universe.EAST,
        year=2024,
        season=Season.S2,
    )
    assert message.edit_text.await_args_list[0] == call('Routing...', reply_markup=None)
    _assert_route_progress_edit(
        message.edit_text.await_args_list[1],
        ('East', '2024', '2', 'Source'),
    )
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_route_action_edits_missing_route_when_pre_clip_text_is_not_a_route() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=76)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='random text'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, text='still not a route'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Missing route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    assert services.chat_message_buffer.peek_raw(77) == []


@pytest.mark.asyncio
async def test_route_action_fails_before_execution_when_later_video_caption_is_invalid() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=75)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='w251',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            caption='oops',
            video=_fake_video(file_id='f3', file_name='three.mp4'),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(
        store=AsyncMock(return_value=StoreResult(stored_count=2, duplicate_count=0)),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    clip_store.store.assert_not_awaited()
    message.edit_text.assert_awaited_once_with('Invalid route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_action_rejects_future_year_before_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 6, 15)

    monkeypatch.setattr(route_planning_module, 'date', _FixedDate)

    message = _fake_message(text='Select action:', chat_id=77, message_id=76)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='w271',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)
    bot = AsyncMock()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Invalid route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_action_ignores_semantically_invalid_pre_clip_route_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 6, 15)

    monkeypatch.setattr(route_planning_module, 'date', _FixedDate)

    message = _fake_message(text='Select action:', chat_id=77, message_id=77)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='w271'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)
    bot = AsyncMock()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Missing route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_action_rejects_year_below_min_clip_year_before_execution() -> None:
    message = _fake_message(text='Select action:', chat_id=77, message_id=77)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='w211',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)
    bot = AsyncMock()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Invalid route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_action_rejects_season_not_allowed_for_current_year_before_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 6, 15)

    monkeypatch.setattr(route_planning_module, 'date', _FixedDate)

    message = _fake_message(text='Select action:', chat_id=77, message_id=78)
    callback = _fake_callback(message)
    state = _FakeState()
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            caption='w264',
            video=_fake_video(file_id='f1', file_name='one.mp4'),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)
    bot = AsyncMock()

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.ROUTE),
        bot,
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Invalid route text', reply_markup=None)
    message.answer.assert_not_awaited()
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


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
        _selected_kwargs('Store', prompt='Select universe:', message_width=settings.message_width),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    _assert_no_dummy_buttons(reply_markup)
    assert _keyboard_rows(reply_markup) == [['West'], ['East'], ['Back']]
    services.clip_store.list_groups.assert_not_awaited()
    services.clip_store.list_clips.assert_not_awaited()
    assert state.data['buffer_version'] == 0


@pytest.mark.asyncio
async def test_reconcile_entry_accepts_same_group_clips_from_different_sub_groups() -> None:
    message = _fake_message(text='Got 1 clip', message_id=31)
    callback = _fake_callback(message)
    state = _FakeState()
    bot = AsyncMock()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    first_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    second_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.EXTRA)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=1,
            message_id=1,
            video=_fake_video(file_id='f1', file_name=_stored_filename(clip_group, first_sub_group, _CLIP_ID_1)),
        ),
        chat_id=1,
    )
    buffer.append(
        _fake_message(
            chat_id=1,
            message_id=2,
            video=_fake_video(file_id='f2', file_name=_stored_filename(clip_group, second_sub_group, _CLIP_ID_2)),
            media_group_id='g1',
        ),
        chat_id=1,
    )
    buffer.append(
        _fake_message(
            chat_id=1,
            message_id=3,
            video=_fake_video(file_id='f3', file_name=_stored_filename(clip_group, second_sub_group, _CLIP_ID_3)),
            media_group_id='g1',
        ),
        chat_id=1,
    )
    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)
    settings = _settings()
    pre_flush_buffer_version = services.chat_message_buffer.version(1)

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
        _selected_kwargs('Reconcile', 'West', '2025', '1', prompt='Select sub-season:', message_width=35),
    )
    assert state.current_state == ReconcileClipFlow.sub_season.state
    assert state.data['clip_group'] == clip_group
    assert state.data['clip_id_batches'] == [[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]]
    assert state.data['buffer_version'] == pre_flush_buffer_version
    assert [message.message_id for message in services.chat_message_buffer.peek_raw(1)] == [1, 2, 3]


@pytest.mark.asyncio
async def test_reconcile_entry_prefers_current_buffered_clips_over_stored_reconcile_state() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=35)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.update_data(
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_id_batches=[[_CLIP_ID_1]],
        buffer_version=0,
    )
    bot = AsyncMock()
    clip_group = ClipGroup(universe=Universe.EAST, year=2025, season=Season.S2)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            video=_fake_video(file_id='f1', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_1)),
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f2', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_2)),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            video=_fake_video(file_id='f3', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_3)),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        bot,
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Reconcile', 'East', '2025', '2', prompt='Select sub-season:', message_width=35),
    )
    assert state.data['clip_group'] == clip_group
    assert state.data['clip_id_batches'] == [[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]]
    assert [message.message_id for message in services.chat_message_buffer.peek_raw(77)] == [1, 2, 3]


@pytest.mark.asyncio
async def test_reconcile_entry_reuses_stored_state_when_current_buffer_has_no_clip_id_batches() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=36)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.update_data(
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        clip_id_batches=[[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]],
        buffer_version=0,
    )
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='note'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, text='ignored', media_group_id='g1'), chat_id=77)
    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Reconcile', 'West', '2025', '1', prompt='Select sub-season:', message_width=35),
    )
    assert state.data['clip_id_batches'] == [[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]]


@pytest.mark.asyncio
async def test_reconcile_entry_ignores_non_video_buffered_messages() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=34)
    callback = _fake_callback(message)
    state = _FakeState()
    bot = AsyncMock()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='note'), chat_id=77)
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f1', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_1)),
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            video=_fake_video(file_id='f2', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_2)),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    buffer.append(_fake_message(chat_id=77, message_id=4, text='ignored', media_group_id='g1'), chat_id=77)
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=5,
            video=_fake_video(file_id='f3', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_3)),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        bot,
        services,
        _settings(),
        state,
    )

    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_sub_season_selection_becomes_stale_when_buffer_changes_after_entry() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=37)
    callback = _fake_callback(message)
    state = _FakeState()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            video=_fake_video(file_id='f1', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_1)),
        ),
        chat_id=77,
    )
    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f2', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_2)),
        ),
        chat_id=77,
    )
    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SUB_SEASON, value=SubSeason.NONE.value),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    assert message.edit_text.await_args_list[-1].args == ('Selection is no longer available',)
    assert message.edit_text.await_args_list[-1].kwargs == {
        'reply_markup': None,
    }


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        clip_id_batches=[[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]],
        buffer_version=0,
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
        _selected_kwargs('Reconcile', 'West', '2025', '1', prompt='Select sub-season:', message_width=35),
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        clip_id_batches=[[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]],
        buffer_version=0,
    )
    services = _services(
        clip_store=SimpleNamespace(),
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

    message.edit_text.assert_awaited_once_with('Invalid input', reply_markup=None)
    assert state.current_state is None
    assert state.clear_count == 1
    assert state.data == {}
    assert services.chat_message_buffer.peek_raw(77) == []
    message.answer.assert_not_awaited()


def test_store_year_options_returns_ascending_range() -> None:
    assert _store_year_options(current_year=2026, min_year=2022) == [2022, 2023, 2024, 2025, 2026]


@pytest.mark.asyncio
async def test_retrieve_back_from_season_keeps_year_slots_and_top_right_priority() -> None:
    message = _fake_message(message_id=11)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(RetrieveClipFlow.season)
    groups = [
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
    ]
    await state.update_data(mode='get', menu_message_id=11, universe=Universe.WEST, year=2025, groups=groups)
    services = _services(clip_store=SimpleNamespace(list_groups=AsyncMock(return_value=groups)))

    await on_retrieve_menu(
        callback,
        RetrieveCallbackData(action=MenuAction.BACK, step=MenuStep.SEASON, value='back'),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', prompt='Select year:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    services.clip_store.list_groups.assert_not_awaited()
    assert state.current_state == RetrieveClipFlow.year.state


@pytest.mark.asyncio
async def test_clip_retrieve_back_from_season_skips_single_year_to_universe_menu() -> None:
    message = _fake_message(message_id=11)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(RetrieveClipFlow.season)
    groups = [
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S2),
    ]
    await state.update_data(mode='get', menu_message_id=11, universe=Universe.WEST, year=2025, groups=groups)
    services = _services(clip_store=SimpleNamespace(list_groups=AsyncMock(return_value=groups)))

    await on_retrieve_menu(
        callback,
        RetrieveCallbackData(action=MenuAction.BACK, step=MenuStep.SEASON, value='back'),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    _assert_one_line_button_message(
        text=message.edit_text.await_args.kwargs['text'],
        message_width=35,
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        ['Get'],
        ['Pull'],
        ['Cancel'],
    ]
    services.clip_store.list_groups.assert_not_awaited()
    assert state.current_state is None


@pytest.mark.asyncio
async def test_on_retrieve_entry_opens_year_menu_with_get_selected() -> None:
    message = _fake_message(text='Select action:', message_id=111)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ]
            )
        )
    )

    await on_retrieve_entry(
        callback,
        RetrieveEntryCallbackData(action=RetrieveEntryAction.GET),
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', prompt='Select universe:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['West'], [DUMMY_BUTTON_TEXT], ['Back']]
    services.clip_store.list_groups.assert_awaited_once()
    assert state.current_state == RetrieveClipFlow.universe.state


@pytest.mark.asyncio
async def test_on_retrieve_entry_opens_year_menu_with_pull_selected() -> None:
    message = _fake_message(text='Select action:', message_id=112)
    callback = _fake_callback(message)
    state = _FakeState()
    services = _services(
        clip_store=SimpleNamespace(
            list_groups=AsyncMock(
                return_value=[
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ]
            )
        )
    )

    await on_retrieve_entry(
        callback,
        RetrieveEntryCallbackData(action=RetrieveEntryAction.PULL),
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Pull', prompt='Select universe:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['West'], [DUMMY_BUTTON_TEXT], ['Back']]
    services.clip_store.list_groups.assert_awaited_once()
    assert state.current_state == RetrieveClipFlow.universe.state


@pytest.mark.asyncio
async def test_retrieve_universe_selection_keeps_cached_groups_and_opens_year_menu() -> None:
    message = _fake_message(text='Select universe:', message_id=113)
    callback = _fake_callback(message)
    state = _FakeState()
    groups = [
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
    ]
    await state.set_state(RetrieveClipFlow.universe)
    await state.update_data(
        mode='get',
        menu_message_id=113,
        groups=groups,
    )

    await on_retrieve_menu(
        callback,
        RetrieveCallbackData(action=MenuAction.SELECT, step=MenuStep.UNIVERSE, value=Universe.WEST.value),
        AsyncMock(),
        _services(clip_store=SimpleNamespace()),
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', prompt='Select year:', message_width=35),
    )
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        [DUMMY_BUTTON_TEXT, '2024', '2025'],
        ['Back'],
    ]
    assert state.current_state == RetrieveClipFlow.year.state
    assert state.data['groups'] == groups


@pytest.mark.asyncio
async def test_missing_retrieve_state_is_treated_as_stale_selection() -> None:
    message = _fake_message(message_id=20)
    callback = _fake_callback(message)
    state = _FakeState()

    await on_retrieve_menu(
        callback,
        RetrieveCallbackData(action=MenuAction.SELECT, step=MenuStep.YEAR, value='2025'),
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
        universe=Universe.WEST,
        year=2026,
    )

    expected = _selected_kwargs('Store', 'West', '2026', prompt='Select season:', message_width=21)
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
    )

    expected = _selected_kwargs('Store', prompt='Select universe:', message_width=18)
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S2),
    )

    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    _assert_no_dummy_buttons(reply_markup)
    assert _keyboard_rows(reply_markup) == [['C', 'None'], ['D', 'B', 'A'], ['Back']]


@pytest.mark.asyncio
async def test_retrieve_scope_menu_uses_fixed_scope_grid_with_dummy_slots() -> None:
    message = _fake_message(message_id=73)
    state = _FakeState()
    services = _services(clip_store=SimpleNamespace())

    await _show_retrieve_scope_menu(
        message=message,
        state=state,
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
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
    await _show_retrieve_scope_menu(
        message=message_single,
        state=state_single,
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
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
async def test_pull_scope_all_with_one_scope_sends_single_scope_normally() -> None:
    message = _fake_message(chat_id=9, message_id=741)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(RetrieveClipFlow.scope)
    await state.update_data(
        mode=FLOW_PULL,
        menu_message_id=741,
        groups=[ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)],
        year=2024,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )
    bot = _RecordingBot()
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    clip_store = _RetrieveClipStore(
        {Scope.COLLECTION: [[FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'1'))]]},
        sub_groups=[clip_sub_group],
    )
    services = _services(clip_store=clip_store)

    await on_retrieve_menu(
        callback,
        RetrieveCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=ALL_SCOPES_CALLBACK_VALUE),
        bot,
        services,
        _settings(),
        state,
    )

    callback.answer.assert_awaited_once()
    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Pull', 'West', '2024', '1', 'All'),
    )
    assert clip_store.calls == [
        (
            clip_group,
            clip_sub_group,
            None,
            None,
        )
    ]
    assert bot.events == [
        ('video', (9, _stored_filename(clip_group, clip_sub_group, _CLIP_ID_1))),
    ]
    assert state.current_state is None


@pytest.mark.asyncio
async def test_get_scope_all_requests_normalized_fetch_before_sending() -> None:
    message = _fake_message(chat_id=9, message_id=742)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(RetrieveClipFlow.scope)
    await state.update_data(
        mode='get',
        menu_message_id=742,
        groups=[ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)],
        year=2024,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
    )
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    bot = AsyncMock()
    clip_store = _RetrieveClipStore(
        {Scope.COLLECTION: [[FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'one'))]]},
        sub_groups=[clip_sub_group],
    )
    services = _services(clip_store=clip_store)

    await on_retrieve_menu(
        callback,
        RetrieveCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=ALL_SCOPES_CALLBACK_VALUE),
        bot,
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Get', 'West', '2024', '1', 'All'),
    )
    assert clip_store.calls == [
        (
            clip_group,
            clip_sub_group,
            None,
            AudioNormalization(loudness=-14, bitrate=128),
        )
    ]
    assert bot.send_video.await_args.kwargs['video'].filename == _stored_filename(
        clip_group, clip_sub_group, _CLIP_ID_1
    )
    assert bot.send_video.await_args.kwargs['video'].data == b'normalized:one'
    bot.send_message.assert_not_awaited()
    assert state.current_state is None


@pytest.mark.asyncio
async def test_retrieve_sub_season_menu_skips_only_when_none_is_only_option() -> None:
    services = _services(clip_store=SimpleNamespace())

    message_none_only = _fake_message(message_id=75)
    state_none_only = _FakeState()
    await _show_retrieve_sub_season_menu(
        message=message_none_only,
        state=state_none_only,
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        services=services,
        settings=_settings(),
        sub_groups=[ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)],
    )

    assert state_none_only.current_state == RetrieveClipFlow.scope.state
    reply_markup_none_only = message_none_only.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup_none_only)
    assert _keyboard_rows(reply_markup_none_only) == [
        [DUMMY_BUTTON_TEXT, 'All'],
        [DUMMY_BUTTON_TEXT, 'Collection'],
        ['Back'],
    ]

    message_with_extra = _fake_message(message_id=76)
    state_with_extra = _FakeState()
    await _show_retrieve_sub_season_menu(
        message=message_with_extra,
        state=state_with_extra,
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        services=services,
        settings=_settings(),
        sub_groups=[
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ],
    )

    assert state_with_extra.current_state == RetrieveClipFlow.sub_season.state
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_season=SubSeason.NONE,
    )

    expected = _selected_kwargs('Store', 'West', '2024', '1', prompt='Select scope:', message_width=35)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [['Source', DUMMY_BUTTON_TEXT], ['Extra', 'Collection'], ['Back']]


@pytest.mark.asyncio
async def test_retrieve_season_menu_uses_store_slot_universe_with_dummy_substitution() -> None:
    message = _fake_message(message_id=78)
    state = _FakeState()
    settings = _settings(message_width=21)
    groups = [
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipGroup(universe=Universe.EAST, year=2024, season=Season.S3),
    ]
    await state.update_data(groups=groups)

    await _show_retrieve_season_menu(
        message=message,
        state=state,
        universe=Universe.WEST,
        year=2024,
        settings=settings,
    )

    expected = _selected_kwargs('Get', 'West', '2024', prompt='Select season:', message_width=21)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, '1'],
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        ['Back'],
    ]


@pytest.mark.asyncio
async def test_retrieve_season_menu_limits_current_year_to_store_boundary_and_uses_dummy_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 6, 15)

    monkeypatch.setattr(retrieve_module, 'date', _FixedDate)

    message = _fake_message(message_id=80)
    state = _FakeState()
    settings = _settings(message_width=21)
    groups = [
        ClipGroup(universe=Universe.WEST, year=2026, season=Season.S1),
    ]
    await state.update_data(groups=groups)

    await _show_retrieve_season_menu(
        message=message,
        state=state,
        universe=Universe.WEST,
        year=2026,
        settings=settings,
    )

    expected = _selected_kwargs('Get', 'West', '2026', prompt='Select season:', message_width=21)
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected)
    reply_markup = message.edit_text.await_args.kwargs['reply_markup']
    _assert_three_rows(reply_markup)
    assert _keyboard_rows(reply_markup) == [
        [DUMMY_BUTTON_TEXT, '1'],
        [DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT, DUMMY_BUTTON_TEXT],
        ['Back'],
    ]


@pytest.mark.asyncio
async def test_retrieve_universe_menu_uses_store_slot_universe_with_dummy_substitution() -> None:
    message = _fake_message(message_id=79)
    state = _FakeState()
    settings = _settings(message_width=18)
    groups = [
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S3),
    ]
    await state.update_data(groups=groups)

    await _show_retrieve_universe_menu(
        message=message,
        state=state,
        settings=settings,
    )

    expected = _selected_kwargs('Get', prompt='Select universe:', message_width=18)
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
    await state.update_data(
        mode='store',
        menu_message_id=50,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )

    store_results = [
        StoreResult(stored_count=1, duplicate_count=0),
        StoreResult(stored_count=2, duplicate_count=2),
    ]

    async def _store_and_assert_collapsed(*args, **kwargs) -> StoreResult:
        _assert_format_kwargs(
            message.edit_text.await_args_list[-1].kwargs,
            _selected_kwargs('Store', 'West', '2025', '1', 'Collection'),
        )
        assert message.edit_text.await_args_list[-1].kwargs['reply_markup'] is None
        return store_results.pop(0)

    clip_store = SimpleNamespace(
        store=AsyncMock(side_effect=_store_and_assert_collapsed),
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

    expected_selected = _selected_kwargs('Store', 'West', '2025', '1', 'Collection')
    _assert_format_kwargs(message.edit_text.await_args.kwargs, expected_selected)
    assert clip_store.store.await_count == 2
    first_group = clip_store.store.await_args_list[0].kwargs['clips']
    second_group = clip_store.store.await_args_list[1].kwargs['clips']
    assert first_group == [_mp4_file(b'one')]
    assert second_group == [_mp4_file(b'two'), _mp4_file(b'three')]
    expected_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    expected_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    assert clip_store.store.await_args_list[0].args[0] == expected_group
    assert clip_store.store.await_args_list[0].args[1] == expected_sub_group
    assert clip_store.store.await_args_list[1].args[0] == expected_group
    assert clip_store.store.await_args_list[1].args[1] == expected_sub_group
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
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_produce_scope_selection_stores_then_fetches_only_new_subset_via_shared_normalize_send_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=60)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            video=_fake_video(file_id='f3', file_name='three.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    await state.update_data(
        mode='produce',
        menu_message_id=60,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )

    store_results = [
        StoreResult(stored_count=1, duplicate_count=0, clip_ids=(_CLIP_ID_1,)),
        StoreResult(stored_count=2, duplicate_count=1, clip_ids=(_CLIP_ID_2, _CLIP_ID_3)),
    ]

    clip_store = _ProduceClipStore(
        store_results=store_results,
        fetched_batches=[
            [FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'one'))],
            [FetchedClip(id=_CLIP_ID_2, file=_mp4_file(b'two')), FetchedClip(id=_CLIP_ID_3, file=_mp4_file(b'three'))],
        ],
    )
    original_store = clip_store.store

    async def _store_and_assert_collapsed(*args, **kwargs) -> StoreResult:
        _assert_format_kwargs(
            message.edit_text.await_args_list[-1].kwargs,
            _selected_kwargs('Produce', 'West', '2025', '1', 'Extra'),
        )
        assert message.edit_text.await_args_list[-1].kwargs['reply_markup'] is None
        return await original_store(*args, **kwargs)

    clip_store.store = AsyncMock(side_effect=_store_and_assert_collapsed)
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.side_effect = [
        SimpleNamespace(file_path='path-1'),
        SimpleNamespace(file_path='path-2'),
        SimpleNamespace(file_path='path-3'),
    ]
    bot.download_file.side_effect = [BytesIO(b'one'), BytesIO(b'two'), BytesIO(b'three')]

    shared_helper = AsyncMock(side_effect=delivery_module.send_fetched_clip_batches)
    monkeypatch.setattr(store_execution_module, 'send_fetched_clip_batches', shared_helper)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.EXTRA.value),
        bot,
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Produce', 'West', '2025', '1', 'Extra'),
    )
    message.answer.assert_awaited_once_with(
        **Text(
            'Stored: ',
            Bold('3'),
            '\n',
            'Deduplicated: ',
            Bold('1'),
        ).as_kwargs()
    )
    assert [event[0] for event in clip_store.events] == ['store', 'store', 'compact', 'fetch']
    assert clip_store.fetch_calls == [
        (
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
            (_CLIP_ID_1, _CLIP_ID_2, _CLIP_ID_3),
            AudioNormalization(loudness=-14, bitrate=128),
        )
    ]
    shared_helper.assert_awaited_once()
    assert shared_helper.await_args.kwargs['group'] == ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    assert shared_helper.await_args.kwargs['sub_group'] == ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA)
    assert bot.send_video.await_args.kwargs['video'].data == b'normalized:one'
    assert bot.send_video.await_args.kwargs['video'].filename == _stored_filename(
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        _CLIP_ID_1,
    )
    sent_media = bot.send_media_group.await_args.kwargs['media']
    assert [item.media.filename for item in sent_media] == [
        _stored_filename(
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
            _CLIP_ID_2,
        ),
        _stored_filename(
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
            _CLIP_ID_3,
        ),
    ]
    assert [item.media.data for item in sent_media] == [b'normalized:two', b'normalized:three']
    bot.send_message.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_produce_scope_selection_with_no_new_clips_sends_summary_only() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=61)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='produce',
        menu_message_id=61,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )

    clip_store = _ProduceClipStore(
        store_results=[StoreResult(stored_count=0, duplicate_count=1)],
        fetched_batches=[[FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'nope'))]],
    )
    services = _services(clip_store=clip_store, buffer=buffer)
    bot = AsyncMock()
    bot.get_file.return_value = SimpleNamespace(file_path='path-1')
    bot.download_file.return_value = BytesIO(b'one')

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        bot,
        services,
        _settings(),
        state,
    )

    _assert_format_kwargs(
        message.edit_text.await_args.kwargs,
        _selected_kwargs('Produce', 'West', '2025', '1', 'Collection'),
    )
    message.answer.assert_awaited_once_with(**Text('Deduplicated: ', Bold('1')).as_kwargs())
    assert clip_store.fetch_calls == []
    assert [event[0] for event in clip_store.events] == ['store']
    bot.send_video.assert_not_awaited()
    bot.send_media_group.assert_not_awaited()
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_store_scope_selection_stores_bytes_when_telegram_filename_is_missing() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=50)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name=None)),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=50,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
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
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        bot,
        services,
        _settings(),
        state,
    )

    stored_clips = clip_store.store.await_args.kwargs['clips']
    assert stored_clips == [_mp4_file(b'one')]


@pytest.mark.asyncio
@pytest.mark.parametrize('scope', [Scope.EXTRA, Scope.SOURCE])
async def test_store_scope_selection_compacts_extra_and_source_batches(scope: Scope) -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=51)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=51,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
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
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.NONE, scope=scope),
        batch_size=intake_module._TELEGRAM_MEDIA_GROUP_LIMIT,
    )


@pytest.mark.asyncio
async def test_store_scope_selection_does_not_compact_when_everything_is_duplicate() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=52)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=52,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
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
async def test_store_scope_selection_keeps_compaction_failure_behavior_after_immediate_collapse() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=54)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=54,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )

    async def _store_and_assert_collapsed(*args, **kwargs) -> StoreResult:
        _assert_format_kwargs(
            message.edit_text.await_args_list[-1].kwargs,
            _selected_kwargs('Store', 'West', '2025', '1', 'Extra'),
        )
        assert message.edit_text.await_args_list[-1].kwargs['reply_markup'] is None
        return StoreResult(stored_count=1, duplicate_count=0)

    clip_store = SimpleNamespace(
        store=AsyncMock(side_effect=_store_and_assert_collapsed),
        compact=AsyncMock(side_effect=RuntimeError('boom')),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()
    bot.get_file.return_value = SimpleNamespace(file_path='path-1')
    bot.download_file.return_value = BytesIO(b'one')

    with pytest.raises(RuntimeError, match='boom'):
        await on_intake_menu(
            callback,
            IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.EXTRA.value),
            bot,
            services,
            _settings(),
            state,
        )

    _assert_format_kwargs(
        message.edit_text.await_args_list[-1].kwargs,
        _selected_kwargs('Store', 'West', '2025', '1', 'Extra'),
    )
    assert message.edit_text.await_args_list[-1].kwargs['reply_markup'] is None
    message.answer.assert_awaited_once_with(**Text('Stored: ', Bold('1')).as_kwargs())
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_store_scope_selection_edit_text_failure_preserves_fsm_state() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=55)
    message.edit_text = AsyncMock(side_effect=RuntimeError('boom'))
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=55,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )

    clip_store = SimpleNamespace(
        store=AsyncMock(),
        compact=AsyncMock(),
    )
    services = _services(clip_store=clip_store, buffer=buffer)

    bot = AsyncMock()

    with pytest.raises(RuntimeError, match='boom'):
        await on_intake_menu(
            callback,
            IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
            bot,
            services,
            _settings(),
            state,
        )

    assert state.current_state == StoreClipFlow.scope.state
    assert state.clear_count == 0
    assert [buffered_message.message_id for buffered_message in services.chat_message_buffer.peek_raw(77)] == [1]
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()


@pytest.mark.asyncio
async def test_produce_scope_selection_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=62)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='produce',
        menu_message_id=62,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name='two.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock(), fetch=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    clip_store.fetch.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_reconcile_scope_selection_uses_stored_clip_id_batches_without_downloading() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=53)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReconcileClipFlow.scope)
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_id_batches = [[_CLIP_ID_1], [_CLIP_ID_2, _CLIP_ID_3]]
    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(file_id='f2', file_name='two.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=3,
            video=_fake_video(file_id='f3', file_name='three.mp4'),
            media_group_id='g1',
        ),
        chat_id=77,
    )
    await state.update_data(
        mode=FLOW_RECONCILE,
        menu_message_id=53,
        sub_season=SubSeason.NONE,
        clip_group=clip_group,
        clip_id_batches=clip_id_batches,
        buffer_version=buffer.version(77),
    )

    clip_store = SimpleNamespace(reconcile=AsyncMock(return_value=ReconcileResult(updated=3, removed=1)))
    services = _services(clip_store=clip_store, buffer=buffer)

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
        clip_group,
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
        clip_id_batches=clip_id_batches,
    )
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()
    assert services.chat_message_buffer.peek_raw(77) == []
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
async def test_store_scope_selection_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=57)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.scope)

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            video=_fake_video(
                file_id='f1',
                file_name=_stored_filename(
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
                    _CLIP_ID_1,
                ),
            ),
        ),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=57,
        year=2025,
        season=Season.S1,
        universe=Universe.WEST,
        sub_season=SubSeason.NONE,
        buffer_version=buffer.version(77),
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name='two.mp4')),
        chat_id=77,
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_store_back_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select season:', chat_id=77, message_id=58)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.season)

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode='store',
        menu_message_id=58,
        year=2025,
        buffer_version=buffer.version(77),
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name='two.mp4')),
        chat_id=77,
    )

    services = _services(clip_store=_NoListClipStore(), buffer=buffer)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.BACK, step=MenuStep.SEASON, value='back'),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    services.clip_store.list_groups.assert_not_awaited()
    services.clip_store.list_clips.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['store', 'produce'])
async def test_store_season_selection_rejects_tampered_unavailable_season(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    class _FixedDate(date):
        @classmethod
        def today(cls) -> '_FixedDate':
            return cls(2026, 4, 9)

    monkeypatch.setattr(intake_module, 'date', _FixedDate)

    message = _fake_message(text='Select season:', chat_id=77, message_id=90)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(StoreClipFlow.season)

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode=mode,
        menu_message_id=90,
        year=2026,
        universe=Universe.WEST,
        buffer_version=buffer.version(77),
    )

    clip_store = SimpleNamespace(store=AsyncMock(), compact=AsyncMock(), fetch=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SEASON, value=str(int(Season.S5))),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    clip_store.store.assert_not_awaited()
    clip_store.compact.assert_not_awaited()
    clip_store.fetch.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_reconcile_scope_selection_becomes_stale_when_buffer_version_changes() -> None:
    message = _fake_message(text='Select scope:', chat_id=77, message_id=59)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.set_state(ReconcileClipFlow.scope)

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(chat_id=77, message_id=1, video=_fake_video(file_id='f1', file_name='one.mp4')),
        chat_id=77,
    )
    await state.update_data(
        mode=FLOW_RECONCILE,
        menu_message_id=59,
        sub_season=SubSeason.NONE,
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        clip_id_batches=[[_CLIP_ID_1]],
        buffer_version=buffer.version(77),
    )
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=2,
            video=_fake_video(
                file_id='f2',
                file_name=_stored_filename(
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
                    _CLIP_ID_2,
                ),
            ),
        ),
        chat_id=77,
    )

    clip_store = SimpleNamespace(reconcile=AsyncMock())
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_menu(
        callback,
        IntakeCallbackData(action=MenuAction.SELECT, step=MenuStep.SCOPE, value=Scope.COLLECTION.value),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.edit_text.assert_awaited_once_with('Selection is no longer available', reply_markup=None)
    clip_store.reconcile.assert_not_awaited()
    assert state.current_state is None
    assert state.clear_count == 1


@pytest.mark.asyncio
async def test_reconcile_entry_skips_text_only_buffered_groups() -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=55)
    callback = _fake_callback(message)
    state = _FakeState()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    filename = _stored_filename(clip_group, clip_sub_group, _CLIP_ID_1)

    buffer = ChatMessageBuffer()
    buffer.append(_fake_message(chat_id=77, message_id=1, text='note'), chat_id=77)
    buffer.append(_fake_message(chat_id=77, message_id=2, text='ignored', media_group_id='g1'), chat_id=77)
    buffer.append(
        _fake_message(chat_id=77, message_id=3, video=_fake_video(file_id='f1', file_name=filename)),
        chat_id=77,
    )
    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.answer.assert_not_awaited()
    assert state.data['clip_group'] == clip_group
    assert state.data['clip_id_batches'] == [[_CLIP_ID_1]]


@pytest.mark.asyncio
@pytest.mark.parametrize('file_name', [None, ''])
async def test_reconcile_entry_with_missing_video_filename_fails_cleanly_without_reusing_saved_state(
    file_name: str | None,
) -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=56)
    callback = _fake_callback(message)
    state = _FakeState()
    await state.update_data(
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_id_batches=[[_CLIP_ID_1]],
        buffer_version=0,
    )
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)

    buffer = ChatMessageBuffer()
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            video=_fake_video(file_id='f1', file_name=_stored_filename(clip_group, clip_sub_group, _CLIP_ID_1)),
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name=file_name)),
        chat_id=77,
    )

    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.answer.assert_not_awaited()
    message.edit_text.assert_awaited_once_with("Can't reconcile not stored", reply_markup=None)
    assert services.chat_message_buffer.peek_raw(77) == []
    assert state.current_state is None
    assert state.data == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('file_name', 'expected_message'),
    [
        (
            _stored_filename(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
                _CLIP_ID_1,
            ),
            "Can't reconcile duplicates",
        ),
        ('not-a-valid-identity.mp4', "Can't reconcile not stored"),
        (
            _stored_filename(
                ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
                _CLIP_ID_1,
            ),
            "Can't reconcile mixed groups",
        ),
        ('bad.identity', "Can't reconcile not stored"),
    ],
)
async def test_reconcile_entry_surfaces_parse_errors(
    file_name: str,
    expected_message: str,
) -> None:
    message = _fake_message(text='Got 1 clip', chat_id=77, message_id=54)
    callback = _fake_callback(message)
    state = _FakeState()

    buffer = ChatMessageBuffer()
    west_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    west_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    buffer.append(
        _fake_message(
            chat_id=77,
            message_id=1,
            video=_fake_video(file_id='f1', file_name=_stored_filename(west_group, west_sub_group, _CLIP_ID_1)),
        ),
        chat_id=77,
    )
    buffer.append(
        _fake_message(chat_id=77, message_id=2, video=_fake_video(file_id='f2', file_name=file_name)),
        chat_id=77,
    )

    clip_store = SimpleNamespace()
    services = _services(clip_store=clip_store, buffer=buffer)

    await on_intake_action(
        callback,
        SimpleNamespace(action=IntakeAction.RECONCILE),
        AsyncMock(),
        services,
        _settings(),
        state,
    )

    message.answer.assert_not_awaited()
    message.edit_text.assert_awaited_once_with(expected_message, reply_markup=None)
    assert state.current_state is None
    assert services.chat_message_buffer.peek_raw(77) == []
    assert state.data == {}


@pytest.mark.asyncio
async def test_send_fetched_clip_batch_preserves_filename() -> None:
    bot = AsyncMock()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)

    await send_fetched_clip_batch(
        bot=bot,
        chat_id=5,
        group=clip_group,
        sub_group=clip_sub_group,
        clips=[
            FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'a')),
            FetchedClip(id=_CLIP_ID_2, file=_mp4_file(b'b')),
        ],
    )

    media = bot.send_media_group.await_args.kwargs['media']
    assert media[0].media.filename == _stored_filename(clip_group, clip_sub_group, _CLIP_ID_1)
    assert media[1].media.filename == _stored_filename(clip_group, clip_sub_group, _CLIP_ID_2)


@pytest.mark.asyncio
async def test_send_fetched_clip_batch_sends_single_clip_as_video() -> None:
    bot = AsyncMock()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)

    await send_fetched_clip_batch(
        bot=bot,
        chat_id=5,
        group=clip_group,
        sub_group=clip_sub_group,
        clips=[FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'a'))],
    )

    assert bot.send_video.await_args.kwargs['video'].filename == _stored_filename(
        clip_group, clip_sub_group, _CLIP_ID_1
    )
    bot.send_media_group.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_retrieve_scopes_sends_separator_only_between_scope_blocks_and_done() -> None:
    bot = _RecordingBot()
    services = _services(
        clip_store=_RetrieveClipStore(
            {
                Scope.COLLECTION: [[FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'1'))]],
                Scope.EXTRA: [[FetchedClip(id=_CLIP_ID_2, file=_mp4_file(b'2'))]],
            }
        )
    )

    await _send_retrieve_scopes(
        bot=bot,
        chat_id=9,
        services=services,
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        sub_season=SubSeason.NONE,
        scopes=[Scope.COLLECTION, Scope.EXTRA],
        settings=_settings(),
        normalize_audio=False,
    )

    assert services.clip_store.calls == [
        (
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
            None,
            None,
        ),
        (
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
            None,
            None,
        ),
    ]
    assert bot.events == [
        (
            'video',
            (
                9,
                _stored_filename(
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
                    _CLIP_ID_1,
                ),
            ),
        ),
        ('message', (9, '.')),
        (
            'video',
            (
                9,
                _stored_filename(
                    ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
                    ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
                    _CLIP_ID_2,
                ),
            ),
        ),
    ]


@pytest.mark.asyncio
async def test_send_retrieve_scopes_requests_normalized_fetch_and_sends_results() -> None:
    bot = AsyncMock()
    clip_group = ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION)
    clip_store = _RetrieveClipStore(
        {
            Scope.COLLECTION: [
                [FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'one'))],
                [
                    FetchedClip(id=_CLIP_ID_2, file=_mp4_file(b'two')),
                    FetchedClip(id=_CLIP_ID_3, file=_mp4_file(b'three')),
                ],
            ]
        }
    )
    services = _services(clip_store=clip_store)

    await _send_retrieve_scopes(
        bot=bot,
        chat_id=9,
        services=services,
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        sub_season=SubSeason.NONE,
        scopes=[Scope.COLLECTION],
        settings=_settings(normalization_loudness=-13, normalization_bitrate=160),
        normalize_audio=True,
    )

    assert clip_store.calls == [
        (
            clip_group,
            clip_sub_group,
            None,
            AudioNormalization(loudness=-13, bitrate=160),
        )
    ]
    assert bot.send_video.await_args.kwargs['video'].filename == _stored_filename(
        clip_group, clip_sub_group, _CLIP_ID_1
    )
    assert bot.send_video.await_args.kwargs['video'].data == b'normalized:one'
    sent_media = bot.send_media_group.await_args.kwargs['media']
    assert [item.media.filename for item in sent_media] == [
        _stored_filename(clip_group, clip_sub_group, _CLIP_ID_2),
        _stored_filename(clip_group, clip_sub_group, _CLIP_ID_3),
    ]
    assert [item.media.data for item in sent_media] == [b'normalized:two', b'normalized:three']
    assert clip_store.batches_by_scope[Scope.COLLECTION] == [
        [FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'one'))],
        [
            FetchedClip(id=_CLIP_ID_2, file=_mp4_file(b'two')),
            FetchedClip(id=_CLIP_ID_3, file=_mp4_file(b'three')),
        ],
    ]


@pytest.mark.asyncio
async def test_send_retrieve_scopes_sends_raw_batches_when_normalization_is_disabled() -> None:
    bot = AsyncMock()
    services = _services(
        clip_store=_RetrieveClipStore(
            {
                Scope.COLLECTION: [
                    [FetchedClip(id=_CLIP_ID_1, file=_mp4_file(b'one'))],
                ]
            }
        )
    )

    await _send_retrieve_scopes(
        bot=bot,
        chat_id=9,
        services=services,
        clip_group=ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
        sub_season=SubSeason.NONE,
        scopes=[Scope.COLLECTION],
        settings=_settings(),
        normalize_audio=False,
    )

    assert services.clip_store.calls == [
        (
            ClipGroup(universe=Universe.WEST, year=2025, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.COLLECTION),
            None,
            None,
        )
    ]
    assert bot.send_video.await_args.kwargs['video'].data == b'one'
    bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_dummy_button_handler_only_answers() -> None:
    message = _fake_message(message_id=90)
    callback = _fake_callback(message)

    await on_dummy_button(callback)

    callback.answer.assert_awaited_once_with()
    message.edit_text.assert_not_awaited()
    message.answer.assert_not_awaited()
