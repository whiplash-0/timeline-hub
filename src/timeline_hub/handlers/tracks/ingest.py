from datetime import date
from enum import StrEnum, auto
from typing import Any

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.formatting import Bold, Text
from loguru import logger

from timeline_hub.handlers.menu import (
    back_button,
    callback_message,
    create_padding_line,
    fixed_option_keyboard,
    handle_stale_selection,
    selected_text,
    selection_keyboard,
    selection_text,
    validate_flow_state,
)
from timeline_hub.handlers.tracks.store_execution import (
    TrackInputError,
    extract_single_photo_audio_messages,
    extract_track_identity_from_photo_message,
    prepare_audio_from_message,
    prepare_tracks_from_buffer,
)
from timeline_hub.services.container import Services
from timeline_hub.services.track_store import (
    InvalidTrackIdentityError,
    Season,
    SubSeason,
    TrackGroup,
    TrackGroupNotFoundError,
    TrackInfo,
    TrackInvalidAudioFormatError,
    TrackManifestCorruptedError,
    TrackPresetsCorruptedError,
    TrackUniverse,
    TrackUpdateManifestSyncError,
)
from timeline_hub.settings import Settings
from timeline_hub.types import InvalidExtensionError

router = Router()
_TRACK_STORE_MODE = 'track_store'
_TRACK_BACK_VALUE = 'back'


class TrackIntakeAction(StrEnum):
    STORE = auto()
    REPLACE = auto()
    TRACK = auto()
    INSTRUMENTAL = auto()
    BACK = auto()
    CANCEL = auto()


class TrackIntakeActionCallbackData(CallbackData, prefix='track_intake'):
    action: TrackIntakeAction
    buffer_version: int


class TrackStoreAction(StrEnum):
    SELECT = auto()
    BACK = auto()


class TrackStoreStep(StrEnum):
    UNIVERSE = auto()
    YEAR = auto()
    SEASON = auto()
    SUB_SEASON = auto()


class TrackStoreCallbackData(CallbackData, prefix='track_store'):
    action: TrackStoreAction
    step: TrackStoreStep
    value: str


class TrackStoreFlow(StatesGroup):
    universe = State()
    year = State()
    season = State()
    sub_season = State()


@router.callback_query(
    TrackIntakeActionCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_track_intake_action(
    callback: CallbackQuery,
    callback_data: TrackIntakeActionCallbackData,
    state: FSMContext,
    services: Services,
    settings: Settings,
) -> None:
    await callback.answer()
    message = callback_message(callback)
    if message is None:
        await state.clear()
        return

    if callback_data.buffer_version != services.chat_message_buffer.version(message.chat.id):
        await handle_stale_selection(message=message, state=state)
        return

    if callback_data.action is TrackIntakeAction.CANCEL:
        await state.clear()
        await message.edit_text('Canceled', reply_markup=None)
        services.chat_message_buffer.flush(message.chat.id)
        return

    if callback_data.action is TrackIntakeAction.REPLACE:
        await message.edit_text(
            **_track_replace_menu_kwargs(
                message_width=settings.message_width,
                buffer_version=callback_data.buffer_version,
                message_count=len(services.chat_message_buffer.peek_flat(message.chat.id)),
            )
        )
        return

    if callback_data.action is TrackIntakeAction.BACK:
        await message.edit_text(
            **_track_intake_menu_kwargs(
                message_width=settings.message_width,
                buffer_version=callback_data.buffer_version,
                message_count=len(services.chat_message_buffer.peek_flat(message.chat.id)),
            )
        )
        return

    if callback_data.action in (TrackIntakeAction.TRACK, TrackIntakeAction.INSTRUMENTAL):
        bot = getattr(callback, 'bot', None)
        if bot is None:
            await _invalidate_track_intake_buffer(message=message, state=state, services=services, text='Invalid input')
            return
        await _execute_track_update(
            message=message,
            state=state,
            services=services,
            action=callback_data.action,
            bot=bot,
            expected_buffer_version=callback_data.buffer_version,
        )
        return

    await _show_store_universe_menu(
        message=message,
        state=state,
        services=services,
        settings=settings,
    )


@router.callback_query(
    TrackStoreCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_track_store_menu(
    callback: CallbackQuery,
    callback_data: TrackStoreCallbackData,
    state: FSMContext,
    services: Services,
    settings: Settings,
    bot: Bot,
) -> None:
    await callback.answer()
    message = callback_message(callback)
    if message is None:
        await state.clear()
        return

    if not await _validate_track_store_callback(
        message=message,
        state=state,
        services=services,
        step=callback_data.step,
    ):
        return

    data = await state.get_data()
    if callback_data.action is TrackStoreAction.BACK:
        await _handle_track_store_back(
            message=message,
            state=state,
            services=services,
            settings=settings,
            step=callback_data.step,
            data=data,
        )
        return

    match callback_data.step:
        case TrackStoreStep.UNIVERSE:
            try:
                universe = TrackUniverse(callback_data.value)
            except ValueError:
                await handle_stale_selection(message=message, state=state)
                return
            await _show_store_year_menu(
                message=message,
                state=state,
                settings=settings,
                buffer_version=_buffer_version_from_state(data),
                universe=universe,
            )
        case TrackStoreStep.YEAR:
            universe = _selected_universe(data)
            if universe is None:
                await handle_stale_selection(message=message, state=state)
                return
            try:
                year = int(callback_data.value)
            except ValueError:
                await handle_stale_selection(message=message, state=state)
                return
            await _show_store_season_menu(
                message=message,
                state=state,
                settings=settings,
                buffer_version=_buffer_version_from_state(data),
                universe=universe,
                year=year,
            )
        case TrackStoreStep.SEASON:
            selection = _selected_universe_year(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            universe, year = selection
            try:
                season = Season(int(callback_data.value))
            except ValueError:
                await handle_stale_selection(message=message, state=state)
                return
            await _show_store_sub_season_menu(
                message=message,
                state=state,
                settings=settings,
                buffer_version=_buffer_version_from_state(data),
                universe=universe,
                year=year,
                season=season,
            )
        case TrackStoreStep.SUB_SEASON:
            selection = _selected_universe_year_season(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            universe, year, season = selection
            try:
                sub_season = SubSeason(callback_data.value)
            except ValueError:
                await handle_stale_selection(message=message, state=state)
                return
            await _execute_track_store(
                message=message,
                state=state,
                services=services,
                bot=bot,
                sub_season=sub_season,
                universe=universe,
                year=year,
                season=season,
            )


async def try_dispatch_track_intake(
    *,
    message: Message,
    services: Services,
    settings: Settings,
) -> bool:
    await message.answer(
        **_track_intake_menu_kwargs(
            message_width=settings.message_width,
            buffer_version=services.chat_message_buffer.version(message.chat.id),
            message_count=len(services.chat_message_buffer.peek_flat(message.chat.id)),
        )
    )
    return True


def _track_intake_menu_kwargs(
    *,
    message_width: int,
    buffer_version: int,
    message_count: int,
) -> dict[str, Any]:
    return {
        **Text(
            create_padding_line(message_width),
            '\n',
            Text('Messages: ', Bold(str(message_count))),
        ).as_kwargs(),
        'reply_markup': selection_keyboard(
            buttons=[
                InlineKeyboardButton(
                    text='Store',
                    callback_data=TrackIntakeActionCallbackData(
                        action=TrackIntakeAction.STORE,
                        buffer_version=buffer_version,
                    ).pack(),
                ),
                InlineKeyboardButton(
                    text='Replace',
                    callback_data=TrackIntakeActionCallbackData(
                        action=TrackIntakeAction.REPLACE,
                        buffer_version=buffer_version,
                    ).pack(),
                ),
            ],
            back_button=InlineKeyboardButton(
                text='Cancel',
                callback_data=TrackIntakeActionCallbackData(
                    action=TrackIntakeAction.CANCEL,
                    buffer_version=buffer_version,
                ).pack(),
            ),
        ),
    }


def _track_replace_menu_kwargs(
    *,
    message_width: int,
    buffer_version: int,
    message_count: int,
) -> dict[str, Any]:
    return {
        **Text(
            create_padding_line(message_width),
            '\n',
            Text('Messages: ', Bold(str(message_count))),
        ).as_kwargs(),
        'reply_markup': selection_keyboard(
            buttons=[
                InlineKeyboardButton(
                    text='Track',
                    callback_data=TrackIntakeActionCallbackData(
                        action=TrackIntakeAction.TRACK,
                        buffer_version=buffer_version,
                    ).pack(),
                ),
                InlineKeyboardButton(
                    text='Instrumental',
                    callback_data=TrackIntakeActionCallbackData(
                        action=TrackIntakeAction.INSTRUMENTAL,
                        buffer_version=buffer_version,
                    ).pack(),
                ),
            ],
            back_button=InlineKeyboardButton(
                text='Back',
                callback_data=TrackIntakeActionCallbackData(
                    action=TrackIntakeAction.BACK,
                    buffer_version=buffer_version,
                ).pack(),
            ),
        ),
    }


async def _show_track_intake_action_menu(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
) -> None:
    await state.clear()
    await message.edit_text(
        **_track_intake_menu_kwargs(
            message_width=settings.message_width,
            buffer_version=services.chat_message_buffer.version(message.chat.id),
            message_count=len(services.chat_message_buffer.peek_flat(message.chat.id)),
        )
    )


async def _show_store_universe_menu(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
) -> None:
    await _set_track_store_context(
        state=state,
        fsm_state=TrackStoreFlow.universe,
        menu_message_id=message.message_id,
        buffer_version=services.chat_message_buffer.version(message.chat.id),
    )
    await message.edit_text(
        **selection_text(
            selected=['Store'],
            prompt='Select universe:',
            message_width=settings.message_width,
        ),
        reply_markup=fixed_option_keyboard(
            option_universe=tuple(TrackUniverse),
            available_options=tuple(TrackUniverse),
            build_button=lambda universe: InlineKeyboardButton(
                text=_format_universe(universe),
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.SELECT,
                    step=TrackStoreStep.UNIVERSE,
                    value=universe.value,
                ).pack(),
            ),
            back_button=back_button(
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.BACK,
                    step=TrackStoreStep.UNIVERSE,
                    value=_TRACK_BACK_VALUE,
                ).pack(),
            ),
        ),
    )


async def _show_store_year_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    buffer_version: int,
    universe: TrackUniverse,
) -> None:
    year_options = list(range(date.today().year, settings.min_clip_year - 1, -1))
    await _set_track_store_context(
        state=state,
        fsm_state=TrackStoreFlow.year,
        menu_message_id=message.message_id,
        buffer_version=buffer_version,
        universe=universe,
    )
    await message.edit_text(
        **selection_text(
            selected=['Store', _format_universe(universe)],
            prompt='Select year:',
            message_width=settings.message_width,
        ),
        reply_markup=fixed_option_keyboard(
            option_universe=year_options,
            available_options=year_options,
            build_button=lambda year: InlineKeyboardButton(
                text=str(year),
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.SELECT,
                    step=TrackStoreStep.YEAR,
                    value=str(year),
                ).pack(),
            ),
            back_button=back_button(
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.BACK,
                    step=TrackStoreStep.YEAR,
                    value=_TRACK_BACK_VALUE,
                ).pack(),
            ),
        ),
    )


async def _show_store_season_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    buffer_version: int,
    universe: TrackUniverse,
    year: int,
) -> None:
    available_seasons = _available_store_seasons(year=year, today=date.today())
    await _set_track_store_context(
        state=state,
        fsm_state=TrackStoreFlow.season,
        menu_message_id=message.message_id,
        buffer_version=buffer_version,
        universe=universe,
        year=year,
    )
    await message.edit_text(
        **selection_text(
            selected=['Store', _format_universe(universe), str(year)],
            prompt='Select season:',
            message_width=settings.message_width,
        ),
        reply_markup=fixed_option_keyboard(
            option_universe=tuple(Season),
            available_options=available_seasons,
            build_button=lambda season: InlineKeyboardButton(
                text=str(int(season)),
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.SELECT,
                    step=TrackStoreStep.SEASON,
                    value=str(int(season)),
                ).pack(),
            ),
            back_button=back_button(
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.BACK,
                    step=TrackStoreStep.SEASON,
                    value=_TRACK_BACK_VALUE,
                ).pack(),
            ),
        ),
    )


async def _show_store_sub_season_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    buffer_version: int,
    universe: TrackUniverse,
    year: int,
    season: Season,
) -> None:
    await _set_track_store_context(
        state=state,
        fsm_state=TrackStoreFlow.sub_season,
        menu_message_id=message.message_id,
        buffer_version=buffer_version,
        universe=universe,
        year=year,
        season=season,
    )
    await message.edit_text(
        **selection_text(
            selected=['Store', _format_universe(universe), str(year), str(int(season))],
            prompt='Select sub-season:',
            message_width=settings.message_width,
        ),
        reply_markup=fixed_option_keyboard(
            option_universe=tuple(SubSeason),
            available_options=tuple(SubSeason),
            build_button=lambda sub_season: InlineKeyboardButton(
                text=_format_sub_season(sub_season),
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.SELECT,
                    step=TrackStoreStep.SUB_SEASON,
                    value=sub_season.value,
                ).pack(),
            ),
            back_button=back_button(
                callback_data=TrackStoreCallbackData(
                    action=TrackStoreAction.BACK,
                    step=TrackStoreStep.SUB_SEASON,
                    value=_TRACK_BACK_VALUE,
                ).pack(),
            ),
        ),
    )


async def _handle_track_store_back(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    step: TrackStoreStep,
    data: dict[str, object],
) -> None:
    match step:
        case TrackStoreStep.UNIVERSE:
            await _show_track_intake_action_menu(
                message=message,
                state=state,
                services=services,
                settings=settings,
            )
        case TrackStoreStep.YEAR:
            await _show_store_universe_menu(
                message=message,
                state=state,
                services=services,
                settings=settings,
            )
        case TrackStoreStep.SEASON:
            universe = _selected_universe(data)
            if universe is None:
                await handle_stale_selection(message=message, state=state)
                return
            await _show_store_year_menu(
                message=message,
                state=state,
                settings=settings,
                buffer_version=_buffer_version_from_state(data),
                universe=universe,
            )
        case TrackStoreStep.SUB_SEASON:
            selection = _selected_universe_year(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            universe, year = selection
            await _show_store_season_menu(
                message=message,
                state=state,
                settings=settings,
                buffer_version=_buffer_version_from_state(data),
                universe=universe,
                year=year,
            )


async def _execute_track_store(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    bot: Bot,
    sub_season: SubSeason,
    universe: TrackUniverse,
    year: int,
    season: Season,
) -> None:
    chat_id = message.chat.id
    expected_version = _buffer_version_from_state(await state.get_data())
    if services.chat_message_buffer.version(chat_id) != expected_version:
        await handle_stale_selection(message=message, state=state)
        return

    await message.edit_text(
        **selected_text(
            selected=_selected_store_path(
                universe=universe,
                year=year,
                season=season,
                sub_season=sub_season,
            ),
        ),
        reply_markup=None,
    )

    buffered_messages = services.chat_message_buffer.peek_flat(chat_id)
    try:
        prepared_tracks = await prepare_tracks_from_buffer(
            bot=bot,
            messages=buffered_messages,
        )
    except TrackInputError as error:
        await state.clear()
        services.chat_message_buffer.flush(message.chat.id)
        await message.edit_text(str(error), reply_markup=None)
        return

    if services.chat_message_buffer.version(chat_id) != expected_version:
        await handle_stale_selection(message=message, state=state)
        return

    group = TrackGroup(universe=universe, year=year, season=season)
    # Multi-track store is best-effort convenience only.
    # Atomicity is per track, so partial success is allowed and reported.
    stored_titles: list[str] = []
    try:
        for track in prepared_tracks:
            await services.track_store.store(group, sub_season, track=track)
            stored_titles.append(track.title)
    except Exception:
        logger.exception('Track store failed')
        await state.clear()
        services.chat_message_buffer.flush(chat_id)
        if stored_titles:
            await message.answer(text='Storing failed\nStored titles:\n' + '\n'.join(stored_titles))
        else:
            await message.answer(text='Storing failed\nNo tracks were stored')
        return

    await state.clear()
    services.chat_message_buffer.flush(chat_id)
    await message.answer(**Text('Stored: ', Bold(str(len(prepared_tracks)))).as_kwargs())


async def _execute_track_update(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    action: TrackIntakeAction,
    bot: Bot,
    expected_buffer_version: int,
) -> None:
    chat_id = message.chat.id
    buffered_messages = services.chat_message_buffer.peek_flat(chat_id)
    owns_buffer = False
    try:
        photo_message, audio_message = extract_single_photo_audio_messages(buffered_messages)
        group, track_id = extract_track_identity_from_photo_message(photo_message)
        tracks_by_sub_season = await services.track_store.list_tracks(group)
        matched_sub_season = _resolve_track_sub_season(
            tracks_by_sub_season,
            track_id,
        )
        # Track id may not exist in current manifest (e.g. stale cover identity)
        # -> treat as user-level invalid input
        if matched_sub_season is None:
            raise TrackInputError('Invalid input')
        if services.chat_message_buffer.version(chat_id) != expected_buffer_version:
            await handle_stale_selection(message=message, state=state)
            return
        services.chat_message_buffer.flush(chat_id)
        await state.clear()
        owns_buffer = True
        action_label = 'Track' if action is TrackIntakeAction.TRACK else 'Instrumental'
        selected = [
            action_label,
            _format_universe(group.universe),
            str(group.year),
            str(int(group.season)),
        ]
        if matched_sub_season.exists:
            selected.append(matched_sub_season.value)
        await message.edit_text(
            **selected_text(selected=selected),
            reply_markup=None,
        )
        prepared_audio = await prepare_audio_from_message(bot=bot, audio_message=audio_message)
        if action is TrackIntakeAction.TRACK:
            await services.track_store.update(group, track_id, track=prepared_audio)
        else:
            await services.track_store.update(group, track_id, instrumental=prepared_audio)
    except (
        TrackInputError,
        InvalidTrackIdentityError,
        TrackGroupNotFoundError,
        TrackInvalidAudioFormatError,
        InvalidExtensionError,
    ):
        await _invalidate_track_intake_buffer(
            message=message,
            state=state,
            services=services,
            text='Invalid input',
            flush_buffer=not owns_buffer,
        )
        return
    except ValueError as error:
        if _is_missing_track_error(error):
            await _invalidate_track_intake_buffer(
                message=message,
                state=state,
                services=services,
                text='Invalid input',
                flush_buffer=not owns_buffer,
            )
            return
        raise
    except (
        TrackUpdateManifestSyncError,
        TrackManifestCorruptedError,
        TrackPresetsCorruptedError,
    ):
        raise

    await message.answer('Done')


async def _invalidate_track_intake_buffer(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    text: str,
    flush_buffer: bool = True,
) -> None:
    await state.clear()
    if flush_buffer:
        services.chat_message_buffer.flush(message.chat.id)
    await message.edit_text(text, reply_markup=None)


async def _set_track_store_context(
    *,
    state: FSMContext,
    fsm_state: State,
    menu_message_id: int,
    buffer_version: int,
    universe: TrackUniverse | None = None,
    year: int | None = None,
    season: Season | None = None,
) -> None:
    await state.set_state(fsm_state)
    await state.update_data(
        mode=_TRACK_STORE_MODE,
        menu_message_id=menu_message_id,
        buffer_version=buffer_version,
        universe=universe,
        year=year,
        season=season,
    )


async def _validate_track_store_callback(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    step: TrackStoreStep,
) -> bool:
    if not await validate_flow_state(
        message=message,
        state=state,
        expected_mode=_TRACK_STORE_MODE,
        expected_state=_state_for_step(step),
    ):
        return False

    data = await state.get_data()
    if _buffer_version_from_state(data) != services.chat_message_buffer.version(message.chat.id):
        await handle_stale_selection(message=message, state=state)
        return False
    return True


def _state_for_step(step: TrackStoreStep) -> State:
    match step:
        case TrackStoreStep.UNIVERSE:
            return TrackStoreFlow.universe
        case TrackStoreStep.YEAR:
            return TrackStoreFlow.year
        case TrackStoreStep.SEASON:
            return TrackStoreFlow.season
        case TrackStoreStep.SUB_SEASON:
            return TrackStoreFlow.sub_season


def _available_store_seasons(*, year: int, today: date) -> list[Season]:
    if year != today.year:
        return list(Season)
    max_season = Season.from_month(today.month)
    return [season for season in Season if season <= max_season]


def _buffer_version_from_state(data: dict[str, object]) -> int:
    buffer_version = data.get('buffer_version')
    if not isinstance(buffer_version, int):
        raise ValueError('Missing track buffer version')
    return buffer_version


def _selected_universe(data: dict[str, object]) -> TrackUniverse | None:
    universe = data.get('universe')
    if isinstance(universe, TrackUniverse):
        return universe
    return None


def _selected_universe_year(data: dict[str, object]) -> tuple[TrackUniverse, int] | None:
    universe = _selected_universe(data)
    year = data.get('year')
    if universe is None or not isinstance(year, int):
        return None
    return universe, year


def _selected_universe_year_season(data: dict[str, object]) -> tuple[TrackUniverse, int, Season] | None:
    selection = _selected_universe_year(data)
    season = data.get('season')
    if selection is None or not isinstance(season, Season):
        return None
    universe, year = selection
    return universe, year, season


def _selected_store_path(
    *,
    universe: TrackUniverse,
    year: int,
    season: Season,
    sub_season: SubSeason,
) -> list[str]:
    selected = ['Store', _format_universe(universe), str(year), str(int(season))]
    if sub_season.exists:
        selected.append(_format_sub_season(sub_season))
    return selected


def _is_missing_track_error(error: ValueError) -> bool:
    return str(error).startswith('Track id ') and ' does not exist in group ' in str(error)


def _resolve_track_sub_season(
    tracks_by_sub_season: dict[SubSeason, list[TrackInfo]],
    track_id: str,
) -> SubSeason | None:
    for sub_season, tracks in tracks_by_sub_season.items():
        if any(track.id == track_id for track in tracks):
            return sub_season
    return None


def _format_universe(universe: TrackUniverse) -> str:
    return universe.value.title()


def _format_sub_season(sub_season: SubSeason) -> str:
    if sub_season is SubSeason.NONE:
        return 'None'
    return sub_season.value
