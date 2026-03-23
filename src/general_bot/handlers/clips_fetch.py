from collections.abc import Sequence
from datetime import date
from enum import StrEnum, auto

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, CallbackQuery, InlineKeyboardButton, InputMediaVideo, Message

from general_bot.handlers.clips_common import (
    ALL_SCOPES_CALLBACK_VALUE,
    FETCH_STATE_BY_STEP,
    FLOW_FETCH,
    FLOW_FETCH_RAW,
    MenuAction,
    MenuStep,
    callback_message,
    encode_sub_season,
    handle_stale_selection,
    parse_scope,
    parse_season,
    parse_sub_season,
    parse_universe,
    parse_year,
    selected_text,
    selection_text,
    stacked_keyboard,
    terminate_menu,
    width_reserved_text,
)
from general_bot.handlers.clips_flow import (
    FlowMenuDefinition,
    available_group_seasons,
    available_group_universes,
    available_group_years,
    available_scopes,
    available_sub_seasons,
    flow_selection_labels,
    scope_option_callback_value,
    scope_option_text,
    selected_year,
    selected_year_season,
    selected_year_season_universe,
    selected_year_season_universe_sub_season,
    show_fixed_option_menu,
    show_or_stale,
    store_allowed_seasons,
    validate_menu_flow_state,
    year_option_universe,
)
from general_bot.infra.ffmpeg import normalize_audio_loudness
from general_bot.services.clip_store import (
    Clip,
    ClipGroup,
    ClipGroupNotFoundError,
    ClipSubGroup,
    Scope,
    Season,
    SubSeason,
    Universe,
)
from general_bot.services.container import Services
from general_bot.settings import Settings
from general_bot.types import ChatId

router = Router()


class FetchEntryAction(StrEnum):
    FETCH = auto()
    FETCH_RAW = auto()
    CANCEL = auto()


class FetchEntryCallbackData(CallbackData, prefix='clip_fetch_entry'):
    action: FetchEntryAction


class FetchCallbackData(CallbackData, prefix='clip_fetch'):
    action: MenuAction
    step: MenuStep
    value: str


def _pack_fetch_menu_callback(action: MenuAction, step: MenuStep, value: str) -> str:
    return FetchCallbackData(action=action, step=step, value=value).pack()


_FETCH_FLOW = FlowMenuDefinition(
    mode=FLOW_FETCH,
    flow_label='Fetch',
    state_by_step=FETCH_STATE_BY_STEP,
    pack_callback=_pack_fetch_menu_callback,
)

_FETCH_RAW_FLOW = FlowMenuDefinition(
    mode=FLOW_FETCH_RAW,
    flow_label='Fetch raw',
    state_by_step=FETCH_STATE_BY_STEP,
    pack_callback=_pack_fetch_menu_callback,
)


@router.message(F.text == 'Clips')
async def on_clips(message: Message, state: FSMContext, settings: Settings) -> None:
    await state.clear()
    await message.answer(
        **width_reserved_text(
            text='Select action:',
            message_width=settings.message_width,
        ),
        reply_markup=_fetch_entry_reply_markup(),
    )


@router.callback_query(
    FetchEntryCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_fetch_entry(
    callback: CallbackQuery,
    callback_data: FetchEntryCallbackData,
    services: Services,
    settings: Settings,
    state: FSMContext,
) -> None:
    await callback.answer()
    message = callback_message(callback)
    if message is None:
        await state.clear()
        return

    if callback_data.action is FetchEntryAction.CANCEL:
        await state.clear()
        await message.edit_text(
            **selected_text(selected='Cancel'),
            reply_markup=None,
        )
        return

    flow = _flow_for_entry_action(callback_data.action)
    if flow is None:
        await state.clear()
        return

    groups = await services.clip_store.list_groups()
    if not groups:
        await terminate_menu(
            message=message,
            state=state,
            text='No clips stored',
        )
        return

    await _show_fetch_year_menu(
        message=message,
        state=state,
        settings=settings,
        flow=flow,
        groups=groups,
    )


@router.callback_query(
    FetchCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_fetch_menu(
    callback: CallbackQuery,
    callback_data: FetchCallbackData,
    bot: Bot,
    services: Services,
    settings: Settings,
    state: FSMContext,
) -> None:
    await callback.answer()
    message = callback_message(callback)
    if message is None:
        await state.clear()
        return

    data = await state.get_data()
    flow = _flow_for_mode(data.get('mode'))
    if flow is None:
        await handle_stale_selection(message=message, state=state)
        return

    if not await validate_menu_flow_state(
        message=message,
        state=state,
        flow=flow,
        step=callback_data.step,
    ):
        return

    if callback_data.action is MenuAction.BACK:
        await _on_fetch_back(
            message=message,
            state=state,
            services=services,
            settings=settings,
            step=callback_data.step,
            flow=flow,
        )
        return

    await _on_fetch_select(
        message=message,
        state=state,
        services=services,
        settings=settings,
        bot=bot,
        callback_data=callback_data,
        flow=flow,
    )


async def _on_fetch_back(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    step: MenuStep,
    flow: FlowMenuDefinition,
) -> None:
    data = await state.get_data()

    match step:
        case MenuStep.YEAR:
            await _show_fetch_entry_menu(message=message, state=state, settings=settings)

        case MenuStep.SEASON:
            await show_or_stale(
                show_menu=_show_fetch_year_menu,
                message=message,
                state=state,
                settings=settings,
                services=services,
                flow=flow,
            )

        case MenuStep.UNIVERSE:
            year = selected_year(data)
            if year is None:
                await handle_stale_selection(message=message, state=state)
                return
            await show_or_stale(
                show_menu=_show_fetch_season_menu,
                message=message,
                state=state,
                year=year,
                services=services,
                settings=settings,
                flow=flow,
            )

        case MenuStep.SUB_SEASON:
            selection = selected_year_season(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season = selection
            await show_or_stale(
                show_menu=_show_fetch_universe_menu,
                message=message,
                state=state,
                year=year,
                season=season,
                services=services,
                settings=settings,
                flow=flow,
            )

        case MenuStep.SCOPE:
            selection = selected_year_season_universe(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season, universe = selection
            clip_group = ClipGroup(year=year, season=season, universe=universe)

            sub_groups = await _fetch_sub_groups(
                services=services,
                clip_group=clip_group,
            )
            if sub_groups is None:
                await handle_stale_selection(message=message, state=state)
                return

            if available_sub_seasons(sub_groups) == [SubSeason.NONE]:
                await show_or_stale(
                    show_menu=_show_fetch_universe_menu,
                    message=message,
                    state=state,
                    year=year,
                    season=season,
                    services=services,
                    settings=settings,
                    flow=flow,
                )
                return

            await show_or_stale(
                show_menu=_show_fetch_sub_season_menu,
                message=message,
                state=state,
                clip_group=clip_group,
                services=services,
                settings=settings,
                sub_groups=sub_groups,
                flow=flow,
            )


async def _on_fetch_select(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    bot: Bot,
    callback_data: FetchCallbackData,
    flow: FlowMenuDefinition,
) -> None:
    data = await state.get_data()

    match callback_data.step:
        case MenuStep.YEAR:
            year = parse_year(callback_data.value)
            if year is None:
                await handle_stale_selection(message=message, state=state)
                return
            await show_or_stale(
                show_menu=_show_fetch_season_menu,
                message=message,
                state=state,
                year=year,
                services=services,
                settings=settings,
                flow=flow,
            )

        case MenuStep.SEASON:
            year = selected_year(data)
            season = parse_season(callback_data.value)
            if year is None or season is None:
                await handle_stale_selection(message=message, state=state)
                return
            await show_or_stale(
                show_menu=_show_fetch_universe_menu,
                message=message,
                state=state,
                year=year,
                season=season,
                services=services,
                settings=settings,
                flow=flow,
            )

        case MenuStep.UNIVERSE:
            selection = selected_year_season(data)
            universe = parse_universe(callback_data.value)
            if selection is None or universe is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season = selection
            clip_group = ClipGroup(year=year, season=season, universe=universe)
            await show_or_stale(
                show_menu=_show_fetch_sub_season_menu,
                message=message,
                state=state,
                clip_group=clip_group,
                services=services,
                settings=settings,
                flow=flow,
            )

        case MenuStep.SUB_SEASON:
            selection = selected_year_season_universe(data)
            sub_season = parse_sub_season(callback_data.value)
            if selection is None or not isinstance(sub_season, SubSeason):
                await handle_stale_selection(message=message, state=state)
                return
            year, season, universe = selection
            clip_group = ClipGroup(year=year, season=season, universe=universe)
            await show_or_stale(
                show_menu=_show_fetch_scope_menu,
                message=message,
                state=state,
                clip_group=clip_group,
                sub_season=sub_season,
                services=services,
                settings=settings,
                flow=flow,
            )

        case MenuStep.SCOPE:
            selection = selected_year_season_universe_sub_season(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season, universe, sub_season = selection
            clip_group = ClipGroup(year=year, season=season, universe=universe)

            sub_groups = await _fetch_sub_groups(
                services=services,
                clip_group=clip_group,
            )
            if sub_groups is None:
                await handle_stale_selection(message=message, state=state)
                return

            scopes = available_scopes(sub_groups, sub_season)
            if not scopes:
                await handle_stale_selection(message=message, state=state)
                return

            if callback_data.value == ALL_SCOPES_CALLBACK_VALUE:
                selected_labels = flow_selection_labels(
                    flow,
                    year=year,
                    season=season,
                    universe=universe,
                    sub_season=sub_season,
                    scope='All',
                )
            else:
                scope = parse_scope(callback_data.value)
                if scope is None or scope not in scopes:
                    await handle_stale_selection(message=message, state=state)
                    return
                scopes = [scope]
                selected_labels = flow_selection_labels(
                    flow,
                    year=year,
                    season=season,
                    universe=universe,
                    sub_season=sub_season,
                    scope=scope,
                )

            await message.edit_text(
                **selection_text(selected=selected_labels),
                reply_markup=None,
            )
            try:
                await _send_fetch_scopes(
                    bot=bot,
                    chat_id=message.chat.id,
                    services=services,
                    clip_group=clip_group,
                    sub_season=sub_season,
                    scopes=scopes,
                    settings=settings,
                    normalize_audio=_normalizes_audio(flow),
                )
            except ClipGroupNotFoundError:
                await handle_stale_selection(message=message, state=state)
                return
            await state.clear()


async def _show_fetch_year_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    flow: FlowMenuDefinition = _FETCH_FLOW,
    services: Services | None = None,
    groups: list[ClipGroup] | None = None,
) -> bool:
    if groups is None:
        if services is None:
            return False
        groups = await services.clip_store.list_groups()

    available_years = available_group_years(groups)
    if not available_years:
        return False
    year_universe = year_option_universe(current_year=date.today().year, min_year=settings.min_clip_year)

    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.YEAR,
        prompt='Select year:',
        option_universe=list(reversed(year_universe)),
        available_options=available_years,
        option_value=str,
        option_text=str,
    )
    return True


async def _show_fetch_season_menu(
    *,
    message: Message,
    state: FSMContext,
    year: int,
    services: Services,
    settings: Settings,
    flow: FlowMenuDefinition = _FETCH_FLOW,
) -> bool:
    groups = await services.clip_store.list_groups()
    available_seasons = available_group_seasons(groups, year=year)
    allowed_seasons = store_allowed_seasons(year=year, today=date.today())
    available_seasons = [season for season in available_seasons if season in allowed_seasons]
    if not available_seasons:
        return False

    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.SEASON,
        prompt='Select season:',
        year=year,
        option_universe=list(Season),
        available_options=available_seasons,
        option_value=lambda season: str(int(season)),
        option_text=lambda season: str(int(season)),
    )
    return True


async def _show_fetch_universe_menu(
    *,
    message: Message,
    state: FSMContext,
    year: int,
    season: Season,
    services: Services,
    settings: Settings,
    flow: FlowMenuDefinition = _FETCH_FLOW,
) -> bool:
    groups = await services.clip_store.list_groups()
    available_universes = available_group_universes(groups, year=year, season=season)
    if not available_universes:
        return False

    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.UNIVERSE,
        prompt='Select universe:',
        year=year,
        season=season,
        option_universe=tuple(Universe),
        available_options=available_universes,
        option_value=lambda universe: universe.value,
        option_text=lambda universe: universe.value.title(),
    )
    return True


async def _show_fetch_sub_season_menu(
    *,
    message: Message,
    state: FSMContext,
    clip_group: ClipGroup,
    services: Services,
    settings: Settings,
    flow: FlowMenuDefinition = _FETCH_FLOW,
    sub_groups: list[ClipSubGroup] | None = None,
) -> bool:
    if sub_groups is None:
        sub_groups = await _fetch_sub_groups(
            services=services,
            clip_group=clip_group,
        )
    if sub_groups is None:
        return False

    sub_seasons = available_sub_seasons(sub_groups)
    if sub_seasons == [SubSeason.NONE]:
        return await _show_fetch_scope_menu(
            message=message,
            state=state,
            clip_group=clip_group,
            sub_season=SubSeason.NONE,
            services=services,
            settings=settings,
            sub_groups=sub_groups,
            flow=flow,
        )
    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.SUB_SEASON,
        prompt='Select sub-season:',
        year=clip_group.year,
        season=clip_group.season,
        universe=clip_group.universe,
        option_universe=tuple(SubSeason),
        available_options=sub_seasons,
        option_value=encode_sub_season,
        option_text=lambda sub_season: sub_season.value.title(),
    )
    return True


async def _show_fetch_scope_menu(
    *,
    message: Message,
    state: FSMContext,
    clip_group: ClipGroup,
    sub_season: SubSeason,
    services: Services,
    settings: Settings,
    flow: FlowMenuDefinition = _FETCH_FLOW,
    sub_groups: list[ClipSubGroup] | None = None,
) -> bool:
    if sub_groups is None:
        sub_groups = await _fetch_sub_groups(
            services=services,
            clip_group=clip_group,
        )
    if sub_groups is None:
        return False

    scopes = available_scopes(sub_groups, sub_season)
    if not scopes:
        return False

    available_scope_options: list[Scope | str] = [ALL_SCOPES_CALLBACK_VALUE, *scopes]

    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.SCOPE,
        prompt='Select scope:',
        year=clip_group.year,
        season=clip_group.season,
        universe=clip_group.universe,
        sub_season=sub_season,
        option_universe=(ALL_SCOPES_CALLBACK_VALUE, *Scope),
        available_options=available_scope_options,
        option_value=scope_option_callback_value,
        option_text=scope_option_text,
    )
    return True


async def _send_fetch_scopes(
    *,
    bot: Bot,
    chat_id: ChatId,
    services: Services,
    clip_group: ClipGroup,
    sub_season: SubSeason,
    scopes: Sequence[Scope],
    settings: Settings,
    normalize_audio: bool,
) -> None:
    for index, scope in enumerate(scopes):
        if index > 0:
            await bot.send_message(chat_id=chat_id, text='.')

        async for batch in services.clip_store.fetch(
            clip_group=clip_group,
            clip_sub_group=ClipSubGroup(sub_season=sub_season, scope=scope),
        ):
            clips = batch
            if normalize_audio:
                clips = await _normalize_clip_batch(clips=clips, settings=settings)
            await _send_stored_clip_batch(bot=bot, chat_id=chat_id, clips=clips)

    await bot.send_message(chat_id=chat_id, text='Done')


async def _send_stored_clip_batch(
    *,
    bot: Bot,
    chat_id: ChatId,
    clips: Sequence[Clip],
) -> None:
    if not clips:
        raise ValueError('`clips` must not be empty')

    if len(clips) == 1:
        clip = clips[0]
        await bot.send_video(
            chat_id=chat_id,
            video=BufferedInputFile(clip.bytes, filename=clip.filename),
        )
        return

    await bot.send_media_group(
        chat_id=chat_id,
        media=[
            InputMediaVideo(
                media=BufferedInputFile(clip.bytes, filename=clip.filename),
            )
            for clip in clips
        ],
    )


async def _normalize_clip_batch(
    *,
    clips: Sequence[Clip],
    settings: Settings,
) -> list[Clip]:
    """Normalize fetched clip audio in memory before sending.

    Preserves clip order inside the fetched batch and never mutates stored
    clip objects in S3.
    """
    normalized_clips: list[Clip] = []
    for clip in clips:
        normalized_clips.append(
            Clip(
                filename=clip.filename,
                bytes=await normalize_audio_loudness(
                    clip.bytes,
                    loudness=settings.normalization_loudness,
                    bitrate=settings.normalization_bitrate,
                ),
            )
        )
    return normalized_clips


async def _fetch_sub_groups(
    *,
    services: Services,
    clip_group: ClipGroup,
) -> list[ClipSubGroup] | None:
    try:
        return await services.clip_store.list_sub_groups(clip_group)
    except ClipGroupNotFoundError:
        return None


def _fetch_entry_reply_markup():
    return stacked_keyboard(
        buttons=[
            InlineKeyboardButton(
                text='Fetch',
                callback_data=FetchEntryCallbackData(action=FetchEntryAction.FETCH).pack(),
            ),
            InlineKeyboardButton(
                text='Fetch raw',
                callback_data=FetchEntryCallbackData(action=FetchEntryAction.FETCH_RAW).pack(),
            ),
            InlineKeyboardButton(
                text='Cancel',
                callback_data=FetchEntryCallbackData(action=FetchEntryAction.CANCEL).pack(),
            ),
        ]
    )


async def _show_fetch_entry_menu(*, message: Message, state: FSMContext, settings: Settings) -> None:
    await state.clear()
    await message.edit_text(
        **width_reserved_text(
            text='Select action:',
            message_width=settings.message_width,
        ),
        reply_markup=_fetch_entry_reply_markup(),
    )


def _flow_for_entry_action(action: FetchEntryAction) -> FlowMenuDefinition | None:
    match action:
        case FetchEntryAction.FETCH:
            return _FETCH_FLOW
        case FetchEntryAction.FETCH_RAW:
            return _FETCH_RAW_FLOW
        case FetchEntryAction.CANCEL:
            return None


def _flow_for_mode(mode: object) -> FlowMenuDefinition | None:
    if mode == _FETCH_FLOW.mode:
        return _FETCH_FLOW
    if mode == _FETCH_RAW_FLOW.mode:
        return _FETCH_RAW_FLOW
    return None


def _normalizes_audio(flow: FlowMenuDefinition) -> bool:
    return flow is _FETCH_FLOW
