from datetime import date
from enum import StrEnum, auto
from typing import Any

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.formatting import Bold, Text
from loguru import logger

from general_bot.handlers.clips.common import (
    ALL_SCOPES_CALLBACK_VALUE,
    FLOW_RECONCILE,
    FLOW_STORE,
    RECONCILE_STATE_BY_STEP,
    STORE_STATE_BY_STEP,
    MenuAction,
    MenuStep,
    callback_message,
    create_padding_line,
    download_video_bytes,
    format_store_summary,
    handle_stale_selection,
    parse_scope,
    parse_season,
    parse_sub_season,
    parse_universe,
    parse_year,
    selected_text,
    selection_text,
    stacked_keyboard,
)
from general_bot.handlers.clips.flow import (
    FlowMenuDefinition,
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
from general_bot.services.message_buffer import MessageGroup
from general_bot.settings import Settings
from general_bot.types import ChatId

router = Router()
_TELEGRAM_MEDIA_GROUP_LIMIT = 10


class IntakeAction(StrEnum):
    CANCEL = auto()
    RECONCILE = auto()
    STORE = auto()


class IntakeActionCallbackData(CallbackData, prefix='clip_action'):
    action: IntakeAction


class IntakeCallbackData(CallbackData, prefix='clip_intake'):
    action: MenuAction
    step: MenuStep
    value: str


def _pack_intake_menu_callback(action: MenuAction, step: MenuStep, value: str) -> str:
    return IntakeCallbackData(action=action, step=step, value=value).pack()


_STORE_FLOW = FlowMenuDefinition(
    mode=FLOW_STORE,
    flow_label='Store',
    state_by_step=STORE_STATE_BY_STEP,
    pack_callback=_pack_intake_menu_callback,
)

_RECONCILE_FLOW = FlowMenuDefinition(
    mode=FLOW_RECONCILE,
    flow_label='Reconcile',
    state_by_step=RECONCILE_STATE_BY_STEP,
    pack_callback=_pack_intake_menu_callback,
)


@router.message(F.chat.type == ChatType.PRIVATE)
async def on_buffered_clip_message(
    message: Message,
    services: Services,
    settings: Settings,
) -> None:
    chat_id = message.chat.id
    services.chat_message_buffer.append(message, chat_id=chat_id)

    async def send_clip_action_selection() -> None:
        kwargs = _intake_action_menu_kwargs(
            services=services,
            chat_id=chat_id,
            message_width=settings.message_width,
        )
        if kwargs is None:
            services.chat_message_buffer.flush(chat_id)
            await message.answer(text='No clips received')
            return
        await message.answer(**kwargs)

    services.task_scheduler.schedule(
        send_clip_action_selection,
        key=chat_id,
        delay=settings.forward_batch_timeout,
    )


@router.callback_query(
    IntakeActionCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_intake_action(
    callback: CallbackQuery,
    callback_data: IntakeActionCallbackData,
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

    match callback_data.action:
        case IntakeAction.CANCEL:
            await state.clear()
            await message.edit_text(
                **selected_text(selected='Cancel'),
                reply_markup=None,
            )
            services.chat_message_buffer.flush(message.chat.id)

        case IntakeAction.RECONCILE:
            stored_data = await state.get_data()
            stored_clip_group = _reconcile_clip_group_from_state(stored_data)
            stored_filename_batches = _reconcile_filename_batches_from_state(stored_data)
            if stored_clip_group is not None and stored_filename_batches is not None:
                await _show_reconcile_sub_season_menu(
                    message=message,
                    state=state,
                    settings=settings,
                    clip_group=stored_clip_group,
                    filename_batches=stored_filename_batches,
                )
                return

            filename_batches = _message_groups_to_filenames(services.chat_message_buffer.peek_grouped(message.chat.id))
            try:
                clip_group = await services.clip_store.derive_group(filename_batches)
            except DuplicateFilenamesError:
                await message.answer("Can't reconcile duplicates")
                return
            except InvalidFilenamesError, UnknownClipsError:
                await message.answer("Can't reconcile not stored")
                return
            except MixedClipGroupsError:
                await message.answer("Can't reconcile mixed groups")
                return

            services.chat_message_buffer.flush(message.chat.id)
            await _show_reconcile_sub_season_menu(
                message=message,
                state=state,
                settings=settings,
                clip_group=clip_group,
                filename_batches=filename_batches,
            )

        case IntakeAction.STORE:
            await _show_store_year_menu(
                message=message,
                state=state,
                settings=settings,
                flow=_STORE_FLOW,
            )


@router.callback_query(
    IntakeCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_intake_menu(
    callback: CallbackQuery,
    callback_data: IntakeCallbackData,
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
    flow = _selection_flow_for_mode(data.get('mode'))
    if flow is None:
        await handle_stale_selection(message=message, state=state)
        return

    if callback_data.step not in flow.state_by_step:
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
        await _on_store_back(
            message=message,
            state=state,
            services=services,
            settings=settings,
            step=callback_data.step,
            flow=flow,
        )
        return

    await _on_store_select(
        message=message,
        state=state,
        services=services,
        settings=settings,
        bot=bot,
        callback_data=callback_data,
        flow=flow,
    )


async def _on_store_back(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    step: MenuStep,
    flow: FlowMenuDefinition,
) -> None:
    data = await state.get_data()

    if flow is _RECONCILE_FLOW:
        match step:
            case MenuStep.SUB_SEASON:
                filename_batches = _reconcile_filename_batches_from_state(data)
                if filename_batches is None:
                    await handle_stale_selection(message=message, state=state)
                    return
                await _show_intake_action_menu(
                    message=message,
                    state=state,
                    services=services,
                    settings=settings,
                    clip_count_override=_filename_batch_clip_count(filename_batches),
                    preserve_state=True,
                )

            case MenuStep.SCOPE:
                clip_group = _reconcile_clip_group_from_state(data)
                filename_batches = _reconcile_filename_batches_from_state(data)
                if clip_group is None or filename_batches is None:
                    await handle_stale_selection(message=message, state=state)
                    return
                await _show_reconcile_sub_season_menu(
                    message=message,
                    state=state,
                    settings=settings,
                    clip_group=clip_group,
                    filename_batches=filename_batches,
                )
        return

    match step:
        case MenuStep.YEAR:
            await _show_intake_action_menu(
                message=message,
                state=state,
                services=services,
                settings=settings,
            )

        case MenuStep.SEASON:
            await show_or_stale(
                show_menu=_show_store_year_menu,
                message=message,
                state=state,
                settings=settings,
                flow=flow,
            )

        case MenuStep.UNIVERSE:
            year = selected_year(data)
            if year is None:
                await handle_stale_selection(message=message, state=state)
                return
            await show_or_stale(
                show_menu=_show_store_season_menu,
                message=message,
                state=state,
                settings=settings,
                year=year,
                flow=flow,
            )

        case MenuStep.SUB_SEASON:
            selection = selected_year_season(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season = selection
            await show_or_stale(
                show_menu=_show_store_universe_menu,
                message=message,
                state=state,
                settings=settings,
                year=year,
                season=season,
                flow=flow,
            )

        case MenuStep.SCOPE:
            selection = selected_year_season_universe(data)
            if selection is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season, universe = selection
            clip_group = ClipGroup(year=year, season=season, universe=universe)
            await show_or_stale(
                show_menu=_show_store_sub_season_menu,
                message=message,
                state=state,
                settings=settings,
                clip_group=clip_group,
                flow=flow,
            )


async def _on_store_select(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    bot: Bot,
    callback_data: IntakeCallbackData,
    flow: FlowMenuDefinition,
) -> None:
    data = await state.get_data()

    if flow is _RECONCILE_FLOW:
        await _on_reconcile_select(
            message=message,
            state=state,
            services=services,
            settings=settings,
            callback_data=callback_data,
        )
        return

    match callback_data.step:
        case MenuStep.YEAR:
            year = parse_year(callback_data.value)
            if year is None:
                await handle_stale_selection(message=message, state=state)
                return
            await show_or_stale(
                show_menu=_show_store_season_menu,
                message=message,
                state=state,
                settings=settings,
                year=year,
                flow=flow,
            )

        case MenuStep.SEASON:
            year = selected_year(data)
            season = parse_season(callback_data.value)
            if year is None or season is None:
                await handle_stale_selection(message=message, state=state)
                return
            await show_or_stale(
                show_menu=_show_store_universe_menu,
                message=message,
                state=state,
                settings=settings,
                year=year,
                season=season,
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
                show_menu=_show_store_sub_season_menu,
                message=message,
                state=state,
                settings=settings,
                clip_group=clip_group,
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
                show_menu=_show_store_scope_menu,
                message=message,
                state=state,
                settings=settings,
                clip_group=clip_group,
                sub_season=sub_season,
                flow=flow,
            )

        case MenuStep.SCOPE:
            selection = selected_year_season_universe_sub_season(data)
            scope = parse_scope(callback_data.value)
            if selection is None or scope is None:
                await handle_stale_selection(message=message, state=state)
                return
            year, season, universe, sub_season = selection
            clip_group = ClipGroup(year=year, season=season, universe=universe)
            clip_sub_group = ClipSubGroup(sub_season=sub_season, scope=scope)

            await message.edit_text(
                **selection_text(
                    selected=flow_selection_labels(
                        flow,
                        year=year,
                        season=season,
                        universe=universe,
                        sub_season=sub_season,
                        scope=scope,
                    )
                ),
                reply_markup=None,
            )
            await state.clear()

            if flow is _STORE_FLOW:
                result = await _store_buffered_clips(
                    bot=bot,
                    chat_id=message.chat.id,
                    services=services,
                    clip_group=clip_group,
                    clip_sub_group=clip_sub_group,
                )

                await message.answer(**_store_summary_kwargs(result))

                if result.stored_count > 0 and scope in {Scope.EXTRA, Scope.SOURCE}:
                    try:
                        await services.clip_store.compact(
                            clip_group=clip_group,
                            clip_sub_group=clip_sub_group,
                            batch_size=_TELEGRAM_MEDIA_GROUP_LIMIT,
                        )
                    except Exception:
                        logger.exception(
                            'Post-store clip compaction failed for {} {}',
                            clip_group,
                            clip_sub_group,
                        )
                        raise
                return


async def _on_reconcile_select(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    callback_data: IntakeCallbackData,
) -> None:
    data = await state.get_data()
    clip_group = _reconcile_clip_group_from_state(data)
    filename_batches = _reconcile_filename_batches_from_state(data)
    if clip_group is None or filename_batches is None:
        await handle_stale_selection(message=message, state=state)
        return

    match callback_data.step:
        case MenuStep.SUB_SEASON:
            sub_season = parse_sub_season(callback_data.value)
            if not isinstance(sub_season, SubSeason):
                await handle_stale_selection(message=message, state=state)
                return
            await _show_reconcile_scope_menu(
                message=message,
                state=state,
                settings=settings,
                clip_group=clip_group,
                sub_season=sub_season,
                filename_batches=filename_batches,
            )

        case MenuStep.SCOPE:
            sub_season = data.get('sub_season')
            scope = parse_scope(callback_data.value)
            if not isinstance(sub_season, SubSeason) or scope is None:
                await handle_stale_selection(message=message, state=state)
                return
            clip_sub_group = ClipSubGroup(sub_season=sub_season, scope=scope)

            await message.edit_text(
                **selection_text(
                    selected=flow_selection_labels(
                        _RECONCILE_FLOW,
                        year=clip_group.year,
                        season=clip_group.season,
                        universe=clip_group.universe,
                        sub_season=sub_season,
                        scope=scope,
                    )
                ),
                reply_markup=None,
            )
            await state.clear()

            result = await services.clip_store.reconcile(
                filename_batches,
                clip_group=clip_group,
                clip_sub_group=clip_sub_group,
            )
            await message.answer(**_reconcile_summary_kwargs(result))

        case _:
            await handle_stale_selection(message=message, state=state)


async def _show_store_year_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    flow: FlowMenuDefinition = _STORE_FLOW,
) -> bool:
    years = _store_year_options(current_year=date.today().year, min_year=settings.min_clip_year)
    if not years:
        return False

    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.YEAR,
        prompt='Select year:',
        option_universe=list(reversed(years)),
        available_options=years,
        option_value=str,
        option_text=str,
    )
    return True


async def _show_store_season_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    year: int,
    flow: FlowMenuDefinition = _STORE_FLOW,
) -> bool:
    if year not in _store_year_options(current_year=date.today().year, min_year=settings.min_clip_year):
        return False
    seasons = _store_season_options(year=year, today=date.today())

    await show_fixed_option_menu(
        flow=flow,
        message=message,
        state=state,
        message_width=settings.message_width,
        step=MenuStep.SEASON,
        prompt='Select season:',
        year=year,
        option_universe=list(Season),
        available_options=seasons,
        option_value=lambda season: str(int(season)),
        option_text=lambda season: str(int(season)),
    )
    return True


async def _show_store_universe_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    year: int,
    season: Season,
    flow: FlowMenuDefinition = _STORE_FLOW,
) -> bool:
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
        available_options=tuple(Universe),
        option_value=lambda universe: universe.value,
        option_text=lambda universe: universe.value.title(),
    )
    return True


async def _show_store_sub_season_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    clip_group: ClipGroup,
    flow: FlowMenuDefinition = _STORE_FLOW,
) -> bool:
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
        available_options=tuple(SubSeason),
        option_value=lambda sub_season: sub_season.value,
        option_text=lambda sub_season: sub_season.value.title(),
    )
    return True


async def _show_store_scope_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    clip_group: ClipGroup,
    sub_season: SubSeason,
    flow: FlowMenuDefinition = _STORE_FLOW,
) -> bool:
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
        available_options=tuple(Scope),
        option_value=scope_option_callback_value,
        option_text=scope_option_text,
    )
    return True


async def _show_reconcile_sub_season_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    clip_group: ClipGroup,
    filename_batches: list[list[str]],
) -> None:
    await _show_store_sub_season_menu(
        message=message,
        state=state,
        settings=settings,
        clip_group=clip_group,
        flow=_RECONCILE_FLOW,
    )
    await state.update_data(
        clip_group=clip_group,
        filename_batches=filename_batches,
    )


async def _show_reconcile_scope_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    clip_group: ClipGroup,
    sub_season: SubSeason,
    filename_batches: list[list[str]],
) -> None:
    await _show_store_scope_menu(
        message=message,
        state=state,
        settings=settings,
        clip_group=clip_group,
        sub_season=sub_season,
        flow=_RECONCILE_FLOW,
    )
    await state.update_data(
        clip_group=clip_group,
        filename_batches=filename_batches,
    )


async def _store_buffered_clips(
    *,
    bot: Bot,
    chat_id: ChatId,
    services: Services,
    clip_group: ClipGroup,
    clip_sub_group: ClipSubGroup,
) -> StoreResult:
    result = StoreResult(stored_count=0, duplicate_count=0)
    message_groups = services.chat_message_buffer.flush_grouped(chat_id)

    for message_group in message_groups:
        clips = await _message_group_to_clips(bot=bot, message_group=message_group)
        if not clips:
            continue
        result += await services.clip_store.store(
            clips,
            clip_group=clip_group,
            clip_sub_group=clip_sub_group,
        )

    return result


async def _message_group_to_clips(
    *,
    bot: Bot,
    message_group: MessageGroup,
) -> list[Clip]:
    clips: list[Clip] = []

    for message in message_group:
        if message.video is None:
            continue
        clips.append(
            Clip(
                filename=_telegram_clip_filename(message),
                bytes=await download_video_bytes(bot, file_id=message.video.file_id),
            )
        )

    return clips


def _message_group_to_filenames(message_group: MessageGroup) -> list[str]:
    filenames: list[str] = []
    for message in message_group:
        if message.video is None:
            continue
        if not message.video.file_name:
            raise ValueError('Reconcile requires every buffered video to have a filename')
        filenames.append(message.video.file_name)
    return filenames


def _message_groups_to_filenames(message_groups: list[MessageGroup]) -> list[list[str]]:
    return [filenames for message_group in message_groups if (filenames := _message_group_to_filenames(message_group))]


def _store_year_options(*, current_year: int, min_year: int) -> list[int]:
    return year_option_universe(current_year=current_year, min_year=min_year)


def _store_season_options(*, year: int, today: date) -> list[Season]:
    return store_allowed_seasons(year=year, today=today)


def _telegram_clip_filename(message: Message) -> str:
    if message.video is not None and message.video.file_name:
        return message.video.file_name
    return f'telegram-{message.chat.id}-{message.message_id}.mp4'


def _intake_action_menu_kwargs(
    *,
    services: Services,
    chat_id: ChatId,
    message_width: int,
    clip_count_override: int | None = None,
) -> dict[str, Any] | None:
    clip_count = clip_count_override
    if clip_count is None:
        clip_count = len(
            [message for message in services.chat_message_buffer.peek(chat_id) if message.video is not None]
        )
    if clip_count == 0:
        return None
    return {
        **Text(
            'Clips: ',
            Bold(str(clip_count)),
            '\n',
            create_padding_line(message_width),
            '\n',
            'Select action:',
        ).as_kwargs(),
        'reply_markup': stacked_keyboard(
            buttons=[
                _create_intake_action_button(IntakeAction.STORE),
                _create_intake_action_button(IntakeAction.RECONCILE),
                _create_intake_action_button(IntakeAction.CANCEL),
            ]
        ),
    }


async def _show_intake_action_menu(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    clip_count_override: int | None = None,
    preserve_state: bool = False,
) -> None:
    if preserve_state:
        await state.set_state(None)
    else:
        await state.clear()
    kwargs = _intake_action_menu_kwargs(
        services=services,
        chat_id=message.chat.id,
        message_width=settings.message_width,
        clip_count_override=clip_count_override,
    )
    if kwargs is None:
        await message.edit_text('No clips received', reply_markup=None)
        return
    await message.edit_text(**kwargs)


def _create_intake_action_button(action: IntakeAction) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=action.title(),
        callback_data=IntakeActionCallbackData(action=action).pack(),
    )


def _store_summary_kwargs(result: StoreResult) -> dict[str, Any]:
    summary = format_store_summary(result)
    if summary == 'Nothing changed':
        return {'text': summary}

    parts: list[object] = []
    for index, line in enumerate(summary.splitlines()):
        if index > 0:
            parts.append('\n')
        label, value = line.split(': ', maxsplit=1)
        parts.extend([f'{label}: ', Bold(value)])
    return Text(*parts).as_kwargs()


def _reconcile_summary_kwargs(result: ReconcileResult) -> dict[str, Any]:
    if result.updated == 0 and result.removed == 0:
        return {'text': 'Nothing changed'}

    parts: list[object] = []

    if result.updated > 0:
        parts.extend(['Updated: ', Bold(str(result.updated))])

    if result.removed > 0:
        if parts:
            parts.append('\n')
        parts.extend(['Removed: ', Bold(str(result.removed))])

    return Text(*parts).as_kwargs()


def _selection_flow_for_mode(mode: object) -> FlowMenuDefinition | None:
    if mode == _STORE_FLOW.mode:
        return _STORE_FLOW
    if mode == _RECONCILE_FLOW.mode:
        return _RECONCILE_FLOW
    return None


def _reconcile_clip_group_from_state(data: dict[str, object]) -> ClipGroup | None:
    clip_group = data.get('clip_group')
    if isinstance(clip_group, ClipGroup):
        return clip_group
    return None


def _reconcile_filename_batches_from_state(data: dict[str, object]) -> list[list[str]] | None:
    filename_batches = data.get('filename_batches')
    if not isinstance(filename_batches, list):
        return None

    normalized_batches: list[list[str]] = []
    for batch in filename_batches:
        if not isinstance(batch, list):
            return None
        normalized_batch: list[str] = []
        for filename in batch:
            if not isinstance(filename, str):
                return None
            normalized_batch.append(filename)
        normalized_batches.append(normalized_batch)

    return normalized_batches


def _filename_batch_clip_count(filename_batches: list[list[str]]) -> int:
    return sum(len(batch) for batch in filename_batches)
