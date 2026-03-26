import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum, auto
from typing import Any

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaVideo, Message
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
    back_button,
    callback_message,
    create_padding_line,
    download_video_bytes,
    dummy_button,
    format_store_summary,
    handle_stale_selection,
    parse_scope,
    parse_season,
    parse_sub_season,
    parse_universe,
    parse_year,
    selected_text,
    selection_keyboard,
    selection_labels,
    selection_text,
    set_flow_context,
    validate_flow_state,
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
_BUFFER_VERSION_KEY = 'buffer_version'
_REORDER_FLOW_MODE = 'reorder'
_REORDER_MAX_CLIPS = 16
_REORDER_SELECTION_PROMPT = 'Select new order:'
_REORDER_RESET_CALLBACK_VALUE = 'reset'
type IntakeShowMenu = Callable[..., Awaitable[bool]]


class IntakeAction(StrEnum):
    CANCEL = auto()
    REORDER = auto()
    RECONCILE = auto()
    ROUTE = auto()
    STORE = auto()


class IntakeActionCallbackData(CallbackData, prefix='clip_action'):
    action: IntakeAction


class IntakeCallbackData(CallbackData, prefix='clip_intake'):
    action: MenuAction
    step: MenuStep
    value: str


class ReorderClipFlow(StatesGroup):
    selecting = State()


class ReorderCallbackData(CallbackData, prefix='clip_reorder'):
    action: MenuAction
    value: str


@dataclass(slots=True)
class _RouteBatch:
    clip_group: ClipGroup
    messages: list[Message]


@dataclass(slots=True)
class _RouteResult:
    selection_groups: list[ClipGroup]
    store_result: StoreResult
    compact_groups: list[ClipGroup]
    error_text: str | None = None


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

        case IntakeAction.REORDER:
            video_messages = _buffered_video_messages(services.chat_message_buffer.peek_grouped(message.chat.id))
            if not video_messages:
                await state.clear()
                await message.edit_text('Selection is no longer available', reply_markup=None)
                return
            if (error_text := _reorder_validation_error(len(video_messages))) is not None:
                await state.clear()
                # Invalid clip counts are treated as a hard rejection rather than
                # a valid interactive flow. We intentionally flush here to keep
                # the UI stateless and require the user to resend clips.
                services.chat_message_buffer.flush_grouped(message.chat.id)
                await message.edit_text(error_text, reply_markup=None)
                return

            await _show_reorder_selection_menu(
                message=message,
                state=state,
                settings=settings,
                total_clips=len(video_messages),
                buffer_version=services.chat_message_buffer.version(message.chat.id),
            )

        case IntakeAction.RECONCILE:
            if _has_pending_reconcile_videos(
                services=services,
                chat_id=message.chat.id,
            ):
                try:
                    filename_batches = _pending_reconcile_filename_batches(
                        services=services,
                        chat_id=message.chat.id,
                    )
                except ValueError:
                    await message.answer("Can't reconcile not stored")
                    return

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

                buffer_version = services.chat_message_buffer.version(message.chat.id)
                await _show_reconcile_sub_season_menu(
                    message=message,
                    state=state,
                    settings=settings,
                    clip_group=clip_group,
                    filename_batches=filename_batches,
                    buffer_version=buffer_version,
                )
                return

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

            await handle_stale_selection(message=message, state=state)

        case IntakeAction.STORE:
            await _show_intake_menu_or_stale(
                show_menu=_show_store_year_menu,
                message=message,
                state=state,
                buffer_version=services.chat_message_buffer.version(message.chat.id),
                settings=settings,
                flow=_STORE_FLOW,
            )

        case IntakeAction.ROUTE:
            await state.clear()
            # Route is a single-shot action: flush at entry, validate after flush,
            # never restore the buffer on failure. This is intentional to keep the
            # UI stateless and simple; users must resend clips if validation fails.
            route_batches, error_text = _plan_route_batches(
                services.chat_message_buffer.flush_grouped(message.chat.id),
                settings=settings,
            )
            if error_text is not None:
                await message.edit_text(error_text, reply_markup=None)
                return
            if not route_batches:
                await message.edit_text('No clips received', reply_markup=None)
                return

            route_result = await _store_route_batches(
                bot=bot,
                services=services,
                route_batches=route_batches,
            )
            await message.edit_text(
                **_route_selection_kwargs(route_result.selection_groups),
                reply_markup=None,
            )
            await message.answer(**_store_summary_kwargs(route_result.store_result))

            for clip_group in route_result.compact_groups:
                try:
                    await services.clip_store.compact(
                        clip_group=clip_group,
                        clip_sub_group=ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
                        batch_size=_TELEGRAM_MEDIA_GROUP_LIMIT,
                    )
                except Exception:
                    logger.exception(
                        'Post-store clip compaction failed for {} {}',
                        clip_group,
                        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE),
                    )
                    raise


@router.callback_query(
    ReorderCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_reorder_menu(
    callback: CallbackQuery,
    callback_data: ReorderCallbackData,
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

    if not await validate_flow_state(
        message=message,
        state=state,
        expected_mode=_REORDER_FLOW_MODE,
        expected_state=ReorderClipFlow.selecting,
    ):
        return

    if callback_data.action is MenuAction.BACK:
        if callback_data.value == 'back':
            await _show_intake_action_menu(
                message=message,
                state=state,
                services=services,
                settings=settings,
            )
            return

        data = await state.get_data()
        selected_order = _reorder_selected_order_from_state(data)
        total_clips = _reorder_total_clips_from_state(data)
        if (
            callback_data.value != _REORDER_RESET_CALLBACK_VALUE
            or selected_order is None
            or total_clips is None
            or not selected_order
        ):
            await handle_stale_selection(message=message, state=state)
            return

        if not _is_intake_buffer_state_valid(
            data=data,
            services=services,
            chat_id=message.chat.id,
        ):
            await handle_stale_selection(message=message, state=state)
            return

        await state.update_data(selected_order=[])
        await message.edit_text(
            **_reorder_selection_kwargs(
                selected_order=[],
                message_width=settings.message_width,
            ),
            reply_markup=_reorder_selection_keyboard(
                total_clips=total_clips,
                selected_order=[],
            ),
        )
        return

    index = _parse_reorder_index(callback_data.value)
    data = await state.get_data()
    selected_order = _reorder_selected_order_from_state(data)
    total_clips = _reorder_total_clips_from_state(data)
    if index is None or selected_order is None or total_clips is None:
        await handle_stale_selection(message=message, state=state)
        return

    if not _is_intake_buffer_state_valid(
        data=data,
        services=services,
        chat_id=message.chat.id,
    ):
        await handle_stale_selection(message=message, state=state)
        return

    if index < 1 or index > total_clips:
        await handle_stale_selection(message=message, state=state)
        return

    if index in selected_order:
        return

    updated_order = [*selected_order, index]
    if len(updated_order) == total_clips:
        await message.edit_text(
            **_reorder_final_kwargs(updated_order),
            reply_markup=None,
        )
        reordered_messages = _reordered_video_messages(
            _buffered_video_messages(services.chat_message_buffer.flush_grouped(message.chat.id)),
            selected_order=updated_order,
            total_clips=total_clips,
        )
        await _send_reordered_video_messages(
            bot=bot,
            chat_id=message.chat.id,
            messages=reordered_messages,
        )
        await state.clear()
        return

    await state.update_data(selected_order=updated_order)
    await message.edit_text(
        **_reorder_selection_kwargs(
            selected_order=updated_order,
            message_width=settings.message_width,
        ),
        reply_markup=_reorder_selection_keyboard(
            total_clips=total_clips,
            selected_order=updated_order,
        ),
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

    if not _is_intake_buffer_state_valid(
        data=data,
        services=services,
        chat_id=message.chat.id,
    ):
        await handle_stale_selection(message=message, state=state)
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            await _show_intake_menu_or_stale(
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
            services.chat_message_buffer.flush(message.chat.id)
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
    buffer_version: int | None = None,
) -> None:
    if not await _show_intake_menu_or_stale(
        show_menu=_show_store_sub_season_menu,
        message=message,
        state=state,
        buffer_version=buffer_version,
        settings=settings,
        clip_group=clip_group,
        flow=_RECONCILE_FLOW,
    ):
        return
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
    if not await _show_intake_menu_or_stale(
        show_menu=_show_store_scope_menu,
        message=message,
        state=state,
        settings=settings,
        clip_group=clip_group,
        sub_season=sub_season,
        flow=_RECONCILE_FLOW,
    ):
        return
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


def _plan_route_batches(
    message_groups: Sequence[MessageGroup],
    *,
    settings: Settings,
) -> tuple[list[_RouteBatch], str | None]:
    batches: list[_RouteBatch] = []
    current_route: ClipGroup | None = None
    today = date.today()
    allowed_years = set(
        year_option_universe(
            current_year=today.year,
            min_year=settings.min_clip_year,
        )
    )

    for message_group in message_groups:
        for message in message_group:
            # Route intentionally operates on video-only input. Mixed-media albums
            # are not supported here, so non-video messages never provide route
            # context and are skipped on purpose.
            if message.video is None:
                continue

            caption = message.caption
            if caption is None:
                if current_route is None:
                    return [], 'Missing route text'
                next_route = current_route
            else:
                next_route = parse_route_text(caption)
                if next_route is None:
                    return [], 'Invalid route text'
                if next_route.year not in allowed_years:
                    return [], 'Invalid route text'
                if next_route.season not in store_allowed_seasons(year=next_route.year, today=today):
                    return [], 'Invalid route text'

            if not batches or batches[-1].clip_group != next_route:
                batches.append(_RouteBatch(clip_group=next_route, messages=[message]))
            else:
                batches[-1].messages.append(message)
            current_route = next_route

    return batches, None


async def _store_route_batches(
    *,
    bot: Bot,
    services: Services,
    route_batches: Sequence[_RouteBatch],
) -> _RouteResult:
    result = StoreResult(stored_count=0, duplicate_count=0)
    compact_groups: list[ClipGroup] = []
    compact_group_set: set[ClipGroup] = set()
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.SOURCE)

    for route_batch in route_batches:
        batch_result = await services.clip_store.store(
            await _video_messages_to_clips(bot=bot, messages=route_batch.messages),
            clip_group=route_batch.clip_group,
            clip_sub_group=clip_sub_group,
        )
        result += batch_result
        if batch_result.stored_count > 0 and route_batch.clip_group not in compact_group_set:
            compact_groups.append(route_batch.clip_group)
            compact_group_set.add(route_batch.clip_group)

    return _RouteResult(
        selection_groups=[route_batch.clip_group for route_batch in route_batches],
        store_result=result,
        compact_groups=compact_groups,
    )


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


async def _video_messages_to_clips(
    *,
    bot: Bot,
    messages: Sequence[Message],
) -> list[Clip]:
    async def to_clip(message: Message) -> Clip:
        if message.video is None:
            raise ValueError('Route batches must contain only video messages')
        return Clip(
            filename=_telegram_clip_filename(message),
            bytes=await download_video_bytes(bot, file_id=message.video.file_id),
        )

    # This personal bot assumes route segments stay practically small
    # (roughly tens of clips), so downloading a whole segment at once is fine.
    # `gather()` preserves input order, which keeps the stored clip order aligned
    # with the original buffered message order.
    return list(await asyncio.gather(*(to_clip(message) for message in messages)))


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


def _pending_reconcile_filename_batches(
    *,
    services: Services,
    chat_id: ChatId,
) -> list[list[str]]:
    return _message_groups_to_filenames(services.chat_message_buffer.peek_grouped(chat_id))


def _has_pending_reconcile_videos(
    *,
    services: Services,
    chat_id: ChatId,
) -> bool:
    return any(message.video is not None for message in services.chat_message_buffer.peek(chat_id))


def _store_year_options(*, current_year: int, min_year: int) -> list[int]:
    return year_option_universe(current_year=current_year, min_year=min_year)


def _store_season_options(*, year: int, today: date) -> list[Season]:
    return store_allowed_seasons(year=year, today=today)


def parse_route_text(text: str) -> ClipGroup | None:
    normalized = text.strip()
    if len(normalized) != 4:
        return None

    year_suffix = normalized[:2]
    season_text = normalized[2]
    universe_text = normalized[3].lower()

    if not year_suffix.isdigit() or not season_text.isdigit():
        return None

    try:
        season = Season(int(season_text))
    except ValueError:
        return None

    if universe_text == 'w':
        universe = Universe.WEST
    elif universe_text == 'e':
        universe = Universe.EAST
    else:
        return None

    return ClipGroup(year=2000 + int(year_suffix), season=season, universe=universe)


def _telegram_clip_filename(message: Message) -> str:
    if message.video is not None and message.video.file_name:
        return message.video.file_name
    return f'telegram-{message.chat.id}-{message.message_id}.mp4'


def _buffered_video_messages(message_groups: Sequence[MessageGroup]) -> list[Message]:
    # Reorder intentionally uses this same video-only flattening for both
    # peek-time validation and final flush-time execution so non-video messages
    # are ignored with identical semantics in both phases.
    return [message for message_group in message_groups for message in message_group if message.video is not None]


def _reorder_validation_error(total_clips: int) -> str | None:
    if total_clips == 1:
        return 'Unexpected number of clips'
    if total_clips > _REORDER_MAX_CLIPS:
        return 'Too many clips'
    return None


def _reorder_selection_keyboard(
    *,
    total_clips: int,
    selected_order: Sequence[int],
) -> InlineKeyboardMarkup:
    buttons = [
        _create_reorder_select_button(
            index=index,
            selected=index in set(selected_order),
        )
        for index in range(1, total_clips + 1)
    ]
    top_row: list[InlineKeyboardButton] = []
    middle_row: list[InlineKeyboardButton] = []
    for index, button in enumerate(reversed(buttons)):
        if index % 2 == 0:
            top_row.append(button)
        else:
            middle_row.append(button)
    if total_clips % 2 != 0:
        middle_row.insert(0, dummy_button())

    return InlineKeyboardMarkup(
        inline_keyboard=[
            top_row,
            middle_row,
            [_reorder_navigation_button(selected_order=selected_order)],
        ]
    )


def _reorder_navigation_button(*, selected_order: Sequence[int]) -> InlineKeyboardButton:
    if not selected_order:
        return back_button(
            callback_data=ReorderCallbackData(
                action=MenuAction.BACK,
                value='back',
            ).pack()
        )
    return InlineKeyboardButton(
        text='Reset',
        callback_data=ReorderCallbackData(
            action=MenuAction.BACK,
            value=_REORDER_RESET_CALLBACK_VALUE,
        ).pack(),
    )


def _create_reorder_select_button(*, index: int, selected: bool) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=str(index),
        style='primary' if selected else None,
        callback_data=ReorderCallbackData(
            action=MenuAction.SELECT,
            value=str(index),
        ).pack(),
    )


def _reorder_selected_content(selected_order: Sequence[int]) -> Text:
    parts: list[object] = ['Selected: ', Bold('Reorder')]
    if selected_order:
        parts.extend([' -> '])
        for index, value in enumerate(selected_order):
            if index > 0:
                parts.append(' ')
            parts.append(Bold(str(value)))
    return Text(*parts)


def _reorder_selection_kwargs(
    *,
    selected_order: Sequence[int],
    message_width: int,
) -> dict[str, Any]:
    return Text(
        _reorder_selected_content(selected_order),
        '\n',
        create_padding_line(message_width),
        '\n',
        _REORDER_SELECTION_PROMPT,
    ).as_kwargs()


def _reorder_final_kwargs(selected_order: Sequence[int]) -> dict[str, Any]:
    return _reorder_selected_content(selected_order).as_kwargs()


async def _show_reorder_selection_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
    total_clips: int,
    buffer_version: int,
) -> None:
    await set_flow_context(
        state=state,
        mode=_REORDER_FLOW_MODE,
        menu_message_id=message.message_id,
        fsm_state=ReorderClipFlow.selecting,
    )
    await state.update_data(
        selected_order=[],
        total_clips=total_clips,
        buffer_version=buffer_version,
    )
    await message.edit_text(
        **_reorder_selection_kwargs(
            selected_order=[],
            message_width=settings.message_width,
        ),
        reply_markup=_reorder_selection_keyboard(
            total_clips=total_clips,
            selected_order=[],
        ),
    )


def _reorder_selected_order_from_state(data: dict[str, object]) -> list[int] | None:
    raw_selected_order = data.get('selected_order')
    if not isinstance(raw_selected_order, list):
        return None
    selected_order: list[int] = []
    for value in raw_selected_order:
        if not isinstance(value, int):
            return None
        selected_order.append(value)
    return selected_order


def _reorder_total_clips_from_state(data: dict[str, object]) -> int | None:
    total_clips = data.get('total_clips')
    if isinstance(total_clips, int):
        return total_clips
    return None


def _parse_reorder_index(value: str) -> int | None:
    if not value.isdigit():
        return None
    return int(value)


def _reordered_video_messages(
    video_messages: Sequence[Message],
    *,
    selected_order: Sequence[int],
    total_clips: int,
) -> list[Message]:
    # Fail fast if the flushed video set no longer matches the validated entry
    # count; interactive reorder should never execute against drifted input.
    if len(video_messages) != total_clips:
        raise RuntimeError('Reorder buffer changed unexpectedly before completion')
    return [video_messages[index - 1] for index in selected_order]


async def _send_reordered_video_messages(
    *,
    bot: Bot,
    chat_id: ChatId,
    messages: Sequence[Message],
) -> None:
    if not messages:
        raise ValueError('`messages` must not be empty')

    for start in range(0, len(messages), _TELEGRAM_MEDIA_GROUP_LIMIT):
        batch = messages[start : start + _TELEGRAM_MEDIA_GROUP_LIMIT]
        if len(batch) == 1:
            await bot.send_video(
                chat_id=chat_id,
                video=_video_file_id(batch[0]),
            )
            continue
        await bot.send_media_group(
            chat_id=chat_id,
            media=[InputMediaVideo(media=_video_file_id(message)) for message in batch],
        )


def _video_file_id(message: Message) -> str:
    if message.video is None:
        raise ValueError('Reorder can resend only video messages')
    return message.video.file_id


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
        'reply_markup': selection_keyboard(
            buttons=[
                _create_intake_action_button(IntakeAction.REORDER),
                _create_intake_action_button(IntakeAction.STORE),
                _create_intake_action_button(IntakeAction.ROUTE),
                _create_intake_action_button(IntakeAction.RECONCILE),
            ],
            back_button=_create_intake_action_button(IntakeAction.CANCEL),
        ),
    }


def _route_selection_kwargs(route_groups: Sequence[ClipGroup]) -> dict[str, Any]:
    parts: list[object] = []

    for index, clip_group in enumerate(route_groups):
        if index > 0:
            parts.append('\n')

        line_parts: list[object] = ['Selected: ', Bold('Route')]
        for value in selection_labels(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
            scope=Scope.SOURCE,
        ):
            line_parts.extend([' → ', Bold(value)])
        parts.append(Text(*line_parts))

    return Text(*parts).as_kwargs()


def _create_intake_action_button(action: IntakeAction) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=action.title(),
        callback_data=IntakeActionCallbackData(action=action).pack(),
    )


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


def _is_intake_buffer_state_valid(
    *,
    data: dict[str, object],
    services: Services,
    chat_id: ChatId,
) -> bool:
    buffer_version = _buffer_version_from_state(data)
    if buffer_version is None:
        return False
    return buffer_version == services.chat_message_buffer.version(chat_id)


async def _intake_buffer_version_for_menu(
    *,
    state: FSMContext,
    buffer_version: int | None,
) -> int | None:
    if buffer_version is not None:
        return buffer_version
    return _buffer_version_from_state(await state.get_data())


async def _store_buffer_version(
    *,
    state: FSMContext,
    buffer_version: int | None,
) -> None:
    if buffer_version is None:
        return
    await state.update_data(buffer_version=buffer_version)


async def _show_intake_menu_or_stale(
    *,
    show_menu: IntakeShowMenu,
    message: Message,
    state: FSMContext,
    buffer_version: int | None = None,
    **kwargs: object,
) -> bool:
    resolved_buffer_version = await _intake_buffer_version_for_menu(
        state=state,
        buffer_version=buffer_version,
    )
    if not await show_menu(
        message=message,
        state=state,
        **kwargs,
    ):
        await handle_stale_selection(message=message, state=state)
        return False
    await _store_buffer_version(state=state, buffer_version=resolved_buffer_version)
    return True


def _buffer_version_from_state(data: dict[str, object]) -> int | None:
    buffer_version = data.get(_BUFFER_VERSION_KEY)
    if isinstance(buffer_version, int):
        return buffer_version
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
