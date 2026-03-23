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

from general_bot.handlers.clips_common import (
    ALL_SCOPES_CALLBACK_VALUE,
    FLOW_STORE,
    STORE_STATE_BY_STEP,
    MenuAction,
    MenuStep,
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
    selection_text,
    stacked_keyboard,
)
from general_bot.handlers.clips_flow import (
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
    Scope,
    Season,
    StoreResult,
    SubSeason,
    Universe,
)
from general_bot.services.container import Services
from general_bot.services.message_buffer import MessageGroup
from general_bot.settings import Settings
from general_bot.types import ChatId

router = Router()
_TELEGRAM_MEDIA_GROUP_LIMIT = 10


class ClipAction(StrEnum):
    CANCEL = auto()
    STORE = auto()


class ClipActionCallbackData(CallbackData, prefix='clip_action'):
    action: ClipAction


class StoreCallbackData(CallbackData, prefix='clip_store'):
    action: MenuAction
    step: MenuStep
    value: str


def _pack_store_menu_callback(action: MenuAction, step: MenuStep, value: str) -> str:
    return StoreCallbackData(action=action, step=step, value=value).pack()


_STORE_FLOW = FlowMenuDefinition(
    mode=FLOW_STORE,
    flow_label='Store',
    state_by_step=STORE_STATE_BY_STEP,
    pack_callback=_pack_store_menu_callback,
)


@router.message(F.chat.type == ChatType.PRIVATE)
async def on_message_buffer_and_schedule_clip_action_selection(
    message: Message,
    services: Services,
    settings: Settings,
) -> None:
    chat_id = message.chat.id
    services.chat_message_buffer.append(message, chat_id=chat_id)

    async def send_clip_action_selection() -> None:
        kwargs = _clip_action_menu_kwargs(
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
    ClipActionCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_clip_action(
    callback: CallbackQuery,
    callback_data: ClipActionCallbackData,
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
        case ClipAction.CANCEL:
            await state.clear()
            await message.edit_text(
                **selected_text(selected='Cancel'),
                reply_markup=None,
            )
            services.chat_message_buffer.flush(message.chat.id)
            await message.answer('Canceled')

        case ClipAction.STORE:
            await _show_store_year_menu(
                message=message,
                state=state,
                settings=settings,
            )


@router.callback_query(
    StoreCallbackData.filter(),
    F.message.chat.type == ChatType.PRIVATE,
)
async def on_store_menu(
    callback: CallbackQuery,
    callback_data: StoreCallbackData,
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

    if not await validate_menu_flow_state(
        message=message,
        state=state,
        flow=_STORE_FLOW,
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
        )
        return

    await _on_store_select(
        message=message,
        state=state,
        services=services,
        settings=settings,
        bot=bot,
        callback_data=callback_data,
    )


async def _on_store_back(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    step: MenuStep,
) -> None:
    data = await state.get_data()

    match step:
        case MenuStep.YEAR:
            await _show_clip_action_menu(
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
            )


async def _on_store_select(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
    bot: Bot,
    callback_data: StoreCallbackData,
) -> None:
    data = await state.get_data()

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
                        _STORE_FLOW,
                        year=year,
                        season=season,
                        universe=universe,
                        sub_season=sub_season,
                        scope=scope,
                    )
                ),
                reply_markup=None,
            )

            result = await _store_buffered_clips(
                bot=bot,
                chat_id=message.chat.id,
                services=services,
                clip_group=clip_group,
                clip_sub_group=clip_sub_group,
            )

            await state.clear()
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


async def _show_store_year_menu(
    *,
    message: Message,
    state: FSMContext,
    settings: Settings,
) -> bool:
    years = _store_year_options(current_year=date.today().year, min_year=settings.min_clip_year)
    if not years:
        return False

    await show_fixed_option_menu(
        flow=_STORE_FLOW,
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
) -> bool:
    if year not in _store_year_options(current_year=date.today().year, min_year=settings.min_clip_year):
        return False
    seasons = _store_season_options(year=year, today=date.today())

    await show_fixed_option_menu(
        flow=_STORE_FLOW,
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
) -> bool:
    await show_fixed_option_menu(
        flow=_STORE_FLOW,
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
) -> bool:
    await show_fixed_option_menu(
        flow=_STORE_FLOW,
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
) -> bool:
    await show_fixed_option_menu(
        flow=_STORE_FLOW,
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


def _store_year_options(*, current_year: int, min_year: int) -> list[int]:
    return year_option_universe(current_year=current_year, min_year=min_year)


def _store_season_options(*, year: int, today: date) -> list[Season]:
    return store_allowed_seasons(year=year, today=today)


def _telegram_clip_filename(message: Message) -> str:
    if message.video is not None and message.video.file_name:
        return message.video.file_name
    return f'telegram-{message.chat.id}-{message.message_id}.mp4'


def _clip_action_menu_kwargs(
    *,
    services: Services,
    chat_id: ChatId,
    message_width: int,
) -> dict[str, Any] | None:
    clip_count = len([message for message in services.chat_message_buffer.peek(chat_id) if message.video is not None])
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
                _create_clip_action_button(ClipAction.STORE),
                dummy_button(),
                _create_clip_action_button(ClipAction.CANCEL),
            ]
        ),
    }


async def _show_clip_action_menu(
    *,
    message: Message,
    state: FSMContext,
    services: Services,
    settings: Settings,
) -> None:
    await state.clear()
    kwargs = _clip_action_menu_kwargs(
        services=services,
        chat_id=message.chat.id,
        message_width=settings.message_width,
    )
    if kwargs is None:
        await message.edit_text('No clips received', reply_markup=None)
        return
    await message.edit_text(**kwargs)


def _create_clip_action_button(action: ClipAction) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=action.title(),
        callback_data=ClipActionCallbackData(action=action).pack(),
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
