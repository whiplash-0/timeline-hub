from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import TypeVar

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.types import InlineKeyboardButton, Message

from general_bot.handlers.clips.common import (
    ALL_SCOPES_CALLBACK_VALUE,
    BACK_CALLBACK_VALUE,
    UNSET,
    MenuAction,
    MenuStep,
    back_button,
    fixed_option_keyboard,
    handle_stale_selection,
    selection_labels,
    selection_text,
    set_flow_context,
    validate_flow_state,
)
from general_bot.services.clip_store import ClipGroup, ClipSubGroup, Scope, Season, SubSeason, Universe

type MenuCallbackPacker = Callable[[MenuAction, MenuStep, str], str]
type ShowMenu = Callable[..., Awaitable[bool]]
T = TypeVar('T')


@dataclass(frozen=True, slots=True)
class FlowMenuDefinition:
    mode: str
    flow_label: str
    state_by_step: Mapping[MenuStep, State]
    pack_callback: MenuCallbackPacker


async def validate_menu_flow_state(
    *,
    message: Message,
    state: FSMContext,
    flow: FlowMenuDefinition,
    step: MenuStep,
) -> bool:
    return await validate_flow_state(
        message=message,
        state=state,
        expected_mode=flow.mode,
        expected_state=flow.state_by_step[step],
    )


def flow_selection_labels(
    flow: FlowMenuDefinition,
    *,
    universe: Universe | object = UNSET,
    year: int | object = UNSET,
    season: Season | object = UNSET,
    sub_season: SubSeason | object = UNSET,
    scope: Scope | str | object = UNSET,
) -> list[str]:
    return [
        flow.flow_label,
        *selection_labels(
            universe=universe,
            year=year,
            season=season,
            sub_season=sub_season,
            scope=scope,
        ),
    ]


def flow_menu_button(
    *,
    flow: FlowMenuDefinition,
    step: MenuStep,
    value: str,
    text: str,
) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=text,
        callback_data=flow.pack_callback(MenuAction.SELECT, step, value),
    )


def flow_back_button(*, flow: FlowMenuDefinition, step: MenuStep) -> InlineKeyboardButton:
    return back_button(
        callback_data=flow.pack_callback(MenuAction.BACK, step, BACK_CALLBACK_VALUE),
    )


async def show_fixed_option_menu(
    *,
    flow: FlowMenuDefinition,
    message: Message,
    state: FSMContext,
    message_width: int,
    step: MenuStep,
    prompt: str,
    option_universe: Sequence[T],
    available_options: Sequence[T],
    option_value: Callable[[T], str],
    option_text: Callable[[T], str],
    universe: Universe | object = UNSET,
    year: int | object = UNSET,
    season: Season | object = UNSET,
    sub_season: SubSeason | object = UNSET,
) -> None:
    await set_flow_context(
        state=state,
        mode=flow.mode,
        menu_message_id=message.message_id,
        fsm_state=flow.state_by_step[step],
        universe=universe,
        year=year,
        season=season,
        sub_season=sub_season,
    )
    await message.edit_text(
        **selection_text(
            prompt=prompt,
            selected=flow_selection_labels(
                flow,
                universe=universe,
                year=year,
                season=season,
                sub_season=sub_season,
            ),
            message_width=message_width,
        ),
        reply_markup=fixed_option_keyboard(
            option_universe=option_universe,
            available_options=available_options,
            build_button=lambda option: flow_menu_button(
                flow=flow,
                step=step,
                value=option_value(option),
                text=option_text(option),
            ),
            back_button=flow_back_button(flow=flow, step=step),
        ),
    )


async def show_or_stale(
    *,
    show_menu: ShowMenu,
    message: Message,
    state: FSMContext,
    **kwargs: object,
) -> bool:
    if await show_menu(message=message, state=state, **kwargs):
        return True
    await handle_stale_selection(message=message, state=state)
    return False


def selected_universe(data: Mapping[str, object]) -> Universe | None:
    universe = data.get('universe')
    if isinstance(universe, Universe):
        return universe
    return None


def selected_universe_year(data: Mapping[str, object]) -> tuple[Universe, int] | None:
    universe = selected_universe(data)
    year = selected_year(data)
    if universe is None or year is None:
        return None
    return universe, year


def selected_year(data: Mapping[str, object]) -> int | None:
    year = data.get('year')
    if isinstance(year, int):
        return year
    return None


def selected_universe_year_season(data: Mapping[str, object]) -> tuple[Universe, int, Season] | None:
    selection = selected_universe_year(data)
    season = data.get('season')
    if selection is None or not isinstance(season, Season):
        return None
    universe, year = selection
    return universe, year, season


def selected_universe_year_season_sub_season(
    data: Mapping[str, object],
) -> tuple[Universe, int, Season, SubSeason] | None:
    selection = selected_universe_year_season(data)
    sub_season = data.get('sub_season')
    if selection is None or not isinstance(sub_season, SubSeason):
        return None
    universe, year, season = selection
    return universe, year, season, sub_season


def year_option_universe(*, current_year: int, min_year: int) -> list[int]:
    if current_year < min_year:
        return []
    return list(range(min_year, current_year + 1))


def store_allowed_seasons(*, year: int, today: date) -> list[Season]:
    if year != today.year:
        return list(Season)
    max_season = Season.from_month(today.month)
    return [season for season in Season if season <= max_season]


def available_group_years(groups: Sequence[ClipGroup], *, universe: Universe) -> list[int]:
    return sorted({group.year for group in groups if group.universe is universe})


def available_group_seasons(
    groups: Sequence[ClipGroup],
    *,
    universe: Universe,
    year: int,
) -> list[Season]:
    return [
        season
        for season in Season
        if any(group.universe is universe and group.year == year and group.season is season for group in groups)
    ]


def available_sub_seasons(sub_groups: Sequence[ClipSubGroup]) -> list[SubSeason]:
    return [
        sub_season for sub_season in SubSeason if any(sub_group.sub_season is sub_season for sub_group in sub_groups)
    ]


def available_scopes(
    sub_groups: Sequence[ClipSubGroup],
    sub_season: SubSeason,
) -> list[Scope]:
    return [
        scope
        for scope in Scope
        if any(sub_group.sub_season is sub_season and sub_group.scope is scope for sub_group in sub_groups)
    ]


def scope_option_callback_value(option: Scope | str) -> str:
    if option == ALL_SCOPES_CALLBACK_VALUE:
        return ALL_SCOPES_CALLBACK_VALUE
    if not isinstance(option, Scope):
        raise ValueError(f'Unsupported scope option: {option!r}')
    return option.value


def scope_option_text(option: Scope | str) -> str:
    if option == ALL_SCOPES_CALLBACK_VALUE:
        return 'All'
    if not isinstance(option, Scope):
        raise ValueError(f'Unsupported scope option: {option!r}')
    return option.value.title()
