from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from aiogram.types import Message

from timeline_hub.handlers.clips.flow import store_allowed_seasons, year_option_universe
from timeline_hub.services.clip_store import ClipGroup, Season, Universe
from timeline_hub.services.message_buffer import MessageGroup
from timeline_hub.settings import Settings


@dataclass(slots=True)
class RouteBatch:
    clip_group: ClipGroup
    messages: list[Message]


def plan_route_batches(
    message_groups: Sequence[MessageGroup],
    *,
    settings: Settings,
) -> tuple[list[RouteBatch], str | None]:
    batches: list[RouteBatch] = []
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
            if message.video is None:
                if message.text is None:
                    continue

                parsed_route = _parse_and_validate_route(
                    message.text,
                    today=today,
                    allowed_years=allowed_years,
                )
                if parsed_route is None:
                    continue
                current_route = parsed_route
                continue

            route_text = message.caption if message.caption is not None else message.text
            if route_text is None:
                if current_route is None:
                    return [], 'Missing route text'
                next_route = current_route
            else:
                next_route = _parse_and_validate_route(
                    route_text,
                    today=today,
                    allowed_years=allowed_years,
                )
                if next_route is None:
                    return [], 'Invalid route text'

            if not batches or batches[-1].clip_group != next_route:
                batches.append(RouteBatch(clip_group=next_route, messages=[message]))
            else:
                batches[-1].messages.append(message)
            current_route = next_route

    return batches, None


def parse_route_text(text: str) -> ClipGroup | None:
    normalized = text.strip()
    if len(normalized) != 4:
        return None

    universe_text = normalized[0].lower()
    year_suffix = normalized[1:3]
    season_text = normalized[3]

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

    return ClipGroup(universe=universe, year=2000 + int(year_suffix), season=season)


def _parse_and_validate_route(
    text: str,
    *,
    today: date,
    allowed_years: set[int],
) -> ClipGroup | None:
    parsed_route = parse_route_text(text)
    if parsed_route is None:
        return None
    if parsed_route.year not in allowed_years:
        return None
    if parsed_route.season not in store_allowed_seasons(year=parsed_route.year, today=today):
        return None
    return parsed_route
