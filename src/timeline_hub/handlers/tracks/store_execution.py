from collections.abc import Sequence

from aiogram import Bot
from aiogram.types import Message

from timeline_hub.infra.ffmpeg import to_opus
from timeline_hub.infra.images import normalize_cover_to_jpg
from timeline_hub.services.track_store import Track, TrackGroup, TrackId, TrackStore
from timeline_hub.types import Extension, FileBytes


class TrackInputError(ValueError):
    pass


def extract_single_photo_audio_messages(messages: Sequence[Message]) -> tuple[Message, Message]:
    """Return exactly one photo message and one audio message, order-independent."""
    if len(messages) != 2:
        raise TrackInputError('Invalid input')

    photo_messages = [message for message in messages if message.photo is not None]
    audio_messages = [message for message in messages if message.audio is not None]
    if len(photo_messages) != 1 or len(audio_messages) != 1:
        raise TrackInputError('Invalid input')
    return photo_messages[0], audio_messages[0]


def extract_photo_messages_for_remove(messages: Sequence[Message]) -> tuple[Message, ...]:
    """Return one or more photo messages for remove actions."""
    if len(messages) < 1:
        raise TrackInputError('Invalid input')
    if any(message.photo is None for message in messages):
        raise TrackInputError('Invalid input')
    return tuple(messages)


def extract_track_identity_from_photo_message(photo_message: Message) -> tuple[TrackGroup, TrackId]:
    """Decode linked-dot cover caption identity into `(group, track_id)`."""
    caption = photo_message.caption
    if caption is None or not caption or caption[0] != '·':
        raise TrackInputError('Invalid input')

    entities = photo_message.caption_entities
    if not entities:
        raise TrackInputError('Invalid input')

    link_url: str | None = None
    for entity in entities:
        entity_type = getattr(entity, 'type', None)
        if getattr(entity_type, 'value', entity_type) != 'text_link':
            continue
        if getattr(entity, 'offset', None) != 0 or getattr(entity, 'length', None) != 1:
            continue
        link_url = getattr(entity, 'url', None)
        break

    if not link_url or not isinstance(link_url, str):
        raise TrackInputError('Invalid input')
    if not link_url.startswith('https://'):
        raise TrackInputError('Invalid input')

    identity = link_url.removeprefix('https://')
    if identity.endswith('.com/'):
        identity = identity.removesuffix('.com/')
    elif identity.endswith('.com'):
        identity = identity.removesuffix('.com')
    else:
        raise TrackInputError('Invalid input')

    if not identity:
        raise TrackInputError('Invalid input')
    return TrackStore.string_to_track_identity(identity)


async def prepare_audio_from_message(*, bot: Bot, audio_message: Message) -> FileBytes:
    """Download one audio message and normalize to OPUS `FileBytes`."""
    audio = audio_message.audio
    if audio is None:
        raise TrackInputError('Invalid input')

    audio_bytes = await _download_file_bytes(bot=bot, file_id=audio.file_id)
    try:
        audio_extension = Extension.try_from_filename(audio.file_name)
        if audio_extension is Extension.OPUS:
            audio_opus = audio_bytes
        else:
            audio_opus = await to_opus(audio_bytes)
    except Exception as error:
        raise TrackInputError("Can't process audio") from error

    return FileBytes(data=audio_opus, extension=Extension.OPUS)


def extract_store_messages(messages: Sequence[Message]) -> list[Message]:
    """Return store-relevant messages in original order."""
    return [message for message in messages if message.photo is not None or message.audio is not None]


def track_count_from_store_messages(messages: Sequence[Message]) -> int:
    return len(extract_store_messages(messages)) // 2


def validate_track_batch(messages: Sequence[Message]) -> list[tuple[tuple[str, ...], str]]:
    if len(messages) < 2 or len(messages) % 2 != 0:
        raise TrackInputError("Can't dispatch input")

    parsed_tracks: list[tuple[tuple[str, ...], str]] = []
    for index in range(0, len(messages), 2):
        photo_message = messages[index]
        audio_message = messages[index + 1]
        if photo_message.photo is None or audio_message.audio is None:
            raise TrackInputError("Can't dispatch input")
        if photo_message.caption is None or not photo_message.caption.strip():
            raise TrackInputError("Can't dispatch input")

        try:
            parsed_tracks.append(_caption_to_artists_and_title(photo_message.caption))
        except TrackInputError as error:
            raise TrackInputError("Can't dispatch input") from error

    return parsed_tracks


async def prepare_tracks_from_buffer(*, bot: Bot, messages: Sequence[Message]) -> list[Track]:
    store_messages = extract_store_messages(messages)
    parsed_tracks = validate_track_batch(store_messages)
    prepared_tracks: list[Track] = []
    for parsed_track, index in zip(parsed_tracks, range(0, len(store_messages), 2), strict=True):
        photo_message = store_messages[index]
        audio_message = store_messages[index + 1]
        photo = photo_message.photo
        audio = audio_message.audio
        if photo is None or audio is None:
            raise TrackInputError("Can't dispatch input")

        artists, title = parsed_track
        cover_bytes = await _download_file_bytes(
            bot=bot,
            file_id=photo[-1].file_id,
        )
        audio_bytes = await _download_file_bytes(
            bot=bot,
            file_id=audio.file_id,
        )

        try:
            cover_jpg = normalize_cover_to_jpg(cover_bytes)
        except Exception as error:
            raise TrackInputError("Can't process cover image") from error

        try:
            # Best-effort extension parse (filename may be missing or invalid).
            audio_extension = Extension.try_from_filename(audio.file_name)
            if audio_extension is Extension.OPUS:
                # Fast-path: avoid re-encoding already-Opus input.
                audio_opus = audio_bytes
            else:
                audio_opus = await to_opus(audio_bytes)
        except Exception as error:
            raise TrackInputError("Can't process audio") from error

        prepared_tracks.append(
            Track(
                artists=artists,
                title=title,
                cover=FileBytes(data=cover_jpg, extension=Extension.JPG),
                audio=FileBytes(data=audio_opus, extension=Extension.OPUS),
            )
        )

    return prepared_tracks


def _caption_to_artists_and_title(caption: str | None) -> tuple[tuple[str, ...], str]:
    lines = [line.strip() for line in (caption or '').splitlines() if line.strip()]
    if len(lines) < 2:
        raise TrackInputError('Not enough lines to extract artists and title')
    return tuple(lines[:-1]), lines[-1]


async def _download_file_bytes(*, bot: Bot, file_id: str) -> bytes:
    telegram_file = await bot.get_file(file_id)
    if telegram_file.file_path is None:
        raise TrackInputError("Can't dispatch input")

    downloaded = await bot.download_file(telegram_file.file_path)
    if downloaded is None:
        raise TrackInputError("Can't dispatch input")

    return downloaded.read()
