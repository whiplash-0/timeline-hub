from collections.abc import AsyncIterator, Sequence

from aiogram import Bot
from aiogram.types import BufferedInputFile, InputMediaVideo

from timeline_hub.services.clip_store import AudioNormalization, ClipGroup, ClipStore, ClipSubGroup, FetchedClip
from timeline_hub.settings import Settings
from timeline_hub.types import ChatId, Extension


async def send_fetched_clip_batches(
    *,
    bot: Bot,
    chat_id: ChatId,
    group: ClipGroup,
    sub_group: ClipSubGroup,
    clip_batches: AsyncIterator[tuple[FetchedClip, ...]],
) -> None:
    async for batch in clip_batches:
        await send_fetched_clip_batch(
            bot=bot,
            chat_id=chat_id,
            group=group,
            sub_group=sub_group,
            clips=batch,
        )


async def send_fetched_clip_batch(
    *,
    bot: Bot,
    chat_id: ChatId,
    group: ClipGroup,
    sub_group: ClipSubGroup,
    clips: Sequence[FetchedClip],
) -> None:
    if not clips:
        raise ValueError('`clips` must not be empty')

    if len(clips) == 1:
        clip = clips[0]
        await bot.send_video(
            chat_id=chat_id,
            video=BufferedInputFile(clip.file.data, filename=_fetched_clip_filename(group, sub_group, clip.id)),
        )
        return

    await bot.send_media_group(
        chat_id=chat_id,
        media=[
            InputMediaVideo(
                media=BufferedInputFile(clip.file.data, filename=_fetched_clip_filename(group, sub_group, clip.id)),
            )
            for clip in clips
        ],
    )


def audio_normalization_from_settings(*, settings: Settings) -> AudioNormalization:
    return AudioNormalization(
        loudness=settings.normalization_loudness,
        bitrate=settings.normalization_bitrate,
    )


def _fetched_clip_filename(group: ClipGroup, sub_group: ClipSubGroup, clip_id: str) -> str:
    identity = ClipStore.clip_identity_to_string(group, sub_group, clip_id)
    return f'{identity}{Extension.MP4.suffix}'
