from collections.abc import Sequence

from timeline_hub.services.clip_store import ClipGroup, ClipId, ClipStore, DuplicateClipIdsError
from timeline_hub.services.message_buffer import MessageGroup

_MIXED_GROUPS = 'mixed_groups'
_INVALID_IDENTITY = 'invalid_identity'


def prepare_reconcile_clip_id_batches(
    message_groups: Sequence[MessageGroup],
) -> tuple[ClipGroup, list[list[ClipId]]]:
    return _parse_reconcile_filename_batches(_message_groups_to_filenames(message_groups))


def clip_id_batch_count(clip_id_batches: list[list[ClipId]]) -> int:
    return sum(len(batch) for batch in clip_id_batches)


def _message_group_to_filenames(message_group: MessageGroup) -> list[str]:
    filenames: list[str] = []
    for message in message_group:
        if message.video is None:
            continue
        if not message.video.file_name:
            raise ValueError('Reconcile requires every buffered video to have a filename')
        filenames.append(message.video.file_name)
    return filenames


def _message_groups_to_filenames(message_groups: Sequence[MessageGroup]) -> list[list[str]]:
    return [filenames for message_group in message_groups if (filenames := _message_group_to_filenames(message_group))]


def _parse_reconcile_filename_batches(
    filename_batches: list[list[str]],
) -> tuple[ClipGroup, list[list[ClipId]]]:
    clip_group: ClipGroup | None = None
    clip_id_batches: list[list[ClipId]] = []
    flat_clip_ids: list[ClipId] = []

    for batch in filename_batches:
        clip_id_batch: list[ClipId] = []
        for filename in batch:
            identity_str = filename.rsplit('.', 1)[0] if '.' in filename else filename
            parsed_group, _parsed_sub_group, clip_id = ClipStore.string_to_clip_identity(identity_str)
            if clip_group is None:
                clip_group = parsed_group
            elif parsed_group != clip_group:
                raise ValueError(_MIXED_GROUPS)
            clip_id_batch.append(clip_id)
            flat_clip_ids.append(clip_id)
        if clip_id_batch:
            clip_id_batches.append(clip_id_batch)

    if clip_group is None or not clip_id_batches:
        raise ValueError(_INVALID_IDENTITY)
    if len(set(flat_clip_ids)) != len(flat_clip_ids):
        raise DuplicateClipIdsError(clip_ids=flat_clip_ids)
    return clip_group, clip_id_batches
