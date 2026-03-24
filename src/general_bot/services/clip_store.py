import asyncio
import hashlib
import itertools
import json
import os
import tempfile
import uuid
from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum, StrEnum
from pathlib import Path
from typing import Any, Self

from general_bot.infra.s3 import Key, Prefix, S3Client, S3ContentType, S3ObjectNotFoundError

_CLIPS_PREFIX = 'clips'
_MANIFEST_FILENAME = 'manifest.json'
_VIDEO_SUFFIX = '.mp4'
_FFMPEG_TIMEOUT = timedelta(seconds=30)
_HASH_READ_SIZE = 64 * 1024
_CLIP_GROUP_SEPARATOR = '-'
# S3 keys often use '/' as delimiter, which is not safe for Telegram/local filenames.
# This token replaces '/' when converting storage keys to portable filenames.
_FILENAME_S3_DELIMITER_ESCAPE = '--'

type Filename = str


class Season(IntEnum):
    """Clip season identifier.

    Month mapping:
        - `S1`: months 1-2
        - `S2`: months 3-5
        - `S3`: months 6-8
        - `S4`: months 9-10
        - `S5`: months 11-12
    """

    S1 = 1
    S2 = 2
    S3 = 3
    S4 = 4
    S5 = 5

    @classmethod
    def from_month(cls, month: int) -> Season:
        """Return the season that contains the provided month."""
        match month:
            case 1 | 2:
                return cls.S1
            case 3 | 4 | 5:
                return cls.S2
            case 6 | 7 | 8:
                return cls.S3
            case 9 | 10:
                return cls.S4
            case 11 | 12:
                return cls.S5
            case _:
                raise ValueError('`month` must be in 1..12')


class Universe(StrEnum):
    """Clip universe identifier."""

    WEST = 'west'
    EAST = 'east'


@dataclass(frozen=True, slots=True)
class ClipGroup:
    """Logical clip-group identifier."""

    year: int
    season: Season
    universe: Universe


class SubSeason(StrEnum):
    """Clip sub-season identifier."""

    NONE = 'none'
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'


class Scope(StrEnum):
    """Clip scope identifier."""

    COLLECTION = 'collection'
    EXTRA = 'extra'
    SOURCE = 'source'


_SUB_SEASON_ORDER = {
    SubSeason.A: 0,
    SubSeason.B: 1,
    SubSeason.C: 2,
    SubSeason.D: 3,
    SubSeason.NONE: 4,
}


@dataclass(frozen=True, slots=True)
class ClipSubGroup:
    """Logical clip sub-group identifier."""

    sub_season: SubSeason
    scope: Scope


@dataclass(frozen=True, slots=True)
class Clip:
    """Clip payload plus portable filename.

    Stored clips may originate from an S3 key, which cannot be used directly
    as a filename because path separators like `/` are not portable across
    Telegram and local filesystems.
    """

    filename: str
    bytes: bytes


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """Single persisted clip row in a clip-group manifest."""

    id: str
    video_hash: str
    sub_season: SubSeason
    scope: Scope
    batch: int
    order: int


class Manifest:
    """Aggregate wrapper over manifest clip entries."""

    def __init__(self, entries: Iterable[ManifestEntry] | None = None) -> None:
        self._entries = list(entries or [])

    def __iter__(self) -> Iterator[ManifestEntry]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def copy(self) -> Self:
        """Return a shallow copy that can be safely mutated."""
        return type(self)(self._entries)

    def append(self, entry: ManifestEntry) -> None:
        """Append a manifest entry."""
        self._entries.append(entry)

    def has_id(self, clip_id: str) -> bool:
        """Return whether the manifest already contains a clip id."""
        return any(entry.id == clip_id for entry in self._entries)

    def has_video_hash(self, video_hash: str) -> bool:
        """Return whether the manifest already contains a video hash."""
        return any(entry.video_hash == video_hash for entry in self._entries)

    def next_batch(self, *, sub_season: SubSeason, scope: Scope) -> int:
        """Return the next logical batch within a sub-group."""
        return (
            max(
                (entry.batch for entry in self._entries if entry.scope is scope and entry.sub_season is sub_season),
                default=0,
            )
            + 1
        )

    def to_list(self) -> list[dict[str, Any]]:
        """Convert the manifest into its JSON-compatible shape."""
        return [
            {
                'id': entry.id,
                'video_hash': entry.video_hash,
                'sub_season': entry.sub_season.value,
                'scope': entry.scope.value,
                'batch': entry.batch,
                'order': entry.order,
            }
            for entry in self._entries
        ]

    @classmethod
    def from_list(cls, data: object) -> Self:
        """Build a manifest from a decoded JSON payload.

        Args:
            data: Decoded JSON value from `manifest.json`.

        Raises:
            ValueError: If the payload does not match the manifest schema.
        """
        if not isinstance(data, list):
            raise ValueError('manifest root must be a list')

        entries: list[ManifestEntry] = []
        seen_ids: set[str] = set()
        seen_hashes: set[str] = set()
        seen_positions: set[tuple[SubSeason, Scope, int, int]] = set()

        for raw_entry in data:
            if not isinstance(raw_entry, dict):
                raise ValueError('manifest clip entry must be an object')
            if set(raw_entry) != {'id', 'video_hash', 'sub_season', 'scope', 'batch', 'order'}:
                raise ValueError('manifest clip entry has unexpected fields')

            clip_id = _parse_uuid7(_expect_str(raw_entry['id'], field='id'), field='id')
            video_hash = _parse_sha256_hex(_expect_str(raw_entry['video_hash'], field='video_hash'))
            sub_season = _parse_sub_season(raw_entry['sub_season'])
            scope = _parse_enum(raw_entry['scope'], Scope, field='scope')

            batch = raw_entry['batch']
            if isinstance(batch, bool) or not isinstance(batch, int):
                raise ValueError('manifest `batch` must be an integer')
            if batch < 1:
                raise ValueError('manifest `batch` must be >= 1')

            order = raw_entry['order']
            if isinstance(order, bool) or not isinstance(order, int):
                raise ValueError('manifest `order` must be an integer')
            if order < 1:
                raise ValueError('manifest `order` must be >= 1')

            if clip_id in seen_ids:
                raise ValueError(f'duplicate manifest clip id: {clip_id}')
            if video_hash in seen_hashes:
                raise ValueError(f'duplicate manifest video hash: {video_hash}')

            position_key = (sub_season, scope, batch, order)
            if position_key in seen_positions:
                raise ValueError(
                    f'duplicate manifest position for sub_season={_format_sub_season(sub_season)} '
                    f'scope={scope.value} batch={batch} order={order}'
                )

            seen_ids.add(clip_id)
            seen_hashes.add(video_hash)
            seen_positions.add(position_key)
            entries.append(
                ManifestEntry(
                    id=clip_id,
                    video_hash=video_hash,
                    sub_season=sub_season,
                    scope=scope,
                    batch=batch,
                    order=order,
                )
            )

        return cls(entries)


@dataclass(frozen=True, slots=True)
class StoreResult:
    """Result summary for a `store()` call."""

    stored_count: int
    duplicate_count: int

    def __add__(self, other: Self) -> Self:
        if not isinstance(other, type(self)):
            return NotImplemented
        return type(self)(
            stored_count=self.stored_count + other.stored_count,
            duplicate_count=self.duplicate_count + other.duplicate_count,
        )


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Result summary for a `reconcile()` call."""

    updated: int
    removed: int


class DuplicateFilenamesError(ValueError):
    """Raised when `reconcile()` receives duplicate filenames."""


class InvalidFilenamesError(ValueError):
    """Raised when `reconcile()` receives filenames that are not stored in the clip group."""


class MixedClipGroupsError(ValueError):
    """Raised when reconcile filenames span more than one clip group."""

    def __init__(self, *, groups: Sequence[ClipGroup]) -> None:
        self.groups = tuple(groups)
        super().__init__(f'Filenames span multiple clip groups: {list(self.groups)}')


class UnknownClipsError(ValueError):
    """Raised when reconcile filenames refer to clip ids missing from the manifest."""

    def __init__(self, *, clip_ids: Sequence[str]) -> None:
        self.clip_ids = tuple(clip_ids)
        super().__init__(f'Clip ids are not present in manifest: {list(self.clip_ids)}')


class ManifestCorruptedError(RuntimeError):
    """Raised when `manifest.json` exists but cannot be decoded or validated."""

    def __init__(self, key: Key, reason: str) -> None:
        self.key = key
        super().__init__(f'Manifest at {key} is corrupted: {reason}')


class ClipGroupNotFoundError(LookupError):
    """Raised when the requested logical clip group has no matching clips."""

    def __init__(
        self,
        *,
        year: int,
        season: Season,
        universe: Universe,
        sub_season: SubSeason | None,
        scope: Scope | None,
    ) -> None:
        self.year = year
        self.season = season
        self.universe = universe
        self.sub_season = sub_season
        self.scope = scope
        super().__init__(
            f'No clips found for {year}-{int(season)}-{universe.value} '
            f'sub_season={_format_optional_sub_season(sub_season)} scope={_format_scope(scope)}'
        )


class ClipStoreRollbackError(RuntimeError):
    """Raised when store rollback cannot fully delete uploaded clip objects."""

    def __init__(self, *, failed_keys: Sequence[Key]) -> None:
        self.failed_keys = tuple(failed_keys)
        super().__init__(f'Rollback failed for {len(self.failed_keys)} uploaded keys: {list(self.failed_keys)}')


class ReconcileDeleteError(RuntimeError):
    """Raised when reconcile cannot fully delete removed clip objects."""

    def __init__(self, *, failed_keys: Sequence[Key]) -> None:
        self.failed_keys = tuple(failed_keys)
        super().__init__(f'Reconcile cleanup failed for {len(self.failed_keys)} removed keys: {list(self.failed_keys)}')


class ClipStore:
    """Domain-specific wrapper over `S3Client` for grouped clip storage.

    Clips are organized into `ClipGroup` objects, with sub-groups represented
    by `ClipSubGroup`. Ordering within a sub-group is hierarchical: each
    `store()` call creates one logical batch, and clips inside that batch
    keep their dense 1-based order. Batch numbers only define relative
    ordering; they do not need to be contiguous.

    Deduplication is enforced only within a single clip group. Newly stored
    objects always receive a fresh UUIDv7 hex `id`, whose embedded timestamp
    reflects object creation time.

    The manifest is the authoritative index for each clip group and is
    validated on load. Clip-group listing is prefix-based, while sub-group
    listing is manifest-based. Store operations stage uploads first and commit
    by writing the updated manifest. If commit fails after clip uploads begin,
    uploaded objects from that call are rolled back on a best-effort basis.
    """

    def __init__(self, s3_client: S3Client) -> None:
        """Initialize the store with an opened generic S3 client."""
        self._s3_client = s3_client
        self._manifest_cache: dict[Prefix, Manifest] = {}

    async def store(
        self,
        clips: Sequence[Clip],
        *,
        clip_group: ClipGroup,
        clip_sub_group: ClipSubGroup,
    ) -> StoreResult:
        """Store one logical clip batch with clip-group-local deduplication.

        One `store()` call maps to exactly one batch within the target
        `ClipSubGroup`. Accepted clips keep their input order and receive
        dense `order` values starting at `1`. If every clip in the call is a
        duplicate, the method returns without creating a new batch. Callers
        must not mix clips from multiple logical batches in a single call.

        The operation is atomic at the store-call level: if persistence fails
        after any clip uploads succeed, all clip objects uploaded during this
        call are deleted before the exception is re-raised.

        Writes to the same `ClipGroup` are assumed to be sequential
        (single-writer). Concurrent writes are not supported and may lead to
        manifest overwrite and orphaned clips.

        Raises:
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
            RuntimeError: If clip hashing fails.
        """
        clip_group_prefix = self._clip_group_prefix(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
        )
        manifest = await self._load_manifest_for_store(clip_group_prefix)
        seen_hashes: set[str] = set()
        seen_ids: set[str] = set()
        duplicate_count = 0
        accepted_clips: list[tuple[str, str, Clip]] = []
        uploaded_keys: list[Key] = []
        for clip in clips:
            stored_clip_id = self._parse_stored_clip_id(clip.filename)
            video_hash = await self._hash_video_bytes(clip.bytes)

            if stored_clip_id is not None and (manifest.has_id(stored_clip_id) or stored_clip_id in seen_ids):
                duplicate_count += 1
                continue

            if manifest.has_video_hash(video_hash) or video_hash in seen_hashes:
                duplicate_count += 1
                continue

            clip_id = self._new_clip_id(manifest=manifest, seen_ids=seen_ids)
            accepted_clips.append((clip_id, video_hash, clip))
            seen_ids.add(clip_id)
            seen_hashes.add(video_hash)

        if not accepted_clips:
            return StoreResult(stored_count=0, duplicate_count=duplicate_count)

        batch = manifest.next_batch(
            sub_season=clip_sub_group.sub_season,
            scope=clip_sub_group.scope,
        )
        new_entries: list[tuple[ManifestEntry, Clip]] = []
        for order, (clip_id, video_hash, clip) in enumerate(accepted_clips, start=1):
            entry = ManifestEntry(
                id=clip_id,
                video_hash=video_hash,
                sub_season=clip_sub_group.sub_season,
                scope=clip_sub_group.scope,
                batch=batch,
                order=order,
            )
            manifest.append(entry)
            new_entries.append((entry, clip))

        manifest_key = self._manifest_key(clip_group_prefix)
        manifest_payload = json.dumps(manifest.to_list(), separators=(',', ':')).encode('utf-8')

        try:
            for entry, clip in new_entries:
                clip_key = self._clip_key(clip_group_prefix, entry.id)
                await self._s3_client.put_bytes(
                    clip_key,
                    bytes_=clip.bytes,
                    content_type=S3ContentType.MP4,
                )
                uploaded_keys.append(clip_key)

            await self._s3_client.put_bytes(
                manifest_key,
                bytes_=manifest_payload,
                content_type=S3ContentType.JSON,
            )
        except Exception as error:
            try:
                await self._rollback_uploads(uploaded_keys)
            except Exception as rollback_error:
                rollback_error.add_note(f'Original store error: {error!r}')
                raise rollback_error from error
            raise

        self._manifest_cache[clip_group_prefix] = manifest.copy()
        return StoreResult(
            stored_count=len(new_entries),
            duplicate_count=duplicate_count,
        )

    async def compact(
        self,
        *,
        clip_group: ClipGroup,
        clip_sub_group: ClipSubGroup,
        batch_size: int,
    ) -> None:
        """Compact one clip sub-group by rewriting manifest batch metadata only.

        Compaction preserves the exact subgroup-local relative order defined by
        `(batch, order)` and rewrites only batching for the specified
        `ClipSubGroup`. Clip objects are never downloaded, rewritten, or
        re-uploaded. After compaction, target batch numbering is dense,
        starts at `1`, and uses local `order` values `1..N` within each batch.

        Raises:
            ValueError: If `batch_size` is less than `1`.
            ClipGroupNotFoundError: If the requested clip group or sub-group has no clips.
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
        """
        if batch_size < 1:
            raise ValueError('`batch_size` must be >= 1')

        clip_group_prefix = self._clip_group_prefix(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=None,
                scope=None,
            ) from error

        target_entries = self._sorted_sub_group_entries(manifest, clip_sub_group)
        if not target_entries:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=clip_sub_group.sub_season,
                scope=clip_sub_group.scope,
            )

        compacted_positions: dict[str, tuple[int, int]] = {}
        changed = False
        for index, entry in enumerate(target_entries, start=1):
            compacted_batch = ((index - 1) // batch_size) + 1
            compacted_order = ((index - 1) % batch_size) + 1
            compacted_positions[entry.id] = (compacted_batch, compacted_order)
            if (entry.batch, entry.order) != (compacted_batch, compacted_order):
                changed = True

        if not changed:
            return

        rewritten_entries: list[ManifestEntry] = []
        for entry in manifest:
            if entry.scope is clip_sub_group.scope and entry.sub_season is clip_sub_group.sub_season:
                compacted_batch, compacted_order = compacted_positions[entry.id]
                rewritten_entries.append(
                    ManifestEntry(
                        id=entry.id,
                        video_hash=entry.video_hash,
                        sub_season=entry.sub_season,
                        scope=entry.scope,
                        batch=compacted_batch,
                        order=compacted_order,
                    )
                )
            else:
                rewritten_entries.append(entry)

        rewritten_manifest = Manifest(rewritten_entries)
        manifest_key = self._manifest_key(clip_group_prefix)
        manifest_payload = json.dumps(rewritten_manifest.to_list(), separators=(',', ':')).encode('utf-8')

        # Compaction is manifest-only; clip objects stay untouched.
        await self._s3_client.put_bytes(
            manifest_key,
            bytes_=manifest_payload,
            content_type=S3ContentType.JSON,
        )

        # Keep the manifest cache synchronized with the rewritten manifest.
        self._manifest_cache[clip_group_prefix] = rewritten_manifest

    async def derive_group(
        self,
        filename_batches: Sequence[Sequence[Filename]],
    ) -> ClipGroup:
        """Validate reconcile filenames and derive their single common clip group.

        Raises:
            ValueError: If `filename_batches` is empty or all batches are empty.
            DuplicateFilenamesError: If the provided filenames contain duplicates.
            InvalidFilenamesError: If any filename is malformed or not a stored-style clip filename.
            MixedClipGroupsError: If filenames refer to more than one clip group.
            UnknownClipsError: If clip ids are missing from the derived group's manifest.
            ManifestCorruptedError: If the derived clip-group manifest exists but is malformed.
        """
        if not filename_batches or all(not batch for batch in filename_batches):
            raise ValueError('`filename_batches` must contain at least one filename')

        flat_filenames = [filename for batch in filename_batches for filename in batch]
        if len(set(flat_filenames)) != len(flat_filenames):
            raise DuplicateFilenamesError('`filename_batches` must not contain duplicate filenames')

        parsed_identities: list[tuple[ClipGroup, str]] = []
        parsed_groups: set[ClipGroup] = set()
        for filename in flat_filenames:
            parsed_identity = self._parse_filename_identity(filename)
            if parsed_identity is None:
                raise InvalidFilenamesError(f'Filename is not a stored clip: {filename}')
            clip_group, clip_id = parsed_identity
            parsed_identities.append((clip_group, clip_id))
            parsed_groups.add(clip_group)

        if len(parsed_groups) != 1:
            raise MixedClipGroupsError(
                groups=sorted(
                    parsed_groups,
                    key=lambda group: (group.year, int(group.season), group.universe.value),
                )
            )

        clip_group = next(iter(parsed_groups))
        clip_group_prefix = self._clip_group_prefix(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise UnknownClipsError(clip_ids=[clip_id for _, clip_id in parsed_identities]) from error

        known_ids = {entry.id for entry in manifest}
        unknown_ids = [clip_id for _, clip_id in parsed_identities if clip_id not in known_ids]
        if unknown_ids:
            raise UnknownClipsError(clip_ids=unknown_ids)

        return clip_group

    async def reconcile(
        self,
        filename_batches: Sequence[Sequence[Filename]],
        *,
        clip_group: ClipGroup,
        clip_sub_group: ClipSubGroup,
    ) -> ReconcileResult:
        """Replace one sub-group with the provided filename-derived manifest state.

        Reconcile is manifest-authoritative for the target `ClipSubGroup`.
        The provided `filename_batches` define the complete desired subgroup
        state: their order becomes canonical, omitted clips are removed from
        that subgroup, and clips may be moved in from other sub-groups within
        the same `ClipGroup`. The operation never hashes, uploads, downloads,
        or rewrites clip bytes. It only rewrites the manifest and deletes clip
        objects that become unreachable from the final manifest.

        Precondition:
            `filename_batches` must refer to clips belonging to the provided
            `clip_group`. Callers are expected to validate and derive the
            common group first via `derive_group()`.

        Raises:
            ValueError: If `filename_batches` is empty or all batches are empty.
            DuplicateFilenamesError: If the provided filenames contain duplicates.
            InvalidFilenamesError: If any filename is malformed or not a stored-style clip filename.
            UnknownClipsError: If a parsed clip id is not present in the provided clip group's manifest.
            ClipGroupNotFoundError: If the requested clip group has no manifest.
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
            ReconcileDeleteError: If one or more removed clip objects cannot be deleted after the manifest rewrite.
        """
        if not filename_batches or all(not batch for batch in filename_batches):
            raise ValueError('`filename_batches` must contain at least one filename')

        flat_filenames = [filename for batch in filename_batches for filename in batch]
        if len(set(flat_filenames)) != len(flat_filenames):
            raise DuplicateFilenamesError('`filename_batches` must not contain duplicate filenames')

        clip_id_batches: list[list[str]] = []
        for batch in filename_batches:
            clip_id_batch: list[str] = []
            for filename in batch:
                parsed_identity = self._parse_filename_identity(filename)
                if parsed_identity is None:
                    raise InvalidFilenamesError(f'Filename is not a stored clip: {filename}')
                parsed_group, clip_id = parsed_identity
                if parsed_group != clip_group:
                    raise ValueError('`filename_batches` must belong to the provided `clip_group`')
                clip_id_batch.append(clip_id)
            clip_id_batches.append(clip_id_batch)

        clip_ids = [clip_id for batch in clip_id_batches for clip_id in batch]
        if not clip_ids:
            raise ValueError('`filename_batches` must contain at least one filename')

        clip_group_prefix = self._clip_group_prefix(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=None,
                scope=None,
            ) from error

        entries_by_id = {entry.id: entry for entry in manifest}
        unknown_ids = [clip_id for clip_id in clip_ids if clip_id not in entries_by_id]
        if unknown_ids:
            raise UnknownClipsError(clip_ids=unknown_ids)

        existing_target_entries = [
            entry
            for entry in manifest
            if entry.scope is clip_sub_group.scope and entry.sub_season is clip_sub_group.sub_season
        ]
        old_subgroup_ids = {entry.id for entry in existing_target_entries}

        new_entries: list[ManifestEntry] = []
        new_subgroup_ids: set[str] = set()
        for batch_index, clip_id_batch in enumerate(clip_id_batches, start=1):
            for order_index, clip_id in enumerate(clip_id_batch, start=1):
                existing_entry = entries_by_id[clip_id]
                new_entries.append(
                    ManifestEntry(
                        id=clip_id,
                        video_hash=existing_entry.video_hash,
                        sub_season=clip_sub_group.sub_season,
                        scope=clip_sub_group.scope,
                        batch=batch_index,
                        order=order_index,
                    )
                )
                new_subgroup_ids.add(clip_id)

        rewritten_entries: list[ManifestEntry] = []
        for entry in manifest:
            if entry.scope is clip_sub_group.scope and entry.sub_season is clip_sub_group.sub_season:
                continue
            if entry.id in new_subgroup_ids:
                continue
            rewritten_entries.append(entry)

        rewritten_entries.extend(new_entries)
        rewritten_manifest = Manifest(rewritten_entries)
        manifest_key = self._manifest_key(clip_group_prefix)
        manifest_payload = json.dumps(rewritten_manifest.to_list(), separators=(',', ':')).encode('utf-8')

        await self._s3_client.put_bytes(
            manifest_key,
            bytes_=manifest_payload,
            content_type=S3ContentType.JSON,
        )

        # The manifest is authoritative, so cache must track the rewritten
        # manifest even if deleting removed clip objects fails afterwards.
        self._manifest_cache[clip_group_prefix] = rewritten_manifest

        removed_ids = old_subgroup_ids - new_subgroup_ids
        failed_keys: list[Key] = []
        for removed_id in removed_ids:
            clip_key = self._clip_key(clip_group_prefix, removed_id)
            try:
                await self._s3_client.delete_key(clip_key)
            except Exception:
                failed_keys.append(clip_key)

        if failed_keys:
            raise ReconcileDeleteError(failed_keys=failed_keys)

        return ReconcileResult(
            updated=len(new_entries),
            removed=len(removed_ids),
        )

    async def fetch(
        self,
        *,
        clip_group: ClipGroup,
        clip_sub_group: ClipSubGroup,
    ) -> AsyncIterator[list[Clip]]:
        """Fetch clips for a clip sub-group in preserved batch order.

        The iterator yields one list per stored batch. Batches are ordered by
        increasing `batch`, and clips inside each batch are ordered by
        increasing `order`.

        Raises:
            ClipGroupNotFoundError: If the requested logical clip group has no matching clips.
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
        """
        clip_group_prefix = self._clip_group_prefix(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=None,
                scope=None,
            ) from error

        matching_entries = self._sorted_sub_group_entries(manifest, clip_sub_group)
        if not matching_entries:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=clip_sub_group.sub_season,
                scope=clip_sub_group.scope,
            )

        # `groupby()` only forms correct batch groups because entries are
        # sorted by `(batch, order)` immediately above.
        for _, batch_entries in itertools.groupby(matching_entries, key=lambda entry: entry.batch):
            clip_batch: list[Clip] = []
            for entry in batch_entries:
                clip_key = self._clip_key(clip_group_prefix, entry.id)
                clip_bytes = await self._s3_client.get_bytes(clip_key)
                clip_batch.append(Clip(filename=self._s3_key_to_filename(clip_key), bytes=clip_bytes))
            yield clip_batch

    async def list_groups(self) -> list[ClipGroup]:
        """List all discovered clip groups from stored S3 prefixes."""
        clip_group_prefixes = await self._s3_client.list_subprefixes(prefix=_CLIPS_PREFIX)
        clip_groups = [self._parse_clip_group_prefix(prefix) for prefix in clip_group_prefixes]
        return sorted(clip_groups, key=lambda group: (group.year, int(group.season), group.universe.value))

    async def list_sub_groups(self, clip_group: ClipGroup) -> list[ClipSubGroup]:
        """List unique sub-groups for a clip group from its manifest."""
        clip_group_prefix = self._clip_group_prefix(
            year=clip_group.year,
            season=clip_group.season,
            universe=clip_group.universe,
        )

        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=None,
                scope=None,
            ) from error

        sub_groups = {ClipSubGroup(entry.sub_season, entry.scope) for entry in manifest}
        return sorted(
            sub_groups,
            key=lambda sub_group: (
                sub_group.sub_season is not SubSeason.NONE,
                _sub_season_order(sub_group.sub_season),
                sub_group.scope.value,
            ),
        )

    @staticmethod
    def _sorted_sub_group_entries(manifest: Manifest, clip_sub_group: ClipSubGroup) -> list[ManifestEntry]:
        """Return sub-group entries in canonical `(batch, order)` order."""
        return sorted(
            (
                entry
                for entry in manifest
                if entry.scope is clip_sub_group.scope and entry.sub_season is clip_sub_group.sub_season
            ),
            key=lambda entry: (entry.batch, entry.order),
        )

    def _clip_group_prefix(self, *, year: int, season: Season, universe: Universe) -> Prefix:
        clip_group = _CLIP_GROUP_SEPARATOR.join((str(year), str(int(season)), universe.value))
        return S3Client.join(_CLIPS_PREFIX, clip_group)

    def _parse_clip_group_prefix(self, prefix: Prefix) -> ClipGroup:
        segments = S3Client.split(prefix)
        if not segments or segments[0] != _CLIPS_PREFIX:
            raise ValueError(f'Invalid clip group prefix {prefix!r}: expected prefix under {_CLIPS_PREFIX!r}')

        remaining_segments = segments[1:]
        if len(remaining_segments) != 1:
            raise ValueError(f'Invalid clip group prefix {prefix!r}: expected exactly one clip group segment')

        clip_group = remaining_segments[0]
        try:
            year_text, season_text, universe_text = clip_group.split(_CLIP_GROUP_SEPARATOR)
            year = int(year_text)
            season = Season(int(season_text))
            universe = Universe(universe_text)
        except ValueError as error:
            raise ValueError(f'Invalid clip group prefix {prefix!r}: malformed clip group segment') from error

        return ClipGroup(year=year, season=season, universe=universe)

    def _manifest_key(self, clip_group_prefix: Prefix) -> Key:
        return S3Client.join(clip_group_prefix, _MANIFEST_FILENAME)

    def _clip_key(self, clip_group_prefix: Prefix, clip_id: str) -> Key:
        return S3Client.join(clip_group_prefix, clip_id + _VIDEO_SUFFIX)

    def _new_clip_id(self, *, manifest: Manifest, seen_ids: set[str]) -> str:
        """Return a fresh hex UUIDv7 clip id for a newly created S3 clip object.

        Its embedded timestamp reflects when that object is created.
        """
        while True:
            clip_id = _uuid7().hex
            if not manifest.has_id(clip_id) and clip_id not in seen_ids:
                return clip_id

    def _parse_stored_clip_id(self, filename: str) -> str | None:
        parsed_identity = self._parse_filename_identity(filename)
        if parsed_identity is None:
            return None
        _, clip_id = parsed_identity
        return clip_id

    def _parse_filename_identity(self, filename: Filename) -> tuple[ClipGroup, str] | None:
        parts = S3Client.split(self._filename_to_s3_key(filename))
        if len(parts) != 3:
            return None

        top_level_prefix, clip_group, object_name = parts
        if top_level_prefix != _CLIPS_PREFIX or not object_name.endswith(_VIDEO_SUFFIX):
            return None

        try:
            parsed_clip_group = self._parse_clip_group_prefix(S3Client.join(top_level_prefix, clip_group))
            clip_id = _parse_uuid7(object_name.removesuffix(_VIDEO_SUFFIX), field='id')
        except ValueError:
            return None
        return parsed_clip_group, clip_id

    async def _load_manifest_for_store(self, clip_group_prefix: Prefix) -> Manifest:
        if (cached_manifest := self._manifest_cache.get(clip_group_prefix)) is not None:
            return cached_manifest.copy()

        try:
            manifest = await self._fetch_manifest(clip_group_prefix)
        except S3ObjectNotFoundError:
            return Manifest()

        self._manifest_cache[clip_group_prefix] = manifest
        return manifest.copy()

    async def _load_manifest_for_read(self, clip_group_prefix: Prefix) -> Manifest:
        if (cached_manifest := self._manifest_cache.get(clip_group_prefix)) is not None:
            return cached_manifest

        manifest = await self._fetch_manifest(clip_group_prefix)
        self._manifest_cache[clip_group_prefix] = manifest
        return manifest

    async def _fetch_manifest(self, clip_group_prefix: Prefix) -> Manifest:
        manifest_key = self._manifest_key(clip_group_prefix)
        raw_manifest = await self._s3_client.get_bytes(manifest_key)

        try:
            decoded_manifest = json.loads(raw_manifest.decode('utf-8'))
            return Manifest.from_list(decoded_manifest)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ManifestCorruptedError(manifest_key, str(error)) from error

    async def _rollback_uploads(self, keys: list[Key]) -> None:
        failed_keys: list[Key] = []

        for key in reversed(keys):
            try:
                await self._s3_client.delete_key(key)
            except Exception:
                failed_keys.append(key)

        if failed_keys:
            raise ClipStoreRollbackError(failed_keys=failed_keys)

    async def _hash_video_bytes(self, video_bytes: bytes) -> str:
        input_fd, input_name = tempfile.mkstemp(suffix=_VIDEO_SUFFIX)
        os.close(input_fd)
        input_path = Path(input_name)

        try:
            input_path.write_bytes(video_bytes)
            return await self._hash_video_path(input_path)
        finally:
            input_path.unlink(missing_ok=True)

    async def _hash_video_path(self, input_path: Path) -> str:
        cmd = (
            'ffmpeg',
            '-hide_banner',
            '-loglevel',
            'error',
            '-nostats',
            '-nostdin',
            '-threads',
            '1',
            '-i',
            str(input_path),
            '-map',
            '0:v:0',
            '-c:v',
            'copy',
            '-an',
            '-sn',
            '-dn',
            '-bsf:v',
            'h264_mp4toannexb',
            '-f',
            'h264',
            'pipe:1',
        )
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        if proc.stdout is None or proc.stderr is None:
            raise RuntimeError('ffmpeg subprocess did not expose stdout/stderr pipes')

        hasher = hashlib.sha256()
        try:
            _, stderr, returncode = await asyncio.wait_for(
                asyncio.gather(
                    _hash_stream(proc.stdout, hasher),
                    proc.stderr.read(),
                    proc.wait(),
                ),
                timeout=_FFMPEG_TIMEOUT.total_seconds(),
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise

        if returncode != 0:
            raise RuntimeError(f'ffmpeg failed while hashing clip: {stderr.decode(errors="replace")}')

        return hasher.hexdigest()

    @staticmethod
    def _s3_key_to_filename(storage_key: Key) -> str:
        return _FILENAME_S3_DELIMITER_ESCAPE.join(S3Client.split(storage_key))

    @staticmethod
    def _filename_to_s3_key(filename: str) -> Key:
        return S3Client.join(*filename.split(_FILENAME_S3_DELIMITER_ESCAPE))


def _uuid7() -> uuid.UUID:
    return uuid.uuid7()


def _expect_str(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f'manifest `{field}` must be a string')
    return value


def _parse_uuid7(value: str, *, field: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f'manifest `{field}` must be a valid UUID') from error
    if parsed.version != 7:
        raise ValueError(f'manifest `{field}` must be a UUIDv7')
    return parsed.hex


def _parse_sha256_hex(value: str) -> str:
    if len(value) != 64:
        raise ValueError('manifest `video_hash` must be a 64-character SHA-256 hex string')
    try:
        int(value, 16)
    except ValueError as error:
        raise ValueError('manifest `video_hash` must be hexadecimal') from error
    return value


def _parse_enum(value: object, enum_type: type[StrEnum], *, field: str) -> Any:
    if not isinstance(value, str):
        raise ValueError(f'manifest `{field}` must be a string')
    try:
        return enum_type(value)
    except ValueError as error:
        raise ValueError(f'manifest `{field}` has unsupported value: {value}') from error


def _parse_sub_season(value: object) -> SubSeason:
    return _parse_enum(value, SubSeason, field='sub_season')


def _format_sub_season(sub_season: SubSeason) -> str:
    return sub_season.value.title()


def _sub_season_order(sub_season: SubSeason) -> int:
    return _SUB_SEASON_ORDER[sub_season]


def _format_optional_sub_season(sub_season: SubSeason | None) -> str:
    if sub_season is None:
        return 'None'
    return _format_sub_season(sub_season)


def _format_scope(scope: Scope | None) -> str:
    return 'None' if scope is None else scope.value


async def _hash_stream(stream: asyncio.StreamReader, hasher: Any) -> None:
    while chunk := await stream.read(_HASH_READ_SIZE):
        hasher.update(chunk)
