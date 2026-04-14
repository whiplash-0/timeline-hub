import itertools
import json
import math
import uuid
from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from enum import IntEnum, StrEnum
from typing import Any, Self, TypeVar

from loguru import logger

from timeline_hub.infra.ffmpeg import hash_video_content, normalize_video_audio_loudness
from timeline_hub.infra.s3 import Key, Prefix, S3Client, S3ContentType, S3ObjectNotFoundError
from timeline_hub.types import Extension, FileBytes, InvalidExtensionError

_CLIPS_PREFIX = 'clips'
_MANIFEST_FILENAME = 'manifest.json'
_CLIP_GROUP_SEPARATOR = '-'
_NORMALIZED_SUFFIX = '-normalized'

type ClipId = str


def _require_extension(file: FileBytes, expected: Extension, field: str) -> None:
    """Require one explicit `FileBytes` extension at a public API boundary."""
    if file.extension is not expected:
        raise InvalidExtensionError(f'{field} must use Extension.{expected.name}')


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

    def order(self) -> int:
        return tuple(type(self)).index(self)


@dataclass(frozen=True, slots=True)
class ClipGroup:
    """Logical clip-group identifier."""

    universe: Universe
    year: int
    season: Season


class SubSeason(StrEnum):
    """Clip sub-season identifier."""

    NONE = 'none'
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'

    def order(self) -> int:
        return tuple(type(self)).index(self)


class Scope(StrEnum):
    """Clip scope identifier."""

    COLLECTION = 'collection'
    EXTRA = 'extra'
    SOURCE = 'source'


@dataclass(frozen=True, slots=True)
class ClipSubGroup:
    """Logical clip sub-group identifier."""

    sub_season: SubSeason
    scope: Scope


@dataclass(frozen=True, slots=True)
class ClipInfo:
    """Minimal read model for a persisted clip."""

    id: ClipId


@dataclass(frozen=True, slots=True)
class FetchedClip:
    """Fetched clip payload identified only by clip id."""

    id: ClipId
    file: FileBytes


@dataclass(frozen=True, slots=True)
class AudioNormalization:
    """Audio-normalization settings applied during clip fetch.

    Normalization produces MP4 output.
    """

    loudness: float
    bitrate: int

    def __post_init__(self) -> None:
        if isinstance(self.loudness, bool) or not isinstance(self.loudness, int | float):
            raise ValueError('`loudness` must be a numeric value')
        if not math.isfinite(self.loudness):
            raise ValueError('`loudness` must be finite')
        if isinstance(self.bitrate, bool) or not isinstance(self.bitrate, int):
            raise ValueError('`bitrate` must be an integer')
        if self.bitrate < 1:
            raise ValueError('`bitrate` must be >= 1')


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """Single persisted clip row in a clip-group manifest.

    `audio_normalization` records the currently authoritative normalized-cache
    parameters for this clip. `None` means no normalized clip is tracked in
    authoritative state, even if an untracked stale normalized object still
    exists in storage after a failed cache write.
    """

    id: ClipId
    video_hash: str
    sub_season: SubSeason
    scope: Scope
    batch: int
    order: int
    audio_normalization: AudioNormalization | None = None


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

    def has_id(self, clip_id: ClipId) -> bool:
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

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        """Convert the manifest into its JSON-compatible shape."""
        return {
            'data': [
                {
                    'id': entry.id,
                    'video_hash': entry.video_hash,
                    'audio_normalization': (
                        None
                        if entry.audio_normalization is None
                        else {
                            'loudness': entry.audio_normalization.loudness,
                            'bitrate': entry.audio_normalization.bitrate,
                        }
                    ),
                    'sub_season': entry.sub_season.value,
                    'scope': entry.scope.value,
                    'batch': entry.batch,
                    'order': entry.order,
                }
                for entry in self._entries
            ]
        }

    @classmethod
    def from_dict(cls, data: object) -> Self:
        """Build a manifest from a decoded JSON payload.

        Args:
            data: Decoded JSON value from `manifest.json`.

        Raises:
            ValueError: If the payload does not match the manifest schema.
        """
        if not isinstance(data, dict):
            raise ValueError("manifest root must be an object with only 'data'")
        if set(data) != {'data'}:
            raise ValueError("manifest root must be an object with only 'data'")

        raw_entries = data['data']
        if not isinstance(raw_entries, list):
            raise ValueError("manifest 'data' must be a list")

        entries: list[ManifestEntry] = []
        seen_ids: set[ClipId] = set()
        seen_hashes: set[str] = set()
        seen_positions: set[tuple[SubSeason, Scope, int, int]] = set()

        for raw_entry in raw_entries:
            if not isinstance(raw_entry, dict):
                raise ValueError('manifest clip entry must be an object')
            if set(raw_entry) != {
                'id',
                'video_hash',
                'audio_normalization',
                'sub_season',
                'scope',
                'batch',
                'order',
            }:
                raise ValueError('manifest clip entry has unexpected fields')

            clip_id = _parse_uuid7(_expect_str(raw_entry['id'], field='id'), field='id')
            video_hash = _parse_sha256_hex(_expect_str(raw_entry['video_hash'], field='video_hash'))
            audio_normalization = _parse_audio_normalization(raw_entry['audio_normalization'])
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
                    audio_normalization=audio_normalization,
                    sub_season=sub_season,
                    scope=scope,
                    batch=batch,
                    order=order,
                )
            )

        return cls(entries)


@dataclass(frozen=True, slots=True)
class StoreResult:
    """Result summary for a `store()` call.

    `clip_ids` contains only the ids of clips stored within a single
    `store()` call for the specific `(clip_group, clip_sub_group)` used by
    that call. These ids are subgroup-local identifiers and must not be
    interpreted outside that subgroup.

    Concatenating results via `__add__` preserves per-call ordering, but is
    semantically meaningful only when combining results from the same clip
    subgroup. The concatenated ids must not be treated as a globally
    meaningful ordered sequence across independent store calls.
    """

    stored_count: int
    duplicate_count: int
    clip_ids: tuple[ClipId, ...] = ()

    def __add__(self, other: Self) -> Self:
        if not isinstance(other, type(self)):
            return NotImplemented
        return type(self)(
            stored_count=self.stored_count + other.stored_count,
            duplicate_count=self.duplicate_count + other.duplicate_count,
            clip_ids=self.clip_ids + other.clip_ids,
        )


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """Result summary for a `reconcile()` call."""

    updated: int
    removed: int


class UnknownClipsError(ValueError):
    """Raised when reconcile filenames refer to clip ids missing from the manifest."""

    def __init__(self, *, clip_ids: Sequence[ClipId]) -> None:
        self.clip_ids = tuple(clip_ids)
        super().__init__(f'Clip ids are not present in manifest: {list(self.clip_ids)}')


class DuplicateClipIdsError(ValueError):
    """Raised when clip-id inputs contain duplicates."""

    def __init__(self, *, clip_ids: Sequence[ClipId]) -> None:
        self.clip_ids = tuple(clip_ids)
        super().__init__(f'Clip ids contain duplicates: {list(self.clip_ids)}')


class ClipIdsNotInSubGroupError(ValueError):
    """Raised when `fetch()` receives clip ids outside the requested subgroup."""

    def __init__(self, *, clip_ids: Sequence[ClipId]) -> None:
        self.clip_ids = tuple(clip_ids)
        super().__init__(f'Clip ids are not present in requested sub-group: {list(self.clip_ids)}')


class InvalidClipIdentityError(ValueError):
    """Raised when an encoded clip identity string is invalid."""


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
        universe: Universe,
        year: int,
        season: Season,
        sub_season: SubSeason | None,
        scope: Scope | None,
    ) -> None:
        self.universe = universe
        self.year = year
        self.season = season
        self.sub_season = sub_season
        self.scope = scope
        super().__init__(
            f'No clips found for {universe.value}-{year}-{int(season)} '
            f'sub_season={_format_optional_sub_season(sub_season)} scope={_format_scope(scope)}'
        )


class ClipManifestSyncError(RuntimeError):
    """Explicit manual-recovery error for staged store failures without rollback.

    Rollback is intentionally not attempted. `stage`, `written_keys`,
    `affected_clip_ids`, and `manifest_key` provide the intended recovery
    context for investigation and manual cleanup.
    """

    def __init__(
        self,
        *,
        stage: str,
        written_keys: Sequence[Key],
        affected_clip_ids: Sequence[ClipId],
        manifest_key: Key,
    ) -> None:
        self.stage = stage
        self.written_keys = tuple(written_keys)
        self.affected_clip_ids = tuple(affected_clip_ids)
        self.manifest_key = manifest_key
        super().__init__(
            f'Staged clip store failed at {self.stage} for clip ids {list(self.affected_clip_ids)}; '
            f'written keys: {list(self.written_keys)}; '
            f'manifest key not synchronized: {self.manifest_key}'
        )


class ReconcileDeleteError(RuntimeError):
    """Raised when reconcile cannot fully delete removed clip objects."""

    def __init__(self, *, failed_keys: Sequence[Key]) -> None:
        self.failed_keys = tuple(failed_keys)
        super().__init__(f'Reconcile cleanup failed for {len(self.failed_keys)} removed keys: {list(self.failed_keys)}')


class NormalizedClipManifestSyncError(RuntimeError):
    """Raised when normalized clip uploads cannot be synchronized with manifest state."""

    def __init__(
        self,
        *,
        written_keys: Sequence[Key],
        affected_clip_ids: Sequence[ClipId],
        stage: str,
    ) -> None:
        self.written_keys = tuple(written_keys)
        self.affected_clip_ids = tuple(affected_clip_ids)
        self.stage = stage
        super().__init__(
            'Normalized clip/manifest synchronization failed '
            f'at stage={self.stage} for clip ids {list(self.affected_clip_ids)} '
            f'after writing normalized keys {list(self.written_keys)}'
        )


class ClipRemoveManifestSyncError(RuntimeError):
    """Raised when staged clip removals are not synchronized back into manifest state."""

    def __init__(
        self,
        *,
        stage: str,
        clip_ids: Sequence[ClipId],
        touched_keys: Sequence[Key],
        manifest_key: Key,
    ) -> None:
        self.stage = stage
        self.clip_ids = tuple(clip_ids)
        self.touched_keys = tuple(touched_keys)
        self.manifest_key = manifest_key
        super().__init__(
            f'Staged clip remove failed at {self.stage} for clip ids {list(self.clip_ids)}; '
            f'touched keys: {list(self.touched_keys)}; '
            f'manifest key not synchronized: {self.manifest_key}'
        )


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
    listing is manifest-based. Store operations stage clip uploads first and
    commit by writing the updated manifest. If a later store stage fails after
    one or more clip uploads succeed, this module prefers fail-fast explicit
    sync errors over rollback complexity. Uploaded orphaned clip objects may
    remain in storage until manual cleanup, while the manifest remains the
    only authoritative logical clip state. Manual cleanup after such failures
    is an expected operational responsibility.

    Normalized clips are cache-like derived artifacts, not authoritative
    storage state. The manifest is the source of truth for whether a
    normalized clip is tracked for a clip id; untracked normalized objects may
    still exist in storage temporarily after failed cache writes and are
    treated as stale cache.

    Clips are stored as `.mp4` objects with MIME type `video/mp4`.
    `ClipStore` validates MP4 `FileBytes` at the public boundary, then keeps
    internal processing byte-based. All returned clip files are MP4.
    """

    def __init__(self, s3_client: S3Client) -> None:
        """Initialize the store with an opened generic S3 client."""
        self._s3_client = s3_client
        self._manifest_cache: dict[Prefix, Manifest] = {}

    async def list_groups(self) -> list[ClipGroup]:
        """List discovered clip groups from stored S3 prefixes.

        This is prefix-based storage discovery, not a manifest-authoritative
        existence check. After failed first writes, orphaned prefixes may
        appear temporarily until manual cleanup removes them.
        """
        clip_group_prefixes = await self._s3_client.list_subprefixes(prefix=_CLIPS_PREFIX)
        clip_groups = [self._parse_clip_group_prefix(prefix) for prefix in clip_group_prefixes]
        return sorted(clip_groups, key=lambda group: (group.universe.order(), group.year, int(group.season)))

    async def list_clips(self, group: ClipGroup) -> dict[ClipSubGroup, list[tuple[ClipInfo, ...]]]:
        """List clips grouped by sub-group for a clip group from its manifest."""
        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )

        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        sorted_sub_groups = sorted(
            {ClipSubGroup(entry.sub_season, entry.scope) for entry in manifest},
            key=lambda sub_group: (
                sub_group.sub_season.order(),
                sub_group.scope.value,
            ),
        )
        clips_by_sub_group: dict[ClipSubGroup, list[tuple[ClipInfo, ...]]] = {}
        for sub_group in sorted_sub_groups:
            batches: list[tuple[ClipInfo, ...]] = []
            current_batch: int | None = None
            batch_clips: list[ClipInfo] = []
            for entry in self._sorted_sub_group_entries(manifest, sub_group):
                if current_batch != entry.batch:
                    if batch_clips:
                        batches.append(tuple(batch_clips))
                    current_batch = entry.batch
                    batch_clips = []
                batch_clips.append(ClipInfo(id=entry.id))
            if batch_clips:
                batches.append(tuple(batch_clips))
            clips_by_sub_group[sub_group] = batches

        return clips_by_sub_group

    async def fetch(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        clip_ids: Sequence[ClipId] | None = None,
        audio_normalization: AudioNormalization | None = None,
    ) -> AsyncIterator[tuple[FetchedClip, ...]]:
        """Fetch clips for a clip sub-group in preserved batch order.

        The iterator yields one immutable tuple snapshot per stored batch.
        Batches are ordered by increasing `batch`, and clips inside each
        batch are ordered by increasing `order`. When `clip_ids` is provided,
        validation and resolution are strictly local to the provided `(clip_group,
        clip_sub_group)`. No cross-group or cross-subgroup lookup is
        performed. Unknown ids are ids not present in the manifest of the
        provided `clip_group`.

        Validation precedence for `clip_ids` is:
        1. Duplicate ids.
        2. Ids missing from the clip-group manifest.
        3. Ids not present in the requested subgroup.

        Filtered results preserve the subgroup's canonical current manifest
        order.

        Returned `FetchedClip.file` values are always MP4, for both raw
        fetches and audio-normalized fetches.

        Raises:
            ClipGroupNotFoundError: If the requested logical clip group has no matching clips.
            DuplicateClipIdsError: If `clip_ids` contains duplicates.
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
            UnknownClipsError: If `clip_ids` contains ids missing from the provided clip-group manifest.
            ClipIdsNotInSubGroupError: If `clip_ids` contains ids outside the requested subgroup.
        """
        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        matching_entries = self._sorted_sub_group_entries(manifest, sub_group)
        if clip_ids is None:
            if not matching_entries:
                raise ClipGroupNotFoundError(
                    universe=group.universe,
                    year=group.year,
                    season=group.season,
                    sub_season=sub_group.sub_season,
                    scope=sub_group.scope,
                )
        else:
            if len(set(clip_ids)) != len(clip_ids):
                raise DuplicateClipIdsError(clip_ids=clip_ids)

            manifest_ids = {entry.id for entry in manifest}
            unknown_ids = [clip_id for clip_id in clip_ids if clip_id not in manifest_ids]
            if unknown_ids:
                raise UnknownClipsError(clip_ids=unknown_ids)

            matching_ids = {entry.id for entry in matching_entries}
            non_matching_ids = [clip_id for clip_id in clip_ids if clip_id not in matching_ids]
            if non_matching_ids:
                raise ClipIdsNotInSubGroupError(clip_ids=non_matching_ids)

            requested_ids = set(clip_ids)
            # Output always follows current manifest ordering after any
            # compaction; input `clip_ids` order does not affect it.
            matching_entries = [entry for entry in matching_entries if entry.id in requested_ids]

        # `groupby()` only forms correct batch groups because entries are
        # sorted by `(batch, order)` immediately above.
        for _, batch_entries in itertools.groupby(matching_entries, key=lambda entry: entry.batch):
            batch_entries_list = list(batch_entries)
            if audio_normalization is None:
                clip_batch: list[FetchedClip] = []
                for entry in batch_entries_list:
                    clip_key = self._clip_key(clip_group_prefix, entry.id)
                    clip_bytes = await self._s3_client.get_bytes(clip_key)
                    clip_batch.append(
                        FetchedClip(
                            id=entry.id,
                            file=FileBytes(data=clip_bytes, extension=Extension.MP4),
                        )
                    )
                yield tuple(clip_batch)
                continue

            clip_batch, manifest = await self._fetch_normalized_batch(
                clip_group_prefix,
                manifest,
                batch_entries_list,
                audio_normalization=audio_normalization,
            )
            yield tuple(clip_batch)

    async def store(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        clips: Sequence[FileBytes],
    ) -> StoreResult:
        """Store one logical clip batch with clip-group-local deduplication.

        One `store()` call maps to exactly one batch within the target
        `ClipSubGroup`. Accepted clips keep their input order and receive
        dense `order` values starting at `1`. If every clip in the call is a
        duplicate, the method returns without creating a new batch. Callers
        must not mix clips from multiple logical batches in a single call.

        Clip uploads are staged before manifest persistence, and the manifest
        remains authoritative for whether stored clips exist in logical state.
        `store()` intentionally does not attempt rollback if a later stage
        fails. Instead, `ClipManifestSyncError` is the intended recovery
        surface for manual investigation and cleanup. Later-stage failures may
        leave uploaded clip objects in storage without manifest entries, and
        callers are expected to use the exception's attached context to clean
        up manually when needed.

        Writes to the same `ClipGroup` are assumed to be sequential
        (single-writer). Concurrent writes are not supported and may lead to
        manifest overwrite and orphaned clips.

        `clips` must contain MP4 `FileBytes`. `store()` validates the
        explicit extension at the boundary, then unwraps to raw bytes for the
        rest of the storage pipeline.

        Raises:
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
            ClipManifestSyncError: If one or more clip objects are written but a later stage fails.
            RuntimeError: If clip hashing fails.
        """
        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        manifest = await self._load_manifest_for_store(clip_group_prefix)
        seen_hashes: set[str] = set()
        seen_ids: set[ClipId] = set()
        duplicate_count = 0
        accepted_clips: list[tuple[ClipId, str, bytes]] = []
        uploaded_keys: list[Key] = []
        for clip_file in clips:
            if not isinstance(clip_file, FileBytes):
                raise ValueError('clips entries must be FileBytes instances')
            _require_extension(clip_file, Extension.MP4, 'clips entries')
            clip_bytes = clip_file.data
            video_hash = await hash_video_content(clip_bytes)
            if manifest.has_video_hash(video_hash) or video_hash in seen_hashes:
                duplicate_count += 1
                continue

            clip_id = self._new_clip_id(manifest=manifest, seen_ids=seen_ids)
            accepted_clips.append((clip_id, video_hash, clip_bytes))
            seen_ids.add(clip_id)
            seen_hashes.add(video_hash)

        if not accepted_clips:
            return StoreResult(stored_count=0, duplicate_count=duplicate_count)

        batch = manifest.next_batch(
            sub_season=sub_group.sub_season,
            scope=sub_group.scope,
        )
        new_entries: list[tuple[ManifestEntry, bytes]] = []
        for order, (clip_id, video_hash, clip_bytes) in enumerate(accepted_clips, start=1):
            entry = ManifestEntry(
                id=clip_id,
                video_hash=video_hash,
                audio_normalization=None,
                sub_season=sub_group.sub_season,
                scope=sub_group.scope,
                batch=batch,
                order=order,
            )
            manifest.append(entry)
            new_entries.append((entry, clip_bytes))

        manifest_key = self._manifest_key(clip_group_prefix)

        for entry, clip_bytes in new_entries:
            clip_key = self._clip_key(clip_group_prefix, entry.id)
            try:
                await self._s3_client.put_bytes(
                    clip_key,
                    clip_bytes,
                    content_type=S3ContentType.MP4,
                )
            except Exception as error:
                if not uploaded_keys:
                    raise
                sync_error = ClipManifestSyncError(
                    stage='clip_upload',
                    written_keys=uploaded_keys,
                    affected_clip_ids=[written_entry.id for written_entry, _ in new_entries[: len(uploaded_keys)]],
                    manifest_key=manifest_key,
                )
                sync_error.add_note(f'Original clip upload error: {error!r}')
                raise sync_error from error
            uploaded_keys.append(clip_key)

        try:
            await self._write_manifest_and_update_cache(
                clip_group_prefix=clip_group_prefix,
                manifest=manifest,
            )
        except Exception as error:
            sync_error = ClipManifestSyncError(
                stage='manifest_write',
                written_keys=uploaded_keys,
                affected_clip_ids=[entry.id for entry, _ in new_entries],
                manifest_key=manifest_key,
            )
            sync_error.add_note(f'Original manifest write error: {error!r}')
            raise sync_error from error

        return StoreResult(
            stored_count=len(new_entries),
            duplicate_count=duplicate_count,
            clip_ids=tuple(entry.id for entry, _ in new_entries),
        )

    async def compact(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
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
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        target_entries = self._sorted_sub_group_entries(manifest, sub_group)
        if not target_entries:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=sub_group.sub_season,
                scope=sub_group.scope,
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
            if entry.scope is sub_group.scope and entry.sub_season is sub_group.sub_season:
                compacted_batch, compacted_order = compacted_positions[entry.id]
                rewritten_entries.append(
                    ManifestEntry(
                        id=entry.id,
                        video_hash=entry.video_hash,
                        audio_normalization=entry.audio_normalization,
                        sub_season=entry.sub_season,
                        scope=entry.scope,
                        batch=compacted_batch,
                        order=compacted_order,
                    )
                )
            else:
                rewritten_entries.append(entry)

        rewritten_manifest = Manifest(rewritten_entries)

        # Compaction is manifest-only; clip objects stay untouched.
        await self._write_manifest_and_update_cache(
            clip_group_prefix=clip_group_prefix,
            manifest=rewritten_manifest,
        )

    async def reorder(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        clip_id_batches: Sequence[Sequence[ClipId]],
    ) -> None:
        """Rewrite the authoritative batch layout for exactly one existing sub-group."""
        clip_ids = self._flatten_clip_id_batches(
            clip_id_batches,
            operation='reorder()',
        )
        if len(set(clip_ids)) != len(clip_ids):
            raise DuplicateClipIdsError(clip_ids=clip_ids)

        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        entries_by_id = {entry.id: entry for entry in manifest}
        unknown_ids = [clip_id for clip_id in clip_ids if clip_id not in entries_by_id]
        if unknown_ids:
            raise UnknownClipsError(clip_ids=unknown_ids)

        target_entries = self._sorted_sub_group_entries(manifest, sub_group)
        target_clip_ids = {entry.id for entry in target_entries}
        non_matching_ids = [clip_id for clip_id in clip_ids if clip_id not in target_clip_ids]
        if non_matching_ids:
            raise ClipIdsNotInSubGroupError(clip_ids=non_matching_ids)

        if target_clip_ids != set(clip_ids):
            raise ValueError('reorder() clip_id_batches must match exactly the full set of clip ids in the sub-group')

        rewritten_entries_by_id: dict[ClipId, ManifestEntry] = {}
        for batch_index, clip_id_batch in enumerate(clip_id_batches, start=1):
            for order_index, clip_id in enumerate(clip_id_batch, start=1):
                rewritten_entries_by_id[clip_id] = dataclass_replace(
                    entries_by_id[clip_id],
                    batch=batch_index,
                    order=order_index,
                )

        rewritten_manifest = Manifest([rewritten_entries_by_id.get(entry.id, entry) for entry in manifest])

        await self._write_manifest_and_update_cache(
            clip_group_prefix=clip_group_prefix,
            manifest=rewritten_manifest,
        )

    async def move(
        self,
        group: ClipGroup,
        *,
        target_sub_group: ClipSubGroup,
        clip_id_batches: Sequence[Sequence[ClipId]],
    ) -> None:
        """Move existing clips into one target sub-group without mutating clip objects."""
        clip_ids = self._flatten_clip_id_batches(
            clip_id_batches,
            operation='move()',
        )
        if len(set(clip_ids)) != len(clip_ids):
            raise DuplicateClipIdsError(clip_ids=clip_ids)

        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        entries_by_id = {entry.id: entry for entry in manifest}
        unknown_ids = [clip_id for clip_id in clip_ids if clip_id not in entries_by_id]
        if unknown_ids:
            raise UnknownClipsError(clip_ids=unknown_ids)

        moved_entries = [entries_by_id[clip_id] for clip_id in clip_ids]
        if any(
            entry.sub_season == target_sub_group.sub_season and entry.scope == target_sub_group.scope
            for entry in moved_entries
        ):
            raise ValueError(
                'move() only supports actual cross-sub-group moves; target sub-group clips must not be included'
            )

        moved_clip_ids = set(clip_ids)
        rewritten_entries_by_id: dict[ClipId, ManifestEntry] = {}
        affected_source_sub_groups = {ClipSubGroup(entry.sub_season, entry.scope) for entry in moved_entries}

        target_entries = self._sorted_sub_group_entries(manifest, target_sub_group)
        next_batch = max((entry.batch for entry in target_entries), default=0) + 1
        for batch_offset, clip_id_batch in enumerate(clip_id_batches):
            for order_index, clip_id in enumerate(clip_id_batch, start=1):
                rewritten_entries_by_id[clip_id] = dataclass_replace(
                    entries_by_id[clip_id],
                    sub_season=target_sub_group.sub_season,
                    scope=target_sub_group.scope,
                    batch=next_batch + batch_offset,
                    order=order_index,
                )

        for source_sub_group in affected_source_sub_groups:
            remaining_entries = [
                entry
                for entry in self._sorted_sub_group_entries(manifest, source_sub_group)
                if entry.id not in moved_clip_ids
            ]
            rewritten_entries_by_id.update(self._build_dense_sub_group_entries(remaining_entries))

        rewritten_manifest = Manifest([rewritten_entries_by_id.get(entry.id, entry) for entry in manifest])

        await self._write_manifest_and_update_cache(
            clip_group_prefix=clip_group_prefix,
            manifest=rewritten_manifest,
        )

    async def reconcile(
        self,
        group: ClipGroup,
        sub_group: ClipSubGroup,
        *,
        clip_id_batches: Sequence[Sequence[ClipId]],
    ) -> ReconcileResult:
        """Replace one sub-group with the provided clip-id manifest state.

        Reconcile is manifest-authoritative for the target `ClipSubGroup`.
        The provided `clip_id_batches` define the complete desired subgroup
        state: their order becomes canonical, omitted clips are removed from
        that subgroup, and clips may be moved in from other sub-groups within
        the same `ClipGroup`. The operation never hashes, uploads, downloads,
        or rewrites clip bytes. It only rewrites the manifest and deletes clip
        objects that become unreachable from the final manifest.

        The manifest remains authoritative for normalized-cache tracking.
        Untracked normalized objects are treated as stale cache, not storage state.

        Raises:
            ValueError: If `clip_id_batches` is empty, contains empty batches, or otherwise contains no clip ids.
            DuplicateClipIdsError: If the provided clip ids contain duplicates.
            UnknownClipsError: If a parsed clip id is not present in the provided clip group's manifest.
            ClipGroupNotFoundError: If the requested clip group has no manifest.
            ManifestCorruptedError: If the clip-group manifest exists but is malformed.
            ReconcileDeleteError: If one or more removed clip objects cannot be deleted after the manifest rewrite.
        """
        clip_ids = self._flatten_clip_id_batches(
            clip_id_batches,
            operation='reconcile()',
        )
        if len(set(clip_ids)) != len(clip_ids):
            raise DuplicateClipIdsError(clip_ids=clip_ids)

        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        entries_by_id = {entry.id: entry for entry in manifest}
        unknown_ids = [clip_id for clip_id in clip_ids if clip_id not in entries_by_id]
        if unknown_ids:
            raise UnknownClipsError(clip_ids=unknown_ids)

        existing_target_entries = [
            entry for entry in manifest if entry.scope is sub_group.scope and entry.sub_season is sub_group.sub_season
        ]
        old_subgroup_ids = {entry.id for entry in existing_target_entries}

        new_entries: list[ManifestEntry] = []
        new_subgroup_ids: set[ClipId] = set()
        for batch_index, clip_id_batch in enumerate(clip_id_batches, start=1):
            for order_index, clip_id in enumerate(clip_id_batch, start=1):
                existing_entry = entries_by_id[clip_id]
                new_entries.append(
                    ManifestEntry(
                        id=clip_id,
                        video_hash=existing_entry.video_hash,
                        audio_normalization=existing_entry.audio_normalization,
                        sub_season=sub_group.sub_season,
                        scope=sub_group.scope,
                        batch=batch_index,
                        order=order_index,
                    )
                )
                new_subgroup_ids.add(clip_id)

        rewritten_entries: list[ManifestEntry] = []
        for entry in manifest:
            if entry.scope is sub_group.scope and entry.sub_season is sub_group.sub_season:
                continue
            if entry.id in new_subgroup_ids:
                continue
            rewritten_entries.append(entry)

        rewritten_entries.extend(new_entries)
        rewritten_manifest = Manifest(rewritten_entries)
        await self._write_manifest_and_update_cache(
            clip_group_prefix=clip_group_prefix,
            manifest=rewritten_manifest,
        )

        # The manifest is authoritative, so cache must track the rewritten
        # manifest even if deleting removed clip objects fails afterwards.

        removed_ids = old_subgroup_ids - new_subgroup_ids
        failed_keys: list[Key] = []
        for removed_id in removed_ids:
            removed_entry = entries_by_id[removed_id]
            raw_clip_key = self._clip_key(clip_group_prefix, removed_id)
            try:
                await self._s3_client.delete_key(raw_clip_key)
            except Exception as exc:
                logger.error('Failed to delete key {}: {}', raw_clip_key, exc)
                failed_keys.append(raw_clip_key)
            if removed_entry.audio_normalization is not None:
                normalized_clip_key = self._normalized_clip_key(clip_group_prefix, removed_id)
                try:
                    await self._s3_client.delete_key(normalized_clip_key)
                except Exception as exc:
                    logger.error('Failed to delete key {}: {}', normalized_clip_key, exc)
                    failed_keys.append(normalized_clip_key)

        if failed_keys:
            raise ReconcileDeleteError(failed_keys=failed_keys)

        return ReconcileResult(
            updated=len(new_entries),
            removed=len(removed_ids),
        )

    async def remove(
        self,
        group: ClipGroup,
        *,
        clip_ids: Sequence[ClipId],
    ) -> None:
        """Remove many clips plus their authoritative stored objects."""
        if not clip_ids:
            raise ValueError('remove() requires at least one clip id')
        if len(set(clip_ids)) != len(clip_ids):
            raise DuplicateClipIdsError(clip_ids=clip_ids)

        clip_group_prefix = self._clip_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            manifest = await self._load_manifest_for_read(clip_group_prefix)
        except S3ObjectNotFoundError as error:
            raise ClipGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=None,
                scope=None,
            ) from error

        entries_by_id = {entry.id: entry for entry in manifest}
        unknown_ids = [clip_id for clip_id in clip_ids if clip_id not in entries_by_id]
        if unknown_ids:
            raise UnknownClipsError(clip_ids=unknown_ids)

        removed_entries = [entries_by_id[clip_id] for clip_id in clip_ids]
        manifest_key = self._manifest_key(clip_group_prefix)
        touched_keys: list[Key] = []

        for entry in removed_entries:
            raw_clip_key = self._clip_key(clip_group_prefix, entry.id)
            try:
                await self._s3_client.delete_key(raw_clip_key)
            except Exception as error:
                if (
                    sync_error := self._build_remove_sync_error(
                        error,
                        stage='raw_clip_delete',
                        clip_ids=clip_ids,
                        touched_keys=touched_keys,
                        assume_touched_keys=[raw_clip_key],
                        manifest_key=manifest_key,
                        note_prefix='Raw clip delete error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.append(raw_clip_key)

            if entry.audio_normalization is None:
                continue

            normalized_clip_key = self._normalized_clip_key(clip_group_prefix, entry.id)
            try:
                await self._s3_client.delete_key(normalized_clip_key)
            except Exception as error:
                if (
                    sync_error := self._build_remove_sync_error(
                        error,
                        stage='normalized_clip_delete',
                        clip_ids=clip_ids,
                        touched_keys=touched_keys,
                        assume_touched_keys=[normalized_clip_key],
                        manifest_key=manifest_key,
                        note_prefix='Normalized clip delete error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.append(normalized_clip_key)

        removed_clip_ids = set(clip_ids)
        remaining_entries = [entry for entry in manifest if entry.id not in removed_clip_ids]
        remaining_manifest = Manifest(remaining_entries)
        rewritten_entries_by_id: dict[ClipId, ManifestEntry] = {}
        affected_sub_groups = {ClipSubGroup(entry.sub_season, entry.scope) for entry in removed_entries}
        for sub_group in affected_sub_groups:
            sub_group_entries = self._sorted_sub_group_entries(remaining_manifest, sub_group)
            rewritten_entries_by_id.update(self._build_dense_sub_group_entries(sub_group_entries))

        rewritten_manifest = Manifest([rewritten_entries_by_id.get(entry.id, entry) for entry in remaining_entries])

        if not remaining_entries:
            try:
                await self._s3_client.delete_key(manifest_key)
            except Exception as error:
                self._manifest_cache.pop(clip_group_prefix, None)
                sync_error = self._build_remove_sync_error(
                    error,
                    stage='manifest_delete',
                    clip_ids=clip_ids,
                    touched_keys=touched_keys,
                    assume_touched_keys=[manifest_key],
                    manifest_key=manifest_key,
                    note_prefix='Remove manifest delete error',
                )
                if sync_error is None:
                    raise
                raise sync_error from error
            self._manifest_cache.pop(clip_group_prefix, None)
            return

        try:
            await self._write_manifest_and_update_cache(
                clip_group_prefix=clip_group_prefix,
                manifest=rewritten_manifest,
            )
        except Exception as error:
            sync_error = ClipRemoveManifestSyncError(
                stage='manifest_write',
                clip_ids=clip_ids,
                touched_keys=touched_keys,
                manifest_key=manifest_key,
            )
            sync_error.add_note(f'Remove manifest write error: {error!r}')
            raise sync_error from error

    @staticmethod
    def clip_identity_to_string(
        group: ClipGroup,
        sub_group: ClipSubGroup,
        clip_id: ClipId,
    ) -> str:
        """Encode logical clip identity as a strict flat string.

        This is a logical identity string only. It is not an S3 key or
        filename abstraction and carries no path or extension handling.
        """
        clip_id = _parse_uuid7(clip_id, field='id')
        clip_group = _CLIP_GROUP_SEPARATOR.join((group.universe.value, str(group.year), str(int(group.season))))
        clip_sub_group = _CLIP_GROUP_SEPARATOR.join((sub_group.sub_season.value, sub_group.scope.value))
        return '--'.join((clip_group, clip_sub_group, clip_id))

    @staticmethod
    def string_to_clip_identity(
        value: str,
    ) -> tuple[ClipGroup, ClipSubGroup, ClipId]:
        """Decode a strict logical clip identity string.

        The input must match the exact logical identity format produced by
        `clip_identity_to_string()`. It is not an S3 key or filename
        abstraction, and this parser does not tolerate paths or extensions.
        """
        if not isinstance(value, str):
            raise InvalidClipIdentityError('clip identity `value` must be a string')
        if '/' in value:
            raise InvalidClipIdentityError('clip identity `value` must not contain path separators')
        if '.' in value:
            raise InvalidClipIdentityError('clip identity `value` must not contain extensions')

        parts = value.split('--')
        if len(parts) != 3:
            raise InvalidClipIdentityError("clip identity `value` must contain exactly two '--' separators")

        group_text, sub_group_text, clip_id_text = parts
        if not group_text or not sub_group_text or not clip_id_text:
            raise InvalidClipIdentityError("clip identity `value` must contain exactly two '--' separators")

        try:
            universe_text, year_text, season_text = group_text.split(_CLIP_GROUP_SEPARATOR)
        except ValueError as error:
            raise InvalidClipIdentityError('clip identity `value` has malformed group segment') from error

        try:
            sub_season_text, scope_text = sub_group_text.split(_CLIP_GROUP_SEPARATOR)
        except ValueError as error:
            raise InvalidClipIdentityError('clip identity `value` has malformed sub-group segment') from error

        try:
            universe = Universe(universe_text)
        except ValueError as error:
            raise InvalidClipIdentityError(
                f'clip identity `value` has unsupported universe: {universe_text}'
            ) from error

        try:
            year = int(year_text)
        except ValueError as error:
            raise InvalidClipIdentityError('clip identity `value` has invalid year') from error

        try:
            season = Season(int(season_text))
        except ValueError as error:
            raise InvalidClipIdentityError('clip identity `value` has invalid season') from error

        try:
            sub_season = SubSeason(sub_season_text)
        except ValueError as error:
            raise InvalidClipIdentityError(
                f'clip identity `value` has unsupported sub_season: {sub_season_text}'
            ) from error

        try:
            scope = Scope(scope_text)
        except ValueError as error:
            raise InvalidClipIdentityError(f'clip identity `value` has unsupported scope: {scope_text}') from error

        try:
            clip_id = _parse_uuid7(clip_id_text, field='id')
        except ValueError as error:
            message = str(error).replace('manifest `id`', 'clip identity `value`')
            raise InvalidClipIdentityError(message) from error

        return (
            ClipGroup(universe=universe, year=year, season=season),
            ClipSubGroup(sub_season=sub_season, scope=scope),
            clip_id,
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

    def _clip_group_prefix(self, *, universe: Universe, year: int, season: Season) -> Prefix:
        clip_group = _CLIP_GROUP_SEPARATOR.join((universe.value, str(year), str(int(season))))
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
            universe_text, year_text, season_text = clip_group.split(_CLIP_GROUP_SEPARATOR)
            universe = Universe(universe_text)
            year = int(year_text)
            season = Season(int(season_text))
        except ValueError as error:
            raise ValueError(f'Invalid clip group prefix {prefix!r}: malformed clip group segment') from error

        return ClipGroup(universe=universe, year=year, season=season)

    def _manifest_key(self, clip_group_prefix: Prefix) -> Key:
        return S3Client.join(clip_group_prefix, _MANIFEST_FILENAME)

    def _clip_key(self, clip_group_prefix: Prefix, clip_id: ClipId) -> Key:
        return S3Client.join(clip_group_prefix, clip_id + Extension.MP4.suffix)

    def _normalized_clip_key(self, clip_group_prefix: Prefix, clip_id: ClipId) -> Key:
        return S3Client.join(clip_group_prefix, clip_id + _NORMALIZED_SUFFIX + Extension.MP4.suffix)

    @staticmethod
    def _flatten_clip_id_batches(
        clip_id_batches: Sequence[Sequence[ClipId]],
        *,
        operation: str,
    ) -> list[ClipId]:
        if not clip_id_batches:
            raise ValueError(f'{operation} requires at least one clip id')
        if any(not batch for batch in clip_id_batches):
            raise ValueError(f'{operation} clip_id_batches must not contain empty batches')
        clip_ids = [clip_id for batch in clip_id_batches for clip_id in batch]
        if not clip_ids:
            raise ValueError(f'{operation} requires at least one clip id')
        return clip_ids

    @staticmethod
    def _build_dense_sub_group_entries(entries: Sequence[ManifestEntry]) -> dict[ClipId, ManifestEntry]:
        rewritten_entries: dict[ClipId, ManifestEntry] = {}
        current_batch = 0
        previous_batch: int | None = None
        next_order = 0
        for entry in entries:
            if entry.batch != previous_batch:
                current_batch += 1
                previous_batch = entry.batch
                next_order = 1
            else:
                next_order += 1
            rewritten_entries[entry.id] = dataclass_replace(
                entry,
                batch=current_batch,
                order=next_order,
            )
        return rewritten_entries

    @staticmethod
    def _build_remove_sync_error(
        error: Exception,
        *,
        stage: str,
        clip_ids: Sequence[ClipId],
        touched_keys: Sequence[Key],
        assume_touched_keys: Sequence[Key] = (),
        manifest_key: Key,
        note_prefix: str,
    ) -> ClipRemoveManifestSyncError | None:
        effective_touched_keys = tuple(dict.fromkeys((*touched_keys, *assume_touched_keys)))
        if not effective_touched_keys:
            return None

        sync_error = ClipRemoveManifestSyncError(
            stage=stage,
            clip_ids=clip_ids,
            touched_keys=effective_touched_keys,
            manifest_key=manifest_key,
        )
        sync_error.add_note(f'{note_prefix}: {error!r}')
        return sync_error

    def _new_clip_id(self, *, manifest: Manifest, seen_ids: set[ClipId]) -> ClipId:
        """Return a fresh hex UUIDv7 clip id for a newly created S3 clip object.

        Its embedded timestamp reflects when that object is created.
        """
        while True:
            clip_id = _uuid7().hex
            if not manifest.has_id(clip_id) and clip_id not in seen_ids:
                return clip_id

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
            return Manifest.from_dict(decoded_manifest)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ManifestCorruptedError(manifest_key, str(error)) from error

    async def _write_manifest_and_update_cache(
        self,
        *,
        clip_group_prefix: Prefix,
        manifest: Manifest,
    ) -> None:
        manifest_key = self._manifest_key(clip_group_prefix)
        manifest_payload = json.dumps(manifest.to_dict(), separators=(',', ':')).encode('utf-8')
        await self._s3_client.put_bytes(
            manifest_key,
            manifest_payload,
            content_type=S3ContentType.JSON,
        )
        self._manifest_cache[clip_group_prefix] = manifest.copy()

    async def _fetch_normalized_batch(
        self,
        clip_group_prefix: Prefix,
        manifest: Manifest,
        batch_entries: Sequence[ManifestEntry],
        *,
        audio_normalization: AudioNormalization,
    ) -> tuple[list[FetchedClip], Manifest]:
        clip_batch: list[FetchedClip | None] = [None] * len(batch_entries)
        rewritten_entries: dict[ClipId, ManifestEntry] = {}
        uploaded_keys: list[Key] = []
        affected_clip_ids: list[ClipId] = []

        try:
            for index, entry in enumerate(batch_entries):
                normalized_key = self._normalized_clip_key(clip_group_prefix, entry.id)

                if entry.audio_normalization == audio_normalization:
                    try:
                        normalized_bytes = await self._s3_client.get_bytes(normalized_key)
                    except S3ObjectNotFoundError:
                        normalized_bytes = await self._regenerate_normalized_twin(
                            clip_group_prefix,
                            entry,
                            audio_normalization=audio_normalization,
                            normalized_key=normalized_key,
                        )
                        uploaded_keys.append(normalized_key)
                        affected_clip_ids.append(entry.id)
                        rewritten_entries[entry.id] = ManifestEntry(
                            id=entry.id,
                            video_hash=entry.video_hash,
                            audio_normalization=audio_normalization,
                            sub_season=entry.sub_season,
                            scope=entry.scope,
                            batch=entry.batch,
                            order=entry.order,
                        )
                    clip_batch[index] = FetchedClip(
                        id=entry.id,
                        file=FileBytes(data=normalized_bytes, extension=Extension.MP4),
                    )
                    continue

                normalized_bytes = await self._regenerate_normalized_twin(
                    clip_group_prefix,
                    entry,
                    audio_normalization=audio_normalization,
                    normalized_key=normalized_key,
                )
                uploaded_keys.append(normalized_key)
                affected_clip_ids.append(entry.id)
                rewritten_entries[entry.id] = ManifestEntry(
                    id=entry.id,
                    video_hash=entry.video_hash,
                    audio_normalization=audio_normalization,
                    sub_season=entry.sub_season,
                    scope=entry.scope,
                    batch=entry.batch,
                    order=entry.order,
                )
                clip_batch[index] = FetchedClip(
                    id=entry.id,
                    file=FileBytes(data=normalized_bytes, extension=Extension.MP4),
                )
        except Exception as error:
            if uploaded_keys:
                sync_error = NormalizedClipManifestSyncError(
                    written_keys=uploaded_keys,
                    affected_clip_ids=affected_clip_ids,
                    stage='before_manifest_write',
                )
                sync_error.add_note(f'Original normalized twin write error: {error!r}')
                raise sync_error from error
            raise

        if not rewritten_entries:
            return [clip for clip in clip_batch if clip is not None], manifest

        rewritten_manifest = Manifest([rewritten_entries.get(entry.id, entry) for entry in manifest])

        try:
            await self._write_manifest_and_update_cache(
                clip_group_prefix=clip_group_prefix,
                manifest=rewritten_manifest,
            )
        except Exception as error:
            sync_error = NormalizedClipManifestSyncError(
                written_keys=uploaded_keys,
                affected_clip_ids=affected_clip_ids,
                stage='manifest_write',
            )
            sync_error.add_note(f'Original manifest write error: {error!r}')
            raise sync_error from error

        return [clip for clip in clip_batch if clip is not None], rewritten_manifest

    async def _regenerate_normalized_twin(
        self,
        clip_group_prefix: Prefix,
        entry: ManifestEntry,
        *,
        audio_normalization: AudioNormalization,
        normalized_key: Key,
    ) -> bytes:
        raw_clip_key = self._clip_key(clip_group_prefix, entry.id)
        raw_bytes = await self._s3_client.get_bytes(raw_clip_key)
        normalized_bytes = await normalize_video_audio_loudness(
            raw_bytes,
            loudness=audio_normalization.loudness,
            bitrate=audio_normalization.bitrate,
        )
        await self._s3_client.put_bytes(
            normalized_key,
            normalized_bytes,
            content_type=S3ContentType.MP4,
        )
        return normalized_bytes


def _uuid7() -> uuid.UUID:
    return uuid.uuid7()


def _parse_audio_normalization(value: object) -> AudioNormalization | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError('manifest `audio_normalization` must be an object or null')
    if set(value) != {'loudness', 'bitrate'}:
        raise ValueError('manifest `audio_normalization` has unexpected fields')
    try:
        return AudioNormalization(
            loudness=value['loudness'],
            bitrate=value['bitrate'],
        )
    except ValueError as error:
        raise ValueError(f'manifest `audio_normalization` is invalid: {error}') from error


_ClipStrEnum = TypeVar('_ClipStrEnum', bound=StrEnum)


def _expect_str(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f'manifest `{field}` must be a string')
    return value


def _parse_uuid7(value: str, *, field: str) -> ClipId:
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


def _parse_enum(value: object, enum_type: type[_ClipStrEnum], *, field: str) -> _ClipStrEnum:
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


def _format_optional_sub_season(sub_season: SubSeason | None) -> str:
    if sub_season is None:
        return 'None'
    return _format_sub_season(sub_season)


def _format_scope(scope: Scope | None) -> str:
    return 'None' if scope is None else scope.value
