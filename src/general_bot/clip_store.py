import asyncio
import hashlib
import json
import os
import tempfile
import uuid
from collections.abc import AsyncIterator, Iterable, Iterator
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
    def from_month(cls, month: int) -> Self:
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

    SOURCE = 'source'
    COLLECTION = 'collection'
    EXTRA = 'extra'


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

    def next_order(self, *, sub_season: SubSeason, scope: Scope) -> int:
        """Return the next logical order within a sub-group."""
        return max(
            (
                entry.order
                for entry in self._entries
                if entry.scope is scope and entry.sub_season is sub_season
            ),
            default=0,
        ) + 1

    def to_list(self) -> list[dict[str, Any]]:
        """Convert the manifest into its JSON-compatible shape."""
        return [
            {
                'id': entry.id,
                'video_hash': entry.video_hash,
                'sub_season': entry.sub_season.value,
                'scope': entry.scope.value,
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
        seen_orders: set[tuple[Scope, SubSeason, int]] = set()

        for raw_entry in data:
            if not isinstance(raw_entry, dict):
                raise ValueError('manifest clip entry must be an object')
            if set(raw_entry) != {'id', 'video_hash', 'sub_season', 'scope', 'order'}:
                raise ValueError('manifest clip entry has unexpected fields')

            clip_id = _parse_uuid7(_expect_str(raw_entry['id'], field='id'), field='id')
            video_hash = _parse_sha256_hex(_expect_str(raw_entry['video_hash'], field='video_hash'))
            sub_season = _parse_sub_season(raw_entry['sub_season'])
            scope = _parse_enum(raw_entry['scope'], Scope, field='scope')

            order = raw_entry['order']
            if isinstance(order, bool) or not isinstance(order, int):
                raise ValueError('manifest `order` must be an integer')
            if order < 1:
                raise ValueError('manifest `order` must be >= 1')

            if clip_id in seen_ids:
                raise ValueError(f'duplicate manifest clip id: {clip_id}')
            if video_hash in seen_hashes:
                raise ValueError(f'duplicate manifest video hash: {video_hash}')

            order_key = (scope, sub_season, order)
            if order_key in seen_orders:
                raise ValueError(
                    f'duplicate manifest order for scope={scope.value} sub_season={_format_sub_season(sub_season)} order={order}'
                )

            seen_ids.add(clip_id)
            seen_hashes.add(video_hash)
            seen_orders.add(order_key)
            entries.append(
                ManifestEntry(
                    id=clip_id,
                    video_hash=video_hash,
                    sub_season=sub_season,
                    scope=scope,
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

    def __init__(self, *, failed_keys: list[Key]) -> None:
        self.failed_keys = failed_keys
        super().__init__(f'Rollback failed for {len(failed_keys)} uploaded keys: {failed_keys}')


class ClipStore:
    """Domain-specific wrapper over `S3Client` for grouped clip storage.

    Clips are organized into `ClipGroup` objects, with sub-groups represented
    by `ClipSubGroup`.

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
        clips: list[Clip],
        *,
        clip_group: ClipGroup,
        clip_sub_group: ClipSubGroup,
    ) -> StoreResult:
        """Store clips in a clip group with clip-group-local deduplication.

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
        next_order = manifest.next_order(
            scope=clip_sub_group.scope,
            sub_season=clip_sub_group.sub_season,
        )

        new_entries: list[tuple[ManifestEntry, Clip]] = []
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
            entry = ManifestEntry(
                id=clip_id,
                video_hash=video_hash,
                sub_season=clip_sub_group.sub_season,
                scope=clip_sub_group.scope,
                order=next_order,
            )
            manifest.append(entry)
            new_entries.append((entry, clip))
            seen_ids.add(entry.id)
            seen_hashes.add(video_hash)
            next_order += 1

        if not new_entries:
            return StoreResult(stored_count=0, duplicate_count=duplicate_count)

        manifest_key = self._manifest_key(clip_group_prefix)
        manifest_payload = json.dumps(manifest.to_list(), separators=(',', ':')).encode('utf-8')

        try:
            for entry, clip in new_entries:
                clip_key = self._clip_key(clip_group_prefix, entry.id)
                await self._s3_client.put_bytes(clip_key, clip.bytes, content_type=S3ContentType.MP4)
                uploaded_keys.append(clip_key)

            await self._s3_client.put_bytes(
                manifest_key,
                manifest_payload,
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

    async def fetch(
        self,
        *,
        clip_group: ClipGroup,
        clip_sub_group: ClipSubGroup,
        batch_size: int = 10,
    ) -> AsyncIterator[list[Clip]]:
        """Fetch clips for a clip sub-group in strict manifest order.

        Raises:
            ValueError: If `batch_size` is less than 1.
            ClipGroupNotFoundError: If the requested logical clip group has no matching clips.
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

        matching_entries = sorted(
            (
                entry
                for entry in manifest
                if entry.scope is clip_sub_group.scope and entry.sub_season is clip_sub_group.sub_season
            ),
            key=lambda entry: entry.order,
        )
        if not matching_entries:
            raise ClipGroupNotFoundError(
                year=clip_group.year,
                season=clip_group.season,
                universe=clip_group.universe,
                sub_season=clip_sub_group.sub_season,
                scope=clip_sub_group.scope,
            )

        batch: list[Clip] = []
        for entry in matching_entries:
            clip_key = self._clip_key(clip_group_prefix, entry.id)
            clip_bytes = await self._s3_client.get_bytes(clip_key)
            batch.append(Clip(filename=self._s3_key_to_filename(clip_key), bytes=clip_bytes))
            if len(batch) == batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

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
        parts = S3Client.split(self._filename_to_s3_key(filename))
        if len(parts) != 3:
            return None

        top_level_prefix, clip_group, object_name = parts
        if top_level_prefix != _CLIPS_PREFIX or not object_name.endswith(_VIDEO_SUFFIX):
            return None

        try:
            year_text, season_text, universe_text = clip_group.split(_CLIP_GROUP_SEPARATOR)
            int(year_text)
            Season(int(season_text))
            Universe(universe_text)
            return _parse_uuid7(object_name.removesuffix(_VIDEO_SUFFIX), field='id')
        except ValueError:
            return None

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
            '-loglevel', 'error',
            '-nostats',
            '-nostdin',
            '-threads', '1',
            '-i', str(input_path),
            '-map', '0:v:0',
            '-c:v', 'copy',
            '-an',
            '-sn',
            '-dn',
            '-bsf:v', 'h264_mp4toannexb',
            '-f', 'h264',
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
    if value is None:
        return SubSeason.NONE
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
