import json
import math
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Self, TypeVar

from general_bot.infra.s3 import Key, Prefix, S3Client, S3ContentType, S3ObjectNotFoundError

_TRACKS_PREFIX = 'tracks'
_PRESETS_FILENAME = 'presets.json'
_MANIFEST_FILENAME = 'manifest.json'
_TRACK_SUFFIX = '.opus'
_COVER_SUFFIX = '-cover.jpg'
_INSTRUMENTAL_SUFFIX = '-instrumental.opus'
_VARIANT_MODE_SEPARATOR = '-'
_VARIANT_MODE_TO_STEM = {
    'slowed': 'slowed',
    'sped_up': 'sped-up',
}
_TRACK_GROUP_SEPARATOR = '-'

type TrackId = str
type PresetId = int


class Season(IntEnum):
    """Track season identifier.

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


class TrackUniverse(StrEnum):
    """Track universe identifier."""

    WEST = 'west'
    EAST = 'east'
    PHONK = 'phonk'
    ELECTRONIC = 'electronic'

    def order(self) -> int:
        """Return the canonical display and listing order for track universes."""
        return tuple(type(self)).index(self)


@dataclass(frozen=True, slots=True)
class TrackGroup:
    """Logical track-group identifier."""

    universe: TrackUniverse
    year: int
    season: Season


@dataclass(frozen=True, slots=True)
class Track:
    """Original track payload for storage."""

    artists: tuple[str, ...]
    title: str
    audio_bytes: bytes
    cover_bytes: bytes

    def __post_init__(self) -> None:
        """Validate original track invariants for all construction paths."""
        if not isinstance(self.artists, tuple):
            raise ValueError('Track.artists must be a tuple')
        if not self.artists:
            raise ValueError('Track.artists must not be empty')
        if any(not isinstance(artist, str) for artist in self.artists):
            raise ValueError('Track.artists entries must be strings')
        if any(not artist.strip() for artist in self.artists):
            raise ValueError('Track.artists entries must be non-empty strings')

        if not isinstance(self.title, str):
            raise ValueError('Track.title must be a string')
        if not self.title.strip():
            raise ValueError('Track.title must be a non-empty string')

        if not isinstance(self.audio_bytes, bytes):
            raise ValueError('Track.audio_bytes must be bytes')
        if not self.audio_bytes:
            raise ValueError('Track.audio_bytes must not be empty')

        if not isinstance(self.cover_bytes, bytes):
            raise ValueError('Track.cover_bytes must be bytes')
        if not self.cover_bytes:
            raise ValueError('Track.cover_bytes must not be empty')


@dataclass(frozen=True, slots=True)
class TrackInfo:
    """Public discovery metadata for one stored track within a specific sub-season."""

    id: TrackId
    artists: tuple[str, ...]
    title: str


class SubSeason(StrEnum):
    """Track sub-season identifier."""

    NONE = 'none'
    A = 'A'
    B = 'B'
    C = 'C'
    D = 'D'

    def order(self) -> int:
        """Return the canonical display and listing order for sub-seasons."""
        return tuple(type(self)).index(self)


@dataclass(frozen=True, slots=True)
class PresetMode:
    """One directional variant mode configuration for a preset."""

    step: float
    levels: int

    def __post_init__(self) -> None:
        """Validate preset mode invariants for all construction paths."""
        if isinstance(self.step, bool) or not isinstance(self.step, int | float):
            raise ValueError('PresetMode.step must be numeric')
        if not math.isfinite(float(self.step)):
            raise ValueError('PresetMode.step must be finite')
        if float(self.step) < 0.0:
            raise ValueError('PresetMode.step must be >= 0')

        if isinstance(self.levels, bool) or not isinstance(self.levels, int):
            raise ValueError('PresetMode.levels must be an integer')
        if self.levels < 1:
            raise ValueError('PresetMode.levels must be >= 1')


@dataclass(frozen=True, slots=True)
class Preset:
    """Pure user-facing preset values."""

    name: str
    slowed: PresetMode | None
    sped_up: PresetMode | None
    reverb_start: float
    reverb_step: float

    def __post_init__(self) -> None:
        """Validate preset invariants for all construction paths."""
        if not isinstance(self.name, str):
            raise ValueError('Preset.name must be a string')
        if not self.name.strip():
            raise ValueError('Preset.name must be a non-empty string')

        if isinstance(self.reverb_start, bool) or not isinstance(self.reverb_start, int | float):
            raise ValueError('Preset.reverb_start must be numeric')
        if not math.isfinite(float(self.reverb_start)):
            raise ValueError('Preset.reverb_start must be finite')
        if float(self.reverb_start) < 0.0:
            raise ValueError('Preset.reverb_start must be >= 0')

        if isinstance(self.reverb_step, bool) or not isinstance(self.reverb_step, int | float):
            raise ValueError('Preset.reverb_step must be numeric')
        if not math.isfinite(float(self.reverb_step)):
            raise ValueError('Preset.reverb_step must be finite')
        if float(self.reverb_step) < 0.0:
            raise ValueError('Preset.reverb_step must be >= 0')


@dataclass(frozen=True, slots=True)
class StoredPreset:
    """Internal persisted preset record with identity and version metadata."""

    id: PresetId
    version: int
    preset: Preset

    def __post_init__(self) -> None:
        """Validate stored preset invariants for all construction paths."""
        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise ValueError('StoredPreset.id must be an integer')
        if self.id < 1:
            raise ValueError('StoredPreset.id must be >= 1')

        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ValueError('StoredPreset.version must be an integer')
        if self.version < 1:
            raise ValueError('StoredPreset.version must be >= 1')

        if not isinstance(self.preset, Preset):
            raise ValueError('StoredPreset.preset must be a Preset')


@dataclass(frozen=True, slots=True)
class Presets:
    """Authoritative preset registry stored at `tracks/presets.json`."""

    default_preset_id: PresetId
    presets: list[StoredPreset]

    def get(self, preset_id: PresetId) -> StoredPreset | None:
        """Return the stored preset with the given id, if present."""
        return next((preset for preset in self.presets if preset.id == preset_id), None)

    def require(self, preset_id: PresetId) -> StoredPreset:
        """Return the stored preset with the given id or raise `ValueError`."""
        preset = self.get(preset_id)
        if preset is None:
            raise ValueError(f'Unknown preset id: {preset_id}')
        return preset

    def default_preset(self) -> StoredPreset:
        """Return the current default stored preset."""
        return self.require(self.default_preset_id)

    def to_dict(self) -> dict[str, object]:
        """Convert presets into their JSON-compatible storage shape."""
        return {
            'default_preset_id': self.default_preset_id,
            'presets': [
                {
                    'id': preset.id,
                    'version': preset.version,
                    'preset': _preset_to_dict(preset.preset),
                }
                for preset in self.presets
            ],
        }

    @classmethod
    def from_dict(cls, data: object) -> Self:
        """Build presets from a decoded JSON payload.

        Args:
            data: Decoded JSON value from `tracks/presets.json`.

        Raises:
            ValueError: If the payload does not match the preset schema.
        """
        if not isinstance(data, dict):
            raise ValueError('presets root must be an object')
        if set(data) != {'default_preset_id', 'presets'}:
            raise ValueError('presets root has unexpected fields')

        default_preset_id = _expect_positive_int(
            data['default_preset_id'],
            field='default_preset_id',
            context='presets',
        )

        raw_presets = data['presets']
        if not isinstance(raw_presets, list):
            raise ValueError('presets `presets` must be a list')
        if not raw_presets:
            raise ValueError('presets `presets` must not be empty')

        presets: list[StoredPreset] = []
        seen_ids: set[int] = set()
        for raw_preset in raw_presets:
            if not isinstance(raw_preset, dict):
                raise ValueError('presets preset entry must be an object')
            if set(raw_preset) != {'id', 'version', 'preset'}:
                raise ValueError('presets preset entry has unexpected fields')

            preset_id = _expect_positive_int(raw_preset['id'], field='id', context='presets preset')
            if preset_id in seen_ids:
                raise ValueError(f'duplicate stored preset id: {preset_id}')

            version = _expect_positive_int(
                raw_preset['version'],
                field='version',
                context=f'presets preset {preset_id}',
            )
            preset_value = _parse_preset(raw_preset['preset'], context=f'presets preset {preset_id}')

            seen_ids.add(preset_id)
            presets.append(
                StoredPreset(
                    id=preset_id,
                    version=version,
                    preset=preset_value,
                )
            )

        if default_preset_id not in seen_ids:
            raise ValueError('presets `default_preset_id` must refer to an existing preset')

        return cls(
            default_preset_id=default_preset_id,
            presets=presets,
        )


@dataclass(frozen=True, slots=True)
class AppliedPreset:
    """Preset identity recorded in the manifest once variants exist."""

    id: int
    version: int


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """Single authoritative logical track row in a track-group manifest.

    `preset is None` means no original variants are tracked. Once original
    variants exist, `preset` stores the exact preset id and version used to
    produce them.

    `has_instrumental` tracks whether an authoritative original instrumental
    object exists.

    `has_instrumental_variants` is tracked separately because instrumental
    storage can happen later than the original `store()` flow and can therefore
    diverge from original variant completeness. Instrumental variants only make
    sense when an instrumental exists and when original variants are tracked
    through `preset`.

    Cover art is mandatory by convention and is therefore not represented by a
    separate manifest flag.
    """

    id: TrackId
    artists: tuple[str, ...]
    title: str
    sub_season: SubSeason
    order: int
    preset: AppliedPreset | None
    has_instrumental: bool
    has_instrumental_variants: bool


class Manifest:
    """Aggregate wrapper over track manifest entries.

    The group `manifest.json` is the authoritative logical index. This wrapper
    centralizes validation and ordering logic while keeping the in-memory state
    easy to inspect and copy. Store-path callers receive a copy so they can
    mutate safely before committing a rewritten manifest.
    """

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

    def has_id(self, track_id: TrackId) -> bool:
        """Return whether the manifest already contains a track id."""
        return any(entry.id == track_id for entry in self._entries)

    def next_order(self, *, sub_season: SubSeason) -> int:
        """Return the next dense 1-based order within the provided sub-season."""
        return max((entry.order for entry in self._entries if entry.sub_season is sub_season), default=0) + 1

    def to_list(self) -> list[dict[str, object]]:
        """Convert the manifest into its JSON-compatible storage shape."""
        return [
            {
                'id': entry.id,
                'artists': list(entry.artists),
                'title': entry.title,
                'sub_season': entry.sub_season.value,
                'order': entry.order,
                'preset': _applied_preset_to_dict(entry.preset),
                'has_instrumental': entry.has_instrumental,
                'has_instrumental_variants': entry.has_instrumental_variants,
            }
            for entry in self._entries
        ]

    @classmethod
    def from_list(cls, data: object) -> Self:
        """Build a manifest from a decoded JSON payload.

        Args:
            data: Decoded JSON value from a group `manifest.json`.

        Raises:
            ValueError: If the payload does not match the manifest schema.
        """
        if not isinstance(data, list):
            raise ValueError('manifest root must be a list')

        entries: list[ManifestEntry] = []
        seen_ids: set[TrackId] = set()
        seen_positions: set[tuple[SubSeason, int]] = set()

        for raw_entry in data:
            if not isinstance(raw_entry, dict):
                raise ValueError('manifest track entry must be an object')
            if set(raw_entry) != {
                'id',
                'artists',
                'title',
                'sub_season',
                'order',
                'preset',
                'has_instrumental',
                'has_instrumental_variants',
            }:
                raise ValueError('manifest track entry has unexpected fields')

            track_id = _parse_uuid7(
                _expect_str(raw_entry['id'], field='id', context='manifest'),
                field='id',
                context='manifest',
            )
            artists = _parse_track_artists(raw_entry['artists'], context='manifest')
            title = _expect_non_empty_str(raw_entry['title'], field='title', context='manifest')
            sub_season = _parse_enum(raw_entry['sub_season'], SubSeason, field='sub_season', context='manifest')
            order = _expect_positive_int(raw_entry['order'], field='order', context='manifest')
            preset = _parse_applied_preset(raw_entry['preset'])
            has_instrumental = _expect_bool(raw_entry['has_instrumental'], field='has_instrumental', context='manifest')
            has_instrumental_variants = _expect_bool(
                raw_entry['has_instrumental_variants'],
                field='has_instrumental_variants',
                context='manifest',
            )

            if has_instrumental_variants and not has_instrumental:
                raise ValueError('manifest `has_instrumental_variants` requires `has_instrumental`')
            if has_instrumental_variants and preset is None:
                raise ValueError('manifest `has_instrumental_variants` requires non-null `preset`')

            if track_id in seen_ids:
                raise ValueError(f'duplicate manifest track id: {track_id}')

            position_key = (sub_season, order)
            if position_key in seen_positions:
                raise ValueError(
                    f'duplicate manifest position for sub_season={_format_sub_season(sub_season)} order={order}'
                )

            seen_ids.add(track_id)
            seen_positions.add(position_key)
            entries.append(
                ManifestEntry(
                    id=track_id,
                    artists=artists,
                    title=title,
                    sub_season=sub_season,
                    order=order,
                    preset=preset,
                    has_instrumental=has_instrumental,
                    has_instrumental_variants=has_instrumental_variants,
                )
            )

        return cls(entries)


class TrackManifestCorruptedError(RuntimeError):
    """Raised when a group manifest exists but cannot be decoded or validated."""

    def __init__(self, key: Key, reason: str) -> None:
        self.key = key
        super().__init__(f'Track manifest at {key} is corrupted: {reason}')


class TrackPresetsCorruptedError(RuntimeError):
    """Raised when `tracks/presets.json` exists but cannot be decoded or validated."""

    def __init__(self, key: Key, reason: str) -> None:
        self.key = key
        super().__init__(f'Track presets at {key} are corrupted: {reason}')


class TrackGroupNotFoundError(LookupError):
    """Raised when a requested logical track group has no manifest."""

    def __init__(
        self,
        *,
        universe: TrackUniverse,
        year: int,
        season: Season,
        sub_season: SubSeason | None,
    ) -> None:
        self.universe = universe
        self.year = year
        self.season = season
        self.sub_season = sub_season
        super().__init__(
            f'No tracks found for {universe.value}-{year}-{int(season)} '
            f'sub_season={_format_optional_sub_season(sub_season)}'
        )


class TrackManifestSyncError(RuntimeError):
    """Raised when staged track-store writes leave uploaded objects unsynchronized."""

    def __init__(
        self,
        *,
        stage: str,
        track_id: TrackId,
        written_keys: Iterable[Key],
        manifest_key: Key,
    ) -> None:
        self.stage = stage
        self.track_id = track_id
        self.written_keys = tuple(written_keys)
        self.manifest_key = manifest_key
        super().__init__(
            f'Staged track store failed at {self.stage} for track id {self.track_id}; '
            f'written keys: {list(self.written_keys)}; '
            f'manifest key not synchronized: {self.manifest_key}'
        )


class TrackInstrumentalManifestSyncError(RuntimeError):
    """Raised when instrumental uploads cannot be synchronized with manifest state."""

    def __init__(
        self,
        *,
        track_id: TrackId,
        instrumental_key: Key,
        manifest_key: Key,
    ) -> None:
        self.track_id = track_id
        self.instrumental_key = instrumental_key
        self.manifest_key = manifest_key
        super().__init__(
            'Instrumental/manifest synchronization failed '
            f'for track id {self.track_id} after writing instrumental key {self.instrumental_key} '
            f'but before persisting manifest key {self.manifest_key}'
        )


class TrackStore:
    """Domain-specific wrapper over `S3Client` for grouped track storage.

    Authoritative storage state is limited to `tracks/presets.json`, each
    group's `manifest.json`, the original track object, the mandatory cover
    object, and the optional instrumental object. Generated variants are
    cache-like and may be regenerated later from those authoritative inputs.

    `tracks/presets.json` is guaranteed to exist before any public async
    method proceeds. The store handles that bootstrap internally and keeps the
    parsed presets in `_presets_cache`.

    The group manifest is authoritative for logical tracks in a `TrackGroup`.
    Store-path reads use copy-safe manifests so writes can stage updates before
    commit, while read-path calls may reuse the cached manifest directly.

    Store writes media objects before persisting the authoritative group
    manifest. If manifest persistence fails after object upload succeeds, the
    uploaded objects remain in storage and an explicit sync error is raised so
    manual cleanup can remove the listed keys later. Manifest cache updates
    happen only after manifest persistence succeeds.

    This phase is intentionally conservative:
        - `store()` writes only the original track and mandatory cover.
        - No hashing or deduplication is performed.
        - Concurrent writes to the same `TrackGroup` are unsupported.
        - `fetch()` validates setup only and defers retrieval and variant work.
    """

    def __init__(self, s3_client: S3Client, *, bootstrap_preset: Preset) -> None:
        """Initialize the store with an opened generic S3 client."""
        self._s3_client = s3_client
        self._bootstrap_preset = bootstrap_preset
        self._presets_cache: Presets | None = None
        self._manifest_cache: dict[Prefix, Manifest] = {}

    async def store(
        self,
        track: Track,
        *,
        group: TrackGroup,
        sub_season: SubSeason,
    ) -> None:
        """Store one original track plus its mandatory cover object.

        `track.audio_bytes` must already be Opus data and `track.cover_bytes` must already
        be JPEG data. This phase treats those media formats as a caller
        contract and does not perform expensive media validation.

        Store creates the target group implicitly by writing its first manifest
        if that group does not yet exist. Track and cover objects are uploaded
        first, then the authoritative manifest is persisted. If a later staged
        upload or manifest persistence fails after one or more objects were
        written, those uploaded objects remain in storage and an explicit sync
        error is raised for manual recovery.

        This phase does not perform hashing or deduplication. Every successful
        call stores a new original track object with a fresh UUIDv7 id.

        Writes to the same `TrackGroup` are assumed to be sequential
        (single-writer). Concurrent writes are not supported and may lead to
        manifest overwrite or orphaned objects.

        If a failure occurs during media uploads (e.g. while writing the cover),
        previously written objects are not rolled back and may remain in storage
        without a corresponding manifest entry. These objects are not referenced
        by the system and can be safely removed manually if needed.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackManifestCorruptedError: If the target group's manifest exists but is malformed.
            TrackManifestSyncError: If one or more track-store objects are written but a later stage fails.
        """
        await self._ensure_presets_loaded()

        track_group_prefix = self._track_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        manifest = await self._load_manifest_for_store(track_group_prefix)
        track_id = self._new_track_id(manifest=manifest)
        order = manifest.next_order(sub_season=sub_season)
        manifest.append(
            ManifestEntry(
                id=track_id,
                artists=track.artists,
                title=track.title,
                sub_season=sub_season,
                order=order,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            )
        )

        track_key = self._track_key(track_group_prefix, track_id)
        cover_key = self._cover_key(track_group_prefix, track_id)
        manifest_key = self._manifest_key(track_group_prefix)
        await self._s3_client.put_bytes(
            track_key,
            track.audio_bytes,
            content_type=S3ContentType.OPUS,
        )

        try:
            await self._s3_client.put_bytes(
                cover_key,
                track.cover_bytes,
                content_type=S3ContentType.JPEG,
            )
        except Exception as error:
            sync_error = TrackManifestSyncError(
                stage='cover_upload',
                track_id=track_id,
                written_keys=[track_key],
                manifest_key=manifest_key,
            )
            sync_error.add_note(f'Original cover upload error: {error!r}')
            raise sync_error from error

        try:
            await self._write_manifest_and_update_cache(
                track_group_prefix=track_group_prefix,
                manifest=manifest,
            )
        except Exception as error:
            sync_error = TrackManifestSyncError(
                stage='manifest_write',
                track_id=track_id,
                written_keys=[track_key, cover_key],
                manifest_key=manifest_key,
            )
            sync_error.add_note(f'Original manifest write error: {error!r}')
            raise sync_error from error

    async def store_instrumental(
        self,
        instrumental_bytes: bytes,
        *,
        group: TrackGroup,
        track_id: TrackId,
    ) -> None:
        """Store or overwrite one track's authoritative instrumental object.

        This method always writes the stable instrumental object key for the
        provided `(group, track_id)` without probing storage first. The upload
        happens before the manifest rewrite. If manifest persistence fails, the
        uploaded instrumental is left in place and an explicit sync error is
        raised for manual recovery.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackManifestCorruptedError: If the target group's manifest exists but is malformed.
            TrackGroupNotFoundError: If the target group's manifest does not exist.
            ValueError: If `track_id` does not exist in the provided group's manifest.
            TrackInstrumentalManifestSyncError: If the instrumental is written but the manifest rewrite fails.
        """
        await self._ensure_presets_loaded()

        track_group_prefix = self._track_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        manifest = (await self._require_group_manifest(group, sub_season=None)).copy()

        if not manifest.has_id(track_id):
            raise ValueError(
                f'Track id {track_id} does not exist in group {group.universe.value}-{group.year}-{int(group.season)}'
            )

        rewritten_manifest = Manifest(
            [
                ManifestEntry(
                    id=entry.id,
                    artists=entry.artists,
                    title=entry.title,
                    sub_season=entry.sub_season,
                    order=entry.order,
                    preset=entry.preset,
                    has_instrumental=True if entry.id == track_id else entry.has_instrumental,
                    has_instrumental_variants=False if entry.id == track_id else entry.has_instrumental_variants,
                )
                for entry in manifest
            ]
        )

        instrumental_key = self._instrumental_key(track_group_prefix, track_id)
        manifest_key = self._manifest_key(track_group_prefix)

        await self._s3_client.put_bytes(
            instrumental_key,
            instrumental_bytes,
            content_type=S3ContentType.OPUS,
        )

        try:
            await self._write_manifest_and_update_cache(
                track_group_prefix=track_group_prefix,
                manifest=rewritten_manifest,
            )
        except Exception as error:
            sync_error = TrackInstrumentalManifestSyncError(
                track_id=track_id,
                instrumental_key=instrumental_key,
                manifest_key=manifest_key,
            )
            sync_error.add_note(f'Original manifest write error: {error!r}')
            raise sync_error from error

    async def fetch(
        self,
        group: TrackGroup,
        sub_season: SubSeason,
        *,
        preset_id: PresetId | None = None,
    ) -> object:
        """Prepare for a future track fetch and variant application flow.

        This method is intentionally partial in this iteration. It guarantees
        preset bootstrap, validates the requested group exists, resolves the
        authoritative preset to use, and then stops. Final retrieval, variant
        generation, and return-shape design will be implemented later.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackGroupNotFoundError: If the requested group manifest does not exist.
            TrackManifestCorruptedError: If the requested group manifest exists but is malformed.
            ValueError: If `preset_id` does not refer to a known preset.
            NotImplementedError: Always, after setup validation succeeds.
        """
        presets = await self._ensure_presets_loaded()
        resolved_preset = presets.default_preset() if preset_id is None else presets.require(preset_id)

        await self._require_group_manifest(group, sub_season=sub_season)
        _ = resolved_preset
        raise NotImplementedError(
            'TrackStore.fetch() will retrieve tracks and apply or generate variants in a later iteration.'
        )

    async def list_presets(self) -> list[tuple[PresetId, Preset]]:
        """List available user-facing presets without exposing version metadata."""
        presets = await self._ensure_presets_loaded()
        return [(stored_preset.id, stored_preset.preset) for stored_preset in presets.presets]

    async def list_groups(self) -> list[TrackGroup]:
        """List all discovered track groups from stored S3 prefixes.

        This method relies on `S3Client.list_subprefixes('tracks')` returning
        only immediate prefixes and not ordinary files. That means
        `tracks/presets.json` is not part of the returned collection and no
        file-specific ignore list is needed here.
        """
        await self._ensure_presets_loaded()
        track_group_prefixes = await self._s3_client.list_subprefixes(prefix=_TRACKS_PREFIX)
        track_groups = [self._parse_track_group_prefix(prefix) for prefix in track_group_prefixes]
        return sorted(track_groups, key=lambda group: (group.universe.order(), group.year, int(group.season)))

    async def list_sub_seasons(self, group: TrackGroup) -> list[SubSeason]:
        """List unique sub-seasons present in a group's authoritative manifest.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackGroupNotFoundError: If the requested group manifest does not exist.
            TrackManifestCorruptedError: If the group manifest exists but is malformed.
        """
        await self._ensure_presets_loaded()
        manifest = await self._require_group_manifest(group, sub_season=None)
        sub_seasons = {entry.sub_season for entry in manifest}
        return sorted(sub_seasons, key=lambda value: value.order())

    async def list_tracks(self, group: TrackGroup, sub_season: SubSeason) -> list[TrackInfo]:
        """List discovery metadata for tracks in one sub-season of a group.

        Returned items are discovery metadata only. Results are ordered by the
        authoritative manifest's ascending internal `order` within the
        requested `sub_season`, but that internal `order` is intentionally not
        exposed in the public return type.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackGroupNotFoundError: If the requested group manifest does not exist.
            TrackManifestCorruptedError: If the group manifest exists but is malformed.
        """
        await self._ensure_presets_loaded()
        manifest = await self._require_group_manifest(group, sub_season=sub_season)
        entries = sorted(
            (entry for entry in manifest if entry.sub_season is sub_season),
            key=lambda entry: entry.order,
        )
        return [
            TrackInfo(
                id=entry.id,
                artists=entry.artists,
                title=entry.title,
            )
            for entry in entries
        ]

    async def _ensure_presets_loaded(self) -> Presets:
        if self._presets_cache is not None:
            return self._presets_cache

        presets_key = self._presets_key()
        try:
            raw_presets = await self._s3_client.get_bytes(presets_key)
        except S3ObjectNotFoundError:
            presets = self._bootstrap_presets()
            await self._s3_client.put_bytes(
                presets_key,
                json.dumps(presets.to_dict(), separators=(',', ':')).encode('utf-8'),
                content_type=S3ContentType.JSON,
            )
            self._presets_cache = presets
            return presets

        try:
            decoded_presets = json.loads(raw_presets.decode('utf-8'))
            presets = Presets.from_dict(decoded_presets)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise TrackPresetsCorruptedError(presets_key, str(error)) from error

        self._presets_cache = presets
        return presets

    def _bootstrap_presets(self) -> Presets:
        bootstrap_preset = StoredPreset(
            id=1,
            version=1,
            preset=self._bootstrap_preset,
        )
        return Presets(
            default_preset_id=1,
            presets=[bootstrap_preset],
        )

    async def _require_group_manifest(self, group: TrackGroup, *, sub_season: SubSeason | None) -> Manifest:
        track_group_prefix = self._track_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        try:
            return await self._load_manifest_for_read(track_group_prefix)
        except S3ObjectNotFoundError as error:
            raise TrackGroupNotFoundError(
                universe=group.universe,
                year=group.year,
                season=group.season,
                sub_season=sub_season,
            ) from error

    async def _load_manifest_for_store(self, track_group_prefix: Prefix) -> Manifest:
        if (cached_manifest := self._manifest_cache.get(track_group_prefix)) is not None:
            return cached_manifest.copy()

        try:
            manifest = await self._fetch_manifest(track_group_prefix)
        except S3ObjectNotFoundError:
            return Manifest()

        self._manifest_cache[track_group_prefix] = manifest
        return manifest.copy()

    async def _load_manifest_for_read(self, track_group_prefix: Prefix) -> Manifest:
        if (cached_manifest := self._manifest_cache.get(track_group_prefix)) is not None:
            return cached_manifest

        manifest = await self._fetch_manifest(track_group_prefix)
        self._manifest_cache[track_group_prefix] = manifest
        return manifest

    async def _fetch_manifest(self, track_group_prefix: Prefix) -> Manifest:
        manifest_key = self._manifest_key(track_group_prefix)
        raw_manifest = await self._s3_client.get_bytes(manifest_key)

        try:
            decoded_manifest = json.loads(raw_manifest.decode('utf-8'))
            return Manifest.from_list(decoded_manifest)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise TrackManifestCorruptedError(manifest_key, str(error)) from error

    async def _write_manifest_and_update_cache(
        self,
        *,
        track_group_prefix: Prefix,
        manifest: Manifest,
    ) -> None:
        manifest_key = self._manifest_key(track_group_prefix)
        manifest_payload = json.dumps(manifest.to_list(), separators=(',', ':')).encode('utf-8')
        await self._s3_client.put_bytes(
            manifest_key,
            manifest_payload,
            content_type=S3ContentType.JSON,
        )
        self._manifest_cache[track_group_prefix] = manifest.copy()

    def _presets_key(self) -> Key:
        return S3Client.join(_TRACKS_PREFIX, _PRESETS_FILENAME)

    def _track_group_prefix(self, *, universe: TrackUniverse, year: int, season: Season) -> Prefix:
        track_group = _TRACK_GROUP_SEPARATOR.join((universe.value, str(year), str(int(season))))
        return S3Client.join(_TRACKS_PREFIX, track_group)

    def _parse_track_group_prefix(self, prefix: Prefix) -> TrackGroup:
        segments = S3Client.split(prefix)
        if not segments or segments[0] != _TRACKS_PREFIX:
            raise ValueError(f'Invalid track group prefix {prefix!r}: expected prefix under {_TRACKS_PREFIX!r}')

        remaining_segments = segments[1:]
        if len(remaining_segments) != 1:
            raise ValueError(f'Invalid track group prefix {prefix!r}: expected exactly one track group segment')

        track_group = remaining_segments[0]
        try:
            universe_text, year_text, season_text = track_group.split(_TRACK_GROUP_SEPARATOR)
            universe = TrackUniverse(universe_text)
            year = int(year_text)
            season = Season(int(season_text))
        except ValueError as error:
            raise ValueError(f'Invalid track group prefix {prefix!r}: malformed track group segment') from error

        return TrackGroup(
            universe=universe,
            year=year,
            season=season,
        )

    def _manifest_key(self, track_group_prefix: Prefix) -> Key:
        return S3Client.join(track_group_prefix, _MANIFEST_FILENAME)

    def _track_key(self, track_group_prefix: Prefix, track_id: TrackId) -> Key:
        return S3Client.join(track_group_prefix, track_id + _TRACK_SUFFIX)

    def _cover_key(self, track_group_prefix: Prefix, track_id: TrackId) -> Key:
        return S3Client.join(track_group_prefix, track_id + _COVER_SUFFIX)

    def _instrumental_key(self, track_group_prefix: Prefix, track_id: TrackId) -> Key:
        return S3Client.join(track_group_prefix, track_id + _INSTRUMENTAL_SUFFIX)

    def _variant_key(self, track_group_prefix: Prefix, track_id: TrackId, *, mode: str, level: int) -> Key:
        mode_stem = _variant_mode_to_stem(mode)
        validated_level = _expect_positive_int(level, field='level', context='variant key')
        object_name = f'{track_id}{_VARIANT_MODE_SEPARATOR}{mode_stem}{_VARIANT_MODE_SEPARATOR}{validated_level}'
        return S3Client.join(track_group_prefix, object_name + _TRACK_SUFFIX)

    def _instrumental_variant_key(self, track_group_prefix: Prefix, track_id: TrackId, *, mode: str, level: int) -> Key:
        mode_stem = _variant_mode_to_stem(mode)
        validated_level = _expect_positive_int(level, field='level', context='instrumental variant key')
        object_name = (
            f'{track_id}{_VARIANT_MODE_SEPARATOR}instrumental{_VARIANT_MODE_SEPARATOR}'
            f'{mode_stem}{_VARIANT_MODE_SEPARATOR}{validated_level}'
        )
        return S3Client.join(track_group_prefix, object_name + _TRACK_SUFFIX)

    def _new_track_id(self, *, manifest: Manifest) -> TrackId:
        while True:
            track_id = _uuid7().hex
            if not manifest.has_id(track_id):
                return track_id


def _uuid7() -> uuid.UUID:
    return uuid.uuid7()


_TrackStrEnum = TypeVar('_TrackStrEnum', bound=StrEnum)


def _preset_mode_to_dict(mode: PresetMode | None) -> dict[str, object] | None:
    if mode is None:
        return None
    return {
        'step': mode.step,
        'levels': mode.levels,
    }


def _preset_to_dict(preset: Preset) -> dict[str, object]:
    return {
        'name': preset.name,
        'slowed': _preset_mode_to_dict(preset.slowed),
        'sped_up': _preset_mode_to_dict(preset.sped_up),
        'reverb_start': preset.reverb_start,
        'reverb_step': preset.reverb_step,
    }


def _applied_preset_to_dict(preset: AppliedPreset | None) -> dict[str, object] | None:
    if preset is None:
        return None
    return {
        'id': preset.id,
        'version': preset.version,
    }


def _parse_preset(value: object, *, context: str) -> Preset:
    if not isinstance(value, dict):
        raise ValueError(f'{context} `preset` must be an object')
    if set(value) != {'name', 'slowed', 'sped_up', 'reverb_start', 'reverb_step'}:
        raise ValueError(f'{context} `preset` has unexpected fields')

    return Preset(
        name=_expect_str(value['name'], field='name', context=f'{context} `preset`'),
        slowed=_parse_preset_mode(value['slowed'], field='slowed', context=f'{context} `preset`'),
        sped_up=_parse_preset_mode(value['sped_up'], field='sped_up', context=f'{context} `preset`'),
        reverb_start=_expect_number(
            value['reverb_start'],
            field='reverb_start',
            context=f'{context} `preset`',
            min_value=0.0,
        ),
        reverb_step=_expect_number(
            value['reverb_step'],
            field='reverb_step',
            context=f'{context} `preset`',
            min_value=0.0,
        ),
    )


def _parse_preset_mode(value: object, *, field: str, context: str) -> PresetMode | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f'{context} `{field}` must be an object or null')
    if set(value) != {'step', 'levels'}:
        raise ValueError(f'{context} `{field}` has unexpected fields')

    return PresetMode(
        step=_expect_number(
            value['step'],
            field='step',
            context=f'{context} `{field}`',
            min_value=0.0,
        ),
        levels=_expect_positive_int(value['levels'], field='levels', context=f'{context} `{field}`'),
    )


def _parse_applied_preset(value: object) -> AppliedPreset | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError('manifest `preset` must be an object or null')
    if set(value) != {'id', 'version'}:
        raise ValueError('manifest `preset` has unexpected fields')

    return AppliedPreset(
        id=_expect_positive_int(value['id'], field='preset.id', context='manifest'),
        version=_expect_positive_int(value['version'], field='preset.version', context='manifest'),
    )


def _parse_track_artists(value: object, *, context: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f'{context} `artists` must be a list')
    if not value:
        raise ValueError(f'{context} `artists` must not be empty')

    artists = tuple(_expect_non_empty_str(artist, field='artists[]', context=context) for artist in value)
    return artists


def _expect_non_empty_str(value: object, *, field: str, context: str) -> str:
    parsed = _expect_str(value, field=field, context=context)
    if not parsed.strip():
        raise ValueError(f'{context} `{field}` must be a non-empty string')
    return parsed


def _expect_str(value: object, *, field: str, context: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f'{context} `{field}` must be a string')
    return value


def _expect_bool(value: object, *, field: str, context: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f'{context} `{field}` must be a boolean')
    return value


def _expect_positive_int(value: object, *, field: str, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'{context} `{field}` must be an integer')
    if value < 1:
        raise ValueError(f'{context} `{field}` must be >= 1')
    return value


def _expect_number(value: object, *, field: str, context: str, min_value: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f'{context} `{field}` must be numeric')

    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f'{context} `{field}` must be finite')
    if min_value is not None and parsed < min_value:
        raise ValueError(f'{context} `{field}` must be >= {min_value}')
    return parsed


def _parse_uuid7(value: str, *, field: str, context: str) -> TrackId:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f'{context} `{field}` must be a valid UUID') from error
    if parsed.version != 7:
        raise ValueError(f'{context} `{field}` must be a UUIDv7')
    return parsed.hex


def _parse_enum(value: object, enum_type: type[_TrackStrEnum], *, field: str, context: str) -> _TrackStrEnum:
    if not isinstance(value, str):
        raise ValueError(f'{context} `{field}` must be a string')
    try:
        return enum_type(value)
    except ValueError as error:
        raise ValueError(f'{context} `{field}` has unsupported value: {value}') from error


def _variant_mode_to_stem(mode: str) -> str:
    try:
        return _VARIANT_MODE_TO_STEM[mode]
    except KeyError as error:
        raise ValueError(f'Unsupported variant mode: {mode}') from error


def _format_sub_season(sub_season: SubSeason) -> str:
    return sub_season.value.title()


def _format_optional_sub_season(sub_season: SubSeason | None) -> str:
    if sub_season is None:
        return 'None'
    return _format_sub_season(sub_season)
