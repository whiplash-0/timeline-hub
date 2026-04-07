import json
import math
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
from enum import IntEnum, StrEnum
from typing import Self, TypeVar

from general_bot.infra.ffmpeg import create_audio_variant, probe_audio_sample_rate
from general_bot.infra.s3 import Key, Prefix, S3Client, S3ContentType, S3ObjectNotFoundError

_TRACKS_PREFIX = 'tracks'
_PRESETS_FILENAME = 'presets.json'
_MANIFEST_FILENAME = 'manifest.json'
_TRACK_SUFFIX = '.opus'
_COVER_SUFFIX = '-cover.jpg'
_INSTRUMENTAL_SUFFIX = '-instrumental.opus'
_VARIANT_MODE_SEPARATOR = '-'
_TRACK_GROUP_SEPARATOR = '-'
_FILENAME_S3_DELIMITER_ESCAPE = '--'

type TrackId = str
type PresetId = int
type Filename = str


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
    has_instrumental: bool


@dataclass(frozen=True, slots=True)
class FetchedVariant:
    """One generated playable track variant returned by `fetch()`."""

    speed: float
    reverb: float
    audio_bytes: bytes


@dataclass(frozen=True, slots=True)
class FetchedVariants:
    """Immutable UI read model for one track's generated variants and shared metadata."""

    track_id: TrackId
    artists: tuple[str, ...]
    title: str
    cover_filename: Filename
    cover_bytes: bytes
    variants: tuple[FetchedVariant, ...]
    instrumental_variants: tuple[FetchedVariant, ...] | None


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

        if self.slowed is None and self.sped_up is None:
            raise ValueError('Preset must define at least one of slowed or sped_up')


@dataclass(frozen=True, slots=True)
class PresetRecord:
    """Persisted preset record with identity and version metadata."""

    id: PresetId
    version: int
    preset: Preset

    def __post_init__(self) -> None:
        """Validate stored preset invariants for all construction paths."""
        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise ValueError('PresetRecord.id must be an integer')
        if self.id < 1:
            raise ValueError('PresetRecord.id must be >= 1')

        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ValueError('PresetRecord.version must be an integer')
        if self.version < 1:
            raise ValueError('PresetRecord.version must be >= 1')

        if not isinstance(self.preset, Preset):
            raise ValueError('PresetRecord.preset must be a Preset')


@dataclass(frozen=True, slots=True)
class AppliedPreset:
    """Manifest-only materialization metadata for one generated variant family.

    This records which preset identity and version produced the currently
    materialized ordered variants for a track family. It does not store full
    preset values. `variant_count` is the number of ordered variants currently
    materialized for that family.
    """

    id: PresetId
    version: int
    variant_count: int

    def __post_init__(self) -> None:
        """Validate applied preset invariants for all construction paths."""
        if isinstance(self.id, bool) or not isinstance(self.id, int):
            raise ValueError('AppliedPreset.id must be an integer')
        if self.id < 1:
            raise ValueError('AppliedPreset.id must be >= 1')

        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ValueError('AppliedPreset.version must be an integer')
        if self.version < 1:
            raise ValueError('AppliedPreset.version must be >= 1')

        if isinstance(self.variant_count, bool) or not isinstance(self.variant_count, int):
            raise ValueError('AppliedPreset.variant_count must be an integer')
        if self.variant_count < 1:
            raise ValueError('AppliedPreset.variant_count must be >= 1')


@dataclass(frozen=True, slots=True)
class Presets:
    """Internal validated preset registry aggregate for `PresetStore`.

    Invariants:
    - `presets` must not be empty.
    - The default preset is always stored at index 0 of `presets`.
    """

    presets: list[PresetRecord]

    def __post_init__(self) -> None:
        """Validate preset collection invariants for all construction paths."""
        if not isinstance(self.presets, list):
            raise ValueError('Presets.presets must be a list')
        if not self.presets:
            raise ValueError('Presets.presets must not be empty')
        if any(not isinstance(preset, PresetRecord) for preset in self.presets):
            raise ValueError('Presets.presets entries must be PresetRecord instances')

    def get(self, preset_id: PresetId) -> PresetRecord | None:
        """Return the stored preset with the given id, if present."""
        return next((preset for preset in self.presets if preset.id == preset_id), None)

    def require(self, preset_id: PresetId) -> PresetRecord:
        """Return the stored preset with the given id or raise `ValueError`."""
        preset = self.get(preset_id)
        if preset is None:
            raise PresetNotFoundError(f'Unknown preset id: {preset_id}')
        return preset

    def default_preset(self) -> PresetRecord:
        """Return the current default stored preset."""
        return self.presets[0]

    def to_dict(self) -> dict[str, list[dict[str, object]]]:
        """Convert presets into their JSON-compatible storage shape."""
        return {
            'data': [
                {
                    'id': preset.id,
                    'version': preset.version,
                    'preset': _preset_to_dict(preset.preset),
                }
                for preset in self.presets
            ]
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
            raise ValueError("presets root must be an object with only 'data'")
        if set(data) != {'data'}:
            raise ValueError("presets root must be an object with only 'data'")

        raw_presets = data['data']
        if not isinstance(raw_presets, list):
            raise ValueError("presets 'data' must be a list")
        if not raw_presets:
            raise ValueError("presets 'data' must not be empty")

        presets: list[PresetRecord] = []
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
                PresetRecord(
                    id=preset_id,
                    version=version,
                    preset=preset_value,
                )
            )

        return cls(
            presets=presets,
        )


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    """Single authoritative logical track row in a track-group manifest.

    `preset` stores the selected `AppliedPreset` metadata for this track family.
    It describes which preset identity/version applies and how many ordered
    variants that preset currently resolves to. It is not a source of truth for
    preset values.

    `has_variants` tracks whether the original-track variants are currently
    materialized in storage.

    `has_instrumental` tracks whether an authoritative original instrumental
    object exists.

    `has_instrumental_variants` is tracked separately because instrumental
    storage can happen later than the original `store()` flow and can therefore
    diverge from original variant completeness. Instrumental variants only make
    sense when an instrumental exists.

    Cover art is mandatory by convention and is therefore not represented by a
    separate manifest flag.
    """

    id: TrackId
    artists: tuple[str, ...]
    title: str
    sub_season: SubSeason
    order: int
    preset: AppliedPreset
    has_variants: bool
    has_instrumental: bool
    has_instrumental_variants: bool


@dataclass(frozen=True, slots=True)
class _ResolvedVariantSpec:
    speed: float
    reverb: float


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

    def to_dict(self) -> dict[str, list[dict[str, object]]]:
        """Convert the manifest into its JSON-compatible storage shape."""
        return {
            'data': [
                {
                    'id': entry.id,
                    'artists': list(entry.artists),
                    'title': entry.title,
                    'sub_season': entry.sub_season.value,
                    'order': entry.order,
                    'preset': _applied_preset_to_dict(entry.preset),
                    'has_variants': entry.has_variants,
                    'has_instrumental': entry.has_instrumental,
                    'has_instrumental_variants': entry.has_instrumental_variants,
                }
                for entry in self._entries
            ]
        }

    @classmethod
    def from_dict(cls, data: object) -> Self:
        """Build a manifest from a decoded JSON payload.

        Args:
            data: Decoded JSON value from a group `manifest.json`.

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
        seen_ids: set[TrackId] = set()
        seen_positions: set[tuple[SubSeason, int]] = set()

        for raw_entry in raw_entries:
            if not isinstance(raw_entry, dict):
                raise ValueError('manifest track entry must be an object')
            if set(raw_entry) != {
                'id',
                'artists',
                'title',
                'sub_season',
                'order',
                'preset',
                'has_variants',
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
            preset = _parse_applied_preset(raw_entry['preset'], context='manifest')
            has_variants = _expect_bool(raw_entry['has_variants'], field='has_variants', context='manifest')
            has_instrumental = _expect_bool(raw_entry['has_instrumental'], field='has_instrumental', context='manifest')
            has_instrumental_variants = _expect_bool(
                raw_entry['has_instrumental_variants'],
                field='has_instrumental_variants',
                context='manifest',
            )

            if has_instrumental_variants and not has_instrumental:
                raise ValueError('manifest `has_instrumental_variants` requires `has_instrumental`')

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
                    has_variants=has_variants,
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


class PresetNotFoundError(ValueError):
    """Raised when a strict preset lookup refers to an unknown preset id."""


class TrackDefaultPresetRemovalError(ValueError):
    """Raised when attempting to remove the current default preset."""


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


class TrackInvalidAudioFormatError(ValueError):
    """Raised when provided audio bytes do not satisfy TrackStore audio invariants."""

    def __init__(self, reason: str, *, track_id: TrackId | None = None) -> None:
        self.track_id = track_id
        self.reason = reason
        if self.track_id is None:
            super().__init__(self.reason)
            return
        super().__init__(f'Track audio format is invalid for track id {self.track_id}: {self.reason}')


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


class TrackUpdateManifestSyncError(RuntimeError):
    """Raised when staged update mutations cannot be synchronized with manifest state."""

    def __init__(
        self,
        *,
        stage: str,
        track_id: TrackId,
        touched_keys: Iterable[Key],
        manifest_key: Key,
    ) -> None:
        self.stage = stage
        self.track_id = track_id
        self.touched_keys = tuple(touched_keys)
        self.manifest_key = manifest_key
        super().__init__(
            f'Staged track update failed at {self.stage} for track id {self.track_id}; '
            f'touched keys: {list(self.touched_keys)}; '
            f'manifest key not synchronized: {self.manifest_key}'
        )


class TrackFetchManifestSyncError(RuntimeError):
    """Raised when fetch-time cache mutations cannot be synchronized with manifest state."""

    def __init__(
        self,
        *,
        stage: str,
        track_id: TrackId,
        touched_keys: Iterable[Key],
        manifest_key: Key,
    ) -> None:
        self.stage = stage
        self.track_id = track_id
        self.touched_keys = tuple(touched_keys)
        self.manifest_key = manifest_key
        super().__init__(
            f'Fetched track variant sync failed at {self.stage} for track id {self.track_id}; '
            f'touched keys: {list(self.touched_keys)}; '
            f'manifest key not synchronized: {self.manifest_key}'
        )


class PresetStore:
    """Owner of authoritative preset registry state stored at `tracks/presets.json`.

    `PresetStore` fully owns bootstrap initialization, lazy cache state, JSON
    decoding, schema validation, corruption handling, persistence, and preset
    cache-backed preset-management operations. It is the only component that
    touches `tracks/presets.json`, and higher-level services do not work with
    raw `Presets` directly.

    Preset invariants:
        - The registry is never empty.
        - The default preset is always stored at index 0.
        - Stored preset ids and versions are integers >= 1.
    """

    def __init__(self, s3_client: S3Client, *, bootstrap_preset: Preset) -> None:
        """Initialize the preset store with an opened generic S3 client."""
        self._s3_client = s3_client
        self._bootstrap_preset = bootstrap_preset
        self._presets_cache: Presets | None = None

    async def all(self) -> list[PresetRecord]:
        """List presets in storage order. Index 0 is always the default preset."""
        presets = await self._load_presets()
        return list(presets.presets)

    async def default(self) -> PresetRecord:
        """Return the current default stored preset."""
        presets = await self._load_presets()
        return presets.default_preset()

    async def require(self, preset_id: PresetId) -> PresetRecord:
        """Return one stored preset by id or raise `ValueError`."""
        presets = await self._load_presets()
        return presets.require(preset_id)

    async def add(self, preset: Preset) -> None:
        """Append a new stored preset record.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
        """
        presets = await self._load_presets()
        updated_presets = Presets(
            presets=[
                *presets.presets,
                PresetRecord(
                    id=self._next_preset_id(presets),
                    version=1,
                    preset=preset,
                ),
            ],
        )
        await self._write_presets_and_update_cache(updated_presets)

    async def replace(self, preset_id: PresetId, preset: Preset) -> None:
        """Replace one stored preset's editable values and bump its version.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            ValueError: If `preset_id` does not refer to a known preset.
        """
        presets = await self._load_presets()
        stored_preset = presets.require(preset_id)
        updated_presets = Presets(
            presets=[
                PresetRecord(
                    id=stored_preset.id,
                    version=stored_preset.version + 1,
                    preset=preset,
                )
                if existing_preset.id == preset_id
                else existing_preset
                for existing_preset in presets.presets
            ],
        )
        await self._write_presets_and_update_cache(updated_presets)

    async def set_default(self, preset_id: PresetId) -> None:
        """Move the selected preset to the default position at index 0.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            ValueError: If `preset_id` does not refer to a known preset.
        """
        validated_preset_id = _expect_positive_int(
            preset_id,
            field='preset_id',
            context='set_default',
        )

        presets = await self._load_presets()
        target = presets.require(validated_preset_id)
        if presets.default_preset().id == target.id:
            return

        updated_presets = Presets(
            presets=[target] + [preset for preset in presets.presets if preset.id != validated_preset_id],
        )
        await self._write_presets_and_update_cache(updated_presets)

    async def remove(self, preset_id: PresetId) -> None:
        """Remove one non-default stored preset.

        The default preset cannot be removed.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            ValueError: If `preset_id` does not refer to a known preset.
            TrackDefaultPresetRemovalError: If `preset_id` refers to the current default preset.
        """
        validated_preset_id = _expect_positive_int(
            preset_id,
            field='preset_id',
            context='remove',
        )

        presets = await self._load_presets()
        target = presets.require(validated_preset_id)
        if target.id == presets.default_preset().id:
            raise TrackDefaultPresetRemovalError(f'Cannot remove default preset: {validated_preset_id}')

        updated_presets = Presets(
            presets=[stored_preset for stored_preset in presets.presets if stored_preset.id != validated_preset_id],
        )
        await self._write_presets_and_update_cache(updated_presets)

    async def _load_presets(self) -> Presets:
        """Return authoritative presets via the single lazy load/cache path."""
        if self._presets_cache is None:
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
            else:
                try:
                    decoded_presets = json.loads(raw_presets.decode('utf-8'))
                    presets = Presets.from_dict(decoded_presets)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                    raise TrackPresetsCorruptedError(presets_key, str(error)) from error

                self._presets_cache = presets

        return self._presets_cache

    def _bootstrap_presets(self) -> Presets:
        return Presets(
            presets=[
                PresetRecord(
                    id=1,
                    version=1,
                    preset=self._bootstrap_preset,
                )
            ],
        )

    async def _write_presets_and_update_cache(self, presets: Presets) -> None:
        await self._s3_client.put_bytes(
            self._presets_key(),
            json.dumps(presets.to_dict(), separators=(',', ':')).encode('utf-8'),
            content_type=S3ContentType.JSON,
        )
        self._presets_cache = Presets(
            presets=list(presets.presets),
        )

    def _next_preset_id(self, presets: Presets) -> PresetId:
        if not presets.presets:
            return 1
        return max(stored_preset.id for stored_preset in presets.presets) + 1

    def _presets_key(self) -> Key:
        return S3Client.join(_TRACKS_PREFIX, _PRESETS_FILENAME)


class TrackStore:
    """Domain-specific wrapper over `S3Client` for grouped track storage.

    Authoritative storage state is limited to each group's `manifest.json`,
    the original track object, the mandatory cover object, and the optional
    instrumental object. Generated variants are cache-like and may be
    regenerated later from those authoritative inputs.

    Preset registry state is owned by an external `PresetStore`. `TrackStore`
    depends on that service for preset data access only and owns its own
    store/fetch preset-selection policy. It does not bootstrap, parse,
    validate, persist, or cache `tracks/presets.json` itself.

    The group manifest is authoritative for logical tracks in a `TrackGroup`.
    Store-path reads use copy-safe manifests so writes can stage updates before
    commit, while read-path calls may reuse the cached manifest directly.

    Store writes media objects before persisting the authoritative group
    manifest. If manifest persistence fails after object upload succeeds, the
    uploaded objects remain in storage and an explicit sync error is raised so
    manual cleanup can remove the listed keys later. Manifest cache updates
    happen only after manifest persistence succeeds.

    This phase is intentionally conservative:
        - `store()` creates new tracks and writes only the original track and mandatory cover.
        - `update()` mutates authoritative components of existing tracks in place.
        - No hashing or deduplication is performed.
        - Concurrent writes to the same `TrackGroup` are unsupported.
        - `fetch()` returns generated variants only and may lazily regenerate stale caches.
    """

    def __init__(self, s3_client: S3Client, *, preset_store: PresetStore) -> None:
        """Initialize the store with an opened generic S3 client and preset store.

        The caller constructs and owns the `PresetStore` instance passed here.
        """
        if preset_store._s3_client is not s3_client:
            raise ValueError('TrackStore and PresetStore must share the same S3 client instance')

        self._s3_client = s3_client
        self._preset_store = preset_store
        self._manifest_cache: dict[Prefix, Manifest] = {}

    async def list_groups(self) -> list[TrackGroup]:
        """List all discovered track groups from stored S3 prefixes.

        This method relies on `S3Client.list_subprefixes('tracks')` returning
        only immediate prefixes and not ordinary files. That means
        `tracks/presets.json` is not part of the returned collection and no
        file-specific ignore list is needed here.
        """
        track_group_prefixes = await self._s3_client.list_subprefixes(prefix=_TRACKS_PREFIX)
        track_groups = [self._parse_track_group_prefix(prefix) for prefix in track_group_prefixes]
        return sorted(track_groups, key=lambda group: (group.universe.order(), group.year, int(group.season)))

    async def list_tracks(self, group: TrackGroup) -> dict[SubSeason, list[TrackInfo]]:
        """List discovery metadata for all tracks in a group, grouped by sub-season.

        Returned items are discovery metadata only. Sub-seasons are ordered by
        `SubSeason.order()`. Tracks within each sub-season are ordered by the
        authoritative manifest's ascending internal `order`, but that internal
        `order` is intentionally not exposed in the public return type.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackGroupNotFoundError: If the requested group manifest does not exist.
            TrackManifestCorruptedError: If the group manifest exists but is malformed.
        """
        manifest = await self._require_group_manifest(group, sub_season=None)
        grouped_entries: dict[SubSeason, list[ManifestEntry]] = {}
        for entry in manifest:
            grouped_entries.setdefault(entry.sub_season, []).append(entry)
        return {
            sub_season: [
                TrackInfo(
                    id=entry.id,
                    artists=entry.artists,
                    title=entry.title,
                    has_instrumental=entry.has_instrumental,
                )
                for entry in sorted(grouped_entries[sub_season], key=lambda entry: entry.order)
            ]
            for sub_season in sorted(grouped_entries, key=lambda value: value.order())
        }

    async def store(
        self,
        track: Track,
        *,
        group: TrackGroup,
        sub_season: SubSeason,
        preset_id: PresetId | None = None,
    ) -> None:
        """Store one original track plus its mandatory cover object.

        `track.audio_bytes` must already be Opus data and `track.cover_bytes` must already
        be JPEG data. This phase treats those media formats as a caller
        contract and does not perform expensive media validation.

        If `preset_id` is omitted, the current default preset is used for the
        new track's initial manifest metadata. If provided, it must refer to an
        existing stored preset.

        Invariant:
            All stored audio must have a sample rate of exactly 48_000 Hz.

        Validation is strict and performed before any S3 writes. No resampling
        is performed.

        Store creates the target group implicitly by writing its first manifest
        if that group does not yet exist. Track and cover objects are uploaded
        first, then the authoritative manifest is persisted. If a later staged
        upload or manifest persistence fails after one or more objects were
        written, those uploaded objects remain in storage and an explicit sync
        error is raised for manual recovery.

        This phase does not perform hashing or deduplication. Every successful
        call creates a new manifest entry and stores a new original track object
        with a fresh UUIDv7 id. No existing track is overwritten.

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
            ValueError: If `preset_id` is provided but does not refer to a known preset.
            TrackInvalidAudioFormatError: If `track.audio_bytes` is not 48_000 Hz audio.
            TrackManifestSyncError: If one or more track-store objects are written but a later stage fails.
        """
        sample_rate = await probe_audio_sample_rate(track.audio_bytes)
        if sample_rate != 48_000:
            raise TrackInvalidAudioFormatError(f'Audio sample rate must be 48000 Hz, got {sample_rate}')

        if preset_id is None:
            resolved_preset = await self._preset_store.default()
        else:
            resolved_preset = await self._preset_store.require(preset_id)
        variant_count = len(self._resolve_variant_specs(resolved_preset.preset))

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
                preset=AppliedPreset(
                    id=resolved_preset.id,
                    version=resolved_preset.version,
                    variant_count=variant_count,
                ),
                has_variants=False,
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

    async def update(
        self,
        group: TrackGroup,
        track_id: TrackId,
        *,
        artists: tuple[str, ...] | None = None,
        title: str | None = None,
        audio_bytes: bytes | None = None,
        instrumental_bytes: bytes | None = None,
        cover_bytes: bytes | None = None,
    ) -> None:
        """Update selected authoritative track components in place.

        This method mutates only the selected authoritative components for the
        provided `(group, track_id)`. Omitted fields remain unchanged.
        `instrumental_bytes` may either attach the first authoritative
        instrumental or replace the existing one.

        Invariant:
            All stored audio must have a sample rate of exactly 48_000 Hz.

        Validation is strict and performed before any S3 writes. No resampling
        is performed.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackManifestCorruptedError: If the target group's manifest exists but is malformed.
            TrackGroupNotFoundError: If the target group's manifest does not exist.
            ValueError: If `track_id` does not exist in the provided group's manifest.
            ValueError: If no update fields are provided.
            TrackInvalidAudioFormatError: If provided audio bytes are not 48_000 Hz audio.
            TrackUpdateManifestSyncError: If one or more object mutations are applied but a later stage fails.
        """
        if (
            artists is None
            and title is None
            and audio_bytes is None
            and instrumental_bytes is None
            and cover_bytes is None
        ):
            raise ValueError('update() requires at least one update field')

        validated_artists = self._validate_update_artists(artists)
        validated_title = self._validate_update_title(title)

        # Resolve authoritative manifest state before validating track-bound audio updates.
        track_group_prefix = self._track_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        manifest = (await self._require_group_manifest(group, sub_season=None)).copy()
        entry = self._require_manifest_entry(manifest, group=group, track_id=track_id)

        validated_audio_bytes = await self._validate_update_audio_bytes(audio_bytes, track_id=track_id)
        validated_instrumental_bytes = await self._validate_update_instrumental_bytes(
            instrumental_bytes,
            track_id=track_id,
        )
        validated_cover_bytes = self._validate_update_cover_bytes(cover_bytes)

        # Set up the authoritative object keys and staged manifest state.
        track_key = self._track_key(track_group_prefix, track_id)
        instrumental_key = self._instrumental_key(track_group_prefix, track_id)
        cover_key = self._cover_key(track_group_prefix, track_id)
        manifest_key = self._manifest_key(track_group_prefix)
        touched_keys: list[Key] = []
        updated_entry = entry

        if validated_artists is not None:
            updated_entry = dataclass_replace(updated_entry, artists=validated_artists)
        if validated_title is not None:
            updated_entry = dataclass_replace(updated_entry, title=validated_title)

        # Original track updates invalidate only the original variant family.
        if validated_audio_bytes is not None and entry.has_variants:
            original_variant_keys = self._variant_storage_keys(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                variant_count=entry.preset.variant_count,
                instrumental=False,
            )
            try:
                await self._s3_client.delete_keys(original_variant_keys)
            except Exception as error:
                if (
                    sync_error := self._build_update_sync_error(
                        error,
                        stage='original_variant_delete',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        assume_touched_keys=original_variant_keys,
                        manifest_key=manifest_key,
                        note_prefix='Original variant delete error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.extend(original_variant_keys)

        if validated_audio_bytes is not None:
            try:
                await self._s3_client.put_bytes(
                    track_key,
                    validated_audio_bytes,
                    content_type=S3ContentType.OPUS,
                )
            except Exception as error:
                if (
                    sync_error := self._build_update_sync_error(
                        error,
                        stage='audio_upload',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        manifest_key=manifest_key,
                        note_prefix='Original audio upload error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.append(track_key)
            updated_entry = dataclass_replace(updated_entry, has_variants=False)

        # Instrumental updates invalidate only the instrumental variant family.
        if validated_instrumental_bytes is not None and entry.has_instrumental_variants:
            instrumental_variant_keys = self._variant_storage_keys(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                variant_count=entry.preset.variant_count,
                instrumental=True,
            )
            try:
                await self._s3_client.delete_keys(instrumental_variant_keys)
            except Exception as error:
                if (
                    sync_error := self._build_update_sync_error(
                        error,
                        stage='instrumental_variant_delete',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        assume_touched_keys=instrumental_variant_keys,
                        manifest_key=manifest_key,
                        note_prefix='Instrumental variant delete error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.extend(instrumental_variant_keys)

        if validated_instrumental_bytes is not None:
            try:
                await self._s3_client.put_bytes(
                    instrumental_key,
                    validated_instrumental_bytes,
                    content_type=S3ContentType.OPUS,
                )
            except Exception as error:
                if (
                    sync_error := self._build_update_sync_error(
                        error,
                        stage='instrumental_upload',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        manifest_key=manifest_key,
                        note_prefix='Instrumental upload error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.append(instrumental_key)
            updated_entry = dataclass_replace(
                updated_entry,
                has_instrumental=True,
                has_instrumental_variants=False,
            )

        # Cover updates do not affect variant state.
        if validated_cover_bytes is not None:
            try:
                await self._s3_client.put_bytes(
                    cover_key,
                    validated_cover_bytes,
                    content_type=S3ContentType.JPEG,
                )
            except Exception as error:
                if (
                    sync_error := self._build_update_sync_error(
                        error,
                        stage='cover_upload',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        manifest_key=manifest_key,
                        note_prefix='Cover upload error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.append(cover_key)

        # Commit the staged manifest only after all requested object mutations succeed.
        rewritten_manifest = self._replace_manifest_entry(
            manifest,
            updated_entry=updated_entry,
        )

        try:
            await self._write_manifest_and_update_cache(
                track_group_prefix=track_group_prefix,
                manifest=rewritten_manifest,
            )
        except Exception as error:
            if (
                sync_error := self._build_update_sync_error(
                    error,
                    stage='manifest_write',
                    track_id=track_id,
                    touched_keys=touched_keys,
                    manifest_key=manifest_key,
                    note_prefix='Update manifest write error',
                )
            ) is None:
                raise
            raise sync_error from error

    async def fetch(
        self,
        group: TrackGroup,
        track_id: TrackId,
        *,
        preset_id: PresetId | None = None,
    ) -> FetchedVariants:
        """Fetch one track's generated variants, materializing stale caches lazily.

        The selected track is identified by `(group, track_id)`. The returned
        payload contains only generated variants plus shared UI metadata:
        cover bytes and a deterministic flat cover filename. The authoritative
        original track and optional authoritative instrumental objects are read
        only when regeneration is required.

        Preset resolution is delegated to `PresetStore`. An explicit
        caller-supplied preset id is strict, while the manifest snapshot id is
        tolerant of stale or deleted presets and falls back to the current
        default preset.

        Staleness is determined only by the manifest's cached `AppliedPreset`
        metadata, the resolved source-of-truth `PresetRecord`, the
        `has_variants` flag, and the `has_instrumental_variants` flag. No S3
        listing is used. Variants are returned in strictly ascending speed
        order across all slowed and sped-up modes, and that same ordering
        determines their stable indexed storage keys.

        Variant generation is intentionally not atomic. If uploads succeed but
        manifest persistence fails, orphaned variant objects may remain in
        storage for manual cleanup, matching the store path's existing staged
        write semantics.

        Raises:
            TrackPresetsCorruptedError: If `tracks/presets.json` exists but is malformed.
            TrackGroupNotFoundError: If the requested group manifest does not exist.
            TrackManifestCorruptedError: If the requested group manifest exists but is malformed.
            ValueError: If `track_id` is missing from the manifest.
            ValueError: If caller-supplied `preset_id` does not refer to a known preset.
        """
        track_group_prefix = self._track_group_prefix(
            universe=group.universe,
            year=group.year,
            season=group.season,
        )
        manifest = await self._require_group_manifest(group, sub_season=None)
        entry = self._require_manifest_entry(manifest, group=group, track_id=track_id)

        if preset_id is not None:
            resolved_stored_preset = await self._preset_store.require(preset_id)
        else:
            try:
                resolved_stored_preset = await self._preset_store.require(entry.preset.id)
            except ValueError:
                resolved_stored_preset = await self._preset_store.default()

        cover_key = self._cover_key(track_group_prefix, track_id)
        cover_bytes = await self._s3_client.get_bytes(cover_key)
        cover_filename = self._s3_key_to_filename(cover_key)

        variant_specs = self._resolve_variant_specs(resolved_stored_preset.preset)
        variant_count = len(variant_specs)
        manifest_key = self._manifest_key(track_group_prefix)
        touched_keys: list[Key] = []

        original_is_current = entry.has_variants and self._is_applied_preset_current(
            entry.preset,
            resolved_preset=resolved_stored_preset,
            variant_count=variant_count,
        )
        instrumental_is_current = (
            entry.has_instrumental
            and entry.has_instrumental_variants
            and self._is_applied_preset_current(
                entry.preset,
                resolved_preset=resolved_stored_preset,
                variant_count=variant_count,
            )
        )

        original_regenerated = False
        if original_is_current:
            variants = await self._load_variants(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                variant_specs=variant_specs,
                instrumental=False,
            )
        else:
            if entry.has_variants:
                original_variant_keys = self._variant_storage_keys(
                    track_group_prefix=track_group_prefix,
                    track_id=track_id,
                    variant_count=entry.preset.variant_count,
                    instrumental=False,
                )
                try:
                    await self._delete_variants(
                        track_group_prefix=track_group_prefix,
                        track_id=track_id,
                        variant_count=entry.preset.variant_count,
                        instrumental=False,
                    )
                except Exception as error:
                    if (
                        sync_error := self._build_fetch_sync_error(
                            error,
                            stage='original_variant_delete',
                            track_id=track_id,
                            touched_keys=touched_keys,
                            assume_touched_keys=original_variant_keys,
                            manifest_key=manifest_key,
                            note_prefix='Original variant delete error',
                        )
                    ) is None:
                        raise
                    raise sync_error from error
                touched_keys.extend(original_variant_keys)

            try:
                source_track_bytes = await self._s3_client.get_bytes(self._track_key(track_group_prefix, track_id))
            except Exception as error:
                if (
                    sync_error := self._build_fetch_sync_error(
                        error,
                        stage='original_source_read',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        manifest_key=manifest_key,
                        note_prefix='Original source read error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            uploaded_original_variant_keys: list[Key] = []
            try:
                variants = await self._generate_and_store_variants(
                    source_bytes=source_track_bytes,
                    track_group_prefix=track_group_prefix,
                    track_id=track_id,
                    variant_specs=variant_specs,
                    instrumental=False,
                    uploaded_keys=uploaded_original_variant_keys,
                )
            except Exception as error:
                if (
                    sync_error := self._build_fetch_sync_error(
                        error,
                        stage='original_variant_upload',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        assume_touched_keys=uploaded_original_variant_keys,
                        manifest_key=manifest_key,
                        note_prefix='Original variant upload error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.extend(uploaded_original_variant_keys)
            original_regenerated = True

        instrumental_variants: tuple[FetchedVariant, ...] | None
        instrumental_regenerated = False
        if not entry.has_instrumental:
            instrumental_variants = None
        elif instrumental_is_current:
            instrumental_variants = await self._load_variants(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                variant_specs=variant_specs,
                instrumental=True,
            )
        else:
            if entry.has_instrumental_variants:
                instrumental_variant_keys = self._variant_storage_keys(
                    track_group_prefix=track_group_prefix,
                    track_id=track_id,
                    variant_count=entry.preset.variant_count,
                    instrumental=True,
                )
                try:
                    await self._delete_variants(
                        track_group_prefix=track_group_prefix,
                        track_id=track_id,
                        variant_count=entry.preset.variant_count,
                        instrumental=True,
                    )
                except Exception as error:
                    if (
                        sync_error := self._build_fetch_sync_error(
                            error,
                            stage='instrumental_variant_delete',
                            track_id=track_id,
                            touched_keys=touched_keys,
                            assume_touched_keys=instrumental_variant_keys,
                            manifest_key=manifest_key,
                            note_prefix='Instrumental variant delete error',
                        )
                    ) is None:
                        raise
                    raise sync_error from error
                touched_keys.extend(instrumental_variant_keys)

            try:
                source_instrumental_bytes = await self._s3_client.get_bytes(
                    self._instrumental_key(track_group_prefix, track_id)
                )
            except Exception as error:
                if (
                    sync_error := self._build_fetch_sync_error(
                        error,
                        stage='instrumental_source_read',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        manifest_key=manifest_key,
                        note_prefix='Instrumental source read error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            uploaded_instrumental_variant_keys: list[Key] = []
            try:
                instrumental_variants = await self._generate_and_store_variants(
                    source_bytes=source_instrumental_bytes,
                    track_group_prefix=track_group_prefix,
                    track_id=track_id,
                    variant_specs=variant_specs,
                    instrumental=True,
                    uploaded_keys=uploaded_instrumental_variant_keys,
                )
            except Exception as error:
                if (
                    sync_error := self._build_fetch_sync_error(
                        error,
                        stage='instrumental_variant_upload',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        assume_touched_keys=uploaded_instrumental_variant_keys,
                        manifest_key=manifest_key,
                        note_prefix='Instrumental variant upload error',
                    )
                ) is None:
                    raise
                raise sync_error from error
            touched_keys.extend(uploaded_instrumental_variant_keys)
            instrumental_regenerated = True

        if original_regenerated or instrumental_regenerated:
            try:
                await self._write_manifest_and_update_cache(
                    track_group_prefix=track_group_prefix,
                    manifest=self._replace_manifest_entry(
                        manifest,
                        updated_entry=dataclass_replace(
                            entry,
                            preset=AppliedPreset(
                                id=resolved_stored_preset.id,
                                version=resolved_stored_preset.version,
                                variant_count=variant_count,
                            ),
                            has_variants=True if original_regenerated else entry.has_variants,
                            has_instrumental_variants=(
                                True if instrumental_regenerated else entry.has_instrumental_variants
                            ),
                        ),
                    ),
                )
            except Exception as error:
                if (
                    sync_error := self._build_fetch_sync_error(
                        error,
                        stage='manifest_write',
                        track_id=track_id,
                        touched_keys=touched_keys,
                        manifest_key=manifest_key,
                        note_prefix='Fetch manifest write error',
                    )
                ) is None:
                    raise
                raise sync_error from error

        return FetchedVariants(
            track_id=entry.id,
            artists=entry.artists,
            title=entry.title,
            cover_filename=cover_filename,
            cover_bytes=cover_bytes,
            variants=variants,
            instrumental_variants=instrumental_variants,
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
            return Manifest.from_dict(decoded_manifest)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise TrackManifestCorruptedError(manifest_key, str(error)) from error

    async def _write_manifest_and_update_cache(
        self,
        *,
        track_group_prefix: Prefix,
        manifest: Manifest,
    ) -> None:
        manifest_key = self._manifest_key(track_group_prefix)
        manifest_payload = json.dumps(manifest.to_dict(), separators=(',', ':')).encode('utf-8')
        await self._s3_client.put_bytes(
            manifest_key,
            manifest_payload,
            content_type=S3ContentType.JSON,
        )
        self._manifest_cache[track_group_prefix] = manifest.copy()

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

    def _variant_key(self, track_group_prefix: Prefix, track_id: TrackId, *, index: int) -> Key:
        validated_index = _expect_positive_int(index, field='index', context='variant key')
        object_name = f'{track_id}{_VARIANT_MODE_SEPARATOR}variant{_VARIANT_MODE_SEPARATOR}{validated_index}'
        return S3Client.join(track_group_prefix, object_name + _TRACK_SUFFIX)

    def _instrumental_variant_key(self, track_group_prefix: Prefix, track_id: TrackId, *, index: int) -> Key:
        validated_index = _expect_positive_int(index, field='index', context='instrumental variant key')
        object_name = (
            f'{track_id}{_VARIANT_MODE_SEPARATOR}instrumental{_VARIANT_MODE_SEPARATOR}'
            f'variant{_VARIANT_MODE_SEPARATOR}{validated_index}'
        )
        return S3Client.join(track_group_prefix, object_name + _TRACK_SUFFIX)

    def _new_track_id(self, *, manifest: Manifest) -> TrackId:
        while True:
            track_id = _uuid7().hex
            if not manifest.has_id(track_id):
                return track_id

    def _require_manifest_entry(self, manifest: Manifest, *, group: TrackGroup, track_id: TrackId) -> ManifestEntry:
        for entry in manifest:
            if entry.id == track_id:
                return entry

        raise ValueError(
            f'Track id {track_id} does not exist in group {group.universe.value}-{group.year}-{int(group.season)}'
        )

    def _is_applied_preset_current(
        self,
        applied_preset: AppliedPreset,
        *,
        resolved_preset: PresetRecord,
        variant_count: int,
    ) -> bool:
        return (
            applied_preset.id == resolved_preset.id
            and applied_preset.version == resolved_preset.version
            and applied_preset.variant_count == variant_count
        )

    def _resolve_variant_specs(self, preset: Preset) -> tuple[_ResolvedVariantSpec, ...]:
        # This final ascending-speed order is a storage invariant: variant
        # index N maps to the Nth sorted spec in persistent S3 keys. Changing
        # the ordering logic without explicit migration/invalidation can make
        # existing indexed variant objects point to different semantics.
        variant_specs: list[_ResolvedVariantSpec] = []
        if preset.slowed is not None:
            for level in range(1, preset.slowed.levels + 1):
                variant_specs.append(
                    _ResolvedVariantSpec(
                        speed=1.0 - level * preset.slowed.step,
                        reverb=preset.reverb_start + (level - 1) * preset.reverb_step,
                    )
                )
        if preset.sped_up is not None:
            for level in range(1, preset.sped_up.levels + 1):
                variant_specs.append(
                    _ResolvedVariantSpec(
                        speed=1.0 + level * preset.sped_up.step,
                        reverb=preset.reverb_start + (level - 1) * preset.reverb_step,
                    )
                )

        return tuple(sorted(variant_specs, key=lambda spec: spec.speed))

    async def _load_variants(
        self,
        *,
        track_group_prefix: Prefix,
        track_id: TrackId,
        variant_specs: tuple[_ResolvedVariantSpec, ...],
        instrumental: bool,
    ) -> tuple[FetchedVariant, ...]:
        variants: list[FetchedVariant] = []
        for index, spec in enumerate(variant_specs, start=1):
            variant_key = self._variant_storage_key(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                index=index,
                instrumental=instrumental,
            )
            variant_bytes = await self._s3_client.get_bytes(variant_key)
            variants.append(
                FetchedVariant(
                    speed=spec.speed,
                    reverb=spec.reverb,
                    audio_bytes=variant_bytes,
                )
            )

        return tuple(variants)

    async def _generate_and_store_variants(
        self,
        *,
        source_bytes: bytes,
        track_group_prefix: Prefix,
        track_id: TrackId,
        variant_specs: tuple[_ResolvedVariantSpec, ...],
        instrumental: bool,
        uploaded_keys: list[Key] | None = None,
    ) -> tuple[FetchedVariant, ...]:
        variants: list[FetchedVariant] = []
        for index, spec in enumerate(variant_specs, start=1):
            generated_bytes = await create_audio_variant(
                source_bytes,
                speed=spec.speed,
                reverb=spec.reverb,
            )
            variant_key = self._variant_storage_key(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                index=index,
                instrumental=instrumental,
            )
            await self._s3_client.put_bytes(
                variant_key,
                generated_bytes,
                content_type=S3ContentType.OPUS,
            )
            if uploaded_keys is not None:
                uploaded_keys.append(variant_key)
            variants.append(
                FetchedVariant(
                    speed=spec.speed,
                    reverb=spec.reverb,
                    audio_bytes=generated_bytes,
                )
            )

        return tuple(variants)

    async def _delete_variants(
        self,
        *,
        track_group_prefix: Prefix,
        track_id: TrackId,
        variant_count: int,
        instrumental: bool,
    ) -> None:
        await self._s3_client.delete_keys(
            self._variant_storage_keys(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                variant_count=variant_count,
                instrumental=instrumental,
            )
        )

    def _variant_storage_keys(
        self,
        *,
        track_group_prefix: Prefix,
        track_id: TrackId,
        variant_count: int,
        instrumental: bool,
    ) -> list[Key]:
        return [
            self._variant_storage_key(
                track_group_prefix=track_group_prefix,
                track_id=track_id,
                index=index,
                instrumental=instrumental,
            )
            for index in range(1, variant_count + 1)
        ]

    def _validate_update_artists(self, artists: tuple[str, ...] | None) -> tuple[str, ...] | None:
        if artists is None:
            return None
        if not isinstance(artists, tuple):
            raise ValueError('artists must be a tuple')
        if not artists:
            raise ValueError('artists must not be empty')
        if any(not isinstance(artist, str) for artist in artists):
            raise ValueError('artists entries must be strings')
        if any(not artist.strip() for artist in artists):
            raise ValueError('artists entries must be non-empty strings')
        return artists

    def _validate_update_title(self, title: str | None) -> str | None:
        if title is None:
            return None
        if not isinstance(title, str):
            raise ValueError('title must be a string')
        if not title.strip():
            raise ValueError('title must be a non-empty string')
        return title

    def _validate_update_cover_bytes(self, cover_bytes: bytes | None) -> bytes | None:
        if cover_bytes is None:
            return None
        if not isinstance(cover_bytes, bytes):
            raise ValueError('cover_bytes must be bytes')
        if not cover_bytes:
            raise ValueError('cover_bytes must not be empty')
        return cover_bytes

    async def _validate_update_audio_bytes(
        self,
        audio_bytes: bytes | None,
        *,
        track_id: TrackId,
    ) -> bytes | None:
        if audio_bytes is None:
            return None
        if not isinstance(audio_bytes, bytes):
            raise ValueError('audio_bytes must be bytes')
        if not audio_bytes:
            raise ValueError('audio_bytes must not be empty')

        sample_rate = await probe_audio_sample_rate(audio_bytes)
        if sample_rate != 48_000:
            raise TrackInvalidAudioFormatError(
                f'Audio sample rate must be 48000 Hz, got {sample_rate}',
                track_id=track_id,
            )
        return audio_bytes

    async def _validate_update_instrumental_bytes(
        self,
        instrumental_bytes: bytes | None,
        *,
        track_id: TrackId,
    ) -> bytes | None:
        if instrumental_bytes is None:
            return None
        if not isinstance(instrumental_bytes, bytes):
            raise ValueError('instrumental_bytes must be bytes')
        if not instrumental_bytes:
            raise ValueError('instrumental_bytes must not be empty')

        sample_rate = await probe_audio_sample_rate(instrumental_bytes)
        if sample_rate != 48_000:
            raise TrackInvalidAudioFormatError(
                f'Audio sample rate must be 48000 Hz, got {sample_rate}',
                track_id=track_id,
            )
        return instrumental_bytes

    def _build_update_sync_error(
        self,
        error: Exception,
        *,
        stage: str,
        track_id: TrackId,
        touched_keys: list[Key],
        assume_touched_keys: Iterable[Key] | None = None,
        manifest_key: Key,
        note_prefix: str,
    ) -> TrackUpdateManifestSyncError | None:
        effective_keys = self._merge_touched_keys(
            touched_keys=touched_keys,
            assume_touched_keys=assume_touched_keys,
        )
        if not effective_keys:
            return None

        sync_error = TrackUpdateManifestSyncError(
            stage=stage,
            track_id=track_id,
            touched_keys=effective_keys,
            manifest_key=manifest_key,
        )
        sync_error.add_note(f'{note_prefix}: {error!r}')
        return sync_error

    def _build_fetch_sync_error(
        self,
        error: Exception,
        *,
        stage: str,
        track_id: TrackId,
        touched_keys: list[Key],
        assume_touched_keys: Iterable[Key] | None = None,
        manifest_key: Key,
        note_prefix: str,
    ) -> TrackFetchManifestSyncError | None:
        effective_keys = self._merge_touched_keys(
            touched_keys=touched_keys,
            assume_touched_keys=assume_touched_keys,
        )
        if not effective_keys:
            return None

        sync_error = TrackFetchManifestSyncError(
            stage=stage,
            track_id=track_id,
            touched_keys=effective_keys,
            manifest_key=manifest_key,
        )
        sync_error.add_note(f'{note_prefix}: {error!r}')
        return sync_error

    @staticmethod
    def _merge_touched_keys(
        *,
        touched_keys: Iterable[Key],
        assume_touched_keys: Iterable[Key] | None = None,
    ) -> tuple[Key, ...]:
        merged_keys: list[Key] = []
        for key in (*tuple(touched_keys), *tuple(assume_touched_keys or ())):
            if key not in merged_keys:
                merged_keys.append(key)
        return tuple(merged_keys)

    def _variant_storage_key(
        self,
        *,
        track_group_prefix: Prefix,
        track_id: TrackId,
        index: int,
        instrumental: bool,
    ) -> Key:
        if instrumental:
            return self._instrumental_variant_key(track_group_prefix, track_id, index=index)
        return self._variant_key(track_group_prefix, track_id, index=index)

    def _replace_manifest_entry(
        self,
        manifest: Manifest,
        *,
        updated_entry: ManifestEntry,
    ) -> Manifest:
        return Manifest([updated_entry if entry.id == updated_entry.id else entry for entry in manifest])

    @staticmethod
    def _s3_key_to_filename(storage_key: Key) -> Filename:
        """Return a flat filename for TrackStore-generated keys.

        This mapping is reversible only for TrackStore-generated keys that use
        the store's own storage layout.
        """
        return _FILENAME_S3_DELIMITER_ESCAPE.join(S3Client.split(storage_key))

    @staticmethod
    def _filename_to_s3_key(filename: Filename) -> Key:
        """Return the TrackStore storage key for a store-generated flat filename."""
        return S3Client.join(*filename.split(_FILENAME_S3_DELIMITER_ESCAPE))


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


def _applied_preset_to_dict(preset: AppliedPreset) -> dict[str, object]:
    return {
        'id': preset.id,
        'version': preset.version,
        'variant_count': preset.variant_count,
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


def _parse_applied_preset(value: object, *, context: str) -> AppliedPreset:
    if not isinstance(value, dict):
        raise ValueError(f'{context} `preset` must be an object')
    if set(value) != {'id', 'version', 'variant_count'}:
        raise ValueError(f'{context} `preset` has unexpected fields')

    return AppliedPreset(
        id=_expect_positive_int(value['id'], field='id', context=f'{context} `preset`'),
        version=_expect_positive_int(value['version'], field='version', context=f'{context} `preset`'),
        variant_count=_expect_positive_int(
            value['variant_count'], field='variant_count', context=f'{context} `preset`'
        ),
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


def _format_sub_season(sub_season: SubSeason) -> str:
    return sub_season.value.title()


def _format_optional_sub_season(sub_season: SubSeason | None) -> str:
    if sub_season is None:
        return 'None'
    return _format_sub_season(sub_season)
