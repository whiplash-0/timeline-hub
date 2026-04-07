import json
import re
import uuid

import pytest

import general_bot.services.track_store as track_store_module
from general_bot.infra.s3 import S3Client, S3ObjectNotFoundError
from general_bot.services.track_store import (
    AppliedPreset,
    FetchedVariant,
    FetchedVariants,
    Manifest,
    ManifestEntry,
    Preset,
    PresetMode,
    PresetRecord,
    Presets,
    PresetStore,
    Season,
    SubSeason,
    Track,
    TrackFetchManifestSyncError,
    TrackGroup,
    TrackGroupNotFoundError,
    TrackInfo,
    TrackInvalidAudioFormatError,
    TrackManifestCorruptedError,
    TrackManifestSyncError,
    TrackPresetsCorruptedError,
    TrackStore,
    TrackUniverse,
    TrackUpdateManifestSyncError,
)

_UUID_1 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e1').hex
_UUID_2 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e2').hex
_UUID_3 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e3').hex


class _FakeS3Client:
    def __init__(
        self,
        objects: dict[str, bytes] | None = None,
        *,
        prefixes: list[str] | None = None,
        put_failures: set[str] | None = None,
        delete_failures: set[str] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.prefixes = list(prefixes or [])
        self.put_failures = set(put_failures or set())
        self.delete_failures = set(delete_failures or set())
        self.put_calls: list[tuple[str, bytes, str | None]] = []
        self.get_calls: list[str] = []
        self.list_subprefixes_calls: list[str | None] = []
        self.deleted_keys: list[str] = []
        self.delete_keys_calls: list[tuple[str, ...]] = []

    async def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        self.put_calls.append((key, data, content_type))
        if key in self.put_failures:
            raise RuntimeError(f'boom putting {key}')
        self.objects[key] = data

    async def get_bytes(self, key: str) -> bytes:
        self.get_calls.append(key)
        try:
            return self.objects[key]
        except KeyError as error:
            raise S3ObjectNotFoundError(key) from error

    async def list_subprefixes(self, prefix: str | None = None) -> list[str]:
        self.list_subprefixes_calls.append(prefix)
        if prefix is None:
            return list(self.prefixes)

        expected_parts = S3Client.split(prefix)
        return [
            candidate
            for candidate in self.prefixes
            if S3Client.split(candidate)[: len(expected_parts)] == expected_parts
        ]

    async def delete_key(self, key: str) -> None:
        if key in self.delete_failures:
            raise RuntimeError(f'boom deleting {key}')
        self.deleted_keys.append(key)
        self.objects.pop(key, None)

    async def delete_keys(self, keys: list[str]) -> int:
        key_list = list(keys)
        self.delete_keys_calls.append(tuple(key_list))
        for key in key_list:
            await self.delete_key(key)
        return len(key_list)


def _track_group_prefix(*, universe: TrackUniverse, year: int, season: Season) -> str:
    return S3Client.join('tracks', f'{universe.value}-{year}-{int(season)}')


def _manifest_key(*, universe: TrackUniverse, year: int, season: Season) -> str:
    return S3Client.join(_track_group_prefix(universe=universe, year=year, season=season), 'manifest.json')


def _presets_key() -> str:
    return S3Client.join('tracks', 'presets.json')


def _track_key(*, universe: TrackUniverse, year: int, season: Season, track_id: str) -> str:
    return S3Client.join(_track_group_prefix(universe=universe, year=year, season=season), track_id + '.opus')


def _cover_key(*, universe: TrackUniverse, year: int, season: Season, track_id: str) -> str:
    return S3Client.join(_track_group_prefix(universe=universe, year=year, season=season), track_id + '-cover.jpg')


def _instrumental_key(*, universe: TrackUniverse, year: int, season: Season, track_id: str) -> str:
    return S3Client.join(
        _track_group_prefix(universe=universe, year=year, season=season),
        track_id + '-instrumental.opus',
    )


def _preset(
    *,
    name: str,
    slowed: PresetMode | None = None,
    sped_up: PresetMode | None = None,
    reverb_start: float = 0.01,
    reverb_step: float = 0.01,
) -> Preset:
    return Preset(
        name=name,
        slowed=slowed,
        sped_up=sped_up,
        reverb_start=reverb_start,
        reverb_step=reverb_step,
    )


def _stored_preset(
    *,
    preset_id: int,
    version: int,
    preset: Preset,
) -> PresetRecord:
    return PresetRecord(
        id=preset_id,
        version=version,
        preset=preset,
    )


def _applied_preset(
    *,
    preset_id: int = 1,
    version: int = 3,
    preset: Preset | None = None,
    variant_count: int | None = None,
) -> AppliedPreset:
    resolved_preset = preset or _sample_stored_presets()[0].preset
    resolved_variant_count = (
        len(_store(_FakeS3Client())._resolve_variant_specs(resolved_preset)) if variant_count is None else variant_count
    )
    return AppliedPreset(
        id=preset_id,
        version=version,
        variant_count=resolved_variant_count,
    )


def _bootstrap_preset() -> Preset:
    return _preset(
        name='Default',
        slowed=PresetMode(step=0.08, levels=4),
        sped_up=PresetMode(step=0.04, levels=2),
        reverb_start=0.03,
        reverb_step=0.02,
    )


def _sample_stored_presets() -> list[PresetRecord]:
    return [
        _stored_preset(
            preset_id=1,
            version=3,
            preset=_preset(
                name='default',
                slowed=PresetMode(step=0.06, levels=3),
                sped_up=PresetMode(step=0.06, levels=2),
                reverb_start=0.01,
                reverb_step=0.01,
            ),
        ),
        _stored_preset(
            preset_id=2,
            version=1,
            preset=_preset(
                name='soft',
                slowed=PresetMode(step=0.05, levels=2),
                sped_up=None,
                reverb_start=0.02,
                reverb_step=0.01,
            ),
        ),
    ]


def _presets_bytes(*, presets: list[PresetRecord] | None = None) -> bytes:
    return json.dumps(
        Presets(
            presets=list(presets or _sample_stored_presets()),
        ).to_dict(),
        separators=(',', ':'),
    ).encode('utf-8')


def _presets_payload(*, presets: list[PresetRecord] | None = None) -> dict[str, list[dict[str, object]]]:
    return Presets(
        presets=list(presets or _sample_stored_presets()),
    ).to_dict()


def _applied_preset_dict(applied_preset: AppliedPreset) -> dict[str, object]:
    return {
        'id': applied_preset.id,
        'version': applied_preset.version,
        'variant_count': applied_preset.variant_count,
    }


def _manifest_bytes(entries: list[ManifestEntry]) -> bytes:
    return json.dumps(Manifest(entries).to_dict(), separators=(',', ':')).encode('utf-8')


def _manifest_payload(entries: list[ManifestEntry]) -> dict[str, list[dict[str, object]]]:
    return Manifest(entries).to_dict()


def _entry(
    *,
    id: str = _UUID_1,
    artists: tuple[str, ...] = ('artist',),
    title: str = 'title',
    sub_season: SubSeason = SubSeason.A,
    order: int = 1,
    preset: AppliedPreset | None = None,
    has_variants: bool = False,
    has_instrumental: bool = False,
    has_instrumental_variants: bool = False,
) -> ManifestEntry:
    return ManifestEntry(
        id=id,
        artists=artists,
        title=title,
        sub_season=sub_season,
        order=order,
        preset=_applied_preset() if preset is None else preset,
        has_variants=has_variants,
        has_instrumental=has_instrumental,
        has_instrumental_variants=has_instrumental_variants,
    )


def _patch_uuid7(monkeypatch: pytest.MonkeyPatch, *track_ids: str) -> None:
    uuids = iter(uuid.UUID(track_id) for track_id in track_ids)
    monkeypatch.setattr(track_store_module, '_uuid7', lambda: next(uuids))


def _store(
    s3_client: _FakeS3Client,
    *,
    bootstrap_preset: Preset | None = None,
    preset_store: PresetStore | None = None,
) -> TrackStore:
    return TrackStore(
        s3_client,
        preset_store=(
            preset_store
            if preset_store is not None
            else _preset_store(
                s3_client,
                bootstrap_preset=bootstrap_preset,
            )
        ),
    )


def _preset_store(s3_client: _FakeS3Client, *, bootstrap_preset: Preset | None = None) -> PresetStore:
    return PresetStore(
        s3_client,
        bootstrap_preset=bootstrap_preset or _bootstrap_preset(),
    )


def _track(
    *,
    artists: tuple[str, ...] = ('artist',),
    title: str = 'title',
    audio_bytes: bytes = b'track',
    cover_bytes: bytes = b'cover',
) -> Track:
    return Track(
        artists=artists,
        title=title,
        audio_bytes=audio_bytes,
        cover_bytes=cover_bytes,
    )


def test_season_from_month_uses_exact_mapping() -> None:
    assert Season.from_month(2) is Season.S1
    assert Season.from_month(3) is Season.S2
    assert Season.from_month(6) is Season.S3
    assert Season.from_month(9) is Season.S4
    assert Season.from_month(12) is Season.S5


def test_presets_from_list_accepts_data_root_schema() -> None:
    parsed = Presets.from_dict(json.loads(_presets_bytes().decode('utf-8')))

    assert [preset.id for preset in parsed.presets] == [1, 2]
    assert parsed.default_preset() == parsed.presets[0]
    assert parsed.presets[1].version == 1
    assert parsed.presets[1].preset.name == 'soft'
    assert parsed.presets[1].preset.sped_up is None


def test_preset_mode_rejects_negative_step() -> None:
    with pytest.raises(ValueError, match='PresetMode.step must be >= 0'):
        PresetMode(step=-0.01, levels=1)


def test_preset_mode_rejects_non_positive_levels() -> None:
    with pytest.raises(ValueError, match='PresetMode.levels must be >= 1'):
        PresetMode(step=0.01, levels=0)


def test_preset_rejects_empty_name() -> None:
    with pytest.raises(ValueError, match='Preset.name must be a non-empty string'):
        _preset(name='')


def test_preset_rejects_negative_reverb_start() -> None:
    with pytest.raises(ValueError, match='Preset.reverb_start must be >= 0'):
        _preset(name='default', reverb_start=-0.01)


def test_preset_rejects_missing_all_variant_modes() -> None:
    with pytest.raises(ValueError, match='Preset must define at least one of slowed or sped_up'):
        Preset(
            name='default',
            slowed=None,
            sped_up=None,
            reverb_start=0.01,
            reverb_step=0.01,
        )


@pytest.mark.parametrize(
    ('kwargs', 'message'),
    [
        ({'artists': ['artist']}, 'Track.artists must be a tuple'),
        ({'artists': ()}, 'Track.artists must not be empty'),
        ({'artists': ('artist', 1)}, 'Track.artists entries must be strings'),
        ({'artists': ('artist', '   ')}, 'Track.artists entries must be non-empty strings'),
        ({'title': 1}, 'Track.title must be a string'),
        ({'title': '   '}, 'Track.title must be a non-empty string'),
        ({'audio_bytes': 'track'}, 'Track.audio_bytes must be bytes'),
        ({'audio_bytes': b''}, 'Track.audio_bytes must not be empty'),
        ({'cover_bytes': 'cover'}, 'Track.cover_bytes must be bytes'),
        ({'cover_bytes': b''}, 'Track.cover_bytes must not be empty'),
    ],
)
def test_track_rejects_invalid_fields(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        Track(
            artists=kwargs.get('artists', ('artist',)),
            title=kwargs.get('title', 'title'),
            audio_bytes=kwargs.get('audio_bytes', b'track'),
            cover_bytes=kwargs.get('cover_bytes', b'cover'),
        )


@pytest.mark.parametrize(
    ('kwargs', 'message'),
    [
        ({'id': 0, 'version': 1}, 'PresetRecord.id must be >= 1'),
        ({'id': 1, 'version': 0}, 'PresetRecord.version must be >= 1'),
    ],
)
def test_preset_record_rejects_non_positive_identity_fields(
    kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        PresetRecord(
            id=kwargs['id'],
            version=kwargs['version'],
            preset=_preset(name='default', slowed=PresetMode(step=0.05, levels=1)),
        )


def test_presets_reject_duplicate_stored_preset_ids() -> None:
    payload = {
        'data': [
            {
                'id': 1,
                'version': 1,
                'preset': {
                    'name': 'default',
                    'slowed': {'step': 0.01, 'levels': 1},
                    'sped_up': None,
                    'reverb_start': 0.0,
                    'reverb_step': 0.0,
                },
            },
            {
                'id': 1,
                'version': 2,
                'preset': {
                    'name': 'other',
                    'slowed': {'step': 0.02, 'levels': 1},
                    'sped_up': None,
                    'reverb_start': 0.1,
                    'reverb_step': 0.1,
                },
            },
        ],
    }

    with pytest.raises(ValueError, match='duplicate stored preset id'):
        Presets.from_dict(payload)


def test_presets_reject_empty_list_construction() -> None:
    with pytest.raises(ValueError, match='Presets.presets must not be empty'):
        Presets(presets=[])


def test_presets_from_list_rejects_old_list_root_schema() -> None:
    with pytest.raises(ValueError, match="presets root must be an object with only 'data'"):
        Presets.from_dict(
            [
                {
                    'id': 1,
                    'version': 1,
                    'preset': {
                        'name': 'default',
                        'slowed': {'step': 0.01, 'levels': 1},
                        'sped_up': None,
                        'reverb_start': 0.0,
                        'reverb_step': 0.0,
                    },
                }
            ]
        )


def test_presets_from_list_rejects_old_object_root_schema() -> None:
    payload = {
        'presets': [
            {
                'id': 1,
                'version': 1,
                'preset': {
                    'name': 'default',
                    'slowed': {'step': 0.01, 'levels': 1},
                    'sped_up': None,
                    'reverb_start': 0.0,
                    'reverb_step': 0.0,
                },
            }
        ],
    }

    with pytest.raises(ValueError, match="presets root must be an object with only 'data'"):
        Presets.from_dict(payload)


def test_presets_default_preset_returns_stored_preset() -> None:
    presets = Presets(
        presets=_sample_stored_presets(),
    )

    assert presets.default_preset() == presets.presets[0]


def test_presets_serialization_preserves_insertion_order() -> None:
    original = Presets(
        presets=[
            _stored_preset(
                preset_id=2,
                version=1,
                preset=_preset(
                    name='soft',
                    slowed=PresetMode(step=0.05, levels=1),
                    sped_up=None,
                    reverb_start=0.02,
                    reverb_step=0.01,
                ),
            ),
            _stored_preset(
                preset_id=1,
                version=3,
                preset=_preset(
                    name='default',
                    slowed=PresetMode(step=0.06, levels=3),
                    sped_up=PresetMode(step=0.06, levels=2),
                    reverb_start=0.01,
                    reverb_step=0.01,
                ),
            ),
        ],
    )

    parsed = Presets.from_dict(original.to_dict())

    assert [preset.id for preset in parsed.presets] == [2, 1]
    assert parsed.to_dict() == original.to_dict()


def test_manifest_uses_data_root_with_preferred_field_order() -> None:
    entry = _entry(
        id=_UUID_1,
        artists=('artist',),
        title='title',
        sub_season=SubSeason.A,
        order=1,
        preset=_applied_preset(),
        has_variants=False,
        has_instrumental=False,
        has_instrumental_variants=False,
    )

    payload = Manifest([entry]).to_dict()

    assert payload == {
        'data': [
            {
                'id': _UUID_1,
                'artists': ['artist'],
                'title': 'title',
                'sub_season': 'A',
                'order': 1,
                'preset': {'id': 1, 'version': 3, 'variant_count': 5},
                'has_variants': False,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            }
        ]
    }
    assert list(payload['data'][0]) == [
        'id',
        'artists',
        'title',
        'sub_season',
        'order',
        'preset',
        'has_variants',
        'has_instrumental',
        'has_instrumental_variants',
    ]
    assert list(Manifest.from_dict(payload)) == [entry]


def test_manifest_round_trips_has_instrumental_variants() -> None:
    entry = _entry(
        id=_UUID_1,
        artists=('artist',),
        title='title',
        sub_season=SubSeason.A,
        order=1,
        preset=_applied_preset(
            preset_id=2,
            version=5,
            preset=_preset(name='snap', slowed=PresetMode(step=0.05, levels=2), sped_up=None),
        ),
        has_variants=True,
        has_instrumental=True,
        has_instrumental_variants=True,
    )

    payload = Manifest([entry]).to_dict()

    assert payload == {
        'data': [
            {
                'id': _UUID_1,
                'artists': ['artist'],
                'title': 'title',
                'sub_season': 'A',
                'order': 1,
                'preset': {'id': 2, 'version': 5, 'variant_count': 2},
                'has_variants': True,
                'has_instrumental': True,
                'has_instrumental_variants': True,
            }
        ]
    }
    assert list(Manifest.from_dict(payload)) == [entry]


def test_manifest_next_order_is_dense_per_sub_season() -> None:
    manifest = Manifest(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
            _entry(
                id=_UUID_2,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.B,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
            _entry(
                id=_UUID_3,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=2,
                preset=None,
                has_instrumental=True,
                has_instrumental_variants=False,
            ),
        ]
    )

    assert manifest.next_order(sub_season=SubSeason.A) == 3
    assert manifest.next_order(sub_season=SubSeason.C) == 1


def test_manifest_rejects_duplicate_sub_season_order_position() -> None:
    with pytest.raises(ValueError, match='duplicate manifest position for sub_season=A order=1'):
        Manifest.from_dict(
            {
                'data': [
                    {
                        'id': _UUID_1,
                        'artists': ['artist'],
                        'title': 'title',
                        'sub_season': 'A',
                        'order': 1,
                        'preset': _applied_preset_dict(_applied_preset()),
                        'has_variants': False,
                        'has_instrumental': False,
                        'has_instrumental_variants': False,
                    },
                    {
                        'id': _UUID_2,
                        'artists': ['artist'],
                        'title': 'title',
                        'sub_season': 'A',
                        'order': 1,
                        'preset': _applied_preset_dict(_applied_preset()),
                        'has_variants': False,
                        'has_instrumental': True,
                        'has_instrumental_variants': False,
                    },
                ]
            }
        )


def test_manifest_rejects_invalid_preset_shape() -> None:
    with pytest.raises(ValueError, match='manifest `preset` has unexpected fields'):
        Manifest.from_dict(
            {
                'data': [
                    {
                        'id': _UUID_1,
                        'artists': ['artist'],
                        'title': 'title',
                        'sub_season': 'A',
                        'order': 1,
                        'preset': {'preset_id': 2, 'version': 5, 'preset': {}},
                        'has_variants': False,
                        'has_instrumental': False,
                        'has_instrumental_variants': False,
                    }
                ]
            }
        )


def test_manifest_rejects_instrumental_variants_without_instrumental() -> None:
    with pytest.raises(ValueError, match=r'manifest `has_instrumental_variants` requires `has_instrumental`'):
        Manifest.from_dict(
            {
                'data': [
                    {
                        'id': _UUID_1,
                        'artists': ['artist'],
                        'title': 'title',
                        'sub_season': 'A',
                        'order': 1,
                        'preset': _applied_preset_dict(
                            _applied_preset(
                                preset_id=2,
                                version=5,
                                preset=_preset(name='snap', slowed=PresetMode(step=0.05, levels=1)),
                                variant_count=1,
                            )
                        ),
                        'has_variants': False,
                        'has_instrumental': False,
                        'has_instrumental_variants': True,
                    }
                ]
            }
        )


def test_manifest_rejects_missing_has_variants() -> None:
    with pytest.raises(ValueError, match='manifest track entry has unexpected fields'):
        Manifest.from_dict(
            {
                'data': [
                    {
                        'id': _UUID_1,
                        'artists': ['artist'],
                        'title': 'title',
                        'sub_season': 'A',
                        'order': 1,
                        'preset': _applied_preset_dict(_applied_preset()),
                        'has_instrumental': True,
                        'has_instrumental_variants': True,
                    }
                ]
            }
        )


@pytest.mark.parametrize(
    ('entry', 'message'),
    [
        (
            {
                'id': _UUID_1,
                'artists': 'artist',
                'title': 'title',
                'sub_season': 'A',
                'order': 1,
                'preset': _applied_preset_dict(_applied_preset()),
                'has_variants': False,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            },
            'manifest `artists` must be a list',
        ),
        (
            {
                'id': _UUID_1,
                'artists': [],
                'title': 'title',
                'sub_season': 'A',
                'order': 1,
                'preset': _applied_preset_dict(_applied_preset()),
                'has_variants': False,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            },
            'manifest `artists` must not be empty',
        ),
        (
            {
                'id': _UUID_1,
                'artists': ['artist', 1],
                'title': 'title',
                'sub_season': 'A',
                'order': 1,
                'preset': _applied_preset_dict(_applied_preset()),
                'has_variants': False,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            },
            'manifest `artists[]` must be a string',
        ),
        (
            {
                'id': _UUID_1,
                'artists': ['artist', '   '],
                'title': 'title',
                'sub_season': 'A',
                'order': 1,
                'preset': _applied_preset_dict(_applied_preset()),
                'has_variants': False,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            },
            'manifest `artists[]` must be a non-empty string',
        ),
        (
            {
                'id': _UUID_1,
                'artists': ['artist'],
                'title': '   ',
                'sub_season': 'A',
                'order': 1,
                'preset': _applied_preset_dict(_applied_preset()),
                'has_variants': False,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            },
            'manifest `title` must be a non-empty string',
        ),
    ],
)
def test_manifest_rejects_invalid_artists_or_title(entry: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        Manifest.from_dict({'data': [entry]})


def test_variant_key_uses_ordered_variant_index() -> None:
    store = _store(_FakeS3Client())
    group_prefix = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)

    assert store._variant_key(group_prefix, _UUID_1, index=1) == S3Client.join(
        group_prefix,
        _UUID_1 + '-variant-1.opus',
    )
    assert store._variant_key(group_prefix, _UUID_1, index=2) == S3Client.join(
        group_prefix,
        _UUID_1 + '-variant-2.opus',
    )


def test_instrumental_variant_key_uses_ordered_variant_index() -> None:
    store = _store(_FakeS3Client())
    group_prefix = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)

    assert store._instrumental_variant_key(group_prefix, _UUID_1, index=1) == S3Client.join(
        group_prefix,
        _UUID_1 + '-instrumental-variant-1.opus',
    )
    assert store._instrumental_variant_key(group_prefix, _UUID_1, index=2) == S3Client.join(
        group_prefix,
        _UUID_1 + '-instrumental-variant-2.opus',
    )


@pytest.mark.asyncio
async def test_preset_store_bootstraps_presets_from_constructor_and_cache() -> None:
    s3_client = _FakeS3Client()
    bootstrap_preset = _bootstrap_preset()
    preset_store = _preset_store(s3_client, bootstrap_preset=bootstrap_preset)

    await preset_store.ensure_ready()

    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == _presets_payload(
        presets=[_stored_preset(preset_id=1, version=1, preset=bootstrap_preset)]
    )
    assert preset_store._presets_cache == Presets(
        presets=[_stored_preset(preset_id=1, version=1, preset=bootstrap_preset)],
    )


@pytest.mark.asyncio
async def test_preset_store_existing_s3_presets_win_over_bootstrap_input() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(
        s3_client,
        bootstrap_preset=_bootstrap_preset(),
    )

    await preset_store.ensure_ready()

    assert preset_store._presets_cache == Presets(
        presets=_sample_stored_presets(),
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8'))['data'][0]['version'] == 3


@pytest.mark.asyncio
async def test_preset_store_all_returns_stored_presets() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.all() == _sample_stored_presets()


@pytest.mark.asyncio
async def test_preset_store_all_returns_shallow_copy_of_cached_list() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    listed_presets = await preset_store.all()

    assert listed_presets == _sample_stored_presets()
    assert listed_presets is not preset_store._presets_cache.presets
    listed_presets.pop()
    assert preset_store._presets_cache.presets == _sample_stored_presets()


@pytest.mark.asyncio
async def test_preset_store_default_returns_current_default_preset() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.default() == _sample_stored_presets()[0]


@pytest.mark.asyncio
async def test_preset_store_require_returns_strict_match() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.require(2) == _sample_stored_presets()[1]


@pytest.mark.asyncio
async def test_preset_store_resolve_uses_default_for_none() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.resolve(None) == _sample_stored_presets()[0]


@pytest.mark.asyncio
async def test_preset_store_resolve_uses_strict_id_when_provided() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.resolve(2) == _sample_stored_presets()[1]


@pytest.mark.asyncio
async def test_preset_store_resolve_for_fetch_uses_snapshot_when_current() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.resolve_with_fallback(requested_id=None, fallback_id=2) == (_sample_stored_presets()[1])


@pytest.mark.asyncio
async def test_preset_store_resolve_for_fetch_falls_back_to_default_for_stale_snapshot() -> None:
    presets = [
        _sample_stored_presets()[1],
        _sample_stored_presets()[0],
    ]
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes(presets=presets)}))

    assert await preset_store.resolve_with_fallback(requested_id=None, fallback_id=99) == presets[0]


@pytest.mark.asyncio
async def test_preset_store_resolve_for_fetch_uses_strict_explicit_id() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    assert await preset_store.resolve_with_fallback(requested_id=2, fallback_id=1) == (_sample_stored_presets()[1])


@pytest.mark.asyncio
async def test_preset_store_add_appends_new_stored_preset_and_preserves_default_first() -> None:
    new_preset = _preset(
        name='hard',
        slowed=PresetMode(step=0.09, levels=4),
        sped_up=PresetMode(step=0.03, levels=3),
        reverb_start=0.04,
        reverb_step=0.02,
    )
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(s3_client)

    await preset_store.add(new_preset)

    expected_presets = [
        *_sample_stored_presets(),
        _stored_preset(
            preset_id=3,
            version=1,
            preset=new_preset,
        ),
    ]
    assert preset_store._presets_cache == Presets(
        presets=expected_presets,
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == _presets_payload(presets=expected_presets)


@pytest.mark.asyncio
async def test_preset_store_replace_preserves_id_increments_version_and_keeps_default() -> None:
    replacement = _preset(
        name='updated soft',
        slowed=None,
        sped_up=PresetMode(step=0.07, levels=5),
        reverb_start=0.08,
        reverb_step=0.03,
    )
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(s3_client)

    await preset_store.replace(2, replacement)

    assert preset_store._presets_cache == Presets(
        presets=[
            _sample_stored_presets()[0],
            _stored_preset(
                preset_id=2,
                version=2,
                preset=replacement,
            ),
        ],
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == _presets_payload(
        presets=[
            _sample_stored_presets()[0],
            _stored_preset(
                preset_id=2,
                version=2,
                preset=replacement,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_preset_store_set_default_reorders_selected_preset_to_front() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(s3_client)

    await preset_store.set_default(2)

    assert preset_store._presets_cache == Presets(
        presets=[
            _sample_stored_presets()[1],
            _sample_stored_presets()[0],
        ],
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == _presets_payload(
        presets=[
            _sample_stored_presets()[1],
            _sample_stored_presets()[0],
        ]
    )


@pytest.mark.asyncio
async def test_preset_store_set_default_is_noop_when_selected_preset_is_already_first() -> None:
    original_bytes = _presets_bytes()
    s3_client = _FakeS3Client(objects={_presets_key(): original_bytes})
    preset_store = _preset_store(s3_client)

    await preset_store.set_default(1)

    assert preset_store._presets_cache == Presets(
        presets=_sample_stored_presets(),
    )
    assert s3_client.objects[_presets_key()] == original_bytes
    assert s3_client.put_calls == []


@pytest.mark.asyncio
async def test_preset_store_remove_removes_non_default_preset() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(s3_client)

    await preset_store.remove(2)

    assert preset_store._presets_cache == Presets(
        presets=[_sample_stored_presets()[0]],
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == _presets_payload(
        presets=[_sample_stored_presets()[0]]
    )


@pytest.mark.asyncio
async def test_preset_store_remove_rejects_removing_default_preset() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(track_store_module.TrackDefaultPresetRemovalError, match='Cannot remove default preset: 1'):
        await preset_store.remove(1)


@pytest.mark.asyncio
async def test_preset_store_remove_rejects_removing_default_preset_after_reordering() -> None:
    presets = [
        _stored_preset(
            preset_id=2,
            version=3,
            preset=_preset(
                name='default',
                slowed=PresetMode(step=0.06, levels=3),
                sped_up=PresetMode(step=0.06, levels=2),
                reverb_start=0.01,
                reverb_step=0.01,
            ),
        ),
        _stored_preset(
            preset_id=5,
            version=1,
            preset=_preset(
                name='soft',
                slowed=PresetMode(step=0.05, levels=2),
                sped_up=None,
                reverb_start=0.02,
                reverb_step=0.01,
            ),
        ),
        _stored_preset(
            preset_id=7,
            version=4,
            preset=_preset(
                name='hard',
                slowed=None,
                sped_up=PresetMode(step=0.03, levels=3),
                reverb_start=0.03,
                reverb_step=0.02,
            ),
        ),
    ]
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(presets=presets)})
    preset_store = _preset_store(s3_client)

    with pytest.raises(track_store_module.TrackDefaultPresetRemovalError, match='Cannot remove default preset: 2'):
        await preset_store.remove(2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('method_name', 'args'),
    [
        ('require', (99,)),
        ('replace', (99, _preset(name='replacement', slowed=PresetMode(step=0.05, levels=1)))),
        ('set_default', (99,)),
        ('remove', (99,)),
    ],
)
async def test_preset_store_management_methods_raise_value_error_for_unknown_preset_id(
    method_name: str,
    args: tuple[object, ...],
) -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(ValueError, match='Unknown preset id: 99'):
        await getattr(preset_store, method_name)(*args)


@pytest.mark.asyncio
async def test_track_store_rejects_preset_store_with_different_s3_client() -> None:
    with pytest.raises(ValueError, match='TrackStore and PresetStore must share the same S3 client instance'):
        TrackStore(_FakeS3Client(), preset_store=_preset_store(_FakeS3Client()))


@pytest.mark.asyncio
async def test_track_store_uses_provided_preset_store_without_own_preset_cache() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(s3_client)
    store = TrackStore(s3_client, preset_store=preset_store)

    assert store._preset_store is preset_store
    assert not hasattr(store, '_presets_cache')
    assert not hasattr(store, 'all')
    assert not hasattr(store, 'add')
    assert not hasattr(store, 'replace')
    assert not hasattr(store, 'set_default')
    assert not hasattr(store, 'remove')

    assert await store.list_groups() == []
    assert preset_store._presets_cache == Presets(presets=_sample_stored_presets())


@pytest.mark.asyncio
async def test_shared_preset_store_cache_is_reused_across_track_store_instances() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    preset_store = _preset_store(s3_client)
    first_store = TrackStore(s3_client, preset_store=preset_store)
    second_store = TrackStore(s3_client, preset_store=preset_store)

    await first_store.list_groups()
    await second_store.list_groups()

    assert s3_client.get_calls == [_presets_key()]


@pytest.mark.asyncio
async def test_list_groups_returns_sorted_groups() -> None:
    store = _store(
        _FakeS3Client(
            objects={_presets_key(): _presets_bytes()},
            prefixes=[
                'tracks/east-2024-2',
                'tracks/phonk-2025-1',
                'tracks/west-2024-1',
                'tracks/electronic-2023-5',
            ],
        )
    )

    assert await store.list_groups() == [
        TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1),
        TrackGroup(universe=TrackUniverse.EAST, year=2024, season=Season.S2),
        TrackGroup(universe=TrackUniverse.PHONK, year=2025, season=Season.S1),
        TrackGroup(universe=TrackUniverse.ELECTRONIC, year=2023, season=Season.S5),
    ]


@pytest.mark.asyncio
async def test_list_groups_fails_on_malformed_prefix() -> None:
    store = _store(
        _FakeS3Client(
            objects={_presets_key(): _presets_bytes()},
            prefixes=['tracks/west-2024-1/extra'],
        )
    )

    with pytest.raises(ValueError, match=r"'tracks/west-2024-1/extra'"):
        await store.list_groups()


@pytest.mark.asyncio
async def test_list_sub_seasons_returns_unique_sorted_values_with_none_first() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2024, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.B,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                        _entry(
                            id=_UUID_2,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.NONE,
                            order=1,
                            preset=None,
                            has_instrumental=True,
                            has_instrumental_variants=False,
                        ),
                        _entry(
                            id=_UUID_3,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.B,
                            order=2,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                    ]
                ),
            }
        )
    )

    assert await store.list_sub_seasons(TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1)) == [
        SubSeason.NONE,
        SubSeason.B,
    ]


@pytest.mark.asyncio
async def test_list_sub_seasons_fails_on_missing_manifest() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(TrackGroupNotFoundError) as excinfo:
        await store.list_sub_seasons(TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1))

    assert excinfo.value.universe is TrackUniverse.WEST
    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.sub_season is None


@pytest.mark.asyncio
async def test_list_sub_seasons_wraps_corrupted_manifest() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2024, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: b'{"tracks": []}',
            }
        )
    )

    with pytest.raises(TrackManifestCorruptedError):
        await store.list_sub_seasons(TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1))


@pytest.mark.asyncio
async def test_list_tracks_filters_requested_sub_season_and_sorts_by_manifest_order() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2024, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist c',),
                            title='third in manifest, second in order',
                            sub_season=SubSeason.A,
                            order=2,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                        _entry(
                            id=_UUID_2,
                            artists=('artist other',),
                            title='other sub-season',
                            sub_season=SubSeason.B,
                            order=1,
                            preset=None,
                            has_instrumental=True,
                            has_instrumental_variants=False,
                        ),
                        _entry(
                            id=_UUID_3,
                            artists=('artist a', 'artist b'),
                            title='second in manifest, first in order',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=_applied_preset(preset_id=1, version=3),
                            has_instrumental=True,
                            has_instrumental_variants=True,
                        ),
                    ]
                ),
            }
        )
    )

    assert await store.list_tracks(
        TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1),
        SubSeason.A,
    ) == [
        TrackInfo(
            id=_UUID_3,
            artists=('artist a', 'artist b'),
            title='second in manifest, first in order',
            has_instrumental=True,
        ),
        TrackInfo(
            id=_UUID_1,
            artists=('artist c',),
            title='third in manifest, second in order',
            has_instrumental=False,
        ),
    ]


@pytest.mark.asyncio
async def test_list_tracks_returns_track_info_with_only_public_discovery_fields() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2024, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.NONE,
                            order=7,
                            preset=_applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset),
                            has_instrumental=True,
                            has_instrumental_variants=False,
                        ),
                    ]
                ),
            }
        )
    )

    result = await store.list_tracks(
        TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1),
        SubSeason.NONE,
    )

    assert result == [TrackInfo(id=_UUID_1, artists=('artist',), title='title', has_instrumental=True)]
    assert tuple(result[0].__slots__) == ('id', 'artists', 'title', 'has_instrumental')


@pytest.mark.asyncio
async def test_list_tracks_returns_empty_list_for_existing_group_with_missing_sub_season() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2024, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                    ]
                ),
            }
        )
    )

    assert (
        await store.list_tracks(
            TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1),
            SubSeason.C,
        )
        == []
    )


@pytest.mark.asyncio
async def test_list_tracks_raises_group_not_found_for_missing_group() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(TrackGroupNotFoundError) as excinfo:
        await store.list_tracks(
            TrackGroup(universe=TrackUniverse.WEST, year=2024, season=Season.S1),
            SubSeason.D,
        )

    assert excinfo.value.universe is TrackUniverse.WEST
    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.sub_season is SubSeason.D


@pytest.mark.asyncio
async def test_store_creates_new_group_and_manifest_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    store = _store(s3_client)

    result = await store.store(
        _track(artists=('artist one', 'artist two'), title='Track Title'),
        group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
        sub_season=SubSeason.A,
    )

    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    track_key = _track_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    cover_key = _cover_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    cache_key = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    assert result is None
    assert s3_client.objects[track_key] == b'track'
    assert s3_client.objects[cover_key] == b'cover'
    expected_manifest = _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist one', 'artist two'),
                title='Track Title',
                sub_season=SubSeason.A,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            )
        ]
    )
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == expected_manifest
    assert store._manifest_cache[cache_key].to_dict() == expected_manifest
    assert probe_calls == [b'track']


@pytest.mark.asyncio
async def test_store_initializes_preset_from_current_default_preset(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
    _patch_probe_audio_sample_rate(monkeypatch)
    default_preset = _sample_stored_presets()[1]
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(
                presets=[
                    default_preset,
                    _sample_stored_presets()[0],
                ]
            )
        }
    )
    store = _store(s3_client)

    await store.store(
        _track(),
        group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
        sub_season=SubSeason.A,
    )

    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_payload = json.loads(s3_client.objects[manifest_key].decode('utf-8'))
    expected_variant_count = len(store._resolve_variant_specs(default_preset.preset))

    assert manifest_payload['data'][0]['preset'] == {
        'id': default_preset.id,
        'version': default_preset.version,
        'variant_count': expected_variant_count,
    }
    assert manifest_payload['data'][0]['has_variants'] is False


@pytest.mark.asyncio
async def test_store_uses_dense_order_within_sub_season_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uuid7(monkeypatch, _UUID_2)
    _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                        _entry(
                            id=_UUID_3,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.B,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                    ]
                ),
            }
        )
    )

    await store.store(
        _track(),
        group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
        sub_season=SubSeason.A,
    )

    assert json.loads(store._s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
            _entry(
                id=_UUID_3,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.B,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
            _entry(
                id=_UUID_2,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=2,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_store_propagates_raw_error_when_track_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
    _patch_probe_audio_sample_rate(monkeypatch)
    track_key = _track_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    s3_client = _FakeS3Client(
        objects={_presets_key(): _presets_bytes()},
        put_failures={track_key},
    )
    store = _store(s3_client)

    with pytest.raises(RuntimeError, match=re.escape(f'boom putting {track_key}')) as excinfo:
        await store.store(
            _track(),
            group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            sub_season=SubSeason.A,
        )

    assert excinfo.value.__cause__ is None
    assert track_key not in s3_client.objects
    assert manifest_key not in s3_client.objects
    cache_key = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    assert cache_key not in store._manifest_cache


@pytest.mark.asyncio
async def test_store_raises_sync_error_and_keeps_uploaded_track_when_cover_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
    _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    track_key = _track_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    cover_key = _cover_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    s3_client = _FakeS3Client(
        objects={_presets_key(): _presets_bytes()},
        put_failures={cover_key},
    )
    store = _store(s3_client)

    with pytest.raises(TrackManifestSyncError, match='cover_upload') as excinfo:
        await store.store(
            _track(),
            group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            sub_season=SubSeason.A,
        )

    assert excinfo.value.stage == 'cover_upload'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.written_keys == (track_key,)
    assert excinfo.value.manifest_key == manifest_key
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == f'boom putting {cover_key}'
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Original cover upload error: RuntimeError('boom putting {cover_key}')"
    ]
    assert s3_client.objects[track_key] == b'track'
    assert cover_key not in s3_client.objects
    assert manifest_key not in s3_client.objects
    cache_key = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    assert cache_key not in store._manifest_cache


@pytest.mark.asyncio
async def test_store_raises_sync_error_and_keeps_uploaded_objects_when_manifest_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
    _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    track_key = _track_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    cover_key = _cover_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1, track_id=_UUID_1)
    s3_client = _FakeS3Client(
        objects={_presets_key(): _presets_bytes()},
        put_failures={manifest_key},
    )
    store = _store(s3_client)

    with pytest.raises(TrackManifestSyncError, match='manifest_write') as excinfo:
        await store.store(
            _track(),
            group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            sub_season=SubSeason.A,
        )

    assert excinfo.value.stage == 'manifest_write'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.written_keys == (track_key, cover_key)
    assert excinfo.value.manifest_key == manifest_key
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == f'boom putting {manifest_key}'
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Original manifest write error: RuntimeError('boom putting {manifest_key}')"
    ]
    assert s3_client.deleted_keys == []
    assert s3_client.objects[track_key] == b'track'
    assert s3_client.objects[cover_key] == b'cover'
    assert manifest_key not in s3_client.objects
    cache_key = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    assert cache_key not in store._manifest_cache


@pytest.mark.asyncio
async def test_store_rejects_non_48k_audio_before_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch, sample_rate=44_100)
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes()})
    store = _store(s3_client)

    with pytest.raises(TrackInvalidAudioFormatError, match='Audio sample rate must be 48000 Hz, got 44100') as excinfo:
        await store.store(
            _track(audio_bytes=b'bad-track'),
            group=TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            sub_season=SubSeason.A,
        )

    assert excinfo.value.track_id is None
    assert excinfo.value.reason == 'Audio sample rate must be 48000 Hz, got 44100'
    assert probe_calls == [b'bad-track']
    assert s3_client.put_calls == []


@pytest.mark.asyncio
async def test_update_attaches_first_instrumental_uploads_and_rewrites_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch, sample_rate=48_000)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    instrumental_key = _instrumental_key(
        universe=TrackUniverse.WEST,
        year=2026,
        season=Season.S1,
        track_id=_UUID_1,
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist one',),
                        title='title one',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=_applied_preset(preset_id=2, version=5),
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                    _entry(
                        id=_UUID_2,
                        artists=('artist two',),
                        title='title two',
                        sub_season=SubSeason.B,
                        order=1,
                        preset=None,
                        has_instrumental=True,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
        }
    )
    store = _store(s3_client)

    await store.update(group, _UUID_1, instrumental_bytes=b'new-instrumental')

    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    expected_manifest = _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist one',),
                title='title one',
                sub_season=SubSeason.A,
                order=1,
                preset=_applied_preset(preset_id=2, version=5),
                has_instrumental=True,
                has_instrumental_variants=False,
            ),
            _entry(
                id=_UUID_2,
                artists=('artist two',),
                title='title two',
                sub_season=SubSeason.B,
                order=1,
                preset=None,
                has_instrumental=True,
                has_instrumental_variants=False,
            ),
        ]
    )
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == expected_manifest
    assert (
        store._manifest_cache[_track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)].to_dict()
        == expected_manifest
    )
    assert probe_calls == [b'new-instrumental']


@pytest.mark.asyncio
async def test_update_instrumental_raises_for_unknown_track_id_in_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                    ]
                ),
            }
        )
    )

    with pytest.raises(ValueError, match=f'Track id {_UUID_2} does not exist in group'):
        await store.update(group, _UUID_2, instrumental_bytes=b'instrumental')


@pytest.mark.asyncio
async def test_update_first_instrumental_attach_wraps_manifest_write_failure_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    instrumental_key = _instrumental_key(
        universe=TrackUniverse.WEST,
        year=2026,
        season=Season.S1,
        track_id=_UUID_1,
    )
    original_manifest_payload = _manifest_bytes(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=_applied_preset(preset_id=2, version=5),
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
        ]
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: original_manifest_payload,
        },
        put_failures={manifest_key},
    )
    store = _store(s3_client)

    with pytest.raises(TrackUpdateManifestSyncError, match='manifest_write') as excinfo:
        await store.update(group, _UUID_1, instrumental_bytes=b'new-instrumental')

    assert excinfo.value.stage == 'manifest_write'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (instrumental_key,)
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Update manifest write error: RuntimeError('boom putting {manifest_key}')"
    ]
    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    assert s3_client.objects[manifest_key] == original_manifest_payload
    assert store._manifest_cache[
        _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    ].to_dict() == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=_applied_preset(preset_id=2, version=5),
                has_instrumental=False,
                has_instrumental_variants=False,
            )
        ]
    )


@pytest.mark.asyncio
async def test_update_instrumental_rejects_non_48k_audio_before_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch, sample_rate=44_100)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    instrumental_key = _instrumental_key(
        universe=TrackUniverse.WEST,
        year=2026,
        season=Season.S1,
        track_id=_UUID_1,
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=None,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
        }
    )
    store = _store(s3_client)

    with pytest.raises(TrackInvalidAudioFormatError, match='Audio sample rate must be 48000 Hz, got 44100') as excinfo:
        await store.update(group, _UUID_1, instrumental_bytes=b'bad-instrumental')

    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.reason == 'Audio sample rate must be 48000 Hz, got 44100'
    assert probe_calls == [b'bad-instrumental']
    assert s3_client.put_calls == []
    assert instrumental_key not in s3_client.objects


@pytest.mark.asyncio
async def test_update_updates_only_manifest_metadata() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    original_entry = _entry(
        id=_UUID_1,
        artists=('artist',),
        title='title',
        sub_season=SubSeason.A,
        order=1,
        preset=_applied_preset(),
        has_variants=True,
        has_instrumental=False,
        has_instrumental_variants=False,
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes([original_entry]),
        }
    )
    store = _store(s3_client)

    await store.update(
        group,
        _UUID_1,
        artists=('updated artist', 'guest'),
        title='updated title',
    )

    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('updated artist', 'guest'),
                title='updated title',
                sub_season=SubSeason.A,
                order=1,
                preset=_applied_preset(),
                has_variants=True,
                has_instrumental=False,
                has_instrumental_variants=False,
            )
        ]
    )
    assert s3_client.delete_keys_calls == []


@pytest.mark.asyncio
async def test_update_cover_only_overwrites_cover_only() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    original_entry = _entry(
        id=_UUID_1,
        artists=('artist',),
        title='title',
        sub_season=SubSeason.A,
        order=1,
        preset=_applied_preset(),
        has_variants=False,
        has_instrumental=True,
        has_instrumental_variants=False,
    )
    original_manifest = _manifest_bytes([original_entry])
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: original_manifest,
            cover_key: b'old-cover',
        }
    )
    store = _store(s3_client)

    await store.update(group, _UUID_1, cover_bytes=b'new-cover')

    assert s3_client.objects[cover_key] == b'new-cover'
    assert s3_client.objects[manifest_key] == original_manifest
    assert s3_client.delete_keys_calls == []


@pytest.mark.asyncio
async def test_update_audio_overwrites_track_deletes_variants_and_preserves_instrumental_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    applied_preset = _applied_preset()
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-orig',
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=applied_preset,
                        has_variants=True,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'old-track',
            instrumental_key: b'old-instrumental',
        }
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-orig',
        )
    )
    store = _store(s3_client)

    await store.update(group, _UUID_1, audio_bytes=b'new-track')

    assert probe_calls == [b'new-track']
    assert s3_client.delete_keys_calls == [original_variant_keys]
    assert s3_client.objects[track_key] == b'new-track'
    assert s3_client.objects[instrumental_key] == b'old-instrumental'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=applied_preset,
                has_variants=False,
                has_instrumental=True,
                has_instrumental_variants=True,
            )
        ]
    )


@pytest.mark.asyncio
async def test_update_audio_wraps_partial_variant_deletion_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    applied_preset = AppliedPreset(id=1, version=3, variant_count=2)
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        store._variant_storage_keys(
            track_group_prefix=_track_group_prefix(
                universe=group.universe,
                year=group.year,
                season=group.season,
            ),
            track_id=_UUID_1,
            variant_count=2,
            instrumental=False,
        )
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=applied_preset,
                        has_variants=True,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    )
                ]
            ),
            track_key: b'old-track',
            original_variant_keys[0]: b'variant-1',
            original_variant_keys[1]: b'variant-2',
        },
        delete_failures={original_variant_keys[1]},
    )
    store = _store(s3_client)

    with pytest.raises(TrackUpdateManifestSyncError, match='original_variant_delete') as excinfo:
        await store.update(group, _UUID_1, audio_bytes=b'new-track')

    assert probe_calls == [b'new-track']
    assert excinfo.value.stage == 'original_variant_delete'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == original_variant_keys
    assert excinfo.value.manifest_key == manifest_key
    assert s3_client.delete_keys_calls == [original_variant_keys]
    assert original_variant_keys[0] not in s3_client.objects
    assert original_variant_keys[1] in s3_client.objects
    assert s3_client.objects[track_key] == b'old-track'
    assert s3_client.objects[manifest_key] == _manifest_bytes(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=applied_preset,
                has_variants=True,
                has_instrumental=False,
                has_instrumental_variants=False,
            )
        ]
    )


@pytest.mark.asyncio
async def test_update_later_stage_sync_error_unions_prior_and_assumed_touched_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    store = _store(_FakeS3Client())
    instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-inst',
            instrumental=True,
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        preset=_applied_preset(),
                        has_variants=False,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'old-track',
            instrumental_key: b'old-instrumental',
        },
        delete_failures={instrumental_variant_keys[1]},
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-inst',
            instrumental=True,
        )
    )
    store = _store(s3_client)

    with pytest.raises(TrackUpdateManifestSyncError, match='instrumental_variant_delete') as excinfo:
        await store.update(group, _UUID_1, audio_bytes=b'new-track', instrumental_bytes=b'new-instrumental')

    assert probe_calls == [b'new-track', b'new-instrumental']
    assert excinfo.value.stage == 'instrumental_variant_delete'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (track_key, *instrumental_variant_keys)
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Instrumental variant delete error: RuntimeError('boom deleting {instrumental_variant_keys[1]}')"
    ]
    assert s3_client.objects[track_key] == b'new-track'
    assert instrumental_variant_keys[0] not in s3_client.objects
    assert instrumental_variant_keys[1] in s3_client.objects


@pytest.mark.asyncio
async def test_update_instrumental_first_attach_sets_manifest_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=_applied_preset(),
                        has_variants=False,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    )
                ]
            ),
        }
    )
    store = _store(s3_client)

    await store.update(group, _UUID_1, instrumental_bytes=b'new-instrumental')

    assert probe_calls == [b'new-instrumental']
    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=_applied_preset(),
                has_variants=False,
                has_instrumental=True,
                has_instrumental_variants=False,
            )
        ]
    )


@pytest.mark.asyncio
async def test_update_instrumental_overwrites_authoritative_key_and_resets_variant_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    applied_preset = _applied_preset()
    store = _store(_FakeS3Client())
    instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-inst',
            instrumental=True,
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=applied_preset,
                        has_variants=False,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'old-track',
            instrumental_key: b'old-instrumental',
        }
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-inst',
            instrumental=True,
        )
    )
    store = _store(s3_client)

    await store.update(group, _UUID_1, instrumental_bytes=b'new-instrumental')

    assert probe_calls == [b'new-instrumental']
    assert s3_client.delete_keys_calls == [instrumental_variant_keys]
    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    assert s3_client.objects[track_key] == b'old-track'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=applied_preset,
                has_variants=False,
                has_instrumental=True,
                has_instrumental_variants=False,
            )
        ]
    )


@pytest.mark.asyncio
async def test_update_instrumental_wraps_partial_variant_deletion_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    probe_calls = _patch_probe_audio_sample_rate(monkeypatch)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    applied_preset = AppliedPreset(id=1, version=3, variant_count=2)
    store = _store(_FakeS3Client())
    instrumental_variant_keys = tuple(
        store._variant_storage_keys(
            track_group_prefix=_track_group_prefix(
                universe=group.universe,
                year=group.year,
                season=group.season,
            ),
            track_id=_UUID_1,
            variant_count=2,
            instrumental=True,
        )
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=applied_preset,
                        has_variants=False,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'old-track',
            instrumental_key: b'old-instrumental',
            instrumental_variant_keys[0]: b'inst-1',
            instrumental_variant_keys[1]: b'inst-2',
        },
        delete_failures={instrumental_variant_keys[1]},
    )
    store = _store(s3_client)

    with pytest.raises(TrackUpdateManifestSyncError, match='instrumental_variant_delete') as excinfo:
        await store.update(group, _UUID_1, instrumental_bytes=b'new-instrumental')

    assert probe_calls == [b'new-instrumental']
    assert excinfo.value.stage == 'instrumental_variant_delete'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == instrumental_variant_keys
    assert excinfo.value.manifest_key == manifest_key
    assert s3_client.delete_keys_calls == [instrumental_variant_keys]
    assert instrumental_variant_keys[0] not in s3_client.objects
    assert instrumental_variant_keys[1] in s3_client.objects
    assert s3_client.objects[instrumental_key] == b'old-instrumental'
    assert s3_client.objects[manifest_key] == _manifest_bytes(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=applied_preset,
                has_variants=False,
                has_instrumental=True,
                has_instrumental_variants=True,
            )
        ]
    )


@pytest.mark.asyncio
async def test_update_rejects_missing_fields() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes([_entry(id=_UUID_1)]),
        }
    )
    store = _store(s3_client)

    with pytest.raises(ValueError, match='update\\(\\) requires at least one update field'):
        await store.update(group, _UUID_1)


@pytest.mark.asyncio
async def test_update_raises_sync_error_when_manifest_write_fails_after_cover_upload() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    original_manifest = _manifest_bytes([_entry(id=_UUID_1)])
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: original_manifest,
            cover_key: b'old-cover',
        },
        put_failures={manifest_key},
    )
    store = _store(s3_client)

    with pytest.raises(TrackUpdateManifestSyncError, match='manifest_write') as excinfo:
        await store.update(group, _UUID_1, cover_bytes=b'new-cover')

    assert excinfo.value.stage == 'manifest_write'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (cover_key,)
    assert excinfo.value.manifest_key == manifest_key
    assert s3_client.objects[cover_key] == b'new-cover'
    assert s3_client.objects[manifest_key] == original_manifest


def _track_group(
    *, universe: TrackUniverse = TrackUniverse.WEST, year: int = 2026, season: Season = Season.S1
) -> TrackGroup:
    return TrackGroup(universe=universe, year=year, season=season)


def _variant_storage_objects(
    store: TrackStore,
    *,
    group: TrackGroup,
    track_id: str,
    preset: Preset,
    payload_prefix: str,
    instrumental: bool = False,
) -> dict[str, bytes]:
    group_prefix = _track_group_prefix(universe=group.universe, year=group.year, season=group.season)
    return {
        store._variant_storage_key(
            track_group_prefix=group_prefix,
            track_id=track_id,
            index=index,
            instrumental=instrumental,
        ): f'{payload_prefix}|{index}'.encode()
        for index, spec in enumerate(store._resolve_variant_specs(preset), start=1)
    }


def _patch_create_audio_variant(monkeypatch: pytest.MonkeyPatch) -> list[tuple[bytes, float, float]]:
    calls: list[tuple[bytes, float, float]] = []

    async def _fake_create_audio_variant(audio_bytes: bytes, *, speed: float, reverb: float, **_: object) -> bytes:
        calls.append((audio_bytes, speed, reverb))
        return f'{audio_bytes.decode()}|{speed:.2f}|{reverb:.2f}'.encode()

    monkeypatch.setattr(track_store_module, 'create_audio_variant', _fake_create_audio_variant)
    return calls


def _patch_probe_audio_sample_rate(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sample_rate: int = 48_000,
) -> list[bytes]:
    calls: list[bytes] = []

    async def _fake_probe_audio_sample_rate(audio_bytes: bytes, **_: object) -> int:
        calls.append(audio_bytes)
        return sample_rate

    monkeypatch.setattr(track_store_module, 'probe_audio_sample_rate', _fake_probe_audio_sample_rate)
    return calls


@pytest.mark.asyncio
async def test_fetch_with_explicit_preset_returns_current_original_and_instrumental_variants() -> None:
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    presets = _sample_stored_presets()
    resolved = presets[0]
    applied_preset = _applied_preset(preset_id=resolved.id, version=resolved.version, preset=resolved.preset)
    store = _store(_FakeS3Client())
    objects = {
        _presets_key(): _presets_bytes(presets=presets),
        manifest_key: _manifest_bytes(
            [
                _entry(
                    id=_UUID_1,
                    artists=('artist', 'featured'),
                    title='title',
                    sub_season=SubSeason.A,
                    order=1,
                    preset=applied_preset,
                    has_variants=True,
                    has_instrumental=True,
                    has_instrumental_variants=True,
                ),
            ]
        ),
        cover_key: b'cover-bytes',
    }
    objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=resolved.preset,
            payload_prefix='orig',
        )
    )
    objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=resolved.preset,
            payload_prefix='inst',
            instrumental=True,
        )
    )
    s3_client = _FakeS3Client(objects=objects)
    store = _store(s3_client)

    result = await store.fetch(group, _UUID_1, preset_id=1)

    assert isinstance(result, FetchedVariants)
    assert result.track_id == _UUID_1
    assert result.artists == ('artist', 'featured')
    assert result.title == 'title'
    assert result.cover_bytes == b'cover-bytes'
    assert result.cover_filename == store._s3_key_to_filename(cover_key)
    assert result.cover_filename == 'tracks--west-2026-1--' + _UUID_1 + '-cover.jpg'
    assert isinstance(result.variants[0], FetchedVariant)
    assert [variant.speed for variant in result.variants] == sorted(variant.speed for variant in result.variants)
    assert [variant.audio_bytes for variant in result.variants] == [
        b'orig|1',
        b'orig|2',
        b'orig|3',
        b'orig|4',
        b'orig|5',
    ]
    assert result.instrumental_variants is not None
    assert [variant.audio_bytes for variant in result.instrumental_variants] == [
        b'inst|1',
        b'inst|2',
        b'inst|3',
        b'inst|4',
        b'inst|5',
    ]
    assert (
        _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
        not in s3_client.get_calls
    )
    assert (
        _instrumental_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
        not in s3_client.get_calls
    )
    assert s3_client.delete_keys_calls == []
    assert all(call[0] != manifest_key for call in s3_client.put_calls)


@pytest.mark.asyncio
async def test_fetch_with_unknown_preset_id_raises_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_calls = _patch_create_audio_variant(monkeypatch)

    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    original_manifest = _manifest_bytes([_entry(id=_UUID_1, preset=_applied_preset(preset_id=1, version=3))])
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(
                presets=[
                    _sample_stored_presets()[1],
                    _sample_stored_presets()[0],
                ]
            ),
            manifest_key: original_manifest,
            track_key: b'authoritative-track',
            cover_key: b'cover',
        }
    )
    store = _store(s3_client)

    with pytest.raises(ValueError, match='Unknown preset id: 99'):
        await store.fetch(group, _UUID_1, preset_id=99)

    assert generation_calls == []
    assert s3_client.put_calls == []
    assert s3_client.delete_keys_calls == []
    assert s3_client.objects[manifest_key] == original_manifest


@pytest.mark.asyncio
async def test_fetch_with_none_resolves_current_default_preset_and_returns_no_instrumental(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_calls = _patch_create_audio_variant(monkeypatch)

    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(
                presets=[
                    _sample_stored_presets()[1],
                    _sample_stored_presets()[0],
                ]
            ),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=_applied_preset(preset_id=99, version=1),
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
        }
    )
    store = _store(s3_client)

    result = await store.fetch(group, _UUID_1)

    assert result.instrumental_variants is None
    assert result.cover_filename == store._s3_key_to_filename(cover_key)
    assert generation_calls == [
        (b'authoritative-track', 0.9, 0.03),
        (b'authoritative-track', 0.95, 0.02),
    ]
    assert isinstance(result, FetchedVariants)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            _entry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=_applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset),
                has_instrumental=False,
                has_instrumental_variants=False,
                has_variants=True,
            )
        ]
    )


@pytest.mark.asyncio
async def test_fetch_raises_group_not_found() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(TrackGroupNotFoundError):
        await store.fetch(_track_group(), _UUID_1)


@pytest.mark.asyncio
async def test_fetch_raises_value_error_for_unknown_track_id() -> None:
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        _entry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                    ]
                ),
            }
        )
    )

    with pytest.raises(ValueError, match=f'Track id {_UUID_2} does not exist in group'):
        await store.fetch(group, _UUID_2)


@pytest.mark.asyncio
async def test_fetch_regenerates_original_variants_when_preset_id_mismatches(monkeypatch: pytest.MonkeyPatch) -> None:
    generation_calls = _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    previous_applied_preset = _applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=previous_applied_preset,
                        has_variants=True,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
        }
    )
    s3_client.objects.update(
        _variant_storage_objects(
            _store(_FakeS3Client()),
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old',
        )
    )
    store = _store(s3_client)

    result = await store.fetch(group, _UUID_1, preset_id=1)

    previous_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old',
        ).keys()
    )
    assert s3_client.delete_keys_calls == [previous_keys]
    assert generation_calls
    assert [variant.audio_bytes for variant in result.variants] == [
        b'authoritative-track|0.82|0.03',
        b'authoritative-track|0.88|0.02',
        b'authoritative-track|0.94|0.01',
        b'authoritative-track|1.06|0.01',
        b'authoritative-track|1.12|0.02',
    ]
    rewritten_manifest = json.loads(s3_client.objects[manifest_key].decode('utf-8'))
    assert rewritten_manifest['data'][0]['preset'] == _applied_preset_dict(
        _applied_preset(preset_id=1, version=3, preset=_sample_stored_presets()[0].preset)
    )
    assert rewritten_manifest['data'][0]['has_instrumental_variants'] is False


@pytest.mark.asyncio
async def test_fetch_regenerates_original_variants_when_preset_version_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_calls = _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    previous_applied_preset = _applied_preset(preset_id=1, version=2)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=previous_applied_preset,
                        has_variants=True,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
        }
    )
    s3_client.objects.update(
        _variant_storage_objects(
            _store(_FakeS3Client()),
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old',
        )
    )
    store = _store(s3_client)

    await store.fetch(group, _UUID_1, preset_id=1)

    assert len(s3_client.delete_keys_calls) == 1
    assert generation_calls


@pytest.mark.asyncio
async def test_fetch_treats_variant_count_mismatch_as_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    generation_calls = _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    stale_applied_preset = AppliedPreset(id=1, version=3, variant_count=4)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=stale_applied_preset,
                        has_variants=True,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
        }
    )
    store = _store(s3_client)

    await store.fetch(group, _UUID_1, preset_id=1)

    assert s3_client.delete_keys_calls == [
        tuple(
            store._variant_key(
                _track_group_prefix(universe=group.universe, year=group.year, season=group.season),
                _UUID_1,
                index=index,
            )
            for index in range(1, 5)
        )
    ]
    assert generation_calls


@pytest.mark.asyncio
async def test_fetch_regenerates_instrumental_variants_when_manifest_flag_is_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation_calls = _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1
    )
    current_applied_preset = _applied_preset(preset_id=1, version=3, preset=_sample_stored_presets()[0].preset)
    store = _store(_FakeS3Client())
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=current_applied_preset,
                        has_variants=True,
                        has_instrumental=True,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            cover_key: b'cover',
            instrumental_key: b'authoritative-instrumental',
        }
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='orig',
        )
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='old-inst',
            instrumental=True,
        )
    )
    store = _store(s3_client)

    result = await store.fetch(group, _UUID_1, preset_id=1)

    assert s3_client.delete_keys_calls == []
    assert instrumental_key in s3_client.get_calls
    assert result.instrumental_variants is not None
    assert all(call[0] == b'authoritative-instrumental' for call in generation_calls)
    rewritten_manifest = json.loads(s3_client.objects[manifest_key].decode('utf-8'))
    assert rewritten_manifest['data'][0]['has_instrumental_variants'] is True


@pytest.mark.asyncio
async def test_fetch_regeneration_rewrites_manifest_with_applied_preset(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=None,
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
        }
    )
    store = _store(s3_client)

    await store.fetch(group, _UUID_1, preset_id=1)

    cached_applied_preset = next(
        iter(store._manifest_cache[_track_group_prefix(universe=group.universe, year=group.year, season=group.season)])
    ).preset
    assert cached_applied_preset == _applied_preset(preset_id=1, version=3, preset=_sample_stored_presets()[0].preset)


@pytest.mark.asyncio
async def test_fetch_wraps_partial_original_variant_upload_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='unused',
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes([_entry(id=_UUID_1, has_variants=False)]),
            track_key: b'authoritative-track',
            cover_key: b'cover',
        },
        put_failures={original_variant_keys[1]},
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='original_variant_upload') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'original_variant_upload'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (original_variant_keys[0],)
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Original variant upload error: RuntimeError('boom putting {original_variant_keys[1]}')"
    ]
    assert s3_client.objects[original_variant_keys[0]] == b'authoritative-track|0.82|0.03'
    assert original_variant_keys[1] not in s3_client.objects
    assert s3_client.objects[manifest_key] == _manifest_bytes([_entry(id=_UUID_1, has_variants=False)])


@pytest.mark.asyncio
async def test_fetch_wraps_partial_instrumental_variant_upload_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    store = _store(_FakeS3Client())
    instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='unused',
            instrumental=True,
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [_entry(id=_UUID_1, has_variants=True, has_instrumental=True, has_instrumental_variants=False)]
            ),
            cover_key: b'cover',
            instrumental_key: b'authoritative-instrumental',
        },
        put_failures={instrumental_variant_keys[1]},
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='orig',
        )
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='instrumental_variant_upload') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'instrumental_variant_upload'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (instrumental_variant_keys[0],)
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Instrumental variant upload error: RuntimeError('boom putting {instrumental_variant_keys[1]}')"
    ]
    assert s3_client.objects[instrumental_variant_keys[0]] == b'authoritative-instrumental|0.82|0.03'
    assert instrumental_variant_keys[1] not in s3_client.objects


@pytest.mark.asyncio
async def test_fetch_wraps_manifest_write_failure_after_regeneration_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    previous_applied_preset = _applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset)
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-orig',
        ).keys()
    )
    instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-inst',
            instrumental=True,
        ).keys()
    )
    regenerated_original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='new-orig',
        ).keys()
    )
    regenerated_instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='new-inst',
            instrumental=True,
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        preset=previous_applied_preset,
                        has_variants=True,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
            instrumental_key: b'authoritative-instrumental',
        },
        put_failures={manifest_key},
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-orig',
        )
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-inst',
            instrumental=True,
        )
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='manifest_write') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'manifest_write'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (
        *regenerated_original_variant_keys,
        *regenerated_instrumental_variant_keys,
    )
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Fetch manifest write error: RuntimeError('boom putting {manifest_key}')"
    ]
    assert s3_client.objects[manifest_key] == _manifest_bytes(
        [
            _entry(
                id=_UUID_1,
                preset=previous_applied_preset,
                has_variants=True,
                has_instrumental=True,
                has_instrumental_variants=True,
            )
        ]
    )
    assert s3_client.objects[original_variant_keys[0]] == b'authoritative-track|0.82|0.03'
    assert s3_client.objects[instrumental_variant_keys[0]] == b'authoritative-instrumental|0.82|0.03'


@pytest.mark.asyncio
async def test_fetch_wraps_stale_original_variant_deletion_as_sync_error() -> None:
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    previous_applied_preset = _applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset)
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-orig',
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes([_entry(id=_UUID_1, preset=previous_applied_preset, has_variants=True)]),
            track_key: b'authoritative-track',
            cover_key: b'cover',
            original_variant_keys[0]: b'old-1',
            original_variant_keys[1]: b'old-2',
        },
        delete_failures={original_variant_keys[1]},
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='original_variant_delete') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'original_variant_delete'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == original_variant_keys
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Original variant delete error: RuntimeError('boom deleting {original_variant_keys[1]}')"
    ]
    assert original_variant_keys[0] not in s3_client.objects
    assert original_variant_keys[1] in s3_client.objects
    assert s3_client.objects[manifest_key] == _manifest_bytes(
        [_entry(id=_UUID_1, preset=previous_applied_preset, has_variants=True)]
    )


@pytest.mark.asyncio
async def test_fetch_wraps_stale_instrumental_variant_deletion_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    previous_applied_preset = _applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset)
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-orig',
        ).keys()
    )
    instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-inst',
            instrumental=True,
        ).keys()
    )
    regenerated_original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='new-orig',
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        preset=previous_applied_preset,
                        has_variants=True,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
            instrumental_key: b'authoritative-instrumental',
        },
        delete_failures={instrumental_variant_keys[1]},
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-orig',
        )
    )
    s3_client.objects.update(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-inst',
            instrumental=True,
        )
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='instrumental_variant_delete') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'instrumental_variant_delete'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (*regenerated_original_variant_keys, *instrumental_variant_keys)
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Instrumental variant delete error: RuntimeError('boom deleting {instrumental_variant_keys[1]}')"
    ]
    assert s3_client.objects[original_variant_keys[0]] == b'authoritative-track|0.82|0.03'
    assert instrumental_variant_keys[0] not in s3_client.objects
    assert instrumental_variant_keys[1] in s3_client.objects


@pytest.mark.asyncio
async def test_fetch_wraps_original_source_read_failure_after_stale_variant_deletion_as_sync_error() -> None:
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    previous_applied_preset = _applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset)
    store = _store(_FakeS3Client())
    original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-orig',
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes([_entry(id=_UUID_1, preset=previous_applied_preset, has_variants=True)]),
            cover_key: b'cover',
            original_variant_keys[0]: b'old-1',
            original_variant_keys[1]: b'old-2',
        }
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='original_source_read') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'original_source_read'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == original_variant_keys
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Original source read error: S3ObjectNotFoundError('Object not found: {track_key}')"
    ]
    assert original_variant_keys[0] not in s3_client.objects
    assert original_variant_keys[1] not in s3_client.objects


@pytest.mark.asyncio
async def test_fetch_wraps_instrumental_source_read_failure_after_prior_mutations_as_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_create_audio_variant(monkeypatch)
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    instrumental_key = _instrumental_key(
        universe=group.universe,
        year=group.year,
        season=group.season,
        track_id=_UUID_1,
    )
    previous_applied_preset = _applied_preset(preset_id=2, version=1, preset=_sample_stored_presets()[1].preset)
    store = _store(_FakeS3Client())
    instrumental_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[1].preset,
            payload_prefix='old-inst',
            instrumental=True,
        ).keys()
    )
    regenerated_original_variant_keys = tuple(
        _variant_storage_objects(
            store,
            group=group,
            track_id=_UUID_1,
            preset=_sample_stored_presets()[0].preset,
            payload_prefix='new-orig',
        ).keys()
    )
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes(
                [
                    _entry(
                        id=_UUID_1,
                        preset=previous_applied_preset,
                        has_variants=False,
                        has_instrumental=True,
                        has_instrumental_variants=True,
                    )
                ]
            ),
            track_key: b'authoritative-track',
            cover_key: b'cover',
            instrumental_variant_keys[0]: b'old-inst-1',
            instrumental_variant_keys[1]: b'old-inst-2',
        }
    )
    store = _store(s3_client)

    with pytest.raises(TrackFetchManifestSyncError, match='instrumental_source_read') as excinfo:
        await store.fetch(group, _UUID_1, preset_id=1)

    assert excinfo.value.stage == 'instrumental_source_read'
    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.touched_keys == (*regenerated_original_variant_keys, *instrumental_variant_keys)
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Instrumental source read error: S3ObjectNotFoundError('Object not found: {instrumental_key}')"
    ]
    assert s3_client.objects[regenerated_original_variant_keys[0]] == b'authoritative-track|0.82|0.03'
    assert instrumental_variant_keys[0] not in s3_client.objects
    assert instrumental_variant_keys[1] not in s3_client.objects


@pytest.mark.asyncio
async def test_fetch_original_source_read_failure_before_any_touches_escapes_raw() -> None:
    group = _track_group()
    manifest_key = _manifest_key(universe=group.universe, year=group.year, season=group.season)
    track_key = _track_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    cover_key = _cover_key(universe=group.universe, year=group.year, season=group.season, track_id=_UUID_1)
    s3_client = _FakeS3Client(
        objects={
            _presets_key(): _presets_bytes(),
            manifest_key: _manifest_bytes([_entry(id=_UUID_1, has_variants=False)]),
            cover_key: b'cover',
        }
    )
    store = _store(s3_client)

    with pytest.raises(S3ObjectNotFoundError, match=track_key):
        await store.fetch(group, _UUID_1, preset_id=1)


@pytest.mark.asyncio
async def test_preset_store_wraps_corrupted_presets() -> None:
    preset_store = _preset_store(_FakeS3Client(objects={_presets_key(): b'[]'}))

    with pytest.raises(TrackPresetsCorruptedError):
        await preset_store.ensure_ready()


@pytest.mark.asyncio
async def test_public_methods_wrap_corrupted_presets() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): b'[]'}))

    with pytest.raises(TrackPresetsCorruptedError):
        await store.list_groups()
