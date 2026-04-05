import json
import re
import uuid

import pytest

import general_bot.services.track_store as track_store_module
from general_bot.infra.s3 import S3Client, S3ObjectNotFoundError
from general_bot.services.track_store import (
    Manifest,
    ManifestEntry,
    Preset,
    PresetMode,
    Presets,
    Season,
    StoredPreset,
    SubSeason,
    Track,
    TrackGroup,
    TrackGroupNotFoundError,
    TrackInfo,
    TrackInstrumentalManifestSyncError,
    TrackManifestCorruptedError,
    TrackManifestSyncError,
    TrackPresetsCorruptedError,
    TrackStore,
    TrackUniverse,
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
) -> StoredPreset:
    return StoredPreset(
        id=preset_id,
        version=version,
        preset=preset,
    )


def _bootstrap_preset() -> Preset:
    return _preset(
        name='Default',
        slowed=PresetMode(step=0.08, levels=4),
        sped_up=PresetMode(step=0.04, levels=2),
        reverb_start=0.03,
        reverb_step=0.02,
    )


def _sample_stored_presets() -> list[StoredPreset]:
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


def _presets_bytes(*, default_preset_id: int = 1, presets: list[StoredPreset] | None = None) -> bytes:
    return json.dumps(
        Presets(
            default_preset_id=default_preset_id,
            presets=list(presets or _sample_stored_presets()),
        ).to_dict(),
        separators=(',', ':'),
    ).encode('utf-8')


def _manifest_bytes(entries: list[ManifestEntry]) -> bytes:
    return json.dumps(Manifest(entries).to_list(), separators=(',', ':')).encode('utf-8')


def _patch_uuid7(monkeypatch: pytest.MonkeyPatch, *track_ids: str) -> None:
    uuids = iter(uuid.UUID(track_id) for track_id in track_ids)
    monkeypatch.setattr(track_store_module, '_uuid7', lambda: next(uuids))


def _store(s3_client: _FakeS3Client, *, bootstrap_preset: Preset | None = None) -> TrackStore:
    return TrackStore(
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


def test_presets_from_dict_accepts_stored_wrapper_shape() -> None:
    parsed = Presets.from_dict(json.loads(_presets_bytes(default_preset_id=2).decode('utf-8')))

    assert parsed.default_preset_id == 2
    assert [preset.id for preset in parsed.presets] == [1, 2]
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
        ({'id': 0, 'version': 1}, 'StoredPreset.id must be >= 1'),
        ({'id': 1, 'version': 0}, 'StoredPreset.version must be >= 1'),
    ],
)
def test_stored_preset_rejects_non_positive_identity_fields(
    kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        StoredPreset(
            id=kwargs['id'],
            version=kwargs['version'],
            preset=_preset(name='default'),
        )


def test_presets_reject_duplicate_stored_preset_ids() -> None:
    payload = {
        'default_preset_id': 1,
        'presets': [
            {
                'id': 1,
                'version': 1,
                'preset': {
                    'name': 'default',
                    'slowed': None,
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
                    'slowed': None,
                    'sped_up': None,
                    'reverb_start': 0.1,
                    'reverb_step': 0.1,
                },
            },
        ],
    }

    with pytest.raises(ValueError, match='duplicate stored preset id'):
        Presets.from_dict(payload)


def test_presets_reject_unknown_default_preset_id() -> None:
    payload = {
        'default_preset_id': 9,
        'presets': [
            {
                'id': 1,
                'version': 1,
                'preset': {
                    'name': 'default',
                    'slowed': None,
                    'sped_up': None,
                    'reverb_start': 0.0,
                    'reverb_step': 0.0,
                },
            }
        ],
    }

    with pytest.raises(ValueError, match='default_preset_id'):
        Presets.from_dict(payload)


def test_presets_default_preset_returns_stored_preset() -> None:
    presets = Presets(
        default_preset_id=2,
        presets=_sample_stored_presets(),
    )

    assert presets.default_preset() == presets.presets[1]


def test_presets_serialization_preserves_insertion_order() -> None:
    original = Presets(
        default_preset_id=2,
        presets=[
            _stored_preset(
                preset_id=2,
                version=1,
                preset=_preset(
                    name='soft',
                    slowed=None,
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


def test_manifest_uses_top_level_list_with_preferred_field_order() -> None:
    entry = ManifestEntry(
        id=_UUID_1,
        artists=('artist',),
        title='title',
        sub_season=SubSeason.A,
        order=1,
        preset=None,
        has_instrumental=False,
        has_instrumental_variants=False,
    )

    payload = Manifest([entry]).to_list()

    assert payload == [
        {
            'id': _UUID_1,
            'artists': ['artist'],
            'title': 'title',
            'sub_season': 'A',
            'order': 1,
            'preset': None,
            'has_instrumental': False,
            'has_instrumental_variants': False,
        }
    ]
    assert list(payload[0]) == [
        'id',
        'artists',
        'title',
        'sub_season',
        'order',
        'preset',
        'has_instrumental',
        'has_instrumental_variants',
    ]
    assert list(Manifest.from_list(payload)) == [entry]


def test_manifest_round_trips_has_instrumental_variants() -> None:
    entry = ManifestEntry(
        id=_UUID_1,
        artists=('artist',),
        title='title',
        sub_season=SubSeason.A,
        order=1,
        preset=track_store_module.AppliedPreset(id=2, version=5),
        has_instrumental=True,
        has_instrumental_variants=True,
    )

    payload = Manifest([entry]).to_list()

    assert payload == [
        {
            'id': _UUID_1,
            'artists': ['artist'],
            'title': 'title',
            'sub_season': 'A',
            'order': 1,
            'preset': {'id': 2, 'version': 5},
            'has_instrumental': True,
            'has_instrumental_variants': True,
        }
    ]
    assert list(Manifest.from_list(payload)) == [entry]


def test_manifest_next_order_is_dense_per_sub_season() -> None:
    manifest = Manifest(
        [
            ManifestEntry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
            ManifestEntry(
                id=_UUID_2,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.B,
                order=1,
                preset=None,
                has_instrumental=False,
                has_instrumental_variants=False,
            ),
            ManifestEntry(
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
        Manifest.from_list(
            [
                {
                    'id': _UUID_1,
                    'artists': ['artist'],
                    'title': 'title',
                    'sub_season': 'A',
                    'order': 1,
                    'preset': None,
                    'has_instrumental': False,
                    'has_instrumental_variants': False,
                },
                {
                    'id': _UUID_2,
                    'artists': ['artist'],
                    'title': 'title',
                    'sub_season': 'A',
                    'order': 1,
                    'preset': None,
                    'has_instrumental': True,
                    'has_instrumental_variants': False,
                },
            ]
        )


def test_manifest_rejects_invalid_preset_shape() -> None:
    with pytest.raises(ValueError, match='manifest `preset` has unexpected fields'):
        Manifest.from_list(
            [
                {
                    'id': _UUID_1,
                    'artists': ['artist'],
                    'title': 'title',
                    'sub_season': 'A',
                    'order': 1,
                    'preset': {'preset_id': 2, 'version': 5},
                    'has_instrumental': False,
                    'has_instrumental_variants': False,
                }
            ]
        )


def test_manifest_rejects_instrumental_variants_without_instrumental() -> None:
    with pytest.raises(ValueError, match=r'manifest `has_instrumental_variants` requires `has_instrumental`'):
        Manifest.from_list(
            [
                {
                    'id': _UUID_1,
                    'artists': ['artist'],
                    'title': 'title',
                    'sub_season': 'A',
                    'order': 1,
                    'preset': {'id': 2, 'version': 5},
                    'has_instrumental': False,
                    'has_instrumental_variants': True,
                }
            ]
        )


def test_manifest_rejects_instrumental_variants_without_preset() -> None:
    with pytest.raises(ValueError, match=r'manifest `has_instrumental_variants` requires non-null `preset`'):
        Manifest.from_list(
            [
                {
                    'id': _UUID_1,
                    'artists': ['artist'],
                    'title': 'title',
                    'sub_season': 'A',
                    'order': 1,
                    'preset': None,
                    'has_instrumental': True,
                    'has_instrumental_variants': True,
                }
            ]
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
                'preset': None,
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
                'preset': None,
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
                'preset': None,
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
                'preset': None,
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
                'preset': None,
                'has_instrumental': False,
                'has_instrumental_variants': False,
            },
            'manifest `title` must be a non-empty string',
        ),
    ],
)
def test_manifest_rejects_invalid_artists_or_title(entry: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        Manifest.from_list([entry])


def test_variant_key_uses_dash_separated_mode_stems() -> None:
    store = _store(_FakeS3Client())
    group_prefix = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)

    assert store._variant_key(group_prefix, _UUID_1, mode='slowed', level=1) == S3Client.join(
        group_prefix,
        _UUID_1 + '-slowed-1.opus',
    )
    assert store._variant_key(group_prefix, _UUID_1, mode='sped_up', level=2) == S3Client.join(
        group_prefix,
        _UUID_1 + '-sped-up-2.opus',
    )


def test_instrumental_variant_key_uses_dash_separated_mode_stems() -> None:
    store = _store(_FakeS3Client())
    group_prefix = _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)

    assert store._instrumental_variant_key(group_prefix, _UUID_1, mode='slowed', level=1) == S3Client.join(
        group_prefix,
        _UUID_1 + '-instrumental-slowed-1.opus',
    )
    assert store._instrumental_variant_key(group_prefix, _UUID_1, mode='sped_up', level=2) == S3Client.join(
        group_prefix,
        _UUID_1 + '-instrumental-sped-up-2.opus',
    )


@pytest.mark.asyncio
async def test_public_methods_bootstrap_presets_from_constructor_and_cache() -> None:
    s3_client = _FakeS3Client()
    bootstrap_preset = _bootstrap_preset()
    store = _store(s3_client, bootstrap_preset=bootstrap_preset)

    groups = await store.list_groups()

    assert groups == []
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == {
        'default_preset_id': 1,
        'presets': [
            {
                'id': 1,
                'version': 1,
                'preset': {
                    'name': 'Default',
                    'slowed': {'step': 0.08, 'levels': 4},
                    'sped_up': {'step': 0.04, 'levels': 2},
                    'reverb_start': 0.03,
                    'reverb_step': 0.02,
                },
            }
        ],
    }
    assert store._presets_cache == Presets(
        default_preset_id=1,
        presets=[_stored_preset(preset_id=1, version=1, preset=bootstrap_preset)],
    )


@pytest.mark.asyncio
async def test_existing_s3_presets_win_over_bootstrap_input() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2)})
    store = _store(
        s3_client,
        bootstrap_preset=_bootstrap_preset(),
    )

    await store.list_groups()

    assert store._presets_cache == Presets(
        default_preset_id=2,
        presets=_sample_stored_presets(),
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8'))['presets'][0]['version'] == 3


@pytest.mark.asyncio
async def test_list_presets_returns_stored_presets() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2)}))

    assert await store.list_presets() == _sample_stored_presets()


@pytest.mark.asyncio
async def test_list_presets_returns_shallow_copy_of_cached_list() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2)}))

    listed_presets = await store.list_presets()

    assert listed_presets == _sample_stored_presets()
    assert listed_presets is not store._presets_cache.presets
    listed_presets.pop()
    assert store._presets_cache.presets == _sample_stored_presets()


@pytest.mark.asyncio
async def test_add_preset_appends_new_stored_preset_and_preserves_default() -> None:
    new_preset = _preset(
        name='hard',
        slowed=PresetMode(step=0.09, levels=4),
        sped_up=PresetMode(step=0.03, levels=3),
        reverb_start=0.04,
        reverb_step=0.02,
    )
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2)})
    store = _store(s3_client)

    await store.add_preset(new_preset)

    expected_presets = [
        *_sample_stored_presets(),
        _stored_preset(
            preset_id=3,
            version=1,
            preset=new_preset,
        ),
    ]
    assert store._presets_cache == Presets(
        default_preset_id=2,
        presets=expected_presets,
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == {
        'default_preset_id': 2,
        'presets': [
            {
                'id': 1,
                'version': 3,
                'preset': {
                    'name': 'default',
                    'slowed': {'step': 0.06, 'levels': 3},
                    'sped_up': {'step': 0.06, 'levels': 2},
                    'reverb_start': 0.01,
                    'reverb_step': 0.01,
                },
            },
            {
                'id': 2,
                'version': 1,
                'preset': {
                    'name': 'soft',
                    'slowed': {'step': 0.05, 'levels': 2},
                    'sped_up': None,
                    'reverb_start': 0.02,
                    'reverb_step': 0.01,
                },
            },
            {
                'id': 3,
                'version': 1,
                'preset': {
                    'name': 'hard',
                    'slowed': {'step': 0.09, 'levels': 4},
                    'sped_up': {'step': 0.03, 'levels': 3},
                    'reverb_start': 0.04,
                    'reverb_step': 0.02,
                },
            },
        ],
    }


@pytest.mark.asyncio
async def test_replace_preset_preserves_id_increments_version_and_keeps_default() -> None:
    replacement = _preset(
        name='updated soft',
        slowed=None,
        sped_up=PresetMode(step=0.07, levels=5),
        reverb_start=0.08,
        reverb_step=0.03,
    )
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2)})
    store = _store(s3_client)

    await store.replace_preset(2, replacement)

    assert store._presets_cache == Presets(
        default_preset_id=2,
        presets=[
            _sample_stored_presets()[0],
            _stored_preset(
                preset_id=2,
                version=2,
                preset=replacement,
            ),
        ],
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == {
        'default_preset_id': 2,
        'presets': [
            {
                'id': 1,
                'version': 3,
                'preset': {
                    'name': 'default',
                    'slowed': {'step': 0.06, 'levels': 3},
                    'sped_up': {'step': 0.06, 'levels': 2},
                    'reverb_start': 0.01,
                    'reverb_step': 0.01,
                },
            },
            {
                'id': 2,
                'version': 2,
                'preset': {
                    'name': 'updated soft',
                    'slowed': None,
                    'sped_up': {'step': 0.07, 'levels': 5},
                    'reverb_start': 0.08,
                    'reverb_step': 0.03,
                },
            },
        ],
    }


@pytest.mark.asyncio
async def test_set_default_preset_changes_only_default_id() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=1)})
    store = _store(s3_client)

    await store.set_default_preset(2)

    assert store._presets_cache == Presets(
        default_preset_id=2,
        presets=_sample_stored_presets(),
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == {
        'default_preset_id': 2,
        'presets': [
            {
                'id': 1,
                'version': 3,
                'preset': {
                    'name': 'default',
                    'slowed': {'step': 0.06, 'levels': 3},
                    'sped_up': {'step': 0.06, 'levels': 2},
                    'reverb_start': 0.01,
                    'reverb_step': 0.01,
                },
            },
            {
                'id': 2,
                'version': 1,
                'preset': {
                    'name': 'soft',
                    'slowed': {'step': 0.05, 'levels': 2},
                    'sped_up': None,
                    'reverb_start': 0.02,
                    'reverb_step': 0.01,
                },
            },
        ],
    }


@pytest.mark.asyncio
async def test_remove_preset_removes_non_default_and_preserves_default() -> None:
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=1)})
    store = _store(s3_client)

    await store.remove_preset(2)

    assert store._presets_cache == Presets(
        default_preset_id=1,
        presets=[_sample_stored_presets()[0]],
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == {
        'default_preset_id': 1,
        'presets': [
            {
                'id': 1,
                'version': 3,
                'preset': {
                    'name': 'default',
                    'slowed': {'step': 0.06, 'levels': 3},
                    'sped_up': {'step': 0.06, 'levels': 2},
                    'reverb_start': 0.01,
                    'reverb_step': 0.01,
                },
            }
        ],
    }


@pytest.mark.asyncio
async def test_remove_preset_reassigns_default_to_smallest_remaining_id() -> None:
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
    s3_client = _FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2, presets=presets)})
    store = _store(s3_client)

    await store.remove_preset(2)

    assert store._presets_cache == Presets(
        default_preset_id=5,
        presets=presets[1:],
    )
    assert json.loads(s3_client.objects[_presets_key()].decode('utf-8')) == {
        'default_preset_id': 5,
        'presets': [
            {
                'id': 5,
                'version': 1,
                'preset': {
                    'name': 'soft',
                    'slowed': {'step': 0.05, 'levels': 2},
                    'sped_up': None,
                    'reverb_start': 0.02,
                    'reverb_step': 0.01,
                },
            },
            {
                'id': 7,
                'version': 4,
                'preset': {
                    'name': 'hard',
                    'slowed': None,
                    'sped_up': {'step': 0.03, 'levels': 3},
                    'reverb_start': 0.03,
                    'reverb_step': 0.02,
                },
            },
        ],
    }


@pytest.mark.asyncio
async def test_remove_preset_rejects_removing_last_remaining_preset() -> None:
    single_preset = [_stored_preset(preset_id=1, version=1, preset=_bootstrap_preset())]
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=1, presets=single_preset)}))

    with pytest.raises(ValueError, match='Cannot remove the last remaining preset'):
        await store.remove_preset(1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('method_name', 'args'),
    [
        ('replace_preset', (99, _preset(name='replacement'))),
        ('set_default_preset', (99,)),
        ('remove_preset', (99,)),
    ],
)
async def test_preset_management_methods_raise_value_error_for_unknown_preset_id(
    method_name: str,
    args: tuple[object, ...],
) -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(ValueError, match='Unknown preset id: 99'):
        await getattr(store, method_name)(*args)


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
                        ManifestEntry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.B,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.NONE,
                            order=1,
                            preset=None,
                            has_instrumental=True,
                            has_instrumental_variants=False,
                        ),
                        ManifestEntry(
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
                        ManifestEntry(
                            id=_UUID_1,
                            artists=('artist c',),
                            title='third in manifest, second in order',
                            sub_season=SubSeason.A,
                            order=2,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            artists=('artist other',),
                            title='other sub-season',
                            sub_season=SubSeason.B,
                            order=1,
                            preset=None,
                            has_instrumental=True,
                            has_instrumental_variants=False,
                        ),
                        ManifestEntry(
                            id=_UUID_3,
                            artists=('artist a', 'artist b'),
                            title='second in manifest, first in order',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=track_store_module.AppliedPreset(id=1, version=3),
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
        ),
        TrackInfo(
            id=_UUID_1,
            artists=('artist c',),
            title='third in manifest, second in order',
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
                        ManifestEntry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.NONE,
                            order=7,
                            preset=track_store_module.AppliedPreset(id=2, version=1),
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

    assert result == [TrackInfo(id=_UUID_1, artists=('artist',), title='title')]
    assert tuple(result[0].__slots__) == ('id', 'artists', 'title')


@pytest.mark.asyncio
async def test_list_tracks_returns_empty_list_for_existing_group_with_missing_sub_season() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2024, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
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
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_1,
            'artists': ['artist one', 'artist two'],
            'title': 'Track Title',
            'sub_season': 'A',
            'order': 1,
            'preset': None,
            'has_instrumental': False,
            'has_instrumental_variants': False,
        }
    ]
    assert store._manifest_cache[cache_key].to_list() == [
        {
            'id': _UUID_1,
            'artists': ['artist one', 'artist two'],
            'title': 'Track Title',
            'sub_season': 'A',
            'order': 1,
            'preset': None,
            'has_instrumental': False,
            'has_instrumental_variants': False,
        }
    ]


@pytest.mark.asyncio
async def test_store_uses_dense_order_within_sub_season_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uuid7(monkeypatch, _UUID_2)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            artists=('artist',),
                            title='title',
                            sub_season=SubSeason.A,
                            order=1,
                            preset=None,
                            has_instrumental=False,
                            has_instrumental_variants=False,
                        ),
                        ManifestEntry(
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

    assert json.loads(store._s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_1,
            'artists': ['artist'],
            'title': 'title',
            'sub_season': 'A',
            'order': 1,
            'preset': None,
            'has_instrumental': False,
            'has_instrumental_variants': False,
        },
        {
            'id': _UUID_3,
            'artists': ['artist'],
            'title': 'title',
            'sub_season': 'B',
            'order': 1,
            'preset': None,
            'has_instrumental': False,
            'has_instrumental_variants': False,
        },
        {
            'id': _UUID_2,
            'artists': ['artist'],
            'title': 'title',
            'sub_season': 'A',
            'order': 2,
            'preset': None,
            'has_instrumental': False,
            'has_instrumental_variants': False,
        },
    ]


@pytest.mark.asyncio
async def test_store_propagates_raw_error_when_track_upload_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uuid7(monkeypatch, _UUID_1)
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
async def test_store_instrumental_uploads_and_rewrites_manifest_for_existing_track() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
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
                    ManifestEntry(
                        id=_UUID_1,
                        artists=('artist one',),
                        title='title one',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=track_store_module.AppliedPreset(id=2, version=5),
                        has_instrumental=False,
                        has_instrumental_variants=False,
                    ),
                    ManifestEntry(
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

    await store.store_instrumental(
        b'new-instrumental',
        group=group,
        track_id=_UUID_1,
    )

    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_1,
            'artists': ['artist one'],
            'title': 'title one',
            'sub_season': 'A',
            'order': 1,
            'preset': {'id': 2, 'version': 5},
            'has_instrumental': True,
            'has_instrumental_variants': False,
        },
        {
            'id': _UUID_2,
            'artists': ['artist two'],
            'title': 'title two',
            'sub_season': 'B',
            'order': 1,
            'preset': None,
            'has_instrumental': True,
            'has_instrumental_variants': False,
        },
    ]
    assert store._manifest_cache[
        _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    ].to_list() == [
        {
            'id': _UUID_1,
            'artists': ['artist one'],
            'title': 'title one',
            'sub_season': 'A',
            'order': 1,
            'preset': {'id': 2, 'version': 5},
            'has_instrumental': True,
            'has_instrumental_variants': False,
        },
        {
            'id': _UUID_2,
            'artists': ['artist two'],
            'title': 'title two',
            'sub_season': 'B',
            'order': 1,
            'preset': None,
            'has_instrumental': True,
            'has_instrumental_variants': False,
        },
    ]


@pytest.mark.asyncio
async def test_store_instrumental_raises_for_unknown_track_id_in_group() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(),
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
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
        await store.store_instrumental(
            b'instrumental',
            group=group,
            track_id=_UUID_2,
        )


@pytest.mark.asyncio
async def test_store_instrumental_overwrites_existing_key_without_probing_storage() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
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
                    ManifestEntry(
                        id=_UUID_1,
                        artists=('artist',),
                        title='title',
                        sub_season=SubSeason.A,
                        order=1,
                        preset=None,
                        has_instrumental=True,
                        has_instrumental_variants=False,
                    ),
                ]
            ),
            instrumental_key: b'old-instrumental',
        }
    )
    store = _store(s3_client)

    await store.store_instrumental(
        b'new-instrumental',
        group=group,
        track_id=_UUID_1,
    )

    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    assert instrumental_key not in s3_client.get_calls
    assert s3_client.list_subprefixes_calls == []


@pytest.mark.asyncio
async def test_store_instrumental_raises_sync_error_and_keeps_uploaded_object_when_manifest_write_fails() -> None:
    group = TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    instrumental_key = _instrumental_key(
        universe=TrackUniverse.WEST,
        year=2026,
        season=Season.S1,
        track_id=_UUID_1,
    )
    original_manifest_payload = _manifest_bytes(
        [
            ManifestEntry(
                id=_UUID_1,
                artists=('artist',),
                title='title',
                sub_season=SubSeason.A,
                order=1,
                preset=track_store_module.AppliedPreset(id=2, version=5),
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

    with pytest.raises(
        TrackInstrumentalManifestSyncError, match='Instrumental/manifest synchronization failed'
    ) as excinfo:
        await store.store_instrumental(
            b'new-instrumental',
            group=group,
            track_id=_UUID_1,
        )

    assert excinfo.value.track_id == _UUID_1
    assert excinfo.value.instrumental_key == instrumental_key
    assert excinfo.value.manifest_key == manifest_key
    assert getattr(excinfo.value, '__notes__', []) == [
        f"Original manifest write error: RuntimeError('boom putting {manifest_key}')"
    ]
    assert s3_client.objects[instrumental_key] == b'new-instrumental'
    assert s3_client.objects[manifest_key] == original_manifest_payload
    assert store._manifest_cache[
        _track_group_prefix(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    ].to_list() == [
        {
            'id': _UUID_1,
            'artists': ['artist'],
            'title': 'title',
            'sub_season': 'A',
            'order': 1,
            'preset': {'id': 2, 'version': 5},
            'has_instrumental': False,
            'has_instrumental_variants': False,
        }
    ]


@pytest.mark.asyncio
async def test_fetch_with_known_preset_id_reaches_deferred_not_implemented() -> None:
    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(default_preset_id=2),
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
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

    with pytest.raises(NotImplementedError, match='later iteration'):
        await store.fetch(
            TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            SubSeason.A,
            preset_id=2,
        )


@pytest.mark.asyncio
async def test_fetch_with_unknown_preset_id_raises_value_error() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes(default_preset_id=2)}))

    with pytest.raises(ValueError, match='Unknown preset id: 99'):
        await store.fetch(
            TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            SubSeason.A,
            preset_id=99,
        )


@pytest.mark.asyncio
async def test_fetch_with_none_resolves_current_default_preset(monkeypatch: pytest.MonkeyPatch) -> None:
    default_calls: list[int] = []
    original_default_preset = Presets.default_preset

    def _tracking_default_preset(self: Presets) -> StoredPreset:
        default_calls.append(self.default_preset_id)
        return original_default_preset(self)

    manifest_key = _manifest_key(universe=TrackUniverse.WEST, year=2026, season=Season.S1)
    monkeypatch.setattr(Presets, 'default_preset', _tracking_default_preset)
    store = _store(
        _FakeS3Client(
            objects={
                _presets_key(): _presets_bytes(default_preset_id=2),
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
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

    with pytest.raises(NotImplementedError, match='later iteration'):
        await store.fetch(
            TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            SubSeason.A,
        )

    assert default_calls == [2]


@pytest.mark.asyncio
async def test_fetch_raises_group_not_found_before_not_implemented() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): _presets_bytes()}))

    with pytest.raises(TrackGroupNotFoundError):
        await store.fetch(
            TrackGroup(universe=TrackUniverse.WEST, year=2026, season=Season.S1),
            SubSeason.A,
        )


@pytest.mark.asyncio
async def test_public_methods_wrap_corrupted_presets() -> None:
    store = _store(_FakeS3Client(objects={_presets_key(): b'{"default_preset_id": 1}'}))

    with pytest.raises(TrackPresetsCorruptedError):
        await store.list_groups()
