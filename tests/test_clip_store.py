import asyncio
import json
import uuid

import pytest

import timeline_hub.services.clip_store as clip_store_module
from timeline_hub.infra.s3 import S3Client, S3ObjectNotFoundError
from timeline_hub.services.clip_store import (
    AudioNormalization,
    Clip,
    ClipGroup,
    ClipGroupNotFoundError,
    ClipIdsNotInSubGroupError,
    ClipManifestSyncError,
    ClipStore,
    ClipSubGroup,
    DuplicateClipIdsError,
    DuplicateFilenamesError,
    InvalidFilenamesError,
    Manifest,
    ManifestCorruptedError,
    ManifestEntry,
    MixedClipGroupsError,
    NormalizedClipManifestSyncError,
    ReconcileDeleteError,
    ReconcileResult,
    Scope,
    Season,
    StoreResult,
    SubSeason,
    Universe,
    UnknownClipsError,
)

_UUID_1 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e1').hex
_UUID_2 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e2').hex
_UUID_3 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e3').hex
_UUID_4 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e4').hex
_UUID_5 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e5').hex
_HASH_A = 'a' * 64
_HASH_B = 'b' * 64
_HASH_C = 'c' * 64
_HASH_D = 'd' * 64


def test_store_result_adds_counts() -> None:
    assert StoreResult(stored_count=1, duplicate_count=2, clip_ids=(_UUID_1, _UUID_2)) + StoreResult(
        stored_count=3,
        duplicate_count=4,
        clip_ids=(_UUID_3,),
    ) == StoreResult(
        stored_count=4,
        duplicate_count=6,
        clip_ids=(_UUID_1, _UUID_2, _UUID_3),
    )


@pytest.mark.parametrize(
    ('kwargs', 'expected_message'),
    [
        ({'loudness': True, 'bitrate': 128}, '`loudness` must be a numeric value'),
        ({'loudness': 'loud', 'bitrate': 128}, '`loudness` must be a numeric value'),
        ({'loudness': float('inf'), 'bitrate': 128}, '`loudness` must be finite'),
        ({'loudness': float('nan'), 'bitrate': 128}, '`loudness` must be finite'),
        ({'loudness': -14, 'bitrate': True}, '`bitrate` must be an integer'),
        ({'loudness': -14, 'bitrate': 128.0}, '`bitrate` must be an integer'),
        ({'loudness': -14, 'bitrate': 0}, '`bitrate` must be >= 1'),
    ],
)
def test_audio_normalization_rejects_invalid_values(
    kwargs: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        AudioNormalization(**kwargs)


def test_season_from_month_uses_exact_mapping() -> None:
    assert Season.from_month(2) is Season.S1
    assert Season.from_month(3) is Season.S2
    assert Season.from_month(6) is Season.S3
    assert Season.from_month(9) is Season.S4
    assert Season.from_month(12) is Season.S5


class _FakeS3Client:
    def __init__(
        self,
        objects: dict[str, bytes] | None = None,
        *,
        delete_failures: set[str] | None = None,
        put_failures: set[str] | None = None,
        prefixes: list[str] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.delete_failures = set(delete_failures or set())
        self.put_failures = set(put_failures or set())
        self.prefixes = list(prefixes or [])
        self.get_calls: list[str] = []
        self.put_calls: list[tuple[str, bytes, str | None]] = []
        self.deleted_keys: list[str] = []

    async def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        if key in self.put_failures:
            raise RuntimeError(f'boom putting {key}')
        self.objects[key] = data
        self.put_calls.append((key, data, content_type))

    async def get_bytes(self, key: str) -> bytes:
        self.get_calls.append(key)
        try:
            return self.objects[key]
        except KeyError as error:
            raise S3ObjectNotFoundError(key) from error

    async def list_subprefixes(self, prefix: str | None = None) -> list[str]:
        if prefix is None:
            return list(self.prefixes)

        expected_parts = S3Client.split(prefix)
        return [
            candidate
            for candidate in self.prefixes
            if S3Client.split(candidate)[: len(expected_parts)] == expected_parts
        ]

    async def list_prefixes(self, prefix: str | None = None) -> list[str]:
        return await self.list_subprefixes(prefix)

    async def delete_key(self, key: str) -> None:
        if key in self.delete_failures:
            raise RuntimeError(f'boom deleting {key}')
        self.deleted_keys.append(key)
        self.objects.pop(key, None)


def _clip_key(*, year: int, season: Season, universe: Universe, clip_id: str) -> str:
    return S3Client.join('clips', f'{universe}-{year}-{season}', clip_id + '.mp4')


def _manifest_key(*, year: int, season: Season, universe: Universe) -> str:
    return S3Client.join('clips', f'{universe}-{year}-{season}', 'manifest.json')


def _manifest_bytes(entries: list[ManifestEntry]) -> bytes:
    return json.dumps(Manifest(entries).to_dict(), separators=(',', ':')).encode('utf-8')


def _manifest_payload(entries: list[ManifestEntry]) -> dict[str, list[dict[str, object]]]:
    return Manifest(entries).to_dict()


def _normalized_clip_key(*, year: int, season: Season, universe: Universe, clip_id: str) -> str:
    return S3Client.join('clips', f'{universe}-{year}-{season}', clip_id + '-normalized.mp4')


def _patch_hashes(monkeypatch: pytest.MonkeyPatch, hashes: dict[bytes, str]) -> None:
    async def _fake_hash(video_bytes: bytes) -> str:
        return hashes[video_bytes]

    monkeypatch.setattr(clip_store_module, 'hash_video_content', _fake_hash)


def _patch_uuid7(monkeypatch: pytest.MonkeyPatch, *clip_ids: str) -> None:
    uuids = iter(uuid.UUID(clip_id) for clip_id in clip_ids)
    monkeypatch.setattr(clip_store_module, '_uuid7', lambda: next(uuids))


@pytest.mark.asyncio
async def test_manifest_uses_data_root_with_preferred_field_order() -> None:
    entry = ManifestEntry(
        id=_UUID_1,
        video_hash=_HASH_A,
        sub_season=SubSeason.A,
        scope=Scope.COLLECTION,
        batch=1,
        order=1,
        audio_normalization=AudioNormalization(loudness=-14, bitrate=128),
    )

    payload = Manifest([entry]).to_dict()

    assert payload == {
        'data': [
            {
                'id': _UUID_1,
                'video_hash': _HASH_A,
                'audio_normalization': {'loudness': -14, 'bitrate': 128},
                'sub_season': 'A',
                'scope': 'collection',
                'batch': 1,
                'order': 1,
            }
        ]
    }
    assert list(payload['data'][0]) == [
        'id',
        'video_hash',
        'audio_normalization',
        'sub_season',
        'scope',
        'batch',
        'order',
    ]
    assert list(Manifest.from_dict(payload)) == [entry]


def test_manifest_rejects_old_object_wrapper_shape() -> None:
    with pytest.raises(ValueError, match="manifest root must be an object with only 'data'"):
        Manifest.from_dict({'clips': []})


def test_manifest_rejects_old_list_root_shape() -> None:
    with pytest.raises(ValueError, match="manifest root must be an object with only 'data'"):
        Manifest.from_dict([])


def test_manifest_rejects_legacy_null_sub_season() -> None:
    with pytest.raises(ValueError, match='manifest `sub_season` must be a string'):
        Manifest.from_dict(
            {
                'data': [
                    {
                        'id': _UUID_1,
                        'video_hash': _HASH_A,
                        'audio_normalization': None,
                        'sub_season': None,
                        'scope': 'extra',
                        'batch': 1,
                        'order': 1,
                    }
                ]
            }
        )


@pytest.mark.parametrize(
    ('field', 'value', 'expected_message'),
    [
        ('batch', 0, 'manifest `batch` must be >= 1'),
        ('order', 0, 'manifest `order` must be >= 1'),
    ],
)
def test_manifest_rejects_non_positive_batch_and_order(
    field: str,
    value: int,
    expected_message: str,
) -> None:
    payload = {
        'id': _UUID_1,
        'video_hash': _HASH_A,
        'audio_normalization': None,
        'sub_season': 'A',
        'scope': 'collection',
        'batch': 1,
        'order': 1,
    }
    payload[field] = value

    with pytest.raises(ValueError, match=expected_message):
        Manifest.from_dict({'data': [payload]})


def test_manifest_rejects_duplicate_batch_order_position() -> None:
    with pytest.raises(
        ValueError,
        match='duplicate manifest position for sub_season=A scope=collection batch=2 order=1',
    ):
        Manifest.from_dict(
            {
                'data': [
                    {
                        'id': _UUID_1,
                        'video_hash': _HASH_A,
                        'audio_normalization': None,
                        'sub_season': 'A',
                        'scope': 'collection',
                        'batch': 2,
                        'order': 1,
                    },
                    {
                        'id': _UUID_2,
                        'video_hash': _HASH_B,
                        'audio_normalization': None,
                        'sub_season': 'A',
                        'scope': 'collection',
                        'batch': 2,
                        'order': 1,
                    },
                ]
            }
        )


@pytest.mark.asyncio
async def test_fetch_returns_grouped_clips_with_portable_filenames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    clip_key_3 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    clip_key_4 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_4,
                        video_hash=_HASH_D,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=2,
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_3,
                        video_hash=_HASH_C,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=2,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                ]
            ),
            clip_key_1: b'batch-1-first',
            clip_key_2: b'batch-1-second',
            clip_key_3: b'batch-2-first',
            clip_key_4: b'batch-2-second',
        }
    )
    store = ClipStore(s3_client)

    async def _unexpected_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        raise AssertionError('raw fetch must not normalize audio')

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _unexpected_normalize)

    batches = [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            audio_normalization=None,
        )
    ]

    assert batches == [
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'batch-1-first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_2), bytes=b'batch-1-second'),
        ],
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_3), bytes=b'batch-2-first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_4), bytes=b'batch-2-second'),
        ],
    ]


@pytest.mark.asyncio
async def test_fetch_with_audio_normalization_generates_normalized_twins_and_updates_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    clip_key_3 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    clip_key_4 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalized_key_2 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    normalized_key_3 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    normalized_key_4 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    normalization = AudioNormalization(loudness=-14, bitrate=128)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_3,
                        video_hash=_HASH_C,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=2,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_4,
                        video_hash=_HASH_D,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=2,
                        order=2,
                    ),
                ]
            ),
            clip_key_1: b'batch-1-first',
            clip_key_2: b'batch-1-second',
            clip_key_3: b'batch-2-first',
            clip_key_4: b'batch-2-second',
        }
    )
    store = ClipStore(s3_client)
    calls: list[tuple[bytes, float, int]] = []

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        calls.append((video_bytes, loudness, bitrate))
        return b'normalized:' + video_bytes

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _fake_normalize)

    batches = [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            audio_normalization=normalization,
        )
    ]

    assert calls == [
        (b'batch-1-first', -14, 128),
        (b'batch-1-second', -14, 128),
        (b'batch-2-first', -14, 128),
        (b'batch-2-second', -14, 128),
    ]
    assert batches == [
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'normalized:batch-1-first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_2), bytes=b'normalized:batch-1-second'),
        ],
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_3), bytes=b'normalized:batch-2-first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_4), bytes=b'normalized:batch-2-second'),
        ],
    ]
    assert s3_client.objects[normalized_key_1] == b'normalized:batch-1-first'
    assert s3_client.objects[normalized_key_2] == b'normalized:batch-1-second'
    assert s3_client.objects[normalized_key_3] == b'normalized:batch-2-first'
    assert s3_client.objects[normalized_key_4] == b'normalized:batch-2-second'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_1,
                video_hash=_HASH_A,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
                audio_normalization=normalization,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=2,
                audio_normalization=normalization,
            ),
            ManifestEntry(
                id=_UUID_3,
                video_hash=_HASH_C,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=2,
                order=1,
                audio_normalization=normalization,
            ),
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_D,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=2,
                order=2,
                audio_normalization=normalization,
            ),
        ]
    )
    assert [key for key, _, _ in s3_client.put_calls] == [
        normalized_key_1,
        normalized_key_2,
        manifest_key,
        normalized_key_3,
        normalized_key_4,
        manifest_key,
    ]


@pytest.mark.asyncio
async def test_fetch_with_same_audio_normalization_reuses_existing_normalized_twins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalized_key_2 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    normalization = AudioNormalization(loudness=-14, bitrate=128)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                        audio_normalization=normalization,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                        audio_normalization=normalization,
                    ),
                ]
            ),
            clip_key_1: b'raw-first',
            clip_key_2: b'raw-second',
            normalized_key_1: b'normalized-first',
            normalized_key_2: b'normalized-second',
        }
    )
    store = ClipStore(s3_client)

    async def _unexpected_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        raise AssertionError('existing normalized twins should be reused')

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _unexpected_normalize)

    batches = [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            audio_normalization=normalization,
        )
    ]

    assert batches == [
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'normalized-first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_2), bytes=b'normalized-second'),
        ]
    ]
    assert s3_client.put_calls == []
    assert s3_client.get_calls == [manifest_key, normalized_key_1, normalized_key_2]


@pytest.mark.asyncio
async def test_fetch_with_changed_audio_normalization_overwrites_stable_normalized_twins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalized_key_2 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    old_normalization = AudioNormalization(loudness=-14, bitrate=128)
    new_normalization = AudioNormalization(loudness=-18, bitrate=192)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                        audio_normalization=old_normalization,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                        audio_normalization=old_normalization,
                    ),
                ]
            ),
            clip_key_1: b'raw-first',
            clip_key_2: b'raw-second',
            normalized_key_1: b'old-normalized-first',
            normalized_key_2: b'old-normalized-second',
        }
    )
    store = ClipStore(s3_client)
    calls: list[tuple[bytes, float, int]] = []

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        calls.append((video_bytes, loudness, bitrate))
        return b'new:' + video_bytes

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _fake_normalize)

    batches = [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            audio_normalization=new_normalization,
        )
    ]

    assert calls == [
        (b'raw-first', -18, 192),
        (b'raw-second', -18, 192),
    ]
    assert batches == [
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'new:raw-first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_2), bytes=b'new:raw-second'),
        ]
    ]
    assert s3_client.objects[normalized_key_1] == b'new:raw-first'
    assert s3_client.objects[normalized_key_2] == b'new:raw-second'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_1,
                video_hash=_HASH_A,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
                audio_normalization=new_normalization,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=2,
                audio_normalization=new_normalization,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_fetch_audio_normalization_runs_sequentially(monkeypatch: pytest.MonkeyPatch) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    clip_key_3 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_3,
                        video_hash=_HASH_C,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=3,
                    ),
                ]
            ),
            clip_key_1: b'one',
            clip_key_2: b'two',
            clip_key_3: b'three',
        }
    )
    store = ClipStore(s3_client)
    active_calls = 0
    max_active_calls = 0
    call_order: list[bytes] = []

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        nonlocal active_calls, max_active_calls
        active_calls += 1
        max_active_calls = max(max_active_calls, active_calls)
        call_order.append(video_bytes)
        await asyncio.sleep(0)
        active_calls -= 1
        return b'n:' + video_bytes

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _fake_normalize)

    [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            audio_normalization=AudioNormalization(loudness=-14, bitrate=128),
        )
    ]

    assert max_active_calls == 1
    assert call_order == [b'one', b'two', b'three']


@pytest.mark.asyncio
async def test_fetch_raises_explicit_error_when_manifest_write_fails_after_normalized_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalized_key_2 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                ]
            ),
            clip_key_1: b'raw-first',
            clip_key_2: b'raw-second',
        },
        put_failures={manifest_key},
    )
    store = ClipStore(s3_client)

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        return b'n:' + video_bytes

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _fake_normalize)

    with pytest.raises(NormalizedClipManifestSyncError, match='manifest synchronization failed') as excinfo:
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
                audio_normalization=AudioNormalization(loudness=-14, bitrate=128),
            )
        ]

    assert excinfo.value.written_keys == (normalized_key_1, normalized_key_2)
    assert excinfo.value.affected_clip_ids == (_UUID_1, _UUID_2)
    assert excinfo.value.stage == 'manifest_write'
    assert s3_client.objects[normalized_key_1] == b'n:raw-first'
    assert s3_client.objects[normalized_key_2] == b'n:raw-second'
    assert s3_client.deleted_keys == []


@pytest.mark.asyncio
async def test_fetch_raises_explicit_error_when_normalized_write_path_fails_before_manifest_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalized_key_2 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                ]
            ),
            clip_key_1: b'raw-first',
            clip_key_2: b'raw-second',
        },
        put_failures={normalized_key_2},
    )
    store = ClipStore(s3_client)

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        return b'n:' + video_bytes

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _fake_normalize)

    with pytest.raises(NormalizedClipManifestSyncError, match='stage=before_manifest_write') as excinfo:
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
                audio_normalization=AudioNormalization(loudness=-14, bitrate=128),
            )
        ]

    assert excinfo.value.written_keys == (normalized_key_1,)
    assert excinfo.value.affected_clip_ids == (_UUID_1,)
    assert excinfo.value.stage == 'before_manifest_write'
    assert s3_client.objects[normalized_key_1] == b'n:raw-first'
    assert manifest_key in s3_client.objects
    assert s3_client.deleted_keys == []


@pytest.mark.asyncio
async def test_fetch_regenerates_missing_normalized_twin_when_manifest_says_it_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    normalization = AudioNormalization(loudness=-14, bitrate=128)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                        audio_normalization=normalization,
                    )
                ]
            ),
            clip_key_1: b'raw-first',
        }
    )
    store = ClipStore(s3_client)
    calls: list[tuple[bytes, float, int]] = []

    async def _fake_normalize(video_bytes: bytes, *, loudness: float, bitrate: int) -> bytes:
        calls.append((video_bytes, loudness, bitrate))
        return b'regenerated:' + video_bytes

    monkeypatch.setattr(clip_store_module, 'normalize_video_audio_loudness', _fake_normalize)

    batches = [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            audio_normalization=normalization,
        )
    ]

    assert calls == [(b'raw-first', -14, 128)]
    assert batches == [[Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'regenerated:raw-first')]]
    assert s3_client.objects[normalized_key_1] == b'regenerated:raw-first'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_1,
                video_hash=_HASH_A,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
                audio_normalization=normalization,
            )
        ]
    )


@pytest.mark.asyncio
async def test_fetch_with_clip_ids_returns_only_requested_sub_group_subset_in_manifest_order() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    clip_key_3 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    clip_key_4 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=2,
                        ),
                        ManifestEntry(
                            id=_UUID_3,
                            video_hash=_HASH_C,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=2,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_4,
                            video_hash=_HASH_D,
                            sub_season=SubSeason.B,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        ),
                    ]
                ),
                clip_key_1: b'batch-1-first',
                clip_key_2: b'batch-1-second',
                clip_key_3: b'batch-2-first',
                clip_key_4: b'other-sub-group',
            }
        )
    )

    batches = [
        batch
        async for batch in store.fetch(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            clip_ids=[_UUID_3, _UUID_1],
        )
    ]

    assert batches == [
        [Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'batch-1-first')],
        [Clip(filename=ClipStore._s3_key_to_filename(clip_key_3), bytes=b'batch-2-first')],
    ]


@pytest.mark.asyncio
async def test_fetch_with_duplicate_clip_ids_raises() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(DuplicateClipIdsError, match=_UUID_1):
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
                clip_ids=[_UUID_1, _UUID_1],
            )
        ]


@pytest.mark.asyncio
async def test_fetch_with_unknown_clip_ids_raises() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(UnknownClipsError, match='not present in manifest'):
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
                clip_ids=[_UUID_2],
            )
        ]


@pytest.mark.asyncio
async def test_fetch_with_clip_ids_from_other_sub_group_raises() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.B,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        ),
                    ]
                )
            }
        )
    )

    with pytest.raises(ClipIdsNotInSubGroupError, match=_UUID_2):
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
                clip_ids=[_UUID_2],
            )
        ]


@pytest.mark.asyncio
async def test_fetch_fails_with_empty_sub_group_fields_when_group_is_missing() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            )
        ]

    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.universe is Universe.WEST
    assert excinfo.value.sub_season is None
    assert excinfo.value.scope is None


@pytest.mark.asyncio
async def test_fetch_fails_with_requested_sub_group_fields_when_sub_group_is_missing() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.B,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        [
            batch
            async for batch in store.fetch(
                ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            )
        ]

    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.universe is Universe.WEST
    assert excinfo.value.sub_season is SubSeason.A
    assert excinfo.value.scope is Scope.COLLECTION


@pytest.mark.asyncio
async def test_list_groups_returns_parsed_groups() -> None:
    store = ClipStore(
        _FakeS3Client(
            prefixes=[
                'clips/west-2024-1/',
                'clips/east-2025-2',
            ]
        )
    )

    assert await store.list_groups() == [
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipGroup(universe=Universe.EAST, year=2025, season=Season.S2),
    ]


@pytest.mark.asyncio
async def test_list_groups_returns_sorted_groups() -> None:
    store = ClipStore(
        _FakeS3Client(
            prefixes=[
                'clips/west-2025-2',
                'clips/east-2024-2',
                'clips/west-2024-1',
                'clips/east-2024-1',
            ]
        )
    )

    assert await store.list_groups() == [
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipGroup(universe=Universe.WEST, year=2025, season=Season.S2),
        ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
        ClipGroup(universe=Universe.EAST, year=2024, season=Season.S2),
    ]


@pytest.mark.asyncio
async def test_list_groups_fails_on_malformed_prefix() -> None:
    store = ClipStore(_FakeS3Client(prefixes=['clips/west-2024-1/extra']))

    with pytest.raises(ValueError, match=r"'clips/west-2024-1/extra'"):
        await store.list_groups()


@pytest.mark.asyncio
async def test_list_sub_groups_returns_unique_pairs() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=2,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_4,
                            video_hash=_HASH_C,
                            sub_season=SubSeason.NONE,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        ),
                    ]
                )
            }
        )
    )

    assert await store.list_sub_groups(
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
    ) == [
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    ]


@pytest.mark.asyncio
async def test_list_sub_groups_returns_sorted_pairs() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.B,
                            scope=Scope.SOURCE,
                            batch=1,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.NONE,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_4,
                            video_hash=_HASH_C,
                            sub_season=SubSeason.B,
                            scope=Scope.COLLECTION,
                            batch=2,
                            order=1,
                        ),
                    ]
                )
            }
        )
    )

    assert await store.list_sub_groups(
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
    ) == [
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ClipSubGroup(sub_season=SubSeason.B, scope=Scope.COLLECTION),
        ClipSubGroup(sub_season=SubSeason.B, scope=Scope.SOURCE),
    ]


@pytest.mark.asyncio
async def test_list_sub_groups_fails_on_missing_manifest() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        await store.list_sub_groups(ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1))

    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.universe is Universe.WEST
    assert excinfo.value.sub_season is None
    assert excinfo.value.scope is None


@pytest.mark.asyncio
async def test_list_sub_groups_fails_on_corrupted_manifest() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(_FakeS3Client({manifest_key: b'{"clips": []}'}))

    with pytest.raises(ManifestCorruptedError):
        await store.list_sub_groups(ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1))


@pytest.mark.asyncio
async def test_store_treats_existing_current_group_id_as_duplicate(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_B})
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    )
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename=clip_key, bytes=b'clip')],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result.stored_count == 0
    assert result.duplicate_count == 1
    assert s3_client.put_calls == []


@pytest.mark.asyncio
async def test_store_generates_fresh_id_for_non_s3_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_A})
    _patch_uuid7(monkeypatch, _UUID_4)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename='incoming.mp4', bytes=b'clip')],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.B, scope=Scope.EXTRA),
    )

    assert result == clip_store_module.StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_4,),
    )
    assert s3_client.objects[clip_key] == b'clip'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_A,
                sub_season=SubSeason.B,
                scope=Scope.EXTRA,
                batch=1,
                order=1,
            )
        ]
    )


@pytest.mark.asyncio
async def test_store_generates_new_id_for_same_group_s3_like_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_C})
    _patch_uuid7(monkeypatch, _UUID_4)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    source_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    target_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    )
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename=source_clip_key, bytes=b'clip')],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == clip_store_module.StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_4,),
    )
    assert s3_client.objects[target_clip_key] == b'clip'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_C,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=2,
                order=1,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_store_generates_new_id_for_s3_like_filename_from_different_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_A})
    _patch_uuid7(monkeypatch, _UUID_4)
    source_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    target_manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.EAST)
    target_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.EAST, clip_id=_UUID_4)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename=source_clip_key, bytes=b'clip')],
        group=ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.B, scope=Scope.EXTRA),
    )

    assert result == clip_store_module.StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_4,),
    )
    assert s3_client.objects[target_clip_key] == b'clip'
    assert json.loads(s3_client.objects[target_manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_A,
                sub_season=SubSeason.B,
                scope=Scope.EXTRA,
                batch=1,
                order=1,
            )
        ]
    )


@pytest.mark.asyncio
async def test_store_treats_existing_video_hash_as_duplicate(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_A})
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    )
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename='incoming.mp4', bytes=b'clip')],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result.stored_count == 0
    assert result.duplicate_count == 1
    assert s3_client.put_calls == []


@pytest.mark.asyncio
async def test_store_generates_new_ids_for_same_call_repeated_unadopted_parsed_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'first': _HASH_A, b'second': _HASH_B})
    _patch_uuid7(monkeypatch, _UUID_4, _UUID_2)
    source_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    target_manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.EAST)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)

    result = await store.store(
        [
            Clip(filename=source_clip_key, bytes=b'first'),
            Clip(filename=source_clip_key, bytes=b'second'),
        ],
        group=ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result.stored_count == 2
    assert result.duplicate_count == 0
    assert result.clip_ids == (_UUID_4, _UUID_2)
    assert json.loads(s3_client.objects[target_manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_A,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=2,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_store_deduplicates_same_call_by_video_hash_and_keeps_dense_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'first': _HASH_C, b'second': _HASH_C, b'third': _HASH_D})
    _patch_uuid7(monkeypatch, _UUID_4, _UUID_5)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)

    result = await store.store(
        [
            Clip(filename='first.mp4', bytes=b'first'),
            Clip(filename='second.mp4', bytes=b'second'),
            Clip(filename='third.mp4', bytes=b'third'),
        ],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.C, scope=Scope.SOURCE),
    )

    assert result == StoreResult(
        stored_count=2,
        duplicate_count=1,
        clip_ids=(_UUID_4, _UUID_5),
    )
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_C,
                sub_season=SubSeason.C,
                scope=Scope.SOURCE,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_5,
                video_hash=_HASH_D,
                sub_season=SubSeason.C,
                scope=Scope.SOURCE,
                batch=1,
                order=2,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_store_creates_new_batch_per_call_and_resets_order(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_hashes(
        monkeypatch,
        {
            b'first': _HASH_A,
            b'second': _HASH_B,
            b'third': _HASH_C,
        },
    )
    _patch_uuid7(monkeypatch, _UUID_1, _UUID_2, _UUID_3)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION)

    first_result = await store.store(
        [
            Clip(filename='first.mp4', bytes=b'first'),
            Clip(filename='second.mp4', bytes=b'second'),
        ],
        group=clip_group,
        sub_group=clip_sub_group,
    )
    second_result = await store.store(
        [Clip(filename='third.mp4', bytes=b'third')],
        group=clip_group,
        sub_group=clip_sub_group,
    )

    assert first_result == StoreResult(
        stored_count=2,
        duplicate_count=0,
        clip_ids=(_UUID_1, _UUID_2),
    )
    assert second_result == StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_3,),
    )
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_1,
                video_hash=_HASH_A,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=2,
            ),
            ManifestEntry(
                id=_UUID_3,
                video_hash=_HASH_C,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=2,
                order=1,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_store_all_duplicates_do_not_create_new_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_A})
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    original_manifest = [
        ManifestEntry(
            id=_UUID_1,
            video_hash=_HASH_A,
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=1,
            order=1,
        )
    ]
    s3_client = _FakeS3Client({manifest_key: _manifest_bytes(original_manifest)})
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename='incoming.mp4', bytes=b'clip')],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == StoreResult(stored_count=0, duplicate_count=1)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == Manifest(original_manifest).to_dict()
    assert s3_client.put_calls == []


@pytest.mark.asyncio
async def test_store_propagates_first_clip_upload_failure_without_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'clip': _HASH_B})
    _patch_uuid7(monkeypatch, _UUID_2)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    original_manifest = [
        ManifestEntry(
            id=_UUID_1,
            video_hash=_HASH_A,
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=1,
            order=1,
        )
    ]
    s3_client = _FakeS3Client(
        {manifest_key: _manifest_bytes(original_manifest)},
        put_failures={clip_key},
    )
    store = ClipStore(s3_client)
    clip_group_prefix = store._clip_group_prefix(
        universe=clip_group.universe,
        year=clip_group.year,
        season=clip_group.season,
    )

    with pytest.raises(RuntimeError, match=f'boom putting {clip_key}'):
        await store.store(
            [Clip(filename='incoming.mp4', bytes=b'clip')],
            group=clip_group,
            sub_group=clip_sub_group,
        )

    assert manifest_key not in [call[0] for call in s3_client.put_calls]
    assert clip_key not in s3_client.objects
    assert s3_client.deleted_keys == []
    assert store._manifest_cache[clip_group_prefix].to_dict() == Manifest(original_manifest).to_dict()


@pytest.mark.asyncio
async def test_store_raises_sync_error_when_later_clip_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'first': _HASH_B, b'second': _HASH_C})
    _patch_uuid7(monkeypatch, _UUID_2, _UUID_3)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    first_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    second_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    original_manifest = [
        ManifestEntry(
            id=_UUID_1,
            video_hash=_HASH_A,
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=1,
            order=1,
        )
    ]
    s3_client = _FakeS3Client(
        {manifest_key: _manifest_bytes(original_manifest)},
        put_failures={second_clip_key},
    )
    store = ClipStore(s3_client)
    clip_group_prefix = store._clip_group_prefix(
        universe=clip_group.universe,
        year=clip_group.year,
        season=clip_group.season,
    )

    with pytest.raises(ClipManifestSyncError, match='Staged clip store failed at clip_upload') as excinfo:
        await store.store(
            [
                Clip(filename='first.mp4', bytes=b'first'),
                Clip(filename='second.mp4', bytes=b'second'),
            ],
            group=clip_group,
            sub_group=clip_sub_group,
        )

    assert excinfo.value.stage == 'clip_upload'
    assert excinfo.value.written_keys == (first_clip_key,)
    assert excinfo.value.affected_clip_ids == (_UUID_2,)
    assert excinfo.value.manifest_key == manifest_key
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == f'boom putting {second_clip_key}'
    assert excinfo.value.__notes__ == [f"Original clip upload error: RuntimeError('boom putting {second_clip_key}')"]
    assert first_clip_key in s3_client.objects
    assert second_clip_key not in s3_client.objects
    assert manifest_key in s3_client.objects
    assert s3_client.objects[manifest_key] == _manifest_bytes(original_manifest)
    assert manifest_key not in [call[0] for call in s3_client.put_calls]
    assert s3_client.deleted_keys == []
    assert store._manifest_cache[clip_group_prefix].to_dict() == Manifest(original_manifest).to_dict()


@pytest.mark.asyncio
async def test_store_raises_sync_error_when_manifest_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'first': _HASH_B, b'second': _HASH_C})
    _patch_uuid7(monkeypatch, _UUID_2, _UUID_3)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    first_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    second_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    original_manifest = [
        ManifestEntry(
            id=_UUID_1,
            video_hash=_HASH_A,
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=1,
            order=1,
        )
    ]
    s3_client = _FakeS3Client(
        {manifest_key: _manifest_bytes(original_manifest)},
        put_failures={manifest_key},
    )
    store = ClipStore(s3_client)
    clip_group_prefix = store._clip_group_prefix(
        universe=clip_group.universe,
        year=clip_group.year,
        season=clip_group.season,
    )

    with pytest.raises(ClipManifestSyncError, match='Staged clip store failed at manifest_write') as excinfo:
        await store.store(
            [
                Clip(filename='first.mp4', bytes=b'first'),
                Clip(filename='second.mp4', bytes=b'second'),
            ],
            group=clip_group,
            sub_group=clip_sub_group,
        )

    assert excinfo.value.stage == 'manifest_write'
    assert excinfo.value.written_keys == (first_clip_key, second_clip_key)
    assert excinfo.value.affected_clip_ids == (_UUID_2, _UUID_3)
    assert excinfo.value.manifest_key == manifest_key
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == f'boom putting {manifest_key}'
    assert excinfo.value.__notes__ == [f"Original manifest write error: RuntimeError('boom putting {manifest_key}')"]
    assert s3_client.objects[first_clip_key] == b'first'
    assert s3_client.objects[second_clip_key] == b'second'
    assert s3_client.objects[manifest_key] == _manifest_bytes(original_manifest)
    assert s3_client.deleted_keys == []
    assert store._manifest_cache[clip_group_prefix].to_dict() == Manifest(original_manifest).to_dict()


@pytest.mark.asyncio
async def test_derive_group_returns_expected_single_clip_group() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.B,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        ),
                    ]
                )
            }
        )
    )

    clip_group = await store.derive_group(
        [[ClipStore._s3_key_to_filename(clip_key_1), ClipStore._s3_key_to_filename(clip_key_2)]]
    )

    assert clip_group == ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)


@pytest.mark.asyncio
async def test_derive_group_rejects_empty_filename_batches() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ValueError, match='`filename_batches` must contain at least one filename'):
        await store.derive_group([[]])


@pytest.mark.asyncio
async def test_derive_group_rejects_duplicate_filenames() -> None:
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    store = ClipStore(_FakeS3Client())

    with pytest.raises(DuplicateFilenamesError):
        await store.derive_group([[ClipStore._s3_key_to_filename(clip_key), ClipStore._s3_key_to_filename(clip_key)]])


@pytest.mark.asyncio
async def test_derive_group_rejects_malformed_filename() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(InvalidFilenamesError, match='incoming.mp4'):
        await store.derive_group([['incoming.mp4']])


@pytest.mark.asyncio
async def test_derive_group_rejects_mixed_clip_groups_before_manifest_load() -> None:
    west_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    east_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.EAST, clip_id=_UUID_2)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)

    with pytest.raises(MixedClipGroupsError) as excinfo:
        await store.derive_group(
            [[ClipStore._s3_key_to_filename(west_clip_key), ClipStore._s3_key_to_filename(east_clip_key)]]
        )

    assert excinfo.value.groups == (
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
    )
    assert s3_client.get_calls == []


@pytest.mark.asyncio
async def test_derive_group_rejects_unknown_clip_ids_in_manifest() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(UnknownClipsError, match=_UUID_4) as excinfo:
        await store.derive_group([[ClipStore._s3_key_to_filename(clip_key)]])

    assert excinfo.value.clip_ids == (_UUID_4,)


@pytest.mark.asyncio
async def test_reconcile_reorders_and_rebatches_target_sub_group() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    clip_key_3 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    clip_key_4 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=3,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=3,
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_3,
                        video_hash=_HASH_C,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=10,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_4,
                        video_hash=_HASH_D,
                        sub_season=SubSeason.NONE,
                        scope=Scope.EXTRA,
                        batch=1,
                        order=1,
                    ),
                ]
            ),
            clip_key_1: b'one',
            clip_key_2: b'two',
            clip_key_3: b'three',
            clip_key_4: b'four',
        }
    )
    store = ClipStore(s3_client)

    result = await store.reconcile(
        [
            [
                ClipStore._s3_key_to_filename(clip_key_3),
                ClipStore._s3_key_to_filename(clip_key_1),
            ],
            [ClipStore._s3_key_to_filename(clip_key_2)],
        ],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == ReconcileResult(updated=3, removed=0)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_4,
                video_hash=_HASH_D,
                sub_season=SubSeason.NONE,
                scope=Scope.EXTRA,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_3,
                video_hash=_HASH_C,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_1,
                video_hash=_HASH_A,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=2,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=2,
                order=1,
            ),
        ]
    )
    assert [key for key, _, _ in s3_client.put_calls] == [manifest_key]
    assert s3_client.deleted_keys == []


@pytest.mark.asyncio
async def test_reconcile_moves_from_other_sub_group_and_deletes_omitted_clip() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    clip_key_3 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_3)
    normalized_key_1 = _normalized_clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    prior_normalization = AudioNormalization(loudness=-14, bitrate=128)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                        audio_normalization=prior_normalization,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_3,
                        video_hash=_HASH_C,
                        sub_season=SubSeason.NONE,
                        scope=Scope.EXTRA,
                        batch=2,
                        order=1,
                    ),
                ]
            ),
            clip_key_1: b'one',
            clip_key_2: b'two',
            clip_key_3: b'three',
            normalized_key_1: b'normalized-one',
        }
    )
    store = ClipStore(s3_client)

    result = await store.reconcile(
        [
            [ClipStore._s3_key_to_filename(clip_key_3), ClipStore._s3_key_to_filename(clip_key_2)],
        ],
        group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == ReconcileResult(updated=2, removed=1)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_3,
                video_hash=_HASH_C,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=2,
            ),
        ]
    )
    assert s3_client.deleted_keys == [clip_key_1, normalized_key_1]
    assert clip_key_1 not in s3_client.objects
    assert normalized_key_1 not in s3_client.objects


@pytest.mark.asyncio
async def test_reconcile_rejects_duplicate_filenames() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(DuplicateFilenamesError):
        await store.reconcile(
            [[ClipStore._s3_key_to_filename(clip_key), ClipStore._s3_key_to_filename(clip_key)]],
            group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )


@pytest.mark.asyncio
async def test_reconcile_rejects_empty_filename_batches() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ValueError, match='`filename_batches` must contain at least one filename'):
        await store.reconcile(
            [[]],
            group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )


@pytest.mark.asyncio
async def test_reconcile_rejects_invalid_filename() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(InvalidFilenamesError, match='incoming.mp4'):
        await store.reconcile(
            [['incoming.mp4']],
            group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )


@pytest.mark.asyncio
async def test_reconcile_rejects_clip_from_other_group() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    other_group_clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.EAST, clip_id=_UUID_4)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(ValueError, match='provided `clip_group`'):
        await store.reconcile(
            [[ClipStore._s3_key_to_filename(other_group_clip_key)]],
            group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )


@pytest.mark.asyncio
async def test_reconcile_updates_cache_even_if_removed_delete_fails() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                ]
            ),
            clip_key_1: b'one',
            clip_key_2: b'two',
        },
        delete_failures={clip_key_1},
    )
    store = ClipStore(s3_client)

    with pytest.raises(ReconcileDeleteError, match=clip_key_1) as excinfo:
        await store.reconcile(
            [[ClipStore._s3_key_to_filename(clip_key_2)]],
            group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )

    assert excinfo.value.failed_keys == (clip_key_1,)
    expected_manifest = _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.A,
                scope=Scope.COLLECTION,
                batch=1,
                order=1,
            )
        ]
    )
    assert list(store._manifest_cache.values())[0].to_dict() == expected_manifest
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == expected_manifest


@pytest.mark.asyncio
async def test_compact_rejects_batch_size_below_one() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ValueError, match='`batch_size` must be >= 1'):
        await store.compact(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            batch_size=0,
        )


@pytest.mark.asyncio
async def test_compact_fails_with_empty_sub_group_fields_when_group_is_missing() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        await store.compact(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            batch_size=2,
        )

    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.universe is Universe.WEST
    assert excinfo.value.sub_season is None
    assert excinfo.value.scope is None


@pytest.mark.asyncio
async def test_compact_fails_with_requested_sub_group_fields_when_sub_group_is_missing() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    store = ClipStore(
        _FakeS3Client(
            {
                manifest_key: _manifest_bytes(
                    [
                        ManifestEntry(
                            id=_UUID_1,
                            video_hash=_HASH_A,
                            sub_season=SubSeason.B,
                            scope=Scope.EXTRA,
                            batch=1,
                            order=1,
                        )
                    ]
                )
            }
        )
    )

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        await store.compact(
            ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            batch_size=2,
        )

    assert excinfo.value.year == 2024
    assert excinfo.value.season is Season.S1
    assert excinfo.value.universe is Universe.WEST
    assert excinfo.value.sub_season is SubSeason.A
    assert excinfo.value.scope is Scope.COLLECTION


@pytest.mark.asyncio
async def test_compact_preserves_relative_order_while_rewriting_positions() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    normalization = AudioNormalization(loudness=-14, bitrate=128)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_4,
                        video_hash=_HASH_D,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=10,
                        order=1,
                        audio_normalization=normalization,
                    ),
                    ManifestEntry(
                        id=_UUID_5,
                        video_hash='e' * 64,
                        sub_season=SubSeason.NONE,
                        scope=Scope.EXTRA,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=3,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_3,
                        video_hash=_HASH_C,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=3,
                        order=2,
                    ),
                ]
            )
        }
    )
    store = ClipStore(s3_client)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION)

    await store.compact(
        clip_group,
        clip_sub_group,
        batch_size=2,
    )

    rewritten_manifest = Manifest.from_dict(json.loads(s3_client.objects[manifest_key].decode('utf-8')))
    compacted_entries = sorted(
        (entry for entry in rewritten_manifest if entry.sub_season is SubSeason.A and entry.scope is Scope.COLLECTION),
        key=lambda entry: (entry.batch, entry.order),
    )

    assert [entry.id for entry in compacted_entries] == [_UUID_1, _UUID_2, _UUID_3, _UUID_4]
    assert [(entry.batch, entry.order) for entry in compacted_entries] == [(1, 1), (1, 2), (2, 1), (2, 2)]
    assert [entry.audio_normalization for entry in compacted_entries] == [None, None, None, normalization]


@pytest.mark.asyncio
async def test_compact_only_affects_specified_sub_group_and_leaves_others_unchanged() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    original_other_entries = [
        ManifestEntry(
            id=_UUID_4,
            video_hash=_HASH_D,
            sub_season=SubSeason.NONE,
            scope=Scope.EXTRA,
            batch=7,
            order=1,
        ),
        ManifestEntry(
            id=_UUID_5,
            video_hash='e' * 64,
            sub_season=SubSeason.A,
            scope=Scope.SOURCE,
            batch=3,
            order=2,
        ),
    ]
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    original_other_entries[0],
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    original_other_entries[1],
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=4,
                        order=1,
                    ),
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    await store.compact(
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        batch_size=2,
    )

    rewritten_manifest = Manifest.from_dict(json.loads(s3_client.objects[manifest_key].decode('utf-8')))
    rewritten_other_entries = [
        entry
        for entry in rewritten_manifest
        if not (entry.sub_season is SubSeason.A and entry.scope is Scope.COLLECTION)
    ]

    assert rewritten_other_entries == original_other_entries


@pytest.mark.asyncio
async def test_compact_does_not_upload_manifest_when_positions_do_not_change() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=2,
                    ),
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    await store.compact(
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        batch_size=2,
    )

    assert s3_client.put_calls == []


@pytest.mark.asyncio
async def test_compact_updates_manifest_cache_consistently_after_rewrite() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    original_manifest = [
        ManifestEntry(
            id=_UUID_1,
            video_hash=_HASH_A,
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=1,
            order=1,
        ),
        ManifestEntry(
            id=_UUID_2,
            video_hash=_HASH_B,
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=3,
            order=1,
        ),
    ]
    s3_client = _FakeS3Client({manifest_key: _manifest_bytes(original_manifest)})
    store = ClipStore(s3_client)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION)

    await store.compact(
        clip_group,
        clip_sub_group,
        batch_size=2,
    )

    clip_group_prefix = store._clip_group_prefix(
        year=clip_group.year,
        season=clip_group.season,
        universe=clip_group.universe,
    )
    cached_entries = list(store._manifest_cache[clip_group_prefix])
    assert [(entry.id, entry.batch, entry.order) for entry in cached_entries] == [
        (_UUID_1, 1, 1),
        (_UUID_2, 1, 2),
    ]

    s3_client.objects[manifest_key] = _manifest_bytes(original_manifest)
    await store.compact(
        clip_group,
        clip_sub_group,
        batch_size=2,
    )

    assert len(s3_client.put_calls) == 1


@pytest.mark.asyncio
async def test_compact_is_manifest_only_and_does_not_touch_clip_objects() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key_1 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_1)
    clip_key_2 = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_2)
    s3_client = _FakeS3Client(
        {
            manifest_key: _manifest_bytes(
                [
                    ManifestEntry(
                        id=_UUID_1,
                        video_hash=_HASH_A,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=1,
                        order=1,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        batch=2,
                        order=1,
                    ),
                ]
            ),
            clip_key_1: b'clip-1',
            clip_key_2: b'clip-2',
        }
    )
    store = ClipStore(s3_client)

    await store.compact(
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        batch_size=2,
    )

    assert s3_client.get_calls == [manifest_key]
    assert [call[0] for call in s3_client.put_calls] == [manifest_key]
    assert s3_client.objects[clip_key_1] == b'clip-1'
    assert s3_client.objects[clip_key_2] == b'clip-2'
    assert s3_client.deleted_keys == []


@pytest.mark.asyncio
async def test_compact_can_pull_newly_stored_single_clip_into_previous_batch_when_there_is_space(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'first': _HASH_A, b'second': _HASH_B})
    _patch_uuid7(monkeypatch, _UUID_1, _UUID_2)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)
    clip_group = ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1)
    clip_sub_group = ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA)

    await store.store(
        [Clip(filename='first.mp4', bytes=b'first')],
        group=clip_group,
        sub_group=clip_sub_group,
    )
    await store.store(
        [Clip(filename='second.mp4', bytes=b'second')],
        group=clip_group,
        sub_group=clip_sub_group,
    )

    await store.compact(
        clip_group,
        clip_sub_group,
        batch_size=2,
    )

    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == _manifest_payload(
        [
            ManifestEntry(
                id=_UUID_1,
                video_hash=_HASH_A,
                sub_season=SubSeason.NONE,
                scope=Scope.EXTRA,
                batch=1,
                order=1,
            ),
            ManifestEntry(
                id=_UUID_2,
                video_hash=_HASH_B,
                sub_season=SubSeason.NONE,
                scope=Scope.EXTRA,
                batch=1,
                order=2,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_compact_with_batch_size_ten_creates_dense_batches_with_final_partial() -> None:
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    entries = [
        ManifestEntry(
            id=uuid.uuid7().hex,
            video_hash=f'{index + 1:064x}',
            sub_season=SubSeason.A,
            scope=Scope.COLLECTION,
            batch=(index * 3) + 1,
            order=1,
        )
        for index in range(12)
    ]
    s3_client = _FakeS3Client({manifest_key: _manifest_bytes(entries)})
    store = ClipStore(s3_client)

    await store.compact(
        ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        batch_size=10,
    )

    rewritten_manifest = Manifest.from_dict(json.loads(s3_client.objects[manifest_key].decode('utf-8')))
    compacted_entries = sorted(
        (entry for entry in rewritten_manifest if entry.sub_season is SubSeason.A and entry.scope is Scope.COLLECTION),
        key=lambda entry: (entry.batch, entry.order),
    )

    assert [(entry.batch, entry.order) for entry in compacted_entries] == [
        *[(1, order) for order in range(1, 11)],
        (2, 1),
        (2, 2),
    ]
