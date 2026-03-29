import json
import uuid

import pytest

import general_bot.services.clip_store as clip_store_module
from general_bot.infra.s3 import S3Client, S3ObjectNotFoundError
from general_bot.services.clip_store import (
    Clip,
    ClipGroup,
    ClipGroupNotFoundError,
    ClipIdsNotInSubGroupError,
    ClipStore,
    ClipSubGroup,
    DuplicateClipIdsError,
    DuplicateFilenamesError,
    InvalidFilenamesError,
    Manifest,
    ManifestCorruptedError,
    ManifestEntry,
    MixedClipGroupsError,
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
        prefixes: list[str] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.delete_failures = set(delete_failures or set())
        self.prefixes = list(prefixes or [])
        self.get_calls: list[str] = []
        self.put_calls: list[tuple[str, bytes, str | None]] = []
        self.deleted_keys: list[str] = []

    async def put_bytes(self, key: str, *, bytes_: bytes, content_type: str | None = None) -> None:
        self.objects[key] = bytes_
        self.put_calls.append((key, bytes_, content_type))

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
    return json.dumps(Manifest(entries).to_list(), separators=(',', ':')).encode('utf-8')


def _patch_hashes(monkeypatch: pytest.MonkeyPatch, hashes: dict[bytes, str]) -> None:
    async def _fake_hash(self: ClipStore, video_bytes: bytes) -> str:
        return hashes[video_bytes]

    monkeypatch.setattr(ClipStore, '_hash_video_bytes', _fake_hash)


def _patch_uuid7(monkeypatch: pytest.MonkeyPatch, *clip_ids: str) -> None:
    uuids = iter(uuid.UUID(clip_id) for clip_id in clip_ids)
    monkeypatch.setattr(clip_store_module, '_uuid7', lambda: next(uuids))


@pytest.mark.asyncio
async def test_manifest_uses_top_level_list_with_preferred_field_order() -> None:
    entry = ManifestEntry(
        id=_UUID_1,
        video_hash=_HASH_A,
        sub_season=SubSeason.A,
        scope=Scope.COLLECTION,
        batch=1,
        order=1,
    )

    payload = Manifest([entry]).to_list()

    assert payload == [
        {
            'id': _UUID_1,
            'video_hash': _HASH_A,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        }
    ]
    assert list(payload[0]) == ['id', 'video_hash', 'sub_season', 'scope', 'batch', 'order']
    assert list(Manifest.from_list(payload)) == [entry]


def test_manifest_rejects_old_object_wrapper_shape() -> None:
    with pytest.raises(ValueError, match='manifest root must be a list'):
        Manifest.from_list({'clips': []})


def test_manifest_rejects_legacy_null_sub_season() -> None:
    with pytest.raises(ValueError, match='manifest `sub_season` must be a string'):
        Manifest.from_list(
            [
                {
                    'id': _UUID_1,
                    'video_hash': _HASH_A,
                    'sub_season': None,
                    'scope': 'extra',
                    'batch': 1,
                    'order': 1,
                }
            ]
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
        'sub_season': 'A',
        'scope': 'collection',
        'batch': 1,
        'order': 1,
    }
    payload[field] = value

    with pytest.raises(ValueError, match=expected_message):
        Manifest.from_list([payload])


def test_manifest_rejects_duplicate_batch_order_position() -> None:
    with pytest.raises(
        ValueError,
        match='duplicate manifest position for sub_season=A scope=collection batch=2 order=1',
    ):
        Manifest.from_list(
            [
                {
                    'id': _UUID_1,
                    'video_hash': _HASH_A,
                    'sub_season': 'A',
                    'scope': 'collection',
                    'batch': 2,
                    'order': 1,
                },
                {
                    'id': _UUID_2,
                    'video_hash': _HASH_B,
                    'sub_season': 'A',
                    'scope': 'collection',
                    'batch': 2,
                    'order': 1,
                },
            ]
        )


@pytest.mark.asyncio
async def test_fetch_returns_grouped_clips_with_portable_filenames() -> None:
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

    batches = [
        batch
        async for batch in store.fetch(
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
                clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
                clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
                clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
                clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
                clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
                clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.B, scope=Scope.EXTRA),
    )

    assert result == clip_store_module.StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_4,),
    )
    assert s3_client.objects[clip_key] == b'clip'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_A,
            'sub_season': 'B',
            'scope': 'extra',
            'batch': 1,
            'order': 1,
        }
    ]


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == clip_store_module.StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_4,),
    )
    assert s3_client.objects[target_clip_key] == b'clip'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_4,
            'video_hash': _HASH_C,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 2,
            'order': 1,
        },
    ]


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
        clip_group=ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.B, scope=Scope.EXTRA),
    )

    assert result == clip_store_module.StoreResult(
        stored_count=1,
        duplicate_count=0,
        clip_ids=(_UUID_4,),
    )
    assert s3_client.objects[target_clip_key] == b'clip'
    assert json.loads(s3_client.objects[target_manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_A,
            'sub_season': 'B',
            'scope': 'extra',
            'batch': 1,
            'order': 1,
        }
    ]


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
        clip_group=ClipGroup(universe=Universe.EAST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result.stored_count == 2
    assert result.duplicate_count == 0
    assert result.clip_ids == (_UUID_4, _UUID_2)
    assert json.loads(s3_client.objects[target_manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_A,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 2,
        },
    ]


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.C, scope=Scope.SOURCE),
    )

    assert result == StoreResult(
        stored_count=2,
        duplicate_count=1,
        clip_ids=(_UUID_4, _UUID_5),
    )
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_C,
            'sub_season': 'C',
            'scope': 'source',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_5,
            'video_hash': _HASH_D,
            'sub_season': 'C',
            'scope': 'source',
            'batch': 1,
            'order': 2,
        },
    ]


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
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
    )
    second_result = await store.store(
        [Clip(filename='third.mp4', bytes=b'third')],
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
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
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_1,
            'video_hash': _HASH_A,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 2,
        },
        {
            'id': _UUID_3,
            'video_hash': _HASH_C,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 2,
            'order': 1,
        },
    ]


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == StoreResult(stored_count=0, duplicate_count=1)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == Manifest(original_manifest).to_list()
    assert s3_client.put_calls == []


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == ReconcileResult(updated=3, removed=0)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_D,
            'sub_season': 'none',
            'scope': 'extra',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_3,
            'video_hash': _HASH_C,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_1,
            'video_hash': _HASH_A,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 2,
        },
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 2,
            'order': 1,
        },
    ]
    assert [key for key, _, _ in s3_client.put_calls] == [manifest_key]
    assert s3_client.deleted_keys == []


@pytest.mark.asyncio
async def test_reconcile_moves_from_other_sub_group_and_deletes_omitted_clip() -> None:
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
        }
    )
    store = ClipStore(s3_client)

    result = await store.reconcile(
        [[ClipStore._s3_key_to_filename(clip_key_3), ClipStore._s3_key_to_filename(clip_key_2)]],
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == ReconcileResult(updated=2, removed=1)
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_3,
            'video_hash': _HASH_C,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 2,
        },
    ]
    assert s3_client.deleted_keys == [clip_key_1]
    assert clip_key_1 not in s3_client.objects


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
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )


@pytest.mark.asyncio
async def test_reconcile_rejects_empty_filename_batches() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ValueError, match='`filename_batches` must contain at least one filename'):
        await store.reconcile(
            [[]],
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        )

    assert excinfo.value.failed_keys == (clip_key_1,)
    assert list(store._manifest_cache.values())[0].to_list() == [
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        }
    ]
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'batch': 1,
            'order': 1,
        }
    ]


@pytest.mark.asyncio
async def test_compact_rejects_batch_size_below_one() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ValueError, match='`batch_size` must be >= 1'):
        await store.compact(
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            batch_size=0,
        )


@pytest.mark.asyncio
async def test_compact_fails_with_empty_sub_group_fields_when_group_is_missing() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        await store.compact(
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
            clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
        batch_size=2,
    )

    rewritten_manifest = Manifest.from_list(json.loads(s3_client.objects[manifest_key].decode('utf-8')))
    compacted_entries = sorted(
        (entry for entry in rewritten_manifest if entry.sub_season is SubSeason.A and entry.scope is Scope.COLLECTION),
        key=lambda entry: (entry.batch, entry.order),
    )

    assert [entry.id for entry in compacted_entries] == [_UUID_1, _UUID_2, _UUID_3, _UUID_4]
    assert [(entry.batch, entry.order) for entry in compacted_entries] == [(1, 1), (1, 2), (2, 1), (2, 2)]


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        batch_size=2,
    )

    rewritten_manifest = Manifest.from_list(json.loads(s3_client.objects[manifest_key].decode('utf-8')))
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
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
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
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
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
    )
    await store.store(
        [Clip(filename='second.mp4', bytes=b'second')],
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
    )

    await store.compact(
        clip_group=clip_group,
        clip_sub_group=clip_sub_group,
        batch_size=2,
    )

    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_1,
            'video_hash': _HASH_A,
            'sub_season': 'none',
            'scope': 'extra',
            'batch': 1,
            'order': 1,
        },
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'none',
            'scope': 'extra',
            'batch': 1,
            'order': 2,
        },
    ]


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
        clip_group=ClipGroup(universe=Universe.WEST, year=2024, season=Season.S1),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
        batch_size=10,
    )

    rewritten_manifest = Manifest.from_list(json.loads(s3_client.objects[manifest_key].decode('utf-8')))
    compacted_entries = sorted(
        (entry for entry in rewritten_manifest if entry.sub_season is SubSeason.A and entry.scope is Scope.COLLECTION),
        key=lambda entry: (entry.batch, entry.order),
    )

    assert [(entry.batch, entry.order) for entry in compacted_entries] == [
        *[(1, order) for order in range(1, 11)],
        (2, 1),
        (2, 2),
    ]
