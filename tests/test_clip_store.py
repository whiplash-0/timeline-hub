import json
import uuid

import pytest

import general_bot.clip_store as clip_store_module
from general_bot.clip_store import (
    Clip,
    ClipGroup,
    ClipGroupNotFoundError,
    ClipSubGroup,
    ClipStore,
    Manifest,
    ManifestCorruptedError,
    ManifestEntry,
    Scope,
    Season,
    StoreResult,
    SubSeason,
    Universe,
)
from general_bot.infra.s3 import S3Client, S3ObjectNotFoundError

_UUID_1 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e1').hex
_UUID_2 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e2').hex
_UUID_4 = uuid.UUID('018f05c1-f1a3-7b34-8d29-1f53a1c9d0e4').hex
_HASH_A = 'a' * 64
_HASH_B = 'b' * 64
_HASH_C = 'c' * 64


def test_store_result_adds_counts() -> None:
    assert StoreResult(stored_count=1, duplicate_count=2) + StoreResult(stored_count=3, duplicate_count=4) == StoreResult(
        stored_count=4,
        duplicate_count=6,
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
        prefixes: list[str] | None = None,
    ) -> None:
        self.objects = dict(objects or {})
        self.prefixes = list(prefixes or [])
        self.put_calls: list[tuple[str, bytes, str | None]] = []
        self.deleted_keys: list[str] = []

    async def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        self.objects[key] = data
        self.put_calls.append((key, data, content_type))

    async def get_bytes(self, key: str) -> bytes:
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
        self.deleted_keys.append(key)
        self.objects.pop(key, None)


def _clip_key(*, year: int, season: Season, universe: Universe, clip_id: str) -> str:
    return S3Client.join('clips', f'{year}-{season}-{universe}', clip_id + '.mp4')


def _manifest_key(*, year: int, season: Season, universe: Universe) -> str:
    return S3Client.join('clips', f'{year}-{season}-{universe}', 'manifest.json')


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
        order=1,
    )

    payload = Manifest([entry]).to_list()

    assert payload == [
        {
            'id': _UUID_1,
            'video_hash': _HASH_A,
            'sub_season': 'A',
            'scope': 'collection',
            'order': 1,
        }
    ]
    assert list(payload[0]) == ['id', 'video_hash', 'sub_season', 'scope', 'order']
    assert list(Manifest.from_list(payload)) == [entry]


def test_manifest_rejects_old_object_wrapper_shape() -> None:
    with pytest.raises(ValueError, match='manifest root must be a list'):
        Manifest.from_list({'clips': []})


def test_manifest_reads_legacy_null_sub_season_and_rewrites_none() -> None:
    manifest = Manifest.from_list(
        [
            {
                'id': _UUID_1,
                'video_hash': _HASH_A,
                'sub_season': None,
                'scope': 'extra',
                'order': 1,
            }
        ]
    )

    assert list(manifest) == [
        ManifestEntry(
            id=_UUID_1,
            video_hash=_HASH_A,
            sub_season=SubSeason.NONE,
            scope=Scope.EXTRA,
            order=1,
        )
    ]
    assert manifest.to_list() == [
        {
            'id': _UUID_1,
            'video_hash': _HASH_A,
            'sub_season': 'none',
            'scope': 'extra',
            'order': 1,
        }
    ]


@pytest.mark.asyncio
async def test_fetch_returns_clips_with_portable_filenames() -> None:
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
                        order=2,
                    ),
                    ManifestEntry(
                        id=_UUID_2,
                        video_hash=_HASH_B,
                        sub_season=SubSeason.A,
                        scope=Scope.COLLECTION,
                        order=1,
                    ),
                ]
            ),
            clip_key_1: b'second',
            clip_key_2: b'first',
        }
    )
    store = ClipStore(s3_client)

    batches = [
        batch
        async for batch in store.fetch(
            clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
            clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
            batch_size=2,
        )
    ]

    assert batches == [
        [
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_2), bytes=b'first'),
            Clip(filename=ClipStore._s3_key_to_filename(clip_key_1), bytes=b'second'),
        ]
    ]


@pytest.mark.asyncio
async def test_fetch_fails_with_empty_sub_group_fields_when_group_is_missing() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        [
            batch
            async for batch in store.fetch(
                clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
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
                clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
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
                'clips/2024-1-west/',
                'clips/2025-2-east',
            ]
        )
    )

    assert await store.list_groups() == [
        ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        ClipGroup(year=2025, season=Season.S2, universe=Universe.EAST),
    ]


@pytest.mark.asyncio
async def test_list_groups_returns_sorted_groups() -> None:
    store = ClipStore(
        _FakeS3Client(
            prefixes=[
                'clips/2025-2-west',
                'clips/2024-2-east',
                'clips/2024-1-west',
                'clips/2024-1-east',
            ]
        )
    )

    assert await store.list_groups() == [
        ClipGroup(year=2024, season=Season.S1, universe=Universe.EAST),
        ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        ClipGroup(year=2024, season=Season.S2, universe=Universe.EAST),
        ClipGroup(year=2025, season=Season.S2, universe=Universe.WEST),
    ]


@pytest.mark.asyncio
async def test_list_groups_fails_on_malformed_prefix() -> None:
    store = ClipStore(_FakeS3Client(prefixes=['clips/2024-1-west/extra']))

    with pytest.raises(ValueError, match=r"'clips/2024-1-west/extra'"):
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
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.A,
                            scope=Scope.COLLECTION,
                            order=2,
                        ),
                        ManifestEntry(
                            id=_UUID_4,
                            video_hash=_HASH_C,
                            sub_season=SubSeason.NONE,
                            scope=Scope.EXTRA,
                            order=1,
                        ),
                    ]
                )
            }
        )
    )

    assert await store.list_sub_groups(
        ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
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
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_2,
                            video_hash=_HASH_B,
                            sub_season=SubSeason.NONE,
                            scope=Scope.EXTRA,
                            order=1,
                        ),
                        ManifestEntry(
                            id=_UUID_4,
                            video_hash=_HASH_C,
                            sub_season=SubSeason.B,
                            scope=Scope.COLLECTION,
                            order=2,
                        ),
                    ]
                )
            }
        )
    )

    assert await store.list_sub_groups(
        ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
    ) == [
        ClipSubGroup(sub_season=SubSeason.NONE, scope=Scope.EXTRA),
        ClipSubGroup(sub_season=SubSeason.B, scope=Scope.COLLECTION),
        ClipSubGroup(sub_season=SubSeason.B, scope=Scope.SOURCE),
    ]


@pytest.mark.asyncio
async def test_list_sub_groups_fails_on_missing_manifest() -> None:
    store = ClipStore(_FakeS3Client())

    with pytest.raises(ClipGroupNotFoundError) as excinfo:
        await store.list_sub_groups(ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST))

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
        await store.list_sub_groups(ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST))


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
                        order=1,
                    )
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename=clip_key, bytes=b'clip')],
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
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
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.B, scope=Scope.EXTRA),
    )

    assert result == clip_store_module.StoreResult(stored_count=1, duplicate_count=0)
    assert s3_client.objects[clip_key] == b'clip'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_A,
            'sub_season': 'B',
            'scope': 'extra',
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
                        order=1,
                    )
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename=source_clip_key, bytes=b'clip')],
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result == clip_store_module.StoreResult(stored_count=1, duplicate_count=0)
    assert s3_client.objects[target_clip_key] == b'clip'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'order': 1,
        },
        {
            'id': _UUID_4,
            'video_hash': _HASH_C,
            'sub_season': 'A',
            'scope': 'collection',
            'order': 2,
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
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.EAST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.B, scope=Scope.EXTRA),
    )

    assert result == clip_store_module.StoreResult(stored_count=1, duplicate_count=0)
    assert s3_client.objects[target_clip_key] == b'clip'
    assert json.loads(s3_client.objects[target_manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_A,
            'sub_season': 'B',
            'scope': 'extra',
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
                        order=1,
                    )
                ]
            )
        }
    )
    store = ClipStore(s3_client)

    result = await store.store(
        [Clip(filename='incoming.mp4', bytes=b'clip')],
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
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
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.EAST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.A, scope=Scope.COLLECTION),
    )

    assert result.stored_count == 2
    assert result.duplicate_count == 0
    assert json.loads(s3_client.objects[target_manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_A,
            'sub_season': 'A',
            'scope': 'collection',
            'order': 1,
        },
        {
            'id': _UUID_2,
            'video_hash': _HASH_B,
            'sub_season': 'A',
            'scope': 'collection',
            'order': 2,
        }
    ]


@pytest.mark.asyncio
async def test_store_deduplicates_same_call_by_video_hash_and_generates_new_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_hashes(monkeypatch, {b'first': _HASH_C, b'second': _HASH_C})
    _patch_uuid7(monkeypatch, _UUID_4)
    manifest_key = _manifest_key(year=2024, season=Season.S1, universe=Universe.WEST)
    clip_key = _clip_key(year=2024, season=Season.S1, universe=Universe.WEST, clip_id=_UUID_4)
    s3_client = _FakeS3Client()
    store = ClipStore(s3_client)

    result = await store.store(
        [
            Clip(filename='first.mp4', bytes=b'first'),
            Clip(filename='second.mp4', bytes=b'second'),
        ],
        clip_group=ClipGroup(year=2024, season=Season.S1, universe=Universe.WEST),
        clip_sub_group=ClipSubGroup(sub_season=SubSeason.C, scope=Scope.SOURCE),
    )

    assert result.stored_count == 1
    assert result.duplicate_count == 1
    assert s3_client.objects[clip_key] == b'first'
    assert json.loads(s3_client.objects[manifest_key].decode('utf-8')) == [
        {
            'id': _UUID_4,
            'video_hash': _HASH_C,
            'sub_season': 'C',
            'scope': 'source',
            'order': 1,
        }
    ]
