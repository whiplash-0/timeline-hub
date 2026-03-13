import io
from pathlib import Path
from typing import Self

import pytest
from botocore.exceptions import ClientError

import general_bot.s3 as s3_module
from general_bot.s3 import S3Config, S3ObjectNotFoundError, S3Client


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({'Error': {'Code': code, 'Message': 'err'}}, operation)


class _FakeBody:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def read(self, size: int = -1) -> bytes:
        if size == -1:
            out = self._data[self._pos:]
            self._pos = len(self._data)
            return out

        out = self._data[self._pos:self._pos + size]
        self._pos += len(out)
        return out


class _FakeClientContext:
    def __init__(self, client) -> None:
        self._client = client
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self._client

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.exited = True
        return None


class _FakeSession:
    def __init__(self, client) -> None:
        self.client = client
        self.create_client_calls: list[dict[str, object]] = []
        self.context = _FakeClientContext(client)

    def create_client(self, *args, **kwargs):
        self.create_client_calls.append({'args': args, 'kwargs': kwargs})
        return self.context


def _config() -> S3Config:
    return S3Config(
        endpoint_url='https://s3.local',
        region='us-test-1',
        bucket='bucket',
        access_key_id='ak',
        secret_access_key='sk',
    )


@pytest.mark.asyncio
async def test_open_creates_path_style_client(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Client:
        async def head_object(self, **_kwargs) -> None:
            return None

    session = _FakeSession(_Client())
    monkeypatch.setattr(s3_module, 'get_session', lambda: session)

    storage = S3Client(_config())
    await storage.open()
    assert await storage.exists('x')
    await storage.close()

    assert len(session.create_client_calls) == 1
    call = session.create_client_calls[0]
    assert call['args'] == ('s3',)
    assert call['kwargs']['endpoint_url'] == 'https://s3.local'
    assert call['kwargs']['region_name'] == 'us-test-1'
    assert call['kwargs']['aws_access_key_id'] == 'ak'
    assert call['kwargs']['aws_secret_access_key'] == 'sk'
    assert call['kwargs']['config'].s3 == {'addressing_style': 'path'}
    assert session.context.entered is True
    assert session.context.exited is True


@pytest.mark.asyncio
async def test_async_with_opens_and_closes_client(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Client:
        async def head_object(self, **_kwargs) -> None:
            return None

    session = _FakeSession(_Client())
    monkeypatch.setattr(s3_module, 'get_session', lambda: session)

    async with S3Client(_config()) as storage:
        assert await storage.exists('x')

    assert session.context.entered is True
    assert session.context.exited is True


@pytest.mark.asyncio
async def test_operation_before_open_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _FakeSession(object())
    monkeypatch.setattr(s3_module, 'get_session', lambda: session)

    storage = S3Client(_config())

    with pytest.raises(RuntimeError) as exc:
        await storage.put_bytes('x', b'data')

    assert 'not open' in str(exc.value)
    assert 'await client.open()' in str(exc.value)
    assert 'async with S3Client(...)' in str(exc.value)


@pytest.mark.asyncio
async def test_get_bytes_missing_key_raises_object_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def get_object(self, **_kwargs):
            raise _client_error('NoSuchKey', 'GetObject')

    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(_Client()))

    storage = S3Client(_config())
    await storage.open()

    with pytest.raises(S3ObjectNotFoundError) as exc:
        await storage.get_bytes('missing.txt')

    await storage.close()
    assert exc.value.key == 'missing.txt'


@pytest.mark.asyncio
async def test_list_keys_collects_all_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Client:
        def __init__(self) -> None:
            self.calls = 0

        async def list_objects_v2(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    'Contents': [{'Key': 'a/1'}, {'Key': 'a/2'}],
                    'IsTruncated': True,
                    'NextContinuationToken': 't2',
                }
            return {
                'Contents': [{'Key': 'a/3'}],
                'IsTruncated': False,
            }

    client = _Client()
    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(client))

    storage = S3Client(_config())
    await storage.open()
    keys = await storage.list_keys('a/')
    await storage.close()

    assert keys == ['a/1', 'a/2', 'a/3']


@pytest.mark.asyncio
async def test_list_prefixes_without_prefix_lists_bucket_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def list_objects_v2(self, **kwargs):
            assert kwargs['Bucket'] == 'bucket'
            assert kwargs['Delimiter'] == '/'
            assert 'Prefix' not in kwargs
            return {
                'CommonPrefixes': [{'Prefix': 'a/'}, {'Prefix': 'b/'}],
                'IsTruncated': False,
            }

    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(_Client()))

    storage = S3Client(_config())
    await storage.open()
    prefixes = await storage.list_prefixes()
    await storage.close()

    assert prefixes == ['a/', 'b/']


@pytest.mark.asyncio
async def test_delete_prefix_batches_and_counts_deleted_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        def __init__(self) -> None:
            self.delete_batches: list[list[str]] = []
            self.list_calls = 0

        async def list_objects_v2(self, **_kwargs):
            self.list_calls += 1
            if self.list_calls == 1:
                contents = [{'Key': f'p/{i}'} for i in range(1000)]
                return {
                    'Contents': contents,
                    'IsTruncated': True,
                    'NextContinuationToken': 't2',
                }
            contents = [{'Key': f'p/{i}'} for i in range(1000, 1003)]
            return {
                'Contents': contents,
                'IsTruncated': False,
            }

        async def delete_objects(self, **kwargs):
            batch = [item['Key'] for item in kwargs['Delete']['Objects']]
            self.delete_batches.append(batch)
            return {'Deleted': [{'Key': key} for key in batch]}

    client = _Client()
    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(client))

    storage = S3Client(_config())
    await storage.open()
    deleted = await storage.delete_prefix('p/')
    await storage.close()

    assert deleted == 1003
    assert len(client.delete_batches) == 2
    assert len(client.delete_batches[0]) == 1000
    assert len(client.delete_batches[1]) == 3


@pytest.mark.asyncio
async def test_delete_prefix_raises_on_partial_delete_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def list_objects_v2(self, **_kwargs):
            return {
                'Contents': [{'Key': 'p/1'}, {'Key': 'p/2'}],
                'IsTruncated': False,
            }

        async def delete_objects(self, **_kwargs):
            return {
                'Deleted': [{'Key': 'p/1'}],
                'Errors': [{'Key': 'p/2', 'Code': 'AccessDenied'}],
            }

    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(_Client()))

    storage = S3Client(_config())
    await storage.open()

    with pytest.raises(RuntimeError) as exc:
        await storage.delete_prefix('p/')

    await storage.close()
    assert 'Failed to delete 1 objects' in str(exc.value)


@pytest.mark.asyncio
async def test_get_file_writes_data_and_refuses_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def get_object(self, **_kwargs):
            return {'Body': _FakeBody(b'hello')}

    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(_Client()))

    storage = S3Client(_config())
    await storage.open()

    target = tmp_path / 'nested' / 'file.txt'
    await storage.get_file('x', target)
    assert target.read_bytes() == b'hello'

    with pytest.raises(FileExistsError):
        await storage.get_file('x', target)

    await storage.close()


@pytest.mark.asyncio
async def test_get_file_overwrite_preserves_existing_file_on_download_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def get_object(self, **_kwargs):
            raise _client_error('NoSuchKey', 'GetObject')

    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(_Client()))

    storage = S3Client(_config())
    await storage.open()

    target = tmp_path / 'file.txt'
    target.write_bytes(b'old-data')

    with pytest.raises(S3ObjectNotFoundError):
        await storage.get_file('missing', target, overwrite=True)

    await storage.close()
    assert target.read_bytes() == b'old-data'


@pytest.mark.asyncio
async def test_put_and_get_stream_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Client:
        def __init__(self) -> None:
            self.body = None

        async def put_object(self, **kwargs):
            self.body = kwargs['Body']
            return {}

        async def get_object(self, **_kwargs):
            source = self.body
            if hasattr(source, 'read'):
                source.seek(0)
                data = source.read()
            else:
                data = source
            return {'Body': _FakeBody(data)}

    stream = io.BytesIO(b'payload')
    out = io.BytesIO()
    monkeypatch.setattr(s3_module, 'get_session', lambda: _FakeSession(_Client()))

    storage = S3Client(_config())
    await storage.open()
    await storage._put_stream('x', stream)
    written = await storage._get_stream('x', out)
    await storage.close()

    assert written == 7
    assert out.getvalue() == b'payload'


def test_join_normalizes_segments() -> None:
    assert S3Client.join('a/', '/b', 'c') == 'a/b/c'


def test_config_repr_hides_secrets() -> None:
    text = repr(_config())
    assert 'endpoint_url=' in text
    assert 'region=' in text
    assert 'bucket=' in text
    assert 'access_key_id=' not in text
    assert 'secret_access_key=' not in text
    assert 'ak' not in text
    assert 'sk' not in text
