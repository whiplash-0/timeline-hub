import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Self

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

type Key = str
type Prefix = str

_DELIMITER = '/'
_LIST_MAX_KEYS = 1000
_DELETE_MAX_OBJECTS = 1000
_NOT_FOUND_CODES = frozenset({'404', 'NoSuchKey', 'NotFound'})


class S3ObjectNotFoundError(Exception):
    """Raised when an S3 object key does not exist."""

    def __init__(self, key: Key) -> None:
        self.key = key
        super().__init__(f'Object not found: {key}')


@dataclass(frozen=True, slots=True)
class S3Config:
    """S3-compatible storage configuration."""

    endpoint_url: str
    region: str
    bucket: str
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.endpoint_url.startswith(('http://', 'https://')):
            raise ValueError('S3 endpoint_url must include scheme, e.g. https://...')


class S3Client:
    """Async client for generic S3-compatible object storage backends.

    This client targets providers that expose an S3-compatible API, rather than
    AWS S3 specifically. It assumes standard S3 operations such as put/get,
    paginated listing, head-based existence checks, and batched deletes.

    The client is bound to a single bucket and must be opened with `await open()`
    before use, or managed via `async with`.
    """

    def __init__(self, config: S3Config) -> None:
        """Initialize the client without opening the underlying S3 session."""
        self._config = config
        self._session = get_session()
        self._client_cm: Any | None = None
        self._client: Any | None = None

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def open(self) -> None:
        """Open the underlying S3 client. Safe to call multiple times."""
        if self._client is not None:
            return

        client_cm = self._session.create_client(
            's3',
            endpoint_url=self._config.endpoint_url,
            region_name=self._config.region,
            aws_access_key_id=self._config.access_key_id,
            aws_secret_access_key=self._config.secret_access_key,
            config=AioConfig(
                signature_version='s3v4',
                s3={'addressing_style': 'path'},  # type: ignore[arg-type]
            ),
        )

        try:
            client = await client_cm.__aenter__()
        except Exception:
            self._client_cm = None
            self._client = None
            raise

        self._client_cm = client_cm
        self._client = client

    async def close(self) -> None:
        """Close the underlying S3 client. Safe to call multiple times."""
        if self._client_cm is None:
            return

        await self._client_cm.__aexit__(None, None, None)
        self._client_cm = None
        self._client = None

    async def put_bytes(self, key: Key, data: bytes, *, content_type: str | None = None) -> None:
        """Store in-memory bytes as a single object."""
        kwargs: dict[str, object] = {
            'Bucket': self._config.bucket,
            'Key': key,
            'Body': data,
        }
        if content_type is not None:
            kwargs['ContentType'] = content_type
        await self._require_client().put_object(**kwargs)

    async def get_bytes(self, key: Key) -> bytes:
        """Load an object into memory.

        Raises:
            ObjectNotFoundError: If the object key does not exist.
        """
        try:
            response = await self._require_client().get_object(Bucket=self._config.bucket, Key=key)
        except ClientError as e:
            self._raise_if_not_found(e, key)
            raise

        async with response['Body'] as stream:
            return await stream.read()

    async def put_file(self, key: Key, path: Path) -> None:
        """Store a local file."""
        with path.open('rb') as file:
            await self._put_stream(key, file)

    async def get_file(self, key: Key, path: Path, *, overwrite: bool = False) -> None:
        """Load an object into a local file path.

        Raises:
            FileExistsError: If the target path exists and overwrite is False.
            ObjectNotFoundError: If the object key does not exist.
        """
        if path.exists() and not overwrite:
            raise FileExistsError(path)

        path.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            with tmp_path.open('wb') as file:
                await self._get_stream(key, file)
            tmp_path.replace(path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    async def exists(self, key: Key) -> bool:
        """Check whether an object key exists."""
        try:
            await self._require_client().head_object(Bucket=self._config.bucket, Key=key)
            return True
        except ClientError as e:
            if self._is_not_found(e):
                return False
            raise

    async def list_keys(self, prefix: Prefix | None = None) -> list[Key]:
        """List all object keys under an optional prefix.

        Uses one S3 list request per page. Each request returns up to 1000 keys,
        so large prefixes may require multiple backend requests.
        """
        keys: list[Key] = []
        token: str | None = None

        while True:
            kwargs: dict[str, object] = {
                'Bucket': self._config.bucket,
                'MaxKeys': _LIST_MAX_KEYS,
            }
            if prefix is not None:
                kwargs['Prefix'] = prefix
            if token is not None:
                kwargs['ContinuationToken'] = token

            response = await self._require_client().list_objects_v2(**kwargs)
            keys.extend(obj['Key'] for obj in response.get('Contents', []))

            if not response.get('IsTruncated'):
                break
            token = response.get('NextContinuationToken')

        return keys

    async def list_prefixes(self, prefix: Prefix | None = None) -> list[Prefix]:
        """List immediate subprefixes under an optional prefix.

        Uses one S3 list request per page. Each request returns up to 1000
        entries, so large results may require multiple backend requests.
        """
        prefixes: list[Prefix] = []
        token: str | None = None

        while True:
            kwargs: dict[str, object] = {
                'Bucket': self._config.bucket,
                'MaxKeys': _LIST_MAX_KEYS,
                'Delimiter': _DELIMITER,
            }
            if prefix is not None:
                kwargs['Prefix'] = prefix
            if token is not None:
                kwargs['ContinuationToken'] = token

            response = await self._require_client().list_objects_v2(**kwargs)
            prefixes.extend(item['Prefix'] for item in response.get('CommonPrefixes', []))

            if not response.get('IsTruncated'):
                break
            token = response.get('NextContinuationToken')

        return prefixes

    async def delete_key(self, key: Key) -> None:
        """Delete a single object by exact key."""
        await self._require_client().delete_object(Bucket=self._config.bucket, Key=key)

    async def delete_prefix(self, prefix: Prefix) -> int:
        """Delete all objects under prefix and return number of deleted keys.

        Uses paginated list requests plus batched delete requests. Listing uses
        one request per 1000 keys, and deletion uses one delete request per
        batch of up to 1000 keys.

        Raises:
            RuntimeError: If the backend reports partial delete failures.
        """
        deleted = 0
        batch: list[Key] = []
        token: str | None = None

        while True:
            kwargs: dict[str, object] = {
                'Bucket': self._config.bucket,
                'MaxKeys': _LIST_MAX_KEYS,
                'Prefix': prefix,
            }
            if token is not None:
                kwargs['ContinuationToken'] = token

            response = await self._require_client().list_objects_v2(**kwargs)
            contents = response.get('Contents', [])
            for obj in contents:
                batch.append(obj['Key'])
                if len(batch) >= _DELETE_MAX_OBJECTS:
                    deleted += await self._delete_batch(batch)
                    batch.clear()

            if not response.get('IsTruncated'):
                break
            token = response.get('NextContinuationToken')

        if batch:
            deleted += await self._delete_batch(batch)

        return deleted

    @staticmethod
    def join(*segments: str) -> str:
        """Join path-like segments with the S3 key delimiter."""
        return _DELIMITER.join(segment.strip(_DELIMITER) for segment in segments if segment)

    async def _put_stream(self, key: Key, stream: BinaryIO, *, content_type: str | None = None) -> None:
        kwargs: dict[str, object] = {
            'Bucket': self._config.bucket,
            'Key': key,
            'Body': stream,
        }
        if content_type is not None:
            kwargs['ContentType'] = content_type
        await self._require_client().put_object(**kwargs)

    async def _get_stream(self, key: Key, stream: BinaryIO) -> int:
        try:
            response = await self._require_client().get_object(Bucket=self._config.bucket, Key=key)
        except ClientError as e:
            self._raise_if_not_found(e, key)
            raise

        bytes_written = 0
        async with response['Body'] as body:
            while chunk := await body.read(64 * 1024):
                bytes_written += len(chunk)
                stream.write(chunk)
        return bytes_written

    async def _delete_batch(self, keys: list[Key]) -> int:
        objects = [{'Key': key} for key in keys]
        response = await self._require_client().delete_objects(
            Bucket=self._config.bucket,
            Delete={'Objects': objects},
        )

        errors = response.get('Errors', [])
        if errors:
            raise RuntimeError(f'Failed to delete {len(errors)} objects: {errors}')

        return len(response.get('Deleted', []))

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError('S3 client is not open. Use `await client.open()` or `async with S3Client(...)`.')
        return self._client

    @staticmethod
    def _is_not_found(error: ClientError) -> bool:
        return error.response.get('Error', {}).get('Code') in _NOT_FOUND_CODES

    @staticmethod
    def _raise_if_not_found(error: ClientError, key: Key) -> None:
        if S3Client._is_not_found(error):
            raise S3ObjectNotFoundError(key) from error
