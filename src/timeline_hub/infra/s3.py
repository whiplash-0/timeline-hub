import tempfile
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Iterable, Self

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import ClientError

type Key = str
type Prefix = str

_DELIMITER = '/'
_LIST_MAX_KEYS = 1000
_DELETE_MAX_OBJECTS = 1000
_NOT_FOUND_CODES = frozenset({'404', 'NoSuchKey', 'NotFound'})
_STREAM_READ_SIZE = 64 * 1024


class S3ObjectNotFoundError(Exception):
    """Raised when an S3 object key does not exist."""

    def __init__(self, key: Key) -> None:
        self.key = key
        super().__init__(f'Object not found: {key}')


class S3OperationError(Exception):
    """Base class for S3 operation failures."""

    def __init__(self, message: str, *, bucket: str) -> None:
        self.bucket = bucket
        super().__init__(message)


class S3PutObjectError(S3OperationError):
    """Raised when an S3 put operation fails."""

    def __init__(self, *, bucket: str, key: Key) -> None:
        self.key = key
        super().__init__(f'S3 put failed for key {key} in bucket {bucket}', bucket=bucket)


class S3GetObjectError(S3OperationError):
    """Raised when an S3 get operation fails."""

    def __init__(self, *, bucket: str, key: Key) -> None:
        self.key = key
        super().__init__(f'S3 get failed for key {key} in bucket {bucket}', bucket=bucket)


class S3DeleteObjectError(S3OperationError):
    """Raised when an S3 delete operation fails."""

    def __init__(self, *, bucket: str, key: Key) -> None:
        self.key = key
        super().__init__(f'S3 delete failed for key {key} in bucket {bucket}', bucket=bucket)


class S3ListObjectsError(S3OperationError):
    """Raised when an S3 list operation fails."""

    def __init__(self, *, bucket: str, prefix: Prefix | None) -> None:
        self.prefix = prefix
        super().__init__(f'S3 list failed for prefix {prefix} in bucket {bucket}', bucket=bucket)


class S3BatchDeleteError(S3OperationError):
    """Raised when an S3 batch delete operation fails."""

    def __init__(
        self,
        *,
        bucket: str,
        keys: list[Key],
        delete_errors: list[dict[str, Any]],
    ) -> None:
        self.keys = keys
        self.delete_errors = delete_errors
        super().__init__(
            (
                f'S3 batch delete failed for keys {keys} in bucket {bucket}: '
                f'backend reported delete errors {delete_errors}'
            ),
            bucket=bucket,
        )


class S3HeadObjectError(S3OperationError):
    """Raised when an S3 head operation fails."""

    def __init__(self, *, bucket: str, key: Key) -> None:
        self.key = key
        super().__init__(f'S3 head failed for key {key} in bucket {bucket}', bucket=bucket)


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


class S3ContentType(StrEnum):
    """Common content types for S3 objects."""

    JSON = 'application/json'  # Structured data (APIs, manifests)
    PARQUET = 'application/vnd.apache.parquet'  # Columnar analytics format
    OCTET_STREAM = 'application/octet-stream'  # Generic binary fallback
    ZIP = 'application/zip'  # Compressed archive

    MP4 = 'video/mp4'  # Standard MP4 video container
    WEBM = 'video/webm'  # Web-optimized video format

    OPUS = 'audio/ogg'  # Opus audio codec stored in Ogg container
    MP3 = 'audio/mpeg'  # Common compressed audio format
    WAV = 'audio/wav'  # Uncompressed audio format

    PNG = 'image/png'  # Lossless image format
    JPEG = 'image/jpeg'  # Compressed image format (photos)
    WEBP = 'image/webp'  # Modern efficient image format
    GIF = 'image/gif'  # Animated or simple images

    HTML = 'text/html'  # Web pages
    PLAIN = 'text/plain'  # Plain text
    CSV = 'text/csv'  # Tabular data


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

    async def put_bytes(
        self,
        key: Key,
        data: bytes,
        *,
        content_type: S3ContentType | str | None = None,
    ) -> None:
        """Store in-memory bytes as a single object."""
        client = self._require_client()
        kwargs: dict[str, object] = {
            'Bucket': self._config.bucket,
            'Key': key,
            'Body': data,
        }
        if content_type is not None:
            kwargs['ContentType'] = content_type
        try:
            await client.put_object(**kwargs)
        except Exception as error:
            raise S3PutObjectError(bucket=self._config.bucket, key=key) from error

    async def put_file(
        self,
        key: Key,
        path: Path,
        *,
        content_type: S3ContentType | str | None = None,
    ) -> None:
        """Store a local file."""
        with path.open('rb') as file:
            await self.put_stream(key, file, content_type=content_type)

    async def put_stream(
        self,
        key: Key,
        stream: BinaryIO,
        *,
        content_type: S3ContentType | str | None = None,
    ) -> None:
        """Store data from a binary stream as a single object."""
        client = self._require_client()
        kwargs: dict[str, object] = {
            'Bucket': self._config.bucket,
            'Key': key,
            'Body': stream,
        }
        if content_type is not None:
            kwargs['ContentType'] = content_type
        try:
            await client.put_object(**kwargs)
        except Exception as error:
            raise S3PutObjectError(bucket=self._config.bucket, key=key) from error

    async def get_bytes(self, key: Key) -> bytes:
        """Load an object into memory.

        Raises:
            S3ObjectNotFoundError: If the object key does not exist.
        """
        client = self._require_client()
        try:
            response = await client.get_object(Bucket=self._config.bucket, Key=key)
        except Exception as error:
            if self._is_not_found(error):
                raise S3ObjectNotFoundError(key) from error
            raise S3GetObjectError(bucket=self._config.bucket, key=key) from error

        async with response['Body'] as body:
            return await body.read()

    async def get_file(self, key: Key, path: Path, *, overwrite: bool = False) -> None:
        """Load an object into a local file path.

        Raises:
            FileExistsError: If the target path exists and overwrite is False.
            S3ObjectNotFoundError: If the object key does not exist.
        """
        if path.exists() and not overwrite:
            raise FileExistsError(path)

        path.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp_path = Path(tmp.name)

        try:
            with tmp_path.open('wb') as file:
                await self.get_stream(key, file)
            tmp_path.replace(path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    async def get_stream(self, key: Key, target: BinaryIO) -> int:
        """Stream an object into a writable binary stream.

        Returns:
            Number of bytes written to the stream.

        Raises:
            S3ObjectNotFoundError: If the object key does not exist.
        """
        client = self._require_client()
        try:
            response = await client.get_object(Bucket=self._config.bucket, Key=key)
        except Exception as error:
            if self._is_not_found(error):
                raise S3ObjectNotFoundError(key) from error
            raise S3GetObjectError(bucket=self._config.bucket, key=key) from error

        bytes_written = 0
        async with response['Body'] as body:
            while chunk := await body.read(_STREAM_READ_SIZE):
                chunk_view = memoryview(chunk)
                while chunk_view:
                    written = target.write(chunk_view)
                    if written is None:
                        raise RuntimeError('Writable binary stream returned None from write().')
                    if written == 0:
                        raise RuntimeError('Writable binary stream made no progress while writing.')
                    chunk_view = chunk_view[written:]
                    bytes_written += written

        return bytes_written

    async def exists(self, key: Key) -> bool:
        """Check whether an object key exists."""
        client = self._require_client()
        try:
            await client.head_object(Bucket=self._config.bucket, Key=key)
            return True
        except Exception as error:
            if self._is_not_found(error):
                return False
            raise S3HeadObjectError(bucket=self._config.bucket, key=key) from error

    async def list_keys(self, prefix: Prefix | None = None) -> list[Key]:
        """List all object keys under an optional prefix.

        Uses one S3 list request per page. Each request returns up to 1000 keys,
        so large prefixes may require multiple backend requests.

        `None` and ``''`` both mean no prefix.
        """
        client = self._require_client()
        prefix = prefix or None
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

            try:
                response = await client.list_objects_v2(**kwargs)
            except Exception as error:
                raise S3ListObjectsError(bucket=self._config.bucket, prefix=prefix) from error
            keys.extend(obj['Key'] for obj in response.get('Contents', []))

            if not response.get('IsTruncated'):
                break

            token = response.get('NextContinuationToken')

        return keys

    async def list_subprefixes(self, prefix: Prefix | None = None) -> list[Prefix]:
        """List immediate logical child prefixes under an optional parent prefix.

        `None` and ``''`` both refer to the bucket root. Non-empty prefixes are
        treated as logical parent prefixes, so `tracks` and `tracks/` are
        equivalent.

        This method returns only the immediate child prefixes (one level deep)
        using S3 delimiter-based grouping. Results correspond to logical
        "folders", not arbitrary key prefixes.

        Uses paginated S3 list requests, with up to 1000 entries per request.
        Large result sets may require multiple backend calls.
        """
        client = self._require_client()
        prefix = prefix or None
        prefixes: list[Prefix] = []
        token: str | None = None

        effective_prefix = prefix
        if effective_prefix and not effective_prefix.endswith(_DELIMITER):
            effective_prefix = effective_prefix + _DELIMITER

        while True:
            kwargs: dict[str, object] = {
                'Bucket': self._config.bucket,
                'MaxKeys': _LIST_MAX_KEYS,
                'Delimiter': _DELIMITER,
            }
            if effective_prefix is not None:
                kwargs['Prefix'] = effective_prefix
            if token is not None:
                kwargs['ContinuationToken'] = token

            try:
                response = await client.list_objects_v2(**kwargs)
            except Exception as error:
                raise S3ListObjectsError(bucket=self._config.bucket, prefix=effective_prefix) from error
            prefixes.extend(item['Prefix'] for item in response.get('CommonPrefixes', []))

            if not response.get('IsTruncated'):
                break

            token = response.get('NextContinuationToken')

        return prefixes

    async def delete_key(self, key: Key) -> None:
        """Delete a single object by exact key."""
        client = self._require_client()
        try:
            await client.delete_object(Bucket=self._config.bucket, Key=key)
        except Exception as error:
            raise S3DeleteObjectError(bucket=self._config.bucket, key=key) from error

    async def delete_keys(self, keys: Iterable[Key]) -> int:
        """Delete exact object keys and return the backend-reported deleted count.

        This deletes the provided exact keys, not a prefix. Keys are
        materialized once and deleted in batches automatically when the total
        exceeds the backend batch-delete limit.

        Raises:
            TypeError: If `keys` is a string.
            S3BatchDeleteError: If a delete request fails or reports delete errors.
        """
        if isinstance(keys, str):
            raise TypeError('delete_keys() expects Iterable[Key], got str; use delete_key() for a single key')

        self._require_client()

        key_list = list(keys)
        if not key_list:
            return 0

        deleted = 0
        for start in range(0, len(key_list), _DELETE_MAX_OBJECTS):
            batch = key_list[start : start + _DELETE_MAX_OBJECTS]
            deleted += await self._delete_batch(batch)

        return deleted

    async def delete_prefix(self, prefix: Prefix, *, allow_root: bool = False) -> int:
        """Delete all objects under prefix and return number of deleted keys.

        Uses paginated list requests plus batched delete requests. Listing uses
        one request per 1000 keys, and deletion uses one delete request per
        batch of up to 1000 keys.

        Args:
            prefix: Key prefix to delete under. Must be non-empty unless allow_root=True.
            allow_root: Explicit opt-in to allow deleting the entire bucket.

        Raises:
            ValueError: If prefix is empty and allow_root is False.
            S3ListObjectsError: If listing objects under the prefix fails.
            S3BatchDeleteError: If a delete request fails or reports delete errors.
        """
        client = self._require_client()
        if not prefix and not allow_root:
            raise ValueError('Refusing to delete entire bucket without allow_root=True')
        effective_prefix = prefix or None

        deleted = 0
        batch: list[Key] = []
        token: str | None = None

        while True:
            kwargs: dict[str, object] = {
                'Bucket': self._config.bucket,
                'MaxKeys': _LIST_MAX_KEYS,
            }
            if effective_prefix is not None:
                kwargs['Prefix'] = effective_prefix
            if token is not None:
                kwargs['ContinuationToken'] = token

            try:
                response = await client.list_objects_v2(**kwargs)
            except Exception as error:
                raise S3ListObjectsError(bucket=self._config.bucket, prefix=effective_prefix) from error
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

    @staticmethod
    def split(key: str) -> list[str]:
        """Split an S3 key into path-like segments.

        Empty segments are ignored, so leading, trailing, or repeated
        delimiters do not produce empty elements.
        """
        return [segment for segment in key.split(_DELIMITER) if segment]

    async def _delete_batch(self, keys: list[Key]) -> int:
        """Delete one exact-key batch and return backend-reported deleted count."""
        client = self._require_client()
        objects = [{'Key': key} for key in keys]
        try:
            response = await client.delete_objects(
                Bucket=self._config.bucket,
                Delete={'Objects': objects},
            )
        except Exception as error:
            raise S3BatchDeleteError(
                bucket=self._config.bucket,
                keys=list(keys),
                delete_errors=[{'exception': repr(error)}],
            ) from error

        errors = response.get('Errors', [])
        if errors:
            raise S3BatchDeleteError(
                bucket=self._config.bucket,
                keys=list(keys),
                delete_errors=errors,
            )

        return len(response.get('Deleted', []))

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError('S3 client is not open. Use `await client.open()` or `async with S3Client(...)`.')
        return self._client

    @staticmethod
    def _is_not_found(error: Exception) -> bool:
        if not isinstance(error, ClientError):
            return False
        return error.response.get('Error', {}).get('Code') in _NOT_FOUND_CODES
