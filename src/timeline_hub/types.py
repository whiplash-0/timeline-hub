from dataclasses import dataclass
from enum import StrEnum
from typing import Self

type ChatId = int
type UserId = int


class InvalidExtensionError(ValueError):
    """Raised when an unsupported or malformed file extension is provided."""


class Extension(StrEnum):
    """Supported file extensions for typed storage-boundary payloads."""

    MP4 = 'mp4'
    MP3 = 'mp3'
    OPUS = 'opus'
    JPG = 'jpg'

    @property
    def suffix(self) -> str:
        return f'.{self.value}'

    @classmethod
    def from_string(cls, value: str) -> Self:
        """Normalize a supported extension string into its canonical enum."""
        if not isinstance(value, str):
            raise InvalidExtensionError('extension value must be a string')

        normalized = value.lstrip('.')
        try:
            return cls(normalized.lower())
        except ValueError as error:
            raise InvalidExtensionError(f'Unsupported extension: {value}') from error

    @classmethod
    def from_filename(cls, filename: str) -> Self:
        """Extract and normalize the final extension segment from a filename."""
        if not isinstance(filename, str):
            raise InvalidExtensionError('filename must be a string')
        if not filename:
            raise InvalidExtensionError('filename must not be empty')

        stem, dot, suffix = filename.rpartition('.')
        if not dot or not suffix:
            raise InvalidExtensionError(f'Unsupported extension: {filename}')

        return cls.from_string(suffix)


@dataclass(frozen=True, slots=True)
class FileBytes:
    """Binary payload paired with its required extension contract."""

    data: bytes
    extension: Extension

    def __post_init__(self) -> None:
        """Validate explicit media payload invariants for all construction paths."""
        if not isinstance(self.data, bytes):
            raise ValueError('FileBytes.data must be bytes')
        if not self.data:
            raise ValueError('FileBytes.data must not be empty')
        if not isinstance(self.extension, Extension):
            raise ValueError('FileBytes.extension must be an Extension')
