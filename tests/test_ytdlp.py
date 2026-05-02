import asyncio
import re
from datetime import timedelta
from pathlib import Path

import pytest

from timeline_hub.infra import ytdlp as ytdlp_module


@pytest.mark.asyncio
async def test_download_audio_as_opus_builds_expected_command_and_returns_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    expected_bytes = b'OggS-opus-bytes'

    class _FakeProc:
        def __init__(self) -> None:
            self.returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            args = observed['args']
            output_template = Path(str(args[args.index('-o') + 1]))
            output_path = output_template.with_suffix('.opus')
            output_path.write_bytes(expected_bytes)
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['args'] = args
        observed['kwargs'] = kwargs
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ytdlp_module.download_audio_as_opus(
        '  https://example.com/watch?v=abc  ',
        timeout=timedelta(seconds=7),
    )

    assert result == expected_bytes
    args = observed['args']
    assert args[0] == 'yt-dlp'
    assert '-f' in args
    assert 'bestaudio[acodec=opus]/bestaudio' in args
    assert '--extract-audio' in args
    assert '--audio-format' in args
    assert 'opus' in args
    assert '--quiet' in args
    assert '--no-playlist' in args
    assert '--write-thumbnail' not in args
    assert '--convert-thumbnails' not in args
    assert '-o' in args
    assert args[-1] == 'https://example.com/watch?v=abc'


@pytest.mark.asyncio
async def test_download_audio_as_opus_and_cover_builds_expected_command_and_returns_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    expected_audio = b'OggS-opus-bytes'
    expected_cover = b'jpg-cover-bytes'

    class _FakeProc:
        def __init__(self) -> None:
            self.returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            args = observed['args']
            output_template = Path(str(args[args.index('-o') + 1]))
            output_template.with_suffix('.opus').write_bytes(expected_audio)
            output_template.with_suffix('.jpg').write_bytes(expected_cover)
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['args'] = args
        observed['kwargs'] = kwargs
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    audio, cover = await ytdlp_module.download_audio_as_opus_and_cover(
        'https://example.com/watch?v=abc',
        timeout=timedelta(seconds=7),
    )

    assert audio == expected_audio
    assert cover == expected_cover
    args = observed['args']
    assert '--write-thumbnail' in args
    assert '--convert-thumbnails' in args
    assert 'jpg' in args


@pytest.mark.asyncio
async def test_download_audio_as_opus_and_cover_raises_when_cover_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            args = observed['args']
            output_template = Path(str(args[args.index('-o') + 1]))
            output_template.with_suffix('.opus').write_bytes(b'OggS-opus-bytes')
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['args'] = args
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match='yt-dlp did not produce cover output'):
        await ytdlp_module.download_audio_as_opus_and_cover('https://example.com/watch?v=abc')


@pytest.mark.asyncio
async def test_download_audio_as_opus_rejects_non_string_url() -> None:
    with pytest.raises(ValueError, match='url must be a string'):
        await ytdlp_module.download_audio_as_opus(123)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_download_audio_as_opus_rejects_blank_url() -> None:
    with pytest.raises(ValueError, match='url must not be empty'):
        await ytdlp_module.download_audio_as_opus('   ')


@pytest.mark.asyncio
async def test_download_audio_as_opus_raises_runtime_error_on_non_zero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b'', b'failure details'

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match=re.escape('yt-dlp failed: failure details')):
        await ytdlp_module.download_audio_as_opus('https://example.com/watch?v=abc')


@pytest.mark.asyncio
async def test_download_audio_as_opus_kills_and_waits_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {'killed': False, 'waited': False}

    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b'', b''

        def kill(self) -> None:
            observed['killed'] = True

        async def wait(self) -> int:
            observed['waited'] = True
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    async def _fake_wait_for(awaitable: object, timeout: float) -> tuple[bytes, bytes]:
        close = getattr(awaitable, 'close', None)
        if callable(close):
            close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)
    monkeypatch.setattr(ytdlp_module.asyncio, 'wait_for', _fake_wait_for)

    with pytest.raises(asyncio.TimeoutError):
        await ytdlp_module.download_audio_as_opus('https://example.com/watch?v=abc')

    assert observed['killed'] is True
    assert observed['waited'] is True


@pytest.mark.asyncio
async def test_download_audio_as_opus_raises_when_no_opus_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match='yt-dlp did not produce opus output'):
        await ytdlp_module.download_audio_as_opus('https://example.com/watch?v=abc')


@pytest.mark.asyncio
async def test_download_audio_as_opus_raises_when_multiple_opus_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            args = observed['args']
            output_template = Path(str(args[args.index('-o') + 1]))
            output_template.with_name('audio1.opus').write_bytes(b'1')
            output_template.with_name('audio2.opus').write_bytes(b'2')
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['args'] = args
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match='yt-dlp produced multiple opus outputs'):
        await ytdlp_module.download_audio_as_opus('https://example.com/watch?v=abc')


@pytest.mark.asyncio
async def test_download_audio_as_opus_raises_when_output_is_not_ogg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            args = observed['args']
            output_template = Path(str(args[args.index('-o') + 1]))
            output_path = output_template.with_suffix('.opus')
            output_path.write_bytes(b'not-ogg-data')
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['args'] = args
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match='yt-dlp output is not a valid Ogg/Opus container'):
        await ytdlp_module.download_audio_as_opus('https://example.com/watch?v=abc')


@pytest.mark.asyncio
async def test_get_media_duration_returns_timedelta_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b'123.45\n', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['args'] = args
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    duration = await ytdlp_module.get_media_duration('https://example.com/watch?v=abc')

    assert duration == timedelta(seconds=123.45)
    args = observed['args']
    assert args[0] == 'yt-dlp'
    assert '--print' in args
    assert '%(duration)s' in args
    assert '--skip-download' in args
    assert '--no-playlist' in args


@pytest.mark.asyncio
async def test_get_media_duration_returns_none_on_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b'NA\n', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    duration = await ytdlp_module.get_media_duration('https://example.com/watch?v=abc')

    assert duration is None


@pytest.mark.asyncio
async def test_get_media_duration_raises_on_non_zero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeProc:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b'', b'probe error'

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match=re.escape('yt-dlp failed: probe error')):
        await ytdlp_module.get_media_duration('https://example.com/watch?v=abc')


@pytest.mark.asyncio
async def test_download_audio_as_opus_with_max_duration_uses_full_path_for_short_media(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {'clipped_called': False}

    async def _fake_get_media_duration(url: str, *, timeout: timedelta) -> timedelta | None:
        return timedelta(seconds=29)

    async def _fake_internal(
        url: str,
        *,
        download_cover: bool,
        timeout: timedelta,
    ) -> tuple[bytes, bytes | None]:
        return b'full', None

    async def _fake_clipped(
        url: str,
        *,
        max_duration: timedelta,
        timeout: timedelta,
    ) -> bytes:
        observed['clipped_called'] = True
        return b'clipped'

    monkeypatch.setattr(ytdlp_module, 'get_media_duration', _fake_get_media_duration)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_internal', _fake_internal)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_clipped', _fake_clipped)

    result = await ytdlp_module.download_audio_as_opus(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=30),
    )

    assert result == b'full'
    assert observed['clipped_called'] is False


@pytest.mark.asyncio
async def test_download_audio_as_opus_with_max_duration_uses_clipped_path_for_long_media(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {'full_called': False}

    async def _fake_get_media_duration(url: str, *, timeout: timedelta) -> timedelta | None:
        return timedelta(seconds=31)

    async def _fake_internal(
        url: str,
        *,
        download_cover: bool,
        timeout: timedelta,
    ) -> tuple[bytes, bytes | None]:
        observed['full_called'] = True
        return b'full', None

    async def _fake_clipped(
        url: str,
        *,
        max_duration: timedelta,
        timeout: timedelta,
    ) -> bytes:
        return b'clipped'

    monkeypatch.setattr(ytdlp_module, 'get_media_duration', _fake_get_media_duration)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_internal', _fake_internal)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_clipped', _fake_clipped)

    result = await ytdlp_module.download_audio_as_opus(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=30),
    )

    assert result == b'clipped'
    assert observed['full_called'] is False


@pytest.mark.asyncio
async def test_download_audio_as_opus_with_max_duration_uses_clipped_path_for_unknown_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {'full_called': False}

    async def _fake_get_media_duration(url: str, *, timeout: timedelta) -> timedelta | None:
        return None

    async def _fake_internal(
        url: str,
        *,
        download_cover: bool,
        timeout: timedelta,
    ) -> tuple[bytes, bytes | None]:
        observed['full_called'] = True
        return b'full', None

    async def _fake_clipped(
        url: str,
        *,
        max_duration: timedelta,
        timeout: timedelta,
    ) -> bytes:
        return b'clipped'

    monkeypatch.setattr(ytdlp_module, 'get_media_duration', _fake_get_media_duration)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_internal', _fake_internal)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_clipped', _fake_clipped)

    result = await ytdlp_module.download_audio_as_opus(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=30),
    )

    assert result == b'clipped'
    assert observed['full_called'] is False


@pytest.mark.asyncio
async def test_download_audio_as_opus_and_cover_with_max_duration_uses_full_path_for_short_media(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {'clipped_called': False}

    async def _fake_get_media_duration(url: str, *, timeout: timedelta) -> timedelta | None:
        return timedelta(seconds=29)

    async def _fake_internal(
        url: str,
        *,
        download_cover: bool,
        timeout: timedelta,
    ) -> tuple[bytes, bytes | None]:
        assert download_cover is True
        return b'full-audio', b'full-cover'

    async def _fake_clipped(
        url: str,
        *,
        max_duration: timedelta,
        timeout: timedelta,
    ) -> bytes:
        observed['clipped_called'] = True
        return b'clipped-audio'

    monkeypatch.setattr(ytdlp_module, 'get_media_duration', _fake_get_media_duration)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_internal', _fake_internal)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_clipped', _fake_clipped)

    audio, cover = await ytdlp_module.download_audio_as_opus_and_cover(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=30),
    )

    assert audio == b'full-audio'
    assert cover == b'full-cover'
    assert observed['clipped_called'] is False


@pytest.mark.asyncio
async def test_download_audio_as_opus_and_cover_with_max_duration_uses_clipped_audio_and_thumbnail_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {'full_called': False}

    async def _fake_get_media_duration(url: str, *, timeout: timedelta) -> timedelta | None:
        return timedelta(seconds=31)

    async def _fake_internal(
        url: str,
        *,
        download_cover: bool,
        timeout: timedelta,
    ) -> tuple[bytes, bytes | None]:
        observed['full_called'] = True
        return b'full-audio', b'full-cover'

    async def _fake_clipped(
        url: str,
        *,
        max_duration: timedelta,
        timeout: timedelta,
    ) -> bytes:
        return b'OggS-clipped-audio'

    class _FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            args = observed['thumb_args']
            output_template = Path(str(args[args.index('-o') + 1]))
            output_template.with_suffix('.jpg').write_bytes(b'thumb-jpg-bytes')
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        observed['thumb_args'] = args
        return _FakeProc()

    monkeypatch.setattr(ytdlp_module, 'get_media_duration', _fake_get_media_duration)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_internal', _fake_internal)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_clipped', _fake_clipped)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    audio, cover = await ytdlp_module.download_audio_as_opus_and_cover(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=30),
    )

    assert audio == b'OggS-clipped-audio'
    assert cover == b'thumb-jpg-bytes'
    assert observed['full_called'] is False
    thumb_args = observed['thumb_args']
    assert thumb_args[0] == 'yt-dlp'
    assert '--skip-download' in thumb_args
    assert '--write-thumbnail' in thumb_args
    assert '--convert-thumbnails' in thumb_args
    assert 'jpg' in thumb_args


@pytest.mark.asyncio
async def test_download_audio_functions_do_not_probe_duration_when_max_duration_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _unexpected_get_media_duration(url: str, *, timeout: timedelta) -> timedelta | None:
        raise AssertionError('duration probe should not be called when max_duration is None')

    async def _fake_internal(
        url: str,
        *,
        download_cover: bool,
        timeout: timedelta,
    ) -> tuple[bytes, bytes | None]:
        if download_cover:
            return b'full-audio', b'full-cover'
        return b'full-audio', None

    monkeypatch.setattr(ytdlp_module, 'get_media_duration', _unexpected_get_media_duration)
    monkeypatch.setattr(ytdlp_module, '_download_audio_as_opus_internal', _fake_internal)

    audio = await ytdlp_module.download_audio_as_opus('https://example.com/watch?v=abc')
    audio_with_cover, cover = await ytdlp_module.download_audio_as_opus_and_cover(
        'https://example.com/watch?v=abc',
    )

    assert audio == b'full-audio'
    assert audio_with_cover == b'full-audio'
    assert cover == b'full-cover'


@pytest.mark.asyncio
async def test_download_audio_functions_reject_non_positive_max_duration() -> None:
    with pytest.raises(ValueError, match='max_duration must be > 0'):
        await ytdlp_module.download_audio_as_opus(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(0),
        )
    with pytest.raises(ValueError, match='max_duration must be > 0'):
        await ytdlp_module.download_audio_as_opus(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(seconds=-1),
        )
    with pytest.raises(ValueError, match='max_duration must be > 0'):
        await ytdlp_module.download_audio_as_opus_and_cover(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(0),
        )
    with pytest.raises(ValueError, match='max_duration must be > 0'):
        await ytdlp_module.download_audio_as_opus_and_cover(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(seconds=-1),
        )


@pytest.mark.asyncio
async def test_download_audio_as_opus_clipped_builds_pipeline_and_returns_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, tuple[str, ...]] = {}

    class _FakeReader:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def read(self) -> bytes:
            return self._payload

    class _YtDlpProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdout = object()
            self.stderr = _FakeReader(b'')
            self.communicate_called = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_called = True
            raise AssertionError('ytdlp communicate must not be called in clipped mode')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    class _FfmpegProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdin = object()
            self.stdout = _FakeReader(b'OggS-clipped')
            self.stderr = _FakeReader(b'')
            self.communicate_called = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_called = True
            raise AssertionError('ffmpeg communicate must not be called in clipped mode')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_pipe_stream(source: object, destination: object) -> None:
        return None

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> object:
        if args[0] == 'yt-dlp':
            observed['yt-dlp'] = args
            return _YtDlpProc()
        observed['ffmpeg'] = args
        return _FfmpegProc()

    monkeypatch.setattr(ytdlp_module, '_pipe_stream', _fake_pipe_stream)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ytdlp_module._download_audio_as_opus_clipped(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=15),
        timeout=timedelta(seconds=10),
    )

    assert result == b'OggS-clipped'
    ytdlp_args = observed['yt-dlp']
    ffmpeg_args = observed['ffmpeg']
    assert '-o' in ytdlp_args
    assert ytdlp_args[ytdlp_args.index('-o') + 1] == '-'
    assert 'pipe:0' in ffmpeg_args
    assert '-t' in ffmpeg_args
    assert ffmpeg_args[ffmpeg_args.index('-t') + 1] == '15.0'
    assert '-c:a' in ffmpeg_args
    assert ffmpeg_args[ffmpeg_args.index('-c:a') + 1] == 'copy'
    assert '-f' in ffmpeg_args
    assert ffmpeg_args[ffmpeg_args.index('-f') + 1] == 'opus'
    assert ffmpeg_args[-1] == 'pipe:1'


@pytest.mark.asyncio
async def test_download_audio_as_opus_clipped_tolerates_ytdlp_broken_pipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeReader:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def read(self) -> bytes:
            return self._payload

    class _YtDlpProc:
        def __init__(self) -> None:
            self.returncode = 1
            self.stdout = object()
            self.stderr = _FakeReader(b'ERROR: Broken pipe')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    class _FfmpegProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdin = object()
            self.stdout = _FakeReader(b'OggS-clipped')
            self.stderr = _FakeReader(b'')
            self.communicate_called = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_called = True
            raise AssertionError('ffmpeg communicate must not be called in clipped mode')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_pipe_stream(source: object, destination: object) -> None:
        return None

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> object:
        if args[0] == 'yt-dlp':
            return _YtDlpProc()
        return _FfmpegProc()

    monkeypatch.setattr(ytdlp_module, '_pipe_stream', _fake_pipe_stream)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ytdlp_module._download_audio_as_opus_clipped(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=15),
        timeout=timedelta(seconds=10),
    )
    assert result == b'OggS-clipped'


@pytest.mark.asyncio
async def test_download_audio_as_opus_clipped_raises_when_ytdlp_fails_without_broken_pipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeReader:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def read(self) -> bytes:
            return self._payload

    class _YtDlpProc:
        def __init__(self) -> None:
            self.returncode = 2
            self.stdout = object()
            self.stderr = _FakeReader(b'network error')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    class _FfmpegProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdin = object()
            self.stdout = _FakeReader(b'OggS-clipped')
            self.stderr = _FakeReader(b'')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_pipe_stream(source: object, destination: object) -> None:
        return None

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> object:
        if args[0] == 'yt-dlp':
            return _YtDlpProc()
        return _FfmpegProc()

    monkeypatch.setattr(ytdlp_module, '_pipe_stream', _fake_pipe_stream)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match=re.escape('yt-dlp failed: network error')):
        await ytdlp_module._download_audio_as_opus_clipped(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(seconds=15),
            timeout=timedelta(seconds=10),
        )


@pytest.mark.asyncio
async def test_download_audio_as_opus_clipped_raises_on_ffmpeg_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeReader:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def read(self) -> bytes:
            return self._payload

    class _YtDlpProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdout = object()
            self.stderr = _FakeReader(b'')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    class _FfmpegProc:
        def __init__(self) -> None:
            self.returncode = 1
            self.stdin = object()
            self.stdout = _FakeReader(b'')
            self.stderr = _FakeReader(b'ffmpeg decode failure')
            self.communicate_called = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_called = True
            raise AssertionError('ffmpeg communicate must not be called in clipped mode')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_pipe_stream(source: object, destination: object) -> None:
        return None

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> object:
        if args[0] == 'yt-dlp':
            return _YtDlpProc()
        return _FfmpegProc()

    monkeypatch.setattr(ytdlp_module, '_pipe_stream', _fake_pipe_stream)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match=re.escape('ffmpeg failed: ffmpeg decode failure')):
        await ytdlp_module._download_audio_as_opus_clipped(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(seconds=15),
            timeout=timedelta(seconds=10),
        )


@pytest.mark.asyncio
async def test_download_audio_as_opus_clipped_never_calls_process_communicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeReader:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        async def read(self) -> bytes:
            return self._payload

    class _YtDlpProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdout = object()
            self.stderr = _FakeReader(b'')
            self.communicate_called = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_called = True
            raise AssertionError('unexpected ytdlp communicate call')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    class _FfmpegProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdin = object()
            self.stdout = _FakeReader(b'OggS-clipped')
            self.stderr = _FakeReader(b'')
            self.communicate_called = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_called = True
            raise AssertionError('unexpected ffmpeg communicate call')

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    ytdlp_proc = _YtDlpProc()
    ffmpeg_proc = _FfmpegProc()

    async def _fake_pipe_stream(source: object, destination: object) -> None:
        return None

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> object:
        if args[0] == 'yt-dlp':
            return ytdlp_proc
        return ffmpeg_proc

    monkeypatch.setattr(ytdlp_module, '_pipe_stream', _fake_pipe_stream)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ytdlp_module._download_audio_as_opus_clipped(
        'https://example.com/watch?v=abc',
        max_duration=timedelta(seconds=15),
        timeout=timedelta(seconds=10),
    )

    assert result == b'OggS-clipped'
    assert ytdlp_proc.communicate_called is False
    assert ffmpeg_proc.communicate_called is False


@pytest.mark.asyncio
async def test_download_audio_as_opus_clipped_timeout_kills_and_waits_for_both_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {
        'ytdlp_killed': False,
        'ffmpeg_killed': False,
        'ytdlp_waited': False,
        'ffmpeg_waited': False,
    }

    class _FakeReader:
        def __init__(self, payload: bytes = b'') -> None:
            self._payload = payload

        async def read(self) -> bytes:
            await asyncio.sleep(1)
            return self._payload

    class _YtDlpProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdout = object()
            self.stderr = _FakeReader()

        def kill(self) -> None:
            observed['ytdlp_killed'] = True

        async def wait(self) -> int:
            observed['ytdlp_waited'] = True
            return self.returncode

    class _FfmpegProc:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdin = object()
            self.stdout = _FakeReader()
            self.stderr = _FakeReader()

        def kill(self) -> None:
            observed['ffmpeg_killed'] = True

        async def wait(self) -> int:
            observed['ffmpeg_waited'] = True
            return self.returncode

    async def _fake_pipe_stream(source: object, destination: object) -> None:
        await asyncio.sleep(1)

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> object:
        if args[0] == 'yt-dlp':
            return _YtDlpProc()
        return _FfmpegProc()

    monkeypatch.setattr(ytdlp_module, '_pipe_stream', _fake_pipe_stream)
    monkeypatch.setattr(ytdlp_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(asyncio.TimeoutError):
        await ytdlp_module._download_audio_as_opus_clipped(
            'https://example.com/watch?v=abc',
            max_duration=timedelta(seconds=15),
            timeout=timedelta(milliseconds=1),
        )

    assert observed['ytdlp_killed'] is True
    assert observed['ffmpeg_killed'] is True
    assert observed['ytdlp_waited'] is True
    assert observed['ffmpeg_waited'] is True
