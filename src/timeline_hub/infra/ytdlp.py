import asyncio
import tempfile
from datetime import timedelta
from pathlib import Path


async def download_audio_as_opus(
    url: str,
    *,
    max_duration: timedelta | None = None,
    timeout: timedelta = timedelta(minutes=3),
) -> bytes:
    """Download one URL audio track as Opus bytes using `yt-dlp`.

    Args:
        url: Source URL to download.
        max_duration: Optional maximum audio duration to return.
        timeout: Maximum time allowed for the `yt-dlp` subprocess run.

    Raises:
        ValueError: If `url` is invalid.
        RuntimeError: If `yt-dlp` fails or output validation fails.
    """
    if max_duration is None:
        audio, _ = await _download_audio_as_opus_internal(
            url,
            download_cover=False,
            timeout=timeout,
        )
        return audio

    _validate_max_duration(max_duration)
    duration = await get_media_duration(url, timeout=timedelta(seconds=30))
    if duration is not None and duration <= max_duration:
        audio, _ = await _download_audio_as_opus_internal(
            url,
            download_cover=False,
            timeout=timeout,
        )
        return audio

    return await _download_audio_as_opus_clipped(
        url,
        max_duration=max_duration,
        timeout=timeout,
    )


async def download_audio_as_opus_and_cover(
    url: str,
    *,
    max_duration: timedelta | None = None,
    timeout: timedelta = timedelta(minutes=3),
) -> tuple[bytes, bytes]:
    """Download one URL audio track as Opus bytes and cover as JPG bytes using `yt-dlp`.

    Args:
        url: Source URL to download.
        max_duration: Optional maximum audio duration to return.
        timeout: Maximum time allowed for the `yt-dlp` subprocess run.

    Raises:
        ValueError: If `url` is invalid.
        RuntimeError: If `yt-dlp` fails or output validation fails.
    """
    if max_duration is None:
        audio, cover = await _download_audio_as_opus_internal(
            url,
            download_cover=True,
            timeout=timeout,
        )
        if cover is None:
            raise RuntimeError('yt-dlp did not produce cover output')
        return audio, cover

    _validate_max_duration(max_duration)
    duration = await get_media_duration(url, timeout=timedelta(seconds=30))
    if duration is not None and duration <= max_duration:
        audio, cover = await _download_audio_as_opus_internal(
            url,
            download_cover=True,
            timeout=timeout,
        )
        if cover is None:
            raise RuntimeError('yt-dlp did not produce cover output')
        return audio, cover

    audio = await _download_audio_as_opus_clipped(
        url,
        max_duration=max_duration,
        timeout=timeout,
    )
    cover = await _download_cover_as_jpg(url, timeout=timeout)
    return audio, cover


async def get_media_duration(
    url: str,
    *,
    timeout: timedelta = timedelta(seconds=30),
) -> timedelta | None:
    normalized_url = _normalize_url(url)
    proc = await asyncio.create_subprocess_exec(
        'yt-dlp',
        '--print',
        '%(duration)s',
        '--skip-download',
        '--no-playlist',
        normalized_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout.total_seconds(),
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise

    if proc.returncode != 0:
        stderr_text = stderr.decode(errors='replace')
        raise RuntimeError(f'yt-dlp failed: {stderr_text}')

    duration_text = stdout.decode(errors='replace').strip()
    if not duration_text or duration_text in {'NA', 'None'}:
        return None
    try:
        duration_seconds = float(duration_text)
    except ValueError:
        return None
    if duration_seconds <= 0:
        return None
    return timedelta(seconds=duration_seconds)


def _normalize_url(url: str) -> str:
    if not isinstance(url, str):
        raise ValueError('url must be a string')

    normalized_url = url.strip()
    if not normalized_url:
        raise ValueError('url must not be empty')
    return normalized_url


def _validate_max_duration(max_duration: timedelta) -> None:
    if not isinstance(max_duration, timedelta):
        raise ValueError('max_duration must be a timedelta')
    if max_duration <= timedelta(0):
        raise ValueError('max_duration must be > 0')


async def _download_audio_as_opus_internal(
    url: str,
    *,
    download_cover: bool,
    timeout: timedelta,
) -> tuple[bytes, bytes | None]:
    normalized_url = _normalize_url(url)

    with tempfile.TemporaryDirectory() as temp_dir:
        output_template = Path(temp_dir) / 'audio.%(ext)s'
        args: list[str] = [
            'yt-dlp',
            '-f',
            'bestaudio[acodec=opus]/bestaudio',
            '--extract-audio',
            '--audio-format',
            'opus',
            '--quiet',
            '--no-playlist',
        ]
        if download_cover:
            args.extend(
                [
                    '--write-thumbnail',
                    '--convert-thumbnails',
                    'jpg',
                ]
            )
        args.extend(['-o', str(output_template), normalized_url])
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            _, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout.total_seconds(),
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise

        if proc.returncode != 0:
            stderr_text = stderr.decode(errors='replace')
            raise RuntimeError(f'yt-dlp failed: {stderr_text}')

        output_files = sorted(Path(temp_dir).glob('*.opus'))
        if not output_files:
            raise RuntimeError('yt-dlp did not produce opus output')
        if len(output_files) > 1:
            raise RuntimeError('yt-dlp produced multiple opus outputs')

        audio_bytes = output_files[0].read_bytes()
        if not audio_bytes.startswith(b'OggS'):
            raise RuntimeError('yt-dlp output is not a valid Ogg/Opus container')

        if not download_cover:
            return audio_bytes, None

        cover_files = sorted(Path(temp_dir).glob('*.jpg'))
        if not cover_files:
            return audio_bytes, None
        if len(cover_files) > 1:
            raise RuntimeError('yt-dlp produced multiple cover outputs')

        cover_bytes = cover_files[0].read_bytes()
        if not cover_bytes:
            raise RuntimeError('yt-dlp produced empty cover output')
        return audio_bytes, cover_bytes


async def _download_cover_as_jpg(
    url: str,
    *,
    timeout: timedelta,
) -> bytes:
    normalized_url = _normalize_url(url)
    with tempfile.TemporaryDirectory() as temp_dir:
        output_template = Path(temp_dir) / 'cover.%(ext)s'
        proc = await asyncio.create_subprocess_exec(
            'yt-dlp',
            '--skip-download',
            '--write-thumbnail',
            '--convert-thumbnails',
            'jpg',
            '--quiet',
            '--no-playlist',
            '-o',
            str(output_template),
            normalized_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            _, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout.total_seconds(),
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise

        if proc.returncode != 0:
            stderr_text = stderr.decode(errors='replace')
            raise RuntimeError(f'yt-dlp failed: {stderr_text}')

        cover_files = sorted(Path(temp_dir).glob('*.jpg'))
        if not cover_files:
            raise RuntimeError('yt-dlp did not produce cover output')
        if len(cover_files) > 1:
            raise RuntimeError('yt-dlp produced multiple cover outputs')

        cover_bytes = cover_files[0].read_bytes()
        if not cover_bytes:
            raise RuntimeError('yt-dlp produced empty cover output')
        return cover_bytes


async def _download_audio_as_opus_clipped(
    url: str,
    *,
    max_duration: timedelta,
    timeout: timedelta,
) -> bytes:
    normalized_url = _normalize_url(url)
    _validate_max_duration(max_duration)
    max_duration_seconds = str(max_duration.total_seconds())

    ytdlp_proc = await asyncio.create_subprocess_exec(
        'yt-dlp',
        '-f',
        'bestaudio[acodec=opus]/bestaudio',
        '--quiet',
        '--no-playlist',
        '-o',
        '-',
        normalized_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    ffmpeg_proc = await asyncio.create_subprocess_exec(
        'ffmpeg',
        '-hide_banner',
        '-loglevel',
        'error',
        '-nostats',
        '-nostdin',
        '-y',
        '-threads',
        '1',
        '-i',
        'pipe:0',
        '-t',
        max_duration_seconds,
        '-vn',
        '-c:a',
        'copy',
        '-f',
        'opus',
        'pipe:1',
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert ytdlp_proc.stdout is not None
    assert ytdlp_proc.stderr is not None
    assert ffmpeg_proc.stdin is not None
    assert ffmpeg_proc.stdout is not None
    assert ffmpeg_proc.stderr is not None
    # We prefer stream-copy for clipped mode and keep `-c:a copy`.
    # If source audio is not Opus-compatible for stream copy, ffmpeg fails explicitly.
    pipe_task = asyncio.create_task(_pipe_stream(ytdlp_proc.stdout, ffmpeg_proc.stdin))
    ffmpeg_stdout_task = asyncio.create_task(ffmpeg_proc.stdout.read())
    ffmpeg_stderr_task = asyncio.create_task(ffmpeg_proc.stderr.read())
    ytdlp_stderr_task = asyncio.create_task(ytdlp_proc.stderr.read())
    ffmpeg_wait_task = asyncio.create_task(ffmpeg_proc.wait())
    ytdlp_wait_task = asyncio.create_task(ytdlp_proc.wait())
    tasks = (
        pipe_task,
        ffmpeg_stdout_task,
        ffmpeg_stderr_task,
        ytdlp_stderr_task,
        ffmpeg_wait_task,
        ytdlp_wait_task,
    )
    try:
        async with asyncio.timeout(timeout.total_seconds()):
            await asyncio.gather(*tasks)
            ffmpeg_stdout = ffmpeg_stdout_task.result()
            ffmpeg_stderr = ffmpeg_stderr_task.result()
            ytdlp_stderr = await ytdlp_stderr_task
            ffmpeg_returncode = ffmpeg_wait_task.result()
            ytdlp_returncode = ytdlp_wait_task.result()
    except asyncio.TimeoutError:
        ytdlp_proc.kill()
        ffmpeg_proc.kill()
        await asyncio.gather(
            ytdlp_proc.wait(),
            ffmpeg_proc.wait(),
            return_exceptions=True,
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if ffmpeg_returncode != 0:
        stderr_text = ffmpeg_stderr.decode(errors='replace')
        raise RuntimeError(f'ffmpeg failed: {stderr_text}')

    if ytdlp_returncode != 0:
        ytdlp_stderr_text = ytdlp_stderr.decode(errors='replace')
        if 'broken pipe' not in ytdlp_stderr_text.lower():
            raise RuntimeError(f'yt-dlp failed: {ytdlp_stderr_text}')

    if not ffmpeg_stdout or not ffmpeg_stdout.startswith(b'OggS'):
        raise RuntimeError('yt-dlp output is not a valid Ogg/Opus container')
    return ffmpeg_stdout


async def _pipe_stream(
    source: asyncio.StreamReader,
    destination: asyncio.StreamWriter,
) -> None:
    try:
        while True:
            chunk = await source.read(65536)
            if not chunk:
                break
            destination.write(chunk)
            await destination.drain()
    except BrokenPipeError, ConnectionResetError:
        return
    finally:
        destination.close()
        await destination.wait_closed()
