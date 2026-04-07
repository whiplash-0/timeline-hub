import asyncio
import hashlib
import json
import math
import os
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any

_HASH_READ_SIZE = 64 * 1024


async def create_audio_variant(
    audio_bytes: bytes,
    *,
    speed: float,
    reverb: float,
    input_sample_rate: int = 48_000,
    bitrate: int = 160,
    timeout: timedelta = timedelta(seconds=30),
) -> bytes:
    """Return an Opus audio variant generated from source audio bytes.

    The input is treated as an ffmpeg-readable audio file. Typical caller
    inputs include formats such as `.opus`, `.mp3`, `.wav`, `.m4a`, or other
    common audio formats supported by ffmpeg. The output is always encoded as
    Opus in an Ogg container.

    Variant generation preserves the current project behavior:
    - for `speed < 1`, apply slowed playback by changing sample rate and
      resampling back to 48 kHz
    - for `speed >= 1`, apply volume boost, sped-up playback, and a limiter
    - if `reverb > 0`, apply echo reverb after resampling
    - if `reverb == 0`, no reverb is applied

    Args:
        audio_bytes: Source audio bytes in an ffmpeg-readable audio format.
        speed: Playback speed multiplier. Must be > 0.
        reverb: Reverb intensity in the closed range 0..1.
        input_sample_rate: Source audio sample rate in Hz.
        bitrate: Target Opus bitrate in kbps.
        timeout: Maximum time allowed for the ffmpeg subprocess run.

    Raises:
        ValueError: If parameters are invalid.
        RuntimeError: If ffmpeg fails.
    """
    if not audio_bytes:
        raise ValueError('audio_bytes must not be empty')

    if isinstance(speed, bool) or not isinstance(speed, int | float):
        raise ValueError('speed must be numeric')
    speed = float(speed)
    if not math.isfinite(speed):
        raise ValueError('speed must be finite')
    if speed <= 0:
        raise ValueError('speed must be > 0')

    if isinstance(reverb, bool) or not isinstance(reverb, int | float):
        raise ValueError('reverb must be numeric')
    reverb = float(reverb)
    if not math.isfinite(reverb):
        raise ValueError('reverb must be finite')
    if reverb < 0 or reverb > 1:
        raise ValueError('reverb must be in 0..1')

    if isinstance(input_sample_rate, bool) or not isinstance(input_sample_rate, int):
        raise ValueError('input_sample_rate must be an integer')
    if input_sample_rate < 1:
        raise ValueError('input_sample_rate must be >= 1')

    if isinstance(bitrate, bool) or not isinstance(bitrate, int):
        raise ValueError('bitrate must be an integer')
    if bitrate < 1:
        raise ValueError('bitrate must be >= 1')

    input_fd, input_name = tempfile.mkstemp(suffix='.audio')
    output_fd, output_name = tempfile.mkstemp(suffix='.opus')
    os.close(input_fd)
    os.close(output_fd)

    input_path = Path(input_name)
    output_path = Path(output_name)

    try:
        input_path.write_bytes(audio_bytes)

        output_sample_rate = 48_000

        if speed < 1.0:
            filter_parts = [
                f'asetrate={input_sample_rate}*{speed}',
                f'aresample={output_sample_rate}',
            ]
        else:
            filter_parts = [
                f'volume={speed}',
                f'asetrate={input_sample_rate}*{speed}',
                f'aresample={output_sample_rate}',
            ]

        if reverb > 0:
            filter_parts.append(f'aecho=1.0:0.95:50:{reverb}')

        if speed >= 1.0:
            filter_parts.append('alimiter=limit=0.95')

        audio_filter = ','.join(filter_parts)

        cmd = (
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
            str(input_path),
            '-vn',
            '-af',
            audio_filter,
            '-ar',
            str(output_sample_rate),
            '-c:a',
            'libopus',
            '-b:a',
            f'{bitrate}k',
            '-vbr',
            'on',
            '-compression_level',
            '10',
            str(output_path),
        )
        await _run_ffmpeg(cmd, timeout)

        return output_path.read_bytes()

    finally:
        input_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)


async def probe_audio_sample_rate(
    audio_bytes: bytes,
    *,
    timeout: timedelta = timedelta(seconds=30),
) -> int:
    """Return the sample rate of source audio bytes in Hz.

    The input is treated as an ffmpeg-readable audio file. Typical caller
    inputs include formats such as `.opus`, `.mp3`, `.wav`, `.m4a`, or other
    common audio formats supported by ffprobe.

    Args:
        audio_bytes: Source audio bytes in an ffprobe-readable audio format.
        timeout: Maximum time allowed for the ffprobe subprocess run.

    Raises:
        ValueError: If `audio_bytes` is empty.
        RuntimeError: If ffprobe fails or the sample rate cannot be parsed.
    """
    if not audio_bytes:
        raise ValueError('audio_bytes must not be empty')

    input_fd, input_name = tempfile.mkstemp(suffix='.audio')
    os.close(input_fd)
    input_path = Path(input_name)

    try:
        input_path.write_bytes(audio_bytes)

        proc = await asyncio.create_subprocess_exec(
            'ffprobe',
            '-v',
            'error',
            '-select_streams',
            'a:0',
            '-show_entries',
            'stream=sample_rate',
            '-of',
            'default=nokey=1:noprint_wrappers=1',
            str(input_path),
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
            raise RuntimeError(f'ffprobe failed: {stderr.decode(errors="replace")}')

        sample_rate_text = stdout.decode().strip()
        try:
            sample_rate = int(sample_rate_text)
        except ValueError as error:
            raise RuntimeError(f'Failed to parse sample rate: {sample_rate_text!r}') from error

        if sample_rate < 1:
            raise RuntimeError(f'Invalid probed sample rate: {sample_rate}')

        return sample_rate

    finally:
        input_path.unlink(missing_ok=True)


async def normalize_video_audio_loudness(
    video_bytes: bytes,
    *,
    loudness: float = -14,
    bitrate: int = 128,
    timeout: timedelta = timedelta(seconds=30),
) -> bytes:
    """Normalize video audio loudness with 2-pass `loudnorm`.

    The original video stream is copied unchanged, while the audio stream is
    normalized and re-encoded.

    Temporary files are used instead of piping MP4 bytes through ffmpeg
    stdin/stdout because MP4 muxing requires a seekable output.

    Args:
        video_bytes: Original MP4 video bytes.
        loudness: Target integrated loudness in LUFS.
        bitrate: Target audio bitrate in kbps for the re-encoded audio stream.
        timeout: Maximum time allowed for each ffmpeg subprocess run.
    """
    input_fd, input_name = tempfile.mkstemp(suffix='.mp4')
    output_fd, output_name = tempfile.mkstemp(suffix='.mp4')
    os.close(input_fd)
    os.close(output_fd)

    input_path = Path(input_name)
    output_path = Path(output_name)

    try:
        input_path.write_bytes(video_bytes)

        analysis_cmd = (
            'ffmpeg',
            '-hide_banner',
            '-loglevel',
            'info',
            '-nostats',
            '-nostdin',
            '-y',
            '-threads',
            '1',
            '-i',
            str(input_path),
            '-vn',
            '-af',
            f'loudnorm=I={loudness}:TP=-1.5:LRA=7:print_format=json',
            '-f',
            'null',
            '-',
        )
        analysis_stderr = await _run_ffmpeg(analysis_cmd, timeout)

        analysis_text = analysis_stderr.decode(errors='replace')
        json_start = analysis_text.rfind('{')
        json_end = analysis_text.rfind('}')
        if json_start == -1 or json_end == -1 or json_end < json_start:
            raise RuntimeError(f'ffmpeg analysis output did not contain loudnorm JSON: {analysis_text}')
        stats = json.loads(analysis_text[json_start : json_end + 1])

        normalize_filter = (
            f'loudnorm=I={loudness}:TP=-1.5:LRA=7:'
            f'measured_I={stats["input_i"]}:'
            f'measured_TP={stats["input_tp"]}:'
            f'measured_LRA={stats["input_lra"]}:'
            f'measured_thresh={stats["input_thresh"]}:'
            f'offset={stats["target_offset"]}:'
            'linear=true,'
            'alimiter=limit=-1.5dB'
        )
        normalize_cmd = (
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
            str(input_path),
            '-c:v',
            'copy',
            '-af',
            normalize_filter,
            '-c:a',
            'aac',
            '-b:a',
            f'{bitrate}k',
            str(output_path),
        )
        await _run_ffmpeg(normalize_cmd, timeout)

        return output_path.read_bytes()

    finally:
        input_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)


async def hash_video_content(
    video_bytes: bytes,
    *,
    timeout: timedelta = timedelta(seconds=30),
) -> str:
    """Return a stable SHA-256 hash of the primary video stream content.

    The hash is computed from ffmpeg's copied first video stream, excluding
    audio, subtitles, and data streams.

    Args:
        video_bytes: Original MP4 video bytes.
        timeout: Maximum time allowed for the ffmpeg subprocess run.
    """
    input_fd, input_name = tempfile.mkstemp(suffix='.mp4')
    os.close(input_fd)
    input_path = Path(input_name)

    try:
        input_path.write_bytes(video_bytes)

        cmd = (
            'ffmpeg',
            '-hide_banner',
            '-loglevel',
            'error',
            '-nostats',
            '-nostdin',
            '-threads',
            '1',
            '-i',
            str(input_path),
            '-map',
            '0:v:0',
            '-c:v',
            'copy',
            '-an',
            '-sn',
            '-dn',
            '-bsf:v',
            'h264_mp4toannexb',
            '-f',
            'h264',
            'pipe:1',
        )
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        if proc.stdout is None or proc.stderr is None:
            raise RuntimeError('ffmpeg subprocess did not expose stdout/stderr pipes')

        hasher = hashlib.sha256()
        try:
            _, stderr, returncode = await asyncio.wait_for(
                asyncio.gather(
                    _hash_stream(proc.stdout, hasher),
                    proc.stderr.read(),
                    proc.wait(),
                ),
                timeout=timeout.total_seconds(),
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise

        if returncode != 0:
            raise RuntimeError(f'ffmpeg failed while hashing clip: {stderr.decode(errors="replace")}')

        return hasher.hexdigest()
    finally:
        input_path.unlink(missing_ok=True)


async def _run_ffmpeg(cmd: tuple[str, ...], timeout: timedelta) -> bytes:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
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
        raise RuntimeError(f'ffmpeg failed: {stderr_text}')

    return stderr


async def _hash_stream(stream: asyncio.StreamReader, hasher: Any) -> None:
    while chunk := await stream.read(_HASH_READ_SIZE):
        hasher.update(chunk)
