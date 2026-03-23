import asyncio
import json
import os
import tempfile
from datetime import timedelta
from pathlib import Path


async def normalize_audio_loudness(
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
