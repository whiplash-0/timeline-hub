import math
import wave
from datetime import timedelta
from io import BytesIO
from pathlib import Path

import pytest

from timeline_hub.infra import ffmpeg as ffmpeg_module


@pytest.mark.asyncio
async def test_create_audio_variant_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match='audio_bytes must not be empty'):
        await ffmpeg_module.create_audio_variant(
            b'',
            speed=1.0,
            reverb=0.0,
            input_sample_rate=48_000,
        )


@pytest.mark.asyncio
async def test_create_audio_variant_rejects_invalid_input_sample_rate() -> None:
    with pytest.raises(ValueError, match='input_sample_rate must be >= 1'):
        await ffmpeg_module.create_audio_variant(
            b'source-audio',
            speed=1.0,
            reverb=0.0,
            input_sample_rate=0,
        )


@pytest.mark.asyncio
async def test_create_audio_variant_rejects_non_integer_mp3_quality() -> None:
    with pytest.raises(ValueError, match='mp3_quality must be an integer'):
        await ffmpeg_module.create_audio_variant(
            b'source-audio',
            speed=1.0,
            reverb=0.0,
            input_sample_rate=48_000,
            output_format='mp3',
            mp3_quality=True,
        )


@pytest.mark.asyncio
async def test_create_audio_variant_rejects_out_of_range_mp3_quality() -> None:
    with pytest.raises(ValueError, match='mp3_quality must be in 0..9'):
        await ffmpeg_module.create_audio_variant(
            b'source-audio',
            speed=1.0,
            reverb=0.0,
            input_sample_rate=48_000,
            output_format='mp3',
            mp3_quality=10,
        )


@pytest.mark.asyncio
async def test_create_audio_variant_builds_slowdown_filter_without_reverb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, tuple[str, ...] | timedelta] = {}

    async def _fake_run_ffmpeg(cmd: tuple[str, ...], timeout: timedelta) -> bytes:
        observed['cmd'] = cmd
        observed['run_timeout'] = timeout
        Path(cmd[-1]).write_bytes(b'variant-audio')
        return b''

    monkeypatch.setattr(ffmpeg_module, '_run_ffmpeg', _fake_run_ffmpeg)

    result = await ffmpeg_module.create_audio_variant(
        b'source-audio',
        speed=0.75,
        reverb=0.0,
        input_sample_rate=44_100,
        output_format='opus',
        opus_bitrate=96,
        timeout=timedelta(seconds=12),
    )

    assert result == b'variant-audio'
    assert observed['run_timeout'] == timedelta(seconds=12)
    assert observed['cmd'] == (
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
        observed['cmd'][10],
        '-vn',
        '-af',
        'asetrate=44100*0.75,aresample=48000:resampler=soxr:precision=28:cheby=1,'
        'equalizer=f=5000:t=q:w=1:g=1,equalizer=f=14000:t=q:w=1:g=-2',
        '-ar',
        '48000',
        '-c:a',
        'libopus',
        '-b:a',
        '96k',
        '-vbr',
        'on',
        '-compression_level',
        '10',
        observed['cmd'][-1],
    )


@pytest.mark.asyncio
async def test_create_audio_variant_builds_speedup_filter_with_reverb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_cmds: list[tuple[str, ...]] = []

    async def _fake_run_ffmpeg(cmd: tuple[str, ...], timeout: timedelta) -> bytes:
        observed_cmds.append(cmd)
        assert timeout == timedelta(seconds=5)
        Path(cmd[-1]).write_bytes(b'variant-audio')
        return b''

    monkeypatch.setattr(ffmpeg_module, '_run_ffmpeg', _fake_run_ffmpeg)

    result = await ffmpeg_module.create_audio_variant(
        b'source-audio',
        speed=1.25,
        reverb=0.4,
        input_sample_rate=48_000,
        output_format='opus',
        timeout=timedelta(seconds=5),
    )

    assert result == b'variant-audio'
    assert observed_cmds == [
        (
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
            observed_cmds[0][10],
            '-vn',
            '-af',
            'asetrate=48000*1.25,aresample=48000:resampler=soxr:precision=28:cheby=1,'
            'equalizer=f=5000:t=q:w=1:g=2,equalizer=f=14000:t=q:w=1:g=-2,'
            'volume=1.125,alimiter=limit=0.98,aecho=1.0:0.95:50:0.8',
            '-ar',
            '48000',
            '-c:a',
            'libopus',
            '-b:a',
            '160k',
            '-vbr',
            'on',
            '-compression_level',
            '10',
            observed_cmds[0][-1],
        )
    ]


@pytest.mark.asyncio
async def test_create_audio_variant_builds_mp3_output_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, tuple[str, ...] | timedelta] = {}

    async def _fake_run_ffmpeg(cmd: tuple[str, ...], timeout: timedelta) -> bytes:
        observed['cmd'] = cmd
        observed['run_timeout'] = timeout
        Path(cmd[-1]).write_bytes(b'variant-audio')
        return b''

    monkeypatch.setattr(ffmpeg_module, '_run_ffmpeg', _fake_run_ffmpeg)

    result = await ffmpeg_module.create_audio_variant(
        b'source-audio',
        speed=1.0,
        reverb=0.0,
        input_sample_rate=48_000,
        output_format='mp3',
        timeout=timedelta(seconds=7),
    )

    assert result == b'variant-audio'
    assert observed['run_timeout'] == timedelta(seconds=7)
    assert observed['cmd'] == (
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
        observed['cmd'][10],
        '-vn',
        '-af',
        'asetrate=48000*1.0,aresample=48000:resampler=soxr:precision=28:cheby=1,'
        'equalizer=f=5000:t=q:w=1:g=2,equalizer=f=14000:t=q:w=1:g=-2,'
        'volume=1.0,alimiter=limit=0.98',
        '-ar',
        '48000',
        '-c:a',
        'libmp3lame',
        '-q:a',
        '1',
        observed['cmd'][-1],
    )


@pytest.mark.asyncio
async def test_create_audio_variant_builds_mp3_output_args_with_custom_quality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, tuple[str, ...] | timedelta] = {}

    async def _fake_run_ffmpeg(cmd: tuple[str, ...], timeout: timedelta) -> bytes:
        observed['cmd'] = cmd
        observed['run_timeout'] = timeout
        Path(cmd[-1]).write_bytes(b'variant-audio')
        return b''

    monkeypatch.setattr(ffmpeg_module, '_run_ffmpeg', _fake_run_ffmpeg)

    result = await ffmpeg_module.create_audio_variant(
        b'source-audio',
        speed=1.0,
        reverb=0.0,
        input_sample_rate=48_000,
        output_format='mp3',
        mp3_quality=4,
        timeout=timedelta(seconds=7),
    )

    assert result == b'variant-audio'
    assert observed['run_timeout'] == timedelta(seconds=7)
    assert observed['cmd'] == (
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
        observed['cmd'][10],
        '-vn',
        '-af',
        'asetrate=48000*1.0,aresample=48000:resampler=soxr:precision=28:cheby=1,'
        'equalizer=f=5000:t=q:w=1:g=2,equalizer=f=14000:t=q:w=1:g=-2,'
        'volume=1.0,alimiter=limit=0.98',
        '-ar',
        '48000',
        '-c:a',
        'libmp3lame',
        '-q:a',
        '4',
        observed['cmd'][-1],
    )


@pytest.mark.asyncio
async def test_probe_audio_sample_rate_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match='audio_bytes must not be empty'):
        await ffmpeg_module.probe_audio_sample_rate(b'')


@pytest.mark.asyncio
async def test_to_opus_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match='audio_bytes must not be empty'):
        await ffmpeg_module.to_opus(b'')


@pytest.mark.asyncio
async def test_to_opus_rejects_invalid_bitrate() -> None:
    with pytest.raises(ValueError, match='bitrate must be >= 1'):
        await ffmpeg_module.to_opus(b'source-audio', bitrate=0)


@pytest.mark.asyncio
async def test_to_opus_converts_audio_to_non_empty_output() -> None:
    audio_bytes = _build_wav_bytes(sample_rate=44_100)

    opus_bytes = await ffmpeg_module.to_opus(audio_bytes)

    assert opus_bytes
    assert opus_bytes.startswith(b'OggS')


@pytest.mark.asyncio
async def test_to_opus_outputs_48khz_audio() -> None:
    audio_bytes = _build_wav_bytes(sample_rate=44_100)

    opus_bytes = await ffmpeg_module.to_opus(audio_bytes)

    assert await ffmpeg_module.probe_audio_sample_rate(opus_bytes) == 48_000


def _build_wav_bytes(*, sample_rate: int, duration_seconds: float = 0.1) -> bytes:
    frame_count = int(sample_rate * duration_seconds)
    amplitude = 12_000
    frequency_hz = 440.0
    frames = bytearray()

    for index in range(frame_count):
        sample = int(amplitude * math.sin(2 * math.pi * frequency_hz * (index / sample_rate)))
        frames.extend(sample.to_bytes(2, byteorder='little', signed=True))

    output = BytesIO()
    with wave.open(output, 'wb') as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(bytes(frames))

    return output.getvalue()
