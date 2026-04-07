from datetime import timedelta
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
        bitrate=96,
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
        'asetrate=44100*0.75,aresample=48000',
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
            'volume=1.25,asetrate=48000*1.25,aresample=48000,aecho=1.0:0.95:50:0.4,alimiter=limit=0.95',
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
async def test_probe_audio_sample_rate_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match='audio_bytes must not be empty'):
        await ffmpeg_module.probe_audio_sample_rate(b'')
