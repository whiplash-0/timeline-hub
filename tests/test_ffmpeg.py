import math
import wave
from datetime import timedelta
from io import BytesIO

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
    observed: dict[str, object] = {}

    async def _fake_run_ffmpeg(
        cmd: tuple[str, ...],
        timeout: timedelta,
        *,
        stdin_bytes: bytes | None = None,
        capture: str = 'none',
    ) -> bytes:
        observed['cmd'] = cmd
        observed['run_timeout'] = timeout
        observed['stdin_bytes'] = stdin_bytes
        observed['capture'] = capture
        return b'variant-audio'

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
    assert observed['stdin_bytes'] == b'source-audio'
    assert observed['capture'] == 'stdout'
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
        'pipe:0',
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
        '-f',
        'opus',
        'pipe:1',
    )


@pytest.mark.asyncio
async def test_create_audio_variant_builds_speedup_filter_with_reverb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_cmds: list[tuple[str, ...]] = []

    async def _fake_run_ffmpeg(
        cmd: tuple[str, ...],
        timeout: timedelta,
        *,
        stdin_bytes: bytes | None = None,
        capture: str = 'none',
    ) -> bytes:
        observed_cmds.append(cmd)
        assert timeout == timedelta(seconds=5)
        assert stdin_bytes == b'source-audio'
        assert capture == 'stdout'
        return b'variant-audio'

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
            'pipe:0',
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
            '-f',
            'opus',
            'pipe:1',
        )
    ]


@pytest.mark.asyncio
async def test_create_audio_variant_builds_mp3_output_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    async def _fake_run_ffmpeg(
        cmd: tuple[str, ...],
        timeout: timedelta,
        *,
        stdin_bytes: bytes | None = None,
        capture: str = 'none',
    ) -> bytes:
        observed['cmd'] = cmd
        observed['run_timeout'] = timeout
        observed['stdin_bytes'] = stdin_bytes
        observed['capture'] = capture
        return b'variant-audio'

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
    assert observed['stdin_bytes'] == b'source-audio'
    assert observed['capture'] == 'stdout'
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
        'pipe:0',
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
        '-f',
        'mp3',
        'pipe:1',
    )


@pytest.mark.asyncio
async def test_create_audio_variant_builds_mp3_output_args_with_custom_quality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    async def _fake_run_ffmpeg(
        cmd: tuple[str, ...],
        timeout: timedelta,
        *,
        stdin_bytes: bytes | None = None,
        capture: str = 'none',
    ) -> bytes:
        observed['cmd'] = cmd
        observed['run_timeout'] = timeout
        observed['stdin_bytes'] = stdin_bytes
        observed['capture'] = capture
        return b'variant-audio'

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
    assert observed['stdin_bytes'] == b'source-audio'
    assert observed['capture'] == 'stdout'
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
        'pipe:0',
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
        '-f',
        'mp3',
        'pipe:1',
    )


@pytest.mark.asyncio
async def test_create_audio_variant_pipe_execution_returns_non_empty_output() -> None:
    audio_bytes = _build_wav_bytes(sample_rate=44_100)

    variant_bytes = await ffmpeg_module.create_audio_variant(
        audio_bytes,
        speed=1.0,
        reverb=0.0,
        input_sample_rate=44_100,
        output_format='opus',
    )

    assert variant_bytes
    assert variant_bytes.startswith(b'OggS')


@pytest.mark.asyncio
async def test_create_audio_variant_pipe_failure_propagates_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_run_ffmpeg(
        cmd: tuple[str, ...],
        timeout: timedelta,
        *,
        stdin_bytes: bytes | None = None,
        capture: str = 'none',
    ) -> bytes:
        raise RuntimeError('ffmpeg failed: broken input')

    monkeypatch.setattr(ffmpeg_module, '_run_ffmpeg', _fake_run_ffmpeg)

    with pytest.raises(RuntimeError, match='ffmpeg failed: broken input'):
        await ffmpeg_module.create_audio_variant(
            b'source-audio',
            speed=1.0,
            reverb=0.0,
            input_sample_rate=48_000,
        )


@pytest.mark.asyncio
async def test_create_audio_variant_pipe_empty_output_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeProc:
        def __init__(self) -> None:
            self.returncode = 0

        async def communicate(self, input_data: bytes | None = None) -> tuple[bytes, bytes]:
            return b'', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ffmpeg_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    with pytest.raises(RuntimeError, match='ffmpeg produced empty stdout'):
        await ffmpeg_module.create_audio_variant(
            b'source-audio',
            speed=1.0,
            reverb=0.0,
            input_sample_rate=48_000,
        )


@pytest.mark.asyncio
async def test_run_ffmpeg_capture_none_returns_empty_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeProc:
        returncode = 0

        async def communicate(self, input_data: bytes | None = None) -> tuple[bytes, bytes]:
            return b'', b'info-on-stderr'

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ffmpeg_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ffmpeg_module._run_ffmpeg(('ffmpeg',), timedelta(seconds=1), capture='none')
    assert result == b''


@pytest.mark.asyncio
async def test_run_ffmpeg_capture_stdout_returns_stdout(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeProc:
        returncode = 0

        async def communicate(self, input_data: bytes | None = None) -> tuple[bytes, bytes]:
            return b'encoded', b''

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ffmpeg_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ffmpeg_module._run_ffmpeg(('ffmpeg',), timedelta(seconds=1), capture='stdout')
    assert result == b'encoded'


@pytest.mark.asyncio
async def test_run_ffmpeg_capture_stderr_returns_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeProc:
        returncode = 0

        async def communicate(self, input_data: bytes | None = None) -> tuple[bytes, bytes]:
            return b'', b'analysis-json'

        def kill(self) -> None:
            return None

        async def wait(self) -> int:
            return self.returncode

    async def _fake_create_subprocess_exec(*args: str, **kwargs: object) -> _FakeProc:
        return _FakeProc()

    monkeypatch.setattr(ffmpeg_module.asyncio, 'create_subprocess_exec', _fake_create_subprocess_exec)

    result = await ffmpeg_module._run_ffmpeg(('ffmpeg',), timedelta(seconds=1), capture='stderr')
    assert result == b'analysis-json'


@pytest.mark.asyncio
async def test_normalize_video_audio_loudness_uses_stderr_capture_for_analysis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    loudnorm_json = (
        '{"input_i":"-20.1","input_tp":"-1.0","input_lra":"3.0","input_thresh":"-30.0","target_offset":"0.5"}'
    )

    async def _fake_run_ffmpeg(
        cmd: tuple[str, ...],
        timeout: timedelta,
        *,
        stdin_bytes: bytes | None = None,
        capture: str = 'none',
    ) -> bytes:
        calls.append({'cmd': cmd, 'capture': capture, 'timeout': timeout, 'stdin_bytes': stdin_bytes})
        if len(calls) == 1:
            return loudnorm_json.encode()
        return b''

    monkeypatch.setattr(ffmpeg_module, '_run_ffmpeg', _fake_run_ffmpeg)

    class _FakePath:
        def __init__(self, name: str) -> None:
            self._name = name

        def write_bytes(self, data: bytes) -> int:
            return len(data)

        def read_bytes(self) -> bytes:
            return b'normalized-video'

        def unlink(self, missing_ok: bool = False) -> None:
            return None

        def __str__(self) -> str:
            return self._name

    monkeypatch.setattr(ffmpeg_module, 'Path', _FakePath)
    monkeypatch.setattr(ffmpeg_module.tempfile, 'mkstemp', lambda suffix='': (0, f'/tmp/fake{suffix}'))
    monkeypatch.setattr(ffmpeg_module.os, 'close', lambda fd: None)

    result = await ffmpeg_module.normalize_video_audio_loudness(b'video-bytes')

    assert result == b'normalized-video'
    assert calls[0]['capture'] == 'stderr'
    assert calls[1]['capture'] == 'none'


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
