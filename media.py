"""Voice transcription, TTS, and image processing for TG media."""

import asyncio
import base64
import logging
import os
import re
import shutil
from pathlib import Path

log = logging.getLogger(__name__)

_MEDIA_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
}

_STT_MODEL = os.environ.get("STT_MODEL", "mimo-v2-omni")
_STT_PROMPT = os.environ.get("STT_PROMPT", "转录这段语音, 中文用简体, 只输出文本, 不要解释。")
_STT_LOCAL_MODEL = os.environ.get("STT_LOCAL_MODEL", "base")  # tiny/base/small/medium/large-v3
_local_whisper = None  # lazy singleton — init cost ~1-3s, model download ~150MB first run


async def _stt_local(wav_path: Path) -> str:
    """faster-whisper local STT — open box default, no API key needed.
    First call downloads model (~150MB for base) to ~/.cache/huggingface/. Subsequent calls reuse.
    """
    global _local_whisper
    if _local_whisper is None:
        try:
            from faster_whisper import WhisperModel
        except ImportError as e:
            raise RuntimeError("STT 未配置: faster-whisper 未装. 跑 `uv sync` 重装依赖.") from e
        _local_whisper = WhisperModel(_STT_LOCAL_MODEL, device="auto", compute_type="auto")
    loop = asyncio.get_running_loop()

    def _do_transcribe() -> str:
        # language=None → 自动检测; faster-whisper 中英 detect 都准. transcribe 是 sync,
        # 必须丢 executor, 不然 base model 几秒解码会卡 event loop (TG long-poll 会 timeout).
        segments, _info = _local_whisper.transcribe(
            str(wav_path), language=None, initial_prompt=_STT_PROMPT
        )
        return "".join(s.text for s in segments).strip()

    return await loop.run_in_executor(None, _do_transcribe)


async def _stt_wav(wav_path: Path) -> str:
    """STT dispatcher: MiMo (if configured, best) → local whisper (default, free)."""
    api_url = os.environ.get("MIMO_API_URL")
    api_key = os.environ.get("MIMO_API_KEY")
    if not api_url or not api_key:
        return await _stt_local(wav_path)

    audio_b64 = base64.b64encode(wav_path.read_bytes()).decode()
    import httpx
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key,
        "Authorization": f"Bearer {api_key}",
    }
    body = {
        "model": _STT_MODEL,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": _STT_PROMPT},
                {"type": "input_audio",
                 "input_audio": {"data": audio_b64, "format": "wav"}},
            ],
        }],
        "max_tokens": 2048,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{api_url.rstrip('/')}/chat/completions",
            json=body, headers=headers,
        )
        if r.status_code != 200:
            raise RuntimeError(f"MiMo STT HTTP {r.status_code}: {r.text[:200]}")
        content = r.json()["choices"][0]["message"].get("content", "").strip()
        if not content:
            raise RuntimeError("MiMo 返回空转录")
        return content


async def transcribe_voice(ogg_path: Path, keep_wav: Path | None = None) -> str:
    """TG OGG voice → text via ffmpeg + MiMo. Fail loud, no fallback.

    keep_wav: 非 None 时把 16kHz mono WAV 在 STT 完成后复制到该路径 (不影响 STT 行为).
    voice-clone skill 用此机制让 CC 拿到 wav 路径做声音克隆.
    """
    wav_path = ogg_path.with_suffix(".wav")
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", str(ogg_path),
            "-ar", "16000", "-ac", "1", str(wav_path),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode != 0 or not wav_path.exists():
            raise RuntimeError(f"ffmpeg 转码失败: {stderr.decode()[:200]}")
        text = await _stt_wav(wav_path)
        if keep_wav is not None:
            # 失败不影响 STT 主路径 (caller 的合约只是拿 text). 用 .partial → os.replace
            # 原子化, 防外部 watcher 读到半文件. 任何异常 log warning 后吞掉.
            try:
                keep_wav.parent.mkdir(parents=True, exist_ok=True)
                tmp = keep_wav.with_suffix(keep_wav.suffix + ".partial")
                shutil.copy2(wav_path, tmp)
                os.replace(tmp, keep_wav)
            except Exception as e:
                log.warning("voice-clone keep_wav failed: %s (STT preserved)", e)
        return text
    finally:
        wav_path.unlink(missing_ok=True)


def silk_to_wav(silk_path: Path, sample_rate: int = 24000) -> Path:
    """Decode WeChat SILK v3 voice → WAV file next to the input.

    Requires `pilk` (pip install pilk). WeChat voice is always 24kHz mono s16le
    after SILK decode; we wrap the raw PCM in a WAV container so STT can ingest.
    """
    try:
        import pilk
    except ImportError as e:
        raise RuntimeError("SILK 解码缺依赖: .venv/bin/pip install pilk") from e
    import wave

    pcm_path = silk_path.with_suffix(".pcm")
    wav_path = silk_path.with_suffix(".wav")
    try:
        pilk.decode(str(silk_path), str(pcm_path), pcm_rate=sample_rate)
        if not pcm_path.exists():
            raise RuntimeError("pilk decode 未生成 PCM")
        pcm = pcm_path.read_bytes()
        with wave.open(str(wav_path), "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)  # 16-bit
            w.setframerate(sample_rate)
            w.writeframes(pcm)
    finally:
        pcm_path.unlink(missing_ok=True)
    return wav_path


async def transcribe_silk(silk_path: Path) -> str:
    """WeChat SILK voice → text."""
    wav_path = silk_to_wav(silk_path)
    try:
        return await _stt_wav(wav_path)
    finally:
        wav_path.unlink(missing_ok=True)


_VIDEO_MODEL = os.environ.get("VIDEO_MODEL", "mimo-v2-omni")


async def understand_video(video_path: Path, question: str = "") -> str | None:
    """Send video to an OpenAI-compatible multimodal endpoint → text description.

    Physical: CC SDK doesn't accept video. Delegate to a video-native model
    (e.g. mimo-v2-omni), feed the textual summary back to CC.
    Returns None if no endpoint configured or call fails.
    """
    api_url = os.environ.get("MIMO_API_URL")
    api_key = os.environ.get("MIMO_API_KEY")
    if not api_url:
        return None
    size = video_path.stat().st_size
    if size > 10 * 1024 * 1024:
        return f"[Video too large for base64 upload: {size // 1024 // 1024}MB > 10MB]"

    import httpx
    data_url = f"data:video/mp4;base64,{base64.b64encode(video_path.read_bytes()).decode()}"
    prompt = question or "请详细描述这段视频的画面和声音内容。"

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["api-key"] = api_key
        headers["Authorization"] = f"Bearer {api_key}"

    body = {
        "model": _VIDEO_MODEL,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "video_url",
                        "video_url": {"url": data_url},
                        "fps": 2,
                        "media_resolution": "default",
                    },
                    {"type": "text", "text": prompt},
                ],
            }
        ],
        "max_completion_tokens": 2048,
    }

    try:
        async with httpx.AsyncClient(timeout=180) as client:
            r = await client.post(
                f"{api_url.rstrip('/')}/chat/completions",
                json=body, headers=headers,
            )
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"].get("content") or None
    except Exception as e:
        log.warning("Video understanding failed: %s", e)
        return None


def image_to_base64(path: Path) -> dict[str, str]:
    """Image file → {media_type, data} for CC SDK."""
    media_type = _MEDIA_TYPES.get(path.suffix.lower(), "image/jpeg")
    data = base64.b64encode(path.read_bytes()).decode()
    return {"media_type": media_type, "data": data}


_MD_STRIP = [
    (re.compile(r"```.*?```", re.DOTALL), ""),
    (re.compile(r"`([^`]+)`"), r"\1"),
    (re.compile(r"\*\*(.+?)\*\*"), r"\1"),
    (re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)"), r"\1"),
    (re.compile(r"~~(.+?)~~"), r"\1"),
    (re.compile(r"^#{1,6}\s*", re.MULTILINE), ""),
    (re.compile(r"!?\[([^\]]*)\]\([^)]+\)"), r"\1"),
    (re.compile(r"https?://\S+"), ""),
    (re.compile(r"^[-*+]\s+", re.MULTILINE), ""),
    (re.compile(r"^-{3,}$", re.MULTILINE), ""),
    (re.compile(r"\|.*?\|", re.MULTILINE), ""),
]


def _strip_md(text: str) -> str:
    for pat, repl in _MD_STRIP:
        text = pat.sub(repl, text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


_TTS_URL = os.environ.get("TTS_URL")
_TTS_MODEL = os.environ.get("TTS_MODEL", "tts-1")
_TTS_VOICE = os.environ.get("TTS_VOICE", "nova")
_TTS_API_KEY = os.environ.get("TTS_API_KEY")
_TTS_BACKEND = os.environ.get("TTS_BACKEND", "openai")  # "mimo" | "openai"


async def _tts_mimo(text: str, voice: str) -> bytes | None:
    """Mimo-v2-tts native chat/completions — script layer wraps CC's simple text
    (which may include <style>...</style> prefix and (cue) markers) into the
    official mimo request spec."""
    import httpx
    headers = {"Content-Type": "application/json"}
    if _TTS_API_KEY:
        headers["api-key"] = _TTS_API_KEY  # mimo uses api-key header, not Bearer
    body = {
        "model": _TTS_MODEL,
        "messages": [
            {"role": "user", "content": ""},
            {"role": "assistant", "content": text},
        ],
        "audio": {"format": "mp3", "voice": voice or "mimo_default"},
    }
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            f"{_TTS_URL.rstrip('/')}/chat/completions",
            json=body, headers=headers,
        )
        if r.status_code != 200:
            log.warning("mimo TTS %d: %s", r.status_code, r.text[:400])
            return None
        audio_b64 = r.json()["choices"][0]["message"]["audio"]["data"]
        return base64.b64decode(audio_b64)


async def _tts_openai(text: str, voice: str) -> bytes | None:
    """OpenAI-compatible /audio/speech."""
    import httpx
    headers = {"Content-Type": "application/json"}
    if _TTS_API_KEY:
        headers["Authorization"] = f"Bearer {_TTS_API_KEY}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(
            f"{_TTS_URL.rstrip('/')}/audio/speech",
            json={"model": _TTS_MODEL, "input": text, "voice": voice,
                  "response_format": "mp3"},
            headers=headers,
        )
        r.raise_for_status()
        return r.content


async def _tts_to_mp3(text: str, voice: str | None, mp3: Path) -> bool:
    """Render TTS to mp3. Backend: TTS_URL (mimo/openai) else edge-tts."""
    clean = _strip_md(text)[:4000]
    if not clean:
        return False
    if _TTS_URL:
        v = voice or _TTS_VOICE
        if _TTS_BACKEND == "mimo":
            audio_bytes = await _tts_mimo(clean, v)
        else:
            audio_bytes = await _tts_openai(clean, v)
        if not audio_bytes:
            return False
        mp3.write_bytes(audio_bytes)
    else:
        import edge_tts
        communicator = edge_tts.Communicate(clean, voice or "zh-CN-XiaoxiaoNeural")
        await communicator.save(str(mp3))
    return mp3.exists()


async def text_to_silk(text: str, voice: str | None = None) -> tuple[Path, int] | None:
    """Text → WeChat SILK v3 voice. Returns (silk_path, duration_ms) or None.

    Format spec from @tencent-weixin/openclaw-weixin TS api/types.ts:
      VoiceItem.encode_type=6 (SILK), bits_per_sample=16, sample_rate=24000.
    Pipeline: TTS → mp3 → ffmpeg → 24kHz mono s16le PCM → pilk encode → SILK.
    """
    import tempfile
    import uuid

    if not _strip_md(text)[:4000]:
        return None

    tmp = Path(tempfile.gettempdir())
    tag = uuid.uuid4().hex
    mp3 = tmp / f"tts_{tag}.mp3"
    pcm = tmp / f"tts_{tag}.pcm"
    silk = tmp / f"tts_{tag}.silk"

    try:
        if not await _tts_to_mp3(text, voice, mp3):
            return None

        # mp3 → 24 kHz mono s16le PCM (matches Weixin SILK_SAMPLE_RATE)
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", str(mp3),
            "-f", "s16le", "-acodec", "pcm_s16le",
            "-ar", "24000", "-ac", "1",
            str(pcm),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=60)
        if not pcm.exists() or pcm.stat().st_size == 0:
            return None

        # PCM → SILK v3 via pilk (already a dep for inbound silk_to_wav)
        try:
            import pilk
        except ImportError as e:
            raise RuntimeError("SILK encode 缺依赖: .venv/bin/pip install pilk") from e
        pilk.encode(str(pcm), str(silk), pcm_rate=24000, tencent=False)
        if not silk.exists() or silk.stat().st_size == 0:
            return None

        # duration_ms = pcm bytes / (2 bytes/sample × 24000 Hz) × 1000
        duration_ms = int(pcm.stat().st_size / (2 * 24000) * 1000)
        return (silk, duration_ms)
    except Exception as e:
        log.warning("text_to_silk failed: %s", e)
        silk.unlink(missing_ok=True)
        return None
    finally:
        mp3.unlink(missing_ok=True)
        pcm.unlink(missing_ok=True)


async def text_to_voice(text: str, voice: str | None = None) -> Path | None:
    """Text → OGG/Opus voice file. Returns path or None.

    Backend priority: TTS_URL (mimo/openai auto-detected) else free edge-tts.
    """
    import tempfile
    import uuid

    clean = _strip_md(text)[:4000]
    if not clean:
        return None

    tmp = Path(tempfile.gettempdir())
    mp3 = tmp / f"tts_{uuid.uuid4().hex}.mp3"
    ogg = mp3.with_suffix(".ogg")

    try:
        if _TTS_URL:
            v = voice or _TTS_VOICE
            if _TTS_BACKEND == "mimo":
                audio_bytes = await _tts_mimo(clean, v)
            else:
                audio_bytes = await _tts_openai(clean, v)
            if not audio_bytes:
                return None
            mp3.write_bytes(audio_bytes)
        else:
            import edge_tts
            communicator = edge_tts.Communicate(clean, voice or "zh-CN-XiaoxiaoNeural")
            await communicator.save(str(mp3))

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-i", str(mp3),
            "-c:a", "libopus", "-b:a", "64k", "-vbr", "off",
            "-ar", "48000", "-ac", "1",
            str(ogg),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=60)

        return ogg if ogg.exists() else None
    except Exception as e:
        log.warning("TTS failed: %s", e)
        return None
    finally:
        mp3.unlink(missing_ok=True)
