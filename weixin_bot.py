"""CC WeChat Bot — thin WeChat transport for Claude Code.

Peer of bot.py (TG). Same CC binary, same memory, same skills. Only the wire
is different — and WeChat's wire is iLink bot HTTP + CDN AES + SILK voice +
per-peer contextToken, so we need a bit more protocol work than TG.

Run:
    .venv/bin/python weixin_bot.py             # reuse stored login
    .venv/bin/python weixin_bot.py --login     # force QR re-login

Bot only does what CC physically cannot: iLink protocol, CDN crypto, SILK decode,
contextToken routing, markdown stripping for WeChat's plain-text display.
"""

import asyncio
import base64
import fcntl
import json
import logging
import os
import re
import secrets
import signal
import sys
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv(override=True)

from constants import PROJECT, STATE_DIR
from engine import VENV_PYTHON, make_engine
from media import transcribe_silk, understand_video
from weixin_account import (
    add_allow_from, clear_stale_for_user, get_context_token, is_allowed,
    list_account_ids, load_account, load_allow_from, load_sync_buf,
    register_account, save_account, save_sync_buf, set_context_token,
)
from weixin_bridge import bridge
from weixin_ilink import (
    ITEM_FILE, ITEM_IMAGE, ITEM_TEXT, ITEM_VIDEO, ITEM_VOICE,
    WeixinClient, WeixinSessionExpired,
    normalize_account_id, start_qr_login, text_item, wait_qr_login,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger(f"{PROJECT}.weixin")

# ── Heartbeat (双 bot 互监控, 零 LLM 成本, 镜像 bot.py 同段) ──────────
# 自己每 30s touch; 看主 TG bot 心跳, stale > 3 min 通过微信推 V (allowFrom[0]).
_HEARTBEAT_ME = STATE_DIR / f"{PROJECT}-weixin-heartbeat"
_HEARTBEAT_PEER = STATE_DIR / f"{PROJECT}-tg-heartbeat"
_HEARTBEAT_STALE_S = 180
_HEARTBEAT_INTERVAL_S = 30

PENDING_WX_UPDATES_FILE = STATE_DIR / f"{PROJECT}-weixin-pending-updates.json"


async def _heartbeat_loop(client: "WeixinClient", account_id: str) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    alerted = False
    while True:
        try:
            _HEARTBEAT_ME.touch()
            if _HEARTBEAT_PEER.exists():
                age = time.time() - _HEARTBEAT_PEER.stat().st_mtime
                if age > _HEARTBEAT_STALE_S and not alerted:
                    allow = load_allow_from(account_id)
                    target = allow[0] if allow else None
                    if target:
                        try:
                            await client.send_message(
                                target,
                                [text_item(
                                    f"⚠️ TG bot 心跳已 {int(age)}s 未更新 "
                                    f"(阈值 {_HEARTBEAT_STALE_S}s)"
                                )],
                                context_token=get_context_token(account_id, target),
                            )
                            alerted = True
                        except Exception as e:
                            log.warning("heartbeat alert send failed: %s", e)
                elif age <= 60:
                    alerted = False
        except Exception as e:
            log.warning("heartbeat loop error: %s", e)
        await asyncio.sleep(_HEARTBEAT_INTERVAL_S)

# ── CC instance (WeChat-scoped) ───────────────────────────────────────

_WEIXIN_MCP_SCRIPT = str(Path(__file__).parent / "weixin_mcp.py")

_WX_SOURCE_PROMPT = (
    "Source: WeChat. "
    "Markdown rendered natively: bold/italic/strikethrough/inline-code/"
    "code-fence-with-syntax-highlight/headings/bullet-lists/numbered-lists/"
    "tables/links/blockquotes/hr; nested markdown supported. "
    "Bare URLs auto-linked (URL visible); [text](url) shows only text (URL hidden). "
    "HTML tags / [x]/[ ] task lists / HTML entities display literally. "
    "New bubble: separate paragraphs with three newlines (\\n\\n\\n). "
    "No edit-message; each bubble is final once sent. "
    "Max 4000 chars/message."
)

cc = make_engine(
    state_file=STATE_DIR / f"{PROJECT}-weixin-session.json",
    source_prompt=_WX_SOURCE_PROMPT,
    mcp_servers={
        "weixin": {
            "command": VENV_PYTHON,
            "args": [_WEIXIN_MCP_SCRIPT],
        },
    },
)

# ── markdown handling ────────────────────────────────────────────────

_MD_STRIPS = [
    # Strip ``` fences but KEEP the inner code (WeChat renders plaintext —
    # without the body the user just sees code disappear). The info string
    # tolerates anything except newline / backtick (covers `c#`, `.env`,
    # `shell script`, etc.); trailing newline after opener is consumed too.
    (re.compile(r"```[^\r\n`]*\r?\n?(.*?)```", re.DOTALL), r"\1"),
    (re.compile(r"`([^`]+)`"), r"\1"),
    (re.compile(r"\*\*(.+?)\*\*"), r"\1"),
    (re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)"), r"\1"),
    (re.compile(r"~~(.+?)~~"), r"\1"),
    (re.compile(r"^#{1,6}\s*", re.MULTILINE), ""),
    (re.compile(r"!\[([^\]]*)\]\([^)]+\)"), ""),
    (re.compile(r"\[([^\]]+)\]\(([^)]+)\)"), r"\1 \2"),
    (re.compile(r"^\s*[-*+]\s+", re.MULTILINE), ""),
    (re.compile(r"^-{3,}$", re.MULTILINE), ""),
    (re.compile(r"^\|.*?\|$", re.MULTILINE), ""),
]


def strip_markdown(text: str) -> str:
    """WX client renders markdown natively (2026-04-30 实测两轮截图证实).
    Strip 默认 disabled — markdown 直传 wire, 客户端渲染. 仅 collapse 多空行/边界 trim.
    Set BABATA_WEIXIN_STRIP_MD=1 兜底 (老客户端不渲染时 opt-in)."""
    if os.environ.get("BABATA_WEIXIN_STRIP_MD") == "1":
        for pat, repl in _MD_STRIPS:
            text = pat.sub(repl, text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


_MAX_WX = 4000


# ── stream split safety ───────────────────────────────────────────────
# WeChat has no edit-message; every streamed flush is final. Even when the
# client renders Markdown natively, splitting inside `...`, **...**, or
# [label](url) leaves half-markers visible. Keep split points natural and
# Markdown-balanced; hard-cut paths sanitize the prefix before sending.
_BACKTICK_RE = re.compile(r"(?<!\\)`")           # unescaped backtick
_BOLD_RE = re.compile(r"(?<!\*)\*\*(?!\*)")      # ** delimiter (not *** etc.)
_SAFE_BOUNDARIES = (
    "\n\n",
    "\n",
    "。", "！", "？", "；",
    ". ", "! ", "? ", "; ",
    "，", "、",
    ", ",
    " ",
)


def _first_unbalanced_link_start(text: str) -> int | None:
    i = 0
    n = len(text)
    while i < n:
        if text[i] == "\\":
            i += 2
            continue
        image = text[i] == "!" and i + 1 < n and text[i + 1] == "["
        if image or text[i] == "[":
            start = i + 1 if image else i
            close = text.find("]", start + 1)
            if close < 0:
                return start
            if close + 1 < n and text[close + 1] == "(":
                depth = 0
                j = close + 1
                while j < n:
                    if text[j] == "\\":
                        j += 2
                        continue
                    if text[j] == "(":
                        depth += 1
                    elif text[j] == ")":
                        depth -= 1
                        if depth == 0:
                            break
                    j += 1
                if depth != 0:
                    return start
                i = j + 1
                continue
            i = close + 1
            continue
        i += 1
    return None


def _md_balanced(text: str) -> bool:
    """True if unambiguous Markdown delimiters are paired in `text`."""
    if len(_BACKTICK_RE.findall(text)) % 2:
        return False
    if len(_BOLD_RE.findall(text)) % 2:
        return False
    return _first_unbalanced_link_start(text) is None


def _find_safe_split(
    text: str,
    hi: int | None = None,
    boundaries: tuple[str, ...] = _SAFE_BOUNDARIES,
) -> int:
    """Largest split point <= hi at a natural Markdown-balanced boundary."""
    n = len(text)
    if hi is None or hi > n:
        hi = n
    if hi <= 0:
        return 0
    for boundary in boundaries:
        search_hi = hi
        while search_hi > 0:
            pos = text.rfind(boundary, 0, search_hi)
            if pos < 0:
                break
            candidate = pos + len(boundary)
            if 0 < candidate <= hi and _md_balanced(text[:candidate]):
                return candidate
            search_hi = pos
    return 0


def _sanitize_unbalanced_markers(text: str) -> str:
    """Last-resort scrubber for force-cut chunks with unmatched markers."""
    if len(_BOLD_RE.findall(text)) % 2:
        idx = text.rfind("**")
        if idx >= 0:
            text = text[:idx] + text[idx + 2:]
    if len(_BACKTICK_RE.findall(text)) % 2:
        matches = list(_BACKTICK_RE.finditer(text))
        if matches:
            last = matches[-1]
            text = text[:last.start()] + text[last.end():]
    link_start = _first_unbalanced_link_start(text)
    if link_start is not None:
        start = link_start - 1 if link_start > 0 and text[link_start - 1] == "!" else link_start
        text = text[:start]
    return text


def chunk_text(text: str, limit: int = _MAX_WX) -> list[str]:
    """Hard-cap chunker — bubbles are pre-split by LLM via \\n\\n\\n; this
    only fires when one bubble exceeds `limit` (rare: LLM verbose without
    breaking). Prefers balanced natural boundaries; hard-cuts as last resort."""
    text = text.strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    out: list[str] = []
    while len(text) > limit:
        cut = _find_safe_split(text, limit)
        if cut <= 0:
            cut = limit
            chunk = _sanitize_unbalanced_markers(text[:cut])
        else:
            chunk = text[:cut]
        out.append(chunk.rstrip())
        text = text[cut:].lstrip()
    if text:
        out.append(text)
    return out


# ── stream coalescer thresholds ───────────────────────────────────────
# LLM marks bubble boundaries with \n\n\n (see _WX_SOURCE_PROMPT). _drain
# splits buf on that marker. Below are safety nets for when the LLM hangs
# mid-bubble or forgets to mark — both still split at safe boundaries
# (paragraph / line / sentence-end), never mid-word, mid-token, or mid-marker.
_STREAM_FLUSH_IDLE_S = 3.0   # idle drain — ship completed boundary or hold
_STREAM_HARD_MAX = 3500      # bubble exceeded this — force ship at safest split


# Boundary preference for splitting trailing partials, longest-context first.
# CJK terminators don't need trailing space; ASCII ones do (avoids cutting
# "v1.0" / "Mr.X"). Bullet markers ("- " / "* ") aren't safe — splitting
# there would orphan them onto the next bubble as raw chars.
_PARTIAL_BOUNDARIES_NEWLINE = ("\n\n", "\n")
_PARTIAL_BOUNDARIES_CJK = ("。", "！", "？", "；", "…")
_PARTIAL_BOUNDARIES_ASCII = (". ", "! ", "? ", "; ")


def _safe_split_partial(text: str) -> tuple[str, str]:
    """Split a trailing partial at the latest safe boundary.

    Returns (head, hold). head is shippable; hold is the leftover that
    callers append back to buf. Empty head means no boundary was found —
    callers must decide between holding all of `text` or hard-cutting.

    Boundary preference: \\n\\n > \\n > CJK terminator > ASCII terminator+space.
    """
    if not text:
        return "", ""
    split = _find_safe_split(
        text,
        boundaries=(
            _PARTIAL_BOUNDARIES_NEWLINE
            + _PARTIAL_BOUNDARIES_CJK
            + _PARTIAL_BOUNDARIES_ASCII
        ),
    )
    if split > 0:
        return text[:split], text[split:]
    return "", text


# ── inbound media decode ─────────────────────────────────────────────

_INBOUND_DIR = Path.home() / f".{PROJECT}" / "weixin" / "media" / "inbound"


# Per-user typing_ticket cache. Mirrors plugin's config-cache.ts: cache the
# ticket returned by getConfig for a random-up-to-24h TTL, fetch again only
# after expiry. Saves one extra HTTP call per inbound message.
_TICKET_CACHE: dict[str, tuple[str, float]] = {}  # user_id → (ticket, expires_at)
_TICKET_TTL_MAX_S = 24 * 60 * 60


async def _get_typing_ticket(
    client: WeixinClient, user_id: str, ctx_token: str | None
) -> str | None:
    import random
    cached = _TICKET_CACHE.get(user_id)
    if cached and cached[1] > time.time():
        return cached[0]
    try:
        cfg = await client.get_config(user_id, context_token=ctx_token)
        ticket = cfg.get("typing_ticket") or ""
    except Exception as e:
        log.debug("getConfig failed for %s: %s", user_id, e)
        return None
    if ticket:
        _TICKET_CACHE[user_id] = (ticket, time.time() + random.random() * _TICKET_TTL_MAX_S)
    return ticket or None


def _inbound_tmp(suffix: str) -> Path:
    _INBOUND_DIR.mkdir(parents=True, exist_ok=True)
    return _INBOUND_DIR / f"{int(time.time())}-{secrets.token_hex(6)}{suffix}"


def _sniff_image_mime(data: bytes) -> str:
    if data[:4] == b"\x89PNG":
        return "image/png"
    if data[:3] == b"GIF":
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


async def _decode_item(
    client: WeixinClient, item: dict[str, Any]
) -> tuple[str, list[dict[str, str]]]:
    """One inbound MessageItem → (text_body, images_for_cc).

    Returns text that goes into the CC prompt + base64 image blocks.
    Voice/video are converted to text descriptions; files are saved locally.
    """
    itype = item.get("type")

    if itype == ITEM_TEXT:
        return ((item.get("text_item") or {}).get("text") or "", [])

    if itype == ITEM_VOICE:
        voice = item.get("voice_item") or {}
        # DIAGNOSTIC: dump full inbound voice_item to compare vs our outbound fields
        try:
            import json as _json
            log.warning("INBOUND voice_item raw: %s", _json.dumps(voice, ensure_ascii=False, default=str)[:1500])
        except Exception:
            pass
        # DIAGNOSTIC: server-side STT fast path skips download — force silk
        # download + copy BEFORE the fast-path so we always have a sample to
        # diff against our outbound silk magic bytes / structure.
        media = voice.get("media") or {}
        if media:
            try:
                _raw = await client.download_media(media)
                _keep = Path("/tmp/inbound-voice-sample.silk")
                _keep.write_bytes(_raw)
                log.warning(
                    "INBOUND silk saved to %s (%d bytes), magic=%r",
                    _keep, _keep.stat().st_size, _raw[:16],
                )
            except Exception as e:
                log.warning("silk diag pre-fetch failed: %s", e)
        if voice.get("text"):  # server-provided transcription
            return (f"[语音] {voice['text']}", [])
        silk_path: Path | None = None
        try:
            raw = await client.download_media(media)
            silk_path = _inbound_tmp(".silk")
            silk_path.write_bytes(raw)
            text = await transcribe_silk(silk_path)
            return (f"[语音] {text}", [])
        except Exception as e:
            log.warning("voice decode failed: %s", e)
            return (f"[语音转文字失败: {e}]", [])
        finally:
            if silk_path:
                silk_path.unlink(missing_ok=True)

    if itype == ITEM_IMAGE:
        image = item.get("image_item") or {}
        media = image.get("media") or {}
        aeskey_hex = image.get("aeskey")
        try:
            raw = await client.download_media(media, aeskey_hex_override=aeskey_hex)
        except Exception as e:
            log.warning("image download failed: %s", e)
            return (f"[图片下载失败: {e}]", [])
        mime = _sniff_image_mime(raw)
        ext = "jpg" if mime == "image/jpeg" else mime.split("/")[-1]
        img_path = _inbound_tmp(f".{ext}")
        img_path.write_bytes(raw)
        return (
            f"[图片: {img_path}]",
            [{"media_type": mime,
              "data": base64.b64encode(raw).decode()}],
        )

    if itype == ITEM_VIDEO:
        video = item.get("video_item") or {}
        media = video.get("media") or {}
        video_path: Path | None = None
        try:
            raw = await client.download_media(media)
            video_path = _inbound_tmp(".mp4")
            video_path.write_bytes(raw)
            desc = await understand_video(video_path)
            return (f"[视频] {desc}" if desc else "[视频：无法理解内容]", [])
        except Exception as e:
            log.warning("video handle failed: %s", e)
            return (f"[视频处理失败: {e}]", [])
        finally:
            if video_path:
                video_path.unlink(missing_ok=True)

    if itype == ITEM_FILE:
        f = item.get("file_item") or {}
        media = f.get("media") or {}
        file_name = f.get("file_name") or "file"
        try:
            raw = await client.download_media(media)
            safe_name = re.sub(r"[/\\\x00]", "_", file_name)
            _INBOUND_DIR.mkdir(parents=True, exist_ok=True)
            path = _INBOUND_DIR / f"{int(time.time())}-{safe_name}"
            path.write_bytes(raw)
            return (f"[用户发来文件: {path}]", [])
        except Exception as e:
            log.warning("file download failed: %s", e)
            return (f"[文件下载失败: {e}]", [])

    return (f"[未知消息类型 type={itype}]", [])


def _describe_ref(ref: dict[str, Any] | None) -> str:
    if not ref:
        return ""
    title = (ref.get("title") or "").strip()
    item = ref.get("message_item") or {}
    t = item.get("type")
    if t == ITEM_TEXT:
        body = ((item.get("text_item") or {}).get("text") or "").strip()[:80]
        return f"[引用: {body}]" if body else "[引用了一条文本]"
    labels = {ITEM_IMAGE: "图片", ITEM_VOICE: "语音", ITEM_FILE: "文件", ITEM_VIDEO: "视频"}
    label = labels.get(t, "消息")
    return f"[引用 {label}: {title}]" if title else f"[引用了一条{label}]"


# ── per-user burst coalescing ────────────────────────────────────────
# WeChat has no album grouping (unlike TG's media_group_id), so image +
# caption arrive as separate msgs. Rule: image is a "comma" (wait for
# follow-up), any non-image (text/voice/video/file) is a "period" (end of
# burst). Non-image without a pending burst bypasses debounce entirely.
_MESSAGE_DEBOUNCE_S = 3.0
_pending: dict[tuple[str, str], dict[str, Any]] = {}
# A recoverable CPU failure must not permanently drop the inbound WeChat turn.
# Retry the same decoded burst before the caller advances to later updates.
_WX_MAX_TURN_RECOVERY_ATTEMPTS = int(
    os.environ.get(
        "BABATA_WX_TURN_RECOVERY_ATTEMPTS",
        os.environ.get("BABATA_TURN_RECOVERY_ATTEMPTS", "2"),
    )
)
_wx_processing_tasks: set[asyncio.Task] = set()
_pending_wx_lock = asyncio.Lock()


def _load_pending_wx_updates() -> dict[str, dict[str, Any]]:
    if not PENDING_WX_UPDATES_FILE.exists():
        return {}
    try:
        data = json.loads(PENDING_WX_UPDATES_FILE.read_text())
        records = data.get("pending", {})
        if isinstance(records, dict):
            return {str(k): v for k, v in records.items() if isinstance(v, dict)}
    except Exception as e:
        log.warning("wx pending-updates load failed: %s, treating as empty", e)
    return {}


_pending_wx_records: dict[str, dict[str, Any]] = _load_pending_wx_updates()


def _write_pending_wx_locked() -> None:
    tmp = PENDING_WX_UPDATES_FILE.with_suffix(".json.partial")
    PENDING_WX_UPDATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(
        json.dumps(
            {"pending": _pending_wx_records},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    os.replace(tmp, PENDING_WX_UPDATES_FILE)


async def _record_pending_wx_batch(
    account_id: str,
    *,
    new_buf: str,
    units: list[list[dict[str, Any]]],
) -> str:
    record_id = f"{int(time.time() * 1000)}-{secrets.token_hex(4)}"
    record = {
        "id": record_id,
        "account_id": account_id,
        "new_buf": new_buf,
        "units": units,
        "received_at": time.time(),
    }
    async with _pending_wx_lock:
        _pending_wx_records[record_id] = record
        _write_pending_wx_locked()
    return record_id


async def _ack_pending_wx_batch(record_id: str) -> None:
    async with _pending_wx_lock:
        if record_id not in _pending_wx_records:
            return
        _pending_wx_records.pop(record_id, None)
        _write_pending_wx_locked()


async def _pending_wx_batches_for_account(account_id: str) -> list[dict[str, Any]]:
    async with _pending_wx_lock:
        records = [
            dict(record)
            for record in _pending_wx_records.values()
            if record.get("account_id") == account_id
        ]
    return sorted(records, key=lambda r: r.get("received_at", 0))


# Single-flight across all users/accounts: concurrent CC queries would race
# on the shared session resume (session_id written per query end).
_cc_lock = asyncio.Lock()


def _track_wx_processing_task(task: asyncio.Task) -> asyncio.Task:
    _wx_processing_tasks.add(task)
    task.add_done_callback(_wx_processing_tasks.discard)
    return task


async def _enqueue_inbound_msg(
    client: WeixinClient, msg: dict[str, Any], account_id: str
) -> asyncio.Task | bool:
    return await _enqueue_inbound_msgs(client, [msg], account_id)


async def _enqueue_inbound_msgs(
    client: WeixinClient, msgs: list[dict[str, Any]], account_id: str
) -> asyncio.Task | bool:
    if not msgs:
        return True

    from_user = msgs[0].get("from_user_id") or ""
    for msg in msgs:
        ctx_token = msg.get("context_token")
        if from_user and ctx_token:
            set_context_token(account_id, from_user, ctx_token)

    if not is_allowed(account_id, from_user):
        log.warning("ignoring unauthorized from=%s", from_user)
        return True

    has_image = any(
        item.get("type") == ITEM_IMAGE
        for msg in msgs
        for item in (msg.get("item_list") or [])
    )
    has_non_image = any(
        item.get("type") != ITEM_IMAGE
        for msg in msgs
        for item in (msg.get("item_list") or [])
    )

    key = (account_id, from_user)
    pending = _pending.get(key)

    if pending is not None:
        # Inside an active burst: always append, and if this is non-image
        # (text/voice/video/file) mark the burst complete for immediate flush.
        pending["msgs"].extend(msgs)
        pending["last_arrival_at"] = time.monotonic()
        if has_non_image:
            pending["non_image_arrived"].set()
        return pending.get("task")

    if not has_image or has_non_image:
        # No pending burst + no image — straight to CC, no debounce.
        async with _cc_lock:
            try:
                return await _process_combined_msgs(client, msgs, account_id)
            except Exception:
                log.exception("msg processing crashed")
                return False

    # Image starts a burst that waits for the follow-up.
    pending = {
        "msgs": list(msgs),
        "client": client,
        "account_id": account_id,
        "last_arrival_at": time.monotonic(),
        "non_image_arrived": asyncio.Event(),
    }
    task = _track_wx_processing_task(asyncio.create_task(_flush_pending(key)))
    pending["task"] = task
    _pending[key] = pending
    return task


async def _flush_pending(key: tuple[str, str]) -> bool:
    pending = _pending.get(key)
    if pending is None:
        return True
    event = pending["non_image_arrived"]
    while True:
        try:
            await asyncio.wait_for(event.wait(), timeout=_MESSAGE_DEBOUNCE_S)
            break  # non-image arrived → burst complete
        except asyncio.TimeoutError:
            pending = _pending.get(key)
            if pending is None:
                return True
            if time.monotonic() - pending["last_arrival_at"] < _MESSAGE_DEBOUNCE_S:
                continue  # more images still arriving, keep waiting
            break  # true quiet window — flush what we have

    _pending.pop(key, None)
    async with _cc_lock:
        try:
            return await _process_combined_msgs(
                pending["client"], pending["msgs"], pending["account_id"],
            )
        except Exception:
            log.exception("combined msg processing crashed")
            return False


async def _process_combined_msgs(
    client: WeixinClient, msgs: list[dict[str, Any]], account_id: str
) -> bool:
    if not msgs:
        return True

    from_user = msgs[0].get("from_user_id") or ""
    ctx_token = next(
        (m.get("context_token") for m in reversed(msgs) if m.get("context_token")),
        None,
    )
    images: list[dict[str, str]] = []
    message_bodies: list[str] = []
    for msg in msgs:
        texts: list[str] = []
        ref_note = ""
        for item in msg.get("item_list") or []:
            if item.get("ref_msg"):
                ref_note = _describe_ref(item.get("ref_msg"))
            text, imgs = await _decode_item(client, item)
            if text:
                texts.append(text)
            images.extend(imgs)
        body = "\n".join(t for t in texts if t).strip()
        if ref_note:
            body = f"{ref_note}\n{body}" if body else ref_note
        if body:
            message_bodies.append(body)

    combined = "\n".join(message_bodies).strip()
    if len(message_bodies) > 1:
        blocks = [
            "The user sent these WeChat messages while the previous turn was "
            "running or before the current poll checkpoint advanced.",
            "Treat them as one user turn, ordered oldest to newest. Later "
            "messages may clarify or supersede earlier messages.",
            "",
        ]
        for idx, body in enumerate(message_bodies, start=1):
            blocks.append(f"<user_message n={idx}>\n{body}\n</user_message>")
            blocks.append("")
        combined = "\n".join(blocks).strip()
    if not combined and not images:
        log.info("inbound from %s: no decodable content", from_user)
        return True

    log.info("← %s: %s (imgs=%d, msgs=%d)", from_user, combined[:80], len(images), len(msgs))

    # Hand bridge the current conversation context so wx_mcp actions can reply
    bridge.set_context(client, from_user, ctx_token, account_id)

    # Typing on (best-effort; ticket cached across inbounds, 24h TTL)
    ticket = await _get_typing_ticket(client, from_user, ctx_token)
    if ticket:
        try:
            await client.send_typing(from_user, ticket, 1)
        except Exception as e:
            log.debug("typing on failed: %s", e)

    # Stream coalescer — wx protocol has no edit-message, so each bubble is
    # final once sent. LLM marks bubble boundaries with \n\n\n in its output
    # (see _WX_SOURCE_PROMPT); _drain splits buf on that marker, ships the
    # complete bubbles, and holds the trailing partial. Idle / hard-max are
    # safety nets for when the LLM hangs mid-bubble or forgets to mark; they
    # split at safe boundaries (paragraph / line / sentence-end) rather than
    # mid-word, and only hard-cut at end-of-stream when nothing safe exists.
    buf: list[str] = []
    # last_flush is initialized lazily on the first chunk so cc.query startup
    # latency (often >3s before the first token) doesn't trip idle immediately
    # and ship the first chunk as a single-character bubble.
    last_flush: float | None = None
    flush_lock = asyncio.Lock()
    sent_any = False
    had_send_failure = False

    async def _send_bubble(text: str) -> bool:
        """Strip markdown (env-gated) + 4000-char hard-cap chunk + send.
        Retries once on transient failure; permanent failure flips
        had_send_failure so turn-end can rebroadcast resp.content."""
        nonlocal sent_any, had_send_failure
        text = strip_markdown(text)
        if not text:
            return True
        ok = True
        for chunk in chunk_text(text):
            success = False
            for attempt in range(2):
                try:
                    await client.send_message(
                        from_user, [text_item(chunk)], context_token=ctx_token,
                    )
                    sent_any = True
                    success = True
                    break
                except Exception:
                    if attempt == 0:
                        log.warning("wx send failed (retry once)", exc_info=True)
                        await asyncio.sleep(0.5)
                    else:
                        log.error("wx send failed (gave up)", exc_info=True)
            if not success:
                had_send_failure = True
                ok = False
        return ok

    async def _drain(allow_hard_cut: bool) -> None:
        """Drain buf. Always ships any complete \\n\\n\\n-bounded bubbles.
        For the trailing partial: split at the latest safe boundary
        (paragraph > line > sentence). If no boundary exists, hold it
        unless allow_hard_cut=True (used at hard-cap / end-of-stream when
        holding indefinitely is worse than a clean cut).

        allow_hard_cut=True iterates until buf is empty: a single boundary
        split would otherwise hold the suffix back in buf and the suffix
        is permanently lost (no further drain after end-of-stream). Inner
        loop ships head, then re-feeds hold so it gets its own boundary
        scan / hard-cut treatment.

        last_flush is reset on every call regardless of ship outcome:
        otherwise an idle drain that finds no safe boundary keeps holding
        on every subsequent chunk (last_flush still stale → idle re-trips).
        """
        nonlocal last_flush
        async with flush_lock:
            while True:
                if not buf:
                    break
                raw = "".join(buf)
                buf.clear()
                parts = re.split(r"\n{3,}", raw)
                tail = parts.pop()
                for p in parts:
                    await _send_bubble(p)
                if not tail:
                    break
                head, hold = _safe_split_partial(tail)
                if head:
                    await _send_bubble(head)
                    if hold:
                        buf.append(hold)
                        if allow_hard_cut:
                            continue  # re-drain the suffix
                    break
                if allow_hard_cut:
                    await _send_bubble(tail)
                    break
                buf.append(tail)  # hold partial, no safe boundary yet
                break
            last_flush = time.monotonic()

    async def _on_stream(tool_name, tool_input, text_chunk, tool_result) -> None:
        nonlocal last_flush
        if not text_chunk:
            return
        if last_flush is None:
            last_flush = time.monotonic()
        buf.append(text_chunk)
        raw = "".join(buf)
        if "\n\n\n" in raw:
            await _drain(allow_hard_cut=False)
        elif len(raw) >= _STREAM_HARD_MAX:
            await _drain(allow_hard_cut=True)
        elif time.monotonic() - last_flush >= _STREAM_FLUSH_IDLE_S:
            await _drain(allow_hard_cut=False)

    resp = None
    for attempt in range(_WX_MAX_TURN_RECOVERY_ATTEMPTS + 1):
        if attempt:
            buf.clear()
            last_flush = None
            sent_any = False
            had_send_failure = False
            log.warning(
                "Replaying WeChat turn after CC query failure "
                "(attempt %d/%d)",
                attempt,
                _WX_MAX_TURN_RECOVERY_ATTEMPTS,
            )
        try:
            resp = await cc.query(
                combined or "[图片]", images=images or None, on_stream=_on_stream,
            )
            break
        except Exception as e:
            log.exception("CC query failed")
            if attempt < _WX_MAX_TURN_RECOVERY_ATTEMPTS:
                await asyncio.sleep(0)
                continue
            log.error("WeChat turn remains unconsumed after retries: %s", e)
            return False
    if resp is None:
        return False

    # End-of-stream drain — allow_hard_cut=True ships everything still in
    # buf even if no safe boundary exists (rare: nothing more is coming).
    await _drain(allow_hard_cut=True)

    # Rebroadcast resp.content when:
    # - had_send_failure: chunks were permanently lost mid-stream; resending
    #   is the only path V sees them (wx has no edit-message). May dup
    #   already-shipped bubbles but visible > missing.
    # - not sent_any: streaming produced no chunks (CC output came only via
    #   resp.content, not deltas).
    if had_send_failure or not sent_any:
        final = resp.content or ""
        if final:
            had_send_failure = False
            for part in re.split(r"\n{3,}", final):
                await _send_bubble(part)
        elif had_send_failure:
            return False

    if resp.resume_note:
        try:
            await client.send_message(
                from_user, [text_item(resp.resume_note)], context_token=ctx_token,
            )
        except Exception:
            pass

    if ticket:
        try:
            await client.send_typing(from_user, ticket, 2)
        except Exception:
            pass

    return not had_send_failure


def _coalesce_update_msgs(msgs: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    units: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_from = None
    for msg in msgs:
        if msg.get("message_type") != 1:  # USER only (ignore BOT echoes)
            continue
        from_user = msg.get("from_user_id") or ""
        if current and from_user != current_from:
            units.append(current)
            current = []
        current.append(msg)
        current_from = from_user
    if current:
        units.append(current)
    return units


async def _process_wx_units(
    client: WeixinClient,
    account_id: str,
    units: list[list[dict[str, Any]]],
) -> bool:
    batch_ok = True
    processing_tasks: list[asyncio.Task] = []
    for msg_unit in units:
        try:
            if len(msg_unit) == 1:
                result = await _enqueue_inbound_msg(client, msg_unit[0], account_id)
            else:
                result = await _enqueue_inbound_msgs(client, msg_unit, account_id)
            if isinstance(result, asyncio.Task):
                processing_tasks.append(result)
            elif result is False:
                batch_ok = False
        except Exception:
            batch_ok = False
            log.exception("msg enqueue crashed")
    if processing_tasks:
        unique_tasks = list(dict.fromkeys(processing_tasks))
        results = await asyncio.gather(*unique_tasks, return_exceptions=True)
        for result in results:
            if result is False or isinstance(result, Exception):
                batch_ok = False
                if isinstance(result, Exception):
                    log.warning("async wx processing failed: %s", result)
    return batch_ok


async def _replay_pending_wx_updates(
    client: WeixinClient,
    account_id: str,
) -> None:
    for record in await _pending_wx_batches_for_account(account_id):
        record_id = str(record.get("id") or "")
        new_buf = str(record.get("new_buf") or "")
        units = record.get("units")
        if not record_id or not isinstance(units, list):
            log.warning("dropping malformed wx pending record: %s", record_id or "?")
            if record_id:
                await _ack_pending_wx_batch(record_id)
            continue
        if new_buf and load_sync_buf(account_id) == new_buf:
            await _ack_pending_wx_batch(record_id)
            continue
        ok = await _process_wx_units(client, account_id, units)
        if not ok:
            log.warning("wx pending replay still unconsumed: %s", record_id)
            break
        if new_buf:
            save_sync_buf(account_id, new_buf)
        await _ack_pending_wx_batch(record_id)
        log.warning("replayed unconsumed WX update batch: %s", record_id)


# ── login ────────────────────────────────────────────────────────────

def _print_qr(url: str) -> None:
    try:
        import qrcode
        qr = qrcode.QRCode(border=1)
        qr.add_data(url)
        qr.make()
        qr.print_ascii(tty=sys.stdout.isatty(), invert=True)
    except ImportError:
        print("(install qrcode for ASCII QR: .venv/bin/pip install qrcode)")
    print(f"QR URL: {url}")


async def _interactive_login() -> str:
    log.info("requesting QR for new WeChat bot login…")
    qr = await start_qr_login()
    _print_qr(qr.qrcode_url)
    log.info("scan QR above (URL: %s)", qr.qrcode_url)

    def on_refresh(new_qr) -> None:
        log.info("QR refreshed:")
        _print_qr(new_qr.qrcode_url)
        log.info("URL: %s", new_qr.qrcode_url)

    result = await wait_qr_login(qr, on_refresh=on_refresh)
    if not result.connected:
        log.error("login failed: %s", result.message)
        sys.exit(1)

    account_id = normalize_account_id(result.account_id or "")
    if not account_id:
        log.error("login success but no accountId returned")
        sys.exit(1)

    save_account(
        account_id,
        token=result.bot_token or "",
        base_url=result.base_url or "https://ilinkai.weixin.qq.com",
        user_id=result.user_id,
    )
    register_account(account_id)
    if result.user_id:
        add_allow_from(account_id, result.user_id)
        removed = clear_stale_for_user(account_id, result.user_id)
        if removed:
            log.info("cleared %d stale accounts", len(removed))
    log.info("logged in as %s (owner=%s)", account_id, result.user_id)
    return account_id


# ── main loop ────────────────────────────────────────────────────────

async def _run_account(account_id: str) -> None:
    meta = load_account(account_id)
    if not meta:
        log.error("account %s not found in store", account_id)
        return
    client = WeixinClient(
        base_url=meta["baseUrl"],
        token=meta["token"],
        account_id=account_id,
    )
    log.info("long-poll starting for %s", account_id)

    asyncio.create_task(_heartbeat_loop(client, account_id))

    await _replay_pending_wx_updates(client, account_id)
    buf = load_sync_buf(account_id)
    fails = 0

    while True:
        try:
            resp = await client.get_updates(buf)
        except WeixinSessionExpired as e:
            log.error("session expired, pausing 1h: %s", e)
            await asyncio.sleep(3600)
            continue
        except Exception as e:
            fails += 1
            log.warning("getUpdates err (%d): %s", fails, e)
            if fails >= 3:
                await asyncio.sleep(30)
                fails = 0
            else:
                await asyncio.sleep(2)
            continue

        fails = 0
        new_buf = resp.get("get_updates_buf", buf)
        units = _coalesce_update_msgs(resp.get("msgs") or [])
        pending_record_id = ""
        batch_ok = True
        if units:
            try:
                pending_record_id = await _record_pending_wx_batch(
                    account_id,
                    new_buf=new_buf,
                    units=units,
                )
            except Exception:
                batch_ok = False
                log.exception("wx pending-updates write failed")
            if batch_ok:
                batch_ok = await _process_wx_units(client, account_id, units)
        if batch_ok and new_buf != buf:
            save_sync_buf(account_id, new_buf)
            buf = new_buf
        if batch_ok and pending_record_id:
            await _ack_pending_wx_batch(pending_record_id)


# Distinguishes two exit-0 paths so launchd KeepAlive { SuccessfulExit: false }
# only suppresses relaunch for the lock-loser path. SIGTERM/SIGINT from system
# (sleep wake reconcile, OOM, ad-hoc kill) flips this to True → __main__ forces
# exit 1 so launchd respawns. Without it, any graceful signal stays exit 0 and
# the bot stays dead until manual kickstart (2026-05-06 incident, codex review).
_signal_received = False


async def main() -> None:
    ids = list_account_ids()
    force_login = "--login" in sys.argv

    if force_login or not ids:
        account_id = await _interactive_login()
    else:
        account_id = ids[0]
        log.info("using cached account %s (use --login to add another)", account_id)

    await bridge.start()

    stop_event = asyncio.Event()

    def _on_signal() -> None:
        global _signal_received
        log.info("stop signal received…")
        _signal_received = True
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    task = asyncio.create_task(_run_account(account_id))
    try:
        await stop_event.wait()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await bridge.stop()


_SINGLETON_LOCK_PATH = Path("/tmp/babata-weixin.lock")
_singleton_lock_fd = None  # module-global so fd stays open for process lifetime


def _acquire_singleton_lock() -> None:
    """OS-level singleton: one weixin_bot per host.

    Multiple launchd plists / OSS spawn paths / manual double-clicks all
    converge here. Lock holder owns /tmp/babata-weixin-bridge.sock and the
    iLink long-poll cursor. Lock auto-releases on process death (BSD flock).

    Without this guard, weixin_bridge.start() does os.unlink(SOCKET_PATH)
    before bind, so a second instance silently steals the socket from a
    live first instance — both keep long-polling iLink, race-handling each
    inbound, V's WeChat replies get duplicated.

    Loser behavior:
      - normal mode: exit 0. Pair with KeepAlive dict + SuccessfulExit=false
        in any launchd plist so launchd does NOT relaunch on intentional
        bow-out (default KeepAlive=true would spin-loop every ThrottleInterval).
      - --login mode: exit 1 with explicit error (interactive account-add;
        silent exit would confuse the user).

    Open with "a+" not "w" so a loser does not truncate the holder's PID
    breadcrumb before reading it for the log.
    """
    global _singleton_lock_fd
    _singleton_lock_fd = open(_SINGLETON_LOCK_PATH, "a+")
    try:
        fcntl.flock(_singleton_lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        _singleton_lock_fd.seek(0)
        try:
            other = _singleton_lock_fd.read().strip() or "?"
        except Exception:
            other = "?"
        if "--login" in sys.argv:
            log.error(
                "another weixin_bot holds %s (pid=%s) — "
                "stop the running bot before adding an account",
                _SINGLETON_LOCK_PATH, other,
            )
            sys.exit(1)
        log.warning(
            "another weixin_bot holds %s (pid=%s) — exiting",
            _SINGLETON_LOCK_PATH, other,
        )
        sys.exit(0)
    _singleton_lock_fd.seek(0)
    _singleton_lock_fd.truncate()
    _singleton_lock_fd.write(f"{os.getpid()}\n")
    _singleton_lock_fd.flush()


if __name__ == "__main__":
    _acquire_singleton_lock()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped.")
    if _signal_received:
        sys.exit(1)
