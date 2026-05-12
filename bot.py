"""CC TG Bot — thin Telegram transport for Claude Code.

TG is just a channel. The only difference from terminal CC is the wire.
Bot only does what CC physically cannot: TG transport, media conversion, UI feedback.
"""

import asyncio
import html
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from dotenv import load_dotenv

# override=False: plist-injected env (per-instance TELEGRAM_BOT_TOKEN /
# BABATA_INSTANCE / ALLOWED_USER_ID) wins over .env defaults. Without this,
# two bot instances launched from the same repo would all grab the same .env
# token. Must run before importing media (which reads env at import time)
# and before `from constants` (which reads BABATA_INSTANCE / PROJECT_NAMESPACE).
load_dotenv(override=False)

# Namespace / paths derive from PROJECT_NAMESPACE + BABATA_INSTANCE env.
# See constants.py for the full derivation. Propagate BRIDGE_SOCKET to
# bridge.py (imported next) and to tg_mcp subprocess below.
from constants import BRIDGE_SOCKET, INSTANCE, INSTANCE_LABELS, LAUNCHD_PREFIX, PROJECT, SESSION_FILE, STATE_DIR, STATE_FILE
os.environ["BABATA_BRIDGE_SOCKET"] = BRIDGE_SOCKET

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bridge import bridge
from cc import Event, LiveSession, Response
from engine import (
    VENV_PYTHON,
    engine_choices,
    engine_label,
    engine_name,
    is_codex_engine,
    make_engine,
    normalize_engine,
    persist_engine,
)
from media import image_to_base64, transcribe_voice, understand_video
from tg_transcript import install_bot_transcript, record_update, transcript_source

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_USER = int(os.environ.get("ALLOWED_USER_ID", "0"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger(PROJECT)

_TG_MCP_SCRIPT = str(Path(__file__).parent / "tg_mcp.py")

# ── Idempotency: TG update_id 持久化 (无感重启) ────────────────────────
# Every decoded user update is first written to pending-updates. Only after a
# final model response has been delivered do we move update_id to processed and
# remove it from pending. On restart, pending-updates is replayed before normal
# polling so a fetched-but-unfinished TG update is not lost.
# drop_pending_updates=False still helps when Telegram itself can redeliver.
# 边界 case: turn 完成但落盘前崩溃 → 重启重做 turn + 重发 reply (毫秒窗口,
# 物理无解, V "做到物理极限就行").
PROCESSED_UPDATES_FILE = STATE_DIR / f"processed-updates-{INSTANCE}.json"
PENDING_UPDATES_FILE = STATE_DIR / f"pending-updates-{INSTANCE}.json"
_PROCESSED_MAX = 1000  # 滚动窗口

def _load_processed() -> set[int]:
    if not PROCESSED_UPDATES_FILE.exists():
        return set()
    try:
        import json as _json
        return set(_json.loads(PROCESSED_UPDATES_FILE.read_text()).get("done", []))
    except Exception as e:
        log.warning("processed-updates load failed: %s, treating as empty", e)
        return set()

_processed_lock = asyncio.Lock()
_processed_set: set[int] = _load_processed()  # 模块加载即填充 (sync read)

def _load_pending_updates() -> dict[str, dict[str, Any]]:
    if not PENDING_UPDATES_FILE.exists():
        return {}
    try:
        data = json.loads(PENDING_UPDATES_FILE.read_text())
        records = data.get("pending", {})
        if isinstance(records, dict):
            return {str(k): v for k, v in records.items() if isinstance(v, dict)}
        if isinstance(records, list):
            return {
                str(item["update_id"]): item
                for item in records
                if isinstance(item, dict) and item.get("update_id") is not None
            }
    except Exception as e:
        log.warning("pending-updates load failed: %s, treating as empty", e)
    return {}


_pending_updates_lock = asyncio.Lock()
_pending_update_records: dict[str, dict[str, Any]] = _load_pending_updates()


def _write_pending_updates_locked() -> None:
    tmp = PENDING_UPDATES_FILE.with_suffix(".json.partial")
    PENDING_UPDATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(
        json.dumps(
            {"pending": _pending_update_records},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    os.replace(tmp, PENDING_UPDATES_FILE)


async def _ack_pending_update(update_id: int | None) -> None:
    if update_id is None:
        return
    key = str(update_id)
    async with _pending_updates_lock:
        if key not in _pending_update_records:
            return
        _pending_update_records.pop(key, None)
        try:
            _write_pending_updates_locked()
        except Exception as e:
            log.warning("pending-updates ack failed: %s", e)


async def _pending_update_exists(update_id: int | None) -> bool:
    if update_id is None:
        return False
    async with _pending_updates_lock:
        return str(update_id) in _pending_update_records


async def _record_pending_payload(payload: "Payload") -> None:
    update_id = payload.update_id
    if update_id is None:
        return
    chat = payload.update.effective_chat
    msg = payload.update.effective_message
    if chat is None or msg is None:
        return
    record = {
        "update_id": update_id,
        "chat_id": chat.id,
        "message_id": msg.message_id,
        "text": payload.text,
        "images": payload.images or [],
        "received_at": time.time(),
    }
    async with _pending_updates_lock:
        _pending_update_records[str(update_id)] = record
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                _write_pending_updates_locked()
                return
            except Exception as e:
                last_exc = e
                log.warning(
                    "pending-updates write failed (attempt %d/3): %s",
                    attempt + 1,
                    e,
                )
                await asyncio.sleep(0.1)
        raise RuntimeError(f"pending-updates write failed: {last_exc}")

async def _mark_processed(update_id: int | None) -> None:
    if update_id is None:
        return
    async with _processed_lock:
        if update_id in _processed_set:
            await _ack_pending_update(update_id)
            return
        _processed_set.add(update_id)
        if len(_processed_set) > _PROCESSED_MAX:
            keep = sorted(_processed_set, reverse=True)[:_PROCESSED_MAX]
            _processed_set.clear()
            _processed_set.update(keep)
        try:
            import json as _json
            tmp = PROCESSED_UPDATES_FILE.with_suffix(".json.partial")
            tmp.write_text(_json.dumps({"done": sorted(_processed_set)}, indent=2))
            os.replace(tmp, PROCESSED_UPDATES_FILE)
        except Exception as e:
            log.warning("processed-updates write failed: %s", e)
            return
    await _ack_pending_update(update_id)


_TG_SOURCE_PROMPT = (
    "Source: Telegram. "
    "Output Markdown (bot auto-converts to HTML subset: b/i/u/s/code/pre/a/blockquote). "
    "Markdown headings/tables/hr unsupported. "
    "iOS system font: prefer █/░ for progress bars; ▓ renders as noisy stipple. "
    "New bubble: separate paragraphs with three newlines (\\n\\n\\n). "
    "Max 4096 chars/message."
)

def _tg_mcp_servers() -> dict[str, Any]:
    return {
        "tg": {
            "command": VENV_PYTHON,
            "args": [_TG_MCP_SCRIPT],
            # Route the MCP subprocess to this instance's bridge socket. CC CLI
            # merges this with inherited env when spawning stdio MCP servers.
            "env": {"BABATA_BRIDGE_SOCKET": BRIDGE_SOCKET},
        },
    }


def _make_tg_engine(target: str | None = None) -> LiveSession:
    return make_engine(
        state_file=SESSION_FILE,
        source_prompt=_TG_SOURCE_PROMPT,
        mcp_servers=_tg_mcp_servers(),
        live=True,
        engine=target,
    )


def _current_cpu_name() -> str:
    name = getattr(cc, "_babata_engine_name", None)
    if isinstance(name, str) and name:
        return normalize_engine(name)
    return engine_name(SESSION_FILE)


def _bot_commands_for_cpu(cpu: str | None = None) -> list[tuple[str, str]]:
    name = normalize_engine(cpu or _current_cpu_name())
    commands = [
        ("new", "Start a fresh session"),
        ("resume", "Resume a recent session"),
        ("status", "Show model, session, verbose"),
        ("verbose", "Tool display: 0=hidden 1=flash 2=keep"),
        ("cpu", "Switch assistant CPU"),
        ("restart", "Restart this bot process"),
    ]
    if name == "claude":
        commands.insert(3, ("context", "Context usage breakdown"))
        commands.insert(6, ("stop", "Interrupt current turn"))
        commands.append(("provider", "切换 Anthropic 渠道"))
    else:  # codex — cmd_provider line 3075 分支切 codex_accounts
        commands.append(("provider", "切换 Codex 账号"))
    return commands


async def _sync_bot_commands(bot_obj: Any) -> None:
    try:
        await bot_obj.set_my_commands(_bot_commands_for_cpu())
    except Exception as e:
        log.warning("bot command sync failed: %s", e)


cc = _make_tg_engine()
_channel_worker: "ChannelWorker | None" = None

# ── Graceful shutdown ─────────────────────────────────────────────────
# SIGTERM / SIGINT / /restart 都走 _graceful_shutdown: 若有 CC 任务在跑
# (_in_flight > 0), 先推 TG 告知, 等跑完再退. launchd plist 的 ExitTimeOut
# 必须调高 (默认 20s, babata 设 600s) 否则 SIGKILL 会强杀.
#
# Live worker 当前是否有 turn 在跑。PTB handler 现在只 enqueue; 真正需要
# graceful drain 的是后台 worker 从首条 user input 到 ResultMessage 的区间.
_in_flight = 0                  # active live CC turns
_shutdown_requested = False     # debounce: 第二次信号 → 强退


def _inflight_enter() -> None:
    global _in_flight
    _in_flight += 1


def _inflight_exit() -> None:
    global _in_flight
    _in_flight = max(0, _in_flight - 1)


async def _wait_inflight_drain(poll: float = 0.5) -> None:
    while _in_flight > 0:
        await asyncio.sleep(poll)


# ── Restart reason channel (file-based, one-shot) ─────────────────────
# 触发重启的外部脚本 (auto-update / babata-daily-restart / self-ops /
# poll-healthcheck) 在 kickstart/kill 前向 STATE_DIR/restart-reason-{LABEL}.txt
# 写一行 reason. 本进程在两处消费它 (各自 read+unlink, 一次性):
#   1) graceful shutdown (SIGTERM 路径) → 拼到 "重启中..." TG alert
#   2) 进程 startup → 拼到 "上线" alert (兜底 SIGKILL 路径, graceful 没跑过)
# 任一路径读到都 unlink, 双重报告自然不发生 (file 只存在到第一次读).
# 没文件 = 未指定原因 (KeepAlive 自动拉起 / 异常 crash / 人工 launchctl).
def _self_launchd_label() -> str:
    return f"{LAUNCHD_PREFIX}.{INSTANCE}" if INSTANCE else LAUNCHD_PREFIX


def _pop_restart_reason() -> str | None:
    """Atomic rename-then-read: SIGKILL between read and unlink would otherwise
    leave a stale file that the next startup mis-attributes. By renaming to a
    sibling .consumed path *first*, any crash after rename leaves nothing at
    the canonical path for the next pop to pick up.
    """
    reason_file = STATE_DIR / f"restart-reason-{_self_launchd_label()}.txt"
    consumed = reason_file.with_name(reason_file.name + ".consumed")
    try:
        os.replace(reason_file, consumed)
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("rename restart-reason failed: %s", e)
        return None
    try:
        reason = consumed.read_text(encoding="utf-8").strip()
    except Exception as e:
        log.warning("read consumed restart-reason failed: %s", e)
        reason = None
    try:
        consumed.unlink()
    except Exception as e:
        log.warning("unlink consumed restart-reason failed: %s", e)
    return reason or None


async def _graceful_shutdown(app: "Application", reason: str) -> None:
    """Wait for the live turn, notify V via TG, then exit."""
    global _shutdown_requested
    if _shutdown_requested:
        log.warning("Second shutdown signal (%s), force exit", reason)
        os._exit(1)
    _shutdown_requested = True
    log.info("Graceful shutdown requested: %s (in_flight=%d)", reason, _in_flight)

    # One-shot read: external trigger 写的具体原因 (e.g. SDK 升级版本号).
    # 没文件 → 未指定 (KeepAlive 拉起 / 人工 launchctl). startup alert 也读
    # 同一路径, 但本路径先 unlink 后, 进程死前的 startup 不会再读到.
    trigger = _pop_restart_reason() or "未指定 (KeepAlive 拉起 / 异常 / 人工 launchctl)"

    if _in_flight > 0 and ALLOWED_USER:
        try:
            await app.bot.send_message(
                ALLOWED_USER,
                f"[{_CURRENT_LABEL}] {reason} ({trigger}) · 等 {_in_flight} 个任务跑完再重启",
            )
        except Exception as e:
            log.warning("Pre-shutdown notice failed: %s", e)

    await _wait_inflight_drain()

    if _channel_worker is not None:
        try:
            await _channel_worker.stop()
        except Exception as e:
            log.warning("Channel worker stop failed: %s", e)

    if ALLOWED_USER:
        try:
            await app.bot.send_message(
                ALLOWED_USER,
                f"[{_CURRENT_LABEL}] 重启中... ({trigger}, ~10s 回来)",
            )
        except Exception as e:
            log.warning("Shutdown notice failed: %s", e)

    # 让 TG round-trip 送出消息再死
    await asyncio.sleep(0.5)
    log.warning("Graceful shutdown complete, exiting pid=%d", os.getpid())
    os._exit(0)


def _install_signal_handlers(app: "Application") -> None:
    """覆盖 PTB 默认 SIGTERM/SIGINT handler, 走 graceful 路径.

    必须在 run_polling(stop_signals=None) 下调用, 否则 PTB 会注册自己的 handler
    立即停止, graceful 逻辑没机会执行.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(
                _graceful_shutdown(app, reason=f"收到 {s.name}")
            ),
        )

# ── Heartbeat (双 bot 互监控, 零 LLM 成本) ──────────────────────────
# 自己每 30s touch; 主 TG bot (INSTANCE="") 监控微信心跳, stale > 3 min 告警
# 一次 (对方 fresh 后允许再告). vvv/vvvv 只写不监控 — 避免重复告警.

_HEARTBEAT_ME = STATE_DIR / f"{PROJECT}-tg{'-' + INSTANCE if INSTANCE else ''}-heartbeat"
_HEARTBEAT_PEER = STATE_DIR / f"{PROJECT}-weixin-heartbeat"
_HEARTBEAT_STALE_S = 180
_HEARTBEAT_INTERVAL_S = 30


async def _heartbeat_loop(app: "Application") -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    alerted = False
    is_primary = not INSTANCE
    while True:
        try:
            _HEARTBEAT_ME.touch()
            if is_primary and _HEARTBEAT_PEER.exists():
                age = time.time() - _HEARTBEAT_PEER.stat().st_mtime
                if age > _HEARTBEAT_STALE_S and not alerted and ALLOWED_USER:
                    try:
                        await app.bot.send_message(
                            ALLOWED_USER,
                            f"⚠️ 微信 bot 心跳已 {int(age)}s 未更新 (阈值 {_HEARTBEAT_STALE_S}s)",
                        )
                        alerted = True
                    except Exception as e:
                        log.warning("heartbeat alert send failed: %s", e)
                elif age <= 60:
                    alerted = False
        except Exception as e:
            log.warning("heartbeat loop error: %s", e)
        await asyncio.sleep(_HEARTBEAT_INTERVAL_S)


# User preferences persisted across restarts.
_STATE_PATH = STATE_FILE


def _load_state() -> dict:
    try:
        return json.loads(_STATE_PATH.read_text())
    except Exception:
        return {}


_BOT_STATE_KEYS = {
    "verbose",
    "session_cost",
    "session_turns",
    "last_cost",
    "last_used_tokens",
    "last_model",
    "last_context_window",
    "last_session_id",
}


def _save_state() -> None:
    try:
        merged = _load_state()
        for key in _BOT_STATE_KEYS:
            if key in _state:
                merged[key] = _state[key]
        _STATE_PATH.write_text(json.dumps(merged))
    except Exception:
        pass


_state = _load_state()
# Tool display mode: 0=hidden, 1=show then delete, 2=show and keep
_verbose = int(_state.get("verbose", 1))

# /status accounting. Per-session accumulators — reset on /new via cc.reset().
# Persisted to state.json so /status survives bot restarts (otherwise the
# first /status after kickstart shows "(no turn yet)").
_session_cost = float(_state.get("session_cost", 0.0))
_session_turns = int(_state.get("session_turns", 0))
_last_model: str | None = _state.get("last_model")
_last_context_window: int | None = _state.get("last_context_window")
_last_used_tokens: int = int(_state.get("last_used_tokens", 0))
_last_cost: float = float(_state.get("last_cost", 0.0))
# Session id that produced the values above. /status uses _last_used_tokens
# only as a fallback when JSONL hasn't flushed yet; cross-session leak (idle
# reset spawns a new sid while state still carries the previous session's
# inflated model_usage aggregate) was the source of "317% of 1M" right after
# bot restart.
_last_session_id: str | None = _state.get("last_session_id")

# ── Formatting (physical: TG requires HTML, max 4096 chars) ──────────

# 4000 not 4096: leaves headroom for HTML entity expansion (`&` → `&amp;`)
# and the (1/N) chunk indicator suffix added by `_split`.
_MAX_TG = 4000

# Stream edit throttle bases (seconds). adaptive backoff in `_handle_text_delta`
# / `_handle_tool_event` doubles the interval on every flood-control failure
# up to _MAX_EDIT_INTERVAL; reset to base on a successful edit.
_TEXT_EDIT_INTERVAL_BASE = 2.0
_TOOL_EDIT_INTERVAL_BASE = 2.0
_MAX_EDIT_INTERVAL = 10.0
# Hermes uses 3 strikes (`stream_consumer.py:_MAX_FLOOD_STRIKES`); same default
# here. After N consecutive flood failures we stop trying to edit and let
# turn-end's `_deliver_response` send the rest as a fresh reply_text.
_MAX_FLOOD_STRIKES = 3
# A broken CPU stream is recoverable: reconnect the CPU and replay the active
# V message before continuing later cut-ins. After this many immediate retries,
# the turn remains unconsumed and queued, but later retries are logged as a
# persistent fault instead of being treated as a consumed user update.
_MAX_TURN_RECOVERY_ATTEMPTS = int(os.environ.get("BABATA_TURN_RECOVERY_ATTEMPTS", "2"))

_TOOL_EMOJI = {
    "Bash": "\U0001f4bb",           # 💻 terminal
    "Read": "\U0001f4d6",           # 📖 book
    "Write": "\u270d\ufe0f",        # ✍️ writing hand
    "Edit": "\U0001f527",           # 🔧 patch
    "MultiEdit": "\U0001f527",      # 🔧
    "Glob": "\U0001f4c2",           # 📂 file match
    "Grep": "\U0001f50d",           # 🔍 content search
    "WebFetch": "\U0001f4c4",       # 📄 page fetch
    "WebSearch": "\U0001f310",      # 🌐 web search
    "Task": "\U0001f500",           # 🔀 delegate
    "TodoWrite": "\u2705",          # ✅
    "NotebookEdit": "\U0001f4d3",   # 📓
    "Skill": "\U0001f4da",          # 📚 skill library
    "ToolSearch": "\U0001f9f0",     # 🧰 toolbox
}

def _fmt_tool(name: str, inp: dict) -> str:
    if name.startswith("mcp__"):
        parts = name.split("__", 2)
        display = f"{parts[1]}/{parts[2]}" if len(parts) >= 3 else name
        emoji = "\U0001f9e9"  # 🧩 MCP plugin
    else:
        display = name
        emoji = _TOOL_EMOJI.get(name, "\U0001f527")

    preview = ""
    for key in ("command", "cmd", "query", "q", "path", "url", "text"):
        value = inp.get(key)
        if isinstance(value, str) and value:
            preview = value
            break
    if not preview:
        # Avoid surfacing transport/internal fields such as Codex item ids.
        skip = {"id", "type", "status", "name"}
        preview = next(
            (
                str(v)
                for k, v in inp.items()
                if k not in skip and isinstance(v, str) and v
            ),
            "",
        )
    preview = preview.replace("\n", " ").strip()
    if not preview:
        return f"{emoji} {display}"
    if len(preview) > 40:
        preview = preview[:40] + "..."
    return f'{emoji} {display}: "{preview}"'


def _to_html(md: str) -> str:
    """Best-effort markdown → TG HTML (the tags TG actually accepts).

    TG's HTML parse_mode ONLY supports: b/i/u/s, code, pre (+ language via
    nested code class), a, blockquote, tg-spoiler. Headings / lists / tables /
    hr are NOT real tags — they must degrade to something TG renders.

    Strategy:
      1. Preserve code blocks, heading-as-bold, blockquote, links as opaque
         placeholders BEFORE html.escape so their inner content escapes
         correctly and outer regexes don't mangle them.
      2. html.escape the remaining plain text so stray <>& become entities.
      3. Run inline replacements on escaped text (backtick / ** / * / ~~).
      4. Restore placeholders.
    """
    if not md:
        return ""

    blocks: list[str] = []

    def _park(html_fragment: str) -> str:
        blocks.append(html_fragment)
        return f"\x00BLK{len(blocks) - 1}\x00"

    # 1a. Fenced code blocks (first — highest precedence, opaque)
    def _save_code(m: re.Match) -> str:
        lang = m.group(1) or ""
        code = html.escape(m.group(2))
        return _park(
            f'<pre><code class="language-{lang}">{code}</code></pre>' if lang
            else f"<pre>{code}</pre>"
        )
    text = re.sub(r"```(\w*)\n(.*?)```", _save_code, md, flags=re.DOTALL)

    # 1b. Pre-existing TG-compatible HTML inline tags. CC may write them
    # directly (e.g. `<b>核心</b>` instead of `**核心**`); we park them so
    # later `html.escape` doesn't turn `<b>` into `&lt;b&gt;` (which TG
    # would render as literal text — exactly the bug V hit 2026-05-06).
    # Single-level only (no nested tag parsing) — adequate for chat.
    # Order matters: longer tag names first so `<strong>` matches before
    # the regex would otherwise try `<s>` on the same span.
    # Keep `u/ins/tg-spoiler` (no markdown equivalent) + add `b/strong/
    # i/em/s/strike/del/code` (have markdown equivalent but model may
    # emit raw HTML directly).
    _RAW_TAGS = (
        "tg-spoiler", "strong", "strike", "code", "ins", "del",
        "em", "b", "i", "s", "u",
    )
    for _raw_tag in _RAW_TAGS:
        def _make_raw_saver(t: str):
            def _save(m: re.Match) -> str:
                return _park(f"<{t}>{html.escape(m.group(1))}</{t}>")
            return _save
        text = re.sub(
            rf"<{_raw_tag}>([^<]*)</{_raw_tag}>",
            _make_raw_saver(_raw_tag),
            text,
        )

    # 1c. Inline backtick code — PARK (not just replace) so later bold/italic
    # regexes can't reach the `**` or `*` inside. Without this, `\`**粗**\``
    # gets turned into <code>**粗**</code>, then bold regex chews through the
    # <code> tag and produces <code><b>粗</b></code> which breaks TG parsing
    # entirely and falls back to plain text.
    def _save_inline_code(m: re.Match) -> str:
        inner = html.escape(m.group(1))
        return _park(f"<code>{inner}</code>")
    text = re.sub(r"`([^`\n]+)`", _save_inline_code, text)

    # 1c. Markdown headings `# / ## / ###...` — TG has no heading tag, degrade
    # to <b> on its own line. Single-line form: `^#+ whatever` until EOL.
    def _save_heading(m: re.Match) -> str:
        inner = html.escape(m.group(2).strip())
        return _park(f"<b>{inner}</b>")
    text = re.sub(r"(?m)^(#{1,6})[ \t]+(.+?)[ \t]*#*[ \t]*$", _save_heading, text)

    # 1d. Blockquote: consecutive `> line` merged into one <blockquote>
    def _save_bq(m: re.Match) -> str:
        raw = m.group(0)
        lines = [re.sub(r"^\s*>\s?", "", l) for l in raw.split("\n")]
        inner = html.escape("\n".join(lines).strip())
        return _park(f"<blockquote>{inner}</blockquote>")
    text = re.sub(r"(?m)(^[ \t]*>.*(?:\n[ \t]*>.*)*)", _save_bq, text)

    # 1e. Markdown links [text](url). Escape both pieces so user-provided `<`
    # can't break the HTML. href gets quote-escape too for the attribute.
    def _save_link(m: re.Match) -> str:
        label = html.escape(m.group(1))
        href = html.escape(m.group(2), quote=True)
        return _park(f'<a href="{href}">{label}</a>')
    text = re.sub(r"\[([^\]\n]+)\]\(([^)\n]+)\)", _save_link, text)

    # 2. Escape everything else
    text = html.escape(text)

    # 3. Inline emphasis on escaped text. All code/tag/link content is already
    # parked, so these regexes only touch real prose.
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text)

    # 4. Restore placeholders (NUL + 'BLK' + digits survives html.escape intact)
    for i, blk in enumerate(blocks):
        text = text.replace(f"\x00BLK{i}\x00", blk)
    return text.strip()


def _utf16_len(s: str) -> int:
    """UTF-16 code-unit count.

    Telegram measures the 4096 message limit in UTF-16 code units, not Unicode
    code-points: emoji / CJK Extension B / surrogate-pair characters consume
    2 units each even though Python `len()` counts them as 1. Hermes pattern,
    ported from `gateway/platforms/base.py:utf16_len`.
    """
    return len(s.encode("utf-16-le")) // 2


def _utf16_to_cp(s: str, budget: int) -> int:
    """Largest codepoint offset n where utf16_len(s[:n]) <= budget. Binary search."""
    if _utf16_len(s) <= budget:
        return len(s)
    lo, hi = 0, len(s)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _utf16_len(s[:mid]) <= budget:
            lo = mid
        else:
            hi = mid - 1
    return lo


def _split(text: str) -> list[str]:
    """Split long message at safe boundaries, UTF-16 aware.

    Borrows hermes pattern (`gateway/platforms/base.py:truncate_message`):
      - Measures length in UTF-16 (TG's wire unit, not codepoints)
      - Closes/reopens ``` code blocks across chunks (`carry_lang`)
      - Avoids splitting inside inline backtick spans (parity check)
      - Appends `(1/N)` indicators when split

    Single message ≤ 4096 wire units returns unchanged. The HTML produced by
    `_to_html` survives this (block placeholders are already restored before
    `_split` runs, so no html-aware logic needed here — only markdown fences).
    """
    if _utf16_len(text) <= _MAX_TG:
        return [text]

    INDICATOR_RESERVE = 10  # " (XX/XX)"
    FENCE_CLOSE = "\n```"

    parts: list[str] = []
    remaining = text
    carry_lang: str | None = None  # set when previous chunk ended mid-code-block

    while remaining:
        prefix = f"```{carry_lang}\n" if carry_lang is not None else ""
        headroom = _MAX_TG - INDICATOR_RESERVE - _utf16_len(prefix) - _utf16_len(FENCE_CLOSE)
        if headroom < 1:
            headroom = _MAX_TG // 2

        if _utf16_len(prefix) + _utf16_len(remaining) <= _MAX_TG - INDICATOR_RESERVE:
            parts.append(prefix + remaining)
            break

        cp_limit = _utf16_to_cp(remaining, headroom)
        # Defensive: if a single character (e.g. surrogate-pair emoji) exceeds
        # headroom, _utf16_to_cp returns 0. Without this floor we'd emit empty
        # chunks and infinite-loop. Force at least 1 codepoint progress; the
        # resulting chunk overflows budget by ≤1 unit, which TG accepts (the
        # 4096 limit has slack vs our 4000 _MAX_TG). Codex round-1 caught.
        if cp_limit < 1:
            cp_limit = 1
        region = remaining[:cp_limit]
        split_at = region.rfind("\n")
        if split_at < cp_limit // 2:
            split_at = region.rfind(" ")
        if split_at < 1:
            split_at = cp_limit

        # HTML attribute-space protection (round-2 follow-up): _to_html emits
        # `<code class="language-python">` with spaces *inside* the opening
        # tag. The space-rfind above can land at the space between `<code`
        # and `class=`, producing a 5-char fragment '<code' followed by a
        # malformed continuation. If split_at falls inside an unclosed
        # `<...>` (last `<` after last `>` before split_at), back off to
        # before that `<`.
        last_lt = remaining.rfind("<", 0, split_at)
        last_gt = remaining.rfind(">", 0, split_at)
        if last_lt > last_gt and last_lt >= 1:
            split_at = last_lt

        # Inline backtick parity: don't split inside `code` (would orphan tick).
        candidate = remaining[:split_at]
        bt_count = candidate.count("`") - candidate.count("\\`")
        if bt_count % 2 == 1:
            last_bt = candidate.rfind("`")
            while last_bt > 0 and candidate[last_bt - 1] == "\\":
                last_bt = candidate.rfind("`", 0, last_bt)
            if last_bt > 0:
                safe_split = max(candidate.rfind(" ", 0, last_bt), candidate.rfind("\n", 0, last_bt))
                if safe_split > cp_limit // 4:
                    split_at = safe_split

        # HTML <pre>/<code> tag pairing (Codex round-1 fix 4a, refined round-2):
        # _to_html converts ``` fences to `<pre><code class="language-X">...</code></pre>`
        # (with class attribute), and inline backticks to `<code>...</code>`.
        # Naive split mid-tag → malformed HTML → TG plain-text fallback or 400.
        # Back the split off to before any unclosed open tag.
        #
        # Open-tag matching uses the prefix `<code` (no `>`) to catch BOTH
        # bare `<code>` AND class-attributed `<code class="...">` — Codex
        # round-2 caught that bare `<code>` prefix missed the class form.
        # `<pre>` has no class form in `_to_html`, so exact match suffices.
        #
        # Limitation: when the unclosed opener sits at offset 0 of the
        # candidate, no backoff is possible (can't split below 0). The
        # chunk emits malformed HTML and TG falls back to plain-text
        # rendering — visually degraded, not a hard failure.
        candidate = remaining[:split_at]
        for open_pattern, close_tag in (("<pre>", "</pre>"), ("<code", "</code>")):
            opens = candidate.count(open_pattern)
            closes = candidate.count(close_tag)
            if opens > closes:
                pos = candidate.rfind(open_pattern)
                if pos >= 1:
                    safe = max(candidate.rfind("\n", 0, pos), candidate.rfind(" ", 0, pos))
                    split_at = safe if safe > cp_limit // 4 else pos
                    candidate = remaining[:split_at]

        chunk_body = remaining[:split_at]
        remaining = remaining[split_at:].lstrip("\n")
        full_chunk = prefix + chunk_body

        # Walk chunk_body to determine whether we end mid-fence.
        in_code = carry_lang is not None
        lang = carry_lang or ""
        for line in chunk_body.split("\n"):
            stripped = line.strip()
            if stripped.startswith("```"):
                if in_code:
                    in_code = False
                    lang = ""
                else:
                    in_code = True
                    tag = stripped[3:].strip()
                    lang = tag.split()[0] if tag else ""

        if in_code:
            full_chunk += FENCE_CLOSE
            carry_lang = lang
        else:
            carry_lang = None

        parts.append(full_chunk)

    if len(parts) > 1:
        total = len(parts)
        parts = [f"{p} ({i + 1}/{total})" for i, p in enumerate(parts)]
    return parts


def _format_bubble_parts(text: str) -> tuple[list[str], str | None]:
    """Return TG-safe message chunks plus parse_mode.

    Telegram rejects malformed HTML, and `_split()` is intentionally plain-text
    oriented. For long bubbles, prefer reliable plain delivery over trying to
    split rendered HTML across tag boundaries.
    """
    bubble = text.strip()
    if not bubble:
        return [], None
    html_text = _to_html(bubble)
    if html_text and _utf16_len(html_text) <= _MAX_TG:
        return [html_text], "HTML"
    return _split(bubble), None


def _parse_kwargs(parse_mode: str | None) -> dict[str, str]:
    return {"parse_mode": parse_mode} if parse_mode else {}


# ── Auth (physical: access control) ──────────────────────────────────

def _allowed(update: Update) -> bool:
    # ALLOWED_USER 必须显式设置 — 默认 deny 防止开源用户首次跑没配置就开放给陌生人.
    if not ALLOWED_USER:
        return False
    return bool(update.effective_user and update.effective_user.id == ALLOWED_USER)


async def _callback_allowed(query: Any) -> bool:
    """Authorize callback-query handlers before they mutate bot state."""
    user = getattr(query, "from_user", None)
    if ALLOWED_USER and user and getattr(user, "id", None) == ALLOWED_USER:
        return True
    if query is not None:
        with suppress(Exception):
            await query.answer("auth denied")
    return False


@dataclass
class Payload:
    update: Update
    ctx: ContextTypes.DEFAULT_TYPE
    text: str
    images: list[dict[str, str]] | None = None
    update_id: int | None = None  # idempotency: turn end 时 _mark_processed


def _format_coalesced_tg_prompt(payloads: list[Payload]) -> str:
    if len(payloads) <= 1:
        return payloads[0].text if payloads else ""

    blocks = [
        "The user sent these follow-up Telegram messages while the previous "
        "turn was running.",
        "Treat them as one user turn, ordered oldest to newest. Later messages "
        "may clarify or supersede earlier messages.",
        "",
    ]
    for idx, payload in enumerate(payloads, start=1):
        msg = payload.update.effective_message
        meta = [f"n={idx}"]
        if payload.update_id is not None:
            meta.append(f"update_id={payload.update_id}")
        if msg is not None and getattr(msg, "message_id", None) is not None:
            meta.append(f"message_id={msg.message_id}")
        if payload.images:
            meta.append(f"images={len(payload.images)}")
        text = payload.text.strip() or "[empty message]"
        blocks.append(f"<user_message {' '.join(meta)}>\n{text}\n</user_message>")
        blocks.append("")
    return "\n".join(blocks).strip()


class _ReplayChat:
    def __init__(self, bot_obj: Any, chat_id: int) -> None:
        self._bot = bot_obj
        self.id = chat_id

    async def send_action(self, action: str) -> None:
        await self._bot.send_chat_action(chat_id=self.id, action=action)


class _ReplayMessage:
    def __init__(self, bot_obj: Any, chat_id: int, message_id: int) -> None:
        self._bot = bot_obj
        self.chat_id = chat_id
        self.message_id = message_id
        self.text = None
        self.caption = None
        self.reply_to_message = None
        self.document = None
        self.photo = None
        self.voice = None
        self.audio = None

    async def reply_text(self, text: str, parse_mode=None, reply_markup=None):
        kwargs: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text,
            "reply_to_message_id": self.message_id,
        }
        if parse_mode:
            kwargs["parse_mode"] = parse_mode
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        return await self._bot.send_message(**kwargs)


class _ReplayUpdate:
    def __init__(
        self,
        *,
        update_id: int,
        bot_obj: Any,
        chat_id: int,
        message_id: int,
    ) -> None:
        self.update_id = update_id
        self.effective_chat = _ReplayChat(bot_obj, chat_id)
        self.effective_message = _ReplayMessage(bot_obj, chat_id, message_id)
        self.message = self.effective_message
        self.effective_user = None


def _payload_from_pending_record(bot_obj: Any, record: dict[str, Any]) -> Payload | None:
    try:
        update_id = int(record["update_id"])
        chat_id = int(record["chat_id"])
        message_id = int(record["message_id"])
        text = str(record["text"])
    except (KeyError, TypeError, ValueError):
        return None
    images = record.get("images") or None
    if images is not None and not isinstance(images, list):
        images = None
    update = _ReplayUpdate(
        update_id=update_id,
        bot_obj=bot_obj,
        chat_id=chat_id,
        message_id=message_id,
    )
    ctx = SimpleNamespace(bot=bot_obj, application=None)
    return Payload(update=update, ctx=ctx, text=text, images=images, update_id=update_id)


class ChannelWorker:
    """Per-process TG channel worker for one long-lived LiveSession."""

    def __init__(self, session: LiveSession, *, instance_label: str) -> None:
        self.session = session
        self.instance_label = instance_label
        self._consume_task: asyncio.Task[None] | None = None
        self._state_lock = asyncio.Lock()
        self._turn_active = False
        self._turn_payload: Payload | None = None
        # P1.4: most-recent submitted payload; used as a fallback anchor when
        # _turn_payload was reset by turn_end before the next turn's events
        # land (race between submit() and _handle_turn_end acquiring _state_lock).
        self._latest_payload: Payload | None = None
        self._last_user_msg_id: int | None = None
        self._turn_anchor: int | None = None
        self._tool_status: Any | None = None
        self._tool_entries: list[str] = []
        self._tool_last_edit = 0.0
        self._text_message: Any | None = None
        self._text_buffer = ""
        self._text_last_edit = 0.0
        self._streamed_bubble_count = 0
        self._stale_text_messages: list[Any] = []
        # Flood-control state per stream lane (text + tool). Hermes pattern
        # (`stream_consumer.py:943-976`): on edit() failure, classify as flood
        # / non-flood; flood → adaptive backoff (×2, cap 10s) + strike count;
        # ≥ MAX_FLOOD_STRIKES or non-flood → enter fallback (clear message
        # ref so turn-end's _deliver_response sends rest as a fresh reply).
        # Reset alongside _text_message / _tool_status on turn boundary.
        self._text_edit_supported = True
        self._text_flood_strikes = 0
        self._text_edit_interval = _TEXT_EDIT_INTERVAL_BASE
        self._tool_edit_supported = True
        self._tool_flood_strikes = 0
        self._tool_edit_interval = _TOOL_EDIT_INTERVAL_BASE
        self._stopping = False  # set on graceful shutdown to break supervisor loop
        # 消息状态 reaction: 👀 = SDK 开始处理这条 / 👌 = 这条触发的 turn 已结束.
        # _pending_marks: submit 后等下个 _begin_turn 接管 (push 到 active_marks 并打 👀)
        # _active_marks: 当前 turn 已 picked_up; turn_end 时打 👌 并清空
        # 每条 mark = (bot, chat_id, message_id). bot 用 Any 因为 PTB Bot 实例 (含 ctx.bot)
        self._pending_marks: list[tuple[Any, int, int]] = []
        # idempotency: 跟 _pending_marks / _active_marks 同步, turn_end 时
        # _mark_processed all (per-V-msg 标 done, 多 V msg batch 一个 turn 全标).
        self._pending_update_ids: list[int | None] = []
        self._active_update_ids: list[int | None] = []
        self._active_marks: list[tuple[Any, int, int]] = []
        self._reaction_tasks: set[asyncio.Task[None]] = set()
        # P2-A: 串行所有 reaction API 调用, 保证 schedule 顺序 = 执行顺序
        # (turn_end finally 先 schedule 👌 再触发 _begin_turn → 👀, lock FIFO
        # 保证 V 看到的最终 reaction 是 👌 不是 👀).
        self._reaction_lock = asyncio.Lock()
        # P1-A/B: anchor generation token. submit / _begin_turn 切 anchor 时 +1;
        # _handle_text_delta / _handle_tool_event 在 await 边界检查, 变了就 abort
        # 防 stale write 覆盖新 anchor 状态.
        self._anchor_generation: int = 0
        # 流式输出 reply 的 anchor — 跟 _turn_payload 解耦.
        # _turn_payload 是 SDK turn 边界 anchor (P1.4 promote 用); 一个 turn 可能
        # 跨多条 V 消息. _active_reply_payload 是 V 视角的 "当前活跃消息" — 每条
        # V message 进来都切到自己, 让流式输出和 tool 状态走新 reply, 不混到上
        # 一条的 reply 上 (即使 SDK 把多条 batch 成一个 turn 也能保持 per-message
        # reply 体验).
        self._active_reply_payload: Payload | None = None
        self._turn_payload_batch: list[Payload] = []
        # Cut-in 队列 (interrupt 模式): V 流式中再发 → SDK interrupt + append 到这.
        # _handle_turn_end 末尾 pop 出来 _begin_turn 启动新 turn. submit() 不再
        # 改 anchor 状态 (留给老 turn 自然收尾), V 看到的 in-flight 气泡保留.
        self._pending_payloads: list[Payload] = []
        # Recoverable CPU stream failures replay the active payload before
        # continuing pending cut-ins. This counter is per active user turn and
        # resets on a successful turn_end or explicit reset/resume.
        self._turn_recovery_attempts = 0
        # Codex round-2 P0: turn epoch token. _begin_turn / _reset_turn_state
        # 每次 bump. _spawn_interrupt capture 当时 epoch, 实际 fire interrupt 前
        # check epoch 没变才继续 — 防 fire-and-forget interrupt 因 race 打断错的
        # 后续 turn (例: A 自然完 → _begin_turn(B) → 老 interrupt 命中 B).
        self._turn_epoch: int = 0
        # Codex round-4 P2 (dedupe): 同 epoch 内多 cut-in (V 连发 m2 m3 m4) 别
        # 重复 spawn interrupt — SDK round-trip 浪费 + log noise. epoch bump 后
        # 自然对不上, 重置.
        self._interrupt_spawned_epoch: int = -1

    async def start(self) -> None:
        await self.session.connect()
        self._consume_task = asyncio.create_task(self._consume_events())

    async def stop(self) -> None:
        self._stopping = True
        await self.session.close()
        if self._consume_task:
            try:
                await asyncio.wait_for(self._consume_task, timeout=5)
            except asyncio.TimeoutError:
                self._consume_task.cancel()
                try:
                    await self._consume_task
                except asyncio.CancelledError:
                    pass
        # 等 in-flight reaction tasks 跑完 (避免 asyncio Task was destroyed warning).
        # 短超时: reaction 是 fire-and-forget, 卡了直接放弃.
        if self._reaction_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._reaction_tasks, return_exceptions=True),
                    timeout=2,
                )
            except (asyncio.TimeoutError, Exception):
                pass

    async def submit(self, payload: Payload, *, interrupt_active: bool = True) -> None:
        chat = payload.update.effective_chat
        msg = payload.update.effective_message
        if chat is None or msg is None:
            return

        # bridge.set_context 移到 _begin_turn (Codex round-1 P1): cut-in 不该
        # 改 reply_to, 否则 A turn 中的 mcp_send 会带 B 的 message_id reply,
        # B 的 turn 拿不到 anchor (turn_end finally 清 reply_to=None).
        self._last_user_msg_id = msg.message_id

        async with self._state_lock:
            if payload.text.strip() == "/new" and not payload.images:
                await self._handle_reset(payload)
                await _mark_processed(payload.update_id)
                return

            # P1.4: always update latest_payload so a mid-turn submit (which
            # doesn't trigger _begin_turn) still leaves a valid anchor for the
            # next SDK turn if the race between submit and turn_end leaves
            # _turn_payload unset.
            self._latest_payload = payload
            # 消息状态: append + 立即 fire 👀 — V 视角"看到这条了". 切入消息也即时
            # 拿到 ack, 不再等下个 _begin_turn (_begin_turn 不再 fire 👀, 只挪账).
            new_mark = (payload.ctx.bot, chat.id, msg.message_id)
            self._pending_marks.append(new_mark)
            self._pending_update_ids.append(payload.update_id)
            self._schedule_marks([new_mark], "👀")
            if self._turn_active:
                # Cut-in (interrupt 模式 + R3 P0 fix): V 流式中再发. 保留老 turn
                # 状态 — 已 ship 气泡 + in-flight 气泡都不动 (V 看见 A 半句留在 A
                # 下面). interrupt 让 SDK 尽快收尾老 turn. **不**立即 session.submit(B)
                # — B 留在 _pending_payloads, 等 A's turn_end → _begin_turn(B) 时才
                # 入 SDK inbox. 否则 stale interrupt 可能命中 B (race) + SDK batch
                # 模式卡死 (m1+m2 合并 ResultMessage 但我们 promote m2 等不到第二
                # turn_end). codex R3 P0 验证.
                self._pending_payloads.append(payload)
                if self._turn_active and interrupt_active:
                    self._spawn_interrupt()
            else:
                self._active_reply_payload = payload
                self._begin_turn(payload, batch_payloads=[payload])
                await self._submit_to_session(payload)

    async def has_update_id(self, update_id: int | None) -> bool:
        if update_id is None:
            return False
        async with self._state_lock:
            known = [
                getattr(self._turn_payload, "update_id", None),
                getattr(self._active_reply_payload, "update_id", None),
                getattr(self._latest_payload, "update_id", None),
            ]
            known.extend(p.update_id for p in self._pending_payloads)
            known.extend(self._active_update_ids)
            known.extend(self._pending_update_ids)
            return update_id in known

    async def interrupt(self) -> None:
        await self.session.interrupt()

    async def resume(self, sid: str) -> bool:
        async with self._state_lock:
            if self._turn_active:
                await self._surface_error(RuntimeError("会话已切换。"))
            # P2-D: drop_pending — resume 后 inbox drain (LiveSession.resume_live
            # 内部 _stop_client_locked 会 drain), pending V messages 永远不会被
            # 处理, 不能让它们的 mark 被下次 _begin_turn promote.
            # 💔: 让 V 看到这些 V msg 被中止, reaction 状态不卡 👀.
            self._reset_turn_state(
                exit_inflight=True, drop_pending=True, fail_emoji="💔"
            )
            self._turn_recovery_attempts = 0
            return await self.session.resume_live(sid)

    async def _handle_reset(self, payload: Payload) -> None:
        if self._turn_active:
            await self._surface_error(RuntimeError("会话已重置。"))
        # P2-D: 同 resume — /new drain 后 pending marks 失效.
        # 💔: 标记这些 V msg 为"未完成" (区别于 turn_end 的 👌).
        self._reset_turn_state(
            exit_inflight=True, drop_pending=True, fail_emoji="💔"
        )
        self._turn_recovery_attempts = 0
        resp = await self.session.reset_live()
        await self._deliver_response(payload, resp)
        self._apply_accounting(resp)

    def _session_supports_hot_input(self) -> bool:
        return bool(getattr(self.session, "supports_hot_input", True))

    def _coalesce_payloads_for_turn(self, payloads: list[Payload]) -> Payload:
        if len(payloads) <= 1:
            return payloads[0]
        anchor = payloads[-1]
        images: list[dict[str, str]] = []
        for payload in payloads:
            images.extend(payload.images or [])
        return Payload(
            update=anchor.update,
            ctx=anchor.ctx,
            text=_format_coalesced_tg_prompt(payloads),
            images=images or None,
            update_id=None,
        )

    def _pop_next_pending_turn(self) -> tuple[Payload, list[Payload]] | None:
        if not self._pending_payloads:
            return None
        if self._session_supports_hot_input():
            payloads = [self._pending_payloads.pop(0)]
        else:
            payloads = list(self._pending_payloads)
            self._pending_payloads = []
        return self._coalesce_payloads_for_turn(payloads), payloads

    async def _start_next_pending_locked(self) -> None:
        if self._turn_active:
            return
        next_turn = self._pop_next_pending_turn()
        if next_turn is None:
            return
        next_payload, batch_payloads = next_turn
        self._begin_turn(next_payload, batch_payloads=batch_payloads)
        await self._submit_to_session(next_payload)

    def _begin_turn(
        self,
        payload: Payload,
        *,
        batch_payloads: list[Payload] | None = None,
    ) -> None:
        batch_payloads = list(batch_payloads or [payload])
        msg = payload.update.effective_message
        self._turn_active = True
        self._turn_payload = payload
        self._turn_payload_batch = batch_payloads
        self._latest_payload = payload  # keep latest in sync
        # P1-A/B: 切 anchor 时 +1 generation. P1.4 promote 路径走这里, 也要 bump.
        self._anchor_generation += 1
        # Codex round-2 P0: bump turn_epoch — invalidate 老 cut-in 还没 fire 的
        # interrupt task (它在 _spawn_interrupt 内 check, epoch 变了直接 return).
        self._turn_epoch += 1
        self._active_reply_payload = payload  # 同步切 reply anchor
        # Codex round-1 P1: bridge anchor 跟 turn 走 — cut-in 不动, _begin_turn
        # 时切到当前 turn 的 V msg, 让 mcp_send (cron 等) reply 到对的 message.
        chat = payload.update.effective_chat
        if msg is not None and chat is not None:
            bridge.set_context(payload.ctx.bot, chat.id, msg.message_id)
        self._turn_anchor = msg.message_id if msg else None
        if self._stale_text_messages:
            self._spawn_orphan_cleanup(self._stale_text_messages)
        self._tool_status = None
        self._tool_entries = []
        self._tool_last_edit = 0.0
        self._text_message = None
        self._text_buffer = ""
        self._text_last_edit = 0.0
        self._streamed_bubble_count = 0
        self._stale_text_messages = []
        self._reset_flood_state()
        # 消息状态 promote: pending → active. 👀 已在 submit fire (每条 V msg
        # 立即 ack), 这里只挪账, 不再 fire — 避免重复 setMessageReaction 调用.
        # Hot-input sessions keep one V message per queued turn. Non-hot
        # sessions (Codex CLI) may coalesce several pending messages into one
        # physical model turn; all their marks/uids move together.
        promote_count = min(
            len(batch_payloads),
            len(self._pending_marks),
            len(self._pending_update_ids),
        )
        if promote_count:
            self._active_marks = self._pending_marks[:promote_count]
            self._active_update_ids = self._pending_update_ids[:promote_count]
            del self._pending_marks[:promote_count]
            del self._pending_update_ids[:promote_count]
        if promote_count != len(batch_payloads):
            log.warning(
                "turn batch/mark mismatch: batch=%d marks=%d uids=%d",
                len(batch_payloads),
                len(self._pending_marks),
                len(self._pending_update_ids),
            )
        _inflight_enter()

    async def _consume_events(self) -> None:
        """P1.2 supervisor: re-establish the LiveSession when events() exits
        on un-recovered error so V's next message still gets processed instead
        of vanishing into a dead inbox.
        """
        backoff = 1.0
        while not self._stopping:
            try:
                async for ev in self.session.events():
                    if ev.kind == "text_delta":
                        await self._handle_text_delta(ev.chunk or "")
                    elif ev.kind in ("tool_use", "tool_result"):
                        await self._handle_tool_event(ev)
                    elif ev.kind == "turn_end" and ev.response:
                        await self._handle_turn_end(ev.response)
                    elif ev.kind == "session_changed":
                        log.info(
                            "Session changed: %s -> %s",
                            (ev.old_sid or "")[:8],
                            (ev.new_sid or "")[:8],
                        )
                    elif ev.kind == "error":
                        await self._handle_error(
                            ev.exception or RuntimeError("CC stream error")
                        )
                        break  # events() will exit after error; supervisor reconnects
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("ChannelWorker consume loop crashed: %s", e)
                # Make sure turn-bound state doesn't leak across reconnect
                async with self._state_lock:
                    if self._turn_active:
                        self._requeue_active_turn_for_recovery()
                        self._reset_turn_state(
                            exit_inflight=True,
                            mark_processed=False,
                        )

            if self._stopping:
                return
            # Reconnect with backoff. session is marked closed after un-recovered
            # error, so connect() will start a fresh CLI subprocess.
            try:
                await self.session.connect()
                backoff = 1.0
                await self._start_next_pending_after_reconnect()
            except Exception as e:
                log.warning("LiveSession reconnect failed: %s; retry in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _reset_flood_state(self) -> None:
        """Reset flood-control / fallback state at every turn / cut-in boundary.

        Called from `_begin_turn`, cut-in path (line ~750), and `_reset_turn_state`.
        Without reset the counters would carry strikes from one turn into the
        next, prematurely disabling edits when only the previous turn hit
        flood control.
        """
        self._text_edit_supported = True
        self._text_flood_strikes = 0
        self._text_edit_interval = _TEXT_EDIT_INTERVAL_BASE
        self._tool_edit_supported = True
        self._tool_flood_strikes = 0
        self._tool_edit_interval = _TOOL_EDIT_INTERVAL_BASE

    @staticmethod
    def _is_flood_error(exc: BaseException) -> bool:
        """Classify a TG edit/send failure as flood-control vs other.

        PTB raises `telegram.error.RetryAfter` for HTTP 429 with retry_after
        attribute (always flood). Some flood errors come back as generic
        `BadRequest` with text containing flood-related keywords — match
        those too. Hermes uses the same string fallback
        (`stream_consumer.py:_is_flood_error`).

        Codex round-1 caught a false negative: bare "Too Many Requests"
        without "rate" / "retry after" / "flood" was misclassified as
        non-flood, prematurely tripping the strike counter into fallback.
        Adding "too many requests" closes that gap.
        """
        try:
            from telegram.error import RetryAfter
            if isinstance(exc, RetryAfter):
                return True
        except ImportError:
            pass
        msg = str(exc).lower()
        return (
            "flood" in msg
            or "retry after" in msg
            or "rate" in msg
            or "too many requests" in msg
        )

    def _on_text_edit_failure(self, exc: BaseException) -> None:
        """Adaptive backoff + 3-strike fallback (hermes pattern).

        Flood error: ×2 interval (cap _MAX_EDIT_INTERVAL), increment strike.
        ≥ _MAX_FLOOD_STRIKES OR non-flood: enter fallback — drop _text_message
        ref so turn-end's `_deliver_response` sends remaining content as a
        fresh reply_text rather than retrying edits forever.
        """
        if self._is_flood_error(exc):
            self._text_flood_strikes += 1
            self._text_edit_interval = min(
                self._text_edit_interval * 2, _MAX_EDIT_INTERVAL,
            )
            if self._text_flood_strikes < _MAX_FLOOD_STRIKES:
                return
        # Non-flood OR strikes exhausted → fallback. Clear ref so the turn-end
        # _deliver_response branch (line ~1099, "_text_message is None") routes
        # final content via reply_text instead of edit_text.
        self._text_edit_supported = False
        self._text_message = None

    def _on_tool_edit_failure(self, exc: BaseException) -> None:
        """Same as `_on_text_edit_failure` but for the tool-status lane."""
        if self._is_flood_error(exc):
            self._tool_flood_strikes += 1
            self._tool_edit_interval = min(
                self._tool_edit_interval * 2, _MAX_EDIT_INTERVAL,
            )
            if self._tool_flood_strikes < _MAX_FLOOD_STRIKES:
                return
        self._tool_edit_supported = False
        self._tool_status = None

    async def _handle_text_delta(self, chunk: str) -> None:
        if not chunk:
            return
        # 优先用 _active_reply_payload (V 视角"当前活跃消息" — 每条 V msg 都切到
        # 自己, 即使 SDK 还没 turn_end 也能让新消息有独立 reply).
        # P1.4 fallback: turn_payload / latest_payload 兜底 (race 窗口).
        # P1-A: snapshot generation 入口, 每次 await 边界检查; 变了就 abort
        # 避免 stale write 覆盖新 anchor 状态.
        gen = self._anchor_generation
        payload = (
            self._active_reply_payload
            or self._turn_payload
            or self._latest_payload
        )
        if payload is None:
            return
        chat = payload.update.effective_chat
        msg = payload.update.effective_message
        if chat is None or msg is None:
            return

        self._text_buffer += chunk

        # Fallback mode (3 prior flood strikes): drop streaming edits entirely.
        # Buffer stays accumulated so turn-end _deliver_response can ship it
        # (it splits on \n\n\n itself).
        if not self._text_edit_supported:
            return

        # \n\n\n marker = LLM-driven bubble break (see _TG_SOURCE_PROMPT).
        # Close the current bubble (final-edit it) and start a fresh one for
        # the next chunks. Loop handles multiple markers in one delta (rare).
        # streamed_bubble_count tracks bubbles successfully shipped — only
        # increments on real send/edit success so _deliver_response can resend
        # any that flood-failed mid-stream. Long bubbles are delivered as plain
        # chunks because rendered HTML cannot be split safely at arbitrary
        # Telegram boundaries.
        while "\n\n\n" in self._text_buffer:
            prefix, tail = self._text_buffer.split("\n\n\n", 1)
            prefix = prefix.rstrip()
            sent_ok = False
            shipped_msgs: list[Any] = []
            if prefix:
                if gen != self._anchor_generation:
                    return
                parts, parse_mode = _format_bubble_parts(prefix)
                if not parts:
                    self._text_buffer = tail.lstrip("\n")
                    continue
                first_part = parts[0]
                rest = parts[1:]
                if self._text_message is not None:
                    edit_ok = False
                    try:
                        await self._text_message.edit_text(
                            first_part, **_parse_kwargs(parse_mode)
                        )
                        edit_ok = True
                    except Exception:
                        # HTML may be rejected; retry the whole bubble as plain.
                        try:
                            raw_parts = _split(prefix) or [prefix[:_MAX_TG]]
                            await self._text_message.edit_text(raw_parts[0])
                            rest = raw_parts[1:]
                            parse_mode = None
                            edit_ok = True
                        except Exception as plain_e:
                            if gen == self._anchor_generation:
                                self._stale_text_messages.append(self._text_message)
                                self._on_text_edit_failure(plain_e)
                    if edit_ok and gen == self._anchor_generation:
                        self._text_flood_strikes = 0
                        self._text_edit_interval = _TEXT_EDIT_INTERVAL_BASE
                        sent_ok = True
                        shipped_msgs.append(self._text_message)
                else:
                    reply_ok = False
                    new_msg = None
                    try:
                        new_msg = await msg.reply_text(
                            first_part, **_parse_kwargs(parse_mode)
                        )
                        reply_ok = True
                    except Exception:
                        try:
                            raw_parts = _split(prefix) or [prefix[:_MAX_TG]]
                            new_msg = await msg.reply_text(raw_parts[0])
                            rest = raw_parts[1:]
                            parse_mode = None
                            reply_ok = True
                        except Exception as plain_e:
                            if gen == self._anchor_generation:
                                self._on_text_edit_failure(plain_e)
                    if reply_ok:
                        if gen != self._anchor_generation:
                            self._spawn_orphan_cleanup([new_msg])
                        else:
                            sent_ok = True
                            shipped_msgs.append(new_msg)
                if sent_ok and rest:
                    rest_failed = False
                    for p in rest:
                        rp = None
                        try:
                            rp = await msg.reply_text(
                                p, **_parse_kwargs(parse_mode)
                            )
                        except Exception:
                            if parse_mode:
                                try:
                                    rp = await msg.reply_text(p)
                                except Exception:
                                    rest_failed = True
                                    break
                            else:
                                rest_failed = True
                                break
                        if rp is not None:
                            shipped_msgs.append(rp)
                    if rest_failed:
                        # Whole-bubble rollback: stale-queue every shipped part
                        # so _deliver_response can resend the bubble cleanly.
                        self._stale_text_messages.extend(shipped_msgs)
                        sent_ok = False
            # P0 gen check: close-bubble state writes must not pollute new anchor.
            if gen != self._anchor_generation:
                # shipped_msgs are now orphan replies under the old anchor.
                if shipped_msgs:
                    self._spawn_orphan_cleanup(shipped_msgs)
                return
            self._text_message = None
            self._text_buffer = tail.lstrip("\n")
            self._text_last_edit = 0.0
            if sent_ok:
                self._streamed_bubble_count += 1

        now = time.monotonic()
        if not self._text_buffer:
            return
        if len(self._text_buffer) <= _MAX_TG:
            display = self._text_buffer
        else:
            display = "…" + self._text_buffer[-(_MAX_TG - 1):]

        if self._text_message is None:
            try:
                new_reply = await msg.reply_text(display or "…")
            except Exception as e:
                # First-send flood: classify and possibly enter fallback so we
                # don't pound TG with retries. Tail still in _text_buffer →
                # _deliver_response covers it at turn end.
                # Gen-check before mutating shared state: cut-in may have
                # bumped generation while reply_text awaited; this failure
                # belongs to the now-stale anchor, don't poison new state.
                # Codex round-1 fix.
                if gen == self._anchor_generation:
                    self._on_text_edit_failure(e)
                return
            # P1-A: gen 没变才装回 _text_message. 变了说明 submit 已切到新 anchor,
            # 这条 reply 在旧 m_old 下是 orphan — 删掉, 让 V 只看到 m_new 下的新
            # reply (符合 V spec "切入后回复在对应消息下面"; 留着会让 V 看到 m_old
            # 下的 "…"/half-chunk 残留, 跟终端体感不齐).
            if gen == self._anchor_generation:
                self._text_message = new_reply
                self._text_last_edit = now
            else:
                self._spawn_orphan_cleanup([new_reply])
            return

        # Adaptive interval: doubles on each flood strike, resets to base on
        # successful edit (handled below). _text_edit_interval is per-stream
        # state set by `_on_text_edit_failure`.
        if now - self._text_last_edit < self._text_edit_interval:
            return
        # P1-A: 在 edit await 前再查一次 gen — 变了就 abort, 不把新 chunk
        # 写到旧 reply (chunk 在 V 视角属于新 anchor).
        if gen != self._anchor_generation:
            return
        self._text_last_edit = now
        target = self._text_message
        try:
            await target.edit_text(display)
            # Gen-check after await: if cut-in landed while edit awaited,
            # this success belongs to the old anchor — don't reset *new*
            # anchor's flood state with this stale outcome. Codex round-1.
            if gen == self._anchor_generation:
                self._text_flood_strikes = 0
                self._text_edit_interval = _TEXT_EDIT_INTERVAL_BASE
        except Exception as e:
            if gen == self._anchor_generation:
                self._on_text_edit_failure(e)
        try:
            await chat.send_action("typing")
        except Exception:
            pass

    async def _handle_tool_event(self, ev: Event) -> None:
        if _verbose == 0:
            return
        # 同 _handle_text_delta: per-message reply anchor 优先, 让 tool 状态也
        # 跟着新 V 消息走 (不混到上一条 reply 链).
        # P1-B: gen check 同 _handle_text_delta.
        gen = self._anchor_generation
        payload = (
            self._active_reply_payload
            or self._turn_payload
            or self._latest_payload
        )
        if payload is None:
            return
        chat = payload.update.effective_chat
        msg = payload.update.effective_message
        if chat is None or msg is None:
            return

        if ev.kind == "tool_use" and ev.name:
            self._tool_entries.append(_fmt_tool(ev.name, ev.input_dict or {}))
        elif ev.kind == "tool_result" and ev.is_error:
            err = (ev.text or "").replace("\n", " ").strip()
            if not err:
                return
            self._tool_entries.append(f"  ❌ {err[:200]}")
        else:
            return

        body = "\n".join(self._tool_entries[-30:])[:_MAX_TG]
        # Fallback mode: stop editing tool_status. Tool entries continue to
        # accumulate in _tool_entries (V loses live progress, but turn-end
        # still shows the work via _deliver_response's resp.tools / final
        # response text). We don't try to flush tool entries as a fresh
        # message because they're transient by design (verbose=1 deletes
        # them on _deliver_response anyway).
        if not self._tool_edit_supported:
            return
        if self._tool_status is None:
            try:
                new_status = await msg.reply_text(body)
            except Exception as e:
                # Gen-check (Codex round-1): same race as _handle_text_delta.
                if gen == self._anchor_generation:
                    self._on_tool_edit_failure(e)
                return
            # P1-B 同 _handle_text_delta: gen mismatch = orphan tool_status 在 m_old
            # 下, 删掉避免 V 在错误 anchor 下看到孤立 tool 状态.
            if gen == self._anchor_generation:
                self._tool_status = new_status
                self._tool_last_edit = time.monotonic()
            else:
                self._spawn_orphan_cleanup([new_status])
            return

        now = time.monotonic()
        if now - self._tool_last_edit < self._tool_edit_interval:
            return
        if gen != self._anchor_generation:
            return
        self._tool_last_edit = now
        target = self._tool_status
        try:
            await target.edit_text(body)
            if gen == self._anchor_generation:
                self._tool_flood_strikes = 0
                self._tool_edit_interval = _TOOL_EDIT_INTERVAL_BASE
        except Exception as e:
            if gen == self._anchor_generation:
                self._on_tool_edit_failure(e)
        try:
            await chat.send_action("typing")
        except Exception:
            pass

    async def _handle_turn_end(self, resp: Response) -> None:
        async with self._state_lock:
            # P1.4: race window — if submit() acquired _state_lock between SDK's
            # ResultMessage and consume_events reaching here, _turn_payload may
            # already point at a newer Payload. Either way, fall back to
            # _latest_payload so V never sees a silent drop.
            # P1-C/D: 优先用 _active_reply_payload (V 视角"当前活跃") — final
            # response 落到 V 最新 message 的 reply, 跟流式期间的 _text_message
            # 同 anchor; 否则 long-response overflow parts 会跨两个 anchor 分裂.
            payload = (
                self._active_reply_payload
                or self._turn_payload
                or self._latest_payload
            )
            # P1.3: try/finally guarantees turn state resets even if TG edits
            # raise — otherwise _in_flight stays >0 and graceful shutdown hangs.
            try:
                if payload is None:
                    log.warning(
                        "turn_end without any payload anchor: sid=%s", resp.session_id
                    )
                    self._apply_accounting(resp)
                    return
                try:
                    await self._deliver_response(payload, resp)
                except Exception as e:
                    log.exception("deliver_response failed: %s", e)
                self._apply_accounting(resp)
                if resp.cost > 0:
                    log.info(
                        "Cost: $%.4f | Session: %s",
                        resp.cost,
                        resp.session_id[:8] if resp.session_id else "new",
                    )
                self._turn_recovery_attempts = 0
            finally:
                # 只 fire 👌 给 _active_marks (= 当前 turn 的 V msgs). _pending_marks
                # 是 cut-in 期间 push 的 (= 下一 turn 的 V msgs), 留着等 _begin_turn
                # promote 进 _active_marks. SDK 真 batch 多 V msg 进一个 turn 的话,
                # _pending_marks 在那时本就是空 (submit 只在 turn_active 才 push 到
                # pending). 所以这里只动 active 是对的.
                done_marks = self._active_marks
                done_uids = self._active_update_ids
                self._active_marks = []
                self._active_update_ids = []
                if done_marks:
                    self._schedule_marks(done_marks, "👌")
                for _uid in done_uids:
                    await _mark_processed(_uid)
                self._reset_turn_state(exit_inflight=True)
                # Turn 结束 → 清 bridge.reply_to. 不清的话, V turn 之后 cron
                # 走 mcp__tg__tg_send_* 发的消息 (gmail PR-merged 通报 / weekly
                # report / X 日报...) 都会带上 V 上一条 message_id 当 reply_to,
                # TG 渲染成"引用 V 上一条". chat_id 保留 — cron 还得知道发哪
                # 个 chat. 只清 reply_to.
                with suppress(Exception):
                    bridge.reply_to = None
                # Cut-in interrupt 模式 (R3 P0 fix): pending payloads 启动下一
                # turn. _begin_turn 内 promote _pending_marks → _active_marks,
                # 设新 anchor + bump gen + epoch. 然后 _submit_to_session 才把
                # B 推入 SDK inbox — cut-in 时不 submit, 这里才 submit, 防 stale
                # interrupt + SDK batch 卡死.
                if self._pending_payloads:
                    await self._start_next_pending_locked()

    async def _handle_error(self, exc: Exception) -> None:
        log.error("CC stream failed: %s", exc)
        async with self._state_lock:
            replaying = False
            if self._turn_recovery_attempts < _MAX_TURN_RECOVERY_ATTEMPTS:
                replaying = self._requeue_active_turn_for_recovery()
                if replaying:
                    self._turn_recovery_attempts += 1
                    log.warning(
                        "Replaying active turn after stream error "
                        "(attempt %d/%d); pending=%d",
                        self._turn_recovery_attempts,
                        _MAX_TURN_RECOVERY_ATTEMPTS,
                        len(self._pending_payloads),
                    )
                    # The active/pending TG updates are still live work. Do not
                    # mark them processed and do not mark reactions failed; after
                    # reconnect, _start_next_pending_after_reconnect() will replay
                    # N and then continue N+1...
                    self._reset_turn_state(
                        exit_inflight=True,
                        mark_processed=False,
                    )
            if replaying:
                return

            # Still no final response after bounded live retries. This is not
            # consumed: keep N at the front, keep N+1... behind it, and let the
            # reconnect supervisor try again after the underlying fault clears.
            if self._requeue_active_turn_for_recovery():
                log.error(
                    "Active TG turn remains unconsumed after %d recovery attempts; "
                    "will retry after reconnect",
                    self._turn_recovery_attempts,
                )
                self._reset_turn_state(exit_inflight=True, mark_processed=False)
                return

            await self._surface_error(exc)
            self._reset_turn_state(
                exit_inflight=True,
                drop_pending=False,
                fail_emoji="💔",
                mark_processed=False,
            )

    async def _start_next_pending_after_reconnect(self) -> None:
        """After a supervised reconnect, resume the queued FIFO work.

        This is the recovery actuator for a broken CPU stream: _handle_error()
        puts the active V message back at the front of _pending_payloads, then
        the supervisor reconnects the physical session and calls here.
        """
        async with self._state_lock:
            if self._turn_active or not self._pending_payloads:
                return
            await self._start_next_pending_locked()

    def _requeue_active_turn_for_recovery(self) -> bool:
        """Put the active V message back before pending cut-ins.

        Returns False when there is no reliable active payload anchor. Must be
        called with _state_lock held.
        """
        payloads = list(self._turn_payload_batch)
        if not payloads:
            payload = self._turn_payload or self._active_reply_payload
            payloads = [payload] if payload is not None else []
        if not payloads:
            return False
        self._pending_payloads = payloads + self._pending_payloads
        if self._active_marks:
            self._pending_marks = list(self._active_marks) + self._pending_marks
        if self._active_update_ids:
            self._pending_update_ids = (
                list(self._active_update_ids) + self._pending_update_ids
            )
        for orphan in (self._text_message, self._tool_status):
            if orphan is not None:
                self._stale_text_messages.append(orphan)
        return True

    async def _surface_error(self, exc: Exception) -> None:
        text = f"Error: {exc}"
        surfaced = False
        for target in (self._text_message, self._tool_status):
            if target is None:
                continue
            try:
                await target.edit_text(text)
                surfaced = True
                break
            except Exception:
                continue
        if surfaced:
            return
        # P2-C: 用 _active_reply_payload 优先 (errors 应落 V 最新 message reply,
        # 不是旧 turn anchor).
        payload = (
            self._active_reply_payload
            or self._turn_payload
            or self._latest_payload
        )
        if payload and payload.update.effective_message:
            try:
                await payload.update.effective_message.reply_text(text)
            except Exception:
                pass

    async def _deliver_response(self, payload: Payload, resp: Response) -> None:
        msg = payload.update.effective_message
        if msg is None:
            return

        if self._tool_status and _verbose == 1:
            try:
                await self._tool_status.delete()
            except Exception:
                pass

        if resp.resume_note:
            try:
                await msg.reply_text(resp.resume_note)
            except Exception:
                pass

        # Delete stale partials early — must run before any return path so
        # they don't stay visible if resp.content is empty.
        if self._stale_text_messages:
            for stale in self._stale_text_messages:
                try:
                    await stale.delete()
                except Exception:
                    pass
            self._stale_text_messages = []

        if not resp.content:
            await msg.reply_text("(no response)")
            return

        # Streaming has already shipped `_streamed_bubble_count` bubbles via
        # reply_text in _handle_text_delta; _text_message (if any) holds the
        # trailing partial. Compute what's still pending from the final.
        bubbles = re.split(r"\n{3,}", resp.content)
        # Drop leading + trailing empty bubbles (LLM may bracket with markers).
        while bubbles and not bubbles[-1].strip():
            bubbles.pop()
        while bubbles and not bubbles[0].strip():
            bubbles.pop(0)
        pending = bubbles[self._streamed_bubble_count:]
        if not pending:
            return

        async def _send_bubble(bubble: str) -> None:
            bubble = bubble.strip()
            if not bubble:
                return
            parts, parse_mode = _format_bubble_parts(bubble)
            for part in parts:
                try:
                    await msg.reply_text(part, **_parse_kwargs(parse_mode))
                except Exception:
                    if parse_mode:
                        for pp in _split(bubble):
                            await msg.reply_text(pp)
                    break

        if self._text_message:
            # First pending bubble = trailing partial in _text_message; finalize.
            target = pending[0]
            target_text = target.strip()
            parts, parse_mode = _format_bubble_parts(target_text)
            if parts:
                try:
                    await self._text_message.edit_text(
                        parts[0], **_parse_kwargs(parse_mode)
                    )
                except Exception:
                    try:
                        raw_parts = _split(target_text)
                        await self._text_message.edit_text(raw_parts[0])
                        for pp in raw_parts[1:]:
                            await msg.reply_text(pp)
                        parts = []
                    except Exception:
                        pass
                for part in parts[1:]:
                    try:
                        await msg.reply_text(part, **_parse_kwargs(parse_mode))
                    except Exception:
                        if parse_mode:
                            await msg.reply_text(part)
            for bubble in pending[1:]:
                await _send_bubble(bubble)
        else:
            for bubble in pending:
                await _send_bubble(bubble)

    def _apply_accounting(self, resp: Response) -> None:
        global _session_cost, _session_turns, _last_model, _last_context_window
        global _last_used_tokens, _last_cost, _last_session_id
        if not resp.session_id and resp.cost == 0.0 and not resp.tools:
            _session_cost = 0.0
            _session_turns = 0
            _last_used_tokens = 0
            _last_cost = 0.0
        else:
            _session_cost += resp.cost
            _session_turns += 1
            _last_cost = resp.cost
            if resp.model:
                _last_model = resp.model
            if resp.context_window:
                _last_context_window = resp.context_window
            _last_used_tokens = (
                resp.input_tokens
                + resp.cache_creation_tokens
                + resp.cache_read_tokens
            )
            if resp.session_id:
                _last_session_id = resp.session_id

        _state["session_cost"] = _session_cost
        _state["session_turns"] = _session_turns
        _state["last_cost"] = _last_cost
        _state["last_used_tokens"] = _last_used_tokens
        if _last_model:
            _state["last_model"] = _last_model
        if _last_context_window:
            _state["last_context_window"] = _last_context_window
        if _last_session_id:
            _state["last_session_id"] = _last_session_id
        _save_state()

    def _reset_turn_state(
        self,
        *,
        exit_inflight: bool,
        drop_pending: bool = False,
        fail_emoji: str | None = None,
        mark_processed: bool = True,
    ) -> None:
        was_active = self._turn_active
        self._turn_active = False
        self._turn_payload = None
        self._turn_payload_batch = []
        self._turn_anchor = None
        if self._stale_text_messages:
            self._spawn_orphan_cleanup(self._stale_text_messages)
        self._tool_status = None
        self._tool_entries = []
        self._tool_last_edit = 0.0
        self._text_message = None
        self._text_buffer = ""
        self._text_last_edit = 0.0
        self._streamed_bubble_count = 0
        self._stale_text_messages = []
        self._reset_flood_state()
        # 失败路径 (error / reset / resume / submit retry fail): fire 💔 给
        # active + (drop 模式下) pending 让 V 一眼看到这些 V message 没正常完成.
        # turn_end 路径不传 fail_emoji — finally 段已经手动 fire 过 👌.
        if fail_emoji:
            failed = list(self._active_marks)
            if drop_pending:
                failed.extend(self._pending_marks)
            if failed:
                self._schedule_marks(failed, fail_emoji)
        self._active_marks = []
        self._active_reply_payload = None
        # P1-A/B: bump generation 让 in-flight handler 看到 stale.
        self._anchor_generation += 1
        # Codex round-2 P1: bump turn_epoch 让 in-flight interrupt task 看到 stale
        # (reset/error 路径 client 可能已 dead, 老 interrupt 命中重置后的 client 危险).
        self._turn_epoch += 1
        # P2-D: reset/resume 可以显式丢弃 pending；transport/CPU 故障路径必须
        # pass mark_processed=False, 因为没有最终回复就不算 consumed.
        # turn_end 路径不清 pending marks, 留给 P1.4 promote 给下个 SDK turn 用.
        if mark_processed:
            self._schedule_mark_processed(self._active_update_ids)
        self._active_update_ids = []
        if drop_pending:
            if mark_processed:
                self._schedule_mark_processed(self._pending_update_ids)
            self._pending_update_ids = []
            self._pending_marks = []
            self._pending_payloads = []
        if exit_inflight and was_active:
            _inflight_exit()

    def _schedule_marks(self, marks: list[tuple[Any, int, int]], emoji: str) -> None:
        """Fire-and-forget: 给一组 (bot, chat_id, msg_id) 打 reaction emoji."""
        if not marks:
            return
        task = asyncio.create_task(self._fire_marks(list(marks), emoji))
        self._reaction_tasks.add(task)
        task.add_done_callback(self._reaction_tasks.discard)

    def _schedule_mark_processed(self, uids: list[int | None]) -> None:
        """Fire-and-forget mark processed: sync-callable async persistence for
        explicit abort paths such as /new or /resume."""
        valid = [u for u in uids if u is not None]
        if not valid:
            return
        async def _do():
            for u in valid:
                await _mark_processed(u)
        task = asyncio.create_task(_do())
        self._reaction_tasks.add(task)
        task.add_done_callback(self._reaction_tasks.discard)

    async def _submit_to_session(self, payload: Payload) -> None:
        """Submit payload to LiveSession with reconnect retry. 二次失败 → reset
        turn state + 💔. 从 submit() else 分支 + _handle_turn_end finally 调.
        Codex R3 P0 fix: cut-in 路径不再立即 submit, 等 A's turn_end 后由
        _begin_turn(B) 触发本函数 — 防 stale interrupt 命中 B + SDK batch 卡死."""
        try:
            self.session.submit(payload.text, payload.images)
            return
        except RuntimeError:
            log.warning("LiveSession was disconnected; reconnecting before submit")
        try:
            await self.session.connect()
            self.session.submit(payload.text, payload.images)
        except Exception as e:
            log.error("Second submit failed: %s — keeping V message pending", e)
            self._requeue_active_turn_for_recovery()
            self._reset_turn_state(
                exit_inflight=True,
                mark_processed=False,
            )

    def _spawn_interrupt(self) -> None:
        """Fire-and-forget SDK interrupt (cut-in 用). 不持 _state_lock 的 await,
        让 A 的 turn_end 不被 block. 失败 → 降级 queue 模式 (A 自然完, B 等).

        Codex round-2 epoch guard: capture self._turn_epoch, fire interrupt 前
        check epoch 没变. 防 race: A 自然完 → _begin_turn(B) bumps epoch → 老
        interrupt 此时 fire 会打断 B 而不是 A. epoch != captured → 直接 return.
        reset/error 也 bump epoch, 防 interrupt 命中 dead/reset client.

        Codex round-4 P2 dedupe: 同 epoch 已 spawn 过 → return (V 连发 m2 m3 m4
        在 m1 turn 内, 三次都触发 cut-in, 但 SDK 只需 interrupt 一次)."""
        captured_epoch = self._turn_epoch
        if self._interrupt_spawned_epoch == captured_epoch:
            return
        self._interrupt_spawned_epoch = captured_epoch
        async def _do() -> None:
            await asyncio.sleep(0)  # yield: 让 A 的自然 turn_end 有机会 fire
            if self._turn_epoch != captured_epoch:
                return
            try:
                await self.session.interrupt()
            except Exception as e:
                log.warning("session.interrupt() failed: %s", e)
        task = asyncio.create_task(_do())
        self._reaction_tasks.add(task)
        task.add_done_callback(self._reaction_tasks.discard)

    def _spawn_orphan_cleanup(self, msgs: list[Any]) -> None:
        """Fire-and-forget delete of orphan TG messages (cut-in / race cleanup).
        复用 _reaction_tasks 集合, 让 stop() 能等清理完成 (避免 Task destroyed warning).
        """
        if not msgs:
            return
        async def _delete_all() -> None:
            for m in msgs:
                try:
                    await m.delete()
                except Exception as e:
                    log.debug("orphan reply delete failed: %s", e)
        task = asyncio.create_task(_delete_all())
        self._reaction_tasks.add(task)
        task.add_done_callback(self._reaction_tasks.discard)

    async def _fire_marks(
        self,
        marks: list[tuple[Any, int, int]],
        emoji: str,
    ) -> None:
        # P2-A: 全局串行所有 reaction API 调用. asyncio.Lock FIFO 保证 schedule
        # 顺序 = 执行顺序 — turn_end finally 先 schedule 👌(active) 再触发
        # _begin_turn → 👀(next), V 看到的最终 reaction 一定是后者覆盖前者.
        # 没这个 lock, 两个 create_task 并发可能让快的 👀 覆盖慢的 👌, 那条
        # 消息卡在 👀 永远不变 👌 (TG setMessageReaction 是 last-write-wins).
        async with self._reaction_lock:
            for bot_obj, chat_id, msg_id in marks:
                try:
                    await bot_obj.set_message_reaction(
                        chat_id=chat_id,
                        message_id=msg_id,
                        reaction=emoji,
                    )
                except Exception as e:
                    # TG API throttling / message too old / bot 无权限 — 不影响主流程
                    log.debug(
                        "set_message_reaction(%s) %s/%s failed: %s",
                        emoji, chat_id, msg_id, e,
                    )


def _worker() -> ChannelWorker:
    if _channel_worker is None:
        raise RuntimeError("ChannelWorker is not started")
    return _channel_worker


async def _replay_pending_updates(app: Application) -> None:
    async with _pending_updates_lock:
        records = list(_pending_update_records.values())
    if not records:
        return

    def _sort_key(record: dict[str, Any]) -> tuple[float, int]:
        try:
            received = float(record.get("received_at") or 0)
        except (TypeError, ValueError):
            received = 0.0
        try:
            update_id = int(record.get("update_id") or 0)
        except (TypeError, ValueError):
            update_id = 0
        return (received, update_id)

    replayed = 0
    for record in sorted(records, key=_sort_key):
        try:
            update_id = int(record.get("update_id"))
        except (TypeError, ValueError):
            continue
        if update_id in _processed_set:
            await _ack_pending_update(update_id)
            continue
        payload = _payload_from_pending_record(app.bot, record)
        if payload is None:
            log.warning("pending update record is malformed: update_id=%s", update_id)
            continue
        try:
            await _worker().submit(payload, interrupt_active=False)
            replayed += 1
        except Exception as e:
            log.warning("pending update replay failed: update_id=%s: %s", update_id, e)
    if replayed:
        log.warning("replayed %d unconsumed TG update(s) from pending journal", replayed)


# ── Handlers ──────────────────────────────────────────────────────────

async def cmd_verbose(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    labels = {0: "hidden", 1: "flash", 2: "keep"}
    buttons = [[
        InlineKeyboardButton(
            f"{'> ' if _verbose == i else ''}{v}",
            callback_data=f"verbose:{i}",
        ) for i, v in labels.items()
    ]]
    await update.message.reply_text(
        "Tool display:", reply_markup=InlineKeyboardMarkup(buttons),
    )


_ALIAS_WINDOW_RE = re.compile(r"\[(\d+)([mk])\]", re.I)


def _infer_window_from_alias(alias: str) -> int | None:
    """Parse CC model alias suffix: 'opus[1m]' → 1_000_000, '[200k]' → 200_000."""
    if not alias:
        return None
    m = _ALIAS_WINDOW_RE.search(alias)
    if not m:
        return None
    n = int(m.group(1))
    return n * 1_000_000 if m.group(2).lower() == "m" else n * 1_000


def _scan_recent_session_model() -> str | None:
    """Grep the most recent session JSONL for message.model. Gives resolved
    model name (e.g. 'claude-opus-4-N') even before this bot instance has run
    a turn, so /status after a restart shows something specific."""
    try:
        recent = cc._load_state().get("recent_sids") or []
    except Exception:
        recent = []
    proj_dir = Path.home() / ".claude" / "projects" / str(Path.home()).replace("/", "-")
    for sid in recent[:5]:
        fp = proj_dir / f"{sid}.jsonl"
        if not fp.is_file():
            continue
        try:
            for line in reversed(fp.read_text().splitlines()):
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                msg = d.get("message")
                if isinstance(msg, dict):
                    m = msg.get("model")
                    if isinstance(m, str) and m.startswith("claude-"):
                        return m
        except Exception:
            continue
    return None


def _cc_version() -> str:
    """Ask the actual claude binary for its version. Same binary bot spawns per
    query — so this number = what /new will run. Subprocess each call (low-
    frequency command, no cache needed)."""
    cli = os.environ.get("CLAUDE_CLI_PATH") or shutil.which("claude")
    if not cli:
        return "—"
    try:
        r = subprocess.run([cli, "-v"], capture_output=True, text=True, timeout=3)
        # "2.1.112 (Claude Code)" → "2.1.112"
        return r.stdout.strip().split()[0] if r.stdout else "—"
    except Exception:
        return "—"


def _sdk_version() -> str:
    """claude-agent-sdk version from this bot's own venv — the same package
    cc.py imports to spawn queries, so this = what /new will actually use."""
    try:
        import claude_agent_sdk  # already a dep of cc.py
        return getattr(claude_agent_sdk, "__version__", "—") or "—"
    except Exception:
        return "—"


def _codex_cli_path() -> str | None:
    return (
        os.environ.get("BABATA_CODEX_CLI_PATH")
        or os.environ.get("CODEX_CLI_PATH")
        or shutil.which("codex")
    )


def _codex_version() -> str:
    cli = _codex_cli_path()
    if not cli:
        return "—"
    try:
        r = subprocess.run([cli, "--version"], capture_output=True, text=True, timeout=3)
        return r.stdout.strip().split()[-1] if r.stdout else "—"
    except Exception:
        return "—"


def _codex_config() -> dict[str, Any]:
    try:
        import tomllib

        return tomllib.loads((Path.home() / ".codex" / "config.toml").read_text())
    except Exception:
        return {}


def _codex_session_file(sid: str | None) -> Path | None:
    if not sid:
        return None
    root = _codex_sessions_root()
    try:
        matches = list(root.glob(f"**/*{sid}.jsonl"))
    except Exception:
        return None
    if not matches:
        return None
    return max(matches, key=lambda p: p.stat().st_mtime)


def _codex_sessions_root() -> Path:
    return Path.home() / ".codex" / "sessions"


def _codex_model_window(model: str | None) -> int | None:
    if not model:
        return None
    try:
        data = json.loads((Path.home() / ".codex" / "models_cache.json").read_text())
        models = data.get("models") if isinstance(data, dict) else None
        if not isinstance(models, list):
            return None
        for item in models:
            if not isinstance(item, dict):
                continue
            if model in {item.get("slug"), item.get("id")}:
                win = item.get("context_window") or item.get("max_context_window")
                if not win:
                    return None
                pct = item.get("effective_context_window_percent") or 100
                return int(float(win) * float(pct) / 100)
    except Exception:
        return None
    return None


def _codex_event_rate_limits(event: dict[str, Any]) -> dict[str, Any] | None:
    payload = event.get("payload") if isinstance(event, dict) else {}
    if not isinstance(payload, dict) or payload.get("type") != "token_count":
        return None
    rate_limits = event.get("rate_limits") or payload.get("rate_limits")
    return rate_limits if isinstance(rate_limits, dict) else None


def _codex_rate_limit_value(data: dict[str, Any], camel: str, snake: str) -> Any:
    value = data.get(camel)
    if value is None:
        value = data.get(snake)
    return value


def _normalize_codex_rate_limit_window(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    used = _codex_rate_limit_value(value, "usedPercent", "used_percent")
    if used is None:
        return None
    return {
        "used_percent": used,
        "window_minutes": _codex_rate_limit_value(value, "windowDurationMins", "window_minutes"),
        "resets_at": _codex_rate_limit_value(value, "resetsAt", "resets_at"),
    }


def _normalize_codex_rate_limit_snapshot(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    primary = _normalize_codex_rate_limit_window(value.get("primary"))
    secondary = _normalize_codex_rate_limit_window(value.get("secondary"))
    if primary is None and secondary is None:
        return None
    return {
        "limit_id": _codex_rate_limit_value(value, "limitId", "limit_id"),
        "limit_name": _codex_rate_limit_value(value, "limitName", "limit_name"),
        "primary": primary,
        "secondary": secondary,
        "credits": value.get("credits"),
        "plan_type": _codex_rate_limit_value(value, "planType", "plan_type"),
        "rate_limit_reached_type": _codex_rate_limit_value(
            value,
            "rateLimitReachedType",
            "rate_limit_reached_type",
        ),
    }


def _normalize_codex_rate_limits_response(result: Any) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    by_limit_id = result.get("rateLimitsByLimitId")
    snapshot = None
    if isinstance(by_limit_id, dict):
        snapshot = by_limit_id.get("codex")
        if not isinstance(snapshot, dict):
            snapshot = next((v for v in by_limit_id.values() if isinstance(v, dict)), None)
    if not isinstance(snapshot, dict):
        snapshot = result.get("rateLimits") or result.get("rate_limits")
    return _normalize_codex_rate_limit_snapshot(snapshot)


async def _fetch_codex_app_rate_limits() -> dict[str, Any] | None:
    """Read current Codex quota from the local app-server account API.

    Session JSONL only updates when a Codex turn starts; this app-server method
    refreshes the account-level rate-limit bucket without sending a model turn.
    """
    cli = _codex_cli_path()
    if not cli:
        return None
    proc = None
    try:
        env = os.environ.copy()
        env["CODEX_SKIP_AUTO_UPGRADE"] = "1"
        proc = await asyncio.create_subprocess_exec(
            cli,
            "app-server",
            "--listen",
            "stdio://",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
            limit=1024 * 1024,
        )
        assert proc.stdin is not None
        assert proc.stdout is not None
        requests = [
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {"name": "babata-status", "version": "0"},
                    "capabilities": {"experimentalApi": True},
                },
            },
            {"id": 2, "method": "account/rateLimits/read"},
        ]
        for req in requests:
            proc.stdin.write((json.dumps(req, separators=(",", ":")) + "\n").encode())
        await proc.stdin.drain()

        deadline = asyncio.get_running_loop().time() + 8.0
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            try:
                raw = await asyncio.wait_for(proc.stdout.readline(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            if not raw:
                return None
            try:
                msg = json.loads(raw.decode("utf-8", errors="replace"))
            except Exception:
                continue
            if msg.get("id") != 2:
                continue
            if isinstance(msg.get("result"), dict):
                return _normalize_codex_rate_limits_response(msg["result"])
            return None
    except Exception as e:
        log.debug("codex app-server rate-limit refresh failed: %s", e)
        return None
    finally:
        if proc is not None and proc.returncode is None:
            if proc.stdin is not None:
                with suppress(Exception):
                    proc.stdin.close()
            with suppress(ProcessLookupError, Exception):
                proc.terminate()
            with suppress(asyncio.TimeoutError, Exception):
                await asyncio.wait_for(proc.wait(), timeout=1)


def _codex_status_snapshot(sid: str | None) -> dict[str, Any]:
    """Best-effort Codex status from the same local files the CLI writes.

    `codex exec --json` does not expose a stable `/status` command. Session
    JSONL gives us the same per-session snapshot that terminal `/status` shows;
    the Codex App settings panel is a separate account-level view and can differ.
    """
    cfg = _codex_config()
    configured_model = os.environ.get("BABATA_CODEX_MODEL") or str(cfg.get("model") or "codex")
    configured_effort = str(cfg.get("model_reasoning_effort") or "—")
    model = configured_model
    effort = configured_effort
    info: dict[str, Any] = {}
    rate_limits: dict[str, Any] | None = None

    fp = _codex_session_file(sid)
    if fp:
        try:
            with fp.open() as f:
                for line in f:
                    if '"turn_context"' in line:
                        try:
                            d = json.loads(line)
                        except Exception:
                            continue
                        payload = d.get("payload") if isinstance(d, dict) else {}
                        if isinstance(payload, dict):
                            model = str(payload.get("model") or model)
                            effort = str(payload.get("effort") or effort)
                            collab = payload.get("collaboration_mode") or {}
                            settings = collab.get("settings") if isinstance(collab, dict) else {}
                            if isinstance(settings, dict):
                                model = str(settings.get("model") or model)
                                effort = str(settings.get("reasoning_effort") or effort)
                    elif '"token_count"' in line:
                        try:
                            d = json.loads(line)
                        except Exception:
                            continue
                        payload = d.get("payload") if isinstance(d, dict) else {}
                        if not isinstance(payload, dict) or payload.get("type") != "token_count":
                            continue
                        event_info = payload.get("info")
                        if isinstance(event_info, dict):
                            info = event_info
                        event_rl = _codex_event_rate_limits(d)
                        if isinstance(event_rl, dict):
                            rate_limits = event_rl
        except Exception:
            pass

    last = info.get("last_token_usage") if isinstance(info, dict) else {}
    if not isinstance(last, dict):
        last = {}
    context_window = info.get("model_context_window") if isinstance(info, dict) else None
    if not context_window:
        context_window = _codex_model_window(model)

    context_used = (
        last.get("input_tokens")
        or last.get("total_tokens")
        or 0
    )
    return {
        "model": model,
        "effort": effort,
        "configured_model": configured_model,
        "configured_effort": configured_effort,
        "context_window": int(context_window or 0),
        "context_used": int(context_used or 0),
        "last_usage": last,
        "rate_limits": rate_limits,
        "session_file": str(fp) if fp else "",
    }


def _codex_limit_entry(
    rate_limits: dict[str, Any] | None,
    key: str,
    window_minutes: int,
) -> dict[str, Any] | None:
    if not isinstance(rate_limits, dict):
        return None
    for value in rate_limits.values():
        if not isinstance(value, dict):
            continue
        try:
            minutes = int(value.get("window_minutes") or 0)
        except Exception:
            continue
        if minutes == window_minutes:
            return value
    entry = rate_limits.get(key)
    if not isinstance(entry, dict):
        return None
    return entry


def _fmt_codex_limit(
    rate_limits: dict[str, Any] | None,
    key: str,
    label: str,
    window_minutes: int,
) -> str | None:
    entry = _codex_limit_entry(rate_limits, key, window_minutes)
    if not isinstance(entry, dict):
        return None
    used = entry.get("used_percent")
    if used is None:
        return f"{label} —"
    try:
        left = 100.0 - float(used)
    except Exception:
        return f"{label} —"
    left = max(0.0, min(100.0, left))
    return f"{label} {left:.0f}% left · resets {_fmt_codex_reset(entry.get('resets_at'))}"


# ── Status: quota / today cost / formatting helpers ──────────────────
#
# V's terminal CC /status shows:
#   [bar] N% · <model> (<window> context)
#   session N% · resets Npm  |  week N% · resets Mon DD  |  $X today
#
# Quota source: send a 1-token POST to /v1/messages and read the
# `anthropic-ratelimit-unified-{5h,7d}-utilization` response headers — same
# numbers the official CLI's TUI surfaces (claude_agent_sdk's RateLimitEvent
# strips utilization in print/SDK mode, and /api/oauth/usage is rate-limited
# upstream, so neither works as a fallback). Costs ~9 input + 1 output token
# of the cheapest model per /status invocation; not cached because V wants
# real-time numbers.


def _fetch_anthropic_quota_sync(token: str) -> dict | None:
    """POST a minimal Claude API request and parse rate-limit headers.
    Returns {"five_hour": {utilization, resets_at}, "seven_day": {...}} or
    None on net/auth failure."""
    if not token:
        return None
    body = json.dumps({
        "model": "claude-haiku-4-5",
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "."}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "oauth-2025-04-20",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            r.read()
            h = r.headers
    except (urllib.error.URLError, TimeoutError, Exception):
        return None

    def _pct(name: str) -> float | None:
        v = h.get(name)
        try:
            return float(v) if v is not None else None
        except ValueError:
            return None

    def _ts(name: str) -> int | None:
        v = h.get(name)
        try:
            return int(v) if v is not None else None
        except ValueError:
            return None

    return {
        "five_hour": {
            "utilization": _pct("anthropic-ratelimit-unified-5h-utilization"),
            "resets_at": _ts("anthropic-ratelimit-unified-5h-reset"),
        },
        "seven_day": {
            "utilization": _pct("anthropic-ratelimit-unified-7d-utilization"),
            "resets_at": _ts("anthropic-ratelimit-unified-7d-reset"),
        },
    }


async def _fetch_anthropic_quota(token: str) -> dict | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_anthropic_quota_sync, token)

# OpenRouter 渠道下 /status 用: 读 /v1/key 拿 usage / limit / daily.
# 5 分钟 cache 跟 statusline.sh 共享同一文件 (/tmp/cc-or-usage-{uid}.json),
# 两边都不 blocking — 谁先过 5min 就异步刷新, 另一方读缓存.
_OR_USAGE_CACHE = Path(f"/tmp/cc-or-usage-{os.getuid()}.json")
_OR_USAGE_STAMP = Path(f"/tmp/cc-or-usage-{os.getuid()}.stamp")
_OR_USAGE_MAX_AGE = 300


def _openrouter_key_from_providers(snapshot: dict | None = None) -> str | None:
    """Find any OpenRouter API key configured in providers.json. Optional
    snapshot lets callers pass a pre-loaded providers dict so a /provider
    switch mid-render can't sneak in a different key (race-safe single-read
    contract for cmd_status)."""
    data = snapshot if snapshot is not None else _load_providers()
    for cfg in data.get("providers", {}).values():
        env = cfg.get("env") or {}
        if "openrouter" in (env.get("ANTHROPIC_BASE_URL", "") or "").lower():
            tok = env.get("ANTHROPIC_AUTH_TOKEN")
            if tok:
                return tok
    return None


# Daemon-thread refresh keeps /status fast (returns stale cache immediately)
# while next call sees fresh data. Lock serializes refreshes so concurrent
# /status calls don't fan out to N parallel HTTP requests.
_OR_REFRESH_LOCK = threading.Lock()


def _refresh_or_cache(key: str) -> None:
    """Background refresher invoked from a daemon thread. Writes the stamp
    ONLY on success — failed refresh stays "stale" so the next /status retries
    instead of suppressing for the full _OR_USAGE_MAX_AGE window."""
    try:
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/key",
            headers={"Authorization": f"Bearer {key}"},
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            body = r.read()
        tmp = _OR_USAGE_CACHE.with_suffix(_OR_USAGE_CACHE.suffix + ".tmp")
        tmp.write_bytes(body)
        tmp.replace(_OR_USAGE_CACHE)
        _OR_USAGE_STAMP.write_text(str(int(time.time())))
    except Exception:
        pass


def _fetch_or_usage_sync(key: str | None) -> dict | None:
    """Return cached OpenRouter /v1/key response. If cache is stale, fire a
    daemon-thread refresh (don't block /status) and return whatever's on disk.
    Token stays in-process (Authorization header on Request) — never enters
    argv, so `ps aux` is clean."""
    if not key:
        return None
    now = int(time.time())
    try:
        last = int(_OR_USAGE_STAMP.read_text().strip() or "0")
    except Exception:
        last = 0
    if now - last >= _OR_USAGE_MAX_AGE and _OR_REFRESH_LOCK.acquire(blocking=False):
        def _runner():
            try:
                _refresh_or_cache(key)
            finally:
                _OR_REFRESH_LOCK.release()
        try:
            threading.Thread(target=_runner, daemon=True).start()
        except Exception:
            _OR_REFRESH_LOCK.release()

    if not _OR_USAGE_CACHE.exists():
        return None
    try:
        return json.loads(_OR_USAGE_CACHE.read_text())
    except Exception:
        return None


async def _fetch_or_usage(key: str | None) -> dict | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_or_usage_sync, key)


def _fmt_reset(value: Any) -> str:
    """Reset-timestamp display. Accepts either ISO8601 UTC string (legacy
    /api/oauth/usage shape) or Unix int (SDK RateLimitInfo.resets_at).
    Same local date → '9pm' (12h lowercase). Different date → 'Apr 24'."""
    if value is None or value == "":
        return "—"
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value)).astimezone()
        else:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone()
    except Exception:
        return "—"
    now = datetime.now().astimezone()
    if dt.date() == now.date():
        h = dt.hour
        ampm = "am" if h < 12 else "pm"
        h12 = h % 12 or 12
        if dt.minute == 0:
            return f"{h12}{ampm}"
        return f"{h12}:{dt.minute:02d}{ampm}"
    return dt.strftime("%b %-d")


def _fmt_codex_reset(value: Any) -> str:
    """Codex TUI-style reset display: '13:11' today, '14:01 on 16 May' later."""
    if value is None or value == "":
        return "—"
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value)).astimezone()
        else:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone()
    except Exception:
        return "—"
    now = datetime.now().astimezone()
    if dt.date() == now.date():
        return dt.strftime("%H:%M")
    return dt.strftime("%H:%M on %-d %b")


def _progress_bar(pct: float, width: int = 15) -> str:
    """Render a block progress bar. Uses █ (full) vs ░ (light) — solid contrast
    renders cleanly in TG's iOS system font; ▓ (medium shade) gets rendered as
    a noisy stipple pattern at display size and looks junk."""
    pct = max(0.0, min(100.0, pct))
    filled = int(round(pct / 100 * width))
    return "█" * filled + "░" * (width - filled)


_MODEL_RE = re.compile(r"claude-(\w+)-(\d+)-(\d+)", re.I)


def _short_model(full: str | None) -> str:
    """'claude-opus-4-7[1m]' → 'Opus 4.7'. Alias suffix is dropped (shown
    separately as context window)."""
    if not full:
        return "—"
    m = _MODEL_RE.search(full)
    if not m:
        return full
    name, maj, min_ = m.groups()
    return f"{name.capitalize()} {maj}.{min_}"


def _short_window(tokens: int | None) -> str:
    """1_000_000 → '1M' / 200_000 → '200K'."""
    if not tokens:
        return "—"
    if tokens >= 1_000_000:
        n = tokens / 1_000_000
        return f"{n:g}M"
    if tokens >= 1_000:
        return f"{tokens // 1_000}K"
    return str(tokens)


_CCUSAGE_BIN = Path.home() / ".bun" / "bin" / "ccusage"
# Share cache with statusline.sh — when V has a terminal CC open, its
# statusline keeps this cache fresh, and /status hits it for free. When
# nothing is keeping it warm, our own daemon thread refreshes it.
_CCUSAGE_CACHE = Path(f"/tmp/cc-ccusage-{os.getuid()}.txt")
_CCUSAGE_STAMP = Path(f"/tmp/cc-ccusage-{os.getuid()}.stamp")
_CCUSAGE_MAX_AGE = 60  # match statusline.sh
_CCUSAGE_REFRESH_LOCK = threading.Lock()
_CCUSAGE_FAKE_INPUT = json.dumps({
    "hook_event_name": "Status",
    "session_id": "00000000-0000-0000-0000-000000000000",
    "transcript_path": "/tmp/none.jsonl",
    "cwd": "/tmp",
    "model": {"display_name": "Opus", "id": "claude-opus-4-7"},
    "workspace": {"current_dir": "/tmp", "project_dir": "/tmp"},
})
_CCUSAGE_TODAY_RE = re.compile(r"\$(\d+(?:\.\d+)?) today")


def _refresh_ccusage_cache() -> None:
    """Run `ccusage statusline` (~10s cold) and write its emoji-text output
    to the cache file. Stamp written only on success."""
    try:
        proc = subprocess.run(
            [str(_CCUSAGE_BIN), "statusline", "-B", "emoji-text"],
            input=_CCUSAGE_FAKE_INPUT,
            capture_output=True, text=True, timeout=20,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return
        tmp = _CCUSAGE_CACHE.with_suffix(_CCUSAGE_CACHE.suffix + ".tmp")
        tmp.write_text(proc.stdout)
        tmp.replace(_CCUSAGE_CACHE)
        _CCUSAGE_STAMP.write_text(str(int(time.time())))
    except Exception:
        pass


def _fetch_ccusage_today_sync() -> float | None:
    """Return today's total spend by reading the shared statusline cache.
    Fires a daemon-thread refresh when stale; returns whatever's on disk
    immediately. None when ccusage isn't installed or cache parse fails."""
    if not _CCUSAGE_BIN.exists():
        return None
    now = int(time.time())
    try:
        last = int(_CCUSAGE_STAMP.read_text().strip() or "0")
    except Exception:
        last = 0
    if now - last >= _CCUSAGE_MAX_AGE and _CCUSAGE_REFRESH_LOCK.acquire(blocking=False):
        def _runner():
            try:
                _refresh_ccusage_cache()
            finally:
                _CCUSAGE_REFRESH_LOCK.release()
        try:
            threading.Thread(target=_runner, daemon=True).start()
        except Exception:
            _CCUSAGE_REFRESH_LOCK.release()
    if not _CCUSAGE_CACHE.exists():
        return None
    try:
        m = _CCUSAGE_TODAY_RE.search(_CCUSAGE_CACHE.read_text())
        return float(m.group(1)) if m else None
    except Exception:
        return None


async def _fetch_ccusage_today() -> float | None:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _fetch_ccusage_today_sync)


def _last_prompt_tokens(sid: str | None) -> int:
    """Context tokens for the most recent API call — NOT the turn aggregate.

    ResultMessage.model_usage sums cache_read across every tool iteration in a
    turn; the same prompt cache can be re-read 5–10× per turn, so the aggregate
    balloons past the context window (seen: 205% of 1M) even though each single
    call sits at ~30%. The real context fill is the *last* assistant message's
    usage. Scan the session jsonl backward for the most recent assistant entry
    and sum its input + cache_read + cache_creation (matches CC terminal's bar).
    """
    if not sid:
        return 0
    projects = Path.home() / ".claude" / "projects"
    for fp in projects.glob(f"*/{sid}.jsonl"):
        try:
            lines = fp.read_text().splitlines()
        except Exception:
            continue
        for line in reversed(lines):
            if '"type":"assistant"' not in line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            usage = (d.get("message") or {}).get("usage") or {}
            if usage:
                return int(
                    (usage.get("input_tokens") or 0)
                    + (usage.get("cache_creation_input_tokens") or 0)
                    + (usage.get("cache_read_input_tokens") or 0)
                )
    return 0


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current model, context usage, cost, session state."""
    if not _allowed(update):
        return

    if _current_cpu_name() == "codex":
        snap = _codex_status_snapshot(cc._session_id)
        fresh_limits = await _fetch_codex_app_rate_limits()
        if fresh_limits:
            snap["rate_limits"] = fresh_limits
        snap_model = str(snap.get("model") or "")
        state_model = _last_model if _last_model and _last_model != "codex" else ""
        actual = (
            snap_model
            if snap_model and snap_model != "codex"
            else state_model or snap_model or os.environ.get("BABATA_CODEX_MODEL") or "codex"
        )
        effort = snap.get("effort") or "—"
        used = int(snap.get("context_used") or 0)
        win = int(snap.get("context_window") or 0)
        fallback_used = _last_used_tokens if _last_session_id == cc._session_id else 0
        if not used:
            used = fallback_used
        pct_ctx = (used / win * 100) if (win and used > 0) else 0.0
        bar = _progress_bar(pct_ctx)
        window_short = _short_window(win)
        last_usage = snap.get("last_usage") if isinstance(snap.get("last_usage"), dict) else {}
        out_tokens = int(last_usage.get("output_tokens") or 0)
        reason_tokens = int(last_usage.get("reasoning_output_tokens") or 0)
        token_bits = []
        if used:
            token_bits.append(f"{_fmt_tok(used)} in")
        if out_tokens:
            token_bits.append(f"{_fmt_tok(out_tokens)} out")
        if reason_tokens:
            token_bits.append(f"{_fmt_tok(reason_tokens)} reasoning")
        token_line = " · ".join(token_bits) if token_bits else "tokens —"
        limits = snap.get("rate_limits") if isinstance(snap.get("rate_limits"), dict) else None
        five_hour_line = _fmt_codex_limit(limits, "primary", "5h limit", 300)
        week_line = _fmt_codex_limit(limits, "secondary", "weekly limit", 10_080)
        plan_type = (limits or {}).get("plan_type") if isinstance(limits, dict) else None
        sids = cc._load_state().get("recent_sids") or []
        sid_now = cc._session_id if cc._session_id else "(new)"
        labels = {0: "hidden", 1: "flash", 2: "keep"}
        lines = [
            "<b>📊 Status</b>",
            "",
            f"{bar} {pct_ctx:.0f}% · {html.escape(_short_model(actual))} {html.escape(str(effort))} ({window_short})",
            "",
            html.escape(token_line),
        ]
        if five_hour_line:
            lines.append(html.escape(five_hour_line))
        if week_line:
            lines.append(html.escape(week_line))
        if plan_type:
            lines.append(html.escape(f"plan {plan_type}"))
        lines += [
            "",
            f"Codex v{_codex_version()} · {labels.get(_verbose, _verbose)}",
            f"<code>{sid_now}</code> · {len(sids)} recent",
        ]
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        return

    # Config-level model (what settings.json asks for — may be alias like "opus[1m]").
    # Differs from actual model name SDK reports (resolved full version).
    cfg_model = "—"
    try:
        cfg = json.loads((Path.home() / ".claude/settings.json").read_text())
        cfg_model = cfg.get("model", "—")
    except Exception:
        pass

    # Actual model resolution order:
    #   1. _last_model — set by this bot instance from ResultMessage.model_usage
    #      (full alias-suffixed form like 'claude-opus-4-N[1m]').
    #   2. Scan recent session JSONL (survives bot restart with no turns yet) —
    #      gives bare 'claude-opus-4-N'; re-attach cfg's [..] suffix so the
    #      displayed string stays consistent with what model_usage would show.
    #   3. Fall back to cfg alias — at least shows something concrete.
    actual = _last_model
    if not actual:
        scanned = _scan_recent_session_model()
        if scanned:
            suffix_match = _ALIAS_WINDOW_RE.search(cfg_model or "")
            actual = f"{scanned}{suffix_match.group(0)}" if suffix_match else scanned
    if not actual:
        actual = cfg_model

    win = _last_context_window or _infer_window_from_alias(cfg_model)
    # Fallback to _last_used_tokens only when it belongs to the current session.
    # Otherwise idle-reset (4am daily restart spawns a fresh sid) leaks the
    # previous session's inflated model_usage aggregate into the new session's
    # bar — observed as "317% of 1M" before JSONL flushed the first turn.
    fallback_used = _last_used_tokens if _last_session_id == cc._session_id else 0
    used = _last_prompt_tokens(cc._session_id) or fallback_used
    pct_ctx = (used / win * 100) if (win and used > 0) else 0.0
    bar = _progress_bar(pct_ctx)
    model_short = _short_model(actual)
    window_short = _short_window(win)

    # Quota policy: show whatever the current /provider points at, plus a
    # secondary OpenRouter line if current isn't already OR. Tokens for other
    # OAuth accounts in providers.json may lack `user:profile` scope (403), so
    # we don't try to render them — only the active account is meaningful.

    # Single snapshot of providers.json — guard against /provider switch racing
    # mid-render and giving us provider A's token but provider B's label.
    snapshot = _load_providers()
    provider_key = snapshot.get("current", "?")
    providers = snapshot.get("providers", {})
    current_cfg = providers.get(provider_key) or {}
    provider_label = current_cfg.get("display_name", provider_key)
    current_env = current_cfg.get("env") or {}
    current_oauth_token = current_env.get("CLAUDE_CODE_OAUTH_TOKEN")
    is_or_current = "openrouter" in (current_env.get("ANTHROPIC_BASE_URL", "") or "").lower()

    # Active provider's primary quota — POST a 1-token Claude request live and
    # read the rate-limit response headers. Real-time, no cache, ~10 tokens
    # of the cheapest model per /status. The request uses the *current*
    # provider token, so quota always matches the active provider.
    session_line = week_line = None
    if current_oauth_token and not is_or_current:
        quota = await _fetch_anthropic_quota(current_oauth_token)
        rl = quota or {}

        def _format(window_key: str, label: str) -> str:
            entry = rl.get(window_key) or {}
            util = entry.get("utilization")
            if util is None:
                return f"{label} —"
            return f"{label} {util * 100:.0f}% · resets {_fmt_reset(entry.get('resets_at'))}"

        session_line = _format("five_hour", "session")
        week_line = _format("seven_day", "week")

    # OpenRouter — full 2-line layout when OR is current, compact 1-line
    # secondary otherwise. OR data is always fetched (cached, non-blocking).
    # Pass key from the same snapshot that drove provider_key/label so a
    # /provider switch mid-render can't desync them.
    or_key = _openrouter_key_from_providers(snapshot)
    or_today_line = or_balance_line = or_compact_line = None
    or_data = await _fetch_or_usage(or_key)
    if is_or_current:
        if or_data:
            d = or_data.get("data") or {}
            or_used = d.get("usage")
            or_today = d.get("usage_daily")
            or_rem = d.get("limit_remaining")
            or_limit = d.get("limit")
            or_today_line = f"${or_today:.2f} today" if or_today is not None else "today —"
            if or_used is not None and or_rem is not None:
                or_balance_line = f"${or_used:.2f} used · ${or_rem:.2f} left USD"
            elif or_used is not None and or_limit is not None:
                or_balance_line = f"${or_used:.2f} used · ${(or_limit - or_used):.2f} left USD"
            elif or_used is not None:
                or_balance_line = f"${or_used:.2f} used · no limit USD"
            else:
                or_balance_line = "usage —"
        else:
            or_today_line, or_balance_line = "today —", "usage —"
    elif or_data:
        d = or_data.get("data") or {}
        or_used = d.get("usage")
        or_today = d.get("usage_daily")
        or_rem = d.get("limit_remaining")
        or_limit = d.get("limit")
        parts = ["openrouter"]
        if or_today is not None:
            parts.append(f"${or_today:.2f} today")
        if or_rem is not None:
            parts.append(f"${or_rem:.2f} left")
        elif or_used is not None and or_limit is not None:
            parts.append(f"${(or_limit - or_used):.2f} left")
        elif or_used is not None:
            parts.append(f"${or_used:.2f} used")
        or_compact_line = " · ".join(parts) if len(parts) > 1 else None

    today_cost = await _fetch_ccusage_today()
    today_str = f"${today_cost:.2f}" if today_cost is not None else "—"
    today_line = f"{today_str} today (ccusage) · {provider_label}"

    sids = cc._load_state().get("recent_sids") or []
    sid_now = cc._session_id if cc._session_id else "(new)"

    labels = {0: "hidden", 1: "flash", 2: "keep"}

    # Layout: header two lines in default font (no <code> — TG renders ▓/░ as
    # noisy stipple inside code blocks). Quota broken into separate lines so
    # mobile doesn't wrap awkwardly. Session UUID on its own line, same reason.
    lines = [
        "<b>📊 Status</b>",
        "",
        f"{bar} {pct_ctx:.0f}% · {html.escape(model_short)} ({window_short})",
        "",
    ]
    if session_line:
        lines.append(html.escape(session_line))
    if week_line:
        lines.append(html.escape(week_line))
    if or_today_line:
        lines.append(html.escape(or_today_line))
        lines.append(html.escape(or_balance_line))
    if or_compact_line:
        lines.append(html.escape(or_compact_line))
    lines += [
        today_line,
        "",
        f"CC v{_cc_version()} · SDK v{_sdk_version()} · {labels.get(_verbose, _verbose)}",
        f"<code>{html.escape(actual)}</code>",
        f"<code>{sid_now}</code> · {len(sids)} recent",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def _fmt_tok(n: int) -> str:
    """Human-readable token count: 34123 → 34.1K, 1000000 → 1.0M."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


async def cmd_context(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """TG /context — query the live SDK context-usage control API."""
    if not _allowed(update):
        return
    if _current_cpu_name() != "claude":
        await update.message.reply_text("当前 CPU 是 Codex，/context 不支持。")
        return

    wait_msg = await update.message.reply_text("查询中…")
    try:
        usage = await cc.context_usage()
        total = int(usage.get("totalTokens") or 0)
        max_tokens = int(usage.get("maxTokens") or 0)
        pct = float(usage.get("percentage") or 0.0)
        model = str(usage.get("model") or "—")
        lines = [
            f"Model: {model}",
            f"Total: {_fmt_tok(total)} / {_fmt_tok(max_tokens)} ({pct:.1f}%)",
            "",
            "Categories:",
        ]
        for cat in usage.get("categories") or []:
            name = cat.get("name", "?")
            tokens = int(cat.get("tokens") or 0)
            lines.append(f"- {name}: {_fmt_tok(tokens)}")
        mcp_tools = usage.get("mcpTools") or []
        if mcp_tools:
            lines.extend(["", "MCP tools:"])
            for item in mcp_tools[:30]:
                name = item.get("name") or item.get("toolName") or "?"
                server = item.get("serverName") or item.get("server") or "?"
                tokens = int(item.get("tokens") or 0)
                loaded = "" if item.get("isLoaded", True) else " (deferred)"
                lines.append(f"- {server}/{name}: {_fmt_tok(tokens)}{loaded}")
        body = f"<pre>{html.escape(chr(10).join(lines))}</pre>"
        await wait_msg.edit_text(body[:4000], parse_mode="HTML")
    except Exception as e:
        await wait_msg.edit_text(f"/context 失败: {type(e).__name__}: {e}")


async def cmd_restart(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Graceful restart: 等 in-flight CC 任务跑完再让 launchd (KeepAlive=true)
    重拉. 和 SIGTERM 走同一条 _graceful_shutdown 路径, 保证 V 手动 /restart
    不中断任务.

    ThrottleInterval=10s + ExitTimeOut=600s 在 plist 里设置, 最多等 10 分钟
    让任务结束, 超时才 SIGKILL. os._exit(0) 在 _graceful_shutdown 末尾触发.
    """
    # Infrastructure-touching command: fail-closed even if ALLOWED_USER==0 (开发态后门).
    if not ALLOWED_USER or not (update.effective_user and update.effective_user.id == ALLOWED_USER):
        return
    if _in_flight > 0:
        await update.message.reply_text(
            f"🕐 /restart 已排队 · 等 {_in_flight} 个 CC 任务跑完后重启"
        )
    else:
        await update.message.reply_text("🔄 重启中… 10 秒后回来")
    # Fire-and-forget: 当前 handler 返回后 _graceful_shutdown 再接管退出.
    # 直接 await 会卡住当前 update 的 response 循环.
    asyncio.create_task(
        _graceful_shutdown(ctx.application, reason="收到 /restart")
    )


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    # Infrastructure-touching command: fail-closed even if ALLOWED_USER==0 (开发态后门).
    if not ALLOWED_USER or not (update.effective_user and update.effective_user.id == ALLOWED_USER):
        return
    if _current_cpu_name() != "claude":
        await update.message.reply_text("当前 CPU 是 Codex，/stop 不支持；cut-in 会排队到当前 turn 结束后处理。")
        return
    try:
        await _worker().interrupt()
        await update.message.reply_text("⏸  当前 turn 已请求中断")
    except Exception as e:
        await update.message.reply_text(f"/stop 失败: {type(e).__name__}: {e}")


def _reset_status_for_cpu_switch() -> None:
    global _session_cost, _session_turns, _last_model, _last_context_window
    global _last_used_tokens, _last_cost, _last_session_id
    _session_cost = 0.0
    _session_turns = 0
    _last_model = None
    _last_context_window = None
    _last_used_tokens = 0
    _last_cost = 0.0
    _last_session_id = None
    _state["session_cost"] = 0.0
    _state["session_turns"] = 0
    _state["last_cost"] = 0.0
    _state["last_used_tokens"] = 0
    for key in ("last_model", "last_context_window", "last_session_id"):
        _state.pop(key, None)


async def _switch_cpu(target: str) -> str:
    global cc, _channel_worker
    target_name = normalize_engine(target)
    current_name = _current_cpu_name()
    if target_name == current_name:
        return f"CPU 已经是 {engine_label(target_name)}"
    worker = _channel_worker
    if _in_flight > 0 or (worker is not None and worker._turn_active):
        raise RuntimeError(f"当前还有 {_in_flight or 1} 个 turn 在跑，等结束后再 /cpu")

    # Older state files only have one top-level session_id. Snapshot the
    # current CPU's sid into the per-engine slot before the top-level value can
    # be replaced by the target CPU.
    if hasattr(cc, "_record_sid"):
        with suppress(Exception):
            cc._record_sid(getattr(cc, "_session_id", None))

    new_cc = _make_tg_engine(target_name)
    new_worker = ChannelWorker(new_cc, instance_label=_CURRENT_LABEL)
    old_worker = _channel_worker

    if old_worker is not None:
        await old_worker.stop()
    try:
        await new_worker.start()
    except Exception:
        log.exception("CPU switch failed; restoring %s", current_name)
        with suppress(Exception):
            restore_cc = _make_tg_engine(current_name)
            restore_worker = ChannelWorker(restore_cc, instance_label=_CURRENT_LABEL)
            await restore_worker.start()
            cc = restore_cc
            _channel_worker = restore_worker
        raise

    cc = new_cc
    _channel_worker = new_worker
    persist_engine(SESSION_FILE, target_name)
    _reset_status_for_cpu_switch()
    _save_state()
    return f"CPU: {engine_label(current_name)} → {engine_label(target_name)}"


async def cmd_cpu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Switch the assistant CPU for this TG process."""
    # Engine-changing command: fail-closed even if ALLOWED_USER==0 (开发态后门).
    if not ALLOWED_USER or not (update.effective_user and update.effective_user.id == ALLOWED_USER):
        return

    args = (update.message.text or "").split(maxsplit=1)
    if len(args) > 1 and args[1].strip():
        wait_msg = await update.message.reply_text("切换 CPU 中…")
        try:
            body = await _switch_cpu(args[1].strip())
            await _sync_bot_commands(ctx.bot)
            await wait_msg.edit_text(body)
        except Exception as e:
            await wait_msg.edit_text(f"/cpu 失败: {type(e).__name__}: {e}")
        return

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    current = _current_cpu_name()
    buttons = [
        [InlineKeyboardButton(
            f"{'● ' if key == current else '○ '}{label}",
            callback_data=f"cpu:{key}",
        )]
        for label, key in engine_choices()
    ]
    await update.message.reply_text(
        f"CPU (当前: {engine_label(current)}):",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def on_cpu_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not await _callback_allowed(q):
        return
    await q.answer()
    data = q.data or ""
    if ":" not in data:
        return
    _, target = data.split(":", 1)
    try:
        await q.edit_message_text("切换 CPU 中…")
        body = await _switch_cpu(target)
        await _sync_bot_commands(ctx.bot)
        await q.edit_message_text(body)
    except Exception as e:
        await q.edit_message_text(f"/cpu 失败: {type(e).__name__}: {e}")


# cc-router 是可选外部服务 (V 私人多 Anthropic 账号切换). 没设 BABATA_CC_ROUTER_DIR
# = OSS 用户没这服务, /provider 命令降级 "未配置". V .env 设路径走原行为.
_CC_ROUTER_DIR = os.environ.get("BABATA_CC_ROUTER_DIR", "")
_CC_ROUTER_CLI = str(Path(_CC_ROUTER_DIR) / "cli.py") if _CC_ROUTER_DIR else ""
_PROVIDERS_JSON = Path(_CC_ROUTER_DIR) / "providers.json" if _CC_ROUTER_DIR else None


def _load_providers() -> dict:
    if not _PROVIDERS_JSON:
        return {}
    try:
        return json.loads(_PROVIDERS_JSON.read_text())
    except Exception:
        return {}


def _provider_choices() -> list[tuple[str, str]]:
    """[(display_name, key), ...] dynamic from providers.json — adding an account
    there auto-surfaces here without touching bot.py."""
    data = _load_providers()
    return [(cfg.get("display_name", key), key) for key, cfg in data.get("providers", {}).items()]


def _current_provider_key() -> str:
    return _load_providers().get("current", "?")


def _current_provider_label() -> str:
    """Display name of current provider. Used by /status + /provider UI."""
    data = _load_providers()
    key = data.get("current")
    if not key:
        return "?"
    return data.get("providers", {}).get(key, {}).get("display_name", key)


def _codex_choices() -> list[tuple[str, str]]:
    """[(display_name, slot), ...] for codex accounts. Same dynamic shape as
    _provider_choices but reads providers.json.codex_accounts."""
    data = _load_providers()
    return [(cfg.get("display_name", key), key) for key, cfg in data.get("codex_accounts", {}).items()]


def _current_codex_key() -> str:
    return _load_providers().get("codex_current", "?")


def _current_codex_label() -> str:
    data = _load_providers()
    key = data.get("codex_current")
    if not key:
        return "?"
    return data.get("codex_accounts", {}).get(key, {}).get("display_name", key)


async def _run_cc_router_switch(key: str) -> tuple[int, str]:
    if not _CC_ROUTER_CLI:
        return 2, "/provider 未配置 (需要 BABATA_CC_ROUTER_DIR env)"
    try:
        result = await asyncio.to_thread(
            subprocess.run,
            [_CC_ROUTER_CLI, "switch", key],
            capture_output=True,
            text=True,
            timeout=15,
        )
        body = (result.stdout + result.stderr).strip() or "(no output)"
        return result.returncode, body
    except subprocess.TimeoutExpired:
        return 124, "/provider 超时 (15s)"
    except Exception as e:
        return 2, f"/provider 失败: {type(e).__name__}: {e}"


async def _run_cc_router_codex(slot: str) -> tuple[int, str]:
    """Run `cli.py codex <slot>` — swaps ~/.codex/auth.json + Codex.app profile
    symlinks. Doesn't touch ~/.claude/settings.json and doesn't restart babata
    (babata fork-execs codex per message; new symlink picked up by next fork)."""
    if not _CC_ROUTER_CLI:
        return 2, "/provider 未配置 (需要 BABATA_CC_ROUTER_DIR env)"
    try:
        result = await asyncio.to_thread(
            subprocess.run,
            [_CC_ROUTER_CLI, "codex", slot],
            capture_output=True,
            text=True,
            timeout=15,
        )
        body = (result.stdout + result.stderr).strip() or "(no output)"
        return result.returncode, body
    except subprocess.TimeoutExpired:
        return 124, "/provider codex 超时 (15s)"
    except Exception as e:
        return 2, f"/provider codex 失败: {type(e).__name__}: {e}"


async def cmd_provider(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """切换 Anthropic 渠道.

    - 无参 → inline keyboard (类似 /verbose), 点按钮触发 switch
    - 带参 `/provider openrouter` → 直接 switch (供 scripting / 快捷路径)

    cc-router cli 改完 ~/.claude/settings.json env 会同步清 4 个 channel 的
    session_id (避免跨 provider 续 redacted_thinking 爆), 然后 self-ops.sh
    detached restart 4 个 launchd 实例. V 在 TG 体感 ~10s 断线再上线, 下条消息
    走新 provider, 新 session 无上下文 (跟手动 /new 一样).
    """
    # Infrastructure-changing command: fail-closed even if ALLOWED_USER==0 (开发态后门).
    if not ALLOWED_USER or not (update.effective_user and update.effective_user.id == ALLOWED_USER):
        return

    args = (update.message.text or "").split(maxsplit=1)
    arg = args[1].strip() if len(args) > 1 else ""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    if _current_cpu_name() == "codex":
        # codex CPU: 切 codex 账号 (symlink swap, 不重启 babata, 下条 message 生效)
        if arg:
            rc, body = await _run_cc_router_codex(arg)
            prefix = "🔄 Codex 切换中" if rc == 0 and "switched to" in body else f"⚠️ exit={rc}"
            await update.message.reply_text(f"{prefix}\n```\n{body}\n```", parse_mode="Markdown")
            return
        current_key = _current_codex_key()
        current_label = _current_codex_label()
        choices = _codex_choices()
        if not choices:
            await update.message.reply_text("⚠️ codex_accounts 未配置 (cc-router codex init 没跑过)")
            return
        buttons = [
            [InlineKeyboardButton(
                f"{'● ' if key == current_key else '○ '}{name}",
                callback_data=f"codex:{key}",
            )]
            for name, key in choices
        ]
        buttons.append([InlineKeyboardButton("➕ 添加新账号", callback_data="codex_add:click")])
        await update.message.reply_text(
            f"Codex 账号 (当前: {current_label}):",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # claude CPU: 切 Anthropic 渠道 (改 settings.json + 重启 babata)
    if arg:
        rc, body = await _run_cc_router_switch(arg)
        prefix = "🔄 切换中" if rc == 0 and "switched to" in body else f"⚠️ exit={rc}"
        await update.message.reply_text(f"{prefix}\n```\n{body}\n```", parse_mode="Markdown")
        return
    current_key = _current_provider_key()
    current_label = _current_provider_label()
    choices = _provider_choices()
    if not choices:
        await update.message.reply_text("⚠️ providers.json 读取失败, 无可选渠道")
        return
    buttons = [
        [InlineKeyboardButton(
            f"{'● ' if key == current_key else '○ '}{name}",
            callback_data=f"provider:{key}",
        )]
        for name, key in choices
    ]
    await update.message.reply_text(
        f"Anthropic 渠道 (当前: {current_label}):",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def on_provider_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not await _callback_allowed(q):
        return
    await q.answer()
    if _current_cpu_name() != "claude":
        await q.edit_message_text("CPU 已切到 Codex, Anthropic 渠道按钮失效。重新 /provider 看 codex 账号选项。")
        return
    data = q.data or ""
    if ":" not in data:
        return
    _, key = data.split(":", 1)
    target_name = dict((k, n) for n, k in _provider_choices()).get(key, key)
    await q.edit_message_text(f"🔄 切换到 {target_name}…")
    rc, body = await _run_cc_router_switch(key)
    prefix = "🔄 切换中" if rc == 0 and "switched to" in body else f"⚠️ exit={rc}"
    await q.edit_message_text(f"{prefix}\n```\n{body}\n```", parse_mode="Markdown")


async def on_codex_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not await _callback_allowed(q):
        return
    await q.answer()
    if _current_cpu_name() != "codex":
        await q.edit_message_text("CPU 已切到 Claude, Codex 账号按钮失效。重新 /provider 看 Anthropic 渠道选项。")
        return
    data = q.data or ""
    if ":" not in data:
        return
    _, key = data.split(":", 1)
    target_name = dict((k, n) for n, k in _codex_choices()).get(key, key)
    await q.edit_message_text(f"🔄 切换 Codex 到 {target_name}…")
    rc, body = await _run_cc_router_codex(key)
    prefix = "🔄 切换中" if rc == 0 and "switched to" in body else f"⚠️ exit={rc}"
    await q.edit_message_text(f"{prefix}\n```\n{body}\n```", parse_mode="Markdown")


async def on_codex_add_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """添加新 Codex 账号 — 显示三步指南。
    OAuth 必须在浏览器完成, 没法 in-bot 跑; 半自动也只能省第 1 步, 价值不大,
    保持纯指南最干净。"""
    q = update.callback_query
    if not await _callback_allowed(q):
        return
    await q.answer()
    text = (
        "*添加新 Codex 账号 (3 步)*\n\n"
        "在 terminal 跑:\n"
        "```\n"
        "cc-router codex <slot-name>\n"
        "codex login\n"
        "```\n"
        "1. `<slot-name>` 起个简称 (如 `personal` / `work`), cc-router 会自动创建空 slot 并切过去, 同时 quit + 重开 Codex.app\n"
        "2. `codex login` 浏览器 OAuth 选目标账号, 写入 `auth.<slot>.json`\n"
        "3. 在已打开的 Codex.app 里用同一账号登一次 (写入 `Codex.<slot>/`)\n\n"
        "完成后再点 /provider 应该能看到新 slot ✓ ✓"
    )
    await q.edit_message_text(text, parse_mode="Markdown")


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    await _process(update, ctx, update.message.text)


async def on_voice(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    voice = update.message.voice or update.message.audio
    if not voice:
        return

    file = await ctx.bot.get_file(voice.file_id)
    path = Path(f"/tmp/voice_{voice.file_id}.ogg")
    await file.download_to_drive(path)

    # voice-clone skill: 检测 pending state, 让 CC 拿到 wav 路径做声音克隆.
    # state 文件按 BABATA_INSTANCE 隔离 (4 launchd 实例不串). 含 expires_at
    # TTL + atomic rename consume 防 race + JSON 校验防 partial state.
    voice_clone_state = STATE_DIR / f"voice-clone-pending-{INSTANCE}.json"
    keep_wav: Path | None = None
    if voice_clone_state.exists():
        try:
            import json as _json
            meta = _json.loads(voice_clone_state.read_text())
            if meta.get("expires_at", 0) > time.time():
                # atomic consume: rename → 仅一个 handler 拿到 wav (FileNotFoundError = race lose)
                consumed = voice_clone_state.with_suffix(".json.consumed")
                try:
                    voice_clone_state.rename(consumed)
                    keep_wav = Path(f"/tmp/voice-clone-{INSTANCE}-{voice.file_id}.wav")
                    consumed.unlink(missing_ok=True)
                except FileNotFoundError:
                    pass  # 别的 handler 抢到了
            else:
                voice_clone_state.unlink(missing_ok=True)  # stale, 自清
        except (_json.JSONDecodeError, OSError) as _e:
            log.warning("voice-clone state malformed: %s", _e)

    try:
        text = await transcribe_voice(path, keep_wav=keep_wav)
    except Exception as e:
        await update.message.reply_text(f"\u274c 转录失败: {e}")
        return
    finally:
        path.unlink(missing_ok=True)

    await update.message.reply_text(f"\U0001f3a4 {text}")
    prompt = f"[语音 wav: {keep_wav}] {text}" if keep_wav else f"[语音] {text}"
    await _process(update, ctx, prompt)


# Physical: TG albums (multi-photo messages) arrive as N separate Updates
# tied by media_group_id. Without coalescing, each photo fires its own CC
# query and the album appears to CC as N independent single-photo messages.
_MEDIA_GROUP_DEBOUNCE = 1.0
_media_groups: dict[str, dict] = {}


async def on_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    photos = update.message.photo
    if not photos:
        return

    photo = photos[-1]
    file = await ctx.bot.get_file(photo.file_id)
    path = Path(f"/tmp/photo_{photo.file_id}.jpg")
    await file.download_to_drive(path)

    image = image_to_base64(path)
    caption = update.message.caption or ""
    gid = update.message.media_group_id

    if not gid:
        prompt = f"[图片: {path}]\n{caption}" if caption else f"[图片: {path}]"
        await _process(update, ctx, prompt, images=[image])
        return

    # Single-threaded asyncio: dict mutation without await is atomic, no lock needed.
    group = _media_groups.get(gid)
    start_flush = group is None
    if start_flush:
        group = {
            "images": [],
            "paths": [],
            "captions": [],
            "first_update": update,
        }
        _media_groups[gid] = group
    group["images"].append(image)
    group["paths"].append(path)
    if caption:
        group["captions"].append(caption)
    group["last_update_at"] = time.monotonic()

    if start_flush:
        asyncio.create_task(_flush_media_group(gid, ctx))


async def _flush_media_group(gid: str, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Wait until the album stops growing for _MEDIA_GROUP_DEBOUNCE seconds, then flush once."""
    while True:
        await asyncio.sleep(_MEDIA_GROUP_DEBOUNCE)
        group = _media_groups.get(gid)
        if group is None:
            return
        if time.monotonic() - group["last_update_at"] < _MEDIA_GROUP_DEBOUNCE:
            continue
        _media_groups.pop(gid, None)
        break

    images = group["images"]
    paths = group["paths"]
    captions = group["captions"]
    first_update = group["first_update"]

    paths_str = ", ".join(str(p) for p in paths)
    header = f"[图片 ×{len(images)}: {paths_str}]" if len(images) > 1 else f"[图片: {paths_str}]"
    caption_text = "\n".join(captions)
    prompt = f"{header}\n{caption_text}" if caption_text else header

    try:
        await _process(first_update, ctx, prompt, images=images)
    except Exception as e:
        log.error("Media group flush failed (gid=%s): %s", gid, e)


async def on_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    video = update.message.video or update.message.video_note
    if not video:
        return

    file = await ctx.bot.get_file(video.file_id)
    path = Path(f"/tmp/video_{video.file_id}.mp4")
    await file.download_to_drive(path)

    caption = update.message.caption or ""
    summary = await understand_video(path, caption)
    path.unlink(missing_ok=True)

    if not summary:
        await update.message.reply_text(
            "Video understanding unavailable. Set VIDEO_API_URL or keep video <10MB."
        )
        return

    prompt = f"{caption}\n\n[Video summary (mimo-v2.5)]: {summary}" if caption else f"[Video summary (mimo-v2.5)]: {summary}"
    await _process(update, ctx, prompt)


async def on_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    doc = update.message.document
    if not doc:
        return

    file = await ctx.bot.get_file(doc.file_id)
    save_dir = Path.home() / "Downloads"
    save_dir.mkdir(exist_ok=True)
    save_path = save_dir / doc.file_name
    await file.download_to_drive(save_path)

    caption = update.message.caption or ""
    prompt = f"[文件: {save_path}]\n{caption}" if caption else f"[文件: {save_path}]"
    await _process(update, ctx, prompt)


async def on_verbose_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not await _callback_allowed(query):
        return
    await query.answer()
    global _verbose
    _verbose = int((query.data or "verbose:1").split(":")[1])
    _state["verbose"] = _verbose
    _save_state()
    labels = {0: "hidden", 1: "flash", 2: "keep"}
    await query.edit_message_text(f"Tool display: {labels[_verbose]}")


# ── /resume (pick a past session to continue) ────────────────────────

def _fmt_ago(ts: float) -> str:
    """Relative-age label for session picker. mtime is already local clock."""
    if not ts:
        return "?"
    dt = time.time() - ts
    if dt < 0:
        return "now"
    if dt < 60:
        return f"{int(dt)}s"
    if dt < 3600:
        return f"{int(dt / 60)}m"
    if dt < 86400:
        return f"{int(dt / 3600)}h"
    return f"{int(dt / 86400)}d"


# 渠道 category → cc.py 的 channel label 白名单. 扩展新 channel 时只在此处维护.
# cc.py 层只认 channel label (巴巴塔 / 巴巴塔2 / wx / term / oneshot / ...),
# 不知 category 概念.
#
# "当前" = 当前 bot 实例自己的 session (按 BABATA_INSTANCE 从 INSTANCE_LABELS
# 取昵称). 每个 bot 进程只看得见自己的 TG 历史, 不跨 bot 混显 — 所以 session 条
# 目上也不需要 [巴巴塔N] 前缀 tag.
#
# "终端" (cli entrypoint) vs "一次性" (sdk-cli = claude -p) 分开, 避免 cron
# 的一次性 session 把 bb 交互列表塞满. 判定在 cc.list_recent_sessions 按 JSONL
# entrypoint 字段打 label.
_CURRENT_LABEL = INSTANCE_LABELS.get(INSTANCE, INSTANCE or PROJECT)
# (category_id, 中文显示名, channel labels in cc.py, scan_all_buckets)
# scan_all_buckets=True: 跨 ~/.claude/projects/<cwd-hash>/ 全部 bucket 扫.
# tg/wx 是 babata 自己拥有的 channel, sids 在 babata 自己 cwd 对应的单 bucket
# 里, 不用全扫. term/oneshot 是 V 在终端 (任意 cwd) 开的原生 CC, sessions
# 散落在不同 bucket, 必须全扫才能跟终端 /resume 对齐.
_RESUME_CATEGORIES: list[tuple[str, str, list[str], bool]] = [
    ("tg",      "当前",   [_CURRENT_LABEL],            False),
    ("wx",      "微信",   [INSTANCE_LABELS["weixin"]], False),
    ("term",    "终端",   ["term"],                    True),
    ("oneshot", "一次性", ["oneshot"],                 True),
]


def _resume_categories_for_current_cpu() -> list[tuple[str, str, list[str], bool]]:
    if _current_cpu_name() != "codex":
        return _RESUME_CATEGORIES
    cat_id, _name, labels, scan_all = _RESUME_CATEGORIES[0]
    return [(cat_id, "当前 Codex", labels, scan_all)]


def _render_resume_channel_picker() -> tuple[str, "InlineKeyboardMarkup"]:
    """Build the Level-1 渠道 picker (header text + keyboard).

    Shared between /resume command (initial display) and resume-back callback
    (从 Level 2 session 列表返回).
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    categories = _resume_categories_for_current_cpu()
    buttons = [
        [InlineKeyboardButton(name, callback_data=f"resume-ch:{cat}")]
        for cat, name, _, _ in categories
    ]
    cur = cc._session_id
    header = f"当前: {cur[:8]}\n选一个渠道:" if cur else "当前: (无)\n选一个渠道:"
    return header, InlineKeyboardMarkup(buttons)


async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Two-level session picker.

    Level 1: 选渠道 (TG / 微信 / 终端 / 一次性). /resume 直接给的这层.
    Level 2: 选具体 session. 对应渠道内最近 5 条, 底部带"← 返回"回到 Level 1.

    两级设计避免跨渠道 session 混在一个 list 里噪音大, V 明确指定"在哪个渠道
    的历史里挑"让 picker 更聚焦. 仍然跨渠道可见 — 在 TG 里也能看到终端 /微信开的
    session, 只是需要先点对应按钮.
    """
    if not _allowed(update):
        return

    header, markup = _render_resume_channel_picker()
    await update.message.reply_text(header, reply_markup=markup)


async def on_resume_back(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Back-button callback: 从 Level 2 session 列表回到 Level 1 渠道 picker."""
    query = update.callback_query
    if not await _callback_allowed(query):
        return
    await query.answer()
    header, markup = _render_resume_channel_picker()
    try:
        await query.edit_message_text(header, reply_markup=markup)
    except Exception:
        pass


async def on_resume_channel_pick(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE,
) -> None:
    """Level-1 callback: 用户选了某个渠道类别, 列该类别最近 session."""
    query = update.callback_query
    if not await _callback_allowed(query):
        return
    await query.answer()
    data = query.data or ""
    if not data.startswith("resume-ch:"):
        return
    cat_id = data.split(":", 1)[1]

    cat = next((c for c in _resume_categories_for_current_cpu() if c[0] == cat_id), None)
    if not cat:
        try:
            await query.edit_message_text(f"❌ 当前 CPU 不支持该 resume 渠道: {cat_id}")
        except Exception:
            pass
        return
    _, cat_name, channel_labels, scan_all_buckets = cat

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    sessions = cc.list_recent_sessions(
        limit=5,
        channel_filter=channel_labels,
        scan_all_buckets=scan_all_buckets,
    )
    if not sessions:
        try:
            await query.edit_message_text(f"{cat_name}: 暂无历史 session")
        except Exception:
            pass
        return

    buttons = []
    for s in sessions:
        # preview 优先用 haiku 生成的一句话总结 (见 cc._spawn_summary_generation).
        # 首次缓存未命中时 fallback first_user, 下次 /resume 就能看见总结.
        preview = (s.get("preview") or s["first_user"]).replace("\n", " ").strip()
        if len(preview) > 48:
            preview = preview[:48] + "…"
        marker = "● " if s["is_current"] else ""
        # 每个 category 都是单 channel 白名单 (当前 bot 自己 / wx / term / oneshot),
        # 不会混入其他渠道 session, 所以不再加 [昵称] 前缀 tag.
        label = f"{marker}{_fmt_ago(s['mtime'])} · {preview}"
        # callback_data hard cap is 64 bytes. "resume:" (7) + uuid (36) = 43 ✓
        buttons.append([
            InlineKeyboardButton(label, callback_data=f"resume:{s['sid']}"),
        ])
    # 底部"← 返回"回到 Level 1 渠道 picker — V 选错渠道时不用重发 /resume.
    buttons.append([
        InlineKeyboardButton("← 返回", callback_data="resume-back"),
    ])
    try:
        await query.edit_message_text(
            f"{cat_name} 渠道最近 session, 选一个恢复:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    except Exception:
        pass


async def on_resume_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Level-2 callback: 用户选了具体 session, 切换 cc 活动 session."""
    query = update.callback_query
    if not await _callback_allowed(query):
        return
    await query.answer()

    data = query.data or ""
    if not data.startswith("resume:"):
        return
    sid = data.split(":", 1)[1]

    try:
        resumed = await _worker().resume(sid)
    except Exception as e:
        try:
            await query.edit_message_text(f"❌ resume 失败: {type(e).__name__}: {e}")
        except Exception:
            pass
        return
    if not resumed:
        try:
            await query.edit_message_text(f"❌ session {sid[:8]} 已失效 (JSONL 被清)")
        except Exception:
            pass
        return

    # Reset per-turn status accumulators so /status reflects the resumed
    # session's cost/model after its next turn, not the previous thread's
    # leftovers. Preserve _last_model/_last_context_window so /status before
    # the next turn still shows something concrete (scanner falls back to
    # JSONL anyway, but this avoids a misleading cost/tokens mismatch).
    global _session_cost, _session_turns, _last_used_tokens, _last_cost
    _session_cost = 0.0
    _session_turns = 0
    _last_used_tokens = 0
    _last_cost = 0.0
    _state["session_cost"] = 0.0
    _state["session_turns"] = 0
    _state["last_used_tokens"] = 0
    _state["last_cost"] = 0.0
    _save_state()

    # Cross-bucket fork: 跨 cwd-bucket 选的 sid, cc 已 import 成新 uuid 写到
    # babata bucket (见 cc._import_jsonl_to_bucket). 这里用 cc._session_id 反映
    # 真实激活的 sid, 让 V 知道发生了 fork.
    active_sid = cc._session_id or sid
    forked = active_sid != sid

    # 读 turn 用 *原* sid — 源文件还在原 bucket 完整可读, fork 后的副本内容跟原
    # 一致, 任选其一. 用原 sid 走 _find_jsonl_any_bucket 避免依赖 import 完整性.
    turns = cc.get_recent_turns(sid, pairs=2)
    head = (
        f"✅ 已恢复 <code>{sid[:8]}</code> "
        f"(fork → <code>{active_sid[:8]}</code>)"
        if forked else
        f"✅ 已恢复 <code>{active_sid[:8]}</code>"
    )
    if turns:
        blocks = []
        for role, text in turns:
            who = "V" if role == "user" else "CC"
            blocks.append(f"<b>{who}:</b> {html.escape(text)}")
        preview = "\n\n".join(blocks)
        body = (
            f"{head}\n\n"
            f"<blockquote>{preview}</blockquote>\n\n"
            "继续发消息即可。"
        )
    else:
        body = f"{head},继续发消息即可。"
    try:
        await query.edit_message_text(body, parse_mode="HTML")
    except Exception:
        # HTML rejection — retry plain.
        try:
            plain = (
                f"✅ 已恢复 {sid[:8]} (fork → {active_sid[:8]}),继续发消息即可。"
                if forked else
                f"✅ 已恢复 {active_sid[:8]},继续发消息即可。"
            )
            await query.edit_message_text(plain)
        except Exception:
            pass


async def on_button_click(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle MCP button click: resolve bridge future, send choice to CC."""
    query = update.callback_query
    if not await _callback_allowed(query):
        return
    await query.answer()

    data = query.data or ""
    if not data.startswith("mcp:"):
        return

    parts = data.split(":", 2)
    if len(parts) < 2:
        return

    option_index = int(parts[1])
    msg_id = query.message.message_id
    label = parts[2] if len(parts) > 2 else str(option_index)

    # Always remove buttons and show selection
    try:
        original = query.message.text or ""
        await query.edit_message_text(f"{original}\n\n\u2705 {label}")
    except Exception:
        pass

    # Try to resolve MCP future (CC is waiting for this)
    if not bridge.resolve(msg_id, option_index, label):
        # MCP future gone (timeout/session ended) — send choice as new message to CC
        await _process(update, ctx, label)


# ── Core flow ─────────────────────────────────────────────────────────

async def _process(
    update: Update,
    ctx: ContextTypes.DEFAULT_TYPE,
    text: str,
    images: list[dict[str, str]] | None = None,
) -> None:
    """Enqueue user input into the live CC session and return immediately."""
    update_id = getattr(update, "update_id", None)
    # Idempotency: 重启后 TG 重交付的 update 跳过, V 不会看到重做的 reply.
    if update_id in _processed_set:
        log.info("idempotent skip: update_id=%s already processed", update_id)
        return
    if await _pending_update_exists(update_id):
        try:
            if await _worker().has_update_id(update_id):
                log.info("idempotent skip: update_id=%s already queued", update_id)
                return
        except RuntimeError:
            pass
    chat = update.effective_chat
    msg = update.effective_message

    # Physical: reply/quote content isn't in msg.text, must prepend
    reply = getattr(msg, "reply_to_message", None)
    if reply:
        quote = getattr(msg, "quote", None)
        quoted = (
            (quote and quote.text)
            or reply.text or reply.caption
            or (reply.document and f"[a file: {reply.document.file_name}]")
            or (reply.photo and "[a photo]")
            or (reply.voice and "[a voice]")
            or (reply.audio and "[an audio]")
            or "[a message]"
        )
        text = f"[Replying to]: {quoted}\n\n{text}"

    try:
        payload = Payload(
            update=update,
            ctx=ctx,
            text=text,
            images=images,
            update_id=update_id,
        )
        await _record_pending_payload(payload)
        with suppress(Exception):
            await chat.send_action("typing")
        await _worker().submit(payload)
    except Exception as e:
        log.error("enqueue failed: %s", e)
        await msg.reply_text(f"Error: {e}")


def _with_transcript(source: str, handler):
    @wraps(handler)
    async def wrapped(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        record_update(update, source)
        with transcript_source(source):
            await handler(update, ctx)

    return wrapped


# ── Main ──────────────────────────────────────────────────────────────

async def _post_init(app: Application) -> None:
    global _channel_worker
    install_bot_transcript(app.bot)
    await bridge.start()
    asyncio.create_task(_heartbeat_loop(app))
    # Default context so terminal CC (no TG message yet) can push to user's TG
    if ALLOWED_USER:
        bridge.set_context(app.bot, ALLOWED_USER, None)
    _channel_worker = ChannelWorker(cc, instance_label=_CURRENT_LABEL)
    await _channel_worker.start()
    # Graceful shutdown: 覆盖 PTB/asyncio 默认 signal handler, 等 live turn
    # 跑完再退 (cmd_restart / launchd SIGTERM / Ctrl+C 都走这条).
    _install_signal_handlers(app)
    await _sync_bot_commands(app.bot)
    await _replay_pending_updates(app)

    # 意外重启 / launchd kickstart / 任务中 /restart → bot 重连后主动告知 V 当
    # 前 session 号. 不走 hook (hook 只在 session 边界触发, bot 重启时 sid 没变).
    # sid 可能为 None (新进程还没跑过任何 CC session) — 显示 (new) 提示 V "接下来
    # 第一句话会开一个新 session".
    #
    # 孤儿 turn 告警: graceful shutdown 正常走完不会留孤儿; 但 SIGKILL / OOM /
    # 硬崩 绕过 graceful 时, CC CLI 被强杀来不及写完 assistant turn, jsonl 里会
    # 留一条 user 没回复. 检测到就附警告让 V 决定 /resume (看片段) 或 /new.
    if ALLOWED_USER:
        sid = cc._session_id
        sid_display = sid if sid else "(new)"
        lines = [f"[{_CURRENT_LABEL}] 上线 · session: {sid_display}"]
        # SIGKILL 兜底: graceful shutdown 没跑过 (e.g. poll-healthcheck SIGKILL),
        # reason file 还在, 启动时读出来报告。Graceful 路径已 unlink, 这里 None.
        startup_trigger = _pop_restart_reason()
        if startup_trigger:
            lines.append(f"上次重启原因: {startup_trigger}")
        if sid and cc.is_last_turn_orphan(sid):
            lines.append("⚠️ 上次 session 最后一条 user 无 assistant 回复 (可能 SIGKILL)")
        try:
            await app.bot.send_message(ALLOWED_USER, "\n".join(lines))
        except Exception as e:
            log.warning("startup notice send failed: %s", e)


def _spawn_weixin_if_configured() -> "subprocess.Popen | None":
    """装了 WX 就 spawn weixin_bot.py 子进程 — 让 babata 一条命令跑 TG+WX.
    判据: ~/.babata/weixin/accounts/ 有 token 文件. 没有则 V 没装 WX, 不 spawn.
    生产 launchd 模式各 channel 独立 plist 跑, 通过 BABATA_NO_AUTO_WX=1 关掉.
    """
    if os.environ.get("BABATA_NO_AUTO_WX"):
        return None
    accounts = Path.home() / ".babata" / "weixin" / "accounts"
    if not accounts.exists() or not any(accounts.iterdir()):
        return None
    weixin_main = Path(__file__).parent / "weixin_bot.py"
    if not weixin_main.exists():
        return None
    log.info("WX channel detected — spawning weixin_bot.py")
    py = VENV_PYTHON if Path(VENV_PYTHON).exists() else "python3"
    proc = subprocess.Popen([py, str(weixin_main)])
    import atexit
    atexit.register(lambda: proc.terminate() if proc.poll() is None else None)
    return proc


def main() -> None:
    _spawn_weixin_if_configured()
    app = Application.builder().token(TOKEN).concurrent_updates(True).post_init(_post_init).build()

    app.add_handler(CommandHandler("status", _with_transcript("cmd_status", cmd_status)))
    app.add_handler(CommandHandler("context", _with_transcript("cmd_context", cmd_context)))
    app.add_handler(CommandHandler("verbose", _with_transcript("cmd_verbose", cmd_verbose)))
    app.add_handler(CommandHandler("cpu", _with_transcript("cmd_cpu", cmd_cpu)))
    app.add_handler(CommandHandler("resume", _with_transcript("cmd_resume", cmd_resume)))
    app.add_handler(CommandHandler("stop", _with_transcript("cmd_stop", cmd_stop)))
    app.add_handler(CommandHandler("restart", _with_transcript("cmd_restart", cmd_restart)))
    app.add_handler(CommandHandler("provider", _with_transcript("cmd_provider", cmd_provider)))
    app.add_handler(CommandHandler("new", _with_transcript("cmd_new", on_text)))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_verbose", on_verbose_click), pattern=r"^verbose:"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_cpu", on_cpu_click), pattern=r"^cpu:"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_provider", on_provider_click), pattern=r"^provider:"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_codex", on_codex_click), pattern=r"^codex:"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_codex_add", on_codex_add_click), pattern=r"^codex_add:"))
    # resume-ch: / resume-back / resume: 三个 pattern 互斥 (第 7 字符不同),
    # 注册顺序无关紧要; 仍按 specific → generic 排列利于阅读.
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_resume_channel", on_resume_channel_pick), pattern=r"^resume-ch:"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_resume_back", on_resume_back), pattern=r"^resume-back$"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_resume", on_resume_click), pattern=r"^resume:"))
    app.add_handler(CallbackQueryHandler(_with_transcript("cb_mcp", on_button_click), pattern=r"^mcp:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _with_transcript("text", on_text)))
    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, _with_transcript("voice", on_voice)))
    app.add_handler(MessageHandler(filters.PHOTO, _with_transcript("photo", on_photo)))
    app.add_handler(MessageHandler(filters.VIDEO | filters.VIDEO_NOTE, _with_transcript("video", on_video)))
    app.add_handler(MessageHandler(filters.Document.ALL, _with_transcript("document", on_document)))

    log.info("Bot starting (user: %s)", ALLOWED_USER)
    # stop_signals=None: 禁用 PTB 默认 SIGTERM/SIGINT 立即停止逻辑; 我们在
    # _post_init 里装 _install_signal_handlers, 走 graceful drain 路径.
    app.run_polling(drop_pending_updates=False, stop_signals=None)


if __name__ == "__main__":
    main()
