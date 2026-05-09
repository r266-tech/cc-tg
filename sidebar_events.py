"""babata sidebar events.jsonl — unified fact stream for audit + page memory.

三线 (translate / chat / proactive / attention) 都 append. chat 进 prompt 时
grep 当前 url 最近事件摘要塞 page_context (= page memory).

哲学: 事实层 > 规则层. 三线不直接互通, 都基于同一事实流自决.

Schema (松散, 每条带 ts/url/kind, 余字段按 kind 自由):
  translate_hit       — L1/L2 cache 命中, 不调 LLM
  translate_spawn     — spawn `claude -p` 翻 N 段
  translate_done      — spawn 完成 (含 spawn_ms 用时)
  translate_fail      — spawn 失败 (timeout / parse / rc)
  chat_turn           — V 在 sidepanel 发了一条
  proactive_run       — V 切 tab 触发 proactive (含 url/title)
  viewport            — content script 推 viewport_hashes (V 当前可见段)
  attention           — content script 推 visibility/sidepanel/idle
"""

import json
import logging
import time
from pathlib import Path
from threading import Lock
from typing import Any

log = logging.getLogger(__name__)

EVENTS_DIR = Path.home() / ".babata" / "sidebar"
EVENTS_FILE = EVENTS_DIR / "events.jsonl"
_lock = Lock()

EVENTS_DIR.mkdir(parents=True, exist_ok=True)


def append(url: str, kind: str, **fields: Any) -> None:
    """Append one event. Best-effort, log IO errors (静默失败会让 page_memory 假成功)."""
    rec: dict[str, Any] = {
        "ts": int(time.time()),
        "url": url or "",
        "kind": kind,
    }
    rec.update(fields)
    try:
        line = json.dumps(rec, ensure_ascii=False) + "\n"
    except (TypeError, ValueError):
        line = json.dumps({"ts": rec["ts"], "url": rec["url"], "kind": kind, "_serialize_error": True}) + "\n"
    try:
        with _lock:
            with EVENTS_FILE.open("a", encoding="utf-8") as f:
                f.write(line)
    except OSError as e:
        log.warning("sidebar_events.append failed (kind=%s url=%s): %s", kind, url, e)


def grep_url(url: str, max_records: int = 100, max_age_sec: int = 86400 * 30) -> list[dict]:
    """Return events for given url, oldest first, capped at max_records (most recent N).

    max_age_sec: 默认 30 天内. 超龄事件不返 (page memory 自然衰减).
    """
    if not url:
        return []
    cutoff = int(time.time()) - max_age_sec
    out: list[dict] = []
    try:
        with EVENTS_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("url") != url:
                    continue
                if int(rec.get("ts", 0)) < cutoff:
                    continue
                out.append(rec)
    except OSError:
        return []
    return out[-max_records:]


def summarize_for_chat(url: str) -> str:
    """Compress events of a url into 1-2 short lines for page_context injection.

    返两段:
      [page_state: ...]   — 当前活态 (最近 5min: viewport / attention)
      [page_memory: ...]  — 历史档案 (跨访问累积)

    都返空 = 第一次看 + 当前没态. LLM 自然不会引用.
    """
    events = grep_url(url)
    if not events:
        return ""

    now = int(time.time())
    recent_cutoff = now - 300  # 5min 内 = 当前活态
    recent = [e for e in events if int(e.get("ts", 0)) >= recent_cutoff]

    state_line = _format_page_state(recent)
    memory_line = _format_page_memory(events, now)
    return "\n".join(s for s in (state_line, memory_line) if s)


def _format_page_state(recent: list[dict]) -> str:
    """最近 5min 内 viewport / attention. V 现在视口几段 / 是不是在看."""
    if not recent:
        return ""
    last_viewport = next(
        (e for e in reversed(recent) if e.get("kind") == "viewport"), None,
    )
    last_attention = next(
        (e for e in reversed(recent) if e.get("kind") == "attention"), None,
    )
    parts: list[str] = []
    if last_viewport:
        n = last_viewport.get("visible_n") or len(last_viewport.get("visible_hashes") or [])
        if n:
            parts.append(f"{n} segments visible")
    if last_attention:
        if last_attention.get("visibility") == "hidden":
            parts.append("tab hidden")
        if last_attention.get("focus") == "no":
            parts.append("window unfocused")
        if last_attention.get("idle") == "yes":
            idle_s = last_attention.get("idle_sec") or 0
            parts.append(f"idle {_humanize_age(int(idle_s))}")
    if not parts:
        return ""
    return "[page_state: " + " | ".join(parts) + "]"


def _format_page_memory(events: list[dict], now: int) -> str:
    last_ts = max(int(e.get("ts", 0)) for e in events)
    age_sec = max(0, now - last_ts)
    age_label = _humanize_age(age_sec)

    n_translate = sum(1 for e in events if e.get("kind", "").startswith("translate_"))
    n_chat = sum(1 for e in events if e.get("kind") == "chat_turn")
    n_proactive = sum(1 for e in events if e.get("kind") == "proactive_run")

    last_chat = next(
        (e for e in reversed(events) if e.get("kind") == "chat_turn"), None,
    )

    parts = [f"last activity {age_label} ago"]
    if n_translate:
        parts.append(f"{n_translate} translate events")
    if n_chat:
        parts.append(f"{n_chat} chat turns")
        if last_chat and last_chat.get("message"):
            preview = str(last_chat["message"])[:80].replace("\n", " ")
            parts.append(f'last said: "{preview}"')
    if n_proactive:
        parts.append(f"{n_proactive} proactive runs")

    return "[page_memory: " + " | ".join(parts) + "]"


def _humanize_age(sec: int) -> str:
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m"
    if sec < 86400:
        return f"{sec // 3600}h"
    return f"{sec // 86400}d"
