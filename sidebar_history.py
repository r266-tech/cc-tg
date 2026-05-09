"""Sidebar chat history — UI state persistence across mount/refresh.

cc.py session 文件持久化 LLM 端 turn state, 但 sidepanel React state 在 unmount
时丢失. 这个模块独立持久化 V 看到的 turn-by-turn UI history, 让 sidebar /
chat popup / 刷新页面都能恢复完整聊天记录.

跟 sidebar_events.jsonl 区别:
- events 是 url-anchored 事实流 (audit + page memory + viewport / attention)
- history 是 chronological chat turns (V 发了什么 / babata 回了什么)

Schema (jsonl, append-only):
  {ts, role: "user"|"assistant"|"boundary", text, url?, title?, has_image?, has_video?, has_file?}
  boundary 标记 V 触发新对话 (/new) — UI mount 时只拉最后一个 boundary 之后的 turns.
"""

import json
import logging
import time
from pathlib import Path
from threading import Lock
from typing import Any

log = logging.getLogger(__name__)

HISTORY_DIR = Path.home() / ".babata" / "sidebar"
HISTORY_FILE = HISTORY_DIR / "chat_history.jsonl"
_lock = Lock()

HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _probe_persistence() -> None:
    """Module-load 时 fail-fast probe — 防 silent storage failure.

    sidebar_history.append 静默失败会让 sidepanel mount 拉不到历史. 启动时
    探一下能不能写, 写不动就在 launchd stderr 留 ERROR (V 后续 grep 可见).
    """
    try:
        marker = HISTORY_DIR / ".history_probe"
        marker.write_text("ok", encoding="utf-8")
        marker.unlink()
    except OSError as e:
        log.error("sidebar_history persistence broken: %s — chat history will silently fail", e)


_probe_persistence()


def append(role: str, text: str, **fields: Any) -> None:
    rec: dict[str, Any] = {
        "ts": int(time.time()),
        "role": role,
        "text": text or "",
    }
    rec.update(fields)
    try:
        line = json.dumps(rec, ensure_ascii=False) + "\n"
    except (TypeError, ValueError):
        return
    try:
        with _lock:
            with HISTORY_FILE.open("a", encoding="utf-8") as f:
                f.write(line)
    except OSError as e:
        # 静默失败会让 sidepanel mount 拉不到历史 — 必须 surface 到 launchd stderr.
        log.warning("sidebar_history.append failed (role=%s): %s", role, e)


def boundary() -> None:
    """Write a session boundary record. UI mount filters to last boundary onwards."""
    append("boundary", "")


def read_since_last_boundary(limit: int = 200) -> list[dict]:
    """Return turns since the most recent boundary (inclusive of new session).
    Capped at limit (newest first capped, then re-ordered chronologically)."""
    if not HISTORY_FILE.exists():
        return []
    try:
        with HISTORY_FILE.open("r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return []

    records: list[dict] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        records.append(rec)

    # 找最后一个 boundary, 取其后所有 turn (boundary 自己不返).
    last_boundary = -1
    for i in range(len(records) - 1, -1, -1):
        if records[i].get("role") == "boundary":
            last_boundary = i
            break
    after = records[last_boundary + 1 :]
    # 仅 user/assistant turn.
    turns = [r for r in after if r.get("role") in {"user", "assistant"}]
    if len(turns) > limit:
        turns = turns[-limit:]
    return turns
