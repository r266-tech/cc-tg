"""Assistant CPU selection for babata."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from cc import CC, LiveSession, VENV_PYTHON
from codex_engine import CodexEngine, CodexLiveSession

ENGINE_STATE_KEY = "assistant_engine"
ENGINE_SESSION_IDS_KEY = "engine_session_ids"

_ALIASES = {
    "claude": "claude",
    "cc": "claude",
    "claude-code": "claude",
    "codex": "codex",
    "openai-codex": "codex",
}


def normalize_engine(raw: str | None) -> str:
    name = (raw or "claude").strip().lower()
    normalized = _ALIASES.get(name)
    if not normalized:
        raise ValueError(f"unsupported assistant engine: {raw!r}")
    return normalized


def engine_label(name: str) -> str:
    normalized = normalize_engine(name)
    return "Claude Code" if normalized == "claude" else "Codex"


def engine_choices() -> list[tuple[str, str]]:
    return [("Claude Code", "claude"), ("Codex", "codex")]


def _load_state(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def engine_name(state_file: Path | None = None, *, override: str | None = None) -> str:
    if override:
        return normalize_engine(override)
    state = _load_state(state_file)
    configured = state.get(ENGINE_STATE_KEY)
    if isinstance(configured, str) and configured.strip():
        with_state = _ALIASES.get(configured.strip().lower())
        if with_state:
            return with_state
    raw = os.environ.get("BABATA_ENGINE") or os.environ.get("ASSISTANT_ENGINE") or "claude"
    return normalize_engine(raw)


def is_codex_engine(state_file: Path | None = None) -> bool:
    return engine_name(state_file) == "codex"


def persist_engine(state_file: Path, name: str) -> None:
    state = _load_state(state_file)
    state[ENGINE_STATE_KEY] = normalize_engine(name)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(state_file)


def _engine_session_id(state_file: Path, name: str) -> str | None:
    state = _load_state(state_file)
    engine_sids = state.get(ENGINE_SESSION_IDS_KEY)
    if isinstance(engine_sids, dict) and isinstance(engine_sids.get(name), str):
        return engine_sids[name] or None
    if name == "claude":
        sid = state.get("session_id")
        return sid if isinstance(sid, str) and sid else None
    return None


def make_engine(
    *,
    state_file: Path,
    source_prompt: str,
    mcp_servers: dict[str, Any] | None = None,
    live: bool = False,
    engine: str | None = None,
) -> CC | LiveSession | CodexEngine | CodexLiveSession:
    name = engine_name(state_file, override=engine)
    if name == "claude":
        cls = LiveSession if live else CC
        obj = cls(
            state_file=state_file,
            source_prompt=source_prompt,
            mcp_servers=mcp_servers,
        )
    elif name == "codex":
        cls = CodexLiveSession if live else CodexEngine
        obj = cls(
            state_file=state_file,
            source_prompt=source_prompt,
            mcp_servers=mcp_servers,
        )
    else:
        raise ValueError(f"unsupported assistant engine: {name!r}")
    setattr(obj, "_babata_engine_name", name)
    obj._session_id = _engine_session_id(state_file, name)
    return obj
