"""Codex CLI engine adapter for babata.

This is intentionally a sibling of ``cc.py`` rather than a rewrite of the
transport layer. TG/WeChat keep their existing channel semantics; this module
only swaps the CPU behind the same small Response/Event surface.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, AsyncIterator

from constants import HOOKS_DIR as _HOOKS_DIR
from cc import CC, Event, Response, StreamCB

log = logging.getLogger(__name__)

_CODEX_SESSIONS_KEY = "codex_sessions"
_CODEX_RECENT_LIMIT = 200
_CODEX_IMAGE_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
}


def _codex_cli_path() -> str:
    return (
        os.environ.get("BABATA_CODEX_CLI_PATH")
        or os.environ.get("CODEX_CLI_PATH")
        or "codex"
    )


def _codex_sandbox() -> str:
    configured = os.environ.get("BABATA_CODEX_SANDBOX")
    if configured:
        return configured
    return (
        "danger-full-access"
        if os.environ.get("BABATA_FULL_TRUST") == "1"
        else "workspace-write"
    )


def _codex_cwd() -> str:
    if os.environ.get("BABATA_FULL_TRUST") == "1":
        return str(Path.home())
    return str(Path(__file__).parent)


def _toml_key(key: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", key):
        return key
    return json.dumps(key)


def _toml_value(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(v) for v in value) + "]"
    if isinstance(value, dict):
        items = ", ".join(
            f"{_toml_key(str(k))} = {_toml_value(v)}"
            for k, v in value.items()
        )
        return "{ " + items + " }"
    raise TypeError(f"unsupported TOML value: {value!r}")


def _codex_mcp_overrides(mcp_servers: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for name, cfg in (mcp_servers or {}).items():
        if not isinstance(cfg, dict) or not cfg.get("command"):
            continue
        table: dict[str, Any] = {
            "command": str(cfg["command"]),
        }
        if cfg.get("args"):
            table["args"] = [str(a) for a in cfg["args"]]
        if cfg.get("env"):
            table["env"] = {str(k): str(v) for k, v in dict(cfg["env"]).items()}
        if cfg.get("cwd"):
            table["cwd"] = str(cfg["cwd"])
        args.extend(["-c", f"mcp_servers.{name}={_toml_value(table)}"])
    return args


def _decode_image_to_file(img: dict[str, str], directory: Path) -> Path:
    media_type = img.get("media_type") or "image/png"
    suffix = _CODEX_IMAGE_EXT.get(media_type, ".img")
    fp = directory / f"image-{time.time_ns()}{suffix}"
    fp.write_bytes(base64.b64decode(img["data"]))
    return fp


def _extract_tool_name(item: dict[str, Any]) -> str | None:
    item_type = str(item.get("type") or "")
    if item_type == "command_execution":
        command = str(item.get("command") or "shell")
        return command.split(None, 1)[0] if command else "shell"
    if item_type.endswith("tool_call") or item_type in {"mcp_call", "function_call"}:
        return str(item.get("name") or item.get("tool_name") or item_type)
    return None


class CodexEngine(CC):
    """One-shot Codex CLI backend with babata-compatible state."""

    def __init__(
        self,
        *,
        state_file: Path,
        source_prompt: str,
        mcp_servers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            state_file=state_file,
            source_prompt=source_prompt,
            mcp_servers=mcp_servers,
        )

    @property
    def session_id(self) -> str | None:
        return self._session_id

    async def query(
        self,
        prompt: str,
        images: list[dict[str, str]] | None = None,
        on_stream: StreamCB | None = None,
    ) -> Response:
        if prompt.strip() == "/new" and not images:
            self.reset()
            return Response(content="会话已重置。", session_id="", cost=0.0)

        self._check_idle_reset()
        return await self._run_codex(prompt, images, on_stream)

    async def _run_codex(
        self,
        prompt: str,
        images: list[dict[str, str]] | None,
        on_stream: StreamCB | None,
    ) -> Response:
        image_dir = Path(tempfile.mkdtemp(prefix="babata-codex-images."))
        last_file = Path(tempfile.mkstemp(prefix="babata-codex-last.", text=True)[1])
        image_paths: list[Path] = []
        try:
            for img in images or []:
                image_paths.append(_decode_image_to_file(img, image_dir))
            cmd = self._build_command(prompt, image_paths, last_file)
            result = await self._run_command(cmd, on_stream)
            content = result["content"]
            if last_file.is_file():
                with suppress(Exception):
                    file_content = last_file.read_text().strip()
                    if file_content:
                        content = file_content
            sid = result["sid"] or self._session_id or ""
            if sid:
                old_sid = self._session_id
                if sid != old_sid:
                    self._fire_hook(_HOOKS_DIR, "session-start.sh", sid)
                self._session_id = sid
                self._record_codex_turn(sid, prompt, content)

            return Response(
                content=content,
                session_id=sid,
                cost=0.0,
                tools=result["tools"],
                model=os.environ.get("BABATA_CODEX_MODEL") or "codex",
                input_tokens=result["usage"].get("input_tokens", 0),
                output_tokens=result["usage"].get("output_tokens", 0),
                cache_read_tokens=result["usage"].get("cached_input_tokens", 0),
            )
        finally:
            with suppress(Exception):
                last_file.unlink()
            for fp in image_paths:
                with suppress(Exception):
                    fp.unlink()
            with suppress(Exception):
                image_dir.rmdir()

    def _build_command(
        self,
        prompt: str,
        image_paths: list[Path],
        last_file: Path,
    ) -> list[str]:
        full_prompt = f"{self._source_prompt}\n\n{prompt}" if self._source_prompt else prompt
        base = [
            _codex_cli_path(),
            "-c", "notify=[]",
            "-c", 'approval_policy="never"',
            *_codex_mcp_overrides(self._mcp_servers),
        ]
        model = os.environ.get("BABATA_CODEX_MODEL")
        if model:
            base.extend(["-m", model])
        if os.environ.get("BABATA_CODEX_IGNORE_USER_CONFIG") == "1":
            exec_flags = ["--ignore-user-config"]
        else:
            exec_flags = []
        if os.environ.get("BABATA_CODEX_SEARCH") == "1":
            base.append("--search")

        image_args: list[str] = []
        for fp in image_paths:
            image_args.extend(["-i", str(fp)])

        if self._session_id:
            return [
                *base,
                "exec",
                "resume",
                *exec_flags,
                "--json",
                "--skip-git-repo-check",
                "-o", str(last_file),
                *image_args,
                self._session_id,
                full_prompt,
            ]
        return [
            *base,
            "exec",
            *exec_flags,
            "--json",
            "--skip-git-repo-check",
            "--sandbox", _codex_sandbox(),
            "-C", _codex_cwd(),
            "-o", str(last_file),
            *image_args,
            full_prompt,
        ]

    async def _run_command(self, cmd: list[str], on_stream: StreamCB | None) -> dict[str, Any]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert proc.stdout is not None
        assert proc.stderr is not None
        stderr_task = asyncio.create_task(proc.stderr.read())
        sid: str | None = None
        content = ""
        tools: list[str] = []
        usage: dict[str, int] = {}
        streamed = False

        try:
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    log.debug("codex non-json stdout: %s", line[:500])
                    continue
                etype = event.get("type")
                if etype == "thread.started":
                    sid = event.get("thread_id") or sid
                    continue
                if etype == "item.started":
                    item = event.get("item") or {}
                    name = _extract_tool_name(item)
                    if name and name not in tools:
                        tools.append(name)
                        if on_stream:
                            await on_stream(name, item, None, None)
                    continue
                if etype == "item.completed":
                    item = event.get("item") or {}
                    if item.get("type") == "agent_message":
                        text = str(item.get("text") or "").strip()
                        if text:
                            content = text
                            if on_stream and not streamed:
                                await on_stream(None, None, text, None)
                                streamed = True
                    else:
                        name = _extract_tool_name(item)
                        if name and name not in tools:
                            tools.append(name)
                    continue
                if etype == "turn.completed":
                    raw_usage = event.get("usage") or {}
                    usage = {
                        "input_tokens": int(raw_usage.get("input_tokens") or 0),
                        "cached_input_tokens": int(raw_usage.get("cached_input_tokens") or 0),
                        "output_tokens": int(raw_usage.get("output_tokens") or 0),
                    }
            rc = await proc.wait()
        except asyncio.CancelledError:
            with suppress(ProcessLookupError, Exception):
                proc.terminate()
            with suppress(asyncio.TimeoutError, Exception):
                await asyncio.wait_for(proc.wait(), timeout=2)
            stderr_task.cancel()
            with suppress(asyncio.CancelledError):
                await stderr_task
            raise

        stderr = (await stderr_task).decode("utf-8", errors="replace").strip()
        if rc != 0:
            raise RuntimeError(stderr or f"codex exited {rc}")
        return {"sid": sid, "content": content, "tools": tools, "usage": usage}

    def _record_codex_turn(self, sid: str, prompt: str, content: str) -> None:
        state = self._load_state()
        sessions = state.get(_CODEX_SESSIONS_KEY)
        if not isinstance(sessions, dict):
            sessions = {}
        now = time.time()
        rec = sessions.get(sid) if isinstance(sessions.get(sid), dict) else {}
        first_user = rec.get("first_user") or prompt
        turns = rec.get("turns") if isinstance(rec.get("turns"), list) else []
        turns.append(["user", prompt])
        if content:
            turns.append(["assistant", content])
        rec.update({
            "first_user": first_user,
            "preview": content or prompt,
            "mtime": now,
            "turns": turns[-8:],
        })
        sessions[sid] = rec
        state[_CODEX_SESSIONS_KEY] = sessions
        state["session_id"] = sid
        self._remember_engine_sid(state, sid)
        state["last_activity_at"] = now
        hist = [s for s in state.get("recent_sids", []) if s != sid]
        hist.insert(0, sid)
        state["recent_sids"] = hist[:_CODEX_RECENT_LIMIT]
        self._save_state(state)

    def list_recent_sessions(
        self,
        limit: int = 10,
        channel_filter: list[str] | None = None,
        scan_all_buckets: bool = False,
    ) -> list[dict]:
        del scan_all_buckets
        state = self._load_state()
        sessions = state.get(_CODEX_SESSIONS_KEY) or {}
        own_channel = self._channel_label()
        if channel_filter is not None and own_channel not in channel_filter:
            return []
        out: list[dict] = []
        for sid in state.get("recent_sids", []) or []:
            rec = sessions.get(sid) if isinstance(sessions, dict) else None
            if not isinstance(rec, dict):
                continue
            first_user = str(rec.get("first_user") or "")
            if not first_user:
                continue
            out.append({
                "sid": sid,
                "first_user": first_user,
                "preview": str(rec.get("preview") or first_user),
                "mtime": float(rec.get("mtime") or 0.0),
                "is_current": sid == self._session_id,
                "channel": own_channel,
                "is_own_channel": True,
            })
            if len(out) >= limit:
                break
        return out

    def resume(self, sid: str) -> bool:
        sessions = self._load_state().get(_CODEX_SESSIONS_KEY) or {}
        if sid not in sessions:
            return False
        old_sid = self._session_id
        self._session_id = sid
        self._record_sid(sid)
        if old_sid != sid:
            if old_sid:
                self._fire_hook(_HOOKS_DIR, "session-end.sh", old_sid)
            self._fire_hook(_HOOKS_DIR, "session-start.sh", sid)
        return True

    def get_recent_turns(
        self,
        sid: str,
        pairs: int = 2,
        char_cap: int = 400,
    ) -> list[tuple[str, str]]:
        sessions = self._load_state().get(_CODEX_SESSIONS_KEY) or {}
        rec = sessions.get(sid) if isinstance(sessions, dict) else None
        turns = rec.get("turns") if isinstance(rec, dict) else None
        if not isinstance(turns, list):
            return []
        out: list[tuple[str, str]] = []
        for item in turns[-(2 * pairs):]:
            if not isinstance(item, list) or len(item) != 2:
                continue
            role, text = str(item[0]), str(item[1])
            if len(text) > char_cap:
                text = text[:char_cap].rstrip() + "..."
            out.append((role, text))
        return out

    def is_last_turn_orphan(self, sid: str | None = None) -> bool:
        del sid
        return False

    async def context_usage(self) -> dict[str, Any]:
        raise RuntimeError("/context is not supported by the Codex engine yet")

    def _channel_label(self) -> str:
        from cc import _channel_label_from_state_file
        return _channel_label_from_state_file(self._state_file)


class CodexLiveSession(CodexEngine):
    """LiveSession-shaped wrapper backed by one Codex CLI process per turn."""

    def __init__(
        self,
        *,
        state_file: Path,
        source_prompt: str,
        mcp_servers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            state_file=state_file,
            source_prompt=source_prompt,
            mcp_servers=mcp_servers,
        )
        self._events: asyncio.Queue[Event | None] = asyncio.Queue()
        self._turn_task: asyncio.Task[None] | None = None
        self._closed = True

    @property
    def is_connected(self) -> bool:
        return not self._closed

    async def connect(self) -> None:
        self._closed = False
        self._check_idle_reset()

    async def close(self) -> None:
        self._closed = True
        await self._cancel_turn()
        await self._events.put(None)
        if self._session_id:
            self._fire_hook(_HOOKS_DIR, "session-end.sh", self._session_id)

    async def reset_live(self) -> Response:
        await self._cancel_turn()
        self.reset()
        return Response(content="会话已重置。", session_id="", cost=0.0)

    async def resume_live(self, sid: str) -> bool:
        await self._cancel_turn()
        return self.resume(sid)

    def submit(self, prompt: str, images: list[dict[str, str]] | None = None) -> None:
        if self._closed:
            raise RuntimeError("CodexLiveSession is not connected")
        if self._turn_task and not self._turn_task.done():
            raise RuntimeError("CodexLiveSession already has an active turn")
        self._turn_task = asyncio.create_task(self._run_live_turn(prompt, images))

    async def interrupt(self) -> None:
        # Codex exec has no stable hot-input control API here. Queue-mode cut-in
        # is safer: ChannelWorker will submit the pending payload after turn_end.
        return None

    async def events(self) -> AsyncIterator[Event]:
        while not self._closed:
            ev = await self._events.get()
            if ev is None:
                return
            yield ev

    async def _cancel_turn(self) -> None:
        task = self._turn_task
        self._turn_task = None
        if task and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def _run_live_turn(
        self,
        prompt: str,
        images: list[dict[str, str]] | None,
    ) -> None:
        try:
            async def _on_stream(tool_name, tool_input, text_chunk, tool_result) -> None:
                if tool_name:
                    await self._events.put(Event(
                        kind="tool_use",
                        name=tool_name,
                        input_dict=tool_input or {},
                    ))
                if tool_result:
                    await self._events.put(Event(
                        kind="tool_result",
                        is_error=bool(tool_result.get("is_error")),
                        text=str(tool_result.get("text") or ""),
                    ))
                if text_chunk:
                    await self._events.put(Event(kind="text_delta", chunk=text_chunk))

            old_sid = self._session_id
            resp = await self.query(prompt, images, _on_stream)
            if resp.session_id and resp.session_id != old_sid:
                await self._events.put(Event(
                    kind="session_changed",
                    old_sid=old_sid,
                    new_sid=resp.session_id,
                ))
            await self._events.put(Event(kind="turn_end", response=resp))
        except Exception as e:
            await self._events.put(Event(kind="error", exception=e))
