import asyncio
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

import codex_engine
import engine


class FakeStream:
    def __init__(self, lines: list[str] | None = None, body: bytes = b""):
        self._lines = [line.encode() for line in (lines or [])]
        self._body = body

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._lines:
            raise StopAsyncIteration
        return self._lines.pop(0)

    async def read(self):
        return self._body


class RaisingStream(FakeStream):
    def __init__(self, exc: Exception):
        super().__init__([])
        self._exc = exc

    async def __anext__(self):
        raise self._exc


class FakeProcess:
    def __init__(self, lines: list[str], returncode: int = 0, stderr: bytes = b""):
        self.stdout = FakeStream(lines)
        self.stderr = FakeStream(body=stderr)
        self._returncode = returncode
        self.terminated = False

    async def wait(self):
        return self._returncode

    def terminate(self):
        self.terminated = True


def _json_line(payload: dict) -> str:
    return json.dumps(payload) + "\n"


def test_codex_engine_query_parses_json_and_persists(monkeypatch, tmp_path):
    captured = {}
    lines = [
        _json_line({"type": "thread.started", "thread_id": "sid-1"}),
        _json_line({
            "type": "item.started",
            "item": {
                "id": "item_0",
                "type": "command_execution",
                "command": "/bin/zsh -lc pwd",
            },
        }),
        _json_line({
            "type": "item.completed",
            "item": {"id": "item_1", "type": "agent_message", "text": "OK"},
        }),
        _json_line({
            "type": "turn.completed",
            "usage": {
                "input_tokens": 10,
                "cached_input_tokens": 4,
                "output_tokens": 2,
            },
        }),
    ]

    async def fake_create(*cmd, **kwargs):
        captured["cmd"] = list(cmd)
        captured["kwargs"] = kwargs
        return FakeProcess(lines)

    async def run():
        monkeypatch.setattr(codex_engine.asyncio, "create_subprocess_exec", fake_create)
        session = codex_engine.CodexEngine(
            state_file=tmp_path / "session.json",
            source_prompt="Source: test.",
            mcp_servers={"tg": {"command": "python", "args": ["tg_mcp.py"], "env": {"S": "x"}}},
        )
        monkeypatch.setattr(session, "_fire_hook", lambda *_: None)
        streamed = []
        resp = await session.query(
            "hello",
            on_stream=lambda tool, inp, text, result: streamed.append((tool, text)) or asyncio.sleep(0),
        )
        assert resp.content == "OK"
        assert resp.session_id == "sid-1"
        assert resp.tools == ["/bin/zsh"]
        assert resp.input_tokens == 10
        assert resp.cache_read_tokens == 4
        assert resp.output_tokens == 2
        assert streamed == [("/bin/zsh", None), (None, "OK")]
        assert "mcp_servers.tg" in " ".join(captured["cmd"])
        assert captured["kwargs"]["stdin"] is codex_engine.asyncio.subprocess.DEVNULL
        assert captured["kwargs"]["limit"] == codex_engine._CODEX_STREAM_LIMIT
        state = json.loads((tmp_path / "session.json").read_text())
        assert state["session_id"] == "sid-1"
        assert state["recent_sids"] == ["sid-1"]
        assert state["codex_sessions"]["sid-1"]["turns"][-2:] == [["user", "hello"], ["assistant", "OK"]]

    asyncio.run(run())


def test_codex_engine_streams_tool_results(monkeypatch, tmp_path):
    lines = [
        _json_line({"type": "thread.started", "thread_id": "sid-tools"}),
        _json_line({
            "type": "item.started",
            "item": {
                "id": "call_0",
                "type": "function_call",
                "name": "browser_tab_list",
                "arguments": {"active": True},
            },
        }),
        _json_line({
            "type": "item.completed",
            "item": {
                "id": "output_0",
                "call_id": "call_0",
                "type": "function_call_output",
                "output": [{"title": "X 每日精华 | 2026-05-10"}],
            },
        }),
        _json_line({
            "type": "item.completed",
            "item": {"id": "item_1", "type": "agent_message", "text": "OK"},
        }),
    ]

    async def fake_create(*_cmd, **_kwargs):
        return FakeProcess(lines)

    async def run():
        monkeypatch.setattr(codex_engine.asyncio, "create_subprocess_exec", fake_create)
        session = codex_engine.CodexEngine(
            state_file=tmp_path / "session.json",
            source_prompt="Source: test.",
        )
        monkeypatch.setattr(session, "_fire_hook", lambda *_: None)
        streamed = []

        resp = await session.query(
            "list tabs",
            on_stream=lambda tool, inp, text, result: streamed.append((tool, text, result)) or asyncio.sleep(0),
        )

        assert resp.content == "OK"
        assert resp.tools == ["browser_tab_list"]
        assert streamed[0][0] == "browser_tab_list"
        assert streamed[1][2]["is_error"] is False
        assert "X 每日精华 | 2026-05-10" in streamed[1][2]["text"]
        assert streamed[2] == (None, "OK", None)

    asyncio.run(run())


def test_codex_engine_handles_stdout_reader_splitter_failure(monkeypatch, tmp_path):
    proc = FakeProcess([])
    proc.stdout = RaisingStream(ValueError("Separator is not found, and chunk exceed the limit"))

    async def fake_create(*_cmd, **_kwargs):
        return proc

    async def run():
        monkeypatch.setattr(codex_engine.asyncio, "create_subprocess_exec", fake_create)
        session = codex_engine.CodexEngine(
            state_file=tmp_path / "session.json",
            source_prompt="Source: test.",
        )

        resp = await session.query("hello")

        assert "long-output splitter limit" in resp.content
        assert proc.terminated is True

    asyncio.run(run())


def test_codex_engine_keeps_content_on_splitter_failure(monkeypatch, tmp_path):
    lines = [
        _json_line({"type": "thread.started", "thread_id": "sid-err"}),
        _json_line({
            "type": "item.completed",
            "item": {
                "id": "item_0",
                "type": "agent_message",
                "text": "usable answer",
            },
        }),
    ]

    async def fake_create(*_cmd, **_kwargs):
        return FakeProcess(
            lines,
            returncode=1,
            stderr=b"Separator is not found, and chunk exceed the limit",
        )

    async def run():
        monkeypatch.setattr(codex_engine.asyncio, "create_subprocess_exec", fake_create)
        session = codex_engine.CodexEngine(
            state_file=tmp_path / "session.json",
            source_prompt="Source: test.",
        )
        monkeypatch.setattr(session, "_fire_hook", lambda *_: None)

        resp = await session.query("hello")

        assert resp.content == "usable answer"
        assert resp.session_id == "sid-err"

    asyncio.run(run())


def test_codex_engine_handles_splitter_turn_failed_event(monkeypatch, tmp_path):
    lines = [
        _json_line({"type": "thread.started", "thread_id": "sid-failed"}),
        _json_line({
            "type": "turn.failed",
            "error": {"message": "Separator is found, but chunk is longer than limit"},
        }),
    ]

    async def fake_create(*_cmd, **_kwargs):
        return FakeProcess(lines, returncode=0)

    async def run():
        monkeypatch.setattr(codex_engine.asyncio, "create_subprocess_exec", fake_create)
        session = codex_engine.CodexEngine(
            state_file=tmp_path / "session.json",
            source_prompt="Source: test.",
        )
        monkeypatch.setattr(session, "_fire_hook", lambda *_: None)

        resp = await session.query("hello")

        assert resp.session_id == "sid-failed"
        assert "long-output splitter limit" in resp.content

    asyncio.run(run())


def test_codex_live_session_emits_events(monkeypatch, tmp_path):
    lines = [
        _json_line({"type": "thread.started", "thread_id": "sid-2"}),
        _json_line({
            "type": "item.completed",
            "item": {"id": "item_0", "type": "agent_message", "text": "DONE"},
        }),
        _json_line({"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 1}}),
    ]

    async def fake_create(*_cmd, **_kwargs):
        return FakeProcess(lines)

    async def run():
        monkeypatch.setattr(codex_engine.asyncio, "create_subprocess_exec", fake_create)
        session = codex_engine.CodexLiveSession(
            state_file=tmp_path / "session.json",
            source_prompt="Source: test.",
        )
        monkeypatch.setattr(session, "_fire_hook", lambda *_: None)
        await session.connect()
        agen = session.events()
        session.submit("go")
        events = [await agen.__anext__() for _ in range(3)]
        await agen.aclose()
        await session.close()
        assert [e.kind for e in events] == ["text_delta", "session_changed", "turn_end"]
        assert events[0].chunk == "DONE"
        assert events[1].new_sid == "sid-2"
        assert events[2].response.content == "DONE"

    asyncio.run(run())


def test_make_engine_selects_codex(monkeypatch, tmp_path):
    monkeypatch.setenv("BABATA_ENGINE", "codex")
    made = engine.make_engine(
        state_file=tmp_path / "session.json",
        source_prompt="Source: test.",
        live=True,
    )
    assert isinstance(made, codex_engine.CodexLiveSession)


def test_engine_state_overrides_env_and_keeps_engine_specific_sid(monkeypatch, tmp_path):
    state_file = tmp_path / "session.json"
    state_file.write_text(json.dumps({
        "assistant_engine": "codex",
        "session_id": "claude-sid",
        "engine_session_ids": {"codex": "codex-sid"},
    }))
    monkeypatch.setenv("BABATA_ENGINE", "claude")

    made = engine.make_engine(
        state_file=state_file,
        source_prompt="Source: test.",
        live=True,
    )

    assert isinstance(made, codex_engine.CodexLiveSession)
    assert made._session_id == "codex-sid"


def test_codex_without_engine_specific_sid_does_not_resume_claude_sid(tmp_path):
    state_file = tmp_path / "session.json"
    state_file.write_text(json.dumps({
        "assistant_engine": "codex",
        "session_id": "claude-sid",
    }))

    made = engine.make_engine(
        state_file=state_file,
        source_prompt="Source: test.",
        live=True,
    )

    assert isinstance(made, codex_engine.CodexLiveSession)
    assert made._session_id is None


def test_claude_record_sid_updates_engine_specific_slot(tmp_path):
    state_file = tmp_path / "session.json"
    made = engine.make_engine(
        state_file=state_file,
        source_prompt="Source: test.",
        live=False,
        engine="claude",
    )

    made._record_sid("claude-sid")

    state = json.loads(state_file.read_text())
    assert state["session_id"] == "claude-sid"
    assert state["engine_session_ids"]["claude"] == "claude-sid"
