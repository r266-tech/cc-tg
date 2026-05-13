import asyncio
import json
import os
import sys
from pathlib import Path
import pytest

_REPO = Path(__file__).resolve().parents[1]
_SDK_SITE = next(iter((_REPO / ".venv/lib").glob("python*/site-packages")), None)
if _SDK_SITE:
    sys.path.insert(0, str(_SDK_SITE))
sys.path.insert(0, str(_REPO))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123:test")
os.environ.setdefault("ALLOWED_USER_ID", "0")

import bot
import bridge as tg_bridge


def test_short_bubble_uses_html_parse_mode():
    parts, parse_mode = bot._format_bubble_parts("**ok**")

    assert parts == ["<b>ok</b>"]
    assert parse_mode == "HTML"


def test_long_bubble_falls_back_to_plain_chunks():
    text = "<b>" + ("x" * 5000) + "</b>"

    parts, parse_mode = bot._format_bubble_parts(text)

    assert parse_mode is None
    assert len(parts) > 1
    assert all(bot._utf16_len(part) <= bot._MAX_TG for part in parts)


def test_long_link_falls_back_to_plain_chunks():
    text = "[docs](https://example.com/" + ("a" * 5000) + ")"

    parts, parse_mode = bot._format_bubble_parts(text)

    assert parse_mode is None
    assert len(parts) > 1
    assert all(bot._utf16_len(part) <= bot._MAX_TG for part in parts)


def test_fmt_tool_skips_codex_internal_item_id():
    line = bot._fmt_tool(
        "/bin/zsh",
        {
            "id": "item_1",
            "type": "command_execution",
            "command": "/bin/zsh -lc 'echo ok'",
        },
    )

    assert line == "💻 Shell · echo"
    assert "/bin/zsh" not in line
    assert "item_1" not in line


def test_fmt_tool_marks_skill_usage_from_shell_command():
    line = bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": (
                "/bin/zsh -lc \"sed -n '1,220p' "
                "~/cc-workspace/babata-skills/second-brain/SKILL.md\""
            ),
        },
    )

    assert line == "📚 Skill · second-brain"
    assert "sed -n" not in line


def test_fmt_tool_marks_memory_injection():
    line = bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": (
                "~/cc-workspace/bin/babata-memory-context "
                "--profile lite --cpu codex --source terminal --include-top force"
            ),
        },
    )

    assert line == "🧠 Memory · inject lite (L0+daily-map) · codex/terminal · top force"


def test_fmt_tool_summarizes_shell_find_with_target_patterns():
    line = bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": (
                "/bin/zsh -lc \"find ~/cc-workspace -maxdepth 4 "
                "\\( -iname '*light*' -o -iname '*home*' -o -iname '*hass*' "
                "-o -iname '*ha*' -o -iname '*mijia*' -o -iname '*yeelight*' \\) "
                "-not -path '*/node_modules/*' -print\""
            ),
        },
    )

    assert line == "📂 Find · cc-workspace · light/home/hass/ha/mijia/yeelight"
    assert "node_modules" not in line


def test_fmt_tool_summarizes_shell_file_reads_and_smart_home_commands():
    read_line = bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": "/bin/zsh -lc \"sed -n '1,220p' ~/cc-workspace/skills-catalog/home/README.md\"",
        },
    )
    home_line = bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": "~/cc-workspace/skills-catalog/home/smart-home/bin/ha light on",
        },
    )

    assert read_line == "📖 Read · home/README.md:1-220"
    assert home_line == "🏠 Smart-home · light on"


def test_fmt_tool_summarizes_common_operational_commands():
    assert bot._fmt_tool(
        "/bin/zsh",
        {"type": "command_execution", "command": "/bin/zsh -lc date"},
    ) == "🕑 Time · now"

    assert bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": "python3 -m pytest tests/test_tg_comm_safety.py -q",
        },
    ) == "✅ Test · test_tg_comm_safety.py"

    assert bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": (
                "for label in com.babata com.babata.vvv com.babata.vvvv com.babata.vvvvv; "
                "do DELAY=3 scripts/self-ops.sh restart \"$label\"; done"
            ),
        },
    ) == "🔁 Restart · TG bots"

    assert bot._fmt_tool(
        "/bin/zsh",
        {
            "type": "command_execution",
            "command": "sleep 8; launchctl list | rg 'com\\\\.babata'",
        },
    ) == "🚀 Launchd · list babata labels"


def test_fmt_tool_marks_subagent_and_web_search():
    assert bot._fmt_tool("Task", {"description": "review bot.py"}) == "👥 Subagent · review bot.py"
    assert bot._fmt_tool("WebSearch", {"query": "openclaw telegram progress"}) == (
        "🌐 WebSearch · openclaw telegram progress"
    )


class FakeUser:
    def __init__(self, user_id: int):
        self.id = user_id


class FakeCallbackMessage:
    def __init__(self):
        self.message_id = 123
        self.text = "pick one"
        self.edits = []

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class FakeCallbackQuery:
    def __init__(self, user_id: int, data: str = "mcp:0:danger"):
        self.from_user = FakeUser(user_id)
        self.data = data
        self.message = FakeCallbackMessage()
        self.answers = []

    async def answer(self, text=None, **kwargs):
        self.answers.append((text, kwargs))

    async def edit_message_text(self, text, **kwargs):
        await self.message.edit_message_text(text, **kwargs)


class FakeCallbackUpdate:
    def __init__(self, query):
        self.callback_query = query


def test_callback_allowed_rejects_wrong_user(monkeypatch):
    async def run():
        monkeypatch.setattr(bot, "ALLOWED_USER", 42)
        query = FakeCallbackQuery(user_id=99)

        assert await bot._callback_allowed(query) is False
        assert query.answers == [("auth denied", {})]

    asyncio.run(run())


def test_button_callback_denies_before_bridge_or_process(monkeypatch):
    async def run():
        monkeypatch.setattr(bot, "ALLOWED_USER", 42)
        query = FakeCallbackQuery(user_id=99)
        update = FakeCallbackUpdate(query)

        def fail_resolve(*args, **kwargs):
            raise AssertionError("unauthorized callback reached bridge")

        async def fail_process(*args, **kwargs):
            raise AssertionError("unauthorized callback reached CC")

        monkeypatch.setattr(bot.bridge, "resolve", fail_resolve)
        monkeypatch.setattr(bot, "_process", fail_process)

        await bot.on_button_click(update, object())

        assert query.answers == [("auth denied", {})]
        assert query.message.edits == []

    asyncio.run(run())


class FakeBridgeBot:
    def __init__(self):
        self.messages = []

    async def send_message(self, **kwargs):
        self.messages.append(kwargs)
        return type("Sent", (), {"message_id": len(self.messages)})()


class FakeWriter:
    def __init__(self):
        self.data = b""

    def write(self, data: bytes):
        self.data += data

    async def drain(self):
        pass


def test_bridge_send_text_chunks_long_messages():
    async def run():
        fake_bot = FakeBridgeBot()
        br = tg_bridge.TGBridge()
        br.set_context(fake_bot, chat_id=7, reply_to=11)
        writer = FakeWriter()

        await br._handle_send_text({"text": "x" * 9000}, writer)

        assert len(fake_bot.messages) > 1
        assert all(
            tg_bridge._utf16_len(item["text"]) <= tg_bridge._TG_MAX_MESSAGE - 96
            for item in fake_bot.messages
        )
        assert all(item["reply_to_message_id"] == 11 for item in fake_bot.messages)
        response = json.loads(writer.data.decode())
        assert response["result"] == f"Text sent ({len(fake_bot.messages)} chunks)"

    asyncio.run(run())


def test_tg_mcp_open_bridge_retries_during_bot_restart(monkeypatch):
    tg_mcp = pytest.importorskip("tg_mcp")

    async def run():
        calls = 0

        async def fake_open(path):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise FileNotFoundError("bridge missing")
            return object(), FakeWriter()

        monkeypatch.setattr(tg_mcp.asyncio, "open_unix_connection", fake_open)
        monkeypatch.setattr(tg_mcp, "_BRIDGE_CONNECT_RETRY_SECONDS", 1.0)
        monkeypatch.setattr(tg_mcp, "_BRIDGE_CONNECT_RETRY_INTERVAL", 0.01)

        reader, writer = await tg_mcp._open_bridge("/tmp/missing.sock")

        assert reader is not None
        assert writer is not None
        assert calls == 2

    asyncio.run(run())
