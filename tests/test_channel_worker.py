import asyncio
import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SDK_SITE = next(iter((_REPO / ".venv/lib").glob("python*/site-packages")), None)
if _SDK_SITE:
    sys.path.insert(0, str(_SDK_SITE))
sys.path.insert(0, str(_REPO))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123:test")
os.environ.setdefault("ALLOWED_USER_ID", "0")

import bot
from cc import Event, Response


class FakeBridge:
    def __init__(self):
        self.contexts = []

    def set_context(self, bot_obj, chat_id, reply_to=None):
        self.contexts.append((bot_obj, chat_id, reply_to))


class FakeSentMessage:
    _next_id = 100

    def __init__(self, text: str):
        self.message_id = FakeSentMessage._next_id
        FakeSentMessage._next_id += 1
        self.text = text
        self.edits = []
        self.deleted = False

    async def edit_text(self, text: str, parse_mode=None, reply_markup=None):
        self.text = text
        self.edits.append((text, parse_mode, reply_markup))

    async def delete(self):
        self.deleted = True


class FakeMessage:
    def __init__(self, message_id: int, text: str = ""):
        self.message_id = message_id
        self.text = text
        self.caption = None
        self.reply_to_message = None
        self.document = None
        self.photo = None
        self.voice = None
        self.audio = None
        self.replies: list[FakeSentMessage] = []

    async def reply_text(self, text: str, parse_mode=None, reply_markup=None):
        msg = FakeSentMessage(text)
        msg.parse_mode = parse_mode
        msg.reply_markup = reply_markup
        self.replies.append(msg)
        return msg


class FakeChat:
    def __init__(self, chat_id: int = 42):
        self.id = chat_id
        self.actions = []

    async def send_action(self, action: str):
        self.actions.append(action)


class FakeUser:
    def __init__(self, user_id: int):
        self.id = user_id


class FakeUpdate:
    def __init__(
        self,
        message: FakeMessage,
        chat: FakeChat,
        user_id: int | None = None,
        update_id: int | None = None,
    ):
        self.update_id = update_id
        self.effective_message = message
        self.message = message
        self.effective_chat = chat
        self.effective_user = FakeUser(user_id) if user_id is not None else None


class FakeCallbackQuery:
    def __init__(self, user_id: int, data: str):
        self.from_user = FakeUser(user_id)
        self.data = data
        self.answers = []
        self.edits = []

    async def answer(self, text=None, **kwargs):
        self.answers.append((text, kwargs))

    async def edit_message_text(self, text, **kwargs):
        self.edits.append((text, kwargs))


class FakeCallbackUpdate:
    def __init__(self, query: FakeCallbackQuery):
        self.callback_query = query


class FakeBot:
    """PTB Bot stub. Records set_message_reaction calls so tests can assert
    👀 fired at turn-begin and 👌 at turn-end."""

    def __init__(self):
        self.reactions: list[tuple[int, int, str]] = []
        self.commands: list[tuple[str, str]] = []
        self.sent_messages: list[dict] = []
        self.chat_actions: list[tuple[int, str]] = []

    async def set_message_reaction(self, *, chat_id, message_id, reaction):
        self.reactions.append((chat_id, message_id, reaction))

    async def set_my_commands(self, commands):
        self.commands = list(commands)

    async def send_chat_action(self, *, chat_id, action):
        self.chat_actions.append((chat_id, action))

    async def send_message(self, **kwargs):
        self.sent_messages.append(kwargs)
        return FakeSentMessage(kwargs["text"])


class FakeCtx:
    def __init__(self):
        self.bot = FakeBot()
        self.application = object()


class FakeSession:
    def __init__(self):
        self.connected = False
        self.closed = False
        self.interrupted = False
        self.submitted = []
        self.queue: asyncio.Queue = asyncio.Queue()

    async def connect(self):
        self.connected = True

    async def close(self):
        self.closed = True
        self.queue.put_nowait(None)

    def submit(self, text, images=None):
        self.submitted.append((text, images))

    async def interrupt(self):
        self.interrupted = True

    async def resume_live(self, sid: str):
        self.resumed = sid
        return True

    async def reset_live(self):
        self.reset = True
        return Response(content="会话已重置。", session_id="", cost=0.0)

    async def events(self):
        while True:
            ev = await self.queue.get()
            if ev is None:
                return
            yield ev


async def wait_for(predicate, timeout: float = 1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timed out waiting for predicate")


def reset_bot_globals(monkeypatch, tmp_path):
    monkeypatch.setattr(bot, "bridge", FakeBridge())
    monkeypatch.setattr(bot, "_STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(bot, "PROCESSED_UPDATES_FILE", tmp_path / "processed.json")
    monkeypatch.setattr(bot, "PENDING_UPDATES_FILE", tmp_path / "pending.json")
    bot._processed_set = set()
    bot._pending_update_records = {}
    bot._state = {}
    bot._verbose = 1
    bot._in_flight = 0
    bot._session_cost = 0.0
    bot._session_turns = 0
    bot._last_model = None
    bot._last_context_window = None
    bot._last_used_tokens = 0
    bot._last_cost = 0.0


class FakeCpuSession:
    def __init__(self, name: str, state_file: Path | None = None, sid: str | None = None):
        self._babata_engine_name = name
        self._state_file = state_file
        self._session_id = sid

    def _load_state(self):
        if self._state_file is None:
            return {}
        try:
            return json.loads(self._state_file.read_text())
        except Exception:
            return {}

    def _record_sid(self, sid: str | None):
        if self._state_file is None:
            return
        try:
            state = json.loads(self._state_file.read_text())
        except Exception:
            state = {}
        state["session_id"] = sid
        engine_sids = state.get("engine_session_ids")
        if not isinstance(engine_sids, dict):
            engine_sids = {}
        engine_sids[self._babata_engine_name] = sid or ""
        state["engine_session_ids"] = engine_sids
        self._state_file.write_text(json.dumps(state))


class FakeCpuWorker:
    instances: list["FakeCpuWorker"] = []

    def __init__(self, session, *, instance_label: str):
        self.session = session
        self.instance_label = instance_label
        self._turn_active = False
        self.started = False
        self.stopped = False
        FakeCpuWorker.instances.append(self)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True


def test_switch_cpu_rebuilds_worker_and_persists_choice(monkeypatch, tmp_path):
    async def run():
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps({"session_id": "claude-old"}))
        monkeypatch.setattr(bot, "SESSION_FILE", state_file)
        monkeypatch.setattr(bot, "_STATE_PATH", state_file)
        bot._state = {}
        bot._in_flight = 0
        monkeypatch.setattr(bot, "cc", FakeCpuSession("claude", state_file, "claude-old"))
        old_worker = FakeCpuWorker(bot.cc, instance_label="test")
        monkeypatch.setattr(bot, "_channel_worker", old_worker)
        FakeCpuWorker.instances = [old_worker]

        def fake_make(target=None):
            return FakeCpuSession(target or "claude")

        monkeypatch.setattr(bot, "_make_tg_engine", fake_make)
        monkeypatch.setattr(bot, "ChannelWorker", FakeCpuWorker)

        result = await bot._switch_cpu("codex")

        assert result == "CPU: Claude Code → Codex"
        assert old_worker.stopped is True
        assert bot._channel_worker is FakeCpuWorker.instances[-1]
        assert bot._channel_worker.started is True
        assert bot._current_cpu_name() == "codex"
        state = json.loads(state_file.read_text())
        assert state["assistant_engine"] == "codex"
        assert state["engine_session_ids"]["claude"] == "claude-old"

    asyncio.run(run())


def test_bot_commands_are_filtered_by_cpu():
    claude = [name for name, _ in bot._bot_commands_for_cpu("claude")]
    codex = [name for name, _ in bot._bot_commands_for_cpu("codex")]

    assert "context" in claude
    assert "stop" in claude
    assert "provider" in claude
    assert "context" not in codex
    assert "stop" not in codex
    assert "provider" in codex
    assert {"new", "resume", "status", "verbose", "cpu", "restart", "provider"} <= set(codex)


def test_codex_rejects_hidden_claude_commands_and_shows_provider(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        monkeypatch.setattr(bot, "ALLOWED_USER", 7)
        monkeypatch.setattr(bot, "cc", FakeCpuSession("codex"))
        monkeypatch.setattr(bot, "_current_codex_key", lambda: "personal")
        monkeypatch.setattr(bot, "_current_codex_label", lambda: "Codex · personal")
        monkeypatch.setattr(bot, "_codex_choices", lambda: [("Codex · personal", "personal")])

        ctx = FakeCtx()
        chat = FakeChat()

        context_msg = FakeMessage(10, "/context")
        await bot.cmd_context(FakeUpdate(context_msg, chat, user_id=7), ctx)
        assert "不支持" in context_msg.replies[-1].text

        stop_msg = FakeMessage(11, "/stop")
        await bot.cmd_stop(FakeUpdate(stop_msg, chat, user_id=7), ctx)
        assert "不支持" in stop_msg.replies[-1].text

        provider_msg = FakeMessage(12, "/provider")
        await bot.cmd_provider(FakeUpdate(provider_msg, chat, user_id=7), ctx)
        assert "Codex 账号" in provider_msg.replies[-1].text

    asyncio.run(run())


def test_codex_rejects_provider_callback(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        monkeypatch.setattr(bot, "ALLOWED_USER", 7)
        monkeypatch.setattr(bot, "cc", FakeCpuSession("codex"))

        async def fail_switch(*args, **kwargs):
            raise AssertionError("provider switch should not run in Codex mode")

        monkeypatch.setattr(bot, "_run_cc_router_switch", fail_switch)
        query = FakeCallbackQuery(user_id=7, data="provider:openrouter")

        await bot.on_provider_click(FakeCallbackUpdate(query), FakeCtx())

        assert query.answers == [(None, {})]
        assert "失效" in query.edits[-1][0]

    asyncio.run(run())


def test_codex_resume_picker_only_shows_current_channel(monkeypatch, tmp_path):
    reset_bot_globals(monkeypatch, tmp_path)
    monkeypatch.setattr(bot, "cc", FakeCpuSession("codex", sid="sid-12345678"))

    _header, markup = bot._render_resume_channel_picker()

    buttons = markup[0][0]
    assert len(buttons) == 1
    button = buttons[0][0]
    assert button[0][0] == "当前 Codex"
    assert button[1]["callback_data"] == "resume-ch:tg"


def test_codex_status_reads_session_usage(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        monkeypatch.setattr(bot, "ALLOWED_USER", 7)
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps({"recent_sids": ["sid-1"]}))
        monkeypatch.setattr(bot, "cc", FakeCpuSession("codex", state_file, "sid-1"))
        monkeypatch.setattr(bot, "_last_model", "codex")
        monkeypatch.setattr(bot, "_codex_version", lambda: "0.128.0")
        session_file = tmp_path / "rollout-sid-1.jsonl"
        session_file.write_text("\n".join([
            json.dumps({
                "type": "turn_context",
                "payload": {
                    "model": "gpt-5.5",
                    "effort": "xhigh",
                },
            }),
            json.dumps({
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "last_token_usage": {
                            "input_tokens": 1000,
                            "output_tokens": 200,
                            "reasoning_output_tokens": 50,
                        },
                        "model_context_window": 2000,
                    },
                },
                "rate_limits": {
                    "primary": {"used_percent": 12, "resets_at": 1_778_418_860},
                    "secondary": {"used_percent": 34, "resets_at": 1_778_911_266},
                    "plan_type": "prolite",
                },
            }),
        ]))
        monkeypatch.setattr(bot, "_codex_session_file", lambda sid: session_file)
        monkeypatch.setattr(bot, "_codex_sessions_root", lambda: tmp_path)
        monkeypatch.setattr(bot, "_codex_config", lambda: {"model": "gpt-5.5", "model_reasoning_effort": "xhigh"})
        monkeypatch.setattr(bot, "_fetch_codex_app_rate_limits", lambda: asyncio.sleep(0, result=None))

        msg = FakeMessage(13, "/status")
        await bot.cmd_status(FakeUpdate(msg, FakeChat(), user_id=7), FakeCtx())

        text = msg.replies[-1].text
        assert "50%" in text
        assert "gpt-5.5 xhigh" in text
        assert "1.0K in" in text
        assert "200 out" in text
        assert "50 reasoning" in text
        assert "5h limit 88% left" in text
        assert "weekly limit 66% left" in text
        assert "plan prolite" in text
        assert "Codex v0.128.0" in text
        assert "current <code>gpt-5.5</code> · effort <code>xhigh</code>" not in text

    asyncio.run(run())


def test_codex_status_prefers_live_app_server_limits(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        monkeypatch.setattr(bot, "ALLOWED_USER", 7)
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps({"recent_sids": ["sid-1"]}))
        monkeypatch.setattr(bot, "cc", FakeCpuSession("codex", state_file, "sid-1"))
        monkeypatch.setattr(bot, "_last_model", "codex")
        monkeypatch.setattr(bot, "_codex_version", lambda: "0.128.0")
        session_file = tmp_path / "rollout-sid-1.jsonl"
        session_file.write_text("\n".join([
            json.dumps({
                "type": "turn_context",
                "payload": {"model": "gpt-5.5", "effort": "xhigh"},
            }),
            json.dumps({
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "last_token_usage": {"input_tokens": 1000},
                        "model_context_window": 2000,
                    },
                },
                "rate_limits": {
                    "primary": {"used_percent": 12, "resets_at": 1_778_418_860},
                    "secondary": {"used_percent": 34, "resets_at": 1_778_911_266},
                    "plan_type": "stale",
                },
            }),
        ]))
        monkeypatch.setattr(bot, "_codex_session_file", lambda sid: session_file)
        monkeypatch.setattr(bot, "_codex_config", lambda: {"model": "gpt-5.5", "model_reasoning_effort": "xhigh"})
        monkeypatch.setattr(bot, "_fetch_codex_app_rate_limits", lambda: asyncio.sleep(0, result={
            "primary": {"used_percent": 29, "window_minutes": 300, "resets_at": 1_778_494_266},
            "secondary": {"used_percent": 33, "window_minutes": 10_080, "resets_at": 1_778_911_266},
            "plan_type": "prolite",
        }))

        msg = FakeMessage(13, "/status")
        await bot.cmd_status(FakeUpdate(msg, FakeChat(), user_id=7), FakeCtx())

        text = msg.replies[-1].text
        assert "5h limit 71% left" in text
        assert "weekly limit 67% left" in text
        assert "plan prolite" in text
        assert "5h limit 88% left" not in text
        assert "plan stale" not in text

    asyncio.run(run())


def test_codex_rate_limits_normalizes_app_server_shape():
    result = {
        "rateLimits": {
            "limitId": "codex",
            "primary": {"usedPercent": 40, "windowDurationMins": 300, "resetsAt": 111},
            "secondary": {"usedPercent": 50, "windowDurationMins": 10_080, "resetsAt": 222},
            "planType": "prolite",
        },
        "rateLimitsByLimitId": {
            "codex": {
                "limitId": "codex",
                "limitName": None,
                "primary": {"usedPercent": 29, "windowDurationMins": 300, "resetsAt": 333},
                "secondary": {"usedPercent": 33, "windowDurationMins": 10_080, "resetsAt": 444},
                "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
                "planType": "prolite",
                "rateLimitReachedType": None,
            },
        },
    }

    normalized = bot._normalize_codex_rate_limits_response(result)

    assert normalized == {
        "limit_id": "codex",
        "limit_name": None,
        "primary": {"used_percent": 29, "window_minutes": 300, "resets_at": 333},
        "secondary": {"used_percent": 33, "window_minutes": 10_080, "resets_at": 444},
        "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
        "plan_type": "prolite",
        "rate_limit_reached_type": None,
    }


def test_codex_rate_limits_rejects_empty_app_server_snapshot():
    result = {
        "rateLimitsByLimitId": {
            "codex": {
                "limitId": "codex",
                "planType": "prolite",
            },
        },
    }

    assert bot._normalize_codex_rate_limits_response(result) is None


def test_channel_worker_single_turn_clean_reset(monkeypatch, tmp_path):
    """Baseline: one user msg → one turn → in_flight returns to 0."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat()
        msg = FakeMessage(1, "hello")
        await worker.submit(
            bot.Payload(update=FakeUpdate(msg, chat), ctx=FakeCtx(), text="hello")
        )
        assert bot._in_flight == 1
        assert worker._turn_anchor == 1

        session.queue.put_nowait(Event(kind="text_delta", chunk="Hi"))
        await wait_for(lambda: len(msg.replies) == 1)

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(
                    content="done", session_id="sid-1", cost=0.1,
                ),
            )
        )
        await wait_for(lambda: bot._in_flight == 0)
        assert worker._turn_active is False
        assert bot._session_turns == 1

        await worker.stop()

    asyncio.run(run())


def test_channel_worker_cut_in_waits_for_next_turn(monkeypatch, tmp_path):
    """V 快速连发: 第二条先 ack + interrupt, 但不立即 submit 进 SDK.
    等第一条 turn_end 后, worker 才 begin_turn + submit 第二条, 避免 stale
    interrupt 命中新 turn."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat()
        first_msg = FakeMessage(1, "hello")
        second_msg = FakeMessage(2, "more")
        ctx = FakeCtx()

        await worker.submit(
            bot.Payload(update=FakeUpdate(first_msg, chat), ctx=ctx, text="hello")
        )
        await worker.submit(
            bot.Payload(update=FakeUpdate(second_msg, chat), ctx=ctx, text="more")
        )

        assert session.submitted == [("hello", None)]
        assert bot._in_flight == 1
        await wait_for(lambda: session.interrupted is True)
        # SDK turn anchor 仍是 msg1; cut-in 不改 bridge reply_to.
        assert worker._turn_anchor == 1
        assert bot.bridge.contexts[-1][2] == 1

        # 第一 turn 的 text/tool 继续落到 msg1, 不污染尚未开始的 msg2.
        session.queue.put_nowait(Event(kind="text_delta", chunk="Hi"))
        await wait_for(lambda: len(first_msg.replies) == 1)
        live_text = first_msg.replies[0]
        assert live_text.text == "Hi"
        assert len(second_msg.replies) == 0

        session.queue.put_nowait(
            Event(kind="tool_use", name="Read", input_dict={"file_path": "a.py"})
        )
        await wait_for(lambda: len(first_msg.replies) == 2)
        tool_status = first_msg.replies[1]
        assert "Read" in tool_status.text

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(
                    content="**done**",
                    session_id="sid-1",
                    cost=0.2,
                    model="claude-test[200k]",
                    context_window=200000,
                    input_tokens=5,
                    cache_creation_tokens=1,
                    cache_read_tokens=2,
                ),
            )
        )
        # 第一 turn_end 后立即启动第二条 queued payload.
        await wait_for(lambda: session.submitted == [("hello", None), ("more", None)])
        assert bot._in_flight == 1
        assert worker._turn_anchor == 2
        assert bot.bridge.contexts[-1][2] == 2
        # final response 编辑第一条的 live text.
        assert live_text.edits[-1][0] == "<b>done</b>"
        assert tool_status.deleted is True
        assert bot._session_turns == 1
        assert bot._last_used_tokens == 8

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(
                    content="second done",
                    session_id="sid-2",
                    cost=0.1,
                ),
            )
        )
        await wait_for(lambda: bot._in_flight == 0)
        assert any("second done" in r.text for r in second_msg.replies)
        assert bot._session_turns == 2
        assert worker._turn_active is False

        await worker.stop()
        assert session.closed

    asyncio.run(run())


def test_channel_worker_codex_coalesces_pending_cut_ins(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        processed: list[int] = []

        async def fake_mark_processed(update_id):
            if update_id is not None:
                processed.append(update_id)

        monkeypatch.setattr(bot, "_mark_processed", fake_mark_processed)
        session = FakeSession()
        session.supports_hot_input = False
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        ctx = FakeCtx()
        m1 = FakeMessage(1, "first")
        m2 = FakeMessage(2, "second")
        m3 = FakeMessage(3, "third")

        await worker.submit(
            bot.Payload(
                update=FakeUpdate(m1, chat),
                ctx=ctx,
                text="first",
                update_id=101,
            )
        )
        await worker.submit(
            bot.Payload(
                update=FakeUpdate(m2, chat),
                ctx=ctx,
                text="second",
                update_id=102,
            )
        )
        await worker.submit(
            bot.Payload(
                update=FakeUpdate(m3, chat),
                ctx=ctx,
                text="third",
                update_id=103,
            )
        )

        assert session.submitted == [("first", None)]

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="done1", session_id="sid-1", cost=0.01),
            )
        )
        await wait_for(lambda: len(session.submitted) == 2)
        prompt, images = session.submitted[1]
        assert images is None
        assert "follow-up Telegram messages" in prompt
        assert "<user_message n=1 update_id=102 message_id=2>" in prompt
        assert "second" in prompt
        assert "<user_message n=2 update_id=103 message_id=3>" in prompt
        assert "third" in prompt
        assert processed == [101]

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="batch done", session_id="sid-2", cost=0.01),
            )
        )
        await wait_for(lambda: processed == [101, 102, 103])
        await wait_for(lambda: (42, 2, "👌") in ctx.bot.reactions)
        await wait_for(lambda: (42, 3, "👌") in ctx.bot.reactions)
        assert len(m2.replies) == 0
        assert any("batch done" in r.text for r in m3.replies)
        await wait_for(lambda: bot._in_flight == 0)

        await worker.stop()

    asyncio.run(run())

def test_channel_worker_reaction_eye_then_ok_single_turn(monkeypatch, tmp_path):
    """单条消息: submit → 👀 立即 fire (因为 _begin_turn inline); turn_end → 👌."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        msg = FakeMessage(1, "hello")
        ctx = FakeCtx()
        await worker.submit(
            bot.Payload(update=FakeUpdate(msg, chat), ctx=ctx, text="hello")
        )
        await wait_for(lambda: (42, 1, "👀") in ctx.bot.reactions)
        # turn_end 之前不该出现 👌
        assert (42, 1, "👌") not in ctx.bot.reactions

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="hi", session_id="sid-1", cost=0.01),
            )
        )
        await wait_for(lambda: (42, 1, "👌") in ctx.bot.reactions)
        assert ctx.bot.reactions == [(42, 1, "👀"), (42, 1, "👌")]

        await worker.stop()

    asyncio.run(run())


def test_channel_worker_reaction_back_to_back_messages(monkeypatch, tmp_path):
    """V 连发两条: 两条都立即 👀; 每条在各自 turn_end 后 👌."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        m1 = FakeMessage(1, "first")
        m2 = FakeMessage(2, "second")
        ctx = FakeCtx()  # 共享同一个 bot 让 reactions 集中收集

        await worker.submit(
            bot.Payload(update=FakeUpdate(m1, chat), ctx=ctx, text="first")
        )
        await wait_for(lambda: (42, 1, "👀") in ctx.bot.reactions)

        await worker.submit(
            bot.Payload(update=FakeUpdate(m2, chat), ctx=ctx, text="second")
        )
        await wait_for(lambda: (42, 2, "👀") in ctx.bot.reactions)
        assert (42, 2, "👌") not in ctx.bot.reactions

        # 第一 turn_end → 只 finalize m1, 并启动 m2 的 queued turn.
        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="ok1", session_id="sid-1", cost=0.01),
            )
        )
        await wait_for(lambda: (42, 1, "👌") in ctx.bot.reactions)
        assert (42, 2, "👌") not in ctx.bot.reactions
        assert bot._in_flight == 1

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="ok2", session_id="sid-2", cost=0.01),
            )
        )
        await wait_for(lambda: (42, 2, "👌") in ctx.bot.reactions)
        await wait_for(lambda: bot._in_flight == 0)

        await worker.stop()

    asyncio.run(run())


def test_channel_worker_stream_error_replays_active_then_pending(monkeypatch, tmp_path):
    """Recoverable CPU stream error must replay N before continuing N+1.

    The old behavior marked active+pending as processed and 💔, permanently
    dropping the queue. The required channel contract is FIFO replay after the
    supervisor reconnects.
    """
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        processed: list[int] = []

        async def fake_mark_processed(update_id):
            if update_id is not None:
                processed.append(update_id)

        monkeypatch.setattr(bot, "_mark_processed", fake_mark_processed)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        m1 = FakeMessage(1, "first")
        m2 = FakeMessage(2, "second")
        ctx = FakeCtx()

        await worker.submit(
            bot.Payload(
                update=FakeUpdate(m1, chat),
                ctx=ctx,
                text="first",
                update_id=101,
            )
        )
        await worker.submit(
            bot.Payload(
                update=FakeUpdate(m2, chat),
                ctx=ctx,
                text="second",
                update_id=102,
            )
        )
        assert session.submitted == [("first", None)]

        session.queue.put_nowait(Event(kind="error", exception=RuntimeError("boom")))

        # After reconnect, m1 is replayed first; m2 stays queued.
        await wait_for(lambda: session.submitted == [("first", None), ("first", None)])
        assert processed == []
        assert (42, 1, "💔") not in ctx.bot.reactions
        assert (42, 2, "💔") not in ctx.bot.reactions
        assert bot._in_flight == 1
        assert worker._turn_anchor == 1

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="ok1", session_id="sid-1", cost=0.01),
            )
        )
        await wait_for(
            lambda: session.submitted == [
                ("first", None),
                ("first", None),
                ("second", None),
            ]
        )
        await wait_for(lambda: 101 in processed)
        assert (42, 1, "👌") in ctx.bot.reactions
        assert worker._turn_anchor == 2

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="ok2", session_id="sid-2", cost=0.01),
            )
        )
        await wait_for(lambda: 102 in processed)
        await wait_for(lambda: bot._in_flight == 0)
        assert (42, 2, "👌") in ctx.bot.reactions
        assert (42, 2, "💔") not in ctx.bot.reactions

        await worker.stop()

    asyncio.run(run())


def test_tg_pending_update_replays_after_restart_and_acks(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)

        session1 = FakeSession()
        worker1 = bot.ChannelWorker(session1, instance_label="test")
        monkeypatch.setattr(bot, "_channel_worker", worker1)
        await worker1.start()

        ctx = FakeCtx()
        chat = FakeChat(chat_id=42)
        msg = FakeMessage(7, "needs replay")
        update = FakeUpdate(msg, chat, user_id=7, update_id=501)

        await bot._process(update, ctx, "needs replay")
        assert session1.submitted == [("needs replay", None)]
        pending = json.loads((tmp_path / "pending.json").read_text())["pending"]
        assert pending["501"]["text"] == "needs replay"

        # Simulate process death before turn_end: no processed mark was written.
        await worker1.stop()

        session2 = FakeSession()
        worker2 = bot.ChannelWorker(session2, instance_label="test")
        monkeypatch.setattr(bot, "_channel_worker", worker2)
        await worker2.start()
        app = type("FakeApp", (), {"bot": ctx.bot})()

        await bot._replay_pending_updates(app)
        assert session2.submitted == [("needs replay", None)]

        session2.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="done", session_id="sid-1", cost=0.01),
            )
        )
        await wait_for(lambda: "501" not in bot._pending_update_records)
        assert 501 in bot._processed_set
        assert json.loads((tmp_path / "pending.json").read_text())["pending"] == {}
        assert ctx.bot.sent_messages[-1]["reply_to_message_id"] == 7

        await worker2.stop()

    asyncio.run(run())


def test_tg_replay_pending_fifo_without_interrupt(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        ctx = FakeCtx()
        bot._pending_update_records = {
            "501": {
                "update_id": 501,
                "chat_id": 42,
                "message_id": 7,
                "text": "first",
                "images": [],
                "received_at": 1.0,
            },
            "502": {
                "update_id": 502,
                "chat_id": 42,
                "message_id": 8,
                "text": "second",
                "images": [],
                "received_at": 2.0,
            },
        }

        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        monkeypatch.setattr(bot, "_channel_worker", worker)
        await worker.start()

        app = type("FakeApp", (), {"bot": ctx.bot})()
        await bot._replay_pending_updates(app)

        assert session.submitted == [("first", None)]
        assert session.interrupted is False
        assert [p.text for p in worker._pending_payloads] == ["second"]

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(content="done1", session_id="sid-1", cost=0.01),
            )
        )
        await wait_for(lambda: session.submitted == [("first", None), ("second", None)])

        await worker.stop()

    asyncio.run(run())


def test_channel_worker_new_message_reply_does_not_merge(monkeypatch, tmp_path):
    """V 发 msg1 流式中又发 msg2: msg1 的后续输出仍在 msg1, msg2
    等第一 turn_end 后开启自己的 reply."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        m1 = FakeMessage(1, "first")
        m2 = FakeMessage(2, "second")
        ctx = FakeCtx()

        await worker.submit(
            bot.Payload(update=FakeUpdate(m1, chat), ctx=ctx, text="first")
        )
        # 第一条流式输出: 应 reply 到 m1
        session.queue.put_nowait(Event(kind="text_delta", chunk="answer-1-part-A"))
        await wait_for(lambda: len(m1.replies) == 1)
        first_reply = m1.replies[0]
        assert "answer-1-part-A" in first_reply.text

        # V 中途发 msg2 (turn 1 还没 turn_end)
        await worker.submit(
            bot.Payload(update=FakeUpdate(m2, chat), ctx=ctx, text="second")
        )

        # 后续 text_delta 仍属于第一 turn. 因 edit throttle 不保证立即刷出,
        # 但绝不能落到尚未 begin_turn 的 msg2.
        session.queue.put_nowait(Event(kind="text_delta", chunk="answer-2"))
        await asyncio.sleep(0.05)
        assert len(m2.replies) == 0
        assert len(m1.replies) == 1

        # 第一 turn 结束后, queued msg2 才 begin_turn; 新流式输出落到 msg2.
        session.queue.put_nowait(
            Event(kind="turn_end", response=Response(content="", session_id="sid-1", cost=0.01))
        )
        await wait_for(lambda: session.submitted == [("first", None), ("second", None)])
        session.queue.put_nowait(Event(kind="text_delta", chunk="answer-msg2"))
        await wait_for(lambda: len(m2.replies) == 1)
        assert m2.replies[0].text == "answer-msg2"

        # 关掉 worker (turn_end 不发, 让 stop 自己 drain)
        await worker.stop()

    asyncio.run(run())


def test_channel_worker_final_response_lands_on_active_reply_anchor(monkeypatch, tmp_path):
    """Cut-in 模式: msg1 final 留在 msg1 anchor; msg2 final 等自己的 turn_end."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        m1 = FakeMessage(1, "first")
        m2 = FakeMessage(2, "second")
        ctx = FakeCtx()

        await worker.submit(
            bot.Payload(update=FakeUpdate(m1, chat), ctx=ctx, text="first")
        )
        # 第一条流式输出: reply 到 m1
        session.queue.put_nowait(Event(kind="text_delta", chunk="streaming-msg1"))
        await wait_for(lambda: len(m1.replies) == 1)

        # V 中途发 msg2 — 只 queue + interrupt, 不切当前 turn anchor.
        await worker.submit(
            bot.Payload(update=FakeUpdate(m2, chat), ctx=ctx, text="second")
        )

        # SDK turn_end 来 — final response 应落 msg1, 然后启动 msg2.
        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(
                    content="final answer", session_id="sid-1", cost=0.05,
                ),
            )
        )
        await wait_for(lambda: "final answer" in m1.replies[0].text)
        assert len(m2.replies) == 0
        await wait_for(lambda: session.submitted == [("first", None), ("second", None)])

        session.queue.put_nowait(
            Event(
                kind="turn_end",
                response=Response(
                    content="second final", session_id="sid-2", cost=0.05,
                ),
            )
        )
        await wait_for(lambda: any("second final" in r.text for r in m2.replies))

        await worker.stop()

    asyncio.run(run())


def test_channel_worker_reset_drops_pending_marks(monkeypatch, tmp_path):
    """P2-D: V 发 m1 → submit (pending=[m1]) → /new → reset_turn_state 应清
    pending_marks. 接着 V 发 m2 → 只有 m2 进 pending, 不会带着 m1 一起 fire 👀."""
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        chat = FakeChat(chat_id=42)
        ctx = FakeCtx()

        m1 = FakeMessage(1, "first")
        await worker.submit(
            bot.Payload(update=FakeUpdate(m1, chat), ctx=ctx, text="first")
        )
        await wait_for(lambda: (42, 1, "👀") in ctx.bot.reactions)

        # V /new — 走 _handle_reset 路径
        new_msg = FakeMessage(99, "/new")
        await worker.submit(
            bot.Payload(update=FakeUpdate(new_msg, chat), ctx=ctx, text="/new")
        )
        # /new 后 _pending_marks 应被清空 (drop_pending=True 路径)
        assert worker._pending_marks == []
        assert worker._active_marks == []
        # m1 在 /new 时被 fire 💔 (区分 turn_end 的 👌, V 一眼看到未完成).
        await wait_for(lambda: (42, 1, "💔") in ctx.bot.reactions)

        # V 接着发 m2 — 只有 m2 进 pending → 👀 给 m2
        m2 = FakeMessage(2, "after-reset")
        await worker.submit(
            bot.Payload(update=FakeUpdate(m2, chat), ctx=ctx, text="after-reset")
        )
        await wait_for(lambda: (42, 2, "👀") in ctx.bot.reactions)

        # 关键断言: m1 的 (chat=42, msg=1) 不应该再次 fire 👀 (它在 reset 里被丢了)
        # 第一次 m1 submit 时已 fire 过一次 👀, 但 /new 之后不应再有第二次
        eye_for_m1 = [r for r in ctx.bot.reactions if r == (42, 1, "👀")]
        assert len(eye_for_m1) == 1, (
            f"m1 should have fired 👀 exactly once, got {eye_for_m1}, "
            f"all reactions: {ctx.bot.reactions}"
        )

        await worker.stop()

    asyncio.run(run())


def test_channel_worker_reset_shortcut(monkeypatch, tmp_path):
    async def run():
        reset_bot_globals(monkeypatch, tmp_path)
        session = FakeSession()
        worker = bot.ChannelWorker(session, instance_label="test")
        await worker.start()

        msg = FakeMessage(1, "/new")
        await worker.submit(
            bot.Payload(update=FakeUpdate(msg, FakeChat()), ctx=FakeCtx(), text="/new")
        )
        assert session.reset is True
        assert msg.replies[0].text == "会话已重置。"
        assert bot._in_flight == 0

        await worker.stop()

    asyncio.run(run())
