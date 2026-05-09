import asyncio
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


class FakeUpdate:
    def __init__(self, message: FakeMessage, chat: FakeChat):
        self.effective_message = message
        self.message = message
        self.effective_chat = chat
        self.effective_user = None


class FakeBot:
    """PTB Bot stub. Records set_message_reaction calls so tests can assert
    👀 fired at turn-begin and 👌 at turn-end."""

    def __init__(self):
        self.reactions: list[tuple[int, int, str]] = []

    async def set_message_reaction(self, *, chat_id, message_id, reaction):
        self.reactions.append((chat_id, message_id, reaction))


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
    bot._state = {}
    bot._verbose = 1
    bot._in_flight = 0
    bot._session_cost = 0.0
    bot._session_turns = 0
    bot._last_model = None
    bot._last_context_window = None
    bot._last_used_tokens = 0
    bot._last_cost = 0.0


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
