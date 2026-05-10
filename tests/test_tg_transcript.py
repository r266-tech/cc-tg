import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SDK_SITE = next(iter((_REPO / ".venv/lib").glob("python*/site-packages")), None)
if _SDK_SITE:
    sys.path.insert(0, str(_SDK_SITE))
sys.path.insert(0, str(_REPO))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123:test")
os.environ.setdefault("ALLOWED_USER_ID", "0")

import tg_transcript


class FakeChat:
    def __init__(self, chat_id: int = 7):
        self.id = chat_id
        self.type = "private"
        self.title = None
        self.username = "chatuser"


class FakeUser:
    def __init__(self, user_id: int = 42):
        self.id = user_id
        self.is_bot = False
        self.username = "alice"
        self.first_name = "Alice"
        self.last_name = None


class FakeFile:
    def __init__(self, file_id: str):
        self.file_id = file_id
        self.file_unique_id = f"{file_id}-u"
        self.file_size = 123
        self.width = 640
        self.height = 480


class FakeMessage:
    def __init__(self, message_id: int, text: str | None = None):
        self.message_id = message_id
        self.text = text
        self.caption = None
        self.chat = FakeChat()
        self.from_user = FakeUser()
        self.date = datetime(2026, 5, 10, tzinfo=timezone.utc)
        self.media_group_id = None
        self.reply_to_message = None
        self.photo = []
        self.document = None
        self.voice = None
        self.audio = None
        self.video = None
        self.video_note = None
        self.animation = None
        self.sticker = None


class FakeUpdate:
    def __init__(self, message: FakeMessage):
        self.update_id = 99
        self.effective_chat = message.chat
        self.effective_user = message.from_user
        self.effective_message = message
        self.callback_query = None


def _read_one(path: Path) -> dict:
    return json.loads(path.read_text().splitlines()[0])


def _read_all(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_record_update_keeps_text_reply_and_media(tmp_path, monkeypatch):
    transcript = tmp_path / "tg.jsonl"
    monkeypatch.setattr(tg_transcript, "TRANSCRIPT_FILE", transcript)
    msg = FakeMessage(10, "hello")
    msg.reply_to_message = FakeMessage(9, "older")
    msg.photo = [FakeFile("small"), FakeFile("large")]

    tg_transcript.record_update(FakeUpdate(msg), "text")

    event = _read_one(transcript)
    assert event["direction"] == "in"
    assert event["source"] == "text"
    assert event["update_id"] == 99
    assert event["message"]["text"] == "hello"
    assert event["message"]["reply_to"]["text"] == "older"
    assert event["message"]["media"][0]["kind"] == "photo"
    assert event["message"]["media"][0]["file_id"] == "large"
    assert event["message"]["media"][0]["photo_count"] == 2


def test_record_update_summary_failure_does_not_raise(tmp_path, monkeypatch):
    transcript = tmp_path / "tg.jsonl"
    monkeypatch.setattr(tg_transcript, "TRANSCRIPT_FILE", transcript)

    class BadUpdate:
        update_id = 100

        @property
        def effective_chat(self):
            raise RuntimeError("bad chat")

    tg_transcript.record_update(BadUpdate(), "text")

    event = _read_one(transcript)
    assert event["direction"] == "in"
    assert event["source"] == "text"
    assert event["update_id"] == 100
    assert event["summary_error"]["type"] == "RuntimeError"


def test_install_bot_transcript_logs_outbound_even_for_slot_bot(tmp_path, monkeypatch):
    transcript = tmp_path / "tg.jsonl"
    monkeypatch.setattr(tg_transcript, "TRANSCRIPT_FILE", transcript)

    class SlotBot:
        __slots__ = ()

        async def send_message(self, chat_id, text, **kwargs):
            msg = FakeMessage(44, text)
            msg.chat = FakeChat(chat_id)
            return msg

    async def run():
        bot = SlotBot()
        tg_transcript.install_bot_transcript(bot)
        with tg_transcript.transcript_source("cmd_status"):
            sent = await bot.send_message(chat_id=123, text="ok", reply_to_message_id=5)
        assert sent.message_id == 44

    asyncio.run(run())

    attempt, result = _read_all(transcript)
    assert attempt["direction"] == "out"
    assert attempt["phase"] == "attempt"
    assert attempt["source"] == "cmd_status"
    assert attempt["method"] == "send_message"
    assert attempt["request"]["chat_id"] == 123
    assert attempt["request"]["text"] == "ok"
    assert attempt["request"]["reply_to_message_id"] == 5
    assert "result" not in attempt
    assert result["phase"] == "result"
    assert result["event_id"] == attempt["event_id"]
    assert result["result"]["message_id"] == 44


def test_install_bot_transcript_summary_failure_still_sends(tmp_path, monkeypatch):
    transcript = tmp_path / "tg.jsonl"
    monkeypatch.setattr(tg_transcript, "TRANSCRIPT_FILE", transcript)

    class SlotBot:
        __slots__ = ()

        async def send_message(self, chat_id, text, **kwargs):
            msg = FakeMessage(45, text)
            msg.chat = FakeChat(chat_id)
            return msg

    async def run():
        bot = SlotBot()
        tg_transcript.install_bot_transcript(bot)
        monkeypatch.setattr(tg_transcript, "_request_summary", lambda *_: (_ for _ in ()).throw(RuntimeError("bad request")))
        sent = await bot.send_message(chat_id=123, text="ok")
        assert sent.message_id == 45

    asyncio.run(run())

    attempt, result = _read_all(transcript)
    assert attempt["phase"] == "attempt"
    assert attempt["summary_error"]["type"] == "RuntimeError"
    assert result["phase"] == "result"
    assert result["result"]["message_id"] == 45


def test_install_bot_transcript_logs_outbound_error_after_attempt(tmp_path, monkeypatch):
    transcript = tmp_path / "tg.jsonl"
    monkeypatch.setattr(tg_transcript, "TRANSCRIPT_FILE", transcript)

    class FailingBot:
        __slots__ = ()

        async def send_message(self, chat_id, text, **kwargs):
            raise RuntimeError("telegram down")

    async def run():
        bot = FailingBot()
        tg_transcript.install_bot_transcript(bot)
        try:
            await bot.send_message(chat_id=123, text="ok")
        except RuntimeError:
            pass
        else:
            raise AssertionError("send_message should fail")

    asyncio.run(run())

    attempt, error = _read_all(transcript)
    assert attempt["phase"] == "attempt"
    assert attempt["request"]["text"] == "ok"
    assert error["phase"] == "error"
    assert error["event_id"] == attempt["event_id"]
    assert error["error"]["type"] == "RuntimeError"
