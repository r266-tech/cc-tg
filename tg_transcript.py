"""Append-only Telegram transcript logging for post-incident debugging."""

from __future__ import annotations

import contextlib
import contextvars
import inspect
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from constants import INSTANCE, PROJECT, STATE_DIR

log = logging.getLogger(__name__)

_INSTANCE_NAME = INSTANCE or "main"
TRANSCRIPT_FILE = Path(
    os.environ.get(
        "BABATA_TG_TRANSCRIPT_FILE",
        str(STATE_DIR / f"tg-transcript-{_INSTANCE_NAME}.jsonl"),
    )
)
_MAX_TEXT = int(os.environ.get("BABATA_TG_TRANSCRIPT_MAX_TEXT", "20000"))
_SOURCE = contextvars.ContextVar("tg_transcript_source", default="bot")

_BOT_METHODS = (
    "send_message",
    "edit_message_text",
    "delete_message",
    "send_chat_action",
    "set_message_reaction",
    "answer_callback_query",
    "edit_message_reply_markup",
    "send_document",
    "send_media_group",
    "send_location",
    "send_video",
    "send_voice",
    "send_photo",
    "send_audio",
    "send_animation",
    "send_video_note",
    "set_my_commands",
)
_PATCHED: set[tuple[type, str]] = set()


@contextlib.contextmanager
def transcript_source(source: str):
    token = _SOURCE.set(source)
    try:
        yield
    finally:
        _SOURCE.reset(token)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(text: Any) -> Any:
    if not isinstance(text, str):
        return text
    if len(text) <= _MAX_TEXT:
        return text
    return f"{text[:_MAX_TEXT]}... [truncated {len(text) - _MAX_TEXT} chars]"


def _json_safe(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return repr(value)
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, str):
        return _truncate(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {
            str(_truncate(k)): _json_safe(v, depth + 1)
            for k, v in value.items()
            if not str(k).startswith("_")
        }
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v, depth + 1) for v in value]
    return repr(value)


def _write(event: dict[str, Any]) -> None:
    event.setdefault("ts", _now())
    event.setdefault("project", PROJECT)
    event.setdefault("instance", _INSTANCE_NAME)
    try:
        TRANSCRIPT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with TRANSCRIPT_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(_json_safe(event), ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        log.warning("tg transcript write failed: %s", e)


def _user_summary(user: Any) -> dict[str, Any] | None:
    if not user:
        return None
    return {
        "id": getattr(user, "id", None),
        "is_bot": getattr(user, "is_bot", None),
        "username": getattr(user, "username", None),
        "first_name": getattr(user, "first_name", None),
        "last_name": getattr(user, "last_name", None),
    }


def _chat_summary(chat: Any) -> dict[str, Any] | None:
    if not chat:
        return None
    return {
        "id": getattr(chat, "id", None),
        "type": getattr(chat, "type", None),
        "title": getattr(chat, "title", None),
        "username": getattr(chat, "username", None),
    }


def _file_ref(obj: Any, kind: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "file_id": getattr(obj, "file_id", None),
        "file_unique_id": getattr(obj, "file_unique_id", None),
        "file_name": getattr(obj, "file_name", None),
        "mime_type": getattr(obj, "mime_type", None),
        "file_size": getattr(obj, "file_size", None),
        "duration": getattr(obj, "duration", None),
        "width": getattr(obj, "width", None),
        "height": getattr(obj, "height", None),
    }


def _media_summary(msg: Any) -> list[dict[str, Any]]:
    media: list[dict[str, Any]] = []
    photos = getattr(msg, "photo", None) or []
    if photos:
        media.append(_file_ref(photos[-1], "photo") | {"photo_count": len(photos)})
    for attr in ("document", "voice", "audio", "video", "video_note", "animation", "sticker"):
        item = getattr(msg, attr, None)
        if item:
            media.append(_file_ref(item, attr))
    return media


def _message_ref(msg: Any) -> dict[str, Any]:
    return {
        "message_id": getattr(msg, "message_id", None),
        "text": _truncate(getattr(msg, "text", None)),
        "caption": _truncate(getattr(msg, "caption", None)),
        "media": _media_summary(msg),
    }


def _message_summary(msg: Any) -> dict[str, Any] | None:
    if not msg:
        return None
    summary = _message_ref(msg)
    summary.update(
        {
            "chat": _chat_summary(getattr(msg, "chat", None)),
            "from_user": _user_summary(getattr(msg, "from_user", None)),
            "date": getattr(getattr(msg, "date", None), "isoformat", lambda: None)(),
            "media_group_id": getattr(msg, "media_group_id", None),
        }
    )
    reply = getattr(msg, "reply_to_message", None)
    if reply:
        summary["reply_to"] = _message_ref(reply)
    return summary


def record_update(update: Any, source: str) -> None:
    """Record a Telegram update before handler code mutates or drops it."""
    try:
        event: dict[str, Any] = {
            "direction": "in",
            "source": source,
            "update_id": getattr(update, "update_id", None),
            "chat": _chat_summary(getattr(update, "effective_chat", None)),
            "user": _user_summary(getattr(update, "effective_user", None)),
            "message": _message_summary(getattr(update, "effective_message", None)),
        }
        query = getattr(update, "callback_query", None)
        if query:
            event["callback_query"] = {
                "id": getattr(query, "id", None),
                "data": getattr(query, "data", None),
                "from_user": _user_summary(getattr(query, "from_user", None)),
                "message": _message_summary(getattr(query, "message", None)),
            }
    except Exception as e:
        log.warning("tg transcript inbound summary failed: %s", e)
        update_id = None
        with contextlib.suppress(Exception):
            update_id = getattr(update, "update_id", None)
        event = {
            "direction": "in",
            "source": source,
            "update_id": update_id,
            "summary_error": {"type": type(e).__name__, "message": str(e)},
        }
    _write(event)


def _arg(args: tuple[Any, ...], idx: int) -> Any:
    return args[idx] if len(args) > idx else None


def _media_item_summary(item: Any) -> dict[str, Any]:
    return {
        "type": type(item).__name__,
        "caption": _truncate(getattr(item, "caption", None)),
        "media": repr(getattr(item, "media", None)),
    }


def _request_summary(method: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    req: dict[str, Any] = {
        "chat_id": kwargs.get("chat_id"),
        "message_id": kwargs.get("message_id"),
        "reply_to_message_id": kwargs.get("reply_to_message_id"),
        "parse_mode": kwargs.get("parse_mode"),
    }
    if method == "send_message":
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["text"] = kwargs.get("text", _arg(args, 1))
    elif method == "edit_message_text":
        req["text"] = kwargs.get("text", _arg(args, 0))
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 1))
        req["message_id"] = kwargs.get("message_id", _arg(args, 2))
        req["inline_message_id"] = kwargs.get("inline_message_id")
    elif method == "delete_message":
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["message_id"] = kwargs.get("message_id", _arg(args, 1))
    elif method == "send_chat_action":
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["action"] = kwargs.get("action", _arg(args, 1))
    elif method == "set_message_reaction":
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["message_id"] = kwargs.get("message_id", _arg(args, 1))
        req["reaction"] = kwargs.get("reaction", _arg(args, 2))
    elif method in {"send_document", "send_video", "send_voice", "send_photo", "send_audio", "send_animation", "send_video_note"}:
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["caption"] = kwargs.get("caption")
        req["filename"] = kwargs.get("filename")
        for key in ("document", "video", "voice", "photo", "audio", "animation", "video_note"):
            if key in kwargs:
                req[key] = repr(kwargs[key])
    elif method == "send_media_group":
        media = kwargs.get("media", _arg(args, 1)) or []
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["media_count"] = len(media)
        req["media"] = [_media_item_summary(item) for item in media[:10]]
    elif method == "send_location":
        req["chat_id"] = kwargs.get("chat_id", _arg(args, 0))
        req["latitude"] = kwargs.get("latitude", _arg(args, 1))
        req["longitude"] = kwargs.get("longitude", _arg(args, 2))
    elif method == "answer_callback_query":
        req["callback_query_id"] = kwargs.get("callback_query_id", _arg(args, 0))
        req["text"] = kwargs.get("text")
    elif method == "set_my_commands":
        commands = kwargs.get("commands", _arg(args, 0)) or []
        req["commands"] = [repr(cmd) for cmd in commands]
    for key in ("caption", "reply_markup", "action"):
        if key in kwargs and key not in req:
            req[key] = kwargs[key]
    return {k: v for k, v in req.items() if v is not None}


def _result_summary(result: Any) -> Any:
    if isinstance(result, (list, tuple)):
        return [_result_summary(item) for item in result]
    msg = _message_summary(result)
    if msg and msg.get("message_id") is not None:
        return msg
    if isinstance(result, bool | int | float | str) or result is None:
        return result
    return repr(result)


def install_bot_transcript(bot: Any) -> None:
    """Wrap PTB bot methods once so all visible outbound TG calls are recorded."""
    cls = type(bot)
    for method in _BOT_METHODS:
        key = (cls, method)
        if key in _PATCHED:
            continue
        original = getattr(cls, method, None)
        if not callable(original):
            continue

        async def wrapped(self, *args, __method=method, __original=original, **kwargs):
            event_id = uuid.uuid4().hex
            event = {
                "direction": "out",
                "event_id": event_id,
                "source": _SOURCE.get(),
                "method": __method,
            }
            try:
                event["request"] = _request_summary(__method, args, kwargs)
            except Exception as e:
                log.warning("tg transcript outbound summary failed: %s", e)
                event["summary_error"] = {"type": type(e).__name__, "message": str(e)}
            _write(event | {"phase": "attempt"})
            try:
                result = __original(self, *args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
            except Exception as e:
                _write(event | {"phase": "error", "error": {"type": type(e).__name__, "message": str(e)}})
                raise
            _write(event | {"phase": "result", "result": _result_summary(result)})
            return result

        setattr(cls, method, wrapped)
        _PATCHED.add(key)
