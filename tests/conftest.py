from __future__ import annotations

import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _disable_codex_memory_inject_by_default(monkeypatch):
    monkeypatch.setenv("BABATA_CODEX_MEMORY_INJECT", "0")
    monkeypatch.setenv("BABATA_CC_MEMORY_INJECT", "0")


@dataclass
class ClaudeAgentOptions:
    tools: list[str] | None = None
    allowed_tools: list[str] = field(default_factory=list)
    system_prompt: str | None = None
    mcp_servers: dict[str, Any] = field(default_factory=dict)
    permission_mode: str | None = None
    resume: str | None = None
    max_turns: int | None = None
    cwd: str | Path | None = None
    cli_path: str | Path | None = None
    include_partial_messages: bool = False
    setting_sources: list[str] | None = None
    max_buffer_size: int | None = None
    can_use_tool: Any = None


class ClaudeSDKClient:
    def __init__(self, options=None):
        self.options = options


@dataclass
class TextBlock:
    text: str


@dataclass
class ToolUseBlock:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class ToolResultBlock:
    tool_use_id: str
    content: str | list[dict[str, Any]] | None = None
    is_error: bool | None = None


@dataclass
class UserMessage:
    content: str | list[Any]
    uuid: str | None = None
    parent_tool_use_id: str | None = None
    tool_use_result: dict[str, Any] | None = None


@dataclass
class AssistantMessage:
    content: list[Any]
    model: str
    parent_tool_use_id: str | None = None
    error: str | None = None
    usage: dict[str, Any] | None = None
    message_id: str | None = None
    stop_reason: str | None = None
    session_id: str | None = None
    uuid: str | None = None


@dataclass
class ResultMessage:
    subtype: str
    duration_ms: int
    duration_api_ms: int
    is_error: bool
    num_turns: int
    session_id: str
    stop_reason: str | None = None
    total_cost_usd: float | None = None
    usage: dict[str, Any] | None = None
    result: str | None = None
    structured_output: Any = None
    model_usage: dict[str, Any] | None = None
    permission_denials: list[Any] | None = None
    errors: list[str] | None = None
    uuid: str | None = None


@dataclass
class StreamEvent:
    uuid: str
    session_id: str
    event: dict[str, Any]
    parent_tool_use_id: str | None = None


@dataclass
class PermissionResultAllow:
    behavior: str = "allow"
    updated_input: dict[str, Any] | None = None
    updated_permissions: list[Any] | None = None


@dataclass
class ToolPermissionContext:
    signal: Any | None = None
    suggestions: list[Any] = field(default_factory=list)
    tool_use_id: str | None = None
    agent_id: str | None = None


sdk = types.ModuleType("claude_agent_sdk")
for name, obj in {
    "AssistantMessage": AssistantMessage,
    "ClaudeAgentOptions": ClaudeAgentOptions,
    "ClaudeSDKClient": ClaudeSDKClient,
    "ResultMessage": ResultMessage,
    "StreamEvent": StreamEvent,
    "TextBlock": TextBlock,
    "ToolResultBlock": ToolResultBlock,
    "ToolUseBlock": ToolUseBlock,
    "UserMessage": UserMessage,
}.items():
    setattr(sdk, name, obj)
sys.modules["claude_agent_sdk"] = sdk

sdk_types = types.ModuleType("claude_agent_sdk.types")
sdk_types.PermissionResultAllow = PermissionResultAllow
sdk_types.StreamEvent = StreamEvent
sdk_types.ToolPermissionContext = ToolPermissionContext
sys.modules["claude_agent_sdk.types"] = sdk_types


class _DummyFilter:
    def __and__(self, other):
        return self

    def __or__(self, other):
        return self

    def __invert__(self):
        return self


class _Filters:
    TEXT = _DummyFilter()
    COMMAND = _DummyFilter()
    VOICE = _DummyFilter()
    AUDIO = _DummyFilter()
    PHOTO = _DummyFilter()
    VIDEO = _DummyFilter()
    VIDEO_NOTE = _DummyFilter()

    class Document:
        ALL = _DummyFilter()


class _Handler:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class _Application:
    @classmethod
    def builder(cls):
        return cls()

    def token(self, *_):
        return self

    def concurrent_updates(self, *_):
        return self

    def post_init(self, *_):
        return self

    def build(self):
        return self

    def add_handler(self, *_):
        return None

    def run_polling(self, *_args, **_kwargs):
        return None


telegram = types.ModuleType("telegram")
telegram.Update = type("Update", (), {})
telegram.InlineKeyboardButton = lambda *args, **kwargs: (args, kwargs)
telegram.InlineKeyboardMarkup = lambda *args, **kwargs: (args, kwargs)
sys.modules["telegram"] = telegram

telegram_ext = types.ModuleType("telegram.ext")
telegram_ext.Application = _Application
telegram_ext.CallbackQueryHandler = _Handler
telegram_ext.CommandHandler = _Handler
telegram_ext.ContextTypes = type("ContextTypes", (), {"DEFAULT_TYPE": object})
telegram_ext.MessageHandler = _Handler
telegram_ext.filters = _Filters()
sys.modules["telegram.ext"] = telegram_ext
