"""Standalone stdio MCP server exposing TG capabilities to CC.

Run as: python tg_mcp.py
CC CLI connects via stdio; we relay requests to the bot through Unix socket.
"""

import asyncio
import errno
import json
import logging
import os
import sys

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from constants import BRIDGE_SOCKET, INSTANCE_LABELS, PROJECT

log = logging.getLogger(__name__)

# Per-instance socket path. bot.py passes BABATA_BRIDGE_SOCKET via mcp_servers
# env dict when spawning this subprocess; constants.py reads that env and
# falls back to the derived namespace-based default for standalone MCP runs.
SOCKET_PATH = BRIDGE_SOCKET
_BRIDGE_CONNECT_RETRY_SECONDS = float(os.environ.get("TG_BRIDGE_CONNECT_RETRY_SECONDS", "90"))
_BRIDGE_CONNECT_RETRY_INTERVAL = float(os.environ.get("TG_BRIDGE_CONNECT_RETRY_INTERVAL", "1"))
_RETRYABLE_CONNECT_ERRNOS = {
    errno.ENOENT,
    errno.ECONNREFUSED,
    errno.ECONNRESET,
}

# TG bot instances exposed via optional `instance` tool argument. Derived from
# INSTANCE_LABELS so OSS forks inherit the map automatically. WeChat has its
# own MCP (weixin_mcp.py), so we exclude it here.
TG_INSTANCES = [k for k in INSTANCE_LABELS if k != "weixin"]

INSTANCE_SCHEMA = {
    "type": "string",
    "enum": TG_INSTANCES,
    "description": (
        "Optional TG bot selector. "
        + "; ".join(f"'{k}' → {INSTANCE_LABELS[k]}" for k in TG_INSTANCES)
        + ". Omit → send via this MCP server's bound socket "
        "(terminal CC → 主 bot; bot-internal CC → that bot's own channel)."
    ),
}


def _socket_for_instance(instance: str | None) -> str:
    """Route to a specific instance's bridge socket, or fall back to the
    MCP server's bound SOCKET_PATH when instance is None."""
    if instance is None:
        return SOCKET_PATH
    ns = f"{PROJECT}-{instance}" if instance else PROJECT
    return f"/tmp/{ns}-bridge.sock"


server = Server("tg")


def _voice_description() -> str:
    """Facts about the actual TTS backend so CC can decide how to use markup."""
    base = (
        "Synthesize text to speech and send as a TG voice message. "
        "text may include backend-specific expressive markup."
    )
    backend = os.environ.get("TTS_BACKEND", "openai").lower()
    has_custom_url = bool(os.environ.get("TTS_URL"))

    if backend == "mimo" and has_custom_url:
        return (
            f"{base} Current backend (mimo-v2-tts) recognizes "
            "<style>arbitrary natural-language description</style> prefix "
            "(emotion/dialect/role/singing/free combinations), and full-width "
            "paren inline cues like （笑）/（咳嗽）/（叹气）/（停顿） for sound events."
        )
    if has_custom_url:
        return f"{base} Current backend is OpenAI-compatible /audio/speech (plain text, no markup)."
    return f"{base} Current backend is edge-tts (plain text, no markup)."


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="tg_send_buttons",
            description=(
                "Present interactive buttons. Each option is either a label string "
                "(callback) or {label, url} (opens link). Returns the callback label "
                "clicked, or 'Links sent' if all are URL buttons."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Message above buttons"},
                    "options": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                {"type": "string"},
                                {
                                    "type": "object",
                                    "properties": {
                                        "label": {"type": "string"},
                                        "url": {"type": "string"},
                                    },
                                    "required": ["label"],
                                },
                            ]
                        },
                        "minItems": 1,
                        "maxItems": 8,
                    },
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["text", "options"],
            },
        ),
        Tool(
            name="tg_send_text",
            description=(
                "Send a plain text message to the user's Telegram. "
                "When invoked from the TG bot channel, the assistant's final turn text "
                "is already auto-delivered — this tool is additive, for mid-turn pushes, "
                "long-running task progress, or proactive sends from terminal/scheduled callers."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["text"],
            },
        ),
        Tool(
            name="tg_send_file",
            description="Send a local file to the user as a TG document.",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Absolute or ~-relative file path"},
                    "caption": {"type": "string"},
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["path"],
            },
        ),
        Tool(
            name="tg_send_album",
            description="Send 2-10 local images as a TG media album.",
            inputSchema={
                "type": "object",
                "properties": {
                    "paths": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 2,
                        "maxItems": 10,
                    },
                    "caption": {"type": "string"},
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["paths"],
            },
        ),
        Tool(
            name="tg_send_location",
            description="Send a pinpoint location to the user. Attaches an Amap open-link button.",
            inputSchema={
                "type": "object",
                "properties": {
                    "latitude": {"type": "number"},
                    "longitude": {"type": "number"},
                    "name": {"type": "string", "description": "Optional place name for the map label"},
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["latitude", "longitude"],
            },
        ),
        Tool(
            name="tg_send_voice",
            description=_voice_description(),
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Text to speak (with optional markup)"},
                    "voice": {
                        "type": "string",
                        "description": "Optional voice identifier (backend-specific, e.g. nova/mimo_default/zh-CN-XiaoxiaoNeural)",
                    },
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["text"],
            },
        ),
        Tool(
            name="tg_send_video",
            description="Send a local video file (mp4/mov) to the user as a TG video message.",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "caption": {"type": "string"},
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["path"],
            },
        ),
        Tool(
            name="tg_send_page",
            description=(
                "Publish a Telegraph (telegra.ph) page from markdown and send the URL to TG — "
                "renders as an Instant View card with full rich layout (h3/h4 headings, real "
                "<ul>/<ol> lists, blockquote, syntax-highlighted code, inline b/i/u/s/code/a). "
                "Use when the reply has headings, long structured content, multi-language code blocks, "
                "or exceeds what TG's inline HTML parse_mode can render cleanly. "
                "content_md accepts standard markdown; unsupported elements (tables, h1/h2 in body) "
                "degrade to plain paragraphs. Returns the Telegraph URL."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "Page title — shown as h1 in the Instant View card and in TG's preview snippet",
                    },
                    "content_md": {
                        "type": "string",
                        "description": "Markdown body. Do NOT repeat the title inside; the page already shows it.",
                    },
                    "caption": {
                        "type": "string",
                        "description": "Optional short text placed above the URL in the TG message (e.g. a one-line summary)",
                    },
                    "instance": INSTANCE_SCHEMA,
                },
                "required": ["title", "content_md"],
            },
        ),
    ]


async def _relay(action: str, instance: str | None = None, **kwargs) -> str:
    """Send action to bridge, await result."""
    socket_path = _socket_for_instance(instance)
    reader, writer = await _open_bridge(socket_path)
    try:
        request = json.dumps({"action": action, **kwargs})
        writer.write(request.encode() + b"\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.readline(), timeout=300)
        return json.loads(response.decode()).get("result", "no result")
    finally:
        writer.close()
        await writer.wait_closed()


async def _open_bridge(socket_path: str):
    """Open the bot bridge, waiting through short bot restart windows.

    The MCP server lives inside the CPU process. During a transport restart the
    Unix socket disappears briefly while the CPU may continue running. Retrying
    connection setup lets post-restart CPU pushes attach to the new bot instead
    of failing permanently.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.0, _BRIDGE_CONNECT_RETRY_SECONDS)
    while True:
        try:
            return await asyncio.open_unix_connection(socket_path)
        except OSError as e:
            retryable = (
                isinstance(e, (FileNotFoundError, ConnectionRefusedError, ConnectionResetError))
                or e.errno in _RETRYABLE_CONNECT_ERRNOS
            )
            remaining = deadline - loop.time()
            if not retryable or remaining <= 0:
                raise
            await asyncio.sleep(min(max(0.1, _BRIDGE_CONNECT_RETRY_INTERVAL), remaining))


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    instance = arguments.get("instance")
    try:
        if name == "tg_send_buttons":
            result = await _relay(
                "buttons",
                instance=instance,
                text=arguments.get("text", ""),
                options=arguments.get("options", []),
            )
        elif name == "tg_send_text":
            result = await _relay("send_text", instance=instance, text=arguments["text"])
        elif name == "tg_send_file":
            result = await _relay(
                "send_file",
                instance=instance,
                path=arguments["path"],
                caption=arguments.get("caption", ""),
            )
        elif name == "tg_send_album":
            result = await _relay(
                "send_album",
                instance=instance,
                paths=arguments["paths"],
                caption=arguments.get("caption", ""),
            )
        elif name == "tg_send_location":
            result = await _relay(
                "send_location",
                instance=instance,
                latitude=arguments["latitude"],
                longitude=arguments["longitude"],
                name=arguments.get("name", ""),
            )
        elif name == "tg_send_voice":
            result = await _relay(
                "send_voice",
                instance=instance,
                text=arguments["text"],
                voice=arguments.get("voice", ""),
            )
        elif name == "tg_send_video":
            result = await _relay(
                "send_video",
                instance=instance,
                path=arguments["path"],
                caption=arguments.get("caption", ""),
            )
        elif name == "tg_send_page":
            result = await _relay(
                "send_page",
                instance=instance,
                title=arguments.get("title", PROJECT),
                content_md=arguments["content_md"],
                caption=arguments.get("caption", ""),
            )
        else:
            result = f"Unknown tool: {name}"
        return [TextContent(type="text", text=result)]
    except asyncio.TimeoutError:
        return [TextContent(type="text", text="Timeout waiting for TG")]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {e}")]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
