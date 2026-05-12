"""Standalone stdio MCP server exposing browser sidebar capabilities to CC.

Channel #3 of babata. sidebar_mcp spawns inside CC's process tree (one stdio
server per cc.query session), relays tool calls to sidebar_bot through Unix
socket bridge (/tmp/babata-sidebar-bridge.sock), which fans out to the
connected browser extension SW over WebSocket.

V0 哲学: 给 LLM raw primitive 不给 workflow.
- Browser-side: raw DOM/page primitive (tab_metadata / dom_query / dom_inject
  / dom_set / dom_click / page_snapshot / page_click_ref + tab_navigate).
  LLM compose 它们做任意页面读写 — 抓正文 / 表单填写 / 点击 / 显式注释.
- Server-side: translate (raw 翻译, 不带抓注入副作用) + suggest_prompts (推
  chip).
"""

import asyncio
import hashlib
import json
import logging

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool
from sidebar_tool_registry import BRIDGE_TOOL_NAMES, tool_specs
from sidebar_translate import translate_batch

log = logging.getLogger(__name__)

SOCKET_PATH = "/tmp/babata-sidebar-bridge.sock"

server = Server("sidebar")


# ── Tool surface ──────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [Tool(**spec) for spec in tool_specs()]


# ── Bridge relay ──────────────────────────────────────────────────────

async def _bridge_call(action: str, **payload) -> dict:
    """Open Unix socket, send {action, ...payload}, await {ok, result, error}."""
    reader, writer = await asyncio.open_unix_connection(SOCKET_PATH)
    try:
        request = json.dumps({"action": action, **payload}, ensure_ascii=False)
        writer.write(request.encode() + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=60)
        return json.loads(line.decode())
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def _format_result(payload: dict) -> str:
    """Bridge return → text MCP result. JSON-encode result on success, expose
    error verbatim on failure (so LLM sees the real cause)."""
    if not payload.get("ok"):
        err = payload.get("error") or "unknown error"
        return f"Error: {err}"
    result = payload.get("result")
    if result is None:
        return "ok"
    if isinstance(result, str):
        return result
    return json.dumps(result, ensure_ascii=False)


# ── tool dispatch ─────────────────────────────────────────────────────

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        # Direct browser-side primitives — round-trip via bridge.
        if name in BRIDGE_TOOL_NAMES:
            payload = await _bridge_call(name, args=arguments or {})
            return [TextContent(type="text", text=_format_result(payload))]

        if name == "translate":
            text = (arguments.get("text") or "").strip()
            if not text:
                return [TextContent(type="text", text="Error: 'text' required")]
            target = (
                arguments.get("target_lang")
                or arguments.get("target")
                or "zh"
            )
            target = str(target).strip() or "zh"
            h = hashlib.sha256(f"{target}\0{text}".encode("utf-8")).hexdigest()[:16]
            results = await translate_batch(
                "mcp",
                target,
                [{"hash": h, "text": text}],
                url="mcp://sidebar/translate",
            )
            translated = next(
                (r.get("translated") for r in results if r.get("hash") == h),
                "",
            )
            if not translated:
                return [TextContent(type="text", text="Error: translate failed")]
            return [TextContent(type="text", text=str(translated))]

        if name == "suggest_prompts":
            prompts = arguments.get("prompts") or []
            if not isinstance(prompts, list):
                return [TextContent(type="text", text="Error: 'prompts' must be a list")]
            payload = await _bridge_call(
                "notify_sw",
                name="suggest_prompts",
                args={"prompts": prompts},
            )
            return [TextContent(type="text", text=_format_result(payload))]

        if name == "mascot_speak":
            text = (arguments.get("text") or "").strip()
            if not text:
                return [TextContent(type="text", text="Error: 'text' required")]
            args = {"text": text}
            if isinstance(arguments.get("tab_id"), int):
                args["tab_id"] = arguments["tab_id"]
            if isinstance(arguments.get("window_id"), int):
                args["window_id"] = arguments["window_id"]
            payload = await _bridge_call(
                "notify_sw",
                name="mascot_speak",
                args=args,
            )
            return [TextContent(type="text", text=_format_result(payload))]

        return [TextContent(type="text", text=f"Unknown tool: {name}")]
    except asyncio.TimeoutError:
        return [TextContent(type="text", text="Timeout waiting for sidebar bridge")]
    except FileNotFoundError:
        return [TextContent(
            type="text",
            text=f"Bridge socket missing ({SOCKET_PATH}) — sidebar_bot not running?",
        )]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {type(e).__name__}: {e}")]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
