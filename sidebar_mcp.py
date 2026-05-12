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
from sidebar_translate import translate_batch

log = logging.getLogger(__name__)

SOCKET_PATH = "/tmp/babata-sidebar-bridge.sock"

server = Server("sidebar")

_TARGET_FIELDS = {
    "tab_id": {
        "type": "integer",
        "description": "Target browser tab id from page_context. Prefer passing this for page tools.",
    },
    "window_id": {
        "type": "integer",
        "description": "Target browser window id from page_context. Fallback when tab_id is unavailable.",
    },
}


# ── Tool surface ──────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="tab_metadata",
            description=(
                "Read current active tab's meta (url / title / current selection / "
                "scrollY / docHeight / lang). Lightweight — no DOM extraction. "
                "Use to verify what V is looking at before deciding whether to "
                "compose dom_query for content. Pass tab_id/window_id from "
                "page_context to avoid racing V's active tab."
            ),
            inputSchema={"type": "object", "properties": {**_TARGET_FIELDS}},
        ),
        Tool(
            name="dom_query",
            description=(
                "querySelectorAll on V's active tab, return per-element prop list. "
                "selector default 'body'. props default ['tag','text']; available: "
                "tag / id / class / text (innerText, capped 1500) / html (innerHTML, "
                "capped 2000) / href / value / name / type / placeholder / rect "
                "(x/y/w/h px) / attrs (full attribute map). Use 'root' (selector) to "
                "scope querying inside one ancestor. limit caps result count (default "
                "50)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "selector": {"type": "string", "description": "CSS selector; default 'body'"},
                    "root": {"type": "string", "description": "Scope querySelectorAll inside this ancestor (default document)"},
                    "limit": {"type": "integer", "description": "Max results (default 50)"},
                    "props": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Properties to extract. Default ['tag','text']",
                    },
                    **_TARGET_FIELDS,
                },
            },
        ),
        Tool(
            name="dom_inject",
            description=(
                "insertAdjacentHTML on every element matching selector. position: "
                "beforebegin | afterbegin | beforeend (default) | afterend. Use for "
                "explicit page annotations or small UI helpers when V asks for page "
                "modification. Automatic page translation is handled by the content "
                "script /translate path, not this tool. Returns {count}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "selector": {"type": "string"},
                    "html": {"type": "string"},
                    "position": {
                        "type": "string",
                        "enum": ["beforebegin", "afterbegin", "beforeend", "afterend"],
                    },
                    **_TARGET_FIELDS,
                },
                "required": ["selector", "html"],
            },
        ),
        Tool(
            name="dom_set",
            description=(
                "Set property on every match. prop: value (sets input/textarea + "
                "fires input/change events) | textContent | <any "
                "attribute name>. Use for filling forms or explicit page edits. "
                "Automatic page translation is handled by the content script "
                "/translate path, not this tool. Returns {count}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "selector": {"type": "string"},
                    "prop": {"type": "string"},
                    "value": {"type": "string"},
                    **_TARGET_FIELDS,
                },
                "required": ["selector", "prop", "value"],
            },
        ),
        Tool(
            name="dom_click",
            description=(
                "Synthetic .click() on first match. Returns {ok}. NOTE: synthetic "
                "(not isTrusted=true); some captcha-protected / OAuth buttons "
                "won't fire. If that happens, report the limitation instead of "
                "claiming trusted input."
            ),
            inputSchema={
                "type": "object",
                "properties": {"selector": {"type": "string"}, **_TARGET_FIELDS},
                "required": ["selector"],
            },
        ),
        Tool(
            name="page_snapshot",
            description=(
                "Build a compact visible-page map for the target tab. Returns "
                "{snapshot_id, tab_id, window_id, url, title, items, lines}. "
                "Each item has ref/role/tag/name/selector/rect/is_new. Prefer this "
                "before clicking or reasoning about page UI; is_new compares with "
                "the previous snapshot for the same tab."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max visible elements, default 120"},
                    **_TARGET_FIELDS,
                },
            },
        ),
        Tool(
            name="page_click_ref",
            description=(
                "Click an element by ref from a previous page_snapshot. Required: "
                "snapshot_id and ref (e.g. e3). Uses the stored selector on the "
                "same tab, scrolls it into view, focuses if possible, then synthetic "
                ".click(). Returns {ok, selector, rect} or a stale-ref error."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "snapshot_id": {"type": "string"},
                    "ref": {"type": "string"},
                    **_TARGET_FIELDS,
                },
                "required": ["snapshot_id", "ref"],
            },
        ),
        Tool(
            name="tab_navigate",
            description="Navigate the target tab to url. Returns {ok, tab_id, url}.",
            inputSchema={
                "type": "object",
                "properties": {"url": {"type": "string"}, **_TARGET_FIELDS},
                "required": ["url"],
            },
        ),
        Tool(
            name="translate",
            description=(
                "Translate plain text to target_lang (default zh) using the sidebar "
                "translation backend. Pure text tool: no DOM read, no DOM injection, "
                "no page side effect. Use when V asks to translate text or when you "
                "need a translation before deciding how to answer."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "target_lang": {"type": "string", "description": "Default zh"},
                },
                "required": ["text"],
            },
        ),
        Tool(
            name="suggest_prompts",
            description=(
                "Push prediction chips to the sidepanel UI. After reading a page / "
                "answering V's question, you may forecast 1–6 likely follow-up "
                "prompts (e.g. '总结全文' / '提取人物关系' / '帮我填表'). UI "
                "renders them as click-to-send chips. Don't over-suggest — only "
                "when you genuinely predict V's next move; empty list clears."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "prompts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "1–6 short prompts (under 20 chars each ideal)",
                    },
                },
                "required": ["prompts"],
            },
        ),
        Tool(
            name="mascot_speak",
            description=(
                "Make the babata mascot float a speech bubble on V's current page "
                "(content widget Shadow DOM). Use sparingly — only when you have "
                "a real opinion / heads-up / invitation worth interrupting V. "
                "Keep text short (≤40 zh chars), spoken-style. Examples: "
                "'这篇凑字数, 别浪费时间', '要我帮你填吗', '8 行总结好了, 看吗'. "
                "Auto-dismisses 30s. V can click bubble to open sidebar. Pass "
                "tab_id/window_id from page_context or proactive trigger when known."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string", "description": "Bubble text"},
                    **_TARGET_FIELDS,
                },
                "required": ["text"],
            },
        ),
        Tool(
            name="bookmarks_search",
            description=(
                "Search V's bookmarks by free-text query (matches title/url). "
                "Returns array of {id, title, url, parent_id, date_added}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "max_results": {"type": "integer", "description": "Default 50"},
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="bookmarks_tree",
            description=(
                "Return bookmark folder tree (folders only, no leaf bookmarks). "
                "Use to find an appropriate parent_id before bookmarks_create."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="bookmarks_create",
            description=(
                "Create a new bookmark. parent_id optional — defaults to top-level. "
                "Use bookmarks_tree first to find a sensible folder."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "url": {"type": "string"},
                    "parent_id": {"type": "string"},
                },
                "required": ["title", "url"],
            },
        ),
        Tool(
            name="tabs_query",
            description=(
                "Query open tabs. Filters all optional: active / audible / pinned "
                "(boolean) / current_window (true to scope) / url (URL pattern e.g. "
                "'*://x.com/*'). Returns array of {id, url, title, active, audible, "
                "pinned, group_id, window_id, last_accessed}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "active": {"type": "boolean"},
                    "audible": {"type": "boolean"},
                    "pinned": {"type": "boolean"},
                    "current_window": {"type": "boolean"},
                    "url": {"type": "string"},
                },
            },
        ),
        Tool(
            name="tabs_close",
            description=(
                "Close tabs by id list. Destructive — use after V's clear ask "
                "('关掉所有包含关键词的 tabs') or after asking. Returns {closed: n}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "tab_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                },
                "required": ["tab_ids"],
            },
        ),
        Tool(
            name="tabs_group",
            description=(
                "Group tabs into a (possibly named/colored) tab group. color enum: "
                "grey/blue/red/yellow/green/pink/purple/cyan/orange. "
                "Returns {group_id, count}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "tab_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "group_title": {"type": "string"},
                    "color": {"type": "string"},
                },
                "required": ["tab_ids"],
            },
        ),
        Tool(
            name="history_search",
            description=(
                "Search V's browsing history. text matches url/title fragments. "
                "start_ms/end_ms are unix epoch ms (default: 0 to now). "
                "Returns {id, url, title, last_visit, visit_count}."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "start_ms": {"type": "integer"},
                    "end_ms": {"type": "integer"},
                    "max_results": {"type": "integer", "description": "Default 100"},
                },
                "required": ["text"],
            },
        ),
    ]


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
        if name in {
            "tab_metadata", "dom_query", "dom_inject", "dom_set",
            "dom_click", "page_snapshot", "page_click_ref", "tab_navigate",
            "bookmarks_search", "bookmarks_tree", "bookmarks_create",
            "tabs_query", "tabs_close", "tabs_group",
            "history_search",
        }:
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
