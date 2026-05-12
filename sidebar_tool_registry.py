"""Single source of truth for sidebar MCP tool schemas and prompt summaries."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


_TARGET_FIELDS: dict[str, dict[str, Any]] = {
    "tab_id": {
        "type": "integer",
        "description": "Target browser tab id from page_context. Prefer passing this for page tools.",
    },
    "window_id": {
        "type": "integer",
        "description": "Target browser window id from page_context. Fallback when tab_id is unavailable.",
    },
}


def _target_props(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return {**(extra or {}), **_TARGET_FIELDS}


SIDEBAR_TOOLS: list[dict[str, Any]] = [
    {
        "name": "tab_metadata",
        "dispatch": "bridge",
        "description": (
            "Read current active tab's meta (url / title / current selection / "
            "scrollY / docHeight / lang). Lightweight — no DOM extraction. "
            "Use to verify what V is looking at before deciding whether to "
            "compose dom_query for content. Pass tab_id/window_id from "
            "page_context to avoid racing V's active tab."
        ),
        "inputSchema": {"type": "object", "properties": _target_props()},
        "prompt": "tab_metadata(tab_id?, window_id?) — url / title / selection / scrollY / docHeight / lang",
    },
    {
        "name": "dom_query",
        "dispatch": "bridge",
        "description": (
            "querySelectorAll on V's active tab, return per-element prop list. "
            "selector default 'body'. props default ['tag','text']; available: "
            "tag / id / class / text (innerText, capped 1500) / html (innerHTML, "
            "capped 2000) / href / value / name / type / placeholder / rect "
            "(x/y/w/h px) / attrs (full attribute map). Use 'root' (selector) to "
            "scope querying inside one ancestor. limit caps result count (default "
            "50)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({
                "selector": {"type": "string", "description": "CSS selector; default 'body'"},
                "root": {"type": "string", "description": "Scope querySelectorAll inside this ancestor (default document)"},
                "limit": {"type": "integer", "description": "Max results (default 50)"},
                "props": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Properties to extract. Default ['tag','text']",
                },
            }),
        },
        "prompt": "dom_query(selector, root?, limit?, props?, tab_id?, window_id?) — querySelectorAll → 元素属性 array",
    },
    {
        "name": "dom_inject",
        "dispatch": "bridge",
        "description": (
            "insertAdjacentHTML on every element matching selector. position: "
            "beforebegin | afterbegin | beforeend (default) | afterend. Use for "
            "explicit page annotations or small UI helpers when V asks for page "
            "modification. Automatic page translation is handled by the content "
            "script /translate path, not this tool. Returns {count}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({
                "selector": {"type": "string"},
                "html": {"type": "string"},
                "position": {
                    "type": "string",
                    "enum": ["beforebegin", "afterbegin", "beforeend", "afterend"],
                },
            }),
            "required": ["selector", "html"],
        },
        "prompt": "dom_inject(selector, html, position?, tab_id?, window_id?) — insertAdjacentHTML 显式注入",
    },
    {
        "name": "dom_set",
        "dispatch": "bridge",
        "description": (
            "Set property on every match. prop: value (sets input/textarea + "
            "fires input/change events) | textContent | <any "
            "attribute name>. Use for filling forms or explicit page edits. "
            "Automatic page translation is handled by the content script "
            "/translate path, not this tool. Returns {count}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({
                "selector": {"type": "string"},
                "prop": {"type": "string"},
                "value": {"type": "string"},
            }),
            "required": ["selector", "prop", "value"],
        },
        "prompt": "dom_set(selector, prop, value, tab_id?, window_id?) — 设 value/textContent/attribute",
    },
    {
        "name": "dom_click",
        "dispatch": "bridge",
        "description": (
            "Synthetic .click() on first match. Returns {ok}. NOTE: synthetic "
            "(not isTrusted=true); some captcha-protected / OAuth buttons "
            "won't fire. If that happens, report the limitation instead of "
            "claiming trusted input."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({"selector": {"type": "string"}}),
            "required": ["selector"],
        },
        "prompt": "dom_click(selector, tab_id?, window_id?) — 合成 click; 失败要说明 synthetic 限制",
    },
    {
        "name": "page_snapshot",
        "dispatch": "bridge",
        "description": (
            "Build a compact visible-page map for the target tab. Returns "
            "{snapshot_id, tab_id, window_id, url, title, items, lines}. "
            "Each item has ref/role/tag/name/selector/rect/is_new. Prefer this "
            "before clicking or reasoning about page UI; is_new compares with "
            "the previous snapshot for the same tab."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({
                "limit": {"type": "integer", "description": "Max visible elements, default 120"},
            }),
        },
        "prompt": "page_snapshot(tab_id?, window_id?, limit?) — 可见页面地图: ref / role / name / selector / rect / is_new",
    },
    {
        "name": "page_click_ref",
        "dispatch": "bridge",
        "description": (
            "Click an element by ref from a previous page_snapshot. Required: "
            "snapshot_id and ref (e.g. e3). Uses the stored selector on the "
            "same tab, scrolls it into view, focuses if possible, then synthetic "
            ".click(). Returns {ok, selector, rect} or a stale-ref error."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({
                "snapshot_id": {"type": "string"},
                "ref": {"type": "string"},
            }),
            "required": ["snapshot_id", "ref"],
        },
        "prompt": "page_click_ref(snapshot_id, ref, tab_id?, window_id?) — 按 page_snapshot ref 点击",
    },
    {
        "name": "tab_navigate",
        "dispatch": "bridge",
        "description": "Navigate the target tab to url. Returns {ok, tab_id, url}.",
        "inputSchema": {
            "type": "object",
            "properties": _target_props({"url": {"type": "string"}}),
            "required": ["url"],
        },
        "prompt": "tab_navigate(url, tab_id?, window_id?) — 导航目标 tab",
    },
    {
        "name": "translate",
        "dispatch": "translate",
        "description": (
            "Translate plain text to target_lang (default zh) using the sidebar "
            "translation backend. Pure text tool: no DOM read, no DOM injection, "
            "no page side effect. Use when V asks to translate text or when you "
            "need a translation before deciding how to answer."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "target_lang": {"type": "string", "description": "Default zh"},
            },
            "required": ["text"],
        },
        "prompt": "translate(text, target_lang?) — server LLM 纯文本翻译, 默认 zh, 不读写页面",
    },
    {
        "name": "suggest_prompts",
        "dispatch": "notify",
        "description": (
            "Push prediction chips to the sidepanel UI. After reading a page / "
            "answering V's question, you may forecast 1–6 likely follow-up "
            "prompts (e.g. '总结全文' / '提取人物关系' / '帮我填表'). UI "
            "renders them as click-to-send chips. Don't over-suggest — only "
            "when you genuinely predict V's next move; empty list clears."
        ),
        "inputSchema": {
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
        "prompt": "suggest_prompts(prompts) — 推 1-6 个 follow-up chip 到 sidepanel UI",
    },
    {
        "name": "mascot_speak",
        "dispatch": "notify",
        "description": (
            "Make the babata mascot float a speech bubble on V's current page "
            "(content widget Shadow DOM). Use sparingly — only when you have "
            "a real opinion / heads-up / invitation worth interrupting V. "
            "Keep text short (≤40 zh chars), spoken-style. Examples: "
            "'这篇凑字数, 别浪费时间', '要我帮你填吗', '8 行总结好了, 看吗'. "
            "Auto-dismisses 30s. V can click bubble to open sidebar. Pass "
            "tab_id/window_id from page_context or proactive trigger when known."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _target_props({
                "text": {"type": "string", "description": "Bubble text"},
            }),
            "required": ["text"],
        },
        "prompt": "mascot_speak(text, tab_id?, window_id?) — 当前页面桌宠气泡, 主动提醒/邀请用",
    },
    {
        "name": "bookmarks_search",
        "dispatch": "bridge",
        "description": (
            "Search V's bookmarks by free-text query (matches title/url). "
            "Returns array of {id, title, url, parent_id, date_added}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "max_results": {"type": "integer", "description": "Default 50"},
            },
            "required": ["query"],
        },
        "prompt": "bookmarks_search(query, max_results?) — 搜索书签 title/url",
    },
    {
        "name": "bookmarks_tree",
        "dispatch": "bridge",
        "description": (
            "Return bookmark folder tree (folders only, no leaf bookmarks). "
            "Use to find an appropriate parent_id before bookmarks_create."
        ),
        "inputSchema": {"type": "object", "properties": {}},
        "prompt": "bookmarks_tree() — 返回书签文件夹树",
    },
    {
        "name": "bookmarks_create",
        "dispatch": "bridge",
        "description": (
            "Create a new bookmark. parent_id optional — defaults to top-level. "
            "Use bookmarks_tree first to find a sensible folder."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "url": {"type": "string"},
                "parent_id": {"type": "string"},
            },
            "required": ["title", "url"],
        },
        "prompt": "bookmarks_create(title, url, parent_id?) — 创建书签",
    },
    {
        "name": "tabs_query",
        "dispatch": "bridge",
        "description": (
            "Query open tabs. Filters all optional: active / audible / pinned "
            "(boolean) / current_window (true to scope) / url (URL pattern e.g. "
            "'*://x.com/*'). Returns array of {id, url, title, active, audible, "
            "pinned, group_id, window_id, last_accessed}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "active": {"type": "boolean"},
                "audible": {"type": "boolean"},
                "pinned": {"type": "boolean"},
                "current_window": {"type": "boolean"},
                "url": {"type": "string"},
            },
        },
        "prompt": "tabs_query(active?, audible?, pinned?, current_window?, url?) — 查询打开的 tabs",
    },
    {
        "name": "tabs_close",
        "dispatch": "bridge",
        "description": (
            "Close tabs by id list. Destructive — use after V's clear ask "
            "('关掉所有包含关键词的 tabs') or after asking. Returns {closed: n}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "tab_ids": {
                    "type": "array",
                    "items": {"type": "integer"},
                },
            },
            "required": ["tab_ids"],
        },
        "prompt": "tabs_close(tab_ids) — 关闭 tabs; 破坏性操作必须有清楚用户意图",
    },
    {
        "name": "tabs_group",
        "dispatch": "bridge",
        "description": (
            "Group tabs into a (possibly named/colored) tab group. color enum: "
            "grey/blue/red/yellow/green/pink/purple/cyan/orange. "
            "Returns {group_id, count}."
        ),
        "inputSchema": {
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
        "prompt": "tabs_group(tab_ids, group_title?, color?) — tab 分组",
    },
    {
        "name": "history_search",
        "dispatch": "bridge",
        "description": (
            "Search V's browsing history. text matches url/title fragments. "
            "start_ms/end_ms are unix epoch ms (default: 0 to now). "
            "Returns {id, url, title, last_visit, visit_count}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "start_ms": {"type": "integer"},
                "end_ms": {"type": "integer"},
                "max_results": {"type": "integer", "description": "Default 100"},
            },
            "required": ["text"],
        },
        "prompt": "history_search(text, start_ms?, end_ms?, max_results?) — 搜索浏览历史",
    },
]


BRIDGE_TOOL_NAMES = frozenset(t["name"] for t in SIDEBAR_TOOLS if t["dispatch"] == "bridge")


def tool_specs() -> list[dict[str, Any]]:
    return [
        {
            "name": tool["name"],
            "description": tool["description"],
            "inputSchema": deepcopy(tool["inputSchema"]),
        }
        for tool in SIDEBAR_TOOLS
    ]


def prompt_tool_lines() -> str:
    return "\n".join(f"- {tool['prompt']}" for tool in SIDEBAR_TOOLS)
