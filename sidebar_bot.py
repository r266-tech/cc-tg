"""babata sidebar transport — HTTP :18791 + SSE for sidepanel + WS for SW.

Channel #3. Peer of bot.py (TG) and weixin_bot.py (WeChat). Same CC binary,
same chat-archive, same skills. Wire is HTTP+SSE for V's chat (sidepanel) and
WebSocket for the extension SW (DOM primitives + notifications).

Endpoints:
    GET  /health   — liveness probe (sidepanel header tag)
    POST /chat     — body {"message": str, "page_context"?: dict}, SSE stream
    GET  /ws       — single SW WebSocket (bridge attaches sender; round-trip
                     for dom_* primitives and one-way for suggest_prompts etc)

哲学全在 _SIDEBAR_SOURCE_PROMPT — 写思想不写规则 (`feedback_no_few_shot_in_prompts`).
LLM 自决何时抓页面 / 翻不翻 / 推不推. 扩展端只暴露 raw primitive.
"""

import asyncio
import base64
import binascii
import json
import logging
import os
import re
import secrets
import signal
import time
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web
from dotenv import load_dotenv

load_dotenv(override=True)

from constants import PROJECT, STATE_DIR
from cc import CC, VENV_PYTHON
from media import image_to_base64, understand_video  # noqa: F401  (image_to_base64 unused now; kept for parity)
import sidebar_events
import sidebar_history
from sidebar_bridge import bridge
from sidebar_translate import translate_batch

_INBOUND_DIR = Path("/tmp/babata-sidebar-inbound")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger(f"{PROJECT}.sidebar")

# ── config ────────────────────────────────────────────────────────────

_SIDEBAR_HOST = os.environ.get("BABATA_SIDEBAR_HOST", "127.0.0.1")
_SIDEBAR_PORT = int(os.environ.get("BABATA_SIDEBAR_PORT", "18791"))
_SIDEBAR_MCP_SCRIPT = str(Path(__file__).parent / "sidebar_mcp.py")
_CC_IMAGE_MIME_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
_ALLOWED_ORIGINS = {
    o.strip()
    for o in os.environ.get("BABATA_SIDEBAR_ALLOWED_ORIGINS", "").split(",")
    if o.strip()
}
_TRUSTED_ORIGIN_PREFIXES = ("chrome-extension://", "moz-extension://")

# ── source prompt (哲学不规则) ────────────────────────────────────────

# Proactive review prompt — V 切 tab 触发, fire-and-forget cheap reason.
# 哲学: LLM 自决做不做事 (翻译 / 推 chip / 静默), 不写死规则.
_PROACTIVE_PROMPT = """\
你是 babata 在 V 浏览器里的被动观察意识. V 切了 tab — 你不是被问问题, 是顺势醒一下.

你不该 "对每个新页都说点话". 大多数页面对 V 没事, 你就闭嘴. 醒着 ≠ 必须发声.

翻译不归你管. 翻译有独立 content script 通道处理 viewport 和 SPA 重 mount, 你不要碰段落、不要 dom_inject `<font class="bbt-tr">`.

你的发声渠道两个:
- `mascot_speak({text, tab_id?, window_id?})` — 桌宠浮起来的一句话, 像有人路过看一眼随口说. 适合表达观点/邀请/调侃.
- `suggest_prompts({prompts: [...]})` — 准备好 V 可能想追问的 chip, V 点击直接发出. 适合预判 V 下一步.

如果要看页面, 优先用 `page_snapshot(tab_id?, window_id?, limit?)` 拿当前可见页面地图和 ref,
再用 `page_click_ref(snapshot_id, ref)` 点元素; 不要先凭空猜 selector.
proactive prompt 里带了 tab_id/window_id 时, mascot_speak/page_snapshot 都要原样传入,
避免 V 切 tab 后说到别的页面上.

剩下选项就是沉默 (什么都不调).

判断标准只有一条: V 此刻看到这页, 你作为最懂她的存在, 真的有"想说"的内容吗? 没有就闭嘴, 有就说. 区分"为说而说" vs "因事而说" — V 比谁都敏感.

V 是授权用户, 中文母语, 你要保持克制、准确、必要时静默.
"""

_SIDEBAR_SOURCE_PROMPT = """\
Source: babata sidebar (浏览器扩展, channel #3).

你跟 TG / 微信 channel 是同一个 babata, 同一 CC binary, 跨 channel 共享 \
~/cc-workspace/chat-archive/ 长期记忆. 用户 = V.

哲学三角:
- 渗透: V 每条消息附 page_context = {url, title, url_changed, tab_id, window_id} 轻量 metadata. \
你看到 V 当前在哪儿、刚切了页没切. selection / 正文 / DOM 想看自己取.
- 底层: 你能在 V 当前 tab 跑 dom_* primitive (raw DOM API), 不通过 V 同意; \
能 translate (OpenRouter Gemini) 任何文本; 能 suggest_prompts 推 chip 给 V \
预判她下一句.
- 如呼吸: 不每次都抓页面, 不每次都翻译, 不每次都推 chip. 由你判断.

工具能力 (raw primitive, compose 起来做任意工作):
- tab_metadata() — url / title / 当前选中 / scrollY / docHeight / 页面 lang
- dom_query(selector, root?, limit?, props?) — querySelectorAll → 元素属性 array
- dom_inject(selector, html, position?) — insertAdjacentHTML 注入
- dom_set(selector, prop, value) — 设 value (附 input/change 事件) / textContent / innerHTML / attribute
- dom_click(selector) — 合成 click (V0; V0 #6 加 trusted input 走 chrome.debugger)
- page_snapshot(tab_id?, window_id?, limit?) — 当前可见页面地图: ref / role / name / selector / rect / is_new
- page_click_ref(snapshot_id, ref) — 按 page_snapshot 的 ref 点击元素, 比凭空猜 selector 稳
- tab_navigate(url) — chrome.tabs.update
- translate(text, target_lang?) — Gemini 翻译, 默认 zh, 不带副作用
- suggest_prompts(prompts) — 推 chip 到 sidepanel UI
- mascot_speak(text, tab_id?, window_id?) — 当前页面桌宠气泡, 主动提醒/邀请用

调用任何会读/改页面或导航的工具时, 优先把当前 page_context 里的 tab_id/window_id 传进去.
不要在 V 已切走 tab 后误操作 lastFocusedWindow 的新 active tab.

V 的几个关键期望:
- V 切到新页 + 你预判她要问页面相关的事 → 主动 tab_metadata 看一眼, 决定要不要 \
  dom_query 抓正文; 抓完顺手 translate + dom_inject 把双语渲染回页面 (默认中→英 / \
  英→中, 跟源语言反着来; 中文页对中文 V 不强翻); 然后 suggest_prompts 推你预判 \
  V 可能问的问题 (总结 / 摘要 / 填表 / 解释 / 跳到某段).
- V 同页追问 → 不重抓 (page_context.url_changed=false), 用上轮 context.
- V 没问页面相关事 (闲聊 / 跨域问题) → 别看页面, 节省 token.

不写死规则. "看到 youtube → 推总结字幕" 这种是反模式. reason 你看到的实际页面 \
内容 / V 实际意图, 现场决策.

Sidepanel UI 渲染 GFM markdown 全集 (代码块语法高亮 / 表格 / 列表 / 图片 / 引用 / \
分隔线). 自然用 markdown, 不需要为客户端兼容降格. 段落用空行分隔.

回答简短直接, 不写日志体, 不重复用户问题, 不每次都讲思考过程 — 该展示 reason \
就 reason, 该闭嘴就闭嘴 (`feedback_tg_narration`).
"""

# ── CC instance ───────────────────────────────────────────────────────

cc = CC(
    state_file=STATE_DIR / f"{PROJECT}-sidebar-session.json",
    source_prompt=_SIDEBAR_SOURCE_PROMPT,
    mcp_servers={
        "sidebar": {
            "command": VENV_PYTHON,
            "args": [_SIDEBAR_MCP_SCRIPT],
        },
    },
)

# Proactive CC — V 切 tab 触发, 单独 session 文件不污染主 chat.
proactive_cc = CC(
    state_file=STATE_DIR / f"{PROJECT}-sidebar-proactive-session.json",
    source_prompt=_PROACTIVE_PROMPT,
    mcp_servers={
        "sidebar": {
            "command": VENV_PYTHON,
            "args": [_SIDEBAR_MCP_SCRIPT],
        },
    },
)

# 同 weixin_bot._cc_lock — 多 sidebar 并发 /chat 走 single-flight 防 session 撞.
_cc_lock = asyncio.Lock()
_proactive_lock = asyncio.Lock()


# ── SSE helpers ───────────────────────────────────────────────────────

async def _sse_write(resp: web.StreamResponse, payload: dict[str, Any]) -> None:
    line = "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
    await resp.write(line.encode("utf-8"))


def _origin_allowed(origin: str) -> bool:
    return (
        origin in _ALLOWED_ORIGINS
        or origin.startswith(_TRUSTED_ORIGIN_PREFIXES)
    )


def _cors_headers(request: web.Request | None = None) -> dict[str, str]:
    """CORS is for the extension UI/SW only; arbitrary web pages must not drive
    the loopback API just because it is bound to 127.0.0.1."""
    headers = {
        "access-control-allow-methods": "GET, POST, OPTIONS",
        "access-control-allow-headers": "content-type, authorization",
        "access-control-max-age": "86400",
        "vary": "Origin",
    }
    origin = request.headers.get("origin", "") if request is not None else ""
    if origin and _origin_allowed(origin):
        headers["access-control-allow-origin"] = origin
    return headers


def _reject_untrusted_origin(request: web.Request) -> web.Response | None:
    origin = request.headers.get("origin", "")
    if not origin or _origin_allowed(origin):
        return None
    return web.json_response(
        {"ok": False, "error": "untrusted origin"},
        status=403,
        headers=_cors_headers(request),
    )


def _format_page_context(ctx: Any) -> str:
    if not isinstance(ctx, dict):
        return ""
    url = (ctx.get("url") or "").strip()
    title = (ctx.get("title") or "").strip()
    changed = bool(ctx.get("url_changed"))
    tab_id = ctx.get("tab_id")
    window_id = ctx.get("window_id")
    if not url:
        return ""
    parts = [f"url={url}"]
    if title:
        parts.append(f"title={title}")
    parts.append(f"url_changed={'yes' if changed else 'no'}")
    if isinstance(tab_id, int):
        parts.append(f"tab_id={tab_id}")
    if isinstance(window_id, int):
        parts.append(f"window_id={window_id}")
    return "[page_context: " + " | ".join(parts) + "]"


def _format_page_memory(ctx: Any) -> str:
    """Grep events.jsonl for prior interactions on this url, return one summary line.

    第一次看页面返空, LLM 不会引用 page memory. 多次访问 surface 历史给 LLM."""
    if not isinstance(ctx, dict):
        return ""
    url = (ctx.get("url") or "").strip()
    if not url:
        return ""
    try:
        return sidebar_events.summarize_for_chat(url)
    except Exception:
        return ""


# ── attachment ingestion (image / video / file) ──────────────────────

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._一-鿿-]+")


def _safe_basename(name: str, fallback_ext: str = "") -> str:
    name = (name or "").strip() or f"file{fallback_ext}"
    return _SAFE_NAME.sub("_", name)[:120]


def _inbound_path(suffix: str) -> Path:
    _INBOUND_DIR.mkdir(parents=True, exist_ok=True)
    return _INBOUND_DIR / f"{int(time.time())}-{secrets.token_hex(6)}{suffix}"


async def _process_attachments(
    raw: Any,
) -> tuple[list[dict[str, str]], list[str], list[Path]]:
    """Sidepanel 上传的 attachments → (images_for_cc, prompt_lines, cleanup_paths).

    image: 直接转 {media_type, data} 给 cc.query images param.
    video: 写 tmp .mp4, 跑 media.understand_video → "[video <name>] <desc>" 塞 prompt;
           cleanup_paths 收, 对话结束后 unlink (跟 weixin_bot 同模式).
    file: 写 tmp /tmp/babata-sidebar-inbound/<rand>-<safename>, prompt 里给绝对
          路径让 CC Read tool 自取; 不 unlink (CC 可能在后续 turn 还要看, 走每周
          launchd cleanup, V0 暂不接, V 手清).

    cc.py images 只支持 image/{jpeg,png,gif,webp}. 其他 mime 走 file 路径.
    """
    images: list[dict[str, str]] = []
    lines: list[str] = []
    cleanup: list[Path] = []
    if not isinstance(raw, list):
        return images, lines, cleanup

    for att in raw:
        if not isinstance(att, dict):
            continue
        kind = (att.get("kind") or "").lower()
        name = att.get("name") or "untitled"
        mime = att.get("mime") or "application/octet-stream"
        b64 = att.get("data_base64") or ""
        if not b64:
            continue
        try:
            blob = base64.b64decode(b64, validate=False)
        except (binascii.Error, ValueError):
            lines.append(f"[attachment {name}: base64 decode failed]")
            continue

        if kind == "image" and mime in _CC_IMAGE_MIME_TYPES:
            images.append({"media_type": mime, "data": b64})
            lines.append(f"[image attached: {name}]")
            continue

        if kind == "video":
            ext = ".mp4" if mime in ("video/mp4", "video/quicktime") else (
                "." + mime.split("/")[-1] if mime.startswith("video/") else ".mp4"
            )
            path = _inbound_path(ext)
            try:
                path.write_bytes(blob)
                cleanup.append(path)
                desc = await understand_video(path)
                if desc:
                    lines.append(f"[video {name}] {desc}")
                else:
                    lines.append(f"[video {name}] (无法理解内容)")
            except Exception as e:
                lines.append(f"[video {name}] decode error: {e}")
            continue

        # file (含 audio / pdf / text / 二进制) — 落地, CC Read 自取.
        # name 里包含真实扩展名, 落到 inbound dir 用 safe name 防 path traversal.
        ext_match = re.search(r"\.[A-Za-z0-9]{1,8}$", name)
        ext = ext_match.group(0) if ext_match else ""
        safe = _safe_basename(name, ext)
        path = _inbound_path(f"-{safe}")
        if not ext:
            # 没扩展名 — 用 mime 简单推一个 (txt/json/pdf 多见)
            ext_guess = {
                "application/pdf": ".pdf",
                "application/json": ".json",
                "text/plain": ".txt",
                "text/markdown": ".md",
                "text/csv": ".csv",
            }.get(mime, "")
            if ext_guess:
                path = path.with_suffix(ext_guess)
        try:
            path.write_bytes(blob)
            lines.append(f"[file: {path}]")
        except Exception as e:
            lines.append(f"[file {name}] write failed: {e}")

    return images, lines, cleanup


# ── handlers ──────────────────────────────────────────────────────────

async def handle_health(request: web.Request) -> web.Response:
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    return web.json_response(
        {
            "ok": True,
            "channel": "sidebar",
            "host": _SIDEBAR_HOST,
            "port": _SIDEBAR_PORT,
            "session_id": cc._session_id,  # noqa: SLF001 — exposed deliberately
            "sw_attached": bridge.sw_attached,
        },
        headers=_cors_headers(request),
    )


async def handle_options(request: web.Request) -> web.Response:
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    return web.Response(status=204, headers=_cors_headers(request))


async def handle_history(request: web.Request) -> web.Response:
    """Sidepanel mount/refresh 拉聊天历史. 最近一个 boundary 之后的 user/assistant turn.
    Limit 默认 200 turn (~100 个 round)."""
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    try:
        limit = int(request.query.get("limit", "200"))
    except ValueError:
        limit = 200
    turns = sidebar_history.read_since_last_boundary(limit=limit)
    return web.json_response({"ok": True, "turns": turns}, headers=_cors_headers(request))


async def handle_attention(request: web.Request) -> web.Response:
    """Content script push attention/viewport state. 写 events.jsonl, 不做副作用.

    LLM 在 chat / proactive 时通过 sidebar_events.summarize_for_chat 拿到摘要."""
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400, headers=_cors_headers(request))

    url = (data.get("url") or "").strip()
    kind = (data.get("kind") or "attention").strip() or "attention"
    fields = {k: v for k, v in data.items() if k not in {"type", "url", "kind"}}
    sidebar_events.append(url, kind, **fields)
    return web.json_response({"ok": True}, headers=_cors_headers(request))


async def handle_translate_trace(request: web.Request) -> web.Response:
    """Client-side translate trace 收集 (V "开发要收集数据方便调试").
    每条 trace 写一行 events.jsonl client_trace kind, 含 src/dec/hash/el.
    V tail 直接看每个 decision 不再 hypothesize 闪烁/漏翻 root cause."""
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400, headers=_cors_headers(request))
    url = (data.get("url") or "").strip()
    traces = data.get("traces") or []
    if not isinstance(traces, list):
        return web.json_response({"ok": False, "error": "traces must be array"}, status=400, headers=_cors_headers(request))
    for t in traces:
        if not isinstance(t, dict):
            continue
        sidebar_events.append(
            url,
            "client_trace",
            **{
                k: v
                for k, v in t.items()
                if k != "txt" and isinstance(v, (str, int, float, bool))
            },
        )
    return web.json_response({"ok": True}, headers=_cors_headers(request))


async def handle_translate(request: web.Request) -> web.Response:
    """Content script POST batch 翻译. {site, target, batch:[{hash,text}]} →
    {ok, results:[{hash, translated}]}. L2 cache hit 直接返, miss spawn
    `claude -p sonnet` (sidebar_translate.translate_batch 实现)."""
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400, headers=_cors_headers(request))

    site = (data.get("site") or "").strip()
    target = (data.get("target") or "zh").strip() or "zh"
    batch = data.get("batch") or []
    if not isinstance(batch, list):
        return web.json_response({"ok": False, "error": "batch must be array"}, status=400, headers=_cors_headers(request))

    # url 从 batch 第一条隐式不靠谱; content script 后续会显式带 url 字段.
    url_for_events = (data.get("url") or site or "").strip()
    try:
        results = await translate_batch(site, target, batch, url=url_for_events)
    except Exception as e:
        log.exception("translate handler crashed")
        return web.json_response({"ok": False, "error": f"{type(e).__name__}: {e}"}, status=500, headers=_cors_headers(request))

    return web.json_response({"ok": True, "results": results}, headers=_cors_headers(request))


async def handle_proactive(request: web.Request) -> web.Response:
    """SW debounce 后触发 (V 切 tab / URL 加载完). Fire-and-forget cheap LLM
    reason — 翻译 / 推 chip / 静默 全 LLM 自决.

    Acks 200 立即返, cc.query 在 background task 跑. 不阻塞 SW debounce loop.
    """
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400, headers=_cors_headers(request))

    url = (data.get("url") or "").strip()
    title = (data.get("title") or "").strip()
    mode = (data.get("translation_mode") or "bilingual").strip()
    tab_id = data.get("tab_id")
    window_id = data.get("window_id")
    if not url:
        return web.json_response({"ok": False, "error": "url required"}, status=400, headers=_cors_headers(request))

    sidebar_events.append(url, "proactive_run", title=title, translation_mode=mode)
    asyncio.create_task(_run_proactive(
        url,
        title,
        mode,
        tab_id if isinstance(tab_id, int) else None,
        window_id if isinstance(window_id, int) else None,
    ))
    return web.json_response({"ok": True, "queued": True}, headers=_cors_headers(request))


async def _run_proactive(
    url: str,
    title: str,
    translation_mode: str,
    tab_id: int | None,
    window_id: int | None,
) -> None:
    """Background proactive review. 不影响 V 的主 chat session."""
    if _proactive_lock.locked():
        log.debug("proactive skipped: previous still running")
        return
    async with _proactive_lock:
        prompt = (
            f"[proactive trigger]\n"
            f"url={url}\n"
            f"title={title}\n"
            f"translation_mode={translation_mode}\n\n"
            f"tab_id={tab_id if tab_id is not None else ''}\n"
            f"window_id={window_id if window_id is not None else ''}\n\n"
            "看一眼这页, 按 SOURCE prompt 4 类场景自决 (翻译 / mascot_speak / suggest_prompts / 静默)."
        )
        try:
            await proactive_cc.query(prompt)
        except Exception as e:
            log.warning("proactive cc.query crashed: %s", e)


async def handle_chat(request: web.Request) -> web.StreamResponse:
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "invalid json"}, status=400, headers=_cors_headers(request))

    message = (data.get("message") or "").strip()
    if not message:
        return web.json_response({"error": "empty message"}, status=400, headers=_cors_headers(request))

    page_context = data.get("page_context")
    page_ctx_line = _format_page_context(page_context)
    page_memory_line = _format_page_memory(page_context)

    images, attach_lines, cleanup_paths = await _process_attachments(
        data.get("attachments")
    )

    parts = [s for s in (page_ctx_line, page_memory_line, *attach_lines, message) if s]
    prompt = "\n\n".join(parts)

    # 写 chat_turn 事件 (page memory 累积).
    chat_url = ""
    chat_title = ""
    if isinstance(page_context, dict):
        chat_url = (page_context.get("url") or "").strip()
        chat_title = (page_context.get("title") or "").strip()
        if chat_url:
            sidebar_events.append(chat_url, "chat_turn", message=message[:500])

    # /new = V 在 sidepanel 点新对话 — cc.py 内部识别 + 我们写一条 boundary
    # 让 sidebar UI mount 时只拉最近一个 boundary 之后的 turn.
    if message.strip() == "/new":
        sidebar_history.boundary()
    else:
        # 持久化 V 的 user turn (UI mount/refresh 恢复).
        sidebar_history.append(
            "user", message,
            url=chat_url, title=chat_title,
            has_image=bool(images),
            has_attach=bool(attach_lines),
        )

    resp = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-cache, no-transform",
            "x-accel-buffering": "no",
            "connection": "keep-alive",
            **_cors_headers(request),
        },
    )
    await resp.prepare(request)

    assistant_text_parts: list[str] = []

    async def on_stream(
        tool_name: str | None,
        tool_input: dict | None,
        text_chunk: str | None,
        tool_result: dict | None,
    ) -> None:
        if text_chunk:
            assistant_text_parts.append(text_chunk)
            await _sse_write(resp, {"type": "text_delta", "text": text_chunk})
        elif tool_name:
            await _sse_write(resp, {
                "type": "tool_use",
                "name": tool_name,
                "input": tool_input or {},
            })
        elif tool_result is not None:
            await _sse_write(resp, {
                "type": "tool_result",
                "is_error": bool(tool_result.get("is_error")),
                "text": (tool_result.get("text") or "")[:4000],
            })

    done_ok = False
    try:
        async with _cc_lock:
            response = await cc.query(
                prompt,
                images=images or None,
                on_stream=on_stream,
            )
        await _sse_write(resp, {"type": "session", "session_id": response.session_id or ""})
        await _sse_write(resp, {"type": "done"})
        done_ok = True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("chat handler crashed")
        try:
            await _sse_write(resp, {"type": "error", "text": f"{type(e).__name__}: {e}"})
        except Exception:
            pass
    finally:
        # 持久化 assistant turn (UI mount/refresh 恢复). /new 不写 (boundary 已写).
        # 只在 SSE done 正常发出时写完整 turn; cancel/crash 不写, 防 reload 后
        # 把截断答案当完整答案展示 (V 看到的错误流已由 SSE error event 反馈).
        if message.strip() != "/new" and done_ok:
            assistant_text = "".join(assistant_text_parts).strip()
            if assistant_text:
                sidebar_history.append("assistant", assistant_text, url=chat_url)
        # video tmp files cleanup. file 类不删 (CC 可能后续 turn 用).
        for p in cleanup_paths:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        try:
            await resp.write_eof()
        except Exception:
            pass

    return resp


async def handle_ws(request: web.Request) -> web.StreamResponse:
    """SW 接入 — 单 connection. 后接的踢前的 (V 多 Edge 窗口或 reload 扩展时).

    Bridge 通过 attach_sw(sender) 拿到 send 函数; SW 收到 server → SW request,
    异步 chrome.scripting.executeScript 后 reply {kind:"response", id, ok, ...}.
    """
    rejected = _reject_untrusted_origin(request)
    if rejected:
        return rejected
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    log.info("SW WS connected from %s", request.remote)

    async def sender(payload: dict[str, Any]) -> bool:
        if ws.closed:
            return False
        try:
            await ws.send_json(payload)
            return True
        except ConnectionResetError:
            return False
        except Exception as e:
            log.warning("SW WS send failed: %s", e)
            return False

    bridge.attach_sw(sender)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                kind = payload.get("kind")
                if kind == "response":
                    bridge.deliver_sw_response(payload)
                elif kind == "notification":
                    # SW → server notification (V0 暂不用; future: tab_changed
                    # / mascot_clicked / V 主动暗示 trigger proactive review).
                    log.debug("SW notification: %s", payload.get("action"))
                # 其他 kind 忽略
            elif msg.type == WSMsgType.ERROR:
                log.warning("SW WS error: %s", ws.exception())
                break
    finally:
        # detach_sw_if 防 race: 如果 V reload 扩展或多窗口, 新 WS 会替换 sender.
        # 旧 WS 的 finally 跑 detach_sw 无脑清会清掉新 sender. 只在自己仍是当前才清.
        bridge.detach_sw_if(sender)
        log.info("SW WS disconnected")

    return ws


# ── app wiring ────────────────────────────────────────────────────────

async def _on_startup(app: web.Application) -> None:
    await bridge.start()
    log.info("sidebar bot ready on http://%s:%d", _SIDEBAR_HOST, _SIDEBAR_PORT)


async def _on_cleanup(app: web.Application) -> None:
    await bridge.stop()


def build_app() -> web.Application:
    app = web.Application()
    app.add_routes([
        web.get("/health", handle_health),
        web.get("/history", handle_history),
        web.get("/ws", handle_ws),
        web.post("/chat", handle_chat),
        web.post("/proactive", handle_proactive),
        web.post("/translate", handle_translate),
        web.post("/translate_trace", handle_translate_trace),
        web.post("/attention", handle_attention),
        web.options("/{tail:.*}", handle_options),
    ])
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


def main() -> None:
    app = build_app()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    runner = web.AppRunner(app)

    async def _run():
        await runner.setup()
        site = web.TCPSite(runner, _SIDEBAR_HOST, _SIDEBAR_PORT, reuse_port=True)
        await site.start()
        stop = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        await stop.wait()
        await runner.cleanup()

    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
