"""Unix socket bridge for sidebar MCP actions.

Mirror of bridge.py / weixin_bridge.py. sidebar_mcp spawns inside CC's process
tree and relays tool calls here; this dispatcher fans them out to the
connected browser extension SW (single live WebSocket maintained by
sidebar_bot) and waits for the SW reply.

V0 哲学: bridge 不知道 action 名字含义, 只做 IPC 路由. ping (本地存活) 和
notify_sw (server → SW 单向) 是 bridge 自带; 其他 action 一律走 SW round-
trip — SW 决定怎么 dispatch (raw DOM primitive). LLM 在 server 端 reason 出
具体 action 名字, bridge 透传.
"""

import asyncio
import json
import logging
import os
import uuid
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)

SOCKET_PATH = "/tmp/babata-sidebar-bridge.sock"

# bridge → SW WS sender. sidebar_bot 在 WS 接入时 set; SW 掉线时 clear.
SwSender = Callable[[dict[str, Any]], Awaitable[bool]]


class SidebarBridge:
    """Unix socket server. MCP tool spawns dial in over SOCKET_PATH; bridge
    correlates with SW WebSocket via per-request id and returns the SW reply.
    """

    def __init__(self) -> None:
        self._server: asyncio.Server | None = None
        self._sw_send: SwSender | None = None
        # request id → Future(result_dict). SW response 用 id 找回 future.
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        # 防 SW 同 id 重复 response 时 future.set_result 二次抛.
        self._lock = asyncio.Lock()

    # ── SW WS lifecycle (sidebar_bot 调用) ───────────────────────────

    def attach_sw(self, sender: SwSender) -> None:
        """sidebar_bot 在 WS 接入时调用. sender 把任意 dict JSON serialize 后
        write 到 WS, 返 True/False 表是否成功."""
        if self._sw_send is not None:
            log.info("sidebar bridge: replacing existing SW sender")
        self._sw_send = sender

    def detach_sw(self) -> None:
        self._sw_send = None
        # 把所有 pending future 标 SW disconnected, MCP 调方拿到 error 而不是干等.
        for fut in self._pending.values():
            if not fut.done():
                fut.set_result({"ok": False, "error": "SW disconnected"})
        self._pending.clear()

    def detach_sw_if(self, expected: SwSender) -> None:
        """只在当前 sender 仍是 expected 时才 detach.

        Race: WS A 关闭走 finally 时, WS B 可能已 attach (V 多窗口或 reload 扩展).
        无脑 detach_sw 会清掉 B 的 sender 和 B 新发的 pending future.
        """
        if self._sw_send is not expected:
            return
        self.detach_sw()

    @property
    def sw_attached(self) -> bool:
        return self._sw_send is not None

    # ── SW message handlers (sidebar_bot WS handler 调) ──────────────

    def deliver_sw_response(self, payload: dict[str, Any]) -> None:
        """SW 发来的 {kind:'response', id, ok, result, error?} → resolve future."""
        rid = payload.get("id")
        if not isinstance(rid, str):
            return
        fut = self._pending.pop(rid, None)
        if fut is None:
            log.debug("sidebar bridge: stray response id=%s", rid)
            return
        if not fut.done():
            fut.set_result(payload)

    # ── Outbound (bridge → SW) ───────────────────────────────────────

    async def request_sw(
        self,
        action: str,
        args: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Send request to SW, await response. Returns the raw {ok, result, error} dict.

        Caller (MCP relay) decides how to surface error vs result to LLM.
        """
        sender = self._sw_send
        if sender is None:
            return {"ok": False, "error": "SW not attached (browser extension not connected)"}
        rid = str(uuid.uuid4())
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
        async with self._lock:
            self._pending[rid] = fut

        try:
            ok = await sender({
                "kind": "request",
                "id": rid,
                "action": action,
                "args": args or {},
            })
            if not ok:
                return {"ok": False, "error": "SW send failed"}

            try:
                return await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                return {"ok": False, "error": f"SW timeout after {timeout}s"}
        finally:
            # 不管 caller 取消 / send 失败 / timeout / 正常 done, 都 cleanup.
            self._pending.pop(rid, None)

    async def notify_sw(self, action: str, args: dict[str, Any] | None = None) -> bool:
        """Server → SW one-way notification (suggest_prompts / clear_suggestions
        / mascot_speak 等). SW 转 sidepanel / mascot. 不等回复."""
        sender = self._sw_send
        if sender is None:
            return False
        return await sender({
            "kind": "notification",
            "action": action,
            "args": args or {},
        })

    # ── Unix socket server (MCP 入站) ────────────────────────────────

    async def start(self) -> None:
        try:
            os.unlink(SOCKET_PATH)
        except FileNotFoundError:
            pass
        self._server = await asyncio.start_unix_server(
            self._handle_connection, path=SOCKET_PATH
        )
        os.chmod(SOCKET_PATH, 0o600)
        log.info("sidebar bridge listening at %s", SOCKET_PATH)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        try:
            os.unlink(SOCKET_PATH)
        except FileNotFoundError:
            pass

    async def _handle_connection(self, reader, writer) -> None:
        try:
            data = await asyncio.wait_for(reader.readline(), timeout=10)
            request = json.loads(data.decode())
            action = request.get("action", "")

            # ping = 本地存活探测, 不打扰 SW.
            if action == "ping":
                await self._respond(writer, {"ok": True, "result": "pong"})
                return

            # notify_sw = MCP tool 想 push 到 SW (suggest_prompts / mascot_speak).
            # bridge 直接 forward, 不等 reply.
            if action == "notify_sw":
                inner_action = request.get("name") or ""
                inner_args = request.get("args") or {}
                if not inner_action:
                    await self._respond(writer, {"ok": False, "error": "notify_sw: missing 'name'"})
                    return
                ok = await self.notify_sw(inner_action, inner_args)
                await self._respond(writer, {"ok": ok, "result": "queued" if ok else "SW not attached"})
                return

            # 其他 action → forward 到 SW round-trip.
            timeout = float(request.get("timeout", 30.0))
            args = request.get("args") or {}
            sw_payload = await self.request_sw(action, args, timeout=timeout)
            await self._respond(writer, sw_payload)
        except Exception as e:
            log.warning("sidebar bridge error: %s", e)
            try:
                await self._respond(writer, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            except Exception:
                pass
        finally:
            writer.close()

    async def _respond(self, writer, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, ensure_ascii=False).encode() + b"\n"
        writer.write(line)
        await writer.drain()


bridge = SidebarBridge()
