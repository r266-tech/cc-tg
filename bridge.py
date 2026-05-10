"""Bridge between MCP tool server and TG bot via Unix socket.

The MCP server (tg_mcp.py) runs as a subprocess of CC CLI. It sends
action requests here; we execute them against the bot and return results.
Actions: buttons (waits for click), send_file / send_album / send_location (immediate).
"""

import asyncio
import json
import logging
import os
from pathlib import Path

from constants import BRIDGE_SOCKET, PROJECT
from tg_transcript import transcript_source

log = logging.getLogger(__name__)

# Per-instance socket path. bot.py pre-sets BABATA_BRIDGE_SOCKET before
# importing this module (plus constants.py reads it), and the tg_mcp
# subprocess inherits the same via mcp_servers["tg"]["env"]. Fall back to
# the derived namespace-based default when not pre-set (terminal CC case).
SOCKET_PATH = BRIDGE_SOCKET

# Telegram caption limit, measured in UTF-16 code units (TG wire unit).
# Borrows openclaw `caption.ts:1-15` `splitTelegramCaption` design: when
# caption exceeds the limit, send the media without caption and ship the
# full text as a separate follow-up message — never silently truncate.
# Codex round-1 fix: codepoint count (`len()`) underestimates by 2x for
# emoji/CJK Extension B / surrogate-pair chars; use UTF-16 to match TG's
# actual rejection threshold ("MEDIA_CAPTION_TOO_LONG").
_TG_MAX_CAPTION = 1024


_TG_MAX_MESSAGE = 4096  # Telegram per-message limit (UTF-16 units)


def _utf16_len(s: str) -> int:
    """UTF-16 code-unit count (TG's wire unit). Mirrors bot._utf16_len."""
    return len(s.encode("utf-16-le")) // 2


def _chunk_for_message(text: str, limit: int = _TG_MAX_MESSAGE - 96) -> list[str]:
    """Split a long text into TG-message-sized chunks (UTF-16 budget).

    Codex round-2 fix Q5: caption overflow text was passed unchunked to
    `bot.send_message`. A caption with > 4096 UTF-16 units (e.g. dense CJK
    user instructions) would land caption-less media + a failed send →
    partial side-effect with no recovery. Chunk before send. Pessimistic
    96-unit slack matches bot.py `_MAX_TG=4000` so chunks survive any
    HTML expansion downstream (none here, but stays consistent).
    """
    if _utf16_len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while remaining:
        if _utf16_len(remaining) <= limit:
            chunks.append(remaining)
            break
        # Binary-search the largest codepoint prefix fitting the budget.
        lo, hi = 1, len(remaining)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if _utf16_len(remaining[:mid]) <= limit:
                lo = mid
            else:
                hi = mid - 1
        cut = lo
        # Prefer newline > space boundary within budget.
        nl = remaining.rfind("\n", 0, cut)
        if nl > cut // 2:
            cut = nl
        else:
            sp = remaining.rfind(" ", 0, cut)
            if sp > cut // 2:
                cut = sp
        chunks.append(remaining[:cut])
        remaining = remaining[cut:].lstrip()
    return chunks


def _split_caption(text: str | None) -> tuple[str | None, str | None]:
    """Return (caption, follow_up_text). caption goes on the media; follow_up
    is sent as one or more separate text messages after the media when the
    original text exceeds the TG caption limit. Either or both may be None.
    """
    if not text:
        return None, None
    trimmed = text.strip()
    if not trimmed:
        return None, None
    if _utf16_len(trimmed) > _TG_MAX_CAPTION:
        return None, trimmed
    return trimmed, None


class TGBridge:
    """Unix socket server dispatching MCP actions to the TG bot."""

    def __init__(self) -> None:
        self.bot = None
        self.chat_id = None
        self.reply_to = None
        self._pending: dict[int, asyncio.Future] = {}
        self._server: asyncio.Server | None = None

    def set_context(self, bot, chat_id: int, reply_to: int | None = None) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self.reply_to = reply_to

    async def start(self) -> None:
        try:
            os.unlink(SOCKET_PATH)
        except FileNotFoundError:
            pass
        self._server = await asyncio.start_unix_server(
            self._handle_connection, path=SOCKET_PATH
        )
        log.info("Bridge socket listening at %s", SOCKET_PATH)

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
            action = request.get("action", "buttons")

            if not self.bot or not self.chat_id:
                await self._respond(writer, "Error: no TG context")
                return

            handlers = {
                "buttons": self._handle_buttons,
                "send_text": self._handle_send_text,
                "send_file": self._handle_send_file,
                "send_album": self._handle_send_album,
                "send_location": self._handle_send_location,
                "send_voice": self._handle_send_voice,
                "send_video": self._handle_send_video,
                "send_page": self._handle_send_page,
            }
            handler = handlers.get(action)
            if not handler:
                await self._respond(writer, f"Unknown action: {action}")
                return

            with transcript_source(f"bridge:{action}"):
                await handler(request, writer)

        except Exception as e:
            log.warning("Bridge error: %s", e)
            try:
                await self._respond(writer, f"Error: {e}")
            except Exception:
                pass
        finally:
            writer.close()

    async def _respond(self, writer, result: str) -> None:
        writer.write(json.dumps({"result": result}).encode() + b"\n")
        await writer.drain()

    async def _handle_buttons(self, request, writer) -> None:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        text = request["text"]
        options = request["options"]

        buttons = []
        has_callback = False
        for i, opt in enumerate(options):
            if isinstance(opt, dict):
                label = opt.get("label", str(i))
                url = opt.get("url")
            else:
                label = str(opt)
                url = None
            if url:
                buttons.append(InlineKeyboardButton(label, url=url))
            else:
                buttons.append(InlineKeyboardButton(label, callback_data=f"mcp:{i}:{label[:32]}"))
                has_callback = True

        keyboard = InlineKeyboardMarkup([[b] for b in buttons])
        msg = await self.bot.send_message(
            chat_id=self.chat_id, text=text, reply_markup=keyboard,
            reply_to_message_id=self.reply_to,
        )

        if not has_callback:
            await self._respond(writer, "Links sent")
            return

        future: asyncio.Future[str] = asyncio.get_event_loop().create_future()
        self._pending[msg.message_id] = future
        try:
            choice = await asyncio.wait_for(future, timeout=300)
        except asyncio.TimeoutError:
            self._pending.pop(msg.message_id, None)
            choice = "timeout"
            try:
                await msg.edit_text(f"{text}\n\n(expired)")
            except Exception:
                pass
        await self._respond(writer, choice)

    async def _handle_send_text(self, request, writer) -> None:
        chunks = _chunk_for_message(request["text"])
        for chunk in chunks:
            await self.bot.send_message(
                chat_id=self.chat_id, text=chunk,
                reply_to_message_id=self.reply_to,
            )
        suffix = f" ({len(chunks)} chunks)" if len(chunks) > 1 else ""
        await self._respond(writer, f"Text sent{suffix}")

    async def _handle_send_file(self, request, writer) -> None:
        path = Path(request["path"]).expanduser()
        if not path.exists():
            await self._respond(writer, f"Error: file not found: {path}")
            return
        caption, follow_up = _split_caption(request.get("caption"))
        with path.open("rb") as f:
            sent = await self.bot.send_document(
                chat_id=self.chat_id, document=f,
                filename=path.name, caption=caption,
                reply_to_message_id=self.reply_to,
            )
        if follow_up:
            # Thread follow-up under the media itself, not the original
            # anchor — keeps text visually paired with the file (Codex
            # round-1 fix 7). Chunk to respect TG 4096-unit message limit
            # (Codex round-2 fix Q5).
            anchor = sent.message_id if sent else self.reply_to
            for chunk in _chunk_for_message(follow_up):
                await self.bot.send_message(
                    chat_id=self.chat_id, text=chunk,
                    reply_to_message_id=anchor,
                )
        await self._respond(writer, f"Sent: {path.name}")

    async def _handle_send_album(self, request, writer) -> None:
        from telegram import InputMediaPhoto

        paths = [Path(p).expanduser() for p in request["paths"]]
        missing = [str(p) for p in paths if not p.exists()]
        if missing:
            await self._respond(writer, f"Error: not found: {missing}")
            return
        caption, follow_up = _split_caption(request.get("caption"))
        handles = [p.open("rb") for p in paths]
        try:
            media = [
                InputMediaPhoto(media=f, caption=caption if i == 0 else None)
                for i, f in enumerate(handles)
            ]
            sent_messages = await self.bot.send_media_group(
                chat_id=self.chat_id, media=media,
                reply_to_message_id=self.reply_to,
            )
        finally:
            for f in handles:
                f.close()
        if follow_up:
            # send_media_group returns a tuple of Messages; reply follow-up
            # to the FIRST photo so the text appears threaded under the
            # album in TG (Codex round-1 fix 7). Chunk to respect TG 4096
            # (Codex round-2 fix Q5).
            anchor = (
                sent_messages[0].message_id
                if sent_messages else self.reply_to
            )
            for chunk in _chunk_for_message(follow_up):
                await self.bot.send_message(
                    chat_id=self.chat_id, text=chunk,
                    reply_to_message_id=anchor,
                )
        await self._respond(writer, f"Sent {len(paths)} images")

    async def _handle_send_location(self, request, writer) -> None:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        from urllib.parse import quote

        lat = request["latitude"]
        lon = request["longitude"]
        name = request.get("name") or ""

        provider = os.environ.get("MAP_PROVIDER", "amap").lower()
        keyboard = None
        if provider == "amap":
            url = f"https://uri.amap.com/marker?position={lon},{lat}"
            if name:
                url += f"&name={quote(name)}"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f5fa\ufe0f 高德打开", url=url)],
            ])
        elif provider == "google":
            url = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f5fa\ufe0f Google Maps", url=url)],
            ])
        elif provider == "osm":
            url = f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=17/{lat}/{lon}"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("\U0001f5fa\ufe0f OpenStreetMap", url=url)],
            ])

        await self.bot.send_location(
            chat_id=self.chat_id,
            latitude=lat, longitude=lon,
            reply_to_message_id=self.reply_to,
            reply_markup=keyboard,
        )
        await self._respond(writer, "Location sent")

    async def _handle_send_video(self, request, writer) -> None:
        path = Path(request["path"]).expanduser()
        if not path.exists():
            await self._respond(writer, f"Error: file not found: {path}")
            return
        caption, follow_up = _split_caption(request.get("caption"))
        with path.open("rb") as f:
            sent = await self.bot.send_video(
                chat_id=self.chat_id, video=f,
                filename=path.name, caption=caption,
                reply_to_message_id=self.reply_to,
            )
        if follow_up:
            # Thread follow-up under the video (Codex round-1 fix 7). Chunk
            # to respect TG 4096 (Codex round-2 fix Q5).
            anchor = sent.message_id if sent else self.reply_to
            for chunk in _chunk_for_message(follow_up):
                await self.bot.send_message(
                    chat_id=self.chat_id, text=chunk,
                    reply_to_message_id=anchor,
                )
        await self._respond(writer, f"Video sent: {path.name}")

    async def _handle_send_page(self, request, writer) -> None:
        """Publish markdown as a Telegraph page and push URL to TG.

        The TG client auto-generates an Instant View preview for telegra.ph
        URLs, giving full rich-layout rendering (h3/h4 / real lists /
        blockquote / highlighted code) that TG inline HTML can't.
        """
        from telegraph import create_page

        title = request.get("title", PROJECT) or PROJECT
        content_md = request["content_md"]
        caption = (request.get("caption") or "").strip()

        # Telegraph HTTP calls are sync; offload to executor so the event loop
        # keeps serving other bridge actions and TG updates.
        loop = asyncio.get_event_loop()
        try:
            url = await loop.run_in_executor(None, create_page, title, content_md)
        except Exception as e:
            await self._respond(writer, f"Error creating page: {e}")
            return

        # Compose TG message. Keep it short — the Instant View card will carry
        # the full content; a long caption would just compete with the preview.
        text = f"{caption}\n\n{url}" if caption else url
        await self.bot.send_message(
            chat_id=self.chat_id, text=text,
            reply_to_message_id=self.reply_to,
        )
        await self._respond(writer, f"Page: {url}")

    async def _handle_send_voice(self, request, writer) -> None:
        from media import text_to_voice
        voice = request.get("voice") or None  # let media.py pick backend-appropriate default
        ogg = await text_to_voice(request["text"], voice=voice)
        if not ogg:
            await self._respond(writer, "Error: TTS failed")
            return
        try:
            with ogg.open("rb") as f:
                await self.bot.send_voice(
                    chat_id=self.chat_id, voice=f,
                    reply_to_message_id=self.reply_to,
                )
        finally:
            ogg.unlink(missing_ok=True)
        await self._respond(writer, "Voice sent")

    def resolve(self, msg_id: int, option_index: int, options_label: str) -> bool:
        """Called by TG callback handler when user clicks a button."""
        future = self._pending.pop(msg_id, None)
        if not future or future.done():
            return False
        future.set_result(options_label)
        return True


bridge = TGBridge()
