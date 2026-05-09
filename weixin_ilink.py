"""Weixin iLink bot protocol — HTTP client + QR login + CDN AES.

Raw protocol layer. No state persistence, no business logic; caller owns
token / sync_buf / context_tokens / account index storage. Port of
@tencent-weixin/openclaw-weixin (MIT) src/api/api.ts + src/auth/login-qr.ts
+ src/cdn/upload.ts + aes-ecb.ts.
"""

import asyncio
import base64
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
import secrets
from secrets import token_bytes
from typing import Any
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7

log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────

DEFAULT_BASE_URL = os.environ.get("WEIXIN_BASE_URL", "https://ilinkai.weixin.qq.com")
DEFAULT_CDN_BASE_URL = os.environ.get(
    "WEIXIN_CDN_BASE_URL",
    "https://novac2c.cdn.weixin.qq.com/c2c",
)
ILINK_APP_ID = os.environ.get("WEIXIN_ILINK_APP_ID", "bot")

# Mirror the upstream plugin version so the server-side client-version gate
# behaves the same. Bump when the iLink protocol changes materially.
CLIENT_VERSION_STR = "2.1.8"  # mirror npm openclaw-weixin; "1.0.0" 直接 CDN 404
CLIENT_VERSION_INT = (
    ((int(CLIENT_VERSION_STR.split(".")[0]) & 0xFF) << 16)
    | ((int(CLIENT_VERSION_STR.split(".")[1]) & 0xFF) << 8)
    | (int(CLIENT_VERSION_STR.split(".")[2]) & 0xFF)
)

SESSION_EXPIRED_ERRCODE = -14
SESSION_PAUSE_SECONDS = 3600

DEFAULT_LONG_POLL_MS = 35_000
DEFAULT_API_MS = 15_000
DEFAULT_CONFIG_MS = 10_000

QR_POLL_INTERVAL_S = 1.0
QR_POLL_TIMEOUT_S = 35
QR_MAX_REFRESH = 3
LOGIN_TOTAL_TIMEOUT_S = 480

# Upload media_type values (proto: UploadMediaType).
MEDIA_IMAGE = 1
MEDIA_VIDEO = 2
MEDIA_FILE = 3
MEDIA_VOICE = 4

# Inbound MessageItem type values (proto: MessageItemType).
ITEM_TEXT = 1
ITEM_IMAGE = 2
ITEM_VOICE = 3
ITEM_FILE = 4
ITEM_VIDEO = 5


class WeixinSessionExpired(Exception):
    """Server errcode=-14: bot re-login required. Account paused 1h."""


class WeixinApiError(Exception):
    """Non-zero ret or HTTP error."""


# ── header helpers ────────────────────────────────────────────────────


def _random_wechat_uin() -> str:
    """X-WECHAT-UIN: random uint32 as decimal string, then base64."""
    n = int.from_bytes(token_bytes(4), "big")
    return base64.b64encode(str(n).encode()).decode()


def _common_headers() -> dict[str, str]:
    return {
        "iLink-App-Id": ILINK_APP_ID,
        "iLink-App-ClientVersion": str(CLIENT_VERSION_INT),
    }


# ── AES-128-ECB (CDN media crypto) ────────────────────────────────────


def aes_ecb_padded_size(rawsize: int) -> int:
    """PKCS7 pads to next 16-byte boundary; full block added when already aligned."""
    return ((rawsize // 16) + 1) * 16


def encrypt_aes_ecb(plaintext: bytes, key: bytes) -> bytes:
    if len(key) != 16:
        raise ValueError(f"AES-128 key must be 16 bytes, got {len(key)}")
    padder = PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    enc = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    return enc.update(padded) + enc.finalize()


def decrypt_aes_ecb(ciphertext: bytes, key: bytes) -> bytes:
    if len(key) != 16:
        raise ValueError(f"AES-128 key must be 16 bytes, got {len(key)}")
    dec = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
    padded = dec.update(ciphertext) + dec.finalize()
    unpadder = PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


def parse_inbound_aes_key(raw: str | None) -> bytes | None:
    """Decode inbound AES key from one of 3 protocol shapes.

    The protocol emits keys in inconsistent encodings across media types:
    - base64 of raw 16 bytes          → decode base64, 16 bytes
    - base64 of 32-char hex string    → decode base64, then fromhex
    - raw 32-char hex string          → fromhex directly
    Try in order; return 16-byte key or None.
    """
    if not raw:
        return None
    try:
        decoded = base64.b64decode(raw)
        if len(decoded) == 16:
            return decoded
        try:
            s = decoded.decode("ascii")
            if len(s) == 32:
                return bytes.fromhex(s)
        except Exception:
            pass
    except Exception:
        pass
    if len(raw) == 32:
        try:
            return bytes.fromhex(raw)
        except ValueError:
            pass
    return None


def encode_outbound_aes_key(key: bytes) -> str:
    """CDNMedia.aes_key = base64 of hex-string (not base64 of raw bytes)."""
    return base64.b64encode(key.hex().encode()).decode()


# ── QR login (stateless; no client/token needed yet) ──────────────────


@dataclass
class QrStart:
    qrcode: str            # opaque token used to poll status
    qrcode_url: str        # URL to encode in terminal QR
    raw: dict[str, Any]    # whole server response, for caller inspection


@dataclass
class LoginResult:
    connected: bool
    message: str
    account_id: str | None = None
    bot_token: str | None = None
    base_url: str | None = None
    user_id: str | None = None


async def start_qr_login(*, base_url: str = DEFAULT_BASE_URL, bot_type: int = 3) -> QrStart:
    """Fetch a fresh QR code. No client-side timeout — the server may hold briefly."""
    url = f"{base_url.rstrip('/')}/ilink/bot/get_bot_qrcode?bot_type={bot_type}"
    async with httpx.AsyncClient(timeout=None) as c:
        r = await c.get(url, headers=_common_headers())
    if r.status_code != 200:
        raise WeixinApiError(f"get_bot_qrcode HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    qrcode = data.get("qrcode") or ""
    qrcode_url = data.get("qrcode_img_content") or data.get("qrcode_url") or ""
    if not qrcode:
        raise WeixinApiError(f"get_bot_qrcode: missing qrcode field; resp={data}")
    return QrStart(qrcode=qrcode, qrcode_url=qrcode_url, raw=data)


async def wait_qr_login(
    qr: QrStart,
    *,
    base_url: str = DEFAULT_BASE_URL,
    total_timeout_s: float = LOGIN_TOTAL_TIMEOUT_S,
    on_refresh: "callable | None" = None,
) -> LoginResult:
    """Poll QR status until confirmed / expired / timeout.

    Handles: `wait` (no-op), `scaned` (user scanned, waiting to confirm),
    `scaned_but_redirect` (IDC switch), `expired` (auto-refresh up to
    QR_MAX_REFRESH times, calling on_refresh(new_qr) so caller can rerender),
    `confirmed` (success).
    """
    deadline = time.time() + total_timeout_s
    host = base_url.rstrip("/")
    current = qr
    refreshes = 0
    seen_scanned = False

    while time.time() < deadline:
        url = f"{host}/ilink/bot/get_qrcode_status?qrcode={current.qrcode}"
        try:
            async with httpx.AsyncClient(timeout=QR_POLL_TIMEOUT_S) as c:
                r = await c.get(url, headers=_common_headers())
        except httpx.TimeoutException:
            continue
        except Exception as e:
            log.warning("qr poll error: %s", e)
            await asyncio.sleep(QR_POLL_INTERVAL_S)
            continue

        if r.status_code != 200:
            await asyncio.sleep(QR_POLL_INTERVAL_S)
            continue

        data = r.json()
        status = data.get("status")

        if status == "confirmed":
            return LoginResult(
                connected=True,
                message="登录成功",
                account_id=data.get("ilink_bot_id"),
                bot_token=data.get("bot_token"),
                base_url=(data.get("baseurl") or host),
                user_id=data.get("ilink_user_id"),
            )

        if status == "scaned_but_redirect":
            new_host = (data.get("redirect_host") or "").rstrip("/")
            if new_host and new_host != host:
                log.info("qr IDC redirect: %s → %s", host, new_host)
                host = new_host
            await asyncio.sleep(QR_POLL_INTERVAL_S)
            continue

        if status == "expired":
            if refreshes >= QR_MAX_REFRESH:
                return LoginResult(connected=False, message="二维码过期超过 3 次")
            refreshes += 1
            current = await start_qr_login(base_url=host)
            seen_scanned = False
            if on_refresh:
                try:
                    on_refresh(current)
                except Exception as e:
                    log.warning("on_refresh callback raised: %s", e)
            continue

        if status == "scaned" and not seen_scanned:
            seen_scanned = True
            log.info("qr: scanned, waiting for user confirmation")

        await asyncio.sleep(QR_POLL_INTERVAL_S)

    return LoginResult(connected=False, message="登录超时 (8min)")


def normalize_account_id(raw: str) -> str:
    """`b0f5860fdecb@im.bot` → `b0f5860fdecb-im-bot` (filesystem-safe)."""
    return raw.replace("@", "-").replace(".", "-").lower()


# ── stateful client (one instance = one logged-in account) ────────────


class WeixinClient:
    """iLink HTTP client for one account. Caller persists token / sync_buf.

    Session-pause state is in-memory only (mirrors TS plugin): on restart,
    the pause forgets itself, and the first getUpdates re-triggers -14 if
    the server still considers the session dead.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        account_id: str = "",
        cdn_base_url: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.account_id = account_id
        self.cdn_base_url = (cdn_base_url or DEFAULT_CDN_BASE_URL).rstrip("/")
        self._paused_until: float = 0.0

    # ── session pause ────────────────────────────────────────────────

    def _assert_active(self) -> None:
        if self._paused_until > time.time():
            remain_min = int((self._paused_until - time.time()) / 60) + 1
            raise WeixinSessionExpired(
                f"session paused for {self.account_id}, {remain_min} min remaining"
            )

    def _pause(self) -> None:
        self._paused_until = time.time() + SESSION_PAUSE_SECONDS
        log.warning("session paused 1h for accountId=%s", self.account_id)

    @property
    def is_paused(self) -> bool:
        return self._paused_until > time.time()

    # ── core POST ────────────────────────────────────────────────────

    async def _post(
        self,
        endpoint: str,
        body: dict[str, Any],
        *,
        timeout_ms: int,
        allow_timeout: bool = False,
    ) -> dict[str, Any]:
        self._assert_active()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        body = {**body, "base_info": {"channel_version": CLIENT_VERSION_STR}}
        headers = {
            "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token",
            "Authorization": f"Bearer {self.token}",
            "X-WECHAT-UIN": _random_wechat_uin(),
            **_common_headers(),
        }
        try:
            async with httpx.AsyncClient(timeout=timeout_ms / 1000) as c:
                r = await c.post(url, headers=headers, json=body)
        except httpx.TimeoutException:
            if allow_timeout:
                return {}
            raise
        if r.status_code != 200:
            raise WeixinApiError(f"{endpoint} HTTP {r.status_code}: {r.text[:300]}")
        data: dict[str, Any] = r.json()
        if data.get("errcode") == SESSION_EXPIRED_ERRCODE:
            self._pause()
            raise WeixinSessionExpired(f"{endpoint} errcode=-14: {data.get('errmsg')}")
        return data

    # ── API endpoints ────────────────────────────────────────────────

    async def get_updates(self, get_updates_buf: str = "") -> dict[str, Any]:
        """Long-poll (35s). Client-side timeout returns empty for caller retry."""
        data = await self._post(
            "ilink/bot/getupdates",
            {"get_updates_buf": get_updates_buf},
            timeout_ms=DEFAULT_LONG_POLL_MS,
            allow_timeout=True,
        )
        if not data:
            return {"ret": 0, "msgs": [], "get_updates_buf": get_updates_buf}
        return data

    async def send_message(
        self,
        to_user_id: str,
        item_list: list[dict[str, Any]],
        *,
        context_token: str | None = None,
    ) -> None:
        """Send one message. `item_list` is 1+ MessageItem dicts.

        Every call auto-generates a fresh `client_id` and carries an empty
        `from_user_id` — the official plugin's sendmessage envelope requires
        both fields (see @tencent-weixin/openclaw-weixin src/messaging/send.ts
        buildTextMessageReq / sendMediaItems). Without them the server returns
        HTTP 200 + {} but silently drops the message.

        Caller builds each item, e.g.
            {"type": 1, "text_item": {"text": "hi"}}
            {"type": 2, "image_item": {"media": {...}, "mid_size": N}}
        """
        client_id = f"babata-weixin-{secrets.token_hex(12)}"
        msg: dict[str, Any] = {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id,
            "message_type": 2,    # BOT
            "message_state": 2,   # FINISH
            "item_list": item_list,
        }
        if context_token:
            msg["context_token"] = context_token
        resp = await self._post(
            "ilink/bot/sendmessage",
            {"msg": msg},
            timeout_ms=DEFAULT_API_MS,
        )
        log.info(
            "sendmessage resp: to=%s clientId=%s types=%s resp=%s",
            to_user_id,
            client_id,
            [it.get("type") for it in item_list],
            resp,
        )

    async def get_upload_url(
        self,
        *,
        filekey: str,
        media_type: int,
        to_user_id: str,
        rawsize: int,
        rawfilemd5: str,
        filesize: int,
        aeskey_hex: str,
        no_need_thumb: bool = True,
        thumb_rawsize: int | None = None,
        thumb_rawfilemd5: str | None = None,
        thumb_filesize: int | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "filekey": filekey,
            "media_type": media_type,
            "to_user_id": to_user_id,
            "rawsize": rawsize,
            "rawfilemd5": rawfilemd5,
            "filesize": filesize,
            "aeskey": aeskey_hex,
            "no_need_thumb": no_need_thumb,
        }
        if thumb_rawsize is not None:
            body["thumb_rawsize"] = thumb_rawsize
            body["thumb_rawfilemd5"] = thumb_rawfilemd5
            body["thumb_filesize"] = thumb_filesize
        return await self._post(
            "ilink/bot/getuploadurl", body, timeout_ms=DEFAULT_API_MS,
        )

    async def get_config(
        self, ilink_user_id: str, context_token: str | None = None
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"ilink_user_id": ilink_user_id}
        if context_token:
            body["context_token"] = context_token
        return await self._post(
            "ilink/bot/getconfig", body, timeout_ms=DEFAULT_CONFIG_MS,
        )

    async def send_typing(
        self, ilink_user_id: str, typing_ticket: str, status: int = 1
    ) -> None:
        """status: 1=typing, 2=cancel. Ticket from get_config()."""
        await self._post(
            "ilink/bot/sendtyping",
            {
                "ilink_user_id": ilink_user_id,
                "typing_ticket": typing_ticket,
                "status": status,
            },
            timeout_ms=DEFAULT_CONFIG_MS,
        )

    # ── CDN upload (end-to-end: encrypt → upload → return ref) ──────

    async def upload_media(
        self,
        data: bytes,
        *,
        to_user_id: str,
        media_type: int,
    ) -> dict[str, Any]:
        """Upload one buffer to Weixin CDN. Returns fields ready for send_message item.

        Output dict:
            encrypt_query_param: str   # put into CDNMedia.encrypt_query_param
            aes_key: str               # base64(hex) — put into CDNMedia.aes_key
            full_url: str              # CDN download URL for clients that need it
            raw_size: int              # plaintext bytes
            cipher_size: int           # ciphertext bytes
            md5: str                   # plaintext hex md5
            filekey: str
        """
        key = token_bytes(16)
        filekey = token_bytes(16).hex()
        ciphertext = encrypt_aes_ecb(data, key)
        rawsize = len(data)
        filesize = len(ciphertext)
        md5 = hashlib.md5(data).hexdigest()

        resp = await self.get_upload_url(
            filekey=filekey,
            media_type=media_type,
            to_user_id=to_user_id,
            rawsize=rawsize,
            rawfilemd5=md5,
            filesize=filesize,
            aeskey_hex=key.hex(),
            no_need_thumb=True,
        )

        upload_full_url = (resp.get("upload_full_url") or "").strip()
        upload_param = resp.get("upload_param")
        if upload_full_url:
            cdn_url = upload_full_url
        elif upload_param:
            # Older protocol path; cdnBaseUrl + upload_param + filekey. TS uses
            # buildCdnUploadUrl; we mirror the simplest join. If this breaks,
            # inspect @tencent-weixin/openclaw-weixin src/cdn/cdn-url.ts.
            cdn_url = f"{self.cdn_base_url}/{upload_param.lstrip('/')}?filekey={filekey}"
        else:
            raise WeixinApiError(f"get_upload_url returned no url: resp={resp}")

        download_param = await self._cdn_upload(cdn_url, ciphertext)

        return {
            "encrypt_query_param": download_param,
            "aes_key": encode_outbound_aes_key(key),
            "full_url": (
                f"{self.cdn_base_url}/download"
                f"?encrypted_query_param={quote(download_param, safe='')}"
            ),
            "raw_size": rawsize,
            "cipher_size": filesize,
            "md5": md5,
            "filekey": filekey,
        }

    async def _cdn_upload(self, url: str, ciphertext: bytes, max_retries: int = 3) -> str:
        """POST ciphertext to CDN. Returns x-encrypted-param header.

        4xx aborts immediately; 5xx / network errors retry up to 3 times.
        """
        last_err: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=120) as c:
                    r = await c.post(
                        url,
                        content=ciphertext,
                        headers={"Content-Type": "application/octet-stream"},
                    )
                if 400 <= r.status_code < 500:
                    msg = r.headers.get("x-error-message") or r.text[:200]
                    raise WeixinApiError(f"CDN client error {r.status_code}: {msg}")
                if r.status_code != 200:
                    msg = r.headers.get("x-error-message") or f"status {r.status_code}"
                    raise WeixinApiError(f"CDN server error: {msg}")
                download_param = r.headers.get("x-encrypted-param")
                if not download_param:
                    raise WeixinApiError("CDN response missing x-encrypted-param header")
                return download_param
            except WeixinApiError as e:
                if "client error" in str(e):
                    raise
                last_err = e
                log.warning("cdn upload attempt %d failed: %s", attempt, e)
            except Exception as e:
                last_err = e
                log.warning("cdn upload attempt %d failed: %s", attempt, e)
            if attempt < max_retries:
                await asyncio.sleep(1.0 * attempt)
        raise last_err or WeixinApiError(f"CDN upload failed after {max_retries} attempts")

    # ── CDN download (inbound media decryption) ─────────────────────

    async def download_media(
        self,
        cdn_media: dict[str, Any],
        *,
        aeskey_hex_override: str | None = None,
    ) -> bytes:
        """Download + decrypt inbound CDN media.

        `cdn_media` is the `media` dict from an inbound item (image_item.media
        etc.). `aeskey_hex_override` takes precedence when the parent item has
        its own `aeskey` field (ImageItem does; VoiceItem/FileItem/VideoItem
        use media.aes_key).
        """
        key: bytes | None = None
        if aeskey_hex_override:
            try:
                key = bytes.fromhex(aeskey_hex_override)
                if len(key) != 16:
                    key = None
            except ValueError:
                key = None
        if key is None:
            key = parse_inbound_aes_key(cdn_media.get("aes_key"))
        if key is None:
            raise WeixinApiError("download_media: could not parse aes_key")

        full_url = (cdn_media.get("full_url") or "").strip()
        if not full_url:
            qp = cdn_media.get("encrypt_query_param")
            if not qp:
                raise WeixinApiError("download_media: no full_url or encrypt_query_param")
            full_url = f"{self.cdn_base_url}/{qp.lstrip('/')}"

        async with httpx.AsyncClient(timeout=120) as c:
            r = await c.get(full_url)
            r.raise_for_status()
            ciphertext = r.content

        return decrypt_aes_ecb(ciphertext, key)


# ── item builders (convenience for send_message) ──────────────────────


def text_item(text: str) -> dict[str, Any]:
    return {"type": ITEM_TEXT, "text_item": {"text": text}}


def _media_ref(uploaded: dict[str, Any]) -> dict[str, Any]:
    return {
        "encrypt_query_param": uploaded["encrypt_query_param"],
        "aes_key": uploaded["aes_key"],
        "encrypt_type": 1,
    }


def image_item(uploaded: dict[str, Any]) -> dict[str, Any]:
    """uploaded = return value of WeixinClient.upload_media()."""
    return {
        "type": ITEM_IMAGE,
        "image_item": {
            "media": _media_ref(uploaded),
            "mid_size": uploaded["cipher_size"],
        },
    }


def video_item(uploaded: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": ITEM_VIDEO,
        "video_item": {
            "media": _media_ref(uploaded),
            "video_size": uploaded["cipher_size"],
        },
    }


def file_item(uploaded: dict[str, Any], file_name: str) -> dict[str, Any]:
    return {
        "type": ITEM_FILE,
        "file_item": {
            "media": _media_ref(uploaded),
            "file_name": file_name,
            "md5": uploaded["md5"],
            "len": str(uploaded["raw_size"]),
        },
    }


def voice_item(uploaded: dict[str, Any], *, playtime: int = 0, **_unused: Any) -> dict[str, Any]:
    """[NOT REACHABLE — see weixin_mcp.py / weixin_bridge.py 同名 verdict]

    2026-04-26 round 2 verdict 实证: ilink server silent filter bot→user voice.
    保留函数仅为 import 不破裂; wx_send_voice MCP tool 已下线, _handle_send_voice
    fail-fast 兜底, 这里不会被调到. 字段是 Java SDK live wire dump 实测值
    (encode_type=7, playtime, sample_rate=24000, media has 3 fields) — 即使将来
    协议放开重启探索, 这是已知最贴近的起点. 实证证据见
    `~/cc-workspace/memory/feedback_ilink_voice_send_blocked.md`.
    """
    media = _media_ref(uploaded)
    if uploaded.get("full_url"):
        media["full_url"] = uploaded["full_url"]
    return {
        "type": ITEM_VOICE,
        "voice_item": {
            "media": media,
            "encode_type": 7,
            "playtime": int(playtime or 0),
            "sample_rate": 24000,
        },
    }
