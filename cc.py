"""Thin wrapper around Claude Code SDK. One session, streaming.

Channel-agnostic: caller passes the channel-specific state file, source prompt,
and MCP servers. This lets the TG bot and the WeChat bot share one class while
keeping session state and exposed tools isolated per channel.
"""

import asyncio
import copy
import hashlib
import json
import logging
import os
import subprocess
import time
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Coroutine, Literal

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import (
    PermissionResultAllow,
    StreamEvent,
    ToolPermissionContext,
)

log = logging.getLogger(__name__)

# (tool_name, tool_input, text_chunk, tool_result) — exactly one non-None.
# tool_result = {"is_error": bool, "text": str} so bot can surface real errors
# instead of letting CC hallucinate high-level reasons ("系统限制了...").
# text_chunk is a *delta* (not a snapshot) — bot must accumulate it. Driven by
# StreamEvent.content_block_delta.text_delta (CLI --include-partial-messages).
StreamCB = Callable[
    [str | None, dict | None, str | None, dict | None],
    Coroutine[Any, Any, None],
]

from constants import (
    HOOKS_DIR as _HOOKS_DIR,
    INSTANCE,
    INSTANCE_LABELS,
    PROJECT,
    SKILL_HOOKS_DIR as _SKILL_HOOKS_DIR,
    STATE_DIR as _STATE_DIR,
)

VENV_PYTHON = str(Path(__file__).parent / ".venv" / "bin" / "python")

_CC_PROJECTS = Path.home() / ".claude" / "projects" / str(Path.home()).replace("/", "-")


def _find_jsonl_any_bucket(sid: str) -> Path | None:
    """Locate <sid>.jsonl across all ~/.claude/projects/<cwd-hash>/ buckets.

    babata 默认 cwd=$HOME, 单 bucket 看到的只是 V 在 $HOME 跑 claude 那批 session.
    V 在 ~/code/foo 跑 claude 的 session 落在 -Users-admin-code-foo/ 别的 bucket,
    list_recent_sessions(scan_all_buckets=True) 会列出来; 但实际 resume / 读 turn
    时还得跨 bucket 找文件 — 这里就做这个查找.

    Read-only — 不动文件. resume 路径要把文件 copy 到 babata bucket 才能让 SDK
    --resume 找到 (CLI cwd-bound), 走 _import_jsonl_to_bucket.
    """
    target = _CC_PROJECTS / f"{sid}.jsonl"
    if target.is_file():
        return target
    projects_root = _CC_PROJECTS.parent
    try:
        for bucket in projects_root.iterdir():
            if not bucket.is_dir() or bucket == _CC_PROJECTS:
                continue
            candidate = bucket / f"{sid}.jsonl"
            if candidate.is_file():
                return candidate
    except Exception:
        pass
    return None


def _import_jsonl_to_bucket(source_sid: str) -> str | None:
    """Resolve <source_sid>.jsonl to a sid that lives in babata's bucket.

    源文件已在 babata bucket → 直接返回 source_sid (no copy, no fork).
    源文件在别的 cwd-bucket (V 在终端别处开的原生 CC) → fork: 用 *新 uuid*
    复制到 babata bucket, 返回新 sid. 关键设计:
      - 新 uuid 避免 sid 冲突: 原 sid 在源 bucket 不动, V 在原 cwd 终端继续
        resume 同一个 sid 仍能续上, 跟 babata 这边的 fork 物理隔离 不会
        silent diverge (Codex A-FORK).
      - SDK --resume <new_sid> 能找到 (CLI cwd-bound, 文件在 babata bucket).
    并发安全:
      - per-source-sid flock 防多 babata 实例同时 import 写花文件 (Codex C-CONCURRENCY)
      - 写 .tmp + os.replace 原子化, 中途 crash 不留半文件
      - 末行 JSON 校验 — 源文件可能正被终端 session 追加 (Codex H-RACE),
        最后一行不完整就 trim 掉
    """
    own = _CC_PROJECTS / f"{source_sid}.jsonl"
    if own.is_file():
        return source_sid
    src = _find_jsonl_any_bucket(source_sid)
    if src is None:
        return None

    import fcntl
    import shutil
    import uuid

    new_sid = str(uuid.uuid4())
    new_target = _CC_PROJECTS / f"{new_sid}.jsonl"
    tmp_path = _CC_PROJECTS / f".{new_sid}.jsonl.tmp"
    lock_path = _CC_PROJECTS / f".import-{source_sid}.lock"
    lock_f = None
    try:
        _CC_PROJECTS.mkdir(parents=True, exist_ok=True)
        lock_f = open(lock_path, "w")
        fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        # Re-check after acquiring lock (another babata instance may have just
        # finished an unrelated import — irrelevant since we use a fresh uuid,
        # but cheap belt-and-suspenders).
        if own.is_file():
            return source_sid
        shutil.copy2(src, tmp_path)
        # 末行 JSON 校验. shutil.copy2 是单次 read+write — 若源 bucket 终端
        # session 正 append, 末行可能截断. 不能 parse 就 trim.
        try:
            content = tmp_path.read_bytes()
            if content and not content.endswith(b"\n"):
                # Last line not newline-terminated; treat as in-progress write.
                last_nl = content.rfind(b"\n")
                if last_nl >= 0:
                    tmp_path.write_bytes(content[: last_nl + 1])
                else:
                    # Single incomplete line — drop to empty rather than corrupt.
                    tmp_path.write_bytes(b"")
            else:
                # Validate last line parses as JSON; if not, trim it.
                lines = content.splitlines()
                if lines:
                    try:
                        json.loads(lines[-1])
                    except Exception:
                        tmp_path.write_bytes(b"\n".join(lines[:-1]) + (b"\n" if lines[:-1] else b""))
        except Exception:
            pass
        with open(tmp_path, "rb") as t:
            os.fsync(t.fileno())
        os.replace(tmp_path, new_target)
        return new_sid
    except Exception as e:
        log.warning("import jsonl %s from %s failed: %s", source_sid, src.parent.name, e)
        with suppress(Exception):
            tmp_path.unlink()
        return None
    finally:
        if lock_f is not None:
            with suppress(Exception):
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
            with suppress(Exception):
                lock_f.close()
        with suppress(Exception):
            lock_path.unlink()

# 默认隔离 — 不读用户 ~/.claude/{settings.json, CLAUDE.md, skills/}, 不动 OAuth keychain.
# 用户日常 CC 完全无感. 开源用户走这条 (.env 必须有 ANTHROPIC_API_KEY).
# V 私人 .env 设 BABATA_SHARED_CC=1 → 共享用户 CC 全套 (skill / settings / OAuth), 跟之前体验一致.
_SETTING_SOURCES: list[str] = ["user"] if os.environ.get("BABATA_SHARED_CC") == "1" else []

# 默认信任边界 — babata 容器化 default: cwd 限定 repo 自身 (不让 babata access 用户整个家),
# permission_mode 走 "default" (重要 tool call 提示授权, 不静默 bypass).
# V 私人 .env 设 BABATA_FULL_TRUST=1 → cwd=~/ + auto mode (官方支持, status "auto mode on").
_FULL_TRUST = os.environ.get("BABATA_FULL_TRUST") == "1"
_DEFAULT_CWD = str(Path.home()) if _FULL_TRUST else str(Path(__file__).parent)
_PERMISSION_MODE = "auto" if _FULL_TRUST else "default"

# _STATE_DIR: all channel state files land here. Cross-channel session picker
# scans this dir, so it's a soft coupling — cc.py isn't fully channel-agnostic
# anymore, but /resume can list sessions from every channel (TG / WX / terminal
# bb) in one picker, sorted by bucket mtime with non-local channels tagged.
# _SKILL_HOOKS_DIR: SDK doesn't fire settings.json SessionEnd/SessionStart
# hooks (those are CC CLI-native), so babata fires them explicitly on
# reset()/init so TG/WeChat channels can plug into skill evolution (v3).


def _channel_label_from_state_file(fp: Path) -> str:
    """state file name → human-readable channel label for the /resume picker.

    Labels come from constants.INSTANCE_LABELS (single source of truth). Stem
    format: ``<PROJECT>-session.json`` for main instance, ``<PROJECT>-<inst>-
    session.json`` for named instances. Unknown instances fall back to the raw
    suffix so new bots show up with a best-effort label instead of disappearing.

    Example (PROJECT=babata, default map):
        babata-session.json        → "巴巴塔"
        babata-vvv-session.json    → "巴巴塔2"
        babata-vvvv-session.json   → "巴巴塔3"
        babata-weixin-session.json → "wx"
    """
    core = fp.stem  # strip .json
    if core.endswith("-session"):
        core = core[: -len("-session")]
    if core == PROJECT:
        return INSTANCE_LABELS.get("", PROJECT)
    if core.startswith(f"{PROJECT}-"):
        suffix = core[len(f"{PROJECT}-"):]
        return INSTANCE_LABELS.get(suffix, suffix)
    return core or "unknown"


_SUMMARY_CACHE_FILE = _STATE_DIR / "session-summaries.json"

# Summary subprocess 的专用 CWD. claude -p 会按 CWD 把 session jsonl 写到对应
# bucket (~/.claude/projects/<cwd-hash>/); 如果 summary 进程跟 babata 日常用同
# 一个 CWD ($HOME), 它产生的 "用一句话总结..." session 会污染主 bucket, 反过来
# 被 list_recent_sessions 扫到显示给 V. 用专用 sandbox CWD 物理隔离.
_SUMMARY_SANDBOX = _STATE_DIR / "summary-sandbox"


def _load_summary_cache() -> dict:
    try:
        return json.loads(_SUMMARY_CACHE_FILE.read_text())
    except Exception:
        return {}


def _save_summary_cache(cache: dict) -> None:
    """原子写 (tmp + rename) 避免跟并发 generator 互踩."""
    try:
        _SUMMARY_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SUMMARY_CACHE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(cache, ensure_ascii=False))
        tmp.replace(_SUMMARY_CACHE_FILE)
    except Exception as e:
        log.warning("Failed to persist summary cache: %s", e)


def _extract_session_text_for_summary(jsonl_path: Path, max_chars: int = 6000) -> str:
    """把 session jsonl 里 user + assistant 真实文本拼成给 claude -p 总结的输入.

    max_chars 控制上限, 避免长会话一次塞爆 (haiku 便宜但仍是成本). 优先拿开头
    + 结尾 —— 开头锚定主题, 结尾反映当前状态. 中间大量 tool-call / context dump
    对"一句话总结"没增量.
    """
    lines = []
    try:
        for raw in jsonl_path.read_text().splitlines():
            try:
                d = json.loads(raw)
            except Exception:
                continue
            if d.get("type") not in ("user", "assistant"):
                continue
            msg = d.get("message")
            if not isinstance(msg, dict):
                continue
            text = _extract_text(msg.get("content"))
            if not text or _is_synthetic_user_text(text):
                continue
            role = d.get("type")
            lines.append(f"[{role}] {text[:500]}")
    except Exception:
        return ""
    if not lines:
        return ""
    joined = "\n".join(lines)
    if len(joined) <= max_chars:
        return joined
    # 头 60% + 尾 40%
    head_n = int(max_chars * 0.6)
    tail_n = max_chars - head_n
    return joined[:head_n] + "\n...[省略中段]...\n" + joined[-tail_n:]


def _spawn_summary_generation(sid: str, source_mtime: float) -> None:
    """Fire-and-forget: 后台线程跑 claude -p haiku 生成 session 一句话总结, 写缓存.

    调用方不等结果 — 本次 /resume 仍用 first_user fallback, 下次 /resume 命中缓存.
    Haiku 一句话总结典型 1-3 秒, 10 session 并发大约 3-5 秒写完缓存.
    """
    jsonl = _find_jsonl_any_bucket(sid)
    if jsonl is None:
        return

    def worker() -> None:
        import subprocess
        try:
            text = _extract_session_text_for_summary(jsonl)
            if not text:
                return
            prompt = (
                "用一句话 (不超过 20 个中文字) 总结下面这段 CC session 的核心主题, "
                "让用户一眼认出是哪个对话. 只输出一句话, 不加任何前缀 / 引号 / 解释.\n\n"
                + text
            )
            cli = os.environ.get("CLAUDE_CLI_PATH") or "claude"
            # Model 不写死, 跟随 ~/.claude/settings.json 全局默认 (babata 哲学:
            # 模型会随 CC 升级, 代码里写死 'haiku' 将来可能指向弃用 tier).
            # 若 V 觉得总结任务用 opus 太慢, 改 settings.json 或加 env override.
            #
            # CWD 用 _SUMMARY_SANDBOX 而不是 $HOME, 避免 subprocess session jsonl
            # 污染主 bucket (2026-04-20 踩过的坑).
            _SUMMARY_SANDBOX.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(
                [cli, "-p", prompt,
                 "--output-format", "text",
                 "--permission-mode", "auto"],
                capture_output=True, text=True, timeout=60,
                cwd=str(_SUMMARY_SANDBOX),
            )
            summary = (result.stdout or "").strip().strip('"').strip("「」")
            if not summary or len(summary) > 60:
                # haiku 偶尔抗命给长输出 / 空输出 — 丢弃不写缓存, 下次再试
                return
            cache = _load_summary_cache()
            cache[sid] = {"summary": summary, "source_mtime": source_mtime}
            _save_summary_cache(cache)
        except Exception as e:
            log.debug("summary gen failed for %s: %s", sid[:8], e)

    import threading
    threading.Thread(target=worker, daemon=True).start()


def _scan_peer_sids() -> dict[str, list[str]]:
    """扫 _STATE_DIR 所有 *-session.json, 返回 {sid: [channel_label, ...]}.

    一个 sid 如果被多个 channel 的 recent_sids 收录 (例 V 从 TG /resume 了 bb 开的
    session → 两个 state 都会有它), 列表里会有多个来源.
    """
    out: dict[str, list[str]] = {}
    if not _STATE_DIR.is_dir():
        return out
    for fp in sorted(_STATE_DIR.glob("*-session.json")):
        label = _channel_label_from_state_file(fp)
        try:
            data = json.loads(fp.read_text())
        except Exception:
            continue
        for sid in data.get("recent_sids") or []:
            out.setdefault(sid, []).append(label)
    return out


async def _always_allow(
    tool_name: str,
    tool_input: dict[str, Any],
    ctx: ToolPermissionContext,
) -> PermissionResultAllow:
    """Auto-approve every SDK permission prompt.

    bypassPermissions mode alone doesn't cover CC's "protected paths"
    (~/.claude, .git, .ssh, .zshrc, .mcp.json, ...). Those still prompt in
    every mode except `auto` and `dontAsk`. Bot is non-interactive, so any
    prompt that reaches SDK = hung tool call.

    Personal Mac full-trust — blanket allow."""
    return PermissionResultAllow()

# Tunables. These are storage / token-budget params, not "importance judgments" —
# CC still decides meaning from the data we expose. Kept explicit so a future
# reader sees the cost model instead of magic numbers.
_MAX_RECENT_SIDS = 200          # ring buffer of past session_ids (~1y at 5/day, ~15KB state file)
_RESUME_INJECT_PAIRS = 3        # last N user+assistant pairs to inject on resume failure
_RESUME_INJECT_CHARS = 300      # per-turn char cap (3 pairs × 300 × 2 ≈ 1.8KB, fits any system_prompt)
_IDLE_RESET_MINUTES_DEFAULT = 1440  # 24h, parity with hermes session_reset.idle_minutes (gateway/config.py:114)
_DEFAULT_MEMORY_INJECT_SCRIPT = Path.home() / "cc-workspace/scripts/memory-inject.sh"
_DEFAULT_MEMORY_REFLEX_SCRIPT = Path.home() / "cc-workspace/bin/babata-memory-reflex"
_DEFAULT_MEMORY_REFLEX_LOG = Path.home() / "cc-workspace/state/memory-reflex/events.jsonl"


def _cc_memory_inject_enabled() -> bool:
    if os.environ.get("BABATA_CRON_AGENT") == "1":
        return False
    return os.environ.get("BABATA_CC_MEMORY_INJECT", "1") != "0"


def _memory_inject_script() -> Path:
    configured = os.environ.get("BABATA_MEMORY_INJECT_SCRIPT")
    return Path(configured).expanduser() if configured else _DEFAULT_MEMORY_INJECT_SCRIPT


def _memory_inject_timeout() -> float:
    raw = os.environ.get("BABATA_CC_MEMORY_INJECT_TIMEOUT", "5")
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 5.0


def _memory_reflex_enabled() -> bool:
    return os.environ.get("BABATA_MEMORY_REFLEX", "1") != "0"


def _memory_reflex_mode() -> str:
    if not _memory_reflex_enabled():
        return "off"
    mode = os.environ.get("BABATA_MEMORY_REFLEX_MODE", "dry-run").strip().lower()
    return mode if mode in {"dry-run", "enforce"} else "dry-run"


def _memory_reflex_script() -> Path:
    configured = os.environ.get("BABATA_MEMORY_REFLEX_SCRIPT")
    return Path(configured).expanduser() if configured else _DEFAULT_MEMORY_REFLEX_SCRIPT


def _memory_reflex_timeout() -> float:
    raw = os.environ.get("BABATA_MEMORY_REFLEX_TIMEOUT", "0.8")
    try:
        return max(0.1, float(raw))
    except ValueError:
        return 0.8


def _memory_source_from_prompt(source_prompt: str) -> str:
    lower = source_prompt.lower()
    if "source: telegram" in lower:
        return "tg"
    if "source: wechat" in lower:
        return "wechat"
    if "source: sidebar" in lower:
        return "sidebar"
    return os.environ.get("BABATA_MEMORY_SOURCE") or "unknown"


def _memory_reflex_for_prompt(source_prompt: str, user_prompt: str | None) -> dict[str, Any]:
    if not _memory_reflex_enabled() or not user_prompt:
        return {}
    script = _memory_reflex_script()
    if not script.is_file():
        log.warning("babata memory reflex script missing: %s", script)
        return {}
    source = _memory_source_from_prompt(source_prompt)
    try:
        result = subprocess.run(
            [
                str(script),
                "--message", "-",
                "--source", source,
                "--cpu", "claude",
                "--cwd", _DEFAULT_CWD,
            ],
            input=user_prompt,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=_memory_reflex_timeout(),
            check=False,
        )
    except Exception as exc:
        log.warning("babata memory reflex failed: %s", exc)
        return {}
    if result.returncode != 0:
        log.warning("babata memory reflex exited %s: %s", result.returncode, result.stderr.strip()[:500])
        return {}
    try:
        parsed = json.loads(result.stdout)
    except Exception as exc:
        log.warning("babata memory reflex returned invalid json: %s", exc)
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _format_memory_reflex_hint(reflex: dict[str, Any]) -> str:
    routes = [str(r) for r in reflex.get("routes", []) if str(r)]
    profile = str(reflex.get("profile") or "lite")
    if not routes or (profile == "lite" and all(r in {"none", "lite"} for r in routes)):
        return ""
    reasons = reflex.get("reasons")
    reason_text = "; ".join(str(r) for r in reasons[:3]) if isinstance(reasons, list) else ""
    return "\n".join([
        "<memory-reflex>",
        f"routes: {', '.join(routes)}",
        f"profile: {profile}",
        "note: router signal only; retrieve deeper evidence only when useful.",
        f"why: {reason_text}" if reason_text else "why: unspecified",
        "</memory-reflex>",
    ])


def _memory_reflex_log_path() -> Path:
    configured = os.environ.get("BABATA_MEMORY_REFLEX_LOG")
    return Path(configured).expanduser() if configured else _DEFAULT_MEMORY_REFLEX_LOG


def _message_summary(text: str | None, limit: int = 180) -> str:
    compact = " ".join((text or "").split())
    return compact[:limit].rstrip()


def _append_memory_reflex_event(payload: dict[str, Any]) -> None:
    try:
        path = _memory_reflex_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception as exc:
        log.warning("babata memory reflex log failed: %s", exc)


def _log_memory_reflex_preflight(
    *,
    reflex: dict[str, Any],
    user_prompt: str | None,
    source: str,
    cpu: str,
    mode: str,
    actual_profile: str,
    memory_injected: bool,
    hint_injected: bool,
) -> str | None:
    if not reflex:
        return None
    now = time.time()
    digest = hashlib.sha256((user_prompt or "").encode("utf-8")).hexdigest()
    event_id = hashlib.sha256(f"{now}:{cpu}:{source}:{digest}".encode("utf-8")).hexdigest()[:16]
    _append_memory_reflex_event({
        "event": "preflight",
        "id": event_id,
        "ts": now,
        "source": source,
        "cpu": cpu,
        "mode": mode,
        "message_sha256": digest,
        "message_summary": _message_summary(user_prompt),
        "router": reflex,
        "actual_profile": actual_profile,
        "memory_injected": memory_injected,
        "hint_injected": hint_injected,
        "post_answer_observation": "pending",
    })
    return event_id


def _answer_memory_observation(content: str) -> dict[str, Any]:
    markers = ("不记得", "没记住", "没有记忆", "没有记录", "查不到", "没查到", "无法确认", "没有找到")
    return {
        "heuristic_only": True,
        "memory_miss_marker": any(marker in content for marker in markers),
        "wrong_recall": None,
        "missed_required_lookup": None,
    }


def _log_memory_reflex_post_answer(event_id: str | None, content: str) -> None:
    if not event_id:
        return
    _append_memory_reflex_event({
        "event": "post_answer",
        "id": event_id,
        "ts": time.time(),
        "answer_sha256": hashlib.sha256((content or "").encode("utf-8")).hexdigest(),
        "answer_summary": _message_summary(content),
        "observation": _answer_memory_observation(content or ""),
    })


def _render_babata_memory_context_event(
    source_prompt: str,
    user_prompt: str | None = None,
) -> tuple[str, str | None]:
    if not _cc_memory_inject_enabled():
        return "", None
    script = _memory_inject_script()
    if not script.is_file():
        log.warning("babata memory inject script missing: %s", script)
        return "", None
    reflex = _memory_reflex_for_prompt(source_prompt, user_prompt)
    mode = _memory_reflex_mode()
    enforce = mode == "enforce"
    source = _memory_source_from_prompt(source_prompt)
    actual_profile = os.environ.get("BABATA_MEMORY_PROFILE") or (
        str(reflex.get("profile") or "lite") if enforce else "lite"
    )
    env = os.environ.copy()
    env["BABATA_MEMORY_PROFILE"] = actual_profile
    env.setdefault("BABATA_MEMORY_CPU", "claude")
    env.setdefault("BABATA_MEMORY_SOURCE", source)
    env.setdefault("BABATA_MEMORY_INCLUDE_TOP", "force")
    try:
        result = subprocess.run(
            [str(script)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=_memory_inject_timeout(),
            check=False,
        )
    except Exception as exc:
        log.warning("babata memory inject failed: %s", exc)
        return "", None
    if result.returncode != 0:
        log.warning(
            "babata memory inject exited %s: %s",
            result.returncode,
            result.stderr.strip()[:500],
        )
        return "", None
    parts = [result.stdout.strip()]
    hint = _format_memory_reflex_hint(reflex) if enforce else ""
    if hint:
        parts.append(hint)
    context = "\n\n".join(part for part in parts if part)
    event_id = _log_memory_reflex_preflight(
        reflex=reflex,
        user_prompt=user_prompt,
        source=source,
        cpu="claude",
        mode=mode,
        actual_profile=actual_profile,
        memory_injected=bool(context),
        hint_injected=bool(hint),
    )
    return context, event_id


def _render_babata_memory_context(source_prompt: str, user_prompt: str | None = None) -> str:
    return _render_babata_memory_context_event(source_prompt, user_prompt)[0]


def _idle_reset_seconds() -> int:
    """Idle threshold in seconds. 0 = disabled. Override via BABATA_IDLE_RESET_MINUTES."""
    raw = os.environ.get("BABATA_IDLE_RESET_MINUTES")
    if raw is None:
        m = _IDLE_RESET_MINUTES_DEFAULT
    else:
        try:
            m = int(raw)
        except ValueError:
            log.warning(
                "Bad BABATA_IDLE_RESET_MINUTES=%r, using default %d",
                raw, _IDLE_RESET_MINUTES_DEFAULT,
            )
            m = _IDLE_RESET_MINUTES_DEFAULT
    return max(0, m) * 60


# CC CLI writes synthetic `type:"user"` entries for its own housekeeping:
#   <local-command-caveat>…</local-command-caveat>  — inserted before slash commands
#   <command-name>/foo</command-name>                — the slash command itself
#   <command-message>foo</command-message>           — plain-language label
#   <command-args></command-args>                    — args after the slash
#   <bash-input>…</bash-input>                       — CC's Bash tool calls
#   <local-command-stdout>…                          — command captures
# Any of these as the first "user" message of a session makes the /resume
# preview useless. Match tag-prefixed content so V sees her real first prompt.
_SYNTHETIC_USER_PREFIXES = (
    "<local-command-",
    "<command-name>",
    "<command-message>",
    "<command-args>",
    "<command-stdout>",
    "<command-stderr>",
    "<bash-input>",
    "<bash-stdout>",
    "<bash-stderr>",
)


def _is_synthetic_user_text(text: str) -> bool:
    """True if `text` is CC-injected scaffolding rather than a V-authored turn."""
    stripped = text.lstrip()
    return stripped.startswith(_SYNTHETIC_USER_PREFIXES)


def _extract_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = [b.get("text", "") for b in content
                 if isinstance(b, dict) and b.get("type") == "text"]
        return " ".join(parts).strip()
    return ""


def _tool_result_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for b in content:
            if isinstance(b, dict):
                t = b.get("text") or b.get("content")
                if t:
                    parts.append(str(t))
            else:
                parts.append(str(b))
        return "".join(parts)
    return str(content)


@dataclass
class Response:
    content: str
    session_id: str
    cost: float
    tools: list[str] = field(default_factory=list)
    resume_note: str | None = None  # populated when SDK resume failed + we recovered
    # Model + token accounting, from ResultMessage.model_usage (first key = actual
    # model CC used this turn). None when SDK didn't report (e.g. /new shortcut).
    model: str | None = None
    context_window: int | None = None
    max_output_tokens: int | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0


@dataclass
class Event:
    kind: Literal[
        "tool_use",
        "tool_result",
        "text_delta",
        "turn_end",
        "session_changed",
        "error",
    ]
    name: str | None = None
    input_dict: dict[str, Any] | None = None
    is_error: bool = False
    text: str | None = None
    chunk: str | None = None
    response: Response | None = None
    old_sid: str | None = None
    new_sid: str | None = None
    exception: Exception | None = None


class CC:
    """Single-session Claude Code interface for one channel.

    Each channel (TG / WeChat) owns its own CC instance: separate state file,
    separate resume history, separate MCP tool surface.
    """

    supports_hot_input = False

    def __init__(
        self,
        *,
        state_file: Path,
        source_prompt: str,
        mcp_servers: dict[str, Any] | None = None,
        model: str | None = None,
    ) -> None:
        self._state_file = state_file
        self._source_prompt = source_prompt
        self._mcp_servers = mcp_servers or {}
        self._model = model
        self._session_id: str | None = self._load_state().get("session_id")
        self._memory_reflex_event_id: str | None = None

    def _source_prompt_with_memory(
        self,
        extra_context: str | None = None,
        *,
        user_prompt: str | None = None,
    ) -> str:
        parts = [self._source_prompt]
        memory_context, event_id = _render_babata_memory_context_event(self._source_prompt, user_prompt)
        self._memory_reflex_event_id = event_id
        if memory_context:
            parts.append(memory_context)
        if extra_context:
            parts.append(extra_context)
        return "\n\n".join(parts)

    def _record_memory_reflex_answer(self, content: str) -> None:
        _log_memory_reflex_post_answer(self._memory_reflex_event_id, content)
        self._memory_reflex_event_id = None

    # ── state persistence (per-channel) ──────────────────────────────

    def _load_state(self) -> dict:
        try:
            return json.loads(self._state_file.read_text())
        except Exception:
            return {}

    def _save_state(self, state: dict) -> None:
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_file.with_suffix(self._state_file.suffix + ".tmp")
            tmp.write_text(json.dumps(state))
            tmp.replace(self._state_file)
        except Exception as e:
            log.warning("Failed to persist session state: %s", e)

    def _remember_engine_sid(self, state: dict, sid: str | None) -> None:
        engine_name = getattr(self, "_babata_engine_name", None)
        if not isinstance(engine_name, str) or not engine_name:
            return
        engine_sids = state.get("engine_session_ids")
        if not isinstance(engine_sids, dict):
            engine_sids = {}
        engine_sids[engine_name] = sid or ""
        state["engine_session_ids"] = engine_sids

    def _record_sid(self, sid: str | None) -> None:
        state = self._load_state()
        state["session_id"] = sid
        self._remember_engine_sid(state, sid)
        # Touch activity timestamp on every sid write — _run() calls this after
        # each successful turn, so it doubles as the idle-reset clock.
        state["last_activity_at"] = time.time()
        if sid:
            hist = [s for s in state.get("recent_sids", []) if s != sid]
            hist.insert(0, sid)
            state["recent_sids"] = hist[:_MAX_RECENT_SIDS]
        self._save_state(state)

    def _check_idle_reset(self) -> bool:
        """Silently reset session if idle exceeds threshold. Returns True if reset.

        Idle reset ≠ /new: fires only the babata-local session-end hook for the
        old sid. Skips skill-evolve session-start (V didn't actively reset) and
        skips the "会话已重置" reply. Next turn starts fresh and picks up the
        standard SessionStart memory inject.

        Default 24h, parity with hermes idle_minutes. Migration: state files
        without last_activity_at fall back to file mtime so an already-stale
        sid doesn't get one free turn before reset kicks in.
        """
        threshold = _idle_reset_seconds()
        if threshold <= 0 or not self._session_id:
            return False
        last = self._load_state().get("last_activity_at")
        if not isinstance(last, (int, float)):
            try:
                last = self._state_file.stat().st_mtime
            except OSError:
                return False
        elapsed = time.time() - last
        if elapsed < 0:
            # Future timestamp (clock rollback / NTP / corruption). Don't
            # reset — _record_sid overwrites with current time on next turn.
            log.warning(
                "idle check: future timestamp on sid=%s (last=%s now=%s); skipping",
                self._session_id[:8], last, time.time(),
            )
            return False
        if elapsed <= threshold:
            return False
        log.info(
            "idle reset: sid=%s elapsed=%.0fs threshold=%ds",
            self._session_id[:8], elapsed, threshold,
        )
        old_sid = self._session_id
        self._fire_hook(_HOOKS_DIR, "session-end.sh", old_sid)
        self._session_id = None
        self._record_sid(None)
        return True

    def _recent_turns_summary(self) -> str:
        """Take the most recent session (tracked in state.recent_sids) and
        extract last _RESUME_INJECT_PAIRS user+assistant pairs. Returns '' if
        state empty or no session file usable."""
        sids = self._load_state().get("recent_sids") or []
        for sid in sids:
            target = _CC_PROJECTS / f"{sid}.jsonl"
            if not target.is_file():
                continue
            turns: list[tuple[str, str]] = []
            try:
                for line in target.read_text().splitlines():
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    msg = d.get("message")
                    if not isinstance(msg, dict):
                        continue
                    role = msg.get("role")
                    if role not in ("user", "assistant"):
                        continue
                    text = _extract_text(msg.get("content"))
                    if text:
                        turns.append((role, text))
            except Exception:
                continue
            if not turns:
                continue
            recent = turns[-(2 * _RESUME_INJECT_PAIRS):]
            lines = [f"{'V' if r == 'user' else 'CC'}: {t[:_RESUME_INJECT_CHARS]}"
                     for r, t in recent]
            return "会话从历史归档恢复, 最近几轮:\n" + "\n".join(lines)
        return ""

    def reset(self) -> None:
        old_sid = self._session_id
        if old_sid:
            self._fire_hook(_SKILL_HOOKS_DIR, "session-end.sh", old_sid)
            self._fire_hook(_HOOKS_DIR, "session-end.sh", old_sid)
        self._session_id = None
        self._record_sid(None)
        # skill-evolve SessionStart: 处理 pending + surface 上次 evolve 给 V (空
        # sid, 它不关心新 sid 是啥). babata-local session-start 不在这里 fire —
        # 新 sid 要等下一次 query 的 ResultMessage 才拿得到, 见 _run().
        self._fire_hook(_SKILL_HOOKS_DIR, "session-start.sh", "")

    def list_recent_sessions(
        self, limit: int = 10, channel_filter: list[str] | None = None,
        scan_all_buckets: bool = False,
    ) -> list[dict]:
        """Return recent sessions for `/resume` picker — 跨渠道可见.

        channel_filter: 白名单 channel label 列表. None = 不过滤 (默认, 扫全部).
        传 ['巴巴塔', '巴巴塔2', '巴巴塔3'] (见 constants.INSTANCE_LABELS) = 只列 TG 类 session.
        'term' / 'oneshot' 是特殊 label = 所有 channel state 都没收录过的 orphan
        session, 按 JSONL 的 entrypoint 字段细分:
          - entrypoint=sdk-cli → 'oneshot' (claude -p 一次性, cron / 手敲 -p)
          - 其他 (cli / claude-desktop / sdk-py orphan / 未知) → 'term' (交互或异常)
        这样 /resume 里 "终端" 和 "一次性" 能分开, V 找 bb 交互 session 不再被
        cron 一次性塞满列表.

        行为变更 (2026-04-20): 从"扫本 channel state.recent_sids"改成"扫整个
        _CC_PROJECTS bucket 的 *.jsonl 按 mtime 排序". 原因: V 的设计是多渠道
        共享一个 CC 内核, TG / WX / 终端 bb 开的 session 应互相可见. 旧实现
        依赖 per-channel state, bb/WX 开的 session TG 永远看不到.

        每条结果附带 `channel` 字段 = 首个归属来源 (or "term" 表示没有 channel
        state 收录过, 典型是 bb/原生 claude/cron 开的) 和 `is_own_channel`
        标记, 供 UI 决定是否给非本渠道的 session 加 prefix 提示来源.

        过滤: jsonl 文件丢失 / 首条真实 user 消息找不到 (纯 synthetic scaffolding)
        的 session 跳过. CC CLI 的 synthetic user 消息见 _SYNTHETIC_USER_PREFIXES.
        """
        peer_map = _scan_peer_sids()
        summary_cache = _load_summary_cache()
        own_channel = _channel_label_from_state_file(self._state_file)
        try:
            if scan_all_buckets:
                # 跨 cwd bucket 扫 — V 在终端不同目录开的 claude session 落在
                # 不同 bucket (~/.claude/projects/<cwd-hash>/), 单 bucket 扫只
                # 看得见 babata 自己 cwd 那个. "终端"/"一次性" 渠道用全局视图,
                # V 不用记自己当时在哪个目录敲的 claude.
                # 排除 summary subprocess 的 sandbox bucket — 那里全是 babata
                # 内部 "用一句话总结..." 的 sdk-cli 调用, 不是 V 真正的 oneshot.
                # 用 realpath 防 _SUMMARY_SANDBOX 路径里有 symlink 时跟 CC CLI
                # 用 realpath 算 bucket name 不一致 (Codex F-SANDBOX-EXCLUSION).
                projects_root = _CC_PROJECTS.parent
                try:
                    sandbox_real = str(_SUMMARY_SANDBOX.resolve())
                except Exception:
                    sandbox_real = str(_SUMMARY_SANDBOX)
                sandbox_bucket_name = sandbox_real.replace("/", "-")
                buckets = [
                    b for b in projects_root.iterdir()
                    if b.is_dir() and b.name != sandbox_bucket_name
                ]
                files = sorted(
                    (fp for b in buckets for fp in b.glob("*.jsonl")),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
            else:
                files = sorted(
                    _CC_PROJECTS.glob("*.jsonl"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
        except Exception:
            return []

        out: list[dict] = []
        for fp in files:
            sid = fp.stem
            first_user: str | None = None
            entrypoint: str | None = None
            try:
                for line in fp.read_text().splitlines():
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    if d.get("type") != "user":
                        continue
                    msg = d.get("message")
                    if not isinstance(msg, dict):
                        continue
                    text = _extract_text(msg.get("content"))
                    if not text or _is_synthetic_user_text(text):
                        continue
                    first_user = text
                    # entrypoint 在同一条 user record 上 (cli / sdk-py / sdk-cli /
                    # claude-desktop). 用来区分 "终端交互" vs "一次性 -p".
                    ep = d.get("entrypoint")
                    if isinstance(ep, str):
                        entrypoint = ep
                    break
            except Exception:
                continue
            if not first_user:
                continue
            try:
                mtime = fp.stat().st_mtime
            except Exception:
                mtime = 0.0

            owners = peer_map.get(sid, [])
            if owners:
                channel_label = owners[0]
            elif entrypoint == "sdk-cli":
                # claude -p 一次性 (cron wrapper / 手敲 -p). 和 bb 交互拆开.
                channel_label = "oneshot"
            else:
                # cli (bb / 原生 claude 交互) / claude-desktop / sdk-py orphan / 未知.
                channel_label = "term"

            # channel_filter 白名单过滤. 'term'/'oneshot' 匹配 "无 owner" 的 orphan.
            # 在 summary spawn 之前过滤, 避免 scan_all_buckets 模式下给被丢弃的
            # session 起 haiku 总结 (Codex E-PERF).
            if channel_filter is not None and channel_label not in channel_filter:
                continue

            # Summary cache: 按 jsonl mtime 失效. 命中用缓存, miss/过期后台生成
            # + 本次 fallback first_user. 下次 /resume 就能看见总结.
            cached = summary_cache.get(sid)
            if cached and cached.get("source_mtime") == mtime:
                preview = cached.get("summary") or first_user
            else:
                preview = first_user
                _spawn_summary_generation(sid, mtime)

            is_own = own_channel in owners
            out.append({
                "sid": sid,
                "first_user": first_user,
                "preview": preview,      # summary (命中缓存) or first_user (fallback)
                "mtime": mtime,
                "is_current": sid == self._session_id,
                "channel": channel_label,
                "is_own_channel": is_own,
            })
            if len(out) >= limit:
                break
        return out

    def resume(self, sid: str) -> bool:
        """Switch active session to `sid` (or its forked counterpart). False if JSONL missing.

        Bumps the active sid to the front of recent_sids. Skill-evolve hooks:
        skipped (pointer switch). Babata-local hooks: fired.

        Cross-bucket fork: 若 sid 文件在别的 cwd-bucket, _import_jsonl_to_bucket
        会用 *新 uuid* 复制一份到 babata bucket 并返回新 sid; 这里把 self._session_id
        切到新 sid (不是原 sid), 保证 babata 后续 turn 写新 sid 文件, 不跟原 sid
        在源 bucket 的状态分叉. 调用方 (bot.py /resume callback) 看 self._session_id
        知道 fork 后真正激活的 sid.
        """
        resolved = _import_jsonl_to_bucket(sid)
        if resolved is None:
            return False
        old_sid = self._session_id
        self._session_id = resolved
        self._record_sid(resolved)
        if old_sid != resolved:
            if old_sid:
                self._fire_hook(_HOOKS_DIR, "session-end.sh", old_sid)
            self._fire_hook(_HOOKS_DIR, "session-start.sh", resolved)
        return True

    def get_recent_turns(
        self,
        sid: str,
        pairs: int = 2,
        char_cap: int = 400,
    ) -> list[tuple[str, str]]:
        """Return last `pairs` user+assistant messages from a session JSONL.

        Filters CC CLI's synthetic user scaffolding (caveats, slash-command
        blocks, bash wrappers — same rules as list_recent_sessions preview)
        and assistant turns that contain only tool_use with no text. Each
        text truncated to char_cap chars with ellipsis.

        Returns up to 2 * pairs (role, text) entries in chronological order,
        or empty list if sid has no JSONL or no meaningful turns. Used by
        the bot's `/resume` button click to show V what she just resumed
        into — selecting by 48-char first-user preview isn't enough to tell
        two nearby threads apart.
        """
        fp = _find_jsonl_any_bucket(sid)
        if fp is None:
            return []
        collected: list[tuple[str, str]] = []
        try:
            for line in fp.read_text().splitlines():
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                msg = d.get("message")
                if not isinstance(msg, dict):
                    continue
                role = msg.get("role")
                if role not in ("user", "assistant"):
                    continue
                text = _extract_text(msg.get("content"))
                if not text:
                    continue
                if role == "user" and _is_synthetic_user_text(text):
                    continue
                collected.append((role, text))
        except Exception:
            return []
        tail = collected[-(2 * pairs):]
        out: list[tuple[str, str]] = []
        for role, text in tail:
            if len(text) > char_cap:
                text = text[:char_cap].rstrip() + "…"
            out.append((role, text))
        return out

    def is_last_turn_orphan(self, sid: str | None = None) -> bool:
        """True 当 session jsonl 最后一条真实 turn 是 user 且没有 assistant 回复.

        典型场景: bot 在 cc.query 跑到一半被 SIGKILL (launchd 异常重启 / OOM /
        硬崩), CC CLI 子进程来不及 flush assistant turn 就死了. user 消息 CC
        一收到就写 jsonl, 所以 jsonl 里会留下孤儿 user turn.

        过滤 synthetic scaffolding (caveats / slash-cmd blocks) —— 跟 /resume
        picker 的判定逻辑一致. assistant 只要写过任何一条 (含纯 tool_use) 就不
        算孤儿, 说明 CC 至少开始响应了.

        用于 bot _post_init 上线通知: 孤儿 = 附加 ⚠️ 告警让 V 决定 /resume 或
        /new. 不自动重试 (工具可能有副作用).
        """
        sid = sid or self._session_id
        if not sid:
            return False
        fp = _CC_PROJECTS / f"{sid}.jsonl"
        if not fp.is_file():
            return False
        last_role: str | None = None
        try:
            for line in fp.read_text().splitlines():
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                t = d.get("type")
                if t not in ("user", "assistant"):
                    continue
                msg = d.get("message")
                if not isinstance(msg, dict):
                    continue
                if t == "user":
                    text = _extract_text(msg.get("content"))
                    if text and _is_synthetic_user_text(text):
                        continue
                    last_role = "user"
                else:
                    last_role = "assistant"
        except Exception:
            return False
        return last_role == "user"

    @staticmethod
    def _fire_hook(hook_dir: Path, script: str, session_id: str) -> None:
        """Fire a lifecycle hook asynchronously (non-blocking).

        Two hook dirs in play (see constants): SKILL_HOOKS_DIR (V-private
        skill-evolve) and HOOKS_DIR (repo-local babata hooks, e.g. push sid
        to TG). Fire site picks which dir(s) to hit — skill-evolve has a
        different "session-start" semantic (fired with empty sid, for pending
        scan), so we don't auto-fire both on every event.
        """
        hook_path = hook_dir / script
        if not hook_path.is_file():
            return
        # BABATA_INSTANCE_LABEL: 把当前 bot 的人话昵称 (巴巴塔 / 巴巴塔2 / ...) 暴
        # 露给 hook 脚本, 省得 shell 层再 import constants 反查映射. skill-evolve
        # hooks 不用这个 env 但多塞一个无害.
        env = {
            **os.environ,
            "CLAUDE_SESSION_ID": session_id,
            "BABATA_INSTANCE_LABEL": INSTANCE_LABELS.get(INSTANCE, INSTANCE or PROJECT),
        }
        try:
            subprocess.Popen(
                [str(hook_path)],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as e:
            log.warning("hook %s/%s spawn failed: %s", hook_dir.name, script, e)

    # ── query ────────────────────────────────────────────────────────

    async def query(
        self,
        prompt: str,
        images: list[dict[str, str]] | None = None,
        on_stream: StreamCB | None = None,
    ) -> Response:
        # Channel-agnostic reset command. Any channel (TG/WX/future) whose
        # transport layer doesn't have its own command dispatch still gets
        # /new for free — and reset() fires the skill-evolve hooks.
        if prompt.strip() == "/new" and not images:
            self.reset()
            return Response(content="会话已重置。", session_id="", cost=0.0)

        # Silent idle reset (default 24h, hermes parity). V didn't ask for it,
        # don't reply "会话已重置" — fresh session + memory inject takes over.
        # Override via BABATA_IDLE_RESET_MINUTES env (0 disables).
        self._check_idle_reset()

        opts = ClaudeAgentOptions(
            max_turns=200,
            permission_mode=_PERMISSION_MODE,
            can_use_tool=_always_allow,  # auto-approve protected-path prompts that bypassPermissions still forwards
            cwd=_DEFAULT_CWD,
            cli_path=os.environ.get("CLAUDE_CLI_PATH"),
            include_partial_messages=on_stream is not None,
            system_prompt=self._source_prompt_with_memory(user_prompt=prompt),
            model=self._model,
            setting_sources=_SETTING_SOURCES,  # 默认 [] 隔离; BABATA_SHARED_CC=1 → ["user"] 共享
            mcp_servers=self._mcp_servers,
            # SDK 默认 max_buffer_size = 1MB; V 发 PDF/大图 或 resume 含 base64
            # 附件的老 session 时, CLI stdout 单条 JSON message 就超了 → 报
            # "JSON message exceeded maximum buffer size" → SDK 抛 Exception
            # → cc.query 走 resume-fail 分支 → fire 🔴 + 🟢 让 V 以为在瞎切 session.
            # 64MB 一次性 settle (单条 JSON message 理论上限 ~ context window
            # 文本量级, 远不到 64MB). 2026-04-22 根因: babata-vvv.err 11:20 事件.
            max_buffer_size=64 * 1024 * 1024,
        )

        if self._session_id:
            opts.resume = self._session_id

        try:
            resp = await self._run(opts, prompt, images, on_stream)
            self._record_memory_reflex_answer(resp.content)
            return resp
        except Exception as e:
            if not self._session_id:
                raise
            log.warning("Session resume failed (%s), injecting recent history", e)
            # Resume failed → old sid is effectively ending (SDK will spawn a
            # fresh one on retry). Fire session-end so TG gets closure; the new
            # sid's session-start fires from _run's ResultMessage path.
            self._fire_hook(_HOOKS_DIR, "session-end.sh", self._session_id)
            self._session_id = None
            self._record_sid(None)
            opts.resume = None
            ctx = self._recent_turns_summary()
            if ctx:
                opts.system_prompt = self._source_prompt_with_memory(ctx, user_prompt=prompt)
                note = f"⚠️ 会话重置 ({type(e).__name__}), 已从归档注入最近 {_RESUME_INJECT_PAIRS} 轮"
            else:
                note = f"⚠️ 会话重置 ({type(e).__name__}), 历史归档也没找到"
            resp = await self._run(opts, prompt, images, on_stream)
            self._record_memory_reflex_answer(resp.content)
            resp.resume_note = note
            return resp

    async def _run(
        self,
        opts: ClaudeAgentOptions,
        prompt: str,
        images: list[dict[str, str]] | None,
        on_stream: StreamCB | None,
    ) -> Response:
        client = ClaudeSDKClient(opts)
        messages = []
        tools_seen: list[str] = []

        try:
            await client.connect()

            if images:
                blocks: list[dict[str, Any]] = [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": img["media_type"],
                            "data": img["data"],
                        },
                    }
                    for img in images
                ]
                if prompt:
                    blocks.append({"type": "text", "text": prompt})

                async def _multi():
                    yield {"type": "user", "message": {"role": "user", "content": blocks}}

                await client.query(_multi())
            else:
                await client.query(prompt)

            async for msg in client.receive_messages():
                messages.append(msg)

                if isinstance(msg, ResultMessage):
                    break

                if not on_stream:
                    continue

                if isinstance(msg, AssistantMessage):
                    for block in getattr(msg, "content", []) or []:
                        if isinstance(block, ToolUseBlock):
                            name = getattr(block, "name", "")
                            inp = getattr(block, "input", {}) or {}
                            if name and name not in tools_seen:
                                tools_seen.append(name)
                            await on_stream(name, inp, None, None)
                        # TextBlock not streamed here — it's the *final* full
                        # text, would duplicate what we already sent via
                        # StreamEvent text deltas below.
                elif isinstance(msg, StreamEvent):
                    ev = msg.event or {}
                    if ev.get("type") == "content_block_delta":
                        delta = ev.get("delta") or {}
                        if delta.get("type") == "text_delta":
                            chunk = delta.get("text") or ""
                            if chunk:
                                await on_stream(None, None, chunk, None)
                elif isinstance(msg, UserMessage):
                    for block in getattr(msg, "content", []) or []:
                        if isinstance(block, ToolResultBlock):
                            await on_stream(None, None, None, {
                                "is_error": bool(block.is_error),
                                "text": _tool_result_text(block.content),
                            })
        finally:
            await client.disconnect()

        content = ""
        cost = 0.0
        sid = None
        model: str | None = None
        context_window: int | None = None
        max_output_tokens: int | None = None
        in_tok = out_tok = cache_r = cache_c = 0

        for msg in messages:
            if isinstance(msg, ResultMessage):
                cost = getattr(msg, "total_cost_usd", 0.0) or 0.0
                sid = getattr(msg, "session_id", None)
                result = getattr(msg, "result", None)
                if result:
                    content = str(result).strip()
                # model_usage shape: {"claude-opus-4-N[1m]": {inputTokens, outputTokens,
                # cacheReadInputTokens, cacheCreationInputTokens, contextWindow,
                # maxOutputTokens, ...}}. First key = the model CC actually ran.
                mu = getattr(msg, "model_usage", None) or {}
                if mu:
                    model = next(iter(mu.keys()))
                    stats = mu[model] or {}
                    context_window = stats.get("contextWindow")
                    max_output_tokens = stats.get("maxOutputTokens")
                    in_tok = int(stats.get("inputTokens") or 0)
                    out_tok = int(stats.get("outputTokens") or 0)
                    cache_r = int(stats.get("cacheReadInputTokens") or 0)
                    cache_c = int(stats.get("cacheCreationInputTokens") or 0)
                break

        if not content:
            parts = []
            for msg in messages:
                if isinstance(msg, AssistantMessage):
                    for block in getattr(msg, "content", []) or []:
                        if hasattr(block, "text"):
                            parts.append(block.text)
            content = "\n".join(parts).strip()

        if not content and tools_seen:
            content = "(done)"

        if sid:
            # Detect session boundary: SDK starts a fresh sid on first turn,
            # after /reset, or when resume failed and a new session was created
            # implicitly. Fire babata session-start so TG sees the new sid.
            # Skipped when sid is unchanged (same session continuing).
            if sid != self._session_id:
                self._fire_hook(_HOOKS_DIR, "session-start.sh", sid)
            self._session_id = sid
            self._record_sid(sid)

        return Response(
            content=content,
            session_id=sid or "",
            cost=cost,
            tools=tools_seen,
            model=model,
            context_window=context_window,
            max_output_tokens=max_output_tokens,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cache_read_tokens=cache_r,
            cache_creation_tokens=cache_c,
        )


class LiveSession(CC):
    """Long-lived Claude Code session with streaming user input injection.

    Physical: one SDK client owns one CC CLI subprocess. ``submit()`` only
    enqueues user messages; a background ``client.query(async_iter)`` task
    serially writes them to CLI stdin, so mid-turn TG messages enter the same
    conversation stream instead of spawning competing clients.
    """

    _STOP = object()
    supports_hot_input = True

    def __init__(
        self,
        *,
        state_file: Path,
        source_prompt: str,
        mcp_servers: dict[str, Any] | None = None,
        model: str | None = None,
    ) -> None:
        super().__init__(
            state_file=state_file,
            source_prompt=source_prompt,
            mcp_servers=mcp_servers,
            model=model,
        )
        self._client: ClaudeSDKClient | None = None
        self._inbox: asyncio.Queue[dict[str, Any] | object] = asyncio.Queue()
        self._input_task: asyncio.Task[None] | None = None
        self._connect_lock = asyncio.Lock()
        self._events_active = False
        self._closed = True
        self._pending_replay: list[dict[str, Any]] = []
        self._resume_note_next: str | None = None
        self._resume_recovered = False
        self._started_with_resume = False

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def is_connected(self) -> bool:
        return self._client is not None and not self._closed

    def _make_options(
        self,
        *,
        system_prompt: str | None = None,
        resume: bool = True,
    ) -> ClaudeAgentOptions:
        opts = ClaudeAgentOptions(
            max_turns=200,
            permission_mode=_PERMISSION_MODE,
            can_use_tool=_always_allow,
            cwd=_DEFAULT_CWD,
            cli_path=os.environ.get("CLAUDE_CLI_PATH"),
            include_partial_messages=True,
            system_prompt=system_prompt if system_prompt is not None else self._source_prompt_with_memory(),
            model=self._model,
            setting_sources=_SETTING_SOURCES,
            mcp_servers=self._mcp_servers,
            # SDK 默认 1MB; PDF/大图 / base64 附件 resume 会爆 buffer (2026-04-22
            # babata-vvv.err 事件). 64MB 一次性 settle. 跟 CC._make_options 同步.
            max_buffer_size=64 * 1024 * 1024,
        )
        if resume and self._session_id:
            opts.resume = self._session_id
        return opts

    async def connect(self) -> None:
        """Connect once and start the open-ended SDK input writer."""
        async with self._connect_lock:
            if self.is_connected:
                return
            self._closed = False
            # Idle reset on connect: process restart (e.g. daily 4am launchd
            # restart, see com.babata*.plist) clears stale sid before resume.
            # Bot doesn't reset mid-flight — that complexity belongs to process
            # lifecycle, not LiveSession's hot path.
            self._check_idle_reset()
            try:
                await self._start_client_locked(self._make_options())
            except Exception as e:
                if not self._session_id:
                    self._closed = True
                    raise
                log.warning("LiveSession initial resume failed (%s), retrying fresh", e)
                prompt = self._prepare_resume_recovery(e)
                await self._start_client_locked(
                    self._make_options(system_prompt=prompt, resume=False)
                )

    async def close(self) -> None:
        """Close stdin iterator, disconnect the SDK client, and fire SessionEnd."""
        async with self._connect_lock:
            await self._stop_client_locked(fire_session_end=True)
            self._closed = True

    async def reset_live(self) -> Response:
        """Async /new for the long-lived subprocess: reset state + reconnect."""
        async with self._connect_lock:
            super().reset()
            self._pending_replay.clear()
            self._resume_note_next = None
            self._resume_recovered = False
            await self._stop_client_locked(fire_session_end=False)
            self._closed = False
            await self._start_client_locked(self._make_options(resume=False))
        return Response(content="会话已重置。", session_id="", cost=0.0)

    async def resume_live(self, sid: str) -> bool:
        """Async /resume: move state pointer (with fork-on-import if cross-bucket),
        then reconnect CLI with --resume."""
        async with self._connect_lock:
            ok = super().resume(sid)
            if not ok:
                return False
            self._pending_replay.clear()
            self._resume_note_next = None
            self._resume_recovered = False
            await self._stop_client_locked(fire_session_end=False)
            self._closed = False
            await self._start_client_locked(self._make_options())
        return True

    def submit(self, prompt: str, images: list[dict[str, str]] | None = None) -> None:
        """Enqueue one user message without blocking the PTB update handler."""
        if self._closed or self._client is None:
            raise RuntimeError("LiveSession is not connected")
        self._inbox.put_nowait(self._user_message(prompt, images))

    async def interrupt(self) -> None:
        if not self._client:
            raise RuntimeError("LiveSession is not connected")
        await self._client.interrupt()

    async def context_usage(self) -> dict[str, Any]:
        if not self._client:
            raise RuntimeError("LiveSession is not connected")
        return await self._client.get_context_usage()

    async def events(self) -> AsyncIterator[Event]:
        """Yield parsed SDK events. Exactly one consumer may read this stream."""
        if self._events_active:
            raise RuntimeError("LiveSession.events() already has a consumer")
        self._events_active = True
        messages: list[Any] = []
        tools_seen: list[str] = []
        try:
            while not self._closed:
                client = self._client
                if client is None:
                    await asyncio.sleep(0.1)
                    continue

                try:
                    async for msg in self._receive_with_input_monitor(client):
                        if client is not self._client:
                            break
                        messages.append(msg)

                        if isinstance(msg, ResultMessage):
                            response, changed = self._handle_result_message(
                                messages, tools_seen
                            )
                            messages = []
                            tools_seen = []
                            self._pending_replay.clear()
                            self._started_with_resume = False
                            if changed:
                                yield Event(
                                    kind="session_changed",
                                    old_sid=changed[0],
                                    new_sid=changed[1],
                                )
                            yield Event(kind="turn_end", response=response)
                            continue

                        if isinstance(msg, AssistantMessage):
                            for block in getattr(msg, "content", []) or []:
                                if isinstance(block, ToolUseBlock):
                                    name = getattr(block, "name", "")
                                    inp = getattr(block, "input", {}) or {}
                                    if name and name not in tools_seen:
                                        tools_seen.append(name)
                                    yield Event(
                                        kind="tool_use",
                                        name=name,
                                        input_dict=inp,
                                    )
                        elif isinstance(msg, StreamEvent):
                            ev = msg.event or {}
                            if ev.get("type") == "content_block_delta":
                                delta = ev.get("delta") or {}
                                if delta.get("type") == "text_delta":
                                    chunk = delta.get("text") or ""
                                    if chunk:
                                        yield Event(kind="text_delta", chunk=chunk)
                        elif isinstance(msg, UserMessage):
                            for block in getattr(msg, "content", []) or []:
                                if isinstance(block, ToolResultBlock):
                                    yield Event(
                                        kind="tool_result",
                                        is_error=bool(block.is_error),
                                        text=_tool_result_text(block.content),
                                    )
                except Exception as e:
                    if self._closed:
                        break
                    recovered = await self._recover_from_stream_error(e)
                    if recovered:
                        messages = []
                        tools_seen = []
                        continue
                    log.warning("LiveSession event stream failed: %s", e)
                    # P1.2: tear down dead client/queue so future submit() raises
                    # RuntimeError instead of silently enqueueing into a dead inbox.
                    # ChannelWorker._consume_events supervises and reconnects.
                    await self._mark_dead_after_error()
                    yield Event(kind="error", exception=e)
                    break
                else:
                    if client is self._client:
                        break
        finally:
            self._events_active = False

    async def _start_client_locked(self, opts: ClaudeAgentOptions) -> None:
        client = ClaudeSDKClient(opts)
        await client.connect()
        self._client = client
        self._started_with_resume = bool(getattr(opts, "resume", None))
        self._input_task = asyncio.create_task(
            client.query(self._inbox_iter(self._inbox))
        )

    async def _stop_client_locked(self, *, fire_session_end: bool) -> None:
        client = self._client
        task = self._input_task
        old_queue = self._inbox
        self._client = None
        self._input_task = None
        self._started_with_resume = False

        # P1.1: drop unsent user messages before sentinel — they belong to
        # the OLD session. Without this drain, /new and /resume let pending
        # input flush into the old subprocess before _STOP halts the writer.
        while True:
            try:
                old_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        old_queue.put_nowait(self._STOP)
        if task:
            with suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=2)
        if client:
            with suppress(asyncio.CancelledError, Exception):
                await client.disconnect()
        self._inbox = asyncio.Queue()

        if fire_session_end and self._session_id:
            self._fire_hook(_HOOKS_DIR, "session-end.sh", self._session_id)

    async def _inbox_iter(
        self,
        queue: asyncio.Queue[dict[str, Any] | object],
    ) -> AsyncIterator[dict[str, Any]]:
        while True:
            msg = await queue.get()
            if msg is self._STOP:
                break
            assert isinstance(msg, dict)
            self._pending_replay.append(copy.deepcopy(msg))
            yield msg

    async def _receive_with_input_monitor(
        self,
        client: ClaudeSDKClient,
    ) -> AsyncIterator[Any]:
        receive_iter = client.receive_messages().__aiter__()
        input_task = self._input_task
        while True:
            next_task = asyncio.create_task(receive_iter.__anext__())
            wait_for: set[asyncio.Task[Any]] = {next_task}
            if input_task is not None and not input_task.done():
                wait_for.add(input_task)
            done, _ = await asyncio.wait(wait_for, return_when=asyncio.FIRST_COMPLETED)

            if input_task is not None and input_task in done:
                try:
                    exc = input_task.exception()
                except asyncio.CancelledError:
                    exc = None
                input_task = None
                if exc is not None:
                    next_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await next_task
                    raise exc

            if next_task in done:
                try:
                    yield next_task.result()
                except StopAsyncIteration:
                    return
            else:
                next_task.cancel()
                with suppress(asyncio.CancelledError):
                    await next_task

    async def _recover_from_stream_error(self, exc: Exception) -> bool:
        if (
            not self._started_with_resume
            or self._resume_recovered
            or not self._session_id
        ):
            return False

        async with self._connect_lock:
            if (
                not self._started_with_resume
                or self._resume_recovered
                or not self._session_id
            ):
                return False

            prompt = self._prepare_resume_recovery(exc)
            old_queue = self._inbox
            replay = [copy.deepcopy(m) for m in self._pending_replay]
            pending = self._drain_queue(old_queue)
            self._inbox = asyncio.Queue()
            self._pending_replay = []
            for msg in replay + pending:
                self._inbox.put_nowait(msg)

            await self._stop_old_client_for_reconnect(old_queue)
            self._closed = False
            await self._start_client_locked(
                self._make_options(system_prompt=prompt, resume=False)
            )
            self._resume_recovered = True
            return True

    async def _mark_dead_after_error(self) -> None:
        """Tear down a broken client + queue so submit() refuses input.

        Called from events() when an un-recovered stream error breaks the
        consumer. The next `connect()` (driven by ChannelWorker supervisor)
        will start fresh.
        """
        async with self._connect_lock:
            client = self._client
            task = self._input_task
            self._client = None
            self._input_task = None
            self._started_with_resume = False
            self._closed = True
            self._pending_replay.clear()
            if task and not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
            if client:
                with suppress(asyncio.CancelledError, Exception):
                    await client.disconnect()
            self._inbox = asyncio.Queue()

    async def _stop_old_client_for_reconnect(
        self,
        old_queue: asyncio.Queue[dict[str, Any] | object],
    ) -> None:
        client = self._client
        task = self._input_task
        self._client = None
        self._input_task = None
        self._started_with_resume = False
        old_queue.put_nowait(self._STOP)
        if task:
            with suppress(asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=2)
        if client:
            with suppress(asyncio.CancelledError, Exception):
                await client.disconnect()

    @staticmethod
    def _drain_queue(
        queue: asyncio.Queue[dict[str, Any] | object],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while True:
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return out
            if item is LiveSession._STOP:
                continue
            if isinstance(item, dict):
                out.append(item)

    def _prepare_resume_recovery(self, exc: Exception) -> str:
        old_sid = self._session_id
        if old_sid:
            self._fire_hook(_HOOKS_DIR, "session-end.sh", old_sid)
        self._session_id = None
        self._record_sid(None)
        ctx = self._recent_turns_summary()
        if ctx:
            self._resume_note_next = (
                f"⚠️ 会话重置 ({type(exc).__name__}), "
                f"已从归档注入最近 {_RESUME_INJECT_PAIRS} 轮"
            )
            return self._source_prompt_with_memory(ctx)
        self._resume_note_next = (
            f"⚠️ 会话重置 ({type(exc).__name__}), 历史归档也没找到"
        )
        return self._source_prompt_with_memory()

    @staticmethod
    def _user_message(
        prompt: str,
        images: list[dict[str, str]] | None,
    ) -> dict[str, Any]:
        if images:
            blocks: list[dict[str, Any]] = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": img["media_type"],
                        "data": img["data"],
                    },
                }
                for img in images
            ]
            if prompt:
                blocks.append({"type": "text", "text": prompt})
            content: str | list[dict[str, Any]] = blocks
        else:
            content = prompt
        return {
            "type": "user",
            "message": {"role": "user", "content": content},
            "parent_tool_use_id": None,
        }

    def _handle_result_message(
        self,
        messages: list[Any],
        tools_seen: list[str],
    ) -> tuple[Response, tuple[str | None, str] | None]:
        response = self._response_from_messages(messages, tools_seen)
        if self._resume_note_next:
            response.resume_note = self._resume_note_next
            self._resume_note_next = None

        sid = response.session_id or None
        changed: tuple[str | None, str] | None = None
        if sid:
            old_sid = self._session_id
            if sid != old_sid:
                self._fire_hook(_HOOKS_DIR, "session-start.sh", sid)
                changed = (old_sid, sid)
            self._session_id = sid
            self._record_sid(sid)
        return response, changed

    @staticmethod
    def _response_from_messages(
        messages: list[Any],
        tools_seen: list[str],
    ) -> Response:
        content = ""
        cost = 0.0
        sid = ""
        model: str | None = None
        context_window: int | None = None
        max_output_tokens: int | None = None
        in_tok = out_tok = cache_r = cache_c = 0

        for msg in messages:
            if isinstance(msg, ResultMessage):
                cost = getattr(msg, "total_cost_usd", 0.0) or 0.0
                sid = getattr(msg, "session_id", None) or ""
                result = getattr(msg, "result", None)
                if result:
                    content = str(result).strip()
                mu = getattr(msg, "model_usage", None) or {}
                if mu:
                    model = next(iter(mu.keys()))
                    stats = mu[model] or {}
                    context_window = stats.get("contextWindow")
                    max_output_tokens = stats.get("maxOutputTokens")
                    in_tok = int(stats.get("inputTokens") or 0)
                    out_tok = int(stats.get("outputTokens") or 0)
                    cache_r = int(stats.get("cacheReadInputTokens") or 0)
                    cache_c = int(stats.get("cacheCreationInputTokens") or 0)
                break

        if not content:
            parts = []
            for msg in messages:
                if isinstance(msg, AssistantMessage):
                    for block in getattr(msg, "content", []) or []:
                        if isinstance(block, TextBlock):
                            parts.append(block.text)
            content = "\n".join(parts).strip()

        if not content and tools_seen:
            content = "(done)"

        return Response(
            content=content,
            session_id=sid,
            cost=cost,
            tools=list(tools_seen),
            model=model,
            context_window=context_window,
            max_output_tokens=max_output_tokens,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cache_read_tokens=cache_r,
            cache_creation_tokens=cache_c,
        )
