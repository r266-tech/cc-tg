#!/usr/bin/env bash
# Daily auto-update for this project: Claude Code CLI + claude-agent-sdk in venv,
# with graceful launchd-agent restart when anything changed.
#
# Invocation: crontab `@ 04:00` or manual (with --delay-restart N to let an
# in-flight TG message finish sending before we kill the bot).
#
# OSS forks: override PROJECT_NAMESPACE env var (default "babata") to drive
# the launchd label prefix. Example fork-time cron:
#   0 4 * * * PROJECT_NAMESPACE=mycoolbot /path/to/repo/auto-update.sh
# Install paths (repo dir, venv, logs) are derived from SCRIPT_DIR, so the
# script runs correctly wherever the repo is cloned — no hard-coded $HOME.

set -uo pipefail

# --delay-restart N: sleep N seconds before restarting (manual trigger, gives
# the TG ack message time to ship before we tear down the event loop).
DELAY_RESTART=0
while [ $# -gt 0 ]; do
  case "$1" in
    --delay-restart) DELAY_RESTART="$2"; shift 2 ;;
    *) shift ;;
  esac
done

export PATH="/opt/homebrew/bin:$HOME/.npm-global/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"

# memory: reference_cc_shell_uv_index_pollution.md — company pypi env 401s public packages
unset UV_INDEX_URL PIP_INDEX_URL UV_EXTRA_INDEX_URL PIP_EXTRA_INDEX_URL 2>/dev/null || true

# Path derivation. SCRIPT_DIR = repo root (this file lives at repo/auto-update.sh).
# Works from any install location: ~/code/babata, ~/projects/mybot, etc.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Pull just PROJECT_STATE_DIR from .env so the restart-reason file lands
# where bot.py's load_dotenv() makes it look. Avoid `source .env` because
# it would blanket-export TG/API tokens into every subprocess this script
# spawns (git fetch, uv, claude update, etc.) — wider secrets surface than
# necessary. Caller-set PROJECT_STATE_DIR (env / plist) takes precedence.
ENV_FILE="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE" ] && [ -z "${PROJECT_STATE_DIR:-}" ]; then
    PROJECT_STATE_DIR=$(grep -m1 '^PROJECT_STATE_DIR=' "$ENV_FILE" 2>/dev/null \
        | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d '\r')
    [ -n "$PROJECT_STATE_DIR" ] && export PROJECT_STATE_DIR
fi

PROJECT_NAMESPACE="${PROJECT_NAMESPACE:-babata}"
LABEL_PREFIX="com.${PROJECT_NAMESPACE}"

LOG="$SCRIPT_DIR/logs/auto-update.log"
VENV_PY="$SCRIPT_DIR/.venv/bin/python"
NPM="/opt/homebrew/bin/npm"
UV="/opt/homebrew/bin/uv"
CLAUDE_BIN="${CLAUDE_CLI_PATH:-$HOME/.local/bin/claude}"

mkdir -p "$(dirname "$LOG")"
exec >> "$LOG" 2>&1

echo ""
echo "=== $(date -Iseconds) ==="

# 1) Claude Code CLI (native installer at ~/.local/bin/claude)
# 注意: 以前用 `npm update -g`, 但 bot 实际跑的是 native installer (独立安装),
# npm 那份更新到了却影响不到 bot -> 版本卡住没人察觉。改走 `claude update` (native 自带).

# 自愈: ~/.local/bin/claude symlink 在 2026-04-20 无故消失过一次, versions/ 还在.
# 丢了就从 versions/ 最新版重建, 避免 bot 下次启动 CLINotFoundError.
if [ ! -e "$CLAUDE_BIN" ]; then
    LATEST_VER=$(ls $HOME/.local/share/claude/versions/ 2>/dev/null | sort -V | tail -1)
    if [ -n "$LATEST_VER" ]; then
        ln -sf "$HOME/.local/share/claude/versions/$LATEST_VER" "$CLAUDE_BIN"
        echo "Restored $CLAUDE_BIN -> $LATEST_VER"
    fi
fi

# 防 npm 回潮 (不走 `npm uninstall` — 那个 hook 会连带干掉 native symlink)
if [ -e "$HOME/.npm-global/bin/claude" ] || [ -d "$HOME/.npm-global/lib/node_modules/@anthropic-ai/claude-code" ]; then
    rm -f "$HOME/.npm-global/bin/claude"
    rm -rf "$HOME/.npm-global/lib/node_modules/@anthropic-ai/claude-code"
    echo "Purged npm-global claude"
fi

# 清半成品 version (install 中断留的 0 字节文件)
find $HOME/.local/share/claude/versions -maxdepth 1 -type f -size 0 -delete 2>/dev/null || true

OLD_CLI=$("$CLAUDE_BIN" --version 2>/dev/null | awk '{print $1}')
"$CLAUDE_BIN" update 2>&1 | tail -5
NEW_CLI=$("$CLAUDE_BIN" --version 2>/dev/null | awk '{print $1}')
echo "CLI: $OLD_CLI -> $NEW_CLI"

# 2) claude-agent-sdk in babata venv (uv-managed, no pip)
OLD_SDK=$("$VENV_PY" -c "import claude_agent_sdk; print(claude_agent_sdk.__version__)" 2>/dev/null)
"$UV" pip install --python "$VENV_PY" --upgrade claude-agent-sdk 2>&1 | tail -5
# Sync uv.lock to match the upgrade. `pip install --upgrade` ignores the lock,
# so without this any dev `uv sync` would downgrade venv to lock's pinned (older)
# version → next auto-update upgrades again → phantom kickstart of all bots.
# Soft-fail: if lock update breaks (network, uv crash), keep going and warn —
# venv is already upgraded so bots still get the new SDK; lock just stays stale
# until next cycle. set -uo pipefail (no -e) means `cmd | tail` non-zero would
# silently continue without this `||` guard.
"$UV" --directory "$SCRIPT_DIR" lock --upgrade-package claude-agent-sdk 2>&1 | tail -3 \
    || echo "WARN: uv lock failed — lock now stale vs venv, expect phantom restart next cycle if dev runs uv sync"
NEW_SDK=$("$VENV_PY" -c "import claude_agent_sdk; print(claude_agent_sdk.__version__)" 2>/dev/null)
echo "SDK: $OLD_SDK -> $NEW_SDK"

# 3) If anything changed, kickstart every launchd agent under LABEL_PREFIX,
# all sharing this repo's .venv. Dynamic enumeration from `launchctl list`
# means adding vvvvv / removing vvv / adding a new channel = zero script
# changes. Naming contract: plist Label ∈ {LABEL_PREFIX, LABEL_PREFIX.*}.
#   $1 ~ /^[0-9]+$/                    : pid is numeric (skip header + dormant agents)
#   $3 ~ ("^" prefix "($|[.])")        : exact prefix match, no com.babataXXX false hit
if [ "$OLD_CLI" != "$NEW_CLI" ] || [ "$OLD_SDK" != "$NEW_SDK" ]; then
    LABELS=$(launchctl list | awk -v prefix="$LABEL_PREFIX" '
        $1 ~ /^[0-9]+$/ && $3 ~ ("^" prefix "($|[.])") {print $3}
    ')
    if [ -z "$LABELS" ]; then
        echo "WARNING: no running ${LABEL_PREFIX}* agents, nothing to restart"
    else
        # Restart reason channel: bot.py reads STATE_DIR/restart-reason-{label}.txt
        # at graceful shutdown (or startup) and surfaces it in the TG alert.
        # STATE_DIR = constants.py default (PROJECT_STATE_DIR env or repo-local state/).
        STATE_DIR_R="${PROJECT_STATE_DIR:-$SCRIPT_DIR/state}"
        mkdir -p "$STATE_DIR_R"
        reason_parts=""
        [ "$OLD_CLI" != "$NEW_CLI" ] && reason_parts="CLI $OLD_CLI→$NEW_CLI"
        if [ "$OLD_SDK" != "$NEW_SDK" ]; then
            [ -n "$reason_parts" ] && reason_parts="$reason_parts, "
            reason_parts="${reason_parts}SDK $OLD_SDK→$NEW_SDK"
        fi
        REASON="auto-update: $reason_parts"

        echo "Changes detected, restarting: $(echo $LABELS | tr '\n' ' ')"
        for label in $LABELS; do
            # Skip wx label: weixin_bot has no TG alert consumer for the
            # restart-reason channel (file write would be unread).
            if [ "$label" != "${LABEL_PREFIX}.weixin" ]; then
                printf '%s\n' "$REASON" > "$STATE_DIR_R/restart-reason-${label}.txt"
            fi
            if launchctl kickstart -k "gui/$(id -u)/$label" 2>&1; then
                echo "  ok   $label"
            else
                echo "  fail $label"
            fi
        done
    fi

    # 4) Optional post-update hook: analysis agent that reads release notes
    # and decides whether to push an alert. Synchronous (not nohup-detached)
    # so the hook inherits auto-update.sh's launchd session — needed for the
    # child `claude -p` to reach the macOS keychain. Hook is bounded by its
    # own internal timeout; worst case adds ~10 min to auto-update runtime.
    # OSS users: set POST_UPDATE_HOOK to your own script (same argv shape) or
    # leave it pointing nowhere — missing file = silent skip.
    POST_UPDATE_HOOK="${POST_UPDATE_HOOK:-$HOME/cc-workspace/cron-skills/version-watch/run.sh}"
    if [ -x "$POST_UPDATE_HOOK" ]; then
        echo "post-update hook running..."
        "$POST_UPDATE_HOOK" \
            --cc-old "$OLD_CLI" --cc-new "$NEW_CLI" \
            --sdk-old "$OLD_SDK" --sdk-new "$NEW_SDK" \
            || echo "post-update hook exited $?"
    fi
else
    echo "No changes, bots untouched."
fi
