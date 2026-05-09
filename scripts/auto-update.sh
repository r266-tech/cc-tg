#!/usr/bin/env bash
# babata auto-update — git pull + uv sync + claude update + restart service.
# Cross-platform (Linux systemd / macOS launchd). Idempotent: 没变化就早退.
# Triggered by systemd timer (Linux) 或 launchd StartCalendarInterval (macOS),
# 配 hourly. install.sh 末尾自动配好.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

# Pull just PROJECT_STATE_DIR from .env (avoid blanket-exporting tokens
# to subprocess). Caller-set PROJECT_STATE_DIR wins.
ENV_FILE="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE" ] && [ -z "${PROJECT_STATE_DIR:-}" ]; then
    PROJECT_STATE_DIR=$(grep -m1 '^PROJECT_STATE_DIR=' "$ENV_FILE" 2>/dev/null \
        | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d '\r')
    [ -n "$PROJECT_STATE_DIR" ] && export PROJECT_STATE_DIR
fi

PROJECT_NAMESPACE="${PROJECT_NAMESPACE:-babata}"
LABEL="com.${PROJECT_NAMESPACE}"
SERVICE="${PROJECT_NAMESPACE}.service"

LOG="$SCRIPT_DIR/logs/auto-update.log"
mkdir -p "$(dirname "$LOG")"
exec >> "$LOG" 2>&1

echo ""
echo "=== $(date -Iseconds) ==="

# CC shell pypi env 污染防御 (用户 env 可能继承公司 nexus)
unset UV_INDEX_URL PIP_INDEX_URL UV_EXTRA_INDEX_URL PIP_EXTRA_INDEX_URL 2>/dev/null || true

export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

UV=$(command -v uv 2>/dev/null || echo "$HOME/.local/bin/uv")
CLAUDE_BIN="${CLAUDE_CLI_PATH:-$(command -v claude 2>/dev/null || echo "$HOME/.local/bin/claude")}"

CODE_CHANGED=0
DEPS_CHANGED=0
CLI_CHANGED=0

# 1) git pull (ff-only, 本地有 modify 时 skip 避冲突)
if [ -d .git ]; then
    if ! git diff-index --quiet HEAD -- 2>/dev/null; then
        echo "本地有未 commit 修改, skip git pull"
    else
        git fetch origin --quiet 2>/dev/null || echo "git fetch failed"
        LOCAL=$(git rev-parse HEAD 2>/dev/null)
        REMOTE=$(git rev-parse "@{u}" 2>/dev/null || echo "$LOCAL")
        if [ "$LOCAL" != "$REMOTE" ]; then
            echo "新 commit: ${LOCAL:0:7} -> ${REMOTE:0:7}"
            if git pull --ff-only --quiet 2>&1; then
                CODE_CHANGED=1
            else
                echo "git pull --ff-only 失败 (非 ff?), skip"
            fi
        fi
    fi
fi

# 2) uv sync (代码 / lock 变了)
if [ "$CODE_CHANGED" = "1" ] && [ -x "$UV" ]; then
    "$UV" sync --quiet 2>&1 | tail -3
    DEPS_CHANGED=1
fi

# 3) claude update (CC native installer 自带升级)
if [ -x "$CLAUDE_BIN" ]; then
    OLD_CLI=$("$CLAUDE_BIN" --version 2>/dev/null | awk '{print $1}')
    "$CLAUDE_BIN" update 2>&1 | tail -3 || true
    NEW_CLI=$("$CLAUDE_BIN" --version 2>/dev/null | awk '{print $1}')
    if [ "$OLD_CLI" != "$NEW_CLI" ]; then
        echo "claude: $OLD_CLI -> $NEW_CLI"
        CLI_CHANGED=1
    fi
fi

# 4) 重启 service (代码 / deps / cli 任一变了)
if [ "$CODE_CHANGED" = "1" ] || [ "$DEPS_CHANGED" = "1" ] || [ "$CLI_CHANGED" = "1" ]; then
    # Build a one-line reason for bot.py's restart-reason channel — V 看到的
    # TG alert 会拼上这串, 知道为啥重启 (而不是空洞的 "launchd 自愈").
    reason_parts=""
    [ "$CODE_CHANGED" = "1" ] && reason_parts="code"
    if [ "$DEPS_CHANGED" = "1" ]; then
        [ -n "$reason_parts" ] && reason_parts="$reason_parts+"
        reason_parts="${reason_parts}deps"
    fi
    if [ "$CLI_CHANGED" = "1" ]; then
        [ -n "$reason_parts" ] && reason_parts="$reason_parts+"
        reason_parts="${reason_parts}cli"
    fi
    REASON="auto-update (scripts/): $reason_parts"

    case "$(uname -s)" in
        Linux)
            if command -v systemctl >/dev/null 2>&1; then
                systemctl --user restart "$SERVICE" 2>&1 && echo "systemd restarted: $SERVICE"
            fi
            ;;
        Darwin)
            if launchctl print "gui/$UID/$LABEL" >/dev/null 2>&1; then
                STATE_DIR_R="${PROJECT_STATE_DIR:-$SCRIPT_DIR/state}"
                mkdir -p "$STATE_DIR_R" 2>/dev/null && \
                    printf '%s\n' "$REASON" > "$STATE_DIR_R/restart-reason-${LABEL}.txt"
                launchctl kickstart -k "gui/$UID/$LABEL" && echo "launchd kickstarted: $LABEL"
            fi
            ;;
    esac
fi

echo "done. (code=$CODE_CHANGED deps=$DEPS_CHANGED cli=$CLI_CHANGED)"
