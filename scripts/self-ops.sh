#!/usr/bin/env bash
# babata self-modification helper — see CLAUDE.md 铁律段.
# 所有会改写 bot 自身 (launchd / claude binary / deps) 的操作走本脚本,
# 内部 `nohup & disown` 脱离 bot 进程管辖, SIGTERM 不连坐.

set -euo pipefail

DELAY="${DELAY:-5}"
UID_N=$(id -u)
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Pull just PROJECT_STATE_DIR from .env (avoid blanket-exporting tokens).
ENV_FILE="$REPO_DIR/.env"
if [ -f "$ENV_FILE" ] && [ -z "${PROJECT_STATE_DIR:-}" ]; then
    PROJECT_STATE_DIR=$(grep -m1 '^PROJECT_STATE_DIR=' "$ENV_FILE" 2>/dev/null \
        | cut -d= -f2- | tr -d '"' | tr -d "'" | tr -d '\r')
    [ -n "$PROJECT_STATE_DIR" ] && export PROJECT_STATE_DIR
fi

LABEL_PREFIX="com.${PROJECT_NAMESPACE:-babata}"
RESTART_IDLE_WAIT_SECONDS="${RESTART_IDLE_WAIT_SECONDS:-3600}"

runtime_file_for_label() {
    local label="$1"
    local prefix="$LABEL_PREFIX"
    local instance=""
    if [ "$label" != "$prefix" ]; then
        instance="${label#${prefix}.}"
    fi
    local state_dir="${PROJECT_STATE_DIR:-$REPO_DIR/state}"
    printf '%s/runtime-status-%s.json\n' "$state_dir" "$instance"
}

restart() {
    local label="${1:-$LABEL_PREFIX}"

    # Self-suicide guard: if the caller is itself running under the launchd
    # service we are about to kickstart -k, we'd SIGKILL our own ancestor
    # before the current Claude turn finishes — V sees the assistant
    # truncated mid-reply + a "可能 SIGKILL" watchdog alert. Detect by
    # walking the parent PID chain looking for the launchd-managed bot
    # process. If found, refuse the inline restart and tell the caller to
    # defer (e.g. via TG /restart, daily-restart 4am, or `at now+1m`).
    #
    # Heuristic: launchctl list <label> reports the live PID. Walk our own
    # process ancestry (ps -o ppid=) up to PID 1; if any ancestor matches
    # the live PID, we're inside the target service. 5-step depth cap
    # guards against ps loops; macOS PID space is small but bounded walks
    # are cheap.
    # `|| true` on both pipeline assignments below is critical:
    # under `set -euo pipefail` (line 6), a non-zero exit from
    # launchctl/ps would abort the function before the guard logic
    # could reach kickstart — turning a probe failure into a silent
    # restart failure. Codex round-1 review caught this.
    local target_pid
    target_pid=$(launchctl list "$label" 2>/dev/null | awk -F'"' '/"PID"/{print $3; exit}' | tr -d '=; \t') || true
    if [ -n "$target_pid" ] && [ "$target_pid" != "-" ]; then
        local p=$$
        local depth=0
        while [ "$p" != "1" ] && [ "$p" != "0" ] && [ "$depth" -lt 12 ]; do
            if [ "$p" = "$target_pid" ]; then
                echo "DETACH: caller runs under $label; scheduling out-of-band helper." >&2
                break
            fi
            # Dead ancestor mid-walk → ps returns non-zero. `|| break`
            # turns that into "stop walking, proceed to kickstart"
            # rather than letting errexit abort the whole script.
            p=$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' ') || break
            [ -z "$p" ] && break
            depth=$((depth + 1))
        done
    fi

    # Restart reason channel: bot reads STATE_DIR/restart-reason-{label}.txt at
    # graceful shutdown to surface trigger in TG alert. Wrap in best-effort
    # group — under set -euo pipefail, an mkdir/printf failure must NOT abort
    # the kickstart below. Worst case: V loses the reason line, still sees
    # restart alert.
    #
    # Skip wx label: weixin_bot has no TG alert consumer for the restart-reason
    # channel — writing the file would be unread + stale-on-first-consume hazard
    # if a wx alert is added later. Same skip lives in babata-daily-restart.sh
    # and root auto-update.sh.
    if [ "$label" != "${LABEL_PREFIX}.weixin" ]; then
        local state_dir="${PROJECT_STATE_DIR:-$REPO_DIR/state}"
        {
            mkdir -p "$state_dir" && \
            printf '%s\n' "manual: self-ops restart" > "$state_dir/restart-reason-${label}.txt"
        } || echo "WARN: failed to write restart-reason file, kickstarting anyway"
    fi
    local runtime_file
    runtime_file="$(runtime_file_for_label "$label")"
    local helper="${label}.restart.$(date +%s).$$"
    launchctl submit -l "$helper" -- /bin/bash -lc '
        delay="$1"
        uid_n="$2"
        label="$3"
        helper="$4"
        runtime_file="$5"
        wait_s="$6"
        sleep "$delay"
        deadline=$(( $(date +%s) + wait_s ))
        while [ -f "$runtime_file" ]; do
            busy=$(python3 - "$runtime_file" <<'"'"'PY'"'"' 2>/dev/null || echo 0
import json, sys, time
try:
    data=json.load(open(sys.argv[1]))
    fresh=time.time()-float(data.get("ts", 0)) < 120
    print(1 if fresh and int(data.get("in_flight") or 0) > 0 else 0)
except Exception:
    print(0)
PY
)
            [ "$busy" != "1" ] && break
            [ "$(date +%s)" -ge "$deadline" ] && break
            sleep 1
        done
        launchctl kickstart -k "gui/$uid_n/$label"
        launchctl remove "$helper" >/dev/null 2>&1 || true
    ' bash "$DELAY" "$UID_N" "$label" "$helper" "$runtime_file" "$RESTART_IDLE_WAIT_SECONDS"
    echo "已排队: ${DELAY}s 后 kickstart -k $label"
}

reload_plist() {
    local label="${1:-$LABEL_PREFIX}"
    local plist="${2:-$HOME/Library/LaunchAgents/${label}.plist}"
    if [ ! -f "$plist" ]; then
        echo "ERR: plist not found: $plist" >&2
        return 1
    fi
    if [ "$label" != "${LABEL_PREFIX}.weixin" ]; then
        local state_dir="${PROJECT_STATE_DIR:-$REPO_DIR/state}"
        {
            mkdir -p "$state_dir" && \
            printf '%s\n' "manual: self-ops reload-plist" > "$state_dir/restart-reason-${label}.txt"
        } || echo "WARN: failed to write restart-reason file, reloading anyway"
    fi
    local helper="${label}.reload.$(date +%s).$$"
    local runtime_file
    runtime_file="$(runtime_file_for_label "$label")"
    launchctl submit -l "$helper" -- /bin/bash -lc '
        delay="$1"
        uid_n="$2"
        label="$3"
        plist="$4"
        helper="$5"
        runtime_file="$6"
        wait_s="$7"
        sleep "$delay"
        deadline=$(( $(date +%s) + wait_s ))
        while [ -f "$runtime_file" ]; do
            busy=$(python3 - "$runtime_file" <<'"'"'PY'"'"' 2>/dev/null || echo 0
import json, sys, time
try:
    data=json.load(open(sys.argv[1]))
    fresh=time.time()-float(data.get("ts", 0)) < 120
    print(1 if fresh and int(data.get("in_flight") or 0) > 0 else 0)
except Exception:
    print(0)
PY
)
            [ "$busy" != "1" ] && break
            [ "$(date +%s)" -ge "$deadline" ] && break
            sleep 1
        done
        launchctl bootout "gui/$uid_n/$label" 2>/dev/null || true
        launchctl bootstrap "gui/$uid_n" "$plist"
        launchctl remove "$helper" >/dev/null 2>&1 || true
    ' bash "$DELAY" "$UID_N" "$label" "$plist" "$helper" "$runtime_file" "$RESTART_IDLE_WAIT_SECONDS"
    echo "已排队: ${DELAY}s 后 reload $label from $plist"
}

bootstrap_plist() {
    local plist="${1:?plist path required}"
    nohup bash -c "launchctl bootstrap gui/$UID_N '$plist'" >/dev/null 2>&1 &
    disown
    echo "已排队: bootstrap $plist"
}

update_claude() {
    # 走 auto-update.sh 而非 `claude update` — 前者含 npm 防护 / symlink 自愈 / 变更时 kickstart.
    nohup "$REPO_DIR/auto-update.sh" >/dev/null 2>&1 &
    disown
    echo "已排队: auto-update.sh"
}

case "${1:-}" in
    restart)        shift; restart "$@" ;;
    reload-plist)   shift; reload_plist "$@" ;;
    bootstrap)      shift; bootstrap_plist "$@" ;;
    update-claude)  update_claude ;;
    *) echo "Usage: $0 {restart [<label>] | reload-plist [<label> [<plist>]] | bootstrap <plist> | update-claude}" >&2; exit 1 ;;
esac
