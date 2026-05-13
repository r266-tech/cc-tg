"""Best-effort turn-end nudge for the skill-evolve consumer.

This module is intentionally dumb: it records that a Babata turn finished and
wakes the existing skill-evolve consumer. Route/skill judgment stays in the
spawned evolution agent, not in the transport path.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _default_nudge_script() -> Path:
    candidates = [
        Path.home() / ".claude" / "skills" / "skill-evolve" / "nudge.sh",
        Path.home() / "cc-workspace" / "babata-skills" / "skill-evolve" / "nudge.sh",
    ]
    configured = os.environ.get("SKILL_EVOLVE_NUDGE_SCRIPT")
    if configured:
        return Path(configured).expanduser()
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return candidates[0]


def _json_default(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    return str(value)


def notify_skill_evolve_turn(
    *,
    session_id: str | None,
    cpu: str,
    source: str,
    channel: str,
    state_file: str | Path | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Wake skill-evolve after a successful user-visible turn.

    This must never affect the user's response path. Any failure is logged at
    debug/warning level and ignored.
    """

    if not session_id:
        return
    if os.environ.get("BABATA_SKILL_EVOLVE_NUDGE", "1") == "0":
        return
    if os.environ.get("SKILL_EVOLVE_SPAWNED") == "1":
        return

    script = _default_nudge_script()
    if not script.is_file():
        log.debug("skill-evolve nudge script missing: %s", script)
        return

    payload = {
        "schema_version": 1,
        "ts": time.time(),
        "event": "turn_end",
        "session_id": session_id,
        "cpu": cpu,
        "source": source,
        "channel": channel,
        "state_file": str(state_file) if state_file else "",
        "cwd": os.getcwd(),
        "pid": os.getpid(),
        "metadata": metadata or {},
    }
    try:
        env = {
            **os.environ,
            "SKILL_EVOLVE_NUDGE_JSON": json.dumps(
                payload,
                ensure_ascii=False,
                default=_json_default,
            ),
        }
        subprocess.Popen(
            ["/bin/bash", str(script)],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as exc:
        log.warning("skill-evolve nudge spawn failed: %s", exc)
