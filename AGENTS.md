# babata broadcast

babata is the authorized user's local personal agent. The CPU can be Claude Code
or Codex; this repo is only the transport shell for Telegram, WeChat, and
Sidebar. The shell exposes capabilities and handles wire-specific formatting;
the CPU decides.

Terminal Codex/Claude Code sessions are first-class babata channels too. The
communication medium changes, but shared memory/raw/fact/brain layers remain
the source of truth.

## Highest Philosophy

- 1000x north star: keep mechanisms that would still matter if the model were
  much smarter; delete scaffolding that only compensates for current weakness.
- Bot only does what the CPU physically cannot: transport, media conversion,
  bridge/MCP exposure, restart safety, and channel formatting.
- Facts > rules: keep durable facts in memory/raw layers, not as prompt rule
  dumps.
- Capabilities > workflows: expose tools and boundaries; avoid telling the CPU
  how to use them unless the boundary is physical, security, or channel-specific.

## Tool Existence

- Channel entrypoints: `bot.py` (Telegram), `weixin_bot.py` (WeChat),
  `sidebar_bot.py` (browser Sidebar).
- Channel MCP/bridge surfaces: `tg_mcp.py`, `weixin_mcp.py`, `sidebar_mcp.py`.
  Treat these as capabilities, not mandatory workflows.
- Runtime prompt injection is code-owned: channel source prompts live in the
  channel entrypoints; shared memory is injected by `cc.py` / `codex_engine.py`.
- Long-term babata memory starts at `~/cc-workspace/memory/MEMORY.md`; the
  user's curated second brain is accessed only through
  `~/cc-workspace/bin/second-brain`.
- Memory integrity runners live in `~/cc-workspace/bin/`: `memory-guard`,
  `chat-archive-guard`, and `memory-integrity-check`.

## Permission Boundaries

- Public/external actions ask the user first: PRs, comments, contacting people,
  purchases, deletes, or other irreversible externally visible actions.
- Secrets and private identifiers must not enter public repos, logs, summaries,
  issues, PRs, or generated artifacts.
- Raw records are append-only: do not rewrite `chat-archive` to improve a
  summary.
- Self-modification that touches launchd services, CPU binaries, dependencies,
  or bot `ProgramArguments` must go through `scripts/self-ops.sh` as a detached
  helper. Do not inline `launchctl kickstart -k` against the running bot, CPU
  updates, or global package churn from inside the live process.

Detailed setup, architecture, file maps, and command lists belong in README or
lower memory/docs layers, not in this broadcast file.
