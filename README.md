# babata

Your coding agent, on Telegram.

babata is a thin transport layer that lets you talk to an agentic coding CLI from any phone, any client. Claude Code is the default CPU; Codex CLI can be selected with `BABATA_ENGINE=codex` or switched from Telegram with `/cpu`.

```
                             ┌─────────────┐
   📱 Telegram / WeChat ────▶│   babata    │────▶  claude / codex
                             │  (transport) │
                             └─────────────┘
```

The bot only does what CC physically cannot: TG HTML / 4096-char chunking / OGG voice transcription / image base64. It gives CC capabilities (MCP tools to push back to TG), never tells CC how to use them.

## Quick Start

```bash
git clone https://github.com/r266-tech/babata.git
cd babata
bash install.sh
```

Detects what's missing (Python / uv / ffmpeg / Claude Code), installs deps, scaffolds `.env`. Then edit `.env` and run.

## You'll need

- **macOS or Linux** with Python 3.11+
- **A Telegram bot** — message [@BotFather](https://t.me/BotFather), `/newbot`, save the token
- **Your TG user ID** — message [@userinfobot](https://t.me/userinfobot)
- **An Anthropic API key** — https://console.anthropic.com → Settings → API keys
  - *Or skip the key + set `BABATA_SHARED_CC=1` to share with your existing logged-in `claude`*

## Run

```bash
$EDITOR .env                    # fill TELEGRAM_BOT_TOKEN, ALLOWED_USER_ID, ANTHROPIC_API_KEY
babata                          # bot starts (foreground, Ctrl+C to stop)
                                # → message it on Telegram
```

`install.sh` symlinks `babata` into `~/.local/bin/`, so it's globally available — same shape as `hermes` / `openclaw`. (If the command isn't found, add `~/.local/bin` to your PATH.)

## Modes

**Default — isolated (recommended for OSS users)**:
babata doesn't touch your `~/.claude/` settings, doesn't read your OAuth keychain, doesn't pollute your existing CC sessions. It runs as its own contained Claude instance, authed via `ANTHROPIC_API_KEY`.

**Shared mode** (`.env`: `BABATA_SHARED_CC=1`):
babata shares your existing logged-in CC — same skills, same settings, same OAuth. No `ANTHROPIC_API_KEY` needed. Quota / settings changes affect both.

**Codex CPU** (`.env`: `BABATA_ENGINE=codex`):
babata keeps the same TG/WeChat/sidebar transport but runs turns through `codex exec --json`. Current first cut supports query, resume within babata's own Codex state, images, MCP server wiring, `/stop`, and final-message streaming. Telegram `/cpu` overrides the `.env` default for that channel and persists in the channel state. Codex does not yet expose the same hot-input control path as Claude Code here, so ordinary TG cut-in messages queue until the active Codex turn ends; `/stop` cancels the active Codex exec turn.

**Full trust** (`.env`: `BABATA_FULL_TRUST=1`):
babata's CC subprocess runs with `cwd=$HOME` and `permission_mode=auto` (CC official auto mode, status shows "auto mode on") — can read your home, run any command without prompts for low-risk work. ⚠️ Only when `ALLOWED_USER_ID` is strictly correct, since anyone who can DM the bot effectively gets shell access.

## Multi-instance

Run multiple babatas on one machine — different TG bots, different chats, shared code, independent state. Second instance:

```bash
BABATA_INSTANCE=alice TELEGRAM_BOT_TOKEN=... ALLOWED_USER_ID=... .venv/bin/python bot.py
```

State files / sockets / launchd labels all derive from `PROJECT_NAMESPACE` + `BABATA_INSTANCE` so nothing collides.

## Persist (macOS launchd)

See [`docs/persist-macos.md`](docs/persist-macos.md) — copy a plist template, edit paths, `launchctl bootstrap`.

## Architecture

| File | Role |
|---|---|
| `bot.py` | TG transport (HTML, 4096 chunks, reactions, auth) |
| `weixin_bot.py` | WeChat transport (iLink protocol, optional) |
| `engine.py` | CPU selector (`BABATA_ENGINE=claude` / `codex`) |
| `cc.py` | Claude Code SDK wrapper, channel-agnostic |
| `codex_engine.py` | Codex CLI adapter using `codex exec --json` |
| `bridge.py` | Unix socket so MCP tools can push to TG |
| `tg_mcp.py` | MCP tools `tg_send_*` exposed to CC |
| `media.py` | OGG → WAV, image base64, video understanding |
| `constants.py` | Single source of truth for paths / labels |

## Commands

| Command | Role |
|---|---|
| `/cpu` | Switch current TG CPU between Claude Code and Codex |
| `/new` | Start a fresh session |
| `/resume` | Resume a recent session |
| `/status` | Show model, session, and tool-display state |
| `/context` | Show Claude Code context usage; hidden on Codex |
| `/verbose` | Tool display: hidden / flash / keep |
| `/stop` | Interrupt current turn |
| `/provider` | Switch Anthropic provider or Codex account through optional cc-router |

## License

MIT
