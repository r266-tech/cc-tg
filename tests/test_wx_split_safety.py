"""Tests for wx stream-split safety helpers (_md_balanced + _find_safe_split).

These guard the wx bot stream coalescer from cutting messages mid-word /
inside ``...`` / inside **...**, which would leak markdown chars verbatim
because strip_markdown's pair-matching regexes fail on broken pairs.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SDK_SITE = next(iter((_REPO / ".venv/lib").glob("python*/site-packages")), None)
if _SDK_SITE:
    sys.path.insert(0, str(_SDK_SITE))
sys.path.insert(0, str(_REPO))

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123:test")
os.environ.setdefault("ALLOWED_USER_ID", "0")

import weixin_bot as wb


# ── _md_balanced ────────────────────────────────────────────────────


def test_md_balanced_empty():
    assert wb._md_balanced("") is True


def test_md_balanced_no_markers():
    assert wb._md_balanced("hello world\n中文测试") is True


def test_md_balanced_paired_backticks():
    assert wb._md_balanced("look at `foo` and `bar`") is True


def test_md_balanced_unpaired_backtick():
    assert wb._md_balanced("look at `foo and bar") is False


def test_md_balanced_paired_bold():
    assert wb._md_balanced("this is **bold** text") is True


def test_md_balanced_unpaired_bold():
    assert wb._md_balanced("this is **bold text") is False


def test_md_balanced_escaped_backtick_does_not_count():
    assert wb._md_balanced(r"escaped \` mark") is True


def test_md_balanced_triple_star_not_bold():
    # *** is not a ** marker — single * variant, ignored by _BOLD_RE
    assert wb._md_balanced("***intense***") is True


def test_md_balanced_chinese_underscore_not_markdown():
    # _receive_ in Chinese context is just an identifier, not markdown
    assert wb._md_balanced("调用 `_receive_with_input_monitor` 主循环") is True


def test_md_balanced_complete_link():
    assert wb._md_balanced("see [docs](https://example.com) for more") is True


def test_md_balanced_unclosed_link_text():
    assert wb._md_balanced("see [docs is missing the close") is False


def test_md_balanced_unclosed_link_url():
    # `]` came but `(` opened and never closed
    assert wb._md_balanced("see [docs](https://exa") is False


def test_md_balanced_complete_image():
    assert wb._md_balanced("look ![alt](pic.png) here") is True


def test_md_balanced_chinese_brackets_not_markdown():
    # Chinese 【】「」 are NOT markdown link openers
    assert wb._md_balanced("【重要】这是中文括号") is True


def test_md_balanced_wikipedia_link_with_paren_in_url():
    # Round 2 codex finding: `[link](https://en.wikipedia.org/wiki/Foo_(bar))`
    # has nested ) in URL; old regex `[^()]*` would not consume it and leave
    # `[` unmatched → false unbalanced → flush stalls.
    text = "see [Foo](https://en.wikipedia.org/wiki/Foo_(bar)) for more"
    assert wb._md_balanced(text) is True


def test_md_balanced_link_with_double_nested_paren():
    # Two paren pairs in URL — very rare but shouldn't break.
    text = "[ref](https://x.com/a_(b)_(c))"
    # Either accept (best) or stay deterministic; current regex allows
    # one level so this case may be False — assert it terminates without
    # exception, doesn't matter which boolean.
    result = wb._md_balanced(text)
    assert isinstance(result, bool)


# ── strip_markdown fence info string ────────────────────────────────


def test_strip_markdown_fence_with_dot_in_lang(monkeypatch):
    # Round 2 codex finding: `.env` info string was leaking before fix.
    monkeypatch.setenv("BABATA_WEIXIN_STRIP_MD", "1")
    text = "```.env\nKEY=value\n```"
    out = wb.strip_markdown(text)
    assert "KEY=value" in out
    assert ".env" not in out
    assert "```" not in out


def test_strip_markdown_fence_with_space_in_lang(monkeypatch):
    monkeypatch.setenv("BABATA_WEIXIN_STRIP_MD", "1")
    text = "```shell script\nls -la\n```"
    out = wb.strip_markdown(text)
    assert "ls -la" in out
    assert "shell script" not in out
    assert "```" not in out


def test_strip_markdown_fence_with_csharp_lang(monkeypatch):
    monkeypatch.setenv("BABATA_WEIXIN_STRIP_MD", "1")
    text = "```c#\nint x = 1;\n```"
    out = wb.strip_markdown(text)
    assert "int x = 1;" in out
    assert "c#" not in out
    assert "```" not in out


# ── _sanitize_unbalanced_markers ────────────────────────────────────


def test_sanitize_drops_unpaired_backtick():
    text = "前文 `unclosed code"
    out = wb._sanitize_unbalanced_markers(text)
    assert "`" not in out
    assert "unclosed code" in out
    assert wb._md_balanced(out) is True


def test_sanitize_drops_unpaired_bold():
    text = "段落 **不闭合的粗体"
    out = wb._sanitize_unbalanced_markers(text)
    assert "**" not in out
    assert "不闭合的粗体" in out
    assert wb._md_balanced(out) is True


def test_sanitize_drops_unmatched_link_opener():
    text = "前文。请看 [文档没闭合"
    out = wb._sanitize_unbalanced_markers(text)
    assert wb._md_balanced(out) is True
    assert "[" not in out


def test_sanitize_keeps_balanced_text_alone():
    # Already balanced → no change
    text = "纯文本 `code` 和 **bold** 都齐"
    out = wb._sanitize_unbalanced_markers(text)
    assert out == text  # no need to mutate


def test_sanitize_handles_multiple_unbalanced_markers():
    text = "before `a [b **c"
    out = wb._sanitize_unbalanced_markers(text)
    assert wb._md_balanced(out) is True
    assert "before" in out  # content survives


# ── _find_safe_split ────────────────────────────────────────────────


def test_find_safe_split_empty():
    assert wb._find_safe_split("") == 0


def test_find_safe_split_paragraph_boundary():
    text = "first paragraph.\n\nsecond paragraph"
    pos = wb._find_safe_split(text)
    # split after \n\n keeps prefix balanced and ends at a paragraph break
    assert text[:pos] == "first paragraph.\n\n"


def test_find_safe_split_chinese_period():
    text = "第一句话。第二句话还没结束"
    pos = wb._find_safe_split(text)
    assert text[:pos].endswith("。")


def test_find_safe_split_avoid_unpaired_backtick():
    # The latest space sits inside an unpaired `code span — splitting there
    # would leave the prefix with an unclosed ` that strip_markdown can't
    # remove. _find_safe_split must retract to a safer boundary.
    text = "前面一句话。后面 `code span 还没"
    pos = wb._find_safe_split(text)
    # Must split at the 。 (balanced) — never at the space inside `code span
    assert wb._md_balanced(text[:pos])
    assert text[:pos] == "前面一句话。"


def test_find_safe_split_avoid_unpaired_bold():
    text = "段落一。段落二 **重要的话还没"
    pos = wb._find_safe_split(text)
    assert wb._md_balanced(text[:pos])
    # Must NOT split inside the open ** span
    assert "**" not in text[:pos] or text[:pos].count("**") % 2 == 0


def test_find_safe_split_no_boundary_returns_zero():
    # Pure run of CJK with no boundary punctuation, no whitespace
    text = "中文连续无标点的长串文字一直没有句号也没有空格"
    pos = wb._find_safe_split(text)
    assert pos == 0


def test_find_safe_split_falls_back_to_space():
    text = "english sentence with no terminator just spaces"
    pos = wb._find_safe_split(text)
    # Must return a valid position split on space, prefix balanced
    assert pos > 0
    assert wb._md_balanced(text[:pos])
    assert text[pos - 1] == " " or text[: pos].endswith("\n")


def test_find_safe_split_screenshot_case():
    # Reproduction of V's actual screenshot — buf cut inside `...`.
    text = (
        "把它设回 False → 新 turn 没人监控\n"
        "中等 (2):\n"
        "4. Resume turn 卡死触发 watchdog → 走 `_recover_from_stream_error` 可能重发已发消息\n"
        "5. Timeout 后 `_turn_active` 没 reset → fresh reconnect 立刻被误杀\n"
        "修复方向: 改用独立 watchdog `asyncio.Task`（不动 `_receive_"
    )
    pos = wb._find_safe_split(text)
    # The legitimate split is after the line ending right before
    # "修复方向" — that keeps all backticks paired. The unbalanced trailing
    # snippet (containing the open `_receive_) must not appear in prefix.
    prefix = text[:pos]
    assert wb._md_balanced(prefix)
    assert "_receive_" not in prefix
    assert "asyncio.Task" not in prefix
    # And the prefix should end at a newline (paragraph/line boundary)
    assert prefix.endswith("\n")


# ── chunk_text untouched: regression guards ─────────────────────────


def test_chunk_text_short_stays_one():
    assert wb.chunk_text("short text") == ["short text"]


def test_chunk_text_long_splits_at_newline():
    para = "para one.\n" + ("x" * 3000) + "\npara three"
    chunks = wb.chunk_text(para, limit=2000)
    assert len(chunks) >= 2
    assert all(len(c) <= 2000 for c in chunks)


# ── strip_markdown: fenced code body must survive ───────────────────


def test_strip_markdown_keeps_fenced_code_body(monkeypatch):
    """Codex review found ` ``` ... ``` ` was being deleted whole; user lost
    code output entirely. Strip fallback keeps the inner text, drops fence."""
    monkeypatch.setenv("BABATA_WEIXIN_STRIP_MD", "1")
    text = "before\n```python\nimport os\nprint('hi')\n```\nafter"
    out = wb.strip_markdown(text)
    assert "import os" in out
    assert "print('hi')" in out
    assert "```" not in out


def test_strip_markdown_keeps_unlabelled_fence(monkeypatch):
    monkeypatch.setenv("BABATA_WEIXIN_STRIP_MD", "1")
    text = "```\nplain block content\n```"
    out = wb.strip_markdown(text)
    assert "plain block content" in out
    assert "```" not in out


def test_strip_markdown_link_to_label_plus_url(monkeypatch):
    monkeypatch.setenv("BABATA_WEIXIN_STRIP_MD", "1")
    out = wb.strip_markdown("see [docs](https://example.com) end")
    assert "docs" in out
    assert "https://example.com" in out
    assert "[" not in out and "](" not in out


# ── _find_safe_split: link safety ───────────────────────────────────


def test_find_safe_split_avoid_unclosed_link():
    # Buffer ends mid-link; safe split must retract to before the `[`.
    text = "前文一句话。请看 [文档没闭合"
    pos = wb._find_safe_split(text)
    assert wb._md_balanced(text[:pos])
    assert "[文档" not in text[:pos]
    assert text[:pos] == "前文一句话。"


# ── _flush hard-cut force-emit (cut=0 escape path) ──────────────────
# This is the regression for codex's HIGH finding: pathological buf that
# never has any md-balanced prefix should still drain — accepting marker
# leak — rather than spin forever. We verify the intent at the helper level
# (full _flush requires asyncio fixtures + mock client; covered by manual
# smoke test post-deploy).


def test_md_balanced_unclosable_buf_returns_false_throughout():
    """A buf that opens with `\`` and never closes it has NO balanced prefix
    at any cut > 0; only cut=0 (empty) is balanced."""
    text = "`unclosed code that goes on and on and on with no closer"
    # No prefix length except 0 should be balanced
    assert wb._md_balanced(text[:0]) is True
    for i in range(1, len(text) + 1):
        assert wb._md_balanced(text[:i]) is False, f"prefix[:{i}] should be unbalanced"
