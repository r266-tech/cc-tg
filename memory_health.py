#!/usr/bin/env python3
"""memory_health.py — structural health scanner for babata memory directory."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

# Threshold: memories should stay concise; 500 lines signals drift / bloat.
LENGTH_THRESHOLD_LINES = 500
# Hard cap for a single root fact. The 5KB guideline is soft; 8KiB means the
# note should be compressed or split before it stays in atomic memory.
BYTE_THRESHOLD = 8192
# Legal types for frontmatter.
LEGAL_TYPES = {"user", "feedback", "project", "reference"}
LINK_TARGET_PATTERN = re.compile(r"\]\(([^)]+)\)")
INDEX_COUNT_PATTERN = re.compile(r"\]\(indexes/([^)]+)\)\s*—\s*(\d+)\s*条")


def parse_frontmatter(text: str) -> dict[str, str] | None:
    """Thin YAML frontmatter parser; no external deps."""
    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    raw = text[3:end].strip()
    result: dict[str, str] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        result[key.strip()] = val.strip()
    return result


def extract_body(text: str) -> str:
    """Return text after the closing frontmatter delimiter."""
    if not text.startswith("---"):
        return text
    end = text.find("\n---", 3)
    if end == -1:
        return text
    start = end + 4
    if start < len(text) and text[start] == "\n":
        start += 1
    return text[start:]


def human_report(issues: dict[str, list[dict[str, Any]]]) -> None:
    """Pretty print grouped issues to stdout."""
    if not any(issues.values()):
        print("No structural issues found.")
        return
    for kind, items in issues.items():
        if not items:
            continue
        print(f"\n[{kind}]")
        for item in items:
            line = item.get("line", 0)
            line_str = f":{line}" if line else ""
            print(f"  {item['file']}{line_str}  {item['detail']}")


def json_report(issues: dict[str, list[dict[str, Any]]]) -> None:
    print(json.dumps(issues, indent=2, ensure_ascii=False))


def iter_markdown_links(path: Path) -> list[tuple[int, str]]:
    links: list[tuple[int, str]] = []
    for line_no, raw in enumerate(path.read_text().splitlines(), 1):
        for target in LINK_TARGET_PATTERN.findall(raw):
            target = target.strip()
            if not target or target.startswith(("#", "http://", "https://")):
                continue
            links.append((line_no, target))
    return links


def resolve_memory_link(root: Path, source: Path, target: str) -> tuple[Path, Path] | None:
    # Strip anchor fragments but keep relative paths.
    target = target.split("#", 1)[0]
    if not target:
        return None
    resolved = (source.parent / target).resolve()
    try:
        rel = resolved.relative_to(root.resolve())
    except ValueError:
        return None
    return rel, resolved


def is_root_memory_file(rel: Path) -> bool:
    return len(rel.parts) == 1 and rel.suffix == ".md" and rel.name != "MEMORY.md"


def is_index_file(rel: Path) -> bool:
    return len(rel.parts) == 2 and rel.parts[0] == "indexes" and rel.suffix == ".md"


def fix_orphans(root: Path, orphans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Do not auto-append facts to L0.

    Since memory v2, MEMORY.md is a thin router. Classification requires choosing
    an indexes/*.md category, so an automatic fix would re-inflate L0.
    """
    return [
        {
            "file": o["file"],
            "line": 0,
            "detail": "choose an indexes/*.md category manually; --fix will not append facts to MEMORY.md",
        }
        for o in orphans
    ]


def run(root: Path, *, json_mode: bool, fix_mode: bool, strict_mode: bool) -> int:
    # NOTE: missing_frontmatter intentionally absent — V's vault has meta docs
    # (e.g. babata_philosophy.md) that begin with `# title` and skip YAML
    # frontmatter on purpose. The retrieval signal that matters is the
    # MEMORY.md hook (covered by orphan / broken_link checks), not the file's
    # own frontmatter. Other frontmatter-derived checks (missing_field /
    # invalid_type / empty_body) still run, but only for files that DO have
    # frontmatter — the absence itself isn't a signal worth reporting.
    issues: dict[str, list[dict[str, Any]]] = {
        "broken_link": [],
        "orphan": [],
        "duplicate_index": [],
        "count_mismatch": [],
        "cross_dir_link": [],
        "missing_field": [],
        "invalid_type": [],
        "empty_body": [],
        "too_long": [],
    }

    # ---- Parse MEMORY.md as L0 router ----
    memory_md = root / "MEMORY.md"
    indexed: set[str] = set()
    seen_files: dict[str, int] = {}
    declared_index_counts: dict[str, tuple[int, int]] = {}

    if memory_md.exists():
        for line_no, raw in enumerate(memory_md.read_text().splitlines(), 1):
            m = INDEX_COUNT_PATTERN.search(raw)
            if m:
                declared_index_counts[m.group(1)] = (int(m.group(2)), line_no)

        for line_no, target in iter_markdown_links(memory_md):
            resolved = resolve_memory_link(root, memory_md, target)
            if resolved is None:
                issues["cross_dir_link"].append(
                    {"file": "MEMORY.md", "line": line_no, "detail": f"points outside memory root: '{target}'"}
                )
                continue
            rel, fpath = resolved

            if not fpath.exists():
                issues["broken_link"].append(
                    {"file": "MEMORY.md", "line": line_no, "detail": f"points to missing '{target}'"}
                )
                continue

            # L0 may link to root hot facts and to indexes/*.md category routers.
            if not (is_root_memory_file(rel) or is_index_file(rel)):
                issues["cross_dir_link"].append(
                    {"file": "MEMORY.md", "line": line_no, "detail": f"unexpected L0 target '{target}'"}
                )
                continue

            if is_root_memory_file(rel):
                if rel.name in seen_files:
                    issues["duplicate_index"].append(
                        {"file": "MEMORY.md", "line": line_no, "detail": f"duplicate of line {seen_files[rel.name]} for '{rel.name}'"}
                    )
                else:
                    seen_files[rel.name] = line_no
                indexed.add(rel.name)
    else:
        issues["broken_link"].append(
            {"file": "MEMORY.md", "line": 0, "detail": "MEMORY.md does not exist"}
        )

    # ---- Parse category indexes ----
    indexes_dir = root / "indexes"
    category_counts: dict[str, int] = {}
    category_seen_files: dict[str, tuple[str, int]] = {}
    if indexes_dir.is_dir():
        for index_file in sorted(indexes_dir.glob("[0-9][0-9]-*.md")):
            category_counts[index_file.name] = 0
            for line_no, target in iter_markdown_links(index_file):
                resolved = resolve_memory_link(root, index_file, target)
                if resolved is None:
                    issues["cross_dir_link"].append(
                        {"file": f"indexes/{index_file.name}", "line": line_no, "detail": f"points outside memory root: '{target}'"}
                    )
                    continue
                rel, fpath = resolved
                if not fpath.exists():
                    issues["broken_link"].append(
                        {"file": f"indexes/{index_file.name}", "line": line_no, "detail": f"points to missing '{target}'"}
                    )
                    continue
                if is_root_memory_file(rel):
                    category_counts[index_file.name] += 1
                    if rel.name in category_seen_files:
                        prev_file, prev_line = category_seen_files[rel.name]
                        issues["duplicate_index"].append(
                            {
                                "file": f"indexes/{index_file.name}",
                                "line": line_no,
                                "detail": f"'{rel.name}' also indexed at indexes/{prev_file}:{prev_line}",
                            }
                        )
                    else:
                        category_seen_files[rel.name] = (index_file.name, line_no)
                    indexed.add(rel.name)
                else:
                    issues["cross_dir_link"].append(
                        {"file": f"indexes/{index_file.name}", "line": line_no, "detail": f"unexpected category target '{target}'"}
                    )

        for index_name, actual_count in category_counts.items():
            declared = declared_index_counts.get(index_name)
            if declared is None:
                issues["count_mismatch"].append(
                    {"file": f"indexes/{index_name}", "line": 0, "detail": "missing count entry in MEMORY.md"}
                )
                continue
            declared_count, memory_line = declared
            if declared_count != actual_count:
                issues["count_mismatch"].append(
                    {
                        "file": "MEMORY.md",
                        "line": memory_line,
                        "detail": f"{index_name} declares {declared_count} 条 but index has {actual_count}",
                    }
                )

    # ---- Scan root-level memory files ----
    root_md_files: list[Path] = []
    for p in root.iterdir():
        if p.is_file() and p.suffix == ".md" and p.name != "MEMORY.md":
            root_md_files.append(p)

    # Check 2: root files not indexed in MEMORY.md or indexes/*.md
    orphans: list[dict[str, Any]] = []
    for p in root_md_files:
        if p.name not in indexed:
            orphans.append({"file": p.name, "line": 0, "detail": "exists but is not linked from MEMORY.md or indexes/*.md"})
            issues["orphan"].append(orphans[-1])

    # ---- Per-file checks ----
    for p in root_md_files:
        text = p.read_text()
        lines = text.splitlines()

        fm = parse_frontmatter(text)
        if fm is not None:
            # Check 4: required fields + type enum (only when frontmatter exists)
            for field in ("name", "description", "type"):
                if field not in fm or not fm[field]:
                    issues["missing_field"].append(
                        {"file": p.name, "line": 0, "detail": f"frontmatter missing required field '{field}'"}
                    )
            if "type" in fm and fm["type"] not in LEGAL_TYPES:
                issues["invalid_type"].append(
                    {"file": p.name, "line": 0, "detail": f"invalid type '{fm['type']}' (legal: {', '.join(sorted(LEGAL_TYPES))})"}
                )

            # Extra check: empty body after frontmatter signals placeholder / incomplete note.
            body = extract_body(text)
            if not body.strip():
                issues["empty_body"].append(
                    {"file": p.name, "line": 0, "detail": "no content after frontmatter"}
                )
        # else: meta doc without frontmatter — only universal length check below applies.

        # Check 5: excessive length
        byte_len = len(text.encode())
        if len(lines) > LENGTH_THRESHOLD_LINES or byte_len > BYTE_THRESHOLD:
            issues["too_long"].append(
                {
                    "file": p.name,
                    "line": 0,
                    "detail": f"{len(lines)} lines / {byte_len} bytes exceeds {LENGTH_THRESHOLD_LINES} lines or {BYTE_THRESHOLD} bytes",
                }
            )

    # ---- Fix mode ----
    if fix_mode and orphans:
        new_issues = fix_orphans(root, orphans)
        for ni in new_issues:
            issues.setdefault("fix_failure", []).append(ni)

    # ---- Output ----
    if json_mode:
        # Strip empty groups for brevity
        json_issues = {k: v for k, v in issues.items() if v}
        json_report(json_issues)
    else:
        human_report(issues)

    total = sum(len(v) for v in issues.values())
    if strict_mode and total > 0:
        return 1
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Structural health scanner for babata memory directory")
    parser.add_argument("--root", required=True, help="Path to memory directory")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    parser.add_argument("--fix", action="store_true", help="Reserved for safe fixes; does not auto-append facts to MEMORY.md")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any issue found")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.is_dir():
        print(f"error: --root '{root}' is not a directory", file=sys.stderr)
        sys.exit(2)

    code = run(root, json_mode=args.json, fix_mode=args.fix, strict_mode=args.strict)
    sys.exit(code)


if __name__ == "__main__":
    main()
