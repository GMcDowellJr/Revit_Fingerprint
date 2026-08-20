#!/usr/bin/env python3
"""Verify audit_results references in tracked text resolve deterministically."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = re.compile(r"audit_results(?:/[A-Za-z0-9_.-]+)?")


def tracked_files() -> list[str]:
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode(
        "utf-8", "surrogateescape"
    )
    return [path for path in output.split("\0") if path]


def main() -> int:
    broken: list[str] = []
    references = 0
    for relative in tracked_files():
        path = ROOT / relative
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for line_number, line in enumerate(text.splitlines(), 1):
            for match in REFERENCE.finditer(line):
                value = match.group(0)
                references += 1
                # Bare directory names describe the retained collection. Markdown
                # section text after a real filename is outside this match.
                if value == "audit_results" or (ROOT / value).is_file():
                    continue
                broken.append(f"{relative}:{line_number}: {value}")
    if broken:
        print("Broken audit_results references:")
        print("\n".join(broken))
        return 1
    print(f"OK: {references} audit_results references in tracked text; 0 broken")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
