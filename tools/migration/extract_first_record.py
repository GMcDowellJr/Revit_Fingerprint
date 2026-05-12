#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract the first record from each domain in a fingerprint JSON payload.

Useful for comparing:
- legacy extractor output
- reformatted canonical output
- new extractor output

The script preserves the original top-level domain envelope shape:

{
  "domain_name": {
    "records": [
      { ...first record... }
    ]
  }
}

Usage:
    python extract_first_records.py input.json output.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def extract_first_records(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Top-level payload must be a JSON object")

    out: dict[str, Any] = {}

    for domain_name, domain_payload in payload.items():
        if not isinstance(domain_payload, dict):
            continue

        records = domain_payload.get("records")

        if not isinstance(records, list):
            continue

        if not records:
            continue

        first = records[0]

        out[domain_name] = {
            "records": [first]
        }

    return out


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "Usage: python extract_first_records.py input.json output.json",
            file=sys.stderr,
        )
        return 2

    input_path = Path(argv[0])
    output_path = Path(argv[1])

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        print(f"ERROR: failed to load input JSON: {exc}", file=sys.stderr)
        return 2

    try:
        out_payload = extract_first_records(payload)
    except Exception as exc:
        print(f"ERROR: failed to process payload: {exc}", file=sys.stderr)
        return 2

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out_payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(
        f"Wrote {len(out_payload)} domains to {output_path}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))