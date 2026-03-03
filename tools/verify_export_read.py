#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

# Ensure repo root is importable when running as a script.
try:
    _HERE = os.path.abspath(os.path.dirname(__file__))
    _REPO_ROOT = os.path.abspath(os.path.join(_HERE, os.pardir))
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)
except Exception:
    pass

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from tools.io_export import (
    detect_export_schema,
    get_domain_records,
    get_definition_items,
    get_id_join_hash,
    get_id_sig_hash,
    iter_domains,
)


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"JSON root is not an object: {path}")
    return data


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke-test export JSON readers")
    ap.add_argument("exports_dir", help="Directory containing export JSON files")
    ap.add_argument("--first-n", type=int, default=3, help="records per domain to probe")
    args = ap.parse_args()

    root = Path(args.exports_dir).resolve()
    files = sorted([p for p in root.glob("*.json") if p.is_file() and not p.name.lower().endswith(".legacy.json")])
    if not files:
        print(f"No JSON files found in {root}")
        return 1

    print(f"Scanning {len(files)} export files in {root}")
    for p in files:
        data = read_json(p)
        schema = detect_export_schema(data)
        domains = iter_domains(data)
        print(f"\n{p.name}: schema={schema}, domains={len(domains)}")
        for d in domains:
            recs = get_domain_records(data, d)
            print(f"  - {d}: records={len(recs)}")
            for rec in recs[: max(0, int(args.first_n))]:
                _ = get_definition_items(rec)
                _ = get_id_sig_hash(rec)
                _ = get_id_join_hash(rec)

    print("\nSmoke read complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
