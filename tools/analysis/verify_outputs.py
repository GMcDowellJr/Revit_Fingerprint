#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def load_contract(path: Path) -> dict:
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def main() -> None:
    ap = argparse.ArgumentParser(description='Smoke-check expected BI outputs in out/current.')
    ap.add_argument('--current-dir', default='out/current')
    ap.add_argument('--contract', default='assets/contracts/analysis_contract.json')
    args = ap.parse_args()

    current = Path(args.current_dir)
    contract = load_contract(Path(args.contract))
    failures = []
    for spec in contract.get('tables', []):
        rel = spec['path']
        required = spec.get('required_columns', [])
        non_zero = bool(spec.get('require_non_zero_rows', False))
        csv_path = current / rel
        if not csv_path.is_file():
            failures.append(f'missing file: {csv_path}')
            continue
        with csv_path.open('r', encoding='utf-8-sig', newline='') as f:
            rows = list(csv.DictReader(f))
        cols = set(rows[0].keys()) if rows else set()
        for c in required:
            if c not in cols:
                failures.append(f'{csv_path}: missing required column {c}')
        if non_zero and len(rows) == 0:
            failures.append(f'{csv_path}: expected non-zero rows')

    if failures:
        for f in failures:
            print(f'[verify_outputs] FAIL: {f}')
        raise SystemExit(1)
    print('[verify_outputs] PASS')


if __name__ == '__main__':
    main()
