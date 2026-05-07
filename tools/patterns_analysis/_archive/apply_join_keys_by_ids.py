# tools/patterns_analysis/apply_join_keys_by_ids.py
"""Apply IDS-scoped join-key policies to compute join_hash_ids (verification output).

Non-destructive: writes a CSV; does not modify export JSONs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .io import load_exports, get_domain_records, load_records_records_with_identity


def md5_utf8_join_pipe(parts: List[str]) -> str:
    s = "|".join(parts).encode("utf-8")
    return hashlib.md5(s).hexdigest()


def extract_identity_map(record: Dict[str, Any]) -> Dict[str, Any]:
    ib = record.get("identity_basis")
    if not isinstance(ib, dict):
        return {}
    items = ib.get("items")
    if not isinstance(items, list):
        return {}
    out: Dict[str, Any] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        k = it.get("k")
        if not k:
            continue
        out[str(k)] = it.get("v")
    return out


def compute_join_hash(
    record: Dict[str, Any],
    required_keys: List[str],
) -> (Optional[str], List[str]):
    imap = extract_identity_map(record)
    missing: List[str] = []
    for k in required_keys:
        if k not in imap or imap[k] is None:
            missing.append(k)
    if missing:
        return None, missing
    parts = [f"{k}={str(imap[k])}" for k in required_keys]
    return md5_utf8_join_pipe(parts), []


def apply_join_keys_by_ids(
    exports_dir: str,
    domain: str,
    file_to_ids_csv: str,
    join_key_policy_json: str,
    out_csv: str,
    *,
    phase0_dir: Optional[str] = None,
) -> None:
    map_df = pd.read_csv(file_to_ids_csv)
    file_to_ids = {str(r["file_id"]): str(r["ids_id"]) for _, r in map_df.iterrows()}

    with open(join_key_policy_json, "r", encoding="utf-8") as f:
        policy_doc = json.load(f)

    policies = policy_doc.get("policies", {})
    if not isinstance(policies, dict):
        raise ValueError("join_key_policy_json missing 'policies' dict")

    rows: List[Dict[str, Any]] = []

    if phase0_dir:
        allowed_files = set(file_to_ids.keys())
        all_recs = load_records_records_with_identity(phase0_dir, domain, allowed_file_ids=allowed_files)

        # Index required_keys by ids_id for quick lookup
        req_by_ids: Dict[str, List[str]] = {}
        for ids_id, pol in policies.items():
            rk = pol.get("required_keys", [])
            req_by_ids[str(ids_id)] = rk if isinstance(rk, list) else []

        for r in all_recs:
            fid = str(r.get("_file_id") or r.get("file_id") or "")
            if not fid or fid not in file_to_ids:
                continue
            ids_id = file_to_ids[fid]
            required_keys = req_by_ids.get(ids_id, [])

            record_id = r.get("record_id")
            sig_hash = r.get("sig_hash")
            jh, missing = compute_join_hash(r, required_keys)

            rows.append(
                {
                    "file_id": fid,
                    "ids_id": ids_id,
                    "record_id": record_id,
                    "sig_hash": sig_hash,
                    "join_hash_ids": jh,
                    "missing_required_keys": ";".join(missing),
                    "required_keys": ";".join(required_keys),
                }
            )
    else:
        for exp in load_exports(exports_dir, max_files=None):
            fid = str(exp.file_id)
            if fid not in file_to_ids:
                continue
            ids_id = file_to_ids[fid]
            ids_policy = policies.get(ids_id, {})
            required_keys = ids_policy.get("required_keys", [])
            if not isinstance(required_keys, list):
                required_keys = []

            recs = get_domain_records(exp.data, domain)
            for r in recs:
                record_id = r.get("record_id")
                sig_hash = r.get("sig_hash")
                jh, missing = compute_join_hash(r, required_keys)

                rows.append(
                    {
                        "file_id": fid,
                        "ids_id": ids_id,
                        "record_id": record_id,
                        "sig_hash": sig_hash,
                        "join_hash_ids": jh,
                        "missing_required_keys": ";".join(missing),
                        "required_keys": ";".join(required_keys),
                    }
                )

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "file_id",
                "ids_id",
                "record_id",
                "sig_hash",
                "join_hash_ids",
                "missing_required_keys",
                "required_keys",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print("[INFO] join_hash_ids CSV written:")
    print(f"  {os.path.abspath(out_csv)}")
    print(f"  Rows: {len(rows)}")


def main() -> None:
    p = argparse.ArgumentParser(description="Apply IDS-scoped join-keys and write join_hash_ids CSV")
    p.add_argument(
        "exports_dir",
        help="Directory containing fingerprint exports (*.json). Ignored if --phase0-dir is provided.",
    )
    p.add_argument(
        "--phase0-dir",
        dest="phase0_dir",
        default=None,
        help="If provided, read v2.1 Phase0 tables from this directory (results/records).",
    )
    p.add_argument("--domain", required=True, help="Domain (use text_types for verification)")
    p.add_argument("--file-to-ids", required=True, dest="file_to_ids_csv", help="Path to <domain>.file_to_ids.v1.csv")
    p.add_argument("--policy", required=True, dest="policy_json", help="Path to <domain>.join_key_policy_by_ids.v1.json")
    p.add_argument("--out", required=True, dest="out_csv", help="Output CSV path")
    args = p.parse_args()

    apply_join_keys_by_ids(
        exports_dir=args.exports_dir,
        domain=args.domain,
        file_to_ids_csv=args.file_to_ids_csv,
        join_key_policy_json=args.policy_json,
        out_csv=args.out_csv,
        phase0_dir=args.phase0_dir,
    )


if __name__ == "__main__":
    main()
