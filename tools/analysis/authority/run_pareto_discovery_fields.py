#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import sys

# Ensure repo_root and repo_root/tools are on sys.path so imports work no matter where invoked from.
_THIS = Path(__file__).resolve()
_REPO_ROOT = _THIS.parents[2]  # .../tools/phase2_analysis/<file> -> repo root
_TOOLS_DIR = _REPO_ROOT / "tools"
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_TOOLS_DIR))

try:
    # repo-local when invoked from root
    from join_key_discovery.eval import build_identity_index, build_kv_index, score_candidate_kv
    from tools.analysis.authority.io import _read_csv_rows, load_phase0_v21_feature_items, load_phase0_v21_stratum_features
except ModuleNotFoundError:
    # package-local when invoked from tools/
    from join_key_discovery.eval import build_identity_index, build_kv_index, score_candidate_kv

try:
    from tools.analysis.authority.io import _read_csv_rows, load_phase0_v21_feature_items, load_phase0_v21_stratum_features
except ModuleNotFoundError:
    from tools.analysis.authority.io import _read_csv_rows, load_phase0_v21_feature_items, load_phase0_v21_stratum_features


def pareto_front(rows: List[dict], objectives: Sequence[str]) -> List[dict]:
    """Minimize objectives; standard non-dominated filtering."""
    out: List[dict] = []
    for r in rows:
        dominated = False
        for s in rows:
            if s is r:
                continue
            if all(float(s[o]) <= float(r[o]) for o in objectives) and any(float(s[o]) < float(r[o]) for o in objectives):
                dominated = True
                break
        if not dominated:
            out.append(r)
    return out

def _list_domains(phase0_dir: str) -> List[str]:
    p = Path(phase0_dir) / "phase0_records.csv"
    if not p.is_file():
        raise FileNotFoundError(f"phase0_records.csv not found: {p}")
    domains = sorted({(r.get("domain") or "").strip() for r in _read_csv_rows(str(p)) if (r.get("domain") or "").strip()}, key=str.lower)
    return domains
    
def _load_records(phase0_dir: str, domain: str) -> List[Dict[str, str]]:
    p = Path(phase0_dir) / "phase0_records.csv"
    if not p.is_file():
        raise FileNotFoundError(f"phase0_records.csv not found: {p}")
    out: List[Dict[str, str]] = []
    for r in _read_csv_rows(str(p)):
        if r.get("domain", "") == domain:
            out.append(r)
    return out


def _load_identity_items(phase0_dir: str, domain: str) -> List[Dict[str, str]]:
    p = Path(phase0_dir) / "phase0_identity_items.csv"
    if not p.is_file():
        raise FileNotFoundError(f"phase0_identity_items.csv not found: {p}")
    out: List[Dict[str, str]] = []
    for r in _read_csv_rows(str(p)):
        if r.get("domain", "") == domain:
            out.append(r)
    return out


def _extract_stratum_discriminator_rows(stratum_rows: List[Dict[str, str]], domain: str) -> List[Dict[str, str]]:
    """Explode discriminators_json into (record_pk, k, q, v) rows."""
    out: List[Dict[str, str]] = []
    for r in stratum_rows:
        if r.get("domain", "") != domain:
            continue
        record_pk = (r.get("record_pk") or "").strip()
        if not record_pk:
            continue
        disc_json = (r.get("discriminators_json") or "").strip()
        if not disc_json:
            continue
        try:
            discs = json.loads(disc_json)
        except Exception:
            continue
        if not isinstance(discs, list):
            continue
        for d in discs:
            if not isinstance(d, dict):
                continue
            k = (d.get("k") or "").strip()
            if not k:
                continue
            out.append({
                "record_pk": record_pk,
                "item_key": k,
                "q": (d.get("q") or "ok"),
                "v": ("" if d.get("v") is None else str(d.get("v"))),
            })
    return out


def _candidate_keys_from_rows(rows: List[Dict[str, str]], key_col: str) -> List[str]:
    keys = sorted({(r.get(key_col) or "").strip() for r in rows if (r.get(key_col) or "").strip()}, key=str.lower)
    return keys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase0-dir", required=True)
    ap.add_argument("--domain", nargs="*", default=None, help="One or more domains. If omitted, runs all domains found in phase0_records.csv")
    ap.add_argument("--source", choices=["identity", "features", "stratum_discriminators"], default="features")
    ap.add_argument("--max-k", type=int, default=4)
    ap.add_argument("--type-allow", default="b,i,s", help="for features source only; comma list of allowed t values")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    phase0_dir = str(args.phase0_dir)
    
    domains_arg = args.domain
    if domains_arg is None or (isinstance(domains_arg, list) and len(domains_arg) == 0):
        domains = _list_domains(phase0_dir)
    else:
        domains = [str(d) for d in domains_arg if str(d).strip()]
        domains = sorted(set(domains), key=str.lower)

    all_payloads: List[dict] = []

    for domain in domains:
        records = _load_records(phase0_dir, domain)

        # Build kv rows + index depending on source
        if args.source == "identity":
            id_rows = _load_identity_items(phase0_dir, domain)
            kv_index = build_identity_index(id_rows)
            candidate_keys = sorted(
                {(r.get("item_key") or r.get("k") or "").strip() for r in id_rows if (r.get("item_key") or r.get("k") or "").strip()},
                key=str.lower
            )
        elif args.source == "features":
            feat_rows = [r for r in load_phase0_v21_feature_items(phase0_dir) if r.get("domain", "") == domain]
            allow = {x.strip() for x in str(args.type_allow).split(",") if x.strip()}
            if allow:
                feat_rows = [r for r in feat_rows if (r.get("feature_type") or r.get("t") or "").strip() in allow]

            # Map columns to generic kv rows
            kv_rows = []
            for r in feat_rows:
                kv_rows.append({
                    "record_pk": (r.get("record_pk") or "").strip(),
                    "item_key": (r.get("feature_key") or "").strip(),
                    "q": (r.get("feature_quality") or r.get("q") or "ok"),
                    "v": (r.get("feature_value") or r.get("v") or ""),
                })
            kv_index = build_kv_index(kv_rows, record_pk_col="record_pk", key_col="item_key", q_col="q", v_col="v")
            candidate_keys = _candidate_keys_from_rows(kv_rows, "item_key")
        else:
            strata_rows = load_phase0_v21_stratum_features(phase0_dir)
            kv_rows = _extract_stratum_discriminator_rows(strata_rows, domain)
            kv_index = build_kv_index(kv_rows, record_pk_col="record_pk", key_col="item_key", q_col="q", v_col="v")
            candidate_keys = _candidate_keys_from_rows(kv_rows, "item_key")

        # Pareto search
        max_k = max(1, int(args.max_k))
        fields = candidate_keys
        rows_out: List[dict] = []

        for k in range(1, min(max_k, len(fields)) + 1):
            for subset in itertools.combinations(fields, k):
                metrics = score_candidate_kv(records, kv_index, list(subset), cfg={"max_k": max_k})
                rows_out.append({
                    "keys": "|".join(subset),
                    "k_count": k,
                    "collision_rate": float(metrics.get("collision_rate", 1.0)),
                    "coverage_gap": 1.0 - float(metrics.get("coverage", 0.0)),
                    "fragmentation_rate": float(metrics.get("fragmentation_rate", 1.0)),
                    "metrics": metrics,
                })

        if not rows_out:
            payload = {"domain": domain, "source": args.source, "frontier": [], "chosen": None}
        else:
            front = pareto_front(rows_out, ["coverage_gap", "collision_rate", "fragmentation_rate", "k_count"])
            front = sorted(front, key=lambda r: (r["collision_rate"], r["coverage_gap"], r["k_count"], r["keys"]))
            payload = {"domain": domain, "source": args.source, "frontier": front, "chosen": front[0]}

        all_payloads.append(payload)

    # Output behavior:
    # - single domain: emit object
    # - multi domain: emit list
    out_obj = all_payloads[0] if len(all_payloads) == 1 else all_payloads

    out_path = args.out.strip()

    # Output behavior:
    # - If out_path is a directory (existing or ends with a separator), write one JSON per domain.
    # - Otherwise, write a single JSON file (object for single domain, list for multi-domain).
    if out_path:
        out_p = Path(out_path)

        is_dir_hint = str(out_path).endswith(("/", "\\"))
        is_dir = out_p.is_dir() or is_dir_hint

        if is_dir:
            out_p.mkdir(parents=True, exist_ok=True)
            for payload in all_payloads:
                dom = payload.get("domain", "unknown")
                src = payload.get("source", "unknown")
                safe_dom = "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in str(dom))
                safe_src = "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in str(src))
                fp = out_p / f"pareto_{safe_dom}__{safe_src}.json"
                fp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        else:
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_text(json.dumps(out_obj, indent=2, sort_keys=True), encoding="utf-8")
    else:
        print(json.dumps(out_obj, indent=2, sort_keys=True))

if __name__ == "__main__":
    raise SystemExit(main())