# -*- coding: utf-8 -*-
"""
Compute drift score of a target run versus a baseline using stable surfaces.

Inputs may be:
- full payload JSON (preferred; supports counts via _features builder)
- manifest JSON
- features JSON

Default scoring (simple, deterministic):
- status penalty: ok=0, degraded=1, blocked/failed/unsupported=3
- hash mismatch: +1
- count delta (if both counts present): + min(1, abs(a-b)/max(b,1))

Exit codes:
  0 = computed successfully
  2 = invalid input
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

from core.features import build_features
from core.manifest import build_manifest


def _load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise ValueError("missing file: {}".format(path))
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _to_features(obj: Any) -> Dict[str, Any]:
    d = _as_dict(obj)
    if isinstance(d.get("_features", None), dict):
        return d["_features"]
    # If it already looks like features (has domains with status/hash), pass through.
    if isinstance(d.get("domains", None), dict) and "run_status" in d:
        return d
    # Otherwise derive from payload/contract.
    return build_features(d)


def _to_manifest(obj: Any) -> Dict[str, Any]:
    d = _as_dict(obj)
    if isinstance(d.get("_manifest", None), dict):
        return d["_manifest"]
    if isinstance(d.get("domains", None), dict) and "run_status" in d and "schema_version" in d:
        return d
    return build_manifest(d)


def _status_penalty(status: Any) -> int:
    s = str(status) if status is not None else ""
    if s == "ok":
        return 0
    if s == "degraded":
        return 1
    # blocked/failed/unsupported/missing -> heavier penalty
    if s in ("blocked", "failed", "unsupported"):
        return 3
    # Unknown statuses should not silently score as ok
    return 3


def _norm_count_delta(a: Optional[int], b: Optional[int]) -> float:
    if a is None or b is None:
        return 0.0
    denom = float(max(int(b), 1))
    return min(1.0, abs(float(a) - float(b)) / denom)


def score_drift(baseline_obj: Dict[str, Any], target_obj: Dict[str, Any]) -> Dict[str, Any]:
    bm = _to_manifest(baseline_obj)
    tm = _to_manifest(target_obj)

    bf = _to_features(baseline_obj)
    tf = _to_features(target_obj)

    b_domains = _as_dict(bm.get("domains", None))
    t_domains = _as_dict(tm.get("domains", None))

    bF_domains = _as_dict(bf.get("domains", None))
    tF_domains = _as_dict(tf.get("domains", None))

    names = sorted(set(b_domains.keys()) | set(t_domains.keys()))

    per_domain: Dict[str, Any] = {}
    total = 0.0

    for n in names:
        be = _as_dict(b_domains.get(n, None))
        te = _as_dict(t_domains.get(n, None))

        b_status = be.get("status", None)
        t_status = te.get("status", None)

        b_hash = be.get("hash", None)
        t_hash = te.get("hash", None)

        # counts from features surface (preferred)
        bfd = _as_dict(bF_domains.get(n, None))
        tfd = _as_dict(tF_domains.get(n, None))
        b_count = bfd.get("count", None)
        t_count = tfd.get("count", None)

        status_part = float(_status_penalty(t_status) - _status_penalty(b_status))
        # Status drift should never reduce score below 0 (baseline worse should not "credit" target).
        status_part = max(0.0, status_part)

        hash_part = 1.0 if (b_hash is not None and t_hash is not None and str(b_hash) != str(t_hash)) else 0.0

        # count delta compared to baseline (only adds; never subtracts)
        try:
            bc = int(b_count) if b_count is not None else None
        except Exception:
            bc = None
        try:
            tc = int(t_count) if t_count is not None else None
        except Exception:
            tc = None

        count_part = _norm_count_delta(tc, bc)

        score = status_part + hash_part + count_part
        total += score

        per_domain[n] = {
            "baseline": {"status": b_status, "hash": b_hash, "count": bc},
            "target": {"status": t_status, "hash": t_hash, "count": tc},
            "parts": {"status": status_part, "hash": hash_part, "count": count_part},
            "score": score,
        }

    return {
        "baseline": {
            "schema_version": bm.get("schema_version", None),
            "hash_mode": bm.get("hash_mode", None),
            "run_status": bm.get("run_status", None),
        },
        "target": {
            "schema_version": tm.get("schema_version", None),
            "hash_mode": tm.get("hash_mode", None),
            "run_status": tm.get("run_status", None),
        },
        "drift_total": total,
        "drift_by_domain": per_domain,
    }


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("baseline", help="Baseline JSON (payload/manifest/features)")
    ap.add_argument("target", help="Target JSON (payload/manifest/features)")
    ap.add_argument("--out", default="", help="Write JSON result to this path")
    args = ap.parse_args(argv)

    try:
        b = _load_json(args.baseline)
        t = _load_json(args.target)
    except Exception as e:
        sys.stderr.write("ERROR: {}\n".format(str(e)))
        return 2

    try:
        result = score_drift(b, t)
    except Exception as e:
        sys.stderr.write("ERROR: drift scoring failed: {}\n".format(str(e)))
        return 2

    txt = json.dumps(result, indent=2, sort_keys=True)
    if args.out:
        try:
            parent = os.path.dirname(args.out)
            if parent and not os.path.exists(parent):
                os.makedirs(parent)
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(txt)
                f.write("\n")
        except Exception as e:
            sys.stderr.write("ERROR: could not write --out: {}\n".format(str(e)))
            return 2
    else:
        sys.stdout.write(txt)
        sys.stdout.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
