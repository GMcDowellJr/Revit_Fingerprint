from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .io import load_exports, get_domain_records
from .report import write_json_report


def _get_join_hash(record: Dict[str, Any]) -> Optional[str]:
    jk = record.get("join_key")
    if not isinstance(jk, dict):
        return None
    h = jk.get("join_hash")
    if h is None:
        return None
    try:
        hs = str(h).strip()
    except Exception:
        return None
    return hs or None


def _stable_json(x: Any, *, max_chars: int = 600) -> str:
    try:
        s = json.dumps(x, sort_keys=True, ensure_ascii=False)
    except Exception:
        try:
            s = str(x)
        except Exception:
            s = "<unserializable>"
    if len(s) > max_chars:
        return s[: max_chars - 12] + "...<truncated>"
    return s


def _is_scalar(x: Any) -> bool:
    return x is None or isinstance(x, (str, int, float, bool))


def _extract_qv_from_value(v: Any) -> Tuple[str, Optional[str]]:
    """
    Normalize a value into (q, v_str) while preserving explicit states when present.

    Recognized patterns:
      - scalar -> ("ok", str(v))
      - {"q": "...", "v": ...} -> (q, str(v) or None)
      - {"display": "..."} / {"value": "..."} / {"name": "..."} -> ("ok", str(...))
      - unknown dict / non-scalar -> ("unreadable", None)
    """
    if _is_scalar(v):
        if v is None:
            return "ok", None
        try:
            return "ok", str(v)
        except Exception:
            return "ok", None

    if isinstance(v, dict):
        q = v.get("q")
        if isinstance(q, str) and q.strip():
            qs = q.strip()
            vv = v.get("v")
            if vv is None:
                return qs, None
            try:
                return qs, str(vv)
            except Exception:
                return qs, None

        for k in ("display", "value", "name"):
            if k in v and v.get(k) is not None:
                try:
                    return "ok", str(v.get(k))
                except Exception:
                    return "ok", None

        return "unreadable", None

    return "unreadable", None


def _phase2_bucket_items(record: Dict[str, Any], bucket: str) -> List[Dict[str, Any]]:
    p2 = record.get("phase2")
    if not isinstance(p2, dict):
        return []
    items = p2.get(bucket)
    if not isinstance(items, list):
        return []
    out = []
    for it in items:
        if isinstance(it, dict):
            out.append(it)
    return out


def _iter_record_parameters(
    record: Dict[str, Any],
    *,
    include_top_level: bool,
    include_phase2_semantic: bool,
    include_phase2_cosmetic: bool,
    include_phase2_coordination: bool,
    include_phase2_unknown: bool,
) -> Tuple[List[Tuple[str, str, Optional[str]]], int]:
    """
    Yield (param_key, q, v) observations for a single record.
    Returns (observations, ambiguous_duplicate_k_count).
    """
    obs: List[Tuple[str, str, Optional[str]]] = []
    ambiguous_dup_k = 0

    # Top-level: include scalar fields and dict fields (q/v or display/value/name)
    if include_top_level:
        for k, v in record.items():
            if k in ("join_key", "phase2", "records"):
                continue
            # keep bounded: only scalar or dict; lists/large objects are represented as type marker
            if _is_scalar(v) or isinstance(v, dict):
                q, vv = _extract_qv_from_value(v)
                obs.append((f"top.{k}", q, vv))
            else:
                obs.append((f"top.{k}", "unreadable", None))

    # Phase2 buckets
    def emit_bucket(bucket: str, prefix: str) -> None:
        nonlocal ambiguous_dup_k
        seen: Set[str] = set()
        for it in _phase2_bucket_items(record, bucket):
            kk = it.get("k")
            if kk is None:
                continue
            try:
                ks = str(kk)
            except Exception:
                continue
            if ks in seen:
                ambiguous_dup_k += 1
                continue
            seen.add(ks)

            q = it.get("q")
            v = it.get("v")
            try:
                qs = "" if q is None else str(q)
            except Exception:
                qs = ""
            if v is None:
                vs = None
            else:
                try:
                    vs = str(v)
                except Exception:
                    vs = None
            obs.append((f"{prefix}.{ks}", qs, vs))

    if include_phase2_semantic:
        emit_bucket("semantic_items", "p2.semantic")
    if include_phase2_cosmetic:
        emit_bucket("cosmetic_items", "p2.cosmetic")
    if include_phase2_coordination:
        emit_bucket("coordination_items", "p2.coordination")
    if include_phase2_unknown:
        emit_bucket("unknown_items", "p2.unknown")

    return obs, ambiguous_dup_k


def run_joinhash_parameter_population(
    *,
    exports_dir: str,
    domain: str,
    out_dir: str,
    include_top_level: bool,
    include_semantic: bool,
    include_cosmetic: bool,
    include_coordination: bool,
    include_unknown: bool,
    max_cell_chars: int,
) -> None:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    os.makedirs(out_dir, exist_ok=True)

    # join_hash totals (records, files)
    jh_records_total = defaultdict(int)       # jh -> record count
    jh_files: Dict[str, Set[str]] = defaultdict(set)  # jh -> files where present

    # join_hash + param presence counts
    jhp_records_with = defaultdict(int)        # (jh,param) -> records where param observed
    jhp_files_with: Dict[Tuple[str, str], Set[str]] = defaultdict(set)

    # join_hash + param + (q,v) variants
    jhpv_records = defaultdict(int)            # (jh,param,q,v) -> record count
    jhpv_files: Dict[Tuple[str, str, str, Optional[str]], Set[str]] = defaultdict(set)

    unjoinable_records_total = 0
    ambiguous_dup_k_total = 0
    records_total = 0

    for e in exports:
        recs = get_domain_records(e.data, domain)
        records_total += len(recs)

        for r in recs:
            if not isinstance(r, dict):
                continue
            jh = _get_join_hash(r)
            if jh is None:
                unjoinable_records_total += 1
                continue

            jh_records_total[jh] += 1
            jh_files[jh].add(e.file_id)

            obs, amb = _iter_record_parameters(
                r,
                include_top_level=include_top_level,
                include_phase2_semantic=include_semantic,
                include_phase2_cosmetic=include_cosmetic,
                include_phase2_coordination=include_coordination,
                include_phase2_unknown=include_unknown,
            )
            ambiguous_dup_k_total += amb

            # de-dupe params per record: if the same param_key appears twice (top-level shouldn’t; phase2 may via dup k)
            # We already exclude dup k per bucket; top-level keys are unique.
            per_record_seen: Set[str] = set()
            for param_key, q, v in obs:
                if param_key in per_record_seen:
                    continue
                per_record_seen.add(param_key)

                jhp_records_with[(jh, param_key)] += 1
                jhp_files_with[(jh, param_key)].add(e.file_id)

                jhpv_records[(jh, param_key, q, v)] += 1
                jhpv_files[(jh, param_key, q, v)].add(e.file_id)

    # Output 1: full population rows (jh, param, q, v)
    pop_csv = os.path.join(out_dir, f"{domain}.joinhash_parameter_population.csv")
    with open(pop_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["domain", "join_hash", "param_key", "q", "v", "records_count", "files_count"])
        keys = list(jhpv_records.keys())
        keys.sort(
            key=lambda k: (
                -len(jhpv_files[k]),
                -jhpv_records[k],
                k[0],  # join_hash
                k[1],  # param_key
                k[2],  # q
                "" if k[3] is None else k[3],
            )
        )
        for (jh, pk, q, v) in keys:
            w.writerow([
                domain,
                jh,
                pk,
                q,
                "" if v is None else v,
                int(jhpv_records[(jh, pk, q, v)]),
                int(len(jhpv_files[(jh, pk, q, v)])),
            ])

    # Output 2: summary per (jh, param)
    # includes explicit missing counts derived from totals
    summary_csv = os.path.join(out_dir, f"{domain}.joinhash_parameter_summary.csv")
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "domain",
            "join_hash",
            "param_key",
            "join_hash_records_total",
            "join_hash_files_total",
            "records_with_param",
            "records_missing_param",
            "files_with_param",
            "files_missing_param",
            "distinct_variant_count",
        ])

        # build distinct variant counts per (jh,param)
        distinct_counts = defaultdict(int)
        for (jh, pk, q, v) in jhpv_records.keys():
            distinct_counts[(jh, pk)] += 1

        all_jhp = set(jhp_records_with.keys())
        # also include params that never observed? not possible without a param universe; keep explicit scope.
        rows = list(all_jhp)
        rows.sort(key=lambda t: (t[0], t[1]))

        for (jh, pk) in rows:
            rec_total = int(jh_records_total[jh])
            file_total = int(len(jh_files[jh]))
            rec_with = int(jhp_records_with[(jh, pk)])
            file_with = int(len(jhp_files_with[(jh, pk)]))
            w.writerow([
                domain,
                jh,
                pk,
                rec_total,
                file_total,
                rec_with,
                rec_total - rec_with,
                file_with,
                file_total - file_with,
                int(distinct_counts[(jh, pk)]),
            ])

    # Output 3: rolled-up per parameter (population-level variability)
    # purely counts: how often it varies within join_hashes, and how often it's missing within join_hashes
    param_summary_csv = os.path.join(out_dir, f"{domain}.parameter_variability_summary.csv")
    with open(param_summary_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "domain",
            "param_key",
            "join_hash_count_present",
            "join_hash_count_variable_distinct_gt_1",
            "join_hash_count_missing_in_some_records",
            "join_hash_count_missing_in_some_files",
        ])

        # re-use distinct_counts from above
        # compute missing flags per (jh,pk)
        missing_records_flag = defaultdict(int)
        missing_files_flag = defaultdict(int)
        for (jh, pk), rec_with in jhp_records_with.items():
            rec_total = int(jh_records_total[jh])
            if rec_total - int(rec_with) > 0:
                missing_records_flag[(jh, pk)] = 1
            file_total = int(len(jh_files[jh]))
            file_with = int(len(jhp_files_with[(jh, pk)]))
            if file_total - file_with > 0:
                missing_files_flag[(jh, pk)] = 1

        by_param: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        for (jh, pk) in all_jhp:
            by_param[pk].append((jh, pk))

        for pk in sorted(by_param.keys()):
            pairs = by_param[pk]
            jh_present = len({jh for (jh, _) in pairs})

            jh_var = 0
            jh_mr = 0
            jh_mf = 0
            for (jh, _) in pairs:
                if distinct_counts[(jh, pk)] > 1:
                    jh_var += 1
                if missing_records_flag[(jh, pk)]:
                    jh_mr += 1
                if missing_files_flag[(jh, pk)]:
                    jh_mf += 1

            w.writerow([domain, pk, jh_present, jh_var, jh_mr, jh_mf])

    report = {
        "phase": "patterns_analysis",
        "analysis": "joinhash_parameter_population",
        "domain": domain,
        "files_total": len(exports),
        "counts": {
            "records_total": int(records_total),
            "unjoinable_records_total": int(unjoinable_records_total),
            "distinct_join_hash_count": int(len(jh_records_total)),
            "distinct_joinhash_param_pairs": int(len(jhp_records_with)),
            "distinct_joinhash_param_qv_pairs": int(len(jhpv_records)),
            "ambiguous_duplicate_k_total": int(ambiguous_dup_k_total),
        },
        "inputs": {
            "include_top_level": bool(include_top_level),
            "include_phase2_semantic": bool(include_semantic),
            "include_phase2_cosmetic": bool(include_cosmetic),
            "include_phase2_coordination": bool(include_coordination),
            "include_phase2_unknown": bool(include_unknown),
            "max_cell_chars": int(max_cell_chars),
        },
        "outputs": {
            "population_csv": os.path.abspath(pop_csv),
            "joinhash_param_summary_csv": os.path.abspath(summary_csv),
            "parameter_variability_summary_csv": os.path.abspath(param_summary_csv),
        },
        "assumptions": {
            "join_key": "record.join_key.join_hash",
            "scope": "descriptive frequencies; no normalization; no winner selection; collisions included",
            "top_level_rule": "top.* includes scalar/dict fields; lists are treated as unreadable type markers",
            "phase2_rule": "p2.<bucket>.<k> from phase2 item lists; duplicate k within bucket counted as ambiguous and excluded",
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.joinhash_parameter_population.report.json")
    write_json_report(out_path=json_path, report=report)

    print("join_hash × parameter population written:")
    print(f"  {pop_csv}")
    print(f"  {summary_csv}")
    print(f"  {param_summary_csv}")
    print(f"  {json_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase-2: join_hash × parameter population (generic)")
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    p.add_argument("--include-top-level", action="store_true", default=True)
    p.add_argument("--no-top-level", action="store_false", dest="include_top_level")
    p.add_argument("--include-semantic", action="store_true", default=True)
    p.add_argument("--no-semantic", action="store_false", dest="include_semantic")
    p.add_argument("--include-cosmetic", action="store_true", default=False)
    p.add_argument("--include-coordination", action="store_true", default=False)
    p.add_argument("--include-unknown", action="store_true", default=False)
    p.add_argument("--max-cell-chars", type=int, default=600, dest="max_cell_chars")
    ns = p.parse_args()

    run_joinhash_parameter_population(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        out_dir=ns.out_dir,
        include_top_level=ns.include_top_level,
        include_semantic=ns.include_semantic,
        include_cosmetic=ns.include_cosmetic,
        include_coordination=ns.include_coordination,
        include_unknown=ns.include_unknown,
        max_cell_chars=ns.max_cell_chars,
    )


if __name__ == "__main__":
    main()
