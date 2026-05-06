from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .io import load_exports, get_domain_records
from .report import write_json_report


# -----------------------------
# Helpers (descriptive only)
# -----------------------------

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


def _is_scalar(x: Any) -> bool:
    return x is None or isinstance(x, (str, int, float, bool))


def _stable_json(x: Any, *, max_chars: int) -> str:
    """JSON-ish string for CSV cells; truncates long values explicitly."""
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


def _phase2_items_map(record: Dict[str, Any], bucket: str) -> Tuple[Dict[str, Tuple[str, Optional[str]]], int]:
    """k -> (q,v) map for a single bucket. Returns duplicate_k_count explicitly."""
    out: Dict[str, Tuple[str, Optional[str]]] = {}
    dup = 0
    for it in _phase2_bucket_items(record, bucket):
        k = it.get("k")
        if k is None:
            continue
        try:
            ks = str(k)
        except Exception:
            continue
        if ks in out:
            dup += 1
            continue
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
        out[ks] = (qs, vs)
    return out, dup


def _top_level_field_variants(
    records: List[Dict[str, Any]],
    *,
    exclude_keys: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    """For each top-level key, collect variants across records.
    Returns key -> {"distinct": [values...], "missing_count": int, "record_count": int}
    Only includes keys that are scalars OR small dicts (kept as values, not flattened).
    """
    exclude = set(exclude_keys)
    all_keys = set()
    for r in records:
        all_keys |= set(r.keys())

    out: Dict[str, Dict[str, Any]] = {}
    n = len(records)

    for k in sorted(all_keys):
        if k in exclude:
            continue

        vals: List[Any] = []
        missing = 0
        for r in records:
            if k not in r:
                missing += 1
                continue
            v = r.get(k)
            # keep only scalar or dict; lists are typically bulky and not stable for a quick diff
            # (this is not a heuristic about meaning; it is an output-bounding choice)
            if _is_scalar(v) or isinstance(v, dict):
                vals.append(v)
            else:
                # represent non-scalar/non-dict as a type marker
                vals.append({"__type__": type(v).__name__})

        # distinct variants by JSON-serialized identity
        seen = {}
        for v in vals:
            key = _stable_json(v, max_chars=10_000)
            if key not in seen:
                seen[key] = v
        distinct = list(seen.values())

        out[k] = {"distinct": distinct, "missing_count": missing, "record_count": n}

    return out


@dataclass
class CollisionGroup:
    file_id: str
    join_hash: str
    group_size: int


# -----------------------------
# Main analysis
# -----------------------------

def run_collision_differencing(
    *,
    exports_dir: str,
    domain: str,
    out_dir: str,
    max_value_chars: int,
) -> None:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    os.makedirs(out_dir, exist_ok=True)

    groups: List[CollisionGroup] = []
    unjoinable_records_total = 0
    collision_groups_total = 0
    collision_records_total = 0

    # CSV 1: group index
    groups_csv = os.path.join(out_dir, f"{domain}.collision_groups.csv")
    # CSV 2: top-level field diffs
    fields_csv = os.path.join(out_dir, f"{domain}.collision_top_level_field_variants.csv")
    # CSV 3: semantic item diffs inside collisions
    semantic_csv = os.path.join(out_dir, f"{domain}.collision_phase2_semantic_diffs.csv")

    with open(groups_csv, "w", newline="", encoding="utf-8") as f_groups, \
         open(fields_csv, "w", newline="", encoding="utf-8") as f_fields, \
         open(semantic_csv, "w", newline="", encoding="utf-8") as f_sem:

        wg = csv.writer(f_groups)
        wf = csv.writer(f_fields)
        ws = csv.writer(f_sem)

        wg.writerow(["domain", "file_id", "join_hash", "group_size", "records_in_file"])
        wf.writerow(["domain", "file_id", "join_hash", "group_size", "field", "distinct_count", "missing_count", "distinct_values_json"])
        ws.writerow(["domain", "file_id", "join_hash", "group_size", "k", "comparisons", "diffs", "q_transitions_json"])

        for e in exports:
            records = get_domain_records(e.data, domain)

            # group by join_hash (including duplicates)
            by_jh: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for r in records:
                if not isinstance(r, dict):
                    continue
                jh = _get_join_hash(r)
                if jh is None:
                    unjoinable_records_total += 1
                    continue
                by_jh[jh].append(r)

            # process only collision groups (size >= 2)
            for jh, recs in by_jh.items():
                if len(recs) < 2:
                    continue

                collision_groups_total += 1
                collision_records_total += len(recs)

                grp = CollisionGroup(file_id=e.file_id, join_hash=jh, group_size=len(recs))
                groups.append(grp)

                wg.writerow([domain, e.file_id, jh, len(recs), len(records)])

                # --- Top-level field variants (descriptive)
                # Exclude bulky/known substructures; we’re not inferring meaning, just controlling output volume.
                variants = _top_level_field_variants(
                    recs,
                    exclude_keys=("phase2",),  # phase2 handled explicitly below
                )

                for field, info in variants.items():
                    distinct = info["distinct"]
                    missing = int(info["missing_count"])
                    # Only emit rows where there is actual variation OR missingness
                    if len(distinct) <= 1 and missing == 0:
                        continue
                    wf.writerow([
                        domain,
                        e.file_id,
                        jh,
                        len(recs),
                        field,
                        len(distinct),
                        missing,
                        _stable_json(distinct, max_chars=max_value_chars),
                    ])

                # --- Phase2 semantic diffs inside the collision group
                # Pairwise comparisons within the group.
                # We do NOT “pick a winner”; we just count disagreements and q transitions.
                k_comparisons = defaultdict(int)
                k_diffs = defaultdict(int)
                k_q_trans = defaultdict(lambda: defaultdict(int))
                ambiguous_dup_k = 0

                maps: List[Dict[str, Tuple[str, Optional[str]]]] = []
                for r in recs:
                    m, dup_k = _phase2_items_map(r, "semantic_items")
                    if dup_k:
                        ambiguous_dup_k += 1
                    maps.append(m)

                # If any record has duplicate semantic k keys, we still proceed, but note it in report.
                # We compare using each record’s map as-is; missing k is naturally handled by intersection.

                for i in range(len(maps)):
                    for j in range(i + 1, len(maps)):
                        mi = maps[i]
                        mj = maps[j]
                        shared = mi.keys() & mj.keys()
                        for k in shared:
                            k_comparisons[k] += 1
                            qi, vi = mi[k]
                            qj, vj = mj[k]
                            if (qi, vi) != (qj, vj):
                                k_diffs[k] += 1
                                k_q_trans[k][f"{qi}->{qj}"] += 1

                for k in sorted(k_comparisons.keys(), key=lambda x: (-k_diffs[x], x)):
                    ws.writerow([
                        domain,
                        e.file_id,
                        jh,
                        len(recs),
                        k,
                        int(k_comparisons[k]),
                        int(k_diffs[k]),
                        _stable_json(dict(k_q_trans[k]), max_chars=max_value_chars),
                    ])

    report = {
        "phase": "patterns_analysis",
        "analysis": "collision_differencing",
        "domain": domain,
        "files_total": len(exports),
        "counts": {
            "unjoinable_records_total": int(unjoinable_records_total),
            "collision_groups_total": int(collision_groups_total),
            "collision_records_total": int(collision_records_total),
        },
        "outputs": {
            "collision_groups_csv": os.path.abspath(groups_csv),
            "top_level_field_variants_csv": os.path.abspath(fields_csv),
            "phase2_semantic_diffs_csv": os.path.abspath(semantic_csv),
        },
        "assumptions": {
            "join_key": "record.join_key.join_hash",
            "collisions": "join_hash with multiplicity >=2 within a single file",
            "no_winner_selection": "all collision groups treated as ambiguous; only differences are described",
            "semantic_scope": "phase2.semantic_items only (cosmetic/unknown can be added later)",
        },
        "output_bounds": {
            "max_value_chars_per_cell": int(max_value_chars),
            "top_level_field_rule": "emit only scalar/dict variants; lists emitted as type markers",
        },
    }

    json_path = os.path.join(out_dir, f"{domain}.collision_differencing.report.json")
    write_json_report(out_path=json_path, report=report)

    print("Collision differencing written:")
    print(f"  {groups_csv}")
    print(f"  {fields_csv}")
    print(f"  {semantic_csv}")
    print(f"  {json_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Phase-2: collision differencing (descriptive)")
    p.add_argument("exports_dir")
    p.add_argument("--domain", default="dimension_types")
    p.add_argument("--out", default="phase2_out", dest="out_dir")
    p.add_argument("--max-value-chars", type=int, default=600, dest="max_value_chars")
    ns = p.parse_args()

    run_collision_differencing(
        exports_dir=ns.exports_dir,
        domain=ns.domain,
        out_dir=ns.out_dir,
        max_value_chars=ns.max_value_chars,
    )


if __name__ == "__main__":
    main()
