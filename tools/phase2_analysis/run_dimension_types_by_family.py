from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .io import load_exports, get_domain_records, get_domain_payload
from .run_change_type import run_change_type
from .run_population_stability import run_population_stability
from .run_candidate_joinkey_simulation import run_candidate_joinkey_simulation
from .run_joinhash_label_population import run_joinhash_label_population
from .run_joinhash_parameter_population import run_joinhash_parameter_population
from .run_collision_differencing import run_collision_differencing
from .run_identity_collision_diagnostics import run_identity_collision_diagnostics


DOMAIN_DEFAULT = "dimension_types"


def _get(d: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def _qv_to_v(x: Any) -> Optional[str]:
    """phase2 item maps store {q,v}. Return v as string if present."""
    if isinstance(x, dict):
        v = x.get("v")
        if v is None:
            return None
        try:
            s = str(v).strip()
        except Exception:
            return None
        return s or None
    return None


def _family_shape(record: Dict[str, Any]) -> str:
    """
    Return dimension family (Greg terminology: 'family'; Phase-2 earlier: 'shape').

    In your dimension_types Phase-2 records, this is carried in join_key.items:
      { "k": "dim_join.family_name", "v": "Linear Dimension Style" }

    Fall back to other locations only if join_key is absent.
    """
    # 1) Primary: join_key.items["dim_join.family_name"]
    jk = record.get("join_key")
    if isinstance(jk, dict):
        items = jk.get("items")
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                if it.get("k") == "dim_join.family_name":
                    v = it.get("v")
                    if v is not None:
                        s = str(v).strip()
                        if s:
                            return s

    # 2) Fallbacks (older/export-variant possibilities)
    candidates = [
        ["phase2", "semantic_items_map", "dim_join.family_name"],
        ["phase2", "semantic_items_map", "shape"],
        ["phase2", "semantic_items_map", "dim_attr.shape"],
        ["shape"],
    ]
    for path in candidates:
        v = _qv_to_v(_get(record, path))
        if v:
            return v

    return "missing"

_slug_rx = re.compile(r"[^a-z0-9]+", re.IGNORECASE)


def _slug(s: str) -> str:
    s = s.strip().lower()
    s = _slug_rx.sub("_", s)
    s = s.strip("_")
    return s or "missing"


def _write_json(path: str, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def _filter_export_domain_records(
    *,
    export_data: Dict[str, Any],
    domain: str,
    family_value: str,
) -> Tuple[Dict[str, Any], int]:
    """Return (new_export_data, kept_record_count). Keeps everything else intact."""
    # Shallow copy top-level and domain payload; keep other domains untouched.
    out: Dict[str, Any] = dict(export_data)

    payload = get_domain_payload(out, domain)
    if not isinstance(payload, dict):
        return out, 0

    # Copy payload so we don't mutate original object graph
    payload2 = dict(payload)
    recs = payload2.get("records")
    if not isinstance(recs, list):
        out[domain] = payload2
        return out, 0

    kept: List[Dict[str, Any]] = []
    for r in recs:
        if not isinstance(r, dict):
            continue
        fam = _family_shape(r)
        if fam == family_value:
            kept.append(r)

    payload2["records"] = kept
    out[domain] = payload2
    return out, len(kept)


def _discover_families_from_exports(exports_dir: str, domain: str) -> Set[str]:
    exports = load_exports(exports_dir)
    fams: Set[str] = set()
    for e in exports:
        for r in get_domain_records(e.data, domain):
            fams.add(_family_shape(r))
    return fams


def _families_present_in_baseline(exports_dir: str, domain: str, baseline_file_id: str) -> Set[str]:
    exports = load_exports(exports_dir)
    by_id = {e.file_id: e for e in exports}
    if baseline_file_id not in by_id:
        raise SystemExit(f"Baseline not found in directory: {baseline_file_id}")
    fams: Set[str] = set()
    for r in get_domain_records(by_id[baseline_file_id].data, domain):
        fams.add(_family_shape(r))
    return fams


@dataclass(frozen=True)
class FamilyRun:
    family_value: str
    family_slug: str
    filtered_dir: str
    out_dir: str


def _prepare_filtered_dirs(
    *,
    exports_dir: str,
    out_root: str,
    domain: str,
    baseline_file_id: str,
    families: Iterable[str],
    keep_temp: bool,
) -> List[FamilyRun]:
    exports = load_exports(exports_dir)
    if not exports:
        raise SystemExit(f"No JSON exports found in: {exports_dir}")

    # Clean temp root each run unless keep_temp requested
    temp_root = os.path.join(out_root, "_tmp_filtered_by_family", domain)
    def _on_rm_error(func, path, exc_info):
        # Windows: clear readonly bit then retry
        try:
            os.chmod(path, 0o777)
        except Exception:
            pass
        try:
            func(path)
        except Exception:
            pass

    if os.path.isdir(temp_root) and not keep_temp:
        # Windows can hold handles briefly (AV/Indexer/Explorer). Retry a few times.
        for _ in range(5):
            try:
                shutil.rmtree(temp_root, onerror=_on_rm_error)
                break
            except PermissionError:
                import time
                time.sleep(0.25)
        else:
            raise

    runs: List[FamilyRun] = []

    for fam in sorted(set(families)):
        fam_slug = _slug(fam)
        filtered_dir = os.path.join(temp_root, fam_slug)
        os.makedirs(filtered_dir, exist_ok=True)

        # Write filtered copies of every export json
        baseline_kept = 0
        for e in exports:
            new_data, kept = _filter_export_domain_records(export_data=e.data, domain=domain, family_value=fam)
            out_path = os.path.join(filtered_dir, e.file_id)
            _write_json(out_path, new_data)
            if e.file_id == baseline_file_id:
                baseline_kept = kept

        # Skip families not present in baseline (no meaningful seed deltas)
        if baseline_kept == 0:
            if not keep_temp:
                shutil.rmtree(filtered_dir, ignore_errors=True)
            continue

        fam_out = os.path.join(out_root, domain, "by_family", fam_slug)
        os.makedirs(fam_out, exist_ok=True)

        runs.append(FamilyRun(family_value=fam, family_slug=fam_slug, filtered_dir=filtered_dir, out_dir=fam_out))

    return runs


def _run_all_phase2(
    *,
    filtered_exports_dir: str,
    domain: str,
    baseline_file_id: str,
    out_dir: str,
) -> None:
    # Minimal set to reproduce your Phase-2 deliverables, per family
    run_change_type(
        exports_dir=filtered_exports_dir,
        domain=domain,
        baseline_file_id=baseline_file_id,
        out_dir=out_dir,
    )

    run_population_stability(
        exports_dir=filtered_exports_dir,
        domain=domain,
        out_dir=out_dir,
        thresholds_pct=[50, 70, 80],
    )

    run_candidate_joinkey_simulation(
        exports_dir=filtered_exports_dir,
        domain=domain,
        out_dir=out_dir,
    )

    run_joinhash_label_population(
        exports_dir=filtered_exports_dir,
        domain=domain,
        out_dir=out_dir,
    )

    run_joinhash_parameter_population(
        exports_dir=filtered_exports_dir,
        domain=domain,
        out_dir=out_dir,
        include_top_level=True,
        include_semantic=True,
        include_cosmetic=True,
        include_unknown=True,
        max_cell_chars=240,
    )

    run_collision_differencing(
        exports_dir=filtered_exports_dir,
        domain=domain,
        out_dir=out_dir,
        max_value_chars=240,
    )

    run_identity_collision_diagnostics(
        exports_dir=filtered_exports_dir,
        domain=domain,
        out_dir=out_dir,
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description="Phase-2 analysis per dimension family (shape) by filtering details JSON exports."
    )
    p.add_argument("exports_dir", help="Directory containing *.details.json exports (non-recursive).")
    p.add_argument("--domain", default=DOMAIN_DEFAULT)
    p.add_argument("--baseline", required=True, dest="baseline_file_id", help="Seed baseline filename (must exist in exports_dir).")
    p.add_argument("--out", required=True, dest="out_root", help="Output root directory (will create per-family subfolders).")

    fam_group = p.add_mutually_exclusive_group()
    fam_group.add_argument(
        "--families",
        default=None,
        help="Comma-separated list of family values to run (matches dim_attr.shape v). Example: \"Linear,Angular\"",
    )
    fam_group.add_argument(
        "--families_from",
        default="baseline",
        choices=["baseline", "all"],
        help="If --families not provided, discover families from: baseline (default) or all files.",
    )

    p.add_argument("--keep_temp", action="store_true", help="Keep filtered temp copies under _tmp_filtered_by_family.")
    ns = p.parse_args()

    exports_dir = os.path.abspath(ns.exports_dir)
    out_root = os.path.abspath(ns.out_root)
    domain = str(ns.domain)
    baseline = str(ns.baseline_file_id)

    if ns.families:
        families = [s.strip() for s in ns.families.split(",") if s.strip()]
    else:
        if ns.families_from == "all":
            families = sorted(_discover_families_from_exports(exports_dir, domain))
        else:
            families = sorted(_families_present_in_baseline(exports_dir, domain, baseline))

    if not families:
        raise SystemExit("No families discovered/selected.")

    runs = _prepare_filtered_dirs(
        exports_dir=exports_dir,
        out_root=out_root,
        domain=domain,
        baseline_file_id=baseline,
        families=families,
        keep_temp=bool(ns.keep_temp),
    )

    if not runs:
        raise SystemExit("No families to run (baseline had zero records for selected families).")

    print(f"Running {len(runs)} family slices for domain={domain}")
    for r in runs:
        print(f"\n=== FAMILY: {r.family_value}  (slug: {r.family_slug}) ===")
        _run_all_phase2(
            filtered_exports_dir=r.filtered_dir,
            domain=domain,
            baseline_file_id=baseline,
            out_dir=r.out_dir,
        )

    print("\nDone.")
    print(f"Outputs: {os.path.join(out_root, domain, 'by_family')}")


if __name__ == "__main__":
    main()
