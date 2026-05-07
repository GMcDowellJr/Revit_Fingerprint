# -*- coding: utf-8 -*-
"""
materials_joinkey_discover.py

Standalone join-key candidate discovery for the materials domain.
Works entirely from phase0_records.csv — no re-extraction required.

Runs two passes:
  PASS 1 — full corpus (baseline, includes Revit system/analytical materials)
  PASS 2 — governed corpus (system/analytical materials filtered out)

Pass 2 shows a governance tier breakdown per strategy:
  converged  >=75% files
  aligned   50-74% files
  emerging  25-49% files
  nascent   10-24% files
  fragmented  <10% files

Usage:
  python tools/materials_joinkey_discover.py --records path/to/phase0_records.csv
  python tools/materials_joinkey_discover.py --records path/to/phase0_records.csv --out-dir results/materials_joinkey
  python tools/materials_joinkey_discover.py --records path/to/phase0_records.csv --top-n-tier 20
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{str(k): ("" if v is None else str(v)) for k, v in row.items()}
                for row in csv.DictReader(f)]


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Load materials records
# ---------------------------------------------------------------------------

def _load_materials(phase0_dir: Path, records_file: Optional[Path] = None) -> List[Dict[str, str]]:
    if records_file is not None:
        records_csv = records_file
    else:
        records_csv = phase0_dir / "phase0_records.csv"
    if not records_csv.is_file():
        raise FileNotFoundError(f"records CSV not found: {records_csv}")

    rows = [r for r in _read_csv(records_csv) if r.get("domain", "").strip() == "materials"]
    if not rows:
        raise SystemExit("No materials rows found in records CSV.")

    files = len({(r.get("export_run_id") or r.get("file_id") or "").strip() for r in rows})
    print(f"[discover] Loaded {len(rows):,} materials records from {files} files.")
    return rows


# ---------------------------------------------------------------------------
# System / analytical material noise detection
# ---------------------------------------------------------------------------

# Substring fragments that identify Revit platform materials (case-insensitive).
_SYSTEM_FRAGMENTS = [
    "analytical", "dynamo", "fixedbc", "pinned", "boundary roller",
    "ydirection", "zdirection", "xdirection",
    "system-zones", "electrical analytical", "air surface", "air opening",
    "air space", "phase - demo", "phase - exist", "phase - temp",
    "default light source",
]

# Exact names that are always platform materials.
_SYSTEM_EXACT = {
    "default", "poche", "default wall", "default roof", "default floor",
    "default ceiling", "dynamoerror", "dynamo",
}


def _is_system_material(r: Dict[str, str]) -> bool:
    name = (r.get("label_display") or "").strip().lower()
    if not name:
        return True
    if name in _SYSTEM_EXACT:
        return True
    for frag in _SYSTEM_FRAGMENTS:
        if frag in name:
            return True
    return False


def _partition_rows(rows: List[Dict[str, str]]) -> Tuple[List, List]:
    system_rows = [r for r in rows if _is_system_material(r)]
    governed_rows = [r for r in rows if not _is_system_material(r)]
    return system_rows, governed_rows


# ---------------------------------------------------------------------------
# Key extraction
# ---------------------------------------------------------------------------

def _extract_name(r: Dict[str, str]) -> str:
    v = (r.get("label_display") or "").strip()
    return v.lower() if v else "__missing__"


def _extract_sig(r: Dict[str, str]) -> str:
    v = (r.get("sig_hash") or "").strip()
    return v if v else "__missing__"


def _extract_uid(r: Dict[str, str]) -> str:
    rid = (r.get("record_id") or "").strip()
    return rid[4:] if rid.startswith("uid:") else (rid or "__missing__")


def _build_key(parts: List[str]) -> str:
    return _md5("|".join(parts))


# ---------------------------------------------------------------------------
# Load material class from identity_items shard (optional)
# ---------------------------------------------------------------------------

def _load_class_map(phase0_dir: Path) -> Dict[str, str]:
    shard = phase0_dir / "phase0_identity_items_by_domain" / "materials.csv"
    if not shard.is_file():
        mono = phase0_dir / "phase0_identity_items.csv"
        if not mono.is_file():
            return {}
        print("[discover] Loading material class from monolithic identity_items (slow)...")
        source = mono
    else:
        print(f"[discover] Loading material class from shard: {shard.name}")
        source = shard

    out: Dict[str, str] = {}
    with source.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            domain = (row.get("domain") or "").strip()
            if source.name != "materials.csv" and domain != "materials":
                continue
            k = (row.get("item_key") or row.get("k") or "").strip()
            if k != "material.class":
                continue
            record_pk = (row.get("record_pk") or "").strip()
            v = (row.get("item_value") or row.get("v") or "").strip()
            if record_pk:
                out[record_pk] = v.lower() if v else "__missing__"
    print(f"[discover] Loaded class for {len(out):,} records.")
    return out


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def _compute_metrics(
    rows: List[Dict[str, str]],
    key_fn,
    strategy_label: str,
    files_total: int,
) -> Dict[str, object]:
    key_files: Dict[str, Set[str]] = defaultdict(set)
    key_sigs: Dict[str, Set[str]] = defaultdict(set)
    sig_keys: Dict[str, Set[str]] = defaultdict(set)
    missing = 0

    for r in rows:
        k = key_fn(r)
        fid = (r.get("export_run_id") or r.get("file_id") or "").strip()
        sig = _extract_sig(r)
        if k in ("__missing__", "", "__unknown__"):
            missing += 1
            continue
        key_files[k].add(fid)
        key_sigs[k].add(sig)
        sig_keys[sig].add(k)

    fps = [len(v) for v in key_files.values()]
    half = max(1, files_total // 2)
    colliding = {k for k, s in key_sigs.items() if len(s) > 1}
    fragmented = {s for s, ks in sig_keys.items() if len(ks) > 1}
    total = len(rows)

    return {
        "strategy": strategy_label,
        "pattern_count": len(key_files),
        "files_total": files_total,
        "avg_files_per_pattern": round(sum(fps) / len(fps), 2) if fps else 0.0,
        "max_files_per_pattern": max(fps) if fps else 0,
        "patterns_in_1_file": sum(1 for c in fps if c == 1),
        "patterns_in_2plus_files": sum(1 for c in fps if c >= 2),
        "patterns_in_half_corpus": sum(1 for c in fps if c >= half),
        "collision_count": len(colliding),
        "collision_records": sum(1 for r in rows if key_fn(r) in colliding),
        "fragmentation_count": len(fragmented),
        "fragmentation_records": sum(1 for r in rows if _extract_sig(r) in fragmented),
        "coverage_pct": round((total - missing) / total * 100, 2) if total else 0.0,
        "records_total": total,
        "records_missing_key": missing,
    }


# ---------------------------------------------------------------------------
# Top-N patterns
# ---------------------------------------------------------------------------

def _top_patterns(rows: List[Dict[str, str]], key_fn, n: int) -> List[Dict[str, str]]:
    key_files: Dict[str, Set[str]] = defaultdict(set)
    key_labels: Dict[str, str] = {}
    for r in rows:
        k = key_fn(r)
        if k in ("__missing__", "", "__unknown__"):
            continue
        key_files[k].add((r.get("export_run_id") or r.get("file_id") or "").strip())
        if k not in key_labels:
            key_labels[k] = (r.get("label_display") or "").strip()
    return [
        {"rank": str(i + 1), "join_key": k,
         "files_present": str(len(fids)), "representative_label": key_labels.get(k, "")}
        for i, (k, fids) in enumerate(sorted(key_files.items(), key=lambda kv: -len(kv[1]))[:n])
    ]


# ---------------------------------------------------------------------------
# Tiered presence breakdown
# ---------------------------------------------------------------------------

_TIERS = [
    ("converged  >=75%", 0.75, 1.01),
    ("aligned   50-74%", 0.50, 0.75),
    ("emerging  25-49%", 0.25, 0.50),
    ("nascent   10-24%", 0.10, 0.25),
    ("fragmented  <10%", 0.00, 0.10),
]


def _build_key_files(rows: List[Dict[str, str]], key_fn) -> Tuple[Dict, Dict]:
    key_files: Dict[str, Set[str]] = defaultdict(set)
    key_labels: Dict[str, str] = {}
    for r in rows:
        k = key_fn(r)
        if k in ("__missing__", "", "__unknown__"):
            continue
        key_files[k].add((r.get("export_run_id") or r.get("file_id") or "").strip())
        if k not in key_labels:
            key_labels[k] = (r.get("label_display") or "").strip()
    return key_files, key_labels


def _print_tiered(rows: List[Dict[str, str]], key_fn, files_total: int,
                  strategy_label: str, top_n_per_tier: int) -> None:
    key_files, key_labels = _build_key_files(rows, key_fn)
    print(f"\n  [{strategy_label}]  {len(key_files):,} patterns | {files_total} files | system materials excluded")
    for tier_label, floor_pct, ceil_pct in _TIERS:
        tier = sorted(
            [(k, fids) for k, fids in key_files.items()
             if floor_pct <= len(fids) / files_total < ceil_pct],
            key=lambda kv: -len(kv[1]),
        )
        print(f"\n  {tier_label}  ({len(tier):,} patterns)")
        if not tier:
            print("    (none)")
            continue
        for i, (k, fids) in enumerate(tier[:top_n_per_tier], 1):
            lbl = key_labels.get(k, k[:60])
            pct = len(fids) / files_total * 100
            print(f"    {i:>3}.  {len(fids):>4} files ({pct:4.1f}%)  {lbl}")
        if len(tier) > top_n_per_tier:
            print(f"         ... and {len(tier) - top_n_per_tier:,} more")


def _tiered_csv_rows(rows: List[Dict[str, str]], key_fn, files_total: int,
                     strategy_label: str, top_n_per_tier: int) -> List[Dict[str, str]]:
    key_files, key_labels = _build_key_files(rows, key_fn)
    out = []
    for tier_label, floor_pct, ceil_pct in _TIERS:
        tier = sorted(
            [(k, fids) for k, fids in key_files.items()
             if floor_pct <= len(fids) / files_total < ceil_pct],
            key=lambda kv: -len(kv[1]),
        )
        out.append({"strategy": strategy_label, "tier": tier_label,
                    "tier_pattern_count": str(len(tier)), "rank": "",
                    "files_present": "", "pct_files": "",
                    "label": f"=== {len(tier)} patterns ==="})
        for i, (k, fids) in enumerate(tier[:top_n_per_tier], 1):
            out.append({"strategy": strategy_label, "tier": tier_label,
                        "tier_pattern_count": "", "rank": str(i),
                        "files_present": str(len(fids)),
                        "pct_files": f"{len(fids)/files_total*100:.1f}%",
                        "label": key_labels.get(k, k[:80])})
    return out


# ---------------------------------------------------------------------------
# Summary table printer
# ---------------------------------------------------------------------------

def _print_summary_table(all_metrics: List[Dict], title: str) -> None:
    cw = {"s": 24, "p": 10, "a": 8, "m": 7, "tp": 8, "h": 6, "co": 10, "fr": 10, "cv": 7}
    hdr = (f"{'Strategy':<{cw['s']}} {'Patterns':>{cw['p']}} {'AvgFiles':>{cw['a']}} "
           f"{'MaxFiles':>{cw['m']}} {'2+Files':>{cw['tp']}} {'>=50%':>{cw['h']}} "
           f"{'Collisions':>{cw['co']}} {'Fragments':>{cw['fr']}} {'Cov%':>{cw['cv']}}")
    sep = "-" * len(hdr)
    print()
    print("=" * len(hdr))
    print(f"  {title}")
    print("=" * len(hdr))
    print(hdr)
    print(sep)
    for m in all_metrics:
        print(f"{m['strategy']:<{cw['s']}} {m['pattern_count']:>{cw['p']},} "
              f"{m['avg_files_per_pattern']:>{cw['a']}.2f} {m['max_files_per_pattern']:>{cw['m']},} "
              f"{m['patterns_in_2plus_files']:>{cw['tp']},} {m['patterns_in_half_corpus']:>{cw['h']},} "
              f"{m['collision_count']:>{cw['co']},} {m['fragmentation_count']:>{cw['fr']},} "
              f"{m['coverage_pct']:>{cw['cv']}.1f}%")
    print(sep)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

def discover(
    phase0_dir: Path,
    out_dir: Optional[Path],
    top_n: int,
    skip_class: bool,
    records_file: Optional[Path] = None,
    top_n_per_tier: int = 15,
) -> None:
    rows = _load_materials(phase0_dir, records_file)
    files_total = len({(r.get("export_run_id") or r.get("file_id") or "").strip() for r in rows})

    system_rows, governed_rows = _partition_rows(rows)
    print(f"[discover] System/analytical filtered out: {len(system_rows):,} records")
    print(f"[discover] Governed corpus (pass 2):       {len(governed_rows):,} records")

    class_map: Dict[str, str] = {}
    if not skip_class:
        class_map = _load_class_map(phase0_dir)

    def key_uid(r):   return _extract_uid(r)
    def key_name(r):  return _extract_name(r)
    def key_sig(r):   return _extract_sig(r)

    def key_name_sig(r):
        n, s = _extract_name(r), _extract_sig(r)
        return "__missing__" if "__missing__" in (n, s) else _build_key([n, s])

    def key_class(r):
        return class_map.get(r.get("record_pk", "").strip(), "__unknown__") if class_map else "__unknown__"

    def key_name_class(r):
        n = _extract_name(r)
        c = class_map.get(r.get("record_pk", "").strip(), "__unknown__") if class_map else "__unknown__"
        return "__missing__" if n == "__missing__" else _build_key([n, c])

    strategies = [
        ("uid (current)",       key_uid),
        ("name",                key_name),
        ("sig_hash (graphics)", key_sig),
        ("name + sig_hash",     key_name_sig),
    ]
    if class_map:
        strategies += [("class", key_class), ("name + class", key_name_class)]
    else:
        print("[discover] Skipping class-based strategies (identity_items shard not found).")

    # Pass 1 — full corpus
    m_full = [_compute_metrics(rows, fn, lbl, files_total) for lbl, fn in strategies]
    _print_summary_table(m_full, f"PASS 1 — FULL CORPUS  ({len(rows):,} records | {files_total} files)")

    # Pass 2 — governed corpus
    gov_files = len({(r.get("export_run_id") or r.get("file_id") or "").strip() for r in governed_rows})
    m_gov = [_compute_metrics(governed_rows, fn, lbl, gov_files) for lbl, fn in strategies]
    _print_summary_table(m_gov, f"PASS 2 — GOVERNED CORPUS  ({len(governed_rows):,} records | {gov_files} files)")

    # Pass 2 tiered breakdown
    tier_strategies = [("sig_hash (graphics)", key_sig), ("name", key_name), ("name + sig_hash", key_name_sig)]
    if class_map:
        tier_strategies.append(("name + class", key_name_class))

    print()
    print("=" * 90)
    print("  PASS 2 — GOVERNANCE TIER BREAKDOWN  (system materials excluded)")
    print("=" * 90)
    for lbl, fn in tier_strategies:
        _print_tiered(governed_rows, fn, gov_files, lbl, top_n_per_tier)

    # Write CSVs
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_fields = [
            "strategy", "pattern_count", "files_total",
            "avg_files_per_pattern", "max_files_per_pattern",
            "patterns_in_1_file", "patterns_in_2plus_files", "patterns_in_half_corpus",
            "collision_count", "collision_records", "fragmentation_count", "fragmentation_records",
            "coverage_pct", "records_total", "records_missing_key",
        ]
        _write_csv(out_dir / "materials_summary_full_corpus.csv", summary_fields,
                   [{k: str(v) for k, v in m.items()} for m in m_full])
        _write_csv(out_dir / "materials_summary_governed_corpus.csv", summary_fields,
                   [{k: str(v) for k, v in m.items()} for m in m_gov])

        tier_fields = ["strategy", "tier", "tier_pattern_count", "rank", "files_present", "pct_files", "label"]
        for lbl, fn in tier_strategies:
            safe = lbl.replace(" ", "_").replace("+", "plus").replace("(", "").replace(")", "")
            _write_csv(out_dir / f"materials_tiers__{safe}.csv", tier_fields,
                       _tiered_csv_rows(governed_rows, fn, gov_files, lbl, top_n_per_tier))

        for lbl, fn in strategies:
            safe = lbl.replace(" ", "_").replace("+", "plus").replace("(", "").replace(")", "")
            _write_csv(out_dir / f"materials_top_patterns__{safe}.csv",
                       ["rank", "join_key", "files_present", "representative_label"],
                       _top_patterns(rows, fn, top_n))

        print(f"\n[discover] Wrote CSVs to: {out_dir}")

    print()
    print("INTERPRETATION GUIDE")
    print("  sig_hash:       zero collisions/fragmentation — cleanest governance signal")
    print("  name:           meaningful convergence, ~1200 collisions (same name, diff graphics)")
    print("  name+sig_hash:  tightest identity, zero collisions, slightly more patterns")
    print("  converged >=75%: strong standard candidates")
    print("  aligned 50-74%: nudgeable to convergence")
    print("  emerging 25-49%: partial adoption — watch, not mandate")
    print("  nascent   <25%: fragmented — governance effort not justified yet")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Discover materials join key candidates from phase0_records.csv. No re-extraction required."
    )
    ap.add_argument("--phase0-dir", default=None,
                    help="Path to phase0_v21/ directory (for identity_items shard lookup).")
    ap.add_argument("--records", default=None,
                    help="Direct path to records CSV. Overrides --phase0-dir for record loading.")
    ap.add_argument("--out-dir", default=None, help="Optional output directory for CSV reports.")
    ap.add_argument("--top-n", type=int, default=20, help="Top-N in full-corpus pattern lists (default: 20).")
    ap.add_argument("--top-n-tier", type=int, default=15, help="Examples shown per governance tier (default: 15).")
    ap.add_argument("--skip-class", action="store_true",
                    help="Skip class-based strategies even if identity_items shard is available.")
    args = ap.parse_args()

    if not args.phase0_dir and not args.records:
        ap.error("At least one of --phase0-dir or --records is required.")

    records_file = Path(args.records).resolve() if args.records else None
    if args.phase0_dir:
        phase0_dir = Path(args.phase0_dir).resolve()
    elif records_file:
        phase0_dir = records_file.parent
    else:
        phase0_dir = Path(".").resolve()

    discover(
        phase0_dir=phase0_dir,
        out_dir=Path(args.out_dir).resolve() if args.out_dir else None,
        top_n=args.top_n,
        skip_class=args.skip_class,
        records_file=records_file,
        top_n_per_tier=args.top_n_tier,
    )


if __name__ == "__main__":
    main()
