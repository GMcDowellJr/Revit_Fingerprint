#!/usr/bin/env python3
"""Step-0 real-data probe for the planned tools/segment_decomposition.py.

Read-only, stdlib-only. Opens the already-materialized outputs of
build_segment_manifest.py / run_segment_orchestrator.py / compare_cross_segment.py
and prints the corpus facts the segment_decomposition design decisions depend on.
Writes nothing (no output files, no caches).

Sections (numbering follows the Step-0 investigation):
  0.3  ALL_DOMAINS self cells in project_mean_file_pair_jaccard_matrix.csv by
       value_status (observed vs synthetic_self_comparison), per view.
  0.4  The --client's Project-role segments (level, run_type, file_count), so
       coexisting discipline vs discipline+business_center segments are visible.
  0.5  File pairs (unordered export_run_id pair, domain) appearing under more
       than one segment pair for the --client in cross_segment_file_pairs.csv,
       and which segment pairs carry the duplicates.
  0.6  cross_segment_file_pairs.csv header, size, row count, same-segment
       (within_project) row count, and within_project rows whose used_jaccard is
       the literal "0.000000" (compare_cross_segment.py:3514 coerces a pair
       missing from the used view to 0.0 on these rows).
  0.7  Domain inventory of cross_segment_file_pairs.csv and the matrix vs the
       DOMAIN_CATEGORY map from micron_project_effect_analysis.py.
  0.8  discipline_label values with file counts (raw values, repr()-quoted so
       whitespace/typos are visible; near-duplicate hints are printed only --
       nothing is remapped).
  labels  Non-unique segment_label values in segment_manifest.csv, split by
       whether the colliding segments differ in unit_system.
  sizing  (unless --skip-membership-scan) per-view intra-segment file-pair counts
       from each runnable segment's bundle_analysis/<view>/<domain>/
       membership_matrix.csv -- the workload of Step 1's recompute path.

Segment folders are resolved exactly as compare_cross_segment.py's
segment_output_dir() does (run_registry.csv output_folder under --segments-root);
that logic is restated here rather than imported only because this probe is
required to be stdlib-only.

Usage:
    python tools/dev/segment_decomposition_step0_probe.py \\
        --records-dir results/records \\
        --cross-segment-dir results/cross_segment \\
        --segments-root segments \\
        [--client Micron] [--top 25] [--skip-membership-scan]
"""
from __future__ import annotations

import argparse
import csv
import difflib
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# From micron_project_effect_analysis.py (supplied verbatim for the 0.7 comparison).
DOMAIN_CATEGORY: Dict[str, str] = {}
for _cat, _domains in [
    ("Core Environment", ["browser_organization", "units", "phases", "phase_filters"]),
    ("Graphic Standards", [
        "line_patterns", "line_styles", "fill_patterns_drafting", "fill_patterns_model",
        "object_styles_analytical", "object_styles_annotation", "object_styles_model",
    ]),
    ("Annotation", [
        "arrowheads", "text_types", "dimension_types_angular", "dimension_types_diameter",
        "dimension_types_linear", "dimension_types_radial", "dimension_types_spot_coordinate",
        "dimension_types_spot_elevation", "dimension_types_spot_slope",
    ]),
    ("Model Content", ["loaded_family_types", "materials", "ceiling_types", "floor_types", "roof_types"]),
    ("View Governance", [
        "view_category_overrides_annotation", "view_category_overrides_model",
        "view_filter_applications_view_templates", "view_filter_definitions",
        "view_templates_ceiling_plans", "view_templates_elevations_sections_detail",
        "view_templates_floor_structural_area_plans", "view_templates_renderings_drafting",
        "view_templates_schedules",
    ]),
    ("Worksharing", ["worksets", "worksets_doc"]),
]:
    for _d in _domains:
        DOMAIN_CATEGORY[_d] = _cat

_NA_TOKEN_STRIP_RE = re.compile(r"[^a-z0-9]")
_NA_TOKENS = {"na", "notapplicable"}


def is_blank_or_na(value: str) -> bool:
    # Same rule as tools/na_token.py's is_blank_or_na(); restated rather than
    # imported only to keep this probe free of repo imports.
    v = (value or "").strip()
    return not v or _NA_TOKEN_STRIP_RE.sub("", v.lower()) in _NA_TOKENS


def header(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{k: (v or "") for k, v in r.items()} for r in csv.DictReader(f)]


def stream_rows(path: Path, columns: List[str]) -> Tuple[List[str], Iterator[Tuple[str, ...]]]:
    """Yield only the requested columns as tuples (fast path for large CSVs)."""
    f = path.open("r", encoding="utf-8-sig", newline="")
    reader = csv.reader(f)
    hdr = next(reader, [])
    idx = []
    for c in columns:
        idx.append(hdr.index(c) if c in hdr else -1)

    def gen() -> Iterator[Tuple[str, ...]]:
        try:
            for row in reader:
                n = len(row)
                yield tuple(row[i] if 0 <= i < n else "" for i in idx)
        finally:
            f.close()

    return hdr, gen()


def peak_memory_mb() -> Optional[float]:
    try:
        import resource  # POSIX only
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return rss / (1024.0 * 1024.0) if sys.platform == "darwin" else rss / 1024.0
    except ImportError:
        pass
    if os.name == "nt":
        try:
            import ctypes
            from ctypes import wintypes

            class PMC(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                return pmc.PeakWorkingSetSize / (1024.0 * 1024.0)
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def section_labels(manifest: Dict[str, Dict[str, str]], top: int) -> None:
    header("labels -- non-unique segment_label values (segment_manifest.csv)")
    by_label: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in manifest.values():
        by_label[row.get("segment_label", "")].append(row)
    blank = len(by_label.get("", []))
    print(f"segments: {len(manifest)}  distinct labels: {len(by_label)}  blank-label segments: {blank}")

    def summarize(rows_filter, title: str) -> None:
        groups = {}
        for label, rows in by_label.items():
            rows = [r for r in rows if rows_filter(r)]
            if label and len(rows) > 1:
                groups[label] = rows
        cross_unit = {l: rs for l, rs in groups.items() if len({r.get("unit_system", "") for r in rs}) > 1}
        same_unit = {l: rs for l, rs in groups.items() if l not in cross_unit}
        print(f"\n[{title}] non-unique labels: {len(groups)} "
              f"(segments involved: {sum(len(v) for v in groups.values())})")
        print(f"  colliding ACROSS unit_system: {len(cross_unit)}   colliding WITHIN one unit_system: {len(same_unit)}")
        units = Counter()
        for rs in groups.values():
            for r in rs:
                units[r.get("unit_system", "")] += 1
        print(f"  segments involved by unit_system: {dict(sorted(units.items()))}")
        for label, rs in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:top]:
            ids = ", ".join(sorted(r["segment_id"] for r in rs))
            print(f"    {label!r}  x{len(rs)}: {ids}")

    summarize(lambda r: True, "all segments")
    summarize(
        lambda r: r.get("governance_role", "").strip().lower() == "project"
        and r.get("run_type", "").strip().lower() in ("bundle", "reference"),
        "runnable Project-role segments (the matrix's row_id/column_id population)",
    )


def section_0_3(cross_dir: Path, manifest: Dict[str, Dict[str, str]]) -> Set[str]:
    header("0.3 -- ALL_DOMAINS self cells, project_mean_file_pair_jaccard_matrix.csv")
    path = cross_dir / "project_mean_file_pair_jaccard_matrix.csv"
    domains: Set[str] = set()
    if not path.exists():
        print(f"[missing] {path}")
        return domains
    by_label: Dict[str, List[str]] = defaultdict(list)
    for sid, row in manifest.items():
        by_label[row.get("segment_label", "")].append(sid)
    status_counts: Counter = Counter()
    ids_by_view_status: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    all_ids: Set[str] = set()
    n = 0
    _, rows = stream_rows(path, ["row_id", "column_id", "view_scope", "domain", "value_status"])
    for row_id, col_id, view, domain, status in rows:
        n += 1
        if domain:
            domains.add(domain)
        all_ids.add(row_id)
        if domain == "ALL_DOMAINS" and row_id == col_id:
            status_counts[(view, status)] += 1
            ids_by_view_status[(view, status)].add(row_id)
    print(f"matrix rows: {n}")
    for (view, status), c in sorted(status_counts.items()):
        print(f"  view={view:<5} status={status:<28} self cells: {c}")
    used_synth = ids_by_view_status.get(("used", "synthetic_self_comparison"), set())
    used_obs = ids_by_view_status.get(("used", "ok"), set())
    print(f"ANSWER 0.3: segments whose ALL_DOMAINS used-view self cell is synthetic: {len(used_synth)} "
          f"(observed 'ok': {len(used_obs)})")

    resolved = ambiguous = unresolved = 0
    ambiguous_ids: List[str] = []
    for rid in sorted(all_ids):
        if rid in manifest:
            # A row_id can be a raw segment_id (label fallback) -- but it can
            # ALSO collide with another segment's label; check both.
            if len(by_label.get(rid, [])) > 1 or (by_label.get(rid) and by_label[rid] != [rid]):
                ambiguous += 1
                ambiguous_ids.append(rid)
            else:
                resolved += 1
        elif len(by_label.get(rid, [])) == 1:
            resolved += 1
        elif len(by_label.get(rid, [])) > 1:
            ambiguous += 1
            ambiguous_ids.append(rid)
        else:
            unresolved += 1
    print(f"matrix row_id -> segment_id resolution via manifest: resolved={resolved} "
          f"ambiguous={ambiguous} unresolved={unresolved} (of {len(all_ids)} distinct row_ids)")
    for rid in ambiguous_ids[:25]:
        print(f"    ambiguous: {rid!r} -> {sorted(by_label.get(rid, [])) or [rid]}")
    return domains


def section_0_4(manifest: Dict[str, Dict[str, str]], client: str) -> Set[str]:
    header(f"0.4 -- Project-role segments for client_label == {client!r}")
    scoped = {
        sid for sid, r in manifest.items()
        if r.get("client_label", "").strip().lower() == client.lower()
    }
    proj = sorted(
        (r for sid, r in manifest.items() if sid in scoped and r.get("governance_role", "").strip().lower() == "project"),
        key=lambda r: (int(r.get("segment_level") or 0), r["segment_id"]),
    )
    print(f"segments with this client_label: {len(scoped)}  (Project-role: {len(proj)})")
    print(f"  {'level':<5} {'run_type':<12} {'files':>5}  {'discipline_label':<24} {'bc':<8} segment_id")
    for r in proj:
        print(f"  {r.get('segment_level',''):<5} {r.get('run_type',''):<12} {r.get('file_count',''):>5}  "
              f"{r.get('discipline_label',''):<24} {r.get('business_center_label',''):<8} {r['segment_id']}")
    coexist = defaultdict(lambda: {"disc_only": [], "disc_bc": []})
    for r in proj:
        disc = r.get("discipline_label", "")
        if not disc or r.get("run_type", "").strip().lower() not in ("bundle", "reference"):
            continue
        key = (r.get("unit_system", ""), disc)
        coexist[key]["disc_bc" if r.get("business_center_label", "") else "disc_only"].append(r["segment_id"])
    both = {k: v for k, v in coexist.items() if v["disc_only"] and v["disc_bc"]}
    print(f"ANSWER 0.4: (unit_system, discipline) with BOTH a runnable discipline-only and a runnable "
          f"discipline+business_center segment: {len(both)}")
    for (unit, disc), v in sorted(both.items()):
        print(f"    {unit} / {disc}: {v['disc_only']} + {v['disc_bc']}")
    return scoped


def section_file_pairs(
    cross_dir: Path,
    manifest: Dict[str, Dict[str, str]],
    scoped: Set[str],
    client: str,
    top: int,
) -> Set[str]:
    header("0.5 / 0.6 -- cross_segment_file_pairs.csv (single streaming pass)")
    path = cross_dir / "cross_segment_file_pairs.csv"
    domains: Set[str] = set()
    if not path.exists():
        print(f"[missing] {path}")
        return domains
    size = path.stat().st_size
    cols = ["comparison_run_id", "segment_id_a", "segment_id_b", "domain", "export_run_id_a",
            "export_run_id_b", "project_label_a", "project_label_b", "used_jaccard", "used_n_shared"]
    t0 = time.perf_counter()
    hdr, rows = stream_rows(path, cols)
    print(f"columns ({len(hdr)}): {', '.join(hdr)}")
    missing = [c for c in cols if c not in hdr]
    if missing:
        print(f"[warn] expected columns absent: {missing}")

    n_rows = 0
    n_same = 0
    n_same_used_zero = 0
    n_same_used_blank = 0
    n_cross_used_zero = 0
    n_cross_used_blank = 0
    same_segments: Set[str] = set()
    same_seg_domains: Set[Tuple[str, str]] = set()
    crids_by_same_seg_domain: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    blank_label_rows = 0
    na_label_rows = 0
    first_seen: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
    dup_segpairs: Dict[Tuple[str, str, str], Set[Tuple[str, str]]] = {}

    for crid, sa, sb, dom, ea, eb, pla, plb, uj, uns in rows:
        n_rows += 1
        domains.add(dom)
        if not pla.strip() or not plb.strip():
            blank_label_rows += 1
        elif is_blank_or_na(pla) or is_blank_or_na(plb):
            na_label_rows += 1
        if sa == sb:
            n_same += 1
            same_segments.add(sa)
            same_seg_domains.add((sa, dom))
            crids_by_same_seg_domain[(sa, dom)].add(crid)
            if uj == "0.000000":
                n_same_used_zero += 1
            elif uj == "":
                n_same_used_blank += 1
        else:
            if uj == "0.000000":
                n_cross_used_zero += 1
            elif uj == "":
                n_cross_used_blank += 1
        if sa in scoped and sb in scoped:
            key = (ea, eb, dom) if ea <= eb else (eb, ea, dom)
            segpair = (sa, sb)
            prev = first_seen.get(key)
            if prev is None:
                first_seen[key] = segpair
            elif prev != segpair:
                dup_segpairs.setdefault(key, {prev}).add(segpair)
    elapsed = time.perf_counter() - t0

    print(f"size: {size} bytes ({size / (1024 ** 3):.3f} GiB)  rows: {n_rows}  scan: {elapsed:.1f}s")
    print(f"rows with a blank project_label_a/b: {blank_label_rows}  with an N/A-spelled label: {na_label_rows}")
    print(f"same-segment rows (segment_id_a == segment_id_b, i.e. within_project): {n_same} "
          f"across {len(same_segments)} segments / {len(same_seg_domains)} (segment, domain)")
    multi_crid = sum(1 for v in crids_by_same_seg_domain.values() if len(v) > 1)
    print(f"(segment, domain) with >1 comparison_run_id among same-segment rows: {multi_crid}")
    runnable = {
        sid for sid, r in manifest.items()
        if r.get("run_type", "").strip().lower() in ("bundle", "reference")
    }
    print(f"runnable (bundle/reference) segments: {len(runnable)}; runnable with NO same-segment rows: "
          f"{len(runnable - same_segments)}")
    print(f"ANSWER 0.6: within_project rows with used_jaccard == '0.000000': {n_same_used_zero} "
          f"(blank: {n_same_used_blank}); for comparison, cross-segment rows: '0.000000'={n_cross_used_zero} "
          f"blank={n_cross_used_blank}")

    print(f"\n[0.5] file pairs (unordered export_run_id pair, domain) with both segments in client {client!r}: "
          f"{len(first_seen)}")
    print(f"ANSWER 0.5: file pairs appearing under more than one segment pair: {len(dup_segpairs)}")
    combo_counts: Counter = Counter()
    same_only = mixed = 0
    for segpairs in dup_segpairs.values():
        combo_counts[tuple(sorted(segpairs))] += 1
        if all(a == b for a, b in segpairs):
            same_only += 1
        else:
            mixed += 1
    print(f"  duplicates where every carrier is a same-segment (within_project) row: {same_only}; "
          f"involving at least one cross-segment row: {mixed}")
    print(f"  distinct carrier combinations: {len(combo_counts)} (top {top} by pair count):")
    for combo, c in combo_counts.most_common(top):
        print(f"    {c:>9} pairs: " + "  +  ".join(f"[{a} | {b}]" if a != b else f"[{a}]" for a, b in combo))
    return domains


def section_0_7(fp_domains: Set[str], matrix_domains: Set[str]) -> None:
    header("0.7 -- domain inventory vs DOMAIN_CATEGORY")
    mapped = set(DOMAIN_CATEGORY)
    for name, doms in [("cross_segment_file_pairs.csv", fp_domains),
                       ("project_mean_file_pair_jaccard_matrix.csv", matrix_domains - {"ALL_DOMAINS"})]:
        doms = {d for d in doms if d}
        print(f"{name}: {len(doms)} distinct domains")
        print(f"  {', '.join(sorted(doms))}")
        print(f"  ANSWER 0.7 unmapped (present, not in DOMAIN_CATEGORY): {sorted(doms - mapped)}")
        print(f"  mapped but absent from this file: {sorted(mapped - doms)}")


def section_0_8(records_dir: Path, manifest: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    header("0.8 -- discipline_label values with file counts")
    meta_path = records_dir / "file_metadata.csv"
    meta: Dict[str, Dict[str, str]] = {}
    if meta_path.exists():
        for r in read_rows(meta_path):
            meta[r.get("export_run_id", "")] = r
        counts: Counter = Counter(r.get("discipline_label", "") for r in meta.values())
        print(f"file_metadata.csv: {len(meta)} files")
        for value, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  {c:>6}  {value!r}")
        values = [v for v in counts if v.strip()]
        hints = []
        for i, a in enumerate(sorted(values)):
            for b in sorted(values)[i + 1:]:
                if a.strip().casefold() == b.strip().casefold() or difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio() >= 0.85:
                    hints.append((a, b))
        print("near-duplicate hints (printed only, not remapped):")
        for a, b in hints:
            print(f"    {a!r} ~ {b!r}")
        if not hints:
            print("    none")
    else:
        print(f"[missing] {meta_path}")
    seg_counts: Counter = Counter(r.get("discipline_label", "") for r in manifest.values() if r.get("discipline_label", ""))
    print(f"segment_manifest.csv: segments per discipline_label: {dict(sorted(seg_counts.items()))}")
    return meta


def section_sizing(
    segments_root: Path,
    records_dir: Path,
    manifest: Dict[str, Dict[str, str]],
    meta: Dict[str, Dict[str, str]],
) -> None:
    header("sizing -- Step 1 recompute workload from membership_matrix.csv")
    reg_path = records_dir / "run_registry.csv"
    if not reg_path.exists():
        print(f"[missing] {reg_path}")
        return
    registry = {r["segment_id"]: r for r in read_rows(reg_path)}
    t0 = time.perf_counter()
    totals: Counter = Counter()
    seg_missing = 0
    n_segments = 0
    unlabeled_files_by_seg: Dict[str, int] = {}
    for sid, r in sorted(manifest.items()):
        if r.get("run_type", "").strip().lower() not in ("bundle", "reference"):
            continue
        if r.get("governance_role", "").strip().lower() != "project":
            continue
        folder = registry.get(sid, {}).get("output_folder", "").strip()
        if not folder:
            seg_missing += 1
            continue
        n_segments += 1
        unl: Set[str] = set()
        for view in ("all", "used"):
            root = segments_root / folder / "results" / "bundle_analysis" / view
            if not root.exists():
                totals[(view, "segments_without_view_dir")] += 1
                continue
            for mm in root.glob("*/membership_matrix.csv"):
                _, rows = stream_rows(mm, ["export_run_id"])
                eids = {e.strip() for (e,) in rows if e.strip()}
                totals[(view, "matrices")] += 1
                totals[(view, "membership_rows_files")] += len(eids)
                totals[(view, "all_pairs")] += len(eids) * (len(eids) - 1) // 2
                by_proj: Counter = Counter()
                labeled = 0
                for e in eids:
                    label = meta.get(e, {}).get("project_label", "")
                    if is_blank_or_na(label):
                        unl.add(e)
                    else:
                        by_proj[label.strip()] += 1
                        labeled += 1
                within = sum(c * (c - 1) // 2 for c in by_proj.values())
                totals[(view, "within_project_pairs_labeled")] += within
                totals[(view, "between_project_pairs_labeled")] += labeled * (labeled - 1) // 2 - within
        unlabeled_files_by_seg[sid] = len(unl)
    print(f"runnable Project-role segments scanned: {n_segments} (no output_folder: {seg_missing})  "
          f"scan: {time.perf_counter() - t0:.1f}s")
    for (view, key), v in sorted(totals.items()):
        print(f"  view={view:<5} {key:<32} {v}")
    with_unl = {s: c for s, c in unlabeled_files_by_seg.items() if c}
    print(f"segments with >=1 unlabeled (blank/N/A project_label) file: {len(with_unl)}")
    for s, c in sorted(with_unl.items(), key=lambda kv: -kv[1])[:25]:
        print(f"    {c:>5}  {s}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--records-dir", required=True, type=Path,
                    help="Directory with segment_manifest.csv, run_registry.csv, file_metadata.csv")
    ap.add_argument("--cross-segment-dir", required=True, type=Path,
                    help="compare_cross_segment.py --out-dir (cross_segment_file_pairs.csv, matrices)")
    ap.add_argument("--segments-root", required=True, type=Path,
                    help="Per-segment output root (same as compare_cross_segment.py --segments-root)")
    ap.add_argument("--client", default="Micron",
                    help="client_label for the 0.4/0.5 nested-segment checks (default: Micron)")
    ap.add_argument("--top", type=int, default=25, help="Rows to list per ranked section")
    ap.add_argument("--skip-membership-scan", action="store_true",
                    help="Skip the membership_matrix.csv workload sizing section")
    args = ap.parse_args(argv)

    t_start = time.perf_counter()
    manifest_path = args.records_dir / "segment_manifest.csv"
    if not manifest_path.exists():
        print(f"[error] {manifest_path} not found", file=sys.stderr)
        return 2
    manifest = {r["segment_id"]: r for r in read_rows(manifest_path)}

    section_labels(manifest, args.top)
    matrix_domains = section_0_3(args.cross_segment_dir, manifest)
    scoped = section_0_4(manifest, args.client)
    fp_domains = section_file_pairs(args.cross_segment_dir, manifest, scoped, args.client, args.top)
    section_0_7(fp_domains, matrix_domains)
    meta = section_0_8(args.records_dir, manifest)
    if not args.skip_membership_scan:
        section_sizing(args.segments_root, args.records_dir, manifest, meta)

    header("run")
    peak = peak_memory_mb()
    print(f"total runtime: {time.perf_counter() - t_start:.1f}s  peak memory: "
          f"{f'{peak:.0f} MiB' if peak is not None else 'unavailable on this platform'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
