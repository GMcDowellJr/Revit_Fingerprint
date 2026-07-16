"""Extract one segment plus its ancestors from the cross_segment corpus.

Given a seed segment (by --segment-id) or a text search against
segment_manifest.csv (--search-term), walks parent_segment_id up to
--ancestor-depth generations, then pulls every cross_segment_*.csv /
comparison_registry.csv row touching the selected segments into two
clearly separated output shapes:

  detail/    filtered rows, copied through verbatim (source schema preserved)
  summary/   the same rows rolled up (count/mean/min/max on numeric fields),
             written only for files where many rows exist per segment pair
             (cross_segment_file_pairs.csv, cross_segment_delta.csv,
             cross_segment_governance_states.csv)

This intentionally does not use pandas: the rest of tools/ (compare_cross_segment.py
in particular) reads/writes CSV with the stdlib csv module and this project has
no pandas dependency, so this tool streams row-by-row with csv.DictReader/Writer
to match that convention and avoid adding a new dependency.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


# ---------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------

def norm(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def norm_fold(value: object) -> str:
    return norm(value).casefold()


def sanitize_label(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.strip().casefold()).strip("_")
    return slug or "segment_subtree"


def atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w", encoding="utf-8-sig", newline="", delete=False,
        dir=str(path.parent), suffix=".tmp",
    ) as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


class Blocked(RuntimeError):
    """Raised for any condition that must stop the run rather than degrade silently."""


# ---------------------------------------------------------------------
# Hierarchy loading (segment_manifest.csv)
# ---------------------------------------------------------------------

REQUIRED_HIERARCHY_COLUMNS = {"segment_id", "parent_segment_id"}

SELECTED_SEGMENT_METADATA_FIELDS = [
    "segment_label", "governance_role", "unit_system",
    "business_center_label", "client_label", "discipline_label",
    "collection_label", "file_count", "run_type",
]


def load_manifest(records_dir: Path) -> Dict[str, Dict[str, str]]:
    path = records_dir / "segment_manifest.csv"
    if not path.is_file():
        raise Blocked(f"segment_manifest.csv not found at {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not REQUIRED_HIERARCHY_COLUMNS.issubset(set(reader.fieldnames or [])):
            raise Blocked(f"{path} is missing segment_id/parent_segment_id columns")

        manifest: Dict[str, Dict[str, str]] = {}
        for raw_row in reader:
            row = {str(k): norm(v) for k, v in raw_row.items()}
            sid = row.get("segment_id", "")
            if not sid:
                continue
            prior = manifest.get(sid)
            if prior is not None and prior != row:
                raise Blocked(
                    f"segment_manifest.csv has conflicting rows for segment_id={sid!r}"
                )
            manifest[sid] = row
    return manifest


def find_seeds_by_search(manifest: Dict[str, Dict[str, str]], search_terms: Tuple[str, ...]) -> Set[str]:
    normalized_terms = tuple(norm_fold(t) for t in search_terms if norm(t))
    if not normalized_terms:
        return set()
    seeds: Set[str] = set()
    for sid, row in manifest.items():
        haystack = " ".join(norm_fold(v) for v in row.values())
        if any(term in haystack for term in normalized_terms):
            seeds.add(sid)
    return seeds


def find_seeds_by_id(manifest: Dict[str, Dict[str, str]], segment_ids: Tuple[str, ...]) -> Set[str]:
    seeds: Set[str] = set()
    for sid in segment_ids:
        sid = norm(sid)
        if not sid:
            continue
        if sid not in manifest:
            raise Blocked(f"--segment-id {sid!r} not found in segment_manifest.csv")
        seeds.add(sid)
    return seeds


def expand_ancestors(
    seed_ids: Set[str],
    parent_map: Dict[str, str],
    depth: int,
) -> Tuple[Set[str], List[Dict[str, object]]]:
    if depth < 0:
        raise ValueError("ancestor depth must be >= 0")

    selected = set(seed_ids)
    ancestry_rows: List[Dict[str, object]] = []

    for seed_id in sorted(seed_ids):
        current = seed_id
        seen = {seed_id}
        ancestry_rows.append(
            {"seed_segment_id": seed_id, "selected_segment_id": seed_id,
             "relationship": "seed", "ancestor_distance": 0}
        )

        for distance in range(1, depth + 1):
            if current not in parent_map:
                raise Blocked(f"selected segment {current!r} is absent from the hierarchy")

            parent_id = parent_map[current]
            if not parent_id:
                break
            if parent_id in seen:
                raise Blocked(f"cyclic hierarchy detected for {seed_id!r} at {parent_id!r}")

            selected.add(parent_id)
            seen.add(parent_id)
            ancestry_rows.append(
                {"seed_segment_id": seed_id, "selected_segment_id": parent_id,
                 "relationship": "parent" if distance == 1 else "grandparent",
                 "ancestor_distance": distance}
            )
            current = parent_id

    ancestry_rows.sort(
        key=lambda row: (str(row["seed_segment_id"]), int(row["ancestor_distance"]), str(row["selected_segment_id"]))
    )
    return selected, ancestry_rows


# ---------------------------------------------------------------------
# Segment-endpoint resolution (works across every current cross_segment schema)
# ---------------------------------------------------------------------

PREFERRED_ENDPOINT_PAIRS = (
    ("segment_id_reference", "segment_id_target"),
    ("segment_id_a", "segment_id_b"),
    ("reference_segment_id", "target_segment_id"),
    ("left_segment_id", "right_segment_id"),
    ("source_segment_id", "target_segment_id"),
)

_SINGLETON_EXCLUDED_COLUMNS = {"parent_segment_id", "ancestor_segment_ids", "seed_segment_id", "selected_segment_id"}


def resolve_endpoint_columns(columns: Iterable[str]) -> Tuple[str, ...]:
    actual = [str(c) for c in columns]
    folded = {c.casefold(): c for c in actual}

    for left, right in PREFERRED_ENDPOINT_PAIRS:
        if left in folded and right in folded:
            return folded[left], folded[right]

    candidates = [
        c for c in actual
        if "segment_id" in c.casefold() and c.casefold() not in _SINGLETON_EXCLUDED_COLUMNS
    ]
    if len(candidates) == 2:
        return tuple(candidates)
    if len(candidates) == 1:
        return (candidates[0],)
    return tuple()


def row_matches(row: Dict[str, str], endpoints: Tuple[str, ...], selected_ids: Set[str]) -> bool:
    if len(endpoints) == 2:
        return norm(row.get(endpoints[0])) in selected_ids and norm(row.get(endpoints[1])) in selected_ids
    if len(endpoints) == 1:
        return norm(row.get(endpoints[0])) in selected_ids
    return False


# ---------------------------------------------------------------------
# Streaming numeric accumulator (for the summary rollups)
# ---------------------------------------------------------------------

class NumericStats:
    __slots__ = ("count", "total", "minimum", "maximum")

    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0
        self.minimum = math.inf
        self.maximum = -math.inf

    def add(self, value: object) -> None:
        text = norm(value)
        if not text:
            return
        try:
            number = float(text)
        except (TypeError, ValueError):
            return
        if not math.isfinite(number):
            return
        self.count += 1
        self.total += number
        self.minimum = min(self.minimum, number)
        self.maximum = max(self.maximum, number)

    def emit(self, prefix: str) -> Dict[str, object]:
        if self.count == 0:
            return {f"{prefix}_count": 0, f"{prefix}_mean": "", f"{prefix}_min": "", f"{prefix}_max": ""}
        return {
            f"{prefix}_count": self.count,
            f"{prefix}_mean": self.total / self.count,
            f"{prefix}_min": self.minimum,
            f"{prefix}_max": self.maximum,
        }


# ---------------------------------------------------------------------
# Per-file specs — which cross_segment/comparison_registry outputs this
# tool knows how to filter, and (for the high-cardinality ones) how to
# roll up into a summary.
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class FileSpec:
    filename: str
    summary_keys: Tuple[str, ...] = ()
    summary_numeric_fields: Tuple[str, ...] = ()
    summary_flag_fields: Tuple[Tuple[str, str], ...] = ()

    @property
    def supports_summary(self) -> bool:
        return bool(self.summary_keys)


FILE_SPECS: Tuple[FileSpec, ...] = (
    FileSpec("cross_segment_summary.csv"),
    FileSpec(
        "cross_segment_file_pairs.csv",
        summary_keys=("segment_id_a", "segment_id_b", "domain"),
        summary_numeric_fields=(
            "n_patterns_a", "n_patterns_b", "n_shared",
            "all_jaccard", "all_containment_a_in_b", "all_containment_b_in_a",
            "used_n_shared", "used_jaccard", "used_containment_a_in_b", "used_containment_b_in_a",
            "all_n_shared_bundle_both", "all_n_shared_bundle_a_only", "all_n_shared_bundle_b_only",
            "used_n_shared_bundle_both", "used_n_shared_bundle_a_only", "used_n_shared_bundle_b_only",
        ),
    ),
    FileSpec(
        "cross_segment_delta.csv",
        summary_keys=("segment_id_reference", "segment_id_target", "comparison_type", "domain", "delta_class"),
        summary_numeric_fields=("n_files_in_target", "pct_files_in_target", "used_pct_files_in_target"),
        summary_flag_fields=(
            ("is_bundle_member_all", "bundle_member_all_count"),
            ("is_bundle_member_used", "bundle_member_used_count"),
        ),
    ),
    FileSpec(
        "cross_segment_governance_states.csv",
        summary_keys=(
            "segment_id_reference", "segment_id_target", "comparison_type",
            "governance_role_reference", "governance_role_target", "unit_system",
            "domain", "state", "reference_usage_interpretable",
            "target_usage_interpretable", "recommended_primary_view",
        ),
        summary_numeric_fields=(
            "n_files_in_target_all", "pct_files_in_target_all",
            "n_files_in_target_used", "pct_files_in_target_used",
        ),
        summary_flag_fields=(
            ("is_bundle_member_target_all", "bundle_member_target_all_count"),
            ("is_bundle_member_target_used", "bundle_member_target_used_count"),
        ),
    ),
    FileSpec("cross_segment_governance_state_summary.csv"),
    FileSpec("cross_segment_pooled.csv"),
    FileSpec("comparison_registry.csv"),
)

DEFAULT_FILENAMES: Tuple[str, ...] = tuple(spec.filename for spec in FILE_SPECS)

# Informational only — these current cross_segment outputs have no segment_id
# grain at all (union/matrix outputs keyed by governance_role/client_label or
# row_id/column_id), so they are never in the default file list. If one is
# passed explicitly via --file, endpoint resolution below fails naturally
# ("no usable segment endpoint columns") rather than needing a hardcoded list
# kept in sync with every matrix file this tool doesn't otherwise know about.
KNOWN_UNSUPPORTED_FILES: Tuple[Tuple[str, str], ...] = (
    ("cross_segment_union_inventory.csv", "grain is (governance_role, client_label, discipline_label, unit_system, domain, view_scope, join_hash) — no segment_id column by design"),
    ("pattern_reuse_distribution.csv", "same union-level grain as cross_segment_union_inventory.csv — no segment_id column by design"),
)


# ---------------------------------------------------------------------
# Streaming extraction + summary (single pass over the source file)
# ---------------------------------------------------------------------

@dataclass
class ProcessResult:
    status: str
    rows_scanned: int = 0
    rows_written: int = 0
    summary_rows: int = 0
    selection_mode: str = ""
    detail_file: str = ""
    summary_file: str = ""
    reason: str = ""


def _update_aggregate(aggregates: Dict[Tuple[str, ...], Dict[str, object]], row: Dict[str, str], spec: FileSpec) -> None:
    key = tuple(norm(row.get(k)) for k in spec.summary_keys)
    entry = aggregates.setdefault(key, {"row_count": 0, "numeric": {}, "flags": defaultdict(int)})
    entry["row_count"] += 1
    for field_name in spec.summary_numeric_fields:
        entry["numeric"].setdefault(field_name, NumericStats()).add(row.get(field_name))
    for flag_field, count_name in spec.summary_flag_fields:
        if norm_fold(row.get(flag_field)) == "true":
            entry["flags"][count_name] += 1


def _write_summary(aggregates: Dict[Tuple[str, ...], Dict[str, object]], spec: FileSpec, path: Path) -> int:
    rows: List[Dict[str, object]] = []
    for key in sorted(aggregates):
        entry = aggregates[key]
        row: Dict[str, object] = dict(zip(spec.summary_keys, key))
        row["row_count"] = entry["row_count"]
        for _, count_name in spec.summary_flag_fields:
            row[count_name] = entry["flags"].get(count_name, 0)
        numeric = entry["numeric"]
        for field_name in spec.summary_numeric_fields:
            row.update(numeric.get(field_name, NumericStats()).emit(field_name))
        rows.append(row)

    fieldnames = list(spec.summary_keys) + ["row_count"] + [c for _, c in spec.summary_flag_fields]
    for field_name in spec.summary_numeric_fields:
        fieldnames += [f"{field_name}_count", f"{field_name}_mean", f"{field_name}_min", f"{field_name}_max"]

    atomic_write_csv(path, fieldnames, rows)
    return len(rows)


def process_file(
    source: Path,
    selected_ids: Set[str],
    spec: FileSpec,
    detail_path: Optional[Path],
    summary_path: Optional[Path],
    max_output_rows: int,
    max_output_gb: float,
    progress_interval: int,
) -> ProcessResult:
    with source.open("r", encoding="utf-8-sig", newline="") as f:
        header_reader = csv.reader(f)
        header = next(header_reader, [])
    endpoints = resolve_endpoint_columns(header)
    if not endpoints:
        raise Blocked("no usable segment endpoint columns in source header")

    aggregates: Dict[Tuple[str, ...], Dict[str, object]] = {}
    rows_scanned = 0
    rows_written = 0
    wrote_detail_header = False

    detail_tmp: Optional[Path] = None
    detail_handle = None
    if detail_path is not None:
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_tmp = detail_path.with_suffix(detail_path.suffix + ".tmp")
        if detail_tmp.exists():
            detail_tmp.unlink()

    try:
        with source.open("r", encoding="utf-8-sig", newline="") as f_in:
            reader = csv.DictReader(f_in)
            detail_writer = None
            if detail_tmp is not None:
                detail_handle = detail_tmp.open("w", encoding="utf-8-sig", newline="")
                detail_writer = csv.DictWriter(detail_handle, fieldnames=reader.fieldnames or [])

            for row in reader:
                rows_scanned += 1
                if not row_matches(row, endpoints, selected_ids):
                    if rows_scanned % progress_interval == 0:
                        print(f"{source.name}: scanned {rows_scanned:,}; selected {rows_written:,}")
                    continue

                rows_written += 1
                if max_output_rows > 0 and rows_written > max_output_rows:
                    raise Blocked(f"max output row threshold exceeded ({rows_written:,} > {max_output_rows:,})")

                if detail_writer is not None:
                    if not wrote_detail_header:
                        detail_writer.writeheader()
                        wrote_detail_header = True
                    detail_writer.writerow(row)

                if summary_path is not None and spec.supports_summary:
                    _update_aggregate(aggregates, row, spec)

                if max_output_gb > 0 and detail_tmp is not None and rows_written % 10_000 == 0:
                    size_gib = detail_tmp.stat().st_size / (1024 ** 3)
                    if size_gib > max_output_gb:
                        raise Blocked(f"max output size threshold exceeded ({size_gib:.3f} GiB > {max_output_gb:.3f} GiB)")

                if rows_scanned % progress_interval == 0:
                    print(f"{source.name}: scanned {rows_scanned:,}; selected {rows_written:,}")

        if detail_handle is not None:
            detail_handle.close()
            detail_handle = None

        if detail_tmp is not None:
            if not wrote_detail_header:
                # Preserve the source schema even when nothing matched.
                with detail_tmp.open("w", encoding="utf-8-sig", newline="") as fh:
                    writer = csv.DictWriter(fh, fieldnames=header)
                    writer.writeheader()
            detail_tmp.replace(detail_path)

    except Exception:
        if detail_handle is not None:
            detail_handle.close()
        if detail_tmp is not None and detail_tmp.exists():
            detail_tmp.unlink()
        raise

    summary_rows_count = 0
    if summary_path is not None and spec.supports_summary:
        summary_rows_count = _write_summary(aggregates, spec, summary_path)

    print(f"{source.name}: done — scanned {rows_scanned:,}; selected {rows_written:,}; summary_rows {summary_rows_count:,}")

    return ProcessResult(
        status="ok" if rows_written > 0 else "degraded",
        rows_scanned=rows_scanned,
        rows_written=rows_written,
        summary_rows=summary_rows_count,
        selection_mode=("both_sides:" if len(endpoints) == 2 else "single_segment:") + "&".join(endpoints),
        detail_file=detail_path.name if detail_path is not None else "",
        summary_file=summary_path.name if summary_path is not None else "",
        reason="" if rows_written > 0 else "no rows had a selected segment on the required endpoint(s)",
    )


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract one segment's subtree (itself plus ancestors) from the "
            "cross_segment comparison corpus, as detail rows and/or rolled-up "
            "summaries, clearly separated into detail/ and summary/ output folders."
        )
    )
    parser.add_argument("--records-dir", required=True, type=Path, help="Directory containing segment_manifest.csv")
    parser.add_argument("--cross-segment-dir", required=True, type=Path, help="Directory containing cross_segment_*.csv and comparison_registry.csv")
    parser.add_argument("--out-dir", type=Path, default=None, help="Defaults to <cross-segment-dir>/<label>_subtree_extract")
    parser.add_argument("--label", default=None, help="Defaults to a sanitized form of the first --segment-id/--search-term")
    parser.add_argument("--segment-id", action="append", dest="segment_ids", default=None, help="Seed segment id. Repeat for multiple seeds.")
    parser.add_argument("--search-term", action="append", dest="search_terms", default=None, help="Case-insensitive substring matched against every segment_manifest.csv column. Repeat for multiple terms.")
    parser.add_argument("--ancestor-depth", type=int, default=2)
    parser.add_argument("--mode", choices=("detail", "summary", "both"), default="both")
    parser.add_argument("--file", action="append", dest="files", default=None, help="Override the default cross-segment filename list. Repeat to extract more than one.")
    parser.add_argument("--max-output-rows", type=int, default=0, help="Per-file hard row limit. 0 disables. Hitting it blocks that file's output rather than truncating.")
    parser.add_argument("--max-output-gb", type=float, default=0.0, help="Per-file hard size limit in GiB. 0 disables.")
    parser.add_argument("--progress-interval", type=int, default=100_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    segment_ids = tuple(args.segment_ids or ())
    search_terms = tuple(args.search_terms or ())
    if not segment_ids and not search_terms:
        print("[error] pass at least one --segment-id or --search-term", file=sys.stderr)
        return 1

    label = args.label or sanitize_label(segment_ids[0] if segment_ids else search_terms[0])
    out_dir = args.out_dir or (args.cross_segment_dir / f"{label}_subtree_extract")
    detail_dir = out_dir / "detail"
    summary_dir = out_dir / "summary"

    try:
        manifest = load_manifest(args.records_dir)
        seed_ids = find_seeds_by_id(manifest, segment_ids) | find_seeds_by_search(manifest, search_terms)
        if not seed_ids:
            raise Blocked(f"no segments matched --segment-id/--search-term {segment_ids + search_terms!r}")

        parent_map = {sid: row.get("parent_segment_id", "") for sid, row in manifest.items()}
        selected_ids, ancestry_rows = expand_ancestors(seed_ids, parent_map, args.ancestor_depth)
    except Blocked as exc:
        print(f"[error] Blocked: {exc}", file=sys.stderr)
        return 1

    relations_by_segment: Dict[str, Set[str]] = defaultdict(set)
    distances_by_segment: Dict[str, List[int]] = defaultdict(list)
    for row in ancestry_rows:
        sid = str(row["selected_segment_id"])
        relations_by_segment[sid].add(str(row["relationship"]))
        distances_by_segment[sid].append(int(row["ancestor_distance"]))

    selected_rows: List[Dict[str, object]] = []
    for sid in sorted(selected_ids):
        meta = manifest.get(sid, {})
        selected_rows.append({
            "segment_id": sid,
            "parent_segment_id": meta.get("parent_segment_id", ""),
            "selection_relationships": "|".join(sorted(relations_by_segment.get(sid, set()))),
            "minimum_ancestor_distance": min(distances_by_segment.get(sid, [0])),
            **{field_name: meta.get(field_name, "") for field_name in SELECTED_SEGMENT_METADATA_FIELDS},
        })

    atomic_write_csv(
        out_dir / "selected_segments.csv",
        ["segment_id", "parent_segment_id", "selection_relationships", "minimum_ancestor_distance"] + SELECTED_SEGMENT_METADATA_FIELDS,
        selected_rows,
    )

    want_detail = args.mode in ("detail", "both")
    want_summary = args.mode in ("summary", "both")

    specs_by_name = {spec.filename: spec for spec in FILE_SPECS}
    target_filenames = tuple(args.files) if args.files else DEFAULT_FILENAMES

    manifest_rows: List[Dict[str, object]] = []
    blocked = False

    for filename in target_filenames:
        spec = specs_by_name.get(filename, FileSpec(filename))
        source = args.cross_segment_dir / filename

        if not source.is_file():
            manifest_rows.append({
                "source_file": filename, "status": "blocked", "rows_scanned": "", "rows_written": "",
                "summary_rows": "", "selection_mode": "", "detail_file": "", "summary_file": "",
                "reason": "source file not found",
            })
            blocked = True
            continue

        detail_path = detail_dir / filename if want_detail else None
        summary_path = summary_dir / f"{Path(filename).stem}.summary.csv" if (want_summary and spec.supports_summary) else None

        try:
            result = process_file(
                source=source,
                selected_ids=selected_ids,
                spec=spec,
                detail_path=detail_path,
                summary_path=summary_path,
                max_output_rows=args.max_output_rows,
                max_output_gb=args.max_output_gb,
                progress_interval=args.progress_interval,
            )
            manifest_rows.append({"source_file": filename, **vars(result)})
        except Blocked as exc:
            manifest_rows.append({
                "source_file": filename, "status": "blocked", "rows_scanned": "", "rows_written": "",
                "summary_rows": "", "selection_mode": "", "detail_file": "", "summary_file": "",
                "reason": str(exc),
            })
            blocked = True

    if not args.files:
        for filename, reason in KNOWN_UNSUPPORTED_FILES:
            manifest_rows.append({
                "source_file": filename, "status": "unsupported", "rows_scanned": "", "rows_written": "",
                "summary_rows": "", "selection_mode": "", "detail_file": "", "summary_file": "",
                "reason": reason,
            })

    manifest_rows.sort(key=lambda r: str(r["source_file"]))
    atomic_write_csv(
        out_dir / "extract_manifest.csv",
        ["source_file", "status", "rows_scanned", "rows_written", "summary_rows", "selection_mode", "detail_file", "summary_file", "reason"],
        manifest_rows,
    )

    print()
    print(f"Label: {label}")
    print(f"Seed segments: {len(seed_ids):,}")
    print(f"Selected segments including ancestors: {len(selected_ids):,}")
    print(f"Output folder: {out_dir}")
    print("Run status: " + ("blocked" if blocked else "ok") + " (see extract_manifest.csv)")

    return 1 if blocked else 0


if __name__ == "__main__":
    raise SystemExit(main())
