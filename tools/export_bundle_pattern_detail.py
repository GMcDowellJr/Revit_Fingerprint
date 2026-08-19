#!/usr/bin/env python3
"""
Export bundle → pattern → identity_items → name_population chain for one segment.

Emits three flat CSVs for BI consumption:
  bundle_pattern_inventory.csv  — one row per (bundle × pattern)
  pattern_settings.csv          — one row per (join_hash × identity item)
  pattern_names.csv             — one row per (join_hash × observed name)

Usage:
    python export_bundle_pattern_detail.py \\
        --segment <segment_id> \\
        --segments-root /path/to/segments \\
        --records-dir /path/to/records \\
        --out-dir /path/to/output \\
        [--domain <domain>] \\
        [--purge-view all|used] \\
        [--top-bundles N]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from jenks_utils import jenks_breaks

# ---------------------------------------------------------------------------
# CSV I/O helpers
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [
            {str(k): "" if v is None else str(v) for k, v in row.items()}
            for row in csv.DictReader(f)
        ]


def _atomic_write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        delete=False,
        dir=str(path.parent),
        suffix=".tmp",
    ) as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _int_safe(val: str) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _float_safe(val: str) -> Optional[float]:
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Auto top-bundles threshold (Jenks natural breaks on support_pct)
# ---------------------------------------------------------------------------

# Same convention as tools/bundle_analysis/step2_find_bundles.py's
# compute_auto_threshold: too few distinct values makes a Jenks break
# meaningless (it degenerates to splitting off a single low/high outlier
# rather than finding a real noise/signal transition). Below this count,
# keep everything rather than manufacture a cut. Matches Greg's Q4 answer:
# "all bundles if too few".
_MIN_DISTINCT_VALUES_FOR_JENKS = 4


def _compute_domain_bundle_threshold(bundles: List[Dict[str, str]]) -> Dict[str, Any]:
    """Derive a domain-specific top-bundles cutoff via Jenks natural breaks
    over each bundle's support_pct, pooled across every scope_key in the
    domain (bundles.csv is already written per-domain across all scopes, so
    no extra pooling step is needed -- see step2_find_bundles.py's
    find_bundles_for_domain, which writes one bundles.csv per domain
    covering every scope).

    support_pct (not files_present) so the break is comparable across
    scopes/segments of very different file-count scale -- same reasoning
    Greg gave for preferring percent over N-files-present at the CLI level.

    n_classes=2: a single break separating "top tier" bundles (the
    behavioral signal for this domain) from the long tail, following the
    same n_classes=2 / break_0-as-floor convention documented in
    compare_cross_segment.py's _compute_containment_thresholds (bundles AT
    the break clear the floor, i.e. keep if support_pct >= break_value).

    Returns a dict with keys: method, break_value (float or None),
    n_bundles_before, n_distinct_values, source_value_min, source_value_max.
    """
    support_values = [v for v in (_float_safe(r.get("support_pct", "")) for r in bundles) if v is not None]
    n_bundles = len(bundles)
    distinct_values = sorted(set(support_values))

    if len(distinct_values) < _MIN_DISTINCT_VALUES_FOR_JENKS:
        return {
            "method": "insufficient_distinct_values_keep_all",
            "break_value": None,
            "n_bundles_before": n_bundles,
            "n_distinct_values": len(distinct_values),
            "source_value_min": min(support_values) if support_values else None,
            "source_value_max": max(support_values) if support_values else None,
        }

    breaks = jenks_breaks(support_values, n_classes=2)
    break_value = breaks[0] if breaks else None
    return {
        "method": "jenks_natural_breaks",
        "break_value": break_value,
        "n_bundles_before": n_bundles,
        "n_distinct_values": len(distinct_values),
        "source_value_min": min(support_values),
        "source_value_max": max(support_values),
    }


def _apply_bundle_threshold(
    bundles: List[Dict[str, str]], threshold: Dict[str, Any]
) -> List[Dict[str, str]]:
    if threshold["break_value"] is None:
        return bundles
    break_value = threshold["break_value"]
    return [r for r in bundles if (_float_safe(r.get("support_pct", "")) or 0.0) >= break_value]


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def _resolve_segment_paths(
    output_folder: str,
    segments_root: Path,
    records_dir: Path,
    purge_view: str,
) -> Tuple[str, Path, Path, Path, Path, Path]:
    """
    Returns (segment_id, ba_root, domain_patterns_path, records_csv,
             identity_items_dir, label_synth_dir).
    Raises SystemExit on missing registry or unknown output_folder.
    """
    registry_path = records_dir / "run_registry.csv"
    if not registry_path.exists():
        raise SystemExit(f"run_registry.csv not found: {registry_path}")

    registry_rows = _read_csv(registry_path)
    seg_row = next((r for r in registry_rows if r.get("output_folder") == output_folder), None)
    if seg_row is None:
        raise SystemExit(f"output_folder '{output_folder}' not found in {registry_path}")

    segment_id = seg_row.get("segment_id", "")

    seg_out = segments_root / output_folder
    ba_root = seg_out / "results" / "bundle_analysis" / purge_view
    domain_patterns_path = seg_out / "results" / "analysis" / "domain_patterns.csv"

    # Prefer segment-scoped records (authoritative source for this segment's
    # domain_patterns.csv / bundles.csv); fall back to corpus-level if the
    # segment hasn't been presharded yet.
    seg_records_dir = seg_out / "results" / "records"
    preshard_marker = seg_records_dir / ".preshard_complete"
    if preshard_marker.is_file():
        records_csv = seg_records_dir / "records.csv"
        identity_items_dir = seg_records_dir / "identity_items_by_domain"
    else:
        print(
            f"[WARN] {preshard_marker} not found; falling back to corpus-level "
            f"records under {records_dir}",
            file=sys.stderr,
        )
        records_csv = records_dir / "records.csv"
        identity_items_dir = records_dir / "identity_items_by_domain"

    # Prefer segment-scoped label population (this is what actually answers
    # "what names were observed for this join_hash within THIS segment" --
    # run_segment_orchestrator.py's patterns stage writes
    # {out_root}/results/label_synthesis/{domain}.joinhash_label_population.csv
    # per segment; --label-synth-dir there only overrides the READ source for
    # LLM cache/curator annotations reuse, not where analysis outputs land --
    # see run_extract_all.py's --label-synth-dir help text). Corpus-level
    # label_synthesis/ never contains this file; falling back to it here was
    # the bug -- every join_hash silently missed and got the blank
    # placeholder row, which had nothing to do with whether LLM synthesis
    # had been run.
    seg_label_synth_dir = seg_out / "results" / "label_synthesis"
    if seg_label_synth_dir.is_dir():
        label_synth_dir = seg_label_synth_dir
    else:
        print(
            f"[WARN] {seg_label_synth_dir} not found; falling back to corpus-level "
            f"label_synthesis under {records_dir.parent} (segment likely hasn't run "
            f"the patterns stage yet)",
            file=sys.stderr,
        )
        label_synth_dir = records_dir.parent / "label_synthesis"

    return segment_id, ba_root, domain_patterns_path, records_csv, identity_items_dir, label_synth_dir


# ---------------------------------------------------------------------------
# Domain discovery
# ---------------------------------------------------------------------------

def _discover_domains(ba_root: Path) -> List[str]:
    if not ba_root.is_dir():
        return []
    return sorted(
        child.name
        for child in ba_root.iterdir()
        if child.is_dir() and (child / "bundles.csv").exists()
    )


# ---------------------------------------------------------------------------
# Per-domain data loaders
# ---------------------------------------------------------------------------

def _load_pattern_map(
    domain_patterns_path: Path, domain: str
) -> Dict[str, Dict[str, str]]:
    """
    pattern_id → {join_hash, pattern_label, pattern_label_human, pattern_files_present}

    join_hash is derived from source_cluster_id.split("|")[-1].
    pattern_files_present maps from pattern_size_files.
    """
    if not domain_patterns_path.exists():
        return {}
    result: Dict[str, Dict[str, str]] = {}
    for row in _read_csv(domain_patterns_path):
        if row.get("domain") != domain:
            continue
        pid = row.get("pattern_id", "")
        if not pid:
            continue
        src = row.get("source_cluster_id", "")
        join_hash = src.split("|")[-1] if src else ""
        result[pid] = {
            "join_hash": join_hash,
            "pattern_label": row.get("pattern_label", ""),
            "pattern_label_human": row.get("pattern_label_human", ""),
            "pattern_files_present": row.get("pattern_size_files", ""),
        }
    return result


def _load_representative_map(
    records_csv: Path, domain: str
) -> Dict[str, Tuple[str, str]]:
    """
    join_hash → (export_run_id, record_pk).  First occurrence wins.
    """
    if not records_csv.exists():
        return {}
    result: Dict[str, Tuple[str, str]] = {}
    for row in _read_csv(records_csv):
        if row.get("domain") != domain:
            continue
        jh = row.get("join_hash", "")
        if not jh or jh in result:
            continue
        result[jh] = (row.get("export_run_id", ""), row.get("record_pk", ""))
    return result


def _iter_identity_csv(path: Path) -> Iterator[Dict[str, str]]:
    """
    Yield rows normalised to {k, v, q, export_run_id, record_pk, ...}.

    Handles both legacy schema (k/v/q columns) and v2.1 schema
    (item_key / item_value / item_role columns).
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {str(col): "" if val is None else str(val) for col, val in raw.items()}
            if "k" not in row and "item_key" in row:
                row["k"] = row.get("item_key", "")
                row["v"] = row.get("item_value", "")
                row["q"] = row.get("item_role", row.get("item_value_type", ""))
            yield row


def _load_identity_items(
    identity_items_dir: Path,
    domain: str,
    needed_keys: Set[Tuple[str, str]],
) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    """
    Stream the domain shard and collect only rows whose (export_run_id, record_pk)
    is in needed_keys.  Memory is bounded by the representative records actually
    referenced by the in-scope bundles, not by the full shard size.
    """
    path = identity_items_dir / f"{domain}.csv"
    if not path.exists() or not needed_keys:
        return {}

    result: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for row in _iter_identity_csv(path):
        key = (row.get("export_run_id", ""), row.get("record_pk", ""))
        if key in needed_keys:
            result.setdefault(key, []).append(row)
    return result


def _load_label_population(
    label_synth_dir: Path, domain: str
) -> Dict[str, List[Dict[str, str]]]:
    """join_hash → list of {label_v, label_q, files_count} rows."""
    path = label_synth_dir / f"{domain}.joinhash_label_population.csv"
    if not path.exists():
        return {}
    result: Dict[str, List[Dict[str, str]]] = {}
    for row in _read_csv(path):
        jh = row.get("join_hash", "")
        if not jh:
            continue
        result.setdefault(jh, []).append(row)
    return result


# ---------------------------------------------------------------------------
# Per-domain processing
# ---------------------------------------------------------------------------

def _process_domain(
    segment_id: str,
    domain: str,
    purge_view: str,
    ba_root: Path,
    domain_patterns_path: Path,
    records_csv: Path,
    identity_items_dir: Path,
    label_synth_dir: Path,
    top_bundles: Optional[int],
    top_bundles_auto: bool,
) -> Tuple[
    List[Dict[str, str]],  # inventory rows
    List[Dict[str, str]],  # settings rows
    List[Dict[str, str]],  # names rows
    int,  # bundles_emitted
    int,  # patterns_emitted
    int,  # join_hashes_with_items
    int,  # join_hashes_with_names
    Optional[Dict[str, Any]],  # bundle selection threshold diagnostics (None if not --top-bundles-auto)
]:
    domain_out_dir = ba_root / domain
    bundles_csv_path = domain_out_dir / "bundles.csv"
    membership_csv_path = domain_out_dir / "bundle_membership.csv"

    if not bundles_csv_path.exists():
        print(
            f"[WARN] bundles.csv missing for domain '{domain}', skipping: {bundles_csv_path}",
            file=sys.stderr,
        )
        return [], [], [], 0, 0, 0, 0, None
    if not membership_csv_path.exists():
        print(
            f"[WARN] bundle_membership.csv missing for domain '{domain}', skipping: {membership_csv_path}",
            file=sys.stderr,
        )
        return [], [], [], 0, 0, 0, 0, None

    bundles = _read_csv(bundles_csv_path)
    threshold_diag: Optional[Dict[str, Any]] = None
    if top_bundles_auto:
        # Domain-specific cutoff, not a fixed N -- a domain with a thin,
        # obvious top tier keeps a couple of bundles; a domain with many
        # comparably-supported bundles keeps most of them. See
        # _compute_domain_bundle_threshold for the method.
        threshold_diag = _compute_domain_bundle_threshold(bundles)
        bundles = _apply_bundle_threshold(bundles, threshold_diag)
        threshold_diag["n_bundles_after"] = len(bundles)
        print(
            f"[top-bundles-auto] domain={domain} method={threshold_diag['method']} "
            f"break_value={threshold_diag['break_value']} "
            f"bundles_before={threshold_diag['n_bundles_before']} "
            f"bundles_after={threshold_diag['n_bundles_after']}",
            file=sys.stderr,
        )
    elif top_bundles is not None:
        bundles = [r for r in bundles if _int_safe(r.get("bundle_rank", "")) <= top_bundles]

    membership_rows = _read_csv(membership_csv_path)
    pattern_map = _load_pattern_map(domain_patterns_path, domain)
    rep_map = _load_representative_map(records_csv, domain)

    # bundle_id → [pattern_id, ...] from membership table
    bundle_to_patterns: Dict[str, List[str]] = {}
    for row in membership_rows:
        bid = row.get("bundle_id", "")
        pid = row.get("pattern_id", "")
        if bid and pid:
            bundle_to_patterns.setdefault(bid, []).append(pid)

    # Determine the representative (export_run_id, record_pk) pairs referenced
    # by the in-scope bundles so the identity shard is filtered while streaming.
    needed_keys: Set[Tuple[str, str]] = set()
    for brow in bundles:
        for pid in bundle_to_patterns.get(brow.get("bundle_id", ""), []):
            jh = pattern_map.get(pid, {}).get("join_hash", "")
            if jh and jh in rep_map:
                needed_keys.add(rep_map[jh])

    identity_map = _load_identity_items(identity_items_dir, domain, needed_keys)
    label_map = _load_label_population(label_synth_dir, domain)

    inventory_rows: List[Dict[str, str]] = []
    settings_rows: List[Dict[str, str]] = []
    names_rows: List[Dict[str, str]] = []
    seen_jh_items: Set[str] = set()
    seen_jh_names: Set[str] = set()

    for brow in bundles:
        bundle_id = brow.get("bundle_id", "")
        scope_key = brow.get("scope_key", "")
        bundle_rank = brow.get("bundle_rank", "")
        bundle_files_present = brow.get("files_present", "")
        bundle_files_total = brow.get("files_total", "")
        bundle_support_pct = brow.get("support_pct", "")
        bundle_pattern_count = brow.get("pattern_count", "")

        for pid in bundle_to_patterns.get(bundle_id, []):
            pm = pattern_map.get(pid, {})
            join_hash = pm.get("join_hash", "")

            inventory_rows.append({
                "segment_id": segment_id,
                "domain": domain,
                "purge_view": purge_view,
                "scope_key": scope_key,
                "bundle_id": bundle_id,
                "bundle_rank": bundle_rank,
                "bundle_files_present": bundle_files_present,
                "bundle_files_total": bundle_files_total,
                "bundle_support_pct": bundle_support_pct,
                "bundle_pattern_count": bundle_pattern_count,
                "pattern_id": pid,
                "join_hash": join_hash,
                "pattern_label": pm.get("pattern_label", ""),
                "pattern_label_human": pm.get("pattern_label_human", ""),
                "pattern_files_present": pm.get("pattern_files_present", ""),
            })

            if join_hash and join_hash not in seen_jh_items:
                seen_jh_items.add(join_hash)
                rep = rep_map.get(join_hash)
                item_rows = identity_map.get(rep, []) if rep else []
                if item_rows:
                    for irow in item_rows:
                        settings_rows.append({
                            "segment_id": segment_id,
                            "domain": domain,
                            "join_hash": join_hash,
                            "k": irow.get("k", ""),
                            "v": irow.get("v", ""),
                            "q": irow.get("q", ""),
                        })
                else:
                    # No representative record or no items found
                    settings_rows.append({
                        "segment_id": segment_id,
                        "domain": domain,
                        "join_hash": join_hash,
                        "k": "__no_items__",
                        "v": "",
                        "q": "missing",
                    })

            if join_hash and join_hash not in seen_jh_names:
                seen_jh_names.add(join_hash)
                name_rows = label_map.get(join_hash, [])
                if name_rows:
                    for nrow in name_rows:
                        names_rows.append({
                            "segment_id": segment_id,
                            "domain": domain,
                            "join_hash": join_hash,
                            "label_v": nrow.get("label_v", ""),
                            "label_q": nrow.get("label_q", ""),
                            "files_count": nrow.get("files_count", ""),
                        })
                else:
                    # label_synth absent or join_hash not present — emit placeholder row
                    names_rows.append({
                        "segment_id": segment_id,
                        "domain": domain,
                        "join_hash": join_hash,
                        "label_v": "",
                        "label_q": "",
                        "files_count": "",
                    })

    return (
        inventory_rows,
        settings_rows,
        names_rows,
        len(bundles),
        len(inventory_rows),
        len(seen_jh_items),
        len(seen_jh_names),
        threshold_diag,
    )


# ---------------------------------------------------------------------------
# Output field lists
# ---------------------------------------------------------------------------

_INVENTORY_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "purge_view", "scope_key",
    "bundle_id", "bundle_rank",
    "bundle_files_present", "bundle_files_total", "bundle_support_pct", "bundle_pattern_count",
    "pattern_id", "join_hash",
    "pattern_label", "pattern_label_human", "pattern_files_present",
)

_SETTINGS_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "join_hash", "k", "v", "q",
)

_NAMES_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "join_hash", "label_v", "label_q", "files_count",
)

_THRESHOLDS_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "purge_view", "method", "break_value",
    "n_bundles_before", "n_bundles_after", "n_distinct_values",
    "source_value_min", "source_value_max",
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Export bundle → pattern → identity_items → name_population chain "
            "for one segment into three BI-ready CSVs."
        )
    )
    ap.add_argument("--output-folder", required=True, help="output_folder value from run_registry.csv")
    ap.add_argument("--segments-root", required=True, help="Path to segments output root")
    ap.add_argument("--records-dir", required=True, help="Path to corpus-level records dir")
    ap.add_argument("--out-dir", required=True, help="Output directory for the three CSVs")
    ap.add_argument(
        "--domain",
        default=None,
        help="Domain to process; if omitted, iterate all domains with bundle analysis output",
    )
    ap.add_argument(
        "--purge-view",
        default="all",
        choices=["all", "used"],
        help="Bundle analysis subdir to read (default: all)",
    )
    top_bundles_group = ap.add_mutually_exclusive_group()
    top_bundles_group.add_argument(
        "--top-bundles",
        type=int,
        default=None,
        metavar="N",
        help="Emit only bundles where bundle_rank <= N (default: emit all). "
        "Mutually exclusive with --top-bundles-auto.",
    )
    top_bundles_group.add_argument(
        "--top-bundles-auto",
        action="store_true",
        help="Emit only bundles whose support_pct clears a domain-specific Jenks "
        "natural-breaks cutoff (pooled across all scopes in the domain), instead "
        "of a fixed rank cutoff -- a domain with an obvious thin top tier keeps "
        "few bundles, a domain with many comparably-supported bundles keeps most. "
        "Falls back to emitting all bundles for a domain when there are fewer "
        "than 4 distinct support_pct values to break on. Diagnostics written to "
        "bundle_selection_thresholds.csv in --out-dir. Mutually exclusive with "
        "--top-bundles.",
    )
    args = ap.parse_args()

    segments_root = Path(args.segments_root).resolve()
    records_dir = Path(args.records_dir).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not segments_root.is_dir():
        raise SystemExit(f"segments-root not found: {segments_root}")
    if not records_dir.is_dir():
        raise SystemExit(f"records-dir not found: {records_dir}")

    segment_id, ba_root, domain_patterns_path, records_csv, identity_items_dir, label_synth_dir = (
        _resolve_segment_paths(args.output_folder, segments_root, records_dir, args.purge_view)
    )

    if args.domain:
        domains = [args.domain]
    else:
        domains = _discover_domains(ba_root)
        if not domains:
            raise SystemExit(f"No domains with bundles.csv found under: {ba_root}")

    all_inventory: List[Dict[str, str]] = []
    all_settings: List[Dict[str, str]] = []
    all_names: List[Dict[str, str]] = []
    all_thresholds: List[Dict[str, str]] = []

    total_domains = 0
    total_bundles = 0
    total_patterns = 0
    total_jh_items = 0
    total_jh_names = 0

    for domain in domains:
        inv, sett, names, nb, np_, jhi, jhn, threshold_diag = _process_domain(
            segment_id=segment_id,
            domain=domain,
            purge_view=args.purge_view,
            ba_root=ba_root,
            domain_patterns_path=domain_patterns_path,
            records_csv=records_csv,
            identity_items_dir=identity_items_dir,
            label_synth_dir=label_synth_dir,
            top_bundles=args.top_bundles,
            top_bundles_auto=args.top_bundles_auto,
        )
        if not inv and not sett and not names and nb == 0:
            # Domain was skipped (warning already printed to stderr)
            continue
        all_inventory.extend(inv)
        all_settings.extend(sett)
        all_names.extend(names)
        total_domains += 1
        total_bundles += nb
        total_patterns += np_
        total_jh_items += jhi
        total_jh_names += jhn
        if threshold_diag is not None:
            all_thresholds.append(
                {
                    "segment_id": segment_id,
                    "domain": domain,
                    "purge_view": args.purge_view,
                    "method": threshold_diag["method"],
                    "break_value": (
                        "" if threshold_diag["break_value"] is None else f"{threshold_diag['break_value']:.6f}"
                    ),
                    "n_bundles_before": str(threshold_diag["n_bundles_before"]),
                    "n_bundles_after": str(threshold_diag.get("n_bundles_after", threshold_diag["n_bundles_before"])),
                    "n_distinct_values": str(threshold_diag["n_distinct_values"]),
                    "source_value_min": (
                        "" if threshold_diag["source_value_min"] is None else f"{threshold_diag['source_value_min']:.6f}"
                    ),
                    "source_value_max": (
                        "" if threshold_diag["source_value_max"] is None else f"{threshold_diag['source_value_max']:.6f}"
                    ),
                }
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write_csv(out_dir / "bundle_pattern_inventory.csv", _INVENTORY_FIELDS, all_inventory)
    _atomic_write_csv(out_dir / "pattern_settings.csv", _SETTINGS_FIELDS, all_settings)
    _atomic_write_csv(out_dir / "pattern_names.csv", _NAMES_FIELDS, all_names)
    if args.top_bundles_auto:
        _atomic_write_csv(out_dir / "bundle_selection_thresholds.csv", _THRESHOLDS_FIELDS, all_thresholds)

    print(
        f"Done. domains={total_domains} bundles={total_bundles} "
        f"patterns={total_patterns} "
        f"join_hashes_with_items={total_jh_items} "
        f"join_hashes_with_names={total_jh_names}"
    )


if __name__ == "__main__":
    main()