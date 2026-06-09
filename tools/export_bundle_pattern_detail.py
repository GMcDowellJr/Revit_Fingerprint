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
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

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

_LARGE_FILE_BYTES = 50 * 1024 * 1024  # 50 MB


def _int_safe(val: str) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


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
    records_csv = records_dir / "records.csv"
    identity_items_dir = records_dir / "identity_items_by_domain"
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
    identity_items_dir: Path, domain: str
) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    """
    (export_run_id, record_pk) → list of normalised item rows.

    For shards >50 MB, rows are streamed directly into the dict rather than
    being materialised as a list first.
    """
    path = identity_items_dir / f"{domain}.csv"
    if not path.exists():
        return {}

    result: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    large = path.stat().st_size > _LARGE_FILE_BYTES

    if large:
        # Stream: never hold the entire file as a Python list
        for row in _iter_identity_csv(path):
            key = (row.get("export_run_id", ""), row.get("record_pk", ""))
            result.setdefault(key, []).append(row)
    else:
        rows = list(_iter_identity_csv(path))
        for row in rows:
            key = (row.get("export_run_id", ""), row.get("record_pk", ""))
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
) -> Tuple[
    List[Dict[str, str]],  # inventory rows
    List[Dict[str, str]],  # settings rows
    List[Dict[str, str]],  # names rows
    int,  # bundles_emitted
    int,  # patterns_emitted
    int,  # join_hashes_with_items
    int,  # join_hashes_with_names
]:
    domain_out_dir = ba_root / domain
    bundles_csv_path = domain_out_dir / "bundles.csv"
    membership_csv_path = domain_out_dir / "bundle_membership.csv"

    if not bundles_csv_path.exists():
        print(
            f"[WARN] bundles.csv missing for domain '{domain}', skipping: {bundles_csv_path}",
            file=sys.stderr,
        )
        return [], [], [], 0, 0, 0, 0
    if not membership_csv_path.exists():
        print(
            f"[WARN] bundle_membership.csv missing for domain '{domain}', skipping: {membership_csv_path}",
            file=sys.stderr,
        )
        return [], [], [], 0, 0, 0, 0

    # Load all domain-level data up front (once per domain)
    bundles = _read_csv(bundles_csv_path)
    if top_bundles is not None:
        bundles = [r for r in bundles if _int_safe(r.get("bundle_rank", "")) <= top_bundles]

    membership_rows = _read_csv(membership_csv_path)
    pattern_map = _load_pattern_map(domain_patterns_path, domain)
    rep_map = _load_representative_map(records_csv, domain)
    identity_map = _load_identity_items(identity_items_dir, domain)
    label_map = _load_label_population(label_synth_dir, domain)

    # bundle_id → [pattern_id, ...] from membership table
    bundle_to_patterns: Dict[str, List[str]] = {}
    for row in membership_rows:
        bid = row.get("bundle_id", "")
        pid = row.get("pattern_id", "")
        if bid and pid:
            bundle_to_patterns.setdefault(bid, []).append(pid)

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
    ap.add_argument(
        "--top-bundles",
        type=int,
        default=None,
        metavar="N",
        help="Emit only bundles where bundle_rank <= N (default: emit all)",
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

    total_domains = 0
    total_bundles = 0
    total_patterns = 0
    total_jh_items = 0
    total_jh_names = 0

    for domain in domains:
        inv, sett, names, nb, np_, jhi, jhn = _process_domain(
            segment_id=segment_id,
            domain=domain,
            purge_view=args.purge_view,
            ba_root=ba_root,
            domain_patterns_path=domain_patterns_path,
            records_csv=records_csv,
            identity_items_dir=identity_items_dir,
            label_synth_dir=label_synth_dir,
            top_bundles=args.top_bundles,
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

    out_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write_csv(out_dir / "bundle_pattern_inventory.csv", _INVENTORY_FIELDS, all_inventory)
    _atomic_write_csv(out_dir / "pattern_settings.csv", _SETTINGS_FIELDS, all_settings)
    _atomic_write_csv(out_dir / "pattern_names.csv", _NAMES_FIELDS, all_names)

    print(
        f"Done. domains={total_domains} bundles={total_bundles} "
        f"patterns={total_patterns} "
        f"join_hashes_with_items={total_jh_items} "
        f"join_hashes_with_names={total_jh_names}"
    )


if __name__ == "__main__":
    main()
