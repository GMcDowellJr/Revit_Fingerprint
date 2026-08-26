#!/usr/bin/env python3
"""
Export bundle → pattern → identity_items → name_population chain for one segment.

Emits three flat CSVs for BI consumption:
  bundle_pattern_inventory.csv  — one row per (bundle × pattern)
  pattern_settings.csv          — invariant items per pattern across distinct sig_hash definitions
  pattern_supplemental_values.csv — deduped variant values with weighted support
  pattern_reconstruction_summary.csv — one row per pattern with reconstruction status
  pattern_names.csv             — one row per (join_hash × observed name)
  reconstruction_source_files.csv — compact greedy source-file cover metrics
  reconstruction_source_file_patterns.json — file/domain/pattern names for inspection

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
import json
import sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, DefaultDict, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

from jenks_utils import jenks_breaks

RecordKey = Tuple[str, str]
ItemValue = Tuple[str, str]

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


def _atomic_write_json(path: Path, payload: Any) -> None:
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
        json.dump(payload, tmp, ensure_ascii=False, indent=2, sort_keys=False)
        tmp.write("\n")
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
) -> Tuple[str, Path, Path, Path, Path, Path, Path]:
    """
    Returns (segment_id, ba_root, domain_patterns_path, pattern_membership_path,
             records_csv, identity_items_dir, label_synth_dir).
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
    analysis_dir = seg_out / "results" / "analysis"
    domain_patterns_path = analysis_dir / "domain_patterns.csv"
    pattern_membership_path = analysis_dir / "record_pattern_membership.csv"

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

    return (
        segment_id, ba_root, domain_patterns_path, pattern_membership_path,
        records_csv, identity_items_dir, label_synth_dir,
    )


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


def _load_pattern_record_map(
    membership_csv: Path,
    domain: str,
    in_scope_pattern_ids: Set[str],
) -> Dict[str, Set[RecordKey]]:
    """Return all source record keys for each requested pattern."""
    if not membership_csv.is_file():
        raise SystemExit(
            "Required record-to-pattern linkage is missing: "
            f"{membership_csv}. Re-run segment analysis; representative-record "
            "fallback is intentionally disabled."
        )
    result: DefaultDict[str, Set[RecordKey]] = defaultdict(set)
    for row in _read_csv(membership_csv):
        if row.get("domain") != domain:
            continue
        pattern_id = row.get("pattern_id", "").strip()
        if not pattern_id or pattern_id not in in_scope_pattern_ids:
            continue
        key = (row.get("export_run_id", "").strip(), row.get("record_pk", "").strip())
        if all(key):
            result[pattern_id].add(key)
    return dict(result)


def _load_record_metadata(
    records_csv: Path,
    domain: str,
    needed_keys: Set[RecordKey],
) -> Dict[RecordKey, Dict[str, str]]:
    result: Dict[RecordKey, Dict[str, str]] = {}
    if not records_csv.is_file() or not needed_keys:
        return result
    for row in _read_csv(records_csv):
        if row.get("domain") != domain:
            continue
        key = (row.get("export_run_id", ""), row.get("record_pk", ""))
        if key in needed_keys:
            result[key] = {
                "join_hash": row.get("join_hash", ""),
                "join_key_schema": row.get("join_key_schema", ""),
                "sig_hash": row.get("sig_hash", ""),
            }
    return result


def _build_pattern_signature_map(
    pattern_record_map: Dict[str, Set[RecordKey]],
    record_meta: Dict[RecordKey, Dict[str, str]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Collapse members to one deterministic representative per sig_hash.

    Population weights are retained. Missing sig_hash records remain separate
    because collapsing them would assert identity equivalence without evidence.
    """
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for pattern_id, record_keys in pattern_record_map.items():
        groups: Dict[str, Dict[str, Any]] = {}
        for record_key in sorted(record_keys):
            sig_hash = record_meta.get(record_key, {}).get("sig_hash", "").strip()
            group_key = sig_hash or f"__missing__|{record_key[0]}|{record_key[1]}"
            group = groups.setdefault(group_key, {
                "sig_hash": sig_hash,
                "representative_key": record_key,
                "record_count": 0,
                "file_ids": set(),
            })
            group["record_count"] += 1
            group["file_ids"].add(record_key[0])
            if record_key < group["representative_key"]:
                group["representative_key"] = record_key
        result[pattern_id] = groups
    return result


def _load_join_policies(policy_path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    if policy_path is None:
        return {}
    if not policy_path.is_file():
        raise SystemExit(f"join policy not found: {policy_path}")
    with policy_path.open("r", encoding="utf-8-sig") as f:
        payload = json.load(f)
    domains = payload.get("domains", {})
    return domains if isinstance(domains, dict) else {}


def _join_role(policy: Dict[str, Any], key: str, discriminator_value: str) -> str:
    required = set(policy.get("required_items", []) or policy.get("required_fields", []))
    optional = set(policy.get("optional_items", []) or policy.get("optional_fields", []))
    excluded = set(policy.get("explicitly_excluded_items", []))
    gating = policy.get("shape_gating", {}) or {}
    shape = (gating.get("shape_requirements", {}) or {}).get(discriminator_value, {})
    if key in required or key in set(shape.get("additional_required", []) or []):
        return "required"
    if key in optional or key in set(shape.get("additional_optional", []) or []):
        return "optional"
    if key in excluded:
        return "excluded"
    return "unclassified"


def _iter_identity_csv(path: Path) -> Iterator[Dict[str, str]]:
    """
    Yield rows normalised to {k, v, q, export_run_id, record_pk, ...}.

    Handles both legacy schema (k/v/q columns) and v2.1 schema
    (item_key / item_value / item_value_type columns; item_role is a
    separate, non-quality tag and is never used for q).
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {str(col): "" if val is None else str(val) for col, val in raw.items()}
            if "k" not in row and "item_key" in row:
                row["k"] = row.get("item_key", "")
                row["v"] = row.get("item_value", "")
                # item_role is NOT a quality field in the v2.1 shard schema -- it's an
                # unrelated tag (blank for a normal extractor.py-written row, but e.g.
                # "synthetic" for tools/run_extract_all.py's synthetic
                # line_pattern.segments_norm_hash row). Quality always lives in
                # item_value_type (see tools/extractor.py's own item_role/item_value_type
                # split at its identity-item CSV read site); never read q from item_role.
                row["q"] = row.get("item_value_type", "")
            yield row


def _load_identity_items(
    identity_items_dir: Path,
    domain: str,
    needed_keys: Set[Tuple[str, str]],
) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    """
    Stream the domain shard and collect only rows whose (export_run_id, record_pk)
    is in needed_keys.  Memory is bounded by the distinct signature representatives referenced by
    the in-scope patterns, not by the full shard size.
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


def _aggregate_pattern_items(
    segment_id: str,
    domain: str,
    pattern_id: str,
    join_hash: str,
    signature_groups: Dict[str, Dict[str, Any]],
    identity_map: Dict[RecordKey, List[Dict[str, str]]],
    policy: Dict[str, Any],
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Aggregate identity definitions once per distinct sig_hash."""
    value_stats: DefaultDict[str, DefaultDict[ItemValue, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(lambda: {
            "sig_hash_count": 0, "record_count": 0, "file_ids": set()
        })
    )
    discriminator_key = ((policy.get("shape_gating", {}) or {}).get("discriminator_key", ""))
    discriminator_values: Set[str] = set()
    records_total = sum(int(g["record_count"]) for g in signature_groups.values())
    all_files: Set[str] = set()
    for group in signature_groups.values():
        all_files.update(group["file_ids"])
    files_total = len(all_files)
    sig_hashes_total = len(signature_groups)

    for group_key in sorted(signature_groups):
        group = signature_groups[group_key]
        representative = group["representative_key"]
        seen: Set[Tuple[str, str, str]] = set()
        for item in identity_map.get(representative, []):
            k, v, q = item.get("k", ""), item.get("v", ""), item.get("q", "")
            if not k or (k, v, q) in seen:
                continue
            seen.add((k, v, q))
            stats = value_stats[k][(v, q)]
            stats["sig_hash_count"] += 1
            stats["record_count"] += int(group["record_count"])
            stats["file_ids"].update(group["file_ids"])
            if k == discriminator_key and q == "ok":
                discriminator_values.add(v)

    discriminator_value = next(iter(discriminator_values)) if len(discriminator_values) == 1 else ""
    settings: List[Dict[str, str]] = []
    supplemental: List[Dict[str, str]] = []
    for k in sorted(value_stats):
        values = value_stats[k]
        role = _join_role(policy, k, discriminator_value)
        if len(values) == 1:
            (v, q), stats = next(iter(values.items()))
            if int(stats["sig_hash_count"]) == sig_hashes_total:
                settings.append({
                    "segment_id": segment_id, "domain": domain,
                    "pattern_id": pattern_id, "join_hash": join_hash,
                    "k": k, "v": v, "q": q, "join_key_role": role,
                    "sig_hashes_present": str(stats["sig_hash_count"]),
                    "sig_hashes_total": str(sig_hashes_total),
                    "records_present": str(stats["record_count"]),
                    "records_total": str(records_total),
                    "files_present": str(len(stats["file_ids"])),
                    "files_total": str(files_total),
                })
                continue
        variation_count = len(values)
        for (v, q), stats in sorted(values.items()):
            record_count = int(stats["record_count"])
            supplemental.append({
                "segment_id": segment_id, "domain": domain,
                "pattern_id": pattern_id, "join_hash": join_hash,
                "k": k, "v": v, "q": q, "join_key_role": role,
                "sig_hash_count": str(stats["sig_hash_count"]),
                "sig_hashes_total": str(sig_hashes_total),
                "record_count": str(record_count),
                "records_total": str(records_total),
                "file_count": str(len(stats["file_ids"])),
                "files_total": str(files_total),
                "support_pct": f"{(100.0 * record_count / records_total) if records_total else 0.0:.6f}",
                "variation_count_for_key": str(variation_count),
            })
    return settings, supplemental


# ---------------------------------------------------------------------------
# Per-domain processing
# ---------------------------------------------------------------------------

def _build_pattern_reconstruction_summary(
    inventory_rows: List[Dict[str, str]],
    settings_rows: List[Dict[str, str]],
    supplemental_rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Build one deterministic summary row per pattern.

    The summary is descriptive, not a new authority decision:
      * INVARIANT_ONLY: at least one invariant setting and no supplemental keys.
      * HAS_VARIATION: one or more supplemental keys exist.
      * NO_RECONSTRUCTION_VALUES: neither output contains values for the pattern.
    """
    patterns: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for row in inventory_rows:
        key = (row.get("segment_id", ""), row.get("domain", ""), row.get("pattern_id", ""))
        if not key[2]:
            continue
        current = patterns.setdefault(key, {
            "segment_id": key[0],
            "domain": key[1],
            "pattern_id": key[2],
            "join_hash": row.get("join_hash", ""),
            "pattern_label": row.get("pattern_label", ""),
            "pattern_label_human": row.get("pattern_label_human", ""),
            "member_record_count": row.get("member_record_count", "0"),
            "member_sig_hash_count": row.get("member_sig_hash_count", "0"),
            "join_hash_consistent": row.get("join_hash_consistent", ""),
        })
        # Inventory repeats patterns across bundles. Retain the maximum observed
        # population diagnostics and the first nonblank descriptive values.
        for field in ("member_record_count", "member_sig_hash_count"):
            current[field] = str(max(_int_safe(current.get(field, "0")), _int_safe(row.get(field, "0"))))
        for field in ("join_hash", "pattern_label", "pattern_label_human", "join_hash_consistent"):
            if not current.get(field) and row.get(field):
                current[field] = row[field]

    settings_by_pattern: DefaultDict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
    supplemental_by_pattern: DefaultDict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in settings_rows:
        key = (row.get("segment_id", ""), row.get("domain", ""), row.get("pattern_id", ""))
        if key[2] and row.get("k") != "__no_items__":
            settings_by_pattern[key].append(row)
    for row in supplemental_rows:
        key = (row.get("segment_id", ""), row.get("domain", ""), row.get("pattern_id", ""))
        if key[2]:
            supplemental_by_pattern[key].append(row)

    output: List[Dict[str, str]] = []
    for key in sorted(patterns):
        base = patterns[key]
        stable = settings_by_pattern.get(key, [])
        variants = supplemental_by_pattern.get(key, [])
        stable_keys = {r.get("k", "") for r in stable if r.get("k", "")}
        variant_keys = {r.get("k", "") for r in variants if r.get("k", "")}
        join_required = {r.get("k", "") for r in stable if r.get("join_key_role") == "required"}
        join_optional = {r.get("k", "") for r in stable if r.get("join_key_role") == "optional"}
        if variant_keys:
            status = "HAS_VARIATION"
        elif stable_keys:
            status = "INVARIANT_ONLY"
        else:
            status = "NO_RECONSTRUCTION_VALUES"
        output.append({
            **base,
            "invariant_setting_count": str(len(stable_keys)),
            "supplemental_key_count": str(len(variant_keys)),
            "supplemental_value_row_count": str(len(variants)),
            "join_required_invariant_count": str(len(join_required)),
            "join_optional_invariant_count": str(len(join_optional)),
            "fully_invariant": "1" if stable_keys and not variant_keys else "0",
            "reconstruction_status": status,
        })
    return output

def _build_reconstruction_source_files(
    segment_id: str,
    domain: str,
    purge_view: str,
    in_scope_patterns: Set[str],
    pattern_record_map: Dict[str, Set[RecordKey]],
    record_meta: Dict[RecordKey, Dict[str, str]],
    pattern_map: Dict[str, Dict[str, str]],
) -> Tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    """Build a deterministic greedy source-file cover for emitted patterns.

    The CSV rows remain compact and contain only counts and coverage metrics.
    The JSON entries carry the human-readable file/domain/pattern relationship.
    A pattern is credited only to the first selected file that covers it.

    This is a greedy set-cover approximation, not proof of a globally minimum
    file count. Ties resolve by most new patterns, most new sig_hashes, most
    total in-scope patterns, then lexical export_run_id.
    """
    patterns_by_file: DefaultDict[str, Set[str]] = defaultdict(set)
    sig_hashes_by_file: DefaultDict[str, Set[str]] = defaultdict(set)

    for pattern_id in sorted(in_scope_patterns):
        for record_key in sorted(pattern_record_map.get(pattern_id, set())):
            export_run_id = record_key[0]
            if not export_run_id:
                continue
            patterns_by_file[export_run_id].add(pattern_id)
            sig_hash = record_meta.get(record_key, {}).get("sig_hash", "").strip()
            if sig_hash:
                sig_hashes_by_file[export_run_id].add(sig_hash)

    uncovered_patterns = set(in_scope_patterns)
    uncovered_sig_hashes: Set[str] = set()
    for values in sig_hashes_by_file.values():
        uncovered_sig_hashes.update(values)

    candidates = set(patterns_by_file)
    csv_rows: List[Dict[str, str]] = []
    json_entries: List[Dict[str, Any]] = []
    cumulative_pattern_count = 0
    cumulative_sig_hash_count = 0
    total_pattern_count = len(in_scope_patterns)
    total_sig_hash_count = len(uncovered_sig_hashes)
    cover_rank = 1

    while uncovered_patterns and candidates:
        ranked: List[Tuple[int, int, int, str, Set[str], Set[str]]] = []
        for export_run_id in candidates:
            new_patterns = patterns_by_file[export_run_id] & uncovered_patterns
            new_sig_hashes = sig_hashes_by_file[export_run_id] & uncovered_sig_hashes
            ranked.append((
                len(new_patterns),
                len(new_sig_hashes),
                len(patterns_by_file[export_run_id]),
                export_run_id,
                new_patterns,
                new_sig_hashes,
            ))
        ranked.sort(key=lambda item: (-item[0], -item[1], -item[2], item[3]))
        new_pattern_count, new_sig_hash_count, file_pattern_count, export_run_id, new_patterns, new_sig_hashes = ranked[0]
        if new_pattern_count == 0:
            break

        candidates.remove(export_run_id)
        uncovered_patterns.difference_update(new_patterns)
        uncovered_sig_hashes.difference_update(new_sig_hashes)
        cumulative_pattern_count += new_pattern_count
        cumulative_sig_hash_count += new_sig_hash_count

        csv_rows.append({
            "segment_id": segment_id,
            "domain": domain,
            "purge_view": purge_view,
            "cover_rank": str(cover_rank),
            "file_name": export_run_id,
            "new_pattern_count": str(new_pattern_count),
            "new_sig_hash_count": str(new_sig_hash_count),
            "file_in_scope_pattern_count": str(file_pattern_count),
            "cumulative_pattern_count": str(cumulative_pattern_count),
            "total_pattern_count": str(total_pattern_count),
            "cumulative_coverage_pct": (
                f"{(100.0 * cumulative_pattern_count / total_pattern_count):.6f}"
                if total_pattern_count else "0.000000"
            ),
            "remaining_pattern_count": str(len(uncovered_patterns)),
            "cumulative_sig_hash_count": str(cumulative_sig_hash_count),
            "total_sig_hash_count": str(total_sig_hash_count),
            "remaining_sig_hash_count": str(len(uncovered_sig_hashes)),
            "cover_complete": "1" if not uncovered_patterns else "0",
        })

        pattern_entries: List[Dict[str, str]] = []
        for pattern_id in sorted(new_patterns):
            metadata = pattern_map.get(pattern_id, {})
            pattern_name = (
                metadata.get("pattern_label_human", "").strip()
                or metadata.get("pattern_label", "").strip()
                or pattern_id
            )
            pattern_entries.append({
                "domain": domain,
                "pattern_name": pattern_name,
                "pattern_id": pattern_id,
            })
        json_entries.append({
            "file_name": export_run_id,
            "domain": domain,
            "cover_rank": cover_rank,
            "new_pattern_count": new_pattern_count,
            "cumulative_coverage_pct": round(
                (100.0 * cumulative_pattern_count / total_pattern_count)
                if total_pattern_count else 0.0,
                6,
            ),
            "patterns": pattern_entries,
        })
        cover_rank += 1

    return csv_rows, json_entries


def _process_domain(
    segment_id: str,
    domain: str,
    purge_view: str,
    ba_root: Path,
    domain_patterns_path: Path,
    pattern_membership_path: Path,
    records_csv: Path,
    identity_items_dir: Path,
    label_synth_dir: Path,
    join_policy: Dict[str, Any],
    top_bundles: Optional[int],
    top_bundles_auto: bool,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, Any]], int, int, int, int, Optional[Dict[str, Any]]]:
    domain_out_dir = ba_root / domain
    bundles_path = domain_out_dir / "bundles.csv"
    bundle_membership_path = domain_out_dir / "bundle_membership.csv"
    if not bundles_path.exists() or not bundle_membership_path.exists():
        print(f"[WARN] bundle inputs missing for domain '{domain}', skipping", file=sys.stderr)
        return [], [], [], [], [], [], 0, 0, 0, 0, None

    bundles = _read_csv(bundles_path)
    threshold_diag: Optional[Dict[str, Any]] = None
    if top_bundles_auto:
        threshold_diag = _compute_domain_bundle_threshold(bundles)
        bundles = _apply_bundle_threshold(bundles, threshold_diag)
        threshold_diag["n_bundles_after"] = len(bundles)
    elif top_bundles is not None:
        bundles = [r for r in bundles if _int_safe(r.get("bundle_rank", "")) <= top_bundles]

    pattern_map = _load_pattern_map(domain_patterns_path, domain)
    bundle_to_patterns: Dict[str, List[str]] = {}
    for row in _read_csv(bundle_membership_path):
        bid, pid = row.get("bundle_id", ""), row.get("pattern_id", "")
        if bid and pid:
            bundle_to_patterns.setdefault(bid, []).append(pid)
    in_scope_patterns = {
        pid for bundle in bundles
        for pid in bundle_to_patterns.get(bundle.get("bundle_id", ""), [])
    }

    pattern_record_map = _load_pattern_record_map(
        pattern_membership_path, domain, in_scope_patterns
    )
    all_member_keys = {
        key for pid in in_scope_patterns for key in pattern_record_map.get(pid, set())
    }
    record_meta = _load_record_metadata(records_csv, domain, all_member_keys)
    reconstruction_source_files, reconstruction_source_file_patterns = _build_reconstruction_source_files(
        segment_id=segment_id,
        domain=domain,
        purge_view=purge_view,
        in_scope_patterns=in_scope_patterns,
        pattern_record_map=pattern_record_map,
        record_meta=record_meta,
        pattern_map=pattern_map,
    )
    signature_map = _build_pattern_signature_map(pattern_record_map, record_meta)
    representative_keys = {
        group["representative_key"]
        for groups in signature_map.values() for group in groups.values()
    }
    identity_map = _load_identity_items(identity_items_dir, domain, representative_keys)
    label_map = _load_label_population(label_synth_dir, domain)

    inventory: List[Dict[str, str]] = []
    settings: List[Dict[str, str]] = []
    supplemental: List[Dict[str, str]] = []
    names: List[Dict[str, str]] = []
    seen_patterns: Set[str] = set()
    seen_names: Set[str] = set()

    for bundle in bundles:
        bundle_id = bundle.get("bundle_id", "")
        for pid in bundle_to_patterns.get(bundle_id, []):
            pm = pattern_map.get(pid, {})
            join_hash = pm.get("join_hash", "")
            members = pattern_record_map.get(pid, set())
            member_join_hashes = {record_meta.get(k, {}).get("join_hash", "") for k in members} - {""}
            groups = signature_map.get(pid, {})
            inventory.append({
                "segment_id": segment_id, "domain": domain, "purge_view": purge_view,
                "scope_key": bundle.get("scope_key", ""), "bundle_id": bundle_id,
                "bundle_rank": bundle.get("bundle_rank", ""),
                "bundle_files_present": bundle.get("files_present", ""),
                "bundle_files_total": bundle.get("files_total", ""),
                "bundle_support_pct": bundle.get("support_pct", ""),
                "bundle_pattern_count": bundle.get("pattern_count", ""),
                "pattern_id": pid, "join_hash": join_hash,
                "pattern_label": pm.get("pattern_label", ""),
                "pattern_label_human": pm.get("pattern_label_human", ""),
                "pattern_files_present": pm.get("pattern_files_present", ""),
                "member_record_count": str(len(members)),
                "member_join_hash_count": str(len(member_join_hashes)),
                "member_sig_hash_count": str(len(groups)),
                "join_hash_consistent": "1" if not member_join_hashes or member_join_hashes == {join_hash} else "0",
            })
            if pid not in seen_patterns:
                seen_patterns.add(pid)
                stable, variants = _aggregate_pattern_items(
                    segment_id, domain, pid, join_hash, groups, identity_map, join_policy
                )
                if stable:
                    settings.extend(stable)
                elif not groups:
                    settings.append({
                        "segment_id": segment_id, "domain": domain, "pattern_id": pid,
                        "join_hash": join_hash, "k": "__no_items__", "v": "",
                        "q": "missing", "join_key_role": "unclassified",
                        "sig_hashes_present": "0", "sig_hashes_total": "0",
                        "records_present": "0", "records_total": "0",
                        "files_present": "0", "files_total": "0",
                    })
                supplemental.extend(variants)
            if join_hash and join_hash not in seen_names:
                seen_names.add(join_hash)
                rows = label_map.get(join_hash, [])
                if rows:
                    for row in rows:
                        names.append({
                            "segment_id": segment_id, "domain": domain,
                            "join_hash": join_hash, "label_v": row.get("label_v", ""),
                            "label_q": row.get("label_q", ""),
                            "files_count": row.get("files_count", ""),
                        })
                else:
                    names.append({
                        "segment_id": segment_id, "domain": domain, "join_hash": join_hash,
                        "label_v": "", "label_q": "", "files_count": "",
                    })
    return (
        sorted(inventory, key=lambda r: (r["domain"], r["bundle_id"], r["pattern_id"])),
        sorted(settings, key=lambda r: (r["domain"], r["pattern_id"], r["k"])),
        sorted(supplemental, key=lambda r: (r["domain"], r["pattern_id"], r["k"], r["v"], r["q"])),
        sorted(names, key=lambda r: (r["domain"], r["join_hash"], r["label_v"])),
        reconstruction_source_files,
        reconstruction_source_file_patterns,
        len(bundles), len(inventory), len(seen_patterns), len(seen_names), threshold_diag,
    )


# ---------------------------------------------------------------------------
# Output field lists
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Output field lists
# ---------------------------------------------------------------------------

_INVENTORY_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "purge_view", "scope_key", "bundle_id", "bundle_rank",
    "bundle_files_present", "bundle_files_total", "bundle_support_pct", "bundle_pattern_count",
    "pattern_id", "join_hash", "pattern_label", "pattern_label_human", "pattern_files_present",
    "member_record_count", "member_join_hash_count", "member_sig_hash_count", "join_hash_consistent",
)
_SETTINGS_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "pattern_id", "join_hash", "k", "v", "q", "join_key_role",
    "sig_hashes_present", "sig_hashes_total", "records_present", "records_total",
    "files_present", "files_total",
)
_SUPPLEMENTAL_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "pattern_id", "join_hash", "k", "v", "q", "join_key_role",
    "sig_hash_count", "sig_hashes_total", "record_count", "records_total",
    "file_count", "files_total", "support_pct", "variation_count_for_key",
)
_SUMMARY_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "pattern_id", "join_hash",
    "pattern_label", "pattern_label_human",
    "member_record_count", "member_sig_hash_count", "join_hash_consistent",
    "invariant_setting_count", "supplemental_key_count", "supplemental_value_row_count",
    "join_required_invariant_count", "join_optional_invariant_count",
    "fully_invariant", "reconstruction_status",
)

_NAMES_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "join_hash", "label_v", "label_q", "files_count",
)
_THRESHOLDS_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "purge_view", "method", "break_value",
    "n_bundles_before", "n_bundles_after", "n_distinct_values",
    "source_value_min", "source_value_max",
)
_RECONSTRUCTION_SOURCE_FIELDS: Tuple[str, ...] = (
    "segment_id", "domain", "purge_view", "cover_rank", "file_name",
    "new_pattern_count", "new_sig_hash_count", "file_in_scope_pattern_count",
    "cumulative_pattern_count", "total_pattern_count", "cumulative_coverage_pct",
    "remaining_pattern_count", "cumulative_sig_hash_count", "total_sig_hash_count",
    "remaining_sig_hash_count", "cover_complete",
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
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
    ap.add_argument("--out-dir", required=True, help="Output directory for CSVs")
    ap.add_argument(
        "--join-policy", default=None,
        help="Optional domain_join_key_policies.json; classifies items but does not filter them",
    )
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

    segment_id, ba_root, domain_patterns_path, pattern_membership_path, records_csv, identity_items_dir, label_synth_dir = (
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
    all_supplemental: List[Dict[str, str]] = []
    all_names: List[Dict[str, str]] = []
    all_reconstruction_source_files: List[Dict[str, str]] = []
    all_reconstruction_source_file_patterns: List[Dict[str, Any]] = []
    all_thresholds: List[Dict[str, str]] = []

    total_domains = 0
    total_bundles = 0
    total_patterns = 0
    total_jh_items = 0
    total_jh_names = 0

    policies = _load_join_policies(Path(args.join_policy).resolve() if args.join_policy else None)

    for domain in domains:
        inv, sett, supplemental, names, reconstruction_source_files, reconstruction_source_file_patterns, nb, np_, jhi, jhn, threshold_diag = _process_domain(
            segment_id=segment_id,
            domain=domain,
            purge_view=args.purge_view,
            ba_root=ba_root,
            domain_patterns_path=domain_patterns_path,
            pattern_membership_path=pattern_membership_path,
            records_csv=records_csv,
            identity_items_dir=identity_items_dir,
            label_synth_dir=label_synth_dir,
            join_policy=policies.get(domain, {}),
            top_bundles=args.top_bundles,
            top_bundles_auto=args.top_bundles_auto,
        )
        if not inv and not sett and not names and nb == 0:
            # Domain was skipped (warning already printed to stderr)
            continue
        all_inventory.extend(inv)
        all_settings.extend(sett)
        all_supplemental.extend(supplemental)
        all_names.extend(names)
        all_reconstruction_source_files.extend(reconstruction_source_files)
        all_reconstruction_source_file_patterns.extend(reconstruction_source_file_patterns)
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
    _atomic_write_csv(out_dir / "pattern_supplemental_values.csv", _SUPPLEMENTAL_FIELDS, all_supplemental)
    all_summaries = _build_pattern_reconstruction_summary(
        all_inventory, all_settings, all_supplemental
    )
    _atomic_write_csv(
        out_dir / "pattern_reconstruction_summary.csv",
        _SUMMARY_FIELDS,
        all_summaries,
    )
    _atomic_write_csv(out_dir / "pattern_names.csv", _NAMES_FIELDS, all_names)
    _atomic_write_csv(
        out_dir / "reconstruction_source_files.csv",
        _RECONSTRUCTION_SOURCE_FIELDS,
        all_reconstruction_source_files,
    )
    _atomic_write_json(
        out_dir / "reconstruction_source_file_patterns.json",
        all_reconstruction_source_file_patterns,
    )
    if args.top_bundles_auto:
        _atomic_write_csv(out_dir / "bundle_selection_thresholds.csv", _THRESHOLDS_FIELDS, all_thresholds)

    print(
        f"Done. domains={total_domains} bundles={total_bundles} "
        f"patterns={total_patterns} "
        f"join_hashes_with_items={total_jh_items} "
        f"join_hashes_with_names={total_jh_names} "
        f"invariant_rows={len(all_settings)} supplemental_rows={len(all_supplemental)} "
        f"summary_rows={len(all_summaries)} "
        f"reconstruction_source_file_rows={len(all_reconstruction_source_files)} "
        f"reconstruction_source_pattern_groups={len(all_reconstruction_source_file_patterns)}"
    )


if __name__ == "__main__":
    main()
