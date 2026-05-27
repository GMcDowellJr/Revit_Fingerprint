#!/usr/bin/env python3
"""
Compute latent purgeable signal for reference domains.

Single-pass GetUnusedElements returns universally false for reference domains
(arrowheads, line_patterns, view_filter_definitions, phase_filters) because
their consumers are still present.  This tool traverses the reference graph
in the flat CSV exports to identify which of those records are one level
deep from being purgeable: if every non-purgeable consumer of a sig_hash is
itself purgeable, the target is latently purgeable.

Reads from existing flat CSV outputs (no re-extraction required).
Writes latent_purgeable.csv.  Does NOT mutate any existing CSV.

Usage:
    python tools/compute_latent_purgeable.py \\
        --records-dir <path_to_extractor_out_dir> \\
        [--out-file   <path>/latent_purgeable.csv] \\
        [--chains     arrowheads,line_patterns] \\
        [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple


OUTPUT_SCHEMA_VERSION = "1.0"

_OUTPUT_FIELDS = [
    "schema_version",
    "export_run_id",
    "domain",
    "record_pk",
    "sig_hash",
    "latent_purgeable",
    "latent_purgeable_reason",
    "consuming_domains",
    "consumer_count_in_use",
    "consumer_count_total",
]

# ---------------------------------------------------------------------------
# Chain configuration — one entry per reference domain
# ---------------------------------------------------------------------------
# Each chain maps a target domain (the reference type whose purgeability we
# are computing) to the consumer domains that hold references to it, and the
# item_key(s) in the consumer's identity_items shard that carry those refs.
#
# ref_item_keys=None signals wildcard matching via _build_matcher().
#
# Deferred (not implemented): materials ← object_styles_model
# The obj_style.material_sig_hash and materials sig_hash live in different
# hash spaces; fix requires ctx lookup in object_styles + re-extraction.
# ---------------------------------------------------------------------------

CHAINS: List[dict] = [
    {
        "target_domain": "arrowheads",
        "consumer_domains": [
            "dimension_types_linear",
            "dimension_types_angular",
            "dimension_types_radial",
            "dimension_types_diameter",
            "dimension_types_spot_elevation",
            "dimension_types_spot_coordinate",
            "dimension_types_spot_slope",
            "text_types",
        ],
        "ref_item_keys": [
            "dim_type.tick_mark_sig_hash",
            "text_type.leader_arrowhead_sig_hash",
        ],
    },
    {
        "target_domain": "line_patterns",
        "consumer_domains": [
            "object_styles_model",
        ],
        "ref_item_keys": [
            "obj_style.pattern_ref.sig_hash",
        ],
    },
    {
        "target_domain": "phase_filters",
        "consumer_domains": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
            "view_templates_elevations_sections_detail",
            "view_templates_schedules",
            "view_templates_renderings_drafting",
        ],
        "ref_item_keys": [
            "view_template.sig.phase_filter",
        ],
    },
    {
        "target_domain": "view_filter_definitions",
        "consumer_domains": [
            "view_templates_floor_structural_area_plans",
            "view_templates_ceiling_plans",
            "view_templates_elevations_sections_detail",
            "view_templates_schedules",
            "view_templates_renderings_drafting",
        ],
        # Wildcard: view_template.sig.filter[NNN].def_sig for any zero-padded NNN
        "ref_item_keys": None,
    },
]


# ---------------------------------------------------------------------------
# Matchers
# ---------------------------------------------------------------------------

def _is_vfd_ref_key(item_key: str) -> bool:
    return (
        item_key.startswith("view_template.sig.filter[")
        and item_key.endswith("].def_sig")
    )


def _build_matcher(chain: dict) -> Callable[[str], bool]:
    """Return a callable(item_key) -> bool for the chain's ref items."""
    if chain["ref_item_keys"] is None:
        if chain["target_domain"] == "view_filter_definitions":
            return _is_vfd_ref_key
        raise ValueError(
            f"No wildcard matcher defined for target domain: {chain['target_domain']}"
        )
    exact_keys: Set[str] = set(chain["ref_item_keys"])
    return lambda key, _ek=exact_keys: key in _ek


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_purgeable_true(val: str) -> bool:
    return (val or "").strip().lower() == "true"


def _make_zero_counts() -> dict:
    return {"in_use": 0, "total": 0}


def _select_chains(chains_arg: Optional[str]) -> List[dict]:
    if not chains_arg:
        return CHAINS
    wanted = {s.strip() for s in chains_arg.split(",") if s.strip()}
    selected = [c for c in CHAINS if c["target_domain"] in wanted]
    unknown = wanted - {c["target_domain"] for c in selected}
    if unknown:
        raise SystemExit(
            f"Unknown chain target domain(s): {', '.join(sorted(unknown))}\n"
            f"Available: {', '.join(c['target_domain'] for c in CHAINS)}"
        )
    return selected


def _domains_of_interest(chains: List[dict]) -> Set[str]:
    out: Set[str] = set()
    for c in chains:
        out.add(c["target_domain"])
        out.update(c["consumer_domains"])
    return out


# ---------------------------------------------------------------------------
# Step 1 — Load records index
# ---------------------------------------------------------------------------

def _load_records(
    records_csv: Path,
    domains_of_interest: Set[str],
) -> Tuple[
    Dict[str, Dict[str, List[dict]]],   # domain_records[run_id][domain] = [{...}]
    Dict[str, Set[str]],                # domains_present[run_id] = set of domain names
    Dict[Tuple[str, str, str], str],    # purgeable_by_pk[(run_id, domain, pk)] = val
]:
    """Single-pass load of records.csv for all domains of interest.

    domain_records carries both target and consumer domain records; the caller
    filters by target_domain when generating output rows.

    domains_present tracks which consumer domains have at least one record per
    export_run_id — used for consumer_domain_absent detection.

    purgeable_by_pk provides the is_purgeable value for each consumer record so
    Step 2 can decide whether a reference is in_use.
    """
    domain_records: Dict[str, Dict[str, List[dict]]] = defaultdict(
        lambda: defaultdict(list)
    )
    domains_present: Dict[str, Set[str]] = defaultdict(set)
    purgeable_by_pk: Dict[Tuple[str, str, str], str] = {}

    with open(records_csv, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            domain = row.get("domain", "")
            if domain not in domains_of_interest:
                continue
            run_id = row.get("export_run_id", "")
            record_pk = row.get("record_pk", "")
            sig_hash = row.get("sig_hash", "")
            is_purgeable = row.get("is_purgeable", "")

            domains_present[run_id].add(domain)
            purgeable_by_pk[(run_id, domain, record_pk)] = is_purgeable
            domain_records[run_id][domain].append(
                {
                    "record_pk": record_pk,
                    "sig_hash": sig_hash,
                    "is_purgeable": is_purgeable,
                }
            )

    return dict(domain_records), dict(domains_present), purgeable_by_pk


# ---------------------------------------------------------------------------
# Step 2 — Load reference items per chain
# ---------------------------------------------------------------------------

def _accumulate_item_rows(
    fh,
    filter_domain: Optional[str],
    matcher: Callable[[str], bool],
    purgeable_by_pk: Dict[Tuple[str, str, str], str],
    consumer_domain: str,
    valid_run_ids: Set[str],
    ref_data_by_run: Dict[str, dict],
) -> None:
    """Stream one item CSV file and accumulate ref sets into ref_data_by_run."""
    reader = csv.DictReader(fh)
    for row in reader:
        if filter_domain and row.get("domain") != filter_domain:
            continue

        item_key = row.get("item_key", "")
        if not matcher(item_key):
            continue

        item_value = row.get("item_value", "")
        if not item_value:
            continue

        row_run_id = row.get("export_run_id", "")
        if row_run_id not in valid_run_ids:
            continue

        record_pk = row.get("record_pk", "")
        is_purg = purgeable_by_pk.get((row_run_id, consumer_domain, record_pk), "")

        ref = ref_data_by_run[row_run_id]
        ref["all_hashes"].add(item_value)
        ref["counts"][item_value]["total"] += 1

        if not _is_purgeable_true(is_purg):
            ref["in_use_hashes"].add(item_value)
            ref["counts"][item_value]["in_use"] += 1


def _load_chain_ref_data(
    records_dir: Path,
    chain: dict,
    all_run_ids: Set[str],
    purgeable_by_pk: Dict[Tuple[str, str, str], str],
) -> Tuple[Dict[str, dict], Set[str]]:
    """Build ref sets for one chain across all consumer domains.

    Returns:
      ref_data_by_run[run_id] = {
          "in_use_hashes": set of sig_hash values from non-purgeable consumers,
          "all_hashes":    set of sig_hash values from all consumers,
          "counts":        defaultdict(dict) keyed by sig_hash,
                           each entry {"in_use": int, "total": int}
      }
      missing_shard_domains: set of consumer domain names whose item file
                             could not be opened (shard missing + no fallback)
    """
    matcher = _build_matcher(chain)

    ref_data_by_run: Dict[str, dict] = {
        run_id: {
            "in_use_hashes": set(),
            "all_hashes": set(),
            "counts": defaultdict(_make_zero_counts),
        }
        for run_id in all_run_ids
    }

    missing_shard_domains: Set[str] = set()
    shard_dir = records_dir / "identity_items_by_domain"
    fallback_csv = records_dir / "identity_items.csv"

    # Split consumer domains into those with shard files and those needing fallback.
    # If the fallback is needed for multiple domains, scan it once.
    shard_list: List[Tuple[str, Path]] = []   # (domain, path) for shard files
    fallback_needed: List[str] = []            # domains that must use fallback

    for consumer_domain in chain["consumer_domains"]:
        shard_path = shard_dir / f"{consumer_domain}.csv"
        if shard_dir.exists() and shard_path.exists():
            shard_list.append((consumer_domain, shard_path))
        elif fallback_csv.exists():
            fallback_needed.append(consumer_domain)
        else:
            missing_shard_domains.add(consumer_domain)

    # Process individual shard files
    for consumer_domain, path in shard_list:
        try:
            with open(path, encoding="utf-8-sig", newline="") as fh:
                _accumulate_item_rows(
                    fh,
                    filter_domain=None,
                    matcher=matcher,
                    purgeable_by_pk=purgeable_by_pk,
                    consumer_domain=consumer_domain,
                    valid_run_ids=all_run_ids,
                    ref_data_by_run=ref_data_by_run,
                )
        except (IOError, OSError) as exc:
            print(
                f"  WARNING: cannot read shard {path}: {exc}",
                file=sys.stderr,
            )
            missing_shard_domains.add(consumer_domain)

    # Process fallback file once, routing rows to each needed domain
    if fallback_needed:
        fallback_domain_set = set(fallback_needed)
        try:
            with open(fallback_csv, encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    row_domain = row.get("domain", "")
                    if row_domain not in fallback_domain_set:
                        continue

                    item_key = row.get("item_key", "")
                    if not matcher(item_key):
                        continue

                    item_value = row.get("item_value", "")
                    if not item_value:
                        continue

                    row_run_id = row.get("export_run_id", "")
                    if row_run_id not in all_run_ids:
                        continue

                    record_pk = row.get("record_pk", "")
                    is_purg = purgeable_by_pk.get(
                        (row_run_id, row_domain, record_pk), ""
                    )

                    ref = ref_data_by_run[row_run_id]
                    ref["all_hashes"].add(item_value)
                    ref["counts"][item_value]["total"] += 1
                    if not _is_purgeable_true(is_purg):
                        ref["in_use_hashes"].add(item_value)
                        ref["counts"][item_value]["in_use"] += 1

        except (IOError, OSError) as exc:
            print(
                f"  WARNING: cannot read fallback identity_items.csv: {exc}",
                file=sys.stderr,
            )
            missing_shard_domains.update(fallback_needed)

    return ref_data_by_run, missing_shard_domains


# ---------------------------------------------------------------------------
# Step 3 — Classify target records
# ---------------------------------------------------------------------------

def _classify(
    rec: dict,
    chain: dict,
    run_id: str,
    domains_present: Dict[str, Set[str]],
    ref_data_by_run: Dict[str, dict],
    missing_shard_domains: Set[str],
) -> Tuple[str, str, int, int]:
    """Return (latent_purgeable, reason, consumer_count_in_use, consumer_count_total).

    Priority order mirrors the spec:
      blocked_no_sig_hash > is_purgeable_direct > consumer_domain_absent >
      ref_field_missing > has_nopurgeable_consumer > all_consumers_purgeable >
      no_consumers_in_scope
    """
    sig_hash = rec["sig_hash"]

    # Blocked: no sig_hash to match against
    if not sig_hash or sig_hash.strip().lower() == "none":
        return ("indeterminate", "blocked_no_sig_hash", 0, 0)

    # Direct: already reported purgeable at extraction time
    if _is_purgeable_true(rec["is_purgeable"]):
        return ("true", "is_purgeable_direct", 0, 0)

    consumer_domains = chain["consumer_domains"]
    present_for_run = domains_present.get(run_id, set())

    # Any consumer domain had zero records for this export_run_id — cannot infer
    if any(d not in present_for_run for d in consumer_domains):
        return ("indeterminate", "consumer_domain_absent", 0, 0)

    # All present consumer domains had missing shard files — no ref data at all
    if all(d in missing_shard_domains for d in consumer_domains):
        return ("indeterminate", "ref_field_missing", 0, 0)

    ref = ref_data_by_run.get(run_id)
    if ref is None:
        return ("indeterminate", "ref_field_missing", 0, 0)

    counts_entry = ref["counts"].get(sig_hash, {"in_use": 0, "total": 0})
    in_use_cnt = counts_entry["in_use"]
    total_cnt = counts_entry["total"]

    if sig_hash in ref["in_use_hashes"]:
        return ("false", "has_nopurgeable_consumer", in_use_cnt, total_cnt)
    if sig_hash in ref["all_hashes"]:
        return ("true", "all_consumers_purgeable", in_use_cnt, total_cnt)
    return ("true", "no_consumers_in_scope", 0, 0)


# ---------------------------------------------------------------------------
# Step 4 — Write output
# ---------------------------------------------------------------------------

def _write_output(out_file: Path, rows: List[dict]) -> None:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Step 5 — Summary
# ---------------------------------------------------------------------------

def _fmt_consumers(consumer_domains: List[str]) -> str:
    """Compact human-readable consumer list for summary output."""
    if not consumer_domains:
        return "(none)"
    # Abbreviate dimension_types_* and view_templates_* groups
    prefixes: Dict[str, List[str]] = defaultdict(list)
    ungrouped: List[str] = []
    for d in consumer_domains:
        for prefix in ("dimension_types_", "view_templates_"):
            if d.startswith(prefix):
                prefixes[prefix].append(d)
                break
        else:
            ungrouped.append(d)
    parts: List[str] = []
    for prefix, members in prefixes.items():
        stem = prefix.rstrip("_")
        if len(members) > 1:
            parts.append(f"{stem}_*")
        else:
            parts.extend(members)
    parts.extend(ungrouped)
    return ", ".join(parts)


def _print_summary(
    chain_summaries: List[dict],
    dry_run: bool,
    out_file: Path,
) -> None:
    if dry_run:
        print("\n[dry-run] No output file written.")
    else:
        print(f"\nWrote: {out_file}")

    print()
    for summary in chain_summaries:
        chain = summary["chain"]
        stats = summary["stats"]
        rc = summary["reason_counts"]
        consumer_desc = _fmt_consumers(chain["consumer_domains"])
        latent_total = (
            rc.get("all_consumers_purgeable", 0) + rc.get("no_consumers_in_scope", 0)
        )
        indet_total = stats["indeterminate"]
        cda = rc.get("consumer_domain_absent", 0)
        rfm = rc.get("ref_field_missing", 0)
        bns = rc.get("blocked_no_sig_hash", 0)
        print(
            f"Chain: {chain['target_domain']}"
            f" (consumers: {consumer_desc})"
        )
        print(f"  Files processed:       {stats['run_count']:>6}")
        print(
            f"  Direct purgeable:      {stats['direct']:>6}"
            f"  (is_purgeable_direct)"
        )
        print(
            f"  Latent purgeable:      {latent_total:>6}"
            f"  (all_consumers_purgeable + no_consumers_in_scope)"
        )
        print(f"  In use:                {stats['in_use']:>6}")
        print(
            f"  Indeterminate:         {indet_total:>6}"
            f"  (consumer_domain_absent: {cda},"
            f" ref_field_missing: {rfm},"
            f" blocked: {bns})"
        )
        print(f"  Total records:         {stats['total']:>6}")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    ap.add_argument(
        "--records-dir",
        required=True,
        metavar="DIR",
        help=(
            "Path to extractor out_dir containing records.csv and"
            " identity_items_by_domain/"
        ),
    )
    ap.add_argument(
        "--out-file",
        default=None,
        metavar="FILE",
        help="Output CSV path (default: <records-dir>/latent_purgeable.csv)",
    )
    ap.add_argument(
        "--chains",
        default=None,
        metavar="DOMAINS",
        help=(
            "Comma-separated target domain names to process"
            " (default: all chains).\n"
            f"Available: {', '.join(c['target_domain'] for c in CHAINS)}"
        ),
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts without writing the output file",
    )
    args = ap.parse_args()

    records_dir = Path(args.records_dir)
    if not records_dir.is_dir():
        raise SystemExit(f"records-dir not found or not a directory: {records_dir}")

    records_csv = records_dir / "records.csv"
    if not records_csv.exists():
        raise SystemExit(f"records.csv not found in: {records_dir}")

    out_file = (
        Path(args.out_file) if args.out_file else records_dir / "latent_purgeable.csv"
    )

    active_chains = _select_chains(args.chains)
    doi = _domains_of_interest(active_chains)

    print(f"Loading records from {records_csv} ...")
    domain_records, domains_present, purgeable_by_pk = _load_records(records_csv, doi)

    all_run_ids: Set[str] = (
        set(domain_records.keys()) | set(domains_present.keys())
    )
    print(
        f"  {len(all_run_ids)} export_run_id(s) found,"
        f" {len(doi)} domain(s) of interest."
    )

    output_rows: List[dict] = []
    chain_summaries: List[dict] = []

    for chain in active_chains:
        target_domain = chain["target_domain"]
        consumer_domains = chain["consumer_domains"]
        print(f"Processing chain: {target_domain} ...")

        ref_data_by_run, missing_shard_domains = _load_chain_ref_data(
            records_dir, chain, all_run_ids, purgeable_by_pk
        )

        stats: Dict[str, int] = {
            "run_count": 0,
            "total": 0,
            "direct": 0,
            "in_use": 0,
            "indeterminate": 0,
        }
        reason_counts: Dict[str, int] = defaultdict(int)
        consuming_domains_str = "|".join(consumer_domains)

        for run_id in sorted(all_run_ids):
            run_domain_map = domain_records.get(run_id, {})
            if target_domain not in run_domain_map:
                continue
            stats["run_count"] += 1
            for rec in run_domain_map[target_domain]:
                lp, reason, in_use_cnt, total_cnt = _classify(
                    rec,
                    chain,
                    run_id,
                    domains_present,
                    ref_data_by_run,
                    missing_shard_domains,
                )
                output_rows.append(
                    {
                        "schema_version": OUTPUT_SCHEMA_VERSION,
                        "export_run_id": run_id,
                        "domain": target_domain,
                        "record_pk": rec["record_pk"],
                        "sig_hash": rec["sig_hash"],
                        "latent_purgeable": lp,
                        "latent_purgeable_reason": reason,
                        "consuming_domains": consuming_domains_str,
                        "consumer_count_in_use": str(in_use_cnt),
                        "consumer_count_total": str(total_cnt),
                    }
                )
                stats["total"] += 1
                reason_counts[reason] += 1
                if lp == "true":
                    if reason == "is_purgeable_direct":
                        stats["direct"] += 1
                elif lp == "false":
                    stats["in_use"] += 1
                else:
                    stats["indeterminate"] += 1

        chain_summaries.append(
            {"chain": chain, "stats": stats, "reason_counts": reason_counts}
        )

    # Sort: (export_run_id, domain, record_pk)
    output_rows.sort(
        key=lambda r: (r["export_run_id"], r["domain"], r["record_pk"])
    )

    if not args.dry_run:
        _write_output(out_file, output_rows)
        print(f"\nWrote {len(output_rows)} row(s) to {out_file}")

    _print_summary(chain_summaries, args.dry_run, out_file)


if __name__ == "__main__":
    main()
