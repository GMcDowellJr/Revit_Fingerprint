#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Identity-Projection-Parameterized pattern generation (PR2).

Parameterizes pattern construction over `--comparison-target {config,name,both}` so
downstream comparison can run against either the existing configuration-based `join_hash`
or the Canonical Name Identity Projection's `join_key_name_identity` (PR1), selected
explicitly per run.

Scope, carried in from the PR2 brief (do not re-litigate here):
  - Consumes ONLY the analysis-side reconstruction path shipped in PR1
    (tools/apply_name_key_policy.py / core/name_key_builder.py) for the `name` target --
    never the inline domains/*.py extractor wiring.
  - `config` target (the default) reproduces today's production pattern output
    byte-for-byte. Rather than re-deriving `tools/extractor.py`'s clustering algorithm (a
    correctness risk with no re-extracted corpus available to validate against), this tool
    treats the already-produced `Results_v21/analysis_v21/domain_patterns.csv` as the
    source of truth and republishes it verbatim under the new namespaced output root --
    it never recomputes or overwrites the authoritative file.
  - `name` target clusters tools/apply_name_key_policy.py's per-record `join_hash` output
    (status == "ok" only -- a missing_required/blocked/missing_policy row has no usable
    join_hash to cluster on) into patterns, restricted to the 25 Native+Widened eligible
    domains (core/name_key_coverage.py), tagging each pattern with its coverage class.
    Excluded-class domains (and any domain outside the traced 37) are reported explicitly
    in domain_coverage.csv with a reason -- never silently absent.
  - `both` target runs both and writes them to separate namespaced subdirectories; pattern
    IDs never collide between them because the pattern_id formula
    (docs/PATTERN_ID_AND_LABEL_RULES.md) hashes in `join_key_schema`, which always differs
    between the two projections for the same domain.

Output layout (nests under Results_v21/, per PR1's own namespacing precedent -- not a new
`analysis/` root):

    Results_v21/name_key/patterns/config/domain_patterns.csv   (verbatim copy of the
                                                                  production join_hash
                                                                  pattern output)
    Results_v21/name_key/patterns/name/domain_patterns.csv     (join_key_name_identity
                                                                  patterns, coverage-class
                                                                  tagged)
    Results_v21/name_key/patterns/name/pattern_membership.csv  (record -> pattern_id)
    Results_v21/name_key/patterns/name/domain_coverage.csv     (all 37 traced domains,
                                                                  eligible or excluded, with
                                                                  reason)

Usage:
    python tools/generate_name_key_patterns.py --comparison-target config \\
        --config-patterns-csv Results_v21/analysis_v21/domain_patterns.csv \\
        --out-root Results_v21/name_key/patterns

    python tools/generate_name_key_patterns.py --comparison-target name \\
        --name-key-csv Results_v21/name_key/name_key_results.csv \\
        --out-root Results_v21/name_key/patterns

    python tools/generate_name_key_patterns.py --comparison-target both \\
        --config-patterns-csv Results_v21/analysis_v21/domain_patterns.csv \\
        --name-key-csv Results_v21/name_key/name_key_results.csv \\
        --out-root Results_v21/name_key/patterns
"""
from __future__ import annotations

import argparse
import csv
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from pattern_id_utils import build_clusters, pattern_label, rank_clusters  # noqa: E402

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from core.name_key_coverage import (  # noqa: E402
    COVERAGE_EXCLUDED,
    COVERAGE_NATIVE,
    COVERAGE_PHASES_REDUNDANT,
    COVERAGE_WIDENED,
    ELIGIBLE_DOMAINS,
    EXCLUDED_DOMAINS,
    coverage_class,
    exclusion_reason,
)

DOMAIN_PATTERNS_FIELDS = [
    "domain",
    "coverage_class",
    "pattern_id",
    "pattern_label",
    "join_key_schema",
    "join_hash",
    "source_cluster_id",
    "pattern_rank",
    "pattern_size_records",
    "pattern_size_files",
]
MEMBERSHIP_FIELDS = ["domain", "coverage_class", "export_file", "record_id", "pattern_id"]
DOMAIN_COVERAGE_FIELDS = ["domain", "coverage_class", "included", "reason"]


def _read_csv(path: Path) -> List[Dict[str, str]]:
    """Read a CSV into a list of str-coerced row dicts (None -> "").

    --- trace ---
    reads: CSV file at `path` -- caller-supplied; in this module's call sites, either the
        production domain_patterns.csv (--config-patterns-csv) or the name-key results CSV
        (--name-key-csv, tools/apply_name_key_policy.py's output).
    calls: none (stdlib csv.DictReader).
    thresholds: none.
    returns: list[dict[str,str]]; consumed by emit_name_patterns() and
        _assert_no_pattern_id_collision().
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [{k: ("" if v is None else str(v)) for k, v in row.items()} for row in reader]


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    """Write `rows` to `path` as CSV with a fixed header, creating parent dirs as needed.

    --- trace ---
    reads: `fieldnames` and `rows`, both caller-supplied (one of the module-level
        DOMAIN_PATTERNS_FIELDS/MEMBERSHIP_FIELDS/DOMAIN_COVERAGE_FIELDS constants plus the
        matching row list, from emit_name_patterns()).
    calls: none (stdlib csv.DictWriter).
    thresholds: none.
    returns: writes CSV to `path`; a row missing a fieldname is written as "" (row.get
        default) rather than raising. Consumed downstream as domain_patterns.csv /
        pattern_membership.csv / domain_coverage.csv by tools/run_segment_orchestrator.py
        and (for domain_patterns.csv's schema shape only) tools/compare_cross_segment.py's
        conventions -- see build_name_patterns()'s source_cluster_id comment.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def emit_config_patterns(config_patterns_csv: Path, out_dir: Path) -> Path:
    """Republish today's production join_hash pattern output verbatim (byte-identical),
    under the new namespaced location. Never overwrites the authoritative source file.

    --- trace ---
    reads: `config_patterns_csv` -- CLI --config-patterns-csv, expected to be the existing
        Results_v21/analysis_v21/domain_patterns.csv produced by tools/extractor.py's
        production pattern pipeline (read as raw bytes via copy, never parsed here).
    calls: shutil.copyfile().
    thresholds: output filename "domain_patterns.csv" is a hardcoded literal (the
        production output filename convention), not sourced from a constant table.
    returns: Path to the copied file (`out_dir/domain_patterns.csv`); used by main() for
        the --comparison-target both collision check and printed to stdout.
    """
    if not config_patterns_csv.is_file():
        raise FileNotFoundError(f"--config-patterns-csv not found: {config_patterns_csv}")
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "domain_patterns.csv"
    shutil.copyfile(config_patterns_csv, dest)
    return dest


def build_name_patterns(name_key_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Cluster name-key rows (tools/apply_name_key_policy.py output) into pattern records,
    restricted to eligible (Native/Widened) domains, tagged with coverage_class.

    --- trace ---
    reads: `name_key_rows` -- tools/apply_name_key_policy.py's output rows, passed in by
        emit_name_patterns() after _read_csv() on --name-key-csv; ELIGIBLE_DOMAINS
        (imported from core.name_key_coverage, l.77-86).
    calls: pattern_id_utils.build_clusters(); pattern_id_utils.rank_clusters() (per
        domain); pattern_id_utils.pattern_label(); core.name_key_coverage.coverage_class().
    thresholds: the row-eligibility filter `status == "ok" and join_hash` is an inline
        literal comparison here, not a named constant -- a scanner that only looks for
        assigned constants would miss this "ok" threshold. ELIGIBLE_DOMAINS itself is a
        module constant in core/name_key_coverage.py, sourced from PR1's Step-0 audit
        (audit_results/audit_6_name_key_step0_within_pr1.md).
    returns: list[dict] pattern rows (domain/coverage_class/pattern_id/pattern_label/join_key_schema
        /join_hash/source_cluster_id/pattern_rank/pattern_size_records/pattern_size_files);
        consumed by emit_name_patterns() (written to
        domain_patterns.csv) and by build_name_membership() (as `pattern_rows`).
    """
    eligible_rows = [
        r for r in name_key_rows
        if r.get("domain") in ELIGIBLE_DOMAINS and r.get("status") == "ok" and r.get("join_hash")
    ]
    clusters = build_clusters(eligible_rows)

    by_domain: Dict[str, List[Dict[str, Any]]] = {}
    for cluster in clusters.values():
        by_domain.setdefault(cluster["domain"], []).append(cluster)

    out_rows: List[Dict[str, Any]] = []
    for domain, domain_clusters in sorted(by_domain.items()):
        ranked = rank_clusters(domain_clusters)
        n = len(ranked)
        for rank, cluster in enumerate(ranked, start=1):
            out_rows.append({
                "domain": domain,
                "coverage_class": coverage_class(domain),
                "pattern_id": cluster["pattern_id"],
                "pattern_label": pattern_label(cluster["join_key_schema"], rank, n),
                "join_key_schema": cluster["join_key_schema"],
                "join_hash": cluster["join_hash"],
                # Matches tools/extractor.py's cluster_id convention exactly (domain|schema|
                # join_hash) -- tools/compare_cross_segment.py's resolve_join_hashes() reads
                # this field (not join_hash directly) to identify a pattern row and treats a
                # row without it as missing_source_cluster_id/skipped. This PR does not wire
                # name-projection output into compare_cross_segment.py (explicitly out of
                # scope -- see PR2 brief's Do-NOT list), but the schema stays consistent with
                # the production domain_patterns.csv convention so a later PR can consume it
                # without a schema migration.
                "source_cluster_id": f"{domain}|{cluster['join_key_schema']}|{cluster['join_hash']}",
                "pattern_rank": rank,
                "pattern_size_records": cluster["pattern_size_records"],
                "pattern_size_files": cluster["pattern_size_files"],
            })
    return out_rows


def build_name_membership(name_key_rows: List[Dict[str, str]], pattern_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map each eligible-domain name-key row to the pattern_id its cluster was assigned in
    build_name_patterns().

    --- trace ---
    reads: `name_key_rows` -- same tools/apply_name_key_policy.py rows as
        build_name_patterns(); `pattern_rows` -- build_name_patterns()'s return value,
        passed in by emit_name_patterns(); ELIGIBLE_DOMAINS.
    calls: core.name_key_coverage.coverage_class().
    thresholds: ELIGIBLE_DOMAINS filter (same source as build_name_patterns()).
    returns: list[dict] membership rows (domain/coverage_class/export_file/record_id/pattern_id);
        consumed by emit_name_patterns() (written to
        pattern_membership.csv). A row whose (domain, join_key_schema, join_hash) key isn't
        found in `pid_by_cluster_key` gets pattern_id="" rather than being dropped -- e.g. a
        row build_name_patterns() itself excluded for status != "ok".
    """
    # Keyed by (domain, join_key_schema, join_hash) -- matching build_clusters()'s cluster
    # key exactly. A (domain, join_hash) pair alone is not unique if the same domain/hash
    # ever appears under two different schemas (e.g. mid schema-migration input); dropping
    # the schema here would silently misroute those records to the wrong pattern_id.
    pid_by_cluster_key: Dict[tuple, str] = {
        (r["domain"], r["join_key_schema"], r["join_hash"]): r["pattern_id"] for r in pattern_rows
    }
    out_rows: List[Dict[str, Any]] = []
    for r in name_key_rows:
        domain = r.get("domain", "")
        if domain not in ELIGIBLE_DOMAINS:
            continue
        pid = pid_by_cluster_key.get((domain, r.get("join_key_schema", ""), r.get("join_hash", "")), "")
        out_rows.append({
            "domain": domain,
            "coverage_class": coverage_class(domain),
            "export_file": r.get("export_file", ""),
            "record_id": r.get("record_id", ""),
            "pattern_id": pid,
        })
    return out_rows


def build_domain_coverage(observed_domains: Iterable[str] = ()) -> List[Dict[str, Any]]:
    """Every domain traced in PR1's Step-0-within-PR1 audit (25 eligible + 12 excluded),
    so absence of an excluded domain from domain_patterns.csv is explicit, not silently
    missing -- per the PR2 brief's explicit prohibition on silent exclusion.

    `observed_domains` (the domains actually present in this run's name-key input) are
    included too: a domain outside both the eligible and excluded registries -- schema
    drift, a stale input, or a new domain the registry hasn't caught up with yet -- would
    otherwise be filtered out of patterns/membership with no coverage row at all, silently
    disappearing instead of surfacing as a `not_traced` exclusion.

    --- trace ---
    reads: `observed_domains` -- an iterable of domain strings, passed in by
        emit_name_patterns() as the domain column of `name_key_rows` (this run's actual
        input, not a static list); ELIGIBLE_DOMAINS, EXCLUDED_DOMAINS (module constants
        from core.name_key_coverage, l.77-86, themselves the codified PR1 Step-0 audit: 25
        eligible + 12 excluded, asserted in core/name_key_coverage.py l.86-88).
    calls: core.name_key_coverage.coverage_class(); core.name_key_coverage.exclusion_reason().
    thresholds: ELIGIBLE_DOMAINS/EXCLUDED_DOMAINS registries (counts asserted at import
        time in core/name_key_coverage.py, not re-validated here).
    returns: list[dict] coverage rows (domain/coverage_class/included/reason), always
        containing every eligible + excluded domain plus any observed-but-untraced ones;
        consumed by emit_name_patterns() (written to domain_coverage.csv).
    """
    known = set(ELIGIBLE_DOMAINS) | set(EXCLUDED_DOMAINS)
    rows: List[Dict[str, Any]] = []
    for domain in sorted(ELIGIBLE_DOMAINS):
        rows.append({
            "domain": domain,
            "coverage_class": coverage_class(domain),
            "included": "true",
            "reason": "",
        })
    for domain in sorted(EXCLUDED_DOMAINS):
        rows.append({
            "domain": domain,
            "coverage_class": COVERAGE_EXCLUDED,
            "included": "false",
            "reason": exclusion_reason(domain),
        })
    for domain in sorted(set(observed_domains) - known):
        rows.append({
            "domain": domain,
            "coverage_class": coverage_class(domain),
            "included": "false",
            "reason": exclusion_reason(domain),
        })
    return rows


def emit_name_patterns(name_key_csv: Path, out_dir: Path) -> Path:
    """Load the name-key CSV, build patterns/membership/coverage, and write all three
    output CSVs under `out_dir`.

    --- trace ---
    reads: `name_key_csv` -- CLI --name-key-csv, tools/apply_name_key_policy.py's output.
    calls: _read_csv(); build_name_patterns(); build_name_membership();
        build_domain_coverage(); _write_csv() (x3, one per output file).
    thresholds: none directly -- inherits ELIGIBLE_DOMAINS/EXCLUDED_DOMAINS/status=="ok"
        thresholds from the functions it calls. Output filenames ("domain_patterns.csv",
        "pattern_membership.csv", "domain_coverage.csv") are hardcoded literals.
    returns: Path to `out_dir/domain_patterns.csv`; used by main() for stdout logging and
        (in `both` mode) passed to _assert_no_pattern_id_collision().
    """
    if not name_key_csv.is_file():
        raise FileNotFoundError(f"--name-key-csv not found: {name_key_csv}")
    name_key_rows = _read_csv(name_key_csv)

    pattern_rows = build_name_patterns(name_key_rows)
    membership_rows = build_name_membership(name_key_rows, pattern_rows)
    coverage_rows = build_domain_coverage(r.get("domain", "") for r in name_key_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    patterns_path = out_dir / "domain_patterns.csv"
    _write_csv(patterns_path, DOMAIN_PATTERNS_FIELDS, pattern_rows)
    _write_csv(out_dir / "pattern_membership.csv", MEMBERSHIP_FIELDS, membership_rows)
    _write_csv(out_dir / "domain_coverage.csv", DOMAIN_COVERAGE_FIELDS, coverage_rows)
    return patterns_path


def _assert_no_pattern_id_collision(config_patterns_csv_out: Path, name_patterns_csv_out: Path) -> None:
    """Re-read both just-written domain_patterns.csv outputs and fail loudly if any
    pattern_id appears in both (would indicate the schema-in-hash guarantee broke).

    --- trace ---
    reads: `config_patterns_csv_out`, `name_patterns_csv_out` -- the two Paths
        emit_config_patterns()/emit_name_patterns() just wrote, passed in by main() only
        when --comparison-target both.
    calls: _read_csv() (x2).
    thresholds: none.
    returns: None; raises RuntimeError on any pattern_id intersection (up to 5 examples in
        the message). Called only by main(); a clean run prints a confirmation to stdout.
    """
    config_ids = {r["pattern_id"] for r in _read_csv(config_patterns_csv_out) if r.get("pattern_id")}
    name_ids = {r["pattern_id"] for r in _read_csv(name_patterns_csv_out) if r.get("pattern_id")}
    collisions = config_ids & name_ids
    if collisions:
        raise RuntimeError(
            f"comparison_target=both produced colliding pattern_ids across projections: {sorted(collisions)[:5]}"
        )


def main() -> None:
    """CLI entry point: dispatch to emit_config_patterns()/emit_name_patterns() per
    --comparison-target, then cross-check for pattern_id collisions in `both` mode.

    --- trace ---
    reads: CLI args --comparison-target (config/name/both, default config),
        --config-patterns-csv, --name-key-csv, --out-root (default
        "Results_v21/name_key/patterns"). Invoked by tools/corpus_update_runbook.ps1's
        Run B-NameKey step (l.163, --comparison-target name) and by
        tools/run_segment_orchestrator.py's per-segment Step 2b (l.1151/1506, also
        --comparison-target name).
    calls: emit_config_patterns() (if target includes "config"); emit_name_patterns() (if
        target includes "name"); _assert_no_pattern_id_collision() (only if target=="both").
    thresholds: default --out-root literal; --comparison-target choices list
        ["config","name","both"]. NOTE: `targets` is built as a runtime list
        (`["config","name"] if ... else [args.comparison_target]`) and dispatch is by
        `if "config" in targets` / `if "name" in targets` membership tests rather than a
        literal per-value branch -- still statically traceable (both call sites are
        lexically present), but a naive "grep for the CLI value next to the call" scan
        would need to follow this list construction rather than matching a direct string.
    returns: writes CSV outputs under `out_root/config/` and/or `out_root/name/`; prints
        each written path to stdout. Downstream consumers: tools/compare_cross_segment.py's
        schema conventions (not wired in this PR -- see build_name_patterns()'s
        source_cluster_id comment) and tools/run_segment_orchestrator.py's per-segment
        results tree.
    """
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--comparison-target", default="config", choices=["config", "name", "both"],
        help="Which join-basis projection to build patterns against: the existing "
             "configuration join_hash (config, default -- byte-identical to today's "
             "production output), the Canonical Name Identity Projection's "
             "join_key_name_identity (name), or both, namespaced separately (both).",
    )
    ap.add_argument("--config-patterns-csv", default=None, help="Existing production domain_patterns.csv (required for config/both).")
    ap.add_argument("--name-key-csv", default=None, help="Output of tools/apply_name_key_policy.py (required for name/both).")
    ap.add_argument("--out-root", default="Results_v21/name_key/patterns", help="Output root directory.")
    args = ap.parse_args()

    out_root = Path(args.out_root)
    targets = ["config", "name"] if args.comparison_target == "both" else [args.comparison_target]

    written: Dict[str, Path] = {}
    if "config" in targets:
        if not args.config_patterns_csv:
            raise SystemExit("--config-patterns-csv is required for comparison_target=config/both")
        written["config"] = emit_config_patterns(Path(args.config_patterns_csv), out_root / "config")
        print(f"[generate_name_key_patterns] config target -> {written['config']}")

    if "name" in targets:
        if not args.name_key_csv:
            raise SystemExit("--name-key-csv is required for comparison_target=name/both")
        written["name"] = emit_name_patterns(Path(args.name_key_csv), out_root / "name")
        print(f"[generate_name_key_patterns] name target -> {written['name']}")

    if args.comparison_target == "both":
        _assert_no_pattern_id_collision(written["config"], written["name"])
        print("[generate_name_key_patterns] both target: no pattern_id collision confirmed")


if __name__ == "__main__":
    main()
