from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

if __package__ in (None, ""):
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import SCHEMA_VERSION, atomic_write_csv, derive_scope_key, read_csv_rows, resolve_analysis_run_id
else:
    from .common import SCHEMA_VERSION, atomic_write_csv, derive_scope_key, read_csv_rows, resolve_analysis_run_id


def _load_population_file_ids(registry_dir: Path, domain: str, population_id: str, run_id: str) -> Set[str]:
    corpus_path = registry_dir / "corpus_populations.csv"
    if not corpus_path.is_file():
        raise FileNotFoundError(
            "corpus_populations.csv not found. Run step0 with --discover-populations "
            "before running step1 with --population-id."
        )
    rows = read_csv_rows(corpus_path)
    available = sorted(
        {
            r.get("population_id", "")
            for r in rows
            if r.get("analysis_run_id", "") == run_id and r.get("domain", "") == domain and r.get("population_role", "") == "primary"
        }
    )
    if population_id not in available:
        raise ValueError(
            f"population_id {population_id!r} not found for domain {domain!r}. "
            f"Available population_ids: {available}"
        )
    return {
        r.get("export_run_id", "")
        for r in rows
        if r.get("analysis_run_id", "") == run_id
        and r.get("domain", "") == domain
        and r.get("population_id", "") == population_id
        and r.get("population_role", "") == "primary"
    }


def build_membership_matrix(
    analysis_dir: Path,
    out_dir: Path,
    domain: str,
    analysis_run_id: str = "",
    population_id: Optional[str] = None,
    population_registry_dir: Optional[Path] = None,
    scope_key_filter: Optional[str] = None,
) -> Dict[str, int]:
    pattern_presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
    domain_pattern_rows = read_csv_rows(analysis_dir / "domain_patterns.csv")

    run_id = resolve_analysis_run_id(pattern_presence_rows, analysis_run_id)
    population_file_ids: Optional[Set[str]] = None
    all_domain_files = {
        (row.get("export_run_id", "") or "").strip()
        for row in pattern_presence_rows
        if row.get("analysis_run_id", "") == run_id and row.get("domain", "") == domain and (row.get("export_run_id", "") or "").strip()
    }
    if population_id:
        registry_dir = population_registry_dir if population_registry_dir is not None else out_dir
        population_file_ids = _load_population_file_ids(registry_dir, domain, population_id, run_id)
        print(
            f"[step1] domain={domain} population_id={population_id} files_after_filter={len(population_file_ids)} "
            f"files_excluded={max(0, len(all_domain_files) - len(population_file_ids))} (population filter applied)"
        )

    pattern_meta: Dict[str, Dict[str, str]] = {}
    cad_patterns: Set[str] = set()

    for row in domain_pattern_rows:
        if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != domain:
            continue
        pid = row.get("pattern_id", "")
        if not pid:
            continue
        pattern_meta[pid] = row
        if (row.get("is_cad_import", "") or "").strip().lower() == "true":
            cad_patterns.add(pid)

    pairs_seen: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)
    membership_rows: List[Dict[str, str]] = []
    files_by_scope: Dict[str, Set[str]] = defaultdict(set)
    patterns_by_scope: Dict[str, Set[str]] = defaultdict(set)

    for row in pattern_presence_rows:
        if row.get("analysis_run_id", "") != run_id or row.get("domain", "") != domain:
            continue
        export_run_id = (row.get("export_run_id", "") or "").strip()
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not export_run_id or not pattern_id:
            continue
        if population_file_ids is not None and export_run_id not in population_file_ids:
            continue
        if pattern_id in cad_patterns:
            continue
        meta = pattern_meta.get(pattern_id)
        if meta is None:
            continue
        scope_key = derive_scope_key(domain, meta)
        if scope_key_filter is not None and scope_key_filter != "" and scope_key != scope_key_filter:
            continue
        pair = (export_run_id, pattern_id)
        if pair in pairs_seen[scope_key]:
            raise ValueError(f"Duplicate (export_run_id, pattern_id) in scope {scope_key!r}: {pair}")
        pairs_seen[scope_key].add(pair)
        membership_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": run_id,
                "domain": domain,
                "scope_key": scope_key,
                "export_run_id": export_run_id,
                "pattern_id": pattern_id,
            }
        )
        files_by_scope[scope_key].add(export_run_id)
        patterns_by_scope[scope_key].add(pattern_id)

    membership_rows.sort(key=lambda r: (r["domain"], r["scope_key"], r["export_run_id"], r["pattern_id"]))

    scope_rows: List[Dict[str, str]] = []
    for scope_key in sorted(set(files_by_scope.keys()) | set(patterns_by_scope.keys())):
        files_count = len(files_by_scope.get(scope_key, set()))
        patterns_count = len(patterns_by_scope.get(scope_key, set()))
        if files_count < 2:
            print(f"[step1][warn] domain={domain} scope={scope_key!r} has fewer than 2 files ({files_count})")
        scope_rows.append(
            {
                "schema_version": SCHEMA_VERSION,
                "analysis_run_id": run_id,
                "domain": domain,
                "scope_key": scope_key,
                "files_in_scope": str(files_count),
                "patterns_in_scope": str(patterns_count),
            }
        )

    domain_out_dir = out_dir / domain
    atomic_write_csv(
        domain_out_dir / "membership_matrix.csv",
        ["schema_version", "analysis_run_id", "domain", "scope_key", "export_run_id", "pattern_id"],
        membership_rows,
    )
    atomic_write_csv(
        domain_out_dir / "scope_registry.csv",
        ["schema_version", "analysis_run_id", "domain", "scope_key", "files_in_scope", "patterns_in_scope"],
        sorted(scope_rows, key=lambda r: (r["domain"], r["scope_key"])),
    )

    total_files = len({row["export_run_id"] for row in membership_rows})
    total_patterns = len({row["pattern_id"] for row in membership_rows})
    print(
        f"[step1] domain={domain} files_in_scope={total_files} patterns_in_scope={total_patterns} scopes_count={len(scope_rows)}"
    )
    return {
        "analysis_run_id": run_id,
        "membership_rows": len(membership_rows),
        "scopes": len(scope_rows),
    }


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build bundle membership matrix")
    p.add_argument("--analysis-dir", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--domain", required=True)
    p.add_argument("--analysis-run-id", default="")
    p.add_argument("--population-id", default=None)
    p.add_argument("--population-registry-dir", type=Path, default=None)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    build_membership_matrix(
        args.analysis_dir,
        args.out_dir,
        args.domain,
        args.analysis_run_id,
        args.population_id,
        args.population_registry_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
