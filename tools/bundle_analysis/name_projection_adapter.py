from __future__ import annotations

"""Adapter between PR2's name-identity-projection pattern output
(`Results_v21/name_key/patterns/name/`) and the bundle-analysis pipeline's expected
`analysis_dir` shape (`domain_patterns.csv` / `pattern_presence_file.csv` as
`tools/extractor.py` writes them for the `config` target).

`step0_discover_populations.py`, `step1_membership_matrix.py`, and
`step2b_bundle_share_profile.py` all read those two files by literal name and column set.
Rather than teaching every step file a second, comparison-target-aware schema (a
larger diff surface touching code that must stay byte-identical for the `config`
target), this module normalizes PR2's output into that exact shape once, at the boundary,
so the unmodified step0-step7 pipeline can run against it unchanged.

See audit_results/audit_8_bundle_pipeline_name_projection.md for the full enumeration of
schema gaps this module papers over and why each default is safe -- every default here is
logged and/or carried into `emit_name_target_provenance()`'s manifest, never silent.
"""

from pathlib import Path
from typing import Dict, List, Optional, Set

if __package__ in (None, ""):
    import sys

    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from common import atomic_write_csv, read_csv_rows
else:
    from .common import atomic_write_csv, read_csv_rows

DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID = "name_projection"
DEFAULT_SCHEMA_VERSION = "2.1"

# Matches tools/extractor.py's domain_patterns.csv column order/names exactly, plus
# join_key_schema appended (needed by bundle_analysis/common.py's derive_scope_key() for
# SHAPE_GATED_DOMAINS; PR2's name-target schema already carries this column natively).
STAGED_DOMAIN_PATTERNS_FIELDS = [
    "schema_version", "analysis_run_id", "domain", "pattern_id", "pattern_label",
    "source_cluster_id", "pattern_size_records", "pattern_size_files", "pattern_rank",
    "is_candidate_standard", "notes",
    "pattern_label_human", "pattern_label_source", "pattern_label_fallback", "is_cad_import",
    "semantic_group", "join_key_schema",
]

# Matches tools/extractor.py's pattern_presence_file.csv column order/names exactly.
STAGED_PATTERN_PRESENCE_FIELDS = [
    "schema_version", "analysis_run_id", "export_run_id", "domain", "pattern_id",
    "pattern_share_pct", "is_dominant_pattern", "deviation_score", "corpus_classification",
]

DOMAIN_COVERAGE_FIELDS = ["domain", "coverage_class", "included", "reason"]

def normalize_export_run_id(export_file: str, known_ids: Optional[Set[str]] = None) -> str:
    """Normalize PR2's `export_file` (`tools/apply_name_key_policy.py`, which prefers
    `*.details.json` per CLAUDE.md's input-format priority) back to the canonical
    `export_run_id` `tools/extractor.py`'s `emit_records()` actually stamps -- the file
    `_iter_export_files()` picks as `primary`. For a split-export pair, `primary` is
    always the `*.index.json` file when one exists (`_iter_export_files()`: `if idx is
    not None: split_pairs.append((idx.name, idx, det))`), never the `*.details.json` file
    it was paired with. Left uncorrected, name-target `export_run_id` values never match
    `file_metadata.csv` / config-target `pattern_presence_file.csv` for a split-export
    corpus, which silently drops every split-export file out of `--roles` filtering and
    breaks cross-target file-level alignment (flagged in PR #389 review).

    `*.index.json` and plain `*.json` export names are left unchanged -- those are
    already the primary/canonical name in those cases.

    Blindly rewriting every `*.details.json` suffix is only correct for a *complete*
    split-export pair; a details-only export (no sibling `*.index.json` at all -- a
    legitimate, `_iter_export_files()`-supported case, not an out-of-contract one, per
    CLAUDE.md's own details-preferred priority) keeps the `*.details.json` name itself as
    its canonical `export_run_id`, and blindly rewriting it produces an id that never
    matches anything real (PR #390 review). Pass `known_ids` (e.g. `file_metadata.csv`'s
    real `export_run_id` set) whenever available to resolve this correctly: the normalized
    form is tried first, and if that isn't in `known_ids`, the raw (un-normalized) value is
    tried before falling back to the normalized guess. Without `known_ids` (the default),
    behavior is unchanged from before this parameter existed -- always the blind rewrite.
    """
    normalized = export_file
    if export_file.lower().endswith(".details.json"):
        normalized = export_file[: -len(".details.json")] + ".index.json"
    if known_ids is None:
        return normalized
    if normalized in known_ids:
        return normalized
    if export_file in known_ids:
        return export_file
    return normalized


PROVENANCE_NOTE_NAME_TARGET = (
    "name-identity values are analysis-side-reconstructed "
    "(tools/apply_name_key_policy.py / core/name_key_builder.py) and have not yet been "
    "cross-checked against a live re-extracted corpus -- PR2's agreement check (N=25, "
    "match_rate=1.0) reconstructed the inline computation from source rather than "
    "validating against real exports. See "
    "audit_results/audit_7_name_key_agreement_and_cli_naming.md Item 0."
)


def stage_name_projection_analysis_dir(
    name_patterns_dir: Path,
    staging_dir: Path,
    analysis_run_id: str = DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    known_export_run_ids: Optional[Set[str]] = None,
) -> Dict[str, int]:
    """Materialize a staging `analysis_dir` from PR2's `name/` pattern output.

    Reads (PR2 shape, `tools/generate_name_key_patterns.py`):
      name_patterns_dir/domain_patterns.csv
      name_patterns_dir/pattern_membership.csv
      name_patterns_dir/domain_coverage.csv   (optional; carried through verbatim if present)

    Writes (bundle-pipeline shape, `tools/extractor.py`):
      staging_dir/domain_patterns.csv
      staging_dir/pattern_presence_file.csv
      staging_dir/domain_coverage.csv         (verbatim copy, for provenance reporting)

    Known, deliberate degradations vs. the `config`-target production schema (see
    audit_results/audit_8_bundle_pipeline_name_projection.md items 4-5):
      - `is_cad_import` is always emitted empty (false) -- PR2's output carries no CAD-import
        evidence, so no name-projection pattern is ever excluded as a CAD import.
      - `pattern_label_human` is populated from PR2's `pattern_label` (same synthesized
        "<schema> - Variant N of M" string) rather than a genuinely human-authored label.
      - `analysis_run_id` is a constant (not derived from the input), since PR2's output has
        no run-id concept of its own; this keeps `resolve_analysis_run_id()`'s "exactly one
        distinct value" invariant satisfied deterministically.

    `export_file` is normalized to the canonical `export_run_id` via
    `normalize_export_run_id()` (not copied verbatim) -- see that function's docstring for
    why a naive copy silently breaks `--roles` filtering and cross-target file alignment on
    split-export corpora. Pass `known_export_run_ids` (real ids from `file_metadata.csv`)
    whenever available so a details-only export (no sibling `*.index.json`) resolves
    correctly instead of being blindly rewritten to a nonexistent id (PR #390 review) --
    `run_bundle_analysis_for_target()` supplies this automatically from `--metadata-file`
    when one is given.
    """
    domain_patterns_path = name_patterns_dir / "domain_patterns.csv"
    membership_path = name_patterns_dir / "pattern_membership.csv"
    coverage_path = name_patterns_dir / "domain_coverage.csv"
    if not domain_patterns_path.is_file():
        raise FileNotFoundError(f"name-projection domain_patterns.csv not found: {domain_patterns_path}")
    if not membership_path.is_file():
        raise FileNotFoundError(f"name-projection pattern_membership.csv not found: {membership_path}")

    pattern_rows = read_csv_rows(domain_patterns_path)
    membership_rows = read_csv_rows(membership_path)

    out_pattern_rows: List[Dict[str, str]] = []
    for row in pattern_rows:
        domain = row.get("domain", "")
        out_pattern_rows.append({
            "schema_version": schema_version,
            "analysis_run_id": analysis_run_id,
            "domain": domain,
            "pattern_id": row.get("pattern_id", ""),
            "pattern_label": row.get("pattern_label", ""),
            "source_cluster_id": row.get("source_cluster_id", ""),
            "pattern_size_records": row.get("pattern_size_records", ""),
            "pattern_size_files": row.get("pattern_size_files", ""),
            "pattern_rank": row.get("pattern_rank", ""),
            "is_candidate_standard": "",
            "notes": "",
            "pattern_label_human": row.get("pattern_label", ""),
            "pattern_label_source": "name_projection_pattern_label",
            "pattern_label_fallback": "",
            "is_cad_import": "",
            "semantic_group": "",
            "join_key_schema": row.get("join_key_schema", ""),
        })

    presence_seen: Set[tuple] = set()
    out_presence_rows: List[Dict[str, str]] = []
    for row in membership_rows:
        domain = row.get("domain", "")
        export_run_id = normalize_export_run_id(
            (row.get("export_file", "") or "").strip(), known_ids=known_export_run_ids
        )
        pattern_id = (row.get("pattern_id", "") or "").strip()
        if not export_run_id or not pattern_id:
            continue
        key = (domain, export_run_id, pattern_id)
        if key in presence_seen:
            continue
        presence_seen.add(key)
        out_presence_rows.append({
            "schema_version": schema_version,
            "analysis_run_id": analysis_run_id,
            "export_run_id": export_run_id,
            "domain": domain,
            "pattern_id": pattern_id,
            "pattern_share_pct": "",
            "is_dominant_pattern": "false",
            "deviation_score": "",
            "corpus_classification": "",
        })

    out_pattern_rows.sort(key=lambda r: (r["domain"], r["pattern_id"]))
    out_presence_rows.sort(key=lambda r: (r["domain"], r["export_run_id"], r["pattern_id"]))

    atomic_write_csv(staging_dir / "domain_patterns.csv", STAGED_DOMAIN_PATTERNS_FIELDS, out_pattern_rows)
    atomic_write_csv(staging_dir / "pattern_presence_file.csv", STAGED_PATTERN_PRESENCE_FIELDS, out_presence_rows)

    coverage_rows: List[Dict[str, str]] = []
    if coverage_path.is_file():
        coverage_rows = read_csv_rows(coverage_path)
        atomic_write_csv(staging_dir / "domain_coverage.csv", DOMAIN_COVERAGE_FIELDS, coverage_rows)

    return {
        "patterns": len(out_pattern_rows),
        "presence_rows": len(out_presence_rows),
        "domains": len({r["domain"] for r in out_pattern_rows}),
        "coverage_rows": len(coverage_rows),
    }


def emit_name_target_provenance(
    view_out_dir: Path,
    name_patterns_dir: Path,
    analysis_run_id: str = DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
) -> Dict[str, int]:
    """Declare, per bundle produced under `comparison_target=name`: comparison_target,
    coverage_class (Native/Widened/phases_redundant, from core/name_key_coverage.py via
    PR2's own domain_coverage.csv), and the analysis-side-reconstruction provenance note.

    Also copies the full domain_coverage.csv registry (all 25 eligible + 12 excluded
    domains) into the output root and writes a README stating excluded-domain absence
    explicitly, per the PR3 brief's "explicit absence, not silent gap" requirement.
    """
    coverage_path = name_patterns_dir / "domain_coverage.csv"
    coverage_rows = read_csv_rows(coverage_path) if coverage_path.is_file() else []
    coverage_by_domain = {r.get("domain", ""): r.get("coverage_class", "") for r in coverage_rows}
    excluded_rows = sorted(
        (r for r in coverage_rows if (r.get("included", "") or "").strip().lower() != "true"),
        key=lambda r: r.get("domain", ""),
    )

    provenance_rows: List[Dict[str, str]] = []
    for bundles_path in sorted(view_out_dir.rglob("bundles.csv")):
        for row in read_csv_rows(bundles_path):
            domain = row.get("domain", "")
            if not domain:
                continue
            provenance_rows.append({
                "analysis_run_id": row.get("analysis_run_id", "") or analysis_run_id,
                "comparison_target": "name",
                "domain": domain,
                "scope_key": row.get("scope_key", ""),
                "bundle_id": row.get("bundle_id", ""),
                "coverage_class": coverage_by_domain.get(domain, ""),
                "provenance_note": PROVENANCE_NOTE_NAME_TARGET,
            })
    provenance_rows.sort(key=lambda r: (r["domain"], r["scope_key"], r["bundle_id"]))

    atomic_write_csv(
        view_out_dir / "bundle_provenance.csv",
        ["analysis_run_id", "comparison_target", "domain", "scope_key", "bundle_id", "coverage_class", "provenance_note"],
        provenance_rows,
    )
    if coverage_rows:
        atomic_write_csv(view_out_dir / "domain_coverage.csv", DOMAIN_COVERAGE_FIELDS, coverage_rows)

    excluded_lines = "\n".join(
        f"- `{r.get('domain', '')}` ({r.get('coverage_class', '')}): {r.get('reason', '')}"
        for r in excluded_rows
    ) or "(no excluded-domain rows found in domain_coverage.csv)"

    readme = f"""# Name-projection bundle output (comparison_target=name)

{PROVENANCE_NOTE_NAME_TARGET}

Only `ALL` view is supported for this comparison_target -- USED-view purgeability
filtering, `--compute-share-profile`, and `--compare` all depend on production
(`config`-target) artifacts with no defined name-projection equivalent yet, and are
blocked explicitly rather than silently downgraded. See
`audit_results/audit_8_bundle_pipeline_name_projection.md` items 7, 9, 10.

## Excluded domains

These domains never appear anywhere in this output -- `core/name_key_coverage.py` has no
usable own-name evidence for them. This is an explicit absence (see `domain_coverage.csv`
for the full registry of all eligible + excluded domains and per-domain reasons), not a
silent gap:

{excluded_lines}

See `bundle_provenance.csv` for the per-bundle `comparison_target`/`coverage_class`
declaration required for every bundle in this output.
"""
    view_out_dir.mkdir(parents=True, exist_ok=True)
    (view_out_dir / "README.md").write_text(readme, encoding="utf-8")

    return {"bundles_annotated": len(provenance_rows), "excluded_domains": len(excluded_rows)}
