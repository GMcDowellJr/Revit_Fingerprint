"""Scope-consistency analysis for governed patterns.

Redesign of an earlier `analyze_promotion_candidates.py` prototype (not
previously checked into this repo). Answers a narrower, more defensible
question than the prototype did: for a locally-active pattern, does its
observed reuse breadth (`reuse_scope`, from `pattern_reuse_distribution.csv`)
exceed the broadest scope at which it is already governed by a Template or
Container (`seeded_scope`, from `cross_segment_governance_states.csv`)?

This is a **scope-consistency classification**, not a promotion decision.
`candidate_class` values describe where a pattern's reuse footprint sits
relative to governance, not an approval. See the "Read this first" section
of the generated `promotion_candidate_summary.md` for the same disclaimer
in the output artifact itself.

Standalone tool. Not wired into `run_extract_all.py` or
`generate_governance_narrative.py`; does not call `assign_tier()`; does not
write into the governance evidence package. `--root` points at a folder
containing `cross_segment_governance_states.csv` and
`pattern_reuse_distribution.csv` (both written by
`tools/compare_cross_segment.py` / its narrative-layer callers).

Scope taxonomy
---------------
Reuses `compare_cross_segment.py`'s own `_scope_level()` taxonomy rather than
inventing a parallel one, with one adaptation forced by what the two input
CSVs actually carry (confirmed by reading both files' row-construction code,
not just their docs):

- `_scope_level()` itself returns exactly three non-null values --
  `enterprise`, `business_center`, `client_business_center` -- plus `None`
  for rows where client_label/business_center_label aren't both cut (a
  roll-up). There is no fourth "project" level in the function itself, even
  though `docs/cross_segment_comparison.md` section 2 describes one; the
  code is authoritative. This tool renames the three to `enterprise` / `bc`
  / `client` and adds `ungoverned` as the floor value for "not seeded by any
  Template/Container at all" / "reuse hasn't reached client-wide breadth
  yet" -- the same four-value shape the task brief asked for, reached by
  reading the real function rather than assuming its return values.

- `seeded_scope` is derived from `cross_segment_governance_states.csv`,
  which carries `comparison_type` + `governance_role_reference` +
  `business_center_label_reference` per row but no `client_label` column at
  all. `comparison_type` already names the scope-level edge by
  construction (`enterprise_to_project`/`enterprise_to_bc`/
  `enterprise_to_client` -> enterprise; `bc_to_project` -> bc;
  `template_to_project`/`template_to_container`/`container_to_project` are
  the client-scoped governance-chain edges per
  `docs/cross_segment_comparison.md` section 2 -> client). `generic_to_*`
  rows are excluded -- Generic is raw stock, not a governance standard,
  matching the prototype's own `already_seeded = any_template | any_container`
  (no `any_generic`).

- `reuse_scope` is derived from `pattern_reuse_distribution.csv`, filtered
  to `view_scope == "all"` and `governance_role == "Project"` rows only.
  The prototype this replaces did *not* apply either filter, which silently
  blended configured-vocabulary breadth from Template/Container/Generic
  rows and used-view breadth into what was reported as reuse. That is a
  real defect, not a style choice: `docs/cross_segment_comparison.md`
  explicitly warns that Template/Generic/most-Container all-view rows are
  "configured/published inventory, not active usage claims."

  Known upstream gap, confirmed by reading `build_pattern_reuse_distribution_rows()`
  in `compare_cross_segment.py`: the row grouping key is
  `(view_scope, governance_role, client_label, discipline_label, unit_system, domain)`
  -- there is no `business_center_label` in that key. `reuse_scope` can
  therefore **never** resolve to `bc`; multiple real business centers
  sharing the same `client_label` (including every Stantec-internal bc,
  since `client_label == "Stantec"` for all of them) collapse into one pool.
  A `client_label == "Stantec"` reuse row is flagged via
  `reuse_client_pool_is_stantec_internal` so a reader can see when "client"
  scope actually means "all of Stantec's business centers pooled together,"
  not one real external client. This means a genuine `seeded_scope == "bc"`
  case can show up as `reuse_scope < seeded_scope` (routed to
  governed_but_underused.csv) purely because reuse breadth has no bc grain
  to resolve into -- not because real-world reuse is actually narrower than
  governance. Treat `bc`-scope rows in governed_but_underused.csv with that
  caveat in mind; this tool does not attempt to paper over the gap with an
  invented threshold.

No corpus data was available in the environment this tool was written in
(no `results/`/`segments/` export directories exist in this repo -- they
are runtime outputs generated from real Revit projects, never checked into
git). Per the project-owner's explicit direction, `--baseline-threshold`
and `--min-enterprise-clients` therefore stay configurable CLI knobs with
the prototype's original defaults rather than Jenks-derived cuts; no
distribution shape was available to check for a natural break, and forcing
one without data would be exactly the kind of false-precision this redesign
is supposed to remove. Run `--verbose` against real data and inspect
`domain_rollup.csv`'s `avg_project_penetration`/`max_project_penetration`
columns before deciding whether a derived cut is warranted later.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# CONSTANTS
# ============================================================

ALL_PRIORITY_DOMAINS = [
    "text_types",
    "dimension_types_linear",
    "dimension_types_angular",
    "dimension_types_diameter",
    "dimension_types_radial",
    "dimension_types_spot_coordinate",
    "dimension_types_spot_elevation",
    "dimension_types_spot_slope",
    "fill_patterns_drafting",
    "fill_patterns_model",
    "floor_types",
    "ceiling_types",
    "object_styles_model",
    "view_category_overrides_model",
    "loaded_family_types",
]

# Ordinal only -- widest reach ranks highest. Never treat as a magnitude;
# it exists purely to compare two scope labels with `<`/`>`/`==`.
SCOPE_RANK = {"ungoverned": 0, "client": 1, "bc": 2, "enterprise": 3}

# comparison_type -> scope level of the reference (Template/Container) side.
# See module docstring for why each mapping is what it is.
COMPARISON_TYPE_TO_SEED_SCOPE = {
    "enterprise_to_bc": "enterprise",
    "enterprise_to_client": "enterprise",
    "enterprise_to_project": "enterprise",
    "bc_to_project": "bc",
    "template_to_project": "client",
    "template_to_container": "client",
    "container_to_project": "client",
    # generic_to_template / generic_to_container / generic_to_project are
    # deliberately absent: Generic is raw stock, not a governance standard.
}

# reuse_bucket -> reuse_scope. single_project/emerging/single_file all sit
# below client-wide breadth, so none of them constitute a governance-scope
# claim on their own -- they are adoption/early-signal buckets, not scope.
REUSE_BUCKET_TO_SCOPE = {
    "corpus_wide": "enterprise",
    "client_wide": "client",
    "multi_project": "client",
    "single_project": "ungoverned",
    "emerging": "ungoverned",
    "single_file": "ungoverned",
    # "unclassified" is intentionally absent -- handled as its own routed
    # diagnostic bucket, never silently folded into "ungoverned".
}


# ============================================================
# CLI
# ============================================================

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Classify locally-active governed patterns by whether their "
            "observed reuse scope exceeds, matches, or falls short of the "
            "scope at which they are already governed. Descriptive "
            "scope-consistency classification, not a promotion decision."
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help=(
            "Folder containing cross_segment_governance_states.csv and "
            "pattern_reuse_distribution.csv."
        ),
    )

    parser.add_argument(
        "--output",
        default="promotion_candidate_analysis",
        help=(
            "Output folder name or path. If relative, it is created under "
            "--root. Default: promotion_candidate_analysis"
        ),
    )

    parser.add_argument(
        "--domains",
        nargs="+",
        default=["all"],
        help=(
            "Domains to analyze. Use 'all' or list one or more domain "
            "names. Default: all priority domains."
        ),
    )

    parser.add_argument(
        "--baseline-threshold",
        type=float,
        default=0.90,
        help=(
            "Project-penetration threshold (fraction of a client's projects "
            "carrying the pattern) used, together with being seeded "
            "somewhere, to classify broadly-distributed adequately-governed "
            "content directly -- independent of the reuse_bucket-derived "
            "reuse_scope comparison, since project_penetration is a "
            "continuous project-count ratio while reuse_bucket is a "
            "coarser file-count-ratio-derived category. Configurable, not "
            "Jenks-derived: no real corpus data was available to check for "
            "a natural break (see module docstring). Default: 0.90"
        ),
    )

    parser.add_argument(
        "--min-enterprise-clients",
        type=int,
        default=3,
        help=(
            "Minimum distinct clients required before a corpus_wide reuse "
            "bucket is trusted as genuine enterprise-scope evidence, "
            "rather than a small-corpus artifact (e.g. 2-of-2 clients "
            "trivially clears an 80%% share threshold). Below this count, "
            "reuse_scope is downgraded from enterprise to client. This is "
            "a policy knob, not a data-derived one -- Step 0 found no "
            "natural break to search for here. Default: 3"
        ),
    )

    parser.add_argument(
        "--enable-semantic-noise-filter",
        action="store_true",
        help=(
            "Route patterns matching known semantic-noise labels (e.g. "
            "'|self', '<Hidden Lines>') to semantic_noise_excluded.csv "
            "instead of classifying them. Default: disabled."
        ),
    )

    parser.add_argument(
        "--disable-semantic-noise-filter",
        action="store_true",
        help=(
            "Explicitly disable semantic noise suppression. This is the "
            "default, but the flag is provided for clarity in batch runs."
        ),
    )

    parser.add_argument(
        "--export-top",
        type=int,
        default=0,
        help=(
            "Optional cap on rows exported per candidate_class within "
            "promotion_candidates.csv. Use 0 to export everything. "
            "Default: 0"
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print additional diagnostics during execution.",
    )

    args = parser.parse_args(argv)

    if args.enable_semantic_noise_filter and args.disable_semantic_noise_filter:
        raise ValueError(
            "Use either --enable-semantic-noise-filter or "
            "--disable-semantic-noise-filter, not both."
        )

    semantic_noise_filter = bool(args.enable_semantic_noise_filter)

    if len(args.domains) == 1 and args.domains[0].lower() == "all":
        selected_domains = set(ALL_PRIORITY_DOMAINS)
    else:
        selected_domains = set(args.domains)

    root = Path(args.root)

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = root / output_path

    return {
        "root": root,
        "output": output_path,
        "domains": selected_domains,
        "baseline_threshold": args.baseline_threshold,
        "min_enterprise_clients": args.min_enterprise_clients,
        "enable_semantic_noise_filter": semantic_noise_filter,
        "export_top": args.export_top,
        "verbose": args.verbose,
    }


# ============================================================
# HELPERS
# ============================================================

def require_columns(df, required_columns, source_name):
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"{source_name} is missing required columns: {missing}")


def safe_bool_series(series):
    if series.dtype == bool:
        return series.fillna(False)
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False})
        .fillna(False)
    )


def apply_export_cap(df, export_top, group_col):
    if not export_top or export_top <= 0:
        return df
    return df.groupby(group_col, group_keys=False).head(export_top)


# ============================================================
# SEEDED SCOPE (from cross_segment_governance_states.csv)
# ============================================================

def compute_seeded_scope(gov: pd.DataFrame) -> pd.DataFrame:
    """Broadest scope at which a Template/Container reference segment's
    all-view mandate already includes a (domain, join_hash). One row per
    (domain, join_hash); patterns never seen as governed anywhere are
    simply absent -- callers treat an absent join as `seeded_scope =
    "ungoverned"`.
    """
    g = gov[
        gov["governance_role_reference"].isin(["Template", "Container"])
        & gov["in_reference_all"]
        & gov["comparison_type"].isin(COMPARISON_TYPE_TO_SEED_SCOPE.keys())
    ].copy()

    if g.empty:
        return pd.DataFrame(
            columns=["domain", "join_hash", "seeded_scope", "seeded_via_comparison_types"]
        )

    g["seed_scope_candidate"] = g["comparison_type"].map(COMPARISON_TYPE_TO_SEED_SCOPE)
    g["seed_scope_rank"] = g["seed_scope_candidate"].map(SCOPE_RANK)

    idx = g.groupby(["domain", "join_hash"])["seed_scope_rank"].idxmax()
    best = (
        g.loc[idx, ["domain", "join_hash", "seed_scope_candidate"]]
        .rename(columns={"seed_scope_candidate": "seeded_scope"})
        .reset_index(drop=True)
    )

    via = (
        g.groupby(["domain", "join_hash"])["comparison_type"]
        .apply(lambda s: ";".join(sorted(set(s))))
        .reset_index()
        .rename(columns={"comparison_type": "seeded_via_comparison_types"})
    )

    return best.merge(via, on=["domain", "join_hash"], how="left")


# ============================================================
# REUSE SCOPE (from pattern_reuse_distribution.csv)
# ============================================================

def compute_reuse_scope(reuse: pd.DataFrame, min_enterprise_clients: int) -> tuple:
    """Broadest reuse_scope observed for a (domain, join_hash), restricted
    to configured (`view_scope == "all"`) Project-role rows -- see module
    docstring for why Template/Container/Generic/used-view rows are
    excluded. Returns (classified, unclassified): `classified` has one row
    per (domain, join_hash) that resolved to a real scope value;
    `unclassified` carries rows whose reuse_bucket was "unclassified"
    (denominators unavailable / degraded source) for their own diagnostic
    output, never silently merged into "ungoverned".
    """
    r = reuse[
        (reuse["view_scope"] == "all") & (reuse["governance_role"] == "Project")
    ].copy()

    if r.empty:
        empty_cols = [
            "domain", "join_hash", "reuse_scope", "reuse_bucket", "client_label",
            "n_clients_present", "n_clients_denominator", "pct_clients_present",
            "n_projects_present", "n_projects_denominator", "pct_projects_present",
            "n_files_present", "n_files_denominator", "pct_files_present",
            "enterprise_evidence_downgraded", "reuse_client_pool_is_stantec_internal",
        ]
        return pd.DataFrame(columns=empty_cols), pd.DataFrame(columns=empty_cols)

    r["reuse_scope"] = r["reuse_bucket"].map(REUSE_BUCKET_TO_SCOPE)

    downgrade_mask = (r["reuse_bucket"] == "corpus_wide") & (
        r["n_clients_present"] < min_enterprise_clients
    )
    r["enterprise_evidence_downgraded"] = downgrade_mask
    r.loc[downgrade_mask, "reuse_scope"] = "client"

    r["_is_stantec_row"] = r["client_label"].astype(str).str.strip().str.lower() == "stantec"

    unclassified = r[r["reuse_scope"].isna()].copy()
    unclassified["reuse_scope"] = "unclassified"
    unclassified["reuse_client_pool_is_stantec_internal"] = unclassified["_is_stantec_row"]

    classified = r[r["reuse_scope"].notna()].copy()
    if classified.empty:
        classified["reuse_client_pool_is_stantec_internal"] = classified.get(
            "_is_stantec_row", pd.Series(dtype=bool)
        )
        keep_cols = [
            "domain", "join_hash", "reuse_scope", "reuse_bucket", "client_label",
            "n_clients_present", "n_clients_denominator", "pct_clients_present",
            "n_projects_present", "n_projects_denominator", "pct_projects_present",
            "n_files_present", "n_files_denominator", "pct_files_present",
            "enterprise_evidence_downgraded", "reuse_client_pool_is_stantec_internal",
        ]
        return classified.reindex(columns=keep_cols), unclassified.reindex(columns=keep_cols)

    # Multiple client-scoped rows routinely tie at the same reuse_scope_rank
    # -- most commonly every client-row for a join_hash hits "corpus_wide"
    # together, since pct_clients_present is a shared, not client-specific,
    # quantity. idxmax() alone would silently keep one arbitrary client's
    # n_projects_present/n_files_present (CSV-row-order dependent) and
    # discard the others. Aggregate across every row tied at the max rank
    # instead: n_projects_present/n_files_present and their denominators are
    # each computed within one client's own pool, so summing numerator and
    # denominator together across distinct clients yields a genuine
    # corpus/multi-client aggregate rather than one client's figures.
    classified["reuse_scope_rank"] = classified["reuse_scope"].map(SCOPE_RANK)
    max_rank = classified.groupby(["domain", "join_hash"])["reuse_scope_rank"].transform("max")
    tied = classified[classified["reuse_scope_rank"] == max_rank].copy()

    classified = (
        tied.groupby(["domain", "join_hash"])
        .agg(
            reuse_scope=("reuse_scope", "first"),
            reuse_bucket=("reuse_bucket", lambda s: ";".join(sorted(set(s)))),
            client_label=("client_label", lambda s: ";".join(sorted(set(s.astype(str))))),
            n_clients_present=("n_clients_present", "max"),
            n_clients_denominator=("n_clients_denominator", "max"),
            pct_clients_present=("pct_clients_present", "max"),
            n_projects_present=("n_projects_present", "sum"),
            n_projects_denominator=("n_projects_denominator", "sum"),
            n_files_present=("n_files_present", "sum"),
            n_files_denominator=("n_files_denominator", "sum"),
            enterprise_evidence_downgraded=("enterprise_evidence_downgraded", "max"),
            reuse_client_pool_is_stantec_internal=("_is_stantec_row", "max"),
        )
        .reset_index()
    )
    classified["pct_projects_present"] = (
        classified["n_projects_present"] / classified["n_projects_denominator"].replace(0, np.nan)
    ).fillna(0)
    classified["pct_files_present"] = (
        classified["n_files_present"] / classified["n_files_denominator"].replace(0, np.nan)
    ).fillna(0)

    keep_cols = [
        "domain", "join_hash", "reuse_scope", "reuse_bucket", "client_label",
        "n_clients_present", "n_clients_denominator", "pct_clients_present",
        "n_projects_present", "n_projects_denominator", "pct_projects_present",
        "n_files_present", "n_files_denominator", "pct_files_present",
        "enterprise_evidence_downgraded", "reuse_client_pool_is_stantec_internal",
    ]
    return classified.reindex(columns=keep_cols), unclassified.reindex(columns=keep_cols)


# ============================================================
# MAIN
# ============================================================

def main(argv=None):
    cfg = parse_args(argv)

    root = cfg["root"]
    out_dir = cfg["output"]
    out_dir.mkdir(parents=True, exist_ok=True)

    gov_states_path = root / "cross_segment_governance_states.csv"
    reuse_dist_path = root / "pattern_reuse_distribution.csv"

    priority_domains = cfg["domains"]
    baseline_threshold = cfg["baseline_threshold"]
    min_enterprise_clients = cfg["min_enterprise_clients"]
    enable_semantic_noise_filter = cfg["enable_semantic_noise_filter"]
    export_top = cfg["export_top"]
    verbose = cfg["verbose"]

    if verbose:
        print("")
        print("Promotion / Scope-Consistency Analysis")
        print(f"Root: {root}")
        print(f"Output: {out_dir}")
        print(f"Domains: {', '.join(sorted(priority_domains))}")
        print(f"Baseline threshold: {baseline_threshold}")
        print(f"Minimum enterprise clients: {min_enterprise_clients}")
        print(f"Semantic noise filter enabled: {enable_semantic_noise_filter}")
        print(f"Export top per class: {export_top}")
        print("")

    if not gov_states_path.exists():
        raise FileNotFoundError(f"Missing file: {gov_states_path}")
    if not reuse_dist_path.exists():
        raise FileNotFoundError(f"Missing file: {reuse_dist_path}")

    # ========================================================
    # LOAD
    # ========================================================

    print("Loading governance states...")
    gov = pd.read_csv(gov_states_path, low_memory=False)

    print("Loading reuse distribution...")
    reuse = pd.read_csv(reuse_dist_path, low_memory=False)

    require_columns(
        gov,
        [
            "domain", "join_hash", "pattern_label", "state",
            "target_usage_interpretable", "n_files_in_target_used",
            "pct_files_in_target_used", "in_any_template", "in_any_container",
            "in_any_generic", "comparison_type", "governance_role_reference",
            "in_reference_all", "segment_id_target",
        ],
        "cross_segment_governance_states.csv",
    )

    require_columns(
        reuse,
        [
            "domain", "join_hash", "pattern_label", "view_scope",
            "governance_role", "client_label", "reuse_bucket",
            "n_projects_present", "n_projects_denominator",
            "n_clients_present", "n_clients_denominator",
            "n_files_present", "n_files_denominator",
            "pct_projects_present", "pct_clients_present",
        ],
        "pattern_reuse_distribution.csv",
    )

    for col in ("in_reference_all", "in_target_all", "in_target_used",
                "in_any_template", "in_any_container", "in_any_generic",
                "target_usage_interpretable"):
        if col in gov.columns:
            gov[col] = safe_bool_series(gov[col])

    gov = gov[gov["domain"].isin(priority_domains)].copy()
    reuse = reuse[reuse["domain"].isin(priority_domains)].copy()

    for col in ("n_clients_present", "n_projects_present", "n_files_present",
                "n_clients_denominator", "n_projects_denominator", "n_files_denominator"):
        reuse[col] = pd.to_numeric(reuse[col], errors="coerce").fillna(0)

    # ========================================================
    # BASE POPULATION: locally-active, usage-interpretable rows
    # ========================================================

    active = gov[
        (gov["state"] == "local_active") & (gov["target_usage_interpretable"])
    ].copy()

    if verbose:
        print(f"Local-active rows after domain filter: {len(active):,}")

    # A single target segment shows up once per reference it was compared
    # against (Template, Enterprise, BC, ...), each carrying the same
    # n_files_in_target_used for that target (it depends only on the
    # target's own file population, not on the reference side). Collapse to
    # one row per (domain, join_hash, pattern_label, segment_id_target)
    # first, or summing n_files_in_target_used below double/triple-counts
    # the same target files once per reference comparison it appeared in.
    active_by_target = (
        active.groupby(
            ["domain", "join_hash", "pattern_label", "segment_id_target"], dropna=False
        )
        .agg(
            n_files_in_target_used=("n_files_in_target_used", "max"),
            pct_files_in_target_used=("pct_files_in_target_used", "max"),
            in_any_template=("in_any_template", "max"),
            in_any_container=("in_any_container", "max"),
            in_any_generic=("in_any_generic", "max"),
        )
        .reset_index()
    )

    base = (
        active_by_target.groupby(["domain", "join_hash", "pattern_label"], dropna=False)
        .agg(
            files_used=("n_files_in_target_used", "sum"),
            max_pct_used=("pct_files_in_target_used", "max"),
            any_template=("in_any_template", "max"),
            any_container=("in_any_container", "max"),
            any_generic=("in_any_generic", "max"),
        )
        .reset_index()
    )

    # ========================================================
    # SCOPE RESOLUTION
    # ========================================================

    seeded = compute_seeded_scope(gov)
    reuse_classified, reuse_unclassified = compute_reuse_scope(reuse, min_enterprise_clients)

    df = base.merge(seeded, on=["domain", "join_hash"], how="left")
    df["seeded_scope"] = df["seeded_scope"].fillna("ungoverned")
    df["seeded_via_comparison_types"] = df["seeded_via_comparison_types"].fillna("")

    df = df.merge(reuse_classified, on=["domain", "join_hash"], how="left")

    unclassified_join_hashes = set(
        zip(reuse_unclassified["domain"], reuse_unclassified["join_hash"])
    ) if not reuse_unclassified.empty else set()

    def _row_is_unclassified(row):
        return (row["domain"], row["join_hash"]) in unclassified_join_hashes

    df["reuse_data_unclassified"] = df.apply(_row_is_unclassified, axis=1) & df["reuse_scope"].isna()
    df["reuse_scope"] = df["reuse_scope"].fillna(
        df["reuse_data_unclassified"].map({True: "unclassified", False: "ungoverned"})
    )

    df["project_penetration"] = (
        df["n_projects_present"].fillna(0)
        / df["n_projects_denominator"].replace(0, np.nan)
    ).fillna(0)

    # Consistency check: did the coarse flattened boolean see governance
    # evidence that the finer comparison_type-based recovery missed? A
    # True here means COMPARISON_TYPE_TO_SEED_SCOPE's mapping has a gap
    # worth investigating -- surfaced for hand-verification, not acted on.
    df["seeded_scope_consistency_flag"] = (
        (df["any_template"] | df["any_container"]) & (df["seeded_scope"] == "ungoverned")
    )

    # ========================================================
    # ROUTING
    # ========================================================

    df["seeded_rank"] = df["seeded_scope"].map(SCOPE_RANK)
    df["reuse_rank"] = df["reuse_scope"].map(SCOPE_RANK)

    df["is_baseline_infrastructure"] = (
        (df["project_penetration"] >= baseline_threshold) & (df["seeded_scope"] != "ungoverned")
    )

    semantic_noise = pd.Series(False, index=df.index)
    if enable_semantic_noise_filter:
        noise_patterns = [r"\|self$", r"<Hidden Lines>"]
        semantic_noise = (
            df["pattern_label"].fillna("").str.contains(
                "|".join(noise_patterns), case=False, regex=True
            )
        )
    df["semantic_noise"] = semantic_noise

    def _route(row):
        if row["semantic_noise"]:
            return "semantic_noise_excluded"
        if row["is_baseline_infrastructure"]:
            return "baseline_adequately_governed"
        if row["reuse_scope"] == "unclassified":
            return "unclassified_reuse"
        if row["reuse_rank"] < 0:
            return "unclassified_reuse"
        if row["reuse_scope"] == "ungoverned" and row["seeded_scope"] == "ungoverned":
            return "below_reuse_floor"
        if row["reuse_rank"] < row["seeded_rank"]:
            return "governed_but_underused"
        if row["reuse_rank"] <= row["seeded_rank"]:
            return "baseline_adequately_governed"
        return "promotion_candidates"

    df["routing_bucket"] = df.apply(_route, axis=1)

    candidate_class_labels = {
        "enterprise": "consistency_footprint_matches_enterprise_scope",
        "bc": "consistency_footprint_matches_bc_scope",
        "client": "consistency_footprint_matches_client_scope",
    }
    df["candidate_class"] = df["reuse_scope"].map(candidate_class_labels)
    df["scope_gap"] = df.apply(
        lambda r: f"reuse={r['reuse_scope']} > seeded={r['seeded_scope']}"
        if r["routing_bucket"] == "promotion_candidates" else "",
        axis=1,
    )

    # ========================================================
    # RANK (ordinal, not magnitude -- no bare "score" column anywhere)
    # ========================================================

    candidates = df[df["routing_bucket"] == "promotion_candidates"].copy()
    candidates["scope_gap_width"] = candidates["reuse_rank"] - candidates["seeded_rank"]
    candidates = candidates.sort_values(
        ["domain", "scope_gap_width", "n_clients_present", "n_projects_present", "files_used", "pattern_label"],
        ascending=[True, False, False, False, False, True],
    )
    candidates["rank"] = candidates.groupby("domain").cumcount() + 1

    underused = df[df["routing_bucket"] == "governed_but_underused"].sort_values(
        ["domain", "seeded_scope", "pattern_label"]
    )
    baseline = df[df["routing_bucket"] == "baseline_adequately_governed"].sort_values(
        ["domain", "pattern_label"]
    )
    below_floor = df[df["routing_bucket"] == "below_reuse_floor"].sort_values(
        ["domain", "pattern_label"]
    )
    unclassified_out = df[df["routing_bucket"] == "unclassified_reuse"].sort_values(
        ["domain", "pattern_label"]
    )
    noise_out = df[df["routing_bucket"] == "semantic_noise_excluded"].sort_values(
        ["domain", "pattern_label"]
    )

    # ========================================================
    # DOMAIN ROLLUP
    # ========================================================

    domain_rollup = (
        df.groupby("domain")
        .agg(
            total_patterns=("join_hash", "nunique"),
            candidates=("routing_bucket", lambda x: (x == "promotion_candidates").sum()),
            governed_but_underused=("routing_bucket", lambda x: (x == "governed_but_underused").sum()),
            baseline_adequately_governed=("routing_bucket", lambda x: (x == "baseline_adequately_governed").sum()),
            below_reuse_floor=("routing_bucket", lambda x: (x == "below_reuse_floor").sum()),
            unclassified_reuse=("routing_bucket", lambda x: (x == "unclassified_reuse").sum()),
            semantic_noise_excluded=("routing_bucket", lambda x: (x == "semantic_noise_excluded").sum()),
            avg_project_penetration=("project_penetration", "mean"),
            max_project_penetration=("project_penetration", "max"),
        )
        .reset_index()
        .sort_values(["candidates", "governed_but_underused"], ascending=[False, False])
    )

    # ========================================================
    # EXPORTS
    # ========================================================

    audit_cols = [
        "domain", "join_hash", "pattern_label", "routing_bucket", "candidate_class",
        "scope_gap", "seeded_scope", "reuse_scope", "seeded_via_comparison_types",
        "reuse_bucket", "client_label", "n_clients_present", "n_clients_denominator",
        "pct_clients_present", "n_projects_present", "n_projects_denominator",
        "pct_projects_present", "n_files_present", "n_files_denominator",
        "pct_files_present", "files_used", "max_pct_used", "project_penetration",
        "is_baseline_infrastructure", "any_template", "any_container", "any_generic",
        "enterprise_evidence_downgraded", "reuse_client_pool_is_stantec_internal",
        "seeded_scope_consistency_flag", "reuse_data_unclassified", "semantic_noise",
    ]
    audit_cols = [c for c in audit_cols if c in df.columns]

    candidate_export_cols = ["rank"] + audit_cols
    apply_export_cap(candidates, export_top, "candidate_class")[candidate_export_cols].to_csv(
        out_dir / "promotion_candidates.csv", index=False
    )
    underused[audit_cols].to_csv(out_dir / "governed_but_underused.csv", index=False)
    baseline[audit_cols].to_csv(out_dir / "baseline_adequately_governed.csv", index=False)
    below_floor[audit_cols].to_csv(out_dir / "below_reuse_floor.csv", index=False)
    unclassified_out[audit_cols].to_csv(out_dir / "unclassified_reuse.csv", index=False)
    if enable_semantic_noise_filter:
        noise_out[audit_cols].to_csv(out_dir / "semantic_noise_excluded.csv", index=False)
    domain_rollup.to_csv(out_dir / "domain_rollup.csv", index=False)
    df[audit_cols].sort_values(["domain", "routing_bucket", "pattern_label"]).to_csv(
        out_dir / "promotion_candidate_full_audit.csv", index=False
    )

    # ========================================================
    # SUMMARY MARKDOWN
    # ========================================================

    summary = []
    summary.append("# Scope-Consistency Analysis Summary")
    summary.append("")
    summary.append(
        "**Read this first:** every classification below describes where a "
        "pattern's observed reuse footprint sits relative to where it is "
        "already governed. None of it is a promotion decision, an approval, "
        "or a recommendation to act -- `candidate_class` names a consistency "
        "footprint, not a verdict. Treat this as a lead list for governance "
        "review, not a queue to execute against."
    )
    summary.append("")

    summary.append("## Run Configuration")
    summary.append("")
    summary.append(f"- Root: `{root}`")
    summary.append(f"- Output: `{out_dir}`")
    summary.append(f"- Domains: `{', '.join(sorted(priority_domains))}`")
    summary.append(f"- Baseline threshold: `{baseline_threshold}`")
    summary.append(f"- Minimum enterprise clients: `{min_enterprise_clients}`")
    summary.append(f"- Semantic noise filter enabled: `{enable_semantic_noise_filter}`")
    summary.append(f"- Export top per class: `{export_top}`")
    summary.append("")

    summary.append("## What the numbers show")
    summary.append("")
    n_candidates = len(candidates)
    n_underused = len(underused)
    n_baseline = len(baseline)
    if n_candidates:
        top_domains = (
            candidates.groupby("domain").size().sort_values(ascending=False).head(5)
        )
        summary.append(
            f"**{n_candidates} patterns are used more broadly than they are governed.** "
            f"The largest concentrations are in "
            f"{', '.join(f'{d} ({int(n)})' for d, n in top_domains.items())}. "
            "Each row in `promotion_candidates.csv` names the exact scope gap "
            "(`scope_gap`, e.g. `reuse=enterprise > seeded=client`) rather than "
            "a single opaque label."
        )
    else:
        summary.append("**No patterns showed reuse exceeding their governed scope under current thresholds.**")
    summary.append("")

    if n_underused:
        summary.append(
            f"**{n_underused} patterns are governed more broadly than they are actually reused** "
            "(`governed_but_underused.csv`). This is an adoption/underuse question, not a "
            "promotion question, and is out of scope for this tool's candidate logic -- "
            "treat it as a lead for the archetype/adoption work. Note: `reuse_scope` can "
            "never resolve to `bc` (see module docstring), so some rows here may reflect "
            "that measurement gap rather than genuinely low bc-level reuse."
        )
        summary.append("")

    summary.append(
        f"**{n_baseline} patterns are adequately governed** -- reuse scope does not exceed "
        "the broadest scope at which they are already seeded, or they cleared the "
        "project-penetration + already-seeded baseline gate directly."
    )
    summary.append("")

    consistency_flags = int(df["seeded_scope_consistency_flag"].sum())
    if consistency_flags:
        summary.append(
            f"**{consistency_flags} rows have `seeded_scope_consistency_flag=True`** -- "
            "`in_any_template`/`in_any_container` saw governance evidence that the "
            "`comparison_type`-based scope recovery in this tool did not map to a scope "
            "level. Worth a manual spot-check before trusting `seeded_scope=ungoverned` "
            "on those specific rows."
        )
        summary.append("")

    summary.append("## Domain Rollup")
    summary.append("")
    for _, r in domain_rollup.iterrows():
        summary.append(
            f"- **{r['domain']}**: {int(r['candidates'])} candidates, "
            f"{int(r['governed_but_underused'])} governed-but-underused, "
            f"{int(r['baseline_adequately_governed'])} baseline, "
            f"{int(r['total_patterns'])} total patterns analyzed."
        )
    summary.append("")

    summary.append("## Top Candidates by Domain")
    summary.append("")
    if n_candidates == 0:
        summary.append("- None found under current thresholds.")
    else:
        for dom in sorted(candidates["domain"].unique()):
            summary.append(f"### {dom}")
            summary.append("")
            for _, r in candidates[candidates["domain"] == dom].head(10).iterrows():
                summary.append(
                    f"{int(r['rank'])}. {r['pattern_label']} -- {r['scope_gap']} "
                    f"(clients={int(r['n_clients_present'])}, "
                    f"projects={int(r['n_projects_present'])}, "
                    f"files_used={int(r['files_used'])})"
                )
            summary.append("")

    summary.append("## Notes")
    summary.append("")
    summary.append(
        "- `rank` is ordinal position within a domain's candidate list, not a magnitude "
        "score. No composite numeric score is exposed anywhere in this tool's output."
    )
    summary.append(
        "- `baseline_adequately_governed` generalizes the prior `is_baseline_infrastructure` "
        "/ `object_style_baseline` special cases into one rule: adequately governed means "
        "reuse scope does not exceed seeded scope, or high project penetration plus being "
        "seeded somewhere. A universal-but-unseeded pattern is no longer excluded as "
        "baseline -- it becomes a candidate, which is the intended behavior change."
    )
    summary.append(
        "- `governed_but_underused` and `unclassified_reuse` are fully separate outputs, "
        "never merged into `promotion_candidates.csv` or `baseline_adequately_governed.csv`."
    )

    (out_dir / "promotion_candidate_summary.md").write_text(
        "\n".join(summary), encoding="utf-8"
    )

    # ========================================================
    # CONSOLE SUMMARY
    # ========================================================

    print("")
    print("Analysis complete")
    print(f"Output folder: {out_dir}")
    print("")
    print(f"Candidates:              {n_candidates:,}")
    print(f"Governed but underused:  {n_underused:,}")
    print(f"Baseline (adequate):     {n_baseline:,}")
    print(f"Below reuse floor:       {len(below_floor):,}")
    print(f"Unclassified reuse data: {len(unclassified_out):,}")
    if enable_semantic_noise_filter:
        print(f"Semantic noise excluded: {len(noise_out):,}")
    print("")
    print("Exports:")
    print(f"- {out_dir / 'promotion_candidates.csv'}")
    print(f"- {out_dir / 'governed_but_underused.csv'}")
    print(f"- {out_dir / 'baseline_adequately_governed.csv'}")
    print(f"- {out_dir / 'below_reuse_floor.csv'}")
    print(f"- {out_dir / 'unclassified_reuse.csv'}")
    if enable_semantic_noise_filter:
        print(f"- {out_dir / 'semantic_noise_excluded.csv'}")
    print(f"- {out_dir / 'domain_rollup.csv'}")
    print(f"- {out_dir / 'promotion_candidate_full_audit.csv'}")
    print(f"- {out_dir / 'promotion_candidate_summary.md'}")


if __name__ == "__main__":
    main()
