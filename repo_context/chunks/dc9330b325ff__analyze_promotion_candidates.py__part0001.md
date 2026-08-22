# Chunk of tools/analyze_promotion_candidates.py

- Source relative path: `tools/analyze_promotion_candidates.py`
- Chunk: 1 of 3
- Original line range: 1-398
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: parse_args, require_columns, safe_bool_series, apply_export_cap, compute_seeded_scope
- Source SHA-256: 1e5dbc478f5d1e9f1e948261cb2b0121cc79fd2657bbc116c27e7eeea845924b
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """Scope-consistency analysis for governed patterns.
     2| 
     3| Redesign of an earlier `analyze_promotion_candidates.py` prototype (not
     4| previously checked into this repo). Answers a narrower, more defensible
     5| question than the prototype did: for a locally-active pattern, does its
     6| observed reuse breadth (`reuse_scope`, from `pattern_reuse_distribution.csv`)
     7| exceed the broadest scope at which it is already governed by a Template or
     8| Container (`seeded_scope`, from `cross_segment_governance_states.csv`)?
     9| 
    10| This is a **scope-consistency classification**, not a promotion decision.
    11| `candidate_class` values describe where a pattern's reuse footprint sits
    12| relative to governance, not an approval. See the "Read this first" section
    13| of the generated `promotion_candidate_summary.md` for the same disclaimer
    14| in the output artifact itself.
    15| 
    16| Standalone tool. Not wired into `run_extract_all.py` or
    17| `generate_governance_narrative.py`; does not call `assign_tier()`; does not
    18| write into the governance evidence package. `--root` points at a folder
    19| containing `cross_segment_governance_states.csv` and
    20| `pattern_reuse_distribution.csv` (both written by
    21| `tools/compare_cross_segment.py` / its narrative-layer callers).
    22| 
    23| Scope taxonomy
    24| ---------------
    25| Reuses `compare_cross_segment.py`'s own `_scope_level()` taxonomy rather than
    26| inventing a parallel one, with one adaptation forced by what the two input
    27| CSVs actually carry (confirmed by reading both files' row-construction code,
    28| not just their docs):
    29| 
    30| - `_scope_level()` itself returns exactly three non-null values --
    31|   `enterprise`, `business_center`, `client_business_center` -- plus `None`
    32|   for rows where client_label/business_center_label aren't both cut (a
    33|   roll-up). There is no fourth "project" level in the function itself, even
    34|   though `docs/cross_segment_comparison.md` section 2 describes one; the
    35|   code is authoritative. This tool renames the three to `enterprise` / `bc`
    36|   / `client` and adds `ungoverned` as the floor value for "not seeded by any
    37|   Template/Container at all" / "reuse hasn't reached client-wide breadth
    38|   yet" -- the same four-value shape the task brief asked for, reached by
    39|   reading the real function rather than assuming its return values.
    40| 
    41| - `seeded_scope` is derived from `cross_segment_governance_states.csv`,
    42|   which carries `comparison_type` + `governance_role_reference` +
    43|   `business_center_label_reference` per row but no `client_label` column at
    44|   all. `comparison_type` already names the scope-level edge by
    45|   construction (`enterprise_to_project`/`enterprise_to_bc`/
    46|   `enterprise_to_client` -> enterprise; `bc_to_project` -> bc;
    47|   `template_to_project`/`template_to_container`/`container_to_project` are
    48|   the client-scoped governance-chain edges per
    49|   `docs/cross_segment_comparison.md` section 2 -> client). `generic_to_*`
    50|   rows are excluded -- Generic is raw stock, not a governance standard,
    51|   matching the prototype's own `already_seeded = any_template | any_container`
    52|   (no `any_generic`).
    53| 
    54| - `reuse_scope` is derived from `pattern_reuse_distribution.csv`, filtered
    55|   to `governance_role == "Project"` rows only, preferring `view_scope ==
    56|   "used"` (active-delivery-practice) over `view_scope == "all"`
    57|   (configured) per (client_label, discipline_label) population -- all-view
    58|   is used only as a fallback for a population with no used-view row at all.
    59|   The prototype this replaces did *not* apply either filter, which silently
    60|   blended configured-vocabulary breadth from Template/Container/Generic
    61|   rows and passively-inherited all-view breadth into what was reported as
    62|   reuse. That is a real defect, not a style choice: `docs/cross_segment_
    63|   comparison.md` explicitly warns that Template/Generic/most-Container
    64|   all-view rows are "configured/published inventory, not active usage
    65|   claims" -- and unconditional all-view would also be inconsistent with
    66|   this tool's own base population, already restricted to `state ==
    67|   "local_active"` rows on the governance side. See `compute_reuse_scope()`'s
    68|   docstring for the fallback mechanics and the `reuse_view_source` audit
    69|   column.
    70| 
    71|   Known upstream gap, confirmed by reading `build_pattern_reuse_distribution_rows()`
    72|   in `compare_cross_segment.py`: the row grouping key is
    73|   `(view_scope, governance_role, client_label, discipline_label, unit_system, domain)`
    74|   -- there is no `business_center_label` in that key. `reuse_scope` can
    75|   therefore **never** resolve to `bc`; multiple real business centers
    76|   sharing the same `client_label` (including every enterprise-internal BC,
    77|   whose labels match the effective EnterprisePolicy) collapse into one pool.
    78|   A reuse row whose `client_label` matches EnterprisePolicy is flagged via
    79|   `reuse_client_pool_is_enterprise` so a reader can see when "client"
    80|   scope actually means "all of the enterprise's business centers pooled together,"
    81|   not one real external client. This means a genuine `seeded_scope == "bc"`
    82|   case can show up as `reuse_scope < seeded_scope` (routed to
    83|   governed_but_underused.csv) purely because reuse breadth has no bc grain
    84|   to resolve into -- not because real-world reuse is actually narrower than
    85|   governance. Treat `bc`-scope rows in governed_but_underused.csv with that
    86|   caveat in mind; this tool does not attempt to paper over the gap with an
    87|   invented threshold.
    88| 
    89| No corpus data was available in the environment this tool was written in
    90| (no `results/`/`segments/` export directories exist in this repo -- they
    91| are runtime outputs generated from real Revit projects, never checked into
    92| git). Per the project-owner's explicit direction, `--baseline-threshold`
    93| and `--min-enterprise-clients` therefore stay configurable CLI knobs with
    94| the prototype's original defaults rather than Jenks-derived cuts; no
    95| distribution shape was available to check for a natural break, and forcing
    96| one without data would be exactly the kind of false-precision this redesign
    97| is supposed to remove. Run `--verbose` against real data and inspect
    98| `domain_rollup.csv`'s `avg_project_penetration`/`max_project_penetration`
    99| columns before deciding whether a derived cut is warranted later.
   100| """
   101| 
   102| from __future__ import annotations
   103| 
   104| import argparse
   105| from pathlib import Path
   106| 
   107| import numpy as np
   108| import pandas as pd
   109| 
   110| from enterprise_policy import EnterprisePolicy, load_enterprise_policy, write_enterprise_policy_provenance
   111| 
   112| 
   113| # ============================================================
   114| # CONSTANTS
   115| # ============================================================
   116| 
   117| ALL_PRIORITY_DOMAINS = [
   118|     "text_types",
   119|     "dimension_types_linear",
   120|     "dimension_types_angular",
   121|     "dimension_types_diameter",
   122|     "dimension_types_radial",
   123|     "dimension_types_spot_coordinate",
   124|     "dimension_types_spot_elevation",
   125|     "dimension_types_spot_slope",
   126|     "fill_patterns_drafting",
   127|     "fill_patterns_model",
   128|     "floor_types",
   129|     "ceiling_types",
   130|     "object_styles_model",
   131|     "view_category_overrides_model",
   132|     "loaded_family_types",
   133| ]
   134| 
   135| # Ordinal only -- widest reach ranks highest. Never treat as a magnitude;
   136| # it exists purely to compare two scope labels with `<`/`>`/`==`.
   137| SCOPE_RANK = {"ungoverned": 0, "client": 1, "bc": 2, "enterprise": 3}
   138| 
   139| # comparison_type -> scope level of the reference (Template/Container) side.
   140| # See module docstring for why each mapping is what it is.
   141| COMPARISON_TYPE_TO_SEED_SCOPE = {
   142|     "enterprise_to_bc": "enterprise",
   143|     "enterprise_to_client": "enterprise",
   144|     "enterprise_to_project": "enterprise",
   145|     "bc_to_project": "bc",
   146|     "template_to_project": "client",
   147|     "template_to_container": "client",
   148|     "container_to_project": "client",
   149|     # generic_to_template / generic_to_container / generic_to_project are
   150|     # deliberately absent: Generic is raw stock, not a governance standard.
   151| }
   152| 
   153| # reuse_bucket -> reuse_scope. single_project/emerging/single_file all sit
   154| # below client-wide breadth, so none of them constitute a governance-scope
   155| # claim on their own -- they are adoption/early-signal buckets, not scope.
   156| REUSE_BUCKET_TO_SCOPE = {
   157|     "corpus_wide": "enterprise",
   158|     "client_wide": "client",
   159|     "multi_project": "client",
   160|     "single_project": "ungoverned",
   161|     "emerging": "ungoverned",
   162|     "single_file": "ungoverned",
   163|     # "unclassified" is intentionally absent -- handled as its own routed
   164|     # diagnostic bucket, never silently folded into "ungoverned".
   165| }
   166| 
   167| 
   168| # ============================================================
   169| # CLI
   170| # ============================================================
   171| 
   172| def parse_args(argv=None):
   173|     parser = argparse.ArgumentParser(
   174|         description=(
   175|             "Classify locally-active governed patterns by whether their "
   176|             "observed reuse scope exceeds, matches, or falls short of the "
   177|             "scope at which they are already governed. Descriptive "
   178|             "scope-consistency classification, not a promotion decision."
   179|         )
   180|     )
   181| 
   182|     parser.add_argument(
   183|         "--root",
   184|         required=True,
   185|         help=(
   186|             "Folder containing cross_segment_governance_states.csv and "
   187|             "pattern_reuse_distribution.csv."
   188|         ),
   189|     )
   190| 
   191|     parser.add_argument(
   192|         "--output",
   193|         default="promotion_candidate_analysis",
   194|         help=(
   195|             "Output folder name or path. If relative, it is created under "
   196|             "--root. Default: promotion_candidate_analysis"
   197|         ),
   198|     )
   199| 
   200|     parser.add_argument(
   201|         "--domains",
   202|         nargs="+",
   203|         default=["all"],
   204|         help=(
   205|             "Domains to analyze. Use 'all' or list one or more domain "
   206|             "names. Default: all priority domains."
   207|         ),
   208|     )
   209| 
   210|     parser.add_argument(
   211|         "--baseline-threshold",
   212|         type=float,
   213|         default=0.90,
   214|         help=(
   215|             "Project-penetration threshold (fraction of a client's projects "
   216|             "carrying the pattern) used, together with being seeded "
   217|             "somewhere, to classify broadly-distributed adequately-governed "
   218|             "content directly -- independent of the reuse_bucket-derived "
   219|             "reuse_scope comparison, since project_penetration is a "
   220|             "continuous project-count ratio while reuse_bucket is a "
   221|             "coarser file-count-ratio-derived category. Configurable, not "
   222|             "Jenks-derived: no real corpus data was available to check for "
   223|             "a natural break (see module docstring). Default: 0.90"
   224|         ),
   225|     )
   226| 
   227|     parser.add_argument(
   228|         "--min-enterprise-clients",
   229|         type=int,
   230|         default=3,
   231|         help=(
   232|             "Minimum distinct clients required before a corpus_wide reuse "
   233|             "bucket is trusted as genuine enterprise-scope evidence, "
   234|             "rather than a small-corpus artifact (e.g. 2-of-2 clients "
   235|             "trivially clears an 80%% share threshold). Below this count, "
   236|             "reuse_scope is downgraded from enterprise to client. This is "
   237|             "a policy knob, not a data-derived one -- Step 0 found no "
   238|             "natural break to search for here. Default: 3"
   239|         ),
   240|     )
   241| 
   242|     parser.add_argument(
   243|         "--enable-semantic-noise-filter",
   244|         action="store_true",
   245|         help=(
   246|             "Route patterns matching known semantic-noise labels (e.g. "
   247|             "'|self', '<Hidden Lines>') to semantic_noise_excluded.csv "
   248|             "instead of classifying them. Default: disabled."
   249|         ),
   250|     )
   251| 
   252|     parser.add_argument(
   253|         "--disable-semantic-noise-filter",
   254|         action="store_true",
   255|         help=(
   256|             "Explicitly disable semantic noise suppression. This is the "
   257|             "default, but the flag is provided for clarity in batch runs."
   258|         ),
   259|     )
   260| 
   261|     parser.add_argument(
   262|         "--export-top",
   263|         type=int,
   264|         default=0,
   265|         help=(
   266|             "Optional cap on rows exported per candidate_class within "
   267|             "promotion_candidates.csv. Use 0 to export everything. "
   268|             "Default: 0"
   269|         ),
   270|     )
   271| 
   272|     parser.add_argument(
   273|         "--verbose",
   274|         action="store_true",
   275|         help="Print additional diagnostics during execution.",
   276|     )
   277|     parser.add_argument("--enterprise-policy", help="Deployment-local enterprise policy JSON")
   278|     parser.add_argument("--enterprise-label", help="Effective enterprise label override")
   279| 
   280|     args = parser.parse_args(argv)
   281| 
   282|     if args.enable_semantic_noise_filter and args.disable_semantic_noise_filter:
   283|         raise ValueError(
   284|             "Use either --enable-semantic-noise-filter or "
   285|             "--disable-semantic-noise-filter, not both."
   286|         )
   287| 
   288|     semantic_noise_filter = bool(args.enable_semantic_noise_filter)
   289| 
   290|     if len(args.domains) == 1 and args.domains[0].lower() == "all":
   291|         selected_domains = set(ALL_PRIORITY_DOMAINS)
   292|     else:
   293|         selected_domains = set(args.domains)
   294| 
   295|     root = Path(args.root)
   296| 
   297|     output_path = Path(args.output)
   298|     if not output_path.is_absolute():
   299|         output_path = root / output_path
   300| 
   301|     return {
   302|         "root": root,
   303|         "output": output_path,
   304|         "domains": selected_domains,
   305|         "baseline_threshold": args.baseline_threshold,
   306|         "min_enterprise_clients": args.min_enterprise_clients,
   307|         "enable_semantic_noise_filter": semantic_noise_filter,
   308|         "export_top": args.export_top,
   309|         "verbose": args.verbose,
   310|         "enterprise_policy": load_enterprise_policy(args.enterprise_policy, args.enterprise_label),
   311|     }
   312| 
   313| 
   314| # ============================================================
   315| # HELPERS
   316| # ============================================================
   317| 
   318| def require_columns(df, required_columns, source_name):
   319|     missing = sorted(set(required_columns) - set(df.columns))
   320|     if missing:
   321|         raise ValueError(f"{source_name} is missing required columns: {missing}")
   322| 
   323| 
   324| def safe_bool_series(series):
   325|     if series.dtype == bool:
   326|         return series.fillna(False)
   327|     return (
   328|         series.astype(str)
   329|         .str.strip()
   330|         .str.lower()
   331|         .map({"true": True, "false": False})
   332|         .fillna(False)
   333|     )
   334| 
   335| 
   336| def apply_export_cap(df, export_top, group_col):
   337|     if not export_top or export_top <= 0:
   338|         return df
   339|     return df.groupby(group_col, group_keys=False).head(export_top)
   340| 
   341| 
   342| # ============================================================
   343| # SEEDED SCOPE (from cross_segment_governance_states.csv)
   344| # ============================================================
   345| 
   346| def compute_seeded_scope(gov: pd.DataFrame) -> pd.DataFrame:
   347|     """Broadest scope at which a Template/Container reference segment's
   348|     all-view mandate already includes a (domain, join_hash, unit_system).
   349|     One row per key; patterns never seen as governed anywhere are simply
   350|     absent -- callers treat an absent join as `seeded_scope = "ungoverned"`.
   351| 
   352|     Keyed on `unit_system` as well as `domain`/`join_hash`: join_hash values
   353|     are computed from behavioral hashes that include unit-bearing values for
   354|     most domains, so an imperial and a metric pattern would not normally
   355|     collide on the same join_hash -- but not every domain's identity
   356|     necessarily includes a unit-bearing property, and `compare_cross_segment
   357|     .py` itself enforces matching unit_system on every pair it discovers
   358|     ("Imperial and metric segments are never compared"). Carrying
   359|     unit_system through here keeps that same partition instead of relying
   360|     on hash collision being impossible for every domain.
   361|     """
   362|     g = gov[
   363|         gov["governance_role_reference"].isin(["Template", "Container"])
   364|         & gov["in_reference_all"]
   365|         & gov["comparison_type"].isin(COMPARISON_TYPE_TO_SEED_SCOPE.keys())
   366|     ].copy()
   367| 
   368|     if g.empty:
   369|         return pd.DataFrame(
   370|             columns=["domain", "join_hash", "unit_system", "seeded_scope",
   371|                      "seeded_via_comparison_types"]
   372|         )
   373| 
   374|     g["seed_scope_candidate"] = g["comparison_type"].map(COMPARISON_TYPE_TO_SEED_SCOPE)
   375|     g["seed_scope_rank"] = g["seed_scope_candidate"].map(SCOPE_RANK)
   376| 
   377|     key_cols = ["domain", "join_hash", "unit_system"]
   378|     idx = g.groupby(key_cols)["seed_scope_rank"].idxmax()
   379|     best = (
   380|         g.loc[idx, key_cols + ["seed_scope_candidate"]]
   381|         .rename(columns={"seed_scope_candidate": "seeded_scope"})
   382|         .reset_index(drop=True)
   383|     )
   384| 
   385|     via = (
   386|         g.groupby(key_cols)["comparison_type"]
   387|         .apply(lambda s: ";".join(sorted(set(s))))
   388|         .reset_index()
   389|         .rename(columns={"comparison_type": "seeded_via_comparison_types"})
   390|     )
   391| 
   392|     return best.merge(via, on=key_cols, how="left")
   393| 
   394| 
   395| # ============================================================
   396| # REUSE SCOPE (from pattern_reuse_distribution.csv)
   397| # ============================================================
   398| 
```
