# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 1 of 17
- Original line range: 1-494
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: pf, pct, fmt, _warn_unrecognized_comparison_types, read_csv, _disc_label, detect_bundle_schema, normalise_summary_schema, normalise_summary_schema.alias, _col, _resolved_col_name, _col_union_or_pairwise, used_view_falls_back_to_legacy, _is_unscoped_segment, _target_scope_label
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """
     2| generate_governance_narrative.py
     3| 
     4| Deterministic governance narrative renderer for the Revit Fingerprint System.
     5| Produces governance_narrative_context.md from pipeline CSV outputs.
     6| No LLM in the loop — all text is assembled from templates filled by computed values.
     7| 
     8| Required inputs (all produced by compare_cross_segment.py / bundle pipeline):
     9|   --summary      cross_segment_summary.csv
    10|   --pooled       cross_segment_pooled.csv
    11| 
    12| Optional inputs (enrich state, delta, and pattern sections when available):
    13|   --governance-state-summary cross_segment_governance_state_summary.csv
    14|   --governance-states        cross_segment_governance_states.csv
    15|   --delta                    cross_segment_delta.csv
    16|   --run-registry             run_registry.csv          (for corpus metadata)
    17|   --file-meta                file_metadata.csv         (for file counts by role/client/discipline)
    18|   --client-sector            client_sector.csv         (client_label,sector -- classifies
    19|                                                           cross-client convergence and
    20|                                                           non-comparable-sector tiering;
    21|                                                           absent = every client unclassified)
    22|   --union-inventory         cross_segment_union_inventory.csv
    23|   --reuse-distribution      pattern_reuse_distribution.csv
    24|   --matrix-manifest         matrix_output_manifest.csv
    25|   --reuse-by-client         pattern_reuse_summary_by_client.csv (adoption-breadth
    26|                               signal -- how many clients use a pattern -- added
    27|                               alongside the existing distinct-pattern reuse table
    28|                               in the Union Inventory Reuse Summary section)
    29|   --project-union-jaccard-matrix        project_union_jaccard_matrix.csv
    30|   --project-density-similarity-matrix   project_density_similarity_matrix.csv
    31|   --project-pool-containment-matrix     project_pool_containment_similarity_matrix.csv
    32|   --project-fragmentation-diagnostic    project_fragmentation_diagnostic.csv
    33|                               (these four feed the Project Portfolio section --
    34|                               project x project grain, intentionally outside
    35|                               assign_tier()/governance_domain_summary.csv; see
    36|                               docs/governance_generator_cross_compare_coverage.md)
    37|   --governance-bc-client-matrix   governance_bc_client_matrix.csv (from
    38|                               tools/governance_relationships.py) -- feeds the
    39|                               Business Center Composition section
    40|   --governance-client-bc-matrix   governance_client_bc_matrix.csv (from
    41|                               tools/governance_relationships.py) -- feeds the
    42|                               Business Center Distribution section
    43| 
    44| Not yet consumed directly; see docs/governance_generator_cross_compare_coverage.md
    45| for recommended integration points:
    46|   comparison_registry.csv, cross_segment_file_pairs.csv,
    47|   pattern_reuse_summary_by_domain.csv (excluded on purpose -- its n_patterns
    48|   duplicates the corpus-wide reuse signal the distinct-pattern table already
    49|   reports), and project_mean_file_pair_jaccard_matrix.csv (its signal is folded
    50|   into project_fragmentation_diagnostic.csv's exact_identity_overlap column
    51|   rather than consumed standalone)
    52| 
    53|   This is the exhaustive list of files this generator writes no code path to
    54|   read -- confirmed against this docstring, not assumed. All four are still
    55|   registered as governance_evidence_map.json artifacts (D-024): each entry's
    56|   columns/row_count are populated by a live scan (the same one
    57|   governance_file_inventory.json uses) when the file is present beside
    58|   --summary, so a reader can see the real header/row-count without opening
    59|   the (potentially multi-GB) file itself. See docs/governance_evidence_package.md.
    60| 
    61| Output:
    62|   --out          governance_narrative_context.md  (default)
    63| 
    64| Usage:
    65|   python generate_governance_narrative.py \\
    66|       --summary cross_segment_summary.csv \\
    67|       --pooled  cross_segment_pooled.csv \\
    68|       [--delta  cross_segment_delta.csv] \\
    69|       [--file-meta file_metadata.csv] \\
    70|       [--client-sector policies/client_sector.csv] \\
    71|       --out governance_narrative_context.md
    72| """
    73| 
    74| from __future__ import annotations
    75| 
    76| import argparse
    77| import csv
    78| import shutil
    79| import statistics
    80| import sys
    81| from collections import defaultdict
    82| from datetime import date
    83| from pathlib import Path
    84| from typing import Optional
    85| 
    86| # compare_cross_segment.py lives in this same directory and is side-effect-free on
    87| # import (its pipeline logic is gated behind `if __name__ == "__main__":`), so its
    88| # GOVERNANCE_STATE_DIRECTED_TYPES is imported directly rather than hand-copied --
    89| # see _DIRECTED_GOVERNANCE_TYPES below for why a hand-copy drifted before.
    90| from compare_cross_segment import GOVERNANCE_STATE_DIRECTED_TYPES, _resolve_runnable_segment
    91| 
    92| # governance_evidence_package.py is a sibling module (same side-effect-free-on-
    93| # import convention) providing the package manifest/health/evidence-map layer
    94| # added around this generator's existing deterministic outputs. See
    95| # docs/governance_evidence_package.md.
    96| from governance_evidence_package import (
    97|     GENERATOR_IDENTITY,
    98|     GENERATOR_ROLE,
    99|     PACKAGE_SCHEMA_VERSION,
   100|     EVIDENCE_MAP_SCHEMA_VERSION,
   101|     FINDINGS_SCHEMA_VERSION,
   102|     FILE_INVENTORY_SCHEMA_VERSION,
   103|     AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   104|     AUTHORITY_CONTROLLED_INTERPRETATION,
   105|     AUTHORITY_CONVENIENCE_SUMMARY,
   106|     FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
   107|     FINDING_FIDELITY_EXACT,
   108|     FINDING_STATUS_SUPPORTED,
   109|     FINDING_STATUS_QUESTION_NOT_CLAIM,
   110|     build_evidence_map,
   111|     build_file_inventory_document,
   112|     build_findings_document,
   113|     build_package_health,
   114|     build_package_manifest,
   115|     comparison_type_coverage as _comparison_type_coverage,
   116|     inventory_export_directory_files,
   117|     write_json,
   118| )
   119| 
   120| # governance_policy.py is a sibling module providing the generic JSON
   121| # policy-profile loader (mechanical load/fallback only) for the externalized
   122| # governance thresholds / domain-governance policy / client-onboarding policy /
   123| # finding-rule documentation profiles in policies/governance/*.json. The
   124| # default profile VALUES and domain-governance business logic stay in this
   125| # file -- see apply_governance_policy() below and docs/governance_evidence_package.md.
   126| from enterprise_policy import load_enterprise_policy, write_enterprise_policy_provenance
   127| from governance_policy import (
   128|     DEFAULT_POLICY_DIR as _DEFAULT_POLICY_DIR,
   129|     load_governance_policy,
   130| )
   131| 
   132| 
   133| # ── helpers ────────────────────────────────────────────────────────────────────
   134| 
   135| def pf(v) -> Optional[float]:
   136|     try:
   137|         return float(v)
   138|     except (TypeError, ValueError):
   139|         return None
   140| 
   141| 
   142| def pct(v: Optional[float], decimals: int = 0) -> str:
   143|     if v is None:
   144|         return "—"
   145|     return f"{v * 100:.{decimals}f}%"
   146| 
   147| 
   148| def fmt(v: Optional[float], decimals: int = 3) -> str:
   149|     if v is None:
   150|         return "—"
   151|     return f"{v:.{decimals}f}"
   152| 
   153| 
   154| def _warn_unrecognized_comparison_types(seen: set, known: set, context: str) -> None:
   155|     """Warn once, to stderr, for any comparison_type not accounted for by name.
   156| 
   157|     Shared by build_cascade() and build_governance_state_summary() so an
   158|     unrecognized/drifted comparison_type is never silently swallowed in either
   159|     place -- see docs/governance_narrative_scope_gap_audit.md A1/A3.
   160|     """
   161|     unrecognized = seen - known
   162|     if unrecognized:
   163|         print(
   164|             f"[warn] {context}: unrecognized comparison_type value(s) not in any "
   165|             f"known group, excluded: {sorted(unrecognized)}",
   166|             file=sys.stderr,
   167|         )
   168| 
   169| 
   170| def read_csv(path: Path) -> list[dict]:
   171|     with open(path, encoding="utf-8-sig", newline="") as f:
   172|         return list(csv.DictReader(f))
   173| 
   174| 
   175| DOMAIN_LABELS = {
   176|     "arrowheads": "Arrowheads",
   177|     "ceiling_types": "Ceiling Types",
   178|     "dimension_types_angular": "Dimension Types — Angular",
   179|     "dimension_types_diameter": "Dimension Types — Diameter",
   180|     "dimension_types_linear": "Dimension Types — Linear",
   181|     "dimension_types_radial": "Dimension Types — Radial",
   182|     "dimension_types_spot_coordinate": "Dimension Types — Spot Coordinate",
   183|     "dimension_types_spot_elevation": "Dimension Types — Spot Elevation",
   184|     "dimension_types_spot_slope": "Dimension Types — Spot Slope",
   185|     "fill_patterns_drafting": "Fill Patterns — Drafting",
   186|     "fill_patterns_model": "Fill Patterns — Model",
   187|     "floor_types": "Floor Types",
   188|     "line_patterns": "Line Patterns",
   189|     "line_styles": "Line Styles",
   190|     "loaded_family_types": "Loaded Family Types",
   191|     "materials": "Materials",
   192|     "object_styles_analytical": "Object Styles — Analytical",
   193|     "object_styles_annotation": "Object Styles — Annotation",
   194|     "object_styles_model": "Object Styles — Model",
   195|     "phase_filters": "Phase Filters",
   196|     "phases": "Phases",
   197|     "roof_types": "Roof Types",
   198|     "text_types": "Text Types",
   199|     "units": "Units",
   200|     "view_category_overrides_annotation": "View Category Overrides — Annotation",
   201|     "view_category_overrides_model": "View Category Overrides — Model",
   202|     "view_filter_applications_view_templates": "View Filter Applications",
   203|     "view_filter_definitions": "View Filter Definitions",
   204|     "view_templates_ceiling_plans": "View Templates — Ceiling Plans",
   205|     "view_templates_elevations_sections_detail": "View Templates — Elevations/Sections",
   206|     "view_templates_floor_structural_area_plans": "View Templates — Floor/Structural Plans",
   207|     "view_templates_renderings_drafting": "View Templates — Renderings/Drafting",
   208|     "view_templates_schedules": "View Templates — Schedules",
   209| }
   210| 
   211| # Domains excluded from aggregate governance scoring (structurally anomalous).
   212| # Default value, used as the fallback when domain_governance_policy.json is
   213| # absent from --policy-dir; apply_governance_policy() below reassigns
   214| # EXCLUDED_FROM_SCORING from the resolved policy at runtime. Kept as a plain
   215| # module global (not threaded through function signatures) so every existing
   216| # reference in this file -- and every existing test importing
   217| # EXCLUDED_FROM_SCORING directly -- keeps working unchanged; see
   218| # docs/governance_evidence_package.md.
   219| _DEFAULT_EXCLUDED_FROM_SCORING = {"view_templates_renderings_drafting"}
   220| EXCLUDED_FROM_SCORING = set(_DEFAULT_EXCLUDED_FROM_SCORING)
   221| 
   222| DISC_LABELS = {
   223|     "architectural": "Architectural",
   224|     "electrical": "Electrical",
   225|     "mechanical_plumbing": "Mechanical/Plumbing",
   226|     "structural": "Structural",
   227|     "fire_protection": "Fire Protection",
   228|     "low_voltage": "Low Voltage",
   229|     "water": "Water",
   230| }
   231| 
   232| 
   233| def _disc_label(disc: str) -> str:
   234|     """Display name for a discipline_label value. DISC_LABELS is an optional
   235|     override for known disciplines' display casing/punctuation (e.g.
   236|     "Mechanical/Plumbing" instead of a plain title-case render) -- it is NOT the
   237|     source of which disciplines exist. A discipline outside DISC_LABELS still
   238|     renders humanely (e.g. "medical_equipment" -> "Medical Equipment") rather
   239|     than crashing or being silently dropped from any disc-keyed section. See
   240|     docs/governance_narrative_scope_gap_audit.md C7.
   241|     """
   242|     return DISC_LABELS.get(disc, disc.replace("_", " ").title())
   243| 
   244| # Domains where passive inheritance is most likely to inflate all-view scores.
   245| # These domains are often fully inherited from templates but rarely customised.
   246| # Same default/override pattern as EXCLUDED_FROM_SCORING above.
   247| _DEFAULT_PASSIVE_INHERITANCE_RISK_DOMAINS = {
   248|     "arrowheads", "fill_patterns_drafting", "fill_patterns_model",
   249|     "line_patterns", "dimension_types_diameter", "dimension_types_radial",
   250|     "dimension_types_spot_coordinate", "dimension_types_spot_elevation",
   251|     "dimension_types_spot_slope", "object_styles_analytical",
   252| }
   253| PASSIVE_INHERITANCE_RISK_DOMAINS = set(_DEFAULT_PASSIVE_INHERITANCE_RISK_DOMAINS)
   254| 
   255| # Fixed editorial guidance text tied to a specific domain (rendered by
   256| # detect_anomalies() when that domain's own data-dependent condition also
   257| # fires), and guidance always rendered once in the findings section
   258| # regardless of domain. Same default/override pattern as above; sourced from
   259| # domain_governance_policy.json's domain_guidance/static_findings_guidance.
   260| _DEFAULT_DOMAIN_GUIDANCE = {
   261|     "phases": (
   262|         "Templates are internally consistent on phases but projects carry "
   263|         "phases not defined in templates — project teams are adding "
   264|         "project-specific phases."
   265|     ),
   266|     "loaded_family_types": (
   267|         "Family loading is inherently project-specific. "
   268|         "Template governance establishes a floor, not a ceiling. "
   269|         "Consider approved-list governance rather than full vocabulary convergence."
   270|     ),
   271| }
   272| DOMAIN_GUIDANCE = dict(_DEFAULT_DOMAIN_GUIDANCE)
   273| 
   274| _DEFAULT_STATIC_FINDINGS_GUIDANCE = [
   275|     "Loaded family types and materials should not be governed like object "
   276|     "styles. These domains are often project-specific. Review them for "
   277|     "approved lists, starter content, exception rules, or documentation "
   278|     "rather than full vocabulary convergence.",
   279| ]
   280| STATIC_FINDINGS_GUIDANCE = list(_DEFAULT_STATIC_FINDINGS_GUIDANCE)
   281| 
   282| 
   283| def detect_bundle_schema(rows: list) -> str:
   284|     """
   285|     Returns which bundle annotation schema is present:
   286|       'dual'   -- all_n_shared_bundle_both AND used_n_shared_bundle_both present
   287|       'single' -- only n_shared_bundle_both (pre-dual-view schema)
   288|       'none'   -- no bundle columns present
   289|     """
   290|     if not rows:
   291|         return "none"
   292|     sample = rows[0]
   293|     if "all_n_shared_bundle_both" in sample and "used_n_shared_bundle_both" in sample:
   294|         return "dual"
   295|     if "n_shared_bundle_both" in sample:
   296|         return "single"
   297|     return "none"
   298| 
   299| 
   300| 
   301| # ── schema normalisation ─────────────────────────────────────────────────────────────────
   302| 
   303| # Maps canonical renderer column names to actual CSV columns for both
   304| # pre-dual-view schema (bare names) and dual-view schema (all_/used_ prefixes).
   305| _SUMMARY_COL_ALIASES: dict = {}
   306| 
   307| 
   308| def normalise_summary_schema(rows: list) -> None:
   309|     """Inspect first row and build _SUMMARY_COL_ALIASES. Called once at startup."""
   310|     global _SUMMARY_COL_ALIASES
   311|     if not rows:
   312|         return
   313|     cols = set(rows[0].keys())
   314| 
   315|     def alias(canonical: str, *candidates: str) -> None:
   316|         """Map a canonical renderer column name to the first matching real
   317|         CSV column, trying names in priority order: current schema first,
   318|         then each older schema generation this file has ever produced."""
   319|         for name in candidates:
   320|             if name in cols:
   321|                 _SUMMARY_COL_ALIASES[canonical] = name
   322|                 return
   323| 
   324|     # Jaccard
   325|     alias("jaccard_mean",            "all_pairwise_jaccard_mean",            "all_jaccard_mean",            "jaccard_mean")
   326|     alias("jaccard_p10",             "all_jaccard_p10",             "jaccard_p10")
   327|     alias("jaccard_p90",             "all_jaccard_p90",             "jaccard_p90")
   328|     alias("used_jaccard_mean",       "used_pairwise_jaccard_mean",  "used_jaccard_mean",           "jaccard_mean")
   329|     alias("used_jaccard_p10",        "used_jaccard_p10",            "jaccard_p10")
   330|     alias("used_jaccard_p90",        "used_jaccard_p90",            "jaccard_p90")
   331|     # Containment
   332|     alias("containment_a_in_b_mean",      "all_pairwise_containment_a_in_b_mean",      "all_containment_a_in_b_mean",      "containment_a_in_b_mean")
   333|     alias("containment_b_in_a_mean",      "all_pairwise_containment_b_in_a_mean",      "all_containment_b_in_a_mean",      "containment_b_in_a_mean")
   334|     alias("used_containment_a_in_b_mean", "used_pairwise_containment_a_in_b_mean",     "used_containment_a_in_b_mean",     "containment_a_in_b_mean")
   335|     alias("used_containment_b_in_a_mean", "used_pairwise_containment_b_in_a_mean",     "used_containment_b_in_a_mean",     "containment_b_in_a_mean")
   336|     # Shared counts
   337|     alias("n_shared_join_hash",      "n_shared_join_hash",      "n_shared_join_hash")
   338|     alias("used_n_shared_join_hash", "used_n_shared_join_hash", "n_shared_join_hash")
   339|     # Bundle columns
   340|     alias("all_n_shared_bundle_both",   "all_n_shared_bundle_both",   "n_shared_bundle_both")
   341|     alias("all_n_shared_bundle_a_only", "all_n_shared_bundle_a_only", "n_shared_bundle_a_only")
   342|     alias("all_n_shared_bundle_b_only", "all_n_shared_bundle_b_only", "n_shared_bundle_b_only")
   343|     alias("used_n_shared_bundle_both",  "used_n_shared_bundle_both",  "n_shared_bundle_both")
   344|     # has_bundles
   345|     alias("has_bundles_a", "all_has_bundles_a", "has_bundles_a")
   346|     alias("has_bundles_b", "all_has_bundles_b", "has_bundles_b")
   347|     # Signal ambiguity
   348|     alias("signal_spread", "signal_spread", "signal_spread")
   349|     # Union metrics (population-footprint; independent of n_files_a x n_files_b).
   350|     # New in this schema generation -- no legacy predecessor to fall back to.
   351|     alias("all_union_jaccard", "all_union_jaccard")
   352|     alias("used_union_jaccard", "used_union_jaccard")
   353|     alias("all_union_containment_a_in_b", "all_union_containment_a_in_b")
   354|     alias("all_union_containment_b_in_a", "all_union_containment_b_in_a")
   355|     alias("used_union_containment_a_in_b", "used_union_containment_a_in_b")
   356|     alias("used_union_containment_b_in_a", "used_union_containment_b_in_a")
   357| 
   358| 
   359| def _col(row: dict, canonical: str) -> str:
   360|     """Read a summary row column using canonical renderer name."""
   361|     actual = _SUMMARY_COL_ALIASES.get(canonical, canonical)
   362|     return row.get(actual, "")
   363| 
   364| 
   365| def _resolved_col_name(canonical: str) -> str:
   366|     """Return the real CSV column name a canonical renderer name currently
   367|     resolves to (via _SUMMARY_COL_ALIASES), for callers that need to pass a
   368|     column NAME rather than read a value through _col()."""
   369|     return _SUMMARY_COL_ALIASES.get(canonical, canonical)
   370| 
   371| 
   372| def _col_union_or_pairwise(row: dict, union_canonical: str, pairwise_canonical: str) -> str:
   373|     """Read a union-metric column, falling back to the pairwise-mean family
   374|     when blank -- specifically for within_project rows. compare_cross_segment.py's
   375|     dedicated within-project branch (a single-segment, project-internal
   376|     aggregation with its own summary-row construction) returns before the
   377|     normal path's _union_similarity() call and never sets all_union_*/
   378|     used_union_* -- only all_pairwise_jaccard_mean/used_pairwise_jaccard_mean
   379|     are ever populated for this comparison type. cross_client/sibling_projects
   380|     rows always populate the union fields when they have real data (both are
   381|     non-directed types that hit the normal path's else branch), so this
   382|     fallback is a no-op for them -- do not use this for xc/xc_by_client/
   383|     xc_dom_by_client reads, which should never silently fall back."""
   384|     value = _col(row, union_canonical)
   385|     if value:
   386|         return value
   387|     return _col(row, pairwise_canonical)
   388| 
   389| 
   390| def used_view_falls_back_to_legacy() -> bool:
   391|     """True when canonical used-view columns resolved to legacy all-view names."""
   392|     legacy_pairs = {
   393|         "used_jaccard_mean": "jaccard_mean",
   394|         "used_jaccard_p10": "jaccard_p10",
   395|         "used_jaccard_p90": "jaccard_p90",
   396|         "used_containment_a_in_b_mean": "containment_a_in_b_mean",
   397|         "used_containment_b_in_a_mean": "containment_b_in_a_mean",
   398|         "used_n_shared_join_hash": "n_shared_join_hash",
   399|         "used_n_shared_bundle_both": "n_shared_bundle_both",
   400|     }
   401|     return any(_SUMMARY_COL_ALIASES.get(k) == v for k, v in legacy_pairs.items())
   402| 
   403| 
   404| # ── data loading ───────────────────────────────────────────────────────────────
   405| 
   406| def _is_unscoped_segment(row: dict, suffix: str) -> bool:
   407|     """True when a segment is the broadest (client/discipline-unscoped) population
   408|     for its governance role — the condition the old is_generic() tried to detect
   409|     via "segment_id has exactly 2 pipe-separated parts" (unit_system + role only).
   410| 
   411|     A blank governance_role is NOT this condition — it is a scope rollup with no
   412|     role filter at all (e.g. a business-center-wide rollup like "imperial|BC_2014"),
   413|     which also happens to produce a 2-part segment_id and was therefore
   414|     misclassified as "generic" by the old part-count check. See
   415|     docs/governance_narrative_scope_gap_audit.md B5.
   416| 
   417|     business_center_label / collection_label are not yet columns on SUMMARY_FIELDS
   418|     (see B6), so a row with role set and client_label/discipline_label both blank
   419|     could still be a business-center- or collection-scoped standard (e.g.
   420|     "imperial|Template|BC_1234" or "imperial|Template|collection:Shared") that
   421|     these three columns alone can't reveal. Once client_label/discipline_label are
   422|     confirmed blank, any EXTRA NON-EMPTY pipe-separated part in segment_id can only
   423|     have come from business_center_label/collection_label (per
   424|     build_segment_manifest.py's fixed field order) and must be rejected — but an
   425|     extra part that is itself EMPTY is not hidden data: build_segment_manifest.py's
   426|     _subset_to_id() emits a literal empty token for a client_label/discipline_label
   427|     dimension that IS selected as part of the segment's key but happens to have a
   428|     blank value (e.g. "imperial|Template||Shared" for a blank client_label
   429|     alongside a real business_center_label "Shared" -- see the comment in
   430|     _subset_to_id() itself), as distinct from a dimension simply absent from the
   431|     key (which contributes no token at all, e.g. "imperial|Template"). A segment
   432|     like "imperial|Generic|" (trailing blank client token, nothing else) is
   433|     therefore still genuinely unscoped and must not be rejected just because it
   434|     has more than 2 raw pipe-separated parts. This is a structural completeness
   435|     check on segment_id, not the positional-parsing anti-pattern removed elsewhere
   436|     in this file: it never reads a VALUE out of segment_id, it only confirms any
   437|     extra part is blank rather than a hidden scope token.
   438|     """
   439|     role = row.get(f"governance_role_{suffix}", "")
   440|     client = row.get(f"client_label_{suffix}", "")
   441|     disc = row.get(f"discipline_label_{suffix}", "")
   442|     if not role or client or disc:
   443|         return False
   444|     seg_id = row.get(f"segment_id_{suffix}", "")
   445|     if not seg_id:
   446|         return True
   447|     return all(p == "" for p in seg_id.split("|")[2:])
   448| 
   449| 
   450| def _target_scope_label(row: dict, suffix: str) -> str:
   451|     """Classify a generic_to_*'s TARGET side (Template/Container/Project) into a
   452|     scope-level bucket for gt/gc/gp's per-scope breakdown, using real columns
   453|     (client_label, business_center_label -- now on SUMMARY_FIELDS per B6,
   454|     discipline_label) rather than segment_id parsing.
   455| 
   456|     "enterprise" reuses _is_unscoped_segment()'s own definition of the broadest
   457|     population. Otherwise the bucket names every populated dimension
   458|     (client/bc/discipline), e.g. "client", "client_discipline". collection_label
   459|     is still not a SUMMARY_FIELDS column (residual B6 gap) -- a segment scoped
   460|     only by collection would have all three known dimensions blank yet still
   461|     fail _is_unscoped_segment()'s segment_id structural check, landing in
   462|     "other_scoped" rather than being silently mislabeled "enterprise".
   463|     """
   464|     if _is_unscoped_segment(row, suffix):
   465|         return "enterprise"
   466|     parts = []
   467|     if row.get(f"client_label_{suffix}", ""):
   468|         parts.append("client")
   469|     if row.get(f"business_center_label_{suffix}", ""):
   470|         parts.append("bc")
   471|     if row.get(f"discipline_label_{suffix}", ""):
   472|         parts.append("discipline")
   473|     return "_".join(parts) if parts else "other_scoped"
   474| 
   475| 
   476| # Maps a _target_scope_label() shape to the field(s) that make it up, so
   477| # _group1_scope_pair() can verify VALUE equality (not just shape equality)
   478| # when both sides land on the same shape. "enterprise" and "other_scoped"
   479| # are intentionally absent: "enterprise" means every one of these fields is
   480| # blank on both sides (nothing to compare), and "other_scoped" is the
   481| # collection-only-scoped catch-all -- collection_label is still not a
   482| # SUMMARY_FIELDS column (residual B6 gap noted in _target_scope_label()'s own
   483| # docstring), so its value can't be verified with the columns available today.
   484| _SCOPE_DIMENSION_FIELDS = {
   485|     "client": ("client_label",),
   486|     "bc": ("business_center_label",),
   487|     "discipline": ("discipline_label",),
   488|     "client_bc": ("client_label", "business_center_label"),
   489|     "client_discipline": ("client_label", "discipline_label"),
   490|     "bc_discipline": ("business_center_label", "discipline_label"),
   491|     "client_bc_discipline": ("client_label", "business_center_label", "discipline_label"),
   492| }
   493| 
   494| 
```
