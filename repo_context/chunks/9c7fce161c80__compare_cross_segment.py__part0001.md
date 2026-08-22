# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 1 of 13
- Original line range: 1-498
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: read_csv_rows, atomic_write_csv
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """Cross-segment comparison tool.
     2| 
     3| Compares pattern vocabularies across segments using join_hash as the
     4| cross-segment identity unit.
     5| 
     6| Single measurement path
     7| -----------------------
     8| Comparisons prefer per-file join_hash inventories from membership_matrix.csv
     9| and resolve join_hash via domain_patterns.csv (source_cluster_id.split('|')[-1]).
    10| Generic/reference segments that only provide analysis outputs can fall back to
    11| domain_patterns.csv for all-view provision inventories. There is no bundle-mode /
    12| file-mode branch. All set operations (Jaccard, containment) operate on the full
    13| join_hash inventories loaded for the selected view.
    14| 
    15| Bundle membership as post-hoc annotation
    16| -----------------------------------------
    17| After computing scores, bundle membership is looked up from
    18| bundle_analysis/{all,used}/<domain>/bundle_membership.csv for each segment and
    19| annotated onto n_shared using two views and three buckets each:
    20| 
    21|   all_n_shared_bundle_both   — join_hashes in shared that are bundle members in
    22|                                BOTH segments under the all view
    23|   all_n_shared_bundle_a_only — bundle member in A (all view), not B
    24|   all_n_shared_bundle_b_only — bundle member in B (all view), not A
    25|   used_*                     — same three columns for the used view
    26| 
    27| The used view excludes patterns that are conclusively purgeable; the delta
    28| between all and used views quantifies passive inheritance.
    29| 
    30| All-view vs used-view scores
    31| -----------------------------
    32| Jaccard and containment scores are computed independently from both the all-view
    33| and used-view membership matrices. All-view scores (all_jaccard_*, all_containment_*)
    34| reflect the full configured pattern vocabulary. Used-view scores (used_jaccard_*,
    35| used_containment_*) reflect only patterns present in active view/sheet assignments.
    36| The delta between all-view and used-view scores quantifies passive inheritance —
    37| patterns configured but never rendered. used_n_shared_join_hash is the count of
    38| join_hashes that appear in both segments' used-view inventories.
    39| 
    40| N-1 pooled comparison (cross_segment_pooled.csv)
    41| -------------------------------------------------
    42| Each segment is compared against the union of all sibling segments sharing the
    43| same (parent_segment_id, governance_role, unit_system). This is the primary
    44| signal for small segments where pairwise Jaccard is dominated by size asymmetry.
    45| Containment in both directions is reported for both all and used views; no
    46| Jaccard is computed on this file.
    47| 
    48| Sufficiency and ambiguity judgment
    49| -----------------------------------
    50| Scores are always computed and emitted, along with the raw counts
    51| (n_files_a/b, n_files_focal/pool, n_shared_join_hash, n_unique_patterns_*)
    52| needed to judge them. This file does not classify a comparison as
    53| interpretable, sufficient, or ambiguous — no score_ambiguity_band label.
    54| signal_spread is reported as a raw float (computed from the same
    55| shared/unique counts) for downstream banding; it is not itself a judgment.
    56| That interpretive layer belongs to generate_governance_narrative.py.
    57| 
    58| comparison_status is the one exception: it is explicit, non-suppressive
    59| cardinality metadata (ok/degraded/blocked, computed purely from file
    60| counts), not a judgment about the scores themselves. blocked means zero
    61| readable file inventory on a required side; degraded means one side has
    62| exactly one file while the other has more; everything else, including a
    63| symmetric 1x1 comparison, is ok. No comparison is ever suppressed on this
    64| basis — this is the replacement for the removed n_files >= 5
    65| data_sufficient gate, which silently hid narrow-but-valid rows instead of
    66| labeling them. cardinality_shape and file_count_ratio are purely
    67| descriptive siblings of comparison_status and never gate output either.
    68| 
    69| Cartesian pairwise means (all_pairwise_jaccard_mean, used_pairwise_jaccard_mean,
    70| all_pairwise_containment_a_in_b_mean, etc.; aggregation_method =
    71| "cartesian_file_pair_mean") answer "what's the mean of all A-file x B-file
    72| pairs" -- a different question from the population-union metrics
    73| (all_union_jaccard, all_union_containment_a_in_b/b_in_a, and their used_
    74| counterparts), which answer "how similar are these two populations" from
    75| each side's union footprint, independent of n_files_a x n_files_b. The two
    76| families diverge exactly when file counts are imbalanced; neither
    77| supersedes the other. all_a_file_mean_similarity_to_b_mean/min and its B
    78| counterpart expose directional population experience for symmetric
    79| comparisons -- in a 1xN comparison, the A-side summary is one file's
    80| average similarity to N files, while the B-side summary is the
    81| distribution of N files against that one A file.
    82| 
    83| Directed comparisons keep the reference-union -> per-target-file-
    84| distribution approach (reference_aggregation = "union", target_aggregation
    85| = "per_file_distribution"); reference_union_pattern_count,
    86| reference_intersection_pattern_count, and reference_core_share (=
    87| intersection/union across every reference file) are heterogeneity
    88| diagnostics that reveal whether a multi-file reference is a coherent
    89| standard or a broad union of conflicting sources, independent of how well
    90| any target matches it. reference_core_share degrades to 1.0 for a
    91| single-file reference -- not an artificial failure.
    92| 
    93| Reference segment participation
    94| --------------------------------
    95| Reference segments participate in generic_to_template, generic_to_container,
    96| generic_to_project, template_to_project, template_to_container, and
    97| container_to_project comparisons using their file inventories from
    98| membership_matrix.csv when present. Generic/reference provided-vocabulary sources
    99| may not emit bundle_analysis membership matrices; for all-view comparisons they
   100| fall back to domain_patterns.csv. They will have has_bundles = "false" for most
   101| domains, often alongside small n_files counts — this is expected and correct.
   102| 
   103| Governance all/used semantics
   104| ------------------------------
   105| The provision chain is Generic / Generic-Host → Template → Container → Project
   106| all-view vocabulary. The usage chain is Project all → Project used. Generic,
   107| Template, and most Container segments are standards-carrier / provided-vocabulary
   108| references; used-view and purge signals are meaningful primarily when the target
   109| role is Project and must not be used to label Template or Generic stock content
   110| as unused bloat. Directed governance-state output therefore compares upstream
   111| reference all-view vocabulary to downstream target all-view and, for Project
   112| targets, target used-view vocabulary.
   113| 
   114| Organizational scope levels
   115| ----------------------------
   116| Scope is derived from explicit, literal client_label/business_center_label
   117| values (see _scope_level() / _is_client_wide_rollup()), not blank inference:
   118| enterprise (client_label=="InternalEnterprise", business_center_label=="0000" --
   119| "BC_0000"/any-case spelling variants canonicalize to "0000" via
   120| _normalize_bc_label(), they are not folded to blank),
   121| business_center (client_label=="InternalEnterprise", a real business_center_label), and
   122| client_business_center (a real external client_label, a real
   123| business_center_label). A row where either dimension isn't cut at all
   124| (blank) is a roll-up pooling multiple real scopes, handled by individual
   125| comparison-type discovery rather than by _scope_level() itself. The
   126| governance chain fans out across these levels: enterprise_to_project,
   127| bc_to_project, enterprise_to_bc, enterprise_to_client (discover_governance_
   128| chain()), bc_to_bc (peer business centers, also in discover_governance_
   129| chain()), cross_client (discover_cross_client(), now grouped by discipline_
   130| label too), and client_cross_bc (discover_client_cross_bc(), a real client's
   131| populations compared across every business center it appears in).
   132| 
   133| Usage:
   134|     python tools/compare_cross_segment.py \\
   135|         --segments-root segments/ \\
   136|         --records-dir   results/records/ \\
   137|         --out-dir       results/cross_segment/ \\
   138|         [--within-segment] [--sibling-segments] [--parent-siblings] \\
   139|         [--within-project] [--governance-chain] [--cross-client] \\
   140|         [--domain DOMAIN] [--segment-a ID] [--segment-b ID] \\
   141|         [--min-patterns INT] [--dry-run] [--no-delta]
   142| """
   143| from __future__ import annotations
   144| 
   145| import argparse
   146| import csv
   147| import hashlib
   148| import os
   149| import sys
   150| import time
   151| from concurrent.futures import ProcessPoolExecutor, as_completed
   152| from collections import defaultdict
   153| from datetime import datetime, timezone
   154| from itertools import combinations
   155| from pathlib import Path
   156| from tempfile import NamedTemporaryFile
   157| from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
   158| 
   159| _TOOLS_DIR = str(Path(__file__).resolve().parent)
   160| if _TOOLS_DIR not in sys.path:
   161|     sys.path.insert(0, _TOOLS_DIR)
   162| 
   163| from na_token import is_blank_or_na, ENTERPRISE_BC_BOOKKEEPING_TOKENS as _ENTERPRISE_BC_BOOKKEEPING_TOKENS
   164| from jenks_utils import jenks_breaks
   165| from enterprise_policy import EnterprisePolicy, load_enterprise_policy, write_enterprise_policy_provenance
   166| 
   167| 
   168| # ---------------------------------------------------------------------------
   169| # I/O helpers
   170| # ---------------------------------------------------------------------------
   171| 
   172| def read_csv_rows(path: Path) -> List[Dict[str, str]]:
   173|     with path.open("r", encoding="utf-8-sig", newline="") as f:
   174|         return [
   175|             {str(k): ("" if v is None else str(v)) for k, v in row.items()}
   176|             for row in csv.DictReader(f)
   177|         ]
   178| 
   179| 
   180| def atomic_write_csv(
   181|     path: Path,
   182|     fieldnames: Sequence[str],
   183|     rows: Iterable[Dict[str, str]],
   184| ) -> None:
   185|     path.parent.mkdir(parents=True, exist_ok=True)
   186|     with NamedTemporaryFile(
   187|         "w", encoding="utf-8", newline="", delete=False,
   188|         dir=str(path.parent), suffix=".tmp",
   189|     ) as tmp:
   190|         tmp_path = Path(tmp.name)
   191|         writer = csv.DictWriter(tmp, fieldnames=list(fieldnames))
   192|         writer.writeheader()
   193|         for row in rows:
   194|             writer.writerow({name: row.get(name, "") for name in fieldnames})
   195|     tmp_path.replace(path)
   196| 
   197| 
   198| # ---------------------------------------------------------------------------
   199| # Output schemas
   200| # ---------------------------------------------------------------------------
   201| 
   202| SUMMARY_FIELDS: List[str] = [
   203|     "comparison_run_id",
   204|     "segment_id_a", "segment_id_b",
   205|     "segment_label_a", "segment_label_b",
   206|     "governance_role_a", "governance_role_b",
   207|     "client_label_a", "client_label_b",
   208|     "business_center_label_a", "business_center_label_b",
   209|     "scope_level_a", "scope_level_b",
   210|     "discipline_label_a", "discipline_label_b",
   211|     "unit_system",
   212|     "comparison_type",
   213|     "domain",
   214|     "n_patterns_a", "n_patterns_b", "n_shared_join_hash",
   215|     "n_unique_patterns_a", "n_unique_patterns_b",
   216|     "signal_spread",
   217|     "all_pairwise_containment_a_in_b_mean", "all_containment_a_in_b_min",
   218|     "all_pairwise_containment_b_in_a_mean", "all_containment_b_in_a_min",
   219|     "all_pairwise_jaccard_mean", "all_jaccard_p10", "all_jaccard_p90",
   220|     "used_pairwise_jaccard_mean", "used_jaccard_p10", "used_jaccard_p90",
   221|     "used_pairwise_containment_a_in_b_mean", "used_containment_a_in_b_min",
   222|     "used_pairwise_containment_b_in_a_mean", "used_containment_b_in_a_min",
   223|     "used_n_shared_join_hash",
   224|     "aggregation_method",
   225|     "all_union_jaccard", "all_union_containment_a_in_b", "all_union_containment_b_in_a",
   226|     "used_union_jaccard", "used_union_containment_a_in_b", "used_union_containment_b_in_a",
   227|     "all_a_file_mean_similarity_to_b_mean", "all_a_file_mean_similarity_to_b_min",
   228|     "all_b_file_mean_similarity_to_a_mean", "all_b_file_mean_similarity_to_a_min",
   229|     "reference_aggregation", "target_aggregation", "n_reference_files",
   230|     "reference_union_pattern_count", "reference_intersection_pattern_count", "reference_core_share",
   231|     "all_has_bundles_a", "all_has_bundles_b",
   232|     "all_n_shared_bundle_both", "all_n_shared_bundle_a_only", "all_n_shared_bundle_b_only",
   233|     "used_has_bundles_a", "used_has_bundles_b",
   234|     "used_n_shared_bundle_both", "used_n_shared_bundle_a_only", "used_n_shared_bundle_b_only",
   235|     "n_files_a", "n_files_b", "n_pairs",
   236|     "comparison_status", "cardinality_shape", "file_count_ratio",
   237|     "inventory_status_a", "inventory_status_b",
   238|     "reference_usage_interpretable",
   239|     "target_usage_interpretable",
   240|     "recommended_primary_view",
   241|     "comparison_role_semantics",
   242|     "executed_utc",
   243| ]
   244| 
   245| PAIRS_FIELDS: List[str] = [
   246|     "comparison_run_id",
   247|     "segment_id_a", "segment_id_b",
   248|     "domain",
   249|     "export_run_id_a", "export_run_id_b",
   250|     "project_label_a", "project_label_b",
   251|     "n_patterns_a", "n_patterns_b", "n_shared",
   252|     "all_jaccard", "all_containment_a_in_b", "all_containment_b_in_a",
   253|     "used_n_shared", "used_jaccard", "used_containment_a_in_b", "used_containment_b_in_a",
   254|     "all_n_shared_bundle_both", "all_n_shared_bundle_a_only", "all_n_shared_bundle_b_only",
   255|     "used_n_shared_bundle_both", "used_n_shared_bundle_a_only", "used_n_shared_bundle_b_only",
   256| ]
   257| 
   258| DELTA_FIELDS: List[str] = [
   259|     "comparison_run_id",
   260|     "segment_id_reference", "segment_id_target",
   261|     "segment_label_reference", "segment_label_target",
   262|     "comparison_type", "domain",
   263|     "join_hash",
   264|     "pattern_label",
   265|     "n_files_in_target",
   266|     "pct_files_in_target",
   267|     "in_any_container",
   268|     "in_any_template",
   269|     "used_pct_files_in_target",
   270|     "is_bundle_member_all",
   271|     "is_bundle_member_used",
   272|     "delta_class",
   273|     "executed_utc",
   274| ]
   275| 
   276| COMPARISON_REGISTRY_FIELDS: List[str] = [
   277|     "segment_id_a", "segment_id_b", "comparison_type", "domain",
   278|     "population_hash_a", "population_hash_b",
   279|     "last_run_utc_a", "last_run_utc_b",
   280|     "conformance_reference_mode",
   281|     "computed_utc",
   282| ]
   283| 
   284| POOLED_FIELDS: List[str] = [
   285|     "comparison_run_id",
   286|     "segment_id", "segment_label",
   287|     "governance_role", "client_label",
   288|     "business_center_label",
   289|     "scope_level",
   290|     "unit_system",
   291|     "domain",
   292|     "pool_scope",
   293|     "n_files_focal", "n_files_pool",
   294|     "comparison_status", "cardinality_shape", "file_count_ratio",
   295|     "n_unique_patterns_focal", "n_unique_patterns_pool", "n_shared_join_hash",
   296|     "signal_spread",
   297|     "all_containment_focal_in_pool", "all_containment_pool_in_focal",
   298|     "used_containment_focal_in_pool", "used_containment_pool_in_focal",
   299|     "all_has_bundles_focal", "all_has_bundles_pool",
   300|     "all_n_shared_bundle_both", "all_n_shared_bundle_focal_only", "all_n_shared_bundle_pool_only",
   301|     "used_has_bundles_focal", "used_has_bundles_pool",
   302|     "used_n_shared_bundle_both", "used_n_shared_bundle_focal_only", "used_n_shared_bundle_pool_only",
   303|     "executed_utc",
   304| ]
   305| 
   306| GOVERNANCE_STATE_FIELDS: List[str] = [
   307|     "comparison_run_id",
   308|     "comparison_type",
   309|     "segment_id_reference", "segment_id_target",
   310|     "segment_label_reference", "segment_label_target",
   311|     "governance_role_reference", "governance_role_target",
   312|     "business_center_label_reference", "business_center_label_target",
   313|     "unit_system",
   314|     "domain",
   315|     "join_hash",
   316|     "pattern_label",
   317|     "in_reference_all",
   318|     "in_target_all",
   319|     "in_target_used",
   320|     "state",
   321|     "n_files_in_target_all",
   322|     "pct_files_in_target_all",
   323|     "n_files_in_target_used",
   324|     "pct_files_in_target_used",
   325|     "in_any_generic",
   326|     "in_any_template",
   327|     "in_any_container",
   328|     "is_bundle_member_target_all",
   329|     "is_bundle_member_target_used",
   330|     "reference_usage_interpretable",
   331|     "target_usage_interpretable",
   332|     "recommended_primary_view",
   333|     "executed_utc",
   334| ]
   335| 
   336| 
   337| UNION_INVENTORY_FIELDS: List[str] = [
   338|     "governance_role",
   339|     "client_label",
   340|     "discipline_label",
   341|     "unit_system",
   342|     "domain",
   343|     "view_scope",
   344|     "join_hash",
   345|     "pattern_label",
   346|     "n_segments_present",
   347|     "n_files_present",
   348|     "n_files_denominator",
   349|     "pct_files_present",
   350|     "n_projects_present",
   351|     "n_projects_denominator",
   352|     "n_clients_present",
   353|     "n_clients_denominator",
   354|     "pct_clients_present",
   355|     "pct_projects_present",
   356|     "usage_interpretable",
   357|     "inventory_status",
   358|     "source_status",
   359|     "executed_utc",
   360| ]
   361| 
   362| 
   363| REUSE_DISTRIBUTION_FIELDS: List[str] = [
   364|     "view_scope",
   365|     "governance_role",
   366|     "client_label",
   367|     "discipline_label",
   368|     "unit_system",
   369|     "domain",
   370|     "join_hash",
   371|     "pattern_label",
   372|     "n_files_present",
   373|     "n_files_denominator",
   374|     "pct_files_present",
   375|     "n_projects_present",
   376|     "n_projects_denominator",
   377|     "pct_projects_present",
   378|     "n_clients_present",
   379|     "n_clients_denominator",
   380|     "pct_clients_present",
   381|     "reuse_bucket",
   382|     "bucket_basis",
   383|     "usage_interpretable",
   384|     "inventory_status",
   385|     "classification_status",
   386|     "executed_utc",
   387| ]
   388| 
   389| 
   390| 
   391| MATRIX_OUTPUT_FIELDS: List[str] = [
   392|     "matrix_name", "row_id", "column_id", "view_scope", "domain", "metric",
   393|     "value", "value_status", "self_comparison", "interpretation", "executed_utc",
   394| ]
   395| 
   396| FRAGMENTATION_DIAGNOSTIC_FIELDS: List[str] = [
   397|     "matrix_name", "row_id", "column_id", "view_scope", "domain",
   398|     "footprint_similarity", "exact_identity_overlap", "fragmentation_diagnostic",
   399|     "value_status", "interpretation", "executed_utc",
   400| ]
   401| 
   402| MATRIX_MANIFEST_FIELDS: List[str] = [
   403|     "matrix_name", "governance_role", "view_scope", "source_file",
   404|     "source_grain", "metric", "identity_unit", "aggregation_method",
   405|     "interpretation", "known_limitations", "executed_utc",
   406| ]
   407| 
   408| REUSE_SUMMARY_FIELDS: List[str] = [
   409|     "view_scope",
   410|     "governance_role",
   411|     "client_label",
   412|     "discipline_label",
   413|     "unit_system",
   414|     "domain",
   415|     "reuse_bucket",
   416|     "bucket_basis",
   417|     "n_patterns",
   418|     "usage_interpretable",
   419|     "classification_status",
   420|     "executed_utc",
   421| ]
   422| 
   423| # Centralized neutral reporting thresholds for reuse breadth. These are
   424| # classifications for governance reporting, not correctness judgments.
   425| REUSE_BUCKET_THRESHOLDS = {
   426|     "corpus_wide_min_pct_clients": 0.80,
   427|     "client_wide_min_pct_files": 0.80,
   428|     "multi_project_min_projects": 3,
   429|     "emerging_min_files": 2,
   430| }
   431| 
   432| GOVERNANCE_STATE_SUMMARY_FIELDS: List[str] = [
   433|     "comparison_run_id",
   434|     "comparison_type",
   435|     "segment_id_reference", "segment_id_target",
   436|     "segment_label_reference", "segment_label_target",
   437|     "governance_role_reference", "governance_role_target",
   438|     "business_center_label_reference", "business_center_label_target",
   439|     "unit_system",
   440|     "domain",
   441|     "reference_all_count",
   442|     "target_all_count",
   443|     "target_used_count",
   444|     "provided_to_configured_containment",
   445|     "provided_to_used_containment",
   446|     "provided_passive_share",
   447|     "provided_missing_share",
   448|     "local_active_share",
   449|     "provided_and_used_count",
   450|     "provided_but_passive_count",
   451|     "provided_but_missing_count",
   452|     "local_active_count",
   453|     "local_passive_count",
   454|     "local_unbundled_count",
   455|     "provided_configured_count",
   456|     "local_configured_count",
   457|     "provided_and_used_pct_of_reference_all",
   458|     "provided_but_passive_pct_of_reference_all",
   459|     "provided_but_missing_pct_of_reference_all",
   460|     "local_active_pct_of_target_used",
   461|     "local_passive_pct_of_target_all",
   462|     "local_unbundled_pct_of_target_all",
   463|     "reference_usage_interpretable",
   464|     "target_usage_interpretable",
   465|     "recommended_primary_view",
   466|     "comparison_role_semantics",
   467|     "executed_utc",
   468| ]
   469| 
   470| # Comparison types for which delta rows are emitted (directed, reference side defined).
   471| DELTA_DIRECTED_TYPES = {
   472|     "template_to_project",
   473|     "template_to_container",
   474|     "container_to_project",
   475|     "enterprise_to_project",
   476|     "bc_to_project",
   477| }
   478| 
   479| GOVERNANCE_STATE_DIRECTED_TYPES = {
   480|     "generic_to_template",
   481|     "generic_to_container",
   482|     "generic_to_project",
   483|     "template_to_project",
   484|     "template_to_container",
   485|     "container_to_project",
   486|     "enterprise_to_project",
   487|     "bc_to_project",
   488|     "enterprise_to_bc",
   489|     "enterprise_to_client",
   490| }
   491| 
   492| GENERIC_ROLE_KEYS = {"generic", "generic-host", "generic_host"}
   493| 
   494| 
   495| # ---------------------------------------------------------------------------
   496| # Delta pattern classification
   497| # ---------------------------------------------------------------------------
   498| 
```
