# Chunk of tools/analyze_promotion_candidates.py

- Source relative path: `tools/analyze_promotion_candidates.py`
- Chunk: 3 of 3
- Original line range: 574-1066
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main, main._join_labels, main._row_is_unclassified, main._route
- Source SHA-256: 1e5dbc478f5d1e9f1e948261cb2b0121cc79fd2657bbc116c27e7eeea845924b
- Starts inside symbol: no
- Ends inside symbol: no

```
   574| def main(argv=None):
   575|     cfg = parse_args(argv)
   576| 
   577|     root = cfg["root"]
   578|     out_dir = cfg["output"]
   579|     gov_states_path = root / "cross_segment_governance_states.csv"
   580|     reuse_dist_path = root / "pattern_reuse_distribution.csv"
   581| 
   582|     priority_domains = cfg["domains"]
   583|     baseline_threshold = cfg["baseline_threshold"]
   584|     min_enterprise_clients = cfg["min_enterprise_clients"]
   585|     enable_semantic_noise_filter = cfg["enable_semantic_noise_filter"]
   586|     export_top = cfg["export_top"]
   587|     verbose = cfg["verbose"]
   588| 
   589|     if verbose:
   590|         print("")
   591|         print("Promotion / Scope-Consistency Analysis")
   592|         print(f"Root: {root}")
   593|         print(f"Output: {out_dir}")
   594|         print(f"Domains: {', '.join(sorted(priority_domains))}")
   595|         print(f"Baseline threshold: {baseline_threshold}")
   596|         print(f"Minimum enterprise clients: {min_enterprise_clients}")
   597|         print(f"Semantic noise filter enabled: {enable_semantic_noise_filter}")
   598|         print(f"Export top per class: {export_top}")
   599|         print("")
   600| 
   601|     if not gov_states_path.exists():
   602|         raise FileNotFoundError(f"Missing file: {gov_states_path}")
   603|     if not reuse_dist_path.exists():
   604|         raise FileNotFoundError(f"Missing file: {reuse_dist_path}")
   605|     policy = cfg["enterprise_policy"]
   606| 
   607|     # ========================================================
   608|     # LOAD
   609|     # ========================================================
   610| 
   611|     print("Loading governance states...")
   612|     gov = pd.read_csv(gov_states_path, low_memory=False)
   613| 
   614|     print("Loading reuse distribution...")
   615|     reuse = pd.read_csv(reuse_dist_path, low_memory=False)
   616| 
   617|     require_columns(
   618|         gov,
   619|         [
   620|             "domain", "join_hash", "pattern_label", "state", "unit_system",
   621|             "target_usage_interpretable", "n_files_in_target_used",
   622|             "pct_files_in_target_used", "in_any_template", "in_any_container",
   623|             "in_any_generic", "comparison_type", "governance_role_reference",
   624|             "in_reference_all", "segment_id_target",
   625|         ],
   626|         "cross_segment_governance_states.csv",
   627|     )
   628| 
   629|     require_columns(
   630|         reuse,
   631|         [
   632|             "domain", "join_hash", "pattern_label", "view_scope", "unit_system",
   633|             "governance_role", "client_label", "discipline_label", "reuse_bucket",
   634|             "n_projects_present", "n_projects_denominator",
   635|             "n_clients_present", "n_clients_denominator",
   636|             "n_files_present", "n_files_denominator",
   637|             "pct_projects_present", "pct_clients_present",
   638|         ],
   639|         "pattern_reuse_distribution.csv",
   640|     )
   641|     # Invalid sources are non-writing operations.
   642|     out_dir.mkdir(parents=True, exist_ok=True)
   643| 
   644|     for col in ("in_reference_all", "in_target_all", "in_target_used",
   645|                 "in_any_template", "in_any_container", "in_any_generic",
   646|                 "target_usage_interpretable"):
   647|         if col in gov.columns:
   648|             gov[col] = safe_bool_series(gov[col])
   649| 
   650|     gov = gov[gov["domain"].isin(priority_domains)].copy()
   651|     reuse = reuse[reuse["domain"].isin(priority_domains)].copy()
   652| 
   653|     for col in ("n_clients_present", "n_projects_present", "n_files_present",
   654|                 "n_clients_denominator", "n_projects_denominator", "n_files_denominator"):
   655|         reuse[col] = pd.to_numeric(reuse[col], errors="coerce").fillna(0)
   656| 
   657|     # ========================================================
   658|     # BASE POPULATION: locally-active, usage-interpretable rows
   659|     # ========================================================
   660| 
   661|     active = gov[
   662|         (gov["state"] == "local_active") & (gov["target_usage_interpretable"])
   663|     ].copy()
   664| 
   665|     if verbose:
   666|         print(f"Local-active rows after domain filter: {len(active):,}")
   667| 
   668|     # Identity is (domain, join_hash, unit_system) -- join_hash is the
   669|     # cross-segment identity per compare_cross_segment.py's own contract;
   670|     # pattern_label is target-derived display text (from that target's
   671|     # domain_patterns.csv, falling back to the reference's label only when
   672|     # the target's own is blank -- see build_governance_state_outputs()),
   673|     # not part of the identity. Grouping by pattern_label as well as
   674|     # join_hash would split one pattern's evidence into multiple rows
   675|     # whenever two targets happen to carry differently-spelled labels for
   676|     # the same join_hash, undercounting files_used/ranking evidence.
   677|     #
   678|     # A single target segment shows up once per reference it was compared
   679|     # against (Template, Enterprise, BC, ...), each carrying the same
   680|     # n_files_in_target_used for that target (it depends only on the
   681|     # target's own file population, not on the reference side). Collapse to
   682|     # one row per (domain, join_hash, unit_system, segment_id_target) first,
   683|     # or summing n_files_in_target_used below double/triple-counts the same
   684|     # target files once per reference comparison it appeared in. Within one
   685|     # target, pattern_label is effectively constant (it's keyed off that
   686|     # same target's own label lookup), so "first" is safe here; label
   687|     # variation across *different* targets is preserved (";"-joined) in the
   688|     # outer aggregation below rather than picked arbitrarily.
   689|     active_by_target = (
   690|         active.groupby(
   691|             ["domain", "join_hash", "unit_system", "segment_id_target"], dropna=False
   692|         )
   693|         .agg(
   694|             pattern_label=("pattern_label", "first"),
   695|             n_files_in_target_used=("n_files_in_target_used", "max"),
   696|             pct_files_in_target_used=("pct_files_in_target_used", "max"),
   697|             in_any_template=("in_any_template", "max"),
   698|             in_any_container=("in_any_container", "max"),
   699|             in_any_generic=("in_any_generic", "max"),
   700|         )
   701|         .reset_index()
   702|     )
   703| 
   704|     def _join_labels(s):
   705|         labels = sorted({str(x) for x in s if pd.notna(x) and str(x).strip()})
   706|         return ";".join(labels)
   707| 
   708|     base = (
   709|         active_by_target.groupby(["domain", "join_hash", "unit_system"], dropna=False)
   710|         .agg(
   711|             pattern_label=("pattern_label", _join_labels),
   712|             files_used=("n_files_in_target_used", "sum"),
   713|             max_pct_used=("pct_files_in_target_used", "max"),
   714|             any_template=("in_any_template", "max"),
   715|             any_container=("in_any_container", "max"),
   716|             any_generic=("in_any_generic", "max"),
   717|         )
   718|         .reset_index()
   719|     )
   720| 
   721|     # ========================================================
   722|     # SCOPE RESOLUTION
   723|     # ========================================================
   724| 
   725|     join_keys = ["domain", "join_hash", "unit_system"]
   726| 
   727|     seeded = compute_seeded_scope(gov)
   728|     reuse_classified, reuse_unclassified = compute_reuse_scope(reuse, min_enterprise_clients, policy)
   729| 
   730|     df = base.merge(seeded, on=join_keys, how="left")
   731|     df["seeded_scope"] = df["seeded_scope"].fillna("ungoverned")
   732|     df["seeded_via_comparison_types"] = df["seeded_via_comparison_types"].fillna("")
   733| 
   734|     df = df.merge(reuse_classified, on=join_keys, how="left")
   735| 
   736|     unclassified_join_hashes = set(
   737|         zip(reuse_unclassified["domain"], reuse_unclassified["join_hash"],
   738|             reuse_unclassified["unit_system"])
   739|     ) if not reuse_unclassified.empty else set()
   740| 
   741|     def _row_is_unclassified(row):
   742|         return (row["domain"], row["join_hash"], row["unit_system"]) in unclassified_join_hashes
   743| 
   744|     df["reuse_data_unclassified"] = df.apply(_row_is_unclassified, axis=1) & df["reuse_scope"].isna()
   745|     df["reuse_scope"] = df["reuse_scope"].fillna(
   746|         df["reuse_data_unclassified"].map({True: "unclassified", False: "ungoverned"})
   747|     )
   748|     df["reuse_view_source"] = df["reuse_view_source"].fillna("")
   749| 
   750|     df["project_penetration"] = (
   751|         df["n_projects_present"].fillna(0)
   752|         / df["n_projects_denominator"].replace(0, np.nan)
   753|     ).fillna(0)
   754| 
   755|     # Consistency check: did the coarse flattened boolean see governance
   756|     # evidence that the finer comparison_type-based recovery missed? A
   757|     # True here means COMPARISON_TYPE_TO_SEED_SCOPE's mapping has a gap
   758|     # worth investigating -- surfaced for hand-verification, not acted on.
   759|     df["seeded_scope_consistency_flag"] = (
   760|         (df["any_template"] | df["any_container"]) & (df["seeded_scope"] == "ungoverned")
   761|     )
   762| 
   763|     # ========================================================
   764|     # ROUTING
   765|     # ========================================================
   766| 
   767|     df["seeded_rank"] = df["seeded_scope"].map(SCOPE_RANK)
   768|     df["reuse_rank"] = df["reuse_scope"].map(SCOPE_RANK)
   769| 
   770|     df["is_baseline_infrastructure"] = (
   771|         (df["project_penetration"] >= baseline_threshold) & (df["seeded_scope"] != "ungoverned")
   772|     )
   773| 
   774|     semantic_noise = pd.Series(False, index=df.index)
   775|     if enable_semantic_noise_filter:
   776|         noise_patterns = [r"\|self$", r"<Hidden Lines>"]
   777|         semantic_noise = (
   778|             df["pattern_label"].fillna("").str.contains(
   779|                 "|".join(noise_patterns), case=False, regex=True
   780|             )
   781|         )
   782|     df["semantic_noise"] = semantic_noise
   783| 
   784|     def _route(row):
   785|         if row["semantic_noise"]:
   786|             return "semantic_noise_excluded"
   787|         if row["is_baseline_infrastructure"]:
   788|             return "baseline_adequately_governed"
   789|         if row["reuse_scope"] == "unclassified":
   790|             return "unclassified_reuse"
   791|         if row["reuse_rank"] < 0:
   792|             return "unclassified_reuse"
   793|         if row["reuse_scope"] == "ungoverned" and row["seeded_scope"] == "ungoverned":
   794|             return "below_reuse_floor"
   795|         if row["reuse_rank"] < row["seeded_rank"]:
   796|             return "governed_but_underused"
   797|         if row["reuse_rank"] <= row["seeded_rank"]:
   798|             return "baseline_adequately_governed"
   799|         return "promotion_candidates"
   800| 
   801|     df["routing_bucket"] = df.apply(_route, axis=1)
   802| 
   803|     candidate_class_labels = {
   804|         "enterprise": "consistency_footprint_matches_enterprise_scope",
   805|         "bc": "consistency_footprint_matches_bc_scope",
   806|         "client": "consistency_footprint_matches_client_scope",
   807|     }
   808|     df["candidate_class"] = df["reuse_scope"].map(candidate_class_labels)
   809|     df["scope_gap"] = df.apply(
   810|         lambda r: f"reuse={r['reuse_scope']} > seeded={r['seeded_scope']}"
   811|         if r["routing_bucket"] == "promotion_candidates" else "",
   812|         axis=1,
   813|     )
   814| 
   815|     # ========================================================
   816|     # RANK (ordinal, not magnitude -- no bare "score" column anywhere)
   817|     # ========================================================
   818| 
   819|     candidates = df[df["routing_bucket"] == "promotion_candidates"].copy()
   820|     candidates["scope_gap_width"] = candidates["reuse_rank"] - candidates["seeded_rank"]
   821|     candidates = candidates.sort_values(
   822|         ["domain", "scope_gap_width", "n_clients_present", "n_projects_present", "files_used", "pattern_label"],
   823|         ascending=[True, False, False, False, False, True],
   824|     )
   825|     candidates["rank"] = candidates.groupby("domain").cumcount() + 1
   826| 
   827|     underused = df[df["routing_bucket"] == "governed_but_underused"].sort_values(
   828|         ["domain", "seeded_scope", "pattern_label"]
   829|     )
   830|     baseline = df[df["routing_bucket"] == "baseline_adequately_governed"].sort_values(
   831|         ["domain", "pattern_label"]
   832|     )
   833|     below_floor = df[df["routing_bucket"] == "below_reuse_floor"].sort_values(
   834|         ["domain", "pattern_label"]
   835|     )
   836|     unclassified_out = df[df["routing_bucket"] == "unclassified_reuse"].sort_values(
   837|         ["domain", "pattern_label"]
   838|     )
   839|     noise_out = df[df["routing_bucket"] == "semantic_noise_excluded"].sort_values(
   840|         ["domain", "pattern_label"]
   841|     )
   842| 
   843|     # ========================================================
   844|     # DOMAIN ROLLUP
   845|     # ========================================================
   846| 
   847|     domain_rollup = (
   848|         df.groupby("domain")
   849|         .agg(
   850|             # df already has exactly one row per (domain, join_hash,
   851|             # unit_system) by construction -- "count" (row count) is the
   852|             # correct total here, not nunique(join_hash), which would
   853|             # undercount whenever the same join_hash is split across
   854|             # imperial/metric unit_system pools with different routing.
   855|             total_patterns=("join_hash", "count"),
   856|             candidates=("routing_bucket", lambda x: (x == "promotion_candidates").sum()),
   857|             governed_but_underused=("routing_bucket", lambda x: (x == "governed_but_underused").sum()),
   858|             baseline_adequately_governed=("routing_bucket", lambda x: (x == "baseline_adequately_governed").sum()),
   859|             below_reuse_floor=("routing_bucket", lambda x: (x == "below_reuse_floor").sum()),
   860|             unclassified_reuse=("routing_bucket", lambda x: (x == "unclassified_reuse").sum()),
   861|             semantic_noise_excluded=("routing_bucket", lambda x: (x == "semantic_noise_excluded").sum()),
   862|             avg_project_penetration=("project_penetration", "mean"),
   863|             max_project_penetration=("project_penetration", "max"),
   864|         )
   865|         .reset_index()
   866|         .sort_values(["candidates", "governed_but_underused"], ascending=[False, False])
   867|     )
   868| 
   869|     # ========================================================
   870|     # EXPORTS
   871|     # ========================================================
   872| 
   873|     audit_cols = [
   874|         "domain", "join_hash", "unit_system", "pattern_label", "routing_bucket", "candidate_class",
   875|         "scope_gap", "seeded_scope", "reuse_scope", "seeded_via_comparison_types",
   876|         "reuse_bucket", "reuse_view_source", "client_label", "n_clients_present", "n_clients_denominator",
   877|         "pct_clients_present", "n_projects_present", "n_projects_denominator",
   878|         "pct_projects_present", "n_files_present", "n_files_denominator",
   879|         "pct_files_present", "files_used", "max_pct_used", "project_penetration",
   880|         "is_baseline_infrastructure", "any_template", "any_container", "any_generic",
   881|         "enterprise_evidence_downgraded", "reuse_client_pool_is_enterprise",
   882|         "seeded_scope_consistency_flag", "reuse_data_unclassified", "semantic_noise",
   883|     ]
   884|     audit_cols = [c for c in audit_cols if c in df.columns]
   885| 
   886|     candidate_export_cols = ["rank"] + audit_cols
   887|     apply_export_cap(candidates, export_top, "candidate_class")[candidate_export_cols].to_csv(
   888|         out_dir / "promotion_candidates.csv", index=False
   889|     )
   890|     underused[audit_cols].to_csv(out_dir / "governed_but_underused.csv", index=False)
   891|     baseline[audit_cols].to_csv(out_dir / "baseline_adequately_governed.csv", index=False)
   892|     below_floor[audit_cols].to_csv(out_dir / "below_reuse_floor.csv", index=False)
   893|     unclassified_out[audit_cols].to_csv(out_dir / "unclassified_reuse.csv", index=False)
   894|     if enable_semantic_noise_filter:
   895|         noise_out[audit_cols].to_csv(out_dir / "semantic_noise_excluded.csv", index=False)
   896|     domain_rollup.to_csv(out_dir / "domain_rollup.csv", index=False)
   897|     df[audit_cols].sort_values(["domain", "routing_bucket", "pattern_label"]).to_csv(
   898|         out_dir / "promotion_candidate_full_audit.csv", index=False
   899|     )
   900| 
   901|     # ========================================================
   902|     # SUMMARY MARKDOWN
   903|     # ========================================================
   904| 
   905|     summary = []
   906|     summary.append("# Scope-Consistency Analysis Summary")
   907|     summary.append("")
   908|     summary.append(
   909|         "**Read this first:** every classification below describes where a "
   910|         "pattern's observed reuse footprint sits relative to where it is "
   911|         "already governed. None of it is a promotion decision, an approval, "
   912|         "or a recommendation to act -- `candidate_class` names a consistency "
   913|         "footprint, not a verdict. Treat this as a lead list for governance "
   914|         "review, not a queue to execute against."
   915|     )
   916|     summary.append("")
   917| 
   918|     summary.append("## Run Configuration")
   919|     summary.append("")
   920|     summary.append(f"- Root: `{root}`")
   921|     summary.append(f"- Output: `{out_dir}`")
   922|     summary.append(f"- Domains: `{', '.join(sorted(priority_domains))}`")
   923|     summary.append(f"- Baseline threshold: `{baseline_threshold}`")
   924|     summary.append(f"- Minimum enterprise clients: `{min_enterprise_clients}`")
   925|     summary.append(f"- Semantic noise filter enabled: `{enable_semantic_noise_filter}`")
   926|     summary.append(f"- Export top per class: `{export_top}`")
   927|     summary.append("")
   928| 
   929|     summary.append("## What the numbers show")
   930|     summary.append("")
   931|     n_candidates = len(candidates)
   932|     n_underused = len(underused)
   933|     n_baseline = len(baseline)
   934|     if n_candidates:
   935|         top_domains = (
   936|             candidates.groupby("domain").size().sort_values(ascending=False).head(5)
   937|         )
   938|         summary.append(
   939|             f"**{n_candidates} patterns are used more broadly than they are governed.** "
   940|             f"The largest concentrations are in "
   941|             f"{', '.join(f'{d} ({int(n)})' for d, n in top_domains.items())}. "
   942|             "Each row in `promotion_candidates.csv` names the exact scope gap "
   943|             "(`scope_gap`, e.g. `reuse=enterprise > seeded=client`) rather than "
   944|             "a single opaque label."
   945|         )
   946|     else:
   947|         summary.append("**No patterns showed reuse exceeding their governed scope under current thresholds.**")
   948|     summary.append("")
   949| 
   950|     if n_underused:
   951|         summary.append(
   952|             f"**{n_underused} patterns are governed more broadly than they are actually reused** "
   953|             "(`governed_but_underused.csv`). This is an adoption/underuse question, not a "
   954|             "promotion question, and is out of scope for this tool's candidate logic -- "
   955|             "treat it as a lead for the archetype/adoption work. Note: `reuse_scope` can "
   956|             "never resolve to `bc` (see module docstring), so some rows here may reflect "
   957|             "that measurement gap rather than genuinely low bc-level reuse."
   958|         )
   959|         summary.append("")
   960| 
   961|     summary.append(
   962|         f"**{n_baseline} patterns are adequately governed** -- reuse scope does not exceed "
   963|         "the broadest scope at which they are already seeded, or they cleared the "
   964|         "project-penetration + already-seeded baseline gate directly."
   965|     )
   966|     summary.append("")
   967| 
   968|     consistency_flags = int(df["seeded_scope_consistency_flag"].sum())
   969|     if consistency_flags:
   970|         summary.append(
   971|             f"**{consistency_flags} rows have `seeded_scope_consistency_flag=True`** -- "
   972|             "`in_any_template`/`in_any_container` saw governance evidence that the "
   973|             "`comparison_type`-based scope recovery in this tool did not map to a scope "
   974|             "level. Worth a manual spot-check before trusting `seeded_scope=ungoverned` "
   975|             "on those specific rows."
   976|         )
   977|         summary.append("")
   978| 
   979|     summary.append("## Domain Rollup")
   980|     summary.append("")
   981|     for _, r in domain_rollup.iterrows():
   982|         summary.append(
   983|             f"- **{r['domain']}**: {int(r['candidates'])} candidates, "
   984|             f"{int(r['governed_but_underused'])} governed-but-underused, "
   985|             f"{int(r['baseline_adequately_governed'])} baseline, "
   986|             f"{int(r['total_patterns'])} total patterns analyzed."
   987|         )
   988|     summary.append("")
   989| 
   990|     summary.append("## Top Candidates by Domain")
   991|     summary.append("")
   992|     if n_candidates == 0:
   993|         summary.append("- None found under current thresholds.")
   994|     else:
   995|         for dom in sorted(candidates["domain"].unique()):
   996|             summary.append(f"### {dom}")
   997|             summary.append("")
   998|             for _, r in candidates[candidates["domain"] == dom].head(10).iterrows():
   999|                 summary.append(
  1000|                     f"{int(r['rank'])}. {r['pattern_label']} -- {r['scope_gap']} "
  1001|                     f"(clients={int(r['n_clients_present'])}, "
  1002|                     f"projects={int(r['n_projects_present'])}, "
  1003|                     f"files_used={int(r['files_used'])})"
  1004|                 )
  1005|             summary.append("")
  1006| 
  1007|     summary.append("## Notes")
  1008|     summary.append("")
  1009|     summary.append(
  1010|         "- `rank` is ordinal position within a domain's candidate list, not a magnitude "
  1011|         "score. No composite numeric score is exposed anywhere in this tool's output."
  1012|     )
  1013|     summary.append(
  1014|         "- `baseline_adequately_governed` generalizes the prior `is_baseline_infrastructure` "
  1015|         "/ `object_style_baseline` special cases into one rule: adequately governed means "
  1016|         "reuse scope does not exceed seeded scope, or high project penetration plus being "
  1017|         "seeded somewhere. A universal-but-unseeded pattern is no longer excluded as "
  1018|         "baseline -- it becomes a candidate, which is the intended behavior change."
  1019|     )
  1020|     summary.append(
  1021|         "- `governed_but_underused` and `unclassified_reuse` are fully separate outputs, "
  1022|         "never merged into `promotion_candidates.csv` or `baseline_adequately_governed.csv`."
  1023|     )
  1024|     summary.append(
  1025|         "- `reuse_scope` prefers used-view (active-delivery-practice) reuse rows over "
  1026|         "all-view (configured) ones; `reuse_view_source` records which was actually used "
  1027|         "per row (`used` vs `all_fallback`, the latter only when no used-view row exists "
  1028|         "for that population at all)."
  1029|     )
  1030| 
  1031|     (out_dir / "promotion_candidate_summary.md").write_text(
  1032|         "\n".join(summary), encoding="utf-8"
  1033|     )
  1034|     write_enterprise_policy_provenance(out_dir, policy)
  1035| 
  1036|     # ========================================================
  1037|     # CONSOLE SUMMARY
  1038|     # ========================================================
  1039| 
  1040|     print("")
  1041|     print("Analysis complete")
  1042|     print(f"Output folder: {out_dir}")
  1043|     print("")
  1044|     print(f"Candidates:              {n_candidates:,}")
  1045|     print(f"Governed but underused:  {n_underused:,}")
  1046|     print(f"Baseline (adequate):     {n_baseline:,}")
  1047|     print(f"Below reuse floor:       {len(below_floor):,}")
  1048|     print(f"Unclassified reuse data: {len(unclassified_out):,}")
  1049|     if enable_semantic_noise_filter:
  1050|         print(f"Semantic noise excluded: {len(noise_out):,}")
  1051|     print("")
  1052|     print("Exports:")
  1053|     print(f"- {out_dir / 'promotion_candidates.csv'}")
  1054|     print(f"- {out_dir / 'governed_but_underused.csv'}")
  1055|     print(f"- {out_dir / 'baseline_adequately_governed.csv'}")
  1056|     print(f"- {out_dir / 'below_reuse_floor.csv'}")
  1057|     print(f"- {out_dir / 'unclassified_reuse.csv'}")
  1058|     if enable_semantic_noise_filter:
  1059|         print(f"- {out_dir / 'semantic_noise_excluded.csv'}")
  1060|     print(f"- {out_dir / 'domain_rollup.csv'}")
  1061|     print(f"- {out_dir / 'promotion_candidate_full_audit.csv'}")
  1062|     print(f"- {out_dir / 'promotion_candidate_summary.md'}")
  1063| 
  1064| 
  1065| if __name__ == "__main__":
  1066|     main()
```
