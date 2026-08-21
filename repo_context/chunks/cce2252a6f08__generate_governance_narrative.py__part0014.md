# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 14 of 17
- Original line range: 5848-6247
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: main

```
  5848| def main():
  5849|     parser = argparse.ArgumentParser(description="Generate governance narrative from pipeline CSVs.")
  5850|     parser.add_argument("--summary", required=True, help="cross_segment_summary.csv")
  5851|     parser.add_argument("--pooled", required=True, help="cross_segment_pooled.csv")
  5852|     parser.add_argument("--segment-manifest",
  5853|                         help="segment_manifest.csv (optional). Lets the within_project "
  5854|                              "score_reliability p10/p90 capture resolve a redundant_single_child-"
  5855|                              "demoted enterprise-wide root segment to its population-identical "
  5856|                              "runnable descendant (see _resolve_runnable_segment() in "
  5857|                              "compare_cross_segment.py) instead of silently finding no "
  5858|                              "unscoped segment at all. Without this flag, behavior is "
  5859|                              "unchanged from before this option existed.")
  5860|     parser.add_argument("--governance-states", help="cross_segment_governance_states.csv (optional)")
  5861|     parser.add_argument("--governance-state-summary", help="cross_segment_governance_state_summary.csv (optional)")
  5862|     parser.add_argument("--delta", help="cross_segment_delta.csv (optional legacy fallback)")
  5863|     parser.add_argument("--file-meta", help="file_metadata.csv (optional)")
  5864|     parser.add_argument("--client-sector", default=str(_DEFAULT_CLIENT_SECTOR_PATH),
  5865|                         help="client_sector.csv (client_label,sector columns — classifies "
  5866|                              "cross-client convergence and non-comparable-sector tiering). "
  5867|                              f"Defaults to {_DEFAULT_CLIENT_SECTOR_PATH} if present, so existing "
  5868|                              "invocations keep today's healthcare cross-client convergence "
  5869|                              "signal without needing to pass this flag. Pass an explicit path "
  5870|                              "to override, or a nonexistent path to run with every client "
  5871|                              "unclassified.")
  5872|     parser.add_argument("--union-inventory",
  5873|                         help="cross_segment_union_inventory.csv (optional)")
  5874|     parser.add_argument("--comparison-registry",
  5875|                         help="comparison_registry.csv (optional). Enables a per-domain "
  5876|                              "Input Completeness / Staleness note near Analytical Notes: an "
  5877|                              "evidence/registry-mismatch proxy that flags a comparison "
  5878|                              "stamped in the registry but stale or missing relative to "
  5879|                              "cross_segment_summary.csv/governance-state evidence (D-032). "
  5880|                              "Cannot detect a comparison absent from ALL evidence sources "
  5881|                              "(never run, never registered, no state evidence either) -- "
  5882|                              "not a not-run-coverage guarantee. Never embedded/reproduced "
  5883|                              "in the output package -- only derived present/missing/stale "
  5884|                              "counts are.")
  5885|     parser.add_argument("--reuse-distribution",
  5886|                         help="pattern_reuse_distribution.csv (optional)")
  5887|     parser.add_argument("--matrix-manifest",
  5888|                         help="matrix_output_manifest.csv (optional). Also used as the "
  5889|                              "availability/limitations source for the Project Portfolio "
  5890|                              "section when the --project-* matrix flags below are supplied.")
  5891|     parser.add_argument("--reuse-by-client",
  5892|                         help="pattern_reuse_summary_by_client.csv (optional). Adds an "
  5893|                              "adoption-breadth (how many clients reach a pattern) cut "
  5894|                              "alongside the existing distinct-pattern reuse table in the "
  5895|                              "Union Inventory Reuse Summary section.")
  5896|     parser.add_argument("--project-union-jaccard-matrix",
  5897|                         help="project_union_jaccard_matrix.csv (optional). Feeds the "
  5898|                              "Project Portfolio section's footprint-identity paragraph.")
  5899|     parser.add_argument("--project-density-similarity-matrix",
  5900|                         help="project_density_similarity_matrix.csv (optional). Feeds the "
  5901|                              "Project Portfolio section's density-similarity paragraph.")
  5902|     parser.add_argument("--project-pool-containment-matrix",
  5903|                         help="project_pool_containment_similarity_matrix.csv (optional). "
  5904|                              "Feeds the Project Portfolio section's peer-pool-containment "
  5905|                              "paragraph.")
  5906|     parser.add_argument("--project-fragmentation-diagnostic",
  5907|                         help="project_fragmentation_diagnostic.csv (optional). Feeds the "
  5908|                              "Project Portfolio section's fragmentation-diagnostic paragraph "
  5909|                              "(also covers project_mean_file_pair_jaccard_matrix.csv's "
  5910|                              "signal via this file's own exact_identity_overlap column, "
  5911|                              "rather than consuming that matrix standalone).")
  5912|     parser.add_argument("--governance-bc-client-matrix",
  5913|                         help="governance_bc_client_matrix.csv (optional, from "
  5914|                              "tools/governance_relationships.py). Feeds the Business Center "
  5915|                              "Composition section.")
  5916|     parser.add_argument("--governance-client-bc-matrix",
  5917|                         help="governance_client_bc_matrix.csv (optional, from "
  5918|                              "tools/governance_relationships.py). Feeds the Business Center "
  5919|                              "Distribution section.")
  5920|     parser.add_argument("--policy-dir", default=str(_DEFAULT_POLICY_DIR),
  5921|                         help="Directory of externalized governance policy files: "
  5922|                              "governance_thresholds.json, domain_governance_policy.json, "
  5923|                              "client_onboarding_policy.json, finding_rules.json (see "
  5924|                              "docs/governance_evidence_package.md). Defaults to "
  5925|                              f"{_DEFAULT_POLICY_DIR}, so existing invocations keep today's "
  5926|                              "threshold/domain-policy values without needing to pass this "
  5927|                              "flag -- the shipped defaults there reproduce this generator's "
  5928|                              "pre-externalization Python literals exactly. A missing profile "
  5929|                              "file within the directory falls back to this generator's own "
  5930|                              "built-in default for that profile only (reported in "
  5931|                              "governance_package_health.json); pass a nonexistent path to run "
  5932|                              "with every profile at its built-in default.")
  5933|     parser.add_argument("--package-schema-version", default=PACKAGE_SCHEMA_VERSION,
  5934|                         help=f"Override the emitted package_schema_version "
  5935|                              f"(default {PACKAGE_SCHEMA_VERSION}).")
  5936|     parser.add_argument("--emit-evidence-package", dest="emit_evidence_package",
  5937|                         action="store_true",
  5938|                         help="Write governance_package_manifest.json / "
  5939|                              "governance_package_health.json / governance_evidence_map.json "
  5940|                              "alongside the existing CSV/MD outputs (default: on).")
  5941|     parser.add_argument("--no-emit-evidence-package", dest="emit_evidence_package",
  5942|                         action="store_false",
  5943|                         help="Suppress the evidence-package JSON outputs; existing CSV/MD "
  5944|                              "outputs are unaffected.")
  5945|     parser.set_defaults(emit_evidence_package=True)
  5946|     parser.add_argument("--emit-interpretation-layer", dest="emit_interpretation_layer",
  5947|                         action="store_true",
  5948|                         help="Write governance_brief.md (default: on) and render its "
  5949|                              "narrative-header pointer. Only takes effect when "
  5950|                              "--emit-evidence-package is also on, since the brief "
  5951|                              "is built from governance_findings.json/"
  5952|                              "governance_package_health.json. PR review finding: this "
  5953|                              "does NOT gate the interpretation-guide/question-routes/"
  5954|                              "reading-order evidence-map entries or their docs/governance/ "
  5955|                              "copy into --out -- those are controlled by --emit-evidence-"
  5956|                              "package alone and by each doc's own presence on disk.")
  5957|     parser.add_argument("--no-emit-interpretation-layer", dest="emit_interpretation_layer",
  5958|                         action="store_false",
  5959|                         help="Suppress governance_brief.md and its narrative-header "
  5960|                              "pointer only; governance_package_manifest.json/_health.json/"
  5961|                              "_evidence_map.json/governance_findings.json, and the "
  5962|                              "interpretation-guide/question-routes/reading-order evidence-"
  5963|                              "map entries and copies, are unaffected.")
  5964|     parser.set_defaults(emit_interpretation_layer=True)
  5965|     parser.add_argument("--out", default="governance_narrative_context.md")
  5966|     parser.add_argument("--enterprise-policy", help="Deployment-local enterprise policy JSON")
  5967|     parser.add_argument("--enterprise-label", help="Effective enterprise label override")
  5968|     parser.add_argument("--date", default=str(date.today()),
  5969|                         help="Analysis date string (default: today)")
  5970|     args = parser.parse_args()
  5971|     enterprise_policy = load_enterprise_policy(args.enterprise_policy, args.enterprise_label)
  5972| 
  5973|     policy_dir_arg = Path(args.policy_dir) if args.policy_dir else None
  5974|     governance_policy = load_governance_policy(policy_dir_arg, _POLICY_DEFAULTS)
  5975|     apply_governance_policy(governance_policy)
  5976|     _policy_files_used = sorted(
  5977|         name for name, status in governance_policy["load_status"].items()
  5978|         if status["source"] == "policy_file"
  5979|     )
  5980|     _policy_files_defaulted = sorted(
  5981|         name for name, status in governance_policy["load_status"].items()
  5982|         if status["source"] == "built_in_default"
  5983|     )
  5984|     if _policy_files_used:
  5985|         print(f"Loaded governance policy profile(s) from {args.policy_dir}: {_policy_files_used}")
  5986|     if _policy_files_defaulted:
  5987|         print(f"[info] Using built-in default for governance policy profile(s) not found "
  5988|               f"under {args.policy_dir}: {_policy_files_defaulted}", file=sys.stderr)
  5989| 
  5990|     print(f"Loading {args.summary}...")
  5991|     summary_rows = read_csv(Path(args.summary))
  5992|     print(f"Loading {args.pooled}...")
  5993|     pooled_rows = read_csv(Path(args.pooled))
  5994| 
  5995|     governance_state_rows = []
  5996|     if args.governance_states:
  5997|         print(f"Loading {args.governance_states}...")
  5998|         governance_state_rows = read_csv(Path(args.governance_states))
  5999| 
  6000|     governance_state_summary_rows = []
  6001|     if args.governance_state_summary:
  6002|         print(f"Loading {args.governance_state_summary}...")
  6003|         governance_state_summary_rows = read_csv(Path(args.governance_state_summary))
  6004| 
  6005|     delta_rows = []
  6006|     if args.delta:
  6007|         print(f"Loading {args.delta}...")
  6008|         delta_rows = read_csv(Path(args.delta))
  6009| 
  6010|     file_meta_rows = None
  6011|     if args.file_meta:
  6012|         print(f"Loading {args.file_meta}...")
  6013|         file_meta_rows = read_csv(Path(args.file_meta))
  6014| 
  6015|     client_sector_rows = []
  6016|     if args.client_sector and Path(args.client_sector).exists():
  6017|         print(f"Loading {args.client_sector}...")
  6018|         client_sector_rows = read_csv(Path(args.client_sector))
  6019|     elif args.client_sector:
  6020|         print(f"[warn] {args.client_sector} not found — every client will be treated as "
  6021|               f"unclassified (no sector). Pass --client-sector explicitly to silence this "
  6022|               f"if that's intended.", file=sys.stderr)
  6023| 
  6024|     union_inventory_rows = []
  6025|     if args.union_inventory:
  6026|         print(f"Loading {args.union_inventory}...")
  6027|         union_inventory_rows = read_csv(Path(args.union_inventory))
  6028| 
  6029|     comparison_registry_rows = []
  6030|     if args.comparison_registry:
  6031|         print(f"Loading {args.comparison_registry}...")
  6032|         comparison_registry_rows = read_csv(Path(args.comparison_registry))
  6033| 
  6034|     reuse_distribution_rows = []
  6035|     if args.reuse_distribution:
  6036|         print(f"Loading {args.reuse_distribution}...")
  6037|         reuse_distribution_rows = read_csv(Path(args.reuse_distribution))
  6038| 
  6039|     matrix_manifest_rows = []
  6040|     if args.matrix_manifest:
  6041|         print(f"Loading {args.matrix_manifest}...")
  6042|         matrix_manifest_rows = read_csv(Path(args.matrix_manifest))
  6043| 
  6044|     reuse_by_client_rows = []
  6045|     if args.reuse_by_client:
  6046|         print(f"Loading {args.reuse_by_client}...")
  6047|         reuse_by_client_rows = read_csv(Path(args.reuse_by_client))
  6048| 
  6049|     project_union_jaccard_rows = []
  6050|     if args.project_union_jaccard_matrix:
  6051|         print(f"Loading {args.project_union_jaccard_matrix}...")
  6052|         project_union_jaccard_rows = read_csv(Path(args.project_union_jaccard_matrix))
  6053| 
  6054|     project_density_similarity_rows = []
  6055|     if args.project_density_similarity_matrix:
  6056|         print(f"Loading {args.project_density_similarity_matrix}...")
  6057|         project_density_similarity_rows = read_csv(Path(args.project_density_similarity_matrix))
  6058| 
  6059|     project_pool_containment_rows = []
  6060|     if args.project_pool_containment_matrix:
  6061|         print(f"Loading {args.project_pool_containment_matrix}...")
  6062|         project_pool_containment_rows = read_csv(Path(args.project_pool_containment_matrix))
  6063| 
  6064|     project_fragmentation_rows = []
  6065|     if args.project_fragmentation_diagnostic:
  6066|         print(f"Loading {args.project_fragmentation_diagnostic}...")
  6067|         project_fragmentation_rows = read_csv(Path(args.project_fragmentation_diagnostic))
  6068| 
  6069|     governance_bc_client_rows = []
  6070|     if args.governance_bc_client_matrix:
  6071|         print(f"Loading {args.governance_bc_client_matrix}...")
  6072|         governance_bc_client_rows = read_csv(Path(args.governance_bc_client_matrix))
  6073| 
  6074|     governance_client_bc_rows = []
  6075|     if args.governance_client_bc_matrix:
  6076|         print(f"Loading {args.governance_client_bc_matrix}...")
  6077|         governance_client_bc_rows = read_csv(Path(args.governance_client_bc_matrix))
  6078| 
  6079|     sector_map = load_client_sectors(client_sector_rows)
  6080| 
  6081|     segment_manifest = None
  6082|     if args.segment_manifest:
  6083|         print(f"Loading {args.segment_manifest}...")
  6084|         segment_manifest = {row["segment_id"]: row for row in read_csv(Path(args.segment_manifest))}
  6085| 
  6086|     normalise_summary_schema(summary_rows)
  6087|     print("Computing cascade scores...")
  6088|     cascade = build_cascade(summary_rows, sector_map, segment_manifest)
  6089| 
  6090|     print("Computing corpus counts...")
  6091|     corpus = load_corpus_counts(summary_rows, file_meta_rows)
  6092| 
  6093|     print("Building client summary...")
  6094|     client_rows = build_client_summary(summary_rows, pooled_rows, sector_map)
  6095| 
  6096|     print("Building BC summary...")
  6097|     bc_rows = build_bc_summary(summary_rows, cascade)
  6098| 
  6099|     print("Building governance state summary...")
  6100|     governance_state_summary = build_governance_state_summary(
  6101|         governance_state_rows, governance_state_summary_rows
  6102|     )
  6103| 
  6104|     union_breadth_by_domain = build_union_breadth_by_domain(union_inventory_rows)
  6105| 
  6106|     comparison_completeness = None
  6107|     if args.comparison_registry:
  6108|         comparison_completeness = build_comparison_completeness(
  6109|             summary_rows, comparison_registry_rows,
  6110|             governance_state_rows, governance_state_summary_rows,
  6111|         )
  6112| 
  6113|     delta_summary = {}
  6114|     if delta_rows:
  6115|         print("Summarising legacy delta patterns...")
  6116|         delta_summary = load_delta_summary(delta_rows)
  6117| 
  6118|     # ── Evidence-package signal computation ────────────────────────────────────
  6119|     # Pure, read-only re-derivations from data already loaded above -- none of
  6120|     # this touches summary_rows/pooled_rows/cascade/client_rows or any existing
  6121|     # CSV/MD output. used_view_falls_back_to_legacy() is hoisted into a single
  6122|     # local var and reused below in place of the two independent calls the
  6123|     # narrative render functions used to make.
  6124|     legacy_fallback = used_view_falls_back_to_legacy()
  6125|     schema_detection = detect_bundle_schema(summary_rows)
  6126| 
  6127|     _cascade_known_types = (
  6128|         CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | CASCADE_GROUP3B_TYPES
  6129|         | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
  6130|     )
  6131|     cascade_coverage = _comparison_type_coverage(
  6132|         {r.get("comparison_type", "") for r in summary_rows},
  6133|         _cascade_known_types,
  6134|         intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()),
  6135|     )
  6136|     gov_state_coverage = _comparison_type_coverage(
  6137|         {r.get("comparison_type", "").strip() for r in governance_state_summary_rows}
  6138|         | {r.get("comparison_type", "").strip() for r in governance_state_rows},
  6139|         _DIRECTED_GOVERNANCE_TYPES,
  6140|     )
  6141| 
  6142|     _client_sector_path_str = args.client_sector or ""
  6143|     _client_sector_is_default = _client_sector_path_str == str(_DEFAULT_CLIENT_SECTOR_PATH)
  6144|     _client_sector_exists = bool(_client_sector_path_str) and Path(_client_sector_path_str).exists()
  6145|     if _client_sector_exists:
  6146|         client_sector_status = "default_path_resolved" if _client_sector_is_default else "explicit_path"
  6147|     else:
  6148|         client_sector_status = "default_path_missing" if _client_sector_is_default else "explicit_path_missing"
  6149| 
  6150|     unit_systems_seen = sorted({r.get("unit_system", "") for r in summary_rows if r.get("unit_system")})
  6151|     matrix_names_seen = sorted({
  6152|         r.get("matrix_name", "") for r in matrix_manifest_rows if r.get("matrix_name")
  6153|     })
  6154| 
  6155|     print("Building structured findings...")
  6156|     findings = build_structured_findings(cascade, client_rows, governance_state_summary)
  6157| 
  6158|     # ── Resolve output paths ───────────────────────────────────────────────────
  6159|     # If --out is a directory (or has no .md suffix), treat it as the output
  6160|     # directory and write governance_narrative_context.md inside it.
  6161|     out_path = Path(args.out)
  6162|     if out_path.is_dir() or out_path.suffix.lower() != ".md":
  6163|         out_dir = out_path if out_path.suffix == "" else out_path.parent
  6164|         out_path = out_dir / "governance_narrative_context.md"
  6165|     else:
  6166|         out_dir = out_path.parent
  6167|     out_dir.mkdir(parents=True, exist_ok=True)
  6168| 
  6169|     # ── Emit governance_domain_summary.csv ────────────────────────────────────
  6170|     print("Writing domain summary CSV...")
  6171|     domain_csv_path = out_dir / "governance_domain_summary.csv"
  6172| 
  6173|     domain_csv_rows = []
  6174|     for dom, d in sorted(cascade.items()):
  6175|         if not _has_renderable_cascade_signal(d):
  6176|             # Scope-only domain (Group 3 fan-out data only) -- captured in
  6177|             # `cascade` but not yet tiered/rendered. See CASCADE_GROUP3_TYPES.
  6178|             continue
  6179|         tier = assign_tier(d, governance_state_summary.get(dom))
  6180|         reliability = score_reliability(d)
  6181|         union_breadth = union_breadth_by_domain.get(dom)
  6182|         anomalies = detect_anomalies(dom, d, governance_state_summary.get(dom), union_breadth)
  6183|         domain_csv_rows.append({
  6184|             "domain": dom,
  6185|             "domain_label": DOMAIN_LABELS.get(dom, dom),
  6186|             "governance_tier": tier,
  6187|             "score_reliability": reliability,
  6188|             # Cascade-computed generic->template/container/project (Group 2), sourced
  6189|             # from the always-required cross_segment_summary.csv -- distinct from the
  6190|             # optional-governance-state-summary-sourced "generic_to_template"/etc.
  6191|             # columns below, which are blank when --governance-state-summary isn't
  6192|             # supplied. These cascade columns are populated regardless.
  6193|             "cascade_generic_to_template": fmt(d.get("gt")),
  6194|             "cascade_generic_to_container": fmt(d.get("gc")),
  6195|             "cascade_generic_to_project": fmt(d.get("gp")),
  6196|             "template_to_container": fmt(d["tc"]),
  6197|             "container_to_project": fmt(d["cp"]),
  6198|             # Rollup-gap fix: populated only when "container_to_project" (the
  6199|             # enterprise::enterprise slice) is None -- the largest data_sufficient
  6200|             # scoped bucket (see cp_by_scope_suff), plus which scope_pair it came
  6201|             # from so this is never mistaken for enterprise-level evidence.
  6202|             "container_to_project_scoped": fmt(d.get("cp_scoped")),
  6203|             "container_to_project_scoped_pair": d.get("cp_scoped_pair") or "",
  6204|             "template_to_project": fmt(d["tp"]),
  6205|             "cross_client_convergence": fmt(d["xc"]),
  6206|             "cross_client_convergence_all_view": fmt(d.get("xc_all")),
  6207|             "within_project_all": fmt(d["wp_all"]),
  6208|             "within_project_p10": fmt(d["wp_p10"]),
  6209|             "within_project_p90": fmt(d["wp_p90"]),
  6210|             "within_project_reliability_source": d.get("wp_p10_source", "none"),
  6211|             "within_project_spread": fmt(
  6212|                 (d["wp_p90"] - d["wp_p10"])
  6213|                 if d["wp_p10"] is not None and d["wp_p90"] is not None
  6214|                 else None
  6215|             ),
  6216|             "within_project_architectural": fmt(d["wp_disc"].get("architectural")),
  6217|             "within_project_mechanical_plumbing": fmt(d["wp_disc"].get("mechanical_plumbing")),
  6218|             "within_project_electrical": fmt(d["wp_disc"].get("electrical")),
  6219|             "within_project_structural": fmt(d["wp_disc"].get("structural")),
  6220|             "bundle_schema": d.get("bundle_schema", "none"),
  6221|             "template_to_project_used": fmt(d.get("tp_used")),
  6222|             "bundle_share_all": fmt(d.get("bundle_share_all")),
  6223|             "bundle_share_used": fmt(d.get("bundle_share_used")),
  6224|             "passive_inheritance_indicator": fmt(d.get("passive_indicator")),
  6225|             "passive_indicator_method": d.get("passive_indicator_method", "none"),
  6226|             "passive_inheritance_risk": "yes" if dom in PASSIVE_INHERITANCE_RISK_DOMAINS else "no",
  6227|             **{
  6228|                 "generic_to_template": fmt(governance_state_summary.get(dom, {}).get("generic_to_template")),
  6229|                 "generic_to_container": fmt(governance_state_summary.get(dom, {}).get("generic_to_container")),
  6230|                 "generic_to_project": fmt(governance_state_summary.get(dom, {}).get("generic_to_project")),
  6231|                 "provided_to_configured_containment": fmt(governance_state_summary.get(dom, {}).get("provided_to_configured_containment")),
  6232|                 "provided_to_used_containment": fmt(governance_state_summary.get(dom, {}).get("provided_to_used_containment")),
  6233|                 "provided_passive_share": fmt(governance_state_summary.get(dom, {}).get("provided_passive_share")),
  6234|                 "provided_missing_share": fmt(governance_state_summary.get(dom, {}).get("provided_missing_share")),
  6235|                 "local_active_share": fmt(governance_state_summary.get(dom, {}).get("local_active_share")),
  6236|                 "provided_and_used_count": governance_state_summary.get(dom, {}).get("provided_and_used_count", ""),
  6237|                 "provided_but_passive_count": governance_state_summary.get(dom, {}).get("provided_but_passive_count", ""),
  6238|                 "provided_but_missing_count": governance_state_summary.get(dom, {}).get("provided_but_missing_count", ""),
  6239|                 "local_active_count": governance_state_summary.get(dom, {}).get("local_active_count", ""),
  6240|                 "local_passive_count": governance_state_summary.get(dom, {}).get("local_passive_count", ""),
  6241|                 "local_unbundled_count": governance_state_summary.get(dom, {}).get("local_unbundled_count", ""),
  6242|                 "primary_governance_read": governance_state_summary.get(dom, {}).get("primary_governance_read", ""),
  6243|             },
  6244|             # Union-inventory-derived reuse-breadth pattern counts (D-033) --
  6245|             # additive only; blank when --union-inventory wasn't supplied or
  6246|             # the domain has no Project/all-view rows there. See
  6247|             # build_union_breadth_by_domain()'s own docstring for the
```
