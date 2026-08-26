# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 15 of 17
- Original line range: 6248-6647
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: main
- Ends inside symbol: main

```
  6248|             # corpus_wide > client_wide > project_wide > file_level tier
  6249|             # classification these counts come from.
  6250|             "union_reuse_patterns_total": union_breadth.get("total", "") if union_breadth else "",
  6251|             "union_reuse_patterns_corpus_wide": union_breadth.get("corpus_wide", "") if union_breadth else "",
  6252|             "union_reuse_patterns_client_wide": union_breadth.get("client_wide", "") if union_breadth else "",
  6253|             "union_reuse_patterns_project_wide": union_breadth.get("project_wide", "") if union_breadth else "",
  6254|             "union_reuse_patterns_file_level": union_breadth.get("file_level", "") if union_breadth else "",
  6255|             "notable_anomalies": " | ".join(anomalies) if anomalies else "",
  6256|         })
  6257| 
  6258|     tier_order_key = lambda r: (TIER_ORDER.get(r["governance_tier"], 10), r["template_to_project"])
  6259|     domain_csv_rows.sort(key=tier_order_key)
  6260| 
  6261|     with open(domain_csv_path, "w", newline="", encoding="utf-8") as f:
  6262|         if domain_csv_rows:
  6263|             w = csv.DictWriter(f, fieldnames=list(domain_csv_rows[0].keys()))
  6264|             w.writeheader()
  6265|             w.writerows(domain_csv_rows)
  6266|     print(f"  → {domain_csv_path} ({len(domain_csv_rows)} rows)")
  6267| 
  6268|     # ── Emit governance_client_summary.csv ────────────────────────────────────
  6269|     print("Writing client summary CSV...")
  6270|     client_csv_path = out_dir / "governance_client_summary.csv"
  6271|     client_csv_rows = []
  6272|     for r in client_rows:
  6273|         strongest_str = "; ".join(
  6274|             f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["strongest"]
  6275|         )
  6276|         weakest_str = "; ".join(
  6277|             f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["weakest"]
  6278|         )
  6279|         onboarding = _client_onboarding_profile(r)
  6280|         client_csv_rows.append({
  6281|             "client": r["client"],
  6282|             "n_project_files": r["n_files"],
  6283|             "alignment_tier": r["tier"],
  6284|             "cross_client_similarity_mean": fmt(r["xc_mean"]),
  6285|             "cross_client_similarity_mean_all_view": fmt(r.get("xc_mean_all")),
  6286|             "within_project_coherence": fmt(r["wp_mean"]),
  6287|             "within_project_coherence_all_view": fmt(r.get("wp_mean_all")),
  6288|             "confidence_note": r["confidence_note"],
  6289|             "most_aligned_domains": strongest_str,
  6290|             "least_aligned_domains": weakest_str,
  6291|             "onboarding_internal_read": onboarding["internal_read"],
  6292|             "onboarding_portability_read": onboarding["portability_read"],
  6293|             "onboarding_common_base": onboarding["common_base"],
  6294|             "onboarding_variant_burden": onboarding["variant_burden"],
  6295|             "onboarding_operating_implication": onboarding["operating_implication"],
  6296|         })
  6297|     with open(client_csv_path, "w", newline="", encoding="utf-8") as f:
  6298|         if client_csv_rows:
  6299|             w = csv.DictWriter(f, fieldnames=list(client_csv_rows[0].keys()))
  6300|             w.writeheader()
  6301|             w.writerows(client_csv_rows)
  6302|     print(f"  → {client_csv_path} ({len(client_csv_rows)} rows)")
  6303| 
  6304|     # ── Emit governance_bc_summary.csv ──────────────────────────────────────
  6305|     # Separate file, not merged into governance_client_summary.csv -- no
  6306|     # shared entity_type column. Enterprise is not a row here (see
  6307|     # render_enterprise_section()).
  6308|     print("Writing BC summary CSV...")
  6309|     bc_csv_path = out_dir / "governance_bc_summary.csv"
  6310|     # Fixed field list (unlike governance_domain_summary.csv/governance_client_
  6311|     # summary.csv, which derive fieldnames from row[0].keys() and therefore
  6312|     # write a 0-byte, headerless file when their row list is empty) -- a
  6313|     # client-only corpus with zero bc_to_bc/enterprise_to_bc/bc_to_project
  6314|     # evidence is a realistic, not just theoretical, empty case for THIS
  6315|     # summary (Codex review finding on this PR), so the header must still be
  6316|     # recoverable by downstream CSV readers/the evidence map even with zero
  6317|     # BC rows.
  6318|     bc_csv_fields = [
  6319|         "business_center", "n_template_container_files", "alignment_tier",
  6320|         "cross_bc_similarity_mean", "cross_bc_similarity_mean_used_view",
  6321|         "internal_template_to_container_coherence",
  6322|         "internal_template_to_container_coherence_used_view",
  6323|         "enterprise_reach", "enterprise_reach_used_view",
  6324|         "confidence_note", "most_aligned_domains", "least_aligned_domains",
  6325|     ]
  6326|     bc_csv_rows = []
  6327|     for r in bc_rows:
  6328|         strongest_str = "; ".join(
  6329|             f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["strongest"]
  6330|         )
  6331|         weakest_str = "; ".join(
  6332|             f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["weakest"]
  6333|         )
  6334|         bc_csv_rows.append({
  6335|             "business_center": r["bc"],
  6336|             "n_template_container_files": r["n_files"],
  6337|             "alignment_tier": r["tier"],
  6338|             "cross_bc_similarity_mean": fmt(r["bb_mean"]),
  6339|             "cross_bc_similarity_mean_used_view": fmt(r.get("bb_mean_used")),
  6340|             "internal_template_to_container_coherence": fmt(r["tc_bc_mean"]),
  6341|             "internal_template_to_container_coherence_used_view": fmt(r.get("tc_bc_mean_used")),
  6342|             "enterprise_reach": fmt(r["eb_bc_mean"]),
  6343|             "enterprise_reach_used_view": fmt(r.get("eb_bc_mean_used")),
  6344|             "confidence_note": r["confidence_note"],
  6345|             "most_aligned_domains": strongest_str,
  6346|             "least_aligned_domains": weakest_str,
  6347|         })
  6348|     with open(bc_csv_path, "w", newline="", encoding="utf-8") as f:
  6349|         w = csv.DictWriter(f, fieldnames=bc_csv_fields)
  6350|         w.writeheader()
  6351|         w.writerows(bc_csv_rows)
  6352|     print(f"  → {bc_csv_path} ({len(bc_csv_rows)} rows)")
  6353| 
  6354|     # ── Render and write narrative MD ─────────────────────────────────────────
  6355|     print("Rendering narrative...")
  6356|     sections = [
  6357|         render_header(args.date, corpus, bool(governance_state_summary), legacy_fallback,
  6358|                       bool(args.emit_evidence_package and INTERPRETATION_GUIDE_PATH.exists())),
  6359|         render_evidence_authority_header(args.package_schema_version, GENERATOR_IDENTITY, args.emit_evidence_package, args.emit_interpretation_layer),
  6360|         render_governance_state_model(bool(governance_state_summary)),
  6361|         render_domain_tiers(cascade, governance_state_summary, union_breadth_by_domain),
  6362|     ]
  6363|     generic_scope_section = render_generic_baseline_scope_section(cascade)
  6364|     if generic_scope_section:
  6365|         sections.append(generic_scope_section)
  6366|     group1_scope_section = render_group1_scope_section(cascade)
  6367|     if group1_scope_section:
  6368|         sections.append(group1_scope_section)
  6369|     sections += [
  6370|         render_discipline_section(cascade, summary_rows),
  6371|         render_client_section(client_rows),
  6372|         render_onboarding_section(client_rows),
  6373|     ]
  6374|     enterprise_section = render_enterprise_section(cascade)
  6375|     if enterprise_section:
  6376|         sections.append(enterprise_section)
  6377|     sections.append(render_bc_section(bc_rows))
  6378|     if governance_state_summary:
  6379|         sections.append(render_governance_state_section(governance_state_summary))
  6380|     elif delta_summary:
  6381|         sections.append(render_delta_section(delta_summary))
  6382|     union_reuse_section = render_union_reuse_summary(
  6383|         union_inventory_rows, reuse_distribution_rows, matrix_manifest_rows, reuse_by_client_rows
  6384|     )
  6385|     if union_reuse_section:
  6386|         sections.append(union_reuse_section)
  6387|     portfolio_section = render_project_portfolio_section(
  6388|         project_union_jaccard_rows, project_density_similarity_rows,
  6389|         project_pool_containment_rows, project_fragmentation_rows, matrix_manifest_rows,
  6390|     )
  6391|     if portfolio_section:
  6392|         sections.append(portfolio_section)
  6393|     bc_composition_section = render_bc_composition_section(governance_bc_client_rows)
  6394|     if bc_composition_section:
  6395|         sections.append(bc_composition_section)
  6396|     client_bc_distribution_section = render_client_bc_distribution_section(
  6397|         governance_client_bc_rows, governance_bc_client_rows
  6398|     )
  6399|     if client_bc_distribution_section:
  6400|         sections.append(client_bc_distribution_section)
  6401|     sections += [
  6402|         render_findings_and_recommendations(cascade, client_rows, governance_state_summary, findings),
  6403|         render_limitations(corpus, legacy_fallback, bool(governance_state_summary), comparison_completeness),
  6404|     ]
  6405| 
  6406|     output = "\n\n".join(sections)
  6407|     out_path.write_text(output, encoding="utf-8")
  6408|     write_enterprise_policy_provenance(out_dir, enterprise_policy)
  6409|     print(f"\nWrote {out_path} ({len(output):,} chars, {len(output.splitlines())} lines)")
  6410| 
  6411|     # ── Evidence-package JSON outputs ───────────────────────────────────────────
  6412|     # Written last, after all three existing outputs are already safely on disk,
  6413|     # so a failure here never blocks or corrupts the existing deliverables. See
  6414|     # docs/governance_evidence_package.md.
  6415|     if args.emit_evidence_package:
  6416|         print("Writing evidence package (manifest/health/evidence_map)...")
  6417| 
  6418|         input_paths = {
  6419|             "cross_segment_summary": Path(args.summary),
  6420|             "cross_segment_pooled": Path(args.pooled),
  6421|             "cross_segment_governance_states": Path(args.governance_states) if args.governance_states else None,
  6422|             "cross_segment_governance_state_summary": Path(args.governance_state_summary) if args.governance_state_summary else None,
  6423|             "cross_segment_delta": Path(args.delta) if args.delta else None,
  6424|             "file_metadata": Path(args.file_meta) if args.file_meta else None,
  6425|             "client_sector": Path(_client_sector_path_str) if _client_sector_path_str else None,
  6426|             "cross_segment_union_inventory": Path(args.union_inventory) if args.union_inventory else None,
  6427|             "pattern_reuse_distribution": Path(args.reuse_distribution) if args.reuse_distribution else None,
  6428|             "matrix_output_manifest": Path(args.matrix_manifest) if args.matrix_manifest else None,
  6429|             "pattern_reuse_summary_by_client": Path(args.reuse_by_client) if args.reuse_by_client else None,
  6430|             "project_union_jaccard_matrix": Path(args.project_union_jaccard_matrix) if args.project_union_jaccard_matrix else None,
  6431|             "project_density_similarity_matrix": Path(args.project_density_similarity_matrix) if args.project_density_similarity_matrix else None,
  6432|             "project_pool_containment_similarity_matrix": Path(args.project_pool_containment_matrix) if args.project_pool_containment_matrix else None,
  6433|             "project_fragmentation_diagnostic": Path(args.project_fragmentation_diagnostic) if args.project_fragmentation_diagnostic else None,
  6434|             "segment_manifest": Path(args.segment_manifest) if args.segment_manifest else None,
  6435|             "governance_bc_client_matrix": Path(args.governance_bc_client_matrix) if args.governance_bc_client_matrix else None,
  6436|             "governance_client_bc_matrix": Path(args.governance_client_bc_matrix) if args.governance_client_bc_matrix else None,
  6437|             # Explicit override, else the same auto-detected-beside-summary
  6438|             # default sibling_paths["comparison_registry"] uses below (D-032)
  6439|             # -- one resolved path, tracked both as an input (this dict, for
  6440|             # governance_package_health.json's required/optional-input
  6441|             # "present" signal) and as an evidence-map artifact (sibling_paths,
  6442|             # for governance_evidence_map.json navigation), so the two never
  6443|             # disagree about which file "comparison_registry" means.
  6444|             "comparison_registry": (
  6445|                 Path(args.comparison_registry) if args.comparison_registry
  6446|                 else Path(args.summary).parent / "comparison_registry.csv"
  6447|             ),
  6448|         }
  6449|         input_required = {"cross_segment_summary": True, "cross_segment_pooled": True}
  6450|         input_roles = {
  6451|             "cross_segment_summary": "authoritative_deterministic_evidence",
  6452|             "cross_segment_pooled": "authoritative_deterministic_evidence",
  6453|             "cross_segment_governance_states": "authoritative_deterministic_evidence",
  6454|             "cross_segment_governance_state_summary": "authoritative_deterministic_evidence",
  6455|             "cross_segment_delta": "authoritative_deterministic_evidence",
  6456|             "file_metadata": "authoritative_deterministic_evidence",
  6457|             "client_sector": "user_provided_note",
  6458|             "cross_segment_union_inventory": "authoritative_deterministic_evidence",
  6459|             "pattern_reuse_distribution": "authoritative_deterministic_evidence",
  6460|             "matrix_output_manifest": "convenience_summary",
  6461|             "pattern_reuse_summary_by_client": "authoritative_deterministic_evidence",
  6462|             "project_union_jaccard_matrix": "authoritative_deterministic_evidence",
  6463|             "project_density_similarity_matrix": "authoritative_deterministic_evidence",
  6464|             "project_pool_containment_similarity_matrix": "authoritative_deterministic_evidence",
  6465|             "project_fragmentation_diagnostic": "authoritative_deterministic_evidence",
  6466|             "segment_manifest": "authoritative_deterministic_evidence",
  6467|             "governance_bc_client_matrix": "authoritative_deterministic_evidence",
  6468|             "governance_client_bc_matrix": "authoritative_deterministic_evidence",
  6469|             "comparison_registry": "authoritative_deterministic_evidence",
  6470|         }
  6471|         input_present = {k: bool(v) and v.exists() for k, v in input_paths.items()}
  6472| 
  6473|         output_paths = {
  6474|             "governance_domain_summary": domain_csv_path,
  6475|             "governance_client_summary": client_csv_path,
  6476|             "governance_bc_summary": bc_csv_path,
  6477|             "governance_narrative_context": out_path,
  6478|             "governance_package_manifest": out_dir / "governance_package_manifest.json",
  6479|             "governance_package_health": out_dir / "governance_package_health.json",
  6480|             "governance_evidence_map": out_dir / "governance_evidence_map.json",
  6481|             "governance_findings": out_dir / "governance_findings.json",
  6482|             "governance_file_inventory": out_dir / "governance_file_inventory.json",
  6483|         }
  6484|         output_types = {
  6485|             "governance_domain_summary": "csv", "governance_client_summary": "csv",
  6486|             "governance_bc_summary": "csv", "governance_narrative_context": "markdown",
  6487|             "governance_package_manifest": "json", "governance_package_health": "json", "governance_evidence_map": "json",
  6488|             "governance_findings": "json", "governance_file_inventory": "json",
  6489|         }
  6490|         output_authority = {
  6491|             "governance_domain_summary": "authoritative_deterministic_evidence",
  6492|             "governance_client_summary": "authoritative_deterministic_evidence",
  6493|             "governance_bc_summary": "authoritative_deterministic_evidence",
  6494|             "governance_narrative_context": "controlled_interpretation",
  6495|             "governance_package_manifest": "authoritative_deterministic_evidence",
  6496|             "governance_package_health": "controlled_interpretation",
  6497|             "governance_evidence_map": "authoritative_deterministic_evidence",
  6498|             "governance_findings": "controlled_interpretation",
  6499|             "governance_file_inventory": AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  6500|         }
  6501|         output_context_role = {
  6502|             "governance_domain_summary": "primary tier/score rollup",
  6503|             "governance_client_summary": "primary client alignment/onboarding rollup",
  6504|             "governance_bc_summary": "primary business-center peer-alignment rollup",
  6505|             "governance_narrative_context": "human-readable synthesis",
  6506|             "governance_package_manifest": "provenance record",
  6507|             "governance_package_health": "coverage/health signal",
  6508|             "governance_evidence_map": "artifact navigation index",
  6509|             "governance_findings": "structured, rule-derived findings",
  6510|             "governance_file_inventory": "live directory-scan inventory of undiscovered drill-down files",
  6511|         }
  6512| 
  6513|         if args.emit_interpretation_layer:
  6514|             brief_path = out_dir / "governance_brief.md"
  6515|             output_paths["governance_brief"] = brief_path
  6516|             output_types["governance_brief"] = "markdown"
  6517|             output_authority["governance_brief"] = "convenience_summary"
  6518|             output_context_role["governance_brief"] = "narrower run-specific digest"
  6519| 
  6520|         # All loaded row sets that carry their own comparison_run_id/executed_utc
  6521|         # (per compare_cross_segment.py's own *_FIELDS definitions) -- not just
  6522|         # summary_rows/pooled_rows. An optional evidence file loaded from a
  6523|         # different comparison run (e.g. --governance-state-summary/--delta
  6524|         # supplied from a stale export) must surface as multiple values here,
  6525|         # or a mixed-run package would misleadingly look like one reproducible
  6526|         # run. union_inventory_rows/reuse_distribution_rows/matrix_manifest_rows
  6527|         # carry executed_utc but not comparison_run_id (they are not scoped to
  6528|         # a single directed-comparison run the way the others are) -- same for
  6529|         # reuse_by_client_rows/project_union_jaccard_rows/
  6530|         # project_density_similarity_rows/project_pool_containment_rows/
  6531|         # project_fragmentation_rows (REUSE_SUMMARY_FIELDS/MATRIX_OUTPUT_FIELDS/
  6532|         # FRAGMENTATION_DIAGNOSTIC_FIELDS in compare_cross_segment.py).
  6533|         # governance_bc_client_rows/governance_client_bc_rows carry neither --
  6534|         # tools/governance_relationships.py aggregates directly from file_
  6535|         # metadata.csv's own columns and has no comparison-run/executed_utc
  6536|         # concept of its own (see BC_CLIENT_MATRIX_FIELDNAMES/CLIENT_BC_MATRIX_
  6537|         # FIELDNAMES there) -- so they are intentionally absent from both sets
  6538|         # below, not an oversight.
  6539|         _run_id_row_sets = (
  6540|             summary_rows, pooled_rows, governance_state_rows,
  6541|             governance_state_summary_rows, delta_rows,
  6542|         )
  6543|         _executed_utc_row_sets = _run_id_row_sets + (
  6544|             union_inventory_rows, reuse_distribution_rows, matrix_manifest_rows,
  6545|             reuse_by_client_rows, project_union_jaccard_rows, project_density_similarity_rows,
  6546|             project_pool_containment_rows, project_fragmentation_rows,
  6547|         )
  6548|         comparison_run_ids = sorted(
  6549|             set().union(*(
  6550|                 {r.get("comparison_run_id", "") for r in rows} for rows in _run_id_row_sets
  6551|             ))
  6552|             - {""}
  6553|         )
  6554|         source_executed_utc = sorted(
  6555|             set().union(*(
  6556|                 {r.get("executed_utc", "") for r in rows} for rows in _executed_utc_row_sets
  6557|             ))
  6558|             - {""}
  6559|         )
  6560| 
  6561|         # health.json and evidence_map.json are built and written *before* the
  6562|         # manifest, and the manifest is built from an output_paths view that
  6563|         # excludes its own file. build_package_manifest() stats each entry in
  6564|         # output_paths via Path.exists()/Path.stat() at call time -- if the
  6565|         # manifest were built (and therefore stat its sibling JSON outputs)
  6566|         # before those files existed on disk, it would permanently record them
  6567|         # as present: false/size_bytes: null once written. A manifest also
  6568|         # cannot accurately stat itself before it has been written -- that
  6569|         # self-description job already belongs to governance_evidence_map.json
  6570|         # (see its self-entry, related_artifacts). Writing health/evidence_map
  6571|         # first, then the manifest last with a self-excluded output_paths view,
  6572|         # avoids both problems without a two-pass write.
  6573|         health = build_package_health(
  6574|             schema_version=args.package_schema_version,
  6575|             schema_detection=schema_detection,
  6576|             used_view_fallback=legacy_fallback,
  6577|             comparison_type_coverage_by_fn={
  6578|                 "build_cascade": cascade_coverage,
  6579|                 "build_governance_state_summary": gov_state_coverage,
  6580|             },
  6581|             required_inputs={k: input_present[k] for k in input_required},
  6582|             optional_inputs={k: input_present[k] for k in input_paths if k not in input_required},
  6583|             client_sector_status=client_sector_status,
  6584|             domain_csv_row_count=len(domain_csv_rows),
  6585|             domain_rows_excluded_no_signal=sum(
  6586|                 1 for d in cascade.values() if not _has_renderable_cascade_signal(d)
  6587|             ),
  6588|             client_csv_row_count=len(client_csv_rows),
  6589|             corpus_project_file_count=corpus.get("Project", 0),
  6590|             excluded_from_scoring=sorted(EXCLUDED_FROM_SCORING),
  6591|             unit_systems_seen=unit_systems_seen,
  6592|             matrix_manifest_row_count=len(matrix_manifest_rows),
  6593|             matrix_names_seen=matrix_names_seen,
  6594|             policy_load_status=governance_policy["load_status"],
  6595|             comparison_completeness=comparison_completeness,
  6596|             interpretation_guide_present=INTERPRETATION_GUIDE_PATH.exists(),
  6597|         )
  6598|         write_json(out_dir / "governance_package_health.json", health)
  6599| 
  6600|         findings_document = build_findings_document(findings, schema_version=FINDINGS_SCHEMA_VERSION)
  6601|         write_json(out_dir / "governance_findings.json", findings_document)
  6602| 
  6603|         # governance_interpretation_guide.md / governance_question_routes.md /
  6604|         # governance_reading_order.md (D-030) are human/LLM-authored static
  6605|         # reference docs, never written by this generator -- always listed in
  6606|         # the evidence map (like the never-consumed sibling CSVs below) with
  6607|         # presence computed from real Path.exists(), independent of
  6608|         # --emit-interpretation-layer (that flag controls the per-run
  6609|         # governance_brief.md only, not whether these repo-level docs are
  6610|         # acknowledged to exist).
  6611|         # governance_relationships.csv (tools/governance_relationships.py) is
  6612|         # never read by this generator -- only governance_bc_client_matrix.csv/
  6613|         # governance_client_bc_matrix.csv (loaded via --governance-bc-client-
  6614|         # matrix/--governance-client-bc-matrix above) are. It is named by path
  6615|         # in the Business Center Composition section's body text ("See
  6616|         # governance_relationships.csv for the underlying per-project rows"),
  6617|         # so it is registered as an inferred sibling path the same way
  6618|         # cross_segment_file_pairs/comparison_registry already are -- but
  6619|         # tools/governance_relationships.py's --out-dir is independent of
  6620|         # --summary's directory (both are free-form CLI paths), and that tool
  6621|         # always writes all three of its outputs into the SAME --out-dir
  6622|         # together. Resolving beside whichever governance-matrix flag was
  6623|         # actually supplied (not beside --summary) means this still finds the
  6624|         # real file when the relationship layer's outputs live somewhere else
  6625|         # entirely -- falling back to --summary's directory only when neither
  6626|         # matrix flag was supplied, at which point none of the three
  6627|         # relationship-layer artifacts are expected to be present anyway.
  6628|         _relationships_anchor = (
  6629|             Path(args.governance_bc_client_matrix) if args.governance_bc_client_matrix
  6630|             else Path(args.governance_client_bc_matrix) if args.governance_client_bc_matrix
  6631|             else Path(args.summary)
  6632|         )
  6633|         # D-024 PR-review fix: pattern_reuse_summary_by_domain.csv and
  6634|         # project_mean_file_pair_jaccard_matrix.csv are written by
  6635|         # compare_cross_segment.py's main() to the SAME --out-dir as their
  6636|         # already-optionally-supplied siblings (pattern_reuse_summary_by_domain.csv
  6637|         # alongside pattern_reuse_distribution.csv/pattern_reuse_summary_by_client.csv;
  6638|         # project_mean_file_pair_jaccard_matrix.csv alongside the other project_*
  6639|         # matrices and project_fragmentation_diagnostic.csv -- see the single
  6640|         # `if reuse_distribution_rows:` / `if matrix_outputs or ...:` write blocks
  6641|         # in compare_cross_segment.py). A caller who points those optional flags
  6642|         # at a directory other than --summary's (allowed, and already how
  6643|         # _relationships_anchor is resolved below) would otherwise get a
  6644|         # permanently `present: false` entry for these two siblings even though
  6645|         # the real files sit right beside the matrix/reuse input actually
  6646|         # supplied. Anchored the same way _relationships_anchor already is:
  6647|         # prefer the most specific supplied sibling, fall back to --summary's directory.
```
