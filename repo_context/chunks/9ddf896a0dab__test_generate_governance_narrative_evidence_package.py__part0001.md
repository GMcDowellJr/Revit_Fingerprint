# Chunk of tests/test_generate_governance_narrative_evidence_package.py

- Source relative path: `tests/test_generate_governance_narrative_evidence_package.py`
- Chunk: 1 of 3
- Original line range: 1-517
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _summary_row, _pooled_row, _delta_row, _gov_state_summary_row, _write_csv, _minimal_fixture, _run_main, test_footer_references_real_generator_identity_not_stale_filename, test_authority_header_states_controlled_interpretation_and_no_llm, test_authority_header_inserted_between_header_and_state_model, test_comparison_type_coverage_matches_known_cascade_groups, test_bc_to_bc_and_client_cross_bc_are_registered_not_unrecognized, test_comparison_type_coverage_governance_state_uses_directed_types, test_unrecognized_comparison_type_still_warns_to_stderr, test_domain_csv_column_set_unchanged, test_client_csv_column_set_unchanged, test_emit_evidence_package_default_writes_three_json_files, test_no_emit_evidence_package_suppresses_json_but_not_existing_outputs, test_no_emit_removes_stale_evidence_package_files_from_prior_run, test_emit_and_no_emit_produce_identical_csvs, test_no_emit_narrative_does_not_point_at_missing_package_files, test_emit_narrative_points_at_package_files, test_package_manifest_records_inputs_and_outputs, test_package_manifest_reports_sibling_json_outputs_as_present_with_real_sizes, test_package_manifest_records_comparison_run_ids, test_package_manifest_comparison_run_ids_include_pooled_only_values, test_package_manifest_comparison_run_ids_include_optional_evidence_values, test_package_health_schema_detection_dual_for_dual_view_rows, test_package_health_optional_inputs_present_reflects_cli_flags, test_segment_manifest_recorded_in_evidence_package_when_supplied
- Source SHA-256: e60c8dba47b5674d967f6b921c70c80a38089dd30ac298318b6e62f873f624ab
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """Tests for the PR1 governance evidence-package layer wired into
     2| tools/generate_governance_narrative.py: the corrected producer-identity
     3| footer, the new evidence-authority narrative header, the comparison_type
     4| coverage hook, and the governance_package_manifest.json /
     5| governance_package_health.json / governance_evidence_map.json outputs.
     6| 
     7| See docs/governance_evidence_package.md and
     8| docs/governance_narrative_scope_gap_audit.md.
     9| """
    10| from __future__ import annotations
    11| 
    12| import csv
    13| import json
    14| from pathlib import Path
    15| import sys
    16| 
    17| sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
    18| 
    19| from compare_cross_segment import (  # noqa: E402
    20|     SUMMARY_FIELDS, POOLED_FIELDS, DELTA_FIELDS, GOVERNANCE_STATE_SUMMARY_FIELDS,
    21|     COMPARISON_REGISTRY_FIELDS, REUSE_SUMMARY_FIELDS, MATRIX_OUTPUT_FIELDS,
    22|     UNION_INVENTORY_FIELDS, MATRIX_MANIFEST_FIELDS,
    23| )
    24| from governance_evidence_package import GENERATOR_IDENTITY  # noqa: E402
    25| from generate_governance_narrative import (  # noqa: E402
    26|     CASCADE_GROUP1_TYPES,
    27|     CASCADE_GROUP2_TYPES,
    28|     CASCADE_GROUP3_TYPES,
    29|     CASCADE_GROUP3B_TYPES,
    30|     CASCADE_GROUP4_EXCLUDED_TYPES,
    31|     EVIDENCE_MAP_SCHEMA_VERSION,
    32|     _comparison_type_coverage,
    33|     _DIRECTED_GOVERNANCE_TYPES,
    34|     main,
    35|     render_evidence_authority_header,
    36|     render_limitations,
    37| )
    38| 
    39| 
    40| def _summary_row(**overrides):
    41|     r = {f: "" for f in SUMMARY_FIELDS}
    42|     r.update(overrides)
    43|     return r
    44| 
    45| 
    46| def _pooled_row(**overrides):
    47|     r = {f: "" for f in POOLED_FIELDS}
    48|     r.update(overrides)
    49|     return r
    50| 
    51| 
    52| def _delta_row(**overrides):
    53|     r = {f: "" for f in DELTA_FIELDS}
    54|     r.update(overrides)
    55|     return r
    56| 
    57| 
    58| def _gov_state_summary_row(**overrides):
    59|     r = {f: "" for f in GOVERNANCE_STATE_SUMMARY_FIELDS}
    60|     r.update(overrides)
    61|     return r
    62| 
    63| 
    64| def _write_csv(path: Path, fields: list, rows: list) -> None:
    65|     with open(path, "w", newline="", encoding="utf-8") as f:
    66|         w = csv.DictWriter(f, fieldnames=fields)
    67|         w.writeheader()
    68|         w.writerows(rows)
    69| 
    70| 
    71| def _minimal_fixture(tmp_path: Path) -> tuple[Path, Path]:
    72|     summary_rows = [
    73|         _summary_row(
    74|             comparison_run_id="run1", segment_id_a="imperial|Template",
    75|             segment_id_b="imperial|Project|acme", governance_role_a="Template",
    76|             governance_role_b="Project", client_label_b="acme",
    77|             comparison_type="template_to_project", domain="line_styles",
    78|             all_pairwise_containment_a_in_b_mean="0.8", all_pairwise_jaccard_mean="0.5",
    79|             n_files_a="3", n_files_b="10",
    80|             executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
    81|         ),
    82|     ]
    83|     pooled_rows = [
    84|         _pooled_row(
    85|             comparison_run_id="run1", segment_id="imperial|Project|acme",
    86|             client_label="acme", governance_role="Project", pool_scope="parent_sibling",
    87|             domain="line_styles", n_files_focal="10", n_files_pool="30",
    88|             executed_utc="2026-07-16T00:00:00Z",
    89|         ),
    90|     ]
    91|     summary_path = tmp_path / "cross_segment_summary.csv"
    92|     pooled_path = tmp_path / "cross_segment_pooled.csv"
    93|     _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
    94|     _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)
    95|     return summary_path, pooled_path
    96| 
    97| 
    98| def _run_main(monkeypatch, argv):
    99|     monkeypatch.setattr(sys, "argv", ["generate_governance_narrative.py"] + argv)
   100|     main()
   101| 
   102| 
   103| # ---------------------------------------------------------------------------
   104| # Footer / producer identity
   105| # ---------------------------------------------------------------------------
   106| 
   107| def test_footer_references_real_generator_identity_not_stale_filename():
   108|     md = render_limitations({"Project": 10})
   109|     assert "generate_governance_narrative_dod_aligned_v2.py" not in md
   110|     assert f"`{GENERATOR_IDENTITY}`" in md
   111|     assert GENERATOR_IDENTITY == "generate_governance_narrative.py"
   112| 
   113| 
   114| # ---------------------------------------------------------------------------
   115| # Authority header
   116| # ---------------------------------------------------------------------------
   117| 
   118| def test_authority_header_states_controlled_interpretation_and_no_llm():
   119|     md = render_evidence_authority_header("1.0", GENERATOR_IDENTITY)
   120|     assert "controlled_interpretation" in md
   121|     assert "no LLM is involved" in md
   122|     assert "governance_package_health.json" in md
   123|     assert "governance_evidence_map.json" in md
   124| 
   125| 
   126| def test_authority_header_inserted_between_header_and_state_model(tmp_path, monkeypatch):
   127|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   128|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   129|     md = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
   130|     idx_header = md.index("## Executive Summary")
   131|     idx_authority = md.index("**Artifact role:**")
   132|     idx_state_model = md.index("## Governance State Model") if "## Governance State Model" in md else len(md)
   133|     assert idx_header < idx_authority
   134|     assert idx_authority < idx_state_model
   135| 
   136| 
   137| # ---------------------------------------------------------------------------
   138| # comparison_type coverage -- backward-compatible refactor
   139| # ---------------------------------------------------------------------------
   140| 
   141| def test_comparison_type_coverage_matches_known_cascade_groups():
   142|     known = (
   143|         CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | CASCADE_GROUP3B_TYPES
   144|         | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
   145|     )
   146|     cov = _comparison_type_coverage({"template_to_project", "bogus_type"}, known,
   147|                                      intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()))
   148|     assert cov["unrecognized"] == ["bogus_type"]
   149|     assert "template_to_project" in cov["recognized"]
   150| 
   151| 
   152| def test_bc_to_bc_and_client_cross_bc_are_registered_not_unrecognized():
   153|     """Regression for a PR #373 review finding: compare_cross_segment.py's
   154|     new bc_to_bc/client_cross_bc comparison types must be in the known set
   155|     (like sibling_templates/sibling_containers), or a default run where
   156|     they're emitted surfaces as unrecognized-comparison-type coverage
   157|     degradation even though the producer intentionally emitted the rows.
   158|     bc_to_bc has since moved from Group 4 (excluded) to Group 3b (captured
   159|     into build_cascade(), still not rendered) -- see CASCADE_GROUP3B_TYPES;
   160|     client_cross_bc remains Group-4-excluded."""
   161|     known = (
   162|         CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | CASCADE_GROUP3B_TYPES
   163|         | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
   164|     )
   165|     cov = _comparison_type_coverage({"bc_to_bc", "client_cross_bc"}, known,
   166|                                      intentionally_excluded=set(CASCADE_GROUP4_EXCLUDED_TYPES.keys()))
   167|     assert cov["unrecognized"] == []
   168|     assert "bc_to_bc" in cov["recognized"]
   169|     assert "bc_to_bc" in CASCADE_GROUP3B_TYPES
   170|     assert "bc_to_bc" not in CASCADE_GROUP4_EXCLUDED_TYPES
   171|     assert "client_cross_bc" in cov["intentionally_excluded"]
   172|     assert "client_cross_bc" in CASCADE_GROUP4_EXCLUDED_TYPES
   173| 
   174| 
   175| def test_comparison_type_coverage_governance_state_uses_directed_types():
   176|     cov = _comparison_type_coverage({"generic_to_template"}, _DIRECTED_GOVERNANCE_TYPES)
   177|     assert cov["unrecognized"] == []
   178| 
   179| 
   180| def test_unrecognized_comparison_type_still_warns_to_stderr(capsys, tmp_path, monkeypatch):
   181|     """Locks in that the _warn_unrecognized_comparison_types refactor (adding a
   182|     return value) did not remove its existing stderr side effect."""
   183|     summary_rows = [
   184|         _summary_row(
   185|             comparison_run_id="run1", segment_id_a="imperial|Template",
   186|             segment_id_b="imperial|Project|acme", governance_role_a="Template",
   187|             governance_role_b="Project", client_label_b="acme",
   188|             comparison_type="totally_bogus_comparison_type", domain="line_styles",
   189|             all_pairwise_containment_a_in_b_mean="0.8", all_pairwise_jaccard_mean="0.5",
   190|             n_files_a="3", n_files_b="10",
   191|             executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
   192|         ),
   193|     ]
   194|     pooled_rows = [
   195|         _pooled_row(comparison_run_id="run1", segment_id="imperial|Project|acme",
   196|                     client_label="acme", governance_role="Project", pool_scope="parent_sibling",
   197|                     domain="line_styles", n_files_focal="10", n_files_pool="30",
   198|                     executed_utc="2026-07-16T00:00:00Z"),
   199|     ]
   200|     summary_path = tmp_path / "cross_segment_summary.csv"
   201|     pooled_path = tmp_path / "cross_segment_pooled.csv"
   202|     _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
   203|     _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)
   204| 
   205|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   206|     captured = capsys.readouterr()
   207|     assert "unrecognized comparison_type" in captured.err
   208|     assert "totally_bogus_comparison_type" in captured.err
   209| 
   210|     health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
   211|     assert "totally_bogus_comparison_type" in health["comparison_type_coverage"]["build_cascade"]["unrecognized"]
   212| 
   213| 
   214| # ---------------------------------------------------------------------------
   215| # Regression lock: PR1 must not change classification output
   216| # ---------------------------------------------------------------------------
   217| 
   218| _EXPECTED_DOMAIN_COLUMNS = [
   219|     "domain", "domain_label", "governance_tier", "score_reliability",
   220|     "cascade_generic_to_template", "cascade_generic_to_container", "cascade_generic_to_project",
   221|     "template_to_container", "container_to_project",
   222|     "container_to_project_scoped", "container_to_project_scoped_pair",
   223|     "template_to_project",
   224|     "cross_client_convergence", "cross_client_convergence_all_view",
   225|     "within_project_all", "within_project_p10", "within_project_p90",
   226|     "within_project_reliability_source",
   227|     "within_project_spread", "within_project_architectural", "within_project_mechanical_plumbing",
   228|     "within_project_electrical", "within_project_structural", "bundle_schema",
   229|     "template_to_project_used", "bundle_share_all", "bundle_share_used",
   230|     "passive_inheritance_indicator", "passive_indicator_method", "passive_inheritance_risk",
   231|     "generic_to_template", "generic_to_container", "generic_to_project",
   232|     "provided_to_configured_containment", "provided_to_used_containment", "provided_passive_share",
   233|     "provided_missing_share", "local_active_share", "provided_and_used_count",
   234|     "provided_but_passive_count", "provided_but_missing_count", "local_active_count",
   235|     "local_passive_count", "local_unbundled_count", "primary_governance_read",
   236|     "union_reuse_patterns_total", "union_reuse_patterns_corpus_wide",
   237|     "union_reuse_patterns_client_wide", "union_reuse_patterns_project_wide",
   238|     "union_reuse_patterns_file_level",
   239|     "notable_anomalies",
   240| ]
   241| 
   242| _EXPECTED_CLIENT_COLUMNS = [
   243|     "client", "n_project_files", "alignment_tier", "cross_client_similarity_mean",
   244|     "cross_client_similarity_mean_all_view",
   245|     "within_project_coherence", "within_project_coherence_all_view",
   246|     "confidence_note", "most_aligned_domains", "least_aligned_domains",
   247|     "onboarding_internal_read", "onboarding_portability_read", "onboarding_common_base",
   248|     "onboarding_variant_burden", "onboarding_operating_implication",
   249| ]
   250| 
   251| 
   252| def test_domain_csv_column_set_unchanged(tmp_path, monkeypatch):
   253|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   254|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   255|     with open(tmp_path / "governance_domain_summary.csv", newline="", encoding="utf-8") as f:
   256|         header = next(csv.reader(f))
   257|     assert header == _EXPECTED_DOMAIN_COLUMNS
   258| 
   259| 
   260| def test_client_csv_column_set_unchanged(tmp_path, monkeypatch):
   261|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   262|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   263|     with open(tmp_path / "governance_client_summary.csv", newline="", encoding="utf-8") as f:
   264|         header = next(csv.reader(f))
   265|     assert header == _EXPECTED_CLIENT_COLUMNS
   266| 
   267| 
   268| # ---------------------------------------------------------------------------
   269| # Evidence-package emission (default on)
   270| # ---------------------------------------------------------------------------
   271| 
   272| def test_emit_evidence_package_default_writes_three_json_files(tmp_path, monkeypatch):
   273|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   274|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   275|     assert (tmp_path / "governance_package_manifest.json").exists()
   276|     assert (tmp_path / "governance_package_health.json").exists()
   277|     assert (tmp_path / "governance_evidence_map.json").exists()
   278| 
   279| 
   280| def test_no_emit_evidence_package_suppresses_json_but_not_existing_outputs(tmp_path, monkeypatch):
   281|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   282|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   283|                             "--out", str(tmp_path), "--no-emit-evidence-package"])
   284|     assert not (tmp_path / "governance_package_manifest.json").exists()
   285|     assert not (tmp_path / "governance_package_health.json").exists()
   286|     assert not (tmp_path / "governance_evidence_map.json").exists()
   287|     assert (tmp_path / "governance_domain_summary.csv").exists()
   288|     assert (tmp_path / "governance_client_summary.csv").exists()
   289|     assert (tmp_path / "governance_narrative_context.md").exists()
   290| 
   291| 
   292| def test_no_emit_removes_stale_evidence_package_files_from_prior_run(tmp_path, monkeypatch):
   293|     """Regression test for a PR review finding: rerunning with
   294|     --no-emit-evidence-package over an --out directory that already has
   295|     package JSONs from an earlier default (emit-on) run must not leave those
   296|     stale files in place -- the narrative just rendered says no package
   297|     health/evidence-map file exists for this run, so leaving old ones would
   298|     let a downstream reader pick up out-of-date provenance/health data."""
   299|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   300|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   301|     assert (tmp_path / "governance_package_manifest.json").exists()
   302|     assert (tmp_path / "governance_package_health.json").exists()
   303|     assert (tmp_path / "governance_evidence_map.json").exists()
   304| 
   305|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   306|                             "--out", str(tmp_path), "--no-emit-evidence-package"])
   307|     assert not (tmp_path / "governance_package_manifest.json").exists()
   308|     assert not (tmp_path / "governance_package_health.json").exists()
   309|     assert not (tmp_path / "governance_evidence_map.json").exists()
   310|     assert (tmp_path / "governance_domain_summary.csv").exists()
   311| 
   312| 
   313| def test_emit_and_no_emit_produce_identical_csvs(tmp_path, monkeypatch):
   314|     """The two CSV outputs are unaffected by --emit-evidence-package -- only
   315|     the narrative's authority-header package-pointer section differs (see
   316|     test_no_emit_narrative_does_not_point_at_missing_package_files)."""
   317|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   318|     out_a = tmp_path / "a"
   319|     out_b = tmp_path / "b"
   320|     out_a.mkdir()
   321|     out_b.mkdir()
   322|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(out_a)])
   323|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   324|                             "--out", str(out_b), "--no-emit-evidence-package"])
   325|     for name in ("governance_domain_summary.csv", "governance_client_summary.csv"):
   326|         assert (out_a / name).read_bytes() == (out_b / name).read_bytes(), name
   327| 
   328| 
   329| def test_no_emit_narrative_does_not_point_at_missing_package_files(tmp_path, monkeypatch):
   330|     """Regression test for a PR review finding: the narrative's authority
   331|     header unconditionally referenced governance_package_health.json/
   332|     governance_findings.json/governance_evidence_map.json even when
   333|     --no-emit-evidence-package means those files are never written."""
   334|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   335|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   336|                             "--out", str(tmp_path), "--no-emit-evidence-package"])
   337|     md = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
   338|     assert "governance_package_health.json" not in md
   339|     assert "governance_findings.json" not in md
   340|     assert "governance_evidence_map.json" not in md
   341|     assert "--no-emit-evidence-package" in md
   342|     # The rest of the narrative (findings section, footer) is unaffected.
   343|     assert "## Key Findings and Governance Questions" in md
   344|     assert f"`{GENERATOR_IDENTITY}`" in md
   345| 
   346| 
   347| def test_emit_narrative_points_at_package_files(tmp_path, monkeypatch):
   348|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   349|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   350|     md = (tmp_path / "governance_narrative_context.md").read_text(encoding="utf-8")
   351|     assert "governance_package_health.json" in md
   352|     assert "governance_findings.json" in md
   353|     assert "governance_evidence_map.json" in md
   354| 
   355| 
   356| def test_package_manifest_records_inputs_and_outputs(tmp_path, monkeypatch):
   357|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   358|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   359|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   360|     by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
   361|     assert by_id["cross_segment_summary"]["present"] is True
   362|     assert by_id["cross_segment_union_inventory"]["present"] is False
   363|     out_by_id = {o["artifact_id"]: o for o in manifest["outputs"]}
   364|     assert out_by_id["governance_domain_summary"]["size_bytes"] > 0
   365|     assert manifest["generator"]["name"] == GENERATOR_IDENTITY
   366|     assert manifest["package_status"] == "complete"
   367| 
   368| 
   369| def test_package_manifest_reports_sibling_json_outputs_as_present_with_real_sizes(tmp_path, monkeypatch):
   370|     """Regression test for a PR review finding: the manifest is built (and
   371|     stats its output_paths) after governance_package_health.json and
   372|     governance_evidence_map.json are already written to disk, so it must not
   373|     report them as present=False/size_bytes=None. The manifest also does not
   374|     describe its own file (see build_package_manifest's manifest_output_paths
   375|     exclusion in main()) -- self-description is governance_evidence_map.json's
   376|     job, not the manifest's."""
   377|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   378|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   379|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   380|     out_by_id = {o["artifact_id"]: o for o in manifest["outputs"]}
   381|     assert "governance_package_manifest" not in out_by_id
   382|     for artifact_id in ("governance_package_health", "governance_evidence_map"):
   383|         assert out_by_id[artifact_id]["present"] is True, artifact_id
   384|         assert out_by_id[artifact_id]["size_bytes"] > 0, artifact_id
   385|         expected_path = tmp_path / {
   386|             "governance_package_health": "governance_package_health.json",
   387|             "governance_evidence_map": "governance_evidence_map.json",
   388|         }[artifact_id]
   389|         assert out_by_id[artifact_id]["size_bytes"] == expected_path.stat().st_size
   390| 
   391| 
   392| def test_package_manifest_records_comparison_run_ids(tmp_path, monkeypatch):
   393|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   394|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   395|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   396|     assert manifest["corpus_scope"]["comparison_run_ids"] == ["run1"]
   397|     assert manifest["corpus_scope"]["source_executed_utc"] == ["2026-07-16T00:00:00Z"]
   398| 
   399| 
   400| def test_package_manifest_comparison_run_ids_include_pooled_only_values(tmp_path, monkeypatch):
   401|     """If --summary and --pooled are accidentally taken from different runs,
   402|     the manifest's provenance sets must surface both run ids / timestamps,
   403|     not just summary's -- otherwise a mixed-run package looks single-run."""
   404|     summary_rows = [
   405|         _summary_row(
   406|             comparison_run_id="run1", segment_id_a="imperial|Template",
   407|             segment_id_b="imperial|Project|acme", governance_role_a="Template",
   408|             governance_role_b="Project", client_label_b="acme",
   409|             comparison_type="template_to_project", domain="line_styles",
   410|             all_pairwise_containment_a_in_b_mean="0.8", all_pairwise_jaccard_mean="0.5",
   411|             n_files_a="3", n_files_b="10",
   412|             executed_utc="2026-07-16T00:00:00Z", unit_system="imperial",
   413|         ),
   414|     ]
   415|     pooled_rows = [
   416|         _pooled_row(
   417|             comparison_run_id="run2", segment_id="imperial|Project|acme",
   418|             client_label="acme", governance_role="Project", pool_scope="parent_sibling",
   419|             domain="line_styles", n_files_focal="10", n_files_pool="30",
   420|             executed_utc="2026-07-15T00:00:00Z",
   421|         ),
   422|     ]
   423|     summary_path = tmp_path / "cross_segment_summary.csv"
   424|     pooled_path = tmp_path / "cross_segment_pooled.csv"
   425|     _write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
   426|     _write_csv(pooled_path, POOLED_FIELDS, pooled_rows)
   427| 
   428|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   429|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   430|     assert manifest["corpus_scope"]["comparison_run_ids"] == ["run1", "run2"]
   431|     assert manifest["corpus_scope"]["source_executed_utc"] == ["2026-07-15T00:00:00Z", "2026-07-16T00:00:00Z"]
   432| 
   433| 
   434| def test_package_manifest_comparison_run_ids_include_optional_evidence_values(tmp_path, monkeypatch):
   435|     """Regression test for a PR review finding: --governance-state-summary/
   436|     --delta rows are parsed and can drive the narrative/findings, and they
   437|     carry their own comparison_run_id/executed_utc (compare_cross_segment.py's
   438|     GOVERNANCE_STATE_SUMMARY_FIELDS/DELTA_FIELDS), but the manifest's
   439|     corpus_scope used to report only summary_rows/pooled_rows -- so a package
   440|     built from a --delta or --governance-state-summary file taken from a
   441|     different comparison run than --summary/--pooled would silently look like
   442|     a single reproducible run."""
   443|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   444| 
   445|     delta_rows = [_delta_row(comparison_run_id="run-delta", domain="line_styles",
   446|                               executed_utc="2026-06-01T00:00:00Z")]
   447|     delta_path = tmp_path / "cross_segment_delta.csv"
   448|     _write_csv(delta_path, DELTA_FIELDS, delta_rows)
   449| 
   450|     state_summary_rows = [_gov_state_summary_row(
   451|         comparison_run_id="run-state", domain="line_styles", comparison_type="template_to_project",
   452|         executed_utc="2026-06-02T00:00:00Z",
   453|     )]
   454|     state_summary_path = tmp_path / "cross_segment_governance_state_summary.csv"
   455|     _write_csv(state_summary_path, GOVERNANCE_STATE_SUMMARY_FIELDS, state_summary_rows)
   456| 
   457|     _run_main(monkeypatch, [
   458|         "--summary", str(summary_path), "--pooled", str(pooled_path),
   459|         "--delta", str(delta_path), "--governance-state-summary", str(state_summary_path),
   460|         "--out", str(tmp_path),
   461|     ])
   462|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   463|     assert manifest["corpus_scope"]["comparison_run_ids"] == ["run-delta", "run-state", "run1"]
   464|     assert manifest["corpus_scope"]["source_executed_utc"] == [
   465|         "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", "2026-07-16T00:00:00Z",
   466|     ]
   467| 
   468| 
   469| def test_package_health_schema_detection_dual_for_dual_view_rows(tmp_path, monkeypatch):
   470|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   471|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   472|     health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
   473|     assert health["schema_detection"] == "dual"
   474|     assert health["used_view_fallback"] is False
   475|     assert health["overall_status"] == "complete"
   476| 
   477| 
   478| def test_package_health_optional_inputs_present_reflects_cli_flags(tmp_path, monkeypatch):
   479|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   480|     file_meta_path = tmp_path / "file_metadata.csv"
   481|     _write_csv(file_meta_path, ["governance_role", "client_label", "discipline_label"], [])
   482|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   483|                             "--out", str(tmp_path), "--file-meta", str(file_meta_path)])
   484|     health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
   485|     assert health["optional_inputs"]["file_metadata"] is True
   486|     assert health["optional_inputs"]["cross_segment_union_inventory"] is False
   487| 
   488| 
   489| def test_segment_manifest_recorded_in_evidence_package_when_supplied(tmp_path, monkeypatch):
   490|     """Regression test for a PR #381 review finding: --segment-manifest changes
   491|     cascade/governance_domain_summary.csv (the within_project_reliability_source
   492|     resolved-segment fallback) but was not being recorded anywhere in the
   493|     evidence package, so a run using the fallback would misleadingly claim it
   494|     was built without the manifest input that actually affected the scores."""
   495|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   496|     manifest_path = tmp_path / "segment_manifest.csv"
   497|     _write_csv(manifest_path, ["segment_id", "run_type", "notes"], [
   498|         {"segment_id": "imperial|Project", "run_type": "registration",
   499|          "notes": "redundant_single_child:imperial|Project|acme"},
   500|         {"segment_id": "imperial|Project|acme", "run_type": "bundle", "notes": ""},
   501|     ])
   502|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   503|                             "--out", str(tmp_path), "--segment-manifest", str(manifest_path)])
   504| 
   505|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   506|     inputs_by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
   507|     assert inputs_by_id["segment_manifest"]["present"] is True
   508|     assert inputs_by_id["segment_manifest"]["path"] == str(manifest_path)
   509| 
   510|     health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
   511|     assert health["optional_inputs"]["segment_manifest"] is True
   512| 
   513|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   514|     ids = [a["artifact_id"] for a in evidence_map["artifacts"]]
   515|     assert "segment_manifest" in ids
   516| 
   517| 
```
