# Chunk of tests/test_generate_governance_narrative_evidence_package.py

- Source relative path: `tests/test_generate_governance_narrative_evidence_package.py`
- Chunk: 2 of 3
- Original line range: 518-1035
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_segment_manifest_absent_from_evidence_package_when_not_supplied, test_evidence_map_lists_thirty_seven_artifacts_with_required_fields, test_governance_relationships_resolved_beside_supplied_matrix_not_summary_dir, test_pattern_reuse_summary_by_domain_resolved_beside_supplied_reuse_by_client_not_summary_dir, test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_fragmentation_diagnostic_not_summary_dir, test_pattern_reuse_summary_by_domain_resolved_beside_supplied_union_inventory_when_no_reuse_flag, test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_matrix_manifest_when_no_project_flag, test_evidence_map_findings_entry_has_a_real_path, test_manifest_output_artifact_ids_match_evidence_map_artifact_ids, test_manifest_input_artifact_ids_match_evidence_map_artifact_ids, test_evidence_map_related_artifacts_use_artifact_ids_not_filenames, test_file_inventory_written_and_registered_in_manifest_and_evidence_map, test_file_inventory_is_empty_when_no_undiscovered_files_present, test_file_inventory_surfaces_an_undiscovered_sibling_csv, test_file_inventory_never_flags_this_runs_own_outputs_as_undiscovered, test_file_inventory_borrows_interpretation_from_matrix_output_manifest, test_no_emit_evidence_package_suppresses_file_inventory, test_stale_file_inventory_removed_when_evidence_package_turned_off_between_runs, test_file_inventory_surfaces_regardless_of_interpretation_layer_flag, test_escalation_target_files_get_real_shape_in_evidence_map_not_generic_inventory, test_cli_accepts_policy_dir_and_package_schema_version_as_inert
- Source SHA-256: e60c8dba47b5674d967f6b921c70c80a38089dd30ac298318b6e62f873f624ab
- Starts inside symbol: no
- Ends inside symbol: no

```
   518| def test_segment_manifest_absent_from_evidence_package_when_not_supplied(tmp_path, monkeypatch):
   519|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   520|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   521| 
   522|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   523|     inputs_by_id = {i["artifact_id"]: i for i in manifest["inputs"]}
   524|     assert inputs_by_id["segment_manifest"]["present"] is False
   525|     assert inputs_by_id["segment_manifest"]["path"] is None
   526| 
   527|     health = json.loads((tmp_path / "governance_package_health.json").read_text(encoding="utf-8"))
   528|     assert health["optional_inputs"]["segment_manifest"] is False
   529| 
   530| 
   531| def test_evidence_map_lists_thirty_seven_artifacts_with_required_fields(tmp_path, monkeypatch):
   532|     # 29 (pre-relationship-layer) + governance_bc_client_matrix +
   533|     # governance_client_bc_matrix + governance_relationships + governance_file_inventory (D-023)
   534|     # + pattern_reuse_summary_by_domain + project_mean_file_pair_jaccard_matrix (D-024)
   535|     # + governance_reading_order (D-030) + governance_classification_rules (D-029).
   536|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   537|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   538|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   539|     ids = [a["artifact_id"] for a in evidence_map["artifacts"]]
   540|     assert len(ids) == 37
   541|     assert len(ids) == len(set(ids))
   542|     assert "governance_findings" in ids
   543|     assert "segment_manifest" in ids
   544|     assert "governance_bc_client_matrix" in ids
   545|     assert "governance_client_bc_matrix" in ids
   546|     assert "governance_relationships" in ids
   547|     assert "governance_file_inventory" in ids
   548|     assert "pattern_reuse_summary_by_domain" in ids
   549|     assert "project_mean_file_pair_jaccard_matrix" in ids
   550|     assert "governance_reading_order" in ids
   551|     assert "governance_classification_rules" in ids
   552|     narrative = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_narrative_context")
   553|     assert narrative["authority_level"] != "authoritative_deterministic_evidence"
   554|     assert set(evidence_map["reasoning_prerequisites"]) == {
   555|         a["artifact_id"] for a in evidence_map["artifacts"] if a["required_before_conclusions"] is True
   556|     }
   557|     assert set(evidence_map["reasoning_prerequisites"]) == {
   558|         a["artifact_id"] for a in evidence_map["artifacts"] if a["required_before_conclusions"] is True
   559|     }
   560| 
   561| 
   562| def test_governance_relationships_resolved_beside_supplied_matrix_not_summary_dir(tmp_path, monkeypatch):
   563|     """Regression test for a PR review finding: tools/governance_relationships.py's
   564|     --out-dir is independent of --summary's directory, but this generator used to
   565|     hard-code governance_relationships.csv's sibling path relative to --summary,
   566|     so a caller pointing --governance-bc-client-matrix at a different directory
   567|     got a permanently-absent governance_relationships evidence-map entry even
   568|     though the real file existed right beside the matrix it did supply."""
   569|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   570| 
   571|     matrix_dir = tmp_path / "relationship_layer_output"
   572|     matrix_dir.mkdir()
   573|     _write_csv(
   574|         matrix_dir / "governance_bc_client_matrix.csv",
   575|         ["business_center_label", "client_label", "project_count", "project_file_count",
   576|          "percentage_of_bc", "percentage_of_client"],
   577|         [{"business_center_label": "2014", "client_label": "ClientBeta", "project_count": "16",
   578|           "project_file_count": "62", "percentage_of_bc": "0.446043", "percentage_of_client": "1.000000"}],
   579|     )
   580|     relationships_path = matrix_dir / "governance_relationships.csv"
   581|     _write_csv(
   582|         relationships_path,
   583|         ["project_id", "project_name", "project_name_is_fallback", "client_label",
   584|          "business_center_label", "discipline_labels", "unit_system",
   585|          "project_file_count", "export_run_ids"],
   586|         [{"project_id": "proj_abc123", "project_name": "Alpha", "project_name_is_fallback": "false",
   587|           "client_label": "ClientBeta", "business_center_label": "2014", "discipline_labels": "architectural",
   588|           "unit_system": "imperial", "project_file_count": "1", "export_run_ids": "f1"}],
   589|     )
   590|     # governance_relationships.csv does NOT exist beside --summary -- only in matrix_dir.
   591|     assert not (tmp_path / "governance_relationships.csv").exists()
   592| 
   593|     _run_main(monkeypatch, [
   594|         "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
   595|         "--governance-bc-client-matrix", str(matrix_dir / "governance_bc_client_matrix.csv"),
   596|     ])
   597| 
   598|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   599|     rel_artifact = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_relationships")
   600|     assert rel_artifact["present"] is True
   601|     assert rel_artifact["path"] == str(relationships_path)
   602| 
   603| 
   604| def test_pattern_reuse_summary_by_domain_resolved_beside_supplied_reuse_by_client_not_summary_dir(tmp_path, monkeypatch):
   605|     """Regression test for a PR review finding (D-024): pattern_reuse_summary_by_domain.csv
   606|     is written by compare_cross_segment.py's main() to the SAME --out-dir as
   607|     pattern_reuse_summary_by_client.csv/pattern_reuse_distribution.csv, but this
   608|     generator used to hard-code its sibling path relative to --summary's
   609|     directory -- so a caller pointing --reuse-by-client at a different
   610|     directory got a permanently-absent evidence-map entry for this file even
   611|     though it sits right beside the reuse input actually supplied."""
   612|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   613| 
   614|     reuse_dir = tmp_path / "reuse_output"
   615|     reuse_dir.mkdir()
   616|     _write_csv(
   617|         reuse_dir / "pattern_reuse_summary_by_client.csv", REUSE_SUMMARY_FIELDS,
   618|         [{f: "" for f in REUSE_SUMMARY_FIELDS}],
   619|     )
   620|     domain_path = reuse_dir / "pattern_reuse_summary_by_domain.csv"
   621|     _write_csv(domain_path, REUSE_SUMMARY_FIELDS, [{f: "" for f in REUSE_SUMMARY_FIELDS}] * 3)
   622|     # pattern_reuse_summary_by_domain.csv does NOT exist beside --summary -- only in reuse_dir.
   623|     assert not (tmp_path / "pattern_reuse_summary_by_domain.csv").exists()
   624| 
   625|     _run_main(monkeypatch, [
   626|         "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
   627|         "--reuse-by-client", str(reuse_dir / "pattern_reuse_summary_by_client.csv"),
   628|     ])
   629| 
   630|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   631|     entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "pattern_reuse_summary_by_domain")
   632|     assert entry["present"] is True
   633|     assert entry["path"] == str(domain_path)
   634|     assert entry["row_count"] == 3
   635| 
   636| 
   637| def test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_fragmentation_diagnostic_not_summary_dir(tmp_path, monkeypatch):
   638|     """Regression test for a PR review finding (D-024): project_mean_file_pair_jaccard_matrix.csv
   639|     is written by compare_cross_segment.py's main() to the SAME --out-dir as
   640|     project_fragmentation_diagnostic.csv and the other project_* matrices, but
   641|     this generator used to hard-code its sibling path relative to --summary's
   642|     directory -- so a caller pointing --project-fragmentation-diagnostic at a
   643|     different directory got a permanently-absent evidence-map entry for this
   644|     file even though it sits right beside the matrix input actually supplied."""
   645|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   646| 
   647|     matrix_dir = tmp_path / "project_matrix_output"
   648|     matrix_dir.mkdir()
   649|     frag_fields = ["matrix_name", "row_id", "column_id", "view_scope", "domain",
   650|                    "footprint_similarity", "exact_identity_overlap", "fragmentation_diagnostic",
   651|                    "value_status", "interpretation", "executed_utc"]
   652|     _write_csv(
   653|         matrix_dir / "project_fragmentation_diagnostic.csv", frag_fields,
   654|         [{f: "" for f in frag_fields}],
   655|     )
   656|     matrix_path = matrix_dir / "project_mean_file_pair_jaccard_matrix.csv"
   657|     _write_csv(matrix_path, MATRIX_OUTPUT_FIELDS, [{f: "" for f in MATRIX_OUTPUT_FIELDS}] * 5)
   658|     # project_mean_file_pair_jaccard_matrix.csv does NOT exist beside --summary -- only in matrix_dir.
   659|     assert not (tmp_path / "project_mean_file_pair_jaccard_matrix.csv").exists()
   660| 
   661|     _run_main(monkeypatch, [
   662|         "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
   663|         "--project-fragmentation-diagnostic", str(matrix_dir / "project_fragmentation_diagnostic.csv"),
   664|     ])
   665| 
   666|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   667|     entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "project_mean_file_pair_jaccard_matrix")
   668|     assert entry["present"] is True
   669|     assert entry["path"] == str(matrix_path)
   670|     assert entry["row_count"] == 5
   671| 
   672| 
   673| def test_pattern_reuse_summary_by_domain_resolved_beside_supplied_union_inventory_when_no_reuse_flag(tmp_path, monkeypatch):
   674|     """Regression test for a PR-review follow-up (D-024): --union-inventory
   675|     (cross_segment_union_inventory.csv) is written by compare_cross_segment.py's
   676|     main() to the same --out-dir as the reuse-distribution family, so it must
   677|     also anchor pattern_reuse_summary_by_domain.csv when neither
   678|     --reuse-by-client nor --reuse-distribution was supplied."""
   679|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   680| 
   681|     union_dir = tmp_path / "union_output"
   682|     union_dir.mkdir()
   683|     _write_csv(
   684|         union_dir / "cross_segment_union_inventory.csv", UNION_INVENTORY_FIELDS,
   685|         [{f: "" for f in UNION_INVENTORY_FIELDS}],
   686|     )
   687|     domain_path = union_dir / "pattern_reuse_summary_by_domain.csv"
   688|     _write_csv(domain_path, REUSE_SUMMARY_FIELDS, [{f: "" for f in REUSE_SUMMARY_FIELDS}] * 2)
   689|     assert not (tmp_path / "pattern_reuse_summary_by_domain.csv").exists()
   690| 
   691|     _run_main(monkeypatch, [
   692|         "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
   693|         "--union-inventory", str(union_dir / "cross_segment_union_inventory.csv"),
   694|     ])
   695| 
   696|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   697|     entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "pattern_reuse_summary_by_domain")
   698|     assert entry["present"] is True
   699|     assert entry["path"] == str(domain_path)
   700|     assert entry["row_count"] == 2
   701| 
   702| 
   703| def test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_matrix_manifest_when_no_project_flag(tmp_path, monkeypatch):
   704|     """Regression test for a PR-review follow-up (D-024): --matrix-manifest
   705|     (matrix_output_manifest.csv) is written by compare_cross_segment.py's
   706|     main() to the same --out-dir as every project_* matrix (the single
   707|     `if matrix_outputs or fragmentation_rows or matrix_manifest_rows:` write
   708|     block), so it must also anchor project_mean_file_pair_jaccard_matrix.csv
   709|     when none of the individual --project-* flags was supplied."""
   710|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   711| 
   712|     matrix_dir = tmp_path / "matrix_manifest_output"
   713|     matrix_dir.mkdir()
   714|     _write_csv(
   715|         matrix_dir / "matrix_output_manifest.csv", MATRIX_MANIFEST_FIELDS,
   716|         [{f: "" for f in MATRIX_MANIFEST_FIELDS}],
   717|     )
   718|     matrix_path = matrix_dir / "project_mean_file_pair_jaccard_matrix.csv"
   719|     _write_csv(matrix_path, MATRIX_OUTPUT_FIELDS, [{f: "" for f in MATRIX_OUTPUT_FIELDS}] * 7)
   720|     assert not (tmp_path / "project_mean_file_pair_jaccard_matrix.csv").exists()
   721| 
   722|     _run_main(monkeypatch, [
   723|         "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
   724|         "--matrix-manifest", str(matrix_dir / "matrix_output_manifest.csv"),
   725|     ])
   726| 
   727|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   728|     entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "project_mean_file_pair_jaccard_matrix")
   729|     assert entry["present"] is True
   730|     assert entry["path"] == str(matrix_path)
   731|     assert entry["row_count"] == 7
   732| 
   733| 
   734| def test_evidence_map_findings_entry_has_a_real_path(tmp_path, monkeypatch):
   735|     """Regression test for a PR review finding: build_evidence_map() looked up
   736|     output_paths["findings_json"], but main() writes that entry under the key
   737|     "governance_findings" -- the evidence-map entry for governance_findings
   738|     reported path: null and present: true simultaneously, since the .get()
   739|     silently returned nothing for the mismatched key. path must resolve to
   740|     the real governance_findings.json file that was actually written."""
   741|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   742|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   743|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   744|     entry = next(a for a in evidence_map["artifacts"] if a["artifact_id"] == "governance_findings")
   745|     assert entry["path"] is not None
   746|     assert Path(entry["path"]).name == "governance_findings.json"
   747|     assert Path(entry["path"]).exists()
   748| 
   749| 
   750| def test_manifest_output_artifact_ids_match_evidence_map_artifact_ids(tmp_path, monkeypatch):
   751|     """Regression test for a PR review finding: governance_package_manifest.json's
   752|     outputs[].artifact_id values (e.g. "domain_summary_csv") used a different
   753|     vocabulary than governance_evidence_map.json's artifacts[].artifact_id
   754|     values (e.g. "governance_domain_summary") for the exact same files, so a
   755|     consumer joining provenance/size data from the manifest to navigation
   756|     metadata in the evidence map by artifact_id could not resolve them."""
   757|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   758|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   759|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   760|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   761|     manifest_output_ids = {o["artifact_id"] for o in manifest["outputs"]}
   762|     evidence_map_ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
   763|     assert manifest_output_ids <= evidence_map_ids, manifest_output_ids - evidence_map_ids
   764| 
   765| 
   766| def test_manifest_input_artifact_ids_match_evidence_map_artifact_ids(tmp_path, monkeypatch):
   767|     """Regression test for a PR review finding: governance_package_manifest.json's
   768|     inputs[].artifact_id values used short CLI-flag-derived names ("summary",
   769|     "pooled", "union_inventory", etc.) while governance_evidence_map.json uses
   770|     the canonical artifact_id for the same source CSVs ("cross_segment_summary",
   771|     "cross_segment_pooled", "cross_segment_union_inventory", etc.), so a
   772|     consumer joining manifest input provenance to evidence-map navigation
   773|     metadata by artifact_id could not resolve them, even though the output
   774|     side had already been made canonical."""
   775|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   776|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   777|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   778|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   779|     manifest_input_ids = {i["artifact_id"] for i in manifest["inputs"]}
   780|     evidence_map_ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
   781|     assert manifest_input_ids <= evidence_map_ids, manifest_input_ids - evidence_map_ids
   782| 
   783| 
   784| def test_evidence_map_related_artifacts_use_artifact_ids_not_filenames(tmp_path, monkeypatch):
   785|     """Regression test for a PR review finding: related_artifacts entries
   786|     hard-coded filenames-with-extension (e.g. 'cross_segment_pooled.csv')
   787|     instead of the corresponding artifact_id ('cross_segment_pooled'), so a
   788|     consumer traversing the evidence map by artifact_id could not resolve
   789|     the links."""
   790|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   791|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   792|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   793|     ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
   794|     for a in evidence_map["artifacts"]:
   795|         for related in a["related_artifacts"]:
   796|             assert related in ids, f"{a['artifact_id']}.related_artifacts references unknown id {related!r}"
   797|             assert not related.endswith((".csv", ".json", ".md")), (
   798|                 f"{a['artifact_id']}.related_artifacts contains a filename, not an artifact_id: {related!r}"
   799|             )
   800| 
   801| 
   802| # ---------------------------------------------------------------------------
   803| # D-023: governance_file_inventory.json (live file-availability inventory)
   804| # ---------------------------------------------------------------------------
   805| 
   806| def test_file_inventory_written_and_registered_in_manifest_and_evidence_map(tmp_path, monkeypatch):
   807|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   808|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   809|     assert (tmp_path / "governance_file_inventory.json").exists()
   810|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
   811|     manifest_ids = {o["artifact_id"] for o in manifest["outputs"]}
   812|     assert "governance_file_inventory" in manifest_ids
   813|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   814|     em_ids = {a["artifact_id"] for a in evidence_map["artifacts"]}
   815|     assert "governance_file_inventory" in em_ids
   816| 
   817| 
   818| def test_file_inventory_is_empty_when_no_undiscovered_files_present(tmp_path, monkeypatch):
   819|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   820|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   821|     fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
   822|     assert fi["files"] == []
   823|     assert fi["file_count"] == 0
   824| 
   825| 
   826| def test_file_inventory_surfaces_an_undiscovered_sibling_csv(tmp_path, monkeypatch):
   827|     """The motivating scenario: a real pipeline export sits beside
   828|     cross_segment_summary.csv but has no artifact_id registered anywhere in
   829|     the evidence-package layer yet -- the live scan must surface it with real
   830|     header/row-count, computed fresh from disk, not from a hand-maintained
   831|     list. Uses a fictitious filename: pattern_reuse_summary_by_domain.csv
   832|     (this scenario's original example) was promoted to its own
   833|     governance_evidence_map.json artifact by D-024, so it is no longer a
   834|     valid stand-in for "undiscovered"."""
   835|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   836|     _write_csv(
   837|         tmp_path / "some_future_pipeline_export.csv",
   838|         ["domain", "reuse_bucket", "n_patterns"],
   839|         [{"domain": "line_styles", "reuse_bucket": "corpus_wide", "n_patterns": "5"}],
   840|     )
   841|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   842|     fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
   843|     assert fi["file_count"] == 1
   844|     entry = fi["files"][0]
   845|     assert entry["filename"] == "some_future_pipeline_export.csv"
   846|     assert entry["row_count"] == 1
   847|     assert [c["name"] for c in entry["columns"]] == ["domain", "reuse_bucket", "n_patterns"]
   848|     assert entry["columns"][2]["inferred_dtype"] == "integer"
   849|     assert "narrative" in entry and entry["narrative"]
   850| 
   851| 
   852| def test_file_inventory_never_flags_this_runs_own_outputs_as_undiscovered(tmp_path, monkeypatch):
   853|     """--out defaults to the same directory as --summary in these fixtures --
   854|     the scan must exclude this generator's own CSV/JSON/MD outputs (already
   855|     tracked via input_paths/output_paths/sibling_paths), not just the two
   856|     required input CSVs."""
   857|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   858|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   859|     fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
   860|     flagged = {f["filename"] for f in fi["files"]}
   861|     assert "governance_domain_summary.csv" not in flagged
   862|     assert "governance_client_summary.csv" not in flagged
   863|     assert "governance_bc_summary.csv" not in flagged
   864| 
   865| 
   866| def test_file_inventory_borrows_interpretation_from_matrix_output_manifest(tmp_path, monkeypatch):
   867|     """When a discovered file's name matches a matrix_name already documented
   868|     in matrix_output_manifest.csv, the narrative must reuse that row's own
   869|     interpretation text rather than falling back to a generic sentence --
   870|     the 'interpretation field pattern already used in the matrix CSVs'.
   871| 
   872|     Uses a fictitious matrix filename: project_mean_file_pair_jaccard_matrix.csv
   873|     (this scenario's original example) was promoted to its own
   874|     governance_evidence_map.json artifact by D-024, so it is no longer picked
   875|     up by the generic undiscovered-file scan this test exercises."""
   876|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   877|     matrix_manifest_path = tmp_path / "matrix_output_manifest.csv"
   878|     _write_csv(
   879|         matrix_manifest_path,
   880|         ["matrix_name", "governance_role", "view_scope", "source_file", "source_grain",
   881|          "metric", "identity_unit", "aggregation_method", "interpretation",
   882|          "known_limitations", "executed_utc"],
   883|         [{
   884|             "matrix_name": "project_hypothetical_future_matrix.csv", "governance_role": "Project",
   885|             "view_scope": "all,used", "source_file": "cross_segment_summary.csv",
   886|             "source_grain": "segment_pair/domain", "metric": "mean_file_pair_jaccard",
   887|             "identity_unit": "file join_hash set",
   888|             "aggregation_method": "Mean of pairwise file Jaccard comparisons",
   889|             "interpretation": "Are individual files typically similar across project groups?",
   890|             "known_limitations": "Not equivalent to union_jaccard.",
   891|             "executed_utc": "2026-07-16T00:00:00Z",
   892|         }],
   893|     )
   894|     _write_csv(
   895|         tmp_path / "project_hypothetical_future_matrix.csv",
   896|         ["matrix_name", "row_id", "column_id", "view_scope", "domain", "metric",
   897|          "value", "value_status", "self_comparison", "interpretation", "executed_utc"],
   898|         [{
   899|             "matrix_name": "project_hypothetical_future_matrix.csv", "row_id": "proj_a",
   900|             "column_id": "proj_b", "view_scope": "all", "domain": "ALL_DOMAINS",
   901|             "metric": "mean_file_pair_jaccard", "value": "0.5", "value_status": "ok",
   902|             "self_comparison": "false", "interpretation": "x", "executed_utc": "2026-07-16T00:00:00Z",
   903|         }],
   904|     )
   905|     _run_main(monkeypatch, [
   906|         "--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path),
   907|         "--matrix-manifest", str(matrix_manifest_path),
   908|     ])
   909|     fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
   910|     entry = next(f for f in fi["files"] if f["filename"] == "project_hypothetical_future_matrix.csv")
   911|     assert "Are individual files typically similar across project groups?" in entry["narrative"]
   912| 
   913| 
   914| def test_no_emit_evidence_package_suppresses_file_inventory(tmp_path, monkeypatch):
   915|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   916|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   917|                             "--out", str(tmp_path), "--no-emit-evidence-package"])
   918|     assert not (tmp_path / "governance_file_inventory.json").exists()
   919| 
   920| 
   921| def test_stale_file_inventory_removed_when_evidence_package_turned_off_between_runs(tmp_path, monkeypatch):
   922|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   923|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   924|     assert (tmp_path / "governance_file_inventory.json").exists()
   925|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   926|                             "--out", str(tmp_path), "--no-emit-evidence-package"])
   927|     assert not (tmp_path / "governance_file_inventory.json").exists()
   928| 
   929| 
   930| def test_file_inventory_surfaces_regardless_of_interpretation_layer_flag(tmp_path, monkeypatch):
   931|     """governance_file_inventory.json is gated by --emit-evidence-package only,
   932|     not --emit-interpretation-layer (that flag controls governance_brief.md's
   933|     section, a separate rendering of the same already-scanned data).
   934| 
   935|     Uses a fictitious filename (see test_file_inventory_surfaces_an_undiscovered_sibling_csv
   936|     for why pattern_reuse_summary_by_domain.csv no longer qualifies as "undiscovered")."""
   937|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   938|     _write_csv(
   939|         tmp_path / "some_future_pipeline_export.csv",
   940|         ["domain", "reuse_bucket", "n_patterns"],
   941|         [{"domain": "line_styles", "reuse_bucket": "corpus_wide", "n_patterns": "5"}],
   942|     )
   943|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
   944|                             "--out", str(tmp_path), "--no-emit-interpretation-layer"])
   945|     fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
   946|     assert fi["file_count"] == 1
   947| 
   948| 
   949| # ---------------------------------------------------------------------------
   950| # D-024: escalation-target files (the large cross_segment_* siblings this
   951| # generator never parses, named in docs/governance/governance_interpretation_guide.md's
   952| # escalation section) get their own governance_evidence_map.json artifact
   953| # with real header/row_count, instead of only the generic file-inventory
   954| # scan bucket.
   955| # ---------------------------------------------------------------------------
   956| 
   957| def test_escalation_target_files_get_real_shape_in_evidence_map_not_generic_inventory(tmp_path, monkeypatch):
   958|     """The four files this generator's own module docstring lists as "not yet
   959|     consumed directly" -- comparison_registry.csv, cross_segment_file_pairs.csv,
   960|     pattern_reuse_summary_by_domain.csv, project_mean_file_pair_jaccard_matrix.csv
   961|     -- must each resolve in governance_evidence_map.json with the real column
   962|     header and row count read straight off disk, and must NOT also appear in
   963|     governance_file_inventory.json's generic undiscovered-file bucket (that
   964|     would be a second, redundant narrative layer for the same file)."""
   965|     summary_path, pooled_path = _minimal_fixture(tmp_path)
   966|     _write_csv(
   967|         tmp_path / "comparison_registry.csv", COMPARISON_REGISTRY_FIELDS,
   968|         [{f: "" for f in COMPARISON_REGISTRY_FIELDS}],
   969|     )
   970|     _write_csv(
   971|         tmp_path / "cross_segment_file_pairs.csv",
   972|         ["segment_id_a", "segment_id_b", "domain", "join_hash"],
   973|         [{"segment_id_a": "imperial|A", "segment_id_b": "imperial|B",
   974|           "domain": "line_styles", "join_hash": "abc123"}] * 3,
   975|     )
   976|     _write_csv(
   977|         tmp_path / "pattern_reuse_summary_by_domain.csv", REUSE_SUMMARY_FIELDS,
   978|         [{f: "" for f in REUSE_SUMMARY_FIELDS}] * 2,
   979|     )
   980|     _write_csv(
   981|         tmp_path / "project_mean_file_pair_jaccard_matrix.csv", MATRIX_OUTPUT_FIELDS,
   982|         [{f: "" for f in MATRIX_OUTPUT_FIELDS}] * 4,
   983|     )
   984| 
   985|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path), "--out", str(tmp_path)])
   986| 
   987|     evidence_map = json.loads((tmp_path / "governance_evidence_map.json").read_text(encoding="utf-8"))
   988|     by_id = {a["artifact_id"]: a for a in evidence_map["artifacts"]}
   989| 
   990|     fp = by_id["cross_segment_file_pairs"]
   991|     assert fp["present"] is True
   992|     assert fp["row_count"] == 3
   993|     assert [c["name"] for c in fp["columns"]] == ["segment_id_a", "segment_id_b", "domain", "join_hash"]
   994| 
   995|     cr = by_id["comparison_registry"]
   996|     assert cr["present"] is True
   997|     assert cr["row_count"] == 1
   998|     assert [c["name"] for c in cr["columns"]] == COMPARISON_REGISTRY_FIELDS
   999| 
  1000|     prsd = by_id["pattern_reuse_summary_by_domain"]
  1001|     assert prsd["present"] is True
  1002|     assert prsd["row_count"] == 2
  1003|     assert [c["name"] for c in prsd["columns"]] == REUSE_SUMMARY_FIELDS
  1004|     assert "can_answer" in prsd and prsd["can_answer"]
  1005|     assert "cannot_answer" in prsd and prsd["cannot_answer"]
  1006| 
  1007|     pmfp = by_id["project_mean_file_pair_jaccard_matrix"]
  1008|     assert pmfp["present"] is True
  1009|     assert pmfp["row_count"] == 4
  1010|     assert [c["name"] for c in pmfp["columns"]] == MATRIX_OUTPUT_FIELDS
  1011|     assert "can_answer" in pmfp and pmfp["can_answer"]
  1012|     assert "cannot_answer" in pmfp and pmfp["cannot_answer"]
  1013| 
  1014|     # Not duplicated into the generic file-inventory scan bucket.
  1015|     fi = json.loads((tmp_path / "governance_file_inventory.json").read_text(encoding="utf-8"))
  1016|     flagged = {f["filename"] for f in fi["files"]}
  1017|     assert flagged.isdisjoint({
  1018|         "comparison_registry.csv", "cross_segment_file_pairs.csv",
  1019|         "pattern_reuse_summary_by_domain.csv", "project_mean_file_pair_jaccard_matrix.csv",
  1020|     })
  1021| 
  1022| 
  1023| def test_cli_accepts_policy_dir_and_package_schema_version_as_inert(tmp_path, monkeypatch):
  1024|     summary_path, pooled_path = _minimal_fixture(tmp_path)
  1025|     policy_dir = tmp_path / "some_policy_dir"
  1026|     _run_main(monkeypatch, ["--summary", str(summary_path), "--pooled", str(pooled_path),
  1027|                             "--out", str(tmp_path), "--policy-dir", str(policy_dir),
  1028|                             "--package-schema-version", "2.0"])
  1029|     manifest = json.loads((tmp_path / "governance_package_manifest.json").read_text(encoding="utf-8"))
  1030|     assert manifest["policy_profiles"]["policy_dir"] == str(policy_dir)
  1031|     assert manifest["package_schema_version"] == "2.0"
  1032|     # No crash, and the domain/client CSV outputs are still produced normally.
  1033|     assert (tmp_path / "governance_domain_summary.csv").exists()
  1034| 
  1035| 
```
