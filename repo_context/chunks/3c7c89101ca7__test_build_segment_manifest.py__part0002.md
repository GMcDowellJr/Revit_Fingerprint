# Chunk of tests/test_build_segment_manifest.py

- Source relative path: `tests/test_build_segment_manifest.py`
- Chunk: 2 of 4
- Original line range: 505-1019
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_main_blocks_on_blank_export_run_id, test_main_fails_on_missing_columns_even_with_no_data_rows, test_level2_project_bundle_with_parent_bundle_runs_enabled, test_level2_project_registration_without_flag, test_mixed_role_client_segment_stays_reference, test_single_child_suppression_still_fires, _disc_rows, test_discipline_cut_level3_segment_generated, test_discipline_cut_level4_segment_generated, test_discipline_cut_extra_dimensions_populated, test_discipline_label_top_level_field_blank_for_non_discipline_segments, test_discipline_label_top_level_field_populated_in_mixed_cut, test_discipline_cut_level3_purpose, test_discipline_cut_level3_label, test_blank_discipline_does_not_generate_discipline_cut, test_no_discipline_column_rows_not_broken, test_discipline_cut_not_required_column_now_blocks, test_discipline_cut_level3_bundle_not_demoted_by_children, test_discipline_cut_level4_bundle_not_affected, test_multi_child_parent_not_demoted_redundant_single_child, test_single_child_same_hash_still_demoted, test_matching_child_demotes_parent_even_with_other_nonmatching_children, test_client_discipline_leaf_purpose_container, test_client_discipline_leaf_label_container, test_client_discipline_leaf_purpose_template, test_client_discipline_leaf_purpose_project, test_registry_first_run_no_existing_file_unaffected, test_registry_preserves_output_folder_across_runs_when_unchanged, test_registry_preserves_status_when_population_hash_unchanged, test_registry_resets_status_when_population_hash_changes, test_registry_new_segment_gets_unique_folder_not_colliding_with_carryover, _manifest_row, test_registry_resets_status_when_run_type_changes, test_registry_reserves_dropped_segment_folder_from_new_reuse, test_registry_drops_removed_segment_ids_with_warning, test_client_discipline_leaf_no_empty_purpose
- Source SHA-256: 9f3ece62e3859182daaa40d64fa48a48dce0364f40520d18b071b30a096c99c4
- Starts inside symbol: no
- Ends inside symbol: no

```
   505| def test_main_blocks_on_blank_export_run_id(tmp_path, capsys):
   506|     # A blank export_run_id must now BLOCK the entire build (not warn and
   507|     # silently exclude the row) -- see the "Required-field blocking" tests
   508|     # further down for the full per-field sweep.
   509|     meta = tmp_path / "file_metadata.csv"
   510|     with meta.open("w", newline="") as f:
   511|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES)
   512|         w.writeheader()
   513|         for row in VALID_ROWS[:3]:
   514|             w.writerow(row)
   515|         bad = dict(VALID_ROWS[3]); bad["export_run_id"] = ""
   516|         w.writerow(bad)
   517| 
   518|     out_dir = tmp_path / "out"
   519|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
   520|     assert rc == 1
   521|     assert not (out_dir / "segment_manifest.csv").exists()
   522|     captured = capsys.readouterr()
   523|     assert "BLOCKED" in captured.err
   524|     assert "field=export_run_id" in captured.err
   525|     assert "reason=missing_value" in captured.err
   526| 
   527| 
   528| def test_main_fails_on_missing_columns_even_with_no_data_rows(tmp_path):
   529|     # Header-only file missing governance_role must still fail, not silently succeed.
   530|     meta = tmp_path / "file_metadata.csv"
   531|     with meta.open("w", newline="") as f:
   532|         w = csv.DictWriter(f, fieldnames=["export_run_id", "unit_system", "client_label"])
   533|         w.writeheader()
   534|         # No data rows — previously validation was skipped in this branch.
   535| 
   536|     out_dir = tmp_path / "out"
   537|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
   538|     assert rc == 1
   539|     assert not (out_dir / "segment_manifest.csv").exists()
   540| 
   541| def test_level2_project_bundle_with_parent_bundle_runs_enabled():
   542|     rows = (
   543|         [_meta_row(f"k{i:02d}", "imperial", "ClientAlpha", "Project") for i in range(1, 4)]
   544|         + [_meta_row(f"r{i:02d}", "imperial", "Renown", "Project") for i in range(1, 4)]
   545|     )
   546|     segs = _build_segments(rows, min_files=3, enable_parent_bundle_runs=True)
   547|     parent = next(r for r in segs if r["segment_id"] == "imperial|Project")
   548|     assert parent["run_type"] == "bundle"
   549| 
   550| 
   551| def test_level2_project_registration_without_flag():
   552|     rows = (
   553|         [_meta_row(f"k{i:02d}", "imperial", "ClientAlpha", "Project") for i in range(1, 4)]
   554|         + [_meta_row(f"r{i:02d}", "imperial", "Renown", "Project") for i in range(1, 4)]
   555|     )
   556|     segs = _build_segments(rows, min_files=3)
   557|     parent = next(r for r in segs if r["segment_id"] == "imperial|Project")
   558|     assert parent["run_type"] == "registration"
   559| 
   560| 
   561| def test_mixed_role_client_segment_stays_reference():
   562|     rows = [
   563|         _meta_row("s01", "imperial", "ClientBeta", "Project"),
   564|         _meta_row("s02", "imperial", "ClientBeta", "Project"),
   565|         _meta_row("s03", "imperial", "ClientBeta", "Project"),
   566|         _meta_row("s04", "imperial", "ClientBeta", "Template"),
   567|         _meta_row("s05", "imperial", "ClientBeta", "Template"),
   568|         _meta_row("s06", "imperial", "ClientBeta", "Template"),
   569|     ]
   570|     segs = _build_segments(rows, min_files=3, enable_parent_bundle_runs=True)
   571|     mixed = next(r for r in segs if r["segment_id"] == "imperial|ClientBeta")
   572|     assert mixed["governance_role"] == ""
   573|     assert mixed["run_type"] == "registration"
   574| 
   575| 
   576| def test_single_child_suppression_still_fires():
   577|     rows = [_meta_row(f"k{i:02d}", "imperial", "ClientAlpha", "Project") for i in range(1, 4)]
   578|     segs = _build_segments(rows, min_files=3, enable_parent_bundle_runs=True)
   579|     parent = next(r for r in segs if r["segment_id"] == "imperial|Project")
   580|     assert parent["run_type"] == "registration"
   581|     assert "redundant_single_child" in (parent.get("notes") or "")
   582| 
   583| 
   584| # ---------------------------------------------------------------------------
   585| # Discipline-cut dimension tests
   586| # ---------------------------------------------------------------------------
   587| 
   588| def _disc_rows():
   589|     """Multi-client, multi-discipline Container corpus for discipline tests."""
   590|     return (
   591|         [_meta_row(f"ka{i:02d}", "imperial", "ClientAlpha", "Container", "Architectural") for i in range(4)]
   592|         + [_meta_row(f"ke{i:02d}", "imperial", "ClientAlpha", "Container", "Electrical") for i in range(3)]
   593|         + [_meta_row(f"ra{i:02d}", "imperial", "Renown", "Container", "Architectural") for i in range(3)]
   594|         # rows with no discipline_label — must not generate discipline cuts
   595|         + [_meta_row(f"nx{i:02d}", "imperial", "ClientAlpha", "Project") for i in range(3)]
   596|     )
   597| 
   598| 
   599| def test_discipline_cut_level3_segment_generated():
   600|     segs = _build_segments(_disc_rows(), min_files=3)
   601|     seg_ids = {r["segment_id"] for r in segs}
   602|     assert "imperial|Container|Architectural" in seg_ids
   603|     assert "imperial|Container|Electrical" in seg_ids
   604| 
   605| 
   606| def test_discipline_cut_level4_segment_generated():
   607|     segs = _build_segments(_disc_rows(), min_files=3)
   608|     seg_ids = {r["segment_id"] for r in segs}
   609|     assert "imperial|Container|ClientAlpha|Architectural" in seg_ids
   610|     assert "imperial|Container|ClientAlpha|Electrical" in seg_ids
   611| 
   612| 
   613| def test_discipline_cut_extra_dimensions_populated():
   614|     segs = _build_segments(_disc_rows(), min_files=3)
   615|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
   616|     assert seg["extra_dimensions"] == "discipline_label=Architectural"
   617|     assert seg["client_label"] == ""
   618|     assert seg["discipline_label"] == "Architectural"
   619| 
   620| 
   621| def test_discipline_label_top_level_field_blank_for_non_discipline_segments():
   622|     segs = _build_segments(_disc_rows(), min_files=3)
   623|     # A pure governance segment has no discipline cut — field must be blank, not absent.
   624|     container = next(r for r in segs if r["segment_id"] == "imperial|Container")
   625|     assert container["discipline_label"] == ""
   626|     # A client-only cut also has no discipline.
   627|     clientalpha = next(r for r in segs if r["segment_id"] == "imperial|ClientAlpha")
   628|     assert clientalpha["discipline_label"] == ""
   629| 
   630| 
   631| def test_discipline_label_top_level_field_populated_in_mixed_cut():
   632|     segs = _build_segments(_disc_rows(), min_files=3)
   633|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha|Architectural")
   634|     assert seg["discipline_label"] == "Architectural"
   635|     assert seg["client_label"] == "ClientAlpha"
   636| 
   637| 
   638| def test_discipline_cut_level3_purpose():
   639|     # With two clients contributing, the discipline-only level-3 segment should NOT be
   640|     # redundant_single_child — it has two distinct child populations (ClientAlpha + Renown).
   641|     segs = _build_segments(_disc_rows(), min_files=3)
   642|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
   643|     assert seg["segment_purpose"] == "discipline_coordination"
   644| 
   645| 
   646| def test_discipline_cut_level3_label():
   647|     segs = _build_segments(_disc_rows(), min_files=3)
   648|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
   649|     assert seg["segment_label"] == "Architectural coordination files"
   650| 
   651| 
   652| def test_blank_discipline_does_not_generate_discipline_cut():
   653|     segs = _build_segments(_disc_rows(), min_files=3)
   654|     seg_ids = {r["segment_id"] for r in segs}
   655|     # Rows with blank discipline contribute to governance and client cuts only
   656|     assert "imperial|Project" in seg_ids
   657|     # No discipline cut that includes blank discipline
   658|     disc_segs = [r for r in segs if "discipline_label=" in r.get("extra_dimensions", "")]
   659|     for s in disc_segs:
   660|         assert s["extra_dimensions"] != "discipline_label="
   661| 
   662| 
   663| def test_no_discipline_column_rows_not_broken():
   664|     # Rows lacking discipline_label entirely must not generate discipline cuts.
   665|     rows = [
   666|         {"export_run_id": f"r{i:02d}", "unit_system": "imperial",
   667|          "client_label": "Acme", "governance_role": "Container"}
   668|         for i in range(3)
   669|     ]
   670|     segs = _build_segments(rows, min_files=3)
   671|     disc_segs = [r for r in segs if "discipline_label=" in r.get("extra_dimensions", "")]
   672|     assert disc_segs == []
   673| 
   674| 
   675| def test_discipline_cut_not_required_column_now_blocks(tmp_path, capsys):
   676|     # Under the explicit-metadata contract, discipline_label is a required
   677|     # row value -- a metadata file that lacks the column entirely means every
   678|     # row is missing it, which now blocks the build (this supersedes the old
   679|     # "discipline_label is optional" contract).
   680|     meta = tmp_path / "file_metadata.csv"
   681|     fieldnames = ["export_run_id", "unit_system", "client_label", "governance_role"]
   682|     with meta.open("w", newline="") as f:
   683|         w = csv.DictWriter(f, fieldnames=fieldnames)
   684|         w.writeheader()
   685|         for row in ROWS:
   686|             w.writerow({k: row.get(k, "") for k in fieldnames})
   687|     out_dir = tmp_path / "out"
   688|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
   689|     assert rc == 1
   690|     assert not (out_dir / "segment_manifest.csv").exists()
   691|     captured = capsys.readouterr()
   692|     assert "field=discipline_label" in captured.err
   693| 
   694| 
   695| # ---------------------------------------------------------------------------
   696| # Bug 2: level-3+ governance-role segments must not be demoted by "has children"
   697| # ---------------------------------------------------------------------------
   698| 
   699| def test_discipline_cut_level3_bundle_not_demoted_by_children():
   700|     # imperial|Container|Architectural has two client children (ClientAlpha + Renown).
   701|     # The "has children → registration" logic must not fire for level-3 governance-role segments.
   702|     segs = _build_segments(_disc_rows(), min_files=3)
   703|     arch = next(r for r in segs if r["segment_id"] == "imperial|Container|Architectural")
   704|     assert arch["run_type"] == "bundle", (
   705|         f"Expected bundle, got {arch['run_type']}; "
   706|         "level-3 scoped segments must not be demoted by child presence"
   707|     )
   708| 
   709| 
   710| def test_discipline_cut_level4_bundle_not_affected():
   711|     # Level-4 combined client+discipline segments have no children and must be bundle.
   712|     segs = _build_segments(_disc_rows(), min_files=3)
   713|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha|Architectural")
   714|     assert seg["run_type"] == "bundle"
   715| 
   716| 
   717| # ---------------------------------------------------------------------------
   718| # Bug 3: redundant_single_child must not fire when a parent has multiple children
   719| # ---------------------------------------------------------------------------
   720| 
   721| def test_multi_child_parent_not_demoted_redundant_single_child():
   722|     # imperial|Container|ClientAlpha has both Architectural and Electrical children.
   723|     # redundant_single_child must NOT fire.
   724|     segs = _build_segments(_disc_rows(), min_files=3)
   725|     clientalpha_container = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha")
   726|     assert "redundant_single_child" not in (clientalpha_container.get("notes") or ""), (
   727|         "Multi-child parent must not be flagged redundant_single_child"
   728|     )
   729|     assert clientalpha_container["run_type"] != "registration" or "redundant_single_child" not in (clientalpha_container.get("notes") or "")
   730| 
   731| 
   732| def test_single_child_same_hash_still_demoted():
   733|     # imperial|Container|Electrical has only one child (ClientAlpha|Electrical) with the same population.
   734|     # redundant_single_child SHOULD fire here.
   735|     segs = _build_segments(_disc_rows(), min_files=3)
   736|     elec = next(r for r in segs if r["segment_id"] == "imperial|Container|Electrical")
   737|     assert "redundant_single_child" in (elec.get("notes") or ""), (
   738|         "Single child with same population_hash must still trigger redundant_single_child"
   739|     )
   740| 
   741| 
   742| def test_matching_child_demotes_parent_even_with_other_nonmatching_children():
   743|     # A business_center-scoped Container pool where every row also happens to
   744|     # share the same (real, non-blank) client_label, so the client+bc child
   745|     # is a byte-identical duplicate of the bc-only parent, AND a subset of
   746|     # those rows also carry a discipline_label (so a second, non-matching
   747|     # discipline-cut child also exists as a sibling). The parent must demote
   748|     # regardless of the extra non-matching sibling.
   749|     rows = (
   750|         [{"export_run_id": f"s{i:02d}", "unit_system": "imperial", "governance_role": "Container",
   751|           "client_label": "Acme", "business_center_label": "Shared", "discipline_label": "architectural"}
   752|          for i in range(2)]
   753|         + [{"export_run_id": f"s{i:02d}", "unit_system": "imperial", "governance_role": "Container",
   754|             "client_label": "Acme", "business_center_label": "Shared"}
   755|            for i in range(2, 5)]
   756|     )
   757|     segs = _build_segments(rows, min_files=3)
   758|     parent = next(r for r in segs if r["segment_id"] == "imperial|Container|Shared")
   759|     twin = next(r for r in segs if r["segment_id"] == "imperial|Container|Acme|Shared")
   760|     disc_child = next(r for r in segs if r["segment_id"] == "imperial|Container|architectural|Shared")
   761| 
   762|     assert parent["file_count"] == "5"
   763|     assert twin["file_count"] == "5"
   764|     assert disc_child["file_count"] == "2"
   765| 
   766|     assert parent["run_type"] == "registration", (
   767|         "Parent with a byte-identical child (the client+bc cut) must demote "
   768|         "even though it also has a second, non-matching discipline-cut child"
   769|     )
   770|     assert "redundant_single_child" in (parent.get("notes") or "")
   771|     # The pointer must name the matching client+bc child, not the non-matching sibling.
   772|     assert "imperial|Container|Acme|Shared" in parent["notes"]
   773|     assert disc_child["file_count"] != parent["file_count"]
   774| 
   775| 
   776| # ---------------------------------------------------------------------------
   777| # Level-4 client+discipline leaf segment purpose and label
   778| # ---------------------------------------------------------------------------
   779| 
   780| def test_client_discipline_leaf_purpose_container():
   781|     segs = _build_segments(_disc_rows(), min_files=3)
   782|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha|Architectural")
   783|     assert seg["segment_purpose"] == "client_discipline_coordination"
   784| 
   785| 
   786| def test_client_discipline_leaf_label_container():
   787|     segs = _build_segments(_disc_rows(), min_files=3)
   788|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha|Architectural")
   789|     assert seg["segment_label"] == "ClientAlpha Architectural coordination files"
   790| 
   791| 
   792| def test_client_discipline_leaf_purpose_template():
   793|     rows = (
   794|         [_meta_row(f"t{i:02d}", "imperial", "ClientAlpha", "Template", "Architectural") for i in range(3)]
   795|         + [_meta_row(f"u{i:02d}", "imperial", "Renown", "Template", "Architectural") for i in range(3)]
   796|     )
   797|     segs = _build_segments(rows, min_files=3)
   798|     seg = next(r for r in segs if r["segment_id"] == "imperial|Template|ClientAlpha|Architectural")
   799|     assert seg["segment_purpose"] == "client_discipline_standard_anchor"
   800|     assert seg["segment_label"] == "ClientAlpha Architectural templates — standards as authored"
   801| 
   802| 
   803| def test_client_discipline_leaf_purpose_project():
   804|     rows = (
   805|         [_meta_row(f"p{i:02d}", "imperial", "ClientAlpha", "Project", "Architectural") for i in range(3)]
   806|         + [_meta_row(f"q{i:02d}", "imperial", "Renown", "Project", "Architectural") for i in range(3)]
   807|     )
   808|     segs = _build_segments(rows, min_files=3)
   809|     seg = next(r for r in segs if r["segment_id"] == "imperial|Project|ClientAlpha|Architectural")
   810|     assert seg["segment_purpose"] == "client_discipline_practice"
   811|     assert seg["segment_label"] == "ClientAlpha Architectural projects — standards as practiced"
   812| 
   813| 
   814| # ---------------------------------------------------------------------------
   815| # Registry stability + population-hash-based status preservation
   816| # ---------------------------------------------------------------------------
   817| 
   818| def test_registry_first_run_no_existing_file_unaffected():
   819|     # Regression guard: calling _build_registry with no existing_registry (or
   820|     # existing_registry=None explicitly) must be byte-for-byte identical to
   821|     # the pre-change behavior.
   822|     segs = _build_segments(ROWS, min_files=3)
   823|     reg_default = _build_registry(segs)
   824|     reg_explicit_none = _build_registry(segs, existing_registry=None)
   825|     assert reg_default == reg_explicit_none
   826|     for r in reg_default:
   827|         assert r["status"] == "pending"
   828|         assert r["last_run_utc"] == ""
   829|     clientalpha_reg = next(r for r in reg_default if r["segment_id"] == "imperial|Project|ClientAlpha")
   830|     assert clientalpha_reg["output_folder"] == "imperial_project_clientalpha"
   831| 
   832| 
   833| def test_registry_preserves_output_folder_across_runs_when_unchanged():
   834|     segs = _build_segments(ROWS, min_files=3)
   835|     reg1 = _build_registry(segs)
   836|     reg2 = _build_registry(segs, existing_registry=reg1)
   837|     folders1 = {r["segment_id"]: r["output_folder"] for r in reg1}
   838|     folders2 = {r["segment_id"]: r["output_folder"] for r in reg2}
   839|     assert folders1 == folders2
   840| 
   841| 
   842| def test_registry_preserves_status_when_population_hash_unchanged():
   843|     segs = _build_segments(ROWS, min_files=3)
   844|     reg1 = _build_registry(segs)
   845|     for r in reg1:
   846|         if r["segment_id"] == "imperial|Project|ClientAlpha":
   847|             r["status"] = "complete"
   848|             r["last_run_utc"] = "2026-01-01T00:00:00Z"
   849| 
   850|     reg2 = _build_registry(segs, existing_registry=reg1)
   851|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
   852|     assert clientalpha2["status"] == "complete"
   853|     assert clientalpha2["last_run_utc"] == "2026-01-01T00:00:00Z"
   854|     assert clientalpha2["output_folder"] == "imperial_project_clientalpha"
   855| 
   856| 
   857| def test_registry_resets_status_when_population_hash_changes():
   858|     segs1 = _build_segments(ROWS, min_files=3)
   859|     reg1 = _build_registry(segs1)
   860|     for r in reg1:
   861|         if r["segment_id"] == "imperial|Project|ClientAlpha":
   862|             r["status"] = "complete"
   863|             r["last_run_utc"] = "2026-01-01T00:00:00Z"
   864| 
   865|     rows2 = ROWS + [_meta_row("r11", "imperial", "ClientAlpha", "Project")]
   866|     segs2 = _build_segments(rows2, min_files=3)
   867|     reg2 = _build_registry(segs2, existing_registry=reg1)
   868|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
   869| 
   870|     clientalpha1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|ClientAlpha")
   871|     assert clientalpha2["population_hash"] != clientalpha1["population_hash"]
   872|     assert clientalpha2["status"] == "pending"
   873|     assert clientalpha2["last_run_utc"] == ""
   874|     assert "population_changed" in clientalpha2["notes"]
   875|     # Folder name must remain stable even though status reset.
   876|     assert clientalpha2["output_folder"] == clientalpha1["output_folder"]
   877| 
   878| 
   879| def test_registry_new_segment_gets_unique_folder_not_colliding_with_carryover():
   880|     # First run: two distinct clients whose sanitized names collide — not
   881|     # case variants (those merge upstream in _normalize_rows() and can no
   882|     # longer produce two segment_ids to collide in the first place; see
   883|     # test_registry_folder_merges_for_client_label_case_variants).
   884|     # "west/coast" and "west_coast" both sanitize to "imperial_..._west_coast";
   885|     # the second gets suffixed -> imperial_project_west_coast_2.
   886|     rows_a = [_meta_row(f"a{i:02d}", "imperial", "west/coast", "Project") for i in range(3)]
   887|     rows_b = [_meta_row(f"b{i:02d}", "imperial", "west_coast", "Project") for i in range(3)]
   888|     segs1 = _build_segments(rows_a + rows_b, min_files=1)
   889|     reg1 = _build_registry(segs1)
   890| 
   891|     folder_a_1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|west/coast")["output_folder"]
   892|     folder_b_1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|west_coast")["output_folder"]
   893|     assert folder_a_1 != folder_b_1
   894| 
   895|     # Second run: a brand-new client "west_coast_2" is added — its natural
   896|     # sanitized name collides with whatever suffix the first run picked for "b".
   897|     rows_c = [_meta_row(f"c{i:02d}", "imperial", "west_coast_2", "Project") for i in range(3)]
   898|     segs2 = _build_segments(rows_a + rows_b + rows_c, min_files=1)
   899|     reg2 = _build_registry(segs2, existing_registry=reg1)
   900| 
   901|     folder_a_2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|west/coast")["output_folder"]
   902|     folder_b_2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|west_coast")["output_folder"]
   903|     folder_new_2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|west_coast_2")["output_folder"]
   904| 
   905|     # Pre-existing segments keep their prior folder names untouched.
   906|     assert folder_a_2 == folder_a_1
   907|     assert folder_b_2 == folder_b_1
   908|     # All three are distinct.
   909|     assert len({folder_a_2, folder_b_2, folder_new_2}) == 3
   910| 
   911| 
   912| def _manifest_row(segment_id, run_type="bundle", population_hash="h1", parent="", notes="", purpose="", label=""):
   913|     """Hand-craft a manifest-row-shaped dict for testing _build_registry() in
   914|     isolation, without routing through _build_segments()."""
   915|     return {
   916|         "segment_id": segment_id, "parent_segment_id": parent, "run_type": run_type,
   917|         "population_hash": population_hash, "notes": notes,
   918|         "segment_purpose": purpose, "segment_label": label,
   919|     }
   920| 
   921| 
   922| def test_registry_resets_status_when_run_type_changes():
   923|     # population_hash alone must not be the only staleness signal — a
   924|     # run_type change (e.g. lowering --min-files turns a "reference" segment
   925|     # into a "bundle" for the same file population) must also reset status,
   926|     # otherwise the orchestrator keeps skipping a segment that now needs a
   927|     # different analysis to be produced.
   928|     segs = _build_segments(ROWS, min_files=3)
   929|     clientalpha = next(r for r in segs if r["segment_id"] == "imperial|Project|ClientAlpha")
   930|     assert clientalpha["run_type"] == "bundle"
   931| 
   932|     reg1 = _build_registry(segs)
   933|     for r in reg1:
   934|         if r["segment_id"] == "imperial|Project|ClientAlpha":
   935|             r["status"] = "complete"
   936|             r["last_run_utc"] = "2026-01-01T00:00:00Z"
   937| 
   938|     segs2 = [dict(r) for r in segs]
   939|     for r in segs2:
   940|         if r["segment_id"] == "imperial|Project|ClientAlpha":
   941|             r["run_type"] = "reference"  # same population_hash, different run_type
   942| 
   943|     reg2 = _build_registry(segs2, existing_registry=reg1)
   944|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
   945|     assert clientalpha2["population_hash"] == clientalpha["population_hash"]
   946|     assert clientalpha2["status"] == "pending"
   947|     assert clientalpha2["last_run_utc"] == ""
   948|     assert "run_type_changed" in clientalpha2["notes"]
   949|     # Folder name must remain stable even though status reset.
   950|     clientalpha1 = next(r for r in reg1 if r["segment_id"] == "imperial|Project|ClientAlpha")
   951|     assert clientalpha2["output_folder"] == clientalpha1["output_folder"]
   952| 
   953| 
   954| def test_registry_reserves_dropped_segment_folder_from_new_reuse():
   955|     # A dropped segment's directory under segments/ still holds its old
   956|     # records/markers/analysis output (the caller is only warned to review it
   957|     # for manual cleanup, not to delete it) — a new segment must never be
   958|     # silently handed that same folder name.
   959|     old_manifest = [_manifest_row("imperial|Project|OldClient", population_hash="h1")]
   960|     reg1 = _build_registry(old_manifest)
   961|     old_row = next(r for r in reg1 if r["segment_id"] == "imperial|Project|OldClient")
   962|     assert old_row["output_folder"] == "imperial_project_oldclient"
   963| 
   964|     # OldClient is dropped entirely; a different, unrelated new segment
   965|     # happens to sanitize to the exact same folder base (distinct separator
   966|     # characters both collapse to "_" under _sanitize_folder).
   967|     new_manifest = [_manifest_row("imperial|Project OldClient", population_hash="h2")]
   968|     reg2 = _build_registry(new_manifest, existing_registry=reg1)
   969| 
   970|     assert not any(r["segment_id"] == "imperial|Project|OldClient" for r in reg2)
   971|     new_row = next(r for r in reg2 if r["segment_id"] == "imperial|Project OldClient")
   972|     assert new_row["output_folder"] != "imperial_project_oldclient"
   973| 
   974| 
   975| def test_registry_drops_removed_segment_ids_with_warning(capsys):
   976|     rows_full = ROWS  # includes both imperial|ClientAlpha and imperial|Renown
   977|     segs1 = _build_segments(rows_full, min_files=3)
   978|     reg1 = _build_registry(segs1)
   979|     assert any(r["segment_id"] == "imperial|Project|Renown" for r in reg1)
   980| 
   981|     rows_dropped = [r for r in rows_full if r.get("client_label") != "Renown"]
   982|     segs2 = _build_segments(rows_dropped, min_files=3)
   983|     reg2 = _build_registry(segs2, existing_registry=reg1)
   984| 
   985|     reg2_ids = {r["segment_id"] for r in reg2}
   986|     assert "imperial|Project|Renown" not in reg2_ids
   987|     assert "imperial|Renown" not in reg2_ids
   988| 
   989|     captured = capsys.readouterr()
   990|     assert "imperial|Project|Renown" in captured.err or "imperial|Renown" in captured.err
   991| 
   992| 
   993| def test_client_discipline_leaf_no_empty_purpose():
   994|     # No level-4 client+discipline segment should have an empty segment_purpose.
   995|     segs = _build_segments(_disc_rows(), min_files=3)
   996|     l4 = [r for r in segs if r["segment_level"] == "4" and r["client_label"] and r["discipline_label"]]
   997|     assert l4, "Expected level-4 client+discipline segments in _disc_rows fixture"
   998|     for r in l4:
   999|         assert r["segment_purpose"], (
  1000|             f"segment_purpose is empty for level-4 segment {r['segment_id']}"
  1001|         )
  1002|         assert r["segment_label"] != r["segment_id"], (
  1003|             f"segment_label fell back to raw ID for {r['segment_id']}"
  1004|         )
  1005| 
  1006| 
  1007| # ---------------------------------------------------------------------------
  1008| # collection_label is no longer a segmentation dimension (PR: segment builder
  1009| # explicit contract) -- it may still exist as a column in file_metadata.csv,
  1010| # and the segment builder simply ignores it. See "Collection exclusion" /
  1011| # "Collapse after collection removal" tests further down for the new
  1012| # ignore-collection_label coverage.
  1013| # ---------------------------------------------------------------------------
  1014| 
  1015| 
  1016| # ---------------------------------------------------------------------------
  1017| # Case normalization for segment dimension fields
  1018| # ---------------------------------------------------------------------------
  1019| 
```
