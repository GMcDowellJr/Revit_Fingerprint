# Chunk of tests/test_build_segment_manifest.py

- Source relative path: `tests/test_build_segment_manifest.py`
- Chunk: 3 of 4
- Original line range: 1020-1532
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_unit_system_case_variants_merge_into_single_segment, test_governance_role_case_variants_merge_and_no_false_warning, test_unknown_governance_role_still_warns_after_normalization_added, test_client_label_first_seen_casing_is_canonical, test_business_center_label_zero_padded_short_digit_values, test_business_center_label_already_four_digits_unaffected, test_business_center_label_non_numeric_unaffected, test_business_center_label_zero_pad_merges_with_correctly_formatted_rows, test_business_center_label_zero_pad_warning_emitted, test_normalization_warning_emitted_with_aggregate_count, test_clean_corpus_unaffected_by_normalization, test_conformance_reference_mode_defaults_to_latest_for_new_segment, test_conformance_reference_mode_carried_over_across_runs, test_conformance_reference_mode_defaults_to_latest_for_old_registry_missing_field, test_registry_no_longer_carries_export_run_ids, test_registry_new_files_reason_when_file_added, test_registry_removed_files_reason_when_file_removed, test_registry_both_new_and_removed_files_reasons_when_combined_change, test_registry_new_files_reason_does_not_cause_false_removal_warnings, test_registry_no_reason_notes_for_brand_new_segment, test_segment_membership_round_trip_reconstructs_in_memory_sets, test_segment_membership_join_keys_present_in_manifest_and_metadata, test_population_hash_unchanged_by_membership_storage_migration, test_manifest_and_registry_fields_stay_under_size_threshold, test_enterprise_bc_0000_preserved_literally_not_folded_to_blank, test_enterprise_identity_not_inferred_from_blank_business_center, test_business_center_case_variants_of_0000_still_fold_by_casing_not_bookkeeping, test_collection_label_ignored_same_segments_same_membership
- Source SHA-256: 9f3ece62e3859182daaa40d64fa48a48dce0364f40520d18b071b30a096c99c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  1020| def test_unit_system_case_variants_merge_into_single_segment():
  1021|     # "imperial" and "Imperial" are the same unit system typed inconsistently
  1022|     # during manual file_metadata.csv editing — they must merge into one
  1023|     # level-1 segment, not fragment into two shadow populations.
  1024|     rows = (
  1025|         [_meta_row(f"r{i:02d}", "imperial", "Acme", "Project") for i in range(3)]
  1026|         + [_meta_row(f"r{i:02d}", "Imperial", "Acme", "Project") for i in range(10, 13)]
  1027|     )
  1028|     segs = _build_segments(rows, min_files=1)
  1029|     l1_ids = {r["segment_id"] for r in segs if r["segment_level"] == "1"}
  1030|     assert l1_ids == {"imperial"}
  1031|     l1 = next(r for r in segs if r["segment_level"] == "1")
  1032|     assert l1["file_count"] == "6"
  1033| 
  1034| 
  1035| def test_governance_role_case_variants_merge_and_no_false_warning(tmp_path, capsys):
  1036|     rows = (
  1037|         [{"export_run_id": f"a{i:02d}", "unit_system": "imperial", "client_label": "Acme", "governance_role": "Container", "discipline_label": "architectural", "business_center_label": "1450"} for i in range(3)]
  1038|         + [{"export_run_id": f"b{i:02d}", "unit_system": "imperial", "client_label": "Acme", "governance_role": "container", "discipline_label": "architectural", "business_center_label": "1450"} for i in range(3)]
  1039|     )
  1040|     meta = tmp_path / "file_metadata.csv"
  1041|     with meta.open("w", newline="") as f:
  1042|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES)
  1043|         w.writeheader()
  1044|         for row in rows:
  1045|             w.writerow(row)
  1046| 
  1047|     out_dir = tmp_path / "out"
  1048|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1049|     assert rc == 0
  1050| 
  1051|     captured = capsys.readouterr()
  1052|     assert "Unrecognised governance_role" not in captured.err
  1053| 
  1054|     ids = _membership_ids(out_dir, "imperial|Container|Acme")
  1055|     assert ids == {f"a{i:02d}" for i in range(3)} | {f"b{i:02d}" for i in range(3)}
  1056| 
  1057| 
  1058| def test_unknown_governance_role_still_warns_after_normalization_added(tmp_path, capsys):
  1059|     meta = tmp_path / "file_metadata.csv"
  1060|     with meta.open("w", newline="") as f:
  1061|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES)
  1062|         w.writeheader()
  1063|         for i in range(3):
  1064|             w.writerow({
  1065|                 "export_run_id": f"r{i:02d}", "unit_system": "imperial",
  1066|                 "client_label": "Acme", "governance_role": "Contractor",
  1067|                 "discipline_label": "architectural", "business_center_label": "1450",
  1068|             })
  1069| 
  1070|     rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "1"])
  1071|     assert rc == 0
  1072|     captured = capsys.readouterr()
  1073|     assert "Unrecognised governance_role value in metadata: 'Contractor'" in captured.err
  1074| 
  1075| 
  1076| def test_client_label_first_seen_casing_is_canonical():
  1077|     # "InternalEnterprise" appears first in row order — all case variants fold to it,
  1078|     # not to an arbitrary or alphabetically-chosen casing.
  1079|     rows = [
  1080|         _meta_row("s01", "imperial", "InternalEnterprise", "Container"),
  1081|         _meta_row("s02", "imperial", "internalenterprise", "Container"),
  1082|         _meta_row("s03", "imperial", "INTERNALENTERPRISE", "Container"),
  1083|     ]
  1084|     segs = _build_segments(rows, min_files=1)
  1085|     seg_ids = {r["segment_id"] for r in segs}
  1086|     assert "imperial|Container|InternalEnterprise" in seg_ids
  1087|     assert "imperial|Container|internalenterprise" not in seg_ids
  1088|     assert "imperial|Container|INTERNALENTERPRISE" not in seg_ids
  1089|     merged = next(r for r in segs if r["segment_id"] == "imperial|Container|InternalEnterprise")
  1090|     assert merged["client_label"] == "InternalEnterprise"
  1091|     assert set(merged["export_run_ids"].split("|")) == {"s01", "s02", "s03"}
  1092| 
  1093| 
  1094| def test_business_center_label_zero_padded_short_digit_values():
  1095|     # Excel-open-without-Text-import gotcha: "0000" silently becomes "0" (or
  1096|     # "0796" becomes "796") if the column isn't imported as Text. A
  1097|     # purely-numeric business_center_label shorter than 4 digits must
  1098|     # zero-pad up to 4, matching every real 4-digit code in this corpus.
  1099|     _, changes = _normalize_rows([
  1100|         _meta_row("r01", "imperial", "Acme", "Container", business_center_label="0"),
  1101|         _meta_row("r02", "imperial", "Acme", "Container", business_center_label="796"),
  1102|         _meta_row("r03", "imperial", "Acme", "Container", business_center_label="14"),
  1103|     ])
  1104|     by_field = {(c[0], c[1]): c[2] for c in changes}
  1105|     assert by_field[("business_center_label", "0")] == "0000"
  1106|     assert by_field[("business_center_label", "796")] == "0796"
  1107|     assert by_field[("business_center_label", "14")] == "0014"
  1108| 
  1109| 
  1110| def test_business_center_label_already_four_digits_unaffected():
  1111|     _, changes = _normalize_rows([
  1112|         _meta_row("r01", "imperial", "Acme", "Container", business_center_label="0000"),
  1113|         _meta_row("r02", "imperial", "Acme", "Container", business_center_label="2014"),
  1114|     ])
  1115|     assert changes == []
  1116| 
  1117| 
  1118| def test_business_center_label_non_numeric_unaffected():
  1119|     # "BC_1234" / "Page" contain non-digit characters -- left untouched,
  1120|     # not padded or otherwise reinterpreted.
  1121|     _, changes = _normalize_rows([
  1122|         _meta_row("r01", "imperial", "Acme", "Container", business_center_label="BC_1234"),
  1123|         _meta_row("r02", "imperial", "Acme", "Container", business_center_label="Page"),
  1124|     ])
  1125|     assert changes == []
  1126| 
  1127| 
  1128| def test_business_center_label_zero_pad_merges_with_correctly_formatted_rows():
  1129|     # The real-world failure mode: some rows still say "0000" (never opened
  1130|     # in Excel), others got collapsed to "0" -- both must fold into the SAME
  1131|     # segment rather than fragmenting into two shadow enterprise segments.
  1132|     rows = (
  1133|         [_meta_row(f"a{i:02d}", "imperial", "Acme", "Container", business_center_label="0000") for i in range(3)]
  1134|         + [_meta_row(f"b{i:02d}", "imperial", "Acme", "Container", business_center_label="0") for i in range(3)]
  1135|     )
  1136|     segs = _build_segments(rows, min_files=1)
  1137|     seg_ids = {r["segment_id"] for r in segs}
  1138|     assert "imperial|Container|Acme|0000" in seg_ids
  1139|     assert "imperial|Container|Acme|0" not in seg_ids
  1140|     merged = next(r for r in segs if r["segment_id"] == "imperial|Container|Acme|0000")
  1141|     assert set(merged["export_run_ids"].split("|")) == {f"a{i:02d}" for i in range(3)} | {f"b{i:02d}" for i in range(3)}
  1142| 
  1143| 
  1144| def test_business_center_label_zero_pad_warning_emitted(tmp_path, capsys):
  1145|     meta = tmp_path / "file_metadata.csv"
  1146|     with meta.open("w", newline="") as f:
  1147|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES)
  1148|         w.writeheader()
  1149|         for i in range(5):
  1150|             w.writerow({
  1151|                 "export_run_id": f"r{i:02d}", "unit_system": "imperial",
  1152|                 "client_label": "Acme", "governance_role": "Project",
  1153|                 "discipline_label": "architectural", "business_center_label": "0",
  1154|             })
  1155| 
  1156|     rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "1"])
  1157|     assert rc == 0
  1158|     captured = capsys.readouterr()
  1159|     warn_lines = [ln for ln in captured.err.splitlines() if "Normalized business_center_label" in ln]
  1160|     assert len(warn_lines) == 1, f"Expected one aggregated warning line, got: {warn_lines}"
  1161|     assert "'0' -> '0000'" in warn_lines[0]
  1162|     assert "(5 row(s))" in warn_lines[0]
  1163| 
  1164| 
  1165| def test_normalization_warning_emitted_with_aggregate_count(tmp_path, capsys):
  1166|     meta = tmp_path / "file_metadata.csv"
  1167|     with meta.open("w", newline="") as f:
  1168|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES)
  1169|         w.writeheader()
  1170|         for i in range(16):
  1171|             w.writerow({
  1172|                 "export_run_id": f"r{i:02d}", "unit_system": "Imperial",
  1173|                 "client_label": "Acme", "governance_role": "Project",
  1174|                 "discipline_label": "architectural", "business_center_label": "1450",
  1175|             })
  1176| 
  1177|     rc = main(["--metadata-file", str(meta), "--out-dir", str(tmp_path / "out"), "--min-files", "1"])
  1178|     assert rc == 0
  1179|     captured = capsys.readouterr()
  1180|     warn_lines = [ln for ln in captured.err.splitlines() if "Normalized unit_system" in ln]
  1181|     assert len(warn_lines) == 1, f"Expected one aggregated warning line, got: {warn_lines}"
  1182|     assert "'Imperial' -> 'imperial'" in warn_lines[0]
  1183|     assert "(16 row(s))" in warn_lines[0]
  1184| 
  1185| 
  1186| def test_clean_corpus_unaffected_by_normalization():
  1187|     # Regression guard: consistently-cased fixtures produce zero normalization
  1188|     # changes, and _build_segments() output is unaffected.
  1189|     _, changes_rows = _normalize_rows(ROWS)
  1190|     assert changes_rows == []
  1191|     _, changes_disc = _normalize_rows(_disc_rows())
  1192|     assert changes_disc == []
  1193| 
  1194|     segs = _build_segments(ROWS, min_files=3)
  1195|     seg_ids = {r["segment_id"] for r in segs}
  1196|     assert "imperial" in seg_ids
  1197|     assert "metric" in seg_ids
  1198|     assert "imperial|ClientAlpha" in seg_ids
  1199|     assert "imperial|Renown" in seg_ids
  1200|     assert "metric|Global" in seg_ids
  1201| 
  1202| 
  1203| # ---------------------------------------------------------------------------
  1204| # Staleness reasons + conformance_reference_mode
  1205| # ---------------------------------------------------------------------------
  1206| 
  1207| def test_conformance_reference_mode_defaults_to_latest_for_new_segment():
  1208|     segs = _build_segments(ROWS, min_files=3)
  1209|     reg = _build_registry(segs)
  1210|     clientalpha = next(r for r in reg if r["segment_id"] == "imperial|Project|ClientAlpha")
  1211|     assert clientalpha["conformance_reference_mode"] == "latest"
  1212| 
  1213| 
  1214| def test_conformance_reference_mode_carried_over_across_runs():
  1215|     segs = _build_segments(ROWS, min_files=3)
  1216|     reg1 = _build_registry(segs)
  1217|     reg2 = _build_registry(segs, existing_registry=reg1)
  1218|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
  1219|     assert clientalpha2["conformance_reference_mode"] == "latest"
  1220| 
  1221| 
  1222| def test_conformance_reference_mode_defaults_to_latest_for_old_registry_missing_field():
  1223|     # Simulate a registry written before this field existed: DictReader on an
  1224|     # older CSV yields no "conformance_reference_mode" key at all.
  1225|     segs = _build_segments(ROWS, min_files=3)
  1226|     reg1 = _build_registry(segs)
  1227|     for r in reg1:
  1228|         r.pop("conformance_reference_mode", None)
  1229|     reg2 = _build_registry(segs, existing_registry=reg1)
  1230|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
  1231|     assert clientalpha2["conformance_reference_mode"] == "latest"
  1232| 
  1233| 
  1234| def test_registry_no_longer_carries_export_run_ids():
  1235|     # run_registry.csv dropped its inline export_run_ids column (moved to
  1236|     # segment_membership.csv) — the in-memory row built by _build_registry()
  1237|     # must not carry the key either, since REGISTRY_FIELDNAMES no longer
  1238|     # includes it.
  1239|     segs = _build_segments(ROWS, min_files=3)
  1240|     reg = _build_registry(segs)
  1241|     clientalpha = next(r for r in reg if r["segment_id"] == "imperial|Project|ClientAlpha")
  1242|     assert "export_run_ids" not in clientalpha
  1243| 
  1244| 
  1245| def test_registry_new_files_reason_when_file_added():
  1246|     segs1 = _build_segments(ROWS, min_files=3)
  1247|     reg1 = _build_registry(segs1)
  1248|     membership1 = _membership_by_segment(_build_membership_rows(segs1))
  1249| 
  1250|     rows2 = ROWS + [_meta_row("r11", "imperial", "ClientAlpha", "Project")]
  1251|     segs2 = _build_segments(rows2, min_files=3)
  1252|     reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
  1253|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
  1254| 
  1255|     assert "population_changed" in clientalpha2["notes"]
  1256|     assert "new_files:1" in clientalpha2["notes"]
  1257|     assert "removed_files" not in clientalpha2["notes"]
  1258| 
  1259| 
  1260| def test_registry_removed_files_reason_when_file_removed():
  1261|     # ClientAlpha needs more than min_files here so removing one file doesn't also
  1262|     # cross the min_files threshold and flip run_type to "skip" (which would
  1263|     # drop the segment from the registry entirely rather than mark it stale).
  1264|     rows1 = ROWS + [_meta_row("r12", "imperial", "ClientAlpha", "Project")]
  1265|     segs1 = _build_segments(rows1, min_files=3)
  1266|     reg1 = _build_registry(segs1)
  1267|     membership1 = _membership_by_segment(_build_membership_rows(segs1))
  1268| 
  1269|     rows2 = [r for r in rows1 if r["export_run_id"] != "r03"]
  1270|     segs2 = _build_segments(rows2, min_files=3)
  1271|     reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
  1272|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
  1273| 
  1274|     assert "population_changed" in clientalpha2["notes"]
  1275|     assert "removed_files:1" in clientalpha2["notes"]
  1276|     assert "new_files" not in clientalpha2["notes"]
  1277| 
  1278| 
  1279| def test_registry_both_new_and_removed_files_reasons_when_combined_change():
  1280|     rows1 = ROWS + [_meta_row("r12", "imperial", "ClientAlpha", "Project")]
  1281|     segs1 = _build_segments(rows1, min_files=3)
  1282|     reg1 = _build_registry(segs1)
  1283|     membership1 = _membership_by_segment(_build_membership_rows(segs1))
  1284| 
  1285|     # Swap r03 out for a new file r11 in the same segment in one run.
  1286|     rows2 = [r for r in rows1 if r["export_run_id"] != "r03"] + [
  1287|         _meta_row("r11", "imperial", "ClientAlpha", "Project")
  1288|     ]
  1289|     segs2 = _build_segments(rows2, min_files=3)
  1290|     reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
  1291|     clientalpha2 = next(r for r in reg2 if r["segment_id"] == "imperial|Project|ClientAlpha")
  1292| 
  1293|     assert "population_changed" in clientalpha2["notes"]
  1294|     assert "new_files:1" in clientalpha2["notes"]
  1295|     assert "removed_files:1" in clientalpha2["notes"]
  1296| 
  1297| 
  1298| def test_registry_new_files_reason_does_not_cause_false_removal_warnings(capsys):
  1299|     # Regression guard: diffing export_run_ids inside the population_changed
  1300|     # branch must not clobber the outer new_ids (segment_id set) used later
  1301|     # for dropped_ids — otherwise a plain file add to one segment would make
  1302|     # every other still-present segment look "removed" and trigger a false
  1303|     # cleanup warning.
  1304|     segs1 = _build_segments(ROWS, min_files=3)
  1305|     reg1 = _build_registry(segs1)
  1306|     membership1 = _membership_by_segment(_build_membership_rows(segs1))
  1307| 
  1308|     rows2 = ROWS + [_meta_row("r11", "imperial", "ClientAlpha", "Project")]
  1309|     segs2 = _build_segments(rows2, min_files=3)
  1310|     reg2 = _build_registry(segs2, existing_registry=reg1, existing_membership=membership1)
  1311| 
  1312|     reg2_ids = {r["segment_id"] for r in reg2}
  1313|     reg1_ids = {r["segment_id"] for r in reg1}
  1314|     assert reg1_ids <= reg2_ids, "no segment should appear removed when only a file was added"
  1315| 
  1316|     captured = capsys.readouterr()
  1317|     assert "removed from registry" not in captured.err
  1318| 
  1319| 
  1320| def test_registry_no_reason_notes_for_brand_new_segment():
  1321|     # A segment that didn't exist in the prior registry is "new", not "stale" —
  1322|     # it must not carry population_changed/new_files/removed_files reasons.
  1323|     segs1 = _build_segments(ROWS, min_files=3)
  1324|     reg1 = _build_registry(segs1)
  1325| 
  1326|     rows2 = ROWS + [_meta_row(f"z{i:02d}", "imperial", "Zenith", "Project") for i in range(3)]
  1327|     segs2 = _build_segments(rows2, min_files=3)
  1328|     reg2 = _build_registry(segs2, existing_registry=reg1)
  1329|     zenith = next(r for r in reg2 if r["segment_id"] == "imperial|Project|Zenith")
  1330| 
  1331|     assert "population_changed" not in zenith["notes"]
  1332|     assert "new_files" not in zenith["notes"]
  1333|     assert "removed_files" not in zenith["notes"]
  1334| 
  1335| 
  1336| # ---------------------------------------------------------------------------
  1337| # segment_membership.csv — normalized join table (replaces inline
  1338| # export_run_ids / seed_export_run_ids pipe-delimited columns)
  1339| # ---------------------------------------------------------------------------
  1340| 
  1341| def test_segment_membership_round_trip_reconstructs_in_memory_sets(tmp_path):
  1342|     # Build segments -> write segment_membership.csv -> reconstruct per-segment
  1343|     # export_run_ids/seed sets by filtering the membership CSV by segment_id ->
  1344|     # assert equality with the in-memory sets used to compute
  1345|     # file_count/has_seed_file/population_hash.
  1346|     meta = tmp_path / "file_metadata.csv"
  1347|     with meta.open("w", newline="") as f:
  1348|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES, extrasaction="ignore")
  1349|         w.writeheader()
  1350|         for row in VALID_ROWS:
  1351|             w.writerow(row)
  1352| 
  1353|     out_dir = tmp_path / "out"
  1354|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
  1355|     assert rc == 0
  1356| 
  1357|     segs = _build_segments(VALID_ROWS, min_files=3)
  1358|     membership_rows = _read_csv(out_dir / "segment_membership.csv")
  1359| 
  1360|     for seg in segs:
  1361|         sid = seg["segment_id"]
  1362|         expected_eids = {x for x in seg.get("export_run_ids", "").split("|") if x}
  1363|         expected_seeds = {x for x in seg.get("seed_export_run_ids", "").split("|") if x}
  1364| 
  1365|         seg_rows = [r for r in membership_rows if r["segment_id"] == sid]
  1366|         reconstructed_eids = {r["export_run_id"] for r in seg_rows}
  1367|         reconstructed_seeds = {r["export_run_id"] for r in seg_rows if r["is_seed"] == "true"}
  1368| 
  1369|         assert reconstructed_eids == expected_eids, f"segment {sid}: export_run_id mismatch"
  1370|         assert reconstructed_seeds == expected_seeds, f"segment {sid}: is_seed mismatch"
  1371|         assert str(len(reconstructed_eids)) == seg["file_count"]
  1372|         assert ("true" if reconstructed_seeds else "false") == seg["has_seed_file"]
  1373| 
  1374| 
  1375| def test_segment_membership_join_keys_present_in_manifest_and_metadata(tmp_path):
  1376|     # segment_id joins back to segment_manifest.csv; export_run_id joins back
  1377|     # to file_metadata.csv (definition grain, unchanged by this migration).
  1378|     meta = tmp_path / "file_metadata.csv"
  1379|     with meta.open("w", newline="") as f:
  1380|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES, extrasaction="ignore")
  1381|         w.writeheader()
  1382|         for row in VALID_ROWS:
  1383|             w.writerow(row)
  1384| 
  1385|     out_dir = tmp_path / "out"
  1386|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
  1387|     assert rc == 0
  1388| 
  1389|     manifest_ids = {r["segment_id"] for r in _read_csv(out_dir / "segment_manifest.csv")}
  1390|     metadata_ids = {r["export_run_id"] for r in VALID_ROWS if r["export_run_id"]}
  1391|     membership_rows = _read_csv(out_dir / "segment_membership.csv")
  1392| 
  1393|     for row in membership_rows:
  1394|         assert row["segment_id"] in manifest_ids
  1395|         assert row["export_run_id"] in metadata_ids
  1396| 
  1397| 
  1398| def test_population_hash_unchanged_by_membership_storage_migration():
  1399|     # population_hash must byte-for-byte match prior runs given the same file
  1400|     # population — it's load-bearing for skip-logic/staleness comparisons.
  1401|     # Confirmed here by hand-tracing: it is still computed from the in-memory
  1402|     # eids list, not by re-reading any CSV (segment_membership.csv included).
  1403|     segs = _build_segments(ROWS, min_files=3)
  1404|     clientalpha = next(r for r in segs if r["segment_id"] == "imperial|ClientAlpha")
  1405|     eids = [x for x in clientalpha["export_run_ids"].split("|") if x]
  1406|     assert clientalpha["population_hash"] == hashlib.sha1("|".join(sorted(eids)).encode()).hexdigest()
  1407| 
  1408| 
  1409| # ---------------------------------------------------------------------------
  1410| # Field-length regression guard — the original bug this migration fixes was a
  1411| # manifest/registry cell exceeding spreadsheet limits (Excel ~32,767 chars,
  1412| # Google Sheets ~50,000 chars) and desyncing downstream CSV parsers.
  1413| # ---------------------------------------------------------------------------
  1414| 
  1415| _MAX_SANE_FIELD_LEN = 10_000
  1416| 
  1417| 
  1418| def test_manifest_and_registry_fields_stay_under_size_threshold(tmp_path):
  1419|     # Large population: enough files that the old inline export_run_ids column
  1420|     # would have blown past the threshold (each id here is ~30 chars; 500 files
  1421|     # -> ~15,000 chars, comfortably over the 10,000-char guard).
  1422|     rows = [
  1423|         _meta_row(f"export-run-id-{i:06d}-looooong-suffix", "imperial", "BigClient", "Project",
  1424|                   discipline_label="architectural", business_center_label="1450")
  1425|         for i in range(500)
  1426|     ]
  1427|     meta = tmp_path / "file_metadata.csv"
  1428|     with meta.open("w", newline="") as f:
  1429|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES, extrasaction="ignore")
  1430|         w.writeheader()
  1431|         for row in rows:
  1432|             w.writerow(row)
  1433| 
  1434|     out_dir = tmp_path / "out"
  1435|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
  1436|     assert rc == 0
  1437| 
  1438|     for csv_name in ("segment_manifest.csv", "run_registry.csv"):
  1439|         for row in _read_csv(out_dir / csv_name):
  1440|             for field, value in row.items():
  1441|                 assert len(value) < _MAX_SANE_FIELD_LEN, (
  1442|                     f"{csv_name} field '{field}' on segment_id={row.get('segment_id')} "
  1443|                     f"is {len(value)} chars — file membership must live in "
  1444|                     f"segment_membership.csv, not an inline manifest/registry column"
  1445|                 )
  1446| 
  1447|     # segment_membership.csv rows themselves must also stay well under the
  1448|     # threshold (each row is one segment_id/export_run_id/is_seed triple).
  1449|     for row in _read_csv(out_dir / "segment_membership.csv"):
  1450|         for field, value in row.items():
  1451|             assert len(value) < _MAX_SANE_FIELD_LEN
  1452| 
  1453| 
  1454| # ---------------------------------------------------------------------------
  1455| # PR "segment builder explicit contract" -- Enterprise (InternalEnterprise/0000) literal
  1456| # preservation. No blank-to-Enterprise fallback, no bookkeeping-token fold.
  1457| # ---------------------------------------------------------------------------
  1458| 
  1459| def test_enterprise_bc_0000_preserved_literally_not_folded_to_blank():
  1460|     rows = [
  1461|         _full_row(f"e{i:02d}", "imperial", "InternalEnterprise", "Container", "architectural", "0000")
  1462|         for i in range(3)
  1463|     ]
  1464|     segs = _build_segments(rows, min_files=3)
  1465|     # The client+bc leaf (level 4: client_label + business_center_label both
  1466|     # selected) carries "0000" literally -- it is never folded to blank.
  1467|     leaf = next(r for r in segs if r["client_label"] == "InternalEnterprise" and r["business_center_label"] == "0000" and r["segment_level"] == "4")
  1468|     assert leaf["business_center_label"] == "0000"
  1469|     assert "0000" in leaf["segment_id"]
  1470|     # And its population is identical to the client-only pool (every row here
  1471|     # shares the same bc), proving "0000" wasn't silently dropped/blanked
  1472|     # anywhere along the way -- not a redundant_single_child artifact of a
  1473|     # bookkeeping-token fold.
  1474|     client_only = next(r for r in segs if r["segment_id"] == "imperial|Container|InternalEnterprise")
  1475|     assert leaf["export_run_ids"] == client_only["export_run_ids"]
  1476| 
  1477| 
  1478| def test_enterprise_identity_not_inferred_from_blank_business_center():
  1479|     # A real (non-InternalEnterprise, non-0000) client with a genuinely blank
  1480|     # business_center_label must not be folded into or conflated with the
  1481|     # InternalEnterprise/0000 Enterprise population -- 0000 is a literal value, not a
  1482|     # stand-in for "unspecified business center".
  1483|     internalenterprise_rows = [_full_row(f"s{i:02d}", "imperial", "InternalEnterprise", "Container", "architectural", "0000") for i in range(3)]
  1484|     other_rows = [_meta_row(f"o{i:02d}", "imperial", "ClientAlpha", "Container", "architectural") for i in range(3)]
  1485|     segs = _build_segments(internalenterprise_rows + other_rows, min_files=3)
  1486|     internalenterprise_leaf = next(r for r in segs if r["client_label"] == "InternalEnterprise" and r["segment_level"] == "3" and r["business_center_label"] == "0000")
  1487|     clientalpha_leaf = next(r for r in segs if r["client_label"] == "ClientAlpha" and r["segment_level"] == "3")
  1488|     assert set(internalenterprise_leaf["export_run_ids"].split("|")).isdisjoint(set(clientalpha_leaf["export_run_ids"].split("|")))
  1489| 
  1490| 
  1491| def test_business_center_case_variants_of_0000_still_fold_by_casing_not_bookkeeping():
  1492|     # "0000" has no case variants to speak of, but a mixed-case bc token like
  1493|     # "Bc1450"/"bc1450" should still fold via the ordinary first-seen-casing
  1494|     # rule (unrelated to the removed enterprise-bookkeeping fold).
  1495|     rows = (
  1496|         [_full_row(f"a{i:02d}", "imperial", "ClientAlpha", "Container", "architectural", "BC1450") for i in range(2)]
  1497|         + [_full_row(f"b{i:02d}", "imperial", "ClientAlpha", "Container", "architectural", "bc1450") for i in range(2, 4)]
  1498|     )
  1499|     segs = _build_segments(rows, min_files=1)
  1500|     bc_values = {r["business_center_label"] for r in segs if r["business_center_label"]}
  1501|     assert bc_values == {"BC1450"}
  1502| 
  1503| 
  1504| # ---------------------------------------------------------------------------
  1505| # PR "segment builder explicit contract" -- collection exclusion: two rows
  1506| # identical except collection_label must produce the same segment identities
  1507| # and the same population memberships (no collection-specific children).
  1508| # ---------------------------------------------------------------------------
  1509| 
  1510| def test_collection_label_ignored_same_segments_same_membership():
  1511|     base = dict(unit_system="imperial", governance_role="Container", client_label="ClientAlpha",
  1512|                 discipline_label="architectural", business_center_label="1450")
  1513|     rows_a = [dict(base, export_run_id=f"a{i:02d}", collection_label="ClientAlpha Standards") for i in range(3)]
  1514|     rows_b = [dict(base, export_run_id=f"b{i:02d}", collection_label="Legacy Collection") for i in range(3)]
  1515| 
  1516|     segs_with_collection = _build_segments(rows_a + rows_b, min_files=1)
  1517|     rows_a_no_coll = [{k: v for k, v in r.items() if k != "collection_label"} for r in rows_a]
  1518|     rows_b_no_coll = [{k: v for k, v in r.items() if k != "collection_label"} for r in rows_b]
  1519|     segs_without_collection = _build_segments(rows_a_no_coll + rows_b_no_coll, min_files=1)
  1520| 
  1521|     ids_with = {r["segment_id"] for r in segs_with_collection}
  1522|     ids_without = {r["segment_id"] for r in segs_without_collection}
  1523|     assert ids_with == ids_without, "collection_label must not affect segment identity at all"
  1524| 
  1525|     # No collection-specific children exist: every generated segment's
  1526|     # collection_label column is always blank.
  1527|     assert all(r.get("collection_label", "") == "" for r in segs_with_collection)
  1528| 
  1529|     leaf = next(r for r in segs_with_collection if r["segment_id"] == "imperial|Container|ClientAlpha|architectural|1450")
  1530|     assert set(leaf["export_run_ids"].split("|")) == {r["export_run_id"] for r in rows_a + rows_b}
  1531| 
  1532| 
```
