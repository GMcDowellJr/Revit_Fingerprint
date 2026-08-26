# Chunk of tests/test_build_segment_manifest.py

- Source relative path: `tests/test_build_segment_manifest.py`
- Chunk: 1 of 4
- Original line range: 1-504
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _meta_row, _full_row, _read_csv, _membership_ids, test_population_hash_deterministic, test_blank_unit_system_excluded, test_level1_segments_present, test_level1_run_type_skip_when_below_min_files, test_level1_run_type_bundle_at_min_files, test_level1_file_counts, test_level2_segments_present, test_level2_run_type_below_min, test_level2_run_type_at_min, test_seed_detection_level2, test_seed_detection_renown_no_seed, test_seed_detection_container_role, test_level1_parent_is_empty, test_level2_parent_is_unit_system, test_sort_order_level1_before_level2, test_sort_order_within_level_alphabetical, test_export_run_ids_sorted_pipe_delimited, test_membership_rows_no_pipe_delimited_values, test_manifest_and_registry_have_no_list_columns, test_population_hash_in_manifest, test_registry_excludes_skip_segments, test_registry_output_folder_sanitized, test_sanitize_folder_strips_path_separators, test_sanitize_folder_preserves_selected_blank_vs_unselected_dimension, test_sanitize_folder_renders_selected_blank_as_neutral_token, test_registry_output_folders_globally_unique_with_suffix_collision, test_registry_distinguishes_selected_blank_client_from_unselected_client_pool, test_registry_initial_status_pending, test_main_writes_files, test_seed_only_note_not_set_for_generic_only_segment, test_seed_only_note_not_suppressed_by_blank_eid_project_row, test_seed_only_note_set_when_segment_has_seeds_no_project, test_registry_folder_merges_for_client_label_case_variants, test_blank_client_label_no_longer_participates_in_subset, test_main_missing_metadata_file, test_main_fails_on_missing_required_columns, test_main_fails_when_export_run_id_column_absent
- Source SHA-256: 9f3ece62e3859182daaa40d64fa48a48dce0364f40520d18b071b30a096c99c4
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """Tests for tools/build_segment_manifest.py."""
     2| from __future__ import annotations
     3| 
     4| import csv
     5| import hashlib
     6| from pathlib import Path
     7| 
     8| import pytest
     9| 
    10| # Allow running without installing; resolve to repo root.
    11| import sys
    12| sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
    13| from build_segment_manifest import (
    14|     _build_segments,
    15|     _build_registry,
    16|     _population_hash,
    17|     _normalize_rows,
    18|     _validate_required_metadata,
    19|     _build_membership_rows,
    20|     _membership_by_segment,
    21|     DIMENSION_CONFIG,
    22|     REQUIRED_ROW_FIELDS,
    23|     MANIFEST_FIELDNAMES,
    24|     REGISTRY_FIELDNAMES,
    25|     main,
    26| )
    27| 
    28| 
    29| # ---------------------------------------------------------------------------
    30| # Helpers
    31| # ---------------------------------------------------------------------------
    32| 
    33| def _meta_row(export_run_id, unit_system, client_label, governance_role, discipline_label="", business_center_label=""):
    34|     return {
    35|         "export_run_id": export_run_id,
    36|         "unit_system": unit_system,
    37|         "client_label": client_label,
    38|         "governance_role": governance_role,
    39|         "discipline_label": discipline_label,
    40|         "business_center_label": business_center_label,
    41|     }
    42| 
    43| 
    44| def _full_row(export_run_id, unit_system, client_label, governance_role, discipline_label, business_center_label):
    45|     """Like _meta_row, but every required field must be passed explicitly --
    46|     for building fixtures fed through main() (which now blocks the whole
    47|     build if any required field is blank on any row)."""
    48|     return _meta_row(export_run_id, unit_system, client_label, governance_role, discipline_label, business_center_label)
    49| 
    50| 
    51| # A fully-valid fixture (every REQUIRED_ROW_FIELDS value populated) for tests
    52| # that route through main() -- ROWS deliberately keeps discipline_label/
    53| # business_center_label blank on most rows (and unit_system blank on r10) to
    54| # exercise _build_segments()'s own permissive combinatorics directly; main()
    55| # would now block on all of that, so main()-level tests use VALID_ROWS.
    56| VALID_ROWS = [
    57|     _full_row("r01", "imperial", "ClientAlpha", "Project", "architectural", "1450"),
    58|     _full_row("r02", "imperial", "ClientAlpha", "Project", "architectural", "1450"),
    59|     _full_row("r03", "imperial", "ClientAlpha", "Project", "architectural", "1450"),
    60|     _full_row("r04", "imperial", "ClientAlpha", "Template", "architectural", "1450"),
    61|     _full_row("r05", "imperial", "Renown", "Project", "structural", "2270"),
    62|     _full_row("r06", "imperial", "Renown", "Project", "structural", "2270"),
    63|     _full_row("r07", "imperial", "Renown", "Project", "structural", "2270"),
    64|     _full_row("r08", "metric", "Global", "Project", "mechanical", "0000"),
    65|     _full_row("r09", "metric", "Global", "Container", "mechanical", "0000"),
    66| ]
    67| VALID_FIELDNAMES = ["export_run_id", "unit_system", "client_label", "governance_role", "discipline_label", "business_center_label"]
    68| 
    69| 
    70| def _read_csv(path: Path):
    71|     with path.open("r", encoding="utf-8-sig", newline="") as f:
    72|         return list(csv.DictReader(f))
    73| 
    74| 
    75| def _membership_ids(out_dir: Path, segment_id: str) -> set:
    76|     """Read segment_membership.csv and return the export_run_id set for one segment_id."""
    77|     rows = _read_csv(out_dir / "segment_membership.csv")
    78|     return {r["export_run_id"] for r in rows if r["segment_id"] == segment_id}
    79| 
    80| 
    81| # ---------------------------------------------------------------------------
    82| # Unit tests
    83| # ---------------------------------------------------------------------------
    84| 
    85| ROWS = [
    86|     _meta_row("r01", "imperial", "ClientAlpha", "Project"),
    87|     _meta_row("r02", "imperial", "ClientAlpha", "Project"),
    88|     _meta_row("r03", "imperial", "ClientAlpha", "Project"),
    89|     _meta_row("r04", "imperial", "ClientAlpha", "Template"),
    90|     _meta_row("r05", "imperial", "Renown", "Project"),
    91|     _meta_row("r06", "imperial", "Renown", "Project"),
    92|     _meta_row("r07", "imperial", "Renown", "Project"),
    93|     _meta_row("r08", "metric",   "Global",  "Project"),
    94|     _meta_row("r09", "metric",   "Global",  "Container"),
    95|     _meta_row("r10", "",        "Unknown",  "Project"),   # blank unit_system — excluded
    96| ]
    97| 
    98| 
    99| def test_population_hash_deterministic():
   100|     ids = ["r03", "r01", "r02"]
   101|     h1 = _population_hash(ids)
   102|     h2 = _population_hash(["r01", "r02", "r03"])  # different order
   103|     assert h1 == h2
   104|     expected = hashlib.sha1(b"r01|r02|r03").hexdigest()
   105|     assert h1 == expected
   106| 
   107| 
   108| def test_blank_unit_system_excluded():
   109|     segs = _build_segments(ROWS, min_files=3)
   110|     all_ids = "|".join(r["export_run_ids"] for r in segs)
   111|     assert "r10" not in all_ids
   112| 
   113| 
   114| def test_level1_segments_present():
   115|     segs = _build_segments(ROWS, min_files=3)
   116|     l1 = [r for r in segs if r["segment_level"] == "1"]
   117|     ids = {r["segment_id"] for r in l1}
   118|     assert ids == {"imperial", "metric"}
   119| 
   120| 
   121| def test_level1_run_type_skip_when_below_min_files():
   122|     # Level-1 unit populations with children are registration-only parents.
   123|     rows = [_meta_row("x01", "metric", "Tiny", "Project"), _meta_row("x02", "metric", "Tiny", "Project")]
   124|     segs = _build_segments(rows, min_files=3)
   125|     metric = next(r for r in segs if r["segment_id"] == "metric" and r["segment_level"] == "1")
   126|     assert metric["run_type"] == "registration"
   127| 
   128| 
   129| def test_level1_run_type_bundle_at_min_files():
   130|     rows = [_meta_row(f"r{i:02d}", "imperial", "Acme", "Project") for i in range(3)]
   131|     segs = _build_segments(rows, min_files=3)
   132|     imp = next(r for r in segs if r["segment_id"] == "imperial" and r["segment_level"] == "1")
   133|     assert imp["run_type"] == "registration"
   134| 
   135| 
   136| def test_level1_file_counts():
   137|     segs = _build_segments(ROWS, min_files=3)
   138|     l1 = {r["segment_id"]: int(r["file_count"]) for r in segs if r["segment_level"] == "1"}
   139|     assert l1["imperial"] == 7   # r01-r07 (r10 excluded)
   140|     assert l1["metric"] == 2     # r08, r09
   141| 
   142| 
   143| def test_level2_segments_present():
   144|     segs = _build_segments(ROWS, min_files=3)
   145|     l2 = [r for r in segs if r["segment_level"] == "2"]
   146|     seg_ids = {r["segment_id"] for r in l2}
   147|     assert "imperial|ClientAlpha" in seg_ids
   148|     assert "imperial|Renown" in seg_ids
   149|     assert "metric|Global" in seg_ids
   150| 
   151| 
   152| def test_level2_run_type_below_min():
   153|     segs = _build_segments(ROWS, min_files=3)
   154|     metric_global = next(r for r in segs if r["segment_id"] == "metric|Global")
   155|     assert metric_global["run_type"] == "registration"
   156| 
   157| 
   158| def test_level2_run_type_at_min():
   159|     segs = _build_segments(ROWS, min_files=3)
   160|     clientalpha = next(r for r in segs if r["segment_id"] == "imperial|ClientAlpha")
   161|     assert clientalpha["run_type"] == "registration"
   162| 
   163| 
   164| def test_seed_detection_level2():
   165|     segs = _build_segments(ROWS, min_files=3)
   166|     clientalpha = next(r for r in segs if r["segment_id"] == "imperial|ClientAlpha")
   167|     assert clientalpha["has_seed_file"] == "true"
   168|     assert "r04" in clientalpha["seed_export_run_ids"].split("|")
   169| 
   170| 
   171| def test_seed_detection_renown_no_seed():
   172|     segs = _build_segments(ROWS, min_files=3)
   173|     renown = next(r for r in segs if r["segment_id"] == "imperial|Renown")
   174|     assert renown["has_seed_file"] == "false"
   175|     assert renown["seed_export_run_ids"] == ""
   176| 
   177| 
   178| def test_seed_detection_container_role():
   179|     segs = _build_segments(ROWS, min_files=3)
   180|     global_seg = next(r for r in segs if r["segment_id"] == "metric|Global")
   181|     assert global_seg["has_seed_file"] == "true"
   182|     assert "r09" in global_seg["seed_export_run_ids"].split("|")
   183| 
   184| 
   185| def test_level1_parent_is_empty():
   186|     segs = _build_segments(ROWS, min_files=3)
   187|     for r in segs:
   188|         if r["segment_level"] == "1":
   189|             assert r["parent_segment_id"] == ""
   190| 
   191| 
   192| def test_level2_parent_is_unit_system():
   193|     segs = _build_segments(ROWS, min_files=3)
   194|     for r in segs:
   195|         if r["segment_level"] == "2":
   196|             assert r["parent_segment_id"] == r["unit_system"]
   197| 
   198| 
   199| def test_sort_order_level1_before_level2():
   200|     segs = _build_segments(ROWS, min_files=3)
   201|     levels = [int(r["segment_level"]) for r in segs]
   202|     assert levels == sorted(levels)
   203| 
   204| 
   205| def test_sort_order_within_level_alphabetical():
   206|     segs = _build_segments(ROWS, min_files=3)
   207|     l1_ids = [r["segment_id"] for r in segs if r["segment_level"] == "1"]
   208|     assert l1_ids == sorted(l1_ids)
   209|     l2_ids = [r["segment_id"] for r in segs if r["segment_level"] == "2"]
   210|     assert l2_ids == sorted(l2_ids)
   211| 
   212| 
   213| def test_export_run_ids_sorted_pipe_delimited():
   214|     # File membership now lives in segment_membership.csv, not an inline
   215|     # pipe-delimited manifest column (which blew past spreadsheet cell limits
   216|     # for large populations). Rows are sorted (segment_id, export_run_id).
   217|     segs = _build_segments(ROWS, min_files=3)
   218|     membership = _build_membership_rows(segs)
   219|     clientalpha_ids = [r["export_run_id"] for r in membership if r["segment_id"] == "imperial|ClientAlpha"]
   220|     assert clientalpha_ids == sorted(clientalpha_ids)
   221|     assert clientalpha_ids  # non-empty for this fixture
   222| 
   223| 
   224| def test_membership_rows_no_pipe_delimited_values():
   225|     # Regression guard for the original bug: export_run_id/is_seed must never
   226|     # be a pipe-joined list (segment_id legitimately contains "|" as its own
   227|     # hierarchical separator, e.g. "imperial|ClientAlpha" — that's unrelated).
   228|     segs = _build_segments(ROWS, min_files=3)
   229|     membership = _build_membership_rows(segs)
   230|     for row in membership:
   231|         assert "|" not in row["export_run_id"]
   232|         assert "|" not in row["is_seed"]
   233| 
   234| 
   235| def test_manifest_and_registry_have_no_list_columns():
   236|     # Regression guard: segment_manifest.csv / run_registry.csv must only ever
   237|     # carry scalar summary fields — file membership belongs in
   238|     # segment_membership.csv exclusively.
   239|     assert "export_run_ids" not in MANIFEST_FIELDNAMES
   240|     assert "seed_export_run_ids" not in MANIFEST_FIELDNAMES
   241|     assert "export_run_ids" not in REGISTRY_FIELDNAMES
   242|     assert "seed_export_run_ids" not in REGISTRY_FIELDNAMES
   243| 
   244| 
   245| def test_population_hash_in_manifest():
   246|     segs = _build_segments(ROWS, min_files=3)
   247|     clientalpha = next(r for r in segs if r["segment_id"] == "imperial|ClientAlpha")
   248|     expected = _population_hash(clientalpha["export_run_ids"].split("|"))
   249|     assert clientalpha["population_hash"] == expected
   250| 
   251| 
   252| def test_registry_excludes_skip_segments():
   253|     segs = _build_segments(ROWS, min_files=3)
   254|     reg = _build_registry(segs)
   255|     reg_ids = {r["segment_id"] for r in reg}
   256|     assert "metric|Global" not in reg_ids
   257| 
   258| 
   259| def test_registry_output_folder_sanitized():
   260|     segs = _build_segments(ROWS, min_files=3)
   261|     reg = _build_registry(segs)
   262|     clientalpha_reg = next(r for r in reg if r["segment_id"] == "imperial|Project|ClientAlpha")
   263|     assert clientalpha_reg["output_folder"] == "imperial_project_clientalpha"
   264| 
   265| 
   266| def test_sanitize_folder_strips_path_separators():
   267|     from build_segment_manifest import _sanitize_folder
   268|     assert "/" not in _sanitize_folder("imperial/west|Client")
   269|     assert "\\" not in _sanitize_folder("imperial\\east|Client")
   270|     # Result should be a flat name, not a path
   271|     result = _sanitize_folder("us/west|Acme Corp")
   272|     assert "/" not in result and "\\" not in result
   273|     assert result == result.lower()
   274| 
   275| 
   276| def test_sanitize_folder_preserves_selected_blank_vs_unselected_dimension():
   277|     # A cut dimension explicitly selected in a subset with a blank value
   278|     # (e.g. client_label == "" chosen as a subset criterion) renders in
   279|     # segment_id as an empty part between/after separator pipes, distinct
   280|     # from that same dimension not being selected at all (which pools every
   281|     # value of the field, blank included — always a superset of the
   282|     # selected-blank population). _sanitize_folder() must not collapse that
   283|     # distinction away, or two segments with genuinely different
   284|     # populations sanitize to the identical folder name.
   285|     from build_segment_manifest import _sanitize_folder
   286| 
   287|     # Trailing blank (client selected blank, nothing follows it).
   288|     assert _sanitize_folder("imperial|Template") != _sanitize_folder("imperial|Template|")
   289|     # Embedded blank (client selected blank, discipline follows it).
   290|     assert (
   291|         _sanitize_folder("imperial|Container|architectural")
   292|         != _sanitize_folder("imperial|Container||architectural")
   293|     )
   294| 
   295| 
   296| def test_sanitize_folder_renders_selected_blank_as_neutral_token():
   297|     # A bare "_" (trailing) or "__" (embedded) reads as a naming mistake, not
   298|     # an intentional "no client selected" segment. Render it as a neutral,
   299|     # explicit data-state token. Not "enterprise" — this token fires for every
   300|     # blank-client segment regardless of whether it also has a real business
   301|     # center, so it does not mean the no-client/no-business-center scope.
   302|     from build_segment_manifest import _sanitize_folder
   303| 
   304|     assert _sanitize_folder("imperial|Template|") == "imperial_template_no_external_client"
   305|     assert (
   306|         _sanitize_folder("imperial|Container||architectural")
   307|         == "imperial_container_no_external_client_architectural"
   308|     )
   309|     # A segment with no blank-selected dimension at all is untouched.
   310|     assert _sanitize_folder("imperial|Template") == "imperial_template"
   311| 
   312| 
   313| def test_registry_output_folders_globally_unique_with_suffix_collision():
   314|     # Reproduce the case where a generated suffix collides with another
   315|     # segment's natural sanitized name. Uses distinct literal client_label
   316|     # strings (not case variants — those now merge upstream in
   317|     # _build_segments() via _normalize_rows(), so they can no longer produce
   318|     # two different segment_ids to collide in the first place):
   319|     #   imperial|west/coast   → imperial_west_coast (natural)
   320|     #   imperial|west_coast   → imperial_west_coast (collision → imperial_west_coast_2)
   321|     #   imperial|west_coast_2 → imperial_west_coast_2 (natural — collides with the suffix!)
   322|     # The registry must still produce three distinct output_folder values.
   323|     rows = (
   324|         [_meta_row(f"a{i:02d}", "imperial", "west/coast", "Project") for i in range(3)]
   325|         + [_meta_row(f"b{i:02d}", "imperial", "west_coast", "Project") for i in range(3)]
   326|         + [_meta_row(f"c{i:02d}", "imperial", "west_coast_2", "Project") for i in range(3)]
   327|     )
   328|     segs = _build_segments(rows, min_files=1)
   329|     reg = _build_registry(segs)
   330|     folders = [r["output_folder"] for r in reg]
   331|     assert len(folders) == len(set(folders)), f"Duplicate output_folder values: {folders}"
   332| 
   333| 
   334| def test_registry_distinguishes_selected_blank_client_from_unselected_client_pool():
   335|     # "Client not selected" (root+governance only — pools every client's
   336|     # rows, blank included) and "client selected as blank" (root+governance
   337|     # +client="" — blank-client rows only) are different populations
   338|     # whenever any non-blank-client rows also exist for that governance_role
   339|     # (the pooled population is then a strict superset of the blank-only
   340|     # one), and both can independently end up run_type="bundle"/"reference"
   341|     # in real corpora. Their segment_ids differ only by a trailing/embedded
   342|     # blank part (e.g. "imperial|Template" vs "imperial|Template|"), which
   343|     # _sanitize_folder() previously collapsed to the identical folder name.
   344|     # _manifest_row() constructs eligible rows directly, decoupled from
   345|     # _build_segments()'s own eligibility-determination rules.
   346|     manifest_rows = [
   347|         _manifest_row("imperial|Template", population_hash="h_pooled"),
   348|         _manifest_row("imperial|Template|", population_hash="h_blank_only"),
   349|     ]
   350|     reg = _build_registry(manifest_rows)
   351| 
   352|     pooled = next(r for r in reg if r["segment_id"] == "imperial|Template")
   353|     blank_only = next(r for r in reg if r["segment_id"] == "imperial|Template|")
   354|     assert pooled["output_folder"] != blank_only["output_folder"]
   355| 
   356| 
   357| def test_registry_initial_status_pending():
   358|     segs = _build_segments(ROWS, min_files=3)
   359|     reg = _build_registry(segs)
   360|     for r in reg:
   361|         assert r["status"] == "pending"
   362|         assert r["last_run_utc"] == ""
   363| 
   364| 
   365| # ---------------------------------------------------------------------------
   366| # Integration test — end-to-end via main()
   367| # ---------------------------------------------------------------------------
   368| 
   369| def test_main_writes_files(tmp_path):
   370|     meta = tmp_path / "file_metadata.csv"
   371|     with meta.open("w", newline="") as f:
   372|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES, extrasaction="ignore")
   373|         w.writeheader()
   374|         for row in VALID_ROWS:
   375|             w.writerow(row)
   376| 
   377|     out_dir = tmp_path / "out"
   378|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "3"])
   379|     assert rc == 0
   380| 
   381|     manifest_path = out_dir / "segment_manifest.csv"
   382|     registry_path = out_dir / "run_registry.csv"
   383|     assert manifest_path.is_file()
   384|     assert registry_path.is_file()
   385| 
   386|     manifest_rows = _read_csv(manifest_path)
   387|     seg_ids = {r["segment_id"] for r in manifest_rows}
   388|     assert "imperial" in seg_ids
   389|     assert "metric" in seg_ids
   390|     assert "imperial|ClientAlpha" in seg_ids
   391| 
   392|     reg_rows = _read_csv(registry_path)
   393|     assert all(r["status"] == "pending" for r in reg_rows)
   394|     assert not any(r["segment_id"] == "metric|Global" for r in reg_rows)
   395| 
   396| 
   397| def test_seed_only_note_not_set_for_generic_only_segment():
   398|     # A segment whose files are all Generic (no Project AND no Template/Container)
   399|     # must NOT be flagged seed_only — it has no actual seed files.
   400|     rows = [_meta_row(f"r{i:02d}", "imperial", "GenericClient", "Generic") for i in range(3)]
   401|     segs = _build_segments(rows, min_files=1)
   402|     l2 = next(r for r in segs if r["segment_level"] == "2")
   403|     assert "seed_only" not in (l2.get("notes") or "")
   404|     assert l2["has_seed_file"] == "false"
   405| 
   406| 
   407| def test_seed_only_note_not_suppressed_by_blank_eid_project_row():
   408|     # A malformed row with blank export_run_id and governance_role=Project must NOT
   409|     # suppress seed_only — it is excluded from membership so it should not influence
   410|     # the no_project predicate either.
   411|     rows = [
   412|         _meta_row("s01", "imperial", "SeedOrg", "Template"),
   413|         _meta_row("s02", "imperial", "SeedOrg", "Template"),
   414|         _meta_row("s03", "imperial", "SeedOrg", "Template"),
   415|         _meta_row("",    "imperial", "SeedOrg", "Project"),   # blank eid — excluded member
   416|     ]
   417|     segs = _build_segments(rows, min_files=1)
   418|     l2 = next(r for r in segs if r["segment_level"] == "2" and r["unit_system"] == "imperial")
   419|     assert "seed_only" in (l2.get("notes") or ""), (
   420|         "Blank-eid Project row should not suppress seed_only"
   421|     )
   422|     assert l2["has_seed_file"] == "true"
   423|     # The blank-eid row must not appear in export_run_ids
   424|     assert "" not in l2["export_run_ids"].split("|")
   425| 
   426| 
   427| def test_seed_only_note_set_when_segment_has_seeds_no_project():
   428|     # Template/Container files with no Project files → seed_only is correct.
   429|     rows = [
   430|         _meta_row("s01", "imperial", "SeedOrg", "Template"),
   431|         _meta_row("s02", "imperial", "SeedOrg", "Container"),
   432|         _meta_row("s03", "imperial", "SeedOrg", "Template"),
   433|     ]
   434|     segs = _build_segments(rows, min_files=1)
   435|     l2 = next(r for r in segs if r["segment_level"] == "2")
   436|     assert "seed_only" in (l2.get("notes") or "")
   437|     assert l2["has_seed_file"] == "true"
   438| 
   439| 
   440| def test_registry_folder_merges_for_client_label_case_variants():
   441|     # "ClientAlpha" and "clientalpha" are case variants of the same client, not two
   442|     # clients — _normalize_rows() folds them together (first-seen casing)
   443|     # before segment_id construction, so this must produce ONE registry row
   444|     # / output_folder, not two. (Previously this scenario produced two
   445|     # distinct segment_ids that both sanitized to "imperial_clientalpha" and had
   446|     # to be disambiguated with a suffix — that was the bug this fix closes.)
   447|     rows = (
   448|         [_meta_row(f"r{i:02d}", "imperial", "ClientAlpha", "Project") for i in range(3)]
   449|         + [_meta_row(f"r{i:02d}", "imperial", "clientalpha", "Project") for i in range(10, 13)]
   450|     )
   451|     segs = _build_segments(rows, min_files=1)
   452|     reg = _build_registry(segs)
   453|     clientalpha_rows = [r for r in reg if r["segment_id"] == "imperial|Project|ClientAlpha"]
   454|     assert len(clientalpha_rows) == 1
   455|     assert not any(r["segment_id"] == "imperial|Project|clientalpha" for r in reg)
   456|     assert clientalpha_rows[0]["output_folder"] == "imperial_project_clientalpha"
   457| 
   458| 
   459| def test_blank_client_label_no_longer_participates_in_subset():
   460|     # Blank-value injection is removed under the explicit-metadata contract:
   461|     # a blank client_label row no longer manufactures a distinct "selected
   462|     # blank client" segment (the old "imperial|" twin). It simply doesn't
   463|     # contribute client_label to the subset lattice at all.
   464|     rows = [_meta_row(f"r{i:02d}", "imperial", "", "Project") for i in range(3)]
   465|     segs = _build_segments(rows, min_files=1)
   466|     seg_ids = {r["segment_id"] for r in segs}
   467|     assert "imperial|" not in seg_ids
   468|     assert "imperial|Project" in seg_ids
   469|     proj = next(r for r in segs if r["segment_id"] == "imperial|Project")
   470|     assert proj["client_label"] == ""
   471| 
   472| 
   473| def test_main_missing_metadata_file(tmp_path):
   474|     rc = main(["--metadata-file", str(tmp_path / "missing.csv"), "--out-dir", str(tmp_path / "out")])
   475|     assert rc == 1
   476| 
   477| 
   478| def test_main_fails_on_missing_required_columns(tmp_path):
   479|     # CSV is present and non-empty but lacks governance_role — tool must exit 1
   480|     # and write no output files (silently dropping every row would be worse).
   481|     meta = tmp_path / "file_metadata.csv"
   482|     with meta.open("w", newline="") as f:
   483|         w = csv.DictWriter(f, fieldnames=["export_run_id", "unit_system", "client_label"])
   484|         w.writeheader()
   485|         w.writerow({"export_run_id": "r01", "unit_system": "imperial", "client_label": "Acme"})
   486| 
   487|     out_dir = tmp_path / "out"
   488|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
   489|     assert rc == 1
   490|     assert not (out_dir / "segment_manifest.csv").exists()
   491| 
   492| 
   493| def test_main_fails_when_export_run_id_column_absent(tmp_path):
   494|     meta = tmp_path / "file_metadata.csv"
   495|     with meta.open("w", newline="") as f:
   496|         w = csv.DictWriter(f, fieldnames=["unit_system", "client_label", "governance_role"])
   497|         w.writeheader()
   498|         w.writerow({"unit_system": "imperial", "client_label": "Acme", "governance_role": "Project"})
   499| 
   500|     out_dir = tmp_path / "out"
   501|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
   502|     assert rc == 1
   503| 
   504| 
```
