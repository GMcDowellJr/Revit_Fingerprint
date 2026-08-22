# Chunk of tests/test_compare_cross_segment_governance.py

- Source relative path: `tests/test_compare_cross_segment_governance.py`
- Chunk: 5 of 5
- Original line range: 1985-2213
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_pool_matrix_keeps_pool_scopes_distinct_for_same_project, test_fragmentation_diagnostic_unavailable_without_required_inputs, test_non_project_union_inventory_blocks_project_union_matrices, test_mean_file_pair_matrix_adds_synthetic_diagonal_cells, test_mean_file_pair_diagonals_limited_to_project_observed_domains, test_mean_file_pair_matrix_emits_symmetric_cells, test_missing_union_inventory_blocks_union_matrix_with_explicit_status, test_matrix_manifest_and_diagonal_are_deterministic, _write_within_project_segment, test_discover_within_project_na_spellings_do_not_group, test_discover_within_project_real_shared_label_still_groups
- Source SHA-256: 41a98d942cef2b25dee2bd74f79b3ba9f6e871cbbff68d9ef81011f7e3336043
- Starts inside symbol: no
- Ends inside symbol: no

```
  1985| def test_pool_matrix_keeps_pool_scopes_distinct_for_same_project():
  1986|     # A project can appear once per applicable pool_scope grain
  1987|     # (parent_sibling/bc/client). Different grains must land on distinct
  1988|     # matrix coordinates instead of colliding on identical
  1989|     # (row_id, col_id, view, domain) with different values.
  1990|     from compare_cross_segment import build_explicit_matrix_outputs
  1991| 
  1992|     pooled = [
  1993|         {
  1994|             "governance_role": "Project", "segment_label": "A", "domain": "d1",
  1995|             "pool_scope": "parent_sibling",
  1996|             "all_containment_focal_in_pool": "0.111111",
  1997|             "used_containment_focal_in_pool": "",
  1998|         },
  1999|         {
  2000|             "governance_role": "Project", "segment_label": "A", "domain": "d1",
  2001|             "pool_scope": "bc",
  2002|             "all_containment_focal_in_pool": "0.222222",
  2003|             "used_containment_focal_in_pool": "",
  2004|         },
  2005|         {
  2006|             "governance_role": "Project", "segment_label": "A", "domain": "d1",
  2007|             "pool_scope": "client",
  2008|             "all_containment_focal_in_pool": "0.333333",
  2009|             "used_containment_focal_in_pool": "",
  2010|         },
  2011|     ]
  2012| 
  2013|     matrices, _, _ = build_explicit_matrix_outputs([], pooled, [], "2026-07-13T00:00:00Z")
  2014|     rows = [
  2015|         r for r in matrices["project_pool_containment_similarity_matrix.csv"]
  2016|         if r["row_id"] == "A" and r["view_scope"] == "all" and r["domain"] == "d1"
  2017|     ]
  2018| 
  2019|     coords = {(r["row_id"], r["column_id"], r["view_scope"], r["domain"]) for r in rows}
  2020|     assert len(coords) == len(rows) == 3
  2021|     by_col = {r["column_id"]: r["value"] for r in rows}
  2022|     assert by_col["peer_pool:parent_sibling:A"] == "0.111111"
  2023|     assert by_col["peer_pool:bc:A"] == "0.222222"
  2024|     assert by_col["peer_pool:client:A"] == "0.333333"
  2025| 
  2026| 
  2027| def test_fragmentation_diagnostic_unavailable_without_required_inputs():
  2028|     from compare_cross_segment import build_explicit_matrix_outputs
  2029| 
  2030|     _, frag, _ = build_explicit_matrix_outputs([], [], [], "2026-06-22T00:00:00Z")
  2031| 
  2032|     assert frag == [{
  2033|         "matrix_name": "project_fragmentation_diagnostic.csv",
  2034|         "row_id": "unavailable",
  2035|         "column_id": "unavailable",
  2036|         "view_scope": "unavailable",
  2037|         "domain": "ALL_DOMAINS",
  2038|         "footprint_similarity": "",
  2039|         "exact_identity_overlap": "",
  2040|         "fragmentation_diagnostic": "",
  2041|         "value_status": "unavailable_required_inputs",
  2042|         "interpretation": "Requires both union_jaccard and mean_file_pair_jaccard inputs.",
  2043|         "executed_utc": "2026-06-22T00:00:00Z",
  2044|     }]
  2045| 
  2046| 
  2047| def test_non_project_union_inventory_blocks_project_union_matrices():
  2048|     from compare_cross_segment import build_explicit_matrix_outputs
  2049| 
  2050|     union_rows = [{
  2051|         "governance_role": "Template",
  2052|         "client_label": "A",
  2053|         "discipline_label": "Arch",
  2054|         "unit_system": "imperial",
  2055|         "domain": "d",
  2056|         "view_scope": "all",
  2057|         "join_hash": "template_only",
  2058|         "n_files_present": "1",
  2059|         "n_files_denominator": "1",
  2060|         "n_projects_present": "1",
  2061|         "n_projects_denominator": "1",
  2062|         "n_clients_present": "1",
  2063|         "n_clients_denominator": "1",
  2064|         "pct_clients_present": "1.000000",
  2065|         "inventory_status": "ok",
  2066|     }]
  2067| 
  2068|     matrices, _, _ = build_explicit_matrix_outputs([], [], union_rows, "2026-06-22T00:00:00Z")
  2069| 
  2070|     assert matrices["project_union_jaccard_matrix.csv"][0]["value_status"] == "blocked_no_ok_project_union_inventory"
  2071|     assert matrices["project_density_similarity_matrix.csv"][0]["value_status"] == "blocked_no_ok_project_union_inventory"
  2072| 
  2073| 
  2074| def test_mean_file_pair_matrix_adds_synthetic_diagonal_cells():
  2075|     from compare_cross_segment import build_explicit_matrix_outputs
  2076| 
  2077|     summary = [{
  2078|         "governance_role_a": "Project",
  2079|         "governance_role_b": "Project",
  2080|         "segment_label_a": "Project A",
  2081|         "segment_label_b": "Project B",
  2082|         "domain": "d",
  2083|         "all_pairwise_jaccard_mean": "0.250000",
  2084|     }]
  2085| 
  2086|     matrices, _, _ = build_explicit_matrix_outputs(summary, [], [], "2026-06-22T00:00:00Z")
  2087|     rows = matrices["project_mean_file_pair_jaccard_matrix.csv"]
  2088| 
  2089|     diagonal = [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project A" and r["domain"] == "ALL_DOMAINS"][0]
  2090|     assert diagonal["value"] == "1.000000"
  2091|     assert diagonal["value_status"] == "synthetic_self_comparison"
  2092|     assert diagonal["self_comparison"] == "true"
  2093| 
  2094| 
  2095| def test_mean_file_pair_diagonals_limited_to_project_observed_domains():
  2096|     from compare_cross_segment import build_explicit_matrix_outputs
  2097| 
  2098|     summary = [
  2099|         {"governance_role_a": "Project", "governance_role_b": "Project", "segment_label_a": "Project A", "segment_label_b": "Project B", "domain": "d1", "all_pairwise_jaccard_mean": "0.250000"},
  2100|         {"governance_role_a": "Project", "governance_role_b": "Project", "segment_label_a": "Project C", "segment_label_b": "Project D", "domain": "d2", "all_pairwise_jaccard_mean": "0.500000"},
  2101|     ]
  2102| 
  2103|     matrices, _, _ = build_explicit_matrix_outputs(summary, [], [], "2026-06-22T00:00:00Z")
  2104|     rows = matrices["project_mean_file_pair_jaccard_matrix.csv"]
  2105| 
  2106|     assert [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project A" and r["domain"] == "d1"]
  2107|     assert not [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project A" and r["domain"] == "d2"]
  2108|     assert [r for r in rows if r["row_id"] == "Project C" and r["column_id"] == "Project C" and r["domain"] == "d2"]
  2109|     assert not [r for r in rows if r["row_id"] == "Project C" and r["column_id"] == "Project C" and r["domain"] == "d1"]
  2110| 
  2111| 
  2112| def test_mean_file_pair_matrix_emits_symmetric_cells():
  2113|     from compare_cross_segment import build_explicit_matrix_outputs
  2114| 
  2115|     summary = [{
  2116|         "governance_role_a": "Project",
  2117|         "governance_role_b": "Project",
  2118|         "segment_label_a": "Project A",
  2119|         "segment_label_b": "Project B",
  2120|         "domain": "d",
  2121|         "all_pairwise_jaccard_mean": "0.250000",
  2122|     }]
  2123| 
  2124|     matrices, _, _ = build_explicit_matrix_outputs(summary, [], [], "2026-06-22T00:00:00Z")
  2125|     rows = matrices["project_mean_file_pair_jaccard_matrix.csv"]
  2126| 
  2127|     forward = [r for r in rows if r["row_id"] == "Project A" and r["column_id"] == "Project B" and r["domain"] == "ALL_DOMAINS"][0]
  2128|     reverse = [r for r in rows if r["row_id"] == "Project B" and r["column_id"] == "Project A" and r["domain"] == "ALL_DOMAINS"][0]
  2129|     assert forward["value"] == reverse["value"] == "0.250000"
  2130| 
  2131| 
  2132| def test_missing_union_inventory_blocks_union_matrix_with_explicit_status():
  2133|     from compare_cross_segment import build_explicit_matrix_outputs
  2134| 
  2135|     matrices, _, _ = build_explicit_matrix_outputs([], [], [], "2026-06-22T00:00:00Z")
  2136| 
  2137|     row = matrices["project_union_jaccard_matrix.csv"][0]
  2138|     assert row["value_status"] == "blocked_missing_union_inventory"
  2139|     assert row["value"] == ""
  2140| 
  2141| 
  2142| def test_matrix_manifest_and_diagonal_are_deterministic():
  2143|     from compare_cross_segment import build_explicit_matrix_outputs
  2144| 
  2145|     union_rows = [{"governance_role": "Project", "client_label": "A", "discipline_label": "Arch", "unit_system": "imperial", "domain": "d", "view_scope": "all", "join_hash": "x", "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"}]
  2146| 
  2147|     first = build_explicit_matrix_outputs([], [], union_rows, "2026-06-22T00:00:00Z")
  2148|     second = build_explicit_matrix_outputs([], [], union_rows, "2026-06-22T00:00:00Z")
  2149| 
  2150|     assert first == second
  2151|     diagonal = first[0]["project_union_jaccard_matrix.csv"][0]
  2152|     assert diagonal["row_id"] == diagonal["column_id"]
  2153|     assert diagonal["self_comparison"] == "true"
  2154|     assert diagonal["value"] == "1.000000"
  2155|     assert {"matrix_name", "governance_role", "view_scope", "source_file", "source_grain", "metric", "identity_unit", "aggregation_method", "interpretation", "known_limitations", "executed_utc"} == set(first[2][0])
  2156| 
  2157| 
  2158| def _write_within_project_segment(segments_root: Path, folder: str, domain: str, export_run_ids):
  2159|     base = segments_root / folder / "results" / "bundle_analysis" / "all" / domain
  2160|     _write_csv(
  2161|         base / "membership_matrix.csv",
  2162|         [{"export_run_id": eid} for eid in export_run_ids],
  2163|     )
  2164| 
  2165| 
  2166| def test_discover_within_project_na_spellings_do_not_group(tmp_path):
  2167|     # Mirrors test_discover_governance_chain_final_fallback_normalizes_na_spelling,
  2168|     # but for Mode D: unlike governance chain's canonical-blank merge, an
  2169|     # unassigned project_label must NOT let unrelated files collide into one
  2170|     # fake "project" — every NA spelling (and repeats of the same spelling)
  2171|     # must fall back to its own per-file singleton, so no within_project
  2172|     # pair is ever discovered for a segment where every file is NA-labeled.
  2173|     segments_root = tmp_path / "segments"
  2174|     domain = "line_patterns"
  2175|     _write_within_project_segment(
  2176|         segments_root, "mixed_na", domain,
  2177|         ["na_t", "na_c", "na_p", "na_dup1", "na_dup2"],
  2178|     )
  2179|     manifest = {"mixed_na": {}}
  2180|     registry = {"mixed_na": {"output_folder": "mixed_na", "run_type": "bundle"}}
  2181|     file_metadata = {
  2182|         "na_t": {"project_label": "__NOT_APPLICABLE__"},
  2183|         "na_c": {"project_label": "n/a"},
  2184|         "na_p": {"project_label": "NA"},
  2185|         # Same exact NA spelling repeated: pre-fix these collapsed into one
  2186|         # fake project too and must also NOT pair post-fix.
  2187|         "na_dup1": {"project_label": "__NOT_APPLICABLE__"},
  2188|         "na_dup2": {"project_label": "__NOT_APPLICABLE__"},
  2189|     }
  2190| 
  2191|     pairs = discover_within_project(manifest, registry, file_metadata, segments_root)
  2192| 
  2193|     assert ("mixed_na", "mixed_na", "within_project") not in pairs
  2194| 
  2195| 
  2196| def test_discover_within_project_real_shared_label_still_groups(tmp_path):
  2197|     segments_root = tmp_path / "segments"
  2198|     domain = "line_patterns"
  2199|     _write_within_project_segment(
  2200|         segments_root, "renown", domain,
  2201|         ["r1", "r2", "na_extra"],
  2202|     )
  2203|     manifest = {"renown": {}}
  2204|     registry = {"renown": {"output_folder": "renown", "run_type": "bundle"}}
  2205|     file_metadata = {
  2206|         "r1": {"project_label": "Renown"},
  2207|         "r2": {"project_label": "Renown"},
  2208|         "na_extra": {"project_label": "__NOT_APPLICABLE__"},
  2209|     }
  2210| 
  2211|     pairs = discover_within_project(manifest, registry, file_metadata, segments_root)
  2212| 
  2213|     assert ("renown", "renown", "within_project") in pairs
```
