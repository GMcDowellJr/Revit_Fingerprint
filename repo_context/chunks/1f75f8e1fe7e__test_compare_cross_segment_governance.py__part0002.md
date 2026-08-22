# Chunk of tests/test_compare_cross_segment_governance.py

- Source relative path: `tests/test_compare_cross_segment_governance.py`
- Chunk: 2 of 5
- Original line range: 489-992
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_discover_governance_chain_excludes_ancestor_descendant_from_scope_fanout, test_discover_governance_chain_enterprise_to_bc_reaches_every_real_bc, test_discover_governance_chain_bc_to_bc_pairs_every_peer_business_center, test_discover_governance_chain_bc_to_bc_excludes_same_bc_and_enterprise, test_discover_governance_chain_disc_match_has_no_blank_wildcard, test_discover_client_cross_bc_multi_bc_enumeration, test_discover_client_cross_bc_single_bc_produces_no_pairs, test_discover_client_cross_bc_and_bc_to_bc_do_not_reference_collection_label, test_pooled_comparison_bc_scope_pools_across_clients_ignoring_client, test_pooled_comparison_bc_scope_pools_enterprise_0000_segments, test_pooled_comparison_client_scope_pools_across_bcs_ignoring_bc, test_pooled_comparison_excludes_rollup_ancestor_from_bc_pool, test_project_target_governance_state_uses_target_used, test_standards_carrier_target_avoids_passive_bloat_label, _write_csv, _write_segment, _write_reference_analysis_segment, test_reference_analysis_segment_discovers_domains_without_bundle_outputs, test_reference_analysis_segment_loads_all_view_from_domain_patterns, test_reference_analysis_segment_groups_fallback_by_export_run_id_column
- Source SHA-256: 41a98d942cef2b25dee2bd74f79b3ba9f6e871cbbff68d9ef81011f7e3336043
- Starts inside symbol: no
- Ends inside symbol: no

```
   489| def test_discover_governance_chain_excludes_ancestor_descendant_from_scope_fanout():
   490|     # The scope-fanout loops group purely by scope level, ignoring
   491|     # parent_segment_id — so an ancestor and its own descendant (e.g. an
   492|     # enterprise-scoped Template and a bc-scoped Template nested directly
   493|     # under it) can otherwise land on opposite sides of one of these edges,
   494|     # even though a descendant's data is always a subset of its ancestor's.
   495|     manifest = {
   496|         "ent_t": {
   497|             **_seg("Template", client="InternalEnterprise"), "business_center_label": "0000",
   498|             "parent_segment_id": "",
   499|         },
   500|         "bc_t_child": {
   501|             **_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234",
   502|             "parent_segment_id": "ent_t",
   503|         },
   504|         "proj_grandchild": {
   505|             **_seg("Project", client="Acme"), "business_center_label": "BC_1234",
   506|             "parent_segment_id": "bc_t_child",
   507|         },
   508|         "bc_t_unrelated": {
   509|             **_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234",
   510|             "parent_segment_id": "",
   511|         },
   512|         "proj_unrelated": {
   513|             **_seg("Project", client="Widgets"), "business_center_label": "BC_1234",
   514|             "parent_segment_id": "",
   515|         },
   516|     }
   517| 
   518|     pairs = set(discover_governance_chain(POLICY, manifest))
   519| 
   520|     # Ancestor/descendant pairs excluded from all four scope-fanout edges.
   521|     assert ("ent_t", "bc_t_child", "enterprise_to_bc") not in pairs
   522|     assert ("ent_t", "proj_grandchild", "enterprise_to_project") not in pairs
   523|     assert ("bc_t_child", "proj_grandchild", "bc_to_project") not in pairs
   524|     # Unrelated peers (no shared lineage) still pair normally.
   525|     assert ("ent_t", "bc_t_unrelated", "enterprise_to_bc") in pairs
   526|     assert ("ent_t", "proj_unrelated", "enterprise_to_project") in pairs
   527|     assert ("bc_t_unrelated", "proj_unrelated", "bc_to_project") in pairs
   528| 
   529| 
   530| def test_discover_governance_chain_enterprise_to_bc_reaches_every_real_bc():
   531|     # An enterprise-scoped Template must fan out to EVERY real business
   532|     # center's same-role Template, not just one -- a fixture with 3+ BCs must
   533|     # show all 3, not a fixed pair.
   534|     manifest = {
   535|         "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
   536|         "bc1_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
   537|         "bc2_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "2270"},
   538|         "bc3_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "9999"},
   539|     }
   540| 
   541|     pairs = {(a, b) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "enterprise_to_bc"}
   542| 
   543|     assert pairs == {("ent_t", "bc1_t"), ("ent_t", "bc2_t"), ("ent_t", "bc3_t")}
   544| 
   545| 
   546| def test_discover_governance_chain_bc_to_bc_pairs_every_peer_business_center():
   547|     # Purpose-built BC-to-BC peer discovery: every pair of real business
   548|     # centers' same-role, same-discipline Template populations.
   549|     manifest = {
   550|         "bc1_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
   551|         "bc2_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "2270"},
   552|         "bc3_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "9999"},
   553|     }
   554| 
   555|     pairs = {(a, b) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "bc_to_bc"}
   556| 
   557|     assert pairs == {("bc1_t", "bc2_t"), ("bc1_t", "bc3_t"), ("bc2_t", "bc3_t")}
   558| 
   559| 
   560| def test_discover_governance_chain_bc_to_bc_excludes_same_bc_and_enterprise():
   561|     manifest = {
   562|         "bc1_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
   563|         "bc1_t_dup": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "1450"},
   564|         "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
   565|     }
   566| 
   567|     pairs = {(a, b, ctype) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "bc_to_bc"}
   568| 
   569|     # Same normalized bc on both sides is not a peer pair.
   570|     assert not any({a, b} == {"bc1_t", "bc1_t_dup"} for a, b, _ in pairs)
   571|     # Enterprise scope never participates in bc_to_bc.
   572|     assert not any("ent_t" in (a, b) for a, b, _ in pairs)
   573| 
   574| 
   575| def test_discover_governance_chain_disc_match_has_no_blank_wildcard():
   576|     # A row with blank discipline_label must not wildcard-pair with a
   577|     # populated-discipline row under discipline-gated comparison types --
   578|     # the removed _disc_match() blank wildcard must not silently reappear.
   579|     manifest = {
   580|         "bc1_t_blank": {
   581|             **_seg("Template", client="InternalEnterprise", discipline=""), "business_center_label": "1450",
   582|         },
   583|         "bc2_t_arch": {
   584|             **_seg("Template", client="InternalEnterprise", discipline="Architectural"), "business_center_label": "2270",
   585|         },
   586|     }
   587| 
   588|     pairs = {(a, b) for a, b, ctype in discover_governance_chain(POLICY, manifest) if ctype == "bc_to_bc"}
   589| 
   590|     assert ("bc1_t_blank", "bc2_t_arch") not in pairs
   591|     assert ("bc2_t_arch", "bc1_t_blank") not in pairs
   592| 
   593| 
   594| def test_discover_client_cross_bc_multi_bc_enumeration():
   595|     # A client present in 3 BCs produces pairs across all 3 (not a fixed
   596|     # two-BC comparison) -- derived from the data, not hardcoded.
   597|     manifest = {
   598|         "acme_bc1": {**_seg("Project", client="Acme"), "business_center_label": "1450"},
   599|         "acme_bc2": {**_seg("Project", client="Acme"), "business_center_label": "2270"},
   600|         "acme_bc3": {**_seg("Project", client="Acme"), "business_center_label": "9999"},
   601|     }
   602| 
   603|     pairs = {(a, b) for a, b, ctype in discover_client_cross_bc(POLICY, manifest) if ctype == "client_cross_bc"}
   604| 
   605|     assert pairs == {("acme_bc1", "acme_bc2"), ("acme_bc1", "acme_bc3"), ("acme_bc2", "acme_bc3")}
   606| 
   607| 
   608| def test_discover_client_cross_bc_single_bc_produces_no_pairs():
   609|     manifest = {
   610|         "acme_bc1": {**_seg("Project", client="Acme"), "business_center_label": "1450"},
   611|         "widgets_bc1": {**_seg("Project", client="Widgets"), "business_center_label": "1450"},
   612|     }
   613| 
   614|     assert discover_client_cross_bc(POLICY, manifest) == []
   615| 
   616| 
   617| def test_discover_client_cross_bc_and_bc_to_bc_do_not_reference_collection_label():
   618|     # Regression guard: these new pair-discovery functions must not gate on
   619|     # collection_label -- PR1 left it an always-blank column, and neither
   620|     # function should depend on it being populated (or its absence) to find
   621|     # a pair a differing collection_label value would otherwise be expected
   622|     # to block, since neither reads the field at all.
   623|     manifest = {
   624|         "acme_bc1": {
   625|             **_seg("Project", client="Acme"), "business_center_label": "1450",
   626|             "collection_label": "Some Collection",
   627|         },
   628|         "acme_bc2": {
   629|             **_seg("Project", client="Acme"), "business_center_label": "2270",
   630|             "collection_label": "A Totally Different Collection",
   631|         },
   632|         "bc1_t": {
   633|             **_seg("Template", client="InternalEnterprise"), "business_center_label": "1450",
   634|             "collection_label": "Some Collection",
   635|         },
   636|         "bc2_t": {
   637|             **_seg("Template", client="InternalEnterprise"), "business_center_label": "2270",
   638|             "collection_label": "A Totally Different Collection",
   639|         },
   640|     }
   641| 
   642|     assert ("acme_bc1", "acme_bc2", "client_cross_bc") in discover_client_cross_bc(POLICY, manifest)
   643|     assert ("bc1_t", "bc2_t", "bc_to_bc") in discover_governance_chain(POLICY, manifest)
   644| 
   645| 
   646| def test_pooled_comparison_bc_scope_pools_across_clients_ignoring_client(tmp_path):
   647|     segments_root = tmp_path / "segments"
   648|     domain = "line_patterns"
   649|     _write_segment(
   650|         segments_root, "proj_a", domain,
   651|         [("p1", "shared", "Shared"), ("p2", "a_only", "A Only")],
   652|         [{"export_run_id": "proj_a_file", "pattern_id": "p1"}, {"export_run_id": "proj_a_file", "pattern_id": "p2"}],
   653|         [{"export_run_id": "proj_a_file", "pattern_id": "p1"}],
   654|         ["p1", "p2"],
   655|     )
   656|     _write_segment(
   657|         segments_root, "proj_b", domain,
   658|         [("p1", "shared", "Shared"), ("p2", "b_only", "B Only")],
   659|         [{"export_run_id": "proj_b_file", "pattern_id": "p1"}, {"export_run_id": "proj_b_file", "pattern_id": "p2"}],
   660|         [{"export_run_id": "proj_b_file", "pattern_id": "p1"}],
   661|         ["p1", "p2"],
   662|     )
   663|     manifest = {
   664|         "proj_a": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234", "segment_label": "Proj A"},
   665|         "proj_b": {**_seg("Project", client="Widgets"), "business_center_label": "BC_1234", "segment_label": "Proj B"},
   666|     }
   667|     registry = {
   668|         "proj_a": {"output_folder": "proj_a", "run_type": "bundle"},
   669|         "proj_b": {"output_folder": "proj_b", "run_type": "bundle"},
   670|     }
   671| 
   672|     rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")
   673| 
   674|     # No shared parent_segment_id and different clients — only the bc-scoped
   675|     # pool should fire, not parent_sibling or client.
   676|     assert {r["pool_scope"] for r in rows} == {"bc"}
   677|     by_sid = {r["segment_id"]: r for r in rows}
   678|     assert by_sid["proj_a"]["n_shared_join_hash"] == "1"
   679|     assert by_sid["proj_a"]["all_containment_focal_in_pool"] == "0.500000"
   680| 
   681| 
   682| def test_pooled_comparison_bc_scope_pools_enterprise_0000_segments(tmp_path):
   683|     # Before this PR, business_center_label=="0000" normalized to blank via
   684|     # _normalize_bc_label(), so `if bc:` at the bc_groups gate in
   685|     # run_pooled_comparison() was always False for Enterprise-scoped rows --
   686|     # they were silently excluded from bc-scoped pooling entirely (not
   687|     # pooled under a "blank" bucket, just never added to bc_groups at all).
   688|     # _bc_of() (used here, same as everywhere else in this file) now returns
   689|     # the literal "0000", so two Enterprise segments correctly pool together
   690|     # under their own real bc bucket.
   691|     segments_root = tmp_path / "segments"
   692|     domain = "line_patterns"
   693|     _write_segment(
   694|         segments_root, "ent_a", domain,
   695|         [("p1", "shared", "Shared"), ("p2", "a_only", "A Only")],
   696|         [{"export_run_id": "ent_a_file", "pattern_id": "p1"}, {"export_run_id": "ent_a_file", "pattern_id": "p2"}],
   697|         [{"export_run_id": "ent_a_file", "pattern_id": "p1"}],
   698|         ["p1", "p2"],
   699|     )
   700|     _write_segment(
   701|         segments_root, "ent_b", domain,
   702|         [("p1", "shared", "Shared"), ("p2", "b_only", "B Only")],
   703|         [{"export_run_id": "ent_b_file", "pattern_id": "p1"}, {"export_run_id": "ent_b_file", "pattern_id": "p2"}],
   704|         [{"export_run_id": "ent_b_file", "pattern_id": "p1"}],
   705|         ["p1", "p2"],
   706|     )
   707|     manifest = {
   708|         "ent_a": {**_seg("Project", client="InternalEnterprise"), "business_center_label": "0000", "segment_label": "Ent A"},
   709|         "ent_b": {**_seg("Project", client="InternalEnterprise"), "business_center_label": "0000", "segment_label": "Ent B"},
   710|     }
   711|     registry = {
   712|         "ent_a": {"output_folder": "ent_a", "run_type": "bundle"},
   713|         "ent_b": {"output_folder": "ent_b", "run_type": "bundle"},
   714|     }
   715| 
   716|     rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")
   717| 
   718|     # Same client (both "InternalEnterprise") -- both bc and client pools fire, since
   719|     # "0000" is no longer folded away and client_groups still fires
   720|     # independently.
   721|     assert {r["pool_scope"] for r in rows} == {"bc", "client"}
   722|     by_sid_scope = {(r["segment_id"], r["pool_scope"]): r for r in rows}
   723|     bc_row = by_sid_scope[("ent_a", "bc")]
   724|     assert bc_row["business_center_label"] == "0000"
   725|     assert bc_row["n_shared_join_hash"] == "1"
   726|     assert bc_row["all_containment_focal_in_pool"] == "0.500000"
   727| 
   728| 
   729| def test_pooled_comparison_client_scope_pools_across_bcs_ignoring_bc(tmp_path):
   730|     segments_root = tmp_path / "segments"
   731|     domain = "line_patterns"
   732|     _write_segment(
   733|         segments_root, "proj_a", domain,
   734|         [("p1", "shared", "Shared"), ("p2", "a_only", "A Only")],
   735|         [{"export_run_id": "proj_a_file", "pattern_id": "p1"}, {"export_run_id": "proj_a_file", "pattern_id": "p2"}],
   736|         [{"export_run_id": "proj_a_file", "pattern_id": "p1"}],
   737|         ["p1", "p2"],
   738|     )
   739|     _write_segment(
   740|         segments_root, "proj_b", domain,
   741|         [("p1", "shared", "Shared"), ("p2", "b_only", "B Only")],
   742|         [{"export_run_id": "proj_b_file", "pattern_id": "p1"}, {"export_run_id": "proj_b_file", "pattern_id": "p2"}],
   743|         [{"export_run_id": "proj_b_file", "pattern_id": "p1"}],
   744|         ["p1", "p2"],
   745|     )
   746|     manifest = {
   747|         "proj_a": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234", "segment_label": "Proj A"},
   748|         "proj_b": {**_seg("Project", client="Acme"), "business_center_label": "BC_9999", "segment_label": "Proj B"},
   749|     }
   750|     registry = {
   751|         "proj_a": {"output_folder": "proj_a", "run_type": "bundle"},
   752|         "proj_b": {"output_folder": "proj_b", "run_type": "bundle"},
   753|     }
   754| 
   755|     rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")
   756| 
   757|     # Same client, different bc, no shared parent_segment_id — only the
   758|     # client-scoped pool should fire.
   759|     assert {r["pool_scope"] for r in rows} == {"client"}
   760|     by_sid = {r["segment_id"]: r for r in rows}
   761|     assert by_sid["proj_b"]["n_shared_join_hash"] == "1"
   762|     assert by_sid["proj_b"]["all_containment_focal_in_pool"] == "0.500000"
   763| 
   764| 
   765| def test_pooled_comparison_excludes_rollup_ancestor_from_bc_pool(tmp_path):
   766|     # A collection-blank BC roll-up and its collection-specific child share
   767|     # the same normalized business_center_label, so a naive bc-pool grouping
   768|     # would put the child in a pool that includes its own ancestor — whose
   769|     # population already contains (a superset of) the child's own data.
   770|     # The child's real peer ("peer", no lineage relation) must be the only
   771|     # pool member; if the rollup leaked in, focal-in-pool containment would
   772|     # be 1.0 instead of 0.0 (peer shares nothing with the child).
   773|     segments_root = tmp_path / "segments"
   774|     domain = "line_patterns"
   775|     _write_segment(
   776|         segments_root, "rollup", domain,
   777|         [("r1", "jh_child", "Child Pattern")],
   778|         [{"export_run_id": "rollup_file", "pattern_id": "r1"}],
   779|         [{"export_run_id": "rollup_file", "pattern_id": "r1"}],
   780|         ["r1"],
   781|     )
   782|     _write_segment(
   783|         segments_root, "child", domain,
   784|         [("c1", "jh_child", "Child Pattern")],
   785|         [{"export_run_id": "child_file", "pattern_id": "c1"}],
   786|         [{"export_run_id": "child_file", "pattern_id": "c1"}],
   787|         ["c1"],
   788|     )
   789|     _write_segment(
   790|         segments_root, "peer", domain,
   791|         [("p1", "jh_peer_only", "Peer Only")],
   792|         [{"export_run_id": "peer_file", "pattern_id": "p1"}],
   793|         [{"export_run_id": "peer_file", "pattern_id": "p1"}],
   794|         ["p1"],
   795|     )
   796|     manifest = {
   797|         "rollup": {
   798|             **_seg("Template", client=""), "business_center_label": "BC_1234",
   799|             "segment_label": "Rollup", "parent_segment_id": "",
   800|         },
   801|         "child": {
   802|             **_seg("Template", client=""), "business_center_label": "BC_1234",
   803|             "segment_label": "Child", "parent_segment_id": "rollup",
   804|         },
   805|         "peer": {
   806|             **_seg("Template", client=""), "business_center_label": "BC_1234",
   807|             "segment_label": "Peer", "parent_segment_id": "",
   808|         },
   809|     }
   810|     registry = {
   811|         "rollup": {"output_folder": "rollup", "run_type": "bundle"},
   812|         "child": {"output_folder": "child", "run_type": "bundle"},
   813|         "peer": {"output_folder": "peer", "run_type": "bundle"},
   814|     }
   815| 
   816|     rows = run_pooled_comparison(POLICY, manifest, registry, segments_root, min_patterns=1, executed_utc="2026-07-13T00:00:00Z")
   817| 
   818|     child_row = [r for r in rows if r["segment_id"] == "child" and r["pool_scope"] == "bc"][0]
   819|     assert child_row["n_files_pool"] == "1"
   820|     assert child_row["n_shared_join_hash"] == "0"
   821|     assert child_row["all_containment_focal_in_pool"] == "0.000000"
   822| 
   823| 
   824| def test_project_target_governance_state_uses_target_used():
   825|     assert _usage_interpretable_for_role("Project") is True
   826|     assert _recommended_primary_view("Template", "Project", "template_to_project") == "used"
   827|     assert (
   828|         _classify_governance_state(True, True, False, True, True)
   829|         == "provided_but_passive"
   830|     )
   831|     assert (
   832|         _classify_governance_state(False, True, True, True, True)
   833|         == "local_active"
   834|     )
   835| 
   836| 
   837| def test_standards_carrier_target_avoids_passive_bloat_label():
   838|     assert _usage_interpretable_for_role("Template") is False
   839|     assert _recommended_primary_view("Generic", "Template", "generic_to_template") == "all"
   840|     assert (
   841|         _classify_governance_state(True, True, False, True, False)
   842|         == "provided_configured"
   843|     )
   844|     assert "all-view is primary" in _comparison_role_semantics(
   845|         "Generic", "Template", "generic_to_template"
   846|     )
   847| 
   848| 
   849| def _write_csv(path: Path, rows):
   850|     path.parent.mkdir(parents=True, exist_ok=True)
   851|     if not rows:
   852|         raise AssertionError("test helper requires at least one row")
   853|     import csv
   854| 
   855|     with path.open("w", encoding="utf-8", newline="") as f:
   856|         writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
   857|         writer.writeheader()
   858|         writer.writerows(rows)
   859| 
   860| 
   861| def _write_segment(seg_root: Path, folder: str, domain: str, patterns, all_rows, used_rows, bundle_all):
   862|     base = seg_root / folder / "results"
   863|     _write_csv(
   864|         base / "analysis" / "domain_patterns.csv",
   865|         [
   866|             {
   867|                 "domain": domain,
   868|                 "pattern_id": pid,
   869|                 "source_cluster_id": f"src|{jh}",
   870|                 "pattern_label_human": label,
   871|                 "pattern_label": label,
   872|             }
   873|             for pid, jh, label in patterns
   874|         ],
   875|     )
   876|     _write_csv(base / "bundle_analysis" / "all" / domain / "membership_matrix.csv", all_rows)
   877|     _write_csv(base / "bundle_analysis" / "used" / domain / "membership_matrix.csv", used_rows)
   878|     _write_csv(
   879|         base / "bundle_analysis" / "all" / domain / "bundle_membership.csv",
   880|         [{"pattern_id": pid} for pid in bundle_all],
   881|     )
   882| 
   883| 
   884| def _write_reference_analysis_segment(
   885|     seg_root: Path,
   886|     folder: str,
   887|     domain: str,
   888|     rows,
   889|     export_run_ids=None,
   890|     presence_rows=None,
   891| ):
   892|     base = seg_root / folder
   893|     if export_run_ids is not None:
   894|         base.mkdir(parents=True, exist_ok=True)
   895|         (base / "export_run_ids.txt").write_text(
   896|             "\n".join(export_run_ids) + "\n", encoding="utf-8"
   897|         )
   898|     _write_csv(
   899|         base / "results" / "analysis" / "domain_patterns.csv",
   900|         [
   901|             {
   902|                 "domain": row.get("domain", domain),
   903|                 "pattern_id": row["pattern_id"],
   904|                 "source_cluster_id": f"src|{row['join_hash']}",
   905|                 "pattern_label_human": row.get("label", row["pattern_id"]),
   906|                 "pattern_label": row.get("label", row["pattern_id"]),
   907|                 **(
   908|                     {"export_run_id": row["export_run_id"]}
   909|                     if "export_run_id" in row
   910|                     else {}
   911|                 ),
   912|             }
   913|             for row in rows
   914|         ],
   915|     )
   916|     if presence_rows is not None:
   917|         _write_csv(
   918|             base / "results" / "analysis" / "pattern_presence_file.csv",
   919|             [
   920|                 {
   921|                     "export_run_id": row["export_run_id"],
   922|                     "domain": row.get("domain", domain),
   923|                     "pattern_id": row["pattern_id"],
   924|                 }
   925|                 for row in presence_rows
   926|             ],
   927|         )
   928| 
   929| 
   930| def test_reference_analysis_segment_discovers_domains_without_bundle_outputs(tmp_path):
   931|     segments_root = tmp_path / "segments"
   932|     registry = {"generic": {"output_folder": "generic"}}
   933|     _write_reference_analysis_segment(
   934|         segments_root,
   935|         "generic",
   936|         "line_patterns",
   937|         [{"pattern_id": "g1", "join_hash": "provided_a"}],
   938|         export_run_ids=["generic_file"],
   939|     )
   940| 
   941|     assert discover_domains_for_segment(segments_root, registry, "generic") == {
   942|         "line_patterns"
   943|     }
   944| 
   945| 
   946| def test_reference_analysis_segment_loads_all_view_from_domain_patterns(tmp_path):
   947|     segments_root = tmp_path / "segments"
   948|     registry = {"generic": {"output_folder": "generic"}}
   949|     _write_reference_analysis_segment(
   950|         segments_root,
   951|         "generic",
   952|         "line_patterns",
   953|         [
   954|             {"pattern_id": "g1", "join_hash": "provided_a"},
   955|             {"pattern_id": "g2", "join_hash": "provided_b"},
   956|         ],
   957|         export_run_ids=["generic_file"],
   958|     )
   959| 
   960|     assert load_file_join_hashes(
   961|         segments_root, registry, "generic", "line_patterns", "all"
   962|     ) == {"generic_file": {"provided_a", "provided_b"}}
   963|     assert (
   964|         load_file_join_hashes(
   965|             segments_root, registry, "generic", "line_patterns", "used"
   966|         )
   967|         == {}
   968|     )
   969| 
   970| 
   971| def test_reference_analysis_segment_groups_fallback_by_export_run_id_column(tmp_path):
   972|     segments_root = tmp_path / "segments"
   973|     registry = {"generic": {"output_folder": "generic"}}
   974|     _write_reference_analysis_segment(
   975|         segments_root,
   976|         "generic",
   977|         "line_patterns",
   978|         [
   979|             {"pattern_id": "g1", "join_hash": "provided_a", "export_run_id": "file_a"},
   980|             {"pattern_id": "g2", "join_hash": "provided_b", "export_run_id": "file_b"},
   981|         ],
   982|         export_run_ids=["file_a", "file_b"],
   983|     )
   984| 
   985|     assert load_file_join_hashes(
   986|         segments_root, registry, "generic", "line_patterns", "all"
   987|     ) == {
   988|         "file_a": {"provided_a"},
   989|         "file_b": {"provided_b"},
   990|     }
   991| 
   992| 
```
