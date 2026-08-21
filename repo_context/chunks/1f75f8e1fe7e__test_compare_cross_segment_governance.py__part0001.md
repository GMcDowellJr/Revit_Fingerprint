# Chunk of tests/test_compare_cross_segment_governance.py

- Source relative path: `tests/test_compare_cross_segment_governance.py`
- Chunk: 1 of 5
- Original line range: 1-488
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _seg, test_discover_governance_chain_includes_generic_upstream_roles, test_discover_governance_chain_falls_back_to_collection_label_for_na_client, test_discover_governance_chain_prefers_business_center_label_over_collection_label, test_discover_governance_chain_namespaces_business_center_fallback_from_real_client, test_discover_governance_chain_preserves_collection_scope_within_business_center, test_discover_governance_chain_final_fallback_normalizes_na_spelling, test_discover_governance_chain_collection_match_is_soft_for_client_scope, test_discover_governance_chain_rollup_does_not_wildcard_match_specific_collection, test_scope_level_derivation, test_0000_flows_through_as_literal_enterprise_value, test_bc_0000_spelling_variants_canonicalize_to_0000, test_na_spelled_business_center_labels_normalize_to_blank, test_discover_governance_chain_enterprise_to_project_reaches_every_scope, test_discover_governance_chain_bc_to_project_scoped_to_matching_bc_only, test_discover_governance_chain_enterprise_to_bc_and_client_are_same_role_only, test_enterprise_to_bc_and_sibling_template_survive_with_distinct_run_ids, test_make_comparison_run_id_differs_by_comparison_type_for_same_pair_and_timestamp, test_discover_governance_chain_excludes_generic_from_scope_fanout
- Source SHA-256: 41a98d942cef2b25dee2bd74f79b3ba9f6e871cbbff68d9ef81011f7e3336043
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """Tests for governance semantics in tools/compare_cross_segment.py."""
     2| from __future__ import annotations
     3| 
     4| from pathlib import Path
     5| import sys
     6| 
     7| sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
     8| 
     9| from enterprise_policy import load_enterprise_policy  # noqa: E402
    10| POLICY = load_enterprise_policy()
    11| 
    12| from compare_cross_segment import (  # noqa: E402
    13|     _bc_of,
    14|     _classify_governance_state,
    15|     _comparison_role_semantics,
    16|     _normalize_bc_label,
    17|     _recommended_primary_view,
    18|     _scope_level,
    19|     _usage_interpretable_for_role,
    20|     REUSE_BUCKET_THRESHOLDS,
    21|     _reuse_bucket_for,
    22|     build_pair_domain_work_items,
    23|     build_pattern_reuse_distribution_rows,
    24|     build_union_inventory_rows,
    25|     deduplicate_pairs,
    26|     discover_client_cross_bc,
    27|     discover_domains_for_segment,
    28|     discover_governance_chain,
    29|     discover_sibling_segments,
    30|     discover_within_project,
    31|     drop_legacy_siblings_covered_by_peer_comparisons,
    32|     load_file_join_hashes,
    33|     main as compare_main,
    34|     make_comparison_run_id,
    35|     run_pooled_comparison,
    36|     sort_pair_detail_rows,
    37|     sort_summary_rows,
    38| )
    39| 
    40| 
    41| def _seg(role: str, client: str = "Acme", unit: str = "imperial", discipline: str = "Arch"):
    42|     return {
    43|         "governance_role": role,
    44|         "client_label": client,
    45|         "unit_system": unit,
    46|         "discipline_label": discipline,
    47|         "run_type": "bundle",
    48|     }
    49| 
    50| 
    51| def test_discover_governance_chain_includes_generic_upstream_roles():
    52|     manifest = {
    53|         "g": _seg("Generic", client="Global"),
    54|         "gh": _seg("Generic-Host", client="Global"),
    55|         "t": _seg("Template"),
    56|         "c": _seg("Container"),
    57|         "p": _seg("Project"),
    58|     }
    59| 
    60|     pairs = set(discover_governance_chain(POLICY, manifest))
    61| 
    62|     assert ("g", "t", "generic_to_template") in pairs
    63|     assert ("g", "c", "generic_to_container") in pairs
    64|     assert ("g", "p", "generic_to_project") in pairs
    65|     assert ("gh", "t", "generic_to_template") in pairs
    66|     assert ("t", "p", "template_to_project") in pairs
    67|     assert ("t", "c", "template_to_container") in pairs
    68|     assert ("c", "p", "container_to_project") in pairs
    69| 
    70| 
    71| def test_discover_governance_chain_falls_back_to_collection_label_for_na_client():
    72|     manifest = {
    73|         "bc_t": {**_seg("Template", client="__NOT_APPLICABLE__"), "collection_label": "BC_2270 Standards"},
    74|         "bc_c": {**_seg("Container", client="n/a"), "collection_label": "BC_2270 Standards"},
    75|         "acme_t": _seg("Template", client="Acme"),
    76|         "acme_p": _seg("Project", client="Acme"),
    77|     }
    78| 
    79|     pairs = set(discover_governance_chain(POLICY, manifest))
    80| 
    81|     # BC_2270's Template/Container (client blank/NA, various spellings) group
    82|     # with each other via collection_label instead of pooling under "".
    83|     assert ("bc_t", "bc_c", "template_to_container") in pairs
    84|     # A real client is entirely unaffected by the fallback.
    85|     assert ("acme_t", "acme_p", "template_to_project") in pairs
    86|     # BC content must not cross-pollinate with an unrelated real client pool.
    87|     assert ("bc_t", "acme_p", "template_to_project") not in pairs
    88| 
    89| 
    90| def test_discover_governance_chain_prefers_business_center_label_over_collection_label():
    91|     manifest = {
    92|         "bc_t": {
    93|             **_seg("Template", client="__NOT_APPLICABLE__"),
    94|             "business_center_label": "BC_2270",
    95|             "collection_label": "BC_2270 Standards",
    96|         },
    97|         "bc_c": {
    98|             **_seg("Container", client="n/a"),
    99|             "business_center_label": "BC_2270",
   100|             "collection_label": "BC_2270 Standards",
   101|         },
   102|         "other_bc_t": {
   103|             **_seg("Template", client=""),
   104|             "business_center_label": "BC_9999",
   105|             "collection_label": "BC_2270 Standards",
   106|         },
   107|         "acme_t": _seg("Template", client="Acme"),
   108|         "acme_p": _seg("Project", client="Acme"),
   109|     }
   110| 
   111|     pairs = set(discover_governance_chain(POLICY, manifest))
   112| 
   113|     # BC_2270's Template/Container (client blank/NA) now group via
   114|     # business_center_label, not collection_label.
   115|     assert ("bc_t", "bc_c", "template_to_container") in pairs
   116|     # A different business_center_label sharing the same collection_label
   117|     # must NOT be pooled together now that business_center_label wins.
   118|     assert ("other_bc_t", "bc_c", "template_to_container") not in pairs
   119|     # A real client is entirely unaffected by the fallback.
   120|     assert ("acme_t", "acme_p", "template_to_project") in pairs
   121| 
   122| 
   123| def test_discover_governance_chain_namespaces_business_center_fallback_from_real_client():
   124|     # A real client whose name happens to match a business_center_label text
   125|     # (e.g. both literally "BC_2270") must not be pooled with the
   126|     # business-center-scoped rows that fall back to that same text via
   127|     # business_center_label. client_label and business_center_label are
   128|     # distinct cut dimensions with independent namespaces.
   129|     manifest = {
   130|         "bc_t": {
   131|             **_seg("Template", client="__NOT_APPLICABLE__"),
   132|             "business_center_label": "BC_2270",
   133|         },
   134|         "bc_c": {
   135|             **_seg("Container", client="n/a"),
   136|             "business_center_label": "BC_2270",
   137|         },
   138|         "real_client_t": _seg("Template", client="BC_2270"),
   139|         "real_client_p": _seg("Project", client="BC_2270"),
   140|     }
   141| 
   142|     pairs = set(discover_governance_chain(POLICY, manifest))
   143| 
   144|     # The business-center rows still group with each other.
   145|     assert ("bc_t", "bc_c", "template_to_container") in pairs
   146|     # The real "BC_2270" client rows still group with each other.
   147|     assert ("real_client_t", "real_client_p", "template_to_project") in pairs
   148|     # But the two namespaces must never cross-pollinate despite sharing text.
   149|     assert ("bc_t", "real_client_p", "template_to_project") not in pairs
   150|     assert ("real_client_t", "bc_c", "template_to_container") not in pairs
   151| 
   152| 
   153| def test_discover_governance_chain_preserves_collection_scope_within_business_center():
   154|     # A single business center can house more than one named collection (its
   155|     # own general standards plus a separately-named legacy collection, per
   156|     # build_segment_manifest.py's collection_label cut dimension). Two rows
   157|     # sharing business_center_label but differing in collection_label must
   158|     # not be pooled together, or Page's standards and the firm-wide
   159|     # "InternalEnterprise Standards" collection (both business_center=BC_0000 in
   160|     # practice) would get spurious template_to_project/container_to_project
   161|     # pairs against each other.
   162|     manifest = {
   163|         "page_t": {
   164|             **_seg("Template", client="__NOT_APPLICABLE__"),
   165|             "business_center_label": "BC_0000",
   166|             "collection_label": "Page Standards",
   167|         },
   168|         "page_c": {
   169|             **_seg("Container", client="n/a"),
   170|             "business_center_label": "BC_0000",
   171|             "collection_label": "Page Standards",
   172|         },
   173|         "internalenterprise_t": {
   174|             **_seg("Template", client="__NOT_APPLICABLE__"),
   175|             "business_center_label": "BC_0000",
   176|             "collection_label": "InternalEnterprise Standards",
   177|         },
   178|         "internalenterprise_c": {
   179|             **_seg("Container", client="n/a"),
   180|             "business_center_label": "BC_0000",
   181|             "collection_label": "InternalEnterprise Standards",
   182|         },
   183|         # No collection_label at all — still groups purely on business_center,
   184|         # unaffected by the new collection-scoping.
   185|         "bc_only_t": {
   186|             **_seg("Template", client="__NOT_APPLICABLE__"),
   187|             "business_center_label": "BC_2270",
   188|         },
   189|         "bc_only_c": {
   190|             **_seg("Container", client="n/a"),
   191|             "business_center_label": "BC_2270",
   192|         },
   193|     }
   194| 
   195|     pairs = set(discover_governance_chain(POLICY, manifest))
   196| 
   197|     # Same business_center AND same collection: still group.
   198|     assert ("page_t", "page_c", "template_to_container") in pairs
   199|     assert ("internalenterprise_t", "internalenterprise_c", "template_to_container") in pairs
   200|     # Same business_center, DIFFERENT collection: must not cross-pollinate.
   201|     assert ("page_t", "internalenterprise_c", "template_to_container") not in pairs
   202|     assert ("internalenterprise_t", "page_c", "template_to_container") not in pairs
   203|     # business_center-only rows (no collection_label) are unaffected.
   204|     assert ("bc_only_t", "bc_only_c", "template_to_container") in pairs
   205| 
   206| 
   207| def test_discover_governance_chain_final_fallback_normalizes_na_spelling():
   208|     # When client_label, business_center_label, and collection_label are all
   209|     # blank/NA, the final fallback must return a canonical blank key, not the
   210|     # raw NA token. Two rows spelled differently ("__NOT_APPLICABLE__" vs
   211|     # "n/a") but otherwise identically blank must still group together —
   212|     # every NA spelling is documented as equivalent to blank for grouping.
   213|     manifest = {
   214|         "na_t": _seg("Template", client="__NOT_APPLICABLE__"),
   215|         "na_c": _seg("Container", client="n/a"),
   216|         "na_p": _seg("Project", client="NA"),
   217|     }
   218| 
   219|     pairs = set(discover_governance_chain(POLICY, manifest))
   220| 
   221|     assert ("na_t", "na_c", "template_to_container") in pairs
   222|     assert ("na_t", "na_p", "template_to_project") in pairs
   223|     assert ("na_c", "na_p", "container_to_project") in pairs
   224| 
   225| 
   226| def test_discover_governance_chain_collection_match_is_soft_for_client_scope():
   227|     # Mirrors real data: a client's own Container/Template rows are tagged
   228|     # with that client's collection_label (e.g. "ClientBeta Standards"), but its
   229|     # Project rows typically carry no collection_label at all. Hard-
   230|     # partitioning by collection_label would put those in different _key()
   231|     # buckets and silently stop producing template_to_project/
   232|     # container_to_project pairs — the tool's primary comparison. A soft
   233|     # match (required only when both sides are populated) must still pair
   234|     # them, while two DIFFERENT populated collections under the same client
   235|     # must not cross-pollinate.
   236|     manifest = {
   237|         "clientbeta_t": {**_seg("Template", client="ClientBeta"), "collection_label": "ClientBeta Standards"},
   238|         "clientbeta_c": {**_seg("Container", client="ClientBeta"), "collection_label": "ClientBeta Standards"},
   239|         # Project rows: no collection_label at all, matching real data.
   240|         "clientbeta_p": _seg("Project", client="ClientBeta"),
   241|         # A second, differently-named collection under the SAME client must
   242|         # not silently pair with "ClientBeta Standards" — two populated,
   243|         # different values are a genuine mismatch.
   244|         "clientbeta_legacy_t": {**_seg("Template", client="ClientBeta"), "collection_label": "ClientBeta Legacy"},
   245|     }
   246| 
   247|     pairs = set(discover_governance_chain(POLICY, manifest))
   248| 
   249|     # Collection-tagged standards still pair with collection-blank usage.
   250|     assert ("clientbeta_t", "clientbeta_p", "template_to_project") in pairs
   251|     assert ("clientbeta_c", "clientbeta_p", "container_to_project") in pairs
   252|     assert ("clientbeta_t", "clientbeta_c", "template_to_container") in pairs
   253|     # Two different, both-populated collections under the same client don't
   254|     # cross-pollinate.
   255|     assert ("clientbeta_legacy_t", "clientbeta_c", "template_to_container") not in pairs
   256|     # But the differently-collectioned template still reaches the
   257|     # collection-blank project, since blank is permissive on one side.
   258|     assert ("clientbeta_legacy_t", "clientbeta_p", "template_to_project") in pairs
   259| 
   260| 
   261| def test_discover_governance_chain_rollup_does_not_wildcard_match_specific_collection():
   262|     # Mirrors real data: business_center_label="BC_0000" hosts two distinct
   263|     # collections (Page Standards, InternalEnterprise Standards). build_segment_manifest.py
   264|     # keeps a runnable, collection-blank aggregate Template alongside its
   265|     # collection-specific children whenever the aggregate's population isn't
   266|     # identical to either child's (i.e. the BC hosts more than one
   267|     # collection). That aggregate's blank collection_label must NOT act as a
   268|     # wildcard against a specific-collection Container — doing so would mix
   269|     # the pooled (both-collections) population into a comparison meant to
   270|     # isolate one collection's own population.
   271|     manifest = {
   272|         "bc_t_rollup": {
   273|             **_seg("Template", client="__NOT_APPLICABLE__"),
   274|             "business_center_label": "BC_0000",
   275|             "segment_id": "bc_t_rollup",
   276|             "parent_segment_id": "",
   277|         },
   278|         # This child's parent_segment_id points back at bc_t_rollup and
   279|         # carries a populated collection_label — that's what marks
   280|         # bc_t_rollup as a roll-up rather than a genuinely collection-blank
   281|         # segment.
   282|         "bc_t_page": {
   283|             **_seg("Template", client="__NOT_APPLICABLE__"),
   284|             "business_center_label": "BC_0000",
   285|             "collection_label": "Page Standards",
   286|             "segment_id": "bc_t_page",
   287|             "parent_segment_id": "bc_t_rollup",
   288|         },
   289|         "bc_c_page": {
   290|             **_seg("Container", client="n/a"),
   291|             "business_center_label": "BC_0000",
   292|             "collection_label": "Page Standards",
   293|         },
   294|         "bc_c_internalenterprise": {
   295|             **_seg("Container", client="n/a"),
   296|             "business_center_label": "BC_0000",
   297|             "collection_label": "InternalEnterprise Standards",
   298|         },
   299|     }
   300| 
   301|     pairs = set(discover_governance_chain(POLICY, manifest))
   302| 
   303|     # The roll-up must not wildcard-match ANY specific-collection Container.
   304|     assert ("bc_t_rollup", "bc_c_page", "template_to_container") not in pairs
   305|     assert ("bc_t_rollup", "bc_c_internalenterprise", "template_to_container") not in pairs
   306|     # A collection-specific Template still correctly pairs with the matching
   307|     # collection's Container, and not with a different one.
   308|     assert ("bc_t_page", "bc_c_page", "template_to_container") in pairs
   309|     assert ("bc_t_page", "bc_c_internalenterprise", "template_to_container") not in pairs
   310| 
   311| 
   312| def test_scope_level_derivation():
   313|     # Scope level is derived from explicit, literal client_label/
   314|     # business_center_label values -- orthogonal to governance_role.
   315|     assert _scope_level({**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"}, POLICY) == "enterprise"
   316|     assert _scope_level({**_seg("Template", client="InternalEnterprise"), "business_center_label": "2270"}, POLICY) == "business_center"
   317|     assert _scope_level({**_seg("Project", client="Acme"), "business_center_label": "2270"}, POLICY) == "client_business_center"
   318|     # Role never enters into the classification -- a client+bc segment can
   319|     # be Template, Container, or Project; scope alone doesn't imply role.
   320|     assert _scope_level({**_seg("Container", client="Acme"), "business_center_label": "2270"}, POLICY) == "client_business_center"
   321|     # Either dimension not cut at all (blank) is a roll-up, not a defined
   322|     # scope level.
   323|     assert _scope_level(_seg("Template", client=""), POLICY) is None
   324|     assert _scope_level({**_seg("Template", client="Acme"), "business_center_label": ""}, POLICY) is None
   325| 
   326| 
   327| def test_0000_flows_through_as_literal_enterprise_value():
   328|     # Under the explicit-metadata contract, "0000" is a real, literal
   329|     # business_center_label value (the Enterprise identity) -- it must not
   330|     # be folded to blank anymore.
   331|     assert _normalize_bc_label("0000") == "0000"
   332|     assert _normalize_bc_label("BC_1234") == "BC_1234"
   333|     assert _bc_of({**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"}) == "0000"
   334|     assert _scope_level({**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"}, POLICY) == "enterprise"
   335| 
   336| 
   337| def test_bc_0000_spelling_variants_canonicalize_to_0000():
   338|     # "0000"/"BC_0000" (any case) are spelling variants of the same
   339|     # enterprise-bookkeeping value elsewhere in the pipeline (e.g. the
   340|     # extraction completeness gate documents both) -- they must canonicalize
   341|     # to the SAME literal "0000", not fragment into two distinct-looking
   342|     # business centers, and must not fold to blank either.
   343|     for token in ("BC_0000", "bc_0000", "Bc_0000"):
   344|         assert _normalize_bc_label(token) == "0000"
   345|         row = {**_seg("Template", client="InternalEnterprise"), "business_center_label": token}
   346|         assert _bc_of(row) == "0000"
   347|         assert _scope_level(row, POLICY) == "enterprise"
   348| 
   349| 
   350| def test_na_spelled_business_center_labels_normalize_to_blank():
   351|     # A missing business_center_label spelled as an NA token (n/a, NA,
   352|     # __NOT_APPLICABLE__, ...) must still normalize to blank -- this is a
   353|     # distinct mechanism (is_blank_or_na()) from the removed "0000" fold, and
   354|     # a blank bc means the segment is a roll-up (not cut on bc), not a
   355|     # defined scope level.
   356|     for token in ("n/a", "NA", "__NOT_APPLICABLE__", "not applicable"):
   357|         assert _normalize_bc_label(token) == ""
   358|         row = {**_seg("Template", client="InternalEnterprise"), "business_center_label": token}
   359|         assert _bc_of(row) == ""
   360|         assert _scope_level(row, POLICY) is None
   361| 
   362| 
   363| def test_discover_governance_chain_enterprise_to_project_reaches_every_scope():
   364|     # An enterprise-scoped Template/Container (InternalEnterprise/"0000") has no real
   365|     # client/bc narrowing of its own and applies across the whole business —
   366|     # it must reach every Project regardless of that project's own client/bc
   367|     # scope.
   368|     manifest = {
   369|         "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
   370|         "proj_a": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234"},
   371|         "proj_b": {**_seg("Project", client="Widgets"), "business_center_label": "BC_9999"},
   372|     }
   373| 
   374|     pairs = set(discover_governance_chain(POLICY, manifest))
   375| 
   376|     assert ("ent_t", "proj_a", "enterprise_to_project") in pairs
   377|     assert ("ent_t", "proj_b", "enterprise_to_project") in pairs
   378| 
   379| 
   380| def test_discover_governance_chain_bc_to_project_scoped_to_matching_bc_only():
   381|     # A business_center-scoped Template (InternalEnterprise + a real bc) only reaches
   382|     # Projects within the SAME (normalized) business center — not projects
   383|     # in a different bc, even though both are still "downstream" of the
   384|     # enterprise.
   385|     manifest = {
   386|         "bc_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234"},
   387|         "proj_same_bc": {**_seg("Project", client="Acme"), "business_center_label": "BC_1234"},
   388|         "proj_other_bc": {**_seg("Project", client="Widgets"), "business_center_label": "BC_9999"},
   389|     }
   390| 
   391|     pairs = set(discover_governance_chain(POLICY, manifest))
   392| 
   393|     assert ("bc_t", "proj_same_bc", "bc_to_project") in pairs
   394|     assert ("bc_t", "proj_other_bc", "bc_to_project") not in pairs
   395| 
   396| 
   397| def test_discover_governance_chain_enterprise_to_bc_and_client_are_same_role_only():
   398|     # enterprise_to_bc / enterprise_to_client are standard-to-standard
   399|     # (Template vs Template, Container vs Container) — they must not mix
   400|     # roles (Template vs Container).
   401|     manifest = {
   402|         "ent_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "0000"},
   403|         "ent_c": {**_seg("Container", client="InternalEnterprise"), "business_center_label": "0000"},
   404|         "bc_t": {**_seg("Template", client="InternalEnterprise"), "business_center_label": "BC_1234"},
   405|         "bc_c": {**_seg("Container", client="InternalEnterprise"), "business_center_label": "BC_1234"},
   406|         # No business_center_label at all -- a client-wide roll-up, a valid
   407|         # (distinct) enterprise_to_client target alongside any
   408|         # client_business_center-scoped standard the client might also have.
   409|         "client_t": _seg("Template", client="Acme"),
   410|     }
   411| 
   412|     pairs = set(discover_governance_chain(POLICY, manifest))
   413| 
   414|     assert ("ent_t", "bc_t", "enterprise_to_bc") in pairs
   415|     assert ("ent_c", "bc_c", "enterprise_to_bc") in pairs
   416|     assert ("ent_t", "bc_c", "enterprise_to_bc") not in pairs
   417|     assert ("ent_t", "client_t", "enterprise_to_client") in pairs
   418| 
   419| 
   420| def test_enterprise_to_bc_and_sibling_template_survive_with_distinct_run_ids():
   421|     # An enterprise (InternalEnterprise/0000) standard and a real-BC standard of the
   422|     # same role sharing a parent_segment_id get paired BOTH as
   423|     # sibling_templates (discover_sibling_segments, symmetric Jaccard) AND
   424|     # as enterprise_to_bc (discover_governance_chain, directed reference-
   425|     # union containment) -- these are genuinely distinct measurements of the
   426|     # same two segments, not duplicates, so neither drop_legacy_siblings_
   427|     # covered_by_peer_comparisons() nor anything else should suppress
   428|     # either row. "seg_0000" sorts before "seg_bc001" alphabetically, so
   429|     # both discover_sibling_segments()'s sorted-ID pairing and
   430|     # discover_governance_chain()'s enterprise-then-bc pairing land on the
   431|     # exact same (seg_a, seg_b) orientation -- the scenario that used to
   432|     # collide on comparison_run_id.
   433|     manifest = {
   434|         "seg_0000": {
   435|             **_seg("Template", client="InternalEnterprise"),
   436|             "business_center_label": "0000",
   437|             "parent_segment_id": "parent1",
   438|         },
   439|         "seg_bc001": {
   440|             **_seg("Template", client="InternalEnterprise"),
   441|             "business_center_label": "BC1",
   442|             "parent_segment_id": "parent1",
   443|         },
   444|     }
   445| 
   446|     sibling_pairs = discover_sibling_segments(POLICY, manifest)
   447|     governance_pairs = discover_governance_chain(POLICY, manifest)
   448|     assert ("seg_0000", "seg_bc001", "sibling_templates") in sibling_pairs
   449|     assert ("seg_0000", "seg_bc001", "enterprise_to_bc") in governance_pairs
   450| 
   451|     pairs = deduplicate_pairs(sibling_pairs + governance_pairs)
   452|     pairs = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
   453| 
   454|     surviving = {ctype for a, b, ctype in pairs if {a, b} == {"seg_0000", "seg_bc001"}}
   455|     assert surviving == {"sibling_templates", "enterprise_to_bc"}
   456| 
   457|     executed_utc = "2026-07-20T00:00:00Z"
   458|     ids = {
   459|         ctype: make_comparison_run_id("seg_0000", "seg_bc001", executed_utc, ctype)
   460|         for ctype in surviving
   461|     }
   462|     assert len(set(ids.values())) == len(ids)
   463| 
   464| 
   465| def test_make_comparison_run_id_differs_by_comparison_type_for_same_pair_and_timestamp():
   466|     executed_utc = "2026-07-20T00:00:00Z"
   467|     id_a = make_comparison_run_id("s1", "s2", executed_utc, "sibling_templates")
   468|     id_b = make_comparison_run_id("s1", "s2", executed_utc, "enterprise_to_bc")
   469|     assert id_a != id_b
   470|     # Deterministic given identical inputs.
   471|     assert id_a == make_comparison_run_id("s1", "s2", executed_utc, "sibling_templates")
   472| 
   473| 
   474| def test_discover_governance_chain_excludes_generic_from_scope_fanout():
   475|     # Generic/Generic-Host already pairs unconditionally against every
   476|     # Template/Container/Project via the existing generic_ids loop — it must
   477|     # not also get a redundant enterprise/bc/client-scoped edge.
   478|     manifest = {
   479|         "g": _seg("Generic", client="Global"),
   480|         "proj": _seg("Project", client="Acme"),
   481|     }
   482| 
   483|     pairs = set(discover_governance_chain(POLICY, manifest))
   484| 
   485|     assert ("g", "proj", "generic_to_project") in pairs
   486|     assert not any(ctype in ("enterprise_to_project", "bc_to_project") for _a, _b, ctype in pairs if _a == "g")
   487| 
   488| 
```
