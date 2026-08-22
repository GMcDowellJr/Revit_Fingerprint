# Chunk of tools/governance_evidence_package.py

- Source relative path: `tools/governance_evidence_package.py`
- Chunk: 3 of 6
- Original line range: 539-938
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_evidence_map, build_evidence_map.p, build_evidence_map._output_local_path
- Source SHA-256: 2fece0426163550ef83e302b52b9f002b12123e12eb35430df07c3d1f4c4b1f3
- Starts inside symbol: no
- Ends inside symbol: build_evidence_map

```
   539| def build_evidence_map(
   540|     *,
   541|     schema_version: str,
   542|     input_paths: dict,          # artifact_id -> Optional[Path]
   543|     input_present: dict,        # artifact_id -> bool
   544|     output_paths: dict,         # artifact_id -> Path
   545|     sibling_paths: dict,        # artifact_id -> Path (inferred, not CLI args)
   546|     sibling_present: dict,      # artifact_id -> bool
   547|     package_schema_version: str = PACKAGE_SCHEMA_VERSION,
   548|     # The actual schema_version governance_package_manifest.json and
   549|     # governance_package_health.json were written with -- i.e. the runtime
   550|     # value (args.package_schema_version), not the module default. Those two
   551|     # files may be written with an overridden --package-schema-version; their
   552|     # evidence-map entries must declare the same value they actually contain,
   553|     # not PACKAGE_SCHEMA_VERSION unconditionally.
   554|     file_inventory_schema_version: str = FILE_INVENTORY_SCHEMA_VERSION,
   555|     out_dir=None,  # D-034/PR review finding: Path this run is writing --out to,
   556|     # used only to compute output_local_path for the four static docs D-034
   557|     # copies alongside this evidence map (see _artifact()'s output_local_path
   558|     # docstring note). Omitted (None, the default) adds no output_local_path
   559|     # field, so a caller that hasn't adopted this keeps prior behavior.
   560| ) -> dict:
   561|     artifacts = []
   562| 
   563|     def p(paths, key):
   564|         v = paths.get(key)
   565|         return str(v) if v else None
   566| 
   567|     def _output_local_path(sibling_key: str):
   568|         # PR review finding: str(Path(out_dir) / name) baked the ORIGINAL
   569|         # machine's --out path (absolute, if --out was) into the package
   570|         # itself -- wrong the moment the whole --out directory is moved or
   571|         # copied elsewhere, which is the exact self-contained-package
   572|         # scenario this field exists for. The D-034 copy always sits flat,
   573|         # directly beside governance_evidence_map.json, so the correct
   574|         # package-relative path is just the basename.
   575|         if out_dir is None or not sibling_present.get(sibling_key, False):
   576|             return None
   577|         return sibling_paths[sibling_key].name
   578| 
   579|     artifacts.append(_artifact(
   580|         "cross_segment_summary", p(input_paths, "cross_segment_summary"), "csv", True,
   581|         input_present.get("cross_segment_summary", False), "compare_cross_segment.py",
   582|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   583|         "primary pairwise comparison evidence -- feeds build_cascade(), "
   584|         "build_client_summary(), render_discipline_section()",
   585|         "one row per (comparison_run_id, segment_id_a, segment_id_b, domain, "
   586|         "comparison_type) directed pair",
   587|         ["comparison_run_id", "segment_id_a", "segment_id_b", "domain", "comparison_type"],
   588|         ["segment_id_a", "segment_id_b"],
   589|         ["segment_id_a", "segment_id_b (join to build_segment_manifest.py's segment_id)"],
   590|         ["containment/Jaccard between two segments for a domain",
   591|          "which comparison_type bucket (directed cascade stage, sibling, within_project) a pair belongs to"],
   592|         ["business_center_label/discipline_label/collection_label on the pooled "
   593|          "(focal-vs-pool) side -- see cross_segment_pooled.csv for pool-relative reads"],
   594|         ["comparison_type is not a closed enum -- see compare_cross_segment.py's "
   595|          "DIRECTED_TYPES/GOVERNANCE_STATE_DIRECTED_TYPES/discover_* literal emissions. "
   596|          "An unrecognized value is excluded from cascade scoring by build_cascade() "
   597|          "and only surfaces via governance_package_health.json's comparison_type_coverage, "
   598|          "not via this CSV itself."],
   599|         _BLANK_STRING_NULL_SEMANTICS,
   600|         ["cross_segment_pooled", "governance_domain_summary", "governance_client_summary"],
   601|         required_before_conclusions=True,
   602|     ))
   603| 
   604|     artifacts.append(_artifact(
   605|         "cross_segment_pooled", p(input_paths, "cross_segment_pooled"), "csv", True,
   606|         input_present.get("cross_segment_pooled", False), "compare_cross_segment.py",
   607|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   608|         "focal-vs-pool containment evidence for client/business-center rollups "
   609|         "-- feeds build_client_summary() and gt_by_scope/gc_by_scope/gp_by_scope",
   610|         "one row per (comparison_run_id, segment_id, pool_scope) -- a focal "
   611|         "segment against a named pool",
   612|         ["comparison_run_id", "segment_id", "pool_scope", "domain"],
   613|         ["segment_id"], ["segment_id (join to build_segment_manifest.py's segment_id)"],
   614|         ["how a focal segment's vocabulary relates to its named pool (parent_sibling/bc/client grain)"],
   615|         ["discipline_label and collection_label are not columns on this file"],
   616|         ["build_client_summary() reads client_label/n_files_focal from every "
   617|          "pool_scope grain without filtering by pool_scope (see "
   618|          "docs/governance_narrative_scope_gap_audit.md finding A2); safe today "
   619|          "only because those two fields are pool-scope-invariant, not because "
   620|          "pool_scope is checked at the read site."],
   621|         _BLANK_STRING_NULL_SEMANTICS,
   622|         ["cross_segment_summary", "governance_client_summary"],
   623|         required_before_conclusions=True,
   624|     ))
   625| 
   626|     artifacts.append(_artifact(
   627|         "cross_segment_governance_states", p(input_paths, "cross_segment_governance_states"), "csv", False,
   628|         input_present.get("cross_segment_governance_states", False), "compare_cross_segment.py",
   629|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   630|         "row-level provided/used/passive/missing/local-active classification, "
   631|         "detail grain behind cross_segment_governance_state_summary.csv",
   632|         "one row per comparison + governance-state classification",
   633|         ["domain", "comparison_type", "state"], ["segment_id_a", "segment_id_b"], [],
   634|         ["individual state transitions (provided_and_used, provided_but_passive, "
   635|          "etc.) per pattern"],
   636|         ["aggregate shares/rankings -- use cross_segment_governance_state_summary.csv for that"],
   637|         ["if absent, build_governance_state_summary() falls back to whatever "
   638|          "cross_segment_governance_state_summary.csv rows are available; if both "
   639|          "are absent, render_header()'s state_note says provided/used/passive/"
   640|          "missing/local signals are inferred only indirectly."],
   641|         _BLANK_STRING_NULL_SEMANTICS,
   642|         ["cross_segment_governance_state_summary", "governance_domain_summary"],
   643|         required_before_conclusions=False,
   644|     ))
   645| 
   646|     artifacts.append(_artifact(
   647|         "cross_segment_governance_state_summary", p(input_paths, "cross_segment_governance_state_summary"), "csv", False,
   648|         input_present.get("cross_segment_governance_state_summary", False), "compare_cross_segment.py",
   649|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   650|         "compact per-domain governance-state aggregate consumed by build_governance_state_summary()",
   651|         "one row per (domain, comparison_type) compact aggregate",
   652|         ["domain", "comparison_type"], [], [],
   653|         ["provided_to_used/passive/missing shares and counts per domain"],
   654|         ["row-level per-pattern detail -- use cross_segment_governance_states.csv for that"],
   655|         ["if upstream rows are not deduplicated to unique patterns, count fields "
   656|          "(provided_and_used_count etc.) should be read as comparison-state rows, "
   657|          "not unique-pattern counts -- see render_limitations()'s state_note."],
   658|         _BLANK_STRING_NULL_SEMANTICS,
   659|         ["cross_segment_governance_states", "governance_domain_summary"],
   660|         required_before_conclusions=False,
   661|     ))
   662| 
   663|     artifacts.append(_artifact(
   664|         "cross_segment_delta", p(input_paths, "cross_segment_delta"), "csv", False,
   665|         input_present.get("cross_segment_delta", False), "compare_cross_segment.py",
   666|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   667|         "legacy delta-pattern summary, only consulted when "
   668|         "cross_segment_governance_state_summary.csv is absent (main()'s "
   669|         "`elif delta_summary:` branch)",
   670|         "one row per legacy delta comparison", ["domain"], [], [],
   671|         ["legacy provided/local drift signal when governance-state outputs are unavailable"],
   672|         ["provided/used/passive/missing state breakdown -- superseded by governance-state outputs"],
   673|         ["main()'s section assembly is `if governance_state_summary: ... elif "
   674|          "delta_summary: ...` -- this file's section is never rendered when "
   675|          "governance-state outputs are also supplied, even if both are passed on the CLI."],
   676|         _BLANK_STRING_NULL_SEMANTICS,
   677|         ["cross_segment_governance_state_summary"],
   678|         required_before_conclusions=False,
   679|     ))
   680| 
   681|     artifacts.append(_artifact(
   682|         "file_metadata", p(input_paths, "file_metadata"), "csv", False,
   683|         input_present.get("file_metadata", False), "fingerprint pipeline (file_metadata.csv export)",
   684|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   685|         "corpus composition (file counts by role/discipline/client) via load_corpus_counts()",
   686|         "one row per Revit file", ["governance_role", "discipline_label", "client_label"],
   687|         [], [],
   688|         ["how many Template/Container/Project files exist, discipline/client vocabulary present"],
   689|         ["per-domain comparison scores -- this file has no domain column"],
   690|         ["if absent, corpus counts default to zero and disc/client lists render "
   691|          "as 'Unknown' in render_header()."],
   692|         _BLANK_STRING_NULL_SEMANTICS,
   693|         ["governance_narrative_context"],
   694|         required_before_conclusions=False,
   695|     ))
   696| 
   697|     artifacts.append(_artifact(
   698|         "segment_manifest", p(input_paths, "segment_manifest"), "csv", False,
   699|         input_present.get("segment_manifest", False), "build_segment_manifest.py",
   700|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   701|         "lets build_cascade()'s within_project score_reliability p10/p90 capture "
   702|         "resolve a redundant_single_child-demoted enterprise-wide root segment to "
   703|         "its population-identical runnable descendant via _resolve_runnable_segment() "
   704|         "(imported from compare_cross_segment.py), instead of finding no unscoped "
   705|         "segment at all",
   706|         "one row per segment_id in the full segmentation lattice",
   707|         ["segment_id", "run_type"], ["segment_id"],
   708|         ["segment_id (join to cross_segment_summary.csv's segment_id_a/_b)"],
   709|         ["whether a segment_id is directly runnable (run_type in bundle/reference) "
   710|          "or redundant_single_child to a population-identical descendant"],
   711|         ["per-domain comparison scores -- this file has no domain column"],
   712|         ["absent: within_project score_reliability p10/p90 only ever populate from "
   713|          "a row that is directly _is_unscoped_segment() -- the pre-existing, "
   714|          "narrower behavior -- rather than also resolving a demoted root through "
   715|          "its redundant_single_child chain."],
   716|         _BLANK_STRING_NULL_SEMANTICS,
   717|         ["cross_segment_summary", "governance_domain_summary"],
   718|         required_before_conclusions=False,
   719|     ))
   720| 
   721|     artifacts.append(_artifact(
   722|         "client_sector", p(input_paths, "client_sector"), "csv", False,
   723|         input_present.get("client_sector", False), "human-curated (policies/client_sector.csv default)",
   724|         AUTHORITY_USER_PROVIDED_NOTE,
   725|         "client_label -> sector classification driving cross-client convergence "
   726|         "tiering (xc in build_cascade())",
   727|         "one row per client_label", ["client_label", "sector"], ["client_label"], [],
   728|         ["which clients share a sector for cross-client comparison purposes"],
   729|         ["any other cascade stage -- sector affects only cross-client convergence grouping"],
   730|         ["absent/missing file: every client is 'unknown' sector (main() warns to "
   731|          "stderr, does not error); --client-sector has a non-empty default path "
   732|          "(policies/client_sector.csv), so 'not passed on CLI' is not the same "
   733|          "as 'absent' -- see governance_package_health.json's client_sector_status "
   734|          "for the explicit/default/missing distinction. See "
   735|          "docs/governance_narrative_scope_gap_audit.md finding C7."],
   736|         {"*": "Missing client_label from this file simply means unclassified sector, not an error."},
   737|         ["governance_client_summary"],
   738|         required_before_conclusions=False,
   739|     ))
   740| 
   741|     artifacts.append(_artifact(
   742|         "cross_segment_union_inventory", p(input_paths, "cross_segment_union_inventory"), "csv", False,
   743|         input_present.get("cross_segment_union_inventory", False), "compare_cross_segment.py",
   744|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   745|         "feeds render_union_reuse_summary() alongside reuse-distribution/matrix-manifest",
   746|         "one row per union-inventory grain (view_scope/unit_system/domain)",
   747|         ["domain", "view_scope", "unit_system"], [], [],
   748|         ["pattern presence breadth across the corpus for the union/reuse narrative block"],
   749|         [], ["the narrative section is entirely omitted (not blank-rendered) if "
   750|              "this + reuse-distribution + matrix-manifest are all absent -- "
   751|              "render_union_reuse_summary() returns None, not an empty string, "
   752|              "in that case."],
   753|         _BLANK_STRING_NULL_SEMANTICS,
   754|         ["pattern_reuse_distribution", "matrix_output_manifest"],
   755|         required_before_conclusions=False,
   756|     ))
   757| 
   758|     artifacts.append(_artifact(
   759|         "pattern_reuse_distribution", p(input_paths, "pattern_reuse_distribution"), "csv", False,
   760|         input_present.get("pattern_reuse_distribution", False), "compare_cross_segment.py",
   761|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   762|         "feeds render_union_reuse_summary()'s top-20 reuse bucket table by domain",
   763|         "one row per reuse bucket", ["domain", "reuse_bucket"], [], [],
   764|         ["pattern reuse concentration for the union/reuse narrative block"],
   765|         [], ["render_union_reuse_summary() consumes this via a top-20 bucket table "
   766|              "only; full distribution detail beyond that is not summarized."],
   767|         _BLANK_STRING_NULL_SEMANTICS,
   768|         ["cross_segment_union_inventory", "matrix_output_manifest", "pattern_reuse_summary_by_domain"],
   769|         required_before_conclusions=False,
   770|     ))
   771| 
   772|     artifacts.append(_artifact(
   773|         "matrix_output_manifest", p(input_paths, "matrix_output_manifest"), "csv", False,
   774|         input_present.get("matrix_output_manifest", False), "compare_cross_segment.py",
   775|         AUTHORITY_CONVENIENCE_SUMMARY,
   776|         "metadata-only today -- not integrated into narrative content beyond "
   777|         "descriptive bullets; see docs/governance_generator_cross_compare_coverage.md",
   778|         "one row per matrix artifact (matrix_name)", ["matrix_name"], [], [],
   779|         ["which project/portfolio matrices exist and their documented interpretation/known_limitations text"],
   780|         ["no narrative claims currently derive from this file's field content -- "
   781|          "only its presence/absence and matrix_name are used"],
   782|         ["no structured block/status column exists on this file today "
   783|          "(MATRIX_MANIFEST_FIELDS has no status/blocked field) -- see "
   784|          "governance_package_health.json's matrix_manifest.note."],
   785|         _BLANK_STRING_NULL_SEMANTICS,
   786|         ["cross_segment_union_inventory", "pattern_reuse_distribution"],
   787|         required_before_conclusions=False,
   788|     ))
   789| 
   790|     artifacts.append(_artifact(
   791|         "pattern_reuse_summary_by_client", p(input_paths, "pattern_reuse_summary_by_client"), "csv", False,
   792|         input_present.get("pattern_reuse_summary_by_client", False), "compare_cross_segment.py",
   793|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   794|         "feeds render_union_reuse_summary()'s adoption-breadth cut (how many "
   795|         "clients reach a corpus-wide-reused pattern per domain) -- additive to, "
   796|         "and independent of, the distinct-pattern reuse table sourced from "
   797|         "pattern_reuse_distribution.csv",
   798|         "one row per (view_scope, governance_role, client_label, "
   799|         "discipline_label, unit_system, domain, reuse_bucket, bucket_basis) "
   800|         "-- n_patterns is a bucket_basis-scoped occurrence count, not a "
   801|         "distinct-pattern count",
   802|         ["domain", "client_label", "reuse_bucket"], [], [],
   803|         ["how many of a domain's clients have at least one corpus-wide-reused pattern"],
   804|         ["distinct-pattern counts across the whole corpus -- use "
   805|          "pattern_reuse_distribution.csv for that; this file is grouped by "
   806|          "client_label so the same pattern is counted once per client, not once total"],
   807|         ["pattern_reuse_summary_by_domain.csv (the by-domain sibling of this file) "
   808|          "is deliberately not consumed -- its n_patterns duplicates the "
   809|          "corpus-wide reuse signal the distinct-pattern table already reports."],
   810|         _BLANK_STRING_NULL_SEMANTICS,
   811|         ["pattern_reuse_distribution", "cross_segment_union_inventory"],
   812|         required_before_conclusions=False,
   813|     ))
   814| 
   815|     artifacts.append(_artifact(
   816|         "project_union_jaccard_matrix", p(input_paths, "project_union_jaccard_matrix"), "csv", False,
   817|         input_present.get("project_union_jaccard_matrix", False), "compare_cross_segment.py",
   818|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   819|         "feeds the Project Portfolio section's footprint-identity paragraph "
   820|         "(render_project_portfolio_section())",
   821|         "one row per (row_id, column_id, view_scope, domain) matrix cell; "
   822|         "ALL_DOMAINS rows carry the system-level union_jaccard used in the narrative",
   823|         ["row_id", "column_id", "view_scope", "domain"], [], [],
   824|         ["whether two projects' systems contain the same canonical patterns "
   825|          "(exact footprint overlap), independent of file-pair identity"],
   826|         ["typical file-to-file similarity -- use project_mean_file_pair_jaccard_matrix.csv's "
   827|          "signal, folded into project_fragmentation_diagnostic.csv, for that"],
   828|         ["symmetric matrix -- both (a, b) and (b, a) rows are emitted; the "
   829|          "narrative dedupes to one row per unordered project pair"],
   830|         _BLANK_STRING_NULL_SEMANTICS,
   831|         ["project_density_similarity_matrix", "project_fragmentation_diagnostic", "matrix_output_manifest"],
   832|         required_before_conclusions=False,
   833|     ))
   834| 
   835|     artifacts.append(_artifact(
   836|         "project_density_similarity_matrix", p(input_paths, "project_density_similarity_matrix"), "csv", False,
   837|         input_present.get("project_density_similarity_matrix", False), "compare_cross_segment.py",
   838|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   839|         "feeds the Project Portfolio section's density-similarity paragraph, "
   840|         "cross-referenced against project_union_jaccard_matrix.csv for the "
   841|         "\"same shape, different content\" caveat when supplied",
   842|         "one row per (row_id, column_id, view_scope, domain) matrix cell; "
   843|         "ALL_DOMAINS rows carry the system-level density_similarity used in the narrative",
   844|         ["row_id", "column_id", "view_scope", "domain"], [], [],
   845|         ["whether two projects populate the same domains to a similar degree "
   846|          "(occupancy-count cosine similarity), independent of exact pattern identity"],
   847|         ["exact pattern identity -- high density similarity with low "
   848|          "union_jaccard means same shape, different content, not the same content"],
   849|         ["symmetric matrix, same dedup treatment as project_union_jaccard_matrix.csv; "
   850|          "the same-shape/different-content cross-check is unavailable when "
   851|          "project_union_jaccard_matrix.csv is not also supplied"],
   852|         _BLANK_STRING_NULL_SEMANTICS,
   853|         ["project_union_jaccard_matrix", "matrix_output_manifest"],
   854|         required_before_conclusions=False,
   855|     ))
   856| 
   857|     artifacts.append(_artifact(
   858|         "project_pool_containment_similarity_matrix", p(input_paths, "project_pool_containment_similarity_matrix"), "csv", False,
   859|         input_present.get("project_pool_containment_similarity_matrix", False), "compare_cross_segment.py",
   860|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   861|         "feeds the Project Portfolio section's peer-pool-containment paragraph, "
   862|         "rendered as a per-project outlier list (not a per-pair table)",
   863|         "one row per (row_id=focal_project, column_id=peer_pool:{pool_scope}:{row_id}, "
   864|         "view_scope, domain) -- unlike the other three project matrices, this "
   865|         "one carries no ALL_DOMAINS aggregate row",
   866|         ["row_id", "column_id", "view_scope", "domain"], [], [],
   867|         ["how much a project's system aligns with its parent-sibling/bc/client peer pool"],
   868|         ["a cross-domain aggregate straight from this file -- the narrative "
   869|          "computes its own mean pool_containment_similarity across a project's "
   870|          "available domains per (project, pool_scope) because no ALL_DOMAINS "
   871|          "row exists here"],
   872|         ["column_id encodes pool_scope (parent_sibling/bc/client) so a "
   873|          "project's separate pool grains never share a matrix cell"],
   874|         _BLANK_STRING_NULL_SEMANTICS,
   875|         ["matrix_output_manifest"],
   876|         required_before_conclusions=False,
   877|     ))
   878| 
   879|     artifacts.append(_artifact(
   880|         "project_fragmentation_diagnostic", p(input_paths, "project_fragmentation_diagnostic"), "csv", False,
   881|         input_present.get("project_fragmentation_diagnostic", False), "compare_cross_segment.py",
   882|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   883|         "feeds the Project Portfolio section's fragmentation-diagnostic "
   884|         "paragraph; also the sole carrier of project_mean_file_pair_jaccard_matrix.csv's "
   885|         "signal in this narrative (its own exact_identity_overlap column), rather "
   886|         "than that matrix being consumed standalone",
   887|         "one row per (row_id, column_id, view_scope, domain=ALL_DOMAINS) -- "
   888|         "footprint_similarity minus exact_identity_overlap when both inputs "
   889|         "were available at production time",
   890|         ["row_id", "column_id", "view_scope"], [], [],
   891|         ["divergence between project footprint overlap (union_jaccard) and "
   892|          "exact per-file identity overlap (mean file-pair jaccard)"],
   893|         ["an authoritative governance index -- diagnostic only, per this "
   894|          "file's own interpretation text"],
   895|         ["value_status other than \"diagnostic\" (e.g. unavailable_required_inputs) "
   896|          "means the cell could not be computed and is excluded from the "
   897|          "narrative's pair list"],
   898|         _BLANK_STRING_NULL_SEMANTICS,
   899|         ["project_union_jaccard_matrix", "matrix_output_manifest", "project_mean_file_pair_jaccard_matrix"],
   900|         required_before_conclusions=False,
   901|     ))
   902| 
   903|     artifacts.append(_artifact(
   904|         "governance_bc_client_matrix", p(input_paths, "governance_bc_client_matrix"), "csv", False,
   905|         input_present.get("governance_bc_client_matrix", False), "tools/governance_relationships.py",
   906|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   907|         "feeds the Business Center Composition section -- client composition "
   908|         "of each business center's PHYSICAL project population (file_metadata.csv's "
   909|         "project_label grain), not the governance-population grain used by the "
   910|         "project_* matrices above",
   911|         "one row per (business_center_label, client_label) pair actually present",
   912|         ["business_center_label", "client_label"], [], [],
   913|         ["how many physical projects/files a client contributes to a business "
   914|          "center's population, and what share of that business center's files "
   915|          "that represents (percentage_of_bc, computed exactly once in "
   916|          "build_bc_client_matrix_rows() and only read here)"],
   917|         ["behavioral similarity between those projects -- see "
   918|          "project_pool_containment_similarity_matrix.csv for that, and note its "
   919|          "\"project\" grain is a (client, discipline, unit_system) governance "
   920|          "population, not the same entity as a row here; the two are not "
   921|          "row-for-row joinable"],
   922|         ["percentage_of_client on this file answers a different question than "
   923|          "percentage_of_bc on the same row -- one BC's share of one client's "
   924|          "total files vs. one client's share of one BC's total files; do not "
   925|          "average or compare them directly"],
   926|         _BLANK_STRING_NULL_SEMANTICS,
   927|         ["governance_client_bc_matrix", "governance_relationships"],
   928|         required_before_conclusions=False,
   929|     ))
   930| 
   931|     artifacts.append(_artifact(
   932|         "governance_relationships", p(sibling_paths, "governance_relationships"), "csv", False,
   933|         sibling_present.get("governance_relationships", False), "tools/governance_relationships.py",
   934|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   935|         "archive_only -- not read by generate_governance_narrative.py; the "
   936|         "one-row-per-physical-project source that governance_bc_client_matrix.csv/"
   937|         "governance_client_bc_matrix.csv are aggregated from, named by path in "
   938|         "the Business Center Composition section's body text",
```
