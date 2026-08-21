# Chunk of tools/governance_evidence_package.py

- Source relative path: `tools/governance_evidence_package.py`
- Chunk: 4 of 6
- Original line range: 939-1338
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_evidence_map
- Source SHA-256: 2fece0426163550ef83e302b52b9f002b12123e12eb35430df07c3d1f4c4b1f3
- Starts inside symbol: build_evidence_map
- Ends inside symbol: build_evidence_map

```
   939|         "one row per (client_label, business_center_label, project_name) "
   940|         "physical-project identity",
   941|         ["client_label", "business_center_label", "project_name"], ["project_id"], [],
   942|         ["which physical projects exist for a client/business center, and how "
   943|          "many files each carries; whether a project_name string is genuinely "
   944|          "one project or a same-named collision across different clients"],
   945|         ["a governance, compliance, or quality read -- project/file counts only"],
   946|         ["project_name_is_fallback == \"true\" means project_name is a synthetic "
   947|          "per-file identifier (that file's own export_run_id), not a human-"
   948|          "assigned project name -- not consumed or checked by this generator, "
   949|          "which only infers this file's presence beside whichever of "
   950|          "--governance-bc-client-matrix/--governance-client-bc-matrix was "
   951|          "supplied (falling back to --summary's directory if neither was) "
   952|          "and never parses it"],
   953|         {},
   954|         ["governance_bc_client_matrix", "governance_client_bc_matrix"],
   955|         required_before_conclusions=False,
   956|     ))
   957| 
   958|     artifacts.append(_artifact(
   959|         "governance_client_bc_matrix", p(input_paths, "governance_client_bc_matrix"), "csv", False,
   960|         input_present.get("governance_client_bc_matrix", False), "tools/governance_relationships.py",
   961|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   962|         "feeds the Business Center Distribution section -- business-center "
   963|         "distribution of each client's physical project population, aggregated "
   964|         "from governance_bc_client_matrix.csv with no independent computation",
   965|         "one row per client_label",
   966|         ["client_label"], [], [],
   967|         ["how many business centers a client's projects span, and how that "
   968|          "client's project/file count divides across them (business_centers is "
   969|          "already ordered by governance_bc_client_matrix.csv's percentage_of_client, "
   970|          "descending)"],
   971|         ["a percentage_of_bc/percentage_of_client column of its own -- this file "
   972|          "only sums project_count/project_file_count from governance_bc_client_"
   973|          "matrix.csv; read percentages from that file, not this one"],
   974|         ["in the corpus this package type was seeded from, no client's projects "
   975|          "actually spanned more than one business center (business_center_count "
   976|          "== 1 for every row) -- a single-BC client here is a real, verified-"
   977|          "common case, not evidence the multi-BC aggregation path is untested "
   978|          "(see tests/test_governance_relationships.py's synthetic multi-BC case)"],
   979|         _BLANK_STRING_NULL_SEMANTICS,
   980|         ["governance_bc_client_matrix"],
   981|         required_before_conclusions=False,
   982|     ))
   983| 
   984|     artifacts.append(_artifact(
   985|         "cross_segment_file_pairs", p(sibling_paths, "file_pairs"), "csv", False,
   986|         sibling_present.get("file_pairs", False), "compare_cross_segment.py",
   987|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
   988|         "archive_only -- not read by generate_governance_narrative.py; reserved "
   989|         "for drill-through/audit-pack use per "
   990|         "docs/governance_generator_cross_compare_coverage.md ('too large for "
   991|         "leadership summary, but the best evidence trail when a tier or anomaly "
   992|         "needs file-level audit')",
   993|         "one row per file pair", ["segment_id_a", "segment_id_b", "domain"], [], [],
   994|         ["file-level audit trail behind any pair-mean metric in cross_segment_summary.csv"],
   995|         ["this generator does not open or parse this file; presence is inferred "
   996|          "as a sibling of --summary's directory, never verified against its own schema"],
   997|         ["not consumed by this generator in PR1; see "
   998|          "docs/governance_generator_cross_compare_coverage.md's suggested "
   999|          "'drill-through only' integration point. columns/row_count below "
  1000|          "(when present) come from the same live directory scan governance_"
  1001|          "file_inventory.json uses (_scan_csv_file, D-023/D-024) -- a "
  1002|          "structural fact about the header, not this generator opening or "
  1003|          "interpreting a single row of it."],
  1004|         {},
  1005|         ["cross_segment_summary"],
  1006|         required_before_conclusions=False,
  1007|     ))
  1008|     artifacts[-1].update(_sibling_scan_fields(sibling_paths.get("file_pairs"), sibling_present.get("file_pairs", False)))
  1009| 
  1010|     artifacts.append(_artifact(
  1011|         "comparison_registry", p(sibling_paths, "comparison_registry"), "csv", False,
  1012|         sibling_present.get("comparison_registry", False), "compare_cross_segment.py",
  1013|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1014|         "path resolved from --comparison-registry when explicitly supplied, else "
  1015|         "auto-detected beside --summary's directory (D-032) -- the same resolved "
  1016|         "path also drives governance_package_manifest.json/governance_package_"
  1017|         "health.json's own 'comparison_registry' input tracking, so the two "
  1018|         "never disagree about which file this means for a given run. Content is "
  1019|         "opened and read ONLY when --comparison-registry is explicitly passed "
  1020|         "(auto-detected presence alone never triggers a read) -- producing "
  1021|         "governance_package_health.json's comparison_completeness field and the "
  1022|         "narrative's Input Completeness / Staleness note near Analytical Notes.",
  1023|         "one row per (domain, segment pair) comparison registry entry", ["domain"], [], [],
  1024|         ["whether an expected segment/domain comparison was actually run, and whether it is "
  1025|          "stale relative to the evidence CSV -- only when --comparison-registry was explicitly "
  1026|          "supplied; presence alone (auto-detected, no flag) answers only 'does this file exist'"],
  1027|         ["when --comparison-registry was NOT explicitly passed, this file's content is never "
  1028|          "opened even if present is true here (auto-detected beside --summary's directory only). "
  1029|          "When it IS passed, build_comparison_completeness() reads only identity (segment_id_a/b, "
  1030|          "comparison_type, domain) and recency (computed_utc) fields -- never row content beyond "
  1031|          "those, and never reproduced in the output package; see governance_package_health.json's "
  1032|          "comparison_completeness for the derived counts."],
  1033|         ["prior to D-032, missing rows in cross_segment_summary.csv were treated as weak "
  1034|          "evidence with no way to distinguish a not-run/stale comparison; "
  1035|          "docs/governance_generator_cross_compare_coverage.md's 'Input Completeness / "
  1036|          "Staleness' row is now marked Done. columns/row_count below (when present) come "
  1037|          "from the same live directory scan governance_file_inventory.json uses "
  1038|          "(_scan_csv_file, D-023/D-024), independent of any --comparison-registry read."],
  1039|         {},
  1040|         ["cross_segment_summary", "governance_package_health"],
  1041|         required_before_conclusions=False,
  1042|     ))
  1043|     artifacts[-1].update(_sibling_scan_fields(
  1044|         sibling_paths.get("comparison_registry"), sibling_present.get("comparison_registry", False),
  1045|     ))
  1046| 
  1047|     artifacts.append(_artifact(
  1048|         "pattern_reuse_summary_by_domain", p(sibling_paths, "pattern_reuse_summary_by_domain"), "csv", False,
  1049|         sibling_present.get("pattern_reuse_summary_by_domain", False), "compare_cross_segment.py",
  1050|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1051|         "archive_only -- not read by generate_governance_narrative.py; "
  1052|         "deliberately excluded (not merely unwired), since its n_patterns "
  1053|         "duplicates the corpus-wide reuse signal pattern_reuse_distribution.csv's "
  1054|         "own distinct-pattern table already reports -- see this generator's "
  1055|         "own module docstring and docs/governance_generator_cross_compare_coverage.md",
  1056|         "one row per (view_scope, governance_role, client_label, "
  1057|         "discipline_label, unit_system, domain, reuse_bucket, bucket_basis) "
  1058|         "-- the by-domain sibling of pattern_reuse_summary_by_client.csv",
  1059|         ["view_scope", "governance_role", "client_label", "discipline_label", "unit_system", "domain"], [], [],
  1060|         ["per-domain reuse_bucket/n_patterns counts, recorded independently of "
  1061|          "pattern_reuse_distribution.csv's own dedup table"],
  1062|         ["a governance signal distinct from what pattern_reuse_distribution.csv "
  1063|          "already reports -- evaluated and confirmed to add no new information "
  1064|          "beyond that file's already-consumed distinct-pattern table"],
  1065|         ["this generator's narrative/scoring logic never opens or interprets "
  1066|          "this file's row content; columns/row_count below (when present) come "
  1067|          "from the same live directory scan governance_file_inventory.json "
  1068|          "uses (_scan_csv_file, D-023/D-024), not from a read this generator "
  1069|          "performs on a normal run"],
  1070|         _BLANK_STRING_NULL_SEMANTICS,
  1071|         ["pattern_reuse_distribution", "pattern_reuse_summary_by_client"],
  1072|         required_before_conclusions=False,
  1073|     ))
  1074|     artifacts[-1].update(_sibling_scan_fields(
  1075|         sibling_paths.get("pattern_reuse_summary_by_domain"),
  1076|         sibling_present.get("pattern_reuse_summary_by_domain", False),
  1077|     ))
  1078| 
  1079|     artifacts.append(_artifact(
  1080|         "project_mean_file_pair_jaccard_matrix", p(sibling_paths, "project_mean_file_pair_jaccard_matrix"), "csv", False,
  1081|         sibling_present.get("project_mean_file_pair_jaccard_matrix", False), "compare_cross_segment.py",
  1082|         AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1083|         "archive_only -- not consumed standalone by generate_governance_narrative.py; "
  1084|         "its signal is folded into project_fragmentation_diagnostic.csv's own "
  1085|         "exact_identity_overlap column instead, per this generator's own module "
  1086|         "docstring and docs/governance_generator_cross_compare_coverage.md",
  1087|         "one row per (row_id, column_id, view_scope, domain) matrix cell, same "
  1088|         "shape as the other project_* matrices; ALL_DOMAINS rows carry the "
  1089|         "cross-domain mean file-pair jaccard",
  1090|         ["row_id", "column_id", "view_scope", "domain"], [], [],
  1091|         ["typical file-to-file similarity between two projects (mean pairwise "
  1092|          "file jaccard), independent of exact system-level footprint overlap"],
  1093|         ["a governance read distinct from project_fragmentation_diagnostic.csv's "
  1094|          "exact_identity_overlap column -- that column already carries this "
  1095|          "file's signal into the narrative; this file itself is never opened "
  1096|          "standalone"],
  1097|         ["this generator's narrative/scoring logic never opens or interprets "
  1098|          "this file's row content directly; columns/row_count below (when "
  1099|          "present) come from the same live directory scan governance_file_"
  1100|          "inventory.json uses (_scan_csv_file, D-023/D-024), not from a read "
  1101|          "this generator performs on a normal run"],
  1102|         _BLANK_STRING_NULL_SEMANTICS,
  1103|         ["project_fragmentation_diagnostic", "project_union_jaccard_matrix"],
  1104|         required_before_conclusions=False,
  1105|     ))
  1106|     artifacts[-1].update(_sibling_scan_fields(
  1107|         sibling_paths.get("project_mean_file_pair_jaccard_matrix"),
  1108|         sibling_present.get("project_mean_file_pair_jaccard_matrix", False),
  1109|     ))
  1110| 
  1111|     artifacts.append(_artifact(
  1112|         "governance_domain_summary", p(output_paths, "governance_domain_summary"), "csv", True, True,
  1113|         GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1114|         "primary tier/score rollup, one row per domain",
  1115|         "one row per domain with a renderable cascade signal (domains failing "
  1116|         "_has_renderable_cascade_signal() -- Group-3-scope-only domains -- are excluded)",
  1117|         ["domain", "governance_tier", "template_to_project"], ["domain"], [],
  1118|         ["tier/reliability/anomaly classification and cascade scores per domain"],
  1119|         ["client-level or discipline-level breakdowns -- see governance_client_summary.csv "
  1120|          "and the narrative's discipline section"],
  1121|         ["excludes EXCLUDED_FROM_SCORING domains from aggregate framing (still "
  1122|          "listed as a row); fmt()/pct() render a present-but-None numeric field "
  1123|          "as the em-dash — string (not an ASCII hyphen), while a governance-state "
  1124|          "column for a domain with no governance_state_summary entry at all "
  1125|          "renders as '' (empty string) -- two different 'missing' states use "
  1126|          "two different cell conventions in this CSV, documented but not "
  1127|          "unified in PR1."],
  1128|         {
  1129|             "*(fmt/pct-formatted columns)": "— (em dash, U+2014 -- not an ASCII hyphen) means the field exists in the schema but has no data for this domain.",
  1130|             "*(governance-state columns)": "'' (empty string) means governance_state_summary has no entry for this domain at all -- a different condition than a present-but-None value.",
  1131|         },
  1132|         ["cross_segment_summary", "cross_segment_pooled", "cross_segment_governance_state_summary"],
  1133|         required_before_conclusions=True,
  1134|     ))
  1135| 
  1136|     artifacts.append(_artifact(
  1137|         "governance_client_summary", p(output_paths, "governance_client_summary"), "csv", True, True,
  1138|         GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1139|         "primary client alignment/onboarding rollup, one row per client",
  1140|         "one row per client with at least one Project file discovered via "
  1141|         "pooled_rows/summary_rows",
  1142|         ["client", "alignment_tier"], ["client"], [],
  1143|         ["cross-client similarity and within-project coherence per client, plus "
  1144|          "onboarding-oriented interpretation fields"],
  1145|         ["per-domain detail -- see governance_domain_summary.csv"],
  1146|         ["inherits the cross_segment_pooled.csv A2 pool_scope caveat -- see that "
  1147|          "artifact's known_limitations."],
  1148|         {"*(fmt-formatted columns)": "— (em dash, U+2014 -- not an ASCII hyphen) means the field exists but has no data for this client."},
  1149|         ["cross_segment_summary", "cross_segment_pooled", "client_sector"],
  1150|         required_before_conclusions=True,
  1151|     ))
  1152| 
  1153|     artifacts.append(_artifact(
  1154|         "governance_bc_summary", p(output_paths, "governance_bc_summary"), "csv", True, True,
  1155|         GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1156|         "primary business-center peer-alignment rollup, one row per business center",
  1157|         "one row per real business center discovered via bc_to_bc/enterprise_to_bc/"
  1158|         "bc_to_project summary rows or cascade's tc_bc_by_bc/eb_by_bc breakouts "
  1159|         "(build_bc_summary()) -- Enterprise itself is never a row here, see "
  1160|         "governance_narrative_context.md's Enterprise Overview section instead",
  1161|         ["business_center", "alignment_tier"], ["business_center"], [],
  1162|         ["cross-BC peer similarity (all-view primary -- opposite convention from "
  1163|          "governance_client_summary.csv's used-view-primary cross-client similarity, "
  1164|          "since bc_to_bc pairs are Template/Container peer comparisons, not Project "
  1165|          "usage comparisons), internal Template->Container coherence per BC, and "
  1166|          "Enterprise standard reach into that BC"],
  1167|         ["per-domain detail beyond the top/bottom-3 most/least-aligned columns -- "
  1168|          "see governance_domain_summary.csv; Enterprise's own rollup -- see the "
  1169|          "narrative's Enterprise Overview section, not this CSV"],
  1170|         ["bc_alignment_high/_moderate and bc_confidence_low/moderate_max_files "
  1171|          "thresholds are hand-picked defaults value-coincident with (but a "
  1172|          "separate policy profile from) governance_client_summary.csv's "
  1173|          "client_alignment_*/client_confidence_* thresholds -- see "
  1174|          "BC_ALIGNMENT_HIGH's definition comment in generate_governance_narrative.py."],
  1175|         {"*(fmt-formatted columns)": "— (em dash, U+2014 -- not an ASCII hyphen) means the field exists but has no data for this business center."},
  1176|         ["cross_segment_summary", "governance_domain_summary", "governance_narrative_context"],
  1177|         required_before_conclusions=True,
  1178|     ))
  1179| 
  1180|     artifacts.append(_artifact(
  1181|         "governance_narrative_context", p(output_paths, "governance_narrative_context"), "markdown", True, True,
  1182|         GENERATOR_IDENTITY, AUTHORITY_CONTROLLED_INTERPRETATION,
  1183|         "human-readable synthesis; sections list assembled from render_* functions",
  1184|         "one markdown document per run", [], [], [],
  1185|         ["a human-readable synthesis of the three CSVs above, with tier labels and framing prose"],
  1186|         ["approves no standard, assigns no owner, judges no team -- this is the "
  1187|          "generator's own stated scope boundary (render_header()'s Executive Summary)"],
  1188|         ["assembled by conditional section inclusion -- governance-state and "
  1189|          "delta sections are mutually exclusive (elif); the union/reuse section "
  1190|          "is entirely omitted, not blank-rendered, when all three of its inputs "
  1191|          "are absent; the Enterprise Overview section is likewise omitted (not "
  1192|          "blank-rendered) when cascade has no tc/eb/ec signal at all."],
  1193|         {},
  1194|         ["governance_domain_summary", "governance_client_summary", "governance_bc_summary",
  1195|          "governance_package_health", "governance_evidence_map", "governance_findings",
  1196|          "governance_brief", "governance_interpretation_guide", "governance_question_routes",
  1197|          "governance_reading_order", "governance_classification_rules"],
  1198|         required_before_conclusions=False,
  1199|     ))
  1200| 
  1201|     artifacts.append(_artifact(
  1202|         "governance_findings", p(output_paths, "governance_findings"), "json", False, True,
  1203|         GENERATOR_IDENTITY, AUTHORITY_CONTROLLED_INTERPRETATION,
  1204|         "structured, rule-derived findings (tier/anomaly/onboarding classifications) "
  1205|         "with provenance -- origin, fidelity, authority, and limits per finding, "
  1206|         "plus leadership questions marked as questions rather than claims",
  1207|         "one object per package generation run, containing one entry per finding",
  1208|         [], ["finding_id"], [],
  1209|         ["which domains/clients meet a specific named governance rule "
  1210|          "(baseline_candidate, high_fragmentation, passive_inheritance_risk, etc.), "
  1211|          "and what CSV fields/rows support that classification"],
  1212|         ["raw metric values -- follow each finding's support[].selector back to "
  1213|          "governance_domain_summary.csv/governance_client_summary.csv for those"],
  1214|         ["derived by build_structured_findings(), which reuses the exact same "
  1215|          "classification buckets governance_narrative_context.md's Key Findings "
  1216|          "section renders as prose -- the two are not independent implementations. "
  1217|          "leadership_question findings carry status: question_not_claim and "
  1218|          "authority_level: convenience_summary -- they are suggested questions, "
  1219|          "not observed results."],
  1220|         {},
  1221|         ["governance_domain_summary", "governance_client_summary", "governance_narrative_context"],
  1222|         schema_version=FINDINGS_SCHEMA_VERSION,
  1223|         required_before_conclusions=True,
  1224|     ))
  1225| 
  1226|     artifacts.append(_artifact(
  1227|         "governance_file_inventory", p(output_paths, "governance_file_inventory"), "json", False, True,
  1228|         GENERATOR_IDENTITY, AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,
  1229|         "live directory-scan inventory of *.csv files actually present under "
  1230|         "the cross_segment export directory (and, when supplied separately, "
  1231|         "the relationship-layer output directory) that are NOT already one of "
  1232|         "the artifacts above -- see inventory_export_directory_files(). Exists "
  1233|         "so an LLM reading this package can name a candidate drill-down file "
  1234|         "it has never been told the schema of, instead of stonewalling on a "
  1235|         "question the rollups can't answer",
  1236|         "one entry per undiscovered CSV file found during this run's scan; a "
  1237|         "corpus with no such files produces an entry with an empty files list, "
  1238|         "not a missing artifact",
  1239|         ["filename", "row_count"], [], [],
  1240|         ["that a given file exists in the scanned directories, its column "
  1241|          "header, an inferred dtype per column (integer/float/boolean/string/"
  1242|          "empty), and its row count"],
  1243|         ["what any column or row actually means -- the per-file narrative "
  1244|          "string is either borrowed verbatim from matrix_output_manifest.csv's "
  1245|          "own interpretation column when the filename matches a known "
  1246|          "matrix_name, or a generic structural fallback sentence; neither is a "
  1247|          "substitute for a real evidence-map entry once a file is understood "
  1248|          "well enough to earn one"],
  1249|         ["computed fresh every run from Path.glob('*.csv') -- a file deleted "
  1250|          "or renamed between runs simply stops/starts appearing, with no "
  1251|          "staleness tracking of its own; no sample cell values are ever "
  1252|          "captured, only header names, inferred dtype, and row count; column "
  1253|          "dtype inference is a best-effort classification over the whole "
  1254|          "column's values, not a schema declaration -- see _column_dtype()."],
  1255|         {"*": "A column classified 'empty' had zero non-blank cells in the scanned file."},
  1256|         [],  # no fixed related_artifacts -- the files it lists vary run to run
  1257|         schema_version=file_inventory_schema_version,
  1258|         required_before_conclusions=False,
  1259|     ))
  1260| 
  1261|     # governance_brief.md is the only PR4 artifact that may genuinely be
  1262|     # absent even when this whole function runs (gated by its own
  1263|     # --emit-interpretation-layer flag, independent of --emit-evidence-package)
  1264|     # -- unlike the artifacts above, whose "present: True" is hardcoded because
  1265|     # build_evidence_map() only ever runs after they're already written.
  1266|     _brief_path = output_paths.get("governance_brief")
  1267|     _brief_present = bool(_brief_path) and Path(_brief_path).exists()
  1268|     artifacts.append(_artifact(
  1269|         "governance_brief", p(output_paths, "governance_brief") if _brief_present else None,
  1270|         "markdown", False, _brief_present,
  1271|         GENERATOR_IDENTITY, AUTHORITY_CONVENIENCE_SUMMARY,
  1272|         "narrower, run-specific digest of governance_findings.json -- a quick "
  1273|         "top-line read, not a new source of evidence",
  1274|         "one markdown document per run (when --emit-interpretation-layer is on)",
  1275|         [], [], [],
  1276|         ["a capped, categorized list of this run's findings by finding_type "
  1277|          "(baseline candidates, high fragmentation, passive-inheritance risk, "
  1278|          "low client coherence), plus the leadership questions"],
  1279|         ["anything beyond what governance_findings.json already contains -- "
  1280|          "this is a distillation, computed from the same findings list, never "
  1281|          "an independent computation"],
  1282|         ["each finding-type section is capped (10-15 items) with a pointer to "
  1283|          "governance_findings.json for the full list; absent entirely when "
  1284|          "--no-emit-interpretation-layer was passed for this run -- check this "
  1285|          "artifact's own present field, not just governance_package_manifest's "
  1286|          "policy_profiles, to know whether it exists for a given run."],
  1287|         {},
  1288|         ["governance_findings", "governance_domain_summary", "governance_client_summary",
  1289|          "governance_interpretation_guide", "governance_question_routes"],
  1290|         required_before_conclusions=False,
  1291|     ))
  1292| 
  1293|     artifacts.append(_artifact(
  1294|         "governance_interpretation_guide", p(sibling_paths, "interpretation_guide"),
  1295|         "markdown", False, sibling_present.get("interpretation_guide", False),
  1296|         "human/LLM-authored (docs/governance/governance_interpretation_guide.md)",
  1297|         AUTHORITY_CONTROLLED_INTERPRETATION,
  1298|         "package-specific interpretation layer: what each metric/tier means, "
  1299|         "comparability rules, missing-value semantics, authority ordering, "
  1300|         "known bad inferences -- read this before reasoning from the rest of "
  1301|         "the package",
  1302|         "one document per package_type (not per-run; not regenerated by this "
  1303|         "generator)",
  1304|         [], [], [],
  1305|         ["what a metric or governance_tier value means and does not mean; how "
  1306|          "to read missing values and comparability caveats for this package type"],
  1307|         ["this run's actual data -- it explains semantics, not this run's results"],
  1308|         ["path/present above describe the checked-in repo doc under "
  1309|          "docs/governance/ (D-034), not written or validated by this "
  1310|          "generator -- a package copied without the repo's docs/ directory "
  1311|          "would show present: false here. A copy of this exact file is ALSO "
  1312|          "written into --out alongside this run's other outputs when present "
  1313|          "(D-034), so a package handed to someone without the repo checked "
  1314|          "out is still self-contained; that copy is a convenience, not a "
  1315|          "second source of truth -- this artifact's path/present always "
  1316|          "describe the repo doc, never the copy."],
  1317|         {},
  1318|         ["governance_question_routes", "governance_brief", "governance_narrative_context",
  1319|          "governance_reading_order", "governance_classification_rules"],
  1320|         required_before_conclusions=True,
  1321|         output_local_path=_output_local_path("interpretation_guide"),
  1322|     ))
  1323| 
  1324|     artifacts.append(_artifact(
  1325|         "governance_question_routes", p(sibling_paths, "question_routes"),
  1326|         "markdown", False, sibling_present.get("question_routes", False),
  1327|         "human/LLM-authored discovery scaffold (docs/governance/governance_question_routes.md)",
  1328|         AUTHORITY_CONVENIENCE_SUMMARY,
  1329|         "candidate catalog of recurring question types and which artifact/"
  1330|         "fields answer them -- navigational only, not evidence",
  1331|         "one document per package_type (not per-run; not regenerated by this "
  1332|         "generator)",
  1333|         [], [], [],
  1334|         ["which artifact to check first for a specific recurring question type"],
  1335|         ["the answer itself -- follow the route to the named artifact"],
  1336|         ["every route in this document is at 'candidate' maturity (see the "
  1337|          "document's own maturity-level scale) -- none has a proven history "
  1338|          "of repeated use for this package type yet; not an exhaustive list "
```
