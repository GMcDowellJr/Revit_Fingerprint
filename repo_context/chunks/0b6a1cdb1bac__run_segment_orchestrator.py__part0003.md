# Chunk of tools/run_segment_orchestrator.py

- Source relative path: `tools/run_segment_orchestrator.py`
- Chunk: 3 of 4
- Original line range: 908-1396
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: validate_membership_against_manifest, _clear_stale_name_all_before_run, _run_one_segment, _run_one_segment.log
- Source SHA-256: c1d79ae240bf0af45e5deb47ebd929be191e1d6bb8a42be87fe41cbe5dfc7646
- Starts inside symbol: no
- Ends inside symbol: no

```
   908| def validate_membership_against_manifest(
   909|     plan: List[tuple[dict, dict]],
   910|     membership: Dict[str, List[str]],
   911| ) -> List[str]:
   912|     """Return one error string per segment where segment_membership.csv disagrees
   913|     with segment_manifest.csv's file_count/population_hash for that segment_id.
   914| 
   915|     Guards against a stale or mismatched segment_membership.csv silently driving
   916|     a segment's export_run_ids.txt/preshard population — e.g. build_segment_manifest.py
   917|     interrupted after replacing segment_manifest.csv/run_registry.csv but before
   918|     replacing segment_membership.csv, or a custom --membership-file pointing at
   919|     the wrong sidecar. A mismatch here means population_hash/file_count on the
   920|     manifest row describe a different population than the membership rows
   921|     actually loaded, which could mark a segment complete for the wrong file set.
   922|     """
   923|     errors: List[str] = []
   924|     for reg_row, mrow in plan:
   925|         sid = reg_row.get("segment_id", "").strip()
   926|         if not sid:
   927|             continue
   928|         ids = membership.get(sid, [])
   929|         expected_count = (mrow.get("file_count") or "").strip()
   930|         if expected_count and str(len(ids)) != expected_count:
   931|             errors.append(
   932|                 f"segment={sid}: segment_membership.csv has {len(ids)} export_run_id(s) "
   933|                 f"but segment_manifest.csv file_count={expected_count}"
   934|             )
   935|             continue
   936|         expected_hash = (mrow.get("population_hash") or "").strip()
   937|         if expected_hash:
   938|             actual_hash = hashlib.sha1("|".join(sorted(ids)).encode()).hexdigest()
   939|             if actual_hash != expected_hash:
   940|                 errors.append(
   941|                     f"segment={sid}: segment_membership.csv population_hash={actual_hash} "
   942|                     f"does not match segment_manifest.csv population_hash={expected_hash}"
   943|                 )
   944|     return errors
   945| 
   946| 
   947| def _clear_stale_name_all_before_run(out_root: Path, run_type: str, comparison_target: str, log) -> None:
   948|     """Clear this segment's stale name-leg BI-facing output before any step of this run
   949|     begins -- not just before step 3b, and not only inside
   950|     run_bundle_analysis_for_target()'s own upfront clear. A failure in step 2b
   951|     (name-pattern generation) or step 3 (config bundle, which gates step 3b even under
   952|     comparison_target=both) skips step 3b entirely, so run_bundle_analysis_for_target()
   953|     is never invoked at all and its own clear never runs -- without this, Power BI would
   954|     keep reading name_all/ from an old successful run even though this run is recorded
   955|     as failed (PR review, #391, second round).
   956| 
   957|     Only fires for segments this call actually intends to (re)run with name-leg work --
   958|     already-complete segments are filtered out of plan_to_run before _run_one_segment()
   959|     is ever invoked, so their still-current name_all/ is never touched by this function.
   960|     """
   961|     if run_type == "bundle" and comparison_target in ("name", "both"):
   962|         stale_name_all = out_root / "results" / "bundle_analysis" / "name_all"
   963|         if stale_name_all.is_dir():
   964|             log(f"[orchestrator]   clearing stale {stale_name_all} before name-leg regeneration")
   965|             retry_fs_op(shutil.rmtree, str(stale_name_all))
   966| 
   967| 
   968| def _run_one_segment(
   969|     idx: int,
   970|     total: int,
   971|     reg_row: dict,
   972|     mrow: dict,
   973|     membership: Dict[str, List[str]],
   974|     records_dir: Path,
   975|     exports_dir: Path,
   976|     segments_root: Path,
   977|     repo_root: Path,
   978|     join_policy: Path,
   979|     skip_bi_merge: bool,
   980|     registry: List[dict],
   981|     reg_index: Dict[str, int],
   982|     registry_file: Path,
   983|     manifest_file: Path,
   984|     results_registry_file: Path,
   985|     registry_lock: threading.Lock,
   986|     counters: Dict[str, object],
   987|     counters_lock: threading.Lock,
   988|     worker_id: int,
   989|     bundle_workers: int,
   990|     comparison_target: str = "config",
   991|     name_key_results_csv: Optional[Path] = None,
   992| ) -> Dict:
   993|     """Process one segment. Returns result dict.
   994| 
   995|     comparison_target="config" (default) is byte-identical to this function before PR4 --
   996|     every new code path below is gated on comparison_target in {"name", "both"} and adds no
   997|     new file writes, subprocess calls, or log lines otherwise. When enabled, it adds a
   998|     parallel name-projection leg (tools/bundle_analysis, --comparison-target name) alongside
   999|     the existing join_hash leg: re-cluster this segment's slice of a corpus-wide
  1000|     name_key_results.csv (tools/apply_name_key_policy.py, computed once up front -- see
  1001|     _filter_name_key_csv_to_segment()'s docstring for why no per-segment JSON re-parse is
  1002|     needed), then bundle-mine it into results/bundle_analysis/name_all/, mirroring
  1003|     results/bundle_analysis/all/ for the config leg but using join_key_name_identity as the
  1004|     join instead of join_hash.
  1005|     """
  1006|     sid = reg_row.get("segment_id", "").strip()
  1007|     output_folder = reg_row.get("output_folder", "").strip()
  1008|     out_root = segments_root / output_folder
  1009| 
  1010|     try:
  1011|         level = int(mrow.get("segment_level", 0))
  1012|     except (ValueError, TypeError):
  1013|         level = 0
  1014| 
  1015|     export_run_ids = sorted(membership.get(sid, []))
  1016|     file_count = len(export_run_ids)
  1017|     run_type = reg_row.get("run_type", "bundle").strip()
  1018| 
  1019|     print(
  1020|         f"\n[orchestrator] ── segment={sid} ({idx}/{total}) level={level} files={file_count} [worker={worker_id}] ──",
  1021|         flush=True,
  1022|     )
  1023| 
  1024|     log_path = out_root / "run.log"
  1025|     log_path.parent.mkdir(parents=True, exist_ok=True)
  1026| 
  1027|     step_failed: Optional[str] = None
  1028|     failure_notes: str = ""
  1029|     notes_parts: List[str] = []
  1030|     patterns_timing_lines: List[str] = []
  1031|     t_start = time.monotonic()
  1032|     t_prepare = 0
  1033|     t_patterns = 0
  1034|     t_bundle: Optional[int] = None
  1035|     t_merge: Optional[int] = None
  1036|     t_patterns_name: Optional[int] = None
  1037|     t_bundle_name: Optional[int] = None
  1038|     t_merge_name: Optional[int] = None
  1039|     elapsed = 0
  1040| 
  1041|     with log_path.open("w", encoding="utf-8", errors="replace") as log_f:
  1042|         def log(msg: str) -> None:
  1043|             log_f.write(msg + "\n")
  1044|             log_f.flush()
  1045| 
  1046|         # Caught here (rather than left to propagate) so a persistent lock -- retry_fs_op
  1047|         # exhausting every attempt, not just a transient one -- still routes through
  1048|         # step_failed and the registry-update block below. An uncaught exception this
  1049|         # early would escape _run_one_segment() entirely; the ThreadPoolExecutor caller's
  1050|         # generic "unhandled exception" handler only updates in-memory counters/
  1051|         # segment_results, never registry_file, leaving the segment's registry row (and
  1052|         # bundle_provenance.csv) at whatever they were before this run -- often
  1053|         # status=complete from a prior successful run -- so the next non-forced run would
  1054|         # skip it forever, silently reading stale Power BI output (PR review, #391, third
  1055|         # round).
  1056|         try:
  1057|             _clear_stale_name_all_before_run(out_root, run_type, comparison_target, log)
  1058|         except Exception as exc:
  1059|             step_failed = "clear_stale_name_all"
  1060|             failure_notes = f"step=clear_stale_name_all error={exc}"
  1061| 
  1062|         # Step 1 — Prepare: directories, export_run_ids.txt, segment-level records
  1063|         log(f"[orchestrator]   step 1/3 prepare...")
  1064|         t_step1_start = time.monotonic()
  1065|         try:
  1066|             segment_records_dir = out_root / "results" / "records"
  1067|             segment_records_dir.mkdir(parents=True, exist_ok=True)
  1068|             (out_root / "results" / "analysis").mkdir(parents=True, exist_ok=True)
  1069|             (out_root / "results" / "bundle_analysis").mkdir(parents=True, exist_ok=True)
  1070|             (out_root / "results" / "label_synthesis").mkdir(parents=True, exist_ok=True)
  1071| 
  1072|             ids_file = out_root / "export_run_ids.txt"
  1073|             ids_file.write_text("\n".join(export_run_ids) + "\n", encoding="utf-8")
  1074| 
  1075|             # _ensure_latent_purgeable() in run_bundle_analysis.py short-circuits
  1076|             # if this file already exists, so a stale one from this segment's
  1077|             # prior population would make the "used" view compute purgeability
  1078|             # against the old file set. This step only runs for segments that
  1079|             # are actually being (re)processed (skipped-complete segments never
  1080|             # reach here), so it is always safe to drop the cached file and let
  1081|             # it regenerate fresh against the current export_run_ids.
  1082|             (segment_records_dir / "latent_purgeable.csv").unlink(missing_ok=True)
  1083| 
  1084|             _write_segment_records(records_dir, segment_records_dir, set(export_run_ids))
  1085|         except Exception as exc:
  1086|             step_failed = "prepare"
  1087|             failure_notes = f"step=prepare error={exc}"
  1088|         t_prepare = int(time.monotonic() - t_step1_start)
  1089|         log(f"[orchestrator]   step 1/3 prepare elapsed={t_prepare}s")
  1090| 
  1091|         # Step 2 — Patterns stage
  1092|         # --records-dir points at corpus records so build_label_population (run internally
  1093|         # by run_extract_all) reads the full population, not just this segment's subset.
  1094|         # --label-synth-dir points at corpus label_synthesis so emit_analysis picks up the
  1095|         # LLM cache and curator annotations built in Run B without rebuilding per segment.
  1096|         if step_failed is None:
  1097|             log(f"[orchestrator]   step 2/3 patterns...")
  1098|             corpus_label_synth_dir = records_dir.parent / "label_synthesis"
  1099|             extract_cmd = [
  1100|                 sys.executable,
  1101|                 str(repo_root / "tools" / "run_extract_all.py"),
  1102|                 str(exports_dir),
  1103|                 "--out-root", str(out_root),
  1104|                 "--stages", "patterns",
  1105|                 "--records-dir", str(records_dir),
  1106|                 "--label-synth-dir", str(corpus_label_synth_dir),
  1107|                 "--filter-export-run-ids", str(out_root / "export_run_ids.txt"),
  1108|                 "--join-policy", str(join_policy),
  1109|                 "--allow-sig-hash-join-key",
  1110|             ]
  1111|             t_step2_start = time.monotonic()
  1112|             rc, tail, patterns_content = run_step_log(extract_cmd, out_root / "patterns.log", cwd=str(repo_root))
  1113|             t_patterns = int(time.monotonic() - t_step2_start)
  1114|             log(f"[orchestrator]   step 2/3 patterns elapsed={t_patterns}s")
  1115|             if rc != 0:
  1116|                 step_failed = "patterns"
  1117|                 failure_notes = f"step=patterns returncode={rc}\n{tail}"
  1118|             else:
  1119|                 presence_csv = out_root / "results" / "analysis" / "pattern_presence_file.csv"
  1120|                 if not presence_csv.is_file():
  1121|                     step_failed = "patterns"
  1122|                     failure_notes = _build_patterns_missing_notes(
  1123|                         sid, out_root, records_dir, patterns_content
  1124|                     )
  1125| 
  1126|             # Surface patterns timing from captured output — top-5 to console
  1127|             patterns_timing_lines = [
  1128|                 ln for ln in patterns_content.splitlines()
  1129|                 if ln.startswith("[patterns_timing]")
  1130|             ]
  1131|             if patterns_timing_lines:
  1132|                 summary_lines = [ln for ln in patterns_timing_lines if "domain=" not in ln]
  1133|                 domain_lines  = [ln for ln in patterns_timing_lines if "domain=" in ln]
  1134|                 lines_to_show = domain_lines + summary_lines
  1135|                 print(f"[orchestrator]   patterns timing:", flush=True)
  1136|                 for ln in lines_to_show:
  1137|                     print(f"[orchestrator]     {ln}", flush=True)
  1138| 
  1139|         # Step 2b — Name-projection patterns stage (opt-in, comparison_target in {name, both})
  1140|         if step_failed is None and comparison_target in ("name", "both"):
  1141|             log(f"[orchestrator]   step 2b name-patterns...")
  1142|             t_step2b_start = time.monotonic()
  1143|             try:
  1144|                 segment_name_key_csv = out_root / "results" / "name_key" / "name_key_results.csv"
  1145|                 rows_written = _filter_name_key_csv_to_segment(
  1146|                     name_key_results_csv, segment_name_key_csv, set(export_run_ids)
  1147|                 )
  1148|                 log(f"[orchestrator]   step 2b name-patterns filtered_rows={rows_written}")
  1149|                 name_patterns_cmd = [
  1150|                     sys.executable,
  1151|                     str(repo_root / "tools" / "generate_name_key_patterns.py"),
  1152|                     "--comparison-target", "name",
  1153|                     "--name-key-csv", str(segment_name_key_csv),
  1154|                     "--out-root", str(out_root / "results" / "name_key" / "patterns"),
  1155|                 ]
  1156|                 rc, tail, _name_patterns_content = run_step_log(
  1157|                     name_patterns_cmd, out_root / "name_patterns.log", cwd=str(repo_root)
  1158|                 )
  1159|                 if rc != 0:
  1160|                     step_failed = "patterns_name"
  1161|                     failure_notes = f"step=patterns_name returncode={rc}\n{tail}"
  1162|             except Exception as exc:
  1163|                 step_failed = "patterns_name"
  1164|                 failure_notes = f"step=patterns_name error={exc}"
  1165|             t_patterns_name = int(time.monotonic() - t_step2b_start)
  1166|             log(f"[orchestrator]   step 2b name-patterns elapsed={t_patterns_name}s")
  1167| 
  1168|         # Step 3 — Bundle stage
  1169|         if step_failed is None and run_type == "bundle":
  1170|             log(f"[orchestrator]   step 3/3 bundle...")
  1171|             bundle_cmd = [
  1172|                 sys.executable,
  1173|                 str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
  1174|                 "--analysis-dir", str(out_root / "results" / "analysis"),
  1175|                 "--out-dir", str(out_root / "results" / "bundle_analysis"),
  1176|                 "--metadata-file", str(records_dir / "file_metadata.csv"),
  1177|                 "--no-discover-populations",
  1178|                 "--purge-view", "both",
  1179|                 "--latent-purgeable-file", str(out_root / "results" / "records" / "latent_purgeable.csv"),
  1180|             ]
  1181|             bundle_cmd += ["--workers", str(bundle_workers)]
  1182|             t_step3_start = time.monotonic()
  1183|             rc, tail, _content = run_step_log(bundle_cmd, out_root / "bundle.log", cwd=str(repo_root))
  1184|             t_bundle = int(time.monotonic() - t_step3_start)
  1185|             log(f"[orchestrator]   step 3/3 bundle elapsed={t_bundle}s")
  1186|             if rc != 0:
  1187|                 step_failed = "bundle"
  1188|                 failure_notes = f"step=bundle returncode={rc}\n{tail}"
  1189| 
  1190|         # Step 3b — Name-projection bundle stage (opt-in, comparison_target in {name, both})
  1191|         # --purge-view is left unset: run_bundle_analysis.py's target-aware default resolves
  1192|         # it to "all" for --comparison-target name (the only view name-target supports).
  1193|         if step_failed is None and run_type == "bundle" and comparison_target in ("name", "both"):
  1194|             log(f"[orchestrator]   step 3b name-bundle...")
  1195|             # Clear any name-leg output from a previous run of this segment before
  1196|             # regenerating. run_bundle_analysis.py only writes per-domain folders for
  1197|             # domains present in *this* run's pattern set -- it never deletes a stale
  1198|             # <domain>/ folder left over from a prior run whose population included a
  1199|             # domain this one doesn't. Left in place, emit_name_target_provenance()'s
  1200|             # rglob("bundles.csv") (inside the run_bundle_analysis.py subprocess below)
  1201|             # would pick up those stale files and report them in a fresh
  1202|             # bundle_provenance.csv even for a segment that now has zero active domains --
  1203|             # merge_bi_outputs()'s *_combined.csv cleanup doesn't cover this, since
  1204|             # provenance is built independently (PR #390 review, third round). Matches the
  1205|             # same explicit stale-file cleanup tools/extractor.py's emit_records() already
  1206|             # does for identity_items_by_domain/*.csv before a fresh regenerate.
  1207|             name_bundle_analysis_dir = out_root / "results" / "bundle_analysis" / "name"
  1208|             if name_bundle_analysis_dir.is_dir():
  1209|                 try:
  1210|                     # retry_fs_op: a cloud-synced segments root (OneDrive, etc.) can hold a
  1211|                     # transient lock on a file/folder this pipeline just finished writing on
  1212|                     # the previous run, producing a Windows PermissionError ([WinError 5]
  1213|                     # Access is denied) on an otherwise-correct rmtree. Caught here (rather
  1214|                     # than left to propagate) for the same reason as
  1215|                     # _clear_stale_name_all_before_run above: retry_fs_op exhausting every
  1216|                     # attempt -- not just a transient lock -- must still route through
  1217|                     # step_failed/registry update rather than escape _run_one_segment()
  1218|                     # uncaught, which would leave this segment's registry row at a stale
  1219|                     # status=complete and cause it to be silently skipped forever.
  1220|                     retry_fs_op(shutil.rmtree, str(name_bundle_analysis_dir))
  1221|                 except Exception as exc:
  1222|                     step_failed = "clear_stale_name_bundle"
  1223|                     failure_notes = f"step=clear_stale_name_bundle error={exc}"
  1224|             if step_failed is None:
  1225|                 name_bundle_cmd = [
  1226|                     sys.executable,
  1227|                     str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
  1228|                     "--analysis-dir", str(out_root / "results" / "analysis"),
  1229|                     "--out-dir", str(out_root / "results" / "bundle_analysis"),
  1230|                     "--comparison-target", "name",
  1231|                     "--name-key-patterns-dir", str(out_root / "results" / "name_key" / "patterns" / "name"),
  1232|                     "--metadata-file", str(records_dir / "file_metadata.csv"),
  1233|                     "--no-discover-populations",
  1234|                 ]
  1235|                 name_bundle_cmd += ["--workers", str(bundle_workers)]
  1236|                 t_step3b_start = time.monotonic()
  1237|                 rc, tail, _content = run_step_log(name_bundle_cmd, out_root / "bundle_name.log", cwd=str(repo_root))
  1238|                 t_bundle_name = int(time.monotonic() - t_step3b_start)
  1239|                 log(f"[orchestrator]   step 3b name-bundle elapsed={t_bundle_name}s")
  1240|                 if rc != 0:
  1241|                     step_failed = "bundle_name"
  1242|                     failure_notes = f"step=bundle_name returncode={rc}\n{tail}"
  1243| 
  1244|         # Post-bundle validation (warn only, runs before registry write so warnings land in notes)
  1245|         if step_failed is None and run_type == "bundle":
  1246|             dag_nodes = out_root / "results" / "bundle_analysis" / "all" / "line_patterns" / "bundle_dag_nodes.csv"
  1247|             if not dag_nodes.is_file() or dag_nodes.stat().st_size == 0:
  1248|                 warn = (
  1249|                     f"[WARN orchestrator] segment={sid} line_patterns/bundle_dag_nodes.csv "
  1250|                     f"missing or empty — bundle analysis may not have run correctly"
  1251|                 )
  1252|                 log(warn)
  1253|                 notes_parts.append(warn)
  1254| 
  1255|         # BI merge (non-fatal; only runs when bundle succeeded)
  1256|         if step_failed is None and run_type == "bundle" and not skip_bi_merge:
  1257|             t_merge_start = time.monotonic()
  1258|             try:
  1259|                 active_domains = _active_domains_from_presence_csv(out_root / "results" / "analysis")
  1260|                 bundle_analysis_dir = out_root / "results" / "bundle_analysis" / "all"
  1261|                 merge_result = merge_bi_outputs(bundle_analysis_dir, active_domains=active_domains)
  1262|                 total_files = sum(v["files_merged"] for v in merge_result.values())
  1263|                 total_rows = sum(v["rows_written"] for v in merge_result.values())
  1264|                 log(
  1265|                     f"[orchestrator] bi_merge segment={sid} files_merged={total_files} rows_written={total_rows}"
  1266|                 )
  1267|             except Exception as merge_exc:
  1268|                 log(f"[WARN orchestrator] bi_merge failed for segment={sid}: {merge_exc}")
  1269|             t_merge = int(time.monotonic() - t_merge_start)
  1270|             log(f"[orchestrator]   bi_merge elapsed={t_merge}s")
  1271| 
  1272|         # Name-projection BI merge (opt-in; mirrors the config-leg merge above but reads
  1273|         # bundle_analysis/name_all/ -- the flat, single-path-segment location
  1274|         # run_bundle_analysis_for_target() relocates the name leg's ALL-view output to, so
  1275|         # it matches the Power BI model's pPurgeView folder-splice convention -- and the
  1276|         # name-target domain set)
  1277|         if step_failed is None and run_type == "bundle" and comparison_target in ("name", "both") and not skip_bi_merge:
  1278|             t_merge_name_start = time.monotonic()
  1279|             try:
  1280|                 active_domains_name = _active_domains_from_name_patterns(
  1281|                     out_root / "results" / "name_key" / "patterns" / "name"
  1282|                 )
  1283|                 bundle_analysis_name_dir = out_root / "results" / "bundle_analysis" / "name_all"
  1284|                 merge_result_name = merge_bi_outputs(bundle_analysis_name_dir, active_domains=active_domains_name)
  1285|                 total_files_name = sum(v["files_merged"] for v in merge_result_name.values())
  1286|                 total_rows_name = sum(v["rows_written"] for v in merge_result_name.values())
  1287|                 log(
  1288|                     f"[orchestrator] bi_merge_name segment={sid} files_merged={total_files_name} rows_written={total_rows_name}"
  1289|                 )
  1290|                 # PR3 BI-output-compatibility brief's "Column-shape constraint": every
  1291|                 # *_combined.csv under name_all/ must additionally declare
  1292|                 # comparison_target/coverage_class/provenance_note per row, strictly
  1293|                 # additive to the existing typed columns the Power BI model already reads.
  1294|                 annotate_stats = annotate_name_target_combined_files(bundle_analysis_name_dir)
  1295|                 log(f"[orchestrator] bi_merge_name_annotate segment={sid} files_annotated={len(annotate_stats)}")
  1296|             except Exception as merge_exc:
  1297|                 # Unlike the config leg's own bi_merge above (deliberately non-fatal --
  1298|                 # its "complete" status has no separate output-verifying marker),
  1299|                 # a failure here MUST fail the segment. _segment_has_name_leg_output()
  1300|                 # only checks that bundle_provenance.csv exists, which step 3b already
  1301|                 # wrote successfully before this block ever runs -- so a merge/annotate
  1302|                 # failure logged as a mere warning would still record status=complete,
  1303|                 # and a later non-forced run would then skip this segment forever,
  1304|                 # permanently leaving Power BI with combined files that are stale or
  1305|                 # missing the required comparison_target/coverage_class/provenance_note
  1306|                 # columns (PR review, #391, second round).
  1307|                 step_failed = "bi_merge_name"
  1308|                 failure_notes = f"step=bi_merge_name error={merge_exc}"
  1309|                 log(f"[WARN orchestrator] bi_merge_name failed for segment={sid}: {merge_exc}")
  1310|             t_merge_name = int(time.monotonic() - t_merge_name_start)
  1311|             log(f"[orchestrator]   bi_merge_name elapsed={t_merge_name}s")
  1312| 
  1313|         elapsed = int(time.monotonic() - t_start)
  1314| 
  1315|         timing_parts = [
  1316|             f"segment={sid}",
  1317|             f"prepare={t_prepare}s",
  1318|             f"patterns={t_patterns}s",
  1319|         ]
  1320|         if t_bundle is not None:
  1321|             timing_parts.append(f"bundle={t_bundle}s")
  1322|         if t_merge is not None:
  1323|             timing_parts.append(f"bi_merge={t_merge}s")
  1324|         if t_patterns_name is not None:
  1325|             timing_parts.append(f"patterns_name={t_patterns_name}s")
  1326|         if t_bundle_name is not None:
  1327|             timing_parts.append(f"bundle_name={t_bundle_name}s")
  1328|         if t_merge_name is not None:
  1329|             timing_parts.append(f"bi_merge_name={t_merge_name}s")
  1330|         timing_parts.append(f"total={elapsed}s")
  1331|         log(f"[orchestrator]   timing {' '.join(timing_parts)}")
  1332| 
  1333|         if step_failed is not None:
  1334|             log(f"[orchestrator]   failure_notes: {failure_notes}")
  1335| 
  1336|     # Update registry under lock
  1337|     with registry_lock:
  1338|         ri = reg_index.get(sid)
  1339|         if ri is not None:
  1340|             if step_failed is None:
  1341|                 registry[ri]["status"] = "complete"
  1342|                 registry[ri]["last_run_utc"] = utc_now_iso()
  1343|                 if "notes" in registry[ri]:
  1344|                     registry[ri]["notes"] = "; ".join(notes_parts)
  1345|             else:
  1346|                 registry[ri]["status"] = "failed"
  1347|                 registry[ri]["last_run_utc"] = utc_now_iso()
  1348|                 registry[ri]["notes"] = failure_notes[:500]
  1349|         write_registry_atomic(registry_file, registry)
  1350|         write_results_registry(
  1351|             manifest_file=manifest_file,
  1352|             registry_file=registry_file,
  1353|             output_file=results_registry_file,
  1354|         )
  1355| 
  1356|     # Update counters and read progress snapshot under lock
  1357|     with counters_lock:
  1358|         if step_failed is None:
  1359|             counters["complete"] += 1
  1360|         else:
  1361|             counters["failed"] += 1
  1362|             counters["failed_ids"].append(sid)
  1363|         done = counters["complete"] + counters["failed"]
  1364|         running = total - done - counters.get("skipped", 0)
  1365|         n_complete_now = counters["complete"]
  1366|         n_failed_now = counters["failed"]
  1367| 
  1368|     # Console: complete/failed status
  1369|     if step_failed is None:
  1370|         print(f"[orchestrator]   ✓ complete elapsed={elapsed}s [worker={worker_id}]", flush=True)
  1371|     else:
  1372|         print(f"[orchestrator]   ✗ failed at step={step_failed} [worker={worker_id}]", flush=True)
  1373| 
  1374|     # Console: progress after every completion
  1375|     print(
  1376|         f"[orchestrator]   progress: {n_complete_now}/{total} complete"
  1377|         f"  {max(0, running)} running  {n_failed_now} failed",
  1378|         flush=True,
  1379|     )
  1380| 
  1381|     return {
  1382|         "segment_id": sid,
  1383|         "status": "complete" if step_failed is None else "failed",
  1384|         "files": file_count,
  1385|         "level": level,
  1386|         "prepare_s": t_prepare,
  1387|         "patterns_s": t_patterns,
  1388|         "bundle_s": t_bundle if t_bundle is not None else 0,
  1389|         "bi_merge_s": t_merge if t_merge is not None else 0,
  1390|         "total_s": elapsed,
  1391|         "worker_id": worker_id,
  1392|         "patterns_top5": patterns_timing_lines[:5],
  1393|         "failure_note": failure_notes if step_failed else "",
  1394|     }
  1395| 
  1396| 
```
