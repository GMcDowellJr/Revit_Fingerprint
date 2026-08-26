# Chunk of tools/run_extract_all.py

- Source relative path: `tools/run_extract_all.py`
- Chunk: 3 of 3
- Original line range: 727-1234
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 6097e03c70f161dde5df1eb1e3887c55c0c5ff53ad8f51c123db9238e5ca6429
- Starts inside symbol: no
- Ends inside symbol: no

```
   727| def main() -> None:
   728|     stage_names = ["flatten", "sig_hash", "discover", "apply", "placeholders", "authority", "patterns", "split", "flat_tables"]
   729|     ap = argparse.ArgumentParser(
   730|         description=(
   731|             "Pipeline orchestrator with explicit stages: flatten (T0), sig_hash (T0.5), discover (T1), apply (T2), split, authority, patterns. "
   732|             "Default stages are flatten,sig_hash,discover."
   733|         ),
   734|         epilog=(
   735|             "Examples:\n"
   736|             "  default (draft prep): --stages flatten,discover\n"
   737|             "  governance prep:      --stages sig_hash,flatten,discover\n"
   738|             "  operational commit:  --stages flatten,discover,apply\n"
   739|             "  placeholder prep:    --stages flatten,discover,apply,placeholders\n"
   740|             "  analysis after apply: --stages flatten,discover,apply,placeholders,split,authority,patterns\n"
   741|             "  degraded exploratory analysis (not governance-grade): add --allow-sig-hash-join-key\n"
   742|             "  matrix reference: docs/extract_stage_matrix.md"
   743|         ),
   744|         formatter_class=argparse.RawTextHelpFormatter,
   745|     )
   746|     ap.add_argument("exports_dir", help="Folder containing fingerprint exports (*__fingerprint.json, or legacy *.details.json / *.index.json).")
   747|     ap.add_argument("--out-root", required=True, help="Output root folder.")
   748|     ap.add_argument(
   749|         "--out-root-is-results-root",
   750|         action="store_true",
   751|         help="Treat --out-root itself as the results root: records/, analysis/, policies/, "
   752|              "label_synthesis/, etc. land directly under it instead of under {out-root}/results/. "
   753|              "Use this when --out-root already names a dedicated results directory (e.g. "
   754|              "corpus_update_runbook.ps1's $RESULTS, sibling to exports/ and segments/). Leave "
   755|              "unset for per-segment callers (run_segment_orchestrator.py) that want results/ "
   756|              "nested under --out-root to stay separate from segment-level log files."
   757|     )
   758|     ap.add_argument("--seed", default=None, help="Path to a seed fingerprint JSON. When provided, emits a seed-comparison sidecar for the seed-baseline BI dashboard (project drift vs template). Not part of standard segment runs.")
   759|     ap.add_argument("--domains", default=None, help="Comma list of domains; if omitted, infer from exports.")
   760|     ap.add_argument("--stages", default="flatten,sig_hash,discover", help="Comma-separated stages to run. Default: flatten,sig_hash,discover.")
   761|     ap.add_argument("--skip-stages", default="", help="Comma-separated stages to skip from --stages.")
   762|     ap.add_argument("--join-policy", default=None, help="Policy JSON path used by apply stage.")
   763|     ap.add_argument("--sig-hash-policy", default=None, help="Policy JSON path used by sig_hash stage.")
   764|     ap.add_argument("--skip-sig-hash-missing-policy", action="store_true", help="Skip sig_hash stage if no policy file is found.")
   765|     ap.add_argument("--allow-sig-hash-join-key", action="store_true", help="Allow degraded identity-mode join keys (sig_hash_as_join_key.v1) for exploratory analysis.")
   766|     ap.add_argument("--split-domains", nargs="?", const="__ALL__", default=None, help="Domains for split stage; optional CSV. If no value, run all discovered domains.")
   767|     ap.add_argument(
   768|         "--flat-tables-emit",
   769|         default="layer_stacks",
   770|         help="Comma-separated emit types for the flat_tables stage. Used primarily for compound type layer stack export (layer_stacks, layer_stack_rows). Default is layer_stacks.",
   771|     )
   772|     ap.add_argument(
   773|         "--filter-export-run-ids",
   774|         default=None,
   775|         help="Path to a text file with one export_run_id per line. "
   776|              "Filters meta_rows and record_rows to the specified population "
   777|              "before running authority/patterns stages. "
   778|              "flatten/apply/placeholders/split stages are unaffected."
   779|     )
   780|     ap.add_argument(
   781|         "--records-dir",
   782|         default=None,
   783|         help="Path to directory containing records.csv and file_metadata.csv. "
   784|              "Overrides the default {results-root}/records/ for authority/patterns stages "
   785|              "(results-root is --out-root itself with --out-root-is-results-root, else "
   786|              "{out-root}/results/). Use when running per-segment analysis where records "
   787|              "live at corpus level."
   788|     )
   789|     ap.add_argument(
   790|         "--label-synth-dir",
   791|         default=None,
   792|         help="Path to a label_synthesis/ directory to use as the read source for label "
   793|              "population, LLM cache, and curator annotations. Overrides the default "
   794|              "{results-root}/label_synthesis/ for the read path only — analysis outputs "
   795|              "still write to {results-root}/ (see --out-root-is-results-root for what "
   796|              "results-root resolves to). Use when running per-segment analysis so "
   797|              "that corpus-level LLM improvements are picked up without rebuilding per segment."
   798|     )
   799|     ap.add_argument(
   800|         "--emit-analysis-workers",
   801|         type=int,
   802|         default=4,
   803|         help="Number of worker processes for the emit_analysis domain loop. Default: 4. "
   804|              "Use 1 to run sequentially (same behaviour as before parallelism was added).",
   805|     )
   806|     args = ap.parse_args()
   807| 
   808|     allow_sig_hash_join_key = args.allow_sig_hash_join_key
   809|     require_join_policy = True
   810|     label_synth_dir = Path(args.label_synth_dir).resolve() if args.label_synth_dir else None
   811| 
   812|     selected_stages = _parse_stage_csv(args.stages) or ["flatten", "sig_hash", "discover"]
   813| 
   814|     skipped = set(_parse_stage_csv(args.skip_stages))
   815|     for st in selected_stages + list(skipped):
   816|         if st not in stage_names:
   817|             raise SystemExit(f"Unknown stage: {st}. Valid stages: {','.join(stage_names)}")
   818|     selected_stages = [s for s in stage_names if s in selected_stages and s not in skipped]
   819|     if "apply" in selected_stages and "flatten" not in selected_stages:
   820|         selected_stages = ["flatten"] + selected_stages
   821|         report_note = "auto_inserted_flatten_for_apply"
   822|     else:
   823|         report_note = None
   824|     if "apply" in selected_stages and "sig_hash" not in selected_stages:
   825|         insert_at = selected_stages.index("apply")
   826|         selected_stages.insert(insert_at, "sig_hash")
   827|         report_note = (report_note + "|auto_inserted_sig_hash_for_apply") if report_note else "auto_inserted_sig_hash_for_apply"
   828| 
   829|     plan_msg = " -> ".join([s if s in selected_stages else f"({s} skipped)" for s in stage_names])
   830|     if require_join_policy and any(s in selected_stages for s in ("split", "authority", "patterns")) and "apply" not in selected_stages:
   831|         plan_msg += " -> (analysis gated: requires policy join keys; include apply stage)"
   832|     print(f"Plan: {plan_msg}")
   833| 
   834|     exports_dir = Path(args.exports_dir).resolve()
   835|     out_root = Path(args.out_root).resolve()
   836|     # Two callers disagree about what --out-root names, so the "results/" nesting
   837|     # level is explicit rather than hardcoded either way:
   838|     #  - run_segment_orchestrator.py passes a per-segment folder as --out-root and
   839|     #    wants results/ nested under it, to separate this script's output from the
   840|     #    segment-level run.log/patterns.log/bundle.log/export_run_ids.txt siblings
   841|     #    it writes itself (default behaviour here, --out-root-is-results-root unset).
   842|     #  - corpus_update_runbook.ps1 passes its corpus-level $RESULTS root (itself
   843|     #    already a dedicated results directory, sibling to exports/ and segments/)
   844|     #    and wants records/analysis/policies/etc. directly under it -- no extra
   845|     #    nesting -- matching patch_all_domain_patterns.py's --results-root and the
   846|     #    runbook's $RECORDS ($RESULTS\records, not $RESULTS\results\records).
   847|     #    Pass --out-root-is-results-root to select this layout.
   848|     v21_root = out_root if args.out_root_is_results_root else out_root / "results"
   849|     v21_phase0_dir = v21_root / "records"
   850|     effective_phase0_dir = v21_phase0_dir
   851|     v21_analysis_dir = v21_root / "analysis"
   852|     v21_split_root = v21_root / "split_analysis"
   853|     flat_tables_dir = v21_root / "flat_tables"
   854| 
   855|     _ensure_dir(out_root)
   856|     surfaces = _detect_surfaces(exports_dir)
   857| 
   858|     if args.domains and str(args.domains).strip():
   859|         domains = [d.strip() for d in str(args.domains).split(",") if d.strip()]
   860|     else:
   861|         domains = _infer_domains(exports_dir)
   862|     active_domains = [d for d in domains if d not in SUPPRESSED_DOWNSTREAM_DOMAINS]
   863|     suppressed_domains = sorted(set(domains) - set(active_domains))
   864|     if suppressed_domains:
   865|         sys.stderr.write(
   866|             f"[INFO extract_all] suppressed_downstream_domains={','.join(suppressed_domains)}\n"
   867|         )
   868|     if not domains and any(s in selected_stages for s in ("patterns",)):
   869|         raise SystemExit("No domains inferred; provide --domains.")
   870| 
   871|     env = os.environ.copy()
   872|     report: Dict[str, Any] = {"tool": "tools/run_extract_all.py", "exports_dir": str(exports_dir), "out_root": str(out_root), "surfaces": surfaces, "domains": domains, "active_domains": active_domains, "selected_stages": selected_stages, "commands": [], "notes": []}
   873|     if report_note:
   874|         report["notes"].append(report_note)
   875|     meta_rows: List[Dict[str, str]] = []
   876|     record_rows: List[Dict[str, str]] = []
   877| 
   878|     if "flatten" in selected_stages:
   879|         print("[extract_all] Stage flatten (T0): emitting flatten outputs...", flush=True)
   880|         _ensure_dir(v21_phase0_dir)
   881|         report["commands"].append({"stage": "flatten", "out": str(v21_phase0_dir)})
   882|         file_count, record_count = emit_records(exports_dir, v21_phase0_dir, file_id_mode="basename")
   883|         print(f"[extract_all] Stage flatten complete: rows={record_count} files={file_count} out={v21_phase0_dir}", flush=True)
   884|         items_csv = v21_phase0_dir / "identity_items.csv"
   885|         stats = _append_line_pattern_synthetic_norm_hash(items_csv)
   886|         print(
   887|             f"[extract_all] line_patterns segments_norm_hash: "
   888|             f"total={stats['total']} ok={stats['ok']} missing={stats['missing']}",
   889|             flush=True,
   890|         )
   891|     if "sig_hash" in selected_stages:
   892|         _records_csv = effective_phase0_dir / "records.csv"
   893|         _items_csv = effective_phase0_dir / "identity_items.csv"
   894|         _native_shard_complete = effective_phase0_dir / "identity_items_by_domain" / ".complete"
   895|         if not _records_csv.is_file() or not (_native_shard_complete.is_file() or _items_csv.is_file()):
   896|             raise SystemExit(
   897|                 "sig_hash stage requires records.csv and identity_items (shards or "
   898|                 "identity_items.csv) to exist. Run the flatten stage first, or include "
   899|                 "flatten in --stages."
   900|             )
   901|         sig_pol = _resolve_sig_hash_policy_path(args.sig_hash_policy, v21_root)
   902|         if sig_pol is None:
   903|             if args.skip_sig_hash_missing_policy:
   904|                 sys.stderr.write(
   905|                     "[WARN extract_all] sig_hash stage skipped: no policy file found. "
   906|                     "sig_hash and join_hash will be empty for all records.\n"
   907|                 )
   908|                 report["notes"].append("sig_hash stage skipped: no policy found")
   909|             else:
   910|                 raise SystemExit("sig_hash stage requested but no policy file found. Use --sig-hash-policy or --skip-sig-hash-missing-policy.")
   911|         else:
   912|             print(f"[extract_all] Stage sig_hash (T0.5): applying policy {sig_pol.name} ...", flush=True)
   913|             diag = _apply_sig_hash_to_phase0(effective_phase0_dir, sig_pol, active_domains or domains)
   914|             diag_dir = v21_root / "diagnostics"
   915|             _ensure_dir(diag_dir)
   916|             diag_path = diag_dir / "sig_hash_policy_diagnostics.json"
   917|             diag_path.write_text(json.dumps(diag, indent=2, sort_keys=True) + "\n", encoding="utf-8")
   918|             print(
   919|                 f"[extract_all] Stage sig_hash complete: "
   920|                 f"processed={diag['records_processed']} hashed={diag['records_hashed']} "
   921|                 f"blocked={diag['records_blocked']} degraded={diag['records_degraded']} "
   922|                 f"basis_items={diag.get('sig_basis_items_written', 0)} "
   923|                 f"domains_without_policy={len(diag['domains_without_policy'])}",
   924|                 flush=True,
   925|             )
   926|             report["commands"].append({"stage": "sig_hash", "policy": str(sig_pol), "out": str(effective_phase0_dir), "diagnostics": str(diag_path)})
   927| 
   928|     if "discover" in selected_stages:
   929|         print("[extract_all] Stage discover (T1): exploring join/sig hash policy candidates...", flush=True)
   930|         cmd_discover = [
   931|             sys.executable,
   932|             "tools/discover_hash_policy.py",
   933|             "--phase0-dir",
   934|             str(effective_phase0_dir),
   935|             "--discovery-target",
   936|             "both",
   937|             "--search-modes",
   938|             "greedy,pareto",
   939|             "--policy-modes",
   940|             "discover,validate,harsh",
   941|         ]
   942|         if args.domains and str(args.domains).strip():
   943|             cmd_discover += ["--domains", str(args.domains)]
   944|         report["commands"].append({"stage": "discover", "cmd": cmd_discover})
   945|         _run(cmd_discover, env=env)
   946| 
   947|     if "apply" in selected_stages:
   948|         print("[extract_all] Stage apply (T2): applying join policy to flatten outputs...", flush=True)
   949|         items_csv = effective_phase0_dir / "identity_items.csv"
   950|         stats = _append_line_pattern_synthetic_norm_hash(items_csv)
   951|         print(
   952|             f"[extract_all] line_patterns segments_norm_hash (pre-apply): "
   953|             f"total={stats['total']} ok={stats['ok']} missing={stats['missing']}",
   954|             flush=True,
   955|         )
   956|         _validate_line_pattern_synthetic_norm_hash(effective_phase0_dir)
   957|         print(f"[apply] using enriched records dir: {effective_phase0_dir}", flush=True)
   958|         policy_path = Path(args.join_policy).resolve() if args.join_policy else (v21_root / "policies" / "domain_join_key_policies.v21.json").resolve()
   959|         cmd_apply = [sys.executable, "tools/apply_join_policy.py", "--phase0-dir", str(effective_phase0_dir), "--join-policy", str(policy_path)]
   960|         report["commands"].append({"stage": "apply", "cmd": cmd_apply})
   961|         _run(cmd_apply, env=env)
   962| 
   963|     if "placeholders" in selected_stages:
   964|         print("[extract_all] Stage placeholders (T2b): generating placeholder exclusion CSVs...", flush=True)
   965|         cmd_ph = [sys.executable, "tools/bundle_analysis/placeholder_exclusions.py", "--phase0-dir", str(v21_phase0_dir), "--policies-dir", "policies", "--out-dir", str(v21_root / "placeholder_exclusions"), "--file-metadata-path", str(v21_phase0_dir / "file_metadata.csv")]
   966|         report["commands"].append({"stage": "placeholders", "cmd": cmd_ph})
   967|         try:
   968|             _run(cmd_ph, env=env)
   969|         except Exception as e:
   970|             sys.stderr.write("[WARN extract_all] placeholders stage failed; continuing: {}\n".format(e))
   971| 
   972|     if "authority" in selected_stages or "patterns" in selected_stages:
   973|         _t_patterns_stage_start = time.perf_counter()
   974|         records_source_dir = Path(args.records_dir).resolve() if args.records_dir else v21_phase0_dir
   975|         phase0_records_csv = records_source_dir / "records.csv"
   976|         if phase0_records_csv.is_file():
   977|             # Always reload from disk here so analyze uses post-apply join_hash values,
   978|             # not in-memory rows captured before join policy application.
   979|             record_rows = _read_csv_rows(phase0_records_csv)
   980|         if (records_source_dir / "file_metadata.csv").is_file():
   981|             meta_rows = _read_csv_rows(records_source_dir / "file_metadata.csv")
   982|         # Snapshot pre-filter rows so split domain auto-discovery (which runs after
   983|         # this block) uses the full post-reload population regardless of filter.
   984|         _pre_filter_record_rows = record_rows
   985| 
   986|         if args.filter_export_run_ids:
   987|             _filter_path = Path(args.filter_export_run_ids)
   988|             if not _filter_path.is_file():
   989|                 raise SystemExit(f"--filter-export-run-ids file not found: {_filter_path}")
   990|             _allowed = {
   991|                 line.strip() for line in _filter_path.read_text(encoding="utf-8-sig").splitlines()
   992|                 if line.strip()
   993|             }
   994|             meta_rows = [r for r in meta_rows if r.get("export_run_id", "").strip() in _allowed]
   995|             record_rows = [r for r in record_rows if r.get("export_run_id", "").strip() in _allowed]
   996|             print(
   997|                 f"[extract_all] export_run_id filter applied: "
   998|                 f"meta_rows={len(meta_rows)} record_rows={len(record_rows)}",
   999|                 flush=True,
  1000|             )
  1001|             if not meta_rows or not record_rows:
  1002|                 _sample_allowed = sorted(_allowed)[:5]
  1003|                 _meta_ids = sorted({r.get("export_run_id", "").strip() for r in _read_csv_rows(records_source_dir / "file_metadata.csv")})[:5] if (records_source_dir / "file_metadata.csv").is_file() else []
  1004|                 sys.stderr.write(
  1005|                     f"[WARN extract_all] filter left meta_rows={len(meta_rows)} record_rows={len(record_rows)} — "
  1006|                     f"emit_analysis will be skipped and pattern_presence_file.csv will NOT be written.\n"
  1007|                     f"[WARN extract_all] records_source_dir={records_source_dir}\n"
  1008|                     f"[WARN extract_all] filter_file={_filter_path} (first 5 IDs: {_sample_allowed})\n"
  1009|                     f"[WARN extract_all] file_metadata.csv first 5 export_run_ids: {_meta_ids}\n"
  1010|                 )
  1011| 
  1012|         # Validate after export-run-id filtering (if any) so a targeted rerun is not
  1013|         # blocked by incomplete governance fields on files outside the requested scope.
  1014|         _check_governance_field_completeness(meta_rows)
  1015| 
  1016|         if require_join_policy and phase0_records_csv.is_file():
  1017|             _enforce_policy_gate(record_rows, v21_root / "diagnostics", active_domains, allow_sig_hash_join_key)
  1018| 
  1019|         if meta_rows and record_rows:
  1020|             _native_shard_dir = v21_phase0_dir / "identity_items_by_domain"
  1021|             if (_native_shard_dir / ".complete").is_file():
  1022|                 report["notes"].append(f"identity_items_shards={_native_shard_dir}")
  1023|             else:
  1024|                 # Legacy fallback for a phase0 dir produced before native shard
  1025|                 # writing existed: derive shards from the monolithic file, if present.
  1026|                 _legacy_shard_dir = _ensure_domain_scoped_identity_items(v21_phase0_dir)
  1027|                 if _legacy_shard_dir is not None:
  1028|                     report["notes"].append(f"identity_items_shards={_legacy_shard_dir}")
  1029| 
  1030|             # Ensure modal label population artifacts exist for the active v2.1 emit path.
  1031|             # records_source_dir and v21_root are already resolved against the chosen
  1032|             # --out-root-is-results-root layout above -- pass them explicitly rather than
  1033|             # letting build_label_population.py re-derive its own {out-root}/results/...
  1034|             # paths, which would only match the nested (default) layout.
  1035|             cmd_label_pop = [
  1036|                 sys.executable,
  1037|                 "tools/label_synthesis/build_label_population.py",
  1038|                 "--out-root",
  1039|                 str(out_root),
  1040|                 "--records-dir",
  1041|                 str(records_source_dir),
  1042|                 "--label-synth-dir",
  1043|                 str(v21_root / "label_synthesis"),
  1044|             ]
  1045|             report["commands"].append({"stage": "analyze", "cmd": cmd_label_pop})
  1046|             _run(cmd_label_pop, env=env)
  1047| 
  1048|             _ensure_dir(v21_analysis_dir)
  1049|             seed_export_run_id = ""
  1050|             seed_path = Path(args.seed).resolve() if args.seed else None
  1051|             if seed_path is not None:
  1052|                 candidate_ids = sorted(
  1053|                     {
  1054|                         str(r.get("export_run_id", "")).strip()
  1055|                         for r in meta_rows
  1056|                         if str(r.get("file_id", "")).strip() == seed_path.name
  1057|                     }
  1058|                 )
  1059|                 if len(candidate_ids) != 1:
  1060|                     raise ValueError(
  1061|                         f"Expected exactly one export_run_id for seed file {seed_path.name!r}; found {candidate_ids}"
  1062|                     )
  1063|                 seed_export_run_id = candidate_ids[0]
  1064| 
  1065|             if seed_export_run_id:
  1066|                 full_seed_dir = v21_analysis_dir / "_seed_full"
  1067|                 _ensure_dir(full_seed_dir)
  1068|                 _t0 = time.perf_counter()
  1069|                 emit_analysis(
  1070|                     meta_rows,
  1071|                     record_rows,
  1072|                     full_seed_dir,
  1073|                     phase0_dir=v21_phase0_dir,
  1074|                     results_v21_dir=v21_root,
  1075|                     label_synth_dir=label_synth_dir,
  1076|                     workers=args.emit_analysis_workers,
  1077|                 )
  1078|                 sys.stderr.write(f"[patterns_timing] stage=emit_analysis_seed elapsed={time.perf_counter()-_t0:.2f}s\n")
  1079|                 sys.stderr.flush()
  1080|                 corpus_meta_rows = [r for r in meta_rows if str(r.get("export_run_id", "")).strip() != seed_export_run_id]
  1081|                 corpus_record_rows = [r for r in record_rows if str(r.get("export_run_id", "")).strip() != seed_export_run_id]
  1082|                 _t0 = time.perf_counter()
  1083|                 analysis_run_id = emit_analysis(
  1084|                     corpus_meta_rows,
  1085|                     corpus_record_rows,
  1086|                     v21_analysis_dir,
  1087|                     phase0_dir=v21_phase0_dir,
  1088|                     results_v21_dir=v21_root,
  1089|                     label_synth_dir=label_synth_dir,
  1090|                     workers=args.emit_analysis_workers,
  1091|                 )
  1092|                 sys.stderr.write(f"[patterns_timing] stage=emit_analysis_corpus elapsed={time.perf_counter()-_t0:.2f}s\n")
  1093|                 sys.stderr.flush()
  1094| 
  1095|                 corpus_domain_patterns = read_csv_rows(v21_analysis_dir / "domain_patterns.csv")
  1096|                 full_domain_patterns = read_csv_rows(full_seed_dir / "domain_patterns.csv")
  1097|                 full_presence = read_csv_rows(full_seed_dir / "pattern_presence_file.csv")
  1098|                 seed_pattern_keys = {
  1099|                     (str(r.get("domain", "")).strip(), str(r.get("pattern_id", "")).strip())
  1100|                     for r in full_presence
  1101|                     if str(r.get("export_run_id", "")).strip() == seed_export_run_id
  1102|                     and str(r.get("domain", "")).strip()
  1103|                     and str(r.get("pattern_id", "")).strip()
  1104|                 }
  1105|                 merged_domain_patterns: List[Dict[str, str]] = []
  1106|                 existing_keys: set[Tuple[str, str]] = set()
  1107|                 for row in corpus_domain_patterns:
  1108|                     key = (str(row.get("domain", "")).strip(), str(row.get("pattern_id", "")).strip())
  1109|                     existing_keys.add(key)
  1110|                     new_row = dict(row)
  1111|                     new_row["is_seed"] = "true" if key in seed_pattern_keys else "false"
  1112|                     merged_domain_patterns.append(new_row)
  1113|                 for row in full_domain_patterns:
  1114|                     key = (str(row.get("domain", "")).strip(), str(row.get("pattern_id", "")).strip())
  1115|                     if key in existing_keys or key not in seed_pattern_keys:
  1116|                         continue
  1117|                     new_row = dict(row)
  1118|                     new_row["analysis_run_id"] = analysis_run_id
  1119|                     new_row["is_seed"] = "true"
  1120|                     merged_domain_patterns.append(new_row)
  1121| 
  1122|                 merged_domain_patterns.sort(
  1123|                     key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("pattern_id", ""))
  1124|                 )
  1125|                 if merged_domain_patterns:
  1126|                     fieldnames = list(merged_domain_patterns[0].keys())
  1127|                     if "is_seed" not in fieldnames:
  1128|                         fieldnames.append("is_seed")
  1129|                     atomic_write_csv(v21_analysis_dir / "domain_patterns.csv", fieldnames, merged_domain_patterns)
  1130| 
  1131|                 schema_version = read_csv_rows(v21_analysis_dir / "corpus_manifest.csv")[0].get("schema_version", "")
  1132|                 seed_sidecar_rows = [
  1133|                     {
  1134|                         "domain": dom,
  1135|                         "pattern_id": pid,
  1136|                         "is_seed": "true",
  1137|                         "seed_file_stem": seed_path.stem if seed_path is not None else "",
  1138|                     }
  1139|                     for dom, pid in sorted(seed_pattern_keys)
  1140|                 ]
  1141|                 sidecar_path = write_sidecar(v21_analysis_dir, seed_export_run_id, seed_sidecar_rows, schema_version)
  1142|                 print(f"[extract] Seed reference bundle written to {sidecar_path}")
  1143|             else:
  1144|                 _t0 = time.perf_counter()
  1145|                 analysis_run_id = emit_analysis(
  1146|                     meta_rows,
  1147|                     record_rows,
  1148|                     v21_analysis_dir,
  1149|                     phase0_dir=v21_phase0_dir,
  1150|                     results_v21_dir=v21_root,
  1151|                     label_synth_dir=label_synth_dir,
  1152|                     workers=args.emit_analysis_workers,
  1153|                 )
  1154|                 sys.stderr.write(f"[patterns_timing] stage=emit_analysis elapsed={time.perf_counter()-_t0:.2f}s\n")
  1155|                 sys.stderr.flush()
  1156|                 domain_patterns = read_csv_rows(v21_analysis_dir / "domain_patterns.csv")
  1157|                 for row in domain_patterns:
  1158|                     row["is_seed"] = "false"
  1159|                 if domain_patterns:
  1160|                     fieldnames = list(domain_patterns[0].keys())
  1161|                     if "is_seed" not in fieldnames:
  1162|                         fieldnames.append("is_seed")
  1163|                     atomic_write_csv(v21_analysis_dir / "domain_patterns.csv", fieldnames, domain_patterns)
  1164|             report["notes"].append(f"analysis_run_id={analysis_run_id}")
  1165|             _t0 = time.perf_counter()
  1166|             emit_element_dominance(v21_analysis_dir)
  1167|             sys.stderr.write(f"[patterns_timing] stage=emit_element_dominance elapsed={time.perf_counter()-_t0:.2f}s\n")
  1168|             sys.stderr.flush()
  1169|             report["notes"].append("element_dominance: emitted")
  1170|         sys.stderr.write(
  1171|             f"[patterns_timing] stage=patterns_total elapsed={time.perf_counter()-_t_patterns_stage_start:.2f}s "
  1172|             f"files={len(meta_rows)}\n"
  1173|         )
  1174|         sys.stderr.flush()
  1175|         record_rows = _pre_filter_record_rows
  1176| 
  1177|     split_domains: List[str] = []
  1178|     if "split" in selected_stages:
  1179|         if args.split_domains is None or str(args.split_domains) == "__ALL__":
  1180|             # Always read from records.csv on disk — it reflects flatten remaps and
  1181|             # suppression correctly and is present whether flatten ran now or previously.
  1182|             # record_rows is empty since emit_records now returns counts only.
  1183|             _phase0_records_csv = v21_phase0_dir / "records.csv"
  1184|             if _phase0_records_csv.is_file():
  1185|                 split_domains = sorted(
  1186|                     {
  1187|                         str(r.get("domain", "")).strip()
  1188|                         for r in _iter_csv_rows(_phase0_records_csv)
  1189|                         if str(r.get("domain", "")).strip() and str(r.get("domain", "")).strip() not in SUPPRESSED_DOWNSTREAM_DOMAINS
  1190|                     },
  1191|                     key=lambda s: s.lower(),
  1192|                 )
  1193|             if not split_domains:
  1194|                 split_domains = [d for d in _discover_domains_from_exports(exports_dir) if d not in SUPPRESSED_DOWNSTREAM_DOMAINS]
  1195|         else:
  1196|             split_domains = [d.strip() for d in str(args.split_domains).split(",") if d.strip() and d.strip() not in SUPPRESSED_DOWNSTREAM_DOMAINS]
  1197| 
  1198|     if split_domains:
  1199|         print(f"[extract_all] Stage split: running split detection for {len(split_domains)} domain(s)...", flush=True)
  1200|         _ensure_dir(v21_split_root)
  1201|         phase0_records_csv = v21_phase0_dir / "records.csv"
  1202|         use_phase0_dir = phase0_records_csv.is_file()
  1203|         if use_phase0_dir and require_join_policy:
  1204|             _enforce_policy_gate(_read_csv_rows(phase0_records_csv), v21_root / "diagnostics", split_domains, allow_sig_hash_join_key)
  1205|         for split_domain in split_domains:
  1206|             cmd_split = [sys.executable, "tools/run_split_detection_all.py", str(exports_dir), "--domain", split_domain, "--out-root", str(v21_split_root / split_domain), "--mode", "allpairs", *(["--phase0-dir", str(v21_phase0_dir)] if use_phase0_dir else []), *(["--allow-sig-hash-join-key"] if allow_sig_hash_join_key else [])]
  1207|             report["commands"].append({"stage": "split", "domain": split_domain, "cmd": cmd_split})
  1208|             _run(cmd_split, env=env)
  1209| 
  1210|     if "flat_tables" in selected_stages:
  1211|         print("[extract_all] Stage flat_tables: writing flat CSV tables...", flush=True)
  1212|         _ensure_dir(flat_tables_dir)
  1213|         cmd_flat = [
  1214|             sys.executable,
  1215|             "tools/export_to_flat_tables.py",
  1216|             "--root_dir", str(exports_dir),
  1217|             "--out_dir", str(flat_tables_dir),
  1218|             "--file_id_mode", "basename",
  1219|             "--emit", str(args.flat_tables_emit),
  1220|         ]
  1221|         if args.domains and str(args.domains).strip():
  1222|             cmd_flat += ["--domains", str(args.domains)]
  1223|         report["commands"].append({"stage": "flat_tables", "cmd": cmd_flat})
  1224|         _run(cmd_flat, env=env)
  1225|         print(f"[extract_all] Stage flat_tables complete: out={flat_tables_dir}", flush=True)
  1226| 
  1227|     report_path = out_root / "extract_all.report.json"
  1228|     with report_path.open("w", encoding="utf-8") as f:
  1229|         json.dump(report, f, indent=2, sort_keys=True)
  1230|     print(f"Wrote: {report_path}")
  1231| 
  1232| 
  1233| if __name__ == "__main__":
  1234|     main()
```
