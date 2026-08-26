# Chunk of tools/label_synthesis/build_semantic_groups.py

- Source relative path: `tools/label_synthesis/build_semantic_groups.py`
- Chunk: 3 of 3
- Original line range: 900-1196
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_semantic_groups, main
- Source SHA-256: ecbac527e320e64b586fce64b729698632c2ac0daced16f12b6c615ec9265668
- Starts inside symbol: no
- Ends inside symbol: no

```
   900| def build_semantic_groups(
   901|     *,
   902|     out_root: Path,
   903|     domain: Optional[str],
   904|     dry_run: bool,
   905|     force_refresh: bool,
   906|     max_patterns: Optional[int],
   907|     export_prompts: Optional[Path],
   908|     import_results: Optional[Path],
   909|     peer_vocab_from_cache: bool,
   910|     export_batch_size: Optional[int],
   911| ) -> None:
   912|     if (out_root / "analysis_v21").is_dir() and (out_root / "phase0_v21").is_dir():
   913|         results_v21 = out_root
   914|     else:
   915|         results_v21 = out_root / "Results_v21"
   916|     analysis_dir = results_v21 / "analysis_v21"
   917|     phase0_dir = results_v21 / "phase0_v21"
   918|     shards_dir = phase0_dir / "identity_items_by_domain"
   919|     cache_path = results_v21 / "label_synthesis" / "label_semantic_groups.json"
   920|     export_progress_path = results_v21 / "label_synthesis" / "prompt_export_progress.json"
   921|     print(f"[build_semantic_groups] results_v21={results_v21}")
   922|     print(f"[build_semantic_groups] analysis_dir={analysis_dir}")
   923|     print(f"[build_semantic_groups] shards_dir={shards_dir}")
   924|     print(f"[build_semantic_groups] cache_path={cache_path}")
   925|     print(f"[build_semantic_groups] export_progress_path={export_progress_path}")
   926| 
   927|     if domain and domain not in SEMANTIC_GROUPING_DOMAINS:
   928|         raise ValueError(f"--domain must be one of {SEMANTIC_GROUPING_DOMAINS}")
   929|     if export_batch_size is not None and export_batch_size <= 0:
   930|         raise ValueError("--export-batch-size must be a positive integer.")
   931| 
   932|     cache = _load_cache(cache_path)
   933|     cache_groups = cache.get("groups", {})
   934|     if not isinstance(cache_groups, dict):
   935|         cache_groups = {}
   936|         cache["groups"] = cache_groups
   937| 
   938|     analysis_run_id = _load_analysis_run_id(analysis_dir)
   939|     for d in SEMANTIC_GROUPING_DOMAINS:
   940|         if domain and d != domain:
   941|             continue
   942|         cache_groups.setdefault(d, {})
   943| 
   944|     if import_results:
   945|         with import_results.open("r", encoding="utf-8") as f:
   946|             rows = json.load(f)
   947|         if not isinstance(rows, list):
   948|             raise ValueError("--import-results must point to a JSON array.")
   949|         imported = 0
   950|         skipped = 0
   951|         for row in rows:
   952|             if not isinstance(row, dict):
   953|                 skipped += 1
   954|                 continue
   955|             d = str(row.get("domain", "")).strip()
   956|             pattern_id = str(row.get("pattern_id", "")).strip()
   957|             if not d or not pattern_id:
   958|                 skipped += 1
   959|                 continue
   960|             if domain and d != domain:
   961|                 skipped += 1
   962|                 continue
   963|             if d not in SEMANTIC_GROUPING_DOMAINS:
   964|                 skipped += 1
   965|                 continue
   966|             cache_groups.setdefault(d, {})
   967|             if not force_refresh and pattern_id in cache_groups[d]:
   968|                 skipped += 1
   969|                 continue
   970|             payload = _normalize_import_payload(row)
   971|             cache_groups[d][pattern_id] = {
   972|                 "semantic_group": payload["semantic_group"],
   973|                 "confidence": payload["confidence"],
   974|                 "rationale": payload["rationale"],
   975|                 "pattern_label_human": str(row.get("pattern_label_human", "")).strip(),
   976|                 "reviewed": False,
   977|             }
   978|             imported += 1
   979|         cache["schema_version"] = CACHE_SCHEMA_VERSION
   980|         cache["analysis_run_id"] = analysis_run_id
   981|         cache["generated_at"] = _utc_now_iso()
   982|         cache["groups"] = cache_groups
   983|         _save_cache(cache_path, cache)
   984|         print(
   985|             f"[build_semantic_groups] Imported {imported} results from {import_results} "
   986|             f"(skipped={skipped}) and wrote cache: {cache_path}"
   987|         )
   988|         return
   989| 
   990|     patterns_by_domain = _load_pattern_rows(analysis_dir, domain)
   991|     if not patterns_by_domain:
   992|         print("[build_semantic_groups] WARN: no eligible patterns found in scope.")
   993|         print("[build_semantic_groups] Check --out-root and ensure domain_patterns.csv has non-missing pattern_label_human/source.")
   994| 
   995|     exported_prompts: list[Dict[str, str]] = []
   996|     export_progress = _load_export_progress(export_progress_path) if export_prompts else {}
   997| 
   998|     for d, pattern_rows in patterns_by_domain.items():
   999|         if not pattern_rows:
  1000|             continue
  1001|         print(f"[build_semantic_groups] domain={d} eligible_patterns={len(pattern_rows)}")
  1002|         pattern_to_record = _load_pattern_to_record_pk(analysis_dir, d)
  1003|         identity_by_record = _load_identity_items_by_record(phase0_dir, shards_dir, d)
  1004|         if identity_by_record is None:
  1005|             continue
  1006| 
  1007|         print(f"[build_semantic_groups] domain={d} patterns={len(pattern_rows)}")
  1008|         processed = 0
  1009|         assigned_this_run: List[str] = []
  1010|         seeded_peer_vocab: List[str] = []
  1011|         if export_prompts and peer_vocab_from_cache:
  1012|             seeded_peer_vocab = sorted({
  1013|                 str(entry.get("semantic_group", "")).strip()
  1014|                 for entry in cache_groups.get(d, {}).values()
  1015|                 if isinstance(entry, dict)
  1016|                 and str(entry.get("semantic_group", "")).strip()
  1017|                 and str(entry.get("semantic_group", "")).strip() != "__parse_error__"
  1018|             })
  1019|         previously_exported = export_progress.get(d, set()) if export_prompts and peer_vocab_from_cache else set()
  1020| 
  1021|         for row in pattern_rows:
  1022|             pattern_id = row["pattern_id"]
  1023|             pattern_label_human = row["pattern_label_human"]
  1024|             if not force_refresh and pattern_id in cache_groups[d]:
  1025|                 continue
  1026|             if not force_refresh and pattern_id in previously_exported:
  1027|                 continue
  1028|             if max_patterns is not None and processed >= max_patterns:
  1029|                 break
  1030| 
  1031|             record_pk = pattern_to_record.get(pattern_id, "")
  1032|             identity_items = identity_by_record.get(record_pk, {}) if record_pk else {}
  1033|             behavioral_props = _extract_behavioral_props(d, identity_items)
  1034|             element_label = _derive_element_label(d, identity_items, pattern_label_human)
  1035|             peer_group_labels = sorted({g for g in assigned_this_run if g} | set(seeded_peer_vocab))
  1036| 
  1037|             if dry_run:
  1038|                 print("\n--- semantic grouping prompt (dry-run) ---")
  1039|                 print(json.dumps({
  1040|                     "domain": d,
  1041|                     "pattern_id": pattern_id,
  1042|                     "pattern_label_human": pattern_label_human,
  1043|                     "element_label": element_label,
  1044|                     "behavioral_props": behavioral_props,
  1045|                     "peer_group_labels": peer_group_labels,
  1046|                 }, indent=2, ensure_ascii=False))
  1047|                 response_payload = {
  1048|                     "semantic_group": "__dry_run__",
  1049|                     "confidence": "low",
  1050|                     "rationale": "Dry run; LLM call skipped.",
  1051|                 }
  1052|             elif export_prompts:
  1053|                 prompt = build_grouping_prompt(
  1054|                     domain=d,
  1055|                     pattern_label_human=pattern_label_human,
  1056|                     behavioral_props=behavioral_props,
  1057|                     peer_group_labels=peer_group_labels,
  1058|                 )
  1059|                 exported_prompts.append({
  1060|                     "pattern_id": pattern_id,
  1061|                     "domain": d,
  1062|                     "pattern_label_human": pattern_label_human,
  1063|                     "element_label": element_label,
  1064|                     "system_prompt": SYSTEM_PROMPT,
  1065|                     "user_prompt": prompt,
  1066|                 })
  1067|                 if peer_vocab_from_cache:
  1068|                     export_progress.setdefault(d, set()).add(pattern_id)
  1069|                 response_payload = {
  1070|                     "semantic_group": "__exported__",
  1071|                     "confidence": "low",
  1072|                     "rationale": "Prompt exported; LLM call skipped.",
  1073|                 }
  1074|             else:
  1075|                 try:
  1076|                     prompt = build_grouping_prompt(
  1077|                         domain=d,
  1078|                         pattern_label_human=pattern_label_human,
  1079|                         behavioral_props=behavioral_props,
  1080|                         peer_group_labels=peer_group_labels,
  1081|                     )
  1082|                     raw_response = _call_grouping_llm(prompt)
  1083|                     response_payload = _parse_grouping_response(raw_response)
  1084|                 except NotImplementedError as e:
  1085|                     response_payload = {
  1086|                         "semantic_group": "__parse_error__",
  1087|                         "confidence": "low",
  1088|                         "rationale": str(e),
  1089|                     }
  1090| 
  1091|             cache_groups[d][pattern_id] = {
  1092|                 "semantic_group": response_payload["semantic_group"],
  1093|                 "confidence": response_payload["confidence"],
  1094|                 "rationale": response_payload["rationale"],
  1095|                 "pattern_label_human": pattern_label_human,
  1096|                 "reviewed": False,
  1097|             }
  1098|             group_value = response_payload["semantic_group"]
  1099|             if group_value and group_value not in {"__parse_error__", "__exported__", "__dry_run__"}:
  1100|                 assigned_this_run.append(group_value)
  1101|             processed += 1
  1102| 
  1103|         print(f"[build_semantic_groups] domain={d} processed={processed}")
  1104| 
  1105|     if export_prompts:
  1106|         export_base_path = _resolve_export_target(cache_path, export_prompts)
  1107|         if export_prompts.parent != export_base_path.parent:
  1108|             print(
  1109|                 "[build_semantic_groups] NOTE: export output is written beside label_semantic_groups.json at "
  1110|                 f"{export_base_path.parent}"
  1111|             )
  1112|         written_paths = _write_export_batches(export_base_path, exported_prompts, export_batch_size)
  1113|         if peer_vocab_from_cache:
  1114|             _save_export_progress(export_progress_path, export_progress)
  1115|         print(
  1116|             f"[build_semantic_groups] Exported {len(exported_prompts)} prompts "
  1117|             f"into {len(written_paths)} file(s) under {export_base_path.parent}"
  1118|         )
  1119|         for path in written_paths:
  1120|             print(f"[build_semantic_groups]   - {path}")
  1121|         if peer_vocab_from_cache:
  1122|             print(
  1123|                 "[build_semantic_groups] Resume tracking enabled: updated exported pattern progress at "
  1124|                 f"{export_progress_path}"
  1125|             )
  1126|         print("[build_semantic_groups] Export mode: cache was not modified.")
  1127|         return
  1128| 
  1129|     cache["schema_version"] = CACHE_SCHEMA_VERSION
  1130|     cache["analysis_run_id"] = analysis_run_id
  1131|     cache["generated_at"] = _utc_now_iso()
  1132|     cache["groups"] = cache_groups
  1133|     _save_cache(cache_path, cache)
  1134|     print(f"[build_semantic_groups] wrote cache: {cache_path}")
  1135| 
  1136| 
  1137| def main() -> None:
  1138|     ap = argparse.ArgumentParser(description="Build semantic group labels for selected pattern domains.")
  1139|     ap.add_argument("--out-root", required=True, help="Path containing Results_v21/")
  1140|     ap.add_argument("--domain", choices=SEMANTIC_GROUPING_DOMAINS, default=None, help="Optional single domain.")
  1141|     ap.add_argument("--dry-run", action="store_true", help="Print prompt inputs; do not call LLM API.")
  1142|     ap.add_argument("--force-refresh", action="store_true", help="Regenerate groups even if cached.")
  1143|     ap.add_argument("--max-patterns", type=int, default=None, help="Limit patterns processed per domain.")
  1144|     ap.add_argument(
  1145|         "--export-prompts",
  1146|         default=None,
  1147|         help="Write assembled prompts to this JSON path instead of calling LLM and without writing cache.",
  1148|     )
  1149|     ap.add_argument(
  1150|         "--import-results",
  1151|         default=None,
  1152|         help="Import semantic-grouping results from a JSON array and write cache (no LLM calls).",
  1153|     )
  1154|     ap.add_argument(
  1155|         "--peer-vocab-from-cache",
  1156|         action="store_true",
  1157|         help=(
  1158|             "When used with --export-prompts, seed peer vocabulary from cached semantic_group labels and "
  1159|             "track exported pattern_ids to resume later batches without re-exporting prior items."
  1160|         ),
  1161|     )
  1162|     ap.add_argument(
  1163|         "--export-batch-size",
  1164|         type=int,
  1165|         default=None,
  1166|         help=(
  1167|             "When used with --export-prompts, split exported prompts into sequential JSON batches of this size "
  1168|             "in Results_v21/label_synthesis/."
  1169|         ),
  1170|     )
  1171|     args = ap.parse_args()
  1172| 
  1173|     if args.export_prompts and args.import_results:
  1174|         raise ValueError("--export-prompts and --import-results are mutually exclusive.")
  1175|     if args.peer_vocab_from_cache and not args.export_prompts:
  1176|         raise ValueError("--peer-vocab-from-cache can only be used with --export-prompts.")
  1177|     if args.dry_run and (args.export_prompts or args.import_results):
  1178|         raise ValueError("--dry-run cannot be combined with --export-prompts or --import-results.")
  1179|     if args.export_batch_size is not None and not args.export_prompts:
  1180|         raise ValueError("--export-batch-size can only be used with --export-prompts.")
  1181| 
  1182|     build_semantic_groups(
  1183|         out_root=Path(args.out_root).resolve(),
  1184|         domain=args.domain,
  1185|         dry_run=bool(args.dry_run),
  1186|         force_refresh=bool(args.force_refresh),
  1187|         max_patterns=args.max_patterns,
  1188|         export_prompts=Path(args.export_prompts).resolve() if args.export_prompts else None,
  1189|         import_results=Path(args.import_results).resolve() if args.import_results else None,
  1190|         peer_vocab_from_cache=bool(args.peer_vocab_from_cache),
  1191|         export_batch_size=args.export_batch_size,
  1192|     )
  1193| 
  1194| 
  1195| if __name__ == "__main__":
  1196|     main()
```
