# Chunk of tools/extractor.py

- Source relative path: `tools/extractor.py`
- Chunk: 2 of 4
- Original line range: 515-982
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _load_semantic_groups, _derive_unit_system, _process_one_domain
- Source SHA-256: d75cdfbab8fb9d4bbc3c46c3611b1bcf54844b8f5421954d15f78ef298ab109e
- Starts inside symbol: no
- Ends inside symbol: no

```
   515| def _load_semantic_groups(results_v21_dir: Optional[Path]) -> Dict[str, Dict[str, str]]:
   516|     if results_v21_dir is None:
   517|         return {}
   518|     cache_path = results_v21_dir / "label_synthesis" / "label_semantic_groups.json"
   519|     if not cache_path.is_file():
   520|         return {}
   521|     try:
   522|         with cache_path.open("r", encoding="utf-8") as f:
   523|             payload = json.load(f)
   524|     except Exception:
   525|         return {}
   526|     if not isinstance(payload, dict):
   527|         return {}
   528|     groups = payload.get("groups")
   529|     if not isinstance(groups, dict):
   530|         return {}
   531| 
   532|     out: Dict[str, Dict[str, str]] = {}
   533|     for domain, by_pattern in groups.items():
   534|         if not isinstance(domain, str) or not isinstance(by_pattern, dict):
   535|             continue
   536|         out[domain] = {}
   537|         for pattern_id, entry in by_pattern.items():
   538|             if not isinstance(pattern_id, str):
   539|                 continue
   540|             semantic_group = ""
   541|             if isinstance(entry, dict):
   542|                 semantic_group = _safe_str(entry.get("semantic_group"))
   543|             elif isinstance(entry, str):
   544|                 semantic_group = entry
   545|             out[domain][pattern_id] = semantic_group
   546|     return out
   547| 
   548| 
   549| def _derive_unit_system(payload: Dict[str, Any], export_run_id: str) -> str:
   550|     units_block = payload.get("units")
   551|     if not isinstance(units_block, dict):
   552|         return ""
   553|     records = units_block.get("records")
   554|     if not isinstance(records, list):
   555|         return ""
   556| 
   557|     length_spec_found = False
   558|     saw_length_unit_type_id = False
   559|     for rec in records:
   560|         if not isinstance(rec, dict):
   561|             continue
   562|         # runner/run_dynamo.py's _canonicalize_all_domain_records() strips identity_basis
   563|         # from every domain's records and replaces it with a flat top-level `items` list
   564|         # (core/canonical_items.py:canonicalize_record) -- that is the shape every current
   565|         # export has. Fall back to identity_basis.items for any older archived exports that
   566|         # predate that canonicalization pass.
   567|         items = rec.get("items")
   568|         if not isinstance(items, list):
   569|             identity_basis = rec.get("identity_basis")
   570|             items = identity_basis.get("items") if isinstance(identity_basis, dict) else None
   571|         if not isinstance(items, list):
   572|             continue
   573| 
   574|         is_length = any(
   575|             isinstance(it, dict)
   576|             and _safe_str(it.get("k")).strip() == "units.spec"
   577|             and _safe_str(it.get("v")).strip().lower() == "length"
   578|             for it in items
   579|         )
   580|         if not is_length:
   581|             continue
   582| 
   583|         # Accept usable records even when optional fields degrade status
   584|         rec_status = _safe_str(rec.get("status")).strip().lower()
   585|         if rec_status not in ("ok", "degraded"):
   586|             continue
   587| 
   588|         length_spec_found = True
   589|         unit_type_id = ""
   590|         for it in items:
   591|             if isinstance(it, dict) and _safe_str(it.get("k")).strip() == "units.unit_type_id":
   592|                 unit_type_id = _safe_str(it.get("v")).strip()
   593|                 break
   594| 
   595|         if not unit_type_id:
   596|             continue
   597| 
   598|         saw_length_unit_type_id = True
   599|         unit_type_id_l = unit_type_id.lower()
   600|         if "feet" in unit_type_id_l or "foot" in unit_type_id_l or "fractional" in unit_type_id_l or "inch" in unit_type_id_l:
   601|             return "Imperial"
   602|         if "millimeter" in unit_type_id_l:
   603|             return "Metric"
   604|         tokens = re.split(r"[^a-z0-9]+", unit_type_id_l)
   605|         if "meter" in tokens or "meters" in tokens or "metre" in tokens or "metres" in tokens or "centimeter" in tokens or "centimeters" in tokens:
   606|             return "Metric"
   607| 
   608|     if not length_spec_found:
   609|         sys.stderr.write(f"[WARN flatten] unit_system: no length spec found for {export_run_id}\n")
   610|         return ""
   611|     if saw_length_unit_type_id:
   612|         sys.stderr.write(
   613|             f"[WARN flatten] unit_system: no recognized unit_type_id found in length records for {export_run_id}\n"
   614|         )
   615|     return ""
   616| 
   617| def _process_one_domain(
   618|     dom: str,
   619|     cluster_items: List,
   620|     domain_records: List[Dict],
   621|     exports: List[str],
   622|     files_total: int,
   623|     analysis_run_id: str,
   624|     phase0_dir: Optional[Path],
   625|     results_v21_dir: Optional[Path],
   626|     label_synth_dir: Optional[Path],
   627| ) -> Dict[str, List]:
   628|     import time
   629|     print(f"[extractor] domain={dom} (start)", flush=True)
   630|     _t_dom_start = time.perf_counter()
   631|     _active_pks: Set[str] = {
   632|         r["record_pk"]
   633|         for r in domain_records
   634|         if r.get("join_hash", "") and r.get("record_pk", "")
   635|     }
   636|     _t_ii = time.perf_counter()
   637|     identity_items_by_record = _load_identity_items_by_record(phase0_dir, dom, allowed_record_pks=_active_pks)
   638|     _t_ii = time.perf_counter() - _t_ii
   639|     domain_files_present = len({r["export_run_id"] for r in domain_records})
   640|     _t_lr = time.perf_counter()
   641|     label_population_by_hash, annotations, llm_cache = _load_label_resolution_inputs(results_v21_dir, dom, label_synth_dir=label_synth_dir)
   642|     _t_lr = time.perf_counter() - _t_lr
   643|     semantic_groups_for_dom: Dict[str, str] = _load_semantic_groups(results_v21_dir).get(dom, {})
   644|     pattern_ids_taken: set = set()
   645|     cluster_rows: List[Dict[str, Any]] = []
   646|     pattern_id_by_cluster: Dict[Tuple[str, str, str], str] = {}
   647|     _t_cl = time.perf_counter()
   648|     for (_, schema, join_hash), rows in sorted(cluster_items, key=lambda kv: (kv[0][1], kv[0][2])):
   649|         pid = _stable_pattern_id(dom, schema, join_hash, pattern_ids_taken)
   650|         files_present = len({r["export_run_id"] for r in rows})
   651|         cluster_rows.append({
   652|             "schema": schema,
   653|             "join_hash": join_hash,
   654|             "rows": rows,
   655|             "pid": pid,
   656|             "files_present": files_present,
   657|             "records_count": len(rows),
   658|             "identity_items": identity_items_by_record.get(rows[0].get("record_pk", ""), []),
   659|         })
   660|         pattern_id_by_cluster[(dom, schema, join_hash)] = pid
   661|     _t_cl = time.perf_counter() - _t_cl
   662| 
   663|     domain_patterns_local: List[Dict[str, str]] = []
   664|     authority_rows_local: List[Dict[str, str]] = []
   665|     presence_rows_local: List[Dict[str, str]] = []
   666|     file_domain_rows_local: List[Dict[str, str]] = []
   667|     rec_membership_local: List[Dict[str, str]] = []
   668|     diag_rows_local: List[Dict[str, str]] = []
   669|     domain_metrics_local: List[Dict[str, str]] = []
   670| 
   671|     sorted_clusters = sorted(
   672|         cluster_rows,
   673|         key=lambda c: (-c["files_present"], -c["records_count"], c["pid"]),
   674|     )
   675|     n = len(sorted_clusters)
   676|     total_dom_records = sum(int(c["records_count"]) for c in sorted_clusters)
   677|     files_present_sum = sum(int(c["files_present"]) for c in sorted_clusters)
   678|     dominant_files_by_pattern: Dict[str, int] = defaultdict(int)
   679|     dominant_files_with_valid_pattern = 0
   680|     files_with_tied_dominant = 0
   681|     near_dup_merge_map = find_near_duplicate_merges(sorted_clusters)
   682|     resolved_labels: Dict[str, Tuple[str, str]] = {}
   683| 
   684|     for rank, cluster in enumerate(sorted_clusters, start=1):
   685|         schema = str(cluster["schema"])
   686|         join_hash = str(cluster["join_hash"])
   687|         rows = list(cluster["rows"])
   688|         files_present = int(cluster["files_present"])
   689|         cluster_id = f"{dom}|{schema}|{join_hash}"
   690|         presence_pct = (files_present / files_total) if files_total else 0.0
   691|         coverage_pct = (len(rows) / total_dom_records) if total_dom_records else 0.0
   692|         cluster_size = len(rows)
   693|         domain_metrics_local.append({
   694|             "schema_version": SCHEMA_VERSION,
   695|             "analysis_run_id": analysis_run_id,
   696|             "domain": dom,
   697|             "group_type": "CORPUS",
   698|             "group_id": "CORPUS",
   699|             "join_key_schema": schema,
   700|             "join_hash": join_hash,
   701|             "cluster_id": cluster_id,
   702|             "cluster_size": str(cluster_size),
   703|             "files_present": str(files_present),
   704|             "files_total": str(files_total),
   705|             "presence_pct": f"{presence_pct:.6f}",
   706|             "coverage_pct": f"{coverage_pct:.6f}",
   707|             "collision_pct": "0.000000",
   708|             "stability_pct": f"{presence_pct:.6f}",
   709|         })
   710| 
   711|         pid = str(cluster["pid"])
   712|         generic_label = f"{schema} — Variant {rank} of {n}"
   713|         near_dup_target_label: Optional[str] = None
   714|         near_dup_target_hash = near_dup_merge_map.get(join_hash)
   715|         if near_dup_target_hash:
   716|             near_dup_target_label = resolved_labels.get(near_dup_target_hash, ("", ""))[0] or None
   717|         resolved_label, resolved_source = resolve_pattern_label(
   718|             domain=dom,
   719|             join_hash=join_hash,
   720|             join_key_schema=schema,
   721|             pattern_rank=rank,
   722|             pattern_count=n,
   723|             identity_items=cluster.get("identity_items") or [],
   724|             label_population=label_population_by_hash.get(join_hash) or [],
   725|             annotations=annotations,
   726|             llm_cache=llm_cache,
   727|             pattern_id=pid,
   728|             near_dup_target_label=near_dup_target_label,
   729|         )
   730|         resolved_labels[join_hash] = (resolved_label, resolved_source)
   731|         domain_patterns_local.append({
   732|             "schema_version": SCHEMA_VERSION,
   733|             "analysis_run_id": analysis_run_id,
   734|             "domain": dom,
   735|             "pattern_id": pid,
   736|             # Back-compat: keep legacy generic label in pattern_label so existing
   737|             # Power BI transforms that parse "Variant X of N" continue to work.
   738|             "pattern_label": generic_label,
   739|             "pattern_label_human": resolved_label,
   740|             "pattern_label_source": resolved_source,
   741|             "pattern_label_fallback": generic_label,
   742|             "source_cluster_id": cluster_id,
   743|             "pattern_size_records": str(cluster_size),
   744|             "pattern_size_files": str(files_present),
   745|             "pattern_rank": str(rank),
   746|             "is_candidate_standard": "true" if presence_pct >= STANDARD_PRESENCE_MIN else "false",
   747|             "notes": "",
   748|             "is_cad_import": (
   749|                 "true"
   750|                 if (
   751|                     dom == "view_category_overrides"
   752|                     and (
   753|                         ".dwg" in resolved_label.lower()
   754|                         or resolved_label.lower().startswith("imports in families|")
   755|                     )
   756|                 )
   757|                 else "false"
   758|             ),
   759|             "semantic_group": semantic_groups_for_dom.get(pid, ""),
   760|         })
   761| 
   762|         for r in rows:
   763|             rec_membership_local.append({
   764|                 "schema_version": SCHEMA_VERSION,
   765|                 "analysis_run_id": analysis_run_id,
   766|                 "export_run_id": r["export_run_id"],
   767|                 "domain": dom,
   768|                 "record_pk": r["record_pk"],
   769|                 "pattern_id": pid,
   770|                 "membership_confidence": "1.000000",
   771|                 "membership_reason_code": "join_hash_exact",
   772|             })
   773| 
   774|         shares = [int(c["records_count"]) / total_dom_records for c in sorted_clusters] if total_dom_records else []
   775|         legacy_hhi = compute_hhi_from_shares(shares)
   776|         hhi = legacy_hhi if legacy_hhi is not None else 0.0
   777|         eff = compute_effective_clusters(legacy_hhi) or 0.0
   778|         authority_rows_local.append({
   779|             "schema_version": SCHEMA_VERSION,
   780|             "analysis_run_id": analysis_run_id,
   781|             "domain": dom,
   782|             "pattern_id": pid,
   783|             "join_key_schema": schema,
   784|             "files_present": str(files_present),
   785|             "files_total": str(files_total),
   786|             "presence_pct": f"{presence_pct:.6f}",
   787|             "hhi": f"{hhi:.6f}",
   788|             "effective_cluster_count": f"{eff:.6f}",
   789|             "authority_score": f"{presence_pct:.6f}",
   790|             "confidence_tier": "high" if presence_pct >= STANDARD_PRESENCE_MIN else "medium",
   791|         })
   792| 
   793|     domain_pattern_presence_pct: Dict[str, float] = {
   794|         r["pattern_id"]: float(r["presence_pct"])
   795|         for r in authority_rows_local
   796|         if r.get("domain") == dom and r.get("pattern_id")
   797|     }
   798|     _records_by_eid: Dict[str, List[Dict[str, str]]] = defaultdict(list)
   799|     for _r in domain_records:
   800|         _records_by_eid[_r["export_run_id"]].append(_r)
   801| 
   802|     _t_file_loop = time.perf_counter()
   803|     for export_run_id in exports:
   804|         dom_records = _records_by_eid.get(export_run_id, [])
   805|         total = len(dom_records)
   806|         per_pat: Dict[str, int] = defaultdict(int)
   807|         unknown = 0
   808|         for r in dom_records:
   809|             jh = r.get("join_hash", "")
   810|             if not jh:
   811|                 unknown += 1
   812|                 continue
   813|             schema = r.get("join_key_schema", "")
   814|             pid = pattern_id_by_cluster.get((dom, schema, jh))
   815|             if not pid:
   816|                 unknown += 1
   817|                 continue
   818|             per_pat[pid] += 1
   819|         dominant_pid = ""
   820|         dominant_share = 0.0
   821|         if per_pat and total > 0:
   822|             ranked = sorted(per_pat.items(), key=lambda kv: (-kv[1], kv[0]))
   823|             dominant_count = ranked[0][1]
   824|             dominant_ties = [pid for pid, cnt in ranked if cnt == dominant_count]
   825|             dominant_share = dominant_count / total
   826|             if len(dominant_ties) == 1:
   827|                 dominant_pid = dominant_ties[0]
   828|                 dominant_files_by_pattern[dominant_pid] += 1
   829|                 dominant_files_with_valid_pattern += 1
   830|             else:
   831|                 files_with_tied_dominant += 1
   832|         shares_file_records = [cnt / total for cnt in per_pat.values()] if total > 0 else []
   833|         if total > 0 and unknown > 0:
   834|             shares_file_records.append(unknown / total)
   835|         hhi_file_records = compute_hhi_from_shares(shares_file_records) if total > 0 else None
   836|         eff_clusters_file_records = compute_effective_clusters(hhi_file_records)
   837|         file_domain_rows_local.append({
   838|             "schema_version": SCHEMA_VERSION,
   839|             "analysis_run_id": analysis_run_id,
   840|             "export_run_id": export_run_id,
   841|             "domain": dom,
   842|             "hhi_file_records": _fmt_metric(hhi_file_records),
   843|             "eff_clusters_file_records": _fmt_metric(eff_clusters_file_records),
   844|         })
   845|         for pid, cnt in sorted(per_pat.items()):
   846|             share = cnt / total if total else 0.0
   847|             presence_rows_local.append({
   848|                 "schema_version": SCHEMA_VERSION,
   849|                 "analysis_run_id": analysis_run_id,
   850|                 "export_run_id": export_run_id,
   851|                 "domain": dom,
   852|                 "pattern_id": pid,
   853|                 "pattern_share_pct": f"{share:.6f}",
   854|                 "is_dominant_pattern": "true" if pid == dominant_pid else "false",
   855|                 "deviation_score": f"{max(0.0, dominant_share - share):.6f}",
   856|                 "corpus_classification": (
   857|                     "CORPUS_STANDARD"
   858|                     if domain_pattern_presence_pct.get(pid, 0.0) >= STANDARD_PRESENCE_MIN
   859|                     else "CORPUS_VARIANT"
   860|                 ),
   861|             })
   862|         if unknown > 0:
   863|             presence_rows_local.append({
   864|                 "schema_version": SCHEMA_VERSION,
   865|                 "analysis_run_id": analysis_run_id,
   866|                 "export_run_id": export_run_id,
   867|                 "domain": dom,
   868|                 "pattern_id": "",
   869|                 "pattern_share_pct": f"{(unknown / total) if total else 0.0:.6f}",
   870|                 "is_dominant_pattern": "false",
   871|                 "deviation_score": "0.000000",
   872|                 "corpus_classification": "UNKNOWN",
   873|             })
   874|     _t_file_loop = time.perf_counter() - _t_file_loop
   875| 
   876|     known_domain_records = sum(int(c["records_count"]) for c in sorted_clusters)
   877|     unknown_domain = max(0, len(domain_records) - known_domain_records)
   878|     total_domain = len(domain_records)
   879|     shares = [int(c["records_count"]) / total_domain for c in sorted_clusters] if total_domain else []
   880|     dominant = max(shares) if shares else 0.0
   881|     entropy = -sum((s * (0.0 if s <= 0 else __import__('math').log(s, 2))) for s in shares) if shares else 0.0
   882|     hhi_domain_presence = compute_hhi_from_shares(
   883|         [int(c["files_present"]) / files_present_sum for c in sorted_clusters]
   884|     ) if files_present_sum > 0 else None
   885|     eff_clusters_domain_presence = compute_effective_clusters(hhi_domain_presence)
   886|     hhi_domain_dominance = compute_hhi_from_shares(
   887|         [cnt / dominant_files_with_valid_pattern for cnt in dominant_files_by_pattern.values()]
   888|     ) if dominant_files_with_valid_pattern > 0 else None
   889|     eff_clusters_domain_dominance = compute_effective_clusters(hhi_domain_dominance)
   890|     shares_domain_records = [int(c["records_count"]) / total_domain for c in sorted_clusters] if total_domain > 0 else []
   891|     if total_domain > 0 and unknown_domain > 0:
   892|         shares_domain_records.append(unknown_domain / total_domain)
   893|     hhi_domain_records = compute_hhi_from_shares(shares_domain_records) if total_domain > 0 else None
   894|     eff_clusters_domain_records = compute_effective_clusters(hhi_domain_records)
   895|     files_excluded_from_dominance = files_total - dominant_files_with_valid_pattern
   896|     unknown_rate = (unknown_domain / total_domain) if total_domain else 0.0
   897|     rec_grain = "DOMAIN_OK"
   898|     if total_domain < MIN_RECORDS_FOR_DOMAIN or domain_files_present < MIN_FILES_FOR_DOMAIN:
   899|         rec_grain = "INSUFFICIENT_EVIDENCE"
   900|     elif unknown_rate > UNKNOWN_RATE_MAX:
   901|         rec_grain = "KEY_REVISION_REQUIRED"
   902|     elif dominant < DOMINANT_SHARE_MIN:
   903|         rec_grain = "PATTERN_REQUIRED"
   904|     mixture_flag = dominant < DOMINANT_SHARE_MIN
   905|     governance_state = "unknown"
   906|     if dom in ROW_KEY_DOMAINS:
   907|         governance_state = "element_grain"
   908|     elif rec_grain == "INSUFFICIENT_EVIDENCE":
   909|         governance_state = "insufficient_evidence"
   910|     elif rec_grain == "KEY_REVISION_REQUIRED":
   911|         governance_state = "key_revision_required"
   912|     elif files_with_tied_dominant == domain_files_present and dominant_files_with_valid_pattern == 0:
   913|         governance_state = "multi_part_standard"
   914|     elif not mixture_flag and len(sorted_clusters) >= 1:
   915|         governance_state = "single_standard"
   916|     elif mixture_flag:
   917|         governance_state = "mixture"
   918|     diag_rows_local.append({
   919|         "schema_version": SCHEMA_VERSION,
   920|         "analysis_run_id": analysis_run_id,
   921|         "domain": dom,
   922|         "pattern_count": str(len(sorted_clusters)),
   923|         "dominant_pattern_share_pct": f"{dominant:.6f}",
   924|         "entropy_index": f"{entropy:.6f}",
   925|         "mixture_flag": "true" if mixture_flag else "false",
   926|         "unknown_rate_pct": f"{unknown_rate:.6f}",
   927|         "recommended_analysis_grain": rec_grain,
   928|         "hhi_domain_presence": _fmt_metric(hhi_domain_presence),
   929|         "eff_clusters_domain_presence": _fmt_metric(eff_clusters_domain_presence),
   930|         "hhi_domain_dominance": _fmt_metric(hhi_domain_dominance),
   931|         "eff_clusters_domain_dominance": _fmt_metric(eff_clusters_domain_dominance),
   932|         "hhi_domain_records": _fmt_metric(hhi_domain_records),
   933|         "eff_clusters_domain_records": _fmt_metric(eff_clusters_domain_records),
   934|         "files_total": str(files_total),
   935|         "files_with_unique_dominant": str(dominant_files_with_valid_pattern),
   936|         "files_with_tied_dominant": str(files_with_tied_dominant),
   937|         "files_excluded_from_dominance": str(files_excluded_from_dominance),
   938|         "pct_files_unique_dominant": f"{(dominant_files_with_valid_pattern / files_total) if files_total else 0.0:.6f}",
   939|         "governance_state": governance_state,
   940|     })
   941|     _t_sort = time.perf_counter()
   942|     domain_patterns_local = _sort_rows(domain_patterns_local, ["analysis_run_id", "domain", "pattern_id"])
   943|     presence_rows_local = _sort_rows(presence_rows_local, ["analysis_run_id", "export_run_id", "domain", "pattern_id"])
   944|     authority_rows_local = _sort_rows(authority_rows_local, ["analysis_run_id", "domain", "pattern_id"])
   945|     rec_membership_local = _sort_rows(rec_membership_local, ["analysis_run_id", "export_run_id", "domain", "record_pk"])
   946|     file_domain_rows_local = _sort_rows(file_domain_rows_local, ["analysis_run_id", "export_run_id", "domain"])
   947|     diag_rows_local = _sort_rows(diag_rows_local, ["analysis_run_id", "domain"])
   948|     _t_sort = time.perf_counter() - _t_sort
   949|     _t_csv = 0.0
   950| 
   951|     print(
   952|         f"[extractor] domain={dom} (done) clusters={len(sorted_clusters)} records={len(domain_records)}",
   953|         flush=True,
   954|     )
   955|     _t_dom_elapsed = time.perf_counter() - _t_dom_start
   956|     _t_residual = max(0.0, _t_dom_elapsed - _t_ii - _t_lr - _t_cl - _t_file_loop - _t_sort - _t_csv)
   957|     return {
   958|         "domain_patterns": domain_patterns_local,
   959|         "authority_rows": authority_rows_local,
   960|         "presence_rows": presence_rows_local,
   961|         "file_domain_rows": file_domain_rows_local,
   962|         "rec_membership": rec_membership_local,
   963|         "diag_rows": diag_rows_local,
   964|         "domain_metrics": domain_metrics_local,
   965|         "pattern_id_by_cluster": {k: v for k, v in pattern_id_by_cluster.items() if k[0] == dom},
   966|         "timing": {
   967|             "domain": dom,
   968|             "total": _t_dom_elapsed,
   969|             "identity_items": _t_ii,
   970|             "label_inputs": _t_lr,
   971|             "cluster_loop": _t_cl,
   972|             "file_loop": _t_file_loop,
   973|             "sort": _t_sort,
   974|             "csv_write": _t_csv,
   975|             "residual": _t_residual,
   976|             "n_records": len(domain_records),
   977|             "n_clusters": len(cluster_items),
   978|             "n_files": len([e for e in exports if _records_by_eid.get(e)]),
   979|         },
   980|     }
   981| 
   982| 
```
