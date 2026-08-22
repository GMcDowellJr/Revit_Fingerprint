# Chunk of tools/extractor.py

- Source relative path: `tools/extractor.py`
- Chunk: 4 of 4
- Original line range: 1274-1515
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: emit_analysis, emit_analysis._merge_result
- Source SHA-256: d75cdfbab8fb9d4bbc3c46c3611b1bcf54844b8f5421954d15f78ef298ab109e
- Starts inside symbol: no
- Ends inside symbol: no

```
  1274| def emit_analysis(
  1275|     meta_rows: List[Dict[str, str]],
  1276|     records: List[Dict[str, str]],
  1277|     out_dir: Path,
  1278|     *,
  1279|     phase0_dir: Optional[Path] = None,
  1280|     results_v21_dir: Optional[Path] = None,
  1281|     label_synth_dir: Optional[Path] = None,
  1282|     workers: int = 1,
  1283| ) -> str:
  1284|     exports = sorted({r["export_run_id"] for r in meta_rows})
  1285|     domains = sorted({r["domain"] for r in records if r.get("domain", "") not in SUPPRESSED_ANALYSIS_DOMAINS})
  1286|     executed_utc = _utc_now_iso()
  1287|     scope_src = "|".join(exports)
  1288|     analysis_scope_hash = hashlib.sha1(scope_src.encode("utf-8")).hexdigest()
  1289|     analysis_run_id = f"ana_{analysis_scope_hash[:12]}"
  1290| 
  1291|     _write_csv(out_dir / "corpus_manifest.csv", [
  1292|         "schema_version", "analysis_run_id", "analysis_scope_hash", "export_run_count", "domain_count",
  1293|         "tool_version", "policy_baseline_version", "policy_pareto_version",
  1294|         "join_key_policy_version", "pattern_promotion_policy_version", "authority_metric_version", "executed_utc",
  1295|         "is_incremental_update", "notes",
  1296|     ], [{
  1297|         "schema_version": SCHEMA_VERSION,
  1298|         "analysis_run_id": analysis_run_id,
  1299|         "analysis_scope_hash": analysis_scope_hash,
  1300|         "export_run_count": str(len(exports)),
  1301|         "domain_count": str(len(domains)),
  1302|         "tool_version": _get_tool_version(),
  1303|         "policy_baseline_version": "0.0.0",
  1304|         "policy_pareto_version": "0.0.0",
  1305|         "join_key_policy_version": "0.0.0",
  1306|         "pattern_promotion_policy_version": "0.0.0",
  1307|         "authority_metric_version": "0.0.0",
  1308|         "executed_utc": executed_utc,
  1309|         "is_incremental_update": "0",
  1310|         "notes": (
  1311|             "defaults: STANDARD_PRESENCE_MIN=0.75; DOMINANT_SHARE_MIN=0.50; "
  1312|             "MIN_RECORDS_FOR_DOMAIN=50; MIN_FILES_FOR_DOMAIN=3; UNKNOWN_RATE_MAX=0.20"
  1313|         ),
  1314|     }])
  1315| 
  1316|     membership_rows = [{
  1317|         "schema_version": SCHEMA_VERSION,
  1318|         "analysis_run_id": analysis_run_id,
  1319|         "export_run_id": ex,
  1320|         "membership_role": "included",
  1321|     } for ex in exports]
  1322|     _write_csv(out_dir / "export_membership.csv", [
  1323|         "schema_version", "analysis_run_id", "export_run_id", "membership_role",
  1324|     ], _sort_rows(membership_rows, ["analysis_run_id", "export_run_id"]))
  1325| 
  1326|     files_total = len(exports)
  1327|     by_dom_cluster: Dict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
  1328|     for r in records:
  1329|         if r.get("domain", "") in SUPPRESSED_ANALYSIS_DOMAINS:
  1330|             continue
  1331|         jh = r.get("join_hash", "")
  1332|         if not jh:
  1333|             continue
  1334|         key = (r["domain"], r.get("join_key_schema", ""), jh)
  1335|         by_dom_cluster[key].append(r)
  1336| 
  1337|     domain_metrics: List[Dict[str, str]] = []
  1338|     domain_patterns: List[Dict[str, str]] = []
  1339|     rec_membership: List[Dict[str, str]] = []
  1340|     authority_rows: List[Dict[str, str]] = []
  1341|     presence_rows: List[Dict[str, str]] = []
  1342|     diag_rows: List[Dict[str, str]] = []
  1343|     file_domain_rows: List[Dict[str, str]] = []
  1344| 
  1345|     dom_clusters: Dict[str, List[Tuple[Tuple[str, str, str], List[Dict[str, str]]]]] = defaultdict(list)
  1346|     for k, v in by_dom_cluster.items():
  1347|         dom_clusters[k[0]].append((k, v))
  1348| 
  1349|     records_by_domain: Dict[str, List[Dict[str, str]]] = defaultdict(list)
  1350|     for r in records:
  1351|         if r.get("domain", "") in SUPPRESSED_ANALYSIS_DOMAINS:
  1352|             continue
  1353|         records_by_domain[r["domain"]].append(r)
  1354|     _pattern_id_by_cluster: Dict[Tuple[str, str, str], str] = {}
  1355|     _t_emit_analysis_start = time.perf_counter()
  1356|     _domain_timings: List[Dict] = []
  1357| 
  1358|     def _merge_result(result: Dict) -> None:
  1359|         domain_patterns.extend(result["domain_patterns"])
  1360|         authority_rows.extend(result["authority_rows"])
  1361|         presence_rows.extend(result["presence_rows"])
  1362|         file_domain_rows.extend(result["file_domain_rows"])
  1363|         rec_membership.extend(result["rec_membership"])
  1364|         diag_rows.extend(result["diag_rows"])
  1365|         domain_metrics.extend(result["domain_metrics"])
  1366|         _pattern_id_by_cluster.update(result["pattern_id_by_cluster"])
  1367|         _domain_timings.append(result["timing"])
  1368| 
  1369|     _domain_failures: List[Tuple[str, BaseException]] = []
  1370| 
  1371|     if workers == 1:
  1372|         # Run inline — avoids ProcessPoolExecutor entirely so emit_analysis is safe
  1373|         # to call from notebooks, python -c, stdin, or any non-importable __main__.
  1374|         for dom in domains:
  1375|             if not dom:
  1376|                 continue
  1377|             try:
  1378|                 result = _process_one_domain(
  1379|                     dom,
  1380|                     dom_clusters.get(dom, []),
  1381|                     records_by_domain.get(dom, []),
  1382|                     exports,
  1383|                     files_total,
  1384|                     analysis_run_id,
  1385|                     phase0_dir,
  1386|                     results_v21_dir,
  1387|                     label_synth_dir,
  1388|                 )
  1389|             except Exception as exc:
  1390|                 print(f"[extractor] domain={dom} failed: {exc}", flush=True)
  1391|                 _domain_failures.append((dom, exc))
  1392|                 continue
  1393|             _merge_result(result)
  1394|             print(f"[extractor] domain={dom} complete", flush=True)
  1395|     else:
  1396|         pool_size = max(1, min(workers, len([d for d in domains if d])))
  1397|         with ProcessPoolExecutor(max_workers=pool_size) as executor:
  1398|             future_to_dom = {
  1399|                 executor.submit(
  1400|                     _process_one_domain,
  1401|                     dom,
  1402|                     dom_clusters.get(dom, []),
  1403|                     records_by_domain.get(dom, []),
  1404|                     exports,
  1405|                     files_total,
  1406|                     analysis_run_id,
  1407|                     phase0_dir,
  1408|                     results_v21_dir,
  1409|                     label_synth_dir,
  1410|                 ): dom
  1411|                 for dom in domains if dom
  1412|             }
  1413|             for future in as_completed(future_to_dom):
  1414|                 dom = future_to_dom[future]
  1415|                 try:
  1416|                     result = future.result()
  1417|                 except Exception as exc:
  1418|                     print(f"[extractor] domain={dom} failed: {exc}", flush=True)
  1419|                     _domain_failures.append((dom, exc))
  1420|                     continue
  1421|                 _merge_result(result)
  1422|                 print(f"[extractor] domain={dom} complete", flush=True)
  1423| 
  1424|     if _domain_failures:
  1425|         summary = "; ".join(f"{d}: {e}" for d, e in _domain_failures)
  1426|         raise RuntimeError(
  1427|             f"emit_analysis: {len(_domain_failures)} domain(s) failed — "
  1428|             f"analysis output is incomplete and has not been written: {summary}"
  1429|         )
  1430| 
  1431|     _domain_timings.sort(key=lambda x: -x["total"])
  1432|     _n_total = len(_domain_timings)
  1433|     for _rank, _dt in enumerate(_domain_timings[:5], 1):
  1434|         sys.stderr.write(
  1435|             f"[patterns_timing] domain={_dt['domain']} elapsed={_dt['total']:.2f}s "
  1436|             f"identity_items={_dt['identity_items']:.2f}s "
  1437|             f"label_inputs={_dt['label_inputs']:.2f}s "
  1438|             f"cluster_loop={_dt['cluster_loop']:.2f}s "
  1439|             f"file_loop={_dt['file_loop']:.2f}s "
  1440|             f"sort={_dt['sort']:.2f}s "
  1441|             f"csv_write={_dt['csv_write']:.2f}s "
  1442|             f"residual={_dt['residual']:.2f}s "
  1443|             f"n_records={_dt['n_records']} n_clusters={_dt['n_clusters']} "
  1444|             f"rank={_rank}/{_n_total}\n"
  1445|         )
  1446|     sys.stderr.flush()
  1447| 
  1448|     # Unknown join_hash rows still get membership rows with blank pattern_id.
  1449|     for r in records:
  1450|         if r.get("join_hash"):
  1451|             continue
  1452|         rec_membership.append({
  1453|             "schema_version": SCHEMA_VERSION,
  1454|             "analysis_run_id": analysis_run_id,
  1455|             "export_run_id": r["export_run_id"],
  1456|             "domain": r["domain"],
  1457|             "record_pk": r["record_pk"],
  1458|             "pattern_id": "",
  1459|             "membership_confidence": "0.000000",
  1460|             "membership_reason_code": "missing_join_hash",
  1461|         })
  1462| 
  1463|     _write_csv(out_dir / "authority_metrics.csv", [
  1464|         "schema_version", "analysis_run_id", "domain", "group_type", "group_id",
  1465|         "join_key_schema", "join_hash", "cluster_id", "cluster_size",
  1466|         "files_present", "files_total", "presence_pct", "coverage_pct", "collision_pct", "stability_pct",
  1467|     ], _sort_rows(domain_metrics, ["domain", "join_key_schema", "join_hash"]))
  1468| 
  1469|     _write_csv(out_dir / "domain_patterns.csv", [
  1470|         # Keep legacy first 11 columns in the original order for Power BI queries
  1471|         # that pin Csv.Document([Columns=11]) and/or type-steps against that shape.
  1472|         "schema_version", "analysis_run_id", "domain", "pattern_id", "pattern_label",
  1473|         "source_cluster_id", "pattern_size_records", "pattern_size_files", "pattern_rank",
  1474|         "is_candidate_standard", "notes",
  1475|         # v2.1 human-readable/audit extensions are appended for compatibility.
  1476|         "pattern_label_human", "pattern_label_source", "pattern_label_fallback", "is_cad_import",
  1477|         "semantic_group",
  1478|     ], _sort_rows(domain_patterns, ["analysis_run_id", "domain", "pattern_id"]))
  1479| 
  1480|     _write_csv(out_dir / "record_pattern_membership.csv", [
  1481|         "schema_version", "analysis_run_id", "export_run_id", "domain", "record_pk",
  1482|         "pattern_id", "membership_confidence", "membership_reason_code",
  1483|     ], _sort_rows(rec_membership, ["analysis_run_id", "export_run_id", "domain", "record_pk"]))
  1484| 
  1485|     _write_csv(out_dir / "authority_patterns.csv", [
  1486|         "schema_version", "analysis_run_id", "domain", "pattern_id", "join_key_schema",
  1487|         "files_present", "files_total", "presence_pct", "hhi", "effective_cluster_count",
  1488|         "authority_score", "confidence_tier",
  1489|     ], _sort_rows(authority_rows, ["analysis_run_id", "domain", "pattern_id"]))
  1490| 
  1491|     _write_csv(out_dir / "pattern_presence_file.csv", [
  1492|         "schema_version", "analysis_run_id", "export_run_id", "domain", "pattern_id",
  1493|         "pattern_share_pct", "is_dominant_pattern", "deviation_score", "corpus_classification",
  1494|     ], _sort_rows(presence_rows, ["analysis_run_id", "export_run_id", "domain", "pattern_id"]))
  1495| 
  1496|     _write_csv(out_dir / "file_domain_concentration.csv", [
  1497|         "schema_version", "analysis_run_id", "export_run_id", "domain",
  1498|         "hhi_file_records", "eff_clusters_file_records",
  1499|     ], _sort_rows(file_domain_rows, ["analysis_run_id", "export_run_id", "domain"]))
  1500| 
  1501|     _write_csv(out_dir / "pattern_diagnostics.csv", [
  1502|         "schema_version", "analysis_run_id", "domain", "pattern_count", "dominant_pattern_share_pct",
  1503|         "entropy_index", "mixture_flag", "unknown_rate_pct", "recommended_analysis_grain",
  1504|         "hhi_domain_presence", "eff_clusters_domain_presence",
  1505|         "hhi_domain_dominance", "eff_clusters_domain_dominance",
  1506|         "hhi_domain_records", "eff_clusters_domain_records",
  1507|         "files_total", "files_with_unique_dominant", "files_with_tied_dominant", "files_excluded_from_dominance",
  1508|         "pct_files_unique_dominant", "governance_state",
  1509|     ], _sort_rows(diag_rows, ["analysis_run_id", "domain"]))
  1510| 
  1511|     _t_emit_analysis_total = time.perf_counter() - _t_emit_analysis_start
  1512|     sys.stderr.write(f"[patterns_timing] emit_analysis_total elapsed={_t_emit_analysis_total:.2f}s domains={len(domains)}\n")
  1513|     sys.stderr.flush()
  1514| 
  1515|     return analysis_run_id
```
