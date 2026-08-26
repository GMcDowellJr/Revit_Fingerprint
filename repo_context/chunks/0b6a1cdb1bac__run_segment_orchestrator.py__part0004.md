# Chunk of tools/run_segment_orchestrator.py

- Source relative path: `tools/run_segment_orchestrator.py`
- Chunk: 4 of 4
- Original line range: 1397-1867
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: run_orchestrator, main
- Source SHA-256: c1d79ae240bf0af45e5deb47ebd929be191e1d6bb8a42be87fe41cbe5dfc7646
- Starts inside symbol: no
- Ends inside symbol: no

```
  1397| def run_orchestrator(args: argparse.Namespace) -> int:
  1398|     manifest_file = Path(args.manifest_file).resolve()
  1399|     registry_file = Path(args.registry_file).resolve()
  1400|     results_registry_file = Path(args.results_registry_file).resolve()
  1401|     membership_file = Path(args.membership_file).resolve()
  1402|     records_dir = Path(args.records_dir).resolve()
  1403|     exports_dir = Path(args.exports_dir).resolve()
  1404|     segments_root = Path(args.segments_root).resolve()
  1405|     repo_root = Path(args.repo_root).resolve()
  1406|     join_policy = Path(args.join_policy).resolve()
  1407| 
  1408|     manifest = load_manifest(manifest_file)
  1409|     registry = load_registry(registry_file)
  1410|     membership = load_membership(membership_file)
  1411| 
  1412|     plan = build_run_plan(
  1413|         manifest, registry, args.segment, args.force
  1414|     )
  1415| 
  1416|     membership_errors = validate_membership_against_manifest(plan, membership)
  1417|     if membership_errors:
  1418|         sys.stderr.write(
  1419|             f"[ERROR orchestrator] segment_membership.csv ({membership_file}) disagrees "
  1420|             f"with segment_manifest.csv ({manifest_file}) for {len(membership_errors)} "
  1421|             f"segment(s) — refusing to run against a possibly stale or mismatched "
  1422|             f"membership file. Re-run build_segment_manifest.py, or check --membership-file:\n"
  1423|         )
  1424|         for err in membership_errors:
  1425|             sys.stderr.write(f"  {err}\n")
  1426|         return 1
  1427| 
  1428|     total = len(plan)
  1429|     n_complete = 0
  1430|     n_failed = 0
  1431|     n_skipped = 0
  1432|     failed_ids: List[str] = []
  1433|     skipped_ids: List[str] = []
  1434| 
  1435|     # Build a lookup from segment_id → index in registry list for in-place update
  1436|     reg_index: Dict[str, int] = {
  1437|         r.get("segment_id", ""): i for i, r in enumerate(registry)
  1438|     }
  1439| 
  1440|     # ── dry-run ──────────────────────────────────────────────────────────────
  1441|     if args.dry_run:
  1442|         print(f"[dry-run] {total} bundle segment(s) in plan")
  1443|         print(
  1444|             f"[dry-run] workers: segment_workers={args.workers} domain_workers={args.bundle_workers}"
  1445|             f" (mode={'auto' if args.workers_auto else 'explicit'})\n"
  1446|         )
  1447|         for idx, (reg_row, mrow) in enumerate(plan, 1):
  1448|             sid = reg_row.get("segment_id", "")
  1449|             output_folder = reg_row.get("output_folder", "").strip()
  1450|             status = reg_row.get("status", "").strip()
  1451|             run_type = reg_row.get("run_type", "bundle").strip()
  1452|             out_root = segments_root / output_folder
  1453| 
  1454|             # --segment filter
  1455|             if args.segment and sid not in set(args.segment):
  1456|                 continue
  1457| 
  1458|             # skip check -- a segment already marked complete under a prior config-only
  1459|             # run still needs (re)processing if this run additionally requests the name
  1460|             # leg and that leg hasn't produced output for this segment yet (PR #390 review).
  1461|             # run_type == "bundle" is required too -- see the matching comment in the
  1462|             # live-run skip-check loop below for why "reference" rows must be excluded.
  1463|             needs_name_leg = args.comparison_target in ("name", "both") and run_type == "bundle"
  1464|             already_satisfied = status == "complete" and (
  1465|                 not needs_name_leg or _segment_has_name_leg_output(out_root)
  1466|             )
  1467|             skip = already_satisfied and not args.force
  1468| 
  1469|             try:
  1470|                 level = int(mrow.get("segment_level", 0))
  1471|             except (ValueError, TypeError):
  1472|                 level = 0
  1473| 
  1474|             file_count = len(membership.get(sid, []))
  1475| 
  1476|             status_label = "complete (would skip)" if skip else status or "pending"
  1477|             reason_note = reg_row.get("notes", "").strip()
  1478|             reason_suffix = f"  reason={reason_note}" if (not skip and reason_note) else ""
  1479|             print(
  1480|                 f"[dry-run] segment={sid}  level={level}  files={file_count}"
  1481|                 f"  output={output_folder}  status={status_label}{reason_suffix}"
  1482|             )
  1483|             if skip:
  1484|                 print(f"  (skipped — already complete; use --force to re-run)")
  1485|                 continue
  1486| 
  1487|             corpus_label_synth_dir = records_dir.parent / "label_synthesis"
  1488|             extract_cmd = [
  1489|                 sys.executable,
  1490|                 str(repo_root / "tools" / "run_extract_all.py"),
  1491|                 str(exports_dir),
  1492|                 "--out-root", str(out_root),
  1493|                 "--stages", "patterns",
  1494|                 "--records-dir", str(records_dir),
  1495|                 "--label-synth-dir", str(corpus_label_synth_dir),
  1496|                 "--filter-export-run-ids", str(out_root / "export_run_ids.txt"),
  1497|                 "--join-policy", str(join_policy),
  1498|                 "--allow-sig-hash-join-key",
  1499|             ]
  1500|             print(f"  step 1: prepare (dirs + segment records filter)")
  1501|             print(f"  step 2: {' '.join(extract_cmd[1:])}")
  1502|             if args.comparison_target in ("name", "both"):
  1503|                 segment_name_key_csv = out_root / "results" / "name_key" / "name_key_results.csv"
  1504|                 name_patterns_cmd = [
  1505|                     sys.executable,
  1506|                     str(repo_root / "tools" / "generate_name_key_patterns.py"),
  1507|                     "--comparison-target", "name",
  1508|                     "--name-key-csv", str(segment_name_key_csv),
  1509|                     "--out-root", str(out_root / "results" / "name_key" / "patterns"),
  1510|                 ]
  1511|                 print(f"  step 2b: filter {args.name_key_results_csv} -> {segment_name_key_csv}")
  1512|                 print(f"  step 2b: {' '.join(name_patterns_cmd[1:])}")
  1513|             if run_type == "bundle":
  1514|                 bundle_cmd = [
  1515|                     sys.executable,
  1516|                     str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
  1517|                     "--analysis-dir", str(out_root / "results" / "analysis"),
  1518|                     "--out-dir", str(out_root / "results" / "bundle_analysis"),
  1519|                     "--metadata-file", str(records_dir / "file_metadata.csv"),
  1520|                     "--no-discover-populations",
  1521|                     "--purge-view", "both",
  1522|                     "--latent-purgeable-file", str(out_root / "results" / "records" / "latent_purgeable.csv"),
  1523|                 ]
  1524|                 bundle_cmd += ["--workers", str(args.bundle_workers)]
  1525|                 print(f"  step 3: {' '.join(bundle_cmd[1:])}")
  1526|                 if args.comparison_target in ("name", "both"):
  1527|                     name_bundle_cmd = [
  1528|                         sys.executable,
  1529|                         str(repo_root / "tools" / "bundle_analysis" / "run_bundle_analysis.py"),
  1530|                         "--analysis-dir", str(out_root / "results" / "analysis"),
  1531|                         "--out-dir", str(out_root / "results" / "bundle_analysis"),
  1532|                         "--comparison-target", "name",
  1533|                         "--name-key-patterns-dir", str(out_root / "results" / "name_key" / "patterns" / "name"),
  1534|                         "--metadata-file", str(records_dir / "file_metadata.csv"),
  1535|                         "--no-discover-populations",
  1536|                     ]
  1537|                     name_bundle_cmd += ["--workers", str(args.bundle_workers)]
  1538|                     print(f"  step 3b: rmtree {out_root / 'results' / 'bundle_analysis' / 'name'} (if exists)")
  1539|                     print(f"  step 3b: {' '.join(name_bundle_cmd[1:])}")
  1540|             print()
  1541|         return 0
  1542| 
  1543|     # ── live run ─────────────────────────────────────────────────────────────
  1544|     run_start_utc = utc_now_iso()
  1545|     run_t_start = time.monotonic()
  1546| 
  1547|     # Build segment_plans for preshard (respects --segment filter)
  1548|     segment_plans: Dict[str, Dict] = {}
  1549|     for reg_row, mrow in plan:
  1550|         sid = reg_row.get("segment_id", "").strip()
  1551|         if args.segment and sid not in set(args.segment):
  1552|             continue
  1553|         output_folder = reg_row.get("output_folder", "").strip()
  1554|         allowed_ids = set(membership.get(sid, []))
  1555|         out_root = segments_root / output_folder
  1556|         segment_records_dir = out_root / "results" / "records"
  1557|         segment_plans[sid] = {
  1558|             "sid": sid,
  1559|             "segment_records_dir": segment_records_dir,
  1560|             "allowed_ids": allowed_ids,
  1561|             "status": reg_row.get("status", "").strip(),
  1562|         }
  1563| 
  1564|     if segment_plans:
  1565|         preshard_marker = records_dir / _CORPUS_PRESHARD_MARKER
  1566|         _do_preshard = False
  1567|         # The corpus marker only means "nothing needs fresh sharded records" if
  1568|         # every planned segment is already complete. A registry-driven skip run
  1569|         # (default, no --force) can carry pending segments whose population
  1570|         # just changed — those need their records.csv/identity shards refreshed
  1571|         # even though the marker predates that change, otherwise _write_segment_records()'s
  1572|         # per-segment .preshard_complete fallback marker (also stale) causes the
  1573|         # segment to run against its OLD population while export_run_ids.txt
  1574|         # reflects the NEW one.
  1575|         _has_pending = any(plan.get("status") != "complete" for plan in segment_plans.values())
  1576|         if args.no_preshard:
  1577|             print("[orchestrator] preshard skipped (--no-preshard)", flush=True)
  1578|         elif args.force_preshard or args.force:
  1579|             _do_preshard = True
  1580|             preshard_marker.unlink(missing_ok=True)
  1581|         elif preshard_marker.is_file() and not _has_pending:
  1582|             print("[orchestrator] preshard skipped (corpus marker found, no pending segments)", flush=True)
  1583|         else:
  1584|             _do_preshard = True
  1585| 
  1586|         if _do_preshard:
  1587|             t_preshard = time.monotonic()
  1588|             _preshard_corpus_records(records_dir, segment_plans, force=args.force)
  1589|             print(f"[orchestrator] preshard complete elapsed={int(time.monotonic()-t_preshard)}s", flush=True)
  1590|             preshard_marker.write_text("ok", encoding="utf-8")
  1591| 
  1592|     # Apply --segment filter and skip check; count skips before submitting to executor
  1593|     segment_results: List[Dict] = []
  1594|     segment_results_lock = threading.Lock()
  1595| 
  1596|     plan_to_run: List[tuple[dict, dict]] = []
  1597|     for reg_row, mrow in plan:
  1598|         sid = reg_row.get("segment_id", "").strip()
  1599|         status = reg_row.get("status", "").strip()
  1600|         run_type = reg_row.get("run_type", "bundle").strip()
  1601|         out_root = segments_root / reg_row.get("output_folder", "").strip()
  1602| 
  1603|         if args.segment and sid not in set(args.segment):
  1604|             continue
  1605| 
  1606|         # A segment already marked complete under a prior config-only run still needs
  1607|         # (re)processing if this run additionally requests the name leg and that leg
  1608|         # hasn't produced output for this segment yet (PR #390 review) -- otherwise
  1609|         # --comparison-target name/both silently produces nothing for already-complete
  1610|         # segments unless the operator also remembers --force (which would needlessly
  1611|         # redo the config leg for every segment, not just the ones missing the name leg).
  1612|         # run_type == "bundle" is required too: step 3/3b (both legs) are gated on
  1613|         # run_type == "bundle", so a "reference" row can never produce a name-leg marker
  1614|         # regardless of comparison_target -- without this gate, reference rows would never
  1615|         # be recognized as satisfied under name/both and would be needlessly reprocessed
  1616|         # (prepare/patterns/name-patterns) on every run instead of honoring the existing
  1617|         # registry-driven skip.
  1618|         needs_name_leg = args.comparison_target in ("name", "both") and run_type == "bundle"
  1619|         already_satisfied = status == "complete" and (
  1620|             not needs_name_leg or _segment_has_name_leg_output(out_root)
  1621|         )
  1622|         if already_satisfied and not args.force:
  1623|             print(f"[orchestrator] skip segment={sid} (status=complete; use --force to re-run)")
  1624|             n_skipped += 1
  1625|             skipped_ids.append(f"{sid} — status=complete")
  1626|             try:
  1627|                 _skip_level = int(mrow.get("segment_level", 0))
  1628|             except (ValueError, TypeError):
  1629|                 _skip_level = 0
  1630|             _skip_files = len(membership.get(sid, []))
  1631|             segment_results.append({
  1632|                 "segment_id": sid,
  1633|                 "status": "skipped",
  1634|                 "files": _skip_files,
  1635|                 "level": _skip_level,
  1636|                 "prepare_s": 0,
  1637|                 "patterns_s": 0,
  1638|                 "bundle_s": 0,
  1639|                 "bi_merge_s": 0,
  1640|                 "total_s": 0,
  1641|                 "worker_id": 0,
  1642|                 "patterns_top5": [],
  1643|                 "failure_note": "",
  1644|             })
  1645|             continue
  1646| 
  1647|         plan_to_run.append((reg_row, mrow))
  1648| 
  1649|     registry_lock = threading.Lock()
  1650|     counters_lock = threading.Lock()
  1651|     counters: Dict[str, object] = {
  1652|         "complete": 0,
  1653|         "failed": 0,
  1654|         "skipped": n_skipped,
  1655|         "failed_ids": [],
  1656|     }
  1657| 
  1658|     with ThreadPoolExecutor(max_workers=args.workers) as executor:
  1659|         futures = {
  1660|             executor.submit(
  1661|                 _run_one_segment,
  1662|                 idx, total, reg_row, mrow, membership,
  1663|                 records_dir, exports_dir, segments_root, repo_root,
  1664|                 join_policy, args.skip_bi_merge,
  1665|                 registry, reg_index, registry_file,
  1666|                 manifest_file, results_registry_file,
  1667|                 registry_lock, counters, counters_lock,
  1668|                 worker_id=(i % args.workers) + 1,
  1669|                 bundle_workers=args.bundle_workers,
  1670|                 comparison_target=args.comparison_target,
  1671|                 name_key_results_csv=(
  1672|                     Path(args.name_key_results_csv).resolve() if args.name_key_results_csv else None
  1673|                 ),
  1674|             ): reg_row.get("segment_id", "")
  1675|             for i, (idx, (reg_row, mrow)) in enumerate(enumerate(plan_to_run, 1))
  1676|         }
  1677|         for future in as_completed(futures):
  1678|             try:
  1679|                 result = future.result()
  1680|                 with segment_results_lock:
  1681|                     segment_results.append(result)
  1682|             except Exception as exc:
  1683|                 sid = futures[future]
  1684|                 print(f"[orchestrator] ✗ segment={sid} unhandled exception: {exc}", flush=True)
  1685|                 with counters_lock:
  1686|                     counters["failed"] += 1
  1687|                     counters["failed_ids"].append(sid)
  1688|                 with segment_results_lock:
  1689|                     segment_results.append({
  1690|                         "segment_id": sid,
  1691|                         "status": "failed",
  1692|                         "files": 0,
  1693|                         "level": 0,
  1694|                         "prepare_s": 0,
  1695|                         "patterns_s": 0,
  1696|                         "bundle_s": 0,
  1697|                         "bi_merge_s": 0,
  1698|                         "total_s": 0,
  1699|                         "worker_id": 0,
  1700|                         "patterns_top5": [],
  1701|                         "failure_note": str(exc),
  1702|                     })
  1703| 
  1704|     run_end_utc = utc_now_iso()
  1705|     n_complete = counters["complete"]
  1706|     n_failed = counters["failed"]
  1707|     failed_ids = counters["failed_ids"]
  1708| 
  1709|     results_registry_failed = False
  1710|     try:
  1711|         rows_written = write_results_registry(
  1712|             manifest_file=manifest_file,
  1713|             registry_file=registry_file,
  1714|             output_file=results_registry_file,
  1715|         )
  1716|         print(
  1717|             f"[orchestrator] results_registry written to {results_registry_file} "
  1718|             f"({rows_written} row(s))",
  1719|             flush=True,
  1720|         )
  1721|     except Exception as exc:
  1722|         results_registry_failed = True
  1723|         print(f"[WARN orchestrator] results_registry write failed: {exc}", flush=True)
  1724| 
  1725|     # ── Final summary ─────────────────────────────────────────────────────────
  1726|     # Count non-bundle rows as additional skips
  1727|     non_bundle = [r for r in registry if r.get("run_type", "").strip() not in {"bundle", "reference"}]
  1728|     non_bundle_skipped = len(non_bundle)
  1729| 
  1730|     print(f"\n[orchestrator] ── run complete ──")
  1731|     print(f"  complete : {n_complete}")
  1732|     if failed_ids:
  1733|         print(f"  failed   : {n_failed}  ({', '.join(failed_ids)})")
  1734|     else:
  1735|         print(f"  failed   : {n_failed}")
  1736|     skip_detail = ""
  1737|     if skipped_ids:
  1738|         skip_detail = f"  ({'; '.join(skipped_ids)})"
  1739|     if non_bundle_skipped:
  1740|         skip_detail += f"  ({non_bundle_skipped} non-bundle rows — run_type!=bundle)"
  1741|     print(f"  skipped  : {n_skipped + non_bundle_skipped}{skip_detail}")
  1742|     print(f"  total    : {total}")
  1743| 
  1744|     segments_run = n_complete + n_failed
  1745|     total_elapsed = int(time.monotonic() - run_t_start)
  1746|     avg_per_segment = total_elapsed // segments_run if segments_run > 0 else 0
  1747|     print(
  1748|         f"[orchestrator] timing_summary segments_run={segments_run}"
  1749|         f" total_elapsed={total_elapsed}s avg_per_segment={avg_per_segment}s"
  1750|     )
  1751| 
  1752|     if segment_results:
  1753|         try:
  1754|             summary_path = _write_run_summary(
  1755|                 segments_root,
  1756|                 run_start_utc,
  1757|                 run_end_utc,
  1758|                 total_elapsed,
  1759|                 segment_results,
  1760|                 workers=args.workers,
  1761|                 bundle_workers=args.bundle_workers,
  1762|                 workers_auto=args.workers_auto,
  1763|             )
  1764|             print(f"[orchestrator] run_summary written to {summary_path}", flush=True)
  1765|         except Exception as _sum_exc:
  1766|             print(f"[WARN orchestrator] run_summary write failed: {_sum_exc}", flush=True)
  1767| 
  1768|     return 1 if n_failed > 0 or results_registry_failed else 0
  1769| 
  1770| 
  1771| # ── CLI ───────────────────────────────────────────────────────────────────────
  1772| 
  1773| def main() -> None:
  1774|     ap = argparse.ArgumentParser(
  1775|         description="Segment orchestrator: run patterns + bundle stages per segment in level order."
  1776|     )
  1777|     ap.add_argument("--manifest-file", required=True, help="Path to segment_manifest.csv")
  1778|     ap.add_argument(
  1779|         "--registry-file", required=True,
  1780|         help="Path to run_registry.csv (updated in-place after each segment)",
  1781|     )
  1782|     ap.add_argument(
  1783|         "--results-registry-file",
  1784|         default=None,
  1785|         help="Path to results_registry.csv (default: sibling of run_registry.csv)",
  1786|     )
  1787|     ap.add_argument(
  1788|         "--membership-file",
  1789|         default=None,
  1790|         help="Path to segment_membership.csv (default: sibling of segment_manifest.csv)",
  1791|     )
  1792|     ap.add_argument(
  1793|         "--records-dir", required=True,
  1794|         help="Path to corpus-level results/records/ directory",
  1795|     )
  1796|     ap.add_argument("--exports-dir", required=True, help="Path to fingerprint JSON exports folder")
  1797|     ap.add_argument(
  1798|         "--segments-root", required=True,
  1799|         help="Output root for segment folders — each segment written under {segments-root}/{output_folder}/",
  1800|     )
  1801|     ap.add_argument("--repo-root", required=True, help="Path to repo root (for resolving tool script paths)")
  1802|     ap.add_argument("--join-policy", required=True, help="Path to domain_join_key_policies.json")
  1803|     ap.add_argument(
  1804|         "--segment", nargs="+", default=None,
  1805|         help="Optional: run only these segment_id(s) (space-separated, targeted re-run or resume)",
  1806|     )
  1807|     ap.add_argument(
  1808|         "--force", action="store_true",
  1809|         help="Re-run segments already marked complete in the registry",
  1810|     )
  1811|     ap.add_argument(
  1812|         "--dry-run", action="store_true",
  1813|         help="Print full run plan without executing anything",
  1814|     )
  1815|     ap.add_argument(
  1816|         "--skip-bi-merge", action="store_true",
  1817|         help="Skip the BI merge post-processing step (useful for dry runs and debugging)",
  1818|     )
  1819|     ap.add_argument(
  1820|         "--workers", default=4,
  1821|         help="Max parallel segments, or 'auto' to derive from CPU count (default: 4)",
  1822|     )
  1823|     ap.add_argument(
  1824|         "--no-preshard", action="store_true",
  1825|         help="Skip preshard unconditionally",
  1826|     )
  1827|     ap.add_argument(
  1828|         "--force-preshard", action="store_true",
  1829|         help="Force preshard even if corpus marker exists",
  1830|     )
  1831|     ap.add_argument(
  1832|         "--comparison-target", choices=sorted(VALID_COMPARISON_TARGETS), default="config",
  1833|         help="config (default, unchanged behavior/output): join_hash only, exactly as "
  1834|              "before this flag existed. name/both additionally re-cluster this segment's "
  1835|              "slice of --name-key-results-csv (PR1's join_key_name_identity) and bundle-mine "
  1836|              "it into results/bundle_analysis/name_all/, alongside the existing "
  1837|              "results/bundle_analysis/{all,used}/ config-target output.",
  1838|     )
  1839|     ap.add_argument(
  1840|         "--name-key-results-csv", default=None,
  1841|         help="Path to a corpus-wide name_key_results.csv (tools/apply_name_key_policy.py "
  1842|              "output, run once for the whole corpus beforehand). Required when "
  1843|              "--comparison-target is name or both.",
  1844|     )
  1845|     args = ap.parse_args()
  1846|     if args.comparison_target in ("name", "both") and not args.name_key_results_csv:
  1847|         ap.error("--name-key-results-csv is required when --comparison-target is name or both")
  1848|     if str(args.workers).strip().lower() == "auto":
  1849|         args.workers, args.bundle_workers = compute_worker_split()
  1850|         args.workers_auto = True
  1851|     else:
  1852|         args.workers = int(args.workers)
  1853|         # Coordinate the bundle-stage pool to the same CPU budget rather than
  1854|         # letting it default to run_bundle_analysis.py's own fixed default of 4
  1855|         # — otherwise total concurrency grows unbounded as --workers N grows
  1856|         # (N x 4 instead of staying near the actual core budget).
  1857|         _, args.bundle_workers = compute_worker_split(segment_workers=args.workers)
  1858|         args.workers_auto = False
  1859|     if args.results_registry_file is None:
  1860|         args.results_registry_file = str(Path(args.registry_file).resolve().with_name("results_registry.csv"))
  1861|     if args.membership_file is None:
  1862|         args.membership_file = str(Path(args.manifest_file).resolve().with_name("segment_membership.csv"))
  1863|     sys.exit(run_orchestrator(args))
  1864| 
  1865| 
  1866| if __name__ == "__main__":
  1867|     main()
```
