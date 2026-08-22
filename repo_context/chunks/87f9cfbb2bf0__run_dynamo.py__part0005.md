# Chunk of runner/run_dynamo.py

- Source relative path: `runner/run_dynamo.py`
- Chunk: 5 of 5
- Original line range: 1165-1581
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _resolve_output_mode, _strip_detail_surfaces, _canonicalize_all_domain_records, _get_output_path_from_dynamo, _ensure_parent_dir, _write_json_to_disk, _write_fingerprint, _write_fingerprint._try_write, _sha256_of_file
- Source SHA-256: ab34a2ab032f21677da5f5b1b79a0089611b4fad01f19f048caf0e2893056e4b
- Starts inside symbol: no
- Ends inside symbol: no

```
  1165| 
  1166| 
  1167| # Detail surface keys suppressed in production-mode writes.
  1168| # This is the single declaration point — no per-domain logic inside _strip_detail_surfaces.
  1169| _DETAIL_SURFACE_KEYS = frozenset([
  1170|     "layer_rows",
  1171|     # Future detail surfaces declared here as domains add them
  1172| ])
  1173| 
  1174| 
  1175| def _resolve_output_mode():
  1176|     """Read REVIT_FINGERPRINT_OUTPUT_MODE from env; absent/unrecognized values resolve to 'production'."""
  1177|     try:
  1178|         mode = str(os.environ.get("REVIT_FINGERPRINT_OUTPUT_MODE", "")).strip().lower()
  1179|     except Exception:
  1180|         mode = ""
  1181|     return "dev" if mode == "dev" else "production"
  1182| 
  1183| 
  1184| def _strip_detail_surfaces(payload):
  1185|     """
  1186|     Return a copy of the fingerprint payload with _DETAIL_SURFACE_KEYS removed from all records.
  1187|     Does not mutate the input. Safe on any payload shape including partially-formed error payloads.
  1188|     """
  1189|     if not isinstance(payload, dict):
  1190|         return payload
  1191| 
  1192|     payload = dict(payload)  # shallow copy at top level
  1193| 
  1194|     # Guard for a potential future "domains" wrapper key
  1195|     domains_val = payload.get("domains")
  1196|     if isinstance(domains_val, dict):
  1197|         new_domains = {}
  1198|         for domain_name, domain_payload in domains_val.items():
  1199|             if not isinstance(domain_payload, dict):
  1200|                 new_domains[domain_name] = domain_payload
  1201|                 continue
  1202|             records = domain_payload.get("records")
  1203|             if not isinstance(records, list):
  1204|                 new_domains[domain_name] = domain_payload
  1205|                 continue
  1206|             new_records = []
  1207|             changed = False
  1208|             for rec in records:
  1209|                 if not isinstance(rec, dict) or _DETAIL_SURFACE_KEYS.isdisjoint(rec.keys()):
  1210|                     new_records.append(rec)
  1211|                 else:
  1212|                     new_records.append({k: v for k, v in rec.items() if k not in _DETAIL_SURFACE_KEYS})
  1213|                     changed = True
  1214|             if changed:
  1215|                 domain_payload = dict(domain_payload)
  1216|                 domain_payload["records"] = new_records
  1217|             new_domains[domain_name] = domain_payload
  1218|         payload["domains"] = new_domains
  1219|         return payload
  1220| 
  1221|     # Current flat structure: domain payloads sit at non-underscore top-level keys
  1222|     for key in list(payload.keys()):
  1223|         if not isinstance(key, str) or key.startswith("_"):
  1224|             continue
  1225|         domain_payload = payload.get(key)
  1226|         if not isinstance(domain_payload, dict):
  1227|             continue
  1228|         records = domain_payload.get("records")
  1229|         if not isinstance(records, list):
  1230|             continue
  1231|         new_records = []
  1232|         changed = False
  1233|         for rec in records:
  1234|             if not isinstance(rec, dict) or _DETAIL_SURFACE_KEYS.isdisjoint(rec.keys()):
  1235|                 new_records.append(rec)
  1236|             else:
  1237|                 new_records.append({k: v for k, v in rec.items() if k not in _DETAIL_SURFACE_KEYS})
  1238|                 changed = True
  1239|         if changed:
  1240|             new_domain_payload = dict(domain_payload)
  1241|             new_domain_payload["records"] = new_records
  1242|             payload[key] = new_domain_payload
  1243| 
  1244|     return payload
  1245| 
  1246| 
  1247| def _canonicalize_all_domain_records(payload):
  1248|     if not isinstance(payload, dict):
  1249|         return payload
  1250|     out = dict(payload)
  1251|     for key, domain_payload in list(out.items()):
  1252|         if not isinstance(key, str) or key.startswith("_") or not isinstance(domain_payload, dict):
  1253|             continue
  1254|         records = domain_payload.get("records")
  1255|         if not isinstance(records, list):
  1256|             continue
  1257|         new_domain = dict(domain_payload)
  1258|         new_domain["records"] = [canonicalize_record(r) for r in records]
  1259|         out[key] = new_domain
  1260|     return out
  1261| 
  1262| 
  1263| # Execute extraction (OUT protection)
  1264| try:
  1265|     _timing = TimingCollector()
  1266|     _timing.start_timer("total_run")
  1267|     # --- Document resolution ---
  1268|     # Default: get from DocumentManager (Dynamo/journal path, unchanged behavior).
  1269|     # Override: if REVIT_FINGERPRINT_DOC_TITLE is set (pyRevit/external runner),
  1270|     # find the document by filename from Application.Documents instead.
  1271|     # This env var is never set by the thin runner, so Dynamo behavior is unaffected.
  1272|     _doc_title_override = ""
  1273|     try:
  1274|         _doc_title_override = str(os.environ.get("REVIT_FINGERPRINT_DOC_TITLE", "")).strip()
  1275|     except Exception:
  1276|         _doc_title_override = ""
  1277| 
  1278|     if _doc_title_override:
  1279|         doc = None
  1280|         try:
  1281|             from RevitServices.Persistence import DocumentManager as _DM_override
  1282|             _ui_app = _DM_override.Instance.CurrentUIApplication
  1283|             for _candidate in _ui_app.Application.Documents:
  1284|                 try:
  1285|                     _candidate_name = os.path.basename(str(_candidate.PathName or ""))
  1286|                     if (
  1287|                         _candidate_name == _doc_title_override
  1288|                         or _candidate.Title == _doc_title_override
  1289|                         or str(_candidate.PathName or "") == _doc_title_override
  1290|                     ):
  1291|                         doc = _candidate
  1292|                         break
  1293|                 except Exception:
  1294|                     continue
  1295|         except Exception:
  1296|             doc = None
  1297|         if doc is None:
  1298|             # Fallback to default if lookup fails
  1299|             doc = get_doc()
  1300|     else:
  1301|         doc = get_doc()
  1302| 
  1303|     fingerprint = run_fingerprint(doc, timing=_timing)
  1304|     _timing = fingerprint.pop("_timing_collector", _timing)
  1305| 
  1306|     domains_emitted = sorted([k for k in fingerprint.keys() if not str(k).startswith("_")])
  1307| 
  1308|     if ENABLED_DOMAINS is None:
  1309|         domains_requested = "ALL"
  1310|     else:
  1311|         domains_requested = list(ENABLED_DOMAINS)
  1312| 
  1313|     fingerprint["_meta"] = {
  1314|         "repo_root": _REPO_ROOT,
  1315|         "tool_version": _TOOL_VERSION,
  1316|         "runner": "M5",
  1317|         "elapsed_seconds": fingerprint.pop("_elapsed_seconds", None),
  1318|         "elapsed_seconds_total": round(time.perf_counter() - _SCRIPT_START, 3),
  1319|         "domains_requested": domains_requested,
  1320|         "domains_emitted": domains_emitted,
  1321|     }
  1322|     fingerprint["_runner_warnings"] = _SYNC_WARNINGS
  1323| 
  1324|     # ------------------------------------------------------------
  1325|     # Output strategy:
  1326|     # - If IN[0] provides an output file path: write full JSON to disk here,
  1327|     #   then return a small summary JSON via OUT (keeps Revit/Dynamo responsive).
  1328|     # - If no path provided: preserve legacy behavior (OUT is the full JSON string).
  1329|     # ------------------------------------------------------------
  1330|     def _get_output_path_from_dynamo():
  1331|         # 1) Preferred: env var injected by thin runner (works across import boundary)
  1332|         try:
  1333|             p = os.getenv("REVIT_FINGERPRINT_OUTPUT_PATH", "")
  1334|             if p is not None:
  1335|                 p = str(p).strip()
  1336|                 if p:
  1337|                     return p
  1338|         except Exception as e:
  1339|             pass
  1340| 
  1341|         # 2) Fallback: direct IN[0] (only works if this module is executed as the Dynamo node)
  1342|         try:
  1343|             _in = IN
  1344|             if _in is not None and len(_in) > 0 and _in[0] is not None:
  1345|                 p = str(_in[0]).strip()
  1346|                 if p:
  1347|                     return p
  1348|         except Exception as e:
  1349|             pass
  1350| 
  1351|         # 3) Default: user temp directory (file named from RVT identity)
  1352|         try:
  1353|             import tempfile
  1354|             from datetime import datetime
  1355| 
  1356|             base = os.path.join(tempfile.gettempdir(), "Revit_Fingerprint")
  1357|             try:
  1358|                 if not os.path.exists(base):
  1359|                     os.makedirs(base)
  1360|             except Exception:
  1361|                 pass
  1362| 
  1363|             # Timestamp control (single source of truth)
  1364|             use_stamp = _use_filename_stamp()
  1365|             stamp = datetime.now().strftime("%Y%m%dT%H%M") if use_stamp else None
  1366| 
  1367|             fname = fp_naming.build_output_filename(
  1368|                 doc,
  1369|                 stamp=stamp,
  1370|                 kind="fingerprint",
  1371|                 ext="json",
  1372|                 include_stamp=use_stamp,
  1373|             )
  1374|             return os.path.join(base, fname)
  1375| 
  1376|         except Exception:
  1377|             return None
  1378| 
  1379|     def _ensure_parent_dir(path):
  1380|         try:
  1381|             parent = os.path.dirname(path)
  1382|             if parent and not os.path.exists(parent):
  1383|                 os.makedirs(parent)
  1384|         except Exception as e:
  1385|             pass
  1386| 
  1387|     def _write_json_to_disk(path, payload):
  1388|         """
  1389|         Writes JSON directly to disk to avoid returning multi-MB payloads through Dynamo.
  1390|         Returns (bytes_written, write_elapsed_seconds).
  1391|         Serialization format and detail surface suppression are controlled by
  1392|         REVIT_FINGERPRINT_OUTPUT_MODE (dev=indent+full, production=compact+stripped).
  1393|         """
  1394|         t0 = time.perf_counter()
  1395|         _ensure_parent_dir(path)
  1396| 
  1397|         _mode = _resolve_output_mode()
  1398|         _is_dev = (_mode == "dev")
  1399|         _suppress_detail_surfaces = not _is_dev
  1400| 
  1401|         _write_payload = _strip_detail_surfaces(payload) if _suppress_detail_surfaces else payload
  1402| 
  1403|         with open(path, "w", encoding="utf-8") as f:
  1404|             if _is_dev:
  1405|                 json.dump(_write_payload, f, indent=2, sort_keys=True)
  1406|             else:
  1407|                 json.dump(_write_payload, f, separators=(',', ':'), sort_keys=True)
  1408| 
  1409|         bytes_written = None
  1410|         try:
  1411|             bytes_written = os.path.getsize(path)
  1412|         except Exception as e:
  1413|             pass
  1414|         return bytes_written, round(time.perf_counter() - t0, 3)
  1415| 
  1416|     def _write_fingerprint(base_payload_path, fingerprint_payload):
  1417|         """Write one monolithic fingerprint JSON file."""
  1418|         import time as _time
  1419| 
  1420|         paths = {
  1421|             "payload": base_payload_path,
  1422|         }
  1423| 
  1424|         bytes_written = {}
  1425|         sha256 = {}
  1426|         errors = []
  1427| 
  1428|         t0 = _time.perf_counter()
  1429|         total_write_sec = 0.0
  1430| 
  1431|         def _try_write(kind, obj):
  1432|             nonlocal total_write_sec
  1433|             try:
  1434|                 b, sec = _write_json_to_disk(paths[kind], obj)
  1435|                 bytes_written[kind] = b
  1436|                 total_write_sec += float(sec) if sec is not None else 0.0
  1437|                 try:
  1438|                     sha256[kind] = _sha256_of_file(paths[kind])
  1439|                 except Exception as e:
  1440|                     errors.append({"surface": kind, "code": "sha256_failed", "message": str(e)})
  1441|             except Exception as e:
  1442|                 errors.append({"surface": kind, "code": "write_failed", "message": str(e)})
  1443| 
  1444|         _try_write("payload", fingerprint_payload)
  1445| 
  1446|         total_write_sec = round(_time.perf_counter() - t0, 3)
  1447| 
  1448|         return paths, bytes_written, sha256, total_write_sec, errors
  1449| 
  1450|     def _sha256_of_file(path, buf_size=1024 * 1024):
  1451|         """
  1452|         Compute SHA-256 of a file without loading it into memory.
  1453|         Returns hex digest string.
  1454|         """
  1455|         h = hashlib.sha256()
  1456|         with open(path, "rb") as f:
  1457|             while True:
  1458|                 chunk = f.read(buf_size)
  1459|                 if not chunk:
  1460|                     break
  1461|                 h.update(chunk)
  1462|         return h.hexdigest()
  1463| 
  1464|     # Timings around the post-extraction phase
  1465|     t_extract_done = round(time.perf_counter() - _SCRIPT_START, 3)
  1466| 
  1467|     if _timing is not None:
  1468|         _timing.start_timer("total_serialization")
  1469|     output_path = _get_output_path_from_dynamo()
  1470| 
  1471|     # If caller provided a directory, write a deterministically-named file into it.
  1472|     # This supports batch runs: set output path once to a folder and let the runner name files.
  1473|     try:
  1474|         if output_path:
  1475|             op = str(output_path).strip()
  1476|             if op:
  1477|                 is_dir = False
  1478|                 try:
  1479|                     if os.path.isdir(op):
  1480|                         is_dir = True
  1481|                 except Exception:
  1482|                     is_dir = False
  1483| 
  1484|                 # Heuristic: treat as directory if it ends with a path separator or has no ".json" suffix.
  1485|                 # (We do NOT want to silently interpret arbitrary filenames as directories.)
  1486|                 try:
  1487|                     if (op.endswith(os.sep) or op.endswith("/") or op.endswith("\\")) and (not os.path.exists(op) or os.path.isdir(op)):
  1488|                         is_dir = True
  1489|                 except Exception:
  1490|                     pass
  1491| 
  1492|                 if is_dir:
  1493|                     try:
  1494|                         if not os.path.exists(op):
  1495|                             os.makedirs(op)
  1496|                     except Exception:
  1497|                         # If we cannot create the directory, fall back to original op and let write fail explicitly.
  1498|                         pass
  1499| 
  1500|                     from datetime import datetime
  1501| 
  1502|                     # Timestamp control (single source of truth)
  1503|                     use_stamp = _use_filename_stamp()
  1504|                     stamp = datetime.now().strftime("%Y%m%dT%H%M") if use_stamp else None
  1505| 
  1506|                     fname = fp_naming.build_output_filename(
  1507|                         doc,
  1508|                         stamp=stamp,
  1509|                         kind="fingerprint",
  1510|                         ext="json",
  1511|                         include_stamp=use_stamp,
  1512| )
  1513|                     output_path = os.path.join(op, fname)
  1514| 
  1515|     except Exception:
  1516|         # Never crash the run due to naming; write will handle errors explicitly.
  1517|         pass
  1518| 
  1519|     # Escape hatch: force legacy behavior (return full JSON via OUT) when explicitly requested
  1520|     force_full_out = False
  1521|     try:
  1522|         force_full_out = str(os.getenv("REVIT_FINGERPRINT_FORCE_FULL_OUT", "")).strip() in ("1", "true", "True", "YES", "yes")
  1523|     except Exception as e:
  1524|         force_full_out = False
  1525| 
  1526|     if output_path and not force_full_out:
  1527|         paths, bytes_written, sha256, write_sec_total, write_errors = _write_fingerprint(output_path, fingerprint)
  1528| 
  1529|         t_total_done = round(time.perf_counter() - _SCRIPT_START, 3)
  1530| 
  1531|         status = "ok" if not write_errors else "degraded"
  1532| 
  1533|         summary = {
  1534|             "status": status,
  1535|             "output_mode": _resolve_output_mode(),
  1536|             "output_paths": paths,
  1537|             "output_surfaces": ["payload"],
  1538|             "filename_stamp_enabled": _use_filename_stamp(),
  1539|             "filename_stamp_env": os.environ.get("REVIT_FINGERPRINT_FILENAME_STAMP", None),
  1540|             "bytes_written": bytes_written,
  1541|             "sha256": sha256,
  1542|             "write_errors": write_errors,
  1543|             "timings": {
  1544|                 "extract_done_sec_from_start": t_extract_done,
  1545|                 "json_write_sec_total": write_sec_total,
  1546|                 "total_done_sec_from_start": t_total_done,
  1547|             },
  1548|             "_meta": fingerprint.get("_meta", {}),
  1549|         }
  1550| 
  1551|         OUT = json.dumps(summary, indent=2, sort_keys=True)
  1552| 
  1553|     else:
  1554|         # Legacy behavior: return full JSON through Dynamo (may hang on large payloads)
  1555|         OUT = json.dumps(fingerprint, indent=2, sort_keys=True)
  1556| 
  1557|     try:
  1558|         if _timing is not None:
  1559|             _timing.end_timer("total_serialization")
  1560|     except Exception:
  1561|         pass
  1562|     try:
  1563|         if _timing is not None:
  1564|             _timing.end_timer("total_run")
  1565|     except Exception:
  1566|         pass
  1567| 
  1568| except Exception as e:
  1569|     import traceback as _traceback
  1570| 
  1571|     err = {
  1572|         "error": str(e),
  1573|         "traceback": _traceback.format_exc(),
  1574|         "_meta": {
  1575|             "runner": "M5",
  1576|             "runner_file": __file__,
  1577|         },
  1578|     }
  1579| 
  1580|     # Keep OUT type consistent (JSON string) even on failure
  1581|     OUT = json.dumps(err, indent=2, sort_keys=True)
```
