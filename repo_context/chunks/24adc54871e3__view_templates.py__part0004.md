# Chunk of domains/view_templates.py

- Source relative path: `domains/view_templates.py`
- Chunk: 4 of 6
- Original line range: 1297-1722
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_elevations_sections_detail, extract_elevations_sections_detail._v2_block, _build_renderings_drafting_viewtype_set
- Source SHA-256: ca478c676990e318341a80d987cc318a4531ef7d17b52cb5fd1b41c67678296d
- Starts inside symbol: no
- Ends inside symbol: no

```
  1297| def extract_elevations_sections_detail(doc, ctx=None):
  1298|     DOMAIN_NAME = "view_templates_elevations_sections_detail"
  1299|     DOMAIN_VIEWTYPE_SET = _ELEVATION_SECTION_DETAIL_VIEWTYPE_SET
  1300|     """
  1301|     Extract view templates fingerprint - Elevations only.
  1302| 
  1303|     Per-template signature: include flags + phase filter hash + filter stack.
  1304|     No category-override iteration (VCO domain handles that separately).
  1305| 
  1306|     Args:
  1307|         doc: Revit document
  1308|         ctx: context dict with mappings from other domains
  1309| 
  1310|     Returns:
  1311|         Dictionary with count, hash_v2, records, record_rows, and debug counters
  1312|     """
  1313|     info = {
  1314|         "count": 0,
  1315|         "raw_count": 0,
  1316|         "names": [],
  1317|         "records": [],
  1318| 
  1319|         # debug counters
  1320|         "debug_not_template": 0,
  1321|         "debug_missing_name": 0,
  1322|         "debug_missing_uid": 0,
  1323|         "debug_fail_read": 0,
  1324|         "debug_kept": 0,
  1325|         "debug_view_type_filtered": 0,
  1326| 
  1327|         # v2 surfaces
  1328|         "hash_v2": None,
  1329|         "signature_hashes_v2": [],
  1330|         "debug_v2_blocked": False,
  1331|         "debug_v2_block_reasons": {},
  1332|         # PR6: deterministic degraded signaling
  1333|         "debug_view_context_problem": 0,
  1334|         "debug_view_context_reasons": {},
  1335|         "debug_collect_types_failed": 0,
  1336|     }
  1337| 
  1338|     ctx_map = ctx or {}
  1339| 
  1340|     try:
  1341|         require_domain(ctx_map.get("_domains", {}), "phase_filters")
  1342|         require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
  1343|     except Blocked as b:
  1344|         info["debug_v2_blocked"] = True
  1345|         info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
  1346|         info["count"] = 0
  1347|         info["records"] = []
  1348|         info["hash_v2"] = None
  1349|         return info
  1350| 
  1351|     phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
  1352|     phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
  1353|     view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})
  1354| 
  1355|     try:
  1356|         col = list(
  1357|             collect_instances(
  1358|                 doc,
  1359|                 of_class=View,
  1360|                 require_unique_id=True,
  1361|                 cctx=(ctx or {}).get("_collect") if ctx is not None else None,
  1362|                 cache_key=_VIEW_INSTANCES_CACHE_KEY,
  1363|             )
  1364|         )
  1365|     except Exception as e:
  1366|         info["debug_collect_types_failed"] += 1
  1367|         info["_domain_status"] = "degraded"
  1368|         info["_domain_diag"] = {
  1369|             "degraded_reasons": ["collect_types_failed"],
  1370|             "degraded_reason_counts": {"collect_types_failed": 1},
  1371|             "error": str(e),
  1372|         }
  1373|         return info
  1374| 
  1375|     info["raw_count"] = len(col)
  1376| 
  1377|     names = []
  1378|     records = []
  1379|     per_hashes = []
  1380|     per_hashes_v2 = []
  1381|     v2_any_blocked = False
  1382| 
  1383|     def _v2_block(reason):
  1384|         nonlocal v2_any_blocked
  1385|         v2_any_blocked = True
  1386|         info["debug_v2_blocked"] += 1
  1387|         try:
  1388|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
  1389|         except Exception:
  1390|             pass
  1391| 
  1392|     for v in col:
  1393|         try:
  1394|             is_template = v.IsTemplate
  1395|         except Exception:
  1396|             is_template = False
  1397| 
  1398|         if not is_template:
  1399|             info["debug_not_template"] += 1
  1400|             continue
  1401| 
  1402|         # Integer ViewType filter (CPython3 returns int string from enum)
  1403|         try:
  1404|             vt_int = int(v.ViewType)
  1405|         except Exception:
  1406|             vt_int = None
  1407|         if vt_int not in DOMAIN_VIEWTYPE_SET:
  1408|             info["debug_view_type_filtered"] += 1
  1409|             continue
  1410| 
  1411|         name = canon_str(getattr(v, "Name", None))
  1412|         if not name:
  1413|             info["debug_missing_name"] += 1
  1414|             name = S_MISSING
  1415|         names.append(name)
  1416| 
  1417|         uid = None
  1418|         try:
  1419|             uid = canon_str(getattr(v, "UniqueId", None))
  1420|         except Exception:
  1421|             uid = None
  1422| 
  1423|         if not uid:
  1424|             info["debug_missing_uid"] += 1
  1425| 
  1426|         # PR6: view-scoped context snapshot
  1427|         try:
  1428|             dv = (ctx or {}).get("_doc_view") if ctx is not None else None
  1429|             if dv is not None:
  1430|                 vi = dv.view_info(v, source="HOST")
  1431|                 if vi.reasons:
  1432|                     info["debug_view_context_problem"] += 1
  1433|                     for r in vi.reasons:
  1434|                         info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
  1435|         except Exception:
  1436|             info["debug_view_context_problem"] += 1
  1437|             info["debug_view_context_reasons"]["view_context_unreadable"] = (
  1438|                 info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
  1439|             )
  1440| 
  1441|         v2_ok = True
  1442|         sig_v2 = []
  1443|         sig = []
  1444| 
  1445|         # Template-controlled parameters ("Include" surface)
  1446|         try:
  1447|             tpl_ids = v.GetTemplateParameterIds() or []
  1448|             tpl_bips = set(
  1449|                 pid.IntegerValue for pid in tpl_ids
  1450|                 if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
  1451|             )
  1452|         except Exception:
  1453|             tpl_ids = []
  1454|             tpl_bips = set()
  1455| 
  1456|         non_ctrl_bips = _non_ctrl_bips_from_view(v)
  1457|         info["debug_non_ctrl_bips_count"] = len(non_ctrl_bips)
  1458| 
  1459|         # Common include flags
  1460|         try:
  1461|             sig.append("include_phase_filter={}".format(_is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")))
  1462|         except Exception:
  1463|             sig.append("include_phase_filter=False")
  1464| 
  1465|         try:
  1466|             sig.append("include_filters={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_FILTERS")))
  1467|         except Exception:
  1468|             sig.append("include_filters=False")
  1469| 
  1470|         try:
  1471|             sig.append("include_appearance={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_APPEARANCE")))
  1472|         except Exception:
  1473|             sig.append("include_appearance=False")
  1474| 
  1475|         # Domain-specific: far clip (elevations/sections control far clipping)
  1476|         try:
  1477|             sig.append("include_far_clip={}".format(_is_template_param_included(non_ctrl_bips, "VIEWER_BOUND_FAR_CLIPPING")))
  1478|         except Exception:
  1479|             sig.append("include_far_clip=False")
  1480| 
  1481|         # Phase Filter (resolved via phase_filters domain)
  1482|         try:
  1483|             include_pf = _is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")
  1484|         except Exception:
  1485|             include_pf = False
  1486| 
  1487|         v2_ok = _append_phase_filter_value(
  1488|             v=v,
  1489|             doc=doc,
  1490|             include_pf=include_pf,
  1491|             phase_filter_map=phase_filter_map,
  1492|             phase_filter_map_v2=phase_filter_map_v2,
  1493|             sig=sig,
  1494|             sig_v2=sig_v2,
  1495|             v2_ok=v2_ok,
  1496|             v2_block_fn=_v2_block,
  1497|             debug_counters=info,
  1498|         )
  1499| 
  1500|         # Filter stack (order-sensitive)
  1501|         v2_ok = _append_filter_stack_signature(v, doc, view_filter_map, sig, sig_v2, v2_ok, _v2_block)
  1502|         v2_ok = _append_workset_visibility(v, doc, sig, sig_v2, v2_ok, _v2_block)
  1503| 
  1504|         # Built-in visual/behavioural parameters
  1505|         emit_builtin_params(v, DOMAIN_NAME, tpl_bips, non_ctrl_bips, sig, sig_v2,
  1506|                             debug_counters=info)
  1507| 
  1508|         # Shared/project parameters (stub — no-op until GUIDs confirmed)
  1509|         emit_shared_params_stub(v, DOMAIN_NAME, tpl_ids, sig, sig_v2,
  1510|                                 debug_counters=info)
  1511| 
  1512|         # Finalize signature (deterministic)
  1513|         sig_final = sorted(sig)
  1514|         def_hash = make_hash(sig_final)
  1515| 
  1516|         # v2 finalize
  1517|         if v2_ok:
  1518|             try:
  1519|                 sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
  1520|                 sig_v2_final = sorted(set(sig_v2))
  1521|                 def_hash_v2 = make_hash(sig_v2_final)
  1522|                 per_hashes_v2.append(def_hash_v2)
  1523|             except Exception:
  1524|                 _v2_block("template_finalize_failed")
  1525|                 v2_ok = False
  1526| 
  1527|         # record.v2 + Phase-2
  1528|         identity_items = _canonical_identity_items_from_signature(def_hash, sig_final)
  1529|         semantic_keys = _semantic_keys_from_identity_items(identity_items)
  1530|         semantic_items = [it for it in identity_items if it.get("k") in set(semantic_keys)]
  1531|         sig_hash = make_hash(serialize_identity_items(semantic_items))
  1532| 
  1533|         rid_info = make_record_id_from_element(v)
  1534|         if rid_info:
  1535|             record_id, record_id_alg = rid_info
  1536|         else:
  1537|             record_id = "eid:{}".format(safe_str(getattr(getattr(v, "Id", None), "IntegerValue", "")))
  1538|             record_id_alg = "revit_elementid_v1"
  1539| 
  1540|         status = STATUS_OK
  1541|         status_reasons = []
  1542|         for it in identity_items:
  1543|             if it.get("q") != ITEM_Q_OK:
  1544|                 status = STATUS_DEGRADED
  1545|                 status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))
  1546|         if not v2_ok:
  1547|             status = STATUS_BLOCKED
  1548|             status_reasons.append("semantic_v2_unresolved_dependency")
  1549|             sig_hash = None
  1550| 
  1551|         vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING
  1552| 
  1553|         rec = build_record_v2(
  1554|             domain=DOMAIN_NAME,
  1555|             record_id=record_id,
  1556|             record_id_alg=record_id_alg,
  1557|             status=status,
  1558|             status_reasons=sorted(set(status_reasons)),
  1559|             sig_hash=sig_hash,
  1560|             identity_items=identity_items,
  1561|             required_qs=tuple(it.get("q") for it in identity_items),
  1562|             label={
  1563|                 "display": safe_str(name),
  1564|                 "quality": "human" if safe_str(name) and safe_str(name) != S_MISSING else "placeholder_missing",
  1565|                 "provenance": "revit.ViewName",
  1566|                 "components": {
  1567|                     "view_type": vt_raw_str,
  1568|                 },
  1569|             },
  1570|         )
  1571|         _ip, _ip_q = purge_lookup(getattr(getattr(v, "Id", None), "IntegerValue", None), ctx)
  1572|         rec["is_purgeable"] = _ip
  1573|         rec["is_purgeable_q"] = _ip_q
  1574| 
  1575|         rec["phase2"] = {
  1576|             "schema": "phase2.{}.v2".format(DOMAIN_NAME),
  1577|             "grouping_basis": "join_key.join_hash",
  1578|             "cosmetic_items": [],
  1579|             "coordination_items": [
  1580|                 make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
  1581|                 make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
  1582|             ],
  1583|             "unknown_items": _traceability_unknown_items(v),
  1584|         }
  1585|         _append_assigned_view_count_cosmetic_item(rec, doc, v, ctx)
  1586| 
  1587|         rec["sig_basis"] = {
  1588|             "hash_alg": "md5_utf8_join_pipe",
  1589|             "keys_used": semantic_keys,
  1590|         }
  1591| 
  1592|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  1593|         vt_join_key, _vt_missing = build_join_key_from_policy(
  1594|             domain_policy=pol,
  1595|             identity_items=identity_items,
  1596|             include_optional_items=False,
  1597|             emit_keys_used=True,
  1598|             hash_optional_items=False,
  1599|             emit_items=False,
  1600|             emit_selectors=True,
  1601|         )
  1602|         rec["join_key"] = vt_join_key
  1603| 
  1604|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
  1605|         # keyed off this record's own label.display-backing item (view_template.name).
  1606|         # view_template.name does not exist in identity_items for any partition --
  1607|         # identity_items are built from _canonical_identity_items_from_signature(def_hash,
  1608|         # sig_final), a structured signature that explicitly strips "name="-prefixed
  1609|         # entries before hashing. Widened items list used only for this call;
  1610|         # identity_basis.items/sig_hash/join_key above are unaffected.
  1611|         vt_name_v, vt_name_q = canonicalize_str(name)
  1612|         name_key_items = identity_items + [
  1613|             make_identity_item("view_template.name", vt_name_v, vt_name_q)
  1614|         ]
  1615|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  1616|         rec["join_key_name_identity"], _vt_name_key_missing = build_join_key_from_policy(
  1617|             domain_policy=name_key_pol,
  1618|             identity_items=name_key_items,
  1619|             include_optional_items=False,
  1620|             emit_keys_used=True,
  1621|             hash_optional_items=False,
  1622|             emit_items=False,
  1623|             emit_selectors=True,
  1624|         )
  1625|         rec["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _vt_name_key_missing)
  1626| 
  1627|         rec["def_hash"] = def_hash
  1628|         rec["def_signature"] = sig_final
  1629| 
  1630|         records.append(rec)
  1631|         per_hashes.append(def_hash)
  1632|         info["debug_kept"] += 1
  1633| 
  1634|     # Finalize
  1635|     info["names"] = sorted(set(names))
  1636|     info["count"] = len(records)
  1637| 
  1638|     info["records"] = sorted(
  1639|         records,
  1640|         key=lambda r: (
  1641|             safe_str(((r.get("label", {}) or {}).get("display", ""))),
  1642|             safe_str(r.get("record_id", "")),
  1643|         ),
  1644|     )
  1645| 
  1646|     info["signature_hashes_v2"] = sorted(per_hashes_v2)
  1647|     if v2_any_blocked:
  1648|         info["hash_v2"] = None
  1649|     else:
  1650|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1651| 
  1652|     info["record_rows"] = []
  1653|     try:
  1654|         recs = info.get("records") or []
  1655|         info["record_rows"] = [{
  1656|             "record_key": safe_str(r.get("record_id", "")),
  1657|             "sig_hash":   safe_str(r.get("sig_hash", "")),
  1658|             "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
  1659|             "view_type":  safe_str(((r.get("label", {}) or {}).get("components", {}) or {}).get("view_type", "")),
  1660|         } for r in recs]
  1661|     except Exception:
  1662|         info["record_rows"] = []
  1663| 
  1664|     # PR6: deterministic degraded signaling
  1665|     degraded_reason_counts = {}
  1666| 
  1667|     try:
  1668|         if int(info.get("debug_missing_uid", 0)) > 0:
  1669|             degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
  1670|     except Exception:
  1671|         pass
  1672| 
  1673|     try:
  1674|         if int(info.get("debug_fail_read", 0)) > 0:
  1675|             degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
  1676|     except Exception:
  1677|         pass
  1678| 
  1679|     try:
  1680|         if int(info.get("debug_view_context_problem", 0)) > 0:
  1681|             for k, vv in dict(info.get("debug_view_context_reasons", {})).items():
  1682|                 key = str(k)
  1683|                 if key.endswith("_not_applicable"):
  1684|                     continue
  1685|                 degraded_reason_counts[key] = int(vv)
  1686|     except Exception:
  1687|         pass
  1688| 
  1689|     try:
  1690|         if int(info.get("debug_v2_blocked", 0)) > 0:
  1691|             degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
  1692|     except Exception:
  1693|         pass
  1694| 
  1695|     if degraded_reason_counts:
  1696|         info["_domain_status"] = "degraded"
  1697|         info["_domain_diag"] = {
  1698|             "degraded_reasons": sorted(degraded_reason_counts.keys()),
  1699|             "degraded_reason_counts": degraded_reason_counts,
  1700|         }
  1701|     else:
  1702|         info["_domain_status"] = "ok"
  1703|         info["_domain_diag"] = {}
  1704| 
  1705|     return info
  1706| 
  1707| def _build_renderings_drafting_viewtype_set():
  1708|     """
  1709|     Build the ViewType integer set for renderings/drafting.
  1710| 
  1711|     Probe-confirmed integers only:
  1712|       10 = DraftingView
  1713| 
  1714|     ThreeD is intentionally excluded because it collides with Section in
  1715|     this Revit version, and Rendering is excluded until probe evidence exists.
  1716|     """
  1717|     return frozenset({10})
  1718| 
  1719| 
  1720| _RENDERINGS_DRAFTING_VIEWTYPE_SET = _build_renderings_drafting_viewtype_set()
  1721| 
  1722| 
```
