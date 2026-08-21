# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 5 of 8
- Original line range: 1453-1870
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_diameter, _apply_family_name_override, _read_symbol_name
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
  1453| def extract_diameter(doc, ctx=None):
  1454|     _HANDLED_SHAPES = _DIAMETER_HANDLED
  1455|     DOMAIN_NAME = "dimension_types_diameter"
  1456|     EXPECTED_FAMILY = _DIAMETER_EXPECTED_FAMILY
  1457|     """
  1458|     Extract Diameter dimension types fingerprint.
  1459| 
  1460|     Args:
  1461|         doc: Revit Document
  1462|         ctx: Context dictionary
  1463| 
  1464|     Returns:
  1465|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
  1466|     """
  1467|     info = {
  1468|         "count": 0,
  1469|         "raw_count": 0,
  1470|         "records": [],
  1471|         "signature_hashes_v2": [],
  1472|         "hash_v2": None,
  1473|         "debug_v2_blocked": False,
  1474|         "debug_v2_block_reasons": {},
  1475|     }
  1476| 
  1477|     if ctx is None:
  1478|         ctx = {}
  1479| 
  1480|     if DimensionType is None:
  1481|         info["debug_v2_blocked"] = True
  1482|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
  1483|         return info
  1484| 
  1485|     try:
  1486|         all_types = _collect_dim_types(doc, ctx)
  1487|     except Exception:
  1488|         all_types = []
  1489| 
  1490|     info["raw_count"] = len(all_types)
  1491|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
  1492| 
  1493|     v2_records = []
  1494|     v2_sig_hashes = []
  1495|     _eligible_type_count = 0
  1496| 
  1497|     for d in all_types:
  1498|         try:
  1499|             type_name = get_type_display_name(d)
  1500| 
  1501|             # Exclude system built-in types with id-based labels (not user-accessible)
  1502|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
  1503|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
  1504|                 continue
  1505| 
  1506|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
  1507| 
  1508|             # Apply family-name heuristic override to detect Spot types
  1509|             shape_v, shape_family, shape_q = _apply_family_name_override(
  1510|                 d, shape_v, shape_family, shape_q, type_name
  1511|             )
  1512| 
  1513|             # Note: no shape-based filter here. Revit's DimensionStyleType enum maps
  1514|             # some Diameter types to SpotElevationFixed (integer collision in the enum).
  1515|             # Family name is the sole authoritative gate for this domain.
  1516| 
  1517|             # Exclude confirmed wrong-family types (e.g. Alignment Station Labels)
  1518|             family_name = None
  1519|             try:
  1520|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
  1521|                 if p_fam:
  1522|                     family_name = _as_string(p_fam)
  1523|                     if family_name:
  1524|                         family_name = canon_str(family_name)
  1525|             except Exception:
  1526|                 pass
  1527|             if family_name and family_name != EXPECTED_FAMILY:
  1528|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
  1529|                 continue
  1530| 
  1531|             _eligible_type_count += 1
  1532| 
  1533|             # --- Read core identity fields ---
  1534| 
  1535|             # Unit format info
  1536|             (unit_format_id_v, unit_format_id_q,
  1537|              rounding_v, rounding_q,
  1538|              accuracy_v, accuracy_q,
  1539|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
  1540| 
  1541|             # Tick mark sig hash
  1542|             tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)
  1543| 
  1544|             # Center marks (radial-family specific)
  1545|             center_marks_v, center_marks_q = (None, ITEM_Q_MISSING)
  1546|             try:
  1547|                 p_cm = first_param(d, ui_names=["Center Marks"])
  1548|                 cm_int = _as_int(p_cm) if p_cm is not None else None
  1549|                 if cm_int is not None:
  1550|                     center_marks_v, center_marks_q = canonicalize_str(safe_str(cm_int))
  1551|                     if center_marks_v is None:
  1552|                         center_marks_q = ITEM_Q_UNREADABLE
  1553|             except Exception:
  1554|                 center_marks_v, center_marks_q = (None, ITEM_Q_UNREADABLE)
  1555| 
  1556|             # Center mark size (radial-family specific), stored in feet, convert to inches
  1557|             center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
  1558|             try:
  1559|                 p_cms = first_param(d, ui_names=["Center Mark Size"])
  1560|                 cms_ft = _as_double(p_cms) if p_cms is not None else None
  1561|                 if cms_ft is not None:
  1562|                     center_mark_size_v, center_mark_size_q = canonicalize_float(_fmt_in_from_ft(cms_ft))
  1563|                 else:
  1564|                     center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
  1565|             except Exception:
  1566|                 center_mark_size_v, center_mark_size_q = (None, ITEM_Q_UNREADABLE)
  1567| 
  1568|             # Diameter symbol location
  1569|             diameter_symbol_location_v, diameter_symbol_location_q = (None, ITEM_Q_MISSING)
  1570|             try:
  1571|                 p_dsl = first_param(d, ui_names=["Diameter Symbol Location", "Symbol Location"])
  1572|                 dsl_raw = _as_string(p_dsl) if p_dsl is not None else None
  1573|                 diameter_symbol_location_v, diameter_symbol_location_q = canonicalize_str_allow_empty(dsl_raw)
  1574|             except Exception:
  1575|                 diameter_symbol_location_v, diameter_symbol_location_q = (None, ITEM_Q_UNREADABLE)
  1576| 
  1577|             # Diameter symbol text
  1578|             diameter_symbol_text_v, diameter_symbol_text_q = (None, ITEM_Q_MISSING)
  1579|             try:
  1580|                 p_dst = first_param(d, ui_names=["Diameter Symbol Text"])
  1581|                 dst_raw = _as_string(p_dst) if p_dst is not None else None
  1582|                 diameter_symbol_text_v, diameter_symbol_text_q = canonicalize_str_allow_empty(dst_raw)
  1583|             except Exception:
  1584|                 diameter_symbol_text_v, diameter_symbol_text_q = (None, ITEM_Q_UNREADABLE)
  1585| 
  1586|             # --- Area 7 §2/§4c: leader config + tick weight (angular/diameter/linear/radial) ---
  1587|             leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
  1588|                 d, ctx, ui_names=["Leader Tick Mark"]
  1589|             )
  1590|             leader_type_v, leader_type_q = (None, ITEM_Q_MISSING)
  1591|             try:
  1592|                 p_lt = first_param(d, ui_names=["Leader Type"])
  1593|                 lt_raw = _as_value_string(p_lt) if p_lt is not None else None
  1594|                 leader_type_v, leader_type_q = canonicalize_str(lt_raw)
  1595|             except Exception:
  1596|                 leader_type_v, leader_type_q = (None, ITEM_Q_UNREADABLE)
  1597|             show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_MISSING)
  1598|             try:
  1599|                 p_slwtm = first_param(d, ui_names=["Show Leader When Text Moves"])
  1600|                 slwtm_raw = _as_value_string(p_slwtm) if p_slwtm is not None else None
  1601|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = canonicalize_str(slwtm_raw)
  1602|             except Exception:
  1603|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_UNREADABLE)
  1604|             tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_MISSING)
  1605|             try:
  1606|                 p_tmlw = first_param(d, ui_names=["Tick Mark Line Weight"])
  1607|                 tmlw_int = _as_int(p_tmlw) if p_tmlw is not None else None
  1608|                 tick_mark_line_weight_v, tick_mark_line_weight_q = canonicalize_int(tmlw_int)
  1609|             except Exception:
  1610|                 tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_UNREADABLE)
  1611| 
  1612|             # --- Area 7 §7: Text Offset (Angular/Diameter/Linear/Radial per probe) ---
  1613|             text_offset_v, text_offset_q = (None, ITEM_Q_MISSING)
  1614|             try:
  1615|                 p_toff = first_param(d, ui_names=["Text Offset"])
  1616|                 toff_ft = _as_double(p_toff) if p_toff is not None else None
  1617|                 text_offset_v, text_offset_q = canonicalize_float(_fmt_in_from_ft(toff_ft))
  1618|             except Exception:
  1619|                 text_offset_v, text_offset_q = (None, ITEM_Q_UNREADABLE)
  1620| 
  1621|             # --- Build identity items ---
  1622|             core_items = [
  1623|                 make_identity_item("dim_type.shape", shape_v, shape_q),
  1624|                 make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
  1625|                 make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
  1626|                 make_identity_item("dim_type.center_marks", center_marks_v, center_marks_q),
  1627|                 make_identity_item("dim_type.center_mark_size", center_mark_size_v, center_mark_size_q),
  1628|                 make_identity_item("dim_type.diameter_symbol_location", diameter_symbol_location_v, diameter_symbol_location_q),
  1629|                 make_identity_item("dim_type.diameter_symbol_text", diameter_symbol_text_v, diameter_symbol_text_q),
  1630|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
  1631|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
  1632|                 make_identity_item("dim_type.leader_tick_mark_sig_hash", leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q),
  1633|                 make_identity_item("dim_type.leader_type", leader_type_v, leader_type_q),
  1634|                 make_identity_item("dim_type.show_leader_when_text_moves", show_leader_when_text_moves_v, show_leader_when_text_moves_q),
  1635|                 make_identity_item("dim_type.tick_mark_line_weight", tick_mark_line_weight_v, tick_mark_line_weight_q),
  1636|                 make_identity_item("dim_type.text_offset_in", text_offset_v, text_offset_q),
  1637|             ]
  1638| 
  1639|             text_items = _build_text_appearance_items(d)
  1640|             alt_units_items = _build_alternate_units_items(d)
  1641|             all_items = core_items + text_items + alt_units_items
  1642| 
  1643|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
  1644| 
  1645|             # Required qualities for blocking
  1646|             # diameter_symbol_location, diameter_symbol_text are optional enrichment — not blocking
  1647|             required_qs = [
  1648|                 shape_q,
  1649|                 accuracy_q,
  1650|                 center_marks_q,
  1651|                 center_mark_size_q,
  1652|                 unit_format_id_q,
  1653|             ]
  1654|             # text/appearance fields, and all Area 7 additions, are cross-family alignment /
  1655|             # non-blocking enrichment — not blocking
  1656| 
  1657|             blocked = any(q != ITEM_Q_OK for q in required_qs)
  1658| 
  1659|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
  1660|                 "dim_type.tick_mark_sig_hash",
  1661|                 "dim_type.leader_tick_mark_sig_hash",
  1662|             })
  1663| 
  1664|             status_reasons = []
  1665|             for it in identity_items:
  1666|                 q = it.get("q")
  1667|                 k = it.get("k", "")
  1668|                 if q == ITEM_Q_OK:
  1669|                     continue
  1670|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
  1671|                     continue
  1672|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
  1673| 
  1674|             if blocked:
  1675|                 status = STATUS_BLOCKED
  1676|             elif status_reasons:
  1677|                 status = STATUS_DEGRADED
  1678|             else:
  1679|                 status = STATUS_OK
  1680| 
  1681|             preimage = serialize_identity_items(identity_items)
  1682|             sig_hash = None if blocked else make_hash(preimage)
  1683| 
  1684|             try:
  1685|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
  1686|             except Exception:
  1687|                 type_id_int = None
  1688| 
  1689|             try:
  1690|                 uid_raw = getattr(d, "UniqueId", None)
  1691|             except Exception:
  1692|                 uid_raw = None
  1693| 
  1694|             label_str = type_name
  1695|             rec_v2 = build_record_v2(
  1696|                 domain=DOMAIN_NAME,
  1697|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
  1698|                 status=status,
  1699|                 status_reasons=sorted(set(status_reasons)),
  1700|                 sig_hash=sig_hash,
  1701|                 identity_items=identity_items,
  1702|                 required_qs=tuple(required_qs),
  1703|                 label={
  1704|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
  1705|                     "quality": "human" if label_str else "placeholder_missing",
  1706|                     "provenance": "revit.DimensionType.params",
  1707|                 },
  1708|             )
  1709|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
  1710|             rec_v2["is_purgeable"] = _ip
  1711|             rec_v2["is_purgeable_q"] = _ip_q
  1712|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
  1713| 
  1714|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  1715|             rec_v2["join_key"], _missing = build_join_key_from_policy(
  1716|                 domain_policy=pol,
  1717|                 identity_items=identity_items,
  1718|                 include_optional_items=False,
  1719|                 emit_keys_used=True,
  1720|                 hash_optional_items=False,
  1721|                 emit_items=False,
  1722|                 emit_selectors=True,
  1723|             )
  1724| 
  1725|             # Canonical Name Identity Projection (PR1): second, independent join_hash
  1726|             # variant keyed off this record's own label.display-backing item
  1727|             # (dim_type.name). dim_type.name does not exist anywhere in this file --
  1728|             # type_name/label_str feeds label.display only. Widened items list used
  1729|             # only for this call; identity_basis.items/sig_hash/join_key above are
  1730|             # unaffected. (dimension_types_spot_coordinate/spot_elevation are excluded
  1731|             # from the name-key policy entirely -- their only other name-shaped item,
  1732|             # dim_type.symbol_name, names a different, referenced tick-mark/leader
  1733|             # symbol element, not this record's own label.)
  1734|             dt_name_v, dt_name_q = canonicalize_str(type_name)
  1735|             name_key_items = identity_items + [
  1736|                 make_identity_item("dim_type.name", dt_name_v, dt_name_q)
  1737|             ]
  1738|             name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  1739|             rec_v2["join_key_name_identity"], _name_key_missing = build_join_key_from_policy(
  1740|                 domain_policy=name_key_pol,
  1741|                 identity_items=name_key_items,
  1742|                 include_optional_items=False,
  1743|                 emit_keys_used=True,
  1744|                 hash_optional_items=False,
  1745|                 emit_items=False,
  1746|                 emit_selectors=True,
  1747|             )
  1748|             rec_v2["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _name_key_missing)
  1749| 
  1750|             coordination_items = [
  1751|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
  1752|             ]
  1753| 
  1754|             unknown_items = []
  1755|             try:
  1756|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
  1757|             except Exception:
  1758|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  1759|             try:
  1760|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
  1761|             except Exception:
  1762|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  1763|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
  1764|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
  1765| 
  1766|             rec_v2["phase2"] = {
  1767|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  1768|                 "grouping_basis": "phase2.hypothesis",
  1769|                 "cosmetic_items": phase2_sorted_items([]),
  1770|                 "coordination_items": phase2_sorted_items(coordination_items),
  1771|                 "unknown_items": phase2_sorted_items(unknown_items),
  1772|             }
  1773| 
  1774|             if sig_hash:
  1775|                 v2_sig_hashes.append(sig_hash)
  1776|             v2_records.append(rec_v2)
  1777| 
  1778|         except Exception:
  1779|             continue  # fail-soft per record
  1780| 
  1781|     _total_type_count = _eligible_type_count
  1782|     for rec in v2_records:
  1783|         try:
  1784|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
  1785|             rec["is_sole_type_in_category_q"] = "ok"
  1786|         except Exception:
  1787|             rec["is_sole_type_in_category"] = None
  1788|             rec["is_sole_type_in_category_q"] = "unreadable"
  1789| 
  1790|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
  1791|     info["count"] = len(v2_records)
  1792|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  1793| 
  1794|     if v2_sig_hashes:
  1795|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1796|         info["debug_v2_blocked"] = False
  1797|     else:
  1798|         info["hash_v2"] = None
  1799|         info["debug_v2_blocked"] = True
  1800|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
  1801| 
  1802|     return info
  1803| 
  1804| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
  1805|     """
  1806|     Heuristic override: use FamilyName prefix to more precisely classify Spot types.
  1807|     Returns updated (shape_v, shape_family, shape_q).
  1808|     """
  1809|     try:
  1810|         family_name = getattr(d, "FamilyName", None)
  1811|         basis = family_name if family_name else type_name
  1812|         bn_l = safe_str(basis).strip().lower()
  1813| 
  1814|         if bn_l.startswith("spot slopes"):
  1815|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
  1816|         elif bn_l.startswith("spot elevations"):
  1817|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
  1818|         elif bn_l.startswith("spot coordinates"):
  1819|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
  1820|     except Exception:
  1821|         pass
  1822|     return (shape_v, shape_family, shape_q)
  1823| 
  1824| 
  1825| def _read_symbol_name(d, doc):
  1826|     """
  1827|     Try to read the "Symbol" parameter that references a loaded family.
  1828|     If ElementId > 0, resolve element and return its name directly.
  1829|     Returns (symbol_name_v, symbol_name_q).
  1830|     """
  1831|     try:
  1832|         p_sym = first_param(d, ui_names=["Symbol"])
  1833|         if p_sym is None:
  1834|             return (None, ITEM_Q_MISSING)
  1835| 
  1836|         if not getattr(p_sym, "HasValue", False):
  1837|             return (None, ITEM_Q_MISSING)
  1838| 
  1839|         eid = None
  1840|         try:
  1841|             eid = p_sym.AsElementId()
  1842|         except Exception:
  1843|             return (None, ITEM_Q_UNREADABLE)
  1844| 
  1845|         if eid is None or getattr(eid, "IntegerValue", -1) <= 0:
  1846|             return (None, ITEM_Q_MISSING)
  1847| 
  1848|         sym_elem = None
  1849|         try:
  1850|             sym_elem = doc.GetElement(eid)
  1851|         except Exception:
  1852|             return (None, ITEM_Q_UNREADABLE)
  1853| 
  1854|         if sym_elem is None:
  1855|             return (None, ITEM_Q_MISSING)
  1856| 
  1857|         sym_name = None
  1858|         try:
  1859|             sym_name = getattr(sym_elem, "Name", None)
  1860|         except Exception:
  1861|             pass
  1862| 
  1863|         if sym_name:
  1864|             return canonicalize_str(str(sym_name))
  1865|         return (None, ITEM_Q_MISSING)
  1866| 
  1867|     except Exception:
  1868|         return (None, ITEM_Q_UNREADABLE)
  1869| 
  1870| 
```
