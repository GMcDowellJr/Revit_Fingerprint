# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 7 of 8
- Original line range: 1440-1839
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_model, extract_model._phase2_build_phase2
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: extract_model
- Ends inside symbol: extract_model

```
  1440|                                 oy = float(v2)
  1441|                                 break
  1442|                             except Exception:
  1443|                                 continue
  1444| 
  1445|                     if origin_kind is None:
  1446|                         semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1447|                     else:
  1448|                         v_kind, q_kind = canonicalize_str(origin_kind)
  1449|                         semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": v_kind, "q": q_kind})
  1450| 
  1451|                         if origin_kind == "uv":
  1452|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.u".format(idx), ox)
  1453|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.v".format(idx), oy)
  1454|                         else:
  1455|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.x".format(idx), ox)
  1456|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.y".format(idx), oy)
  1457| 
  1458|                     try:
  1459|                         _phase2_add_float(semantic, "fill_pattern.grid[{}].offset".format(idx), float(getattr(g, "Offset")))
  1460|                     except Exception:
  1461|                         semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1462| 
  1463|                     try:
  1464|                         _phase2_add_float(semantic, "fill_pattern.grid[{}].shift".format(idx), float(getattr(g, "Shift")))
  1465|                     except Exception:
  1466|                         semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1467| 
  1468|         # Derived structural identity helper for Phase-2:
  1469|         # Collapse all per-grid semantic items into a single hash so join-key discovery
  1470|         # can treat the grid bundle as one "field" without losing the detailed items.
  1471|         #
  1472|         # IMPORTANT: grid order is identity-significant; do NOT sort the preimage.
  1473|         try:
  1474|             grid_like = []
  1475|             for it in (semantic or []):
  1476|                 k = safe_str(it.get("k", ""))
  1477|                 if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
  1478|                     # Stable preimage: include k/q/v so unreadables affect the hash deterministically
  1479|                     grid_like.append("k={}|q={}|v={}".format(
  1480|                         safe_str(it.get("k", "")),
  1481|                         safe_str(it.get("q", "")),
  1482|                         safe_str(it.get("v", "")),
  1483|                     ))
  1484|             grids_def_hash = make_hash(grid_like) if grid_like else None
  1485|         except Exception:
  1486|             grids_def_hash = None
  1487| 
  1488|         if grids_def_hash:
  1489|             semantic.append({"k": "fill_pattern.grids_def_hash", "v": grids_def_hash, "q": ITEM_Q_OK})
  1490|         else:
  1491|             # If we can't compute it, make the failure explicit (but keep it out of identity)
  1492|             semantic.append({"k": "fill_pattern.grids_def_hash", "v": None, "q": ITEM_Q_UNREADABLE})
  1493| 
  1494|         # Phase-2 bloat control:
  1495|         # The full grid definition is already present in identity_basis.items (for sig_hash reproducibility).
  1496|         # Avoid duplicating per-grid items in phase2.semantic_items; keep only pointer + small scalars.
  1497|         semantic_reduced = []
  1498|         for it in (semantic or []):
  1499|             k = safe_str(it.get("k", ""))
  1500|             if k.startswith("fill_pattern.grid["):
  1501|                 continue
  1502|             semantic_reduced.append(it)
  1503| 
  1504|         return {
  1505|             "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  1506|             "grouping_basis": "phase2.hypothesis",
  1507|             "semantic_items": phase2_sorted_items(semantic_reduced),
  1508|             "cosmetic_items": phase2_sorted_items(cosmetic),
  1509|             "coordination_items": phase2_sorted_items(coordination),
  1510|             "unknown_items": phase2_sorted_items(unknown),
  1511|         }
  1512| 
  1513|     records = []
  1514|     per_hashes = []
  1515|     per_hashes_v2 = []
  1516|     v2_records = []
  1517|     v2_sig_hashes = []
  1518|     names = []
  1519|     uid_to_hash_v2 = {}
  1520|     id_to_value = {}
  1521|     uid_to_hash = {}
  1522| 
  1523|     for e in col:
  1524|         info["debug_total_elements"] += 1
  1525| 
  1526|         name = canon_str(getattr(e, "Name", None))
  1527|         if not name:
  1528|             info["debug_skipped_no_name"] += 1
  1529|             continue
  1530| 
  1531|         uid = getattr(e, "UniqueId", "") or ""
  1532| 
  1533|         # Always keep the element, even if we can't read its FillPattern
  1534|         fp = None
  1535|         try:
  1536|             fp = e.GetFillPattern()
  1537|         except Exception as e:
  1538|             fp = None
  1539| 
  1540|         # Filter: only process patterns matching this domain's target
  1541|         if fp is not None:
  1542|             try:
  1543|                 _fp_target_int = int(fp.Target)
  1544|             except Exception:
  1545|                 _fp_target_int = -1
  1546|             if _fp_target_int != _TARGET_INT:
  1547|                 info["debug_skipped_wrong_target"] += 1
  1548|                 continue
  1549| 
  1550|         # Filter: skip solid fills — system defaults, ungoverned
  1551|         if fp is not None:
  1552|             try:
  1553|                 if fp.IsSolidFill:
  1554|                     id_to_value[safe_str(e.Id.IntegerValue)] = FILL_PATTERN_SYMBOLIC_SOLID
  1555|                     continue
  1556|             except Exception:
  1557|                 pass  # if unreadable, proceed and let field-level q handle it
  1558| 
  1559|         names.append(name)
  1560| 
  1561|         # -------------------------
  1562|         # Legacy signature (UNCHANGED meaning)
  1563|         # -------------------------
  1564|         if fp is None:
  1565|             info["debug_fail_getfillpattern"] += 1
  1566|             sig = [
  1567|                 f"is_solid={S_MISSING}",
  1568|                 f"target={_TARGET_NAME}",
  1569|                 f"grid_count={S_MISSING}",
  1570|                 f"grid[000].unreadable={S_MISSING}",
  1571|                 "error=GetFillPatternFailed",
  1572|             ]
  1573|         else:
  1574|             is_solid = None
  1575|             try: is_solid = fp.IsSolidFill
  1576|             except Exception as e: pass
  1577| 
  1578|             gc = None
  1579|             try: gc = fp.GridCount
  1580|             except Exception as e: pass
  1581| 
  1582|             sig = [
  1583|                 "is_solid={}".format(canon_str(is_solid)),
  1584|                 "target={}".format(_TARGET_NAME),
  1585|                 "grid_count={}".format(canon_str(gc)),
  1586|             ]
  1587| 
  1588|             if gc:
  1589|                 try:
  1590|                     for i in range(int(gc)):
  1591|                         sig.extend(grid_sig(fp, i))
  1592|                 except Exception as e:
  1593|                     info["debug_fail_grid_read"] += 1
  1594|                     sig.append("error=GridLoopFailed")
  1595| 
  1596|         sig_sorted = sorted(sig)
  1597|         def_hash = make_hash(sig_sorted)
  1598|         if uid:
  1599|             uid_to_hash[uid] = def_hash
  1600| 
  1601|         # -------------------------
  1602|         # v2 (contract semantic): NO names; block on unreadable/missing
  1603|         # -------------------------
  1604|         v2_ok = True
  1605|         v2_reason = None
  1606|         sig_v2 = []
  1607| 
  1608|         if fp is None:
  1609|             v2_ok = False
  1610|             v2_reason = "get_fillpattern_failed"
  1611|         else:
  1612|             # is_solid: require bool-coercible
  1613|             try:
  1614|                 is_solid_v2 = fp.IsSolidFill
  1615|             except Exception as e:
  1616|                 v2_ok = False
  1617|                 v2_reason = "is_solid_unreadable"
  1618| 
  1619|             if v2_ok:
  1620|                 # grid_count: require int (0 allowed)
  1621|                 try:
  1622|                     gc_v2 = fp.GridCount
  1623|                     gc_i = int(gc_v2)
  1624|                 except Exception as e:
  1625|                     v2_ok = False
  1626|                     v2_reason = "grid_count_unreadable"
  1627| 
  1628|             if v2_ok:
  1629|                 sig_v2.append("target={}".format(_TARGET_NAME))
  1630|                 sig_v2.append("is_solid={}".format(canon_str(bool(is_solid_v2))))
  1631|                 sig_v2.append("grid_count={}".format(canon_str(gc_i)))
  1632| 
  1633|                 # grids: every grid must be readable
  1634|                 if gc_i:
  1635|                     for i in range(gc_i):
  1636|                         ok, parts, reason = _grid_sig_v2(fp, i)
  1637|                         if not ok:
  1638|                             v2_ok = False
  1639|                             v2_reason = reason
  1640|                             break
  1641|                         sig_v2.extend(parts)
  1642| 
  1643|         if v2_ok:
  1644|             # keep deterministic: sort like legacy (order-insensitive at record level)
  1645|             sig_v2_sorted = sorted(sig_v2)
  1646|             def_hash_v2 = make_hash(sig_v2_sorted)
  1647|             per_hashes_v2.append(def_hash_v2)
  1648|             if uid:
  1649|                 uid_to_hash_v2[uid] = def_hash_v2
  1650|         else:
  1651|             _bump_v2_reason(v2_reason or "unknown")
  1652| 
  1653|         phase2_payload = _phase2_build_phase2(
  1654|             name=name,
  1655|             uid=uid,
  1656|             elem_id_str=safe_str(e.Id.IntegerValue),
  1657|             fp=fp,
  1658|             elem=e,
  1659|         )
  1660| 
  1661|         rec = {
  1662|             "id": safe_str(e.Id.IntegerValue),
  1663|             "uid": uid,
  1664|             "name": name,          # metadata only
  1665|             "def_hash": def_hash,  # hashed legacy definition
  1666|         }
  1667| 
  1668|         if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
  1669|             rec["def_signature"] = sig_sorted
  1670| 
  1671|         status_v2 = STATUS_OK
  1672|         status_reasons_v2 = []
  1673|         
  1674|         identity_items_v2 = []
  1675| 
  1676|         # NOTE: name/uid/elem_id are labels/metadata and MUST NOT participate in identity.
  1677|         # Name is carried in label{} and in the phase2 cosmetic surface.
  1678| 
  1679|         if fp is None:
  1680|             gc_v, gc_q = (None, ITEM_Q_UNREADABLE)
  1681|             gc_i = None
  1682|         else:
  1683|             try:
  1684|                 gc_i = int(fp.GridCount)
  1685|                 gc_v, gc_q = canonicalize_int(gc_i)
  1686|             except Exception:
  1687|                 gc_i = None
  1688|                 gc_v, gc_q = (None, ITEM_Q_UNREADABLE)
  1689| 
  1690|         # target is always _TARGET_NAME / ITEM_Q_OK - not part of required_qs check
  1691|         identity_items_v2.append(make_identity_item("fill_pattern.target", _TARGET_NAME, ITEM_Q_OK))
  1692|         # is_solid is a filter criterion, not an identity field — omitted from identity_items
  1693|         identity_items_v2.append(make_identity_item("fill_pattern.grid_count", gc_v, gc_q))
  1694|         required_qs = [gc_q]
  1695| 
  1696|         if gc_i and gc_i > 0:
  1697|             for i in range(gc_i):
  1698|                 idx = "{:03d}".format(int(i))
  1699|                 g = _phase2_try_get_grid(fp, i)
  1700|                 if g is None:
  1701|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", None, ITEM_Q_UNREADABLE))
  1702|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", None, ITEM_Q_UNREADABLE))
  1703|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", None, ITEM_Q_UNREADABLE))
  1704|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", None, ITEM_Q_UNREADABLE))
  1705|                     required_qs.extend([ITEM_Q_UNREADABLE] * 4)
  1706|                     continue
  1707| 
  1708|                 # angle / offset / shift
  1709|                 try:
  1710|                     ang_v, ang_q = canonicalize_float(getattr(g, "Angle", None))
  1711|                 except Exception:
  1712|                     ang_v, ang_q = (None, ITEM_Q_UNREADABLE)
  1713| 
  1714|                 try:
  1715|                     off_v, off_q = canonicalize_float(getattr(g, "Offset", None))
  1716|                 except Exception:
  1717|                     off_v, off_q = (None, ITEM_Q_UNREADABLE)
  1718| 
  1719|                 try:
  1720|                     sh_v, sh_q = canonicalize_float(getattr(g, "Shift", None))
  1721|                 except Exception:
  1722|                     sh_v, sh_q = (None, ITEM_Q_UNREADABLE)
  1723| 
  1724|                 # origin: explicit kind + conditional leaf members (uv vs xy)
  1725|                 origin_kind = None
  1726|                 a = b = None
  1727| 
  1728|                 # UV origin
  1729|                 try:
  1730|                     o = getattr(g, "Origin", None)
  1731|                     u = getattr(o, "U", None)
  1732|                     v = getattr(o, "V", None)
  1733|                     if u is not None and v is not None:
  1734|                         origin_kind = "uv"
  1735|                         a = u
  1736|                         b = v
  1737|                 except Exception:
  1738|                     pass
  1739| 
  1740|                 # XY origin
  1741|                 if origin_kind is None:
  1742|                     try:
  1743|                         o = getattr(g, "Origin", None)
  1744|                         x = getattr(o, "X", None)
  1745|                         y = getattr(o, "Y", None)
  1746|                         if x is not None and y is not None:
  1747|                             origin_kind = "xy"
  1748|                             a = x
  1749|                             b = y
  1750|                     except Exception:
  1751|                         pass
  1752| 
  1753|                 # Scalar origin props (treated as uv)
  1754|                 if origin_kind is None:
  1755|                     for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
  1756|                         try:
  1757|                             u2 = getattr(g, u_name)
  1758|                             v2 = getattr(g, v_name)
  1759|                             if u2 is None or v2 is None:
  1760|                                 continue
  1761|                             origin_kind = "uv"
  1762|                             a = u2
  1763|                             b = v2
  1764|                             break
  1765|                         except Exception:
  1766|                             continue
  1767| 
  1768|                 if origin_kind is None:
  1769|                     ok_kind = (None, ITEM_Q_UNREADABLE)
  1770|                 else:
  1771|                     ok_kind = canonicalize_str(origin_kind)
  1772| 
  1773|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", ang_v, ang_q))
  1774|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", ok_kind[0], ok_kind[1]))
  1775| 
  1776|                 if origin_kind == "uv":
  1777|                     try:
  1778|                         ou_v, ou_q = canonicalize_float(a)
  1779|                     except Exception:
  1780|                         ou_v, ou_q = (None, ITEM_Q_UNREADABLE)
  1781|                     try:
  1782|                         ov_v, ov_q = canonicalize_float(b)
  1783|                     except Exception:
  1784|                         ov_v, ov_q = (None, ITEM_Q_UNREADABLE)
  1785| 
  1786|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.u", ou_v, ou_q))
  1787|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.v", ov_v, ov_q))
  1788|                     required_qs.extend([ang_q, ok_kind[1], ou_q, ov_q, off_q, sh_q])
  1789| 
  1790|                 elif origin_kind == "xy":
  1791|                     try:
  1792|                         ox_v, ox_q = canonicalize_float(a)
  1793|                     except Exception:
  1794|                         ox_v, ox_q = (None, ITEM_Q_UNREADABLE)
  1795|                     try:
  1796|                         oy_v, oy_q = canonicalize_float(b)
  1797|                     except Exception:
  1798|                         oy_v, oy_q = (None, ITEM_Q_UNREADABLE)
  1799| 
  1800|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.x", ox_v, ox_q))
  1801|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.y", oy_v, oy_q))
  1802|                     required_qs.extend([ang_q, ok_kind[1], ox_q, oy_q, off_q, sh_q])
  1803| 
  1804|                 else:
  1805|                     # kind unreadable => identity blocked; no leaf members
  1806|                     required_qs.extend([ang_q, ok_kind[1], off_q, sh_q])
  1807| 
  1808|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", off_v, off_q))
  1809|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", sh_v, sh_q))
  1810| 
  1811|         # Derived join helper (policy-required key): capture the entire grid definition bundle.
  1812|         # Canonical evidence source is identity_basis.items; selectors reference subsets.
  1813|         # Keep preimage order-sensitive so grid index order remains identity-significant.
  1814|         try:
  1815|             grid_like = []
  1816|             for it in (identity_items_v2 or []):
  1817|                 k = safe_str(it.get("k", ""))
  1818|                 if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
  1819|                     grid_like.append("k={}|q={}|v={}".format(
  1820|                         safe_str(it.get("k", "")),
  1821|                         safe_str(it.get("q", "")),
  1822|                         safe_str(it.get("v", "")),
  1823|                     ))
  1824|             grids_def_hash_v, grids_def_hash_q = (
  1825|                 (make_hash(grid_like), ITEM_Q_OK) if grid_like else (None, ITEM_Q_UNREADABLE)
  1826|             )
  1827|         except Exception:
  1828|             grids_def_hash_v, grids_def_hash_q = (None, ITEM_Q_UNREADABLE)
  1829| 
  1830|         identity_items_v2.append(
  1831|             make_identity_item("fill_pattern.grids_def_hash", grids_def_hash_v, grids_def_hash_q)
  1832|         )
  1833| 
  1834|         if any(q != ITEM_Q_OK for q in required_qs):
  1835|             status_v2 = STATUS_BLOCKED
  1836|             status_reasons_v2.append("required_identity_not_ok")
  1837| 
  1838|         identity_items_v2_sorted = sorted(identity_items_v2, key=lambda d: str(d.get("k","")))
  1839|         sig_preimage_v2 = serialize_identity_items(identity_items_v2_sorted)
```
