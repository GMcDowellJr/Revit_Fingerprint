# Chunk of domains/view_templates.py

- Source relative path: `domains/view_templates.py`
- Chunk: 5 of 6
- Original line range: 1723-2144
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_renderings_drafting, extract_renderings_drafting._v2_block, _is_schedule_view
- Source SHA-256: ca478c676990e318341a80d987cc318a4531ef7d17b52cb5fd1b41c67678296d
- Starts inside symbol: no
- Ends inside symbol: no

```
  1723| def extract_renderings_drafting(doc, ctx=None):
  1724|     DOMAIN_NAME = "view_templates_renderings_drafting"
  1725|     DOMAIN_VIEWTYPE_SET = _RENDERINGS_DRAFTING_VIEWTYPE_SET
  1726|     """
  1727|     Extract view templates fingerprint - 3D Views and Drafting Views only.
  1728| 
  1729|     Per-template signature: include flags + phase filter hash + filter stack.
  1730|     No category-override iteration (VCO domain handles that separately).
  1731| 
  1732|     Args:
  1733|         doc: Revit document
  1734|         ctx: context dict with mappings from other domains
  1735| 
  1736|     Returns:
  1737|         Dictionary with count, hash_v2, records, record_rows, and debug counters
  1738|     """
  1739|     info = {
  1740|         "count": 0,
  1741|         "raw_count": 0,
  1742|         "names": [],
  1743|         "records": [],
  1744| 
  1745|         # debug counters
  1746|         "debug_not_template": 0,
  1747|         "debug_missing_name": 0,
  1748|         "debug_missing_uid": 0,
  1749|         "debug_fail_read": 0,
  1750|         "debug_kept": 0,
  1751|         "debug_view_type_filtered": 0,
  1752| 
  1753|         # v2 surfaces
  1754|         "hash_v2": None,
  1755|         "signature_hashes_v2": [],
  1756|         "debug_v2_blocked": False,
  1757|         "debug_v2_block_reasons": {},
  1758|         # PR6: deterministic degraded signaling
  1759|         "debug_view_context_problem": 0,
  1760|         "debug_view_context_reasons": {},
  1761|         "debug_collect_types_failed": 0,
  1762|     }
  1763| 
  1764|     ctx_map = ctx or {}
  1765| 
  1766|     try:
  1767|         require_domain(ctx_map.get("_domains", {}), "phase_filters")
  1768|         require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
  1769|     except Blocked as b:
  1770|         info["debug_v2_blocked"] = True
  1771|         info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
  1772|         info["count"] = 0
  1773|         info["records"] = []
  1774|         info["hash_v2"] = None
  1775|         return info
  1776| 
  1777|     phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
  1778|     phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
  1779|     view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})
  1780| 
  1781|     try:
  1782|         col = list(
  1783|             collect_instances(
  1784|                 doc,
  1785|                 of_class=View,
  1786|                 require_unique_id=True,
  1787|                 cctx=(ctx or {}).get("_collect") if ctx is not None else None,
  1788|                 cache_key=_VIEW_INSTANCES_CACHE_KEY,
  1789|             )
  1790|         )
  1791|     except Exception as e:
  1792|         info["debug_collect_types_failed"] += 1
  1793|         info["_domain_status"] = "degraded"
  1794|         info["_domain_diag"] = {
  1795|             "degraded_reasons": ["collect_types_failed"],
  1796|             "degraded_reason_counts": {"collect_types_failed": 1},
  1797|             "error": str(e),
  1798|         }
  1799|         return info
  1800| 
  1801|     info["raw_count"] = len(col)
  1802| 
  1803|     names = []
  1804|     records = []
  1805|     per_hashes = []
  1806|     per_hashes_v2 = []
  1807|     v2_any_blocked = False
  1808| 
  1809|     def _v2_block(reason):
  1810|         nonlocal v2_any_blocked
  1811|         v2_any_blocked = True
  1812|         info["debug_v2_blocked"] += 1
  1813|         try:
  1814|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
  1815|         except Exception:
  1816|             pass
  1817| 
  1818|     for v in col:
  1819|         try:
  1820|             is_template = v.IsTemplate
  1821|         except Exception:
  1822|             is_template = False
  1823| 
  1824|         if not is_template:
  1825|             info["debug_not_template"] += 1
  1826|             continue
  1827| 
  1828|         # Integer ViewType filter (CPython3 returns int string from enum)
  1829|         try:
  1830|             vt_int = int(v.ViewType)
  1831|         except Exception:
  1832|             vt_int = None
  1833|         if vt_int not in DOMAIN_VIEWTYPE_SET:
  1834|             info["debug_view_type_filtered"] += 1
  1835|             continue
  1836| 
  1837|         name = canon_str(getattr(v, "Name", None))
  1838|         if not name:
  1839|             info["debug_missing_name"] += 1
  1840|             name = S_MISSING
  1841|         names.append(name)
  1842| 
  1843|         uid = None
  1844|         try:
  1845|             uid = canon_str(getattr(v, "UniqueId", None))
  1846|         except Exception:
  1847|             uid = None
  1848| 
  1849|         if not uid:
  1850|             info["debug_missing_uid"] += 1
  1851| 
  1852|         # PR6: view-scoped context snapshot
  1853|         try:
  1854|             dv = (ctx or {}).get("_doc_view") if ctx is not None else None
  1855|             if dv is not None:
  1856|                 vi = dv.view_info(v, source="HOST")
  1857|                 if vi.reasons:
  1858|                     info["debug_view_context_problem"] += 1
  1859|                     for r in vi.reasons:
  1860|                         info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
  1861|         except Exception:
  1862|             info["debug_view_context_problem"] += 1
  1863|             info["debug_view_context_reasons"]["view_context_unreadable"] = (
  1864|                 info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
  1865|             )
  1866| 
  1867|         v2_ok = True
  1868|         sig_v2 = []
  1869|         sig = []
  1870| 
  1871|         # Template-controlled parameters ("Include" surface)
  1872|         try:
  1873|             tpl_ids = v.GetTemplateParameterIds() or []
  1874|             tpl_bips = set(
  1875|                 pid.IntegerValue for pid in tpl_ids
  1876|                 if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
  1877|             )
  1878|         except Exception:
  1879|             tpl_ids = []
  1880|             tpl_bips = set()
  1881| 
  1882|         non_ctrl_bips = _non_ctrl_bips_from_view(v)
  1883|         info["debug_non_ctrl_bips_count"] = len(non_ctrl_bips)
  1884| 
  1885|         # Common include flags
  1886|         try:
  1887|             sig.append("include_phase_filter={}".format(_is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")))
  1888|         except Exception:
  1889|             sig.append("include_phase_filter=False")
  1890| 
  1891|         try:
  1892|             sig.append("include_filters={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_FILTERS")))
  1893|         except Exception:
  1894|             sig.append("include_filters=False")
  1895| 
  1896|         try:
  1897|             sig.append("include_appearance={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_APPEARANCE")))
  1898|         except Exception:
  1899|             sig.append("include_appearance=False")
  1900| 
  1901|         # Phase Filter (resolved via phase_filters domain)
  1902|         try:
  1903|             include_pf = _is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")
  1904|         except Exception:
  1905|             include_pf = False
  1906| 
  1907|         v2_ok = _append_phase_filter_value(
  1908|             v=v,
  1909|             doc=doc,
  1910|             include_pf=include_pf,
  1911|             phase_filter_map=phase_filter_map,
  1912|             phase_filter_map_v2=phase_filter_map_v2,
  1913|             sig=sig,
  1914|             sig_v2=sig_v2,
  1915|             v2_ok=v2_ok,
  1916|             v2_block_fn=_v2_block,
  1917|             debug_counters=info,
  1918|         )
  1919| 
  1920|         # Filter stack (order-sensitive)
  1921|         v2_ok = _append_filter_stack_signature(v, doc, view_filter_map, sig, sig_v2, v2_ok, _v2_block)
  1922|         v2_ok = _append_workset_visibility(v, doc, sig, sig_v2, v2_ok, _v2_block)
  1923| 
  1924|         # Built-in visual/behavioural parameters
  1925|         emit_builtin_params(v, DOMAIN_NAME, tpl_bips, non_ctrl_bips, sig, sig_v2,
  1926|                             debug_counters=info)
  1927| 
  1928|         # Shared/project parameters (stub — no-op until GUIDs confirmed)
  1929|         emit_shared_params_stub(v, DOMAIN_NAME, tpl_ids, sig, sig_v2,
  1930|                                 debug_counters=info)
  1931| 
  1932|         # Finalize signature (deterministic)
  1933|         sig_final = sorted(sig)
  1934|         def_hash = make_hash(sig_final)
  1935| 
  1936|         # v2 finalize
  1937|         if v2_ok:
  1938|             try:
  1939|                 sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
  1940|                 sig_v2_final = sorted(set(sig_v2))
  1941|                 def_hash_v2 = make_hash(sig_v2_final)
  1942|                 per_hashes_v2.append(def_hash_v2)
  1943|             except Exception:
  1944|                 _v2_block("template_finalize_failed")
  1945|                 v2_ok = False
  1946| 
  1947|         # record.v2 + Phase-2
  1948|         identity_items = _canonical_identity_items_from_signature(def_hash, sig_final)
  1949|         semantic_keys = _semantic_keys_from_identity_items(identity_items)
  1950|         semantic_items = [it for it in identity_items if it.get("k") in set(semantic_keys)]
  1951|         sig_hash = make_hash(serialize_identity_items(semantic_items))
  1952| 
  1953|         rid_info = make_record_id_from_element(v)
  1954|         if rid_info:
  1955|             record_id, record_id_alg = rid_info
  1956|         else:
  1957|             record_id = "eid:{}".format(safe_str(getattr(getattr(v, "Id", None), "IntegerValue", "")))
  1958|             record_id_alg = "revit_elementid_v1"
  1959| 
  1960|         status = STATUS_OK
  1961|         status_reasons = []
  1962|         for it in identity_items:
  1963|             if it.get("q") != ITEM_Q_OK:
  1964|                 status = STATUS_DEGRADED
  1965|                 status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))
  1966|         if not v2_ok:
  1967|             status = STATUS_BLOCKED
  1968|             status_reasons.append("semantic_v2_unresolved_dependency")
  1969|             sig_hash = None
  1970| 
  1971|         vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING
  1972| 
  1973|         rec = build_record_v2(
  1974|             domain=DOMAIN_NAME,
  1975|             record_id=record_id,
  1976|             record_id_alg=record_id_alg,
  1977|             status=status,
  1978|             status_reasons=sorted(set(status_reasons)),
  1979|             sig_hash=sig_hash,
  1980|             identity_items=identity_items,
  1981|             required_qs=tuple(it.get("q") for it in identity_items),
  1982|             label={
  1983|                 "display": safe_str(name),
  1984|                 "quality": "human" if safe_str(name) and safe_str(name) != S_MISSING else "placeholder_missing",
  1985|                 "provenance": "revit.ViewName",
  1986|                 "components": {
  1987|                     "view_type": vt_raw_str,
  1988|                 },
  1989|             },
  1990|         )
  1991|         _ip, _ip_q = purge_lookup(getattr(getattr(v, "Id", None), "IntegerValue", None), ctx)
  1992|         rec["is_purgeable"] = _ip
  1993|         rec["is_purgeable_q"] = _ip_q
  1994| 
  1995|         rec["phase2"] = {
  1996|             "schema": "phase2.{}.v2".format(DOMAIN_NAME),
  1997|             "grouping_basis": "join_key.join_hash",
  1998|             "cosmetic_items": [],
  1999|             "coordination_items": [
  2000|                 make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
  2001|                 make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
  2002|             ],
  2003|             "unknown_items": _traceability_unknown_items(v),
  2004|         }
  2005|         _append_assigned_view_count_cosmetic_item(rec, doc, v, ctx)
  2006| 
  2007|         rec["sig_basis"] = {
  2008|             "hash_alg": "md5_utf8_join_pipe",
  2009|             "keys_used": semantic_keys,
  2010|         }
  2011| 
  2012|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  2013|         vt_join_key, _vt_missing = build_join_key_from_policy(
  2014|             domain_policy=pol,
  2015|             identity_items=identity_items,
  2016|             include_optional_items=False,
  2017|             emit_keys_used=True,
  2018|             hash_optional_items=False,
  2019|             emit_items=False,
  2020|             emit_selectors=True,
  2021|         )
  2022|         rec["join_key"] = vt_join_key
  2023| 
  2024|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
  2025|         # keyed off this record's own label.display-backing item (view_template.name).
  2026|         # view_template.name does not exist in identity_items for any partition --
  2027|         # identity_items are built from _canonical_identity_items_from_signature(def_hash,
  2028|         # sig_final), a structured signature that explicitly strips "name="-prefixed
  2029|         # entries before hashing. Widened items list used only for this call;
  2030|         # identity_basis.items/sig_hash/join_key above are unaffected.
  2031|         vt_name_v, vt_name_q = canonicalize_str(name)
  2032|         name_key_items = identity_items + [
  2033|             make_identity_item("view_template.name", vt_name_v, vt_name_q)
  2034|         ]
  2035|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  2036|         rec["join_key_name_identity"], _vt_name_key_missing = build_join_key_from_policy(
  2037|             domain_policy=name_key_pol,
  2038|             identity_items=name_key_items,
  2039|             include_optional_items=False,
  2040|             emit_keys_used=True,
  2041|             hash_optional_items=False,
  2042|             emit_items=False,
  2043|             emit_selectors=True,
  2044|         )
  2045|         rec["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _vt_name_key_missing)
  2046| 
  2047|         rec["def_hash"] = def_hash
  2048|         rec["def_signature"] = sig_final
  2049| 
  2050|         records.append(rec)
  2051|         per_hashes.append(def_hash)
  2052|         info["debug_kept"] += 1
  2053| 
  2054|     # Finalize
  2055|     info["names"] = sorted(set(names))
  2056|     info["count"] = len(records)
  2057| 
  2058|     info["records"] = sorted(
  2059|         records,
  2060|         key=lambda r: (
  2061|             safe_str(((r.get("label", {}) or {}).get("display", ""))),
  2062|             safe_str(r.get("record_id", "")),
  2063|         ),
  2064|     )
  2065| 
  2066|     info["signature_hashes_v2"] = sorted(per_hashes_v2)
  2067|     if v2_any_blocked:
  2068|         info["hash_v2"] = None
  2069|     else:
  2070|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  2071| 
  2072|     info["record_rows"] = []
  2073|     try:
  2074|         recs = info.get("records") or []
  2075|         info["record_rows"] = [{
  2076|             "record_key": safe_str(r.get("record_id", "")),
  2077|             "sig_hash":   safe_str(r.get("sig_hash", "")),
  2078|             "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
  2079|             "view_type":  safe_str(((r.get("label", {}) or {}).get("components", {}) or {}).get("view_type", "")),
  2080|         } for r in recs]
  2081|     except Exception:
  2082|         info["record_rows"] = []
  2083| 
  2084|     # PR6: deterministic degraded signaling
  2085|     degraded_reason_counts = {}
  2086| 
  2087|     try:
  2088|         if int(info.get("debug_missing_uid", 0)) > 0:
  2089|             degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
  2090|     except Exception:
  2091|         pass
  2092| 
  2093|     try:
  2094|         if int(info.get("debug_fail_read", 0)) > 0:
  2095|             degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
  2096|     except Exception:
  2097|         pass
  2098| 
  2099|     try:
  2100|         if int(info.get("debug_view_context_problem", 0)) > 0:
  2101|             for k, vv in dict(info.get("debug_view_context_reasons", {})).items():
  2102|                 key = str(k)
  2103|                 if key.endswith("_not_applicable"):
  2104|                     continue
  2105|                 degraded_reason_counts[key] = int(vv)
  2106|     except Exception:
  2107|         pass
  2108| 
  2109|     try:
  2110|         if int(info.get("debug_v2_blocked", 0)) > 0:
  2111|             degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
  2112|     except Exception:
  2113|         pass
  2114| 
  2115|     if degraded_reason_counts:
  2116|         info["_domain_status"] = "degraded"
  2117|         info["_domain_diag"] = {
  2118|             "degraded_reasons": sorted(degraded_reason_counts.keys()),
  2119|             "degraded_reason_counts": degraded_reason_counts,
  2120|         }
  2121|     else:
  2122|         info["_domain_status"] = "ok"
  2123|         info["_domain_diag"] = {}
  2124| 
  2125|     return info
  2126| 
  2127| def _is_schedule_view(v):
  2128|     """Return True if this view element is a schedule (ViewSchedule or ViewType='Schedule')."""
  2129|     if ViewSchedule is not None:
  2130|         try:
  2131|             if isinstance(v, ViewSchedule):
  2132|                 return True
  2133|         except Exception:
  2134|             pass
  2135|     # Fallback: check ViewType string
  2136|     try:
  2137|         vt_str = safe_str(getattr(v, "ViewType", None)).strip()
  2138|         if vt_str == _SCHEDULE_VIEW_TYPE:
  2139|             return True
  2140|     except Exception:
  2141|         pass
  2142|     return False
  2143| 
  2144| 
```
