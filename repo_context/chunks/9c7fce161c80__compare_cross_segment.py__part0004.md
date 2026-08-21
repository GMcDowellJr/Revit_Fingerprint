# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 4 of 13
- Original line range: 1485-2004
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_pattern_reuse_distribution_rows, build_pattern_reuse_summary_rows, load_segment_join_hash_union, load_bundle_join_hash_set, annotate_bundle_overlap, _pct, _fmt, _mean, _min, _comparison_status, _cardinality_shape, _file_count_ratio, _cardinality_fields, _union_similarity, compare_directed_file, compare_symmetric_file, _normalize_bc_label, _bc_of, _client_of, _is_enterprise_client, _is_enterprise_bc, _scope_level
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  1485| def build_pattern_reuse_distribution_rows(
  1486|     union_rows: List[Dict[str, str]],
  1487|     executed_utc: str,
  1488| ) -> List[Dict[str, str]]:
  1489|     """Build reuse distribution rows from normalized union-inventory join_hash rows."""
  1490|     candidate_rows = [
  1491|         r for r in union_rows
  1492|         if r.get("join_hash", "").strip() or r.get("inventory_status", "") != "ok"
  1493|     ]
  1494|     positive = [r for r in candidate_rows if r.get("join_hash", "").strip()]
  1495|     file_den_by_group: Dict[Tuple[str, str, str, str, str, str], int] = {}
  1496|     project_den_by_group: Dict[Tuple[str, str, str, str, str, str], int] = {}
  1497|     clients_by_group: Dict[Tuple[str, str, str, str, str], Set[str]] = defaultdict(set)
  1498|     clients_by_pattern: Dict[Tuple[str, str, str, str, str, str], Set[str]] = defaultdict(set)
  1499| 
  1500|     for r in candidate_rows:
  1501|         key = (
  1502|             r.get("view_scope", ""), r.get("governance_role", ""),
  1503|             r.get("client_label", ""), r.get("discipline_label", ""),
  1504|             r.get("unit_system", ""), r.get("domain", ""),
  1505|         )
  1506|         file_den_by_group[key] = max(
  1507|             file_den_by_group.get(key, 0),
  1508|             int(r.get("n_files_denominator") or r.get("n_files_present") or "0"),
  1509|         )
  1510|         project_den_by_group[key] = max(
  1511|             project_den_by_group.get(key, 0),
  1512|             int(r.get("n_projects_denominator") or r.get("n_projects_present") or "0"),
  1513|         )
  1514|         clients_by_group[(key[0], key[1], key[3], key[4], key[5])].add(key[2])
  1515|         clients_by_pattern[(
  1516|             key[0], key[1], key[3], key[4], key[5], r.get("join_hash", "")
  1517|         )].add(key[2])
  1518| 
  1519|     rows: List[Dict[str, str]] = []
  1520|     for r in candidate_rows:
  1521|         key = (
  1522|             r.get("view_scope", ""), r.get("governance_role", ""),
  1523|             r.get("client_label", ""), r.get("discipline_label", ""),
  1524|             r.get("unit_system", ""), r.get("domain", ""),
  1525|         )
  1526|         client_group = (key[0], key[1], key[3], key[4], key[5])
  1527|         n_files = int(r.get("n_files_present") or "0")
  1528|         n_projects = int(r.get("n_projects_present") or "0")
  1529|         n_files_den = file_den_by_group.get(key, 0)
  1530|         n_projects_den = project_den_by_group.get(key, 0)
  1531|         n_clients = len(clients_by_pattern.get((
  1532|             key[0], key[1], key[3], key[4], key[5], r.get("join_hash", "")
  1533|         ), set()))
  1534|         n_clients_den = len(clients_by_group.get(client_group, set()))
  1535|         if r.get("source_status", "ok") != "ok":
  1536|             bucket, basis, status = (
  1537|                 "unclassified", "source_status",
  1538|                 "degraded_" + r.get("source_status", "unknown"),
  1539|             )
  1540|         elif r.get("inventory_status") != "ok":
  1541|             bucket, basis, status = (
  1542|                 "unclassified", "inventory_status",
  1543|                 "blocked_" + r.get("inventory_status", "unknown"),
  1544|             )
  1545|         else:
  1546|             bucket, basis, status = _reuse_bucket_for(
  1547|                 n_files=n_files, n_files_den=n_files_den,
  1548|                 n_projects=n_projects, n_projects_den=n_projects_den,
  1549|                 n_clients=n_clients, n_clients_den=n_clients_den,
  1550|             )
  1551|         rows.append({
  1552|             "view_scope": r.get("view_scope", ""),
  1553|             "governance_role": r.get("governance_role", ""),
  1554|             "client_label": r.get("client_label", ""),
  1555|             "discipline_label": r.get("discipline_label", ""),
  1556|             "unit_system": r.get("unit_system", ""),
  1557|             "domain": r.get("domain", ""),
  1558|             "join_hash": r.get("join_hash", ""),
  1559|             "pattern_label": r.get("pattern_label", ""),
  1560|             "n_files_present": str(n_files),
  1561|             "n_files_denominator": str(n_files_den),
  1562|             "pct_files_present": _safe_pct(n_files, n_files_den),
  1563|             "n_projects_present": str(n_projects),
  1564|             "n_projects_denominator": str(n_projects_den),
  1565|             "pct_projects_present": _safe_pct(n_projects, n_projects_den),
  1566|             "n_clients_present": str(n_clients),
  1567|             "n_clients_denominator": str(n_clients_den),
  1568|             "pct_clients_present": _safe_pct(n_clients, n_clients_den),
  1569|             "reuse_bucket": bucket,
  1570|             "bucket_basis": basis,
  1571|             "usage_interpretable": r.get("usage_interpretable", ""),
  1572|             "inventory_status": r.get("inventory_status", ""),
  1573|             "classification_status": status,
  1574|             "executed_utc": executed_utc,
  1575|         })
  1576|     rows.sort(key=lambda r: (
  1577|         r["view_scope"], r["governance_role"], r["client_label"],
  1578|         r["discipline_label"], r["unit_system"], r["domain"], r["join_hash"],
  1579|     ))
  1580|     return rows
  1581| 
  1582| 
  1583| def build_pattern_reuse_summary_rows(
  1584|     distribution_rows: List[Dict[str, str]],
  1585|     *,
  1586|     by_client: bool,
  1587| ) -> List[Dict[str, str]]:
  1588|     grouped: Dict[Tuple[str, ...], Dict[str, str]] = {}
  1589|     counts: Dict[Tuple[str, ...], int] = defaultdict(int)
  1590|     for r in distribution_rows:
  1591|         key = (
  1592|             r["view_scope"], r["governance_role"],
  1593|             r["client_label"] if by_client else "",
  1594|             r["discipline_label"], r["unit_system"], r["domain"],
  1595|             r["reuse_bucket"], r["bucket_basis"], r["usage_interpretable"],
  1596|             r["classification_status"], r["executed_utc"],
  1597|         )
  1598|         counts[key] += 1
  1599|         grouped[key] = r
  1600|     rows = []
  1601|     for key in sorted(counts):
  1602|         r = grouped[key]
  1603|         rows.append({
  1604|             "view_scope": r["view_scope"],
  1605|             "governance_role": r["governance_role"],
  1606|             "client_label": r["client_label"] if by_client else "",
  1607|             "discipline_label": r["discipline_label"],
  1608|             "unit_system": r["unit_system"],
  1609|             "domain": r["domain"],
  1610|             "reuse_bucket": r["reuse_bucket"],
  1611|             "bucket_basis": r["bucket_basis"],
  1612|             "n_patterns": str(counts[key]),
  1613|             "usage_interpretable": r["usage_interpretable"],
  1614|             "classification_status": r["classification_status"],
  1615|             "executed_utc": r["executed_utc"],
  1616|         })
  1617|     return rows
  1618| 
  1619| def load_segment_join_hash_union(
  1620|     segments_root: Path,
  1621|     registry: Dict[str, Dict[str, str]],
  1622|     segment_id: str,
  1623|     domain: str,
  1624|     purge_view: str = "all",
  1625| ) -> Set[str]:
  1626|     result: Set[str] = set()
  1627|     for jhs in load_file_join_hashes(segments_root, registry, segment_id, domain, purge_view).values():
  1628|         result |= jhs
  1629|     return result
  1630| 
  1631| 
  1632| def load_bundle_join_hash_set(
  1633|     segments_root: Path,
  1634|     registry: Dict[str, Dict[str, str]],
  1635|     segment_id: str,
  1636|     domain: str,
  1637|     purge_view: str = "all",
  1638| ) -> Set[str]:
  1639|     """Return join_hashes that are bundle members for segment/domain/purge_view.
  1640| 
  1641|     Empty set if bundle_membership.csv absent for this view.
  1642|     Path: {segment_output_folder}/results/bundle_analysis/{purge_view}/{domain}/bundle_membership.csv
  1643|     """
  1644|     key = (segment_id, domain, purge_view)
  1645|     if key in _bundle_jh_cache:
  1646|         return _bundle_jh_cache[key]
  1647| 
  1648|     seg_out = segment_output_dir(segments_root, registry, segment_id)
  1649|     if seg_out is None:
  1650|         _bundle_jh_cache[key] = set()
  1651|         return set()
  1652| 
  1653|     bm_path = bundle_analysis_dir(seg_out, domain, purge_view) / "bundle_membership.csv"
  1654|     if not bm_path.exists():
  1655|         _bundle_jh_cache[key] = set()
  1656|         return set()
  1657| 
  1658|     jh_map = resolve_join_hashes(segments_root, registry, segment_id, domain)
  1659|     result: Set[str] = set()
  1660|     for row in read_csv_rows(bm_path):
  1661|         pid = row.get("pattern_id", "").strip()
  1662|         if not pid:
  1663|             continue
  1664|         jh = jh_map.get(pid)
  1665|         if jh:
  1666|             result.add(jh)
  1667| 
  1668|     _bundle_jh_cache[key] = result
  1669|     return result
  1670| 
  1671| 
  1672| # ---------------------------------------------------------------------------
  1673| # Bundle annotation
  1674| # ---------------------------------------------------------------------------
  1675| 
  1676| def annotate_bundle_overlap(
  1677|     shared_jhs: Set[str],
  1678|     bundle_jhs_a: Set[str],
  1679|     bundle_jhs_b: Set[str],
  1680| ) -> Tuple[int, int, int]:
  1681|     """Return (n_both, n_a_only, n_b_only) for shared join_hashes."""
  1682|     n_both = len(shared_jhs & bundle_jhs_a & bundle_jhs_b)
  1683|     n_a_only = len(shared_jhs & bundle_jhs_a - bundle_jhs_b)
  1684|     n_b_only = len(shared_jhs & bundle_jhs_b - bundle_jhs_a)
  1685|     return n_both, n_a_only, n_b_only
  1686| 
  1687| 
  1688| # ---------------------------------------------------------------------------
  1689| # Metrics helpers
  1690| # ---------------------------------------------------------------------------
  1691| 
  1692| def _pct(xs: List[float], p: float) -> float:
  1693|     if not xs:
  1694|         return 0.0
  1695|     xs_sorted = sorted(xs)
  1696|     idx = (len(xs_sorted) - 1) * p / 100.0
  1697|     lo = int(idx)
  1698|     hi = min(lo + 1, len(xs_sorted) - 1)
  1699|     frac = idx - lo
  1700|     return xs_sorted[lo] * (1 - frac) + xs_sorted[hi] * frac
  1701| 
  1702| 
  1703| def _fmt(v: float) -> str:
  1704|     return f"{v:.6f}"
  1705| 
  1706| 
  1707| def _mean(xs: List[float]) -> str:
  1708|     return _fmt(sum(xs) / len(xs)) if xs else ""
  1709| 
  1710| 
  1711| def _min(xs: List[float]) -> str:
  1712|     return _fmt(min(xs)) if xs else ""
  1713| 
  1714| 
  1715| # ---------------------------------------------------------------------------
  1716| # Cardinality / status classification — explicit, non-suppressive.
  1717| #
  1718| # comparison_status replaces the removed n_files >= 5 data_sufficient gate.
  1719| # Scores are always computed and emitted regardless of status; status is
  1720| # purely interpretive metadata. blocked is reserved for "no data at all" —
  1721| # degraded/ok comparisons still carry full, trustworthy metrics, just with
  1722| # narrower (degraded) or normal (ok) evidence breadth. cardinality_shape and
  1723| # file_count_ratio are descriptive only and never gate anything.
  1724| # ---------------------------------------------------------------------------
  1725| 
  1726| def _comparison_status(n_files_a: int, n_files_b: int) -> str:
  1727|     if n_files_a == 0 or n_files_b == 0:
  1728|         return "blocked"
  1729|     if (n_files_a == 1 or n_files_b == 1) and n_files_a != n_files_b:
  1730|         return "degraded"
  1731|     return "ok"
  1732| 
  1733| 
  1734| def _cardinality_shape(n_files_a: int, n_files_b: int) -> str:
  1735|     if n_files_a == n_files_b:
  1736|         return "balanced"
  1737|     if n_files_a == 1:
  1738|         return "single_a"
  1739|     if n_files_b == 1:
  1740|         return "single_b"
  1741|     return "imbalanced"
  1742| 
  1743| 
  1744| def _file_count_ratio(n_files_a: int, n_files_b: int) -> str:
  1745|     if n_files_a == 0 or n_files_b == 0:
  1746|         return ""
  1747|     return _fmt(max(n_files_a, n_files_b) / min(n_files_a, n_files_b))
  1748| 
  1749| 
  1750| def _cardinality_fields(n_files_a: int, n_files_b: int) -> Dict[str, str]:
  1751|     return {
  1752|         "comparison_status": _comparison_status(n_files_a, n_files_b),
  1753|         "cardinality_shape": _cardinality_shape(n_files_a, n_files_b),
  1754|         "file_count_ratio": _file_count_ratio(n_files_a, n_files_b),
  1755|     }
  1756| 
  1757| 
  1758| def _union_similarity(jhs_a: Set[str], jhs_b: Set[str]) -> Tuple[str, str, str]:
  1759|     """Population-footprint metrics: union(A) vs union(B), independent of
  1760|     n_files_a x n_files_b. Returns (jaccard, containment_a_in_b, containment_b_in_a)."""
  1761|     union = jhs_a | jhs_b
  1762|     shared = jhs_a & jhs_b
  1763|     jac = _fmt(len(shared) / len(union)) if union else ""
  1764|     c_ab = _fmt(len(shared) / len(jhs_a)) if jhs_a else ""
  1765|     c_ba = _fmt(len(shared) / len(jhs_b)) if jhs_b else ""
  1766|     return jac, c_ab, c_ba
  1767| 
  1768| 
  1769| # ---------------------------------------------------------------------------
  1770| # Comparison engine — directed (containment)
  1771| # ---------------------------------------------------------------------------
  1772| 
  1773| def compare_directed_file(
  1774|     ref_files: Dict[str, Set[str]],
  1775|     tgt_files: Dict[str, Set[str]],
  1776| ) -> Dict[str, str]:
  1777|     ref_union: Set[str] = set()
  1778|     for jhs in ref_files.values():
  1779|         ref_union |= jhs
  1780| 
  1781|     if not ref_union:
  1782|         return {}
  1783| 
  1784|     b_in_a: List[float] = []
  1785|     a_in_b: List[float] = []
  1786| 
  1787|     for jhs in tgt_files.values():
  1788|         shared = len(jhs & ref_union)
  1789|         b_in_a.append(shared / len(ref_union))
  1790|         a_in_b.append(shared / len(jhs) if jhs else 0.0)
  1791| 
  1792|     all_b: Set[str] = set()
  1793|     for jhs in tgt_files.values():
  1794|         all_b |= jhs
  1795| 
  1796|     # Reference heterogeneity: is a multi-file reference a coherent standard
  1797|     # (high core share) or a broad union of conflicting sources (low core
  1798|     # share)? Degrades gracefully to 1.0 for a single-file reference — a
  1799|     # lone file is trivially coherent with itself, not an artificial failure.
  1800|     ref_intersection: Optional[Set[str]] = None
  1801|     for jhs in ref_files.values():
  1802|         ref_intersection = jhs if ref_intersection is None else (ref_intersection & jhs)
  1803|     ref_intersection = ref_intersection or set()
  1804|     ref_core_share = (
  1805|         len(ref_intersection) / len(ref_union) if ref_union else 0.0
  1806|     )
  1807| 
  1808|     return {
  1809|         "n_shared_join_hash": str(len(ref_union & all_b)),
  1810|         "all_pairwise_containment_a_in_b_mean": _mean(a_in_b),
  1811|         "all_containment_a_in_b_min": _min(a_in_b),
  1812|         "all_pairwise_containment_b_in_a_mean": _mean(b_in_a),
  1813|         "all_containment_b_in_a_min": _min(b_in_a),
  1814|         "n_files_a": str(len(ref_files)),
  1815|         "n_files_b": str(len(tgt_files)),
  1816|         "n_pairs": str(len(tgt_files)),
  1817|         "n_reference_files": str(len(ref_files)),
  1818|         "reference_union_pattern_count": str(len(ref_union)),
  1819|         "reference_intersection_pattern_count": str(len(ref_intersection)),
  1820|         "reference_core_share": _fmt(ref_core_share),
  1821|     }
  1822| 
  1823| 
  1824| # ---------------------------------------------------------------------------
  1825| # Comparison engine — symmetric (Jaccard + containment)
  1826| # ---------------------------------------------------------------------------
  1827| 
  1828| def compare_symmetric_file(
  1829|     files_a: Dict[str, Set[str]],
  1830|     files_b: Dict[str, Set[str]],
  1831| ) -> Tuple[Dict[str, str], List[Dict[str, str]]]:
  1832|     """Return (summary_metrics, pairwise_rows).
  1833| 
  1834|     Containment is computed per file pair in both directions and aggregated to
  1835|     mean/min for the summary — these columns are always populated regardless of
  1836|     comparison type.
  1837|     """
  1838|     jaccards: List[float] = []
  1839|     c_ab_list: List[float] = []
  1840|     c_ba_list: List[float] = []
  1841|     pair_rows: List[Dict[str, str]] = []
  1842|     per_a_jaccards: Dict[str, List[float]] = defaultdict(list)
  1843|     per_b_jaccards: Dict[str, List[float]] = defaultdict(list)
  1844| 
  1845|     for eid_a, jhs_a in files_a.items():
  1846|         for eid_b, jhs_b in files_b.items():
  1847|             union = jhs_a | jhs_b
  1848|             j = len(jhs_a & jhs_b) / len(union) if union else 0.0
  1849|             c_ab = len(jhs_a & jhs_b) / len(jhs_a) if jhs_a else 0.0
  1850|             c_ba = len(jhs_a & jhs_b) / len(jhs_b) if jhs_b else 0.0
  1851|             jaccards.append(j)
  1852|             c_ab_list.append(c_ab)
  1853|             c_ba_list.append(c_ba)
  1854|             per_a_jaccards[eid_a].append(j)
  1855|             per_b_jaccards[eid_b].append(j)
  1856|             pair_rows.append({
  1857|                 "export_run_id_a": eid_a,
  1858|                 "export_run_id_b": eid_b,
  1859|                 "n_patterns_a": str(len(jhs_a)),
  1860|                 "n_patterns_b": str(len(jhs_b)),
  1861|                 "n_shared": str(len(jhs_a & jhs_b)),
  1862|                 "all_jaccard": _fmt(j),
  1863|                 "all_containment_a_in_b": _fmt(c_ab),
  1864|                 "all_containment_b_in_a": _fmt(c_ba),
  1865|             })
  1866| 
  1867|     all_a: Set[str] = set()
  1868|     for jhs in files_a.values():
  1869|         all_a |= jhs
  1870|     all_b: Set[str] = set()
  1871|     for jhs in files_b.values():
  1872|         all_b |= jhs
  1873| 
  1874|     # Side-balanced summaries: each A-file's own mean similarity to every B
  1875|     # file, then mean/min of those per-file means (and the inverse for B).
  1876|     # Exposes directional population experience — in a 1xN comparison, the
  1877|     # A-side summary is one file's average similarity to N files; the B-side
  1878|     # summary is the distribution of N files against that one A file.
  1879|     a_file_means = [sum(v) / len(v) for v in per_a_jaccards.values()]
  1880|     b_file_means = [sum(v) / len(v) for v in per_b_jaccards.values()]
  1881| 
  1882|     summary = {
  1883|         "n_shared_join_hash": str(len(all_a & all_b)),
  1884|         "all_pairwise_containment_a_in_b_mean": _mean(c_ab_list),
  1885|         "all_containment_a_in_b_min": _min(c_ab_list),
  1886|         "all_pairwise_containment_b_in_a_mean": _mean(c_ba_list),
  1887|         "all_containment_b_in_a_min": _min(c_ba_list),
  1888|         "all_pairwise_jaccard_mean": _mean(jaccards),
  1889|         "all_jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
  1890|         "all_jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
  1891|         "n_files_a": str(len(files_a)),
  1892|         "n_files_b": str(len(files_b)),
  1893|         "n_pairs": str(len(jaccards)),
  1894|         "all_a_file_mean_similarity_to_b_mean": _mean(a_file_means),
  1895|         "all_a_file_mean_similarity_to_b_min": _min(a_file_means),
  1896|         "all_b_file_mean_similarity_to_a_mean": _mean(b_file_means),
  1897|         "all_b_file_mean_similarity_to_a_min": _min(b_file_means),
  1898|     }
  1899|     return summary, pair_rows
  1900| 
  1901| 
  1902| # ---------------------------------------------------------------------------
  1903| # Pair descriptor
  1904| # ---------------------------------------------------------------------------
  1905| 
  1906| DIRECTED_TYPES = {
  1907|     "generic_to_template",
  1908|     "generic_to_container",
  1909|     "generic_to_project",
  1910|     "template_to_project",
  1911|     "template_to_container",
  1912|     "container_to_project",
  1913|     "parent_sibling_roles",
  1914|     "governance_chain",
  1915|     "enterprise_to_project",
  1916|     "bc_to_project",
  1917|     "enterprise_to_bc",
  1918|     "enterprise_to_client",
  1919| }
  1920| 
  1921| # ---------------------------------------------------------------------------
  1922| # Scope-level classification (enterprise / business_center / client_business_center)
  1923| #
  1924| # Under the explicit-metadata contract (PR1), client_label and
  1925| # business_center_label are real, literal, non-blank values on every
  1926| # file_metadata.csv row -- "InternalEnterprise" / "0000" for InternalEnterprise-internal work, a
  1927| # real client name / business center number otherwise. A blank value on a
  1928| # segment_manifest.csv row therefore no longer means "not a client
  1929| # engagement" -- it means this segment's subset simply did not cut on that
  1930| # dimension, so the segment pools every value of it (a roll-up). Scope level
  1931| # is a classification of a segment's OWN cut values, not of what it pools;
  1932| # roll-ups are handled separately by the callers that need them (see
  1933| # discover_cross_client() / the enterprise_to_client target logic below),
  1934| # not by this function.
  1935| #
  1936| # A row is Enterprise-scoped only when BOTH client_label == "InternalEnterprise" AND
  1937| # business_center_label == "0000" -- either alone is not sufficient (a real
  1938| # external client can still carry the "0000" bookkeeping tag in principle,
  1939| # and InternalEnterprise-internal work can carry a real business center). Scope level
  1940| # is orthogonal to governance_role -- do not encode Project into it; a
  1941| # client+bc segment can be Template, Container, or Project.
  1942| # ---------------------------------------------------------------------------
  1943| 
  1944| _ENTERPRISE_BC_LABEL = "0000"
  1945| 
  1946| 
  1947| def _normalize_bc_label(value: str) -> str:
  1948|     v = (value or "").strip()
  1949|     if is_blank_or_na(v):
  1950|         return ""
  1951|     # "0000"/"BC_0000" (any case) are spelling variants of the same
  1952|     # enterprise-bookkeeping value elsewhere in the pipeline (e.g. the
  1953|     # extraction completeness gate documents both) -- canonicalize to the
  1954|     # literal "0000" so they group/classify identically instead of
  1955|     # fragmenting into two distinct-looking business centers. This is
  1956|     # distinct from the removed blank-fold: a real, non-blank value is
  1957|     # still returned, just spelled consistently.
  1958|     if v.lower() in _ENTERPRISE_BC_BOOKKEEPING_TOKENS:
  1959|         return _ENTERPRISE_BC_LABEL
  1960|     return v
  1961| 
  1962| 
  1963| def _bc_of(row: Dict[str, str]) -> str:
  1964|     return _normalize_bc_label(row.get("business_center_label", ""))
  1965| 
  1966| 
  1967| def _client_of(row: Dict[str, str]) -> str:
  1968|     v = row.get("client_label", "").strip()
  1969|     return "" if is_blank_or_na(v) else v
  1970| 
  1971| 
  1972| 
  1973| def _is_enterprise_client(client_label: str, policy: EnterprisePolicy) -> bool:
  1974|     return policy.is_enterprise(client_label)
  1975| 
  1976| 
  1977| def _is_enterprise_bc(bc_label: str, policy: EnterprisePolicy) -> bool:
  1978|     return bc_label.strip() == policy.enterprise_business_center_token
  1979| 
  1980| 
  1981| def _scope_level(row: Dict[str, str], policy: EnterprisePolicy) -> Optional[str]:
  1982|     """Classify a segment row's own (client_label, business_center_label)
  1983|     cut values. Returns None when either dimension is not cut on this row
  1984|     (a roll-up pooling multiple real scopes) -- callers that need roll-up
  1985|     populations (client-wide standards, cross-client comparisons) handle
  1986|     that case explicitly rather than treating it as a fourth scope level.
  1987|     """
  1988|     client = _client_of(row)
  1989|     bc = _bc_of(row)
  1990|     if not client or not bc:
  1991|         return None
  1992|     internal = _is_enterprise_client(client, policy)
  1993|     enterprise_bc = _is_enterprise_bc(bc, policy)
  1994|     if internal and enterprise_bc:
  1995|         return "enterprise"
  1996|     if internal and not enterprise_bc:
  1997|         return "business_center"
  1998|     if not internal and not enterprise_bc:
  1999|         return "client_business_center"
  2000|     # Real external client literally tagged with the "0000" bookkeeping
  2001|     # value -- does not fit a defined scope level.
  2002|     return None
  2003| 
  2004| 
```
