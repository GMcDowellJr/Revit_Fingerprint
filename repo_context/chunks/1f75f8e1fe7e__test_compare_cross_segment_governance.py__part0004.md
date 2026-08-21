# Chunk of tests/test_compare_cross_segment_governance.py

- Source relative path: `tests/test_compare_cross_segment_governance.py`
- Chunk: 4 of 5
- Original line range: 1500-1984
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_union_inventory_non_project_used_view_not_active_usage, test_union_inventory_duplicate_join_hash_collapses_counts, test_union_inventory_pattern_id_not_cross_segment_identity, test_union_inventory_missing_source_cluster_status_no_synthetic_pattern, test_union_inventory_output_order_is_deterministic, test_union_inventory_used_view_unavailable_keeps_source_status_ok, test_union_inventory_client_denominator_includes_status_rows_used_by_reuse, test_union_inventory_missing_domain_patterns_keeps_source_status_ok, test_pattern_reuse_many_files_gets_broad_classification, test_pattern_reuse_one_file_gets_single_file_classification, test_project_used_view_uses_project_and_file_denominators_for_emerging_bucket, test_single_project_reuse_takes_precedence_over_emerging, test_missing_source_identity_degrades_reuse_classification, test_template_all_view_is_not_interpreted_as_active_usage, test_reuse_zero_denominator_is_degraded_unclassified, test_reuse_distribution_order_is_deterministic, test_reuse_thresholds_are_centralized_and_used, test_explicit_matrices_union_jaccard_differs_from_mean_file_pair, test_fragmentation_diagnostic_uses_all_domains_file_pair_aggregate, test_density_similarity_uses_domain_density_vectors_not_containment
- Source SHA-256: 41a98d942cef2b25dee2bd74f79b3ba9f6e871cbbff68d9ef81011f7e3336043
- Starts inside symbol: no
- Ends inside symbol: no

```
  1500| def test_union_inventory_non_project_used_view_not_active_usage(tmp_path):
  1501|     domain = "line_patterns"
  1502|     segments_root = tmp_path / "segments"
  1503|     _write_segment(
  1504|         segments_root,
  1505|         "template",
  1506|         domain,
  1507|         [("t1", "join_a", "Join A")],
  1508|         [{"export_run_id": "template_file", "pattern_id": "t1"}],
  1509|         [{"export_run_id": "template_file", "pattern_id": "t1"}],
  1510|         ["t1"],
  1511|     )
  1512|     manifest = {"template": {**_seg("Template"), "segment_label": "Template"}}
  1513|     registry = {"template": {"output_folder": "template", "run_type": "bundle"}}
  1514| 
  1515|     used_rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "used"]
  1516| 
  1517|     assert used_rows[0]["usage_interpretable"] == "false"
  1518|     assert used_rows[0]["inventory_status"] == "not_interpretable"
  1519| 
  1520| 
  1521| def test_union_inventory_duplicate_join_hash_collapses_counts(tmp_path):
  1522|     domain = "line_patterns"
  1523|     segments_root = tmp_path / "segments"
  1524|     _write_segment(
  1525|         segments_root,
  1526|         "project_a",
  1527|         domain,
  1528|         [("p1", "same_join", "Same"), ("p2", "same_join", "Same")],
  1529|         [
  1530|             {"export_run_id": "file_1", "pattern_id": "p1"},
  1531|             {"export_run_id": "file_2", "pattern_id": "p2"},
  1532|         ],
  1533|         [{"export_run_id": "file_1", "pattern_id": "p1"}],
  1534|         ["p1", "p2"],
  1535|     )
  1536|     _write_segment(
  1537|         segments_root,
  1538|         "project_b",
  1539|         domain,
  1540|         [("x1", "same_join", "Same")],
  1541|         [{"export_run_id": "file_3", "pattern_id": "x1"}],
  1542|         [{"export_run_id": "file_3", "pattern_id": "x1"}],
  1543|         ["x1"],
  1544|     )
  1545|     manifest = {
  1546|         "project_a": {**_seg("Project"), "segment_label": "Project A"},
  1547|         "project_b": {**_seg("Project"), "segment_label": "Project B"},
  1548|     }
  1549|     registry = {
  1550|         "project_a": {"output_folder": "project_a", "run_type": "bundle"},
  1551|         "project_b": {"output_folder": "project_b", "run_type": "bundle"},
  1552|     }
  1553| 
  1554|     rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "all"]
  1555| 
  1556|     assert len(rows) == 1
  1557|     assert rows[0]["join_hash"] == "same_join"
  1558|     assert rows[0]["n_segments_present"] == "2"
  1559|     assert rows[0]["n_files_present"] == "3"
  1560| 
  1561| 
  1562| def test_union_inventory_pattern_id_not_cross_segment_identity(tmp_path):
  1563|     domain = "line_patterns"
  1564|     segments_root = tmp_path / "segments"
  1565|     _write_segment(
  1566|         segments_root,
  1567|         "project_a",
  1568|         domain,
  1569|         [("same_pid", "join_a", "A")],
  1570|         [{"export_run_id": "file_1", "pattern_id": "same_pid"}],
  1571|         [{"export_run_id": "file_1", "pattern_id": "same_pid"}],
  1572|         ["same_pid"],
  1573|     )
  1574|     _write_segment(
  1575|         segments_root,
  1576|         "project_b",
  1577|         domain,
  1578|         [("same_pid", "join_b", "B")],
  1579|         [{"export_run_id": "file_2", "pattern_id": "same_pid"}],
  1580|         [{"export_run_id": "file_2", "pattern_id": "same_pid"}],
  1581|         ["same_pid"],
  1582|     )
  1583|     manifest = {
  1584|         "project_a": {**_seg("Project"), "segment_label": "Project A"},
  1585|         "project_b": {**_seg("Project"), "segment_label": "Project B"},
  1586|     }
  1587|     registry = {
  1588|         "project_a": {"output_folder": "project_a", "run_type": "bundle"},
  1589|         "project_b": {"output_folder": "project_b", "run_type": "bundle"},
  1590|     }
  1591| 
  1592|     rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "all"]
  1593| 
  1594|     assert [r["join_hash"] for r in rows] == ["join_a", "join_b"]
  1595| 
  1596| 
  1597| def test_union_inventory_missing_source_cluster_status_no_synthetic_pattern(tmp_path):
  1598|     domain = "line_patterns"
  1599|     base = tmp_path / "segments" / "project" / "results"
  1600|     _write_csv(
  1601|         base / "analysis" / "domain_patterns.csv",
  1602|         [{"domain": domain, "pattern_id": "p1", "source_cluster_id": "", "pattern_label_human": "", "pattern_label": ""}],
  1603|     )
  1604|     _write_csv(
  1605|         base / "bundle_analysis" / "all" / domain / "membership_matrix.csv",
  1606|         [{"export_run_id": "file_1", "pattern_id": "p1"}],
  1607|     )
  1608|     manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
  1609|     registry = {"project": {"output_folder": "project", "run_type": "bundle"}}
  1610| 
  1611|     rows = _union_rows_for(tmp_path, manifest, registry, domain)
  1612| 
  1613|     assert all(row["join_hash"] == "" for row in rows)
  1614|     assert {row["source_status"] for row in rows} == {"missing_source_cluster_id"}
  1615|     assert "no_patterns" in {row["inventory_status"] for row in rows}
  1616| 
  1617| 
  1618| def test_union_inventory_output_order_is_deterministic(tmp_path):
  1619|     domain = "line_patterns"
  1620|     segments_root = tmp_path / "segments"
  1621|     _write_segment(
  1622|         segments_root,
  1623|         "project",
  1624|         domain,
  1625|         [("p2", "join_b", "B"), ("p1", "join_a", "A")],
  1626|         [
  1627|             {"export_run_id": "file_2", "pattern_id": "p2"},
  1628|             {"export_run_id": "file_1", "pattern_id": "p1"},
  1629|         ],
  1630|         [
  1631|             {"export_run_id": "file_2", "pattern_id": "p2"},
  1632|             {"export_run_id": "file_1", "pattern_id": "p1"},
  1633|         ],
  1634|         ["p2", "p1"],
  1635|     )
  1636|     manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
  1637|     registry = {"project": {"output_folder": "project", "run_type": "bundle"}}
  1638| 
  1639|     first = _union_rows_for(tmp_path, manifest, registry, domain)
  1640|     second = _union_rows_for(tmp_path, manifest, registry, domain)
  1641| 
  1642|     assert first == second
  1643|     assert [(r["view_scope"], r["join_hash"]) for r in first] == [
  1644|         ("all", "join_a"),
  1645|         ("all", "join_b"),
  1646|         ("used", "join_a"),
  1647|         ("used", "join_b"),
  1648|     ]
  1649| 
  1650| 
  1651| def test_union_inventory_used_view_unavailable_keeps_source_status_ok(tmp_path):
  1652|     domain = "line_patterns"
  1653|     base = tmp_path / "segments" / "project" / "results"
  1654|     _write_csv(
  1655|         base / "analysis" / "domain_patterns.csv",
  1656|         [{"domain": domain, "pattern_id": "p1", "source_cluster_id": "src|join_a", "pattern_label_human": "A", "pattern_label": "A"}],
  1657|     )
  1658|     _write_csv(
  1659|         base / "bundle_analysis" / "all" / domain / "membership_matrix.csv",
  1660|         [{"export_run_id": "file_1", "pattern_id": "p1"}],
  1661|     )
  1662|     manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
  1663|     registry = {"project": {"output_folder": "project", "run_type": "bundle"}}
  1664| 
  1665|     rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "used"]
  1666| 
  1667|     assert rows == [
  1668|         {
  1669|             "governance_role": "Project",
  1670|             "client_label": "Acme",
  1671|             "discipline_label": "Arch",
  1672|             "unit_system": "imperial",
  1673|             "domain": domain,
  1674|             "view_scope": "used",
  1675|             "join_hash": "",
  1676|             "pattern_label": "",
  1677|             "n_segments_present": "0",
  1678|             "n_files_present": "0",
  1679|             "n_files_denominator": "0",
  1680|             "pct_files_present": "0.000000",
  1681|             "n_projects_present": "0",
  1682|             "n_projects_denominator": "0",
  1683|             "n_clients_present": "1",
  1684|             "n_clients_denominator": "1",
  1685|             "pct_clients_present": "1.000000",
  1686|             "pct_projects_present": "0.000000",
  1687|             "usage_interpretable": "true",
  1688|             "inventory_status": "used_view_unavailable",
  1689|             "source_status": "ok",
  1690|             "executed_utc": "2026-06-22T00:00:00Z",
  1691|         }
  1692|     ]
  1693| 
  1694| 
  1695| 
  1696| def test_union_inventory_client_denominator_includes_status_rows_used_by_reuse(tmp_path):
  1697|     domain = "line_patterns"
  1698|     segments_root = tmp_path / "segments"
  1699|     _write_segment(
  1700|         segments_root,
  1701|         "project_a",
  1702|         domain,
  1703|         [("p1", "shared", "Shared")],
  1704|         [{"export_run_id": "file_a", "pattern_id": "p1"}],
  1705|         [{"export_run_id": "file_a", "pattern_id": "p1"}],
  1706|         ["p1"],
  1707|     )
  1708|     base_b = segments_root / "project_b" / "results"
  1709|     _write_csv(
  1710|         base_b / "analysis" / "domain_patterns.csv",
  1711|         [{"domain": domain, "pattern_id": "p1", "source_cluster_id": "src|shared", "pattern_label_human": "Shared", "pattern_label": "Shared"}],
  1712|     )
  1713|     _write_csv(
  1714|         base_b / "bundle_analysis" / "all" / domain / "membership_matrix.csv",
  1715|         [{"export_run_id": "file_b", "pattern_id": "p1"}],
  1716|     )
  1717|     manifest = {
  1718|         "project_a": {**_seg("Project", client="A"), "segment_label": "Project A"},
  1719|         "project_b": {**_seg("Project", client="B"), "segment_label": "Project B"},
  1720|     }
  1721|     registry = {
  1722|         "project_a": {"output_folder": "project_a", "run_type": "bundle"},
  1723|         "project_b": {"output_folder": "project_b", "run_type": "bundle"},
  1724|     }
  1725| 
  1726|     union_rows = _union_rows_for(tmp_path, manifest, registry, domain)
  1727|     shared = [r for r in union_rows if r["view_scope"] == "used" and r["join_hash"] == "shared"][0]
  1728|     reuse = build_pattern_reuse_distribution_rows(union_rows, "2026-06-22T00:00:00Z")
  1729|     reuse_shared = [r for r in reuse if r["view_scope"] == "used" and r["join_hash"] == "shared"][0]
  1730| 
  1731|     assert shared["n_clients_present"] == "1"
  1732|     assert shared["n_clients_denominator"] == "2"
  1733|     assert shared["pct_clients_present"] == "0.500000"
  1734|     status_row = [r for r in union_rows if r["view_scope"] == "used" and r["inventory_status"] == "used_view_unavailable"][0]
  1735|     reuse_status = [r for r in reuse if r["view_scope"] == "used" and r["inventory_status"] == "used_view_unavailable"][0]
  1736| 
  1737|     assert reuse_shared["n_clients_denominator"] == shared["n_clients_denominator"]
  1738|     assert reuse_shared["pct_clients_present"] == shared["pct_clients_present"]
  1739|     assert status_row["n_clients_present"] == "1"
  1740|     assert status_row["n_clients_denominator"] == "2"
  1741|     assert status_row["pct_clients_present"] == "0.500000"
  1742|     assert reuse_status["n_clients_present"] == status_row["n_clients_present"]
  1743|     assert reuse_status["n_clients_denominator"] == status_row["n_clients_denominator"]
  1744|     assert reuse_status["pct_clients_present"] == status_row["pct_clients_present"]
  1745| 
  1746| def test_union_inventory_missing_domain_patterns_keeps_source_status_ok(tmp_path):
  1747|     domain = "line_patterns"
  1748|     (tmp_path / "segments" / "project").mkdir(parents=True)
  1749|     manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
  1750|     registry = {"project": {"output_folder": "project", "run_type": "bundle"}}
  1751| 
  1752|     rows = _union_rows_for(tmp_path, manifest, registry, domain)
  1753| 
  1754|     assert {row["inventory_status"] for row in rows} == {"missing_domain_patterns"}
  1755|     assert {row["source_status"] for row in rows} == {"ok"}
  1756| 
  1757| 
  1758| def test_pattern_reuse_many_files_gets_broad_classification():
  1759|     rows = [
  1760|         {
  1761|             "view_scope": "used", "governance_role": "Project", "client_label": "Acme",
  1762|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1763|             "join_hash": "broad", "pattern_label": "Broad", "n_files_present": "4",
  1764|             "n_files_denominator": "5", "n_projects_present": "2", "n_projects_denominator": "2",
  1765|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1766|             "usage_interpretable": "true", "inventory_status": "ok",
  1767|         }
  1768|     ]
  1769| 
  1770|     out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1771| 
  1772|     assert out[0]["reuse_bucket"] == "client_wide"
  1773|     assert out[0]["bucket_basis"] == "files_in_role_client_domain"
  1774|     assert out[0]["pct_files_present"] == "0.800000"
  1775| 
  1776| 
  1777| def test_pattern_reuse_one_file_gets_single_file_classification():
  1778|     rows = [
  1779|         {
  1780|             "view_scope": "all", "governance_role": "Project", "client_label": "Acme",
  1781|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1782|             "join_hash": "one", "pattern_label": "One", "n_files_present": "1",
  1783|             "n_files_denominator": "3", "n_projects_present": "1", "n_projects_denominator": "2",
  1784|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1785|             "usage_interpretable": "true", "inventory_status": "ok",
  1786|         }
  1787|     ]
  1788| 
  1789|     out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1790| 
  1791|     assert out[0]["reuse_bucket"] == "single_file"
  1792|     assert out[0]["bucket_basis"] == "files_in_role_client_domain"
  1793| 
  1794| 
  1795| def test_project_used_view_uses_project_and_file_denominators_for_emerging_bucket():
  1796|     rows = [
  1797|         {
  1798|             "view_scope": "used", "governance_role": "Project", "client_label": "Acme",
  1799|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1800|             "join_hash": "multi", "pattern_label": "Multi", "n_files_present": "2",
  1801|             "n_files_denominator": "5", "n_projects_present": "2", "n_projects_denominator": "3",
  1802|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1803|             "usage_interpretable": "true", "inventory_status": "ok",
  1804|         }
  1805|     ]
  1806| 
  1807|     out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1808| 
  1809|     assert out[0]["reuse_bucket"] == "emerging"
  1810|     assert out[0]["bucket_basis"] == "files_in_role_client_domain"
  1811|     assert out[0]["pct_files_present"] == "0.400000"
  1812|     assert out[0]["pct_projects_present"] == "0.666667"
  1813| 
  1814| 
  1815| def test_single_project_reuse_takes_precedence_over_emerging():
  1816|     rows = [
  1817|         {
  1818|             "view_scope": "used", "governance_role": "Project", "client_label": "Acme",
  1819|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1820|             "join_hash": "single_project", "pattern_label": "Single Project",
  1821|             "n_files_present": "2", "n_files_denominator": "5",
  1822|             "n_projects_present": "1", "n_projects_denominator": "3",
  1823|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1824|             "usage_interpretable": "true", "inventory_status": "ok",
  1825|         }
  1826|     ]
  1827| 
  1828|     out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1829| 
  1830|     assert out[0]["reuse_bucket"] == "single_project"
  1831|     assert out[0]["bucket_basis"] == "projects_in_client_domain"
  1832| 
  1833| 
  1834| def test_missing_source_identity_degrades_reuse_classification():
  1835|     rows = [
  1836|         {
  1837|             "view_scope": "all", "governance_role": "Project", "client_label": "Acme",
  1838|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1839|             "join_hash": "partial", "pattern_label": "Partial",
  1840|             "n_files_present": "4", "n_files_denominator": "4",
  1841|             "n_projects_present": "2", "n_projects_denominator": "2",
  1842|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1843|             "usage_interpretable": "true", "inventory_status": "ok",
  1844|             "source_status": "missing_source_cluster_id",
  1845|         }
  1846|     ]
  1847| 
  1848|     out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1849| 
  1850|     assert out[0]["reuse_bucket"] == "unclassified"
  1851|     assert out[0]["bucket_basis"] == "source_status"
  1852|     assert out[0]["classification_status"] == "degraded_missing_source_cluster_id"
  1853| 
  1854| 
  1855| def test_template_all_view_is_not_interpreted_as_active_usage():
  1856|     rows = [
  1857|         {
  1858|             "view_scope": "all", "governance_role": "Template", "client_label": "Acme",
  1859|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1860|             "join_hash": "stock", "pattern_label": "Stock", "n_files_present": "1",
  1861|             "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1",
  1862|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1863|             "usage_interpretable": "false", "inventory_status": "ok",
  1864|         }
  1865|     ]
  1866| 
  1867|     out = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1868| 
  1869|     assert out[0]["usage_interpretable"] == "false"
  1870|     assert out[0]["reuse_bucket"] == "client_wide"
  1871| 
  1872| 
  1873| def test_reuse_zero_denominator_is_degraded_unclassified():
  1874|     bucket, basis, status = _reuse_bucket_for(
  1875|         n_files=0, n_files_den=0, n_projects=0, n_projects_den=0, n_clients=0, n_clients_den=0
  1876|     )
  1877| 
  1878|     assert bucket == "unclassified"
  1879|     assert basis == "denominator_unavailable"
  1880|     assert status == "degraded_zero_denominator"
  1881| 
  1882| 
  1883| def test_reuse_distribution_order_is_deterministic():
  1884|     rows = [
  1885|         {
  1886|             "view_scope": "all", "governance_role": "Project", "client_label": "Acme",
  1887|             "discipline_label": "Arch", "unit_system": "imperial", "domain": "line_patterns",
  1888|             "join_hash": jh, "pattern_label": jh, "n_files_present": "1",
  1889|             "n_files_denominator": "2", "n_projects_present": "1", "n_projects_denominator": "1",
  1890|             "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000",
  1891|             "usage_interpretable": "true", "inventory_status": "ok",
  1892|         }
  1893|         for jh in ["b", "a"]
  1894|     ]
  1895| 
  1896|     first = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1897|     second = build_pattern_reuse_distribution_rows(rows, "2026-06-22T00:00:00Z")
  1898| 
  1899|     assert first == second
  1900|     assert [r["join_hash"] for r in first] == ["a", "b"]
  1901| 
  1902| 
  1903| def test_reuse_thresholds_are_centralized_and_used():
  1904|     assert "client_wide_min_pct_files" in REUSE_BUCKET_THRESHOLDS
  1905|     bucket, basis, status = _reuse_bucket_for(
  1906|         n_files=4, n_files_den=5, n_projects=1, n_projects_den=2, n_clients=1, n_clients_den=1
  1907|     )
  1908| 
  1909|     assert bucket == "client_wide"
  1910|     assert basis == "files_in_role_client_domain"
  1911|     assert status == "ok"
  1912| 
  1913| 
  1914| def test_explicit_matrices_union_jaccard_differs_from_mean_file_pair():
  1915|     from compare_cross_segment import build_explicit_matrix_outputs
  1916| 
  1917|     union_rows = [
  1918|         {"governance_role": "Project", "client_label": "A", "discipline_label": "Arch", "unit_system": "imperial", "domain": "d", "view_scope": "all", "join_hash": j, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"}
  1919|         for j in ("x", "y")
  1920|     ] + [
  1921|         {"governance_role": "Project", "client_label": "B", "discipline_label": "Arch", "unit_system": "imperial", "domain": "d", "view_scope": "all", "join_hash": j, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"}
  1922|         for j in ("x", "y")
  1923|     ]
  1924|     summary = [{
  1925|         "governance_role_a": "Project", "governance_role_b": "Project",
  1926|         "client_label_a": "A", "client_label_b": "B",
  1927|         "discipline_label_a": "Arch", "discipline_label_b": "Arch", "unit_system": "imperial",
  1928|         "segment_label_a": "Project A", "segment_label_b": "Project B",
  1929|         "domain": "d", "all_pairwise_jaccard_mean": "0.000000", "used_pairwise_jaccard_mean": "",
  1930|     }]
  1931| 
  1932|     matrices, frag, manifest = build_explicit_matrix_outputs(summary, [], union_rows, "2026-06-22T00:00:00Z")
  1933| 
  1934|     union_ab = [r for r in matrices["project_union_jaccard_matrix.csv"] if r["row_id"] == "Project A" and r["column_id"] == "Project B"][0]
  1935|     pair_ab = [r for r in matrices["project_mean_file_pair_jaccard_matrix.csv"] if r["row_id"] == "Project A" and r["column_id"] == "Project B" and r["domain"] == "d"][0]
  1936|     assert union_ab["value"] == "1.000000"
  1937|     assert pair_ab["value"] == "0.000000"
  1938|     frag_ab = [r for r in frag if r["row_id"] == "Project A" and r["column_id"] == "Project B"][0]
  1939|     assert frag_ab["fragmentation_diagnostic"] == "1.000000"
  1940|     assert frag_ab["domain"] == "ALL_DOMAINS"
  1941|     assert [m["matrix_name"] for m in manifest] == sorted(m["matrix_name"] for m in manifest)
  1942| 
  1943| 
  1944| def test_fragmentation_diagnostic_uses_all_domains_file_pair_aggregate():
  1945|     from compare_cross_segment import build_explicit_matrix_outputs
  1946| 
  1947|     union_rows = []
  1948|     for client in ("A", "B"):
  1949|         for domain, hashes in {"d1": ["shared"], "d2": [f"{client}_unique"]}.items():
  1950|             for jh in hashes:
  1951|                 union_rows.append({"governance_role": "Project", "client_label": client, "discipline_label": "Arch", "unit_system": "imperial", "domain": domain, "view_scope": "all", "join_hash": jh, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"})
  1952|     summary = [
  1953|         {"governance_role_a": "Project", "governance_role_b": "Project", "client_label_a": "A", "client_label_b": "B", "discipline_label_a": "Arch", "discipline_label_b": "Arch", "unit_system": "imperial", "segment_label_a": "Project A", "segment_label_b": "Project B", "domain": "d2", "all_pairwise_jaccard_mean": "0.000000"},
  1954|         {"governance_role_a": "Project", "governance_role_b": "Project", "client_label_a": "A", "client_label_b": "B", "discipline_label_a": "Arch", "discipline_label_b": "Arch", "unit_system": "imperial", "segment_label_a": "Project A", "segment_label_b": "Project B", "domain": "d1", "all_pairwise_jaccard_mean": "1.000000"},
  1955|     ]
  1956| 
  1957|     matrices, frag, _ = build_explicit_matrix_outputs(summary, [], union_rows, "2026-06-22T00:00:00Z")
  1958| 
  1959|     aggregate = [r for r in matrices["project_mean_file_pair_jaccard_matrix.csv"] if r["row_id"] == "Project A" and r["column_id"] == "Project B" and r["domain"] == "ALL_DOMAINS"][0]
  1960|     assert aggregate["value"] == "0.500000"
  1961|     frag_ab = [r for r in frag if r["row_id"] == "Project A" and r["column_id"] == "Project B"][0]
  1962|     assert frag_ab["domain"] == "ALL_DOMAINS"
  1963|     assert frag_ab["exact_identity_overlap"] == "0.500000"
  1964| 
  1965| 
  1966| def test_density_similarity_uses_domain_density_vectors_not_containment():
  1967|     from compare_cross_segment import build_explicit_matrix_outputs
  1968| 
  1969|     union_rows = []
  1970|     for client, domains in {"A": {"d1": ["a"], "d2": ["b", "c"]}, "B": {"d1": ["x"], "d2": ["y", "z"]}}.items():
  1971|         for domain, hashes in domains.items():
  1972|             for jh in hashes:
  1973|                 union_rows.append({"governance_role": "Project", "client_label": client, "discipline_label": "Arch", "unit_system": "imperial", "domain": domain, "view_scope": "all", "join_hash": jh, "n_files_present": "1", "n_files_denominator": "1", "n_projects_present": "1", "n_projects_denominator": "1", "n_clients_present": "1", "n_clients_denominator": "1", "pct_clients_present": "1.000000", "inventory_status": "ok"})
  1974|     pooled = [{"governance_role": "Project", "segment_label": "A", "domain": "d1", "all_containment_focal_in_pool": "0.123456"}]
  1975| 
  1976|     matrices, _, _ = build_explicit_matrix_outputs([], pooled, union_rows, "2026-06-22T00:00:00Z")
  1977|     density_ab = [r for r in matrices["project_density_similarity_matrix.csv"] if r["row_id"] == "Project|A|Arch|imperial" and r["column_id"] == "Project|B|Arch|imperial"][0]
  1978|     pool_row = matrices["project_pool_containment_similarity_matrix.csv"][0]
  1979|     assert density_ab["metric"] == "density_similarity"
  1980|     assert density_ab["value"] == "1.000000"
  1981|     assert pool_row["metric"] == "pool_containment_similarity"
  1982|     assert pool_row["value"] == "0.123456"
  1983| 
  1984| 
```
