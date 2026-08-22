# Chunk of tests/test_build_segment_manifest.py

- Source relative path: `tests/test_build_segment_manifest.py`
- Chunk: 4 of 4
- Original line range: 1533-1886
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_collection_label_column_absence_produces_identical_manifest, test_collection_label_column_absence_produces_identical_manifest._write_and_build, _write_metadata_csv, test_required_field_blank_blocks_entire_build, test_required_field_na_sentinel_blocks_entire_build, test_required_field_semicolon_blocks_entire_build, test_export_run_id_semicolon_does_not_block_build, test_validate_required_metadata_reports_row_and_field_directly, test_validate_required_metadata_empty_for_fully_valid_rows, test_duplicate_export_run_id_blocks_as_distinct_conflict_reason, test_unreadable_input_reported_distinctly_not_bare_except, test_business_center_0000_is_a_valid_value_not_a_validation_failure, test_business_center_0000_main_succeeds, test_project_label_not_a_required_field, test_project_label_sentinel_does_not_affect_segmentation, test_project_label_sentinel_does_not_affect_segmentation._build_with_project_label, test_running_builder_twice_on_identical_input_is_byte_identical, test_reordering_input_rows_does_not_change_segment_ids_or_parents, test_former_collection_specific_rows_collapse_with_union_membership, test_ancestor_segment_ids_semicolon_joined_not_pipe, test_ancestor_segment_ids_two_element_roundtrip
- Source SHA-256: 9f3ece62e3859182daaa40d64fa48a48dce0364f40520d18b071b30a096c99c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  1533| def test_collection_label_column_absence_produces_identical_manifest(tmp_path):
  1534|     # A metadata file with a collection_label column vs. one entirely without
  1535|     # it must produce byte-identical segment_manifest.csv content (ignoring
  1536|     # collection_label really means ignoring it, column present or not).
  1537|     base_rows = [
  1538|         _full_row(f"r{i:02d}", "imperial", "ClientAlpha", "Container", "architectural", "1450")
  1539|         for i in range(3)
  1540|     ]
  1541| 
  1542|     def _write_and_build(out_name, extra_field):
  1543|         fieldnames = list(VALID_FIELDNAMES)
  1544|         if extra_field:
  1545|             fieldnames = fieldnames + ["collection_label"]
  1546|         meta = tmp_path / f"{out_name}.csv"
  1547|         with meta.open("w", newline="") as f:
  1548|             w = csv.DictWriter(f, fieldnames=fieldnames)
  1549|             w.writeheader()
  1550|             for row in base_rows:
  1551|                 r = dict(row)
  1552|                 if extra_field:
  1553|                     r["collection_label"] = "ClientAlpha Standards"
  1554|                 w.writerow(r)
  1555|         out_dir = tmp_path / out_name
  1556|         rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1557|         assert rc == 0
  1558|         return _read_csv(out_dir / "segment_manifest.csv")
  1559| 
  1560|     with_coll = _write_and_build("with_coll", True)
  1561|     without_coll = _write_and_build("without_coll", False)
  1562|     assert with_coll == without_coll
  1563| 
  1564| 
  1565| # ---------------------------------------------------------------------------
  1566| # PR "segment builder explicit contract" -- required-field blocking. Missing
  1567| # or N/A-sentinel value in export_run_id/unit_system/governance_role/
  1568| # client_label/discipline_label/business_center_label blocks the ENTIRE
  1569| # build; no partial manifest is ever written.
  1570| # ---------------------------------------------------------------------------
  1571| 
  1572| def _write_metadata_csv(path, rows):
  1573|     with path.open("w", newline="") as f:
  1574|         w = csv.DictWriter(f, fieldnames=VALID_FIELDNAMES)
  1575|         w.writeheader()
  1576|         for row in rows:
  1577|             w.writerow(row)
  1578| 
  1579| 
  1580| @pytest.mark.parametrize("field", ["export_run_id", "unit_system", "governance_role", "client_label", "discipline_label", "business_center_label"])
  1581| def test_required_field_blank_blocks_entire_build(tmp_path, capsys, field):
  1582|     rows = [dict(r) for r in VALID_ROWS]
  1583|     rows[3][field] = ""
  1584|     meta = tmp_path / "file_metadata.csv"
  1585|     _write_metadata_csv(meta, rows)
  1586|     out_dir = tmp_path / "out"
  1587| 
  1588|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1589| 
  1590|     assert rc == 1, f"blank {field} must block the build"
  1591|     assert not (out_dir / "segment_manifest.csv").exists()
  1592|     assert not (out_dir / "run_registry.csv").exists()
  1593|     assert not (out_dir / "segment_membership.csv").exists()
  1594|     captured = capsys.readouterr()
  1595|     assert "BLOCKED" in captured.err
  1596|     assert f"field={field}" in captured.err
  1597|     assert "reason=missing_value" in captured.err
  1598|     # row 3 of VALID_ROWS is the 4th data row -> CSV row_number 5 (1=header).
  1599|     assert "row=5" in captured.err
  1600| 
  1601| 
  1602| @pytest.mark.parametrize("field", ["export_run_id", "unit_system", "governance_role", "client_label", "discipline_label", "business_center_label"])
  1603| def test_required_field_na_sentinel_blocks_entire_build(tmp_path, capsys, field):
  1604|     rows = [dict(r) for r in VALID_ROWS]
  1605|     rows[0][field] = "N/A"
  1606|     meta = tmp_path / "file_metadata.csv"
  1607|     _write_metadata_csv(meta, rows)
  1608|     out_dir = tmp_path / "out"
  1609| 
  1610|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1611| 
  1612|     assert rc == 1, f"N/A {field} must block the build"
  1613|     assert not (out_dir / "segment_manifest.csv").exists()
  1614|     captured = capsys.readouterr()
  1615|     assert "BLOCKED" in captured.err
  1616|     assert f"field={field}" in captured.err
  1617|     assert "reason=not_applicable_sentinel" in captured.err
  1618| 
  1619| 
  1620| @pytest.mark.parametrize("field", ["unit_system", "governance_role", "client_label", "discipline_label", "business_center_label"])
  1621| def test_required_field_semicolon_blocks_entire_build(tmp_path, capsys, field):
  1622|     # D-028 review finding (PR #423): a dimension value containing ";" would
  1623|     # silently reintroduce the exact delimiter-collision bug ";" was chosen
  1624|     # to fix, since _build_segments() now joins ancestor_segment_ids with
  1625|     # ";". Reject it at the metadata-validation source instead. Only the 5
  1626|     # DIMENSION_CONFIG fields are checked -- export_run_id is a separate
  1627|     # case, see test_export_run_id_semicolon_does_not_block_build below.
  1628|     rows = [dict(r) for r in VALID_ROWS]
  1629|     rows[0][field] = "Acme;West"
  1630|     meta = tmp_path / "file_metadata.csv"
  1631|     _write_metadata_csv(meta, rows)
  1632|     out_dir = tmp_path / "out"
  1633| 
  1634|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1635| 
  1636|     assert rc == 1, f"';' in {field} must block the build"
  1637|     assert not (out_dir / "segment_manifest.csv").exists()
  1638|     captured = capsys.readouterr()
  1639|     assert "BLOCKED" in captured.err
  1640|     assert f"field={field}" in captured.err
  1641|     assert "reason=semicolon_not_allowed" in captured.err
  1642| 
  1643| 
  1644| def test_export_run_id_semicolon_does_not_block_build(tmp_path, capsys):
  1645|     # PR #423 review finding: export_run_id is never embedded in segment_id
  1646|     # or ancestor_segment_ids, so it can't collide with the ";" delimiter --
  1647|     # the semicolon restriction only applies to DIMENSION_CONFIG fields.
  1648|     rows = [dict(r) for r in VALID_ROWS]
  1649|     rows[0]["export_run_id"] = "r99;odd_but_valid"
  1650|     meta = tmp_path / "file_metadata.csv"
  1651|     _write_metadata_csv(meta, rows)
  1652|     out_dir = tmp_path / "out"
  1653| 
  1654|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1655| 
  1656|     assert rc == 0
  1657|     assert (out_dir / "segment_manifest.csv").exists()
  1658|     captured = capsys.readouterr()
  1659|     assert "semicolon_not_allowed" not in captured.err
  1660| 
  1661| 
  1662| def test_validate_required_metadata_reports_row_and_field_directly():
  1663|     rows = [dict(r) for r in VALID_ROWS[:2]]
  1664|     rows[1]["business_center_label"] = ""
  1665|     diagnostics = _validate_required_metadata(rows)
  1666|     assert len(diagnostics) == 1
  1667|     d = diagnostics[0]
  1668|     assert d["field"] == "business_center_label"
  1669|     assert d["reason"] == "missing_value"
  1670|     assert d["row_number"] == "3"  # header=1, rows[0]=2, rows[1]=3
  1671|     assert d["export_run_id"] == rows[1]["export_run_id"]
  1672| 
  1673| 
  1674| def test_validate_required_metadata_empty_for_fully_valid_rows():
  1675|     assert _validate_required_metadata(VALID_ROWS) == []
  1676| 
  1677| 
  1678| def test_duplicate_export_run_id_blocks_as_distinct_conflict_reason(tmp_path, capsys):
  1679|     rows = [dict(r) for r in VALID_ROWS]
  1680|     dup = dict(rows[0]); dup["export_run_id"] = rows[1]["export_run_id"]
  1681|     rows.append(dup)
  1682|     meta = tmp_path / "file_metadata.csv"
  1683|     _write_metadata_csv(meta, rows)
  1684|     out_dir = tmp_path / "out"
  1685| 
  1686|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1687| 
  1688|     assert rc == 1
  1689|     assert not (out_dir / "segment_manifest.csv").exists()
  1690|     captured = capsys.readouterr()
  1691|     assert "duplicate_row_conflict" in captured.err
  1692|     assert f"export_run_id={rows[1]['export_run_id']}" in captured.err
  1693| 
  1694| 
  1695| def test_unreadable_input_reported_distinctly_not_bare_except(tmp_path, capsys):
  1696|     # A file that exists but cannot be decoded as UTF-8/text (e.g. binary
  1697|     # garbage) must be reported as an "Unreadable input" failure, distinct
  1698|     # from a "BLOCKED" required-metadata failure, and must not crash with an
  1699|     # unhandled traceback.
  1700|     meta = tmp_path / "file_metadata.csv"
  1701|     meta.write_bytes(b"\xff\xfe\x00\xff\xff\xfe\x00\x01garbage-not-utf8-\xfe\xff")
  1702|     out_dir = tmp_path / "out"
  1703| 
  1704|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1705| 
  1706|     assert rc == 1
  1707|     assert not (out_dir / "segment_manifest.csv").exists()
  1708|     captured = capsys.readouterr()
  1709|     assert "Unreadable input" in captured.err
  1710|     assert "BLOCKED" not in captured.err
  1711| 
  1712| 
  1713| # ---------------------------------------------------------------------------
  1714| # PR "segment builder explicit contract" -- business_center_label="0000" must
  1715| # never be treated as missing/N-A by validation (it is a valid literal).
  1716| # ---------------------------------------------------------------------------
  1717| 
  1718| def test_business_center_0000_is_a_valid_value_not_a_validation_failure():
  1719|     rows = [_full_row(f"r{i:02d}", "imperial", "InternalEnterprise", "Container", "architectural", "0000") for i in range(3)]
  1720|     assert _validate_required_metadata(rows) == []
  1721| 
  1722| 
  1723| def test_business_center_0000_main_succeeds(tmp_path):
  1724|     rows = [_full_row(f"r{i:02d}", "imperial", "InternalEnterprise", "Container", "architectural", "0000") for i in range(3)]
  1725|     meta = tmp_path / "file_metadata.csv"
  1726|     _write_metadata_csv(meta, rows)
  1727|     out_dir = tmp_path / "out"
  1728|     rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1729|     assert rc == 0
  1730|     manifest_rows = _read_csv(out_dir / "segment_manifest.csv")
  1731|     assert any(r["business_center_label"] == "0000" for r in manifest_rows)
  1732| 
  1733| 
  1734| # ---------------------------------------------------------------------------
  1735| # PR "segment builder explicit contract" -- project_label sentinel handling.
  1736| # project_label is not a DIMENSION_CONFIG field and is not read by this file
  1737| # at all, so it never participates in segmentation and may carry any value
  1738| # (including an explicit not-applicable sentinel) without affecting output.
  1739| # ---------------------------------------------------------------------------
  1740| 
  1741| def test_project_label_not_a_required_field():
  1742|     assert "project_label" not in REQUIRED_ROW_FIELDS
  1743|     assert "project_label" not in [d["field"] for d in DIMENSION_CONFIG]
  1744| 
  1745| 
  1746| def test_project_label_sentinel_does_not_affect_segmentation(tmp_path):
  1747|     # An extra project_label column carrying an explicit not-applicable
  1748|     # sentinel (permitted only for this field) segments identically to the
  1749|     # same rows with a different, non-participating project_label value —
  1750|     # project_label plays no role in segment identity either way.
  1751|     rows_a = [dict(r, project_label="__NOT_APPLICABLE__") for r in VALID_ROWS]
  1752|     rows_b = [dict(r, project_label="Some Other Project") for r in VALID_ROWS]
  1753| 
  1754|     def _build_with_project_label(out_name, rows):
  1755|         fieldnames = VALID_FIELDNAMES + ["project_label"]
  1756|         meta = tmp_path / f"{out_name}.csv"
  1757|         with meta.open("w", newline="") as f:
  1758|             w = csv.DictWriter(f, fieldnames=fieldnames)
  1759|             w.writeheader()
  1760|             for row in rows:
  1761|                 w.writerow(row)
  1762|         out_dir = tmp_path / out_name
  1763|         rc = main(["--metadata-file", str(meta), "--out-dir", str(out_dir), "--min-files", "1"])
  1764|         assert rc == 0
  1765|         return _read_csv(out_dir / "segment_manifest.csv")
  1766| 
  1767|     manifest_a = _build_with_project_label("proj_a", rows_a)
  1768|     manifest_b = _build_with_project_label("proj_b", rows_b)
  1769|     assert manifest_a == manifest_b
  1770| 
  1771| 
  1772| # ---------------------------------------------------------------------------
  1773| # PR "segment builder explicit contract" -- determinism. Identical input ->
  1774| # identical output; reordering input rows doesn't change segment_ids, parent
  1775| # ids, or sorted memberships.
  1776| # ---------------------------------------------------------------------------
  1777| 
  1778| def test_running_builder_twice_on_identical_input_is_byte_identical(tmp_path):
  1779|     meta = tmp_path / "file_metadata.csv"
  1780|     _write_metadata_csv(meta, VALID_ROWS)
  1781| 
  1782|     out1 = tmp_path / "out1"
  1783|     out2 = tmp_path / "out2"
  1784|     assert main(["--metadata-file", str(meta), "--out-dir", str(out1), "--min-files", "1"]) == 0
  1785|     assert main(["--metadata-file", str(meta), "--out-dir", str(out2), "--min-files", "1"]) == 0
  1786| 
  1787|     for name in ("segment_manifest.csv", "run_registry.csv", "segment_membership.csv"):
  1788|         assert _read_csv(out1 / name) == _read_csv(out2 / name), f"{name} not byte-identical across runs"
  1789| 
  1790| 
  1791| def test_reordering_input_rows_does_not_change_segment_ids_or_parents(tmp_path):
  1792|     import random
  1793|     shuffled = list(VALID_ROWS)
  1794|     random.Random(42).shuffle(shuffled)
  1795| 
  1796|     meta_orig = tmp_path / "orig.csv"
  1797|     meta_shuf = tmp_path / "shuf.csv"
  1798|     _write_metadata_csv(meta_orig, VALID_ROWS)
  1799|     _write_metadata_csv(meta_shuf, shuffled)
  1800| 
  1801|     out_orig = tmp_path / "out_orig"
  1802|     out_shuf = tmp_path / "out_shuf"
  1803|     assert main(["--metadata-file", str(meta_orig), "--out-dir", str(out_orig), "--min-files", "1"]) == 0
  1804|     assert main(["--metadata-file", str(meta_shuf), "--out-dir", str(out_shuf), "--min-files", "1"]) == 0
  1805| 
  1806|     manifest_orig = {r["segment_id"]: (r["parent_segment_id"], r["population_hash"]) for r in _read_csv(out_orig / "segment_manifest.csv")}
  1807|     manifest_shuf = {r["segment_id"]: (r["parent_segment_id"], r["population_hash"]) for r in _read_csv(out_shuf / "segment_manifest.csv")}
  1808|     assert manifest_orig == manifest_shuf
  1809| 
  1810|     membership_orig = sorted((r["segment_id"], r["export_run_id"]) for r in _read_csv(out_orig / "segment_membership.csv"))
  1811|     membership_shuf = sorted((r["segment_id"], r["export_run_id"]) for r in _read_csv(out_shuf / "segment_membership.csv"))
  1812|     assert membership_orig == membership_shuf
  1813| 
  1814| 
  1815| # ---------------------------------------------------------------------------
  1816| # PR "segment builder explicit contract" -- collapse after collection
  1817| # removal. Former collection-specific rows that now collapse into one
  1818| # segment retain the union of all distinct file memberships exactly once.
  1819| # ---------------------------------------------------------------------------
  1820| 
  1821| def test_former_collection_specific_rows_collapse_with_union_membership():
  1822|     # Before this PR, two rows sharing every dimension except collection_label
  1823|     # would have produced two distinct collection-scoped segments. Now they
  1824|     # collapse into a single segment whose membership is the exact union of
  1825|     # both groups' export_run_ids, with no duplicates.
  1826|     base = dict(unit_system="imperial", governance_role="Template", client_label="ClientBeta",
  1827|                 discipline_label="architectural", business_center_label="1450")
  1828|     rows_collection_a = [dict(base, export_run_id=f"a{i:02d}", collection_label="ClientBeta Standards") for i in range(3)]
  1829|     rows_collection_b = [dict(base, export_run_id=f"b{i:02d}", collection_label="Legacy") for i in range(2)]
  1830|     all_rows = rows_collection_a + rows_collection_b
  1831| 
  1832|     segs = _build_segments(all_rows, min_files=1)
  1833|     leaf = next(r for r in segs if r["segment_id"] == "imperial|Template|ClientBeta|architectural|1450")
  1834| 
  1835|     expected_eids = {r["export_run_id"] for r in all_rows}
  1836|     actual_eids = set(leaf["export_run_ids"].split("|"))
  1837|     assert actual_eids == expected_eids
  1838|     assert len(leaf["export_run_ids"].split("|")) == len(expected_eids), "no duplicate export_run_ids in the collapsed membership"
  1839| 
  1840|     membership = _build_membership_rows(segs)
  1841|     leaf_membership = [m for m in membership if m["segment_id"] == "imperial|Template|ClientBeta|architectural|1450"]
  1842|     assert {m["export_run_id"] for m in leaf_membership} == expected_eids
  1843|     assert len(leaf_membership) == len(expected_eids), "each file appears exactly once in segment_membership rows"
  1844| 
  1845| 
  1846| # ---------------------------------------------------------------------------
  1847| # ancestor_segment_ids serialization (D-028)
  1848| # ---------------------------------------------------------------------------
  1849| 
  1850| def test_ancestor_segment_ids_semicolon_joined_not_pipe():
  1851|     # imperial|Container|ClientAlpha|Architectural has 3 non-root fields present
  1852|     # (governance, client, discipline), so it has 3 immediate one-field-drop
  1853|     # ancestors -- a genuine multi-ancestor case, not a degenerate 1-element one.
  1854|     segs = _build_segments(_disc_rows(), min_files=3)
  1855|     leaf = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha|Architectural")
  1856|     raw = leaf["ancestor_segment_ids"]
  1857| 
  1858|     expected_ancestor_ids = [
  1859|         "imperial|Container|Architectural",
  1860|         "imperial|Container|ClientAlpha",
  1861|         "imperial|ClientAlpha|Architectural",
  1862|     ]
  1863|     assert raw == ";".join(sorted(expected_ancestor_ids))
  1864| 
  1865|     # Round trip: splitting on ";" recovers the exact original list, with each
  1866|     # element's own internal "|" delimiters untouched.
  1867|     recovered = raw.split(";")
  1868|     assert recovered == sorted(expected_ancestor_ids)
  1869|     for ancestor_id in recovered:
  1870|         assert "|" in ancestor_id, "each ancestor id keeps its own internal pipe delimiters intact"
  1871| 
  1872|     # Contrast: the prior "|".join(ancestor_ids) encoding collapsed the outer
  1873|     # and inner delimiters into one ambiguous string that could not be split
  1874|     # back into the original list (D-028) -- demonstrate the old encoding is
  1875|     # indeed lossy for this same fixture, as the reason the fix was needed.
  1876|     lossy_old_encoding = "|".join(expected_ancestor_ids)
  1877|     assert lossy_old_encoding.split("|") != expected_ancestor_ids
  1878| 
  1879| 
  1880| def test_ancestor_segment_ids_two_element_roundtrip():
  1881|     # A simpler 2-ancestor case (2 non-root fields present).
  1882|     segs = _build_segments(_disc_rows(), min_files=3)
  1883|     seg = next(r for r in segs if r["segment_id"] == "imperial|Container|ClientAlpha")
  1884|     expected = ["imperial|Container", "imperial|ClientAlpha"]
  1885|     assert seg["ancestor_segment_ids"] == ";".join(sorted(expected))
  1886|     assert seg["ancestor_segment_ids"].split(";") == sorted(expected)
```
