# Chunk of tools/governance_evidence_package.py

- Source relative path: `tools/governance_evidence_package.py`
- Chunk: 6 of 6
- Original line range: 1482-1621
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _classify_scalar, _column_dtype, _scan_csv_file, inventory_export_directory_files, build_file_inventory_document
- Source SHA-256: 2fece0426163550ef83e302b52b9f002b12123e12eb35430df07c3d1f4c4b1f3
- Starts inside symbol: no
- Ends inside symbol: no

```
  1482| 
  1483| 
  1484| # ── file inventory (live directory scan) ─────────────────────────────────────
  1485| # Step 0 for this feature confirmed: (a) no query/tool-calling path exists
  1486| # anywhere in this package -- generate_governance_narrative.py's outputs are
  1487| # consumed single-shot, so an LLM reader can only know a drill-down file
  1488| # exists if this package says so; (b) no prior "csv_inventory.md"-style
  1489| # utility exists in this repo. The functions below are the mechanical
  1490| # directory-scan/schema-inference layer that fills that gap -- they read
  1491| # column headers and infer per-column dtype from the data, but never retain
  1492| # or report sample values (inventory, not analysis).
  1493| 
  1494| def _classify_scalar(value: str) -> str:
  1495|     """Classify one non-blank cell value as 'bool' / 'int' / 'float' / 'string'.
  1496| 
  1497|     Matches this codebase's own CSV-writing conventions: compare_cross_segment.py's
  1498|     _bool_str() emits exactly "true"/"false" (see its own definition) for boolean
  1499|     fields, never "True"/"1"/"yes" -- so bool detection is intentionally narrow
  1500|     (case-insensitive true/false only) rather than guessing at every truthy-looking
  1501|     token.
  1502|     """
  1503|     lowered = value.strip().lower()
  1504|     if lowered in ("true", "false"):
  1505|         return "bool"
  1506|     try:
  1507|         int(value)
  1508|         return "int"
  1509|     except ValueError:
  1510|         pass
  1511|     try:
  1512|         float(value)
  1513|         return "float"
  1514|     except ValueError:
  1515|         pass
  1516|     return "string"
  1517| 
  1518| 
  1519| def _column_dtype(seen: set) -> str:
  1520|     """Combine the set of per-cell classifications observed for one column
  1521|     (plus "empty" for blank cells) into a single inferred dtype. Pure
  1522|     function over a set of labels -- no field name or domain knowledge.
  1523|     """
  1524|     non_empty = seen - {"empty"}
  1525|     if not non_empty:
  1526|         return "empty"
  1527|     if non_empty == {"bool"}:
  1528|         return "boolean"
  1529|     if "string" in non_empty:
  1530|         return "string"
  1531|     if "float" in non_empty:
  1532|         return "float"
  1533|     if non_empty == {"int"}:
  1534|         return "integer"
  1535|     return "string"
  1536| 
  1537| 
  1538| def _scan_csv_file(path: Path) -> dict:
  1539|     """Single-pass header + dtype-inference + row-count scan of one CSV.
  1540| 
  1541|     Reads with utf-8-sig (matches read_csv() elsewhere in this codebase) and a
  1542|     plain comma delimiter -- every file compare_cross_segment.py writes via
  1543|     atomic_write_csv() is comma-delimited, so no delimiter sniffing is needed
  1544|     here (unlike a general-purpose inventory tool over an arbitrary pipeline
  1545|     output folder). Never stores a row or a cell value beyond the single pass
  1546|     used to update each column's running dtype-classification set --
  1547|     "type of data, not shape of values": no sample rows are retained or
  1548|     returned.
  1549|     """
  1550|     try:
  1551|         with path.open("r", encoding="utf-8-sig", newline="") as f:
  1552|             reader = csv.reader(f)
  1553|             try:
  1554|                 header = next(reader)
  1555|             except StopIteration:
  1556|                 return {"columns": [], "row_count": 0, "empty_file": True, "parse_error": None}
  1557|             seen_by_col = [set() for _ in header]
  1558|             row_count = 0
  1559|             for row in reader:
  1560|                 row_count += 1
  1561|                 for i in range(len(header)):
  1562|                     cell = row[i] if i < len(row) else ""
  1563|                     seen_by_col[i].add("empty" if cell.strip() == "" else _classify_scalar(cell))
  1564|             columns = [
  1565|                 {"name": name, "inferred_dtype": _column_dtype(seen_by_col[i])}
  1566|                 for i, name in enumerate(header)
  1567|             ]
  1568|             return {"columns": columns, "row_count": row_count, "empty_file": False, "parse_error": None}
  1569|     except Exception as e:  # noqa: BLE001 -- reported per-file, scan continues for the rest
  1570|         return {"columns": [], "row_count": 0, "empty_file": False, "parse_error": f"{type(e).__name__}: {e}"}
  1571| 
  1572| 
  1573| def inventory_export_directory_files(scan_dirs: list, known_paths: set) -> list:
  1574|     """Live directory scan: every *.csv file actually present under scan_dirs
  1575|     that is NOT already one of known_paths (every path this generator already
  1576|     reads as an input, writes as an output, or tracks as a sibling artifact --
  1577|     see build_evidence_map()). Pure filesystem read -- no interpretation of
  1578|     file content beyond the structural facts _scan_csv_file() returns.
  1579| 
  1580|     This is deliberately live/computed, not a hand-maintained filename list:
  1581|     a future compare_cross_segment.py export nobody has wired an artifact_id
  1582|     for yet is picked up automatically the next time this runs, with no code
  1583|     change required here.
  1584|     """
  1585|     known_resolved = {p.resolve() for p in known_paths if p}
  1586|     seen_resolved = set()
  1587|     entries = []
  1588|     for scan_dir in scan_dirs:
  1589|         if not scan_dir or not scan_dir.is_dir():
  1590|             continue
  1591|         for path in sorted(scan_dir.glob("*.csv")):
  1592|             resolved = path.resolve()
  1593|             if resolved in known_resolved or resolved in seen_resolved:
  1594|                 continue
  1595|             seen_resolved.add(resolved)
  1596|             scan = _scan_csv_file(path)
  1597|             entries.append({
  1598|                 "filename": path.name,
  1599|                 "path": str(path),
  1600|                 **scan,
  1601|             })
  1602|     return entries
  1603| 
  1604| 
  1605| def build_file_inventory_document(
  1606|     *,
  1607|     schema_version: str,
  1608|     scanned_directories: list,
  1609|     files: list,
  1610| ) -> dict:
  1611|     """Pure envelope wrapper (matches build_findings_document()'s convention):
  1612|     files is already fully built (scan + narrative attached) by the caller;
  1613|     this function performs no filesystem I/O and no further computation.
  1614|     """
  1615|     return {
  1616|         "schema_version": schema_version,
  1617|         "generated_at": _utc_now_iso(),
  1618|         "scanned_directories": [str(d) for d in scanned_directories],
  1619|         "file_count": len(files),
  1620|         "files": files,
  1621|     }
```
