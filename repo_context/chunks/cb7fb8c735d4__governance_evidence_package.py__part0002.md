# Chunk of tools/governance_evidence_package.py

- Source relative path: `tools/governance_evidence_package.py`
- Chunk: 2 of 6
- Original line range: 516-538
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _sibling_scan_fields
- Source SHA-256: 2fece0426163550ef83e302b52b9f002b12123e12eb35430df07c3d1f4c4b1f3
- Starts inside symbol: no
- Ends inside symbol: no

```
   516| def _sibling_scan_fields(path, present: bool) -> dict:
   517|     """Reuse _scan_csv_file() -- the same D-023 live scan governance_file_
   518|     inventory.json already performs for undiscovered files -- to populate an
   519|     excluded sibling artifact's own governance_evidence_map.json entry with
   520|     its column header (name + inferred dtype) and row count. A reader who
   521|     never opens governance_file_inventory.json still gets this for the
   522|     specific large files docs/governance/governance_interpretation_guide.md's
   523|     escalation section names by filename (D-024). Returns {} when the file
   524|     is not present -- scanning a path that does not exist is meaningless,
   525|     not an error to report. Never returns a sample row or cell value, same
   526|     scope decision as the D-023 scan itself.
   527|     """
   528|     if not present or not path:
   529|         return {}
   530|     scan = _scan_csv_file(Path(path))
   531|     fields = {"row_count": scan["row_count"]}
   532|     if scan.get("parse_error"):
   533|         fields["parse_error"] = scan["parse_error"]
   534|     else:
   535|         fields["columns"] = scan["columns"]
   536|     return fields
   537| 
   538| 
```
