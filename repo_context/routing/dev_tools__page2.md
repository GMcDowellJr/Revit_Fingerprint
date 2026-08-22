# Routing catalog: `dev_tools (page 2)`

- Generated (UTC): 2026-08-22T17:32:12Z
- Tool version: 0.1.0
- Files covered (this page): 5
- Catalog source hash (sha256 of sorted `path:sha256` pairs for the full `dev_tools` partition): `9aa3e3deeb23069b772796499560ce1afc45675df5c88f61bbfc31e1ba96197a`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `dev_tools/repo_context/rc_validate.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: `validate` command: sanity-check a previously generated output directory.
  - filename/path terms: rc validate
- Important symbols (6 total):
  - `_looks_absolute_or_backslashed` (function) — line 54
  - `ValidationResult` (class) — line 60
  - `_read_csv_rows` (function) — line 76
  - `validate_output_dir` (function) — line 83
  - `_symbol_name_matches` (function) — line 266
  - `format_report` (function) — line 270
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `validate_output_dir (dev_tools/repo_context/rc_validate.py:112)`
  - `validate_output_dir (dev_tools/repo_context/rc_validate.py:236)`
  - `validate_output_dir (dev_tools/repo_context/rc_validate.py:258)`
  - `validate_output_dir (dev_tools/repo_context/rc_validate.py:84)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`98473370de559b4f…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_validate.py`)

### `dev_tools/repo_context/rc_writers.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: CSV / JSONL table writers, sharing schemas with rc_validate.py.
  - filename/path terms: rc writers
- Important symbols (10 total):
  - `_bool_str` (function) — line 12
  - `_rows_to_csv_text` (function) — line 16
  - `file_record_to_row` (function) — line 25
  - `file_record_to_dict` (function) — line 34
  - `symbol_record_to_row` (function) — line 46
  - `symbol_record_to_dict` (function) — line 55
  - `import_record_to_row` (function) — line 67
  - `call_record_to_row` (function) — line 74
  - `chunk_record_to_row` (function) — line 81
  - `write_all_tables` (function) — line 89
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `file_record_to_row (dev_tools/repo_context/rc_writers.py:29)`
  - `symbol_record_to_row (dev_tools/repo_context/rc_writers.py:50)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:100)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:104)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:108)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:112)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:116)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:120)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:124)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:128)`
  - `write_all_tables (dev_tools/repo_context/rc_writers.py:132)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`7f9cbdcbfe503899…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_writers.py`)

### `dev_tools/repo_context/repo_context.py`
- Role: `operator_entrypoint` (evidence: contains `if __name__ == "__main__":` guard; located under operator-facing directory 'dev_tools/')
- Purpose clues:
  - module docstring: repo_context.py — local, read-only repository-context generator.
  - filename/path terms: repo context
- Important symbols (9 total):
  - `_positive_int` (function) — line 38
  - `_non_negative_int` (function) — line 45
  - `_resolve_output_dir` (function) — line 52
  - `cmd_scan` (function) — line 78
  - `cmd_packet` (function) — line 151
  - `cmd_discover` (function) — line 196
  - `cmd_validate` (function) — line 219
  - `build_parser` (function) — line 226
  - `main` (function) — line 313
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `<module> (dev_tools/repo_context/repo_context.py:320)`
  - `cmd_scan (dev_tools/repo_context/repo_context.py:84)`
  - `main (dev_tools/repo_context/repo_context.py:314)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`06e230d48ec74f81…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/repo_context.py`)

## Other files (non-Python / boilerplate)

| Path | Title/summary | Role |
|---|---|---|
| `dev_tools/repo_context/README.md` | repo_context | `unknown` |
| `dev_tools/repo_context/schema/packet_request.schema.json` | packet request.schema | `unknown` |

