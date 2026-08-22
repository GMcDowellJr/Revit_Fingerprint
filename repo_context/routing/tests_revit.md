# Routing catalog: `tests/revit`

- Generated (UTC): 2026-08-22T11:28:23Z
- Tool version: 0.1.0
- Files covered (this page): 4
- Catalog source hash (sha256 of sorted `path:sha256` pairs for the full `tests/revit` partition): `e97d1be96513e9cfafd064cce5b118e1af4244bebf62d142f3685c397cbb5760`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tests/revit/_json_diff.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - module docstring: Deterministic JSON comparison helpers (pure CPython).
  - filename/path terms: json diff
- Important symbols (6 total):
  - `_canon_obj` (function) — line 19
  - `canonical_json_bytes` (function) — line 29
  - `sha256_of_json` (function) — line 36
  - `pretty_json` (function) — line 40
  - `diff_paths` (function) — line 45
  - `compare_json` (function) — line 138
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_canon_obj (tests/revit/_json_diff.py:23)`
  - `_canon_obj (tests/revit/_json_diff.py:25)`
  - `_write_json (tests/revit/revit_test_runner_pyrevit.py:53)`
  - `canonical_json_bytes (tests/revit/_json_diff.py:31)`
  - `compare_json (tests/revit/_json_diff.py:143)`
  - `compare_json (tests/revit/_json_diff.py:144)`
  - `compare_json (tests/revit/_json_diff.py:148)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:162)`
  - `pretty_json (tests/revit/_json_diff.py:41)`
  - `sha256_of_json (tests/revit/_json_diff.py:37)`
- Related tests:
  - `tests/revit/_json_diff.py`
  - `tests/revit/revit_test_runner_pyrevit.py`
- Retrieval identity: sha256=`f45080505381f92e…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/revit/_json_diff.py`)

### `tests/revit/revit_test_runner_pyrevit.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - module docstring: Revit-executed integration test runner (pyRevit-friendly).
  - filename/path terms: revit test runner pyrevit
- Important symbols (5 total):
  - `_load_json` (function) — line 39
  - `_write_text` (function) — line 44
  - `_write_json` (function) — line 52
  - `_now_stamp` (function) — line 56
  - `main` (function) — line 60
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `core/manifest.py`
  - imports `runner/run_dynamo.py`
  - imports `tests/revit/_json_diff.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tests/revit/revit_test_runner_pyrevit.py:239)`
  - `_write_json (tests/revit/revit_test_runner_pyrevit.py:53)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:121)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:129)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:139)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:143)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:150)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:161)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:170)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:187)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:230)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:67)`
  - `main (tests/revit/revit_test_runner_pyrevit.py:91)`
- Related tests:
  - `tests/revit/revit_test_runner_pyrevit.py`
- Retrieval identity: sha256=`982119a851b84e0d…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/revit/revit_test_runner_pyrevit.py`)

## Other files (non-Python / boilerplate)

| Path | Title/summary | Role |
|---|---|---|
| `tests/revit/config.example.json` | config.example | `unknown` |
| `tests/revit/README.md` | Revit integration tests (golden baselines) | `unknown` |

