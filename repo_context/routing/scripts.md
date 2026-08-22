# Routing catalog: `scripts`

- Generated (UTC): 2026-08-22T06:21:11Z
- Tool version: 0.1.0
- Files covered: 1
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `1044d477605091c8509afb1daf308f801b71c6c8e1c4d8429e9375252617d32d`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `scripts/check_audit_references.py`
- Role: `operator_entrypoint` (evidence: contains `if __name__ == "__main__":` guard; located under operator-facing directory 'scripts/')
- Purpose clues:
  - module docstring: Verify audit_results references in tracked text resolve deterministically.
  - filename/path terms: check audit references
- Important symbols (2 total):
  - `tracked_files` (function) — line 14
  - `main` (function) — line 21
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `<module> (scripts/check_audit_references.py:48)`
  - `main (scripts/check_audit_references.py:24)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`0f99f256a5c65348…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `scripts/check_audit_references.py`)

