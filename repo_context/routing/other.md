# Routing catalog: `other`

- Generated (UTC): 2026-08-22T09:56:48Z
- Tool version: 0.1.0
- Files covered: 14
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `a154ae69a12aefd39d04c0f7dc04d6c2176f8039e3724136b96dd11168041e02`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `sync_revitlookup_reference.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - module docstring: sync_revitlookup_reference.py
  - filename/path terms: sync revitlookup reference
- Important symbols (6 total):
  - `github_get` (function) — line 106
  - `fetch_raw` (function) — line 123
  - `get_current_commit_sha` (function) — line 137
  - `list_all_cs_files` (function) — line 142
  - `sync` (function) — line 153
  - `main` (function) — line 264
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `<module> (sync_revitlookup_reference.py:302)`
  - `get_current_commit_sha (sync_revitlookup_reference.py:138)`
  - `list_all_cs_files (sync_revitlookup_reference.py:143)`
  - `main (sync_revitlookup_reference.py:293)`
  - `sync (sync_revitlookup_reference.py:162)`
  - `sync (sync_revitlookup_reference.py:179)`
  - `sync (sync_revitlookup_reference.py:210)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`aec273660507e9e2…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `sync_revitlookup_reference.py`)

## Other files (non-Python)

| Path | Title/summary | Role |
|---|---|---|
| `.gitignore` | .gitignore | `unknown` |
| `.graphifyignore` | .graphifyignore | `unknown` |
| `AGENTS.md` | graphify | `unknown` |
| `ARCHITECTURE.md` | Architecture Overview | `unknown` |
| `CHANGELOG.md` | CHANGELOG | `unknown` |
| `CLAUDE.md` | CLAUDE.md | `unknown` |
| `DECISIONS.md` | DECISIONS | `unknown` |
| `INVARIANTS.md` | Fingerprinting Invariants | `unknown` |
| `README.md` | Revit Fingerprint — MVP → Baseline | `unknown` |
| `REFACTOR.md` | REFACTOR.md | `unknown` |
| `REPO_OPERATIONAL_REVIEW.md` | Repository Operational Review | `unknown` |
| `Revit fingerprint MVP.dyn` | Revit fingerprint MVP | `unknown` |
| `REVIT_LOOKUP_DOMAIN_MAP.md` | RevitLookup Descriptor → Fingerprint Domain Map | `unknown` |

