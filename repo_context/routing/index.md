# Routing index: Revit_Fingerprint

- Generated (UTC): 2026-08-22T04:24:28Z
- Tool version: 0.1.0
- Repository revision: `08f43f90f1a90e611c17dbcc16f95b057296c771` (dirty worktree)
- Source manifest hash (sha256 over every included file's `path:sha256`): `e4f6caadabc41c84def3d2c0ec6ffc14ae7acbddba20a1f09468de4d5a2df8df`
- Freshness rule: if this hash no longer matches a fresh `scan`, re-run `scan` before trusting any catalog below or building a `packet_request.json` from it.
- Graphify: graphify-out/graph.json was built at commit 28ef09a287a6, which does not match the current scan's HEAD commit 08f43f90f1a9; Graphify-derived routing/expansion evidence omitted by default (revision alignment could not be proven).

## How to use this index

1. Skim the catalog summaries below and pick the one(s) most likely to cover your question.
2. Open that catalog (`routing/<name>.md`) and identify candidate files/symbols/search terms.
3. Write a `packet_request.json` (schema: `schema/packet_request.schema.json`) naming those selectors, e.g.:

```json
{
  "schema_version": "1.0",
  "question": "<your question>",
  "selectors": {"files": ["path/to/file.py"], "symbols": [{"name": "some_function"}], "search_terms": [], "lines": []},
  "expansion": {"include_callers": true, "include_callees": true, "include_imports": true, "include_related_tests": true, "max_hops": 1},
  "limits": {"max_estimated_tokens": 12000, "max_files": 12}
}
```
4. Run:
   `python repo_context.py packet ROOT --output OUT --request packet_request.json`
5. Read the generated packet in `packets/`; it contains exact source excerpts, a selector-resolution report, and an explicit list of anything omitted or ambiguous.

## Catalogs (38)

### `routing/.agents.md`
- Covers: 10 file(s), 0 top-level symbol(s)

### `routing/.claude.md`
- Covers: 2 file(s), 0 top-level symbol(s)

### `routing/.codex.md`
- Covers: 4 file(s), 0 top-level symbol(s)

### `routing/.copilot.md`
- Covers: 10 file(s), 0 top-level symbol(s)

### `routing/.github.md`
- Covers: 6 file(s), 0 top-level symbol(s)

### `routing/archived.md`
- Covers: 35 file(s), 260 top-level symbol(s)
- Note: 22 file(s) omitted from this catalog by its own size limit

### `routing/audit_results.md`
- Covers: 16 file(s), 0 top-level symbol(s)

### `routing/config.md`
- Covers: 1 file(s), 0 top-level symbol(s)

### `routing/contracts.md`
- Covers: 4 file(s), 0 top-level symbol(s)

### `routing/core.md`
- Covers: 25 file(s), 159 top-level symbol(s)
- Note: 13 file(s) omitted from this catalog by its own size limit

### `routing/dev_tools.md`
- Covers: 25 file(s), 143 top-level symbol(s)
- Note: 7 file(s) omitted from this catalog by its own size limit

### `routing/docs.md`
- Covers: 41 file(s), 0 top-level symbol(s)
- Note: 11 file(s) omitted from this catalog by its own size limit

### `routing/domains.md`
- Covers: 31 file(s), 165 top-level symbol(s)
- Note: 15 file(s) omitted from this catalog by its own size limit

### `routing/graphify-out.md`
- Covers: 5 file(s), 0 top-level symbol(s)

### `routing/mapping.md`
- Covers: 5 file(s), 41 top-level symbol(s)

### `routing/other.md`
- Covers: 14 file(s), 6 top-level symbol(s)

### `routing/policies.md`
- Covers: 13 file(s), 0 top-level symbol(s)

### `routing/reference_revit_lookup.md`
- Covers: 3 file(s), 0 top-level symbol(s)

### `routing/reference_revit_lookup_Descriptors.md`
- Covers: 117 file(s), 0 top-level symbol(s)
- Note: 89 file(s) omitted from this catalog by its own size limit

### `routing/runner.md`
- Covers: 6 file(s), 55 top-level symbol(s)

### `routing/scripts.md`
- Covers: 1 file(s), 2 top-level symbol(s)

### `routing/tests.md`
- Covers: 107 file(s), 1411 top-level symbol(s)
- Note: 96 file(s) omitted from this catalog by its own size limit

### `routing/tests_golden.md`
- Covers: 2 file(s), 0 top-level symbol(s)

### `routing/tests_probes.md`
- Covers: 1 file(s), 10 top-level symbol(s)

### `routing/tests_repo_context.md`
- Covers: 15 file(s), 146 top-level symbol(s)
- Note: 4 file(s) omitted from this catalog by its own size limit

### `routing/tests_revit.md`
- Covers: 4 file(s), 11 top-level symbol(s)

### `routing/tools.md`
- Covers: 49 file(s), 559 top-level symbol(s)
- Note: 36 file(s) omitted from this catalog by its own size limit

### `routing/tools_archetype.md`
- Covers: 19 file(s), 140 top-level symbol(s)
- Note: 4 file(s) omitted from this catalog by its own size limit

### `routing/tools_bundle_analysis.md`
- Covers: 19 file(s), 79 top-level symbol(s)
- Note: 7 file(s) omitted from this catalog by its own size limit

### `routing/tools_compare_templates_stand-alone.md`
- Covers: 2 file(s), 24 top-level symbol(s)

### `routing/tools_governance.md`
- Covers: 1 file(s), 9 top-level symbol(s)

### `routing/tools_join_key_discovery.md`
- Covers: 3 file(s), 29 top-level symbol(s)

### `routing/tools_label_synthesis.md`
- Covers: 26 file(s), 141 top-level symbol(s)
- Note: 11 file(s) omitted from this catalog by its own size limit

### `routing/tools_lib.md`
- Covers: 4 file(s), 37 top-level symbol(s)

### `routing/tools_migration.md`
- Covers: 5 file(s), 22 top-level symbol(s)

### `routing/tools_patterns_analysis.md`
- Covers: 4 file(s), 31 top-level symbol(s)

### `routing/tools_probes.md`
- Covers: 33 file(s), 633 top-level symbol(s)
- Note: 21 file(s) omitted from this catalog by its own size limit

### `routing/validators.md`
- Covers: 1 file(s), 8 top-level symbol(s)
