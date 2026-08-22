# Routing index: Revit_Fingerprint

- Generated (UTC): 2026-08-22T11:28:23Z
- Tool version: 0.1.0
- Repository revision: `227dcc5711e377a55a5352503e133364a29ef36e` (dirty worktree)
- Source manifest hash (sha256 over every included file's `path:sha256`): `ecad660d830724b8331a2d91393c183856b662723165ec9e5380edec4ab3d5f1`
- Freshness rule: if this hash no longer matches a fresh `scan`, re-run `scan` before trusting any catalog below or building a `packet_request.json` from it.
- Graphify: graphify-out/graph.json was built at commit 28ef09a287a6, which does not match the current scan's HEAD commit 227dcc5711e3; Graphify-derived routing/expansion evidence omitted by default (revision alignment could not be proven).

## How to use this index

**This index has counts, not names -- it is not enough on its own to pick good selectors.** Do not draft `packet_request.json` from this file alone. Follow these steps in order:

1. Skim the catalog summaries below and pick the one(s) most likely to cover your question.
2. **Ask for that catalog file (`routing/<name>.md`) and read it before writing a request.** It has the actual file paths, symbol names, and line numbers this index does not. If you have not been given a catalog yet, your next reply should ask for it -- not a `packet_request.json`.
3. From the catalog, prefer `selectors.files` and/or `selectors.symbols` (an exact symbol name, optionally narrowed with `file`) that name the actual code involved. Use `selectors.search_terms` only for a short, distinctive phrase or identifier you expect to appear in very few places -- a single common word (e.g. "blocked", "Model", "error") can match hundreds of files across the whole repository and will get crowded out of the packet by `limits.max_files` before it reaches anything relevant. Treat search terms as a supplement to file/symbol selectors, never a substitute.
4. Write a `packet_request.json` (schema: `schema/packet_request.schema.json`) naming those selectors, e.g.:

```json
{
  "schema_version": "1.0",
  "question": "<your question>",
  "selectors": {"files": ["path/to/file.py"], "symbols": [{"name": "some_function"}], "search_terms": [], "lines": []},
  "expansion": {"include_callers": true, "include_callees": true, "include_imports": true, "include_related_tests": true, "max_hops": 1},
  "limits": {"max_estimated_tokens": 12000, "max_files": 12}
}
```
5. Run:
   `python repo_context.py packet ROOT --output OUT --request packet_request.json`
6. Read the generated packet in `packets/`. If any selector came back `missing`/`ambiguous` in its resolution report, or search terms were reported omitted under `limits.max_files`, revise the request (narrow the term, add a `file` qualifier, raise the limit) and re-run -- do not answer from a packet that reports selectors it couldn't resolve.

## Catalogs (65)

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
- Covers: 13 file(s), 109 top-level symbol(s)

### `routing/archived__page2.md`
- Covers: 11 file(s), 79 top-level symbol(s)

### `routing/archived__page3.md`
- Covers: 11 file(s), 72 top-level symbol(s)

### `routing/audit_results.md`
- Covers: 16 file(s), 0 top-level symbol(s)

### `routing/config.md`
- Covers: 1 file(s), 0 top-level symbol(s)

### `routing/contracts.md`
- Covers: 4 file(s), 0 top-level symbol(s)

### `routing/core.md`
- Covers: 12 file(s), 83 top-level symbol(s)

### `routing/core__page2.md`
- Covers: 10 file(s), 63 top-level symbol(s)

### `routing/core__page3.md`
- Covers: 3 file(s), 13 top-level symbol(s)

### `routing/dev_tools.md`
- Covers: 20 file(s), 133 top-level symbol(s)

### `routing/dev_tools__page2.md`
- Covers: 5 file(s), 25 top-level symbol(s)

### `routing/docs.md`
- Covers: 41 file(s), 0 top-level symbol(s)

### `routing/domains.md`
- Covers: 16 file(s), 86 top-level symbol(s)

### `routing/domains__page2.md`
- Covers: 15 file(s), 79 top-level symbol(s)

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

### `routing/runner.md`
- Covers: 6 file(s), 55 top-level symbol(s)

### `routing/scripts.md`
- Covers: 1 file(s), 2 top-level symbol(s)

### `routing/tests.md`
- Covers: 11 file(s), 211 top-level symbol(s)

### `routing/tests__page2.md`
- Covers: 6 file(s), 187 top-level symbol(s)

### `routing/tests__page3.md`
- Covers: 9 file(s), 103 top-level symbol(s)

### `routing/tests__page4.md`
- Covers: 9 file(s), 107 top-level symbol(s)

### `routing/tests__page5.md`
- Covers: 5 file(s), 119 top-level symbol(s)

### `routing/tests__page6.md`
- Covers: 5 file(s), 154 top-level symbol(s)

### `routing/tests__page7.md`
- Covers: 6 file(s), 74 top-level symbol(s)

### `routing/tests__page8.md`
- Covers: 8 file(s), 144 top-level symbol(s)

### `routing/tests__page9.md`
- Covers: 12 file(s), 117 top-level symbol(s)

### `routing/tests__page10.md`
- Covers: 15 file(s), 65 top-level symbol(s)

### `routing/tests__page11.md`
- Covers: 14 file(s), 102 top-level symbol(s)

### `routing/tests__page12.md`
- Covers: 7 file(s), 28 top-level symbol(s)

### `routing/tests_golden.md`
- Covers: 2 file(s), 0 top-level symbol(s)

### `routing/tests_probes.md`
- Covers: 1 file(s), 10 top-level symbol(s)

### `routing/tests_repo_context.md`
- Covers: 10 file(s), 127 top-level symbol(s)

### `routing/tests_repo_context__page2.md`
- Covers: 5 file(s), 83 top-level symbol(s)

### `routing/tests_revit.md`
- Covers: 4 file(s), 11 top-level symbol(s)

### `routing/tools.md`
- Covers: 15 file(s), 184 top-level symbol(s)

### `routing/tools__page2.md`
- Covers: 9 file(s), 195 top-level symbol(s)

### `routing/tools__page3.md`
- Covers: 14 file(s), 88 top-level symbol(s)

### `routing/tools__page4.md`
- Covers: 11 file(s), 92 top-level symbol(s)

### `routing/tools_archetype.md`
- Covers: 17 file(s), 140 top-level symbol(s)

### `routing/tools_archetype__page2.md`
- Covers: 2 file(s), 0 top-level symbol(s)

### `routing/tools_bundle_analysis.md`
- Covers: 11 file(s), 54 top-level symbol(s)

### `routing/tools_bundle_analysis__page2.md`
- Covers: 8 file(s), 25 top-level symbol(s)

### `routing/tools_compare_templates_stand-alone.md`
- Covers: 2 file(s), 24 top-level symbol(s)

### `routing/tools_governance.md`
- Covers: 1 file(s), 9 top-level symbol(s)

### `routing/tools_join_key_discovery.md`
- Covers: 3 file(s), 29 top-level symbol(s)

### `routing/tools_label_synthesis.md`
- Covers: 16 file(s), 104 top-level symbol(s)

### `routing/tools_label_synthesis__page2.md`
- Covers: 10 file(s), 37 top-level symbol(s)

### `routing/tools_lib.md`
- Covers: 4 file(s), 37 top-level symbol(s)

### `routing/tools_migration.md`
- Covers: 5 file(s), 22 top-level symbol(s)

### `routing/tools_patterns_analysis.md`
- Covers: 4 file(s), 31 top-level symbol(s)

### `routing/tools_probes.md`
- Covers: 12 file(s), 181 top-level symbol(s)

### `routing/tools_probes__page2.md`
- Covers: 9 file(s), 199 top-level symbol(s)

### `routing/tools_probes__page3.md`
- Covers: 9 file(s), 214 top-level symbol(s)

### `routing/tools_probes__page4.md`
- Covers: 3 file(s), 39 top-level symbol(s)

### `routing/validators.md`
- Covers: 1 file(s), 8 top-level symbol(s)
