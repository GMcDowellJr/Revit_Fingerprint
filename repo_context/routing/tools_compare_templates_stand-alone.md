# Routing catalog: `tools/compare_templates_stand-alone`

- Generated (UTC): 2026-08-22T10:53:20Z
- Tool version: 0.1.0
- Files covered: 2
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `8f0edbcd1530934fbe09b667ad93747ea5ae50be36ff5785ffa37546c59df6c8`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - module docstring: compare_view_templates.py
  - filename/path terms: compare view templates stand alone
- Important symbols (24 total):
  - `_extract_records` (function) — line 52
  - `_get_label_display` (function) — line 71
  - `_get_label_component` (function) — line 78
  - `_get_view_type_family` (function) — line 85
  - `_get_template_uid` (function) — line 97
  - `_get_vco_template_uid` (function) — line 105
  - `_get_vco_category_path` (function) — line 113
  - `_parse_vt_signature` (function) — line 121
  - `_parse_vco_items` (function) — line 133
  - `_index_vco_by_template` (function) — line 155
  - `_jaccard` (function) — line 169
  - `_best_match_index` (function) — line 178
  - `_make_pair` (function) — line 212
  - `match_templates` (function) — line 233
  - `_diff_dicts` (function) — line 316
  - `_diff_vco` (function) — line 337
  - `_print_pair` (function) — line 388
  - `_print_report` (function) — line 439
  - `_esc` (function) — line 554
  - `_vt_diff_rows` (function) — line 558
  - `_vco_rows` (function) — line 588
  - `_pair_html` (function) — line 628
  - `_build_html` (function) — line 676
  - `main` (function) — line 715
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `<module> (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:777)`
  - `_best_match_index (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:186)`
  - `_best_match_index (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:190)`
  - `_best_match_index (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:191)`
  - `_best_match_index (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:199)`
  - `_best_match_index (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:200)`
  - `_build_html (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:682)`
  - `_build_html (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:686)`
  - `_build_html (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:690)`
  - `_build_html (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:696)`
  - `_build_html (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:701)`
  - `_diff_vco (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:344)`
  - `_diff_vco (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:345)`
  - `_diff_vco (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:360)`
  - `_diff_vco (tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py:361)`
  - ... and 45 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`5d941df515762a01…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py`)

## Other files (non-Python)

| Path | Title/summary | Role |
|---|---|---|
| `tools/compare_templates_stand-alone/compare_view_templates_stand-alone_mapping.json` | compare view templates stand alone mapping | `unknown` |

