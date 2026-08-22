# Routing catalog: `tools/bundle_analysis (page 2)`

- Generated (UTC): 2026-08-22T17:32:12Z
- Tool version: 0.1.0
- Files covered (this page): 8
- Catalog source hash (sha256 of sorted `path:sha256` pairs for the full `tools/bundle_analysis` partition): `f736b277d9e1f6fd82b4fe97f9be754014de1e4e507b028c1e996c87ecd81442`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tools/bundle_analysis/step2b_bundle_share_profile.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step2b bundle share profile
- Important symbols (5 total):
  - `_is_true` (function) — line 19
  - `_fmt_float` (function) — line 23
  - `build_bundle_share_profile` (function) — line 27
  - `_parse_args` (function) — line 233
  - `main` (function) — line 243
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step2b_bundle_share_profile.py:256)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:227)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:310)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:142)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:164)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:165)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:167)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:194)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:195)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:197)`
  - `build_bundle_share_profile (tools/bundle_analysis/step2b_bundle_share_profile.py:198)`
  - `main (tools/bundle_analysis/step2b_bundle_share_profile.py:244)`
  - `main (tools/bundle_analysis/step2b_bundle_share_profile.py:245)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`aa081c73c59e8f3b…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step2b_bundle_share_profile.py`)

### `tools/bundle_analysis/step3_build_dag.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step3 build dag
- Important symbols (3 total):
  - `build_dag_for_domain` (function) — line 18
  - `_parse_args` (function) — line 228
  - `main` (function) — line 235
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step3_build_dag.py:242)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:238)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:320)`
  - `main (tools/bundle_analysis/step3_build_dag.py:236)`
  - `main (tools/bundle_analysis/step3_build_dag.py:237)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`b0e54a7ae5de0558…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step3_build_dag.py`)

### `tools/bundle_analysis/step4_difference_sets.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step4 difference sets
- Important symbols (3 total):
  - `emit_stub` (function) — line 17
  - `_parse_args` (function) — line 140
  - `main` (function) — line 147
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step4_difference_sets.py:154)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:244)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:326)`
  - `main (tools/bundle_analysis/step4_difference_sets.py:148)`
  - `main (tools/bundle_analysis/step4_difference_sets.py:149)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`27b0c89b2cf5e5d1…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step4_difference_sets.py`)

### `tools/bundle_analysis/step5_classify_patterns.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step5 classify patterns
- Important symbols (3 total):
  - `emit_stub` (function) — line 17
  - `_parse_args` (function) — line 223
  - `main` (function) — line 230
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step5_classify_patterns.py:237)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:249)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:331)`
  - `main (tools/bundle_analysis/step5_classify_patterns.py:231)`
  - `main (tools/bundle_analysis/step5_classify_patterns.py:232)`
  - `test_emit_stub_classifies_root_to_leaf_patterns_as_differentiating (tests/test_bundle_pattern_classification_roles.py:68)`
- Related tests:
  - `tests/test_bundle_pattern_classification_roles.py`
- Retrieval identity: sha256=`f139c30cf0faec61…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step5_classify_patterns.py`)

### `tools/bundle_analysis/step6_classify_files.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step6 classify files
- Important symbols (3 total):
  - `emit_stub` (function) — line 17
  - `_parse_args` (function) — line 216
  - `main` (function) — line 223
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step6_classify_files.py:230)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:254)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:336)`
  - `main (tools/bundle_analysis/step6_classify_files.py:224)`
  - `main (tools/bundle_analysis/step6_classify_files.py:225)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`0c30628e71dd3c2a…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step6_classify_files.py`)

### `tools/bundle_analysis/step7_overlap_report.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step7 overlap report
- Important symbols (3 total):
  - `emit_stub` (function) — line 17
  - `_parse_args` (function) — line 52
  - `main` (function) — line 59
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step7_overlap_report.py:66)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:260)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:342)`
  - `main (tools/bundle_analysis/step7_overlap_report.py:60)`
  - `main (tools/bundle_analysis/step7_overlap_report.py:61)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`0bacfe00cd8ad98b…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step7_overlap_report.py`)

### `tools/bundle_analysis/step_compare.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: step compare
- Important symbols (2 total):
  - `_compute_gap_rows` (function) — line 30
  - `run_compare_for_domain` (function) — line 133
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `run_bundle_analysis (tools/bundle_analysis/run_bundle_analysis.py:806)`
  - `run_compare_for_domain (tools/bundle_analysis/step_compare.py:146)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`83b12c25156fdbf4…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step_compare.py`)

### `tools/bundle_analysis/utils.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: utils
- Important symbols (3 total):
  - `_supporting_files_by_superset` (function) — line 8
  - `find_closed_itemsets` (function) — line 15
  - `find_root_bundles` (function) — line 83
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:209)`
  - `find_bundles_for_domain (tools/bundle_analysis/step2_find_bundles.py:229)`
  - `find_closed_itemsets (tools/bundle_analysis/utils.py:44)`
  - `find_root_bundles (tools/bundle_analysis/utils.py:97)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`2fe8b72ed4ecbdab…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/utils.py`)

