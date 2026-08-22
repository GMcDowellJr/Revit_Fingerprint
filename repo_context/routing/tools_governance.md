# Routing catalog: `tools/governance`

- Generated (UTC): 2026-08-22T06:08:45Z
- Tool version: 0.1.0
- Files covered: 1
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `8459e1d7a9f75084f0798295491ad400ef39c92f0b58dcbb582a5ce921092c8d`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tools/governance/standards_governance_report.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - module docstring: Standards Governance Report Generator
  - filename/path terms: standards governance report
- Important symbols (9 total):
  - `ProjectExport` (class) — line 26
  - `StandardsGovernanceAnalyzer` (class) — line 32
  - `_get_identity_value` (function) — line 235
  - `_build_html_report` (function) — line 243
  - `_row_template` (function) — line 416
  - `_pattern_row_template` (function) — line 427
  - `build_table` (function) — line 442
  - `build_pattern_table` (function) — line 456
  - `main` (function) — line 470
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `<module> (tools/governance/standards_governance_report.py:497)`
  - `StandardsGovernanceAnalyzer._get_canonical_baselines (tools/governance/standards_governance_report.py:212)`
  - `StandardsGovernanceAnalyzer.analyze_baseline_drift (tools/governance/standards_governance_report.py:68)`
  - `StandardsGovernanceAnalyzer.analyze_template_overrides (tools/governance/standards_governance_report.py:102)`
  - `StandardsGovernanceAnalyzer.generate_report (tools/governance/standards_governance_report.py:194)`
  - `StandardsGovernanceAnalyzer.generate_report (tools/governance/standards_governance_report.py:195)`
  - `StandardsGovernanceAnalyzer.load_exports (tools/governance/standards_governance_report.py:49)`
  - `_build_html_report (tools/governance/standards_governance_report.py:259)`
  - `_build_html_report (tools/governance/standards_governance_report.py:268)`
  - `_build_html_report (tools/governance/standards_governance_report.py:277)`
  - `_build_html_report (tools/governance/standards_governance_report.py:404)`
  - `_build_html_report (tools/governance/standards_governance_report.py:407)`
  - `_build_html_report (tools/governance/standards_governance_report.py:410)`
  - `main (tools/governance/standards_governance_report.py:478)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`a4a29f568b1a2aa3…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/governance/standards_governance_report.py`)

