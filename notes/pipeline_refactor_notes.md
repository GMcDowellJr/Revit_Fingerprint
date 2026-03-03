# Pipeline refactor inventory and mapping

## New tools layout

- tools/flatten/
- tools/policy/
- tools/analysis/
  - authority/
- tools/governance/
- tools/io/
- tools/legacy/

## Script inventory (A)

- flatten: `tools/flatten/emit.py`
- policy: `tools/policy/discover_join_policy.py`, `tools/policy/apply_join_policy.py`
- analysis authority/join-key discovery: `tools/analysis/authority/*.py`
- governance orchestration: `tools/governance/run_pipeline.py`, `tools/governance/validate_contract.py`
- probes/research: `research/probes/*.py`

## Move map (old -> new)

- `tools/v21_emit.py` -> `tools/flatten/emit.py`
- `tools/v21_discover_join_policy.py` -> `tools/policy/discover_join_policy.py`
- `tools/v21_apply_join_policy.py` -> `tools/policy/apply_join_policy.py`
- `tools/phase2_analysis/` -> `tools/analysis/authority/`
- `tools/run_extract_all.py` -> `tools/governance/run_pipeline.py`
- `tools/validate_v21_contract.py` -> `tools/governance/validate_contract.py`
- `tools/probes/` -> `research/probes/`

## Output path migration

Stable BI paths now targeted under `out/current/`:

- `out/current/flatten/file_metadata.csv`
- `out/current/analysis/domain_patterns.csv`
- `out/current/analysis/pattern_presence_file.csv`
- `out/current/analysis/export_membership.csv`
- `out/current/analysis/record_pattern_membership.csv`
- `out/current/analysis/authority_pattern.csv`
- `out/current/analysis/domain_pattern_diagnostics.csv`
- `out/current/analysis/manifest.csv`

Immutable run paths:

- `out/runs/<run_id>/flatten/...`
- `out/runs/<run_id>/analysis/...`
- `out/runs/<run_id>/manifest.json`

## How to run

Canonical entrypoint:

`python tools/governance/run_pipeline.py <exports_dir> [--out-root out] --stages flatten,discover,apply,analyze1,analyze2`

Verify outputs:

`python tools/analysis/verify_outputs.py --current-dir out/current --contract assets/contracts/analysis_contract.json`

Legacy wrappers are available in `tools/legacy/` and old top-level script names print deprecation warnings.
