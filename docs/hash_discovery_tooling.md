# Hash Discovery Tooling (`tools/discover_hash_policy.py`)

## Data source
`discover_hash_policy.py` reads **flattened CSVs** produced by extraction/flatten stages (for example via `tools/run_extract_all.py`).

It does **not** read the original fingerprint/export JSON files directly.

Expected inputs under `--phase0-dir`:
- `records.csv` (or legacy `phase0_records.csv`)
- item tables used per target:
  - `sig`: `signature_items.csv` -> fallback `identity_items.csv` -> fallback `phase0_identity_items.csv`
  - `join`: `join_items.csv` -> fallback `identity_items.csv` -> fallback `phase0_identity_items.csv`

## `--policy-json` intent
`--policy-json` (or fallback `--base-policy`) is optional and provides baseline constraints:
- `required_fields` / `required_items`
- `optional_items`
- `explicitly_excluded_items`
- `gates`/`shape_gating`

This is most relevant to:
- `--policy-modes validate` (strictly constrained to required+optional), and
- `--policy-modes harsh` (required+optional plus discovered candidates).

If omitted, discovery runs unconstrained except for built-in behavior (such as `loaded_family_types` category gating).

## Option reference
- `--phase0-dir`: flattened input directory.
- `--policy-json`: primary baseline/constraint policy input.
- `--base-policy`: fallback baseline path when `--policy-json` is not supplied.
- `--out-policy`: optional candidate-only output JSON (`policy_version: candidate`, non-governed).
- `--domains`: optional comma-separated domain allow-list.
- `--discovery-target`: `join`, `sig`, or `both`.
- `--search-modes`: comma-separated `greedy`, `pareto`.
- `--policy-modes`:
  - `discover`: discovered candidates only (minus exclusions).
  - `validate`: required+optional only.
  - `harsh`: required+optional+discovered.
- `--sample-size`: per-domain sample cap (`0` => uncapped).
- `--sample-seed`: deterministic sampling seed.
- `--max-candidate-fields`: cap candidate-field pool size.
- `--max-k`: max subset size considered by search/evaluation.

## Gating behavior (`loaded_family_types`)
For `loaded_family_types`, discovery is partitioned by `shape_gate.category`; candidates are discovered per gate to prevent global cross-category key nomination.
