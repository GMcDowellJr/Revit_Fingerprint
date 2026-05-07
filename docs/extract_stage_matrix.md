# Extract Orchestrator Stage Matrix

This matrix defines the explicit state-machine semantics used by `tools/run_extract_all.py`.

## Stage execution matrix

| Stage | T-label | Purpose | Default in `--stages` | Requires policy-applied join keys by default? | Notes |
|---|---|---|---|---|---|
| `flatten` | T0 | Emit flatten outputs (`Results_v21/phase0_v21`) with identity-mode join fields (`join_key_schema=sig_hash_as_join_key.v1`). | ✅ Yes | No | v2.1 flatten is the default path. |
| `discover` | T1 | Explore per-domain join policy candidates from flatten identity items. | ✅ Yes | No | Writes PowerBI-ready diagnostics CSVs for `discover`/`validate`/`harsh`. |
| `apply` | T2 | Apply policy and overwrite flatten `phase0_records.csv` join fields. | ❌ No | N/A | Explicit opt-in for operational commit path. Defaults to `results/policies/domain_join_key_policies.v21.json` when `--join-policy` is not provided. |
| `placeholders` | T2b | Generate per-domain placeholder exclusion CSVs from purgeable heuristics, instance count, sole-type flags, and known-defaults reference JSON. Human reviews output before analyze stages. | ❌ No | ✅ Yes | Writes to `Results_v21/placeholder_exclusions/`. Requires `apply` stage. Does not block downstream stages on failure. |
| `split` | — | Split detection analysis over selected domains. | ❌ No | ✅ Yes | Fails if join identity mode is detected, unless explicit override is used. |
| `authority` | — | v2.1 analysis output (authority-related). | ❌ No | ✅ Yes | Gate can be overridden only by explicit degraded-mode flag. |
| `patterns` | — | v2.1 analysis output (per-domain patterns). | ❌ No | ✅ Yes | Gate can be overridden only by explicit degraded-mode flag. |
| `flat_tables` | — | Write flat CSV tables (layer stacks etc.) via `export_to_flat_tables.py`. | ❌ No | No | |

> Temporary object-styles stopgap (March 2026): flatten currently drops object-style rows that match imported CAD noise markers (`"Imports in Families"` or `".dwg"` in object-style key/name text), and downstream stages suppress `object_styles_imported`. This is intentionally reversible in pipeline code and should be moved into exporter-side domain assignment logic in a future exporter update.

## Join-policy gate matrix

| Condition | Default behavior | Explicit override |
|---|---|---|
| `join_key_schema == sig_hash_as_join_key.v1` | ❌ Gate failure for join-dependent stages | `--allow-sig-hash-join-key` |
| `join_key_status != ok` | ❌ Gate failure for join-dependent stages | `--allow-sig-hash-join-key` |

> `sig_hash_as_join_key` is identity-mode clustering and is **DEGRADED** for governance conclusions.

## Canonical command matrix

| Goal | Command |
|---|---|
| Draft prep (default) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root>` |
| Explicit default | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages flatten,discover` |
| Operational commit (policy applied) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages flatten,discover,apply` |
| Join-dependent analysis (safe default) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages flatten,discover,apply,placeholders,[human review],split,authority,patterns` |
| Degraded exploratory analysis (explicitly unsafe for governance) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages flatten,discover,split --allow-sig-hash-join-key` |

## Join-policy compatibility notes

- v2.1 apply accepts both `required_fields` (native) and legacy `required_items` aliases when computing required keys.
- `optional_items` are preserved for compatibility/forensics and are **not** equivalent to required join keys (`selected_fields`/effective required set).
- v2.1 apply supports shape-gated requirements through both `gates` and legacy `shape_gating` blocks (`discriminator_key` + per-shape `additional_required`).
- Discover remains schema-compatible, can preserve existing gate blocks with `--base-policy`, and emits compatibility mirrors (`required_items`, `optional_items`, `explicitly_excluded_items`, and `shape_gating` when gates exist).
