# Extract Orchestrator Stage Matrix

This matrix defines the explicit state-machine semantics used by `tools/run_extract_all.py`.

## Stage execution matrix

| Stage | T-label | Purpose | Default in `--stages` | Requires policy-applied join keys by default? | Notes |
|---|---|---|---|---|---|
| `flatten` | T0 | Emit flatten outputs (`Results_v21/phase0_v21`) with identity-mode join fields (`join_key_schema=sig_hash_as_join_key.v1`). | ✅ Yes | No | v2.1 flatten is the default path. |
| `discover` | T1 | Discover per-domain join policy candidates from flatten identity items. | ✅ Yes | No | Writes policy JSON (default: `Results_v21/policies/domain_join_key_policies.v21.json`). |
| `apply` | T2 | Apply policy and overwrite flatten `phase0_records.csv` join fields. | ❌ No | N/A | Explicit opt-in for operational commit path. |
| `split` | — | Split detection analysis over selected domains. | ❌ No | ✅ Yes | Fails if join identity mode is detected, unless explicit override is used. |
| `analyze1` | Legacy Phase1 | Analysis stage (authority-related legacy path and/or v2.1 analysis output). | ❌ No | ✅ Yes | Gate can be overridden only by explicit degraded-mode flag. |
| `analyze2` | Legacy Phase2 | Per-domain analysis packet stage and/or v2.1 analysis output. | ❌ No | ✅ Yes | Gate can be overridden only by explicit degraded-mode flag. |

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
| Join-dependent analysis (safe default) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages flatten,discover,apply,split,analyze1,analyze2` |
| Degraded exploratory analysis (explicitly unsafe for governance) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages flatten,discover,split --allow-sig-hash-join-key` |
| Noisy discover domains (e.g., fill_patterns) | `python tools/run_extract_all.py <exports_dir> --out-root <out_root> --stages discover --discover-sample-size 100 --discover-max-candidate-fields 20` |

## Backward-compatible alias matrix

| Preferred flag | Deprecated alias | Current behavior |
|---|---|---|
| `--allow-sig-hash-join-key` | `--allow-bootstrap` | Alias accepted; warning printed. |
| `--require-join-policy` | `--strict-join-policy` | Alias accepted; warning printed. |
| Stage defaults (`flatten,discover`) | `--emit-v21`, `--emit-phase0-v21` | Accepted as no-op aliases; warning printed. |
| `--stages flatten` | `--phase0-only` | Alias accepted; warning printed. |
| `--stages analyze1` | `--phase1-only` | Alias accepted; warning printed. |
| `--stages analyze2` | `--phase2-only` | Alias accepted; warning printed. |
| `--stages split` | `--split-only` | Alias accepted; warning printed. |
