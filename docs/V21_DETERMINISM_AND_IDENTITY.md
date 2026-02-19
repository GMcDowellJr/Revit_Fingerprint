# V2.1 Determinism & Identity

## Determinism

- All v2.1 CSV outputs are explicitly sorted by stable key columns before writing.
- `analysis_scope_hash` = `sha1("|".join(sorted(export_run_id_list)))`.
- `analysis_run_id` = `ana_` + first 12 hex chars of `analysis_scope_hash`.

## Pattern Identity

- Scope is `CORPUS` only.
- Default source for clustering is Phase0 record-level `join_hash` + `join_key_schema` from fingerprint export JSON.
- Pattern id base rule:

```text
pat_ + base32lower_nopad(sha1(f"{domain}|{join_key_schema}|{join_hash}"))[:16]
```

- Collision handling: if duplicate pattern id appears within analysis/domain/schema context, extend token length deterministically until unique.

## Threshold placeholders

v2.1 emits placeholder/default threshold fields for downstream DAX alignment and records constants in `analysis_manifest.notes`.
