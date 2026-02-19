# Pattern ID and Label Rules (v2.1)

Pattern identity is stable and does not include analysis run identity.

## Pattern ID

```text
pattern_id = "pat_" + base32lower_nopad(sha1(f"{domain}|{join_key_schema}|{join_hash}"))[:16]
```

If a collision occurs within `(analysis_run_id, domain, join_key_schema)`, extend the base32 token length deterministically until unique.

## Pattern Label

```text
pattern_label = f"{join_key_schema} — Variant {pattern_rank} of {N}"
```

`pattern_rank` is assigned by deterministic ordering: `pattern_size_files` desc, then `pattern_size_records` desc, then `pattern_id` asc.

Pattern construction source is Phase0 record-level `join_hash`/`join_key_schema` from fingerprint export JSON by default.
