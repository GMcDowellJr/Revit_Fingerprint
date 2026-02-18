# Pattern ID and Label Rules (v2.1)

Pattern identity is stable and does not include analysis run identity.

## Pattern ID

```text
pattern_id = "pat_" + base32lower_nopad(sha1(f"{domain}|{join_key_schema}|{join_hash}"))[:16]
```

## Pattern Label

```text
pattern_label = f"{join_key_schema} — Variant {pattern_rank} of {N}"
```

`pattern_rank` is assigned by deterministic ordering: descending cluster size, then schema, then join hash.
