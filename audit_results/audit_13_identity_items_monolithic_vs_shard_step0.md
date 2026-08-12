# Audit 13: `identity_items.csv` monolithic vs. per-domain shard — Step 0 findings

**Status:** findings-only, no code changes. Non-code commit per repo convention.

**Trigger:** Greg reported the combined `identity_items.csv` is 17.8GB and asked whether the
per-domain shards (`identity_items_by_domain/*.csv`) could replace it for analysis work.

**Out of scope for this pass:** no files modified. `build_segment_manifest.py` and
`compare_cross_segment.py` untouched (neither reads `identity_items.csv`, confirmed by grep).

---

## 1. The monolithic file is not primary — it's a second write pass over the shard data

`extractor.py`'s `emit_analysis()` writes items natively into per-domain shard files
(`identity_items_by_domain/{domain}.csv`) while streaming exports record-by-record
(`_item_shard_writers[domain]`, ~line 1178). Only *after* every shard is closed does it
reopen and re-stream every shard back into one monolithic file:

```python
# tools/extractor.py, ~line 1237
_monolithic_items_path = out_dir / "identity_items.csv"
with _monolithic_items_path.open("w", ...) as _mono_f:
    for _shard_path in sorted(shard_dir.glob("*.csv")):
        with _shard_path.open("r", ...) as _sf:
            for _srow in csv.DictReader(_sf):
                _mono_w.writerow(...)
```

A second, independent rebuild path exists in `run_extract_all.py`:
`_append_line_pattern_synthetic_norm_hash()` (called from both the `flatten` and `apply`
stages) writes the synthetic norm-hash augmentation rows to the `line_patterns` shard when
shards are in use, then calls `_rebuild_monolithic_identity_items(items_csv, shard_dir)` to
regenerate the entire monolithic file from all shards again (~line 197). In a normal Run A,
this means the full 17.8GB file gets serialized at least twice (once at the end of
`emit_analysis`, again whenever the flatten-stage augmentation pass finds new unaugmented
`line_patterns` rows), sometimes three times if the apply-stage call also finds work to do.

**This is the parallel-computation-path smell called out in `pattern_id_utils.py`** — same
underlying data, written out twice, once as the authoritative per-domain shard and once as a
full concatenation. The shards are the true native output; the monolithic file is a derived
convenience artifact for consumers that haven't been ported to shard-based loading.

---

## 2. Consumer survey — every script that touches `identity_items.csv` / shards

Checked every non-test, non-archive reference (grep across `tools/`). Findings grouped by
actual behavior, not just file mention.

### 2a. Already shard-preferred (5 confirmed — this pattern already exists and works)

These already implement "read the per-domain shard if present + `.complete` sentinel valid,
fall back to filtering the monolithic file only if shards are absent":

| Script | Notes |
|---|---|
| `apply_join_policy.py` | Canonical implementation. Checks `.complete` sentinel; warns and falls back to monolithic on a partial/interrupted shard set rather than silently returning empty items. |
| `compute_latent_purgeable.py` | Per-consumer-domain shard lookup with fallback; scans the monolithic file only once for whichever domains lack a shard. |
| `discover_hash_policy.py` | `_run_target()` is shard-preferred with `.complete`-gated fallback. Has a dead `_load_items()` helper that loads the monolithic file unconditionally — never called anywhere in the file (verified via grep for the call site — none exists). Safe to delete, not required. |
| `materials_joinkey_discover.py` | Single-domain (`materials`) shard lookup, prints an explicit `(slow)` warning on the monolithic fallback path. **Bug: checks `phase0_identity_items_by_domain/materials.csv`, not the canonical `identity_items_by_domain/materials.csv` the pipeline actually writes — see §3.** |
| `discover_vfd_edges.py` | Single-domain (`view_filter_definitions`), shard-only, no monolithic fallback at all — expects the caller to pass a shard directory via `--identity-items-dir`. |
| `build_semantic_groups.py` | Shard-preferred with monolithic fallback. **Same directory-name bug as above — see §3.** |

### 2b. Domain-scoped access, not yet ported (3 — the actual migration targets)

These load the entire monolithic file into memory once, then filter it per domain inside a
loop — functionally identical to what `apply_join_policy.py` already replaced with shard
reads:

| Script | Current pattern |
|---|---|
| `discover_join_policy.py` | `items = _read_csv(items_path)` (whole file), then `dom_items_all = [it for it in items if it.get("domain") == domain]` per domain in the loop. |
| `suggest_discovery_params.py` | Same shape: full load once, `compute_domain_stats(records, items, domain)` filters internally. |
| `discover_hash_policy.py`'s dead `_load_items()` | Not reachable in practice (see 2a) — no action needed beyond optional cleanup. |

No cross-domain joins found in either live script — every domain's items are consumed
independently. These are straightforward ports to the `apply_join_policy.py` pattern.

### 2c. Genuine cross-domain single-pass consumer (1 — needs a different treatment)

`build_identity_items_lookup.py` streams `identity_items.csv` **once, across all domains in
a single pass**, filtering rows against a global `rep_pk_set` keyed by
`(export_run_id, domain, record_pk)` built from *every* domain's representative records. This
is not "filter one big list by domain" — it's a genuinely corpus-wide streaming read.

Migrating this to shards is still straightforward (loop over
`identity_items_by_domain/*.csv` instead of one file, since `rep_pk_set` is already keyed by
domain and the filtering logic is domain-agnostic per row), but it's a different shape of
change than 2b and should be scoped as its own step so the diff is easy to hand-verify.

### 2d. Not actual consumers (ruled out during this pass)

- `label_resolver.py` — takes `identity_items` as an in-memory parameter; never opens a file
  itself. The caller (`build_identity_items_lookup.py`, covered in 2c) is the real consumer.
- `export_to_flat_tables.py` — reads raw Dynamo/CPython3 JSON exports directly and *writes*
  `identity_items.csv` / `identity_items__{domain}.csv` as its own output. It never reads the
  pipeline's `identity_items.csv`. Unrelated tool, out of scope.
- Two probe scripts (`tools/probes/check_line_patterns_normhash.py`,
  `tools/probes/sweep_line_pattern_normhash_precision.py`) — ad hoc, hardcoded
  `C:\Users\gmcdowell\...` paths, single-domain (`line_patterns`) interest, not invoked by the
  runbook. Low-priority, trivial one-line fix if ever touched (point at the shard instead of
  the monolithic file), but not worth a scoped PR on their own.

---

## 3. Shard-directory naming is inconsistent — a real bug, independent of the size question

Canonical name, used by the actual pipeline writers and the majority of readers
(`extractor.py`, `run_extract_all.py`, `apply_join_policy.py`, `discover_hash_policy.py`,
`compute_latent_purgeable.py`, `run_segment_orchestrator.py`, `export_bundle_pattern_detail.py`,
the archetype tools, `reset_wall_types_for_reapply.py`):

```
identity_items_by_domain/
```

Two scripts check different names and will **never find the canonical shard directory the
pipeline actually produces**, silently falling through to the slow monolithic path every time
regardless of whether shards exist:

- `materials_joinkey_discover.py` checks only `phase0_identity_items_by_domain/`.
- `build_semantic_groups.py` checks `identity_items_shards/` first, then
  `phase0_identity_items_by_domain/` — neither matches `identity_items_by_domain/`.

This means their "shard-preferred" code paths are effectively dead code today: they always
take the monolithic branch. Worth a one-line fix independent of the larger consolidation
decision, since it's currently costing the slow path on every run for those two scripts.

---

## 4. Open questions before scoping a PR

1. **Do we keep writing the monolithic file at all, or drop it entirely?** Dropping it
   removes the double/triple write and the 17.8GB file, but every 2b/2c consumer needs to be
   migrated first (or given the same `.complete`-gated fallback apply_join_policy.py uses, so
   a missing monolithic file doesn't silently break an unported script).
2. **Order of operations:** the naming-bug fix in §3 is safe and independent — it can land
   first, on its own, with no behavior change to anything that currently uses the correct
   directory name. Migrating 2b (2 scripts) is the next-safest increment. 2c
   (`build_identity_items_lookup.py`) is a different shape of change and should be its own
   step so before/after row counts are easy to hand-verify against a live corpus.
3. **Should the monolithic file be kept as a deliberately-generated, opt-in export** (e.g. via
   `export_to_flat_tables.py`, which already produces a differently-sourced version of the
   same filename) rather than a byproduct of every Run A, for the rare case someone actually
   wants one file? Greg's call — no code implication either way for this findings pass.
4. Acceptance criteria for any follow-up PR should be hand-traceable real numbers from a live
   re-run (row counts per shard vs. monolithic, before/after wall-clock and disk usage for Run
   A), not golden files, per standing convention.

---

## 5. Recommended sequencing (pending Greg's sign-off — no PRs opened yet)

1. **PR 1 (trivial, isolated):** Fix the `identity_items_by_domain` naming mismatch in
   `materials_joinkey_discover.py` and `build_semantic_groups.py`. Zero behavior change for
   any script already using the canonical name; only affects the two scripts whose
   shard-preferred paths are currently dead.
2. **PR 2:** Port `discover_join_policy.py` and `suggest_discovery_params.py` to the
   shard-preferred-with-fallback pattern from `apply_join_policy.py`.
3. **PR 3:** Port `build_identity_items_lookup.py`'s cross-domain streaming read to loop over
   shards, with a `.complete`-gated fallback to the monolithic file.
4. **PR 4 (only after 1–3 land and are verified against a live corpus run):** Stop writing the
   monolithic `identity_items.csv` from `extractor.py` and `_rebuild_monolithic_identity_items`
   in `run_extract_all.py`. This is the step that actually removes the 17.8GB file and the
   double-write; it should be last and should have hand-traced before/after numbers proving no
   consumer regressed.
