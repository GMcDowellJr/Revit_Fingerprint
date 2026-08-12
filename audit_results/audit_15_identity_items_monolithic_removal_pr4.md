# Audit 15 / PR 4: Stop writing the monolithic `identity_items.csv`

**Status:** code change, no hash-semantic change (refactor only -- no `sig_hash`/`join_hash`
value changes; hashes are computed identically whether items are read from the shard files or
the monolithic file, since both carry the same rows). Follows
`audit_results/audit_13_identity_items_monolithic_vs_shard_step0.md`'s recommended PR 4
sequencing (PR 1/2/3 already merged as PR #418/#419/#420).

---

## Gate check before starting

The PR 4 brief requires PR 1-3 "landed and independently verified against a live corpus run"
before starting, with sign-off. PR 1-3 are merged to `main` (`57cbf2a`), but PR 2's own audit
doc (`audit_14`) states its verification used a synthetic fixture, not a live corpus -- this
sandbox has no Revit/corpus access either. Flagged to Greg directly; he confirmed to proceed.

## Step 0 -- re-confirmation against current `main` (57cbf2a)

Re-ran `grep -rn "identity_items.csv" tools/ | grep -v tests` fresh rather than trusting
audit_13, per the brief's explicit instruction.

**Confirmed already shard-preferred / ported (matches audit_13 §2a + PR1/PR2/PR3), re-verified
by reading each script's current gating logic, not just grepping filenames:**

| Script | Gate observed |
|---|---|
| `apply_join_policy.py` | `.complete`-gated, canonical `identity_items_by_domain/` name |
| `compute_latent_purgeable.py` | Same pattern |
| `discover_hash_policy.py` | `_run_target()` shard-preferred; dead `_load_items()` confirmed zero call sites via grep, left alone (not required to remove) |
| `materials_joinkey_discover.py` | PR1 naming fix confirmed landed -- checks `identity_items_by_domain/` (not the old `phase0_identity_items_by_domain/`) |
| `build_semantic_groups.py` | PR1 naming fix confirmed landed -- checks `identity_items_by_domain/` |
| `discover_vfd_edges.py` | Shard-only, no monolithic fallback at all |
| `discover_join_policy.py` | PR2 port confirmed landed -- `.complete`-gated with monolithic fallback |
| `suggest_discovery_params.py` | PR2 port confirmed landed -- same pattern |
| `build_identity_items_lookup.py` | PR3 port confirmed landed -- `.complete`-gated, loops shards |

`.complete` sentinel content is never parsed anywhere in the codebase (`grep -rn "\.complete"
tools/` across every reader) -- every consumer only calls `.is_file()`. Content is therefore
free to change without touching any reader.

`extractor.py`'s own internal `_load_identity_items_by_record()` (used by the `patterns`/
`authority` engine via `emit_analysis()` at the `_process_one_domain()` call site) was already
shard-preferred before this PR -- not one of the PR1-3 ports, but confirmed safe.

**Confirmed non-consumers (re-verified, matches audit_13 §2d):**
- `label_resolver.py` -- takes `identity_items` as an in-memory parameter, opens no file.
- `export_to_flat_tables.py` -- re-verified via fresh grep (4 hits, all inside its own
  `identity_items.csv` / `identity_items__{domain}.csv` *write* path from raw JSON exports).
  Never reads the pipeline's `identity_items.csv`.

**Probe scripts (Step 0 item 2 -- decided, not left silently broken):** Both
`tools/probes/check_line_patterns_normhash.py` and
`tools/probes/sweep_line_pattern_normhash_precision.py` hardcode
`...\phase0_identity_items.csv`. That filename is a **legacy naming convention the current
pipeline has never written** (only `identity_items.csv`, with `phase0_identity_items.csv`
appearing elsewhere in the codebase only as an *optional fallback check* for very old exports)
-- so these two probes were already broken against any current-pipeline output, independent of
this PR. Chose option (b) from the brief: one-line port to point them at
`identity_items_by_domain/line_patterns.csv` directly (single-domain interest, so the shard is
a straight drop-in). Left a comment explaining why in both files.

## A finding beyond the brief's literal scope -- required, not optional

The brief named one call site to remove
(`run_extract_all.py`'s `_rebuild_monolithic_identity_items()`). Re-auditing every reference to
`identity_items.csv` inside `run_extract_all.py` itself (not just the external consumer
scripts audit_13 enumerated) surfaced three more internal gates that hard-require the
monolithic file's existence, independent of the call being removed:

1. **`_append_line_pattern_synthetic_norm_hash()`** -- `if not items_csv.is_file(): return
   {...}` was the *first* line, before the shard-mode check. Removing the monolithic write
   would turn this into a silent permanent no-op.
2. **`_validate_line_pattern_synthetic_norm_hash()`** -- raised `SystemExit` if the monolithic
   file was absent, unconditionally. This runs in the `apply` stage (default pipeline) --
   **Run A would hard-crash**, not degrade, without a fix.
3. **`_apply_sig_hash_to_phase0()`** (the `sig_hash` stage, T0.5, auto-inserted before
   `discover`/`apply`) -- `if not records_csv.is_file() or not items_csv.is_file(): return
   diag` early-returned even though the actual per-domain item lookup already preferred
   `_ensure_domain_scoped_identity_items()`'s shard dir. Without a fix, every future run would
   silently produce empty `sig_hash`/`join_hash` for every record.

None of these three have test coverage (`grep -rln` for each function name across `tests/`
returns nothing). Fixed all three in this PR using the same shard-preferred-with-fallback
pattern already used everywhere else in the codebase (native `identity_items_by_domain/
.complete` checked first; falls back to `_ensure_domain_scoped_identity_items()`'s
rebuild-from-monolithic-if-stale logic only for a legacy phase0 dir that has a monolithic file
but no native shard sentinel). `_ensure_domain_scoped_identity_items()` itself is untouched --
it already correctly returns `None` when no monolithic file exists; the bug was in callers
gating on its *absence* before even trying it.

The one purely-cosmetic caller (`report["notes"]` "identity_items_shards=..." message in the
`analyze`-stage block) was also updated to check the native shard dir directly instead of the
now-permanently-`None` legacy call, so the note doesn't silently disappear from every future
run's report.

## Change made

- **`tools/extractor.py`** (`emit_records()`, ~line 1238): removed the block that reopened
  every shard under `identity_items_by_domain/` and rewrote them into
  `out_dir / "identity_items.csv"`. Shard-writing itself (`_item_shard_writers`, one file
  handle per domain, closed at the end of the loop) is untouched. The `.complete` sentinel is
  still written immediately after the shards close, now stamped with a wall-clock timestamp
  instead of the (no-longer-existing) monolithic file's mtime -- harmless, since sentinel
  content is never parsed (see Step 0 above).
- **`tools/run_extract_all.py`**: removed `_rebuild_monolithic_identity_items()` (confirmed via
  grep: zero remaining call sites) and its call from
  `_append_line_pattern_synthetic_norm_hash()`'s `use_shard` branch -- that branch still writes
  the augmented rows into the `line_patterns` shard exactly as before; only the rebuild-and-touch
  step after it is gone. Fixed the three additional internal gates described above.
- **`tools/probes/check_line_patterns_normhash.py`** /
  **`tools/probes/sweep_line_pattern_normhash_precision.py`**: one-line port from the
  never-written `phase0_identity_items.csv` name to `identity_items_by_domain/line_patterns.csv`.

## Verification

No live corpus available in this sandbox (same limitation as PR2/audit_14). Two synthetic
harnesses were built and are the actual regression check for this PR, since the goal is "does
removing the monolithic write break anything downstream" -- these exercise the real code paths,
not fixtures reverse-engineered from assumptions about them.

**1. Native shard-only mode** (`emit_records()` output as this PR now produces it -- 2 synthetic
export files, `arrowheads` + `line_patterns` domains, 14 records total):
- `identity_items.csv` confirmed absent after `emit_records()`.
- `identity_items_by_domain/.complete` sentinel present.
- Shard row counts match exactly what `emit_records()` wrote (10 `arrowheads` item rows, 54
  `line_patterns` item rows, hand-computed from the synthetic fixture's item lists).
- `_append_line_pattern_synthetic_norm_hash()` appended 9/9 synthetic norm-hash rows (one per
  `line_patterns` record) directly into the shard, `missing=0`.
- `_validate_line_pattern_synthetic_norm_hash()` raised no `SystemExit`.
- `_apply_sig_hash_to_phase0()` processed and hashed all 14/14 records from native shards
  (`records_blocked=0`), writing non-empty `sig_hash` into `records.csv` for every record --
  confirms the sig_hash stage no longer silently no-ops.

**2. Legacy monolithic-only mode** (a hand-built phase0 dir with only `identity_items.csv`, no
`identity_items_by_domain/` at all -- simulates a pre-shard-era export directory someone might
still have on disk):
- `_append_line_pattern_synthetic_norm_hash()`: 3/3 synthetic rows appended via the monolithic
  fallback branch, unchanged from before this PR.
- `_validate_line_pattern_synthetic_norm_hash()`: no `SystemExit`.
- `_apply_sig_hash_to_phase0()`: processed all 3 records via `_ensure_domain_scoped_identity_items()`'s
  rebuild-from-monolithic fallback, which itself derived and wrote a fresh
  `identity_items_by_domain/.complete` -- confirms the legacy path (still needed for any
  pre-existing phase0 output that predates shard writing) is completely unchanged by this PR.

**3. Full suite:** `pytest tests/ -v` -- 1175 passed, 7 skipped (identical counts to the PR2
baseline in audit_14). No test references any of the removed/changed functions directly
(confirmed via grep before editing), so this is a coverage gap this PR does not close, not a
result this PR could have broken.

**Not done (needs Greg, real corpus access):** the acceptance criteria's wall-clock/disk-usage
deltas for an actual Run A against the ~17.8GB corpus, and re-running the 9 confirmed-ported
consumers (`apply_join_policy.py` etc.) against real post-change output to diff byte-for-byte
against their pre-change output. This sandbox cannot produce those numbers -- they're the real
payoff of this PR and need to come from an actual run.

## Open questions (not resolved here, flagged per the brief)

1. **Probe scripts**: fixed with the one-line shard port (option b) rather than left broken or
   deferred, since the fix was trivial and the alternative (a stale hardcoded path pointing at
   a file the pipeline has never produced) was strictly worse.
2. **Opt-in monolithic export via `export_to_flat_tables.py`**: not implemented, per the brief
   ("this PR should not add that feature, just note it as a follow-up option"). Greg's call
   whether it's ever worth adding.
3. **Nothing-outside-the-repo confirmation**: repo-code investigation cannot see Greg's BI
   layer or any manual scripts outside this repository. Needs explicit confirmation from Greg
   before this PR merges that nothing outside the repo depends on
   `results/records/identity_items.csv` existing on disk.
