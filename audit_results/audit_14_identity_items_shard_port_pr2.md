# Audit 14 / PR 2: Port `discover_join_policy.py` and `suggest_discovery_params.py` to shard-preferred loading

**Status:** code change, no semantic change. Follows `audit_13_identity_items_monolithic_vs_shard_step0.md`'s
recommended PR 2 sequencing (PR 1, the `identity_items_by_domain` naming fix, already merged as PR #418).

---

## Step 0 — re-confirmation against current `main`

Re-checked both scripts before editing, since audit_13's claims should not be assumed still current:

* **`discover_join_policy.py`**: confirmed the exact pattern described in audit_13 §2b — `items =
  _read_csv(items_path)` loaded the whole monolithic file once at module scope (old line 406), then
  `dom_items_all = [it for it in items if it.get("domain") == domain]` filtered it per domain inside the
  `for i, domain in enumerate(domains, ...)` loop (old line 441). No other use of the full `items` list existed
  except a `len(items)` count in the startup log line.
* **`suggest_discovery_params.py`**: confirmed the same shape — `items = _read_csv(items_path) if
  items_path.exists() else []` loaded the whole file once (old line 545), then `compute_domain_stats(records,
  items, domain)` filtered internally (`dom_items = [it for it in items if it.get("domain", "") == domain]`,
  line 141). `compute_domain_stats()` is directly unit-tested with a full, multi-domain `items` list and is
  expected to filter it internally (`tests/test_suggest_discovery_params.py::test_compute_domain_stats_counts_n_g_f_and_candidates`),
  so its public signature/behavior was left unchanged — only the caller in `main()` now passes an
  already-per-domain-scoped list instead of the full monolithic one. Filtering a list that's already scoped to
  one domain is a no-op (shard rows carry the same `domain` column the monolithic rows do), so this is safe.
* **`apply_join_policy.py`**'s `_get_domain_items()` closure (lines ~58-89) re-read in full as the pattern to
  replicate: shard-preferred, gated on `identity_items_by_domain/.complete`, with a one-time monolithic
  load-and-partition fallback and an explicit stderr warning when CSVs exist under the shard directory but the
  sentinel is absent (interrupted-flatten protection).
* **No cross-domain access found** in either script — grepped every use of the `items`/`dom_items_all` variables;
  each script only ever narrows to one domain's items before scoring/sampling. Confirms audit_13's §2b
  classification still holds; this is a straightforward port, not a §2c-style (`build_identity_items_lookup.py`)
  cross-domain streaming case.

## Change made

Both scripts now load identity items via a per-script `_get_domain_items(domain)` closure, structurally
identical to `apply_join_policy.py`'s:

1. Check `identity_items_by_domain/.complete`. If present, read `identity_items_by_domain/{domain}.csv` per
   domain (empty list if that domain's shard file happens to be absent).
2. If absent, and the shard directory exists with CSV files in it (an interrupted/partial flatten run), print a
   `[WARN discover]` / `[WARN suggest]` message to stderr and fall back to loading the monolithic
   `identity_items.csv` once, partitioned into an in-memory `{domain: [rows]}` dict.
3. If the shard directory doesn't exist at all, silently use the monolithic fallback (same as before this port
   for any flatten output produced by a pre-shard pipeline run).

One deliberate behavioral difference between the two scripts, kept because it mirrors each script's original
tolerance: `discover_join_policy.py` treats a missing monolithic file (when shards aren't in use) as fatal
(`SystemExit`, matching its original unchecked `_read_csv(items_path)` which would previously raise
uncaught on a missing file) — `suggest_discovery_params.py` keeps its original tolerant behavior (empty items,
`compute_domain_stats` degrades to `candidate_field_count=0` for the affected domain rather than crashing),
matching its original `items_path.exists()` guard.

The `[discover] loaded records=... identity_items=<N>` startup log line no longer reports a total item count
(that would require the full monolithic load this port exists to avoid); it now reports `item_source=shards`
or `item_source=monolithic` instead. No test asserted on the old count value.

`suggest_discovery_params.py`'s `compute_domain_stats()` function signature and behavior are unchanged — only
its caller now passes pre-scoped items.

## Verification (hand-traceable, synthetic corpus — no golden files)

No live 17.8GB corpus is available in this environment, so verification used a synthetic phase0 directory
(2,452 records / 8,598 identity items across 4 domains: `units`, `object_styles_model`, `line_patterns`,
`dimension_types_linear`) with both a complete shard set (`.complete` sentinel present) and a monolithic
`identity_items.csv` built from the exact same source rows, so the two loading paths can be compared
byte-for-byte instead of just "looks reasonable."

1. **Shard path** (`identity_items_by_domain/.complete` present): ran both scripts; `discover_join_policy.py`
   logged `item_source=shards`.
2. **Per-domain row counts**: independently cross-checked, for every domain, that
   `len([it for it in _read_csv(identity_items.csv) if it["domain"] == d])` equals
   `len(_read_csv(identity_items_by_domain/{d}.csv))` — all four domains matched exactly
   (1423/1423, 2943/2943, 2797/2797, 1435/1435).
3. **Monolithic fallback path**: renamed `identity_items_by_domain/` aside (simulating "shards absent") and
   re-ran both scripts with identical CLI args.
   * `discover_join_policy.py`'s diagnostics directory (`join_key_discovery_exploration*.csv`,
     `join_key_discover*.csv`, `join_key_validate*.csv`, `join_key_harsh*.csv`) was **byte-for-byte identical**
     between the shard run and the monolithic-fallback run (`diff -rq` exit 0); the emitted `--out-policy` JSON
     was also identical (`diff` exit 0).
   * `suggest_discovery_params.py`'s `discovery_param_suggestions.csv` and full `--emit-commands` stdout were
     both **byte-for-byte identical** between the two paths (`diff` exit 0).
4. **Wall-clock** (secondary, non-blocking, synthetic corpus so not representative of the real 17.8GB case):
   `discover_join_policy.py` ~0.48s (shards) vs. ~0.54s (monolithic); `suggest_discovery_params.py` ~0.11s vs.
   ~0.11s (population too small at this scale for the monolithic-read cost to show up — the real win is avoiding
   the 17.8GB single-file read/scan on a live corpus, which this synthetic fixture can't reproduce).
5. Full `pytest tests/ -v` suite: 1175 passed, 7 skipped (unchanged skip count), including
   `tests/test_discover_join_policy_verification.py` and `tests/test_suggest_discovery_params.py`.

## Open questions (not resolved here, per the PR 2 prompt)

* No cross-domain access pattern was found in either script during Step 0 — the §2c-style concern in the PR 2
  prompt does not apply.
* Warning-message wording was adapted per-script (`[WARN discover]` / `[WARN suggest]`) rather than extracted
  into a shared helper, matching the prompt's stated default (a shared helper is a larger refactor than this
  PR's scope).
