# Hash Discovery Tooling (`tools/discover_hash_policy.py`)

## Data source
`discover_hash_policy.py` reads **flattened CSVs** produced by extraction/flatten stages (for example via `tools/run_extract_all.py`).

It does **not** read the original fingerprint/export JSON files directly.

Expected inputs under `--phase0-dir`:
- Canonical: `records.csv`
- Legacy fallback: `phase0_records.csv`
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
`--phase0-dir` may point either to:
- the direct phase0 folder (e.g., `Results_v21/phase0_v21`), or
- a results root containing `records/records.csv` (e.g., `.../compact/results`),
- the `Results_v21` root (tool will resolve `phase0_v21/` automatically when present).
- a repo/workspace root where pipeline outputs are under `results/records/` (tool resolves that automatically).

## `--stratify-by` (both `tools/discover_join_policy.py` and `tools/discover_hash_policy.py`)

`_stratified_sample()` lives in `tools/discover_join_policy.py` (imported by `discover_hash_policy.py`, which shares most of its I/O/sampling/candidate-selection primitives with it — see that module's own import block). Without it, sampling pools every record for a domain and draws an unweighted sample from the pool, which a few high-volume groups can dominate (e.g. a handful of Template/Container files, which this project's own governance model expects to carry a much larger configured vocabulary than typical Project files — see `corpus_update_runbook.ps1`'s "All view = full configured vocabulary" framing). `--stratify-by` gives every unique value of the chosen key roughly equal representation in the sample instead.

Two kinds of stratify key are supported:
- `file_id` (or `record_id`): read directly off `records.csv` — use this for per-file balance.
- any other value: treated as an identity-item key and looked up per `record_pk` from the items table (e.g. `lft.family_name` for `loaded_family_types` — its original use case). Falls back to flat sampling when the key has no coverage in the data.

When the number of distinct groups exceeds `--sample-size` (the common case for `--stratify-by file_id` on a corpus with more files than the sample cap — e.g. 250 files, `--sample-size 50`), only some groups can get a representative at all. Which groups survive that cap is itself decided by the same seeded deterministic-hash rank `_sample_domain_records` uses for individual records — not group name — so the survivors are a reproducible-but-unbiased (seeded) subset rather than always "whichever files sort first alphabetically."

Records the stratifier has no value for at all (e.g. blank `file_id`) get an **unconditionally reserved share** up front — computed as though "ungrouped" were one more stratum among `n_groups + 1` — rather than being subject to that same seeded survival cap alongside the real groups. Equal-but-not-guaranteed odds would still let them land at zero purely by chance whenever known groups alone already exceed `--sample-size`; for records the stratifier has no data on at all, that's a worse failure than any single real group losing the same lottery — it can silently drop an entire uncharacterized slice of the population, not just one specific file/value — so this slice gets a hard guarantee instead.

`tools/suggest_discovery_params.py` (below) will recommend `--stratify-by file_id` automatically when a domain's population looks concentrated in relatively few files.

## Full-population verification (`tools/discover_join_policy.py` only)

Discovery search (`--search-modes pareto` especially) always runs against the *sampled* record set for tractability — sampling exists specifically so combinatorial search stays cheap. That means the `coverage`/`collision_rate`/`fragmentation_rate` columns in the exploration CSVs, on their own, only certify a candidate against the sample, never against the domain's actual full population. By default, `discover_join_policy.py` now also re-scores whatever candidate was selected against the FULL (unsampled) domain population — a single `O(records_total_domain)` pass via `score_candidate()`, not a combinatorial search, so it stays cheap even when the search itself was sampled — and emits parallel `_full`-suffixed columns: `coverage_full`, `collision_rate_full`, `fragmentation_rate_full`, `records_total_full`, `records_covered_full`, `collision_records_full`, `fragmented_sig_count_full`, `join_group_count_full`, `hhi_full`, `effective_cluster_count_full`, plus `full_verify_status` (`ok` / `skipped_no_full_verify_flag` / `skipped_no_selection`) and `sample_vs_full_diverges`.

`sample_vs_full_diverges` is `true` when any of:
- the full population shows fragmentation the sample reported as zero;
- `collision_rate_full` exceeds the sample's collision rate by more than `--divergence-collision-delta` (default `0.01`);
- `coverage` drops from the sample to `coverage_full` by more than `--coverage-drop-threshold` (default `0.05`) — a candidate that happened to cover every sampled record but is largely absent from the rest of the population isn't globally applicable (Phase-2's own "Global Consistency" principle), and `collision_rate`/`fragmentation_rate` alone won't catch this since both are computed only over *covered* records — a coverage collapse can leave both unchanged while most of the population silently gets no join key at all.

A `[discover] WARNING ...` line is also printed to stdout for any diverging row, so it's visible without opening a CSV.

This closes the gap without requiring a separate `apply_join_policy.py` + split-detection run just to find out whether a sample-derived candidate actually holds up on the real corpus. Disable with `--no-full-verify` if a domain's full population is itself too large to re-score cheaply.

- `--no-full-verify`: skip the full-population re-score (verification runs by default).
- `--divergence-collision-delta`: absolute `collision_rate_full - collision_rate` threshold above which a row is flagged/warned (default `0.01`).
- `--coverage-drop-threshold`: absolute `coverage - coverage_full` threshold above which a row is flagged/warned (default `0.05`).

This does **not** apply to `discover_hash_policy.py` — its exploration CSVs remain sample-only.

## Sizing `--sample-size`/`--max-candidate-fields`/`--max-k`/`--stratify-by` (`tools/suggest_discovery_params.py`)

These four knobs are constants on the CLI, but the right value for each is a function of the domain's actual data, not a flat number applied uniformly to every domain (e.g. `units` with ~11 candidate fields vs. `dimension_types_linear` with 30+). `tools/suggest_discovery_params.py` reads the same `records.csv`/`identity_items.csv` `discover_join_policy.py` reads (no discovery run required) and computes, per domain:

- `--sample-size`: sized off *diversity* (distinct `sig_hash` groups), not just population size — the project's own acceptance criterion is `fragmentation == 0` (`docs/phase_2_join-key_discovery.md`), and fragmentation is only detectable when the sample contains multiple records of the *same* group. `--sample-k-per-group` (default 15) sets the target representatives-per-group; `--sample-floor` (default 500) keeps small domains fully covered.
- `--max-candidate-fields` / `--max-k`: solved *jointly* against a Pareto subset-count compute budget (`--subset-budget`, default 20000; `tools/pareto_joinkey_search.py`'s cost is `Σ C(n, i)` for `i=1..max_k`) — prefers keeping every candidate field and growing `max_k` as far as the budget allows, only trimming the field pool (keeping the highest-frequency fields, same ranking `_pick_candidate_fields` already uses) if even `--min-k` (default 2) doesn't fit the budget at the domain's full field count. Reports a `discover`-mode suggestion and a `harsh`/`validate`-mode suggestion, since those modes fold the domain's existing `required_items` baseline into the search space and need `max_k` large enough to represent it. Critically, the harsh/validate value is **never** bumped past what's verified to still fit the same budget: `tools/discover_join_policy.py`'s actual `pareto_search()` has no required-fields-aware enumeration (plain `itertools.combinations` over every candidate up to `max_k`, blind to which fields are "required"), so a naive `required_count`-sized bump can recommend a command that would evaluate billions of subsets. Because `solve_candidate_fields_and_k` already returns the *largest* `k` affordable at a given pool/budget, `required_count` exceeding it is *provably* infeasible at that same pool/budget (monotonicity of `Σ C(pool, i)`) — there's no smaller-but-still-useful `max_k` to fall back to. When this happens, `harsh_pareto_feasible` is `false`, `suggested_max_k_harsh_validate` stays at the budget-safe discover value, a note explains why, and `--emit-commands` forces `--search-modes greedy` on the harsh/validate command specifically (`discover_greedy()` is `O(max_k * candidates)`, not combinatorial, so it has no such ceiling) while leaving discover/validate free to still use Pareto.
- `--stratify-by`: recommends `file_id` based on an actual file-concentration measure, not a population-size proxy — `file_hhi` (Herfindahl-Hirschman Index over per-file record-count shares, following this repo's own established HHI convention in `docs/METRICS.md`: closed universe, explicit unknown bucket for blank `file_id`) and its derived `file_effective_cluster_count` (`1/file_hhi`). A plain N/F average can't distinguish "6000 records in 1 file" from "6000 records spread evenly across 6000 files" (both average to the same N/F); `file_effective_cluster_count` meaningfully below the domain's actual distinct file count is what a real concentration signal looks like. Only recommended when sampling would actually apply (`suggested_sample_size < N` — with no cap, imbalance can't bias anything).

```bash
python tools/suggest_discovery_params.py \
  --phase0-dir results/records \
  --policy-json policies/domain_join_key_policies.json \
  --emit-commands
```

`--emit-commands` prints ready-to-run `discover_join_policy.py` invocations per domain using the suggested values — two commands, not one, whenever a domain's existing `required_items` baseline pushes `suggested_max_k_harsh_validate` above `suggested_max_k_discover` (one scoped to `--policy-modes discover` at the smaller budget-fit value, one to `--policy-modes validate,harsh` at the larger baseline-aware value), since a single combined command at the smaller value with `discover_join_policy.py`'s default `discover,validate,harsh` modes left enabled would under-size harsh mode's search space relative to what the suggestion itself says is needed. Output also always writes `<phase0-dir>/../diagnostics/discovery_param_suggestions.csv` (override with `--out`).
