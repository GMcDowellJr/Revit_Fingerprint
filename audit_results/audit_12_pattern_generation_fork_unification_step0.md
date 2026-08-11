# Audit 12 — Pattern-Generation Fork Unification, Step 0

Date: 2026-08-11
Scope: Findings-only. Determine whether `tools/generate_name_key_patterns.py`'s output can
be made to natively match `tools/extractor.py`'s config-target patterns-stage shape, closing
`tools/bundle_analysis/name_projection_adapter.py` as a reshaping layer, so bundle analysis,
`compare_cross_segment.py`, and `generate_governance_narrative.py` can consume patterns
through one interface parameterized by `comparison_target`. No code changes in this PR.

## Files read in full

`tools/extractor.py` (`_stable_pattern_id`, `_process_one_domain`, `emit_analysis`, the
`domain_patterns.csv`/`pattern_presence_file.csv`/`authority_patterns.csv`/
`pattern_diagnostics.csv` writers, `_load_label_resolution_inputs`, `_load_semantic_groups`,
`_load_identity_items_by_record`), `tools/generate_name_key_patterns.py` (PR2),
`tools/pattern_id_utils.py`, `tools/bundle_analysis/name_projection_adapter.py`,
`tools/apply_name_key_policy.py` (output schema), `core/name_key_coverage.py` (eligibility
registry), and the relevant read paths of `tools/compare_cross_segment.py`
(`domain_patterns_path`/`pattern_presence_file_path`/`resolve_join_hashes`/
`load_pattern_labels`/`load_file_join_hashes`/`discover_domains_for_segment`) and
`tools/generate_governance_narrative.py` (its slicer-dimension usage, `argparse` surface).
Cross-referenced against `audit_results/audit_8_bundle_pipeline_name_projection.md` items 1–12.

## 1. The two writers, side by side

### `domain_patterns.csv`

| Column | Production (`extractor.py` `emit_analysis`) | PR2 `config` target | PR2 `name` target | Adapter output |
|---|---|---|---|---|
| `schema_version` | ✅ real | ✅ (verbatim copy) | ❌ missing | synthesized constant |
| `analysis_run_id` | ✅ real (`ana_<hash>`) | ✅ (verbatim copy) | ❌ missing | synthesized constant `"name_projection"` |
| `domain` | ✅ | ✅ | ✅ | passthrough |
| `pattern_id` | ✅ (`_stable_pattern_id`) | ✅ | ✅ (`pattern_id_utils.stable_pattern_id` — same formula, independent impl) | passthrough |
| `pattern_label` | ✅ generic `"<schema> — Variant N of M"` | ✅ | ✅ (`pattern_id_utils.pattern_label`, same formula) | passthrough |
| `source_cluster_id` | ✅ `domain\|schema\|hash` | ✅ | ✅ (same shape, hash is `join_key_name_identity`'s hash not `sig_hash`) | passthrough |
| `pattern_size_records` / `_files` | ✅ computed | ✅ | ✅ computed by `build_clusters` | passthrough |
| `pattern_rank` | ✅ (files desc, records desc, pid asc) | ✅ | ✅ (`rank_clusters`, same ordering) | passthrough |
| `is_candidate_standard` | ✅ `presence_pct >= STANDARD_PRESENCE_MIN` (needs `files_total`) | ✅ | ❌ missing — **no `files_total`/presence computation exists in PR2 at all** | synthesized `""` |
| `notes` | ✅ (always `""` in practice) | ✅ | ❌ missing | synthesized `""` |
| `pattern_label_human` | ✅ `resolve_pattern_label()` (label-synthesis pipeline, gracefully falls back to `pattern_label` when inputs absent) | ✅ | ❌ missing | copied from PR2's `pattern_label` (i.e. always the fallback value) |
| `pattern_label_source` | ✅ | ✅ | ❌ missing | synthesized `"name_projection_pattern_label"` |
| `pattern_label_fallback` | ✅ | ✅ | ❌ missing | synthesized `""` |
| `is_cad_import` | ✅ substring check on `resolved_label`, `domain == "view_category_overrides"` only | ✅ | ❌ missing (**and structurally can never be non-false** — VCO domains are all in `EXCLUDED_DOMAINS`, see §3 item 5) | synthesized `""` (→ false) |
| `semantic_group` | ✅ `_load_semantic_groups()` (label-synthesis cache, keyed `domain→pattern_id`, gracefully empty when absent) | ✅ | ❌ missing | synthesized `""` |
| `join_key_schema` | ❌ not a column (folded into `source_cluster_id` only) | ❌ | ✅ present natively | **appended** (needed by `derive_scope_key()`/`SHAPE_GATED_DOMAINS`) |
| `join_hash` | ❌ not a column | ❌ | ✅ present natively | dropped |
| `coverage_class` | ❌ not a column | ❌ | ✅ present natively | dropped (carried separately via `domain_coverage.csv`) |

### `pattern_presence_file.csv` vs `pattern_membership.csv`

This is the deeper gap. Production `pattern_presence_file.csv` is **file-level, one row per
(export_run_id, domain, pattern_id)**, and every non-key column
(`pattern_share_pct`, `is_dominant_pattern`, `deviation_score`, `corpus_classification`) is
the *output of real aggregation logic* in `_process_one_domain`'s per-export loop
(`tools/extractor.py:802–874`): count records per pattern per file, resolve dominant-pattern
ties, compute `deviation_score = dominant_share − share`, and threshold
`corpus_classification` against `STANDARD_PRESENCE_MIN` using `domain_pattern_presence_pct`
(itself built from `authority_rows_local`, i.e. HHI/presence-pct machinery computed earlier
in the same function).

PR2's `pattern_membership.csv` is **record-level, one row per (export_file, domain,
record_id, pattern_id)** — a straight per-record cluster assignment from `build_name_membership()`.
It performs **no aggregation at all**: no per-file counting, no dominance resolution, no
deviation score, no corpus-standard threshold. The adapter's `stage_name_projection_analysis_dir()`
does not compute these either — it dedupes rows to `(domain, export_run_id, pattern_id)` and
fills every numeric/classification column with a neutral placeholder (`""`, `"false"`).

**This means the presence/dominance/deviation/corpus-classification computation PR2 needs
does not exist anywhere in the PR2/adapter code today.** It is not a reshaping problem the
adapter is hiding — it is unwritten logic. The only place this logic exists is inline inside
`_process_one_domain`.

## 2. Does the same logic actually generalize?

Checked whether `_process_one_domain`'s presence/dominance computation is intrinsically
tied to the config projection's data shape, or would operate unchanged on name-projection
rows:

- **Input row shape**: `_process_one_domain` operates on generic dicts with
  `export_run_id`, `record_pk`, `domain`, `join_hash`, `join_key_schema` keys — it never
  reads `sig_hash` or any config-specific field directly. `apply_name_key_policy.py`'s output
  (`export_file, domain, record_id, label_display, join_key_schema, join_hash, status,
  missing_required`) supplies the same information under different key names
  (`export_file`→`export_run_id`, `record_id`→ *not* `record_pk` — see below) plus a `status`
  gate PR2 already applies (`status == "ok"`). Structurally compatible after a rename, with
  one real gap: **`record_pk` is a flatten-time composite (`f"{file_id}|{domain}|{record_ordinal}"`,
  `tools/extractor.py:1129`), not `record_id`** (record.v2's own `record_id` field, what
  `apply_name_key_policy.py` emits). Reusing `_load_identity_items_by_record()`'s
  `phase0_dir`-sourced identity-items table (needed for `pattern_label_human` resolution, see
  below) for name-target rows requires resolving `record_pk` from `(export_run_id, domain,
  record_id)` via phase0's own `records.csv` — a real join, not automatic, but the underlying
  evidence (identity_items keyed by record) is **already computed once at flatten time and is
  reusable verbatim** — it does not need recomputation for the `name` target, since identity
  items are a per-record artifact independent of which join_hash scheme clusters them.
- **Label resolution (`pattern_label_human`, `semantic_group`)**: both are already optional
  and gracefully degrade to the generic fallback when their backing files
  (`Results_v21/label_synthesis/*`, keyed by domain/join_hash/pattern_id) are absent
  (`_load_label_resolution_inputs` returns `{}, {}, {}`; `_load_semantic_groups` returns `{}`
  when the cache file is missing). Nothing in the resolution code path assumes the join_hash
  came from the config projection specifically — it is keyed generically by whatever
  `join_hash` value is passed in. Running label-synthesis *against* the name-projection's
  join_hash population is a separate, real body of work (a full corpus pass through
  `tools/label_synthesis/`) — but the **column and fallback machinery in `domain_patterns.csv`
  itself already supports "no label synthesis has run for this hash space" as a first-class
  state** (`pattern_label_human == pattern_label_fallback`, `pattern_label_source` says so).
  This is a data-availability gap, not a shape gap.
- **`is_cad_import`**: audit_8 item 5 already correctly flags this as unavailable for
  name-target, but the mechanism is worth restating precisely: it is not that name-projection
  records lack import evidence in general — it's that the *only* domain this flag is computed
  for (`view_category_overrides`, keyed on a `.dwg`/`Imports in families|` substring match on
  the resolved label) is itself in `EXCLUDED_DOMAINS` (`no_name_like_key`) for the entire
  domain family (`view_category_overrides`, `_model`, `_annotation`). So under
  `comparison_target=name` this column is not just always-false by missing data — it is
  **always false by construction**, because the one domain it could ever be true for never
  appears in name-target patterns at all. Confirms audit_8 item 5/6's conclusion from a
  different angle.

**Conclusion**: the presence/dominance/deviation/corpus-classification logic in
`_process_one_domain` is input-shape-agnostic once fed normalized rows. It is not
config-specific business logic — it is a general "cluster records by (domain, schema, hash),
then compute per-file participation stats" algorithm that happens to only have one caller
today.

## 3. Re-classified audit_8 delta catalog

| # | audit_8 item | Classification | Rationale |
|---|---|---|---|
| 1 | `analysis_run_id` single-value requirement | **Representational** | Purely a `bundle_analysis/common.py` schema contract. A unified writer emits a real (or deliberately-constant, documented) `analysis_run_id` the same way for either target — no information the name projection lacks. |
| 2 | Hardcoded filenames (`pattern_presence_file.csv` vs `pattern_membership.csv`) | **Representational** | Naming only, once #(the presence-computation gap below) is closed. |
| 3 | Column-name/grain mismatch (record-level vs file-level, `export_file` vs `export_run_id`) | **Representational, but non-trivial** | Column renames are cosmetic; the record→file aggregation is real, unwritten logic — see §2. Closing it means either porting `_process_one_domain`'s per-file loop into PR2, or (recommended, §4) calling the same function PR2 does not currently call at all. Not a semantic limit of name-identity data — the data needed (which file each record came from) is already in `apply_name_key_policy.py`'s output. |
| 4 | `domain_patterns.csv` column-set difference | **Representational** | Every missing column (`schema_version`, `analysis_run_id`, `is_candidate_standard`, `notes`, `pattern_label_human`, `pattern_label_source`, `pattern_label_fallback`, `semantic_group`) is either a constant, a derivable aggregate (§2), or an already-optional field with a defined degraded state. `is_cad_import` is the one column here that is semantically forced to `false`, not merely representationally absent — see item 5. |
| 5 | CAD-import exclusion unavailable for name-target | **Semantic** (confirmed) | Real limitation of what name-identity data represents for the one domain the signal applies to (`view_category_overrides`), and that domain is structurally excluded from name-target entirely (§2). Unification cannot manufacture CAD-import evidence that was never captured under the name projection. |
| 6 | Row-key/shape-gated scope quirk (`ROW_KEY_DOMAINS`/`SHAPE_GATED_DOMAINS` pre-D-015 names) | **Representational / moot, confirmed pre-existing** | audit_8 already correctly scoped this as a `config`-target quirk unrelated to unification; out of scope here too (fixing it changes `config` output, violating the byte-identical requirement). |
| 7 | USED view / latent-purgeable filtering blocked for name-target | **Semantic** (confirmed) | `source_cluster_id`'s trailing hash is `join_key_name_identity`'s hash, not `sig_hash` — cross-referencing it against `records/latent_purgeable.csv` (a `sig_hash`-keyed artifact) would silently compare the wrong identity space. No column rename fixes this; it requires either a name-projection-native purgeability signal (does not exist) or an explicit permanent block. Unification does not change this — it only means the block can now be expressed as "USED view requires `comparison_target=config`" in one place instead of the adapter's ad hoc guard. |
| 8 | Placeholder-exclusion / `records.csv`'s `is_purgeable` dependency | **Semantic** (confirmed) | Same root cause as #7 — `is_purgeable` is a `sig_hash`-space artifact with no name-projection equivalent. |
| 9 | `--compute-share-profile` needs `pattern_share_pct`/`is_dominant_pattern` | **Representational, contingent on #3** | Once the presence/dominance computation is ported/shared (§2), these fields exist natively for name-target with the same semantics they have for config-target — the values are computed from record→file membership, which name-projection data has in full. audit_8 filed this as blocked because PR2 never computed it, not because the name projection lacks the underlying evidence. **Re-classifying this item is the biggest scope change from audit_8**: unification, if it includes porting the presence computation, closes this gap rather than just relocating it. |
| 10 | `--compare` / `reference_bundle.json` baseline | **Semantic** (confirmed) | No name-projection reference baseline exists or is remotely close to being defined (D-024/D-025-style baseline work would be required); orthogonal to the shape-unification question. |
| 11 | `--roles` / `file_metadata.csv` filtering | **Representational** (confirmed, already noted as composing cleanly) | No change from audit_8. |
| 12 | Output-directory namespacing/collision risk | **Representational** | A tooling/CLI convention (separate `--out-dir` roots), unrelated to schema shape. |

**Net correction to audit_8's framing**: item 9 (share-profile) was filed as a hard semantic
gap in the PR3 brief's original scoping, but Step 0's read shows it is representational
*conditional on* closing item 3's presence-computation gap. Only 7, 8, and 10 survive as
genuine semantic gaps — CAD-import (5) is best read as a *specific instance* of "the config
projection's `view_category_overrides` family has evidence the name projection structurally
cannot have," not a separate general-purpose gap, since the mechanism (label substring match)
would degrade to always-false even if ported verbatim. Item 6 was already correctly scoped
as moot/pre-existing in audit_8.

## 4. Q1 — Which direction should unification go?

**Recommendation: refactor `tools/extractor.py`'s pattern/presence computation to be
`comparison_target`-parameterized and shared, not teach `generate_name_key_patterns.py` to
reimplement it.**

Reasons:

1. **The missing logic is non-trivial and stateful** — HHI, dominant-pattern tie resolution,
   deviation score, and the `STANDARD_PRESENCE_MIN` threshold all live inside
   `_process_one_domain`'s per-export loop (`tools/extractor.py:802–874`) and share state with
   the per-cluster loop just above it (`domain_pattern_presence_pct`, built from
   `authority_rows_local`). Reimplementing this in `generate_name_key_patterns.py` means a
   second, independent implementation of the same statistics — precisely the kind of
   dual-implementation risk `tools/pattern_id_utils.py`'s own docstring already flags for the
   much simpler `pattern_id`/`pattern_label` formula ("No test currently cross-checks the two
   implementations stay in agreement... a future change to only one would go uncaught").
   Duplicating the *harder* logic (presence/dominance/HHI) doubles down on that exact
   known-gap pattern instead of retiring it.
2. **The shared logic is already input-shape-agnostic** (§2) — it does not need to "know"
   which projection it is computing over. The natural boundary is: normalize whichever
   source's rows into the `(export_run_id, record_pk_or_equivalent, domain, join_hash,
   join_key_schema)` shape `_process_one_domain` already consumes, then call one function.
   `comparison_target` would select the *input adapter* (flatten records for `config`,
   `apply_name_key_policy.py` rows restricted to `ELIGIBLE_DOMAINS` for `name`) and a couple
   of target-scoped knobs (domain scope list, whether label-synthesis/CAD-import inputs are
   even attempted), not a second computation.
3. **`pattern_id`/`pattern_label` already prove the "kept deliberately independent" approach
   creates drift risk** — PR2 chose the independent-reimplementation path there specifically
   *because* `_stable_pattern_id` was small enough to duplicate safely, and even that decision
   left a known, documented, untested gap. The presence/dominance logic is an order of
   magnitude larger; the same choice would not be safe here.
4. **Going the other direction (teach the config generator nothing, keep everything in
   `generate_name_key_patterns.py`)** would also require duplicating the identity-items
   lookup and label-resolution fallback machinery (§2) to get `pattern_label_human`/
   `semantic_group` parity — again logic that already exists once in `extractor.py`.

This means Step 0's honest answer to "does unification require refactoring the config
generator" is **yes** — closing the presence/dominance gap (item 3/9 above) is the load-bearing
piece of work, and it lives in `tools/extractor.py`, not `tools/generate_name_key_patterns.py`.
The column-renaming/constant-filling half of the adapter (items 1, 2, 4, 11, 12) is
comparatively cheap and can ride along with that refactor rather than needing its own separate
effort.

## 5. Q2 — What `compare_cross_segment.py` needs

Read the actual pair-discovery/aggregation read surface directly (not inferred from the
adapter's target shape). It is **substantially lighter than bundle_analysis's**:

- `resolve_join_hashes()` / `load_pattern_labels()` read `domain_patterns.csv` via
  `row.get()` and use exactly 4 columns: `domain`, `pattern_id`, `source_cluster_id` (split on
  `|`, take the last segment as the join_hash), and `pattern_label_human`/`pattern_label` for
  display. No `analysis_run_id`, `schema_version`, `is_cad_import`, `semantic_group`, or
  `is_candidate_standard` is ever read at this layer.
- `load_file_join_hashes()` prefers `bundle_analysis`'s `membership_matrix.csv` when present,
  but for Generic/reference/analysis-only segments falls back to `pattern_presence_file.csv`
  reading exactly `domain`, `export_run_id`, `pattern_id` — again no share/dominance/deviation
  columns.
- `discover_domains_for_segment()` falls back to scanning `domain_patterns.csv` /
  `pattern_presence_file.csv` for distinct `domain` values only.
- **No `resolve_analysis_run_id()`-style single-value enforcement anywhere in
  `compare_cross_segment.py`** — that constraint (audit_8 item 1) is specific to
  `bundle_analysis/common.py`, not this layer.

So `compare_cross_segment.py` itself does not need the full production column set — it needs:
(a) a `comparison_target`-aware path resolver analogous to `domain_patterns_path()`/
`pattern_presence_file_path()` (today hardcoded to `results/analysis/`; would need to select
`results/analysis/` vs. a name-projection-equivalent root per segment, per target), and
(b) whatever minimal columns are listed above to exist under that path, in either the exact
production shape (once unified per Q1) or any shape a thin, target-specific reader translates
into those same four/three fields. Full production-schema parity is what *bundle_analysis*
demands (via its unmodified step0–step7 files), not what compare_cross_segment demands — this
is a materially different bar than the brief's framing might suggest, and matters for scoping
how much of the Q1 refactor `compare_cross_segment.py` actually needs before it can pick up
`comparison_target=name` output. Separately, and per this Step 0's explicit brief, this is
lighter than today's cross_segment ask because it becomes a `comparison_target` selector on
existing plumbing rather than new machinery, once a unified patterns writer exists at all.

## 6. Q3 — What `generate_governance_narrative.py` needs

The brief's framing ("consistent with how `comparison_type`/`comparison_mode` already work
there") needs one correction: **`comparison_mode` does not exist as a field or CLI concept
anywhere in `generate_governance_narrative.py` or `compare_cross_segment.py`.** The actual
existing analogous slicer is **`view_scope`** (`"all"` vs `"used"`, i.e. `--purge-view`),
which appears as a column on `compare_cross_segment.py`'s union-inventory and matrix outputs
and is read directly by narrative-section builders (e.g. `_unordered_project_pairs(...,
view_scope="all", ...)`, the fragmentation-diagnostic and matrix sections all filter on
`row.get("view_scope")`). `comparison_type` (`within_project`, `sibling_projects`,
`cross_client`, `governance_chain`, etc.) is the other real slicer, enforced by
`_warn_unrecognized_comparison_types()`'s coverage-checking convention so a drifted/unhandled
value is never silently swallowed.

`comparison_target` would need to follow the same convention as `view_scope`, not invent a
new mechanism:

1. `compare_cross_segment.py` needs to stamp `comparison_target` (`"config"`/`"name"`) onto
   every row it emits into its union-inventory / summary / matrix CSVs — the same way
   `view_scope` is already carried as a column, not derived implicitly from which CLI flags
   were passed.
2. `generate_governance_narrative.py`'s section builders that currently hard-filter on
   `view_scope == "all"` (or don't filter at all, implicitly assuming a single target) would
   need an equivalent `comparison_target` filter/parameter wherever a section is meant to be
   target-specific, plus a coverage-check analogous to `_warn_unrecognized_comparison_types()`
   so a report accidentally mixing config- and name-projection rows in one aggregate is never
   silent.
3. Given the semantic gaps confirmed in §3 (USED view, share-profile pre-unification,
   `--compare`/reference-baseline), any narrative section whose underlying metric depends on
   one of those blocked capabilities needs to either be explicitly `config`-only, or carry a
   provenance caveat analogous to `name_projection_adapter.py`'s
   `PROVENANCE_NOTE_NAME_TARGET`/`README.md` pattern — that provenance-declaration convention
   is worth keeping even after the adapter itself is retired, since the underlying semantic
   gaps (§3 items 5/7/8/10) don't go away just because the shape gaps do.

## 7. Q4 — Zero behavioral change to `comparison_target=config`

PR1–PR4 all held byte-identical `config`-target output as a hard requirement (PR2's own
docstring: "treats the already-produced `Results_v21/analysis_v21/domain_patterns.csv` as the
source of truth... never recomputes or overwrites the authoritative file"). If unification
proceeds via the Q1-recommended direction (refactor `_process_one_domain`/`emit_analysis` to
be `comparison_target`-parameterized), the config path becomes **the same code, called with
`comparison_target="config"` instead of the only caller it has today** — the risk profile
shifts from "does a copy step stay byte-identical" (PR2's current, easy guarantee) to "does a
refactored function produce byte-identical output to the pre-refactor function" (a real
regression risk, since the function is being restructured, not just given a new caller).
This makes a **golden-file byte-identical regression test for `comparison_target=config`
output a hard prerequisite of any Q1 refactor**, not an optional nice-to-have — there is
currently no such test (only PR2's copy-and-verify manual precedent). Confirming zero
behavioral change stops being "read the diff and confirm no computation changed" and becomes
"run the refactored function against a real corpus and diff every output CSV byte-for-byte
against pre-refactor output," which is a meaningfully larger verification burden than PR1–PR4
faced and should be budgeted as such in any implementation plan.

## 8. Scope assessment (informational — not a recommendation to implement)

The two halves of this unification have different risk profiles and could reasonably be
staged as separate PRs:

- **Cheap, low-risk half**: column-set/naming parity for `domain_patterns.csv`'s constant and
  already-optional-with-fallback fields (audit_8 items 1, 2, 4 minus `is_cad_import`, 11, 12).
  This is close to what the adapter already does today, just moved into
  `generate_name_key_patterns.py` natively instead of a post-hoc normalization step.
- **Expensive, higher-risk half**: porting/sharing `_process_one_domain`'s presence/dominance/
  deviation/corpus-classification computation (item 3, and by extension item 9) so
  `pattern_presence_file.csv` exists natively for `name`-target with real, not placeholder,
  values — this is the piece that requires refactoring `tools/extractor.py` itself and
  carries the Q4 regression-test burden.

Splitting these lets the cheap half land (and retire most of `name_projection_adapter.py`'s
constant-filling code) without gating on the `extractor.py` refactor, while the presence/
dominance work — the part that actually changes what `comparison_target=name` output *means*
(real corpus-standard classification instead of always-empty placeholders) — gets its own
review cycle with the golden-file test as an explicit acceptance gate. Whether that split is
worth it in practice depends on how disruptive touching `_process_one_domain` turns out to be
in a real diff, which is exactly the kind of thing an implementation-planning pass (out of
scope for this Step 0) should assess directly against the current `tools/extractor.py`
source rather than estimate here.
