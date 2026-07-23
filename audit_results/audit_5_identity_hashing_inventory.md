# Audit 5 — Step 0 Inventory: Existing Identity, Hashing, and Parameterization Machinery
Date: 2026-07-23
Scope: Read-only investigation for the name-identity-projection work (canonical name key, identity-basis parameterization, correspondence reporting). No code changes made. Every claim below cites a file/line actually read during this session.

## Summary Table

| # | Area | Existing mechanism | Reuse verdict |
|---|------|---------------------|---------------|
| 1a | Hash primitive | `core/hashing.make_hash()` + `core/record_v2.serialize_identity_items()` | Reuse as-is |
| 1b | Canonicalization | `core/record_v2.canonicalize_*()`, `core/canon.canon_*()` | Reuse as-is (no case-fold/unicode/UID normalization exists anywhere — would be new if needed) |
| 1c | `identity_basis.items` | `core/record_v2.build_record_v2()` | Reuse as-is (contractual; do not rename/duplicate) |
| 1d | `discover_hash_policy.py` promotion ordering | No literal "promotion" gate exists | Prior-planning framing does not match code; restate |
| 2 | Blank/NA check | `tools/na_token.is_blank_or_na()` | Reuse as-is |
| 3 | Mode/basis CLI precedent | `--policy-modes`, `--discovery-target`, `--search-modes` | Naming convention reusable; no `--basis`/`--identity-basis` flag exists anywhere today |
| 4 | Output namespacing | `Results_v21/{phase0_v21,analysis_v21,placeholder_exclusions,label_synthesis}/` | No `analysis/` root exists; new trees don't collide, but should nest under `Results_v21/` for consistency |
| 5 | `population_hash` inputs | `tools/build_segment_manifest.py` / `tools/governance_manifest.py` `_population_hash()` | New CSV fields are outside the hash *unless* added as a segmentation dimension |
| 6 | Threshold precedent | `tools/jenks_utils.jenks_breaks()` | Reusable only for continuous metrics, not categorical eligibility |

---

## 1. Identity/hash construction

### 1.1 The actual hash primitive (shared by sig_hash and join_hash)

`core/hashing.py`:
- `safe_str(x)` (line 14): `str(x)`, with an `unicode(x)` fallback for IronPython, else `"<unrepr>"`. **Does no normalization** — no trim, no case-folding, no unicode normalization (NFC/NFKC).
- `make_hash(values)` (line 29) / `_make_hash_impl` (line 62): streaming MD5 over `"|".join(safe_str(v) for v in values)`, dual runtime (CLR `System.Security.Cryptography.MD5` for Revit/pythonnet, `hashlib.md5` fallback for CPython). Timing-instrumented only; hash output is unaffected by instrumentation.

`core/record_v2.py`:
- `serialize_identity_items(items)` (line 455): **the single authoritative preimage builder**. Always sorts items by key `k` (lexicographic), emits `"k={k}|q={q}|v={v_or_empty}"` strings. This is what both sig_hash and join_hash preimages are built from.
- Canonicalization helpers `canonicalize_str` / `canonicalize_str_allow_empty` / `canonicalize_int` / `canonicalize_float` / `canonicalize_bool` / `canonicalize_enum` (lines 83–261): all `None -> missing`, conversion-exception `-> unreadable`, `strip()` whitespace, empty-after-strip `-> missing` (except the `_allow_empty` variant, which keeps `""` as a valid `ok` value). No case-folding, no unicode normalization, no UID-specific handling anywhere in this file.

`core/canon.py` (a parallel, older canonicalization layer that emits sentinel *strings* rather than `(v,q)` tuples): `canon_str/_bool/_num/_id` (lines 41–139) — same strip/empty-to-sentinel rules, plus legacy-token folding (`"<None>" -> S_MISSING`, `"<Unreadable>" -> S_UNREADABLE`). Also no case-folding/unicode normalization.

**No UID normalization function exists.** UniqueId strings are canonicalized exactly like any other identity string (trim + empty-check via `canonicalize_str`). The only UID-specific logic in the codebase is `core/record_v2.make_record_id_from_element()` (line 268) — this builds `record_id` (`"uid:<UniqueId>"` else `"eid:<int>"`), which is **record identity**, not hash-preimage identity, and is out of scope for a name-identity hash.

### 1.2 sig_hash — two distinct code paths

**(a) Inline, at export time** — every domain computes its own sig_hash today. Pattern confirmed in `domains/units.py:284-305`: the domain filters its own `items_sorted` down to a hardcoded "semantic" key subset (e.g. `UNITS_SEMANTIC_KEYS`), then `preimage = serialize_identity_items(semantic_items); sig_hash = make_hash(preimage)`. This selection is domain-hardcoded Python, not policy-driven.

**(b) Policy-driven reconstruction, analysis-side only** — `core/sig_hash_builder.build_sig_hash_from_policy()` (line 38): given a domain's `policies/domain_sig_hash_policies.json` entry (`allowed_items`/`allowed_item_prefixes`/`required_items`/`minima`), filters a record's flat `items` to `hash_items`, then `preimage = serialize_identity_items(hash_items); sig_hash = make_hash(preimage)`. This runs only in `tools/run_extract_all.py`'s `sig_hash` stage (T0.5) over **flattened CSV rows** — confirmed by `core/sig_hash_policy.py` docstring ("mirrors the join-key policy pattern") and `tools/generate_sig_hash_policy.py`'s note `"sig_hash is computed post-extraction from canonical identity_basis.items."` **Not wired into the live Dynamo extraction path** (also stated explicitly in CLAUDE.md's Warnings section).

### 1.3 join_hash — one code path, invoked at export time

`core/join_key_builder.build_join_key_from_policy()` (line 194) is called directly from domains at export time (e.g. `domains/units.py:324`, via `ctx["join_key_policies"]` — so, unlike sig_hash_builder, **this runs inside the Dynamo runner path**, not only in analysis). It selects `required_items + optional_items` (with shape-gating support) from the same `items_sorted`/`identity_items` source, then computes:
```
join_hash = phase2_join_hash(hash_items)   # = make_hash(serialize_identity_items(sorted(hash_items)))
```
**except** a structured-domain passthrough rule (line 262): if exactly one hashed item is a key ending in `_def_hash` whose value is already a 32-hex MD5 string, that value **is** the join_hash verbatim (no re-hash). `core/phase2.phase2_join_hash()` (`core/phase2.py:65`) is the shared low-level function — itself just `serialize_identity_items` + `make_hash` again, defensively re-sorted.

**Conclusion: sig_hash and join_hash are the same underlying mechanism** (`serialize_identity_items` + `make_hash`, MD5, pipe-delimited preimage, sorted by key) applied to two independently-selected item subsets. A new name-identity hash should reuse this mechanism directly — do not invent a new hash primitive.

### 1.4 `discover_hash_policy.py` — what it does, who populates/reads `identity_basis.items`, and the claimed ordering dependency

Read in full (`tools/discover_hash_policy.py`, 283 lines). It is a CLI tool that:
- Resolves a phase0 directory (`_resolve_phase0_dir`, line 79) across several known root shapes (direct `records.csv`, `<root>/records/records.csv`, `<root>/results/records/records.csv`, `<root>/phase0_v21/records.csv`).
- Reads flattened `records.csv` plus per-target item CSVs: `sig` prefers `signature_items.csv` → falls back to `identity_items.csv` → `phase0_identity_items.csv`; `join` prefers `join_items.csv` with the same fallback chain (line 15, `TARGET_FILES`).
- Optionally trusts sharded per-domain CSVs under `identity_items_by_domain/<domain>.csv`, but **only** if a `.complete` sentinel file exists next to them (line 138) — explicitly to avoid silently treating a partial/interrupted flatten as authoritative.
- Runs the same greedy/pareto candidate search shared with join-key discovery (`tools/join_key_discovery/{eval,greedy}.py`) across `--policy-modes` (`discover`/`validate`/`harsh`) × `--search-modes` (`greedy`/`pareto`) × `--discovery-target` (`sig`/`join`/`both`), per domain and (for `loaded_family_types`) per shape-gate category.
- Writes diagnostics CSVs (`hash_sig_discovery_exploration.csv`, `hash_join_discovery_exploration.csv`) and an optional `--out-policy` candidate JSON explicitly stamped `"governance_status": "discovered_candidate_not_governed"` (line 276) — i.e. its output is never itself a governed policy.

**Who populates `identity_basis.items`:** every domain's `extract()`, via `core/record_v2.build_record_v2(identity_items=...)`, which stores it at `record["identity_basis"]["items"]` (`core/record_v2.py:589-593`).

**Who reads it:** (1) the flatten stage (`tools/run_extract_all.py` T0 / `tools/export_to_flat_tables.py`), which is what actually produces the `identity_items.csv`/`signature_items.csv`/`join_items.csv` that `discover_hash_policy.py` reads — **`discover_hash_policy.py` never reads `identity_basis.items` from export JSON directly, only the flattened CSV form**; (2) `core/sig_hash_builder.py` (analysis-side sig_hash reconstruction); (3) `core/canonical_items.canonicalize_record()` (migration path, merges `identity_basis.items` with the phase2 buckets into one flat list — see §1.5).

**The "promotion-must-precede-discovery" ordering claim:** I searched `tools/discover_hash_policy.py`, `tools/generate_sig_hash_policy.py`, `core/sig_hash_policy.py`, `docs/hash_discovery_tooling.md`, and all `.md` files in the repo for the word "promotion" — **zero hits tied to this tool**. No code enforces or documents an ordering dependency by that name. The two real, confirmed ordering dependencies in this area are:
1. `tools/run_extract_all.py`'s stage machine (`stage_names`, line 707): `flatten -> sig_hash -> discover -> apply -> placeholders -> authority -> patterns -> split -> flat_tables`. The `sig_hash` stage (T0.5, policy-driven recompute) is documented to run after `flatten` and before `discover` (CLAUDE.md, `docs/extract_stage_matrix.md`). This is a different `discover` than `discover_hash_policy.py` — the pipeline's `discover` stage runs `tools/discover_join_policy.py`, not `discover_hash_policy.py`, which is a standalone/manual tool not wired into `--stages` at all.
2. `tools/generate_sig_hash_policy.py` compiles `contracts/domain_identity_keys_v2.json` (human-governed registry) into `policies/domain_sig_hash_policies.json` (line 58-63). If this compile step is **not** re-run after editing the registry, `discover_hash_policy.py --policy-modes validate|harsh --policy-json policies/domain_sig_hash_policies.json` will silently constrain against a stale baseline rather than erroring — this is the closest real analog to "promotion must precede discovery," but it is not asserted or tested anywhere (confirmed via `tests/test_discover_hash_policy.py`, read in full — no test exercises this).

**Recommendation:** drop the "promotion-must-precede-discovery" framing as stated (it doesn't match anything in code) and instead scope PR1 against the two concrete dependencies above if this ordering matters to the new work.

### 1.5 Full `identity_basis` / phase2 / join_key relationship

- **`identity_basis.items`** (`core/record_v2.build_record_v2`, line 589): the canonical, complete evidence superset for a record — every collected `IdentityItem {k,v,q}`, sorted by `k`. This is the **only** one of these structures documented in the formal contract (`contracts/record_contract_v2.md` — confirmed via grep: `identity_basis` appears at lines 31/45/95/109; `semantic_items`/`cosmetic_items`/`coordination_items`/`unknown_items`/`join_key` do **not** appear anywhere in that file).
- **`phase2.{cosmetic_items,coordination_items,unknown_items}`** (and, in some domains, `semantic_items`): an additive, non-contract categorization layer (`core/phase2.py`). Each is a **re-filtered subset** of the exact same `items_sorted` list that feeds `identity_basis.items` — domains build these by filtering on hardcoded key-set membership (`domains/units.py:343-350`: `cosmetic_keys`/`semantic_keys` sets, with `unknown_items` as the complement). **Not every domain emits an explicit `semantic_items` key** — confirmed by grep: only `domains/identity.py`, `phase_graphics.py`, `text_types.py`, and `fill_patterns.py` literally write `"semantic_items"` into `rec["phase2"]`; `units.py`, `dimension_types.py`, and others omit it, leaving "semantic" implicit as `identity_basis.items` minus (cosmetic ∪ coordination ∪ unknown). CLAUDE.md's "four buckets" table describes the intent correctly but is not a literal per-domain JSON-shape guarantee.
- **`join_key.items`** (`core/join_key_builder.build_join_key_from_policy`, `emit_items=True`): a **third, independently-selected** subset of the same underlying items, chosen by `policies/domain_join_key_policies.json`'s `required_items`+`optional_items` (with shape-gating) — used to compute `join_hash` for cross-file matching. Distinct selection logic from both the phase2 buckets and the domain's own inline sig_hash selection.
- **`core/canonical_items.py`** (migration path, pilot domain `text_types.py`) makes this relationship explicit in code: `merge_legacy_buckets()`/`canonicalize_record()` (lines 58-151) literally merge `identity_basis.items` + `phase2.{semantic,lineage,cosmetic,coordination,unknown}_items` into **one flat `items` list** (first-seen-key wins), then `canonicalize_record()` **deletes** `identity_basis`, `phase2`, `join_key`, `sig_hash`, `sig_basis` from the output entirely. The migration endpoint is: all of the above collapse into a single flat `items:[{k,v,q}]` array, with semantic/cosmetic/coordination/unknown role resolved at runtime from policy (`compile_role_policy`/`resolve_item_roles`) rather than baked into the JSON shape.

**Verdict:** `identity_basis.items`, the phase2 buckets, and `join_key.items` are **not three separate data sources** — they are three different projections/categorizations of the same underlying per-record item list. Any new identity-basis-like structure for name-projection work must either (a) plug into this existing flat-items + policy-categorization machinery, or (b) if genuinely novel, must not be named `identity_basis` — that name is already contractual at the record level.

### 1.6 Per-domain schema inconsistency

Of the domain files, only 11 (`object_styles.py`, `phase_filters.py`, `phases.py`, `text_types.py`, `units.py`, `view_filter_applications_view_templates.py`, `view_filter_definitions.py`, `identity.py`, `line_patterns.py`, `line_styles.py`, `fill_patterns.py`) directly reference `identity_basis` by that literal grep term in a build-record context, while a broader set of 17 emit the phase2 bucket keys. All confirmed emitters route through `build_record_v2`, so there is no domain silently missing `identity_basis` and falling back to legacy fields — the apparent gap is just that some domain files' `identity_basis` usage occurs inside shared helper calls the grep didn't directly match, not an actual contract gap. Any new identity-projection work reading `identity_basis.items` across domains should be robust to the confirmed variance in whether `phase2.semantic_items` is explicitly present (§1.5) rather than assuming its presence.

---

## 2. Status / blank / missing handling

`tools/na_token.py` (34 lines, read in full):
- `is_na_token(value: str) -> bool` (line 17): true for any spelling of "not applicable" (`na`, `n/a`, `N/A`, `not applicable`, `not_applicable`, `__NOT_APPLICABLE__`, ...) via `_NA_TOKEN_STRIP_RE.sub("", value.lower()) in {"na","notapplicable"}`. Deliberately does **not** treat blank as NA.
- `is_blank_or_na(value: str) -> bool` (line 26): `not value.strip() or is_na_token(value.strip())` — true for blank (not-yet-filled-in) OR any NA spelling. The docstring is explicit that blank and NA carry different meaning for manual-entry QA (todo vs. reviewed) and that callers needing that distinction should check them separately.

**Call sites** (all confirmed via grep): `tools/governance_relationships.py:107`; `tools/compare_cross_segment.py` — 12 call sites (lines 1182, 1857, 1877, 2226, 2229-2230, 2456, 2462, 2488, 2510, 2739, 2885, 2893), all folding `client_label`/`business_center_label`/`collection_label`/`project_label` for cross-segment comparison (Mode D/E normalization); `tests/test_na_token.py` (direct unit tests).

**Reusability:** `is_blank_or_na()` is a generic string predicate with **no coupling** to the `project_label`/segmentation use case it was introduced alongside — the module-level `ENTERPRISE_BC_BOOKKEEPING_TOKENS` set is the domain-specific part of `na_token.py`, not this function. **Directly reusable as-is** for a new name-eligibility/status check. If the new work needs to distinguish "blank" (todo) from "explicit N/A" (reviewed) rather than collapse them — which a name-eligibility field plausibly does need — use `is_na_token()` plus a separate blank check, exactly as the function's own docstring recommends, rather than reusing `is_blank_or_na()`'s collapsed semantics.

---

## 3. Basis / mode parameterization precedent

Confirmed CLI flags across `tools/` (grepped for `--mode`/`--*-modes`/`--basis`/`--identity-basis`):

| Flag | Location | Values | Meaning |
|------|----------|--------|---------|
| `--policy-modes` | `discover_hash_policy.py:238`, `discover_join_policy.py:105` | `discover,validate,harsh` | Candidate-search strictness: unconstrained / required+optional-only / required+optional+discovered |
| `--discovery-target` | `discover_hash_policy.py:235` | `join`, `sig`, `both` | **Closest existing analog to "which identity/hash to run analysis against"** — literally switches the tool between sig_hash-candidate world and join_hash-candidate world |
| `--search-modes` | `discover_hash_policy.py:236`, `discover_join_policy.py:104` | `greedy,pareto` | Search algorithm selection |
| `--mode` | `extract_segment_subtree.py:546` | `detail,summary,both` | Output granularity, unrelated domain |
| `--mode` | `tools/_archive/join_key_derivation_phase05.py:529` (archived, superseded) | `validate,harsh` | Predates `--policy-modes`, same strictness concept |

**No flag named `--basis` or `--identity-basis` exists anywhere in `tools/`** (confirmed empty grep across the whole tree).

**Naming convention observed:** multi-word flags are hyphenated; plural when the value is a comma-separated list (`--search-modes`, `--policy-modes`, `--domains`), singular when the value is a single scalar/choice (`--discovery-target`, `--mode`).

**Verdict:** no direct code collision exists for `--identity-basis` as a flag name, but it would be read by anyone familiar with this codebase as referring to the contractual `identity_basis` record field (§1.5), which is not what a mode-selecting flag would mean. Recommend renaming; something in the `--discovery-target` family (purpose-scoped, not reusing the contractual noun) fits the established convention better.

---

## 4. Output namespacing precedent

There is **no committed or hardcoded top-level `analysis/` directory anywhere in this repo** (confirmed via `find . -maxdepth 2 -iname '*analysis*'` — only doc files and `tools/patterns_analysis/`, `tools/bundle_analysis/`, `tools/pairwise_analysis.py` matched, none of which is an output root).

The one real, fixed-path convention is produced by `tools/run_extract_all.py` under `Results_v21/`:
- `phase0_v21/` — flatten output (T0)
- `analysis_v21/` — documented in full in `docs/V21_ANALYSIS_SCHEMA.md` (`analysis_manifest.csv`, `domain_patterns.csv`, `phase1_domain_metrics.csv`, `element_dominance.csv`, etc.)
- `placeholder_exclusions/` — T2b output
- `label_synthesis/` — label-semantic-group cache

Everything downstream of `run_extract_all.py` — `tools/build_segment_manifest.py` (`--out-dir`, required), `tools/governance_manifest.py` (`--out-dir`, defaults to `--records-dir`), `tools/run_segment_orchestrator.py` (`--repo-root` + a per-segment `output_folder` **column value**, not a hardcoded literal), `tools/compare_cross_segment.py`/`compare_governance_populations.py` — all take an **operator-supplied** output directory via CLI flag rather than writing to any fixed repo-relative path.

**Verdict:** a new output tree named `analysis/name_key/` or `analysis/identity_crosswalk/` would not collide with any existing hardcoded path, because no `analysis/` root exists at all today — the closest similarly-named thing is `Results_v21/analysis_v21/`, a different, already-populated path, and `docs/analysis-phases-question-map.md`/`docs/V21_ANALYSIS_SCHEMA.md`, which use "analysis" to mean the analysis *side of the codebase* generally, not a directory. **Recommend nesting under `Results_v21/`** (e.g. `Results_v21/name_key/`) for consistency with the one real fixed-path convention that exists, rather than introducing an unprecedented `analysis/` root.

---

## 5. `population_hash` inputs

Two implementations, both read in full and identical in formula:
- `tools/build_segment_manifest.py:58-59`: `_population_hash(export_run_ids) = sha1("|".join(sorted(export_run_ids)))`, called at line 377 with the segment's member `export_run_id` list.
- `tools/governance_manifest.py:154-156`: identical formula, called at line 294 with the governance population's member `export_run_id` list.

**Exactly what feeds it:** only the sorted list of `export_run_id` strings — i.e., *which* exports/files belong to the segment or population. **It does not hash any field value or column content from `file_metadata.csv`** beyond the identity of member export_run_ids.

**Direct answer:** adding new fields/columns to `file_metadata.csv` (e.g. a new name-key column) is **outside** `population_hash`'s computation — the hash is a pure function of segment/population *membership*, not of any row's field values. **Caveat:** if the new field were also added to `DIMENSION_CONFIG` (`tools/build_segment_manifest.py:35-41`) as a new segmentation dimension, that would change how `export_run_ids` are partitioned into segments in the first place, changing membership for existing segments, which would then change `population_hash` indirectly. `tools/run_segment_orchestrator.py:808-809` explicitly detects and reports this as a mismatch (`"segment_membership.csv population_hash=... does not match segment_manifest.csv population_hash=..."`), forcing reprocessing of every affected segment. **Recommendation:** any new name-key field added to `file_metadata.csv` should explicitly **not** be added to `DIMENSION_CONFIG` unless deliberate re-segmentation/reprocessing across the whole lattice is intended.

---

## 6. Threshold/derivation precedent

`tools/jenks_utils.py` — `jenks_breaks(values, n_classes=2)`: pure-Python Fisher-Jenks natural-breaks, no external dependency (consistent with the stdlib-only-tools rule in CLAUDE.md), handles empty/constant-value lists gracefully.

**Consumers** (confirmed via grep): `tools/compute_governance_thresholds.py`, `tools/emit_element_dominance.py` (feeds `element_characterization_thresholds.csv` per `docs/V21_ANALYSIS_SCHEMA.md` — Jenks breaks over `dominant_presence_pct`), `tools/bundle_analysis/step2_find_bundles.py`, `tools/archetype/cluster_archetype_signals.py`, `tools/analyze_promotion_candidates.py`, `tests/test_split_named_clusters_and_thresholds.py`.

This is the established "data-derived, not hardcoded" threshold pattern in this codebase, used for genuinely continuous metrics (presence percentages, cluster distances, governance scores).

**Reusability for this work:** reusable **only if** the next prompt defines a genuinely continuous per-domain metric (e.g. a coverage/uniqueness score deciding what counts as a "meaningful name field"). It is **not** a fit for a binary/categorical decision (e.g. "does this domain have a name field", "is this string a name vs. a code") — Jenks breaks split continuous distributions into natural classes; they don't replace a categorical eligibility rule. For categorical policy decisions, the established pattern in this codebase is a governed allow-list (`domain_join_key_policies.json`/`domain_sig_hash_policies.json` precedent), not Jenks.

---

## Proposed new terms from prior planning drafts — recommendations

| Proposed term | Finding | Recommendation |
|---|---|---|
| `name_hash` / `name_join_key.v1` / `name_key` | Three spellings for one concept. `join_hash`/`sig_hash`/`population_hash`/`phase2_join_hash` are all literal existing hash-value field names; `join_key_schema` follows a `"<domain>.join_key.v<N>"` naming pattern (e.g. `"dimension_types.join_key.v3"`, `core/join_key_builder.py` docstring). | Pick **one** spelling, and let it be determined by which mechanism it actually reuses: if built via `build_join_key_from_policy`, its output is a `join_hash` under a new `join_key_schema` string (e.g. `"name_identity.join_key.v1"`) — don't also mint a parallel `name_hash`/`name_key` field for the same value. If it's a genuinely new kind of value outside the join_hash/sig_hash system, it must not reuse bare "hash"/"key" vocabulary that implies it slots into `join_key_builder.py`/`sig_hash_builder.py` when it doesn't. |
| `--identity-basis` (CLI flag) | Collides in vocabulary (not in code) with the contractual `identity_basis` record field (§1.5, §3). | Rename. |
| `display_name` | No literal field named `display_name` exists, but `label.display` (record.v2 `label` dict — e.g. `domains/units.py:277-282`) and the helper functions `get_element_display_name()`/`get_type_display_name()` (`core/rows.py:259,301`) already occupy this exact vocabulary for a related-but-distinct concept. | Soft collision — rename to avoid confusion, or explicitly document how it differs from `label.display` if kept. |
| `name_status` | No bare `name_status` field exists, but `selected_file_name_status` (`tools/archetype/prepare_archetype_review.py:152` — an enrichment-failure-reason field for the archetype DP1/DP2 review workflow, values like `not_in_review_sample`, `missing_sig_hash`) already establishes a `*_name_status` vocabulary meaning "why we couldn't resolve a name," in an unrelated subsystem. | Soft collision only (different domain, no direct code interaction) — safe to reuse the term as long as the new field's value vocabulary doesn't need to be distinguished from the archetype one in shared tooling/docs. |
| `analysis/name_key/`, `analysis/identity_crosswalk/` | No `analysis/` root exists anywhere in the repo (§4). | Confirmed novel as paths, but recommend nesting under `Results_v21/` instead, to follow the one real existing fixed-path convention rather than introducing an unprecedented root. |
