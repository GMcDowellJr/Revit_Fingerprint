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

---

# Addendum — join_hash Construction and Label Overlap

Date: 2026-07-23 (follow-up session). Read-only; no code changes. Confirms/corrects the working model that `join_hash` is a downstream, policy-layer key computed from contract-governed records, and checks whether `label.*` fields feed it for any domain.

## A.1 Where `join_hash` is actually computed — two paths, and a bootstrap step that discards the first one

**Path 1 — inline, at export time.** `core/join_key_builder.build_join_key_from_policy()` is called directly inside 16 domain files (confirmed via grep for the call): `arrowheads.py`, `dimension_types.py`, `fill_patterns.py`, `identity.py`, `line_patterns.py`, `line_styles.py`, `object_styles.py`, `phase_filters.py`, `phases.py`, `text_types.py`, `units.py`, `view_category_overrides_annotation.py`, `view_category_overrides_model.py`, `view_filter_applications_view_templates.py`, `view_filter_definitions.py`, `view_templates.py`. It reads the domain's own `identity_items`/`identity_basis.items`, filters to the policy's `required_items` (+shape-gated additions) and, only if `hash_optional_items` is left at its default `True`, also `optional_items`, then hashes via `core/phase2.phase2_join_hash()` (itself `serialize_identity_items` + `make_hash`). This produces `rec["join_key"]["join_hash"]` in the raw export JSON.

**Five domain files never call it at all**: `materials.py`, `compound_types.py` (wall/floor/roof/ceiling types), `loaded_family_types.py`, `phase_graphics.py` (disabled per D-013 anyway), and the `view_category_overrides.py` coordinator file (its `_model`/`_annotation` partitions do call it — only the coordinator itself doesn't). For these, no `join_key` field exists in the raw export JSON at all; confirmed for `materials.py` by grep — the file computes `graphics_sig_hash_v2` (`domains/materials.py:449`) and an internal `material_sig_hash = make_hash([name, class])` (`materials.py:452`, a helper aligned with `obj_style.material_sig_hash`, not the record's own join_hash) but never constructs `join_key`/`join_hash`.

**Path 2 — the flatten stage bootstraps over Path 1 entirely.** `tools/extractor.py:1122-1149` (the flatten writer) does **not** read `rec["join_key"]["join_hash"]` from the export JSON at all, for any domain — it explicitly writes, per its own comment ("Day-1 identity-mode flatten join regime"): `join_hash = sig_hash`, `join_key_schema = "sig_hash_as_join_key.v1"`, `join_key_status = "bootstrap"` (line 1138-1140), unconditionally. This matches `docs/extract_stage_matrix.md`'s description of `flatten` (T0) as emitting "identity-mode join fields" by design. So immediately after flatten, every domain's `join_hash` column in `records.csv` is a straight copy of `sig_hash`, regardless of whether Path 1 computed something different inline.

**The real, governance-relevant `join_hash`** only exists after the `apply` stage (T2) runs: `tools/apply_join_policy.py` reads `identity_items.csv` fresh, applies a join-key policy (default `results/policies/domain_join_key_policies.v21.json`, a generated/discovered file not present in this checked-out tree — falls back to the committed `required_items`/`optional_items` shape via `normalize_policy_block()`'s legacy-alias handling, `tools/join_key_discovery/eval.py:68-92`), and **overwrites** `records.csv`'s `join_hash`/`join_key_schema`/`join_key_status` columns in place (`apply_join_policy.py:189-191`).

**Important correction to the working model's implicit assumption:** at the `apply` stage, `tools/join_key_discovery/eval.py:build_candidate_join_key_with_details()` (lines 95-140) computes `base_required = gates.get("required_fields") or selected_fields` (line 105) — since `apply_join_policy.py` always passes `required_fields` explicitly inside `gates` (line 159 of `apply_join_policy.py`), the `selected_fields` param (which would come from a policy's `optional_items` if present) is **never reached**; only `required_items` (+ shape-gated `additional_required`) are ever hashed at this stage. This is confirmed textually by the `materials` policy's own note: `"join_hash basis: md5(material.graphics_sig_hash_v2 | material.keynote)"` — the two *required* items only; `material.class`/`material.name_class_hash`/`material.manufacturer`/`material.model`, though listed as `optional_items`, are **not** part of the actual `join_hash` preimage once policy is applied. **Conclusion: for the purposes of "what feeds `join_hash`," only each domain's `required_items` (+ shape-gated additions) matter — `optional_items` are candidate/diagnostic-only at the apply stage that governs `records.csv`.**

## A.2 Per-domain table — does `join_hash` (required_items) incorporate any `label.*` field?

All required-items lists below are read directly from `policies/domain_join_key_policies.json` (37 domain entries, full file parsed). "Label source" is cross-checked against each domain's own extractor code where relevant.

| Domain | `required_items` (what actually feeds `join_hash`) | Label/name in `join_hash`? | Detail |
|---|---|---|---|
| **phases** | `phase.name` | **YES — entangled** | `domains/phases.py:177` reads `name = canon_str(p.Name)`; this exact value backs both `make_identity_item("phase.name", ...)` (line 230, → `join_hash`) **and** `label={"display": safe_str(name), ...}` (line 262). Normalization: `canon_str` (strip, sentinel-on-empty) then `canonicalize_str` (strip again, missing-check) — trim only, no case-fold/unicode normalization. Consistent with D-010 (phase names are deliberately included in behavioral hashes for cross-project comparability) — evidently `join_hash` follows the same rule. |
| identity | `identity.is_workshared` (+ `identity.revit_version_number` when workshared) | No | `identity.filename`, `identity.project_title`, `identity.central_path`, `identity.central_path_norm` are explicitly in `explicitly_excluded_items`. `identity.revit_version_name` (a Revit build label like "Revit 2024", not a project name) is optional-only, so per §A.1 it's not hashed anyway. |
| materials | `material.graphics_sig_hash_v2`, `material.keynote` | No | `material.name` is explicitly excluded; `material.name_class_hash` is optional-only (not hashed, per §A.1). `graphics_sig_hash_v2` is a sub-hash over graphics/render properties (`domains/materials.py:449`); `keynote` is a code/tag field, not the material's display name. |
| dimension_types (all 7 partitions) | e.g. linear/angular: `dim_type.shape,.accuracy,.tick_mark_sig_hash,.witness_line_control,.unit_format_id,.rounding,.prefix,.suffix`; spot_coordinate/spot_elevation additionally require `dim_type.symbol_name` | No (own name); partial exception | `dim_type.name` (the dimension type's own display name, feeding its `label.display` via `get_type_display_name()`) is explicitly excluded in all 7 partitions. `dim_type.symbol_name` (required for spot_coordinate/spot_elevation) is **not** this record's own name — `_read_symbol_name()` (`dimension_types.py:1334-1377`) resolves a *different*, referenced element (a leader/tick-mark symbol via a `Symbol` ElementId parameter) and reads **that** element's `Name`. Name-shaped, but not self-referential — it's a configuration/reference property, not this record's own label. |
| arrowheads | `arrowhead.style`, `arrowhead.tick_size_in` | No | `arrowhead.name` explicitly excluded. |
| line_patterns | `line_pattern.segments_norm_hash` | No | `line_pattern.name` and `line_pattern.uid_or_namekey` explicitly excluded — join key is the D-017 scale-invariant normalized-segment structural hash instead. |
| line_styles | `line_style.weight.projection`, `line_style.color.rgb`, `line_style.pattern_ref.sig_hash` | No | No name-like key present at all (required or excluded) — purely graphic-property based. |
| fill_patterns (both partitions) | `fill_pattern.target`, `.grid_count`, `.grids_def_hash` | No | `fill_pattern.name` explicitly excluded. |
| object_styles (all 4 partitions) | `obj_style.row_key`, `.weight.projection`, `.color.rgb`, `.pattern_ref.sig_hash` (+`.weight.cut` for model) | No | No name-like key present in required/optional/excluded lists at all — category-path (`row_key`) and graphic properties only. |
| phase_filters | `phase_filter.demolished/existing/new/temporary.presentation_id` | No | `phase_filter.name` explicitly excluded. |
| text_types | `text_type.font,.size_in,.bold,.italic,.underline,.color_rgb,.width_factor` | No | `text_type.name` and `text_type.leader_arrowhead_name` explicitly excluded. |
| units | `units.spec`, `.unit_type_id`, `.rounding_method` | No | No name-like key present — units are keyed by spec identity, not a Revit-authored name at all. |
| view_templates (all 5 partitions) | `view_template.def_hash` | No | `view_template.name` explicitly excluded in all 5 partitions — join key is a def-hash of the template's full behavioral definition. |
| view_filter_definitions | `vf.def_hash` | No | `vf.uid_or_namekey` **and** `label.components.name` are both explicitly excluded — this is the one policy file that names a `label.*` sub-path literally, confirming a `label.components.name` item candidate exists for this domain (fed from `domains/view_filter_definitions.py:280`, `"components": {"name": safe_str(name_v or "")}`) but it is deliberately kept **out** of `join_hash`. |
| view_filter_applications_view_templates | `vfa.stack_def_hash` | No | `vfa.template_uid_or_namekey` explicitly excluded. |
| view_category_overrides (all partitions) | `vco.baseline_category_path`, `.baseline_sig_hash`, `.override_properties_hash` | No | No name-like key present. |
| wall_types / floor_types / roof_types / ceiling_types (compound_types.py) | e.g. `wt.function,.layer_count,.total_thickness_in,.stack_hash_loose` (+wraps flags for walls) | No | `*.type_name` explicitly excluded in all four. **Note:** `compound_types.py` never calls `build_join_key_from_policy` inline (§A.1) — this policy is only ever applied at the analysis-side `apply` stage. |
| loaded_family_types | `lft.shape_gate.category`, `lft.type_parameter_schema_hash` | No | No name-like key present in required/optional. **Note:** same as above — no inline call; policy-application is analysis-side only. |

## A.3 `join_hash`'s status/quality companion field

Confirmed name: **`join_key_status`** (a `records.csv` column, `docs/V21_PHASE0_EXPORT_SCHEMA.md:31`) — **not** a field inside the record.v2 JSON's `join_key` dict itself (`core/join_key_builder.build_join_key_from_policy()` returns `schema`/`hash_alg`/`join_hash`/optionally `items`/`selectors`/`keys_used`/`missing_required`/`shape_gating` — no `status` key at all inline; `join_key_status` is purely an analysis-pipeline/CSV-column concept, populated first by flatten and then by apply).

Confirmed value vocabulary (every value traced to the line that sets it):
- `"bootstrap"` — set unconditionally by the flatten stage (`tools/extractor.py:1140`) before any policy is applied; paired with `join_key_schema="sig_hash_as_join_key.v1"` and `join_hash=sig_hash` (identity-mode, degraded for governance per the stage matrix docs).
- `"ok"` — set by the apply stage (`tools/apply_join_policy.py:191`) when all policy-required fields (from `build_candidate_join_key_with_details`, `eval.py:140`) are present for that record.
- `"missing_required"` — returned by `eval.py:137` and propagated verbatim into the CSV column (`apply_join_policy.py:174`) when one or more required fields are absent from that record's identity items.
- `"blocked"` — returned by `eval.py:139` in the edge case where the selected-field set is empty despite no field being reported missing (no fields configured).
- `"missing_policy"` — set by `apply_join_policy.py:141` when the domain has no entry at all in the policy JSON being applied.

This is the field a new name-based collapse key's own status companion should mirror in spirit (a small closed vocabulary of "how was this value derived / can it be trusted," attached to the record row) — **not** `record.v2`'s `identity_quality` (which is a different concept: the record's own required-key completeness for `sig_hash`, computed via `core/record_v2.compute_identity_quality()` and using a five-value dominance-ordered vocabulary unrelated to join-key policy application).

## A.4 Verdict

**The working model holds, with one confirmed, narrow exception.** `sig_hash` is the only identity governed by `contracts/record_contract_v2.md`; `join_hash` is a downstream, policy-layer collapse key — confirmed structurally in §A.1 (it isn't even present in the raw export JSON for 5 of ~21 domain files, and for every domain the CSV column that governance tooling actually reads is bootstrapped from `sig_hash` until an explicit `apply` step overwrites it from a policy). Across all 21 governed domains/partitions read in `policies/domain_join_key_policies.json`, **`label.*` content is excluded from `join_hash`'s actual preimage (required_items) in every domain except `phases`**, where `phase.name` — the exact string backing `label.display` — is the domain's sole required join key.

**Affected domain: `phases` only.** A name-based collapse key built from `label.display` (or an equivalent canonicalized name string) would be **fully orthogonal** to `join_hash` for every domain in this repo except `phases`, where it would be **substantially redundant with `join_hash` already** (both key off the same canonicalized phase-name string, modulo whatever additional normalization the new work applies). Two second-order notes, not full entanglement but worth carrying into interpretation:
- `dimension_types_spot_coordinate`/`_spot_elevation`'s `dim_type.symbol_name` is name-shaped and required, but it names a *different, referenced* element (a leader/tick-mark symbol), not the record's own label — a name-based collapse key over *this record's own* name would not overlap with it, but a naive "does any name touch join_hash" check would incorrectly flag these two partitions if it didn't distinguish self-referential name from referenced-element name.
- `view_filter_definitions` is the only other domain where a `label.*` sub-path (`label.components.name`) is even present as a candidate in the policy file at all — it is explicitly excluded from `join_hash`, so no entanglement, but it demonstrates the mechanism exists for `label.*` values to reach the join-key candidate pool if a future policy edit ever included it.

**Implication for later correspondence/counterfactual interpretation:** for the `phases` domain, any comparison of "does the name-based collapse key predict/diverge from `join_hash` collapse" will be measuring near-tautological agreement (both derive from the same name string) rather than an independent cross-check — that comparison should be flagged as non-informative (or reframed around the small residual difference in normalization/canonicalization between the two paths, if any) rather than reported alongside the other domains' genuinely-orthogonal results. For every other domain, the two axes are confirmed independent (governed `join_hash` uses no label content at all), so correspondence/divergence there is a genuine, informative signal.

