# CHANGELOG

> **Historical-record notice (2026-08-20):** Entries below are an append-only account of earlier behavior and may contain former client, organization, or shared-parameter identities. They are non-operational; current configuration and policy come from maintained code and documentation.

This file tracks **semantic changes only**:
- anything that changes hashes
- anything that changes what a hash *means*
- anything that changes interpretation, scope, or dependency structure

Pure refactors, moves, renames, formatting, and perf tweaks do **not** belong here.

---

## [Unreleased]

### Added
- **`mapping/` line_patterns Revit mapping utility (D-038).** New downstream,
  Revit-writing package (separate from `core/`/`domains/`/`runner/`/`tools/`)
  that reads `tools/export_bundle_pattern_detail.py`'s
  `bundle_pattern_inventory.csv`/`pattern_settings.csv`/`pattern_names.csv`
  and materializes representative `LinePatternElement` objects in the
  currently open Revit document for a mapping/configuration RVT. Every
  requested `(domain="line_patterns", join_hash)` is reconstructed from
  evidence and blocked (never inferred) on incomplete/inconsistent data;
  creation happens inside a per-configuration `Transaction` that is only
  committed after the created element is read back and its `join_hash`
  (via the existing `line_patterns.join_key.v3` policy -- not `sig_hash`)
  is verified to match the request, with rollback on any mismatch or
  exception. No existing extraction, join-key, bundle-analysis, or
  `export_bundle_pattern_detail.py` semantics change -- this is purely
  additive. See `docs/line_pattern_mapping.md` and D-038.

### Added
- **Five fields promoted from phase2-only buckets into `identity_basis.items`, visible
  to policy/pareto discovery for the first time (D-040).** `arrowhead.record_class`
  (arrowheads.py), `lp.is_import` (line_patterns.py), `line_style.pattern_ref.synopsis`
  (line_styles.py), `text_type.leader_arrowhead_sig_hash` (text_types.py -- already
  present in `contracts/domain_identity_keys_v2.json`'s `allowed_keys` but never wired
  into the extractor), and `vt.assigned_view_count` (view_templates.py) were previously
  captured but routed only to `phase2.cosmetic_items`/`coordination_items`/`unknown_items`,
  which `tools/extractor.py`'s flatten stage never reads -- structurally invisible to
  `discover_hash_policy.py`'s/`discover_join_policy.py`'s pareto search. Purely additive:
  no domain's `sig_hash`/`join_key` policy includes any of these five keys, so no hash
  value changes. See DECISIONS.md D-040.

### Changed
- **Policy-driven inline `sig_hash` resolution (D-040).** `wall_types`, `floor_types`,
  `roof_types`, `ceiling_types`, `units`, `units_doc`, `worksets`, `worksets_doc`,
  `browser_organization`, `object_styles` (all 4 partitions), and `arrowheads` now
  resolve their inline sig_hash preimage key set via `core/sig_hash_policy.py`'s new
  `resolve_sig_hash_keys()`, reading `ctx["sig_hash_policies"]` (populated by
  `runner/extraction_context.py` from `policies/domain_sig_hash_policies.json`, the
  same file `core/sig_hash_builder.py`'s post-stage recompute already reads) instead of
  a hardcoded Python list/set. The hardcoded lists remain as fallbacks for when
  `ctx["sig_hash_policies"]` is absent (e.g. a unit test), so this is a no-op against
  today's values everywhere it's wired in -- not hash-breaking. Closes the structural
  gap that let D-039 happen unnoticed: going forward there is one source of truth for
  "which fields drive sig_hash" per domain, consumed by both the inline extractor and
  the post-stage recompute, rather than two independently hand-maintained copies that
  can silently drift apart. Each domain's own required/degraded/blocked status logic is
  unchanged. See DECISIONS.md D-040.
- **Repository-neutral runtime and sample defaults.** Dynamo runner discovery no
  longer searches an organization-specific user-profile path, both checked-in
  Dynamo graphs have blank workstation inputs/hints, and the default
  `client_sector.csv` contains synthetic examples only. Deployments using real
  client labels must pass their approved mapping with `--client-sector`.
  Selected-blank segment folder components now render as
  `no_external_client` rather than an organization name; rebuilding such a
  segment therefore changes its `output_folder`, but not its `segment_id` or
  population membership.

### Fixed
- **`wall_types`/`floor_types`/`roof_types`/`ceiling_types`: post-stage `sig_hash`
  policy no longer hashes in the type's own display name/fill color (D-039).**
  `policies/domain_sig_hash_policies.json`'s `allowed_items` for these four
  domains previously mirrored the full `identity_basis.items` set (mechanically
  inherited from `contracts/domain_identity_keys_v2.json`'s `allowed_keys`,
  with no `sig_hash_keys` override present to narrow it) rather than the
  narrower "semantic" subset each extractor (`domains/{wall,floor,roof,
  ceiling}_types.py`) already hashes inline. A post-stage `sig_hash` stage
  recompute (`core/sig_hash_builder.py`, invoked by `run_extract_all.py`'s
  `sig_hash` stage and unconditionally overwriting `records.csv`) would
  therefore have hashed `wt.type_name`/`ft.type_name`/`rt.type_name`/
  `ct.type_name` and `*.coarse_fill_color_rgb` into `sig_hash`, contradicting
  this project's own "names are metadata only, never in behavior hashes" rule
  and diverging from the extractor's own inline `sig_hash` value for the same
  record. Added an explicit `sig_hash_keys` override to each of the four
  registry blocks and hand-narrowed the compiled policy to match (same
  hand-patch convention already used for `object_styles_*`/`worksets`/
  `browser_organization`). **Hash-breaking** for these four domains' `sig_hash`
  values specifically: two records differing only by type name or fill color
  now produce the same `sig_hash`. `join_hash` is unaffected -- the join-key
  policies for these domains already excluded these fields correctly via
  `explicitly_excluded_items`. See DECISIONS.md D-039.
- **`text_types`: `text_type.name` removed from the domain-level `hash_v2` rollup's
  hashed key set (D-041).** `TEXT_TYPE_SEMANTIC_KEYS` (now
  `TEXT_TYPE_SEMANTIC_KEYS_FALLBACK`) included the type's own display name, diverging
  from `policies/domain_sig_hash_policies.json`'s already-correct 12-key
  `text_types` policy (which excludes it) with no D-0xx decision ever authorizing the
  inclusion -- discovered incidentally while wiring `text_types.py` into D-040's
  `resolve_sig_hash_keys()` mechanism. Impact is scoped: `domains/text_types.py`
  already omits `sig_hash`/`join_key` from its own exported record entirely ("canonical
  mode"), so this only changes the domain-level `hash_v2` aggregate computed inside the
  extractor, not any per-record `sig_hash` field (which this domain never emits at
  extraction time). Fixed a test (`tests/test_text_types_canonical_selectors.py`) that
  had encoded the divergence as expected behavior. See DECISIONS.md D-041.
- **`object_styles`: `obj_style.row_key` (a name-derived value) removed from
  all four partitions' sig_hash key set (D-042).** Found by automated PR
  review on the D-040 PR: `contracts/domain_identity_keys_v2.json`'s
  pre-existing `sig_hash_keys` override for `object_styles_model`/
  `_annotation`/`_analytical`/`_imported` included `obj_style.row_key`
  (`"{parent}|{row}"`, derived from `Category.Name`), diverging from the
  extractor's own `_MODEL_SEMANTIC_KEYS`/`_NON_MODEL_SEMANTIC_KEYS`
  constants, which never included it. No decision record authorized hashing
  a name into this domain's behavioral identity. Removed `row_key` from the
  registry override and the compiled `policies/domain_sig_hash_policies.json`
  `allowed_items` for all four domains (still required for `identity_basis.items`/
  `join_key`, unaffected). **Hash-breaking** for these four domains' `sig_hash`
  specifically: two categories/subcategories with identical graphic-override
  behavior but different names now correctly produce the same `sig_hash`.
  Does not affect already-exported JSON (the extractor's own inline
  `sig_hash_v2` never included `row_key`); only the analysis-side `sig_hash`
  stage recompute is affected. Also fixed the D-040 drift-guard test
  (`test_resolve_sig_hash_keys_real_policy_file_matches_hardcoded_fallbacks`),
  which had never checked any `object_styles_*` domain. See DECISIONS.md D-042.
- **`core/sig_hash_policy.py`: `resolve_sig_hash_keys()` now resolves
  `allowed_item_prefixes` and no longer treats a validated empty
  `allowed_items` as policy-absent (D-042).** Also found by automated PR
  review: the D-040 3-argument signature never resolved
  `allowed_item_prefixes` against a record's actual keys (unlike
  `core/sig_hash_builder.py`'s `_key_allowed()`, the post-stage recompute's
  reference implementation), and treated a policy-validated but empty
  `allowed_items` list as "absent," incorrectly falling back to the
  hardcoded default instead of honoring the empty set. No currently-wired
  domain relies on `allowed_item_prefixes` for sig_hash today, so this had
  not yet produced an observably wrong hash, but would have reproduced the
  exact D-039-class drift the first time a prefix-based policy adopted this
  resolver. Signature is now `(policies, domain_name, candidate_keys,
  fallback)`; every call site introduced in D-040
  (`wall_types`/`floor_types`/`roof_types`/`ceiling_types`, `units` x2,
  `worksets` x2, `browser_organization`, `object_styles`, `arrowheads`,
  `text_types`) updated to pass the record's own identity-item keys as
  `candidate_keys`. `object_styles.py`'s call moved inside its per-category
  loop (it previously ran once before the loop, with no per-record keys
  available for prefix resolution). See DECISIONS.md D-042.
- **Registered three D-040 identity-visibility promotions that were never
  added to the domain key registry, closing a contract-validation gap
  (D-043).** `arrowhead.record_class`, `lp.is_import`, and
  `vt.assigned_view_count` were promoted into `identity_basis.items` by
  D-040 but never added to `contracts/domain_identity_keys_v2.json`'s
  `allowed_keys` for `arrowheads`/`line_patterns`/`view_templates_*`, so
  `validators/record_v2.py`'s `validate_record_v2()` would reject every
  real-export record from these domains with
  `identity.key.not_allowed:<key>`. Found by automated PR review; no test
  in the suite called `validate_record_v2()` against any of these three
  domains, which is why this shipped across two PRs unnoticed. Added the
  missing `allowed_keys` entries; for `arrowheads` and the 5
  `view_templates_*` partitions (which had no prior `sig_hash_keys`
  override), also added an explicit override reproducing today's actual
  sig_hash preimage so the newly-visible key can't silently widen sig_hash
  on a future policy regeneration (the D-039 mechanism, this time closed
  proactively). Not hash-breaking anywhere. Added regression tests
  (`test_arrowhead_record_class_passes_contract_validation`,
  `test_lp_is_import_passes_contract_validation`,
  `test_vt_assigned_view_count_passes_contract_validation_for_every_partition`)
  that build a record.v2 and assert clean `validate_record_v2()` results,
  closing the coverage gap directly. See DECISIONS.md D-043.
- **`sig_basis.keys_used` now reports the keys actually present in the hash
  preimage instead of a hardcoded/fallback key set, across every domain
  converted to `resolve_sig_hash_keys()` in D-040/D-042 (D-043).** Found by
  automated PR review: `arrowheads.py`'s `sig_basis.keys_used` reported the
  full cross-record-class union of sig_hash policy keys regardless of
  record class, so a `SizeOnly`/`Unknown` arrowhead (whose
  `identity_basis.items` never contains Arrow- or Tick-specific keys at
  all) claimed those keys were hashed when they were never present on the
  record -- unreproducible audit metadata. The same pattern (reporting a
  fixed constant instead of the record's actual filtered key set) existed,
  though not yet observably wrong given today's data, in `wall_types`,
  `floor_types`, `roof_types`, `ceiling_types`, `units`/`units_doc`,
  `worksets`/`worksets_doc`, `browser_organization`, and `object_styles`
  (all 4 partitions) -- all fixed the same way. Not hash-breaking anywhere
  (only the `sig_basis.keys_used` metadata field changes, and only where it
  previously diverged from reality). Added
  `test_sig_basis_keys_used_reflects_only_keys_present_for_record_class` to
  prove a `SizeOnly` arrowhead's `keys_used` excludes keys it never had. See
  DECISIONS.md D-043.
- **`text_types`: `text_type.leader_arrowhead_sig_hash` no longer collapses
  "unresolved leader-arrowhead reference" into the same state as "no leader
  arrowhead" (D-044).** Found by automated PR review: when a text type has a
  leader arrowhead assigned but `ctx["arrowheads_by_type_id"]` doesn't
  resolve it (missing entry, absent map, or the lookup itself raises), the
  item emitted `v=None, q=ok` -- identical to the genuine "no leader
  arrowhead" state, violating the project's fail-soft policy against
  collapsing distinct states. Now emits `q=missing` (dependency unresolved)
  or `q=unreadable` (lookup raised) when a reference exists but its hash
  didn't resolve; the explicit-none case (`v=None, q=ok`) is unchanged. Not
  hash-breaking -- this key isn't in `required_keys` or
  `TEXT_TYPE_SEMANTIC_KEYS_FALLBACK`, so `sig_hash`/`status`/`identity_quality`
  are unaffected. Added `tests/test_text_types_leader_arrowhead_quality.py`,
  the first test to exercise `text_types.py`'s `extract()` with an actual
  leader-arrowhead reference present, closing the coverage gap that let this
  ship. See DECISIONS.md D-044.
- **`sig_hash_schema` bumped from `.v1` to `.v2` for `wall_types`/`floor_types`/
  `roof_types`/`ceiling_types` and all four `object_styles_*` partitions
  (D-045).** Found by automated PR review: D-039 (compound-types) and D-042
  (`object_styles`) both narrowed these domains' `sig_hash` preimage in a
  hash-breaking way but left the schema label at `.v1`, so an old export and
  a freshly recomputed one carry the same schema string despite hashing
  different fields -- no signal that the two `sig_hash` values aren't
  comparable. No `sig_hash` value changes from this decision; only the
  schema label. For the four `object_styles_*` domains this also adds an
  explicit `sig_hash_schema` to the registry for the first time (previously
  implicit via `generate_sig_hash_policy.py`'s `.v1` fallback). See
  DECISIONS.md D-045.
- **`text_types`: a stale/unresolvable leader-arrowhead element reference no
  longer collapses into "no leader arrowhead" (D-046).** Direct follow-up to
  D-044, found by automated PR review: `leader_arrow_ref_present` was set
  only after `doc.GetElement(arrow_id)` successfully returned an element,
  so a positive `AsElementId()` reference that then failed to resolve (a
  stale/deleted element, or `GetElement()` raising) still fell back to
  `v=None, q=ok` -- the same state-collapse D-044 fixed for the sig_hash
  lookup, one step earlier. Now `leader_arrow_ref_present` is set as soon as
  a positive reference ID is observed, and a failed/empty element
  resolution reports `q=missing` (not found) or `q=unreadable` (lookup
  raised). Not hash-breaking, same reasoning as D-044. Added two tests to
  `tests/test_text_types_leader_arrowhead_quality.py` covering the
  stale-reference and lookup-exception cases directly.
- **`line_styles`: registered `line_style.pattern_ref.kind` in the domain key
  registry and corrected the compiled `sig_hash` preimage to match the
  extractor's real behavior (D-047).** Found by automated PR review while
  pinning `sig_hash_keys` against a `pattern_ref.synopsis` widening risk
  (same D-039 mechanism, a fourth domain): `contracts/domain_identity_keys_v2.json`'s
  `line_styles` `allowed_keys` was missing `line_style.pattern_ref.kind`
  entirely -- a key the extractor has always emitted into every record's
  `identity_basis.items` -- so `validate_record_v2()` rejected every
  real-export `line_styles` record with
  `identity.key.not_allowed:line_style.pattern_ref.kind`. The same registry
  block also allowed `color.rgb`/`weight.cut` into the compiled `sig_hash`
  policy even though the extractor never hashes either (`weight.cut` isn't
  even emitted). Added `pattern_ref.kind` to `allowed_keys`, added an
  explicit `sig_hash_keys` override matching `LINE_STYLE_SEMANTIC_KEYS`
  exactly (the extractor's real inline preimage), corrected the compiled
  policy's `allowed_items` to match, and bumped `sig_hash_schema` to `.v2`
  (D-045 discipline). Not hash-breaking for the extractor's own inline
  `sig_hash` (unchanged); hash-breaking for the analysis-side `sig_hash`
  stage recompute only, correcting it to match what the extractor has
  always actually computed. Documented the resulting, legitimate
  sig_hash/join_hash divergence (`pattern_ref.kind` is join-excluded for
  collision-coarseness, not because it's non-behavioral) in
  `ACCEPTED_SIG_HASH_JOIN_KEY_OVERLAPS`. Added regression tests closing the
  "no test validates line_styles against the registry" gap. See
  DECISIONS.md D-047.
- **`mapping/create_line_pattern_mappings.py`: an explicit but invalid `IN[2]`
  repo root now fails loudly instead of silently falling back to the
  environment or `__file__`.** A caller-supplied `IN[2]` is an explicit
  selection, not a hint -- previously, a typo'd or incomplete path there fell
  through to `REVIT_FINGERPRINT_REPO_ROOT_SELECTED`/`REVIT_FINGERPRINT_REPO_DIR`
  or `__file__`, any of which could resolve a *different* checkout than the
  one requested (e.g. a stale env var left over from a previous run in the
  same persistent Dynamo session), applying mappings with unintended code and
  no error at all. Now raises immediately when `IN[2]` is given but doesn't
  look like a checkout.
- **`mapping/create_line_pattern_mappings.py`: cached `mapping`/`core`/`domains`
  modules from a previously-selected checkout are now always purged, and the
  resolved repo root is always promoted to the front of `sys.path`, before
  (re-)importing.** A persistent Dynamo CPython session keeps its interpreter
  (and `sys.modules`/`sys.path`) alive across node re-runs, which surfaced two
  compounding bugs found across two rounds of automated PR review:
  1. A first fix purged stale modules only when a check keyed on a single
     representative module (`mapping.line_pattern_reconstruction`) detected a
     mismatch -- but if a *different* script (the extraction runner) had
     already cached `core.*`/`domains.*` from another checkout in the same
     interpreter without ever importing `mapping.*`, that check saw "nothing
     cached yet" and skipped the purge entirely, so reconstruction would
     still silently reuse the other checkout's hashing/domain code.
  2. Even with modules purged, `sys.path.insert(0, repo_root)` guarded by
     `if repo_root not in sys.path` does nothing when `repo_root` is already
     present but not first (e.g. checkout B ran, then A ran and inserted
     itself ahead, then B runs again) -- imports would still resolve from
     whichever checkout sits earlier in `sys.path`.
  Fixed by dropping the staleness heuristic entirely (an unconditional purge
  of every `mapping`/`core`/`domains`-prefixed `sys.modules` entry is cheap)
  and by removing-then-reinserting the resolved root at `sys.path[0]`
  unconditionally, exactly mirroring `runner/thin_runner.py`'s existing
  `_purge_repo_modules()` / checkout-promotion handling for the same class of
  bug.
- **`mapping/create_line_pattern_mappings.py`: repo-root resolution no longer
  depends on `__file__`.** This entry point is meant to be pasted directly
  into a Dynamo Python Script node, where Dynamo executes the code from a
  string (`File "<string>"`) rather than loading it from disk -- `__file__`
  is undefined in that context, so `NameError: name '__file__' is not
  defined` fired immediately on the first real-world run. Fixed by mirroring
  `runner/run_dynamo.py`'s existing env-var-first resolution
  (`REVIT_FINGERPRINT_REPO_ROOT_SELECTED`/`REVIT_FINGERPRINT_REPO_DIR`),
  falling back to `__file__` only when it's actually available (a node that
  loads the script from a file on disk), and otherwise requiring the
  checkout path as a new optional `IN[2]`. No change to reconstruction,
  hashing, or Revit-mutation behavior -- `mapping/line_pattern_reconstruction.py`
  and `mapping/line_pattern_revit_apply.py` are unaffected (they're always
  imported as real files, where `__file__` is defined normally).
- **`tools/export_bundle_pattern_detail.py`: `_iter_identity_csv()` now always
  resolves item quality from `item_value_type` on the v2.1 identity-item shard
  schema, never from `item_role`.** `item_role` is not a quality field at all
  on this schema -- it is a separate, unrelated tag: blank for a normal row
  written by `tools/extractor.py`'s shard writer, but populated with a
  non-quality marker (e.g. `"synthetic"`) for `tools/run_extract_all.py`'s
  synthetic `line_pattern.segments_norm_hash` row. The original code,
  `row.get("item_role", row.get("item_value_type", ""))`, only fell through to
  `item_value_type` on a *missing* key -- since `item_role` is always present
  (just usually empty), this returned `""` for every normal row (leaving
  `pattern_settings.csv`'s `q` column blank domain-wide) and, after a first
  attempted fix that preferred `item_role` whenever non-empty, would have
  returned the literal string `"synthetic"` as `q` for norm-hash rows instead
  of their real quality. Fixed by reading `q` from `item_value_type`
  unconditionally on this schema. Found via automated PR review while
  validating the new `mapping/` line_patterns utility (which depends on `q` to
  gate reconstruction) against a realistic v2.1 export shape; the bug is not
  specific to that consumer -- it affected `q` resolution for every domain
  read through `export_bundle_pattern_detail.py`'s v2.1 code path. Does not
  affect the legacy `k`/`v`/`q` schema path, `sig_hash`/`join_hash`
  computation (which reads identity items via a different path), or any other
  exported artifact.
- **`materials`: `material.keynote` no longer silently omitted from `identity_basis.items` when blank.**
  `domains/materials.py`'s keynote emission previously used a truthy guard
  (`if keynote and keynote[0]:`) with no `else` branch, so materials with a blank
  or unset Keynote parameter had the `material.keynote` identity item omitted
  entirely instead of present with `q: "missing"`. Since `materials.join_key.v3`
  lists `material.keynote` as `required_items`, and join-key building treats
  required-key *presence* independently of value completeness (see
  `tools/join_key_discovery/eval.py`'s `build_identity_index`/
  `build_candidate_join_key_with_details`), this caused the join-policy gate to
  fail with `reason=missing_required, missing_keys=material.keynote` for every
  blank-keynote material, blocking `join_hash` computation for those records
  entirely. Fixed by always appending the item via the existing `_mk_item()`
  helper (`identity_items.append(_mk_item("material.keynote", keynote))`), which
  already produces `v: null, q: "missing"` for blank/unset values via
  `_read_param_as_string`'s `canonicalize_str`. This is join-key/hash-affecting
  for previously-blank-keynote materials (they now clear the gate and receive a
  real `join_hash` instead of being excluded from comparison), but does not
  change `sig_hash` (computed separately from `name`/`class` only). No other
  domain fields are affected by this change; `material.manufacturer`/
  `material.model` are read but were never emitted as identity items at all
  (not merely guarded) -- flagged as a separate follow-up, not addressed here.
- **`tools/apply_join_policy.py`: diagnostics `policy_id` fallback now reflects
  the actual policy schema instead of a hardcoded `.v21` literal.** Line ~165's
  `policy_id = str(p.get("policy_id") or f"{domain}.join_key.v21")` fell back
  straight to a hardcoded `{domain}.join_key.v21` whenever a domain's policy
  block had no explicit `policy_id` key -- true for every domain in
  `policies/domain_join_key_policies.json`, which key off `join_key_schema`
  instead (e.g. `materials.join_key.v3`). This made `join_key_policy_id` (and
  the `policy_id` column in `join_policy_gate_diagnostics.csv`/
  `join_policy_failures.csv`) cosmetically wrong -- reporting-only, since
  `join_key_schema` itself (line ~167) already read `p.get("join_key_schema")`
  first and was unaffected. Fixed by falling back to `p.get("join_key_schema")`
  before the hardcoded literal. Does not change gate evaluation, `join_hash`,
  or any hash semantics -- diagnostics/reporting output only.
- **`tools/build_segment_manifest.py`: `business_center_label` values shorter
  than 4 digits are now zero-padded before segment_id construction.** A
  purely-numeric `business_center_label` (e.g. `"0"`, `"796"`) is left-padded
  to 4 digits (`"0000"`, `"0796"`) in `_normalize_rows()`, ahead of the
  existing first-seen-casing fold, so it merges with any correctly-formatted
  occurrence of the same code instead of fragmenting into a spurious second
  segment. Fixes a real operator-workflow failure mode: opening
  `file_metadata.csv` in Excel without explicitly importing the
  `business_center_label` column as Text causes Excel to reinterpret `"0000"`
  as the number `0` and silently drop the leading zeros on save -- every real
  business_center_label value in this corpus (the `"0000"` enterprise
  bookkeeping token included) is exactly 4 digits, so padding up is safe. A
  value already at 4+ digits, or containing any non-digit character (e.g.
  `"BC_1234"`, `"Page"`), is left untouched. Segment-id/hash-affecting for any
  corpus that was already carrying a collapsed short-digit
  `business_center_label` value (that value now folds into the padded
  segment instead of its own).
- **`tools/governance_manifest.py`: `normalize_business_center_label()` gets
  the same zero-padding fix (PR #425 review follow-up).** This module reads
  `file_metadata.csv` directly and independently of `build_segment_
  manifest.py` (see the module's own docstring — disjoint governance
  populations are deliberately not built on the segment lattice), so the
  fix above did not reach it. A purely-numeric value shorter than 4 digits
  is now zero-padded before the enterprise-bookkeeping-token check and the
  `BC_`-prefix strip, so a collapsed `"0"`/`"796"` is recognized correctly
  (`"0"` as the `"0000"` enterprise token, `"796"` merging with `"0796"`)
  instead of fragmenting a governance population or spawning a fake
  business center. `governance_relationships.py` imports and reuses this
  same function, so it inherits the fix without a separate change.

### Added
- **Governance narrative evidence-package layer, Phases 7-10 (D-030 through D-034):
  reading order/completeness gate, known-bad-inference clarifications,
  comparison-registry completeness note, union-breadth domain confidence, and a
  package-portable docs subfolder.**
  - **D-030 (Phase 7):** New `docs/governance_reading_order.md` states this
    package's intended audience (a leadership reader without Revit domain
    knowledge, expected to ask governance convergence/fragmentation questions
    rather than resolve them unassisted) and an explicit cold-start reading
    path through the package. `governance_evidence_package.py`'s `_artifact()`
    gains a mandatory `required_before_conclusions: bool` field, and
    `build_evidence_map()`'s output gains a top-level `reasoning_prerequisites`
    list -- the full set of `required_before_conclusions=true` artifact_ids,
    exposed as a completeness set to exhaust rather than an ordinal to sample
    from (an ordinal was considered and rejected: it invites reading only the
    top few and reasoning from a partial picture while technically "following
    the order"). `render_evidence_authority_header()` gains a pointer line.
    No classification/CSV output changes -- descriptive fields and a new
    static doc only.
  - **D-031 (Phase 8, docs-only):** Two new entries in
    `docs/governance_interpretation_guide.md`'s "Known bad inferences": (1)
    a domain's enterprise-scoped `Insufficient Evidence` tier does not mean
    the domain has no usable evidence anywhere in the package -- check the
    client/BC summaries and `cross_client_convergence` first; (2) the corpus
    currently contains files from a single region, so a future `region`
    segmentation dimension will read identically to the existing
    enterprise-level rollup until a second region's data exists -- a fact
    about current data coverage, not a methodology gap. Both entries also get
    first-class placement in the D-030 reading-order doc's "read this before
    drawing conclusions" callout. Also adds an explicit audience/intent
    statement to the guide's "What this package is for" section.
  - **D-032 (Phase 9a):** New optional `--comparison-registry` CLI argument
    (same present-or-absent, gracefully-degrading pattern as other optional
    inputs). When supplied, renders a per-domain **Input Completeness /
    Staleness** note near Analytical Notes -- an evidence/registry-mismatch
    proxy (not a not-run-coverage guarantee: a comparison absent from every
    evidence source has no key to inspect and is not counted) via new
    `build_comparison_completeness()`. Where a mismatch IS detected, it
    distinguishes "this domain's registry stamp is missing or stale relative
    to its evidence" from "this domain's evidence is thin because
    convergence is actually weak," which previously looked identical to a
    reader of `governance_domain_summary.csv`.
    The registry file itself is never embedded in the output package -- only
    the derived counts are. `governance_package_health.json` gains a
    `comparison_completeness` field when the registry is supplied. No
    existing classification/tier output changes when the flag is absent.
  - **D-033 (Phase 9b):** `governance_domain_summary.csv` gains new breadth
    columns derived from `cross_segment_union_inventory.csv` via new
    `build_union_breadth_by_domain()` (corpus-wide/client-wide/project-wide/
    file-level reuse counts per domain; corpus-wide/client-wide classification
    requires more than one client in the denominator, matching
    `compare_cross_segment.py`'s own `_reuse_bucket_for()` guard). Only the
    strongest narrative exceptions -- broad reuse with weak formal cascade
    (a natural-standard candidate the cascade metrics alone would miss), or
    narrow reuse despite strong cascade (worth flagging as fragile) -- are
    rendered as a new `detect_anomalies()` note category, using the same
    policy-externalized-threshold discipline D-029 established (new
    `union_breadth_*` keys in `anomaly_thresholds.json`). Closes the last
    open item in `docs/governance_generator_cross_compare_coverage.md`'s
    implementation sequence.
  - **D-034 (Phase 10):** The four static, package-type-level reference docs
    (`governance_interpretation_guide.md`, `governance_question_routes.md`,
    `governance_reading_order.md`, `governance_classification_rules.md`) move
    into a new `docs/governance/` subfolder -- filenames unchanged, only
    `_DOCS_DIR` in `generate_governance_narrative.py` is repointed. `main()`
    now copies each of the four (via `shutil.copy2`, only when present) into
    `--out` alongside a run's other outputs, so a `--out` directory handed to
    someone without the repo checked out is self-contained -- the narrative
    and evidence map already point readers at these docs by name. Only these
    four are copied; the CSV siblings the same block registers
    (`cross_segment_file_pairs.csv`, `comparison_registry.csv`, etc.) are
    never embedded, per D-023/D-024/D-032. Evidence-map/manifest `path`/
    `present` fields for these four artifacts continue to describe the
    checked-in repo doc, not the copy -- the copy is a portability
    convenience, not a second source of truth.
  - All five phases: no existing classification, tier, or CSV/JSON field
    *value* changes for any prior invocation -- verified via the same
    byte-identical `governance_domain_summary.csv`/`governance_client_
    summary.csv`/`governance_bc_summary.csv` regression discipline D-021
    established. See D-030 through D-034 and `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py`'s remaining anomaly/note materiality
  thresholds -- `detect_anomalies()`'s ~18 bare numeric literals across 11
  distinct `notable_anomalies` findings (including one, the
  `provided_to_configured`/`provided_to_used` "carried but not actively used"
  check, not previously called out as its own finding), the
  `provided_to_used_containment` primary-read threshold in
  `build_governance_state_summary()`'s `_finalize_state_bucket()`,
  `render_findings_and_recommendations()`'s independently-duplicated phases
  check, `_passive_inheritance_risk_domains()`'s bundle-share/passive-indicator
  thresholds, and `_shape_note()`'s Project Portfolio density-similarity
  thresholds -- now load from a fifth JSON policy profile,
  `policies/governance/anomaly_thresholds.json`, via the same
  `apply_governance_policy()` global-reassignment mechanism D-021 established
  (function bodies unchanged except reading a named constant instead of a
  literal). `_passive_inheritance_risk_domains()`'s dual-schema branch now
  reads the existing `governance_thresholds.json` `passive_material_threshold`
  key directly instead of an independent, drifted `0.20` literal, closing a
  gap where a change to that policy value would not previously have
  propagated to this function. New `docs/governance_classification_rules.md`
  documents the branch order and exception conditions of `assign_tier()`,
  `score_reliability()`, `detect_anomalies()`, `build_governance_state_summary()`'s
  `primary_governance_read` selection, and `_passive_inheritance_risk_domains()`
  by threshold-key name (not value) -- added to `governance_evidence_map.json`
  alongside the existing static docs. `render_header()`'s "How to Read the
  Analysis" block, which restated containment/cross-client-similarity/
  all-view-used-view/score-range definitions already covered by
  `docs/governance_interpretation_guide.md`, is now a one-paragraph pointer
  at that guide's "Metric semantics" section; three concepts the block
  covered but the guide didn't yet (containment as reuse/propagation
  evidence rather than approval, used-view's Project-target-primary scope,
  and the 0-1 score range) were added to the guide first, in the same
  change. Every default value in the new JSON profile reproduces this
  generator's pre-externalization Python literal exactly -- no existing
  classification, tier, `notable_anomalies` text, or CSV column changes for
  any existing invocation (verified by the same byte-identical
  `governance_domain_summary.csv` regression test D-021 established, run
  twice: default vs. explicit `--policy-dir policies/governance/`). See
  D-029 and `docs/governance_evidence_package.md`.
- **`dimension_types` domain family: field expansion across all 7 partitions (Area 7):**
  Adds ~25 new identity items across `dimension_types_linear`/`_angular`/`_radial`/
  `_diameter`/`_spot_elevation`/`_spot_coordinate`/`_spot_slope`, closing the gap
  identified in `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
  §7. All fields are read via the existing `first_param(bip_names=..., ui_names=...)`
  pattern (`core/rows.py`) and verified against three consistent runs from the
  approved external probe dataset described in `tools/probes/Exports/README.md`
  (including its per-shape `observed_on_shapes` breakdown), rather than a fresh
  live-Revit run in this pass —
  where the probe data corrected this area's own initial family-applicability
  guesses (Centerline/Interior Tick Mark/Equality are Linear+Angular only, **not**
  Radial/Diameter as originally guessed; Alternate Units genuinely applies to all 7
  partitions per Greg's correction, not just Linear), the probe evidence was
  followed over the guess.
  - **§1 spot-family Leader Arrowhead cluster** (`extract_spot_elevation`/
    `_spot_coordinate`/`_spot_slope`): new shared helper
    `core/dimension_type_helpers._read_leader_arrowhead()` (mirrors the working
    `domains/text_types.py` Leader Arrowhead read/resolve pattern — `BuiltInParameter.
    LEADER_ARROWHEAD` → `ctx["arrowheads_by_type_id"]`) implements the
    previously-reserved-but-never-implemented `dim_type.leader_arrowhead_sig_hash`
    identity key, plus new `dim_type.leader_arrowhead_uid`/`_name` (file-local/cosmetic
    metadata, same status as `text_type.leader_arrowhead_uid`/`_name`) and ordinary
    `dim_type.leader_arrowhead_line_weight`/`dim_type.leader_line_weight` reads.
  - **§2 linear/angular/radial/diameter leader config**: `dim_type.leader_tick_mark_sig_hash`
    (new generic `core/dimension_type_helpers._read_arrowhead_ref_sig_hash()` helper,
    reused for the other tick-mark-family fields below), `dim_type.leader_type`,
    `dim_type.show_leader_when_text_moves` — a distinct Revit feature from §1's spot
    Leader Arrowhead despite the shared word "leader"; no shared helper or key-naming
    prefix between the two.
  - **§3 witness lines** (linear/angular only): `dim_type.witness_line_extension_in`/
    `_gap_to_element_in`/`_length_in`, plus `dim_type.witness_line_tick_mark_sig_hash`
    (Linear only — confirmed asymmetry vs. Angular, consistent across all 3 probe runs).
  - **§4 equality/centerline/tick-weight** (linear/angular only, correcting this area's
    own initial radial/diameter guess): `dim_type.equality_text`/`_witness_display`
    (`dim_type.equality_formula` dropped — probe storage=`None`/unsupported, not a
    plain-parameter read); `dim_type.centerline_symbol_name` (new generic
    `core/dimension_type_helpers._read_element_ref_name()` helper — name-only
    resolution, not routed through `arrowheads_by_type_id` since it isn't a confirmed
    arrowhead-family reference) and `dim_type.centerline_tick_mark_sig_hash`/
    `dim_type.interior_tick_mark_sig_hash`/`_display`. `dim_type.tick_mark_line_weight`
    (angular/diameter/linear/radial — distinct from the existing `dim_type.line_weight`).
    `dim_type.centerline_pattern_sig_hash` (linear/angular only) resolves via a new
    `core/dimension_type_helpers._read_line_pattern_ref_sig_hash()` helper — the same
    `ctx["line_pattern_uid_to_hash"]`/`ctx["line_pattern_special_values"]` resolution
    `domains/object_styles.py`/`domains/line_styles.py` already use for their own
    `GetLinePatternId()` references, so a built-in pattern id (e.g. the probe's
    `-3000010`, "Solid") resolves to the same `"<Solid>"` sentinel those domains use
    instead of collapsing to the same `missing` value as "no centerline pattern"
    (PR #412 review fix — an earlier revision routed this through a plain
    `doc.GetElement()` name lookup that returned `missing` for built-in pattern ids,
    losing the distinction between "Solid" and "none").
  - **§5 alternate units** (all 7 partitions): `dim_type.alternate_units`,
    `dim_type.alternate_units_prefix`/`_suffix`. `dim_type.alternate_units_format_id`
    was dropped (PR #412 review fix): it required
    `DimensionType.GetAlternateUnitsFormatOptions()`, an accessor not confirmed to
    exist on the Revit surface this repo's committed probe data represents (raises
    `AttributeError` there) — since every extractor added it as a non-required item and
    the status-reason loop counts any non-OK optional item, this made every
    dimension-type record degrade without the field ever capturing real data. Rather
    than ship a field permanently pinned to `unsupported_not_applicable`, it was
    removed along with `_read_unit_format_info()`'s `alternate=` parameter.
  - **§6 `Suppress Spaces`** (all 7 partitions, wherever `_read_unit_format_info()` is
    already called): extends that function's return tuple with `suppress_spaces_v/_q`
    read off the same `FormatOptions` object as `rounding`/`accuracy` — same
    `FormatOptions`-boolean-flag gap Area 8 documented for `units.py`, applied here to
    `DimensionType.GetUnitsFormatOptions()` instead of the doc-level `Units` object.
  - **§7 provisional cluster**: implemented with probe-confirmed family assignments —
    `dim_type.text_offset_in` (angular/diameter/linear/radial), `dim_type.
    text_offset_from_leader_in` (all 3 spot partitions), `dim_type.
    text_offset_from_symbol_in` (spot_elevation/spot_coordinate only — not spot_slope,
    consistent with spot_slope having no `symbol_name` field either), `dim_type.
    dimension_string_type`/`dim_type.show_opening_height` (linear only), `dim_type.
    rotate_with_component` (all 3 spot partitions), `dim_type.elevation_base`
    (spot_elevation only), `dim_type.coordinate_base` (spot_coordinate only).
  All new fields are non-required/non-blocking enrichment (added to each partition's
  `identity_items`/hash preimage but never `required_qs`) — a conservative posture for
  a mass field-addition pass without a live Revit run to per-family-verify blocking
  behavior against. New ElementId-referenced sig_hash/name fields (`leader_tick_mark_sig_hash`,
  `witness_line_tick_mark_sig_hash`, `centerline_tick_mark_sig_hash`,
  `interior_tick_mark_sig_hash`, `centerline_pattern_sig_hash`, `centerline_symbol_name`,
  `leader_arrowhead_uid`/`_name`/`_sig_hash`) are added to each partition's existing
  "missing is acceptable" status-reason exemption set (same treatment as the
  pre-existing `dim_type.tick_mark_sig_hash`), since a negative/absent reference
  legitimately means "none selected," not a data gap. All 4 of `_read_arrowhead_ref_sig_hash()`
  (`leader_tick_mark_sig_hash`/`witness_line_tick_mark_sig_hash`/`centerline_tick_mark_sig_hash`/
  `interior_tick_mark_sig_hash`), `_read_leader_arrowhead()`'s `sig_hash` element, and
  `_read_line_pattern_ref_sig_hash()` (`centerline_pattern_sig_hash`, both its positive-id and
  negative/built-in branches) distinguish a genuine "no reference selected" state
  (`ITEM_Q_MISSING`, still exempted) from a *real* reference — positive custom id, or a
  negative built-in id like "Solid" — that could not be resolved because its ctx dependency
  map (`arrowheads_by_type_id`/`line_pattern_uid_to_hash`/`line_pattern_special_values`) was
  never populated or doesn't cover that id (`ITEM_Q_UNREADABLE`, **not** exempted). Without
  this distinction, distinct custom/built-in tick marks, arrowheads, or centerline patterns
  that are all unresolved for the same reason (e.g. `arrowheads`/`line_patterns` excluded
  from a given run's domain allowlist) would silently collapse to the same identity value
  and hash instead of degrading the record (PR #412 review, two rounds:
  `core/dimension_type_helpers._read_line_pattern_ref_sig_hash()`'s positive-id branch first,
  then its negative/built-in branch plus `_read_arrowhead_ref_sig_hash()`/
  `_read_leader_arrowhead()` in a follow-up round).
  **Hash-breaking (content-driven, not an algorithm change):** new identity items are
  included in each partition's `identity_items`/`serialize_identity_items()` preimage by
  construction (matching the existing `loaded_family_types`/Area 12 precedent above), so
  `sig_hash` changes for every record across all 7 `dimension_types_*` domains; full
  re-extraction required (D-015 "hash-breaking" precedent). **Exception:** in the 3 spot
  partitions, `dim_type.leader_arrowhead_uid`/`_name` are excluded from the sig_hash
  preimage specifically (filtered out immediately before `serialize_identity_items()`) even
  though they remain in `identity_items`/`identity_basis.items` for visibility — a raw
  Revit `UniqueId` and a cosmetic name are file-local/presentation metadata (D-004; Hash
  Semantics: "Names are metadata only"), and including them would have made two files with
  a semantically-identical spot dimension type (same arrowhead style/name) hash differently
  purely because `UniqueId` is per-file-random. This is the fix for a real regression the
  original Area 7 commit shipped: the contract's `sig_hash_keys` pin only governs the
  *analysis-side* reconstruction (`core/sig_hash_builder.py`, not wired into live
  extraction), so it never actually kept the uid/name out of the canonical extracted
  `sig_hash`/`hash_v2` until this extractor-level exclusion was added (PR #412 review).
  `contracts/domain_identity_keys_v2.json`'s 7 `dimension_types_*` blocks are updated
  (`allowed_keys` only — no new `required_keys`); the 3 spot domains additionally get an
  explicit `sig_hash_keys` override (hand-patched, not regenerated via
  `tools/generate_sig_hash_policy.py`, to avoid clobbering unrelated hand-tuned notes
  on other domains — same precedent as the `loaded_family_types`/Area 12 entry above)
  pinning the analysis-side sig_hash preimage to exclude `dim_type.leader_arrowhead_uid`/
  `_name` (file-local/cosmetic) while retaining `dim_type.leader_arrowhead_sig_hash`
  itself (content-derived, already an `allowed_keys` entry before this change) —
  same silent-drift risk `tools/generate_sig_hash_policy.py` defaulting the preimage to
  every `allowed_keys` entry that the Area 10 `text_types` `leader_arrowhead` pin
  addressed. `policies/domain_sig_hash_policies.json`'s 7 `dimension_types_*` blocks are
  hand-patched to match (same clobber-avoidance rationale).
  **Schema version bump (PR #412 review, 4th round):** all 7 `dimension_types_*` blocks'
  `sig_hash_schema` is bumped from the implicit `.sig_hash.v1` to an explicit `.sig_hash.v2`
  in both `contracts/domain_identity_keys_v2.json` and the hand-patched
  `policies/domain_sig_hash_policies.json`, matching the `loaded_family_types`/Area 12
  precedent — the analysis-side hash preimage (`allowed_items`) widened for every record in
  all 7 partitions, so consumers comparing pre-/post-change extractions need the version to
  distinguish real drift from an incompatible hash-definition change.
  **Label-synthesis/join-key exclusions (PR #412 review, 4th round):**
  `tools/label_synthesis/domain_prompts/dimension_types.py`'s `_SKIP_KEYS` gains
  `dim_type.leader_arrowhead_uid`/`_name` (same treatment as the pre-existing
  `tick_mark_uid` entries) so a per-file-random UID and cosmetic name aren't presented to
  label-synthesis prompts as behavioral parameters. The 3 spot domains'
  `explicitly_excluded_items` in `policies/domain_join_key_policies.json` gain the same two
  keys, matching `text_types`' existing `leader_arrowhead_uid`/`_name` exclusions, so
  `tools/discover_join_policy.py`'s `discover`/`harsh` modes can't nominate them as join-key
  candidates. `_OPAQUE_KEYS` also gains the other 5 new `*_sig_hash` reference-hash fields
  (`leader_tick_mark_sig_hash`, `witness_line_tick_mark_sig_hash`,
  `centerline_tick_mark_sig_hash`, `interior_tick_mark_sig_hash`,
  `centerline_pattern_sig_hash`), matching the pre-existing `tick_mark_sig_hash`/
  `leader_arrowhead_sig_hash` treatment (PR #412 review, 5th round): the raw digest isn't
  interpretable, so label-synthesis prompts show a presence note instead of the value,
  keeping canonical labels dependent on the referenced configuration rather than opaque
  implementation details. **Follow-up fix (PR #413 review):** adding a key to
  `_OPAQUE_KEYS` alone doesn't make `_format_identity_items()` emit its presence note —
  the note is only produced inside the `priority_order` loop, and the separate
  remaining-items loop further down explicitly skips every `_OPAQUE_KEYS` member. Without
  also adding the 5 new keys to `priority_order`, they were silently omitted from the
  prompt entirely (worse than the original raw-digest bug: two configurations differing
  only in these fields now produced identical prompts with no signal at all). Added all 5
  to `priority_order`, verified their presence notes now emit correctly. **Second follow-up
  fix (PR #413 review):** the opaque-key presence note itself was a bare, constant literal
  (`"[present — consistent configuration]"`) for every value, so two records referencing
  genuinely different configurations (e.g. different custom tick marks) still produced
  identical prompt text for that field — losing all discriminating signal and risking
  behaviorally-distinct join-hash clusters being synthesized under the same canonical
  label. Changed the presence note to include a short, stable discriminator
  (`ref={value[:8]}` — 8 hex chars for real digests; short symbolic values like `<Solid>`
  pass through unchanged) instead of exposing the full digest or nothing at all. Applies to
  all `_OPAQUE_KEYS` entries, including the 2 pre-existing ones
  (`tick_mark_sig_hash`/`leader_arrowhead_sig_hash`), not just the 5 new Area 7 keys, since
  the underlying formatting logic is shared.
- **`loaded_family_types` domain: `structural_material_type`/`is_active` identity fields (Area 12):**
  `domains/loaded_family_types.py`'s existing per-family loop now reads
  `FamilySymbol.StructuralMaterialType` (via `canonicalize_str`, same
  read-then-canonicalize pattern already used for `cat_name_v`/`fam_name_v`)
  off `first`, the representative `FamilySymbol` for the family group — a new
  read site, not a new call pattern (the Area 12 probe observed
  `unique_value_count=1` for this property in every sampled family, i.e. it
  does not vary by type within a family, so reading a single representative
  symbol is safe). Both fields are plain-property reads confirmed `ok` 60/60
  across every probed sample
  (`audit_results/audit_11_domain_extractor_delta_step0_findings.md` §12); no
  new `ITEM_Q_*` status category is introduced — unreadable/missing still
  collapse through the file's existing `_safe_attr` + `canonicalize_*`
  convention. `CanHaveStructuralSection`/`HasThermalProperties` are
  explicitly out of scope for this change (zero-arg methods, not on the
  probe's safety allowlist, no structural-model probe coverage yet).
  `FamilySymbol.IsActive`, by contrast, **is** per-symbol (type) state — the
  same probe observed both `True`/`False` within a project — so reading it
  off a single `first` symbol would make the family-level record depend on
  `collect_types()`'s (unordered) enumeration order, violating the
  determinism invariant. Instead it is aggregated via `canonicalize_bool`
  over every symbol in the family group to a `"true"`/`"false"`/`"partial"`
  tri-state (mirroring the `any_true`/`all_true` -> `has_value_agg` pattern
  already used in this file for `lftp.has_value`), with `ITEM_Q_UNREADABLE`
  dominant over `ITEM_Q_MISSING` dominant over `ITEM_Q_OK` across the group.
  Added as `lft.structural_material_type` and `lft.is_active` in
  `identity_items`.
  **Hash-breaking:** unlike `object_styles`/`units` (which gate `sig_hash`
  through an explicit allowed-key filter), `loaded_family_types`'s `sig_hash`
  is `make_hash(serialize_identity_items(identity_items))` with no filtering
  step, so every item in `identity_items` already contributes to `sig_hash`
  by construction — adding these 2 items necessarily changes `sig_hash` for
  every record in this domain; full re-extraction required (consistent with
  the existing D-015 "hash-breaking" precedent for domain-shape changes).
  `contracts/domain_identity_keys_v2.json`'s `loaded_family_types.allowed_keys`/
  `optional` and `policies/domain_sig_hash_policies.json`'s
  `loaded_family_types.allowed_items` are updated to register both new keys
  (hand-patched, not regenerated via `tools/generate_sig_hash_policy.py`, to
  avoid clobbering unrelated hand-tuned notes on other domains — same
  precedent as the Area 9 `object_styles` entry above), so
  `validators.record_v2.validate_record_v2()` no longer reports
  `identity.key.not_allowed` for either field and the analysis-side
  `sig_hash` stage (`core/sig_hash_builder.py`) reconstructs the same
  preimage as the extractor's inline hash instead of silently dropping the 2
  new items. `policies/domain_join_key_policies.json`'s `loaded_family_types.
  explicitly_excluded_items` is updated to add `lft.structural_material_type`/
  `lft.is_active` — without this, `tools/discover_join_policy.py`'s default
  `discover`/`harsh` modes build join-key candidates from every emitted
  identity-item key not explicitly excluded, so the 2 new fields (`is_active`
  in particular — operational per-symbol usage state, not a definitional
  family property) would have been nominated as join-key candidates despite
  not being intended as such.
  **`core/sig_hash_builder.py` status-gating fix (shared, not `loaded_family_types`-only):**
  the analysis-side `sig_hash` rehash stage (`tools/run_extract_all.py`'s
  `sig_hash` stage, and `apply_sig_hash_policy_to_record()`) previously
  derived `degraded`/`blocked` status only from `required_items` quality —
  any non-required item that is still part of the hash preimage
  (`allowed_items`) but not `q=ok` was silently invisible to the recomputed
  status. This meant `lft.is_active` going `missing`/`unreadable` for a
  family (a real, expected scenario per the Area 12 probe) would have the
  extractor correctly report `status=degraded`, but the analysis-side
  rehash stage would overwrite it back to `status=ok` with `status_reasons=
  []`, since `is_active` is `allowed` but deliberately not `required` (see
  `optional_items` reasoning above). Fixed generally in
  `build_sig_hash_from_policy()`: any hashed item not in `required_items`
  that is not `q=ok` now degrades status too (reason
  `identity.incomplete:optional_not_ok:<k>`), while `blocked` remains gated
  on `required_items` only — an optional-item read failure degrades, it
  never blocks. This changes analysis-side status recomputation for every
  domain that uses `core/sig_hash_builder.py`, not just `loaded_family_types`
  — a deliberate, repo-owner-directed decision (PR review follow-up) over
  the narrower alternative of promoting `lft.is_active` to `required_items`
  for this domain only, which would have made a mid-symbol read failure
  `status=blocked`/`sig_hash=None` (harsher than the extractor's own
  `degraded`). New coverage:
  `tests/test_sig_hash_policy_builder.py::test_sig_hash_builder_degrades_when_optional_hash_item_not_ok`.
  **Schema version bump:** `sig_hash_schema` for this domain is pinned to
  `loaded_family_types.sig_hash.v2` (was the generator's implicit
  `...v1` default) in both `contracts/domain_identity_keys_v2.json` and
  `policies/domain_sig_hash_policies.json`, following the same precedent as
  the `identity` domain's D-025 `sig_hash_schema` pin — otherwise
  `tools/generate_sig_hash_policy.py` regenerating the policy file would
  silently drop back to the `v1` label despite the widened preimage,
  making pre/post-PR hashes look like the same hash definition to
  drift-detection consumers and risking a mis-reported "drift" finding.
  **`is_active` missing-quality fix:** the per-symbol aggregation now checks
  `any(...ITEM_Q_MISSING...)` (not `all(...)`) before falling through to the
  OK-only aggregation branch — a single symbol with an unreadable `IsActive`
  read no longer gets silently dropped from the readable subset (which would
  have reported `q=ok` off a partially-observed family); it now correctly
  propagates to `q=missing` and `status=degraded` for the whole record.
- **`line_styles` domain: `parent_cat.id`/`parent_cat.name` metadata fields (Area 11):**
  `domains/line_styles.py`'s existing per-subcategory loop (`sc`, iterating
  `Category.SubCategories` under `OST_Lines`) now also reads `sc.Parent`
  (the same `Category.Parent` call pattern already used by
  `domains/object_styles.py:440` for its purge-lookup helper — no new
  traversal idiom introduced) and emits `line_style.parent_cat.id`
  (`canonicalize_int` of `Id.IntegerValue`) and `line_style.parent_cat.name`
  (`canonicalize_str` of `Name`). A subcategory with no parent (a genuinely
  top-level category) canonicalizes `None` to `q=missing`/`v=null`, not
  `unreadable` — actual API read failures still canonicalize to
  `unreadable`. **Bucket placement**: added to `phase2.unknown_items`,
  matching the existing treatment of `line_style.source_element_id`/
  `source_unique_id` in this file (metadata/traceability, never in
  hash/sig/join) rather than `identity_basis.items`. This was an open
  design question per the task scoping and is being resolved here as
  "cosmetic/traceability metadata, not identity" — flagging for
  confirmation rather than assuming it's settled; if `parent_cat` should
  instead be identity-affecting, that's a separate, deliberate follow-up.
  **Non-hash-breaking:** not added to `LINE_STYLE_SEMANTIC_KEYS`, so
  `sig_hash`/`join_key` are unchanged. `contracts/domain_identity_keys_v2.json`'s
  `line_styles.allowed_keys` is intentionally left unchanged — it does not
  currently list `source_element_id`/`source_unique_id` either, since that
  registry governs `identity_basis.items`/sig-hash-policy keys, not the
  phase2 `unknown_items` bucket.
- **`object_styles` domain family: 5 per-category identity fields (Area 9):**
  `domains/object_styles.py`'s existing per-`Category` loop (shared by all 4
  `object_styles_*` partitions) now reads `Category.CanAddSubcategory`,
  `Category.HasMaterialQuantities`, and `Category.IsCuttable` off the same
  `cat_obj` already used for line weight/color/pattern reads (canonicalized
  via `core.record_v2.canonicalize_bool`), plus a new `obj_style.parent_name`
  identity item derived from the loop's already-tracked `parent` variable
  (not a fresh `cat.Parent` call, which returns missing for top-level
  categories in this API — see `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
  §9.2). `parent_name` is `q=ok`/`v=<name>` for subcategories and
  `q=missing`/`v=None` for genuinely top-level categories, per the
  record.v2 sentinel policy (`v: null` + `q: "missing"` for a confirmed
  absent value, never a bare sentinel literal) and matching the probe's
  own classification (§9.3: `ok=364; missing=279` for top-level rows) —
  not `unreadable`, which stays reserved for actual read failures. A 5th
  field, `obj_style.tab` (Model/Annotation/
  Analytical/Imported), is derived from which `extract_*`/`kind` partition
  produced the record (no new Category read) and added to the existing
  `phase2.coordination_items` bucket, matching `obj_style.category_type`/
  `obj_style.is_subcategory` — **not** hash-contributing.
  **Non-hash-breaking:** `can_add_subcategory`/`has_material_quantities`/
  `is_cuttable`/`parent_name` are added to `identity_basis.items` but not to
  `_MODEL_SEMANTIC_KEYS`/`_NON_MODEL_SEMANTIC_KEYS`, so the extractor's
  inline `sig_hash` is unchanged. `contracts/domain_identity_keys_v2.json`'s
  `object_styles_{model,annotation,analytical,imported}.allowed_keys`
  updated to register all 4 new keys; a new `sig_hash_keys` override pins
  each domain's sig-hash preimage to its pre-existing key set so the
  analysis-side `policies/domain_sig_hash_policies.json` (hand-patched, not
  regenerated, to avoid clobbering unrelated hand-tuned notes on other
  domains) doesn't silently widen via `allowed_items` defaulting from
  `allowed_keys`.
  **Open question, not resolved here:** whether `obj_style.tab` should stay
  a coordination-bucket field (current default, conservative, matches the
  `dim_type.leader_arrowhead_sig_hash` allowed-not-required precedent) or
  be promoted to a true hash-contributing identity item — flagged for Greg
  to confirm; if promoted, that is a separate, deliberate hash-breaking
  follow-up, not an accidental side effect of this change.
- **`units` domain: 6 per-spec boolean formatting flags:**
  `domains/units.py`'s existing per-spec `FormatOptions` loop now reads
  `units.use_default`, `units.use_digit_grouping`, `units.use_plus_prefix`,
  `units.suppress_leading_zeros`, `units.suppress_spaces`,
  `units.suppress_trailing_zeros` off the same `fmt` object already used
  for `accuracy`/`rounding_method`, canonicalized via
  `core.record_v2.canonicalize_bool` (newly imported). Optional (not
  required — `q=unreadable` on a `FormatOptions` read failure, mirroring
  `accuracy`/`rounding_method`, never blocks the record).
  **Hash-breaking:** all 6 keys added to `UNITS_SEMANTIC_KEYS`, so every
  `units` per-spec `sig_hash` changes going forward — these are genuine
  numeric-formatting behavior properties, not presentation/naming, so
  belong in the hash per the existing `accuracy`/`rounding_method`
  precedent. Also added to the `phase2.semantic_keys` hypothesis bucket
  (alongside `rounding_method`) so they don't fall into `unknown_items`.
  `contracts/domain_identity_keys_v2.json`'s `units.allowed_keys` and
  `policies/domain_sig_hash_policies.json`'s `units.allowed_items`
  updated accordingly (the latter hand-patched, not regenerated, to avoid
  clobbering unrelated hand-tuned notes on other domains — same approach
  as the `identity`/D-025 entry below). `required_keys` (`units.spec`,
  `units.unit_type_id`) unchanged; existing `accuracy`/`rounding_method`/
  `spec`/`symbol_type_id`/`unit_type_id` values and statuses unchanged.
- **New `units_doc` domain: doc-level formatting fields:**
  `domains/units.py` gains `extract_units_doc(doc, ctx=None)`, a second,
  independent top-level domain (`domain="units_doc"`) emitting a single
  synthetic record `record_id="units:_doc"` with
  `units_doc.decimal_symbol` / `units_doc.digit_grouping_amount` /
  `units_doc.digit_grouping_symbol`, read off `doc.GetUnits()` directly
  (`Units.DecimalSymbol`/`DigitGroupingAmount`/`DigitGroupingSymbol`) —
  document-level cardinality, not per-spec. `decimal_symbol` and
  `digit_grouping_symbol` are Revit API enums, canonicalized via
  `canonicalize_enum` (same as `units.rounding_method`);
  `digit_grouping_amount` is an int, via `canonicalize_int` (newly
  imported). All 3 fields optional/never-block, matching
  `worksets_doc.active_workset_name`'s degraded-not-blocked precedent —
  `status=degraded` (not `blocked`) with `sig_hash` still computed if
  `doc.GetUnits()` itself fails.
  **Why a separate domain, not a record folded into `domain="units"`:**
  a bare `record_id="units:_doc"` under `domain="units"` fails contract
  validation — `units.required_keys` (`units.spec`, `units.unit_type_id`,
  `block_if_any_required_not_ok`) has no conditional-required exemption,
  and a doc-level record has neither field to supply. Confirmed by direct
  test before deciding the fix (`identity.required_key.missing:units.spec`,
  `identity.required_key.missing:units.unit_type_id`,
  `identity.key.not_allowed:units.decimal_symbol` etc.). Mirrors
  `domains/worksets.py`'s `worksets`/`worksets_doc` split exactly (same
  problem, same resolution, same rationale in that file's own docstring)
  — a `worksets_doc`-style sibling domain, wired independently in
  `runner/run_dynamo.py` (`_enabled("units_doc")` →
  `units.extract_units_doc`), with its own contract entry
  (`required_keys: []`, `block_if_any_required_not_ok: false`) in
  `contracts/domain_identity_keys_v2.json`, its own hand-patched entry in
  `policies/domain_sig_hash_policies.json`, and its own entry in
  `policies/domain_join_key_policies.json` (all fields optional, no
  required discriminator — same rationale as `worksets_doc`'s policy
  notes). This was flagged as a design decision rather than a mechanical
  add (see `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
  §8.3) and confirmed with the requester before implementing.
- **`identity` domain expansion: `project_info.*` fields (D-025):**
  `domains/identity.py` now reads `doc.ProjectInformation` and adds 13 new
  identity items to its existing single `record_id="document"` record.
  Twelve are read via the `BuiltInParameter` enum (locale-independent,
  unlike matching by display name): `project_info.name`
  (`ProjectInformation.Name`), `.number`, `.status`, `.address`,
  `.issue_date`, `.client_name`, `.building_name`, `.organization_name`,
  `.organization_description`, and `.ifc_building_guid`/`.ifc_project_guid`/
  `.ifc_site_guid` (`IFC_BUILDING_GUID`/`IFC_PROJECT_GUID`/`IFC_SITE_GUID`,
  confirmed real built-ins via `tools/archetype/bip_lookup.json`). All twelve
  report `q=unreadable` if the `Parameter` object itself is absent (an
  unexpected API/document gap) and `q=missing` if it's present but blank.
  The remaining field, `.office` (Stantec's shared parameter), is read via
  its confirmed GUID (`Element.get_Parameter(Guid("6b61afc7-13eb-4af5-8b65-
  889f978af4f3"))`) rather than `LookupParameter("Office")` by display name,
  which the Revit API can otherwise resolve to an arbitrary same-named
  parameter if a project contains more than one "Office" definition; it
  reports `q=unsupported.not_applicable` (not `q=unreadable`) when the
  shared parameter definition isn't loaded — the expected, legitimate state
  on any non-Stantec-template project, not a read failure.
  **Hash-breaking:** these fields are included in `identity_items` /
  `identity_basis.items` / `sig_hash` (an explicit, documented exception to
  the "names are metadata only" default rule — see D-025 for the full
  rationale and the mitigations applied: excluded from the join-key policy,
  excluded from status/status_reasons/identity_quality computation). Every
  `identity` domain `sig_hash` changes going forward; previously captured
  values are not comparable — `identity.py`'s `sig_basis.schema`
  (`identity.sig_basis.v1` → `.v2`) and both
  `contracts/domain_identity_keys_v2.json`'s and the hand-patched
  `policies/domain_sig_hash_policies.json`'s `sig_hash_schema`
  (`identity.sig_hash.v1` → `.v2`) are bumped so a consumer comparing
  `sig_hash` across a pre-D-025 and post-D-025 export can tell the two hash
  definitions apart instead of reading the mismatch as fingerprint drift.
  `phase2.semantic_keys` ("Phase-2 behavior-defining") stays exactly the
  pre-D-025 `is_workshared`/`revit_version_number`/`revit_build` core —
  `project_info.*` is naming/label metadata, not Phase-2-semantic content —
  while a separate `sig_basis.keys_used` selector correctly lists every
  `identity_items` key actually hashed (fixing a pre-existing drift where
  `identity.revit_version_name` was hashed but missing from that list).
  Office's Address/City/State/Zip/Country/Telephone/Fax/Legal Entity
  sub-fields are deliberately NOT implemented pending confirmation of their
  exact parameter names against a real Stantec-template project (no live
  Revit/Dynamo access was available to confirm them).
  `contracts/domain_identity_keys_v2.json` and
  `policies/domain_join_key_policies.json`/`policies/domain_sig_hash_policies.json`
  updated accordingly (the latter hand-patched, not regenerated, to avoid
  clobbering unrelated hand-tuned notes on other domains). No change to
  `file_metadata.csv`, `tools/build_segment_manifest.py`, or any governance
  narrative file — confirmed zero overlap in Step 0
  (`audit_results/audit_11_domain_extractor_delta_step0_findings.md` §5).
  Refined through several PR-review rounds after the initial implementation
  (which read `Office` and the three IFC GUID fields by display name via
  `LookupParameter`, and left the hash schemas unversioned) — see DECISIONS.md
  D-025 for the full history of what changed and why.
- **Name-target bundle BI output location correction (PR3 follow-up):**
  `run_bundle_analysis_for_target()` (`tools/bundle_analysis/run_bundle_analysis.py`) now
  relocates the `--comparison-target name` leg's completed ALL-view output from the
  internal `out_dir/name/all/` staging path to a flat `out_dir/name_all/` as its final
  step (self-clearing any stale `name_all/` from a previous run first). This matches the
  Power BI model (`Fingerprint_Segmented_Bundles.vpax`)'s confirmed `pPurgeView`
  convention -- a free-text parameter spliced in as a single path segment
  (`<segment>\results\bundle_analysis\<pPurgeView>\*_combined.csv`) -- which the
  previous two-segment `name/all/` location could never satisfy.
  `tools/run_segment_orchestrator.py`'s name-leg BI merge and
  `_segment_has_name_leg_output()` marker-file check were updated to the new path. New
  `annotate_name_target_combined_files()`
  (`tools/bundle_analysis/name_projection_adapter.py`), called once per segment right
  after the name-leg `merge_bi_outputs()` call, appends `comparison_target` /
  `coverage_class` / `provenance_note` columns to every `*_combined.csv` under
  `name_all/` -- strictly additive to the existing typed columns the Power BI model's
  `Table.TransformColumnTypes` steps already read by name, never inserted/renamed/
  reordered -- so a report author can point `pPurgeView` at `name_all` and get the same
  ten filenames as `all`/`used` today, now carrying per-row name-projection provenance.
  `comparison_target=config` output (`out_dir/all`, `out_dir/used`, and under `both`,
  `out_dir/config/...`) is completely unchanged -- the relocation/annotation code paths
  are gated entirely inside the name-leg branch. See
  `audit_results/audit_10_bundle_bi_output_location_correction.md`.
- **Canonical Name Identity Projection (PR1):** a second, independent, policy-driven
  `join_hash` variant computed via the same `core/join_key_builder.build_join_key_from_policy()`
  mechanism used by the existing configuration-based `join_hash`, governed by a new,
  separate policy file (`policies/domain_name_key_policies.json`) and namespaced under
  its own `join_key_schema` (`"name_identity.join_key.v1"`, or
  `"phases.name_identity.join_key.v1.redundant"` for `phases`, whose configuration
  `join_hash` already keys off the same `phase.name` string per D-010). Stored on each
  eligible record as `join_key_name_identity` (same `{schema, hash_alg, join_hash, status,
  ...}` shape as the existing `join_key`), computed inline at export time in
  `domains/{phases,materials,text_types,compound_types,identity,phase_filters,
  line_patterns,fill_patterns,arrowheads,loaded_family_types,view_templates,
  view_filter_definitions,dimension_types}.py` (13 files; `text_types.py` is the one
  exception -- its "canonical mode" pipeline already treats `join_key`/`sig_hash` as
  post-extraction artifacts and strips them before finalizing its output, so
  `join_key_name_identity` is likewise reconstructed downstream rather than stamped
  inline for that domain only). A parallel, independent analysis-side reconstruction
  path (`core/name_key_builder.py` + `tools/apply_name_key_policy.py`) computes the
  identical value directly from already-exported `*.details.json` records -- no
  re-extraction required, since every value this projection needs (`identity_basis.items`,
  phase2 bucket items, `label.display`) is already present in existing exports today.
  Status companion field mirrors `join_key_status`'s closed vocabulary in spirit
  (`ok`/`missing_required`/`blocked`/`missing_policy` -- `"bootstrap"` does not apply,
  since this projection has no flatten-then-apply two-phase pipeline) via the new
  `core.join_key_builder.compute_projection_status()` helper.

  Eligibility is an explicit allow-list (25 of 37 `domain_join_key_policies.json`
  entries), not an inferred/derived rule -- see
  `audit_results/audit_6_name_key_step0_within_pr1.md` for the full per-domain trace.
  For 7 domains (`phases`, `materials`, `text_types`, `wall_types`, `floor_types`,
  `roof_types`, `ceiling_types`) the record's own name is already a native
  `identity_items` key. For 18 domains, the record's own name is real and reaches
  `label.display` today but was never captured as a flat `identity_items` key -- it
  either lives only in a phase2 bucket (`identity`, `phase_filters`, `line_patterns`,
  `fill_patterns` x2) or nowhere but `label.display`/`label.components.*`
  (`arrowheads`, `loaded_family_types`, `view_templates` x5, `view_filter_definitions`,
  `dimension_types` x5) -- these use a *locally widened* items list (the domain's
  existing `identity_items` plus one freshly-`make_identity_item`-wrapped value, built
  only for the name-key call) rather than touching `identity_basis.items`, `sig_hash`,
  or the existing `join_key`. 12 domains/partitions are excluded entirely: 9 have no
  name-like value at all (`units`, `line_styles`, `object_styles` x4,
  `view_category_overrides` x3), 2 have only a referenced-element name rather than
  their own (`dimension_types_spot_coordinate`/`_spot_elevation`), and 1
  (`view_filter_applications_view_templates`) has only a UID-preferring composite
  candidate, not a name string.

  Canonicalization intentionally matches what `join_hash`/`sig_hash` already do today
  (`core/record_v2.canonicalize_str`/`canonicalize_str_allow_empty`: trim + missing-check
  only) -- no case-folding or Unicode normalization is introduced. `DIMENSION_CONFIG`
  (`tools/build_segment_manifest.py`) is unchanged and does not reference this
  projection, so it cannot affect `population_hash`/segment membership. No existing
  `join_hash`, `sig_hash`, or `identity_basis.items` value changes for any domain.

- **Escalation-target file coverage (D-024):** the four files
  `generate_governance_narrative.py`'s own module docstring lists as "not
  yet consumed directly" -- `comparison_registry.csv`,
  `cross_segment_file_pairs.csv`, `pattern_reuse_summary_by_domain.csv`, and
  `project_mean_file_pair_jaccard_matrix.csv` -- are now all registered as
  `sibling_paths` (beside `--summary`'s directory, same inference the first
  two already used) and get a full `governance_evidence_map.json` artifact
  entry (`context_role`/`grain`/`can_answer`/`cannot_answer`/
  `known_limitations`, same voice as every other archive_only sibling). A
  new `_sibling_scan_fields()` helper (`tools/governance_evidence_package.py`)
  reuses D-023's `_scan_csv_file()` -- no second scanning implementation --
  to populate each entry's `columns` (name + inferred dtype) and `row_count`
  when the file is present; both fields are simply absent when the file is
  not present, since scanning a nonexistent path is meaningless. No sample
  row or cell value is ever retained. Registering
  `pattern_reuse_summary_by_domain`/`project_mean_file_pair_jaccard_matrix`
  as known siblings also excludes them from
  `inventory_export_directory_files()`'s generic undiscovered-file scan, so
  each file gets exactly one narrative home (its own `can_answer`/
  `cannot_answer`) instead of two competing descriptions of the same file.
  `pattern_reuse_distribution`/`project_fragmentation_diagnostic` gained a
  reverse `related_artifacts` link to their newly-registered siblings.
  `governance_evidence_map.json` grows from 33 to 35 artifacts. Three D-023
  tests that used these two files as stand-ins for "a generic undiscovered
  file" were updated to fictitious filenames, since those two real filenames
  no longer qualify. `docs/governance_interpretation_guide.md`'s escalation
  section now pins the exhaustive four-file list (previously only gestured
  at "another large sibling artifact") and points a reader at the new
  `columns`/`row_count` fields before writing a filtered extraction script.
  `docs/governance_question_routes.md`'s bc_to_bc/enterprise cascade note
  gained a pointer to this same escalation path for cross-BC/enterprise
  pattern-consistency questions needing file-level audit. No existing
  classification, scoring, CSV column, or narrative content changed.
  `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
  (read-only dependency). PR-review fix folded in: `pattern_reuse_summary_
  by_domain.csv`/`project_mean_file_pair_jaccard_matrix.csv`'s sibling paths
  are anchored to whichever related optional flag was actually supplied
  (`--reuse-by-client`/`--reuse-distribution`;
  `--project-fragmentation-diagnostic`/`--project-union-jaccard-matrix`/
  `--project-density-similarity-matrix`/`--project-pool-containment-matrix`),
  falling back to `--summary`'s directory -- the same anchoring
  `governance_relationships.csv` already used -- so a mixed-directory run
  does not silently report these two escalation targets as absent. The
  D-023 live-scan directories grew to include both new anchor directories
  too. Second follow-up: both anchor chains also fall back to
  `--union-inventory`/`--matrix-manifest` (written by the same
  `compare_cross_segment.py` invocation to the same `--out-dir` as their
  respective escalation target) before falling back to `--summary`'s
  directory, so a run supplying only that broader optional input still
  anchors correctly. See D-024 and `docs/governance_evidence_package.md`.
- **Live file-availability inventory (D-023):** new `governance_file_inventory.json`
  artifact in the governance evidence package, built fresh on every run by
  `inventory_export_directory_files()` (`tools/governance_evidence_package.py`):
  a `Path.glob("*.csv")` scan of the cross_segment export directory
  (`--summary`'s parent) and, when it differs, the relationship-layer output
  directory, excluding every path already tracked as an input/output/sibling
  artifact elsewhere in the package. For each undiscovered file it records
  the column header, an inferred per-column dtype (`integer`/`float`/
  `boolean`/`string`/`empty` via `_column_dtype()`), and the row count --
  never a sample row or cell value. `generate_governance_narrative.py`
  attaches a one/two-sentence narrative per file
  (`_narrative_for_inventory_entry()`): when the filename matches a
  `matrix_name` already documented in `matrix_output_manifest.csv`, it
  reuses that row's own `interpretation`/`known_limitations` text verbatim
  (the same free-text narrative pattern `compare_cross_segment.py`'s
  `add_manifest()` already uses for the registered `project_*` matrix
  artifacts); otherwise it falls back to a structural sentence built only
  from the header/row-count the scan already computed. Neither path
  hand-maintains a per-filename description, so a brand-new future export
  is picked up automatically with no follow-up code change. Confirmed
  against the real gap this closes: `pattern_reuse_summary_by_domain.csv`
  and `project_mean_file_pair_jaccard_matrix.csv` are both written by
  `compare_cross_segment.py` but were never represented as evidence-package
  artifacts before this change -- this generator's own code comments
  already noted them "deliberately not consumed," but that note lived only
  in Python, invisible to a reader of the package. `governance_brief.md`
  gains a new `## Detail-Layer File Inventory` section rendering the same
  already-scanned data (`render_file_inventory_brief_section()`, no second
  scan) -- appended after the leadership questions, entirely omitted (not
  blank-rendered) when the scan finds nothing undiscovered, and deliberately
  not interleaved into the per-domain findings sections above it. Gating:
  `governance_file_inventory.json` follows manifest/health/evidence-map/
  findings under `--emit-evidence-package`; the `governance_brief.md`
  section additionally requires `--emit-interpretation-layer`, matching the
  rest of the brief. `governance_narrative_context.md` itself is unchanged
  -- this phase adds no section there, preserving that document's existing
  `--no-emit-evidence-package` "CSV/MD outputs unaffected" guarantee.
  `governance_file_inventory` is registered as a 33rd `governance_evidence_map.json`
  artifact (`authority_level: authoritative_deterministic_evidence`, empty
  `related_artifacts` since the files it lists vary run to run). No existing
  classification, scoring, CSV column, or narrative section changed.
  `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
  (read-only dependency). See D-023 and `docs/governance_evidence_package.md`.

### Fixed
- **A persistent failure in the new orchestrator-entry `name_all/` pre-clean itself
  could escape unhandled, leaving the registry row untouched (PR3 follow-up, PR
  review, third round):** `_clear_stale_name_all_before_run()` (added in the previous
  fix below) ran before `_run_one_segment()`'s try/except machinery and its
  registry-update block. If `retry_fs_op` exhausted every retry attempt (a persistent
  lock, not just a transient one) and re-raised, the exception propagated straight out
  of `_run_one_segment()`; the `ThreadPoolExecutor` caller's generic "unhandled
  exception" handler only updates in-memory counters/`segment_results`, never
  `registry_file`, so the segment's registry row (and `bundle_provenance.csv`) were
  left at whatever they were before this run -- often `status=complete` from a prior
  successful run -- and a later non-forced run would skip it forever, silently reading
  stale Power BI output. The call is now wrapped in the same try/except pattern every
  other step in `_run_one_segment()` already uses, setting
  `step_failed = "clear_stale_name_all"` so the failure correctly reaches the
  registry-update block. See
  `audit_results/audit_10_bundle_bi_output_location_correction.md`.
- **Stale `name_all/` survives an orchestrator-level failure, and an annotation failure
  was recorded as segment success (PR3 follow-up, PR review, second round):** two
  further gaps in the previous `name_all/` staleness fix. (1) A failure in
  `run_segment_orchestrator.py`'s step 2b (name-pattern generation) or step 3 (config
  bundle, which gates step 3b even under `comparison_target=both`) skips step 3b
  entirely, so `run_bundle_analysis_for_target()` -- and its own upfront `name_all/`
  clear -- is never invoked at all; a prior successful run's `name_all/` survived
  completely untouched even though the segment is recorded as failed. New
  `_clear_stale_name_all_before_run()` helper now clears it at the very start of
  `_run_one_segment()`, before step 1 even runs, independent of which later step fails
  (or whether the segment was already skipped as complete, which never reaches this
  point at all). (2) The name-leg BI-merge block (`merge_bi_outputs()` +
  `annotate_name_target_combined_files()`) only logged a warning on exception, leaving
  `step_failed` unset -- since `_segment_has_name_leg_output()`'s "already ran" marker
  (`bundle_provenance.csv`) is written earlier by step 3b, independent of this block, a
  merge/annotate failure here still recorded `status=complete`, and a later non-forced
  run would then skip this segment forever, permanently leaving Power BI with combined
  files that are stale or missing the required `comparison_target`/`coverage_class`/
  `provenance_note` columns. This block now sets `step_failed = "bi_merge_name"` on
  exception, unlike the config leg's own (deliberately unchanged, pre-existing)
  non-fatal `bi_merge` handling, which has no equivalent completion marker to protect.
  See `audit_results/audit_10_bundle_bi_output_location_correction.md`.
- **Stale `name_all/` survives a failed name-target bundle run (PR3 follow-up, PR review):**
  `run_bundle_analysis_for_target()`'s name leg relocates its completed output to
  `out_dir/name_all/` as its last step -- if staging, mining, or provenance generation
  raised before reaching that step, a prior successful run's `name_all/` was left
  completely untouched, so Power BI (pointed at `pPurgeView=name_all`) would silently
  keep reading stale combined files from an old run even though the orchestrator marks
  the current segment run failed. `name_all/` is now cleared upfront, before staging
  starts, so a failed rerun leaves an empty/missing `name_all/` instead of misleadingly
  stale data; a successful run still repopulates it normally. See
  `audit_results/audit_10_bundle_bi_output_location_correction.md`.
- `generate_governance_narrative.py`'s within-project `score_reliability` p10/p90
  capture (the sole feeder of `score_reliability()`) was returning `Unknown` for
  all 32 rendered domains in real corpora. Root cause: it only accepted a
  `within_project` row when `a == b and _is_unscoped_segment(r,"a")` -- but
  post `business_center_label`-promotion, the genuinely enterprise-wide root
  segment for the only role that produces `within_project` pairs (`Project`) is
  routinely demoted to `run_type="registration"` by `build_segment_manifest.py`'s
  `redundant_single_child` pass (all Project-role files sitting in one business
  center), and `compare_cross_segment.py`'s `discover_within_project()` -- unlike
  `discover_cross_client()`/`discover_sibling_segments()`/`discover_parent_siblings()`,
  fixed for the same mechanism in PR #380 -- never resolves the demoted root
  through `_resolve_runnable_segment()`, so no `within_project` row for the root
  is ever emitted at all. `build_cascade()` now accepts an optional
  `segment_manifest` dict (loaded from a new optional `--segment-manifest`
  CLI flag) and, when a row's own segment isn't directly unscoped, resolves the
  true root (`f"{unit_system}|{role}"`) via `_resolve_runnable_segment()`
  (imported read-only from `compare_cross_segment.py`); a row is accepted as
  the enterprise-wide evidence source when it IS that resolved segment.
  This is not a scope widening: `redundant_single_child` only fires on
  byte-identical `population_hash`, so the resolved segment is the exact same
  file population as the (never-discovered) root, just under a more specific
  `segment_id` -- `score_reliability()`'s meaning is unchanged. A new
  `wp_p10_source`/`within_project_reliability_source` field (cascade dict /
  `governance_domain_summary.csv`) records which path fired
  (`"enterprise"` vs `"enterprise_resolved:<segment_id>"`) for auditability only.
  Verified against a real corpus: `score_reliability` goes from `Unknown` for
  all 32 rendered domains to a real value for 31 of them (`materials` stays
  `Unknown` -- no `within_project` data at all, an unrelated pre-existing gap);
  `governance_tier` in `governance_domain_summary.csv` is byte-identical
  before/after; the only other CSV column affected is `notable_anomalies`
  (now correctly surfacing the pre-existing Presence-based/Sparse reliability
  note instead of `Unknown` suppressing it). `compare_cross_segment.py` and
  `build_segment_manifest.py` are unchanged (read-only dependency).

### Added
- New `governance_bc_summary.csv` + "Business Center Analysis" narrative section
  in `tools/generate_governance_narrative.py`, structurally mirroring
  `build_client_summary()`/`render_client_section()`/`governance_client_summary.csv`
  (`build_bc_summary()`/`render_bc_section()`, one row per real business center).
  Enterprise is deliberately NOT a row in this file -- it gets its own short
  `## Enterprise Overview` section (`render_enterprise_section()`, reading the
  existing `cascade[dom]["tc"]` enterprise::enterprise reading plus the pooled
  Group 3 `eb`/`ec` means, rendered here for the first time -- still not tiered
  or anomaly-detected). Two new additive parallel accumulators feed this:
  `eb_by_bc[dom][bc_label]` (per-BC breakout of the existing pooled
  `eb[dom]`/`eb_used[dom]`) and `tc_bc_by_bc[dom][bc_label]` (per-BC breakout of
  `tc_by_scope[dom]["bc::bc"]`, which pools every real business center's own
  Template->Container reading into one bucket today) -- both leave their
  existing pooled/scoped counterparts byte-identical. New `bc_alignment_high`/
  `_moderate`/`bc_confidence_low`/`_moderate_max_files` policy thresholds
  (`policies/governance/governance_thresholds.json`) are hand-picked defaults
  value-coincident with, but a separate profile from, `client_alignment_*`/
  `client_confidence_*` -- confirmed via Step 0 that the existing client
  thresholds are hardcoded literals, not Jenks-derived (`tools/jenks_utils.py`/
  `compute_governance_thresholds.py` compute an unrelated split-detection
  threshold and are not wired into this generator at all), so this follows the
  established convention rather than introducing a new one. BC-to-BC peer
  alignment (`cross_bc_similarity_mean`) uses ALL-view as primary -- the
  OPPOSITE convention from `governance_client_summary.csv`'s used-view-primary
  `cross_client_similarity_mean` -- since `bc_to_bc` pairs compare Template/
  Container populations, not Project usage (see `_recommended_primary_view()`
  in `compare_cross_segment.py`); this is the exact bug class PR1's own
  bc_to_bc capture was written to avoid. Also fixes a real gap PR1's `bb`/
  `bb_used` accumulator had: its key was `f"{bc_a}::{bc_b}"` with no role
  component, so a Template-role bc_to_bc row and a Container-role bc_to_bc row
  for the same BC pair + domain would silently average together under one
  bucket; the key is now role-scoped (`f"{role}::{bc_a}::{bc_b}"`), caught
  while hand-verifying this PR's BC rows end-to-end against raw source values.
  `governance_client_summary.csv`, `governance_domain_summary.csv`, and every
  pre-existing narrative section are unaffected (verified via a synthetic-
  corpus trace: `build_client_summary()` output is dict-identical with/without
  BC-only comparison-type rows present, and BC-only cascade fields don't flip
  `_has_renderable_cascade_signal()` for any domain). `governance_bc_summary`
  is registered as a new artifact in the evidence-package layer
  (`governance_package_manifest.json`/`governance_evidence_map.json`,
  now 28 artifacts, up from 27) -- `governance_evidence_package.py`'s
  generic, dict-driven manifest builder required no changes; `build_evidence_map()`'s
  own hand-maintained artifact list did.
- `tools/generate_governance_narrative.py`'s `build_cascade()` now captures
  `bc_to_bc` rows (peer business-center comparisons from
  `discover_governance_chain()`'s scope-level fan-out) into the `cascade`
  dict under new keys `bb`/`bb_used`, keyed by domain then by the real
  `f"{business_center_label_a}::{business_center_label_b}"` pair (not by
  scope shape — `discover_governance_chain()` already guarantees the two
  sides are real, distinct business centers by construction, so no
  `_group1_scope_pair()`-style value-equality guard is needed at this
  layer). Uses `all_union_jaccard`/`used_union_jaccard` (population-similarity,
  directionless) rather than Group 3's `containment_a_in_b_mean`, because
  `bc_to_bc` pairs are symmetric peers, not a directed reference→target
  relationship — a single `containment_a_in_b` reading would silently
  privilege whichever business center's `segment_id` happened to sort first
  in `discover_governance_chain()`'s `combinations(sorted(sids), 2)`. Moved
  `bc_to_bc` out of `CASCADE_GROUP4_EXCLUDED_TYPES` into new
  `CASCADE_GROUP3B_TYPES`; same "captured only, not rendered/tiered/
  anomaly-detected" contract as `CASCADE_GROUP3_TYPES`. Additive only —
  `governance_domain_summary.csv`, `governance_client_summary.csv`, and the
  narrative brief are unaffected (verified byte-identical: `bb`/`bb_used`
  are not in `_CASCADE_RENDERABLE_SIGNAL_KEYS`, so
  `_has_renderable_cascade_signal()` and the domains it gates are unchanged).
  `client_cross_bc` remains excluded (separate, unresolved decision).
- `tools/generate_governance_narrative.py` now consumes two more
  `compare_cross_segment.py` outputs, narrative-side only (no changes to the
  producer). `render_union_reuse_summary()` gains an additive adoption-breadth
  cut from `pattern_reuse_summary_by_client.csv` (new optional
  `--reuse-by-client` flag) — how many of a domain's clients have at least
  one corpus-wide-reused pattern — alongside, and independent of, the
  existing distinct-pattern reuse table (unchanged; verified byte-identical
  before/after when `--reuse-by-client` is omitted).
  `pattern_reuse_summary_by_domain.csv` was evaluated and deliberately not
  wired in: its `n_patterns` duplicates the corpus-wide signal the existing
  distinct-pattern table already reports.
  A new top-level **Project Portfolio** section (new `render_project_portfolio_section()`,
  behind four new optional flags — `--project-union-jaccard-matrix`,
  `--project-density-similarity-matrix`, `--project-pool-containment-matrix`,
  `--project-fragmentation-diagnostic`) renders four paragraphs: footprint
  identity (`project_union_jaccard_matrix.csv`, ALL_DOMAINS-only, top/bottom-N
  project pairs), density similarity (`project_density_similarity_matrix.csv`,
  ALL_DOMAINS-only, cross-referenced against footprint identity for an
  explicit "same shape, different content" caveat when both matrices are
  supplied), peer-pool containment (`project_pool_containment_similarity_matrix.csv`,
  rendered as a per-project outlier list — this matrix carries no ALL_DOMAINS
  aggregate row, so the narrative means `pool_containment_similarity` across
  a project's available domains per `(project, pool_scope)` itself), and a
  fragmentation diagnostic (`project_fragmentation_diagnostic.csv`, which also
  folds in `project_mean_file_pair_jaccard_matrix.csv`'s signal via the
  diagnostic's own `exact_identity_overlap` column rather than consuming that
  matrix standalone). Each paragraph degrades to a one-line not-provided note
  when its source file is absent; the whole section is omitted only when all
  four are absent. This section is deliberately kept outside `assign_tier()`
  and `governance_domain_summary.csv` — project x project grain has no
  natural domain-tier slot, matching the existing guardrail in
  `docs/governance_generator_cross_compare_coverage.md` ("Do not use matrix
  values to override domain governance tiers directly"), not an oversight.
  `governance_domain_summary.csv`/`governance_client_summary.csv` output is
  unchanged before/after (verified byte-identical).
  `tools/governance_evidence_package.py`'s `build_evidence_map()` gains five
  matching artifact entries for the new inputs (evidence-map artifact count:
  22 → 27), required to keep the existing
  `test_manifest_input_artifact_ids_match_evidence_map_artifact_ids`
  invariant (every `governance_package_manifest.json` input must also appear
  in `governance_evidence_map.json`) satisfied — `build_package_manifest()`/
  `build_package_health()` already derive their input lists dynamically from
  `input_paths`, so they needed no code change, only the five new dict
  entries in `generate_governance_narrative.py`'s `main()`.

### Fixed
- `compare_cross_segment.py`'s `discover_cross_client()`, `discover_sibling_segments()`,
  and `discover_parent_siblings()` were silently starved once
  `business_center_label` was promoted to a real `DIMENSION_CONFIG` cut
  dimension (peer to `client_label`/`discipline_label`): a client (or
  Template rollup) whose files all sit in a single business center makes its
  blank-bc rollup segment population-identical to that business-center-scoped
  child, so `build_segment_manifest.py`'s pre-existing `redundant_single_child`
  pass correctly demotes the rollup to `run_type="registration"` (avoiding a
  duplicate-population run) — but all three discovery functions require
  `run_type in ("bundle", "reference")`, so a demoted rollup vanished from
  `cross_client`/`sibling_projects`/`parent_sibling_roles` entirely rather
  than being paired via its population-identical descendant. A single-bc
  client is now the common case in real corpora post-promotion (previously
  business_center_label was always blank, so the rollup itself was always the
  only representative). New `_redundant_child_segment_id()` reads the
  `redundant_single_child:<segment_id>` note `build_segment_manifest.py`
  already records (segment_id itself uses `|` as its internal separator, and
  the pass always runs last, so everything after the marker to end-of-string
  is taken rather than naively splitting on `|`); new
  `_resolve_runnable_segment()` follows that pointer *transitively* (a
  redundant rollup's pointed-to child can itself be redundant one level
  deeper — e.g. a Template rollup with a real, effectively-constant client
  value colliding with a further BC-scoped collision) until it reaches an
  eligible segment or a dead end, with cycle protection. All three discovery
  functions now resolve each candidate segment through this helper before
  admitting it, using the *resolved* descendant as the actual pairing unit
  while classifying role/grain from the *original* row (a blank-role,
  client-only "all governance roles" rollup can itself be redundant to a
  role-scoped descendant if that client happens to have only one role
  present; classifying by the descendant's role would misfile it as a
  genuine Project/Template sibling it was never scoped to be —
  `discover_parent_siblings()` guards this explicitly). This is not the
  "loosen the blank-bc requirement" anti-pattern `_is_client_only_project_segment()`'s
  docstring warns against: the substitute segment carries the exact same
  `population_hash` the demoted rollup would have, not a narrower slice of
  it. `build_segment_manifest.py`, `generate_governance_narrative.py`, and
  `_is_unscoped_segment()`/the Group 1/2/3 cascade logic are unchanged.
- (PR #380 Codex review) The fix above kept `segment_id_a`/`_b` as the
  resolved descendant (required — it's the only segment with real on-disk
  data), but `_build_summary_row()` also derives
  `business_center_label_a`/`_b`, `discipline_label_a`/`_b`, and
  `scope_level_a`/`_b` straight from that same segment's own manifest row —
  so a rescued row showed the resolved descendant's own (narrower) scope in
  `cross_segment_summary.csv` (e.g. `business_center_label_b="BC_C"`) instead
  of the broader, typically blank-bc population the pair was actually matched
  under. New `_stash_scope_override()`/`_scope_override_key()` record the
  *original* row's `business_center_label`/`discipline_label`/`scope_level`
  onto the resolved descendant's manifest entry, namespaced by
  `comparison_type` (the same physical segment can legitimately appear under
  its own true bc-scoped identity in a different comparison_type, e.g.
  `discover_client_cross_bc()`); `_build_summary_row()` now prefers this
  override when present. Applied to `cross_client`/`sibling_projects`
  (neither has a consumer that re-derives scope from `segment_id`, so this is
  a pure accuracy fix) but deliberately **not** to `parent_sibling_roles`:
  that comparison_type feeds `generate_governance_narrative.py`'s
  `_group1_scope_pair()`/`_is_unscoped_segment()`, which classifies
  "enterprise" scope by re-deriving structure from `segment_id_a`/`_b` itself
  (every `|`-part past index 2 must be blank) rather than trusting the label
  columns — since `segment_id` can't be overridden without breaking data
  lookup, no column override changes that already-shipped classification; it
  would only make the row internally inconsistent (columns disagreeing with
  segment_id) for no benefit. A rescued `parent_sibling_roles` row therefore
  still reports its resolved descendant's true (non-blank) scope, landing in
  whichever non-enterprise `tp_by_scope` bucket that shape implies — a real,
  if not headline, Group 1 evidence source, not a regression.
- (PR #376 review) The union-metric adoption above silently dropped all
  `within_project` evidence: `compare_cross_segment.py`'s dedicated
  within-project branch (project-internal file-pair aggregation) returns
  its summary row before reaching the normal path's `_union_similarity()`
  call, so `all_union_jaccard`/`used_union_jaccard` are never populated for
  this comparison type — only `all_pairwise_jaccard_mean`/
  `used_pairwise_jaccard_mean` ever are. `wp_all`/`wp_disc`/`wp_used`/`tw`
  in `build_cascade()`, `wp_by_client` in `build_client_summary()`, and
  `disc_domain_wp` in `render_discipline_section()` now read the union
  field first via a new `_col_union_or_pairwise()` helper, falling back to
  the pairwise-mean family only when union is blank — a no-op for
  `cross_client`/`sibling_projects` (which always populate union fields
  when they have real data, confirmed by reading the producer's non-directed
  branch) and a real value for `within_project`. Without this, every
  client's `wp_mean`/within-project coherence and every within_project-fed
  cascade signal (`wp_all`, `tw`, the `phases` domain guidance trigger,
  reliability-note text) silently read as unavailable against real exports,
  despite the underlying pairwise data being present in the CSV the whole
  time.
- (PR #376 review, P2) `render_discipline_section()`'s within-project loop
  has no `governance_role_a` gate — unlike `wp_by_client` in
  `build_client_summary()`, it also processes discipline-scoped Template/
  Container/Generic standards segments self-compared for internal
  consistency, not just Project rows. The union-metric adoption above made
  the bare/primary value used-view unconditionally, but
  `_recommended_primary_view()` only makes used-view primary for Project
  targets — a Template/Container/Generic segment can have no used-view
  membership at all (used-view is annotation-only for those roles), which
  silently dropped that discipline's real all-view coherence from the
  section entirely. Primary is now picked per row by
  `governance_role_a == "Project"`: used-view for Project rows (unchanged),
  all-view for every other role, matching this section's pre-union-adoption
  behavior for non-Project rows. The all-view secondary (`disc_domain_wp_all`)
  is only populated for Project rows — there is no meaningful secondary to
  show for a role where used-view isn't primary in the first place.
- (PR #376 review, second P2 finding) The fix above stores the all-view
  value into `domain_means` for non-Project rows, but the rendered
  "Mean within-population coherence" sentence still unconditionally said
  `used-view, active practice` regardless of what actually fed that
  discipline's aggregate — misstating configured standards evidence as
  active usage for a Template/Container/Generic-only discipline. A new
  per-discipline `disc_role_mix` tracks whether a discipline's rows were
  Project-only, non-Project-only, or both; the label is now
  `used-view, active practice` (Project-only), `all-view, configured
  standards` (non-Project-only), or a neutral
  `mixed used-view (Project rows) / all-view (standards rows)` for a
  discipline fed by both.

### Changed
- `tools/generate_governance_narrative.py`'s cross-client/within-project
  metrics (`xc_by_client`/`wp_by_client`/`xc_dom_by_client` in
  `build_client_summary()`, `disc_domain_wp` in `render_discipline_section()`,
  and `xc`/`wp_all`/`wp_used`/`tw` in `build_cascade()`) now read
  `compare_cross_segment.py`'s population-union metrics
  (`all_union_jaccard`/`used_union_jaccard`) instead of the pairwise-file
  mean (`all_pairwise_jaccard_mean`, previously read via the canonical
  `jaccard_mean` alias). This is a genuine interpretation change, not a
  rename follow-up: union jaccard measures footprint overlap between two
  populations' full file unions, independent of `n_files_a x n_files_b`,
  which is materially different from (and more resistant to file-count
  skew than) a mean of pairwise file comparisons — the exact problem
  `all_union_jaccard`/`used_union_jaccard` were added to
  `compare_cross_segment.py` to solve.
  - For `cross_client`/`sibling_projects`/`within_project`(Project role),
    `compare_cross_segment.py`'s own `_recommended_primary_view()` states
    used-view is primary ("active practice") for these types — the
    **opposite** convention from `tc`/`cp`/`tp` (Group 1 governance chain),
    where all-view is primary and `_used` is the secondary diagnostic. So
    `xc_mean`/`wp_mean`/`d["xc"]` (the bare, tier-driving names) are now
    sourced from `used_union_jaccard`; a new secondary value (`xc_mean_all`/
    `wp_mean_all`/`d["xc_all"]`, and `cross_client_convergence_all_view`/
    `cross_client_similarity_mean_all_view`/`within_project_coherence_all_view`
    in the CSV outputs) carries the all-view union metric as context. This
    changes `CLIENT_ALIGNMENT_HIGH`/`CLIENT_ALIGNMENT_MODERATE` tier
    assignment, `XC_STRONG_CONVERGENCE`/`cross_client_convergence` findings,
    `CLIENT_COHERENCE_LOW`/`low_client_coherence` findings, and onboarding
    profile reads (`_client_onboarding_profile()`) for any client/domain with
    a real gap between pairwise-mean and used-view-union scores — real tier
    movement is expected, not a bug.
  - `wp_all[dom]`/`wp_used[dom]` in `build_cascade()` were already a genuine
    all-view/used-view pair (unlike `xc`, which had no used companion before
    this change) — only the metric family swapped (pairwise mean → union);
    which side is "all" and which is "used" is unchanged, since
    `passive_indicator`'s `(all - used)` delta depends on that assignment
    staying fixed. `tw[dom]` (Template self-comparison) shares `wp_all`'s `v`
    and is therefore also now union-sourced, still all-view.
  - `CASCADE_GROUP4_EXCLUDED_TYPES["client_cross_bc"]`'s docstring updated:
    the "provisional pending a population-union aggregation fix" text was
    stale (the fix has shipped and is now adopted for the other three
    types) — `client_cross_bc` itself remains unrouted into any
    cascade/client-summary accumulator; that is still a separate, unresolved
    design decision, not something this change does implicitly.
  - `_SUMMARY_COL_ALIASES` gains 6 new canonical entries for the union
    fields (`all_union_jaccard`, `used_union_jaccard`,
    `all_union_containment_a_in_b`, `all_union_containment_b_in_a`,
    `used_union_containment_a_in_b`, `used_union_containment_b_in_a`), read
    via `_col()` like every other field in this file rather than a raw
    `row.get()` bypass. `all_pairwise_*`/`used_pairwise_*` fields and their
    aliases are unchanged — this is an addition of what else is read, not a
    removal.

### Fixed
- Six correctness bugs in the `comparison_status="blocked"` row-emission
  path added earlier in this changeset (found via code review), all in
  `tools/compare_cross_segment.py`:
  - **Blocked rows reported the populated side's bundle availability as
    false.** Both blocked-row builders (`run_pair()` and
    `_build_pooled_row()`) hardcoded `all_has_bundles_*`/
    `used_has_bundles_*` to `"false"` for every side, even when the
    populated side (or, for pooled rows, one or more pool members) actually
    had `bundle_membership.csv` output for the domain. These columns
    document per-side output *availability*, not a similarity score, so
    they're now computed from `load_bundle_join_hash_set()` per side (the
    pool side aggregated across every `pool_sids` member, same as the
    non-blocked path) — only the genuinely-empty side/pool reads `false`.
    The shared-overlap bucket counts stay at `0` either way, since there's
    no trustworthy shared set when one side has zero files.
  - **Lineage-emptied pools were reported as blocked instead of skipped.**
    `_emit_for_groups()` excludes any pool member in the focal segment's own
    `parent_segment_id` lineage before calling `_build_pooled_row()` — for a
    2-member bc/client pool group where the other member is the focal's own
    ancestor or descendant, this leaves `pool_sids` empty. The zero-inventory
    blocked-row branch doesn't distinguish "no eligible pool exists" from
    "the pool's inventory couldn't be read," so it emitted a
    `comparison_status="blocked", n_files_pool=0` row for every one of the
    focal's own domains — a comparison that was never eligible in the first
    place, inflating blocked-pool counts. Now skipped entirely (`continue`)
    as soon as lineage filtering leaves `pool_sids` empty, before any domain
    is even considered.
  - **Blocked rows corrupted the populated side's own counts.** `run_pair()`'s
    blocked-row builder hardcoded `n_patterns_a`/`n_patterns_b`/
    `n_unique_patterns_a`/`n_unique_patterns_b` to `0` for *both* sides, even
    when only one side was actually empty and `n_a`/`n_b` (the populated
    side's real counts) were already computed. Now uses the real per-side
    counts; only the genuinely-empty side reads `0`.
  - **Blocked directed references produced false delta findings.** Before
    this changeset, a directed comparison with a zero-file reference side
    returned `None` from `run_pair()`, so `main()`'s delta-generation block
    (for `DELTA_DIRECTED_TYPES`) never ran. Now that a blocked comparison
    returns a real row, that block *did* run — with an empty `ref_union`,
    `tgt_union - ref_union` equals `tgt_union`, so every target join_hash
    was written to `cross_segment_delta.csv` as if the target had invented
    it locally, when the true story is "reference unknown," not "target
    drifted." Delta generation now skips rows with
    `comparison_status == "blocked"`.
  - **Pool-only domains were never scheduled for an empty focal segment.**
    `run_pooled_comparison()` iterated only `discover_domains_for_segment
    (focal_sid)` when deciding which domains to run `_build_pooled_row()`
    for. A focal segment with zero inventory for a domain that exists only
    in its pool (`n_files_focal=0, n_files_pool>0` — precisely the case the
    blocked-row path exists to report) was therefore never scheduled at all
    for that domain, silently dropping the row instead of reporting it
    blocked. Domain discovery now unions the focal segment's domains with
    every pool member's domains (memoized per segment_id across the whole
    call, since the same segment recurs across the three pool grains).
- `tools/compare_cross_segment.py`'s `make_comparison_run_id()` now includes
  `comparison_type` in its hash input (`seg_a|seg_b|comparison_type|
  executed_utc`, was `seg_a|seg_b|executed_utc`). An enterprise (Stantec/
  `"0000"`) standard and a real-BC standard of the same role that share a
  `parent_segment_id` get paired both as `sibling_templates`/
  `sibling_containers` (`discover_sibling_segments()`, symmetric Jaccard)
  and as `enterprise_to_bc` (`discover_governance_chain()`, directed
  reference-union containment) — genuinely distinct measurements of the
  same two segments, not duplicates (unlike the `cross_client`/`bc_to_bc`/
  `client_cross_bc` case `drop_legacy_siblings_covered_by_peer_comparisons()`
  already handles, which are symmetric duplicates and correctly get the
  sibling row dropped). Because `discover_sibling_segments()`'s sorted-ID
  pairing and `discover_governance_chain()`'s enterprise-then-bc pairing can
  land on the identical `(seg_a, seg_b)` orientation (whenever the
  enterprise segment's generated ID happens to sort first, e.g. `"0000"`
  segments), both rows previously collided on the same `comparison_run_id`
  even though `cross_segment_file_pairs.csv` carries no `comparison_type`
  column to disambiguate by. `enterprise_to_client` has the identical
  structural risk (same shared-parent/same-role precondition) and is fixed
  by the same change. All callers within `compare_cross_segment.py` that
  build a `comparison_run_id` for a `run_pair()`-style comparison now pass
  their `comparison_type` through; the two `_build_pooled_row()` pooled-
  comparison call sites are unaffected (their second `make_comparison_run_id`
  argument already embeds `pool_scope`, so there is no analogous collision
  there). This changes every `comparison_run_id` value produced by the tool
  (the hash input format changed for all rows, not just the previously-
  colliding ones) — `comparison_run_id` is a per-run bookkeeping ID
  (embeds `executed_utc` already, so never reproducible across runs
  regardless), not one of the record.v2 identity/fingerprint hashes D-002
  protects, so no `DECISIONS.md` entry is needed.

### Changed
- `tools/compare_cross_segment.py` cardinality and aggregation semantics are
  now explicit. Adds non-suppressive `comparison_status` (`ok`/`degraded`/
  `blocked`) computed purely from file counts on each side of a comparison
  (`blocked` = zero readable file inventory on a required side; `degraded` =
  exactly one side has a single file while the other has more; everything
  else, including a symmetric 1×1 comparison, is `ok`) to `cross_segment_
  summary.csv` and `cross_segment_pooled.csv`. This is a genuine behavior
  change: a comparison where either side has zero files previously produced
  no row at all (`run_pair()`'s shared `min_patterns` gate silently returned
  `None`); it now emits a real, schema-complete row with `comparison_status
  = "blocked"` and blank (not zero-valued) similarity fields instead.
  `n_files_a >= 1 and n_files_b >= 1` comparisons that used to be silently
  suppressed via the same path are unaffected — that "can we say anything at
  all" pattern-count gate (`min_patterns`, default 3, unrelated to file
  count) is untouched and still silently suppresses rows below it, by
  design (out of scope for this change).
  - Purely descriptive `cardinality_shape` (`single_a`/`single_b`/`balanced`/
    `imbalanced`) and `file_count_ratio` siblings added alongside — neither
    ever gates output; `balanced` classifies equal file counts on both
    sides, including 1×1, as symmetric rather than narrow.
  - `inventory_status_a`/`inventory_status_b` (populated only on `blocked`
    rows) distinguish a confirmed-empty domain (segment read succeeded,
    zero patterns — `no_patterns`) from a segment/domain that couldn't be
    read at all (`missing_domain_patterns`), reusing the existing
    `_segment_domain_source_status()` helper. Both have zero files but are
    not the same fact.
  - Adds population-union metrics for every comparison routed through
    `compare_symmetric_file()` (this now covers the `bc_to_bc` and
    `client_cross_bc` comparison types PR2/#373 left at provisional status
    specifically because of this imbalance problem): `all_union_jaccard`,
    `all_union_containment_a_in_b`, `all_union_containment_b_in_a`, and
    `used_*` counterparts — Jaccard/containment between each side's full
    file-union footprint, independent of `n_files_a × n_files_b`. These
    answer "how similar are these two populations", a different question
    from the existing pairwise mean ("what's the mean of all file pairs"),
    and are stable when a side gains an exact-duplicate file where the
    pairwise mean is not.
  - Renames `all_jaccard_mean` → `all_pairwise_jaccard_mean`,
    `used_jaccard_mean` → `used_pairwise_jaccard_mean`,
    `all_containment_a_in_b_mean` → `all_pairwise_containment_a_in_b_mean`
    (and the `b_in_a`/`used_*` counterparts) in `cross_segment_summary.csv`,
    and adds `aggregation_method = "cartesian_file_pair_mean"` (symmetric
    rows only) to label them explicitly. The underlying computation is
    unchanged for symmetric rows; directed rows now populate the same
    renamed columns via the same reference-union-vs-per-target-file-
    distribution computation they always used (unchanged) — `reference_
    aggregation`/`target_aggregation`/`n_reference_files` make that
    directed-specific meaning explicit per row instead of requiring the
    reader to already know it from `comparison_type`.
  - **Breaking for downstream consumers of the renamed fields** — this was
    a deliberate correctness-over-compatibility call, not an oversight.
    `tools/compare_governance_populations.py` imports `compare_symmetric_
    file()`/`compare_directed_file()` directly and spreads their return
    dict into its own rows (`row.update(metrics)`); it will silently read
    blank values for `all_jaccard_mean`/`all_containment_a_in_b_mean`/
    `all_containment_b_in_a_mean` until migrated. `tools/generate_
    governance_narrative.py` reads the pre-rename names at ~15 call sites
    via its `_SUMMARY_COL_ALIASES`-style alias helper; it will also
    silently read blank values for the same three field families until
    migrated. Neither is touched by this change (out of scope; migrate in
    a follow-up PR) — 40 tests across `tests/test_generate_governance_
    narrative_brief.py`, `tests/test_generate_governance_narrative_
    evidence_package.py`, `tests/test_generate_governance_narrative_
    policy.py`, and `tests/test_compare_governance_populations.py` now fail
    as a direct, documented consequence and are left failing pending that
    migration.
  - Adds directed-reference heterogeneity diagnostics:
    `reference_union_pattern_count`, `reference_intersection_pattern_count`,
    `reference_core_share` (= intersection/union across every file on the
    reference side) — reveals whether a multi-file reference (e.g. a
    Template segment backed by several files) is a coherent standard or a
    broad union of conflicting sources, independent of how well any target
    matches it. Degrades to `1.0` for a single-file reference — not an
    artificial failure.
  - Adds side-balanced summaries for symmetric comparisons:
    `all_a_file_mean_similarity_to_b_mean/min`,
    `all_b_file_mean_similarity_to_a_mean/min` — each A-file's own mean
    Jaccard to every B file, then mean/min of those per-file means (and the
    inverse for B), exposing directional population experience that a
    single pooled mean/min hides in an imbalanced comparison.
  - `docs/cross_segment_comparison.md` updated to match; also corrects two
    stale claims (a `n_pairs ≤ 50` row-count suppression threshold for
    `cross_segment_file_pairs.csv` that does not exist anywhere in the
    code).
- `tools/compare_cross_segment.py` organizational scope is now derived from
  explicit, literal `client_label`/`business_center_label` values instead of
  blank inference, matching `build_segment_manifest.py`'s explicit-metadata
  contract: **enterprise** (`client_label == "Stantec"`,
  `business_center_label == "0000"`), **business_center** (`client_label ==
  "Stantec"`, a real `business_center_label`), **client_business_center** (a
  real external `client_label`, a real `business_center_label`) via the
  rewritten `_scope_level()`. A row where either dimension isn't cut at all
  (blank) is a roll-up pooling multiple real scopes and is handled per
  comparison type (`_is_client_wide_rollup()`), not classified by
  `_scope_level()` itself.
  - `_normalize_bc_label()` no longer folds `"0000"`/`"BC_0000"` to blank —
    `"0000"` now flows through as the literal Enterprise business-center
    value everywhere this file uses `business_center_label` (`"BC_0000"`/any
    case spelling variant canonicalizes to the same literal `"0000"` rather
    than being left as a separately-fragmenting literal — see the "Fixed"
    entry below). This was a live inconsistency left in place by the
    segment-manifest explicit-contract change: since `client_label` is now
    always populated (literally
    `"Stantec"` for internal work, never blank),
    `discover_governance_chain()`'s prior blank-based scope inference meant
    `_scope_level()` could never return `"enterprise"` for real data at all
    (an internal-work row's populated `client_label` always won the old
    3-way branch before blank-derived `"bc"`/`"client"` were ever reached) —
    `enterprise_to_project`/`enterprise_to_bc`/`enterprise_to_client` pairs
    were silently produced for zero pairs against current data. Fixed.
  - `discover_governance_chain()`'s `_key()` now folds `business_center_label`
    into its client-populated bucket too — without this, an Enterprise
    Template (`Stantec`/`0000`) and a specific business center's Template
    (`Stantec`/`2270`) collapsed into one `client=="Stantec"` bucket and
    incorrectly produced `template_to_project`/`template_to_container` pairs
    against each other's downstream population.
  - `_disc_match()`'s blank-discipline wildcard is removed — discipline-gated
    comparisons now require an exact `discipline_label` match, full stop.
  - `discover_cross_client()`'s grain now includes `discipline_label`
    (previously excluded any discipline-scoped Project segment from
    `cross_client` entirely); grouping key is now `(client_label,
    unit_system, discipline_label)`.
  - `SUMMARY_FIELDS` gains `scope_level_a`/`scope_level_b`; `POOLED_FIELDS`
    gains `scope_level` (empty string for roll-up rows).
  - `run_pooled_comparison()`'s `bc_groups` pooling (`pool_scope == "bc"`)
    calls `_bc_of()`, which calls the now-fixed `_normalize_bc_label()`
    directly (no independent re-implementation) — so this same fix also
    stops silently excluding Enterprise-scoped (`"0000"`) rows from
    bc-scoped pooling entirely (previously `if bc:` was always False for
    them, since `_bc_of()` folded `"0000"` to blank; they simply never
    entered `bc_groups`). New coverage:
    `test_pooled_comparison_bc_scope_pools_enterprise_0000_segments`.
- Cardinality/aggregation semantics (`data_sufficient` gate, pairwise-mean
  computation, `jaccard_mean`/`containment_*_mean` field naming) are
  unchanged by this entry — `cross_client` and the new `client_cross_bc`
  comparison type reuse the existing metrics functions as-is and remain
  pairwise/provisional pending a population-union aggregation fix.

### Design notes
- `pool_scope` (`run_pooled_comparison()`) and `scope_level` (`_scope_level()`)
  are intentionally distinct — the former describes which axis a sibling pool
  is grouped along, the latter describes a segment's organizational position.
  Both now derive from the same corrected `_normalize_bc_label()`, so they no
  longer risk drifting apart on how `business_center_label` is interpreted
  (verified: `_bc_of()` calls the shared function directly, no independent
  re-implementation). No unification needed; documented at the `pool_scope`
  definition site to prevent future confusion.

### Fixed
- (PR #373 review) `_normalize_bc_label()` now canonicalizes `"BC_0000"`/any
  case spelling to the literal `"0000"` instead of leaving it as a separate,
  fragmenting literal — `_is_enterprise_bc()` only ever compared against
  `"0000"` exactly, so a row spelled `"BC_0000"` (a real spelling used
  elsewhere in the pipeline, e.g. the extraction completeness gate) was
  classified `business_center` instead of `enterprise`, omitting the
  intended `enterprise_to_project`/`enterprise_to_bc` fan-out and able to
  emit a bogus `bc_to_bc` peer pairing between the enterprise segment and a
  real business center. Reuses the shared `na_token.
  ENTERPRISE_BC_BOOKKEEPING_TOKENS` set (re-imported) rather than
  reimplementing it.
- (PR #373 review) `drop_legacy_sibling_projects_covered_by_cross_client()`
  renamed to `drop_legacy_siblings_covered_by_peer_comparisons()` and
  generalized: it previously only dropped a `sibling_projects` row covered
  by a `cross_client` pair. The new `bc_to_bc`/`client_cross_bc` types have
  the identical collision risk against `sibling_templates`/
  `sibling_containers`/`sibling_projects` (same-role BC-scoped segments, or
  a client's per-BC segments, can share an immediate `parent_segment_id`
  with what a purpose-built peer function already pairs) — both would have
  collided on `comparison_run_id` and double-counted the pair in
  `cross_segment_file_pairs.csv`, which carries no `comparison_type` column.
  The generalized function drops any `sibling_*` row for a pair any of
  `cross_client`/`bc_to_bc`/`client_cross_bc` already covers.
- (PR #373 review) `bc_to_bc` and `client_cross_bc` registered in
  `generate_governance_narrative.py`'s `CASCADE_GROUP4_EXCLUDED_TYPES`
  (same-role/same-client peer comparison, no cascade treatment designed
  yet — same reason class as `sibling_templates`/`sibling_containers`).
  Without this, any default run where these types fire fed
  `_warn_unrecognized_comparison_types()` an unrecognized value. This is a
  narrow, additive exception to keeping `generate_governance_narrative.py`
  out of scope for this PR — registering a type name in the existing
  documented-exclusion registry, not new narrative/cascade logic.

### Added
- New `bc_to_bc` comparison type in `tools/compare_cross_segment.py`
  (`discover_governance_chain()`, fires under `--governance-chain`): pairs
  every combination of real business centers' same-role, same-discipline
  Template/Container/Project populations against each other (peer-to-peer,
  not routed through `parent_segment_id`/collection_label).
- New `client_cross_bc` comparison type in `tools/compare_cross_segment.py`
  (`discover_client_cross_bc()`, fires under `--cross-client`): for a real
  client whose work spans more than one real business center, pairs that
  client's per-business-center (`client_business_center` scope) populations
  against each other for every business-center pair it actually appears in
  (derived from the data, not a fixed two-BC comparison), matched by
  `client_label`, `governance_role`, `discipline_label`, `unit_system`.
  Provisional metric pending PR3's population-union aggregation fix, same as
  `cross_client`.
- New `cross_client` comparison type in `tools/compare_cross_segment.py`
  (`discover_cross_client()`, `--cross-client` CLI flag, default-on): pairs
  each client's own broadest (client-only-scoped) Project population against
  every other client's, within the same unit_system, independent of segment
  lineage. Fixes `cross_client_convergence` (governance_domain_summary.csv)
  and `cross_client_similarity_mean` (governance_client_summary.csv) being
  blank for every row -- the only prior source for those columns was
  `sibling_projects`, which only pairs Project segments sharing an immediate
  `parent_segment_id` and is additionally sector-gated (both clients must be
  tagged `healthcare` in `policies/client_sector.csv`) in
  `build_cascade()`'s `xc` accumulation. `cross_client` has no shared-parent
  requirement and no hardcoded sector gate (sector filtering, where wanted,
  is left to downstream consumers). `tools/generate_governance_narrative.py`'s
  `build_cascade()` and `build_client_summary()` now also accumulate `xc`/
  `xc_mean` from `cross_client` rows alongside the existing `sibling_projects`
  source. Jaccard-based, undirected (mirrors `sibling_projects`'s scoring
  path); no governance-state rows are written for it (not in
  `GOVERNANCE_STATE_DIRECTED_TYPES`), matching `sibling_projects`.
  `build_client_summary()`'s `xc_by_client`/`xc_dom_by_client` read
  `client_label_a`/`client_label_b` directly rather than positionally parsing
  `segment_id` (the old `len(pa) == 3` assumption only held for the
  `unit|role|client`-shaped IDs `build_segment_manifest.py` happens to emit
  for a client-only Project segment; `discover_cross_client()` places no such
  constraint on `segment_id` shape), with an explicit `ca != cb` guard to
  preserve the existing within-client-sibling exclusion the old check
  enforced incidentally. `client_files`'s `n_project_files` backfill now also
  recognizes `cross_client` rows (previously `sibling_projects`-only), so a
  client discoverable only via a `cross_client` row no longer falsely reports
  `n_project_files=0`. New `drop_legacy_sibling_projects_covered_by_cross_client()`
  in `compare_cross_segment.py` drops a `sibling_projects` pair when
  `cross_client` already covers the identical two segments (they can share an
  immediate `parent_segment_id`, since `discover_sibling_segments()` groups
  purely by parent/role/unit) -- otherwise both would double-count that one
  pair in `xc`/`xc_by_client` and collide on `comparison_run_id`
  (`make_comparison_run_id()` hashes only segment IDs + timestamp, not
  comparison_type -- a broader, pre-existing characteristic of that
  identifier, not changed here). `cross_client`'s contribution to `xc`
  (`build_cascade()`) is gated to both-healthcare pairs, matching
  `sibling_projects`'s existing gate -- `xc` is documented and consumed
  elsewhere (client-tier "Non-comparable (different sector)" logic) as a
  healthcare-cohort metric; `discover_cross_client()` itself is unaffected and
  still emits every client pair into `cross_segment_summary.csv` regardless
  of sector. `xc_by_client`/`xc_dom_by_client` (`build_client_summary()`,
  feeding `cross_client_similarity_mean`) gain a softer, consumer-appropriate
  exclusion -- a pair is dropped only when a side has a CONFIRMED
  non-healthcare sector (`sector not in ("unknown", "healthcare")`), matching
  this function's own definition of "comparable"; an unclassified client
  still counts. This closes a pre-existing gap (this rollup never filtered by
  sector for either source type) that `cross_client` being default-on and
  pairing every client made routinely consequential. `main()` in
  `compare_cross_segment.py` now applies `--segment-a`/`--segment-b`
  filtering *before*
  `drop_legacy_sibling_projects_covered_by_cross_client()` rather than after:
  `discover_sibling_segments()` orders its pair by sorted segment ID while
  `discover_cross_client()` orders by sorted client label, so the surviving
  `cross_client` row replacing a dropped `sibling_projects` row can be in the
  reverse orientation -- which the position-sensitive segment filters would
  then also reject, making a scoped run silently report zero pairs for
  segments that do have a comparison. No effect on the default (unscoped)
  path.
- `governance_domain_summary.csv` gains `container_to_project_scoped` /
  `container_to_project_scoped_pair` columns in
  `tools/generate_governance_narrative.py`. Root cause: `container_to_project`
  (`cp`) is populated only from rows where BOTH sides are the fully unscoped
  ("enterprise::enterprise") segment -- real Project segments are almost never
  fully unscoped, so `cp` stayed empty for effectively every domain even
  though real, `data_sufficient == "true"` container_to_project evidence
  existed at other scope levels (`cp_by_scope`, already computed but never
  surfaced in this CSV). The new columns report the mean of the largest
  (most rows) non-enterprise, `data_sufficient` scope_pair bucket, plus which
  scope_pair it came from, and are populated only when `container_to_project`
  itself is empty -- `container_to_project`'s own enterprise-only meaning is
  unchanged, so this never competes with or is mistaken for enterprise-level
  evidence (same posture as `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE`).
  Sourced from a new, separate accumulator (`cp_by_scope_suff`) rather than a
  filtered view of `cp_by_scope`, so `_has_group1_bc_pooled_evidence()`/
  `render_group1_scope_section()` (existing `cp_by_scope` consumers) are
  unaffected. No other comparison type's `data_sufficient` handling changed.
  `_TIER_DRIVER_SUPPORT_FIELDS` (the shared list of `governance_domain_summary.csv`
  columns every tier-based `governance_findings.json` finding's `support[].fields`
  references) now includes both new columns, so a `missing_or_degraded_evidence`
  finding for a domain whose only evidence is the scoped fallback (i.e.
  `container_to_project` itself is blank) still points a consumer at the
  actual populated value instead of only the blank primary column.
  `build_client_summary()`'s `xc_by_client` (feeding `cross_client_similarity_mean`)
  now also skips rows for domains in `EXCLUDED_FROM_SCORING`, matching the
  gate `xc_dom_by_client` (right below it) and `build_cascade()`'s own
  per-domain `xc` already apply -- previously a `cross_client` row for a
  policy-excluded domain (e.g. `view_templates_renderings_drafting`) could
  still drive a client's overall alignment tier, disagreeing with the rest
  of the scoring policy. Pre-existing gap for `sibling_projects` too; made
  routinely reachable by `cross_client` pairing every client for every
  domain by default.
- `tools/generate_governance_narrative.py` now emits an interpretation/
  routing layer: `docs/governance_interpretation_guide.md` (stable,
  package-type-level -- what each metric/tier means, comparability rules,
  missing-value semantics, authority ordering, known bad inferences),
  `docs/governance_question_routes.md` (a candidate question-route catalog,
  all routes at "candidate" maturity, following the discovery scaffold in
  the design-reference `llm_evidence_framework` repo's
  `discovery/question_route_discovery.md`), and `governance_brief.md` (the
  one new generated, per-run artifact -- a narrower digest built by a new
  `render_governance_brief()`, which consumes the already-computed findings
  list and package health directly, computing nothing new). New CLI flags
  `--emit-interpretation-layer`/`--no-emit-interpretation-layer` (default:
  on) control `governance_brief.md` only, independently of
  `--emit-evidence-package`. `governance_evidence_map.json` grows from 19 to
  22 artifacts; `governance_narrative_context.md`'s authority header gains
  pointers to all three new artifacts. No existing classification, scoring,
  or CSV column changed. See D-022 and `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py`'s governance thresholds, excluded/
  passive-inheritance-risk domain lists, per-domain guidance text, and
  client-onboarding interpretation thresholds are now loaded from JSON policy
  profiles under `policies/governance/` (`governance_thresholds.json`,
  `domain_governance_policy.json`, `client_onboarding_policy.json`,
  `finding_rules.json`), via a new sibling module `tools/governance_policy.py`
  (generic load/fallback loader; no governance business content of its own).
  `--policy-dir` (accepted but inert since the Phase 1 evidence-package work)
  now defaults to `policies/governance/` and is actually read: a new
  `apply_governance_policy()` reassigns every module-level threshold/domain-
  policy constant this file's existing functions already read as plain
  globals, so no existing function body changed -- only the source of each
  constant's value did. The shipped JSON files reproduce this generator's
  pre-externalization Python literals value-for-value, so no existing
  invocation's classification output changes by default (locked in by a
  regression test running the CLI twice -- default vs. explicit
  `--policy-dir policies/governance/` -- and asserting byte-identical
  `governance_domain_summary.csv`). A profile file missing from `--policy-dir`
  falls back, per file, to this generator's own built-in default for that
  profile only, reported in `governance_package_health.json`'s new
  `policy_load_status`/a `governance_policy_profile_defaulted` warning
  (degrades `overall_status` to `degraded`) and in
  `governance_package_manifest.json`'s `policy_profiles.profiles` (resolved
  `profile_id`/`schema_version`/`source` per profile). See D-021 and
  `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py` now emits `governance_findings.json`:
  structured, rule-derived governance findings (`baseline_candidate`,
  `strong_baseline_candidate`, `local_review_required`, `high_fragmentation`,
  `active_local_practice`, `cross_client_convergence`, `low_client_coherence`,
  `passive_inheritance_risk`, `missing_or_degraded_evidence`,
  `leadership_question`) with epistemic provenance (`origin`/`fidelity`/
  `authority_level`/`limits`) and `support[]` references back to specific
  `governance_domain_summary.csv`/`governance_client_summary.csv` rows and
  fields, via a new `build_structured_findings()`. `render_findings_and_recommendations()`
  now consumes the same structured findings instead of independently
  recomputing the classification, via a new shared
  `_classify_domains_for_findings()`, so the narrative's prose and the JSON
  findings can no longer disagree. Leadership questions are marked
  `status: question_not_claim` / `authority_level: convenience_summary`,
  distinct from evidence findings (`status: supported`). No existing CSV
  column, classification/scoring logic, or threshold changed. See D-020 and
  `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py` now emits a governance evidence-package
  layer alongside its existing outputs: `governance_package_manifest.json`
  (provenance -- which inputs were provided/found, which outputs were written and
  their sizes, comparison_run_id(s)/executed_utc observed in the loaded rows),
  `governance_package_health.json` (schema detection, used-view fallback,
  comparison_type coverage, blocking conditions, warnings), and
  `governance_evidence_map.json` (one entry per artifact -- the CSVs the
  generator reads, two sibling CSVs it produces but never reads
  (`cross_segment_file_pairs.csv`, `comparison_registry.csv`), and its own six
  generated artifacts -- with authority_level/grain/can_answer/cannot_answer/
  known_limitations per the new `tools/governance_evidence_package.py` module).
  New CLI flags `--emit-evidence-package`/`--no-emit-evidence-package` (default:
  on), `--policy-dir` (recorded, not yet read), and `--package-schema-version`.
  The narrative gains a new authority-header section stating its own
  `controlled_interpretation` role, and the previously-stale producer-identity
  footer (`generate_governance_narrative_dod_aligned_v2.py`, which never matched
  the actual script) now references the real generator name. No existing CSV
  column, classification/scoring logic, or threshold changed -- see D-019 and
  `docs/governance_evidence_package.md`. Structured findings
  (`governance_findings.json`) and policy externalization are deferred to later
  work.
- `tools/generate_governance_narrative.py`'s `build_cascade()` now breaks
  `gt`/`gc`/`gp` (generic->template/container/project containment) down by the
  TARGET's own scope level, instead of discarding every row where the target
  isn't the single broadest ("enterprise") population. `compare_cross_segment.py`
  intentionally emits `generic_to_template`/`_container`/`_project` rows for
  client-/bc-/discipline-scoped targets too — real baseline-propagation evidence
  that a prior pass (PR #350) deliberately gated away to keep `gt`/`gc`/`gp` as a
  single clean enterprise-wide number (Option A, avoiding the blend-distinct-
  scope-grains anti-pattern this file's other fixes already correct for). `gt`/
  `gc`/`gp` themselves are unchanged — still the enterprise-only slice — but a
  new `gt_by_scope`/`gc_by_scope`/`gp_by_scope` (`{scope_label: mean_containment}`,
  mirroring the existing `wp_disc` per-discipline breakdown pattern) now captures
  every other scope level (`client`, `bc`, `discipline`, and combinations, via a
  new `_target_scope_label()` using the `business_center_label_a/b` columns added
  in the intervening B6 schema fix) rather than silently dropping it. The
  GENERIC (reference) side of the comparison is still required to be the one
  canonical enterprise-wide Generic population.

  Rendering/anomaly-detection followed as a second pass: `detect_anomalies()`
  now flags a material (≥0.25 absolute) divergence between the enterprise
  reading and the mean of a domain's scoped buckets, in either direction, per
  cascade stage (Generic→Template/Container/Project); a new
  `render_generic_baseline_scope_section()` renders one row per
  `(domain, scope)` pair actually observed (`Domain | Scope | G→Template |
  G→Container | G→Project`) — a fixed-column table doesn't fit here since scope
  buckets are combinatorial (`client`, `bc`, `discipline`, `client_discipline`,
  etc.), not a small fixed set like disciplines. The section is omitted
  entirely when no domain has any scope-breakdown data.

- `tools/generate_governance_narrative.py`'s Group 1 dispatch (`tc`/`cp`/`tp`
  from `template_to_container`/`container_to_project`/`template_to_project`)
  gets the same Option C treatment Group 2 (`gt`/`gc`/`gp`) got above, closing
  the gap documented in
  `docs/governance_narrative_group1_scope_gap_investigation.md`: since
  `business_center_label` became a real segmentation cut, almost no segment is
  fully unscoped anymore, so `tp`/`cp` were `None` for effectively every
  domain and `assign_tier()` always fell to `TIER_INSUFFICIENT` regardless of
  real bc-pooled evidence sitting unused in `cross_segment_summary.csv`. `tc`/
  `cp`/`tp` themselves are unchanged — still populated only from the
  `"enterprise::enterprise"` (both sides pass `_is_unscoped_segment()`) pair —
  but new `tc_by_scope`/`cp_by_scope`/`tp_by_scope` (`{scope_pair:
  mean_containment}`, keyed `f"{scope_a}::{scope_b}"` since, unlike Group 2,
  neither side of a Group 1 pair is gated to a fixed role population) now
  capture every other `(scope_a, scope_b)` pair instead of discarding it. The
  separator is `"::"`, not a bare `"_"`, because `_target_scope_label()`'s own
  multi-dimension labels (e.g. `"bc_discipline"`, `"client_bc"`) already
  contain underscores — joining two such labels with `"_"` is ambiguous
  (`("client", "bc_discipline")` and `("client_bc", "discipline")` both
  produce the literal string `"client_bc_discipline"`) and this was confirmed
  to actually occur against a real `cross_segment_summary.csv` export during
  review, not just a theoretical edge case.

  A same-bc-both-sides (`"bc::bc"`) pooled value gives `assign_tier()` a new,
  distinctly-named fallback tier, `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE`
  (ordered directly before `TIER_INSUFFICIENT`, i.e. the weakest tier that
  still has *some* evidence), when `tp`/`cp` are both `None` — deliberately
  NOT blended into the existing enterprise-only `primary`/score-banded tiers,
  since bc-pooled evidence is not enterprise-level evidence. The `T→Container`/
  `T→Project`/`C→Project` columns in `render_domain_tiers()` stay `—` for
  domains in the new tier (never silently repointed at a pooled number); a new
  `render_group1_scope_section()` (mirroring `render_generic_baseline_scope_section()`)
  renders the per-`(domain, scope_pair)` detail instead. `detect_anomalies()`
  gained a Group 1 analog of the existing scope-divergence note: since Group 1
  usually has no enterprise reading to diverge from (that's the gap this fix
  closes), the check instead flags when a pooled bucket's own intra-bucket
  spread (min/max across the individual rows pooled into it) is ≥0.25
  absolute — the same materiality threshold as Group 2's check — meaning the
  pooled mean is hiding sharp disagreement rather than reflecting genuine
  convergence. The note's wording is deliberately scope-neutral rather than
  always saying "business-center": validating against a real
  `cross_segment_summary.csv` showed most divergence notes actually fire for
  scope pairs like `client_bc::client_discipline`, where the client and
  business center are held constant and only the discipline varies across the
  pooled rows — an earlier wording draft said "across individual
  business-center pairs" unconditionally, which was accurate only for the
  `"bc::bc"` case and misleading for every other scope_pair.

- Four PR-review findings on the Group 1 bc-pooled fallback above, all
  confirmed against the real `cross_segment_summary.csv`/`segment_manifest.csv`
  export supplied during review:
  1. **Value-mismatch guard (new `_group1_scope_pair()`)**: `_target_scope_label()`
     only records SHAPE (which dimensions are populated), not VALUE.
     `discover_within_segment()` in `compare_cross_segment.py` pairs same-parent,
     same-unit Template/Container/Project segments without checking that scope
     label VALUES match, so a `BC_1`-scoped segment paired against a
     `BC_2`-scoped segment was silently bucketed as `"bc::bc"` — the same key as
     genuine same-business-center evidence. Confirmed reachable in the real
     export: one real row (`client_bc_discipline` shape on both sides, one field
     mismatched) was landing in a merged bucket, corrupting 20 domains'
     `tc_by_scope` entries. New `_group1_scope_pair()` verifies every field
     making up a shared shape actually matches before using the plain
     `f"{scope_a}::{scope_b}"` key; a same-shape-different-value pair now gets a
     distinct `f"{scope_a}!cross::{scope_b}!cross"` key instead — captured, not
     discarded, but never conflated with same-value pooled evidence. `tc`/`cp`/`tp`
     remain byte-for-byte unchanged (re-verified: 0 mismatches across all 32 real
     domains).
  2. **`_has_renderable_cascade_signal()` scope-only gap**: a domain whose ONLY
     Group 1 signal is scoped evidence (e.g. `tp_by_scope["bc::bc"]` populated
     but no enterprise `tc`/`cp`/`tp` and no `wp_all`/Group 2 signal) would get
     `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE` from `assign_tier()` but never
     appear in `render_domain_tiers()`/the domain CSV, since
     `_has_renderable_cascade_signal()`'s key list didn't include
     `tc_by_scope`/`cp_by_scope`/`tp_by_scope` (which are always non-`None`
     dicts, so they can't reuse the existing `is not None` check). Now also
     checks for a non-empty by-scope dict.
  3. **`render_group1_scope_section()` prose overclaimed "business-center-level"**:
     the section's intro described every non-enterprise row as "pooled
     business-center-level evidence," but it renders every scope_pair, most of
     which (`client::bc`, `client_bc::discipline`, etc.) are not business-center
     evidence at all. Reworded to name only `"bc::bc"` as business-center-level
     and tier-relevant; other scope pairs are described as real evidence in
     their own right that does not by itself grant the new tier. (The
     equivalent `detect_anomalies()` wording was already fixed in the prior
     commit.)

### Fixed
- `tools/archetype/generate_archetype_candidates.py`'s `_governance_question_hint()`
  only ever inspected `target_domain`, so it couldn't distinguish a dynamic
  View Filter Definition (VFD) edge from a structural one. Dynamic VFD edges
  carry `source_domain == "view_filter_definitions"` but `target_domain` ==
  whatever element-type domain the filter scopes to (`wall_types`,
  `ceiling_types`, `floor_types`, `roof_types`); the static
  `view_filter_applications_view_templates.stack_filter__view_filter_definitions`
  chain edge instead carries `target_domain == "view_filter_definitions"`.
  Two consequences, both independently documented in
  `tools/archetype/review/archetype_dp1_prompt.md`'s known-misfire list as a
  manual correction required every Decision Point 1 cycle: (1) a VFD-to-VFD
  pair targeting `wall_types` collided with the `wall_graphics` predicate
  (`"wall_types" in target_domain`) before any VFD-aware check existed; (2) a
  VFD-to-VFD pair targeting `ceiling_types`/`floor_types`/`roof_types` matched
  none of the target-domain predicates and fell through to `"unknown"`.
  Fixed by adding `_is_vfd_related(source_domain, target_domain)` (true when
  `source_domain == "view_filter_definitions"` OR `target_domain ==
  "view_filter_definitions"`), returning `"view_filter_strategy"` when both
  sides of the pair are VFD-related, checked before the existing
  target-domain-only priority list. The first version of this fix only
  checked `source_domain_a == source_domain_b == "view_filter_definitions"`
  (VFD-to-VFD only) and still misclassified a VFD edge paired with the static
  stack_filter chain edge as `wall_graphics` — caught in PR #357 review and
  corrected to the broader `_is_vfd_related()` form above. This only affects
  auto-generated candidates in `archetype_definitions_candidates.json`; it
  does not retroactively change `governance_question` on already-promoted
  archetypes in `config/archetype/archetype_definitions.json`, which are set
  by human curation at DP1 independent of this hint.

- `tools/generate_governance_narrative.py` read `client_label`/`discipline_label`/
  the "is this the broadest population for its role" condition by parsing
  `segment_id` positionally (`get_client()`, `get_disc()`, `is_generic()`, a
  `"Template" in segment_id` substring check) instead of the real
  `client_label_a/b`/`discipline_label_a/b`/`governance_role_a/b` columns that
  already exist on `SUMMARY_FIELDS`. This silently misparsed segments whose
  third pipe-separated part is a `business_center_label`/`collection_label`
  rather than a client (e.g. `imperial|Template|Shared` read as
  `client="Shared"`), and `is_generic()`'s length-2 heuristic couldn't
  distinguish a genuine broadest-role segment from a blank-`governance_role`
  scope rollup that also happens to produce 2 parts (e.g. `imperial|BC_2014`).
  Replaced with direct column reads and a `_is_unscoped_segment()` helper
  (role non-blank, `client_label`/`discipline_label` both blank). Two follow-on
  refinements to that helper, both confirmed against real segment-manifest
  construction: (1) `business_center_label`/`collection_label` are not yet
  columns on `SUMMARY_FIELDS`, so a segment scoped only by one of those two
  dimensions (e.g. `imperial|Template|BC_1234`) can slip past the column checks
  — rejected via a structural check that any segment_id part beyond
  `unit_system+role` must be blank once client/discipline are confirmed blank
  via their own columns; (2) that same check initially rejected a *genuinely*
  unscoped segment whose `client_label`/`discipline_label` dimension is
  explicitly selected-but-blank in its key (`build_segment_manifest.py`'s
  `_subset_to_id()` emits a literal empty token for this, e.g.
  `imperial|Template||Shared` for a blank client alongside a real
  `business_center_label` — see that function's own code comment), which is
  not hidden scope data and must not cause rejection; fixed by requiring only
  that any extra part be *empty*, not merely that there are exactly 2 parts.

- `tools/generate_governance_narrative.py`'s `build_cascade()` was a bare
  `if/elif` chain recognizing 5 of the ~16 `comparison_type` values
  `compare_cross_segment.py` can emit, silently dropping every other row with
  no signal that anything was excluded — including all four new scope-level
  types (`enterprise_to_project`, `bc_to_project`, `enterprise_to_bc`,
  `enterprise_to_client`) and the `generic_to_template`/`_container`/`_project`
  triple that is the literal top rung of the "Governance Cascade" diagram the
  narrative's own header already describes but never computed. Replaced with
  an explicit dispatch naming every known type across four groups (already-
  handled cascade stages; the newly-wired generic-to-* stage, threaded through
  as new `gt`/`gc`/`gp` fields and rendered as new table columns; the four
  scope-level types, captured under new `ep`/`bp`/`eb`/`ec` keys but
  deliberately not rendered/tiered yet — a scope-level axis, not one more
  cascade stage; and an explicit "known, deliberately excluded" registry for
  `sibling_templates`/`sibling_containers`/`sibling_generic`/`sibling_segments`/
  `governance_chain`, each with a verified reason) plus a coverage-check
  warning for any comparison_type not accounted for by name in any group.

- `build_governance_state_summary()`'s compact-summary loop had no
  `comparison_type` filter on any of its count/share fields, so rows for the
  four new scope-level types were silently averaged into the same per-domain
  number as `template_to_project`/`container_to_project` — a scope-level axis
  blended into a cascade-stage number with no indication it happened (traced:
  a synthetic `bc_to_project` + `template_to_project` pair for one domain
  produced a blended `provided_passive_share` of 0.375 pre-fix; 0.05 —
  `template_to_project` alone — post-fix). Its detailed per-pattern loop's own
  `_DIRECTED_GOVERNANCE_TYPES` gate was a stale hand-maintained copy of
  `compare_cross_segment.py`'s `GOVERNANCE_STATE_DIRECTED_TYPES`, missing all
  four new types and carrying two entries (`generic_to_downstream`,
  `parent_sibling_roles`) confirmed to never reach a governance-state output
  file today. Fixed by keying aggregation by `(domain, comparison_type)`
  throughout and importing `GOVERNANCE_STATE_DIRECTED_TYPES` directly instead
  of hand-copying it; the two unexplained legacy entries are kept rather than
  silently dropped pending confirmation of their disposition. A domain whose
  *entire* governance-state signal is scope-level-only is now correctly
  omitted from the returned map rather than stored as an all-`None`-valued but
  still-truthy dict, which had been switching its whole tier group's rendered
  table to state-columns mode with every visible state value blank.

- `DISC_KEYWORDS`/`DISC_LABELS` hardcoded a 7-discipline set that `get_disc()`
  used as the sole vocabulary for discipline detection, and
  `render_discipline_section()` iterated `DISC_LABELS.keys()` to decide which
  disciplines to render a section for — so any discipline outside that set
  (confirmed real: `lighting`, `medical_equipment`, `security`, alongside the
  existing 7) was invisible in that section even though the underlying
  `discipline_label_a/b` data already had it. Discipline vocabulary is now
  computed from the data actually present (`disc_domain_wp.keys()`);
  `DISC_LABELS` is kept only as an optional display-name override, falling
  back to a humanized title-case render (e.g. `medical_equipment` ->
  `"Medical Equipment"`) for anything not in the override map.

- `HEALTHCARE_CLIENTS = {"Kaiser", "Sutter", "Renown", "DCMH"}` plus a
  standalone `if client == "Intel": tier = "Non-comparable (different
  sector)"` special case hardcoded a business fact (client sector membership)
  that cannot be derived from the pipeline's own data into Python literals,
  requiring a code change and redeploy for every new client. Replaced with a
  `sector_map` lookup loaded from a new optional `client_sector.csv`
  (`client_label,sector` columns, `--client-sector`, defaulting to
  `policies/client_sector.csv` so existing invocations that don't pass the
  flag still get today's classification rather than silently losing the
  cross-client-convergence signal for every domain). An unclassified client
  (absent from the file, or the file itself absent) is `sector = "unknown"`,
  which now falls through to normal alignment tiering rather than being
  treated as either "Non-comparable" (that requires an explicit, *known*
  non-healthcare sector) or a confirmed different-sector profile in the
  onboarding-implications text — both of those previously fired for any
  `is_healthcare == False`, which conflated "known different sector" with "we
  don't know."
- `tools/compare_cross_segment.py` Mode D (`within_project`) grouped files by
  `project_label` using `.strip() or eid` — a fallback that only catches a
  truly-blank string, not a populated NA placeholder like
  `"__NOT_APPLICABLE__"`, `"n/a"`, or `"NA"`. Every file in a segment whose
  project is unassigned carries the exact same placeholder string, so all of
  them collapsed into one giant fake "project" and got pairwise-compared
  against each other (`C(n,2)` spurious pairs for `n` unassigned files —
  484 files in the `imperial` segment pre-fix). Fixed at all four sites that
  used this pattern: the `discover_within_project()` pair-discovery gate,
  both grouping loops (`by_proj`/`by_proj_used`, all-view and used-view) in
  `run_pair()`'s `is_within_project` branch, and `_project_label_for_file()`
  (used by `build_union_inventory_rows()` for the `n_projects_present`/
  `n_projects_denominator` union-inventory counts). All four now use
  `na_token.is_blank_or_na()` — the same NA-recognition helper Mode E's
  `discover_governance_chain()` already uses for `client_label`/
  `collection_label` — to decide when to fall back to the per-file `eid`
  singleton key, so unassigned-project files no longer group with each
  other (each remains its own singleton, same as a truly-blank label
  already did) while real shared `project_label` values (e.g. `"Renown"`,
  41 files) are unaffected.

- `tools/build_segment_manifest.py` `_sanitize_folder()` collapsed consecutive
  separator characters into one `_` and trimmed leading/trailing `_`, which
  erased a real distinction in `segment_id`: a cut dimension explicitly
  selected in a subset with a blank value (today only `client_label` — see
  `_build_segments()`'s blank-client handling) renders as an empty part
  between/after separator pipes (e.g. `imperial|Template|` or
  `imperial|Container||architectural`), which is a *different, smaller*
  population than that same dimension not being selected at all (e.g.
  `imperial|Template`, which pools every value of the field, blank
  included — always a superset of the selected-blank population). Both
  forms sanitized to the identical folder name, so once enough blank-client
  rows exist for the two populations to diverge (no longer collapsible via
  the existing `redundant_single_child` dedup), both become real,
  independently `bundle`/`reference`-eligible segments competing for the
  same `output_folder` — surfaced only as an opaque `_2` collision-avoidance
  suffix rather than a clear identity. `_UNSAFE_FOLDER_CHARS` no longer uses
  a `+` quantifier (each unsafe character is replaced one-for-one, so
  consecutive separators no longer collapse to a single `_`) and the final
  `.strip("_")` was removed, so a trailing/embedded blank-selected segment
  now sanitizes to a distinguishable folder name. Each blank part is also
  rendered as the literal token `enterprise` (the same scope-level term
  `compare_cross_segment.py` already uses for "no client, no bc" rows)
  rather than a bare `_`/`__`, so e.g. `imperial|Template|` sanitizes to
  `imperial_template_enterprise` instead of `imperial_template_` — a
  self-explanatory name instead of something that reads as a naming
  mistake. `segment_id` text itself (used elsewhere — parsed positionally
  in `tools/generate_governance_narrative.py` and hardcoded across dozens
  of existing tests) is completely unchanged; only the derived folder name
  changes, and only for segments that select a blank cut-dimension value.
  Verified against a real corpus manifest: 5 `bundle`/`reference`-eligible
  folder-name collisions, all resolved.

### Changed (breaking pipeline-contract)
- `segment_manifest.csv` and `run_registry.csv` no longer carry per-segment file
  membership as inline pipe-delimited columns (`export_run_ids`,
  `seed_export_run_ids`). For large populations these columns exceeded
  spreadsheet cell limits (Excel ~32,767 chars/cell, Google Sheets ~50,000
  chars/cell — confirmed offenders in the current corpus reached 59,271
  chars), and a viewer truncating a field mid-quote desyncs the CSV parser
  for every row after it. Membership now lives in a new normalized join
  table, `segment_membership.csv` (`segment_id,export_run_id,is_seed`), one
  row per (segment, file) pair, written alongside the other two files by
  `build_segment_manifest.py`. `segment_manifest.csv` keeps only scalar
  summary fields (`file_count`, `has_seed_file`, `population_hash`);
  `run_registry.csv` keeps `population_hash` only — neither file will ever
  again carry a variable-length filename list. `population_hash` is computed
  identically to before (still from the in-memory `eids` list, not a
  round-trip through any CSV), so it is unchanged for any segment given the
  same file population — skip-logic/staleness comparisons are unaffected.
  `tools/run_segment_orchestrator.py` gains a `--membership-file` flag
  (default: sibling of `--manifest-file`) and now sources every
  `export_run_ids`/`allowed_ids` lookup from `segment_membership.csv` instead
  of the retired manifest column. `_build_registry()`'s `new_files`/
  `removed_files` staleness-reason diffing now reads the prior run's
  population from `existing_membership` (loaded from a prior
  `segment_membership.csv`) instead of an `export_run_ids` field embedded in
  the prior `run_registry.csv` row; a registry rebuilt for the first time
  after this migration (no prior `segment_membership.csv` on disk) will show
  every current file as `new_files` with no `removed_files` on that one
  transitional run, since there is no prior per-segment file list to diff
  against — a one-time artifact of the migration, not an ongoing dual-path.
  No fallback to the old inline column was kept; this is a schema change, not
  an additive one.

### Added
- `tools/compare_cross_segment.py` governance comparisons now fan out across
  enterprise/bc/client scope levels instead of routing everything through a
  single client-scoped grouping key. Scope level is derived per-row from
  which of `client_label`/`business_center_label` are populated (enterprise =
  neither; bc = business_center_label only; client = client_label only;
  project = both) — orthogonal to `governance_role`, and computed the same
  way for every comparison in this file.
  `discover_governance_chain()` gains four new directed pairwise comparison
  types, each an independent parallel edge (no fixed override precedence
  between enterprise/bc/client standards, since any one may or may not have
  adapted from another): `enterprise_to_project` (an enterprise-scoped
  Template/Container reaches every Project regardless of its own client/bc),
  `bc_to_project` (a bc-scoped Template/Container reaches only Projects in
  the same normalized business center), `enterprise_to_bc`, and
  `enterprise_to_client` (same-role, standard-to-standard — Template vs.
  Template or Container vs. Container, never mixed roles). All four are
  registered in `DIRECTED_TYPES` and `GOVERNANCE_STATE_DIRECTED_TYPES`;
  `enterprise_to_project`/`bc_to_project` are additionally registered in
  `DELTA_DIRECTED_TYPES` alongside `template_to_project`/`container_to_project`,
  since they are the same shape of comparison (standard reference vs. Project
  target) just at a different scope level. Generic/Generic-Host is
  deliberately excluded from this fan-out — it already pairs unconditionally
  against every Template/Container/Project via the pre-existing `generic_ids`
  loop, so a separate scope-scoped edge would be redundant.
  `run_pooled_comparison()` gains two new pool grains alongside the existing
  `(parent_segment_id, role, unit_system)` pool (now labeled `pool_scope=
  parent_sibling` in `cross_segment_pooled.csv`, a new column): `pool_scope=
  bc` pools `(business_center_label, role, unit_system)` ignoring
  client_label (whichever clients happen to have work in that bc), and
  `pool_scope=client` pools `(client_label, role, unit_system)` ignoring
  business_center_label (whichever bcs happen to have work for that client).
  These are genuinely different pools with different membership, not two
  views of the same pool — a segment can now appear in `cross_segment_pooled.csv`
  once per applicable pool grain. The per-pool containment/bundle computation
  itself is unchanged; it was extracted into a shared `_build_pooled_row()`
  helper so all three grains share one implementation.
- Segment staleness model extended (build_segment_manifest.py `_build_registry()`):
  `run_registry.csv` gains `export_run_ids` (persisted per-run member list, enabling
  next-run diffing) and `conformance_reference_mode` (currently always `"latest"` —
  compare_cross_segment.py always resolves reference segments dynamically against
  current output; a pinned/snapshot mode is deferred until Phase-2 baseline authority
  is established). When `population_hash` changes, the registry now records
  `new_files:<n>` and/or `removed_files:<n>` reason counts alongside the existing
  `population_changed` marker, diffed against the prior run's `export_run_ids`. A
  metadata edit that moves a file between segments (e.g. a corrected `client_label`)
  surfaces as `removed_files` on the old segment and `new_files` on the new one —
  no separate "metadata change" detection path was needed or added.
- `tools/run_segment_orchestrator.py --dry-run` now prints each pending segment's
  registry `notes` (the staleness reason) alongside its status.
- `tools/compare_cross_segment.py` now writes `comparison_registry.csv` after every
  run: one row per actually-recomputed (segment_a, segment_b, comparison_type,
  domain) work item, stamped with each side's `population_hash`/`last_run_utc`
  (read from `run_registry.csv`) and `computed_utc`. Keyed at the domain granularity
  matching `work_items`, not the coarser pair — a `--domain`-scoped invocation only
  recomputes one domain per pair, so stamping at pair granularity would silently
  mark every other domain on that pair "current" without having recomputed it,
  hiding real staleness from a later `--dry-run`. This is new tracking state
  only — comparisons are still always fully recomputed on every invocation; nothing
  is skipped based on this registry. `--dry-run` now looks up each discovered
  (pair, domain) work item against this registry and labels it `stale` (never
  computed, or either side's stamp has moved since — this is how a Template/
  Container reference re-running with new bundle output is surfaced as
  invalidating its downstream Project/Container comparisons, even though the
  target's own file population is unchanged) or `current`.

### Fixed
- `tools/compare_cross_segment.py` `discover_governance_chain()`'s four
  scope-level fan-out edges (`enterprise_to_project`, `bc_to_project`,
  `enterprise_to_bc`, `enterprise_to_client`) group purely by scope level,
  ignoring `parent_segment_id` — the same class of bug just fixed in
  `run_pooled_comparison()`'s bc/client pool grains, but in the pairwise
  path. Verified against a real corpus manifest: 14 of 139 new-type pairs
  were a segment paired against its own `parent_segment_id` ancestor or
  descendant (e.g. an enterprise-scoped Template paired against a
  bc-scoped Template nested directly under it), which would inflate
  containment toward a false 1.0 the same way. `_build_ancestor_map()` and
  a shared `_is_lineage_related()` helper (used by both this fix and the
  pooled-comparison one) now exclude any such pair from all four edges.
- `tools/compare_cross_segment.py` `run_pooled_comparison()`'s new `bc`/`client`
  pool grains ignore `parent_segment_id` for grouping, so a collection-blank
  BC roll-up and its own collection-specific child (or any ancestor/descendant
  pair sharing the same normalized bc/client value) could land in the same
  pool. Since segments are hierarchical cuts of the same underlying file
  population, an ancestor's data is always a superset of its descendants' —
  pooling them as peers compared a segment against a pool already containing
  its own data, inflating `all_containment_focal_in_pool`/
  `used_containment_focal_in_pool` toward a false 1.0. A new `_build_ancestor_map()`
  walks each segment's `parent_segment_id` chain once per invocation;
  `run_pooled_comparison()` now excludes any segment in the focal segment's
  own ancestor/descendant lineage from its pool, for all three pool_scope
  grains.
- `tools/compare_cross_segment.py` `build_explicit_matrix_outputs()`'s pool
  matrix (`project_pool_containment_similarity_matrix.csv`) keyed cells only
  as `row_id -> peer_pool:<row_id>` by view/domain, ignoring `pool_scope`.
  Since a project can now emit one pooled row per applicable `pool_scope`
  grain (`parent_sibling`/`bc`/`client`), different grains for the same
  project collided on identical matrix coordinates with different values.
  `column_id` now folds in `pool_scope` (`peer_pool:<pool_scope>:<row_id>`)
  so each grain gets its own cell.
- `tools/compare_cross_segment.py` `_normalize_bc_label()` only blanked empty
  strings and the `"0000"`/`"BC_0000"` bookkeeping tokens, dropping the
  `is_blank_or_na()` NA-token handling (`n/a`, `NA`, `__NOT_APPLICABLE__`, ...)
  that `discover_governance_chain()`'s `_key()` previously relied on for its
  `business_center_label` fallback. An NA-spelled bc was being treated as a
  real peer business center by `_bc_of()`, `_scope_level()` (misclassified as
  `"bc"` scope instead of `"enterprise"`), and the new bc-scoped
  pooling/pairwise comparisons. Restored the `is_blank_or_na()` check
  alongside the bookkeeping-token check.
- `tools/compare_cross_segment.py` `discover_governance_chain()`'s `_key()`
  business_center_label fallback (used when `client_label` is blank/NA) no
  longer treats bookkeeping tags `"0000"`/`"BC_0000"` (any case) as a real
  peer business center. Those values mean "enterprise work, tagged for
  bookkeeping," not a specific business center; grouping by the raw string
  would have silently pooled unrelated enterprise-wide rows together as if
  they shared one bc. The fallback now normalizes through the same
  `_bc_of()`/`_normalize_bc_label()` helpers the new enterprise/bc/client
  scope-level fan-out uses, falling through to the collection/blank
  fallbacks below when the tag normalizes to blank. Existing fixtures using
  `business_center_label="BC_0000"` alongside a populated `collection_label`
  are unaffected (they already grouped via collection_label, not
  business_center_label); a row with an unadorned `"0000"`/`"BC_0000"` and no
  collection_label now falls through to the blank-key fallback instead of
  pairing with unrelated same-tagged rows.
- `tools/compare_cross_segment.py` `comparison_registry.csv`: a (pair, domain) is now
  also omitted from the stamp if either side's `run_registry.csv` `status` is not
  `"complete"`. `build_segment_manifest.py` updates `population_hash` to a segment's
  new file population immediately on manifest rebuild, resetting `status` to
  `"pending"` (and clearing `last_run_utc`) until the orchestrator actually re-runs
  that segment — but its output folder on disk still holds the *old* population's
  results until then. A compare run in that window read the stale on-disk data yet
  got stamped with the segment's already-updated (new) `population_hash`; once the
  segment reached `"complete"` with that same hash, a later `--dry-run` would have
  wrongly reported the pair as already current.
- `tools/compare_cross_segment.py` `comparison_registry.csv`: removed the carryover of
  prior (pair, domain) entries not recomputed this run, and stopped stamping work
  items that produced no output. Every other output this tool writes
  (`cross_segment_summary.csv` etc.) is a full `atomic_write_csv` replace from only
  the current invocation's rows, never a merge — so a `--domain`/`--segment`-scoped
  run sharing the same `--out-dir` as an earlier full run already destroys those
  other domains'/pairs' output rows. Carrying their old `comparison_registry.csv`
  stamp forward falsely claimed that data was still current. The registry is now a
  full snapshot of only what this invocation actually produced: a scoped run leaves
  every non-recomputed (pair, domain) with no row at all (correctly staleness-flagged
  on the next `--dry-run`), and a work item where `run_pair()`/`_run_pair_domain()`
  returned `None` with no governance-state rows either (e.g. below `--min-patterns`,
  or a within-project pair with no eligible file pairs) is omitted rather than
  stamped current for output that was never written.
- `build_segment_manifest.py` `_build_registry()`: the new `new_files`/`removed_files`
  reason diff reused the name `new_ids` for the per-segment export_run_id diff,
  shadowing the outer `new_ids` (the full eligible segment_id set) used later to
  compute `dropped_ids`. Any retained segment whose population changed left
  `new_ids` holding export_run_ids instead of segment_ids, so every other
  still-present segment was reported as removed from the registry with a false
  "review corresponding folders for manual cleanup" warning. Renamed the
  per-segment locals to `old_export_ids`/`new_export_ids` so they no longer
  collide with the outer set.
- line_patterns sig_hash policy corrected to segments_def_hash (sig_hash.v2):
  segments_norm_hash was incorrectly used as sig_hash basis — it belongs in join_hash only.
  sig_hash answers exact identity (scale variants are distinct records);
  join_hash answers governance equivalence (scale variants collapse to one pattern).
  Cross-domain reference joins (obj_style.pattern_ref.sig_hash, line_style.pattern_ref.sig_hash)
  were broken while norm_hash was in sig_hash; this change restores them.
  Downstream: re-run sig_hash stage only. Bundle/pattern/segment pipelines unaffected
  (they operate on join_hash exclusively).

### Added
- `instance_count` and `is_sole_type_in_category` metadata fields added to
  `text_types`, `dimension_types` (all 7 splits), `arrowheads`, and `compound_types`
  (all 4 partitions — wall, floor, roof, ceiling). Both fields are additive metadata
  only — never in sig_hash, join_key, or identity_basis.items. Arrowheads emit
  `instance_count = None / not_applicable` (tick-mark reverse-lookup deferred).
  Enables compound placeholder condition `is_purgeable OR (is_sole_type_in_category
  AND instance_count == 0)` at pipeline/BI layer.

### Fixed
- `view_filter_applications_view_templates` and `view_templates`: `GetIsFilterEnabled`
  now captured alongside `GetFilterVisibility` — a toggled-off filter is now
  distinguishable from a visible one.
- `view_category_overrides_model` / `view_category_overrides_annotation`:
  `GetCategoryHidden()` captured per category via `_category_hidden_item()` — template-
  hidden categories are now reflected in the override record.
- `object_styles_model`: material resolved to name + class hash (`obj_style.material_sig_hash`)
  via `_material_ref_item()`, not raw ElementId — cross-project stable.
- `identity`: `doc.IsWorkshared`, `app.VersionNumber`, `app.VersionName`,
  `app.VersionBuild` all captured and emitted.
- `view_templates`: `GetWorksetVisibility()` captured per user workset via
  `_append_workset_visibility()`.

### Changed
- `units` domain expanded from 3 specs (length/area/volume) to 38 specs covering
  all Revit disciplines. Common additions: angle, slope, speed, time, mass_density,
  currency, rotation_angle, distance. Electrical: 7 specs. HVAC: 8 specs. Piping:
  5 specs. Structural: 7 specs. All specs are extracted for every document regardless
  of discipline — GetFormatOptions returns a valid FormatOptions object for all
  SpecTypeId specs on any live document. ITEM_Q_UNREADABLE paths are defensive
  fallbacks only and are not expected to fire in normal execution. SpecTypeId nested
  attribute paths are resolved once at function entry and filtered before the loop —
  unresolvable paths are skipped cleanly without blocking the domain. Attribute names
  verified against probe_units_2026-02-04.json.
- units domain SpecTypeId access corrected to Python flat top-level members for Electrical/HVAC/Piping/Structural entries (instead of nested C#-style paths), enabling those discipline records to resolve and emit.
- file_metadata.csv: `project_label` now extracted from Autodesk Docs:// central path
  (ACC projects only); blank for non-ACC paths
- line_patterns join key policy upgraded from `line_patterns.join_key.v2`
  (`line_pattern.segments_def_hash`) to `line_patterns.join_key.v3`
  (`line_pattern.segments_norm_hash`) to enforce scale-invariant structural identity;
  same kind sequence + ratio now collapses length-scaled variants into one pattern
- Bundle analysis `bundle_id` stability explicitly scopes hash identity to
  `(domain, scope_key, sorted_pattern_ids)`; identical pattern sets in different
  scope keys (for example `dimension_types` linear vs angular) intentionally
  receive different bundle IDs and are not cross-scope comparable
- `line_pattern.segments_norm_hash` is now computed automatically during flatten
  in `tools/run_extract_all.py` (no `--synthetic-domains line_patterns` flag required)
- line_patterns normalized token precision set to `.6f` (from `.9f`) after
  sensitivity sweep; decision now includes a documented ±2 decimal neighbor
  validation practice to confirm elbow stability over time
- view_category_overrides split into `view_category_overrides_model` and
  `view_category_overrides_annotation` partitions; `vco.include_controlled`
  coordination item removed; include state now sourced from
  `view_templates.include_vg_model` / `view_templates.include_vg_annotation`
- view_templates V/G include surface changed from a single `include_vg` flag
  to per-tab flags: `include_vg_model`, `include_vg_annotation`,
  `include_vg_analytical`

### Fixed
- file_metadata.csv: re-running the pipeline now preserves existing non-empty
  `client_label` and `governance_role` values by `export_run_id`
- VCO `dflt_map` computation hoisted out of O(templates × categories) inner loop;
  `other_seconds` reduced from ~920s to ~9s on large files, total VCO time reduced ~73%
- FEC cache deduplication: all `(doc, View, instances)` collection sites normalized to
  `_VIEW_INSTANCES_CACHE_KEY`; redundant FEC calls reduced from 12 to 7 per run
- View instances cache pre-warm repositioned before `view_filter_applications_view_templates`,
  ensuring the cache is populated before any view-related domain runs
- `_timing` scope resolved via injection pattern (`run_fingerprint(doc, timing=None)`);
  timing report merge restored to correct location inside `run_fingerprint()`

### Added
- file_metadata.csv: added `client_label` and `governance_role` columns
  (empty strings, manually curated)
- `TimingCollector.record_elapsed()` for hot-loop accumulation without per-iteration
  lock overhead
- VCO inner loop sub-timers: `vco.enumerate_categories`, `vco.get_param_ids`,
  `vco.get_category_overrides`, `vco.extract_graphics` — `other_seconds` is now
  attributable residual Python overhead rather than a black hole
- `total_serialization` and `total_run` timer scaffolding in runner; both correctly
  report 0.0 in written fingerprint (ordering constraint — captured in Dynamo summary
  surface instead)

---

### Changed (hash-breaking — full re-extraction required)
**Domain family splits (D-015):**
- `dimension_types` split into 7 domains: `dimension_types_linear`
  (Linear/LinearFixed/Angular/ArcLength), `dimension_types_angular`,
  `dimension_types_radial`, `dimension_types_diameter`,
  `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  `dimension_types_spot_slope`
- `object_styles` split into 4 domains by CategoryType tab:
  `object_styles_model`, `object_styles_annotation`,
  `object_styles_analytical`, `object_styles_imported`
- `fill_patterns` split into 2 domains by target:
  `fill_patterns_drafting`, `fill_patterns_model`. Solid fills
  (system defaults) excluded from both domains.
- `view_templates` split into 5 domains by ViewType group:
  `view_templates_floor_structural_area_plans`,
  `view_templates_ceiling_plans`,
  `view_templates_elevations_sections_detail`,
  `view_templates_renderings_drafting`,
  `view_templates_schedules`

**Arrowhead record class corrections:**
- Dot, Diagonal, Box, Loop, Elevation Target, Datum triangle record
  classes corrected to size-only (tick_size_in only). Previous hashes
  for these styles incorrectly included tick_mark_centered and
  heavy_end_pen_weight.

**object_styles join-key correction:**
- pattern_ref.kind record class gate removed. Was incorrect —
  pattern_ref.sig_hash moves to optional_items.

**Dimension type policy corrections:**
- Angular: witness_line_control added to required identity
  (confirmed active in UI for Angular, not previously included)
- Radial: radius_symbol_location and radius_symbol_text added
- Diameter: diameter_symbol_location and diameter_symbol_text added
- Spot families: shape-specific indicator and placement fields added

**System type exclusion:**
- Dimension type extractors now exclude system built-in types not
  accessible in the Revit UI (detected via id-based label fallback
  and family name gate). These types cannot be governed.
- Arrowhead extractor now excludes placeholder_missing records
  (unidentifiable system types).
- Domain routing bugs fixed: DiameterLinked/Alignment Station Labels
  excluded from dimension_types_diameter; Diameter types with
  SpotElevationFixed shape enum correctly routed to diameter domain
  via family name gate.

### Added
- `policies/cross_domain_alignment_keys.json` — domain family registry
  and alignment key definitions
- `arrowhead.record_class` in coordination_items for all arrowhead records
- `lp.is_import` in coordination_items for line_patterns records
- `dim_type.domain_family` in coordination_items for all dimension type records
- `obj_style.category_type`, `obj_style.domain_family`, `obj_style.is_subcategory`
  in coordination_items for all object style records
- `vt.view_type_family`, `vt.view_type_raw` in coordination_items for all
  view template records
- `object_styles_annotation` now populates
  `ctx["object_style_annotation_row_key_to_sig_hash"]` for VCO baseline lookup
- View category overrides: `vco.include_controlled`, `vco.vg_category_type`,
  `vco.context_type` added to coordination_items (D-016)
- View category overrides: category 2 (latent overrides, V/G checkbox unchecked)
  now captured alongside category 1

### Decisions captured
- D-015: Domain family architecture — split criteria, vocabulary, alignment key
  registry
- D-016: VCO scope — category 1 (template-controlled) and category 2 (latent)
  implemented; category 3 (view-local) deferred with hooks

---

### Changed (D-015 — Domain Family Split Architecture)

Domain scope redefined: four monolithic extractors split into 18 per-partition domains.
No hash values changed within any record class — this is a structural change only.

- **`object_styles`** split into `object_styles_model`, `object_styles_annotation`,
  `object_styles_analytical`, `object_styles_imported` — each covers one CategoryType.
  `require_domain` references updated to split names throughout.

- **`fill_patterns`** split into `fill_patterns_drafting`, `fill_patterns_model` —
  each covers one FillPatternTarget. Join-key policy updated to use `fill_pattern.target`
  (was `fill_pattern.target_id`) and `fill_pattern.grid_count` as co-required keys.

- **`dimension_types`** split into 7 per-shape domains (`dimension_types_linear`,
  `dimension_types_angular`, `dimension_types_radial`, `dimension_types_diameter`,
  `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  `dimension_types_spot_slope`). Shape discrimination now happens at domain-level
  (handled shapes frozenset). Shared helpers moved to `core/dimension_type_helpers.py`.

- **`view_templates`** split into 5 per-ViewType-family domains
  (`view_templates_floor_structural_area_plans`, `view_templates_ceiling_plans`,
  `view_templates_elevations_sections_detail`, `view_templates_renderings_drafting`,
  `view_templates_schedules`). Shared VG helpers in `core/vg_sig.py`.

- Dependency chain (`require_domain` calls) updated in `view_category_overrides`
  and runner to reference split domain names.

- Join-key policies updated: all split domains have flat per-domain policies.
  Arrowheads policy corrected: shape-gated keys moved from `explicitly_excluded_items`
  to `optional_items` to satisfy A3 validation rule.

---

### Removed
- Legacy hash infrastructure (pipe-delimited signatures) removed across all domains
- `REVIT_FINGERPRINT_HASH_MODE` environment variable (semantic mode now default and only mode)
- `domains/view_filters_deprecated.py` (unused, 741 lines)
- `core/canon.py`: deprecated `sig_val()` helper
- Phase-2 `semantic_keys` duplication in domain payloads
- Legacy context maps: `*_uid_to_hash_v2` (replaced by canonical `*_uid_to_hash`)

### Changed
- All domains now emit only `hash_v2` as the canonical domain hash in runner contract output
- Context maps simplified: removed `_v2` suffix from semantic hash maps
- Contract building simplified: single semantic hash source instead of mode-dependent logic

### Added
- Root governance docs: `INVARIANTS.md`, `ARCHITECTURE.md`, `DECISIONS.md`.
- **NEW DOMAINS (M4):**
  - `view_filter_definitions` - Global domain capturing filter definitions (rules, categories)
  - `phases` - Global domain capturing phase inventory and sequence (names included in hash per D-010 revised)
  - `phase_filters` - Global domain capturing phase filter settings (New/Existing/Demolished/Temporary visibility)
  - `phase_graphics` - Global domain capturing phase graphic override settings (disabled per D-013)
- Context dictionary (`ctx`) now populated by global domains:
  - `filter_uid_to_hash` - Mapping of view filter UIDs to definition hashes
  - `phase_uid_to_hash` - Mapping of phase UIDs to definition hashes
  - `phase_filter_uid_to_hash` - Mapping of phase filter UIDs to definition hashes
- **Canonical evidence selectors (PRs #106–#119):** All 15 domain extractors migrated to policy-driven join-key and sig-hash composition via `build_join_key_from_policy()`. Each domain now emits `join_key`, `join_hash`, and `sig_basis` fields in records, derived from `identity_basis.items` per the join-key policy.
- **Element traceability (PR #126):** `source_element_id` and `source_unique_id` added to `phase2.unknown_items` across all element-backed domains.
- **Timing instrumentation (PR #127):** `core/timing_collector.py` added for extraction profiling. Runner emits `timings.json` sibling artifact.

### Changed
- **BREAKING: View Templates (M5):** Moved from name-only presence hashing to behavior-based hashing
  - Template identity: Now uses UniqueId (was: name)
  - Template hash: Now derived from controlled behavior (was: name presence)
  - Behavioral inputs: view type, detail level, scale, discipline, phase, phase filter, view filters (ordered), display style
  - Names: Now metadata-only (excluded from hash per D-008)
  - Filter stack: Order-sensitive (preserved)
  - References global domains: filters, phases, phase_filters via context
  - record_rows emitted with per-template sig_hash
- Execution order now enforces dependency: global domains run before contextual domains.
- **record_id stabilization (PR #123):** `record_id` generation made deterministic across runs using domain + identity_basis hash.
- **Join-key deduplication (PR #125):** `join_key.items` no longer duplicates `k/q/v` triples already present in `identity_basis.items`; join_key references the canonical source.
- **Object_styles shape-gating (PR #124):** Join-key policy uses `obj_style.pattern_ref.kind` as discriminator; `ref` shape requires `pattern_ref.sig_hash`, `solid` shape does not.

### Semantic Rules Applied
- **View Filters:** Filter rules are order-sensitive (preserved), categories are sorted
- **Phases:** Phase names are included in behavioral hashes for cross-project comparability (D-010 revised), sequence number captured where available
- **Phase Filters:** Settings are order-insensitive (sorted before hashing)
- **Phase Graphics:** Intentionally disabled — API does not expose graphic overrides (D-013)
- **View Templates (M5):**
  - Template names: metadata-only (per D-008)
  - Filter stack: order-sensitive (filter application order matters)
  - Other settings: order-insensitive (sorted)
  - Global references: uses hashes from filters/phases/phase_filters domains
  - Unreadable templates: fail-soft with explicit markers

### Decisions captured
- Nested fenced code blocks are prohibited in documentation (portability rule).
- View filters are global definitions referenced by views and view templates.
- Phase filters and phase graphic overrides are global.
- Phase names ARE included in behavioral hashes (D-010 revised for cross-project comparability).
- Phase sequence number is included in phase signatures to capture ordering.
- Hash mode migration timeline completed (D-014).

---

## 2025-12-17

### Added
- Documented invariants: deterministic hashes, behavior-first, fail-soft, explicit ordering rules.
- Documented architecture layering: core / domains / context / runner.
- Documented decision log to prevent drift and re-litigation.

### Fixed
- Documentation formatting made portable across GitHub Mobile + Obsidian by avoiding nested fenced blocks.

## 2026-08-20

### Corrected
- Identity-aware generated packages now carry deterministic, path-safe
  enterprise-policy provenance.
- Removed maintained Stantec-branded narrative output and migrated the public
  promotion flag to `reuse_client_pool_is_enterprise` without a legacy alias.
