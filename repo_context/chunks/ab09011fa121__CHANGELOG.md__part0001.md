# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 1 of 7
- Original line range: 1-400
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # CHANGELOG
     2| 
     3| > **Historical-record notice (2026-08-20):** Entries below are an append-only account of earlier behavior and may contain former client, organization, or shared-parameter identities. They are non-operational; current configuration and policy come from maintained code and documentation.
     4| 
     5| This file tracks **semantic changes only**:
     6| - anything that changes hashes
     7| - anything that changes what a hash *means*
     8| - anything that changes interpretation, scope, or dependency structure
     9| 
    10| Pure refactors, moves, renames, formatting, and perf tweaks do **not** belong here.
    11| 
    12| ---
    13| 
    14| ## [Unreleased]
    15| 
    16| ### Added
    17| - **`mapping/` line_patterns Revit mapping utility (D-038).** New downstream,
    18|   Revit-writing package (separate from `core/`/`domains/`/`runner/`/`tools/`)
    19|   that reads `tools/export_bundle_pattern_detail.py`'s
    20|   `bundle_pattern_inventory.csv`/`pattern_settings.csv`/`pattern_names.csv`
    21|   and materializes representative `LinePatternElement` objects in the
    22|   currently open Revit document for a mapping/configuration RVT. Every
    23|   requested `(domain="line_patterns", join_hash)` is reconstructed from
    24|   evidence and blocked (never inferred) on incomplete/inconsistent data;
    25|   creation happens inside a per-configuration `Transaction` that is only
    26|   committed after the created element is read back and its `join_hash`
    27|   (via the existing `line_patterns.join_key.v3` policy -- not `sig_hash`)
    28|   is verified to match the request, with rollback on any mismatch or
    29|   exception. No existing extraction, join-key, bundle-analysis, or
    30|   `export_bundle_pattern_detail.py` semantics change -- this is purely
    31|   additive. See `docs/line_pattern_mapping.md` and D-038.
    32| 
    33| ### Changed
    34| - **Repository-neutral runtime and sample defaults.** Dynamo runner discovery no
    35|   longer searches an organization-specific user-profile path, both checked-in
    36|   Dynamo graphs have blank workstation inputs/hints, and the default
    37|   `client_sector.csv` contains synthetic examples only. Deployments using real
    38|   client labels must pass their approved mapping with `--client-sector`.
    39|   Selected-blank segment folder components now render as
    40|   `no_external_client` rather than an organization name; rebuilding such a
    41|   segment therefore changes its `output_folder`, but not its `segment_id` or
    42|   population membership.
    43| 
    44| ### Fixed
    45| - **`mapping/create_line_pattern_mappings.py`: an explicit but invalid `IN[2]`
    46|   repo root now fails loudly instead of silently falling back to the
    47|   environment or `__file__`.** A caller-supplied `IN[2]` is an explicit
    48|   selection, not a hint -- previously, a typo'd or incomplete path there fell
    49|   through to `REVIT_FINGERPRINT_REPO_ROOT_SELECTED`/`REVIT_FINGERPRINT_REPO_DIR`
    50|   or `__file__`, any of which could resolve a *different* checkout than the
    51|   one requested (e.g. a stale env var left over from a previous run in the
    52|   same persistent Dynamo session), applying mappings with unintended code and
    53|   no error at all. Now raises immediately when `IN[2]` is given but doesn't
    54|   look like a checkout.
    55| - **`mapping/create_line_pattern_mappings.py`: cached `mapping`/`core`/`domains`
    56|   modules from a previously-selected checkout are now always purged, and the
    57|   resolved repo root is always promoted to the front of `sys.path`, before
    58|   (re-)importing.** A persistent Dynamo CPython session keeps its interpreter
    59|   (and `sys.modules`/`sys.path`) alive across node re-runs, which surfaced two
    60|   compounding bugs found across two rounds of automated PR review:
    61|   1. A first fix purged stale modules only when a check keyed on a single
    62|      representative module (`mapping.line_pattern_reconstruction`) detected a
    63|      mismatch -- but if a *different* script (the extraction runner) had
    64|      already cached `core.*`/`domains.*` from another checkout in the same
    65|      interpreter without ever importing `mapping.*`, that check saw "nothing
    66|      cached yet" and skipped the purge entirely, so reconstruction would
    67|      still silently reuse the other checkout's hashing/domain code.
    68|   2. Even with modules purged, `sys.path.insert(0, repo_root)` guarded by
    69|      `if repo_root not in sys.path` does nothing when `repo_root` is already
    70|      present but not first (e.g. checkout B ran, then A ran and inserted
    71|      itself ahead, then B runs again) -- imports would still resolve from
    72|      whichever checkout sits earlier in `sys.path`.
    73|   Fixed by dropping the staleness heuristic entirely (an unconditional purge
    74|   of every `mapping`/`core`/`domains`-prefixed `sys.modules` entry is cheap)
    75|   and by removing-then-reinserting the resolved root at `sys.path[0]`
    76|   unconditionally, exactly mirroring `runner/thin_runner.py`'s existing
    77|   `_purge_repo_modules()` / checkout-promotion handling for the same class of
    78|   bug.
    79| - **`mapping/create_line_pattern_mappings.py`: repo-root resolution no longer
    80|   depends on `__file__`.** This entry point is meant to be pasted directly
    81|   into a Dynamo Python Script node, where Dynamo executes the code from a
    82|   string (`File "<string>"`) rather than loading it from disk -- `__file__`
    83|   is undefined in that context, so `NameError: name '__file__' is not
    84|   defined` fired immediately on the first real-world run. Fixed by mirroring
    85|   `runner/run_dynamo.py`'s existing env-var-first resolution
    86|   (`REVIT_FINGERPRINT_REPO_ROOT_SELECTED`/`REVIT_FINGERPRINT_REPO_DIR`),
    87|   falling back to `__file__` only when it's actually available (a node that
    88|   loads the script from a file on disk), and otherwise requiring the
    89|   checkout path as a new optional `IN[2]`. No change to reconstruction,
    90|   hashing, or Revit-mutation behavior -- `mapping/line_pattern_reconstruction.py`
    91|   and `mapping/line_pattern_revit_apply.py` are unaffected (they're always
    92|   imported as real files, where `__file__` is defined normally).
    93| - **`tools/export_bundle_pattern_detail.py`: `_iter_identity_csv()` now always
    94|   resolves item quality from `item_value_type` on the v2.1 identity-item shard
    95|   schema, never from `item_role`.** `item_role` is not a quality field at all
    96|   on this schema -- it is a separate, unrelated tag: blank for a normal row
    97|   written by `tools/extractor.py`'s shard writer, but populated with a
    98|   non-quality marker (e.g. `"synthetic"`) for `tools/run_extract_all.py`'s
    99|   synthetic `line_pattern.segments_norm_hash` row. The original code,
   100|   `row.get("item_role", row.get("item_value_type", ""))`, only fell through to
   101|   `item_value_type` on a *missing* key -- since `item_role` is always present
   102|   (just usually empty), this returned `""` for every normal row (leaving
   103|   `pattern_settings.csv`'s `q` column blank domain-wide) and, after a first
   104|   attempted fix that preferred `item_role` whenever non-empty, would have
   105|   returned the literal string `"synthetic"` as `q` for norm-hash rows instead
   106|   of their real quality. Fixed by reading `q` from `item_value_type`
   107|   unconditionally on this schema. Found via automated PR review while
   108|   validating the new `mapping/` line_patterns utility (which depends on `q` to
   109|   gate reconstruction) against a realistic v2.1 export shape; the bug is not
   110|   specific to that consumer -- it affected `q` resolution for every domain
   111|   read through `export_bundle_pattern_detail.py`'s v2.1 code path. Does not
   112|   affect the legacy `k`/`v`/`q` schema path, `sig_hash`/`join_hash`
   113|   computation (which reads identity items via a different path), or any other
   114|   exported artifact.
   115| - **`materials`: `material.keynote` no longer silently omitted from `identity_basis.items` when blank.**
   116|   `domains/materials.py`'s keynote emission previously used a truthy guard
   117|   (`if keynote and keynote[0]:`) with no `else` branch, so materials with a blank
   118|   or unset Keynote parameter had the `material.keynote` identity item omitted
   119|   entirely instead of present with `q: "missing"`. Since `materials.join_key.v3`
   120|   lists `material.keynote` as `required_items`, and join-key building treats
   121|   required-key *presence* independently of value completeness (see
   122|   `tools/join_key_discovery/eval.py`'s `build_identity_index`/
   123|   `build_candidate_join_key_with_details`), this caused the join-policy gate to
   124|   fail with `reason=missing_required, missing_keys=material.keynote` for every
   125|   blank-keynote material, blocking `join_hash` computation for those records
   126|   entirely. Fixed by always appending the item via the existing `_mk_item()`
   127|   helper (`identity_items.append(_mk_item("material.keynote", keynote))`), which
   128|   already produces `v: null, q: "missing"` for blank/unset values via
   129|   `_read_param_as_string`'s `canonicalize_str`. This is join-key/hash-affecting
   130|   for previously-blank-keynote materials (they now clear the gate and receive a
   131|   real `join_hash` instead of being excluded from comparison), but does not
   132|   change `sig_hash` (computed separately from `name`/`class` only). No other
   133|   domain fields are affected by this change; `material.manufacturer`/
   134|   `material.model` are read but were never emitted as identity items at all
   135|   (not merely guarded) -- flagged as a separate follow-up, not addressed here.
   136| - **`tools/apply_join_policy.py`: diagnostics `policy_id` fallback now reflects
   137|   the actual policy schema instead of a hardcoded `.v21` literal.** Line ~165's
   138|   `policy_id = str(p.get("policy_id") or f"{domain}.join_key.v21")` fell back
   139|   straight to a hardcoded `{domain}.join_key.v21` whenever a domain's policy
   140|   block had no explicit `policy_id` key -- true for every domain in
   141|   `policies/domain_join_key_policies.json`, which key off `join_key_schema`
   142|   instead (e.g. `materials.join_key.v3`). This made `join_key_policy_id` (and
   143|   the `policy_id` column in `join_policy_gate_diagnostics.csv`/
   144|   `join_policy_failures.csv`) cosmetically wrong -- reporting-only, since
   145|   `join_key_schema` itself (line ~167) already read `p.get("join_key_schema")`
   146|   first and was unaffected. Fixed by falling back to `p.get("join_key_schema")`
   147|   before the hardcoded literal. Does not change gate evaluation, `join_hash`,
   148|   or any hash semantics -- diagnostics/reporting output only.
   149| - **`tools/build_segment_manifest.py`: `business_center_label` values shorter
   150|   than 4 digits are now zero-padded before segment_id construction.** A
   151|   purely-numeric `business_center_label` (e.g. `"0"`, `"796"`) is left-padded
   152|   to 4 digits (`"0000"`, `"0796"`) in `_normalize_rows()`, ahead of the
   153|   existing first-seen-casing fold, so it merges with any correctly-formatted
   154|   occurrence of the same code instead of fragmenting into a spurious second
   155|   segment. Fixes a real operator-workflow failure mode: opening
   156|   `file_metadata.csv` in Excel without explicitly importing the
   157|   `business_center_label` column as Text causes Excel to reinterpret `"0000"`
   158|   as the number `0` and silently drop the leading zeros on save -- every real
   159|   business_center_label value in this corpus (the `"0000"` enterprise
   160|   bookkeeping token included) is exactly 4 digits, so padding up is safe. A
   161|   value already at 4+ digits, or containing any non-digit character (e.g.
   162|   `"BC_1234"`, `"Page"`), is left untouched. Segment-id/hash-affecting for any
   163|   corpus that was already carrying a collapsed short-digit
   164|   `business_center_label` value (that value now folds into the padded
   165|   segment instead of its own).
   166| - **`tools/governance_manifest.py`: `normalize_business_center_label()` gets
   167|   the same zero-padding fix (PR #425 review follow-up).** This module reads
   168|   `file_metadata.csv` directly and independently of `build_segment_
   169|   manifest.py` (see the module's own docstring — disjoint governance
   170|   populations are deliberately not built on the segment lattice), so the
   171|   fix above did not reach it. A purely-numeric value shorter than 4 digits
   172|   is now zero-padded before the enterprise-bookkeeping-token check and the
   173|   `BC_`-prefix strip, so a collapsed `"0"`/`"796"` is recognized correctly
   174|   (`"0"` as the `"0000"` enterprise token, `"796"` merging with `"0796"`)
   175|   instead of fragmenting a governance population or spawning a fake
   176|   business center. `governance_relationships.py` imports and reuses this
   177|   same function, so it inherits the fix without a separate change.
   178| 
   179| ### Added
   180| - **Governance narrative evidence-package layer, Phases 7-10 (D-030 through D-034):
   181|   reading order/completeness gate, known-bad-inference clarifications,
   182|   comparison-registry completeness note, union-breadth domain confidence, and a
   183|   package-portable docs subfolder.**
   184|   - **D-030 (Phase 7):** New `docs/governance_reading_order.md` states this
   185|     package's intended audience (a leadership reader without Revit domain
   186|     knowledge, expected to ask governance convergence/fragmentation questions
   187|     rather than resolve them unassisted) and an explicit cold-start reading
   188|     path through the package. `governance_evidence_package.py`'s `_artifact()`
   189|     gains a mandatory `required_before_conclusions: bool` field, and
   190|     `build_evidence_map()`'s output gains a top-level `reasoning_prerequisites`
   191|     list -- the full set of `required_before_conclusions=true` artifact_ids,
   192|     exposed as a completeness set to exhaust rather than an ordinal to sample
   193|     from (an ordinal was considered and rejected: it invites reading only the
   194|     top few and reasoning from a partial picture while technically "following
   195|     the order"). `render_evidence_authority_header()` gains a pointer line.
   196|     No classification/CSV output changes -- descriptive fields and a new
   197|     static doc only.
   198|   - **D-031 (Phase 8, docs-only):** Two new entries in
   199|     `docs/governance_interpretation_guide.md`'s "Known bad inferences": (1)
   200|     a domain's enterprise-scoped `Insufficient Evidence` tier does not mean
   201|     the domain has no usable evidence anywhere in the package -- check the
   202|     client/BC summaries and `cross_client_convergence` first; (2) the corpus
   203|     currently contains files from a single region, so a future `region`
   204|     segmentation dimension will read identically to the existing
   205|     enterprise-level rollup until a second region's data exists -- a fact
   206|     about current data coverage, not a methodology gap. Both entries also get
   207|     first-class placement in the D-030 reading-order doc's "read this before
   208|     drawing conclusions" callout. Also adds an explicit audience/intent
   209|     statement to the guide's "What this package is for" section.
   210|   - **D-032 (Phase 9a):** New optional `--comparison-registry` CLI argument
   211|     (same present-or-absent, gracefully-degrading pattern as other optional
   212|     inputs). When supplied, renders a per-domain **Input Completeness /
   213|     Staleness** note near Analytical Notes -- an evidence/registry-mismatch
   214|     proxy (not a not-run-coverage guarantee: a comparison absent from every
   215|     evidence source has no key to inspect and is not counted) via new
   216|     `build_comparison_completeness()`. Where a mismatch IS detected, it
   217|     distinguishes "this domain's registry stamp is missing or stale relative
   218|     to its evidence" from "this domain's evidence is thin because
   219|     convergence is actually weak," which previously looked identical to a
   220|     reader of `governance_domain_summary.csv`.
   221|     The registry file itself is never embedded in the output package -- only
   222|     the derived counts are. `governance_package_health.json` gains a
   223|     `comparison_completeness` field when the registry is supplied. No
   224|     existing classification/tier output changes when the flag is absent.
   225|   - **D-033 (Phase 9b):** `governance_domain_summary.csv` gains new breadth
   226|     columns derived from `cross_segment_union_inventory.csv` via new
   227|     `build_union_breadth_by_domain()` (corpus-wide/client-wide/project-wide/
   228|     file-level reuse counts per domain; corpus-wide/client-wide classification
   229|     requires more than one client in the denominator, matching
   230|     `compare_cross_segment.py`'s own `_reuse_bucket_for()` guard). Only the
   231|     strongest narrative exceptions -- broad reuse with weak formal cascade
   232|     (a natural-standard candidate the cascade metrics alone would miss), or
   233|     narrow reuse despite strong cascade (worth flagging as fragile) -- are
   234|     rendered as a new `detect_anomalies()` note category, using the same
   235|     policy-externalized-threshold discipline D-029 established (new
   236|     `union_breadth_*` keys in `anomaly_thresholds.json`). Closes the last
   237|     open item in `docs/governance_generator_cross_compare_coverage.md`'s
   238|     implementation sequence.
   239|   - **D-034 (Phase 10):** The four static, package-type-level reference docs
   240|     (`governance_interpretation_guide.md`, `governance_question_routes.md`,
   241|     `governance_reading_order.md`, `governance_classification_rules.md`) move
   242|     into a new `docs/governance/` subfolder -- filenames unchanged, only
   243|     `_DOCS_DIR` in `generate_governance_narrative.py` is repointed. `main()`
   244|     now copies each of the four (via `shutil.copy2`, only when present) into
   245|     `--out` alongside a run's other outputs, so a `--out` directory handed to
   246|     someone without the repo checked out is self-contained -- the narrative
   247|     and evidence map already point readers at these docs by name. Only these
   248|     four are copied; the CSV siblings the same block registers
   249|     (`cross_segment_file_pairs.csv`, `comparison_registry.csv`, etc.) are
   250|     never embedded, per D-023/D-024/D-032. Evidence-map/manifest `path`/
   251|     `present` fields for these four artifacts continue to describe the
   252|     checked-in repo doc, not the copy -- the copy is a portability
   253|     convenience, not a second source of truth.
   254|   - All five phases: no existing classification, tier, or CSV/JSON field
   255|     *value* changes for any prior invocation -- verified via the same
   256|     byte-identical `governance_domain_summary.csv`/`governance_client_
   257|     summary.csv`/`governance_bc_summary.csv` regression discipline D-021
   258|     established. See D-030 through D-034 and `docs/governance_evidence_package.md`.
   259| - `tools/generate_governance_narrative.py`'s remaining anomaly/note materiality
   260|   thresholds -- `detect_anomalies()`'s ~18 bare numeric literals across 11
   261|   distinct `notable_anomalies` findings (including one, the
   262|   `provided_to_configured`/`provided_to_used` "carried but not actively used"
   263|   check, not previously called out as its own finding), the
   264|   `provided_to_used_containment` primary-read threshold in
   265|   `build_governance_state_summary()`'s `_finalize_state_bucket()`,
   266|   `render_findings_and_recommendations()`'s independently-duplicated phases
   267|   check, `_passive_inheritance_risk_domains()`'s bundle-share/passive-indicator
   268|   thresholds, and `_shape_note()`'s Project Portfolio density-similarity
   269|   thresholds -- now load from a fifth JSON policy profile,
   270|   `policies/governance/anomaly_thresholds.json`, via the same
   271|   `apply_governance_policy()` global-reassignment mechanism D-021 established
   272|   (function bodies unchanged except reading a named constant instead of a
   273|   literal). `_passive_inheritance_risk_domains()`'s dual-schema branch now
   274|   reads the existing `governance_thresholds.json` `passive_material_threshold`
   275|   key directly instead of an independent, drifted `0.20` literal, closing a
   276|   gap where a change to that policy value would not previously have
   277|   propagated to this function. New `docs/governance_classification_rules.md`
   278|   documents the branch order and exception conditions of `assign_tier()`,
   279|   `score_reliability()`, `detect_anomalies()`, `build_governance_state_summary()`'s
   280|   `primary_governance_read` selection, and `_passive_inheritance_risk_domains()`
   281|   by threshold-key name (not value) -- added to `governance_evidence_map.json`
   282|   alongside the existing static docs. `render_header()`'s "How to Read the
   283|   Analysis" block, which restated containment/cross-client-similarity/
   284|   all-view-used-view/score-range definitions already covered by
   285|   `docs/governance_interpretation_guide.md`, is now a one-paragraph pointer
   286|   at that guide's "Metric semantics" section; three concepts the block
   287|   covered but the guide didn't yet (containment as reuse/propagation
   288|   evidence rather than approval, used-view's Project-target-primary scope,
   289|   and the 0-1 score range) were added to the guide first, in the same
   290|   change. Every default value in the new JSON profile reproduces this
   291|   generator's pre-externalization Python literal exactly -- no existing
   292|   classification, tier, `notable_anomalies` text, or CSV column changes for
   293|   any existing invocation (verified by the same byte-identical
   294|   `governance_domain_summary.csv` regression test D-021 established, run
   295|   twice: default vs. explicit `--policy-dir policies/governance/`). See
   296|   D-029 and `docs/governance_evidence_package.md`.
   297| - **`dimension_types` domain family: field expansion across all 7 partitions (Area 7):**
   298|   Adds ~25 new identity items across `dimension_types_linear`/`_angular`/`_radial`/
   299|   `_diameter`/`_spot_elevation`/`_spot_coordinate`/`_spot_slope`, closing the gap
   300|   identified in `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
   301|   §7. All fields are read via the existing `first_param(bip_names=..., ui_names=...)`
   302|   pattern (`core/rows.py`) and verified against three consistent runs from the
   303|   approved external probe dataset described in `tools/probes/Exports/README.md`
   304|   (including its per-shape `observed_on_shapes` breakdown), rather than a fresh
   305|   live-Revit run in this pass —
   306|   where the probe data corrected this area's own initial family-applicability
   307|   guesses (Centerline/Interior Tick Mark/Equality are Linear+Angular only, **not**
   308|   Radial/Diameter as originally guessed; Alternate Units genuinely applies to all 7
   309|   partitions per Greg's correction, not just Linear), the probe evidence was
   310|   followed over the guess.
   311|   - **§1 spot-family Leader Arrowhead cluster** (`extract_spot_elevation`/
   312|     `_spot_coordinate`/`_spot_slope`): new shared helper
   313|     `core/dimension_type_helpers._read_leader_arrowhead()` (mirrors the working
   314|     `domains/text_types.py` Leader Arrowhead read/resolve pattern — `BuiltInParameter.
   315|     LEADER_ARROWHEAD` → `ctx["arrowheads_by_type_id"]`) implements the
   316|     previously-reserved-but-never-implemented `dim_type.leader_arrowhead_sig_hash`
   317|     identity key, plus new `dim_type.leader_arrowhead_uid`/`_name` (file-local/cosmetic
   318|     metadata, same status as `text_type.leader_arrowhead_uid`/`_name`) and ordinary
   319|     `dim_type.leader_arrowhead_line_weight`/`dim_type.leader_line_weight` reads.
   320|   - **§2 linear/angular/radial/diameter leader config**: `dim_type.leader_tick_mark_sig_hash`
   321|     (new generic `core/dimension_type_helpers._read_arrowhead_ref_sig_hash()` helper,
   322|     reused for the other tick-mark-family fields below), `dim_type.leader_type`,
   323|     `dim_type.show_leader_when_text_moves` — a distinct Revit feature from §1's spot
   324|     Leader Arrowhead despite the shared word "leader"; no shared helper or key-naming
   325|     prefix between the two.
   326|   - **§3 witness lines** (linear/angular only): `dim_type.witness_line_extension_in`/
   327|     `_gap_to_element_in`/`_length_in`, plus `dim_type.witness_line_tick_mark_sig_hash`
   328|     (Linear only — confirmed asymmetry vs. Angular, consistent across all 3 probe runs).
   329|   - **§4 equality/centerline/tick-weight** (linear/angular only, correcting this area's
   330|     own initial radial/diameter guess): `dim_type.equality_text`/`_witness_display`
   331|     (`dim_type.equality_formula` dropped — probe storage=`None`/unsupported, not a
   332|     plain-parameter read); `dim_type.centerline_symbol_name` (new generic
   333|     `core/dimension_type_helpers._read_element_ref_name()` helper — name-only
   334|     resolution, not routed through `arrowheads_by_type_id` since it isn't a confirmed
   335|     arrowhead-family reference) and `dim_type.centerline_tick_mark_sig_hash`/
   336|     `dim_type.interior_tick_mark_sig_hash`/`_display`. `dim_type.tick_mark_line_weight`
   337|     (angular/diameter/linear/radial — distinct from the existing `dim_type.line_weight`).
   338|     `dim_type.centerline_pattern_sig_hash` (linear/angular only) resolves via a new
   339|     `core/dimension_type_helpers._read_line_pattern_ref_sig_hash()` helper — the same
   340|     `ctx["line_pattern_uid_to_hash"]`/`ctx["line_pattern_special_values"]` resolution
   341|     `domains/object_styles.py`/`domains/line_styles.py` already use for their own
   342|     `GetLinePatternId()` references, so a built-in pattern id (e.g. the probe's
   343|     `-3000010`, "Solid") resolves to the same `"<Solid>"` sentinel those domains use
   344|     instead of collapsing to the same `missing` value as "no centerline pattern"
   345|     (PR #412 review fix — an earlier revision routed this through a plain
   346|     `doc.GetElement()` name lookup that returned `missing` for built-in pattern ids,
   347|     losing the distinction between "Solid" and "none").
   348|   - **§5 alternate units** (all 7 partitions): `dim_type.alternate_units`,
   349|     `dim_type.alternate_units_prefix`/`_suffix`. `dim_type.alternate_units_format_id`
   350|     was dropped (PR #412 review fix): it required
   351|     `DimensionType.GetAlternateUnitsFormatOptions()`, an accessor not confirmed to
   352|     exist on the Revit surface this repo's committed probe data represents (raises
   353|     `AttributeError` there) — since every extractor added it as a non-required item and
   354|     the status-reason loop counts any non-OK optional item, this made every
   355|     dimension-type record degrade without the field ever capturing real data. Rather
   356|     than ship a field permanently pinned to `unsupported_not_applicable`, it was
   357|     removed along with `_read_unit_format_info()`'s `alternate=` parameter.
   358|   - **§6 `Suppress Spaces`** (all 7 partitions, wherever `_read_unit_format_info()` is
   359|     already called): extends that function's return tuple with `suppress_spaces_v/_q`
   360|     read off the same `FormatOptions` object as `rounding`/`accuracy` — same
   361|     `FormatOptions`-boolean-flag gap Area 8 documented for `units.py`, applied here to
   362|     `DimensionType.GetUnitsFormatOptions()` instead of the doc-level `Units` object.
   363|   - **§7 provisional cluster**: implemented with probe-confirmed family assignments —
   364|     `dim_type.text_offset_in` (angular/diameter/linear/radial), `dim_type.
   365|     text_offset_from_leader_in` (all 3 spot partitions), `dim_type.
   366|     text_offset_from_symbol_in` (spot_elevation/spot_coordinate only — not spot_slope,
   367|     consistent with spot_slope having no `symbol_name` field either), `dim_type.
   368|     dimension_string_type`/`dim_type.show_opening_height` (linear only), `dim_type.
   369|     rotate_with_component` (all 3 spot partitions), `dim_type.elevation_base`
   370|     (spot_elevation only), `dim_type.coordinate_base` (spot_coordinate only).
   371|   All new fields are non-required/non-blocking enrichment (added to each partition's
   372|   `identity_items`/hash preimage but never `required_qs`) — a conservative posture for
   373|   a mass field-addition pass without a live Revit run to per-family-verify blocking
   374|   behavior against. New ElementId-referenced sig_hash/name fields (`leader_tick_mark_sig_hash`,
   375|   `witness_line_tick_mark_sig_hash`, `centerline_tick_mark_sig_hash`,
   376|   `interior_tick_mark_sig_hash`, `centerline_pattern_sig_hash`, `centerline_symbol_name`,
   377|   `leader_arrowhead_uid`/`_name`/`_sig_hash`) are added to each partition's existing
   378|   "missing is acceptable" status-reason exemption set (same treatment as the
   379|   pre-existing `dim_type.tick_mark_sig_hash`), since a negative/absent reference
   380|   legitimately means "none selected," not a data gap. All 4 of `_read_arrowhead_ref_sig_hash()`
   381|   (`leader_tick_mark_sig_hash`/`witness_line_tick_mark_sig_hash`/`centerline_tick_mark_sig_hash`/
   382|   `interior_tick_mark_sig_hash`), `_read_leader_arrowhead()`'s `sig_hash` element, and
   383|   `_read_line_pattern_ref_sig_hash()` (`centerline_pattern_sig_hash`, both its positive-id and
   384|   negative/built-in branches) distinguish a genuine "no reference selected" state
   385|   (`ITEM_Q_MISSING`, still exempted) from a *real* reference — positive custom id, or a
   386|   negative built-in id like "Solid" — that could not be resolved because its ctx dependency
   387|   map (`arrowheads_by_type_id`/`line_pattern_uid_to_hash`/`line_pattern_special_values`) was
   388|   never populated or doesn't cover that id (`ITEM_Q_UNREADABLE`, **not** exempted). Without
   389|   this distinction, distinct custom/built-in tick marks, arrowheads, or centerline patterns
   390|   that are all unresolved for the same reason (e.g. `arrowheads`/`line_patterns` excluded
   391|   from a given run's domain allowlist) would silently collapse to the same identity value
   392|   and hash instead of degrading the record (PR #412 review, two rounds:
   393|   `core/dimension_type_helpers._read_line_pattern_ref_sig_hash()`'s positive-id branch first,
   394|   then its negative/built-in branch plus `_read_arrowhead_ref_sig_hash()`/
   395|   `_read_leader_arrowhead()` in a follow-up round).
   396|   **Hash-breaking (content-driven, not an algorithm change):** new identity items are
   397|   included in each partition's `identity_items`/`serialize_identity_items()` preimage by
   398|   construction (matching the existing `loaded_family_types`/Area 12 precedent above), so
   399|   `sig_hash` changes for every record across all 7 `dimension_types_*` domains; full
   400|   re-extraction required (D-015 "hash-breaking" precedent). **Exception:** in the 3 spot
```
