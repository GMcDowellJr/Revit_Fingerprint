# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 2 of 7
- Original line range: 391-796
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
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
   401|   partitions, `dim_type.leader_arrowhead_uid`/`_name` are excluded from the sig_hash
   402|   preimage specifically (filtered out immediately before `serialize_identity_items()`) even
   403|   though they remain in `identity_items`/`identity_basis.items` for visibility — a raw
   404|   Revit `UniqueId` and a cosmetic name are file-local/presentation metadata (D-004; Hash
   405|   Semantics: "Names are metadata only"), and including them would have made two files with
   406|   a semantically-identical spot dimension type (same arrowhead style/name) hash differently
   407|   purely because `UniqueId` is per-file-random. This is the fix for a real regression the
   408|   original Area 7 commit shipped: the contract's `sig_hash_keys` pin only governs the
   409|   *analysis-side* reconstruction (`core/sig_hash_builder.py`, not wired into live
   410|   extraction), so it never actually kept the uid/name out of the canonical extracted
   411|   `sig_hash`/`hash_v2` until this extractor-level exclusion was added (PR #412 review).
   412|   `contracts/domain_identity_keys_v2.json`'s 7 `dimension_types_*` blocks are updated
   413|   (`allowed_keys` only — no new `required_keys`); the 3 spot domains additionally get an
   414|   explicit `sig_hash_keys` override (hand-patched, not regenerated via
   415|   `tools/generate_sig_hash_policy.py`, to avoid clobbering unrelated hand-tuned notes
   416|   on other domains — same precedent as the `loaded_family_types`/Area 12 entry above)
   417|   pinning the analysis-side sig_hash preimage to exclude `dim_type.leader_arrowhead_uid`/
   418|   `_name` (file-local/cosmetic) while retaining `dim_type.leader_arrowhead_sig_hash`
   419|   itself (content-derived, already an `allowed_keys` entry before this change) —
   420|   same silent-drift risk `tools/generate_sig_hash_policy.py` defaulting the preimage to
   421|   every `allowed_keys` entry that the Area 10 `text_types` `leader_arrowhead` pin
   422|   addressed. `policies/domain_sig_hash_policies.json`'s 7 `dimension_types_*` blocks are
   423|   hand-patched to match (same clobber-avoidance rationale).
   424|   **Schema version bump (PR #412 review, 4th round):** all 7 `dimension_types_*` blocks'
   425|   `sig_hash_schema` is bumped from the implicit `.sig_hash.v1` to an explicit `.sig_hash.v2`
   426|   in both `contracts/domain_identity_keys_v2.json` and the hand-patched
   427|   `policies/domain_sig_hash_policies.json`, matching the `loaded_family_types`/Area 12
   428|   precedent — the analysis-side hash preimage (`allowed_items`) widened for every record in
   429|   all 7 partitions, so consumers comparing pre-/post-change extractions need the version to
   430|   distinguish real drift from an incompatible hash-definition change.
   431|   **Label-synthesis/join-key exclusions (PR #412 review, 4th round):**
   432|   `tools/label_synthesis/domain_prompts/dimension_types.py`'s `_SKIP_KEYS` gains
   433|   `dim_type.leader_arrowhead_uid`/`_name` (same treatment as the pre-existing
   434|   `tick_mark_uid` entries) so a per-file-random UID and cosmetic name aren't presented to
   435|   label-synthesis prompts as behavioral parameters. The 3 spot domains'
   436|   `explicitly_excluded_items` in `policies/domain_join_key_policies.json` gain the same two
   437|   keys, matching `text_types`' existing `leader_arrowhead_uid`/`_name` exclusions, so
   438|   `tools/discover_join_policy.py`'s `discover`/`harsh` modes can't nominate them as join-key
   439|   candidates. `_OPAQUE_KEYS` also gains the other 5 new `*_sig_hash` reference-hash fields
   440|   (`leader_tick_mark_sig_hash`, `witness_line_tick_mark_sig_hash`,
   441|   `centerline_tick_mark_sig_hash`, `interior_tick_mark_sig_hash`,
   442|   `centerline_pattern_sig_hash`), matching the pre-existing `tick_mark_sig_hash`/
   443|   `leader_arrowhead_sig_hash` treatment (PR #412 review, 5th round): the raw digest isn't
   444|   interpretable, so label-synthesis prompts show a presence note instead of the value,
   445|   keeping canonical labels dependent on the referenced configuration rather than opaque
   446|   implementation details. **Follow-up fix (PR #413 review):** adding a key to
   447|   `_OPAQUE_KEYS` alone doesn't make `_format_identity_items()` emit its presence note —
   448|   the note is only produced inside the `priority_order` loop, and the separate
   449|   remaining-items loop further down explicitly skips every `_OPAQUE_KEYS` member. Without
   450|   also adding the 5 new keys to `priority_order`, they were silently omitted from the
   451|   prompt entirely (worse than the original raw-digest bug: two configurations differing
   452|   only in these fields now produced identical prompts with no signal at all). Added all 5
   453|   to `priority_order`, verified their presence notes now emit correctly. **Second follow-up
   454|   fix (PR #413 review):** the opaque-key presence note itself was a bare, constant literal
   455|   (`"[present — consistent configuration]"`) for every value, so two records referencing
   456|   genuinely different configurations (e.g. different custom tick marks) still produced
   457|   identical prompt text for that field — losing all discriminating signal and risking
   458|   behaviorally-distinct join-hash clusters being synthesized under the same canonical
   459|   label. Changed the presence note to include a short, stable discriminator
   460|   (`ref={value[:8]}` — 8 hex chars for real digests; short symbolic values like `<Solid>`
   461|   pass through unchanged) instead of exposing the full digest or nothing at all. Applies to
   462|   all `_OPAQUE_KEYS` entries, including the 2 pre-existing ones
   463|   (`tick_mark_sig_hash`/`leader_arrowhead_sig_hash`), not just the 5 new Area 7 keys, since
   464|   the underlying formatting logic is shared.
   465| - **`loaded_family_types` domain: `structural_material_type`/`is_active` identity fields (Area 12):**
   466|   `domains/loaded_family_types.py`'s existing per-family loop now reads
   467|   `FamilySymbol.StructuralMaterialType` (via `canonicalize_str`, same
   468|   read-then-canonicalize pattern already used for `cat_name_v`/`fam_name_v`)
   469|   off `first`, the representative `FamilySymbol` for the family group — a new
   470|   read site, not a new call pattern (the Area 12 probe observed
   471|   `unique_value_count=1` for this property in every sampled family, i.e. it
   472|   does not vary by type within a family, so reading a single representative
   473|   symbol is safe). Both fields are plain-property reads confirmed `ok` 60/60
   474|   across every probed sample
   475|   (`audit_results/audit_11_domain_extractor_delta_step0_findings.md` §12); no
   476|   new `ITEM_Q_*` status category is introduced — unreadable/missing still
   477|   collapse through the file's existing `_safe_attr` + `canonicalize_*`
   478|   convention. `CanHaveStructuralSection`/`HasThermalProperties` are
   479|   explicitly out of scope for this change (zero-arg methods, not on the
   480|   probe's safety allowlist, no structural-model probe coverage yet).
   481|   `FamilySymbol.IsActive`, by contrast, **is** per-symbol (type) state — the
   482|   same probe observed both `True`/`False` within a project — so reading it
   483|   off a single `first` symbol would make the family-level record depend on
   484|   `collect_types()`'s (unordered) enumeration order, violating the
   485|   determinism invariant. Instead it is aggregated via `canonicalize_bool`
   486|   over every symbol in the family group to a `"true"`/`"false"`/`"partial"`
   487|   tri-state (mirroring the `any_true`/`all_true` -> `has_value_agg` pattern
   488|   already used in this file for `lftp.has_value`), with `ITEM_Q_UNREADABLE`
   489|   dominant over `ITEM_Q_MISSING` dominant over `ITEM_Q_OK` across the group.
   490|   Added as `lft.structural_material_type` and `lft.is_active` in
   491|   `identity_items`.
   492|   **Hash-breaking:** unlike `object_styles`/`units` (which gate `sig_hash`
   493|   through an explicit allowed-key filter), `loaded_family_types`'s `sig_hash`
   494|   is `make_hash(serialize_identity_items(identity_items))` with no filtering
   495|   step, so every item in `identity_items` already contributes to `sig_hash`
   496|   by construction — adding these 2 items necessarily changes `sig_hash` for
   497|   every record in this domain; full re-extraction required (consistent with
   498|   the existing D-015 "hash-breaking" precedent for domain-shape changes).
   499|   `contracts/domain_identity_keys_v2.json`'s `loaded_family_types.allowed_keys`/
   500|   `optional` and `policies/domain_sig_hash_policies.json`'s
   501|   `loaded_family_types.allowed_items` are updated to register both new keys
   502|   (hand-patched, not regenerated via `tools/generate_sig_hash_policy.py`, to
   503|   avoid clobbering unrelated hand-tuned notes on other domains — same
   504|   precedent as the Area 9 `object_styles` entry above), so
   505|   `validators.record_v2.validate_record_v2()` no longer reports
   506|   `identity.key.not_allowed` for either field and the analysis-side
   507|   `sig_hash` stage (`core/sig_hash_builder.py`) reconstructs the same
   508|   preimage as the extractor's inline hash instead of silently dropping the 2
   509|   new items. `policies/domain_join_key_policies.json`'s `loaded_family_types.
   510|   explicitly_excluded_items` is updated to add `lft.structural_material_type`/
   511|   `lft.is_active` — without this, `tools/discover_join_policy.py`'s default
   512|   `discover`/`harsh` modes build join-key candidates from every emitted
   513|   identity-item key not explicitly excluded, so the 2 new fields (`is_active`
   514|   in particular — operational per-symbol usage state, not a definitional
   515|   family property) would have been nominated as join-key candidates despite
   516|   not being intended as such.
   517|   **`core/sig_hash_builder.py` status-gating fix (shared, not `loaded_family_types`-only):**
   518|   the analysis-side `sig_hash` rehash stage (`tools/run_extract_all.py`'s
   519|   `sig_hash` stage, and `apply_sig_hash_policy_to_record()`) previously
   520|   derived `degraded`/`blocked` status only from `required_items` quality —
   521|   any non-required item that is still part of the hash preimage
   522|   (`allowed_items`) but not `q=ok` was silently invisible to the recomputed
   523|   status. This meant `lft.is_active` going `missing`/`unreadable` for a
   524|   family (a real, expected scenario per the Area 12 probe) would have the
   525|   extractor correctly report `status=degraded`, but the analysis-side
   526|   rehash stage would overwrite it back to `status=ok` with `status_reasons=
   527|   []`, since `is_active` is `allowed` but deliberately not `required` (see
   528|   `optional_items` reasoning above). Fixed generally in
   529|   `build_sig_hash_from_policy()`: any hashed item not in `required_items`
   530|   that is not `q=ok` now degrades status too (reason
   531|   `identity.incomplete:optional_not_ok:<k>`), while `blocked` remains gated
   532|   on `required_items` only — an optional-item read failure degrades, it
   533|   never blocks. This changes analysis-side status recomputation for every
   534|   domain that uses `core/sig_hash_builder.py`, not just `loaded_family_types`
   535|   — a deliberate, repo-owner-directed decision (PR review follow-up) over
   536|   the narrower alternative of promoting `lft.is_active` to `required_items`
   537|   for this domain only, which would have made a mid-symbol read failure
   538|   `status=blocked`/`sig_hash=None` (harsher than the extractor's own
   539|   `degraded`). New coverage:
   540|   `tests/test_sig_hash_policy_builder.py::test_sig_hash_builder_degrades_when_optional_hash_item_not_ok`.
   541|   **Schema version bump:** `sig_hash_schema` for this domain is pinned to
   542|   `loaded_family_types.sig_hash.v2` (was the generator's implicit
   543|   `...v1` default) in both `contracts/domain_identity_keys_v2.json` and
   544|   `policies/domain_sig_hash_policies.json`, following the same precedent as
   545|   the `identity` domain's D-025 `sig_hash_schema` pin — otherwise
   546|   `tools/generate_sig_hash_policy.py` regenerating the policy file would
   547|   silently drop back to the `v1` label despite the widened preimage,
   548|   making pre/post-PR hashes look like the same hash definition to
   549|   drift-detection consumers and risking a mis-reported "drift" finding.
   550|   **`is_active` missing-quality fix:** the per-symbol aggregation now checks
   551|   `any(...ITEM_Q_MISSING...)` (not `all(...)`) before falling through to the
   552|   OK-only aggregation branch — a single symbol with an unreadable `IsActive`
   553|   read no longer gets silently dropped from the readable subset (which would
   554|   have reported `q=ok` off a partially-observed family); it now correctly
   555|   propagates to `q=missing` and `status=degraded` for the whole record.
   556| - **`line_styles` domain: `parent_cat.id`/`parent_cat.name` metadata fields (Area 11):**
   557|   `domains/line_styles.py`'s existing per-subcategory loop (`sc`, iterating
   558|   `Category.SubCategories` under `OST_Lines`) now also reads `sc.Parent`
   559|   (the same `Category.Parent` call pattern already used by
   560|   `domains/object_styles.py:440` for its purge-lookup helper — no new
   561|   traversal idiom introduced) and emits `line_style.parent_cat.id`
   562|   (`canonicalize_int` of `Id.IntegerValue`) and `line_style.parent_cat.name`
   563|   (`canonicalize_str` of `Name`). A subcategory with no parent (a genuinely
   564|   top-level category) canonicalizes `None` to `q=missing`/`v=null`, not
   565|   `unreadable` — actual API read failures still canonicalize to
   566|   `unreadable`. **Bucket placement**: added to `phase2.unknown_items`,
   567|   matching the existing treatment of `line_style.source_element_id`/
   568|   `source_unique_id` in this file (metadata/traceability, never in
   569|   hash/sig/join) rather than `identity_basis.items`. This was an open
   570|   design question per the task scoping and is being resolved here as
   571|   "cosmetic/traceability metadata, not identity" — flagging for
   572|   confirmation rather than assuming it's settled; if `parent_cat` should
   573|   instead be identity-affecting, that's a separate, deliberate follow-up.
   574|   **Non-hash-breaking:** not added to `LINE_STYLE_SEMANTIC_KEYS`, so
   575|   `sig_hash`/`join_key` are unchanged. `contracts/domain_identity_keys_v2.json`'s
   576|   `line_styles.allowed_keys` is intentionally left unchanged — it does not
   577|   currently list `source_element_id`/`source_unique_id` either, since that
   578|   registry governs `identity_basis.items`/sig-hash-policy keys, not the
   579|   phase2 `unknown_items` bucket.
   580| - **`object_styles` domain family: 5 per-category identity fields (Area 9):**
   581|   `domains/object_styles.py`'s existing per-`Category` loop (shared by all 4
   582|   `object_styles_*` partitions) now reads `Category.CanAddSubcategory`,
   583|   `Category.HasMaterialQuantities`, and `Category.IsCuttable` off the same
   584|   `cat_obj` already used for line weight/color/pattern reads (canonicalized
   585|   via `core.record_v2.canonicalize_bool`), plus a new `obj_style.parent_name`
   586|   identity item derived from the loop's already-tracked `parent` variable
   587|   (not a fresh `cat.Parent` call, which returns missing for top-level
   588|   categories in this API — see `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
   589|   §9.2). `parent_name` is `q=ok`/`v=<name>` for subcategories and
   590|   `q=missing`/`v=None` for genuinely top-level categories, per the
   591|   record.v2 sentinel policy (`v: null` + `q: "missing"` for a confirmed
   592|   absent value, never a bare sentinel literal) and matching the probe's
   593|   own classification (§9.3: `ok=364; missing=279` for top-level rows) —
   594|   not `unreadable`, which stays reserved for actual read failures. A 5th
   595|   field, `obj_style.tab` (Model/Annotation/
   596|   Analytical/Imported), is derived from which `extract_*`/`kind` partition
   597|   produced the record (no new Category read) and added to the existing
   598|   `phase2.coordination_items` bucket, matching `obj_style.category_type`/
   599|   `obj_style.is_subcategory` — **not** hash-contributing.
   600|   **Non-hash-breaking:** `can_add_subcategory`/`has_material_quantities`/
   601|   `is_cuttable`/`parent_name` are added to `identity_basis.items` but not to
   602|   `_MODEL_SEMANTIC_KEYS`/`_NON_MODEL_SEMANTIC_KEYS`, so the extractor's
   603|   inline `sig_hash` is unchanged. `contracts/domain_identity_keys_v2.json`'s
   604|   `object_styles_{model,annotation,analytical,imported}.allowed_keys`
   605|   updated to register all 4 new keys; a new `sig_hash_keys` override pins
   606|   each domain's sig-hash preimage to its pre-existing key set so the
   607|   analysis-side `policies/domain_sig_hash_policies.json` (hand-patched, not
   608|   regenerated, to avoid clobbering unrelated hand-tuned notes on other
   609|   domains) doesn't silently widen via `allowed_items` defaulting from
   610|   `allowed_keys`.
   611|   **Open question, not resolved here:** whether `obj_style.tab` should stay
   612|   a coordination-bucket field (current default, conservative, matches the
   613|   `dim_type.leader_arrowhead_sig_hash` allowed-not-required precedent) or
   614|   be promoted to a true hash-contributing identity item — flagged for Greg
   615|   to confirm; if promoted, that is a separate, deliberate hash-breaking
   616|   follow-up, not an accidental side effect of this change.
   617| - **`units` domain: 6 per-spec boolean formatting flags:**
   618|   `domains/units.py`'s existing per-spec `FormatOptions` loop now reads
   619|   `units.use_default`, `units.use_digit_grouping`, `units.use_plus_prefix`,
   620|   `units.suppress_leading_zeros`, `units.suppress_spaces`,
   621|   `units.suppress_trailing_zeros` off the same `fmt` object already used
   622|   for `accuracy`/`rounding_method`, canonicalized via
   623|   `core.record_v2.canonicalize_bool` (newly imported). Optional (not
   624|   required — `q=unreadable` on a `FormatOptions` read failure, mirroring
   625|   `accuracy`/`rounding_method`, never blocks the record).
   626|   **Hash-breaking:** all 6 keys added to `UNITS_SEMANTIC_KEYS`, so every
   627|   `units` per-spec `sig_hash` changes going forward — these are genuine
   628|   numeric-formatting behavior properties, not presentation/naming, so
   629|   belong in the hash per the existing `accuracy`/`rounding_method`
   630|   precedent. Also added to the `phase2.semantic_keys` hypothesis bucket
   631|   (alongside `rounding_method`) so they don't fall into `unknown_items`.
   632|   `contracts/domain_identity_keys_v2.json`'s `units.allowed_keys` and
   633|   `policies/domain_sig_hash_policies.json`'s `units.allowed_items`
   634|   updated accordingly (the latter hand-patched, not regenerated, to avoid
   635|   clobbering unrelated hand-tuned notes on other domains — same approach
   636|   as the `identity`/D-025 entry below). `required_keys` (`units.spec`,
   637|   `units.unit_type_id`) unchanged; existing `accuracy`/`rounding_method`/
   638|   `spec`/`symbol_type_id`/`unit_type_id` values and statuses unchanged.
   639| - **New `units_doc` domain: doc-level formatting fields:**
   640|   `domains/units.py` gains `extract_units_doc(doc, ctx=None)`, a second,
   641|   independent top-level domain (`domain="units_doc"`) emitting a single
   642|   synthetic record `record_id="units:_doc"` with
   643|   `units_doc.decimal_symbol` / `units_doc.digit_grouping_amount` /
   644|   `units_doc.digit_grouping_symbol`, read off `doc.GetUnits()` directly
   645|   (`Units.DecimalSymbol`/`DigitGroupingAmount`/`DigitGroupingSymbol`) —
   646|   document-level cardinality, not per-spec. `decimal_symbol` and
   647|   `digit_grouping_symbol` are Revit API enums, canonicalized via
   648|   `canonicalize_enum` (same as `units.rounding_method`);
   649|   `digit_grouping_amount` is an int, via `canonicalize_int` (newly
   650|   imported). All 3 fields optional/never-block, matching
   651|   `worksets_doc.active_workset_name`'s degraded-not-blocked precedent —
   652|   `status=degraded` (not `blocked`) with `sig_hash` still computed if
   653|   `doc.GetUnits()` itself fails.
   654|   **Why a separate domain, not a record folded into `domain="units"`:**
   655|   a bare `record_id="units:_doc"` under `domain="units"` fails contract
   656|   validation — `units.required_keys` (`units.spec`, `units.unit_type_id`,
   657|   `block_if_any_required_not_ok`) has no conditional-required exemption,
   658|   and a doc-level record has neither field to supply. Confirmed by direct
   659|   test before deciding the fix (`identity.required_key.missing:units.spec`,
   660|   `identity.required_key.missing:units.unit_type_id`,
   661|   `identity.key.not_allowed:units.decimal_symbol` etc.). Mirrors
   662|   `domains/worksets.py`'s `worksets`/`worksets_doc` split exactly (same
   663|   problem, same resolution, same rationale in that file's own docstring)
   664|   — a `worksets_doc`-style sibling domain, wired independently in
   665|   `runner/run_dynamo.py` (`_enabled("units_doc")` →
   666|   `units.extract_units_doc`), with its own contract entry
   667|   (`required_keys: []`, `block_if_any_required_not_ok: false`) in
   668|   `contracts/domain_identity_keys_v2.json`, its own hand-patched entry in
   669|   `policies/domain_sig_hash_policies.json`, and its own entry in
   670|   `policies/domain_join_key_policies.json` (all fields optional, no
   671|   required discriminator — same rationale as `worksets_doc`'s policy
   672|   notes). This was flagged as a design decision rather than a mechanical
   673|   add (see `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
   674|   §8.3) and confirmed with the requester before implementing.
   675| - **`identity` domain expansion: `project_info.*` fields (D-025):**
   676|   `domains/identity.py` now reads `doc.ProjectInformation` and adds 13 new
   677|   identity items to its existing single `record_id="document"` record.
   678|   Twelve are read via the `BuiltInParameter` enum (locale-independent,
   679|   unlike matching by display name): `project_info.name`
   680|   (`ProjectInformation.Name`), `.number`, `.status`, `.address`,
   681|   `.issue_date`, `.client_name`, `.building_name`, `.organization_name`,
   682|   `.organization_description`, and `.ifc_building_guid`/`.ifc_project_guid`/
   683|   `.ifc_site_guid` (`IFC_BUILDING_GUID`/`IFC_PROJECT_GUID`/`IFC_SITE_GUID`,
   684|   confirmed real built-ins via `tools/archetype/bip_lookup.json`). All twelve
   685|   report `q=unreadable` if the `Parameter` object itself is absent (an
   686|   unexpected API/document gap) and `q=missing` if it's present but blank.
   687|   The remaining field, `.office` (Stantec's shared parameter), is read via
   688|   its confirmed GUID (`Element.get_Parameter(Guid("6b61afc7-13eb-4af5-8b65-
   689|   889f978af4f3"))`) rather than `LookupParameter("Office")` by display name,
   690|   which the Revit API can otherwise resolve to an arbitrary same-named
   691|   parameter if a project contains more than one "Office" definition; it
   692|   reports `q=unsupported.not_applicable` (not `q=unreadable`) when the
   693|   shared parameter definition isn't loaded — the expected, legitimate state
   694|   on any non-Stantec-template project, not a read failure.
   695|   **Hash-breaking:** these fields are included in `identity_items` /
   696|   `identity_basis.items` / `sig_hash` (an explicit, documented exception to
   697|   the "names are metadata only" default rule — see D-025 for the full
   698|   rationale and the mitigations applied: excluded from the join-key policy,
   699|   excluded from status/status_reasons/identity_quality computation). Every
   700|   `identity` domain `sig_hash` changes going forward; previously captured
   701|   values are not comparable — `identity.py`'s `sig_basis.schema`
   702|   (`identity.sig_basis.v1` → `.v2`) and both
   703|   `contracts/domain_identity_keys_v2.json`'s and the hand-patched
   704|   `policies/domain_sig_hash_policies.json`'s `sig_hash_schema`
   705|   (`identity.sig_hash.v1` → `.v2`) are bumped so a consumer comparing
   706|   `sig_hash` across a pre-D-025 and post-D-025 export can tell the two hash
   707|   definitions apart instead of reading the mismatch as fingerprint drift.
   708|   `phase2.semantic_keys` ("Phase-2 behavior-defining") stays exactly the
   709|   pre-D-025 `is_workshared`/`revit_version_number`/`revit_build` core —
   710|   `project_info.*` is naming/label metadata, not Phase-2-semantic content —
   711|   while a separate `sig_basis.keys_used` selector correctly lists every
   712|   `identity_items` key actually hashed (fixing a pre-existing drift where
   713|   `identity.revit_version_name` was hashed but missing from that list).
   714|   Office's Address/City/State/Zip/Country/Telephone/Fax/Legal Entity
   715|   sub-fields are deliberately NOT implemented pending confirmation of their
   716|   exact parameter names against a real Stantec-template project (no live
   717|   Revit/Dynamo access was available to confirm them).
   718|   `contracts/domain_identity_keys_v2.json` and
   719|   `policies/domain_join_key_policies.json`/`policies/domain_sig_hash_policies.json`
   720|   updated accordingly (the latter hand-patched, not regenerated, to avoid
   721|   clobbering unrelated hand-tuned notes on other domains). No change to
   722|   `file_metadata.csv`, `tools/build_segment_manifest.py`, or any governance
   723|   narrative file — confirmed zero overlap in Step 0
   724|   (`audit_results/audit_11_domain_extractor_delta_step0_findings.md` §5).
   725|   Refined through several PR-review rounds after the initial implementation
   726|   (which read `Office` and the three IFC GUID fields by display name via
   727|   `LookupParameter`, and left the hash schemas unversioned) — see DECISIONS.md
   728|   D-025 for the full history of what changed and why.
   729| - **Name-target bundle BI output location correction (PR3 follow-up):**
   730|   `run_bundle_analysis_for_target()` (`tools/bundle_analysis/run_bundle_analysis.py`) now
   731|   relocates the `--comparison-target name` leg's completed ALL-view output from the
   732|   internal `out_dir/name/all/` staging path to a flat `out_dir/name_all/` as its final
   733|   step (self-clearing any stale `name_all/` from a previous run first). This matches the
   734|   Power BI model (`Fingerprint_Segmented_Bundles.vpax`)'s confirmed `pPurgeView`
   735|   convention -- a free-text parameter spliced in as a single path segment
   736|   (`<segment>\results\bundle_analysis\<pPurgeView>\*_combined.csv`) -- which the
   737|   previous two-segment `name/all/` location could never satisfy.
   738|   `tools/run_segment_orchestrator.py`'s name-leg BI merge and
   739|   `_segment_has_name_leg_output()` marker-file check were updated to the new path. New
   740|   `annotate_name_target_combined_files()`
   741|   (`tools/bundle_analysis/name_projection_adapter.py`), called once per segment right
   742|   after the name-leg `merge_bi_outputs()` call, appends `comparison_target` /
   743|   `coverage_class` / `provenance_note` columns to every `*_combined.csv` under
   744|   `name_all/` -- strictly additive to the existing typed columns the Power BI model's
   745|   `Table.TransformColumnTypes` steps already read by name, never inserted/renamed/
   746|   reordered -- so a report author can point `pPurgeView` at `name_all` and get the same
   747|   ten filenames as `all`/`used` today, now carrying per-row name-projection provenance.
   748|   `comparison_target=config` output (`out_dir/all`, `out_dir/used`, and under `both`,
   749|   `out_dir/config/...`) is completely unchanged -- the relocation/annotation code paths
   750|   are gated entirely inside the name-leg branch. See
   751|   `audit_results/audit_10_bundle_bi_output_location_correction.md`.
   752| - **Canonical Name Identity Projection (PR1):** a second, independent, policy-driven
   753|   `join_hash` variant computed via the same `core/join_key_builder.build_join_key_from_policy()`
   754|   mechanism used by the existing configuration-based `join_hash`, governed by a new,
   755|   separate policy file (`policies/domain_name_key_policies.json`) and namespaced under
   756|   its own `join_key_schema` (`"name_identity.join_key.v1"`, or
   757|   `"phases.name_identity.join_key.v1.redundant"` for `phases`, whose configuration
   758|   `join_hash` already keys off the same `phase.name` string per D-010). Stored on each
   759|   eligible record as `join_key_name_identity` (same `{schema, hash_alg, join_hash, status,
   760|   ...}` shape as the existing `join_key`), computed inline at export time in
   761|   `domains/{phases,materials,text_types,compound_types,identity,phase_filters,
   762|   line_patterns,fill_patterns,arrowheads,loaded_family_types,view_templates,
   763|   view_filter_definitions,dimension_types}.py` (13 files; `text_types.py` is the one
   764|   exception -- its "canonical mode" pipeline already treats `join_key`/`sig_hash` as
   765|   post-extraction artifacts and strips them before finalizing its output, so
   766|   `join_key_name_identity` is likewise reconstructed downstream rather than stamped
   767|   inline for that domain only). A parallel, independent analysis-side reconstruction
   768|   path (`core/name_key_builder.py` + `tools/apply_name_key_policy.py`) computes the
   769|   identical value directly from already-exported `*.details.json` records -- no
   770|   re-extraction required, since every value this projection needs (`identity_basis.items`,
   771|   phase2 bucket items, `label.display`) is already present in existing exports today.
   772|   Status companion field mirrors `join_key_status`'s closed vocabulary in spirit
   773|   (`ok`/`missing_required`/`blocked`/`missing_policy` -- `"bootstrap"` does not apply,
   774|   since this projection has no flatten-then-apply two-phase pipeline) via the new
   775|   `core.join_key_builder.compute_projection_status()` helper.
   776| 
   777|   Eligibility is an explicit allow-list (25 of 37 `domain_join_key_policies.json`
   778|   entries), not an inferred/derived rule -- see
   779|   `audit_results/audit_6_name_key_step0_within_pr1.md` for the full per-domain trace.
   780|   For 7 domains (`phases`, `materials`, `text_types`, `wall_types`, `floor_types`,
   781|   `roof_types`, `ceiling_types`) the record's own name is already a native
   782|   `identity_items` key. For 18 domains, the record's own name is real and reaches
   783|   `label.display` today but was never captured as a flat `identity_items` key -- it
   784|   either lives only in a phase2 bucket (`identity`, `phase_filters`, `line_patterns`,
   785|   `fill_patterns` x2) or nowhere but `label.display`/`label.components.*`
   786|   (`arrowheads`, `loaded_family_types`, `view_templates` x5, `view_filter_definitions`,
   787|   `dimension_types` x5) -- these use a *locally widened* items list (the domain's
   788|   existing `identity_items` plus one freshly-`make_identity_item`-wrapped value, built
   789|   only for the name-key call) rather than touching `identity_basis.items`, `sig_hash`,
   790|   or the existing `join_key`. 12 domains/partitions are excluded entirely: 9 have no
   791|   name-like value at all (`units`, `line_styles`, `object_styles` x4,
   792|   `view_category_overrides` x3), 2 have only a referenced-element name rather than
   793|   their own (`dimension_types_spot_coordinate`/`_spot_elevation`), and 1
   794|   (`view_filter_applications_view_templates`) has only a UID-preferring composite
   795|   candidate, not a name string.
   796| 
```
