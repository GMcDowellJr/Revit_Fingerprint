# Chunk of audit_results/audit_11_domain_extractor_delta_step0_findings.md

- Source relative path: `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
- Chunk: 2 of 2
- Original line range: 401-517
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 01f11c8c682797050e9ae4b111c8ac4d175573981d3e9c5285b02c1256fd9bbd
- Starts inside symbol: no
- Ends inside symbol: no

```
   401| `decimal_symbol`, `digit_grouping_amount`, `digit_grouping_symbol` come from the `Units` object `u` directly (confirmed via the probe's reflection sweep: `refl.Units.DecimalSymbol`/`DigitGroupingAmount`/`DigitGroupingSymbol`, all `q=ok`), not from any per-spec `FormatOptions`. Grepping `domains/units.py` for `decimal_symbol`/`digit_grouping`/`DecimalSymbol`/`DigitGrouping` returns zero matches — these are genuinely absent from the extractor today, not just unwired.
   402| 
   403| Because every existing record is keyed to a spec label, adding these 3 doc-level fields has no natural home among the current 38 per-spec records — but **a bare synthetic doc-level record (e.g. `record_id = "units:_doc"`) is not a viable fix as stated**, and this document originally proposed one without checking the contract. `contracts/domain_identity_keys_v2.json`'s `units.required_keys` is `["units.spec", "units.unit_type_id"]`, and `validators/record_v2.py:218-231` treats a missing required key as a hard violation (`identity.required_key.missing:{rk}`) that blocks the record. A genuine document-level record has neither a real `spec` nor a real `unit_type_id` to supply, so it would either fail contract validation as-is or need one of: (a) a contract change making `units.spec`/`units.unit_type_id` conditionally required (not required for a designated doc-level `record_id`), (b) placing these 3 fields on a different, non-`units` domain (e.g. folded into `identity.py`'s existing single document record), or (c) some other record-shape accommodation not yet designed. Whichever path is chosen is itself a small design decision, not a mechanical add — flagging it as such rather than prescribing the doc-level record as a given.
   404| 
   405| ### 8.4 Probe confirmation
   406| 
   407| `tools/probes/probe_units.py`'s `FORMAT_SURFACE` (lines 223-240) declares all 6 flags (plus `suppress_unit_symbol`, not on the promotion list) as `prop_bool` reads off `fmt`. `UNITS_GLOBAL_SURFACE` (lines 243-247) declares `decimal_symbol`/`digit_grouping_amount`/`digit_grouping_symbol` as `prop_any` reads off `u` (the `Units` object), separately from the per-spec surface — reinforcing the doc-level-vs-per-spec split.
   408| 
   409| Observed values (`tools/probes/Exports/PROBE_INVENTORY.csv`, Revit 2025, 3 runs): all 6 flags at `ok=450; unreadable=3` (lines 1966-1974), example `0`/`False`, pure API-property reads (no BuiltInParameter path observed or used). `p.units.decimal_symbol`/`digit_grouping_amount`/`digit_grouping_symbol` each at `ok=3` (lines 1975-1977), sampled once per probe run (global, not per-spec) — confirming doc-level cardinality.
   410| 
   411| ### 8.5 Contract check
   412| 
   413| `contracts/domain_identity_keys_v2.json`'s `units.allowed_keys` currently lists only `units.spec`, `units.unit_type_id`, `units.symbol_type_id`, `units.accuracy`, `units.rounding_method`. None of the 6 flags nor the 3 doc-level fields appear — genuinely new (unlike the `floor_types.function` false-positive in Area 2).
   414| 
   415| ---
   416| 
   417| ## 9. `object_styles` additions
   418| 
   419| ### 9.1 Category iteration loop (`domains/object_styles.py`)
   420| 
   421| The shared loop is `for cat, is_subcategory, parent in list(cats or []):` at line 271, inside `_extract_object_styles` — shared by all 4 partitions (`extract_model`/`extract_annotation`/`extract_analytical`/`extract_imported`, lines 530-543, which all call this one function with different `kind`/flags). `cats` is built once per document via `_collect_categories` (lines 71-116), cached on `ctx` (line 73), then filtered per-category-type inside the loop (`_matches_category_type`, line 273).
   422| 
   423| ### 9.2 Same loop, no new collector pass needed
   424| 
   425| `cat` (aliased `cat_obj` at line 295) is already the working object for the whole loop body (271-481). Existing reads directly off it include `cat_obj.GetLineWeight(...)` (323), `cat_obj.LineColor` (339), `cat_obj.GetLinePatternId(...)` (352), `cat_obj.CategoryType` (440), `cat_obj.Id`/`.UniqueId` (413, 452, 456), and `getattr(cat_obj, "Material"/"MaterialId", None)` in `_material_ref_item` (160/171). The 5 candidate fields are the same shape:
   426| 
   427| - `can_add_subcategory` → `Category.CanAddSubcategory`, a plain boolean property on the same `cat`/`cat_obj`
   428| - `has_material_quantities` → `Category.HasMaterialQuantities`, same
   429| - `is_cuttable` → `Category.IsCuttable`, same
   430| - `parent_name` → **already computed in-loop, but the existing variable is NOT directly promotable as-is** — line 278: `parent_name = canon_str(getattr(parent if is_subcategory else cat, "Name", None))`, used today to build `row_key` (296) and the record label. For a top-level (non-subcategory) row, this line deliberately assigns the category's *own* name — `row_name` is separately set to the literal string `"self"` for that case — so the existing `parent_name` local encodes "self-referential parent, for row_key purposes" for top-level categories, not "no parent." Directly promoting this variable to a new `parent_name` identity item would misrepresent every top-level category as its own parent, contrary to the probe's `c.parent_name` (`probe_object_styles.py:284`), which reads `parent` and is correctly `missing` for the 279 top-level rows in the probe data. A correct new identity item must derive from `parent` only when `is_subcategory` is true, and emit missing/`None` otherwise — it can reuse the existing tracked `parent` value from `_collect_categories`, just not the existing `parent_name` local verbatim. (`Category.Parent` itself is not the right source either — the probe's own reflection sweep shows `refl.Category.Parent` returns `q=missing` for top-level categories, consistent with the tracked-`parent`-variable approach being the only reliable path, just applied correctly this time.)
   431| - `tab` → not an API property; the probe computes it heuristically via `_infer_object_styles_tab` (`probe_object_styles.py:186-207`, using `CategoryType` + name-string matching for "Imported"/"Annotation"/"Analytical"/"Model"/"Other"). In production this is effectively already implicit in which of the 4 `extract_*`/`kind` partitions emitted the record (`kind` param, line 217; `_matches_category_type`, line 126) — an explicit `tab` field would be a derived local computation from data already in scope (`kind`/`cat.CategoryType`), not a new object read.
   432| 
   433| So: `can_add_subcategory`/`has_material_quantities`/`is_cuttable` are additional `getattr(cat_obj, "...", None)` reads inserted into the existing loop body (near the other `cat_obj`-derived items, lines 300-386); `parent_name` needs a *new* identity item derived from the already-tracked `parent` value (correctly `None` for top-level categories), not a verbatim promotion of the existing `parent_name` local; `tab` needs no new Category read at all. **No new `FilteredElementCollector` pass and no separate category-lookup pass required for any of the 5** — all the needed data is already in scope in the existing loop, but `parent_name` needs new derivation logic, not just a promotion.
   434| 
   435| ### 9.3 Probe confirmation
   436| 
   437| `tools/probes/probe_object_styles.py`'s `PARAM_DEFS` (lines 282-313): `c.can_add_subcategory` (310, `cat.CanAddSubcategory`) → CSV `ok=643`, example `True`; `c.has_material_quantities` (311) → `ok=643`, example `False`; `c.is_cuttable` (312) → `ok=643`, example `False`; `c.parent_name` (284, `_get_name(parent)`) → `ok=364; missing=279` (missing for top-level, non-subcategory rows), example `"MEP Fabrication Ductwork"`; `c.tab` (288) → `ok=643`, example `"Analytical"`. Reflection rows corroborate: `refl.Category.CanAddSubcategory`/`HasMaterialQuantities`/`IsCuttable` all `q=ok`; `refl.Category.Parent` is `q=missing` — confirming `parent_name` must stay derived from the tracked `parent` variable, not `cat.Parent`, or it would go missing for many rows.
   438| 
   439| ### 9.4 Contract check
   440| 
   441| `contracts/domain_identity_keys_v2.json`'s `allowed_keys` for all 4 `object_styles_*` domains currently list only `obj_style.row_key`, `.weight.projection`, `.weight.cut` (model only), `.color.rgb`, `.pattern_ref.sig_hash`, `.material_sig_hash` (model only). None of the 5 candidate fields appear — genuinely new/unregistered.
   442| 
   443| ### 9.5 Open question
   444| 
   445| Whether `tab` should be modeled as a derived/coordination field (like the existing `obj_style.category_type` coordination item, line 446) versus a true identity item is a design choice not resolvable from the code alone.
   446| 
   447| ---
   448| 
   449| ## 10. `text_types` — `Leader Arrowhead`
   450| 
   451| **Stop-and-report — this field already exists, same pattern as Area 2.** `Leader Arrowhead` is already fully implemented in `domains/text_types.py`: read, cross-referenced against the `arrowheads` domain, and emitted as metadata — but deliberately excluded from the identity/sig-hash surface.
   452| 
   453| - **Flat items shape confirmed**: `text_types.py` uses `core.canonical_items.build_flat_items` (imported 48-50, called 537-543) to assemble `rec_v2["items"]` from `identity_basis.items` plus the phase2 semantic/cosmetic/coordination/unknown buckets, then strips `identity_basis`/`phase2`/`join_key`/etc. (544-548). "Follows the existing param-read pattern" here means: read via `first_param`, then route the result into the phase2 `unknown_items` bucket (never `identity_items_v2`/`TEXT_TYPE_SEMANTIC_KEYS`), which `build_flat_items` folds into the flat `items:[{k,v,q}]` array with the item's bucket-derived role.
   454| - **Revit parameter**: `BuiltInParameter.LEADER_ARROWHEAD`, UI-name fallback `"Leader Arrowhead"` — `domains/text_types.py:328`: `first_param(t, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])`. Confirmed by probe: `tools/probes/Exports/PROBE_INVENTORY.csv:1900` — storage `ElementId`, `ok=13` across all 3 probe runs, example `"22 Degree Filled Arrow-Medium"`.
   455| - **Read path**: `p.AsElementId()` → `doc.GetElement(arrow_id)` → resolves to the arrowhead element (`text_types.py:330-334`).
   456| - **Cross-reference to `arrowheads.py`**: already wired via `ctx["arrowheads_by_type_id"]`, populated in `domains/arrowheads.py:298,648` and consumed in `text_types.py:337-340` to attach `leader_arrowhead_sig_hash`. The same ctx-key pattern is reused by `core/dimension_type_helpers.py:521,547` for `dimension_types` (`dim_type.leader_arrowhead_sig_hash`, `contracts/domain_identity_keys_v2.json:237,281,317`).
   457| - **Param-read helper**: `first_param(elem, bip_names=None, ui_names=None)` at `core/rows.py:134-171` — tries `BuiltInParameter` by name first, falls back to `LookupParameter` by UI name. This is the exact helper `text_types.py` already uses throughout (e.g. lines 266, 272, 275, 278, 328).
   458| - **Identity/contract status**: `contracts/domain_identity_keys_v2.json:711-724` (`text_types.allowed_keys`) does **not** include any `leader_arrowhead*` key — confirmed by an explicit code comment at `text_types.py:372`: *"Leader Arrowhead is excluded from v2 by policy decision."* It lives only in phase2 `unknown_items` and thus in the flat `items` array with `q="ok"`/`v=None` as a valid tri-state "no arrowhead" signal, or populated uid/name/sig_hash when present. Also excluded from `debug.uid_excluded_from_sig`/`leader_arrowhead_uid_excluded_from_sig` (line 533).
   459| 
   460| **Conclusion**: there is no new field to add. If the intent was promoting `leader_arrowhead` into the v2 identity surface, that's a hash/policy-scope change, out of scope for this pass (same shape as the `floor_types.function` finding in Area 2).
   461| 
   462| **Errata note**: `tools/probes/PROBE_EXPORTER_DELTA.md` also lists `Leader Arrowhead` as the one delta field for `text_types`. Same root cause as the Area 2 and Area 7 errata: `git blame` on `domains/text_types.py:328` shows the read was added in commit `26523e5` (2026-07-15 19:34:13 -0700), three weeks before that document's 2026-08-05 probe run.
   463| 
   464| ---
   465| 
   466| ## 11. `line_styles` — `parent_cat`
   467| 
   468| **No existing `.Parent` traversal in this file.** `domains/line_styles.py` iterates `Category.SubCategories` directly (line 208: `subs = list(getattr(lines_cat, "SubCategories", []) or [])`), so the loop variable `sc` is already a `Category` object (not a `GraphicsStyle`) — it calls `sc.GetLineWeight(...)`, `sc.LineColor`, `sc.GetLinePatternId(...)` directly as `Category` members (lines 231-241, 291). There is no `GraphicsStyle → Category` walk anywhere in this file to reuse. Adding `parent_cat` would be a net-new lookup in this file, but a trivial one — `sc.Parent` (`Category.Parent`), not the fuller `GraphicsStyle.GraphicsStyleCategory → Category.Parent` chain the prompt's premise assumed.
   469| 
   470| A near-identical `.Parent` call already exists in a **sibling domain**: `domains/object_styles.py:415` — `parent_cat_obj = cat_obj.Parent` (try/except at 414-417), used there for a purge-lookup helper (`_subcategory_purge_lookup`), not to emit a `parent_cat` identity field. This confirms `Category.Parent` is an established, low-risk call pattern in this codebase generally — just not yet used to emit a field in `line_styles.py` specifically.
   471| 
   472| The probe file does walk `GraphicsStyle → Category → Parent`, but that's a different object graph than the domain file's `sc`: `tools/probes/probe_line_styles.py:406-407` (`c = gs.GraphicsStyleCategory`; `parent = c.Parent`), where `gs` is a `GraphicsStyle` obtained differently than `line_styles.py`'s `sc`.
   473| 
   474| **Probe evidence the field is real and populated**: `tools/probes/Exports/PROBE_INVENTORY.csv:908-909` — `v.parent_cat.id` and `v.parent_cat.name`, both `ok=150` across 3 runs, example id `-2000051` / name `"Lines"`.
   475| 
   476| **Contract check**: `contracts/domain_identity_keys_v2.json:475-493` (`line_styles.allowed_keys`) currently has only `line_style.weight.projection`, `.color.rgb`, `.weight.cut`, `.pattern_ref.sig_hash`, `.pattern_ref.synopsis` — no `parent_cat` key. Genuinely not yet registered or emitted.
   477| 
   478| **Open question**: whether `parent_cat` should land in `identity_basis.items` (identity-affecting) or a phase2 `cosmetic_items`/`unknown_items` bucket (metadata-only, matching how `line_style.source_element_id`/`source_unique_id` are handled at lines 447-458) — not resolvable from the code since the field doesn't exist yet in this file.
   479| 
   480| ---
   481| 
   482| ## 12. `loaded_family_types` additions
   483| 
   484| None of `StructuralMaterialType`, `IsActive`, `CanHaveStructuralSection`, `HasThermalProperties` currently appear in `domains/loaded_family_types.py` (confirmed absent from the full 418-line file) or in `contracts/domain_identity_keys_v2.json:494-529` (`loaded_family_types.allowed_keys` lists only `lft.shape_gate.category(_id)`, `.type_parameter_schema_hash`, `.type_parameter_count`, `family_is_in_place`, `family_is_editable`, `family_symbol_count`, `type_count`). These are genuinely new.
   485| 
   486| Evidence comes from the `reflection` probe kind in `tools/probes/probe_loaded_family_types.py`/`PROBE_INVENTORY.csv` (not the curated `param` inventory) — these are `.NET` reflection members on `FamilySymbol`, not Revit `Parameter`s.
   487| 
   488| | Field | member_kind | Invoked by probe? | Observed |
   489| |---|---|---|---|
   490| | `StructuralMaterialType` | property | yes | `ok`, 60/60 samples every run; example raw/norm=0 (`StructuralMaterialType.Undefined`), `unique_value_count=1` (never varied in sampled data) |
   491| | `IsActive` | property | yes | `ok`, 60/60 every run; both `False`/`True` observed — varies |
   492| | `CanHaveStructuralSection` | **method** | **no — `not_invoked`** | not on the probe's `_ALLOWLISTED_REFLECTION_METHODS` (`probe_loaded_family_types.py:485-518`); `_reflect_try_get` returns `_METHOD_NOT_INVOKED_SENTINEL` without ever calling it |
   493| | `HasThermalProperties` | **method** | **no — `not_invoked`** | same as above |
   494| 
   495| **Critical caveats:**
   496| 
   497| 1. **Two of the four are zero-arg methods, not properties**, and the probe's reflection sweep explicitly refuses to invoke any zero-arg method that isn't on its ground-truth-verified allowlist (rationale at `probe_loaded_family_types.py:458-484`). `CanHaveStructuralSection`/`HasThermalProperties` are not on that allowlist — there is **zero empirical evidence in this repo** of whether they throw on non-applicable categories; the probe only confirms the members exist via reflection, never their runtime behavior.
   498| 2. **No structural-discipline model in the available probe corpus.** The 60-object reflection sample is drawn from `FilteredElementCollector(doc).OfClass(Family)` sorted by name (capped at 60, `probe_loaded_family_types.py:356,374-375`). Across all 3 available probe runs, the full inventory (757 records total) is exclusively non-structural categories (Casework, Detail Items, Specialty Equipment, Generic Models, Lighting/Plumbing/Electrical Fixtures, Doors, Furniture, etc.) — confirmed via filtering the raw JSON exports; the 3 source projects (`KSRF_Hosp_Interior_AR`, `000000000_Arch_Int_ContainerModel_r25`, `2014351100_stn_arch_asc_int_dd`) are all architectural/interior models. **None of the 4 fields have been empirically exercised against a genuinely structural family category** — open question, not resolvable from existing probe data.
   499| 3. **No existing sentinel-distinction precedent in this file.** `domains/loaded_family_types.py` imports only `ITEM_Q_OK`/`ITEM_Q_MISSING`/`ITEM_Q_UNREADABLE` — it does not import `ITEM_Q_UNSUPPORTED`/`ITEM_Q_UNSUPPORTED_NOT_APPLICABLE` at all. Every existing field goes through `_safe_attr(obj, attr, None)` (swallows exceptions to `None`) followed by `canonicalize_bool`/`canonicalize_int`/`canonicalize_str`, whose `None` handling always yields `ITEM_Q_MISSING` — a genuine read exception and a legitimately-N/A value currently collapse to the same `"missing"` q in this file. There's no in-file case distinguishing `<NOT_APPLICABLE>`/`ITEM_Q_UNSUPPORTED_NOT_APPLICABLE` from `<UNREADABLE>`/`ITEM_Q_UNREADABLE` to follow. The convention exists elsewhere in the codebase (`domains/compound_types.py:493-495,902-904,1089-1091,1258-1260`; `domains/object_styles.py:327,459-461`) but would need to be introduced fresh here for these 4 fields.
   500| 
   501| **Open questions to flag explicitly**: (a) real-world exception behavior of `CanHaveStructuralSection`/`HasThermalProperties` on non-applicable categories is unverified; (b) no probe run in this repo includes a structural-discipline model, so category-bucketed ok/unsupported behavior for any of the 4 fields is unconfirmed for structural categories specifically.
   502| 
   503| ---
   504| 
   505| ## Summary — acceptance criteria check
   506| 
   507| - **All 12 areas have a findings section with file/line citations** — done above, one section per area, in the prompt's order.
   508| - **Every "stop and report" trigger is explicitly answered**:
   509|   - Area 1 (shared mutable module-level state across the 4 `extract_*` functions): **not triggered** — none found (§1.5).
   510|   - Area 5 (`build_segment_manifest.py` sourcing 3+ promotion-list fields independently): **not triggered** — it sources zero of them, because it doesn't read Revit at all (§5.1, §5.4).
   511|   - No explicit stop-and-report trigger was defined for areas 2-4, 6-12, but this pass surfaced unprompted premise mismatches in several of them anyway, flagged inline rather than silently worked around: Area 2 (`ft.function` already implemented), Area 6 (the probe doesn't call the dedicated V/G-override APIs, and the underlying global-table domain is blocked by a 2025-12-18 decision finding no API access exists at all — D-013 — though the probe does surface presence-only signals for per-view V/G-overrides parameters, see §6.1b), Area 7 (`PROBE_EXPORTER_DELTA.md` was missing at the start of this pass, later supplied and added to the repo — see below; several "delta" fields it named already implemented; Alternate Units family-applicability data looks self-contradictory and is flagged as unresolved rather than assumed), Area 10 (`Leader Arrowhead` already fully implemented in `text_types.py`).
   512| - **The compound_types mapping table (Area 1) is complete** — §1.4 maps every function/constant to a destination file, plus §1.3's full downstream-import-path list (`runner/run_dynamo.py`, `tests/test_compound_types_wall.py` — the only two files that import the module path rather than reference domain-name strings).
   513| - **No code, test, or config file was created or modified** other than this document and `tools/probes/PROBE_EXPORTER_DELTA.md` (added 2026-08-07 after being supplied — see the top-of-document revision note and its own provenance header; it is a probe-derived reference document, not extractor/test/config code).
   514| 
   515| ## Cross-area pattern worth calling out once, not per-area
   516| 
   517| Three separate areas (2, 6, 10) and one within-area finding (7's `leader_arrowhead_sig_hash`) turned out to already be implemented, reserved-but-unimplemented, or resting on a probe that doesn't test what the prompt assumed it tests. `tools/probes/PROBE_EXPORTER_DELTA.md` (supplied and added to the repo after the initial pass, see the top-of-document revision note) independently corroborates most of this document's field-level findings, but itself contains 3 confirmed-wrong "not captured" claims (`ft.function`, dimension_types text-appearance, `text_types` Leader Arrowhead) — all three fields were added in commit `26523e5` (2026-07-15), three weeks before that document's own 2026-08-05 probe run, meaning its underlying comparison ran against a stale checkout. This is a second, independent confirmation of the same lesson: before starting any of the 12 downstream PRs, re-run a targeted grep for the specific field/BuiltInParameter name across `domains/`, `contracts/domain_identity_keys_v2.json`, and `policies/*.json` regardless of what any prior document (including this one, and including `PROBE_EXPORTER_DELTA.md`) claims — this document did that check everywhere it could and reconciled a known-stale source where it found one, but a PR author should re-verify against the repo state at the time they actually start, since both documents are snapshots (2026-08-07 and 2026-08-05 respectively).
```
