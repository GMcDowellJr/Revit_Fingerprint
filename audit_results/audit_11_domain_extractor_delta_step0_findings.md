# Audit 11 — Domain Extractor Delta: Consolidated Step 0 Findings

Date: 2026-08-07
Scope: Read-only, pre-implementation groundwork for ~12 upcoming PRs (`compound_types` refactor + 11 field/domain additions). This document is the shared Step 0 for all of them — implementation PRs should treat it as already-done reconnaissance and not re-investigate the same ground. If an implementation PR discovers this document is wrong or incomplete for its area, that is a stop-and-report moment for that PR, not a silent workaround.

No extractor, test, or config file was created or modified as part of this pass. `tools/build_segment_manifest.py` was read but not modified, per instruction, and no follow-up PR from this document may modify it either.

**Revision note (2026-08-07, same-day follow-up):** the original pass below (§7.0, §2, §10) reported `tools/probes/PROBE_EXPORTER_DELTA.md` as missing from the repo — it existed outside the repo and has since been added at that path, with a provenance/errata header. This revision reconciles that file's claims against the independently-verified findings below rather than replacing them. Three of that file's "not currently captured" claims are wrong as of the current repo state — verified via `git blame` against commit `26523e5` (2026-07-15), which predates both the file's own 2026-08-05 probe run and the file itself: `ft.function`/`ft.has_embedded_sweeps` (Area 2), the dimension_types text-appearance cluster (Area 7), and `text_types` Leader Arrowhead (Area 10) were all already implemented when that comparison was made. Area 6 (`phase_graphics`) required a substantive correction in the other direction: the newly-available file's claim that the probe resolves "V/G Overrides Model/Annotation/Filters/Import/RVT Links/Worksets" at ~91% is real and independently confirmed against `tools/probes/Exports/PROBE_INVENTORY.csv` — the original pass's claim that the probe captures none of this was too strong. See §6.1b for the corrected, narrower read on what that resolution actually means.

**Second revision note (2026-08-07, PR review pass):** automated PR review on #402 caught 7 further errors, all verified independently against the code/data before fixing (not taken on faith): §6.1b/§6.3's "already covered for Model/Annotation/Filters/Worksets" claim didn't account for `view_category_overrides_model.py`/`_annotation.py`/`view_templates.py` all being template-only (`v.IsTemplate` filters), leaving non-template-view overrides uncovered too, not just Import/RVT Links (Area 6); §8.3's `units:_doc` synthetic-record proposal is contract-invalid as stated — `units.spec`/`units.unit_type_id` are both `required_keys` and `validators/record_v2.py` blocks a record missing either (Area 8); §9.2's `parent_name` promotion glossed over the existing variable assigning a top-level category its own name rather than `None`, which would misrepresent every top-level category as its own parent if promoted verbatim (Area 9); §5.2's nullability caveat wrongly claimed all 3 probe captures were the same document — they're 3 distinct Stantec-hosted projects, traced to only checking one raw JSON file's `run_metadata.document` while assuming a CSV rollup's single aggregated example name applied to all 3 (Area 5); Areas 3 and 4's domain-wiring checklists omitted regenerating `policies/domain_sig_hash_policies.json` via `tools/generate_sig_hash_policy.py`, a step `tools/run_extract_all.py` fails silently (not loudly) without; and Areas 3 and 4's API-call-chain listings each omitted one real, probe-confirmed field (`doc.active_workset_name` for worksets; `filter_param_has_value`/`refl.BrowserOrganization.FamilyName` for browser_organization). All 7 are fixed in place below.

---

## 1. `compound_types.py` refactor scoping

### 1.1 Full inventory: functions, helpers, module-level constants

`domains/compound_types.py` is 1362 lines. Full inventory (public/private, module scope):

**Module-level constants**
| Name | Line | Scope |
|---|---|---|
| `_DOMAIN_WALL` / `_DOMAIN_FLOOR` / `_DOMAIN_ROOF` / `_DOMAIN_CEILING` | 114-117 | One per partition |
| `_WALL_KIND_BASIC` / `_STACKED` / `_CURTAIN` / `_WALL_KIND_NAMES` | 118-125 | wall only |
| `_WALL_FUNCTION_NAMES` | 134-137 | wall only |
| `_FLOOR_FUNCTION_NAMES` | 138 | floor only |
| `_LAYER_FUNCTION_NAMES` | 139-142 | shared — used by all 4 via `_layer_function_str` |
| `_WALL_WRAPPING_NAMES` | 143-145 | wall only (used in `_read_compound_structure` under `family == "wall"` guard) |
| `_DECK_EMBEDDING_NAMES` | 146-150 | floor only in practice — the `_read_compound_structure` branch that uses it is the `else` of `if family in ("wall","roof","ceiling")`, and floor is the only family value that reaches that `else` today (line 347) |
| `_CORE_BOUNDARY_SENTINEL` | 151 | shared — used by all 4 |
| `_LAYER_RECORD_ID_PREFIX` | 152 | **dead** — defined, never referenced anywhere else in the file (verified by grep; only the one definition line matches) |

**Functions**
| Name | Line | Used by |
|---|---|---|
| `_build_name_key` | 42-60 | all 4 (called with a different `domain_name` string per partition) |
| `_enum_name` | 126-131 | shared generic helper; wall (`WallFunction`, `WrapAtInserts`/`Ends`), floor (`FloorFunction`), and indirectly all 4 via `_layer_function_str`/deck-usage |
| `_build_instance_count_map` | 155-173 | all 4 (different `BuiltInCategory`/`where_key` args) |
| `_attach_placeholder_metadata` | 176-194 | all 4 |
| `_na_or` | 197-200 | shared function signature, but the only call site (line 337, `structural_material`) always passes `allowed_family="wall"` — currently wall-only in effect even though generically written |
| `_canon_non_sentinel_str` | 203-209 | **wall only** — all 4 call sites (576, 667, 668, 674) are inside `extract_wall_types`; floor/roof/ceiling have no equivalent fields (`kind`, `wraps_at_inserts/ends`) that need it |
| `_material_identity_from_layer` | 212-230 | all 4, via `_read_compound_structure` |
| `_layer_function_str` | 233-238 | all 4, via `_read_compound_structure` |
| `_stack_hash_field` | 240-243 | all 4, via `_read_compound_structure` |
| `_read_compound_structure` | 246-447 | **the big shared function** — all 4, parameterized by `family` string (`"wall"`/`"floor"`/`"roof"`/`"ceiling"`) |
| `_read_type_name` | 450-465 | all 4 (name says `wall_type` but it's generic — reads `.Name` then falls back to `BuiltInParameter.SYMBOL_NAME_PARAM`) |
| `_read_wall_kind` | 468-479 | wall only |
| `_label_for_wall_type` | 482-488 | wall only (3 call sites: 586, 622, 706) |
| `_blocked_required_items` | 490-496 | wall only — hardcodes the `wt.` key prefix, so it is not directly reusable for floor/roof/ceiling, which build their blocked-item lists inline instead (900-905, 1087-1092, 1256-1261) |
| `_require_compound_dependencies` | 498-513 | all 4 |
| `extract_wall_types` | 516-749 | entry point, wall only |
| `_label_for_type` | 752-758 | floor/roof/ceiling (6 call sites: 914, 988, 1101, 1157, 1270, 1326) — **byte-for-byte identical function body to `_label_for_wall_type`** (both just wrap `safe_str(type_name)` into the same dict shape); this is a genuine duplicate, not a look-alike |
| `_coarse_fill_reads` | 761-831 | all 4 |
| `_family_name_of` | 833-837 | **dead** — defined, zero call sites anywhere in the file |
| `extract_floor_types` | 840-1024 | entry point, floor only |
| `extract_roof_types` | 1027-1193 | entry point, roof only |
| `extract_ceiling_types` | 1196-1362 | entry point, ceiling only |

### 1.2 Classification: identical vs. only-similar across partitions

- **Genuinely shared, byte-identical logic**: `_read_compound_structure` (parameterized by `family`, not duplicated per-partition), `_material_identity_from_layer`, `_layer_function_str`, `_stack_hash_field`, `_build_instance_count_map`, `_attach_placeholder_metadata`, `_require_compound_dependencies`, `_coarse_fill_reads`, `_read_type_name`, `_build_name_key`.
- **Two functions that are the same code but currently kept as separate definitions**: `_label_for_wall_type` (482-488) and `_label_for_type` (752-758) are identical bodies under different names, split only by which partition calls them. A shared module should collapse these to one.
- **Only-similar, not identical, per-partition blocks**: the four `extract_*` functions each build their own `semantic`/`coordination`/`cosmetic` identity-item lists and `blocked_items` inline (they are NOT calling a shared list-builder) — the field sets differ per partition (e.g. wall has `wt.kind`, `wt.wraps_at_inserts/ends`, `wt.has_embedded_sweeps`; roof/ceiling have neither `.function` nor `.has_embedded_sweeps`; floor has `.function` and `.has_embedded_sweeps` but not `.kind`/wrapping). These blocks look similar but are genuinely different per partition and should NOT be merged into one shared function — only extracted per-destination-file.
- **wall-only helpers that look generic but aren't used generically**: `_na_or`, `_canon_non_sentinel_str`, `_blocked_required_items`, `_read_wall_kind` — all four are effectively wall-only today (see table above), despite being module-level rather than nested inside `extract_wall_types`.

### 1.3 Repo-wide references to `compound_types`

Grepped the full repo (not just `domains/`) for `compound_types` and for the 4 domain-name strings.

**Direct module imports / calls to `domains.compound_types`:**
- `runner/run_dynamo.py:151` — `from domains import compound_types`; calls `compound_types.extract_wall_types` (627), `.extract_floor_types` (655), `.extract_roof_types` (683), `.extract_ceiling_types` (711). This is the **only production code caller** that imports the module directly, and it would need 4 import/call-site updates if the file is split.
- `tests/test_compound_types_wall.py:182` — `importlib.import_module("domains.compound_types")`, then calls `m.extract_wall_types(...)` ~25 times through the file. This test only exercises `extract_wall_types` — it would need its import path updated to whatever `wall_types.py` module the split produces, but does not touch floor/roof/ceiling.

**Everything else references the domain-name strings (`"wall_types"`, `"floor_types"`, `"roof_types"`, `"ceiling_types"`), not the module path — these are unaffected by a module split:**
- `policies/domain_join_key_policies.json` (53, 298, 613, 910 + `"Domain family: compound_types."` doc strings at 67, 313, 627, 928)
- `policies/domain_name_key_policies.json` (70-104, with explicit comments referencing `domains/compound_types.py` as the current file — these comments would go stale after a split but the JSON keys themselves are domain-name-keyed, not path-keyed)
- `policies/domain_sig_hash_policies.json` (30-56, 402-430, 682-708, 988-1019 — `sig_hash_schema` strings like `"wall_types.sig_hash.v1"`)
- `contracts/domain_identity_keys_v2.json` (`domains.wall_types/.floor_types/.roof_types/.ceiling_types`, each tagged `"domain_family": "compound_types"` — data field, not an import)
- `tools/export_to_flat_tables.py:253` — comment only (`# layer_rows — compound_types families only`)
- `tools/reset_wall_types_for_reapply.py` — operates on `DOMAIN = "wall_types"` as a CLI/data string; no module import
- `tools/bundle_analysis/placeholder_exclusions.py:126-127`, `placeholder_exclusions_legacy.py:19` — `TARGET_DOMAINS` tuples of domain-name strings
- `tools/generate_governance_narrative.py:175/185/195` — display-label string map (`"ceiling_types": "Ceiling Types"`, etc.)
- `tools/analyze_promotion_candidates.py` — domain-name strings only
- `config/archetype/archetype_definitions.json`, `tools/archetype/vfd_category_domain_map.json`, `tools/archetype/vfd_bip_target_domain_hints.json` — `target_domain`/domain-name data fields, not imports
- `core/name_key_coverage.py` — domain-name strings in a coverage list

**No golden-file or Revit-integration test references found**: `tests/golden/` and `tests/revit/` were grepped for `compound_types`/`wall_types`/`floor_types`/`roof_types`/`ceiling_types` — zero matches. No golden fixtures constrain the split.

**Other tests referencing domain-name strings only (fixture data, not module imports — unaffected by a file split):**
- `tests/test_name_key_inline_analysis_agreement.py:108-111` — `_native_case("wall_types", "wt.type_name", ...)` etc. for all 4
- `tests/test_name_key_policy.py:24-27` — domain list
- `tests/test_placeholder_exclusions.py:12,20,22` — fixture rows with `'domain':'wall_types'`
- `tests/test_discover_vfd_edges.py:357-444` — CSV fixture filenames `wall_types.csv`/`floor_types.csv`, and target/candidate domain strings
- `tests/test_export_layer_stacks.py` — fixture dicts keyed `"wall_types"`/`"floor_types"`, and output filenames `layer_stacks__wall_types.csv` (asserted, but the string comes from the fixture domain name, not the module)

### 1.4 Proposed mapping table (map only — not implemented)

| Current function/constant | Line(s) | Proposed destination |
|---|---|---|
| `_DOMAIN_WALL`, `_WALL_KIND_*`, `_WALL_FUNCTION_NAMES`, `_WALL_WRAPPING_NAMES` | 114-125, 134-137, 143-145 | `wall_types.py` |
| `_DOMAIN_FLOOR`, `_FLOOR_FUNCTION_NAMES`, `_DECK_EMBEDDING_NAMES` | 115, 138, 146-150 | `floor_types.py` |
| `_DOMAIN_ROOF` | 116 | `roof_types.py` |
| `_DOMAIN_CEILING` | 117 | `ceiling_types.py` |
| `_LAYER_FUNCTION_NAMES`, `_CORE_BOUNDARY_SENTINEL` | 139-142, 151 | shared helper module |
| `_LAYER_RECORD_ID_PREFIX` (dead) | 152 | delete, or shared module if revived |
| `_build_name_key`, `_enum_name`, `_build_instance_count_map`, `_attach_placeholder_metadata`, `_material_identity_from_layer`, `_layer_function_str`, `_stack_hash_field`, `_read_compound_structure`, `_read_type_name`, `_require_compound_dependencies`, `_coarse_fill_reads` | various | shared helper module |
| `_label_for_wall_type` + `_label_for_type` (collapse to one) | 482-488, 752-758 | shared helper module, single function |
| `_na_or`, `_canon_non_sentinel_str`, `_read_wall_kind`, `_blocked_required_items` | 197-200, 203-209, 468-479, 490-496 | `wall_types.py` (currently wall-only in effect) |
| `_family_name_of` (dead) | 833-837 | delete, or `roof_types.py`/`ceiling_types.py` if a use is found |
| `extract_wall_types` | 516-749 | `wall_types.py` |
| `extract_floor_types` | 840-1024 | `floor_types.py` |
| `extract_roof_types` | 1027-1193 | `roof_types.py` |
| `extract_ceiling_types` | 1196-1362 | `ceiling_types.py` |

**Downstream import-path changes this map would require:** `runner/run_dynamo.py:151,627,655,683,711` (import + 4 call sites) and `tests/test_compound_types_wall.py:182` (import only — this test doesn't touch floor/roof/ceiling). Nothing else in the repo imports the module path; every other reference is domain-name-string-keyed and is unaffected.

### 1.5 Stop-and-report items for Area 1

- **Mutable module-level state**: none found. Every constant is a read-only lookup dict/int used via `.get()`; no module-level list/dict is ever mutated after definition. Per-call state (`info`, `records`, `sigs`, `_instance_count_map`) is created fresh inside each `extract_*` call. **Not triggered** — the shared module can be a plain function/constant library with no shape complications from shared mutable state.
- **Unrelated but worth flagging**: the module docstring (lines 4-8) claims `extract_floor_types (stub)`, `extract_roof_types (stub)`, `extract_ceiling_types (stub)` — this is stale. All three are fully implemented (compound-structure reads, sig_hash, blocked-record handling, layer rows), matching `DECISIONS.md` D-018's statement that "`compound_types` already covers some system families (standard wall, floor, ceiling types)". A refactor PR should drop the stale docstring rather than propagate it into the new files.

---

## 2. `floor_types.function` field

**Stop-and-report — this field already exists.** `extract_floor_types` in `domains/compound_types.py` already reads and emits `ft.function`:
- Read at lines 932-938: `raw = getattr(ft, "Function", None)`; `ft_function = _enum_name(FloorFunction, int(str(raw)), _FLOOR_FUNCTION_NAMES)`, with `ft_function_q = ITEM_Q_OK` on success / `ITEM_Q_UNREADABLE` on exception.
- Emitted at line 960 as `make_identity_item("ft.function", ft_function if ft_function != S_UNREADABLE else None, ft_function_q)`, inside the `coordination` bucket (not `semantic` — it does not currently contribute to `sig_hash`, since only `ft.layer_count`/`ft.total_thickness_in`/`ft.stack_hash_loose` are in `semantic`/required).
- Already registered in `contracts/domain_identity_keys_v2.json`'s `floor_types.allowed_keys` (confirmed: `"ft.function"` is present in the list).

**Read path — neither BuiltInParameter nor string lookup.** Both `wt.function` (line 642, `getattr(wt, "Function", None)`) and `ft.function` (line 933, `getattr(ft, "Function", None)`) use the direct .NET/Revit-API **property** `WallType.Function` / `FloorType.Function`, not `get_Parameter(BuiltInParameter.X)` and not `LookupParameter("name")`. The premise that this is a BuiltInParameter-vs-string-lookup question doesn't match how either field is actually read.

**Asymmetry worth flagging**: `wt.function` is a `semantic` item (contributes to `wall_types`' `sig_hash`, is in its `required_keys`), while `ft.function` is a `coordination` item (does NOT contribute to `floor_types`' `sig_hash`, is not required). This is an existing, real asymmetry in the current code — not something this pass is proposing to change (hash composition is out of scope), but a future PR working in this area should know the two are treated differently today rather than assume symmetry.

**Conclusion for a follow-up PR in this area**: there is no new field to add. If the actual intent was to promote `ft.function` from `coordination` to `semantic` (i.e., make it hash-contributing like `wt.function`), that is a hash-semantics change and is explicitly out of scope for this investigation and for any PR following from it without a separate hash-policy discussion.

**Errata note**: `tools/probes/PROBE_EXPORTER_DELTA.md` (added to the repo 2026-08-07, see its own provenance header) independently claims `ft.function` "resolves 31/34 (91%) ... but not captured as `ft.function`" and that `has_embedded_sweeps` "isn't in any of their identity-item lists." Both claims are wrong against the current repo: `git blame` on `domains/compound_types.py:960` and `:965` shows both lines were added in commit `26523e5` (2026-07-15 19:34:13 -0700), three weeks before that document's own 2026-08-05 probe run. Whatever comparison produced that document's claim ran against a stale view of `domains/`. Trust the direct code read above over that document for this specific field.

---

## 3. `worksets` — new exporter

### 3.1 Domain-registration mechanism

Confirmed: there is **no naming-convention auto-discovery** for the real extraction pipeline (`runner/run_dynamo.py`). Domain wiring is fully explicit, in at least 5 places (the original pass here found 4 and missed a 5th, analysis-side step — see the correction below):
1. Import the module: `runner/run_dynamo.py:141-154` (e.g. `from domains import compound_types`).
2. Add an `if _enabled("domain_name"): legacy = _domain_run("domain_name", <module>.extract, doc, ctx, contract_domains, run_diag, runner_notes); if legacy is not None: fingerprint["domain_name"] = legacy` block (pattern shown at `run_dynamo.py:549-731`; `_enabled` is defined at line 470, a simple allowlist gate against module-level `ENABLED_DOMAINS`, line 162).
3. Register `allowed_keys`/`required_keys` in `contracts/domain_identity_keys_v2.json`.
4. Add a join-key policy entry in `policies/domain_join_key_policies.json` if the domain needs one.
5. **Regenerate `policies/domain_sig_hash_policies.json`** via `tools/generate_sig_hash_policy.py`. This one is easy to miss because it's not extraction-side wiring at all — it's a separate, checked-in, generated file consumed by the analysis-side `sig_hash` stage. `tools/run_extract_all.py`'s sig-hash stage (`get_domain_sig_hash_policy`, checked at `run_extract_all.py:314-317`) skips any domain absent from that policy file entirely and reports it under `diag["domains_without_policy"]` (`run_extract_all.py:278,282,316,382-387`) rather than erroring — silent, not loud. Registering a domain in `contracts/domain_identity_keys_v2.json` (step 3) does not, by itself, update `policies/domain_sig_hash_policies.json`; that file is compiled from the contract by running the generator script (see Sig-Hash Policy System section), a step that's easy to forget precisely because nothing fails loudly if it's skipped — the domain's records just never get an analysis-side sig_hash and silently fall into `domains_without_policy` instead.

A new `domains/worksets.py` would need all 5 steps done by hand — nothing auto-discovers a new `domains/*.py` file, and nothing regenerates the sig-hash policy file automatically either.

**Important distinction — `runner/probe_thin_runner.py` is a separate, unrelated mechanism** that DOES use naming-convention discovery, but only for probe-mode measurement, not the real pipeline: `_discover_probe_files()` (`probe_thin_runner.py:294-308`) globs `tools/probes/probe_*.py` by filename convention. This is how `tools/probes/probe_worksets.py` already gets picked up in probe runs — but that has no bearing on whether `domains/worksets.py` gets wired into `run_dynamo.py`; those are two independent systems. `runner/thin_runner.py` is a third file, but it does no domain wiring itself — it just imports/reloads `runner.run_dynamo` as a module (`thin_runner.py:519-526`).

### 3.2 Duplication check

**One partial overlap found**: `domains/view_templates.py:274-302` (`_append_workset_visibility`) already reads workset data — `FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset).ToWorksets()` (line 281) and, per workset, `ws.Name` and `v.GetWorksetVisibility(ws.Id)` (lines 286-289) — and folds `workset[i].name`/`workset[i].visibility` into the view-template behavioral signature (called at 5 call sites: 642, 1059, 1502, 1922, 2355, once per view_templates partition).

This is a **narrow, not full, overlap**:
- Scope: only `WorksetKind.UserWorkset` worksets, never the other kinds (`StandardWorkset`, `FamilyWorkset`, `ViewWorkset` per the probe's own enum-introspection results).
- Fields: only `Name` and per-template visibility toggle — nothing about `Kind`, `IsEditable`, `IsDefaultWorkset`, `Owner`, `IsActiveWorkset`, `UniqueId`, or per-kind counts.
- Purpose: workset name there is a cross-reference key for template visibility state, not a workset inventory.

`domains/loaded_family_types.py:73` references the string `"workset"` only as one of several `_OPERATIONAL_TOKENS` used to filter out operational/non-governed parameters — not a workset data read.

No references to worksets found in `tools/bundle_analysis/`, `tools/patterns_analysis/`, or any other analysis tool.

**Conclusion**: a new `worksets` exporter would be the sole source of workset-level metadata (kind, editable, owner, active-workset, per-kind counts, full kind coverage beyond UserWorkset). The one real single-source-of-truth risk is workset **names**: `view_templates.py` already captures `UserWorkset` names as part of its own hash; a new `worksets.py` domain capturing names again is not a contradiction (different domains, different purposes) but is a second source of the same name string that a future consumer should be aware of.

### 3.3 Exact API call chain per field (from `tools/probes/probe_worksets.py`)

- **Discovery of worksets**: `WorksetKind` members are discovered via `dir(WorksetKind)` + `getattr` introspection (lines 152-175), **not** a hardcoded name list and **not** `System.Enum.GetNames()` — the probe's own comments (129-143) explain `System.Enum.GetNames()` was tried and failed to resolve reliably in this Dynamo CPython3 host; `dir()`+`getattr()` is the proven-working pattern. For each discovered kind, worksets are collected via `FilteredWorksetCollector(doc).OfKind(kind_attr)` (line 191).
- `workset.name` → `ws.Name` (line 252)
- `workset.kind` → `ws.Kind`, coerced `int(str(kind_raw))` (line 249) — `.NET` enum requiring the `str()` round-trip pattern used elsewhere in this codebase (same pattern as `_read_wall_kind` in compound_types.py)
- `workset.is_editable` → `ws.IsEditable` (line 262)
- `workset.is_default_workset` → `ws.IsDefaultWorkset` (line 270)
- `workset.owner` → `ws.Owner` (line 278)
- `workset.unique_id` → `ws.UniqueId` — **Note**: `Workset.UniqueId` is a `System.Guid`, not `System.String` (unlike `Element.UniqueId`); the probe's `_pv()` helper (lines 104-125) has an explicit comment documenting a real 2026-08-04 failure where this was passed through un-coerced and broke `json.dump()`. Any extractor implementation must `str()`-coerce this.
- `workset.is_active_workset` → not a direct property; computed by comparing each workset's `.Id` against `doc.GetWorksetTable().GetActiveWorksetId()` (lines 204-211, 288)
- Per-`WorksetKind` counts → `len(FilteredWorksetCollector(doc).OfKind(kind_attr))` per discovered kind (lines 191-192, 303-308), i.e. one full collector count per kind, not capped (only the *sample* used for the reflection/inventory sweep is capped by `max_worksets_per_kind`)
- `doc.is_workshared` → `doc.IsWorkshared` (line 181), gates the whole discovery block — if `False`, `selected` stays empty and only the doc-level `doc.is_workshared` row is emitted
- `doc.active_workset_name` — omitted from the original pass's call-chain list despite being named in the field promotion list; added here. → resolved alongside `workset.is_active_workset` from the same `doc.GetWorksetTable().GetActiveWorksetId()` lookup (lines 204-206): once the active `WorksetId` is known, the probe scans the already-collected `selected` worksets for a matching `.Id` and takes that match's `.Name` (lines 207-211). Emitted as its own doc-level inventory row only when non-empty (`if active_workset_name: _add_inventory_obs("doc.active_workset_name", ...)`, lines 300-301) — confirmed populated in `PROBE_INVENTORY.csv` across all 3 runs.

---

## 4. `browser_organization` — new exporter

### 4.1 Wiring mechanism

Identical to Area 3 — no naming-convention auto-discovery for `run_dynamo.py`; a new `domains/browser_organization.py` needs the same 5 manual steps (import, `_enabled()`/`_domain_run()` block, `contracts/domain_identity_keys_v2.json` entry, join-key policy entry if needed, and regenerating `policies/domain_sig_hash_policies.json` via `tools/generate_sig_hash_policy.py` — see §3.1's correction). `probe_thin_runner.py`'s glob discovery already picks up `tools/probes/probe_browser_organization.py` for probe runs, same caveat as Area 3 that this has no bearing on real-pipeline wiring.

### 4.2 Duplication check

**One real overlap, and it's the same one as Area 3**: `BrowserOrganization.WorksetId` is resolved via `_resolve_workset()` (`probe_browser_organization.py:167-183`, using `WorksetTable.GetWorkset()`, not `doc.GetElement()` — `WorksetId` is a distinct .NET type from `ElementId`, both happen to expose `.IntegerValue`, but `Workset` doesn't derive from `Element`). If a `worksets` domain from Area 3 exists, `browser_organization`'s workset reference should resolve against that domain's records via `ctx` (the pattern this codebase already uses elsewhere — e.g. `material_uid_to_name` in `ctx`) rather than re-deriving workset names independently. No other domain currently reads `BrowserOrganization`/`FolderItemInfo` — this domain has no other duplication risk found.

### 4.3 Exact API call chain per field (from `tools/probes/probe_browser_organization.py`)

- `org_id` → one of 3 `BrowserOrganization` instances per category (`views`/`sheets`/`schedules`), obtained via `BrowserOrganization.GetCurrentBrowserOrganizationForViews(doc)` / `...ForSheets(doc)` / `...ForSchedules(doc)` (lines 431-433), then `org.Id.IntegerValue` (lines 441-442)
- `sorting_order` → `org.SortingOrder`, coerced `int(str(...))` via the `_int_enum` helper (line 486)
- `sorting_parameter_id` → `org.SortingParameterId.IntegerValue` (lines 483-484); resolved to a human name via a `BuiltInParameter` reverse-lookup dict built once via `dir(BuiltInParameter)` + `getattr` introspection filtered to negative `IntegerValue`s (lines 420-427), i.e. same enum-introspection pattern as worksets, not a hardcoded BIP list
- `folder_items_walked_count` → `len(_folder_item_records)` (line 537), a running count accumulated across the recursive tree walk (`_walk_tree`, lines 315-369) rooted at `org.GetFolderItems(ElementId(org_id_int))` (line 325), capped by `max_items_per_level`/`max_tree_depth` inputs
- `name_fallback_used_count` → `_name_fallback_used_count` (line 529), incremented (line 347) each time a `FolderItemInfo.Name` returns the literal `"???"` and a fallback name is resolved instead via `_best_name()` (lines 241-261), which prefers, in order: `folder_item_name` → `definition_name` (via `elem.GetDefinition().Name`) → `element_name` (`elem.Name`) → `bip_label` (`LabelUtils.GetLabelFor(BuiltInParameter)`) → `bip_name` (the reverse-lookup dict). Folder-item classification itself (`_resolve_folder_item`, lines 264-302) is: `ElementId.IntegerValue < 0` → built-in parameter; `== current seed or org.Id` → cycle/self-reference (skip); `> 0, != org.Id` → shared-parameter/regular element via `doc.GetElement(...)`.
- `filter_param_has_value` — omitted from the original pass's call-chain list; added here. → `org.GetParameters("Filter")` (line 514), first result's `.HasValue` (lines 515-517). Confirmed populated: `PROBE_INVENTORY.csv` shows `browserorg.filter_param_has_value` at `ok=9` across all 3 runs (9 = 3 categories × 3 runs), example `False`.
- `refl.BrowserOrganization.FamilyName` — also omitted from the original pass; added here. Not read via the curated `param` inventory path at all — it's a `.NET` reflection property discovered by the generic reflection sweep (`_run_reflection_sweep(_browserorg_objs, "BrowserOrganization", "browser_organization")`, `probe_browser_organization.py:852-853`), i.e. `getattr(org, "FamilyName")` on the same `BrowserOrganization` instances used for `org_id`/`sorting_order`/etc. above, not a new object. `PROBE_INVENTORY.csv` shows it `ok` across all 3 runs, example `"Browser - Views"`.

---

## 5. `identity` domain expansion

### 5.1 `build_segment_manifest.py` overlap check (the key single-source-of-truth question for this area)

**`tools/build_segment_manifest.py` never reads Revit at all.** No `Autodesk.Revit.DB` import, no `doc.ProjectInformation` access, no `BuiltInParameter` reads anywhere in the file. It reads exclusively from a pre-existing `file_metadata.csv` (path passed via `--metadata-file`, `build_segment_manifest.py:710`), whose relevant columns are `unit_system`, `governance_role`, `client_label`, `discipline_label`, `business_center_label`, `collection_label` (the `DIMENSION_CONFIG` list, lines 35-41, plus `REQUIRED_ROW_FIELDS`, line 24).

**Therefore: zero overlap with `domains/identity.py` today**, for two independent reasons:
1. `build_segment_manifest.py` doesn't read Project Information parameters through its own path or any path — it consumes an external, presumably hand-curated/analyst-labeled CSV.
2. `domains/identity.py` (full file read, 280 lines) doesn't currently read any Project Information parameters either. Its `extract()` (94-279) reads only: `doc.Title` (108), central path via `WorksharingUtils.GetModelPath(doc)`/`doc.PathName` (110-121), `doc.IsWorkshared` (123), and `app.VersionNumber`/`VersionName`/`VersionBuild` (126-128). No `doc.ProjectInformation` access anywhere in the file.

**Stop-and-report trigger explicitly not triggered**: the trigger condition was "`build_segment_manifest.py` turns out to already source 3+ of these fields independently" — it sources zero of them (it doesn't read Revit at all), so this is **not triggered**, but the underlying assumption in the prompt (that `build_segment_manifest.py` might read ProjectInfo directly) does not hold either. Worth noting precisely rather than just "not triggered": `file_metadata.csv`'s `client_label`/`discipline_label`/`business_center_label`/`governance_role` are governance/segmentation labels, not verbatim raw `ProjectInfo` parameter values — even if `identity.py` later starts emitting `project_info.p.Client Name` etc., those would be a *different, new* value (the raw Revit field) sitting alongside an *existing, separately-curated* value (`file_metadata.csv`'s `client_label`) that is conceptually related but not database-derived from it. A later PR should not assume these two would auto-reconcile — that decision is out of scope here.

Related but distinct context (not the requested overlap, but adjacent): `docs/CENTRAL_PATH_NORM_RULE.md` and `policies/governance_role_path_patterns.json` infer `governance_role` from `central_path_norm`, which is itself derived from `identity.py`'s existing `central_path` field (`_phase2_build_lineage_items`, line 73). That inference path is unrelated to `build_segment_manifest.py`/`file_metadata.csv` and was not investigated further here as it's outside this area's scope.

### 5.2 `project_info.*` field origin (BuiltInParameter vs. shared-parameter)

Confirmed via `tools/probes/probe_identity.py` (`_definition_origin()`, lines 101-122: `built_in` if `Parameter.Id.IntegerValue < 0`; `shared` if `Parameter.IsShared == True`; else `project_custom`) and cross-checked against real probe export data (`tools/probes/Exports/probes_2025_20260805T120010-8cf8cf.json`, `identity` domain inventory records' `breadth.definition_origin`):

| Field (from the prompt's example list) | `definition_origin` | Notes |
|---|---|---|
| `Client Name` | `built_in` | |
| `Organization Name` | `built_in` | |
| `Organization Description` | `built_in` | |
| `Building Name` | `built_in` | |
| `Author` | `built_in` | |
| `Project Number` | `built_in` | |
| `Project Name` | `built_in` | |
| `Project Address` | `built_in` | |
| `Project Status` | `built_in` | |
| `Project Issue Date` | `built_in` | |
| **`Office`** | **`shared`** (GUID `6b61afc7-13eb-4af5-8b65-889f978af4f3`) | This is a Stantec-authored shared parameter, not a Revit built-in |

`PROBE_INVENTORY.csv` also shows Stantec-specific numbered issue-block fields (`project_info.p.PE_01_*` through at least `PE_03_*` — approval/date/issue-status/number/name) which are clearly custom project parameters unique to Stantec's template (not checked for `is_shared` individually, but structurally distinct from the built-in ProjectInfo surface).

**Nullability caveat, corrected — the 3 probe captures are 3 different projects, not the same document run 3 times.** The original pass here claimed all 3 captures were the same document; that was wrong, and traced to a real mistake — the `definition_origin` cross-check in this section only queried one raw JSON file (`probes_2025_20260805T120010-8cf8cf.json`), and the "Kaiser Permanente San Rafael Replacement Hospital" project name seen in `PROBE_INVENTORY.csv`'s single aggregated example column was assumed (incorrectly) to represent all 3 runs, when that CSV column only ever holds one representative example across the merged runs, not one per file. Checking each raw JSON file's own `run_metadata.document` directly: `probes_2025_20260805T110757-84cfc9.json` → `KSRF_Hosp_Interior_AR_V50_4.1 GB_2025-11-08` (path `Autodesk Docs://000000000_Kaizen_Standard_Resources/...`, likely the Kaiser San Rafael project the CSV's example name refers to); `probes_2025_20260805T111532-d738fd.json` → `000000000_Arch_Int_ContainerModel_r25` (same `000000000_Kaizen_Standard_Resources` folder — a container/template model, not a live project); `probes_2025_20260805T120010-8cf8cf.json` (the file actually queried for `definition_origin` above) → `2014351100_stn_arch_asc_int_dd` (path `Autodesk Docs://2014351100_Renown_Kiley_Ranch/...`). Three distinct documents, confirmed.

This is a real, if partial, improvement on the original claim: it does provide **cross-project** (not cross-firm) evidence — `Client Name`/`Project Number`/etc. resolving `ok` across all 3 confirms the built-in `ProjectInfo` surface behaves consistently across at least 3 different Stantec-numbered projects/models (`000000000_...` and `2014351100_...` prefixes are Stantec's own project-numbering convention, consistent with all 3 paths living under Stantec's Autodesk Docs hub), not just one. It does **not** provide cross-firm evidence: all 3 paths are Stantec-hosted, and nothing in the available data confirms behavior on a non-Stantec project lacking Stantec's shared parameter file (the `Office` shared parameter specifically). For the true built-in fields (Client Name, Project Number, etc.), nullability across firms isn't at issue regardless (they're native `ProjectInfo` parameters present in every Revit project by default) — but for `Office` and any other shared-parameter-backed field found, the cross-firm caveat stands as originally stated, just for the right reason this time.

### 5.3 Current record shape

`domains/identity.py` emits **exactly one record per document** — `record_id="document"` (line 249), single record in `info["records"]` (line 272). New `project_info.*` fields would attach as additional `identity_items` on this same single record (following the existing pattern at lines 182-193), **not** as new per-element records — `identity.py` has no per-element iteration today; it's a flat document-scoped extractor.

### 5.4 Stop-and-report

**Not triggered.** `build_segment_manifest.py` sources 0 of the promotion-list fields (it doesn't read Revit at all — see 5.1). The investigation surfaced a different, softer risk instead (two separately-sourced-but-related concepts: governance labels in `file_metadata.csv` vs. raw `project_info.*` values that would newly exist in `identity.py`) which is documented above rather than assumed away.

---

## 6. `phase_graphics` reimplementation

This section is a design-scoping writeup per the prompt's own framing, not a decision — but the investigation surfaced a blocking finding that should sit ahead of any design discussion.

### 6.1 Stop-and-report — the premise doesn't match either the code history or the current probe

**`DECISIONS.md` D-013 (accepted 2025-12-18, `phase_graphics` domain) states plainly that no API access exists for the data this domain is supposed to capture:**
> A targeted API probe in Revit 2025 (and consistent with behavior back to 2021) confirmed: `PhaseFilter.GetPhaseStatusPresentation` **is available**. No API access exists for: per-status graphic overrides, line style assignments, color/pattern overrides.

Warnings section still says: *"The `phase_graphics` domain is intentionally disabled - do not attempt to enable without API justification."* Nothing found in this pass constitutes that justification — see below.

**The current `domains/phase_graphics.py` (full file read, 203 lines) is about the GLOBAL Phase Graphic Overrides table** (Manage tab → Phases dialog → Graphic Overrides tab: per-phase-status line style/color/pattern overrides), matching its own docstring ("captures the GLOBAL phase graphic override settings... single global configuration", lines 5-17) and D-013's description. It currently emits a single blocked record (`record_id="phase_graphics:global"`, `ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED`, line 144) with no real data, by design, because D-013 found the underlying API unavailable.

**`tools/probes/probe_phase_graphics.py` (full file read, 1039 lines) does NOT call any dedicated V/G-override-reading API.** Grepped the file for `GetCategoryOverrides`, `GetFilterOverrides`, `GetWorksetVisibility`, `GetCategoryHidden`, `GetLinePatternOverride`, `OverrideGraphicSettings` — **zero matches**. What the probe actually does:
- Collects `View` elements (`FilteredElementCollector(doc).OfClass(View)`, line 269) and `PhaseFilter` elements (line 318), then runs a **generic `GetOrderedParameters()`/`.Parameters` walk** over both (lines 367-414) — the same generic parameter-inventory pattern every other probe uses, not a dedicated V/G-override read.
- Adds a View → PhaseFilter crosswalk: `BuiltInParameter.VIEW_PHASE_FILTER` (preferred) or name-candidate `LookupParameter` fallback (lines 526-550), i.e. which Phase Filter each view has assigned — this is **view-level phase filter assignment**, not phase-status graphic overrides.
- Adds `GetFilters()`/`GetOrderedFilters()` per view (lines 504-507, 520-523) — the list of *view filter* element IDs applied to the view (filter membership), not their override *settings*.
- Adds `BodyTextTypeId`/`HeaderTextTypeId`/`TitleTextTypeId` for `ViewSchedule` only (lines 483-502) — unrelated to phase graphics.

The actual dedicated override-reading Revit APIs (`GetCategoryOverrides`, `GetCategoryHidden`, `GetFilterOverrides`, `GetWorksetVisibility`) are **not called anywhere in this probe file**. That part of the original finding stands. What was wrong in the original pass is the conclusion drawn from it — see §6.1b.

**Those dedicated override APIs are already used elsewhere in the codebase, by different domains, for different (non-phase) purposes:**
- `domains/view_category_overrides_model.py:99-100,172,215` and `view_category_overrides_annotation.py:75-76,172` — `template.GetCategoryHidden(cat.Id)`, `template.GetCategoryOverrides(cat.Id)`, for Model and Annotation categories respectively.
- `domains/view_templates.py:245` — `v.GetFilterOverrides(fid)`.
- `domains/view_templates.py:277,289` — `v.GetWorksetVisibility(ws.Id)`, scoped to `WorksetKind.UserWorkset` (same code cited in Area 3.2 above).

### 6.1b Correction — the generic parameter walk does surface "V/G Overrides *" as presence signals, and that changes what's genuinely gapped

`tools/probes/PROBE_EXPORTER_DELTA.md` (added to the repo 2026-08-07) claims the probe resolves `V/G Overrides Model`, `Annotation`, `Filters`, `Import`, `RVT Links`, `Worksets` (plus `Phase`, `Phase Filter`) per view at ~91%. **This is real and independently confirmed** — `tools/probes/Exports/PROBE_INVENTORY.csv` has explicit rows for `phase_graphics,param,p.V/G Overrides Model` / `Annotation` / `Filters` / `Import` / `RVT Links` / `Worksets`, each `missing=0;ok=136;unreadable=0;unsupported=0` across all 3 runs, plus `p.Phase Filter` (`ElementId`, `ok=204`) and `p.Phase` (`ElementId`, `ok=60`). The original pass above was too strong in saying the probe reads none of this — it does, just not through the mechanism (or with the content) that framing implied.

**But the resolution is presence-only, not content.** All six `V/G Overrides *` rows have `storage=None` and a permanently empty example (`raw`/`display`/`norm` all blank across every run). This matches the probe's own `_format_param_contract()` handling for `StorageType.None` params (`probe_phase_graphics.py:188-192`: "Often represents non-primitive / complex parameter surfaces. Keep it auditably present but not value-typed"). In other words: Revit exposes `V/G Overrides Model` etc. as real, readable `Parameter` objects on `View` (the ones behind the "Edit…" buttons in View Properties), and `q="ok"` here means *the parameter exists and didn't throw* — it does **not** mean the probe decoded which categories/filters/worksets/links are overridden or what the override values are. That decoding is exactly what `GetCategoryOverrides`/`GetFilterOverrides`/`GetWorksetVisibility` do, and none of them are called here (confirmed above).

**This also does not touch D-013.** D-013 is about the *global* Manage→Phases→Graphic Overrides table (per-phase-status line style/color/pattern), a different Revit feature from the per-view V/G Overrides parameters this probe touched. Confirming that a per-view V/G-overrides parameter exists and is readable is not new evidence about whether the global phase-status override table has API access — it's unrelated data. D-013's finding stands as-is.

**Correction to "already mostly covered" — the existing coverage is template-only, not view-wide.** `view_category_overrides_model.py`/`_annotation.py` filter their collected views to `v.IsTemplate` before extracting anything (`view_category_overrides_model.py:151`: `templates = [v for v in all_views if _safe_bool(lambda: v.IsTemplate)]`; `view_category_overrides_annotation.py:119`, identical). `view_templates.py` does the same at every one of its 5 partition call sites (`view_templates.py:107`, `519`, `944`, `1394`, `1820`, `2242`, all gating on `v.IsTemplate`). But `probe_phase_graphics.py` explicitly sweeps **both** templates and non-template views (§6.2 below) — and Revit allows Model/Annotation/Filter/Workset V/G overrides to be set directly on an ordinary view, not just inherited from its template. None of the three existing domains would capture an override set directly on a non-template view. So "already captured for Model/Annotation/Filters/Worksets" is only true for the template-scoped subset of that data; overrides applied directly to non-template views are uncovered by any current domain, on top of the previously-identified Import/RVT-Link gap.

**Revised bottom line**: content decoding for Model/Annotation/Filters/Worksets on *templates* already exists elsewhere in the repo (`view_category_overrides_model.py`/`_annotation.py`/`view_templates.py`, per the citations above) using the real dedicated APIs — the presence-only signal this probe found adds nothing beyond confirming those parameters are non-null on ~91% of the sampled views (136/150), which was never in doubt. The genuinely uncovered gap is **Import-category and RVT-Link overrides on any view, plus Model/Annotation/Filter/Workset overrides specifically on non-template views** — neither is touched by any domain or probe call in the repo today, and the presence-only `p.V/G Overrides *` rows don't change that; they only confirm the parameters exist, not what they contain or which views' overrides are template-inherited vs. view-local.

**Conclusion — this still needs a scoping decision before any design write-up is actionable:** "phase_graphics" as named in `DECISIONS.md`/the current domain file means the **global per-phase-status override table**, which D-013 found has no API access at all (not "not yet implemented" — API-inaccessible), and nothing in this pass or in the newly-reviewed delta document changes that. The prompt's area 6 describes investigating **per-view** V/G overrides conditioned on Phase/Phase Filter, which is a different Revit concept. It is only partially covered by other domains — template-scoped Model/Annotation/Filters/Worksets — and genuinely gapped for Import/RVT Links on any view and for Model/Annotation/Filters/Worksets specifically on non-template views. A "reimplementation" can't proceed on design questions (per-view vs. aggregated record shape) until it's decided which of these two things is actually being reimplemented — and if it's the first (the global table), someone needs new evidence overturning D-013's finding, since nothing produced in this pass (including the newly-added delta document) provides that.

### 6.2 View universe swept by the probe (answered regardless of the above, since it's simple fact)

`probe_phase_graphics.py` sweeps **both** templates and non-template views, template-biased: `FilteredElementCollector(doc).OfClass(View)` (line 269) is split into `templates`/`non_templates` via `v.IsTemplate` (lines 276-281), then `selected_views` is filled from `templates` first (capped by `per_bucket_limit`, default 50, and `max_views_to_inspect`, default 200), and only fills remaining capacity from `non_templates` if room is left (lines 296-312).

### 6.3 Options writeup (per-view vs. aggregated record) — not a decision

Presented only in case the scoping question in 6.1 resolves toward "yes, reimplement something here" — genuinely contingent on that resolution, and Greg's sign-off is needed regardless per the prompt's own instruction that this is a new design, not confirmed precedent.

- **Option A — single global record** (matches the *current* file's own stated scope and `record_id` scheme, `"phase_graphics:global"`, `phase_graphics.py:163`). Only sensible if the target really is the Manage→Phases→Graphic Overrides table, and only implementable if new evidence contradicts D-013.
- **Option B — per-view record**, `record_id` keyed by view `UniqueId` (the existing convention for element-backed, identity-persistent entities per UniqueId Usage rule) or by view Name-based composite if templates are the intended scope (matching `view_category_overrides_model.py`/`view_templates.py`'s existing per-template record pattern). Only sensible if the target is per-view V/G overrides — and even then, redundant with `view_category_overrides_model.py`/`_annotation.py`/`view_templates.py` only for the template-scoped subset of Model/Annotation/Filters/Worksets; still net-new for Import/RVT Link categories on any view and for Model/Annotation/Filters/Worksets on non-template views specifically (per the 6.1b correction).
- **Option C — narrow the scope to just the genuinely-uncovered gap**: Import-category and RVT-Link overrides on any view, plus Model/Annotation/Filter/Workset overrides specifically on non-template views, using the same `GetCategoryOverrides`/`GetCategoryHidden` pattern already proven in `view_category_overrides_model.py` but applied without the existing `v.IsTemplate` filter. This is the option best supported by what this pass actually found (a real, narrower-but-not-tiny gap — not just Import/RVT Links) rather than a broad reimplementation of already-covered ground — but is still a genuinely new scope decision, not something already decided by precedent.

None of these are chosen here. This is presented as raw material for that design conversation, per the prompt's own instruction.

---

## 7. `dimension_types` field expansion

### 7.0 Premise mismatch, now resolved — `tools/probes/PROBE_EXPORTER_DELTA.md` has been added to the repo

The original pass here found the file missing (`find`/`ls` against `tools/probes/`, zero hits) and derived everything below independently from `tools/probes/probe_dimension_types.py` and `tools/probes/Exports/PROBE_INVENTORY.csv`/`.md`, plus the raw per-run JSON (which carries `observed_on_families`, not present in the CSV/MD rollups). The file has since been supplied and committed to the repo at that exact path (2026-08-07), with a provenance/errata header. Cross-checking it against the independent findings below: the field list matches closely (Alternate Units, Leader config, Witness lines, Equality dimensions, Centerline/tick marks, plus a few names not independently found here — folded in at the end of §7.4), but its "not currently captured" framing is wrong for the entire text-appearance cluster (Text Font/Size/Bold/Italic/Underline/Width Factor/Background/Color/Line Weight) — see the errata note after §7.3. Everything else in §7.1-7.5 below was derived and verified independently and is unchanged by the file's arrival.

### 7.1 How the probe reads fields — and why that limits BIP confirmation

For most candidate fields, the probe does **not** call `get_Parameter(BuiltInParameter.X)` or `LookupParameter("Name")` per field — it generically enumerates `t.GetOrderedParameters()` and keys each observation by `Definition.Name` (`probe_dimension_types.py:857-869`: `params = list(t.GetOrderedParameters())` at 860, `pk = "p.{}".format(dn)` at 869). This means the probe alone only confirms the **UI display name**, not which BuiltInParameter (if any) backs it. The probe does a targeted lookup in exactly two places: `_get_family_name_param()` (100-139, BIP `SYMBOL_FAMILY_NAME_PARAM` then `LookupParameter("Family Name")` fallback) and the tick-mark crosswalk `_find_tick_param()` (979-991, pure `LookupParameter` over name candidates). So confirming real BIP names for delta fields requires cross-referencing the existing extractor (`domains/dimension_types.py`, `core/dimension_type_helpers.py`) and the sibling `domains/text_types.py`, not the probe in isolation — done below field-by-field; flagged as an open question where no BIP is known anywhere in-repo.

### 7.2 Existing param-read helper pattern (new fields should follow this, not invent a second one)

`first_param(elem, bip_names=None, ui_names=None)`, `core/rows.py:134-171`: tries each name in `bip_names` via `getattr(BuiltInParameter, name)` + `elem.get_Parameter(bip)` (148-160), falls back to `elem.LookupParameter(name)` per name in `ui_names` (162-169), returns the first parameter with `HasValue`. Imported into `core/dimension_type_helpers.py:30-40` and re-exported into `domains/dimension_types.py:20`; every existing field read in this domain already goes through it. Companion extractors: `_as_string`, `_as_value_string` (Integer/enum params), `_as_double`, `_as_int`, all in `core/rows.py`.

Two dimension-type-specific composite helpers in `core/dimension_type_helpers.py` are directly reusable for new fields: `_read_tick_mark_sig_hash(d, ctx, doc)` (line 518, resolves a Tick-Mark ElementId through `ctx["arrowheads_by_type_id"]` to a `sig_hash`; used today only by linear/angular/radial/diameter) and `_read_unit_format_info(d)` (line 567, reads `d.GetUnitsFormatOptions()` — a `FormatOptions` object, not a plain parameter — for primary units).

### 7.3 Fields that already exist — verify before treating as new (the `floor_types.function` trap recurs here repeatedly)

| Field (probe `p.*` key) | Emits as | Where implemented |
|---|---|---|
| Text Font / Size / Bold / Italic / Underline / Width Factor / Background | `dim_type.text_font/text_size_in/text_bold/text_italic/text_underline/text_width_factor/text_background` | `core/dimension_type_helpers.py:412-484` |
| Color | `dim_type.color_rgb` | `core/dimension_type_helpers.py:487-496` |
| Line Weight | `dim_type.line_weight` | `core/dimension_type_helpers.py:499-509` (`bip_names=["LINE_WEIGHT","DIM_LINE_WEIGHT"]`) |
| Witness Line Control | `dim_type.witness_line_control` | `domains/dimension_types.py:270, 581` |
| Center Marks / Center Mark Size | `dim_type.center_marks`/`center_mark_size` | `domains/dimension_types.py:880-892, 1204-1216` |
| Radius/Diameter Symbol Location & Text | `dim_type.radius_symbol_*`/`diameter_symbol_*` | `domains/dimension_types.py:904-913, 1228-1237` |
| Text Orientation / Location / Symbol | `dim_type.text_orientation/text_location/symbol_name` | `domains/dimension_types.py:1621-1639, 2008-2026` |
| Indicator variants (Top/Bottom/Elevation/N-S/E-W/Coordinate/Include Elevation) | `dim_type.*_indicator*` | `domains/dimension_types.py:1567-1615, 1945-1993` |
| Slope Direction / Read Convention | `dim_type.slope_direction` | `domains/dimension_types.py:2287-2290` (reads `ui_names=["Slope Direction","Read Convention"]` as a name fallback pair — probe shows "Read Convention" observed on angular/diameter/linear/radial, not spot, so this fallback rarely engages for spot_slope in practice; worth closer scrutiny but not a delta-field candidate) |
| Leader Line Length | `dim_type.leader_line_length` | `domains/dimension_types.py:2297-2300` (spot_slope only) |

None of these belong in a "new fields" PR.

**Errata note**: `tools/probes/PROBE_EXPORTER_DELTA.md` lists the entire "Text styling" cluster above (Text Font, Text Size, Bold, Italic, Underline, Width Factor, Text Background, plus Color and Line Weight elsewhere in its list) as uncaptured. This is wrong — `git blame` on `core/dimension_type_helpers.py:411` (`_build_text_appearance_items`, which reads all of these) and its 7 call sites in `domains/dimension_types.py` (lines 295, 606, 931, 1255, 1657, 2045, 2317 — one per partition, confirmed wired in, not dead code) both trace to commit `26523e5` (2026-07-15 19:34:13 -0700), three weeks before that document's 2026-08-05 probe run. Same root cause as the Area 2 errata: whatever comparison produced that document ran against a stale view of the code. Three of `PROBE_EXPORTER_DELTA.md`'s items (Text Offset, Text Offset from Leader, Text Offset from Symbol) were independently re-verified here as genuinely absent (zero grep matches for "Text Offset" anywhere in `core/dimension_type_helpers.py`/`domains/dimension_types.py`) and are folded into §7.4 below.

### 7.4 Genuinely new candidate fields

Family applicability below comes from the raw per-run JSON's `observed_on_families` (the probe's own `_shape_family_from_label` bucketing — **not** the same partitioning as the 7 `extract_*` functions; see the cross-cutting caveat in 7.5.1). Storage/q_counts cited from `tools/probes/Exports/PROBE_INVENTORY.csv`.

**Alternate units** (linear-only tab in the Revit UI, per standard Revit behavior — but see caveat): `Alternate Units`, `Alternate Units Format`, `Alternate Units Prefix`, `Alternate Units Suffix` (CSV lines 250-253). No BIP confirmed in-repo for any of these. **Open question, flagged as important**: the probe's `observed_on_families` shows these present on **all** families (angular/diameter/linear/other/radial/spot), which contradicts the standard Revit UI restriction of Alternate Units to Linear Dimension Style. Two unconfirmed explanations: either these params genuinely exist on every `DimensionType`'s parameter set regardless of shape (UI just hides the tab for non-applicable shapes), or the probe's label-based family bucketing is misclassifying some linear types. **Do not treat "missing" on angular/radial/diameter as a real gap** until this is resolved with either a document containing confirmed non-linear alternate-units types, or Revit API documentation. `Alternate Units Format` is `storage=None, q=unsupported` (line 251) — not a plain parameter, same situation as primary Units Format; the unused helper `_format_options_to_kv()` (`core/dimension_type_helpers.py:269-310`, confirmed zero call sites anywhere in the repo) has adaptable `suppressspaces`/`suppressleadingzeros` logic but no `_read_alternate_unit_format_info`-equivalent exists today; whether `DimensionType` exposes a second `FormatOptions` accessor for alternate units is unconfirmed.

**Leader — spot-family arrowhead/line-weight config**: `Leader Arrowhead` (line 287), `Leader Arrowhead Line Weight` (288), `Leader Line Weight` (290) — families `['other','spot']`. High-confidence finding: `dim_type.leader_arrowhead_sig_hash` is **already reserved** in `contracts/domain_identity_keys_v2.json` `allowed_keys` (not `required_keys`) for all three spot sub-domains (line 237 spot_coordinate, 281 spot_elevation, 317 spot_slope), but is **not implemented anywhere in `domains/dimension_types.py`** (zero hits on repo-wide grep) — the only real implementation of `leader_arrowhead*` in the repo is the sibling `domains/text_types.py:328,449-451` (same field documented in Area 10 above). Since it's `allowed` but not `required` this doesn't block records today, but it's a real contract/code gap. The exact pattern to reuse is already worked out in `text_types.py`: `first_param(t, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])` then resolve through `ctx["arrowheads_by_type_id"]` to a `sig_hash`, mirroring `_read_tick_mark_sig_hash`. A follow-up PR should likely promote this into a shared `core/dimension_type_helpers.py` helper (it would then be used in 4 places: text_types + 3 spot domains) rather than duplicate it again. `Leader Arrowhead Line Weight`/`Leader Line Weight` have no BIP known in-repo — open question.

**Leader — a *different* subsystem for linear/angular/radial/diameter**: `Leader Tick Mark` (line 291), `Leader Type` (292), `Show Leader When Text Moves` (300) — families `['angular','diameter','linear','radial']`, the **opposite** family split from the spot-family leader fields above. This is the "Beyond Witness Lines" leader shown when dimension text is moved, a structurally distinct Revit feature from the spot-type Leader Arrowhead despite the shared word "Leader" — a follow-up PR must not conflate the two. No BIP known in-repo for any of the three.

**Witness lines** (linear/angular only): `Witness Line Extension` (323), `Witness Line Gap to Element` (324), `Witness Line Length` (325) — families `['angular','linear']`. `Witness Line Tick Mark` (326) — family `['linear']` **only**, unlike its three siblings (asymmetry consistent across the one run inspected but not cross-checked against the other two runs — flagged as open question, possibly a small-sample artifact). Not applicable to radial/diameter/spot, consistent with `dim_type.witness_line_control` already being linear/angular-only. No BIP known in-repo for any of the four.

**Equality dimensions** (linear/angular only): `Equality Text` (278), `Equality Witness Display` (279), `Equality Formula` (277, `storage=None, q=unsupported` — same "not a plain parameter" situation as Units Format, likely needs a dedicated API call). No BIP known in-repo.

**Centerline / interior tick marks** (linear/angular only): `Centerline Symbol` (261), `Centerline Pattern` (260), `Centerline Tick Mark` (262), `Interior Tick Mark` (284), `Interior Tick Mark Display` (285). Not applicable to radial/diameter/spot, consistent with the Revit UI (Centerline tab exists only for Linear/Angular Dimension Style). `Centerline Symbol`/`Centerline Tick Mark` are `ElementId` storage (like Tick Mark and Leader Arrowhead) and likely candidates for the same `sig_hash`-via-`ctx["arrowheads_by_type_id"]` pattern — but the probe's example value for `Centerline Symbol` is `"ANG-Centerline"` (line 261), which reads like a line-style/annotation-symbol name rather than an obvious arrowhead-map entry; whether `ctx["arrowheads_by_type_id"]` actually covers it, or a different ctx map is needed, is an open question.

**Radial/diameter tick weight**: `Tick Mark Line Weight` (315, families `['angular','diameter','linear','radial']`) — distinct from the already-implemented `dim_type.line_weight` (7.3); this is the tick-mark glyph's own weight, not the dimension line's. No BIP known in-repo.

**Text offsets** (from `PROBE_EXPORTER_DELTA.md`, independently re-verified as absent via grep — zero matches for "Text Offset" anywhere in `core/dimension_type_helpers.py`/`domains/dimension_types.py`): `Text Offset`, `Text Offset from Leader`, `Text Offset from Symbol` — companions to the already-implemented text-appearance cluster (§7.3 errata note) but not covered by `_build_text_appearance_items`. No BIP confirmed in-repo; not independently cross-checked against `PROBE_INVENTORY.csv` for family applicability in this pass.

**Other behavioral fields from `PROBE_EXPORTER_DELTA.md`**, not independently re-derived from the CSV in this pass but consistent with the field naming already seen elsewhere in this domain: `Dimension String Type` (Continuous/Baseline/Ordinate — a dimension-chaining mode, not to be confused with the out-of-scope `Ordinate Dimension Settings` item below), `Rotate with Component`, `Suppress Spaces`, `Coordinate Base`, `Elevation Base`, `Show Opening Height`. `Suppress Spaces` is worth flagging specifically: it's the same `FormatOptions` boolean-flag gap already documented for `units.py` in Area 8 (§8.2's `use_default`/`use_digit_grouping`/etc.), but here on the *DimensionType's own* `GetUnitsFormatOptions()` object rather than the doc-level `Units` object — `core/dimension_type_helpers.py:_read_unit_format_info` (§7.2 above) already reads `RoundingMethod`/`Accuracy`/`unit_type_id` off that same `FormatOptions` object but not any of its boolean flags. A follow-up PR could plausibly extend `_read_unit_format_info` the same way Area 8 proposes extending `units.py`'s per-spec loop, rather than treating it as an unrelated field. None of these six were independently verified against `contracts/domain_identity_keys_v2.json` or cross-checked for family applicability in this pass — treat as provisional pending that check.

**Out of scope for "add a delta field to an existing partition"**: `Ordinate Dimension Settings` (family `['linear']`, `storage=None, q=unsupported`) has **no corresponding `extract_ordinate` function anywhere in `domains/dimension_types.py`** — confirmed via `grep '^def extract_'`, only 7 functions exist (linear/angular/radial/diameter/spot_elevation/spot_coordinate/spot_slope). Adding this would mean a new 8th partition/domain plus a new contract entry — materially bigger scope than the other candidates; should not be silently folded into the ~25-field estimate. `Include Station`/`Station Indicator` (family `['other']`) look tied to `SHAPE_ALIGNMENT_STATION_LABEL`, which the code already buckets into spot_coordinate's handled-shapes set (`_SPOT_COORD_HANDLED`, `domains/dimension_types.py:77`) but neither field is implemented; given they're civil/alignment-specific and bucketed under "other" rather than cleanly "spot" by the probe, treat as lower-confidence/niche rather than core to the ~25.

### 7.5 Cross-cutting open questions

1. **Probe family buckets ≠ extractor partitions.** The probe's `_shape_family_from_label()` (`probe_dimension_types.py:282-304`) produces buckets `linear/angular/radial/diameter/arc/spot/ordinate/other` — `spot` is a *single* bucket, while the actual code has three separate spot extractors (`extract_spot_elevation`/`extract_spot_coordinate`/`extract_spot_slope`). The probe data alone cannot say which of the 3 spot sub-domains a `spot`-bucketed field (e.g. Leader Arrowhead, Leader Line Weight) belongs to — resolving this needs either a probe run against a document with all three types well-populated (inspecting individual `dim_type.id`/`shape_key` samples in the raw per-type records, not just the aggregated `param_index`) or Revit API/UI research. Not resolved in this pass — only the aggregated inventory was available.
2. **No BuiltInParameter names confirmed for most new fields** — everything in 7.4 except `Leader Arrowhead` (`LEADER_ARROWHEAD`, via `text_types.py:328`) and the already-implemented fields in 7.3. A follow-up PR will likely need `LookupParameter` (UI-name) fallback for most of these unless BIP names are sourced from Revit SDK documentation outside this repo.
3. **Alternate Units family-applicability contradiction (7.4)** is the single most important open item to resolve before writing extraction code — acting on the probe's literal "observed on all families" without confirming whether that's real risks over- or under-reporting "missing" on non-linear shapes.
4. Family-applicability data above is sourced from **one** run's raw JSON (`probes_2025_20260805T120010-8cf8cf.json`); the other two run files were not cross-checked for consistency in this pass (though the CSV/MD rollup's `run_count=3` fields suggest agreement) — worth a spot-check before finalizing the field list.

---

## 8. `units.py` formatting flags

### 8.1 Per-spec loop structure (`domains/units.py`)

The per-spec loop runs at line 194 (`for label, spec_id in specs:`) through line 365. `fmt` (the `FormatOptions` object) is fetched once per iteration at line 203: `fmt = u.GetFormatOptions(spec_id)`. Existing field reads inside this loop, all off the same `fmt` object:
- `unit_type_id` — `fmt.GetUnitTypeId()` (line 212, method call, not property)
- `symbol_type_id` — `fmt.GetSymbolTypeId()` (line 224)
- `accuracy` — `getattr(fmt, "Accuracy", None)` (line 236), via `canonicalize_float`
- `rounding_method` — `getattr(fmt, "RoundingMethod", None)` (line 246), via `canonicalize_enum`

### 8.2 Same per-spec loop, no new code path needed

All 6 candidate flags (`use_default`, `use_digit_grouping`, `use_plus_prefix`, `suppress_leading_zeros`, `suppress_spaces`, `suppress_trailing_zeros`) are properties of the same `FormatOptions` object (`fmt`), confirmed by the probe's `FORMAT_SURFACE` table (`tools/probes/probe_units.py:232-238`: `UseDefault`, `UseDigitGrouping`, `UsePlusPrefix`, `SuppressLeadingZeros`, `SuppressTrailingZeros`, `SuppressSpaces`, all read via `getattr(fmt, accessor)` — same pattern as `Accuracy`/`RoundingMethod`). They would be additional `getattr(fmt, "...", None)` reads inside the existing loop, immediately alongside the accuracy/rounding_method reads — no new collector pass, no new per-spec resolution.

One trivial code change needed: `core.record_v2.canonicalize_bool` exists (`core/record_v2.py:205`) but is not currently imported in `domains/units.py` (its import block, lines 40-54, imports only `canonicalize_str`/`canonicalize_enum`/`canonicalize_float`) — an import addition, not a new code path.

### 8.3 Doc-level fields have no existing record to attach to

Record ID scheme: `record_id = "units:{}".format(label)` (line 195), where `label` is always one of the per-spec labels from `specs_raw` (lines 141-189: `length`, `area`, `volume`, ... `structural_displacement`) — there is no synthetic/doc-level pseudo-spec entry in that list.

`decimal_symbol`, `digit_grouping_amount`, `digit_grouping_symbol` come from the `Units` object `u` directly (confirmed via the probe's reflection sweep: `refl.Units.DecimalSymbol`/`DigitGroupingAmount`/`DigitGroupingSymbol`, all `q=ok`), not from any per-spec `FormatOptions`. Grepping `domains/units.py` for `decimal_symbol`/`digit_grouping`/`DecimalSymbol`/`DigitGrouping` returns zero matches — these are genuinely absent from the extractor today, not just unwired.

Because every existing record is keyed to a spec label, adding these 3 doc-level fields has no natural home among the current 38 per-spec records — but **a bare synthetic doc-level record (e.g. `record_id = "units:_doc"`) is not a viable fix as stated**, and this document originally proposed one without checking the contract. `contracts/domain_identity_keys_v2.json`'s `units.required_keys` is `["units.spec", "units.unit_type_id"]`, and `validators/record_v2.py:218-231` treats a missing required key as a hard violation (`identity.required_key.missing:{rk}`) that blocks the record. A genuine document-level record has neither a real `spec` nor a real `unit_type_id` to supply, so it would either fail contract validation as-is or need one of: (a) a contract change making `units.spec`/`units.unit_type_id` conditionally required (not required for a designated doc-level `record_id`), (b) placing these 3 fields on a different, non-`units` domain (e.g. folded into `identity.py`'s existing single document record), or (c) some other record-shape accommodation not yet designed. Whichever path is chosen is itself a small design decision, not a mechanical add — flagging it as such rather than prescribing the doc-level record as a given.

### 8.4 Probe confirmation

`tools/probes/probe_units.py`'s `FORMAT_SURFACE` (lines 223-240) declares all 6 flags (plus `suppress_unit_symbol`, not on the promotion list) as `prop_bool` reads off `fmt`. `UNITS_GLOBAL_SURFACE` (lines 243-247) declares `decimal_symbol`/`digit_grouping_amount`/`digit_grouping_symbol` as `prop_any` reads off `u` (the `Units` object), separately from the per-spec surface — reinforcing the doc-level-vs-per-spec split.

Observed values (`tools/probes/Exports/PROBE_INVENTORY.csv`, Revit 2025, 3 runs): all 6 flags at `ok=450; unreadable=3` (lines 1966-1974), example `0`/`False`, pure API-property reads (no BuiltInParameter path observed or used). `p.units.decimal_symbol`/`digit_grouping_amount`/`digit_grouping_symbol` each at `ok=3` (lines 1975-1977), sampled once per probe run (global, not per-spec) — confirming doc-level cardinality.

### 8.5 Contract check

`contracts/domain_identity_keys_v2.json`'s `units.allowed_keys` currently lists only `units.spec`, `units.unit_type_id`, `units.symbol_type_id`, `units.accuracy`, `units.rounding_method`. None of the 6 flags nor the 3 doc-level fields appear — genuinely new (unlike the `floor_types.function` false-positive in Area 2).

---

## 9. `object_styles` additions

### 9.1 Category iteration loop (`domains/object_styles.py`)

The shared loop is `for cat, is_subcategory, parent in list(cats or []):` at line 271, inside `_extract_object_styles` — shared by all 4 partitions (`extract_model`/`extract_annotation`/`extract_analytical`/`extract_imported`, lines 530-543, which all call this one function with different `kind`/flags). `cats` is built once per document via `_collect_categories` (lines 71-116), cached on `ctx` (line 73), then filtered per-category-type inside the loop (`_matches_category_type`, line 273).

### 9.2 Same loop, no new collector pass needed

`cat` (aliased `cat_obj` at line 295) is already the working object for the whole loop body (271-481). Existing reads directly off it include `cat_obj.GetLineWeight(...)` (323), `cat_obj.LineColor` (339), `cat_obj.GetLinePatternId(...)` (352), `cat_obj.CategoryType` (440), `cat_obj.Id`/`.UniqueId` (413, 452, 456), and `getattr(cat_obj, "Material"/"MaterialId", None)` in `_material_ref_item` (160/171). The 5 candidate fields are the same shape:

- `can_add_subcategory` → `Category.CanAddSubcategory`, a plain boolean property on the same `cat`/`cat_obj`
- `has_material_quantities` → `Category.HasMaterialQuantities`, same
- `is_cuttable` → `Category.IsCuttable`, same
- `parent_name` → **already computed in-loop, but the existing variable is NOT directly promotable as-is** — line 278: `parent_name = canon_str(getattr(parent if is_subcategory else cat, "Name", None))`, used today to build `row_key` (296) and the record label. For a top-level (non-subcategory) row, this line deliberately assigns the category's *own* name — `row_name` is separately set to the literal string `"self"` for that case — so the existing `parent_name` local encodes "self-referential parent, for row_key purposes" for top-level categories, not "no parent." Directly promoting this variable to a new `parent_name` identity item would misrepresent every top-level category as its own parent, contrary to the probe's `c.parent_name` (`probe_object_styles.py:284`), which reads `parent` and is correctly `missing` for the 279 top-level rows in the probe data. A correct new identity item must derive from `parent` only when `is_subcategory` is true, and emit missing/`None` otherwise — it can reuse the existing tracked `parent` value from `_collect_categories`, just not the existing `parent_name` local verbatim. (`Category.Parent` itself is not the right source either — the probe's own reflection sweep shows `refl.Category.Parent` returns `q=missing` for top-level categories, consistent with the tracked-`parent`-variable approach being the only reliable path, just applied correctly this time.)
- `tab` → not an API property; the probe computes it heuristically via `_infer_object_styles_tab` (`probe_object_styles.py:186-207`, using `CategoryType` + name-string matching for "Imported"/"Annotation"/"Analytical"/"Model"/"Other"). In production this is effectively already implicit in which of the 4 `extract_*`/`kind` partitions emitted the record (`kind` param, line 217; `_matches_category_type`, line 126) — an explicit `tab` field would be a derived local computation from data already in scope (`kind`/`cat.CategoryType`), not a new object read.

So: `can_add_subcategory`/`has_material_quantities`/`is_cuttable` are additional `getattr(cat_obj, "...", None)` reads inserted into the existing loop body (near the other `cat_obj`-derived items, lines 300-386); `parent_name` needs a *new* identity item derived from the already-tracked `parent` value (correctly `None` for top-level categories), not a verbatim promotion of the existing `parent_name` local; `tab` needs no new Category read at all. **No new `FilteredElementCollector` pass and no separate category-lookup pass required for any of the 5** — all the needed data is already in scope in the existing loop, but `parent_name` needs new derivation logic, not just a promotion.

### 9.3 Probe confirmation

`tools/probes/probe_object_styles.py`'s `PARAM_DEFS` (lines 282-313): `c.can_add_subcategory` (310, `cat.CanAddSubcategory`) → CSV `ok=643`, example `True`; `c.has_material_quantities` (311) → `ok=643`, example `False`; `c.is_cuttable` (312) → `ok=643`, example `False`; `c.parent_name` (284, `_get_name(parent)`) → `ok=364; missing=279` (missing for top-level, non-subcategory rows), example `"MEP Fabrication Ductwork"`; `c.tab` (288) → `ok=643`, example `"Analytical"`. Reflection rows corroborate: `refl.Category.CanAddSubcategory`/`HasMaterialQuantities`/`IsCuttable` all `q=ok`; `refl.Category.Parent` is `q=missing` — confirming `parent_name` must stay derived from the tracked `parent` variable, not `cat.Parent`, or it would go missing for many rows.

### 9.4 Contract check

`contracts/domain_identity_keys_v2.json`'s `allowed_keys` for all 4 `object_styles_*` domains currently list only `obj_style.row_key`, `.weight.projection`, `.weight.cut` (model only), `.color.rgb`, `.pattern_ref.sig_hash`, `.material_sig_hash` (model only). None of the 5 candidate fields appear — genuinely new/unregistered.

### 9.5 Open question

Whether `tab` should be modeled as a derived/coordination field (like the existing `obj_style.category_type` coordination item, line 446) versus a true identity item is a design choice not resolvable from the code alone.

---

## 10. `text_types` — `Leader Arrowhead`

**Stop-and-report — this field already exists, same pattern as Area 2.** `Leader Arrowhead` is already fully implemented in `domains/text_types.py`: read, cross-referenced against the `arrowheads` domain, and emitted as metadata — but deliberately excluded from the identity/sig-hash surface.

- **Flat items shape confirmed**: `text_types.py` uses `core.canonical_items.build_flat_items` (imported 48-50, called 537-543) to assemble `rec_v2["items"]` from `identity_basis.items` plus the phase2 semantic/cosmetic/coordination/unknown buckets, then strips `identity_basis`/`phase2`/`join_key`/etc. (544-548). "Follows the existing param-read pattern" here means: read via `first_param`, then route the result into the phase2 `unknown_items` bucket (never `identity_items_v2`/`TEXT_TYPE_SEMANTIC_KEYS`), which `build_flat_items` folds into the flat `items:[{k,v,q}]` array with the item's bucket-derived role.
- **Revit parameter**: `BuiltInParameter.LEADER_ARROWHEAD`, UI-name fallback `"Leader Arrowhead"` — `domains/text_types.py:328`: `first_param(t, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])`. Confirmed by probe: `tools/probes/Exports/PROBE_INVENTORY.csv:1900` — storage `ElementId`, `ok=13` across all 3 probe runs, example `"22 Degree Filled Arrow-Medium"`.
- **Read path**: `p.AsElementId()` → `doc.GetElement(arrow_id)` → resolves to the arrowhead element (`text_types.py:330-334`).
- **Cross-reference to `arrowheads.py`**: already wired via `ctx["arrowheads_by_type_id"]`, populated in `domains/arrowheads.py:298,648` and consumed in `text_types.py:337-340` to attach `leader_arrowhead_sig_hash`. The same ctx-key pattern is reused by `core/dimension_type_helpers.py:521,547` for `dimension_types` (`dim_type.leader_arrowhead_sig_hash`, `contracts/domain_identity_keys_v2.json:237,281,317`).
- **Param-read helper**: `first_param(elem, bip_names=None, ui_names=None)` at `core/rows.py:134-171` — tries `BuiltInParameter` by name first, falls back to `LookupParameter` by UI name. This is the exact helper `text_types.py` already uses throughout (e.g. lines 266, 272, 275, 278, 328).
- **Identity/contract status**: `contracts/domain_identity_keys_v2.json:711-724` (`text_types.allowed_keys`) does **not** include any `leader_arrowhead*` key — confirmed by an explicit code comment at `text_types.py:372`: *"Leader Arrowhead is excluded from v2 by policy decision."* It lives only in phase2 `unknown_items` and thus in the flat `items` array with `q="ok"`/`v=None` as a valid tri-state "no arrowhead" signal, or populated uid/name/sig_hash when present. Also excluded from `debug.uid_excluded_from_sig`/`leader_arrowhead_uid_excluded_from_sig` (line 533).

**Conclusion**: there is no new field to add. If the intent was promoting `leader_arrowhead` into the v2 identity surface, that's a hash/policy-scope change, out of scope for this pass (same shape as the `floor_types.function` finding in Area 2).

**Errata note**: `tools/probes/PROBE_EXPORTER_DELTA.md` also lists `Leader Arrowhead` as the one delta field for `text_types`. Same root cause as the Area 2 and Area 7 errata: `git blame` on `domains/text_types.py:328` shows the read was added in commit `26523e5` (2026-07-15 19:34:13 -0700), three weeks before that document's 2026-08-05 probe run.

---

## 11. `line_styles` — `parent_cat`

**No existing `.Parent` traversal in this file.** `domains/line_styles.py` iterates `Category.SubCategories` directly (line 208: `subs = list(getattr(lines_cat, "SubCategories", []) or [])`), so the loop variable `sc` is already a `Category` object (not a `GraphicsStyle`) — it calls `sc.GetLineWeight(...)`, `sc.LineColor`, `sc.GetLinePatternId(...)` directly as `Category` members (lines 231-241, 291). There is no `GraphicsStyle → Category` walk anywhere in this file to reuse. Adding `parent_cat` would be a net-new lookup in this file, but a trivial one — `sc.Parent` (`Category.Parent`), not the fuller `GraphicsStyle.GraphicsStyleCategory → Category.Parent` chain the prompt's premise assumed.

A near-identical `.Parent` call already exists in a **sibling domain**: `domains/object_styles.py:415` — `parent_cat_obj = cat_obj.Parent` (try/except at 414-417), used there for a purge-lookup helper (`_subcategory_purge_lookup`), not to emit a `parent_cat` identity field. This confirms `Category.Parent` is an established, low-risk call pattern in this codebase generally — just not yet used to emit a field in `line_styles.py` specifically.

The probe file does walk `GraphicsStyle → Category → Parent`, but that's a different object graph than the domain file's `sc`: `tools/probes/probe_line_styles.py:406-407` (`c = gs.GraphicsStyleCategory`; `parent = c.Parent`), where `gs` is a `GraphicsStyle` obtained differently than `line_styles.py`'s `sc`.

**Probe evidence the field is real and populated**: `tools/probes/Exports/PROBE_INVENTORY.csv:908-909` — `v.parent_cat.id` and `v.parent_cat.name`, both `ok=150` across 3 runs, example id `-2000051` / name `"Lines"`.

**Contract check**: `contracts/domain_identity_keys_v2.json:475-493` (`line_styles.allowed_keys`) currently has only `line_style.weight.projection`, `.color.rgb`, `.weight.cut`, `.pattern_ref.sig_hash`, `.pattern_ref.synopsis` — no `parent_cat` key. Genuinely not yet registered or emitted.

**Open question**: whether `parent_cat` should land in `identity_basis.items` (identity-affecting) or a phase2 `cosmetic_items`/`unknown_items` bucket (metadata-only, matching how `line_style.source_element_id`/`source_unique_id` are handled at lines 447-458) — not resolvable from the code since the field doesn't exist yet in this file.

---

## 12. `loaded_family_types` additions

None of `StructuralMaterialType`, `IsActive`, `CanHaveStructuralSection`, `HasThermalProperties` currently appear in `domains/loaded_family_types.py` (confirmed absent from the full 418-line file) or in `contracts/domain_identity_keys_v2.json:494-529` (`loaded_family_types.allowed_keys` lists only `lft.shape_gate.category(_id)`, `.type_parameter_schema_hash`, `.type_parameter_count`, `family_is_in_place`, `family_is_editable`, `family_symbol_count`, `type_count`). These are genuinely new.

Evidence comes from the `reflection` probe kind in `tools/probes/probe_loaded_family_types.py`/`PROBE_INVENTORY.csv` (not the curated `param` inventory) — these are `.NET` reflection members on `FamilySymbol`, not Revit `Parameter`s.

| Field | member_kind | Invoked by probe? | Observed |
|---|---|---|---|
| `StructuralMaterialType` | property | yes | `ok`, 60/60 samples every run; example raw/norm=0 (`StructuralMaterialType.Undefined`), `unique_value_count=1` (never varied in sampled data) |
| `IsActive` | property | yes | `ok`, 60/60 every run; both `False`/`True` observed — varies |
| `CanHaveStructuralSection` | **method** | **no — `not_invoked`** | not on the probe's `_ALLOWLISTED_REFLECTION_METHODS` (`probe_loaded_family_types.py:485-518`); `_reflect_try_get` returns `_METHOD_NOT_INVOKED_SENTINEL` without ever calling it |
| `HasThermalProperties` | **method** | **no — `not_invoked`** | same as above |

**Critical caveats:**

1. **Two of the four are zero-arg methods, not properties**, and the probe's reflection sweep explicitly refuses to invoke any zero-arg method that isn't on its ground-truth-verified allowlist (rationale at `probe_loaded_family_types.py:458-484`). `CanHaveStructuralSection`/`HasThermalProperties` are not on that allowlist — there is **zero empirical evidence in this repo** of whether they throw on non-applicable categories; the probe only confirms the members exist via reflection, never their runtime behavior.
2. **No structural-discipline model in the available probe corpus.** The 60-object reflection sample is drawn from `FilteredElementCollector(doc).OfClass(Family)` sorted by name (capped at 60, `probe_loaded_family_types.py:356,374-375`). Across all 3 available probe runs, the full inventory (757 records total) is exclusively non-structural categories (Casework, Detail Items, Specialty Equipment, Generic Models, Lighting/Plumbing/Electrical Fixtures, Doors, Furniture, etc.) — confirmed via filtering the raw JSON exports; the 3 source projects (`KSRF_Hosp_Interior_AR`, `000000000_Arch_Int_ContainerModel_r25`, `2014351100_stn_arch_asc_int_dd`) are all architectural/interior models. **None of the 4 fields have been empirically exercised against a genuinely structural family category** — open question, not resolvable from existing probe data.
3. **No existing sentinel-distinction precedent in this file.** `domains/loaded_family_types.py` imports only `ITEM_Q_OK`/`ITEM_Q_MISSING`/`ITEM_Q_UNREADABLE` — it does not import `ITEM_Q_UNSUPPORTED`/`ITEM_Q_UNSUPPORTED_NOT_APPLICABLE` at all. Every existing field goes through `_safe_attr(obj, attr, None)` (swallows exceptions to `None`) followed by `canonicalize_bool`/`canonicalize_int`/`canonicalize_str`, whose `None` handling always yields `ITEM_Q_MISSING` — a genuine read exception and a legitimately-N/A value currently collapse to the same `"missing"` q in this file. There's no in-file case distinguishing `<NOT_APPLICABLE>`/`ITEM_Q_UNSUPPORTED_NOT_APPLICABLE` from `<UNREADABLE>`/`ITEM_Q_UNREADABLE` to follow. The convention exists elsewhere in the codebase (`domains/compound_types.py:493-495,902-904,1089-1091,1258-1260`; `domains/object_styles.py:327,459-461`) but would need to be introduced fresh here for these 4 fields.

**Open questions to flag explicitly**: (a) real-world exception behavior of `CanHaveStructuralSection`/`HasThermalProperties` on non-applicable categories is unverified; (b) no probe run in this repo includes a structural-discipline model, so category-bucketed ok/unsupported behavior for any of the 4 fields is unconfirmed for structural categories specifically.

---

## Summary — acceptance criteria check

- **All 12 areas have a findings section with file/line citations** — done above, one section per area, in the prompt's order.
- **Every "stop and report" trigger is explicitly answered**:
  - Area 1 (shared mutable module-level state across the 4 `extract_*` functions): **not triggered** — none found (§1.5).
  - Area 5 (`build_segment_manifest.py` sourcing 3+ promotion-list fields independently): **not triggered** — it sources zero of them, because it doesn't read Revit at all (§5.1, §5.4).
  - No explicit stop-and-report trigger was defined for areas 2-4, 6-12, but this pass surfaced unprompted premise mismatches in several of them anyway, flagged inline rather than silently worked around: Area 2 (`ft.function` already implemented), Area 6 (the probe doesn't call the dedicated V/G-override APIs, and the underlying global-table domain is blocked by a 2025-12-18 decision finding no API access exists at all — D-013 — though the probe does surface presence-only signals for per-view V/G-overrides parameters, see §6.1b), Area 7 (`PROBE_EXPORTER_DELTA.md` was missing at the start of this pass, later supplied and added to the repo — see below; several "delta" fields it named already implemented; Alternate Units family-applicability data looks self-contradictory and is flagged as unresolved rather than assumed), Area 10 (`Leader Arrowhead` already fully implemented in `text_types.py`).
- **The compound_types mapping table (Area 1) is complete** — §1.4 maps every function/constant to a destination file, plus §1.3's full downstream-import-path list (`runner/run_dynamo.py`, `tests/test_compound_types_wall.py` — the only two files that import the module path rather than reference domain-name strings).
- **No code, test, or config file was created or modified** other than this document and `tools/probes/PROBE_EXPORTER_DELTA.md` (added 2026-08-07 after being supplied — see the top-of-document revision note and its own provenance header; it is a probe-derived reference document, not extractor/test/config code).

## Cross-area pattern worth calling out once, not per-area

Three separate areas (2, 6, 10) and one within-area finding (7's `leader_arrowhead_sig_hash`) turned out to already be implemented, reserved-but-unimplemented, or resting on a probe that doesn't test what the prompt assumed it tests. `tools/probes/PROBE_EXPORTER_DELTA.md` (supplied and added to the repo after the initial pass, see the top-of-document revision note) independently corroborates most of this document's field-level findings, but itself contains 3 confirmed-wrong "not captured" claims (`ft.function`, dimension_types text-appearance, `text_types` Leader Arrowhead) — all three fields were added in commit `26523e5` (2026-07-15), three weeks before that document's own 2026-08-05 probe run, meaning its underlying comparison ran against a stale checkout. This is a second, independent confirmation of the same lesson: before starting any of the 12 downstream PRs, re-run a targeted grep for the specific field/BuiltInParameter name across `domains/`, `contracts/domain_identity_keys_v2.json`, and `policies/*.json` regardless of what any prior document (including this one, and including `PROBE_EXPORTER_DELTA.md`) claims — this document did that check everywhere it could and reconciled a known-stale source where it found one, but a PR author should re-verify against the repo state at the time they actually start, since both documents are snapshots (2026-08-07 and 2026-08-05 respectively).

