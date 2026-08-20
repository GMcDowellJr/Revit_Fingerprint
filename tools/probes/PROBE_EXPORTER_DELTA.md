<!--
Provenance note (added 2026-08-07, not part of the original document):

This file was produced externally on 2026-08-05 (see title below) and was
missing from the repo when the audit work now summarized by `DECISIONS.md` D-037
looked for it under this exact path. It was supplied after the fact
and is committed here verbatim, unedited, for future tracking -- do not
silently rewrite its findings in place; append corrections instead, the way
this note does.

Known errata, verified against the live repo as of 2026-08-07: three items
below ("not currently captured") are wrong. `ft.function` /
`ft.has_embedded_sweeps` (`domains/compound_types.py`), `Leader Arrowhead`
(`domains/text_types.py:328`), and the full dimension_types text-appearance
cluster (Text Font/Size/Bold/Italic/Underline/Width Factor/Background/Color/
Line Weight, via `core/dimension_type_helpers.py:_build_text_appearance_items`,
wired into all 7 `domains/dimension_types.py` partitions) were all added in
commit `26523e5` (2026-07-15 19:34:13 -0700) -- three weeks before this
document's own 2026-08-05 probe run and before this document was written.
Whatever comparison step produced this file's "not currently captured"
claims for those items ran against a stale view of `domains/`. Every other
claim in this document that Audit 11 independently cross-checked (worksets,
browser_organization, identity, object_styles, line_styles,
loaded_family_types field lists; the units.py formatting-flag gap; the
majority of the dimension_types delta) held up. See Audit 11 Areas 2, 6, 7,
and 10 for the full reconciliation, including a corrected read on the
phase_graphics "V/G Overrides" claim below (real, but presence-only --
StorageType.None, no decoded override content -- not the same as
`view_category_overrides_model.py`/`view_templates.py`'s decoded overrides,
and not new evidence against `DECISIONS.md` D-013's finding that the
*global* per-phase-status override table has no API access at all).

This file also covers domains outside Audit 11's 12-area scope (materials,
arrowheads, fill_patterns, line_patterns, phases, phase_filters,
view_category_overrides, view_filter_definitions,
view_filter_applications_view_templates, view_templates) and the `views`
domain, which Audit 11 explicitly excluded pending its own scoping
conversation. Treat that material as reference for a future Step 0 pass,
not as already-investigated by Audit 11.
-->

# Probe -> Exporter Delta (2026-08-05 probe run)

Findings-only pass. For each of the 25 probed domains, compares what
`domains/*.py` currently captures as identity items (`make_identity_item`
keys, verified in the live repo) against what the probe run shows as
populated/resolvable (`q=ok`, real values, decent presence). Lists only the
delta -- fields the probe demonstrates are available and aren't currently
captured. Generic Element-API bookkeeping already tracked by the crosswalk
workstream (Id, UniqueId, WorksetId-as-raw-id, GroupId, LevelId,
CreatedPhaseId/DemolishedPhaseId, OwnerViewId, AssemblyInstanceId,
GetTypeId, Pinned, IsValidObject, VersionGuid) is intentionally excluded
here -- that's the batch-1 crosswalk PR's job, not this list's.

No code changed. Read-only comparison against current `domains/` source.

---

## Zero-exporter domains (probed, nothing in `domains/` at all)

### `worksets` -- no exporter exists
- `workset.name`, `workset.kind`, `is_default_workset`, `is_editable`, `is_active_workset`, `owner`, `unique_id`
- Per-`WorksetKind` collector counts: `UserWorkset`, `StandardWorkset`, `ViewWorkset`, `FamilyWorkset`, `OtherWorkset`
- Doc-level: `is_workshared`, `active_workset_name`

### `browser_organization` -- no exporter exists
- `org_id`, `sorting_order`, `sorting_parameter_id` (e.g. `VIEW_NAME`)
- `filter_param_has_value`, `folder_items_walked_count`, `name_fallback_used_count`
- `refl.BrowserOrganization.FamilyName` (e.g. `Browser - Views`)

### `views` -- no exporter exists (distinct from `view_templates.py`, which is template-level only)
Largest untapped surface (388 probed keys). High-signal subset:
- `CropBoxActive`, `CropBoxVisible`, `IsPerspective`, `IsSectionBoxActive`, `IsCallout`
- `BodyTextTypeId`/`HeaderTextTypeId`/`TitleTextTypeId` (schedule text-type crosswalk targets)
- `ViewTemplateId`, `ViewType`, `Discipline`, `DetailLevel`, `Scale`, `PartsVisibility`
- `GetFilters`/`GetOrderedFilters` (already a batch-1 crosswalk target)
- Recommend scoping this one explicitly before promoting -- 388 keys is a lot to take wholesale.

---

## Domains with a real exporter, field-level delta

### `identity` -- currently only 5 fields captured (`is_workshared`, `project_title`, `revit_build`, `revit_version_name`, `revit_version_number`)
This is the biggest surprise in the whole review: virtually none of Project Information is captured today. Probe shows all of this resolves cleanly:
- `project_info.name`, `Project Number`, `Project Status`, `Project Address`, `Project Issue Date`
- `Client Name`, `Building Name`, `Organization Name`, `Organization Description`
- `Office` (+ Address/City/State/Zip/Country/Telephone/Fax/Legal Entity)
- IFC GUIDs (`IfcBuilding GUID`, `IfcProject GUID`, `IfcSite GUID`)
- `doc.central_path`, `doc.path_name`, `app.username`
- These are exactly the fields BC/client segmentation would want as ground truth rather than sourced elsewhere -- worth checking whether `build_segment_manifest.py` already gets these from a different path (it's protected/out-of-scope, but worth confirming there's no gap between what it assumes and what's actually capturable here).

### `phase_graphics` -- explicitly `unsupported.not_implemented` in production today
The code comment says "does not implement a full capture" and the API access for a global PhaseGraphicsSettings object isn't reliable. The probe took a different approach -- sweeping per-View override parameters instead of a global settings object -- and it resolves at ~91% (136/150 views):
- `V/G Overrides Model`, `Annotation`, `Filters`, `Import`, `RVT Links`, `Worksets` (per view)
- `Phase`, `Phase Filter` (per view)
This is a real path to implementing the domain that's currently a documented gap, via a different extraction strategy than what was tried before.

### `compound_types.py` (`floor_types`, `roof_types`, `ceiling_types`) -- module docstring says "stub," code says otherwise; real delta is one field
- `floor_types`: `p.Function` (Interior/Exterior) resolves 31/34 (91%) -- directly parallel to `wt.function` on wall_types but not captured as `ft.function`.
- `has_embedded_sweeps` is probed at 100% ok for all three but isn't in any of their identity-item lists -- confirm whether it's computed and dropped, or genuinely unused.
- No `Function`-equivalent param exists for `roof_types`/`ceiling_types` in the probe data -- nothing to promote there.
- Suggest fixing the stale docstring while in the file.

### `dimension_types` -- largest single-domain delta among implemented domains
35 identity items already captured (accuracy, prefix/suffix, indicators, tick mark, leader length, symbol, rounding, shape, text_location/orientation, witness_line_control...). Probe shows a large cluster of populated params with no identity-item equivalent:
- **Text styling** (only location/orientation captured, not the styling itself): `Text Font`, `Text Size`, `Text Background`, `Text Offset`, `Text Offset from Leader`, `Text Offset from Symbol`, `Bold`, `Italic`, `Underline`, `Width Factor`
- **Line weights**: `Line Weight` (main), `Leader Line Weight`, `Tick Mark Line Weight`
- **Leader config**: `Leader Arrowhead`, `Leader Arrowhead Line Weight`, `Leader Tick Mark`, `Leader Type`, `Show Leader When Text Moves`
- **Witness lines** (only `witness_line_control` captured): `Witness Line Extension`, `Witness Line Gap to Element`, `Witness Line Length`, `Witness Line Tick Mark`
- **Equality dimensions**: `Equality Text`, `Equality Witness Display`
- **Alternate units**: `Alternate Units`, `Alternate Units Prefix`, `Alternate Units Suffix`
- **Radial/centerline**: `Centerline Pattern`, `Centerline Symbol`, `Centerline Tick Mark`, `Interior Tick Mark`, `Interior Tick Mark Display`
- **Behavioral**: `Dimension String Type` (Continuous/Baseline/...), `Read Convention`, `Rotate with Component`, `Suppress Spaces`, `Color` (line color), `Coordinate Base`, `Elevation Base`, `Station Indicator`, `Include Station`, `Show Opening Height`

### `units` -- 5 per-spec fields captured (`accuracy`, `rounding_method`, `spec`, `symbol_type_id`, `unit_type_id`)
Probe shows 6 boolean formatting flags resolving at 100% (450/450) per unit spec, none captured:
- `use_default`, `use_digit_grouping`, `use_plus_prefix`, `suppress_leading_zeros`, `suppress_spaces`, `suppress_trailing_zeros`
Plus 3 document-wide (not per-spec) settings, also uncaptured: `decimal_symbol`, `digit_grouping_amount`, `digit_grouping_symbol`.

### `object_styles` -- 11 identity items captured
Delta from probe:
- `can_add_subcategory`, `has_material_quantities`, `is_cuttable` (category capability flags)
- `parent_name` (parent category -- hierarchy signal, not currently captured)
- `tab` (Model/Annotation/Analytical classification -- currently not captured)

### `text_types` -- 13 identity items captured, close to complete
Delta: `Leader Arrowhead` (which arrowhead symbol text leaders use) -- everything else in the probe matches an existing identity item.

### `line_styles` -- 9 identity items captured
Delta: `parent_cat.id`/`parent_cat.name` (parent category of the line-style's owning category) -- not currently captured; would add hierarchy context alongside the existing category color/weight/pattern fields.

### `loaded_family_types` -- 9 identity items captured, reflection-only domain (no static param loop)
Delta from reflection sweep:
- `StructuralMaterialType`
- `IsActive` (whether this is the currently active type)
- `CanHaveStructuralSection` / `HasThermalProperties` (capability flags -- likely N/A for most non-structural families, but resolves cleanly where applicable)

### `materials` -- 12 identity items captured (name, class, 10 graphics sig keys)
- `Keynote` resolves for 20/362 (~6%) -- low but real; ties to the `material.keynote` gap already flagged in your notes as a known extraction issue.
- Thermal/structural asset id+name remain low-resolution (~10%) per the crosswalk data already reviewed -- not a new finding, just confirming it shows up here too.
- No new high-value delta beyond what's already flagged; this domain's gap is a crosswalk-resolution problem, not a missing-field problem.

### `arrowheads` -- 13 identity items captured
No meaningful delta. Every populated probe param maps to an existing identity item.

### `fill_patterns` -- 5 identity items captured
Minor delta: `is_solid` (explicit solid-fill boolean) isn't captured directly, though `name` (`<Solid fill>`) already conveys it indirectly. Low priority.

### `line_patterns` -- 6 identity items captured
Minor delta: `is_solid` flag, same as fill_patterns. Low priority.

### `phases` -- 2 identity items per phase (`seq`, `name`)
Matches the probe's populated fields (`phase.name`, `phase.uid`) essentially 1:1. No meaningful delta.

### `phase_filters` -- name + 4x per-status presentation_id (built dynamically in a loop)
Already exhaustive against the 4 fixed `ElementOnPhaseStatus` values. No delta found.

### `view_category_overrides` (annotation + model) -- hash-based (`override_properties_hash`, `baseline_sig_hash`)
Probe's raw per-category delta/effective fields (`cut_line_color`, `cut_line_pattern`, `cut_line_weight`, `halftone`, `projection_line_*`, `transparency`) appear to be exactly what feeds the existing hash. No missing raw field found -- flagging only that the hash means governance narrative can detect "these diverge" but not cite *which* setting diverges without decoding the hash inputs.

### `view_filter_definitions` -- 5 identity items, hash-based (`def_hash`)
Probe's `rule_types` (e.g. `FilterDoubleRule`) doesn't have an obvious standalone identity item -- likely folded into `def_hash`/`rule_sig_hash` already. No confirmed gap, flagged for verification only.

### `view_filter_applications_view_templates` -- 3 identity items, hash-based (`stack_def_hash`)
Same pattern as above: probe's raw per-filter override graphic settings (`ogs.cut_line_color`, `ogs.halftone`, `ogs.transparency`, etc.) are likely already inputs to `stack_def_hash`. No confirmed gap.

### `view_templates` -- 6 identity items, hash-based (`def_hash`, `category_overrides_def_hash`)
Same hash pattern, but at much larger scale -- the probe shows ~35 individually-named, high-resolution view-template settings (`Detail Level`, `Discipline`, `Phase Filter`, `Visual Style`, `Far Clipping`, `Depth Clipping`, `Sun Path`, `Underlay Orientation`, `Parts Visibility`, `Color Scheme Location`, `View Classification`, `Scale`, ...) that all currently collapse into `def_hash`. Worth a scoping conversation: is convergence-detection-without-attribution acceptable for this domain long-term, or does governance narrative eventually want to name *which* setting diverges between two templates? If the latter, this is the domain where breaking the hash open would pay off most.

---

## Cross-cutting note
Several domains (`view_category_overrides`, `view_filter_definitions`,
`view_filter_applications_view_templates`, `view_templates`) intentionally
fold many raw settings into a single hash rather than exposing them as
individual identity items. That's a legitimate design choice for
convergence detection, but it means the delta above isn't really "missing
fields" for those four -- it's "fields that exist only inside a hash,
not as named, individually-queryable identity items." Worth deciding
per-domain whether that's the permanent design or a future breakout
candidate, separately from the domains above that are genuinely missing
data (`identity`, `phase_graphics`, `dimension_types`, `units`,
`worksets`, `browser_organization`).
