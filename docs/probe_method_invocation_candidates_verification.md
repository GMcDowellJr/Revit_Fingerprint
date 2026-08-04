# Probe Method Invocation Candidates — Step 0 Verification (Findings Only)

This is a findings-only pass over `method_invocation_candidates.csv` (52 rows), the
name/return-type-shape filtered shortlist of methods that might be worth invoking during the
probe reflection sweep in a **later** PR. It does not change any code. It closes the gap the
original filtering left open: arity, staticness, return-type confirmation, mutation/side-effect
behavior, and document-context requirements per method.

The annotated CSV is `docs/method_invocation_candidates_annotated.csv` (all 52 original columns
plus `arity`, `is_static`, `return_type_confirmed`, `mutates_state`, `notes`).

## All 52 rows verified against ground-truth cached doc pages

Every row's `source_url` points at `www.revitapidocs.com`, which is blocked by this session's
outbound egress policy (the proxy's CONNECT tunnel returns a 403 policy denial for that host
specifically). An initial pass therefore relied on WebSearch corroboration plus trained Revit API
knowledge, with several rows flagged moderate-confidence or excluded on suspected mismatches.

That gap has since been closed. A local `cache/` directory (hash-named `.htm` + `.meta.json`
sidecar pairs, populated by whatever tool built `raw_index.json`/`graph_2025.json` for this
project) exists outside this sandbox; the `.meta.json` sidecar's `url` field is the hash→URL
connector (confirmed: hashing the bare URL under SHA-256/MD5 with several normalizations does
*not* reproduce the cache filenames, so the mapping only exists via the sidecar, not by
recomputation). A locally-run extraction script matched all 52 `source_url` values against that
index — **all 52 resolved (0 misses)** — and dumped each cached page to plain text. That combined
dump (`matched_52_pages.json`) was parsed both programmatically (regex over the C# `Syntax` block)
and by direct reading of all 52 entries, cross-checked against each other. **Every row in the
annotated CSV now reflects the actual cached RevitAPI 2025 documentation page** (RevitAPI.dll
Version 25.0.0.0), not inference — arity, staticness, and return type are all directly transcribed
from the page's C# signature line; `notes` includes the exact signature, documented exceptions, and
Remarks text for each row.

## Corrections the ground-truth pass made to the earlier (inference-based) pass

Five rows changed from the first pass; two of those change which methods count as "clean":

- **`FamilySymbol.GetFamilyPointLocations` — moves from excluded to clean.** The earlier pass
  inferred a `Reference reference` parameter from a WebSearch snippet describing the method's
  *purpose*, not its signature. Ground truth: `public IList<FamilyPointLocation>
  GetFamilyPointLocations ()` — zero-arg.
- **`View.GetDirectContext3DHandleOverrides` — moves from excluded to clean.** The earlier pass
  flagged a suspected return-type mismatch (WebSearch surfaced a same-namespace class named
  `DirectContext3DHandleSettings` and no exact hit for "DirectContext3DHandleOverrides", making the
  CSV's own evidence field look wrong). Ground truth: `public DirectContext3DHandleOverrides
  GetDirectContext3DHandleOverrides ()` — the CSV's evidence was correct all along; zero-arg,
  instance.
- **`View.GetCropRegionShapeManagerForReferenceCallout` — staticness corrected (stays excluded).**
  The earlier pass had the `(Document, ElementId)` parameter list right but labeled it "instance"
  by association with its zero-arg sibling `GetCropRegionShapeManager()`. Ground truth: `public
  static ViewCropRegionShapeManager GetCropRegionShapeManagerForReferenceCallout(Document doc,
  ElementId callout)` — static. Doesn't change its exclusion (still non-zero-arg either way).
- **Four rows upgraded from moderate- to full-confidence, no change in classification:**
  `LinePatternElement.GetSolidPatternId` (confirmed static zero-arg, no Document param),
  `View.GetPointCloudOverrides`, `View.GetReferenceElevations`, `View.GetReferenceSections`, and
  `View.GetTemporaryViewPropertiesId` (all confirmed zero-arg instance, exactly as previously
  inferred by analogy/partial search).

The task's own flagged assumption — that the four `Element.GetChangeType*` methods and the three
`View` methods (`GetCategoryOverrides`/`GetColorFillSchemeId`/`GetFilterOverrides`) are "likely
non-zero-arg from general API familiarity" — is now fully ground-truth confirmed as **half right**:
the three `View` methods are correctly non-zero-arg (each takes a single `ElementId` parameter);
the four `Element.GetChangeType*` methods are, per their actual cached pages, `public static
ChangeType GetChangeTypeX ()` — static, zero-arg factory methods for `UpdaterRegistry.AddTrigger`
that require no document and no `this`.

## Answer to the deliverable's central question: how many are clean?

**35 of 52** are confirmed zero-arg, instance, non-mutating getters — the shape safe to consider
for actual invocation in a later PR. All 35 are now ground-truth confirmed (no more
confidence-tiering needed):

`Element.GetTypeId`, `CompoundStructure.GetLayers`, `Element.GetEntitySchemaGuids`,
`Element.GetSubelements`, `FamilySymbol.GetFamilyPointLocations`,
`View.GetModelToProjectionTransforms`, `AppearanceAssetElement.GetRenderingAsset`,
`Element.GetExternalFileReference`, `Element.GetMonitoredLinkElementIds`,
`Element.GetMonitoredLocalElementIds`, `Element.GetValidTypes`, `ElementType.GetSimilarTypes`,
`FamilySymbol.GetStructuralSection`, `FamilySymbol.GetThermalProperties`,
`FillPatternElement.GetFillPattern`, `LinePatternElement.GetLinePattern`,
`ParameterFilterElement.GetCategories`, `ParameterFilterElement.GetElementFilter`,
`Subelement.GetReference`, `Subelement.GetValidTypes`, `View.GetBackground`,
`View.GetCalloutParentId`, `View.GetCropRegionShapeManager`, `View.GetDepthCueing`,
`View.GetDirectContext3DHandleOverrides`, `View.GetFilters`, `View.GetOrderedFilters`,
`View.GetPointCloudOverrides`, `View.GetPrimaryViewId`, `View.GetReferenceCallouts`,
`View.GetReferenceElevations`, `View.GetReferenceSections`, `View.GetSketchyLines`,
`View.GetTemporaryViewPropertiesId`, `View.GetViewDisplayModel`

**Excluded (17), with the ground-truth reason:**

- **Non-zero-arg (12)** — each confirmed to require at least one argument the reflection sweep has
  no way to supply from `this` alone: `BrowserOrganization.GetFolderItems(ElementId)`,
  `CompoundStructure.GetWallSweepsInfo(WallSweepType)`,
  `Element.GetExternalResourceReferenceExpanded(ExternalResourceType)`,
  `CompoundStructure.GetDeckProfileId(int)`, `CompoundStructure.GetRegionEnvelope(int)`,
  `Element.GetEntity(Schema)`, `View.GetCategoryOverrides(ElementId)`,
  `View.GetColorFillSchemeId(ElementId)`,
  `View.GetCropRegionShapeManagerForReferenceCallout(Document, ElementId)`,
  `View.GetElementOverrides(ElementId)`, `View.GetFilterOverrides(ElementId)`,
  `View.GetLinkOverrides(ElementId)`.
- **Static, zero-arg but not instance (5)** — not invocable through the current
  `_reflect_try_get(obj, member_kind, name)` per-instance model even though they take no
  arguments: the four `Element.GetChangeType*` methods and
  `LinePatternElement.GetSolidPatternId()`.

None of the 52 pages document any transaction requirement, regeneration side effect, or document
mutation anywhere in their Syntax/Parameters/Return Value/Exceptions/Remarks sections (checked by
both a full-text keyword scan for `transaction`/`modif*`/`regenerat*`/`commit` and a full manual
read of all 52 pages) — every candidate is confirmed to be a pure `Get*` read accessor, consistent
with the original name-based mutator filtering. Several rows document `ArgumentException`/
`InvalidOperationException` conditions (e.g. `View.GetElementOverrides` throws if "the element
'this View' does not belong to a project document"; `View.GetCalloutParentId` throws if "this view
is not a callout") — those are argument-validity/applicability preconditions on the target object,
not document-context requirements beyond what the probe already has open, and not mutations. Full
text of each is in the annotated CSV's `notes` column per row.

## Step 0: where the placeholder branch point lives

`tools/probes/` has **no shared reflection module** — the sweep is duplicated verbatim across
`probe_*.py` files. There are 26 `probe_*.py` files in total; 24 of them contain an identical
copy-pasted `_reflect_try_get` function. The 2 exceptions: `probe_dimension_types.py` uses a
different, narrower targeted-keyword helper `_reflect_members()` instead of the generic sweep, and
`probe_roof_type_import.py` has no reflection sweep at all — it's a data-import script, not a
probe (confirmed via `grep -L "_reflect_try_get" probe_*.py`).

In every file that has it, the function is named `_reflect_try_get(obj, member_kind, name)`. A
representative instance:

**`tools/probes/probe_identity.py:558-565`**

```python
def _reflect_try_get(obj, member_kind, name):
    if member_kind == "method":
        # SAFETY: never invoke a reflection-discovered method. Revit API
        # methods can have side effects (printing, export, regenerate,
        # delete, transaction commits, ...) and there is no reliable way to
        # tell a safe zero-arg query method from a side-effecting one by
        # name alone. Record that the method exists without calling it.
        return (True, "<method not invoked>", None)
    try:
        v = getattr(obj, name)
    except Exception as ex:
        return (False, None, "{}: {}".format(type(ex).__name__, ex))
    return (True, v, None)
```

The branch is `if member_kind == "method":` at line 559, returning the placeholder at line 565.
The candidate member list itself (what counts as a reflectable zero-arg "method" in the first
place — public, non-special-name, zero-`GetParameters()`, not a `get_`/`set_`/`add_`/`remove_`
accessor) is filtered upstream in the sibling function `_reflect_member_names(obj)`
(`probe_identity.py:511-556`), specifically the `t.GetMethods()` loop at lines 532-546.

**No allowlist/denylist mechanism exists anywhere in this code path today** — grepped
`tools/probes/*.py` for `allowlist|denylist|allow_list|deny_list|safe_methods|ALLOWED_METHODS`
and got zero hits. The comment at lines 560-564 explains why: the sweep's author considered and
explicitly rejected name-based safety heuristics ("there is no reliable way to tell a safe
zero-arg query method from a side-effecting one by name alone"). A later implementation PR needs
to build the allowlist mechanism from scratch, not extend an existing (currently empty) one — and
since the function is duplicated per-file rather than centralized, that PR should also decide
whether to factor `_reflect_try_get` (and `_reflect_member_names`) into a shared module before
wiring in a policy-driven allowlist, or patch all 24 call sites in place.

Because the function is copy-pasted per probe file, the exact line numbers differ slightly by
file (each file has different content above the reflection-sweep section); confirmed identical
`_reflect_try_get` definitions exist in: `probe_arrowheads.py`, `probe_browser_organization.py`,
`probe_ceiling_types.py`, `probe_fill_patterns.py`, `probe_floor_types.py`, `probe_identity.py`,
`probe_line_patterns.py`, `probe_line_styles.py`, `probe_loaded_family_types.py`,
`probe_materials.py`, `probe_object_styles.py`, `probe_phase_filters.py`,
`probe_phase_graphics.py`, `probe_phases.py`, `probe_roof_types.py`, `probe_text_types.py`,
`probe_units.py`, `probe_view_category_overrides.py`, `probe_view_filter_applications.py`,
`probe_view_filter_definitions.py`, `probe_view_templates.py`, `probe_views.py`,
`probe_wall_types.py`, `probe_worksets.py`.

## Out of scope, confirmed honored

- No file under `tools/probes/` was modified (verified: `git status` shows only the two `docs/`
  files added by this pass).
- No Revit API method was invoked against a live session — all verification was documentation
  research (reading the actual cached RevitAPI 2025 documentation pages, resolved locally by the
  user via a `cache/`-directory extraction script), never execution.
- No new domains or classes were added beyond the 52 rows already in
  `method_invocation_candidates.csv`; incidental findings about other reference-shaped methods
  (if any) are noted per-row in the CSV's `notes` column, not added as new rows.
- No decision was made about which methods to actually wire up — that's explicitly the follow-on
  PR's job once these findings are reviewed. The 35/52 breakdown above is a *candidate set* scoped
  by arity/staticness/mutation only, not a final invocation decision.
