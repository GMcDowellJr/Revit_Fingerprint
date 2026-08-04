# Probe Method Invocation Candidates — Step 0 Verification (Findings Only)

This is a findings-only pass over `method_invocation_candidates.csv` (52 rows), the
name/return-type-shape filtered shortlist of methods that might be worth invoking during the
probe reflection sweep in a **later** PR. It does not change any code. It closes the gap the
original filtering left open: arity, staticness, return-type confirmation, mutation/side-effect
behavior, and document-context requirements per method.

The annotated CSV is `docs/method_invocation_candidates_annotated.csv` (all 52 original columns
plus `arity`, `is_static`, `return_type_confirmed`, `mutates_state`, `notes`).

## Environment constraint: source pages could not be fetched live

Every row's `source_url` points at `www.revitapidocs.com`. That host is blocked by this session's
outbound egress policy — the proxy's CONNECT tunnel returns a 403 policy denial for it
specifically (confirmed via `$HTTPS_PROXY/__agentproxy/status`'s `recentRelayFailures`:
`"connect_rejected" ... "gateway answered 403 to CONNECT (policy denial or upstream failure)"
host: "www.revitapidocs.com:443"`). No local `cache/` directory with hashed source-url filenames
exists anywhere in this environment (checked repo root and full filesystem depth-3/4) — the task's
described cache-resolution fallback isn't available here either.

General WebFetch to other documentation mirrors (thebuildingcoder.typepad.com,
help.autodesk.com) was also blocked; only a narrow allowlist of developer-infrastructure hosts
(npm, PyPI, crates.io, Go proxy, GitHub raw, Anthropic) passes through. WebSearch, however, does
work (it isn't proxied the same way), so verification for each row was done by:

1. Cross-referencing the method against trained knowledge of the Revit .NET API (a long-stable,
   heavily-documented API well represented in training data), and
2. Independently corroborating via WebSearch wherever a search returned a specific, quotable
   description of the method's signature (`revitapidocs.com` page titles/snippets, Autodesk
   Knowledge Network pages, Autodesk Community threads, AEC DevBlog / Building Coder posts).

Every row's `notes` column states this constraint explicitly per the task's own escape hatch
("If a source page can't be reached or resolved, say so explicitly in notes rather than leaving
the row incomplete"). Rows where WebSearch could not independently corroborate the specific
signature are marked **moderate confidence** in `notes` and are called out below; everything else
is corroborated by at least one specific search result, not knowledge alone.

## Key correction to the CSV's own working assumptions

The task asked me to confirm or correct the CSV's flag that four `Element.GetChangeType*` rows
and three `View` rows (`GetCategoryOverrides`, `GetColorFillSchemeId`, `GetFilterOverrides`) are
"likely non-zero-arg from general API familiarity." The result splits down the middle:

- **The three `View` rows: the flag is correct.** `GetCategoryOverrides(ElementId categoryId)`,
  `GetColorFillSchemeId(ElementId categoryId)`, and `GetFilterOverrides(ElementId filterId)` all
  require an id argument — confirmed via WebSearch quotes of each method's description. These are
  per-category/per-filter lookups into a per-view override table, not `this`-only getters.

- **The four `Element.GetChangeType*` rows: the flag is wrong.** `GetChangeTypeAny()`,
  `GetChangeTypeElementAddition()`, `GetChangeTypeElementDeletion()`, and `GetChangeTypeGeometry()`
  are **static, zero-argument factory methods** that build `ChangeType` flag values for
  `UpdaterRegistry.AddTrigger(...)` (the IUpdater / Dynamic Model Update framework), e.g.
  `UpdaterRegistry.AddTrigger(id, filter, Element.GetChangeTypeGeometry())`. Confirmed via two
  independent WebSearch sources (AEC DevBlog's DMU how-to, an Autodesk Community thread using
  `GetChangeTypeElementAddition()` with no arguments). Being static, they're called on the
  *class* (`Element.GetChangeTypeGeometry()`), never on a sampled instance — so even though they
  are technically zero-arg, they don't fit the reflection sweep's per-object invocation model at
  all (there is no `this` to invoke them against), and they touch no document state whatsoever.

## Answer to the deliverable's central question: how many are clean?

**33 of 52** are zero-arg, instance, non-mutating getters — the shape safe to consider for actual
invocation in a later PR. Of those 33, **29 are high-confidence** (independently corroborated via
WebSearch or unambiguous, well-known API surface) and **4 are moderate-confidence** (same shape by
strong analogy or a partial search hit, but not independently confirmed against the live doc page
— flagged individually below and in the CSV's `notes`).

**High-confidence clean (29):**
`Element.GetTypeId`, `CompoundStructure.GetLayers`, `Element.GetEntitySchemaGuids`,
`Element.GetSubelements`, `View.GetModelToProjectionTransforms`,
`AppearanceAssetElement.GetRenderingAsset`, `Element.GetExternalFileReference`,
`Element.GetMonitoredLinkElementIds`, `Element.GetMonitoredLocalElementIds`,
`Element.GetValidTypes`, `ElementType.GetSimilarTypes`, `FamilySymbol.GetStructuralSection`,
`FamilySymbol.GetThermalProperties`, `FillPatternElement.GetFillPattern`,
`LinePatternElement.GetLinePattern`, `ParameterFilterElement.GetCategories`,
`ParameterFilterElement.GetElementFilter`, `Subelement.GetReference`,
`Subelement.GetValidTypes`, `View.GetBackground`, `View.GetCalloutParentId`,
`View.GetCropRegionShapeManager`, `View.GetDepthCueing`, `View.GetFilters`,
`View.GetOrderedFilters`, `View.GetPrimaryViewId`, `View.GetReferenceCallouts`,
`View.GetSketchyLines`, `View.GetViewDisplayModel`

**Moderate-confidence clean (4) — same shape, but spot-check the live doc page before wiring up:**
`View.GetPointCloudOverrides` (WebSearch describes it as a zero-arg per-view container getter,
matching the `GetBackground`/`GetDepthCueing` family, but wasn't independently confirmed with a
full signature quote), `View.GetReferenceElevations` and `View.GetReferenceSections` (inferred by
direct analogy to the confirmed `View.GetReferenceCallouts` — same API family, same era, same
shape — but not independently searched), and `View.GetTemporaryViewPropertiesId` (inferred from
its confirmed sibling state-control methods `EnableTemporaryViewPropertiesMode` /
`DisableTemporaryViewPropertiesMode` / `IsTemporaryViewPropertiesModeEnabled`, not directly
searched).

**Excluded from the clean count (19), with reasons:**
- **Non-zero-arg (14):** `BrowserOrganization.GetFolderItems`, `CompoundStructure.GetWallSweepsInfo`,
  `Element.GetExternalResourceReferenceExpanded`, `FamilySymbol.GetFamilyPointLocations`,
  `CompoundStructure.GetDeckProfileId`, `CompoundStructure.GetRegionEnvelope`, `Element.GetEntity`,
  `View.GetCategoryOverrides`, `View.GetColorFillSchemeId`,
  `View.GetCropRegionShapeManagerForReferenceCallout`, `View.GetElementOverrides`,
  `View.GetFilterOverrides`, `View.GetLinkOverrides` — each confirmed to require at least one
  argument the reflection sweep has no way to supply from `this` alone.
- **Static, not instance (4):** the four `Element.GetChangeType*` methods (see correction above)
  and `LinePatternElement.GetSolidPatternId` — static methods aren't invocable through the
  current `_reflect_try_get(obj, member_kind, name)` per-instance model even though three of the
  five are zero-arg.
- **Uncertain/flagged for exclusion (1):** `View.GetDirectContext3DHandleOverrides` — its
  declared return type in the CSV's `evidence` field doesn't match any class name WebSearch
  surfaced in the actual `Autodesk.Revit.DB.DirectContext3D` namespace (the real class is
  `DirectContext3DHandleSettings`, not `DirectContext3DHandleOverrides`), and its parameter list
  could not be confirmed. Recommend excluding until directly verified against the live doc page.

None of the 52 rows have documented mutation, transaction requirements, or other side effects —
every candidate is a `Get*` read accessor by both name and (where corroborated) documented
behavior. None require document context beyond what the probe already has open; the 14 non-zero-arg
rows need a same-document *value* (a category id, filter id, layer index, etc.) as an argument,
which is a harness/argument-sourcing problem for the follow-on PR to solve, not a
different-document problem.

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
wiring in a policy-driven allowlist, or patch all 22 call sites in place.

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

- No file under `tools/probes/` was modified (verified: `git status` shows only the two new
  `docs/` files added by this pass).
- No Revit API method was invoked against a live session — all verification was documentation
  research (WebSearch + trained API knowledge), never execution.
- No new domains or classes were added beyond the 52 rows already in
  `method_invocation_candidates.csv`; incidental findings about other reference-shaped methods
  (if any) are noted per-row in the CSV's `notes` column, not added as new rows.
- No decision was made about which methods to actually wire up — that's explicitly the follow-on
  PR's job once these findings are reviewed. The 33/29+4 breakdown above is a *candidate set*
  scoped by arity/staticness/mutation only, not a final invocation decision.
