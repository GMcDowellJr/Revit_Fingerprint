# RevitLookup Descriptor → Fingerprint Domain Map

This document maps each Fingerprint extraction domain to the RevitLookup
descriptor files that cover the same Revit API surface.

**How to use:** When auditing or extending an extractor, open the listed
descriptor file(s) in `reference/revit_lookup/Descriptors/` and compare
what API calls RevitLookup makes vs what the extractor calls.

Remember: RevitLookup calls everything for display. The extractor calls
only configuration-stable signals. The traversal is reference; the
selection of calls is your judgment.

---

## Direct descriptor coverage

These domains have a descriptor that directly covers the primary type.

| Fingerprint Domain | RevitLookup Descriptor(s) | Notes |
|---|---|---|
| `arrowheads` | `ElementTypeDescriptor.cs` | Arrowheads are `ElementType` instances filtered by param presence. Check param enumeration pattern. |
| `text_types` | `TextNoteTypeDescriptor.cs` (if exists) or `ElementTypeDescriptor.cs` | `TextNoteType` is a subclass of `ElementType`. Look for leader arrowhead param resolution pattern. |
| `line_patterns` | `LinePatternElementDescriptor.cs` | Check segment enumeration: `GetSegments()` call and `LinePatternSegmentType` enum values. |
| `line_styles` | `GraphicsStyleDescriptor.cs` | `LineStyle` surfaces as `GraphicsStyle`. Check `GraphicsStyleType` discrimination and `GetGraphicsStyleCategory()` traversal. |
| `fill_patterns_drafting` / `fill_patterns_model` | `FillPatternElementDescriptor.cs` | Check `FillPattern.GetFillGrid()` enumeration and `IsSolidFill` guard. Target discrimination is `FillPatternTarget` enum. |
| `object_styles_model` / `object_styles_annotation` / `object_styles_analytical` / `object_styles_imported` | `CategoryDescriptor.cs` | Object styles surface as `Category`. Check `CategoryType` discrimination, `GetLineWeight()`, `GetLinePatternId()`, `Material` access. |
| `dimension_types_linear` | `DimensionTypeDescriptor.cs` | Check `DimensionStyleType.Linear` discrimination and which params are read vs computed. |
| `dimension_types_angular` | `DimensionTypeDescriptor.cs` | `DimensionStyleType.Angular` |
| `dimension_types_radial` | `DimensionTypeDescriptor.cs` | `DimensionStyleType.Radial` |
| `dimension_types_diameter` | `DimensionTypeDescriptor.cs` | `DimensionStyleType.Diameter` |
| `dimension_types_spot_elevation` | `DimensionTypeDescriptor.cs` | `DimensionStyleType.SpotElevation` |
| `dimension_types_spot_coordinate` | `DimensionTypeDescriptor.cs` | `DimensionStyleType.SpotCoordinate` |
| `dimension_types_spot_slope` | `DimensionTypeDescriptor.cs` | `DimensionStyleType.SpotSlope` |
| `phases` | `PhaseDescriptor.cs` (if exists) or `ElementDescriptor.cs` | Phases are `Phase` elements. Check name/sequence access. |
| `phase_filters` | `ElementDescriptor.cs` | `PhaseFilter` elements. Check parameter enumeration. |
| `phase_graphics` | `ElementDescriptor.cs` | Disabled domain (D-013). Still useful to confirm which API surfaces are accessible. |
| `units` | `ForgeTypeIdDescriptor.cs` + document-level extensions | Check `UnitUtils.GetAllDisciplines()`, `Document.GetUnits()`, `FormatOptions` access. |
| `identity` | `DocumentDescriptor.cs` | UIDs, project info, file path. Confirm which `ProjectInfo` params RevitLookup exposes. |

---

## Indirect / compound coverage

These domains don't map to a single descriptor — they require combining
multiple descriptors or traversing through document-level collections.

| Fingerprint Domain | Relevant Descriptor(s) | Traversal notes |
|---|---|---|
| `view_filter_definitions` | `ParameterFilterElementDescriptor.cs` | Filter rules: check `GetElementFilter()` → rule enumeration. `FilterRule`, `FilterStringRule`, `FilterValueRule` subtypes each have separate handling in the descriptor. |
| `view_filter_applications_view_templates` | `ViewDescriptor.cs` | Filter application lives on `View`. Check `GetFilters()`, `GetFilterOverrides()`, `GetFilterVisibility()` call sequence. |
| `view_category_overrides_model` / `view_category_overrides_annotation` | `ViewDescriptor.cs` + `CategoryDescriptor.cs` | `View.GetCategoryHidden()`, `View.GetOverrideGraphicSettings()` per category. Check `OverrideGraphicSettings` descriptor for what fields it exposes. |
| `view_templates_*` (all 5) | `ViewDescriptor.cs` | Template-specific: `View.IsTemplate`, `View.GetNonControlledTemplateParameterIds()`, `ViewTemplateId`. Check how RevitLookup surfaces V/G settings. |

---

## Future domains — descriptor availability

System family fingerprinting work (compound layers):

| Planned Domain | Primary Descriptor | Key methods to check |
|---|---|---|
| Wall type layers | `WallTypeDescriptor.cs` | `WallType.Kind` guard before `GetCompoundStructure()`. Check `WallKind` discrimination. |
| Compound structure (all) | `CompoundStructureDescriptor.cs` | `GetLayers()`, `GetLayerFunction()`, `GetLayerWidth()`, `GetMaterialId()`, `GetCoreBoundaryLayerIndex()`, `IsCoreLayer()`, `GetDeckEmbeddingType()`, `GetDeckProfileId()`. All added in 2026.0.0. |
| Floor type layers | `FloorTypeDescriptor.cs` (check if exists) | Same `CompoundStructure` traversal. |
| Roof type layers | `RoofTypeDescriptor.cs` (check if exists) | Same `CompoundStructure` traversal. Check `RoofType.Kind` if equivalent discrimination exists. |
| Ceiling type layers | Check `ElementTypeDescriptor.cs` | `CeilingType` may not have its own descriptor — check DescriptorsMap. |

---

## Descriptor files that don't map to current domains
## (potential future domain signals)

These descriptors cover Revit types we don't currently fingerprint.
Listed here as a prompt for future domain discovery.

| Descriptor | Revit Type | Potential signal |
|---|---|---|
| `GlobalParameterDescriptor.cs` | `GlobalParameter` | Global parameter inventory — governance-relevant if org uses them for standards |
| `AssemblyInstanceDescriptor.cs` | `AssemblyInstance` | Assembly usage patterns |
| `GridDescriptor.cs` (if exists) | `Grid` | Grid naming conventions |
| `LevelDescriptor.cs` (if exists) | `Level` | Level naming and elevation patterns |
| `FamilyDescriptor.cs` | `Family` | Loadable family inventory — relevant to Detail Intelligence |
| `FamilySymbolDescriptor.cs` | `FamilySymbol` | Symbol parameter defaults |
| `ViewDescriptor.cs` | `View` | Already partially used — check for any missed V/G surfaces |
| `ParameterFilterElementDescriptor.cs` | `ParameterFilterElement` | Already covered — but check for rule-depth completeness |

---

## How to read a descriptor against an extractor

1. Find the descriptor for the domain's primary type
2. Look at `RegisterExtensions()` or equivalent — lists every method/property called
3. Look for guard conditions (`if element is null`, type checks, version guards)
4. Compare to what the extractor's `extract()` function calls
5. Flag any methods RevitLookup calls that the extractor doesn't — these are
   potential missing signals or edge-case handlers

Pay particular attention to:
- **Null guards** before calling methods that can return null (e.g. `GetCompoundStructure()` on curtain walls)
- **Version guards** (`#if REVIT2025` etc.) — signals that an API changed between versions
- **Static Utils calls** — RevitLookup 2027 explicitly linked Utils methods to their types;
  these are often missed in extractors because they're not on the type itself
