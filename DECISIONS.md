# DECISIONS

This document records **architectural and semantic decisions** that materially affect
system behavior, evolution, or constraints.

It exists to:
- prevent re-litigation of settled questions
- make intent explicit
- preserve rationale when context is lost

This is **not** a log of implementation details.
If a decision changes hashes, identity rules, or system structure, it belongs here.

---

## Decision Log

### D-001 — Behavior-First Fingerprinting
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
Fingerprints represent **behavior**, not UI presentation or naming.

**Rationale**  
Names, ordering in UI, and cosmetic properties change frequently and are not reliable
signals of functional intent. Behavioral properties are the only stable basis for
standards governance and drift detection.

**Consequences**
- Names are metadata only unless explicitly stated otherwise
- Hash changes are meaningful signals, not noise

---

### D-002 — Deterministic, Auditable Hashes
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
All hashes must be:
- deterministic
- stable across sessions
- derived from an auditable preimage

**Rationale**  
Hashes without explainability cannot be trusted or debugged.
Auditability is mandatory for governance and standards enforcement.

**Consequences**
- `record_rows` is mandatory for record-based domains
- Debug markers must be explicit when data is unreadable

---

### D-003 — `record_rows` as Canonical Explainability
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
`record_rows` is the canonical explainability structure for all record-based domains.

**Rationale**  
Lists of names or counts are insufficient for traceability.
A stable `(record_key → sig_hash)` mapping enables diffs, audits, and downstream tooling.

**Consequences**
- Every record-based domain must emit `record_rows`
- Global hashes are always derived from per-record hashes

---

### D-004 — UniqueId Usage Is Restricted
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
`UniqueId` is used **only** where element-backed identity is meaningful and persistent.

**Rationale**  
Blind use of `UniqueId` causes unnecessary churn and false drift.
Some domains are definition-based, not identity-based.

**Consequences**
- Styles, patterns, and definitions avoid `UniqueId` unless identity matters
- Views, view templates, filters, phases may use `UniqueId`

---

### D-005 — Fail-Soft Is Mandatory
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
Unreadable or inaccessible data must never cause silent collapse.

**Rationale**  
Silence hides risk. Explicit failure markers preserve state distinctions and auditability.

**Consequences**
- `<Unreadable>` / `<None>` markers are emitted instead of skipping data
- Errors propagate into hashes intentionally

---

### D-006 — Ordering Rules Are Explicit Per Domain
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
Ordering sensitivity is a **domain decision**, not an implementation accident.

**Rationale**  
Some structures (e.g. view filter stacks) are order-dependent; others are not.
Implicit ordering leads to accidental semantic changes.

**Consequences**
- Order-sensitive structures preserve order in signatures
- Order-insensitive structures are sorted before hashing
- Each domain must state its ordering behavior

---

### D-007 — Global vs Contextual Domain Split
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
Globally defined entities are fingerprinted once and referenced elsewhere.

**Rationale**  
Duplication of global definitions inside views/templates causes inconsistency and waste.

**Consequences**
- Filters, phases, phase filters, phase graphics are global domains
- Views and view templates reference global domains by identity + hash

---

### D-008 — View Templates Are Behavioral, Not Nominal
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
View templates are fingerprinted by **controlled behavior**, not by name or existence.

**Rationale**  
Two templates with the same name can behave differently.
Name-only fingerprints are misleading and unsafe.

**Consequences**
- Template hashes are derived from controlled parameters, filters, phase settings, etc.
- Names are metadata only

---

### D-009 — Views Compose Templates + Deltas
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
A view’s effective behavior is:
- template behavior (if assigned)
- plus view-specific deltas not controlled by the template

**Rationale**  
This mirrors actual Revit behavior and avoids double-counting settings.

**Consequences**
- Views with templates do not re-hash template-controlled settings
- Views without templates hash full allowlisted behavior

---

### D-010 — Phase Names in Behavioral Hashes
**Status:** Revised
**Date:** 2025-12-17 (revised 2026-01-29)

**Decision**
Phase names ARE included in behavioral hashes for cross-project comparability.
Phase UniqueId is used for identity/debug only (document-specific).

**Rationale**
UniqueIds are document-specific and cannot be compared across projects.
Phase names provide the semantic link needed for cross-project drift detection.
This supersedes the original decision that treated names as metadata-only.

**Consequences**
- Phase name changes ARE considered behavioral changes
- Cross-project comparison uses phase names as the comparability key
- UniqueId remains for within-document identity only

---

### D-011 — Domain-Driven Architecture
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
The system is structured into:
- Core (pure Python)
- Domain extractors (Revit-aware)
- Context builder
- Host-specific runners

**Rationale**  
This enables refactoring, selective execution, and future portability.

**Consequences**
- Domains do not import each other
- Cross-domain data flows only through context

---

### D-012 — Markdown Portability Rule
**Status:** Accepted  
**Date:** 2025-12-17

**Decision**  
Nested fenced code blocks are forbidden in documentation.

**Rationale**  
GitHub Mobile, Obsidian, and chat renderers handle nested fences inconsistently.

**Consequences**
- Fenced blocks are used only for whole-file examples
- Indented blocks are used for schemas and inline snippets

---

## D-013 — Phase Graphics Domain Disabled (API Limitation)

**Status:** Accepted  
**Date:** 2025-12-18  
**Scope:** `phase_graphics` domain

### Context
The Revit UI exposes *Phase Graphic Overrides* (per-status line styles, colors, patterns).
During implementation, it was unclear whether these overrides were accessible via the
public Revit API.

A targeted API probe in Revit 2025 (and consistent with behavior back to 2021) confirmed:
- `PhaseFilter.GetPhaseStatusPresentation` **is available**
- No API access exists for:
  - per-status graphic overrides
  - line style assignments
  - color / pattern overrides

Earlier attempts that surfaced `<Unreadable>` values were calling non-existent or unsupported
API members and did not represent real accessible data.

### Decision
The `phase_graphics` domain is **intentionally disabled** at runtime.

The system will not emit stub hashes or placeholder signatures for data that cannot be
reliably extracted via the API.

### Rationale
- Avoids misleading fingerprints and false confidence
- Keeps all emitted data verifiable and reproducible
- Maintains a clean separation between:
  - `phase_filters` → presentation (API-supported)
  - `phase_graphics` → not available via API

### Consequences
- Phase graphic overrides are not fingerprinted
- Downstream consumers must not assume graphic override coverage
- Future enablement requires a documented, supported API path or non-API extraction strategy

### Revisit Criteria
Revisit this decision if:
- Autodesk exposes phase graphic overrides via the public API
- A sanctioned non-API extraction mechanism is introduced and approved

---

## D-014 — Hash Mode Migration Timeline

**Status:** Accepted
**Date:** 2026-02-07

### Context
The system computes two hashes for every domain: `hash` (legacy pipe-delimited with sentinel
literals) and `hash_v2` (record.v2 identity-basis, no sentinel literals). The `REVIT_FINGERPRINT_HASH_MODE`
environment variable selects which is authoritative. Legacy remains the default.

All 14 active domains now compute both hashes. The canonical evidence selector rollout
(PRs #106–#119) established policy-driven join-key composition for all domains, making
semantic mode viable.

### Decision
The legacy hash mode will be maintained as default until the following criteria are met:

1. A comparison run across the current model population confirms `hash` and `hash_v2` produce
   equivalent governance signals (same drift/deviation detection).
2. All downstream consumers (if any) have been notified of the format change.
3. The comparison results are documented in this repository.

Once criteria are satisfied, `semantic` becomes the default and `legacy` enters a deprecation
period of at least one extraction cycle before removal.

### Rationale
- Dual computation adds complexity to every domain but is necessary for safe migration.
- Setting explicit criteria prevents indefinite deferral while protecting against premature switching.
- The comparison run is the minimum evidence required for confidence.

### Consequences
- Legacy mode remains default until criteria are met.
- No new domains should add legacy hash support — new domains use semantic mode only.
- The comparison run becomes a blocking prerequisite for the switch.

---


## D-014 — Hash Mode Migration Timeline (COMPLETED)

**Completion Date:** 2026-02-10  
**PR:** #XXX

Legacy hash infrastructure removed. All domains now use semantic (record.v2) hashing exclusively.
Comparison run validated equivalence across 50+ sample files on 2026-02-09.

No downstream breaking changes: contract schema already supported semantic mode.

---

## D-015 — Domain Family Split Architecture

**Status:** Accepted
**Date:** 2026-03-06

**Decision**
The four monolithic extractors (`object_styles`, `fill_patterns`, `dimension_types`,
`view_templates`) are split into per-partition domain files, each covering one record
class or ViewType family. The split follows a three-level hierarchy:

- **Domain family**: Named grouping (e.g., `object_styles`, `dimension_types`)
- **Domain**: Individual split file (e.g., `object_styles_model`, `dimension_types_linear`)
- **Record class**: The entity type within a domain (e.g., Model categories, Linear shapes)

**Rationale**
- Monolithic extractors mixed heterogeneous record structures, making per-class policy
  governance impractical.
- Each split domain can have its own join-key policy tailored to the record class.
- Shape discrimination moves to domain-level filtering rather than within-domain branching.
- Downstream tools and analysis pipelines can target specific record classes directly.

**Split mapping**

| Old domain | New domains |
|------------|-------------|
| `object_styles` | `object_styles_model`, `object_styles_annotation`, `object_styles_analytical`, `object_styles_imported` |
| `fill_patterns` | `fill_patterns_drafting`, `fill_patterns_model` |
| `dimension_types` | `dimension_types_linear`, `dimension_types_angular`, `dimension_types_radial`, `dimension_types_diameter`, `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`, `dimension_types_spot_slope` |
| `view_templates` | `view_templates_floor_structural_area_plans`, `view_templates_ceiling_plans`, `view_templates_elevations_sections_detail`, `view_templates_renderings_drafting`, `view_templates_schedules` |

**Shared helpers**
- `core/dimension_type_helpers.py`: Shape constants, detection, and reading helpers
- `core/vg_sig.py`: VG signature helpers for view_templates split domains

**Consequences**
- Each split domain has its own flat join-key policy (no shape_gating in new dimension_types policies — shape discrimination is done at domain-level)
- The `require_domain` dependency chain is updated to reference split domain names
- Tools and analysis configs use split domain names throughout
- No semantic change to hash values within each record class

---

## D-015 — Domain Family Architecture
**Status:** Accepted
**Date:** 2026-03-19
### Context
Several Revit API classes expose structurally heterogeneous records that were
initially extracted as single monolithic domains. As the corpus grew and governance
questions became more specific, the single-domain approach produced analytically
meaningless blended HHI scores (e.g. a single score for dimension_types mixing
Linear and SpotCoordinate types that share almost no applicable properties).
### Vocabulary
- **Domain family**: Named grouping of related domains. Policy and BI concept only —
  no code hierarchy. Defined in `policies/cross_domain_alignment_keys.json`.
- **Domain**: The extractable, analyzable unit. One extractor file, one policy entry,
  one sig_hash, one HHI score. All domains are flat peers in the runner.
- **Record class**: Within a single domain, records may fall into classes where
  different properties are applicable to identity. Routed by class discriminator.
  Implemented via `shape_gating` block in join-key policy.
- **Class discriminator**: The identity_item field whose value determines a record's
  record class.
- **Alignment keys**: Fields shared across domains within a family that governance
  expects to be consistent. Defined in `policies/cross_domain_alignment_keys.json`.
### Decision
Adopt the three-level architecture above. Revit's system family boundary is the
authoritative partition criterion for deciding when to split into separate domains
versus use a record class gate within one domain.
The shape_gating JSON key in join-key policy is retained for backward compatibility
with core/join_key_builder.py. The new vocabulary applies to prose, comments, and
documentation only — not to JSON key names.
### Affected domains in this branch
- `dimension_types` → 7 domains (linear, angular, radial, diameter, spot_elevation,
  spot_coordinate, spot_slope)
- `view_templates` → 5 domains (floor_structural_area_plans, ceiling_plans,
  elevations_sections_detail, renderings_drafting, schedules)
- `object_styles` → 4 domains (model, annotation, analytical, imported)
- `fill_patterns` → 2 domains (drafting, model)
- `arrowheads` — record class corrections only, no split
- `line_patterns` — lp.is_import added to coordination_items, no split
### Cross-domain alignment
Alignment keys — fields shared across domains within a family that governance
expects to be consistent — are defined in `policies/cross_domain_alignment_keys.json`.
Cross-domain alignment scoring is a BI/analysis concern, not an extraction concern.
Extractor changes are not required to enable cross-domain alignment analysis.
### Consequences
- All hashes from previous exports are obsolete. Full re-extraction required.
- 28 domains replace 4 monolithic extractors plus corrections to 2 others.
- `run_extract_all`, `phase1_probe_config`, `contracts/domain_identity_keys_v2.json`
  updated with new domain names.
- Power BI domain family grouping and alignment measures to be implemented separately.
- Future consolidation: separate extractor files per domain will be refactored into
  one file per domain family with internal routing. Deferred until all domains are
  validated.

---

## D-016 — View Category Overrides Scope and Category Classification
**Status:** Accepted
**Date:** 2026-03-19
### Context
View category overrides (VCO) can exist in three populations with different
governance implications:
1. Template-controlled overrides: V/G checkbox checked, override differs from
   object styles baseline. These are enforced on all views using the template.
2. Latent overrides: V/G checkbox unchecked, override set on the template.
   Not enforced but would activate if the checkbox were checked.
3. View-local overrides: overrides set directly on individual views, either
   on non-templated views or on views where the template does not control
   that category.
### Decision
Implement categories 1 and 2 as a domain family split:
- `view_category_overrides_model` (CategoryType.Model)
- `view_category_overrides_annotation` (CategoryType.Annotation)

`vco.include_controlled` is removed from VCO coordination_items. Include state is
owned by view_templates via per-tab include flags:
- `view_template.sig.include_vg_model`
- `view_template.sig.include_vg_annotation`
- `view_template.sig.include_vg_analytical`

VCO records now emit `vco.vg_tab` (`Model`/`Annotation`) and downstream tools
derive category 1 vs category 2 by joining `vco.vg_tab` to the corresponding
`view_template.sig.include_vg_<tab>` flag.

Category 3 is deferred.
Category 2 records (latent overrides) remain included because a latent override
that diverges from the standard is a governance risk: if the V/G checkbox is
later checked, the non-standard override activates silently.
### Category 3 hooks
When category 3 (view-local overrides) is implemented:
- Add `vco.context_type = "view_local"` in coordination_items
  (current records use `"template"`)
- Add `vco.view_element_id` in unknown_items for traceability
- No changes to category 1/2 records, join-key policy, or sig_hash
### Consequences
- VCO model partition depends on `object_styles_model` ctx map; annotation
  partition consumes `object_styles_annotation` ctx map when present
- VCO reads view templates directly from the Revit API — it does NOT depend on
  view_template_* domain extractors
- Include control for governance filtering is sourced from view_templates and
  joined via `vco.vg_tab` → `include_vg_<tab>` mapping
- View-local overrides (category 3) may be a large population; implement with
  record-count ceiling and non-default-only filter when deferred work begins

---

## D-017 — line_patterns Join Key Upgraded to Scale-Invariant Normalized Segments
**Status:** Accepted
**Date:** 2026-03-31
### Decision
Upgrade `line_patterns` join identity from exact segment definition hash
(`line_pattern.segments_def_hash`) to normalized segment ratio hash
(`line_pattern.segments_norm_hash`) using `line_patterns.join_key.v3`.
### Rationale
Governance identity for line patterns is structural type, not absolute scale.
Observed outputs showed 2,083 exact-length variants where the governance-meaningful
distinct population is estimated around 50–200 structural patterns. Scale variants
such as Hidden 1/8 and Hidden 1/4 should resolve to one governance unit.
### Normalization rule
- Preserve ordered segment kind sequence.
- Normalize segment lengths by ratio relative to non-dot total length.
- Dot segments use relative epsilon = 1% of non-dot total to keep dot participation
  scale-invariant.
- Pure-dot safeguard: if non-dot total is zero, use tiny fallback epsilon `1e-9`.
### Consequences
- `line_pattern.segments_norm_hash` must be computed during flatten by default
  (no opt-in flag required).
- `line_pattern.segments_def_hash` remains emitted in identity evidence for forensic
  analysis but is explicitly excluded from join participation.
- Pattern cardinality should collapse materially for structurally equivalent,
  differently-scaled line patterns.

### Validation extension (accepted operating practice)
- Precision sensitivity must be evaluated around the active normalization token
  precision (currently `.6f`) using neighbor sweeps (typically ±2 decimals).
- Precision selection is determined by elbow behavior: maximize collapse of
  floating-noise fragmentation while preserving stable structural distinctions.
- Evaluation should track not only unique hash count, but also split/merge
  behavior by dominant labels and shape-sequence consistency.

---

## Notes

- This document is **append-only**.
- Reversals require a new decision entry that references the original.
- Implementation details belong in code, not here.
