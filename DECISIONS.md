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

## D-018 — loaded_family_types scope: loaded families only; system families deferred

### Status
Accepted (2026-05-13)

### Context
`loaded_family_types` extracts `FamilySymbol` records using the parameter-schema
evidence model (`lft.*` / `lftp.*`). In practice, `FamilySymbol` collectors also
surface system-family types (e.g. curtain panels, stacked walls, curtain walls,
MEP system types) where `Family.IsEditable == False`. These appear in extraction
output with `lft.family_is_editable: "false"` and `lft.type_name` often missing,
causing `status: degraded` and `identity_quality: incomplete_missing`.

`compound_types` already covers some system families (standard wall, floor, ceiling
types) via their specific `ElementType` subclasses and layer-structure semantics, but
does not reach stacked walls, curtain walls, curtain systems, or MEP system types.

### Decision
`loaded_family_types` is scoped to user-loaded families only as its **governed
primary audience**. System families are not filtered out at extraction time — they
appear in output with `lft.family_is_editable: "false"` as a discrimination signal —
but they are not governed under this domain's identity contract.

A dedicated `system_family_types` domain (or expansion of `compound_types`) to cover
the remaining system families (stacked walls, curtain walls, curtain systems, MEP
system types) is deferred until Phase-1 analysis establishes which categories are
worth the extraction investment.

### Consequences
- No filter change to `loaded_family_types` extractor.
- Downstream analysis should use `lft.family_is_editable` to segment loaded vs.
  system records if needed.
- `compound_types` gap (stacked walls, curtain walls) is a known open item.

---

## D-019 — Governance narrative evidence-package layer (Phase 1: manifest/health/evidence-map)

### Status
Accepted (2026-07-16)

### Context
`tools/generate_governance_narrative.py` produced three outputs
(`governance_domain_summary.csv`, `governance_client_summary.csv`,
`governance_narrative_context.md`) that conflated multiple epistemic roles:
deterministic evidence, package-health/coverage reporting, interpretation
guide, findings store, and executive narrative, with no explicit statement
of which output carries which kind of authority. The generator's own footer
also referenced a stale producer filename
(`generate_governance_narrative_dod_aligned_v2.py`) that never matched the
actual script name.

A companion discovery-scaffold repository (`GMcDowellJr/llm_evidence_framework`,
explicitly not a finalized standard) documents a pattern for separating
deterministic evidence from interpretation: an authority-level vocabulary
(`authoritative_deterministic_evidence` / `controlled_interpretation` /
`convenience_summary` / `user_provided_note` /
`llm_generated_provisional_interpretation`) and an evidence-map shape
(artifact_id/producer/authority_level/context_role/grain/can_answer/
cannot_answer/known_limitations/null_semantics/related_artifacts).

### Decision
Add a package-boundary layer around the existing generator without changing
any of its deterministic calculations, thresholds, or CSV columns:

- A new sibling module, `tools/governance_evidence_package.py`, defines the
  authority-level vocabulary (independently, as this repo's own constants —
  no import from or runtime dependency on `llm_evidence_framework`) and
  builds three new JSON artifacts: `governance_package_manifest.json`
  (provenance: inputs/outputs/comparison_run_ids), `governance_package_health.json`
  (schema detection, used-view fallback, comparison_type coverage, blocking
  conditions, warnings — all mechanical/factual text, no severity judgment),
  and `governance_evidence_map.json` (one entry per artifact — the 10 CSVs
  the generator reads via CLI args, 2 sibling CSVs it produces but never
  reads (`cross_segment_file_pairs.csv`, `comparison_registry.csv`), and its
  6 own generated artifacts, 18 total).
- The narrative gains a new authority-header section stating its own
  `controlled_interpretation` role and the authority ordering (package
  health and source CSVs outrank rollup CSVs, which outrank narrative
  prose), and the stale producer-identity footer is corrected to reference
  the real script name via a shared `GENERATOR_IDENTITY` constant.
- Structured findings (`governance_findings.json`) and policy externalization
  (thresholds, domain-governance policy, onboarding rules into
  `policies/governance/`) are explicitly deferred to later PRs — this
  decision covers Phase 1 only.
- `--emit-evidence-package` defaults to **on**: every existing invocation of
  the generator starts producing the three new JSON files with no CLI change
  required. `--no-emit-evidence-package`, `--policy-dir` (recorded but not
  yet read), and `--package-schema-version` are additive, backward-compatible
  CLI flags.

### Consequences
- Every run of `generate_governance_narrative.py` now writes 3 additional
  JSON files by default, alongside the unchanged CSV/MD outputs.
- `governance_domain_summary.csv` and `governance_client_summary.csv`'s
  column sets, and all classification/scoring logic, are unchanged (locked
  in by regression tests asserting the exact column lists).
- `docs/governance_evidence_package.md` documents the artifact inventory,
  authority ordering, and the "documented but not fixed in this phase"
  limitations (the `governance_narrative_scope_gap_audit.md` A2 pool_scope
  caveat, the "—" vs "" missing-value inconsistency in `governance_domain_summary.csv`,
  and the C8 missing domain-label contract).
- Downstream tooling that reads `generate_governance_narrative.py`'s output
  directory will now find three new JSON files unless it opts out.

---

## D-020 — Governance narrative evidence-package layer (Phase 2: structured findings)

### Status
Accepted (2026-07-16)

### Context
D-019's package manifest/health/evidence-map layer made the governance
package's provenance and coverage machine-legible, but the actual
classification conclusions (which domains are baseline candidates, which
show high fragmentation or passive-inheritance risk, which clients have low
coherence) still existed only as prose sentences inside
`governance_narrative_context.md`'s "Key Findings and Governance
Recommendations" section, generated independently of any structured data
model. A downstream reader (human or LLM) could not enumerate "every domain
currently classified `baseline_candidate`" without parsing narrative text.

### Decision
Add `governance_findings.json`: one structured finding per (subject, rule)
match, covering the ten required categories (`baseline_candidate`,
`strong_baseline_candidate`, `local_review_required`, `high_fragmentation`,
`active_local_practice`, `cross_client_convergence`, `low_client_coherence`,
`passive_inheritance_risk`, `missing_or_degraded_evidence`,
`leadership_question`). Each finding carries `finding_id`, `subject`
(`type`/`id`), `finding_type`, `status`, `origin`, `fidelity`,
`authority_level`, `summary`, `support[]` (`artifact_id` + `selector` +
`fields`), `rule_ids[]`, and `limits[]` — the origin/fidelity/authority/
limits fields are this repo's own vocabulary (`tools/governance_evidence_package.py`),
modeled on but independent of the design-reference `llm_evidence_framework`
repo's stated epistemic-provenance components.

A new `_classify_domains_for_findings()` in `generate_governance_narrative.py`
is the single source of truth for tier-derived classification buckets,
shared by `build_structured_findings()` (which produces the JSON) and
`render_findings_and_recommendations()` (which now consumes the same
findings list instead of recomputing an independent classification) — the
two can no longer drift into disagreeing readings of the same underlying
data. Leadership questions are marked `status: question_not_claim` /
`authority_level: convenience_summary`, distinct from evidence findings
(`status: supported`), so a suggested review question is never mistaken for
an observed result.

### Consequences
- `governance_findings.json` becomes the 19th evidence-package artifact
  (added to `governance_evidence_map.json`); no existing CSV column,
  classification/scoring logic, or threshold changed.
- A finding's `support[].artifact_id` always resolves to a real artifact and
  `selector`/`fields` that exist on it — enforced by construction, since
  both consumers read from the same `cascade`/`client_rows`/
  `governance_state_summary` inputs used to write `governance_domain_summary.csv`/
  `governance_client_summary.csv`.
- No baseline finding is ever emitted for a domain whose primary metric is
  unavailable: `assign_tier()` itself routes that domain to
  `TIER_INSUFFICIENT` before `_classify_domains_for_findings()` runs, so
  the gate is structural, not a separate check that could be forgotten.

---

## D-021 — Governance narrative evidence-package layer (Phase 3: policy externalization)

### Status
Accepted (2026-07-17)

### Context
D-019/D-020 made the governance package's provenance, health, and findings
machine-legible, but the actual governance judgments underneath those
findings — tier-assignment thresholds, reliability-band cutoffs,
cross-client convergence/coherence thresholds, which domains are excluded
from aggregate scoring, which are flagged as passive-inheritance risk, fixed
editorial guidance text for specific domains, and client-onboarding
interpretation thresholds — were still Python literals scattered across
`generate_governance_narrative.py`. These are deterministic classification
rules, not raw corpus observations, and the task's own framing distinguishes
"authoritative deterministic evidence" from "controlled interpretation"
(rule-derived classification on top of that evidence) — a rule's threshold
value is itself part of the interpretation layer, not something a reader
can audit or override without reading Python source.

### Decision
Move these values into four JSON policy profiles under
`policies/governance/`: `governance_thresholds.json` (reliability bands,
tier-assignment bands, cross-client convergence/coherence thresholds, client
confidence bands), `domain_governance_policy.json`
(`excluded_from_scoring`, `passive_inheritance_risk_domains`, per-domain
`domain_guidance` text, and `static_findings_guidance` always rendered in
the findings section), `client_onboarding_policy.json`
(`_client_onboarding_profile()`'s interpretation thresholds — kept as a
separate profile from `governance_thresholds.json` even where a default
value numerically coincides, since these gate onboarding narrative text, not
`governance_tier`), and `finding_rules.json` (documentation-only
`rule_id → {finding_type, description}` metadata for D-020's `rule_ids`).

A new sibling module, `tools/governance_policy.py`, is a generic JSON
policy-profile loader (mechanical load/fallback only — no governance
business content of its own, mirroring `tools/governance_evidence_package.py`'s
separation of the generic envelope layer from the domain-governance logic
that stays in `generate_governance_narrative.py`). `--policy-dir` (accepted
but inert since D-019) now defaults to `policies/governance/` and is
actually read: `apply_governance_policy()` reassigns every module-level
threshold/domain-policy constant this file's existing functions already
read as plain globals (`EXCLUDED_FROM_SCORING`, `PASSIVE_INHERITANCE_RISK_DOMAINS`,
`DOMAIN_GUIDANCE`, `STATIC_FINDINGS_GUIDANCE`, and ~25 threshold constants)
from the resolved policy at the start of `main()`, so no existing function
body or call site needed to change — only the *source* of each constant's
value changed, from a Python literal to a policy-file-or-fallback lookup.
The shipped `policies/governance/*.json` files reproduce this generator's
pre-externalization Python literals value-for-value (verified by a
regression test comparing on-disk JSON against the module's own
`_POLICY_DEFAULTS`), so no existing invocation's output changes by default.
A profile file missing from `--policy-dir` falls back, per file, to this
generator's own built-in default for that profile only — reported in
`governance_package_health.json`'s `policy_load_status`/`fallbacks_used`/
`warnings` (a `governance_policy_profile_defaulted` warning degrades
`overall_status`) and in `governance_package_manifest.json`'s
`policy_profiles.profiles` (resolved `profile_id`/`schema_version`/`source`
per profile).

### Consequences
- No governance classification output (tier assignment, reliability
  banding, cross-client convergence/coherence tiering, onboarding narrative
  text, excluded/passive-inheritance-risk domain sets, or the two
  domain-specific/static findings-guidance sentences) changed for any
  existing invocation — locked in by a regression test running `main()`
  twice (default vs. explicit `--policy-dir policies/governance/`) and
  asserting byte-identical `governance_domain_summary.csv` output.
- A governance threshold, excluded/passive-inheritance-risk domain set, or
  guidance sentence can now be changed by editing JSON under
  `--policy-dir`, without a code change — verified with tests that override
  one policy file and observe the corresponding classification/prose output
  change (e.g. lowering `tier_strong_baseline_min` promotes a previously
  `Investigate Before Baseline` domain to `Strong Baseline Candidate`).
- Because the overridden constants are process-global module attributes
  (not threaded through function signatures), every test that calls
  `apply_governance_policy()` with a non-default policy must reset it
  afterward (an autouse pytest fixture in
  `tests/test_generate_governance_narrative_policy.py` does this) — a test
  that forgot to reset could leak an overridden threshold into an unrelated
  test file running later in the same pytest session. This is a known
  trade-off of the "reassign existing module globals" approach chosen to
  avoid threading a policy object through dozens of existing call sites in
  one pass; a future phase may thread policy explicitly instead if this
  proves fragile in practice.
- `DOMAIN_LABELS` (human-readable domain display names) is **not**
  externalized in this phase — it is a display-name contract issue (see the
  evidence map's existing C8 known-limitation note), not a governance
  threshold or policy rule, and remains a Python literal.

---

## D-022 — Governance narrative evidence-package layer (Phase 4: interpretation guide, question routes, governance brief)

### Status
Accepted (2026-07-17)

### Context
D-019/D-020/D-021 made the governance package's provenance, health,
findings, and classification thresholds machine-legible and externally
editable, but a reader (human or LLM) still had no dedicated place to learn
*how to interpret* the package's metrics and classifications (what a tier
does and doesn't mean, comparability rules, missing-value semantics,
known bad inferences), no catalog of where to look for a recurring
question, and no artifact shorter than the full `governance_narrative_context.md`
for a quick top-line read of a specific run.

The design-reference `GMcDowellJr/llm_evidence_framework` repository
(explicitly provisional, no runtime dependency) documents this gap as the
"interpretation layer" and "question routing" artifact roles
(`patterns/deterministic_to_llm_boundary.md`, `notes/current_thesis.md`),
and a discovery scaffold for capturing question routes as they recur rather
than inventing them upfront (`discovery/question_route_discovery.md`,
`discovery/script_recipe_discovery.md`): "a route should not be codified
just because it was imagined."

### Decision
Add three artifacts:

- `docs/governance_interpretation_guide.md` — a **stable, package-type-level**
  document (not regenerated per run, versioned via its own
  `interpretation_guide_version` header) explaining cascade-field and
  `governance_tier`/`score_reliability` semantics, comparability rules
  (sector, unit system, all-view/used-view), missing-value conventions,
  authority ordering, and a "known bad inferences" section specific to this
  package type.
- `docs/governance_question_routes.md` — a **candidate** question-route
  catalog (all routes at "candidate" maturity per the reference framework's
  own maturity scale — none has a proven history of repeated use for this
  package type yet), following that framework's discovery scaffold
  (Status / Question forms / Intent / Primary+Secondary artifacts /
  Relevant fields / Evidence type / Supported+Unsupported conclusion types /
  Comparability requirements / Common traps / Escalation). Seeded from
  questions this generator already treats as recurring (the leadership
  questions rendered in the narrative, and the ten `governance_findings.json`
  finding types) rather than invented from nothing.
- `governance_brief.md` — the one new **generated, per-run** artifact:
  built by `render_governance_brief()`, which consumes the already-computed
  `findings` list and `governance_package_health.json` directly (no new
  classification logic — the same "consume, not recompute" discipline
  D-020 established), rendering package status, corpus counts, each
  finding category capped at 10–15 items with a pointer to
  `governance_findings.json` for the full list, and the leadership
  questions as a distinctly-marked numbered list. `authority_level:
  convenience_summary`, subordinate to package health, the source CSVs,
  the rollup CSVs, and `governance_findings.json`.

A new `--emit-interpretation-layer`/`--no-emit-interpretation-layer` CLI
flag (default: on) controls `governance_brief.md` only, independently of
`--emit-evidence-package` (but only takes effect when that flag is also
on, since the brief depends on findings/health). The two static docs are
unaffected by either flag — they are always listed in
`governance_evidence_map.json` with real `Path.exists()`-based presence,
since they are checked-in repo docs, not per-run outputs.
`governance_narrative_context.md` is retained unchanged as a compatibility
artifact; its authority header gains pointers to all three new artifacts.

### Consequences
- `governance_evidence_map.json` grows from 19 to 22 artifacts.
- No existing classification, scoring, CSV column, or narrative section
  changed — `governance_brief.md` computes nothing new, and the two static
  docs are pure documentation.
- Unlike every other "generated this run" evidence-map entry (whose
  `present` is asserted `True` by construction), `governance_brief.md`'s
  `present` is a genuine per-run check, since `--no-emit-interpretation-layer`
  can suppress it while the rest of the package still generates normally —
  a consumer must check this artifact's own `present` field, not just
  package-level flags, before assuming it exists for a given run.
- Script recipes and deterministic extractors (the next two rungs on the
  reference framework's promotion ladder: ad hoc question → candidate route
  → active route → recipe → extractor) are explicitly out of scope for this
  phase — no route in `docs/governance_question_routes.md` has earned
  promotion past "candidate" yet.

---

## D-023 — Governance narrative evidence-package layer (Phase 5: live file-availability inventory)

### Status
Accepted (2026-07-22)

### Context
The evidence package (D-019 through D-022) describes, in detail, every
artifact this generator itself reads or writes. It has no way to describe
a file it does *not* read: `compare_cross_segment.py` writes several CSVs
(`pattern_reuse_summary_by_domain.csv`, `project_mean_file_pair_jaccard_matrix.csv`)
that this generator's own code comments already note are "deliberately not
consumed," but that note lived only in Python — no package artifact told a
reader (human or LLM) that these files exist at all. When a question needs
more detail than the rollups carry, the LLM reading this package has no way
to know a candidate drill-down file exists; it either guesses or stonewalls
with "I need more data" and no path forward.

Step 0 for this phase confirmed two facts that shape the whole design:
(1) this package has, and will continue to have, no query/tool-calling
path — `generate_governance_narrative.py`'s outputs are consumed single-shot
by a reader that cannot fetch anything itself, so *naming* a file is the
only lever available, not fetching it; (2) no `csv_inventory.md`-style
utility already existed anywhere in this repo to reuse.

### Decision
Add `governance_file_inventory.json`, built fresh on every run by
`inventory_export_directory_files()` (`tools/governance_evidence_package.py`):
a `Path.glob("*.csv")` scan of the cross_segment export directory
(`--summary`'s parent) and, when it differs, the relationship-layer output
directory, excluding every path already tracked as an input, output, or
sibling artifact elsewhere in the package. For each undiscovered file it
records the column header, an inferred per-column dtype (`integer`/`float`/
`boolean`/`string`/`empty`), and the row count — **never a sample row or
cell value**, matching the "type of data, not shape of values" scope
decision for this phase. A short narrative sentence per file is attached in
`generate_governance_narrative.py`: when the filename matches a `matrix_name`
already documented in `matrix_output_manifest.csv`, it reuses that row's own
`interpretation`/`known_limitations` text verbatim (the same free-text
narrative field pattern `compare_cross_segment.py`'s `add_manifest()`
already uses for the registered `project_*` matrix artifacts); otherwise it
falls back to a structural sentence built only from the header/row-count
the scan already computed. Neither path hand-maintains a per-filename
description, so a brand-new future export (a promotion-candidates output,
a future PR) is picked up automatically the next time this generator runs,
with no follow-up edit to this code.

`governance_brief.md` renders the same already-scanned data as its own new
`## Detail-Layer File Inventory` section — a directory of what exists at
the detail layer, appended after the leadership questions and deliberately
not interleaved into the per-domain findings sections above it. The
section is entirely omitted (not blank-rendered) when the scan found
nothing undiscovered. `governance_file_inventory.json` is gated by
`--emit-evidence-package` (matching manifest/health/evidence-map/findings);
the `governance_brief.md` section additionally requires
`--emit-interpretation-layer`, matching the rest of the brief's existing
gating. `governance_narrative_context.md` itself is unchanged — this phase
adds no section there, preserving that document's existing documented
guarantee that `--no-emit-evidence-package` leaves CSV/MD outputs
unaffected.

`governance_file_inventory` is registered as a 33rd `governance_evidence_map.json`
artifact: `authority_level: authoritative_deterministic_evidence` (the
header/dtype/row-count facts are directly observed, not interpreted), with
an intentionally empty `related_artifacts` — unlike every other entry, the
files this artifact lists vary run to run, so no fixed relationship list
would stay accurate.

### Consequences
- `governance_evidence_map.json` grows to 33 artifacts.
- No existing classification, scoring, CSV column, or narrative section
  changed — the scan only ever describes files nothing else in the package
  already reads, and `governance_brief.md`'s new section is additive,
  omitted entirely when there is nothing to report.
- The design-reference `GMcDowellJr/llm_evidence_framework` repository's
  `discovery/evidence_map_discovery.md` scaffold (candidate evidence-map
  field list, already cited by D-019) covers "what files exist" as an
  explicit evidence-map purpose; this phase is the first to close that gap
  for files this generator does not itself consume, still with no runtime
  dependency on that repository.
- Explicitly out of scope, per this phase's own boundary: any query, fetch,
  or tool-calling mechanism that would let an LLM actually retrieve a named
  file's contents. That remains a different initiative, if it ever happens
  — this phase only makes the file's *existence and shape* discoverable
  within the single-shot package.
- `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
  (read-only dependency, per this phase's own scope boundary).

---

## D-024 — Governance narrative evidence-package layer (Phase 6: escalation-target file coverage)

### Status
Accepted (2026-07-22)

### Context
`docs/governance_interpretation_guide.md`'s "What to do when a pre-built
route isn't enough" section tells a reader escalating past the compact layer
to "name which large source file is needed" and names
`cross_segment_file_pairs.csv`/`comparison_registry.csv` as examples,
gesturing at "another large sibling artifact the generator never parses."
Before this phase, that gesture was unresolved: a reader had no way to
learn the exhaustive list of such files, and the two named files'
`governance_evidence_map.json` entries carried only hand-written
`context_role`/`can_answer`/`cannot_answer` text — no real column header or
row count — so escalating still meant opening a multi-GB file cold to learn
its schema before the interpretation guide's own step 2 (write a filtered
extraction script) was possible.

Step 0 for this phase confirmed two facts against the code, not assumed:
(1) `generate_governance_narrative.py`'s own module docstring already lists
the exhaustive set of files it writes no code path to read: `comparison_
registry.csv`, `cross_segment_file_pairs.csv`, `pattern_reuse_summary_by_
domain.csv`, and `project_mean_file_pair_jaccard_matrix.csv` — four files,
not two; (2) no `csv_inventory.md`-style utility needed rebuilding —
D-023's `_scan_csv_file()` already does exactly the header/dtype/row-count
scan this phase needs, just scoped to files with no artifact_id at all.

### Decision
All four files above are now registered as `sibling_paths` in
`generate_governance_narrative.py`'s `main()` and get a full
`governance_evidence_map.json` artifact entry
(`build_evidence_map()` in `tools/governance_evidence_package.py`) with
`context_role`, `grain`, `can_answer`, `cannot_answer`, and
`known_limitations` text in the same voice as every other archive_only
sibling entry.

A new helper, `_sibling_scan_fields(path, present)`, wraps D-023's
`_scan_csv_file()` — no second scanning implementation — to populate each of
the four entries' `columns` (column name + inferred dtype) and `row_count`
fields when the file is present on disk; both fields are simply absent from
the entry when the file is not present, since scanning a nonexistent path is
meaningless, not an all-zeros result. No sample row or cell value is ever
retained, the same "type of data, not shape of values" scope decision D-023
made.

Because `pattern_reuse_summary_by_domain.csv` and `project_mean_file_pair_
jaccard_matrix.csv` are now registered `sibling_paths`, they are
automatically excluded from `inventory_export_directory_files()`'s generic
undiscovered-file scan (the same `_known_artifact_paths` exclusion set
D-023 already built) — each file now has exactly one narrative home, not
two competing descriptions of the same file. This is `can_answer`/
`cannot_answer` doing the job it already does, not a second per-file
narrative layer beside it.

`pattern_reuse_distribution`'s and `project_fragmentation_diagnostic`'s
`related_artifacts` lists gained a reverse link to their newly-registered
by-domain/mean-file-pair siblings, matching the bidirectional linking
already used elsewhere in the evidence map (e.g.
`pattern_reuse_summary_by_client` already named `pattern_reuse_distribution`
as related).

**PR-review fix (anchor point):** `pattern_reuse_summary_by_domain.csv` and
`project_mean_file_pair_jaccard_matrix.csv`'s sibling paths are not
hard-coded beside `--summary`'s directory. `compare_cross_segment.py`
writes both files to the same `--out-dir` as already-optional, already-
CLI-supplied siblings (`pattern_reuse_summary_by_domain.csv` alongside
`pattern_reuse_summary_by_client.csv`/`pattern_reuse_distribution.csv`;
`project_mean_file_pair_jaccard_matrix.csv` alongside
`project_fragmentation_diagnostic.csv` and the other `project_*` matrices),
so each is anchored to whichever of those related optional flags
(`--reuse-by-client`/`--reuse-distribution`; `--project-fragmentation-
diagnostic`/`--project-union-jaccard-matrix`/`--project-density-similarity-
matrix`/`--project-pool-containment-matrix`) was actually supplied, falling
back to `--summary`'s directory when none were — the identical pattern
`_relationships_anchor` already established for `governance_relationships.csv`
in the prior relationship-layer phase. Without this, a caller running a
mixed-directory pipeline (optional reuse/project outputs living somewhere
other than `--summary`'s directory, which the CLI already allows) would get
a permanently `present: false` entry for these two escalation targets even
though the real files sit right beside the input they did supply. The
D-023 live-scan directories (`_export_scan_dirs`) grew to include both new
anchor directories too, for the same reason `_relationships_anchor.parent`
was already scanned.

### Consequences
- `governance_evidence_map.json` grows from 33 to 35 artifacts.
- Three existing tests that used `pattern_reuse_summary_by_domain.csv`/
  `project_mean_file_pair_jaccard_matrix.csv` as stand-ins for "a generic
  undiscovered file" (`test_file_inventory_surfaces_an_undiscovered_sibling_csv`,
  `test_file_inventory_borrows_interpretation_from_matrix_output_manifest`,
  `test_file_inventory_surfaces_regardless_of_interpretation_layer_flag`)
  were updated to use fictitious filenames instead — those two real
  filenames are no longer valid "undiscovered" examples now that they carry
  their own artifact_id, which is the intended effect of this phase, not a
  regression.
- No existing classification, scoring, CSV column, or narrative content
  changed — this phase only adds evidence-map metadata (context_role,
  can_answer/cannot_answer, columns, row_count) for files this generator
  already declared it never reads.
- `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
  (read-only dependency, per this phase's own scope boundary).
- No query/fetch/tool-calling mechanism was added — the package remains
  single-shot; a reader still cannot fetch a named file's contents through
  this package, only see its existence and real shape ahead of writing their
  own extraction script.

---

## Notes

- This document is **append-only**.
- Reversals require a new decision entry that references the original.
- Implementation details belong in code, not here.
