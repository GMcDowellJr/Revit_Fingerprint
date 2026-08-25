# DECISIONS

> **Historical-record notice (2026-08-20):** This append-only decision log may name identities and deployment details that were accurate when recorded. Those references are non-operational and do not define current executable defaults, policy, or supported configuration.

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

**Second PR-review follow-up (anchor completeness):** the two anchor chains
above also fall back to `--union-inventory` (`cross_segment_union_inventory.csv`,
for `pattern_reuse_summary_by_domain.csv`) and `--matrix-manifest`
(`matrix_output_manifest.csv`, for `project_mean_file_pair_jaccard_matrix.csv`)
before falling back to `--summary`'s directory — both are written by the
same `compare_cross_segment.py` invocation to the same `--out-dir` as their
respective escalation target (`matrix_output_manifest.csv` in particular
shares the exact same write block as every `project_*` matrix), so a run
that supplies only that broader optional input, without any of the more
specific reuse/project-matrix flags, still anchors correctly instead of
silently falling through to `--summary`'s directory.

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

## D-025 — `identity` domain expansion: `project_info.*` fields, included in sig_hash

### Status
Accepted (2026-08-10)

### Context
`domains/identity.py` captured only worksharing status, Revit version/build,
and file-local lineage signals (central path, filename, project title). It
never read `doc.ProjectInformation` at all. Step 0 for this area (`audit_
results/audit_11_domain_extractor_delta_step0_findings.md` §5) confirmed: (1)
zero overlap with `tools/build_segment_manifest.py`, which never reads Revit
and sources its governance labels (`client_label`/`discipline_label`/etc.)
from a hand-curated `file_metadata.csv`, not from `ProjectInformation`; (2)
the requested built-in fields (Project Number/Status/Address/Issue Date,
Client Name, Building Name, Organization Name/Description) are confirmed
Revit built-ins, present on every project's `ProjectInformation` element
regardless of template; (3) `Office` is a confirmed Stantec-authored shared
parameter (GUID `6b61afc7-13eb-4af5-8b65-889f978af4f3`), null by design on
any non-Stantec-template project.

This creates real tension with this project's own non-negotiable rule
("Names are metadata only — never included in behavior hashes unless
explicitly stated", with D-010's phase-name hash inclusion as the only prior
exception): most of these new fields (project name, client name, building
name, organization name/description, address) are literally human-entered
naming/labeling metadata, not technical behavior. `identity.py`'s existing
design, however, hashes every item it puts in `identity_items` indiscriminately
(no separate non-hashed identity_basis slot exists in record.v2 today — the
precedent for "descriptive but non-hashed" signals, e.g. `project_title`, is
to live only in `phase2.unknown_items`, entirely outside `identity_basis.items`
and `sig_hash`).

### Decision
`project_info.*` items ARE included in `identity_items` / `identity_basis.items`
/ `sig_hash` for the `identity` domain — a second explicit, documented
exception to the "names are metadata only" default rule, alongside D-010.
This was a deliberate scope call for this domain (the identity domain's own
module docstring already described itself as "metadata only", and its
sig_hash was already a thin worksharing/version/build fingerprint, not a
behavioral one in the sense other domains use) rather than something forced
by the record.v2 schema.

To limit the blast radius of that call:
- `project_info.*` is explicitly excluded from the identity `join_key` policy
  (`policies/domain_join_key_policies.json`) — it does not participate in
  cross-project join-key matching, the same treatment `identity.project_title`
  already got.
- `status`/`status_reasons`/`identity_quality` remain driven only by the
  pre-existing core items (`is_workshared`/`revit_version_number`/
  `revit_version_name`/`revit_build`) — project_info.* fields being blank or
  legitimately not-applicable (both extremely common: many real projects leave
  Client Name/Project Status blank, and `Office` is absent on every
  non-Stantec project by design) must not flip this domain's record status to
  degraded on every ordinary export.
- Built-ins use `q=unreadable` only if the `Parameter` object itself is
  missing (an unexpected API/document gap); `Office` — the only remaining
  shared/custom field — uses `q=unsupported.not_applicable` when its
  definition isn't found at all, distinct from `q=missing` (definition
  present, value blank) and `q=unreadable` (read exception).
- Built-ins are read via `BuiltInParameter` enum (`pi.get_Parameter(...)`),
  not `LookupParameter` by display name, so behavior does not depend on
  Revit's UI display-language locale. **Second PR-review follow-up:** the
  three IFC GUID fields turned out to be real `BuiltInParameter` members too
  (`IFC_BUILDING_GUID`/`IFC_PROJECT_GUID`/`IFC_SITE_GUID`, confirmed via
  `tools/archetype/bip_lookup.json`, a generated BuiltInParameter id→name
  registry already consumed elsewhere in this repo, e.g.
  `domains/browser_organization.py`) — not custom/shared parameters as
  originally assumed — and were moved from the named/shared-field table into
  the built-in table, so they now follow the same unreadable/missing
  semantics as the other built-ins rather than Office's not_applicable
  semantics. `Office` itself is read via its confirmed shared-parameter GUID
  (`Element.get_Parameter(Guid(...))`, GUID
  `6b61afc7-13eb-4af5-8b65-889f978af4f3`) rather than `LookupParameter("Office")`
  by display name, which the Revit API can resolve to an arbitrary same-named
  parameter if a project happens to contain more than one "Office" definition
  (first PR-review follow-up); it is the only field left using
  `LookupParameter`-style name resolution (by GUID, not display name).
- `identity.py`'s `sig_basis.schema` is bumped `identity.sig_basis.v1` →
  `.v2`, and the hand-patched `policies/domain_sig_hash_policies.json` entry's
  `sig_hash_schema` likewise `identity.sig_hash.v1` → `.v2` (PR review
  follow-up), so a consumer comparing `sig_hash` across a pre-D-025 export and
  a post-D-025 export can tell the two hash definitions apart instead of
  reading the resulting hash mismatch as fingerprint drift. **Third PR-review
  follow-up:** the `.v2` override is also recorded as `sig_hash_schema` in
  `contracts/domain_identity_keys_v2.json`'s `identity` block itself, not just
  the derived `policies/domain_sig_hash_policies.json` file — the generator
  (`tools/generate_sig_hash_policy.py`'s `build_policy()`) defaults any domain
  lacking that key to `<domain>.sig_hash.v1`, so without this the next
  regeneration would have silently reverted the version bump.
- Office's Address/City/State/Zip/Country/Telephone/Fax/Legal Entity
  sub-fields are deliberately NOT implemented: their exact parameter names
  need confirmation against a real Stantec-template project, which this
  change's environment had no live Revit/Dynamo access to do — guessing names
  that might not match real Stantec parameters was rejected in favor of
  leaving them for a follow-up once that confirmation is possible.
- `identity.py`'s `sig_basis.keys_used` (previously a hardcoded 3-of-4-item
  list that had already drifted from what `sig_hash` actually hashes) is now
  computed dynamically as every `identity_items` key, fixing that drift as a
  side effect rather than as a second, separate hash-algorithm change.
  **Fourth PR-review follow-up:** that fix was initially applied by having
  `phase2.semantic_keys` share the same dynamically-computed list as
  `sig_basis.keys_used` — which was wrong, because it made every newly-hashed
  `project_info.*` naming/label field (and `identity.revit_version_name`,
  previously cosmetic-only) look like Phase-2 "semantic" (behavior-defining)
  content, contradicting this same decision's framing of `project_info.*` as
  metadata whose hash inclusion is an explicit exception, not a behavioral
  reclassification. `sig_basis_keys_used` (all `identity_items` keys, what
  `sig_hash` actually hashes) and `semantic_keys` (unchanged from pre-D-025:
  just `is_workshared`/`revit_version_number`/`revit_build`, what Phase-2
  calls behavior-defining) are now two separate variables/selectors.

### Consequences
- `identity` domain `sig_hash` values change for every export going forward —
  expected and intentional, since real project-metadata content is now part
  of the hash input, not a change to *how* hashes are computed. Previously
  captured `identity` sig_hash values are not comparable to post-D-025 values.
- `contracts/domain_identity_keys_v2.json`'s `identity.allowed_keys` and the
  hand-patched `identity` entry in `policies/domain_sig_hash_policies.json`
  both grow to include the 13 new `project_info.*` keys (the latter was
  hand-patched rather than regenerated via `tools/generate_sig_hash_policy.py`,
  since a full regen would also clobber unrelated hand-tuned `notes` on other
  domains that have already drifted from a strict mechanical regen).
- `file_metadata.csv` / `tools/build_segment_manifest.py` / the governance
  narrative pipeline are unchanged — `project_info.*` are raw Revit reads and
  are not assumed to reconcile with `file_metadata.csv`'s separately-curated
  `client_label`/etc.; a later PR may find they frequently disagree, which is
  worth flagging, not resolved here.
- Office's 8 sub-fields remain a known gap pending live-Revit confirmation of
  their exact parameter names.

---

## D-026 — Backfill: segment lattice is a dimensional powerset, not a single-parent tree

### Status
Accepted (retroactive backfill — already-shipped, load-bearing architecture;
documented now as part of D-027's lineage-model audit)

### Context
`tools/build_segment_manifest.py`'s `DIMENSION_CONFIG` (currently 5 entries:
`unit_system` as the root dimension, `governance_role` as the governance
dimension, `client_label`/`discipline_label`/`business_center_label` as cut
dimensions) and `_build_segments()` construct every segment as a subset —
via `itertools.combinations` over a `frozenset` key — of the declared
dimension values present on a row: the full powerset of non-root dimensions,
not a single-parent classification tree. Every segment carries one recorded
primary `parent_segment_id` (derived by dropping the last-declared present
field) for folder/registry bookkeeping, but a segment's true position in the
hierarchy is a lattice — it can have as many immediate structural parents as
it has non-root dimensions present, one per dropped field. This architecture
underlies every downstream segment/governance tool (`run_segment_
orchestrator.py`, `compare_cross_segment.py`, `governance_manifest.py`,
`compare_governance_populations.py`, etc.) and predates this decision-log
entry, which had no record of it.

### Decision
Accepted as-is; this entry backfills documentation only; the architecture
itself is not being changed here (out of scope for this session — see
D-027). Recording it because D-027's lineage-completeness fix depends
directly on this shape: `parent_segment_id` is a single bookkeeping pointer,
not the segment's full ancestor set, which is exactly why a tree-walk over
it under-reports ancestors.

### Consequences
- Any tool reasoning about segment hierarchy must treat `parent_segment_id`
  as one designated primary parent, not the segment's complete set of
  structural ancestors.
- `ancestor_segment_ids` (see D-027, D-028) is the field that actually
  carries the lattice's multi-parent structure.

---

## D-027 — `structural_ancestor` and `population_containment`: two independent lineage relations for comparison-discovery validity

### Status
Accepted (2026-08-12)

### Context
`tools/compare_cross_segment.py`'s `discover_*` functions generate segment
pairs for cross-segment comparison. A pair is invalid whenever one
segment's real file population contains the other's — comparing a segment
against data that already contains some or all of its own. Before this
decision, the only guard was `_is_lineage_related()`/`_build_ancestor_map()`,
which walked `parent_segment_id` as a single-parent chain — under-reporting
ancestors whenever a segment had more than one non-root dimension present
(D-026) — and had no empirical, non-dimensional counterpart at all.

A live audit against the real corpus (481 segments, `segment_manifest.csv` +
`segment_membership.csv`) found: (1) the dimension-lattice ("structural")
ancestor relation, once corrected to a full transitive closure, never
produces a false population-superset claim — 0 counterexamples across all
115,440 segment pairs checked — but is incomplete: 1,806 of 4,080 real
population-subset pairs in the corpus have no dimensional explanation at
all; (2) `discover_sibling_segments()` carried no lineage guard of any kind
and emitted 101 real population-containment violations (`sibling_templates`/
`sibling_containers`/`sibling_projects`), via its `redundant_single_child`
resolution mechanism bucketing a structural ancestor and its own descendant
as if they were unrelated peers.

### Decision
Lineage/containment is split into two explicitly separate,
independently-computed relations:

- **`structural_ancestor`** — the dimension-lattice relation
  (`_build_ancestor_map()`/`_is_lineage_related()`), now a full transitive
  closure computed by walking each segment's `ancestor_segment_ids` (D-028)
  as a multi-parent adjacency list, rather than a single
  `parent_segment_id` chain. Reliable (no known false positives on the
  audited corpus) but incomplete. Governs authority/provision semantics
  (e.g. `discover_governance_chain()`'s Template/Container→Project fan-out).
- **`population_containment`** — a new, empirically-derived relation
  computed directly from real `export_run_id` membership
  (`segment_membership.csv`), independent of any dimensional relationship.
  Threshold-gated by two Jenks-natural-breaks passes (`tools/jenks_utils.
  jenks_breaks()`, `n_classes=2` — the more general of the two independent
  Jenks implementations in this repo, reused rather than duplicated a
  third time) fit on real *non-structural* population-subset pairs: a
  size-noise floor (`min_population_for_containment`) and a
  containment-ratio floor (`min_containment_ratio`), so a
  materially-insignificant coincidental subset (e.g. a 1-file segment
  trivially "contained" in almost anything) is not mistaken for real
  inheritance. Computed thresholds and resulting candidate pairs are
  written to `population_containment_thresholds.csv` for human review, not
  silently baked into code — a first pass, not a locked policy.

`discover_sibling_segments()` checks BOTH relations before finalizing a
pair. On the audited corpus, `structural_ancestor`'s completeness fix alone
already resolves all 101 known violations — `population_containment` did
not independently flag any of them, since every one of the 101 turned out
to be dimensionally explained once the lattice closure was corrected.
`population_containment` is retained there anyway as a second, independent
guard against non-structural coincidental containment (which the corpus
audit shows exists as a general phenomenon in this data, distinct from and
not corpus-verified as an active `discover_sibling_segments` defect).
`discover_governance_chain()` keeps `structural_ancestor` only — it had
zero real violations both before and after the completeness fix, so layering
`population_containment` there too is deferred rather than speculative.
`discover_within_segment()`, `discover_cross_client()`,
`discover_client_cross_bc()`, and `discover_parent_siblings()` currently
carry neither guard; the audit found zero real violations from any of them
on the current corpus, but this is flagged as a known, currently-latent gap
(see the "Lineage/containment guard audit" comment in
`compare_cross_segment.py`) rather than fixed in this pass.

### Consequences
- `discover_sibling_segments()`'s emitted pair count drops on any corpus
  where the defect fires (699 → 598 pairs on the audited corpus).
- A new output file, `population_containment_thresholds.csv`, is written by
  `compare_cross_segment.py` runs whenever `segment_membership.csv` is
  present.
- The Jenks-derived thresholds are a first-pass default, not a locked
  governance decision — the specific pairs they surface should get a human
  sanity check before being treated as final policy.
- Revisiting `discover_within_segment()` / `discover_cross_client()` /
  `discover_client_cross_bc()` / `discover_parent_siblings()` for the same
  guard is left open for a future session if corpus growth ever produces a
  real violation there.

---

## D-028 — `ancestor_segment_ids` serialization fix: `;`-joined, not `|`-joined

### Status
Accepted (2026-08-12)

### Context
`tools/build_segment_manifest.py`'s `segment_manifest.csv` column
`ancestor_segment_ids` joined a segment's list of ancestor segment_ids with
`"|"` — the same character `segment_id` itself uses internally to delimit
dimension values (e.g. `"imperial|Container|0000"`). Since each list
element is itself `"|"`-delimited, the outer `"|".join()` collided with the
inner delimiters: the resulting string cannot be losslessly split back into
the original ancestor-id list (two different real ancestor-id lists can
produce the identical serialized string). A repo-wide grep found exactly
one other reference to the column (`tools/extract_segment_subtree.py`,
which excludes the column *name* from a segment_id-endpoint heuristic — it
never reads the column's *values*) — the field was write-only, and its
serialized form had never actually been consumed by any code path.

### Decision
Re-serialize `ancestor_segment_ids` with `";"` instead of `"|"` (`";"` does
not otherwise occur in a segment_id, since dimension values are themselves
only `"|"`-delimited into segment_id — one level up). `compare_cross_
segment.py`'s `_build_ancestor_map()` (D-027) is the first real consumer of
this field's values, and parses on `";"`.

### Consequences
- Any `segment_manifest.csv` already on disk carries the old, unparseable
  `"|"`-joined `ancestor_segment_ids` values; these are stale and require a
  full manifest regeneration (rerunning `build_segment_manifest.py`) to
  pick up the corrected encoding. There is no migration path for old
  values, because they were never losslessly parseable in the first place.

---

## Notes

- This document is **append-only**.
- Reversals require a new decision entry that references the original.
- Implementation details belong in code, not here.

## D-029 — Governance narrative evidence-package layer (Phase 6: anomaly/note threshold externalization + classification-logic legibility)

### Status
Accepted (2026-08-18)

### Context
D-021 externalized `assign_tier()`, `score_reliability()`, `build_client_summary()`/`build_bc_summary()`, and `_low_coherence_clients()`'s threshold literals to `policies/governance/governance_thresholds.json`, on the stated basis that "a rule's threshold value is itself part of the interpretation layer, not something a reader can audit or override without reading Python source." A Step 0 read of the rest of the file found that sweep was incomplete: `detect_anomalies()` (1997–2193) — the function that produces every `notable_anomalies` entry in `governance_domain_summary.csv` — still carries roughly fifteen bare numeric literals across eleven distinct findings (gt→tp gap `0.75`/`0.55`, dual-schema passive-indicator `0.40`/`0.20`, single-schema bundle-share `0.25`/`0.15`, Group 2 scope-divergence gap `0.25`, Group 1 by-scope spread gap `0.25`, tp>tc gap `0.25`, weak-tc `0.20`, weak-cp `0.50`, view-template zero-discipline `0.05`, phases `0.85`/`0.80`). `render_findings_and_recommendations()` (5017) duplicates the same phases `0.85`/`0.80` check independently — two literals, one governance question, no shared source. `build_governance_state_summary()` (2812) has a bare `0.85` for `provided_to_used_containment` driving `primary_governance_read`. `_passive_inheritance_risk_domains()` (4752, 4756) hardcodes `0.20` and `0.25` independently of `PASSIVE_MATERIAL_THRESHOLD` (already a named, policy-sourced constant) — so a change to that policy value today would *not* propagate to this function, an already-live drift risk rather than a hypothetical one. The portfolio section's `_shape_note()` (4302) hardcodes `0.8`/`0.3` for its "same shape, different content" note.

Separately, `render_header()`'s "How to Read the Analysis" block (2992–3109) restates definitions of containment, cross-client similarity, all-view/used-view, and score interpretation that substantively overlap `docs/governance_interpretation_guide.md`'s "Metric semantics" section — two independently-authored descriptions of the same concepts, one in Python, one in docs, with nothing keeping them in sync.

Externalizing every remaining literal to policy JSON is necessary but not sufficient for the package's stated goal (an LLM or human reader reasoning through a hypothetical threshold change from the package's own artifacts, without rediscovering logic from Python source). `assign_tier()` and `detect_anomalies()` are not single threshold comparisons; they are ordered branches with exception carve-outs (e.g., `assign_tier()`'s strong-baseline branch has two sub-exceptions that reroute to `TIER_BASELINE_LOCAL_REVIEW` before returning `TIER_STRONG_BASELINE`). A reader holding `governance_domain_summary.csv` and a fully-externalized `governance_thresholds.json` can verify whether one named value crosses one named cutoff, but cannot correctly re-derive which tier or which anomaly note fires without also knowing evaluation order — that ordering currently exists only as Python control flow.

### Decision
Three changes, none altering any existing classification, tier, anomaly-note, or CSV column value for any existing invocation:

1. **Threshold sweep.** Add a new policy profile, `policies/governance/anomaly_thresholds.json`, following D-021's precedent of a separate profile per conceptually distinct threshold family even where a value numerically coincides with another profile's (e.g. this profile's `passive_inheritance_risk_bundle_share_max` and `governance_thresholds.json`'s `passive_material_threshold` both default to values already in use, but gate different code paths and must be independently editable). Every literal cited above gets a named key. The two duplicate phases checks (`detect_anomalies()` line ~2187, `render_findings_and_recommendations()` line 5017) both read the same `phases_tp_extension_max`/`phases_tw_min` keys. `_passive_inheritance_risk_domains()`'s `0.20`/`0.25` are replaced with reads of `passive_material_threshold` (existing `governance_thresholds.json` key) and a new `passive_inheritance_risk_bundle_share_max` key, closing the drift gap rather than just relocating it. `apply_governance_policy()` gains a fourth profile load, mirroring the existing `governance_thresholds.json`/`domain_governance_policy.json`/`client_onboarding_policy.json` load-with-fallback pattern (per-profile default, `governance_policy_profile_defaulted` warning on fallback, `policy_profiles.profiles` entry in the manifest).

2. **Classification-logic legibility.** Add `docs/governance_classification_rules.md` — a stable, non-regenerated, package-type-level artifact following the D-022 precedent set by `governance_interpretation_guide.md`/`governance_question_routes.md` (versioned via its own header, not per-run). It states, in prose/pseudocode, the branch order and exception conditions of `assign_tier()`, `score_reliability()`, `detect_anomalies()`, `build_governance_state_summary()`'s `primary_governance_read` selection, and `_passive_inheritance_risk_domains()` — referencing threshold keys by name from `governance_thresholds.json`/`anomaly_thresholds.json` rather than restating values, so the two artifacts (values in JSON, order/logic in this doc) together let a reader recreate an output instead of rediscovering it from Python. Added to `governance_evidence_map.json` alongside the two existing static docs. This is a legibility aid, not a source of truth — a known limitation (below) tracks the risk of the doc drifting from the code it describes.

3. **`render_header()` trim.** Replace the "How to Read the Analysis" block's restated definitions with a pointer at `docs/governance_interpretation_guide.md`'s "Metric semantics" section. Corpus-specific content in `render_header()` that is not conceptual restatement (the file-role count table, discipline/client lists, the governance-cascade diagram) stays, since that content is per-run data, not interpretation, and has no equivalent in the static docs.

Out of scope: `build_segment_manifest.py` and `compare_cross_segment.py` remain untouched (protected files). No change to any threshold's *value* — this phase is externalization and documentation only, matching D-021's "no existing invocation's output changes by default" discipline.

### Consequences
- No governance classification, tier assignment, `notable_anomalies` content, or CSV column changes for any existing invocation — verified by the same byte-identical regression test D-021 used (`governance_domain_summary.csv` diffed before/after, run twice: default vs. explicit `--policy-dir`).
- A threshold in `detect_anomalies()`, the phases check, `build_governance_state_summary()`'s primary-read selection, or `_passive_inheritance_risk_domains()` can now be changed via JSON without a code change, and can be tested against already-exported CSV fields without a corpus re-run — closing the gap identified in the phase 5 discussion where the phases check was the one tier-adjacent rule that couldn't be tested from provided data alone.
- The two independently-hardcoded phases literals collapse to one named source; `_passive_inheritance_risk_domains()`'s drifted `0.20`/`0.25` collapse to the same source `PASSIVE_MATERIAL_THRESHOLD` already governs elsewhere, so a future change to that policy value now propagates everywhere it should.
- `render_header()` shrinks and `docs/governance_interpretation_guide.md` becomes the sole source for metric/reading definitions — requires confirming the guide's existing "Metric semantics" section actually covers every concept currently explained inline (containment, cross-client similarity, all-view/used-view, score-range interpretation) before the inline text is removed; any gap found gets added to the guide, not left unexplained.
- `docs/governance_classification_rules.md` is a new hand-maintained artifact describing Python control flow in prose. It is not mechanically verified against the functions it describes — a future code change to `assign_tier()`'s branch order could make this document stale without anything failing. This is a known limitation, not resolved in this phase; a future phase could add a regression test that asserts specific documented example inputs produce the documented example outputs, catching drift without executing the whole file line-by-line as a spec.
- `governance_evidence_map.json` grows by one artifact (`docs/governance_classification_rules.md`), following the same `Path.exists()`-based presence check as the two existing static docs.
- `docs/governance_evidence_package.md`'s "Policy profiles and threshold profiles" table gains a fifth row for `anomaly_thresholds.json`, and its phase-log intro paragraph gains a "Phase 6 (D-029)" sentence, matching the existing Phase 1–5 narration pattern.
- `CHANGELOG.md`'s `[Unreleased]` section gains an entry, matching every prior phase's changelog discipline.

## D-030 — Governance narrative evidence-package layer (Phase 7: reading-order artifact and completeness gate)

### Status
Accepted (2026-08-18)

### Context
The package has an authority hierarchy (which artifact wins on disagreement — `render_evidence_authority_header()`) and a topic index (`docs/governance_question_routes.md` — where to look for a recurring question), but nothing states a reading *sequence* for a cold-start reader, human or LLM. `governance_evidence_map.json`'s per-artifact fields (`context_role`, `can_answer`/`cannot_answer`, related-artifact lists) have no ordering or completeness signal at all — a reader can open any artifact first and has no structural cue about what else it depends on.

An ordinal field (e.g. `read_priority: 1, 2, 3...`) was considered and rejected: an ordinal invites an LLM to sort, read the top few, and reason from a partial picture while still technically "following the order." The failure mode this package needs to guard against is a reader stopping partway through a required set, not a reader reading things in a suboptimal sequence. Given D-023's confirmed finding that this package has, and will continue to have, no query/tool-calling path — a reader cannot fetch more context once reasoning starts, only work from what's already in front of it — the only available guardrail is a structural, self-checkable completeness signal in the artifacts themselves.

### Decision
Two additions:

1. **`docs/governance_reading_order.md`** — a new, stable, non-regenerated, package-type-level doc (versioned via its own header, following the `governance_interpretation_guide.md`/`governance_question_routes.md` precedent). States, at the top, this package's intended audience and purpose in the terms the package was actually designed for: a leadership reader who does not know Revit, who understands operational tradeoffs, and who is meant to ask governance convergence/fragmentation questions — not decide standards unassisted. Below that, an explicit ordered path through the package (health check → evidence map/interpretation guide orientation → brief → domain/client rollups → narrative prose → question routes if a specific question → file inventory if deeper drill-down is needed), and a short "read this before drawing conclusions" callout pointing at the two known-bad-inference additions from D-031.
2. **Evidence-map completeness fields**, not an ordinal. `governance_evidence_package.py`'s `_artifact()` gains a `required_before_conclusions: bool` field (which artifacts must be incorporated before a governance conclusion is stated), and `build_evidence_map()`'s top-level output gains a `reasoning_prerequisites: [artifact_id, ...]` list — the full set of `required_before_conclusions=true` artifact_ids, exposed once at the manifest level so a reader can check it as a set to exhaust, not a sequence to sample from. `render_evidence_authority_header()` gains one line naming this field and pointing at `docs/governance_reading_order.md`.

### Consequences
- No existing classification, CSV column, or finding changes — this phase adds a new static doc and two new descriptive fields to already-generated JSON; nothing recomputes.
- `governance_package_manifest.json` and `governance_evidence_map.json` schema versions bump to reflect the new field (per the existing `package_schema_version` override mechanism).
- A future artifact added to the package must have an explicit `required_before_conclusions` value at the point it's added to `build_evidence_map()` — there is no default that silently opts an artifact in or out, since either default is a real content decision about that specific artifact.
- This is a convention, not an enforcement mechanism — nothing in this package can stop a reader from ignoring `reasoning_prerequisites` and stating a conclusion anyway. The gate only works if a reader (human or LLM) actually checks it, consistent with every other guardrail in this package being self-checkable rather than enforced.

---

## D-031 — Governance narrative evidence-package layer (Phase 8: insufficient-evidence and single-region known-bad-inference clarifications)

### Status
Accepted (2026-08-18)

### Context
`docs/governance_interpretation_guide.md`'s "Known bad inferences" section (eight entries) does not address two recurring misreadings, both confirmed live in the current corpus rather than hypothetical:

1. Every domain in the current corpus sits at `TIER_INSUFFICIENT` or `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE` for its *enterprise-scoped* reading, because only business center 2014 currently has Project-role files. The tier names and intro text (`render_domain_tiers()`, ~3239) already distinguish "no enterprise evidence, but BC-level pooled evidence exists" from "no evidence at all," but nothing states that this is a *scope-specific* gap — a domain lacking enterprise-scoped evidence can still have solid client-level, discipline-level, or cross-client-convergence evidence for that same domain sitting in a different summary CSV. Left unstated, this reads as "the package has no evidence," when the accurate statement is "the package has no evidence *at the enterprise scope*."
2. The corpus currently contains files from a single region. `render_header()` already notes region is an "unavailable... future segment dimension" (~3038), but that's a "we don't have this yet" statement, not a "and here is what will happen once we do" statement — specifically, that a future region column will read identically to the existing enterprise-level rollup until a second region's data actually exists in the corpus. This is a fact about current data coverage, not a methodology gap to be fixed.

### Decision
Add two entries to `docs/governance_interpretation_guide.md`'s "Known bad inferences" section:

- *"Insufficient Evidence" is scope-specific, not package-wide.* A domain's enterprise-scoped tier being `Insufficient Evidence` does not mean the domain has no usable evidence anywhere in the package — check `governance_client_summary.csv`, `governance_bc_summary.csv`, and the domain's `cross_client_convergence` field before concluding nothing is known about it.
- *"Region" and "Enterprise" currently read identically, and will continue to until the corpus changes.* All corpus files currently come from one region. If/when a `region` segmentation dimension is added, region-level and enterprise-level results will be identical by construction until a second region's data exists — this reflects current data coverage, not completed cross-region standardization.

Both entries get first-class placement (not buried at entry 9/10) in `docs/governance_reading_order.md`'s "read this before drawing conclusions" callout, per D-030.

Separately, add an explicit audience/intent statement to `docs/governance_interpretation_guide.md`'s existing "What this package is for" section (which currently states subject matter but not audience): this package is written for a reader who does not need Revit domain knowledge, who is expected to ask governance convergence/fragmentation questions rather than resolve them unassisted, and for whom "what to do about it" is explicitly out of this package's scope. `docs/governance_reading_order.md` references this statement rather than restating it, per the same "point, don't duplicate" discipline used for `render_header()`'s trim in D-029.

### Consequences
- Docs-only change — no code, no schema, no classification output affected.
- The next corpus expansion that adds a second business center's Project-role files or a second region should prompt revisiting whether these two known-bad-inference entries are still accurate as written, since both describe a *current* corpus-composition fact, not a permanent structural one.

---

## D-032 — Governance narrative evidence-package layer (Phase 9a: comparison-registry input-completeness note)

### Status
Accepted (2026-08-18)

### Context
`docs/governance_generator_cross_compare_coverage.md` recommended `comparison_registry.csv` be wired in as an optional input to distinguish "this domain's evidence is thin because the comparison wasn't run or is stale" from "this domain's evidence is thin because convergence is actually weak" — currently both look identical (a missing or low row) to a reader of `governance_domain_summary.csv`. At 5.5MB, the file is small enough to read once at generation time without a package-size concern; the earlier assumption that this was excluded for the same size reasons as `cross_segment_file_pairs.csv` (9.8GB) does not hold once the two are considered separately — `cross_segment_file_pairs.csv` should never be read by the generator at all (D-023's file-inventory scan already handles it via header/row-count only, never content), while `comparison_registry.csv` is a normal-sized optional input, structurally identical to `--governance-state-summary` or `--reuse-by-client`.

### Decision
Add an optional `--comparison-registry` CLI argument, following the existing optional-input pattern (present-or-absent, degrades gracefully, reported in `governance_package_health.json`'s `required_inputs`/`optional_inputs` when absent). When supplied, render a small **Input Completeness / Staleness** note near Analytical Notes, per-domain, stating the count of expected segment/domain comparison pairs present vs. missing vs. stale (per the registry's own recency/run-id fields). The registry file itself is never embedded or reproduced in the output package — only the derived counts are — and its own path is exposed as a drill-down source via `governance_file_inventory.json`/`governance_evidence_map.json`, the same treatment `cross_segment_file_pairs.csv` already gets.

Implemented in the same PR as D-033 (shared optional-input plumbing and shared touch points in `main()`/`governance_package_health.json`), but tracked as a separate decision since the two serve different governance questions (completeness vs. reuse-breadth confidence) and could ship independently if one were descoped.

### Consequences
- `governance_package_health.json` gains a new optional-input entry and, when the registry is supplied, a `comparison_completeness` field.
- No existing classification or tier output changes when the flag is absent — this is strictly additive, matching every prior optional-input phase's discipline.
- A domain currently reading `Insufficient Evidence` due to a not-run/stale comparison (rather than genuinely weak convergence) becomes distinguishable from the outside for the first time — directly closing part of the D-031 "insufficient evidence is scope-specific" caveat with an actual mechanism, not just a documented caveat.

---

## D-033 — Governance narrative evidence-package layer (Phase 9b: union-inventory-derived domain confidence enrichment)

### Status
Accepted (2026-08-18)

### Context
`docs/governance_generator_cross_compare_coverage.md` marks this item "(Still open.)" in its own implementation sequence (step 3, "Domain confidence enrichment"): `cross_segment_union_inventory.csv` is currently only partially consumed (to count blocked project domains when manifest metadata exists), and its corpus-wide/project/client/file pattern-prevalence signal — which pairwise Jaccard cannot express — is not yet surfaced per domain. The doc's own framing: this identifies domains with broad natural reuse but weak formal cascade (a natural-standard candidate the cascade metrics alone would miss), or the reverse (narrow reuse despite strong formal cascade, worth flagging as fragile).

### Decision
Extend `governance_domain_summary.csv` with breadth columns derived from `cross_segment_union_inventory.csv` (corpus-wide/project-wide/client-wide/file-level reuse counts per domain, following the same naming convention as existing `_by_scope` fields). Render only the strongest narrative exceptions per the coverage doc's own guardrail — broad reuse with weak cascade, or narrow reuse with strong cascade — as a new anomaly-note category in `detect_anomalies()`, using the same policy-externalized-threshold discipline established in D-029 (thresholds for "broad," "weak," "narrow," "strong" in this context go into `anomaly_thresholds.json` alongside D-029's other additions, not as new bare literals).

Implemented in the same PR as D-032 (shared optional-input plumbing), tracked separately per the reasoning in D-032's Context.

### Consequences
- `governance_domain_summary.csv` gains new columns; existing columns and their values are unaffected.
- New anomaly-note category is gated by the same "only render the strongest exceptions" discipline the coverage doc specifies — this is a deliberate scope limit to avoid restating every domain's raw breadth numbers as narrative prose, matching the "consume, not recompute, and don't over-render" discipline established across D-020/D-022.
- Closes the last open item in `docs/governance_generator_cross_compare_coverage.md`'s implementation sequence — that doc's table should be updated to mark this row "Done" once shipped, matching its own existing convention for the other now-complete rows.

---

## D-034 — Governance narrative evidence-package layer (Phase 10: static-doc subfolder + self-contained package copy)

### Status
Accepted (2026-08-19)

### Context
The four static, package-type-level reference docs `generate_governance_narrative.py` points readers at by hardcoded path constant (`INTERPRETATION_GUIDE_PATH`/`QUESTION_ROUTES_PATH`/`READING_ORDER_PATH`/`CLASSIFICATION_RULES_PATH`, added across D-022/D-030/D-029) lived directly under `docs/` alongside ~15 unrelated technical-documentation files, with no grouping signal that they specifically belong to the governance narrative package. Separately, these docs were only ever referenced by name/pointer from the generated package (`governance_narrative_context.md`'s authority header, `governance_evidence_map.json`'s sibling-artifact entries) — never actually present alongside a run's output — so a `--out` directory handed to someone without the repo checked out contained pointers to files that reader could not open.

### Decision
Two changes, neither altering any classification, tier, or CSV/JSON field value:

1. Move the four docs into a new `docs/governance/` subfolder (filenames unchanged): `governance_interpretation_guide.md`, `governance_question_routes.md`, `governance_reading_order.md`, `governance_classification_rules.md`. `_DOCS_DIR` in `generate_governance_narrative.py` now points at `docs/governance/` instead of `docs/`; the four path constants are otherwise unchanged. Every other `docs/*.md` file (`governance_evidence_package.md`, `governance_generator_cross_compare_coverage.md`, `governance_narrative_scope_gap_audit.md`, `governance_narrative_group1_scope_gap_investigation.md`) stays in `docs/` — those are developer/design documentation about the system, never pointed at by a path constant or copied into a run's output, so they don't belong in the same "package-portable" subfolder.
2. `main()`, inside the existing `if args.emit_evidence_package:` block, now copies each of the four docs into `--out` (via `shutil.copy2`, only when the source doc is present) after building `sibling_paths`/`sibling_present`. Only these four are copied — never the CSV siblings the same block registers (`cross_segment_file_pairs.csv`, `comparison_registry.csv`, `pattern_reuse_summary_by_domain.csv`, `project_mean_file_pair_jaccard_matrix.csv`, `governance_relationships.csv`), which D-023/D-024/D-032 are explicit are never embedded or reproduced in the output package. `governance_evidence_map.json`/`governance_package_manifest.json`'s `path`/`present` fields for these four artifacts continue to describe the checked-in repo doc (the source of truth), not the copy — the copy is a portability convenience, not a second source of truth, and is silently skipped when the source doc is absent (e.g. a stripped-down deployment without `docs/`).

### Consequences
- No existing classification, tier, CSV column, or JSON field value changes — this phase moves files and adds a copy step, nothing else. Verified: `governance_domain_summary.csv`/`governance_client_summary.csv`/`governance_bc_summary.csv` are byte-identical before/after: only new files appear in `--out`.
- A `--out` directory is now self-contained: a reader with only that directory (no repo checkout) can open the four static reference docs the narrative and evidence map already point them at by name.
- `docs/governance_evidence_package.md` and `CLAUDE.md`'s live path references to the four docs are updated to the new location; `DECISIONS.md`/`CHANGELOG.md`'s own historical entries (D-019 through D-033) are left describing the paths that were true at the time, per this repo's append-only convention for those two files.
- A future fifth static package-type-level doc, if added, should default to `docs/governance/` and the same copy-into-`--out` treatment unless there's a specific reason not to.

## D-035 — Join-policy gate exemption for single-record-per-file domains (`units_doc`, `worksets_doc`)

### Status
Accepted (2026-08-19)

### Context
`units_doc` and `worksets_doc` are the two domains already flagged elsewhere as structurally mismatched to the pairwise join/comparison machinery the rest of the corpus uses (see the open backlog item: "single-record-per-file domains... right fix is a dedicated aggregate/distribution reporter; decision and DECISIONS.md entry not yet written"). `run_extract_all.py`'s join-policy gate (`_enforce_policy_gate()`, ~line 711) enforces `join_key_status == ok` across all in-scope domains before authority/patterns processing begins, and these two domains cannot satisfy that check by construction — a `record_pk` grain of one row per file has no meaningful pairwise join key to compute, so their `join_key_status` reads `blocked`/`non_ok_status` on every record, every run (confirmed 2026-08-19: 1,256 of 1,256 flagged rows across two prior diagnostics runs were both domains, 100% of their records, `join_key_status=blocked`).

Because `_enforce_policy_gate()` fires immediately after records load and before any authority/patterns output is written (confirmed by reading the call site, ~line 1017 — it precedes the analysis/pattern-mining work entirely), a gate failure here is a fast, silent, whole-run abort: no partial output, no `analysis/` directory, and — critically — no distinguishing console signal loud enough to notice if the run's tail isn't watched closely. This is exactly what happened: `units_doc`/`worksets_doc` were introduced and the corpus re-extracted, every subsequent Run B invocation hit this gate and exited in seconds, and the resulting absence of an `analysis/` directory was read as "hasn't gotten there yet" rather than "failing" for an extended, unknown period — only surfaced as a side effect of debugging an unrelated OneDrive relocation issue. The only reason it was caught is that a run finally succeeded (via `-AllowSigHashJoinKey`, added this session) and the runtime jump from "seconds" to "actually doing the work" made the change visible.

`--allow-sig-hash-join-key` unblocks the run, but it is a **global** escape hatch — it doesn't scope to the two known-exempt domains, so a real, unexpected join-key regression in an unrelated domain would be silently waved through by the same flag, undetected, for as long as the flag stays on. Relying on a human remembering to pass it by hand every Run B invocation is also exactly the failure mode that produced this incident in the first place.

### Decision
Scope the gate exemption to the two known domains, rather than either (a) leaving this as a manually-remembered flag, or (b) permanently defaulting the global bypass on. At the `_enforce_policy_gate()` call site(s) in `run_extract_all.py`, introduce a new constant — distinct from `SUPPRESSED_DOWNSTREAM_DOMAINS`, since that constant's effect is broader (excludes a domain from downstream processing generally) and this exemption should apply *only* to the gate check, not to authority/patterns output for these domains:

```python
# Single-record-per-file domains structurally cannot produce a pairwise join key
# (record_pk grain is one row per file) -- join_key_status=blocked is expected and
# permanent for these, not a data-quality regression. Exempt from the join-policy
# gate specifically; still fully processed by authority/patterns otherwise. See
# DECISIONS.md D-035.
JOIN_GATE_EXEMPT_SINGLE_RECORD_DOMAINS = {"units_doc", "worksets_doc"}
```

and filter the `domains` list passed into `_enforce_policy_gate()` at each call site (~1017, ~1204) to exclude this set before the gate evaluates it — `_emit_join_policy_diagnostics()`'s existing `dom_filter` inclusion-list mechanism already supports this without further code changes to that function.

Until this lands, `-AllowSigHashJoinKey` (added to `corpus_update_runbook.ps1` this session) remains the manual interim workaround, and this decision is the record of why it exists and why it shouldn't become the permanent answer.

This is explicitly a narrower, faster fix than the "dedicated aggregate/distribution reporter" — it stops the gate from silently killing the whole run over a known, structural, non-actionable condition; it does not give `units_doc`/`worksets_doc` a real pairwise-comparable governance signal, which remains the open, undecided problem the reporter would actually solve.

### Consequences
- Once implemented, Run B no longer requires `-AllowSigHashJoinKey` for the currently-known case, and the flag reverts to what it always should have been: a rare, visible, deliberate override for a genuinely new problem, not routine cover for a permanent one.
- A future domain that legitimately regresses into `blocked`/identity-mode status is still caught by the gate and still halts the run loudly — this fix narrows the exemption to exactly the two domains it's justified for, rather than widening tolerance generally.
- `analysis/` output for `units_doc`/`worksets_doc` continues to be produced using whatever degraded/identity-mode join behavior they've always structurally had — this decision does not change what that output means or whether it's governance-grade; that's still gated on the reporter work.
- If a third domain is ever found to have the same single-record-per-file structural property, it needs to be added to `JOIN_GATE_EXEMPT_SINGLE_RECORD_DOMAINS` explicitly — this is not auto-detected, by design, so a new case doesn't silently inherit an exemption it wasn't reviewed for.
- Worth a follow-up check once this lands: confirm `domain_patterns.csv`/`analysis/` row counts for `units_doc`/`worksets_doc` from *today's* run (the first successful one) against whatever the corpus should actually contain, since this is the first time this data has existed at all since those domains were introduced — there's no prior "last known good" version to diff against, only the extraction/export side to cross-check.

## D-026 — Audit reports remain historical, not operational

`audit_results/` is retained as clearly historical evidence for earlier releases.
Production correctness must be justified by maintained contracts, policies, code
comments, and tests; live tools must not require an audit file at runtime. The consolidation is complete: current contract rationale and implementation
explanations are maintained here and beside the relevant code; production modules,
tests, and operator runbooks no longer depend on an audit report for correctness.
Historical changelog and audit-to-audit links remain only where their targets exist.
The deterministic tracked-reference check is `scripts/check_audit_references.py`.

## D-037 — Consolidated name-projection and extractor rationale

### Status
Accepted (2026-08-20)

### Decision
The canonical name-identity projection has 7 native domains, 18 widened domains,
and 12 explicitly excluded domains. Native values already occur on the canonical
identity surface. Widened values come from a phase-2 bucket or undecorated
`label.display`/`label.components` value; they therefore are not evidence-equivalent
to a configuration join hash. The `phases` projection is intentionally marked
redundant. `core/name_key_coverage.py` is the maintained registry.

Bundle name-projection staging deliberately adapts its reduced schema at one
boundary. It emits empty CAD-import evidence, uses the synthesized pattern label as
the human label, and supplies one deterministic analysis run ID. Split export IDs
normalize from details to index names only when the known metadata IDs support that
choice. Provenance must continue to disclose that agreement evidence reconstructed
the inline calculation rather than validating a live re-extracted corpus.

Extractor domain differences are intentional only when encoded in maintained domain
code and covered by selector/shape-gating tests. Canonical selectors constrain broad
collector APIs; dimensions require subtype shape gates; system and import coercions
are explicit; and identity-item migration rebuilds the canonical flat item surface
rather than retaining the former monolithic representation.

### Consequences
- Historical audits 6–15 record how these conclusions were reached, but are not part
  of the contract.
- Tests assert current registries, schemas, provenance, selectors, and output paths
  directly; audit prose is never needed to interpret a pass or failure.
- Operators use maintained command help and runbooks. Historical audit shorthand is
  not an operational reference.

## D-036 — Enterprise artifact provenance and promotion schema v2

### Status
Accepted (2026-08-20)

### Decision
Every maintained artifact package whose interpretation depends on enterprise
identity accompanies its outputs with canonical `enterprise_policy.json`.
Sorted UTF-8 JSON excludes local paths and is published only after primary
artifacts. Validation and dry-run paths remain non-writing.

Promotion analysis now emits `reuse_client_pool_is_enterprise`. The
organization-specific predecessor is removed rather than retained as an alias:
no maintained external consumer requires it. Classification uses the effective
policy label, never the enterprise BC bookkeeping token alone. Existing BC-grain
limitations remain.

### Consequences
- Pre-v2 CSV consumers must explicitly rename the retired field.
- Policy overrides are reproducible beside identity-aware artifacts.
- Historical audit text remains historical; maintained prose/examples are neutral.

## D-038 — `mapping/` line_patterns Revit mapping utility (model-writing, join_hash-verified)

### Status
Accepted (2026-08-21)

### Decision
Introduce `mapping/`, a new top-level package separate from `core/`/`domains/`/
`runner/`/`tools/`, that reads the `tools/export_bundle_pattern_detail.py` CSV
triple (`bundle_pattern_inventory.csv`/`pattern_settings.csv`/`pattern_names.csv`)
and materializes representative `LinePatternElement` objects in the currently
open Revit document, for use in a mapping/configuration RVT consumed by
downstream governance tooling. This is the first behavior in the repository
that writes to a Revit document rather than only reading it.

Scope is locked to the `line_patterns` domain only. Every unique
`(domain="line_patterns", join_hash)` in `bundle_pattern_inventory.csv` is a
requested configuration; reconstruction from `pattern_settings.csv` blocks
(never infers) on any incomplete/inconsistent evidence. Verification against
the requested identity uses `join_hash` (the `line_patterns.join_key.v3`
policy value, D-017's `line_pattern.segments_norm_hash`-derived scale-invariant
identity) computed via the existing `core/join_key_builder.py` +
`policies/domain_join_key_policies.json`, never `sig_hash` (which remains
`line_pattern.segments_def_hash`-derived, exact-scale identity) -- these answer
different questions and are not interchangeable. Each requested configuration
is created inside its own `Autodesk.Revit.DB.Transaction`, read back, and
re-verified against the requested `join_hash` before commit; any mismatch or
exception rolls that one transaction back without affecting others. Mapping
elements are named `MAP__<observed_name>`, with a deterministic
`MAP__<observed_name>__<short_join_hash>` fallback on a name collision against
a *different* configuration; an existing nonmatching element is never modified
or replaced.

`line_pattern.segments_norm_hash` is computed synthetically by
`tools/run_extract_all.py`'s private `_append_line_pattern_synthetic_norm_hash()`
during the flatten stage and is not exposed as an importable function. Rather
than import it (and couple this Revit-writing utility to that CLI
orchestrator's machinery), `mapping/line_pattern_reconstruction.py` carries a
deliberately independent reimplementation of the same per-record algorithm --
the same "independent reimplementation over import" precedent
`tools/pattern_id_utils.py` already established for `tools/extractor.py`'s
private `_stable_pattern_id()`. A cross-check test
(`tests/test_line_pattern_mapping_reconstruction.py::test_segments_norm_hash_matches_run_extract_all_reference`)
asserts the two implementations agree over synthetic segment lists.

### Rationale
Existing extraction/analysis hash and join-key semantics are reused verbatim
(no hash-affecting change to any domain or policy). The only genuinely new
rules introduced are downstream-only and non-hash-affecting: a Revit
element-name sanitizer (`mapping/line_pattern_reconstruction.py::sanitize_revit_name`,
since nothing upstream previously needed to construct Revit-legal names from
arbitrary observed labels), and a defensive re-application of the existing
Dot-length-normalization rule (`domains/line_patterns.py` already forces Dot
segment length to `0.0` at extraction time; this utility re-applies the same
normalization to defend against a hand-edited/stale CSV, marking the result
degraded rather than blocked when it has to).

### Consequences
- No existing extraction, join-key, bundle-analysis, or
  `export_bundle_pattern_detail.py` behavior changes; `mapping/` is
  purely additive and downstream.
- `core/`, `domains/`, and `runner/` must never import from `mapping/`
  (dependency direction stays one-way, same as the existing
  Core -> Domains -> Context -> Runner rule).
- A subsequent fill-pattern (or other domain) mapping PR must redo its own
  domain-specific reconstruction/naming/verification; no shared
  "materialize-a-domain-into-Revit" abstraction was introduced here to avoid
  generalizing prematurely from a single domain.

## D-039 — `wall_types`/`floor_types`/`roof_types`/`ceiling_types` sig_hash policy: close the `sig_hash_keys` registry gap

### Status
Accepted (2026-08-24)

### Context
`tools/generate_sig_hash_policy.py` compiles each domain's
`policies/domain_sig_hash_policies.json` entry from
`contracts/domain_identity_keys_v2.json`. A domain block's `sig_hash_keys`
field, when present, overrides `allowed_keys` as the actual sig_hash preimage
set; when absent, the generator falls back to the full `allowed_keys` list
(`tools/generate_sig_hash_policy.py:24-30`). Several domains rely on this
override because their extractor hashes a narrower "semantic" subset of what
it exports to `identity_basis.items` — `object_styles_model/_annotation/
_analytical/_imported`, `worksets`, `worksets_doc`, `browser_organization`,
`line_patterns`, `materials`, and `text_types` all carry an explicit
`sig_hash_keys` override, several with a hand-written note explaining exactly
why (e.g. object_styles': "pinned ... so the Area 9 additions register as
identity_basis.items without widening the sig_hash preimage").

`wall_types`, `floor_types`, `roof_types`, and `ceiling_types` are the same
shape — each extractor (`domains/{wall,floor,roof,ceiling}_types.py`)
classifies captured fields into `semantic`/`coordination`/`cosmetic` Python
lists, exports `identity_items = sorted(semantic + coordination + cosmetic)`
to `identity_basis.items`, but computes its own inline `sig_hash` from
`semantic` only (3-6 keys per domain, excluding the type's own display name
and coarse fill color). These four domains had **no** `sig_hash_keys`
override in the registry and no note explaining a deliberate choice either
way — the compiled policy's `allowed_items` mechanically mirrored the full
`allowed_keys` list, meaning `core/sig_hash_builder.py`'s post-stage
recompute (`run_extract_all.py`'s `sig_hash` stage, which unconditionally
overwrites `records.csv`'s `sig_hash` column for any domain with a policy
entry) would hash `wt.type_name`/`ft.type_name`/`rt.type_name`/
`ct.type_name` and `*.coarse_fill_color_rgb` into `sig_hash` — reintroducing
exactly the "names are metadata only, never in behavior hashes" violation
this project's hashing rules exist to prevent (see D-002 and the top-level
Critical Rules). Absent any documented rationale, and given every sibling
domain with a genuine narrowing need already has one, this reads as an
un-reviewed gap from a mechanical registry regen, not a considered decision.

The gap was latent rather than actively corrupting governance output: the
actual clustering/pattern machinery (`tools/extractor.py`'s
`_stable_pattern_id()`) keys exclusively off `join_hash`, and the
*join-key* policies for these same four domains
(`policies/domain_join_key_policies.json`) already correctly carry
`explicitly_excluded_items: ["wt.type_name", "wt.coarse_fill_color_rgb"]`
(and the floor/roof/ceiling equivalents). The gap becomes live-and-wrong the
moment identity-mode fallback applies to one of these domains
(`join_key_schema == "sig_hash_as_join_key.v1"` — CLAUDE.md's own
documented degraded mode), since `_apply_sig_hash_to_phase0` sets
`join_hash = sig_hash` in that case: "sig_hash isn't the clustering key" is
one config state away from being false for these domains, not a stable
invariant to rely on.

`units`/`units_doc` were investigated under the same hypothesis and found
**not** to have this gap: `domains/units.py`'s actual sig_hash-driving
constant, `UNITS_SEMANTIC_KEYS` (11 items), already includes
`units.symbol_type_id`/`units.accuracy` and matches the compiled policy's
`allowed_items` exactly. A second, unused local variable also named
`semantic_keys`/`cosmetic_keys` inside `units.py`'s per-record loop excludes
those same two fields, but only feeds the informational
`phase2.cosmetic_items` dict (marked "Deprecated duplication path" in the
module's own comment) — it never affects `sig_hash`. That is a separate,
milder internal-consistency question about `units.py`'s own two competing
notions of "semantic" and is explicitly out of scope for this decision.

### Decision
Add an explicit `sig_hash_keys` override to each of the four
`contracts/domain_identity_keys_v2.json` blocks, matching exactly the
key set each extractor already hashes inline:
- `wall_types`: `wt.function`, `wt.layer_count`, `wt.total_thickness_in`,
  `wt.stack_hash_loose`, `wt.wraps_at_inserts`, `wt.wraps_at_ends`
- `floor_types`: `ft.layer_count`, `ft.total_thickness_in`,
  `ft.stack_hash_loose`
- `roof_types`: `rt.layer_count`, `rt.total_thickness_in`,
  `rt.stack_hash_loose`
- `ceiling_types`: `ct.layer_count`, `ct.total_thickness_in`,
  `ct.stack_hash_loose`

`policies/domain_sig_hash_policies.json`'s four corresponding entries were
hand-patched to the same narrower `allowed_items` (not regenerated via
`tools/generate_sig_hash_policy.py`, to avoid clobbering unrelated
hand-tuned notes already present on other domains in that file — the same
convention `object_styles_*`/`worksets`/`browser_organization` already
follow). `required_items`/`minima` are unchanged; `wt.kind`,
`wt.total_layer_rows`, `wt.stack_hash_strict`,
`wt.stack_hash_function_only`, `wt.coarse_fill_pattern_sig_hash`,
`wt.has_embedded_sweeps`, `*.type_name`, and `*.coarse_fill_color_rgb`
remain fully present in `identity_basis.items` (and therefore still visible
to `discover_hash_policy.py`/`discover_join_policy.py`'s pareto search and
to the correctly-narrow join-key policy) — only the sig_hash preimage
narrows.

This is a **hash-breaking correction** for these four domains: any two
records that previously produced different `sig_hash` values solely because
of a differing type name or fill color will now produce the same
`sig_hash`. `join_hash` (the value actually used for cross-file governance
comparison) is unaffected — the join-key policies were already correct.

### Consequences
- `records.csv`'s `sig_hash` column for `wall_types`/`floor_types`/
  `roof_types`/`ceiling_types` changes on any re-run of the `sig_hash`
  stage; a full corpus re-run of that stage (not full re-extraction — the
  underlying `identity_basis.items` evidence is unchanged) is required to
  pick this up.
- Closes the identity-mode-fallback exposure described above: if
  `join_key_schema` ever falls back to `sig_hash_as_join_key.v1` for one of
  these four domains, `join_hash` now inherits a name-free `sig_hash`
  instead of silently absorbing type names/colors into governance
  clustering.
- Does not address `units.py`'s own internal `semantic_keys`/
  `cosmetic_keys` inconsistency (see Context) — left as a separate, later
  question.
- Does not change the bucketing-vs-flat-emission architecture of these four
  extractors; they remain in the `semantic`/`coordination`/`cosmetic`
  pattern rather than moving to the `arrowheads.py`/`identity.py` flat
  `identity_items` pattern. That migration, if pursued, is a separate,
  larger initiative.

## D-040 — Policy-driven inline sig_hash resolution (`resolve_sig_hash_keys`); identity-items visibility promotions

### Status
Accepted (2026-08-25)

### Context
D-039 closed one instance of a structural risk it did not fully retire: every
bucketing domain (`wall_types`, `floor_types`, `roof_types`, `ceiling_types`,
`units`, `units_doc`, `worksets`, `worksets_doc`, `browser_organization`,
`object_styles_*`, `arrowheads`) hand-maintained its own inline "which fields
drive sig_hash" key set as a **hardcoded Python list/set**, entirely
independent of `policies/domain_sig_hash_policies.json`'s `allowed_items` for
that same domain — the exact JSON file `core/sig_hash_builder.py`'s
post-extraction stage reads. Two independently-maintained copies of "what
counts as behavioral" can drift apart with no mechanism to notice; D-039 was
one such drift, discovered and fixed, but the underlying two-copies
architecture that let it happen unnoticed was untouched. `tests/
test_sig_hash_join_key_policy_consistency.py` (added alongside D-039) guards
one drift *symptom* (sig_hash quietly re-including a field the domain's own
join-key policy already flagged as non-behavioral); it does not remove the
duplication that produces that symptom.

Separately, several fields were captured during extraction but only ever
reached a domain's `phase2.cosmetic_items`/`coordination_items`/
`unknown_items` bucket, never `identity_basis.items` — structurally
invisible to `discover_hash_policy.py`'s/`discover_join_policy.py`'s pareto
search (`tools/extractor.py`'s flatten stage reads only
`identity_basis.items`; the other three phase2 buckets never reach
`identity_items.csv`). A sweep across every domain's phase2-bucket
construction found five genuine cases (excluding the project's own
ElementId/UniqueId/name exclusion rule, which the sweep confirmed accounts
for the overwhelming majority of what's phase2-only): `arrowhead.record_class`
(a coordination classifier, arrowheads.py), `lp.is_import` (line_patterns.py),
`line_style.pattern_ref.synopsis` (a derived shape-summary string, not the
pattern's own Revit name; line_styles.py), `text_type.leader_arrowhead_sig_hash`
(a resolved cross-domain reference; text_types.py — already anticipated in
`contracts/domain_identity_keys_v2.json`'s `allowed_keys` before this change,
just never wired into the extractor), and `vt.assigned_view_count`
(view_templates.py). Everything else found in the sweep was either
ElementId/UniqueId/name (excluded per existing project convention) or a
constant BI-slicer literal (e.g. `obj_style.domain_family`,
`dim_type.domain_family`, `vt.view_type_family` — always the same value
within a given domain's export, so promoting it adds no information) —
deliberately left unpromoted.

While wiring `text_types.py` (the flat canonical-items pilot domain) into
this same mechanism, a **second, independent instance of the D-039 bug
class** surfaced: `TEXT_TYPE_SEMANTIC_KEYS` included `text_type.name`,
while `policies/domain_sig_hash_policies.json`'s compiled policy (and its
own `contracts/domain_identity_keys_v2.json` `sig_hash_keys` override)
already correctly excluded it — and a test
(`tests/test_text_types_canonical_selectors.py`) had encoded the mismatch as
apparently intentional ("semantic basis includes identity-bearing
name/background"). No D-0xx decision ever authorized this; unlike D-010's
documented phase-name exception, this reads as an unreviewed mistake. See
D-041 below for the fix and its scoped impact (text_types.py already omits
`sig_hash`/`join_key` from its own extractor output entirely in "canonical
mode" — see the module's own comment — so this affects the domain's
`hash_v2` aggregate rollup, not a per-record `sig_hash` field, which this
domain never emits at extraction time regardless).

### Decision
1. Add `resolve_sig_hash_keys(policies, domain_name, fallback)` to
   `core/sig_hash_policy.py`: resolves a domain's sig_hash preimage key set
   from `ctx["sig_hash_policies"]`'s compiled `allowed_items` when present,
   falling back to the caller-supplied hardcoded default otherwise (e.g. a
   unit test building a minimal `ctx` by hand, or a call site that hasn't
   adopted the new `ctx` key). `runner/extraction_context.py` now loads
   `policies/domain_sig_hash_policies.json` into `ctx["sig_hash_policies"]`
   at the same point it already loads `join_key_policies`/`name_key_policies`,
   so a live Revit extraction run has it populated end to end.
2. Convert `wall_types`, `floor_types`, `roof_types`, `ceiling_types`,
   `units`, `units_doc`, `worksets`, `worksets_doc`, `browser_organization`,
   `object_styles` (all 4 partitions, keyed correctly per-partition off
   `domain_name` — `object_styles_model`/`_annotation`/`_analytical`/
   `_imported` each have their own distinct policy entry, not one shared
   "object_styles" key), `arrowheads`, and `text_types` to call
   `resolve_sig_hash_keys()` instead of filtering by a hardcoded
   list/set directly. The hardcoded lists are retained as the `fallback`
   argument (renamed with a `_SIG_HASH_KEYS_FALLBACK` suffix where they
   weren't already suitably named) — not deleted — so behavior is identical
   when `ctx["sig_hash_policies"]` is absent, but a future *intentional* and
   *reviewed* change to the JSON policy now propagates automatically to the
   inline extractor too, rather than requiring someone to remember to
   hand-edit two independent copies (exactly the step that got missed for
   D-039). `arrowheads.py`'s conversion is a no-op against today's values
   (already flat emission — hashes its whole `identity_items` list, and its
   policy already matched that set exactly).
   `tests/test_sig_hash_policy_builder.py::test_resolve_sig_hash_keys_real_policy_file_matches_hardcoded_fallbacks`
   asserts every one of these fallbacks equals the real compiled policy, so a
   future silent divergence between the two fails a test immediately instead
   of waiting to be discovered.

   Explicitly **not** touched: each domain's own required/degraded/blocked
   status computation. `core/sig_hash_builder.py`'s `build_sig_hash_from_policy`
   has different (stricter) status semantics than several domains' hand-rolled
   logic — e.g. `wall_types.py` currently reports `status=ok` even when a
   non-required semantic field (`wt.wraps_at_inserts`) is unreadable, which
   `build_sig_hash_from_policy` would report as `degraded`
   (`tests/test_compound_types_wall.py::test_unreadable_wrap_fields_do_not_block_required_identity`
   pins this). Unifying status semantics across the two code paths is a
   distinct, larger question, deliberately out of scope here — this decision
   only unifies *which fields get hashed*, not *how completeness/blocking is
   judged*.
3. Promote the five genuinely-hidden fields identified above into their
   domain's `identity_basis.items` (additively — each domain's sig_hash
   fallback/policy key set does **not** include the newly-added key, so no
   domain's `sig_hash` value changes from this promotion alone).
   `arrowhead.record_class`'s promotion specifically required item 2's
   conversion first (arrowheads hashes its entire `identity_items` list, so
   adding a new key without first switching to policy-filtered hashing would
   have been hash-breaking); the other four were safe to promote without any
   hash-computation change since each domain already filters its hash
   preimage to a narrower key set that doesn't include them.
4. Fix `text_types.py`'s `TEXT_TYPE_SEMANTIC_KEYS` (renamed
   `TEXT_TYPE_SEMANTIC_KEYS_FALLBACK`): removed `text_type.name`. Fixed the
   corresponding assertion and misleading comment in
   `tests/test_text_types_canonical_selectors.py`. See D-041.

### Consequences
- `records.csv`'s `sig_hash` column is unaffected by this decision for
  every domain listed in item 2 (values are unchanged; only the source of
  the key set moves from Python-hardcoded to ctx-provided-with-hardcoded-
  fallback). A full corpus re-run of the `sig_hash` stage is **not** required
  for this decision alone (contrast D-039, which was hash-breaking).
- `identity_basis.items` gains one new key for each of the five promoted
  fields, on every future extraction of arrowheads/line_patterns/
  line_styles/text_types/view_templates records. `discover_hash_policy.py`/
  `discover_join_policy.py` can now see these fields; whether policy
  actually selects them for sig_hash/join_key remains a separate, future,
  evidence-based decision — this change only grants visibility.
- Going forward, any new domain wired into `resolve_sig_hash_keys()` gets
  the single-source-of-truth property automatically; a domain not yet
  wired in (anything not listed in item 2) still carries the D-039-class
  drift risk until it is converted. This decision does not convert every
  remaining bucketing domain — only the ones already touched by D-039 plus
  `text_types`/`arrowheads` (the domains already under active work this
  session).
- `tests/test_compound_types_wall.py`, `tests/test_compound_types_floor.py`
  (new), `tests/test_compound_types_roof.py` (new),
  `tests/test_compound_types_ceiling.py` (new),
  `tests/test_units_canonical_selectors.py`,
  `tests/test_worksets_sig_hash_policy.py` (new),
  `tests/test_browser_organization_sig_hash_policy.py` (new),
  `tests/test_object_styles_sig_hash_policy.py` (new),
  `tests/test_arrowheads_sig_hash_policy.py` (new), and
  `tests/test_view_templates_assigned_view_count.py` (new) each include a
  test that points `ctx["sig_hash_policies"]` at a deliberately different
  key set and confirms the resulting `sig_hash` changes accordingly —
  proving the ctx-wiring is load-bearing, not just present and ignored.

## D-041 — `text_types` sig_hash: remove `text_type.name` from the hashed key set

### Status
Accepted (2026-08-25)

### Context
See D-040's Context section for how this was found (incidentally, while
converting `text_types.py` to `resolve_sig_hash_keys()`). `TEXT_TYPE_SEMANTIC_KEYS`
included `text_type.name`; `policies/domain_sig_hash_policies.json`'s
`text_types` entry (compiled from `contracts/domain_identity_keys_v2.json`'s
`sig_hash_keys` override, which already existed and already excluded
`text_type.name`) did not. No decision record authorizes hashing this
domain's own display name — unlike D-010's phase-name inclusion, which is a
documented, deliberate exception. A test
(`tests/test_text_types_canonical_selectors.py`) had asserted the divergent
behavior as expected, with a comment describing the name as
"identity-bearing" — read together with the complete absence of any D-0xx
entry for it, this looks like an implementation-time mistake that was never
caught, not a considered choice that just lacked paperwork.

Actual impact is narrower than a typical sig_hash bug for this domain
specifically: `domains/text_types.py` is the canonical flat-items pilot and
its own extractor code explicitly omits `sig_hash`/`join_key` from its
*exported* record entirely ("join_key/sig_hash are intentionally omitted
from extractor output in canonical mode; they are post-extraction
artifacts" — the module's own comment). The locally-computed `sig_hash_v2`
this decision corrects is used for exactly one thing inside the extractor:
contributing to `info["hash_v2"]`, the domain-level rollup hash summarizing
"did anything change across all text_type records in this file" — it is
not the per-record `sig_hash` ultimately used for governance/pattern
clustering (that value is computed later, entirely from the JSON policy,
which was already correct).

### Decision
Remove `text_type.name` from `TEXT_TYPE_SEMANTIC_KEYS_FALLBACK` (the
renamed fallback constant; see D-040). `domains/text_types.py`'s inline
`sig_hash_v2` computation now excludes the type's own display name,
matching `policies/domain_sig_hash_policies.json`'s (and
`contracts/domain_identity_keys_v2.json`'s `sig_hash_keys` override's)
pre-existing, correct 12-key set exactly. Fixed the misleading
"identity-bearing name" comment and added an explicit
`"text_type.name" not in TEXT_TYPE_SEMANTIC_KEYS_FALLBACK` assertion to
`tests/test_text_types_canonical_selectors.py` so this cannot silently
regress.

### Consequences
- **Hash-breaking for `text_types`' domain-level `hash_v2` rollup only**:
  two text-note-type records that previously produced different `hash_v2`
  contributions solely because of a differing type name now contribute the
  same value. No exported per-record `sig_hash` field changes, because this
  domain does not emit one at extraction time (see Context).
- No change to `identity_basis.items` (the name was never removed from
  there — `text_type.name` remains fully captured and exported, per D-004/
  the general "names are metadata only" pattern of keeping the name as
  identity evidence while excluding it from the hash).
- Consistent with every other domain's "names are metadata only, never in
  behavior hashes" rule; the domain no longer has a code-level exception
  that was never actually decided.

## D-042 — `object_styles`: remove `obj_style.row_key` from the sig_hash key set

### Status
Accepted (2026-08-25)

### Context
Found by an automated PR review (Codex bot) on the D-040 PR, immediately
after D-040 wired `resolve_sig_hash_keys()` into `runner/extraction_context.py`
via `ctx["sig_hash_policies"]`. `contracts/domain_identity_keys_v2.json`'s
`sig_hash_keys` override for all four `object_styles_*` domains
(`object_styles_model`, `_annotation`, `_analytical`, `_imported`) already
included `obj_style.row_key` — pre-dating this session's work, not
introduced by it — and `policies/domain_sig_hash_policies.json`'s compiled
`allowed_items` carried it forward. `domains/object_styles.py`'s own inline
`_MODEL_SEMANTIC_KEYS`/`_NON_MODEL_SEMANTIC_KEYS` constants, which the
extractor actually hashes against today, do **not** include `row_key` and
never have.

`obj_style.row_key` is `"{parent_name}|{row_name}"` — derived entirely from
`Category.Name`/subcategory `Category.Name`, i.e. a name, not a behavioral
property. Including it in `sig_hash` means two subcategories with
identical graphic overrides (color, weights, pattern, material) but
different names would never cluster as the same pattern — the opposite of
what `sig_hash` exists to do (D-002/D-010's naming/behavior distinction).
No `DECISIONS.md` entry authorizes this domain's `sig_hash_keys` override
including `row_key`; the misleading state was invisible before D-040
because the inline extractor's own hardcoded key set didn't include it and
nothing routed the JSON policy's `allowed_items` back into that inline
computation to expose the mismatch. My own drift-guard test added in D-040
(`test_resolve_sig_hash_keys_real_policy_file_matches_hardcoded_fallbacks`)
had the same gap — it never checked any `object_styles_*` domain — so it
did not catch this either; that gap is fixed as part of this decision.

A companion, independent finding from the same review pass
(`core/sig_hash_policy.py`'s `resolve_sig_hash_keys()`): the function's
original 3-argument design (a) treated a validated, non-empty-but-legitimate
empty `allowed_items` list as "policy absent" and silently fell back to the
hardcoded default instead of honoring the empty set, and (b) never resolved
`allowed_item_prefixes` at all, unlike `core/sig_hash_builder.py`'s
`_key_allowed()` (the post-stage recompute's reference implementation),
which checks both an exact membership test and a prefix test. No currently-
wired domain relies on `allowed_item_prefixes` for its sig_hash policy today,
so this had not yet produced an observable wrong hash, but it would have
recreated the exact D-039-class drift the moment a domain using prefixes
(e.g. a future `view_filter_definitions`-style `"vf.rule["` policy) adopted
this resolver.

### Decision
1. Remove `obj_style.row_key` from the `sig_hash_keys` override in
   `contracts/domain_identity_keys_v2.json` for all four `object_styles_*`
   domains (keeping it in `required_keys`, since it is still required
   canonical identity evidence for `identity_basis.items`/`join_key` — only
   its presence in the *sig_hash* preimage is wrong). Removed the same key
   from `policies/domain_sig_hash_policies.json`'s compiled `allowed_items`
   for the same four domains, with a note citing this decision.
2. Fix `tests/test_object_styles_canonical_selectors.py`, which had
   asserted the sig_hash includes `row_key` (matching the buggy policy, not
   the extractor's real behavior) — it now filters by
   `domains.object_styles._MODEL_SEMANTIC_KEYS` before hashing, with an
   explicit `"obj_style.row_key" not in _MODEL_SEMANTIC_KEYS` assertion.
3. Add all four `object_styles_*` domains to
   `test_resolve_sig_hash_keys_real_policy_file_matches_hardcoded_fallbacks`'s
   checks (D-040's drift guard), closing the coverage gap that let this
   ship in the first place.
4. Redesign `resolve_sig_hash_keys()`'s signature from
   `(policies, domain_name, fallback)` to
   `(policies, domain_name, candidate_keys, fallback)`. `candidate_keys` is
   the record's own identity-item keys at the call site (e.g.
   `[it["k"] for it in identity_items]`), used to resolve
   `allowed_item_prefixes` into concrete matches exactly as
   `core/sig_hash_builder.py`'s `_key_allowed()` does. A policy-validated
   empty `allowed_items` (with no prefix matches) now correctly resolves to
   an empty key set rather than falling back to the hardcoded default —
   only a missing/malformed policy triggers fallback. Updated every call
   site (`wall_types`/`floor_types`/`roof_types`/`ceiling_types`, `units`
   (x2), `worksets` (x2), `browser_organization`, `object_styles`,
   `arrowheads`, `text_types`) to pass `candidate_keys`. `object_styles.py`'s
   call moved from before its per-category loop to inside it (after
   `identity_items_sorted` is built for that category), since prefix
   resolution needs the actual per-record item keys, not a value computed
   once outside the loop.

### Consequences
- **Hash-breaking for `object_styles_model`/`_annotation`/`_analytical`/
  `_imported`'s exported `sig_hash` field**, but only in the direction of
  correcting a name-derived value that should never have been present:
  any two categories/subcategories whose graphic-override behavior was
  identical but whose `row_key` differed will now (correctly) share the
  same `sig_hash`. This affects the analysis-side `sig_hash` stage
  (T0.5, policy-driven recompute) on any future re-run; it does **not**
  affect the extractor's own inline `sig_hash_v2` (`_MODEL_SEMANTIC_KEYS`/
  `_NON_MODEL_SEMANTIC_KEYS` never included `row_key`, so already-exported
  JSON is unaffected — see the extraction-vs-re-extraction analysis
  discussed with the user; no Revit re-extraction is required for this fix).
- `resolve_sig_hash_keys()`'s signature change is a breaking API change
  for all ~12 call sites introduced in D-040, all updated in this same
  change; no caller is left on the old 3-arg form.
- `join_hash`/`identity_basis.items` are unaffected — `row_key` remains
  required canonical identity evidence and part of the join-key policy,
  which was already correct and independent of this bug.

## D-043 — Register the five D-040 identity-visibility promotions in the domain key registry; report only used keys in `sig_basis`

### Status
Accepted (2026-08-25)

### Context
Found by automated PR review (Codex bot) on the D-042 PR, immediately after
that PR pushed. D-040 promoted five fields from phase2-only buckets into
`identity_basis.items` (`arrowhead.record_class`, `lp.is_import`,
`line_style.pattern_ref.synopsis`, `text_type.leader_arrowhead_sig_hash`,
`vt.assigned_view_count`) so they'd be visible to
`discover_hash_policy.py`'s/`discover_join_policy.py`'s pareto search. That
PR's own description called this "purely additive," and it is for
`sig_hash`/`join_key` — but `contracts/domain_identity_keys_v2.json`'s
per-domain `allowed_keys` is a **closed** registry: `validators/record_v2.py`'s
`validate_record_v2()` rejects any `identity_basis.items` key not in a
domain's `allowed_keys` (or matching an `allowed_key_prefixes` entry) with
`identity.key.not_allowed:<key>`. Three of the five promotions
(`arrowhead.record_class`, `lp.is_import`, `vt.assigned_view_count`) were
never added to their domains' registry entries, so every real extraction
producing these records would fail strict contract validation the moment
anything actually calls `validate_record_v2()` against them — which nothing
in the test suite did, for any of arrowheads/line_patterns/view_templates,
which is exactly why this shipped unnoticed across two PRs. (The other two
promotions, `line_style.pattern_ref.synopsis` on `line_styles` and
`text_type.leader_arrowhead_sig_hash` on `text_types`, were already present
in those domains' registries beforehand — no gap there.)

A second, independent finding from the same review pass: `sig_basis.keys_used`
(introduced alongside each domain's inline `sig_hash` as audit metadata
describing which keys fed the hash preimage) was, in several domains,
populated from the domain's full resolved/fallback key set rather than the
keys actually present on that specific record. For `arrowheads.py` this was
observably wrong: `resolve_sig_hash_keys()`'s `allowed_items` is the union
across all record classes (Arrow ∪ Tick ∪ common), but a `SizeOnly` or
`Unknown` record's `identity_basis.items` never contains the Arrow- or
Tick-specific keys at all (`class_items = []` for those classes) — so its
`sig_basis.keys_used` claimed keys were hashed that were never present on
the record, making the audit trail unable to reproduce or explain the hash.
The same hardcoded-metadata pattern (reporting a fixed key list/constant
rather than the keys actually used) was present, though not currently
observably wrong given today's data, in every other domain converted to
`resolve_sig_hash_keys()` in D-040/D-042: `wall_types`, `floor_types`,
`roof_types`, `ceiling_types`, `units`/`units_doc`, `worksets`/`worksets_doc`,
`browser_organization`, and `object_styles` (all 4 partitions).

### Decision
1. Add the three missing keys to their domains' `allowed_keys` in
   `contracts/domain_identity_keys_v2.json`: `arrowhead.record_class`
   (`arrowheads`), `lp.is_import` (`line_patterns`), and
   `vt.assigned_view_count` (all 5 `view_templates_*` partitions).
2. For `arrowheads` and the 5 `view_templates_*` partitions, which had no
   prior `sig_hash_keys` override (so `tools/generate_sig_hash_policy.py`
   would otherwise fall back to the full, now-widened `allowed_keys` on any
   future regeneration — the exact D-039 mechanism), add an explicit
   `sig_hash_keys` override reproducing today's actual preimage set (the
   pre-existing 9 arrowhead keys; `view_template.def_hash` alone per
   partition) so the newly-visible key cannot silently widen sig_hash on a
   future policy regeneration. `line_patterns` already had a correct
   `sig_hash_keys` override excluding `lp.is_import`, so no change was
   needed there. `policies/domain_sig_hash_policies.json` required no edits
   — the new overrides reproduce values already compiled there.
3. Fix `sig_basis.keys_used` in every domain listed above to report the
   keys actually present in the filtered item list that fed the hash
   preimage (e.g. `sorted(it["k"] for it in sig_hash_items)`), not a
   hardcoded literal, a module-level constant, or the raw resolved
   `allowed_items` union. `arrowheads.py`'s `sig_hash_items` (previously
   scoped only inside its non-blocked branch) is now initialized
   unconditionally so `keys_used` is always well-defined; likewise
   `worksets.py`'s and `browser_organization.py`'s `semantic_items`.
4. Added regression coverage that was structurally absent before this:
   `test_arrowhead_record_class_passes_contract_validation`,
   `test_lp_is_import_passes_contract_validation`, and
   `test_vt_assigned_view_count_passes_contract_validation_for_every_partition`
   each build a record.v2 including the promoted key and assert
   `validate_record_v2()` returns no violations against the real registry —
   none of these three domains had any test exercising that validator
   before. `test_sig_basis_keys_used_reflects_only_keys_present_for_record_class`
   proves a `SizeOnly` arrowhead's `sig_basis.keys_used` excludes
   Arrow/Tick-specific keys it never had as items.

### Consequences
- **Not hash-breaking anywhere.** `sig_hash`/`join_key`/`join_hash` values
  are unchanged for every domain touched — this decision only fixes contract
  validation (a real record that was silently invalid becomes valid) and
  audit-metadata accuracy (`sig_basis.keys_used` now describes what was
  actually hashed).
  `sig_basis.keys_used` values change for every domain listed in Decision
  item 3, but only in cases where they previously diverged from the actual
  keys used, which is scoped to records where a resolved/fallback key isn't
  present on that specific record (currently only demonstrated for
  `arrowheads`' non-Arrow record classes; a no-op everywhere the sets already
  coincided).
- Real exports of `arrowheads`/`line_patterns`/`view_templates_*` records
  produced since D-040/D-041 landed (this PR's own commits, not yet released)
  were never actually run through `validate_record_v2()` in production, so no
  externally-visible breakage occurred — this closes the gap before it could.
- Going forward, any new key promoted into `identity_basis.items` for
  visibility only (not affecting `sig_hash`/`join_key`) must be added to the
  domain's registry `allowed_keys` in the same change, and, if the domain has
  no `sig_hash_keys` override, an explicit override must be added at the same
  time to freeze the current preimage set — this is the same discipline
  D-039/D-042 established for the opposite direction (an
  already-registered key silently entering `sig_hash`); this decision closes
  the other direction (a newly-visible key never being registered at all).

## D-044 — `text_types`: distinguish "no leader arrowhead" from "unresolved leader-arrowhead reference"

### Status
Accepted (2026-08-25)

### Context
Found by automated PR review (Codex bot) on the D-043 PR. D-040 promoted
`text_type.leader_arrowhead_sig_hash` into `identity_basis.items` with a
hardcoded `q=ITEM_Q_OK` whenever the computed value was `None`, carried over
unchanged from the pre-existing phase2 `unknown_items` bucket convention
("Tri-state: q=ok always -- v=None is the explicit 'no leader arrowhead'
state"). That convention is correct for a text type that genuinely has no
leader arrowhead assigned. But `leader_arrow_sig_hash` also comes back
`None` in a second, distinct case: a leader arrowhead **is** assigned (a
valid `LEADER_ARROWHEAD` parameter resolves to a real element with a
`UniqueId`), but its `sig_hash` couldn't be resolved --
`ctx["arrowheads_by_type_id"]` lacks that type's entry (that arrowhead's own
record was blocked, the map wasn't populated, or `extract()` was called
without the dependency wired at all) or the lookup itself raised. Before
this decision, both cases produced identical `v=None, q=ok` items in
`identity_basis.items` -- an unresolved dependency was indistinguishable
from an explicit "none" state. This is exactly the kind of state-collapse
the project's Fail-Soft Policy forbids ("NEVER silently collapse distinct
states... Unreadable/inaccessible data MUST emit explicit markers"). No test
in the suite exercised `domains/text_types.py`'s `extract()` with a leader
arrowhead actually present, which is why this shipped across two PRs
unnoticed -- `tests/test_text_types_conversion_convergence.py`'s existing
extractor-level test hardcodes `first_param` to always return `None`, so the
leader-arrowhead branch was never taken.

### Decision
Track two additional booleans through the leader-arrowhead read in
`domains/text_types.py`: `leader_arrow_ref_present` (a valid element was
actually resolved from the `LEADER_ARROWHEAD` parameter) and
`leader_arrow_lookup_unreadable` (the `ctx["arrowheads_by_type_id"]` lookup
itself raised, as opposed to simply not containing the entry). At the
`identity_basis.items` promotion site: if no reference is present, keep the
existing `v=None, q=ok` ("explicit none," unchanged). If a reference is
present but its sig_hash didn't resolve, emit `q=ITEM_Q_UNREADABLE` when the
lookup raised, else `q=ITEM_Q_MISSING` (dependency map absent or missing the
entry) -- both with `v=None`. The pre-existing phase2 `unknown_items` bucket
emission (`_phase2_build_payload()`, driven off the already-flattened `rec`
dict, not this same code path) is unchanged; it's presentation/traceability
metadata, not authoritative identity evidence, and correcting it wasn't part
of this finding. Added `tests/test_text_types_leader_arrowhead_quality.py`,
which exercises `extract()` with a real (mocked) leader-arrowhead element
across all three states -- none / resolved / unresolved -- closing the
coverage gap that let this ship.

### Consequences
- **Not hash-breaking.** `text_type.leader_arrowhead_sig_hash` is not in
  `TEXT_TYPE_SEMANTIC_KEYS_FALLBACK` / `policies/domain_sig_hash_policies.json`'s
  `text_types` `allowed_items` (D-041), and is not in `required_keys`, so
  `sig_hash`, `status`, and `identity_quality` (which is computed only from
  `required_qs` per `core/record_v2.py`'s `compute_identity_quality()`) are
  all unaffected by this item's `q` value. This is purely an evidence-quality
  correction on one non-required, non-hashed item.
  `identity_basis.items` now surfaces the real state for any text type whose
  leader arrowhead couldn't be resolved (going forward, on any extraction run
  where `ctx["arrowheads_by_type_id"]` is present but incomplete, or absent
  entirely, for a text type that does reference a leader arrowhead) -- these
  records previously reported a misleading "no leader arrowhead" state for
  what is actually an unresolved dependency.

## D-045 — Bump `sig_hash_schema` to `.v2` for the four compound-type domains and four object_styles partitions

### Status
Accepted (2026-08-25)

### Context
Found by automated PR review (Codex bot) on the D-044 PR (two separate
comments, same root issue). D-039 narrowed `wall_types`/`floor_types`/
`roof_types`/`ceiling_types`'s `sig_hash` preimage from the full
`identity_basis.items` set down to a pinned semantic subset (excluding type
name and fill color). D-042 removed the name-derived `obj_style.row_key`
from all four `object_styles_*` partitions' `sig_hash` preimage. Both are
correct, intentional, hash-breaking fixes -- but both left
`sig_hash_schema` unchanged at `.v1` in `contracts/domain_identity_keys_v2.json`
and the compiled `policies/domain_sig_hash_policies.json` (for
`object_styles_*`, there was no explicit `sig_hash_schema` at all;
`tools/generate_sig_hash_policy.py`'s fallback `"%s.sig_hash.v1" % name`
supplied it). `sig_hash_schema` exists specifically so that a `sig_hash`
value's preimage definition can be identified without recomputing it --
an old export produced before D-039/D-042 and a freshly recomputed one
produced after both carry the same schema string despite hashing different
fields, so a comparison tool (or a person) has no signal that the two
`sig_hash` values are not comparable, and no schema-version-gated migration
path can distinguish "value computed under the wide preimage" from "value
computed under the narrow one."

### Decision
Bump `sig_hash_schema` from `.v1` to `.v2` for all eight affected domains,
in both `contracts/domain_identity_keys_v2.json` (the source of truth) and
`policies/domain_sig_hash_policies.json` (the compiled artifact, hand-patched
to match per the established convention for this file): `wall_types`,
`floor_types`, `roof_types`, `ceiling_types`, `object_styles_model`,
`object_styles_annotation`, `object_styles_analytical`,
`object_styles_imported`. For the four `object_styles_*` domains, this also
adds an explicit `sig_hash_schema` field to the registry for the first time
(previously relying on `generate_sig_hash_policy.py`'s implicit `.v1`
fallback), so future regenerations no longer depend on that fallback for
these domains.

### Consequences
- No change to any `sig_hash` **value** -- this decision only changes the
  schema label attached to those values. The actual preimage narrowing was
  already completed and hash-breaking under D-039/D-042; this closes the
  versioning gap those decisions left open.
- Any tooling or stored artifact that keyed off the literal string
  `"wall_types.sig_hash.v1"` / `"floor_types.sig_hash.v1"` /
  `"roof_types.sig_hash.v1"` / `"ceiling_types.sig_hash.v1"` /
  `"object_styles_{model,annotation,analytical,imported}.sig_hash.v1"` will
  see `.v2` going forward -- this is the intended signal that a `sig_hash`
  computed under the old (wider) preimage is not comparable to one computed
  under the current (narrower) preimage without recomputation. No code in
  this repository parses or branches on the schema string's version suffix
  today (confirmed via full-suite pass with no assertions on the literal
  value), so this is safe to bump without an accompanying migration.
