# Fingerprint API Semantic Mapping Research

## Purpose

This behavior-preserving research artifact maps the current emitted Revit Fingerprint domain inventory to Revit 2025 API semantic concepts and records candidate dependency relationships for future review. It does **not** change extraction behavior, canonicalization, hashes, schemas, comparison outputs, or downstream reports.

## Inputs

- Current emitted domain inventory from `runner/run_dynamo.py` `_enabled(...)` domain routing.
- Full Revit 2025 API semantic graph from `domains/graph_2025.json`.
- Current semantic projection criteria used for this review: persistent element/value concepts in the 2025 graph, with transient/API-infrastructure concepts excluded.
- Existing extractor modules under `domains/` for current representation checks.

## Method

1. Parsed runner domain routing and recorded each emitted domain exactly once.
2. Mapped each domain to zero or more API concepts while preserving project-owned domain identifiers.
3. Reviewed candidate relationship families against extractor behavior and recorded explicit evidence states.
4. Classified 105 excluded core-confidence graph edges without treating heuristic communities as domains, archetypes, governance roles, or evidence of RVT usage.
5. Documented feasible runtime collection approaches only as future implementation notes.

## Output Status

- `fingerprint_api_concept_map.csv`: degraded; complete domain coverage with explicit zero-concept/unresolved handling.
- `candidate_dependency_edges.csv`: degraded; candidate rows complete, but all remain research candidates.
- `unsupported_api_concepts.csv`: ok; explicit exclusions documented.
- `semantic_label_review.csv`: ok; 105 rows classified exactly once.

## Output Descriptions

- `fingerprint_api_concept_map.csv` maps each current fingerprint domain to API concepts, semantic-core membership, unresolved concepts, and extractor references.
- `candidate_dependency_edges.csv` records candidate relationships, current representation state, API evidence, collector feasibility, target resolution, signature verification state, and semantic validation state.
- `unsupported_api_concepts.csv` records explicit exclusions for API-only, runtime, transient, operational, and heuristic concepts.
- `semantic_label_review.csv` classifies the 105 excluded core-confidence edges while preserving distinct signature, target-resolution, semantic-validation, and fingerprint-relevance states.

## Edge Classification Totals

- intentionally_irrelevant_to_persistent_file_analysis: 41
- relevant_but_outside_current_semantic_node_set: 29
- core_projection_drops_useful_fingerprint_concept: 35
- total: 105

## Major Candidate Dependency Families

- Host object types to compound structures, compound layers, and materials.
- Materials to fill patterns and appearance assets.
- Categories/graphics styles/object styles/line styles to line patterns and materials.
- Family symbols to families.
- View filters to categories and view templates to applied filters.
- View templates to phase filters and category override targets.

## Unresolved Questions

- Whether appearance assets should become a first-class fingerprint domain or remain a supporting relationship target.
- Whether category should remain supporting topology or become a shared target inventory.
- Which usage relationships are valuable enough to justify behavior-changing output additions.
- How to version future dependency-edge output without changing existing domain hashes.

## Exclusions

API vocabulary enums, utilities, collectors/iterators, options classes, event/event-argument classes, built-in failure catalogs, transient geometry objects, import/export runtime objects, transaction infrastructure, API query filters, analysis result wrappers, UI/command classes, and heuristic graph communities are explicitly excluded unless a later PR defines a persistent-file exception.

## Recommended Implementation Order

1. Current-domain cross-domain dependencies.
2. Current definition-to-usage relationships.
3. Evidence-supported new domain candidates.
4. Targeted supporting topology.

Each implementation stage must be a separate behavior-changing PR or PR series.

## Behavior Preservation

No fingerprint extraction, canonicalization, serializers, schemas, domain versions, hashes, comparison outputs, downstream reports, collectors, or runtime behavior changed in this PR.
