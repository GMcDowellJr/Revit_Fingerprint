# Discovery candidate eligibility audit

## Data flow and finding

Domain extractors build `identity_basis.items` and may also retain legacy
phase-2 cosmetic, coordination, and unknown buckets. Canonical migration uses
`core/canonical_items.py::canonicalize_record`, which deliberately unions all
of those buckets into one flat `items` envelope. Flattening writes that evidence
to `identity_items.csv` and its completed per-domain shards. Both discovery
drivers count every distinct `item_key` in those files, cap by observed
frequency, and previously removed only the governed policy's
`explicitly_excluded_items` before passing the list to greedy/Pareto. Thus a
traceability or routing item promoted into the flat evidence envelope could
compete even though its original phase-2 role was non-identity.

The new diagnostic-only sequence is: exposed flattened evidence; centralized
eligibility classification; explicit alias suppression; governed discovery
exclusions; unchanged greedy/Pareto evaluation. Runtime required/optional
membership is reported but does not establish the discovery surface.

## Classification scheme

The registry supports global exact-leaf rules where semantics are universal,
global/domain exact exclusions, and explicit canonical-to-alias declarations.
Only `source_element_id` and `source_unique_id` exact leaves are globally
classified as traceability identifiers. There is no substring `id` heuristic,
so stable fields such as `presentation_id` and `sorting_parameter_id` remain
eligible. Unlisted evidence remains broadly discoverable as
`semantic_candidate`; future uncertain semantics should be recorded as
`unknown_requires_domain_review` without exclusion.

Arrowhead `record_class` is routing metadata. `arrowhead.style` is canonical;
the raw integer and display values are explicitly suppressed aliases. The
registry is not a runtime fingerprint policy.

## Arrowhead extractor coverage follow-up (not changed)

`Fill Tick` is read before style routing. `Dot` is in
`STYLE_BUCKET_SIZE_ONLY`; that branch assigns `class_items = []`, so
`arrowhead.fill_tick` is not placed in Dot's canonical `identity_items` and
discovery cannot evaluate it. The same branch suppresses all read
style-specific values: width angle, fill tick, arrow closed, tick-mark
centered, and heavy-end pen weight. Whether any are definition-bearing for Dot
or another size-only style requires a separate extractor-coverage change.

## Regeneration

Run `python tools/audit_discovery_candidates.py [--phase0-dir PATH]`. Static
extractor declarations are unioned with flattened corpus evidence when supplied;
the deterministic outputs are `discovery_candidate_inventory.csv` and
`discovery_candidate_inventory_summary.json`. Static discovery covers both
identity-item constructor calls and dictionary-based `{"k": ...}` declarations.
Items in multi-domain extractor modules are attributed using governed policy
membership and the enclosing record builder's emitted domain, rather than the
module filename.

Eligibility is applied to the complete frequency-ranked field list before
`--max-candidate-fields` is enforced. Consequently, an excluded high-frequency
traceability field cannot consume a cap slot that should be available to the
next eligible semantic field.

Because eligibility changes the ranked candidate count used for sizing,
regenerate `discovery_param_suggestions.csv` with
`tools/suggest_discovery_params.py` before the next sweep. Existing suggestions
are not runtime fingerprint truth, but retaining pre-eligibility sizing can
overstate candidate-pool and Pareto search costs. Sweep cache fingerprints
include the registry's global and per-domain rules, so changing eligibility
rules invalidates affected cached evidence automatically.

> Discovery previously allowed any exposed, non-policy-excluded item to compete, which permitted traceability identifiers, routing metadata, and duplicate representations to become candidate fingerprint fields. Discovery now applies an explicit, observable candidate-eligibility layer before greedy/Pareto evaluation. Runtime fingerprint policies and hashes are unchanged. Some discovery selections and metrics may change because non-semantic shortcuts no longer compete.
