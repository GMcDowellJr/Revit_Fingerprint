# Discovery and validation policy semantics

## Verification findings

Before this change, both discovery entry points normalized the current policy and
constructed `gates` with its required fields for every policy mode. The join tool
did so in `discover_join_policy.main`; the hash tool repeated the same construction
in `_run_target`. `discover_greedy` then seeded its reported selection from that
gate, while `build_candidate_join_key_with_details` preferred
`gates.required_fields` to the candidate subset. Per-shape
`shape_requirements.additional_required` values were also appended by that builder.
Consequently ordinary discovery was policy-seeded, and shape-gated discovery also
scored ratified per-shape requirements. The callable Pareto search had equivalent
baseline seeding. A candidate labelled `C` could therefore be evaluated as the
policy baseline, or as the baseline plus gated fields, rather than as `C`.

The regression fixture in `tests/test_discovery_policy_semantics.py` makes the
distinction deterministic: the policy field is constant, candidate `C` separates
the signatures, and an Alpha-only ratified field also separates them. Candidate
mode reports and scores only `C`; runtime mode reports the base field and exposes
the Alpha field in `effective_fields_actually_scored`.

## Mode contract

- **discover** uses the canonical emitted item-key surface minus explicit policy
  exclusions. A loaded policy contributes exclusions, descriptive context, and a
  discriminator used for partitioning. Required, optional, and per-shape required
  fields do not seed candidate scoring.
- **validate** retains the governed required/optional search surface and applies
  base and matching per-shape runtime requirements. Effective fields are emitted
  separately from the selected contender so gated fields cannot be hidden.
- **harsh** retains its established meaning: a broad challenger search seeded by
  the required/optional policy baseline and extended with discovered fields. It is
  not stricter validation. This PR preserves that behavior.

For policies with a discriminator, join discovery now emits the common/global row
and independent per-discriminator-value rows. The discriminator provenance is
`existing_policy`; its configured `additional_required` fields are not used in
those partition searches.

## Diagnostic example

Given runtime required field `A`, Alpha additional required field
`alpha_specific`, and challenger `C`:

| mode | selected_fields | effective_fields_actually_scored |
| --- | --- | --- |
| discover (before) | policy-seeded/misleading candidate selection | `A\|alpha_specific` |
| discover (now) | `C` | `C` |
| validate (now) | `A` | `A\|alpha_specific` |

Real-corpus before/after examples can only be generated when flattened
`records.csv` and canonical item CSVs are available. This checkout contains no
representative flattened corpus, so no synthetic result is presented as real
evidence.

## Change note

Discovery previously evaluated candidates with existing policy-required and gated
fields implicitly added. Discovery now evaluates candidate evidence independently,
while validation retains policy constraints. Users should expect some selected
fields and collision/fragmentation metrics in discovery diagnostics to change.
Runtime sig and join fingerprints are not changed by this change.

This change does not establish or promote domain policy decisions. Extractor
coverage, optional-item schema questions, and any evidence absent from canonical
identity items remain out of scope.

## Sweep entry point and cache compatibility

The normal entry point remains `python run_discovery_sweep.py --run` (or the
repository-root PowerShell wrapper). Those wrappers delegate to
`tools.discovery_orchestrator`, which continues to invoke the corrected join and
sig discovery stage tools. Because this change alters discovery evidence, the
orchestrator engine version is bumped to `discovery-sweep-v3`; cached v2 evidence
is not compatible with these semantics.
