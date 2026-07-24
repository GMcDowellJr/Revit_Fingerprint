# Audit 7 — PR2 Item 0 (inline/analysis-side agreement) and Item 3 (CLI flag naming)

Date: 2026-07-23
Scope: Findings-before-code for PR2 (Identity-Projection-Parameterized Analysis). Both
items below are gating decisions the rest of the PR (pattern-generation parameterization)
depends on, per the PR2 brief's "Deliverable" section — reported here, before the
pattern-generation refactor, as required.

## Item 0 — inline vs. analysis-side agreement check

**Result: N=25, matched=25, match_rate=1.0 (100%).**

No Revit-extracted corpus exists in this environment or repo (extraction requires the
Revit API, only available inside Dynamo; no `*.details.json`/`*.index.json` exports are
checked into this repo, and no Revit host is available to trigger a fresh extraction).
Per the brief's own fallback ("if none exist yet, for a small manually-triggered
re-extraction... confirm... matches"), the check was instead performed by reading every
domain's actual "Canonical Name Identity Projection (PR1)" inline call site in
`domains/*.py` and independently reconstructing what it computes, then comparing that
independent reconstruction against `core/name_key_builder.build_name_key_for_record()`
(the analysis-side path) run against a synthetic record built in the same shape a real
export would produce. This is implemented as
`tests/test_name_key_inline_analysis_agreement.py`, parametrized over all 25 eligible
domains (one representative record per domain, one per coverage class:
7 native + 5 phase2-bucket-widened + 13 label-only-widened = 25).

Confirmed by direct code inspection (verbatim across every domain file): every inline call
site is

```python
build_join_key_from_policy(
    domain_policy=name_key_pol,
    identity_items=name_key_items,   # identity_items [+ one make_identity_item(...) for widened domains]
    include_optional_items=False,
    emit_keys_used=True,
    hash_optional_items=False,
    emit_items=False,
    emit_selectors=True,
)
```

— i.e. the same function (`core/join_key_builder.build_join_key_from_policy`) with the
same kwargs that `core/name_key_builder.py` also calls. The only place the two paths could
actually diverge is in what items list gets passed in: inline builds it from raw
Revit-sourced values at extraction time; analysis-side reconstructs it by merging
`identity_basis.items` + phase2 buckets (`core.canonical_items.build_flat_items`), falling
back to `label.display`/`label.components` for the domains with no phase2 bucket item
(`core/name_key_builder.py`'s `LABEL_ONLY_NAME_KEYS`). The test drives both constructions
from the same underlying raw value per case and asserts `join_hash` equality — a genuine
two-path check, not a tautology (a wiring bug in either path's item-widening logic would
show up as a mismatch here).

All 25 cases pass with zero mismatches. **No discrepancy found — the rest of this PR
proceeds on the assumption (now verified, not assumed) that the analysis-side
`join_key_name_identity` value is trustworthy.**

Caveat carried forward explicitly: this is a code-level agreement check (does the wiring
compute the same thing from the same inputs), not a live-corpus data check (do real
exported records from a real Revit model actually round-trip identically end to end). The
brief anticipated exactly this contingency ("if none exist yet... for a small
manually-triggered re-extraction") — a live-corpus re-check is recommended the first time a
real Revit re-extraction against the current `domains/*.py` is available, but is not
blocking for this PR, which (per its own scope) consumes only the analysis-side path
against already-exported data regardless.

## Item 3 — CLI flag naming

**Decision: `--comparison-target`, values `config` / `name` / `both`.**

Per the brief's instruction not to reuse `--identity-basis` (collides in vocabulary with
the contractual `identity_basis` record field) and to check for naming collisions against
the current `tools/` flag inventory: grepped every `add_argument` call across `tools/*.py`
and `tools/**/*.py` for `target`/`basis`-shaped flag names. Existing flags:

- `tools/discover_hash_policy.py --discovery-target {join,sig,both}` — closest sibling
  precedent (purpose-scoped noun + `both`-shaped enum), but names a different axis
  (candidate-family discovery, not join-basis selection) and would be misleading to reuse
  verbatim for this PR's axis.
- `tools/population_framing.py --coverage-target` — unrelated (a float threshold, not an
  enum).
- `tools/archetype/discover_vfd_edges.py --bip-hints` (target only in the default path
  string) and `tools/archetype/prepare_archetype_review.py --cluster-id` (docstring says
  "Target cluster_id") — neither is a real naming collision.

No existing flag is named `--comparison-target`, `--join-target`, or anything that would
collide. `--comparison-target` is adopted (over `--join-target`) because it reads correctly
for the `both` value — "compare join_hash vs. join_key_name_identity" is a comparison
between the two projections, not a single join being targeted twice.
