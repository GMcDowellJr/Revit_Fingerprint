# Governance Reading Order

`reading_order_version: 0.1`

This is a **stable, package-type-level reading sequence** for a
`revit_fingerprint_governance` evidence package (the outputs of
`tools/generate_governance_narrative.py`). It is not regenerated per run.
See `docs/governance/governance_interpretation_guide.md`'s "What this package is for"
section for this package's full audience/intent statement — in short: it is
written for a reader who does not need Revit domain knowledge, who
understands operational tradeoffs, and who is meant to **ask** governance
convergence/fragmentation questions, not decide standards unassisted.

An earlier design considered an ordinal `read_priority: 1, 2, 3...` field on
each artifact instead of this doc. It was rejected: an ordinal invites a
reader to sort, read the top few, and reason from a partial picture while
still technically "following the order." This package has no query/
tool-calling path — a reader cannot fetch more context once reasoning
starts, only work from what is already in front of it — so the guardrail
that matters is completeness, not sequence polish. See `DECISIONS.md` D-030.

---

## Read this before drawing conclusions

Two known-bad-inference entries in `docs/governance/governance_interpretation_guide.md`
are called out here explicitly, not just by pointer, because both are
confirmed live in the current corpus, not hypothetical:

- **"Insufficient Evidence" is scope-specific, not package-wide.** A
  domain's enterprise-scoped tier reading `Insufficient Evidence` does not
  mean the domain has no usable evidence anywhere in the package — check
  `governance_client_summary.csv`, `governance_bc_summary.csv`, and the
  domain's `cross_client_convergence` field before concluding nothing is
  known about it.
- **"Region" and "Enterprise" currently read identically, and will continue
  to until the corpus changes.** All corpus files currently come from one
  region. If/when a `region` segmentation dimension is added, region-level
  and enterprise-level results will be identical by construction until a
  second region's data exists — this reflects current data coverage, not
  completed cross-region standardization.

See `docs/governance/governance_interpretation_guide.md`'s "Known bad inferences"
section for the full list (these two plus eight others).

---

## The ordered path

1. **Health check** — `governance_package_health.json`. Establishes whether
   this run's data is trustworthy before reading anything derived from it:
   schema detection, used-view fallback, comparison_type coverage, which
   optional inputs were present.
2. **Evidence map / interpretation guide orientation** —
   `governance_evidence_map.json` (what exists, what each artifact can and
   cannot answer, and its `reasoning_prerequisites` list) and
   `docs/governance/governance_interpretation_guide.md` (what the metrics and tiers
   mean, comparability rules, authority ordering, known bad inferences).
3. **Brief** — `governance_brief.md`, a capped top-line digest of
   `governance_findings.json` for a quick read.
4. **Domain/client rollups** — `governance_domain_summary.csv`,
   `governance_client_summary.csv`, `governance_bc_summary.csv`: the
   primary tier/score evidence a governance conclusion is actually drawn
   from.
5. **Narrative prose** — `governance_narrative_context.md`, the
   human-readable synthesis of the rollups above with tier labels and
   framing.
6. **Question routes** — `docs/governance/governance_question_routes.md`, if there is
   a specific recurring question rather than a general read.
7. **File inventory** — `governance_file_inventory.json`, only if deeper
   drill-down into a file this generator does not itself read is needed.

---

## The machine-checkable version of this order

`governance_evidence_map.json`'s top-level `reasoning_prerequisites` field
is the machine-checkable counterpart to this document: the full list of
`artifact_id`s whose per-artifact `required_before_conclusions` is `true`.
Treat it as **a set to exhaust, not a sequence to sample from** — reading
some of the artifacts named there is not the same as reading all of them,
and this document's ordering is a reading aid, not a substitute for
checking that every prerequisite has actually been incorporated. Nothing in
this package enforces that a reader does this — the gate only works if a
reader, human or LLM, actually checks it, consistent with every other
guardrail in this package being self-checkable rather than enforced.
