# Investigation: Group 1 (`tc`/`cp`/`tp`) scope-gating gap in `build_cascade()`

**Status:** Historical investigation — findings partially implemented; consult current code and tests for active behavior. No code was changed as part of this document.
**Scope:** `tools/generate_governance_narrative.py` — `_is_unscoped_segment()`, the
Group 1 dispatch block in `build_cascade()`, and `assign_tier()`. Group 2
(`gt`/`gc`/`gp`) and its `_by_scope` machinery are read only as a reference
pattern, not modified. `compare_cross_segment.py`, `build_segment_manifest.py`,
and all contract/policy files are treated as correct producer-side inputs and
are not touched.

**Data caveat:** No real `segment_manifest.csv` or `cross_segment_summary.csv`
exists in this repository or in this session's environment — they are pipeline
outputs generated at runtime, not checked in, and none was supplied with this
task despite the prompt referring to an "uploaded" copy. This is the same
caveat the prior scope-gap audit (`docs/governance_narrative_scope_gap_audit.md`,
Step 0) already recorded for the same files. Question 5's exact fraction
therefore **cannot be computed from real data** in this pass. What follows
instead is a structural argument, backed by reading the actual producer code
that emits Group 1 rows, plus the exact numbers from PR #354's own synthetic
test fixtures — sufficient to answer "would this matter" and "is the fix shape
right," but not "exactly how many of the 32 domains move."

---

## 1. `_is_unscoped_segment()` — current behavior (confirmed against its test file)

`_is_unscoped_segment(row, suffix)` (`generate_governance_narrative.py:255-296`)
returns `True` only when, for the given side (`"a"` or `"b"`):
- `governance_role_{suffix}` is non-empty, **and**
- `client_label_{suffix}` is empty, **and**
- `discipline_label_{suffix}` is empty, **and**
- every pipe-separated token in `segment_id_{suffix}` after the first two
  (`unit_system|role`) is either absent or literally blank (rejects a hidden
  `business_center_label`/`collection_label` scope token; accepts a trailing
  empty token from `build_segment_manifest.py`'s `_subset_to_id()`).

`tests/test_generate_governance_narrative_unscoped_segment.py` (7 tests, all
currently passing per the code) confirms exactly this: broadest segments and
trailing-blank-token segments pass; bc-scoped, collection-scoped,
client-scoped, and blank-role rollup segments all fail. There is no
"loosen to accept matching-bc" path anywhere in the function today — it is a
strict enterprise-or-nothing predicate, used identically by every call site.

## 2. Group 1 dispatch block — confirmed no fallback path

```python
if ct == "template_to_container" and _is_unscoped_segment(r, "a") and _is_unscoped_segment(r, "b"):
    ...
elif ct == "container_to_project" and _is_unscoped_segment(r, "a") and _is_unscoped_segment(r, "b"):
    ...
elif ct in ("template_to_project", "parent_sibling_roles") and _is_unscoped_segment(r, "a") and _is_unscoped_segment(r, "b"):
    ...
```
(`generate_governance_narrative.py:600-622`)

All three branches require **both** sides to pass `_is_unscoped_segment`. A row
that fails either side's check is silently dropped — there is no `_by_scope`
capture, no fallback bucket, nothing. This is exactly Group 2's pre-PR#354
shape (Option A only), confirmed against `b72d27a`'s own inline comment: *"gt/gc/gp
... deliberately keep the SAME single-broadest-pair gating as tc/cp/tp anyway
... matching Group 1's existing 'one clean enterprise-wide number' semantics."*
Group 1 was the **precedent** Group 2 was designed to match, not the other way
around — and Group 1 itself was never revisited once Group 2 got Option C.

## 3. `assign_tier()` — confirmed `primary` derivation and downstream use

```python
tc, cp, tp = d["tc"], d["cp"], d["tp"]
primary = tp if tp is not None else cp
...
if primary is None:
    return TIER_INSUFFICIENT
```
(`generate_governance_narrative.py:1015-1020`)

`primary` is a single scalar used for every subsequent threshold check in the
function (`RELIABILITY_SPARSE` gate, `local_active` gate, the `>= 0.90` /
`>= 0.75` / `>= 0.55` tier bands, and the nested `tc < 0.60` container-gap
check at `primary >= 0.90`). Nothing downstream is scope-aware — `assign_tier`
has no concept of "which scope did this number come from." A domain reaches
`TIER_INSUFFICIENT` today whenever **both** `tp` and `cp` are `None`, which
(per §2) happens whenever no `template_to_project`/`parent_sibling_roles` row
*and* no `container_to_project` row has both sides passing
`_is_unscoped_segment` — regardless of how much scoped (bc/client/discipline)
evidence exists for that domain.

## 4. Group 2 "Option C" precedent (read-only reference)

`_target_scope_label()` (`generate_governance_narrative.py:299-322`) classifies
a row's target side into `"enterprise"` / `"client"` / `"bc"` / `"discipline"` /
combinations / `"other_scoped"` using the real `client_label_x` /
`business_center_label_x` / `discipline_label_x` columns (no `segment_id`
parsing for scope classification — `_is_unscoped_segment` is reused only for
the `"enterprise"` case).

`build_cascade()`'s Group 2 branches (`generate_governance_narrative.py:678-715`)
keep the reference (Generic) side gated to `_is_unscoped_segment(r, "a")` —
unchanged — but classify the **target** side with `_target_scope_label(r, "b")`
and always append into `gt_by_scope[dom][scope]` / `gc_by_scope[...]` /
`gp_by_scope[...]`; only when `scope == "enterprise"` does the value *also* land
in the flat `gt`/`gc`/`gp` list that feeds the headline number. So:
- The headline (`gt`/`gc`/`gp`, and by extension what a Group-1-equivalent
  `tc`/`cp`/`tp` fix would keep unchanged) stays exactly what it is today —
  Option A, single enterprise-vs-enterprise pair.
- Every other scope level is captured, not discarded, in the `_by_scope` dict.
- `render_generic_baseline_scope_section()` renders one row per
  `(domain, scope)` actually observed, explicitly labeled — never silently
  blended into the enterprise number (`generate_governance_narrative.py:2015-2059`).
- `detect_anomalies()`'s scope-divergence loop (`generate_governance_narrative.py:1174-1191`)
  flags a ≥0.25 absolute gap between the enterprise reading and the mean of the
  scoped buckets, in either direction, per cascade stage — again without
  touching the headline value itself.

This is a clean, already-shipped, already-tested pattern
(`tests/test_generate_governance_narrative_scope_breakdown.py`, 12 tests) that
Group 1 could mirror exactly.

## 5. Does the producer actually emit scoped Group 1 rows the same way it emits scoped Group 2 rows?

This is answerable from `compare_cross_segment.py` even without real CSVs,
because it's a question about code structure, not data values.

`discover_within_segment()` (`compare_cross_segment.py:1796-1842`) groups
manifest segments **by `parent_segment_id`** — which includes bc-scoped,
client-scoped, and discipline-scoped parents, not only the one enterprise-wide
parent — and for each parent's role-bucketed children emits
`generic_to_template`/`generic_to_container`/`generic_to_project` **and**
`template_to_project`/`template_to_container`/`container_to_project`
pairs from the exact same loop, with no special-casing between the two groups.

A second, broader discovery block (`compare_cross_segment.py:2029-2087`) groups
by `by_key(row) = (dim, client, us)` — i.e. explicitly by client/discipline/
collection scope key — and again emits all six comparison types
(`generic_to_*` and `template_to_container`/`container_to_project`/
`template_to_project`) from one shared loop over `by_key.items()`.

**Conclusion: there is no structural reason Group 1 would have less scoped-row
coverage than Group 2.** Both groups are emitted by the identical producer
loops over the identical scope groupings. Whatever fraction of
`generic_to_template` rows PR #354 found landing in non-enterprise scope
buckets (its own test fixture models enterprise/client/discipline/bc all
populated for one domain — not a real-data measurement, but illustrative of
what the producer is *capable* of emitting), the same shape of coverage should
be structurally present for `template_to_container`/`container_to_project`/
`template_to_project` too, since one shared code path produces both.

The one caveat: whether **most real segments in a given corpus have siblings
sharing a common bc/client/discipline scope grain that also all pass the
role-bucketing** is a population-shape question, not a code-shape question,
and genuinely requires real `segment_manifest.csv` to answer with a number
("what fraction of `container_to_project`/`template_to_project` rows are
same-bc-both-sides vs. one-side-only vs. client/discipline-scoped instead").
That data does not exist in this repository or session, so this can only be
flagged as the open empirical question for whoever runs this fix against real
output — not answered here.

---

## Findings vs. the four questions asked

**Q1. How many of the 32 `TIER_INSUFFICIENT` domains would move if Group 1 adopted Option C using only same-bc-both-sides pairs as the "next best" primary?**

Cannot be computed without real `cross_segment_summary.csv`/`segment_manifest.csv`
row counts (none exist in-repo or in this session). Structurally: a domain
moves out of `TIER_INSUFFICIENT` under this fix *only if* it currently has
`tp is None and cp is None` (both enterprise-pair gated out) *and* has at
least one bc-scoped-both-sides `container_to_project` or
`template_to_project`/`parent_sibling_roles` row. Given §5's finding that the
producer emits scoped Group 1 rows via the same mechanism as Group 2, this set
is very unlikely to be zero, but an exact count needs the real data this
investigation doesn't have access to.

**Q2. Does `assign_tier()`'s single `primary` need to become scope-aware, or is widening `_is_unscoped_segment`'s Group 1 usage to accept matching-bc pairs sufficient without touching `assign_tier()`?**

Widening `_is_unscoped_segment` usage directly is the wrong shape and should
not be done — see the recommendation below (widening the predicate itself vs.
building `_by_scope` dicts are different fixes with different blast radii, and
the predicate itself is also used to gate the Generic/reference side in Group
2 and the bundle-share block at `generate_governance_narrative.py:788`,
so silently changing what it accepts would change more than Group 1).
`assign_tier()` does **not** need to become scope-aware if the fix instead
mirrors Group 2 exactly: keep `primary` derived only from the enterprise-only
`tc`/`cp`/`tp` (Option A, unchanged), and let a *separate* bc-pooled fallback
value (e.g. `tp_bc_pooled`, populated only when `tp`/`cp` are both `None`) be
consulted by `assign_tier` as an explicit, clearly-labeled fallback tier
distinct from `TIER_INSUFFICIENT` — see recommendation (b) below for the exact
shape.

**Q3. Do `detect_anomalies()` or `render_domain_tiers()` assume `tc`/`cp`/`tp` are enterprise-only in a way a bc-pooled fallback would violate?**

Yes, in wording. `render_domain_tiers()`'s table header is literally
`T→Container | T→Project | C→Project` (`generate_governance_narrative.py:1958,
1963`) with no scope qualifier, and `detect_anomalies()`'s notes speak in
absolute terms — *"Templates propagate weakly into coordination files (T→C =
{pct(tc)})"*, *"Coordination-file-to-project cascade is weak (C→P =
{pct(cp)})"* (`generate_governance_narrative.py:1199-1208`) — with no
indication the number could be a bc-pooled mean rather than one clean
enterprise figure. If a bc-pooled value were ever written into `d["tc"]`/
`d["cp"]`/`d["tp"]` directly (rather than a separate field), both the column
header and every one of these notes would silently misrepresent a pooled
number as the enterprise one — exactly the "blend-distinct-scope-grains"
anti-pattern the file's own A2/A3 fixes were written to eliminate. This is the
strongest argument for *not* widening `_is_unscoped_segment`'s Group 1 usage
in place, and for keeping any bc-pooled number in a distinctly-named field
with its own column/wording if it's ever rendered.

**Q4. Was Group 1 deliberately deferred from the Option C treatment, or simply out of scope?**

Simply out of scope, not a rejected idea. `git log` shows the sequence clearly:
- PR #350 (`0fca970`) fixed `is_generic`/positional parsing and introduced
  `_is_unscoped_segment`, applying the same Option-A gating to **both** Group 1
  and Group 2 identically — Group 1 was the existing pattern, Group 2 was
  written to match it.
- `b72d27a` ("docs: record Group 2 scope-blending decision and defer per-scope
  breakdown") explicitly deferred Option C **for Group 2 only** — its own text
  says gt/gc/gp "keep the same single-broadest-pair gating as tc/cp/tp," i.e.
  Group 1 is cited as the reason to *keep* Option A for Group 2, not flagged as
  something that also needs revisiting.
- PR #354 (`cb0231e`, `3cec980`) implemented Option C — but its commit
  messages, docstrings, and tests (`test_generate_governance_narrative_scope_breakdown.py`)
  are scoped entirely to "gt/gc/gp" / "generic->template/container/project."
  Group 1 (`tc`/`cp`/`tp`) is not mentioned anywhere in either commit as
  considered-and-rejected.

No entry in `DECISIONS.md` or `CHANGELOG.md` discusses Group 1 scope-gating at
all. This is a real, unaddressed gap that was never in scope for the PR #350 →
PR #354 sequence — not a documented decision to leave it as-is.

---

## Recommendation

**(a) Fix shape:** Build `tc_by_scope`/`cp_by_scope`/`tp_by_scope`, mirroring
Group 2's `gt_by_scope`/`gc_by_scope`/`gp_by_scope` exactly — **not** widening
`_is_unscoped_segment`'s Group 1 usage in place. Reasons:
- `_is_unscoped_segment` is a shared predicate (also gates the Generic
  reference side in Group 2 and the bundle-share accumulation block); loosening
  it changes semantics for callers that were never asked about.
- Widening it to "accept matching-bc pairs" would make it return `True` for
  two *different* things (genuinely enterprise-wide, and bc-scoped-on-both-
  sides) with no way for a caller to tell which one it got — reintroducing
  exactly the blend-distinct-scope-grains defect A2/A3 already fixed elsewhere,
  and the Q3 rendering hazard above.
- The `_by_scope` dict shape is proven (12 passing tests), keeps the reference
  side's own gating untouched, and requires touching only the three Group 1
  `elif` branches plus the new dict wiring — the same footprint PR #354 already
  used for Group 2.

  Concretely: in each of the three Group 1 branches
  (`generate_governance_narrative.py:600-622`), classify `a` and `b`
  independently via a Group-1-appropriate scope label (both sides matter here,
  unlike Group 2 where only the reference side is gated), bucket into
  `tc_by_scope[dom][scope_pair]` etc., and only promote into the flat
  `tc`/`cp`/`tp` lists when both sides are `"enterprise"` — unchanged from
  today.

**(b) What `assign_tier()` should treat as `primary`:** Do **not** make
`primary` itself scope-aware by blending a pooled number into it. Instead, add
a distinct fallback path: when `tp is None and cp is None` (today's
`TIER_INSUFFICIENT` trigger) but a same-bc-both-sides pooled value exists in
`tp_by_scope`/`cp_by_scope`, introduce a new, explicitly-named tier — e.g.
`TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE` — rather than silently reclassifying
into an existing tier that implies enterprise-level evidence. This preserves
`TIER_STRONG_BASELINE`/`TIER_INVESTIGATE`/etc.'s existing meaning (enterprise-
level primary) while still surfacing "there is bc-level evidence, just not
enterprise-level" instead of a bare "Insufficient Evidence." This mirrors how
Group 2 kept `gt`/`gc`/`gp` as Option A untouched and added a parallel
`_by_scope` view rather than redefining what `gt` means.

**(c) Rendering implications:** `render_domain_tiers()`'s column headers
(`T→Container | T→Project | C→Project`) must stay reserved for the enterprise-
only number — never silently repointed at a bc-pooled mean, per the Q3
finding. If a domain lands in the new fallback tier, its row should either
show the pooled value in a clearly-labeled additional column (e.g. "T→Project
(bc-pooled)") or, more consistently with the Group 2 precedent, leave the
`T→Project`/`C→Project` cells as `—` and let a new
`render_group1_scope_section()` (mirroring `render_generic_baseline_scope_section()`)
carry the per-`(domain, scope-pair)` detail, with `detect_anomalies()` gaining
a Group-1 analog of the existing scope-divergence note.

**(d) Estimate of domains moved:** Not computable in this pass — no real
`cross_segment_summary.csv` or `segment_manifest.csv` exists in this
repository or session (same caveat the prior scope-gap audit already
recorded). What can be said with confidence from code alone (§5): the
producer emits scoped Group 1 rows via the identical discovery functions and
groupings that emit scoped Group 2 rows, so there is no structural reason to
expect Group 1's scoped-row coverage to be smaller than Group 2's — meaning
this fix is very unlikely to move zero domains, but the exact count out of the
32 currently in `TIER_INSUFFICIENT` requires running the fix against a real
export.
