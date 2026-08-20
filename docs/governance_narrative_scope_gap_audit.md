# Investigation: `generate_governance_narrative.py` — Scope-Level Gap & Hardcoding Audit

**Status:** Historical investigation — findings partially implemented; consult current code and tests for active behavior. No code was changed as part of this document.
**Scope:** `tools/generate_governance_narrative.py` vs. `tools/compare_cross_segment.py`
as of this audit's HEAD.

**Data caveat (Step 0):** No real `segment_manifest.csv`, `cross_segment_summary.csv`,
`cross_segment_pooled.csv`, or `cross_segment_governance_state_summary.csv` exist in
this repository — they are pipeline outputs, generated at runtime, not checked in.
Every claim below is therefore backed by one of:
(a) direct reading of the current source of `generate_governance_narrative.py` and
`compare_cross_segment.py`, or
(b) concrete `segment_id` / row examples pulled from `tests/test_build_segment_manifest.py`
and `tests/test_compare_cross_segment_governance.py`, which exercise the real
segment-ID-construction and comparison-type-generation code paths.
Where a claim depends on production data casing/values that aren't present in this
repo, that is flagged explicitly as unconfirmed rather than asserted.

---

## A. Comparison-type coverage gap

### A1. `build_cascade()` comparison-type enumeration

**Checked:** Every `pairs.append((..., "<comparison_type>"))` call site in
`compare_cross_segment.py` (`discover_within_segment`, the enterprise/bc/client
scope-level fan-out block, `discover_sibling_segments`, `discover_parent_siblings`),
diffed against the `if/elif` chain in `build_cascade()`
(`tools/generate_governance_narrative.py:322-388`).

**Full set of comparison types the producer can emit**, by source function:

| Source | Types |
|---|---|
| `discover_within_segment` | `generic_to_template`, `generic_to_container`, `generic_to_project`, `template_to_project`, `template_to_container`, `container_to_project` |
| Scope-level fan-out (`compare_cross_segment.py:2100-2151`) | `enterprise_to_project`, `bc_to_project`, `enterprise_to_bc`, `enterprise_to_client` |
| `discover_sibling_segments` | `sibling_templates`, `sibling_projects`, `sibling_containers`, `sibling_generic`, `sibling_segments` (fallback) |
| `discover_parent_siblings` | `parent_sibling_roles` (Template-vs-Project level-2 siblings) |
| (governance-chain discovery, referenced at `DIRECTED_TYPES`) | `governance_chain` |
| `discover_within_project` | `within_project` |

**`build_cascade()`'s branch coverage** (`generate_governance_narrative.py:322-388`):
only handles `template_to_container`, `container_to_project`,
`template_to_project`/`parent_sibling_roles` (aliased), `sibling_projects`,
`within_project`. It is a plain `if/elif` chain with **no `else`** — every row whose
`comparison_type` doesn't match one of these five falls through silently. That drops:

- `generic_to_template`, `generic_to_container`, `generic_to_project` — the actual
  enterprise-baseline propagation signal the "Governance Cascade" diagram in
  `render_header()` describes (`Generic / Enterprise Baseline ↓ [generic → template/
  container/project containment]`). The diagram promises this signal; `build_cascade`
  never computes it. (It's partially recovered elsewhere — see the note on
  `build_governance_state_summary` below — but never in `cascade`, so `assign_tier`,
  `render_domain_tiers`, `detect_anomalies`, and `render_findings_and_recommendations`
  never see it.)
- **All four new scope-level types**: `enterprise_to_project`, `bc_to_project`,
  `enterprise_to_bc`, `enterprise_to_client`.
- `sibling_templates`, `sibling_containers`, `sibling_generic`, `sibling_segments`,
  `governance_chain`.

**Quantification:** Cannot be quantified in row/domain counts without a real
`cross_segment_summary.csv` (none exists in-repo). What can be stated precisely:
of the ~16 distinct `comparison_type` values the producer can emit, `build_cascade`
recognizes 4 literal values (5 counting the `parent_sibling_roles` alias) — every
row of every domain carrying one of the other ~11 types is silently excluded from
`cascade`, hence from every downstream tier/anomaly/finding computed from `cascade`.

**Verdict:** Confirms and extends the prompt's lead — the drop list in the prompt
(`generic_to_template/container/project`, the 4 enterprise/bc types,
`sibling_templates`, `sibling_containers`, `governance_chain`) is correct and is not
exhaustive: `sibling_generic` and the `sibling_segments` fallback are also dropped.

**Recommendation:** `build_cascade` needs either a mapped dispatch table keyed by
the full `comparison_type` vocabulary (with an explicit "known but intentionally
excluded" bucket, e.g. for `sibling_generic`), or an assertion/warning path that
surfaces unrecognized types instead of swallowing them — the current silent
`if/elif` makes future producer additions invisible by default.

---

### A2. `render_client_section` / `build_client_summary` and `pool_scope`

**Checked:** Every reference to `pooled_rows` inside `build_client_summary`
(`generate_governance_narrative.py:787-890`), and a repo-wide grep of
`generate_governance_narrative.py` for `pool_scope` and `business_center`.

**Found:** Zero matches for `pool_scope` or `business_center` anywhere in
`generate_governance_narrative.py`. `build_client_summary` reads `pooled_rows` in
exactly two places:
- L792-796 (`all_clients` set): iterates every row, calls `get_client(r["segment_id"])`,
  keeps it if `r["governance_role"] == "Project"` — no `pool_scope` check.
- L820-827 (`client_files`): same iteration, takes `max(n_files_focal)` per client —
  no `pool_scope` check.

Confirmed against the producer: `run_pooled_comparison()`
(`compare_cross_segment.py:3012-3106`) calls `_emit_for_groups` three times — once
each for `parent_groups`, `bc_groups`, `client_groups` — writing `"parent_sibling"`,
`"bc"`, and `"client"` into the `pool_scope` column (`POOLED_FIELDS` includes
`pool_scope` at `compare_cross_segment.py:218`). So for any segment with siblings in
more than one grouping, `cross_segment_pooled.csv` now contains **up to three rows
per (segment_id, domain)** — one per grain — and `build_client_summary` reads all of
them as if they were one undifferentiated view.

**Practical impact today:** Narrower than it looks, because the two fields actually
read (`segment_id`'s parsed client, and `n_files_focal`) describe the *focal* segment
itself, not the pool — so `n_files_focal` is identical across a segment's 3 grain
rows and the `max()` pick is numerically idempotent-safe right now. But this is
accidental safety, not design: any pool-dependent metric on `POOLED_FIELDS`
(`all_containment_focal_in_pool`, `used_containment_pool_in_focal`,
`n_shared_join_hash`, etc.) is *never* read by the narrative today, and the moment
someone adds a per-client "how does this client's practice compare to its bc peers
vs. its client-wide peers" read using those columns, the missing `pool_scope` filter
will silently blend three semantically distinct pools (siblings-under-same-parent,
same-business-center, same-client) into one number.

**Verdict:** Confirms the risk noted in prior discussion — `bc` and `client` grain
rows are read undifferentiated from `parent_sibling` grain rows, because there is no
`pool_scope` filtering logic at all (not "wrong filter", literally none). No visible
numeric corruption in current output, but the aggregation is scope-blind by
construction.

**Recommendation:** Add `if r.get("pool_scope") not in ("parent_sibling", "")` `continue`
(or an explicit grain parameter) before `build_client_summary` is extended to use any
pool-relative metric; document which grain the client section is meant to represent.

---

### A3. Do `enterprise_to_bc` / `enterprise_to_client` / `bc_to_project` governance-state rows reach `render_governance_state_section`?

**Checked:** `GOVERNANCE_STATE_DIRECTED_TYPES` in `compare_cross_segment.py:402-413`
(producer side) vs. `_DIRECTED_GOVERNANCE_TYPES` in
`generate_governance_narrative.py:944-953` (consumer side), and both code paths in
`build_governance_state_summary` (`generate_governance_narrative.py:960-1084`) that
consume, respectively, `--governance-state-summary` (compact) and
`--governance-states` (detailed per-pattern) rows.

**Found — producer side:** `GOVERNANCE_STATE_DIRECTED_TYPES` (compare_cross_segment.py)
includes all ten types: `generic_to_template`, `generic_to_container`,
`generic_to_project`, `template_to_project`, `template_to_container`,
`container_to_project`, `enterprise_to_project`, `bc_to_project`, `enterprise_to_bc`,
`enterprise_to_client`. The gate at `compare_cross_segment.py:3697`
(`if ctype in GOVERNANCE_STATE_DIRECTED_TYPES:`) means **both**
`cross_segment_governance_states.csv` (detailed) and
`cross_segment_governance_state_summary.csv` (compact) genuinely contain rows for
all four new scope types — they are not held back upstream.

**Found — consumer side, two different bugs, not one:**

1. **Compact summary rows (`--governance-state-summary`), loop at
   `generate_governance_narrative.py:974-1009`:** this loop has **no
   `comparison_type` filter at all** for the count fields
   (`provided_and_used_count`, etc.) or the float fields
   (`provided_to_configured_containment`, `provided_passive_share`,
   `provided_missing_share`, `local_active_share`) — it accumulates every row into
   `by_domain[dom]` regardless of type, only branching on `ctype` for the three
   `generic_to_*` derived fields. `GOVERNANCE_STATE_SUMMARY_FIELDS`
   (`compare_cross_segment.py:356-391`) confirms `comparison_type` is a real column
   on this file. **Consequence: rows for `enterprise_to_project`, `bc_to_project`,
   `enterprise_to_bc`, `enterprise_to_client` DO reach `render_governance_state_section`
   — but get summed/averaged into the same per-domain bucket as
   `template_to_project`, `container_to_project`, etc., with zero scope separation.**
   This is arguably worse than a silent drop: the section renders confidently with a
   number that is a blend across scope levels the reader has no way to detect from
   the output.
2. **Detailed per-pattern rows (`--governance-states`), loop at
   `generate_governance_narrative.py:1011-1029`:** this loop *does* gate on type —
   `if ctype and ctype not in _DIRECTED_GOVERNANCE_TYPES: continue`
   (`generate_governance_narrative.py:1016`) — but the narrative's own
   `_DIRECTED_GOVERNANCE_TYPES` set (`generate_governance_narrative.py:944-953`) is a
   **stale, independently-maintained copy** of the producer's
   `GOVERNANCE_STATE_DIRECTED_TYPES` that predates the four new types (it has
   `generic_to_downstream` — not in the producer's current set at all — but is
   missing all four of `enterprise_to_project`, `bc_to_project`, `enterprise_to_bc`,
   `enterprise_to_client`). **Consequence: detailed per-pattern rows for the four new
   types are silently dropped entirely** at this stage, never contributing to
   `by_domain[dom]` via this path.

**Verdict:** Extends the prompt's lead with a materially more specific finding: it's
not "never reach" — the compact/summary path lets the new types through but blends
them invisibly with old types; the detailed/per-pattern path drops them outright.
Both are real, both independently confirmed by reading the two loops and the two
`_DIRECTED_GOVERNANCE_TYPES` definitions side by side.

**Recommendation:** (a) sync `_DIRECTED_GOVERNANCE_TYPES` in the narrative to the
producer's `GOVERNANCE_STATE_DIRECTED_TYPES` (or import it, if these files are ever
allowed to share a module) and decide whether `generic_to_downstream` is a
still-live type that needs to stay; (b) add a `comparison_type` grouping key to the
compact-summary accumulation loop so scope levels are never averaged together, then
decide explicitly whether/how `render_governance_state_section` should present them
(separate table per scope level is the obvious shape, given the section already
does per-domain tables).

---

## B. Positional parsing removal

### B4. Call-site-by-call-site table: `get_client()` / `get_disc()` / `is_generic()`

All call sites (verified via full-file grep, `generate_governance_narrative.py`):

| Line | Call | Row source | Direct column already available? |
|---|---|---|---|
| 279 | `get_disc(r["segment_id_a"]) or get_disc(r["segment_id_b"])` | `summary_rows` (fallback path in `load_corpus_counts`, only used when `--file-meta` absent) | **Yes** — `discipline_label_a`, `discipline_label_b` are real `SUMMARY_FIELDS` columns (`compare_cross_segment.py:146`) |
| 282 | `get_client(r["segment_id_a"]) or get_client(r["segment_id_b"])` | `summary_rows` (same fallback path) | **Yes** — `client_label_a`, `client_label_b` (`compare_cross_segment.py:145`) |
| 329, 337, 345, 375, 408 | `is_generic(a)` / `is_generic(b)` | `summary_rows` | **No direct equivalent.** `is_generic()` tests "is this segment the unscoped generic-role population" via `len(segment_id.split("|")) == 2`. There is no `segment_level` or `scope_level` column on `SUMMARY_FIELDS` today — `governance_role_a/b` tells you the *role*, not whether the segment is scope-unqualified. See B5 for why the length-2 proxy itself is unsound, independent of whether a direct column exists. |
| 365 | `get_disc(a)` | `summary_rows`, `within_project` rows (`a == b`) | **Yes** — `discipline_label_a` (same segment on both sides for `within_project`) |
| 408 | `is_generic(r["segment_id_a"])` / `is_generic(r["segment_id_b"])` | `summary_rows` | Same as above |
| 794 | `get_client(r["segment_id"])` | `pooled_rows` | **Partial** — `POOLED_FIELDS` (`compare_cross_segment.py:212-229`) carries `client_label` directly (singular, not `_a/_b` since pooled rows describe one focal segment), so **yes**, a direct column exists here too and is unused. |
| 815 | `get_client(r["segment_id_a"])` | `summary_rows`, `within_project` rows | **Yes** — `client_label_a` |
| 823 | `get_client(r["segment_id"])` | `pooled_rows` | **Yes** — `client_label` (as above) |
| 1409 | `get_disc(r["segment_id_a"])` | `summary_rows`, `within_project` rows | **Yes** — `discipline_label_a` |
| 1419-1420 | `get_disc(r["segment_id_a"])` (inside `"Template" in r["segment_id_a"]` string check) | `summary_rows` | **Yes** for the discipline part; the `"Template" in segment_id` substring test itself is a *third* form of positional/stringly-typed parsing not called out in the original prompt — see extension note below. |

**Extension not in the original prompt:** `render_discipline_section`
(`generate_governance_narrative.py:1419`) does `if "Template" in r["segment_id_a"]`
— a raw substring test against `segment_id`, not even split-based. This has the same
class of defect as `get_client`/`get_disc`/`is_generic` (reading structure out of a
formatted string instead of `governance_role_a`, which is a real, already-present
`SUMMARY_FIELDS` column) but is a distinct code path from the three functions named
in the prompt, so it would be missed by a fix that only touches
`get_client`/`get_disc`/`is_generic`.

**Verdict:** Confirms the prompt's framing for 8 of the 8 non-`is_generic` call
sites (every one of them has a ready-made direct column). Extends it with the
`"Template" in segment_id` substring check at L1419, which is the same defect class
but not one of the three named functions.

**Recommendation:** Replace all `get_client(segment_id_x)` / `get_disc(segment_id_x)`
calls with direct reads of `client_label_x` / `discipline_label_x` (via a helper like
existing `_pick()`), and replace the L1419 substring test with
`_role_key(row.get("governance_role_a","")) == "template"`. `is_generic()` needs a
different fix — see B5.

---

### B5. `is_generic()` — concrete failure trace

**Checked:** `is_generic()`'s `len(seg_id.split("|")) == 2` definition against how
`segment_id` is actually constructed in `build_segment_manifest.py`.

**Mechanism (confirmed from source):** `_subset_to_id()`
(`build_segment_manifest.py:226-269`) builds `segment_id` by iterating
`cfg_fields = [unit_system, governance_role, client_label, discipline_label,
business_center_label, collection_label]` **in fixed order, but skipping any field
not present in that segment's key** (`if f not in kv: continue` —
`build_segment_manifest.py:230-231`). Segment keys themselves come from
`combinations(non_root_pairs, size)` over **all subsets** of the five non-root
dimensions (`build_segment_manifest.py:296-298`), including `governance_role` —
i.e., `governance_role` is not pinned into every segment's key; a segment can
legitimately have `governance_role == ""` if the size-N combination selected for it
didn't include the governance dimension.

**Concrete trace:** take `size=1`, `subset = [(business_center_label, "BC_2014")]`.
This produces `key = {(unit_system, "imperial"), (business_center_label, "BC_2014")}`
— a real, legitimately-generated segment with `governance_role = ""` (confirmed:
`dim_map.get(governance_field, "")` at `build_segment_manifest.py:339` returns `""`
when the field isn't in the key). Its `segment_id` via `_subset_to_id()` is
`unit_system` + `business_center_label` only (client_label/discipline_label/
collection_label all skipped since absent from the key, and governance_role also
skipped) → **`"imperial|BC_2014"`** — exactly 2 pipe-separated parts.
`is_generic("imperial|BC_2014")` returns `True`.

But this segment is **not** a Generic-role segment — it's a business-center-wide
rollup with **no role filter at all**, potentially spanning Template, Container, and
Project files together. `is_generic()` cannot distinguish it from a true
`"imperial|Template"` (unit_system + governance_role, 2 parts) or
`"imperial|Container"` generic-role segment, because both produce exactly 2 parts
and `is_generic()` only counts parts — it never checks that the second part is
actually a role token.

**Is this currently exploitable, or accidentally safe?** Traced one level further:
for `"imperial|BC_2014"` to actually appear as `a`/`b` in a `template_to_container` /
`container_to_project` / `template_to_project` row that `build_cascade` would then
gate with `is_generic(a) and is_generic(b)`, it would need to come out of
`discover_within_segment()`'s `role_map` bucketing
(`compare_cross_segment.py:1783-1786`), which keys strictly on
`"generic" if _is_generic_role(role) else role` — a blank `governance_role` maps to
bucket key `""`, which none of `role_map.get("generic"/"template"/"project"/
"container")` ever read. It's also excluded from the scope-level fan-out's
`standard_rows`/`project_rows` (both require role in `{"template","container"}` or
`"project"}` respectively). **So today, this specific blank-role rollup shape is not
reachable through any comparison-type branch `is_generic()` gates in `build_cascade`
— the defect is real and reachable in principle (any code that calls `is_generic()`
and only checks part-count, e.g. `get_disc`/`get_client`'s own callers at
L279/282/365/1409/1420 would also see this segment and misparse it the same way
`get_client` misparses `imperial|Project|BC_2014`), but it is currently inert for the
five `ct ==` branches in `build_cascade` specifically, because the pair-discovery
functions that feed those branches happen to require a non-blank role today.** That
is a coincidence of the current pair-discovery implementation, not a property
`is_generic()` itself guarantees — the moment any pair-discovery path is extended to
pool blank-role rollups (or a future collection-level rollup like
`imperial|collection:Kaizen Standards`, which is also exactly 2 parts under the same
mechanism), `is_generic()` will silently admit it.

**Verdict:** Confirms the prompt's lead precisely, and sharpens "accidentally-safe"
into a specific, mechanism-level explanation: the length-2 check is unconditionally
unsound as a definition of "generic segment," but is empirically inert today only
because current pair-discovery functions gate on non-blank governance_role before
`is_generic()` ever gets a chance to misclassify. This is a latent landmine, not
resolved.

**Recommendation:** Replace `is_generic()` with a role-based check
(`governance_role_x in ("", "Generic")` read from the actual column, once B4's fix
lands) rather than any string-shape heuristic on `segment_id`.

---

### B6. Does `business_center_label_a`/`_b` need to be added to `SUMMARY_FIELDS`?

**Checked:** `SUMMARY_FIELDS` (`compare_cross_segment.py:140-171`),
`GOVERNANCE_STATE_SUMMARY_FIELDS` (`compare_cross_segment.py:356-391`),
`GOVERNANCE_STATE_FIELDS` (`compare_cross_segment.py:231-258`), `POOLED_FIELDS`
(`compare_cross_segment.py:212-229`), `DELTA_FIELDS`
(`compare_cross_segment.py:186-202`).

**Found:** `business_center_label` does not appear as a column in any of these five
schemas. `SUMMARY_FIELDS` carries `governance_role_a/b`, `client_label_a/b`,
`discipline_label_a/b` — no `business_center_label_a/b`. `POOLED_FIELDS` carries
`client_label` (singular, focal-segment-only) but no `business_center_label`.
`GOVERNANCE_STATE_SUMMARY_FIELDS`/`GOVERNANCE_STATE_FIELDS` carry
`governance_role_reference/target` but neither client nor business-center labels at
all.

**Consequence:** even after fixing B4/B5 (reading `client_label_x`/
`discipline_label_x` directly instead of parsing `segment_id`), the narrative still
cannot determine *scope level* (`enterprise` / `bc` / `client` / `project`, per
`compare_cross_segment.py`'s own `_scope_level()`, `compare_cross_segment.py:1695-1704`)
for a row without either (a) re-deriving it from `segment_id` text — the exact
positional-parsing anti-pattern this audit is about — or (b) a new
`business_center_label_a/b` column landing in `SUMMARY_FIELDS` (and the equivalent
field in the governance-state/pooled/delta schemas) so the narrative can call an
equivalent of `_scope_level()` on real columns instead of guessing from string shape.

**Verdict:** Confirms the dependency exactly as flagged in the prompt. This is a
producer-side (`compare_cross_segment.py`) schema gap, out of scope for this pass,
but is a hard prerequisite for making the narrative properly scope-level-aware
rather than merely less-buggy about client/discipline.

**Recommendation (flagged as dependency, not to be implemented here):** add
`business_center_label_a`/`business_center_label_b` to `SUMMARY_FIELDS`, and the
corresponding reference/target or focal fields to `GOVERNANCE_STATE_SUMMARY_FIELDS`,
`GOVERNANCE_STATE_FIELDS`, and `POOLED_FIELDS`, sourced from the same manifest rows
`_scope_level()` already reads (`_bc_of(row)` / `_client_of(row)`,
`compare_cross_segment.py:1686-1692`) — so the narrative can eventually compute
`_scope_level()`-equivalent logic from columns, not by re-deriving it from
`segment_id`.

---

## C. Hardcoded classification tables

### C7. Inventory and classification

| Name | Location | Classification | Notes |
|---|---|---|---|
| `DOMAIN_LABELS` | `generate_governance_narrative.py:69-103` (~35 entries) | **(a) derivable** | Every key is a `domain` string emitted by an extractor/partition; a human-readable label is presentation-only. See C8 — no existing contract carries this today, so "derivable" means "should be computed from a data file the producer maintains," not "already available." |
| `DISC_LABELS` / `DISC_KEYWORDS` | `generate_governance_narrative.py:108-118` | **(a) derivable** | Confirmed incomplete per the prompt's lead (7 of 10 real disciplines); the vocabulary is exactly the set of distinct `discipline_label` values in `segment_manifest.csv` / `file_metadata.csv` — should be computed at load time (e.g., collected from `discipline_label_a/b` across summary rows, or from `--file-meta`), not hardcoded. |
| `EXCLUDED_FROM_SCORING = {"view_templates_renderings_drafting"}` | `generate_governance_narrative.py:106` | **(c) deliberate, documented** | Has an explicit inline comment ("structurally anomalous") and is echoed in `render_limitations()`'s "Excluded domain" note (`generate_governance_narrative.py:2010`) — this is a stated, self-documenting editorial exclusion, not a silent one. Fine to leave as code, though it is itself a single-domain allowlist that would benefit from a comment pointer to *why* (which decision/finding established the anomaly) if one exists in `DECISIONS.md`/`CHANGELOG.md`. |
| A hard-coded healthcare-client set | `generate_governance_narrative.py` (historical location) | **(b) genuine business classification, wrong medium** | Sector membership is a real business fact that cannot be derived from the fingerprint pipeline's own data (nothing in `segment_manifest.csv`/`file_metadata.csv` encodes "sector"). It should live in an editable data file (e.g. a small CSV/JSON keyed by `client_label` with a `sector` column), not a Python literal — every new healthcare client onboarding today requires a code change and redeploy of the narrative script to be classified correctly, and a mis-set/missing entry silently changes both `xc` cross-client-convergence membership (`build_cascade`, `ct == "sibling_projects"` branch, `generate_governance_narrative.py:353-361`) and the `is_healthcare` flag in `build_client_summary`. |
| A standalone known-non-healthcare client special case | `generate_governance_narrative.py` (historical location) | **(b) genuine business classification, wrong medium** | Same defect class as `HEALTHCARE_CLIENTS` but worse: it's a single hardcoded client name with no accompanying set/constant, no comment explaining *why* Intel specifically is non-comparable, and no way to add a second non-healthcare/non-comparable client without another code edit. Should fold into the same sector/classification data file as `HEALTHCARE_CLIENTS` (e.g. `sector: "semiconductor"` or `sector: "other"` vs. `sector: "healthcare"`), replacing both the `HEALTHCARE_CLIENTS` set check and this special case with one data-driven lookup. |
| `PASSIVE_INHERITANCE_RISK_DOMAINS` | `generate_governance_narrative.py:124-129` | **(c) deliberate, documented** | Has a explanatory comment ("domains where passive inheritance is most likely to inflate all-view scores... often fully inherited from templates but rarely customised"). This reads as an editorial judgment about which domains are *prone to* a certain interpretation risk, not a fact derivable from data — reasonable to leave as an editorial constant, though it overlaps in spirit with `EXCLUDED_FROM_SCORING` (both are lists of "domains needing special interpretive handling") and could arguably be unified into one config block with reasons attached. |
| `LOCAL_ACTIVE_MATERIAL_THRESHOLD`, `PASSIVE_MATERIAL_THRESHOLD`, `MISSING_MATERIAL_THRESHOLD`, `ACTIVE_USE_MIN_FOR_STRONG_BASELINE` | `generate_governance_narrative.py:566-569` | **(c) deliberate, documented** | Explicitly labeled in a comment as "narrative thresholds, not governance policy," and the same numbers are echoed verbatim to the reader in `render_domain_tiers()`'s classification-key note (`generate_governance_narrative.py:1318-1320`: "local-active ≥15%, passive ≥20%, missing ≥20%, and strong-baseline active-use containment ≥75%"). This is the cleanest example in the file of a hardcoded constant that is fine to leave as code — it's transparent to the reader and explicitly disclaimed as editorial. |
| `TIER_*` labels / `TIER_ORDER` / `RELIABILITY_*` labels | `generate_governance_narrative.py:553-561, 486-492, 648-658` | **(c) deliberate, documented** | These are the report's own vocabulary (classification names it invented), not something that could be "derived from pipeline data" — they're editorial by definition. Fine as code. |

**Verdict:** Confirms the prompt's framing exactly. `DISC_LABELS`/`HEALTHCARE_CLIENTS`
are the same defect class (a data-derivable-or-external fact hardcoded into Python),
and the standalone `Intel` check is a third, more acute instance of the same
`HEALTHCARE_CLIENTS`-style defect that the prompt didn't name explicitly but that
belongs in the same fix.

---

### C8. Does any contract file already carry a canonical domain label?

**Checked:** `contracts/domain_identity_keys_v2.json` in full (field-by-field: each
domain entry has `domain_family`, `allowed_keys`, `required_keys`, `minima` — no
`label`/`display_name`/`domain_label` key anywhere), plus a repo-wide grep for
literal `DOMAIN_LABELS` values (e.g. `"Object Styles — Model"`,
`"Dimension Types — Linear"`, `"View Templates — Schedules"`).

**Found:** Zero matches outside `generate_governance_narrative.py` itself. No other
file in the repository — not `domain_identity_keys_v2.json`, not any other contract
under `contracts/`, not `docs/` — carries a human-readable label per domain.
`DOMAIN_LABELS` in the narrative is the sole source of these strings anywhere in the
codebase.

**Verdict:** Refutes the "already exists elsewhere" possibility cleanly — there is
nothing to source from today. Stating this plainly rather than guessing, per the
prompt's instruction.

**Recommendation:** If `DOMAIN_LABELS` is judged worth de-duplicating (item C7,
classification (a)), the label would need a *new* field added to
`contracts/domain_identity_keys_v2.json` (e.g. `"display_label"` alongside
`domain_family`) — this is itself a contract change with its own review path, not a
narrative-only fix, and should be scoped as its own follow-up rather than bundled
into the `get_client`/`get_disc`/`DISC_LABELS` fix.

---

## Summary table (fix-direction only, not authorized for this pass)

| # | Finding | Fix direction |
|---|---|---|
| A1 | `build_cascade` silently drops ~11 of ~16 comparison types incl. all 4 new scope types | Explicit dispatch table over the full comparison-type vocabulary; warn on unrecognized types |
| A2 | `build_client_summary` never filters `pooled_rows` by `pool_scope` | Filter to `parent_sibling` (or make grain explicit) before adding any pool-relative metric |
| A3 | Compact governance-state rows blend all scope types per domain; detailed rows drop the 4 new types outright | Sync `_DIRECTED_GOVERNANCE_TYPES`; add `comparison_type`/scope grouping to the compact-summary loop |
| B4 | 8 of 8 non-`is_generic` call sites already have a direct column available | Replace `get_client`/`get_disc`(segment_id) with direct `client_label_x`/`discipline_label_x` reads; fix the L1419 `"Template" in segment_id` substring check too |
| B5 | `is_generic()`'s length-2 heuristic misclassifies blank-role scope rollups; currently inert only because pair-discovery happens to gate on non-blank role first | Replace with a `governance_role_x in ("", "Generic")` column check |
| B6 | `business_center_label_a/b` absent from `SUMMARY_FIELDS` and siblings | Producer-side schema addition — prerequisite for true scope-level awareness, out of scope here |
| C7 | `HEALTHCARE_CLIENTS` + standalone `Intel` check are the same undocumented-business-fact-in-code defect; `DISC_LABELS` is derivable; thresholds/tiers/exclusions are legitimately editorial | Move client-sector membership to an editable data file; compute discipline vocabulary from data; leave thresholds/tiers as code |
| C8 | No existing contract carries domain display labels | Confirmed gap — would require a new field in `domain_identity_keys_v2.json`, scoped as its own follow-up |
