# Fill Pattern Mapping Utility

`mapping/` is a Revit-side downstream utility that materializes representative
`FillPatternElement` objects in the currently open Revit document from the
CSV outputs of `tools/export_bundle_pattern_detail.py`, for use in a
mapping/configuration RVT consumed by downstream (Guardian) governance
tooling. This document covers the `fill_patterns` domain family's mapping
utility, built on the same pattern as `docs/line_pattern_mapping.md`'s
`line_patterns` utility (D-038) -- read that document first if you haven't;
this one focuses on what's different for `fill_patterns`.

This is intentionally a separate top-level package from `core/` / `domains/`
/ `runner/` / `tools/` (same reasoning as `line_patterns`): it is not
extraction (never invoked by `runner/run_dynamo.py`, writes to the document
rather than reading it) and it is not analysis (has a hard Revit API
dependency, unlike the stdlib-only `tools/`).

## Scope (this PR)

- Only the `fill_patterns` domain family -- both D-015 partitions,
  `fill_patterns_drafting` and `fill_patterns_model`. No other domain can be
  materialized by this code path.
- Input: a directory containing `bundle_pattern_inventory.csv`,
  `pattern_settings.csv`, and `pattern_names.csv` -- the same three CSVs
  `tools/export_bundle_pattern_detail.py` already produces for one segment,
  confirmed to require **no changes** for this domain family: the exporter is
  entirely domain-agnostic (it discovers domains from
  `results/bundle_analysis/<purge_view>/<domain>/bundles.csv` and streams
  `identity_items_by_domain/<domain>.csv` by whatever domain string is
  passed), so `--domain fill_patterns_drafting` and
  `--domain fill_patterns_model` work today with zero code changes -- the
  task description's phrase "fill_patterns" is the domain *family* name; the
  two actual exportable domains are the family's partitions.
- Output: `FillPatternElement` objects in the currently open document, plus a
  deterministic CSV report (see below) covering both partitions.

Explicitly out of scope for this PR (same exclusions as `line_patterns`): any
other domain; standards maintenance; modifying/replacing/deleting existing
project elements or configurations; continuous synchronization; generalized
dependency resolution; opening source RVTs to copy elements;
opening/saving/closing the target RVT.

## One entry point, not two

`fill_patterns_drafting` and `fill_patterns_model` are handled by a single
Dynamo entry point (`mapping/create_fill_pattern_mappings.py`), which loops
over both domain names against the same input directory and writes one
combined report. This was a deliberate choice, not an oversight:

- The reconstruction, hashing, naming, and creation logic is 100% identical
  between the two partitions -- only the expected `fill_pattern.target` value
  (`"Drafting"` vs `"Model"`) and which join-key policy entry applies differ
  (`mapping/fill_pattern_reconstruction.py::TARGET_NAME_BY_DOMAIN` and
  `get_fill_pattern_join_key_policy(domain_name)` parameterize both).
- Both partitions' evidence comes from the exact same
  `export_bundle_pattern_detail.py` export directory (they're two rows in the
  same `bundle_pattern_inventory.csv`, distinguished only by the `domain`
  column).
- `FillPatternElement` objects for both partitions share one namespace in a
  single Revit document (`build_name_index()` collects `FillPatternElement`
  instances regardless of `Target`), so collision detection/naming has to
  reason about both partitions together in one document anyway -- splitting
  into two entry points wouldn't isolate anything, it would just require
  running the name-index build twice and coordinating collision state across
  two separate script invocations for no benefit.

A second, drafting-only or model-only entry point would be pure duplication.

## Files

- `mapping/fill_pattern_reconstruction.py` -- pure Python, no Revit
  dependency. CSV loading, evidence validation/blocking, grid reconstruction,
  hash reconstruction/verification, deterministic naming, and report
  formatting -- domain-parameterized so both partitions share this one
  module. Unit tested directly
  (`tests/test_fill_pattern_mapping_reconstruction.py`).
- `mapping/fill_pattern_revit_apply.py` -- the Revit-API half: reading grids
  back off a live `FillPatternElement`, bounded
  create-then-verify-then-commit/rollback transactions, and the
  existing-vs-create-vs-block decision per requested `(domain, join_hash)`.
- `mapping/create_fill_pattern_mappings.py` -- the Dynamo CPython3 entry
  point (`IN[0]` = input directory, `IN[1]` = report path, `IN[2]` = optional
  repo-root override), built on the shared `mapping/_dynamo_bootstrap.py`
  module (see that module's own docstring, and
  `docs/line_pattern_mapping.md`, for the bootstrap details -- this entry
  point carries the same small, unavoidable loader shim every mapping entry
  point needs).

## Identity model (domains/fill_patterns.py)

Both partitions capture, per record:

- `fill_pattern.target` -- always `"Drafting"` or `"Model"` (hardcoded per
  partition, `q="ok"` always).
- `fill_pattern.grid_count` -- integer grid count.
- For each grid index `i` in `[0, grid_count)`, in this fixed insertion order
  (domains/fill_patterns.py never sorts these before hashing):
  1. `fill_pattern.grid[NNN].angle`
  2. `fill_pattern.grid[NNN].origin.kind` (`"uv"` or `"xy"`)
  3. either `fill_pattern.grid[NNN].origin.u` + `.origin.v` (if kind is
     `"uv"`) or `fill_pattern.grid[NNN].origin.x` + `.origin.y` (if kind is
     `"xy"`) -- no leaf items at all if kind is unmapped/unreadable
  4. `fill_pattern.grid[NNN].offset`
  5. `fill_pattern.grid[NNN].shift`
- `fill_pattern.grids_def_hash` -- a hash of `grid_count` + every
  `fill_pattern.grid[NNN].*` item above, computed inline by the domain at
  extraction time (see next section).

`fill_pattern.is_solid` and `fill_pattern.is_import` are coordination-only
(filter criteria, never identity) -- solid fills are excluded from both
domains entirely (system defaults, ungoverned), so this utility never needs
to reason about them. `fill_pattern.name` is cosmetic-only, never identity.

## `grids_def_hash` is NOT a flatten-stage synthetic augmentation

This is the key difference from `line_patterns` (D-017/D-038): `line_patterns`'
`join_hash` is derived from `line_pattern.segments_norm_hash`, which is
appended **synthetically** by `tools/run_extract_all.py`'s private
`_append_line_pattern_synthetic_norm_hash()` during the flatten stage (T0.5)
-- it does not exist in the domain's own inline output at all.
`fill_pattern.grids_def_hash` is different: `domains/fill_patterns.py` computes
it **inline, at extraction time**, directly from the grid items above (a raw
`"k=..|q=..|v=.."` token join + `make_hash`, in insertion order -- confirmed
by grep: there is no `fill_pattern` reference anywhere in
`tools/run_extract_all.py`).

`mapping/fill_pattern_reconstruction.py::compute_grids_def_hash()` reconstructs
this exact algorithm from already-exported evidence (deliberately NOT
imported from `domains/fill_patterns.py` -- the same "independent
reimplementation over import" precedent `tools/pattern_id_utils.py` and
`mapping/line_pattern_reconstruction.py::compute_segments_norm_hash` already
established, so this Revit-writing utility doesn't couple to the extractor's
Revit-API-guarded import surface).
`tests/test_fill_pattern_mapping_reconstruction.py` cross-checks the
implementation against a hand-copied reference of the same token algorithm
over synthetic grid lists, closing the same kind of gap `CLAUDE.md` flags as
an open TODO for `pattern_id_utils.py`.

### Grid item insertion order is lost in the exported CSV

`identity_basis.items` (and therefore `identity_items_by_domain/<domain>.csv`,
and therefore `pattern_settings.csv`) always stores the **lexically-sorted-
by-key** item list, because that's what feeds `sig_hash`/`identity_basis` --
the domain's original insertion order (needed to reproduce `grids_def_hash`,
which is explicitly NOT sorted) is not preserved anywhere in the export.
`compute_grids_def_hash()` does not try to recover insertion order from the
CSV -- it can't, the information is gone -- it reconstructs the **known,
fixed** insertion order directly from `domains/fill_patterns.py`'s own
field-building sequence (a code invariant, not data): `grid_count`, then per
grid `angle` / `origin.kind` / origin leaf pair / `offset` / `shift`.

## Join-key verification (not sig_hash, and not bare grids_def_hash either)

`domains/fill_patterns.py` emits `sig_hash` from
`core.record_v2.serialize_identity_items()` over the **full, sorted**
`identity_basis.items` list (every individual `fill_pattern.grid[NNN].*`
field, not just the summary). The `fill_patterns_drafting`/
`fill_patterns_model` join-key policy
(`policies/domain_join_key_policies.json`) requires exactly three items:

```json
"required_items": [
    "fill_pattern.target",
    "fill_pattern.grid_count",
    "fill_pattern.grids_def_hash"
]
```

Critically, **this is three required items, not one** -- unlike
`line_patterns`' single-required-item policy
(`["line_pattern.segments_norm_hash"]`), which hits
`core/join_key_builder.py::build_join_key_from_policy`'s
`preserve_single_def_hash_passthrough` shortcut (making `join_hash` literally
equal `segments_norm_hash`). `fill_patterns`' three-item required set does
NOT trigger that shortcut, so `join_hash` is computed via
`core.phase2.phase2_join_hash` over all three items together
(`target`+`grid_count`+`grids_def_hash`, sorted by key, `k=..|q=..|v=..`
tokens joined and hashed) -- **`join_hash` != `grids_def_hash` alone**, and
of course `join_hash` != `sig_hash` either. This was verified directly (not
assumed) -- see
`tests/test_fill_pattern_mapping_reconstruction.py::test_join_hash_differs_from_bare_grids_def_hash`
and `::test_join_hash_differs_between_drafting_and_model_for_same_grids`.

This utility verifies against `join_hash`
(`mapping/fill_pattern_reconstruction.py::compute_join_hash_for_grids`, via
`core/join_key_builder.py` + `core/join_key_policy.py` against the real
policy) for the same reason `line_patterns` does: `bundle_pattern_inventory.csv`
/`domain_patterns.csv` clustering keys on `join_hash`
(`tools/extractor.py`'s `cluster_id = f"{domain}|{schema}|{join_hash}"`), not
`sig_hash`.

## `"xy"`-shaped origin evidence is blocked, not reconstructed

`domains/fill_patterns.py`'s grid-reading code has a defensive `"xy"` fallback
(reading `Origin.X`/`.Y` if `.U`/`.V` aren't present) for a runtime `Origin`
shape that has never actually been observed -- the real Revit API's
`FillGrid.Origin` is exclusively `UV`-typed. `mapping/fill_pattern_reconstruction.py`
therefore **blocks** (`grid_origin_kind_not_creatable:idx:xy`) any evidence
declaring `origin.kind == "xy"`, rather than accepting it as reconstructable:
this mapping utility can only ever construct a `FillGrid` with a `UV` origin
(`Autodesk.Revit.DB.UV(u, v)`), and reading a grid back off any live
`FillPatternElement` can only ever report `"uv"` too (`read_fill_pattern_from_element`
only checks `.U`/`.V`) -- so an `"xy"`-labeled requested `join_hash` could
never be reproduced by a created element in the first place, and blocking it
up front (before any transaction) is more useful than a doomed
create-then-rollback attempt. `compute_grids_def_hash()` itself still
supports `"xy"` as a general hash-preimage shape (it's a pure hashing
utility, not a creatability check), but `reconstruct_pattern()` -- the
function that actually feeds the Revit-mutation pipeline -- never produces a
grid with `origin_kind == "xy"`.

## Verification reads the element's ACTUAL target, never a caller assumption

Both partitions share one `FillPatternElement` namespace within a single
Revit document (`build_name_index()` in `mapping/fill_pattern_revit_apply.py`
indexes both regardless of `Target`), and
`mapping/create_fill_pattern_mappings.py` processes both partitions against
the same, shared `name_index`. This means an existing element found by name
while resolving a `fill_patterns_model` request could actually be a
Drafting-target element created earlier while resolving
`fill_patterns_drafting` (same observed name, coincidentally matching
grids). `verify_element_join_hash()` therefore reads the element's *actual*
`FillPattern.Target` (`read_fill_pattern_from_element()`) and rejects a
mismatch against the domain's expected target (`target_mismatch:<actual>`)
**before** computing any hash -- it never trusts a caller-supplied expected
target value over what the live element actually reports. Without this
check, a same-named, same-grid-geometry element from the *other* partition
could falsely pass verification, since the target string participates in the
`join_hash` preimage but was otherwise being asserted rather than read.
See `tests/test_fill_pattern_revit_apply_verification.py` for the regression
coverage.

## Naming

Identical convention to `line_patterns` (`docs/line_pattern_mapping.md`):
`select_observed_name()` picks the highest-`files_count`, lexically-tie-broken
acceptable (`label_q == "ok"`, non-empty `label_v`) observed name from
`pattern_names.csv`; a deterministic synthetic `unnamed_<short_join_hash>`
name is used (and the result marked degraded) if none is acceptable. Mapping
elements are named `MAP__<observed_name>` (sanitized for Revit's name-character
restrictions), with a `MAP__<observed_name>__<short_join_hash>` collision-safe
fallback if that name already exists under a *different* configuration.
`short_join_hash` is the first 12 hex characters of `join_hash`. An existing
nonmatching element is never touched.

## Revit mutation and verification

Construction path (see
`mapping/fill_pattern_revit_apply.py::create_and_verify_fill_pattern`),
confirmed against Autodesk's own CreateFillPattern SDK sample (Revit 2012 SDK
`Samples/CreateFillPattern/CS/FillPatternForm.cs` -- the same construction
shape is current through at least Revit 2026's `FillPattern`/`FillGrid` API
surface):

```python
fp = Autodesk.Revit.DB.FillPattern(name, target, FillPatternHostOrientation.ToHost)
grid = Autodesk.Revit.DB.FillGrid()
grid.Origin = Autodesk.Revit.DB.UV(u, v)
grid.Angle = angle
grid.Offset = offset
grid.Shift = shift
grid.SetSegments(List[float]())  # see "Known evidence gaps" below
fp.SetFillGrids(List[FillGrid]([grid, ...]))
element = Autodesk.Revit.DB.FillPatternElement.Create(doc, fp)
```

Each requested `(domain, join_hash)` gets its own
`Autodesk.Revit.DB.Transaction`. After `FillPatternElement.Create`, the
element's grids are read back (`read_grids_from_element`, mirroring
`domains/fill_patterns.py`'s own `GetFillPattern()` -> `GetFillGrids()` path)
and the resulting `join_hash` is recomputed via the same
`compute_join_hash_for_grids()` used for pre-mutation validation, still
inside the open transaction. Only if the verified `join_hash` equals the
requested one is the transaction committed -- and `Transaction.Commit()`'s
**actual returned `TransactionStatus`** is checked before reporting success
(the same check `line_pattern_revit_apply.py` added after PR #441 found
Revit can resolve a commit-time failure by internally rolling back without
`Commit()` raising). Any mismatch, or any exception during construction,
rolls the transaction back (`RollBack`); the configuration is reported
`blocked`/`post_creation_identity_mismatch` -- never reported as a success.

### Known evidence gaps: `FillPatternHostOrientation` and per-grid `Segments`

`FillPattern`'s constructor requires a `FillPatternHostOrientation`, and each
`FillGrid` can carry a dash-pattern `Segments` list -- neither is captured
anywhere in `domains/fill_patterns.py`'s identity items, `join_key`, or
`sig_hash` for this domain. Since there is no evidence to reconstruct either
from, this utility sets both to a fixed default (`FillPatternHostOrientation.ToHost`,
an empty/continuous `Segments` list) rather than inferring a value -- per the
project's fail-soft principle, an unevidenced value is defaulted explicitly,
never guessed from context. Neither field participates in `join_hash`
computation, so this cannot cause a `join_hash` verification failure -- but it
does mean a created mapping element's grid lines are always continuous, even
if the original pattern's lines used a dashed/dotted style along each grid
(a purely presentational difference this domain's identity model does not
capture in the first place).

## Status / action vocabulary and the report

Identical vocabulary to `line_patterns`: `status` is `ok`|`degraded`|`blocked`;
`action` is `existing`|`created`|`skipped`|`blocked` (`skipped` only for
inventory rows whose `join_hash` was blank). Dominance order is
`blocked > degraded > ok` (`dominant_status()`/`compute_run_status()`).

The report (`mapping/fill_pattern_reconstruction.py::REPORT_FIELDS`) has one
row per unique requested `(domain, join_hash)` across BOTH partitions (plus
one row per distinct blank-`join_hash` `pattern_id`, `action=skipped`):

```
segment_id, domain, join_hash, observed_name, mapping_name, action, status,
status_reason, revit_element_id, requested_join_hash, verified_join_hash,
source_bundle_ids, source_pattern_ids
```

Processing order is `domain` ascending, then `join_hash` ascending (so
`fill_patterns_drafting` rows sort before `fill_patterns_model` rows), giving
the same inputs against the same starting RVT always the same requested
configuration set, mapping names, and report.

## Verification procedure

See `docs/fill_pattern_mapping_verification.md` for the manual, Revit-side
procedure demonstrating the round trip (existing pattern -> fingerprint ->
bundle-pattern-detail export -> mapping utility -> re-fingerprint -> matching
`join_hash`) that this utility's own post-creation check automates per
pattern, for both partitions.

## What a future domain mapping PR needs to redo

Same as `line_patterns` (D-038's own note, unchanged by this PR): a mapping
utility for any other domain must still implement its own domain-specific
reconstruction, naming, and verification logic. No shared
"materialize-a-domain-into-Revit" abstraction exists across domains --
`fill_patterns` reused the same *pattern* `line_patterns` established
(reconstruction module / Revit-apply module / Dynamo entry point, each
domain-specific), plus the newly-shared `mapping/_dynamo_bootstrap.py`
bootstrap logic, but not a shared reconstruction/verification layer. A
domain with cross-domain UID dependencies (e.g. a `line_styles` mapping that
needs to resolve `line_patterns` references) would still need its own
dependency-resolution approach -- neither this PR nor D-038 introduces a
generalized mechanism for it.
