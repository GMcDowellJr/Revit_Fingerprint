# Line Pattern Mapping Utility

`mapping/` is a Revit-side downstream utility that materializes representative
`LinePatternElement` objects in the currently open Revit document from the
CSV outputs of `tools/export_bundle_pattern_detail.py`, for use in a
mapping/configuration RVT consumed by downstream (Guardian) governance
tooling.

This is intentionally a separate top-level package from `core/` / `domains/`
/ `runner/` / `tools/`:

- It is not extraction: it is never invoked by `runner/run_dynamo.py`, and it
  writes to the document rather than reading it.
- It is not analysis: unlike `tools/`, it has a hard Revit API dependency, so
  it cannot live under `tools/` (which the project keeps stdlib-only /
  Revit-independent by convention).

## Scope (this PR)

- Only the `line_patterns` domain. No other domain can be materialized by
  this code path.
- Input: a directory containing `bundle_pattern_inventory.csv`,
  `pattern_settings.csv`, and `pattern_names.csv` -- the three CSVs
  `tools/export_bundle_pattern_detail.py` already produces for one segment.
  Bundle selection/support filtering (which bundles/patterns make it into
  those CSVs) remains entirely the exporter's responsibility; this utility
  treats every unique `(domain="line_patterns", join_hash)` present in
  `bundle_pattern_inventory.csv` as a requested mapping configuration.
- Output: LinePatternElement objects in the currently open document, plus a
  deterministic CSV report (see below).

Explicitly out of scope for this PR (unchanged from the task): fill patterns
or any other domain; standards maintenance; modifying/replacing/deleting
existing project elements or configurations; continuous synchronization;
generalized dependency resolution; opening source RVTs to copy elements;
opening/saving/closing the target RVT (this utility only ever acts on the
document already open in the host session).

## Files

- `mapping/line_pattern_reconstruction.py` -- pure Python, no Revit
  dependency. CSV loading, evidence validation/blocking, segment
  reconstruction, hash reconstruction/verification, deterministic naming,
  and report formatting. Unit tested directly
  (`tests/test_line_pattern_mapping_reconstruction.py`).
- `mapping/line_pattern_revit_apply.py` -- the Revit-API half: reading
  segments back off a live `LinePatternElement`, bounded
  create-then-verify-then-commit/rollback transactions, and the
  existing-vs-create-vs-block decision per requested join_hash.
- `mapping/create_line_pattern_mappings.py` -- the Dynamo CPython3 entry
  point (`IN[0]` = input directory, `IN[1]` = report path), following the
  same `DocumentManager.Instance.CurrentDBDocument` / `IN`/`OUT` convention
  as `tools/probes/*.py`.

## Reconstruction and evidence validation

For each requested `join_hash`, the ordered segment definition
`(idx, kind, length)` is reconstructed from `pattern_settings.csv`'s
`line_pattern.segment_count` / `line_pattern.seg[NNN].kind` /
`line_pattern.seg[NNN].length` rows. The pattern is blocked (never
Revit-mutated) if:

- `join_hash` is missing (no request to key on at all -- reported separately
  with `action=skipped`, grouped by `pattern_id`, see below);
- `pattern_settings.csv` has zero rows for the join_hash (`settings_absent`);
- the `__no_items__` placeholder row is present (`no_items_marker`);
- any settings key is duplicated in the raw CSV (`duplicate_settings_key:*`);
- a required reconstruction item (`segment_count`, or any `seg[idx].kind`/
  `.length`) has `q != "ok"`;
- segment indices are missing, duplicated (via the raw-key duplicate check
  above), or non-contiguous (`segment_indices_non_contiguous`);
- the declared `segment_count` disagrees with the indexed segments found
  (`segment_count_mismatch:declared=X:found=Y`);
- a segment `kind` cannot be mapped unambiguously to the Revit API
  (`segment_kind_unmapped:idx:kind` -- only 0/1/2 = Dash/Space/Dot are
  recognized, per `domains/line_patterns.py`'s own "canonical, locked"
  mapping);
- a numeric value is invalid/non-finite, or a non-Dot segment's length is
  `<= 0` (Revit requires a positive length for Dash/Space segments).

No missing value is ever inferred.

As two additional, evidence-internal-consistency checks (not literally
required by the task's blocking-condition list, but necessary given this
utility's own reconstruction could otherwise silently diverge from what was
actually fingerprinted):

- if `line_pattern.segments_def_hash` is present with `q="ok"`, it must equal
  the hash recomputed from the reconstructed segments
  (`segments_def_hash_mismatch` if not) -- degraded (not blocked) if the
  evidence simply isn't present;
- the join_hash recomputed from the reconstructed segments (via the real
  join-key policy, see below) must equal the requested `join_hash`
  (`reconstructed_join_hash_mismatch` if not).

**Dot-length normalization**: `domains/line_patterns.py` always writes `0.0`
for a Dot segment's length at extraction time. If a (hand-edited or stale)
CSV nonetheless records a non-zero Dot length, this utility normalizes it
back to `0.0` before reconstruction/hashing (matching the extractor's own
authoritative behavior) and marks the result degraded
(`dot_length_not_normalized:idx`) rather than blocking it.

## Join-key verification (not sig_hash)

`domains/line_patterns.py` emits two different hashes for the same pattern:

- `sig_hash`, computed from `line_pattern.segments_def_hash` (exact,
  scale-sensitive identity);
- `join_hash` (via `line_patterns.join_key.v3`,
  `policies/domain_join_key_policies.json`), computed from
  `line_pattern.segments_norm_hash` (scale-invariant governance identity,
  D-017).

`bundle_pattern_inventory.csv`/`domain_patterns.csv` clustering keys on
`join_hash`, not `sig_hash` (`tools/extractor.py`'s
`cluster_id = f"{domain}|{schema}|{join_hash}"`, itself sourced from
`phase0_records.csv`'s `join_hash` column as written by
`tools/apply_join_policy.py`'s call into
`core/join_key_builder.py::build_join_key_from_policy`). This utility
verifies against `join_hash` for the same reason -- see
`mapping/line_pattern_reconstruction.py`'s module docstring, and
`tests/test_line_patterns_canonical_selectors.py` for the same
reuse pattern applied elsewhere in the repo.

`line_pattern.segments_norm_hash` itself is not emitted by
`domains/line_patterns.py` -- it is appended synthetically by
`tools/run_extract_all.py`'s `_append_line_pattern_synthetic_norm_hash()`
during the flatten stage. That function is private and embedded in a CLI
orchestrator script with its own heavy import surface, so rather than import
it directly, `mapping/line_pattern_reconstruction.py::compute_segments_norm_hash()`
is a deliberately independent reimplementation of the same per-record
algorithm -- the same "independent reimplementation, not an import" pattern
`tools/pattern_id_utils.py` already uses for `tools/extractor.py`'s private
`_stable_pattern_id()` (see `CLAUDE.md`'s note on that module).
`tests/test_line_pattern_mapping_reconstruction.py::test_segments_norm_hash_matches_run_extract_all_reference`
cross-checks the two implementations agree over a battery of synthetic
segment lists, closing the same kind of gap `CLAUDE.md` flags as an open TODO
for `pattern_id_utils.py`.

## Naming

The observed name from `pattern_names.csv` is evidence, never the identity of
the mapping element itself. Selection (`select_observed_name`):

1. only rows with `label_q == "ok"` and non-empty `label_v` are considered;
2. highest `files_count` wins;
3. ties are broken by ascending lexical order on `label_v`.

If no acceptable observed name exists, a deterministic synthetic name
(`unnamed_<short_join_hash>`) is used and the result is marked degraded.

Mapping elements are always named `MAP__<observed_name>` (sanitized for
Revit's name-character restrictions). If that name already exists in the
document:

- if it reproduces the requested `join_hash` (verified, not assumed) -- it is
  reused (`action=existing`);
- if it represents a *different* configuration, the existing element is never
  touched. A collision-safe name `MAP__<observed_name>__<short_join_hash>` is
  tried instead (reused if it already matches, created if free, or the
  request is blocked with `name_collision_unresolved` if that name is also
  claimed by yet another configuration).

`short_join_hash` is the first 12 hex characters of `join_hash`.

## Revit mutation and verification

Construction path (see `mapping/line_pattern_revit_apply.py::create_and_verify_line_pattern`):

```python
lp = Autodesk.Revit.DB.LinePattern(name)
lp.SetSegments(List[LinePatternSegment]([
    LinePatternSegment(LinePatternSegmentType.<Dash|Space|Dot>, length),
    ...
]))
element = Autodesk.Revit.DB.LinePatternElement.Create(doc, lp)
```

Each requested join_hash gets its own `Autodesk.Revit.DB.Transaction`. After
`LinePatternElement.Create`, the element's segments are read back
(`read_segments_from_element`, reusing
`domains/line_patterns.py::_lp_seg_type_id_and_name` so the "Type vs
SegmentType" API fallback and the 0/1/2 kind mapping stay in exactly one
place) and the resulting join_hash is recomputed via the same
`compute_join_hash_for_segments()` used for pre-mutation validation, still
inside the open transaction. Only if the verified join_hash equals the
requested one is the transaction committed; any mismatch, or any exception
during construction, rolls the transaction back (`RollBack`), and the
configuration is reported `blocked` / `post_creation_identity_mismatch` --
never reported as a success. No bare `except:` -- every caught exception path
returns an explicit reason string.

## Status / action vocabulary and the report

Every requested configuration receives:

- `status`: `ok` | `degraded` | `blocked` (record.v2's own vocabulary,
  reused directly from `core.record_v2`);
- `action`: `existing` | `created` | `skipped` | `blocked` -- `skipped`
  applies only to inventory rows whose `join_hash` was blank (nothing to
  reconstruct or verify at all); every other outcome is `blocked` if the
  identity could not be reproduced.

Dominance order for both row-level combination and the overall run status is
`blocked > degraded > ok` (`dominant_status()` /
`compute_run_status()`), mirroring the "worse wins" convention record.v2
already uses for `identity_quality`.

The report (`mapping/line_pattern_reconstruction.py::REPORT_FIELDS`) has one
row per unique requested `join_hash` (plus one row per distinct
blank-join_hash `pattern_id`, action=`skipped`):

```
segment_id, domain, join_hash, observed_name, mapping_name, action, status,
status_reason, revit_element_id, requested_join_hash, verified_join_hash,
source_bundle_ids, source_pattern_ids
```

`source_bundle_ids`/`source_pattern_ids` are the semicolon-joined, sorted,
deduplicated set of every `bundle_id`/`pattern_id` in
`bundle_pattern_inventory.csv` that referenced this `join_hash` -- if
multiple inventory rows reference the same join_hash, exactly one Revit
element is created/reused, and all of those bundle associations are
preserved in this one row.

Processing order (both for Revit creation and for report rows) is `join_hash`
ascending, so the same inputs against the same starting RVT always produce
the same requested configuration set, the same mapping names (including
collision resolution), and the same report.

## Verification procedure

See `docs/line_pattern_mapping_verification.md` for the manual, Revit-side
procedure demonstrating the round trip (existing pattern -> fingerprint ->
bundle-pattern-detail export -> mapping utility -> re-fingerprint -> matching
join_hash) that this utility's own post-creation check automates per pattern.

## Remaining scope for a subsequent fill-pattern PR

Not implemented here, and deliberately not designed around in this PR's
code: fill-pattern reconstruction/mutation, any shared
"materialize-a-domain-into-Revit" abstraction across domains, or a
generalized dependency-resolution layer for domains (like `line_styles`)
that reference `line_patterns` by UID. Building any of that now would be
scope creep against this PR's single-domain mandate; see the top-level PR
report for specifics on what a fill-pattern PR would need to redo versus
what could realistically be shared.
