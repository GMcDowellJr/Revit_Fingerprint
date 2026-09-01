# Step 0 findings: `compare_reference_multi.py` fan-out driver

Read-only audit of `tools/compare_reference.py` on the current `main` branch
(commit `f13c276`, "Merge pull request #479 ... zero-pad business_center_label"),
performed before writing any driver code. Every claim below was re-verified
by reading the live file, not recalled from the task prompt or prior
conversation. Line numbers refer to `tools/compare_reference.py` as of that
commit.

## 1. CLI flags on `build_arg_parser()` (lines 2008-2055)

Confirmed exactly as the prompt assumes, plus exact required/default status:

| Flag | Required | Default | Notes |
|---|---|---|---|
| `--segments-root` | yes | — | `type=Path` |
| `--registry-file` | yes | — | `type=Path` |
| `--reference-segment` | yes | — | normalized `output_folder`, not raw `segment_id` |
| `--target-segment` | no | `None` | falls back to `--reference-segment` in `main()` (line 2072) if omitted |
| `--reference` | yes | — | filename/export selector, resolved against reference segment's `file_metadata.csv` |
| `--target` | no | `None` | omit to compare against the whole target segment |
| `--out-dir` | yes | — | `type=Path` |
| `--overwrite` | no | `action="store_true"` (`False`) | |
| `--domains` | no | `None` | comma-separated; default = every domain in target's `pattern_presence_file.csv` |
| `--purge-view` | no | `"both"` | `choices=["all", "used", "both"]` |
| `--include-name-overlap` | no | `action="store_true"` (`False`) | |

No drift from the prompt's assumptions here.

## 2. `main()` exit codes (lines 2058-2298, `if __name__` at 2301-2302)

Confirmed: `raise SystemExit(main())`, and `main()` returns `int` on every
path — `0` (line 2298, full success) or `2` (two `except CompareReferenceError`
blocks: lines 2094-2096 for the out-dir pre-flight, and 2256-2262 for
segment/reference/target resolution).

**Drift from the prompt's assumption ("confirm no other exit codes are
possible"):** this is *not* fully true. `main()` has exactly one `try` block
that catches `CompareReferenceError` (lines 2098-2262), covering segment
resolution, artifact-presence checks, unit-system/schema cross-segment
gates, and reference/target selector resolution. The two calls after that
block — `run_comparisons(...)` (line 2264) and `assemble_final_outputs(...)`
(line 2279) — are **not** wrapped in any `try/except`. A repo-wide grep for
`raise CompareReferenceError` (9 call sites) shows the only one reachable
from inside `assemble_final_outputs()` is `build_file_metadata_label_lookup()`
(line 1287) — but `main()` already calls that same function once earlier,
*inside* the protected try block (lines 2136/2138), specifically so that
failure is caught there; the second call inside `assemble_final_outputs()`
(lines 1765/1767) is guaranteed not to raise given already-validated data
(the code's own comment at lines 2126-2135 says this explicitly: "redundantly
but harmlessly"). So under normal/anticipated conditions, no
`CompareReferenceError` reaches `run_comparisons()`/`assemble_final_outputs()`
uncaught.

However, **any other uncaught exception** in those two unprotected calls
(e.g. a `KeyError`/`FileNotFoundError` from a malformed or truncated
intermediate CSV written by `run_comparisons()` itself, a bug, a disk error)
would propagate out of `main()` as a Python traceback, and Python's default
`sys.exit()` behavior for an uncaught exception is **exit code 1**, not 2.
Separately, `argparse` itself calls `sys.exit(2)` on malformed CLI arguments
(bad flag, missing required arg) — coincidentally the same code as
`CompareReferenceError`, but for a different reason (this only matters if
the driver ever constructs a malformed child command line, which it
shouldn't under correct construction).

**Implication for the driver:** treat child return code `0` as
ok/degraded-per-manifest, `2` as a `CompareReferenceError`-class block
(inspect the child's `reference_comparison_report.json` /
`reference_comparison_diagnostics.json` for the real per-combo status), and
**any other non-zero code (in practice, `1`) as an unexpected crash** —
record it distinctly in `compare_reference_multi_run_report.json` rather
than assuming it means the same thing as `2`.

## 3. `check_out_dir_safety()` / `prepare_out_dir()` refusal conditions (lines 1965-2000)

`check_out_dir_safety(out_dir, segments_root, registry_file)`: refuses only
if `out_dir` (resolved) **is** or **is an ancestor of**
`segments_root.resolve()`, `registry_file.resolve()`, or
`registry_file.resolve().parent`. It does **not** check whether `out_dir` is
a *descendant* of any of those, and it does not consult a parent
`--out-root` at all — only the three candidates named above.

`prepare_out_dir(out_dir, overwrite)`: unconditionally clears (`rmtree` then
`mkdir`) `out_dir` on every run. Refuses (raises `CompareReferenceError`)
only if `out_dir` already exists, is non-empty, **and** does not already
contain this tool's own `reference_comparison_report.json` manifest file,
**and** `--overwrite` was not passed. An empty existing dir, a dir that
doesn't exist yet, or a dir already owned by a prior run of this same tool
are all accepted without `--overwrite`.

**Answering the prompt's specific question:** a parent directory that holds
many combos' out-dirs (i.e. the driver's own `--out-root`) does **not** need
to itself avoid overlapping `--segments-root`/`--registry-file` — only each
leaf `--out-dir` passed to a given child invocation is checked, and only
against exact-match-or-ancestor-of the three paths above, never
descendant-of. So the driver's `--out-root` can safely sit anywhere (even,
in principle, share a parent with `--segments-root`) as long as no
individual combo's `--out-dir` collides with `segments_root`/`registry_file`/
`registry_file`'s parent directly — which the driver's own combo-directory
naming (nested under `--out-root`) will never do by construction. One
driver-level requirement this does impose: the driver must NOT pre-create
`--out-dir` before invoking the child if it wants the child's own creation
logic to run cleanly — but pre-creating an *empty* dir is also fine per
`prepare_out_dir`'s empty-dir exception above, so either approach (create
nothing, or `mkdir(parents=True)` an empty dir) works.

## 4. `reference_comparison_summary.csv` fieldnames (`_finalize_view()` at 1113-1156, `assemble_final_outputs()` at 1706-1887, `_SUMMARY_FIELDNAMES` at 219-239)

**Drift from the prompt's assumption:** `reference_segment_id` is **NOT**
present on `reference_comparison_summary.csv` rows. `_SUMMARY_FIELDNAMES`
(lines 219-239) is exactly:

```
segment_id
purge_view
reference_bundle_id
reference_governance_role
reference_client_label
reference_discipline_label
reference_business_center_label
reference_collection_label
reference_project_label
analysis_run_id
target_export_run_id
target_governance_role
target_client_label
target_discipline_label
target_business_center_label
target_collection_label
target_project_label
domain
population_id
comparison_status
comparison_reason_codes
reference_pattern_count
target_pattern_count
shared_count
reference_only_count
target_only_count
union_count
reference_coverage_pct
jaccard
```

(the six `reference_*`/`target_*` label columns come from
`_file_metadata_label_fieldnames("reference"/"target")`, i.e.
`_FILE_METADATA_LABEL_FIELDS = ["governance_role", "client_label",
"discipline_label", "business_center_label", "collection_label",
"project_label"]`, lines 172-179.)

`segment_id` here is always the **target** segment's canonical `segment_id`
(see `_finalize_view(out_dir, view, segment_id)` called with
`segment_id=target_segment_id` from `main()`/`assemble_final_outputs()`, and
the docstring at lines 436-444 confirming `segment_id` in every output field
is the canonical registry `segment_id`, not the CLI folder selector).
`reference_segment_id` exists only in `reference_comparison_diagnostics.json`
(line 1843: `"reference_segment_id": reference_segment_id`) and
`reference_comparison_report.json`/the manifest dict returned by
`assemble_final_outputs()` (line 1869) — never in the summary or detail CSV
rows themselves.

**Implication for the driver's aggregator:** since
`reference_comparison_summary.csv` carries the *target* `segment_id` but not
`reference_segment_id`, the aggregator must read each combo's own
`reference_comparison_report.json` (the manifest — always written, success
or block, per finding 2/5 below) to get `reference_segment_id`, and stitch
it onto the row itself as an added column (the acceptance criteria's "each
row correctly tagged with its `reference`, `reference_segment_id`, and
`segment_id`" requires this to be synthesized by the driver, not read
straight off the child's summary CSV).

`reference_comparison_detail.csv` (`_DETAIL_FIELDNAMES`, lines 241-259) has
the same `segment_id`-but-no-`reference_segment_id` shape, plus
`pattern_id`, `comparison_class`, and the six `reference_revit_name*`/
`target_revit_name*` columns. Not in this driver's v1 aggregation scope, but
recorded here since it was read in the course of confirming fieldnames.

## 5. Does any output already identify the `--reference` filename?

No output file — summary, detail, semantic-changes, name-overlap, or
diagnostics — carries the literal `--reference` selector string the caller
passed on the command line. The closest values are:

- `reference_bundle_id` (summary/detail CSVs) — an internal id from the
  comparator's own bundle construction, not the filename selector.
- `resolved_reference_export_run_id` (manifest JSON only, e.g.
  `assemble_final_outputs()` line 1871 / `write_top_level_blocked()` line
  1944) — the *resolved* `export_run_id` (from `file_metadata.csv`), which
  may differ textually from the raw `--reference` selector string
  (`resolve_export_run_id()`, lines 372-408, does exact match first, then a
  stem-normalized fallback via `_export_stem()`).
- The six `reference_*` file-metadata label columns (governance_role,
  client_label, etc.) — organizational labels, not the file identity.

**Confirmed per the prompt's finding #5 assumption:** the aggregator will
need to add its own `reference` column, sourced from the driver's own
`--references` CSV row (the raw selector string as typed), to disambiguate
multiple references drawn from the same `reference_segment` in the
long-term multi-reference case. Optionally the driver can *also* surface
`resolved_reference_export_run_id` from each combo's manifest as a second,
distinct column for traceability, but the `reference` column itself must
come from the driving CSV, not from any child output.

## 6. Invocation convention: `python tools/compare_reference.py ...`

Confirmed `tools/compare_reference.py` is a standalone script (`if __name__
== "__main__": raise SystemExit(main())`, lines 2301-2302), not a package
entry point.

**Drift from the prompt's assumption:** a repo-wide grep for
`compare_reference\.py` found **no existing caller that invokes it as a
subprocess** — not in `tools/corpus_update_runbook.ps1` (no match at all),
not in `tools/corpus_update_runbook.py` (no match), and not in
`tools/run_segment_orchestrator.py` (one match, but it's a comment
referencing the tool by name at line 856, not an invocation). The only
current callers are `tests/test_compare_reference.py` and
`tests/test_compare_reference_name_overlap.py`, and those call `cr.main()`
in-process after doing `sys.path.insert(0, str(repo_root))` /
`sys.path.insert(0, str(repo_root / "tools"))` (lines 28-34) — not a
subprocess convention at all.

Since there is no existing subprocess-invocation precedent for this
specific tool, the closest established convention in this codebase is
`tools/run_segment_orchestrator.py`'s own subprocess pattern for *other*
tools it drives (e.g. `run_extract_all.py`, `generate_name_key_patterns.py`,
lines 1546-1557/1596-1602): build `cmd = [sys.executable, str(repo_root /
"tools" / "<script>.py"), ...str(...) args...]`, then
`subprocess.run(cmd, ..., cwd=str(repo_root))` — i.e. invoke with the
current Python interpreter, an absolute path to the script, all path
arguments passed as `str(Path)`, and `cwd` pinned to the repo root (rather
than relying on the caller's own cwd or manipulating `PYTHONPATH`). The
driver should follow this exact pattern: `[sys.executable, str(repo_root /
"tools" / "compare_reference.py"), "--segments-root", str(...), ...]` run
with `cwd=str(repo_root)`.

## Additional findings not explicitly asked for, but load-bearing for Step 1

### `require_segment_artifacts()` exact required-artifact set (lines 480-517)

Always required (`required_files` dict, lines 499-503):
- `results/records/records.csv`
- `results/records/file_metadata.csv`
- `results/analysis/pattern_presence_file.csv`

Additionally required only when `require_domain_patterns=True` (cross-segment
mode, or same-segment mode too as of `main()`'s call sites — see below):
- `results/analysis/domain_patterns.csv`

Plus, for every entry in `views` (`["all", "used"]` when `--purge-view
both`, the default): `results/bundle_analysis/<view>/` must exist as a
directory (no specific file inside it is checked at this stage — but
`compare_dir` inside `run_comparisons()`/`_finalize_view()` reads
`<domain>/membership_matrix.csv` under it later, so a synthetic fixture
needs that populated too for the compare step itself to succeed rather than
just pass this pre-flight check).

Note `main()` calls `require_segment_artifacts(reference_segment_root,
views, require_domain_patterns=not same_segment)` for the reference side
(line 2109-2111) and `require_segment_artifacts(target_segment_root, views,
require_domain_patterns=True)` for the target side unconditionally (line
2123) — so **the target side always requires `domain_patterns.csv`**, even
in same-segment mode; only the reference side's requirement depends on
`same_segment`. `corpus_manifest.csv` (schema_version) is read by
`read_extractor_schema_version()` (lines 566-572) but is soft-optional —
missing or absent, it just yields `""`, which then trips the
`REASON_CROSS_SEGMENT_SCHEMA_MISMATCH` gate only in cross-segment mode
(never same-segment). A synthetic fixture exercising cross-segment combos
should include a matching `corpus_manifest.csv` `schema_version` on both
sides to reach `ok`/`degraded` instead of always blocking there.

`tests/test_compare_reference.py`'s own `_build_segment()` helper
(lines 75-212) already builds exactly this minimal tree (registry row +
`file_metadata.csv` + `records.csv` + `pattern_presence_file.csv` +
`corpus_manifest.csv` + `domain_patterns.csv` + per-view
`membership_matrix.csv`) — Step 1's synthetic fixture-builder should mirror
this helper's shape directly rather than reinventing it, since it's already
proven to drive `compare_reference.py` end-to-end to `ok` status.

### Whether `reference` filenames need path-sanitizing for use as a directory-name component

`--reference` is resolved via `resolve_export_run_id()` (lines 372-408)
against `file_metadata.csv`'s `export_run_id` column — an arbitrary string
with no format constraint enforced by `compare_reference.py` itself (in
practice, per `tools/build_segment_manifest.py`'s conventions elsewhere in
this repo, `export_run_id` is often filename-derived and can contain
spaces, dots, and other characters that are legal in a Windows/Revit
central-file name but not safe to use verbatim as a single path segment
across platforms, e.g. embedded path separators are not ruled out by
anything in this function). **The driver must sanitize `reference` (and, for
robustness, `reference_segment`/`target_segment`) before using it as a
directory-name component** — e.g. replace any character outside
`[A-Za-z0-9._-]` with `_`, and should not assume the raw CSV value is
filesystem-safe as-is.

### `--workers` default/cap convention (`tools/compare_cross_segment.py`, lines 4724-4798)

`compare_cross_segment.py` defines `resolve_worker_count(value, headroom=2)`
(lines 4731-4745) and `--workers` with `default="auto"` (line 4788,
`ap.add_argument("--workers", default="auto", ...)`). `"auto"` resolves to
`max(1, os.cpu_count() - 2)` (falling back to `4` if `os.cpu_count()` returns
`None`), capped at `_WIN32_MAX_WORKERS = 61` on `sys.platform == "win32"`
(documented as `ProcessPoolExecutor`'s hard `max_workers` ceiling on Windows
— `WaitForMultipleObjects` handle-count limit). An explicit integer value is
accepted as-is; `< 1` is rejected with `sys.exit("[error] --workers must be
>= 1")` (line 4798). **The driver should mirror this exact convention**:
`--workers` accepting int-or-`"auto"`, same `headroom=2` default, same
Windows cap, same `>= 1` validation — rather than inventing a different
default.

## Summary: no-code confirmation

All six numbered questions in the prompt were checked against the live
`main` branch. Two areas drift materially from the prompt's stated
assumptions and must inform Step 1's design:

1. **Exit codes are not strictly {0, 2}** — `run_comparisons()`/
   `assemble_final_outputs()` run outside the `CompareReferenceError`-catching
   `try` block, so an unanticipated exception there would surface as exit
   code 1 (Python's default), not 2. The driver's subprocess-return-code
   handling should treat "not 0, not 2" as a distinct "child crashed
   unexpectedly" case, not silently coerce it into the 2/blocked bucket.
2. **`reference_segment_id` is absent from `reference_comparison_summary.csv`**
   (present only in the JSON manifest/diagnostics) — the aggregator must
   read each combo's `reference_comparison_report.json` to recover it, and
   stitch both `reference_segment_id` and the driving `reference` column
   onto every aggregated row itself; neither is present in the raw summary
   CSV as shipped today.

Every other assumption in the prompt (CLI flags, `check_out_dir_safety`/
`prepare_out_dir` refusal scope, the standalone-script invocation shape, and
the general existence of a `reference`-identifying gap requiring an added
column) is confirmed as stated, with the additional detail above (exact
fieldname lists, exact required-artifact set, path-sanitizing need, and the
`--workers` convention to mirror) filled in for Step 1.
