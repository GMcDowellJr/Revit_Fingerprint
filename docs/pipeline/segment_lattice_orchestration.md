# Segment & Governance Lattice Orchestration

## What this stage does

This is Phase-1 stage 2 of the pipeline-doc effort (see `docs/pipeline_doc_topology_findings.md`
for the corpus-wide topology this stage sits in, and `docs/pipeline/name_key_projection.md` for
stage 1's sibling trace-block pass). It covers the segment/governance lattice leg of
`tools/corpus_update_runbook.ps1`'s **Run C**:

1. **Run C1** (`tools/build_segment_manifest.py`, invoked at `corpus_update_runbook.ps1:186`)
   reads the post-human-edit `file_metadata.csv` and builds the full segmentation lattice —
   every subset of (`unit_system`, `governance_role`, `client_label`, `discipline_label`,
   `business_center_label`) — as `segment_manifest.csv`, classifies each segment into a
   `run_type` (`bundle` / `reference` / `registration` / `skip`), and writes the companion
   `run_registry.csv` and `segment_membership.csv`.
2. **Run C2** (`tools/run_segment_orchestrator.py`, invoked at `corpus_update_runbook.ps1:245`)
   reads those three files and, per `bundle`/`reference` segment in level order, runs the
   `patterns` stage (`tools/run_extract_all.py --stages patterns`, segment-scoped) then the
   bundle-mining stage (`tools/bundle_analysis/run_bundle_analysis.py`) for `bundle` segments,
   updating `run_registry.csv` in place after each segment and pre-merging per-domain outputs
   for Power BI.
3. **Run C2.5** (`tools/build_results_registry.py`, invoked at `corpus_update_runbook.ps1:266`)
   folds `segment_manifest.csv` and the now-updated `run_registry.csv` into a single
   `results_registry.csv` — one row per segment, meant as a stable BI query surface instead of
   hand-wiring individual segment output folders.

`tools/extract_segment_subtree.py` is a related but standalone diagnostic: given a seed
segment, it walks `segment_manifest.csv`'s `parent_segment_id` chain to pull one segment's
ancestor subtree out of `tools/compare_cross_segment.py`'s already-produced
`cross_segment_*.csv`/`comparison_registry.csv` output. It has no in-repo caller and is not
part of the Run C chain — see its own section below.

## Cross-file lineage (the "connective tissue" this doc traces)

`build_segment_manifest.py` writes `segment_manifest.csv` (`MANIFEST_FIELDNAMES`: `segment_id`,
`parent_segment_id`, `segment_level`, `unit_system`, `governance_role`, `client_label`,
`discipline_label`, `business_center_label`, `collection_label`, `extra_dimensions`,
`ancestor_segment_ids`, `run_type`, `file_count`, `has_seed_file`, `population_hash`, `notes`,
`segment_purpose`, `segment_label`) and `run_registry.csv` (`REGISTRY_FIELDNAMES`: `segment_id`,
`parent_segment_id`, `run_type`, `population_hash`, `conformance_reference_mode`,
`output_folder`, `status`, `last_run_utc`, `notes`, `segment_purpose`, `segment_label`), plus
`segment_membership.csv` (`MEMBERSHIP_FIELDNAMES`: `segment_id`, `export_run_id`, `is_seed`) —
the sidecar that replaced the old pipe-delimited `export_run_ids` column on
`segment_manifest.csv` once populations grew past spreadsheet cell limits.

`run_segment_orchestrator.py`'s `load_manifest()`/`load_registry()`/`load_membership()` read
those three files by exactly those column names, and its `_run_one_segment()` mutates the
in-memory `registry` rows' `status`/`last_run_utc`/`notes` columns in place before persisting
them back to `run_registry.csv` via `write_registry_atomic()` after every segment — so
`run_registry.csv` is a live, incrementally-updated file across one orchestrator run, not a
single end-of-run batch write.

`build_results_registry.py`'s `build_results_registry_rows()` then joins `segment_manifest.csv`
(for `segment_id`/`parent_segment_id`/`segment_level`/`governance_role`) against the
now-current `run_registry.csv` (for `output_folder`/`run_type`/`status`/`last_run_utc`) into
`results_registry.csv`'s fixed `RESULTS_REGISTRY_FIELDNAMES` shape — every field name traced
above is a real, checked column name from the producing script's own module-level
`*_FIELDNAMES` constant, not a paraphrase.

## `../../tools/build_segment_manifest.py`

### `_read_csv(path: Path) -> tuple` — [build_segment_manifest.py:L43](../../tools/build_segment_manifest.py#L43)

- **Reads:** `path` -- a Path to file_metadata.csv (main()'s --metadata-file), or, later in main(), to an existing run_registry.csv/segment_membership.csv under --out-dir (for incremental-build carry-over).
- **Calls:** none (stdlib csv.DictReader only).
- **Thresholds:** none.
- **Returns:** (fieldnames: list[str], rows: list[dict[str,str]]) -- fieldnames preserves the file's header order (used by main() for REQUIRED_COLUMNS presence checking); rows has every value normalized to str with None replaced by "". Consumed by main() (for file_metadata.csv, and for existing_registry_rows/ existing_membership_rows when those files already exist).

### `_atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None` — [build_segment_manifest.py:L65](../../tools/build_segment_manifest.py#L65)

- **Reads:** `path`, `fieldnames`, `rows` -- caller-supplied; main() calls this three times with (manifest_path, MANIFEST_FIELDNAMES, manifest_rows), (registry_path, REGISTRY_FIELDNAMES, registry_rows), and (membership_path, MEMBERSHIP_FIELDNAMES, membership_rows).
- **Calls:** none (stdlib csv.DictWriter, tempfile.NamedTemporaryFile).
- **Thresholds:** none.
- **Returns:** None; writes `path` atomically (temp file in the same directory, then Path.replace()) so segment_manifest.csv/run_registry.csv/ segment_membership.csv are never observed half-written.

### `_population_hash(export_run_ids: List[str]) -> str` — [build_segment_manifest.py:L86](../../tools/build_segment_manifest.py#L86)

- **Reads:** `export_run_ids` -- list[str], from _build_segments()'s per-key `eids` (the deduplicated, sorted export_run_id membership of one candidate segment).
- **Calls:** hashlib.sha1() (stdlib).
- **Thresholds:** none.
- **Returns:** 40-char hex sha1 of the "|"-joined, sorted export_run_ids; written into segment_manifest.csv's population_hash column and consumed downstream by tools/run_segment_orchestrator.py's incremental-skip logic (a changed population_hash for a segment_id triggers re-processing) and by _build_segments()'s own "redundant_single_child" detection (a parent whose population_hash matches a direct child's is collapsed to run_type="registration").

### `_sanitize_folder(segment_id: str) -> str` — [build_segment_manifest.py:L111](../../tools/build_segment_manifest.py#L111)

- **Reads:** `segment_id` -- a "|"-joined dimension-value string, from _build_registry()'s per-row `sid` (new-segment folder assignment) or from main()'s existing-registry fallback for a segment whose registry row has no stored output_folder.
- **Calls:** none (re.sub() only).
- **Thresholds:** _UNSAFE_FOLDER_CHARS (module-level compiled regex, l.61: characters replaced with "_"); _BLANK_SELECTED_FOLDER_TOKEN = "no_external_client" (module constant, l.67: the literal substituted for an empty-but-selected segment_id part, to keep it distinguishable from a not-selected dimension -- see the comment block above for the full rationale).
- **Returns:** str; consumed by _build_registry() as a new segment's output_folder (with a "_2", "_3", ... suffix appended on a folder-name collision).

### `_build_membership_rows(manifest_rows: List[Dict[str, str]]) -> List[Dict[str, str]]` — [build_segment_manifest.py:L150](../../tools/build_segment_manifest.py#L150)

- **Reads:** `manifest_rows` -- _build_segments()'s return value, passed in by main(); reads each row's segment_id plus its internal (not written to segment_manifest.csv) export_run_ids/seed_export_run_ids pipe-delimited fields.
- **Calls:** none.
- **Thresholds:** none.
- **Returns:** list[dict] with keys segment_id/export_run_id/is_seed ("true"/"false" strings), sorted by (segment_id, export_run_id); this is segment_membership.csv's row shape (MEMBERSHIP_FIELDNAMES). Consumed by main() (written to segment_membership.csv) and, downstream, by tools/run_segment_orchestrator.py's load_membership() and tools/extract_segment_subtree.py -- both read this file grouped by segment_id -> sorted export_run_ids.

### `_membership_by_segment(membership_rows: List[Dict[str, str]]) -> Dict[str, List[str]]` — [build_segment_manifest.py:L183](../../tools/build_segment_manifest.py#L183)

- **Reads:** `membership_rows` -- previously-written segment_membership.csv rows (segment_id/export_run_id/is_seed), read by main() via _read_csv() when membership_path already exists on disk.
- **Calls:** none.
- **Thresholds:** none.
- **Returns:** Dict[segment_id, sorted list[export_run_id]]; consumed by main() as `existing_membership`, passed to _build_registry() so population_changed diffing (new_files/removed_files notes) can compare against the prior run's actual file set instead of only its population_hash.

### `_append_note(row, k, v='')` — [build_segment_manifest.py:L207](../../tools/build_segment_manifest.py#L207)

- **Reads:** `row` -- a manifest or registry row dict, mutated in place; `k`, `v` -- caller-supplied note key and optional value, from _build_segments() (e.g. "redundant_single_child") and _build_registry() (e.g. "population_changed", "new_files", "run_type_changed").
- **Calls:** none.
- **Thresholds:** none.
- **Returns:** None; mutates `row["notes"]` in place, appending "k" or "k:v" after a "|" separator if notes already has content. Consumed wherever the caller later writes that row to segment_manifest.csv/run_registry.csv (the `notes` column).

### `_invalid_required_value_reason(value: str) -> 'str | None'` — [build_segment_manifest.py:L230](../../tools/build_segment_manifest.py#L230)

- **Reads:** `value` -- one raw file_metadata.csv cell value, from _validate_required_metadata()'s per-row/per-field loop.
- **Calls:** is_na_token() (tools/na_token.py, imported at module top).
- **Thresholds:** none named beyond the blank-string and is_na_token() checks themselves (there is no separate constant table of "invalid" values here).
- **Returns:** "missing_value" | "not_applicable_sentinel" | None; consumed by _validate_required_metadata() to decide whether to emit a diagnostic for that row/field.

### `_invalid_dimension_value_reason(value: str) -> 'str | None'` — [build_segment_manifest.py:L265](../../tools/build_segment_manifest.py#L265)

- **Reads:** `value` -- one raw file_metadata.csv cell value, from _validate_required_metadata()'s per-row/per-field loop, only for fields in _DIMENSION_FIELD_NAMES.
- **Calls:** none.
- **Thresholds:** the literal `";"` character (D-028, per this function's own docstring above) -- hardcoded, not a named constant.
- **Returns:** "semicolon_not_allowed" | None; consumed by _validate_required_metadata() as an additional check layered on top of _invalid_required_value_reason() for DIMENSION_CONFIG fields only.

### `_validate_required_metadata(rows: List[Dict[str, str]]) -> List[Dict[str, str]]` — [build_segment_manifest.py:L292](../../tools/build_segment_manifest.py#L292)

- **Reads:** `rows` -- file_metadata.csv rows as read by main() via _read_csv(); reads each row's REQUIRED_ROW_FIELDS values (export_run_id, unit_system, governance_role, client_label, discipline_label, business_center_label).
- **Calls:** _invalid_required_value_reason(); _invalid_dimension_value_reason() (only for _DIMENSION_FIELD_NAMES fields).
- **Thresholds:** REQUIRED_ROW_FIELDS (module constant, l.24); _DIMENSION_FIELD_NAMES (module constant, l.151, derived from DIMENSION_CONFIG).
- **Returns:** list[dict] diagnostics (row_number/export_run_id/field/raw_value/reason), empty if every row is valid; never raises. Consumed by main(), which blocks the entire build (returns 1, writes nothing) if this list is non-empty.

### `_normalize_rows(rows: List[Dict[str, str]]) -> 'tuple[List[Dict[str, str]], List[tuple]]'` — [build_segment_manifest.py:L360](../../tools/build_segment_manifest.py#L360)

- **Reads:** `rows` -- file_metadata.csv rows, from main() (post-validation) and from _build_segments() (which re-normalizes internally even when called directly, e.g. from tests, since normalization is idempotent).
- **Calls:** none (is_na_token() is NOT called here -- DIMENSION_CONFIG fields are exempt from sentinel folding, per the "Sentinel handling" paragraph above).
- **Thresholds:** DIMENSION_CONFIG (module constant, l.35-41: which fields get normalized and how -- root/governance/cut types); _GOVERNANCE_ROLE_CANONICAL (module constant, l.125-127: case-insensitive role-name canonicalization map); the business_center_label zero-pad threshold `len(raw) < 4` (hardcoded literal, per the Excel-truncation rationale above).
- **Returns:** (normalized_rows: list[dict], changes: list[tuple(field, raw_value, normalized_value)]); normalized_rows is consumed by _build_segments() to construct segment keys; `changes` is consumed by main() to emit aggregated "[WARN] Normalized ..." diagnostics.

### `_build_segments(rows: List[Dict[str, str]], min_files: int, enable_cross_org_template_bundles: bool=False, enable_parent_bundle_runs: bool=False) -> List[Dict[str, str]]` — [build_segment_manifest.py:L458](../../tools/build_segment_manifest.py#L458)

- **Reads:** `rows` -- file_metadata.csv rows (post _validate_required_metadata(), pre- or post-normalization -- this function re-normalizes internally via _normalize_rows()); `min_files` -- int, from main()'s --min-files (default 3); `enable_cross_org_template_bundles`/`enable_parent_bundle_runs` -- bool, from main()'s --enable-cross-org-template-bundles/--enable-parent-bundle-runs flags.
- **Calls:** _normalize_rows(); _population_hash() (per segment key); _append_note() (for below_min_files/seed_only/redundant_single_child notes); itertools.combinations() (stdlib, to enumerate the non-root-field powerset); nested _subset_to_id() (many times, to render a frozenset key as a "|"-joined segment_id string) and nested child_span() (once per row with children, to classify "multi_client" vs "single_client" for segment_purpose assignment).
- **Thresholds:** DIMENSION_CONFIG (module constant, l.35-41: exactly one root + one governance dimension enforced by a raised ValueError, plus the cut dimensions); SEED_ROLES = {"Template","Container"} (module constant, l.15: which governance_role values count a file as a "seed" for has_seed_file/ seed_export_run_ids); `min_files` (run_type "bundle" threshold, passed in, not hardcoded); the run_type/segment_purpose assignment itself is a long sequential if/elif chain over (segment_level, is_*_cut booleans, role, run_type) combinations -- see notes below.
- **Returns:** list[dict] with MANIFEST_FIELDNAMES-shaped keys plus two internal-only columns export_run_ids/seed_export_run_ids (pipe-delimited, consumed by _build_membership_rows() and _build_registry() but never written to segment_manifest.csv itself, since MANIFEST_FIELDNAMES excludes them), sorted by (segment_level, segment_id). Consumed by main() as `manifest_rows`, then passed to _build_membership_rows() and _build_registry().
- **Notes:** (mechanical-extraction risk) `rows_out` entries are mutated in place across 3 sequential passes within this one function (run_type assignment, segment_purpose/segment_label assignment, then the redundant-population-hash collapse pass) -- a naive single-pass static parser could misread this as dead/overwritten state rather than sequential refinement, since a row's run_type/segment_purpose/segment_label set by an earlier pass can be overwritten by a later one (the redundant-hash pass in particular). The segment_purpose/segment_label if/elif chain (roughly l.502-537 as originally read) is sequential-control-flow-as-policy: the ~35-member governance-purpose taxonomy (population_denominator/client_population/cross_template_agreement/...) exists only as branch order in this chain, not as a lookup table, so determining "what purpose does segment X get" requires reading the chain top-to-bottom rather than consulting one data structure.

### `_build_segments._subset_to_id(key: frozenset) -> str` — [build_segment_manifest.py:L524](../../tools/build_segment_manifest.py#L524)

- **Reads:** `key` -- a frozenset of (field, value) tuples; closes over `cfg_fields` (the enclosing _build_segments()'s DIMENSION_CONFIG field-name list, root dimension first).
- **Calls:** none.
- **Thresholds:** none (field order comes from the closed-over `cfg_fields` list, not re-derived here).
- **Returns:** str, e.g. "imperial|Template|Acme"; called repeatedly throughout _build_segments() (populations grouping, project_presence_by_l2 keys, segment_id/parent_id/ancestor_segment_ids construction) -- this is the single place segment_id strings are actually assembled from a dimension-value key.

### `_build_segments.child_span(r)` — [build_segment_manifest.py:L692](../../tools/build_segment_manifest.py#L692)

- **Reads:** `r` -- one row from `rows_out`, passed by the purpose/label assignment loop; closes over `row_to_key`, `key_to_children`, `key_to_row` (all built earlier in the enclosing _build_segments() call).
- **Calls:** none (set comprehension only).
- **Thresholds:** none named -- "distinct scope" is `client_label or business_center_label` on a level-3 child, treated as mutually exclusive per the comment above this function.
- **Returns:** "multi_client" if more than one distinct client_label/ business_center_label value appears among direct level-3 children, else "single_client"; consumed only by the segment_purpose if/elif chain (distinguishes "cross_org_template_pool" from "redundant_single_child" for a level-2/3 role_alone Template segment).

### `_build_registry(manifest_rows: List[Dict[str, str]], existing_registry: List[Dict[str, str]] | None=None, existing_membership: Dict[str, List[str]] | None=None) -> List[Dict[str, str]]` — [build_segment_manifest.py:L807](../../tools/build_segment_manifest.py#L807)

- **Reads:** `manifest_rows` -- _build_segments()'s return value, from main(); reads segment_id/parent_segment_id/run_type/population_hash/segment_purpose/ segment_label/export_run_ids per row. `existing_registry` -- prior run_registry.csv rows (Optional, from main() via _read_csv() when registry_path already exists); reads segment_id/output_folder/status/ last_run_utc/notes/conformance_reference_mode/population_hash/run_type per row. `existing_membership` -- Dict[segment_id, sorted export_run_ids] from _membership_by_segment() on a prior segment_membership.csv (Optional).
- **Calls:** _sanitize_folder() (new segments only); _append_note() (population_changed/new_files/removed_files/run_type_changed notes).
- **Thresholds:** `{"bundle", "reference"}` (hardcoded literal set, used twice: eligible_rows filter and the run_type-changed detection) -- the only two run_types that get a run_registry.csv row at all; "latest" (hardcoded literal: the only implemented conformance_reference_mode).
- **Returns:** list[dict], REGISTRY_FIELDNAMES-shaped (segment_id/parent_segment_id/ run_type/population_hash/conformance_reference_mode/output_folder/status/ last_run_utc/notes/segment_purpose/segment_label); consumed by main() as `registry_rows`, written to run_registry.csv, and subsequently read and mutated in place by tools/run_segment_orchestrator.py's load_registry()/ write_registry_atomic() after each segment run. Writes a `[WARN]` line to stderr (not the return value) for any segment_id present in `existing_registry` but absent from the new manifest (dropped_ids), naming folders under segments/ the caller should manually review.

### `_print_summary(manifest_path: Path, registry_path: Path, manifest_rows: List[Dict[str, str]], min_files: int) -> None` — [build_segment_manifest.py:L972](../../tools/build_segment_manifest.py#L972)

- **Reads:** `manifest_path`, `registry_path` -- Paths, used only for the "written:" lines; `manifest_rows` -- _build_segments()'s return value, from main(); `min_files` -- int, from main()'s --min-files, for the "Skipped (below min_files=N)" label.
- **Calls:** none (print() only).
- **Thresholds:** none beyond `min_files` itself (caller-supplied, used only in the printed label text).
- **Returns:** None; side effect is stdout output only. Called once by main(), as the final step of a successful (non-blocked) build.

### `main(argv: List[str] | None=None) -> int` — [build_segment_manifest.py:L1021](../../tools/build_segment_manifest.py#L1021)

- **Reads:** CLI args --metadata-file (required), --out-dir (required), --min-files (default 3), --enable-cross-org-template-bundles, --enable-parent-bundle-runs. Reads file_metadata.csv at --metadata-file; reads any pre-existing run_registry.csv/segment_membership.csv under --out-dir for incremental carry-over.
- **Calls:** _read_csv() (metadata file, plus existing registry/membership if present); _validate_required_metadata(); _normalize_rows(); _build_segments(); _membership_by_segment(); _build_registry(); _build_membership_rows(); _atomic_write_csv() (x3: segment_manifest.csv, run_registry.csv, segment_membership.csv); _print_summary().
- **Thresholds:** REQUIRED_COLUMNS (module constant, l.17: header-presence check); REQUIRED_ROW_FIELDS (module constant, l.24, via _validate_required_metadata()); KNOWN_ROLES = {"Project","Template","Container","Generic",""} (l.819 as originally read: unrecognized governance_role values get a [WARN], not a block).
- **Returns:** int exit code (1 on missing/invalid --metadata-file, missing required columns, or any _validate_required_metadata() diagnostic -- in every blocking case, no output file is written; 0 on success). Invoked as **Run C1** (l.186 of tools/corpus_update_runbook.ps1); its three outputs (segment_manifest.csv, run_registry.csv, segment_membership.csv) are consumed by **Run C2** (tools/run_segment_orchestrator.py) and **Run C2.5** (tools/build_results_registry.py).

## `../../tools/run_segment_orchestrator.py`

### `load_manifest(path: Path) -> Dict[str, dict]` — [run_segment_orchestrator.py:L69](../../tools/run_segment_orchestrator.py#L69)

- **Reads:** `path` -- Path to segment_manifest.csv, from run_orchestrator()'s --manifest-file (tools/build_segment_manifest.py's MANIFEST_FIELDNAMES output: segment_id, parent_segment_id, segment_level, unit_system, governance_role, client_label, discipline_label, business_center_label, run_type, file_count, ...).
- **Calls:** none (stdlib csv.DictReader).
- **Thresholds:** none.
- **Returns:** Dict[segment_id, row-dict]; consumed by run_orchestrator() as `manifest`, passed to build_run_plan()/validate_membership_against_manifest() and read per segment (segment_level, governance_role, etc.) throughout _run_one_segment().

### `load_registry(path: Path) -> List[dict]` — [run_segment_orchestrator.py:L93](../../tools/run_segment_orchestrator.py#L93)

- **Reads:** `path` -- Path to run_registry.csv, from run_orchestrator()'s --registry-file (tools/build_segment_manifest.py's REGISTRY_FIELDNAMES output: segment_id, parent_segment_id, run_type, population_hash, conformance_reference_mode, output_folder, status, last_run_utc, notes, segment_purpose, segment_label).
- **Calls:** none (stdlib csv.DictReader).
- **Thresholds:** none.
- **Returns:** list[dict], one row per registered segment; consumed by run_orchestrator() as `registry`, mutated in place by _run_one_segment() (status/last_run_utc/ notes) under registry_lock and persisted via write_registry_atomic() after each segment.

### `load_membership(path: Path) -> Dict[str, List[str]]` — [run_segment_orchestrator.py:L113](../../tools/run_segment_orchestrator.py#L113)

- **Reads:** `path` -- Path to segment_membership.csv, from run_orchestrator()'s --membership-file (default: sibling of --manifest-file); reads its segment_id/export_run_id columns (tools/build_segment_manifest.py's MEMBERSHIP_FIELDNAMES output).
- **Calls:** none (stdlib csv.DictReader).
- **Thresholds:** none.
- **Returns:** Dict[segment_id, sorted list[export_run_id]]; consumed by run_orchestrator() as `membership`, passed to validate_membership_against_manifest() and used throughout (_run_one_segment()'s export_run_ids, preshard's segment_plans allowed_ids, dry-run's file_count) as the authoritative per-segment file population.

### `write_registry_atomic(path: Path, rows: List[dict]) -> None` — [run_segment_orchestrator.py:L145](../../tools/run_segment_orchestrator.py#L145)

- **Reads:** `path`, `rows` -- caller-supplied; _run_one_segment() calls this with registry_file and the shared in-memory `registry` list (mutated in place under registry_lock immediately before this call).
- **Calls:** none (stdlib csv.DictWriter).
- **Thresholds:** none.
- **Returns:** None; no-op if `rows` is empty. Writes run_registry.csv's exact current in-memory field set (fieldnames = list(rows[0].keys())) atomically (temp file + Path.replace()). Called by _run_one_segment() after every segment's status update, so run_registry.csv reflects live progress rather than only a final batch write.

### `utc_now_iso() -> str` — [run_segment_orchestrator.py:L171](../../tools/run_segment_orchestrator.py#L171)

- **Reads:** system clock (datetime.now(timezone.utc)).
- **Calls:** none (stdlib datetime).
- **Thresholds:** the format string "%Y-%m-%dT%H:%M:%SZ" is a hardcoded literal.
- **Returns:** str; consumed by _run_one_segment() (registry last_run_utc) and run_orchestrator() (run_start_utc/run_end_utc for the run summary).

### `compute_worker_split(total_budget: Optional[int]=None, headroom: int=2, segment_workers: Optional[int]=None) -> tuple[int, int]` — [run_segment_orchestrator.py:L184](../../tools/run_segment_orchestrator.py#L184)

- **Reads:** `total_budget` -- Optional int, from main() only when --workers is not "auto" (the explicit-N path calls this with segment_workers=N and total_budget left None, so it's derived from os.cpu_count() internally); `headroom` -- int, default 2 (not overridden by any caller in this file); `segment_workers` -- Optional int, from main()'s parsed --workers value in the explicit-N branch.
- **Calls:** os.cpu_count() (stdlib).
- **Thresholds:** `headroom = 2` (default param); the hardcoded (4, 4) fallback when os.cpu_count() returns None; the sqrt-biased split constant `0.8` (`round(math.sqrt(total_budget) * 0.8)`) for the auto (segment_workers=None) case.
- **Returns:** (segment_workers, domain_workers) tuple of ints; consumed by main() to set args.workers/args.bundle_workers, for both the "auto" (--workers auto) and explicit-N (coordinate bundle_workers to the same CPU budget) cases.

### `_write_run_summary(segments_root: Path, run_start_utc: str, run_end_utc: str, total_elapsed_s: int, segment_results: List[Dict], workers: int, bundle_workers: int, workers_auto: bool) -> Path` — [run_segment_orchestrator.py:L239](../../tools/run_segment_orchestrator.py#L239)

- **Reads:** `segments_root` -- Path, from run_orchestrator()'s --segments-root; `run_start_utc`/`run_end_utc` -- str, from run_orchestrator()'s utc_now_iso() calls bracketing the run; `total_elapsed_s` -- int, run_orchestrator()'s wall-clock elapsed; `segment_results` -- list[dict], accumulated by run_orchestrator() from every _run_one_segment() return value plus skip/exception entries (keys: segment_id/status/level/files/prepare_s/ patterns_s/bundle_s/bi_merge_s/total_s/worker_id/patterns_top5/failure_note); `workers`/`bundle_workers`/`workers_auto` -- from run_orchestrator()'s args.
- **Calls:** none (str.format()/print-style formatting only).
- **Thresholds:** none named -- column widths, "top 3"/"top-5" slicing (`sorted_by_pat[:3]`, `[:5]`), and the 120-char failure-note truncation (`[:120]`) are hardcoded literals.
- **Returns:** Path to the written run_summary.txt (segments_root/run_summary.txt, written atomically via a ".tmp" sibling + Path.replace()); consumed by run_orchestrator() only to print the path, not read back.

### `run_step(cmd: List[str]) -> subprocess.CompletedProcess` — [run_segment_orchestrator.py:L391](../../tools/run_segment_orchestrator.py#L391)

- **Reads:** `cmd` -- list[str], a subprocess argv; no caller in this file currently invokes run_step() directly (run_step_capture()/run_step_log() are used instead throughout _run_one_segment()/run_orchestrator()) -- retained as a simple raising variant.
- **Calls:** subprocess.run() (stdlib, check=True).
- **Thresholds:** none.
- **Returns:** subprocess.CompletedProcess; raises subprocess.CalledProcessError on non-zero exit (check=True).

### `run_step_capture(cmd: List[str], cwd: Optional[str]=None) -> tuple[int, str, str]` — [run_segment_orchestrator.py:L407](../../tools/run_segment_orchestrator.py#L407)

- **Reads:** `cmd` -- list[str] subprocess argv; `cwd` -- Optional[str] working directory; no caller in this file currently invokes run_step_capture() directly (run_step_log() is used throughout instead) -- retained as a capture-without-file-logging variant.
- **Calls:** subprocess.run() (stdlib, capture_output=True).
- **Thresholds:** `-20` (last-20-lines tail, hardcoded literal, matching run_step_log()'s own convention).
- **Returns:** (returncode, tail (last 20 stderr lines joined), full stderr); never raises on non-zero exit (no check=True).

### `run_step_log(cmd: List[str], log_path: Path, cwd: Optional[str]=None) -> tuple[int, str, str]` — [run_segment_orchestrator.py:L427](../../tools/run_segment_orchestrator.py#L427)

- **Reads:** `cmd` -- list[str] subprocess argv, from _run_one_segment()'s per-step command lists (extract_cmd/name_patterns_cmd/bundle_cmd/name_bundle_cmd); `log_path` -- Path, one of out_root/{patterns,name_patterns,bundle,bundle_name}.log; `cwd` -- Optional[str], always str(repo_root) at every call site in this file.
- **Calls:** subprocess.run() (stdlib, stdout+stderr merged to `log_path`).
- **Thresholds:** `-20` (last-20-lines tail, hardcoded literal).
- **Returns:** (returncode, tail (last 20 lines of the combined log), full log content); consumed by _run_one_segment() to decide step_failed/failure_notes for each of the 4 subprocess steps, and to scan for "[patterns_timing]"-prefixed lines after the patterns step.

### `_preshard_one_shard(shard_file: Path, segment_plans: Dict[str, Dict], force: bool) -> tuple[str, int, int]` — [run_segment_orchestrator.py:L463](../../tools/run_segment_orchestrator.py#L463)

- **Reads:** `shard_file` -- one Path under records_dir/identity_items_by_domain/, from _preshard_corpus_records()'s ThreadPoolExecutor.submit() loop over every *.csv shard; `segment_plans` -- Dict[sid, plan] built by run_orchestrator() (each plan: sid/segment_records_dir/allowed_ids/status), passed through unchanged; `force` -- bool, from run_orchestrator()'s args.force/--force-preshard.
- **Calls:** csv.reader()/csv.writer() (stdlib) only; no other module function.
- **Thresholds:** _PRESHARD_BATCH = 64 (module constant, l.49: max simultaneous open destination file handles per fan-out batch, so total open fds stay well below typical OS limits regardless of segment count); the shard file's own "export_run_id" column is looked up by name (`header.index("export_run_id")`), not a hardcoded column index.
- **Returns:** (shard_name, files_written, files_skipped) tuple; consumed by _preshard_corpus_records() only to accumulate `total_written` for its own summary print -- no other function reads the individual per-shard result.

### `_preshard_corpus_records(records_dir: Path, segment_plans: Dict[str, Dict], force: bool) -> None` — [run_segment_orchestrator.py:L559](../../tools/run_segment_orchestrator.py#L559)

- **Reads:** `records_dir` -- Path, corpus-level results/records/ (run_orchestrator()'s --records-dir); reads records.csv/file_metadata.csv directly and identity_items_by_domain/*.csv via _preshard_one_shard(); `segment_plans` -- Dict built by run_orchestrator() (see _preshard_one_shard()); `force` -- bool, from run_orchestrator()'s args.force.
- **Calls:** csv.reader()/csv.writer() (stdlib, for records.csv/file_metadata.csv); _preshard_one_shard() (via ThreadPoolExecutor, one submission per shard file); concurrent.futures.ThreadPoolExecutor/as_completed() (stdlib).
- **Thresholds:** _PRESHARD_BATCH = 64 (module constant, shared with _preshard_one_shard()); `shard_pool_size = max(1, min(8, _PRESHARD_BATCH // max_seg))` (hardcoded `8` cap on concurrent shard-processing threads, derived from _PRESHARD_BATCH and segment count); records.csv/file_metadata.csv's "export_run_id" column looked up by name, matching _preshard_one_shard()'s convention.
- **Returns:** None; writes each segment's records.csv/file_metadata.csv/ identity_items_by_domain/*.csv under segment_plans[sid]["segment_records_dir"], plus ".preshard_complete" and identity_items_by_domain/.complete marker files -- but only for segments in `segments_to_write` (force=True, or status != "complete"), so an already-complete segment's existing sharded records are left untouched. Consumed by run_orchestrator() as a side-effecting step before the per-segment executor runs; the resulting ".preshard_complete" markers are read back by _write_segment_records().

### `_write_segment_records(records_dir: Path, segment_records_dir: Path, allowed_ids: set) -> None` — [run_segment_orchestrator.py:L725](../../tools/run_segment_orchestrator.py#L725)

- **Reads:** `records_dir` -- corpus records dir; `segment_records_dir` -- this segment's own results/records/ dir, from _run_one_segment()'s Step 1; `allowed_ids` -- set of export_run_ids for this segment, from _run_one_segment()'s `export_run_ids` (membership.get(sid, [])). Also checks `segment_records_dir / ".preshard_complete"` and whether `segment_records_dir/records.csv` actually exists.
- **Calls:** none (stdlib csv.DictReader/DictWriter only) -- this is the per-segment row-by-row fallback path used only when _preshard_corpus_records() did NOT already write this segment's inputs.
- **Thresholds:** none named -- the "trust the marker only if records.csv is also present" defense-in-depth check (`preshard_marker_valid`) is inline control flow, not a named constant.
- **Returns:** None; writes segment_records_dir/{records.csv,file_metadata.csv} and segment_records_dir/identity_items_by_domain/*.csv (plus a .complete marker), each filtered to rows whose export_run_id is in `allowed_ids` -- but only for files/rows not already written by a valid preshard marker. Missing source files are skipped silently (see the docstring above): the patterns stage surfaces the resulting empty/absent input as its own failure via _build_patterns_missing_notes().

### `_filter_name_key_csv_to_segment(name_key_results_csv: Path, out_csv: Path, allowed_ids: set) -> int` — [run_segment_orchestrator.py:L819](../../tools/run_segment_orchestrator.py#L819)

- **Reads:** `name_key_results_csv` -- Path to the corpus-wide name_key_results.csv (tools/apply_name_key_policy.py's output, e.g. from --name-key-results-csv), passed in by _run_one_segment()'s Step 2b; `out_csv` -- this segment's results/name_key/name_key_results.csv Path; `allowed_ids` -- this segment's export_run_ids set.
- **Calls:** nested _in_segment() (per row); normalize_export_run_id() (tools/bundle_analysis/name_projection_adapter.py, imported at module top), via _in_segment().
- **Thresholds:** none named -- membership is tested against `allowed_ids` itself (this segment's real population), not a hardcoded list.
- **Returns:** int rows written; raises FileNotFoundError if `name_key_results_csv` is missing. Writes `out_csv` with the same fieldnames as the corpus-wide input, filtered to rows whose (normalized-or-raw) export_file is in `allowed_ids`. Consumed by _run_one_segment(), which then invokes tools/generate_name_key_patterns.py --comparison-target name against this filtered file.

### `_filter_name_key_csv_to_segment._in_segment(raw_export_file: str) -> bool` — [run_segment_orchestrator.py:L874](../../tools/run_segment_orchestrator.py#L874)

- **Reads:** `raw_export_file` -- one row's raw `export_file` cell value; closes over `allowed_ids` (the enclosing _filter_name_key_csv_to_segment()'s parameter).
- **Calls:** normalize_export_run_id() (tools/bundle_analysis/name_projection_adapter.py).
- **Thresholds:** none.
- **Returns:** bool; consumed by the enclosing function's row filter (list comprehension).

### `_build_patterns_missing_notes(sid: str, out_root: Path, records_dir: Path, patterns_stderr: str) -> str` — [run_segment_orchestrator.py:L907](../../tools/run_segment_orchestrator.py#L907)

- **Reads:** `sid` -- segment_id, for the message header; `out_root` -- this segment's output root, to locate export_run_ids.txt; `records_dir` -- this segment's results/records/ dir, to locate records.csv/file_metadata.csv; `patterns_stderr` -- the captured stderr/stdout content from the patterns subprocess (run_step_log()'s third return value), passed in by _run_one_segment().
- **Calls:** none (file reads + csv.reader() only).
- **Thresholds:** `[WARN extract_all]` (hardcoded literal substring used to filter relevant warning lines out of patterns_stderr); `-10` (last-10-warning-lines slice).
- **Returns:** str diagnostic message (export_run_ids.txt id count, records.csv/ file_metadata.csv first export_run_id, relevant [WARN] lines); consumed by _run_one_segment() as `failure_notes` when the patterns step exits 0 but pattern_presence_file.csv was never written -- distinguishing a silent zero-records-matched failure from a genuine subprocess error.

### `_active_domains_from_presence_csv(analysis_dir: Path) -> Optional[frozenset]` — [run_segment_orchestrator.py:L984](../../tools/run_segment_orchestrator.py#L984)

- **Reads:** `analysis_dir` -- this segment's results/analysis/ dir, from _run_one_segment()'s BI-merge step; reads `analysis_dir/pattern_presence_file.csv`'s `domain` column.
- **Calls:** none (stdlib csv.DictReader).
- **Thresholds:** none.
- **Returns:** frozenset[str] of domain names, or None if the file is absent or has no domain rows; consumed by _run_one_segment() as `active_domains`, passed to merge_bi_outputs() to restrict the config-leg BI merge to genuinely active domains (excluding stale per-domain folders from an earlier, larger-population run).

### `_active_domains_from_name_patterns(name_patterns_dir: Path) -> Optional[frozenset]` — [run_segment_orchestrator.py:L1016](../../tools/run_segment_orchestrator.py#L1016)

- **Reads:** `name_patterns_dir` -- this segment's results/name_key/patterns/name/ dir, from _run_one_segment()'s name-leg BI-merge step; reads `name_patterns_dir/domain_patterns.csv`'s `domain` column (tools/generate_name_key_patterns.py's output schema).
- **Calls:** none (stdlib csv.DictReader).
- **Thresholds:** none.
- **Returns:** frozenset[str] (possibly empty) if the file exists, or None if it's missing; consumed by _run_one_segment() as `active_domains_name`, passed to merge_bi_outputs() for the name-leg BI merge. The empty-vs-None distinction (see the docstring above) is load-bearing: empty means "exclude every domain folder", None means "file missing, treat as unfiltered".

### `_segment_has_name_leg_output(out_root: Path) -> bool` — [run_segment_orchestrator.py:L1054](../../tools/run_segment_orchestrator.py#L1054)

- **Reads:** `out_root` -- this segment's output root; checks `out_root/results/bundle_analysis/name_all/bundle_provenance.csv` for existence.
- **Calls:** none (Path.is_file()).
- **Thresholds:** none.
- **Returns:** bool; consumed by run_orchestrator() (dry-run and live-run skip-check loops) to decide `already_satisfied` for a segment whose registry status is already "complete" but comparison_target requests the name leg.

### `merge_bi_outputs(bundle_analysis_dir: Path, active_domains: Optional[frozenset]=None) -> dict` — [run_segment_orchestrator.py:L1081](../../tools/run_segment_orchestrator.py#L1081)

- **Reads:** `bundle_analysis_dir` -- this segment's results/bundle_analysis/all/ (config leg) or results/bundle_analysis/name_all/ (name leg), from _run_one_segment(); `active_domains` -- Optional[frozenset], from _active_domains_from_presence_csv()/_active_domains_from_name_patterns(); globs `bundle_analysis_dir/*/<filename>` for each name in BI_MERGE_FILES.
- **Calls:** none (stdlib csv.DictReader/atomic_write_csv() from tools/bundle_analysis/common.py, imported at module top).
- **Thresholds:** BI_MERGE_FILES (module constant, l.53-64: the 10 per-domain filenames merged into `{stem}_combined.csv`); the "_population_discovery"/ "_population_runs" substring exclusions (hardcoded literals) filtering out non-domain subfolders from the glob.
- **Returns:** dict[filename, {"files_merged": int, "rows_written": int}]; consumed by _run_one_segment() only to log totals. Deletes a stale `{stem}_combined.csv` when no current candidates exist (rather than leaving it in place), and skips (with a WARN print) any candidate file whose header doesn't match the first file's header.

### `build_run_plan(manifest: Dict[str, dict], registry: List[dict], segment_filter: Optional[str], force: bool) -> List[tuple[dict, dict]]` — [run_segment_orchestrator.py:L1171](../../tools/run_segment_orchestrator.py#L1171)

- **Reads:** `manifest` -- load_manifest()'s return value; `registry` -- load_registry()'s return value, reads each row's run_type/segment_id; `segment_filter` -- unused by this function's own body (the --segment CLI filter is applied later, by run_orchestrator() itself, not here); `force` -- also unused by this function's own body (also applied later by run_orchestrator()).
- **Calls:** nested sort_key() (via list.sort()).
- **Thresholds:** `{"bundle", "reference"}` (hardcoded literal set: which run_type values are even eligible to appear in the plan).
- **Returns:** List[(registry_row, manifest_row)] tuples, ordered by (segment_level asc, segment_id asc); consumed by run_orchestrator() as `plan`, then filtered again by --segment/--force/status before being split into `plan_to_run` vs. skipped.
- **Notes:** `segment_filter`/`force` are accepted parameters this function's own body never reads -- both are applied later downstream (run_orchestrator()'s per-row loop), so a reader relying only on this function's signature would wrongly assume filtering happens here.

### `build_run_plan.sort_key(row: dict) -> tuple` — [run_segment_orchestrator.py:L1202](../../tools/run_segment_orchestrator.py#L1202)

- **Reads:** `row` -- one registry row; closes over `manifest` (the enclosing build_run_plan()'s parameter) to look up the matching manifest row's segment_level.
- **Calls:** none.
- **Thresholds:** `0` -- the fallback segment_level on ValueError/TypeError (hardcoded literal).
- **Returns:** (int, str) tuple; consumed by `run_rows.sort(key=sort_key)`.

### `validate_membership_against_manifest(plan: List[tuple[dict, dict]], membership: Dict[str, List[str]]) -> List[str]` — [run_segment_orchestrator.py:L1233](../../tools/run_segment_orchestrator.py#L1233)

- **Reads:** `plan` -- build_run_plan()'s return value, from run_orchestrator(); reads each manifest_row's file_count/population_hash; `membership` -- load_membership()'s return value, reads each segment_id's export_run_id list.
- **Calls:** hashlib.sha1() (stdlib, to recompute population_hash for comparison).
- **Thresholds:** none named -- the comparison is a direct equality check against the manifest row's own file_count/population_hash values (not a separate constant table).
- **Returns:** list[str] error messages (empty if everything agrees); consumed by run_orchestrator(), which aborts the entire run (prints to stderr, returns 1) before any segment is processed if this list is non-empty -- refusing to run against a stale/mismatched segment_membership.csv.

### `_clear_stale_name_all_before_run(out_root: Path, run_type: str, comparison_target: str, log) -> None` — [run_segment_orchestrator.py:L1285](../../tools/run_segment_orchestrator.py#L1285)

- **Reads:** `out_root` -- this segment's output root; `run_type` -- from the registry row ("bundle"/"reference"/etc.); `comparison_target` -- from run_orchestrator()'s args.comparison_target ("config"/"name"/"both"); `log` -- the per-segment log closure from _run_one_segment().
- **Calls:** retry_fs_op() (tools/bundle_analysis/common.py, imported at module top), wrapping shutil.rmtree().
- **Thresholds:** `run_type == "bundle" and comparison_target in ("name", "both")` -- inline condition, not a named constant; the target path `out_root/results/bundle_analysis/name_all` is a hardcoded relative path.
- **Returns:** None; deletes `name_all/` if present and the condition holds. Called once at the very start of _run_one_segment(), before Step 1, specifically so a failure in step 2b or step 3 (which would otherwise skip step 3b's own upfront clear entirely) still leaves this segment's stale name-leg BI output removed rather than silently stale.

### `_run_one_segment(idx: int, total: int, reg_row: dict, mrow: dict, membership: Dict[str, List[str]], records_dir: Path, exports_dir: Path, segments_root: Path, repo_root: Path, join_policy: Path, skip_bi_merge: bool, registry: List[dict], reg_index: Dict[str, int], registry_file: Path, manifest_file: Path, results_registry_file: Path, registry_lock: threading.Lock, counters: Dict[str, object], counters_lock: threading.Lock, worker_id: int, bundle_workers: int, comparison_target: str='config', name_key_results_csv: Optional[Path]=None) -> Dict` — [run_segment_orchestrator.py:L1322](../../tools/run_segment_orchestrator.py#L1322)

- **Reads:** `reg_row` -- one run_registry.csv row (segment_id/output_folder/run_type), from build_run_plan()'s plan; `mrow` -- the matching segment_manifest.csv row (segment_level); `membership` -- load_membership()'s Dict, indexed by sid for export_run_ids; `records_dir`/`exports_dir`/`segments_root`/`repo_root`/ `join_policy` -- Paths, from run_orchestrator()'s args, passed through unchanged; `skip_bi_merge` -- from args.skip_bi_merge; `registry`/`reg_index`/ `registry_file`/`manifest_file`/`results_registry_file` -- shared state for the registry-update block; `registry_lock`/`counters`/`counters_lock` -- threading.Lock/dict shared across every worker in the ThreadPoolExecutor; `worker_id`/`bundle_workers` -- for logging and the bundle subprocess's --workers flag; `comparison_target`/`name_key_results_csv` -- from args.comparison_target/args.name_key_results_csv.
- **Calls:** _clear_stale_name_all_before_run(); _write_segment_records(); run_step_log() (x4: patterns, bundle, and -- gated on comparison_target -- name patterns, name bundle); _filter_name_key_csv_to_segment(); _build_patterns_missing_notes(); _active_domains_from_presence_csv(); _active_domains_from_name_patterns(); merge_bi_outputs(); annotate_name_target_combined_files() (tools/bundle_analysis/name_projection_adapter.py); retry_fs_op() (via shutil.rmtree, for the name-bundle stale-output clear); utc_now_iso(); write_registry_atomic(); write_results_registry() (tools/build_results_registry.py, imported at module top); nested log().
- **Thresholds:** none named beyond the module-level VALID_COMPARISON_TARGETS check (enforced earlier, in main()) -- this function's own step gating is `run_type == "bundle"` / `comparison_target in ("name", "both")` inline conditions, not named constants.
- **Returns:** dict (segment_id/status/files/level/prepare_s/patterns_s/bundle_s/ bi_merge_s/total_s/worker_id/patterns_top5/failure_note); consumed by run_orchestrator()'s ThreadPoolExecutor future-collection loop, appended to `segment_results` (feeds _write_run_summary()) and used to update `counters`. Also has the side effect (under registry_lock) of mutating the shared `registry` list in place and persisting it via write_registry_atomic()/ write_results_registry() before returning -- so run_registry.csv/ results_registry.csv reflect this segment's outcome immediately, not only after the whole run completes.
- **Notes:** (mechanical-extraction risk) `step_failed`/`failure_notes` accumulate through 7 sequential try/except-guarded steps (clear_stale_name_all, prepare, patterns, patterns_name, bundle, bundle_name, bi_merge/bi_merge_name), each gated on `step_failed is None` from the previous step -- sequential-control-flow-as-policy: which steps actually execute for a given segment depends on run_type and comparison_target, evaluated fresh at each gate, not declared as a single up-front plan. `registry` (the list) and `counters` (the dict) are both caller-owned mutable state shared across every concurrent worker thread and mutated in place under their respective locks -- a per-function reader would need to also read run_orchestrator() to know these mutations are thread-safe only because of that locking discipline.

### `_run_one_segment.log(msg: str) -> None` — [run_segment_orchestrator.py:L1444](../../tools/run_segment_orchestrator.py#L1444)

- **Reads:** `msg` -- caller-supplied string; closes over `log_f` (the enclosing _run_one_segment()'s open file handle for out_root/run.log).
- **Calls:** none (file write + flush).
- **Thresholds:** none.
- **Returns:** None; used throughout _run_one_segment() as the per-step logging call (separate from the `print()` calls that go to console).

### `run_orchestrator(args: argparse.Namespace) -> int` — [run_segment_orchestrator.py:L1809](../../tools/run_segment_orchestrator.py#L1809)

- **Reads:** `args` -- argparse.Namespace from main() (--manifest-file, --registry-file, --results-registry-file, --membership-file, --records-dir, --exports-dir, --segments-root, --repo-root, --join-policy, --segment, --force, --dry-run, --skip-bi-merge, --workers/--bundle-workers/--workers-auto, --no-preshard, --force-preshard, --comparison-target, --name-key-results-csv).
- **Calls:** load_manifest(); load_registry(); load_membership(); build_run_plan(); validate_membership_against_manifest(); _segment_has_name_leg_output() (skip-check, dry-run and live); _preshard_corpus_records() (gated on marker/force state); ThreadPoolExecutor/_run_one_segment() (live run, once per segment in plan_to_run); write_results_registry(); _write_run_summary().
- **Thresholds:** `_CORPUS_PRESHARD_MARKER = ".preshard_complete_corpus"` (module constant, l.51: gates whether preshard re-runs); the preshard skip condition itself (`preshard_marker.is_file() and not _has_pending`) is inline control flow built from that marker plus a freshly-computed `_has_pending` flag, not a single named constant.
- **Returns:** int exit code (1 if segment_membership.csv disagrees with segment_manifest.csv -- via validate_membership_against_manifest() -- or if any segment failed or results_registry write failed; 0 otherwise). In --dry-run mode, returns 0 unconditionally after printing the full plan without executing anything. Writes run_registry.csv (incrementally, per segment, and once more at the end via write_results_registry()) and segments_root/run_summary.txt. Called by main() as `run_orchestrator(args)`.

### `main() -> None` — [run_segment_orchestrator.py:L2214](../../tools/run_segment_orchestrator.py#L2214)

- **Reads:** CLI args (see the module-level argparse block for the full list: --manifest-file, --registry-file, --results-registry-file, --membership-file, --records-dir, --exports-dir, --segments-root, --repo-root, --join-policy, --segment, --force, --dry-run, --skip-bi-merge, --workers, --no-preshard, --force-preshard, --comparison-target, --name-key-results-csv).
- **Calls:** compute_worker_split() (when --workers is "auto", or to derive bundle_workers from an explicit --workers N); run_orchestrator().
- **Thresholds:** VALID_COMPARISON_TARGETS = {"config", "name", "both"} (module constant, l.44, enforced via argparse choices); default --workers=4; the --name-key-results-csv-required-when-name/both check (ap.error(), inline, not a named constant).
- **Returns:** None; calls sys.exit(run_orchestrator(args)) -- this is **Run C2** (l.245 of tools/corpus_update_runbook.ps1), invoked per segment_manifest.csv/ run_registry.csv (Run C1's output) after the mandatory file_metadata.csv human-edit pause, and its own output (run_registry.csv, updated in place; results_registry.csv) is consumed by **Run C2.5** (tools/build_results_registry.py).

## `../../tools/build_results_registry.py`

### `read_csv_rows(path: Path) -> List[Dict[str, str]]` — [build_results_registry.py:L30](../../tools/build_results_registry.py#L30)

- **Reads:** `path` -- a Path to a CSV file; write_results_registry() calls this with manifest_file (--manifest-file, segment_manifest.csv from tools/build_segment_manifest.py) and registry_file (--registry-file, run_registry.csv, also from build_segment_manifest.py and subsequently updated in place by tools/run_segment_orchestrator.py's write_registry_atomic() after each segment run).
- **Calls:** none (stdlib csv.DictReader only).
- **Thresholds:** none.
- **Returns:** list[dict[str,str]], every value normalized to str with None replaced by ""; consumed by build_results_registry_rows() as manifest_rows/registry_rows.

### `atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None` — [build_results_registry.py:L53](../../tools/build_results_registry.py#L53)

- **Reads:** `path`, `fieldnames`, `rows` -- caller-supplied; write_results_registry() calls this with output_file (--output-file), the module-level RESULTS_REGISTRY_FIELDNAMES constant, and build_results_registry_rows()'s return value.
- **Calls:** none (stdlib csv.DictWriter, tempfile.NamedTemporaryFile).
- **Thresholds:** none.
- **Returns:** None; writes `path` atomically (temp file in the same directory, then Path.replace()) so a reader never observes a partially-written results_registry.csv.

### `build_results_registry_rows(manifest_rows: Iterable[Dict[str, str]], registry_rows: Iterable[Dict[str, str]]) -> List[Dict[str, str]]` — [build_results_registry.py:L84](../../tools/build_results_registry.py#L84)

- **Reads:** `manifest_rows` -- rows read from segment_manifest.csv (tools/build_segment_manifest.py's MANIFEST_FIELDNAMES; this function reads segment_id/parent_segment_id/segment_level/governance_role/run_type from it); `registry_rows` -- rows read from run_registry.csv (tools/build_segment_manifest.py's REGISTRY_FIELDNAMES, as subsequently updated in place by tools/run_segment_orchestrator.py's write_registry_atomic() after each segment run; this function reads parent_segment_id/output_folder/run_type/ status/last_run_utc from it).
- **Calls:** _safe_int() (via the sort key).
- **Thresholds:** none named -- a manifest_row's own parent_segment_id/run_type values are only used as a fallback when the matching registry_row is missing or blank (registry_row.get(...) or manifest_row.get(...) or "").
- **Returns:** list[dict] with keys segment_id/parent_segment_id/segment_level/ governance_role/output_folder/run_type/status/last_run_utc (RESULTS_REGISTRY_FIELDNAMES), one row per segment_manifest.csv segment_id, sorted by (segment_level, segment_id); consumed by write_results_registry(), which writes it to results_registry.csv -- the single stable query surface this stage exists to produce.

### `_safe_int(value: object) -> int` — [build_results_registry.py:L138](../../tools/build_results_registry.py#L138)

- **Reads:** `value` -- caller-supplied (build_results_registry_rows()'s sort-key call passes row.get("segment_level")).
- **Calls:** none (int(), str()).
- **Thresholds:** none.
- **Returns:** int(value), or 0 if value is missing/non-numeric (ValueError swallowed); consumed by build_results_registry_rows()'s sort key so a blank or malformed segment_level sorts first rather than raising.

### `write_results_registry(manifest_file: Path, registry_file: Path, output_file: Path) -> int` — [build_results_registry.py:L156](../../tools/build_results_registry.py#L156)

- **Reads:** `manifest_file`, `registry_file`, `output_file` -- Paths, from main()'s CLI --manifest-file/--registry-file/--output-file, or (when this module is imported directly) from tools/run_segment_orchestrator.py's own call with manifest_file/ registry_file/results_registry_file.
- **Calls:** read_csv_rows() (x2); build_results_registry_rows(); atomic_write_csv().
- **Thresholds:** none.
- **Returns:** int row count written; writes results_registry.csv to `output_file`. Consumed by main() for the printed summary, and directly by tools/run_segment_orchestrator.py's run_orchestrator()/_run_one_segment(), which import write_results_registry from this module and call it after every segment registry update so results_registry.csv stays current mid-run, not only at Run C2.5.

### `main(argv: List[str] | None=None) -> int` — [build_results_registry.py:L180](../../tools/build_results_registry.py#L180)

- **Reads:** CLI args --manifest-file, --registry-file, --output-file (all required).
- **Calls:** write_results_registry().
- **Thresholds:** none.
- **Returns:** int exit code (1 if --manifest-file/--registry-file is missing, else 0); prints a row-count summary to stdout. Invoked as **Run C2.5** (l.266 of tools/corpus_update_runbook.ps1) after tools/run_segment_orchestrator.py's Run C2 completes.

## `../../tools/extract_segment_subtree.py`

### `norm(value: object) -> str` — [extract_segment_subtree.py:L52](../../tools/extract_segment_subtree.py#L52)

- **Reads:** `value` -- caller-supplied (used pervasively: row field lookups, CLI args, manifest values).
- **Calls:** none (str(), strip()).
- **Thresholds:** none.
- **Returns:** str; consumed throughout this module wherever a raw CSV/dict value needs stripped-string normalization before comparison.

### `norm_fold(value: object) -> str` — [extract_segment_subtree.py:L68](../../tools/extract_segment_subtree.py#L68)

- **Reads:** `value` -- caller-supplied.
- **Calls:** norm().
- **Thresholds:** none.
- **Returns:** str.casefold() of the normalized value; consumed by find_seeds_by_search() and row_matches() for case-insensitive membership tests.

### `sanitize_label(text: str) -> str` — [extract_segment_subtree.py:L81](../../tools/extract_segment_subtree.py#L81)

- **Reads:** `text` -- caller-supplied (main() passes the first --segment-id or --search-term when --label is not given).
- **Calls:** re.sub() (stdlib).
- **Thresholds:** the slug pattern `[^a-z0-9]+` -> "_" and the "segment_subtree" fallback for an empty/all-punctuation input are hardcoded literals here, not named constants.
- **Returns:** str; consumed by main() to build --out-dir's default (<cross-segment-dir>/<label>_subtree_extract).

### `atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None` — [extract_segment_subtree.py:L98](../../tools/extract_segment_subtree.py#L98)

- **Reads:** `path`, `fieldnames`, `rows` -- caller-supplied; called by main() (for selected_segments.csv and extract_manifest.csv) and by _write_summary()/ process_file() (for the detail/summary output files).
- **Calls:** none (stdlib csv.DictWriter, tempfile.NamedTemporaryFile).
- **Thresholds:** none.
- **Returns:** None; writes `path` atomically (temp file in the same directory via NamedTemporaryFile, then Path.replace()).

### `load_manifest(records_dir: Path) -> Dict[str, Dict[str, str]]` — [extract_segment_subtree.py:L140](../../tools/extract_segment_subtree.py#L140)

- **Reads:** `records_dir` -- Path, from main()'s --records-dir; opens `records_dir/segment_manifest.csv` (tools/build_segment_manifest.py's output -- MANIFEST_FIELDNAMES columns, though this function only requires segment_id and parent_segment_id per REQUIRED_HIERARCHY_COLUMNS).
- **Calls:** none (stdlib csv.DictReader); raises Blocked.
- **Thresholds:** REQUIRED_HIERARCHY_COLUMNS = {"segment_id", "parent_segment_id"} (module constant, l.81) -- header-presence check only.
- **Returns:** Dict[segment_id, row-dict]; raises Blocked if the file is missing, the required columns are absent, or two rows disagree for the same segment_id. Consumed by main() to build parent_map and to resolve seed segments.

### `find_seeds_by_search(manifest: Dict[str, Dict[str, str]], search_terms: Tuple[str, ...]) -> Set[str]` — [extract_segment_subtree.py:L179](../../tools/extract_segment_subtree.py#L179)

- **Reads:** `manifest` -- load_manifest()'s return value, passed in by main(); `search_terms` -- tuple from main()'s --search-term (repeatable CLI flag).
- **Calls:** norm(), norm_fold().
- **Thresholds:** none named -- substring containment (`term in haystack`) against the space-joined, case-folded values of every column in the row.
- **Returns:** Set[str] of matching segment_ids (empty if no non-blank search terms); consumed by main(), unioned with find_seeds_by_id()'s result into `seed_ids`.

### `find_seeds_by_id(manifest: Dict[str, Dict[str, str]], segment_ids: Tuple[str, ...]) -> Set[str]` — [extract_segment_subtree.py:L203](../../tools/extract_segment_subtree.py#L203)

- **Reads:** `manifest` -- load_manifest()'s return value; `segment_ids` -- tuple from main()'s --segment-id (repeatable CLI flag).
- **Calls:** norm(); raises Blocked.
- **Thresholds:** none.
- **Returns:** Set[str]; raises Blocked if a given --segment-id is not a key in `manifest`. Consumed by main(), unioned with find_seeds_by_search()'s result.

### `expand_ancestors(seed_ids: Set[str], parent_map: Dict[str, str], depth: int) -> Tuple[Set[str], List[Dict[str, object]]]` — [extract_segment_subtree.py:L225](../../tools/extract_segment_subtree.py#L225)

- **Reads:** `seed_ids` -- Set[str], from main()'s find_seeds_by_id()/find_seeds_by_search() union; `parent_map` -- Dict[segment_id, parent_segment_id], built by main() from the manifest's own parent_segment_id column; `depth` -- int, from main()'s --ancestor-depth (default 2).
- **Calls:** none; raises Blocked, ValueError.
- **Thresholds:** `depth` itself is the only threshold, sourced from --ancestor-depth (no hardcoded default inside this function -- the argparse default of 2 lives in parse_args()).
- **Returns:** (Set[str] selected segment_ids including ancestors, List[dict] ancestry_rows with seed_segment_id/selected_segment_id/relationship/ancestor_distance) sorted by (seed_segment_id, ancestor_distance, selected_segment_id); consumed by main() to build selected_segments.csv and the relations_by_segment/distances_by_segment lookups. Raises Blocked on a missing hierarchy entry or a cyclic parent chain.

### `resolve_endpoint_columns(columns: Iterable[str]) -> Tuple[str, ...]` — [extract_segment_subtree.py:L302](../../tools/extract_segment_subtree.py#L302)

- **Reads:** `columns` -- an iterable of header column names, from process_file()'s peek at the source file's first row.
- **Calls:** none (str.casefold() only).
- **Thresholds:** PREFERRED_ENDPOINT_PAIRS (module constant, l.187-193: the 5 known left/right column-name pairs used across compare_cross_segment.py's various output schemas, tried in order); _SINGLETON_EXCLUDED_COLUMNS (module constant, l.195: columns containing "segment_id" that must NOT be treated as a lone endpoint column, e.g. parent_segment_id).
- **Returns:** tuple of 0, 1, or 2 column names; consumed by process_file(), which raises Blocked if this returns an empty tuple ("no usable segment endpoint columns").

### `row_matches(row: Dict[str, str], endpoints: Tuple[str, ...], selected_ids: Set[str], require_both_endpoints: bool=False) -> bool` — [extract_segment_subtree.py:L336](../../tools/extract_segment_subtree.py#L336)

- **Reads:** `row` -- one CSV row dict, from process_file()'s per-row loop; `endpoints` -- resolve_endpoint_columns()'s return value; `selected_ids` -- expand_ancestors()'s selected-segment Set[str]; `require_both_endpoints` -- from main()'s --require-both-endpoints flag (default False).
- **Calls:** norm().
- **Thresholds:** `len(endpoints) == 2` / `== 1` branch selection is inline control flow, not a named constant -- process_file() already guards the 0-endpoint case before calling this.
- **Returns:** bool; consumed by process_file() to decide whether a row is written to the detail output and folded into the summary aggregate.

### `NumericStats.__init__(self) -> None` — [extract_segment_subtree.py:L383](../../tools/extract_segment_subtree.py#L383)

- **Reads:** none.
- **Calls:** none.
- **Thresholds:** none.
- **Returns:** None; sets self.count=0, self.total=0.0, self.minimum=math.inf, self.maximum=-math.inf (the inf/-inf sentinels are overwritten by the first add() call and never read back unless self.count stays 0, in which case emit() reports "" rather than the sentinel itself).

### `NumericStats.add(self, value: object) -> None` — [extract_segment_subtree.py:L400](../../tools/extract_segment_subtree.py#L400)

- **Reads:** `value` -- caller-supplied (via self); _update_aggregate() calls this once per row per summary_numeric_fields entry.
- **Calls:** norm(); float(); math.isfinite().
- **Thresholds:** none.
- **Returns:** None; mutates self.count/total/minimum/maximum in place. A blank, non-numeric, or non-finite (inf/nan) value is silently skipped rather than raising or counted.

### `NumericStats.emit(self, prefix: str) -> Dict[str, object]` — [extract_segment_subtree.py:L427](../../tools/extract_segment_subtree.py#L427)

- **Reads:** self (count/total/minimum/maximum, accumulated by add()); `prefix` -- caller-supplied field-name stem, from _write_summary()'s per-numeric-field loop.
- **Calls:** none.
- **Thresholds:** `self.count == 0` gates whether mean/min/max are emitted as "" (never computed from the math.inf/-math.inf sentinels) versus real numbers.
- **Returns:** dict with keys {prefix}_count/{prefix}_mean/{prefix}_min/{prefix}_max; consumed by _write_summary() to build one summary row's numeric columns.

### `FileSpec.supports_summary(self) -> bool` — [extract_segment_subtree.py:L464](../../tools/extract_segment_subtree.py#L464)

- **Reads:** self.summary_keys (set on construction, from the FILE_SPECS tuple literal or a bare FileSpec(filename) for an unknown file).
- **Calls:** none.
- **Thresholds:** none.
- **Returns:** bool; consumed by process_file() (whether to call _update_aggregate()/_write_summary()) and main() (whether to compute a summary_path for this filename at all).

### `_update_aggregate(aggregates: Dict[Tuple[str, ...], Dict[str, object]], row: Dict[str, str], spec: FileSpec) -> None` — [extract_segment_subtree.py:L553](../../tools/extract_segment_subtree.py#L553)

- **Reads:** `aggregates` -- Dict keyed by a tuple of spec.summary_keys values, mutated in place across every call within one process_file() run; `row` -- one CSV row dict, from process_file()'s per-row loop; `spec` -- the FileSpec for the current source file (summary_keys/summary_numeric_fields/summary_flag_fields).
- **Calls:** norm(); NumericStats.add() (via aggregates[key]["numeric"].setdefault(...)); norm_fold().
- **Thresholds:** `"true"` (case-folded) is the hardcoded literal a flag_field value must equal to increment its count_name -- FileSpec.summary_flag_fields (e.g. ("is_bundle_member_all", "bundle_member_all_count")) names which columns.
- **Returns:** None; mutates `aggregates` in place (row_count, per-numeric-field NumericStats, per-flag-field counts). NOTE (mechanical-extraction risk): `aggregates` is caller-owned mutable state threaded across every row of one process_file() call -- a naive per-function static parser reading only this function's own reads/returns would miss that its effect only becomes visible via _write_summary()'s later read of the same dict.

### `_write_summary(aggregates: Dict[Tuple[str, ...], Dict[str, object]], spec: FileSpec, path: Path) -> int` — [extract_segment_subtree.py:L583](../../tools/extract_segment_subtree.py#L583)

- **Reads:** `aggregates` -- _update_aggregate()'s accumulated dict, passed in by process_file() after the full source file has been scanned; `spec` -- the FileSpec defining summary_keys/summary_numeric_fields/summary_flag_fields; `path` -- the summary output Path, from process_file()'s summary_path.
- **Calls:** NumericStats.emit(); atomic_write_csv().
- **Thresholds:** none beyond spec's own field lists (not named constants in this function).
- **Returns:** int row count written; consumed by process_file() as `summary_rows_count` (recorded on the ProcessResult and printed to stdout).

### `process_file(source: Path, selected_ids: Set[str], spec: FileSpec, detail_path: Optional[Path], summary_path: Optional[Path], max_output_rows: int, max_output_gb: float, progress_interval: int, require_both_endpoints: bool=False) -> ProcessResult` — [extract_segment_subtree.py:L617](../../tools/extract_segment_subtree.py#L617)

- **Reads:** `source` -- Path to one cross_segment_*.csv/comparison_registry.csv file, from main()'s per-filename loop (args.cross_segment_dir / filename); `selected_ids` -- Set[str], expand_ancestors()'s return value; `spec` -- the FileSpec for this filename (FILE_SPECS or a bare FileSpec(filename) for an unknown file); `detail_path`/`summary_path` -- Optional Paths, gated by main()'s --mode; `max_output_rows`/`max_output_gb` -- from main()'s --max-output-rows/--max-output-gb (0 disables each); `progress_interval` -- from --progress-interval; `require_both_endpoints` -- from --require-both-endpoints.
- **Calls:** resolve_endpoint_columns(); row_matches() (per row); _update_aggregate() (per matching row, if summary requested); _write_summary() (once, at the end); raises Blocked.
- **Thresholds:** `max_output_rows`/`max_output_gb` are caller-supplied (CLI), not hardcoded here, but the check cadence is: row-count check on every matching row, size check every 10,000 written rows (`rows_written % 10_000 == 0`, hardcoded literal) -- so a size overage can be detected up to 10,000 rows late.
- **Returns:** ProcessResult dataclass (status "ok"/"degraded", rows_scanned, rows_written, summary_rows, selection_mode, detail_file, summary_file, reason); consumed by main(), which appends `{"source_file": filename, **vars(result)}` to manifest_rows (written to extract_manifest.csv). Raises Blocked if no usable endpoint columns are found or a row/size threshold is exceeded partway through -- in either case the partial detail temp file is deleted (never left half-written) and the exception propagates to main()'s per-file try/except.

### `parse_args() -> argparse.Namespace` — [extract_segment_subtree.py:L758](../../tools/extract_segment_subtree.py#L758)

- **Reads:** sys.argv (via argparse).
- **Calls:** argparse.ArgumentParser (stdlib).
- **Thresholds:** argparse defaults -- --ancestor-depth=2, --mode="both", --max-output-rows=0, --max-output-gb=0.0, --progress-interval=100_000 -- are all hardcoded literals in the argument definitions, not named module constants.
- **Returns:** argparse.Namespace; consumed by main() as `args`.

### `main() -> int` — [extract_segment_subtree.py:L801](../../tools/extract_segment_subtree.py#L801)

- **Reads:** parse_args()'s Namespace (--records-dir, --cross-segment-dir, --out-dir, --label, --segment-id/--search-term, --ancestor-depth, --require-both-endpoints, --mode, --file, --max-output-rows, --max-output-gb, --progress-interval); DEFAULT_FILENAMES / FILE_SPECS / KNOWN_UNSUPPORTED_FILES (module constants) when --file is not given.
- **Calls:** parse_args(); load_manifest(); find_seeds_by_id(); find_seeds_by_search(); expand_ancestors(); atomic_write_csv() (x2: selected_segments.csv, extract_manifest.csv); process_file() (once per target filename).
- **Thresholds:** DEFAULT_FILENAMES (derived from FILE_SPECS, l.302-344: the 7 cross_segment/comparison_registry files this tool knows how to filter); KNOWN_UNSUPPORTED_FILES (l.354-357: 2 files with no segment_id grain, informational only, appended to extract_manifest.csv as status="unsupported" rather than attempted).
- **Returns:** int exit code (0 if not `blocked`, else 1); writes selected_segments.csv and extract_manifest.csv under --out-dir (default <cross-segment-dir>/<label>_subtree_extract), plus each requested file's detail/<filename> and/or summary/<stem>.summary.csv. `blocked` is set true if an explicitly-requested --file is missing, if process_file() raises Blocked for any file, or if none of the target filenames were found at all (treated as a --cross-segment-dir misconfiguration, not a legitimately sparse run).
