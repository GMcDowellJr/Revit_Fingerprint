from __future__ import annotations
from itertools import combinations
import argparse,csv,hashlib,re,sys
from collections import defaultdict
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict,Iterable,List,Sequence

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from na_token import is_na_token

SEED_ROLES={"Template","Container"}
# Header-presence check only (main()) — does not validate per-row values.
REQUIRED_COLUMNS={"export_run_id","unit_system","client_label","governance_role"}
# Per-row required-VALUE check (see _validate_required_metadata()): every row
# must carry a real, non-blank, non-N/A value for each of these, or the
# entire manifest build is blocked. project_label is deliberately excluded —
# it is not a segmentation dimension (not in DIMENSION_CONFIG, not read by
# this file at all) and may carry an explicit not-applicable sentinel under
# the current metadata contract.
REQUIRED_ROW_FIELDS=["export_run_id","unit_system","governance_role","client_label","discipline_label","business_center_label"]
MANIFEST_FIELDNAMES=["segment_id","parent_segment_id","segment_level","unit_system","governance_role","client_label","discipline_label","business_center_label","collection_label","extra_dimensions","ancestor_segment_ids","run_type","file_count","has_seed_file","population_hash","notes","segment_purpose","segment_label"]
REGISTRY_FIELDNAMES=["segment_id","parent_segment_id","run_type","population_hash","conformance_reference_mode","output_folder","status","last_run_utc","notes","segment_purpose","segment_label"]
MEMBERSHIP_FIELDNAMES=["segment_id","export_run_id","is_seed"]
# Segment dimensions under the explicit-metadata contract: every row must
# carry a real, non-sentinel value for each of these (validated by
# _validate_required_metadata() before _build_segments() ever sees the row
# via main()). collection_label intentionally does not participate — it may
# still exist as a column in file_metadata.csv (and as an always-blank
# column in segment_manifest.csv, for downstream schema stability), but the
# segment builder ignores its value entirely.
DIMENSION_CONFIG = [
    {"field": "unit_system", "type": "root"},
    {"field": "governance_role", "type": "governance"},
    {"field": "client_label", "type": "cut"},
    {"field": "discipline_label", "type": "cut"},
    {"field": "business_center_label", "type": "cut"},
]

def _read_csv(path: Path) -> tuple:
    """Read a CSV file, returning both its header order and its rows as
    string-normalized dicts.

    --- trace ---
    reads: `path` -- a Path to file_metadata.csv (main()'s --metadata-file), or, later
        in main(), to an existing run_registry.csv/segment_membership.csv under
        --out-dir (for incremental-build carry-over).
    calls: none (stdlib csv.DictReader only).
    thresholds: none.
    returns: (fieldnames: list[str], rows: list[dict[str,str]]) -- fieldnames preserves
        the file's header order (used by main() for REQUIRED_COLUMNS presence
        checking); rows has every value normalized to str with None replaced by "".
        Consumed by main() (for file_metadata.csv, and for existing_registry_rows/
        existing_membership_rows when those files already exist).
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [{str(k): ("" if v is None else str(v)) for k, v in row.items()} for row in reader]
        fieldnames = list(reader.fieldnames or [])
    return fieldnames, rows

def _atomic_write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    """Write CSV rows atomically using a temp file in the destination directory.

    --- trace ---
    reads: `path`, `fieldnames`, `rows` -- caller-supplied; main() calls this three
        times with (manifest_path, MANIFEST_FIELDNAMES, manifest_rows),
        (registry_path, REGISTRY_FIELDNAMES, registry_rows), and
        (membership_path, MEMBERSHIP_FIELDNAMES, membership_rows).
    calls: none (stdlib csv.DictWriter, tempfile.NamedTemporaryFile).
    thresholds: none.
    returns: None; writes `path` atomically (temp file in the same directory, then
        Path.replace()) so segment_manifest.csv/run_registry.csv/
        segment_membership.csv are never observed half-written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=str(path.parent), suffix=".tmp") as tmp:
        tmp_path = Path(tmp.name)
        writer = csv.DictWriter(tmp, fieldnames=list(fieldnames));writer.writeheader()
        for row in rows: writer.writerow({name: row.get(name, "") for name in fieldnames})
    tmp_path.replace(path)

def _population_hash(export_run_ids: List[str]) -> str:
    """Compute a stable content hash of a segment's file population.

    --- trace ---
    reads: `export_run_ids` -- list[str], from _build_segments()'s per-key `eids` (the
        deduplicated, sorted export_run_id membership of one candidate segment).
    calls: hashlib.sha1() (stdlib).
    thresholds: none.
    returns: 40-char hex sha1 of the "|"-joined, sorted export_run_ids; written into
        segment_manifest.csv's population_hash column and consumed downstream by
        tools/run_segment_orchestrator.py's incremental-skip logic (a changed
        population_hash for a segment_id triggers re-processing) and by
        _build_segments()'s own "redundant_single_child" detection (a parent whose
        population_hash matches a direct child's is collapsed to
        run_type="registration").
    """
    token="|".join(sorted(export_run_ids));return hashlib.sha1(token.encode()).hexdigest()

_UNSAFE_FOLDER_CHARS = re.compile(r'[|/\\:*?"<>=\s]')
# A selected blank dimension must remain distinguishable from an unselected
# dimension, but its folder component must describe the data state rather than a
# particular enterprise. ``no_external_client`` means exactly that: the cut
# selected a blank client value. It does not imply enterprise scope, because a
# real business-center value may still be present on the same segment.
_BLANK_SELECTED_FOLDER_TOKEN = "no_external_client"
def _sanitize_folder(segment_id:str)->str:
    """Turn a segment_id into a filesystem-safe, lowercase output-folder name,
    preserving the distinction between an unselected dimension and one explicitly
    selected-blank.

    --- trace ---
    reads: `segment_id` -- a "|"-joined dimension-value string, from
        _build_registry()'s per-row `sid` (new-segment folder assignment) or from
        main()'s existing-registry fallback for a segment whose registry row has no
        stored output_folder.
    calls: none (re.sub() only).
    thresholds: _UNSAFE_FOLDER_CHARS (module-level compiled regex, l.61: characters
        replaced with "_"); _BLANK_SELECTED_FOLDER_TOKEN = "no_external_client" (module
        constant, l.67: the literal substituted for an empty-but-selected segment_id
        part, to keep it distinguishable from a not-selected dimension -- see the
        comment block above for the full rationale).
    returns: str; consumed by _build_registry() as a new segment's output_folder (with
        a "_2", "_3", ... suffix appended on a folder-name collision).
    """
    # No "+" quantifier on _UNSAFE_FOLDER_CHARS and no .strip("_") at the end:
    # both are deliberate. An empty part between/after separator pipes is
    # distinct from that same dimension not being selected at all (e.g.
    # "imperial|Template", which pools every value of the field, blank
    # included) — collapsing consecutive separator runs into one "_" and
    # trimming leading/trailing "_" would erase exactly that distinguishing
    # signal, so both segment_ids would sanitize to the identical folder name
    # even though they are different populations (the not-selected form is
    # always a superset of the selected-blank form). Rendering the blank part
    # as _BLANK_SELECTED_FOLDER_TOKEN before the generic substitution below
    # keeps that distinction self-explanatory rather than relying on a bare
    # underscore. segment_id can never itself start with an unsafe char (the
    # root dimension, unit_system, is never blank — rows with a blank root
    # are skipped before any subset is built), so the first part is never
    # replaced this way.
    readable = "|".join(
        part or _BLANK_SELECTED_FOLDER_TOKEN for part in segment_id.split("|")
    )
    return _UNSAFE_FOLDER_CHARS.sub("_", readable).lower()

def _build_membership_rows(manifest_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Flatten each manifest row's internal export_run_ids/seed_export_run_ids
    (pipe-delimited, not written to segment_manifest.csv) into one
    (segment_id, export_run_id, is_seed) row per membership pair.

    Covers every segment in manifest_rows, not just run_registry-eligible
    (bundle/reference) ones, so segment_membership.csv joins cleanly against
    the full segment_manifest.csv the same way file_count does today.

    --- trace ---
    reads: `manifest_rows` -- _build_segments()'s return value, passed in by main();
        reads each row's segment_id plus its internal (not written to
        segment_manifest.csv) export_run_ids/seed_export_run_ids pipe-delimited fields.
    calls: none.
    thresholds: none.
    returns: list[dict] with keys segment_id/export_run_id/is_seed ("true"/"false"
        strings), sorted by (segment_id, export_run_id); this is
        segment_membership.csv's row shape (MEMBERSHIP_FIELDNAMES). Consumed by main()
        (written to segment_membership.csv) and, downstream, by
        tools/run_segment_orchestrator.py's load_membership() and
        tools/extract_segment_subtree.py -- both read this file grouped by
        segment_id -> sorted export_run_ids.
    """
    rows: List[Dict[str, str]] = []
    for r in manifest_rows:
        sid = r["segment_id"]
        eids = [x for x in (r.get("export_run_ids") or "").split("|") if x]
        seeds = {x for x in (r.get("seed_export_run_ids") or "").split("|") if x}
        for eid in eids:
            rows.append({"segment_id": sid, "export_run_id": eid, "is_seed": "true" if eid in seeds else "false"})
    rows.sort(key=lambda row: (row["segment_id"], row["export_run_id"]))
    return rows

def _membership_by_segment(membership_rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    """Group membership CSV rows into segment_id -> sorted export_run_ids, for
    reconstructing a prior run's per-segment population (used by _build_registry's
    population_changed diffing in place of the old registry-embedded export_run_ids).

    --- trace ---
    reads: `membership_rows` -- previously-written segment_membership.csv rows
        (segment_id/export_run_id/is_seed), read by main() via _read_csv() when
        membership_path already exists on disk.
    calls: none.
    thresholds: none.
    returns: Dict[segment_id, sorted list[export_run_id]]; consumed by main() as
        `existing_membership`, passed to _build_registry() so population_changed
        diffing (new_files/removed_files notes) can compare against the prior run's
        actual file set instead of only its population_hash.
    """
    grouped: Dict[str, List[str]] = defaultdict(list)
    for row in membership_rows:
        sid = (row.get("segment_id") or "").strip()
        eid = (row.get("export_run_id") or "").strip()
        if sid and eid:
            grouped[sid].append(eid)
    return {sid: sorted(eids) for sid, eids in grouped.items()}

def _append_note(row,k,v=""):
    """Append a "|"-delimited note token to row["notes"], creating the field if absent.

    --- trace ---
    reads: `row` -- a manifest or registry row dict, mutated in place; `k`, `v` --
        caller-supplied note key and optional value, from _build_segments() (e.g.
        "redundant_single_child") and _build_registry() (e.g. "population_changed",
        "new_files", "run_type_changed").
    calls: none.
    thresholds: none.
    returns: None; mutates `row["notes"]` in place, appending "k" or "k:v" after a "|"
        separator if notes already has content. Consumed wherever the caller later
        writes that row to segment_manifest.csv/run_registry.csv (the `notes` column).
    """
    note=f"{k}:{v}" if v else k
    if row.get("notes"): row["notes"] += f"|{note}"
    else: row["notes"]=note

_GOVERNANCE_ROLE_CANONICAL = {
    "project": "Project", "template": "Template", "container": "Container", "generic": "Generic",
}


def _invalid_required_value_reason(value: str) -> "str | None":
    """Return why `value` fails the required-field contract for a segment
    dimension, or None if it's valid. Blank means "missing metadata"; an
    explicit N/A-style spelling means "reviewed, does not apply" — both are
    invalid for a required segment dimension under the current metadata
    contract (only project_label, which this file does not use, is allowed
    to carry a not-applicable sentinel).

    --- trace ---
    reads: `value` -- one raw file_metadata.csv cell value, from
        _validate_required_metadata()'s per-row/per-field loop.
    calls: is_na_token() (tools/na_token.py, imported at module top).
    thresholds: none named beyond the blank-string and is_na_token() checks themselves
        (there is no separate constant table of "invalid" values here).
    returns: "missing_value" | "not_applicable_sentinel" | None; consumed by
        _validate_required_metadata() to decide whether to emit a diagnostic for that
        row/field.
    """
    stripped = (value or "").strip()
    if not stripped:
        return "missing_value"
    if is_na_token(stripped):
        return "not_applicable_sentinel"
    return None


# Fields whose values feed segment_id/ancestor_segment_ids construction
# (DIMENSION_CONFIG's own field set) -- export_run_id is REQUIRED_ROW_FIELDS'
# other member, but is never embedded in a segment_id or ancestor_segment_ids
# value, so it doesn't need the extra ";" restriction below (Codex review
# finding on PR #423: scoping this too broadly would block an otherwise-valid
# export_run_id for no functional reason).
_DIMENSION_FIELD_NAMES = {d["field"] for d in DIMENSION_CONFIG}


def _invalid_dimension_value_reason(value: str) -> "str | None":
    """Additional check for DIMENSION_CONFIG fields only: a literal ";" is
    rejected (D-028). _build_segments() serializes ancestor_segment_ids by
    joining per-segment ancestor ids with ";", relying on ";" never
    appearing inside a segment_id itself (segment_id is built by
    "|"-joining these same dimension values). A dimension value containing
    ";" would silently reintroduce the exact delimiter-collision class of
    bug ";" was chosen to fix in the first place -- reject it at the source
    instead of trying to detect/repair a corrupted ancestor_segment_ids
    field downstream.

    --- trace ---
    reads: `value` -- one raw file_metadata.csv cell value, from
        _validate_required_metadata()'s per-row/per-field loop, only for fields in
        _DIMENSION_FIELD_NAMES.
    calls: none.
    thresholds: the literal `";"` character (D-028, per this function's own docstring
        above) -- hardcoded, not a named constant.
    returns: "semicolon_not_allowed" | None; consumed by _validate_required_metadata()
        as an additional check layered on top of _invalid_required_value_reason() for
        DIMENSION_CONFIG fields only.
    """
    if ";" in (value or ""):
        return "semicolon_not_allowed"
    return None


def _validate_required_metadata(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Validate REQUIRED_ROW_FIELDS on every row of file_metadata.csv, plus
    export_run_id uniqueness. Returns a list of diagnostic dicts (empty if
    every row is valid); never raises — the caller (main()) decides whether
    and how to block the build.

    Each diagnostic has:
      row_number    — source row number, 1-indexed including the header, so
                       the first data row is 2 (matches what a human editing
                       the CSV in a spreadsheet would see).
      export_run_id — best-effort, for cross-referencing even when
                       export_run_id itself is the failing field.
      field         — the invalid field name (or "export_run_id" for a
                       duplicate-row conflict).
      raw_value     — the offending raw value.
      reason        — "missing_value" | "not_applicable_sentinel" |
                       "semicolon_not_allowed" (DIMENSION_CONFIG fields only) |
                       "duplicate_row_conflict:first_seen_row=<N>".

    export_run_id is meant to be a unique join key into file_metadata.csv —
    any export_run_id repeated across rows is a data-integrity conflict this
    tool cannot silently resolve (which row is authoritative?), so it blocks
    the build rather than picking one arbitrarily.

    --- trace ---
    reads: `rows` -- file_metadata.csv rows as read by main() via _read_csv(); reads
        each row's REQUIRED_ROW_FIELDS values (export_run_id, unit_system,
        governance_role, client_label, discipline_label, business_center_label).
    calls: _invalid_required_value_reason(); _invalid_dimension_value_reason() (only
        for _DIMENSION_FIELD_NAMES fields).
    thresholds: REQUIRED_ROW_FIELDS (module constant, l.24); _DIMENSION_FIELD_NAMES
        (module constant, l.151, derived from DIMENSION_CONFIG).
    returns: list[dict] diagnostics (row_number/export_run_id/field/raw_value/reason),
        empty if every row is valid; never raises. Consumed by main(), which blocks the
        entire build (returns 1, writes nothing) if this list is non-empty.
    """
    diagnostics: List[Dict[str, str]] = []
    first_seen_row_by_eid: Dict[str, int] = {}
    for row_number, row in enumerate(rows, start=2):
        eid = (row.get("export_run_id") or "").strip()
        for field in REQUIRED_ROW_FIELDS:
            raw = row.get(field, "")
            reason = _invalid_required_value_reason(raw)
            if not reason and field in _DIMENSION_FIELD_NAMES:
                reason = _invalid_dimension_value_reason(raw)
            if reason:
                diagnostics.append({
                    "row_number": str(row_number),
                    "export_run_id": eid,
                    "field": field,
                    "raw_value": raw or "",
                    "reason": reason,
                })
        if eid:
            prior_row = first_seen_row_by_eid.get(eid)
            if prior_row is None:
                first_seen_row_by_eid[eid] = row_number
            else:
                diagnostics.append({
                    "row_number": str(row_number),
                    "export_run_id": eid,
                    "field": "export_run_id",
                    "raw_value": eid,
                    "reason": f"duplicate_row_conflict:first_seen_row={prior_row}",
                })
    return diagnostics


def _normalize_rows(rows: List[Dict[str, str]]) -> "tuple[List[Dict[str, str]], List[tuple]]":
    """Case-normalize DIMENSION_CONFIG fields before they enter segment_id
    construction, so a manual-edit typo (e.g. "Imperial" vs "imperial" during
    the Run A -> Run B annotation pause) does not silently fragment one
    population into two shadow segments that never merge.

    This function assumes every field value has already passed
    _validate_required_metadata() (blank/N/A values are rejected there, not
    folded here) — it only performs casing canonicalization:

      - unit_system: folds to lowercase — the established canonical form
        ("imperial" / "metric") used throughout the pipeline.
      - governance_role: values matching a KNOWN_ROLES member case-insensitively
        fold to that member's canonical casing (Project/Template/Container/
        Generic). A value that is NOT a case variant of a known role is not
        assumed to be a typo of one — it falls through to the same
        first-seen-casing fold as client_label/discipline_label below, so
        repeated variants of an unrecognized role still converge on one
        segment instead of fragmenting (main()'s unrecognized-role warning
        still fires for it either way).
      - client_label / discipline_label / business_center_label: no fixed
        enum. Case-insensitive fold to the casing of the first occurrence in
        row order. `rows` is a list, not a set/dict, so "first occurrence" is
        deterministic regardless of any hash/iteration order.
      - business_center_label additionally: a purely-numeric value shorter
        than 4 digits is left-zero-padded to 4 digits (e.g. "0" -> "0000",
        "796" -> "0796") BEFORE the case-fold above, so it folds together
        with any correctly-formatted 4-digit occurrence of the same code
        instead of fragmenting into a second, spurious segment. This is a
        defensive fix for a real operator-workflow failure mode: opening
        file_metadata.csv in Excel without importing the business_center_
        label column as Text causes Excel to reinterpret "0000" as the
        number 0 and silently drop the leading zeros on save. Every real
        business_center_label value observed in this corpus (enterprise
        "0000" included) is exactly 4 digits, so padding up is safe; a
        value already at or above 4 digits, or containing any non-digit
        character (e.g. "BC_1234", "Page"), is left untouched.

    Sentinel handling (N/A-style spellings, enterprise-bookkeeping-token
    folding) does not apply to any DIMENSION_CONFIG field — they are the
    required, explicit-metadata segment dimensions validated by
    _validate_required_metadata() before this function ever sees them via
    main(). In particular, "0000"/"BC_0000" are ordinary literal
    business_center_label values under the current contract (the configured
    internal-enterprise client label paired with "0000"), not enterprise-
    bookkeeping tokens to be folded away. Only project_label (not a
    DIMENSION_CONFIG field; not read by this file at all) may carry an
    explicit not-applicable sentinel.

    Returns (normalized_rows, changes) where changes is a list of
    (field, raw_value, normalized_value) tuples, one per row-field whose value
    was altered by normalization (duplicates included — callers aggregate).

    --- trace ---
    reads: `rows` -- file_metadata.csv rows, from main() (post-validation) and from
        _build_segments() (which re-normalizes internally even when called directly,
        e.g. from tests, since normalization is idempotent).
    calls: none (is_na_token() is NOT called here -- DIMENSION_CONFIG fields are exempt
        from sentinel folding, per the "Sentinel handling" paragraph above).
    thresholds: DIMENSION_CONFIG (module constant, l.35-41: which fields get normalized
        and how -- root/governance/cut types); _GOVERNANCE_ROLE_CANONICAL (module
        constant, l.125-127: case-insensitive role-name canonicalization map); the
        business_center_label zero-pad threshold `len(raw) < 4` (hardcoded literal, per
        the Excel-truncation rationale above).
    returns: (normalized_rows: list[dict], changes: list[tuple(field, raw_value,
        normalized_value)]); normalized_rows is consumed by _build_segments() to
        construct segment keys; `changes` is consumed by main() to emit aggregated
        "[WARN] Normalized ..." diagnostics.
    """
    fields = [d["field"] for d in DIMENSION_CONFIG]
    first_seen: Dict[str, Dict[str, str]] = {f: {} for f in fields}
    changes: List[tuple] = []
    normalized_rows: List[Dict[str, str]] = []

    for row in rows:
        new_row = dict(row)
        for field in fields:
            original_raw = (row.get(field) or "").strip()
            if not original_raw:
                continue
            raw = original_raw
            if field == "business_center_label" and raw.isdigit() and len(raw) < 4:
                raw = raw.zfill(4)
            if field == "unit_system":
                canon = raw.lower()
            elif field == "governance_role":
                canon = _GOVERNANCE_ROLE_CANONICAL.get(raw.lower())
                if canon is None:
                    canon = first_seen[field].setdefault(raw.lower(), raw)
            else:
                canon = first_seen[field].setdefault(raw.lower(), raw)
            if canon != original_raw:
                changes.append((field, original_raw, canon))
            new_row[field] = canon
        normalized_rows.append(new_row)

    return normalized_rows, changes

def _build_segments(rows:List[Dict[str,str]],min_files:int,enable_cross_org_template_bundles:bool=False,enable_parent_bundle_runs:bool=False)->List[Dict[str,str]]:
    """Build every candidate segment (one row per subset of non-root dimension values,
    scoped under the single root dimension) from file_metadata.csv rows, then classify
    each into a run_type, segment_purpose, and segment_label.

    This is the core of the segmentation-lattice build: for every distinct root_value
    (unit_system), it enumerates the full powerset of the present non-root dimension
    values (governance_role/client_label/discipline_label/business_center_label) per
    file, unions file populations across the powerset via `populations[key]`, then
    converts each distinct key into one segment_manifest.csv row with its parent/
    ancestor relationships, run_type classification, and human-readable purpose/label.

    --- trace ---
    reads: `rows` -- file_metadata.csv rows (post _validate_required_metadata(), pre-
        or post-normalization -- this function re-normalizes internally via
        _normalize_rows()); `min_files` -- int, from main()'s --min-files (default 3);
        `enable_cross_org_template_bundles`/`enable_parent_bundle_runs` -- bool, from
        main()'s --enable-cross-org-template-bundles/--enable-parent-bundle-runs flags.
    calls: _normalize_rows(); _population_hash() (per segment key); _append_note() (for
        below_min_files/seed_only/redundant_single_child notes); itertools.combinations()
        (stdlib, to enumerate the non-root-field powerset); nested _subset_to_id() (many
        times, to render a frozenset key as a "|"-joined segment_id string) and nested
        child_span() (once per row with children, to classify "multi_client" vs
        "single_client" for segment_purpose assignment).
    thresholds: DIMENSION_CONFIG (module constant, l.35-41: exactly one root + one
        governance dimension enforced by a raised ValueError, plus the cut dimensions);
        SEED_ROLES = {"Template","Container"} (module constant, l.15: which
        governance_role values count a file as a "seed" for has_seed_file/
        seed_export_run_ids); `min_files` (run_type "bundle" threshold, passed in, not
        hardcoded); the run_type/segment_purpose assignment itself is a long sequential
        if/elif chain over (segment_level, is_*_cut booleans, role, run_type)
        combinations -- see notes below.
    returns: list[dict] with MANIFEST_FIELDNAMES-shaped keys plus two internal-only
        columns export_run_ids/seed_export_run_ids (pipe-delimited, consumed by
        _build_membership_rows() and _build_registry() but never written to
        segment_manifest.csv itself, since MANIFEST_FIELDNAMES excludes them), sorted
        by (segment_level, segment_id). Consumed by main() as `manifest_rows`, then
        passed to _build_membership_rows() and _build_registry().
    notes: (mechanical-extraction risk) `rows_out` entries are mutated in place across
        3 sequential passes within this one function (run_type assignment,
        segment_purpose/segment_label assignment, then the redundant-population-hash
        collapse pass) -- a naive single-pass static parser could misread this as
        dead/overwritten state rather than sequential refinement, since a row's
        run_type/segment_purpose/segment_label set by an earlier pass can be
        overwritten by a later one (the redundant-hash pass in particular). The
        segment_purpose/segment_label if/elif chain (roughly l.502-537 as originally
        read) is sequential-control-flow-as-policy: the ~35-member governance-purpose
        taxonomy (population_denominator/client_population/cross_template_agreement/...)
        exists only as branch order in this chain, not as a lookup table, so
        determining "what purpose does segment X get" requires reading the chain
        top-to-bottom rather than consulting one data structure.
    """
    root_dims = [d for d in DIMENSION_CONFIG if d["type"] == "root"]
    gov_dims = [d for d in DIMENSION_CONFIG if d["type"] == "governance"]
    cut_dims = [d for d in DIMENSION_CONFIG if d["type"] == "cut"]
    if len(root_dims) != 1 or len(gov_dims) != 1:
        raise ValueError("DIMENSION_CONFIG must have exactly one root and one governance dimension")
    root_field = root_dims[0]["field"]
    governance_field = gov_dims[0]["field"]
    client_field = "client_label"
    cfg_fields = [d["field"] for d in DIMENSION_CONFIG]

    populations = defaultdict(list)
    seed_pops = defaultdict(list)
    project_presence_by_l2: Dict[str, bool] = defaultdict(bool)

    def _subset_to_id(key: frozenset) -> str:
        """Render a frozenset of (field, value) pairs as this segment's canonical
        "|"-joined segment_id string, in DIMENSION_CONFIG field order.

        --- trace ---
        reads: `key` -- a frozenset of (field, value) tuples; closes over `cfg_fields`
            (the enclosing _build_segments()'s DIMENSION_CONFIG field-name list, root
            dimension first).
        calls: none.
        thresholds: none (field order comes from the closed-over `cfg_fields` list, not
            re-derived here).
        returns: str, e.g. "imperial|Template|Acme"; called repeatedly throughout
            _build_segments() (populations grouping, project_presence_by_l2 keys,
            segment_id/parent_id/ancestor_segment_ids construction) -- this is the
            single place segment_id strings are actually assembled from a
            dimension-value key.
        """
        kv = dict(key)
        return "|".join(kv[f] for f in cfg_fields if f in kv)

    normalized_rows, _changes = _normalize_rows(rows)

    for row in normalized_rows:
        export_run_id = (row.get("export_run_id") or "").strip()
        if not export_run_id:
            continue
        dim_values = {}
        for field in cfg_fields:
            value = (row.get(field) or "").strip()
            if value:
                dim_values[field] = value
        root_value = dim_values.get(root_field, "")
        if not root_value:
            continue
        non_root_pairs = [(f, dim_values[f]) for f in cfg_fields if f != root_field and f in dim_values]
        for size in range(len(non_root_pairs) + 1):
            for subset in combinations(non_root_pairs, size):
                key = frozenset([(root_field, root_value), *subset])
                populations[key].append(export_run_id)
                governance_value = dim_values.get(governance_field, "")
                if governance_value in SEED_ROLES:
                    seed_pops[key].append(export_run_id)
                if size == 1 and subset and subset[0][0] == client_field and governance_value == "Project":
                    project_presence_by_l2[_subset_to_id(key)] = True

    keys = sorted(populations.keys(), key=lambda k: (len(k), _subset_to_id(k)))
    key_set = set(keys)
    rows_out = []
    key_to_row = {}
    key_to_children = defaultdict(list)
    row_to_key = {}
    for key in keys:
        dim_map = dict(key)
        non_root_fields_present = [f for f in cfg_fields if f != root_field and f in dim_map]
        segment_id = _subset_to_id(key)
        if not non_root_fields_present:
            parent_id = ""
        else:
            parent_key = frozenset((f, v) for f, v in key if f != non_root_fields_present[-1])
            parent_id = _subset_to_id(parent_key)
        # Each entry here is this segment's own immediate structural parent
        # for one dropped non-root field (there can be more than one, since
        # multiple non-root fields may be present) -- NOT the full transitive
        # ancestor closure. Full closure (structural_ancestor, D-027) is
        # computed downstream by compare_cross_segment.py's
        # _build_ancestor_map(), which walks this field as a multi-parent
        # adjacency list and recursively unions each parent's own ancestors.
        ancestor_ids = []
        for field in non_root_fields_present:
            anc_key = frozenset((f, v) for f, v in key if f != field)
            if anc_key in key_set:
                ancestor_ids.append(_subset_to_id(anc_key))
        ancestor_ids = sorted(ancestor_ids)
        eids = sorted(set(populations[key]))
        seeds = sorted(set(seed_pops.get(key, [])))
        extra = []
        for d in cut_dims:
            if d["field"] == client_field:
                continue
            if d["field"] in dim_map:
                extra.append(f"{d['field']}={dim_map[d['field']]}")
        row = {
            "segment_id": segment_id,
            "parent_segment_id": parent_id,
            "segment_level": str(len(key)),
            "unit_system": dim_map.get("unit_system", ""),
            "governance_role": dim_map.get(governance_field, ""),
            "client_label": dim_map.get(client_field, ""),
            "discipline_label": dim_map.get("discipline_label", ""),
            "business_center_label": dim_map.get("business_center_label", ""),
            # Retained as a schema column only (downstream tools read it by
            # name with a "" default) — collection_label is no longer a
            # segmentation dimension, see DIMENSION_CONFIG.
            "collection_label": "",
            "extra_dimensions": "|".join(extra),
            # ";"-joined, NOT "|"-joined (D-028): each element of ancestor_ids is
            # itself a segment_id, which is internally "|"-delimited (see
            # _subset_to_id()). Joining a list of already-"|"-delimited
            # strings with "|" collides the outer and inner delimiters and
            # cannot be losslessly split back into the original ancestor-id
            # list (e.g. ["imperial|0000", "imperial|Container"] and
            # ["imperial", "0000|imperial|Container"] both serialize to the
            # same "|".join result). ";" does not otherwise occur in a
            # segment_id (dimension values are themselves "|"-delimited into
            # segment_id, so a ";" separator one level up is unambiguous).
            "ancestor_segment_ids": ";".join(ancestor_ids),
            "run_type": "",
            "file_count": str(len(eids)),
            "export_run_ids": "|".join(eids),
            "has_seed_file": "true" if seeds else "false",
            "seed_export_run_ids": "|".join(seeds),
            "population_hash": _population_hash(eids),
            "notes": "",
            "segment_purpose": "",
            "segment_label": "",
        }
        rows_out.append(row)
        key_to_row[key] = row
        row_to_key[id(row)] = key

    for parent_key in keys:
        parent_size = len(parent_key)
        for child_key in keys:
            if len(child_key) == parent_size + 1 and parent_key.issubset(child_key):
                key_to_children[parent_key].append(child_key)

    for r in rows_out:
        fc=int(r["file_count"]); role=r["governance_role"]
        notes = []
        if fc < min_files:
            notes.append("below_min_files")
        if r["segment_level"] == "2" and r["has_seed_file"] == "true":
            if not role and not project_presence_by_l2.get(r["segment_id"], False):
                notes.append("seed_only")
            elif role and role != "Project":
                notes.append("seed_only")
        if notes:
            r["notes"] = "|".join(notes)
        seg = r["segment_id"]
        key = row_to_key[id(r)]
        has = bool(key_to_children.get(key))
        if has:
            is_cross_org_template = (
                enable_cross_org_template_bundles
                and r["segment_level"] == "2"
                and r["governance_role"] == "Template"
                and not r["client_label"]
            )
            is_role_fixed_parent = (
                enable_parent_bundle_runs
                and r["segment_level"] == "2"
                and r["governance_role"] != ""
                and not r["client_label"]
                and fc >= min_files
            )
            is_scoped_leaf = (
                int(r["segment_level"]) >= 3
                and r["governance_role"] != ""
            )
            if not is_cross_org_template and not is_role_fixed_parent and not is_scoped_leaf:
                r["run_type"] = "registration"; continue
        if fc>=min_files: r["run_type"]="bundle"
        elif role in {"Template","Container","Generic"}: r["run_type"]="reference"
        elif role=="Project": r["run_type"]="skip"
        elif role == "":
            r["run_type"] = "registration"
        else: r["run_type"]="registration"
    # purpose/label
    def child_span(r):
        """Classify a segment's direct level-3 children as "multi_client" or
        "single_client", used to distinguish a genuine cross-org template pool from a
        redundant single-child pass-through.

        --- trace ---
        reads: `r` -- one row from `rows_out`, passed by the purpose/label assignment
            loop; closes over `row_to_key`, `key_to_children`, `key_to_row` (all built
            earlier in the enclosing _build_segments() call).
        calls: none (set comprehension only).
        thresholds: none named -- "distinct scope" is `client_label or
            business_center_label` on a level-3 child, treated as mutually exclusive
            per the comment above this function.
        returns: "multi_client" if more than one distinct client_label/
            business_center_label value appears among direct level-3 children, else
            "single_client"; consumed only by the segment_purpose if/elif chain
            (distinguishes "cross_org_template_pool" from "redundant_single_child" for
            a level-2/3 role_alone Template segment).
        """
        row_key = row_to_key[id(r)]
        # A distinct scope is a real client_label or business_center_label on
        # a level-3 child (mutually exclusive per row today, so a plain set
        # of "whichever is present" is enough to detect more than one
        # distinct scope among direct children).
        cs={
            key_to_row[k]["client_label"] or key_to_row[k]["business_center_label"]
            for k in key_to_children.get(row_key,[])
            if key_to_row[k]["segment_level"]=="3"
            and (key_to_row[k]["client_label"] or key_to_row[k]["business_center_label"])
        }
        return "multi_client" if len(cs)>1 else "single_client"
    for r in rows_out:
        pur="insufficient_population" if r["run_type"]=="skip" else ""
        lev,role,rt=int(r["segment_level"]),r["governance_role"],r["run_type"]
        disc=r["discipline_label"]
        bc=r["business_center_label"]
        client=r["client_label"]
        # client_label and business_center_label are mutually exclusive "why
        # was this captured" scopes; each is a distinct differentiator, so a
        # level-3/4 segment is only "disc cut" or "bc cut" when the other
        # scope isn't also present in that segment's key.
        is_disc_cut=bool(disc and not client and not bc)
        is_client_disc_cut=bool(disc and client)
        is_bc_cut=bool(bc and not client and not disc)
        is_bc_disc_cut=bool(bc and disc and not client)
        is_role_alone=bool(not disc and not bc and not client)
        if lev==1: pur="population_denominator"
        elif lev == 2 and client and not role:
            pur = "client_population"
        elif lev == 2 and bc and not role:
            pur = "business_center_population"
        elif lev in (2,3) and is_role_alone and role=="Template":
            if rt=="bundle": pur="cross_template_agreement"
            elif rt in {"registration","reference"}: pur="cross_org_template_pool" if child_span(r)=="multi_client" else "redundant_single_child"
        elif lev in (2,3) and is_role_alone and role=="Project": pur="cross_project_practice" if rt=="bundle" else "practiced_standards_corpus"
        elif lev in (2,3) and is_role_alone and role=="Container": pur="coordination_corpus"
        elif lev in (2,3) and is_role_alone and role=="Generic" and rt=="reference": pur="generic_reference_corpus"
        elif lev in (3,4) and is_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="discipline_templates"
        elif lev in (3,4) and is_disc_cut and role=="Project": pur="discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (3,4) and is_disc_cut and role=="Container": pur="discipline_coordination"
        elif lev in (3,4) and is_disc_cut and role=="Generic" and rt=="reference": pur="discipline_reference"
        elif lev in (3,4) and is_bc_cut and role=="Template" and rt in {"bundle","reference"}: pur="business_center_standard_anchor"
        elif lev in (3,4) and is_bc_cut and role=="Project": pur="business_center_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (3,4) and is_bc_cut and role=="Container": pur="business_center_coordination"
        elif lev in (3,4) and is_bc_cut and role=="Generic" and rt=="reference": pur="business_center_reference"
        elif lev==3 and role=="Template" and client and rt in {"bundle","reference"}: pur="client_standard_anchor"
        elif lev==3 and role=="Project" and client: pur="client_practice" if rt=="bundle" else "insufficient_population"
        elif lev==3 and role=="Container" and client: pur="client_coordination"
        elif lev==3 and role=="Generic" and client and rt=="reference": pur="client_reference"
        elif lev==4 and is_client_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="client_discipline_standard_anchor"
        elif lev==4 and is_client_disc_cut and role=="Project": pur="client_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev==4 and is_client_disc_cut and role=="Container": pur="client_discipline_coordination"
        elif lev==4 and is_client_disc_cut and role=="Generic" and rt=="reference": pur="client_discipline_reference"
        elif lev in (4,5) and is_bc_disc_cut and role=="Template" and rt in {"bundle","reference"}: pur="business_center_discipline_standard_anchor"
        elif lev in (4,5) and is_bc_disc_cut and role=="Project": pur="business_center_discipline_practice" if rt=="bundle" else "insufficient_population"
        elif lev in (4,5) and is_bc_disc_cut and role=="Container": pur="business_center_discipline_coordination"
        elif lev in (4,5) and is_bc_disc_cut and role=="Generic" and rt=="reference": pur="business_center_discipline_reference"
        r["segment_purpose"]=pur
        unit=r["unit_system"].title(); sid=r["segment_id"]
        templates={"population_denominator":f"All {unit} files","cross_org_template_pool":f"{unit} templates — all organisations (registration only)","cross_template_agreement":f"{unit} templates — cross-template agreement","practiced_standards_corpus":f"{unit} projects — full corpus","cross_project_practice":f"{unit} projects — cross-project practice","coordination_corpus":f"{unit} coordination files","generic_reference_corpus":f"{unit} generic reference","client_population":f"{client} — all roles combined","client_standard_anchor":f"{client} templates — standards as authored","client_practice":f"{client} projects — standards as practiced","client_coordination":f"{client} coordination files","client_reference":f"{client} generic reference","insufficient_population":f"{sid} — below minimum file threshold","discipline_practice":f"{disc} projects — standards as practiced","discipline_templates":f"{disc} templates — standards as authored","discipline_coordination":f"{disc} coordination files","discipline_reference":f"{disc} generic reference","client_discipline_standard_anchor":f"{client} {disc} templates — standards as authored","client_discipline_practice":f"{client} {disc} projects — standards as practiced","client_discipline_coordination":f"{client} {disc} coordination files","client_discipline_reference":f"{client} {disc} generic reference","business_center_population":f"{bc} — all roles combined","business_center_standard_anchor":f"{bc} templates — standards as authored","business_center_practice":f"{bc} projects — standards as practiced","business_center_coordination":f"{bc} coordination files","business_center_reference":f"{bc} generic reference","business_center_discipline_standard_anchor":f"{bc} {disc} templates — standards as authored","business_center_discipline_practice":f"{bc} {disc} projects — standards as practiced","business_center_discipline_coordination":f"{bc} {disc} coordination files","business_center_discipline_reference":f"{bc} {disc} generic reference"}
        if r["segment_purpose"]:
            r["segment_label"]=templates.get(r["segment_purpose"],sid)
        else:
            r["segment_label"]=sid
    # pass5 redundant hash
    #
    # A parent is redundant whenever ANY direct child has byte-identical
    # population — not only when it happens to have exactly one child. A
    # node's only plausible sibling is typically a discipline-cut child, a
    # business-center-cut child, a client-cut child, etc. — most parents have
    # several distinct children, but one specific child can still turn out to
    # carry the parent's *entire* population (e.g. a business-center pool
    # where every file also happens to share the same discipline_label). Such
    # a parent is a true duplicate of that child: same files, same hash, yet
    # both would otherwise be left as independently runnable bundle/reference
    # segments.
    #
    # If more than one child ties on population_hash, the pointer target is
    # picked deterministically by segment_id — which specific matching child
    # gets named in the note doesn't change whether the parent itself is
    # correctly recognized as redundant.
    for r in rows_out:
        if r["run_type"] not in {"bundle", "registration", "reference"}: continue
        row_key = row_to_key[id(r)]
        direct_children = [key_to_row[k] for k in key_to_children.get(row_key, [])]
        matches = sorted(
            (c for c in direct_children if c["population_hash"] == r["population_hash"]),
            key=lambda c: c["segment_id"],
        )
        if matches:
            ch=matches[0]["segment_id"]; _append_note(r,"redundant_single_child",ch)
            r["run_type"]="registration"; r["segment_purpose"]="redundant_single_child"; r["segment_label"]=f"{r['segment_id']} — same population as {ch}"
    rows_out.sort(key=lambda r:(int(r["segment_level"]),r["segment_id"]))
    return rows_out

def _build_registry(
    manifest_rows: List[Dict[str, str]],
    existing_registry: List[Dict[str, str]] | None = None,
    existing_membership: Dict[str, List[str]] | None = None,
) -> List[Dict[str, str]]:
    """Build run_registry.csv rows from freshly computed manifest rows.

    When existing_registry is supplied, prior segment_id -> output_folder
    mappings are reused verbatim (folder-name stability across runs), and
    status/last_run_utc are carried over unless population_hash or run_type
    changed for that segment_id (population-hash-based incremental skip).
    A run_type change (e.g. a --min-files threshold change turning a
    "reference" segment into a "bundle" with the same file population) must
    also reset status, since population_hash alone would miss it and the
    orchestrator would keep skipping a segment that now needs a different
    analysis to be produced.

    When population_hash changes, the reason is diffed against the prior run's
    export_run_ids and recorded as new_files and/or removed_files counts
    alongside the population_changed marker, so the run plan states why a
    segment went stale rather than just that it did. The prior population is
    read from existing_membership (segment_id -> export_run_ids, built from a
    previously-written segment_membership.csv) rather than from
    existing_registry — run_registry.csv no longer carries a per-segment
    member list. A segment with no entry in existing_membership (e.g. the
    first registry rebuild after this migration, before any
    segment_membership.csv existed) is treated as having an empty prior
    population, so its notes will show every current file as new_files with
    no removed_files on that one transitional run.
    conformance_reference_mode is carried over verbatim (defaulting to "latest"
    for new segments and for older registries written before this field
    existed); no other mode is implemented yet.

    --- trace ---
    reads: `manifest_rows` -- _build_segments()'s return value, from main(); reads
        segment_id/parent_segment_id/run_type/population_hash/segment_purpose/
        segment_label/export_run_ids per row. `existing_registry` -- prior
        run_registry.csv rows (Optional, from main() via _read_csv() when
        registry_path already exists); reads segment_id/output_folder/status/
        last_run_utc/notes/conformance_reference_mode/population_hash/run_type per
        row. `existing_membership` -- Dict[segment_id, sorted export_run_ids] from
        _membership_by_segment() on a prior segment_membership.csv (Optional).
    calls: _sanitize_folder() (new segments only); _append_note()
        (population_changed/new_files/removed_files/run_type_changed notes).
    thresholds: `{"bundle", "reference"}` (hardcoded literal set, used twice:
        eligible_rows filter and the run_type-changed detection) -- the only two
        run_types that get a run_registry.csv row at all; "latest" (hardcoded literal:
        the only implemented conformance_reference_mode).
    returns: list[dict], REGISTRY_FIELDNAMES-shaped (segment_id/parent_segment_id/
        run_type/population_hash/conformance_reference_mode/output_folder/status/
        last_run_utc/notes/segment_purpose/segment_label); consumed by main() as
        `registry_rows`, written to run_registry.csv, and subsequently read and
        mutated in place by tools/run_segment_orchestrator.py's load_registry()/
        write_registry_atomic() after each segment run. Writes a `[WARN]` line to
        stderr (not the return value) for any segment_id present in
        `existing_registry` but absent from the new manifest (dropped_ids), naming
        folders under segments/ the caller should manually review.
    """
    existing_membership = existing_membership or {}
    existing_by_id: Dict[str, Dict[str, str]] = {}
    if existing_registry:
        for row in existing_registry:
            sid = (row.get("segment_id") or "").strip()
            if sid:
                existing_by_id[sid] = row

    eligible_rows = [r for r in manifest_rows if r["run_type"] in {"bundle", "reference"}]
    new_ids = {r["segment_id"] for r in eligible_rows}

    # Reserve every folder the old registry ever assigned — carried-over AND
    # dropped segments alike — before assigning any folder to a genuinely new
    # segment. A dropped segment's directory under segments/ still holds its
    # old records/markers/analysis output (the caller is only warned to review
    # it for manual cleanup, not to delete it), so a new segment must never be
    # handed that same folder name — it would start writing into a directory
    # still full of a different segment's stale data.
    assigned_folders: set = set()
    for old_row in existing_by_id.values():
        of = old_row.get("output_folder", "")
        if of:
            assigned_folders.add(of)

    registry = []
    for row in eligible_rows:
        sid = row["segment_id"]
        old = existing_by_id.get(sid)
        if old is not None:
            folder = old.get("output_folder", "") or _sanitize_folder(sid)
            reg_row = {
                "segment_id": sid,
                "parent_segment_id": row["parent_segment_id"],
                "run_type": row["run_type"],
                "population_hash": row["population_hash"],
                # "latest" is the only mode implemented: conformance comparisons
                # (tools/compare_cross_segment.py) always resolve reference segments
                # dynamically against current output. A pinned/snapshot mode is
                # deferred until Phase-2 baseline authority is established (see
                # "current operating mode").
                "conformance_reference_mode": old.get("conformance_reference_mode", "") or "latest",
                "output_folder": folder,
                "status": old.get("status", "pending"),
                "last_run_utc": old.get("last_run_utc", ""),
                "notes": old.get("notes", ""),
                "segment_purpose": row.get("segment_purpose", ""),
                "segment_label": row.get("segment_label", ""),
            }
            population_changed = old.get("population_hash", "") != row["population_hash"]
            run_type_changed = old.get("run_type", "") != row["run_type"]
            if population_changed or run_type_changed:
                reg_row["status"] = "pending"
                reg_row["last_run_utc"] = ""
                reg_row["notes"] = row.get("notes", "")
                if population_changed:
                    _append_note(reg_row, "population_changed")
                    old_export_ids = set(existing_membership.get(sid, []))
                    new_export_ids = {x for x in row.get("export_run_ids", "").split("|") if x}
                    added = new_export_ids - old_export_ids
                    removed = old_export_ids - new_export_ids
                    # A population_hash change is purely a function of the
                    # export_run_id set, so at least one of added/removed is always
                    # non-empty here — this also covers a metadata edit that moves a
                    # file between segments (e.g. a corrected client_label): it
                    # surfaces as removed_files on the file's old segment and
                    # new_files on its new segment, with no separate detection path
                    # needed for "metadata change" as its own reason.
                    if added:
                        _append_note(reg_row, "new_files", str(len(added)))
                    if removed:
                        _append_note(reg_row, "removed_files", str(len(removed)))
                if run_type_changed:
                    _append_note(reg_row, "run_type_changed", f"{old.get('run_type', '')}->{row['run_type']}")
        else:
            base = _sanitize_folder(sid)
            folder = base
            n = 2
            while folder in assigned_folders:
                folder = f"{base}_{n}"
                n += 1
            assigned_folders.add(folder)
            reg_row = {
                "segment_id": sid,
                "parent_segment_id": row["parent_segment_id"],
                "run_type": row["run_type"],
                "population_hash": row["population_hash"],
                "conformance_reference_mode": "latest",
                "output_folder": folder,
                "status": "pending",
                "last_run_utc": "",
                "notes": row.get("notes", ""),
                "segment_purpose": row.get("segment_purpose", ""),
                "segment_label": row.get("segment_label", ""),
            }
        registry.append(reg_row)

    dropped_ids = sorted(set(existing_by_id) - new_ids)
    if dropped_ids:
        sys.stderr.write(
            f"[WARN] Segment(s) removed from registry (no longer in manifest): "
            f"{', '.join(dropped_ids)} — review corresponding folders under segments/ "
            f"for manual cleanup\n"
        )

    return registry


def _print_summary(
    manifest_path: Path,
    registry_path: Path,
    manifest_rows: List[Dict[str, str]],
    min_files: int,
) -> None:
    """Print the human-readable run-plan summary (bundle/reference/skipped/
    registration segments) to stdout after a successful build.

    --- trace ---
    reads: `manifest_path`, `registry_path` -- Paths, used only for the "written:"
        lines; `manifest_rows` -- _build_segments()'s return value, from main();
        `min_files` -- int, from main()'s --min-files, for the "Skipped (below
        min_files=N)" label.
    calls: none (print() only).
    thresholds: none beyond `min_files` itself (caller-supplied, used only in the
        printed label text).
    returns: None; side effect is stdout output only. Called once by main(), as the
        final step of a successful (non-blocked) build.
    """
    bundles = [r for r in manifest_rows if r["run_type"] == "bundle"]
    refs = [r for r in manifest_rows if r["run_type"] == "reference"]
    skips = [r for r in manifest_rows if r["run_type"] == "skip"]
    regs = [r for r in manifest_rows if r["run_type"] == "registration"]

    print(f"Segment manifest written: {manifest_path}")
    print(f"Run registry written: {registry_path}")
    print()
    print(f"Run plan ({len(bundles) + len(refs)} segments):")

    print("\n  Bundle runs:")
    for r in bundles:
        print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    print("\n  Reference runs:")
    for r in refs:
        print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    if skips:
        print(f"\n  Skipped (below min_files={min_files}):")
        for r in skips:
            print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")

    if regs:
        print("\n  Registration only (hierarchy anchors):")
        for r in regs:
            print(f"    {r['segment_label']} [{r['segment_purpose']}]  ({r['segment_id']}, {r['file_count']} files)")


def main(argv: List[str] | None = None) -> int:
    """CLI entry point: read file_metadata.csv, validate required metadata, build the
    segment lattice, and write segment_manifest.csv / run_registry.csv /
    segment_membership.csv.

    --- trace ---
    reads: CLI args --metadata-file (required), --out-dir (required), --min-files
        (default 3), --enable-cross-org-template-bundles, --enable-parent-bundle-runs.
        Reads file_metadata.csv at --metadata-file; reads any pre-existing
        run_registry.csv/segment_membership.csv under --out-dir for incremental
        carry-over.
    calls: _read_csv() (metadata file, plus existing registry/membership if present);
        _validate_required_metadata(); _normalize_rows(); _build_segments();
        _membership_by_segment(); _build_registry(); _build_membership_rows();
        _atomic_write_csv() (x3: segment_manifest.csv, run_registry.csv,
        segment_membership.csv); _print_summary().
    thresholds: REQUIRED_COLUMNS (module constant, l.17: header-presence check);
        REQUIRED_ROW_FIELDS (module constant, l.24, via _validate_required_metadata());
        KNOWN_ROLES = {"Project","Template","Container","Generic",""} (l.819 as
        originally read: unrecognized governance_role values get a [WARN], not a
        block).
    returns: int exit code (1 on missing/invalid --metadata-file, missing required
        columns, or any _validate_required_metadata() diagnostic -- in every blocking
        case, no output file is written; 0 on success). Invoked as **Run C1** (l.186 of
        tools/corpus_update_runbook.ps1); its three outputs (segment_manifest.csv,
        run_registry.csv, segment_membership.csv) are consumed by **Run C2**
        (tools/run_segment_orchestrator.py) and **Run C2.5**
        (tools/build_results_registry.py).
    """
    parser = argparse.ArgumentParser(
        description="Build segment_manifest.csv and run_registry.csv from file_metadata.csv.",
    )
    parser.add_argument("--metadata-file", required=True, help="Path to file_metadata.csv")
    parser.add_argument("--out-dir", required=True, help="Directory to write output files")
    parser.add_argument("--min-files", type=int, default=3, help="Minimum file count for a segment (default: 3)")
    parser.add_argument("--enable-cross-org-template-bundles", action="store_true", help="Allow cross-org level-2 Template segments to run as bundle/reference")
    parser.add_argument(
        "--enable-parent-bundle-runs",
        action="store_true",
        help="Allow level-2 role-fixed segments (e.g. imperial|Project) to run bundle analysis even when they have child segments."
    )
    args = parser.parse_args(argv)

    metadata_path = Path(args.metadata_file)
    if not metadata_path.is_file():
        sys.stderr.write(f"[ERROR] --metadata-file not found: {metadata_path}\n")
        return 1

    out_dir = Path(args.out_dir)
    min_files: int = args.min_files

    try:
        fieldnames, rows = _read_csv(metadata_path)
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        sys.stderr.write(f"[ERROR] Unreadable input: failed to read {metadata_path}: {exc}\n")
        return 1

    # Validate headers unconditionally — even a header-only file must declare the required columns.
    if not fieldnames:
        sys.stderr.write(f"[WARN] file_metadata.csv is completely empty (no header): {metadata_path}\n")
    else:
        missing_columns = REQUIRED_COLUMNS - set(fieldnames)
        if missing_columns:
            sys.stderr.write(
                f"[ERROR] file_metadata.csv is missing required columns: {sorted(missing_columns)}\n"
            )
            return 1
        if not rows:
            sys.stderr.write(f"[WARN] file_metadata.csv has a valid header but no data rows: {metadata_path}\n")

    # Required segmentation metadata: export_run_id, unit_system,
    # governance_role, client_label, discipline_label, business_center_label
    # must all be present, nonblank, and not an N/A-style sentinel on every
    # row. A violation on any of these blocks the entire build — no partial
    # manifest is ever written. (If a required column is missing from the CSV
    # entirely, every row fails on that field here, which blocks the build
    # just as surely as the header-presence check above, with one diagnostic
    # line per row/field instead of a single column-name message.)
    diagnostics = _validate_required_metadata(rows)
    if diagnostics:
        sys.stderr.write(
            f"[ERROR] Segment manifest build BLOCKED: {len(diagnostics)} required-metadata "
            f"violation(s) in {metadata_path}. No output written.\n"
        )
        for d in diagnostics:
            sys.stderr.write(
                f"[ERROR]   row={d['row_number']} export_run_id={d['export_run_id'] or '<missing>'} "
                f"field={d['field']} value={d['raw_value']!r} reason={d['reason']}\n"
            )
        return 1

    # Normalize once here — for the KNOWN_ROLES check and the aggregated
    # normalization-change warning below — routing both through the same
    # helper _build_segments() uses internally, so there is one source of
    # truth for "what does this dimension value canonically mean" rather than
    # two independent checks that could drift apart. _build_segments() still
    # normalizes rows itself when called directly (e.g. from tests), so
    # passing the original (un-normalized) rows through to it here is safe —
    # normalization is idempotent.
    normalized_rows, normalization_changes = _normalize_rows(rows)

    KNOWN_ROLES = {"Project", "Template", "Container", "Generic", ""}
    unknown_roles = {
        (r.get("governance_role") or "").strip()
        for r in normalized_rows
        if (r.get("governance_role") or "").strip() not in KNOWN_ROLES
    }
    for role in sorted(unknown_roles):
        sys.stderr.write(f"[WARN] Unrecognised governance_role value in metadata: '{role}' — rows with this role will create unexpected segments\n")

    if normalization_changes:
        agg: Dict[tuple, int] = defaultdict(int)
        for field, raw, canon in normalization_changes:
            agg[(field, raw, canon)] += 1
        for (field, raw, canon), count in sorted(agg.items()):
            sys.stderr.write(
                f"[WARN] Normalized {field} '{raw}' -> '{canon}' ({count} row(s)) — "
                f"check file_metadata.csv for manual-edit typos\n"
            )

    manifest_rows = _build_segments(rows, min_files, args.enable_cross_org_template_bundles, args.enable_parent_bundle_runs)

    for r in manifest_rows:
        if r["run_type"] == "bundle" and int(r["file_count"]) < min_files:
            sys.stderr.write(f"[WARN] Bundle below min_files: {r['segment_id']} ({r['file_count']} < {min_files})\n")

    ids = {r["segment_id"] for r in manifest_rows}
    for r in manifest_rows:
        if r["segment_level"] == "3" and r["parent_segment_id"] not in ids:
            sys.stderr.write(f"[WARN] Orphaned level-3 segment missing parent: {r['segment_id']} -> {r['parent_segment_id']}\n")

    bundle_by_hash = defaultdict(list)
    for r in manifest_rows:
        if r["run_type"] == "bundle":
            bundle_by_hash[r["population_hash"]].append(r["segment_id"])
    for pop_hash, segs in bundle_by_hash.items():
        if len(segs) > 1:
            sys.stderr.write(f"[WARN] Duplicate bundle population_hash {pop_hash}: {', '.join(sorted(segs))}\n")

    manifest_path = out_dir / "segment_manifest.csv"
    registry_path = out_dir / "run_registry.csv"
    membership_path = out_dir / "segment_membership.csv"

    existing_registry_rows: List[Dict[str, str]] | None = None
    if registry_path.is_file():
        _, existing_registry_rows = _read_csv(registry_path)

    existing_membership: Dict[str, List[str]] | None = None
    if membership_path.is_file():
        _, existing_membership_rows = _read_csv(membership_path)
        existing_membership = _membership_by_segment(existing_membership_rows)

    registry_rows = _build_registry(
        manifest_rows,
        existing_registry=existing_registry_rows,
        existing_membership=existing_membership,
    )
    membership_rows = _build_membership_rows(manifest_rows)

    _atomic_write_csv(manifest_path, MANIFEST_FIELDNAMES, manifest_rows)
    _atomic_write_csv(registry_path, REGISTRY_FIELDNAMES, registry_rows)
    _atomic_write_csv(membership_path, MEMBERSHIP_FIELDNAMES, membership_rows)

    _print_summary(manifest_path, registry_path, manifest_rows, min_files)
    return 0


if __name__ == "__main__":
    sys.exit(main())
