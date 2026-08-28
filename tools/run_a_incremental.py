#!/usr/bin/env python3
"""Run A's incremental flatten -> sig_hash -> apply pipeline.

Replaces the three separately-invoked stage bodies
(`extractor.emit_records` / `run_extract_all._apply_sig_hash_to_phase0` /
`apply_join_policy.main`) with a single per-export-file pass, used only when
`run_extract_all.py` is invoked with `--incremental` and all three of
flatten/sig_hash/apply are selected (Run A's exact stage set:
`--stages sig_hash,flatten,apply,placeholders`).

Why one pass instead of three independently-gated stages: flatten's own rows
(bootstrap sig_hash/join_hash), sig_hash's derived sig_hash/status/
sig_basis_items, and apply's derived join_hash/join_key_status are each a pure
function of (that one record's own extracted items, the sig_hash policy, the
join policy) -- confirmed by tracing `core/sig_hash_builder.py` and
`tools/join_key_discovery/eval.py::build_identity_index` /
`build_candidate_join_key_with_details`: neither reads anything beyond the
items belonging to the record being hashed. There is no cross-record or
cross-file aggregation anywhere in this chain (contrast with
`placeholder_exclusions.py`'s population-wide threshold, which this module
never touches -- see the module docstring in tools/run_a_cache.py and
audit_results/audit_17_abc_reprocessing_scope_investigation.md section 6).
That means a file's *final* post-apply row set can be computed in one pass and
cached as a single unit per file, and is safe to reuse verbatim whenever that
file's content and both policy files are unchanged since it was last computed
(tools/run_a_cache.py owns that validity decision).

The line_patterns domain's synthetic `line_pattern.segments_norm_hash` item
(normally appended by `run_extract_all._append_line_pattern_synthetic_norm_hash`
as a separate corpus-wide shard-rewrite pass between flatten and sig_hash) is
folded into this same per-file pass instead, via the shared
`_line_pattern_synthetic_norm_hash_row` helper -- it is likewise a pure
per-record computation (grouped strictly by record_pk), so computing it inline
before sig_hash, per file, is equivalent to the corpus-wide pass it replaces.

Output artifacts and their exact on-disk schema (column names/types) are
unchanged from the non-incremental pipeline; only *when* a given file's rows
are (re)computed changes. `placeholders` (T2b) is not touched by this module
at all -- it always runs afterward, as a separate subprocess, over the
records.csv / file_metadata.csv this module just wrote for the FULL current
population (cached-reused rows unioned with freshly-computed rows), which is
exactly what its population-wide threshold requires (see module docstring in
tools/run_a_cache.py).
"""
from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from extractor import (  # noqa: E402
    _ITEM_FIELDS,
    _LABEL_FIELDS,
    _PARAM_FIELDS,
    _REASON_FIELDS,
    _RECORD_FIELDS,
    _file_id,
    _flatten_one_file,
    _get_tool_version,
    _iter_export_files,
    _load_governance_role_rules,
    _merge_meta_row,
    _read_existing_csv,
    _sort_rows,
    _utc_now_iso,
    _write_csv,
)
from run_extract_all import _line_pattern_synthetic_norm_hash_row  # noqa: E402
from join_key_discovery.eval import (  # noqa: E402
    build_candidate_join_key_with_details,
    build_identity_index,
    normalize_policy_block,
)
from join_key_derivation import md5_utf8_join_pipe, serialize_identity_items  # noqa: E402
from core.sig_hash_policy import get_domain_sig_hash_policy, load_sig_hash_policies  # noqa: E402
from core.sig_hash_builder import build_sig_hash_from_policy  # noqa: E402

import run_a_cache  # noqa: E402
from progress_reporter import ProgressReporter  # noqa: E402

# Matches `_apply_sig_hash_to_phase0`'s fieldname extension exactly: flatten's own
# _RECORD_FIELDS has no "status_reasons" column (status_reasons live in the separate
# status_reasons.csv file) -- the sig_hash stage is what appends "sig_basis_schema"
# and "status_reasons" as extra records.csv columns, in that order (confirmed against
# the non-incremental pipeline's `for extra in ("sig_hash", "sig_basis_schema",
# "status", "status_reasons")` loop; "sig_hash"/"status" are already present).
_RECORDS_CSV_FIELDS = list(_RECORD_FIELDS) + ["sig_basis_schema", "status_reasons"]
_SIG_BASIS_FIELDS = ["record_pk", "domain", "item_key", "ordinal"]
_JOIN_FAILURE_FIELDS = [
    "domain", "file_id", "record_pk", "reason", "missing_keys",
    "effective_required_keys", "discriminator_key", "discriminator_value",
    "policy_id", "policy_version",
]


def _items_map_for_domain(item_shard_rows: Dict[str, List[Dict[str, str]]]) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    """domain -> record_pk -> [{"k","v","q"}] built from _flatten_one_file's item rows."""
    out: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    for domain, rows in item_shard_rows.items():
        by_pk: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            pk = row.get("record_pk", "")
            by_pk.setdefault(pk, []).append({
                "k": row.get("item_key", ""),
                "v": row.get("item_value", ""),
                "q": row.get("item_value_type", ""),
            })
        out[domain] = by_pk
    return out


def _augment_line_pattern_items(item_shard_rows: Dict[str, List[Dict[str, str]]]) -> Dict[str, int]:
    """Append the synthetic segments_norm_hash item to each line_patterns record.

    Mutates item_shard_rows["line_patterns"] in place. See module docstring.
    """
    lp_rows = item_shard_rows.get("line_patterns")
    if not lp_rows:
        return {"total": 0, "ok": 0, "missing": 0}
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for r in lp_rows:
        grouped.setdefault(r.get("record_pk", ""), []).append(r)
    ok = 0
    missing = 0
    new_rows: List[Dict[str, str]] = []
    for record_pk, group in grouped.items():
        row, is_ok = _line_pattern_synthetic_norm_hash_row(
            record_pk, group, "item_key", "item_value_type", "item_value", "item_role"
        )
        if is_ok:
            ok += 1
        else:
            missing += 1
        new_rows.append({k: v for k, v in row.items() if k in _ITEM_FIELDS})
    lp_rows.extend(new_rows)
    return {"total": len(new_rows), "ok": ok, "missing": missing}


def _sig_hash_one_record(
    row: Dict[str, str],
    domain_policy: Optional[Dict[str, Any]],
    items: List[Dict[str, str]],
) -> Tuple[bool, List[Dict[str, str]], Dict[str, int]]:
    """Mutate `row` in place with sig_hash stage results for one record.

    Returns (was_processed, basis_rows, diag_delta). was_processed is False
    when the record's domain has no sig_hash policy entry -- exactly mirroring
    `_apply_sig_hash_to_phase0`'s `continue` on a missing policy, which leaves
    flatten's own bootstrap sig_hash/join_hash/status untouched for that
    domain.
    """
    diag = {"records_processed": 0, "records_hashed": 0, "records_blocked": 0, "records_degraded": 0, "basis_items_written": 0}
    if not isinstance(domain_policy, dict):
        return False, [], diag

    diag["records_processed"] = 1
    sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(
        domain_policy=domain_policy, items=items, status_reasons=[],
    )
    row["sig_hash"] = "" if sig_hash is None else str(sig_hash)
    if str(row.get("join_key_schema", "")) == "sig_hash_as_join_key.v1":
        row["join_hash"] = row["sig_hash"]

    prior_status = str(row.get("status", "")).strip()
    if prior_status == "blocked":
        prior_reasons = [r for r in str(row.get("status_reasons", "")).split("|") if r]
        apply_stage_blocked = any(r.startswith("identity.incomplete:required_not_ok:") for r in prior_reasons)
        if apply_stage_blocked:
            row["status"] = str(status)
            row["status_reasons"] = "|".join(reasons)
    else:
        row["status"] = str(status)
        row["status_reasons"] = "|".join(reasons)
    row["sig_basis_schema"] = str(domain_policy.get("sig_hash_schema") or "")

    basis_rows: List[Dict[str, str]] = []
    for ordinal, it in enumerate(hash_items):
        k = it.get("k")
        if isinstance(k, str) and k:
            basis_rows.append({"record_pk": row["record_pk"], "domain": row["domain"], "item_key": k, "ordinal": str(ordinal)})
            diag["basis_items_written"] += 1

    if sig_hash is not None:
        diag["records_hashed"] = 1
    if status == "blocked":
        diag["records_blocked"] = 1
    elif status == "degraded":
        diag["records_degraded"] = 1
    return True, basis_rows, diag


def _apply_one_domain_records(
    dom_records: List[Dict[str, str]],
    domain_policy_block: Optional[Dict[str, Any]],
    domain_items: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Mutate each record in `dom_records` in place with apply-stage join fields.

    Mirrors apply_join_policy.py's per-domain loop body exactly (including the
    missing_policy branch). Returns the failure rows for this domain's records.
    """
    failures: List[Dict[str, str]] = []
    if not isinstance(domain_policy_block, dict):
        for r in dom_records:
            record_pk = (r.get("record_pk") or "").strip()
            r["join_hash"] = r.get("join_hash", "")
            r["join_key_schema"] = "sig_hash_as_join_key.v1"
            r["join_key_status"] = "missing_policy"
            r["join_key_policy_id"] = ""
            r["join_key_policy_version"] = ""
            failures.append({
                "domain": r.get("domain", ""), "file_id": r.get("file_id", ""), "record_pk": record_pk,
                "reason": "missing_policy", "missing_keys": "", "effective_required_keys": "",
                "discriminator_key": "", "discriminator_value": "", "policy_id": "", "policy_version": "",
            })
        return failures

    identity_index = build_identity_index(domain_items)
    normalized = normalize_policy_block(domain_policy_block)
    selected_fields = normalized["selected_fields"]
    required_fields = normalized["required_fields"]

    for r in dom_records:
        record_pk = (r.get("record_pk") or "").strip()
        status, selected_items, reason, details = build_candidate_join_key_with_details(
            identity_index,
            record_pk,
            selected_fields,
            {
                "required_fields": required_fields,
                "optional_fields": normalized["optional_items"],
                "discriminator_key": normalized["gates"].get("discriminator_key"),
                "shape_requirements": normalized["gates"].get("shape_requirements"),
                "default_shape_behavior": normalized["gates"].get("default_shape_behavior"),
            },
        )
        policy_id = str(domain_policy_block.get("policy_id") or domain_policy_block.get("join_key_schema") or f"{r.get('domain','')}.join_key.v21")
        policy_version = str(domain_policy_block.get("policy_version") or "1")
        join_key_schema = str(domain_policy_block.get("join_key_schema") or f"policy.{policy_id}.v{policy_version}")
        r["join_key_policy_id"] = policy_id
        r["join_key_policy_version"] = policy_version
        r["join_key_schema"] = join_key_schema

        if status != "ok":
            r["join_hash"] = ""
            r["join_key_status"] = status
            failures.append({
                "domain": r.get("domain", ""), "file_id": r.get("file_id", ""), "record_pk": record_pk,
                "reason": status, "missing_keys": reason,
                "effective_required_keys": "|".join(details.get("effective_required_fields", [])),
                "discriminator_key": str(details.get("discriminator_key") or ""),
                "discriminator_value": str(details.get("discriminator_value") or ""),
                "policy_id": policy_id, "policy_version": policy_version,
            })
            continue

        preimage = serialize_identity_items(selected_items)
        r["join_hash"] = md5_utf8_join_pipe(preimage)
        r["join_key_status"] = "ok"
    return failures


def _validate_line_pattern_synthetic_norm_hash_for_file(
    file_id: str,
    records: List[Dict[str, str]],
    item_shard_rows: Dict[str, List[Dict[str, str]]],
) -> None:
    """Per-file equivalent of run_extract_all._validate_line_pattern_synthetic_norm_hash()
    / apply_join_policy.py's own line_patterns PK check.

    A line_patterns record with zero identity items in the export (or any other
    reason `_augment_line_pattern_items` couldn't produce its synthetic item)
    must hard-fail here, exactly like the non-incremental pipeline does --
    without this, such a record silently gets a blocked/missing sig_hash and a
    missing join_hash, and that degraded-but-"successful" result gets cached.
    """
    line_pattern_pks = [r["record_pk"] for r in records if r.get("domain") == "line_patterns"]
    if not line_pattern_pks:
        return
    pks_with_norm = {
        r.get("record_pk", "")
        for r in item_shard_rows.get("line_patterns", [])
        if r.get("item_key") == "line_pattern.segments_norm_hash"
    }
    missing = [pk for pk in line_pattern_pks if pk not in pks_with_norm]
    if missing:
        sample = ",".join(missing[:10])
        more = "" if len(missing) <= 10 else f" (+{len(missing) - 10} more)"
        raise SystemExit(
            "flatten/enrichment stage did not produce synthetic norm hashes before apply: "
            f"missing line_pattern.segments_norm_hash for {len(missing)} line_patterns records "
            f"in {file_id}. sample_record_pks={sample}{more}"
        )


def _compute_fresh_entry(
    primary: Path,
    secondary: Optional[Path],
    file_id_mode: str,
    tool_version: str,
    exported_utc: str,
    governance_rules: List[Dict[str, str]],
    sig_hash_policies: Dict[str, Any],
    join_policy_domains: Dict[str, Any],
    dom_filter: Optional[set],
) -> Tuple[Dict[str, Any], Dict[str, int], Dict[str, int]]:
    """Run flatten + line-pattern augmentation + sig_hash + apply for one file.

    `dom_filter`, when not None, mirrors `_apply_sig_hash_to_phase0`'s own
    `dom_filter` semantics: a domain outside it never gets a policy-driven
    sig_hash at all (its records keep flatten's bootstrap sig_hash/join_hash/
    status untouched), regardless of whether that domain actually has a policy
    entry. The apply/join stage has no equivalent filter in the non-incremental
    pipeline either, so it is intentionally NOT filtered here.

    Returns (cache_entry, sig_hash_diag_delta, line_pattern_stats_delta).
    """
    bundle = _flatten_one_file(primary, secondary, file_id_mode, tool_version, exported_utc, governance_rules)
    lp_stats = _augment_line_pattern_items(bundle["item_shard_rows"])
    _validate_line_pattern_synthetic_norm_hash_for_file(bundle["file_id"], bundle["records"], bundle["item_shard_rows"])
    items_by_domain_pk = _items_map_for_domain(bundle["item_shard_rows"])

    file_diag = {"records_processed": 0, "records_hashed": 0, "records_blocked": 0, "records_degraded": 0, "basis_items_written": 0}
    sig_basis_rows: List[Dict[str, str]] = []
    records_by_domain: Dict[str, List[Dict[str, str]]] = {}
    for row in bundle["records"]:
        row.setdefault("sig_basis_schema", "")
        domain = row["domain"]
        records_by_domain.setdefault(domain, []).append(row)
        if dom_filter is not None and domain not in dom_filter:
            continue  # sig_hash stage never considers this domain at all -- matches _apply_sig_hash_to_phase0
        pol = get_domain_sig_hash_policy(sig_hash_policies, domain)
        items = items_by_domain_pk.get(domain, {}).get(row["record_pk"], [])
        _was_processed, basis_rows, delta = _sig_hash_one_record(row, pol, items)
        sig_basis_rows.extend(basis_rows)
        for k in file_diag:
            file_diag[k] += delta[k]

    join_failure_rows: List[Dict[str, str]] = []
    for domain, dom_records in records_by_domain.items():
        pol_block = join_policy_domains.get(domain)
        domain_items = bundle["item_shard_rows"].get(domain, [])
        join_failure_rows.extend(_apply_one_domain_records(dom_records, pol_block, domain_items))

    entry = {
        "meta_core": bundle["meta_core"],
        "records": bundle["records"],
        "label_rows": bundle["label_rows"],
        "reason_rows": bundle["reason_rows"],
        "param_rows": bundle["param_rows"],
        "item_shard_rows": bundle["item_shard_rows"],
        "sig_basis_rows": sig_basis_rows,
        "join_failure_rows": join_failure_rows,
    }
    return entry, file_diag, lp_stats


def run_incremental(
    exports_dir: Path,
    out_dir: Path,
    *,
    cache_dir: Path,
    sig_hash_policy_path: Optional[Path],
    join_policy_path: Optional[Path],
    file_id_mode: str = "basename",
    force_full: bool = False,
    sig_hash_domains: Optional[List[str]] = None,
    progress: Optional[ProgressReporter] = None,
) -> Dict[str, Any]:
    """Run Run A's flatten+sig_hash+apply as one cache-aware pass.

    Writes records.csv, label_components.csv, status_reasons.csv,
    parameter_rows.csv, identity_items_by_domain/*.csv (+ .complete sentinel),
    file_metadata.csv, sig_basis_items.csv, and
    {out_dir.parent}/diagnostics/join_policy_failures.csv -- the exact same
    artifact set as flatten+sig_hash+apply combined. Does NOT finalize the Run
    A cache manifest; callers must call `run_a_cache.finalize_manifest(state)`
    themselves once every downstream stage they care about (placeholders, in
    Run A's case) has also completed successfully.

    `sig_hash_domains`, when given, is the same domain filter the
    non-incremental sig_hash stage receives (`active_domains or domains` in
    run_extract_all.py) -- a domain outside it is left entirely untouched by
    sig_hash (bootstrap values from flatten persist). None/empty means
    unfiltered, matching `_apply_sig_hash_to_phase0`'s own `dom_filter`
    semantics. Participates in cache validity (tools/run_a_cache.py) so a
    changed filter forces a full recompute instead of reusing entries computed
    under a different filter.
    """
    started = time.perf_counter()
    progress = progress or ProgressReporter()
    progress.start_heartbeat()
    progress.event("startup and export discovery started", phase="discovery_setup")
    timings = {k: 0.0 for k in ("discovery_setup", "source_signature_scan", "cache_entry_reads", "fresh_parse_flatten_signature_join", "cache_entry_writes", "consolidated_csv_writes_and_finalization")}
    dom_filter = set(sig_hash_domains) if sig_hash_domains else None

    export_files = _iter_export_files(exports_dir)
    file_id_to_paths: Dict[str, List[Path]] = {}
    for _, primary, secondary in export_files:
        fid = _file_id(primary, file_id_mode)
        file_id_to_paths[fid] = [primary] + ([secondary] if secondary is not None else [])

    sig_hash_policies = load_sig_hash_policies(str(sig_hash_policy_path)) if sig_hash_policy_path else {"domains": {}}
    join_policy_domains: Dict[str, Any] = {}
    if join_policy_path is not None:
        join_policy_path = Path(join_policy_path)
        if not join_policy_path.is_file():
            raise SystemExit(f"Join policy not found: {join_policy_path}")
        join_policy_raw = json.loads(join_policy_path.read_text(encoding="utf-8"))
        # Matches apply_join_policy.py's own validation exactly: a malformed
        # policy (missing/non-dict "domains") must hard-fail here too, not
        # silently degrade to "every domain has no policy" -- that would mark
        # every record join_key_status=missing_policy and let a corrupt policy
        # file get cached as a "successful" run.
        join_policy_domains = join_policy_raw.get("domains") if isinstance(join_policy_raw, dict) else None
        if not isinstance(join_policy_domains, dict):
            raise SystemExit("Invalid policy format: missing domains")

    tool_version = _get_tool_version()
    progress.event("cache setup complete", phase="discovery_setup", selected_exports=len(export_files), tool_version=tool_version)
    if tool_version == "0.0.0+nogit":
        print("[WARN run-a] tool_version=0.0.0+nogit weakens code-change cache invalidation", file=sys.stderr, flush=True)
    timings["discovery_setup"] = time.perf_counter() - started
    scan_started = time.perf_counter()
    state = run_a_cache.compute_run_state(
        cache_dir=cache_dir,
        file_id_to_paths=file_id_to_paths,
        sig_hash_policy_path=sig_hash_policy_path,
        join_policy_path=join_policy_path,
        tool_version=tool_version,
        sig_hash_domains=sig_hash_domains,
        force_full=force_full,
        progress=progress,
    )
    timings["source_signature_scan"] = time.perf_counter() - scan_started
    progress.event("cache validity known", phase="cache_setup", cache_was_valid=state.cache_was_valid,
                   invalidation_reason=state.invalidation_reason or "none", reuse_candidates=len(state.reuse_candidate_file_ids))

    exported_utc = _utc_now_iso()
    governance_rules = _load_governance_role_rules()

    annotation_columns = [
        "client_label", "governance_role", "discipline_label", "project_label",
        "business_center_label", "collection_label",
    ]
    existing_annotations: Dict[str, Dict[str, str]] = {}
    existing_meta_path = out_dir / "file_metadata.csv"
    if existing_meta_path.exists():
        for _ar in _read_existing_csv(existing_meta_path):
            eid = _ar.get("export_run_id", "").strip()
            if eid:
                preserved = {col: _ar.get(col, "").strip() for col in annotation_columns}
                preserved["unit_system"] = _ar.get("unit_system", "").strip()
                if any(v for v in preserved.values()):
                    existing_annotations[eid] = preserved

    # Every artifact below is written incrementally, one export file's rows at a
    # time, and a processed file's `entry` (records/labels/item-shard rows/etc.)
    # is discarded once its rows are streamed out -- so peak memory is bounded by
    # one file's data, not the whole corpus, whether that file's rows come from
    # cache or a fresh recompute. This preserves the same bound `emit_records()`
    # already has (see its own docstring) instead of quietly discarding it; a
    # zero-change run over a large corpus (the case this cache mainly targets)
    # must not have to hold every record/label/reason/param/identity-item row in
    # memory just to confirm nothing changed.
    out_dir.mkdir(parents=True, exist_ok=True)
    shard_dir = out_dir / "identity_items_by_domain"
    shard_dir.mkdir(parents=True, exist_ok=True)
    for stale in shard_dir.glob("*.csv"):
        stale.unlink(missing_ok=True)
    (shard_dir / ".complete").unlink(missing_ok=True)
    item_shard_handles: Dict[str, Any] = {}
    item_shard_writers: Dict[str, Any] = {}

    diag_dir = out_dir.parent / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    _streaming_stems = ["records", "label_components", "status_reasons", "parameter_rows", "sig_basis_items"]
    _tmp: Dict[str, Path] = {s: out_dir / f"{s}.csv.tmp" for s in _streaming_stems}
    _join_failures_tmp = diag_dir / "join_policy_failures.csv.tmp"

    meta_rows: List[Dict[str, str]] = []  # one row per file -- stays in memory, matches emit_records()
    files_reused = 0
    files_recomputed = 0
    record_count = 0
    total_diag = {"records_processed": 0, "records_hashed": 0, "records_blocked": 0, "records_degraded": 0, "basis_items_written": 0}
    lp_total = {"total": 0, "ok": 0, "missing": 0}
    active_domains: set = set()
    entry_loads = 0
    fallback_reasons: Dict[str, int] = {}
    basis_rows_written = 0
    csv_started = time.perf_counter()

    with (
        _tmp["records"].open("w", newline="", encoding="utf-8") as _rec_f,
        _tmp["label_components"].open("w", newline="", encoding="utf-8") as _lbl_f,
        _tmp["status_reasons"].open("w", newline="", encoding="utf-8") as _rsn_f,
        _tmp["parameter_rows"].open("w", newline="", encoding="utf-8") as _par_f,
        _tmp["sig_basis_items"].open("w", newline="", encoding="utf-8") as _basis_f,
        _join_failures_tmp.open("w", newline="", encoding="utf-8") as _fail_f,
    ):
        _rec_w = csv.DictWriter(_rec_f, fieldnames=_RECORDS_CSV_FIELDS)
        _lbl_w = csv.DictWriter(_lbl_f, fieldnames=_LABEL_FIELDS)
        _rsn_w = csv.DictWriter(_rsn_f, fieldnames=_REASON_FIELDS)
        _par_w = csv.DictWriter(_par_f, fieldnames=_PARAM_FIELDS)
        _basis_w = csv.DictWriter(_basis_f, fieldnames=_SIG_BASIS_FIELDS)
        _fail_w = csv.DictWriter(_fail_f, fieldnames=_JOIN_FAILURE_FIELDS)
        for _w in (_rec_w, _lbl_w, _rsn_w, _par_w, _basis_w, _fail_w):
            _w.writeheader()

        for completed, (_, primary, secondary) in enumerate(export_files):
            file_id = _file_id(primary, file_id_mode)
            entry: Optional[Dict[str, Any]] = None
            if file_id in state.reuse_candidate_file_ids:
                progress.update(phase="candidate_cache_load", current=file_id, completed=completed, total=len(export_files), reused=files_reused, recomputed=files_recomputed, records=record_count, basis_rows=basis_rows_written)
                t = time.perf_counter(); entry, reason = run_a_cache.load_entry_diagnostic(cache_dir, file_id)
                timings["cache_entry_reads"] += time.perf_counter() - t; entry_loads += 1
                if entry is None:
                    fallback_reasons[reason] = fallback_reasons.get(reason, 0) + 1
                    print(f"[WARN run-a] cache reuse fallback file={file_id} reason={reason}", file=sys.stderr, flush=True)
            if entry is not None:
                files_reused += 1
                for k in total_diag:
                    total_diag[k] += entry.get("sig_hash_diag", {}).get(k, 0)
                lp = entry.get("line_pattern_stats", {"total": 0, "ok": 0, "missing": 0})
                for k in lp_total:
                    lp_total[k] += lp.get(k, 0)
            else:
                files_recomputed += 1
                progress.update(phase="fresh_parse_flatten_signature_join", current=file_id, completed=completed, total=len(export_files), reused=files_reused, recomputed=files_recomputed)
                t = time.perf_counter()
                entry, file_diag, lp_stats = _compute_fresh_entry(
                    primary, secondary, file_id_mode, tool_version, exported_utc,
                    governance_rules, sig_hash_policies, join_policy_domains, dom_filter,
                )
                timings["fresh_parse_flatten_signature_join"] += time.perf_counter() - t
                entry["sig_hash_diag"] = file_diag
                entry["line_pattern_stats"] = lp_stats
                for k in total_diag:
                    total_diag[k] += file_diag[k]
                for k in lp_total:
                    lp_total[k] += lp_stats[k]
                progress.update(phase="cache_entry_write", current=file_id)
                t = time.perf_counter(); run_a_cache.save_entry(cache_dir, file_id, entry)
                timings["cache_entry_writes"] += time.perf_counter() - t

            meta_row = _merge_meta_row(entry["meta_core"], existing_annotations, annotation_columns, governance_rules)
            meta_rows.append(meta_row)

            for row in entry["records"]:
                _rec_w.writerow(row)
                active_domains.add(row["domain"])
                record_count += 1
            for row in entry["label_rows"]:
                _lbl_w.writerow(row)
            for row in entry["reason_rows"]:
                _rsn_w.writerow(row)
            for row in entry["param_rows"]:
                _par_w.writerow(row)
            for domain, rows in entry["item_shard_rows"].items():
                if domain not in item_shard_writers:
                    _fp = (shard_dir / f"{domain}.csv").open("w", newline="", encoding="utf-8")
                    item_shard_handles[domain] = _fp
                    _shard_w = csv.DictWriter(_fp, fieldnames=_ITEM_FIELDS)
                    _shard_w.writeheader()
                    item_shard_writers[domain] = _shard_w
                for row in rows:
                    item_shard_writers[domain].writerow(row)
            for row in entry["sig_basis_rows"]:
                _basis_w.writerow(row)
                basis_rows_written += 1
            for row in entry["join_failure_rows"]:
                _fail_w.writerow(row)
            # `entry` (and any large per-domain row lists it held) is now free to
            # be garbage-collected before the next export file is processed.
            progress.update(phase="consolidated_csv_output", current=file_id, completed=completed + 1,
                            total=len(export_files), reused=files_reused, recomputed=files_recomputed,
                            records=record_count, basis_rows=basis_rows_written)
            entry = None

    for _fp in item_shard_handles.values():
        _fp.close()
    item_shard_handles.clear()
    item_shard_writers.clear()
    # Sentinel content is never parsed by any reader (existence-only gate), so a
    # wall-clock timestamp is sufficient -- matches extractor.emit_records().
    (shard_dir / ".complete").write_text(str(time.time()), encoding="utf-8")
    (out_dir / "identity_items.csv").unlink(missing_ok=True)

    _write_csv(
        out_dir / "file_metadata.csv",
        [
            "schema_version", "export_run_id", "file_id", "project_id", "model_id",
            "project_label", "model_label", "central_path", "central_path_norm",
            "lineage_hash", "revit_version_number", "revit_version_name", "revit_build",
            "is_workshared", "tool_version", "exported_utc",
            "client_label", "governance_role", "unit_system", "discipline_label",
            "business_center_label", "collection_label",
        ],
        _sort_rows(meta_rows, ["export_run_id"]),
    )

    # Files are promoted to their final names only after every writer above
    # succeeds, matching extractor.emit_records()'s own .tmp-then-replace
    # convention -- a mid-loop failure leaves the previous complete output
    # intact instead of a half-written records.csv.
    progress.event("output promotion started", phase="output_finalization")
    for stem in _streaming_stems:
        _tmp[stem].replace(out_dir / f"{stem}.csv")
    _join_failures_tmp.replace(diag_dir / "join_policy_failures.csv")

    # sig_basis_items.csv and join_policy_failures.csv are no longer sorted
    # corpus-wide (that required materializing every row in memory first) --
    # rows are written in file-iteration order instead. Neither file's row
    # order carries semantic meaning to any downstream reader (both are
    # per-record traceability/audit tables, not order-sensitive structures).
    # Only domains the sig_hash stage actually considered (i.e. within
    # dom_filter, or every domain when unfiltered) can be "without policy" --
    # a filtered-out domain was never looked up at all, matching
    # _apply_sig_hash_to_phase0's `continue`-before-considering-policy behavior.
    sig_hash_considered_domains = active_domains if dom_filter is None else (active_domains & dom_filter)
    domains_without_policy = sorted(d for d in sig_hash_considered_domains if not isinstance(get_domain_sig_hash_policy(sig_hash_policies, d), dict))
    # Matches _apply_sig_hash_to_phase0's diagnostics contract exactly:
    # "files_processed" counts sig_hash *stage invocations* (always 1 per run, not
    # export file count), and "sig_basis_items_written" is present only when > 0.
    sig_hash_diag = {
        "policy_path": str(sig_hash_policy_path) if sig_hash_policy_path else "",
        "files_processed": 1,
        "records_processed": total_diag["records_processed"],
        "records_hashed": total_diag["records_hashed"],
        "records_blocked": total_diag["records_blocked"],
        "records_degraded": total_diag["records_degraded"],
        "domains_without_policy": domains_without_policy,
    }
    if total_diag["basis_items_written"]:
        sig_hash_diag["sig_basis_items_written"] = total_diag["basis_items_written"]

    timings["consolidated_csv_writes_and_finalization"] = time.perf_counter() - csv_started
    total_seconds = time.perf_counter() - started
    progress.close()
    progress.event("incremental processing complete", phase="complete", files=len(export_files), reused=files_reused,
                   recomputed=files_recomputed, records=record_count, basis_rows=basis_rows_written)
    return {
        "files_total": len(export_files),
        "files_reused": files_reused,
        "files_recomputed": files_recomputed,
        "record_count": record_count,
        "sig_hash_diag": sig_hash_diag,
        "line_pattern_stats": lp_total,
        "cache_was_valid": state.cache_was_valid,
        "cache_invalidation_reason": state.invalidation_reason,
        "run_state": state,
        "performance": {
            "schema_version": 1, "timing_scope": "phase durations are inclusive within the combined incremental total; fresh parse/flatten/signature/join is intentionally combined",
            "phase_seconds": timings, "combined_incremental_seconds": total_seconds,
            "cache_entry_loads": entry_loads, "reuse_candidates": len(state.reuse_candidate_file_ids),
            "fallback_reasons": fallback_reasons, "source_files_hashed": state.source_files_hashed,
            "source_bytes_hashed": state.source_bytes_hashed, "records_emitted": record_count,
            "basis_rows_emitted": basis_rows_written, "effective_tool_version": tool_version,
            "cache_invalidation_reason": state.invalidation_reason,
        },
    }
