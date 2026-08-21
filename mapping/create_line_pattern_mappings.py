# Dynamo Python (Revit, CPython3) -- line_patterns mapping utility entry point.
#
# Creates representative LinePatternElement objects in the currently open Revit
# document from the CSV outputs of tools/export_bundle_pattern_detail.py
# (bundle_pattern_inventory.csv / pattern_settings.csv / pattern_names.csv),
# for use in a mapping/configuration RVT consumed by downstream governance
# tooling. See docs/line_pattern_mapping.md for the full design note and
# docs/line_pattern_mapping_verification.md for the manual verification
# procedure this script's own post-creation checks automate.
#
# This script only WRITES LinePatternElement objects (via bounded, individually
# verified Transactions -- see mapping/line_pattern_revit_apply.py). It never
# opens, saves, or closes any RVT; it operates on whatever document is already
# open, and the caller is responsible for saving it afterward.
#
# Inputs:
#   IN[0] input_dir (str)
#        Directory containing bundle_pattern_inventory.csv, pattern_settings.csv,
#        and pattern_names.csv (the output of
#        tools/export_bundle_pattern_detail.py for one segment).
#
#   IN[1] report_path (str)
#        Path to write the deterministic CSV report to (one row per requested
#        (domain="line_patterns", join_hash), plus one row per inventory entry
#        whose join_hash was blank -- see mapping/line_pattern_reconstruction.py's
#        REPORT_FIELDS).
#
#   IN[2] repo_root (str, optional)
#        Absolute path to the Revit_Fingerprint checkout, needed only when
#        __file__ can't be used to find it (see below) and neither
#        REVIT_FINGERPRINT_REPO_ROOT_SELECTED nor REVIT_FINGERPRINT_REPO_DIR is
#        set in the environment. Omit/None otherwise.
#
# Output:
#   OUT = {
#       "run_status": "ok" | "degraded" | "blocked",
#       "report_path": "<path>",
#       "rows_total": int,
#       "rows_ok": int,
#       "rows_degraded": int,
#       "rows_blocked": int,
#       "actions": {"existing": int, "created": int, "skipped": int, "blocked": int},
#   }

import os
import sys


def _looks_like_repo_root(p):
    try:
        base = os.path.abspath(str(p))
    except Exception:
        return False
    return all(
        os.path.exists(os.path.join(base, sub))
        for sub in ("mapping", "core", "domains")
    )


def _resolve_repo_root(explicit_repo_root):
    # A Dynamo Python Script node runs pasted code as a string (File "<string>"),
    # not as a loaded .py file -- __file__ is undefined in that case, unlike every
    # other module in this repo (which are always imported from disk). Mirrors
    # runner/run_dynamo.py's own env-var-first / __file__-fallback resolution,
    # plus an explicit IN[2] override for exactly this "pasted into a node" case,
    # where neither the env vars nor __file__ can be relied on.
    if explicit_repo_root and _looks_like_repo_root(explicit_repo_root):
        return os.path.abspath(explicit_repo_root)

    for env_key in ("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", "REVIT_FINGERPRINT_REPO_DIR"):
        try:
            env_val = str(os.environ.get(env_key, "")).strip()
        except Exception:
            env_val = ""
        if env_val and _looks_like_repo_root(env_val):
            return os.path.abspath(env_val)

    try:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        candidate = os.path.dirname(this_dir)
        if _looks_like_repo_root(candidate):
            return candidate
    except Exception:
        pass

    raise RuntimeError(
        "Could not determine the Revit_Fingerprint repo root: this script was run "
        "from pasted code (no __file__), and neither REVIT_FINGERPRINT_REPO_ROOT_SELECTED/"
        "REVIT_FINGERPRINT_REPO_DIR is set nor was IN[2] given. Pass the checkout's "
        "absolute path as IN[2]."
    )


_REPO_ROOT = _resolve_repo_root(IN[2] if len(IN) > 2 else None)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# A fresh Dynamo CPython3 Python Script node does not expose RevitServices/
# RevitAPI to pythonnet until these assemblies are explicitly referenced --
# same convention as tools/probes/*.py (e.g. probe_line_patterns.py) and
# runner/run_dynamo.py's own clr.AddReference("RevitServices") before
# importing DocumentManager. Must happen before importing DocumentManager
# below and before importing mapping.line_pattern_revit_apply (which imports
# Autodesk.Revit.DB symbols at module load time).
import clr
clr.AddReference("RevitServices")
clr.AddReference("RevitAPI")

from RevitServices.Persistence import DocumentManager

from mapping.line_pattern_reconstruction import (
    ACTION_SKIPPED,
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    MappingOutcome,
    build_report_rows,
    compute_run_status,
    get_line_patterns_join_key_policy,
    group_names_by_join_hash,
    group_requested_join_hashes,
    group_settings_by_join_hash,
    load_bundle_pattern_detail_export,
    reconstruct_pattern,
    write_report_csv,
)
from mapping.line_pattern_revit_apply import build_name_index, resolve_mapping


def run(doc, input_dir, report_path):
    export = load_bundle_pattern_detail_export(input_dir)
    requested, skipped = group_requested_join_hashes(export["inventory"])
    settings_by_jh = group_settings_by_join_hash(export["settings"])
    names_by_jh = group_names_by_join_hash(export["names"])

    domain_policy = get_line_patterns_join_key_policy()
    # Built once, then kept current in-place by resolve_mapping() as elements are
    # created, so join_hashes processed later in this same run see name
    # collisions against elements this run itself just created.
    name_index = build_name_index(doc)

    outcomes = []
    for join_hash in sorted(requested.keys()):
        request = requested[join_hash]
        reconstructed = reconstruct_pattern(join_hash, settings_by_jh.get(join_hash, []))
        name_rows = names_by_jh.get(join_hash, [])
        outcome = resolve_mapping(
            doc,
            join_hash,
            request,
            reconstructed,
            name_rows,
            name_index,
            domain_policy=domain_policy,
        )
        outcomes.append(outcome)

    for s in skipped:
        outcomes.append(
            MappingOutcome(
                join_hash="",
                segment_id=s.segment_id,
                action=ACTION_SKIPPED,
                status=STATUS_BLOCKED,
                reasons=[s.reason],
                bundle_ids=list(s.bundle_ids),
                pattern_ids=[s.pattern_id] if s.pattern_id else [],
            )
        )

    rows = build_report_rows(outcomes)
    write_report_csv(report_path, rows)
    run_status = compute_run_status(outcomes)

    actions = {"existing": 0, "created": 0, "skipped": 0, "blocked": 0}
    status_counts = {STATUS_OK: 0, STATUS_DEGRADED: 0, STATUS_BLOCKED: 0}
    for o in outcomes:
        actions[o.action] = actions.get(o.action, 0) + 1
        status_counts[o.status] = status_counts.get(o.status, 0) + 1

    return {
        "run_status": run_status,
        "report_path": report_path,
        "rows_total": len(outcomes),
        "rows_ok": status_counts[STATUS_OK],
        "rows_degraded": status_counts[STATUS_DEGRADED],
        "rows_blocked": status_counts[STATUS_BLOCKED],
        "actions": actions,
    }


doc = DocumentManager.Instance.CurrentDBDocument
input_dir = IN[0]
report_path = IN[1]

OUT = run(doc, input_dir, report_path)
