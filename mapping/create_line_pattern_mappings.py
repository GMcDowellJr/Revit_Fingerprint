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
# Repo-root resolution, sys.modules purging, sys.path promotion, and the
# RevitServices/RevitAPI assembly references are all handled by the shared
# mapping/_dynamo_bootstrap.py module (see its docstring for the failure modes
# each piece defends against). This script only carries the small, unavoidable
# loader shim needed to load that module before a repo root is known -- every
# future domain's entry point should reuse mapping/_dynamo_bootstrap.py the
# same way rather than re-deriving any of this.
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

import importlib.util
import os


def _load_bootstrap_module_from(candidate):
    module_path = os.path.join(os.path.abspath(str(candidate)), "mapping", "_dynamo_bootstrap.py")
    spec = importlib.util.spec_from_file_location("mapping._dynamo_bootstrap", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("no loadable module at {}".format(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_dynamo_bootstrap(explicit_repo_root):
    """Locate and load mapping/_dynamo_bootstrap.py directly from disk, without
    relying on sys.path -- this script may be pasted into a Dynamo Python
    Script node with no __file__, so the shared bootstrap module cannot be
    found via a normal package import until AFTER a repo root is known (see
    mapping/_dynamo_bootstrap.py's own docstring for why).

    A caller-supplied explicit_repo_root (IN[2]) is an explicit selection, not
    a hint: if mapping/_dynamo_bootstrap.py can't be loaded from exactly that
    path -- missing entirely, or present but raising on import (e.g. a stale/
    partially-updated checkout) -- that failure is propagated immediately
    rather than silently trying environment-variable or __file__ candidates
    instead. Falling back there would resolve a *different* checkout's
    bootstrap code than the one explicitly requested while still reporting
    IN[2] as the selected repo_root (resolve_repo_root() validates
    explicit_repo_root by path, not by which module instance is running) --
    exactly the "explicit-but-wrong falls through silently" failure mode
    resolve_repo_root() itself already guards against for a structurally
    invalid explicit root (see PR #442 review). Only non-explicit candidates
    (env vars, then __file__) get try-the-next-one-on-failure semantics,
    matching resolve_repo_root()'s own fallback order.
    """
    if explicit_repo_root:
        return _load_bootstrap_module_from(explicit_repo_root)

    candidates = []
    for env_key in ("REVIT_FINGERPRINT_REPO_ROOT_SELECTED", "REVIT_FINGERPRINT_REPO_DIR"):
        try:
            env_val = str(os.environ.get(env_key, "")).strip()
        except Exception:
            env_val = ""
        if env_val:
            candidates.append(env_val)
    try:
        candidates.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    except Exception:
        pass

    last_error = None
    for candidate in candidates:
        try:
            return _load_bootstrap_module_from(candidate)
        except Exception as ex:
            last_error = ex
            continue

    raise RuntimeError(
        "Could not locate/load mapping/_dynamo_bootstrap.py from any candidate repo root "
        "(REVIT_FINGERPRINT_REPO_ROOT_SELECTED/REVIT_FINGERPRINT_REPO_DIR, or __file__). "
        "Pass the checkout's absolute path as IN[2]. Last error: {}".format(last_error)
    )


_IN2 = IN[2] if len(IN) > 2 else None
_dynamo_bootstrap = _load_dynamo_bootstrap(_IN2)
_REPO_ROOT = _dynamo_bootstrap.bootstrap(explicit_repo_root=_IN2)

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
