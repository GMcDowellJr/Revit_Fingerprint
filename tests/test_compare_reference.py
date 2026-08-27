# tests/test_compare_reference.py
#
# Regression tests for the segment-native tools/compare_reference.py.
#
# This tool implements no comparison mathematics of its own -- it resolves a
# reference/target filename against a segment's own already-materialized
# results/records/file_metadata.csv, then calls
# tools.bundle_analysis.step_compare.run_compare_for_domain directly
# (in-process, no subprocess) against the segment's own already-built
# results/bundle_analysis/{all,used}/<domain>/membership_matrix.csv. These
# tests build a minimal, realistic on-disk segment tree (corpus-level
# run_registry.csv + one segment's records/analysis/bundle_analysis outputs)
# and exercise the tool end-to-end via its own main(), since there is no
# subprocess boundary left to stub out.
#
# Use synthetic fixtures only. No Revit dependency.

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for _candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import compare_reference as cr  # noqa: E402
from tools.bundle_analysis.comparison_status import (  # noqa: E402
    COMPARISON_STATUS_OK,
    COMPARISON_STATUS_BLOCKED,
)
from tools.bundle_analysis.step_compare import run_compare_for_domain  # noqa: E402


def _write_csv(path: Path, fieldnames: Sequence[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


DOMAIN = "object_styles_model"
RUN_ID = "run1"


def _registry_row(segment_id: str, status: str = "complete") -> Dict[str, str]:
    return {
        "segment_id": segment_id, "parent_segment_id": "", "run_type": "bundle",
        "population_hash": "", "conformance_reference_mode": "", "output_folder": segment_id,
        "status": status, "last_run_utc": "", "notes": "", "segment_purpose": "", "segment_label": "",
    }


_REGISTRY_FIELDNAMES = [
    "segment_id", "parent_segment_id", "run_type", "population_hash",
    "conformance_reference_mode", "output_folder", "status", "last_run_utc",
    "notes", "segment_purpose", "segment_label",
]


def _build_segment(
    tmp_path: Path,
    presence: Dict[str, List[str]],
    segment_id: str = "seg_a",
    status: str = "complete",
    file_metadata_ids: Optional[List[str]] = None,
    extra_records_rows: Optional[List[Dict[str, str]]] = None,
    views: Sequence[str] = ("all", "used"),
    write_pattern_presence: bool = True,
    write_records: bool = True,
    write_file_metadata: bool = True,
    write_bundle_dirs: bool = True,
    join_key_schema: Optional[str] = "object_styles_model.join_key.v1",
    inconsistent_join_key_schema: bool = False,
    blank_join_key_schema: bool = False,
    partial_join_key_provenance: bool = False,
    other_registry_rows: Optional[List[Dict[str, str]]] = None,
    schema_version: str = "2.1.0",
) -> Dict[str, Path]:
    """Build a corpus-level run_registry.csv plus one segment's
    results/{records,analysis,bundle_analysis} tree.

    `presence` maps export_run_id -> list of pattern_id values it has in
    DOMAIN (an empty list means the file is a materialized member of the
    segment but has no evidence in DOMAIN at all).

    `other_registry_rows` lets a second (or later) call to this helper add
    its own segment's row to the *same* run_registry.csv alongside rows
    already written by prior calls, instead of each call overwriting the
    single corpus-level registry file with only its own segment's row --
    see _build_two_segments().
    """
    corpus_records_dir = tmp_path / "corpus" / "records"
    segments_root = tmp_path / "segments"
    seg_root = segments_root / segment_id
    seg_records = seg_root / "results" / "records"
    seg_analysis = seg_root / "results" / "analysis"
    seg_bundle = seg_root / "results" / "bundle_analysis"

    registry_file = corpus_records_dir / "run_registry.csv"
    registry_rows = list(other_registry_rows or []) + [_registry_row(segment_id, status)]
    _write_csv(registry_file, _REGISTRY_FIELDNAMES, registry_rows)

    all_ids = file_metadata_ids if file_metadata_ids is not None else sorted(presence.keys())
    if write_file_metadata:
        _write_csv(
            seg_records / "file_metadata.csv",
            ["export_run_id", "central_path", "governance_role"],
            [{"export_run_id": eid, "central_path": f"/x/{eid}", "governance_role": "Project"} for eid in all_ids],
        )

    if write_records:
        records_rows: List[Dict[str, str]] = []
        for idx, eid in enumerate(sorted(presence.keys())):
            schema = join_key_schema or ""
            policy_id, policy_version = "p1", "1"
            if inconsistent_join_key_schema and idx == 0:
                schema = "object_styles_model.join_key.v2"
            if blank_join_key_schema:
                schema, policy_id, policy_version = "", "", ""
            if partial_join_key_provenance:
                policy_id, policy_version = "", ""
            records_rows.append(
                {
                    "export_run_id": eid, "domain": DOMAIN,
                    "join_key_schema": schema, "join_key_policy_id": policy_id, "join_key_policy_version": policy_version,
                }
            )
        records_rows.extend(extra_records_rows or [])
        _write_csv(
            seg_records / "records.csv",
            ["export_run_id", "domain", "join_key_schema", "join_key_policy_id", "join_key_policy_version"],
            records_rows,
        )

    if write_pattern_presence:
        presence_rows: List[Dict[str, str]] = []
        membership_rows: List[Dict[str, str]] = []
        for eid, pattern_ids in presence.items():
            if not pattern_ids:
                continue
            for pid in pattern_ids:
                presence_rows.append(
                    {"analysis_run_id": RUN_ID, "domain": DOMAIN, "export_run_id": eid, "pattern_id": pid, "pattern_share_pct": "1.000000"}
                )
                membership_rows.append({"analysis_run_id": RUN_ID, "export_run_id": eid, "pattern_id": pid})
        _write_csv(
            seg_analysis / "pattern_presence_file.csv",
            ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"],
            presence_rows,
        )
        _write_csv(seg_analysis / "corpus_manifest.csv", ["schema_version"], [{"schema_version": schema_version}])
        if write_bundle_dirs:
            for view in views:
                _write_csv(
                    seg_bundle / view / DOMAIN / "membership_matrix.csv",
                    ["analysis_run_id", "export_run_id", "pattern_id"],
                    membership_rows,
                )

    return {
        "registry_file": registry_file,
        "segments_root": segments_root,
        "segment_root": seg_root,
        "registry_row": _registry_row(segment_id, status),
    }


def _build_two_segments(
    tmp_path: Path,
    presence_a: Dict[str, List[str]],
    presence_b: Dict[str, List[str]],
    segment_a: str = "seg_a",
    segment_b: str = "seg_b",
    **kwargs,
) -> Dict[str, Path]:
    """Build two independent segments (segment_a, segment_b) sharing one
    corpus-level run_registry.csv, for cross-segment tests. Any keyword also
    accepted by _build_segment (e.g. join_key_schema, inconsistent_join_key_schema)
    is forwarded to the *second* (segment_b) call only, so a test can vary
    segment_b's join-key provenance relative to segment_a's default.
    """
    ctx_a = _build_segment(tmp_path, presence_a, segment_id=segment_a)
    ctx_b = _build_segment(
        tmp_path, presence_b, segment_id=segment_b, other_registry_rows=[ctx_a["registry_row"]], **kwargs
    )
    return {
        "registry_file": ctx_b["registry_file"],
        "segments_root": ctx_b["segments_root"],
        "segment_a_root": ctx_a["segment_root"],
        "segment_b_root": ctx_b["segment_root"],
    }


def _build_multidomain_segment(
    tmp_path: Path,
    domain_presence: Dict[str, Dict[str, List[str]]],
    segment_id: str,
    domain_join_key: Optional[Dict[str, Tuple[str, str, str]]] = None,
    status: str = "complete",
    views: Sequence[str] = ("all", "used"),
    other_registry_rows: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Path]:
    """Build one segment spanning multiple domains, for cross-segment
    join-policy compatibility-gate tests where one domain's join-key
    provenance needs to differ between two segments while another domain's
    agrees. `domain_presence` maps domain -> {export_run_id: [pattern_id,...]}.
    `domain_join_key` maps domain -> (join_key_schema, join_key_policy_id,
    join_key_policy_version); a domain absent from it gets the default
    ("<domain>.join_key.v1", "p1", "1") -- matching _build_segment's own
    default for DOMAIN, so two segments built with no override agree.
    """
    domain_join_key = domain_join_key or {}
    corpus_records_dir = tmp_path / "corpus" / "records"
    segments_root = tmp_path / "segments"
    seg_root = segments_root / segment_id
    seg_records = seg_root / "results" / "records"
    seg_analysis = seg_root / "results" / "analysis"
    seg_bundle = seg_root / "results" / "bundle_analysis"

    registry_file = corpus_records_dir / "run_registry.csv"
    registry_rows = list(other_registry_rows or []) + [_registry_row(segment_id, status)]
    _write_csv(registry_file, _REGISTRY_FIELDNAMES, registry_rows)

    all_export_ids = sorted({eid for presence in domain_presence.values() for eid in presence.keys()})
    _write_csv(
        seg_records / "file_metadata.csv",
        ["export_run_id", "central_path", "governance_role"],
        [{"export_run_id": eid, "central_path": f"/x/{eid}", "governance_role": "Project"} for eid in all_export_ids],
    )

    records_rows: List[Dict[str, str]] = []
    presence_rows: List[Dict[str, str]] = []
    membership_rows_by_domain: Dict[str, List[Dict[str, str]]] = {}
    for domain, presence in domain_presence.items():
        schema, policy_id, policy_version = domain_join_key.get(domain, (f"{domain}.join_key.v1", "p1", "1"))
        for eid in sorted(presence.keys()):
            records_rows.append(
                {
                    "export_run_id": eid, "domain": domain,
                    "join_key_schema": schema, "join_key_policy_id": policy_id, "join_key_policy_version": policy_version,
                }
            )
        domain_membership: List[Dict[str, str]] = []
        for eid, pattern_ids in presence.items():
            for pid in pattern_ids:
                presence_rows.append(
                    {
                        "analysis_run_id": RUN_ID, "domain": domain, "export_run_id": eid,
                        "pattern_id": pid, "pattern_share_pct": "1.000000",
                    }
                )
                domain_membership.append({"analysis_run_id": RUN_ID, "export_run_id": eid, "pattern_id": pid})
        membership_rows_by_domain[domain] = domain_membership

    _write_csv(
        seg_records / "records.csv",
        ["export_run_id", "domain", "join_key_schema", "join_key_policy_id", "join_key_policy_version"],
        records_rows,
    )
    _write_csv(
        seg_analysis / "pattern_presence_file.csv",
        ["analysis_run_id", "domain", "export_run_id", "pattern_id", "pattern_share_pct"],
        presence_rows,
    )
    _write_csv(seg_analysis / "corpus_manifest.csv", ["schema_version"], [{"schema_version": "2.1.0"}])
    for view in views:
        for domain, membership_rows in membership_rows_by_domain.items():
            _write_csv(
                seg_bundle / view / domain / "membership_matrix.csv",
                ["analysis_run_id", "export_run_id", "pattern_id"],
                membership_rows,
            )

    return {
        "registry_file": registry_file,
        "segments_root": segments_root,
        "segment_root": seg_root,
        "registry_row": _registry_row(segment_id, status),
    }


def _run(tmp_path, ctx, out_name="out", **extra) -> int:
    out_dir = tmp_path / out_name
    argv = [
        "--segments-root", str(ctx["segments_root"]),
        "--registry-file", str(ctx["registry_file"]),
        "--reference-segment", extra.pop("reference_segment", extra.pop("segment", "seg_a")),
        "--reference", extra.pop("reference", "ref.json"),
        "--out-dir", str(out_dir),
    ]
    if "target_segment" in extra:
        argv += ["--target-segment", extra.pop("target_segment")]
    if "target" in extra:
        argv += ["--target", extra.pop("target")]
    if "purge_view" in extra:
        argv += ["--purge-view", extra.pop("purge_view")]
    if "domains" in extra:
        argv += ["--domains", extra.pop("domains")]
    rc = cr.main(argv)
    return rc, out_dir


# ---------------------------------------------------------------------------
# 1/3. Reference vs one target, both materialized -- all and used.
# ---------------------------------------------------------------------------


def test_reference_vs_one_target_both_views(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A", "B"], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, target="target.json")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    views = {r["purge_view"] for r in summary}
    assert views == {"all", "used"}
    for row in summary:
        assert row["target_export_run_id"] == "target.json"
        assert row["comparison_status"] == COMPARISON_STATUS_OK
        assert row["shared_count"] == "1"
        assert row["reference_only_count"] == "1"
        assert row["target_only_count"] == "0"


def test_purge_view_single_selection(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="used")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert {r["purge_view"] for r in summary} == {"used"}


# ---------------------------------------------------------------------------
# 2. Reference vs complete segment.
# ---------------------------------------------------------------------------


def test_reference_vs_complete_segment(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "target_1.json": ["A"], "target_2.json": ["B"]},
    )
    rc, out_dir = _run(tmp_path, ctx, purge_view="all")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    targets = {r["target_export_run_id"] for r in summary}
    assert targets == {"target_1.json", "target_2.json"}


# ---------------------------------------------------------------------------
# 4/5. Reference filename resolution: zero matches / ambiguous.
# ---------------------------------------------------------------------------


def test_reference_zero_matches_is_blocked(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, reference="nonexistent.json", target="target.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert diag["run_comparison_reason_codes"] == [cr.REASON_REFERENCE_NOT_MATERIALIZED]
    assert _read_csv(out_dir / cr.SUMMARY_FILENAME) == []


def test_reference_ambiguous_is_blocked(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"dup.json": ["A"], "dup.index.json": ["A"], "target.json": ["A"]},
        file_metadata_ids=["dup.json", "dup.index.json", "target.json"],
    )
    rc, out_dir = _run(tmp_path, ctx, reference="dup.details.json", target="target.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_REFERENCE_AMBIGUOUS]


# ---------------------------------------------------------------------------
# 6/7. Target filename resolution: zero matches / ambiguous.
# ---------------------------------------------------------------------------


def test_target_zero_matches_is_blocked(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, target="missing.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_TARGET_NOT_MATERIALIZED]


def test_target_ambiguous_is_blocked(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "dup.json": ["A"], "dup.index.json": ["A"]},
        file_metadata_ids=["ref.json", "dup.json", "dup.index.json"],
    )
    rc, out_dir = _run(tmp_path, ctx, target="dup.details.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_TARGET_AMBIGUOUS]


# ---------------------------------------------------------------------------
# 8. Incomplete segment materialization -> blocked.
# ---------------------------------------------------------------------------


def test_incomplete_segment_is_blocked(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]}, status="failed")
    rc, out_dir = _run(tmp_path, ctx, target="target.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_SEGMENT_MATERIALIZATION_INCOMPLETE]


# ---------------------------------------------------------------------------
# 9. Required analysis artifact missing (despite status=complete) -> blocked.
# ---------------------------------------------------------------------------


def test_missing_pattern_presence_file_is_blocked(tmp_path):
    ctx = _build_segment(
        tmp_path, {"ref.json": ["A"], "target.json": ["A"]}, write_pattern_presence=False
    )
    rc, out_dir = _run(tmp_path, ctx, target="target.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING]


def test_missing_bundle_view_dir_is_blocked(tmp_path):
    ctx = _build_segment(
        tmp_path, {"ref.json": ["A"], "target.json": ["A"]}, views=("all",), write_bundle_dirs=True
    )
    # "used" view directory was never created.
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="both")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_REQUIRED_ANALYSIS_ARTIFACT_MISSING]


# ---------------------------------------------------------------------------
# 10/11. Materialization compatibility: incompatible / unproven.
# ---------------------------------------------------------------------------


def test_incompatible_join_key_schema_blocks_domain(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "target.json": ["A"]},
        inconsistent_join_key_schema=True,
    )
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="all")
    assert rc == 0  # the run completes; the affected domain reports blocked
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert len(summary) == 1
    row = summary[0]
    assert row["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert row["comparison_reason_codes"] == cr.REASON_MATERIALIZATION_VERSION_INCOMPATIBLE
    assert row["shared_count"] == ""  # blocked rows never fabricate a zero


def test_unproven_join_key_schema_blocks_domain(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "target.json": ["A"]},
        blank_join_key_schema=True,
    )
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="all")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert summary[0]["comparison_reason_codes"] == cr.REASON_MATERIALIZATION_COMPATIBILITY_UNPROVEN


def test_partially_populated_tuple_is_unproven_not_ok(tmp_path):
    # join_key_schema is populated but join_key_policy_id/version are blank
    # for every record -- no *complete* tuple exists, so this must not be
    # accepted as a legitimate compatibility signal (Codex review, PR #467).
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "target.json": ["A"]},
        partial_join_key_provenance=True,
    )
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="all")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert summary[0]["comparison_reason_codes"] == cr.REASON_MATERIALIZATION_COMPATIBILITY_UNPROVEN


def test_incomplete_record_alongside_complete_one_is_unproven(tmp_path):
    # One record has a fully-populated (join_key_schema, policy_id, version)
    # tuple; another record for the same domain has an entirely blank one.
    # The blank record must not be silently discarded in favor of the
    # populated one -- the domain as a whole is not provably compatible
    # (Codex review, PR #467).
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "target.json": ["A"]},
        extra_records_rows=[
            {"export_run_id": "target.json", "domain": DOMAIN, "join_key_schema": "", "join_key_policy_id": "", "join_key_policy_version": ""}
        ],
    )
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="all")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert summary[0]["comparison_reason_codes"] == cr.REASON_MATERIALIZATION_COMPATIBILITY_UNPROVEN


# ---------------------------------------------------------------------------
# Empty comparison target set (after excluding the reference) must block,
# never silently roll up to "ok" (Codex review, PR #467).
# ---------------------------------------------------------------------------


def test_target_resolving_to_reference_itself_is_blocked(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, reference="ref.json", target="ref.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert diag["run_comparison_reason_codes"] == [cr.REASON_NO_COMPARISON_TARGETS]
    assert _read_csv(out_dir / cr.SUMMARY_FILENAME) == []


def test_whole_segment_with_only_reference_file_is_blocked(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, reference="ref.json")  # no --target: whole-segment mode
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert diag["run_comparison_reason_codes"] == [cr.REASON_NO_COMPARISON_TARGETS]


# ---------------------------------------------------------------------------
# 12/13. No JSON export access; no run_extract_all.py invocation.
# ---------------------------------------------------------------------------


def test_no_json_export_file_ever_created_or_needed(tmp_path):
    # No *.details.json / *.index.json / *__fingerprint.json file exists
    # anywhere under tmp_path -- only CSVs. A successful run proves the tool
    # never needed to open one.
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    assert not list(tmp_path.rglob("*.details.json"))
    assert not list(tmp_path.rglob("*.index.json"))
    assert not list(tmp_path.rglob("*__fingerprint.json"))
    rc, out_dir = _run(tmp_path, ctx, target="target.json")
    assert rc == 0
    assert not list(tmp_path.rglob("*.details.json"))
    assert not list(tmp_path.rglob("*.index.json"))


def test_module_never_shells_out_to_run_extract_all():
    import ast

    tree = ast.parse(Path(cr.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert not any(alias.name == "subprocess" for alias in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module != "subprocess"
    assert not hasattr(cr, "subprocess")


# ---------------------------------------------------------------------------
# 14. Deterministic output ordering.
# ---------------------------------------------------------------------------


def test_deterministic_output_ordering(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A", "B"], "target_z.json": ["A"], "target_a.json": ["B"]},
    )
    rc1, out1 = _run(tmp_path, ctx, out_name="out1")
    rc2, out2 = _run(tmp_path, ctx, out_name="out2")
    assert rc1 == 0 and rc2 == 0
    s1 = (out1 / cr.SUMMARY_FILENAME).read_text()
    s2 = (out2 / cr.SUMMARY_FILENAME).read_text()
    assert s1 == s2
    rows = _read_csv(out1 / cr.SUMMARY_FILENAME)
    targets_in_order = [r["target_export_run_id"] for r in rows if r["purge_view"] == "all"]
    assert targets_in_order == sorted(targets_in_order)


# ---------------------------------------------------------------------------
# 15. Reference-vs-segment does not self-compare.
# ---------------------------------------------------------------------------


def test_reference_excluded_from_segment_targets(tmp_path):
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A"], "target_1.json": ["A"], "target_2.json": ["B"]},
    )
    rc, out_dir = _run(tmp_path, ctx, purge_view="all")
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert "ref.json" not in {r["target_export_run_id"] for r in summary}


# ---------------------------------------------------------------------------
# A reference that is materialized but has no usable pattern evidence in any
# domain must block, not silently succeed as REFERENCE_DOMAIN_UNDEFINED
# everywhere (Codex review, PR #467).
# ---------------------------------------------------------------------------


def test_reference_with_no_patterns_anywhere_is_blocked(tmp_path):
    # "ref.json" is a materialized member of the segment (present in
    # file_metadata.csv) but has zero pattern_id presence rows in DOMAIN.
    ctx = _build_segment(tmp_path, {"ref.json": [], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, target="target.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_REFERENCE_HAS_NO_PATTERNS]
    assert _read_csv(out_dir / cr.SUMMARY_FILENAME) == []


# ---------------------------------------------------------------------------
# A `.legacy.json` selector must resolve through an exact match only, never
# via the suffix-stripping stem fallback (Codex review, PR #467).
# ---------------------------------------------------------------------------


def test_legacy_selector_does_not_fall_back_to_a_different_file(tmp_path):
    ctx = _build_segment(tmp_path, {"foo.json": ["A"], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, reference="foo.legacy.json", target="target.json")
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_reason_codes"] == [cr.REASON_REFERENCE_NOT_MATERIALIZED]


# ---------------------------------------------------------------------------
# A stale membership_matrix.csv (rows only for an old analysis_run_id) must
# block that domain/view, never silently compare as if the target has none
# of the reference's patterns (Codex review, PR #467).
# ---------------------------------------------------------------------------


def test_stale_membership_matrix_blocks_domain(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    stale_path = ctx["segment_root"] / "results" / "bundle_analysis" / "all" / DOMAIN / "membership_matrix.csv"
    _write_csv(
        stale_path,
        ["analysis_run_id", "export_run_id", "pattern_id"],
        [{"analysis_run_id": "old_run", "export_run_id": "ref.json", "pattern_id": "A"}],
    )
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="all")
    assert rc == 0  # domain/view-level block, not a process failure
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert len(summary) == 1
    assert summary[0]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert summary[0]["comparison_reason_codes"] == cr.REASON_STALE_MEMBERSHIP_MATRIX
    assert summary[0]["shared_count"] == ""

    # The "used" view (untouched, still current) must be unaffected.
    rc2, out_dir2 = _run(tmp_path, ctx, out_name="out2", target="target.json", purge_view="used")
    assert rc2 == 0
    summary2 = _read_csv(out_dir2 / cr.SUMMARY_FILENAME)
    assert summary2[0]["comparison_status"] == COMPARISON_STATUS_OK


# ---------------------------------------------------------------------------
# 16. Parity with directly calling the authoritative comparator.
# ---------------------------------------------------------------------------


def test_parity_with_direct_run_compare_for_domain(tmp_path):
    presence = {"ref.json": ["A", "B"], "target_1.json": ["A"], "target_2.json": ["B", "C"]}
    ctx = _build_segment(tmp_path, presence)

    rc, out_dir = _run(tmp_path, ctx, purge_view="all")
    assert rc == 0
    tool_rows = {r["target_export_run_id"]: r for r in _read_csv(out_dir / cr.SUMMARY_FILENAME) if r["purge_view"] == "all"}

    direct_out = tmp_path / "direct_compare"
    reference = {
        "reference_bundle_id": "irrelevant",
        "effective_date": "2026-01-01",
        "seed_export_run_id": "ref.json",
        "domains": {DOMAIN: ["A", "B"]},
    }
    direct_summary = run_compare_for_domain(
        ctx["segment_root"] / "results" / "analysis",
        ctx["segment_root"] / "results" / "bundle_analysis" / "all",
        reference,
        DOMAIN,
        compare_out_dir=direct_out,
    )
    direct_rows = {r["export_run_id"]: r for r in _read_csv(direct_out / "file_gap_report.csv")}

    assert set(tool_rows.keys()) == set(direct_rows.keys())
    for export_run_id, tool_row in tool_rows.items():
        direct_row = direct_rows[export_run_id]
        for field, direct_key in (
            ("reference_pattern_count", "reference_pattern_count"),
            ("target_pattern_count", "target_pattern_count"),
            ("shared_count", "shared_count"),
            ("reference_only_count", "reference_only_count"),
            ("target_only_count", "target_only_count"),
            ("union_count", "union_count"),
            ("reference_coverage_pct", "reference_coverage_pct"),
            ("jaccard", "jaccard"),
            ("comparison_status", "comparison_status"),
            ("comparison_reason_codes", "comparison_reason_codes"),
        ):
            assert tool_row[field] == direct_row[direct_key], f"{export_run_id}.{field}"
    assert direct_summary["comparison_status"] == COMPARISON_STATUS_OK


# ---------------------------------------------------------------------------
# Additional coverage: out-dir safety, overwrite semantics, domain filter.
# ---------------------------------------------------------------------------


def test_out_dir_inside_segments_root_refused(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    out_dir = ctx["segments_root"] / "seg_a"  # same as the segment root itself
    argv = [
        "--segments-root", str(ctx["segments_root"]),
        "--registry-file", str(ctx["registry_file"]),
        "--reference-segment", "seg_a",
        "--reference", "ref.json",
        "--target", "target.json",
        "--out-dir", str(out_dir),
    ]
    rc = cr.main(argv)
    assert rc == 2
    # Must not have been cleared/touched.
    assert (out_dir / "results" / "records" / "file_metadata.csv").is_file()


def test_out_dir_overwrite_required_for_foreign_directory(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    out_dir = tmp_path / "foreign"
    out_dir.mkdir()
    (out_dir / "unrelated.txt").write_text("keep me", encoding="utf-8")
    argv = [
        "--segments-root", str(ctx["segments_root"]),
        "--registry-file", str(ctx["registry_file"]),
        "--reference-segment", "seg_a",
        "--reference", "ref.json",
        "--target", "target.json",
        "--out-dir", str(out_dir),
    ]
    rc = cr.main(argv)
    assert rc == 2
    assert (out_dir / "unrelated.txt").is_file()

    rc = cr.main(argv + ["--overwrite"])
    assert rc == 0
    assert not (out_dir / "unrelated.txt").is_file()


def test_domains_filter_restricts_comparison(tmp_path):
    ctx = _build_segment(tmp_path, {"ref.json": ["A"], "target.json": ["A"]})
    rc, out_dir = _run(tmp_path, ctx, target="target.json", domains="other_domain", purge_view="all")
    # A requested domain this segment never observed at all blocks at the
    # domain level (no membership_matrix.csv / no compatibility signal) --
    # this is a row/domain-level blocked outcome, not a process failure, per
    # the existing "row/domain blocked -> exit 0, check diagnostics" convention.
    assert rc == 0
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_status"] == COMPARISON_STATUS_BLOCKED


# ---------------------------------------------------------------------------
# Cross-segment support: --reference-segment / --target-segment.
# ---------------------------------------------------------------------------


def test_same_segment_output_identical_omitted_vs_explicit_target_segment(tmp_path):
    # --target-segment omitted must produce byte-identical output to passing
    # --target-segment explicitly equal to --reference-segment -- the
    # regression requirement for this feature.
    ctx = _build_segment(
        tmp_path,
        {"ref.json": ["A", "B"], "target_1.json": ["A"], "target_2.json": ["B", "C"]},
    )
    rc1, out1 = _run(tmp_path, ctx, out_name="implicit", purge_view="all")
    rc2, out2 = _run(tmp_path, ctx, out_name="explicit", purge_view="all", target_segment="seg_a")
    assert rc1 == 0 and rc2 == 0
    assert (out1 / cr.SUMMARY_FILENAME).read_text() == (out2 / cr.SUMMARY_FILENAME).read_text()
    assert (out1 / cr.DETAIL_FILENAME).read_text() == (out2 / cr.DETAIL_FILENAME).read_text()


def test_cross_segment_matching_join_policy_compares_normally(tmp_path):
    ctx2 = _build_two_segments(
        tmp_path,
        presence_a={"ref.json": ["A", "B"]},
        presence_b={"target.json": ["A"]},
    )
    ctx = {"registry_file": ctx2["registry_file"], "segments_root": ctx2["segments_root"]}
    rc, out_dir = _run(
        tmp_path,
        ctx,
        reference_segment="seg_a",
        target_segment="seg_b",
        reference="ref.json",
        target="target.json",
        purge_view="all",
    )
    assert rc == 0
    summary = _read_csv(out_dir / cr.SUMMARY_FILENAME)
    assert len(summary) == 1
    row = summary[0]
    assert row["comparison_status"] == COMPARISON_STATUS_OK
    assert cr.REASON_CROSS_SEGMENT_JOIN_POLICY_MISMATCH not in row["comparison_reason_codes"]
    assert row["shared_count"] == "1"
    assert row["reference_only_count"] == "1"
    assert row["target_only_count"] == "0"


def test_cross_segment_differing_join_policy_blocks_only_that_domain(tmp_path):
    domain2 = "line_styles"
    presence_a = {DOMAIN: {"ref.json": ["A"]}, domain2: {"ref.json": ["X"]}}
    presence_b = {DOMAIN: {"target.json": ["A"]}, domain2: {"target.json": ["X"]}}

    ctx_a = _build_multidomain_segment(tmp_path, presence_a, segment_id="seg_a")
    ctx_b = _build_multidomain_segment(
        tmp_path,
        presence_b,
        segment_id="seg_b",
        other_registry_rows=[ctx_a["registry_row"]],
        # DOMAIN's join_key_schema disagrees with seg_a's default; domain2's
        # matches seg_a's default and must compare unaffected.
        domain_join_key={DOMAIN: ("object_styles_model.join_key.v2", "p1", "1")},
    )
    ctx = {"registry_file": ctx_b["registry_file"], "segments_root": ctx_b["segments_root"]}

    rc, out_dir = _run(
        tmp_path,
        ctx,
        reference_segment="seg_a",
        target_segment="seg_b",
        reference="ref.json",
        target="target.json",
        purge_view="all",
        domains=f"{DOMAIN},{domain2}",
    )
    assert rc == 0  # domain-level block, not a process failure
    summary = {r["domain"]: r for r in _read_csv(out_dir / cr.SUMMARY_FILENAME)}

    assert summary[DOMAIN]["comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert summary[DOMAIN]["comparison_reason_codes"] == cr.REASON_CROSS_SEGMENT_JOIN_POLICY_MISMATCH
    assert summary[DOMAIN]["shared_count"] == ""  # blocked rows never fabricate a zero

    assert summary[domain2]["comparison_status"] == COMPARISON_STATUS_OK
    assert summary[domain2]["shared_count"] == "1"


def test_cross_segment_differing_extractor_schema_version_blocks_whole_run(tmp_path):
    # Two segments share the same join-key tuple for DOMAIN (so the
    # CROSS_SEGMENT_JOIN_POLICY_MISMATCH gate alone would pass), but were
    # materialized under different extractor_schema_version values. Mirrors
    # reference_bundle.py::load_and_validate's own sidecar-schema rejection
    # (Codex review, PR #471) -- this must block the entire run rather than
    # silently reporting "ok" comparison metrics across incompatible pattern
    # evidence.
    ctx_a = _build_segment(tmp_path, {"ref.json": ["A", "B"]}, segment_id="seg_a", schema_version="2.1.0")
    ctx_b = _build_segment(
        tmp_path,
        {"target.json": ["A"]},
        segment_id="seg_b",
        other_registry_rows=[ctx_a["registry_row"]],
        schema_version="2.2.0",
    )
    ctx = {"registry_file": ctx_b["registry_file"], "segments_root": ctx_b["segments_root"]}

    rc, out_dir = _run(
        tmp_path,
        ctx,
        reference_segment="seg_a",
        target_segment="seg_b",
        reference="ref.json",
        target="target.json",
        purge_view="all",
    )
    assert rc == 2
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert diag["run_comparison_status"] == COMPARISON_STATUS_BLOCKED
    assert diag["run_comparison_reason_codes"] == [cr.REASON_CROSS_SEGMENT_SCHEMA_MISMATCH]
    assert _read_csv(out_dir / cr.SUMMARY_FILENAME) == []


def test_same_segment_schema_mismatch_check_never_runs(tmp_path):
    # The new cross-segment schema gate must never fire in same-segment mode
    # (schema_version is trivially self-consistent there anyway) -- part of
    # the byte-identical same-segment regression guarantee.
    ctx = _build_segment(tmp_path, {"ref.json": ["A", "B"], "target.json": ["A"]}, schema_version="2.1.0")
    rc, out_dir = _run(tmp_path, ctx, target="target.json", purge_view="all")
    assert rc == 0
    diag = json.loads((out_dir / cr.DIAGNOSTICS_FILENAME).read_text())
    assert cr.REASON_CROSS_SEGMENT_SCHEMA_MISMATCH not in diag["run_comparison_reason_codes"]
