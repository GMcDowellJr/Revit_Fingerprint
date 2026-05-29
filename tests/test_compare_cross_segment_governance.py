"""Tests for governance semantics in tools/compare_cross_segment.py."""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from compare_cross_segment import (  # noqa: E402
    _classify_governance_state,
    _comparison_role_semantics,
    _recommended_primary_view,
    _usage_interpretable_for_role,
    discover_governance_chain,
)


def _seg(role: str, client: str = "Acme", unit: str = "imperial", discipline: str = "Arch"):
    return {
        "governance_role": role,
        "client_label": client,
        "unit_system": unit,
        "discipline_label": discipline,
        "run_type": "bundle",
    }


def test_discover_governance_chain_includes_generic_upstream_roles():
    manifest = {
        "g": _seg("Generic", client="Global"),
        "gh": _seg("Generic-Host", client="Global"),
        "t": _seg("Template"),
        "c": _seg("Container"),
        "p": _seg("Project"),
    }

    pairs = set(discover_governance_chain(manifest))

    assert ("g", "t", "generic_to_template") in pairs
    assert ("g", "c", "generic_to_container") in pairs
    assert ("g", "p", "generic_to_project") in pairs
    assert ("gh", "t", "generic_to_template") in pairs
    assert ("t", "p", "template_to_project") in pairs
    assert ("t", "c", "template_to_container") in pairs
    assert ("c", "p", "container_to_project") in pairs


def test_project_target_governance_state_uses_target_used():
    assert _usage_interpretable_for_role("Project") is True
    assert _recommended_primary_view("Template", "Project", "template_to_project") == "used"
    assert (
        _classify_governance_state(True, True, False, True, True)
        == "provided_but_passive"
    )
    assert (
        _classify_governance_state(False, True, True, True, True)
        == "local_active"
    )


def test_standards_carrier_target_avoids_passive_bloat_label():
    assert _usage_interpretable_for_role("Template") is False
    assert _recommended_primary_view("Generic", "Template", "generic_to_template") == "all"
    assert (
        _classify_governance_state(True, True, False, True, False)
        == "provided_configured"
    )
    assert "all-view is primary" in _comparison_role_semantics(
        "Generic", "Template", "generic_to_template"
    )


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise AssertionError("test helper requires at least one row")
    import csv

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_segment(seg_root: Path, folder: str, domain: str, patterns, all_rows, used_rows, bundle_all):
    base = seg_root / folder / "results"
    _write_csv(
        base / "analysis" / "domain_patterns.csv",
        [
            {
                "domain": domain,
                "pattern_id": pid,
                "source_cluster_id": f"src|{jh}",
                "pattern_label_human": label,
                "pattern_label": label,
            }
            for pid, jh, label in patterns
        ],
    )
    _write_csv(base / "bundle_analysis" / "all" / domain / "membership_matrix.csv", all_rows)
    _write_csv(base / "bundle_analysis" / "used" / domain / "membership_matrix.csv", used_rows)
    _write_csv(
        base / "bundle_analysis" / "all" / domain / "bundle_membership.csv",
        [{"pattern_id": pid} for pid in bundle_all],
    )


def test_build_governance_state_rows_include_inherited_unused_and_local_active(tmp_path):
    from compare_cross_segment import build_governance_state_outputs  # noqa: E402

    domain = "line_patterns"
    segments_root = tmp_path / "segments"
    _write_segment(
        segments_root,
        "ref",
        domain,
        [("r1", "provided_used", "Provided Used"), ("r2", "provided_passive", "Provided Passive")],
        [
            {"export_run_id": "ref_file", "pattern_id": "r1"},
            {"export_run_id": "ref_file", "pattern_id": "r2"},
        ],
        [{"export_run_id": "ref_file", "pattern_id": "r1"}],
        ["r1", "r2"],
    )
    _write_segment(
        segments_root,
        "tgt",
        domain,
        [
            ("t1", "provided_used", "Provided Used"),
            ("t2", "provided_passive", "Provided Passive"),
            ("t3", "local_active", "Local Active"),
        ],
        [
            {"export_run_id": "target_file", "pattern_id": "t1"},
            {"export_run_id": "target_file", "pattern_id": "t2"},
            {"export_run_id": "target_file", "pattern_id": "t3"},
        ],
        [
            {"export_run_id": "target_file", "pattern_id": "t1"},
            {"export_run_id": "target_file", "pattern_id": "t3"},
        ],
        ["t1", "t2", "t3"],
    )
    manifest = {
        "ref": {**_seg("Template"), "segment_label": "Template"},
        "tgt": {**_seg("Project"), "segment_label": "Project"},
    }
    registry = {
        "ref": {"output_folder": "ref", "run_type": "bundle"},
        "tgt": {"output_folder": "tgt", "run_type": "bundle"},
    }

    rows, summary = build_governance_state_outputs(
        "cmp_test",
        "ref",
        "tgt",
        "template_to_project",
        domain,
        manifest,
        registry,
        segments_root,
        "2026-05-29T00:00:00Z",
    )

    states = {row["join_hash"]: row["state"] for row in rows}
    assert states == {
        "provided_used": "provided_and_used",
        "provided_passive": "provided_but_passive",
        "local_active": "local_active",
    }
    assert summary["provided_to_configured_containment"] == "1.000000"
    assert summary["provided_to_used_containment"] == "0.500000"
    assert summary["provided_passive_share"] == "0.500000"
    assert summary["local_active_share"] == "0.500000"
