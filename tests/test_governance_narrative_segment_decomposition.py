"""D-053: segment_decomposition.py (D-052) outputs surfaced in
generate_governance_narrative.py's Segment Decomposition section and in
governance_evidence_map.json -- presentation only.

Covers outputs present / folder absent / failed_reconciliation, the evidence
map's new entries, and that assign_tier() output, governance_domain_summary.csv
and governance_findings.json's findings are byte-identical with and without the
decomposition folder present.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

import generate_governance_narrative as ggn  # noqa: E402
import segment_decomposition as sd  # noqa: E402
from compare_cross_segment import POOLED_FIELDS, SUMMARY_FIELDS  # noqa: E402
from governance_evidence_package import (  # noqa: E402
    SEGMENT_DECOMPOSITION_FILES,
    SEGMENT_DECOMPOSITION_RUN_FILE,
    SEGMENT_DECOMPOSITION_SCHEMA_VERSION,
    SEGMENT_DECOMPOSITION_VIEWS,
)

LIMITATION = (
    "Within-project similarity is partly mechanical (shared/copied content within a project) "
    "and does not by itself indicate governance quality."
)
BANDS_CAVEAT = "are descriptive labels, not governance thresholds"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_csv(path: Path, fields: list, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def _corpus(tmp_path: Path) -> tuple:
    """Small cross-segment corpus with several renderable domains."""
    root = tmp_path / "cross_segment"
    summary_rows = []
    for domain, cont, jac in (("line_styles", "0.8", "0.5"), ("fill_patterns_drafting", "0.4", "0.3"),
                              ("text_types", "0.95", "0.9")):
        base = dict(comparison_run_id="run1", domain=domain, unit_system="imperial",
                    executed_utc="2026-07-16T00:00:00Z")
        summary_rows.append({**base, "segment_id_a": "imperial|Template", "segment_id_b": "imperial|Project|acme",
                             "governance_role_a": "Template", "governance_role_b": "Project",
                             "client_label_b": "acme", "comparison_type": "template_to_project",
                             "all_pairwise_containment_a_in_b_mean": cont, "all_pairwise_jaccard_mean": jac,
                             "n_files_a": "3", "n_files_b": "10"})
        summary_rows.append({**base, "segment_id_a": "imperial|Project|acme", "segment_id_b": "imperial|Project|acme",
                             "governance_role_a": "Project", "governance_role_b": "Project",
                             "client_label_a": "acme", "client_label_b": "acme",
                             "comparison_type": "within_project",
                             "all_pairwise_jaccard_mean": jac, "used_pairwise_jaccard_mean": jac,
                             "n_files_a": "10", "n_files_b": "10"})
    pooled_rows = [{"comparison_run_id": "run1", "segment_id": "imperial|Project|acme",
                    "client_label": "acme", "governance_role": "Project", "pool_scope": "parent_sibling",
                    "domain": "line_styles", "n_files_focal": "10", "n_files_pool": "30",
                    "executed_utc": "2026-07-16T00:00:00Z"}]
    summary = root / "cross_segment_summary.csv"
    pooled = root / "cross_segment_pooled.csv"
    _write_csv(summary, SUMMARY_FIELDS, summary_rows)
    _write_csv(pooled, POOLED_FIELDS, pooled_rows)
    return summary, pooled


def _run_summary(view: str, status: str = "ok", **extra) -> dict:
    run = {
        "schema_version": SEGMENT_DECOMPOSITION_SCHEMA_VERSION,
        "status": status,
        "executed_utc": "2026-10-21T00:00:00Z",
        "args": {"view_scope": view, "split_dimension": "discipline_label"},
        "segments_processed": 3,
        "split_families": 1,
        "file_pairs_scored": 120,
        "outputs_written": status == "ok",
        "skip_counts": {},
        "unmapped_domains": ["identity"],
        "domains_below_min_patterns": {"min_patterns": 3, "segments_skipped_by_domain": {"units_doc": 2}},
        "reconciliation": {"tolerance": 1e-6, "checked": 9, "matched": 9 if status == "ok" else 8,
                           "no_reference": 0, "mismatches": [] if status == "ok" else [{"segment_id": "x"}]},
        "findings": [],
        "summary_conflicts": [],
        "known_limitation": LIMITATION,
    }
    run.update(extra)
    return run


def _write_decomp(folder: Path, view: str, status: str = "ok", write_csvs: bool = True,
                  marker: str = "0.123") -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / SEGMENT_DECOMPOSITION_RUN_FILE).write_text(
        json.dumps(_run_summary(view, status)), encoding="utf-8")
    if not write_csvs:
        return folder
    parent = {"row_grain": "parent", "view_scope": view, "split_dimension": "discipline_label",
              "parent_segment_id": "imperial|Project|Micron", "parent_segment_label": "imperial|Project|Micron",
              "segment_id": "imperial|Project|Micron", "segment_label": "imperial|Project|Micron",
              "n_children": "2", "internal_mean_equal_weighted": "0.700", "internal_mean_file_weighted": "0.710",
              "sibling_mean_equal_weighted": "0.500", "sibling_mean_file_weighted": "0.520",
              "lift_equal_weighted": marker, "lift_file_weighted": "0.190",
              "presentation_band_descriptive": "large_lift", "row_status": "ok"}
    children = [
        {"row_grain": "child", "view_scope": view, "split_dimension": "discipline_label",
         "parent_segment_id": "imperial|Project|Micron", "parent_segment_label": "imperial|Project|Micron",
         "segment_id": "imperial|Project|Micron|4001", "segment_label": "imperial|Project|Micron|4001",
         "lift_equal_weighted": "0.250", "lift_file_weighted": "0.240",
         "presentation_band_descriptive": "large_lift", "row_status": "ok"},
        {"row_grain": "child", "view_scope": view, "split_dimension": "discipline_label",
         "parent_segment_id": "imperial|Project|Micron", "parent_segment_label": "imperial|Project|Micron",
         "segment_id": "imperial|Project|Micron|4002", "segment_label": "imperial|Project|Micron|4002",
         "lift_equal_weighted": "-0.010", "lift_file_weighted": "0.000",
         "presentation_band_descriptive": "small_difference", "row_status": "ok"},
        {"row_grain": "child", "view_scope": view, "split_dimension": "discipline_label",
         "parent_segment_id": "imperial|Project|Micron", "parent_segment_label": "imperial|Project|Micron",
         "segment_id": "imperial|Project|Micron|4003", "segment_label": "imperial|Project|Micron|4003",
         "row_status": "unavailable", "skip_reason": "no_sibling_comparison"},
    ]
    _write_csv(folder / SEGMENT_DECOMPOSITION_FILES["split_lift"], sd.SPLIT_LIFT_FIELDS, [parent] + children)
    loo = [
        {"basis": "segment_wide", "view_scope": view, "segment_id": "imperial|Project|Micron|4001",
         "segment_label": "imperial|Project|Micron|4001", "domain": "text_types",
         "mean_all": "0.600", "mean_without": "0.580", "impact": "0.020",
         "presentation_band_descriptive": "domain_pulls_mean_up", "row_status": "ok"},
        {"basis": "segment_wide", "view_scope": view, "segment_id": "imperial|Project|Micron|4001",
         "segment_label": "imperial|Project|Micron|4001", "domain": "line_styles",
         "mean_all": "0.600", "mean_without": "0.640", "impact": "-0.040",
         "presentation_band_descriptive": "domain_pulls_mean_down", "row_status": "ok"},
        {"basis": "within_project_only", "view_scope": view, "segment_id": "imperial|Project|Micron|4001",
         "segment_label": "imperial|Project|Micron|4001", "domain": "fill_patterns_drafting",
         "mean_all": "0.900", "mean_without": "0.100", "impact": "0.999", "row_status": "ok"},
    ]
    _write_csv(folder / SEGMENT_DECOMPOSITION_FILES["loo"], sd.LOO_FIELDS, loo)
    seg = [{"view_scope": view, "segment_id": "imperial|Project|Micron|4001",
            "segment_label": "imperial|Project|Micron|4001", "n_files": "12", "n_projects": "3",
            "equal_domain_within_mean": "0.810", "equal_domain_between_mean": "0.420",
            "equal_domain_effect": "0.390", "comparison_weighted_effect": "0.370",
            "n_within_pairs": "20", "n_between_pairs": "46",
            "presentation_band_descriptive": "large_project_effect", "row_status": "ok"}]
    _write_csv(folder / SEGMENT_DECOMPOSITION_FILES["effect_segment"], sd.EFFECT_SEGMENT_FIELDS, seg)
    _write_csv(folder / SEGMENT_DECOMPOSITION_FILES["effect_category"], sd.EFFECT_CATEGORY_FIELDS, [])
    _write_csv(folder / SEGMENT_DECOMPOSITION_FILES["effect_domain"], sd.EFFECT_DOMAIN_FIELDS, [])
    return folder


def _run(monkeypatch, summary: Path, pooled: Path, out: Path, extra=()) -> str:
    out.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sys, "argv", ["generate_governance_narrative.py", "--summary", str(summary),
                                      "--pooled", str(pooled), "--out", str(out), "--date", "2026-10-22",
                                      *extra])
    ggn.main()
    return (out / "governance_narrative_context.md").read_text(encoding="utf-8")


def _section(md: str) -> str:
    start = md.index("## Segment Decomposition (D-052)")
    nxt = md.find("\n## ", start + 1)
    return md[start:] if nxt == -1 else md[start:nxt]


# ---------------------------------------------------------------------------
# Constants agree with the producer
# ---------------------------------------------------------------------------

def test_constants_match_segment_decomposition_producer():
    assert SEGMENT_DECOMPOSITION_FILES == sd.OUTPUT_FILES
    assert SEGMENT_DECOMPOSITION_RUN_FILE == sd.RUN_SUMMARY_FILE
    assert SEGMENT_DECOMPOSITION_SCHEMA_VERSION == sd.SCHEMA_VERSION
    assert set(SEGMENT_DECOMPOSITION_VIEWS) == set(sd.VIEW_COLUMN)


def test_rendered_columns_exist_in_producer_schemas():
    read = {
        "split": {"row_grain", "split_dimension", "parent_segment_id", "parent_segment_label", "n_children",
                  "segment_id", "segment_label", "internal_mean_equal_weighted", "internal_mean_file_weighted",
                  "sibling_mean_equal_weighted", "sibling_mean_file_weighted", "lift_equal_weighted",
                  "lift_file_weighted", "presentation_band_descriptive", "row_status", "skip_reason"},
        "loo": {"basis", "segment_id", "segment_label", "domain", "mean_all", "mean_without", "impact",
                "presentation_band_descriptive"},
        "seg": {"segment_id", "segment_label", "n_files", "n_projects", "equal_domain_within_mean",
                "equal_domain_between_mean", "equal_domain_effect", "comparison_weighted_effect",
                "n_within_pairs", "n_between_pairs", "presentation_band_descriptive", "row_status", "skip_reason"},
    }
    assert read["split"] <= set(sd.SPLIT_LIFT_FIELDS)
    assert read["loo"] <= set(sd.LOO_FIELDS)
    assert read["seg"] <= set(sd.EFFECT_SEGMENT_FIELDS)


# ---------------------------------------------------------------------------
# Narrative rendering
# ---------------------------------------------------------------------------

def test_section_with_outputs_present(tmp_path, monkeypatch):
    summary, pooled = _corpus(tmp_path)
    _write_decomp(summary.parent / "segment_decomposition", "used")
    md = _run(monkeypatch, summary, pooled, tmp_path / "out")
    sec = _section(md)

    assert "### Used-view" in sec
    assert "never overrides a domain tier" in sec
    assert "Segments processed: 3; split families: 1; file pairs scored: 120" in sec
    assert "9 checked, 9 matched, 0 with no compare reference" in sec
    assert "Unmapped domains: identity" in sec
    assert "units_doc (2 segments)" in sec
    # Every paragraph carries the known limitation and the descriptive-bands statement.
    for heading in ("#### Split lift", "#### Project effect", "#### Domain leave-one-out impact"):
        para = sec[sec.index(heading):]
        nxt = para.find("\n#### ", 1)
        para = para if nxt == -1 else para[:nxt]
        assert LIMITATION in para, heading
        assert BANDS_CAVEAT in para, heading
    # Values are read verbatim (formatted), never recomputed.
    assert "| imperial\\|Project\\|Micron | 2 | 0.700 / 0.710 | 0.500 / 0.520 | 0.123 / 0.190 | large_lift | ok |" in sec
    assert "imperial|Project|Micron|4001, parent imperial|Project|Micron: lift 0.250 equal / 0.240 file-weighted (large_lift)" in sec
    assert "1 `no_sibling_comparison`" in sec
    assert ("| imperial\\|Project\\|Micron\\|4001 | 12 | 3 | 0.810 | 0.420 | 0.390 | 0.370 | 20 | 46 "
            "| large_project_effect | ok |") in sec
    assert "Text Types impact 0.020" in sec
    assert "Line Styles impact -0.040" in sec
    assert sec.index("pulling a segment mean up") < sec.index("Text Types impact") < sec.index("pulling a segment mean down") < sec.index("Line Styles impact")
    # within_project_only basis rows are not rendered as segment-wide impact.
    assert "0.999" not in sec
    # Child lift ordering: highest first; with <= N valued children the bottom
    # list would only repeat them, so it is omitted.
    assert sec.index("Highest child lift") < sec.index("|4001, parent") < sec.index("|4002, parent")
    assert "Lowest child lift" not in sec


def test_section_folder_absent_is_one_line_not_provided(tmp_path, monkeypatch):
    summary, pooled = _corpus(tmp_path)
    md = _run(monkeypatch, summary, pooled, tmp_path / "out")
    sec = _section(md).strip()
    assert sec == "## Segment Decomposition (D-052)\n\n" + ggn.SEGMENT_DECOMPOSITION_NOT_PROVIDED


def test_section_failed_reconciliation_renders_status_and_no_numbers(tmp_path, monkeypatch):
    summary, pooled = _corpus(tmp_path)
    folder = summary.parent / "segment_decomposition"
    # Stale CSVs with a distinctive marker sit beside a failed run summary; none may render.
    _write_decomp(folder, "used", marker="0.777")
    (folder / SEGMENT_DECOMPOSITION_RUN_FILE).write_text(
        json.dumps(_run_summary("used", "failed_reconciliation")), encoding="utf-8")
    md = _run(monkeypatch, summary, pooled, tmp_path / "out")
    sec = _section(md)
    assert "status `failed_reconciliation`" in sec
    assert "no decomposition numbers are rendered" in sec
    assert "0.777" not in sec
    assert "####" not in sec
    assert "Run status" not in sec


def test_both_views_via_repeatable_flag_and_view_read_from_run_summary(tmp_path, monkeypatch):
    summary, pooled = _corpus(tmp_path)
    used = _write_decomp(summary.parent / "segment_decomposition", "used")
    # Folder name deliberately says nothing about the view.
    all_dir = _write_decomp(tmp_path / "elsewhere" / "decomp_b", "all", marker="0.456")
    md = _run(monkeypatch, summary, pooled, tmp_path / "out",
              ["--segment-decomposition-dir", str(all_dir), "--segment-decomposition-dir", str(used)])
    sec = _section(md)
    assert sec.index("### Used-view") < sec.index("### All-view")
    assert "0.456" in sec[sec.index("### All-view"):]


def test_duplicate_view_folder_is_skipped(tmp_path, capsys):
    a = _write_decomp(tmp_path / "a", "used", marker="0.111")
    b = _write_decomp(tmp_path / "b", "used", marker="0.222")
    bundles = ggn.load_segment_decomposition([a, b])
    assert [x["dir"] for x in bundles] == [a]
    assert "already supplied" in capsys.readouterr().err


def test_unreadable_and_schema_mismatch_render_no_numbers(tmp_path):
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / SEGMENT_DECOMPOSITION_RUN_FILE).write_text("{not json", encoding="utf-8")
    other = _write_decomp(tmp_path / "other", "all", marker="0.888")
    run = _run_summary("all")
    run["schema_version"] = "segment_decomposition.v2"
    (other / SEGMENT_DECOMPOSITION_RUN_FILE).write_text(json.dumps(run), encoding="utf-8")
    sec = ggn.render_segment_decomposition_section(ggn.load_segment_decomposition([bad, other]))
    assert "could not be parsed" in sec
    assert "schema_version `segment_decomposition.v2`" in sec
    assert "0.888" not in sec


def test_ok_run_with_missing_csv_degrades_that_paragraph_only(tmp_path):
    folder = _write_decomp(tmp_path / "d", "used")
    (folder / SEGMENT_DECOMPOSITION_FILES["loo"]).unlink()
    sec = ggn.render_segment_decomposition_section(ggn.load_segment_decomposition([folder]))
    assert "`segment_domain_loo_impact.csv` not provided" in sec
    assert "#### Split lift" in sec and "0.123" in sec


# ---------------------------------------------------------------------------
# Evidence map
# ---------------------------------------------------------------------------

def test_evidence_map_entries(tmp_path, monkeypatch):
    summary, pooled = _corpus(tmp_path)
    _write_decomp(summary.parent / "segment_decomposition", "used")
    out = tmp_path / "out"
    _run(monkeypatch, summary, pooled, out)
    em = json.loads((out / "governance_evidence_map.json").read_text(encoding="utf-8"))
    by_id = {a["artifact_id"]: a for a in em["artifacts"]}
    assert len(em["artifacts"]) == 49

    expected = ["segment_decomposition_run", "segment_split_lift", "segment_domain_loo_impact",
                "segment_project_effect_by_domain", "segment_project_effect_by_category",
                "segment_project_effect_by_segment"]
    for view in ("used", "all"):
        for base in expected:
            a = by_id[f"{base}_{view}"]
            assert a["required_before_conclusions"] is False
            assert a["required"] is False
            assert a["producer"] == "segment_decomposition.py"
            assert a["authority_level"] == "authoritative_deterministic_evidence"
            assert any("partly mechanical" in k for k in a["known_limitations"])
            assert any("not a governance threshold" in k for k in a["known_limitations"])
            assert a["artifact_id"] not in em["reasoning_prerequisites"]
    # used supplied, all not supplied
    run_used = by_id["segment_decomposition_run_used"]
    assert run_used["present"] is True and run_used["artifact_type"] == "json"
    assert run_used["schema_version"] == SEGMENT_DECOMPOSITION_SCHEMA_VERSION
    lift = by_id["segment_split_lift_used"]
    assert lift["present"] is True
    assert lift["key_fields"] == ["row_grain", "view_scope", "split_dimension", "parent_segment_id", "segment_id"]
    assert lift["row_count"] == 4
    assert [c["name"] for c in lift["columns"]] == sd.SPLIT_LIFT_FIELDS
    assert by_id["segment_domain_loo_impact_used"]["key_fields"] == ["basis", "view_scope", "segment_id", "domain"]
    for base in expected:
        a = by_id[f"{base}_all"]
        assert a["present"] is False and a["path"] is None
        assert "row_count" not in a
    # Not listed again by the file inventory (subfolder, and registered anyway).
    inv = json.loads((out / "governance_file_inventory.json").read_text(encoding="utf-8"))
    assert not any(f["filename"].startswith("segment_") for f in inv["files"])


# ---------------------------------------------------------------------------
# Placement guardrail: nothing feeds assign_tier()/domain summary/findings
# ---------------------------------------------------------------------------

def test_tiers_domain_summary_and_findings_identical_with_and_without_folder(tmp_path, monkeypatch):
    summary, pooled = _corpus(tmp_path)
    calls = []
    real = ggn.assign_tier

    def recording(d, state=None):
        tier = real(d, state)
        calls.append(repr((sorted((k, repr(v)) for k, v in d.items()), repr(state), tier)))
        return tier

    monkeypatch.setattr(ggn, "assign_tier", recording)

    _run(monkeypatch, summary, pooled, tmp_path / "without")
    calls_without = list(calls)
    calls.clear()
    _write_decomp(summary.parent / "segment_decomposition", "used")
    _write_decomp(tmp_path / "all_view", "all")
    md = _run(monkeypatch, summary, pooled, tmp_path / "with",
              ["--segment-decomposition-dir", str(summary.parent / "segment_decomposition"),
               "--segment-decomposition-dir", str(tmp_path / "all_view")])
    calls_with = list(calls)

    assert "### Used-view" in md and "### All-view" in md
    assert calls_without and calls_with == calls_without
    a = (tmp_path / "without" / "governance_domain_summary.csv").read_bytes()
    b = (tmp_path / "with" / "governance_domain_summary.csv").read_bytes()
    assert a and a == b

    def findings(p):
        return json.loads((p / "governance_findings.json").read_text(encoding="utf-8"))["findings"]
    assert findings(tmp_path / "without") == findings(tmp_path / "with")


@pytest.mark.parametrize("view", ["used", "all"])
def test_view_dirs_only_for_known_views(tmp_path, view):
    folder = _write_decomp(tmp_path / view, view)
    bundles = ggn.load_segment_decomposition([folder, tmp_path / "missing"])
    assert ggn.segment_decomposition_view_dirs(bundles) == {view: folder}
