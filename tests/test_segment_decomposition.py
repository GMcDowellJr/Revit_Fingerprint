"""Synthetic-fixture coverage for tools/segment_decomposition.py (D-052).

The end-to-end fixture runs the real compare_cross_segment.py main() first, so
the reconciliation gate and the matrix drift guard are exercised against
genuine compare outputs rather than hand-written summary rows.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

import segment_decomposition as sd  # noqa: E402
from compare_cross_segment import main as compare_main  # noqa: E402

BANDS = [{"label": "any", "lower": None, "upper": None}]

PARENT = "imperial|Project|Acme"
ARCH = "imperial|Project|Acme|Arch"
ELEC = "imperial|Project|Acme|Elec"

# file -> (project_label, discipline)
FILES = {
    "a1": ("X", "Arch"), "a2": ("X", "Arch"), "a3": ("Y", "Arch"),
    "e1": ("X", "Elec"), "e2": ("Y", "Elec"), "e3": ("Y", "Elec"),
    "u1": ("", "Elec"),  # unlabeled project
}
# domain -> file -> join_hashes (all view)
UNITS = {
    "a1": {"j1", "j2", "j3"}, "a2": {"j1", "j2"}, "a3": {"j1", "j4"},
    "e1": {"j1", "j2", "j5"}, "e2": {"j5", "j6"}, "e3": {"j5", "j6", "j7"},
    "u1": {"j1", "j6"},
}
ODD = {"a1": {"k1"}, "a2": {"k1", "k2"}, "e1": {"k2"}, "e2": {"k1"}, "e3": {"k1", "k2"}}
# used view drops a3 from "units" entirely (all its patterns purgeable-only).
USED_DROP = {"units": {"a3"}}


def _write_csv(path: Path, rows, fieldnames=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = fieldnames or list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _read_csv(path: Path):
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _manifest_row(sid, level, parent, disc, label, files):
    return {
        "segment_id": sid, "parent_segment_id": parent, "segment_level": str(level),
        "unit_system": "imperial", "governance_role": "Project", "client_label": "Acme",
        "discipline_label": disc, "business_center_label": "", "run_type": "bundle",
        "file_count": str(len(files)), "segment_label": label, "ancestor_segment_ids": parent,
    }


def _write_segment(root: Path, folder: str, files):
    base = root / folder / "results"
    dp, all_mm, used_mm = [], {}, {}
    for domain, inv in (("units", UNITS), ("odd_domain", ODD)):
        jhs = sorted({jh for e in files if e in inv for jh in inv[e]})
        pid = {jh: f"{folder}_{domain}_{jh}" for jh in jhs}
        dp += [{"domain": domain, "pattern_id": pid[jh], "source_cluster_id": f"src|{jh}",
                "pattern_label_human": jh, "pattern_label": jh} for jh in jhs]
        all_mm[domain] = [{"export_run_id": e, "pattern_id": pid[jh]}
                          for e in files if e in inv for jh in sorted(inv[e])]
        used_mm[domain] = [r for r in all_mm[domain] if r["export_run_id"] not in USED_DROP.get(domain, set())]
    _write_csv(base / "analysis" / "domain_patterns.csv", dp)
    for domain in all_mm:
        for view, mm in (("all", all_mm), ("used", used_mm)):
            _write_csv(base / "bundle_analysis" / view / domain / "membership_matrix.csv", mm[domain],
                       ["export_run_id", "pattern_id"])


@pytest.fixture()
def corpus(tmp_path, monkeypatch):
    records, segments_root, cross = tmp_path / "records", tmp_path / "segments", tmp_path / "cross"
    arch_files = [e for e, (_p, d) in FILES.items() if d == "Arch"]
    elec_files = [e for e, (_p, d) in FILES.items() if d == "Elec"]
    all_files = list(FILES)
    seg_files = {PARENT: all_files, ARCH: arch_files, ELEC: elec_files}
    # PARENT is label-keyed in the matrix; ARCH is id-keyed (label == segment_id).
    _write_csv(records / "segment_manifest.csv", [
        _manifest_row(PARENT, 3, "imperial|Project", "", "Acme projects — standards as practiced", all_files),
        _manifest_row(ARCH, 4, PARENT, "Arch", ARCH, arch_files),
        _manifest_row(ELEC, 4, PARENT, "Elec", "Acme Elec projects — standards as practiced", elec_files),
    ])
    folders = {PARENT: "p", ARCH: "arch", ELEC: "elec"}
    _write_csv(records / "run_registry.csv", [
        {"segment_id": s, "output_folder": f, "run_type": "bundle"} for s, f in folders.items()])
    _write_csv(records / "file_metadata.csv", [
        {"export_run_id": e, "project_label": p, "discipline_label": d} for e, (p, d) in FILES.items()])
    for sid, folder in folders.items():
        _write_segment(segments_root, folder, seg_files[sid])

    monkeypatch.setattr(sys, "argv", [
        "compare_cross_segment.py", "--segments-root", str(segments_root), "--records-dir", str(records),
        "--out-dir", str(cross), "--within-project", "--sibling-segments",
        "--min-patterns", "1", "--workers", "1", "--no-delta",
    ])
    assert compare_main() == 0
    return records, segments_root, cross


def _run(corpus, *extra):
    records, segments_root, cross = corpus
    rc = sd.main(["--records-dir", str(records), "--cross-segment-dir", str(cross),
                  "--segments-root", str(segments_root), *extra])
    out = cross / "segment_decomposition"
    return rc, out, json.loads((out / sd.RUN_SUMMARY_FILE).read_text())


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("view", ["all", "used"])
def test_end_to_end_reconciles_with_compare(corpus, view):
    rc, out, run = _run(corpus, "--view-scope", view)
    assert rc == 0, run["reconciliation"]
    assert run["status"] == "ok"
    assert run["reconciliation"]["mismatches"] == []
    assert run["reconciliation"]["checked"] > 0
    assert run["reconciliation"]["checked"] == run["reconciliation"]["matched"]
    assert run["findings"] == []  # matrix drift guard agrees with the summary
    for name in sd.OUTPUT_FILES.values():
        assert (out / name).is_file()


def test_nested_segments_count_each_pair_once_per_segment(corpus):
    _rc, out, _run_json = _run(corpus, "--view-scope", "all")
    rows = {(r["segment_id"], r["domain"]): r for r in _read_csv(out / sd.OUTPUT_FILES["effect_domain"])}
    parent = rows[(PARENT, "units")]
    # 7 files -> 21 pairs, scored once each even though the (a1, a2) pair also
    # appears under ARCH's within_project rows and (a1, e1) under the sibling rows.
    assert parent["n_segment_pairs"] == "21"
    # Same-project pairs: X={a1,a2,e1} -> 3, Y={a3,e2,e3} -> 3.
    assert parent["n_within_pairs"] == "6"
    # u1 is unlabeled: its 6 pairs are excluded from both buckets.
    assert parent["n_pairs_excluded_unlabeled"] == "6"
    assert parent["n_between_pairs"] == str(21 - 6 - 6)
    assert parent["n_unlabeled_files"] == "1"
    assert rows[(ARCH, "units")]["n_within_pairs"] == "1"
    seg_rows = {r["segment_id"]: r for r in _read_csv(out / sd.OUTPUT_FILES["effect_segment"])}
    assert seg_rows[PARENT]["n_unlabeled_files"] == "1"
    assert seg_rows[PARENT]["n_projects"] == "2"


def test_used_view_missing_pairs_are_blank_not_zero(corpus):
    _rc, out, _run_json = _run(corpus, "--view-scope", "used")
    rows = {(r["segment_id"], r["domain"]): r for r in _read_csv(out / sd.OUTPUT_FILES["effect_domain"])}
    arch = rows[(ARCH, "units")]
    # a3 is absent from the used view: pairs (a1,a3),(a2,a3) are excluded, not scored 0.
    assert arch["n_files"] == "2" and arch["n_files_all_view"] == "3"
    assert arch["n_pairs_missing_view"] == "2"
    assert arch["n_segment_pairs"] == "1"
    # The only remaining pair is (a1, a2): |{j1,j2}| / |{j1,j2,j3}|.
    assert float(arch["segment_wide_mean"]) == pytest.approx(2 / 3, abs=1e-6)
    assert arch["between_mean"] == "" and arch["project_effect"] == ""
    assert arch["row_status"] == "unavailable"


def test_unmapped_domain_is_surfaced(corpus):
    _rc, out, run = _run(corpus, "--view-scope", "all")
    assert run["unmapped_domains"] == ["odd_domain"]
    cats = {r["functional_category"] for r in _read_csv(out / sd.OUTPUT_FILES["effect_category"])}
    assert sd.UNMAPPED in cats and "Core Environment" in cats
    loo = [r for r in _read_csv(out / sd.OUTPUT_FILES["loo"]) if r["domain"] == "odd_domain"]
    assert loo and {r["functional_category"] for r in loo} == {sd.UNMAPPED}


def test_split_lift_rows_and_matrix_label_resolution(corpus):
    _rc, out, run = _run(corpus, "--view-scope", "all")
    rows = _read_csv(out / sd.OUTPUT_FILES["split_lift"])
    child = {r["segment_id"]: r for r in rows if r["row_grain"] == "child"}
    assert set(child) == {ARCH, ELEC}
    assert all(r["row_status"] == "ok" for r in child.values())
    parent = [r for r in rows if r["row_grain"] == "parent"]
    assert len(parent) == 1 and parent[0]["segment_id"] == PARENT and parent[0]["n_children"] == "2"
    # within_project_only is reported separately from segment-wide internal consistency.
    assert child[ARCH]["within_project_only_mean"] != child[ARCH]["internal_mean_equal_weighted"]
    # Label-keyed (PARENT, ELEC) and id-keyed (ARCH) diagonal cells all resolve.
    loo = [r for r in _read_csv(out / sd.OUTPUT_FILES["loo"]) if r["basis"] == "within_project_only"]
    assert {r["segment_id"] for r in loo if r["published_status"] == "ok"} == {PARENT, ARCH, ELEC}
    assert run["coverage_gaps"]["ambiguous_matrix_row_ids"] == []


def test_reconciliation_mismatch_is_hard_failure(corpus):
    records, segments_root, cross = corpus
    summary = cross / "cross_segment_summary.csv"
    rows = _read_csv(summary)
    for r in rows:
        if r["comparison_type"] == "within_project" and r["segment_id_a"] == ARCH and r["domain"] == "units":
            r["all_pairwise_jaccard_mean"] = "0.123456"
    _write_csv(summary, rows)
    out = cross / "segment_decomposition"
    out.mkdir(parents=True, exist_ok=True)
    stale = out / sd.OUTPUT_FILES["split_lift"]
    stale.write_text("stale\n")
    rc, out, run = _run(corpus, "--view-scope", "all")
    assert rc == 1
    assert run["status"] == "failed_reconciliation"
    assert [(m["segment_id"], m["domain"]) for m in run["reconciliation"]["mismatches"]] == [(ARCH, "units")]
    assert not any((out / name).exists() for name in sd.OUTPUT_FILES.values())


def test_parent_segment_filter(corpus):
    rc, _out, run = _run(corpus, "--view-scope", "all", "--parent-segment", PARENT)
    assert rc == 0 and run["segments_processed"] == 3 and run["split_families"] == 1
    rc, _out, _run_json = _run(corpus, "--view-scope", "all", "--parent-segment", ARCH)
    assert rc == 2


# ---------------------------------------------------------------------------
# Unit-level arithmetic and readers
# ---------------------------------------------------------------------------

def test_split_lift_file_weighted_vs_equal_weighted_arithmetic():
    segs = {
        "P": {"segment_label": "P", "file_count": "12"},
        "c1": {"segment_label": "c1", "file_count": "2", "discipline_label": "A"},
        "c2": {"segment_label": "c2", "file_count": "4", "discipline_label": "B"},
        "c3": {"segment_label": "c3", "file_count": "6", "discipline_label": "C"},
    }
    seg_wide = {"c1": {"d1": 0.8}, "c2": {"d1": 0.6}, "c3": {"d1": 0.5}}
    sibling = {
        (frozenset(("c1", "c2")), "d1"): 0.4,
        (frozenset(("c1", "c3")), "d1"): 0.1, (frozenset(("c1", "c3")), "d2"): 0.3,  # ALL_DOMAINS -> 0.2
        # c2-c3 never compared
    }
    rows = sd.build_split_lift_rows([("P", ["c1", "c2", "c3"])], segs, "discipline_label", "used",
                                    seg_wide, {}, {}, sibling, BANDS, Counter())
    c1 = next(r for r in rows if r["segment_id"] == "c1")
    assert float(c1["sibling_mean_equal_weighted"]) == pytest.approx(0.3)
    assert float(c1["sibling_mean_file_weighted"]) == pytest.approx((0.4 * 4 + 0.2 * 6) / 10)
    assert float(c1["lift_equal_weighted"]) == pytest.approx(0.5)
    assert float(c1["lift_file_weighted"]) == pytest.approx(0.52)
    assert float(c1["weighting_difference"]) == pytest.approx(0.02)
    c3 = next(r for r in rows if r["segment_id"] == "c3")
    assert c3["n_siblings_with_value"] == "1"  # c2 pair unavailable, not zero
    parent = next(r for r in rows if r["row_grain"] == "parent")
    assert float(parent["internal_mean_equal_weighted"]) == pytest.approx((0.8 + 0.6 + 0.5) / 3)
    assert float(parent["internal_mean_file_weighted"]) == pytest.approx((0.8 * 2 + 0.6 * 4 + 0.5 * 6) / 12)
    assert float(parent["sibling_mean_equal_weighted"]) == pytest.approx(0.3)
    assert float(parent["sibling_mean_file_weighted"]) == pytest.approx((0.4 * 8 + 0.2 * 12) / 20)


def test_split_lift_no_sibling_comparison_is_unavailable():
    segs = {"P": {"file_count": "4"}, "c1": {"file_count": "2"}, "c2": {"file_count": "2"}}
    skips = Counter()
    rows = sd.build_split_lift_rows([("P", ["c1", "c2"])], segs, "discipline_label", "used",
                                    {"c1": {"d": 0.9}, "c2": {"d": 0.7}}, {}, {}, {}, BANDS, skips)
    for r in rows:
        assert r["row_status"] == "unavailable" and r["skip_reason"] == "no_sibling_comparison"
        assert r["sibling_mean_equal_weighted"] == "" and r["lift_equal_weighted"] == ""
    assert skips["split_lift:no_sibling_comparison"] == 2


def test_leave_one_out_hand_computed():
    findings = []
    rows = sd.build_loo_rows("segment_wide", "used", {"s": {"segment_label": "S"}},
                             {"s": {"a": 0.2, "b": 0.5, "c": 0.8}}, {}, {"a": "Cat"}, BANDS, 1e-6,
                             None, findings, Counter())
    by_domain = {r["domain"]: r for r in rows}
    assert all(r["mean_all"] == "0.500000" for r in rows)
    assert float(by_domain["a"]["mean_without"]) == pytest.approx(0.65)
    assert float(by_domain["a"]["impact"]) == pytest.approx(-0.15)
    assert float(by_domain["b"]["impact"]) == pytest.approx(0.0)
    assert float(by_domain["c"]["impact"]) == pytest.approx(0.15)
    assert by_domain["b"]["functional_category"] == sd.UNMAPPED
    single = sd.build_loo_rows("segment_wide", "used", {"s": {}}, {"s": {"a": 0.4}}, {}, {}, BANDS, 1e-6,
                               None, findings, Counter())
    assert single[0]["mean_without"] == "" and single[0]["impact"] == "" and single[0]["row_status"] == "unavailable"


def test_drift_guard_skips_synthetic_and_records_drift_without_failing():
    segs = {"syn": {}, "ok": {}, "drift": {}}
    values = {"syn": {"a": 0.4}, "ok": {"a": 0.4, "b": 0.6}, "drift": {"a": 0.4, "b": 0.6}}
    diagonal = {"syn": ("synthetic_self_comparison", 1.0), "ok": ("ok", 0.5), "drift": ("ok", 0.51)}
    findings, skips = [], Counter()
    rows = sd.build_loo_rows("within_project_only", "used", segs, values, {}, {}, BANDS, 1e-6,
                             diagonal, findings, skips)
    syn = [r for r in rows if r["segment_id"] == "syn"]
    assert syn[0]["published_status"] == "synthetic_self_comparison" and syn[0]["published_all_domains"] == ""
    assert syn[0]["recalc_difference"] == ""
    assert [f["segment_id"] for f in findings] == ["drift"]
    assert findings[0]["finding"] == "loo_published_vs_recalculated_drift"
    assert skips["loo:drift_guard_not_applicable:synthetic_self_comparison"] == 1


def test_read_matrix_diagonal_label_and_id_keys(tmp_path):
    manifest = {
        "u|P|A": {"segment_label": "A projects"},                 # label-keyed
        "u|P|B|x": {"segment_label": "u|P|B|x"},                  # id-keyed (label fallback)
        "imperial|P|C": {"segment_label": "C projects"},          # label collides across units
        "metric|P|C": {"segment_label": "C projects"},
    }
    rows = [
        {"row_id": rid, "column_id": rid, "view_scope": "used", "domain": "ALL_DOMAINS",
         "value": val, "value_status": status}
        for rid, val, status in [("A projects", "0.400000", "ok"), ("u|P|B|x", "1.000000", "synthetic_self_comparison"),
                                 ("C projects", "0.300000", "ok")]
    ] + [{"row_id": "A projects", "column_id": "u|P|B|x", "view_scope": "used", "domain": "ALL_DOMAINS",
          "value": "0.2", "value_status": "ok"}]
    path = tmp_path / "m.csv"
    _write_csv(path, rows)
    diagonal, ambiguous = sd.read_matrix_diagonal(path, "used", manifest)
    assert diagonal == {"u|P|A": ("ok", 0.4), "u|P|B|x": ("synthetic_self_comparison", 1.0)}
    assert ambiguous == ["C projects"]


def test_read_summary_excludes_blank_and_directed_and_flags_conflicts(tmp_path):
    base = {"comparison_type": "", "segment_id_a": "", "segment_id_b": "", "domain": "d",
            "used_pairwise_jaccard_mean": "", "all_pairwise_jaccard_mean": ""}
    rows = [
        {**base, "comparison_type": "within_project", "segment_id_a": "c1", "segment_id_b": "c1",
         "used_pairwise_jaccard_mean": "0.5"},
        {**base, "comparison_type": "within_project", "segment_id_a": "c2", "segment_id_b": "c2"},  # blank
        {**base, "comparison_type": "sibling_projects", "segment_id_a": "c1", "segment_id_b": "c2",
         "used_pairwise_jaccard_mean": "0.3"},
        {**base, "comparison_type": "cross_client", "segment_id_a": "c2", "segment_id_b": "c1",
         "used_pairwise_jaccard_mean": "0.3"},  # same measurement, other orientation -> no conflict
        {**base, "comparison_type": "bc_to_project", "segment_id_a": "c1", "segment_id_b": "c3",
         "used_pairwise_jaccard_mean": "0.9"},  # directed -> ignored
        {**base, "comparison_type": "sibling_projects", "segment_id_a": "c1", "segment_id_b": "c3",
         "domain": "e", "used_pairwise_jaccard_mean": "0.1"},
        {**base, "comparison_type": "bc_to_bc", "segment_id_a": "c1", "segment_id_b": "c3",
         "domain": "e", "used_pairwise_jaccard_mean": "0.2"},  # conflicting value
    ]
    path = tmp_path / "s.csv"
    _write_csv(path, rows)
    pairs = {frozenset(("c1", "c2")), frozenset(("c1", "c3"))}
    within, sibling, conflicts, skips = sd.read_summary(path, "used", {"c1", "c2", "c3"}, pairs, 1e-6)
    assert within == {("c1", "d"): 0.5}
    assert sibling == {(frozenset(("c1", "c2")), "d"): 0.3}
    assert [c["kind"] for c in conflicts] == ["sibling"]
    assert skips["summary:within_project_value_blank"] == 1


def test_find_split_families_by_columns_not_ids():
    def row(level, disc="", bc="", client="Micron", unit="imperial"):
        return {"segment_level": str(level), "unit_system": unit, "governance_role": "Project",
                "client_label": client, "discipline_label": disc, "business_center_label": bc}
    segs = {
        "P_bc": row(4, bc="4001"),
        "E_bc": row(5, disc="Electrical", bc="4001"),
        "M_bc": row(5, disc="Mechanical", bc="4001"),
        "E_other_bc": row(5, disc="Electrical", bc="4002"),   # different bc -> not a child of P_bc
        "E_metric": row(5, disc="Electrical", bc="4001", unit="metric"),
        "P_client": row(3),                                   # no bc: its discipline children would lack bc
    }
    assert sd.find_split_families(segs, "discipline_label") == [("P_bc", ["E_bc", "M_bc"])]


def test_policy_file_loads_and_validates(tmp_path):
    policy = sd.load_policy(sd.DEFAULT_POLICY_PATH)
    cats = policy["categories"]
    assert cats["wall_types"] == "Model Content"
    assert len(cats) == 37 and policy["drift_tolerance"] == 1e-6
    bad = dict(json.loads(sd.DEFAULT_POLICY_PATH.read_text()))
    bad["categories"] = {"units": sd.UNMAPPED}
    p = tmp_path / "bad.json"
    p.write_text(json.dumps(bad))
    with pytest.raises(ValueError):
        sd.load_policy(p)
    bad["schema"] = "other"
    p.write_text(json.dumps(bad))
    with pytest.raises(ValueError):
        sd.load_policy(p)


def test_band_for_is_half_open():
    bands = [{"label": "lo", "lower": None, "upper": 0.0}, {"label": "hi", "lower": 0.0, "upper": 1.0}]
    assert sd.band_for(-0.1, bands) == "lo"
    assert sd.band_for(0.0, bands) == "hi"
    assert sd.band_for(1.0, bands) == ""
    assert sd.band_for(None, bands) == ""
