import csv
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import compare_reference as cr  # noqa: E402


def _write(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _segment(tmp_path, records, memberships):
    root = tmp_path / "segment"
    _write(
        root / "results/records/records.csv",
        ["export_run_id", "domain", "record_pk", "label_display", "label_quality"],
        records,
    )
    _write(
        root / "results/analysis/record_pattern_membership.csv",
        ["export_run_id", "domain", "record_pk", "pattern_id"],
        memberships,
    )
    return root


def _record(export_id, pk, name, quality="human"):
    return {"export_run_id": export_id, "domain": "d", "record_pk": pk,
            "label_display": name, "label_quality": quality}


def _membership(export_id, pk, pattern):
    return {"export_run_id": export_id, "domain": "d", "record_pk": pk, "pattern_id": pattern}


def test_names_are_per_file_evidence_and_do_not_change_identity(tmp_path):
    root = _segment(
        tmp_path,
        [_record("ref", "r", "Hidden"), _record("a", "a", "Hidden"),
         _record("b", "b", "A-Hidden"), _record("c", "c", "Hidden-1"),
         _record("other", "o", "Hidden")],
        [_membership("ref", "r", "same"), _membership("a", "a", "same"),
         _membership("b", "b", "same"), _membership("c", "c", "same"),
         _membership("other", "o", "different")],
    )
    status, lookup = cr.build_revit_name_lookup(root)
    rows = [
        {"domain": "d", "pattern_id": "same", "target_export_run_id": target,
         "comparison_class": "shared"}
        for target in ("a", "b", "c")
    ]
    rows.append({"domain": "d", "pattern_id": "different", "target_export_run_id": "other",
                 "comparison_class": "target_only"})
    cr.add_revit_names(rows, "ref", status, lookup, status, lookup)

    assert [(r["target_export_run_id"], r["target_revit_name"]) for r in rows[:3]] == [
        ("a", "Hidden"), ("b", "A-Hidden"), ("c", "Hidden-1")]
    assert all(r["reference_revit_name"] == "Hidden" for r in rows[:3])
    assert all(r["comparison_class"] == "shared" for r in rows[:3])
    assert rows[3]["target_revit_name"] == "Hidden"
    assert rows[3]["reference_revit_name_status"] == "missing"
    assert rows[3]["comparison_class"] == "target_only"


def test_missing_unreadable_and_ambiguous_are_explicit_and_deterministic(tmp_path):
    root = _segment(
        tmp_path,
        [_record("ref", "m", "", "placeholder_missing"),
         _record("ref", "u", "", "placeholder_unreadable"),
         _record("ref", "z", "Zulu"), _record("ref", "a", "Alpha"),
         _record("ref", "a2", "Alpha")],
        [_membership("ref", "m", "missing"), _membership("ref", "u", "unreadable"),
         _membership("ref", "z", "ambiguous"), _membership("ref", "a", "ambiguous"),
         _membership("ref", "a2", "ambiguous")],
    )
    status, lookup = cr.build_revit_name_lookup(root)
    assert status == "ok"
    assert lookup[("ref", "d", "missing")]["status"] == "missing"
    assert lookup[("ref", "d", "unreadable")]["status"] == "unreadable"
    assert lookup[("ref", "d", "ambiguous")] == {
        "name": '["Alpha","Zulu"]', "status": "ambiguous", "count": "2"
    }


def test_missing_name_source_blocks_only_name_resolution(tmp_path):
    root = tmp_path / "segment"
    status, lookup = cr.build_revit_name_lookup(root)
    row = {"domain": "d", "pattern_id": "p", "target_export_run_id": "target",
           "comparison_class": "shared", "jaccard": "1.000000"}
    cr.add_revit_names([row], "ref", status, lookup, status, lookup)
    assert row["reference_revit_name_status"] == "blocked"
    assert row["target_revit_name_status"] == "blocked"
    assert row["comparison_class"] == "shared"
    assert row["jaccard"] == "1.000000"


def test_cross_segment_identity_translation_is_descriptive_only(tmp_path):
    root = _segment(tmp_path, [_record("ref", "r", "Hidden")], [_membership("ref", "r", "local")])
    status, lookup = cr.build_revit_name_lookup(root, {"d": {"local": "join-hash"}})
    assert status == "ok"
    assert lookup[("ref", "d", "join-hash")]["name"] == "Hidden"


def test_cross_segment_different_local_ids_resolve_per_file_and_batch(tmp_path):
    reference = _segment(
        tmp_path / "reference", [_record("ref", "r", 'Hidden 1/8"')],
        [_membership("ref", "r", "pat_A")],
    )
    target = _segment(
        tmp_path / "target",
        [_record("one", "1", "A-Hidden"), _record("two", "2", "Hidden-2"),
         _record("three", "3", "Hidden-3")],
        [_membership("one", "1", "pat_B"), _membership("two", "2", "pat_B"),
         _membership("three", "3", "pat_B")],
    )
    ref_status, ref_lookup = cr.build_revit_name_lookup(reference, {"d": {"pat_A": "J1"}})
    tgt_status, tgt_lookup = cr.build_revit_name_lookup(target, {"d": {"pat_B": "J1"}})
    rows = [
        {"domain": "d", "pattern_id": "J1", "target_export_run_id": export_id,
         "comparison_class": "shared", "jaccard": "1.000000"}
        for export_id in ("one", "two", "three")
    ]
    cr.add_revit_names(rows, "ref", ref_status, ref_lookup, tgt_status, tgt_lookup)
    assert [row["reference_revit_name"] for row in rows] == ['Hidden 1/8"'] * 3
    assert [row["target_revit_name"] for row in rows] == ["A-Hidden", "Hidden-2", "Hidden-3"]
    assert all(row["comparison_class"] == "shared" and row["jaccard"] == "1.000000" for row in rows)


def test_cross_segment_same_name_different_join_hashes_remain_distinct(tmp_path):
    reference = _segment(tmp_path / "reference", [_record("ref", "r", "Hidden")],
                         [_membership("ref", "r", "pat_A")])
    target = _segment(tmp_path / "target", [_record("target", "t", "Hidden")],
                      [_membership("target", "t", "pat_B")])
    ref_status, ref_lookup = cr.build_revit_name_lookup(reference, {"d": {"pat_A": "J1"}})
    tgt_status, tgt_lookup = cr.build_revit_name_lookup(target, {"d": {"pat_B": "J2"}})
    rows = [
        {"domain": "d", "pattern_id": "J1", "target_export_run_id": "target", "comparison_class": "reference_only"},
        {"domain": "d", "pattern_id": "J2", "target_export_run_id": "target", "comparison_class": "target_only"},
    ]
    cr.add_revit_names(rows, "ref", ref_status, ref_lookup, tgt_status, tgt_lookup)
    assert [row["comparison_class"] for row in rows] == ["reference_only", "target_only"]
    assert rows[0]["reference_revit_name"] == rows[1]["target_revit_name"] == "Hidden"


def test_cross_segment_missing_and_ambiguous_names(tmp_path):
    target = _segment(
        tmp_path,
        [_record("target", "m", "", "placeholder_missing"),
         _record("target", "a", "Alpha"), _record("target", "z", "Zulu")],
        [_membership("target", "m", "missing_local"),
         _membership("target", "a", "amb_local"), _membership("target", "z", "amb_local")],
    )
    status, lookup = cr.build_revit_name_lookup(
        target, {"d": {"missing_local": "J1", "amb_local": "J2"}}
    )
    assert lookup[("target", "d", "J1")]["status"] == "missing"
    assert lookup[("target", "d", "J2")] == {
        "name": '["Alpha","Zulu"]', "status": "ambiguous", "count": "2"
    }


def test_cross_segment_semantic_changes_use_observed_names_without_rematching_identity():
    rows = [
        {"segment_id": "b", "purge_view": "all", "reference_bundle_id": "ref",
         "analysis_run_id": "run", "target_export_run_id": "target", "domain": "d",
         "population_id": "p", "pattern_id": "J1", "comparison_class": "reference_only",
         "reference_revit_name": "Hidden", "reference_revit_name_status": "ok",
         "target_revit_name": "", "target_revit_name_status": "missing"},
        {"segment_id": "b", "purge_view": "all", "reference_bundle_id": "ref",
         "analysis_run_id": "run", "target_export_run_id": "target", "domain": "d",
         "population_id": "p", "pattern_id": "J2", "comparison_class": "target_only",
         "reference_revit_name": "", "reference_revit_name_status": "missing",
         "target_revit_name": "Hidden", "target_revit_name_status": "ok"},
    ]
    semantic = cr.compute_cross_segment_semantic_changes_rows(rows)
    assert semantic == [{
        "segment_id": "b", "purge_view": "all", "reference_bundle_id": "ref",
        "analysis_run_id": "run", "target_export_run_id": "target", "domain": "d",
        "population_id": "p", "pattern_name": "Hidden", "reference_pattern_id": "J1",
        "target_pattern_id": "J2", "semantic_change_class": "changed",
        "name_match_basis": "revit_observed_label_display", "reference_revit_name": "Hidden",
        "reference_revit_name_status": "ok", "target_revit_name": "Hidden",
        "target_revit_name_status": "ok",
    }]
