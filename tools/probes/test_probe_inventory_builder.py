import csv
import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path("tools/probes/build_probe_inventory.py")


def _run(probes_dir, out_md, out_csv, domains_dir=None, force=False, expect_returncode=0,
         out_crosswalk_md=None, out_crosswalk_csv=None):
    cmd = [
        sys.executable,
        str(SCRIPT),
        "--probes-dir", str(probes_dir),
        "--out-md", str(out_md),
        "--out-csv", str(out_csv),
    ]
    if domains_dir is not None:
        cmd += ["--domains-dir", str(domains_dir)]
    if force:
        cmd += ["--force"]
    if out_crosswalk_md is not None:
        cmd += ["--out-crosswalk-md", str(out_crosswalk_md)]
    if out_crosswalk_csv is not None:
        cmd += ["--out-crosswalk-csv", str(out_crosswalk_csv)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == expect_returncode, proc.stderr
    return proc


def _read_csv_rows(out_csv):
    with open(out_csv, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_merges_and_dedupes_across_dated_runs(tmp_path):
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    domains_dir = tmp_path / "domains"
    domains_dir.mkdir()
    (domains_dir / "widgets.py").write_text("# stub\n", encoding="utf-8")
    (domains_dir / "gadgets.py").write_text("# stub\n", encoding="utf-8")

    run1 = [
        {
            "kind": "inventory",
            "domain": "widgets",
            "records": [
                {
                    "domain": "widgets",
                    "param_key": "p.Foo",
                    "example": {"q": "missing", "storage": "String", "raw": None, "display": None, "norm": None},
                    "observed": {
                        "storage_types": ["String"],
                        "q_counts": {"ok": 0, "missing": 1, "unreadable": 0, "unsupported": 0},
                        "unique_value_count": 1,
                    },
                }
            ],
        },
        {"kind": "crosswalk", "domain": "widgets", "records": [{"a": 1}, {"a": 2}]},
    ]
    run2 = [
        {
            "kind": "inventory",
            "domain": "widgets",
            "records": [
                {
                    "domain": "widgets",
                    "param_key": "p.Foo",
                    "example": {"q": "ok", "storage": "String", "raw": "hi", "display": "hi", "norm": "hi"},
                    "observed": {
                        "storage_types": ["String"],
                        "q_counts": {"ok": 3, "missing": 0, "unreadable": 0, "unsupported": 0},
                        "unique_value_count": 2,
                    },
                },
                {
                    "domain": "widgets",
                    "param_key": "p.Bar",
                    "example": {"q": "ok", "storage": "Integer", "raw": 5, "display": "5", "norm": 5},
                    "observed": {
                        "storage_types": ["Integer"],
                        "q_counts": {"ok": 1, "missing": 0, "unreadable": 0, "unsupported": 0},
                        "unique_value_count": 1,
                    },
                },
            ],
        },
        {
            "kind": "reflection",
            "domain": "widgets",
            "records": [
                {
                    "domain": "widgets",
                    "member_key": "refl.Category.SubCategories",
                    "member_kind": "property",
                    "type_label": "Category",
                    "example": {"q": "ok", "storage": "None", "raw": None, "display": "3 items", "norm": "3 items"},
                    "observed": {"ok_count": 1, "error_count": 0, "unique_value_count": 1},
                }
            ],
        },
    ]

    (probes_dir / "probe_widgets_2026-01-01.json").write_text(json.dumps(run1), encoding="utf-8")
    (probes_dir / "probe_widgets_2026-02-01.json").write_text(json.dumps(run2), encoding="utf-8")
    # Malformed filename (no date) -- should be skipped, not crash the run.
    (probes_dir / "probe_bad.json").write_text("[]", encoding="utf-8")

    out_md = tmp_path / "out" / "PROBE_INVENTORY.md"
    out_csv = tmp_path / "out" / "PROBE_INVENTORY.csv"
    _run(probes_dir, out_md, out_csv, domains_dir=domains_dir)

    rows = _read_csv_rows(out_csv)
    by_key = {(r["key_kind"], r["key"]): r for r in rows}

    assert ("param", "p.Foo") in by_key
    assert ("param", "p.Bar") in by_key
    assert ("reflection", "refl.Category.SubCategories") in by_key

    foo = by_key[("param", "p.Foo")]
    # Representative example must be the higher-scored ("ok", with display/raw/norm) one.
    assert foo["example_q"] == "ok"
    assert foo["example_display"] == "hi"
    assert foo["run_count"] == "2"
    assert foo["q_counts"] == "missing=1;ok=3;unreadable=0;unsupported=0"

    bar = by_key[("param", "p.Bar")]
    assert bar["run_count"] == "1"

    md_text = out_md.read_text(encoding="utf-8")
    assert "## domain — `widgets`" in md_text
    assert "p.Foo" in md_text
    assert "p.Bar" in md_text
    assert "refl.Category.SubCategories" in md_text
    # gadgets.py has no probe JSON at all -> should show up in the coverage gap list.
    assert "`gadgets`" in md_text
    assert "probe_bad.json" in md_text


def test_empty_probes_dir_refuses_to_overwrite_by_default(tmp_path):
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    out_md = tmp_path / "out.md"
    out_csv = tmp_path / "out.csv"
    # Simulate a previously-populated inventory sitting at the output paths --
    # a no-input run must not silently clobber it (PR #358 review comment).
    out_md.write_text("# populated\n", encoding="utf-8")
    out_csv.write_text("domain,key_kind,key\nwidgets,param,p.Foo\n", encoding="utf-8")

    proc = _run(probes_dir, out_md, out_csv, expect_returncode=1)
    assert "Refused" in proc.stdout

    # Untouched -- still the "populated" content from before the run.
    assert out_md.read_text(encoding="utf-8") == "# populated\n"
    rows = _read_csv_rows(out_csv)
    assert rows == [{"domain": "widgets", "key_kind": "param", "key": "p.Foo"}]


def test_all_inputs_invalid_refuses_to_overwrite_by_default(tmp_path):
    # Files match the expected naming pattern but none of them parse into
    # usable domain data (e.g. truncated/malformed JSON, or valid JSON in
    # the wrong shape) -- this must refuse the same way as the zero-files
    # case, not silently clobber the inventory with empty output.
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    (probes_dir / "probes_2025_bad.json").write_text("{not valid json", encoding="utf-8")
    (probes_dir / "probe_widgets_2026-01-01.json").write_text('{"oops": "not a list"}', encoding="utf-8")

    out_md = tmp_path / "out.md"
    out_csv = tmp_path / "out.csv"
    out_md.write_text("# populated\n", encoding="utf-8")
    out_csv.write_text("domain,key_kind,key\nwidgets,param,p.Foo\n", encoding="utf-8")

    proc = _run(probes_dir, out_md, out_csv, expect_returncode=1)
    assert "Refused" in proc.stdout
    assert "2 input file(s) matched" in proc.stdout

    assert out_md.read_text(encoding="utf-8") == "# populated\n"
    rows = _read_csv_rows(out_csv)
    assert rows == [{"domain": "widgets", "key_kind": "param", "key": "p.Foo"}]


def test_empty_probes_dir_with_force_writes_empty_inventory(tmp_path):
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    out_md = tmp_path / "out.md"
    out_csv = tmp_path / "out.csv"
    _run(probes_dir, out_md, out_csv, force=True)
    rows = _read_csv_rows(out_csv)
    assert rows == []
    assert out_md.exists()


def _run_shaped_payload(revit_version, run_id, extraction_date, domains):
    return {
        "run_metadata": {
            "run_id": run_id,
            "extraction_date": extraction_date,
            "revit_version": revit_version,
            "tool_version": "1.2.3",
            "document": {"title": "sample.rvt", "path_name": None, "is_workshared": False},
            "source": "thin_runner",
            "probes_run": sorted(domains.keys()),
        },
        "domains": domains,
    }


def test_merges_run_shaped_files_and_tracks_revit_version(tmp_path):
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()

    run_a = _run_shaped_payload(
        "2025", "20260101T000000-aaaaaa", "2026-01-01T00:00:00",
        {
            "sprockets": [
                {
                    "kind": "inventory",
                    "domain": "sprockets",
                    "records": [
                        {
                            "domain": "sprockets",
                            "param_key": "p.Size",
                            "example": {"q": "missing", "storage": "Integer", "raw": None, "display": None, "norm": None},
                            "observed": {"storage_types": ["Integer"], "q_counts": {"missing": 1}, "unique_value_count": 1},
                        }
                    ],
                }
            ]
        },
    )
    run_b = _run_shaped_payload(
        "2026", "20260201T000000-bbbbbb", "2026-02-01T00:00:00",
        {
            "sprockets": [
                {
                    "kind": "inventory",
                    "domain": "sprockets",
                    "records": [
                        {
                            "domain": "sprockets",
                            "param_key": "p.Size",
                            "example": {"q": "ok", "storage": "Integer", "raw": 4, "display": "4", "norm": 4},
                            "observed": {"storage_types": ["Integer"], "q_counts": {"ok": 1}, "unique_value_count": 1},
                        }
                    ],
                }
            ]
        },
    )
    (probes_dir / "probes_2025_20260101T000000-aaaaaa.json").write_text(json.dumps(run_a), encoding="utf-8")
    (probes_dir / "probes_2026_20260201T000000-bbbbbb.json").write_text(json.dumps(run_b), encoding="utf-8")

    out_md = tmp_path / "out" / "PROBE_INVENTORY.md"
    out_csv = tmp_path / "out" / "PROBE_INVENTORY.csv"
    _run(probes_dir, out_md, out_csv)

    rows = _read_csv_rows(out_csv)
    assert len(rows) == 1
    row = rows[0]
    assert row["domain"] == "sprockets"
    assert row["key"] == "p.Size"
    assert row["example_q"] == "ok"  # higher-scored example wins across runs
    assert row["run_count"] == "2"
    assert set(row["revit_versions_seen"].split(";")) == {"2025", "2026"}

    md_text = out_md.read_text(encoding="utf-8")
    assert "## domain — `sprockets`" in md_text
    assert "revit_versions_seen" in md_text


def test_merges_across_legacy_and_run_shapes_for_same_domain(tmp_path):
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()

    legacy = [
        {
            "kind": "inventory",
            "domain": "gizmos",
            "records": [
                {
                    "domain": "gizmos",
                    "param_key": "p.Color",
                    "example": {"q": "ok", "storage": "String", "raw": "Red", "display": "Red", "norm": "Red"},
                    "observed": {"storage_types": ["String"], "q_counts": {"ok": 1}, "unique_value_count": 1},
                }
            ],
        }
    ]
    (probes_dir / "probe_gizmos_2025-06-01.json").write_text(json.dumps(legacy), encoding="utf-8")

    run = _run_shaped_payload(
        "2025", "20260301T000000-cccccc", "2026-03-01T00:00:00",
        {
            "gizmos": [
                {
                    "kind": "inventory",
                    "domain": "gizmos",
                    "records": [
                        {
                            "domain": "gizmos",
                            "param_key": "p.Weight",
                            "example": {"q": "ok", "storage": "Double", "raw": 1.5, "display": "1.5", "norm": 1.5},
                            "observed": {"storage_types": ["Double"], "q_counts": {"ok": 1}, "unique_value_count": 1},
                        }
                    ],
                }
            ]
        },
    )
    (probes_dir / "probes_2025_20260301T000000-cccccc.json").write_text(json.dumps(run), encoding="utf-8")

    out_md = tmp_path / "out" / "PROBE_INVENTORY.md"
    out_csv = tmp_path / "out" / "PROBE_INVENTORY.csv"
    result_proc = _run(probes_dir, out_md, out_csv)
    assert "run files matched    : 1" in result_proc.stdout
    assert "legacy files matched : 1" in result_proc.stdout

    rows = _read_csv_rows(out_csv)
    keys = {(r["domain"], r["key"]) for r in rows}
    assert ("gizmos", "p.Color") in keys
    assert ("gizmos", "p.Weight") in keys


def test_crosswalk_column_profile_across_runs(tmp_path):
    """Concrete, hand-traceable acceptance numbers for the crosswalk report,
    across two runs for a domain ("gadgets") that has ONLY crosswalk records
    -- no inventory/reflection at all -- to also confirm that doesn't trip
    the empty-rebuild refusal.

    Row layout (5 rows total across both runs):
      run1: {a.id: 1, a.class: "Glass", b.id: 10}
            {a.id: 2, a.class: "Metal", b.id: None}
            {a.id: 3, a.class: None,    b.id: 30}
      run2: {a.id: 4, a.class: "Glass"}                # b.id key absent entirely
            {a.id: 5, a.class: "Wood",  b.id: 50}

    Expected per column, over 5 total rows:
      a.id:    present=5/5, non_null=5, resolution=100.0%, unique=5 (1..5)
      a.class: present=5/5, non_null=4, resolution=80.0%,  unique=3 (Glass/Metal/Wood)
      b.id:    present=4/5 (absent in run2 row1), non_null=3, resolution=75.0%, unique=3 (10/30/50)
    """
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()

    run1 = _run_shaped_payload(
        "2025", "20260401T000000-dddddd", "2026-04-01T00:00:00",
        {
            "gadgets": [
                {
                    "kind": "crosswalk",
                    "domain": "gadgets",
                    "records": [
                        {"a.id": 1, "a.class": "Glass", "b.id": 10},
                        {"a.id": 2, "a.class": "Metal", "b.id": None},
                        {"a.id": 3, "a.class": None, "b.id": 30},
                    ],
                }
            ]
        },
    )
    (probes_dir / "probes_2025_20260401T000000-dddddd.json").write_text(json.dumps(run1), encoding="utf-8")

    run2 = _run_shaped_payload(
        "2025", "20260402T000000-eeeeee", "2026-04-02T00:00:00",
        {
            "gadgets": [
                {
                    "kind": "crosswalk",
                    "domain": "gadgets",
                    "records": [
                        {"a.id": 4, "a.class": "Glass"},
                        {"a.id": 5, "a.class": "Wood", "b.id": 50},
                    ],
                }
            ]
        },
    )
    (probes_dir / "probes_2025_20260402T000000-eeeeee.json").write_text(json.dumps(run2), encoding="utf-8")

    out_md = tmp_path / "out" / "PROBE_INVENTORY.md"
    out_csv = tmp_path / "out" / "PROBE_INVENTORY.csv"
    out_cw_md = tmp_path / "out" / "PROBE_CROSSWALK.md"
    out_cw_csv = tmp_path / "out" / "PROBE_CROSSWALK.csv"
    proc = _run(probes_dir, out_md, out_csv, out_crosswalk_md=out_cw_md, out_crosswalk_csv=out_cw_csv)
    assert "crosswalk domains    : 1" in proc.stdout

    # gadgets has zero inventory/reflection rows (crosswalk-only) but the
    # run must still succeed rather than tripping the empty-rebuild guard.
    inv_rows = _read_csv_rows(out_csv)
    assert not [r for r in inv_rows if r["domain"] == "gadgets"]

    # ...and still shows up in PROBE_INVENTORY.md's summary table with its
    # crosswalk row count, since that table is diagnostics-driven, not
    # domains-driven.
    assert "`gadgets`" in out_md.read_text(encoding="utf-8")

    cw_rows = {r["column"]: r for r in _read_csv_rows(out_cw_csv) if r["domain"] == "gadgets"}
    assert set(cw_rows.keys()) == {"a.id", "a.class", "b.id"}

    assert cw_rows["a.id"]["rows_in_domain"] == "5"
    assert cw_rows["a.id"]["present_count"] == "5"
    assert cw_rows["a.id"]["non_null_count"] == "5"
    assert cw_rows["a.id"]["resolution_rate"] == "100.0%"
    assert cw_rows["a.id"]["unique_value_count"] == "5"

    assert cw_rows["a.class"]["present_count"] == "5"
    assert cw_rows["a.class"]["non_null_count"] == "4"
    assert cw_rows["a.class"]["null_count"] == "1"
    assert cw_rows["a.class"]["resolution_rate"] == "80.0%"
    assert cw_rows["a.class"]["unique_value_count"] == "3"

    assert cw_rows["b.id"]["present_count"] == "4"
    assert cw_rows["b.id"]["present_rate"] == "80.0%"
    assert cw_rows["b.id"]["non_null_count"] == "3"
    assert cw_rows["b.id"]["null_count"] == "1"
    assert cw_rows["b.id"]["resolution_rate"] == "75.0%"
    assert cw_rows["b.id"]["unique_value_count"] == "3"
    assert cw_rows["b.id"]["run_count"] == "2"

    cw_md_text = out_cw_md.read_text(encoding="utf-8")
    assert "`gadgets`" in cw_md_text
    assert "`a.id`" in cw_md_text
    assert "`b.id`" in cw_md_text
