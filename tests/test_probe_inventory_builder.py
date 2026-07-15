import csv
import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path("tools/probes/build_probe_inventory.py")


def _run(probes_dir, out_md, out_csv, domains_dir=None):
    cmd = [
        sys.executable,
        str(SCRIPT),
        "--probes-dir", str(probes_dir),
        "--out-md", str(out_md),
        "--out-csv", str(out_csv),
    ]
    if domains_dir is not None:
        cmd += ["--domains-dir", str(domains_dir)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
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


def test_empty_probes_dir_does_not_crash(tmp_path):
    probes_dir = tmp_path / "probes"
    probes_dir.mkdir()
    out_md = tmp_path / "out.md"
    out_csv = tmp_path / "out.csv"
    _run(probes_dir, out_md, out_csv)
    rows = _read_csv_rows(out_csv)
    assert rows == []
    assert out_md.exists()
