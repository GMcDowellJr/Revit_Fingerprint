# -*- coding: utf-8 -*-
"""Tests for the layer_stacks emit type in export_to_flat_tables.py."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tools.export_to_flat_tables import main as export_main


def _write_fingerprint(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _make_layer_row(
    layer_index: int,
    is_core_boundary: bool = False,
    function: str = "Structure [1]",
    thickness_in: float = 0.5,
    material_name: str = "Concrete",
    material_class: str = "Concrete",
) -> Dict[str, Any]:
    return {
        "layer_index": layer_index,
        "is_core_boundary": is_core_boundary,
        "wl.function": function,
        "wl.thickness_in": thickness_in,
        "wl.material_name": material_name,
        "wl.material_class": material_class,
        "wl.participates_in_wrapping": False,
        "wl.structural_material": True,
        "wl.is_variable": False,
        "wl.is_structural_deck": False,
        "wl.deck_usage": None,
        "wl.deck_profile_name": None,
    }


def _make_wall_record(
    record_id: str,
    stack_hash_loose: str,
    stack_hash_strict: str,
    stack_hash_function_only: str,
    layer_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_version": "record.v2",
        "record_id": record_id,
        "domain": "wall_types",
        "status": "ok",
        "status_reasons": [],
        "sig_hash": "aabbccddeeff0011223344556677889900112233",
        "identity_quality": "complete",
        "label": {"display": record_id, "quality": "complete", "provenance": "name"},
        "identity_basis": {
            "sig_hash": "aabbccddeeff0011223344556677889900112233",
            "items": [
                {"k": "wt.stack_hash_loose", "q": "ok", "v": stack_hash_loose},
                {"k": "wt.stack_hash_strict", "q": "ok", "v": stack_hash_strict},
                {"k": "wt.stack_hash_function_only", "q": "ok", "v": stack_hash_function_only},
            ],
        },
        "layer_rows": layer_rows,
    }


def _run_export(root_dir: Path, out_dir: Path, emit: str = "layer_stacks", extra_args: List[str] = None, monkeypatch=None) -> None:
    argv = [
        "export_to_flat_tables.py",
        "--root_dir", str(root_dir),
        "--out_dir", str(out_dir),
        "--emit", emit,
    ]
    if extra_args:
        argv.extend(extra_args)
    import sys as _sys
    old_argv = _sys.argv[:]
    _sys.argv = argv
    try:
        export_main()
    finally:
        _sys.argv = old_argv


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── basic layer_stacks output ──────────────────────────────────────────────────

def test_layer_stacks_basic(tmp_path: Path) -> None:
    """Single type with simple layers emits one stack row and correct layer rows."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    layers = [
        _make_layer_row(0, is_core_boundary=True, function="Core Boundary"),
        _make_layer_row(1, function="Structure [1]", thickness_in=0.5, material_name="Concrete"),
        _make_layer_row(2, is_core_boundary=True, function="Core Boundary"),
    ]
    record = _make_wall_record("WT-001", "loosehash1", "stricthash1", "fonlyhash1", layers)
    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {"wall_types": {"domain": "wall_types", "status": "ok", "diag": {"count": 1}}}},
        "wall_types": {"records": [record]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out)

    stacks_csv = out / "layer_stacks.csv"
    rows_csv = out / "layer_stack_rows.csv"
    assert stacks_csv.exists(), "layer_stacks.csv not written"
    assert rows_csv.exists(), "layer_stack_rows.csv not written"

    stacks = _read_csv(stacks_csv)
    assert len(stacks) == 1
    s = stacks[0]
    assert s["stack_hash_loose"] == "loosehash1"
    assert s["stack_hash_strict"] == "stricthash1"
    assert s["stack_hash_function_only"] == "fonlyhash1"
    assert s["domain"] == "wall_types"
    assert s["layer_count"] == "1"  # only non-core-boundary rows
    assert float(s["total_thickness_in"]) == pytest.approx(0.5, abs=1e-4)
    assert s["type_count"] == "1"

    detail_rows = _read_csv(rows_csv)
    assert len(detail_rows) == 3  # all layers including core boundaries
    core_rows = [r for r in detail_rows if r["is_core_boundary"] == "true"]
    assert len(core_rows) == 2
    real_rows = [r for r in detail_rows if r["is_core_boundary"] != "true"]
    assert len(real_rows) == 1
    assert real_rows[0]["wl.material_name"] == "Concrete"
    assert real_rows[0]["wl.thickness_in"] == "0.5"


def test_layer_stacks_type_count_deduplication(tmp_path: Path) -> None:
    """Two types sharing the same stack_hash_loose collapse to one stack row with type_count=2."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    layers = [
        _make_layer_row(0, is_core_boundary=True, function="Core Boundary"),
        _make_layer_row(1, function="Structure [1]", thickness_in=1.0, material_name="CMU"),
        _make_layer_row(2, is_core_boundary=True, function="Core Boundary"),
    ]
    r1 = _make_wall_record("WT-A", "shared_loose", "strict_A", "fonly_A", layers)
    r2 = _make_wall_record("WT-B", "shared_loose", "strict_A", "fonly_A", layers)
    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {"wall_types": {"domain": "wall_types", "status": "ok", "diag": {"count": 2}}}},
        "wall_types": {"records": [r1, r2]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out)

    stacks = _read_csv(out / "layer_stacks.csv")
    assert len(stacks) == 1, "Two types with same loose hash should collapse to one row"
    assert stacks[0]["type_count"] == "2"

    detail_rows = _read_csv(out / "layer_stack_rows.csv")
    assert len(detail_rows) == 3, "Layer rows should be deduplicated to single stack copy"


def test_layer_stacks_multiple_domains(tmp_path: Path) -> None:
    """wall_types and floor_types each emit separate rows distinguished by domain."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    def _make_floor_record(record_id: str, loose: str) -> Dict[str, Any]:
        layers = [_make_layer_row(1, thickness_in=0.333, function="Structure [1]")]
        rec = _make_wall_record(record_id, loose, "strict_f", "fonly_f", layers)
        # floor prefix differs but key names match
        rec["domain"] = "floor_types"
        rec["identity_basis"]["items"] = [
            {"k": "ft.stack_hash_loose", "q": "ok", "v": loose},
            {"k": "ft.stack_hash_strict", "q": "ok", "v": "strict_f"},
            {"k": "ft.stack_hash_function_only", "q": "ok", "v": "fonly_f"},
        ]
        return rec

    wall_rec = _make_wall_record("WT-001", "wall_loose", "wall_strict", "wall_fo", [_make_layer_row(1, thickness_in=0.5)])
    floor_rec = _make_floor_record("FT-001", "floor_loose")

    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {
            "wall_types": {"domain": "wall_types", "status": "ok", "diag": {"count": 1}},
            "floor_types": {"domain": "floor_types", "status": "ok", "diag": {"count": 1}},
        }},
        "wall_types": {"records": [wall_rec]},
        "floor_types": {"records": [floor_rec]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out)

    stacks = _read_csv(out / "layer_stacks.csv")
    assert len(stacks) == 2
    domains = {r["domain"] for r in stacks}
    assert domains == {"wall_types", "floor_types"}


def test_layer_stacks_not_in_default_emit(tmp_path: Path) -> None:
    """layer_stacks is NOT written when --emit uses the default set."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    layers = [_make_layer_row(1, thickness_in=0.5)]
    record = _make_wall_record("WT-001", "loosehash1", "stricthash1", "fonlyhash1", layers)
    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {"wall_types": {"domain": "wall_types", "status": "ok", "diag": {"count": 1}}}},
        "wall_types": {"records": [record]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out, emit="runs,records,status_reasons,identity_items,label_components")

    assert not (out / "layer_stacks.csv").exists(), "layer_stacks.csv should not be written without layer_stacks in emit"
    assert not (out / "layer_stack_rows.csv").exists(), "layer_stack_rows.csv should not be written without layer_stacks in emit"
    assert (out / "records.csv").exists(), "records.csv should still be written"


def test_layer_stacks_split_by_domain(tmp_path: Path) -> None:
    """With --split_by_domain, per-domain layer files are written."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    layers = [_make_layer_row(1, thickness_in=0.5)]
    record = _make_wall_record("WT-001", "loosehash1", "stricthash1", "fonlyhash1", layers)
    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {"wall_types": {"domain": "wall_types", "status": "ok", "diag": {"count": 1}}}},
        "wall_types": {"records": [record]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out, emit="layer_stacks", extra_args=["--split_by_domain"])

    assert (out / "layer_stacks__wall_types.csv").exists(), "layer_stacks__wall_types.csv not written"
    assert (out / "layer_stack_rows__wall_types.csv").exists(), "layer_stack_rows__wall_types.csv not written"
    assert not (out / "layer_stacks.csv").exists(), "combined layer_stacks.csv should not exist in split mode"


def test_layer_stacks_total_thickness_excludes_core_boundary(tmp_path: Path) -> None:
    """total_thickness_in sums only non-core-boundary layers."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    layers = [
        _make_layer_row(0, is_core_boundary=True, function="Core Boundary", thickness_in=0.0),
        _make_layer_row(1, thickness_in=0.25),
        _make_layer_row(2, thickness_in=0.75),
        _make_layer_row(3, is_core_boundary=True, function="Core Boundary", thickness_in=0.0),
    ]
    record = _make_wall_record("WT-001", "lh", "sh", "foh", layers)
    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {"wall_types": {"domain": "wall_types", "status": "ok", "diag": {"count": 1}}}},
        "wall_types": {"records": [record]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out)

    stacks = _read_csv(out / "layer_stacks.csv")
    assert len(stacks) == 1
    assert stacks[0]["layer_count"] == "2"
    assert float(stacks[0]["total_thickness_in"]) == pytest.approx(1.0, abs=1e-4)


def test_layer_stacks_records_without_layer_rows_ignored(tmp_path: Path) -> None:
    """Records without layer_rows (e.g., identity domain) do not produce stack rows."""
    exports = tmp_path / "exports"
    exports.mkdir()
    out = tmp_path / "out"

    payload = {
        "_contract": {"schema_version": "record.v2", "run_status": "ok", "domains": {
            "units": {"domain": "units", "status": "ok", "diag": {"count": 1}},
        }},
        "units": {"records": [{"schema_version": "record.v2", "record_id": "U1", "status": "ok", "status_reasons": [], "sig_hash": "aabb", "identity_quality": "complete", "identity_basis": {"items": []}}]},
    }
    _write_fingerprint(exports / "fp__test__001__fingerprint.json", payload)

    _run_export(exports, out)

    stacks = _read_csv(out / "layer_stacks.csv")
    detail_rows = _read_csv(out / "layer_stack_rows.csv")
    assert len(stacks) == 0, "Non-compound records should not produce stack rows"
    assert len(detail_rows) == 0, "Non-compound records should not produce layer detail rows"


import pytest
