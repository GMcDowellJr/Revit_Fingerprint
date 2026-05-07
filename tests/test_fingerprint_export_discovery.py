from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tools.patterns_analysis._archive.io import load_exports
from tools.run_extract_all import (
    _detect_surfaces,
    _discover_domains_from_exports,
    _infer_domains,
    _pick_sample_file,
)
from tools.extractor import _iter_export_files


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _fingerprint_payload(*domains: str) -> dict:
    contract_domains = {
        domain: {"domain": domain, "status": "ok", "diag": {"count": 1}}
        for domain in domains
    }
    payload = {"_contract": {"domains": contract_domains}}
    for domain in domains:
        payload[domain] = {"records": [{"record_id": f"{domain}-1"}]}
    return payload


def test_iter_export_files_prioritizes_fingerprint_and_uses_none_secondary(tmp_path: Path) -> None:
    _write_json(tmp_path / "zeta.report.json", {"status": "ok"})
    _write_json(tmp_path / "legacy.index.json", {"_contract": {"domains": {}}})
    _write_json(tmp_path / "legacy.details.json", {"units": {"records": []}})
    _write_json(tmp_path / "fp__alpha__001__fingerprint.json", _fingerprint_payload("units"))
    _write_json(tmp_path / "fp__beta__002__fingerprint.json", _fingerprint_payload("arrowheads"))

    routed = _iter_export_files(tmp_path)

    assert [entry[1].name for entry in routed[:2]] == [
        "fp__alpha__001__fingerprint.json",
        "fp__beta__002__fingerprint.json",
    ]
    assert all(entry[2] is None for entry in routed[:2])
    assert routed[2][1].name == "legacy.index.json"
    assert routed[2][2] is not None and routed[2][2].name == "legacy.details.json"
    assert routed[3][1].name == "zeta.report.json"
    assert routed[3][2] is None


def test_pick_sample_file_prefers_fingerprint_and_falls_back_to_split(tmp_path: Path) -> None:
    fingerprint = tmp_path / "fp__sample__001__fingerprint.json"
    _write_json(fingerprint, _fingerprint_payload("dimension_types_linear"))
    _write_json(tmp_path / "old.index.json", {"_contract": {"domains": {}}})
    _write_json(tmp_path / "old.details.json", {"units": {"records": []}})

    primary, secondary = _pick_sample_file(tmp_path)

    assert primary == fingerprint
    assert secondary is None

    only_split_dir = tmp_path / "split_only"
    only_split_dir.mkdir()
    index = only_split_dir / "old.index.json"
    details = only_split_dir / "old.details.json"
    _write_json(index, {"_contract": {"domains": {}}})
    _write_json(details, {"units": {"records": []}})

    primary, secondary = _pick_sample_file(only_split_dir)

    assert primary == index
    assert secondary == details


def test_detect_surfaces_counts_fingerprint_separately(tmp_path: Path) -> None:
    _write_json(tmp_path / "fp__one__001__fingerprint.json", _fingerprint_payload("units"))
    _write_json(tmp_path / "fp__two__002__fingerprint.json", _fingerprint_payload("arrowheads"))

    surfaces = _detect_surfaces(tmp_path)

    assert surfaces["fingerprint_json"] == 2
    assert surfaces["plain_json"] == 0
    assert surfaces["total_json"] == 2


def test_domain_discovery_prefers_fingerprint_candidates(tmp_path: Path) -> None:
    _write_json(tmp_path / "report.json", {"artifacts": {"records": []}, "junk": {"value": 1}})
    _write_json(
        tmp_path / "fp__project__001__fingerprint.json",
        _fingerprint_payload("dimension_types_linear", "dimension_types_angular", "arrowheads"),
    )

    discovered = _discover_domains_from_exports(tmp_path)
    inferred = _infer_domains(tmp_path)

    assert discovered == ["arrowheads", "dimension_types_angular", "dimension_types_linear"]
    assert inferred == ["arrowheads", "dimension_types_angular", "dimension_types_linear"]


def test_load_exports_prefers_fingerprint_files_before_plain_fallback(tmp_path: Path) -> None:
    _write_json(tmp_path / "zeta.report.json", {"status": "ok"})
    _write_json(tmp_path / "fp__alpha__001__fingerprint.json", _fingerprint_payload("units"))
    _write_json(tmp_path / "fp__beta__002__fingerprint.json", _fingerprint_payload("arrowheads"))

    exports = load_exports(str(tmp_path))

    assert [Path(exp.path).name for exp in exports] == [
        "fp__alpha__001__fingerprint.json",
        "fp__beta__002__fingerprint.json",
        "zeta.report.json",
    ]


def test_analyze2_runs_dimension_types_by_family_once(tmp_path: Path, monkeypatch, capsys) -> None:
    out_root = tmp_path / "out"
    exports_dir = tmp_path / "exports"
    exports_dir.mkdir()
    _write_json(exports_dir / "fp__project__001__fingerprint.json", _fingerprint_payload("dimension_types_linear"))

    commands = []

    monkeypatch.setattr("tools.run_extract_all._detect_surfaces", lambda _: {"fingerprint_json": 1, "plain_json": 0, "details": 0, "index": 0, "legacy": 0, "total_json": 1})
    monkeypatch.setattr("tools.run_extract_all.emit_analysis_v21", lambda *args, **kwargs: "analysis-run")
    monkeypatch.setattr("tools.run_extract_all._run", lambda cmd, env: commands.append(cmd))

    argv = [
        "run_extract_all.py",
        str(exports_dir),
        "--out-root",
        str(out_root),
        "--stages",
        "analyze2",
        "--emit-legacy",
        "--domains",
        "dimension_types_linear,dimension_types_angular,dimension_types_radial",
        "--no-require-join-policy",
    ]
    monkeypatch.setattr(sys, "argv", argv)

    from tools import run_extract_all

    run_extract_all.main()
    captured = capsys.readouterr()

    dimtype_calls = [cmd for cmd in commands if "tools.phase2_analysis.run_dimension_types_by_family" in cmd]
    assert len(dimtype_calls) == 1
    assert "Invoking run_dimension_types_by_family once" in captured.err
