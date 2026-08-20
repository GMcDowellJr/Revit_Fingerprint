from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_TOOLS_DIR = _REPO_ROOT / "tools"
for candidate in (str(_REPO_ROOT), str(_TOOLS_DIR)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from tools.run_extract_all import _check_governance_field_completeness


def _row(export_run_id: str, client_label: str, business_center_label: str) -> dict:
    return {
        "export_run_id": export_run_id,
        "client_label": client_label,
        "business_center_label": business_center_label,
    }


def test_blank_client_label_fails_with_export_run_id() -> None:
    rows = [_row("run-001", "", "2014")]
    with pytest.raises(SystemExit) as exc:
        _check_governance_field_completeness(rows)
    message = str(exc.value)
    assert "run-001: client_label" in message


def test_blank_business_center_label_fails_with_export_run_id() -> None:
    rows = [_row("run-002", "InternalEnterprise", "")]
    with pytest.raises(SystemExit) as exc:
        _check_governance_field_completeness(rows)
    message = str(exc.value)
    assert "run-002" in message
    assert "business_center_label" in message


def test_na_spelling_fails_same_as_blank() -> None:
    rows = [_row("run-003", "N/A", "2014"), _row("run-004", "InternalEnterprise", "not_applicable")]
    with pytest.raises(SystemExit) as exc:
        _check_governance_field_completeness(rows)
    message = str(exc.value)
    assert "run-003" in message
    assert "run-004" in message


def test_fully_populated_row_passes() -> None:
    rows = [
        _row("run-005", "InternalEnterprise", "0000"),
        _row("run-006", "ClientBeta", "2014"),
    ]
    _check_governance_field_completeness(rows)  # no raise


def test_multiple_offenders_all_reported() -> None:
    rows = [
        _row("run-007", "", ""),
        _row("run-008", "InternalEnterprise", "2014"),
        _row("run-009", "", "2014"),
    ]
    with pytest.raises(SystemExit) as exc:
        _check_governance_field_completeness(rows)
    message = str(exc.value)
    assert "run-007" in message
    assert "run-008" not in message
    assert "run-009" in message
