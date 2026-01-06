# tests/test_contracts_run_status.py

import pytest

from core.contracts import (
    SCHEMA_VERSION,
    compute_run_status,
    new_domain_envelope,
    new_run_diag,
    new_run_envelope,
    RUN_STATUS_OK,
    RUN_STATUS_DEGRADED,
    RUN_STATUS_FAILED,
    DOMAIN_STATUS_OK,
    DOMAIN_STATUS_DEGRADED,
    DOMAIN_STATUS_BLOCKED,
    DOMAIN_STATUS_FAILED,
    DOMAIN_STATUS_UNSUPPORTED,
)


def _env(name: str, status: str):
    return new_domain_envelope(
        domain=name,
        domain_version="1.0",
        status=status,
        block_reasons=[],
        diag={},
        records=None,
        hash_value="abc" if status == DOMAIN_STATUS_OK else None,
    )


def test_run_status_all_ok():
    domains = {
        "units": _env("units", DOMAIN_STATUS_OK),
        "line_patterns": _env("line_patterns", DOMAIN_STATUS_OK),
    }
    run_status, run_diag = compute_run_status(domains)
    assert run_status == RUN_STATUS_OK
    assert run_diag["counters"]["domain_ok"] == 2
    assert run_diag["counters"]["domain_failed"] == 0


def test_run_status_degraded_if_any_degraded():
    domains = {
        "units": _env("units", DOMAIN_STATUS_OK),
        "line_patterns": _env("line_patterns", DOMAIN_STATUS_DEGRADED),
    }
    run_status, _ = compute_run_status(domains)
    assert run_status == RUN_STATUS_DEGRADED


def test_run_status_degraded_if_any_blocked():
    domains = {
        "units": _env("units", DOMAIN_STATUS_OK),
        "views": _env("views", DOMAIN_STATUS_BLOCKED),
    }
    run_status, _ = compute_run_status(domains)
    assert run_status == RUN_STATUS_DEGRADED


def test_run_status_degraded_if_any_unsupported():
    domains = {
        "units": _env("units", DOMAIN_STATUS_OK),
        "phase_graphics": _env("phase_graphics", DOMAIN_STATUS_UNSUPPORTED),
    }
    run_status, _ = compute_run_status(domains)
    assert run_status == RUN_STATUS_DEGRADED


def test_run_status_failed_if_any_failed():
    domains = {
        "units": _env("units", DOMAIN_STATUS_OK),
        "categories": _env("categories", DOMAIN_STATUS_FAILED),
    }
    run_status, run_diag = compute_run_status(domains)
    assert run_status == RUN_STATUS_FAILED
    assert run_diag["counters"]["domain_failed"] == 1


def test_invalid_domain_status_counts_as_failed_and_records_error():
    domains = {
        "units": _env("units", DOMAIN_STATUS_OK),
        "bad_domain": {"domain": "bad_domain", "domain_version": "1.0"},  # no status
    }
    run_status, run_diag = compute_run_status(domains, base_run_diag=new_run_diag())
    assert run_status == RUN_STATUS_FAILED
    assert run_diag["counters"]["domain_failed"] == 1
    assert len(run_diag["errors"]) >= 1
    assert run_diag["errors"][0]["code"] == "invalid_domain_status"


def test_new_run_envelope_rejects_mismatched_version():
    domains = {"units": _env("units", DOMAIN_STATUS_OK)}
    run_status, run_diag = compute_run_status(domains)
    with pytest.raises(ValueError):
        new_run_envelope(
            schema_version="999.0",
            run_status=run_status,
            run_diag=run_diag,
            domains=domains,
        )


def test_new_run_envelope_accepts_current_version():
    domains = {"units": _env("units", DOMAIN_STATUS_OK)}
    run_status, run_diag = compute_run_status(domains)
    env = new_run_envelope(
        schema_version=SCHEMA_VERSION,
        run_status=run_status,
        run_diag=run_diag,
        domains=domains,
    )
    assert env["schema_version"] == SCHEMA_VERSION
    assert env["run_status"] == RUN_STATUS_OK
    assert "domains" in env
