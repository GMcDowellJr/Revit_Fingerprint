# tests/test_contracts_bounded_errors.py

from core.contracts import add_bounded_error, new_run_diag


def test_bounded_errors_caps_and_counts_dropped():
    run_diag = new_run_diag()
    cap = 5

    for i in range(25):
        add_bounded_error(
            run_diag,
            domain="units",
            status="failed",
            code="boom",
            message=f"err {i}",
            cap=cap,
        )

    assert len(run_diag["errors"]) == cap
    # 25 total attempts - 5 stored = 20 dropped
    assert run_diag["counters"]["errors_dropped"] == 20


def test_bounded_errors_defensive_cap_nonpositive():
    run_diag = new_run_diag()
    add_bounded_error(
        run_diag,
        domain="units",
        status="failed",
        code="boom",
        message="err",
        cap=0,
    )
    assert len(run_diag["errors"]) == 1
