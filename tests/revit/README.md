# Revit integration tests (golden baselines)

These tests are executed *inside Revit* (pyRevit or Dynamo CPython3) because the Revit API is required.

## Setup

1) Copy `config.example.json` to `config.json`
2) Edit `config.json` and point `rvt_path` to your local sample RVT(s)

> RVTs are intentionally NOT stored in the repo.

## Run

From pyRevit, execute:

- `tests/revit/revit_test_runner_pyrevit.py`

Environment variables:
- `REVIT_FP_TEST_CONFIG` (optional): full path to config.json
- `REVIT_FP_UPDATE_GOLDEN=1` (optional): creates/overwrites goldens when missing/different
- `REVIT_FINGERPRINT_HASH_MODE` (optional): `legacy` or `semantic` (runner already supports this)

Outputs:
- Actual outputs: `tests/revit/out/<case>.actual.json`
- Run summary: `tests/revit/out/_run_summary.json`
- Goldens: `tests/golden/<case>.golden.json`

## Update workflow (intentional semantic changes)

Run with:

- `REVIT_FP_UPDATE_GOLDEN=1`

This overwrites goldens *only* when explicitly enabled.
