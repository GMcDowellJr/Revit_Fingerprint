# Golden outputs

Each golden file is a deterministic JSON output for a named integration case:

- `tests/golden/<case>.golden.json`

Policy:
- Missing golden is a test failure unless the run is in update mode.
- Goldens should be updated only when a semantic change is intentional.
- Keep case names stable; treat renames as a deliberate compatibility break.

---

## Revit integration tests (golden baselines)

A Revit-executed harness (pyRevit/Dynamo CPython3) can run selected domains against known sample models and compare against golden JSON outputs.

See:
- `tests/revit/README.md`
- `tests/revit/revit_test_runner_pyrevit.py`
