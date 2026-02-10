# -*- coding: utf-8 -*-
"""
Revit-executed integration test runner (pyRevit-friendly).

How it works:
- Reads a config JSON listing RVT paths ("cases").
- For each case:
  - Opens the RVT (in Revit context)
  - Runs runner.run_dynamo.run_fingerprint(doc)
  - Writes actual JSON to tests/revit/out/<case>.actual.json
  - Compares to tests/golden/<case>.golden.json
  - Prints a bounded diff summary

Update workflow:
- Set env var REVIT_FP_UPDATE_GOLDEN=1 to overwrite goldens on diff/missing.

Environment vars:
- REVIT_FP_TEST_CONFIG: path to config.json (defaults to tests/revit/config.json beside this file)
- REVIT_FP_UPDATE_GOLDEN: "1" enables updating goldens
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from tests.revit._json_diff import compare_json, pretty_json  # noqa: E402
from core.manifest import build_manifest  # noqa: E402

def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_text(path, txt):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d)
    with open(path, "w", encoding="utf-8") as f:
        f.write(txt)


def _write_json(path, obj):
    _write_text(path, pretty_json(obj))


def _now_stamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    update_golden = os.getenv("REVIT_FP_UPDATE_GOLDEN", "").strip() == "1"
    cfg_path = os.getenv("REVIT_FP_TEST_CONFIG", "").strip() or os.path.join(THIS_DIR, "config.json")

    if not os.path.exists(cfg_path):
        raise RuntimeError("Missing config file: {}".format(cfg_path))

    cfg = _load_json(cfg_path)

    cases = cfg.get("cases", None)
    if not isinstance(cases, list) or not cases:
        raise RuntimeError("Config must contain non-empty 'cases' list. See config.example.json")

    golden_dir = cfg.get("golden_dir", os.path.join(REPO_ROOT, "tests", "golden"))
    out_dir = cfg.get("out_dir", os.path.join(THIS_DIR, "out"))
    max_diffs = int(cfg.get("max_diffs", 200))

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Import runner entrypoint (Revit-context safe)
    from runner.run_dynamo import run_fingerprint  # noqa: E402

    # Revit API access (pyRevit provides __revit__ as UIApplication)
    if "__revit__" not in globals():
        raise RuntimeError("This script must run inside Revit (pyRevit/Dynamo CPython). '__revit__' not found.")

    uiapp = globals()["__revit__"]
    app = uiapp.Application

    results = {
        "run_at": _now_stamp(),
        "config": cfg_path,
        "update_golden": update_golden,
        "cases": [],
        "summary": {"total": 0, "passed": 0, "failed": 0, "updated": 0},
    }

    for case in cases:
        name = str(case.get("name", "")).strip()
        rvt_path = str(case.get("rvt_path", "")).strip()

        if not name:
            raise RuntimeError("Case missing required 'name'.")
        if not rvt_path:
            raise RuntimeError("Case '{}' missing required 'rvt_path'.".format(name))
        if not os.path.exists(rvt_path):
            raise RuntimeError("Case '{}' RVT path does not exist: {}".format(name, rvt_path))

        results["summary"]["total"] += 1

        actual_path = os.path.join(out_dir, "{}.actual.json".format(name))
        golden_path = os.path.join(golden_dir, "{}.golden.json".format(name))
        case_rec = {"name": name, "rvt_path": rvt_path, "status": None, "notes": [], "actual": actual_path, "golden": golden_path}

        doc = None
        try:
            # Open document (simple local-path open; central/cloud variants are out of scope here)
            doc = app.OpenDocumentFile(rvt_path)

            payload = run_fingerprint(doc)
            _write_json(actual_path, payload)

            # Stable comparison artifacts
            actual_manifest = payload.get("_manifest", None)
            if not isinstance(actual_manifest, dict):
                actual_manifest = build_manifest(payload)

            actual_manifest_path = os.path.join(out_dir, "{}.manifest.actual.json".format(name))
            _write_json(actual_manifest_path, actual_manifest)
            case_rec["actual_manifest"] = actual_manifest_path

            golden_manifest_path = os.path.join(golden_dir, "{}.manifest.golden.json".format(name))
            case_rec["golden_manifest"] = golden_manifest_path

            # Back-compat: if old golden exists, compare manifests extracted from it.
            old_full_golden_exists = os.path.exists(golden_path)
            if not os.path.exists(golden_manifest_path) and old_full_golden_exists:
                try:
                    old_full_golden = _load_json(golden_path)
                    golden_manifest = old_full_golden.get("_manifest", None)
                    if not isinstance(golden_manifest, dict):
                        golden_manifest = build_manifest(old_full_golden)
                    _write_json(golden_manifest_path, golden_manifest)
                    case_rec["notes"].append("Derived manifest golden from legacy full golden: {}".format(golden_path))
                except Exception as e:
                    case_rec["notes"].append("WARNING: failed to derive manifest from legacy golden: {}".format(str(e)))

            if not os.path.exists(golden_manifest_path):
                if update_golden:
                    _write_json(golden_manifest_path, actual_manifest)
                    case_rec["status"] = "UPDATED (manifest golden created)"
                    results["summary"]["updated"] += 1
                    results["summary"]["passed"] += 1
                else:
                    case_rec["status"] = "FAILED (missing manifest golden)"
                    case_rec["notes"].append("Manifest golden missing. Re-run with REVIT_FP_UPDATE_GOLDEN=1 to create.")
                    results["summary"]["failed"] += 1
                results["cases"].append(case_rec)
                continue

            golden_manifest = _load_json(golden_manifest_path)
            equal, summary = compare_json(golden_manifest, actual_manifest, max_diffs=max_diffs)
            case_rec["compare"] = summary

            if equal:
                case_rec["status"] = "PASSED"
                results["summary"]["passed"] += 1
            else:
                if update_golden:
                    _write_json(golden_manifest_path, actual_manifest)
                    case_rec["status"] = "UPDATED (manifest golden overwritten)"
                    results["summary"]["updated"] += 1
                    results["summary"]["passed"] += 1
                else:
                    case_rec["status"] = "FAILED (manifest diff)"
                    results["summary"]["failed"] += 1

            results["cases"].append(case_rec)

            case_rec["compare"] = summary

            if equal:
                case_rec["status"] = "PASSED"
                results["summary"]["passed"] += 1
            else:
                if update_golden:
                    _write_json(golden_path, payload)
                    case_rec["status"] = "UPDATED (golden overwritten)"
                    results["summary"]["updated"] += 1
                    results["summary"]["passed"] += 1
                else:
                    case_rec["status"] = "FAILED (diff)"
                    results["summary"]["failed"] += 1

            results["cases"].append(case_rec)

        except Exception as e:
            case_rec["status"] = "FAILED (exception)"
            case_rec["error"] = str(e)
            case_rec["traceback"] = traceback.format_exc()
            results["cases"].append(case_rec)
            results["summary"]["failed"] += 1

        finally:
            try:
                if doc is not None:
                    # Always close doc we opened; don't save.
                    doc.Close(False)
            except Exception:
                # We *record* but do not raise, because runner results are still useful.
                case_rec["notes"].append("WARNING: failed to close document cleanly.")

    # Print a compact summary first (human scan)
    print("=== Revit Fingerprint Integration Tests ===")
    print("Total: {total}  Passed: {passed}  Failed: {failed}  Updated: {updated}".format(**results["summary"]))
    for c in results["cases"]:
        print("- {name}: {status}".format(**c))
        cmp_ = c.get("compare", None)
        if isinstance(cmp_, dict) and cmp_.get("equal") is False:
            diffs = cmp_.get("diffs", [])
            print("  sha256(golden)={}".format(cmp_.get("sha256_a")))
            print("  sha256(actual)={}".format(cmp_.get("sha256_b")))
            if diffs:
                print("  first diffs:")
                for d in diffs[:10]:
                    print("   * {path}: {a} -> {b}".format(path=d.get("path"), a=d.get("a"), b=d.get("b")))

    # Write machine-readable run record
    run_record_path = os.path.join(out_dir, "_run_summary.json")
    _write_json(run_record_path, results)
    print("Wrote run summary: {}".format(run_record_path))

    # Non-zero style signal: raise if any failed (pyRevit will show error)
    if results["summary"]["failed"] > 0:
        raise RuntimeError("One or more integration cases failed. See {}".format(run_record_path))


if __name__ == "__main__":
    main()
