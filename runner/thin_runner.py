import sys
import os
import traceback

# Explicitly request semantic (v2) hashing
os.environ["REVIT_FINGERPRINT_HASH_MODE"] = "semantic"

import importlib

sys.dont_write_bytecode = True

# Provide output path to imported runner via env var (import boundary safe)
try:
    if IN is not None and len(IN) > 0 and IN[0] is not None and str(IN[0]).strip():
        os.environ["REVIT_FINGERPRINT_OUTPUT_PATH"] = str(IN[0]).strip()
    else:
        os.environ.pop("REVIT_FINGERPRINT_OUTPUT_PATH", None)
except Exception as e:
    os.environ.pop("REVIT_FINGERPRINT_OUTPUT_PATH", None)

# Optional: IN[1] == True forces full OUT (debug/back-compat)
try:
    if IN is not None and len(IN) > 1 and bool(IN[1]) is True:
        os.environ["REVIT_FINGERPRINT_FORCE_FULL_OUT"] = "1"
    else:
        os.environ.pop("REVIT_FINGERPRINT_FORCE_FULL_OUT", None)
except Exception as e:
    os.environ.pop("REVIT_FINGERPRINT_FORCE_FULL_OUT", None)

# MUST be the repo root that contains: core/, domains/, runner/
REPO_DIR = r"C:\Users\gmcdowell\Documents\Revit_Fingerprint"

# Basic validation to catch the most common path mistake
expected = [
    os.path.join(REPO_DIR, "runner", "run_dynamo.py"),
    os.path.join(REPO_DIR, "domains"),
    os.path.join(REPO_DIR, "core"),
]
missing = [p for p in expected if not os.path.exists(p)]
if missing:
    OUT = {
        "error": "REPO_DIR does not look like the repo root (missing expected paths).",
        "REPO_DIR": REPO_DIR,
        "missing": missing,
    }
else:
    # Ensure this repo wins import resolution
    if REPO_DIR in sys.path:
        sys.path.remove(REPO_DIR)
    sys.path.insert(0, REPO_DIR)

    try:
        # ---- CPython 3: purge cached modules so edits on disk are picked up ----
        # Only purge the repo's packages to avoid destabilizing stdlib / Dynamo internals.
        prefixes = ("runner", "domains", "core")

        for name in list(sys.modules.keys()):
            if name in prefixes or name.startswith("runner.") or name.startswith("domains.") or name.startswith("core."):
                sys.modules.pop(name, None)

        # Import triggers execution in this repo (run_dynamo computes OUT at import time)
        exporter = importlib.import_module("runner.run_dynamo")

        # Forward the computed OUT from the runner module
        OUT = exporter.OUT

    except Exception as e:
        OUT = {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "REPO_DIR": REPO_DIR,
            "sys_path_head": sys.path[:8],
            "exporter_file": getattr(sys.modules.get("runner.run_dynamo", None), "__file__", None),
        }
