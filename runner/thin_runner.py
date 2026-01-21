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

# Optional: control which output surfaces are written
# Values: "all", "minimal", or comma list e.g. "payload,manifest"
try:
    os.environ["REVIT_FINGERPRINT_OUTPUT_SURFACES"] = "payload"
except Exception:
    pass

# Optional: IN[1] == True forces full OUT (debug/back-compat)
try:
    if IN is not None and len(IN) > 1 and bool(IN[1]) is True:
        os.environ["REVIT_FINGERPRINT_FORCE_FULL_OUT"] = "1"
    else:
        os.environ.pop("REVIT_FINGERPRINT_FORCE_FULL_OUT", None)
except Exception as e:
    os.environ.pop("REVIT_FINGERPRINT_FORCE_FULL_OUT", None)

# Optional: IN[2] controls whether a timestamp is appended to output filenames
#  - True / 1 / "true"  => stamp ON  (REVIT_FINGERPRINT_FILENAME_STAMP=1)
#  - False / 0 / "false" => stamp OFF (REVIT_FINGERPRINT_FILENAME_STAMP=0)
#  - Missing/None => stamp OFF (safer for batch determinism)
#  - Unparseable => stamp OFF + warning
_thinrunner_warnings = []

# Install path segments (centralized)
# Override REVIT_FINGERPRINT_ORG_DIR to change "Company" without editing the graph.
try:
    ORG_DIR = str(os.environ.get("REVIT_FINGERPRINT_ORG_DIR", "Company")).strip() or "Company"
except Exception:
    ORG_DIR = "Stantec"

APP_DIR = "RevitFingerprint"
CHANNEL_DIR = "current"

def _parse_boolish(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    try:
        # Dynamo sometimes gives int-like values
        if isinstance(v, (int, float)) and int(v) == v:
            return bool(int(v))
    except Exception:
        pass
    try:
        s = str(v).strip()
    except Exception:
        return None
    if not s:
        return None
    sl = s.lower()
    if sl in ("1", "true", "t", "yes", "y", "on"):
        return True
    if sl in ("0", "false", "f", "no", "n", "off"):
        return False
    return None

try:
    raw_in2 = None
    have_in2 = (IN is not None and len(IN) > 2)
    if have_in2:
        raw_in2 = IN[2]

    stamp_choice = _parse_boolish(raw_in2) if have_in2 else None

    if stamp_choice is True:
        os.environ["REVIT_FINGERPRINT_FILENAME_STAMP"] = "1"
    elif stamp_choice is False:
        os.environ["REVIT_FINGERPRINT_FILENAME_STAMP"] = "0"
    else:
        # Missing/None/unparseable => default OFF
        os.environ["REVIT_FINGERPRINT_FILENAME_STAMP"] = "0"
        if have_in2 and raw_in2 is not None:
            _thinrunner_warnings.append(
                "IN[2] unparseable for timestamp flag; defaulting REVIT_FINGERPRINT_FILENAME_STAMP=0"
            )
except Exception:
    # Default OFF on any thinrunner failure here
    os.environ["REVIT_FINGERPRINT_FILENAME_STAMP"] = "0"

# MUST be the repo root that contains: core/, domains/, runner/
# Dynamo-node safe behavior:
#  - No __file__ reliance (this code is pasted into a Dynamo Python node)
#  - Repo root is discovered from env var or conventional local install paths
#  - Hard block if repo appears to be on SharePoint/OneDrive sync or UNC/network path

def _looks_like_unc_path(p):
    try:
        s = str(p)
    except Exception:
        return False
    return s.startswith("\\\\")

def _is_probably_sync_path(p):
    """
    Heuristic, Windows-centric: block common SharePoint/OneDrive sync roots.
    We block (not degrade) because runtime location affects determinism.
    """
    try:
        s = os.path.abspath(str(p))
    except Exception:
        return False

    sl = s.lower()

    # Common sync markers
    for m in ("\\onedrive\\", "\\sharepoint\\", "\\microsoft teams\\"):
        if m in sl:
            return True

    # Some orgs sync SharePoint under Documents with tenant/library naming
    if "\\documents\\" in sl and ("- sharepoint" in sl or "sharepoint" in sl):
        return True

    return False

def _is_repo_root(p):
    expected_local = [
        os.path.join(p, "runner", "run_dynamo.py"),
        os.path.join(p, "domains"),
        os.path.join(p, "core"),
    ]
    missing_local = [x for x in expected_local if not os.path.exists(x)]
    return (len(missing_local) == 0), missing_local

def _candidate_repo_dirs():
    tried = []

    # 1) Optional override: power users can set this once
    try:
        v = os.environ.get("REVIT_FINGERPRINT_REPO_DIR", "")
    except Exception:
        v = ""
    v = str(v).strip()
    if v:
        tried.append(("env:REVIT_FINGERPRINT_REPO_DIR", v))

    # 2) Preferred per-user install location (Documents)
    # NOTE: Documents is sometimes redirected into OneDrive/SharePoint.
    # We will still *search* here, but runtime will be BLOCKED if it looks synced.
    up = os.environ.get("USERPROFILE", "")
    if up:
        tried.append(("documents:current", os.path.join(up, "Documents", ORG_DIR, APP_DIR, CHANNEL_DIR)))

    # 3) Fallback: LocalAppData (less visible, usually not synced)
    lad = os.environ.get("LOCALAPPDATA", "")
    if lad:
        tried.append(("localappdata:current", os.path.join(lad, ORG_DIR, APP_DIR, CHANNEL_DIR)))

    # 4) Fallback: legacy-ish user profile location
    if up:
        tried.append(("userprofile:RevitFingerprint_current", os.path.join(up, "RevitFingerprint", "current")))

    return tried

_selected = None
_tried = []
for src, p in _candidate_repo_dirs():
    if not p:
        continue
    repo_dir = os.path.abspath(str(p))
    _tried.append({"source": src, "path": repo_dir})

    unsafe = []
    if _looks_like_unc_path(repo_dir):
        unsafe.append("repo_dir_is_unc_path")
    if _is_probably_sync_path(repo_dir):
        unsafe.append("repo_dir_looks_like_sharepoint_onedrive_sync")

    ok, missing = _is_repo_root(repo_dir)
    if ok and not unsafe:
        _selected = {"repo_dir": repo_dir, "source": src}
        break

if _selected is None:
    OUT = {
        "status": "blocked",
        "error": "Local install not found (or only found in unsafe locations).",
        "expected_install": {
            "recommended_current": r"%USERPROFILE%\Documents\{ORG}\{APP}\{CH}".format(ORG=ORG_DIR, APP=APP_DIR, CH=CHANNEL_DIR),
            "zip_extract_example": r"%USERPROFILE%\Documents\{ORG}\{APP}\vX.Y.Z".format(ORG=ORG_DIR, APP=APP_DIR),
            "fallback_current_localappdata": r"%LOCALAPPDATA%\{ORG}\{APP}\{CH}".format(ORG=ORG_DIR, APP=APP_DIR, CH=CHANNEL_DIR),
            "override_env_var": "REVIT_FINGERPRINT_ORG_DIR",
        },
        "tried": _tried,
        "notes": [
            "Do not run from SharePoint/OneDrive-synced folders or UNC paths.",
            "Install the code locally, then run the Dynamo Player graph from SharePoint.",
            "Optional override: set REVIT_FINGERPRINT_REPO_DIR to your local install root.",
        ],
    }
else:
    REPO_DIR = _selected["repo_dir"]

    # Ensure this repo wins import resolution
    if REPO_DIR in sys.path:
        sys.path.remove(REPO_DIR)
    sys.path.insert(0, REPO_DIR)

    try:
        # ---- CPython 3: purge cached modules so edits on disk are picked up ----
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
            "status": "failed",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "REPO_DIR": REPO_DIR,
            "repo_dir_source": _selected.get("source", None),
            "sys_path_head": sys.path[:8],
            "exporter_file": getattr(sys.modules.get("runner.run_dynamo", None), "__file__", None),
        }
