import sys
import os
import traceback
import json

# Explicitly request semantic (v2) hashing

import importlib

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
    Heuristic, Windows-centric: previously used to hard-block sync paths.
    Retained for detection only — callers decide whether to block or warn.
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

def _iter_dyn_path_candidates():
    out = []
    seen = set()

    def _normalize_host_path(raw):
        s = str(raw or "").strip()
        if not s:
            return s

        # Trim wrapping quotes often injected by host shells.
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            s = s[1:-1].strip()

        # file:// URI support
        if s.lower().startswith("file:///"):
            s = s[8:].replace("/", os.sep)

        # Windows device paths:
        #   \\?\C:\path -> C:\path
        #   \\?\UNC\server\share\path -> \\server\share\path (preserve UNC absoluteness)
        sl = s.lower()
        if sl.startswith("\\\\?\\unc\\"):
            s = "\\\\" + s[8:]
        elif sl.startswith("\\\\?\\"):
            s = s[4:]

        return s

    def _add(src, value):
        try:
            raw = _normalize_host_path(value)
        except Exception:
            return
        if not raw:
            return

        try:
            v = os.path.abspath(raw)
        except Exception:
            return
        if not v:
            return

        # This collector is DYN-centric by design:
        # - accept explicit .dyn files
        # - accept directories (caller may provide a containing folder)
        # - reject non-.dyn files (e.g., .rvt) so we do not anchor root discovery
        #   to a Revit model path.
        vl = v.lower()
        if os.path.isfile(v) and (not vl.endswith(".dyn")):
            return
        if (not os.path.exists(v)) and (os.path.splitext(vl)[1] not in ("", ".dyn")):
            return

        k = v.lower()
        if k in seen:
            return
        seen.add(k)
        out.append((src, v))

    # Auto-discovery (best effort): use current process context as "current graph"
    # starting point when explicit graph path is unavailable.
    try:
        cwd = os.getcwd()
    except Exception:
        cwd = ""
    if cwd:
        try:
            dyns = [x for x in os.listdir(cwd) if str(x).lower().endswith(".dyn")]
            if len(dyns) == 1:
                _add("auto:cwd_single_dyn", os.path.join(cwd, dyns[0]))
        except Exception:
            pass

    # Some hosts include an opened .dyn path on argv.
    try:
        for a in list(sys.argv):
            s = str(a or "").strip()
            if s.lower().endswith(".dyn"):
                _add("auto:argv_dyn", s)
    except Exception:
        pass

    # Explicit overrides from host/invoker
    for k in (
        "REVIT_FINGERPRINT_DYN_PATH",
        "DYNAMO_GRAPH_PATH",
        "DYNAMO_FILE_PATH",
    ):
        try:
            v = str(os.environ.get(k, "")).strip()
        except Exception:
            v = ""
        if v:
            _add("env:{}".format(k), v)

    # Optional thin-runner input extension: IN[3] = .dyn path or containing folder
    try:
        if IN is not None and len(IN) > 3 and IN[3] is not None:
            v = str(IN[3]).strip()
            if v:
                _add("in:IN[3]", v)
    except Exception:
        pass

    return out

def _nearest_repo_root_from_path(p, max_up=64):
    try:
        cur = os.path.abspath(str(p))
    except Exception:
        return None

    # Treat explicit *.dyn values as file paths even if the host has not
    # materialized them on disk yet (some Dynamo host surfaces provide
    # unresolved/current-document values).
    if str(cur).lower().endswith(".dyn") or os.path.isfile(cur):
        cur = os.path.dirname(cur)

    steps = 0
    while steps <= int(max_up):
        ok, missing = _is_repo_root(cur)
        if ok:
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
        steps += 1
    return None

def _candidate_repo_dirs():
    tried = []

    # 0) If a Dynamo graph path is known, discover the nearest repo root upward
    # from the .dyn file/folder location.
    for src, p in _iter_dyn_path_candidates():
        # Keep the original graph-derived candidate visible in diagnostics even
        # when it is not itself a repo root.
        tried.append((src + ":graph_path_candidate", p))
        rr = _nearest_repo_root_from_path(p)
        if rr:
            tried.append((src + ":nearest_repo_root", rr))

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

    # 5) Network-share friendly convention used by current Dynamo deployments
    # (user requested explicit fallback root)
    if up:
        tried.append(
            (
                "userprofile:stantec_general_code",
                os.path.join(up, "Stantec", "Revit_Fingerprint - General", "Code"),
            )
        )

    return tried

_selected = None
_tried = []
for src, p in _candidate_repo_dirs():
    if not p:
        continue
    repo_dir = os.path.abspath(str(p))
    _tried.append({"source": src, "path": repo_dir})

    unc = _looks_like_unc_path(repo_dir)
    sync = _is_probably_sync_path(repo_dir)

    ok, missing = _is_repo_root(repo_dir)
    if ok and not unc:
        _selected = {"repo_dir": repo_dir, "source": src}
        if sync:
            _selected["warnings"] = ["repo_dir_looks_like_sharepoint_onedrive_sync"]
        break

if _selected is None:
    OUT = {
        "status": "blocked",
        "error": "Local install not found (or only found in unsafe locations).",
        "expected_install": {
            "recommended_current": r"%USERPROFILE%\Documents\{ORG}\{APP}\{CH}".format(ORG=ORG_DIR, APP=APP_DIR, CH=CHANNEL_DIR),
            "zip_extract_example": r"%USERPROFILE%\Documents\{ORG}\{APP}\vX.Y.Z".format(ORG=ORG_DIR, APP=APP_DIR),
            "fallback_current_localappdata": r"%LOCALAPPDATA%\{ORG}\{APP}\{CH}".format(ORG=ORG_DIR, APP=APP_DIR, CH=CHANNEL_DIR),
            "fallback_stantec_general_code": r"%USERPROFILE%\Stantec\Revit_Fingerprint - General\Code",
            "override_env_var": "REVIT_FINGERPRINT_ORG_DIR",
        },
        "tried": _tried,
        "notes": [
            "Do not run from UNC/network paths (\\\\server\\share\\...).",
            "SharePoint/OneDrive-synced paths are permitted but will emit a warning.",
            "Install the code locally, then run the Dynamo Player graph from SharePoint.",
            "Optional override: set REVIT_FINGERPRINT_REPO_DIR to your local install root.",
        ],
    }
else:
    REPO_DIR = _selected["repo_dir"]
    try:
        os.environ["REVIT_FINGERPRINT_REPO_ROOT_SELECTED"] = REPO_DIR
    except Exception:
        pass

    # Ensure this repo wins import resolution
    if REPO_DIR in sys.path:
        sys.path.remove(REPO_DIR)
    sys.path.insert(0, REPO_DIR)

    try:
        # Import triggers execution in this repo (run_dynamo computes OUT at import time)
        # Execute exactly once per invocation:
        # - first run: import
        # - subsequent runs: reload existing module
        # If an existing module is bound to a different repo root, drop it and import fresh.
        _existing = sys.modules.get("runner.run_dynamo", None)

        def _purge_repo_modules():
            prefixes = ("runner", "core", "domains")
            for _name in list(sys.modules.keys()):
                if (
                    _name in prefixes
                    or _name.startswith("runner.")
                    or _name.startswith("core.")
                    or _name.startswith("domains.")
                ):
                    sys.modules.pop(_name, None)

        if _existing is not None:
            try:
                _f = os.path.abspath(str(getattr(_existing, "__file__", "") or ""))
            except Exception:
                _f = ""
            if not _f.startswith(os.path.abspath(REPO_DIR) + os.sep):
                # Repo root switched: purge runner + dependencies so imports come
                # from the selected repo consistently.
                _purge_repo_modules()
                _existing = None

        if _existing is None:
            exporter = importlib.import_module("runner.run_dynamo")
        else:
            try:
                exporter = importlib.reload(_existing)
            except ModuleNotFoundError:
                # Some embedded hosts lose module spec metadata; fall back to fresh import.
                _purge_repo_modules()
                exporter = importlib.import_module("runner.run_dynamo")

        # Forward the computed OUT from the runner module
        OUT = exporter.OUT
        if _selected.get("warnings"):
            try:
                _out = OUT if isinstance(OUT, dict) else json.loads(OUT)
                _out.setdefault("_runner_warnings", []).extend(_selected["warnings"])
                OUT = json.dumps(_out, indent=2, sort_keys=True)
            except Exception:
                pass
        try:
            _out = OUT if isinstance(OUT, dict) else json.loads(OUT)
            _out["_thinrunner_repo_resolution"] = {
                "selected": {
                    "source": _selected.get("source"),
                    "repo_dir": REPO_DIR,
                },
                "tried": _tried,
            }
            OUT = json.dumps(_out, indent=2, sort_keys=True)
        except Exception:
            pass

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
