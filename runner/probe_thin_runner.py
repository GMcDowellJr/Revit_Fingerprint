# -*- coding: utf-8 -*-
"""
probe_thin_runner.py

Dynamo CPython3 thin runner that executes every breadth probe under
tools/probes/ against the current Revit document in one pass, and writes
each domain's result to tools/probes/probe_<domain>_<YYYY-MM-DD>.json --
the same naming convention the probes already use when run individually
with write_json=True.

This does NOT replace running an individual probe by hand (each probe_*.py
is still a self-contained, paste-able Dynamo Python node). It exists so a
full breadth sweep across every domain can be captured in one Revit session
without pasting/running 20 scripts one at a time, and so
tools/probes/build_probe_inventory.py always has a fresh, complete set of
dated JSON files to consolidate.

Paste into a Dynamo Python Script node (CPython3 engine) and run.

Inputs (IN):
  IN[0] output_directory (str)
      Where to write the dated JSON files. Default: None -> same fallback
      chain the individual probes use (current .rvt's folder, else TEMP/TMP,
      else cwd).

  IN[1] domain_filter (str | list[str])
      Comma-separated string or list restricting which probes run, matched
      against the probe's domain name (e.g. "dimension_types") or its
      filename stem (e.g. "probe_dimension_types"). Default: None -> run
      every probe_*.py found.

  IN[2] probe_inputs (dict[str, list])
      Optional per-probe IN overrides, keyed by domain name or filename
      stem, e.g. {"object_styles": [500, False, 30, True]}. Any probe not
      named here runs with IN = [] (its own documented defaults apply).
      Default: {}.

  IN[3] repo_dir_override (str)
      Optional explicit repo root (must contain tools/probes/). Same escape
      hatch as runner/thin_runner.py's REVIT_FINGERPRINT_REPO_DIR.

  IN[4] fail_fast (bool)
      If True, stop the whole run on the first probe exception instead of
      recording it and continuing. Default: False (fail-soft: one broken
      probe must not block the rest of the sweep).

Output (OUT): a single JSON string summarizing the run -- per-probe status,
files written, and repo-resolution diagnostics.
"""

import fnmatch
import json
import os
import sys
import traceback
from datetime import datetime

# ---------------------------------------------------------------------------
# Repo discovery (mirrors runner/thin_runner.py's approach: no __file__
# reliance, since this is pasted into a Dynamo node; conventional per-user
# install locations, with an explicit override escape hatch).
# ---------------------------------------------------------------------------

ORG_DIR = str(os.environ.get("REVIT_FINGERPRINT_ORG_DIR", "Company")).strip() or "Company"
APP_DIR = "RevitFingerprint"
CHANNEL_DIR = "current"


def _looks_like_unc_path(p):
    try:
        return str(p).startswith("\\\\")
    except Exception:
        return False


def _is_probably_sync_path(p):
    try:
        s = os.path.abspath(str(p)).lower()
    except Exception:
        return False
    for m in ("\\onedrive\\", "\\sharepoint\\", "\\microsoft teams\\"):
        if m in s:
            return True
    if "\\documents\\" in s and ("- sharepoint" in s or "sharepoint" in s):
        return True
    return False


def _is_probes_root(p):
    try:
        base = os.path.abspath(str(p))
    except Exception:
        return False
    return os.path.isdir(os.path.join(base, "tools", "probes"))


def _candidate_repo_dirs(explicit_override):
    tried = []

    if explicit_override:
        tried.append(("in:IN[3]", str(explicit_override).strip()))

    try:
        v = str(os.environ.get("REVIT_FINGERPRINT_REPO_DIR", "")).strip()
    except Exception:
        v = ""
    if v:
        tried.append(("env:REVIT_FINGERPRINT_REPO_DIR", v))

    up = os.environ.get("USERPROFILE", "")
    if up:
        tried.append(("documents:current", os.path.join(up, "Documents", ORG_DIR, APP_DIR, CHANNEL_DIR)))

    lad = os.environ.get("LOCALAPPDATA", "")
    if lad:
        tried.append(("localappdata:current", os.path.join(lad, ORG_DIR, APP_DIR, CHANNEL_DIR)))

    if up:
        tried.append(("userprofile:RevitFingerprint_current", os.path.join(up, "RevitFingerprint", "current")))
        tried.append((
            "userprofile:stantec_general_code",
            os.path.join(up, "Stantec", "Revit_Fingerprint - General", "Code"),
        ))

    # Last resort: this file's own location, if the host ever does expose
    # __file__ (some Dynamo hosts do for certain node types).
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        tried.append(("file:runner_dir_parent", os.path.dirname(here)))
    except Exception:
        pass

    return tried


def _resolve_repo_dir(explicit_override):
    tried_out = []
    for src, p in _candidate_repo_dirs(explicit_override):
        if not p:
            continue
        repo_dir = os.path.abspath(p)
        tried_out.append({"source": src, "path": repo_dir})
        if _looks_like_unc_path(repo_dir):
            continue
        if _is_probes_root(repo_dir):
            warnings = []
            if _is_probably_sync_path(repo_dir):
                warnings.append("repo_dir_looks_like_sharepoint_onedrive_sync")
            return {"repo_dir": repo_dir, "source": src, "warnings": warnings}, tried_out
    return None, tried_out


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def _get_in(idx, default=None):
    try:
        if IN is not None and len(IN) > idx and IN[idx] is not None:
            return IN[idx]
    except Exception:
        pass
    return default


output_directory = _get_in(0, None)
domain_filter_raw = _get_in(1, None)
probe_inputs = _get_in(2, {}) or {}
repo_dir_override = _get_in(3, None)
fail_fast = bool(_get_in(4, False))

if isinstance(domain_filter_raw, str):
    domain_filter = [s.strip() for s in domain_filter_raw.split(",") if s.strip()]
elif isinstance(domain_filter_raw, (list, tuple)):
    domain_filter = [str(s).strip() for s in domain_filter_raw if str(s).strip()]
else:
    domain_filter = None

if not isinstance(probe_inputs, dict):
    probe_inputs = {}

_selected, _tried = _resolve_repo_dir(repo_dir_override)

if _selected is None:
    OUT = json.dumps(
        {
            "status": "blocked",
            "error": "Local install not found (repo root must contain tools/probes/).",
            "tried": _tried,
            "notes": [
                "Install the code locally (same convention as runner/thin_runner.py), "
                "or set REVIT_FINGERPRINT_REPO_DIR, or pass IN[3] explicitly.",
            ],
        },
        indent=2,
        sort_keys=True,
    )
    raise SystemExit

REPO_DIR = _selected["repo_dir"]
PROBES_DIR = os.path.join(REPO_DIR, "tools", "probes")

if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

# ---------------------------------------------------------------------------
# Probe discovery
# ---------------------------------------------------------------------------

# Excluded: not domain breadth probes (different OUT contract, or a one-off
# import-availability smoke test rather than a document-scoped inventory).
_NON_DOMAIN_PROBE_STEMS = set(["probe_roof_type_import"])


def _discover_probe_files():
    try:
        names = sorted(os.listdir(PROBES_DIR))
    except OSError as ex:
        return [], "could not list {}: {}".format(PROBES_DIR, ex)

    files = []
    for name in names:
        if not (name.startswith("probe_") and name.endswith(".py")):
            continue
        stem = name[:-3]
        if stem in _NON_DOMAIN_PROBE_STEMS:
            continue
        files.append(os.path.join(PROBES_DIR, name))
    return files, None


def _matches_filter(stem, declared_domains):
    if domain_filter is None:
        return True
    candidates = set([stem, stem.replace("probe_", "", 1)])
    candidates.update(declared_domains or [])
    for want in domain_filter:
        if want in candidates:
            return True
        if fnmatch.fnmatch(stem, want) or any(fnmatch.fnmatch(d, want) for d in (declared_domains or [])):
            return True
    return False


def _probe_in_for(stem, domain_guess):
    for key in (domain_guess, stem, stem.replace("probe_", "", 1)):
        if key and key in probe_inputs:
            v = probe_inputs[key]
            return list(v) if isinstance(v, (list, tuple)) else []
    return []


def _default_output_dir():
    if output_directory:
        return str(output_directory)
    try:
        rvt_path = DocumentManager.Instance.CurrentDBDocument.PathName  # noqa: F821
        if rvt_path:
            d = os.path.dirname(rvt_path)
            if d:
                return d
    except Exception:
        pass
    return os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()


def _run_one_probe(path):
    """Executes a single probe_*.py file's source in an isolated namespace
    and returns (ok, out_value, error_or_None)."""
    stem = os.path.splitext(os.path.basename(path))[0]
    domain_guess = stem[len("probe_"):] if stem.startswith("probe_") else stem

    try:
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
    except Exception as ex:
        return stem, domain_guess, False, None, "read failed: {}: {}".format(type(ex).__name__, ex)

    ns = {"__name__": "__main__", "__file__": path}
    ns["IN"] = _probe_in_for(stem, domain_guess)

    try:
        code = compile(source, path, "exec")
        exec(code, ns)
    except SystemExit:
        pass
    except Exception as ex:
        err = "{}: {}\n{}".format(type(ex).__name__, ex, traceback.format_exc())
        return stem, domain_guess, False, None, err

    return stem, domain_guess, True, ns.get("OUT"), None


def _domains_declared_in_out(out_value):
    if not isinstance(out_value, list):
        return []
    domains = []
    for entry in out_value:
        if isinstance(entry, dict) and entry.get("domain"):
            d = entry["domain"]
            if d not in domains:
                domains.append(d)
    return domains


def _write_domain_json(domain, out_value, out_dir, date_stamp):
    fname = "probe_{}_{}.json".format(domain, date_stamp)
    target = os.path.join(out_dir, fname)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    with open(target, "w", encoding="utf-8") as f:
        json.dump(out_value, f, indent=2, sort_keys=True)
    return target


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

probe_files, discover_err = _discover_probe_files()
date_stamp = datetime.now().strftime("%Y-%m-%d")
out_dir = _default_output_dir()

results = []
ok_count = 0
failed_count = 0

if discover_err:
    OUT = json.dumps(
        {"status": "failed", "error": discover_err, "repo_dir": REPO_DIR},
        indent=2, sort_keys=True,
    )
    raise SystemExit

for path in probe_files:
    stem, domain_guess, ok, out_value, err = None, None, None, None, None
    try:
        stem, domain_guess, ok, out_value, err = _run_one_probe(path)
    except Exception as ex:
        # Defensive: _run_one_probe itself should not raise, but never let
        # one probe take down the whole sweep.
        stem = os.path.splitext(os.path.basename(path))[0]
        domain_guess = stem[len("probe_"):] if stem.startswith("probe_") else stem
        ok = False
        err = "runner-level failure: {}: {}".format(type(ex).__name__, ex)

    if not _matches_filter(stem, _domains_declared_in_out(out_value) if ok else [domain_guess]):
        continue

    record = {"probe_file": os.path.basename(path), "domain_guess": domain_guess, "status": None,
              "files_written": [], "error": None}

    if not ok:
        record["status"] = "failed"
        record["error"] = err
        failed_count += 1
        results.append(record)
        if fail_fast:
            break
        continue

    if out_value is None:
        record["status"] = "failed"
        record["error"] = "probe produced no OUT"
        failed_count += 1
        results.append(record)
        if fail_fast:
            break
        continue

    declared_domains = _domains_declared_in_out(out_value)

    try:
        if declared_domains:
            # Standard probe contract: OUT is a list of {"kind":..., "domain":...}
            # entries. Split per declared domain (normally just one) so the
            # output file naming/shape matches what write_json=True already
            # produces when a probe is run individually.
            for domain in declared_domains:
                domain_entries = [e for e in out_value if isinstance(e, dict) and e.get("domain") == domain]
                target = _write_domain_json(domain, domain_entries, out_dir, date_stamp)
                record["files_written"].append(target)
            record["status"] = "ok"
        else:
            # Non-standard OUT shape (e.g. a findings dict rather than a
            # domain-tagged list). Still capture it rather than dropping it,
            # tagged under the probe's own filename stem.
            target = _write_domain_json(domain_guess, out_value, out_dir, date_stamp)
            record["files_written"].append(target)
            record["status"] = "ok_nonstandard_shape"
        ok_count += 1
    except Exception as ex:
        record["status"] = "failed"
        record["error"] = "write failed: {}: {}".format(type(ex).__name__, ex)
        failed_count += 1
        if fail_fast:
            results.append(record)
            break

    results.append(record)

status = "ok" if failed_count == 0 else ("degraded" if ok_count > 0 else "failed")

OUT = json.dumps(
    {
        "status": status,
        "date_stamp": date_stamp,
        "output_directory": out_dir,
        "probes_discovered": len(probe_files),
        "probes_run": len(results),
        "probes_ok": ok_count,
        "probes_failed": failed_count,
        "results": results,
        "repo_resolution": {
            "selected": _selected,
            "tried": _tried,
        },
    },
    indent=2,
    sort_keys=True,
)
