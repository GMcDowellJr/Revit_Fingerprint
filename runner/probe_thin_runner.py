# -*- coding: utf-8 -*-
"""
probe_thin_runner.py

Dynamo CPython3 thin runner that executes every breadth probe under
tools/probes/ against the current Revit document in one pass and writes a
single combined result file:

    tools/probes/probes_<revit_version>_<run_id>.json

    {
      "run_metadata": {
        "run_id": "...",
        "extraction_date": "<ISO8601>",
        "revit_version": "2025",
        "tool_version": "<VERSION.txt contents, or null>",
        "document": {"title":..., "path_name":..., "is_workshared":...},
        "source": "thin_runner",
        "probes_run": ["arrowheads", "dimension_types", ...]
      },
      "domains": {
        "arrowheads": [ ...same per-domain entries a standalone probe run
                         with write_json=True would have produced... ],
        ...
      }
    }

This is deliberately ONE file per batch run rather than one file per probe:
tools/probes/build_probe_inventory.py consolidates across domains/runs
anyway, so per-probe files were only ever an artifact of running probes one
at a time by hand. The extraction date lives as JSON metadata here, not as
a filename token -- the filename groups by Revit release (revit_version)
plus an opaque run_id, so repeated runs against the same release don't
collide but also aren't pretending the date is the meaningful axis.

Each probe_*.py file is still a self-contained, paste-able Dynamo Python
node in its own right (unchanged by this runner) -- this just orchestrates
running all of them in one Revit session instead of pasting/running ~20
scripts one at a time.

Paste into a Dynamo Python Script node (CPython3 engine) and run.

Inputs (IN):
  IN[0] output_directory (str)
      Where to write the combined JSON file. Default: None -> same fallback
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

Output (OUT): a JSON string summarizing the run -- per-probe status, the
single combined file written, and repo-resolution diagnostics.
"""

import fnmatch
import json
import os
import sys
import traceback
import uuid
from datetime import datetime

import clr
clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

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


def _read_tool_version(repo_root):
    try:
        p = os.path.join(repo_root, "VERSION.txt")
        if not os.path.exists(p):
            return None
        with open(p, "r") as f:
            s = f.read().strip()
        return s if s else None
    except Exception:
        return None


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
# Run-level metadata (computed once for the whole batch, not per probe)
# ---------------------------------------------------------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except Exception:
        return default


def _revit_version():
    try:
        uiapp = DocumentManager.Instance.CurrentUIApplication
        app = uiapp.Application if uiapp is not None else None
        v = _safe(lambda: app.VersionNumber, None)
        return str(v) if v else None
    except Exception:
        return None


def _document_identity():
    try:
        doc = DocumentManager.Instance.CurrentDBDocument
    except Exception:
        return {"title": None, "path_name": None, "is_workshared": None}
    return {
        "title": _safe(lambda: doc.Title, None),
        "path_name": _safe(lambda: doc.PathName, None),
        "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
    }


RUN_ID = datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
REVIT_VERSION = _revit_version() or "unknown"
TOOL_VERSION = _read_tool_version(REPO_DIR)
EXTRACTION_DATE = datetime.now().isoformat()
DOCUMENT_IDENTITY = _document_identity()

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
    pn = DOCUMENT_IDENTITY.get("path_name")
    if pn:
        d = os.path.dirname(pn)
        if d:
            return d
    return os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()


def _run_one_probe(path):
    """Executes a single probe_*.py file's source in an isolated namespace
    and returns (stem, domain_guess, ok, out_value, error_or_None)."""
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


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

probe_files, discover_err = _discover_probe_files()
out_dir = _default_output_dir()

if discover_err:
    OUT = json.dumps(
        {"status": "failed", "error": discover_err, "repo_dir": REPO_DIR},
        indent=2, sort_keys=True,
    )
    raise SystemExit

import gc

# ---------------------------------------------------------------------------
# Incremental, crash-safe write.
#
# The combined payload is written to a temp file AS EACH PROBE COMPLETES,
# not buffered in memory and dumped in one shot at the end. Two independent
# problems this fixes (see the 2026-08-04 worksets truncation: a run
# interrupted partway lost every probe's output -- including everything
# that had already finished cleanly -- with the file left mid-token and no
# exception ever surfaced, because nothing touched disk until the last
# line of the script):
#
#   1. Whatever completed before an interruption is already on disk and
#      readable, instead of the whole run's output being lost regardless
#      of how much of it had already succeeded.
#   2. A domain's records are written and the in-memory reference dropped
#      immediately after, instead of every domain being retained for the
#      whole run (some probes -- loaded_family_types, identity,
#      phase_graphics -- produce hundreds of records each). Peak memory
#      tracks roughly one probe's working set, not the sum of all of them.
#
# The temp file is only ever renamed onto the real target path (an atomic
# os.replace, same filesystem) after the JSON has been fully and validly
# closed out -- a reader can never observe a partially-written file at the
# expected filename, only the previous good file or the complete new one.
#
# This assumes (true for every current probe_*.py) that each domain is
# contributed by exactly one probe file, so a domain's block can be
# streamed and closed the moment that probe finishes. See _flush_domain
# for what happens in the (currently never-hit) case where that stops
# being true.
# ---------------------------------------------------------------------------


def _json_block(value):
    """Render a value as an indented JSON fragment suitable for splicing
    into the hand-written envelope below. Not re-indented to visually align
    under its parent key -- purely cosmetic, doesn't affect validity, and
    every consumer (json.load) is indent-agnostic."""
    return json.dumps(value, indent=2, sort_keys=True)


results = []
ok_count = 0
failed_count = 0
probes_run_names = []
written_domains = set()
_first_domain_written = False

fname = "probes_{}_{}.json".format(REVIT_VERSION, RUN_ID)
target = os.path.join(out_dir, fname)
tmp_target = target + ".tmp-{}".format(os.getpid())

file_written = None
write_error = None
partial_file = None  # set from the first byte written until the final rename succeeds

try:
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    tmp_f = open(tmp_target, "w", encoding="utf-8")
    tmp_f.write('{\n  "domains": {\n')
    partial_file = tmp_target
except Exception as ex:
    tmp_f = None
    write_error = "open_tmp_failed: {}: {}".format(type(ex).__name__, ex)


def _flush_domain(domain, entries):
    global _first_domain_written
    if tmp_f is None:
        return False
    key = domain
    if domain in written_domains:
        # A second probe declared a domain another probe already streamed
        # and closed. Doesn't happen with any probe_*.py today -- each
        # domain has exactly one owning probe file -- but streaming can't
        # retroactively merge into an already-closed block, and silently
        # emitting a duplicate JSON key would make json.load quietly keep
        # only the last one, discarding the first. Write it under a
        # clearly-flagged side key instead so nothing is lost and the
        # conflict is visible in the file.
        key = "{}__unmerged_duplicate_probe_output_{}".format(domain, len(written_domains))
    if _first_domain_written:
        tmp_f.write(",\n")
    tmp_f.write('    "{}": '.format(key))
    tmp_f.write(_json_block(entries))
    _first_domain_written = True
    written_domains.add(domain)
    try:
        tmp_f.flush()
        os.fsync(tmp_f.fileno())
    except Exception:
        pass  # best-effort durability; a failed fsync isn't fatal here
    return True


for path in probe_files:
    # Filter on the filename/domain guess BEFORE executing: a probe can do a
    # full FilteredElementCollector scan of the document, so a filtered run
    # (e.g. domain_filter=["units"]) must not still execute every other
    # probe just to discard the result afterward.
    stem = os.path.splitext(os.path.basename(path))[0]
    domain_guess = stem[len("probe_"):] if stem.startswith("probe_") else stem
    if not _matches_filter(stem, [domain_guess]):
        continue

    ok, out_value, err = None, None, None
    try:
        stem, domain_guess, ok, out_value, err = _run_one_probe(path)
    except Exception as ex:
        # Defensive: _run_one_probe itself should not raise, but never let
        # one probe take down the whole sweep.
        ok = False
        err = "runner-level failure: {}: {}".format(type(ex).__name__, ex)

    record = {"probe_file": os.path.basename(path), "domain_guess": domain_guess, "status": None, "error": None}

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

    if declared_domains:
        # Standard probe contract: OUT is a list of {"kind":..., "domain":...}
        # entries. Stream per declared domain (normally just one per probe)
        # straight to the temp file rather than accumulating in memory.
        for domain in declared_domains:
            domain_entries = [e for e in out_value if isinstance(e, dict) and e.get("domain") == domain]
            _flush_domain(domain, domain_entries)
        record["status"] = "ok"
        record["domains_written"] = declared_domains
    else:
        # Non-standard OUT shape (e.g. a findings dict rather than a
        # domain-tagged list). Still capture it rather than dropping it,
        # tagged under the probe's own filename stem so nothing is lost.
        _flush_domain(domain_guess, [out_value])
        record["status"] = "ok_nonstandard_shape"
        record["domains_written"] = [domain_guess]

    ok_count += 1
    probes_run_names.append(domain_guess)
    results.append(record)

    # Drop this probe's payload before moving to the next one so peak
    # memory tracks one probe's working set, not the accumulated total.
    # gc.collect() is a deliberate extra nudge, not just refcounting relief:
    # probes hold pythonnet-wrapped Revit API objects (FilteredElement
    # Collector results, sampled Views/Worksets/...), and references that
    # cross the CLR/Python boundary don't always get reclaimed as promptly
    # on refcount alone as pure-Python objects do.
    out_value = None
    declared_domains = None
    gc.collect()

status = "ok" if failed_count == 0 else ("degraded" if ok_count > 0 else "failed")

run_metadata = {
    "run_id": RUN_ID,
    "extraction_date": EXTRACTION_DATE,
    "revit_version": REVIT_VERSION,
    "tool_version": TOOL_VERSION,
    "document": DOCUMENT_IDENTITY,
    "source": "thin_runner",
    "probes_run": sorted(probes_run_names),
}

if tmp_f is not None:
    if ok_count > 0:
        try:
            tmp_f.write('\n  },\n  "run_metadata": ')
            tmp_f.write(_json_block(run_metadata))
            tmp_f.write("\n}\n")
            tmp_f.flush()
            os.fsync(tmp_f.fileno())
            tmp_f.close()
            os.replace(tmp_target, target)  # atomic on the same filesystem
            file_written = target
            partial_file = None
        except Exception as ex:
            write_error = "{}: {}".format(type(ex).__name__, ex)
            status = "degraded" if status == "ok" else status
            try:
                tmp_f.close()
            except Exception:
                pass
    else:
        # Nothing succeeded -- matches the previous behavior of never
        # producing a probes_<version>_<run_id>.json at all in this case.
        try:
            tmp_f.close()
        except Exception:
            pass
        try:
            os.remove(tmp_target)
        except Exception:
            pass
        partial_file = None

OUT = json.dumps(
    {
        "status": status,
        "run_id": RUN_ID,
        "revit_version": REVIT_VERSION,
        "extraction_date": EXTRACTION_DATE,
        "output_directory": out_dir,
        "file_written": file_written,
        "file_write_error": write_error,
        "partial_file": partial_file,
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
