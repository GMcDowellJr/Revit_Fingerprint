# -*- coding: utf-8 -*-
"""
Dynamo runner for Revit Fingerprint extraction.

This runner:
- Acquires the Revit document from Dynamo context
- Selects which domains to run (allowlist mechanism)
- Assembles final JSON output

Current implementation (M5): full modular architecture with behavioral view templates
"""

import clr
import json
import sys
import os
import time
_SCRIPT_START = time.perf_counter()

# Add parent directory to path for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Revit/Dynamo plumbing
clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

# Import domain extractors
from domains import identity, units, object_styles, line_patterns, line_styles
from domains import fill_patterns, text_types, dimension_types
from domains import view_filters, phases, phase_filters, phase_graphics
from domains import view_templates

# Domain selection configuration
# Set to None to run all domains, or provide a list of domain names to run specific domains
ENABLED_DOMAINS = None  # None = all domains

def _new_domain_envelope(domain_name):
    return {
        "status": None,        # ok | blocked | unsupported | error
        "capability": {},      # api reachability / environment notes
        "semantic": {"hash": None},
        "quality": {},
        "debug": {},
        "_notes": [],
        # Points back to the legacy payload location (non-breaking)
        "legacy_ref": domain_name,
    }


def _extract_legacy_hash(payload):
    """
    Best-effort extraction of the legacy semantic hash without changing behavior.
    """
    try:
        if isinstance(payload, dict) and "hash" in payload:
            return payload.get("hash", None)
    except:
        pass
    return None


def _extract_legacy_quality(payload):
    q = {}
    try:
        if isinstance(payload, dict) and "count" in payload:
            q["count"] = payload.get("count", None)
        if isinstance(payload, dict) and "raw_count" in payload:
            q["raw_count"] = payload.get("raw_count", None)
    except:
        pass
    return q


def _domain_run(domain_name, fn, doc, ctx, runner_notes):
    """
    Runs a domain extractor safely and returns (legacy_payload, envelope).
    Does not modify legacy payload contents.
    """
    env = _new_domain_envelope(domain_name)
    try:
        legacy = fn(doc, ctx)
        env["status"] = "ok"
        env["capability"] = {"api_reachable": True}
        env["semantic"]["hash"] = _extract_legacy_hash(legacy)
        env["quality"] = _extract_legacy_quality(legacy)
        return legacy, env
    except Exception as e:
        import traceback as _traceback
        env["status"] = "error"
        env["capability"] = {"api_reachable": True}
        env["_notes"].append("Domain threw an exception; legacy payload omitted.")
        env["debug"] = {
            "error": str(e),
            "traceback": _traceback.format_exc(),
        }
        runner_notes.append("One or more domains errored; see _domains.*.debug for details.")
        return None, env

def _enabled(domain_name):
    """
    Allowlist gate for domain execution.
    - ENABLED_DOMAINS = None  -> run all domains
    - ENABLED_DOMAINS = [...] -> run only listed domains (exact key match)
    """
    if ENABLED_DOMAINS is None:
        return True
    try:
        allowed = set(ENABLED_DOMAINS)
    except:
        allowed = set()
    return domain_name in allowed

def get_doc():
    """Get current Revit document from Dynamo context."""
    return DocumentManager.Instance.CurrentDBDocument

def run_fingerprint(doc):
    """
    Execute fingerprint extraction on the given document.

    Args:
        doc: Revit Document

    Returns:
        Dictionary with all domain fingerprints
    """
    start_ts = time.time()

    # Context dictionary for cross-domain references
    # Populated by global domains, consumed by contextual domains
    ctx = {}
    ctx["debug_vg_details"] = False

    # Assemble fingerprint by calling each domain extractor (legacy payloads)
    fingerprint = {}

    # Contract envelope (additive; legacy domain payloads remain unchanged at top-level)
    domains_v2 = {}
    runner_notes = []

    # Metadata domains (no behavioral hash)
    if _enabled("identity"):
        legacy, env = _domain_run("identity", identity.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["identity"] = legacy
        domains_v2["identity"] = env

    if _enabled("units"):
        legacy, env = _domain_run("units", units.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["units"] = legacy
        domains_v2["units"] = env

    # Global style domains (locked semantics)
    if _enabled("objectstyles"):
        legacy, env = _domain_run("objectstyles", object_styles.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["objectstyles"] = legacy
        domains_v2["objectstyles"] = env

    if _enabled("line_patterns"):
        legacy, env = _domain_run("line_patterns", line_patterns.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["line_patterns"] = legacy
        domains_v2["line_patterns"] = env

    if _enabled("line_styles"):
        legacy, env = _domain_run("line_styles", line_styles.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["line_styles"] = legacy
        domains_v2["line_styles"] = env

    if _enabled("fill_patterns"):
        legacy, env = _domain_run("fill_patterns", fill_patterns.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["fill_patterns"] = legacy
        domains_v2["fill_patterns"] = env

    if _enabled("text_types"):
        legacy, env = _domain_run("text_types", text_types.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["text_types"] = legacy
        domains_v2["text_types"] = env

    if _enabled("dimension_types"):
        legacy, env = _domain_run("dimension_types", dimension_types.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["dimension_types"] = legacy
        domains_v2["dimension_types"] = env

    # New global domains (M4) - run before contextual domains
    # These populate ctx with mappings for views/templates to reference
    if _enabled("view_filters"):
        legacy, env = _domain_run("view_filters", view_filters.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["view_filters"] = legacy
        domains_v2["view_filters"] = env

    if _enabled("phases"):
        legacy, env = _domain_run("phases", phases.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["phases"] = legacy
        domains_v2["phases"] = env

    if _enabled("phase_filters"):
        legacy, env = _domain_run("phase_filters", phase_filters.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["phase_filters"] = legacy
        domains_v2["phase_filters"] = env

    # Phase graphics are not exposed via the Revit API (as of 2021–2025).
    # Domain intentionally disabled to avoid misleading fingerprints.
    # if _enabled("phase_graphics"):
    #     fingerprint["phase_graphics"] = phase_graphics.extract(doc, ctx)

    # Contract-only emission: phase graphics are known API-unreachable in supported versions.
    # Do not produce a semantic hash.
    if _enabled("phase_graphics"):
        env = _new_domain_envelope("phase_graphics")
        env["status"] = "unsupported"
        env["capability"] = {
            "api_reachable": False,
            "reason": "Phase graphics are not reachable via Revit API in supported versions.",
        }
        env["_notes"].append("Domain not executed; semantic.hash is None by contract.")
        domains_v2["phase_graphics"] = env

    # Contextual domains (can reference global domains via ctx)
    if _enabled("view_templates"):
        legacy, env = _domain_run("view_templates", view_templates.extract, doc, ctx, runner_notes)
        if legacy is not None:
            fingerprint["view_templates"] = legacy
        domains_v2["view_templates"] = env

    elapsed_seconds = round(time.time() - start_ts, 3)
    fingerprint["_elapsed_seconds"] = elapsed_seconds

    # Additive contract surfaces
    fingerprint["_domains"] = domains_v2
    fingerprint["_notes"] = runner_notes

    return fingerprint


# Execute extraction (OUT protection)
try:
    doc = get_doc()
    fingerprint = run_fingerprint(doc)

    domains_emitted = sorted([k for k in fingerprint.keys() if k not in ("_domains", "_notes")])

    if ENABLED_DOMAINS is None:
        domains_requested = "ALL"
    else:
        domains_requested = list(ENABLED_DOMAINS)

    fingerprint["_meta"] = {
        "runner": "M5",
        "elapsed_seconds": fingerprint.pop("_elapsed_seconds", None),
        "elapsed_seconds_total": round(time.perf_counter() - _SCRIPT_START, 3),
        "domains_requested": domains_requested,
        "domains_emitted": domains_emitted,
    }

    # Output JSON (Dynamo expects OUT variable)
    OUT = json.dumps(fingerprint, indent=2, sort_keys=True)

except Exception as e:
    import traceback as _traceback

    err = {
        "error": str(e),
        "traceback": _traceback.format_exc(),
        "_meta": {
            "runner": "M5",
            "runner_file": __file__,
        },
    }

    # Keep OUT type consistent (JSON string) even on failure
    OUT = json.dumps(err, indent=2, sort_keys=True)
