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
ENABLED_DOMAINS = ["view_templates","view_filters","phase_filters"]  # None = all domains

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
    ctx["debug_vg_details"] = True

    # Assemble fingerprint by calling each domain extractor
    fingerprint = {}

    # Metadata domains (no behavioral hash)
    if _enabled("identity"):
        fingerprint["identity"] = identity.extract(doc, ctx)
    if _enabled("units"):
        fingerprint["units"] = units.extract(doc, ctx)

    # Global style domains (locked semantics)
    if _enabled("objectstyles"):
        fingerprint["objectstyles"] = object_styles.extract(doc, ctx)
    if _enabled("line_patterns"):
        fingerprint["line_patterns"] = line_patterns.extract(doc, ctx)
    if _enabled("line_styles"):
        fingerprint["line_styles"] = line_styles.extract(doc, ctx)
    if _enabled("fill_patterns"):
        fingerprint["fill_patterns"] = fill_patterns.extract(doc, ctx)
    if _enabled("text_types"):
        fingerprint["text_types"] = text_types.extract(doc, ctx)
    if _enabled("dimension_types"):
        fingerprint["dimension_types"] = dimension_types.extract(doc, ctx)

    # New global domains (M4) - run before contextual domains
    # These populate ctx with mappings for views/templates to reference
    if _enabled("view_filters"):
        fingerprint["view_filters"] = view_filters.extract(doc, ctx)
    if _enabled("phases"):
        fingerprint["phases"] = phases.extract(doc, ctx)
    if _enabled("phase_filters"):
        fingerprint["phase_filters"] = phase_filters.extract(doc, ctx)
    # Phase graphics are not exposed via the Revit API (as of 2021–2025).
    # Domain intentionally disabled to avoid misleading fingerprints.
    # if _enabled("phase_graphics"):
    #     fingerprint["phase_graphics"] = phase_graphics.extract(doc, ctx)

    # Contextual domains (can reference global domains via ctx)
    if _enabled("view_templates"):
        fingerprint["view_templates"] = view_templates.extract(doc, ctx)
    
    elapsed_seconds = round(time.time() - start_ts, 3)
    fingerprint["_elapsed_seconds"] = elapsed_seconds
    return fingerprint


# Execute extraction (OUT protection)
try:
    doc = get_doc()
    fingerprint = run_fingerprint(doc)

    domains_emitted = sorted(fingerprint.keys())

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
