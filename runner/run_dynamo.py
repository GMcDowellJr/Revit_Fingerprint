# -*- coding: utf-8 -*-
"""
Dynamo runner for Revit Fingerprint extraction.

This runner:
- Acquires the Revit document from Dynamo context
- Selects which domains to run (allowlist mechanism)
- Assembles final JSON output

Current implementation (M4): uses modular domain extractors with new global domains
"""

import clr
import json
import sys
import os

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
    # Context dictionary for cross-domain references
    # Populated by global domains, consumed by contextual domains
    ctx = {}

    # Assemble fingerprint by calling each domain extractor
    fingerprint = {}

    # Metadata domains (no behavioral hash)
    fingerprint["identity"] = identity.extract(doc, ctx)
    fingerprint["units"] = units.extract(doc, ctx)

    # Global style domains (locked semantics)
    fingerprint["objectstyles"] = object_styles.extract(doc, ctx)
    fingerprint["line_patterns"] = line_patterns.extract(doc, ctx)
    fingerprint["line_styles"] = line_styles.extract(doc, ctx)
    fingerprint["fill_patterns"] = fill_patterns.extract(doc, ctx)
    fingerprint["text_types"] = text_types.extract(doc, ctx)
    fingerprint["dimension_types"] = dimension_types.extract(doc, ctx)

    # New global domains (M4) - run before contextual domains
    # These populate ctx with mappings for views/templates to reference
    fingerprint["view_filters"] = view_filters.extract(doc, ctx)
    fingerprint["phases"] = phases.extract(doc, ctx)
    fingerprint["phase_filters"] = phase_filters.extract(doc, ctx)
    fingerprint["phase_graphics"] = phase_graphics.extract(doc, ctx)

    # Contextual domains (can reference global domains via ctx)
    fingerprint["view_templates"] = view_templates.extract(doc, ctx)

    return fingerprint


# Execute extraction
doc = get_doc()
fingerprint = run_fingerprint(doc)

# Output JSON (Dynamo expects OUT variable)
OUT = json.dumps(fingerprint, indent=2, sort_keys=True)
