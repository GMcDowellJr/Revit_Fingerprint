# -*- coding: utf-8 -*-
"""
Phase Graphics domain extractor.

Fingerprints phase graphic override settings including:
- Projection and cut line styles
- Line colors
- Surface patterns and colors
- Material overrides

This captures the GLOBAL phase graphic override settings that apply
across the project.

This is a GLOBAL domain - settings are defined once.

Per-record identity: N/A (single global configuration)
Ordering: order-insensitive (settings are unordered)
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)


try:
    from Autodesk.Revit.DB import ElementOnPhaseStatus
except ImportError:
    ElementOnPhaseStatus = None


def extract(doc, ctx=None):
    """
    Extract Phase Graphics override settings from document.

    NOTE: This captures project-level phase graphic settings.
    In many Revit versions, these are stored in Categories.
    Some versions may not expose these settings via API.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with hash and signature components
    """
    info = {
        "hash": None,
        "signature": [],
        "debug_note": "Phase graphic overrides may not be fully exposed in all API versions"
    }

    sig = []

    # Try to access phase graphic override settings
    # Note: API access to these settings varies by Revit version
    try:
        cats = doc.Settings.Categories

        # Phase statuses to check
        statuses = []
        if ElementOnPhaseStatus:
            try:
                statuses = [
                    ("New", ElementOnPhaseStatus.New),
                    ("Existing", ElementOnPhaseStatus.Existing),
                    ("Demolished", ElementOnPhaseStatus.Demolished),
                    ("Temporary", ElementOnPhaseStatus.Temporary),
                ]
            except Exception as e:
                pass

        # For each category that supports phase overrides, capture settings
        # This is a simplified implementation - full implementation would
        # require iterating through categories and checking phase override settings

        sig.append("note=PhaseGraphicsNotFullyImplemented")
        sig.append("statuses_count={}".format(len(statuses)))

    except Exception as e:
        sig.append("error=CannotAccessPhaseGraphics")

    # Sort signature components (order-insensitive)
    sig_sorted = sorted(sig)
    info["signature"] = sig_sorted

    # Hash the settings
    info["hash"] = make_hash(sig_sorted) if sig_sorted else None

    return info
