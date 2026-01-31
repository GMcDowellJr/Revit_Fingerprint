# -*- coding: utf-8 -*-
"""
View Category Overrides domain extractor.

Captures category override deltas relative to object_styles baseline.
This domain is designed for reuse across view templates and future view-level overrides.
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
    canonicalize_str,
    canonicalize_int,
)
from core.phase2 import phase2_sorted_items
from core.deps import require_domain, Blocked
from core.graphic_overrides import (
    extract_projection_graphics,
    extract_cut_graphics,
    extract_halftone,
    extract_transparency,
)

try:
    from Autodesk.Revit.DB import View, OverrideGraphicSettings
except ImportError:
    View = None
    OverrideGraphicSettings = None


def _phase2_build_join_key_items():
    """Build Phase-2 join-key IdentityItems (domain-specific, hypothesis-only).

    Stub implementation; to be completed in Part 2.
    """
    return []


def _phase2_partition_items(items):
    """Partition IdentityItems into semantic/cosmetic/unknown buckets (stub).

    Stub implementation; to be completed in Part 2.
    """
    return (
        phase2_sorted_items(items or []),
        [],
        [],
    )


def extract(doc, ctx=None):
    """
    Extract view category override deltas relative to object_styles baseline.

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

    Returns:
        Dictionary with records, hashes, and debug counters
    """
    info = {
        "count": 0,
        "records": [],
        "hash_v2": None,
        "signature_hashes_v2": [],

        # Debug counters
        "debug_templates_processed": 0,
        "debug_categories_checked": 0,
        "debug_overrides_found": 0,
        "debug_no_baseline": 0,
        "debug_no_change": 0,
        "debug_v2_blocked": False,
    }

    if View is None or OverrideGraphicSettings is None:
        info["debug_v2_blocked"] = True
        return info

    try:
        require_domain((ctx or {}).get("_domains", {}), "object_styles")
        require_domain((ctx or {}).get("_domains", {}), "line_patterns")
        require_domain((ctx or {}).get("_domains", {}), "fill_patterns")
    except Blocked:
        info["debug_v2_blocked"] = True
        return info

    return info
