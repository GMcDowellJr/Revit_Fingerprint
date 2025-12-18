# -*- coding: utf-8 -*-
"""
View Templates domain extractor.

CURRENT IMPLEMENTATION (M0-M3): Name-only presence fingerprinting
PLANNED (M5): Behavioral fingerprinting with record_rows

Per-record identity: View template name (will change to UniqueId in M5)
Ordering: order-insensitive (sorted)
"""

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
core_dir = os.path.join(parent_dir, 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from hashing import make_hash
from canon import canon_str

try:
    from Autodesk.Revit.DB import FilteredElementCollector, View
except ImportError:
    FilteredElementCollector = None
    View = None


def extract(doc, ctx=None):
    """
    Extract View Templates fingerprint from document.

    NOTE: This is the legacy implementation that only captures template names.
    Will be upgraded to behavioral fingerprinting in M5.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain currently)

    Returns:
        Dictionary with count, names, and hash
    """
    info = {
        "count": 0,
        "names": [],
        "hash": None
    }

    try:
        col = FilteredElementCollector(doc).OfClass(View)
        names = []
        for v in col:
            try:
                if v.IsTemplate:
                    names.append(canon_str(v.Name))
            except:
                continue
        names_sorted = sorted(set(names))
        info["count"] = len(names_sorted)
        info["names"] = names_sorted
        info["hash"] = make_hash(names_sorted)
    except:
        pass

    return info
