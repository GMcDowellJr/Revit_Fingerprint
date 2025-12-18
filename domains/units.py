# -*- coding: utf-8 -*-
"""
Units domain extractor.

Captures project units settings including:
- Length, area, volume format options
- Unit types and symbols
- Accuracy settings

Per-domain identity: N/A (single global hash)
"""

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
core_dir = os.path.join(parent_dir, 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from hashing import make_hash, safe_str

try:
    from Autodesk.Revit.DB import SpecTypeId
except ImportError:
    SpecTypeId = None


def extract(doc, ctx=None):
    """
    Extract Units fingerprint from document.

    Version-safe units snapshot (Revit 2022+).
    - 'repr' is the raw Units.ToString() for quick sanity.
    - 'specs' holds explicit Length/Area/Volume format options.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with repr, specs, and hash
    """
    result = {
        "repr": None,
        "specs": {},
        "hash": None
    }

    try:
        u = doc.GetUnits()
    except:
        return result

    result["repr"] = safe_str(u)

    records = []

    specs = [
        ("length", SpecTypeId.Length),
        ("area",   SpecTypeId.Area),
        ("volume", SpecTypeId.Volume)
    ]

    for label, spec_id in specs:
        try:
            fmt = u.GetFormatOptions(spec_id)
        except:
            continue

        try:
            unit_id   = safe_str(fmt.GetUnitTypeId())
        except:
            unit_id   = "<no-unit>"

        try:
            symbol_id = safe_str(fmt.GetSymbolTypeId())
        except:
            symbol_id = "<no-symbol>"

        try:
            acc = fmt.Accuracy
        except:
            acc = None

        rec = {
            "spec": label,
            "unit_id": unit_id,
            "symbol_id": symbol_id,
            "accuracy": acc
        }
        result["specs"][label] = rec
        records.append("{}|{}|{}|{}".format(label, unit_id, symbol_id, acc))

    if records:
        result["hash"] = make_hash(sorted(records))

    return result
