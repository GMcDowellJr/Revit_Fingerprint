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
        "hash": None,

        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},
    }

    try:
        u = doc.GetUnits()
    except:
        return result

    result["repr"] = safe_str(u)

    records = []

    # v2 build state (domain-level block; no partial coverage semantics)
    v2_records = []
    v2_blocked = False
    v2_reasons = {}

    def _v2_block(reason_key):
        nonlocal v2_blocked
        if not v2_blocked:
            v2_blocked = True
        v2_reasons[reason_key] = True

    def _looks_guid_like(s):
        # Conservative: if it looks like a GUID, we treat it as disallowed for v2.
        try:
            import re
            s = safe_str(s)
            return re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s) is not None
        except:
            return False

    if SpecTypeId is None:
        # Legacy stays as-is (hash will remain None/partial depending on downstream),
        # but v2 explicitly blocks because required spec identifiers are unavailable.
        _v2_block("SpecTypeId_unavailable")
        specs = []
    else:
        specs = [
            ("length", SpecTypeId.Length),
            ("area",   SpecTypeId.Area),
            ("volume", SpecTypeId.Volume)
        ]

    # Track which required specs were successfully read (for v2 domain-level completeness)
    required_labels = set(["length", "area", "volume"])
    read_labels = set()

    for label, spec_id in specs:
        try:
            fmt = u.GetFormatOptions(spec_id)
        except:
            # legacy behavior: continue partial
            # v2 contract: blocks if any required spec is unreadable
            if label in required_labels:
                _v2_block("format_options_unreadable_{}".format(label))
            continue

        if label in required_labels:
            read_labels.add(label)

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
        
        # v2 signature: allow only non-guid-like identifiers; otherwise block (no ids/guids contract)
        if _looks_guid_like(unit_id) or _looks_guid_like(symbol_id):
            _v2_block("guid_like_unit_or_symbol_{}".format(label))
        else:
            v2_records.append("{}|{}|{}|{}".format(label, unit_id, symbol_id, safe_str(acc)))
        
        result["specs"][label] = rec
        records.append("{}|{}|{}|{}".format(label, unit_id, symbol_id, acc))

    if records:
        result["hash"] = make_hash(sorted(records))

    # v2 finalize (domain-level block; no partial coverage)
    if v2_blocked:
        result["hash_v2"] = None
        result["debug_v2_blocked"] = True
        result["debug_v2_block_reasons"] = v2_reasons
    else:
        # Must have all required specs read for v2
        if SpecTypeId is None:
            result["hash_v2"] = None
            result["debug_v2_blocked"] = True
            result["debug_v2_block_reasons"] = {"SpecTypeId_unavailable": True}
        else:
            if set(["length", "area", "volume"]).issubset(read_labels):
                # deterministic: preserve fixed spec order (no sorting needed)
                result["hash_v2"] = make_hash(v2_records) if v2_records else None
                result["debug_v2_blocked"] = False
                result["debug_v2_block_reasons"] = {}
            else:
                result["hash_v2"] = None
                result["debug_v2_blocked"] = True
                reasons = dict(v2_reasons)
                reasons["missing_required_specs"] = True
                result["debug_v2_block_reasons"] = reasons

    return result
