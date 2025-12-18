# -*- coding: utf-8 -*-
"""
Phase Filters domain extractor.

Fingerprints phase filter definitions including:
- New/Existing/Demolished/Temporary visibility and line style overrides

Phase filters define how elements in different phases are displayed.
This is a GLOBAL domain - filters are defined once and referenced by views.

Per-record identity: UniqueId (element-backed)
Ordering: order-insensitive (settings are unordered)
"""

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
core_dir = os.path.join(parent_dir, 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from core.hashing import make_hash, safe_str
from core.canon import canon_str, sig_val

try:
    from Autodesk.Revit.DB import FilteredElementCollector, PhaseFilter, ElementOnPhaseStatus
except ImportError:
    FilteredElementCollector = None
    PhaseFilter = None
    ElementOnPhaseStatus = None


def extract(doc, ctx=None):
    """
    Extract Phase Filters fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (will be populated with phase_filter_uid -> def_hash mapping)

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
        "records": [],
        "signature_hashes": [],
        "hash": None,

        # debug counters
        "debug_missing_name": 0,
        "debug_fail_read": 0,
        "debug_kept": 0,
    }

    try:
        col = list(FilteredElementCollector(doc).OfClass(PhaseFilter))
    except:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}  # For context population

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
        except:
            pass

    for pf in col:
        # Name is metadata only
        name = canon_str(getattr(pf, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = "<unnamed>"
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(pf, "UniqueId", None))
        except:
            uid = None

        # Build phase filter signature
        sig = []

        # For each phase status, get the graphic settings
        for status_name, status_enum in statuses:
            try:
                # Line style override (if any)
                line_style_id = pf.GetPhaseStatusLineStyle(status_enum)
                line_style_uid = "<None>"
                if line_style_id and line_style_id.IntegerValue > 0:
                    try:
                        ls_elem = doc.GetElement(line_style_id)
                        line_style_uid = canon_str(getattr(ls_elem, "UniqueId", None)) or "<None>"
                    except:
                        pass

                # Visibility override
                is_overridden = pf.IsPhaseStatusOverridden(status_enum)

                sig.append("{}|overridden={}".format(status_name, sig_val(is_overridden)))
                sig.append("{}|line_style={}".format(status_name, line_style_uid))
            except:
                sig.append("{}|<Unreadable>".format(status_name))

        # Sort signature components (order-insensitive)
        sig_sorted = sorted(sig)

        # Hash the definition
        def_hash = make_hash(sig_sorted)

        rec = {
            "id": safe_str(pf.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "def_hash": def_hash,
            "def_signature": sig_sorted
        }

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

        # Populate context mapping
        if uid:
            uid_to_hash[uid] = def_hash

    # Populate context for downstream domains
    if ctx is not None:
        ctx["phase_filter_uid_to_hash"] = uid_to_hash

    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
