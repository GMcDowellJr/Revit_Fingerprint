# -*- coding: utf-8 -*-
"""
View Filters domain extractor.

Fingerprints view filter definitions including:
- Filter rules (parameter-based conditions)
- Categories the filter applies to
- Filter type (rule-based vs selection)

This is a GLOBAL domain - filters are defined once and referenced
by views and view templates.

Per-record identity: UniqueId (element-backed)
Ordering: rules are order-sensitive (preserved), categories are sorted
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
    from Autodesk.Revit.DB import FilteredElementCollector, ParameterFilterElement
except ImportError:
    FilteredElementCollector = None
    ParameterFilterElement = None


def extract(doc, ctx=None):
    """
    Extract View Filters fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (will be populated with filter_uid -> def_hash mapping)

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
        col = list(FilteredElementCollector(doc).OfClass(ParameterFilterElement))
    except:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}  # For context population

    for f in col:
        # Name is metadata only
        name = canon_str(getattr(f, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = "<unnamed>"
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(f, "UniqueId", None))
        except:
            uid = None

        # Build filter signature
        sig = []

        # Filter type (rule-based vs selection-based)
        try:
            # Check if this is a selection filter
            is_selection = hasattr(f, "GetElementFilter") and f.GetElementFilter() is None
            sig.append("is_selection={}".format(sig_val(is_selection)))
        except:
            sig.append("is_selection=<None>")

        # Categories the filter applies to
        try:
            cat_ids = list(f.GetCategories())
            cat_names = []
            for cid in cat_ids:
                try:
                    cat = doc.Settings.Categories.get_Item(cid)
                    cat_name = canon_str(cat.Name) if cat else None
                    if cat_name:
                        cat_names.append(cat_name)
                except:
                    pass
            # Sort categories (order-insensitive for categories)
            cat_names_sorted = sorted(set(cat_names))
            sig.append("categories={}".format(sig_val(",".join(cat_names_sorted) if cat_names_sorted else "<None>")))
        except:
            sig.append("categories=<None>")

        # Filter rules (order-sensitive - preserve order)
        try:
            elem_filter = f.GetElementFilter()
            if elem_filter:
                # Try to extract rules
                try:
                    rules = list(elem_filter.GetFilters()) if hasattr(elem_filter, 'GetFilters') else []
                    sig.append("rule_count={}".format(len(rules)))

                    # For each rule, capture a simplified signature
                    for i, rule in enumerate(rules):
                        idx = "{:03d}".format(i)
                        try:
                            rule_str = safe_str(rule)
                            sig.append("rule[{}]={}".format(idx, sig_val(rule_str)))
                        except:
                            sig.append("rule[{}]=<Unreadable>".format(idx))
                except:
                    sig.append("rules=<Unreadable>")
            else:
                sig.append("rule_count=0")
        except:
            sig.append("rules=<None>")

        # Hash the definition (rules are NOT sorted - order matters)
        def_hash = make_hash(sig)

        rec = {
            "id": safe_str(f.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "def_hash": def_hash,
            "def_signature": sig  # Include for explainability
        }

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

        # Populate context mapping
        if uid:
            uid_to_hash[uid] = def_hash

    # Populate context for downstream domains (views, templates)
    if ctx is not None:
        ctx["filter_uid_to_hash"] = uid_to_hash

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
