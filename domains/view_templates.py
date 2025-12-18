# -*- coding: utf-8 -*-
"""
View Templates domain extractor.

BEHAVIORAL FINGERPRINTING (M5):
Captures controlled behavior of view templates including:
- Applied view filters (references global filters domain)
- Phase settings (references global phases domain)
- Phase filter (references global phase_filters domain)
- Detail level, discipline, scale
- Display settings (visual style, graphic display options)

Per-record identity: UniqueId (element-backed)
Ordering: filter stack is order-sensitive (preserved), other settings order-insensitive
Names: metadata only (excluded from hash per D-008)
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
    from Autodesk.Revit.DB import FilteredElementCollector, View, BuiltInParameter
except ImportError:
    FilteredElementCollector = None
    View = None
    BuiltInParameter = None


def extract(doc, ctx=None):
    """
    Extract View Templates behavioral fingerprint from document.

    M5 IMPLEMENTATION: Behavior-based fingerprinting.
    Templates are fingerprinted by controlled behavior, not names.

    Args:
        doc: Revit Document
        ctx: Context dictionary with global domain mappings:
             - filter_uid_to_hash: view filter UID -> definition hash
             - phase_uid_to_hash: phase UID -> definition hash
             - phase_filter_uid_to_hash: phase filter UID -> definition hash

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
        "debug_not_template": 0,
        "debug_missing_name": 0,
        "debug_missing_uid": 0,
        "debug_fail_read": 0,
        "debug_kept": 0,
    }

    # Get context mappings (may be None if global domains not run)
    filter_map = ctx.get("filter_uid_to_hash", {}) if ctx else {}
    phase_map = ctx.get("phase_uid_to_hash", {}) if ctx else {}
    phase_filter_map = ctx.get("phase_filter_uid_to_hash", {}) if ctx else {}

    try:
        col = list(FilteredElementCollector(doc).OfClass(View))
    except:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []

    for v in col:
        # Only process view templates
        try:
            is_template = v.IsTemplate
        except:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # Name is metadata only (excluded from hash per D-008)
        name = canon_str(getattr(v, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = "<unnamed>"
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(v, "UniqueId", None))
        except:
            uid = None

        if not uid:
            info["debug_missing_uid"] += 1

        # Build template behavioral signature
        sig = []

        # View Type (plan, section, elevation, 3D, etc.)
        try:
            vtype = safe_str(v.ViewType)
            sig.append("view_type={}".format(sig_val(vtype)))
        except:
            sig.append("view_type=<None>")

        # Detail Level
        try:
            detail_level = safe_str(v.DetailLevel)
            sig.append("detail_level={}".format(sig_val(detail_level)))
        except:
            sig.append("detail_level=<None>")

        # Scale
        try:
            scale = v.Scale
            sig.append("scale={}".format(sig_val(scale)))
        except:
            sig.append("scale=<None>")

        # Discipline
        try:
            p = v.get_Parameter(BuiltInParameter.VIEW_DISCIPLINE) if BuiltInParameter else None
            if p and p.HasValue:
                discipline = safe_str(p.AsValueString())
                sig.append("discipline={}".format(sig_val(discipline)))
            else:
                sig.append("discipline=<None>")
        except:
            sig.append("discipline=<None>")

        # Phase (reference global phases domain)
        try:
            phase_id = v.get_Parameter(BuiltInParameter.VIEW_PHASE) if BuiltInParameter else None
            if phase_id and phase_id.HasValue:
                phase_elem_id = phase_id.AsElementId()
                if phase_elem_id and phase_elem_id.IntegerValue > 0:
                    phase_elem = doc.GetElement(phase_elem_id)
                    phase_uid = canon_str(getattr(phase_elem, "UniqueId", None)) if phase_elem else None
                    # Reference global phase hash (not name)
                    phase_hash = phase_map.get(phase_uid, "<NotInGlobalDomain>") if phase_uid else "<None>"
                    sig.append("phase={}".format(sig_val(phase_hash)))
                else:
                    sig.append("phase=<None>")
            else:
                sig.append("phase=<None>")
        except:
            sig.append("phase=<None>")

        # Phase Filter (reference global phase_filters domain)
        try:
            pf_id = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER) if BuiltInParameter else None
            if pf_id and pf_id.HasValue:
                pf_elem_id = pf_id.AsElementId()
                if pf_elem_id and pf_elem_id.IntegerValue > 0:
                    pf_elem = doc.GetElement(pf_elem_id)
                    pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                    # Reference global phase filter hash
                    pf_hash = phase_filter_map.get(pf_uid, "<NotInGlobalDomain>") if pf_uid else "<None>"
                    sig.append("phase_filter={}".format(sig_val(pf_hash)))
                else:
                    sig.append("phase_filter=<None>")
            else:
                sig.append("phase_filter=<None>")
        except:
            sig.append("phase_filter=<None>")

        # View Filters (reference global view_filters domain)
        # IMPORTANT: Filter order matters (filter stack is order-sensitive)
        try:
            filter_ids = list(v.GetFilters())
            if filter_ids:
                # Preserve order (do NOT sort)
                filter_hashes = []
                for i, fid in enumerate(filter_ids):
                    try:
                        f_elem = doc.GetElement(fid)
                        f_uid = canon_str(getattr(f_elem, "UniqueId", None)) if f_elem else None
                        # Reference global filter hash
                        f_hash = filter_map.get(f_uid, "<NotInGlobalDomain>") if f_uid else "<None>"

                        # Capture filter visibility override
                        try:
                            visibility = v.GetFilterVisibility(fid)
                            vis_str = safe_str(visibility)
                        except:
                            vis_str = "<None>"

                        # Format: filter[idx]=hash|visibility
                        idx = "{:03d}".format(i)
                        filter_hashes.append("filter[{}]={}|vis={}".format(idx, f_hash, vis_str))
                    except:
                        idx = "{:03d}".format(i)
                        filter_hashes.append("filter[{}]=<Unreadable>".format(idx))

                sig.append("filter_count={}".format(len(filter_ids)))
                sig.extend(filter_hashes)  # Order preserved
            else:
                sig.append("filter_count=0")
        except:
            sig.append("filters=<None>")

        # Display settings (visual style, graphic display options)
        try:
            display_style = safe_str(v.DisplayStyle)
            sig.append("display_style={}".format(sig_val(display_style)))
        except:
            sig.append("display_style=<None>")

        # Sort non-filter settings (filters already added in order)
        # Separate filter entries from other settings
        filter_entries = [s for s in sig if s.startswith("filter[") or s.startswith("filter_count=")]
        other_entries = [s for s in sig if not (s.startswith("filter[") or s.startswith("filter_count="))]

        # Sort other entries (order-insensitive)
        other_entries_sorted = sorted(other_entries)

        # Combine: sorted settings + ordered filters
        sig_final = other_entries_sorted + filter_entries

        # Hash the behavioral signature
        def_hash = make_hash(sig_final)

        rec = {
            "id": safe_str(v.Id.IntegerValue),
            "uid": uid or "",
            "name": name,  # metadata only
            "view_type": None,  # will be populated below
            "def_hash": def_hash,
            "def_signature": sig_final
        }

        # Add view type to record for debugging
        try:
            rec["view_type"] = safe_str(v.ViewType)
        except:
            pass

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

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
            "name":       safe_str(r.get("name", "")),  # metadata
            "view_type":  safe_str(r.get("view_type", "")),  # metadata
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
