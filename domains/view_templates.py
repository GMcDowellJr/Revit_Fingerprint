# -*- coding: utf-8 -*-
"""
View Templates domain extractor.

Captures controlled behavior of view templates including:
- Visibility/Graphics (VG) signature (hidden categories + override presence) [when supported]
- Applied view filters (references global filters domain)                   [when supported]
- Phase filter (references global phase_filters domain)                     [when supported]
- Detail level, discipline, scale
- Display settings (visual style)

NOTE:
- This extractor does not currently capture per-category VG hide/override state.
- Schedule templates emit deterministic sentinels for VG/filters where unsupported.

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
    from Autodesk.Revit.DB import (
        FilteredElementCollector,
        View,
        ViewSchedule,
        ViewType,
        BuiltInParameter,
    )
except ImportError:
    FilteredElementCollector = None
    View = None
    ViewSchedule = None
    ViewType = None
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
    phase_filter_map = ctx.get("phase_filter_uid_to_hash", {}) if ctx else {}
    debug_vg_details = bool(ctx.get("debug_vg_details", False)) if ctx else False

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

        # Determine whether this view type supports VG/filter APIs.
        # Evidence: ViewSchedule templates throw on GetFilters / VG overrides.
        is_schedule = False
        try:
            if ViewSchedule and isinstance(v, ViewSchedule):
                is_schedule = True
            elif ViewType and safe_str(v.ViewType) == safe_str(ViewType.Schedule):
                is_schedule = True
        except:
            pass

        # Build template behavioral signature
        sig = []
        
        # CONTRACT: Schedule view templates only expose Phase Filter + Appearance include state.
        # Do not add additional tokens here without standards approval.

        if is_schedule:
            # Schedule templates only control:
            # - Phase Filter
            # - Appearance (include only)
            # All other settings are instance-specific or unsupported.

            # View Type
            try:
                vtype = safe_str(v.ViewType)
                sig.append("view_type={}".format(sig_val(vtype)))
            except:
                sig.append("view_type=<None>")

            # Phase Filter (reference global phase_filters domain)
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p and p.HasValue:
                    pf_id = p.AsElementId()
                    if pf_id and pf_id.IntegerValue > 0:
                        pf_elem = doc.GetElement(pf_id)
                        pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                        pf_hash = phase_filter_map.get(pf_uid, "<NotInGlobalDomain>") if pf_uid else "<None>"
                        sig.append("phase_filter={}".format(sig_val(pf_hash)))
                    else:
                        sig.append("phase_filter=<None>")
                else:
                    sig.append("phase_filter=<None>")
            except:
                sig.append("phase_filter=<None>")

            # Template-controlled parameters (Include checkboxes)
            try:
                tpl_ids = v.GetTemplateParameterIds() or []
                tpl_bips = set(
                    pid.IntegerValue for pid in tpl_ids
                    if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
                )
            except:
                tpl_bips = set()

            try:
                sig.append(
                    "include_phase_filter={}".format(
                        int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
                    )
                )
            except:
                sig.append("include_phase_filter=False")

            try:
                sig.append(
                    "include_appearance={}".format(
                        int(BuiltInParameter.VIEW_SCHEDULE_APPEARANCE) in tpl_bips
                    )
                )
            except:
                sig.append("include_appearance=False")

            # Finalize schedule signature
            sig_final = sorted(sig)
            def_hash = make_hash(sig_final)

            records.append({
                "id": safe_str(v.Id.IntegerValue),
                "uid": uid or "",
                "name": name,
                "view_type": safe_str(v.ViewType),
                "def_hash": def_hash,
                "def_signature": sig_final,
            })

            per_hashes.append(def_hash)
            info["debug_kept"] += 1
            continue

        # Template-controlled parameters ("Include" surface)
        # Explicit binary flags for the behaviors we fingerprint (readable, schedule-style).
        try:
            tpl_ids = v.GetTemplateParameterIds() or []
            tpl_bips = set(
                pid.IntegerValue for pid in tpl_ids
                if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
            )
        except:
            tpl_bips = set()

        def _incl_bip_name(bip_name):
            try:
                bip = getattr(BuiltInParameter, bip_name, None)
                if bip is None:
                    return False
                return int(bip) in tpl_bips
            except:
                return False

        # Core settings we fingerprint
        sig.append("include_detail_level={}".format(_incl_bip_name("VIEW_DETAIL_LEVEL")))
        sig.append("include_scale={}".format(_incl_bip_name("VIEW_SCALE")))
        sig.append("include_discipline={}".format(_incl_bip_name("VIEW_DISCIPLINE")))
        sig.append("include_phase_filter={}".format(_incl_bip_name("VIEW_PHASE_FILTER")))

        # Display style: best-effort BIP mapping (varies by Revit version)
        include_display_style = (
            _incl_bip_name("MODEL_GRAPHICS_STYLE") or
            _incl_bip_name("VIEW_DISPLAYSTYLE") or
            _incl_bip_name("VIEW_DISPLAY_STYLE") or
            _incl_bip_name("VIEWER_DISPLAY_STYLE")
        )
        sig.append("include_display_style={}".format(include_display_style))

        # Filters + VG are conceptual surfaces; BIP names can vary by version.
        include_filters = (
            _incl_bip_name("VIEW_FILTERS") or
            _incl_bip_name("VIS_GRAPHICS_FILTERS")
        )
        sig.append("include_filters={}".format(include_filters))

        include_vg = (
            _incl_bip_name("VIS_GRAPHICS_MODEL") or
            _incl_bip_name("VIS_GRAPHICS_ANNOTATION") or
            _incl_bip_name("VIS_GRAPHICS")
        )
        sig.append("include_vg={}".format(include_vg))

        # View Type (plan, section, elevation, 3D, etc.)
        try:
            vtype = safe_str(v.ViewType)
            sig.append("view_type={}".format(sig_val(vtype)))
        except:
            sig.append("view_type=<None>")

        # Detail Level
        if not is_schedule:
            try:
                detail_level = safe_str(v.DetailLevel)
                sig.append("detail_level={}".format(sig_val(detail_level)))
            except:
                sig.append("detail_level=<None>")

        # Scale
        if not is_schedule:
            try:
                scale = v.Scale
                sig.append("scale={}".format(sig_val(scale)))
            except:
                sig.append("scale=<None>")

        # Discipline
        if not is_schedule:
            try:
                p = v.get_Parameter(BuiltInParameter.VIEW_DISCIPLINE) if BuiltInParameter else None
                if p and p.HasValue:
                    discipline = safe_str(p.AsValueString())
                    sig.append("discipline={}".format(sig_val(discipline)))
                else:
                    sig.append("discipline=<None>")
            except:
                sig.append("discipline=<None>")

        # Phase Filter (reference global phase_filters domain)
        try:
            pf_param = None
            try:
                pf_param = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
            except:
                pf_param = None

            # Schedule fallback (English name; avoids API calls that throw)
            if is_schedule and (pf_param is None or (hasattr(pf_param, "HasValue") and not pf_param.HasValue)):
                try:
                    pf_param = v.LookupParameter("Phase Filter")
                except:
                    pass

            if pf_param and getattr(pf_param, "HasValue", False):
                pf_id = pf_param.AsElementId()
                if pf_id and pf_id.IntegerValue > 0:
                    pf_elem = doc.GetElement(pf_id)
                    pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                    pf_hash = phase_filter_map.get(pf_uid, "<NotInGlobalDomain>") if pf_uid else "<None>"
                    sig.append("phase_filter={}".format(sig_val(pf_hash)))
                else:
                    sig.append("phase_filter=<None>")
            else:
                sig.append("phase_filter=<None>")
        except:
            sig.append("phase_filter=<None>")

        # Visibility/Graphics (VG) signature
        # Contract: avoid names + avoid positive element ids in hash.
        # Hashes are based on negative category ids + stable primitives only.
        if not is_schedule:
            try:
                from Autodesk.Revit.DB import CategoryType, OverrideGraphicSettings, Color
            except:
                CategoryType = None
                OverrideGraphicSettings = None
                Color = None

            vg_records = []  # detailed per-category records (debug only, optional)
            vg_sig_records = []  # hashed record lines (deterministic)

            try:
                cats = doc.Settings.Categories
            except:
                cats = None

            default_ogs = None
            try:
                default_ogs = OverrideGraphicSettings() if OverrideGraphicSettings else None
            except:
                default_ogs = None

            def _rgb(c):
                try:
                    return "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
                except:
                    return "<None>"

            def _bool01(x):
                try:
                    return "1" if bool(x) else "0"
                except:
                    return "0"

            def _int_or_none(x):
                try:
                    if x is None:
                        return "<None>"
                    return safe_str(int(x))
                except:
                    return "<None>"

            if cats and default_ogs:
                for cat in cats:
                    if cat is None:
                        continue

                    # Skip import categories (often unstable/noisy)
                    try:
                        if CategoryType and cat.CategoryType == CategoryType.Import:
                            continue
                    except:
                        pass

                    try:
                        cid = cat.Id
                        cid_int = cid.IntegerValue if cid else None
                        if cid_int is None:
                            continue
                    except:
                        continue

                    # Hidden state
                    hidden = False
                    try:
                        hidden = bool(v.GetCategoryHidden(cid))
                    except:
                        hidden = False

                    # Overrides (no ids are recorded; pattern ids become boolean flags only)
                    try:
                        ogs = v.GetCategoryOverrides(cid)
                    except:
                        ogs = None

                    if not ogs:
                        # Record hidden-only categories (optional; still deterministic)
                        if hidden:
                            line = "cat={}|hidden=1|ovr=0".format(cid_int)
                            vg_sig_records.append(line)
                            if debug_vg_details:
                                vg_records.append(line)
                        continue

                    # Extract stable primitives
                    try: dl = ogs.DetailLevel
                    except: dl = None                    
                    try: proj_wt = ogs.ProjectionLineWeight
                    except: proj_wt = None
                    try: cut_wt = ogs.CutLineWeight
                    except: cut_wt = None
                    try: proj_col = ogs.ProjectionLineColor
                    except: proj_col = None
                    try: cut_col = ogs.CutLineColor
                    except: cut_col = None
                    try: halftone = ogs.Halftone
                    except: halftone = False
                    try: trans = ogs.Transparency
                    except: trans = None

                    # Pattern overrides as boolean flags (never record ElementId)
                    try: proj_pat_ovr = (ogs.ProjectionLinePatternId != default_ogs.ProjectionLinePatternId)
                    except: proj_pat_ovr = False
                    try: cut_pat_ovr = (ogs.CutLinePatternId != default_ogs.CutLinePatternId)
                    except: cut_pat_ovr = False

                    # Determine "has override" by comparing stable primitives + pattern override flags
                    has_override = False
                    try:
                        if proj_wt != default_ogs.ProjectionLineWeight: has_override = True
                        elif cut_wt != default_ogs.CutLineWeight: has_override = True
                        elif _rgb(proj_col) != _rgb(default_ogs.ProjectionLineColor): has_override = True
                        elif _rgb(cut_col) != _rgb(default_ogs.CutLineColor): has_override = True
                        elif _bool01(halftone) != _bool01(default_ogs.Halftone): has_override = True
                        elif _int_or_none(trans) != _int_or_none(default_ogs.Transparency): has_override = True
                        elif safe_str(dl) != safe_str(default_ogs.DetailLevel): has_override = True                       
                        elif proj_pat_ovr or cut_pat_ovr: has_override = True
                    except:
                        has_override = False

                    # Keep record only if something is non-default (hidden or overrides)
                    if hidden or has_override:
                        line = (
                            "cat={}|hidden={}|ovr={}|dl={}|proj_wt={}|cut_wt={}|"
                            "proj_col={}|cut_col={}|half={}|trans={}|"
                            "proj_pat_ovr={}|cut_pat_ovr={}"
                        ).format(
                            cid_int,
                            "1" if hidden else "0",
                            "1" if has_override else "0",
                            safe_str(dl),
                            _int_or_none(proj_wt),
                            _int_or_none(cut_wt),
                            _rgb(proj_col),
                            _rgb(cut_col),
                            _bool01(halftone),
                            _int_or_none(trans),
                            _bool01(proj_pat_ovr),
                            _bool01(cut_pat_ovr),
                        )

                        vg_sig_records.append(line)
                        if debug_vg_details:
                            vg_records.append(line)

            # Deterministic: sort records (order-insensitive surface)
            vg_sig_sorted = sorted(set(vg_sig_records))

            # Summary tokens (readable + hash reflects detail)
            sig.append("vg_record_count={}".format(len(vg_sig_sorted)))
            sig.append("vg_records_hash={}".format(make_hash(vg_sig_sorted) if vg_sig_sorted else "<None>"))

            # Optional legacy tokens (keep for now if downstream expects them)
            # (You can delete these later once consumers migrate.)
            hidden_only = []
            ovr_only = []
            for r in vg_sig_sorted:
                try:
                    # cat=<int>|hidden=...|ovr=...
                    parts = r.split("|")
                    cid_part = parts[0]  # cat=...
                    hidden_part = parts[1]  # hidden=...
                    ovr_part = parts[2]  # ovr=...
                    cid_int = int(cid_part.split("=")[1])
                    if hidden_part.endswith("=1"):
                        hidden_only.append(cid_int)
                    if ovr_part.endswith("=1"):
                        ovr_only.append(cid_int)
                except:
                    pass

            hidden_sorted = sorted(set(hidden_only))
            ovr_sorted = sorted(set(ovr_only))

            sig.append("vg_hidden_count={}".format(len(hidden_sorted)))
            sig.append("vg_hidden_cats_hash={}".format(make_hash([safe_str(i) for i in hidden_sorted]) if hidden_sorted else "<None>"))
            sig.append("vg_ogs_count={}".format(len(ovr_sorted)))
            sig.append("vg_ogs_cats_hash={}".format(make_hash([safe_str(i) for i in ovr_sorted]) if ovr_sorted else "<None>"))

            # Stash detailed records for this template (metadata only)
            _vg_records_for_rec = vg_records if debug_vg_details else None
        else:
            _vg_records_for_rec = None

        # View Filters (reference global view_filters domain)
        # IMPORTANT: Filter order matters (filter stack is order-sensitive)
        if not is_schedule:
            try:
                filter_ids = list(v.GetFilters())
                if filter_ids:
                    filter_hashes = []
                    for i, fid in enumerate(filter_ids):
                        try:
                            f_elem = doc.GetElement(fid)
                            f_uid = canon_str(getattr(f_elem, "UniqueId", None)) if f_elem else None
                            f_hash = filter_map.get(f_uid, "<NotInGlobalDomain>") if f_uid else "<None>"

                            try:
                                visibility = v.GetFilterVisibility(fid)
                                vis_str = safe_str(visibility)
                            except:
                                vis_str = "<None>"

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
                sig.append("filters=<Unreadable>")

        if not is_schedule:
            # Display settings (visual style, graphic display options)
            try:
                display_style = safe_str(v.DisplayStyle)
                sig.append("display_style={}".format(sig_val(display_style)))
            except:
                sig.append("display_style=<None>")

        # Sort non-filter settings (filters already added in order)
        filter_entries = [s for s in sig if s.startswith("filter[") or s.startswith("filter_count=") or s.startswith("filters=")]
        other_entries = [s for s in sig if s not in filter_entries]

        other_entries_sorted = sorted(other_entries)
        sig_final = other_entries_sorted + filter_entries

        def_hash = make_hash(sig_final)

        rec = {
            "id": safe_str(v.Id.IntegerValue),
            "uid": uid or "",
            "name": name,  # metadata only
            "view_type": None,  # populated below
            "def_hash": def_hash,
            "def_signature": sig_final
        }

        try:
            rec["view_type"] = safe_str(v.ViewType)
        except:
            pass
            
        # Optional VG detail dump (metadata only; not in hash surface)
        try:
            if debug_vg_details and _vg_records_for_rec:
                rec["vg_records"] = list(_vg_records_for_rec)
        except:
            pass

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

    # metadata-only
    info["names"] = sorted(set(names))

    # IMPORTANT: count should represent templates captured, not unique names
    info["count"] = len(records)

    info["records"] = sorted(records, key=lambda r: (r.get("name", ""), r.get("id", "")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),      # metadata
            "view_type":  safe_str(r.get("view_type", "")), # metadata
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
