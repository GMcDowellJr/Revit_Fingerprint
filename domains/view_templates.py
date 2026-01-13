# -*- coding: utf-8 -*-
"""
View Templates domain extractor.

Captures a deterministic fingerprint of view template behavior without relying on
project-specific ids (other than stable negative category ids and built-in parameters).

Legacy hash:
- Continues to use existing behavior including sentinel strings where present.
- Uses ctx maps: filter_uid_to_hash, phase_filter_uid_to_hash.

semantic_v2 hash (additive):
- Uses only semantic-safe fields and upstream semantic_v2 hashes.
- BLOCKS (hash_v2=None) if any required dependency resolution fails:
  - any referenced phase filter cannot be resolved to phase_filter_uid_to_hash_v2
  - any referenced view filter cannot be resolved to filter_uid_to_hash_v2
- No sentinel hashing for v2.
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
    sig_val,
    fnum,
    canon_num,
    canon_bool,
    canon_id,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)


try:
    from Autodesk.Revit.DB import (
        FilteredElementCollector,
        View,
        ViewSchedule,
        BuiltInParameter,
        OverrideGraphicSettings,
        Category,
        CategoryType,
    )
except Exception as e:
    FilteredElementCollector = None
    View = None
    ViewSchedule = None
    BuiltInParameter = None
    OverrideGraphicSettings = None
    Category = None
    CategoryType = None


def extract(doc, ctx=None):
    """
    Extract view templates fingerprint.

    Args:
        doc: Revit document
        ctx: context dict with mappings from other domains

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

        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},
    }

    # Get context mappings (may be None if global domains not run)
    filter_map = ctx.get("filter_uid_to_hash", {}) if ctx else {}
    phase_filter_map = ctx.get("phase_filter_uid_to_hash", {}) if ctx else {}
    filter_map_v2 = ctx.get("filter_uid_to_hash_v2", {}) if ctx else {}
    phase_filter_map_v2 = ctx.get("phase_filter_uid_to_hash_v2", {}) if ctx else {}
    line_pattern_map_v2 = ctx.get("line_pattern_uid_to_hash_v2", {}) if ctx else {}

    debug_vg_details = bool(ctx.get("debug_vg_details", False)) if ctx else False

    try:
        col = list(FilteredElementCollector(doc).OfClass(View))
    except Exception as e:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    per_hashes_v2 = []
    v2_any_blocked = False

    def _v2_block(reason):
        nonlocal v2_any_blocked
        v2_any_blocked = True
        info["debug_v2_blocked"] += 1
        try:
            info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
        except Exception as e:
            pass

    for v in col:
        # Only process view templates
        try:
            is_template = v.IsTemplate
        except Exception as e:
            is_template = False

        if not is_template:
            info["debug_not_template"] += 1
            continue

        # name/uid metadata
        name = canon_str(getattr(v, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = S_MISSING
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(v, "UniqueId", None))
        except Exception as e:
            uid = None

        if not uid:
            info["debug_missing_uid"] += 1

        # v2 per-template signature (contract semantic)
        v2_ok = True
        sig_v2 = []

        # Determine whether this view type supports VG/filter APIs.
        # Evidence: ViewSchedule templates behave differently.
        is_schedule = False
        try:
            is_schedule = isinstance(v, ViewSchedule)
        except Exception as e:
            is_schedule = False

        # -----------------------------------------
        # SCHEDULE templates: minimal stable surface
        # -----------------------------------------
        if is_schedule:
            sig = []

            # Template-controlled parameters ("Include" surface)
            try:
                tpl_ids = v.GetTemplateParameterIds() or []
                tpl_bips = set(
                    pid.IntegerValue for pid in tpl_ids
                    if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
                )
            except Exception as e:
                tpl_bips = set()

            # Include flags (stable)
            try:
                sig.append(
                    "include_phase_filter={}".format(
                        int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
                    )
                )
            except Exception as e:
                sig.append("include_phase_filter=False")

            try:
                sig.append(
                    "include_filters={}".format(
                        int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips
                    )
                )
            except Exception as e:
                sig.append("include_filters=False")

            try:
                sig.append(
                    "include_vg={}".format(
                        int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips
                    )
                )
            except Exception as e:
                sig.append("include_vg=False")

            try:
                sig.append(
                    "include_appearance={}".format(
                        int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips
                    )
                )
            except Exception as e:
                sig.append("include_appearance=False")

            # Phase Filter (reference global phase_filters domain) - legacy
            try:
                include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
            except Exception as e:
                include_pf = False

            if include_pf:
                try:
                    pf_id = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER).AsElementId()
                    pf_elem = doc.GetElement(pf_id) if pf_id else None
                    if pf_elem:
                        pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                        pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                        sig.append("phase_filter={}".format(sig_val(pf_hash)))
                        # v2: require upstream v2 hash when phase filter is present
                        if v2_ok:
                            pf_hash_v2 = None
                            try:
                                pf_hash_v2 = phase_filter_map_v2.get(pf_uid) if pf_uid else None
                            except Exception as e:
                                pf_hash_v2 = None
                            if not pf_hash_v2:
                                _v2_block("phase_filter_unresolved")
                                v2_ok = False
                            else:
                                sig_v2.append("phase_filter_hash={}".format(sig_val(pf_hash_v2)))
                    else:
                        sig.append(f"phase_filter={S_MISSING}")
                except Exception as e:
                    info["debug_fail_read"] += 1
                    sig.append(f"phase_filter={S_UNREADABLE}")
            else:
                sig.append("phase_filter={S_MISSING}")

            # NOTE: Schedule filter stack + VG signatures are not consistently supported across versions.
            # We keep schedule signature minimal and stable.

            # Finalize schedule signature
            sig_final = sorted(sig)
            def_hash = make_hash(sig_final)

            # v2 finalize (schedule)
            if v2_ok:
                try:
                    sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                    sig_v2_final = sorted(set(sig_v2))
                    def_hash_v2 = make_hash(sig_v2_final)
                    per_hashes_v2.append(def_hash_v2)
                except Exception as e:
                    _v2_block("schedule_finalize_failed")
                    v2_ok = False

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

        # -----------------------------------------
        # NON-SCHEDULE templates
        # -----------------------------------------
        sig = []

        # Template-controlled parameters ("Include" surface)
        try:
            tpl_ids = v.GetTemplateParameterIds() or []
            tpl_bips = set(
                pid.IntegerValue for pid in tpl_ids
                if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
            )
        except Exception as e:
            tpl_bips = set()

        # Include flags (stable)
        try:
            sig.append(
                "include_phase_filter={}".format(
                    int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
                )
            )
        except Exception as e:
            sig.append("include_phase_filter=False")

        try:
            sig.append(
                "include_filters={}".format(
                    int(BuiltInParameter.VIS_GRAPHICS_FILTERS) in tpl_bips
                )
            )
        except Exception as e:
            sig.append("include_filters=False")

        try:
            sig.append(
                "include_vg={}".format(
                    int(BuiltInParameter.VIS_GRAPHICS_OVERRIDES) in tpl_bips
                )
            )
        except Exception as e:
            sig.append("include_vg=False")

        try:
            sig.append(
                "include_appearance={}".format(
                    int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips
                )
            )
        except Exception as e:
            sig.append("include_appearance=False")

        # Phase Filter (reference global phase_filters domain)
        try:
            include_pf = int(BuiltInParameter.VIEW_PHASE_FILTER) in tpl_bips
        except Exception as e:
            include_pf = False

        if include_pf:
            try:
                pf_id = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER).AsElementId()
                pf_elem = doc.GetElement(pf_id) if pf_id else None
                if pf_elem:
                    pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                    pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                    sig.append("phase_filter={}".format(sig_val(pf_hash)))
                    # v2: require upstream v2 hash when phase filter is present
                    if v2_ok:
                        pf_hash_v2 = None
                        try:
                            pf_hash_v2 = phase_filter_map_v2.get(pf_uid) if pf_uid else None
                        except Exception as e:
                            pf_hash_v2 = None
                        if not pf_hash_v2:
                            _v2_block("phase_filter_unresolved")
                            v2_ok = False
                        else:
                            sig_v2.append("phase_filter_hash={}".format(sig_val(pf_hash_v2)))
                else:
                    sig.append(f"phase_filter={S_MISSING}")
            except Exception as e:
                info["debug_fail_read"] += 1
                sig.append(f"phase_filter={S_UNREADABLE}")
        else:
            sig.append("phase_filter={S_MISSING}")

        # Visibility/Graphics (VG) signature
        # Contract: avoid names + avoid positive element ids in hash.
        # We hash only negative category ids (BuiltInCategory-style) for hidden + overridden categories.
        try:
            cats = doc.Settings.Categories
        except Exception as e:
            cats = None

        vg_sig_records = []
        vg_records = []
        if cats:
            try:
                default_ogs = OverrideGraphicSettings()
            except Exception as e:
                default_ogs = None

            # Iterate all categories; keep deterministic ordering by category id
            try:
                cats_list = list(cats)
            except Exception as e:
                cats_list = []

            def cat_int_id(c):
                try:
                    return int(c.Id.IntegerValue)
                except Exception as e:
                    return None

            cats_list = [c for c in cats_list if c is not None]
            cats_list = sorted(cats_list, key=lambda c: (cat_int_id(c) is None, cat_int_id(c) or 0))

            for c in cats_list:
                cid_int = cat_int_id(c)
                if cid_int is None:
                    continue

                # Only include negative category ids (built-in categories)
                if cid_int >= 0:
                    continue

                # Skip import categories when possible
                try:
                    if CategoryType and c.CategoryType == CategoryType.Import:
                        continue
                except Exception as e:
                    pass

                # Hidden?
                try:
                    hidden = bool(v.GetCategoryHidden(c.Id))
                except Exception as e:
                    hidden = False

                # Overrides
                try:
                    ogs = v.GetCategoryOverrides(c.Id)
                except Exception as e:
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
                try:
                    try:
                        dl = ogs.DetailLevel
                        dl_int = int(dl)
                    except Exception as e:
                        dl_int = None

                    try:
                        proj_wt = ogs.ProjectionLineWeight
                    except Exception as e:
                        proj_wt = None
                    try:
                        cut_wt = ogs.CutLineWeight
                    except Exception as e:
                        cut_wt = None
                    try:
                        proj_col = ogs.ProjectionLineColor
                    except Exception as e:
                        proj_col = None
                    try:
                        cut_col = ogs.CutLineColor
                    except Exception as e:
                        cut_col = None
                    try:
                        halftone = ogs.Halftone
                    except Exception as e:
                        halftone = False
                    try:
                        trans = ogs.Transparency
                    except Exception as e:
                        trans = None

                    # Pattern overrides as boolean flags (never record ElementId)
                    try:
                        proj_pat_ovr = (ogs.ProjectionLinePatternId != default_ogs.ProjectionLinePatternId) if default_ogs else False
                    except Exception as e:
                        proj_pat_ovr = False
                    try:
                        cut_pat_ovr = (ogs.CutLinePatternId != default_ogs.CutLinePatternId) if default_ogs else False
                    except Exception as e:
                        cut_pat_ovr = False

                    # Determine "has override" by comparing stable primitives + pattern override flags
                    has_override = False
                    try:
                        if dl_int is not None:
                            has_override = True
                        if proj_wt is not None and int(proj_wt) >= 0:
                            has_override = True
                        if cut_wt is not None and int(cut_wt) >= 0:
                            has_override = True
                        if proj_col is not None:
                            has_override = True
                        if cut_col is not None:
                            has_override = True
                        if halftone:
                            has_override = True
                        if trans is not None and int(trans) >= 0:
                            has_override = True
                        if proj_pat_ovr:
                            has_override = True
                        if cut_pat_ovr:
                            has_override = True
                    except Exception as e:
                        pass

                    if not has_override:
                        if hidden:
                            line = "cat={}|hidden=1|ovr=0".format(cid_int)
                            vg_sig_records.append(line)
                            if debug_vg_details:
                                vg_records.append(line)
                        continue

                    # Pack (avoid ids; colors are packed as RGB triples)
                    try:
                        proj_col_s = "{}-{}-{}".format(proj_col.Red, proj_col.Green, proj_col.Blue) if proj_col else S_MISSING
                    except Exception as e:
                        proj_col_s = S_MISSING
                    try:
                        cut_col_s = "{}-{}-{}".format(cut_col.Red, cut_col.Green, cut_col.Blue) if cut_col else S_MISSING
                    except Exception as e:
                        cut_col_s = S_MISSING

                    line = (
                        "cat={}|hidden={}|ovr=1|dl={}|proj_wt={}|cut_wt={}|proj_col={}|cut_col={}|half={}|trans={}|"
                        "proj_pat_ovr={}|cut_pat_ovr={}"
                    ).format(
                        cid_int,
                        int(bool(hidden)),
                        sig_val(dl_int),
                        sig_val(proj_wt),
                        sig_val(cut_wt),
                        sig_val(proj_col_s),
                        sig_val(cut_col_s),
                        int(bool(halftone)),
                        sig_val(trans),
                        int(bool(proj_pat_ovr)),
                        int(bool(cut_pat_ovr)),
                    )

                    vg_sig_records.append(line)
                    if debug_vg_details:
                        vg_records.append(line)
                except Exception as e:
                    info["debug_fail_read"] += 1
                    continue

        if vg_sig_records:
            vg_sig_sorted = sorted(vg_sig_records)
            sig.append("vg_count={}".format(sig_val(len(vg_sig_sorted))))
            for i, line in enumerate(vg_sig_sorted):
                sig.append("vg[{}]={}".format("{:04d}".format(i), sig_val(line)))
        else:
            sig.append("vg_count=0")

        # Appearance (placeholder surface; legacy keeps minimal)
        # This can be expanded later with stable primitives as available.
        try:
            include_app = int(BuiltInParameter.VIS_GRAPHICS_APPEARANCE) in tpl_bips
        except Exception as e:
            include_app = False
        sig.append("appearance_included={}".format(int(bool(include_app))))

        # View Filters (reference global view_filters domain)
        # IMPORTANT: Filter order matters (filter stack is order-sensitive)
        filter_hashes = []
        filter_hashes_v2 = []
        if not is_schedule:
            try:
                filter_ids = list(v.GetFilters())
                if filter_ids:
                    filter_hashes = []
                    filter_hashes_v2 = []
                    for i, fid in enumerate(filter_ids):
                        try:
                            f_elem = doc.GetElement(fid)
                            f_uid = canon_str(getattr(f_elem, "UniqueId", None)) if f_elem else None
                            f_hash = filter_map.get(f_uid, S_UNREADABLE) if f_uid else S_MISSING
                            f_hash_v2 = None
                            if v2_ok:
                                try:
                                    f_hash_v2 = filter_map_v2.get(f_uid) if f_uid else None
                                except Exception as e:
                                    f_hash_v2 = None
                                if not f_hash_v2:
                                    _v2_block("filter_unresolved")
                                    v2_ok = False

                            try:
                                visibility = v.GetFilterVisibility(fid)
                                vis_str = safe_str(visibility)
                            except Exception as e:
                                vis_str = S_MISSING

                            idx = "{:03d}".format(i)
                            filter_hashes.append("filter[{}]={}|vis={}".format(idx, f_hash, vis_str))
                            if v2_ok:
                                filter_hashes_v2.append("filter[{}]={}|vis={}".format(idx, f_hash_v2, vis_str))
                        except Exception as e:
                            info["debug_fail_read"] += 1
                            continue
            except Exception as e:
                info["debug_fail_read"] += 1

        if filter_hashes:
            # Preserve order; do not sort
            sig.append("filter_count={}".format(sig_val(len(filter_hashes))))
            sig.extend(filter_hashes)
        else:
            sig.append("filter_count=0")

        # Finalize signature
        # Split: stable entries + filter entries (order-sensitive)
        other_entries = [s for s in sig if not s.startswith("filter[")]
        filter_entries = [s for s in sig if s.startswith("filter[")]

        other_entries_sorted = sorted(other_entries)
        sig_final = other_entries_sorted + filter_entries

        def_hash = make_hash(sig_final)

        # v2 finalize (non-schedule)
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
                if filter_hashes_v2:
                    sig_v2 = [s for s in sig_v2 if not s.startswith("filter[")]
                    sig_v2.extend(filter_hashes_v2)
                sig_v2_final = sorted(set(sig_v2))
                def_hash_v2 = make_hash(sig_v2_final)
                per_hashes_v2.append(def_hash_v2)
            except Exception as e:
                _v2_block("template_finalize_failed")
                v2_ok = False

        rec = {
            "id": safe_str(v.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "view_type": safe_str(v.ViewType),
            "def_hash": def_hash,
            "def_signature": sig_final,
        }

        # Optional VG debug
        if debug_vg_details:
            try:
                rec["vg_debug"] = _vg_records_for_rec
            except Exception as e:
                pass

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

    # Finalize
    info["names"] = sorted(set(names))

    # IMPORTANT: count should represent templates captured, not unique names
    info["count"] = len(records)

    info["records"] = sorted(records, key=lambda r: (r.get("name", ""), r.get("id", "")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if v2_any_blocked:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"]) if info["signature_hashes_v2"] else None

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),      # metadata
            "view_type":  safe_str(r.get("view_type", "")), # metadata
        } for r in recs]
    except Exception as e:
        info["record_rows"] = []

    return info
