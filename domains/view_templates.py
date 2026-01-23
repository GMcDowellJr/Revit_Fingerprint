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
from core.collect import collect_instances
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

from core.record_v2 import (
    ITEM_Q_OK,
    make_identity_item,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
    phase2_join_hash,
)

try:
    from Autodesk.Revit.DB import (
        View,
        ViewSchedule,
        BuiltInParameter,
        OverrideGraphicSettings,
        Category,
        CategoryType,
    )
except Exception as e:
    View = None
    ViewSchedule = None
    BuiltInParameter = None
    OverrideGraphicSettings = None
    Category = None
    CategoryType = None

def _phase2_build_join_key_items(*, name, view_type, is_schedule):
    """Build Phase-2 join-key IdentityItems (domain-specific, hypothesis-only).

    Join-key hypothesis (reversible):
      - view_template.name + view_template.view_type + view_template.is_schedule
        are the natural join components for cross-file stability checks.
    """
    name_v, name_q = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
    vt_v, vt_q = phase2_qv_from_legacy_sentinel_str(view_type, allow_empty=False)
    sched_v, sched_q = phase2_qv_from_legacy_sentinel_str(str(int(bool(is_schedule))), allow_empty=False)

    return [
        make_identity_item("view_template.name", name_v, name_q),
        make_identity_item("view_template.view_type", vt_v, vt_q),
        make_identity_item("view_template.is_schedule", sched_v, sched_q),
    ]


def _phase2_items_from_def_signature(def_signature):
    """Convert legacy def_signature entries ('k=v') into IdentityItems safely.

    Hard rule: do not emit legacy sentinel literals into IdentityItem.v.
    """
    out = []
    for s in (def_signature or []):
        try:
            ss = safe_str(s)
        except Exception:
            continue

        if "=" not in ss:
            # Keep explicit shape, but treat value as missing
            k = "view_template.sig.{}".format(ss)
            out.append(make_identity_item(k, None, "missing"))
            continue

        left, right = ss.split("=", 1)
        k = "view_template.sig.{}".format(safe_str(left).strip())

        rr = safe_str(right).strip()
        # Mechanical unwrapping: some legacy signatures may serialize sentinels with quotes.
        if len(rr) >= 2 and ((rr[0] == rr[-1] == "'") or (rr[0] == rr[-1] == '"')):
            rr = rr[1:-1].strip()

        # If RHS is a packed k=v|k=v|... payload (e.g., vg lines), expand into sub-items so
        # legacy sentinel literals never appear as substrings inside IdentityItem.v.
        if ("|" in rr) and ("=" in rr):
            parts = [p.strip() for p in rr.split("|") if p.strip()]
            for p in parts:
                if "=" not in p:
                    sk = "{}.part".format(k)
                    out.append(make_identity_item(sk, None, "missing"))
                    continue

                subk_raw, subv_raw = p.split("=", 1)
                subk = safe_str(subk_raw).strip()
                subv = safe_str(subv_raw).strip()
                if len(subv) >= 2 and ((subv[0] == subv[-1] == "'") or (subv[0] == subv[-1] == '"')):
                    subv = subv[1:-1].strip()

                sv, sq = phase2_qv_from_legacy_sentinel_str(subv, allow_empty=True)
                out.append(make_identity_item("{}.{}".format(k, subk), sv, sq))
        else:
            v, q = phase2_qv_from_legacy_sentinel_str(rr, allow_empty=True)
            out.append(make_identity_item(k, v, q))

    return phase2_sorted_items(out)


def _phase2_partition_items(items):
    """Partition IdentityItems into semantic/cosmetic/unknown (hypotheses only)."""
    semantic = []
    cosmetic = []
    unknown = []

    for it in (items or []):
        k = safe_str(it.get("k", ""))

        # Cosmetic-ish signals (style / overrides / appearance)
        if (
            ".ovr" in k
            or "appearance" in k
            or "halftone" in k
            or "pattern" in k
            or "lineweight" in k
            or "color" in k
            or "transparency" in k
        ):
            cosmetic.append(it)
            continue

        # Semantic-ish signals (inclusion / visibility / filtering)
        if (
            k.startswith("view_template.sig.include_")
            or k.startswith("view_template.sig.phase_filter")
            or k.startswith("view_template.sig.filter[")
            or k.startswith("view_template.sig.cat[")
            or "visible" in k
        ):
            semantic.append(it)
            continue

        unknown.append(it)

    return (
        phase2_sorted_items(semantic),
        phase2_sorted_items(cosmetic),
        phase2_sorted_items(unknown),
    )

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
        # PR6: deterministic degraded signaling
        "debug_view_context_problem": 0,
        "debug_view_context_reasons": {},
        "debug_collect_types_failed": 0,
    }

    # Get context mappings (may be None if global domains not run)
    phase_filter_map = ctx.get("phase_filter_uid_to_hash", {}) if ctx else {}
    phase_filter_map_v2 = ctx.get("phase_filter_uid_to_hash_v2", {}) if ctx else {}
    line_pattern_map_v2 = ctx.get("line_pattern_uid_to_hash_v2", {}) if ctx else {}

    debug_vg_details = bool(ctx.get("debug_vg_details", False)) if ctx else False

    try:
        col = list(
            collect_instances(
                doc,
                of_class=View,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="view_templates:View:instances",
            )
        )
    except Exception as e:
        info["debug_collect_types_failed"] += 1
        info["_domain_status"] = "degraded"
        info["_domain_diag"] = {
            "degraded_reasons": ["collect_types_failed"],
            "degraded_reason_counts": {"collect_types_failed": 1},
            "error": str(e),
        }
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

        # PR6: view-scoped context snapshot (explicit missing vs unreadable)
        try:
            dv = (ctx or {}).get("_doc_view") if ctx is not None else None
            if dv is not None:
                vi = dv.view_info(v, source="HOST")
                if vi.reasons:
                    info["debug_view_context_problem"] += 1
                    for r in vi.reasons:
                        info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
        except Exception:
            # Context read failed; treat as unreadable (explicit) but keep going
            info["debug_view_context_problem"] += 1
            info["debug_view_context_reasons"]["view_context_unreadable"] = (
                info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
            )

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
                    p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                    if p is None:
                        # Schedule templates often don't expose phase filter meaningfully.
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_id = p.AsElementId()
                        # Invalid/None phase filter for schedules should be treated as NOT_APPLICABLE
                        if not pf_id or canon_id(pf_id) == S_MISSING:
                            sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                        else:
                            pf_elem = doc.GetElement(pf_id)
                            if pf_elem:
                                pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                                pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                                sig.append("phase_filter={}".format(sig_val(pf_hash)))
                                # v2: require upstream v2 hash when phase filter is present
                                if v2_ok:
                                    pf_hash_v2 = None
                                    try:
                                        pf_hash_v2 = phase_filter_map_v2.get(pf_uid) if pf_uid else None
                                    except Exception:
                                        pf_hash_v2 = None
                                    if not pf_hash_v2:
                                        _v2_block("phase_filter_unresolved")
                                        v2_ok = False
                                    else:
                                        sig_v2.append("phase_filter_hash={}".format(sig_val(pf_hash_v2)))
                            else:
                                sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                except Exception as e:
                    info["debug_fail_read"] += 1
                    sig.append(f"phase_filter={S_UNREADABLE}")

            else:
                sig.append(f"phase_filter={S_MISSING}")

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

            rec = {
                "id": safe_str(v.Id.IntegerValue),
                "uid": uid or "",
                "name": name,
                "view_type": safe_str(v.ViewType),
                "def_hash": def_hash,
                "def_signature": sig_final,
            }

            # -------------------------
            # Phase-2 additions (additive, explanatory, reversible)
            # -------------------------
            join_key_items = _phase2_build_join_key_items(
                name=name,
                view_type=safe_str(v.ViewType),
                is_schedule=True,
            )
            join_key_items_sorted = phase2_sorted_items(join_key_items)
            rec["join_key"] = {
                "schema": "view_templates.join_key.v1",
                "hash_alg": "md5_utf8_join_pipe",
                "items": join_key_items_sorted,
                "join_hash": phase2_join_hash(join_key_items_sorted),
            }

            sig_items = _phase2_items_from_def_signature(sig_final)

            # Add explicit file-local identifiers as unknown (do not affect identity_basis)
            uid_v, uid_q = phase2_qv_from_legacy_sentinel_str(uid or None, allow_empty=False)
            extra_unknown = [
                make_identity_item("view_template.uid", uid_v, uid_q),
                make_identity_item("view_template.element_id", safe_str(v.Id.IntegerValue), ITEM_Q_OK),
            ]

            semantic_items, cosmetic_items, unknown_items = _phase2_partition_items(sig_items)
            unknown_items = phase2_sorted_items(list(unknown_items) + extra_unknown)

            rec["phase2"] = {
                "schema": "phase2.view_templates.v1",
                "grouping_basis": "phase2.hypothesis",
                "semantic_items": semantic_items,
                "cosmetic_items": cosmetic_items,
                "unknown_items": unknown_items,
            }

            records.append(rec)

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
                p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
                if p is None:
                    # Schedule templates often don't expose phase filter meaningfully.
                    sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                else:
                    pf_id = p.AsElementId()
                    # Invalid/None phase filter for schedules should be treated as NOT_APPLICABLE
                    if not pf_id or canon_id(pf_id) == S_MISSING:
                        sig.append(f"phase_filter={S_NOT_APPLICABLE}")
                    else:
                        pf_elem = doc.GetElement(pf_id)
                        if pf_elem:
                            pf_uid = canon_str(getattr(pf_elem, "UniqueId", None)) if pf_elem else None
                            pf_hash = phase_filter_map.get(pf_uid, S_UNREADABLE) if pf_uid else S_MISSING
                            sig.append("phase_filter={}".format(sig_val(pf_hash)))
                            # v2: require upstream v2 hash when phase filter is present
                            if v2_ok:
                                pf_hash_v2 = None
                                try:
                                    pf_hash_v2 = phase_filter_map_v2.get(pf_uid) if pf_uid else None
                                except Exception:
                                    pf_hash_v2 = None
                                if not pf_hash_v2:
                                    _v2_block("phase_filter_unresolved")
                                    v2_ok = False
                                else:
                                    sig_v2.append("phase_filter_hash={}".format(sig_val(pf_hash_v2)))
                        else:
                            sig.append(f"phase_filter={S_NOT_APPLICABLE}")
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

                    def _rgb(cobj):
                        try:
                            return (int(cobj.Red), int(cobj.Green), int(cobj.Blue))
                        except Exception:
                            return None

                    proj_rgb = _rgb(proj_col)
                    cut_rgb = _rgb(cut_col)
                    def_proj_rgb = _rgb(default_ogs.ProjectionLineColor) if default_ogs else None
                    def_cut_rgb = _rgb(default_ogs.CutLineColor) if default_ogs else None

                    # Determine "has override" by comparing stable primitives + pattern override flags
                    has_override = False
                    try:
                        if dl_int is not None:
                            has_override = True
                        if proj_wt is not None and int(proj_wt) >= 0:
                            has_override = True
                        if cut_wt is not None and int(cut_wt) >= 0:
                            has_override = True
                            
                        if proj_rgb is not None and (def_proj_rgb is None or proj_rgb != def_proj_rgb):
                            has_override = True
                        if cut_rgb is not None and (def_cut_rgb is None or cut_rgb != def_cut_rgb):
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
                        proj_col_s = "{}-{}-{}".format(*proj_rgb) if proj_rgb is not None else S_NOT_APPLICABLE
                    except Exception:
                        proj_col_s = S_UNREADABLE
                    try:
                        cut_col_s = "{}-{}-{}".format(*cut_rgb) if cut_rgb is not None else S_NOT_APPLICABLE
                    except Exception:
                        cut_col_s = S_UNREADABLE

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

        # View Filters are split into a separate record.v2 domain:
        #   view_filter_applications_view_templates
        # This domain (view_templates) must not depend on filter definitions or applications.

        # Template filter stack identity (order matters)
        # This references definitions by record.v2 sig_hash and includes per-template settings.
        #
        # NOTE: legacy hash may include sentinel strings; v2 must BLOCK if dependencies cannot resolve.
        try:
            filter_ids = list(v.GetFilters() or []) if hasattr(v, "GetFilters") else []
            sig.append("filter_stack_count={}".format(len(filter_ids)))
            if v2_ok:
                sig_v2.append("vts.filter_stack_count={}".format(sig_val(len(filter_ids))))
        except Exception:
            filter_ids = None
            sig.append("filter_stack_count=<UNREADABLE>")
            if v2_ok:
                _v2_block("filter_stack_unreadable")
                v2_ok = False

        if filter_ids is not None:
            # mapping published by view_filter_definitions domain
            vf_map = ctx.get("view_filter_uid_to_sig_hash_v2", {}) if ctx else {}

            for i, fid in enumerate(filter_ids):
                idx3 = "{:03d}".format(i)

                # Resolve filter UniqueId -> definition sig_hash (stable ref)
                f_uid = None
                try:
                    fe = doc.GetElement(fid)
                    f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
                except Exception:
                    f_uid = None

                def_sig = vf_map.get(f_uid) if f_uid else None

                if def_sig:
                    sig.append("filter[{}].def_sig={}".format(idx3, sig_val(def_sig)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].def_sig_hash={}".format(idx3, sig_val(def_sig)))
                else:
                    # legacy: keep explicit unreadable marker
                    sig.append("filter[{}].def_sig=<UNREADABLE>".format(idx3))
                    # v2: cannot produce a correct semantic hash without stable dependency
                    if v2_ok:
                        _v2_block("view_filter_unresolved")
                        v2_ok = False

                # Visibility
                try:
                    vis = bool(v.GetFilterVisibility(fid)) if hasattr(v, "GetFilterVisibility") else None
                except Exception:
                    vis = None

                if vis is None:
                    sig.append("filter[{}].vis=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_visibility_unreadable")
                        v2_ok = False
                else:
                    sig.append("filter[{}].vis={}".format(idx3, int(vis)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].visibility={}".format(idx3, int(vis)))

                # Overrides presence (bool-like)
                try:
                    ogs = v.GetFilterOverrides(fid) if hasattr(v, "GetFilterOverrides") else None
                except Exception:
                    ogs = None

                try:
                    # minimal and stable: just detect any readable non-default
                    has_ovr = False
                    if ogs is not None:
                        if getattr(ogs, "Halftone", False):
                            has_ovr = True
                        for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
                            vattr = getattr(ogs, attr, None)
                            if vattr is not None and int(vattr) > 0:
                                has_ovr = True
                        for attr in ("ProjectionLinePatternId", "CutLinePatternId"):
                            eid = getattr(ogs, attr, None)
                            if eid is not None and int(getattr(eid, "IntegerValue", 0)) not in (0, -1):
                                has_ovr = True

                    sig.append("filter[{}].ovr={}".format(idx3, int(has_ovr)))
                    if v2_ok:
                        sig_v2.append("vts.filter[{}].overrides={}".format(idx3, int(has_ovr)))
                except Exception:
                    sig.append("filter[{}].ovr=<UNREADABLE>".format(idx3))
                    if v2_ok:
                        _v2_block("filter_overrides_unreadable")
                        v2_ok = False

        # Finalize signature (deterministic)
        sig_final = sorted(sig)

        def_hash = make_hash(sig_final)

        # v2 finalize
        if v2_ok:
            try:
                sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
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

        # -------------------------
        # Phase-2 additions (additive, explanatory, reversible)
        # -------------------------
        join_key_items = _phase2_build_join_key_items(
            name=name,
            view_type=safe_str(v.ViewType),
            is_schedule=False,
        )
        join_key_items_sorted = phase2_sorted_items(join_key_items)
        rec["join_key"] = {
            "schema": "view_templates.join_key.v1",
            "hash_alg": "md5_utf8_join_pipe",
            "items": join_key_items_sorted,
            "join_hash": phase2_join_hash(join_key_items_sorted),
        }

        sig_items = _phase2_items_from_def_signature(sig_final)

        # Add explicit file-local identifiers as unknown (do not affect identity_basis)
        uid_v, uid_q = phase2_qv_from_legacy_sentinel_str(uid or None, allow_empty=False)
        extra_unknown = [
            make_identity_item("view_template.uid", uid_v, uid_q),
            make_identity_item("view_template.element_id", safe_str(v.Id.IntegerValue), ITEM_Q_OK),
        ]

        semantic_items, cosmetic_items, unknown_items = _phase2_partition_items(sig_items)
        unknown_items = phase2_sorted_items(list(unknown_items) + extra_unknown)

        rec["phase2"] = {
            "schema": "phase2.view_templates.v1",
            "grouping_basis": "phase2.hypothesis",
            "semantic_items": semantic_items,
            "cosmetic_items": cosmetic_items,
            "unknown_items": unknown_items,
        }

        # Optional VG debug
        if debug_vg_details:
            try:
                rec["vg_debug"] = list(vg_records)
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
    info["hash"] = make_hash(info["signature_hashes"])

    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if v2_any_blocked:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

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

    # PR6: deterministic degraded signaling into contract
    degraded_reason_counts = {}

    try:
        if int(info.get("debug_missing_uid", 0)) > 0:
            degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
    except Exception:
        pass

    try:
        if int(info.get("debug_fail_read", 0)) > 0:
            degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
    except Exception:
        pass

    try:
        if int(info.get("debug_view_context_problem", 0)) > 0:
            # Roll up view context reasons as distinct degraded reasons.
            # NOTE: *_not_applicable signals are expected and must not degrade the domain.
            for k, v in dict(info.get("debug_view_context_reasons", {})).items():
                key = str(k)
                if key.endswith("_not_applicable"):
                    continue
                degraded_reason_counts[key] = int(v)
    except Exception:
        pass

    try:
        if int(info.get("debug_v2_blocked", 0)) > 0:
            degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
    except Exception:
        pass

    if degraded_reason_counts:
        info["_domain_status"] = "degraded"
        info["_domain_diag"] = {
            "degraded_reasons": sorted(degraded_reason_counts.keys()),
            "degraded_reason_counts": degraded_reason_counts,
        }
    else:
        info["_domain_status"] = "ok"
        info["_domain_diag"] = {}

    return info
