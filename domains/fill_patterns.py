
# -*- coding: utf-8 -*-
"""Fill Patterns domain family extractor."""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_instances
from core.canon import canon_str, fnum, canon_num, canon_bool, canon_id, S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
from core.phase2 import phase2_sorted_items, phase2_qv_from_legacy_sentinel_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_UNREADABLE,
    canonicalize_str,
    canonicalize_int,
    canonicalize_bool,
    canonicalize_float,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import FillPatternElement
except ImportError:
    FillPatternElement = None

DEBUG_INCLUDE_FILLPATTERN_SIGNATURES = False
_CTX_FILL_PATTERNS_CACHE_KEY = "_fill_patterns_cache"
_TARGET_DRAFTING_INT = 0
_TARGET_MODEL_INT = 1


def _collect_fill_patterns(doc, ctx):
    if ctx is not None and _CTX_FILL_PATTERNS_CACHE_KEY in ctx:
        return ctx[_CTX_FILL_PATTERNS_CACHE_KEY]
    col = list(
        collect_instances(
            doc,
            of_class=FillPatternElement,
            require_unique_id=True,
            cctx=(ctx or {}).get("_collect") if ctx is not None else None,
            cache_key="fill_patterns:FillPatternElement:instances",
        )
    )
    if ctx is not None:
        ctx[_CTX_FILL_PATTERNS_CACHE_KEY] = col
    return col

def extract_drafting(doc, ctx=None):
    _TARGET_INT = _TARGET_DRAFTING_INT
    _TARGET_NAME = "Drafting"
    """
    Extract Fill Patterns fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
                "records": [],

        # debug counters so you can see why things disappear
        "debug_total_elements": 0,
        "debug_kept": 0,
        "debug_skipped_no_name": 0,
        "debug_skipped_wrong_target": 0,
        "debug_fail_getfillpattern": 0,
        "debug_fail_grid_read": 0,

        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},
    }

    try:
        col = _collect_fill_patterns(doc, ctx)
    except Exception as e:
        return info
    info["raw_count"] = len(col)

    def f(v, nd=9):
        if v is None:
            return S_MISSING
        try:
            return format(float(v), ".{}f".format(nd))
        except Exception as e:
            return canon_str(v)

    def grid_sig(fp, i):
        # Return a stable list; never raise
        idx = "{:03d}".format(int(i))
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except Exception as e:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except Exception as e:
                g = None

        if g is None:
            info["debug_fail_grid_read"] += 1
            return ["grid[{}].unreadable={}".format(idx, S_MISSING)]

        parts = []

        def add_float(prop_name, key):
            try:
                v = getattr(g, prop_name)
                parts.append("grid[{}].{}={}".format(idx, key, f(v)))
            except Exception as e:
                parts.append("grid[{}].{}={}".format(idx, key, S_MISSING))

        # origin can vary across versions; try a couple shapes
        def add_origin_2d():
            # Try UV-style origin (U,V)
            try:
                o = g.Origin
                u = getattr(o, "U", None)
                v = getattr(o, "V", None)
                if u is not None and v is not None:
                    parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
                    return
            except Exception as e:
                pass

            # Try XYZ-style origin but store only X,Y
            try:
                o = g.Origin
                x = getattr(o, "X", None)
                y = getattr(o, "Y", None)
                if x is not None and y is not None:
                    parts.append("grid[{}].origin_xy={},{}".format(idx, f(x), f(y)))
                    return
            except Exception as e:
                pass

            # Try separate scalars
            for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                try:
                    u = getattr(g, u_name)
                    v = getattr(g, v_name)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
                    return
                except Exception as e:
                    pass

            parts.append("grid[{}].origin={}".format(idx, S_MISSING))

        add_float("Angle", "angle")
        add_origin_2d()
        add_float("Offset", "offset")
        add_float("Shift", "shift")

        return parts

    # v2 helpers (strict: block on unreadables / missing)
    def _bump_v2_reason(reason):
        info["debug_v2_blocked"] += 1
        try:
            info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
        except Exception as e:
            pass

    def _grid_sig_v2(fp, i):
        """
        Return (ok, parts, reason). parts contain only numeric primitives.
        """
        idx = "{:03d}".format(int(i))
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except Exception as e:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except Exception as e:
                g = None

        if g is None:
            return False, [], "grid_unreadable"

        parts = []

        def req_float(prop_name, key):
            try:
                v = getattr(g, prop_name)
            except Exception as e:
                return False, "grid_{}_unreadable".format(key)
            if v is None:
                return False, "grid_{}_none".format(key)
            try:
                fv = float(v)
            except Exception as e:
                return False, "grid_{}_not_float".format(key)
            parts.append("grid[{}].{}={}".format(idx, key, canon_str(f(v, 9))))
            return True, None

        # origin: require 2 floats, pick first supported shape
        def req_origin():
            # UV origin
            try:
                o = g.Origin
                u = getattr(o, "U", None)
                v = getattr(o, "V", None)
                if u is not None and v is not None:
                    fu = float(u)
                    fv = float(v)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
                    return True, None
            except Exception as e:
                pass

            # XY origin
            try:
                o = g.Origin
                x = getattr(o, "X", None)
                y = getattr(o, "Y", None)
                if x is not None and y is not None:
                    fx = float(x)
                    fy = float(y)
                    parts.append("grid[{}].origin_xy={},{}".format(idx, canon_str(f(fx, 9)), canon_str(f(fy, 9))))
                    return True, None
            except Exception as e:
                pass

            # scalar origin props
            for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                try:
                    u = getattr(g, u_name)
                    v = getattr(g, v_name)
                    if u is None or v is None:
                        continue
                    fu = float(u)
                    fv = float(v)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
                    return True, None
                except Exception as e:
                    continue

            return False, "grid_origin_unreadable"

        ok, reason = req_float("Angle", "angle")
        if not ok:
            return False, [], reason
        ok, reason = req_origin()
        if not ok:
            return False, [], reason
        ok, reason = req_float("Offset", "offset")
        if not ok:
            return False, [], reason
        ok, reason = req_float("Shift", "shift")
        if not ok:
            return False, [], reason

        return True, parts, None

    # -------------------------
    # Phase 2 (additive-only) builders
    # -------------------------

    def _phase2_try_get_grid(fp, i):
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except Exception:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except Exception:
                g = None
        return g

    def _phase2_add_float(items, k, v, *, unreadable=False):
        if unreadable:
            items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
            return
        v2, q2 = canonicalize_float(v)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_add_int(items, k, v, *, unreadable=False):
        if unreadable:
            items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
            return
        v2, q2 = canonicalize_int(v)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_add_bool(items, k, v, *, unreadable=False):
        if unreadable:
            items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
            return
        v2, q2 = canonicalize_bool(v)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_add_str(items, k, v, *, allow_empty=False):
        if allow_empty:
            v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=True)
        else:
            v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=False)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_build_phase2(name, uid, elem_id_str, fp, elem):
        semantic = []
        cosmetic = []
        coordination = []
        unknown = []

        # cosmetic
        v_name, q_name = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
        cosmetic.append({"k": "fill_pattern.name", "v": v_name, "q": q_name})

        # unknown identifiers (do not affect semantic hypotheses)
        v_uid, q_uid = canonicalize_str(uid)
        unknown.append({"k": "fill_pattern.uid", "v": v_uid, "q": q_uid})
        v_eid, q_eid = canonicalize_str(elem_id_str)
        unknown.append({"k": "fill_pattern.elem_id", "v": v_eid, "q": q_eid})

        # Traceability fields (metadata only — never in hash/sig/join)
        try:
            _eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
            _eid_v, _eid_q = canonicalize_int(_eid_raw)
        except Exception:
            _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
        try:
            _uid_raw = getattr(elem, "UniqueId", None)
            _uid_v, _uid_q = canonicalize_str(_uid_raw)
        except Exception:
            _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
        unknown.append({"k": "fill_pattern.source_element_id", "v": _eid_v, "q": _eid_q})
        unknown.append({"k": "fill_pattern.source_unique_id", "v": _uid_v, "q": _uid_q})

        # target is always _TARGET_NAME for this domain
        semantic.append({"k": "fill_pattern.target", "v": _TARGET_NAME, "q": ITEM_Q_OK})

        if fp is None:
            # Explicit unreadable (GetFillPattern failed)
            _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
            # is_solid in coordination only (filter criterion, not identity)
            _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
        else:
            # is_solid goes to coordination_items only — it is a filter criterion, not identity
            try:
                is_solid = fp.IsSolidFill
            except Exception:
                _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
            else:
                _phase2_add_bool(coordination, "fill_pattern.is_solid", bool(is_solid))

            # grid_count
            try:
                gc = fp.GridCount
            except Exception:
                _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
                gc_i = None
            else:
                if gc is None:
                    _phase2_add_int(semantic, "fill_pattern.grid_count", None)
                    gc_i = None
                else:
                    try:
                        gc_i = int(gc)
                    except Exception:
                        _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
                        gc_i = None
                    else:
                        _phase2_add_int(semantic, "fill_pattern.grid_count", gc_i)

            # grids (no inference; explicit kind for origin)
            if gc_i:
                for i in range(int(gc_i)):
                    idx = "{:03d}".format(int(i))
                    g = _phase2_try_get_grid(fp, i)
                    if g is None:
                        semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        continue

                    # Angle / Offset / Shift
                    try:
                        _phase2_add_float(semantic, "fill_pattern.grid[{}].angle".format(idx), float(getattr(g, "Angle")))
                    except Exception:
                        semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})

                    # Origin (explicit kind)
                    origin_kind = None
                    ox = oy = None

                    # UV origin
                    try:
                        o = g.Origin
                        u = getattr(o, "U", None)
                        v = getattr(o, "V", None)
                        if u is not None and v is not None:
                            origin_kind = "uv"
                            ox = float(u)
                            oy = float(v)
                    except Exception:
                        pass

                    # XY origin
                    if origin_kind is None:
                        try:
                            o = g.Origin
                            x = getattr(o, "X", None)
                            y = getattr(o, "Y", None)
                            if x is not None and y is not None:
                                origin_kind = "xy"
                                ox = float(x)
                                oy = float(y)
                        except Exception:
                            pass

                    # Scalar origin props
                    if origin_kind is None:
                        for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                            try:
                                u2 = getattr(g, u_name)
                                v2 = getattr(g, v_name)
                                if u2 is None or v2 is None:
                                    continue
                                origin_kind = "uv"
                                ox = float(u2)
                                oy = float(v2)
                                break
                            except Exception:
                                continue

                    if origin_kind is None:
                        semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                    else:
                        v_kind, q_kind = canonicalize_str(origin_kind)
                        semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": v_kind, "q": q_kind})

                        if origin_kind == "uv":
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.u".format(idx), ox)
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.v".format(idx), oy)
                        else:
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.x".format(idx), ox)
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.y".format(idx), oy)

                    try:
                        _phase2_add_float(semantic, "fill_pattern.grid[{}].offset".format(idx), float(getattr(g, "Offset")))
                    except Exception:
                        semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})

                    try:
                        _phase2_add_float(semantic, "fill_pattern.grid[{}].shift".format(idx), float(getattr(g, "Shift")))
                    except Exception:
                        semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})

        # Derived structural identity helper for Phase-2:
        # Collapse all per-grid semantic items into a single hash so join-key discovery
        # can treat the grid bundle as one "field" without losing the detailed items.
        #
        # IMPORTANT: grid order is identity-significant; do NOT sort the preimage.
        try:
            grid_like = []
            for it in (semantic or []):
                k = safe_str(it.get("k", ""))
                if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
                    # Stable preimage: include k/q/v so unreadables affect the hash deterministically
                    grid_like.append("k={}|q={}|v={}".format(
                        safe_str(it.get("k", "")),
                        safe_str(it.get("q", "")),
                        safe_str(it.get("v", "")),
                    ))
            grids_def_hash = make_hash(grid_like) if grid_like else None
        except Exception:
            grids_def_hash = None

        if grids_def_hash:
            semantic.append({"k": "fill_pattern.grids_def_hash", "v": grids_def_hash, "q": ITEM_Q_OK})
        else:
            # If we can't compute it, make the failure explicit (but keep it out of identity)
            semantic.append({"k": "fill_pattern.grids_def_hash", "v": None, "q": ITEM_Q_UNREADABLE})

        # Phase-2 bloat control:
        # The full grid definition is already present in identity_basis.items (for sig_hash reproducibility).
        # Avoid duplicating per-grid items in phase2.semantic_items; keep only pointer + small scalars.
        semantic_reduced = []
        for it in (semantic or []):
            k = safe_str(it.get("k", ""))
            if k.startswith("fill_pattern.grid["):
                continue
            semantic_reduced.append(it)

        return {
            "schema": "phase2.{}.v1".format(DOMAIN_NAME),
            "grouping_basis": "phase2.hypothesis",
            "semantic_items": phase2_sorted_items(semantic_reduced),
            "cosmetic_items": phase2_sorted_items(cosmetic),
            "coordination_items": phase2_sorted_items(coordination),
            "unknown_items": phase2_sorted_items(unknown),
        }

    records = []
    per_hashes = []
    per_hashes_v2 = []
    v2_records = []
    v2_sig_hashes = []
    names = []
    uid_to_hash_v2 = {}
    uid_to_hash = {}

    for e in col:
        info["debug_total_elements"] += 1

        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_skipped_no_name"] += 1
            continue

        uid = getattr(e, "UniqueId", "") or ""

        # Always keep the element, even if we can't read its FillPattern
        fp = None
        try:
            fp = e.GetFillPattern()
        except Exception as e:
            fp = None

        # Filter: only process patterns matching this domain's target
        if fp is not None:
            try:
                _fp_target_int = int(fp.Target)
            except Exception:
                _fp_target_int = -1
            if _fp_target_int != _TARGET_INT:
                info["debug_skipped_wrong_target"] += 1
                continue

        # Filter: skip solid fills — system defaults, ungoverned
        if fp is not None:
            try:
                if fp.IsSolidFill:
                    continue
            except Exception:
                pass  # if unreadable, proceed and let field-level q handle it

        names.append(name)

        # -------------------------
        # Legacy signature (UNCHANGED meaning)
        # -------------------------
        if fp is None:
            info["debug_fail_getfillpattern"] += 1
            sig = [
                f"is_solid={S_MISSING}",
                f"target={_TARGET_NAME}",
                f"grid_count={S_MISSING}",
                f"grid[000].unreadable={S_MISSING}",
                "error=GetFillPatternFailed",
            ]
        else:
            is_solid = None
            try: is_solid = fp.IsSolidFill
            except Exception as e: pass

            gc = None
            try: gc = fp.GridCount
            except Exception as e: pass

            sig = [
                "is_solid={}".format(canon_str(is_solid)),
                "target={}".format(_TARGET_NAME),
                "grid_count={}".format(canon_str(gc)),
            ]

            if gc:
                try:
                    for i in range(int(gc)):
                        sig.extend(grid_sig(fp, i))
                except Exception as e:
                    info["debug_fail_grid_read"] += 1
                    sig.append("error=GridLoopFailed")

        sig_sorted = sorted(sig)
        def_hash = make_hash(sig_sorted)
        if uid:
            uid_to_hash[uid] = def_hash

        # -------------------------
        # v2 (contract semantic): NO names; block on unreadable/missing
        # -------------------------
        v2_ok = True
        v2_reason = None
        sig_v2 = []

        if fp is None:
            v2_ok = False
            v2_reason = "get_fillpattern_failed"
        else:
            # is_solid: require bool-coercible
            try:
                is_solid_v2 = fp.IsSolidFill
            except Exception as e:
                v2_ok = False
                v2_reason = "is_solid_unreadable"

            if v2_ok:
                # grid_count: require int (0 allowed)
                try:
                    gc_v2 = fp.GridCount
                    gc_i = int(gc_v2)
                except Exception as e:
                    v2_ok = False
                    v2_reason = "grid_count_unreadable"

            if v2_ok:
                sig_v2.append("target={}".format(_TARGET_NAME))
                sig_v2.append("is_solid={}".format(canon_str(bool(is_solid_v2))))
                sig_v2.append("grid_count={}".format(canon_str(gc_i)))

                # grids: every grid must be readable
                if gc_i:
                    for i in range(gc_i):
                        ok, parts, reason = _grid_sig_v2(fp, i)
                        if not ok:
                            v2_ok = False
                            v2_reason = reason
                            break
                        sig_v2.extend(parts)

        if v2_ok:
            # keep deterministic: sort like legacy (order-insensitive at record level)
            sig_v2_sorted = sorted(sig_v2)
            def_hash_v2 = make_hash(sig_v2_sorted)
            per_hashes_v2.append(def_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = def_hash_v2
        else:
            _bump_v2_reason(v2_reason or "unknown")

        phase2_payload = _phase2_build_phase2(
            name=name,
            uid=uid,
            elem_id_str=safe_str(e.Id.IntegerValue),
            fp=fp,
            elem=e,
        )

        rec = {
            "id": safe_str(e.Id.IntegerValue),
            "uid": uid,
            "name": name,          # metadata only
            "def_hash": def_hash,  # hashed legacy definition
        }

        if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
            rec["def_signature"] = sig_sorted

        status_v2 = STATUS_OK
        status_reasons_v2 = []
        
        identity_items_v2 = []

        # NOTE: name/uid/elem_id are labels/metadata and MUST NOT participate in identity.
        # Name is carried in label{} and in the phase2 cosmetic surface.

        if fp is None:
            gc_v, gc_q = (None, ITEM_Q_UNREADABLE)
            gc_i = None
        else:
            try:
                gc_i = int(fp.GridCount)
                gc_v, gc_q = canonicalize_int(gc_i)
            except Exception:
                gc_i = None
                gc_v, gc_q = (None, ITEM_Q_UNREADABLE)

        # target is always _TARGET_NAME / ITEM_Q_OK - not part of required_qs check
        identity_items_v2.append(make_identity_item("fill_pattern.target", _TARGET_NAME, ITEM_Q_OK))
        # is_solid is a filter criterion, not an identity field — omitted from identity_items
        identity_items_v2.append(make_identity_item("fill_pattern.grid_count", gc_v, gc_q))
        required_qs = [gc_q]

        if gc_i and gc_i > 0:
            for i in range(gc_i):
                idx = "{:03d}".format(int(i))
                g = _phase2_try_get_grid(fp, i)
                if g is None:
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", None, ITEM_Q_UNREADABLE))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", None, ITEM_Q_UNREADABLE))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", None, ITEM_Q_UNREADABLE))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", None, ITEM_Q_UNREADABLE))
                    required_qs.extend([ITEM_Q_UNREADABLE] * 4)
                    continue

                # angle / offset / shift
                try:
                    ang_v, ang_q = canonicalize_float(getattr(g, "Angle", None))
                except Exception:
                    ang_v, ang_q = (None, ITEM_Q_UNREADABLE)

                try:
                    off_v, off_q = canonicalize_float(getattr(g, "Offset", None))
                except Exception:
                    off_v, off_q = (None, ITEM_Q_UNREADABLE)

                try:
                    sh_v, sh_q = canonicalize_float(getattr(g, "Shift", None))
                except Exception:
                    sh_v, sh_q = (None, ITEM_Q_UNREADABLE)

                # origin: explicit kind + conditional leaf members (uv vs xy)
                origin_kind = None
                a = b = None

                # UV origin
                try:
                    o = getattr(g, "Origin", None)
                    u = getattr(o, "U", None)
                    v = getattr(o, "V", None)
                    if u is not None and v is not None:
                        origin_kind = "uv"
                        a = u
                        b = v
                except Exception:
                    pass

                # XY origin
                if origin_kind is None:
                    try:
                        o = getattr(g, "Origin", None)
                        x = getattr(o, "X", None)
                        y = getattr(o, "Y", None)
                        if x is not None and y is not None:
                            origin_kind = "xy"
                            a = x
                            b = y
                    except Exception:
                        pass

                # Scalar origin props (treated as uv)
                if origin_kind is None:
                    for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                        try:
                            u2 = getattr(g, u_name)
                            v2 = getattr(g, v_name)
                            if u2 is None or v2 is None:
                                continue
                            origin_kind = "uv"
                            a = u2
                            b = v2
                            break
                        except Exception:
                            continue

                if origin_kind is None:
                    ok_kind = (None, ITEM_Q_UNREADABLE)
                else:
                    ok_kind = canonicalize_str(origin_kind)

                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", ang_v, ang_q))
                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", ok_kind[0], ok_kind[1]))

                if origin_kind == "uv":
                    try:
                        ou_v, ou_q = canonicalize_float(a)
                    except Exception:
                        ou_v, ou_q = (None, ITEM_Q_UNREADABLE)
                    try:
                        ov_v, ov_q = canonicalize_float(b)
                    except Exception:
                        ov_v, ov_q = (None, ITEM_Q_UNREADABLE)

                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.u", ou_v, ou_q))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.v", ov_v, ov_q))
                    required_qs.extend([ang_q, ok_kind[1], ou_q, ov_q, off_q, sh_q])

                elif origin_kind == "xy":
                    try:
                        ox_v, ox_q = canonicalize_float(a)
                    except Exception:
                        ox_v, ox_q = (None, ITEM_Q_UNREADABLE)
                    try:
                        oy_v, oy_q = canonicalize_float(b)
                    except Exception:
                        oy_v, oy_q = (None, ITEM_Q_UNREADABLE)

                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.x", ox_v, ox_q))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.y", oy_v, oy_q))
                    required_qs.extend([ang_q, ok_kind[1], ox_q, oy_q, off_q, sh_q])

                else:
                    # kind unreadable => identity blocked; no leaf members
                    required_qs.extend([ang_q, ok_kind[1], off_q, sh_q])

                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", off_v, off_q))
                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", sh_v, sh_q))

        # Derived join helper (policy-required key): capture the entire grid definition bundle.
        # Canonical evidence source is identity_basis.items; selectors reference subsets.
        # Keep preimage order-sensitive so grid index order remains identity-significant.
        try:
            grid_like = []
            for it in (identity_items_v2 or []):
                k = safe_str(it.get("k", ""))
                if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
                    grid_like.append("k={}|q={}|v={}".format(
                        safe_str(it.get("k", "")),
                        safe_str(it.get("q", "")),
                        safe_str(it.get("v", "")),
                    ))
            grids_def_hash_v, grids_def_hash_q = (
                (make_hash(grid_like), ITEM_Q_OK) if grid_like else (None, ITEM_Q_UNREADABLE)
            )
        except Exception:
            grids_def_hash_v, grids_def_hash_q = (None, ITEM_Q_UNREADABLE)

        identity_items_v2.append(
            make_identity_item("fill_pattern.grids_def_hash", grids_def_hash_v, grids_def_hash_q)
        )

        if any(q != ITEM_Q_OK for q in required_qs):
            status_v2 = STATUS_BLOCKED
            status_reasons_v2.append("required_identity_not_ok")

        identity_items_v2_sorted = sorted(identity_items_v2, key=lambda d: str(d.get("k","")))
        sig_preimage_v2 = serialize_identity_items(identity_items_v2_sorted)
        sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(sig_preimage_v2)

        # Selector-only phase2 semantic surface: no duplicated k/q/v evidence.
        semantic_keys = sorted({it.get("k") for it in identity_items_v2_sorted if isinstance(it.get("k"), str)})
        phase2_payload.pop("semantic_items", None)
        phase2_payload["semantic_keys"] = semantic_keys

        # Policy-driven join_key from canonical evidence (identity_basis.items) only.
        # Optional keys stay in identity evidence for future exploration but are not hashed by default.
        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
        join_key, _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items_v2_sorted,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )

        rec_v2 = build_record_v2(
            domain=DOMAIN_NAME,
            record_id=safe_str(name) if safe_str(name) else safe_str(e.Id.IntegerValue),
            status=status_v2,
            status_reasons=sorted(set(status_reasons_v2)),
            sig_hash=sig_hash_v2,
            identity_items=identity_items_v2_sorted,
            required_qs=required_qs,
            label={
                "display": safe_str(name),
                "quality": "human",
                "provenance": "revit.FillPatternElement.Name",
            },
            debug={
                "sig_preimage_sample": sig_preimage_v2[:6],
                "uid_excluded_from_sig": True,
            },
        )
        rec_v2["join_key"] = join_key
        rec_v2["phase2"] = phase2_payload
        rec_v2["sig_basis"] = {
            "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
            "keys_used": semantic_keys,
        }

        # Keep legacy record additive payload aligned with record.v2 selectors.
        rec["join_key"] = join_key
        rec["phase2"] = phase2_payload

        v2_records.append(rec_v2)
        if sig_hash_v2 is not None:
            v2_sig_hashes.append(sig_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = sig_hash_v2

        info["debug_kept"] += 1

        info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = v2_records

    # v2 finalize: block domain hash if any record is blocked
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)
    if any((r or {}).get("status") == STATUS_BLOCKED for r in (v2_records or [])):
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    # Context mapping (UID is allowed only as lookup key; values are semantic hashes)
    # Both drafting and model domains contribute to the same fill_pattern_uid_to_hash map
    if ctx is not None:
        existing = ctx.get("fill_pattern_uid_to_hash") or {}
        existing.update(uid_to_hash_v2)
        ctx["fill_pattern_uid_to_hash"] = existing

    info["record_rows"] = [{
        "record_key": safe_str(r.get("record_id", "")),
        "sig_hash":   safe_str(r.get("sig_hash", "")),
        "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
    } for r in v2_records if isinstance(r, dict)]

    return info

def extract_model(doc, ctx=None):
    _TARGET_INT = _TARGET_MODEL_INT
    _TARGET_NAME = "Model"
    """
    Extract Fill Patterns fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with count, hash, signature_hashes, records,
        record_rows, and debug counters
    """
    info = {
        "count": 0,
        "raw_count": 0,
        "names": [],
                "records": [],

        # debug counters so you can see why things disappear
        "debug_total_elements": 0,
        "debug_kept": 0,
        "debug_skipped_no_name": 0,
        "debug_skipped_wrong_target": 0,
        "debug_fail_getfillpattern": 0,
        "debug_fail_grid_read": 0,

        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},
    }

    try:
        col = _collect_fill_patterns(doc, ctx)
    except Exception as e:
        return info
    info["raw_count"] = len(col)

    def f(v, nd=9):
        if v is None:
            return S_MISSING
        try:
            return format(float(v), ".{}f".format(nd))
        except Exception as e:
            return canon_str(v)

    def grid_sig(fp, i):
        # Return a stable list; never raise
        idx = "{:03d}".format(int(i))
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except Exception as e:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except Exception as e:
                g = None

        if g is None:
            info["debug_fail_grid_read"] += 1
            return ["grid[{}].unreadable={}".format(idx, S_MISSING)]

        parts = []

        def add_float(prop_name, key):
            try:
                v = getattr(g, prop_name)
                parts.append("grid[{}].{}={}".format(idx, key, f(v)))
            except Exception as e:
                parts.append("grid[{}].{}={}".format(idx, key, S_MISSING))

        # origin can vary across versions; try a couple shapes
        def add_origin_2d():
            # Try UV-style origin (U,V)
            try:
                o = g.Origin
                u = getattr(o, "U", None)
                v = getattr(o, "V", None)
                if u is not None and v is not None:
                    parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
                    return
            except Exception as e:
                pass

            # Try XYZ-style origin but store only X,Y
            try:
                o = g.Origin
                x = getattr(o, "X", None)
                y = getattr(o, "Y", None)
                if x is not None and y is not None:
                    parts.append("grid[{}].origin_xy={},{}".format(idx, f(x), f(y)))
                    return
            except Exception as e:
                pass

            # Try separate scalars
            for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                try:
                    u = getattr(g, u_name)
                    v = getattr(g, v_name)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
                    return
                except Exception as e:
                    pass

            parts.append("grid[{}].origin={}".format(idx, S_MISSING))

        add_float("Angle", "angle")
        add_origin_2d()
        add_float("Offset", "offset")
        add_float("Shift", "shift")

        return parts

    # v2 helpers (strict: block on unreadables / missing)
    def _bump_v2_reason(reason):
        info["debug_v2_blocked"] += 1
        try:
            info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
        except Exception as e:
            pass

    def _grid_sig_v2(fp, i):
        """
        Return (ok, parts, reason). parts contain only numeric primitives.
        """
        idx = "{:03d}".format(int(i))
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except Exception as e:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except Exception as e:
                g = None

        if g is None:
            return False, [], "grid_unreadable"

        parts = []

        def req_float(prop_name, key):
            try:
                v = getattr(g, prop_name)
            except Exception as e:
                return False, "grid_{}_unreadable".format(key)
            if v is None:
                return False, "grid_{}_none".format(key)
            try:
                fv = float(v)
            except Exception as e:
                return False, "grid_{}_not_float".format(key)
            parts.append("grid[{}].{}={}".format(idx, key, canon_str(f(v, 9))))
            return True, None

        # origin: require 2 floats, pick first supported shape
        def req_origin():
            # UV origin
            try:
                o = g.Origin
                u = getattr(o, "U", None)
                v = getattr(o, "V", None)
                if u is not None and v is not None:
                    fu = float(u)
                    fv = float(v)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
                    return True, None
            except Exception as e:
                pass

            # XY origin
            try:
                o = g.Origin
                x = getattr(o, "X", None)
                y = getattr(o, "Y", None)
                if x is not None and y is not None:
                    fx = float(x)
                    fy = float(y)
                    parts.append("grid[{}].origin_xy={},{}".format(idx, canon_str(f(fx, 9)), canon_str(f(fy, 9))))
                    return True, None
            except Exception as e:
                pass

            # scalar origin props
            for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                try:
                    u = getattr(g, u_name)
                    v = getattr(g, v_name)
                    if u is None or v is None:
                        continue
                    fu = float(u)
                    fv = float(v)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
                    return True, None
                except Exception as e:
                    continue

            return False, "grid_origin_unreadable"

        ok, reason = req_float("Angle", "angle")
        if not ok:
            return False, [], reason
        ok, reason = req_origin()
        if not ok:
            return False, [], reason
        ok, reason = req_float("Offset", "offset")
        if not ok:
            return False, [], reason
        ok, reason = req_float("Shift", "shift")
        if not ok:
            return False, [], reason

        return True, parts, None

    # -------------------------
    # Phase 2 (additive-only) builders
    # -------------------------

    def _phase2_try_get_grid(fp, i):
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except Exception:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except Exception:
                g = None
        return g

    def _phase2_add_float(items, k, v, *, unreadable=False):
        if unreadable:
            items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
            return
        v2, q2 = canonicalize_float(v)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_add_int(items, k, v, *, unreadable=False):
        if unreadable:
            items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
            return
        v2, q2 = canonicalize_int(v)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_add_bool(items, k, v, *, unreadable=False):
        if unreadable:
            items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
            return
        v2, q2 = canonicalize_bool(v)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_add_str(items, k, v, *, allow_empty=False):
        if allow_empty:
            v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=True)
        else:
            v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=False)
        items.append({"k": k, "v": v2, "q": q2})

    def _phase2_build_phase2(name, uid, elem_id_str, fp, elem):
        semantic = []
        cosmetic = []
        coordination = []
        unknown = []

        # cosmetic
        v_name, q_name = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
        cosmetic.append({"k": "fill_pattern.name", "v": v_name, "q": q_name})

        # unknown identifiers (do not affect semantic hypotheses)
        v_uid, q_uid = canonicalize_str(uid)
        unknown.append({"k": "fill_pattern.uid", "v": v_uid, "q": q_uid})
        v_eid, q_eid = canonicalize_str(elem_id_str)
        unknown.append({"k": "fill_pattern.elem_id", "v": v_eid, "q": q_eid})

        # Traceability fields (metadata only — never in hash/sig/join)
        try:
            _eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
            _eid_v, _eid_q = canonicalize_int(_eid_raw)
        except Exception:
            _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
        try:
            _uid_raw = getattr(elem, "UniqueId", None)
            _uid_v, _uid_q = canonicalize_str(_uid_raw)
        except Exception:
            _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
        unknown.append({"k": "fill_pattern.source_element_id", "v": _eid_v, "q": _eid_q})
        unknown.append({"k": "fill_pattern.source_unique_id", "v": _uid_v, "q": _uid_q})

        # target is always _TARGET_NAME for this domain
        semantic.append({"k": "fill_pattern.target", "v": _TARGET_NAME, "q": ITEM_Q_OK})

        if fp is None:
            # Explicit unreadable (GetFillPattern failed)
            _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
            # is_solid in coordination only (filter criterion, not identity)
            _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
        else:
            # is_solid goes to coordination_items only — it is a filter criterion, not identity
            try:
                is_solid = fp.IsSolidFill
            except Exception:
                _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
            else:
                _phase2_add_bool(coordination, "fill_pattern.is_solid", bool(is_solid))

            # grid_count
            try:
                gc = fp.GridCount
            except Exception:
                _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
                gc_i = None
            else:
                if gc is None:
                    _phase2_add_int(semantic, "fill_pattern.grid_count", None)
                    gc_i = None
                else:
                    try:
                        gc_i = int(gc)
                    except Exception:
                        _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
                        gc_i = None
                    else:
                        _phase2_add_int(semantic, "fill_pattern.grid_count", gc_i)

            # grids (no inference; explicit kind for origin)
            if gc_i:
                for i in range(int(gc_i)):
                    idx = "{:03d}".format(int(i))
                    g = _phase2_try_get_grid(fp, i)
                    if g is None:
                        semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                        continue

                    # Angle / Offset / Shift
                    try:
                        _phase2_add_float(semantic, "fill_pattern.grid[{}].angle".format(idx), float(getattr(g, "Angle")))
                    except Exception:
                        semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})

                    # Origin (explicit kind)
                    origin_kind = None
                    ox = oy = None

                    # UV origin
                    try:
                        o = g.Origin
                        u = getattr(o, "U", None)
                        v = getattr(o, "V", None)
                        if u is not None and v is not None:
                            origin_kind = "uv"
                            ox = float(u)
                            oy = float(v)
                    except Exception:
                        pass

                    # XY origin
                    if origin_kind is None:
                        try:
                            o = g.Origin
                            x = getattr(o, "X", None)
                            y = getattr(o, "Y", None)
                            if x is not None and y is not None:
                                origin_kind = "xy"
                                ox = float(x)
                                oy = float(y)
                        except Exception:
                            pass

                    # Scalar origin props
                    if origin_kind is None:
                        for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                            try:
                                u2 = getattr(g, u_name)
                                v2 = getattr(g, v_name)
                                if u2 is None or v2 is None:
                                    continue
                                origin_kind = "uv"
                                ox = float(u2)
                                oy = float(v2)
                                break
                            except Exception:
                                continue

                    if origin_kind is None:
                        semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
                    else:
                        v_kind, q_kind = canonicalize_str(origin_kind)
                        semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": v_kind, "q": q_kind})

                        if origin_kind == "uv":
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.u".format(idx), ox)
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.v".format(idx), oy)
                        else:
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.x".format(idx), ox)
                            _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.y".format(idx), oy)

                    try:
                        _phase2_add_float(semantic, "fill_pattern.grid[{}].offset".format(idx), float(getattr(g, "Offset")))
                    except Exception:
                        semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})

                    try:
                        _phase2_add_float(semantic, "fill_pattern.grid[{}].shift".format(idx), float(getattr(g, "Shift")))
                    except Exception:
                        semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})

        # Derived structural identity helper for Phase-2:
        # Collapse all per-grid semantic items into a single hash so join-key discovery
        # can treat the grid bundle as one "field" without losing the detailed items.
        #
        # IMPORTANT: grid order is identity-significant; do NOT sort the preimage.
        try:
            grid_like = []
            for it in (semantic or []):
                k = safe_str(it.get("k", ""))
                if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
                    # Stable preimage: include k/q/v so unreadables affect the hash deterministically
                    grid_like.append("k={}|q={}|v={}".format(
                        safe_str(it.get("k", "")),
                        safe_str(it.get("q", "")),
                        safe_str(it.get("v", "")),
                    ))
            grids_def_hash = make_hash(grid_like) if grid_like else None
        except Exception:
            grids_def_hash = None

        if grids_def_hash:
            semantic.append({"k": "fill_pattern.grids_def_hash", "v": grids_def_hash, "q": ITEM_Q_OK})
        else:
            # If we can't compute it, make the failure explicit (but keep it out of identity)
            semantic.append({"k": "fill_pattern.grids_def_hash", "v": None, "q": ITEM_Q_UNREADABLE})

        # Phase-2 bloat control:
        # The full grid definition is already present in identity_basis.items (for sig_hash reproducibility).
        # Avoid duplicating per-grid items in phase2.semantic_items; keep only pointer + small scalars.
        semantic_reduced = []
        for it in (semantic or []):
            k = safe_str(it.get("k", ""))
            if k.startswith("fill_pattern.grid["):
                continue
            semantic_reduced.append(it)

        return {
            "schema": "phase2.{}.v1".format(DOMAIN_NAME),
            "grouping_basis": "phase2.hypothesis",
            "semantic_items": phase2_sorted_items(semantic_reduced),
            "cosmetic_items": phase2_sorted_items(cosmetic),
            "coordination_items": phase2_sorted_items(coordination),
            "unknown_items": phase2_sorted_items(unknown),
        }

    records = []
    per_hashes = []
    per_hashes_v2 = []
    v2_records = []
    v2_sig_hashes = []
    names = []
    uid_to_hash_v2 = {}
    uid_to_hash = {}

    for e in col:
        info["debug_total_elements"] += 1

        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_skipped_no_name"] += 1
            continue

        uid = getattr(e, "UniqueId", "") or ""

        # Always keep the element, even if we can't read its FillPattern
        fp = None
        try:
            fp = e.GetFillPattern()
        except Exception as e:
            fp = None

        # Filter: only process patterns matching this domain's target
        if fp is not None:
            try:
                _fp_target_int = int(fp.Target)
            except Exception:
                _fp_target_int = -1
            if _fp_target_int != _TARGET_INT:
                info["debug_skipped_wrong_target"] += 1
                continue

        # Filter: skip solid fills — system defaults, ungoverned
        if fp is not None:
            try:
                if fp.IsSolidFill:
                    continue
            except Exception:
                pass  # if unreadable, proceed and let field-level q handle it

        names.append(name)

        # -------------------------
        # Legacy signature (UNCHANGED meaning)
        # -------------------------
        if fp is None:
            info["debug_fail_getfillpattern"] += 1
            sig = [
                f"is_solid={S_MISSING}",
                f"target={_TARGET_NAME}",
                f"grid_count={S_MISSING}",
                f"grid[000].unreadable={S_MISSING}",
                "error=GetFillPatternFailed",
            ]
        else:
            is_solid = None
            try: is_solid = fp.IsSolidFill
            except Exception as e: pass

            gc = None
            try: gc = fp.GridCount
            except Exception as e: pass

            sig = [
                "is_solid={}".format(canon_str(is_solid)),
                "target={}".format(_TARGET_NAME),
                "grid_count={}".format(canon_str(gc)),
            ]

            if gc:
                try:
                    for i in range(int(gc)):
                        sig.extend(grid_sig(fp, i))
                except Exception as e:
                    info["debug_fail_grid_read"] += 1
                    sig.append("error=GridLoopFailed")

        sig_sorted = sorted(sig)
        def_hash = make_hash(sig_sorted)
        if uid:
            uid_to_hash[uid] = def_hash

        # -------------------------
        # v2 (contract semantic): NO names; block on unreadable/missing
        # -------------------------
        v2_ok = True
        v2_reason = None
        sig_v2 = []

        if fp is None:
            v2_ok = False
            v2_reason = "get_fillpattern_failed"
        else:
            # is_solid: require bool-coercible
            try:
                is_solid_v2 = fp.IsSolidFill
            except Exception as e:
                v2_ok = False
                v2_reason = "is_solid_unreadable"

            if v2_ok:
                # grid_count: require int (0 allowed)
                try:
                    gc_v2 = fp.GridCount
                    gc_i = int(gc_v2)
                except Exception as e:
                    v2_ok = False
                    v2_reason = "grid_count_unreadable"

            if v2_ok:
                sig_v2.append("target={}".format(_TARGET_NAME))
                sig_v2.append("is_solid={}".format(canon_str(bool(is_solid_v2))))
                sig_v2.append("grid_count={}".format(canon_str(gc_i)))

                # grids: every grid must be readable
                if gc_i:
                    for i in range(gc_i):
                        ok, parts, reason = _grid_sig_v2(fp, i)
                        if not ok:
                            v2_ok = False
                            v2_reason = reason
                            break
                        sig_v2.extend(parts)

        if v2_ok:
            # keep deterministic: sort like legacy (order-insensitive at record level)
            sig_v2_sorted = sorted(sig_v2)
            def_hash_v2 = make_hash(sig_v2_sorted)
            per_hashes_v2.append(def_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = def_hash_v2
        else:
            _bump_v2_reason(v2_reason or "unknown")

        phase2_payload = _phase2_build_phase2(
            name=name,
            uid=uid,
            elem_id_str=safe_str(e.Id.IntegerValue),
            fp=fp,
            elem=e,
        )

        rec = {
            "id": safe_str(e.Id.IntegerValue),
            "uid": uid,
            "name": name,          # metadata only
            "def_hash": def_hash,  # hashed legacy definition
        }

        if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
            rec["def_signature"] = sig_sorted

        status_v2 = STATUS_OK
        status_reasons_v2 = []
        
        identity_items_v2 = []

        # NOTE: name/uid/elem_id are labels/metadata and MUST NOT participate in identity.
        # Name is carried in label{} and in the phase2 cosmetic surface.

        if fp is None:
            gc_v, gc_q = (None, ITEM_Q_UNREADABLE)
            gc_i = None
        else:
            try:
                gc_i = int(fp.GridCount)
                gc_v, gc_q = canonicalize_int(gc_i)
            except Exception:
                gc_i = None
                gc_v, gc_q = (None, ITEM_Q_UNREADABLE)

        # target is always _TARGET_NAME / ITEM_Q_OK - not part of required_qs check
        identity_items_v2.append(make_identity_item("fill_pattern.target", _TARGET_NAME, ITEM_Q_OK))
        # is_solid is a filter criterion, not an identity field — omitted from identity_items
        identity_items_v2.append(make_identity_item("fill_pattern.grid_count", gc_v, gc_q))
        required_qs = [gc_q]

        if gc_i and gc_i > 0:
            for i in range(gc_i):
                idx = "{:03d}".format(int(i))
                g = _phase2_try_get_grid(fp, i)
                if g is None:
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", None, ITEM_Q_UNREADABLE))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", None, ITEM_Q_UNREADABLE))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", None, ITEM_Q_UNREADABLE))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", None, ITEM_Q_UNREADABLE))
                    required_qs.extend([ITEM_Q_UNREADABLE] * 4)
                    continue

                # angle / offset / shift
                try:
                    ang_v, ang_q = canonicalize_float(getattr(g, "Angle", None))
                except Exception:
                    ang_v, ang_q = (None, ITEM_Q_UNREADABLE)

                try:
                    off_v, off_q = canonicalize_float(getattr(g, "Offset", None))
                except Exception:
                    off_v, off_q = (None, ITEM_Q_UNREADABLE)

                try:
                    sh_v, sh_q = canonicalize_float(getattr(g, "Shift", None))
                except Exception:
                    sh_v, sh_q = (None, ITEM_Q_UNREADABLE)

                # origin: explicit kind + conditional leaf members (uv vs xy)
                origin_kind = None
                a = b = None

                # UV origin
                try:
                    o = getattr(g, "Origin", None)
                    u = getattr(o, "U", None)
                    v = getattr(o, "V", None)
                    if u is not None and v is not None:
                        origin_kind = "uv"
                        a = u
                        b = v
                except Exception:
                    pass

                # XY origin
                if origin_kind is None:
                    try:
                        o = getattr(g, "Origin", None)
                        x = getattr(o, "X", None)
                        y = getattr(o, "Y", None)
                        if x is not None and y is not None:
                            origin_kind = "xy"
                            a = x
                            b = y
                    except Exception:
                        pass

                # Scalar origin props (treated as uv)
                if origin_kind is None:
                    for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                        try:
                            u2 = getattr(g, u_name)
                            v2 = getattr(g, v_name)
                            if u2 is None or v2 is None:
                                continue
                            origin_kind = "uv"
                            a = u2
                            b = v2
                            break
                        except Exception:
                            continue

                if origin_kind is None:
                    ok_kind = (None, ITEM_Q_UNREADABLE)
                else:
                    ok_kind = canonicalize_str(origin_kind)

                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", ang_v, ang_q))
                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", ok_kind[0], ok_kind[1]))

                if origin_kind == "uv":
                    try:
                        ou_v, ou_q = canonicalize_float(a)
                    except Exception:
                        ou_v, ou_q = (None, ITEM_Q_UNREADABLE)
                    try:
                        ov_v, ov_q = canonicalize_float(b)
                    except Exception:
                        ov_v, ov_q = (None, ITEM_Q_UNREADABLE)

                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.u", ou_v, ou_q))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.v", ov_v, ov_q))
                    required_qs.extend([ang_q, ok_kind[1], ou_q, ov_q, off_q, sh_q])

                elif origin_kind == "xy":
                    try:
                        ox_v, ox_q = canonicalize_float(a)
                    except Exception:
                        ox_v, ox_q = (None, ITEM_Q_UNREADABLE)
                    try:
                        oy_v, oy_q = canonicalize_float(b)
                    except Exception:
                        oy_v, oy_q = (None, ITEM_Q_UNREADABLE)

                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.x", ox_v, ox_q))
                    identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.y", oy_v, oy_q))
                    required_qs.extend([ang_q, ok_kind[1], ox_q, oy_q, off_q, sh_q])

                else:
                    # kind unreadable => identity blocked; no leaf members
                    required_qs.extend([ang_q, ok_kind[1], off_q, sh_q])

                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", off_v, off_q))
                identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", sh_v, sh_q))

        # Derived join helper (policy-required key): capture the entire grid definition bundle.
        # Canonical evidence source is identity_basis.items; selectors reference subsets.
        # Keep preimage order-sensitive so grid index order remains identity-significant.
        try:
            grid_like = []
            for it in (identity_items_v2 or []):
                k = safe_str(it.get("k", ""))
                if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
                    grid_like.append("k={}|q={}|v={}".format(
                        safe_str(it.get("k", "")),
                        safe_str(it.get("q", "")),
                        safe_str(it.get("v", "")),
                    ))
            grids_def_hash_v, grids_def_hash_q = (
                (make_hash(grid_like), ITEM_Q_OK) if grid_like else (None, ITEM_Q_UNREADABLE)
            )
        except Exception:
            grids_def_hash_v, grids_def_hash_q = (None, ITEM_Q_UNREADABLE)

        identity_items_v2.append(
            make_identity_item("fill_pattern.grids_def_hash", grids_def_hash_v, grids_def_hash_q)
        )

        if any(q != ITEM_Q_OK for q in required_qs):
            status_v2 = STATUS_BLOCKED
            status_reasons_v2.append("required_identity_not_ok")

        identity_items_v2_sorted = sorted(identity_items_v2, key=lambda d: str(d.get("k","")))
        sig_preimage_v2 = serialize_identity_items(identity_items_v2_sorted)
        sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(sig_preimage_v2)

        # Selector-only phase2 semantic surface: no duplicated k/q/v evidence.
        semantic_keys = sorted({it.get("k") for it in identity_items_v2_sorted if isinstance(it.get("k"), str)})
        phase2_payload.pop("semantic_items", None)
        phase2_payload["semantic_keys"] = semantic_keys

        # Policy-driven join_key from canonical evidence (identity_basis.items) only.
        # Optional keys stay in identity evidence for future exploration but are not hashed by default.
        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
        join_key, _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items_v2_sorted,
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
            emit_items=False,
            emit_selectors=True,
        )

        rec_v2 = build_record_v2(
            domain=DOMAIN_NAME,
            record_id=safe_str(name) if safe_str(name) else safe_str(e.Id.IntegerValue),
            status=status_v2,
            status_reasons=sorted(set(status_reasons_v2)),
            sig_hash=sig_hash_v2,
            identity_items=identity_items_v2_sorted,
            required_qs=required_qs,
            label={
                "display": safe_str(name),
                "quality": "human",
                "provenance": "revit.FillPatternElement.Name",
            },
            debug={
                "sig_preimage_sample": sig_preimage_v2[:6],
                "uid_excluded_from_sig": True,
            },
        )
        rec_v2["join_key"] = join_key
        rec_v2["phase2"] = phase2_payload
        rec_v2["sig_basis"] = {
            "schema": "{}.sig_basis.v1".format(DOMAIN_NAME),
            "keys_used": semantic_keys,
        }

        # Keep legacy record additive payload aligned with record.v2 selectors.
        rec["join_key"] = join_key
        rec["phase2"] = phase2_payload

        v2_records.append(rec_v2)
        if sig_hash_v2 is not None:
            v2_sig_hashes.append(sig_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = sig_hash_v2

        info["debug_kept"] += 1

        info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = v2_records

    # v2 finalize: block domain hash if any record is blocked
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)
    if any((r or {}).get("status") == STATUS_BLOCKED for r in (v2_records or [])):
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    # Context mapping (UID is allowed only as lookup key; values are semantic hashes)
    # Both drafting and model domains contribute to the same fill_pattern_uid_to_hash map
    if ctx is not None:
        existing = ctx.get("fill_pattern_uid_to_hash") or {}
        existing.update(uid_to_hash_v2)
        ctx["fill_pattern_uid_to_hash"] = existing

    info["record_rows"] = [{
        "record_key": safe_str(r.get("record_id", "")),
        "sig_hash":   safe_str(r.get("sig_hash", "")),
        "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
    } for r in v2_records if isinstance(r, dict)]

    return info
