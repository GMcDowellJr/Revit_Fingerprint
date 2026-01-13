# -*- coding: utf-8 -*-
"""
Fill Patterns domain extractor.

Fingerprints fill pattern definitions including:
- Solid vs. patterned
- Model vs. drafting
- Grid definitions (angle, origin, offset, shift)

Per-record identity: UniqueId
Ordering: grid order is preserved (order-sensitive for grids)
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

try:
    from Autodesk.Revit.DB import FillPatternElement
except ImportError:
    FillPatternElement = None

# Global debug flag
DEBUG_INCLUDE_FILLPATTERN_SIGNATURES = False


def extract(doc, ctx=None):
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
        "signature_hashes": [],
        "hash": None,
        "records": [],

        # debug counters so you can see why things disappear
        "debug_total_elements": 0,
        "debug_kept": 0,
        "debug_skipped_no_name": 0,
        "debug_fail_getfillpattern": 0,
        "debug_fail_grid_read": 0,

        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},
    }

    try:
        col = list(
            collect_instances(
                doc,
                of_class=FillPatternElement,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="fill_patterns:FillPatternElement:instances",
            )
        )
    except Exception as e:
        return info
    info["raw_count"] = len(col)

    def f(v, nd=9):
        if v is None:
            return S_MISSING
        try:
            return format(float(v), ".{}f".format(nd))
        except Exception as e:
            return sig_val(v)

    def read_is_model(fp, target):
        # Prefer explicit property, else infer from target when possible
        is_model = None
        for attr in ["IsModelFillPattern", "IsModel", "IsModelFill"]:
            try:
                if hasattr(fp, attr):
                    is_model = getattr(fp, attr)
                    break
            except Exception as e:
                pass
        if is_model is None:
            try:
                if target is not None:
                    is_model = (int(target) == 1)  # Drafting=0, Model=1 in many builds
            except Exception as e:
                pass
        return is_model

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
            parts.append("grid[{}].{}={}".format(idx, key, sig_val(f(v, 9))))
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
                    parts.append("grid[{}].origin_uv={},{}".format(idx, sig_val(f(fu, 9)), sig_val(f(fv, 9))))
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
                    parts.append("grid[{}].origin_xy={},{}".format(idx, sig_val(f(fx, 9)), sig_val(f(fy, 9))))
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
                    parts.append("grid[{}].origin_uv={},{}".format(idx, sig_val(f(fu, 9)), sig_val(f(fv, 9))))
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

    records = []
    per_hashes = []
    per_hashes_v2 = []
    names = []
    uid_to_hash_v2 = {}
    uid_to_hash = {}

    for e in col:
        info["debug_total_elements"] += 1

        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_skipped_no_name"] += 1
            continue
        names.append(name)

        uid = getattr(e, "UniqueId", "") or ""

        # Always keep the element, even if we can't read its FillPattern
        fp = None
        try:
            fp = e.GetFillPattern()
        except Exception as e:
            fp = None

        # -------------------------
        # Legacy signature (UNCHANGED meaning)
        # -------------------------
        if fp is None:
            info["debug_fail_getfillpattern"] += 1
            sig = [
                f"is_solid={S_MISSING}",
                f"is_model={S_MISSING}",
                f"target={S_MISSING}",
                f"grid_count={S_MISSING}",
                f"grid[000].unreadable={S_MISSING}",
                "error=GetFillPatternFailed",
            ]
        else:
            is_solid = None
            try: is_solid = fp.IsSolidFill
            except Exception as e: pass

            target = None
            try: target = fp.Target
            except Exception as e: pass

            is_model = read_is_model(fp, target)

            gc = None
            try: gc = fp.GridCount
            except Exception as e: pass

            sig = [
                "is_solid={}".format(sig_val(is_solid)),
                "is_model={}".format(sig_val(is_model)),
                "target={}".format(sig_val(target)),
                "grid_count={}".format(sig_val(gc)),
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
                # target: require int
                try:
                    target_v2 = fp.Target
                    target_id = int(target_v2)
                except Exception as e:
                    v2_ok = False
                    v2_reason = "target_unreadable"

            if v2_ok:
                # is_model: require bool (direct or inferred); but must resolve, else block
                try:
                    is_model_v2 = read_is_model(fp, target_v2)
                    if is_model_v2 is None:
                        v2_ok = False
                        v2_reason = "is_model_unresolved"
                except Exception as e:
                    v2_ok = False
                    v2_reason = "is_model_unreadable"

            if v2_ok:
                # grid_count: require int (0 allowed)
                try:
                    gc_v2 = fp.GridCount
                    gc_i = int(gc_v2)
                except Exception as e:
                    v2_ok = False
                    v2_reason = "grid_count_unreadable"

            if v2_ok:
                sig_v2.append("is_solid={}".format(sig_val(bool(is_solid_v2))))
                sig_v2.append("is_model={}".format(sig_val(bool(is_model_v2))))
                sig_v2.append("target_id={}".format(sig_val(target_id)))
                sig_v2.append("grid_count={}".format(sig_val(gc_i)))

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

        rec = {
            "id": safe_str(e.Id.IntegerValue),
            "uid": uid,
            "name": name,          # metadata only
            "def_hash": def_hash,  # hashed legacy definition
        }
        if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
            rec["def_signature"] = sig_sorted

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

    info["signature_hashes"] = sorted(per_hashes)
    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["hash"] = make_hash(info["signature_hashes"])
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))

    # v2 finalize: block domain hash if any record blocked
    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if info["debug_v2_blocked"] > 0:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    # Context mapping (UID is allowed only as lookup key; values are semantic hashes)
    if ctx is not None:
        ctx["fill_pattern_uid_to_hash"] = uid_to_hash
        ctx["fill_pattern_uid_to_hash_v2"] = uid_to_hash_v2

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),        # <-- UniqueId
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),       # optional metadata
        } for r in recs]
    except Exception as e:
        info["record_rows"] = []

    return info
