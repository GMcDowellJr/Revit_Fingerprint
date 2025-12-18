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

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
core_dir = os.path.join(parent_dir, 'core')
if core_dir not in sys.path:
    sys.path.insert(0, core_dir)

from hashing import make_hash, safe_str
from canon import canon_str, sig_val

try:
    from Autodesk.Revit.DB import FilteredElementCollector, FillPatternElement
except ImportError:
    FilteredElementCollector = None
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
    }

    try:
        col = list(FilteredElementCollector(doc).OfClass(FillPatternElement))
    except:
        return info
    info["raw_count"] = len(col)

    def f(v, nd=9):
        if v is None:
            return "<None>"
        try:
            return format(float(v), ".{}f".format(nd))
        except:
            return sig_val(v)

    def read_is_model(fp, target):
        # Prefer explicit property, else infer from target when possible
        is_model = None
        for attr in ["IsModelFillPattern", "IsModel", "IsModelFill"]:
            try:
                if hasattr(fp, attr):
                    is_model = getattr(fp, attr)
                    break
            except:
                pass
        if is_model is None:
            try:
                if target is not None:
                    is_model = (int(target) == 1)  # Drafting=0, Model=1 in many builds
            except:
                pass
        return is_model

    def grid_sig(fp, i):
        # Return a stable list; never raise
        idx = "{:03d}".format(int(i))
        g = None
        try:
            if hasattr(fp, "GetFillPatternGrid"):
                g = fp.GetFillPatternGrid(i)
        except:
            g = None
        if g is None:
            try:
                if hasattr(fp, "GetFillGrid"):
                    g = fp.GetFillGrid(i)
            except:
                g = None

        if g is None:
            info["debug_fail_grid_read"] += 1
            return ["grid[{}].unreadable=<None>".format(idx)]

        parts = []

        def add_float(prop_name, key):
            try:
                v = getattr(g, prop_name)
                parts.append("grid[{}].{}={}".format(idx, key, f(v)))
            except:
                parts.append("grid[{}].{}=<None>".format(idx, key))

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
            except:
                pass

            # Try XYZ-style origin but store only X,Y
            try:
                o = g.Origin
                x = getattr(o, "X", None)
                y = getattr(o, "Y", None)
                if x is not None and y is not None:
                    parts.append("grid[{}].origin_xy={},{}".format(idx, f(x), f(y)))
                    return
            except:
                pass

            # Try separate scalars
            for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
                try:
                    u = getattr(g, u_name)
                    v = getattr(g, v_name)
                    parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
                    return
                except:
                    pass

            parts.append("grid[{}].origin=<None>".format(idx))

        add_float("Angle", "angle")
        add_origin_2d()
        add_float("Offset", "offset")
        add_float("Shift", "shift")

        return parts

    records = []
    per_hashes = []
    names = []

    for e in col:
        info["debug_total_elements"] += 1

        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_skipped_no_name"] += 1
            continue
        names.append(name)

        # Always keep the element, even if we can't read its FillPattern
        fp = None
        try:
            fp = e.GetFillPattern()
        except:
            fp = None

        if fp is None:
            info["debug_fail_getfillpattern"] += 1
            sig = [
                "is_solid=<None>",
                "is_model=<None>",
                "target=<None>",
                "grid_count=<None>",
                "grid[000].unreadable=<None>",
                "error=GetFillPatternFailed",
            ]
        else:
            # Core fields
            is_solid = None
            try: is_solid = fp.IsSolidFill
            except: pass

            target = None
            try: target = fp.Target
            except: pass

            is_model = read_is_model(fp, target)

            gc = None
            try: gc = fp.GridCount
            except: pass

            sig = [
                "is_solid={}".format(sig_val(is_solid)),
                "is_model={}".format(sig_val(is_model)),
                "target={}".format(sig_val(target)),
                "grid_count={}".format(sig_val(gc)),
            ]

            # Grids (fail-soft: if grid read fails, you still keep pattern)
            if gc:
                try:
                    for i in range(int(gc)):
                        sig.extend(grid_sig(fp, i))
                except:
                    info["debug_fail_grid_read"] += 1
                    sig.append("error=GridLoopFailed")

        # Keep signature deterministic
        sig_sorted = sorted(sig)
        def_hash = make_hash(sig_sorted)

        rec = {
            "id": safe_str(e.Id.IntegerValue),
            "uid": getattr(e, "UniqueId", "") or "",
            "name": name,          # metadata only
            "def_hash": def_hash,  # hashed definition
        }
        if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
            rec["def_signature"] = sig_sorted

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

    per_hashes = sorted(per_hashes)
    info["signature_hashes"] = sorted(per_hashes)
    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),        # <-- UniqueId
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),       # optional metadata
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
