# -*- coding: utf-8 -*-
"""
Line Patterns domain extractor.

Fingerprints line pattern definitions including:
- Segment count
- Per-segment type and length (order-sensitive)

Per-record identity: UniqueId
Ordering: segment order is preserved (order-sensitive for segments)
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
    from Autodesk.Revit.DB import FilteredElementCollector, LinePatternElement
except ImportError:
    FilteredElementCollector = None
    LinePatternElement = None

# Global debug flag (will be configurable via runner later)
DEBUG_INCLUDE_LINEPATTERN_SIGNATURES = True


def extract(doc, ctx=None):
    """
    Extract Line Patterns fingerprint from document.

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
        "signature_hashes": [],
        "hash": None,

        # debug counters
        "debug_missing_name": 0,
        "debug_fail_getpattern": 0,
        "debug_fail_segment_read": 0,
        "debug_kept": 0,

        "debug_getpattern_ex_types": {},
        "debug_getpattern_ex_samples": [],
        "debug_segment_ex_types": {},
        "debug_segment_ex_samples": [],
    }

    try:
        col = list(FilteredElementCollector(doc).OfClass(LinePatternElement))
    except:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}

    def fnum(v, nd=9):
        if v is None:
            return "<None>"
        try:
            return format(float(v), ".{}f".format(nd))
        except:
            return sig_val(v)

    for e in col:
        # name is metadata only
        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = "<unnamed>"
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(e, "UniqueId", None))
        except:
            uid = None

        lp = None
        try:
            # Use static overload to avoid pythonnet/IronPython method-binding issues
            lp = LinePatternElement.GetLinePattern(doc, e.Id)
        except Exception as ex:
            info["debug_fail_getpattern"] += 1

            t = ex.__class__.__name__
            info["debug_getpattern_ex_types"][t] = info["debug_getpattern_ex_types"].get(t, 0) + 1

            if len(info["debug_getpattern_ex_samples"]) < 5:
                info["debug_getpattern_ex_samples"].append({
                    "name": name,
                    "id": safe_str(e.Id.IntegerValue),
                    "uid": uid,
                    "ex_type": t,
                    "ex_msg": safe_str(str(ex)),
                })
            lp = None

        sig = []

        if lp is None:
            # Fail-soft: keep element, but signature will collapse unless we add distinguishing info.
            # We add uid as metadata marker ONLY for the failure case to avoid "all same hash".
            sig.append("error=GetLinePatternFailed")
        else:
            segs = None
            try:
                # Prefer method (often binds better in pythonnet) if present
                get_segs = getattr(lp, "GetSegments", None)
                if get_segs:
                    segs = list(get_segs())
                else:
                    segs = list(getattr(lp, "Segments"))
            except Exception as ex:
                segs = None
                info["debug_fail_segment_read"] += 1

                # Optional: capture why segments are unreadable (bounded)
                t = ex.__class__.__name__
                info.setdefault("debug_segment_ex_types", {})
                info["debug_segment_ex_types"][t] = info["debug_segment_ex_types"].get(t, 0) + 1
                if len(info.setdefault("debug_segment_ex_samples", [])) < 5:
                    info["debug_segment_ex_samples"].append({
                        "name": name,
                        "id": safe_str(e.Id.IntegerValue),
                        "uid": uid,
                        "ex_type": t,
                        "ex_msg": safe_str(str(ex)),
                    })

            if segs is None:
                sig.append("error=SegmentsUnreadable")
            else:
                # IMPORTANT: do NOT sort; segment order is part of the definition.
                sig.append("segment_count={}".format(sig_val(len(segs))))
                for i, s in enumerate(segs):
                    idx = "{:03d}".format(int(i))
                    try:
                        # Segment type (Revit API: LinePatternSegment.Type)
                        stype_out = "<None>"
                        try:
                            st = s.Type
                            try:
                                stype_out = canon_str(st.ToString()) or "<None>"
                            except:
                                stype_out = safe_str(int(st))
                        except:
                            stype_out = "<None>"

                        try:
                            slen = getattr(s, "Length", None)
                        except:
                            slen = None
                        sig.append("seg[{}].type={}".format(idx, sig_val(stype_out)))
                        sig.append("seg[{}].len={}".format(idx, sig_val(fnum(slen, 9))))
                    except:
                        info["debug_fail_segment_read"] += 1
                        sig.append("seg[{}].error=SegmentReadFailed".format(idx))

        # Deterministic: keep order (don't sort), hash the definition signature
        def_hash = make_hash(sig)
        if uid:
            uid_to_hash[uid] = def_hash


        rec = {
            "id": safe_str(e.Id.IntegerValue),
            "name": name,          # metadata only
            "uid": uid,            # metadata only
            "def_hash": def_hash,  # hashed definition (or failure-signature)
        }
        if DEBUG_INCLUDE_LINEPATTERN_SIGNATURES:
            rec["def_signature"] = sig

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

    info["names"] = sorted(set(names))
    info["count"] = len(info["names"])
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    # Populate context for downstream domains (UID allowed only as lookup key)
    if ctx is not None:
        ctx["line_pattern_uid_to_hash"] = uid_to_hash

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
