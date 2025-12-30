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
    import traceback

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

        # v2 (contract semantic) surfaces - additive only
        "hash_v2": None,
        "signature_hashes_v2": [],
        "debug_v2_blocked": 0,
        "debug_v2_block_reasons": {},
    }

    try:
        col = list(FilteredElementCollector(doc).OfClass(LinePatternElement))
    except:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    per_hashes_v2 = []
    uid_to_hash = {}
    uid_to_hash_v2 = {}

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
            # Prefer instance method (most reliable under CPython/pythonnet)
            lp = e.GetLinePattern()
        except Exception:
            lp = None

        if lp is None:
            try:
                # Fallback: some API surfaces expose a static helper
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

        # -------------------------
        # Legacy signature (UNCHANGED behavior)
        # -------------------------
        sig = []

        if lp is None:
            # Fail-soft: keep element, but signature will collapse unless we add distinguishing info.
            # We add uid as metadata marker ONLY for the failure case to avoid "all same hash".
            sig.append("error=GetLinePatternFailed")
        else:
            segs = None
            try:
                # Revit API commonly exposes segments via GetSegments()
                if hasattr(lp, "GetSegments"):
                    segs = list(lp.GetSegments() or [])
                else:
                    segs = list(getattr(lp, "Segments", None) or [])
            except Exception as ex:
                info["debug_fail_segment_read"] += 1

                t = ex.__class__.__name__
                info["debug_segment_ex_types"][t] = info["debug_segment_ex_types"].get(t, 0) + 1

                if len(info["debug_segment_ex_samples"]) < 5:
                    info["debug_segment_ex_samples"].append({
                        "name": name,
                        "id": safe_str(e.Id.IntegerValue),
                        "uid": uid,
                        "ex_type": t,
                        "ex_msg": safe_str(str(ex)),
                    })
                segs = None

            if segs is None:
                sig.append("error=SegmentsUnreadable")
            else:
                sig.append("seg_count={}".format(sig_val(len(segs))))
                for idx, s in enumerate(segs):
                    try:
                        # segment type
                        stype_out = "<None>"
                        try:
                            st = getattr(s, "SegmentType", None)
                            if st is None:
                                stype_out = "<None>"
                            else:
                                try:
                                    stype_out = safe_str(int(st))
                                except:
                                    stype_out = safe_str(int(st))
                        except:
                            stype_out = "<None>"

                        # length
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

        # -------------------------
        # v2 (contract semantic) signature (NO names, NO uid; BLOCK on unreadables/sentinels)
        # -------------------------
        sig_v2 = []
        v2_ok = True
        v2_reason = None

        if lp is None:
            v2_ok = False
            v2_reason = "get_line_pattern_failed"
        else:
            try:
                segs_v2 = list(getattr(lp, "Segments", None) or [])
            except Exception:
                v2_ok = False
                v2_reason = "segments_unreadable"
                segs_v2 = None

            if v2_ok:
                sig_v2.append("seg_count={}".format(sig_val(len(segs_v2))))
                for idx, s in enumerate(segs_v2):
                    # SegmentType must be readable as an int enum id
                    try:
                        st = getattr(s, "SegmentType", None)
                    except Exception:
                        v2_ok = False
                        v2_reason = "segment_type_unreadable"
                        break

                    if st is None:
                        v2_ok = False
                        v2_reason = "segment_type_none"
                        break

                    try:
                        st_id = int(st)
                    except Exception:
                        v2_ok = False
                        v2_reason = "segment_type_not_int"
                        break

                    # Length must be readable as float
                    try:
                        slen = getattr(s, "Length", None)
                    except Exception:
                        v2_ok = False
                        v2_reason = "segment_length_unreadable"
                        break

                    if slen is None:
                        v2_ok = False
                        v2_reason = "segment_length_none"
                        break

                    try:
                        slen_f = float(slen)
                    except Exception:
                        v2_ok = False
                        v2_reason = "segment_length_not_float"
                        break

                    sig_v2.append("seg[{}].type_id={}".format(idx, sig_val(st_id)))
                    sig_v2.append("seg[{}].len={}".format(idx, sig_val(fnum(slen_f, 9))))

        def_hash_v2 = None
        if v2_ok:
            def_hash_v2 = make_hash(sig_v2)
            per_hashes_v2.append(def_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = def_hash_v2
        else:
            info["debug_v2_blocked"] += 1
            try:
                info["debug_v2_block_reasons"][v2_reason] = info["debug_v2_block_reasons"].get(v2_reason, 0) + 1
            except:
                pass

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
    info["count"] = len(records)
    info["records"] = sorted(records, key=lambda r: (r.get("name", ""), r.get("id", "")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    # v2 finalize: block domain hash if any record blocked
    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if info["debug_v2_blocked"] > 0:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"]) if info["signature_hashes_v2"] else None

    # Populate context for downstream domains (UID allowed only as lookup key)
    if ctx is not None:
        ctx["line_pattern_uid_to_hash"] = uid_to_hash
        ctx["line_pattern_uid_to_hash_v2"] = uid_to_hash_v2

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
