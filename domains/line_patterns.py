# -*- coding: utf-8 -*-
"""
Line Patterns domain extractor.

Fingerprints line pattern definitions including:
- Segment count
- Per-segment type and length (order-sensitive)

Per-record identity: UniqueId
Ordering: segment order is preserved (order-sensitive for segments)
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
    canon_num,
    canon_bool,
    canon_id,
    sig_val,
    fnum,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    canonicalize_str,
    canonicalize_int,
    canonicalize_float,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)


try:
    from Autodesk.Revit.DB import LinePatternElement
except ImportError:
    LinePatternElement = None

# Global debug flag (will be configurable via runner later)
DEBUG_INCLUDE_LINEPATTERN_SIGNATURES = True

# Canonical, locked mapping observed in Dynamo output:
# 0 = Dash, 1 = Space, 2 = Dot
_LP_SEG_TYPE_NAME = {0: "Dash", 1: "Space", 2: "Dot"}

def _lp_seg_type_id_and_name(seg):
    """
    Robustly read a line pattern segment type across API surfaces.

    Preferred property in your Dynamo environment: LinePatternSegment.Type
    Fallback (some older/other surfaces): SegmentType

    Returns: (type_id:int|None, type_name:str|None)
    """
    st = None
    # Preferred: .Type
    try:
        if hasattr(seg, "Type"):
            st = getattr(seg, "Type", None)
    except Exception:
        st = None

    # Fallback: .SegmentType
    if st is None:
        try:
            if hasattr(seg, "SegmentType"):
                st = getattr(seg, "SegmentType", None)
        except Exception:
            st = None

    if st is None:
        return None, None

    try:
        st_id = int(st)
    except Exception:
        return None, None

    return st_id, _LP_SEG_TYPE_NAME.get(st_id, "Unknown")

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
        col = list(
            collect_instances(
                doc,
                of_class=LinePatternElement,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="line_patterns:LinePatternElement:instances",
            )
        )
    except Exception as e:
        return info

    info["raw_count"] = len(col)

    names = []
    legacy_records = []
    v2_records = []
    per_hashes = []
    v2_sig_hashes = []
    uid_to_hash = {}
    uid_to_hash_v2 = {}

    def fnum(v, nd=9):
        if v is None:
            return S_MISSING
        try:
            return format(float(v), ".{}f".format(nd))
        except Exception as e:
            return sig_val(v)

    for e in col:
        # name is metadata only
        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = S_MISSING
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(e, "UniqueId", None))
        except Exception as e:
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
                        st_id, st_name = _lp_seg_type_id_and_name(s)

                        try:
                            slen = getattr(s, "Length", None)
                        except Exception:
                            slen = None

                        # Dot normalization (observed: dots are always 0.0 length)
                        if st_id == 2:
                            slen = 0.0

                        sig.append("seg[{}].type_id={}".format(idx, sig_val(st_id)))
                        sig.append("seg[{}].type={}".format(idx, sig_val(st_name)))
                        sig.append("seg[{}].len={}".format(idx, sig_val(fnum(slen, 9))))
                    except Exception as e:
                        info["debug_fail_segment_read"] += 1
                        sig.append("seg[{}].error=SegmentReadFailed".format(idx))

        # Deterministic: keep order (don't sort), hash the definition signature
        def_hash = make_hash(sig)
        if uid:
            uid_to_hash[uid] = def_hash

# -------------------------
# record.v2 identity + sig_hash (NO sentinel literals; q marks missing/unreadable)
# -------------------------
identity_items = []

# Required: uid_or_namekey
raw_uid = None
try:
    raw_uid = getattr(e, "UniqueId", None)
except Exception:
    raw_uid = None
uid_v, uid_q = canonicalize_str(raw_uid)
if uid_v is None:
    # Fallback: Name as key only when UID is absent/unreadable
    raw_name = None
    try:
        raw_name = getattr(e, "Name", None)
    except Exception:
        raw_name = None
    uid_v, uid_q = canonicalize_str(raw_name)
identity_items.append(make_identity_item("line_pattern.uid_or_namekey", uid_v, uid_q))

# Segments acquisition
segs_v2 = None
segs_ok = True
if lp is None:
    segs_ok = False
else:
    try:
        if hasattr(lp, "GetSegments"):
            segs_v2 = list(lp.GetSegments() or [])
        else:
            segs_v2 = list(getattr(lp, "Segments", None) or [])
    except Exception:
        segs_ok = False
        segs_v2 = None

# Required: segment_count
if segs_ok and segs_v2 is not None:
    seg_count_v, seg_count_q = canonicalize_int(len(segs_v2))
else:
    seg_count_v, seg_count_q = (None, ITEM_Q_UNREADABLE)
identity_items.append(make_identity_item("line_pattern.segment_count", seg_count_v, seg_count_q))

# Indexed segment items
any_segment_incomplete = False
if segs_ok and segs_v2 is not None:
    for idx, s in enumerate(segs_v2):
        idx3 = "{:03d}".format(idx)
        st_id, _st_name = _lp_seg_type_id_and_name(s)
        kind_v, kind_q = canonicalize_int(st_id) if st_id is not None else (None, ITEM_Q_UNREADABLE)
        identity_items.append(make_identity_item("line_pattern.seg[{}].kind".format(idx3), kind_v, kind_q))

        try:
            slen = getattr(s, "Length", None)
        except Exception:
            slen = None
        length_v, length_q = canonicalize_float(slen, nd=9)
        identity_items.append(make_identity_item("line_pattern.seg[{}].length".format(idx3), length_v, length_q))

        if kind_q != ITEM_Q_OK or length_q != ITEM_Q_OK:
            any_segment_incomplete = True

        # Sort items by k for validator determinism.
        identity_items_sorted = sorted(identity_items, key=lambda it: it.get("k", ""))

        # Block if required keys not ok (contract minima) OR any segment item incomplete
        required_qs = [uid_q, seg_count_q]
        blocked_required = any(q != ITEM_Q_OK for q in required_qs)
        blocked = bool(blocked_required or any_segment_incomplete)

        status_reasons_v2 = []
        for it in identity_items_sorted:
            q = it.get("q")
            if q != ITEM_Q_OK:
                status_reasons_v2.append("identity.incomplete:{}:{}".format(q, it.get("k")))
        if lp is None:
            status_reasons_v2.append("get_line_pattern_failed")
        if not segs_ok:
            status_reasons_v2.append("segments_unreadable")

        status_v2 = STATUS_BLOCKED if blocked else STATUS_OK
        preimage_v2 = serialize_identity_items(identity_items_sorted)
        sig_hash_v2 = make_hash(preimage_v2)
        rec_v2 = build_record_v2(
            domain="line_patterns",
            record_id=safe_str(raw_uid or getattr(getattr(e, "Id", None), "IntegerValue", "")),
            status=status_v2,
            status_reasons=sorted(set(status_reasons_v2)),
            sig_hash=sig_hash_v2,
            identity_items=identity_items_sorted,
            required_qs=required_qs,
            label={
                "display": safe_str(getattr(e, "Name", None) or ""),
                "quality": "human",
                "provenance": "revit.LinePatternElement.Name",
                "components": {
                    "element_id": safe_str(getattr(getattr(e, "Id", None), "IntegerValue", "")),
                },
            },
        )
        v2_records.append(rec_v2)

        # Only publish non-blocked records to dependency map (UID as lookup key)
        if status_v2 != STATUS_BLOCKED and uid_v is not None and raw_uid is not None:
            try:
                uid_to_hash_v2[safe_str(raw_uid)] = sig_hash_v2
            except Exception:
                pass
            v2_sig_hashes.append(sig_hash_v2)
        else:
            info["debug_v2_blocked"] += 1
            try:
                info["debug_v2_block_reasons"]["blocked_record"] = info["debug_v2_block_reasons"].get("blocked_record", 0) + 1
            except Exception:
                pass

        rec = {
            "id": safe_str(e.Id.IntegerValue),
            "name": name,          # metadata only
            "uid": uid,            # metadata only
            "def_hash": def_hash,  # hashed definition (or failure-signature)
        }
        if DEBUG_INCLUDE_LINEPATTERN_SIGNATURES:
            rec["def_signature"] = sig

    legacy_records.append(rec)
    per_hashes.append(def_hash)
    info["debug_kept"] += 1

    info["names"] = sorted(set(names))
    info["count"] = len(v2_records)
    info["legacy_records"] = sorted(legacy_records, key=lambda r: (r.get("name", ""), r.get("id", "")))
    info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"])

    # v2 finalize: block domain hash if any record blocked
    info["signature_hashes_v2"] = sorted(v2_sig_hashes)
    if info["debug_v2_blocked"] > 0:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    # Populate context for downstream domains (UID allowed only as lookup key)
    if ctx is not None:
        ctx["line_pattern_uid_to_hash"] = uid_to_hash
        ctx["line_pattern_uid_to_hash_v2"] = uid_to_hash_v2

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [
            {
                "record_key": safe_str(r.get("record_id", "")),
                "sig_hash": r.get("sig_hash", None),
                "name": safe_str(r.get("label", {}).get("display", "")),
            }
            for r in recs
        ]
    except Exception as e:
        info["record_rows"] = []

    return info

