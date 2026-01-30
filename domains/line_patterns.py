# -*- coding: utf-8 -*-
"""
Line Patterns domain extractor.

Fingerprints line pattern definitions including:
- Segment count
- Per-segment type and length (order-sensitive)

Per-record identity: Structural segment definition (segment_count + per-segment kind/length)
Join key: Definition-based (segment_count + initial segment kind/length pairs)
Ordering: segment order is preserved (order-sensitive for segments)

Note: Per Phase 2 architecture, identity is definition-based (no UID/name).
UID and name are retained in unknown_items for context mappings but excluded from identity_basis.
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

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)

from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

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

def _phase2_build_join_key_items(*, segment_count, segments):
    """Build Phase-2 join-key IdentityItems (domain-specific, definition-based).

    Join-key per Phase 2 architecture:
      - Identity is defined by the ordered segment sequence, not name/UID.
      - For Phase-2 grouping, collapse the correlated seg[###] structure into
        a single definition-hash to avoid prefix collisions and reduce key width.

    Join-key items:
      - line_pattern.segment_count
      - line_pattern.sequence_hash  (hash of ordered (kind,length) sequence)
    """
    items = []

    # Segment count is required for join key
    seg_count_v, seg_count_q = canonicalize_int(segment_count)
    items.append(make_identity_item("line_pattern.segment_count", seg_count_v, seg_count_q))

    # Full sequence hash (definition-based; no UID/name)
    seq_hash_v, seq_hash_q = (None, ITEM_Q_UNREADABLE)
    if segments is not None:
        try:
            tokens = []
            for idx, seg in enumerate(segments):
                # kind
                st_id, _st_name = _lp_seg_type_id_and_name(seg)
                tokens.append("seg[{:03d}].kind={}".format(idx, safe_str(st_id)))

                # length (canonicalized to same precision as identity_items)
                try:
                    slen = getattr(seg, "Length", None)
                except Exception:
                    slen = None
                length_v, _length_q = canonicalize_float(slen, nd=9)
                tokens.append("seg[{:03d}].length={}".format(idx, safe_str(length_v)))

            # Deterministic preimage: preserve order in tokens
            seq_hash = make_hash(tokens)
            seq_hash_v, seq_hash_q = canonicalize_str(seq_hash)
        except Exception:
            seq_hash_v, seq_hash_q = (None, ITEM_Q_UNREADABLE)

    items.append(make_identity_item("line_pattern.sequence_hash", seq_hash_v, seq_hash_q))

    return phase2_sorted_items(items)

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
    except Exception:
        return info

    info["raw_count"] = len(col)

    names = []
    legacy_records = []
    v2_records = []
    per_hashes = []
    v2_sig_hashes = []
    uid_to_hash = {}
    uid_to_hash_v2 = {}

    def _fnum_local(v, nd=9):
        if v is None:
            return S_MISSING
        try:
            return format(float(v), ".{}f".format(nd))
        except Exception:
            return sig_val(v)

    for e in col:
        # ---- name metadata ----
        name = canon_str(getattr(e, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = S_MISSING
        names.append(name)

        # ---- uid metadata ----
        uid = None
        try:
            uid = canon_str(getattr(e, "UniqueId", None))
        except Exception:
            uid = None

        # ---- get pattern ----
        lp = None
        try:
            lp = e.GetLinePattern()
        except Exception:
            lp = None

        if lp is None:
            try:
                lp = LinePatternElement.GetLinePattern(doc, e.Id)
            except Exception as ex:
                info["debug_fail_getpattern"] += 1
                t = ex.__class__.__name__
                info["debug_getpattern_ex_types"][t] = info["debug_getpattern_ex_types"].get(t, 0) + 1
                if len(info["debug_getpattern_ex_samples"]) < 5:
                    info["debug_getpattern_ex_samples"].append(
                        {
                            "name": name,
                            "id": safe_str(e.Id.IntegerValue),
                            "uid": uid,
                            "ex_type": t,
                            "ex_msg": safe_str(str(ex)),
                        }
                    )
                lp = None

        # -------------------------
        # Legacy signature (UNCHANGED behavior)
        # -------------------------
        sig = []
        if lp is None:
            sig.append("error=GetLinePatternFailed")
        else:
            segs = None
            try:
                if hasattr(lp, "GetSegments"):
                    segs = list(lp.GetSegments() or [])
                else:
                    segs = list(getattr(lp, "Segments", None) or [])
            except Exception as ex:
                info["debug_fail_segment_read"] += 1
                t = ex.__class__.__name__
                info["debug_segment_ex_types"][t] = info["debug_segment_ex_types"].get(t, 0) + 1
                if len(info["debug_segment_ex_samples"]) < 5:
                    info["debug_segment_ex_samples"].append(
                        {
                            "name": name,
                            "id": safe_str(e.Id.IntegerValue),
                            "uid": uid,
                            "ex_type": t,
                            "ex_msg": safe_str(str(ex)),
                        }
                    )
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

                        if st_id == 2:
                            slen = 0.0

                        sig.append("seg[{}].type_id={}".format(idx, sig_val(st_id)))
                        sig.append("seg[{}].type={}".format(idx, sig_val(st_name)))
                        sig.append("seg[{}].len={}".format(idx, sig_val(_fnum_local(slen, 9))))
                    except Exception:
                        info["debug_fail_segment_read"] += 1
                        sig.append("seg[{}].error=SegmentReadFailed".format(idx))

        def_hash = make_hash(sig)
        if uid:
            uid_to_hash[uid] = def_hash

        # -------------------------
        # record.v2 identity + sig_hash
        # -------------------------
        # Phase-2 compliant: definition-based identity (no UID/name references)
        # Identity is based on structural segment data per Phase 2 architecture
        identity_items = []

        # Capture UID for context mappings (valid use) but NOT for identity
        raw_uid = None
        try:
            raw_uid = getattr(e, "UniqueId", None)
        except Exception:
            raw_uid = None
        uid_v, uid_q = canonicalize_str(raw_uid)

        # Segments acquisition (v2)
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

        # Indexed segment items (optional for minima; still identity-bearing)
        any_segment_incomplete = False
        if segs_ok and segs_v2 is not None:
            for idx, s in enumerate(segs_v2):
                idx3 = "{:03d}".format(idx)

                st_id, _st_name = _lp_seg_type_id_and_name(s)
                if st_id is not None:
                    kind_v, kind_q = canonicalize_int(st_id)
                else:
                    kind_v, kind_q = (None, ITEM_Q_UNREADABLE)
                identity_items.append(make_identity_item("line_pattern.seg[{}].kind".format(idx3), kind_v, kind_q))

                try:
                    slen = getattr(s, "Length", None)
                except Exception:
                    slen = None

                # Normalize Dot segment length to 0.0 for stability (matches legacy signature behavior)
                if st_id == 2:
                    slen = 0.0

                length_v, length_q = canonicalize_float(slen, nd=9)
                identity_items.append(make_identity_item("line_pattern.seg[{}].length".format(idx3), length_v, length_q))

                if kind_q != ITEM_Q_OK or length_q != ITEM_Q_OK:
                    any_segment_incomplete = True

        # ---- element-level finalize (once per element) ----
        identity_items_sorted = sorted(identity_items, key=lambda it: it.get("k", ""))

        # Build join-key candidate item set (structured pointer surface; excludes full seg[###] leaf duplication)
        join_key_items_sorted = _phase2_build_join_key_items(
            segment_count=(len(segs_v2) if (segs_ok and segs_v2 is not None) else None),
            segments=segs_v2 if (segs_ok and segs_v2 is not None) else None,
        )

        # Phase-2 compliant: required identity is segment-based, not UID-based
        required_qs = [seg_count_q]
        blocked_required = any(q != ITEM_Q_OK for q in required_qs)

        # Prefer fewer misses: segment item issues => degraded, not blocked
        blocked = bool(blocked_required)
        degraded = bool((not blocked) and (any_segment_incomplete or (lp is None) or (not segs_ok)))

        status_reasons_v2 = []
        for it in identity_items_sorted:
            q = it.get("q")
            if q != ITEM_Q_OK:
                status_reasons_v2.append("identity.incomplete:{}:{}".format(q, it.get("k")))

        if lp is None:
            status_reasons_v2.append("get_line_pattern_failed")
        if not segs_ok:
            status_reasons_v2.append("segments_unreadable")

        if blocked:
            status_v2 = STATUS_BLOCKED
        elif degraded:
            status_v2 = STATUS_DEGRADED
        else:
            status_v2 = STATUS_OK

        preimage_v2 = serialize_identity_items(identity_items_sorted)
        sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(preimage_v2)

        label_display = safe_str(getattr(e, "Name", None) or "")
        if label_display:
            label_quality = "human"
        else:
            label_quality = "placeholder_missing"

        rec_v2 = build_record_v2(
            domain="line_patterns",
            record_id=safe_str(getattr(getattr(e, "Id", None), "IntegerValue", "")),
            status=status_v2,
            status_reasons=sorted(set(status_reasons_v2)),
            sig_hash=sig_hash_v2,
            identity_items=identity_items_sorted,
            required_qs=required_qs,
            label={
                "display": label_display,
                "quality": label_quality,
                "provenance": "revit.Name",
                "components": {
                    "element_id": safe_str(getattr(getattr(e, "Id", None), "IntegerValue", "")),
                },
            },
        )
        
        # -------------------------
        # Phase-2 additions (additive, explanatory, reversible)
        # -------------------------

        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "line_patterns")
        rec_v2["join_key"], _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=join_key_items_sorted,
        )

        semantic_items = []
        cosmetic_items = []
        unknown_items = []

        # Phase-2 grouping:
        # - Keep a single pointer to the structural definition (definition-based; no name/UID)
        # - Avoid duplicating the full seg[###] structure which already exists in identity_basis.items
        seg_ct_it = None
        seq_hash_it = None
        for it in (join_key_items_sorted or []):
            k = safe_str(it.get("k", ""))
            if k == "line_pattern.segment_count":
                seg_ct_it = it
            elif k == "line_pattern.sequence_hash":
                seq_hash_it = it

        # Breadcrumb: segment count (small scalar)
        if seg_ct_it is not None:
            semantic_items.append(seg_ct_it)

        # Derived Phase-2 pointer (hash of ordered segment definition)
        if seq_hash_it is not None:
            semantic_items.append(
                make_identity_item(
                    "line_pattern.segments_def_hash",
                    seq_hash_it.get("v", None),
                    seq_hash_it.get("q", ITEM_Q_UNREADABLE),
                )
            )

        # Add file-local identifiers to unknown_items (not part of identity_basis)
        unknown_items.append(
            make_identity_item(
                "line_pattern.uid",
                uid_v,
                uid_q,
            )
        )
        name_v, name_q = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
        unknown_items.append(
            make_identity_item(
                "line_pattern.name",
                name_v,
                name_q,
            )
        )
        unknown_items.append(
            make_identity_item(
                "line_pattern.element_id",
                safe_str(getattr(getattr(e, "Id", None), "IntegerValue", "")),
                ITEM_Q_OK,
            )
        )

        rec_v2["phase2"] = {
            "schema": "phase2.line_patterns.v1",
            "grouping_basis": "phase2.hypothesis",
            "semantic_items": phase2_sorted_items(semantic_items),
            "cosmetic_items": phase2_sorted_items(cosmetic_items),
            "unknown_items": phase2_sorted_items(unknown_items),
        }
        
        v2_records.append(rec_v2)

        # Only publish non-blocked records to dependency map (UID as lookup key)
        if status_v2 != STATUS_BLOCKED and uid_v is not None and raw_uid is not None:
            uid_to_hash_v2[safe_str(raw_uid)] = sig_hash_v2
            v2_sig_hashes.append(sig_hash_v2)
        else:
            info["debug_v2_blocked"] += 1
            info["debug_v2_block_reasons"]["blocked_record"] = info["debug_v2_block_reasons"].get("blocked_record", 0) + 1

        # legacy record (per element)
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

    # ---- domain-level finalize (once) ----
    info["names"] = sorted(set(names))
    info["count"] = len(v2_records)
    info["legacy_records"] = sorted(legacy_records, key=lambda r: (r.get("name", ""), r.get("id", "")))
    info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"])

    info["signature_hashes_v2"] = sorted(v2_sig_hashes)
    info["hash_v2"] = None if info["debug_v2_blocked"] > 0 else make_hash(info["signature_hashes_v2"])

    if ctx is not None:
        ctx["line_pattern_uid_to_hash"] = uid_to_hash
        ctx["line_pattern_uid_to_hash_v2"] = uid_to_hash_v2

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
    except Exception:
        info["record_rows"] = []

    return info
