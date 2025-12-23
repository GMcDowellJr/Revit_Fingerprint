# -*- coding: utf-8 -*-
"""
Phases domain extractor.

Fingerprints phase inventory and ordering including:
- Phase sequence number (ordering where available)
- Phase UniqueId (identity)

NOTE: Phase names are INCLUDED in behavioral hashes for cross-project comparability.
UniqueId remains identity/debug only (document-specific).

This is a GLOBAL domain - phases are defined once and referenced
by views, phase filters, and phase graphics.

Per-record identity: UniqueId (element-backed)
Ordering: sequence number (order-sensitive if available)
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
    from Autodesk.Revit.DB import FilteredElementCollector, Phase
except ImportError:
    FilteredElementCollector = None
    Phase = None


def extract(doc, ctx=None):
    """
    Extract Phases fingerprint from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (will be populated with phase_uid -> def_hash mapping)

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

        # v2 (contract semantic hash) — additive only; legacy behavior unchanged
        "hash_v2": None,
        "debug_v2_blocked": False,
        "debug_v2_block_reasons": {},

        # debug counters
        "debug_missing_name": 0,
        "debug_missing_uid": 0,
        "debug_kept": 0,
    }

    # Prefer doc.Phases to preserve true phase ordering in the document.
    # Fall back to collector if unavailable.
    try:
        col = list(doc.Phases)
    except:
        try:
            col = list(FilteredElementCollector(doc).OfClass(Phase))
        except:
            return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}  # For context population
    
    # v2 build state (domain-level block; no partial coverage semantics)
    per_hashes_v2 = []
    v2_blocked = False
    v2_reasons = {}

    def _v2_block(reason_key):
        nonlocal v2_blocked
        if not v2_blocked:
            v2_blocked = True
        v2_reasons[reason_key] = True

    for i, p in enumerate(col):
        name = canon_str(getattr(p, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            # legacy behavior unchanged
            name = "<unnamed>"
            # v2 contract: no sentinel hashing; block domain v2
            _v2_block("missing_name")
        names.append(name)

        try:
            uid = canon_str(p.UniqueId)
        except:
            uid = None
            info["debug_missing_uid"] += 1

        try:
            seq = p.SequenceNumber
        except:
            seq = None
        if seq is None:
            seq = i + 1  # stable fallback based on document order

        sig = [
            "seq={}".format(sig_val(seq)),
            "name={}".format(sig_val(name))
        ]

        # v2 signature: exclude element ids/UniqueIds; keep only semantic fields
        sig_v2 = [
            "seq={}".format(sig_val(seq)),
            "name={}".format(sig_val(name)),
        ]
        def_hash_v2 = make_hash(sig_v2)
        per_hashes_v2.append(def_hash_v2)

        def_hash = make_hash(sig)

        rec = {
            "id": safe_str(p.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "def_hash": def_hash,
            "def_signature": sig
        }

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

        if uid:
            uid_to_hash[uid] = def_hash

    # Populate context for downstream domains
    if ctx is not None:
        ctx["phase_uid_to_hash"] = uid_to_hash

    info["names"] = sorted(set(names))
    info["count"] = len(records)
    info["records"] = records
    info["signature_hashes"] = per_hashes
    info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None

    # v2 finalize (domain-level block; no partial coverage)
    if v2_blocked:
        info["hash_v2"] = None
        info["debug_v2_blocked"] = True
        info["debug_v2_block_reasons"] = v2_reasons
    else:
        info["hash_v2"] = make_hash(per_hashes_v2) if per_hashes_v2 else None
        info["debug_v2_blocked"] = False
        info["debug_v2_block_reasons"] = {}

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),
        } for r in recs]
    except:
        info["record_rows"] = []

    return info
