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

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.collect import collect_types
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
    sig_val,
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import Phase
except ImportError:
    Phase = None

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
)
from core.record_v2 import (
    STATUS_OK,
    STATUS_BLOCKED,
    ITEM_Q_OK,
    canonicalize_int,
    canonicalize_str,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)


def _phase2_build_phase2_payload(*, phase_name, seq, uid):
    """
    Phase-2 grouping (hypotheses only; no enforcement).
    """
    # Selector-only semantic basis (no duplicated k/q/v evidence).
    # Canonical evidence lives in record.identity_basis.items.
    semantic_keys = ["phase.name", "phase.seq"]
    cosmetic_items = []
    unknown_items = []

    v_name, q_name = phase2_qv_from_legacy_sentinel_str(phase_name, allow_empty=False)
    cosmetic_items.append({"k": "phase.name", "q": q_name, "v": v_name})

    # seq is retained as semantic selector key only; avoid duplicating k/q/v here.
    _ = canon_num(seq, nd=0)

    # uid is document-scoped; keep explicit but do not classify as semantic/cosmetic here.
    uid_in = uid if uid else None
    v_uid, q_uid = phase2_qv_from_legacy_sentinel_str(uid_in, allow_empty=False)
    unknown_items.append({"k": "phase.uid", "q": q_uid, "v": v_uid})

    return {
        "schema": "phase2.phases.v1",
        "grouping_basis": "phase2.hypothesis",
        # Selector-based semantic declaration; values come from identity_basis.items.
        "semantic_keys": sorted(semantic_keys),
        "cosmetic_items": phase2_sorted_items(cosmetic_items),
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items(unknown_items),
    }


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
    except Exception as e:
        try:
            col = list(
                collect_types(
                    doc,
                    of_class=Phase,
                    require_unique_id=True,
                    cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                    cache_key="phases:Phase:types",
                )
            )
        except Exception as e:
            return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    uid_to_hash = {}  # For context population
    
    # v2 build state (domain-level block; no partial coverage semantics)
    per_hashes_v2 = []
    v2_records = []
    v2_sig_hashes = []
    uid_to_hash_v2 = {}
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
            name = S_MISSING
            # v2 contract: no sentinel hashing; block domain v2
            _v2_block("missing_name")
        names.append(name)

        try:
            uid = canon_str(p.UniqueId)
        except Exception as e:
            uid = None
            info["debug_missing_uid"] += 1

        try:
            seq = p.SequenceNumber
        except Exception as e:
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

        # Phase-2 attribute grouping (hypotheses only)
        phase2_payload = _phase2_build_phase2_payload(
            phase_name=name,
            seq=seq,
            uid=uid,
        )

        status_v2 = STATUS_OK
        status_reasons_v2 = []
        identity_items_v2 = []

        seq_v2, seq_q = canonicalize_int(seq)
        name_v2, name_q = canonicalize_str(name)

        identity_items_v2.append(make_identity_item("phase.seq", seq_v2, seq_q))
        identity_items_v2.append(make_identity_item("phase.name", name_v2, name_q))

        required_qs = [seq_q, name_q]
        if any(q != ITEM_Q_OK for q in required_qs):
            status_v2 = STATUS_BLOCKED
            status_reasons_v2.append("required_identity_not_ok")

        identity_items_v2_sorted = sorted(identity_items_v2, key=lambda d: str(d.get("k", "")))

        pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "phases")
        join_key_v2, _missing = build_join_key_from_policy(
            domain_policy=pol,
            identity_items=identity_items_v2_sorted,
            # Pilot: join hash from base required (+ gated required) only.
            # Optional keys remain available in identity_basis.items for future exploration.
            include_optional_items=False,
            emit_keys_used=True,
            hash_optional_items=False,
        )

        sig_preimage_v2 = serialize_identity_items(identity_items_v2_sorted)
        sig_hash_v2 = None if status_v2 == STATUS_BLOCKED else make_hash(sig_preimage_v2)

        rec_v2 = build_record_v2(
            domain="phases",
            record_id=safe_str(name) if safe_str(name) else safe_str(p.Id.IntegerValue),
            status=status_v2,
            status_reasons=sorted(set(status_reasons_v2)),
            sig_hash=sig_hash_v2,
            identity_items=identity_items_v2_sorted,
            required_qs=required_qs,
            label={
                "display": safe_str(name),
                "quality": "human",
                "provenance": "revit.Phase.Name",
                "components": {"seq": safe_str(seq)},
            },
            debug={
                "sig_preimage_sample": sig_preimage_v2[:6],
                "uid_excluded_from_sig": True,
            },
        )
        rec_v2["join_key"] = join_key_v2
        rec_v2["phase2"] = phase2_payload
        rec_v2["sig_basis"] = {
            "schema": "phases.sig_basis.v1",
            "keys_used": ["phase.name", "phase.seq"],
        }

        v2_records.append(rec_v2)
        if sig_hash_v2 is not None:
            v2_sig_hashes.append(sig_hash_v2)
            if uid:
                uid_to_hash_v2[uid] = sig_hash_v2

        rec = {
            "id": safe_str(p.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "def_hash": def_hash,
            "def_signature": sig,

            # Phase-2 additions (required)
            "phase2": phase2_payload,
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
    info["legacy_records"] = records
    info["records"] = v2_records
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

    info["signature_hashes_v2"] = sorted(v2_sig_hashes)
    info["record_rows"] = [{
        "record_key": safe_str(r.get("record_id", "")),
        "sig_hash":   safe_str(r.get("sig_hash", "")),
        "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
    } for r in v2_records if isinstance(r, dict)]

    return info
