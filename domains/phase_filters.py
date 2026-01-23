# -*- coding: utf-8 -*-
"""
Phase Filters domain extractor.

Fingerprints phase filter definitions including:
- New/Existing/Demolished/Temporary visibility and line style overrides

Phase filters define how elements in different phases are displayed.
This is a GLOBAL domain - filters are defined once and referenced by views.

Per-record identity: UniqueId (element-backed)
Ordering: deterministic (status order is fixed); record list order-insensitive
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
    S_MISSING,
    S_UNREADABLE,
    S_NOT_APPLICABLE,
)

from core.phase2 import (
    phase2_sorted_items,
    phase2_qv_from_legacy_sentinel_str,
    phase2_join_hash,
)
from core.record_v2 import (
    canonicalize_int,
    canonicalize_str_allow_empty,
    ITEM_Q_UNREADABLE,
)


def _phase2_build_join_key_items(*, name):
    """Domain-specific Phase-2 join key items for phase_filters."""
    v_name, q_name = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
    return phase2_sorted_items([
        {"k": "phase_filter.name", "q": q_name, "v": v_name},
    ])

try:
    from Autodesk.Revit.DB import PhaseFilter, ElementOnPhaseStatus
except ImportError:
    PhaseFilter = None
    ElementOnPhaseStatus = None


def extract(doc, ctx=None):
    """
    Extract phase filter fingerprint.

    Args:
        doc: Revit document
        ctx: Context dictionary (will be populated with phase_filter_uid -> def_hash mapping)

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
        "debug_fail_read": 0,
        "debug_kept": 0,

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
                of_class=PhaseFilter,
                require_unique_id=True,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="phase_filters:PhaseFilter:instances",
            )
        )
    except Exception as e:
        return info

    info["raw_count"] = len(col)

    names = []
    records = []
    per_hashes = []
    per_hashes_v2 = []
    uid_to_hash_v2 = {}  # For downstream v2 mapping (only when record v2 is ok)
    uid_to_hash = {}  # For context population

    # Phase statuses to check
    statuses = []
    if ElementOnPhaseStatus:
        try:
            statuses = [
                ("New", ElementOnPhaseStatus.New),
                ("Existing", ElementOnPhaseStatus.Existing),
                ("Demolished", ElementOnPhaseStatus.Demolished),
                ("Temporary", ElementOnPhaseStatus.Temporary),
            ]
        except Exception as e:
            pass

    for pf in col:
        # Name is metadata only
        name = canon_str(getattr(pf, "Name", None))
        if not name:
            info["debug_missing_name"] += 1
            name = S_MISSING
        names.append(name)

        uid = None
        try:
            uid = canon_str(getattr(pf, "UniqueId", None))
        except Exception as e:
            uid = None

        # Build v2 (contract semantic) signature in parallel (no names; block on unreadables)
        sig_v2 = []
        v2_ok = True
        v2_reason = None

        # Build phase filter signature (DETERMINISTIC ORDER)
        # Include name for semantic identity across projects.
        sig = ["name={}".format(sig_val(name))]

        # For each phase status (fixed order), get the graphic settings
        for status_name, status_enum in statuses:
            try:
                pres = pf.GetPhaseStatusPresentation(status_enum)
                sig.append("{}|presentation={}".format(status_name, sig_val(str(pres))))
            except Exception as e:
                info["debug_fail_read"] += 1
                sig.append("{}|presentation={}".format(status_name, sig_val(int(pres))))

            # v2: strict numeric enum id only; block on unreadable
            if v2_ok:
                try:
                    pres_v2 = pf.GetPhaseStatusPresentation(status_enum)
                    pres_int = int(pres_v2)
                    sig_v2.append("{}|presentation_id={}".format(status_name, sig_val(pres_int)))
                except Exception:
                    v2_ok = False
                    v2_reason = "presentation_unreadable"

        # Hash the definition (keep the deterministic order; do NOT sort)
        def_hash = make_hash(sig)
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
            except Exception as e:
                pass

        # Phase-2 (empirical, explanatory, reversible): join_key + attribute buckets
        join_key_items = _phase2_build_join_key_items(name=name)

        phase2_semantic_items = []
        for status_name, status_enum in statuses:
            k = "phase_filter.{}.presentation_id".format(safe_str(status_name).lower())
            try:
                pres_p2 = pf.GetPhaseStatusPresentation(status_enum)
                v_pres, q_pres = canonicalize_int(int(pres_p2))
            except Exception:
                v_pres, q_pres = None, ITEM_Q_UNREADABLE
            phase2_semantic_items.append({"k": k, "q": q_pres, "v": v_pres})

        v_uid, q_uid = canonicalize_str_allow_empty(uid)
        v_id, q_id = canonicalize_int(getattr(pf.Id, "IntegerValue", None))

        v_name_p2, q_name_p2 = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)

        phase2_unknown_items = phase2_sorted_items([
            {"k": "phase_filter.uid", "q": q_uid, "v": v_uid},
            {"k": "phase_filter.id.int", "q": q_id, "v": v_id},
            {"k": "phase_filter.name", "q": q_name_p2, "v": v_name_p2},
        ])

        rec_join_key = {
            "schema": "phase_filters.join_key.v1",
            "hash_alg": "md5_utf8_join_pipe",
            "items": join_key_items,
            "join_hash": phase2_join_hash(join_key_items),
        }

        rec_phase2 = {
            "schema": "phase2.phase_filters.v1",
            "grouping_basis": "phase2.hypothesis",
            "semantic_items": phase2_sorted_items(phase2_semantic_items),
            "cosmetic_items": [],
            "unknown_items": phase2_unknown_items,
        }

        rec = {
            "id": safe_str(pf.Id.IntegerValue),
            "uid": uid or "",
            "name": name,
            "def_hash": def_hash,
            "def_signature": sig,
            "join_key": rec_join_key,
            "phase2": rec_phase2,
        }

        records.append(rec)
        per_hashes.append(def_hash)
        info["debug_kept"] += 1

        # Populate context mapping
        if uid:
            uid_to_hash[uid] = def_hash

    # Populate context for downstream domains
    if ctx is not None:
        ctx["phase_filter_uid_to_hash"] = uid_to_hash
        ctx["phase_filter_uid_to_hash_v2"] = uid_to_hash_v2

    info["names"] = sorted(set(names))
    info["count"] = len(records)
    info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
    info["signature_hashes"] = sorted(per_hashes)
    info["hash"] = make_hash(info["signature_hashes"])
    info["signature_hashes_v2"] = sorted(per_hashes_v2)
    if info["debug_v2_blocked"] > 0:
        info["hash_v2"] = None
    else:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    info["record_rows"] = []
    try:
        recs = info.get("records") or []
        info["record_rows"] = [{
            "record_key": safe_str(r.get("uid", "")),
            "sig_hash":   safe_str(r.get("def_hash", "")),
            "name":       safe_str(r.get("name", "")),
        } for r in recs]
    except Exception as e:
        info["record_rows"] = []

    return info
