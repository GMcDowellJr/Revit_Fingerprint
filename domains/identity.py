# -*- coding: utf-8 -*-
"""
Identity domain extractor.

Captures project metadata including:
- Project title
- Central path / file path
- Worksharing status
- Revit version information

This is not a fingerprinted domain (no hash) - purely metadata.
"""

import os
import sys

# Ensure repo root is importable (so `import core...` works everywhere)
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from core.canon import (
    canon_str,
    canon_num,
    canon_bool,
    canon_id,
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
    STATUS_BLOCKED,
    STATUS_DEGRADED,
    STATUS_OK,
    ITEM_Q_OK,
    build_record_v2,
    canonicalize_bool,
    canonicalize_str,
    make_identity_item,
    serialize_identity_items,
)
from core.join_key_policy import get_domain_join_key_policy
from core.join_key_builder import build_join_key_from_policy

try:
    from Autodesk.Revit.DB import WorksharingUtils
except ImportError:
    WorksharingUtils = None

def _phase2_build_lineage_items(info):
    """
    Phase-2 lineage signals for identity domain (heuristic, non-authoritative).

    Purpose:
    - Support file history / lineage hypotheses (moved/copied/renamed) without
      presenting a domain join key that could be mistaken for identity.
    """
    items = []

    # Central path (raw) and a normalized variant (best-effort; still heuristic).
    cp_raw = info.get("central_path", None)
    v, q = phase2_qv_from_legacy_sentinel_str(cp_raw, allow_empty=False)
    items.append({"k": "identity.central_path", "q": q, "v": v})

    cp_norm = safe_str(cp_raw).strip().replace("\\", "/").lower()
    v, q = phase2_qv_from_legacy_sentinel_str(cp_norm, allow_empty=False)
    items.append({"k": "identity.central_path_norm", "q": q, "v": v})

    # Filename (weak signal; helpful when paths move but names persist).
    fn = os.path.basename(safe_str(cp_raw).replace("\\", "/").strip())
    v, q = phase2_qv_from_legacy_sentinel_str(fn, allow_empty=False)
    items.append({"k": "identity.filename", "q": q, "v": v})

    # Workshared flag (context signal).
    v_raw = "true" if bool(info.get("is_workshared", False)) else "false"
    v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
    items.append({"k": "identity.is_workshared", "q": q, "v": v})

    # Project title (very weak; include explicitly so it never “sneaks in” elsewhere).
    pt_raw = info.get("project_title", None)
    v, q = phase2_qv_from_legacy_sentinel_str(pt_raw, allow_empty=False)
    items.append({"k": "identity.project_title", "q": q, "v": v})

    return phase2_sorted_items(items)

def extract(doc, ctx=None):
    """
    Extract project identity metadata from document.

    Args:
        doc: Revit Document
        ctx: Context dictionary (unused for this domain)

    Returns:
        Dictionary with project metadata (no hash)
    """
    app = doc.Application
    info = {}

    info["project_title"] = safe_str(doc.Title)

    try:
        if doc.IsWorkshared:
            # Central path or model path
            try:
                mp = WorksharingUtils.GetModelPath(doc)
                info["central_path"] = safe_str(mp.CentralServerPath)
            except Exception as e:
                info["central_path"] = safe_str(doc.PathName)
        else:
            info["central_path"] = safe_str(doc.PathName)
    except Exception as e:
        info["central_path"] = safe_str(doc.PathName)

    info["is_workshared"] = bool(getattr(doc, "IsWorkshared", False))

    # Revit version/build
    info["revit_version_number"] = safe_str(app.VersionNumber)
    info["revit_version_name"]   = safe_str(app.VersionName)
    info["revit_build"]          = safe_str(app.VersionBuild)

    # ---------------------------
    # Phase-2 additive emission + record.v2 pilot (single-record domain)
    # ---------------------------

    lineage_items = _phase2_build_lineage_items(info)
    lineage_hash = phase2_join_hash(lineage_items)

    # Attribute hypotheses (Phase-2 only; no enforcement / no inference)
    semantic_items = []
    cosmetic_items = []
    unknown_items = []

    # semantic (hypothesis): worksharing + version/build metadata
    # NOTE: file-local identifiers (paths/title/filename) are intentionally excluded from
    # canonical identity evidence and remain only in label/lineage/debug surfaces.
    v, q = phase2_qv_from_legacy_sentinel_str(
        "true" if bool(info.get("is_workshared", False)) else "false",
        allow_empty=False,
    )
    semantic_items.append({"k": "identity.is_workshared", "q": q, "v": v})

    for k in ("revit_version_number", "revit_build"):
        v_raw = info.get(k, None)
        v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
        semantic_items.append({"k": "identity.{}".format(k), "q": q, "v": v})

    # cosmetic (hypothesis): application/version/build metadata
    for k in ("revit_version_number", "revit_version_name", "revit_build"):
        v_raw = info.get(k, None)
        v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
        cosmetic_items.append({"k": "identity.{}".format(k), "q": q, "v": v})

    # unknown (hypothesis): title may be stable or may change (keep explicit)
    v, q = phase2_qv_from_legacy_sentinel_str(info.get("project_title", None), allow_empty=False)
    unknown_items.append({"k": "identity.project_title", "q": q, "v": v})

    info["phase2"] = {
        "schema": "phase2.identity.v1",
        "grouping_basis": "phase2.hypothesis",
        "semantic_items": phase2_sorted_items(semantic_items),
        "cosmetic_items": phase2_sorted_items(cosmetic_items),
        "coordination_items": phase2_sorted_items([]),
        "unknown_items": phase2_sorted_items(unknown_items),

        # lineage (heuristic): explicit non-authoritative correlation surface
        "lineage_items": lineage_items,
        "lineage_hash": lineage_hash,
    }

    # Canonical evidence superset for this pilot is identity_basis.items (record.v2).
    # Selectors (join_key.keys_used, phase2.semantic_keys, sig_basis.keys_used) define
    # hashed/semantic subsets without duplicating k/q/v evidence.
    identity_items = []
    is_workshared_v, is_workshared_q = canonicalize_bool(info.get("is_workshared", False))
    identity_items.append(make_identity_item("identity.is_workshared", is_workshared_v, is_workshared_q))

    rvn_v, rvn_q = canonicalize_str(info.get("revit_version_number", None))
    identity_items.append(make_identity_item("identity.revit_version_number", rvn_v, rvn_q))

    rvname_v, rvname_q = canonicalize_str(info.get("revit_version_name", None))
    identity_items.append(make_identity_item("identity.revit_version_name", rvname_v, rvname_q))

    rb_v, rb_q = canonicalize_str(info.get("revit_build", None))
    identity_items.append(make_identity_item("identity.revit_build", rb_v, rb_q))

    identity_items = sorted(identity_items, key=lambda it: safe_str(it.get("k", "")))
    status_reasons = []
    if any(it.get("q") != ITEM_Q_OK for it in identity_items):
        status_reasons = [
            "identity.incomplete:{}:{}".format(it.get("q"), it.get("k"))
            for it in identity_items
            if it.get("q") != ITEM_Q_OK
        ]

    status = STATUS_OK if not status_reasons else STATUS_DEGRADED
    sig_preimage = serialize_identity_items(identity_items)
    sig_hash = make_hash(sig_preimage) if status != STATUS_BLOCKED else None

    pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), "identity")
    join_key, _missing = build_join_key_from_policy(
        domain_policy=pol,
        identity_items=identity_items,
        include_optional_items=False,
        emit_keys_used=True,
        hash_optional_items=False,
    )

    semantic_keys = sorted(["identity.is_workshared", "identity.revit_version_number", "identity.revit_build"])
    info["phase2"].pop("semantic_items", None)
    info["phase2"]["semantic_keys"] = semantic_keys

    rec_v2 = build_record_v2(
        domain="identity",
        record_id="document",
        status=status,
        status_reasons=sorted(set(status_reasons)),
        sig_hash=sig_hash,
        identity_items=identity_items,
        required_qs=[is_workshared_q, rvn_q, rb_q],
        label={
            "display": safe_str(info.get("project_title", "")),
            "quality": "human",
            "provenance": "revit.Document.Title",
        },
    )
    rec_v2["join_key"] = join_key
    rec_v2["phase2"] = info["phase2"]
    rec_v2["sig_basis"] = {
        "schema": "identity.sig_basis.v1",
        "keys_used": semantic_keys,
    }

    # Back-compat conveniences while the ecosystem pivots to record.v2.
    info["records"] = [rec_v2]
    info["record_rows"] = [{"record_key": "document", "sig_hash": sig_hash, "name": safe_str(info.get("project_title", ""))}]
    info["signature_hashes_v2"] = [sig_hash] if sig_hash else []
    info["hash_v2"] = sig_hash
    info["join_key"] = join_key
    info["sig_hash"] = sig_hash

    return info
