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
    # Phase-2 additive emission (single-record domain)
    # ---------------------------

    lineage_items = _phase2_build_lineage_items(info)
    lineage_hash = phase2_join_hash(lineage_items)

    # Attribute hypotheses (Phase-2 only; no enforcement / no inference)
    semantic_items = []
    cosmetic_items = []
    unknown_items = []

    # semantic (hypothesis): worksharing + central path (often used as file identity signals)
    v, q = phase2_qv_from_legacy_sentinel_str(
        "true" if bool(info.get("is_workshared", False)) else "false",
        allow_empty=False,
    )
    semantic_items.append({"k": "identity.is_workshared", "q": q, "v": v})

    v, q = phase2_qv_from_legacy_sentinel_str(info.get("central_path", None), allow_empty=False)
    semantic_items.append({"k": "identity.central_path", "q": q, "v": v})

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
        "unknown_items": phase2_sorted_items(unknown_items),

        # lineage (heuristic): explicit non-authoritative correlation surface
        "lineage_items": lineage_items,
        "lineage_hash": lineage_hash,
    }

    return info
