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

def _phase2_build_join_key_items(info):
    """
    Phase-2 join-key hypothesis for identity domain.

    Notes (Phase-2, non-prescriptive):
    - identity domain emits a single metadata payload per file.
    - join_key is intended to support stable joins across exports without
      changing any existing domain hash behavior (identity has no hash_v2).
    """
    items = []

    # Candidate natural join components (hypotheses only):
    # - central_path: best-available stable locator (may still vary in non-workshared cases)
    # - project_title: human-readable but may change; included as a separate key component
    for k in ("central_path", "project_title"):
        v_raw = info.get(k, None)
        v, q = phase2_qv_from_legacy_sentinel_str(v_raw, allow_empty=False)
        items.append({"k": "identity.{}".format(k), "q": q, "v": v})

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

    phase2_join_items = _phase2_build_join_key_items(info)
    join_hash = phase2_join_hash(phase2_join_items)

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

    info["join_key"] = {
        "schema": "identity.join_key.v1",
        "hash_alg": "md5_utf8_join_pipe",
        "items": phase2_join_items,
        "join_hash": join_hash,
    }

    info["phase2"] = {
        "schema": "phase2.identity.v1",
        "grouping_basis": "phase2.hypothesis",
        "semantic_items": phase2_sorted_items(semantic_items),
        "cosmetic_items": phase2_sorted_items(cosmetic_items),
        "unknown_items": phase2_sorted_items(unknown_items),
    }

    return info
