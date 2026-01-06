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


try:
    from Autodesk.Revit.DB import WorksharingUtils
except ImportError:
    WorksharingUtils = None


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

    return info
