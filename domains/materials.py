# -*- coding: utf-8 -*-
"""Materials domain extractor (ctx-only stub).

This domain is ctx-only by design: it intentionally emits ``hash_v2: None`` and
no records while populating material lookup maps for downstream consumers.

Future promotion path: when cut/surface fill pattern capture is added, this
extractor will become a governance domain, add ``require_domain("fill_patterns")``,
and consume ``fill_pattern_uid_to_sig_hash_v2`` from ``ctx``.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.collect import collect_instances
from core.canon import S_MISSING, S_UNREADABLE

try:
    from Autodesk.Revit.DB import Material
except ImportError:
    Material = None


CTX_MATERIAL_UID_TO_NAME = "material_uid_to_name"
CTX_MATERIAL_UID_TO_CLASS = "material_uid_to_class"
_CTX_ONLY = True  # Suppresses runner "no hash" warnings — ctx-only domain by design


def extract(doc, ctx=None):
    """Extract material lookups into ctx; this stub emits no records or hash."""
    result = {
        "count": 0,
        "raw_count": 0,
        "hash_v2": None,
        "records": [],
        "record_rows": [],
        "status": "ok",
        "debug_missing_name": 0,
        "debug_missing_class": 0,
        "debug_unreadable": 0,
        "debug_v2_blocked": False,
    }

    uid_to_name = {}
    uid_to_class = {}

    if Material is None:
        result["status"] = "blocked"
        result["debug_v2_blocked"] = True
        if ctx is not None:
            existing_names = ctx.get(CTX_MATERIAL_UID_TO_NAME) or {}
            existing_classes = ctx.get(CTX_MATERIAL_UID_TO_CLASS) or {}
            existing_names.update(uid_to_name)
            existing_classes.update(uid_to_class)
            ctx[CTX_MATERIAL_UID_TO_NAME] = existing_names
            ctx[CTX_MATERIAL_UID_TO_CLASS] = existing_classes
        return result

    try:
        elems = list(
            collect_instances(
                doc,
                of_class=Material,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="materials:Material:instances",
            )
        )
    except Exception:
        elems = []

    result["raw_count"] = len(elems)

    for e in elems:
        uid = None
        try:
            uid = str(e.UniqueId)
        except Exception:
            uid = None
        if not uid:
            continue

        name = S_UNREADABLE
        mat_class = S_UNREADABLE

        try:
            v = e.Name
            if v is None:
                name = S_MISSING
            else:
                s = str(v).strip()
                name = s if s else S_MISSING
        except Exception:
            name = S_UNREADABLE

        try:
            v = e.MaterialClass
            if v is None:
                mat_class = S_MISSING
            else:
                s = str(v).strip()
                mat_class = s if s else S_MISSING
        except Exception:
            mat_class = S_UNREADABLE

        if name == S_UNREADABLE:
            result["debug_unreadable"] += 1
        elif name == S_MISSING:
            result["debug_missing_name"] += 1
        else:
            result["count"] += 1

        if mat_class == S_UNREADABLE:
            result["debug_unreadable"] += 1
        elif mat_class == S_MISSING:
            result["debug_missing_class"] += 1

        uid_to_name[uid] = name
        uid_to_class[uid] = mat_class

    if result["debug_unreadable"] > 0:
        result["status"] = "degraded"

    if ctx is not None:
        existing_names = ctx.get(CTX_MATERIAL_UID_TO_NAME) or {}
        existing_classes = ctx.get(CTX_MATERIAL_UID_TO_CLASS) or {}
        existing_names.update(uid_to_name)
        existing_classes.update(uid_to_class)
        ctx[CTX_MATERIAL_UID_TO_NAME] = existing_names
        ctx[CTX_MATERIAL_UID_TO_CLASS] = existing_classes

    return result
