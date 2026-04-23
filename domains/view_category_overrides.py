# -*- coding: utf-8 -*-
"""Dispatcher for view category overrides domain family."""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.hashing import make_hash, safe_str
from domains import view_category_overrides_model
from domains import view_category_overrides_annotation
from domains.view_templates import _VIEW_INSTANCES_CACHE_KEY


def extract(doc, ctx=None):
    """Legacy aggregate extractor for backward compatibility."""
    model = view_category_overrides_model.extract(doc, ctx)
    annotation = view_category_overrides_annotation.extract(doc, ctx)

    records = list(model.get("records") or []) + list(annotation.get("records") or [])
    signatures = list(model.get("signature_hashes_v2") or []) + list(annotation.get("signature_hashes_v2") or [])
    signatures_sorted = sorted(signatures)

    out = {
        "count": int(model.get("count", 0) or 0) + int(annotation.get("count", 0) or 0),
        "raw_count": int(model.get("raw_count", 0) or 0) + int(annotation.get("raw_count", 0) or 0),
        "records": sorted(records, key=lambda r: safe_str(r.get("record_id", ""))),
        "signature_hashes_v2": signatures_sorted,
        "hash_v2": make_hash(signatures_sorted) if signatures_sorted else None,
        "debug_v2_blocked": bool(model.get("debug_v2_blocked") or annotation.get("debug_v2_blocked")),
        "debug_partitions": {
            "view_category_overrides_model": model,
            "view_category_overrides_annotation": annotation,
        },
    }

    if out["debug_v2_blocked"]:
        out["hash_v2"] = None

    return out


# Backward-compatible re-export for callers/tests.
_compute_override_properties_hash = view_category_overrides_model._compute_override_properties_hash
_phase2_partition_items = view_category_overrides_model._phase2_partition_items

# Compatibility note for cache-key consistency tests:
# cache_key=_VIEW_INSTANCES_CACHE_KEY
