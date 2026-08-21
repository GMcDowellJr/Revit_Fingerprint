# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 1 of 8
- Original line range: 1-132
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _phase2_fill_pattern_is_import, _export_fill_pattern_ctx, _collect_fill_patterns
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| 
     2| # -*- coding: utf-8 -*-
     3| """Fill Patterns domain family extractor."""
     4| 
     5| import os
     6| import re
     7| import sys
     8| 
     9| current_dir = os.path.dirname(os.path.abspath(__file__))
    10| repo_root = os.path.dirname(current_dir)
    11| if repo_root not in sys.path:
    12|     sys.path.insert(0, repo_root)
    13| 
    14| from core.hashing import make_hash, safe_str
    15| from core.collect import purge_lookup, collect_instances
    16| from core.canon import canon_str, fnum, canon_num, canon_bool, canon_id, S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
    17| from core.phase2 import phase2_sorted_items, phase2_qv_from_legacy_sentinel_str
    18| from core.record_v2 import (
    19|     STATUS_OK,
    20|     STATUS_BLOCKED,
    21|     ITEM_Q_OK,
    22|     ITEM_Q_MISSING,
    23|     ITEM_Q_UNREADABLE,
    24|     canonicalize_str,
    25|     canonicalize_int,
    26|     canonicalize_bool,
    27|     canonicalize_float,
    28|     make_identity_item,
    29|     serialize_identity_items,
    30|     build_record_v2,
    31| )
    32| from core.join_key_policy import get_domain_join_key_policy
    33| from core.join_key_builder import build_join_key_from_policy, compute_projection_status
    34| 
    35| try:
    36|     from Autodesk.Revit.DB import FillPatternElement
    37| except ImportError:
    38|     FillPatternElement = None
    39| 
    40| DEBUG_INCLUDE_FILLPATTERN_SIGNATURES = False
    41| _CTX_FILL_PATTERNS_CACHE_KEY = "_fill_patterns_cache"
    42| _TARGET_DRAFTING_INT = 0
    43| _TARGET_MODEL_INT = 1
    44| CTX_FILL_PATTERN_UID_TO_HASH = "fill_pattern_uid_to_hash"
    45| CTX_FILL_PATTERN_ID_TO_VALUE = "fill_pattern_id_to_value"
    46| CTX_FILL_PATTERN_SPECIAL_VALUES = "fill_pattern_special_values"
    47| FILL_PATTERN_SYMBOLIC_NO_PATTERN = "<No Pattern>"
    48| FILL_PATTERN_SYMBOLIC_SOLID = "<" + "Solid>"
    49| 
    50| _FILL_PATTERN_IMPORT_NAME_RE = re.compile(r"^(?:AR|ANSI|ISO|IMPORT)[-_ ]", re.IGNORECASE)
    51| _FILL_PATTERN_IMPORT_CATEGORY_MARKERS = ("IMPORT", "IMPORTED")
    52| 
    53| 
    54| def _phase2_fill_pattern_is_import(elem, name):
    55|     """Best-effort PAT-import flag for FillPatternElement extraction.
    56| 
    57|     Returns (v, q) matching the IdentityItem.v contract: v is "true"/"false"/None
    58|     (never a raw bool), q is an ITEM_Q_* quality flag. Every success path routes
    59|     through canonicalize_bool() so callers can pass the result straight into the
    60|     identity-item constructor without an extra coercion step.
    61|     """
    62|     direct_attrs = (
    63|         "IsImported",
    64|         "IsImport",
    65|         "Imported",
    66|         "FromFile",
    67|         "IsFromFile",
    68|         "IsExternal",
    69|         "IsFromExternalResource",
    70|     )
    71|     for attr in direct_attrs:
    72|         try:
    73|             if hasattr(elem, attr):
    74|                 v = getattr(elem, attr)
    75|                 if callable(v):
    76|                     v = v()
    77|                 return canonicalize_bool(bool(v))
    78|         except Exception:
    79|             continue
    80| 
    81|     try:
    82|         cat = getattr(elem, "Category", None)
    83|         cat_name = safe_str(getattr(cat, "Name", "")).upper() if cat is not None else ""
    84|         if any(marker in cat_name for marker in _FILL_PATTERN_IMPORT_CATEGORY_MARKERS):
    85|             return canonicalize_bool(True)
    86|     except Exception:
    87|         pass
    88| 
    89|     try:
    90|         nm = safe_str(name).strip()
    91|         if nm:
    92|             return canonicalize_bool(bool(_FILL_PATTERN_IMPORT_NAME_RE.match(nm)))
    93|         return None, ITEM_Q_MISSING
    94|     except Exception:
    95|         return None, ITEM_Q_UNREADABLE
    96| 
    97| 
    98| def _export_fill_pattern_ctx(ctx, uid_to_hash_v2, id_to_value):
    99|     if ctx is None:
   100|         return
   101|     existing_uid = ctx.get(CTX_FILL_PATTERN_UID_TO_HASH) or {}
   102|     existing_uid.update(uid_to_hash_v2 or {})
   103|     ctx[CTX_FILL_PATTERN_UID_TO_HASH] = existing_uid
   104| 
   105|     existing_id = ctx.get(CTX_FILL_PATTERN_ID_TO_VALUE) or {}
   106|     existing_id.update(id_to_value or {})
   107|     ctx[CTX_FILL_PATTERN_ID_TO_VALUE] = existing_id
   108| 
   109|     existing_specials = ctx.get(CTX_FILL_PATTERN_SPECIAL_VALUES) or {}
   110|     existing_specials.update({
   111|         "no_pattern": FILL_PATTERN_SYMBOLIC_NO_PATTERN,
   112|         "solid": FILL_PATTERN_SYMBOLIC_SOLID,
   113|     })
   114|     ctx[CTX_FILL_PATTERN_SPECIAL_VALUES] = existing_specials
   115| 
   116| 
   117| def _collect_fill_patterns(doc, ctx):
   118|     if ctx is not None and _CTX_FILL_PATTERNS_CACHE_KEY in ctx:
   119|         return ctx[_CTX_FILL_PATTERNS_CACHE_KEY]
   120|     col = list(
   121|         collect_instances(
   122|             doc,
   123|             of_class=FillPatternElement,
   124|             require_unique_id=True,
   125|             cctx=(ctx or {}).get("_collect") if ctx is not None else None,
   126|             cache_key="fill_patterns:FillPatternElement:instances",
   127|         )
   128|     )
   129|     if ctx is not None:
   130|         ctx[_CTX_FILL_PATTERNS_CACHE_KEY] = col
   131|     return col
   132| 
```
