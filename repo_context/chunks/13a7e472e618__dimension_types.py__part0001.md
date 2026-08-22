# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 1 of 8
- Original line range: 1-177
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _collect_dim_types, _build_dimension_instance_count_map, _attach_placeholder_metadata, _apply_family_name_override
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| 
     2| # -*- coding: utf-8 -*-
     3| """
     4| Dimension Types domain family extractor.
     5| 
     6| One extract_* function per domain. All share module-level constants,
     7| helper imports, and cached DimensionType collection.
     8| """
     9| 
    10| import os
    11| import sys
    12| 
    13| current_dir = os.path.dirname(os.path.abspath(__file__))
    14| repo_root = os.path.dirname(current_dir)
    15| if repo_root not in sys.path:
    16|     sys.path.insert(0, repo_root)
    17| 
    18| from core.hashing import make_hash, safe_str
    19| from core.collect import collect_types, collect_instances, purge_lookup
    20| from core.rows import first_param, _as_string, _as_value_string, _as_double, _as_int, format_len_inches
    21| from core.canon import canon_str, S_MISSING, S_UNREADABLE
    22| from core.record_v2 import (
    23|     canonicalize_str,
    24|     canonicalize_str_allow_empty,
    25|     canonicalize_int,
    26|     canonicalize_float,
    27|     canonicalize_bool,
    28|     ITEM_Q_OK,
    29|     ITEM_Q_MISSING,
    30|     ITEM_Q_UNREADABLE,
    31|     ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    32|     build_record_v2,
    33|     make_identity_item,
    34|     serialize_identity_items,
    35|     STATUS_OK,
    36|     STATUS_DEGRADED,
    37|     STATUS_BLOCKED,
    38| )
    39| from core.phase2 import phase2_sorted_items
    40| from core.join_key_policy import get_domain_join_key_policy
    41| from core.join_key_builder import build_join_key_from_policy, compute_projection_status
    42| from core.dimension_type_helpers import (
    43|     _fmt_in_from_ft,
    44|     _get_dimension_shape,
    45|     _build_text_appearance_items,
    46|     _read_tick_mark_sig_hash,
    47|     _read_unit_format_info,
    48|     _read_prefix_suffix,
    49|     _read_leader_arrowhead,
    50|     _read_arrowhead_ref_sig_hash,
    51|     _read_element_ref_name,
    52|     _read_line_pattern_ref_sig_hash,
    53|     _build_alternate_units_items,
    54|     get_type_display_name,
    55|     SHAPE_LINEAR,
    56|     SHAPE_LINEAR_FIXED,
    57|     SHAPE_ARC_LENGTH,
    58|     SHAPE_ANGULAR,
    59|     SHAPE_RADIAL,
    60|     SHAPE_DIAMETER,
    61|     SHAPE_SPOT_ELEVATION,
    62|     SHAPE_SPOT_ELEVATION_FIXED,
    63|     SHAPE_SPOT_COORDINATE,
    64|     SHAPE_ALIGNMENT_STATION_LABEL,
    65|     SHAPE_SPOT_SLOPE,
    66|     FAMILY_LINEAR,
    67|     FAMILY_ANGULAR,
    68|     FAMILY_RADIAL,
    69|     FAMILY_SPOT,
    70| )
    71| 
    72| try:
    73|     from Autodesk.Revit.DB import BuiltInCategory, DimensionType
    74| except ImportError:
    75|     DimensionType = None
    76| 
    77| _CTX_DIM_TYPES_CACHE_KEY = "_dim_types_cache"
    78| _LINEAR_HANDLED = frozenset({SHAPE_LINEAR, SHAPE_LINEAR_FIXED, SHAPE_ARC_LENGTH})
    79| _ANGULAR_HANDLED = frozenset({SHAPE_ANGULAR})
    80| _RADIAL_HANDLED = frozenset({SHAPE_RADIAL})
    81| _DIAMETER_HANDLED = frozenset({SHAPE_DIAMETER, SHAPE_SPOT_ELEVATION_FIXED})
    82| _SPOT_ELEV_HANDLED = frozenset({SHAPE_SPOT_ELEVATION})
    83| _SPOT_COORD_HANDLED = frozenset({SHAPE_SPOT_COORDINATE, SHAPE_ALIGNMENT_STATION_LABEL})
    84| _SPOT_SLOPE_HANDLED = frozenset({SHAPE_SPOT_SLOPE})
    85| _LINEAR_EXPECTED_FAMILY = "Linear Dimension Style"
    86| _ANGULAR_EXPECTED_FAMILY = "Angular Dimension Style"
    87| _RADIAL_EXPECTED_FAMILY = "Radial Dimension Style"
    88| _DIAMETER_EXPECTED_FAMILY = "Diameter Dimension Style"
    89| _SPOT_ELEV_EXPECTED_FAMILY = "Spot Elevations"
    90| _SPOT_COORD_EXPECTED_FAMILY = "Spot Coordinates"
    91| _SPOT_SLOPE_EXPECTED_FAMILY = "Spot Slopes"
    92| _SPOT_ELEV_PURGE_CATEGORY = (
    93|     getattr(BuiltInCategory, "OST_SpotElevations", BuiltInCategory.OST_Dimensions)
    94|     if "BuiltInCategory" in globals() and BuiltInCategory is not None else None
    95| )
    96| _SPOT_COORD_PURGE_CATEGORY = (
    97|     getattr(BuiltInCategory, "OST_SpotCoordinates", BuiltInCategory.OST_Dimensions)
    98|     if "BuiltInCategory" in globals() and BuiltInCategory is not None else None
    99| )
   100| _SPOT_SLOPE_PURGE_CATEGORY = (
   101|     getattr(BuiltInCategory, "OST_SpotSlopes", BuiltInCategory.OST_Dimensions)
   102|     if "BuiltInCategory" in globals() and BuiltInCategory is not None else None
   103| )
   104| 
   105| 
   106| def _collect_dim_types(doc, ctx):
   107|     if ctx is not None and _CTX_DIM_TYPES_CACHE_KEY in ctx:
   108|         return ctx[_CTX_DIM_TYPES_CACHE_KEY]
   109|     types = list(
   110|         collect_types(
   111|             doc,
   112|             of_class=DimensionType,
   113|             require_unique_id=True,
   114|             cctx=(ctx or {}).get("_collect") if ctx is not None else None,
   115|             cache_key="dimension_types:DimensionType:types",
   116|         )
   117|     )
   118|     if ctx is not None:
   119|         ctx[_CTX_DIM_TYPES_CACHE_KEY] = types
   120|     return types
   121| 
   122| 
   123| def _build_dimension_instance_count_map(doc, ctx):
   124|     out = {}
   125|     try:
   126|         instances = collect_instances(
   127|             doc,
   128|             of_category=getattr(BuiltInCategory, "OST_Dimensions", None),
   129|             cctx=(ctx or {}).get("_collect") if ctx is not None else None,
   130|             where_key="dimension_types.instances",
   131|         )
   132|         for inst in instances:
   133|             try:
   134|                 tid = int(getattr(getattr(inst, "GetTypeId", lambda: None)(), "IntegerValue", -1))
   135|                 if tid > 0:
   136|                     out[tid] = out.get(tid, 0) + 1
   137|             except Exception:
   138|                 continue
   139|         return out, "ok"
   140|     except Exception:
   141|         return {}, "unreadable"
   142| 
   143| 
   144| def _attach_placeholder_metadata(rec_v2, type_id_int, instance_count_map, instance_count_map_q):
   145|     if instance_count_map_q == "ok" and type_id_int is not None:
   146|         try:
   147|             rec_v2["instance_count"] = instance_count_map.get(int(type_id_int), 0)
   148|             rec_v2["instance_count_q"] = "ok"
   149|         except Exception:
   150|             rec_v2["instance_count"] = None
   151|             rec_v2["instance_count_q"] = "unreadable"
   152|     else:
   153|         rec_v2["instance_count"] = None
   154|         rec_v2["instance_count_q"] = "unreadable"
   155| 
   156| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
   157|     """
   158|     Heuristic override: if the FamilyName prefix indicates a Spot family,
   159|     force Spot classification so we skip this record (spot shapes have their own domain).
   160|     Returns updated (shape_v, shape_family, shape_q).
   161|     """
   162|     try:
   163|         family_name = getattr(d, "FamilyName", None)
   164|         basis = family_name if family_name else type_name
   165|         bn_l = safe_str(basis).strip().lower()
   166| 
   167|         if bn_l.startswith("spot slopes"):
   168|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
   169|         elif bn_l.startswith("spot elevations"):
   170|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
   171|         elif bn_l.startswith("spot coordinates"):
   172|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
   173|     except Exception:
   174|         pass
   175|     return (shape_v, shape_family, shape_q)
   176| 
   177| 
```
