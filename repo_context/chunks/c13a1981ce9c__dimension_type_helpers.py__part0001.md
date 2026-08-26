# Chunk of core/dimension_type_helpers.py

- Source relative path: `core/dimension_type_helpers.py`
- Chunk: 1 of 3
- Original line range: 1-401
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _get_dimension_shape, _fmt_in_from_ft, _fmt_float, _format_options_to_kv, get_type_display_name
- Source SHA-256: dc024129e8ca371f3567208f529d49c1000eb622f9944da778190d69805bdfd6
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # -*- coding: utf-8 -*-
     2| """
     3| core/dimension_type_helpers.py
     4| 
     5| Shared helpers for the dimension_types_* domain extractors.
     6| 
     7| Provides:
     8|   - Shape detection constants and _get_dimension_shape()
     9|   - _format_options_to_kv() for FormatOptions serialization
    10|   - _fmt_in_from_ft() and _fmt_float() unit conversion helpers
    11|   - get_type_display_name() for DimensionType display names
    12|   - _build_text_appearance_items() for text/appearance identity items
    13|   - _read_tick_mark_sig_hash() for tick mark arrowhead sig hash lookup
    14|   - _read_unit_format_info() for primary/alternate UnitsFormatOptions reading
    15|   - _read_leader_arrowhead() for the spot-family Leader Arrowhead cluster
    16|     (Area 7 §1) -- shared by the 3 spot dimension_types partitions so the
    17|     text_types.py-derived resolve pattern isn't duplicated 3 times
    18|   - _read_arrowhead_ref_sig_hash() -- generic ElementId-param -> arrowheads
    19|     sig_hash resolver for the other tick-mark-family fields (Leader Tick
    20|     Mark, Centerline Tick Mark, Interior Tick Mark, Witness Line Tick Mark)
    21|   - _read_element_ref_name() -- generic ElementId-param -> referenced
    22|     element's display name resolver (Centerline Pattern/Symbol; no ctx
    23|     sig_hash map coverage confirmed for these, unlike the arrowhead family)
    24|   - _build_alternate_units_items() for the Alternate Units cluster (Area 7 §5)
    25| 
    26| Pure-Python and Revit-agnostic except where guarded by try/except ImportError.
    27| No domain imports.
    28| """
    29| 
    30| import os
    31| import sys
    32| 
    33| current_dir = os.path.dirname(os.path.abspath(__file__))
    34| repo_root = os.path.dirname(current_dir)
    35| if repo_root not in sys.path:
    36|     sys.path.insert(0, repo_root)
    37| 
    38| from core.hashing import make_hash, safe_str
    39| from core.canon import canon_str, S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
    40| from core.rows import (
    41|     first_param,
    42|     _as_string,
    43|     _as_value_string,
    44|     _as_double,
    45|     _as_int,
    46|     format_len_inches,
    47|     try_get_color_rgb_from_elem,
    48|     get_element_display_name,
    49|     _canon_rgb,
    50| )
    51| from core.record_v2 import (
    52|     canonicalize_str,
    53|     canonicalize_str_allow_empty,
    54|     canonicalize_int,
    55|     canonicalize_float,
    56|     canonicalize_bool,
    57|     canonicalize_enum,
    58|     ITEM_Q_OK,
    59|     ITEM_Q_MISSING,
    60|     ITEM_Q_UNREADABLE,
    61|     ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,
    62|     make_identity_item,
    63| )
    64| 
    65| 
    66| # ---------------------------------------------------------------------------
    67| # Shape Detection Constants
    68| # ---------------------------------------------------------------------------
    69| 
    70| # Canonical shape names (normalized from DimensionStyleType enum)
    71| SHAPE_LINEAR = "Linear"
    72| SHAPE_ANGULAR = "Angular"
    73| SHAPE_RADIAL = "Radial"
    74| SHAPE_DIAMETER = "Diameter"
    75| SHAPE_ARC_LENGTH = "ArcLength"
    76| SHAPE_SPOT_ELEVATION = "SpotElevation"
    77| SHAPE_SPOT_COORDINATE = "SpotCoordinate"
    78| SHAPE_SPOT_SLOPE = "SpotSlope"
    79| SHAPE_LINEAR_FIXED = "LinearFixed"
    80| SHAPE_SPOT_ELEVATION_FIXED = "SpotElevationFixed"
    81| SHAPE_DIAMETER_LINKED = "DiameterLinked"
    82| SHAPE_ALIGNMENT_STATION_LABEL = "AlignmentStationLabel"
    83| SHAPE_UNKNOWN = "Unknown"
    84| 
    85| # Shape family constants for property gating
    86| FAMILY_LINEAR = "linear"
    87| FAMILY_RADIAL = "radial"
    88| FAMILY_ANGULAR = "angular"
    89| FAMILY_SPOT = "spot"
    90| FAMILY_UNKNOWN = "unknown"
    91| 
    92| # Map canonical shape names to shape families
    93| SHAPE_TO_FAMILY = {
    94|     SHAPE_LINEAR: FAMILY_LINEAR,
    95|     SHAPE_LINEAR_FIXED: FAMILY_LINEAR,
    96|     SHAPE_RADIAL: FAMILY_RADIAL,
    97|     SHAPE_DIAMETER: FAMILY_RADIAL,
    98|     SHAPE_DIAMETER_LINKED: FAMILY_RADIAL,
    99|     SHAPE_ANGULAR: FAMILY_ANGULAR,
   100|     SHAPE_ARC_LENGTH: FAMILY_ANGULAR,
   101|     SHAPE_SPOT_ELEVATION: FAMILY_SPOT,
   102|     SHAPE_SPOT_COORDINATE: FAMILY_SPOT,
   103|     SHAPE_SPOT_SLOPE: FAMILY_SPOT,
   104|     SHAPE_SPOT_ELEVATION_FIXED: FAMILY_SPOT,
   105|     SHAPE_ALIGNMENT_STATION_LABEL: FAMILY_SPOT,
   106|     SHAPE_UNKNOWN: FAMILY_UNKNOWN,
   107| }
   108| 
   109| # Map integer enum values to canonical shape names (fallback for non-enum access)
   110| SHAPE_INT_TO_NAME = {
   111|     0: SHAPE_LINEAR,
   112|     1: SHAPE_ANGULAR,
   113|     2: SHAPE_RADIAL,
   114|     3: SHAPE_DIAMETER,
   115|     4: SHAPE_ARC_LENGTH,
   116|     5: SHAPE_SPOT_ELEVATION,
   117|     6: SHAPE_SPOT_COORDINATE,
   118|     7: SHAPE_SPOT_SLOPE,
   119|     8: SHAPE_LINEAR_FIXED,
   120|     9: SHAPE_SPOT_ELEVATION_FIXED,
   121|     10: SHAPE_DIAMETER_LINKED,
   122|     11: SHAPE_ALIGNMENT_STATION_LABEL,
   123| }
   124| 
   125| 
   126| # ---------------------------------------------------------------------------
   127| # Shape Detection Helper
   128| # ---------------------------------------------------------------------------
   129| 
   130| def _get_dimension_shape(dim_type):
   131|     """
   132|     Detect dimension shape from a Revit DimensionType object.
   133| 
   134|     Revit exposes shape via multiple API paths depending on version:
   135|       - DimensionType.StyleType (preferred, returns DimensionStyleType enum)
   136|       - DimensionType.Shape (some versions)
   137|       - DimensionType.DimensionShape (legacy)
   138|       - DimensionType.DimensionStyleType (redundant accessor)
   139| 
   140|     Returns:
   141|         tuple: (shape_name, shape_family, quality)
   142|             - shape_name: str - Canonical shape name (e.g., "Linear", "Radial")
   143|             - shape_family: str - Shape family for property gating (e.g., "linear", "radial")
   144|             - quality: str - ITEM_Q_OK, ITEM_Q_MISSING, or ITEM_Q_UNREADABLE
   145| 
   146|     Fail-soft behavior:
   147|         - If shape cannot be read, returns (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)
   148|         - If shape is None/empty, returns (None, FAMILY_UNKNOWN, ITEM_Q_MISSING)
   149|         - Unknown enum values return (str(value), FAMILY_UNKNOWN, ITEM_Q_OK)
   150|     """
   151|     if dim_type is None:
   152|         return (None, FAMILY_UNKNOWN, ITEM_Q_MISSING)
   153| 
   154|     # Try multiple API paths in order of preference
   155|     shape_raw = None
   156|     read_exception = None
   157| 
   158|     for attr_name in ("StyleType", "Shape", "DimensionShape", "DimensionStyleType"):
   159|         try:
   160|             if hasattr(dim_type, attr_name):
   161|                 val = getattr(dim_type, attr_name, None)
   162|                 if val is not None:
   163|                     shape_raw = val
   164|                     break
   165|         except Exception as ex:
   166|             if read_exception is None:
   167|                 read_exception = ex
   168|             continue
   169| 
   170|     # Handle missing shape
   171|     if shape_raw is None:
   172|         if read_exception is not None:
   173|             return (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)
   174|         return (None, FAMILY_UNKNOWN, ITEM_Q_MISSING)
   175| 
   176|     # Extract canonical shape name from enum or value
   177|     shape_name = None
   178| 
   179|     # Try 1: Enum with .name attribute (preferred - gives string like "Linear")
   180|     try:
   181|         enum_name = getattr(shape_raw, "name", None)
   182|         if isinstance(enum_name, str) and enum_name.strip():
   183|             shape_name = enum_name.strip()
   184|     except Exception:
   185|         pass
   186| 
   187|     # Try 2: Enum with .Name attribute (some .NET enums use PascalCase)
   188|     if shape_name is None:
   189|         try:
   190|             enum_name = getattr(shape_raw, "Name", None)
   191|             if isinstance(enum_name, str) and enum_name.strip():
   192|                 shape_name = enum_name.strip()
   193|         except Exception:
   194|             pass
   195| 
   196|     # Try 3: Integer value lookup
   197|     if shape_name is None:
   198|         try:
   199|             int_val = None
   200|             for int_attr in ("value", "Value", "value__", "__int__"):
   201|                 try:
   202|                     if int_attr == "__int__":
   203|                         int_val = int(shape_raw)
   204|                     elif hasattr(shape_raw, int_attr):
   205|                         int_val = getattr(shape_raw, int_attr)
   206|                         if callable(int_val):
   207|                             int_val = int_val()
   208|                     if int_val is not None:
   209|                         break
   210|                 except Exception:
   211|                     continue
   212| 
   213|             if int_val is not None and int_val in SHAPE_INT_TO_NAME:
   214|                 shape_name = SHAPE_INT_TO_NAME[int_val]
   215|         except Exception:
   216|             pass
   217| 
   218|     # Try 4: String conversion fallback
   219|     if shape_name is None:
   220|         try:
   221|             str_val = str(shape_raw).strip()
   222|             if str_val:
   223|                 for known_name in SHAPE_TO_FAMILY.keys():
   224|                     if str_val.lower() == known_name.lower():
   225|                         shape_name = known_name
   226|                         break
   227|                 if shape_name is None:
   228|                     shape_name = str_val
   229|         except Exception:
   230|             pass
   231| 
   232|     # Explicit handling for AlignmentStationLabel (spot-like)
   233|     try:
   234|         _sn = safe_str(shape_name).lower().replace(" ", "")
   235|         if _sn == "alignmentstationlabel":
   236|             return (SHAPE_ALIGNMENT_STATION_LABEL, FAMILY_SPOT, ITEM_Q_OK)
   237|     except Exception:
   238|         pass
   239| 
   240|     # Final fallback
   241|     if shape_name is None:
   242|         return (None, FAMILY_UNKNOWN, ITEM_Q_UNREADABLE)
   243| 
   244|     # Determine shape family
   245|     shape_family = SHAPE_TO_FAMILY.get(shape_name, FAMILY_UNKNOWN)
   246| 
   247|     return (shape_name, shape_family, ITEM_Q_OK)
   248| 
   249| 
   250| # ---------------------------------------------------------------------------
   251| # Unit conversion helpers
   252| # ---------------------------------------------------------------------------
   253| 
   254| def _fmt_in_from_ft(ft, places=6):
   255|     """Convert feet to inches and format as string with given decimal places."""
   256|     if ft is None:
   257|         return None
   258|     try:
   259|         inches = float(ft) * 12.0
   260|         return format(inches, ".{}f".format(int(places)))
   261|     except Exception:
   262|         return None
   263| 
   264| 
   265| def _fmt_float(x, places=12):
   266|     """Format a float with given significant digits."""
   267|     if x is None:
   268|         return None
   269|     try:
   270|         return format(float(x), ".{}g".format(int(places)))
   271|     except Exception:
   272|         return None
   273| 
   274| 
   275| # ---------------------------------------------------------------------------
   276| # FormatOptions serialization
   277| # ---------------------------------------------------------------------------
   278| 
   279| def _format_options_to_kv(fo):
   280|     """
   281|     Serialize Autodesk.Revit.DB.FormatOptions to a stable, hashable dict.
   282|     Only include semantically relevant fields; stringify enums.
   283|     """
   284|     if fo is None:
   285|         return None
   286| 
   287|     out = {}
   288|     try:
   289|         out["use_default"] = bool(getattr(fo, "UseDefault", False))
   290|     except Exception:
   291|         out["use_default"] = False
   292| 
   293|     if out["use_default"]:
   294|         return out
   295| 
   296|     keys = [
   297|         "Accuracy",
   298|         "RoundingMethod",
   299|         "UseDigitGrouping",
   300|         "SuppressLeadingZeros",
   301|         "SuppressTrailingZeros",
   302|         "SuppressSpaces",
   303|         "SuppressZeroFeet",
   304|         "SuppressZeroInches",
   305|         "UsePlusPrefix",
   306|     ]
   307| 
   308|     for k in keys:
   309|         try:
   310|             if not hasattr(fo, k):
   311|                 continue
   312|             v = getattr(fo, k)
   313|             if k == "Accuracy":
   314|                 out["accuracy_in"] = _fmt_in_from_ft(v)
   315|             else:
   316|                 out[k.lower()] = safe_str(v)
   317|         except Exception:
   318|             continue
   319| 
   320|     return out
   321| 
   322| 
   323| # ---------------------------------------------------------------------------
   324| # Display name helper
   325| # ---------------------------------------------------------------------------
   326| 
   327| def get_type_display_name(elem_type):
   328|     """
   329|     Deterministic, defensive type name extraction for DimensionType.
   330| 
   331|     IMPORTANT:
   332|     - DimensionType.Name may throw TypeError and MUST NOT be relied on.
   333|     - UI-visible names are exposed via parameters:
   334|         SYMBOL_FAMILY_NAME_PARAM (-1002002)
   335|         SYMBOL_NAME_PARAM        (-1002001)
   336| 
   337|     Preference order:
   338|       1) Family Name + ":" + Type Name   (matches Revit UI grouping)
   339|       2) Type Name
   340|       3) Family Name
   341|       4) id:ElementId
   342|     """
   343|     if elem_type is None:
   344|         return S_MISSING
   345| 
   346|     fam = None
   347|     typ = None
   348| 
   349|     # Family Name
   350|     try:
   351|         p_fam = first_param(
   352|             elem_type,
   353|             bip_names=["SYMBOL_FAMILY_NAME_PARAM"],
   354|             ui_names=["Family Name"],
   355|         )
   356|         fam = canon_str(_as_string(p_fam))
   357|         if fam in (S_MISSING, S_UNREADABLE, "", None):
   358|             fam = None
   359|     except Exception:
   360|         fam = None
   361| 
   362|     # Type Name
   363|     try:
   364|         p_typ = first_param(
   365|             elem_type,
   366|             bip_names=["SYMBOL_NAME_PARAM", "ALL_MODEL_TYPE_NAME"],
   367|             ui_names=["Type Name", "Name"],
   368|         )
   369|         typ = canon_str(_as_string(p_typ))
   370|         if typ in (S_MISSING, S_UNREADABLE, "", None):
   371|             typ = None
   372|     except Exception:
   373|         typ = None
   374| 
   375|     if fam and typ:
   376|         return "{}:{}".format(fam, typ)
   377|     if typ:
   378|         return typ
   379|     if fam:
   380|         try:
   381|             eid = getattr(elem_type, "Id", None)
   382|             if eid is not None:
   383|                 return "{}:id:{}".format(fam, safe_str(getattr(eid, "IntegerValue", eid)))
   384|         except Exception:
   385|             pass
   386|         return fam
   387| 
   388|     try:
   389|         eid = getattr(elem_type, "Id", None)
   390|         if eid is not None:
   391|             return "id:{}".format(str(eid))
   392|     except Exception:
   393|         pass
   394| 
   395|     return S_MISSING
   396| 
   397| 
   398| # ---------------------------------------------------------------------------
   399| # Text/Appearance Identity Items Builder
   400| # ---------------------------------------------------------------------------
   401| 
```
