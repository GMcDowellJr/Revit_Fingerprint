# tools/probes/probe_line_styles.py
#
# Dynamo Python (Revit) — Breadth Probe: line_styles (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "line_styles",
#     "records": [...],
#     "file_written": "<path>|None",
#     "file_write_error": "<error>|None"
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "line_styles",
#     "records": [...]
#   }
# ]
#
# Inputs:
#   IN[0] max_styles_to_inspect (int)
#        Maximum number of line styles (GraphicsStyle) to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit LineStyle → LinePattern crosswalk.
#        Default: False
#
#   IN[2] per_bucket_limit (int)
#        Sample at most N styles per bucket (GraphicsStyleType + parent category).
#        Default: 50  (set large to effectively scan all)
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_line_styles_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.
#
#   IN[5] crosswalk_scan_limit (int)
#        Maximum number of line styles to *inspect* when building the crosswalk.
#        This controls how far the probe scans to discover distinct
#        LineStyle → LinePattern relationships.
#
#        This limit is:
#          - independent of inventory sampling limits
#          - applied before relationship deduplication
#
#        Default: 2000
#        Set to a large value to approach full-document breadth.
#
#   IN[6] crosswalk_emit_limit (int)
#        Maximum number of *distinct relationship records* emitted
#        in the LineStyle → LinePattern crosswalk.
#
#        Deduplication key:
#          (line_pattern.id, graphics_style_type)
#
#        This bounds output size while preserving relationship breadth.
#
#        Default: 200
#
# Notes:
#   - "line styles" are modeled as GraphicsStyle elements whose GraphicsStyleCategory
#     is a subcategory under the built-in Lines category (OST_Lines).
#   - Many meaningful attributes for a line style are *category properties* (color, pattern, weights),
#     not Revit Parameters. This probe captures both:
#       * real parameters on GraphicsStyle (p.<DefinitionName>)
#       * virtual properties from Category/GraphicsStyle (v.<...>) as parameter-like evidence
#
# Reference pattern: probe_arrowheads.py (authoritative structure & IO).  :contentReference[oaicite:0]{index=0}


import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    BuiltInCategory
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

try:
    from Autodesk.Revit.DB import GraphicsStyle, GraphicsStyleType, LinePatternElement
except:
    GraphicsStyle = None
    GraphicsStyleType = None
    LinePatternElement = None

doc = DocumentManager.Instance.CurrentDBDocument

max_styles_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_bucket_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 50
write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None


# -------------------------
# Helpers (defensive)
# -------------------------

def _safe(fn, default=None):
    try:
        return fn()
    except:
        return default

def _safe_param_def_name(p):
    try:
        d = p.Definition
        return d.Name if d is not None else None
    except:
        return None

def _safe_get_datatype(p):
    try:
        d = p.Definition
        if d is None:
            return None
        return d.GetDataType()
    except:
        return None

def _is_length_datatype(dt):
    if dt is None or SpecTypeId is None:
        return False
    try:
        return dt == SpecTypeId.Length
    except:
        return False

def _is_angle_datatype(dt):
    if dt is None or SpecTypeId is None:
        return False
    try:
        return dt == SpecTypeId.Angle
    except:
        return False

def _fmt_display_param(p, raw_double=None):
    try:
        if raw_double is not None:
            dt = _safe_get_datatype(p)
            if dt is not None:
                return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
            return str(raw_double)
        return p.AsValueString()
    except:
        return _safe(lambda: p.AsValueString(), None)

def _format_param_contract(p):
    """
    Contract:
      {
        "q": "ok|missing|unreadable|unsupported",
        "storage": "String|Integer|Double|ElementId|None",
        "raw": ...,
        "display": ...,
        "norm": ...
      }

    Probe choices:
      - Integer.norm stays int (enum-safe).
      - Length -> inches, Angle -> degrees (when datatype detected).
      - ElementId -> IntegerValue; attempt to resolve name cheaply.
    """
    if p is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}

    st = _safe(lambda: p.StorageType, None)
    if st is None:
        return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}

    if st == StorageType.String:
        raw = _safe(lambda: p.AsString(), None)
        return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}

    if st == StorageType.Integer:
        raw = _safe(lambda: p.AsInteger(), None)
        disp = _fmt_display_param(p, None)
        return {
            "q": "ok",
            "storage": "Integer",
            "raw": raw,
            "display": disp if disp is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    if st == StorageType.Double:
        raw = _safe(lambda: p.AsDouble(), None)
        disp = _fmt_display_param(p, raw)
        dt = _safe_get_datatype(p)
        if raw is None:
            norm = None
        elif _is_length_datatype(dt):
            norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
        elif _is_angle_datatype(dt):
            norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
        else:
            norm = raw
        return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}

    if st == StorageType.ElementId:
        eid = _safe(lambda: p.AsElementId(), None)
        if eid is None or eid == ElementId.InvalidElementId:
            return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}

        raw = _safe(lambda: eid.IntegerValue, None)
        ref = _safe(lambda: doc.GetElement(eid), None)
        ref_name = _safe(lambda: ref.Name, None) if ref is not None else None
        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _contract_value(q, storage, raw, display, norm):
    # small helper to treat non-Parameter properties as parameter-like evidence
    return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}

def _rgb_triplet(color):
    # Autodesk.Revit.DB.Color -> "R|G|B" string
    if color is None:
        return None
    r = _safe(lambda: int(color.Red), None)
    g = _safe(lambda: int(color.Green), None)
    b = _safe(lambda: int(color.Blue), None)
    if r is None or g is None or b is None:
        return None
    return "{}|{}|{}".format(r, g, b)

def _hex_rgb_from_triplet(rgb_triplet):
    if not rgb_triplet:
        return None
    try:
        parts = rgb_triplet.split("|")
        if len(parts) != 3:
            return None
        r = int(parts[0]); g = int(parts[1]); b = int(parts[2])
        return "#{:02X}{:02X}{:02X}".format(r & 0xFF, g & 0xFF, b & 0xFF)
    except:
        return None

def _get_lines_category_id():
    cat = _safe(lambda: doc.Settings.Categories.get_Item(BuiltInCategory.OST_Lines), None)
    return _safe(lambda: cat.Id.IntegerValue, None) if cat is not None else None

def _is_line_style_graphicsstyle(gs, lines_cat_id_int):
    # True if GraphicsStyleCategory is a subcategory of Lines category
    if gs is None or lines_cat_id_int is None:
        return False
    c = _safe(lambda: gs.GraphicsStyleCategory, None)
    if c is None:
        return False
    parent = _safe(lambda: c.Parent, None)
    if parent is None:
        return False
    pid = _safe(lambda: parent.Id.IntegerValue, None)
    return True if pid == lines_cat_id_int else False

def _bucket_key(gs):
    # sampling bucket: style type + parent category (usually "Lines")
    gst = _safe(lambda: gs.GraphicsStyleType, None)
    c = _safe(lambda: gs.GraphicsStyleCategory, None)
    parent = _safe(lambda: c.Parent, None) if c is not None else None
    return "{}|{}".format(str(gst), _safe(lambda: parent.Name, None))


# -------------------------
# Discovery + Sampling
# -------------------------

lines_cat_id_int = _get_lines_category_id()

all_gs = []
if GraphicsStyle is not None:
    all_gs = _safe(
        lambda: list(FilteredElementCollector(doc).OfClass(GraphicsStyle).ToElements()),
        default=[]
    )

# Filter to line styles (subcategory under Lines)
hits = []
for gs in all_gs:
    if _is_line_style_graphicsstyle(gs, lines_cat_id_int):
        hits.append(gs)

# Cap AFTER filtering
try:
    max_n = int(max_styles_to_inspect)
    if max_n >= 0:
        hits = hits[:max_n]
except:
    pass

# Sample per bucket (breadth bias)
selected = []
by_bucket = {}  # bucket_key -> count
for gs in hits:
    bk = _bucket_key(gs)
    c = by_bucket.get(bk, 0)

    if per_bucket_limit is None:
        ok = True
    else:
        try:
            ok = c < int(per_bucket_limit)
        except:
            ok = c < 50

    if ok:
        selected.append(gs)
        by_bucket[bk] = c + 1

# Ensure at least one per bucket if limits were too strict
if len(selected) == 0 and len(hits) > 0:
    seen = set()
    for gs in hits:
        bk = _bucket_key(gs)
        if bk not in seen:
            selected.append(gs)
            seen.add(bk)


# -------------------------
# Build inventory (union over selected)
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   observed_on_buckets: set(str)
# }
param_index = {}

def _maybe_set_example(entry, pv):
    # exactly one example: prefer first "ok", else first non-ok
    if pv is None:
        return
    ex = entry.get("example")
    if ex is None:
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }
        return
    if ex.get("q") != "ok" and pv.get("q") == "ok":
        entry["example"] = {
            "q": pv.get("q"),
            "storage": pv.get("storage"),
            "raw": pv.get("raw"),
            "display": pv.get("display"),
            "norm": pv.get("norm")
        }

def _index_param(pk, pv, bucket_key):
    if pk not in param_index:
        param_index[pk] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set()
        }

    e = param_index[pk]
    st = pv.get("storage")
    q = pv.get("q") or "unreadable"

    if st:
        e["storage_types"].add(st)
    if q not in e["q_counts"]:
        e["q_counts"][q] = 0
    e["q_counts"][q] += 1
    e["observed_on_buckets"].add(bucket_key)

    _maybe_set_example(e, pv)

def _virtual_surface(gs):
    """
    Produce virtual properties treated as parameter-like evidence.
    """
    out = {}

    c = _safe(lambda: gs.GraphicsStyleCategory, None)
    parent = _safe(lambda: c.Parent, None) if c is not None else None

    # Names / ids
    gs_id = _safe(lambda: gs.Id.IntegerValue, None)
    gs_name = _safe(lambda: gs.Name, None)
    cat_name = _safe(lambda: c.Name, None) if c is not None else None
    cat_id = _safe(lambda: c.Id.IntegerValue, None) if c is not None else None
    parent_name = _safe(lambda: parent.Name, None) if parent is not None else None
    parent_id = _safe(lambda: parent.Id.IntegerValue, None) if parent is not None else None

    gst = _safe(lambda: gs.GraphicsStyleType, None)

    out["v.gs.id"] = _contract_value("ok", "Integer", gs_id, str(gs_id) if gs_id is not None else None, gs_id)
    out["v.gs.name"] = _contract_value("ok", "String", gs_name, gs_name, gs_name)
    out["v.gs.type"] = _contract_value("ok", "String", str(gst), str(gst), str(gst))

    out["v.cat.id"] = _contract_value("ok", "Integer", cat_id, str(cat_id) if cat_id is not None else None, cat_id)
    out["v.cat.name"] = _contract_value("ok", "String", cat_name, cat_name, cat_name)

    out["v.parent_cat.id"] = _contract_value("ok", "Integer", parent_id, str(parent_id) if parent_id is not None else None, parent_id)
    out["v.parent_cat.name"] = _contract_value("ok", "String", parent_name, parent_name, parent_name)

    # Category properties: line color (R|G|B) + hex
    color = _safe(lambda: c.LineColor, None) if c is not None else None
    rgb = _rgb_triplet(color)
    rgb_hex = _hex_rgb_from_triplet(rgb)

    if rgb is None:
        out["v.line_color.rgb"] = _contract_value("missing", "String", None, None, None)
        out["v.line_color.hex"] = _contract_value("missing", "String", None, None, None)
    else:
        out["v.line_color.rgb"] = _contract_value("ok", "String", rgb, rgb, rgb)
        out["v.line_color.hex"] = _contract_value("ok", "String", rgb_hex, rgb_hex, rgb_hex)

    # Line pattern is an ElementId on Category
    pat_id = _safe(lambda: c.GetLinePatternId(GraphicsStyleType.Projection), None) if (c is not None and GraphicsStyleType is not None) else None
    if pat_id is None and c is not None:
        pat_id = _safe(lambda: c.LinePatternId, None)

    pat_int = _safe(lambda: pat_id.IntegerValue, None) if pat_id is not None else None
    pat_name = None
    if pat_id is not None and pat_id != ElementId.InvalidElementId:
        pe = _safe(lambda: doc.GetElement(pat_id), None)
        pat_name = _safe(lambda: pe.Name, None) if pe is not None else None

    if pat_int is None:
        out["v.line_pattern.id"] = _contract_value("missing", "ElementId", None, None, None)
        out["v.line_pattern.name"] = _contract_value("missing", "String", None, None, None)
    else:
        out["v.line_pattern.id"] = _contract_value("ok", "ElementId", pat_int, str(pat_int), pat_int)
        out["v.line_pattern.name"] = _contract_value("ok", "String", pat_name, pat_name if pat_name is not None else str(pat_int), pat_name)

    # Line weight: projection only (line styles do not have a "cut" weight surface)
    lw_proj = None
    if c is not None and GraphicsStyleType is not None:
        lw_proj = _safe(lambda: c.GetLineWeight(GraphicsStyleType.Projection), None)

    out["v.line_weight.projection"] = _contract_value(
        "ok" if lw_proj is not None else "missing",
        "Integer",
        lw_proj,
        str(lw_proj) if lw_proj is not None else None,
        lw_proj
    )

    return out

for gs in selected:
    bk = _bucket_key(gs)

    # 1) real parameters on GraphicsStyle (if any)
    params = _safe(lambda: list(gs.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(gs.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)
        pv = _format_param_contract(p)
        _index_param(pk, pv, bk)

    # 2) virtual properties (Category/GraphicsStyle surface)
    v = _virtual_surface(gs)
    for vk in v.keys():
        _index_param(vk, v[vk], bk)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "line_styles",
        "param_key": pk,
        "selected_style_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
        }
    })


# -------------------------
# Optional Crosswalk: LineStyle -> LinePattern
# -------------------------

optional_crosswalk = []

if enable_crosswalk:
    # Separate limits for crosswalk breadth (independent from inventory sampling)
    crosswalk_scan_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 2000
    crosswalk_emit_limit = IN[6] if len(IN) > 6 and IN[6] is not None else 200

    try:
        crosswalk_scan_limit = int(crosswalk_scan_limit)
    except:
        crosswalk_scan_limit = 2000

    try:
        crosswalk_emit_limit = int(crosswalk_emit_limit)
    except:
        crosswalk_emit_limit = 200

    # Relationship-breadth key: (line_pattern_id, graphics_style_type)
    seen_rel = set()

    scanned = 0
    for gs in hits:
        if crosswalk_scan_limit >= 0 and scanned >= crosswalk_scan_limit:
            break
        if crosswalk_emit_limit >= 0 and len(optional_crosswalk) >= crosswalk_emit_limit:
            break

        scanned += 1

        gst = _safe(lambda: gs.GraphicsStyleType, None)
        c = _safe(lambda: gs.GraphicsStyleCategory, None)
        if c is None:
            continue

        # pattern id on category
        pat_id = None
        if GraphicsStyleType is not None:
            pat_id = _safe(lambda: c.GetLinePatternId(GraphicsStyleType.Projection), None)
        if pat_id is None:
            pat_id = _safe(lambda: c.LinePatternId, None)

        pat_int = _safe(lambda: pat_id.IntegerValue, None) if pat_id is not None else None
        pat_name = None
        if pat_id is not None and pat_id != ElementId.InvalidElementId:
            pe = _safe(lambda: doc.GetElement(pat_id), None)
            pat_name = _safe(lambda: pe.Name, None) if pe is not None else None

        rel_key = "{}|{}".format(str(pat_int), str(gst))
        if rel_key in seen_rel:
            continue

        style_name = _safe(lambda: c.Name, None) or _safe(lambda: gs.Name, None)

        row = {
            "line_style.id": _safe(lambda: gs.Id.IntegerValue, None),
            "line_style.type": str(gst),
            "line_style.name": style_name,
            "line_pattern.resolved": True if (pat_int is not None and pat_name is not None) else False,
            "line_pattern.id": pat_int,
            "line_pattern.name": pat_name
        }

        seen_rel.add(rel_key)
        optional_crosswalk.append(row)


# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "inventory",
        "domain": "line_styles",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "line_styles",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON (valid JSON, stable order)
file_written = None
write_error = None

if write_json:
    try:
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None

        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            default_dir = _safe(lambda: os.path.dirname(rvt_path), None)

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_line_styles_{}.json".format(date_stamp)

        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(OUT_payload, f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload
