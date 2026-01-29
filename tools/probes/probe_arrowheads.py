# Dynamo Python (Revit) — Arrowheads domain probe
# Outputs:
#   OUT[0] = arrowheads_table (list[dict])
#   OUT[1] = dimtypes_tickmark_map (list[dict])  # optional; see IN[1]
#
# Inputs (optional):
#   IN[0] = sample_dimtypes_n (int) default 25
#   IN[1] = do_dimtype_mapping (bool) default True

import clr
clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, BuiltInCategory, ElementId,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils
)

doc = DocumentManager.Instance.CurrentDBDocument

sample_dimtypes_n = IN[0] if len(IN) > 0 and IN[0] is not None else 25
do_dimtype_mapping = IN[1] if len(IN) > 1 and IN[1] is not None else True


# -------------------------
# Helpers
# -------------------------

def _safe_lookup_param(elem, name):
    try:
        return elem.LookupParameter(name)
    except:
        return None

def _safe_elem_name(elem):
    try:
        return elem.Name
    except:
        return None

from Autodesk.Revit.DB import BuiltInParameter

def _safe_type_name(elem):
    # Prefer built-in type name parameters over elem.Name (which can be unreadable)
    for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
        try:
            p = elem.get_Parameter(bip)
            if p is not None:
                s = p.AsString()
                if s:
                    return s
        except:
            pass
    # fallback
    return _safe_elem_name(elem)

def _format_param_value(p):
    """
    Returns a dict: { "q": <quality>, "storage": <StorageType>, "raw": <raw>, "norm": <normalized>, "display": <formatted> }
    """
    if p is None:
        return {"q": "missing", "storage": None, "raw": None, "norm": None, "display": None}

    st = p.StorageType
    try:
        if st == StorageType.String:
            s = p.AsString()
            return {"q": "ok", "storage": "String", "raw": s, "norm": s, "display": s}

        if st == StorageType.Integer:
            i = p.AsInteger()

            display = None
            try:
                display = p.AsValueString()
            except:
                display = None
            if display is None:
                display = str(i)

            b = None
            try:
                if i in (0, 1):
                    b = bool(i)
            except:
                b = None

            return {"q": "ok", "storage": "Integer", "raw": i, "norm": b if b is not None else i, "display": display}

        if st == StorageType.Double:
            d = p.AsDouble()
            # Try best-effort formatting using project units
            # (works well for length/angle-type params; if it fails we still return raw).
            display = None
            try:
                display = UnitFormatUtils.Format(doc.GetUnits(), p.Definition.GetDataType(), d, False)
            except:
                display = str(d)
            return {"q": "ok", "storage": "Double", "raw": d, "norm": d, "display": display}

        if st == StorageType.ElementId:
            eid = p.AsElementId()
            if eid is None or eid == ElementId.InvalidElementId:
                return {"q": "ok.null", "storage": "ElementId", "raw": None, "norm": None, "display": None}
            ref = doc.GetElement(eid)
            disp = None
            try:
                disp = ref.Name if ref is not None else str(eid.IntegerValue)
            except:
                disp = str(eid.IntegerValue)
            return {"q": "ok", "storage": "ElementId", "raw": eid.IntegerValue, "norm": eid.IntegerValue, "display": disp}

        # Other / None
        return {"q": "unsupported.storage_type", "storage": str(st), "raw": None, "norm": None, "display": None}

    except Exception as ex:
        return {"q": "unreadable", "storage": str(st), "raw": None, "norm": None, "display": "EX: {}".format(ex)}

def _deg_from_double_internal(val):
    # Revit angles are typically stored in radians; this is a best-effort normalization.
    # If the project param is actually degrees internally, this will look wrong — check the output.
    try:
        return UnitUtils.ConvertFromInternalUnits(val, UnitTypeId.Degrees)
    except:
        # fallback: assume already degrees
        return val

def _feet_to_fractional_in(val_feet):
    # best-effort: return internal feet and also inches as float
    try:
        inches = UnitUtils.ConvertFromInternalUnits(val_feet, UnitTypeId.Inches)
        return inches
    except:
        return None


# -------------------------
# Probe A: Arrowheads inventory + parameter dump
# -------------------------

# Parameter names taken directly from your screenshot
ARROWHEAD_PARAM_NAMES = [
    ("Arrow Style", "arrowhead.style"),
    ("Fill Tick", "arrowhead.fill_tick"),
    ("Arrow Closed", "arrowhead.arrow_closed"),
    ("Arrow Width Angle", "arrowhead.width_angle"),
    ("Tick Size", "arrowhead.tick_size"),
    ("Heavy End Pen Weight", "arrowhead.heavy_end_pen_weight"),
    ("Tick Mark Centered", "arrowhead.tick_mark_centered"),
]

from Autodesk.Revit.DB import ElementType

def collect_arrowhead_types():
    """
    Category-free discovery:
    - Scan all ElementTypes
    - Keep those that expose Arrowhead-defining parameters (per your Type Properties dialog)
    """
    # These parameter names are the hard evidence from your screenshot
    required_params = ["Arrow Style", "Tick Size"]  # minimal discriminators
    optional_params = ["Fill Tick", "Arrow Closed", "Arrow Width Angle", "Heavy End Pen Weight", "Tick Mark Centered"]

    types = (FilteredElementCollector(doc)
             .WhereElementIsElementType()
             .OfClass(ElementType)
             .ToElements())

    hits = []
    for t in types:
        try:
            # Require at least the minimal signature of an Arrowhead type
            ok = True
            for pn in required_params:
                if t.LookupParameter(pn) is None:
                    ok = False
                    break
            if not ok:
                continue

            # Extra guard: must have at least one optional param too (reduces false positives)
            opt_ok = False
            for pn in optional_params:
                if t.LookupParameter(pn) is not None:
                    opt_ok = True
                    break
            if not opt_ok:
                continue

            hits.append(t)
        except:
            # Ignore weird/unreadable element types
            continue

    return hits

arrowhead_types = collect_arrowhead_types()

arrowheads_table = []
arrowhead_by_typeid = {}  # ElementId(int) -> row dict (for mapping later)

for t in arrowhead_types:
    row = {}
    row["arrowhead.type_id"] = t.Id.IntegerValue
    row["arrowhead.name"] = _safe_type_name(t)

    # Pull all params
    for ui_name, key in ARROWHEAD_PARAM_NAMES:
        p = _safe_lookup_param(t, ui_name)
        pv = _format_param_value(p)

        # Add targeted normalizations for the two most important numeric fields
        if key == "arrowhead.width_angle" and pv["q"] == "ok" and pv["storage"] == "Double":
            # Provide degrees norm alongside raw
            row["arrowhead.width_angle_deg"] = _deg_from_double_internal(pv["raw"])
            row["arrowhead.width_angle_raw"] = pv["raw"]
            row["arrowhead.width_angle_display"] = pv["display"]
        elif key == "arrowhead.tick_size" and pv["q"] == "ok" and pv["storage"] == "Double":
            # Provide inches norm alongside raw
            row["arrowhead.tick_size_inches"] = _feet_to_fractional_in(pv["raw"])
            row["arrowhead.tick_size_raw"] = pv["raw"]
            row["arrowhead.tick_size_display"] = pv["display"]
        else:
            # generic
            row[key + ".q"] = pv["q"]
            row[key + ".storage"] = pv["storage"]
            row[key + ".raw"] = pv["raw"]
            row[key + ".display"] = pv["display"]

    arrowheads_table.append(row)
    arrowhead_by_typeid[t.Id.IntegerValue] = row


# -------------------------
# Probe B: DimensionType → Arrowhead mapping (best-effort)
# -------------------------

DIM_TICK_PARAM_CANDIDATES = [
    # try the most likely UI labels first
    "Tick Mark",
    "Tick mark",
    "Tick Mark Type",
    "Tick Mark Symbol",
]

def collect_dimension_types():
    """
    Category-free discovery of dimension styles:
    Keep ElementTypes that have one of the candidate tick-mark parameters.
    """
    from Autodesk.Revit.DB import ElementType

    candidates = set([n.strip().lower() for n in DIM_TICK_PARAM_CANDIDATES])

    types = (FilteredElementCollector(doc)
             .WhereElementIsElementType()
             .OfClass(ElementType)
             .ToElements())

    hits = []
    for t in types:
        try:
            # Some types throw when reading Parameters; try ordered first.
            try:
                params = list(t.GetOrderedParameters())
            except:
                try:
                    params = list(t.Parameters)
                except:
                    continue

            found = False
            for p in params:
                try:
                    dn = p.Definition.Name
                    if dn and dn.strip().lower() in candidates:
                        found = True
                        break
                except:
                    continue

            if found:
                hits.append(t)
        except:
            continue

    return hits

dimtypes_tickmark_map = []

if do_dimtype_mapping:
    dim_types = collect_dimension_types()
    # sample first N for speed / sanity
    dim_types = dim_types[:max(0, int(sample_dimtypes_n))]

    for dt in dim_types:
        out = {
            "dim_type.id": dt.Id.IntegerValue,
            "dim_type.name": _safe_type_name(dt),            "tick_param.matched_name": None,
            "tick_param.q": None,
            "tick_param.storage": None,
            "tick_param.raw": None,
            "tick_param.display": None,
            "arrowhead.resolved": False,
            "arrowhead.type_id": None,
            "arrowhead.name": None,
        }

        p = None
        matched = None
        for cand in DIM_TICK_PARAM_CANDIDATES:
            p = _safe_lookup_param(dt, cand)
            if p is not None:
                matched = cand
                break

        if p is None:
            out["tick_param.q"] = "missing"
            dimtypes_tickmark_map.append(out)
            continue

        out["tick_param.matched_name"] = matched
        pv = _format_param_value(p)
        out["tick_param.q"] = pv["q"]
        out["tick_param.storage"] = pv["storage"]
        out["tick_param.raw"] = pv["raw"]
        out["tick_param.display"] = pv["display"]

        # Attempt resolution if ElementId
        if pv["storage"] == "ElementId" and pv["raw"] is not None:
            ah_id = int(pv["raw"])
            out["arrowhead.type_id"] = ah_id
            if ah_id in arrowhead_by_typeid:
                out["arrowhead.resolved"] = True
                out["arrowhead.name"] = arrowhead_by_typeid[ah_id].get("arrowhead.name")
            else:
                # It might be a different element type; still try doc lookup for name
                ref = doc.GetElement(ElementId(ah_id))
                try:
                    out["arrowhead.name"] = ref.Name if ref is not None else None
                except:
                    out["arrowhead.name"] = None

        dimtypes_tickmark_map.append(out)


OUT = (arrowheads_table, dimtypes_tickmark_map)
