# Dynamo Python (Revit) — Breadth Probe: text_types (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "text_types",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "text_types",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_types_to_inspect (int)
#        Maximum number of candidate Text Types (ElementTypes) to inspect AFTER filtering.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit TextType -> Leader Arrowhead crosswalk (if present).
#        Default: False
#
#   IN[2] per_font_limit (int)
#        Sample at most N text types per Text Font value (breadth bias).
#        Default: 2
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_text_types_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.
#
# Notes:
#  - This probe is exploratory evidence capture for join-key / semantic policy design.
#  - Discovery is progressive:
#      (1) parameter-signature discovery across ElementType
#      (2) fallback: OfClass(TextNoteType) if signature yields nothing
#  - Inventory is deduped probe-locally for q_counts by (param_key, storage, norm).


import clr
import os
import json
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId, ElementType,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    BuiltInParameter
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

# Optional: class-based fallback
try:
    from Autodesk.Revit.DB import TextNoteType
except:
    TextNoteType = None

doc = DocumentManager.Instance.CurrentDBDocument

max_types_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_font_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 2
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

def _safe_type_name(elem):
    for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
        try:
            p = elem.get_Parameter(bip)
            if p is not None:
                s = p.AsString()
                if s:
                    return s
        except:
            pass
    try:
        return elem.Name
    except:
        return None

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

def _fmt_display(p, raw_double=None):
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
      - Integer.norm stays int (enum-safe)
      - Length -> inches (float) when datatype is Length
      - Angle  -> degrees (float) when datatype is Angle
      - ElementId -> IntegerValue (norm=int), display resolves name cheaply if possible
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
        disp = _fmt_display(p, None)
        return {
            "q": "ok",
            "storage": "Integer",
            "raw": raw,
            "display": disp if disp is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    if st == StorageType.Double:
        raw = _safe(lambda: p.AsDouble(), None)
        disp = _fmt_display(p, raw)
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
        ref_name = None
        ref = _safe(lambda: doc.GetElement(eid), None)
        if ref is not None:
            ref_name = _safe(lambda: ref.Name, None)
            if ref_name is None:
                ref_name = _safe(lambda: _safe_type_name(ref), None)

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _contract_value(q, storage, raw, display, norm):
    # helper to treat derived/virtual properties as parameter-like evidence
    return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}

def _rgb_triplet_from_int(color_int):
    """
    Best-effort parse for Revit integer color surfaces.
    Assumes a 24-bit packed RGB (0xRRGGBB). If Revit uses a different packing
    in your environment, this will show up immediately in evidence.
    """
    if color_int is None:
        return None
    try:
        n = int(color_int)
        # only trust lower 24 bits
        r = (n >> 16) & 0xFF
        g = (n >> 8) & 0xFF
        b = n & 0xFF
        return "{}|{}|{}".format(r, g, b)
    except:
        return None

def _hex32_from_int(n):
    if n is None:
        return None
    try:
        u = int(n) & 0xFFFFFFFF
        return "0x{:08X}".format(u)
    except:
        return None

def _rgb_rrggbb_from_int(n):
    if n is None:
        return None
    try:
        u = int(n) & 0xFFFFFFFF
        # assume low 24 bits are RRGGBB
        r = (u >> 16) & 0xFF
        g = (u >> 8) & 0xFF
        b = u & 0xFF
        return "{}|{}|{}".format(r, g, b)
    except:
        return None

def _rgb_bbgrr_from_int(n):
    if n is None:
        return None
    try:
        u = int(n) & 0xFFFFFFFF
        # assume low 24 bits are BBGGRR
        b = (u >> 16) & 0xFF
        g = (u >> 8) & 0xFF
        r = u & 0xFF
        return "{}|{}|{}".format(r, g, b)
    except:
        return None

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

def _slug(s):
    try:
        return "".join([c.lower() if c.isalnum() else "_" for c in str(s)]).strip("_")
    except:
        return "unknown"

def _looks_like_text_type(t):
    """
    Signature heuristic for TextNoteType / text styles:
      Required-ish params: Text Font, Text Size
      Helpful params (any): Text Width Scale, Background, Show Border, Keep Readable, Bold/Italic/Underline
    """
    required = ["Text Font", "Text Size"]
    optional = [
        "Text Width Scale",
        "Background",
        "Show Border",
        "Keep Readable",
        "Bold",
        "Italic",
        "Underline",
        "Leader Arrowhead",
        "Leader Arrowhead Type",
        "Leader Arrowhead Symbol"
    ]
    try:
        for pn in required:
            if t.LookupParameter(pn) is None:
                return False
        for pn in optional:
            if t.LookupParameter(pn) is not None:
                return True
        # If it has the required set but none of the optional set, still accept
        # (some templates expose fewer toggles)
        return True
    except:
        return False

def _text_font_key(t):
    p = _safe(lambda: t.LookupParameter("Text Font"), None)
    if p is None:
        return ("missing", None)
    pv = _format_param_contract(p)
    raw = pv.get("raw")
    disp = pv.get("display")
    return ("{}|{}".format(raw, disp), pv)


# -------------------------
# Discovery + Sampling
# -------------------------

hits = []

# Step 1 (preferred): class-based collector for Text Types
# This avoids signature bleed-through from unrelated ElementTypes (e.g., stairs).
if TextNoteType is not None:
    hits = _safe(
        lambda: list(
            FilteredElementCollector(doc)
            .WhereElementIsElementType()
            .OfClass(TextNoteType)
            .ToElements()
        ),
        default=[]
    )

# Step 2 (fallback): parameter-signature discovery across ElementType
# Only used if TextNoteType is unavailable or yields no results.
if len(hits) == 0:
    all_types = _safe(
        lambda: (FilteredElementCollector(doc)
                 .WhereElementIsElementType()
                 .OfClass(ElementType)
                 .ToElements()),
        default=[]
    )

    try:
        all_types = list(all_types)
    except:
        all_types = list(all_types)

    for t in all_types:
        if _looks_like_text_type(t):
            hits.append(t)

# Cap AFTER filtering / collection
try:
    max_n = int(max_types_to_inspect)
    if max_n >= 0:
        hits = hits[:max_n]
except:
    pass

# Sample first N per Text Font (breadth bias)
selected = []
by_font = {}  # font_key -> count
for t in hits:
    fk, _ = _text_font_key(t)
    c = by_font.get(fk, 0)
    if per_font_limit is None:
        per_font_ok = True
    else:
        try:
            per_font_ok = c < int(per_font_limit)
        except:
            per_font_ok = c < 2
    if per_font_ok:
        selected.append(t)
        by_font[fk] = c + 1

# Fallback: ensure at least 1 per font if per_font_limit <= 0
if len(selected) == 0 and len(hits) > 0:
    seen = set()
    for t in hits:
        fk, _ = _text_font_key(t)
        if fk not in seen:
            selected.append(t)
            seen.add(fk)


# -------------------------
# Build inventory (union over selected)
# Dedup for q_counts by (param_key, storage, norm)
# -------------------------

# param_key -> {
#   storage_types: set(str),
#   q_counts: dict,
#   example: dict or None,
#   observed_on_font_keys: set(str),
#   seen_sigs: set(tuple(storage, norm, q))
# }
param_index = {}

def _maybe_set_example(entry, pv):
    # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
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

for t in selected:
    font_key, _ = _text_font_key(t)

    params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(t.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)

        pv = _format_param_contract(p)

        # Derived color evidence (rgb/hex) for integer color-like parameters
        # e.g. "Text Color" often surfaces as Integer; we preserve raw int AND add rgb/hex.
        try:
            dn_l = dn.lower()
        except:
            dn_l = ""

        if ("color" in dn_l) and (pv.get("storage") == "Integer"):
            raw_int = pv.get("raw")

            raw_hex32 = _hex32_from_int(raw_int)

            rgb_rrggbb = _rgb_rrggbb_from_int(raw_int)
            hex_rrggbb = _hex_rgb_from_triplet(rgb_rrggbb) if rgb_rrggbb else None

            rgb_bbgrr = _rgb_bbgrr_from_int(raw_int)
            hex_bbgrr = _hex_rgb_from_triplet(rgb_bbgrr) if rgb_bbgrr else None

            base = "v.color.{}".format(_slug(dn))

            derived = [
                ("{}.raw_hex32".format(base),
                 _contract_value("ok" if raw_hex32 else "missing", "String", raw_hex32, raw_hex32, raw_hex32)),
                ("{}.rgb_rrggbb".format(base),
                 _contract_value("ok" if rgb_rrggbb else "missing", "String", rgb_rrggbb, rgb_rrggbb, rgb_rrggbb)),
                ("{}.hex_rrggbb".format(base),
                 _contract_value("ok" if hex_rrggbb else "missing", "String", hex_rrggbb, hex_rrggbb, hex_rrggbb)),
                ("{}.rgb_bbgrr".format(base),
                 _contract_value("ok" if rgb_bbgrr else "missing", "String", rgb_bbgrr, rgb_bbgrr, rgb_bbgrr)),
                ("{}.hex_bbgrr".format(base),
                 _contract_value("ok" if hex_bbgrr else "missing", "String", hex_bbgrr, hex_bbgrr, hex_bbgrr)),
            ]

            for _pk, _pv in derived:
                if _pk not in param_index:
                    param_index[_pk] = {
                        "storage_types": set(),
                        "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
                        "example": None,
                        "observed_on_font_keys": set(),
                        "seen_sigs": set()
                    }

                _entry = param_index[_pk]
                _st = _pv.get("storage")
                _q = _pv.get("q") or "unreadable"
                _norm = _pv.get("norm")
                _sig = (str(_st), str(_norm), str(_q))

                if _sig not in _entry["seen_sigs"]:
                    _entry["seen_sigs"].add(_sig)
                    if _st:
                        _entry["storage_types"].add(_st)
                    if _q not in _entry["q_counts"]:
                        _entry["q_counts"][_q] = 0
                    _entry["q_counts"][_q] += 1
                    _entry["observed_on_font_keys"].add(font_key)
                    _maybe_set_example(_entry, _pv)
                else:
                    _entry["observed_on_font_keys"].add(font_key)

        if pk not in param_index:
            param_index[pk] = {
                "storage_types": set(),
                "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
                "example": None,
                "observed_on_font_keys": set(),
                "seen_sigs": set()
            }

        entry = param_index[pk]

        st = pv.get("storage")
        q = pv.get("q") or "unreadable"
        norm = pv.get("norm")

        # Dedup signature (probe-local)
        sig = (str(st), str(norm), str(q))
        if sig in entry["seen_sigs"]:
            # still mark breadth (where seen)
            entry["observed_on_font_keys"].add(font_key)
            continue

        entry["seen_sigs"].add(sig)

        if st:
            entry["storage_types"].add(st)
        if q not in entry["q_counts"]:
            entry["q_counts"][q] = 0
        entry["q_counts"][q] += 1

        entry["observed_on_font_keys"].add(font_key)
        _maybe_set_example(entry, pv)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "text_types",
        "param_key": pk,
        "selected_type_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_fonts": sorted(list(e["observed_on_font_keys"]))[:25]
        }
    })


# -------------------------
# Optional Crosswalk: TextType -> Leader Arrowhead
# -------------------------

optional_crosswalk = []

LEADER_ARROW_PARAM_CANDIDATES = [
    "Leader Arrowhead",
    "Leader Arrowhead Type",
    "Leader Arrowhead Symbol",
    "Leader Arrow Head",   # odd variants
]

def _find_leader_arrow_param(t):
    for cand in LEADER_ARROW_PARAM_CANDIDATES:
        p = _safe(lambda: t.LookupParameter(cand), None)
        if p is not None:
            return cand, p
    return None, None

if enable_crosswalk:
    # Optional extra input: max crosswalk rows to emit (default 25)
    crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 25

    seen_arrowhead_ids = set()

    for tt in selected:
        if len(optional_crosswalk) >= int(crosswalk_limit):
            break

        matched, p = _find_leader_arrow_param(tt)
        if p is None:
            continue

        pv = _format_param_contract(p)

        # Must be ElementId pointing to an Arrowhead type
        if pv.get("storage") != "ElementId" or pv.get("raw") is None:
            continue

        ah_id = int(pv.get("raw"))
        if ah_id in seen_arrowhead_ids:
            continue

        ah_name = None
        ref = _safe(lambda: doc.GetElement(ElementId(ah_id)), None)
        if ref is not None:
            ah_name = _safe(lambda: ref.Name, None)
            if ah_name is None:
                ah_name = _safe(lambda: _safe_type_name(ref), None)

        row = {
            "text_type.id": _safe(lambda: tt.Id.IntegerValue, None),
            "text_type.name": _safe(lambda: _safe_type_name(tt), None),
            "leader_arrow_param.matched_name": matched,
            "leader_arrow_param": pv,
            "arrowhead.resolved": True if ah_name is not None else False,
            "arrowhead.type_id": ah_id,
            "arrowhead.name": ah_name
        }

        if not row["arrowhead.resolved"]:
            continue

        seen_arrowhead_ids.add(ah_id)
        optional_crosswalk.append(row)


# -------------------------
# Assemble labeled output payload
# -------------------------

OUT_payload = [
    {
        "kind": "inventory",
        "domain": "text_types",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "text_types",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
file_written = None
write_error = None

if write_json:
    try:
        rvt_path = _safe(lambda: doc.PathName, None)
        default_dir = None

        if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
            try:
                default_dir = os.path.dirname(rvt_path)
            except:
                default_dir = None

        if not default_dir:
            default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()

        date_stamp = datetime.now().strftime("%Y-%m-%d")
        fixed_name = "probe_text_types_{}.json".format(date_stamp)

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
