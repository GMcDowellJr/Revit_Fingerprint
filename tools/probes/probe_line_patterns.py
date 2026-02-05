# Dynamo Python (Revit) — Breadth Probe: line_patterns (INVENTORY OUTPUT)
#
# OUT = [
#   {
#     "kind": "inventory",
#     "domain": "line_patterns",
#     "records": param_inventory,
#     "file_written": "<path>|None",        # present only if write_json=True
#     "file_write_error": "<error>|None"    # present only on failure
#   },
#   {
#     "kind": "crosswalk",
#     "domain": "line_patterns",
#     "records": optional_crosswalk
#   }
# ]
#
# Inputs:
#   IN[0] max_patterns_to_inspect (int)
#        Maximum number of LinePatternElements to inspect.
#        Default: 500
#
#   IN[1] enable_crosswalk (bool)
#        Whether to emit LineStyle → LinePattern crosswalk.
#        Default: False
#
#   IN[2] per_segment_count_limit (int)
#        Sample at most N patterns per segment_count bucket (breadth bias).
#        Default: 5
#
#   IN[3] write_json (bool)
#        When True, serialize OUT to a valid JSON file on disk.
#        Default: False
#
#   IN[4] output_directory (str)
#        Directory path where JSON will be written.
#        Filename is fixed as: probe_line_patterns_YYYY-MM-DD.json
#        If None, falls back to RVT directory, then TEMP.

import clr
import os
import json
import hashlib
from datetime import datetime

clr.AddReference("RevitServices")
from RevitServices.Persistence import DocumentManager

clr.AddReference("RevitAPI")
from Autodesk.Revit.DB import (
    FilteredElementCollector, ElementId,
    StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    BuiltInCategory, GraphicsStyleType,
    LinePatternElement
)

try:
    from Autodesk.Revit.DB import SpecTypeId
except:
    SpecTypeId = None

doc = DocumentManager.Instance.CurrentDBDocument

max_patterns_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 2000
enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
per_segment_count_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 10
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

def _safe_elem_name(elem):
    try:
        n = elem.Name
        return n if n else None
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

    Probe choice:
      - Integer.norm stays integer (enum-safe).
      - Length -> inches (float) when datatype is Length.
      - Angle  -> degrees (float) when datatype is Angle.
      - ElementId -> IntegerValue (norm=int), display tries to resolve name cheaply.
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

        return {
            "q": "ok",
            "storage": "ElementId",
            "raw": raw,
            "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
            "norm": raw
        }

    return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}

def _contract_from_raw(q, storage, raw, display, norm):
    return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}

def _to_inches(val_internal):
    if val_internal is None:
        return None
    return _safe(lambda: UnitUtils.ConvertFromInternalUnits(val_internal, UnitTypeId.Inches), val_internal)

# Canonical mapping observed in Dynamo output / extractor:
# 0 = Dash, 1 = Space, 2 = Dot
_LP_SEG_TYPE_NAME = {0: "Dash", 1: "Space", 2: "Dot"}

def _lp_seg_type_id_and_name(seg):
    """
    Robustly read a line pattern segment type across API surfaces.

    Preferred property in many Dynamo/Revit contexts: LinePatternSegment.Type
    Fallback: SegmentType

    Returns: (type_id:int|None, type_name:str|None)
    """
    st = None
    try:
        if hasattr(seg, "Type"):
            st = getattr(seg, "Type", None)
    except Exception:
        st = None

    if st is None:
        try:
            if hasattr(seg, "SegmentType"):
                st = getattr(seg, "SegmentType", None)
        except Exception:
            st = None

    if st is None:
        return None, None

    try:
        st_id = int(st)
    except Exception:
        return None, None

    return st_id, _LP_SEG_TYPE_NAME.get(st_id, "Unknown")

def _linepattern_signature(lp):
    """
    Build a stable (probe-local) signature for a LinePattern:
      - segment sequence (type_id, length_in) order-sensitive
      - md5 of that sequence string

    meta:
      - access: ok | lp_none | segments_none | segments_throw
      - bucket: "<seg_count>|solid=<bool>" OR "lp=None" OR "segments=None" OR "segments:throw"
      - seq: list[str] | None
    """
    if lp is None:
        return (None, None, None, {"access": "lp_none", "bucket": "lp=None", "seq": None})

    # Read segments across API surfaces
    segs = None
    try:
        if hasattr(lp, "GetSegments"):
            segs = list(lp.GetSegments() or [])
        else:
            segs = list(getattr(lp, "Segments", None) or [])
    except Exception:
        return (None, None, None, {"access": "segments_throw", "bucket": "segments:throw", "seq": None})

    if segs is None:
        return (None, None, None, {"access": "segments_none", "bucket": "segments=None", "seq": None})

    seq = []
    for idx, s in enumerate(segs):
        st_id, _st_name = _lp_seg_type_id_and_name(s)

        # length
        try:
            slen = getattr(s, "Length", None)
        except Exception:
            slen = None

        # Normalize Dot length to 0.0 for stability (matches production extractor)
        if st_id == 2:
            slen = 0.0

        slen_in = _to_inches(slen) if slen is not None else None

        # token (fixed precision for hashing)
        if st_id is None:
            kind_tok = "None"
        else:
            kind_tok = str(int(st_id))

        if slen_in is None:
            tok = "seg[{:03d}].kind={};len=None".format(idx, kind_tok)
        else:
            tok = "seg[{:03d}].kind={};len={:.6f}".format(idx, kind_tok, float(slen_in))

        seq.append(tok)

    seq_str = "|".join(seq)

    try:
        h = hashlib.md5(seq_str.encode("utf-8")).hexdigest()
    except Exception:
        h = None

    seg_count = len(seq)
    is_solid = True if seg_count == 0 else False
    bucket = "{}|solid={}".format(seg_count, is_solid)

    return (seg_count, is_solid, h, {"access": "ok", "bucket": bucket, "seq": seq})

# -------------------------
# Discovery + Sampling
# -------------------------

all_patterns = _safe(
    lambda: (FilteredElementCollector(doc)
             .OfClass(LinePatternElement)
             .ToElements()),
    default=[]
)

try:
    all_patterns = list(all_patterns)
except:
    all_patterns = list(all_patterns)

# Cap AFTER collection
try:
    max_n = int(max_patterns_to_inspect)
    if max_n >= 0:
        all_patterns = all_patterns[:max_n]
except:
    pass

# Breadth-biased sampling: cap per segment-count bucket
selected = []
by_bucket = {}  # bucket_key -> count
for e in all_patterns:
    # Robust LP acquisition (sampling stage)
    lp = None
    try:
        lp = e.GetLinePattern()
    except Exception:
        lp = None

    if lp is None:
        try:
            lp = LinePatternElement.GetLinePattern(doc, e.Id)
        except Exception:
            lp = None

    seg_count, is_solid, h, meta = _linepattern_signature(lp)
    bucket_key = meta.get("bucket") if meta else "unknown"
    c = by_bucket.get(bucket_key, 0)

    if per_segment_count_limit is None:
        ok = True
    else:
        try:
            ok = c < int(per_segment_count_limit)
        except:
            ok = c < 5

    if ok:
        selected.append(e)
        by_bucket[bucket_key] = c + 1

# If limit is 0/negative, fallback to at least 1 per bucket
if len(selected) == 0 and len(all_patterns) > 0:
    seen = set()
    for e in all_patterns:
        lp = _safe(lambda: e.GetLinePattern(), None)
        _, _, _, meta = _linepattern_signature(lp)
        bucket_key = meta.get("bucket") if meta else "unknown"
        if bucket_key not in seen:
            selected.append(e)
            seen.add(bucket_key)

# Ensure at least one solid bucket is represented if present
try:
    want_bucket = "0|solid=True"
    have_solid = False
    for e in selected:
        lp = None
        try:
            lp = e.GetLinePattern()
        except Exception:
            lp = None
        if lp is None:
            try:
                lp = LinePatternElement.GetLinePattern(doc, e.Id)
            except Exception:
                lp = None
        _, _, _, meta = _linepattern_signature(lp)
        b = meta.get("bucket") if meta else "unknown"
        if b == want_bucket:
            have_solid = True
            break

    if not have_solid:
        for e in all_patterns:
            lp = None
            try:
                lp = e.GetLinePattern()
            except Exception:
                lp = None
            if lp is None:
                try:
                    lp = LinePatternElement.GetLinePattern(doc, e.Id)
                except Exception:
                    lp = None
            _, _, _, meta = _linepattern_signature(lp)
            b = meta.get("bucket") if meta else "unknown"
            if b == want_bucket:
                selected.append(e)
                break
except Exception:
    pass

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

def _touch_param(pk, pv, bucket_key):
    if pk not in param_index:
        param_index[pk] = {
            "storage_types": set(),
            "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
            "example": None,
            "observed_on_buckets": set(),
            "bucket_counts": {}
        }

    entry = param_index[pk]
    st = pv.get("storage")
    q = pv.get("q") or "unreadable"

    if st:
        entry["storage_types"].add(st)
    if q not in entry["q_counts"]:
        entry["q_counts"][q] = 0
    entry["q_counts"][q] += 1

    if bucket_key is not None:
        entry["observed_on_buckets"].add(bucket_key)
        bc = entry.get("bucket_counts") or {}
        bc[bucket_key] = bc.get(bucket_key, 0) + 1
        entry["bucket_counts"] = bc

    _maybe_set_example(entry, pv)

for e in selected:
    # GetLinePattern: match production extractor fallback behavior
    lp = None
    try:
        lp = e.GetLinePattern()
    except Exception:
        lp = None

    if lp is None:
        try:
            # Static fallback is required in some environments
            lp = LinePatternElement.GetLinePattern(doc, e.Id)
        except Exception:
            lp = None

    seg_count, is_solid, h, meta = _linepattern_signature(lp)
    bucket_key = meta.get("bucket") if meta else ("lp=None" if lp is None else "unknown")

    # Real parameters (if any exist for LinePatternElement in this environment)
    params = _safe(lambda: list(e.GetOrderedParameters()), default=None)
    if params is None:
        params = _safe(lambda: list(e.Parameters), default=[])

    for p in params:
        dn = _safe(lambda: _safe_param_def_name(p), None)
        if not dn:
            continue
        pk = "p.{}".format(dn)
        pv = _format_param_contract(p)
        _touch_param(pk, pv, bucket_key)

    # Synthetic properties (these are typically the meaningful surface for line patterns)
    name = _safe_elem_name(e)
    _touch_param("prop.name", _contract_from_raw("ok", "String", name, name, name), bucket_key)

    if seg_count is None:
        _touch_param(
            "prop.segment_count",
            _contract_from_raw("unreadable", "Integer", None, None, None),
            bucket_key
        )
    else:
        _touch_param(
            "prop.segment_count",
            _contract_from_raw("ok", "Integer", seg_count, str(seg_count), seg_count),
            bucket_key
        )

    solid_raw = 1 if is_solid else 0
    _touch_param("prop.is_solid", _contract_from_raw("ok", "Integer", solid_raw, str(bool(is_solid)), solid_raw), bucket_key)

    if h is None:
        _touch_param("prop.sequence_hash", _contract_from_raw("unreadable", "String", None, None, None), bucket_key)
    else:
        _touch_param("prop.sequence_hash", _contract_from_raw("ok", "String", h, h, h), bucket_key)

    seq_str = None
    if meta is not None:
        try:
            seq_str = "|".join(meta.get("seq") or [])
        except Exception:
            seq_str = None

    if seq_str is None:
        _touch_param("prop.sequence", _contract_from_raw("unreadable", "String", None, None, None), bucket_key)
    else:
        _touch_param("prop.sequence", _contract_from_raw("ok", "String", seq_str, seq_str, seq_str), bucket_key)

# Emit inventory records (stable order)
param_inventory = []
for pk in sorted(param_index.keys()):
    e = param_index[pk]
    param_inventory.append({
        "domain": "line_patterns",
        "param_key": pk,
        "selected_sample_count": len(selected),
        "example": e["example"],
        "observed": {
            "storage_types": sorted(list(e["storage_types"])),
            "q_counts": e["q_counts"],
            "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25],
            "bucket_counts": e.get("bucket_counts") or {}
        }
    })

# -------------------------
# Optional Crosswalk: LineStyle -> LinePattern
# -------------------------

optional_crosswalk = []

def _iter_line_style_categories():
    """
    Prefer category-driven discovery for line styles only (crosswalk),
    because LineStyle is not a distinct element class we can collect directly.

    Returns Categories (subcategories) under OST_Lines when available.
    """
    cats = _safe(lambda: doc.Settings.Categories, None)
    if cats is None:
        return []
    lines_cat = _safe(lambda: cats.get_Item(BuiltInCategory.OST_Lines), None)
    if lines_cat is None:
        return []

    subs = _safe(lambda: list(lines_cat.SubCategories), default=[])
    try:
        subs = list(subs)
    except:
        subs = list(subs)

    return subs

def _category_line_pattern_id(cat, gst):
    # Some categories may throw or return InvalidElementId
    try:
        return cat.GetLinePatternId(gst)
    except:
        return ElementId.InvalidElementId

# Build quick lookup: pattern_id -> name
pattern_name_by_id = {}
for pe in all_patterns:
    pid = _safe(lambda: pe.Id.IntegerValue, None)
    if pid is not None and pid not in pattern_name_by_id:
        pattern_name_by_id[pid] = _safe_elem_name(pe)

if enable_crosswalk:
    crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 50

    seen = set()  # (gst_label, pattern_id)
    gst_plan = [
        (GraphicsStyleType.Projection, "Projection"),
        (GraphicsStyleType.Cut, "Cut")
    ]

    for gst, gst_label in gst_plan:
        for cat in _iter_line_style_categories():
            if len(optional_crosswalk) >= int(crosswalk_limit):
                break

            row = {
                "linestyle.category_id": _safe(lambda: cat.Id.IntegerValue, None),
                "linestyle.name": _safe(lambda: cat.Name, None),
                "linestyle.graphics_style_type": gst_label,
                "pattern.resolved": False,
                "pattern.id": None,
                "pattern.name": None
            }

            pid = _category_line_pattern_id(cat, gst)
            if pid is None or pid == ElementId.InvalidElementId:
                continue

            raw = _safe(lambda: pid.IntegerValue, None)
            if raw is None:
                continue

            k = (gst_label, raw)
            if k in seen:
                continue

            row["pattern.id"] = raw
            row["pattern.name"] = pattern_name_by_id.get(raw)

            if row["pattern.name"] is None:
                ref = _safe(lambda: doc.GetElement(pid), None)
                row["pattern.name"] = _safe_elem_name(ref) if ref is not None else None

            row["pattern.resolved"] = True if row["pattern.name"] is not None else False
            if not row["pattern.resolved"]:
                continue

            seen.add(k)
            optional_crosswalk.append(row)

# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "inventory",
        "domain": "line_patterns",
        "records": param_inventory
    },
    {
        "kind": "crosswalk",
        "domain": "line_patterns",
        "records": optional_crosswalk
    }
]

# Optional: write to JSON for future reference (valid JSON, stable order)
file_written = None
write_error = None

if write_json:
    try:
        # Choose default directory: RVT folder if possible, else temp
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
        fixed_name = "probe_line_patterns_{}.json".format(date_stamp)

        # IN[4] is treated as an output directory (not a filename)
        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(OUT_payload, f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to inventory header (keeps OUT shape stable)
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload
