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
#        Filename is fixed as: probes_<revit_version>_<run_id>.json
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

def _resolve_workset(doc, ws_id_obj):
    """Resolve an Element.WorksetId value to (name, resolved_bool) via
    WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
    distinct .NET type from ElementId (both happen to expose .IntegerValue,
    which is why reflection reports this member as ElementId-storage), and
    Workset is not derived from Element, so doc.GetElement() would never
    resolve it even with the right type assumed."""
    if ws_id_obj is None:
        return (None, False)
    wt_table = _safe(lambda: doc.GetWorksetTable(), None)
    if wt_table is None:
        return (None, False)
    ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
    if ws is None:
        return (None, False)
    name = _safe(lambda: ws.Name, None)
    return (name, name is not None)


# Build quick lookup: pattern_id -> name / workset. The crosswalk row's
# subject is a line-style Category (linestyle.category_id/.name), and
# Category is not an Element -- it has no WorksetId at all. The pattern it
# resolves to (pattern.id/.name) IS an Element (LinePatternElement), so
# that's the side WorksetId belongs on.
pattern_name_by_id = {}
pattern_workset_by_id = {}
for pe in all_patterns:
    pid = _safe(lambda: pe.Id.IntegerValue, None)
    if pid is not None and pid not in pattern_name_by_id:
        pattern_name_by_id[pid] = _safe_elem_name(pe)
        pe_ws_id_obj = _safe(lambda: pe.WorksetId, None)
        pe_ws_name, _pe_ws_resolved = _resolve_workset(doc, pe_ws_id_obj)
        pe_ws_id_int = _safe(lambda: pe_ws_id_obj.IntegerValue, None) if pe_ws_id_obj is not None else None
        pattern_workset_by_id[pid] = (pe_ws_id_int, pe_ws_name)

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

            p_ws_id_int, p_ws_name = pattern_workset_by_id.get(raw, (None, None))
            row["pattern.workset_id"] = p_ws_id_int
            row["pattern.workset_name"] = p_ws_name

            seen.add(k)
            optional_crosswalk.append(row)


# -------------------------
# Reflection sweep (breadth): non-Parameter .NET members via reflection
# -------------------------
# Complements the curated/dynamic capture above with a breadth-only sweep of
# the sampled objects' .NET properties and zero-arg methods. This is
# diagnostics/breadth, not identity -- it surfaces members a fixed/curated
# key list or a Parameters-only walk could otherwise miss.

_REFLECTION_SKIP = set([
    "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
    "Dispose", "GetEnumerator", "Clone",
])

def _reflect_member_names(obj):
    out = []
    if obj is None:
        return out
    try:
        t = obj.GetType()
    except:
        return out
    try:
        for p in t.GetProperties():
            try:
                n = p.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if p.GetIndexParameters():
                    continue
                out.append(("property", n))
            except:
                pass
    except:
        pass
    try:
        for m in t.GetMethods():
            try:
                n = m.Name
                if n in _REFLECTION_SKIP or n.startswith("_"):
                    continue
                if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
                    continue
                if m.GetParameters().Length != 0:
                    continue
                if m.IsSpecialName:
                    continue
                out.append(("method", n))
            except:
                pass
    except:
        pass
    seen = set()
    uniq = []
    for kind, n in out:
        if n in seen:
            continue
        seen.add(n)
        uniq.append((kind, n))
    return sorted(uniq, key=lambda x: x[1])

# Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
# docs/method_invocation_candidates_annotated.csv): these 33 method names (34
# (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
# zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
# GetValidTypes, removed post-merge -- see the note below the dict) are
# ground-truth confirmed, against the live
# RevitAPI 2025 documentation (not name/return-type inference), to be
# zero-arg, instance, non-mutating getters. Declared here as data, separate
# from the branching logic in _reflect_try_get below, so it can be reviewed
# as one block and extended later without touching control flow.
#
# Keyed by method NAME only, not (declaring_class, name): this reflection
# sweep is invoked per concrete probed type_label (e.g. "WallType",
# "ProjectInformation", "FamilySymbol"), which is almost never the literal
# Revit API class that actually declares the member -- e.g. Element.GetTypeId
# is reached in this codebase via more than a dozen different concrete
# type_labels across the probe domains, never via type_label=="Element"
# itself, since no probe in this file reflects a bare Element instance.
# Scoping the allowlist by declaring-class name would silently fail to match
# nearly every real call site and defeat the point of this allowlist.
# _reflect_member_names() below already restricts candidate methods to
# public, non-special-name, zero-parameter methods before this allowlist is
# ever consulted, so a name-only match here does not weaken the zero-arg/
# no-side-effect intent the allowlist exists to enforce -- it only widens
# which already-zero-arg, already-name-matched members get invoked. The
# dict value (declaring class) is kept for traceability back to the Step 0
# CSV only; it is not used in the match.
_ALLOWLISTED_REFLECTION_METHODS = {
    "GetTypeId": "Element",
    "GetLayers": "CompoundStructure",
    "GetEntitySchemaGuids": "Element",
    "GetSubelements": "Element",
    "GetFamilyPointLocations": "FamilySymbol",
    "GetModelToProjectionTransforms": "View",
    "GetRenderingAsset": "AppearanceAssetElement",
    "GetExternalFileReference": "Element",
    "GetMonitoredLinkElementIds": "Element",
    "GetMonitoredLocalElementIds": "Element",
    "GetSimilarTypes": "ElementType",
    "GetStructuralSection": "FamilySymbol",
    "GetThermalProperties": "FamilySymbol",
    "GetFillPattern": "FillPatternElement",
    "GetLinePattern": "LinePatternElement",
    "GetCategories": "ParameterFilterElement",
    "GetElementFilter": "ParameterFilterElement",
    "GetReference": "Subelement",
    "GetBackground": "View",
    "GetCalloutParentId": "View",
    "GetCropRegionShapeManager": "View",
    "GetDepthCueing": "View",
    "GetDirectContext3DHandleOverrides": "View",
    "GetFilters": "View",
    "GetOrderedFilters": "View",
    "GetPointCloudOverrides": "View",
    "GetPrimaryViewId": "View",
    "GetReferenceCallouts": "View",
    "GetReferenceElevations": "View",
    "GetReferenceSections": "View",
    "GetSketchyLines": "View",
    "GetTemporaryViewPropertiesId": "View",
    "GetViewDisplayModel": "View",
}

# Element.GetValidTypes / Subelement.GetValidTypes were removed from the
# allowlist above after a live re-run (PR #395 discussion) showed
# Element.GetValidTypes fails 100% of the time -- not with a documented
# Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
# GetModelToProjectionTransforms above, which each match a real
# InvalidOperationException precondition stated on their own RevitAPI doc
# pages), but with a CLR/pythonnet interop binding failure:
# `TypeError: No method matches given arguments for GetValidTypes: (<class
# '...'>)`, confirmed via a standalone diagnostic against live ElementType/
# WallType/View objects. .NET reflection sees exactly one GetValidTypes
# overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
# overload-ambiguity problem either -- the call is rejected by the binder
# before it ever reaches Revit's implementation. This will never succeed
# through `getattr(obj, name)()` regardless of model/version, so keeping it
# allowlisted only adds permanent error noise with zero chance of a real
# value. Subelement.GetValidTypes was never independently tested (no probe
# in this codebase reflects a raw Subelement object as its own type_label),
# but shares the same allowlist name and the same removal; re-evaluate
# independently against a live Subelement instance before re-adding either.

def _reflect_try_get(obj, member_kind, name):
    if member_kind == "method":
        if name not in _ALLOWLISTED_REFLECTION_METHODS:
            # SAFETY: never invoke a reflection-discovered method that is not
            # on the allowlist above. Revit API methods can have side effects
            # (printing, export, regenerate, delete, transaction commits,
            # ...) and there is no reliable way to tell a safe zero-arg
            # query method from a side-effecting one by name alone for
            # anything outside the allowlist's ground-truth-verified set.
            # Record that the method exists without calling it.
            return (True, "<method not invoked>", None)
        try:
            v = getattr(obj, name)()
        except Exception as ex:
            return (False, None, "{}: {}".format(type(ex).__name__, ex))
        return (True, v, None)
    try:
        v = getattr(obj, name)
    except Exception as ex:
        return (False, None, "{}: {}".format(type(ex).__name__, ex))
    return (True, v, None)

def _reflect_contract(raw_v):
    if raw_v is None:
        return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
    if isinstance(raw_v, bool):
        return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
    if isinstance(raw_v, int):
        return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, float):
        return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
    if isinstance(raw_v, str):
        return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
    try:
        if hasattr(raw_v, "IntegerValue"):
            iv = int(raw_v.IntegerValue)
            return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
    except:
        pass
    try:
        if hasattr(raw_v, "ToString"):
            s = raw_v.ToString()
            if s and "Autodesk.Revit" not in s and "System." not in s:
                return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
    except:
        pass
    try:
        ids = []
        for item in raw_v:
            if not hasattr(item, "IntegerValue"):
                raise TypeError("non-ElementId item in collection")
            ids.append(int(item.IntegerValue))
        disp = ",".join(str(i) for i in ids)
        return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
    except:
        pass
    return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}

def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
    idx = {}
    for obj in sample_objs:
        if obj is None:
            continue
        for member_kind, name in _reflect_member_names(obj)[:max_members]:
            ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
            key = "refl.{}.{}".format(type_label, name)
            if key not in idx:
                idx[key] = {
                    "domain": domain_name, "member_key": key, "member_kind": member_kind,
                    "type_label": type_label, "example": None,
                    "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
                }
            e = idx[key]
            if not ok:
                e["error_count"] += 1
                continue
            contract = _reflect_contract(raw_v)
            e["ok_count"] += 1
            sig = (str(contract.get("storage")), str(contract.get("norm")))
            if sig not in e["_seen"]:
                e["_seen"].add(sig)
                e["unique_value_count"] += 1
            if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
                e["example"] = contract
    records = []
    for key in sorted(idx.keys()):
        e = idx[key]
        records.append({
            "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
            "type_label": e["type_label"], "example": e["example"],
            "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
        })
    return records

_reflection_records_0 = _run_reflection_sweep(selected, "LinePatternElement", "line_patterns")
_reflection_records = _reflection_records_0

# Assemble labeled output payload
OUT_payload = [
    {
        "kind": "reflection",
        "domain": "line_patterns",
        "records": _reflection_records
    },
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

# -------------------------
# Unified run metadata (release-separated, not date-filename-separated)
# -------------------------
# extraction_date lives as JSON metadata, not as a filename token; the
# filename groups by Revit release (revit_version) plus an opaque run_id so
# repeated runs don't collide. See tools/probes/build_probe_inventory.py,
# which consumes this shape directly.

import uuid as _uuid_mod

def _probe_revit_version():
    try:
        _uiapp = DocumentManager.Instance.CurrentUIApplication
        _app = _uiapp.Application if _uiapp is not None else None
        v = _safe(lambda: _app.VersionNumber, None)
        return str(v) if v else None
    except:
        return None

def _probe_document_identity():
    return {
        "title": _safe(lambda: doc.Title, None),
        "path_name": _safe(lambda: doc.PathName, None),
        "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
    }

def _probe_run_id():
    try:
        return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
    except:
        return _uuid_mod.uuid4().hex[:12]

_PROBE_RUN_ID = _probe_run_id()
_PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"

def _probe_wrap(domain, out_payload):
    return {
        "run_metadata": {
            "run_id": _PROBE_RUN_ID,
            "extraction_date": datetime.now().isoformat(),
            "revit_version": _PROBE_REVIT_VERSION,
            "tool_version": None,
            "document": _probe_document_identity(),
            "source": "single_probe",
            "probe": domain,
        },
        "domains": {domain: out_payload},
    }


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
        fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)

        # IN[4] is treated as an output directory (not a filename)
        target_dir = out_path if out_path else default_dir
        target_path = os.path.join(target_dir, fixed_name)

        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir)

        with open(target_path, "w") as f:
            json.dump(_probe_wrap("line_patterns", OUT_payload), f, indent=2, sort_keys=True)

        file_written = target_path

    except Exception as ex:
        write_error = "{}: {}".format(type(ex).__name__, ex)

# Attach write metadata to inventory header (keeps OUT shape stable)
OUT_payload[0]["file_written"] = file_written
if write_error:
    OUT_payload[0]["file_write_error"] = write_error

OUT = OUT_payload
