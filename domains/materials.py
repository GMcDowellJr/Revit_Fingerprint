# -*- coding: utf-8 -*-
"""Materials domain extractor (materials.v1).

Promoted from ctx-only into a standalone governance domain.

v1 policy:
- Record content captures Identity + displayed Graphics state.
- Signature is graphics-only (identity labels excluded).
- Appearance/Physical/Thermal payload capture is deferred.
- Domain continues exporting ctx maps for downstream consumers.
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(current_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from core.collect import collect_instances, purge_lookup
from core.hashing import make_hash, safe_str
from core.canon import S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE, S_NONE, S_UNRESOLVED, canon_str
from core.record_v2 import (
    STATUS_OK,
    STATUS_DEGRADED,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    canonicalize_str,
    canonicalize_int,
    make_identity_item,
    serialize_identity_items,
    build_record_v2,
)

try:
    from Autodesk.Revit.DB import Material, BuiltInParameter
except ImportError:
    Material = None
    BuiltInParameter = None


CTX_MATERIAL_ID_TO_UID = "material_id_to_uid"
CTX_MATERIAL_ID_TO_NAME = "material_id_to_name"
CTX_MATERIAL_ID_TO_SIG_HASH = "material_id_to_sig_hash"
CTX_MATERIAL_UID_TO_NAME = "material_uid_to_name"
CTX_MATERIAL_UID_TO_CLASS = "material_uid_to_class"
CTX_MATERIAL_UID_TO_RECORD = "material_uid_to_record"
CTX_MATERIAL_UID_TO_SIG_HASH = "material_uid_to_sig_hash"
CTX_MATERIAL_UID_TO_GRAPHICS_SIG_HASH = "material_uid_to_graphics_sig_hash"

CTX_FILL_PATTERN_UID_TO_HASH = "fill_pattern_uid_to_hash"
CTX_FILL_PATTERN_ID_TO_VALUE = "fill_pattern_id_to_value"
CTX_FILL_PATTERN_SPECIAL_VALUES = "fill_pattern_special_values"

def _read_prop(obj, name):
    try:
        return getattr(obj, name)
    except Exception:
        return S_UNREADABLE


def _canon_id_local(element):
    try:
        eid = getattr(getattr(element, "Id", None), "IntegerValue", None)
    except Exception:
        eid = None
    if eid is None:
        return S_MISSING
    try:
        return str(int(eid))
    except Exception:
        return S_UNREADABLE


def _rgb_sig(col):
    if col is None:
        return S_MISSING
    try:
        return "{},{},{}".format(int(col.Red), int(col.Green), int(col.Blue))
    except Exception:
        return S_UNREADABLE


def _read_param_as_string(mat, *, bip_names, lookup_names):
    """Return (v, q) for optional identity metadata fields."""
    # BuiltInParameter candidates (if available in this API/runtime)
    if BuiltInParameter is not None:
        for bip_name in list(bip_names or []):
            try:
                bip = getattr(BuiltInParameter, bip_name)
            except Exception:
                bip = None
            if bip is None:
                continue
            try:
                p = mat.get_Parameter(bip)
            except Exception:
                p = None
            if p is None:
                continue
            try:
                if hasattr(p, "HasValue") and p.HasValue is False:
                    return None, ITEM_Q_MISSING
            except Exception:
                pass
            for accessor in ("AsString", "AsValueString"):
                try:
                    v = getattr(p, accessor)()
                    return canonicalize_str(v)
                except Exception:
                    continue
            return None, ITEM_Q_UNREADABLE

    # Parameter name fallbacks
    for pname in list(lookup_names or []):
        try:
            p = mat.LookupParameter(pname)
        except Exception:
            p = None
        if p is None:
            continue
        try:
            if hasattr(p, "HasValue") and p.HasValue is False:
                return None, ITEM_Q_MISSING
        except Exception:
            pass
        for accessor in ("AsString", "AsValueString"):
            try:
                v = getattr(p, accessor)()
                return canonicalize_str(v)
            except Exception:
                continue
        return None, ITEM_Q_UNREADABLE

    return None, ITEM_Q_MISSING


def _resolve_pattern_slot(*, doc, ctx, pattern_id_obj, debug):
    pid_v, pid_q = canonicalize_int(getattr(pattern_id_obj, "IntegerValue", None) if pattern_id_obj is not None else None)

    out = {
        "id_local": pid_v if pid_q == ITEM_Q_OK else (S_MISSING if pid_q == ITEM_Q_MISSING else S_UNREADABLE),
        "uid": S_MISSING,
        "name": S_MISSING,
        "sig_hash": S_MISSING,
        "_ctx_missing": False,
    }

    if pid_q == ITEM_Q_UNREADABLE:
        debug["debug_pattern_id_unreadable"] += 1
        return out
    if pid_q == ITEM_Q_MISSING:
        debug["debug_pattern_id_missing"] += 1
        return out

    if pid_v == "-1":
        out["uid"] = S_NONE
        out["name"] = S_NONE
        out["sig_hash"] = S_NONE
        debug["debug_pattern_none"] += 1
        return out

    id_to_value = (ctx or {}).get(CTX_FILL_PATTERN_ID_TO_VALUE) if ctx is not None else None
    if not isinstance(id_to_value, dict) or not id_to_value:
        debug["debug_pattern_ctx_missing"] += 1
        out["_ctx_missing"] = True
        out["sig_hash"] = S_UNRESOLVED
    else:
        mapped = id_to_value.get(pid_v)
        if mapped is None:
            debug["debug_pattern_sig_unresolved"] += 1
            out["sig_hash"] = S_UNRESOLVED
        else:
            out["sig_hash"] = canon_str(mapped)

    # uid/name can always be attempted from document by ElementId
    try:
        elem = doc.GetElement(pattern_id_obj)
    except Exception:
        elem = None
    if elem is None:
        if out["uid"] == S_MISSING:
            out["uid"] = S_UNRESOLVED
        if out["name"] == S_MISSING:
            out["name"] = S_UNRESOLVED
        debug["debug_pattern_doc_lookup_fail"] += 1
        return out

    try:
        out["uid"] = canon_str(getattr(elem, "UniqueId", None))
    except Exception:
        out["uid"] = S_UNREADABLE
    try:
        out["name"] = canon_str(getattr(elem, "Name", None))
    except Exception:
        out["name"] = S_UNREADABLE
    return out


def _export_ctx(ctx, maps):
    if ctx is None:
        return
    for key, value in maps.items():
        existing = ctx.get(key) or {}
        existing.update(value or {})
        ctx[key] = existing


def _safe_item_value(v):
    """Return (value, q) safe for make_identity_item without sentinel literals."""
    if v == S_MISSING:
        return None, ITEM_Q_MISSING
    if v == S_UNREADABLE:
        return None, ITEM_Q_UNREADABLE
    if v == S_NOT_APPLICABLE:
        return None, ITEM_Q_MISSING
    return canonicalize_str(v)


def _mk_item(k, v):
    if isinstance(v, tuple) and len(v) == 2:
        vv, qq = v
        if isinstance(vv, str) and vv in (S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE):
            vv = None
        return make_identity_item(k, vv, qq)
    vv, qq = _safe_item_value(v)
    return make_identity_item(k, vv, qq)


_MATERIAL_SIG_KEYS = [
    "material.sig.shading_color_rgb",
    "material.sig.shading_transparency",
    "material.sig.surface_foreground_pattern.sig_hash",
    "material.sig.surface_foreground_pattern_color_rgb",
    "material.sig.surface_background_pattern.sig_hash",
    "material.sig.surface_background_pattern_color_rgb",
    "material.sig.cut_foreground_pattern.sig_hash",
    "material.sig.cut_foreground_pattern_color_rgb",
    "material.sig.cut_background_pattern.sig_hash",
    "material.sig.cut_background_pattern_color_rgb",
]


def extract(doc, ctx=None):
    info = {
        "count": 0,
        "raw_count": 0,
        "hash_v2": None,
        "signature_hashes_v2": [],
        "records": [],
        "record_rows": [],
        "status": "ok",
        "debug_unreadable": 0,
        "debug_v2_blocked": False,
        "debug_pattern_ctx_missing": 0,
        "debug_pattern_sig_unresolved": 0,
        "debug_pattern_doc_lookup_fail": 0,
        "debug_pattern_id_missing": 0,
        "debug_pattern_id_unreadable": 0,
        "debug_pattern_none": 0,
    }

    id_to_uid = {}
    id_to_name = {}
    id_to_sig_hash = {}
    uid_to_name = {}
    uid_to_class = {}
    uid_to_record = {}
    uid_to_sig_hash = {}
    uid_to_graphics_sig_hash = {}

    if Material is None:
        info["status"] = "blocked"
        info["debug_v2_blocked"] = True
        _export_ctx(ctx, {
            CTX_MATERIAL_ID_TO_UID: id_to_uid,
            CTX_MATERIAL_ID_TO_NAME: id_to_name,
            CTX_MATERIAL_ID_TO_SIG_HASH: id_to_sig_hash,
            CTX_MATERIAL_UID_TO_NAME: uid_to_name,
            CTX_MATERIAL_UID_TO_CLASS: uid_to_class,
            CTX_MATERIAL_UID_TO_RECORD: uid_to_record,
            CTX_MATERIAL_UID_TO_SIG_HASH: uid_to_sig_hash,
            CTX_MATERIAL_UID_TO_GRAPHICS_SIG_HASH: uid_to_graphics_sig_hash,
        })
        return info

    try:
        mats = list(
            collect_instances(
                doc,
                of_class=Material,
                cctx=(ctx or {}).get("_collect") if ctx is not None else None,
                cache_key="materials:Material:instances",
            )
        )
    except Exception:
        mats = []

    info["raw_count"] = len(mats)

    for m in mats:
        uid = canon_str(getattr(m, "UniqueId", None))
        if uid in (S_MISSING, S_UNREADABLE):
            info["debug_unreadable"] += 1
            continue

        id_local = _canon_id_local(m)
        name = canon_str(_read_prop(m, "Name"))
        mat_class = canon_str(_read_prop(m, "MaterialClass"))

        use_render_appearance = canon_str(_read_prop(m, "UseRenderAppearanceForShading"))
        shading_color_rgb = _rgb_sig(_read_prop(m, "Color"))
        shading_transparency = canon_str(_read_prop(m, "Transparency"))

        # Identity fields sourced from Material.Identity tab (direct/parameter backed)
        description = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_DESCRIPTION"],
            lookup_names=["Description"],
        )
        comments = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_TYPE_COMMENTS", "ALL_MODEL_INSTANCE_COMMENTS"],
            lookup_names=["Comments"],
        )
        keywords = _read_param_as_string(
            m,
            bip_names=["MATERIAL_KEYWORDS"],
            lookup_names=["Keywords"],
        )
        manufacturer = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_MANUFACTURER"],
            lookup_names=["Manufacturer"],
        )
        model = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_MODEL"],
            lookup_names=["Model"],
        )
        cost = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_COST"],
            lookup_names=["Cost"],
        )
        url = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_URL"],
            lookup_names=["URL"],
        )
        keynote = _read_param_as_string(
            m,
            bip_names=["KEYNOTE_PARAM"],
            lookup_names=["Keynote"],
        )
        mark = _read_param_as_string(
            m,
            bip_names=["ALL_MODEL_MARK"],
            lookup_names=["Mark"],
        )

        surface_fg = _resolve_pattern_slot(
            doc=doc,
            ctx=ctx,
            pattern_id_obj=_read_prop(m, "SurfaceForegroundPatternId"),
            debug=info,
        )
        surface_fg_color = _rgb_sig(_read_prop(m, "SurfaceForegroundPatternColor"))

        surface_bg = _resolve_pattern_slot(
            doc=doc,
            ctx=ctx,
            pattern_id_obj=_read_prop(m, "SurfaceBackgroundPatternId"),
            debug=info,
        )
        surface_bg_color = _rgb_sig(_read_prop(m, "SurfaceBackgroundPatternColor"))

        cut_fg = _resolve_pattern_slot(
            doc=doc,
            ctx=ctx,
            pattern_id_obj=_read_prop(m, "CutForegroundPatternId"),
            debug=info,
        )
        cut_fg_color = _rgb_sig(_read_prop(m, "CutForegroundPatternColor"))

        cut_bg = _resolve_pattern_slot(
            doc=doc,
            ctx=ctx,
            pattern_id_obj=_read_prop(m, "CutBackgroundPatternId"),
            debug=info,
        )
        cut_bg_color = _rgb_sig(_read_prop(m, "CutBackgroundPatternColor"))

        sig_basis_items = [
            _mk_item("material.sig.shading_color_rgb", shading_color_rgb),
            _mk_item("material.sig.shading_transparency", shading_transparency),
            _mk_item("material.sig.surface_foreground_pattern.sig_hash", surface_fg["sig_hash"]),
            _mk_item("material.sig.surface_foreground_pattern_color_rgb", surface_fg_color),
            _mk_item("material.sig.surface_background_pattern.sig_hash", surface_bg["sig_hash"]),
            _mk_item("material.sig.surface_background_pattern_color_rgb", surface_bg_color),
            _mk_item("material.sig.cut_foreground_pattern.sig_hash", cut_fg["sig_hash"]),
            _mk_item("material.sig.cut_foreground_pattern_color_rgb", cut_fg_color),
            _mk_item("material.sig.cut_background_pattern.sig_hash", cut_bg["sig_hash"]),
            _mk_item("material.sig.cut_background_pattern_color_rgb", cut_bg_color),
        ]
        identity_items = [_mk_item("material.uid", uid)] + sig_basis_items

        material_payload = {
            "uid": uid,
            "id_local": id_local,
            "name": name,
            "class": mat_class,
            "description": description[0],
            "comments": comments[0],
            "keywords": keywords[0],
            "manufacturer": manufacturer[0],
            "model": model[0],
            "cost": cost[0],
            "url": url[0],
            "keynote": keynote[0],
            "mark": mark[0],
            "use_render_appearance": use_render_appearance,
            "shading_color_rgb": shading_color_rgb,
            "shading_transparency": shading_transparency,
            "surface_foreground_pattern": dict(surface_fg),
            "surface_foreground_pattern_color_rgb": surface_fg_color,
            "surface_background_pattern": dict(surface_bg),
            "surface_background_pattern_color_rgb": surface_bg_color,
            "cut_foreground_pattern": dict(cut_fg),
            "cut_foreground_pattern_color_rgb": cut_fg_color,
            "cut_background_pattern": dict(cut_bg),
            "cut_background_pattern_color_rgb": cut_bg_color,
            "appearance_asset_capture_status": "deferred",
            "physical_asset_capture_status": "deferred",
            "thermal_asset_capture_status": "deferred",
        }
        identity_items_sorted = sorted(identity_items, key=lambda it: safe_str(it.get("k", "")))
        sig_basis_items_sorted = sorted(sig_basis_items, key=lambda it: safe_str(it.get("k", "")))
        graphics_sig_hash_v2 = make_hash(serialize_identity_items(sig_basis_items_sorted))

        status_v2 = STATUS_OK
        status_reasons = []
        if any(
            v == S_UNREADABLE
            for v in [
                name,
                mat_class,
                use_render_appearance,
                shading_color_rgb,
                shading_transparency,
                surface_fg_color,
                surface_bg_color,
                cut_fg_color,
                cut_bg_color,
            ]
        ):
            status_v2 = STATUS_DEGRADED
            status_reasons.append("unreadable_material_property")
        if any(v == S_UNRESOLVED for v in [surface_fg["sig_hash"], surface_bg["sig_hash"], cut_fg["sig_hash"], cut_bg["sig_hash"]]):
            status_v2 = STATUS_DEGRADED
            status_reasons.append("fill_pattern_resolution_unresolved")
        if any(bool(slot.get("_ctx_missing", False)) for slot in [surface_fg, surface_bg, cut_fg, cut_bg]):
            status_v2 = STATUS_DEGRADED
            status_reasons.append("fill_pattern_ctx_missing")

        rec = build_record_v2(
            domain="materials",
            record_id="uid:{}".format(uid),
            status=status_v2,
            status_reasons=sorted(set(status_reasons)),
            sig_hash=graphics_sig_hash_v2,
            identity_items=identity_items_sorted,
            required_qs=[identity_items_sorted[0].get("q", ITEM_Q_OK)],
            label={
                "display": safe_str(name),
                "quality": "human" if name not in (S_MISSING, S_UNREADABLE) else "computed",
                "provenance": "computed.path",
                "components": {"uid": safe_str(uid), "name": safe_str(name)},
            },
        )
        element_id_int = None
        try:
            element_id_int = int(rec["material"]["id_local"])
        except Exception:
            pass
        _ip, _ip_q = purge_lookup(element_id_int, ctx)
        rec["is_purgeable"] = _ip
        rec["is_purgeable_q"] = _ip_q
        rec["graphics_sig_hash_v2"] = graphics_sig_hash_v2
        rec["material"] = material_payload
        rec["sig_basis"] = {
            "schema": "materials.sig_basis.v1",
            "keys_used": list(_MATERIAL_SIG_KEYS),
        }

        info["records"].append(rec)
        info["count"] += 1

        sig = rec.get("sig_hash")
        if sig:
            info["signature_hashes_v2"].append(sig)

        if id_local not in (S_MISSING, S_UNREADABLE):
            id_to_uid[id_local] = uid
            id_to_name[id_local] = name
            id_to_sig_hash[id_local] = rec.get("sig_hash")
        uid_to_name[uid] = name
        uid_to_class[uid] = mat_class
        uid_to_record[uid] = rec
        uid_to_sig_hash[uid] = rec.get("sig_hash")
        uid_to_graphics_sig_hash[uid] = graphics_sig_hash_v2

    info["signature_hashes_v2"] = sorted([s for s in info["signature_hashes_v2"] if s])
    info["record_rows"] = [{"record_key": safe_str(r.get("record_id", "")), "sig_hash": r.get("sig_hash", None)} for r in info["records"]]

    if info["signature_hashes_v2"]:
        info["hash_v2"] = make_hash(info["signature_hashes_v2"])

    if info["status"] != "blocked":
        if any(r.get("status") == STATUS_DEGRADED for r in info["records"]):
            info["status"] = STATUS_DEGRADED
        else:
            info["status"] = STATUS_OK

    _export_ctx(ctx, {
        CTX_MATERIAL_ID_TO_UID: id_to_uid,
        CTX_MATERIAL_ID_TO_NAME: id_to_name,
        CTX_MATERIAL_ID_TO_SIG_HASH: id_to_sig_hash,
        CTX_MATERIAL_UID_TO_NAME: uid_to_name,
        CTX_MATERIAL_UID_TO_CLASS: uid_to_class,
        CTX_MATERIAL_UID_TO_RECORD: uid_to_record,
        CTX_MATERIAL_UID_TO_SIG_HASH: uid_to_sig_hash,
        CTX_MATERIAL_UID_TO_GRAPHICS_SIG_HASH: uid_to_graphics_sig_hash,
    })

    return info
