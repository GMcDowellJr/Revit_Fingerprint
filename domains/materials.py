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

from core.collect import collect_instances
from core.hashing import make_hash, safe_str
from core.canon import S_MISSING, S_UNREADABLE, S_NONE, S_UNRESOLVED, canon_str
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
                    return S_MISSING
            except Exception:
                pass
            for accessor in ("AsString", "AsValueString"):
                try:
                    v = getattr(p, accessor)()
                    return canon_str(v)
                except Exception:
                    continue
            return S_UNREADABLE

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
                return S_MISSING
        except Exception:
            pass
        for accessor in ("AsString", "AsValueString"):
            try:
                v = getattr(p, accessor)()
                return canon_str(v)
            except Exception:
                continue
        return S_UNREADABLE

    return S_MISSING


def _resolve_pattern_slot(*, doc, ctx, pattern_id_obj, debug):
    pid_v, pid_q = canonicalize_int(getattr(pattern_id_obj, "IntegerValue", None) if pattern_id_obj is not None else None)

    out = {
        "id_local": pid_v if pid_q == ITEM_Q_OK else (S_MISSING if pid_q == ITEM_Q_MISSING else S_UNREADABLE),
        "uid": S_MISSING,
        "name": S_MISSING,
        "sig_hash": S_MISSING,
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

        identity_items = [
            make_identity_item("material.uid", *canonicalize_str(uid)),
            make_identity_item("material.id_local", *canonicalize_str(id_local)),
            make_identity_item("material.name", *canonicalize_str(name)),
            make_identity_item("material.class", *canonicalize_str(mat_class)),
            make_identity_item("material.description", *canonicalize_str(description)),
            make_identity_item("material.comments", *canonicalize_str(comments)),
            make_identity_item("material.keywords", *canonicalize_str(keywords)),
            make_identity_item("material.manufacturer", *canonicalize_str(manufacturer)),
            make_identity_item("material.model", *canonicalize_str(model)),
            make_identity_item("material.cost", *canonicalize_str(cost)),
            make_identity_item("material.url", *canonicalize_str(url)),
            make_identity_item("material.keynote", *canonicalize_str(keynote)),
            make_identity_item("material.mark", *canonicalize_str(mark)),
            make_identity_item("material.use_render_appearance", *canonicalize_str(use_render_appearance)),
            make_identity_item("material.shading_color_rgb", *canonicalize_str(shading_color_rgb)),
            make_identity_item("material.shading_transparency", *canonicalize_str(shading_transparency)),
            make_identity_item("material.surface_foreground_pattern.id_local", *canonicalize_str(surface_fg["id_local"])),
            make_identity_item("material.surface_foreground_pattern.uid", *canonicalize_str(surface_fg["uid"])),
            make_identity_item("material.surface_foreground_pattern.name", *canonicalize_str(surface_fg["name"])),
            make_identity_item("material.surface_foreground_pattern.sig_hash", *canonicalize_str(surface_fg["sig_hash"])),
            make_identity_item("material.surface_foreground_pattern_color_rgb", *canonicalize_str(surface_fg_color)),
            make_identity_item("material.surface_background_pattern.id_local", *canonicalize_str(surface_bg["id_local"])),
            make_identity_item("material.surface_background_pattern.uid", *canonicalize_str(surface_bg["uid"])),
            make_identity_item("material.surface_background_pattern.name", *canonicalize_str(surface_bg["name"])),
            make_identity_item("material.surface_background_pattern.sig_hash", *canonicalize_str(surface_bg["sig_hash"])),
            make_identity_item("material.surface_background_pattern_color_rgb", *canonicalize_str(surface_bg_color)),
            make_identity_item("material.cut_foreground_pattern.id_local", *canonicalize_str(cut_fg["id_local"])),
            make_identity_item("material.cut_foreground_pattern.uid", *canonicalize_str(cut_fg["uid"])),
            make_identity_item("material.cut_foreground_pattern.name", *canonicalize_str(cut_fg["name"])),
            make_identity_item("material.cut_foreground_pattern.sig_hash", *canonicalize_str(cut_fg["sig_hash"])),
            make_identity_item("material.cut_foreground_pattern_color_rgb", *canonicalize_str(cut_fg_color)),
            make_identity_item("material.cut_background_pattern.id_local", *canonicalize_str(cut_bg["id_local"])),
            make_identity_item("material.cut_background_pattern.uid", *canonicalize_str(cut_bg["uid"])),
            make_identity_item("material.cut_background_pattern.name", *canonicalize_str(cut_bg["name"])),
            make_identity_item("material.cut_background_pattern.sig_hash", *canonicalize_str(cut_bg["sig_hash"])),
            make_identity_item("material.cut_background_pattern_color_rgb", *canonicalize_str(cut_bg_color)),
            make_identity_item("material.appearance_asset_capture_status", *canonicalize_str("deferred")),
            make_identity_item("material.physical_asset_capture_status", *canonicalize_str("deferred")),
            make_identity_item("material.thermal_asset_capture_status", *canonicalize_str("deferred")),
        ]

        sem_keys = {
            "material.shading_color_rgb",
            "material.shading_transparency",
            "material.surface_foreground_pattern.sig_hash",
            "material.surface_foreground_pattern_color_rgb",
            "material.surface_background_pattern.sig_hash",
            "material.surface_background_pattern_color_rgb",
            "material.cut_foreground_pattern.sig_hash",
            "material.cut_foreground_pattern_color_rgb",
            "material.cut_background_pattern.sig_hash",
            "material.cut_background_pattern_color_rgb",
        }
        identity_items_sorted = sorted(identity_items, key=lambda it: safe_str(it.get("k", "")))
        semantic_items = [it for it in identity_items_sorted if it.get("k") in sem_keys]
        graphics_sig_hash_v2 = make_hash(serialize_identity_items(semantic_items))

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
        if info["debug_pattern_ctx_missing"] > 0:
            status_v2 = STATUS_DEGRADED
            status_reasons.append("fill_pattern_ctx_missing")

        rec = build_record_v2(
            domain="materials",
            record_id="uid:{}".format(uid),
            status=status_v2,
            status_reasons=sorted(set(status_reasons)),
            sig_hash=graphics_sig_hash_v2,
            identity_items=identity_items_sorted,
            required_qs=[ITEM_Q_OK],
            label={
                "display": safe_str(name),
                "quality": "human" if name not in (S_MISSING, S_UNREADABLE) else "computed",
                "provenance": "computed.material",
                "components": {"uid": safe_str(uid), "name": safe_str(name)},
            },
        )
        rec["graphics_sig_hash_v2"] = graphics_sig_hash_v2

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
