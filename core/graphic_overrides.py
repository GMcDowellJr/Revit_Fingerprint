# -*- coding: utf-8 -*-
"""core/graphic_overrides.py

Shared helpers for extracting graphics overrides across Category and
OverrideGraphicSettings APIs.

Revit exposes two parallel surfaces for graphics data:
  - Category API (object styles):
      * cat.GetLineWeight(GraphicsStyleType.Projection)
      * cat.LineColor
      * cat.GetLinePatternId(GraphicsStyleType.Projection)
  - OverrideGraphicSettings API (view/template overrides):
      * ogs.ProjectionLineWeight
      * ogs.ProjectionLineColor
      * ogs.ProjectionLinePatternId

These helpers normalize both surfaces into record.v2 IdentityItems with
consistent keys, values, and quality markers. All reads are fail-soft and
annotated with ITEM_Q_* values so downstream callers can degrade gracefully.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple
import importlib.util

from core.record_v2 import (
    make_identity_item,
    ITEM_Q_OK,
    ITEM_Q_MISSING,
    ITEM_Q_UNREADABLE,
    ITEM_Q_UNSUPPORTED,
    canonicalize_int,
    canonicalize_str,
    canonicalize_bool,
)
from core.canon import canon_str


_spec = importlib.util.find_spec("Autodesk.Revit.DB")
if _spec is not None:
    from Autodesk.Revit.DB import GraphicsStyleType, Category, OverrideGraphicSettings
else:
    GraphicsStyleType = None
    Category = None
    OverrideGraphicSettings = None


def _is_category(source: Any) -> bool:
    """Return True if source is a Revit Category instance."""
    return Category is not None and isinstance(source, Category)


def _is_ogs(source: Any) -> bool:
    """Return True if source is an OverrideGraphicSettings instance."""
    return OverrideGraphicSettings is not None and isinstance(source, OverrideGraphicSettings)


def _is_invalid_element_id(elem_id: Any) -> bool:
    """Return True if elem_id is a non-reference ElementId.

    In OverrideGraphicSettings, invalid ElementIds (0 or -1) mean
    "no override" and should be treated as missing.
    """
    if elem_id is None:
        return True
    try:
        iv = getattr(elem_id, "IntegerValue", None)
        if iv is None:
            return False
        return int(iv) <= 0
    except Exception:
        return False


def _rgb_from_color(color_obj: Any) -> Tuple[Optional[str], str]:
    """Convert a Revit Color object into an "R-G-B" string."""
    if color_obj is None:
        return None, ITEM_Q_MISSING
    try:
        rgb = "{}-{}-{}".format(int(color_obj.Red), int(color_obj.Green), int(color_obj.Blue))
    except Exception:
        return None, ITEM_Q_UNREADABLE
    return canonicalize_str(rgb)


def _read_attr(obj: Any, name: str) -> Tuple[Optional[Any], str]:
    """Safely read an attribute, returning (value, q)."""
    try:
        return getattr(obj, name), ITEM_Q_OK
    except AttributeError:
        return None, ITEM_Q_UNSUPPORTED
    except Exception:
        return None, ITEM_Q_UNREADABLE


def _read_first_attr(obj: Any, names: Sequence[str]) -> Tuple[Optional[Any], str, Optional[str]]:
    """Try multiple attribute names and return the first one found."""
    for name in names:
        try:
            return getattr(obj, name), ITEM_Q_OK, name
        except AttributeError:
            continue
        except Exception:
            return None, ITEM_Q_UNREADABLE, name
    return None, ITEM_Q_UNSUPPORTED, None


def _read_category_line_weight(cat: Any, style_type: Any, *, is_cut: bool) -> Tuple[Optional[str], str]:
    """Read Category line weight for projection/cut with fail-soft handling."""
    if GraphicsStyleType is None or style_type is None:
        return None, ITEM_Q_UNSUPPORTED
    try:
        val = cat.GetLineWeight(style_type)
    except Exception:
        return None, ITEM_Q_UNSUPPORTED if is_cut else ITEM_Q_UNREADABLE
    return canonicalize_int(val)


def _read_category_line_pattern_id(cat: Any, style_type: Any, *, is_cut: bool) -> Tuple[Optional[Any], str]:
    """Read Category line pattern ElementId for projection/cut."""
    if GraphicsStyleType is None or style_type is None:
        return None, ITEM_Q_UNSUPPORTED
    try:
        return cat.GetLinePatternId(style_type), ITEM_Q_OK
    except Exception:
        return None, ITEM_Q_UNSUPPORTED if is_cut else ITEM_Q_UNREADABLE


def _read_category_line_color(cat: Any, *, is_cut: bool) -> Tuple[Optional[str], str]:
    """Read Category line color for projection/cut."""
    try:
        if is_cut:
            color_obj = getattr(cat, "CutLineColor", None)
        else:
            color_obj = getattr(cat, "LineColor", None)
    except Exception:
        return None, ITEM_Q_UNSUPPORTED if is_cut else ITEM_Q_UNREADABLE
    return _rgb_from_color(color_obj)


def _read_category_fill_pattern_id(cat: Any, style_type: Any, *, is_cut: bool) -> Tuple[Optional[Any], str]:
    """Read Category fill pattern ElementId (if available in this API surface)."""
    if GraphicsStyleType is None or style_type is None:
        return None, ITEM_Q_UNSUPPORTED

    getter = getattr(cat, "GetFillPatternId", None)
    if getter is None:
        return None, ITEM_Q_UNSUPPORTED

    try:
        return getter(style_type), ITEM_Q_OK
    except Exception:
        return None, ITEM_Q_UNSUPPORTED if is_cut else ITEM_Q_UNREADABLE


def _read_category_fill_color(cat: Any, *, is_cut: bool) -> Tuple[Optional[str], str]:
    """Read Category fill color (if available in this API surface)."""
    attr = "CutFillColor" if is_cut else "FillColor"
    color_obj, q = _read_attr(cat, attr)
    if q != ITEM_Q_OK:
        return None, q
    return _rgb_from_color(color_obj)


def _resolve_pattern_sig_hash(
    doc: Any,
    pattern_id: Any,
    uid_map: Optional[Dict[str, str]],
) -> Tuple[Optional[str], str]:
    """Resolve a pattern ElementId to a sig_hash using a ctx uid->hash map."""
    if uid_map is None:
        return None, ITEM_Q_MISSING

    try:
        elem = doc.GetElement(pattern_id) if doc is not None else None
        uid = canon_str(getattr(elem, "UniqueId", None)) if elem is not None else None
    except Exception:
        return None, ITEM_Q_UNREADABLE

    if not uid:
        return None, ITEM_Q_UNREADABLE

    sig_hash = uid_map.get(uid)
    if sig_hash:
        return sig_hash, ITEM_Q_OK

    return None, ITEM_Q_MISSING


def _append_pattern_items(
    items: List[Dict[str, Any]],
    *,
    doc: Any,
    pattern_id: Any,
    key_prefix: str,
    uid_map: Optional[Dict[str, str]],
    solid_on_invalid: bool,
    invalid_means_missing: bool,
) -> None:
    """Append pattern_ref.* items for a line or fill pattern."""
    if pattern_id is None:
        if solid_on_invalid:
            kind_v, kind_q = canonicalize_str("solid")
            items.append(make_identity_item(f"{key_prefix}.kind", kind_v, kind_q))
        else:
            items.append(make_identity_item(f"{key_prefix}.sig_hash", None, ITEM_Q_MISSING))
        return

    if _is_invalid_element_id(pattern_id):
        if invalid_means_missing:
            items.append(make_identity_item(f"{key_prefix}.sig_hash", None, ITEM_Q_MISSING))
        elif solid_on_invalid:
            kind_v, kind_q = canonicalize_str("solid")
            items.append(make_identity_item(f"{key_prefix}.kind", kind_v, kind_q))
        else:
            items.append(make_identity_item(f"{key_prefix}.sig_hash", None, ITEM_Q_MISSING))
        return

    sig_hash_v, sig_hash_q = _resolve_pattern_sig_hash(doc, pattern_id, uid_map)
    items.append(make_identity_item(f"{key_prefix}.sig_hash", sig_hash_v, sig_hash_q))


def _append_color_item(items: List[Dict[str, Any]], key: str, color_obj: Any) -> None:
    """Append a color.rgb identity item."""
    rgb_v, rgb_q = _rgb_from_color(color_obj)
    items.append(make_identity_item(key, rgb_v, rgb_q))


def _append_value_item(
    items: List[Dict[str, Any]],
    key: str,
    raw_value: Any,
    *,
    canonicalizer,
    fallback_q: Optional[str] = None,
) -> None:
    """Append a canonicalized value to items with optional fallback quality."""
    if fallback_q is not None:
        items.append(make_identity_item(key, None, fallback_q))
        return

    v, q = canonicalizer(raw_value)
    items.append(make_identity_item(key, v, q))


def extract_projection_graphics(
    doc: Any,
    source: Any,
    ctx: Optional[Dict[str, Any]],
    key_prefix: str = "projection",
) -> List[Dict[str, Any]]:
    """Extract projection graphics from a Category or OverrideGraphicSettings.

    Args:
        doc: Revit document (needed to resolve ElementId -> UniqueId)
        source: Category or OverrideGraphicSettings
        ctx: context dict with sig_hash maps
        key_prefix: prefix for keys (default: "projection")

    Returns:
        List of IdentityItem dicts for projection line/color/pattern/fill.
    """
    items: List[Dict[str, Any]] = []
    line_pattern_map = (ctx or {}).get("line_pattern_uid_to_sig_hash_v2")
    fill_pattern_map = (ctx or {}).get("fill_pattern_uid_to_sig_hash_v2")

    if _is_category(source):
        # Category API (object styles domain)
        weight_v, weight_q = _read_category_line_weight(source, GraphicsStyleType.Projection if GraphicsStyleType else None, is_cut=False)
        items.append(make_identity_item(f"{key_prefix}.line_weight", weight_v, weight_q))

        color_v, color_q = _read_category_line_color(source, is_cut=False)
        items.append(make_identity_item(f"{key_prefix}.color.rgb", color_v, color_q))

        pattern_id, pattern_q = _read_category_line_pattern_id(source, GraphicsStyleType.Projection if GraphicsStyleType else None, is_cut=False)
        if pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.pattern_ref.sig_hash", None, pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=pattern_id,
                key_prefix=f"{key_prefix}.pattern_ref",
                uid_map=line_pattern_map,
                solid_on_invalid=True,
                invalid_means_missing=False,
            )

        fill_pattern_id, fill_pattern_q = _read_category_fill_pattern_id(
            source,
            GraphicsStyleType.Projection if GraphicsStyleType else None,
            is_cut=False,
        )
        if fill_pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.fill_pattern_ref.sig_hash", None, fill_pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=fill_pattern_id,
                key_prefix=f"{key_prefix}.fill_pattern_ref",
                uid_map=fill_pattern_map,
                solid_on_invalid=True,
                invalid_means_missing=False,
            )

        fill_color_v, fill_color_q = _read_category_fill_color(source, is_cut=False)
        items.append(make_identity_item(f"{key_prefix}.fill_color.rgb", fill_color_v, fill_color_q))
        return items

    if _is_ogs(source):
        # OverrideGraphicSettings API (view/template overrides)
        weight_raw, weight_q = _read_attr(source, "ProjectionLineWeight")
        if weight_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.line_weight", None, weight_q))
        else:
            _append_value_item(items, f"{key_prefix}.line_weight", weight_raw, canonicalizer=canonicalize_int)

        color_raw, color_q = _read_attr(source, "ProjectionLineColor")
        if color_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.color.rgb", None, color_q))
        else:
            _append_color_item(items, f"{key_prefix}.color.rgb", color_raw)

        pattern_raw, pattern_q = _read_attr(source, "ProjectionLinePatternId")
        if pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.pattern_ref.sig_hash", None, pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=pattern_raw,
                key_prefix=f"{key_prefix}.pattern_ref",
                uid_map=line_pattern_map,
                solid_on_invalid=False,
                invalid_means_missing=True,
            )

        fill_pattern_raw, fill_pattern_q, _fill_pattern_name = _read_first_attr(
            source,
            ["ProjectionFillPatternId", "SurfaceForegroundPatternId"],
        )
        if fill_pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.fill_pattern_ref.sig_hash", None, fill_pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=fill_pattern_raw,
                key_prefix=f"{key_prefix}.fill_pattern_ref",
                uid_map=fill_pattern_map,
                solid_on_invalid=False,
                invalid_means_missing=True,
            )

        fill_color_raw, fill_color_q, _fill_color_name = _read_first_attr(
            source,
            ["ProjectionFillColor", "SurfaceForegroundPatternColor"],
        )
        if fill_color_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.fill_color.rgb", None, fill_color_q))
        else:
            _append_color_item(items, f"{key_prefix}.fill_color.rgb", fill_color_raw)

        return items

    # Unknown source type
    for suffix in [
        "line_weight",
        "color.rgb",
        "pattern_ref.sig_hash",
        "fill_pattern_ref.sig_hash",
        "fill_color.rgb",
    ]:
        items.append(make_identity_item(f"{key_prefix}.{suffix}", None, ITEM_Q_UNSUPPORTED))
    return items


def extract_cut_graphics(
    doc: Any,
    source: Any,
    ctx: Optional[Dict[str, Any]],
    key_prefix: str = "cut",
) -> List[Dict[str, Any]]:
    """Extract cut graphics from a Category or OverrideGraphicSettings.

    Args:
        doc: Revit document (needed to resolve ElementId -> UniqueId)
        source: Category or OverrideGraphicSettings
        ctx: context dict with sig_hash maps
        key_prefix: prefix for keys (default: "cut")

    Returns:
        List of IdentityItem dicts for cut line/color/pattern/fill.
    """
    items: List[Dict[str, Any]] = []
    line_pattern_map = (ctx or {}).get("line_pattern_uid_to_sig_hash_v2")
    fill_pattern_map = (ctx or {}).get("fill_pattern_uid_to_sig_hash_v2")

    if _is_category(source):
        # Category cut properties are not supported for all categories (e.g., annotations).
        weight_v, weight_q = _read_category_line_weight(source, GraphicsStyleType.Cut if GraphicsStyleType else None, is_cut=True)
        items.append(make_identity_item(f"{key_prefix}.line_weight", weight_v, weight_q))

        color_v, color_q = _read_category_line_color(source, is_cut=True)
        items.append(make_identity_item(f"{key_prefix}.color.rgb", color_v, color_q))

        pattern_id, pattern_q = _read_category_line_pattern_id(source, GraphicsStyleType.Cut if GraphicsStyleType else None, is_cut=True)
        if pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.pattern_ref.sig_hash", None, pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=pattern_id,
                key_prefix=f"{key_prefix}.pattern_ref",
                uid_map=line_pattern_map,
                solid_on_invalid=True,
                invalid_means_missing=False,
            )

        fill_pattern_id, fill_pattern_q = _read_category_fill_pattern_id(
            source,
            GraphicsStyleType.Cut if GraphicsStyleType else None,
            is_cut=True,
        )
        if fill_pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.fill_pattern_ref.sig_hash", None, fill_pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=fill_pattern_id,
                key_prefix=f"{key_prefix}.fill_pattern_ref",
                uid_map=fill_pattern_map,
                solid_on_invalid=True,
                invalid_means_missing=False,
            )

        fill_color_v, fill_color_q = _read_category_fill_color(source, is_cut=True)
        items.append(make_identity_item(f"{key_prefix}.fill_color.rgb", fill_color_v, fill_color_q))
        return items

    if _is_ogs(source):
        weight_raw, weight_q = _read_attr(source, "CutLineWeight")
        if weight_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.line_weight", None, weight_q))
        else:
            _append_value_item(items, f"{key_prefix}.line_weight", weight_raw, canonicalizer=canonicalize_int)

        color_raw, color_q = _read_attr(source, "CutLineColor")
        if color_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.color.rgb", None, color_q))
        else:
            _append_color_item(items, f"{key_prefix}.color.rgb", color_raw)

        pattern_raw, pattern_q = _read_attr(source, "CutLinePatternId")
        if pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.pattern_ref.sig_hash", None, pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=pattern_raw,
                key_prefix=f"{key_prefix}.pattern_ref",
                uid_map=line_pattern_map,
                solid_on_invalid=False,
                invalid_means_missing=True,
            )

        fill_pattern_raw, fill_pattern_q, _fill_pattern_name = _read_first_attr(
            source,
            ["CutFillPatternId", "CutForegroundPatternId"],
        )
        if fill_pattern_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.fill_pattern_ref.sig_hash", None, fill_pattern_q))
        else:
            _append_pattern_items(
                items,
                doc=doc,
                pattern_id=fill_pattern_raw,
                key_prefix=f"{key_prefix}.fill_pattern_ref",
                uid_map=fill_pattern_map,
                solid_on_invalid=False,
                invalid_means_missing=True,
            )

        fill_color_raw, fill_color_q, _fill_color_name = _read_first_attr(
            source,
            ["CutFillColor", "CutForegroundPatternColor"],
        )
        if fill_color_q != ITEM_Q_OK:
            items.append(make_identity_item(f"{key_prefix}.fill_color.rgb", None, fill_color_q))
        else:
            _append_color_item(items, f"{key_prefix}.fill_color.rgb", fill_color_raw)

        return items

    for suffix in [
        "line_weight",
        "color.rgb",
        "pattern_ref.sig_hash",
        "fill_pattern_ref.sig_hash",
        "fill_color.rgb",
    ]:
        items.append(make_identity_item(f"{key_prefix}.{suffix}", None, ITEM_Q_UNSUPPORTED))
    return items


def extract_halftone(source: Any, key_prefix: str = "halftone") -> List[Dict[str, Any]]:
    """Extract halftone override from a Category or OverrideGraphicSettings."""
    items: List[Dict[str, Any]] = []

    if _is_category(source):
        items.append(make_identity_item(key_prefix, None, ITEM_Q_UNSUPPORTED))
        return items

    if _is_ogs(source):
        raw, q = _read_attr(source, "Halftone")
        if q != ITEM_Q_OK:
            items.append(make_identity_item(key_prefix, None, q))
        else:
            val, val_q = canonicalize_bool(raw)
            items.append(make_identity_item(key_prefix, val, val_q))
        return items

    items.append(make_identity_item(key_prefix, None, ITEM_Q_UNSUPPORTED))
    return items


def extract_transparency(source: Any, key_prefix: str = "transparency") -> List[Dict[str, Any]]:
    """Extract transparency override from a Category or OverrideGraphicSettings."""
    items: List[Dict[str, Any]] = []

    if _is_category(source):
        items.append(make_identity_item(key_prefix, None, ITEM_Q_UNSUPPORTED))
        return items

    if _is_ogs(source):
        raw, q = _read_attr(source, "Transparency")
        if q != ITEM_Q_OK:
            items.append(make_identity_item(key_prefix, None, q))
        else:
            val, val_q = canonicalize_int(raw)
            items.append(make_identity_item(key_prefix, val, val_q))
        return items

    items.append(make_identity_item(key_prefix, None, ITEM_Q_UNSUPPORTED))
    return items
