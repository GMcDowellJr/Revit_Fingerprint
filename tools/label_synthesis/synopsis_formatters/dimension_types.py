"""
tools/label_synthesis/synopsis_formatters/dimension_types.py

Behavioral synopsis formatter for the dimension_types domain.

Produces a short, human-readable label from identity_items, e.g.:
  "Linear | 1/8\" acc | Arrow-Filled | w/gaps"
  "Radial | 1/32\" acc | Dot Small | center marks"
  "Angular | 1° acc | Open Arrow"

Identity items available (from domains/dimension_types.py):

  Common (all shapes):
    dim_type.shape              - "Linear", "Radial", "Angular", "SpotElevation", etc.
    dim_type.accuracy           - numeric string (inches), e.g. "0.125000"
    dim_type.unit_format_id     - unit system string
    dim_type.rounding           - rounding method
    dim_type.prefix             - prefix string
    dim_type.suffix             - suffix string
    dim_type.tick_sig_hash      - behavioral hash of arrowhead (opaque, used for identity only)

  Linear only:
    dim_type.witness_line_control - "Gap and Line", "Gap Only", etc.

  Radial only:
    dim_type.center_marks       - "0" / "1"
    dim_type.center_mark_size   - numeric inches

The synopsis picks 2-4 discriminating fields and formats them concisely.
It intentionally omits tick_sig_hash (opaque hash) and unit_format_id (verbose)
unless they are the only distinguishing signal.

Total target length: <= 45 characters for BI slicer legibility.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def format_synopsis(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    """
    Produce a short synopsis string from identity_items.

    Returns None if insufficient data to produce a meaningful label
    (caller will fall through to modal / LLM layer).
    """
    kv = _extract_kv(identity_items)
    if not kv:
        return None

    parts = []

    # --- Part 1: Shape (always first) ---
    shape_raw = kv.get("dim_type.shape", "")
    shape_label = _shape_label(shape_raw)
    if shape_label:
        parts.append(shape_label)

    # --- Part 2: Accuracy (the primary behavioral discriminator) ---
    acc_raw = kv.get("dim_type.accuracy", "")
    acc_label = _accuracy_label(acc_raw, shape_family=_shape_family(shape_raw))
    if acc_label:
        parts.append(acc_label)

    # --- Part 3: Shape-specific behavioral detail ---
    shape_family = _shape_family(shape_raw)

    if shape_family == "linear":
        wl = kv.get("dim_type.witness_line_control", "")
        wl_label = _witness_label(wl)
        if wl_label:
            parts.append(wl_label)

    elif shape_family == "radial":
        cm = kv.get("dim_type.center_marks", "")
        cm_label = _center_marks_label(cm)
        if cm_label:
            parts.append(cm_label)

    # --- Part 4: Prefix/suffix decoration (if non-empty) ---
    prefix = kv.get("dim_type.prefix", "").strip()
    suffix = kv.get("dim_type.suffix", "").strip()
    decoration = _decoration_label(prefix, suffix)
    if decoration:
        parts.append(decoration)

    if not parts:
        return None

    result = " | ".join(parts)

    # Hard truncation safety net — should not be needed with 4 short parts
    if len(result) > 50:
        result = result[:47] + "..."

    return result


# ---------------------------------------------------------------------------
# Field formatters
# ---------------------------------------------------------------------------

def _shape_label(raw: str) -> Optional[str]:
    """Map Revit dimension shape string to short label."""
    if not raw or raw in ("__missing__", "__unreadable__", "__na__"):
        return None
    _MAP = {
        "Linear":           "Linear",
        "LinearFixed":      "Linear Fixed",
        "Radial":           "Radial",
        "Diameter":         "Diameter",
        "DiameterLinked":   "Diameter",
        "Angular":          "Angular",
        "ArcLength":        "Arc Length",
        "SpotElevation":    "Spot Elev",
        "SpotCoordinate":   "Spot Coord",
        "SpotSlope":        "Spot Slope",
    }
    return _MAP.get(raw, raw)


def _shape_family(raw: str) -> str:
    """Return family bucket: 'linear', 'radial', 'angular', 'spot', or 'unknown'."""
    if not raw:
        return "unknown"
    if raw in ("Linear", "LinearFixed"):
        return "linear"
    if raw in ("Radial", "Diameter", "DiameterLinked"):
        return "radial"
    if raw in ("Angular", "ArcLength"):
        return "angular"
    if raw.startswith("Spot"):
        return "spot"
    return "unknown"


def _accuracy_label(raw: str, shape_family: str = "linear") -> Optional[str]:
    """
    Format accuracy value to a human label.
    Angular dimensions: value is degrees. All others: decimal inches.
    """
    if not raw or raw in ("__missing__", "__unreadable__", "__na__"):
        return None
    try:
        val = float(raw)
    except (ValueError, TypeError):
        return None

    # Angular shape — accuracy is in degrees
    if shape_family == "angular":
        _DEGREES = [
            (0.016667, "1'"),
            (0.25,     "0.25°"),
            (0.5,      "0.5°"),
            (1.0,      "1°"),
            (2.0,      "2°"),
            (5.0,      "5°"),
        ]
        for deg_val, label in _DEGREES:
            if abs(val - deg_val) < 1e-4:
                return label
        return f"{val}°"

    # Linear / radial / spot — decimal inches
    _FRACTIONS = [
        (1/64,  '1/64"'),
        (1/32,  '1/32"'),
        (1/16,  '1/16"'),
        (1/8,   '1/8"'),
        (3/16,  '3/16"'),
        (1/4,   '1/4"'),
        (3/8,   '3/8"'),
        (1/2,   '1/2"'),
        (3/4,   '3/4"'),
        (1.0,   '1"'),
        (2.0,   '2"'),
    ]
    for frac_val, label in _FRACTIONS:
        if abs(val - frac_val) < 1e-6:
            return label
    if val < 0.01:
        return f"{val:.4f}\""
    if val < 0.1:
        return f"{val:.3f}\""
    return f"{val:.2f}\""

def _witness_label(raw: str) -> Optional[str]:
    """Shorten witness line control values."""
    if not raw or raw in ("__missing__", "__unreadable__", "__na__", "__not_applicable__"):
        return None
    _MAP = {
        "Gap and Line":  "w/ gaps",
        "Gap Only":      "gap only",
        "No Gap":        "no gap",
        "":              None,
    }
    if raw in _MAP:
        return _MAP[raw]
    # Passthrough abbreviated (truncate long values)
    return raw[:15] if len(raw) > 15 else raw


def _center_marks_label(raw: str) -> Optional[str]:
    """Format center marks enabled/disabled."""
    if not raw or raw in ("__missing__", "__unreadable__", "__na__", "__not_applicable__"):
        return None
    if raw in ("1", "true", "True"):
        return "ctr marks"
    if raw in ("0", "false", "False"):
        return "no ctr marks"
    return None


def _decoration_label(prefix: str, suffix: str) -> Optional[str]:
    """Format prefix/suffix decoration into a compact label."""
    if not prefix and not suffix:
        return None
    # Skip uninformative defaults
    _SKIP = {"", " ", "—", "-", "__missing__", "__na__"}
    p = prefix if prefix not in _SKIP else ""
    s = suffix if suffix not in _SKIP else ""
    if p and s:
        return f'"{p}…{s}"'
    if p:
        return f'"{p}…"'
    if s:
        return f'"…{s}"'
    return None


# ---------------------------------------------------------------------------
# KV extraction helper
# ---------------------------------------------------------------------------

def _extract_kv(identity_items: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Flatten identity_items list into {k: v} dict.
    Only includes items with q == "ok" and non-null v.
    """
    result = {}
    for item in identity_items:
        if not isinstance(item, dict):
            continue
        q = item.get("q", "ok")
        if q != "ok":
            continue
        k = item.get("k", "")
        v = item.get("v", None)
        if k and v is not None:
            result[k] = str(v)
    return result
