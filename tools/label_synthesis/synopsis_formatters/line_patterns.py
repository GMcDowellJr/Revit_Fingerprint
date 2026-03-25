"""
tools/label_synthesis/synopsis_formatters/line_patterns.py

Behavioral synopsis formatter for the line_patterns domain.

Produces labels like:
  "Dash | 1 seg"
  "Dash-Space | 2 seg"
  "Dash-Dot-Space | 3 seg"
  "Dash-Space-Dash-Space | 4 seg"
  "Dot-Space | 2 seg"
  "6 seg"   (fallback when pattern is complex)

Identity items available:
  line_pattern.segment_count   — integer string
  line_pattern.seg[NNN].kind   — 0=Dash, 1=Space, 2=Dot
  line_pattern.seg[NNN].length — decimal inches
  line_pattern.segments_def_hash    — opaque hash
  line_pattern.segments_norm_hash   — opaque hash (join key)
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import re

_SEG_KEY_RE = re.compile(r"^line_pattern\.seg\[(\d+)\]\.(kind|length)$")
_KIND_NAME = {0: "Dash", 1: "Space", 2: "Dot"}


def format_synopsis(identity_items: List[Dict[str, Any]]) -> Optional[str]:
    kv = {
        item["k"]: item["v"]
        for item in identity_items
        if isinstance(item, dict)
        and item.get("q") == "ok"
        and item.get("v") not in (None, "", "__missing__", "__na__", "__not_applicable__")
    }

    # Parse segments from indexed keys
    segments: Dict[int, Dict[str, Any]] = {}
    for k, v in kv.items():
        m = _SEG_KEY_RE.match(k)
        if not m:
            continue
        idx = int(m.group(1))
        field = m.group(2)
        segments.setdefault(idx, {})
        try:
            if field == "kind":
                segments[idx]["kind"] = int(v)
            else:
                segments[idx]["length"] = float(v)
        except (ValueError, TypeError):
            pass

    if not segments:
        # Fall back to segment_count only
        count_raw = kv.get("line_pattern.segment_count")
        if count_raw:
            try:
                return f"{int(count_raw)} seg"
            except (ValueError, TypeError):
                pass
        return None

    ordered = sorted(segments.items())
    seg_count = len(ordered)

    # Build kind sequence label
    kind_names = []
    for _, seg_data in ordered:
        kind_id = seg_data.get("kind")
        if kind_id is None:
            return f"{seg_count} seg"
        kind_names.append(_KIND_NAME.get(kind_id, f"k{kind_id}"))

    # Collapse repetitive patterns for brevity (e.g. D-S-D-S → "Dash-Space ×2")
    pattern_str = "-".join(kind_names)

    # Limit label length — complex patterns get count only
    if len(pattern_str) > 40:
        return f"{seg_count} seg"

    return f"{pattern_str} | {seg_count} seg"