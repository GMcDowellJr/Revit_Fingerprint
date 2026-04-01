#!/usr/bin/env python3
"""
Build semantic group labels for selected analysis domains.

This tool reads resolved pattern labels plus representative behavioral properties,
then calls an LLM (one call per pattern) to assign a governance-intent
`semantic_group` label. Results are cached in:

    Results_v21/label_synthesis/label_semantic_groups.json
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

SEMANTIC_GROUPING_DOMAINS = [
    "text_types",
    "arrowheads",
    "line_patterns",
    "line_styles",
    "fill_patterns_drafting",
    "fill_patterns_model",
]

CACHE_SCHEMA_VERSION = "1.0"
SYSTEM_PROMPT = """You are a Revit standards governance analyst at a large architecture and engineering firm. Your task is to assign a short semantic group label to a Revit configuration pattern. The label should capture what this configuration is *for* — its governance intent — not just describe its properties.

Governance groups are used by standards managers to identify where the firm has converged on common practice and where drift exists. A good group label answers the question: "What role does this configuration play in a Revit project?"

Rules:
- Return ONLY a JSON object with exactly three keys: "semantic_group", "confidence", and "rationale"
- "semantic_group": a lowercase label, 2–5 words, hyphen-separated if multi-word (e.g. "standard-note", "hidden-line", "concrete-fill")
- "confidence": exactly "high", "medium", or "low"
- "rationale": one sentence (max 20 words) explaining your grouping decision
- Do not add any text before or after the JSON object
- Do not use markdown code fences

If the pattern name or properties are too ambiguous to group with confidence, assign the closest plausible group and set confidence to "low"."""


def build_grouping_prompt(
    domain: str,
    pattern_label_human: str,
    behavioral_props: dict[str, str],
    peer_group_labels: list[str],
) -> str:
    """
    Build the user-turn prompt for the LLM grouping call.
    System prompt is passed separately.
    """
    if domain == "text_types":
        return _prompt_text_types(pattern_label_human, behavioral_props, peer_group_labels)
    if domain == "arrowheads":
        return _prompt_arrowheads(pattern_label_human, behavioral_props, peer_group_labels)
    if domain == "line_patterns":
        return _prompt_line_patterns(pattern_label_human, behavioral_props, peer_group_labels)
    if domain == "line_styles":
        return _prompt_line_styles(pattern_label_human, behavioral_props, peer_group_labels)
    if domain in ("fill_patterns_drafting", "fill_patterns_model"):
        return _prompt_fill_patterns(domain, pattern_label_human, behavioral_props, peer_group_labels)
    raise ValueError(f"No grouping prompt defined for domain: {domain}")


def _peer_block(peer_group_labels: list[str]) -> str:
    """Format the peer vocabulary block."""
    if not peer_group_labels:
        return "EXISTING GROUPS IN THIS DOMAIN: (none yet — you are establishing the vocabulary)"
    labels = sorted(set(peer_group_labels))
    lines = ["EXISTING GROUPS IN THIS DOMAIN (reuse these labels when appropriate):"]
    for label in labels:
        lines.append(f"  - {label}")
    lines.append("Only create a new label if none of the above fits.")
    return "\n".join(lines)


def _normalize_text_size(size_raw: str) -> str:
    """
    Convert decimal inch value to readable fraction string.
    Input may be "0.125000" or '1/8"' already.
    """
    if not size_raw:
        return "unknown"
    if '"' in size_raw or "/" in size_raw:
        return size_raw
    try:
        val = float(size_raw)
    except ValueError:
        return size_raw

    size_map = {
        0.046875: '3/64"',
        0.0625: '1/16"',
        0.078125: '5/64"',
        0.09375: '3/32"',
        0.109375: '7/64"',
        0.125: '1/8"',
        0.15625: '5/32"',
        0.1875: '3/16"',
        0.21875: '7/32"',
        0.25: '1/4"',
        0.3125: '5/16"',
        0.375: '3/8"',
        0.5: '1/2"',
        0.75: '3/4"',
        1.0: '1"',
        1.5: '1-1/2"',
        2.0: '2"',
        2.4: '2.4"',
        3.0: '3"',
    }

    for standard, label in size_map.items():
        if abs(val - standard) < 0.002:
            return label
    return f'{val:.3g}"'


def _parse_text_type_label_fields(pattern_label_human: str) -> tuple[Optional[str], Optional[str], list[str]]:
    tokens = [t.strip() for t in pattern_label_human.split("|") if t.strip()]
    if not tokens:
        return None, None, []

    format_keywords = {"bold", "regular", "underline", "border", "opaque", "italic", "bordered"}
    size_pattern = re.compile(r'^\d+(?:\.\d+)?"$|^\d+/\d+"$|^\d+-\d+/\d+"$')

    font = None
    size = None
    remaining: list[str] = []

    for token in tokens:
        lower = token.lower()
        if size is None and size_pattern.match(token):
            size = token
            continue
        if font is None and '"' not in token and '/' not in token and lower not in format_keywords:
            font = token
            continue
        remaining.append(token)

    return font, size, remaining


def _prompt_text_types(label: str, props: dict[str, str], peers: list[str]) -> str:
    size_raw = props.get("size_in", "")
    size_display = _normalize_text_size(size_raw)

    parsed_font, parsed_size, parsed_format_tokens = _parse_text_type_label_fields(label)

    font = parsed_font or props.get("font", "")
    if parsed_size:
        size_display = parsed_size

    bold = props.get("bold", "")
    italic = props.get("italic", "")
    color = props.get("color_rgb", "")
    show_border = props.get("show_border", "")
    background = props.get("background_raw", "")

    format_parts: list[str] = []
    if parsed_format_tokens:
        format_parts.extend(parsed_format_tokens)
    else:
        if bold == "True":
            format_parts.append("Bold")
        if italic == "True":
            format_parts.append("Italic")
        if show_border == "True":
            format_parts.append("Border")
        if background and background not in ("0", "transparent", ""):
            format_parts.append("Opaque")
        if not format_parts:
            format_parts.append("Regular")
    format_str = ", ".join(format_parts)

    color_note = ""
    if color and color not in ("0,0,0", "000000", "0"):
        color_note = f"  Color: {color} (non-black — may indicate special annotation role)\n"

    background_note = ""
    if background and background not in ("0", "transparent", ""):
        background_note = "  Background: opaque\n"

    return f"""PATTERN: Text Type

LABEL: {label}
FONT: {font}
SIZE: {size_display}
FORMAT: {format_str}
{color_note}{background_note}
CONTEXT:
Text types in Revit serve specific annotation roles. Size and format together indicate intended use:
- Very small (≤ 1/16"): reference numbers, keynote tags, room schedule callouts
- Small (3/32"–1/8"): standard general notes, typical body text for most annotation
- Medium (3/16"–1/4"): headings, sub-headings, zone labels
- Large (3/8"+): drawing titles, sheet titles, major callouts
- Very large (1"+): title block elements, cover sheet graphics
- Bold text at any size: headings, emphasis, title text
- Bordered text: revision clouds, special callout boxes, keynotes
- Non-black color: revision markup, coordination notes, discipline-specific annotation

Size and format define separate groups. A bold 1/8" type and a regular 1/8" type serve different governance roles even if the same font.

Firm naming conventions in Stantec projects often include:
- ".01_" or numeric prefixes: standard firm styles in preferred sort order
- "AR-", "ST-", "ME-": discipline prefixes (Architecture, Structural, MEP)
- "Title", "Note", "Tag", "Label", "Head": role suffixes

{_peer_block(peers)}

Assign a semantic group for this text type. Examples of valid group labels:
  standard-note (regular small annotation text, no border)
  bold-heading (bold text used for headings or section labels)
  drawing-title (large text for drawing/sheet titles)
  keynote-tag (small bordered or specially formatted text for keynotes)
  room-tag (text used in room/space tags)
  title-block-text (very large text for title block elements)
  revision-markup (non-black or bordered text for revision annotation)
  dimension-prefix (very small text used as dimension prefix/suffix)

Respond with ONLY the JSON object."""


def _prompt_arrowheads(label: str, props: dict[str, str], peers: list[str]) -> str:
    style = props.get("style", "")
    size = props.get("tick_size_in", "")
    filled = props.get("fill_tick", "")
    heavy_end = props.get("heavy_end_pen_weight", "")

    size_display = "unknown"
    if size:
        try:
            size_display = f'{float(size):.4f}"'
        except ValueError:
            size_display = size

    style_context = {
        "Arrow": "Standard arrow head. Used on leaders and annotation leaders. Filled vs open arrow indicates different conventions.",
        "Diagonal": "Diagonal tick mark. The dominant arrowhead style in architectural practice — standard for linear dimensions.",
        "Dot": "Dot arrowhead. Used on radial dimensions, spot elevations, or as an alternative leader terminator.",
        "Loop": "Loop arrowhead. Less common; used in some structural or specialty annotation conventions.",
        "Box": "Box terminator. Rare; used in specialty dimensions or imported from CAD standards.",
        "Heavy end tick mark": "Heavy end tick. Used in structural dimension conventions for emphasis.",
        "Datum triangle": "Datum triangle. Used on elevation markers and datum references.",
        "Elevation Target": "Elevation target symbol. Used on interior elevation markers.",
    }.get(style, f"Style: {style}")

    filled_note = ""
    if filled == "True":
        filled_note = "  Filled: yes\n"
    elif filled == "False":
        filled_note = "  Filled: no (open)\n"

    heavy_note = ""
    if heavy_end and heavy_end not in ("", "0", "None"):
        heavy_note = f"  Heavy end pen weight: {heavy_end}\n"

    return f"""PATTERN: Arrowhead Type

LABEL: {label}
STYLE: {style}
SIZE: {size_display}
{filled_note}{heavy_note}
CONTEXT:
{style_context}

Size matters within a style family: a large diagonal tick and a small diagonal tick may serve different dimension type families (e.g., a large tick for primary structural dimensions, small tick for interior dimensions).

{_peer_block(peers)}

Assign a semantic group for this arrowhead. Examples of valid group labels:
  diagonal-tick (standard architectural dimension tick, any size)
  filled-arrow (filled/closed arrowhead for leaders)
  open-arrow (open arrowhead for leaders)
  dot-terminator (dot used on radial dimensions or leaders)
  datum-marker (datum triangle or elevation target)
  structural-tick (heavy end or specialty tick for structural dimensions)
  loop-terminator (loop style for specialty annotation)

Respond with ONLY the JSON object."""


def _prompt_line_patterns(label: str, props: dict[str, str], peers: list[str]) -> str:
    seg_count = props.get("segment_count", "")

    return f"""PATTERN: Line Pattern

LABEL: {label}
SEGMENT COUNT: {seg_count if seg_count else "unknown"}

CONTEXT:
Line patterns in Revit are referenced by line styles (which add weight and color) and by object styles. The pattern itself defines only the dash/dot/space rhythm.

Key pattern families in architectural practice:
- Solid (0 segments): continuous line — used for visible edges, walls, most objects
- Dash patterns (Dash-Space, Dash-Space-Dash-Space): hidden lines, beyond-cut elements, dashed annotation
- Dash-dot patterns (Dash-Space-Dot-Space, Dash-Dot-Space): centerlines, grid lines, reference planes
- Dot patterns (Dot-Space, Dot-Space-Dot-Space): property lines, setback lines, cloud annotation
- Complex patterns (many segments, "N seg"): specialty patterns from CAD imports or custom definitions

The label encodes the segment sequence. "Dash-Space | 2 seg" is a simple hidden-line pattern. "Dash-Space-Dot-Space | 4 seg" is a centerline. Patterns with 6+ segments without a readable sequence ("8 seg", "12 seg") are likely complex or custom.

{_peer_block(peers)}

Assign a semantic group for this line pattern. Examples of valid group labels:
  solid (continuous line, no dashes)
  hidden-line (simple dash pattern, typically Dash-Space or Dash-Dash-Space)
  centerline (dash-dot pattern, Dash-Dot-Space or similar)
  property-line (dot or complex dot pattern)
  custom-complex (pattern with many segments, no standard sequence)
  annotation-dash (short dash pattern used for annotation clouds or borders)

Respond with ONLY the JSON object."""


def _prompt_line_styles(label: str, props: dict[str, str], peers: list[str]) -> str:
    weight = props.get("weight_projection", "")
    color = props.get("color_rgb", "")
    pattern_synopsis = props.get("pattern_synopsis", "[solid]")

    color_note = ""
    if color and color not in ("0,0,0", "000000", "0"):
        color_note = f"  Color: {color} (non-black)\n"

    try:
        w = int(weight)
        if w <= 2:
            weight_desc = f"LW{w} (hairline/fine)"
        elif w <= 4:
            weight_desc = f"LW{w} (light)"
        elif w <= 6:
            weight_desc = f"LW{w} (medium)"
        elif w <= 8:
            weight_desc = f"LW{w} (medium-heavy)"
        else:
            weight_desc = f"LW{w} (heavy)"
    except (ValueError, TypeError):
        weight_desc = str(weight)

    return f"""PATTERN: Line Style

LABEL: {label}
LINE WEIGHT: {weight_desc}
LINE PATTERN: {pattern_synopsis}
{color_note}
CONTEXT:
A line style combines weight + color + pattern. It is applied to model elements via object styles or directly to detail lines. The governance question is: what drawing convention does this line style represent?

Common line style roles in architectural production:
- Thin solid black: fine detail lines, text leaders, annotation work
- Medium solid black: standard visible edges, general linework
- Heavy solid black: section cuts, major element boundaries, walls in plan
- Dashed/hidden lines: elements below cut plane, hidden edges, dashed dimension lines
- Dash-dot (centerline weight): centerlines, grid lines, reference lines
- Colored lines: discipline-specific markup, coordination, phasing indicators
- Non-black solid: often phasing (demolished, new construction) or discipline color-coding

Firm naming conventions:
- "LW" prefix with number: pen weight designation (matches label format)
- Color in name: explicit color designation
- Pattern type in name: indicates intended use (Hidden, Center, Phantom)

{_peer_block(peers)}

Assign a semantic group for this line style. Examples of valid group labels:
  thin-solid (fine/hairline solid black line)
  medium-solid (standard weight solid black line)
  heavy-solid (heavy weight solid line, section cuts)
  hidden-line (dashed pattern, any weight)
  centerline (dash-dot or centerline pattern)
  colored-line (non-black line, discipline or phase marking)
  demolition-line (typically dashed, may be colored, phasing)
  annotation-line (thin lines for leaders, detail annotation)

Respond with ONLY the JSON object."""


def _prompt_fill_patterns(domain: str, label: str, props: dict[str, str], peers: list[str]) -> str:
    grid_count_raw = props.get("grid_count", "")
    spacing_raw = props.get("spacing_in", "")
    target = "Drafting" if "drafting" in domain else "Model"

    try:
        gc = int(grid_count_raw)
    except (ValueError, TypeError):
        gc = None

    is_solid = gc == 0 or (gc is None and not spacing_raw)

    if is_solid:
        complexity = "solid fill (no grid — completely filled region)"
        spacing_note = ""
    elif gc is not None and gc >= 10:
        complexity = f"complex pattern ({gc} grids — likely imported from PAT file)"
        spacing_note = "  The pattern geometry is too complex to summarize; name is the primary signal.\n"
    else:
        complexity = f"geometric pattern ({gc} grids)" if gc else "geometric pattern"
        if spacing_raw:
            try:
                sp = float(spacing_raw)
                if "drafting" in domain:
                    if sp <= 0.16:
                        density = 'fine (≤ 0.16" spacing)'
                    elif sp <= 0.39:
                        density = 'medium (0.16–0.39" spacing)'
                    else:
                        density = 'coarse (> 0.39" spacing)'
                else:
                    if sp <= 10:
                        density = 'fine (≤ 10" spacing)'
                    elif sp <= 31:
                        density = 'medium (10–31" spacing)'
                    else:
                        density = 'coarse (> 31" spacing)'
                spacing_note = f'  Spacing: {sp:.3f}" — {density}\n'
            except ValueError:
                spacing_note = f"  Spacing: {spacing_raw}\n"
        else:
            spacing_note = ""

    is_fallback = "join_key.v1" in label or "Variant" in label
    is_cad_import = ".dwg" in label or "-" in label.split(".")[-1].split("-")[-1]

    if is_fallback:
        label_note = "NOTE: The pattern name is a system-generated fallback — no human-readable name is available. Base your grouping decision primarily on the geometry (type and density).\n"
    elif is_cad_import:
        label_note = "NOTE: The pattern name appears to be derived from a CAD import (contains .dwg reference or import suffix). The name may not reflect Revit convention intent.\n"
    else:
        label_note = ""

    name_context = """
Known fill pattern naming conventions:
- Revit built-in patterns: "Concrete", "Earth", "Sand", "Gravel", "Diagonal Crosshatch", "Horizontal", "Vertical", "Steel", "Wood"
- Autodesk pattern prefixes: "AR-" (architectural hatches from AutoCAD: AR-CONC, AR-BRSTD, AR-SAND, AR-HBONE)
- ANSI patterns: "ANSI31" (steel/iron), "ANSI32" (steel), "ANSI33" (bronze), "ANSI34" (rubber/plastic), "ANSI35" (fire brick), "ANSI36" (marble/glass), "ANSI37" (lead/zinc), "ANSI38" (aluminum)
- Scale suffixes: "Small", "Medium", "Large", "Dense", "2mm", "4mm" — indicate pattern density variant
- Application suffixes: "(Cut)" vs "(Surface)" — indicates which surface the pattern applies to
- Custom firm patterns often use material names directly: "Concrete Block", "CMU", "Batt Insulation", "Rigid Insulation"
"""

    return f"""PATTERN: Fill Pattern ({target})

LABEL: {label}
COMPLEXITY: {complexity}
{spacing_note}{label_note}
CONTEXT:
{name_context}
Fill patterns are applied to cut or surface regions of building materials in Revit sections and plans. The semantic group should reflect the material or drawing convention the fill represents.

{_peer_block(peers)}

Assign a semantic group for this fill pattern. Examples of valid group labels:
  solid-fill (completely opaque fill, no pattern)
  concrete-hatch (concrete material pattern)
  earth-fill (earth/soil/grade material)
  insulation-batt (batt insulation wavy lines)
  insulation-rigid (rigid insulation diagonal lines)
  masonry-brick (brick coursing pattern)
  masonry-cmu (concrete block/CMU pattern)
  diagonal-line (simple diagonal line pattern, no specific material)
  crosshatch (crossing diagonal lines, steel or general)
  horizontal-line (horizontal line pattern)
  vertical-line (vertical line pattern)
  sand-gravel (sand or gravel aggregate pattern)
  wood-grain (wood grain or board pattern)
  metal-steel (steel or metal hatch, ANSI patterns)
  complex-import (complex PAT-file pattern, ungroupable by name alone)
  unknown-fill (fallback pattern with no usable name or geometry signal)

Respond with ONLY the JSON object."""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _load_analysis_run_id(analysis_dir: Path) -> str:
    manifest = analysis_dir / "analysis_manifest.csv"
    if not manifest.is_file():
        return ""
    rows = _read_csv_rows(manifest)
    if not rows:
        return ""
    return (rows[0].get("analysis_run_id") or "").strip()


def _load_cache(cache_path: Path) -> Dict[str, Any]:
    if not cache_path.is_file():
        return {
            "schema_version": CACHE_SCHEMA_VERSION,
            "analysis_run_id": "",
            "generated_at": "",
            "groups": {},
        }
    with cache_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {"schema_version": CACHE_SCHEMA_VERSION, "analysis_run_id": "", "generated_at": "", "groups": {}}
    data.setdefault("schema_version", CACHE_SCHEMA_VERSION)
    data.setdefault("analysis_run_id", "")
    data.setdefault("generated_at", "")
    groups = data.get("groups")
    data["groups"] = groups if isinstance(groups, dict) else {}
    return data


def _save_cache(cache_path: Path, cache: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True, ensure_ascii=False)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def _resolve_export_target(cache_path: Path, export_arg: Path) -> Path:
    """Place export files alongside label_semantic_groups.json."""
    return cache_path.parent / export_arg.name


def _write_export_batches(base_path: Path, prompts: list[Dict[str, str]], batch_size: Optional[int]) -> list[Path]:
    if not batch_size or batch_size <= 0:
        _write_json(base_path, prompts)
        return [base_path]

    written: list[Path] = []
    total = len(prompts)
    if total == 0:
        _write_json(base_path, prompts)
        return [base_path]

    stem = base_path.stem
    suffix = base_path.suffix or '.json'
    for idx, start in enumerate(range(0, total, batch_size), start=1):
        chunk = prompts[start:start + batch_size]
        chunk_path = base_path.with_name(f"{stem}.batch_{idx:03d}{suffix}")
        _write_json(chunk_path, chunk)
        written.append(chunk_path)
    return written


def _load_export_progress(path: Path) -> Dict[str, set[str]]:
    if not path.is_file():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    raw = data.get("exported_pattern_ids", {}) if isinstance(data, dict) else {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, set[str]] = {}
    for domain, values in raw.items():
        if isinstance(values, list):
            out[str(domain)] = {str(v).strip() for v in values if str(v).strip()}
    return out


def _save_export_progress(path: Path, progress: Dict[str, set[str]]) -> None:
    serializable = {
        "schema_version": "1.0",
        "updated_at": _utc_now_iso(),
        "exported_pattern_ids": {k: sorted(v) for k, v in sorted(progress.items())},
    }
    _write_json(path, serializable)


def _derive_element_label(domain: str, items: Dict[str, str], fallback: str) -> str:
    candidate_keys: Dict[str, List[str]] = {
        "text_types": ["text_type.name", "text_type.type_name", "text_type.label"],
        "arrowheads": ["arrowhead.name", "arrowhead.type_name", "arrowhead.label"],
        "line_patterns": ["line_pattern.name", "line_pattern.label"],
        "line_styles": ["line_style.name", "line_style.subcategory_name", "line_style.label"],
        "fill_patterns_drafting": ["fill_pattern.name", "fill_pattern.label"],
        "fill_patterns_model": ["fill_pattern.name", "fill_pattern.label"],
    }
    for key in candidate_keys.get(domain, []):
        val = (items.get(key) or "").strip()
        if val:
            return val
    return fallback


def _load_pattern_rows(analysis_dir: Path, only_domain: Optional[str]) -> Dict[str, List[Dict[str, str]]]:
    domain_patterns_csv = analysis_dir / "domain_patterns.csv"
    if not domain_patterns_csv.is_file():
        raise FileNotFoundError(f"Missing required input: {domain_patterns_csv}")
    rows = _read_csv_rows(domain_patterns_csv)
    out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    stats: Dict[str, int] = defaultdict(int)
    for row in rows:
        domain = (row.get("domain") or "").strip().lower()
        stats["rows_total"] += 1
        if domain not in SEMANTIC_GROUPING_DOMAINS:
            stats["rows_out_of_scope_domain"] += 1
            continue
        if only_domain and domain != only_domain:
            stats["rows_filtered_by_domain_arg"] += 1
            continue
        pattern_id = (row.get("pattern_id") or "").strip()
        label = (row.get("pattern_label_human") or "").strip()
        source = (row.get("pattern_label_source") or "").strip().lower()
        if not pattern_id:
            stats["rows_skipped_missing_pattern_id"] += 1
            continue
        if source == "missing":
            stats["rows_skipped_missing_source"] += 1
            continue
        if not label:
            stats["rows_skipped_blank_pattern_label_human"] += 1
            continue
        stats["rows_eligible"] += 1
        out[domain].append({
            "pattern_id": pattern_id,
            "pattern_label_human": label,
        })
    for domain in list(out.keys()):
        out[domain] = sorted(out[domain], key=lambda r: r["pattern_id"])
    print(f"[build_semantic_groups] domain_patterns scan stats: {dict(stats)}")
    return out


def _load_pattern_to_record_pk(analysis_dir: Path, domain: str) -> Dict[str, str]:
    membership_csv = analysis_dir / "record_pattern_membership.csv"
    if not membership_csv.is_file():
        raise FileNotFoundError(f"Missing required input: {membership_csv}")
    rows = _read_csv_rows(membership_csv)
    out: Dict[str, str] = {}
    for row in rows:
        if (row.get("domain") or "").strip().lower() != domain:
            continue
        pattern_id = (row.get("pattern_id") or "").strip()
        record_pk = (row.get("record_pk") or "").strip()
        if pattern_id and record_pk and pattern_id not in out:
            out[pattern_id] = record_pk
    return out


def _resolve_identity_items_source(phase0_dir: Path, shards_dir: Path, domain: str) -> Optional[Path]:
    shard_candidates = [
        shards_dir / f"{domain}.identity_items.csv",
        shards_dir / f"{domain}.csv",
    ]
    for candidate in shard_candidates:
        if candidate.is_file():
            return candidate
    fallback = phase0_dir / "phase0_identity_items.csv"
    if fallback.is_file():
        return fallback
    print(
        "[build_semantic_groups] WARN: missing identity-items source for domain "
        f"'{domain}'. looked for shard(s) and fallback: {fallback}"
    )
    return None


def _load_identity_items_by_record(phase0_dir: Path, shards_dir: Path, domain: str) -> Optional[Dict[str, Dict[str, str]]]:
    src_csv = _resolve_identity_items_source(phase0_dir, shards_dir, domain)
    if src_csv is None:
        return None
    rows = _read_csv_rows(src_csv)
    out: Dict[str, Dict[str, str]] = defaultdict(dict)
    for row in rows:
        if (row.get("domain") or "").strip().lower() != domain:
            continue
        record_pk = (row.get("record_pk") or "").strip()
        key = (row.get("k") or "").strip()
        value = (row.get("v") or "").strip()
        quality = (row.get("q") or "").strip()
        if not record_pk or not key or quality != "ok":
            continue
        if value:
            out[record_pk][key] = value
    print(f"[build_semantic_groups] domain={domain} identity_items_source={src_csv}")
    return out


def _line_pattern_segment_keys(items: Dict[str, str]) -> List[str]:
    keys = [k for k in items.keys() if k.startswith("line_pattern.seg[") and k.endswith("].kind")]
    return sorted(keys)


def _is_nullish(value: str) -> bool:
    v = value.strip().lower()
    return v in {"", "none", "null", "nil", "n/a", "na"}


def _extract_behavioral_props(domain: str, items: Dict[str, str]) -> Dict[str, str]:
    props: Dict[str, str] = {}
    if domain == "text_types":
        key_map = {
            "text_type.font": "font",
            "text_type.size_in": "size_in",
            "text_type.bold": "bold",
            "text_type.italic": "italic",
            "text_type.color_rgb": "color_rgb",
            "text_type.show_border": "show_border",
            "text_type.background_raw": "background_raw",
        }
        for src_key, dst_key in key_map.items():
            if items.get(src_key):
                props[dst_key] = items[src_key]
    elif domain == "arrowheads":
        key_map = {
            "arrowhead.style": "style",
            "arrowhead.tick_size_in": "tick_size_in",
            "arrowhead.filled": "fill_tick",
            "arrowhead.heavy_end_pen_weight": "heavy_end_pen_weight",
        }
        for src_key, dst_key in key_map.items():
            if items.get(src_key):
                props[dst_key] = items[src_key]
    elif domain == "line_patterns":
        if items.get("line_pattern.segment_count"):
            props["segment_count"] = items["line_pattern.segment_count"]
    elif domain == "line_styles":
        if items.get("line_style.color.rgb"):
            props["color_rgb"] = items["line_style.color.rgb"]
        if items.get("line_style.weight.projection"):
            props["weight_projection"] = items["line_style.weight.projection"]
        pattern_synopsis = (
            items.get("line_style.pattern_ref.pattern_label_human", "")
            or items.get("line_style.pattern_ref.label", "")
            or items.get("line_style.pattern_ref.synopsis", "")
        )
        if pattern_synopsis:
            props["pattern_synopsis"] = pattern_synopsis
        else:
            sig_hash = items.get("line_style.pattern_ref.sig_hash", "")
            props["pattern_synopsis"] = "[solid]" if _is_nullish(sig_hash) else sig_hash
    elif domain in {"fill_patterns_drafting", "fill_patterns_model"}:
        if items.get("fill_pattern.grid_count"):
            props["grid_count"] = items["fill_pattern.grid_count"]
        offset = items.get("fill_pattern.grid[000].offset", "")
        if offset:
            try:
                props["spacing_in"] = str(abs(float(offset)) * 12.0)
            except ValueError:
                props["spacing_in"] = offset
    return props


def _parse_grouping_response(raw_text: str) -> Dict[str, str]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text[: text.rfind("```")]
    try:
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("Expected object")
        semantic_group = str(parsed.get("semantic_group", "")).strip()
        confidence = str(parsed.get("confidence", "low")).strip().lower()
        rationale = str(parsed.get("rationale", "")).strip()
        if confidence not in {"high", "medium", "low"}:
            confidence = "low"
        if not semantic_group:
            semantic_group = "__parse_error__"
            confidence = "low"
            rationale = rationale or "Missing semantic_group in LLM response."
        return {
            "semantic_group": semantic_group,
            "confidence": confidence,
            "rationale": rationale,
        }
    except Exception:
        return {
            "semantic_group": "__parse_error__",
            "confidence": "low",
            "rationale": raw_text.strip(),
        }


def _normalize_import_payload(row: Dict[str, Any]) -> Dict[str, str]:
    semantic_group = str(row.get("semantic_group", "")).strip()
    confidence = str(row.get("confidence", "low")).strip().lower()
    rationale = str(row.get("rationale", "")).strip()
    if confidence not in {"high", "medium", "low"}:
        confidence = "low"
    if not semantic_group:
        semantic_group = "__parse_error__"
        confidence = "low"
        rationale = rationale or "Missing semantic_group in imported result."
    return {
        "semantic_group": semantic_group,
        "confidence": confidence,
        "rationale": rationale,
    }


def _call_grouping_llm(prompt: str) -> str:
    raise NotImplementedError("LLM call wiring for semantic grouping is not implemented yet.")


def build_semantic_groups(
    *,
    out_root: Path,
    domain: Optional[str],
    dry_run: bool,
    force_refresh: bool,
    max_patterns: Optional[int],
    export_prompts: Optional[Path],
    import_results: Optional[Path],
    peer_vocab_from_cache: bool,
    export_batch_size: Optional[int],
) -> None:
    if (out_root / "analysis_v21").is_dir() and (out_root / "phase0_v21").is_dir():
        results_v21 = out_root
    else:
        results_v21 = out_root / "Results_v21"
    analysis_dir = results_v21 / "analysis_v21"
    phase0_dir = results_v21 / "phase0_v21"
    shards_dir = phase0_dir / "identity_items_shards"
    if not shards_dir.is_dir():
        shards_dir = phase0_dir / "phase0_identity_items_by_domain"
    cache_path = results_v21 / "label_synthesis" / "label_semantic_groups.json"
    export_progress_path = results_v21 / "label_synthesis" / "prompt_export_progress.json"
    print(f"[build_semantic_groups] results_v21={results_v21}")
    print(f"[build_semantic_groups] analysis_dir={analysis_dir}")
    print(f"[build_semantic_groups] shards_dir={shards_dir}")
    print(f"[build_semantic_groups] cache_path={cache_path}")
    print(f"[build_semantic_groups] export_progress_path={export_progress_path}")

    if domain and domain not in SEMANTIC_GROUPING_DOMAINS:
        raise ValueError(f"--domain must be one of {SEMANTIC_GROUPING_DOMAINS}")
    if export_batch_size is not None and export_batch_size <= 0:
        raise ValueError("--export-batch-size must be a positive integer.")

    cache = _load_cache(cache_path)
    cache_groups = cache.get("groups", {})
    if not isinstance(cache_groups, dict):
        cache_groups = {}
        cache["groups"] = cache_groups

    analysis_run_id = _load_analysis_run_id(analysis_dir)
    for d in SEMANTIC_GROUPING_DOMAINS:
        if domain and d != domain:
            continue
        cache_groups.setdefault(d, {})

    if import_results:
        with import_results.open("r", encoding="utf-8") as f:
            rows = json.load(f)
        if not isinstance(rows, list):
            raise ValueError("--import-results must point to a JSON array.")
        imported = 0
        skipped = 0
        for row in rows:
            if not isinstance(row, dict):
                skipped += 1
                continue
            d = str(row.get("domain", "")).strip()
            pattern_id = str(row.get("pattern_id", "")).strip()
            if not d or not pattern_id:
                skipped += 1
                continue
            if domain and d != domain:
                skipped += 1
                continue
            if d not in SEMANTIC_GROUPING_DOMAINS:
                skipped += 1
                continue
            cache_groups.setdefault(d, {})
            if not force_refresh and pattern_id in cache_groups[d]:
                skipped += 1
                continue
            payload = _normalize_import_payload(row)
            cache_groups[d][pattern_id] = {
                "semantic_group": payload["semantic_group"],
                "confidence": payload["confidence"],
                "rationale": payload["rationale"],
                "pattern_label_human": str(row.get("pattern_label_human", "")).strip(),
                "reviewed": False,
            }
            imported += 1
        cache["schema_version"] = CACHE_SCHEMA_VERSION
        cache["analysis_run_id"] = analysis_run_id
        cache["generated_at"] = _utc_now_iso()
        cache["groups"] = cache_groups
        _save_cache(cache_path, cache)
        print(
            f"[build_semantic_groups] Imported {imported} results from {import_results} "
            f"(skipped={skipped}) and wrote cache: {cache_path}"
        )
        return

    patterns_by_domain = _load_pattern_rows(analysis_dir, domain)
    if not patterns_by_domain:
        print("[build_semantic_groups] WARN: no eligible patterns found in scope.")
        print("[build_semantic_groups] Check --out-root and ensure domain_patterns.csv has non-missing pattern_label_human/source.")

    exported_prompts: list[Dict[str, str]] = []
    export_progress = _load_export_progress(export_progress_path) if export_prompts else {}

    for d, pattern_rows in patterns_by_domain.items():
        if not pattern_rows:
            continue
        print(f"[build_semantic_groups] domain={d} eligible_patterns={len(pattern_rows)}")
        pattern_to_record = _load_pattern_to_record_pk(analysis_dir, d)
        identity_by_record = _load_identity_items_by_record(phase0_dir, shards_dir, d)
        if identity_by_record is None:
            continue

        print(f"[build_semantic_groups] domain={d} patterns={len(pattern_rows)}")
        processed = 0
        assigned_this_run: List[str] = []
        seeded_peer_vocab: List[str] = []
        if export_prompts and peer_vocab_from_cache:
            seeded_peer_vocab = sorted({
                str(entry.get("semantic_group", "")).strip()
                for entry in cache_groups.get(d, {}).values()
                if isinstance(entry, dict)
                and str(entry.get("semantic_group", "")).strip()
                and str(entry.get("semantic_group", "")).strip() != "__parse_error__"
            })
        previously_exported = export_progress.get(d, set()) if export_prompts and peer_vocab_from_cache else set()

        for row in pattern_rows:
            pattern_id = row["pattern_id"]
            pattern_label_human = row["pattern_label_human"]
            if not force_refresh and pattern_id in cache_groups[d]:
                continue
            if not force_refresh and pattern_id in previously_exported:
                continue
            if max_patterns is not None and processed >= max_patterns:
                break

            record_pk = pattern_to_record.get(pattern_id, "")
            identity_items = identity_by_record.get(record_pk, {}) if record_pk else {}
            behavioral_props = _extract_behavioral_props(d, identity_items)
            element_label = _derive_element_label(d, identity_items, pattern_label_human)
            peer_group_labels = sorted({g for g in assigned_this_run if g} | set(seeded_peer_vocab))

            if dry_run:
                print("\n--- semantic grouping prompt (dry-run) ---")
                print(json.dumps({
                    "domain": d,
                    "pattern_id": pattern_id,
                    "pattern_label_human": pattern_label_human,
                    "element_label": element_label,
                    "behavioral_props": behavioral_props,
                    "peer_group_labels": peer_group_labels,
                }, indent=2, ensure_ascii=False))
                response_payload = {
                    "semantic_group": "__dry_run__",
                    "confidence": "low",
                    "rationale": "Dry run; LLM call skipped.",
                }
            elif export_prompts:
                prompt = build_grouping_prompt(
                    domain=d,
                    pattern_label_human=pattern_label_human,
                    behavioral_props=behavioral_props,
                    peer_group_labels=peer_group_labels,
                )
                exported_prompts.append({
                    "pattern_id": pattern_id,
                    "domain": d,
                    "pattern_label_human": pattern_label_human,
                    "element_label": element_label,
                    "system_prompt": SYSTEM_PROMPT,
                    "user_prompt": prompt,
                })
                if peer_vocab_from_cache:
                    export_progress.setdefault(d, set()).add(pattern_id)
                response_payload = {
                    "semantic_group": "__exported__",
                    "confidence": "low",
                    "rationale": "Prompt exported; LLM call skipped.",
                }
            else:
                try:
                    prompt = build_grouping_prompt(
                        domain=d,
                        pattern_label_human=pattern_label_human,
                        behavioral_props=behavioral_props,
                        peer_group_labels=peer_group_labels,
                    )
                    raw_response = _call_grouping_llm(prompt)
                    response_payload = _parse_grouping_response(raw_response)
                except NotImplementedError as e:
                    response_payload = {
                        "semantic_group": "__parse_error__",
                        "confidence": "low",
                        "rationale": str(e),
                    }

            cache_groups[d][pattern_id] = {
                "semantic_group": response_payload["semantic_group"],
                "confidence": response_payload["confidence"],
                "rationale": response_payload["rationale"],
                "pattern_label_human": pattern_label_human,
                "reviewed": False,
            }
            group_value = response_payload["semantic_group"]
            if group_value and group_value not in {"__parse_error__", "__exported__", "__dry_run__"}:
                assigned_this_run.append(group_value)
            processed += 1

        print(f"[build_semantic_groups] domain={d} processed={processed}")

    if export_prompts:
        export_base_path = _resolve_export_target(cache_path, export_prompts)
        if export_prompts.parent != export_base_path.parent:
            print(
                "[build_semantic_groups] NOTE: export output is written beside label_semantic_groups.json at "
                f"{export_base_path.parent}"
            )
        written_paths = _write_export_batches(export_base_path, exported_prompts, export_batch_size)
        if peer_vocab_from_cache:
            _save_export_progress(export_progress_path, export_progress)
        print(
            f"[build_semantic_groups] Exported {len(exported_prompts)} prompts "
            f"into {len(written_paths)} file(s) under {export_base_path.parent}"
        )
        for path in written_paths:
            print(f"[build_semantic_groups]   - {path}")
        if peer_vocab_from_cache:
            print(
                "[build_semantic_groups] Resume tracking enabled: updated exported pattern progress at "
                f"{export_progress_path}"
            )
        print("[build_semantic_groups] Export mode: cache was not modified.")
        return

    cache["schema_version"] = CACHE_SCHEMA_VERSION
    cache["analysis_run_id"] = analysis_run_id
    cache["generated_at"] = _utc_now_iso()
    cache["groups"] = cache_groups
    _save_cache(cache_path, cache)
    print(f"[build_semantic_groups] wrote cache: {cache_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build semantic group labels for selected pattern domains.")
    ap.add_argument("--out-root", required=True, help="Path containing Results_v21/")
    ap.add_argument("--domain", choices=SEMANTIC_GROUPING_DOMAINS, default=None, help="Optional single domain.")
    ap.add_argument("--dry-run", action="store_true", help="Print prompt inputs; do not call LLM API.")
    ap.add_argument("--force-refresh", action="store_true", help="Regenerate groups even if cached.")
    ap.add_argument("--max-patterns", type=int, default=None, help="Limit patterns processed per domain.")
    ap.add_argument(
        "--export-prompts",
        default=None,
        help="Write assembled prompts to this JSON path instead of calling LLM and without writing cache.",
    )
    ap.add_argument(
        "--import-results",
        default=None,
        help="Import semantic-grouping results from a JSON array and write cache (no LLM calls).",
    )
    ap.add_argument(
        "--peer-vocab-from-cache",
        action="store_true",
        help=(
            "When used with --export-prompts, seed peer vocabulary from cached semantic_group labels and "
            "track exported pattern_ids to resume later batches without re-exporting prior items."
        ),
    )
    ap.add_argument(
        "--export-batch-size",
        type=int,
        default=None,
        help=(
            "When used with --export-prompts, split exported prompts into sequential JSON batches of this size "
            "in Results_v21/label_synthesis/."
        ),
    )
    args = ap.parse_args()

    if args.export_prompts and args.import_results:
        raise ValueError("--export-prompts and --import-results are mutually exclusive.")
    if args.peer_vocab_from_cache and not args.export_prompts:
        raise ValueError("--peer-vocab-from-cache can only be used with --export-prompts.")
    if args.dry_run and (args.export_prompts or args.import_results):
        raise ValueError("--dry-run cannot be combined with --export-prompts or --import-results.")
    if args.export_batch_size is not None and not args.export_prompts:
        raise ValueError("--export-batch-size can only be used with --export-prompts.")

    build_semantic_groups(
        out_root=Path(args.out_root).resolve(),
        domain=args.domain,
        dry_run=bool(args.dry_run),
        force_refresh=bool(args.force_refresh),
        max_patterns=args.max_patterns,
        export_prompts=Path(args.export_prompts).resolve() if args.export_prompts else None,
        import_results=Path(args.import_results).resolve() if args.import_results else None,
        peer_vocab_from_cache=bool(args.peer_vocab_from_cache),
        export_batch_size=args.export_batch_size,
    )


if __name__ == "__main__":
    main()
