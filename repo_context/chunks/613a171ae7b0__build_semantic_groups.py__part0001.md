# Chunk of tools/label_synthesis/build_semantic_groups.py

- Source relative path: `tools/label_synthesis/build_semantic_groups.py`
- Chunk: 1 of 3
- Original line range: 1-431
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_grouping_prompt, _peer_block, _normalize_text_size, _parse_text_type_label_fields, _prompt_text_types, _prompt_arrowheads, _prompt_line_patterns, _prompt_line_styles, _normalise_fill_angle, _is_fill_angle_close, _infer_fill_geometry_description
- Source SHA-256: ecbac527e320e64b586fce64b729698632c2ac0daced16f12b6c615ec9265668
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| #!/usr/bin/env python3
     2| """
     3| Build semantic group labels for selected analysis domains.
     4| 
     5| This tool reads resolved pattern labels plus representative behavioral properties,
     6| then calls an LLM (one call per pattern) to assign a governance-intent
     7| `semantic_group` label. Results are cached in:
     8| 
     9|     Results_v21/label_synthesis/label_semantic_groups.json
    10| """
    11| 
    12| from __future__ import annotations
    13| 
    14| import argparse
    15| import csv
    16| import json
    17| import re
    18| from collections import defaultdict
    19| from datetime import datetime, timezone
    20| from pathlib import Path
    21| from typing import Any, Dict, List, Optional
    22| 
    23| SEMANTIC_GROUPING_DOMAINS = [
    24|     "text_types",
    25|     "arrowheads",
    26|     "line_patterns",
    27|     "line_styles",
    28|     "fill_patterns_drafting",
    29|     "fill_patterns_model",
    30| ]
    31| 
    32| CACHE_SCHEMA_VERSION = "1.0"
    33| SYSTEM_PROMPT = """You are a Revit standards governance analyst at a large architecture and engineering firm. Your task is to assign a short semantic group label to a Revit configuration pattern. The label should capture what this configuration is *for* — its governance intent — not just describe its properties.
    34| 
    35| Governance groups are used by standards managers to identify where the firm has converged on common practice and where drift exists. A good group label answers the question: "What role does this configuration play in a Revit project?"
    36| 
    37| Rules:
    38| - Return ONLY a JSON object with exactly three keys: "semantic_group", "confidence", and "rationale"
    39| - "semantic_group": a lowercase label, 2–5 words, hyphen-separated if multi-word (e.g. "standard-note", "hidden-line", "concrete-fill")
    40| - "confidence": exactly "high", "medium", or "low"
    41| - "rationale": one sentence (max 20 words) explaining your grouping decision
    42| - Do not add any text before or after the JSON object
    43| - Do not use markdown code fences
    44| 
    45| If the pattern name or properties are too ambiguous to group with confidence, assign the closest plausible group and set confidence to "low"."""
    46| 
    47| 
    48| def build_grouping_prompt(
    49|     domain: str,
    50|     pattern_label_human: str,
    51|     behavioral_props: dict[str, str],
    52|     peer_group_labels: list[str],
    53| ) -> str:
    54|     """
    55|     Build the user-turn prompt for the LLM grouping call.
    56|     System prompt is passed separately.
    57|     """
    58|     if domain == "text_types":
    59|         return _prompt_text_types(pattern_label_human, behavioral_props, peer_group_labels)
    60|     if domain == "arrowheads":
    61|         return _prompt_arrowheads(pattern_label_human, behavioral_props, peer_group_labels)
    62|     if domain == "line_patterns":
    63|         return _prompt_line_patterns(pattern_label_human, behavioral_props, peer_group_labels)
    64|     if domain == "line_styles":
    65|         return _prompt_line_styles(pattern_label_human, behavioral_props, peer_group_labels)
    66|     if domain in ("fill_patterns_drafting", "fill_patterns_model"):
    67|         return _prompt_fill_patterns(domain, pattern_label_human, behavioral_props, peer_group_labels)
    68|     raise ValueError(f"No grouping prompt defined for domain: {domain}")
    69| 
    70| 
    71| def _peer_block(peer_group_labels: list[str]) -> str:
    72|     """Format the peer vocabulary block."""
    73|     if not peer_group_labels:
    74|         return "EXISTING GROUPS IN THIS DOMAIN: (none yet — you are establishing the vocabulary)"
    75|     labels = sorted(set(peer_group_labels))
    76|     lines = ["EXISTING GROUPS IN THIS DOMAIN (reuse these labels when appropriate):"]
    77|     for label in labels:
    78|         lines.append(f"  - {label}")
    79|     lines.append("Only create a new label if none of the above fits.")
    80|     return "\n".join(lines)
    81| 
    82| 
    83| def _normalize_text_size(size_raw: str) -> str:
    84|     """
    85|     Convert decimal inch value to readable fraction string.
    86|     Input may be "0.125000" or '1/8"' already.
    87|     """
    88|     if not size_raw:
    89|         return "unknown"
    90|     if '"' in size_raw or "/" in size_raw:
    91|         return size_raw
    92|     try:
    93|         val = float(size_raw)
    94|     except ValueError:
    95|         return size_raw
    96| 
    97|     size_map = {
    98|         0.046875: '3/64"',
    99|         0.0625: '1/16"',
   100|         0.078125: '5/64"',
   101|         0.09375: '3/32"',
   102|         0.109375: '7/64"',
   103|         0.125: '1/8"',
   104|         0.15625: '5/32"',
   105|         0.1875: '3/16"',
   106|         0.21875: '7/32"',
   107|         0.25: '1/4"',
   108|         0.3125: '5/16"',
   109|         0.375: '3/8"',
   110|         0.5: '1/2"',
   111|         0.75: '3/4"',
   112|         1.0: '1"',
   113|         1.5: '1-1/2"',
   114|         2.0: '2"',
   115|         2.4: '2.4"',
   116|         3.0: '3"',
   117|     }
   118| 
   119|     for standard, label in size_map.items():
   120|         if abs(val - standard) < 0.002:
   121|             return label
   122|     return f'{val:.3g}"'
   123| 
   124| 
   125| def _parse_text_type_label_fields(pattern_label_human: str) -> tuple[Optional[str], Optional[str], list[str]]:
   126|     tokens = [t.strip() for t in pattern_label_human.split("|") if t.strip()]
   127|     if not tokens:
   128|         return None, None, []
   129| 
   130|     format_keywords = {"bold", "regular", "underline", "border", "opaque", "italic", "bordered"}
   131|     size_pattern = re.compile(r'^\d+(?:\.\d+)?"$|^\d+/\d+"$|^\d+-\d+/\d+"$')
   132| 
   133|     font = None
   134|     size = None
   135|     remaining: list[str] = []
   136| 
   137|     for token in tokens:
   138|         lower = token.lower()
   139|         if size is None and size_pattern.match(token):
   140|             size = token
   141|             continue
   142|         if font is None and '"' not in token and '/' not in token and lower not in format_keywords:
   143|             font = token
   144|             continue
   145|         remaining.append(token)
   146| 
   147|     return font, size, remaining
   148| 
   149| 
   150| def _prompt_text_types(label: str, props: dict[str, str], peers: list[str]) -> str:
   151|     size_raw = props.get("size_in", "")
   152|     size_display = _normalize_text_size(size_raw)
   153| 
   154|     parsed_font, parsed_size, parsed_format_tokens = _parse_text_type_label_fields(label)
   155| 
   156|     font = parsed_font or props.get("font", "")
   157|     if parsed_size:
   158|         size_display = parsed_size
   159| 
   160|     bold = props.get("bold", "")
   161|     italic = props.get("italic", "")
   162|     color = props.get("color_rgb", "")
   163|     show_border = props.get("show_border", "")
   164|     background = props.get("background_raw", "")
   165| 
   166|     format_parts: list[str] = []
   167|     if parsed_format_tokens:
   168|         format_parts.extend(parsed_format_tokens)
   169|     else:
   170|         if bold == "True":
   171|             format_parts.append("Bold")
   172|         if italic == "True":
   173|             format_parts.append("Italic")
   174|         if show_border == "True":
   175|             format_parts.append("Border")
   176|         if background and background not in ("0", "transparent", ""):
   177|             format_parts.append("Opaque")
   178|         if not format_parts:
   179|             format_parts.append("Regular")
   180|     format_str = ", ".join(format_parts)
   181| 
   182|     color_note = ""
   183|     if color and color not in ("0,0,0", "000000", "0"):
   184|         color_note = f"  Color: {color} (non-black — may indicate special annotation role)\n"
   185| 
   186|     background_note = ""
   187|     if background and background not in ("0", "transparent", ""):
   188|         background_note = "  Background: opaque\n"
   189| 
   190|     return f"""PATTERN: Text Type
   191| 
   192| LABEL: {label}
   193| FONT: {font}
   194| SIZE: {size_display}
   195| FORMAT: {format_str}
   196| {color_note}{background_note}
   197| CONTEXT:
   198| Text types in Revit serve specific annotation roles. Size and format together indicate intended use:
   199| - Very small (≤ 1/16"): reference numbers, keynote tags, room schedule callouts
   200| - Small (3/32"–1/8"): standard general notes, typical body text for most annotation
   201| - Medium (3/16"–1/4"): headings, sub-headings, zone labels
   202| - Large (3/8"+): drawing titles, sheet titles, major callouts
   203| - Very large (1"+): title block elements, cover sheet graphics
   204| - Bold text at any size: headings, emphasis, title text
   205| - Bordered text: revision clouds, special callout boxes, keynotes
   206| - Non-black color: revision markup, coordination notes, discipline-specific annotation
   207| 
   208| Size and format define separate groups. A bold 1/8" type and a regular 1/8" type serve different governance roles even if the same font.
   209| 
   210| Enterprise naming conventions in deployment projects often include:
   211| - ".01_" or numeric prefixes: standard firm styles in preferred sort order
   212| - "AR-", "ST-", "ME-": discipline prefixes (Architecture, Structural, MEP)
   213| - "Title", "Note", "Tag", "Label", "Head": role suffixes
   214| 
   215| {_peer_block(peers)}
   216| 
   217| Assign a semantic group for this text type. Examples of valid group labels:
   218|   standard-note (regular small annotation text, no border)
   219|   bold-heading (bold text used for headings or section labels)
   220|   drawing-title (large text for drawing/sheet titles)
   221|   keynote-tag (small bordered or specially formatted text for keynotes)
   222|   room-tag (text used in room/space tags)
   223|   title-block-text (very large text for title block elements)
   224|   revision-markup (non-black or bordered text for revision annotation)
   225|   dimension-prefix (very small text used as dimension prefix/suffix)
   226| 
   227| Respond with ONLY the JSON object."""
   228| 
   229| 
   230| def _prompt_arrowheads(label: str, props: dict[str, str], peers: list[str]) -> str:
   231|     style = props.get("style", "")
   232|     size = props.get("tick_size_in", "")
   233|     filled = props.get("fill_tick", "")
   234|     heavy_end = props.get("heavy_end_pen_weight", "")
   235| 
   236|     size_display = "unknown"
   237|     if size:
   238|         try:
   239|             size_display = f'{float(size):.4f}"'
   240|         except ValueError:
   241|             size_display = size
   242| 
   243|     style_context = {
   244|         "Arrow": "Standard arrow head. Used on leaders and annotation leaders. Filled vs open arrow indicates different conventions.",
   245|         "Diagonal": "Diagonal tick mark. The dominant arrowhead style in architectural practice — standard for linear dimensions.",
   246|         "Dot": "Dot arrowhead. Used on radial dimensions, spot elevations, or as an alternative leader terminator.",
   247|         "Loop": "Loop arrowhead. Less common; used in some structural or specialty annotation conventions.",
   248|         "Box": "Box terminator. Rare; used in specialty dimensions or imported from CAD standards.",
   249|         "Heavy end tick mark": "Heavy end tick. Used in structural dimension conventions for emphasis.",
   250|         "Datum triangle": "Datum triangle. Used on elevation markers and datum references.",
   251|         "Elevation Target": "Elevation target symbol. Used on interior elevation markers.",
   252|     }.get(style, f"Style: {style}")
   253| 
   254|     filled_note = ""
   255|     if filled == "True":
   256|         filled_note = "  Filled: yes\n"
   257|     elif filled == "False":
   258|         filled_note = "  Filled: no (open)\n"
   259| 
   260|     heavy_note = ""
   261|     if heavy_end and heavy_end not in ("", "0", "None"):
   262|         heavy_note = f"  Heavy end pen weight: {heavy_end}\n"
   263| 
   264|     return f"""PATTERN: Arrowhead Type
   265| 
   266| LABEL: {label}
   267| STYLE: {style}
   268| SIZE: {size_display}
   269| {filled_note}{heavy_note}
   270| CONTEXT:
   271| {style_context}
   272| 
   273| Size matters within a style family: a large diagonal tick and a small diagonal tick may serve different dimension type families (e.g., a large tick for primary structural dimensions, small tick for interior dimensions).
   274| 
   275| {_peer_block(peers)}
   276| 
   277| Assign a semantic group for this arrowhead. Examples of valid group labels:
   278|   diagonal-tick (standard architectural dimension tick, any size)
   279|   filled-arrow (filled/closed arrowhead for leaders)
   280|   open-arrow (open arrowhead for leaders)
   281|   dot-terminator (dot used on radial dimensions or leaders)
   282|   datum-marker (datum triangle or elevation target)
   283|   structural-tick (heavy end or specialty tick for structural dimensions)
   284|   loop-terminator (loop style for specialty annotation)
   285| 
   286| Respond with ONLY the JSON object."""
   287| 
   288| 
   289| def _prompt_line_patterns(label: str, props: dict[str, str], peers: list[str]) -> str:
   290|     seg_count = props.get("segment_count", "")
   291| 
   292|     return f"""PATTERN: Line Pattern
   293| 
   294| LABEL: {label}
   295| SEGMENT COUNT: {seg_count if seg_count else "unknown"}
   296| 
   297| CONTEXT:
   298| Line patterns in Revit are referenced by line styles (which add weight and color) and by object styles. The pattern itself defines only the dash/dot/space rhythm.
   299| 
   300| Key pattern families in architectural practice:
   301| - Solid (0 segments): continuous line — used for visible edges, walls, most objects
   302| - Dash patterns (Dash-Space, Dash-Space-Dash-Space): hidden lines, beyond-cut elements, dashed annotation
   303| - Dash-dot patterns (Dash-Space-Dot-Space, Dash-Dot-Space): centerlines, grid lines, reference planes
   304| - Dot patterns (Dot-Space, Dot-Space-Dot-Space): property lines, setback lines, cloud annotation
   305| - Complex patterns (many segments, "N seg"): specialty patterns from CAD imports or custom definitions
   306| 
   307| The label encodes the segment sequence. "Dash-Space | 2 seg" is a simple hidden-line pattern. "Dash-Space-Dot-Space | 4 seg" is a centerline. Patterns with 6+ segments without a readable sequence ("8 seg", "12 seg") are likely complex or custom.
   308| 
   309| {_peer_block(peers)}
   310| 
   311| Assign a semantic group for this line pattern. Examples of valid group labels:
   312|   solid (continuous line, no dashes)
   313|   hidden-line (simple dash pattern, typically Dash-Space or Dash-Dash-Space)
   314|   centerline (dash-dot pattern, Dash-Dot-Space or similar)
   315|   property-line (dot or complex dot pattern)
   316|   custom-complex (pattern with many segments, no standard sequence)
   317|   annotation-dash (short dash pattern used for annotation clouds or borders)
   318| 
   319| Respond with ONLY the JSON object."""
   320| 
   321| 
   322| def _prompt_line_styles(label: str, props: dict[str, str], peers: list[str]) -> str:
   323|     weight = props.get("weight_projection", "")
   324|     color = props.get("color_rgb", "")
   325|     pattern_synopsis = props.get("pattern_synopsis", "[solid]")
   326| 
   327|     color_note = ""
   328|     if color and color not in ("0,0,0", "000000", "0"):
   329|         color_note = f"  Color: {color} (non-black)\n"
   330| 
   331|     try:
   332|         w = int(weight)
   333|         if w <= 2:
   334|             weight_desc = f"LW{w} (hairline/fine)"
   335|         elif w <= 4:
   336|             weight_desc = f"LW{w} (light)"
   337|         elif w <= 6:
   338|             weight_desc = f"LW{w} (medium)"
   339|         elif w <= 8:
   340|             weight_desc = f"LW{w} (medium-heavy)"
   341|         else:
   342|             weight_desc = f"LW{w} (heavy)"
   343|     except (ValueError, TypeError):
   344|         weight_desc = str(weight)
   345| 
   346|     return f"""PATTERN: Line Style
   347| 
   348| LABEL: {label}
   349| LINE WEIGHT: {weight_desc}
   350| LINE PATTERN: {pattern_synopsis}
   351| {color_note}
   352| CONTEXT:
   353| A line style combines weight + color + pattern. It is applied to model elements via object styles or directly to detail lines. The governance question is: what drawing convention does this line style represent?
   354| 
   355| Common line style roles in architectural production:
   356| - Thin solid black: fine detail lines, text leaders, annotation work
   357| - Medium solid black: standard visible edges, general linework
   358| - Heavy solid black: section cuts, major element boundaries, walls in plan
   359| - Dashed/hidden lines: elements below cut plane, hidden edges, dashed dimension lines
   360| - Dash-dot (centerline weight): centerlines, grid lines, reference lines
   361| - Colored lines: discipline-specific markup, coordination, phasing indicators
   362| - Non-black solid: often phasing (demolished, new construction) or discipline color-coding
   363| 
   364| Firm naming conventions:
   365| - "LW" prefix with number: pen weight designation (matches label format)
   366| - Color in name: explicit color designation
   367| - Pattern type in name: indicates intended use (Hidden, Center, Phantom)
   368| 
   369| {_peer_block(peers)}
   370| 
   371| Assign a semantic group for this line style. Examples of valid group labels:
   372|   thin-solid (fine/hairline solid black line)
   373|   medium-solid (standard weight solid black line)
   374|   heavy-solid (heavy weight solid line, section cuts)
   375|   hidden-line (dashed pattern, any weight)
   376|   centerline (dash-dot or centerline pattern)
   377|   colored-line (non-black line, discipline or phase marking)
   378|   demolition-line (typically dashed, may be colored, phasing)
   379|   annotation-line (thin lines for leaders, detail annotation)
   380| 
   381| Respond with ONLY the JSON object."""
   382| 
   383| 
   384| def _normalise_fill_angle(deg: float) -> float:
   385|     return deg % 180.0
   386| 
   387| 
   388| def _is_fill_angle_close(value: float, target: float, tol: float = 5.0) -> bool:
   389|     value_n = _normalise_fill_angle(value)
   390|     target_n = _normalise_fill_angle(target)
   391|     diff = abs(value_n - target_n)
   392|     circular_diff = min(diff, 180.0 - diff)
   393|     return circular_diff <= tol
   394| 
   395| 
   396| def _infer_fill_geometry_description(grid_count: Optional[int], angles: List[float]) -> str:
   397|     """Derive an orientation description from grid count + angle, only ever
   398|     using fields actually present in props (grid_count, grid_angles)."""
   399|     if not angles:
   400|         return "geometry unknown (no angle data available)"
   401|     if grid_count == 1 or (grid_count is None and len(angles) == 1):
   402|         a0 = _normalise_fill_angle(angles[0])
   403|         if _is_fill_angle_close(a0, 0):
   404|             return "single-grid horizontal lines"
   405|         if _is_fill_angle_close(a0, 90):
   406|             return "single-grid vertical lines"
   407|         if _is_fill_angle_close(a0, 45):
   408|             return "single-grid diagonal lines (45°)"
   409|         if _is_fill_angle_close(a0, 135):
   410|             return "single-grid diagonal lines (135°/-45°)"
   411|         return f"single-grid lines at {a0:.1f}°"
   412|     if grid_count == 2 or (grid_count is None and len(angles) == 2):
   413|         if len(angles) < 2:
   414|             return "two-grid pattern, second angle unknown"
   415|         a0, a1 = _normalise_fill_angle(angles[0]), _normalise_fill_angle(angles[1])
   416|         diag_pair = (_is_fill_angle_close(a0, 45) and _is_fill_angle_close(a1, 135)) or (
   417|             _is_fill_angle_close(a1, 45) and _is_fill_angle_close(a0, 135)
   418|         )
   419|         if diag_pair:
   420|             return "two-grid crosshatch (opposing diagonals)"
   421|         hv_pair = (_is_fill_angle_close(a0, 0) and _is_fill_angle_close(a1, 90)) or (
   422|             _is_fill_angle_close(a1, 0) and _is_fill_angle_close(a0, 90)
   423|         )
   424|         if hv_pair:
   425|             return "two-grid net (horizontal + vertical)"
   426|         return f"two-grid pattern ({a0:.1f}° + {a1:.1f}°)"
   427|     if grid_count and grid_count > 2:
   428|         return f"complex pattern ({grid_count} grids, angle data incomplete)"
   429|     return "geometry undetermined from available fields"
   430| 
   431| 
```
