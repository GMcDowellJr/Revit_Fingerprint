# Chunk of tools/label_synthesis/build_semantic_groups.py

- Source relative path: `tools/label_synthesis/build_semantic_groups.py`
- Chunk: 2 of 3
- Original line range: 432-899
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _prompt_fill_patterns, _utc_now_iso, _read_csv_rows, _load_analysis_run_id, _load_cache, _save_cache, _write_json, _resolve_export_target, _write_export_batches, _load_export_progress, _save_export_progress, _derive_element_label, _load_pattern_rows, _load_pattern_to_record_pk, _resolve_identity_items_source, _load_identity_items_by_record, _line_pattern_segment_keys, _is_nullish, _extract_behavioral_props, _parse_grouping_response, _normalize_import_payload, _call_grouping_llm
- Source SHA-256: ecbac527e320e64b586fce64b729698632c2ac0daced16f12b6c615ec9265668
- Starts inside symbol: no
- Ends inside symbol: no

```
   432| def _prompt_fill_patterns(domain: str, label: str, props: dict[str, str], peers: list[str]) -> str:
   433|     grid_count_raw = props.get("grid_count", "")
   434|     spacing_raw = props.get("spacing_in", "")
   435|     grid_angles_raw = props.get("grid_angles", "")
   436|     target = "Drafting" if "drafting" in domain else "Model"
   437| 
   438|     try:
   439|         gc = int(grid_count_raw)
   440|     except (ValueError, TypeError):
   441|         gc = None
   442| 
   443|     grid_angles: List[float] = []
   444|     for tok in grid_angles_raw.split(","):
   445|         tok = tok.strip()
   446|         if not tok:
   447|             continue
   448|         try:
   449|             grid_angles.append(float(tok))
   450|         except ValueError:
   451|             continue
   452|     geometry_desc = _infer_fill_geometry_description(gc, grid_angles)
   453| 
   454|     is_solid = gc == 0 or (gc is None and not spacing_raw)
   455| 
   456|     if is_solid:
   457|         complexity = "solid fill (no grid — completely filled region)"
   458|         spacing_note = ""
   459|     elif gc is not None and gc >= 10:
   460|         complexity = f"complex pattern ({gc} grids — likely imported from PAT file)"
   461|         spacing_note = "  The pattern geometry is too complex to summarize; name is the primary signal.\n"
   462|     else:
   463|         complexity = f"geometric pattern ({gc} grids)" if gc else "geometric pattern"
   464|         if spacing_raw:
   465|             try:
   466|                 sp = float(spacing_raw)
   467|                 if "drafting" in domain:
   468|                     if sp <= 0.16:
   469|                         density = 'fine (≤ 0.16" spacing)'
   470|                     elif sp <= 0.39:
   471|                         density = 'medium (0.16–0.39" spacing)'
   472|                     else:
   473|                         density = 'coarse (> 0.39" spacing)'
   474|                 else:
   475|                     if sp <= 10:
   476|                         density = 'fine (≤ 10" spacing)'
   477|                     elif sp <= 31:
   478|                         density = 'medium (10–31" spacing)'
   479|                     else:
   480|                         density = 'coarse (> 31" spacing)'
   481|                 spacing_note = f'  Spacing: {sp:.3f}" — {density}\n'
   482|             except ValueError:
   483|                 spacing_note = f"  Spacing: {spacing_raw}\n"
   484|         else:
   485|             spacing_note = ""
   486| 
   487|     is_fallback = "join_key.v1" in label or "Variant" in label
   488|     is_cad_import = ".dwg" in label or "-" in label.split(".")[-1].split("-")[-1]
   489| 
   490|     if is_fallback:
   491|         label_note = "NOTE: The pattern name is a system-generated fallback — no human-readable name is available. Base your grouping decision primarily on the geometry (type and density).\n"
   492|     elif is_cad_import:
   493|         label_note = "NOTE: The pattern name appears to be derived from a CAD import (contains .dwg reference or import suffix). The name may not reflect Revit convention intent.\n"
   494|     else:
   495|         label_note = ""
   496| 
   497|     name_context = """
   498| Known fill pattern naming conventions:
   499| - Revit built-in patterns: "Concrete", "Earth", "Sand", "Gravel", "Diagonal Crosshatch", "Horizontal", "Vertical", "Steel", "Wood"
   500| - Autodesk pattern prefixes: "AR-" (architectural hatches from AutoCAD: AR-CONC, AR-BRSTD, AR-SAND, AR-HBONE)
   501| - ANSI patterns: "ANSI31" (steel/iron), "ANSI32" (steel), "ANSI33" (bronze), "ANSI34" (rubber/plastic), "ANSI35" (fire brick), "ANSI36" (marble/glass), "ANSI37" (lead/zinc), "ANSI38" (aluminum)
   502| - Scale suffixes: "Small", "Medium", "Large", "Dense", "2mm", "4mm" — indicate pattern density variant
   503| - Application suffixes: "(Cut)" vs "(Surface)" — indicates which surface the pattern applies to
   504| - Custom firm patterns often use material names directly: "Concrete Block", "CMU", "Batt Insulation", "Rigid Insulation"
   505| """
   506| 
   507|     return f"""PATTERN: Fill Pattern ({target})
   508| 
   509| LABEL: {label}
   510| COMPLEXITY: {complexity}
   511| GEOMETRY: {geometry_desc}
   512| {spacing_note}{label_note}
   513| CONTEXT:
   514| {name_context}
   515| Fill patterns are applied to cut or surface regions of building materials in Revit sections and plans.
   516| 
   517| Group by geometric identity first: the orientation/structure in the GEOMETRY line
   518| above (derived from grid count and grid angle) plus density from the spacing note
   519| is the anchor for the group label. Use geometric descriptors — "Diagonal Hatch",
   520| "Cross-Hatch", "Horizontal Ruled", "Vertical Ruled" — as the primary group name
   521| when the GEOMETRY line supports them. Do not infer geometric descriptors (e.g.
   522| masonry coursing) that the GEOMETRY/COMPLEXITY fields above do not support.
   523| 
   524| Material names (Concrete, Sand, Earth, Wood) may annotate a group as a secondary
   525| label but must not replace a geometry-derived name. A pattern named "Concrete" with
   526| diagonal hatch geometry belongs in "Diagonal Hatch", annotated with "(Concrete-like)"
   527| — not in a "Concrete" group that would also capture geometrically unrelated patterns.
   528| 
   529| Exception: when multiple patterns share identical geometry (undifferentiated simple
   530| hatch) and differ only in material names, or when GEOMETRY is "unknown"/"undetermined",
   531| use the material name as the group differentiator since geometry alone cannot
   532| distinguish them.
   533| 
   534| {_peer_block(peers)}
   535| 
   536| Assign a semantic group for this fill pattern. Examples of valid group labels:
   537|   solid-fill (completely opaque fill, no pattern)
   538|   concrete-hatch (concrete material pattern)
   539|   earth-fill (earth/soil/grade material)
   540|   insulation-batt (batt insulation wavy lines)
   541|   insulation-rigid (rigid insulation diagonal lines)
   542|   masonry-brick (brick coursing pattern)
   543|   masonry-cmu (concrete block/CMU pattern)
   544|   diagonal-line (simple diagonal line pattern, no specific material)
   545|   crosshatch (crossing diagonal lines, steel or general)
   546|   horizontal-line (horizontal line pattern)
   547|   vertical-line (vertical line pattern)
   548|   sand-gravel (sand or gravel aggregate pattern)
   549|   wood-grain (wood grain or board pattern)
   550|   metal-steel (steel or metal hatch, ANSI patterns)
   551|   complex-import (complex PAT-file pattern, ungroupable by name alone)
   552|   unknown-fill (fallback pattern with no usable name or geometry signal)
   553| 
   554| Respond with ONLY the JSON object."""
   555| 
   556| 
   557| def _utc_now_iso() -> str:
   558|     return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
   559| 
   560| 
   561| def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
   562|     with path.open("r", encoding="utf-8-sig", newline="") as f:
   563|         return list(csv.DictReader(f))
   564| 
   565| 
   566| def _load_analysis_run_id(analysis_dir: Path) -> str:
   567|     manifest = analysis_dir / "analysis_manifest.csv"
   568|     if not manifest.is_file():
   569|         return ""
   570|     rows = _read_csv_rows(manifest)
   571|     if not rows:
   572|         return ""
   573|     return (rows[0].get("analysis_run_id") or "").strip()
   574| 
   575| 
   576| def _load_cache(cache_path: Path) -> Dict[str, Any]:
   577|     if not cache_path.is_file():
   578|         return {
   579|             "schema_version": CACHE_SCHEMA_VERSION,
   580|             "analysis_run_id": "",
   581|             "generated_at": "",
   582|             "groups": {},
   583|         }
   584|     with cache_path.open("r", encoding="utf-8") as f:
   585|         data = json.load(f)
   586|     if not isinstance(data, dict):
   587|         return {"schema_version": CACHE_SCHEMA_VERSION, "analysis_run_id": "", "generated_at": "", "groups": {}}
   588|     data.setdefault("schema_version", CACHE_SCHEMA_VERSION)
   589|     data.setdefault("analysis_run_id", "")
   590|     data.setdefault("generated_at", "")
   591|     groups = data.get("groups")
   592|     data["groups"] = groups if isinstance(groups, dict) else {}
   593|     return data
   594| 
   595| 
   596| def _save_cache(cache_path: Path, cache: Dict[str, Any]) -> None:
   597|     cache_path.parent.mkdir(parents=True, exist_ok=True)
   598|     with cache_path.open("w", encoding="utf-8") as f:
   599|         json.dump(cache, f, indent=2, sort_keys=True, ensure_ascii=False)
   600| 
   601| 
   602| def _write_json(path: Path, payload: Any) -> None:
   603|     path.parent.mkdir(parents=True, exist_ok=True)
   604|     with path.open("w", encoding="utf-8") as f:
   605|         json.dump(payload, f, indent=2, ensure_ascii=False)
   606| 
   607| 
   608| def _resolve_export_target(cache_path: Path, export_arg: Path) -> Path:
   609|     """Place export files alongside label_semantic_groups.json."""
   610|     return cache_path.parent / export_arg.name
   611| 
   612| 
   613| def _write_export_batches(base_path: Path, prompts: list[Dict[str, str]], batch_size: Optional[int]) -> list[Path]:
   614|     if not batch_size or batch_size <= 0:
   615|         _write_json(base_path, prompts)
   616|         return [base_path]
   617| 
   618|     written: list[Path] = []
   619|     total = len(prompts)
   620|     if total == 0:
   621|         _write_json(base_path, prompts)
   622|         return [base_path]
   623| 
   624|     stem = base_path.stem
   625|     suffix = base_path.suffix or '.json'
   626|     for idx, start in enumerate(range(0, total, batch_size), start=1):
   627|         chunk = prompts[start:start + batch_size]
   628|         chunk_path = base_path.with_name(f"{stem}.batch_{idx:03d}{suffix}")
   629|         _write_json(chunk_path, chunk)
   630|         written.append(chunk_path)
   631|     return written
   632| 
   633| 
   634| def _load_export_progress(path: Path) -> Dict[str, set[str]]:
   635|     if not path.is_file():
   636|         return {}
   637|     try:
   638|         with path.open("r", encoding="utf-8") as f:
   639|             data = json.load(f)
   640|     except Exception:
   641|         return {}
   642|     raw = data.get("exported_pattern_ids", {}) if isinstance(data, dict) else {}
   643|     if not isinstance(raw, dict):
   644|         return {}
   645|     out: Dict[str, set[str]] = {}
   646|     for domain, values in raw.items():
   647|         if isinstance(values, list):
   648|             out[str(domain)] = {str(v).strip() for v in values if str(v).strip()}
   649|     return out
   650| 
   651| 
   652| def _save_export_progress(path: Path, progress: Dict[str, set[str]]) -> None:
   653|     serializable = {
   654|         "schema_version": "1.0",
   655|         "updated_at": _utc_now_iso(),
   656|         "exported_pattern_ids": {k: sorted(v) for k, v in sorted(progress.items())},
   657|     }
   658|     _write_json(path, serializable)
   659| 
   660| 
   661| def _derive_element_label(domain: str, items: Dict[str, str], fallback: str) -> str:
   662|     candidate_keys: Dict[str, List[str]] = {
   663|         "text_types": ["text_type.name", "text_type.type_name", "text_type.label"],
   664|         "arrowheads": ["arrowhead.name", "arrowhead.type_name", "arrowhead.label"],
   665|         "line_patterns": ["line_pattern.name", "line_pattern.label"],
   666|         "line_styles": ["line_style.name", "line_style.subcategory_name", "line_style.label"],
   667|         "fill_patterns_drafting": ["fill_pattern.name", "fill_pattern.label"],
   668|         "fill_patterns_model": ["fill_pattern.name", "fill_pattern.label"],
   669|     }
   670|     for key in candidate_keys.get(domain, []):
   671|         val = (items.get(key) or "").strip()
   672|         if val:
   673|             return val
   674|     return fallback
   675| 
   676| 
   677| def _load_pattern_rows(analysis_dir: Path, only_domain: Optional[str]) -> Dict[str, List[Dict[str, str]]]:
   678|     domain_patterns_csv = analysis_dir / "domain_patterns.csv"
   679|     if not domain_patterns_csv.is_file():
   680|         raise FileNotFoundError(f"Missing required input: {domain_patterns_csv}")
   681|     rows = _read_csv_rows(domain_patterns_csv)
   682|     out: Dict[str, List[Dict[str, str]]] = defaultdict(list)
   683|     stats: Dict[str, int] = defaultdict(int)
   684|     for row in rows:
   685|         domain = (row.get("domain") or "").strip().lower()
   686|         stats["rows_total"] += 1
   687|         if domain not in SEMANTIC_GROUPING_DOMAINS:
   688|             stats["rows_out_of_scope_domain"] += 1
   689|             continue
   690|         if only_domain and domain != only_domain:
   691|             stats["rows_filtered_by_domain_arg"] += 1
   692|             continue
   693|         pattern_id = (row.get("pattern_id") or "").strip()
   694|         label = (row.get("pattern_label_human") or "").strip()
   695|         source = (row.get("pattern_label_source") or "").strip().lower()
   696|         if not pattern_id:
   697|             stats["rows_skipped_missing_pattern_id"] += 1
   698|             continue
   699|         if source == "missing":
   700|             stats["rows_skipped_missing_source"] += 1
   701|             continue
   702|         if not label:
   703|             stats["rows_skipped_blank_pattern_label_human"] += 1
   704|             continue
   705|         stats["rows_eligible"] += 1
   706|         out[domain].append({
   707|             "pattern_id": pattern_id,
   708|             "pattern_label_human": label,
   709|         })
   710|     for domain in list(out.keys()):
   711|         out[domain] = sorted(out[domain], key=lambda r: r["pattern_id"])
   712|     print(f"[build_semantic_groups] domain_patterns scan stats: {dict(stats)}")
   713|     return out
   714| 
   715| 
   716| def _load_pattern_to_record_pk(analysis_dir: Path, domain: str) -> Dict[str, str]:
   717|     membership_csv = analysis_dir / "record_pattern_membership.csv"
   718|     if not membership_csv.is_file():
   719|         raise FileNotFoundError(f"Missing required input: {membership_csv}")
   720|     rows = _read_csv_rows(membership_csv)
   721|     out: Dict[str, str] = {}
   722|     for row in rows:
   723|         if (row.get("domain") or "").strip().lower() != domain:
   724|             continue
   725|         pattern_id = (row.get("pattern_id") or "").strip()
   726|         record_pk = (row.get("record_pk") or "").strip()
   727|         if pattern_id and record_pk and pattern_id not in out:
   728|             out[pattern_id] = record_pk
   729|     return out
   730| 
   731| 
   732| def _resolve_identity_items_source(phase0_dir: Path, shards_dir: Path, domain: str) -> Optional[Path]:
   733|     if (shards_dir / ".complete").is_file():
   734|         shard_candidates = [
   735|             shards_dir / f"{domain}.identity_items.csv",
   736|             shards_dir / f"{domain}.csv",
   737|         ]
   738|         for candidate in shard_candidates:
   739|             if candidate.is_file():
   740|                 return candidate
   741|     fallback = phase0_dir / "phase0_identity_items.csv"
   742|     if fallback.is_file():
   743|         return fallback
   744|     print(
   745|         "[build_semantic_groups] WARN: missing identity-items source for domain "
   746|         f"'{domain}'. looked for shard(s) and fallback: {fallback}"
   747|     )
   748|     return None
   749| 
   750| 
   751| def _load_identity_items_by_record(phase0_dir: Path, shards_dir: Path, domain: str) -> Optional[Dict[str, Dict[str, str]]]:
   752|     src_csv = _resolve_identity_items_source(phase0_dir, shards_dir, domain)
   753|     if src_csv is None:
   754|         return None
   755|     rows = _read_csv_rows(src_csv)
   756|     out: Dict[str, Dict[str, str]] = defaultdict(dict)
   757|     for row in rows:
   758|         if (row.get("domain") or "").strip().lower() != domain:
   759|             continue
   760|         record_pk = (row.get("record_pk") or "").strip()
   761|         key = (row.get("k") or "").strip()
   762|         value = (row.get("v") or "").strip()
   763|         quality = (row.get("q") or "").strip()
   764|         if not record_pk or not key or quality != "ok":
   765|             continue
   766|         if value:
   767|             out[record_pk][key] = value
   768|     print(f"[build_semantic_groups] domain={domain} identity_items_source={src_csv}")
   769|     return out
   770| 
   771| 
   772| def _line_pattern_segment_keys(items: Dict[str, str]) -> List[str]:
   773|     keys = [k for k in items.keys() if k.startswith("line_pattern.seg[") and k.endswith("].kind")]
   774|     return sorted(keys)
   775| 
   776| 
   777| def _is_nullish(value: str) -> bool:
   778|     v = value.strip().lower()
   779|     return v in {"", "none", "null", "nil", "n/a", "na"}
   780| 
   781| 
   782| def _extract_behavioral_props(domain: str, items: Dict[str, str]) -> Dict[str, str]:
   783|     props: Dict[str, str] = {}
   784|     if domain == "text_types":
   785|         key_map = {
   786|             "text_type.font": "font",
   787|             "text_type.size_in": "size_in",
   788|             "text_type.bold": "bold",
   789|             "text_type.italic": "italic",
   790|             "text_type.color_rgb": "color_rgb",
   791|             "text_type.show_border": "show_border",
   792|             "text_type.background_raw": "background_raw",
   793|         }
   794|         for src_key, dst_key in key_map.items():
   795|             if items.get(src_key):
   796|                 props[dst_key] = items[src_key]
   797|     elif domain == "arrowheads":
   798|         key_map = {
   799|             "arrowhead.style": "style",
   800|             "arrowhead.tick_size_in": "tick_size_in",
   801|             "arrowhead.filled": "fill_tick",
   802|             "arrowhead.heavy_end_pen_weight": "heavy_end_pen_weight",
   803|         }
   804|         for src_key, dst_key in key_map.items():
   805|             if items.get(src_key):
   806|                 props[dst_key] = items[src_key]
   807|     elif domain == "line_patterns":
   808|         if items.get("line_pattern.segment_count"):
   809|             props["segment_count"] = items["line_pattern.segment_count"]
   810|     elif domain == "line_styles":
   811|         if items.get("line_style.color.rgb"):
   812|             props["color_rgb"] = items["line_style.color.rgb"]
   813|         if items.get("line_style.weight.projection"):
   814|             props["weight_projection"] = items["line_style.weight.projection"]
   815|         pattern_synopsis = (
   816|             items.get("line_style.pattern_ref.pattern_label_human", "")
   817|             or items.get("line_style.pattern_ref.label", "")
   818|             or items.get("line_style.pattern_ref.synopsis", "")
   819|         )
   820|         if pattern_synopsis:
   821|             props["pattern_synopsis"] = pattern_synopsis
   822|         else:
   823|             sig_hash = items.get("line_style.pattern_ref.sig_hash", "")
   824|             props["pattern_synopsis"] = "[solid]" if _is_nullish(sig_hash) else sig_hash
   825|     elif domain in {"fill_patterns_drafting", "fill_patterns_model"}:
   826|         if items.get("fill_pattern.grid_count"):
   827|             props["grid_count"] = items["fill_pattern.grid_count"]
   828|         offset = items.get("fill_pattern.grid[000].offset", "")
   829|         if offset:
   830|             try:
   831|                 props["spacing_in"] = str(abs(float(offset)) * 12.0)
   832|             except ValueError:
   833|                 props["spacing_in"] = offset
   834|         angle_pattern = re.compile(r"^fill_pattern\.grid\[(\d+)\]\.angle$")
   835|         angles_by_idx: Dict[int, str] = {}
   836|         for k, v in items.items():
   837|             m = angle_pattern.match(k)
   838|             if m and v:
   839|                 angles_by_idx[int(m.group(1))] = v
   840|         if angles_by_idx:
   841|             props["grid_angles"] = ",".join(
   842|                 angles_by_idx[i] for i in sorted(angles_by_idx.keys())
   843|             )
   844|     return props
   845| 
   846| 
   847| def _parse_grouping_response(raw_text: str) -> Dict[str, str]:
   848|     text = raw_text.strip()
   849|     if text.startswith("```"):
   850|         text = text.split("\n", 1)[-1]
   851|         if text.endswith("```"):
   852|             text = text[: text.rfind("```")]
   853|     try:
   854|         parsed = json.loads(text)
   855|         if not isinstance(parsed, dict):
   856|             raise ValueError("Expected object")
   857|         semantic_group = str(parsed.get("semantic_group", "")).strip()
   858|         confidence = str(parsed.get("confidence", "low")).strip().lower()
   859|         rationale = str(parsed.get("rationale", "")).strip()
   860|         if confidence not in {"high", "medium", "low"}:
   861|             confidence = "low"
   862|         if not semantic_group:
   863|             semantic_group = "__parse_error__"
   864|             confidence = "low"
   865|             rationale = rationale or "Missing semantic_group in LLM response."
   866|         return {
   867|             "semantic_group": semantic_group,
   868|             "confidence": confidence,
   869|             "rationale": rationale,
   870|         }
   871|     except Exception:
   872|         return {
   873|             "semantic_group": "__parse_error__",
   874|             "confidence": "low",
   875|             "rationale": raw_text.strip(),
   876|         }
   877| 
   878| 
   879| def _normalize_import_payload(row: Dict[str, Any]) -> Dict[str, str]:
   880|     semantic_group = str(row.get("semantic_group", "")).strip()
   881|     confidence = str(row.get("confidence", "low")).strip().lower()
   882|     rationale = str(row.get("rationale", "")).strip()
   883|     if confidence not in {"high", "medium", "low"}:
   884|         confidence = "low"
   885|     if not semantic_group:
   886|         semantic_group = "__parse_error__"
   887|         confidence = "low"
   888|         rationale = rationale or "Missing semantic_group in imported result."
   889|     return {
   890|         "semantic_group": semantic_group,
   891|         "confidence": confidence,
   892|         "rationale": rationale,
   893|     }
   894| 
   895| 
   896| def _call_grouping_llm(prompt: str) -> str:
   897|     raise NotImplementedError("LLM call wiring for semantic grouping is not implemented yet.")
   898| 
   899| 
```
