# Chunk of tools/extractor.py

- Source relative path: `tools/extractor.py`
- Chunk: 1 of 4
- Original line range: 1-514
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe_str, _utc_now_iso, _iter_export_files, _read_json, _merge_index_details, _iter_domains, _file_id, _get_tool_version, _identity_metadata, _extract_acc_project_label, _model_label_from_path, _norm_central_path, _b32_sha1_16, _load_governance_role_rules, _infer_governance_role, _stable_pattern_id, _write_csv, _read_existing_csv, _sort_rows, compute_hhi_from_shares, compute_effective_clusters, _fmt_metric, compute_attribute_concentration_metrics, _iter_object_style_name_candidates, _remap_object_style_domain, _remap_vco_domain, _load_identity_items_by_record, _load_label_resolution_inputs
- Source SHA-256: d75cdfbab8fb9d4bbc3c46c3611b1bcf54844b8f5421954d15f78ef298ab109e
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| #!/usr/bin/env python3
     2| from __future__ import annotations
     3| 
     4| import base64
     5| import csv
     6| import hashlib
     7| import json
     8| import os
     9| import re
    10| import subprocess
    11| import sys
    12| import time
    13| from collections import defaultdict
    14| from datetime import datetime, timezone
    15| from pathlib import Path
    16| from concurrent.futures import ProcessPoolExecutor, as_completed
    17| from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
    18| 
    19| _TOOLS_DIR = str(Path(__file__).resolve().parent)
    20| if _TOOLS_DIR not in sys.path:
    21|     sys.path.insert(0, _TOOLS_DIR)
    22| 
    23| from label_synthesis.label_resolver import (
    24|     find_near_duplicate_merges,
    25|     load_annotations,
    26|     load_label_population,
    27|     load_llm_cache,
    28|     resolve_pattern_label,
    29| )
    30| 
    31| SCHEMA_VERSION = "2.1.0"
    32| STANDARD_PRESENCE_MIN = 0.75
    33| DOMINANT_SHARE_MIN = 0.50
    34| MIN_RECORDS_FOR_DOMAIN = 50
    35| MIN_FILES_FOR_DOMAIN = 3
    36| UNKNOWN_RATE_MAX = 0.20
    37| SUPPRESSED_ANALYSIS_DOMAINS = {"object_styles_imported"}
    38| ROW_KEY_DOMAINS = {"object_styles_model", "object_styles_annotation", "view_category_overrides"}
    39| 
    40| # See docs/CENTRAL_PATH_NORM_RULE.md for normalization contract.
    41| _VOLATILE_SEGMENTS = {
    42|     "documents",
    43|     "desktop",
    44|     "downloads",
    45|     "appdata",
    46|     "local",
    47|     "roaming",
    48|     "autodesk",
    49|     "revit",
    50|     "cache",
    51| }
    52| _GOVERNANCE_ROLE_RULES_PATH = Path(__file__).resolve().parent.parent / "policies" / "governance_role_path_patterns.json"
    53| 
    54| 
    55| def _safe_str(v: Any) -> str:
    56|     if v is None:
    57|         return ""
    58|     if isinstance(v, bool):
    59|         return "true" if v else "false"
    60|     return str(v)
    61| 
    62| 
    63| def _utc_now_iso() -> str:
    64|     return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    65| 
    66| 
    67| def _iter_export_files(exports_dir: Path) -> List[Tuple[str, Path, Optional[Path]]]:
    68|     files = [p for p in exports_dir.glob("*.json") if p.is_file() and not p.name.lower().endswith(".legacy.json")]
    69|     index_by_base: Dict[str, Path] = {}
    70|     details_by_base: Dict[str, Path] = {}
    71|     fingerprint: List[Tuple[str, Path, Optional[Path]]] = []
    72|     plain: List[Tuple[str, Path, Optional[Path]]] = []
    73|     for p in files:
    74|         lower = p.name.lower()
    75|         if lower.endswith("__fingerprint.json"):
    76|             fingerprint.append((p.name, p, None))
    77|         elif lower.endswith(".index.json"):
    78|             base = lower[:-len(".index.json")]
    79|             index_by_base[base] = p
    80|         elif lower.endswith(".details.json"):
    81|             base = lower[:-len(".details.json")]
    82|             details_by_base[base] = p
    83|         else:
    84|             plain.append((p.name, p, None))
    85| 
    86|     split_pairs: List[Tuple[str, Path, Optional[Path]]] = []
    87|     for base in sorted(set(index_by_base) | set(details_by_base)):
    88|         idx = index_by_base.get(base)
    89|         det = details_by_base.get(base)
    90|         if idx is not None:
    91|             split_pairs.append((idx.name, idx, det))
    92|         elif det is not None:
    93|             split_pairs.append((det.name, det, None))
    94| 
    95|     sys.stderr.write(
    96|         "[INFO extractor] export surfaces: "
    97|         f"fingerprint={len(fingerprint)} split_pairs={len(split_pairs)} plain={len(plain)}\n"
    98|     )
    99| 
   100|     merged: List[Tuple[str, Path, Optional[Path]]] = []
   101|     merged.extend(sorted(fingerprint, key=lambda t: t[0].lower()))
   102|     merged.extend(split_pairs)
   103|     merged.extend(sorted(plain, key=lambda t: t[0].lower()))
   104|     return merged
   105| 
   106| 
   107| def _read_json(p: Path) -> Dict[str, Any]:
   108|     with p.open("r", encoding="utf-8") as f:
   109|         d = json.load(f)
   110|     if not isinstance(d, dict):
   111|         raise TypeError(f"JSON root must be object: {p}")
   112|     return d
   113| 
   114| 
   115| def _merge_index_details(index_fp: Dict[str, Any], details_fp: Dict[str, Any]) -> Dict[str, Any]:
   116|     merged = dict(index_fp)
   117|     for key, value in details_fp.items():
   118|         if not key.startswith("_") and key not in merged:
   119|             merged[key] = value
   120|     return merged
   121| 
   122| 
   123| def _iter_domains(d: Dict[str, Any]) -> List[str]:
   124|     c = d.get("_contract")
   125|     if isinstance(c, dict):
   126|         doms = c.get("domains")
   127|         if isinstance(doms, dict):
   128|             return sorted([str(k) for k in doms.keys()])
   129|     out: List[str] = []
   130|     for k, v in d.items():
   131|         if isinstance(k, str) and not k.startswith("_") and isinstance(v, dict) and isinstance(v.get("records"), list):
   132|             out.append(k)
   133|     return sorted(out)
   134| 
   135| 
   136| def _file_id(path: Path, mode: str) -> str:
   137|     if mode == "basename":
   138|         return path.name
   139|     if mode == "stem":
   140|         return path.stem
   141|     return str(path.resolve())
   142| 
   143| 
   144| def _get_tool_version() -> str:
   145|     env_v = os.environ.get("FINGERPRINT_TOOL_VERSION", "").strip()
   146|     if env_v:
   147|         return env_v if re.match(r"^\d+\.\d+\.\d+([+-].+)?$", env_v) else f"0.0.0+{env_v}"
   148|     base = "0.0.0"
   149|     try:
   150|         gitsha = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
   151|         if gitsha:
   152|             return f"{base}+{gitsha}"
   153|     except Exception:
   154|         pass
   155|     return f"{base}+nogit"
   156| 
   157| 
   158| def _identity_metadata(data: Dict[str, Any]) -> Dict[str, str]:
   159|     identity = data.get("identity") if isinstance(data.get("identity"), dict) else {}
   160|     contract = data.get("_contract") if isinstance(data.get("_contract"), dict) else {}
   161|     contract_ident = contract.get("identity") if isinstance(contract.get("identity"), dict) else {}
   162|     phase2 = identity.get("phase2") if isinstance(identity.get("phase2"), dict) else {}
   163|     lineage_items = phase2.get("lineage_items") if isinstance(phase2.get("lineage_items"), dict) else {}
   164| 
   165|     central_path = _safe_str(
   166|         lineage_items.get("central_path")
   167|         or contract_ident.get("central_path")
   168|         or identity.get("central_path")
   169|         or data.get("central_path")
   170|     )
   171|     return {
   172|         "project_label": _extract_acc_project_label(central_path),
   173|         "model_label": _safe_str(
   174|             lineage_items.get("filename")
   175|             or identity.get("filename")
   176|             or identity.get("model_title")
   177|             or contract_ident.get("model_title")
   178|             or _model_label_from_path(central_path)
   179|         ),
   180|         "central_path": central_path,
   181|         "central_path_norm": _safe_str(lineage_items.get("central_path_norm") or _norm_central_path(central_path)),
   182|         "lineage_hash": _safe_str(phase2.get("lineage_hash") or data.get("lineage_hash") or data.get("_lineage_hash")),
   183|         "revit_version_number": _safe_str(identity.get("revit_version_number") or contract_ident.get("revit_version_number")),
   184|         "revit_version_name": _safe_str(identity.get("revit_version_name") or contract_ident.get("revit_version_name")),
   185|         "revit_build": _safe_str(identity.get("revit_build") or contract_ident.get("revit_build")),
   186|         "is_workshared": _safe_str(identity.get("is_workshared") if "is_workshared" in identity else contract_ident.get("is_workshared")),
   187|     }
   188| 
   189| 
   190| def _extract_acc_project_label(central_path: str) -> str:
   191|     """Extract project folder name from Autodesk Docs:// path. Returns empty string for non-ACC paths."""
   192|     s = (central_path or "").strip()
   193|     prefix = "Autodesk Docs://"
   194|     if not s.lower().startswith(prefix.lower()):
   195|         return ""
   196|     remainder = s[len(prefix):]
   197|     parts = remainder.replace("\\", "/").split("/")
   198|     folder = parts[0].strip() if parts else ""
   199|     return folder
   200| 
   201| 
   202| def _model_label_from_path(central_path: str) -> str:
   203|     """Extract model filename stem from central path. Works for ACC and server paths."""
   204|     s = (central_path or "").strip().replace("\\", "/")
   205|     if not s:
   206|         return ""
   207|     basename = s.split("/")[-1]
   208|     stem, _ = os.path.splitext(basename)
   209|     return stem
   210| 
   211| 
   212| def _norm_central_path(path: str) -> str:
   213|     s = (path or "").strip().replace("\\", "/")
   214|     s = re.sub(r"/+", "/", s).lower()
   215|     s = re.sub(r"^[a-z]:/", "/", s)
   216|     s = re.sub(r"/users/[^/]+/", "/users/<user>/", s)
   217|     parts = [p for p in s.split("/") if p]
   218|     cleaned: List[str] = []
   219|     for p in parts:
   220|         if p.startswith("onedrive"):
   221|             continue
   222|         if p in _VOLATILE_SEGMENTS:
   223|             continue
   224|         if cleaned and cleaned[-1] == p:
   225|             continue
   226|         cleaned.append(p)
   227|     out = "/" + "/".join(cleaned) if cleaned else ""
   228|     return out.rstrip("/")
   229| 
   230| 
   231| def _b32_sha1_16(text: str) -> str:
   232|     digest = hashlib.sha1(text.encode("utf-8")).digest()
   233|     [REDACTED-POSSIBLE-SECRET](digest).decode("ascii").lower().rstrip("=")
   234|     return token[:16]
   235| 
   236| 
   237| def _load_governance_role_rules() -> List[Dict[str, str]]:
   238|     try:
   239|         with _GOVERNANCE_ROLE_RULES_PATH.open("r", encoding="utf-8") as f:
   240|             payload = json.load(f)
   241|     except Exception as exc:
   242|         sys.stderr.write(
   243|             f"[WARN extractor] governance_role inference disabled: could not load "
   244|             f"{_GOVERNANCE_ROLE_RULES_PATH}: {exc}\n"
   245|         )
   246|         return []
   247| 
   248|     if not isinstance(payload, dict) or not isinstance(payload.get("rules"), list):
   249|         sys.stderr.write(
   250|             f"[WARN extractor] governance_role inference disabled: malformed rules file "
   251|             f"{_GOVERNANCE_ROLE_RULES_PATH}\n"
   252|         )
   253|         return []
   254| 
   255|     cleaned: List[Dict[str, str]] = []
   256|     for idx, rule in enumerate(payload.get("rules", [])):
   257|         if not isinstance(rule, dict):
   258|             continue
   259|         role = _safe_str(rule.get("role")).strip()
   260|         path_contains = _safe_str(rule.get("path_contains")).strip().lower()
   261|         filename_contains = _safe_str(rule.get("filename_contains")).strip().lower()
   262|         if not role or not path_contains:
   263|             sys.stderr.write(f"[WARN extractor] skipping invalid governance rule at index {idx}\n")
   264|             continue
   265|         row: Dict[str, str] = {"role": role, "path_contains": path_contains}
   266|         if filename_contains:
   267|             row["filename_contains"] = filename_contains
   268|         cleaned.append(row)
   269|     return cleaned
   270| 
   271| 
   272| def _infer_governance_role(central_path_norm: str, rules: List[Dict[str, str]]) -> str:
   273|     normalized_path = (central_path_norm or "").lower()
   274|     if not normalized_path or not rules:
   275|         return ""
   276| 
   277|     filename = normalized_path.replace("\\", "/").rsplit("/", 1)[-1]
   278|     for rule in rules:
   279|         path_contains = rule.get("path_contains", "")
   280|         if path_contains not in normalized_path:
   281|             continue
   282|         filename_contains = rule.get("filename_contains", "")
   283|         if filename_contains and filename_contains not in filename:
   284|             continue
   285|         return rule.get("role", "")
   286|     return ""
   287| 
   288| 
   289| def _stable_pattern_id(
   290|     domain: str,
   291|     join_key_schema: str,
   292|     join_hash: str,
   293|     taken: set[str],
   294| ) -> str:
   295|     raw = f"{domain}|{join_key_schema}|{join_hash}"
   296|     digest = hashlib.sha1(raw.encode("utf-8")).digest()
   297|     [REDACTED-POSSIBLE-SECRET](digest).decode("ascii").lower().rstrip("=")
   298|     for n in range(16, len(token) + 1):
   299|         candidate = f"pat_{token[:n]}"
   300|         if candidate not in taken:
   301|             taken.add(candidate)
   302|             return candidate
   303|     candidate = f"pat_{token}"
   304|     taken.add(candidate)
   305|     return candidate
   306| 
   307| 
   308| def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
   309|     path.parent.mkdir(parents=True, exist_ok=True)
   310|     with path.open("w", newline="", encoding="utf-8") as f:
   311|         w = csv.DictWriter(f, fieldnames=fieldnames)
   312|         w.writeheader()
   313|         for row in rows:
   314|             w.writerow({k: row.get(k, "") for k in fieldnames})
   315| 
   316| 
   317| def _read_existing_csv(path: Path) -> List[Dict[str, str]]:
   318|     try:
   319|         with path.open("r", encoding="utf-8-sig", newline="") as f:
   320|             reader = csv.DictReader(f)
   321|             return [{k: ("" if v is None else str(v)) for k, v in row.items()} for row in reader]
   322|     except Exception:
   323|         return []
   324| 
   325| 
   326| def _sort_rows(rows: List[Dict[str, str]], keys: List[str]) -> List[Dict[str, str]]:
   327|     return sorted(rows, key=lambda r: tuple(r.get(k, "") for k in keys))
   328| 
   329| 
   330| def compute_hhi_from_shares(
   331|     shares: Iterable[float],
   332|     *,
   333|     require_closed_universe: bool = True,
   334|     closure_tolerance: float = 1e-9,
   335| ) -> Optional[float]:
   336|     """Compute HHI from a generic share vector.
   337| 
   338|     Shares must be non-negative proportions. By default this helper enforces a
   339|     closed universe (sum(shares) ~= 1.0) and returns None if invalid.
   340|     """
   341|     vals: List[float] = []
   342|     for s in shares:
   343|         if s is None:
   344|             continue
   345|         try:
   346|             v = float(s)
   347|         except (TypeError, ValueError):
   348|             return None
   349|         if v < 0.0:
   350|             return None
   351|         vals.append(v)
   352|     if not vals:
   353|         return None
   354|     total = sum(vals)
   355|     if total <= 0.0:
   356|         return None
   357|     if require_closed_universe and abs(total - 1.0) > closure_tolerance:
   358|         return None
   359|     return sum(v * v for v in vals)
   360| 
   361| 
   362| def compute_effective_clusters(hhi_value: Optional[float]) -> Optional[float]:
   363|     """Return effective cluster count (1/HHI) or None when undefined.
   364| 
   365|     This helper is null-safe and never divides by zero. It does not coerce
   366|     undefined inputs into numeric defaults.
   367|     """
   368|     if hhi_value is None or hhi_value <= 0.0:
   369|         return None
   370|     return 1.0 / hhi_value
   371| 
   372| 
   373| def _fmt_metric(value: Optional[float]) -> str:
   374|     return f"{value:.6f}" if value is not None else ""
   375| 
   376| 
   377| def compute_attribute_concentration_metrics(*_: Any, **__: Any) -> None:
   378|     """Placeholder extension hook for future attribute-level concentration.
   379| 
   380|     Future attribute-level metrics should derive share vectors and call
   381|     compute_hhi_from_shares(...) + compute_effective_clusters(...).
   382|     """
   383|     return None
   384| 
   385| 
   386| def _iter_object_style_name_candidates(rec: Dict[str, Any]) -> Iterable[str]:
   387|     for k in ("record_id", "id", "name"):
   388|         v = rec.get(k)
   389|         if isinstance(v, str) and v.strip():
   390|             yield v
   391|     label = rec.get("label")
   392|     if isinstance(label, dict):
   393|         disp = label.get("display")
   394|         if isinstance(disp, str) and disp.strip():
   395|             yield disp
   396|     identity_basis = rec.get("identity_basis")
   397|     if isinstance(identity_basis, dict):
   398|         items = identity_basis.get("items")
   399|         if isinstance(items, list):
   400|             for it in items:
   401|                 if not isinstance(it, dict):
   402|                     continue
   403|                 v = it.get("v")
   404|                 if isinstance(v, str) and v.strip():
   405|                     yield v
   406| 
   407| 
   408| def _remap_object_style_domain(source_domain: str, rec: Dict[str, Any]) -> Optional[str]:
   409|     if not source_domain.startswith("object_styles_"):
   410|         return source_domain
   411|     haystack = " | ".join([s.lower() for s in _iter_object_style_name_candidates(rec)])
   412| 
   413|     # Temporary flatten-side hygiene:
   414|     # - Skip known imported DWG / Imports-in-Families rows from mainline model domain.
   415|     # - Route explicit analytical names into object_styles_analytical.
   416|     # TODO(move-to-exporter): move this classification upstream into exporter probe/domain emission.
   417|     if "imports in families" in haystack or "-.dwg-" in haystack or ".dwg" in haystack:
   418|         return None
   419|     if "-analytical-" in haystack:
   420|         return "object_styles_analytical"
   421|     return source_domain
   422| 
   423| 
   424| def _remap_vco_domain(source_domain: str, rec: Dict[str, Any]) -> Optional[str]:
   425|     if source_domain != "view_category_overrides":
   426|         return source_domain
   427|     # Suppress CAD import noise records — same pattern as object_styles.
   428|     # TODO(move-to-exporter): move this suppression upstream into exporter domain emission.
   429|     candidates = " | ".join([s.lower() for s in _iter_object_style_name_candidates(rec)])
   430|     if "imports in families" in candidates or ".dwg" in candidates:
   431|         return None
   432|     return source_domain
   433| 
   434| 
   435| def _load_identity_items_by_record(
   436|     phase0_dir: Optional[Path],
   437|     domain: Optional[str] = None,
   438|     allowed_record_pks: Optional[Set[str]] = None,
   439| ) -> Dict[str, List[Dict[str, Any]]]:
   440|     if phase0_dir is None:
   441|         return {}
   442| 
   443|     csv_path: Optional[Path] = None
   444|     if domain:
   445|         scoped = phase0_dir / "identity_items_by_domain" / f"{domain}.csv"
   446|         if scoped.is_file():
   447|             csv_path = scoped
   448| 
   449|     if csv_path is None:
   450|         fallback = phase0_dir / "identity_items.csv"
   451|         if not fallback.is_file():
   452|             return {}
   453|         csv_path = fallback
   454| 
   455|     out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
   456|     with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
   457|         for row in csv.DictReader(f):
   458|             if domain and _safe_str(row.get("domain")) and _safe_str(row.get("domain")) != domain:
   459|                 continue
   460|             record_pk = _safe_str(row.get("record_pk"))
   461|             if not record_pk:
   462|                 continue
   463|             if allowed_record_pks is not None and record_pk not in allowed_record_pks:
   464|                 continue  # skip rows for records not participating in analysis
   465|             out[record_pk].append({
   466|                 "k": _safe_str(row.get("item_key")),
   467|                 "v": _safe_str(row.get("item_value")),
   468|                 "q": _safe_str(row.get("item_value_type")),
   469|                 "role": _safe_str(row.get("item_role")),
   470|             })
   471|     return out
   472| 
   473| 
   474| def _load_label_resolution_inputs(
   475|     results_v21_dir: Optional[Path],
   476|     domain: str,
   477|     label_synth_dir: Optional[Path] = None,
   478| ) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str], Dict[str, Any]]:
   479|     if results_v21_dir is None and label_synth_dir is None:
   480|         return {}, {}, {}
   481| 
   482|     # label_synth_dir overrides results_v21_dir/label_synthesis for all read paths so
   483|     # segment runs can point at the corpus label_synthesis (richer population, LLM cache,
   484|     # curator annotations) without redirecting the analysis write path.
   485|     effective_synth = label_synth_dir if label_synth_dir is not None else (results_v21_dir / "label_synthesis" if results_v21_dir else None)
   486|     analysis_dir = (results_v21_dir / "analysis") if results_v21_dir else None
   487| 
   488|     population_candidates = [p for p in [
   489|         (effective_synth / f"{domain}.joinhash_label_population.csv") if effective_synth else None,
   490|         (analysis_dir / f"{domain}.joinhash_label_population.csv") if analysis_dir else None,
   491|         (analysis_dir / "label_population" / f"{domain}.joinhash_label_population.csv") if analysis_dir else None,
   492|     ] if p is not None]
   493|     pop_path = next((p for p in population_candidates if p.is_file()), None)
   494|     label_pop = load_label_population(str(pop_path), domain) if pop_path else {}
   495| 
   496|     annotation_candidates = [p for p in [
   497|         (effective_synth / f"{domain}.pattern_annotations.csv") if effective_synth else None,
   498|         (effective_synth / "pattern_annotations.csv") if effective_synth else None,
   499|         (analysis_dir / "pattern_annotations.csv") if analysis_dir else None,
   500|     ] if p is not None]
   501|     anno_path = next((p for p in annotation_candidates if p.is_file()), None)
   502|     annotations = load_annotations(str(anno_path)) if anno_path else {}
   503| 
   504|     llm_cache_candidates = [p for p in [
   505|         (effective_synth / f"{domain}.llm_name_cache.json") if effective_synth else None,
   506|         (effective_synth / "llm_name_cache.json") if effective_synth else None,
   507|         (analysis_dir / f"{domain}.llm_name_cache.json") if analysis_dir else None,
   508|         (analysis_dir / "llm_name_cache.json") if analysis_dir else None,
   509|     ] if p is not None]
   510|     llm_path = next((p for p in llm_cache_candidates if p.is_file()), None)
   511|     llm_cache = load_llm_cache(str(llm_path)) if llm_path else {}
   512|     return label_pop, annotations, llm_cache
   513| 
   514| 
```
