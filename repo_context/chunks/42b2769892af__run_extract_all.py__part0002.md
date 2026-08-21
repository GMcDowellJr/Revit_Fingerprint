# Chunk of tools/run_extract_all.py

- Source relative path: `tools/run_extract_all.py`
- Chunk: 2 of 3
- Original line range: 500-726
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _validate_line_pattern_synthetic_norm_hash, _emit_join_policy_diagnostics, _detect_surfaces, _merge_index_details, _pick_sample_file, _read_json, _infer_domains, _parse_stage_csv, _warn_deprecated_alias, _enforce_policy_gate
- Source SHA-256: 6097e03c70f161dde5df1eb1e3887c55c0c5ff53ad8f51c123db9238e5ca6429
- Starts inside symbol: no
- Ends inside symbol: no

```
   500| def _validate_line_pattern_synthetic_norm_hash(phase0_dir: Path) -> None:
   501|     records_csv = phase0_dir / "records.csv"
   502|     shard_dir = phase0_dir / "identity_items_by_domain"
   503|     lp_shard = shard_dir / "line_patterns.csv"
   504|     # A complete shard set is sufficient on its own -- a corpus with zero line_patterns
   505|     # records legitimately has no line_patterns.csv shard at all (extractor.py only
   506|     # creates a domain's shard file lazily, on its first item row), so requiring
   507|     # lp_shard to exist here would wrongly abort a perfectly valid apply run.
   508|     use_shard = (shard_dir / ".complete").is_file()
   509|     items_csv = phase0_dir / "identity_items.csv"
   510|     if not records_csv.is_file() or not (use_shard or items_csv.is_file()):
   511|         raise SystemExit("flatten/enrichment stage did not produce records.csv and identity_items (shards or identity_items.csv) before apply")
   512|     line_pattern_pks: List[str] = []
   513|     for r in _iter_csv_rows(records_csv):
   514|         if str(r.get("domain", "")) == "line_patterns":
   515|             line_pattern_pks.append(r.get("record_pk", ""))
   516|     if not line_pattern_pks:
   517|         return
   518|     pks_with_norm: set = set()
   519|     if use_shard:
   520|         # line_pattern_pks is non-empty here, so a complete shard set genuinely missing
   521|         # lp_shard is a real inconsistency -- fall through with pks_with_norm empty so
   522|         # every pk is reported "missing" below, rather than crashing on a FileNotFoundError.
   523|         items_rows = _iter_csv_rows(lp_shard) if lp_shard.is_file() else []
   524|     else:
   525|         items_rows = _iter_csv_rows(items_csv)
   526|     for r in items_rows:
   527|         key = str(r.get("item_key", "") or r.get("k", ""))
   528|         if str(r.get("domain", "")) == "line_patterns" and key == "line_pattern.segments_norm_hash":
   529|             pks_with_norm.add(str(r.get("record_pk", "")))
   530|     missing = [pk for pk in line_pattern_pks if pk not in pks_with_norm]
   531|     if missing:
   532|         sample = ",".join(missing[:10])
   533|         more = "" if len(missing) <= 10 else f" (+{len(missing)-10} more)"
   534|         raise SystemExit(
   535|             "flatten/enrichment stage did not produce synthetic norm hashes before apply: "
   536|             f"missing line_pattern.segments_norm_hash for {len(missing)} line_patterns records. "
   537|             f"sample_record_pks={sample}{more}"
   538|         )
   539| 
   540| 
   541| def _emit_join_policy_diagnostics(rows: List[Dict[str, str]], diagnostics_dir: Path, domains: Optional[List[str]] = None) -> List[Dict[str, str]]:
   542|     import csv
   543| 
   544|     dom_filter = set(domains or [])
   545|     problems: List[Dict[str, str]] = []
   546|     for r in rows:
   547|         dom = str(r.get("domain", "")).strip()
   548|         if dom_filter and dom not in dom_filter:
   549|             continue
   550|         schema = str(r.get("join_key_schema", "")).strip()
   551|         status = str(r.get("join_key_status", "")).strip()
   552|         if schema == "sig_hash_as_join_key.v1" or status != "ok":
   553|             problems.append(
   554|                 {
   555|                     "domain": dom,
   556|                     "file_id": str(r.get("file_id", "")),
   557|                     "record_pk": str(r.get("record_pk", "")),
   558|                     "join_key_schema": schema,
   559|                     "join_key_status": status,
   560|                     "reason": "bootstrap_schema" if schema == "sig_hash_as_join_key.v1" else "non_ok_status",
   561|                 }
   562|             )
   563|     diagnostics_dir.mkdir(parents=True, exist_ok=True)
   564|     out_csv = diagnostics_dir / "join_policy_gate_diagnostics.csv"
   565|     fields = ["domain", "file_id", "record_pk", "join_key_schema", "join_key_status", "reason"]
   566|     with out_csv.open("w", encoding="utf-8", newline="") as f:
   567|         w = csv.DictWriter(f, fieldnames=fields)
   568|         w.writeheader()
   569|         for row in sorted(problems, key=lambda x: (x["domain"], x["file_id"], x["record_pk"])):
   570|             w.writerow(row)
   571|     return problems
   572| 
   573| 
   574| def _detect_surfaces(exports_dir: Path) -> Dict[str, int]:
   575|     names = [p.name for p in exports_dir.iterdir() if p.is_file() and p.name.lower().endswith(".json")]
   576|     details = sum(1 for n in names if n.lower().endswith(".details.json"))
   577|     index = sum(1 for n in names if n.lower().endswith(".index.json"))
   578|     legacy = sum(1 for n in names if n.lower().endswith(".legacy.json"))
   579|     fingerprint = sum(1 for n in names if n.lower().endswith("__fingerprint.json"))
   580|     plain = len(names) - details - index - legacy - fingerprint
   581|     return {
   582|         "details": details,
   583|         "index": index,
   584|         "legacy": legacy,
   585|         "fingerprint_json": fingerprint,
   586|         "plain_json": plain,
   587|         "total_json": len(names),
   588|     }
   589| 
   590| 
   591| def _merge_index_details(index_fp: Dict[str, Any], details_fp: Dict[str, Any]) -> Dict[str, Any]:
   592|     """Merge index (metadata) and details (domain payloads) into a single fingerprint object."""
   593|     merged = {**index_fp}
   594|     for key, value in details_fp.items():
   595|         # Domain payloads don't start with underscore; index metadata does
   596|         if not key.startswith("_") and key not in merged:
   597|             merged[key] = value
   598|     return merged
   599| 
   600| 
   601| def _pick_sample_file(exports_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
   602|     """Pick sample files for domain inference.
   603| 
   604|     Priority order:
   605|       1. *__fingerprint.json monolithic exports
   606|       2. *.details.json / *.index.json split exports
   607|       3. other non-legacy *.json files
   608|       4. *.legacy.json files
   609| 
   610|     Returns (index_path, details_path) tuple. Both may be None if no files found.
   611|     For split exports, returns both index and details paths.
   612|     For monolithic, plain, or legacy exports, returns (path, None).
   613|     """
   614|     fingerprints = sorted(exports_dir.glob("*__fingerprint.json"))
   615|     if fingerprints:
   616|         return (fingerprints[0], None)
   617| 
   618|     details = sorted(exports_dir.glob("*.details.json"))
   619|     index = sorted(exports_dir.glob("*.index.json"))
   620| 
   621|     if index and details:
   622|         # Split export: return first matching pair
   623|         index_by_stem = {p.stem.lower().replace('.index', ''): p for p in index}
   624|         details_by_stem = {p.stem.lower().replace('.details', ''): p for p in details}
   625|         for stem in sorted(index_by_stem.keys()):
   626|             if stem in details_by_stem:
   627|                 return (index_by_stem[stem], details_by_stem[stem])
   628|         # Fallback: return first index even without matching details
   629|         return (index[0], details_by_stem.get(index[0].stem.lower().replace('.index', '')))
   630| 
   631|     if index:
   632|         return (index[0], None)
   633| 
   634|     if details:
   635|         return (None, details[0])
   636| 
   637|     plain = sorted([p for p in exports_dir.glob("*.json") if not (p.name.lower().endswith(".legacy.json") or p.name.lower().endswith("__fingerprint.json"))])
   638|     if plain:
   639|         return (plain[0], None)
   640| 
   641|     legacy = sorted(exports_dir.glob("*.legacy.json"))
   642|     if legacy:
   643|         return (legacy[0], None)
   644| 
   645|     return (None, None)
   646| 
   647| 
   648| def _read_json(path: Path) -> Dict[str, Any]:
   649|     with path.open("r", encoding="utf-8") as f:
   650|         data = json.load(f)
   651|     if not isinstance(data, dict):
   652|         raise TypeError(f"JSON root must be object: {path}")
   653|     return data
   654| 
   655| 
   656| def _infer_domains(exports_dir: Path) -> List[str]:
   657|     """Infer domain names from sample export files.
   658| 
   659|     Handles split exports by merging index + details for reliable domain discovery.
   660|     """
   661|     index_path, details_path = _pick_sample_file(exports_dir)
   662| 
   663|     if index_path is None and details_path is None:
   664|         return []
   665| 
   666|     # Load and potentially merge files
   667|     fp: Dict[str, Any] = {}
   668|     if index_path and details_path:
   669|         # Split export: merge index + details
   670|         sys.stderr.write("[INFO run_extract_all] Found split exports. Merging index + details for domain inference.\n")
   671|         index_fp = _read_json(index_path)
   672|         details_fp = _read_json(details_path)
   673|         fp = _merge_index_details(index_fp, details_fp)
   674|     elif index_path:
   675|         fp = _read_json(index_path)
   676|     elif details_path:
   677|         fp = _read_json(details_path)
   678| 
   679|     # Try contract first (most reliable)
   680|     c = fp.get("_contract")
   681|     if isinstance(c, dict):
   682|         doms = c.get("domains")
   683|         if isinstance(doms, dict):
   684|             return sorted([str(k) for k in doms.keys()])
   685| 
   686|     # Try _domains (back-compat surface)
   687|     d = fp.get("_domains")
   688|     if isinstance(d, dict):
   689|         return sorted([str(k) for k in d.keys()])
   690| 
   691|     # Fallback: scan top-level keys for domain-like payloads
   692|     out: List[str] = []
   693|     for k, v in fp.items():
   694|         if not isinstance(k, str) or k.startswith("_"):
   695|             continue
   696|         if isinstance(v, dict) and (("records" in v) or ("status" in v) or ("domain_version" in v)):
   697|             out.append(k)
   698|     return sorted(out)
   699| 
   700| 
   701| def _parse_stage_csv(raw: Optional[str]) -> List[str]:
   702|     if not raw:
   703|         return []
   704|     return [s.strip().lower() for s in str(raw).split(',') if s.strip()]
   705| 
   706| 
   707| def _warn_deprecated_alias(flag: str, replacement: str) -> None:
   708|     sys.stderr.write(f"[WARN extract_all] Deprecated alias: use {replacement} instead of {flag}.\n")
   709| 
   710| 
   711| def _enforce_policy_gate(rows: List[Dict[str, str]], diagnostics_dir: Path, domains: Optional[List[str]], allow_sig_hash_join_key: bool) -> None:
   712|     problems = _emit_join_policy_diagnostics(rows, diagnostics_dir, domains)
   713|     if problems and not allow_sig_hash_join_key:
   714|         raise SystemExit(
   715|             "Join-policy gate failed: identity-mode join keys detected (join_key_schema=sig_hash_as_join_key.v1 or join_key_status!=ok). "
   716|             "Re-run with --stages flatten,discover,apply,split (or include authority/patterns with apply), "
   717|             "or use --allow-sig-hash-join-key for degraded exploratory analysis. "
   718|             f"Diagnostics: {diagnostics_dir / 'join_policy_gate_diagnostics.csv'}"
   719|         )
   720|     if problems and allow_sig_hash_join_key:
   721|         sys.stderr.write("\n" + "!" * 80 + "\n")
   722|         sys.stderr.write("[WARN extract_all] --allow-sig-hash-join-key enabled; proceeding with DEGRADED identity-mode clustering (not for governance conclusions).\n")
   723|         sys.stderr.write(f"[WARN extract_all] Diagnostics: {diagnostics_dir / 'join_policy_gate_diagnostics.csv'}\n")
   724|         sys.stderr.write("!" * 80 + "\n\n")
   725| 
   726| 
```
