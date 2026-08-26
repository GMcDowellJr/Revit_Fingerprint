# Chunk of tools/run_extract_all.py

- Source relative path: `tools/run_extract_all.py`
- Chunk: 1 of 3
- Original line range: 1-499
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _append_line_pattern_synthetic_norm_hash, _discover_domains_from_exports, _ensure_dir, _resolve_sig_hash_policy_path, _apply_sig_hash_to_phase0, _apply_sig_hash_to_phase0._load_items_for_domain, _run, _read_csv_rows, _iter_csv_rows, _check_governance_field_completeness, _ensure_domain_scoped_identity_items
- Source SHA-256: 6097e03c70f161dde5df1eb1e3887c55c0c5ff53ad8f51c123db9238e5ca6429
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| #!/usr/bin/env python3
     2| from __future__ import annotations
     3| 
     4| import argparse
     5| import csv
     6| import hashlib
     7| import json
     8| import os
     9| import re
    10| import subprocess
    11| import sys
    12| import time
    13| from pathlib import Path
    14| from typing import Any, Dict, List, Optional, Tuple
    15| 
    16| SCRIPT_DIR = Path(__file__).resolve().parent
    17| REPO_ROOT = SCRIPT_DIR.parent
    18| if str(REPO_ROOT) not in sys.path:
    19|     sys.path.insert(0, str(REPO_ROOT))
    20| 
    21| from emit_element_dominance import emit_element_dominance
    22| from extractor import emit_analysis, emit_records
    23| from bundle_analysis.common import atomic_write_csv, read_csv_rows
    24| from bundle_analysis.reference_bundle import write_sidecar
    25| from core.sig_hash_policy import load_sig_hash_policies, get_domain_sig_hash_policy
    26| from core.sig_hash_builder import build_sig_hash_from_policy
    27| from na_token import is_na_token
    28| 
    29| try:
    30|     csv.field_size_limit(sys.maxsize)
    31| except Exception:
    32|     pass
    33| 
    34| SUPPRESSED_DOWNSTREAM_DOMAINS = {"object_styles_imported"}
    35| 
    36| 
    37| _LP_SEGMENT_KEY_RE = re.compile(r"^line_pattern\.(?:seg|segment)\[(\d{3})\]\.(kind|length)$")
    38| _LP_SEGMENT_COUNT_KEY = "line_pattern.segment_count"
    39| 
    40| 
    41| def _append_line_pattern_synthetic_norm_hash(items_csv: Path) -> Dict[str, int]:
    42|     """Append synthetic line_pattern.segments_norm_hash rows to the line_patterns shard
    43|     (or, for a legacy monolithic-only phase0 dir with no shards, to identity_items.csv)."""
    44|     shard_dir = items_csv.parent / "identity_items_by_domain"
    45|     lp_shard = shard_dir / "line_patterns.csv"
    46|     use_shard = (shard_dir / ".complete").is_file() and lp_shard.is_file()
    47| 
    48|     if not use_shard and not items_csv.is_file():
    49|         return {"total": 0, "ok": 0, "missing": 0}
    50| 
    51|     if use_shard:
    52|         with lp_shard.open("r", encoding="utf-8-sig", newline="") as f:
    53|             rows = list(csv.DictReader(f))
    54|             fieldnames = list(rows[0].keys() if rows else [
    55|                 "schema_version", "export_run_id", "domain", "record_pk",
    56|                 "item_key", "item_value", "item_value_type", "item_role",
    57|             ])
    58|     else:
    59|         with items_csv.open("r", encoding="utf-8-sig", newline="") as f:
    60|             rows = list(csv.DictReader(f))
    61|             fieldnames = list((rows[0].keys() if rows else [
    62|                 "schema_version", "export_run_id", "file_id", "domain", "record_id", "record_ordinal", "record_pk", "item_index", "k", "q", "v",
    63|             ]))
    64|     key_col = "k" if "k" in fieldnames else "item_key"
    65|     quality_col = "q" if "q" in fieldnames else "item_value_type"
    66|     value_col = "v" if "v" in fieldnames else "item_value"
    67|     item_index_col = "item_index" if "item_index" in fieldnames else "item_role"
    68| 
    69|     grouped: Dict[str, List[Dict[str, str]]] = {}
    70|     already_augmented: set = set()
    71|     for r in rows:
    72|         if str(r.get("domain", "")) != "line_patterns":
    73|             continue
    74|         pk = str(r.get("record_pk", ""))
    75|         if str(r.get(key_col, "")) == "line_pattern.segments_norm_hash":
    76|             already_augmented.add(pk)
    77|         grouped.setdefault(pk, []).append(r)
    78| 
    79|     out_rows: List[Dict[str, str]] = []
    80|     ok = 0
    81|     missing = 0
    82|     for record_pk, group in grouped.items():
    83|         if record_pk in already_augmented:
    84|             continue
    85|         seg_rows = [r for r in group if _LP_SEGMENT_KEY_RE.match(str(r.get(key_col, "")))]
    86|         status = "ok"
    87|         hash_v = ""
    88| 
    89|         if not seg_rows:
    90|             seg_count_rows = [r for r in group if str(r.get(key_col, "")) == _LP_SEGMENT_COUNT_KEY]
    91|             seg_count_v = str(seg_count_rows[0].get(value_col, "")).strip() if seg_count_rows else ""
    92|             seg_count_q = str(seg_count_rows[0].get(quality_col, "")).strip() if seg_count_rows else ""
    93|             try:
    94|                 seg_count_is_zero = int(seg_count_v) == 0
    95|             except Exception:
    96|                 seg_count_is_zero = False
    97| 
    98|             if seg_count_q == "ok" and seg_count_is_zero:
    99|                 hash_v = hashlib.md5("segment_count=0".encode("utf-8")).hexdigest()
   100|                 ok += 1
   101|             else:
   102|                 status = "missing"
   103|                 missing += 1
   104|         elif any(str(r.get(quality_col, "")) != "ok" for r in seg_rows):
   105|             status = "missing"
   106|             missing += 1
   107|         else:
   108|             segments: Dict[int, Dict[str, float]] = {}
   109|             parse_error = False
   110|             for r in seg_rows:
   111|                 m = _LP_SEGMENT_KEY_RE.match(str(r.get(key_col, "")))
   112|                 if not m:
   113|                     continue
   114|                 idx = int(m.group(1))
   115|                 key = m.group(2)
   116|                 segments.setdefault(idx, {})
   117|                 try:
   118|                     if key == "kind":
   119|                         segments[idx]["kind"] = int(str(r.get(value_col, "")))
   120|                     else:
   121|                         segments[idx]["length"] = float(str(r.get(value_col, "")))
   122|                 except Exception:
   123|                     parse_error = True
   124|                     break
   125| 
   126|             if parse_error or any("kind" not in d or "length" not in d for d in segments.values()):
   127|                 status = "missing"
   128|                 missing += 1
   129|             else:
   130|                 ordered = [(idx, int(v["kind"]), float(v["length"])) for idx, v in sorted(segments.items())]
   131|                 non_dot_total = sum(length for _, kind, length in ordered if kind != 2)
   132|                 has_non_dot = any(kind != 2 for _, kind, _ in ordered)
   133|                 dot_count = sum(1 for _, kind, _ in ordered if kind == 2)
   134|                 eff_total = non_dot_total if has_non_dot else float(dot_count)
   135|                 tokens: List[str] = []
   136|                 for idx, kind, length in ordered:
   137|                     if kind == 2:
   138|                         eff_length = 0.0 if has_non_dot else 1.0
   139|                     else:
   140|                         eff_length = length
   141|                     norm = (eff_length / eff_total) if eff_total > 0 else 0.0
   142|                     tokens.append(f"seg[{idx:03d}].kind={kind}")
   143|                     tokens.append(f"seg[{idx:03d}].norm_length={norm:.6f}")
   144|                 hash_v = hashlib.md5("|".join(tokens).encode("utf-8")).hexdigest()
   145|                 ok += 1
   146| 
   147|         base = group[0]
   148|         out_rows.append({
   149|             "schema_version": str(base.get("schema_version", "")),
   150|             "export_run_id": str(base.get("export_run_id", "")),
   151|             "file_id": str(base.get("file_id", "")),
   152|             "domain": "line_patterns",
   153|             "record_id": str(base.get("record_id", "")),
   154|             "record_ordinal": str(base.get("record_ordinal", "")),
   155|             "record_pk": record_pk,
   156|             item_index_col: "synthetic",
   157|             key_col: "line_pattern.segments_norm_hash",
   158|             quality_col: status,
   159|             value_col: hash_v,
   160|         })
   161| 
   162|     if out_rows:
   163|         rows.extend(out_rows)
   164|         rows = sorted(
   165|             rows,
   166|             key=lambda r: (
   167|                 str(r.get("export_run_id", "")),
   168|                 str(r.get("domain", "")),
   169|                 str(r.get("record_pk", "")),
   170|                 str(r.get(key_col, "")),
   171|                 str(r.get(value_col, "")),
   172|             ),
   173|         )
   174|         if use_shard:
   175|             with lp_shard.open("w", encoding="utf-8", newline="") as f:
   176|                 w = csv.DictWriter(f, fieldnames=fieldnames)
   177|                 w.writeheader()
   178|                 for r in rows:
   179|                     w.writerow({k: r.get(k, "") for k in fieldnames})
   180|         else:
   181|             with items_csv.open("w", encoding="utf-8", newline="") as f:
   182|                 w = csv.DictWriter(f, fieldnames=fieldnames)
   183|                 w.writeheader()
   184|                 for r in rows:
   185|                     w.writerow({k: r.get(k, "") for k in fieldnames})
   186| 
   187|     return {"total": len(out_rows), "ok": ok, "missing": missing}
   188| 
   189| 
   190| def _discover_domains_from_exports(exports_dir: Path) -> List[str]:
   191|     """
   192|     Best-effort discovery of domains from fingerprint JSON exports.
   193|     Assumes domains are top-level keys excluding meta keys (leading underscore) and known non-domain keys.
   194|     Deterministic: returns sorted list.
   195|     """
   196|     exports_dir = Path(exports_dir)
   197|     domains: set[str] = set()
   198| 
   199|     # Prefer fingerprint files; fall back to generic .json if none found.
   200|     candidates = sorted(exports_dir.glob("*__fingerprint.json"))
   201|     if not candidates:
   202|         candidates = [p for p in exports_dir.glob("*.json") if not p.name.lower().endswith(".legacy.json")]
   203|     if not candidates:
   204|         return []
   205| 
   206|     for p in candidates[:200]:
   207|         try:
   208|             with p.open("r", encoding="utf-8") as f:
   209|                 data = json.load(f)
   210|         except Exception:
   211|             continue
   212| 
   213|         if not isinstance(data, dict):
   214|             continue
   215| 
   216|         for k, v in data.items():
   217|             if not isinstance(k, str):
   218|                 continue
   219|             if k.startswith("_"):
   220|                 continue
   221|             if k in ("artifacts",):
   222|                 continue
   223|             # Domain payloads are typically dict-like.
   224|             if isinstance(v, dict):
   225|                 domains.add(k)
   226| 
   227|     return sorted(domains, key=lambda s: s.lower())
   228| 
   229| 
   230| def _ensure_dir(p: Path) -> None:
   231|     p.mkdir(parents=True, exist_ok=True)
   232| 
   233| 
   234| def _resolve_sig_hash_policy_path(explicit: Optional[str], results_root: Path) -> Optional[Path]:
   235|     if explicit:
   236|         p = Path(explicit).resolve()
   237|         return p if p.is_file() else None
   238|     candidate1 = (results_root / "policies" / "domain_sig_hash_policies.json").resolve()
   239|     if candidate1.is_file():
   240|         return candidate1
   241|     # CWD-relative (works when invoked from repo root)
   242|     candidate2 = Path("policies/domain_sig_hash_policies.json").resolve()
   243|     if candidate2.is_file():
   244|         return candidate2
   245|     # Repo-root-relative (works regardless of CWD)
   246|     candidate3 = (REPO_ROOT / "policies" / "domain_sig_hash_policies.json").resolve()
   247|     if candidate3.is_file():
   248|         return candidate3
   249|     return None
   250| 
   251| 
   252| def _apply_sig_hash_to_phase0(phase0_dir: Path, policy_path: Path, domains: Optional[List[str]] = None):
   253|     policies = load_sig_hash_policies(str(policy_path))
   254|     dom_filter = set(domains or [])
   255|     diag = {
   256|         "policy_path": str(policy_path),
   257|         "files_processed": 0,
   258|         "records_processed": 0,
   259|         "records_hashed": 0,
   260|         "domains_without_policy": [],
   261|         "records_blocked": 0,
   262|         "records_degraded": 0,
   263|     }
   264|     domains_without = set()
   265|     records_csv = phase0_dir / "records.csv"
   266|     items_csv = phase0_dir / "identity_items.csv"
   267|     native_shard_dir = phase0_dir / "identity_items_by_domain"
   268|     if not records_csv.is_file():
   269|         return diag
   270|     if (native_shard_dir / ".complete").is_file():
   271|         shard_dir: Optional[Path] = native_shard_dir
   272|     elif items_csv.is_file():
   273|         # Legacy fallback for a phase0 dir produced before native shard writing
   274|         # existed (no identity_items_by_domain/.complete): derive shards from the
   275|         # monolithic file, refreshing them only if stale.
   276|         shard_dir = _ensure_domain_scoped_identity_items(phase0_dir)
   277|     else:
   278|         return diag
   279|     records = _read_csv_rows(records_csv)
   280| 
   281|     # Group record indices by domain so each domain's identity_items shard is
   282|     # loaded once, fully processed, and released before moving to the next
   283|     # domain -- holding every domain's items in memory simultaneously (the
   284|     # previous unbounded grouped_cache) is what exhausted memory on large
   285|     # corpora (millions of records across hundreds of files).
   286|     indices_by_domain: Dict[str, List[int]] = {}
   287|     for idx, row in enumerate(records):
   288|         dom = str(row.get("domain", "")).strip()
   289|         if dom_filter and dom not in dom_filter:
   290|             continue
   291|         indices_by_domain.setdefault(dom, []).append(idx)
   292| 
   293|     def _load_items_for_domain(domain: str) -> Dict[str, List[Dict[str, Any]]]:
   294|         out: Dict[str, List[Dict[str, Any]]] = {}
   295|         src = (shard_dir / f"{domain}.csv") if shard_dir is not None else items_csv
   296|         if not src.is_file():
   297|             return out
   298|         for r in _iter_csv_rows(src):
   299|             if str(r.get("domain", "")).strip() != domain:
   300|                 continue
   301|             pk = str(r.get("record_pk", ""))
   302|             k = str(r.get("item_key", "") or r.get("k", ""))
   303|             if not pk or not k:
   304|                 continue
   305|             out.setdefault(pk, []).append({"k": k, "v": r.get("v", r.get("item_value")), "q": r.get("q", r.get("item_value_type"))})
   306|         return out
   307| 
   308|     # Stream sig_basis_items.csv rows straight to disk instead of accumulating
   309|     # them in a single list -- across a large corpus the basis-item count is a
   310|     # multiple of the record count and the accumulated list was the other
   311|     # major contributor to the MemoryError.
   312|     basis_csv = phase0_dir / "sig_basis_items.csv"
   313|     basis_tmp_csv = phase0_dir / "sig_basis_items.csv.tmp"
   314|     basis_fields = ["record_pk", "domain", "item_key", "ordinal"]
   315|     basis_rows_written = 0
   316|     with basis_tmp_csv.open("w", encoding="utf-8", newline="") as bf:
   317|         bw = csv.DictWriter(bf, fieldnames=basis_fields)
   318|         bw.writeheader()
   319|         for dom, idx_list in indices_by_domain.items():
   320|             pol = get_domain_sig_hash_policy(policies, dom)
   321|             if not isinstance(pol, dict):
   322|                 domains_without.add(dom)
   323|                 continue
   324|             dom_items = _load_items_for_domain(dom)
   325|             for idx in idx_list:
   326|                 row = records[idx]
   327|                 pk = str(row.get("record_pk", ""))
   328|                 rec_items = dom_items.get(pk, [])
   329|                 diag["records_processed"] += 1
   330|                 sig_hash, status, reasons, hash_items = build_sig_hash_from_policy(
   331|                     domain_policy=pol,
   332|                     items=rec_items,
   333|                     status_reasons=[],
   334|                 )
   335|                 row["sig_hash"] = "" if sig_hash is None else str(sig_hash)
   336|                 if str(row.get("join_key_schema", "")) == "sig_hash_as_join_key.v1":
   337|                     row["join_hash"] = row["sig_hash"]
   338|                 prior_status = str(row.get("status", "")).strip()
   339|                 if prior_status == "blocked":
   340|                     # Extractor-blocked records are sticky — the sig_hash stage cannot upgrade them.
   341|                     # Exception: records blocked by a *prior apply run* must be re-evaluated so that
   342|                     # policy corrections or updated identity_items can take effect.
   343|                     # Distinguisher: the apply stage writes "identity.incomplete:required_not_ok:<k>";
   344|                     # extractors write "identity.incomplete:<q>:<k>" (e.g. "identity.incomplete:missing:…").
   345|                     # Matching on the apply-specific "required_not_ok" middle segment avoids
   346|                     # misclassifying genuine extractor blocks as apply-stage blocks.
   347|                     prior_reasons = [r for r in str(row.get("status_reasons", "")).split("|") if r]
   348|                     apply_stage_blocked = any(r.startswith("identity.incomplete:required_not_ok:") for r in prior_reasons)
   349|                     if not apply_stage_blocked:
   350|                         pass  # genuine extractor block — preserve it
   351|                     else:
   352|                         row["status"] = str(status)
   353|                         row["status_reasons"] = "|".join(reasons)
   354|                 else:
   355|                     row["status"] = str(status)
   356|                     row["status_reasons"] = "|".join(reasons)
   357|                 row["sig_basis_schema"] = str(pol.get("sig_hash_schema") or "")
   358|                 for ordinal, it in enumerate(hash_items):
   359|                     k = it.get("k")
   360|                     if isinstance(k, str) and k:
   361|                         bw.writerow({"record_pk": pk, "domain": dom, "item_key": k, "ordinal": str(ordinal)})
   362|                         basis_rows_written += 1
   363|                 if sig_hash is not None:
   364|                     diag["records_hashed"] += 1
   365|                 if status == "blocked":
   366|                     diag["records_blocked"] += 1
   367|                 elif status == "degraded":
   368|                     diag["records_degraded"] += 1
   369|             del dom_items
   370|     if records:
   371|         fieldnames = list(records[0].keys())
   372|         for extra in ("sig_hash", "sig_basis_schema", "status", "status_reasons"):
   373|             if extra not in fieldnames:
   374|                 fieldnames.append(extra)
   375|         # Drop sig_basis_keys_used if it was written by a prior run; key traceability
   376|         # is now in sig_basis_items.csv which is more query-friendly at scale.
   377|         fieldnames = [f for f in fieldnames if f != "sig_basis_keys_used"]
   378|         with records_csv.open("w", encoding="utf-8", newline="") as f:
   379|             w = csv.DictWriter(f, fieldnames=fieldnames)
   380|             w.writeheader()
   381|             for r in records:
   382|                 w.writerow({k: r.get(k, "") for k in fieldnames})
   383|     if basis_rows_written:
   384|         basis_tmp_csv.replace(basis_csv)
   385|         diag["sig_basis_items_written"] = basis_rows_written
   386|     else:
   387|         basis_tmp_csv.unlink(missing_ok=True)
   388|     diag["files_processed"] = 1
   389|     diag["domains_without_policy"] = sorted(domains_without)
   390|     if domains_without:
   391|         sys.stderr.write(
   392|             "[WARN extract_all] sig_hash stage: {} domain(s) have no policy entry — "
   393|             "sig_hash will be empty for their records: {}\n".format(
   394|                 len(domains_without), ", ".join(sorted(domains_without))
   395|             )
   396|         )
   397|     return diag
   398| 
   399| 
   400| def _run(cmd: List[str], *, env: Dict[str, str]) -> None:
   401|     start = time.time()
   402|     print(f"[extract_all] RUN: {' '.join(cmd)}", flush=True)
   403|     subprocess.run(cmd, check=True, env=env)
   404|     print(f"[extract_all] DONE ({time.time() - start:.1f}s): {cmd[1] if len(cmd) > 1 else cmd[0]}", flush=True)
   405| 
   406| 
   407| def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
   408|     with path.open("r", encoding="utf-8-sig", newline="") as f:
   409|         return [{str(k): "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(f)]
   410| 
   411| 
   412| def _iter_csv_rows(path: Path):
   413|     with path.open("r", encoding="utf-8-sig", newline="") as f:
   414|         for row in csv.DictReader(f):
   415|             yield {str(k): "" if v is None else str(v) for k, v in row.items()}
   416| 
   417| 
   418| _GOVERNANCE_COMPLETENESS_COLUMNS = ("client_label", "business_center_label")
   419| 
   420| 
   421| def _check_governance_field_completeness(meta_rows: List[Dict[str, str]]) -> None:
   422|     """Hard-fail if any file_metadata.csv row has a blank or N/A-spelled
   423|     client_label or business_center_label. These columns are expected to carry
   424|     real values (e.g. "InternalEnterprise" for enterprise work, a bare
   425|     numeric business-center code, or "0000"/"BC_0000" for enterprise-scoped
   426|     rows) after the manual annotation pause between Run A and Run B — this is
   427|     a pure completeness check, not a fallback; it does not fill in anything.
   428|     """
   429|     offenders: Dict[str, List[str]] = {}
   430|     for row in meta_rows:
   431|         export_run_id = row.get("export_run_id", "").strip() or "<missing export_run_id>"
   432|         bad_columns = [
   433|             column
   434|             for column in _GOVERNANCE_COMPLETENESS_COLUMNS
   435|             if not (raw := row.get(column, "").strip()) or is_na_token(raw)
   436|         ]
   437|         if bad_columns:
   438|             offenders[export_run_id] = bad_columns
   439|     if offenders:
   440|         detail_lines = "\n".join(
   441|             f"  {export_run_id}: {', '.join(offenders[export_run_id])}"
   442|             for export_run_id in sorted(offenders)
   443|         )
   444|         raise SystemExit(
   445|             "[FATAL extract_all] governance-field completeness gate failed: "
   446|             f"{len(offenders)} row(s) in file_metadata.csv have a blank or N/A "
   447|             "client_label/business_center_label. Fill in real values for these "
   448|             "rows (see corpus_update_runbook.ps1's manual-pause instructions) "
   449|             "before re-running Run B.\n" + detail_lines
   450|         )
   451| 
   452| 
   453| def _ensure_domain_scoped_identity_items(phase0_dir: Path) -> Optional[Path]:
   454|     src = phase0_dir / "identity_items.csv"
   455|     if not src.is_file():
   456|         return None
   457| 
   458|     shard_dir = phase0_dir / "identity_items_by_domain"
   459|     shard_dir.mkdir(parents=True, exist_ok=True)
   460|     sentinel = shard_dir / ".complete"
   461| 
   462|     try:
   463|         if sentinel.is_file():
   464|             stored = sentinel.read_text(encoding="utf-8").strip()
   465|             if stored == str(src.stat().st_mtime):
   466|                 return shard_dir
   467|     except OSError:
   468|         pass
   469| 
   470|     for old in shard_dir.glob("*.csv"):
   471|         old.unlink(missing_ok=True)
   472| 
   473|     handles: Dict[str, Any] = {}
   474|     writers: Dict[str, csv.DictWriter] = {}
   475|     try:
   476|         with src.open("r", encoding="utf-8-sig", newline="") as f:
   477|             reader = csv.DictReader(f)
   478|             fieldnames = list(reader.fieldnames or [])
   479|             if not fieldnames:
   480|                 return shard_dir
   481|             for row in reader:
   482|                 domain = str(row.get("domain", "")).strip()
   483|                 if not domain:
   484|                     continue
   485|                 if domain not in writers:
   486|                     fp = (shard_dir / f"{domain}.csv").open("w", encoding="utf-8", newline="")
   487|                     handles[domain] = fp
   488|                     w = csv.DictWriter(fp, fieldnames=fieldnames)
   489|                     w.writeheader()
   490|                     writers[domain] = w
   491|                 writers[domain].writerow({k: row.get(k, "") for k in fieldnames})
   492|     finally:
   493|         for fp in handles.values():
   494|             fp.close()
   495| 
   496|     sentinel.write_text(str(src.stat().st_mtime), encoding="utf-8")
   497|     return shard_dir
   498| 
   499| 
```
