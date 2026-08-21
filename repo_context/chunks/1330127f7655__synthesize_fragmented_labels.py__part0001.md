# Chunk of tools/label_synthesis/synthesize_fragmented_labels.py

- Source relative path: `tools/label_synthesis/synthesize_fragmented_labels.py`
- Chunk: 1 of 2
- Original line range: 1-504
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _collect_union_bundle_join_hashes, _load_governance_join_hashes, _strip_json_fences, _call_llm, _groups_vocab_path, load_groups_vocab, save_groups_vocab, _load_identity_items_from_csv, _load_representative_identity_items, _get_domain_records
- Source SHA-256: 4ae8d38bd620968b5c02d23c147ff7d09f661014f6d5a52c2f0040495114a524
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| """
     2| tools/label_synthesis/synthesize_fragmented_labels.py
     3| 
     4| Offline batch script to pre-populate the LLM name cache for fragmented patterns.
     5| 
     6| This script:
     7|   1. Loads joinhash_label_population.csv for the given domain
     8|   2. Identifies join_hashes where modal label promotion fails (fragmented)
     9|   3. Finds representative identity_items for each fragmented join_hash
    10|   4. Calls Claude API to synthesize canonical name candidates
    11|   5. Writes results to llm_name_cache.json (keyed by join_hash, stable across re-runs)
    12| 
    13| The LLM cache is then read by label_resolver.py during v21_emit.py runs.
    14| Never called at emit time — this is a background enrichment step.
    15| 
    16| Usage:
    17|     python -m tools.label_synthesis.synthesize_fragmented_labels \
    18|         --exports-dir Results_v21/exports \
    19|         --analysis-dir Results_v21/analysis_v21 \
    20|         --domain dimension_types \
    21|         --cache label_synthesis/llm_name_cache.json \
    22|         [--dry-run]              # print prompts/responses without writing
    23|         [--force-refresh]        # re-synthesize even if join_hash already in cache
    24|         [--only-unreviewed]      # only synthesize entries missing "reviewed": true
    25|         [--review-csv out.csv]   # emit pending-review CSV for curator workflow
    26| """
    27| 
    28| from __future__ import annotations
    29| 
    30| import argparse
    31| import csv
    32| import json
    33| import os
    34| import re
    35| import sys
    36| import threading
    37| import time
    38| from concurrent.futures import ThreadPoolExecutor, as_completed
    39| from datetime import date
    40| from pathlib import Path
    41| from typing import Any, Dict, List, Optional, Tuple
    42| 
    43| 
    44| from .label_resolver import (
    45|     is_fragmented,
    46|     load_label_population,
    47|     load_llm_cache,
    48|     save_llm_cache,
    49|     MODAL_THRESHOLD,
    50| )
    51| 
    52| 
    53| def _collect_union_bundle_join_hashes(
    54|     *,
    55|     domain: str,
    56|     segments_root: str,
    57|     registry_file: str,
    58| ) -> set:
    59|     """Collect bundle member join_hashes across active segments and purge views."""
    60|     eligible_jhs: set = set()
    61|     segments_checked = 0
    62|     root = Path(segments_root)
    63| 
    64|     with Path(registry_file).open(newline="", encoding="utf-8-sig") as f:
    65|         for row in csv.DictReader(f):
    66|             run_type = (row.get("run_type") or "").strip()
    67|             status = (row.get("status") or "").strip()
    68|             if run_type not in {"bundle", "reference"}:
    69|                 continue
    70|             if status in {"skip", "registration"}:
    71|                 continue
    72| 
    73|             output_folder = (row.get("output_folder") or "").strip()
    74|             if not output_folder:
    75|                 print(
    76|                     "[union_discovery] WARN missing output_folder "
    77|                     f"for segment_id={row.get('segment_id', '')}; skipping segment."
    78|                 )
    79|                 continue
    80| 
    81|             segments_checked += 1
    82|             seg_out = root / output_folder
    83|             patterns_path = seg_out / "results" / "analysis" / "domain_patterns.csv"
    84|             if not patterns_path.exists():
    85|                 print(
    86|                     "[union_discovery] WARN domain_patterns.csv not found "
    87|                     f"at {patterns_path}; skipping segment."
    88|                 )
    89|                 continue
    90| 
    91|             pid_to_jh: dict = {}
    92|             with patterns_path.open(newline="", encoding="utf-8-sig") as pf:
    93|                 for pattern_row in csv.DictReader(pf):
    94|                     if pattern_row.get("domain") != domain:
    95|                         continue
    96|                     pid = (pattern_row.get("pattern_id") or "").strip()
    97|                     raw_src = (pattern_row.get("source_cluster_id") or "").strip()
    98|                     join_hash = raw_src.split("|")[-1] if raw_src else ""
    99|                     if pid and join_hash:
   100|                         pid_to_jh[pid] = join_hash
   101| 
   102|             for purge_view in ("all", "used"):
   103|                 membership_path = (
   104|                     seg_out
   105|                     / "results"
   106|                     / "bundle_analysis"
   107|                     / purge_view
   108|                     / domain
   109|                     / "bundle_membership.csv"
   110|                 )
   111|                 if not membership_path.exists():
   112|                     continue
   113|                 with membership_path.open(newline="", encoding="utf-8-sig") as mf:
   114|                     for membership_row in csv.DictReader(mf):
   115|                         pid = (membership_row.get("pattern_id") or "").strip()
   116|                         if not pid:
   117|                             continue
   118|                         join_hash = pid_to_jh.get(pid)
   119|                         if join_hash:
   120|                             eligible_jhs.add(join_hash)
   121|                         else:
   122|                             print(
   123|                                 "[union_discovery] WARN pattern_id "
   124|                                 f"{pid!r} from {membership_path} not found in "
   125|                                 f"{patterns_path}; skipping row."
   126|                             )
   127| 
   128|     print(
   129|         f"[union_discovery] domain={domain} "
   130|         f"segments_checked={segments_checked} "
   131|         f"join_hashes_eligible={len(eligible_jhs)}"
   132|     )
   133|     return eligible_jhs
   134| 
   135| 
   136| def _load_governance_join_hashes(
   137|     *,
   138|     domain: str,
   139|     filter_mode: str,
   140|     analysis_dir: str,
   141|     domain_patterns_csv: Optional[str] = None,
   142|     bundle_dir: Optional[str] = None,
   143|     segments_root: Optional[str] = None,
   144|     registry_file: Optional[str] = None,
   145| ) -> Optional[set]:
   146|     """
   147|     Return the set of join_hashes eligible for synthesis under filter_mode.
   148|     Returns None when filter_mode == 'all' (no filtering).
   149| 
   150|     Join surface:
   151|       domain_patterns.csv  →  source_cluster_id column is pipe-delimited:
   152|                                {domain}|{join_key_schema}|{join_hash}
   153|                                join_hash = split('|')[-1]
   154| 
   155|       bundle_membership.csv  →  (domain, pattern_id) rows;
   156|                                  pattern_id joins to domain_patterns.pattern_id
   157|                                  to recover join_hash
   158|     """
   159|     if filter_mode == "all":
   160|         return None
   161| 
   162|     union_bundle_mode = (
   163|         filter_mode in ("bundles", "governance")
   164|         and bool(segments_root)
   165|         and bool(registry_file)
   166|     )
   167|     needs_corpus_patterns = (
   168|         filter_mode in ("candidates", "governance")
   169|         or (filter_mode == "bundles" and not union_bundle_mode)
   170|     )
   171| 
   172|     candidate_jhs: set = set()
   173|     jh_to_pid: dict = {}
   174|     pid_to_jh: dict = {}
   175| 
   176|     if needs_corpus_patterns:
   177|         analysis_path = Path(analysis_dir)
   178|         candidate_paths = []
   179|         if domain_patterns_csv:
   180|             candidate_paths.append(Path(domain_patterns_csv))
   181|         else:
   182|             candidate_paths.extend([
   183|                 analysis_path / "domain_patterns.csv",
   184|                 analysis_path.parent / "analysis_v21" / "domain_patterns.csv",
   185|             ])
   186|         dp_path = next((p for p in candidate_paths if p.exists()), candidate_paths[0])
   187|         if not dp_path.exists():
   188|             searched_paths = ", ".join(str(p) for p in candidate_paths)
   189|             raise FileNotFoundError(
   190|                 f"domain_patterns.csv is required for --filter-mode {filter_mode!r} "
   191|                 f"but was not found. Looked at: {searched_paths}"
   192|             )
   193| 
   194|         with dp_path.open(newline="", encoding="utf-8") as f:
   195|             for row in csv.DictReader(f):
   196|                 if row.get("domain") != domain:
   197|                     continue
   198|                 pid = row.get("pattern_id", "").strip()
   199|                 raw_src = row.get("source_cluster_id", "").strip()
   200|                 jh = raw_src.split("|")[-1] if raw_src else ""
   201|                 is_cand = row.get("is_candidate_standard", "").strip().lower()
   202|                 if pid and jh:
   203|                     jh_to_pid[jh] = pid
   204|                     pid_to_jh[pid] = jh
   205|                 if is_cand == "true" and jh:
   206|                     candidate_jhs.add(jh)
   207| 
   208|     bundle_jhs: set = set()
   209|     if filter_mode in ("bundles", "governance"):
   210|         if union_bundle_mode:
   211|             bundle_jhs = _collect_union_bundle_join_hashes(
   212|                 domain=domain,
   213|                 segments_root=segments_root,
   214|                 registry_file=registry_file,
   215|             )
   216|         elif bundle_dir is None:
   217|             raise ValueError(
   218|                 f"--bundle-dir is required when --filter-mode is {filter_mode!r}"
   219|             )
   220|         else:
   221|             bm_path = Path(bundle_dir) / "bundle_membership.csv"
   222|             if bm_path.exists():
   223|                 with bm_path.open(newline="", encoding="utf-8") as f:
   224|                     for row in csv.DictReader(f):
   225|                         if row.get("domain") != domain:
   226|                             continue
   227|                         pid = row.get("pattern_id", "").strip()
   228|                         if pid and pid in pid_to_jh:
   229|                             bundle_jhs.add(pid_to_jh[pid])
   230|             else:
   231|                 print(
   232|                     f"  WARN: bundle_membership.csv not found at {bm_path}. "
   233|                     f"Bundle filter will match nothing."
   234|                 )
   235| 
   236|     if filter_mode == "candidates":
   237|         result = candidate_jhs
   238|     elif filter_mode == "bundles":
   239|         result = bundle_jhs
   240|     elif filter_mode == "governance":
   241|         result = candidate_jhs | bundle_jhs
   242|     else:
   243|         raise ValueError(f"Unknown filter_mode: {filter_mode!r}")
   244| 
   245|     print(
   246|         f"  [filter_mode={filter_mode}] "
   247|         f"candidates={len(candidate_jhs)} "
   248|         f"bundle_members={len(bundle_jhs)} "
   249|         f"eligible={len(result)}"
   250|     )
   251|     return result
   252| 
   253| 
   254| # ---------------------------------------------------------------------------
   255| # API call
   256| # ---------------------------------------------------------------------------
   257| 
   258| def _strip_json_fences(raw_text: str) -> str:
   259|     cleaned = raw_text.strip()
   260|     if cleaned.startswith("```"):
   261|         cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
   262|         cleaned = re.sub(r"\s*```$", "", cleaned)
   263|     return cleaned.strip()
   264| 
   265| 
   266| def _call_llm(
   267|     system_prompt: str,
   268|     user_prompt: str,
   269|     *,
   270|     provider: str = "anthropic",
   271|     model: str | None = None,
   272|     max_tokens: int = 512,
   273|     retry_count: int = 2,
   274|     retry_delay: float = 2.0,
   275|     groups_vocab: Optional[Dict[str, str]] = None,
   276| ) -> Optional[Dict[str, Any]]:
   277|     """Call the configured LLM provider and parse JSON response."""
   278|     resolved_prompt = user_prompt
   279|     if groups_vocab:
   280|         lines = ["EXISTING GROUPS IN THIS DOMAIN:"]
   281|         for label, definition in sorted(groups_vocab.items()):
   282|             lines.append(f"  {label}: {definition}")
   283|         resolved_prompt = resolved_prompt.replace(
   284|             "EXISTING GROUPS IN THIS DOMAIN: (none yet — you are establishing the vocabulary)",
   285|             "\n".join(lines),
   286|         )
   287| 
   288|     provider = provider.lower().strip()
   289|     if provider not in {"anthropic", "openrouter"}:
   290|         raise ValueError(f"Unsupported provider: {provider}")
   291| 
   292|     if provider == "openrouter" and not os.getenv("OPENROUTER_API_KEY"):
   293|         raise RuntimeError("OPENROUTER_API_KEY is required when --provider openrouter is used")
   294| 
   295|     resolved_model = model or (
   296|         "anthropic/claude-haiku-4-5" if provider == "openrouter" else "claude-haiku-4-5"
   297|     )
   298| 
   299|     anthropic_client = None
   300|     if provider == "anthropic":
   301|         try:
   302|             import anthropic
   303|         except ImportError:
   304|             print("ERROR: anthropic package not installed. Run: pip install anthropic")
   305|             sys.exit(1)
   306|         anthropic_client = anthropic.Anthropic()
   307| 
   308|     for attempt in range(retry_count + 1):
   309|         try:
   310|             if provider == "anthropic":
   311|                 response = anthropic_client.messages.create(
   312|                     model=resolved_model,
   313|                     max_tokens=max_tokens,
   314|                     system=system_prompt,
   315|                     messages=[{"role": "user", "content": resolved_prompt}],
   316|                 )
   317|                 raw_text = response.content[0].text.strip()
   318|             else:
   319|                 import urllib.request
   320|                 import urllib.error
   321| 
   322|                 payload = json.dumps({
   323|                     "model": resolved_model,
   324|                     "max_tokens": max_tokens,
   325|                     "messages": [
   326|                         {"role": "system", "content": system_prompt},
   327|                         {"role": "user", "content": resolved_prompt},
   328|                     ],
   329|                 }).encode("utf-8")
   330| 
   331|                 req = urllib.request.Request(
   332|                     "https://openrouter.ai/api/v1/chat/completions",
   333|                     data=payload,
   334|                     headers={
   335|                         "Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}",
   336|                         "Content-Type": "application/json",
   337|                     },
   338|                     method="POST",
   339|                 )
   340|                 with urllib.request.urlopen(req) as resp:
   341|                     response_body = json.loads(resp.read().decode("utf-8"))
   342|                 raw_text = response_body["choices"][0]["message"]["content"].strip()
   343| 
   344|             result = json.loads(_strip_json_fences(raw_text))
   345|             if "recommended" not in result:
   346|                 raise ValueError("Missing 'recommended' key in response")
   347|             return result
   348|         except json.JSONDecodeError as e:
   349|             print(f"  WARN: JSON parse failed (attempt {attempt + 1}): {e}")
   350|         except Exception as e:
   351|             print(f"  WARN: API call failed (attempt {attempt + 1}): {e}")
   352| 
   353|         if attempt < retry_count:
   354|             time.sleep(retry_delay)
   355| 
   356|     return None
   357| 
   358| 
   359| def _groups_vocab_path(cache_path: str) -> str:
   360|     stem, _ = os.path.splitext(cache_path)
   361|     return f"{stem}_groups.json"
   362| 
   363| 
   364| def load_groups_vocab(cache_path: str) -> Dict[str, str]:
   365|     groups_path = _groups_vocab_path(cache_path)
   366|     if not os.path.exists(groups_path):
   367|         return {}
   368|     try:
   369|         with open(groups_path, "r", encoding="utf-8") as f:
   370|             data = json.load(f)
   371|     except Exception:
   372|         return {}
   373|     if not isinstance(data, dict):
   374|         return {}
   375|     return {str(k): str(v) for k, v in data.items()}
   376| 
   377| 
   378| def save_groups_vocab(cache_path: str, vocab: Dict[str, str]) -> None:
   379|     groups_path = _groups_vocab_path(cache_path)
   380|     os.makedirs(os.path.dirname(os.path.abspath(groups_path)), exist_ok=True)
   381|     with open(groups_path, "w", encoding="utf-8") as f:
   382|         json.dump(dict(sorted(vocab.items())), f, indent=2, ensure_ascii=False)
   383|         f.write("\n")
   384| 
   385| 
   386| # ---------------------------------------------------------------------------
   387| 
   388| # ---------------------------------------------------------------------------
   389| # CSV-based identity_items lookup (for flattened export format)
   390| # ---------------------------------------------------------------------------
   391| 
   392| def _load_identity_items_from_csv(
   393|     lookup_csv: str,
   394| ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
   395|     """
   396|     Load identity_items_by_joinhash.csv into a dict keyed by (domain, join_hash).
   397| 
   398|     CSV schema: domain, join_hash, k, v, q
   399|     Built by: tools/label_synthesis/build_identity_items_lookup.py
   400|     """
   401|     import csv as _csv
   402|     result: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
   403|     if not lookup_csv or not os.path.exists(lookup_csv):
   404|         return result
   405|     try:
   406|         with open(lookup_csv, "r", encoding="utf-8-sig", newline="") as f:
   407|             for row in _csv.DictReader(f):
   408|                 domain = (row.get("domain") or "").strip()
   409|                 join_hash = (row.get("join_hash") or "").strip()
   410|                 k = (row.get("k") or "").strip()
   411|                 v = row.get("v", "")
   412|                 q = (row.get("q") or "ok").strip()
   413|                 if not domain or not join_hash or not k:
   414|                     continue
   415|                 key = (domain, join_hash)
   416|                 result.setdefault(key, []).append({
   417|                     "k": k,
   418|                     "v": v if v else None,
   419|                     "q": q,
   420|                 })
   421|     except Exception as e:
   422|         print(f"  WARN: Failed to load identity items lookup: {e}")
   423|     print(
   424|         f"  [identity_items_lookup] Loaded {len(result):,} "
   425|         f"(domain, join_hash) entries from {lookup_csv}"
   426|     )
   427|     return result
   428| 
   429| # Representative identity_items lookup
   430| # ---------------------------------------------------------------------------
   431| 
   432| def _load_representative_identity_items(
   433|     exports_dir: str,
   434|     domain: str,
   435|     join_hash: str,
   436|     *,
   437|     items_lookup: Optional[Dict] = None,
   438| ) -> List[Dict[str, Any]]:
   439|     """
   440|     Return identity_items for a representative record with this join_hash.
   441| 
   442|     Priority:
   443|       1. CSV lookup (identity_items_by_joinhash.csv) — fast, no JSON scan
   444|       2. JSON scan of exports_dir — original behavior, works when export
   445|          JSONs contain inline identity_items
   446| 
   447|     Args:
   448|         exports_dir:   Directory with fingerprint export JSON files
   449|         domain:        Domain name (e.g. "fill_patterns_drafting")
   450|         join_hash:     Target join_hash to look up
   451|         items_lookup:  Pre-loaded dict from _load_identity_items_from_csv().
   452|                        Pass None to skip CSV lookup and go straight to JSON scan.
   453|     """
   454|     # Path 1: CSV lookup (preferred — fast, no JSON scan needed)
   455|     if items_lookup is not None:
   456|         items = items_lookup.get((domain, join_hash))
   457|         if items:
   458|             return items
   459|         # join_hash not in lookup — fall through to JSON scan
   460| 
   461|     # Path 2: Original JSON scan
   462|     if not exports_dir or not os.path.isdir(exports_dir):
   463|         return []
   464|     for fname in sorted(os.listdir(exports_dir)):
   465|         if not fname.endswith(".json"):
   466|             continue
   467|         fpath = os.path.join(exports_dir, fname)
   468|         try:
   469|             with open(fpath, "r", encoding="utf-8") as f:
   470|                 data = json.load(f)
   471|         except Exception:
   472|             continue
   473| 
   474|         records = _get_domain_records(data, domain)
   475|         for rec in records:
   476|             jk = rec.get("join_key") or {}
   477|             jh = jk.get("join_hash", "") if isinstance(jk, dict) else ""
   478|             if jh == join_hash:
   479|                 items = rec.get("identity_items") or rec.get("identity_basis", {}).get("items", [])
   480|                 if isinstance(items, list):
   481|                     return items
   482|     return []
   483| 
   484| def _get_domain_records(data: Any, domain: str) -> List[Dict[str, Any]]:
   485|     """Extract records for a domain from export JSON."""
   486|     if isinstance(data, dict):
   487|         # Try common shapes
   488|         for key in ("records", domain, f"{domain}_records"):
   489|             val = data.get(key)
   490|             if isinstance(val, list):
   491|                 return val
   492|         # Nested: data[domain][records]
   493|         dom_data = data.get(domain)
   494|         if isinstance(dom_data, dict):
   495|             recs = dom_data.get("records", [])
   496|             if isinstance(recs, list):
   497|                 return recs
   498|     return []
   499| 
   500| 
   501| # ---------------------------------------------------------------------------
   502| # Domain prompt loader
   503| # ---------------------------------------------------------------------------
   504| 
```
