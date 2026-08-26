# Chunk of tools/label_synthesis/synthesize_fragmented_labels.py

- Source relative path: `tools/label_synthesis/synthesize_fragmented_labels.py`
- Chunk: 2 of 2
- Original line range: 505-1024
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _load_domain_prompt_module, _write_review_csv, synthesize, synthesize._process_join_hash, _generic_system_prompt, _generic_build_prompt, main
- Source SHA-256: 4ae8d38bd620968b5c02d23c147ff7d09f661014f6d5a52c2f0040495114a524
- Starts inside symbol: no
- Ends inside symbol: no

```
   505| def _load_domain_prompt_module(domain: str):
   506|     """Import domain prompt module, with progressive base-name fallback.
   507| 
   508|     Tries exact match first, then progressively strips trailing underscore
   509|     segments to find a base module. Examples:
   510|       dimension_types_linear  → dimension_types_linear, dimension_types
   511|       fill_patterns_drafting  → fill_patterns_drafting, fill_patterns
   512|       object_styles_model     → object_styles_model, object_styles
   513|     """
   514|     import importlib
   515| 
   516|     parts = domain.split("_")
   517|     # Try from full name down to 1-segment base.
   518|     # This preserves underscore fallback behavior while still supporting
   519|     # single-word domains like "arrowheads".
   520|     for n in range(len(parts), 0, -1):
   521|         candidate = "_".join(parts[:n])
   522|         try:
   523|             return importlib.import_module(
   524|                 f"tools.label_synthesis.domain_prompts.{candidate}"
   525|             )
   526|         except ImportError:
   527|             continue
   528|     return None
   529| 
   530| 
   531| # ---------------------------------------------------------------------------
   532| # Review CSV emitter
   533| # ---------------------------------------------------------------------------
   534| 
   535| def _write_review_csv(review_path: str, cache: Dict[str, Any], domain: str) -> None:
   536|     """Write pending-review CSV for curator workflow."""
   537|     rows = []
   538|     for join_hash, entry in sorted(cache.items()):
   539|         if entry.get("domain", domain) != domain:
   540|             continue
   541|         rows.append({
   542|             "domain": domain,
   543|             "join_hash": join_hash,
   544|             "recommended_name": entry.get("recommended", ""),
   545|             "candidates": " | ".join(entry.get("candidates", [])),
   546|             "rationale": entry.get("rationale", ""),
   547|             "reviewed": entry.get("reviewed", False),
   548|             "generated_at": entry.get("generated_at", ""),
   549|         })
   550| 
   551|     if not rows:
   552|         print(f"  No entries for domain '{domain}' in cache.")
   553|         return
   554| 
   555|     os.makedirs(os.path.dirname(os.path.abspath(review_path)), exist_ok=True)
   556|     with open(review_path, "w", newline="", encoding="utf-8") as f:
   557|         w = csv.DictWriter(f, fieldnames=[
   558|             "domain", "join_hash", "recommended_name", "candidates",
   559|             "rationale", "reviewed", "generated_at",
   560|         ])
   561|         w.writeheader()
   562|         w.writerows(rows)
   563|     print(f"  Review CSV written: {review_path}  ({len(rows)} entries)")
   564| 
   565| 
   566| # ---------------------------------------------------------------------------
   567| # Main synthesis loop
   568| # ---------------------------------------------------------------------------
   569| 
   570| def synthesize(
   571|     *,
   572|     exports_dir: str,
   573|     analysis_dir: str,
   574|     domain: str,
   575|     cache_path: str,
   576|     dry_run: bool = False,
   577|     force_refresh: bool = False,
   578|     only_unreviewed: bool = False,
   579|     review_csv: Optional[str] = None,
   580|     export_prompts: Optional[str] = None,
   581|     import_results: Optional[str] = None,
   582|     provider: str = "anthropic",
   583|     model: Optional[str] = None,
   584|     workers: int = 3,
   585|     filter_mode: str = "all",
   586|     domain_patterns_csv: Optional[str] = None,
   587|     bundle_dir: Optional[str] = None,
   588|     segments_root: Optional[str] = None,
   589|     registry_file: Optional[str] = None,
   590|     items_lookup_csv: Optional[str] = None,
   591| ) -> None:
   592|     print(f"\n=== Label Synthesis: {domain} ===")
   593|     print(f"  Exports dir:   {exports_dir}")
   594|     print(f"  Analysis dir:  {analysis_dir}")
   595|     print(f"  Cache path:    {cache_path}")
   596|     print(f"  Dry run:       {dry_run}")
   597|     print(f"  Export prompts: {export_prompts or '(disabled)'}")
   598|     print(f"  Import results: {import_results or '(disabled)'}")
   599|     print(f"  Provider:      {provider}")
   600|     print(f"  Model:         {model or '(provider default)'}")
   601|     print(f"  Workers:       {workers}")
   602| 
   603|     # Load existing cache
   604|     cache = load_llm_cache(cache_path)
   605|     print(f"  Existing cache entries: {len(cache)}")
   606| 
   607|     # Load CSV-based identity items lookup if provided
   608|     items_lookup: Optional[Dict] = None
   609|     if items_lookup_csv:
   610|         items_lookup = _load_identity_items_from_csv(items_lookup_csv)
   611|     elif not os.path.isdir(exports_dir):
   612|         print(f"  WARN: --exports-dir does not exist and no --identity-items-lookup provided.")
   613|         print(f"        LLM prompts will use observed names only (no behavioral parameters).")
   614| 
   615|     if import_results:
   616|         with open(import_results, "r", encoding="utf-8") as f:
   617|             imported_entries = json.load(f)
   618|         for entry in imported_entries:
   619|             join_hash = entry["join_hash"]
   620|             cache[join_hash] = {
   621|                 "domain": domain,
   622|                 "recommended": entry["recommended"],
   623|                 "candidates": entry.get("candidates", [entry["recommended"]]),
   624|                 "rationale": entry.get("rationale", ""),
   625|                 "reviewed": False,
   626|                 "generated_at": date.today().isoformat(),
   627|                 "source": "import",
   628|             }
   629|         save_llm_cache(cache_path, cache)
   630|         print(f"  Imported {len(imported_entries)} results → cache written to {cache_path}")
   631|         return
   632| 
   633|     # Load domain prompt module
   634|     prompt_mod = _load_domain_prompt_module(domain)
   635|     if prompt_mod is None:
   636|         print(f"  WARN: No prompt module for domain '{domain}'. "
   637|               f"Create tools/label_synthesis/domain_prompts/{domain}.py")
   638|         print("  Falling back to generic prompt.")
   639|         system_prompt = _generic_system_prompt(domain)
   640|         build_prompt_fn = _generic_build_prompt
   641|     else:
   642|         system_prompt = prompt_mod.SYSTEM_PROMPT
   643|         build_prompt_fn = prompt_mod.build_prompt
   644| 
   645|     # Load label population
   646|     pop_csv = os.path.join(analysis_dir, f"{domain}.joinhash_label_population.csv")
   647|     if not os.path.exists(pop_csv):
   648|         # Try alternate location
   649|         pop_csv = os.path.join(analysis_dir, "label_population", f"{domain}.joinhash_label_population.csv")
   650|     if not os.path.exists(pop_csv):
   651|         print(f"  ERROR: Label population CSV not found. Run run_joinhash_label_population first.")
   652|         print(f"  Looked at: {pop_csv}")
   653|         return
   654| 
   655|     label_pop_by_hash = load_label_population(pop_csv, domain)
   656|     print(f"  Loaded {len(label_pop_by_hash)} join_hash entries from population CSV")
   657| 
   658|     # Identify fragmented hashes
   659|     fragmented_hashes = [
   660|         jh for jh, rows in label_pop_by_hash.items()
   661|         if is_fragmented(rows)
   662|     ]
   663|     print(f"  Fragmented patterns: {len(fragmented_hashes)} / {len(label_pop_by_hash)}")
   664| 
   665|     if not fragmented_hashes:
   666|         print("  Nothing to synthesize.")
   667|         return
   668| 
   669|     # Determine which hashes need synthesis
   670|     to_process = []
   671|     for jh in fragmented_hashes:
   672|         if not force_refresh and jh in cache:
   673|             if only_unreviewed and not cache[jh].get("reviewed", False):
   674|                 to_process.append(jh)
   675|             elif not only_unreviewed:
   676|                 pass  # already cached, skip
   677|             continue
   678|         to_process.append(jh)
   679| 
   680|     # governance filter
   681|     if filter_mode != "all":
   682|         eligible_jhs = _load_governance_join_hashes(
   683|             domain=domain,
   684|             filter_mode=filter_mode,
   685|             analysis_dir=analysis_dir,
   686|             domain_patterns_csv=domain_patterns_csv,
   687|             bundle_dir=bundle_dir,
   688|             segments_root=segments_root,
   689|             registry_file=registry_file,
   690|         )
   691|         if eligible_jhs is not None:
   692|             before = len(to_process)
   693|             to_process = [jh for jh in to_process if jh in eligible_jhs]
   694|             print(f"  Filter applied: {before} → {len(to_process)} patterns")
   695| 
   696|     print(f"  To process: {len(to_process)}")
   697|     if not to_process:
   698|         print("  Cache is current. Use --force-refresh to re-synthesize.")
   699|         if review_csv:
   700|             _write_review_csv(review_csv, cache, domain)
   701|         return
   702| 
   703|     if export_prompts:
   704|         prompt_exports = []
   705|         for jh in to_process:
   706|             rows = label_pop_by_hash.get(jh, [])
   707|             rows_sorted = sorted(rows, key=lambda r: -int(r.get("files_count", 0)))
   708|             identity_items = _load_representative_identity_items(
   709|                 exports_dir, domain, jh, items_lookup=items_lookup
   710|             )
   711|             user_prompt = build_prompt_fn(
   712|                 join_hash=jh,
   713|                 observed_labels=rows_sorted,
   714|                 identity_items=identity_items,
   715|             )
   716|             prompt_exports.append({
   717|                 "join_hash": jh,
   718|                 "domain": domain,
   719|                 "system_prompt": system_prompt,
   720|                 "user_prompt": user_prompt,
   721|             })
   722|         os.makedirs(os.path.dirname(os.path.abspath(export_prompts)), exist_ok=True)
   723|         with open(export_prompts, "w", encoding="utf-8") as f:
   724|             json.dump(prompt_exports, f, indent=2, ensure_ascii=False)
   725|             f.write("\n")
   726|         print(f"  Exported {len(prompt_exports)} prompts → {export_prompts}")
   727|         return
   728| 
   729|     if not dry_run and provider == "openrouter" and not os.getenv("OPENROUTER_API_KEY"):
   730|         raise RuntimeError("OPENROUTER_API_KEY is required when --provider openrouter is used")
   731| 
   732|     # Process each fragmented hash
   733|     success = 0
   734|     failed = 0
   735|     today = date.today().isoformat()
   736|     groups_vocab = load_groups_vocab(cache_path)
   737|     discovered_groups: Dict[str, str] = {}
   738|     cache_lock = threading.Lock()
   739| 
   740|     def _process_join_hash(jh: str):
   741|         rows = label_pop_by_hash.get(jh, [])
   742|         rows_sorted = sorted(rows, key=lambda r: -int(r.get("files_count", 0)))
   743|         identity_items = _load_representative_identity_items(
   744|             exports_dir, domain, jh, items_lookup=items_lookup
   745|         )
   746|         user_prompt = build_prompt_fn(
   747|             join_hash=jh,
   748|             observed_labels=rows_sorted,
   749|             identity_items=identity_items,
   750|         )
   751|         if dry_run:
   752|             return (
   753|                 jh,
   754|                 {
   755|                     "dry_run": True,
   756|                     "user_prompt": user_prompt,
   757|                     "labels": [r.get("label_v", "") for r in rows_sorted[:5]],
   758|                 },
   759|                 rows_sorted,
   760|             )
   761| 
   762|         result = _call_llm(
   763|             system_prompt=system_prompt,
   764|             user_prompt=user_prompt,
   765|             provider=provider,
   766|             model=model,
   767|             groups_vocab=groups_vocab,
   768|         )
   769|         return (jh, result, rows_sorted)
   770| 
   771|     with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
   772|         futures = {executor.submit(_process_join_hash, jh): jh for jh in to_process}
   773| 
   774|         for i, fut in enumerate(as_completed(futures), 1):
   775|             jh = futures[fut]
   776|             try:
   777|                 jh, result, rows_sorted = fut.result()
   778|             except Exception as e:
   779|                 print(f"\n  [{i}/{len(to_process)}] join_hash={jh[:16]}...")
   780|                 print(f"    FAILED — worker exception: {e}")
   781|                 failed += 1
   782|                 continue
   783| 
   784|             print(f"\n  [{i}/{len(to_process)}] join_hash={jh[:16]}...")
   785|             print(f"    Labels: {[r.get('label_v', '') for r in rows_sorted[:5]]}")
   786| 
   787|             if dry_run:
   788|                 print("    --- SYSTEM PROMPT (first 300 chars) ---")
   789|                 print(f"    {system_prompt[:300]}...")
   790|                 injected_prompt = result["user_prompt"]
   791|                 if groups_vocab:
   792|                     vocab_lines = ["EXISTING GROUPS IN THIS DOMAIN:"]
   793|                     for label, definition in sorted(groups_vocab.items()):
   794|                         vocab_lines.append(f"  {label}: {definition}")
   795|                     injected_prompt = injected_prompt.replace(
   796|                         "EXISTING GROUPS IN THIS DOMAIN: (none yet — you are establishing the vocabulary)",
   797|                         "\n".join(vocab_lines),
   798|                     )
   799|                 print("    --- USER PROMPT ---")
   800|                 print(f"    {injected_prompt[:500]}...")
   801|                 print("    [DRY RUN — skipping API call]")
   802|                 continue
   803| 
   804|             if result is None:
   805|                 print("    FAILED — skipping")
   806|                 failed += 1
   807|                 continue
   808| 
   809|             recommended = result.get("recommended", "")
   810|             candidates = result.get("candidates", [])
   811|             rationale = result.get("rationale", "")
   812|             print(f"    Recommended: {recommended!r}")
   813|             print(f"    Candidates:  {candidates}")
   814| 
   815|             semantic_group = (result.get("semantic_group") or "").strip()
   816|             if semantic_group and semantic_group not in groups_vocab:
   817|                 discovered_groups[semantic_group] = rationale
   818| 
   819|             with cache_lock:
   820|                 cache[jh] = {
   821|                     "domain": domain,
   822|                     "recommended": recommended,
   823|                     "candidates": candidates,
   824|                     "rationale": rationale,
   825|                     "reviewed": False,
   826|                     "generated_at": today,
   827|                     "observed_labels": [r.get("label_v", "") for r in rows_sorted[:10]],
   828|                 }
   829|                 save_llm_cache(cache_path, cache)
   830|             success += 1
   831| 
   832|     if not dry_run:
   833|         if discovered_groups:
   834|             groups_vocab.update(discovered_groups)
   835|         save_groups_vocab(cache_path, groups_vocab)
   836| 
   837|     print(f"\n  Done. Success: {success}  Failed: {failed}  Skipped: {len(to_process) - success - failed}")
   838| 
   839| 
   840|     if review_csv and not dry_run:
   841|         _write_review_csv(review_csv, cache, domain)
   842| 
   843| 
   844| # ---------------------------------------------------------------------------
   845| # Generic fallbacks for domains without a prompt module
   846| # ---------------------------------------------------------------------------
   847| 
   848| def _generic_system_prompt(domain: str) -> str:
   849|     return (
   850|         f"You are a Revit standards specialist naming {domain.replace('_', ' ')} "
   851|         f"configuration patterns for a standards analytics dashboard at an engineering firm. "
   852|         f"Produce concise canonical names under 40 characters that standards managers will recognize."
   853|     )
   854| 
   855| 
   856| def _generic_build_prompt(
   857|     join_hash: str,
   858|     observed_labels: List[Dict[str, Any]],
   859|     identity_items: List[Dict[str, Any]],
   860|     corpus_context: Optional[Dict[str, Any]] = None,
   861| ) -> str:
   862|     lines = ["OBSERVED NAMES:"]
   863|     for r in observed_labels[:8]:
   864|         lines.append(f'  "{r.get("label_v", "")}" ({r.get("files_count", 0)} files)')
   865|     lines.append("\nPARAMETERS:")
   866|     for item in identity_items:
   867|         if item.get("q") == "ok":
   868|             lines.append(f"  {item.get('k')}: {item.get('v')}")
   869|     lines.append(
   870|         '\nRespond with ONLY JSON: {"candidates": [...], "recommended": "...", "rationale": "..."}'
   871|     )
   872|     return "\n".join(lines)
   873| 
   874| 
   875| # ---------------------------------------------------------------------------
   876| # CLI entry point
   877| # ---------------------------------------------------------------------------
   878| 
   879| def main():
   880|     ap = argparse.ArgumentParser(
   881|         description="Pre-populate LLM name cache for fragmented dimension type patterns."
   882|     )
   883|     ap.add_argument("--exports-dir", required=True,
   884|                     help="Directory containing fingerprint export JSON files")
   885|     ap.add_argument("--analysis-dir", required=True,
   886|                     help="Directory containing joinhash_label_population.csv")
   887|     ap.add_argument("--domain", required=True,
   888|                     help="Domain to synthesize labels for (e.g. dimension_types)")
   889|     ap.add_argument("--cache", required=True, dest="cache_path",
   890|                     help="Path to llm_name_cache.json (created/updated by this script)")
   891|     ap.add_argument("--dry-run", action="store_true",
   892|                     help="Print prompts without calling API or writing cache")
   893|     ap.add_argument("--force-refresh", action="store_true",
   894|                     help="Re-synthesize even if join_hash already in cache")
   895|     ap.add_argument("--only-unreviewed", action="store_true",
   896|                     help="Only process cache entries where reviewed=false")
   897|     ap.add_argument("--review-csv", default=None,
   898|                     help="Path to write pending-review CSV for curator workflow")
   899|     ap.add_argument(
   900|         "--export-prompts", default=None, metavar="PATH",
   901|         help="Write assembled prompts to this JSON file instead of calling the API. "
   902|              "No API calls are made and the cache is not written.",
   903|     )
   904|     ap.add_argument(
   905|         "--import-results", default=None, metavar="PATH",
   906|         help="Import LLM results from this JSON file and merge into cache. "
   907|              "No API calls are made.",
   908|     )
   909|     ap.add_argument("--provider", choices=["anthropic", "openrouter"], default="openrouter",
   910|                     help="LLM provider backend")
   911|     ap.add_argument("--model", default=None,
   912|                     help="Optional model override for selected provider")
   913|     ap.add_argument("--workers", type=int, default=3,
   914|                     help="Concurrent worker count for API calls")
   915|     ap.add_argument(
   916|         "--filter-mode",
   917|         choices=["all", "candidates", "bundles", "governance"],
   918|         default="all",
   919|         help=(
   920|             "Which patterns to synthesize. "
   921|             "'all' = every fragmented pattern (default). "
   922|             "'candidates' = is_candidate_standard=true only. "
   923|             "'bundles' = patterns in at least one bundle. "
   924|             "'governance' = union of candidates and bundle members."
   925|         ),
   926|     )
   927|     ap.add_argument(
   928|         "--bundle-dir",
   929|         default=None,
   930|         metavar="PATH",
   931|         help=(
   932|             "Directory containing bundle_membership.csv "
   933|             "(required when --filter-mode is 'bundles' or 'governance' unless "
   934|             "union mode is enabled with --segments-root and --registry-file)."
   935|         ),
   936|     )
   937|     ap.add_argument(
   938|         "--segments-root",
   939|         default=None,
   940|         metavar="PATH",
   941|         help=(
   942|             "Root directory containing one subfolder per segment "
   943|             "(e.g. C:\\Fingerprint_Out\\segments). When provided together with "
   944|             "--registry-file and --filter-mode bundles/governance, activates "
   945|             "union bundle discovery across all active segments and both purge "
   946|             "views (all + used). Replaces --bundle-dir for multi-segment corpora."
   947|         ),
   948|     )
   949|     ap.add_argument(
   950|         "--registry-file",
   951|         default=None,
   952|         metavar="PATH",
   953|         help=(
   954|             "Path to run_registry.csv. Required when --segments-root is provided. "
   955|             "Used to enumerate active segments (run_type=bundle|reference, "
   956|             "status!=skip|registration) for union bundle discovery."
   957|         ),
   958|     )
   959|     ap.add_argument(
   960|         "--identity-items-lookup",
   961|         default=None,
   962|         metavar="PATH",
   963|         help=(
   964|             "Path to identity_items_by_joinhash.csv built by "
   965|             "build_identity_items_lookup.py. Use when export JSONs do not "
   966|             "contain inline identity_items (flattened export format). "
   967|             "When provided, JSON scan is skipped for matched patterns."
   968|         ),
   969|     )
   970|     ap.add_argument(
   971|         "--domain-patterns-csv",
   972|         default=None,
   973|         metavar="PATH",
   974|         help=(
   975|             "Optional explicit path to domain_patterns.csv used by non-'all' filter modes. "
   976|             "Defaults to <analysis-dir>/domain_patterns.csv, then "
   977|             "<analysis-dir>/../analysis_v21/domain_patterns.csv."
   978|         ),
   979|     )
   980|     args = ap.parse_args()
   981| 
   982|     if args.export_prompts and args.import_results:
   983|         ap.error("--export-prompts and --import-results are mutually exclusive.")
   984|     if args.dry_run and (args.export_prompts or args.import_results):
   985|         ap.error("--dry-run cannot be combined with --export-prompts or --import-results.")
   986|     if args.filter_mode in ("bundles", "governance"):
   987|         has_single = bool(args.bundle_dir)
   988|         has_union = bool(args.segments_root) and bool(args.registry_file)
   989|         if not has_single and not has_union:
   990|             ap.error(
   991|                 "--filter-mode bundles/governance requires either --bundle-dir "
   992|                 "(single directory) or both --segments-root and --registry-file "
   993|                 "(union mode across all segments)."
   994|             )
   995|         if args.segments_root and not args.registry_file:
   996|             ap.error("--registry-file is required when --segments-root is provided.")
   997|         if args.registry_file and not args.segments_root:
   998|             ap.error("--segments-root is required when --registry-file is provided.")
   999| 
  1000|     synthesize(
  1001|         exports_dir=args.exports_dir,
  1002|         analysis_dir=args.analysis_dir,
  1003|         domain=args.domain,
  1004|         cache_path=args.cache_path,
  1005|         dry_run=args.dry_run,
  1006|         force_refresh=args.force_refresh,
  1007|         only_unreviewed=args.only_unreviewed,
  1008|         review_csv=args.review_csv,
  1009|         export_prompts=args.export_prompts,
  1010|         import_results=args.import_results,
  1011|         provider=args.provider,
  1012|         model=args.model,
  1013|         workers=args.workers,
  1014|         filter_mode=args.filter_mode,
  1015|         domain_patterns_csv=args.domain_patterns_csv,
  1016|         bundle_dir=args.bundle_dir,
  1017|         segments_root=args.segments_root,
  1018|         registry_file=args.registry_file,
  1019|         items_lookup_csv=args.identity_items_lookup,
  1020|     )
  1021| 
  1022| 
  1023| if __name__ == "__main__":
  1024|     main()
```
