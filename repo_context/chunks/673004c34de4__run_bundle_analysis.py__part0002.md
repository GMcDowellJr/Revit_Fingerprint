# Chunk of tools/bundle_analysis/run_bundle_analysis.py

- Source relative path: `tools/bundle_analysis/run_bundle_analysis.py`
- Chunk: 2 of 3
- Original line range: 354-897
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: run_bundle_analysis
- Source SHA-256: f78aca08e1415706021084b7fd5c84367e1d0625c9c323e2394dc61b67f02fa2
- Starts inside symbol: no
- Ends inside symbol: no

```
   354| def run_bundle_analysis(
   355|     analysis_dir: Path,
   356|     out_dir: Path,
   357|     domain: str = "",
   358|     min_support_count: int = 3,
   359|     min_support_pct: float = 0.0,
   360|     analysis_run_id: str = "",
   361|     discover_populations_flag: bool = True,
   362|     min_population_size: int = 0,
   363|     max_population_overlap: float = 0.20,
   364|     min_population_jaccard: float = 0.30,
   365|     discovery_support_pct: float = 0.10,
   366|     compare: bool = False,
   367|     compute_share_profile: bool = False,
   368|     roles: Optional[List[str]] = None,
   369|     metadata_file: Optional[Path] = None,
   370|     purge_view: str = "both",
   371|     latent_purgeable_file: Optional[Path] = None,
   372|     workers: int = 4,
   373| ) -> Dict[str, int]:
   374|     presence_rows = read_csv_rows(analysis_dir / "pattern_presence_file.csv")
   375|     run_id = resolve_analysis_run_id(presence_rows, analysis_run_id)
   376| 
   377|     domains = [domain] if domain else sorted({r.get("domain", "") for r in presence_rows if r.get("analysis_run_id", "") == run_id})
   378|     resolved_roles: Optional[List[str]] = None
   379|     allowed_export_run_ids: Optional[Set[str]] = None
   380|     if roles:
   381|         if metadata_file is None:
   382|             raise ValueError("--metadata-file is required when --roles is provided")
   383|         expanded_roles: List[str] = []
   384|         for role in roles:
   385|             if role in ROLE_GROUP_ALIASES:
   386|                 expanded_roles.extend(ROLE_GROUP_ALIASES[role])
   387|             else:
   388|                 expanded_roles.append(role)
   389|         invalid_roles = sorted({r for r in expanded_roles if r not in VALID_ROLES})
   390|         if invalid_roles:
   391|             raise ValueError(f"invalid --roles values: {', '.join(invalid_roles)}")
   392|         resolved_roles = sorted(set(expanded_roles))
   393|         role_set = set(resolved_roles)
   394|         allowed_export_run_ids = set()
   395|         with metadata_file.open("r", encoding="utf-8-sig", newline="") as f:
   396|             reader = csv.DictReader(f)
   397|             for row in reader:
   398|                 role = (row.get("governance_role", "") or "").strip()
   399|                 eid = (row.get("export_run_id", "") or "").strip()
   400|                 if role in role_set and eid:
   401|                     allowed_export_run_ids.add(eid)
   402|         print(f"[role_filter] roles={resolved_roles} allowed_files={len(allowed_export_run_ids)}")
   403| 
   404|     # Pre-step: ensure latent_purgeable.csv exists when needed
   405|     if purge_view in ("used", "both"):
   406|         records_candidates = [
   407|             analysis_dir / "records",
   408|             analysis_dir.parent / "records",
   409|         ]
   410|         records_dir_derived = next((p for p in records_candidates if p.is_dir()), analysis_dir.parent / "records")
   411|         if latent_purgeable_file is None:
   412|             latent_purgeable_file = records_dir_derived / "latent_purgeable.csv"
   413|         latent_purgeable_file = _ensure_latent_purgeable(latent_purgeable_file, records_dir_derived)
   414| 
   415|     views_to_run = ["all", "used"] if purge_view == "both" else [purge_view]
   416| 
   417|     total_bundles = 0
   418|     total_edges = 0
   419|     total_files_no_bundle = 0
   420|     processed = len([d for d in domains if d])
   421| 
   422|     reference: Optional[Dict[str, object]] = None
   423|     if compare:
   424|         reference = load_and_validate(analysis_dir, SCHEMA_VERSION)
   425| 
   426|     if not discover_populations_flag:
   427|         for view in views_to_run:
   428|             view_out = _view_out_dir(out_dir, view)
   429|             view_out.mkdir(parents=True, exist_ok=True)
   430|             lp_file = latent_purgeable_file if view == "used" else None
   431|             purgeable_only_set: Optional[Set[Tuple[str, str, str]]] = None
   432|             if view == "used" and lp_file is not None:
   433|                 t_lp = time.time()
   434|                 purgeable_only_set = _load_purgeable_only_set(lp_file)
   435|                 print(f"[run] purgeable_only_set load elapsed={time.time()-t_lp:.2f}s")
   436| 
   437|             role_dir_name = f"role_{'_'.join(resolved_roles)}" if resolved_roles else ""
   438|             role_stage_root = view_out / "_role_stage"
   439|             if resolved_roles and role_stage_root.exists():
   440|                 shutil.rmtree(role_stage_root)
   441| 
   442|             view_timing_rows: List[Dict[str, str]] = []
   443|             view_compare_summary_rows: List[Dict[str, str]] = []
   444|             compare_reset_domains: Set[str] = set()
   445| 
   446|             compare_out: Optional[Path] = None
   447|             if compare:
   448|                 compare_out = view_out.parent / f"compare_{view}"
   449|                 compare_out.mkdir(parents=True, exist_ok=True)
   450| 
   451|             if not compare:
   452|                 active_domains = [d for d in domains if d]
   453|                 if not active_domains:
   454|                     print(f"[run] view={view} no active domains — skipping")
   455|                     continue
   456| 
   457|                 pool_size = min(workers, len(active_domains))
   458| 
   459|                 print(f"[run] view={view} submitting {len(active_domains)} domains to {pool_size} workers")
   460| 
   461|                 with ProcessPoolExecutor(max_workers=pool_size) as executor:
   462|                     future_to_dom = {
   463|                         executor.submit(
   464|                             _run_pipeline_once,
   465|                             analysis_dir=analysis_dir,
   466|                             work_out_dir=role_stage_root if resolved_roles else view_out,
   467|                             domain=dom,
   468|                             run_id=run_id,
   469|                             min_support_count=min_support_count,
   470|                             min_support_pct=min_support_pct,
   471|                             compute_share_profile=compute_share_profile,
   472|                             analysis_run_id=run_id,
   473|                             allowed_export_run_ids=allowed_export_run_ids,
   474|                             purge_view=view,
   475|                             latent_purgeable_file=lp_file,
   476|                             purgeable_only_set=purgeable_only_set,
   477|                         ): dom
   478|                         for dom in active_domains
   479|                     }
   480|                     for future in as_completed(future_to_dom):
   481|                         dom = future_to_dom[future]
   482|                         try:
   483|                             stats = future.result()
   484|                         except Exception as exc:
   485|                             print(f"[run][error] domain={dom} view={view} failed: {exc}")
   486|                             continue
   487|                         if resolved_roles:
   488|                             produced = (role_stage_root if resolved_roles else view_out) / dom
   489|                             final_out = view_out / dom / role_dir_name
   490|                             if final_out.exists():
   491|                                 shutil.rmtree(final_out)
   492|                             final_out.parent.mkdir(parents=True, exist_ok=True)
   493|                             shutil.move(str(produced), str(final_out))
   494|                         total_bundles += stats["total_bundles_found"]
   495|                         total_edges += stats["total_dag_edges"]
   496|                         total_files_no_bundle += stats["files_with_no_bundle_match"]
   497|                         step_times = stats.get("step_times", {})
   498|                         for step_name in TIMING_STEPS:
   499|                             view_timing_rows.append(
   500|                                 {
   501|                                     "schema_version": SCHEMA_VERSION,
   502|                                     "analysis_run_id": run_id,
   503|                                     "domain": dom,
   504|                                     "population_id": "",
   505|                                     "step": step_name,
   506|                                     "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
   507|                                 }
   508|                             )
   509|                         print(f"[run] domain={dom} view={view} complete")
   510|             else:
   511|                 for dom in domains:
   512|                     if not dom:
   513|                         continue
   514|                     print(f"[run] domain={dom} start")
   515|                     try:
   516|                         work_out_base = role_stage_root if resolved_roles else view_out
   517|                         t0 = time.time()
   518|                         build_membership_matrix(
   519|                             analysis_dir,
   520|                             work_out_base,
   521|                             dom,
   522|                             run_id,
   523|                             None,
   524|                             None,
   525|                             None,
   526|                             allowed_export_run_ids,
   527|                             view,
   528|                             lp_file,
   529|                             purgeable_only_set=purgeable_only_set,
   530|                         )
   531|                         t1 = time.time() - t0
   532|                         print(f"[run] domain={dom} step1_seconds={t1:.3f}")
   533| 
   534|                         _thread_workers = max(2, min(4, (len(domains) or 1)))
   535|                         with ThreadPoolExecutor(max_workers=_thread_workers) as executor:
   536|                             discovery_future = executor.submit(
   537|                                 _run_step2_to_step7,
   538|                                 analysis_dir,
   539|                                 work_out_base,
   540|                                 dom,
   541|                                 min_support_count,
   542|                                 min_support_pct,
   543|                                 run_id,
   544|                                 compute_share_profile,
   545|                             )
   546|                             compare_started = time.time()
   547|                             compare_future = executor.submit(
   548|                                 run_compare_for_domain,
   549|                                 analysis_dir,
   550|                                 work_out_base,
   551|                                 reference or {},
   552|                                 dom,
   553|                                 compare_out_dir=compare_out,
   554|                                 eligible_export_run_ids=allowed_export_run_ids,
   555|                             )
   556|                             tail = discovery_future.result()
   557|                             compare_summary = compare_future.result()
   558|                         compare_seconds = time.time() - compare_started
   559|                         view_compare_summary_rows.append(compare_summary)
   560|                         step_times = {"step1": t1, **tail.get("step_times", {})}
   561|                         print(
   562|                             f"[timing] domain={dom} discovery_seconds={sum(float(step_times.get(k, 0.0)) for k in ('step1','step2','step2b','step3','step4','step5','step6','step7')):.3f} "
   563|                             f"compare_seconds={compare_seconds:.3f}"
   564|                         )
   565|                         stats = {
   566|                             "total_bundles_found": tail.get("total_bundles_found", 0),
   567|                             "total_dag_edges": tail.get("total_dag_edges", 0),
   568|                             "files_with_no_bundle_match": tail.get("files_with_no_bundle_match", 0),
   569|                             "step_times": step_times,
   570|                         }
   571| 
   572|                         if resolved_roles:
   573|                             produced = work_out_base / dom
   574|                             final_out = view_out / dom / role_dir_name
   575|                             if final_out.exists():
   576|                                 shutil.rmtree(final_out)
   577|                             final_out.parent.mkdir(parents=True, exist_ok=True)
   578|                             shutil.move(str(produced), str(final_out))
   579| 
   580|                         total_bundles += stats["total_bundles_found"]
   581|                         total_edges += stats["total_dag_edges"]
   582|                         total_files_no_bundle += stats["files_with_no_bundle_match"]
   583|                         step_times = stats.get("step_times", {})
   584|                         for step_name in TIMING_STEPS:
   585|                             view_timing_rows.append(
   586|                                 {
   587|                                     "schema_version": SCHEMA_VERSION,
   588|                                     "analysis_run_id": run_id,
   589|                                     "domain": dom,
   590|                                     "population_id": "",
   591|                                     "step": step_name,
   592|                                     "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
   593|                                 }
   594|                             )
   595|                     except Exception as exc:
   596|                         print(f"[run][error] domain={dom} failed: {exc}")
   597| 
   598|             existing_timing_rows = read_csv_rows(view_out / "bundle_analysis_timing.csv") if (view_out / "bundle_analysis_timing.csv").exists() else []
   599|             merged_timing_rows = [r for r in existing_timing_rows if r.get("analysis_run_id", "") != run_id] + view_timing_rows
   600|             merged_timing_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("step", "")))
   601|             atomic_write_csv(view_out / "bundle_analysis_timing.csv", TIMING_FIELDNAMES, merged_timing_rows)
   602| 
   603|             if compare and compare_out is not None:
   604|                 compare_rows = [r for r in view_compare_summary_rows if r.get("analysis_run_id", "") == run_id]
   605|                 compare_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", "")))
   606|                 atomic_write_csv(
   607|                     compare_out / "compare_run_summary.csv",
   608|                     [
   609|                         "reference_bundle_id",
   610|                         "effective_date",
   611|                         "analysis_run_id",
   612|                         "domain",
   613|                         "population_id",
   614|                         "files_scored",
   615|                         "full_count",
   616|                         "partial_count",
   617|                         "none_count",
   618|                         "no_reference_count",
   619|                     ],
   620|                     compare_rows,
   621|                 )
   622| 
   623|             _emit_meta_scatter_thresholds(view_out, run_id, domain)
   624| 
   625|         print(
   626|             f"[run] complete domains_processed={processed} total_bundles_found={total_bundles} "
   627|             f"total_dag_edges={total_edges} files_with_no_bundle_match={total_files_no_bundle}"
   628|         )
   629|         return {
   630|             "domains_processed": processed,
   631|             "total_bundles_found": total_bundles,
   632|             "total_dag_edges": total_edges,
   633|             "files_with_no_bundle_match": total_files_no_bundle,
   634|         }
   635| 
   636|     # ── Population-aware path ──────────────────────────────────────────────────
   637|     # TODO: pre-load purgeable_only_set for population-aware path
   638|     records_csv_candidates = [
   639|         analysis_dir / "records" / "records.csv",
   640|         analysis_dir.parent / "records" / "records.csv",
   641|         analysis_dir / "records.csv",
   642|         analysis_dir.parent / "records.csv",
   643|         analysis_dir / "records.csv",
   644|         analysis_dir.parent / "records.csv",
   645|     ]
   646|     records_csv_path = next((p for p in records_csv_candidates if p.exists()), None)
   647|     placeholder_exclusions_path: Optional[Path] = None
   648|     if records_csv_path is None:
   649|         searched = ", ".join(str(p) for p in records_csv_candidates)
   650|         print(f"[run_bundle_analysis] WARNING: records CSV not found for placeholder exclusion; searched: {searched}")
   651|     else:
   652|         with records_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
   653|             reader = csv.DictReader(f)
   654|             has_purgeable = "is_purgeable" in (reader.fieldnames or [])
   655|         if has_purgeable:
   656|             placeholder_exclusions_path = out_dir / "domain_placeholder_exclusions.csv"
   657|             compute_placeholder_exclusions(records_csv_path, placeholder_exclusions_path)
   658|             print(f"[run_bundle_analysis] placeholder exclusions computed: {placeholder_exclusions_path}")
   659|         else:
   660|             print("[run_bundle_analysis] WARNING: is_purgeable column not found in records CSV — placeholder exclusion skipped")
   661| 
   662|     step0_times: Dict[str, float] = {}
   663|     domain_primary_counts: Dict[str, int] = {}
   664|     outliers_by_domain: Dict[str, int] = {}
   665|     domain_elapsed_seconds: Dict[str, float] = {}
   666|     domain_population_counts: Dict[str, int] = {}
   667|     populations_analyzed = 0
   668| 
   669|     # Per-view accumulation structures (keyed by view name)
   670|     view_timing_rows: Dict[str, List[Dict[str, str]]] = {v: [] for v in views_to_run}
   671|     view_compare_summary_rows: Dict[str, List[Dict[str, str]]] = {v: [] for v in views_to_run}
   672|     compare_reset_domains_by_view: Dict[str, Set[str]] = {v: set() for v in views_to_run}
   673| 
   674|     for dom in domains:
   675|         if not dom:
   676|             continue
   677|         try:
   678|             t0 = time.time()
   679|             discover_populations(
   680|                 analysis_dir=analysis_dir,
   681|                 out_dir=out_dir,
   682|                 domain=dom,
   683|                 analysis_run_id=run_id,
   684|                 min_population_size=min_population_size,
   685|                 max_population_overlap=max_population_overlap,
   686|                 min_population_jaccard=min_population_jaccard,
   687|                 discovery_support_pct=discovery_support_pct,
   688|                 placeholder_exclusions_path=placeholder_exclusions_path,
   689|                 allowed_export_run_ids=allowed_export_run_ids,
   690|             )
   691|             step0_elapsed = time.time() - t0
   692|             step0_times[dom] = step0_elapsed
   693|             print(f"[timing] stage=step0 domain={dom} seconds={step0_elapsed:.2f}")
   694|         except Exception as exc:
   695|             print(f"[run][error] domain={dom} step0 failed: {exc}")
   696|             continue
   697| 
   698|         summary_rows = read_csv_rows(out_dir / "corpus_population_summary.csv") if (out_dir / "corpus_population_summary.csv").exists() else []
   699|         corpus_population_rows = read_csv_rows(out_dir / "corpus_populations.csv") if (out_dir / "corpus_populations.csv").exists() else []
   700|         pop_ids = sorted(
   701|             {
   702|                 (row.get("population_id", ""), row.get("scope_key", ""))
   703|                 for row in summary_rows
   704|                 if row.get("analysis_run_id", "") == run_id
   705|                 and row.get("domain", "") == dom
   706|                 and row.get("population_role", "") == "primary"
   707|                 and row.get("population_id", "")
   708|             }
   709|         )
   710|         domain_primary_counts[dom] = len(pop_ids)
   711|         outlier_count = sum(
   712|             int(row.get("file_count", "0") or "0")
   713|             for row in summary_rows
   714|             if row.get("analysis_run_id", "") == run_id
   715|             and row.get("domain", "") == dom
   716|             and row.get("population_role", "") == "outlier"
   717|         )
   718|         outliers_by_domain[dom] = outlier_count
   719|         if not pop_ids:
   720|             print(f"[run][warn] domain={dom} has no primary populations; skipping main pass")
   721|             continue
   722| 
   723|         for pid, _scope_key_from_summary in pop_ids:
   724|             scope_keys_for_population = sorted(
   725|                 {
   726|                     (row.get("scope_key", "") or "").strip()
   727|                     for row in corpus_population_rows
   728|                     if row.get("analysis_run_id", "") == run_id
   729|                     and row.get("domain", "") == dom
   730|                     and row.get("population_id", "") == pid
   731|                 }
   732|             )
   733|             if not scope_keys_for_population:
   734|                 print(f"[run][warn] domain={dom} population_id={pid} has no scope_key mapping; skipping")
   735|                 continue
   736|             if len(scope_keys_for_population) > 1:
   737|                 raise ValueError(
   738|                     f"Population invariant violation for analysis_run_id={run_id}, domain={dom!r}, "
   739|                     f"population_id={pid!r}: expected exactly one scope_key, found {scope_keys_for_population}"
   740|                 )
   741|             population_scope_key = scope_keys_for_population[0]
   742|             print(f"[run] domain={dom} population_id={pid} start")
   743|             populations_analyzed += 1
   744|             domain_population_counts[dom] = domain_population_counts.get(dom, 0) + 1
   745| 
   746|             for view in views_to_run:
   747|                 view_out = _view_out_dir(out_dir, view)
   748|                 view_out.mkdir(parents=True, exist_ok=True)
   749|                 lp_file = latent_purgeable_file if view == "used" else None
   750| 
   751|                 staging_root = view_out / "_population_runs"
   752|                 stage_out = staging_root / f"{dom}__{pid}"
   753|                 final_out_base = view_out / dom
   754|                 if resolved_roles:
   755|                     final_out_base = final_out_base / f"role_{'_'.join(resolved_roles)}"
   756|                 final_out = final_out_base / pid
   757| 
   758|                 if stage_out.exists():
   759|                     shutil.rmtree(stage_out)
   760|                 if final_out.exists():
   761|                     shutil.rmtree(final_out)
   762| 
   763|                 try:
   764|                     t0 = time.time()
   765|                     stats = _run_pipeline_once(
   766|                         analysis_dir=analysis_dir,
   767|                         work_out_dir=stage_out,
   768|                         domain=dom,
   769|                         run_id=run_id,
   770|                         min_support_count=min_support_count,
   771|                         min_support_pct=min_support_pct,
   772|                         compute_share_profile=compute_share_profile,
   773|                         population_id=pid,
   774|                         analysis_run_id=run_id,
   775|                         population_registry_dir=out_dir,
   776|                         scope_key_filter=population_scope_key,
   777|                         allowed_export_run_ids=allowed_export_run_ids,
   778|                         purge_view=view,
   779|                         latent_purgeable_file=lp_file,
   780|                     )
   781|                     domain_elapsed_seconds[dom] = domain_elapsed_seconds.get(dom, 0.0) + (time.time() - t0)
   782|                     total_bundles += stats["total_bundles_found"]
   783|                     total_edges += stats["total_dag_edges"]
   784|                     total_files_no_bundle += stats["files_with_no_bundle_match"]
   785|                     step_times = stats.get("step_times", {})
   786|                     for step_name in TIMING_STEPS:
   787|                         view_timing_rows[view].append(
   788|                             {
   789|                                 "schema_version": SCHEMA_VERSION,
   790|                                 "analysis_run_id": run_id,
   791|                                 "domain": dom,
   792|                                 "population_id": pid,
   793|                                 "step": step_name,
   794|                                 "seconds": f"{float(step_times.get(step_name, 0.0)):.3f}",
   795|                             }
   796|                         )
   797| 
   798|                     if compare and reference is not None:
   799|                         membership_csv = stage_out / dom / "membership_matrix.csv"
   800|                         eligible_export_run_ids = {
   801|                             str(row.get("export_run_id", "")).strip()
   802|                             for row in read_csv_rows(membership_csv)
   803|                             if row.get("analysis_run_id", "") == run_id and str(row.get("export_run_id", "")).strip()
   804|                         } if membership_csv.exists() else set()
   805|                         compare_out_dir = view_out.parent / f"compare_{view}"
   806|                         compare_summary = run_compare_for_domain(
   807|                             analysis_dir=analysis_dir,
   808|                             out_dir=stage_out,
   809|                             reference=reference,
   810|                             domain=dom,
   811|                             compare_out_dir=compare_out_dir,
   812|                             population_id=pid,
   813|                             eligible_export_run_ids=eligible_export_run_ids,
   814|                             reset_domain_rows=dom not in compare_reset_domains_by_view[view],
   815|                         )
   816|                         compare_reset_domains_by_view[view].add(dom)
   817|                         view_compare_summary_rows[view].append(compare_summary)
   818| 
   819|                     produced = stage_out / dom
   820|                     final_out.parent.mkdir(parents=True, exist_ok=True)
   821|                     shutil.move(str(produced), str(final_out))
   822|                 except Exception as exc:
   823|                     print(f"[run][error] domain={dom} population_id={pid} view={view} failed: {exc}")
   824| 
   825|     total_outliers = sum(outliers_by_domain.get(dom, 0) for dom in domains)
   826|     print("[run] complete (population-aware)")
   827|     print(f"  domains_processed={processed}")
   828|     print(f"  populations_analyzed={populations_analyzed}")
   829|     print(f"  total_outlier_files={total_outliers}")
   830|     print(f"  total_bundles_found={total_bundles}")
   831|     print(f"  total_dag_edges={total_edges}")
   832|     print("  populations_detail:")
   833|     for dom in domains:
   834|         if not dom:
   835|             continue
   836|         print(
   837|             f"    {dom}: {domain_primary_counts.get(dom, 0)} populations, "
   838|             f"{outliers_by_domain.get(dom, 0)} outliers"
   839|         )
   840|     for dom in domains:
   841|         if not dom:
   842|             continue
   843|         print(
   844|             f"[timing] domain_total domain={dom} populations={domain_population_counts.get(dom, 0)} "
   845|             f"total_seconds={domain_elapsed_seconds.get(dom, 0.0):.2f}"
   846|         )
   847|         if dom in step0_times:
   848|             for view in views_to_run:
   849|                 view_timing_rows[view].append(
   850|                     {
   851|                         "schema_version": SCHEMA_VERSION,
   852|                         "analysis_run_id": run_id,
   853|                         "domain": dom,
   854|                         "population_id": "",
   855|                         "step": "step0",
   856|                         "seconds": f"{step0_times.get(dom, 0.0):.3f}",
   857|                     }
   858|                 )
   859| 
   860|     for view in views_to_run:
   861|         view_out = _view_out_dir(out_dir, view)
   862|         existing_timing_rows = read_csv_rows(view_out / "bundle_analysis_timing.csv") if (view_out / "bundle_analysis_timing.csv").exists() else []
   863|         merged_timing_rows = [r for r in existing_timing_rows if r.get("analysis_run_id", "") != run_id] + view_timing_rows[view]
   864|         merged_timing_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", ""), r.get("step", "")))
   865|         atomic_write_csv(view_out / "bundle_analysis_timing.csv", TIMING_FIELDNAMES, merged_timing_rows)
   866| 
   867|         if compare:
   868|             compare_out_dir = view_out.parent / f"compare_{view}"
   869|             compare_rows = [r for r in view_compare_summary_rows[view] if r.get("analysis_run_id", "") == run_id]
   870|             compare_rows.sort(key=lambda r: (r.get("analysis_run_id", ""), r.get("domain", ""), r.get("population_id", "")))
   871|             atomic_write_csv(
   872|                 compare_out_dir / "compare_run_summary.csv",
   873|                 [
   874|                     "reference_bundle_id",
   875|                     "effective_date",
   876|                     "analysis_run_id",
   877|                     "domain",
   878|                     "population_id",
   879|                     "files_scored",
   880|                     "full_count",
   881|                     "partial_count",
   882|                     "none_count",
   883|                     "no_reference_count",
   884|                 ],
   885|                 compare_rows,
   886|             )
   887| 
   888|         _emit_meta_scatter_thresholds(view_out, run_id, domain)
   889| 
   890|     return {
   891|         "domains_processed": processed,
   892|         "populations_analyzed": populations_analyzed,
   893|         "total_outlier_files": total_outliers,
   894|         "total_bundles_found": total_bundles,
   895|         "total_dag_edges": total_edges,
   896|         "files_with_no_bundle_match": total_files_no_bundle,
   897|     }
```
