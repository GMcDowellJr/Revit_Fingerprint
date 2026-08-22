# Chunk of tools/run_segment_orchestrator.py

- Source relative path: `tools/run_segment_orchestrator.py`
- Chunk: 2 of 4
- Original line range: 411-907
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _preshard_corpus_records, _write_segment_records, _filter_name_key_csv_to_segment, _filter_name_key_csv_to_segment._in_segment, _build_patterns_missing_notes, _active_domains_from_presence_csv, _active_domains_from_name_patterns, _segment_has_name_leg_output, merge_bi_outputs, build_run_plan, build_run_plan.sort_key
- Source SHA-256: c1d79ae240bf0af45e5deb47ebd929be191e1d6bb8a42be87fe41cbe5dfc7646
- Starts inside symbol: no
- Ends inside symbol: no

```
   411| def _preshard_corpus_records(
   412|     records_dir: Path,
   413|     segment_plans: Dict[str, Dict],
   414|     force: bool,
   415| ) -> None:
   416|     """
   417|     Stream each corpus source file once and fan out rows to per-segment
   418|     destination files keyed by export_run_id.  Segments whose destination
   419|     files already exist and are non-empty are skipped when force=False.
   420|     """
   421|     # csv.field_size_limit() converts to a C long; on Windows CPython the C long
   422|     # is 32-bit so sys.maxsize overflows.  Cap at 2^31-1 which fits everywhere.
   423|     try:
   424|         csv.field_size_limit(2 ** 31 - 1)
   425|     except OverflowError:
   426|         csv.field_size_limit(2 ** 30)
   427| 
   428|     t0 = time.monotonic()
   429| 
   430|     # Segments actually being (re)processed this pass. Skip only completed
   431|     # segments; pending/failed segments always get fresh inputs so retries
   432|     # without --force don't run against stale data. Computed once and reused
   433|     # below for marker-stamping too -- a segment excluded here must NOT have
   434|     # its .preshard_complete / identity_items_by_domain/.complete markers
   435|     # touched, since those markers are what _write_segment_records() trusts to
   436|     # skip re-copying. Stamping them for every planned segment regardless of
   437|     # whether records.csv/shards were actually written let a segment whose
   438|     # registry status was stale-"complete" end up "marked done" with an empty
   439|     # records dir (imperial_container_2014 step=bundle incident).
   440|     segments_to_write = {
   441|         sid: plan for sid, plan in segment_plans.items()
   442|         if force or plan.get("status") != "complete"
   443|     }
   444| 
   445|     # ── records.csv and file_metadata.csv ─────────────────────────────────────
   446|     for fname in ("records.csv", "file_metadata.csv"):
   447|         src = records_dir / fname
   448|         if not src.is_file():
   449|             continue
   450|         if not segments_to_write:
   451|             print(f"[preshard] {fname} → 0 segments written, {len(segment_plans)} skipped")
   452|             continue
   453| 
   454|         # Read header once; eid_col is stable across all batches.
   455|         with src.open("r", encoding="utf-8-sig", newline="") as _hf:
   456|             header: Optional[List[str]] = next(csv.reader(_hf), None)
   457|         if not header:
   458|             continue
   459|         eid_col = header.index("export_run_id") if "export_run_id" in header else None
   460|         if eid_col is None:
   461|             continue
   462| 
   463|         # Fan out in batches so at most _PRESHARD_BATCH destination handles are
   464|         # open simultaneously.  Each batch re-streams the source file once.
   465|         seg_items = list(segments_to_write.items())
   466|         for batch_start in range(0, len(seg_items), _PRESHARD_BATCH):
   467|             batch = dict(seg_items[batch_start : batch_start + _PRESHARD_BATCH])
   468| 
   469|             # Build one-to-many lookup scoped to this batch.
   470|             id_to_plans: Dict[str, List] = {}
   471|             for plan in batch.values():
   472|                 for eid in plan["allowed_ids"]:
   473|                     id_to_plans.setdefault(eid, []).append(plan)
   474| 
   475|             writers: Dict[str, Any] = {}
   476|             handles: Dict[str, Any] = {}
   477|             try:
   478|                 for sid, plan in batch.items():
   479|                     dst = plan["segment_records_dir"] / fname
   480|                     dst.parent.mkdir(parents=True, exist_ok=True)
   481|                     fh = dst.open("w", newline="", encoding="utf-8")
   482|                     handles[sid] = fh
   483|                     w = csv.writer(fh)
   484|                     w.writerow(header)
   485|                     writers[sid] = w
   486| 
   487|                 with src.open("r", encoding="utf-8-sig", newline="") as src_f:
   488|                     reader = csv.reader(src_f)
   489|                     next(reader, None)  # skip header row
   490|                     for row in reader:
   491|                         if len(row) <= eid_col:
   492|                             continue
   493|                         eid = row[eid_col].strip()
   494|                         plans = id_to_plans.get(eid)
   495|                         if not plans:
   496|                             continue
   497|                         for plan in plans:
   498|                             writers[plan["sid"]].writerow(row)
   499|             finally:
   500|                 for fh in handles.values():
   501|                     fh.close()
   502| 
   503|         print(f"[preshard] {fname} → {len(segments_to_write)} segments written, "
   504|               f"{len(segment_plans)-len(segments_to_write)} skipped (already exist)")
   505| 
   506|     # ── identity_items_by_domain/ shards ──────────────────────────────────────
   507|     corpus_shard_dir = records_dir / "identity_items_by_domain"
   508|     if corpus_shard_dir.is_dir():
   509|         shard_files = sorted(f for f in corpus_shard_dir.iterdir() if f.is_file() and f.suffix == ".csv")
   510|         # Cap concurrency so total open handles ≤ _PRESHARD_BATCH:
   511|         # each worker opens one handle per segment, so workers = BATCH // segments.
   512|         max_seg = max(1, len(segment_plans))
   513|         shard_pool_size = max(1, min(8, _PRESHARD_BATCH // max_seg)) if shard_files else 1
   514| 
   515|         total_written = 0
   516|         with ThreadPoolExecutor(max_workers=shard_pool_size) as executor:
   517|             futures = {
   518|                 executor.submit(_preshard_one_shard, shard_file, segment_plans, force): shard_file
   519|                 for shard_file in shard_files
   520|             }
   521|             for future in as_completed(futures):
   522|                 name, written, skipped = future.result()
   523|                 total_written += written
   524| 
   525|         # Write .complete markers only for segments actually (re)processed this
   526|         # pass -- a segment excluded from segments_to_write (registry status
   527|         # already "complete") keeps whatever marker it already has rather than
   528|         # having a fresh "ok" stamped over a shard dir this pass never wrote to.
   529|         for plan_entry in segments_to_write.values():
   530|             seg_shard_dir = plan_entry["segment_records_dir"] / "identity_items_by_domain"
   531|             if seg_shard_dir.is_dir():
   532|                 (seg_shard_dir / ".complete").write_text("ok", encoding="utf-8")
   533| 
   534|         print(
   535|             f"[preshard] identity_items shards → {len(shard_files)} shards processed, "
   536|             f"{total_written} segment×shard files written",
   537|             flush=True,
   538|         )
   539| 
   540|     # Write per-segment completion markers only for segments actually
   541|     # (re)processed this pass (see segments_to_write comment above). Done
   542|     # after all source files and shards so a partial run (exception before
   543|     # this point) leaves no markers, meaning the next run re-processes those
   544|     # segments from scratch.
   545|     for plan_entry in segments_to_write.values():
   546|         plan_entry["segment_records_dir"].mkdir(parents=True, exist_ok=True)
   547|         (plan_entry["segment_records_dir"] / ".preshard_complete").write_text("ok", encoding="utf-8")
   548| 
   549|     elapsed = int(time.monotonic() - t0)
   550|     print(f"[preshard] complete elapsed={elapsed}s", flush=True)
   551| 
   552| 
   553| def _write_segment_records(
   554|     records_dir: Path,
   555|     segment_records_dir: Path,
   556|     allowed_ids: set,
   557| ) -> None:
   558|     """
   559|     Copy records.csv and file_metadata.csv from corpus records_dir into the
   560|     segment records dir, filtered to the segment's export_run_ids.
   561| 
   562|     Also copies filtered identity_items shards from
   563|     records_dir/identity_items_by_domain/ into
   564|     segment_records_dir/identity_items_by_domain/ so that emit_analysis can
   565|     load identity_items for synopsis label resolution.
   566| 
   567|     Missing source files are skipped silently — patterns stage will simply see
   568|     an empty (or absent) input and the guard will surface the failure cleanly.
   569|     """
   570|     preshard_marker = segment_records_dir / ".preshard_complete"
   571|     # Defense in depth: trust the marker only if records.csv is actually present
   572|     # alongside it. _preshard_corpus_records() now only stamps this marker for
   573|     # segments it actually wrote to, but a marker/reality mismatch from any other
   574|     # cause (manual cleanup, interrupted write, older data) must not cause this
   575|     # step to silently skip regenerating a segment's records -- that's exactly
   576|     # what let imperial_container_2014 reach run_bundle_analysis.py with no
   577|     # records.csv on disk despite both completion markers reading "ok".
   578|     preshard_marker_valid = preshard_marker.is_file() and (segment_records_dir / "records.csv").is_file()
   579|     for fname in ("records.csv", "file_metadata.csv"):
   580|         src = records_dir / fname
   581|         if not src.is_file():
   582|             continue
   583|         dst = segment_records_dir / fname
   584|         if preshard_marker_valid:
   585|             continue  # preshard already wrote this segment's inputs
   586|         with src.open("r", encoding="utf-8-sig", newline="") as f:
   587|             reader = csv.DictReader(f)
   588|             fieldnames = list(reader.fieldnames or [])
   589|             rows = [r for r in reader if r.get("export_run_id", "").strip() in allowed_ids]
   590|         with dst.open("w", newline="", encoding="utf-8") as f:
   591|             writer = csv.DictWriter(f, fieldnames=fieldnames)
   592|             writer.writeheader()
   593|             writer.writerows(rows)
   594| 
   595|     # Copy filtered identity_items shards so synopsis formatter has behavioral
   596|     # parameters at segment emit time. Without this, _load_identity_items_by_record
   597|     # returns {} for every domain and all synopsis-resolvable patterns fall through
   598|     # to modal or fallback.
   599|     corpus_shard_dir = records_dir / "identity_items_by_domain"
   600|     if corpus_shard_dir.is_dir():
   601|         seg_shard_dir = segment_records_dir / "identity_items_by_domain"
   602|         seg_shard_dir.mkdir(parents=True, exist_ok=True)
   603|         for shard_file in sorted(corpus_shard_dir.iterdir()):
   604|             if not shard_file.is_file() or not shard_file.suffix == ".csv":
   605|                 continue
   606|             dst_shard = seg_shard_dir / shard_file.name
   607|             if preshard_marker_valid:
   608|                 continue  # preshard already wrote this segment's inputs
   609|             with shard_file.open("r", encoding="utf-8-sig", newline="") as f:
   610|                 reader = csv.DictReader(f)
   611|                 fieldnames = list(reader.fieldnames or [])
   612|                 rows = [
   613|                     r for r in reader
   614|                     if r.get("export_run_id", "").strip() in allowed_ids
   615|                 ]
   616|             if not rows:
   617|                 continue
   618|             with dst_shard.open("w", newline="", encoding="utf-8") as f:
   619|                 writer = csv.DictWriter(f, fieldnames=fieldnames)
   620|                 writer.writeheader()
   621|                 writer.writerows(rows)
   622|         # Write completion marker so partial runs are detectable
   623|         (seg_shard_dir / ".complete").write_text("ok", encoding="utf-8")
   624| 
   625| 
   626| def _filter_name_key_csv_to_segment(
   627|     name_key_results_csv: Path,
   628|     out_csv: Path,
   629|     allowed_ids: set,
   630| ) -> int:
   631|     """Filter a corpus-wide name_key_results.csv (tools/apply_name_key_policy.py output,
   632|     computed once for the whole corpus -- there is no per-segment re-parse of raw JSON,
   633|     unlike the join_hash "patterns" step below) down to one segment's file population, so
   634|     tools/generate_name_key_patterns.py re-clusters name-identity patterns scoped to just
   635|     this segment -- the name-projection analog of run_extract_all.py's
   636|     --filter-export-run-ids for the config/join_hash "patterns" step.
   637| 
   638|     name_key_results.csv's `export_file` column is the raw *.details.json/*.index.json
   639|     basename PR1 saw on disk, not necessarily the canonical export_run_id a segment's
   640|     export_run_ids.txt uses (tools/bundle_analysis/name_projection_adapter.py's
   641|     normalize_export_run_id() documents why those differ for a split-export pair).
   642|     Membership is tested against the normalized id first so a segment's export_run_ids.txt
   643|     actually matches split-export rows; each row's own export_file value is left
   644|     unmodified in the output -- stage_name_projection_analysis_dir() normalizes it again
   645|     downstream when building bundle-pipeline input, so re-normalizing here would be
   646|     redundant, not incorrect, but keeping the raw value is what the filter's one job
   647|     (membership, not transformation) calls for.
   648| 
   649|     If the normalized id isn't in allowed_ids, the raw (un-normalized) export_file is also
   650|     tried before excluding the row. normalize_export_run_id() can't distinguish a genuine
   651|     split-export pair from a details-only export with no sibling *.index.json file --
   652|     tools/extractor.py's _iter_export_files() keeps the *.details.json name itself as the
   653|     canonical export_run_id in that case (there is no *.index.json to rewrite to), so
   654|     blindly normalizing every *.details.json row would silently drop every row for that
   655|     export from the segment (PR #390 review). allowed_ids is this segment's own real
   656|     membership list, not a heuristic guess, so trying the raw id against it is safe.
   657| 
   658|     Returns the number of rows written.
   659|     """
   660|     if not name_key_results_csv.is_file():
   661|         raise FileNotFoundError(f"--name-key-results-csv not found: {name_key_results_csv}")
   662| 
   663|     def _in_segment(raw_export_file: str) -> bool:
   664|         if not raw_export_file:
   665|             return False
   666|         if normalize_export_run_id(raw_export_file) in allowed_ids:
   667|             return True
   668|         return raw_export_file in allowed_ids
   669| 
   670|     with name_key_results_csv.open("r", encoding="utf-8-sig", newline="") as f:
   671|         reader = csv.DictReader(f)
   672|         fieldnames = list(reader.fieldnames or [])
   673|         rows = [r for r in reader if _in_segment((r.get("export_file", "") or "").strip())]
   674|     out_csv.parent.mkdir(parents=True, exist_ok=True)
   675|     with out_csv.open("w", newline="", encoding="utf-8") as f:
   676|         writer = csv.DictWriter(f, fieldnames=fieldnames)
   677|         writer.writeheader()
   678|         writer.writerows(rows)
   679|     return len(rows)
   680| 
   681| 
   682| # ── Diagnostic helpers ────────────────────────────────────────────────────────
   683| 
   684| def _build_patterns_missing_notes(
   685|     sid: str,
   686|     out_root: Path,
   687|     records_dir: Path,
   688|     patterns_stderr: str,
   689| ) -> str:
   690|     """Build a diagnostic failure message when patterns exits 0 but writes no output."""
   691|     parts = [
   692|         f"step=patterns returncode=0 but pattern_presence_file.csv was not written.",
   693|         f"segment={sid}",
   694|         "emit_analysis was skipped — most likely because no records matched the export_run_id filter.",
   695|         "",
   696|     ]
   697| 
   698|     ids_file = out_root / "export_run_ids.txt"
   699|     if ids_file.is_file():
   700|         ids = [l.strip() for l in ids_file.read_text(encoding="utf-8").splitlines() if l.strip()]
   701|         parts.append(f"export_run_ids.txt: {len(ids)} IDs")
   702|         if ids:
   703|             parts.append(f"  first 3: {ids[:3]}")
   704|     else:
   705|         parts.append(f"export_run_ids.txt NOT FOUND at {ids_file}")
   706| 
   707|     records_csv = records_dir / "records.csv"
   708|     if records_csv.is_file():
   709|         with records_csv.open("r", encoding="utf-8-sig", newline="") as f:
   710|             rdr = csv.reader(f)
   711|             header = next(rdr, [])
   712|             first_row = next(rdr, [])
   713|         row_dict = dict(zip(header, first_row)) if first_row else {}
   714|         first_eid = row_dict.get("export_run_id", "<column missing>")
   715|         parts.append(f"records.csv first export_run_id: {first_eid!r}")
   716|     else:
   717|         parts.append(f"records.csv NOT FOUND at {records_csv}")
   718| 
   719|     meta_csv = records_dir / "file_metadata.csv"
   720|     if meta_csv.is_file():
   721|         with meta_csv.open("r", encoding="utf-8-sig", newline="") as f:
   722|             rdr = csv.reader(f)
   723|             header = next(rdr, [])
   724|             first_row = next(rdr, [])
   725|         row_dict = dict(zip(header, first_row)) if first_row else {}
   726|         first_eid = row_dict.get("export_run_id", "<column missing>")
   727|         parts.append(f"file_metadata.csv first export_run_id: {first_eid!r}")
   728|     else:
   729|         parts.append(f"file_metadata.csv NOT FOUND at {meta_csv}")
   730| 
   731|     # Surface WARN lines from patterns stderr (run_extract_all.py warnings)
   732|     warn_lines = [ln for ln in patterns_stderr.splitlines() if "[WARN extract_all]" in ln]
   733|     if warn_lines:
   734|         parts.append("")
   735|         parts.append("patterns stderr warnings:")
   736|         parts.extend(f"  {ln}" for ln in warn_lines[-10:])
   737| 
   738|     return "\n".join(parts)
   739| 
   740| 
   741| # ── BI merge ─────────────────────────────────────────────────────────────────
   742| 
   743| def _active_domains_from_presence_csv(analysis_dir: Path) -> Optional[frozenset]:
   744|     """Return the set of domain names present in pattern_presence_file.csv, or None on failure.
   745| 
   746|     Mirrors the domain-discovery logic in run_bundle_analysis.py so the merge
   747|     uses exactly the same domain set that the bundle step processed.
   748|     Returns None (not an empty frozenset) when the file is absent or contains no
   749|     domains, so callers fall back to unfiltered behaviour rather than writing
   750|     empty combined files.
   751|     """
   752|     presence_csv = analysis_dir / "pattern_presence_file.csv"
   753|     if not presence_csv.is_file():
   754|         return None
   755|     with presence_csv.open("r", encoding="utf-8-sig", newline="") as fh:
   756|         reader = csv.DictReader(fh)
   757|         domains = frozenset(
   758|             r.get("domain", "").strip() for r in reader if r.get("domain", "").strip()
   759|         )
   760|     return domains if domains else None
   761| 
   762| 
   763| def _active_domains_from_name_patterns(name_patterns_dir: Path) -> Optional[frozenset]:
   764|     """Same purpose as _active_domains_from_presence_csv(), but for the name-projection
   765|     pattern shape (tools/generate_name_key_patterns.py's domain_patterns.csv has no
   766|     pattern_presence_file.csv equivalent -- see DECISIONS.md D-037 for the schema diff).
   767| 
   768|     Unlike _active_domains_from_presence_csv(), an empty-but-present domain_patterns.csv
   769|     is a legitimate, expected outcome for the name projection (a segment whose files don't
   770|     intersect any of the 25 eligible domains -- see DECISIONS.md D-037's "what this PR
   771|     does not attempt"), not a signal to fall back to "unfiltered." This function therefore
   772|     returns `frozenset()` (not `None`) when the file exists but has zero domain rows, so
   773|     merge_bi_outputs() excludes every domain subfolder instead of treating None-as-unfiltered
   774|     and resurrecting stale per-domain output left over from a previous run of this segment
   775|     under a different (larger) population. `None` is reserved for "the file is missing" --
   776|     a genuinely different condition (the name-patterns step never ran or failed).
   777|     """
   778|     patterns_csv = name_patterns_dir / "domain_patterns.csv"
   779|     if not patterns_csv.is_file():
   780|         return None
   781|     with patterns_csv.open("r", encoding="utf-8-sig", newline="") as fh:
   782|         reader = csv.DictReader(fh)
   783|         return frozenset(
   784|             r.get("domain", "").strip() for r in reader if r.get("domain", "").strip()
   785|         )
   786| 
   787| 
   788| def _segment_has_name_leg_output(out_root: Path) -> bool:
   789|     """Whether this segment's name-projection leg (step 2b/3b/BI-merge-name) has already
   790|     completed at least once. emit_name_target_provenance() (tools/bundle_analysis's
   791|     --comparison-target name path) always writes bundle_provenance.csv on a successful run,
   792|     even when the segment's name-target pattern set comes back empty -- so its presence is
   793|     a reliable "name leg already ran" marker, independent of run_registry.csv's single
   794|     whole-segment `status` column, which has no notion of per-leg completion. Used so a
   795|     segment already marked complete under a config-only run isn't skipped once the operator
   796|     later asks for --comparison-target name/both -- see PR #390 review.
   797| 
   798|     bundle_provenance.csv lives at bundle_analysis/name_all/ (the flat, single-path-segment
   799|     BI-facing output location -- see run_bundle_analysis_for_target()'s docstring), not
   800|     under the internal bundle_analysis/name/ staging path."""
   801|     return (out_root / "results" / "bundle_analysis" / "name_all" / "bundle_provenance.csv").is_file()
   802| 
   803| 
   804| def merge_bi_outputs(bundle_analysis_dir: Path, active_domains: Optional[frozenset] = None) -> dict:
   805|     """Pre-merge per-domain bundle analysis CSVs into single combined files for Power BI.
   806| 
   807|     active_domains: when provided, only subfolders whose name is in this set are
   808|     merged.  Pass the set derived from pattern_presence_file.csv so that stale
   809|     domain folders left over from earlier runs are excluded.
   810| 
   811|     When a filename has no current candidates (active_domains excludes every existing
   812|     folder, or none exist at all), any pre-existing `{stem}_combined.csv` from a previous
   813|     run is deleted rather than left in place -- otherwise a rerun that legitimately finds
   814|     nothing (e.g. a segment whose active domain set has genuinely gone from non-empty to
   815|     empty) would leave Power BI reading stale bundle data as if it were current (PR #390
   816|     review).
   817|     """
   818|     if not bundle_analysis_dir.is_dir():
   819|         return {}
   820| 
   821|     result: Dict[str, dict] = {}
   822|     for filename in BI_MERGE_FILES:
   823|         stem = Path(filename).stem
   824|         out_path = bundle_analysis_dir / f"{stem}_combined.csv"
   825| 
   826|         candidates = [
   827|             p for p in bundle_analysis_dir.glob(f"*/{filename}")
   828|             if "_population_discovery" not in str(p)
   829|             and "_population_runs" not in str(p)
   830|             and (active_domains is None or p.parent.name in active_domains)
   831|         ]
   832|         if not candidates:
   833|             if out_path.is_file():
   834|                 out_path.unlink()
   835|             continue
   836| 
   837|         header: Optional[List[str]] = None
   838|         all_rows: List[Dict[str, str]] = []
   839|         files_merged = 0
   840|         for csv_path in sorted(candidates):
   841|             with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
   842|                 reader = csv.DictReader(fh)
   843|                 file_header = list(reader.fieldnames or [])
   844|                 rows = [
   845|                     {str(k): "" if v is None else str(v) for k, v in row.items()}
   846|                     for row in reader
   847|                 ]
   848|             if not file_header:
   849|                 # Truly empty file — no header at all; skip without counting
   850|                 continue
   851|             if header is None:
   852|                 header = file_header
   853|             elif file_header != header:
   854|                 print(
   855|                     f"[WARN orchestrator] bi_merge header mismatch in {csv_path} "
   856|                     f"(expected {header}, got {file_header}) — skipping",
   857|                     flush=True,
   858|                 )
   859|                 continue
   860|             all_rows.extend(rows)
   861|             files_merged += 1
   862| 
   863|         if header is None:
   864|             if out_path.is_file():
   865|                 out_path.unlink()
   866|             continue
   867| 
   868|         atomic_write_csv(out_path, header, all_rows)
   869|         result[filename] = {"files_merged": files_merged, "rows_written": len(all_rows)}
   870| 
   871|     return result
   872| 
   873| 
   874| # ── Core orchestration ────────────────────────────────────────────────────────
   875| 
   876| def build_run_plan(
   877|     manifest: Dict[str, dict],
   878|     registry: List[dict],
   879|     segment_filter: Optional[str],
   880|     force: bool,
   881| ) -> List[tuple[dict, dict]]:
   882|     """
   883|     Return ordered list of (registry_row, manifest_row) pairs for bundle segments,
   884|     sorted by segment_level asc then segment_id asc.
   885|     Segments to skip are excluded; dry-run callers handle skip annotation separately.
   886|     """
   887|     run_rows = [r for r in registry if r.get("run_type", "").strip() in {"bundle", "reference"}]
   888| 
   889|     def sort_key(row: dict) -> tuple:
   890|         sid = row.get("segment_id", "")
   891|         mrow = manifest.get(sid, {})
   892|         try:
   893|             level = int(mrow.get("segment_level", 0))
   894|         except (ValueError, TypeError):
   895|             level = 0
   896|         return (level, sid)
   897| 
   898|     run_rows.sort(key=sort_key)
   899| 
   900|     plan: List[tuple[dict, dict]] = []
   901|     for reg_row in run_rows:
   902|         sid = reg_row.get("segment_id", "").strip()
   903|         mrow = manifest.get(sid, {})
   904|         plan.append((reg_row, mrow))
   905|     return plan
   906| 
   907| 
```
