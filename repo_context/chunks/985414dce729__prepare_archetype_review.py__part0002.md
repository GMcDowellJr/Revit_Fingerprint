# Chunk of tools/archetype/prepare_archetype_review.py

- Source relative path: `tools/archetype/prepare_archetype_review.py`
- Chunk: 2 of 3
- Original line range: 521-900
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _selected_file_name_status, _select_schedule_rows_for_cluster, _write_review_schedule_outputs, _sort_key, _process_cluster
- Source SHA-256: 03bdf22e06a40e3b31dd69dea0931eb1695547b7a49d5dbbd3ada414575c6244
- Starts inside symbol: no
- Ends inside symbol: no

```
   521| def _selected_file_name_status(selected_rows: List[Dict[str, str]], selected_file_is_in_review_sample: bool) -> str:
   522|     if not selected_rows:
   523|         return "missing_validation_detail_for_selected_file"
   524|     if any(_is_named_element(r.get("element_name", "")) for r in selected_rows):
   525|         return "named"
   526| 
   527|     reasons: List[str] = []
   528|     if not selected_file_is_in_review_sample:
   529|         reasons.append("not_in_review_sample")
   530|     if all(not r.get("sig_hash", "") for r in selected_rows):
   531|         if any("material" in (r.get("source_domain", "").lower()) for r in selected_rows):
   532|             reasons.append("unresolved_materials_pending_hash_policy")
   533|         else:
   534|             reasons.append("missing_sig_hash")
   535|     else:
   536|         reasons.append("no_records_match")
   537|     return "|".join(dict.fromkeys(reasons))
   538| 
   539| 
   540| def _select_schedule_rows_for_cluster(
   541|     cluster_id: str,
   542|     cluster_label_stub: str,
   543|     review_rows: List[Dict[str, str]],
   544|     candidate_rows: Optional[List[Dict[str, str]]] = None,
   545| ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
   546|     """Select manual file-open schedule rows.
   547| 
   548|     ``review_rows`` are the final review_<cluster>.csv sample and are used for
   549|     cluster-level name diagnostics. ``candidate_rows`` may include enriched
   550|     rows outside the top-N review sample so Project files can still be selected
   551|     for manual file-open review.
   552|     """
   553|     cluster_has_named_review_examples = any(_is_named_element(r.get("element_name", "")) for r in review_rows)
   554|     candidates = candidate_rows if candidate_rows is not None else review_rows
   555|     usable_candidates = [r for r in candidates if r.get("signal_id", "") != FILE_LEVEL_SENTINEL_SIGNAL_ID]
   556| 
   557|     selected_export_run_id = ""
   558|     selected_rows: List[Dict[str, str]] = []
   559|     if usable_candidates:
   560|         rows_by_file: Dict[str, List[Dict[str, str]]] = defaultdict(list)
   561|         for row in usable_candidates:
   562|             rows_by_file[row.get("export_run_id", "")].append(row)
   563|         selected_export_run_id, selected_rows = sorted(
   564|             rows_by_file.items(),
   565|             key=lambda kv: _schedule_file_sort_key(kv[1]),
   566|         )[0]
   567| 
   568|     review_export_run_ids = {r.get("export_run_id", "") for r in review_rows}
   569|     selected_file_is_in_review_sample = bool(selected_export_run_id and selected_export_run_id in review_export_run_ids)
   570|     selected_file_has_named_examples = any(_is_named_element(r.get("element_name", "")) for r in selected_rows)
   571|     selected_file_name_status = _selected_file_name_status(selected_rows, selected_file_is_in_review_sample)
   572|     selected_project_file_unresolved_but_cluster_has_named_examples = bool(
   573|         cluster_has_named_review_examples
   574|         and selected_rows
   575|         and selected_rows[0].get("governance_role", "") == "Project"
   576|         and not selected_file_has_named_examples
   577|     )
   578| 
   579|     schedule_rows: List[Dict[str, Any]] = []
   580|     for row in sorted(selected_rows, key=_schedule_row_sort_key):
   581|         schedule_rows.append({
   582|             "cluster_id": cluster_id,
   583|             "cluster_label_stub": cluster_label_stub,
   584|             "signal_id": row.get("signal_id", ""),
   585|             "representative_source": "review_file" if selected_file_is_in_review_sample else "classification_only",
   586|             "file_path": row.get("file_path", ""),
   587|             "export_run_id": row.get("export_run_id", ""),
   588|             "governance_role": row.get("governance_role", ""),
   589|             "n_signals_fired": row.get("n_signals_fired", ""),
   590|             "all_signals_fired": row.get("all_signals_fired", ""),
   591|             "source_domain": row.get("source_domain", ""),
   592|             "source_join_hash": row.get("source_join_hash", ""),
   593|             "element_name": row.get("element_name", ""),
   594|             "sig_hash": row.get("sig_hash", ""),
   595|             "param_names": row.get("param_names", ""),
   596|             "category_names": row.get("category_names", ""),
   597|         })
   598| 
   599|     schedule_named_rows = sum(1 for r in schedule_rows if _is_named_element(str(r.get("element_name", ""))))
   600|     schedule_name_regression = bool(
   601|         not selected_project_file_unresolved_but_cluster_has_named_examples
   602|         and cluster_has_named_review_examples
   603|         and schedule_rows
   604|         and schedule_named_rows == 0
   605|     )
   606|     diagnostics = {
   607|         "review_rows": len(review_rows),
   608|         "review_named_rows": sum(1 for r in review_rows if _is_named_element(r.get("element_name", ""))),
   609|         "schedule_rows": len(schedule_rows),
   610|         "schedule_named_rows": schedule_named_rows,
   611|         "cluster_has_named_review_examples": cluster_has_named_review_examples,
   612|         "selected_file_has_named_examples": selected_file_has_named_examples,
   613|         "selected_file_is_in_review_sample": selected_file_is_in_review_sample,
   614|         "selected_file_name_status": selected_file_name_status,
   615|         "selected_project_file_unresolved_but_cluster_has_named_examples": selected_project_file_unresolved_but_cluster_has_named_examples,
   616|         "schedule_name_regression": schedule_name_regression,
   617|         "has_usable_review_rows": bool(usable_candidates),
   618|     }
   619|     for row in schedule_rows:
   620|         row.update({
   621|             "review_rows": diagnostics["review_rows"],
   622|             "review_named_rows": diagnostics["review_named_rows"],
   623|             "schedule_rows": diagnostics["schedule_rows"],
   624|             "schedule_named_rows": diagnostics["schedule_named_rows"],
   625|             "cluster_has_named_review_examples": "true" if cluster_has_named_review_examples else "false",
   626|             "selected_file_has_named_examples": "true" if selected_file_has_named_examples else "false",
   627|             "selected_file_is_in_review_sample": "true" if selected_file_is_in_review_sample else "false",
   628|             "selected_file_name_status": selected_file_name_status,
   629|             "selected_project_file_unresolved_but_cluster_has_named_examples": "true" if selected_project_file_unresolved_but_cluster_has_named_examples else "false",
   630|             "schedule_name_regression": "true" if schedule_name_regression else "false",
   631|         })
   632|     return schedule_rows, diagnostics
   633| 
   634| 
   635| def _write_review_schedule_outputs(
   636|     out_dir: Path,
   637|     results: List[Dict[str, Any]],
   638|     dry_run: bool,
   639| ) -> None:
   640|     """Build review schedule/gap CSVs from review files plus enriched candidates."""
   641|     schedule_rows: List[Dict[str, Any]] = []
   642|     gap_rows: List[Dict[str, Any]] = []
   643| 
   644|     for result in results:
   645|         cluster_id = str(result.get("cluster_id", ""))
   646|         cluster_label_stub = str(result.get("cluster_label_stub", ""))
   647|         out_path = Path(result.get("out_path", out_dir / f"review_{cluster_id}.csv"))
   648|         review_rows = read_csv_rows(out_path) if out_path.is_file() else []
   649|         candidate_rows = result.get("schedule_candidate_rows")
   650|         cluster_schedule_rows, diagnostics = _select_schedule_rows_for_cluster(
   651|             cluster_id,
   652|             cluster_label_stub,
   653|             review_rows,
   654|             candidate_rows if isinstance(candidate_rows, list) else None,
   655|         )
   656|         schedule_rows.extend(cluster_schedule_rows)
   657| 
   658|         log(
   659|             STAGE,
   660|             f"cluster_id={cluster_id}: review_rows={diagnostics['review_rows']} "
   661|             f"review_named_rows={diagnostics['review_named_rows']} "
   662|             f"schedule_rows={diagnostics['schedule_rows']} "
   663|             f"schedule_named_rows={diagnostics['schedule_named_rows']} "
   664|             f"cluster_has_named_review_examples={str(diagnostics['cluster_has_named_review_examples']).lower()} "
   665|             f"selected_file_has_named_examples={str(diagnostics['selected_file_has_named_examples']).lower()} "
   666|             f"selected_file_is_in_review_sample={str(diagnostics['selected_file_is_in_review_sample']).lower()} "
   667|             f"selected_file_name_status={diagnostics['selected_file_name_status']} "
   668|             f"selected_project_file_unresolved_but_cluster_has_named_examples={str(diagnostics['selected_project_file_unresolved_but_cluster_has_named_examples']).lower()} "
   669|             f"schedule_name_regression={str(diagnostics['schedule_name_regression']).lower()}",
   670|         )
   671| 
   672|         if not diagnostics["has_usable_review_rows"]:
   673|             gap_rows.append({
   674|                 "cluster_id": cluster_id,
   675|                 "cluster_label_stub": cluster_label_stub,
   676|                 "reason": "no_usable_review_rows",
   677|                 "review_rows": diagnostics["review_rows"],
   678|                 "review_named_rows": diagnostics["review_named_rows"],
   679|                 "schedule_rows": diagnostics["schedule_rows"],
   680|                 "schedule_named_rows": diagnostics["schedule_named_rows"],
   681|                 "cluster_has_named_review_examples": "true" if diagnostics["cluster_has_named_review_examples"] else "false",
   682|                 "selected_file_has_named_examples": "true" if diagnostics["selected_file_has_named_examples"] else "false",
   683|                 "selected_file_is_in_review_sample": "true" if diagnostics["selected_file_is_in_review_sample"] else "false",
   684|                 "selected_file_name_status": diagnostics["selected_file_name_status"],
   685|                 "selected_project_file_unresolved_but_cluster_has_named_examples": "true" if diagnostics["selected_project_file_unresolved_but_cluster_has_named_examples"] else "false",
   686|                 "schedule_name_regression": "true" if diagnostics["schedule_name_regression"] else "false",
   687|             })
   688| 
   689|     schedule_path = out_dir / "archetype_review_schedule.csv"
   690|     gaps_path = out_dir / "archetype_review_gaps.csv"
   691|     if dry_run:
   692|         log(STAGE, f"dry-run: would write {len(schedule_rows)} rows to {schedule_path}")
   693|         log(STAGE, f"dry-run: would write {len(gap_rows)} rows to {gaps_path}")
   694|         return
   695| 
   696|     atomic_write_csv(schedule_path, SCHEDULE_FIELDS, schedule_rows)
   697|     log(STAGE, f"wrote {len(schedule_rows)} rows to {schedule_path}")
   698|     atomic_write_csv(gaps_path, GAPS_FIELDS, gap_rows)
   699|     log(STAGE, f"wrote {len(gap_rows)} rows to {gaps_path}")
   700| 
   701| 
   702| def _sort_key(row: Dict[str, str]) -> Tuple[int, int, int, int, str]:
   703|     # Detail-backed rows should consume --top-n slots before unresolved
   704|     # file-level sentinels, even when the sentinel file has stronger
   705|     # classification ranking metadata.
   706|     sentinel_rank = 1 if row["signal_id"] == FILE_LEVEL_SENTINEL_SIGNAL_ID else 0
   707|     governance_role_rank = GOVERNANCE_ROLE_ORDER.get(row["governance_role"], 3)
   708|     try:
   709|         n_signals_fired = int(row["n_signals_fired"])
   710|     except (TypeError, ValueError):
   711|         n_signals_fired = 0
   712|     all_signals_fired = 1 if row["all_signals_fired"] == "true" else 0
   713|     return (sentinel_rank, governance_role_rank, -n_signals_fired, -all_signals_fired, row["export_run_id"])
   714| 
   715| 
   716| def _process_cluster(
   717|     ctx: ClusterContext,
   718|     label_by_domain_sig: Dict[Tuple[str, str, str], Tuple[str, str]],
   719|     label_by_sig: Dict[Tuple[str, str], Tuple[str, str]],
   720|     label_by_record_pk: Dict[Tuple[str, str, str], Tuple[str, str]],
   721|     vfd_sig_to_record_pk: Dict[Tuple[str, str, str], str],
   722|     vfd_resolution: Dict[Tuple[str, str], Tuple[str, str]],
   723|     file_path_lookup: Dict[str, str],
   724|     out_dir: Path,
   725|     top_n: int,
   726|     dry_run: bool,
   727|     verbose: bool,
   728| ) -> Dict[str, Any]:
   729|     # Stage 7: assemble and sort the review table.
   730|     review_rows: List[Dict[str, str]] = []
   731|     for detail in ctx.detail_by_file_signal.values():
   732|         export_run_id = detail.get("export_run_id", "")
   733|         signal_id = detail.get("signal_id", "") or detail.get("edge_id", "")
   734|         source_join_hash = detail.get("source_join_hash", "")
   735|         cls = ctx.classification_by_file.get(export_run_id, {})
   736|         source_domain = detail.get("source_domain", "")
   737|         sig_hash = detail.get("sig_hash", "")
   738|         source_record_pk = detail.get("source_record_pk", "")
   739| 
   740|         label_display, label_quality = ("", "")
   741|         if source_domain and source_record_pk:
   742|             label_display, label_quality = label_by_record_pk.get((export_run_id, source_domain, source_record_pk), ("", ""))
   743|         if not label_display and source_domain and sig_hash:
   744|             label_display, label_quality = label_by_domain_sig.get((export_run_id, source_domain, sig_hash), ("", ""))
   745|         if not label_display and sig_hash:
   746|             label_display, label_quality = label_by_sig.get((export_run_id, sig_hash), ("", ""))
   747| 
   748|         if label_display:
   749|             element_name = label_display
   750|         elif not sig_hash:
   751|             element_name = "(unresolved — missing sig_hash)"
   752|         else:
   753|             quality_suffix = f"; label_quality={label_quality}" if label_quality else ""
   754|             element_name = f"(unresolved — no records.csv label match for sig_hash {sig_hash[:8]}{quality_suffix})"
   755| 
   756|         param_names = ""
   757|         category_names = ""
   758|         if source_domain == "view_filter_definitions":
   759|             record_pk = source_record_pk or vfd_sig_to_record_pk.get((export_run_id, source_domain, sig_hash), "")
   760|             param_names, category_names = vfd_resolution.get((export_run_id, record_pk), ("", ""))
   761| 
   762|         file_path = file_path_lookup.get(export_run_id) or export_run_id
   763| 
   764|         review_rows.append({
   765|             "file_path": file_path,
   766|             "export_run_id": export_run_id,
   767|             "governance_role": cls.get("governance_role", ""),
   768|             "discipline_label": cls.get("discipline_label", ""),
   769|             "unit_system": cls.get("unit_system", ""),
   770|             "client_label": cls.get("client_label", ""),
   771|             "n_signals_fired": cls.get("n_signals_fired", ""),
   772|             "all_signals_fired": cls.get("all_signals_fired", ""),
   773|             "signal_id": signal_id,
   774|             "source_domain": source_domain,
   775|             "source_join_hash": source_join_hash,
   776|             "element_name": element_name,
   777|             "sig_hash": sig_hash,
   778|             "param_names": param_names,
   779|             "category_names": category_names,
   780|         })
   781| 
   782|     # Fallback: detail join missed some qualifying files.
   783|     #
   784|     # This happens when archetype_validation_detail.csv has no entries that
   785|     # match the cluster's signal_ids for the qualifying files -- most commonly
   786|     # seen on VFD clusters where the detail rows don't propagate through the
   787|     # governance_question reclassification join. Rather than silently writing
   788|     # a header-only CSV, or dropping partially unresolved qualifying files from
   789|     # a mixed-detail cluster, emit one file-level sentinel row per qualifying
   790|     # file that has no assembled detail-driven review rows.
   791|     # Do not expand ctx.signal_ids here: archetype_cluster_classifications.csv
   792|     # only carries the fired count/all-fired flag at this grain, not the
   793|     # per-file fired signal IDs, so expanding the whole cluster would create
   794|     # false-positive signal rows for partially matching files.
   795|     files_with_review_rows = {row["export_run_id"] for row in review_rows}
   796|     missing_detail_export_run_ids = [
   797|         export_run_id
   798|         for export_run_id in ctx.classification_by_file
   799|         if export_run_id not in files_with_review_rows
   800|     ]
   801|     if missing_detail_export_run_ids:
   802|         if verbose:
   803|             log(
   804|                 STAGE,
   805|                 f"WARNING: cluster_id={ctx.cluster_id} has "
   806|                 f"{len(missing_detail_export_run_ids)} qualifying files without "
   807|                 f"assembled validation detail rows; emitting classification-only "
   808|                 f"file-level sentinel rows",
   809|             )
   810|         for export_run_id in missing_detail_export_run_ids:
   811|             cls = ctx.classification_by_file[export_run_id]
   812|             file_path = file_path_lookup.get(export_run_id) or export_run_id
   813|             review_rows.append({
   814|                 "file_path": file_path,
   815|                 "export_run_id": export_run_id,
   816|                 "governance_role": cls.get("governance_role", ""),
   817|                 "discipline_label": cls.get("discipline_label", ""),
   818|                 "unit_system": cls.get("unit_system", ""),
   819|                 "client_label": cls.get("client_label", ""),
   820|                 "n_signals_fired": cls.get("n_signals_fired", ""),
   821|                 "all_signals_fired": cls.get("all_signals_fired", ""),
   822|                 "signal_id": FILE_LEVEL_SENTINEL_SIGNAL_ID,
   823|                 "source_domain": "",
   824|                 "source_join_hash": "",
   825|                 "element_name": "(unresolved — validation detail missing)",
   826|                 "sig_hash": "",
   827|                 "param_names": "",
   828|                 "category_names": "",
   829|             })
   830| 
   831|     review_rows.sort(key=_sort_key)
   832| 
   833|     # Apply --top-n: keep only the first N unique export_run_id values.
   834|     ordered_export_run_ids: List[str] = []
   835|     seen_export_run_ids: Set[str] = set()
   836|     for row in review_rows:
   837|         eid = row["export_run_id"]
   838|         if eid not in seen_export_run_ids:
   839|             seen_export_run_ids.add(eid)
   840|             ordered_export_run_ids.append(eid)
   841| 
   842|     if top_n > 0:
   843|         selected_export_run_ids = ordered_export_run_ids[:top_n]
   844|     else:
   845|         selected_export_run_ids = ordered_export_run_ids
   846|     selected_set = set(selected_export_run_ids)
   847| 
   848|     output_rows = [row for row in review_rows if row["export_run_id"] in selected_set]
   849| 
   850|     # Stage 8: write output.
   851|     out_path = out_dir / f"review_{ctx.cluster_id}.csv"
   852|     if dry_run:
   853|         log(STAGE, f"dry-run: would write {len(output_rows)} rows to {out_path}")
   854|     else:
   855|         atomic_write_csv(out_path, OUT_FIELDS, output_rows)
   856|         log(STAGE, f"wrote {len(output_rows)} rows to {out_path}")
   857| 
   858|     total_files = len(ctx.qualifying_files)
   859|     n_all_signals_fired = sum(1 for row in ctx.classification_by_file.values() if row.get("all_signals_fired") == "true")
   860|     pct_all = (n_all_signals_fired / total_files * 100.0) if total_files else 0.0
   861| 
   862|     if verbose:
   863|         print(f"Cluster: {ctx.cluster_id}")
   864|         print(f"Signals: {' | '.join(ctx.signal_ids)}")
   865|         print(f"Total files: {total_files}  |  All signals fired: {n_all_signals_fired} ({pct_all:.1f}%)  |  Top-N shown: {len(selected_export_run_ids)}")
   866|         print("Top example files (templates first):")
   867| 
   868|         rows_by_file: Dict[str, List[Dict[str, str]]] = defaultdict(list)
   869|         for row in output_rows:
   870|             rows_by_file[row["export_run_id"]].append(row)
   871| 
   872|         for eid in selected_export_run_ids[:_MAX_CONSOLE_EXAMPLES]:
   873|             rows_for_file = rows_by_file.get(eid, [])
   874|             governance_role = rows_for_file[0]["governance_role"] if rows_for_file else ""
   875|             file_label = f"[{governance_role}] " if governance_role else ""
   876|             print(f"  {file_label}{Path(rows_for_file[0]['file_path']).name if rows_for_file else eid}")
   877|             for row in rows_for_file:
   878|                 kind = row["signal_id"].split("__")[0]
   879|                 if row["source_domain"] == "view_filter_definitions":
   880|                     suffix = f" filter → categories: {row['category_names']}" if row["category_names"] else " filter"
   881|                 else:
   882|                     suffix = f" ({row['source_domain']})"
   883|                 print(f"    {kind}: \"{row['element_name']}\"{suffix}")
   884| 
   885|         if len(selected_export_run_ids) > _MAX_CONSOLE_EXAMPLES:
   886|             print(f"  ... ({len(selected_export_run_ids) - _MAX_CONSOLE_EXAMPLES} more in CSV)")
   887| 
   888|     return {
   889|         "cluster_id": ctx.cluster_id,
   890|         "cluster_label_stub": ctx.cluster_label_stub,
   891|         "total_files": total_files,
   892|         "n_all_signals_fired": n_all_signals_fired,
   893|         "pct_all": pct_all,
   894|         "top_n_shown": len(selected_export_run_ids),
   895|         "n_rows": len(output_rows),
   896|         "out_path": out_path,
   897|         "schedule_candidate_rows": review_rows,
   898|     }
   899| 
   900| 
```
