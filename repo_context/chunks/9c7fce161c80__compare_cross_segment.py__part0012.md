# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 12 of 13
- Original line range: 5148-5379
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: main
- Ends inside symbol: no

```
  5148|                     policy=policy,
  5149|                     crid=crid,
  5150|                     seg_ref=seg_a,
  5151|                     seg_tgt=seg_b,
  5152|                     comparison_type=ctype,
  5153|                     domain=domain,
  5154|                     manifest=manifest,
  5155|                     registry=registry,
  5156|                     segments_root=segments_root,
  5157|                     executed_utc=executed_utc,
  5158|                 )
  5159|                 if state_rows:
  5160|                     governance_state_rows.extend(state_rows)
  5161|                     governance_state_summary_rows.append(state_summary)
  5162|                     governance_combo_count += 1
  5163|                     produced_output = True
  5164| 
  5165|             # comparison_registry.csv must only stamp (pair, domain) work items
  5166|             # that actually produced a persisted output row somewhere this run
  5167|             # (cross_segment_summary.csv via `result`, or governance-state
  5168|             # output) — a domain below --min-patterns or a within-project pair
  5169|             # with no eligible file pairs must not get a fresh "current" stamp
  5170|             # for output that was never written.
  5171|             if produced_output:
  5172|                 completed_work_items.append((seg_a, seg_b, ctype, domain))
  5173| 
  5174|             done = n_complete + n_skipped
  5175|             if done % 50 == 0 or done == len(work_items):
  5176|                 print(
  5177|                     f"[compare] progress: {done}/{len(work_items)} "
  5178|                     f"complete={n_complete} skipped={n_skipped}",
  5179|                     flush=True,
  5180|                 )
  5181| 
  5182|     elapsed = time.perf_counter() - t0
  5183|     print(
  5184|         f"[compare] done  pairs={len(runnable_pairs)}  active_domains={len(active_domain_filter)}  "
  5185|         f"work_items={len(work_items)}  complete={n_complete}  skipped={n_skipped}  "
  5186|         f"elapsed={elapsed:.1f}s  ({elapsed/60:.1f} min)",
  5187|         flush=True,
  5188|     )
  5189| 
  5190|     # Rows for cross_segment_file_pairs.csv are fully streamed to the temp file at
  5191|     # this point, but the rename into place is deferred to the "Write outputs"
  5192|     # section below (after pooled/union/reuse/matrix computation and the registry
  5193|     # write) rather than published here. Publishing immediately would let this one
  5194|     # file jump ahead to reflect the new run while comparison_registry.csv and the
  5195|     # other outputs still reflect the old run, if any later step below raises —
  5196|     # breaking the previous all-or-nothing guarantee across the output set.
  5197|     pair_detail_tmp.close()
  5198| 
  5199|     # Pooled comparison
  5200|     focal_filter: Optional[Set[str]] = None
  5201|     if args.segment_a or args.segment_b:
  5202|         focal_filter = set()
  5203|         if args.segment_a:
  5204|             focal_filter.add(args.segment_a)
  5205|         if args.segment_b:
  5206|             focal_filter.add(args.segment_b)
  5207| 
  5208|     pooled_rows = run_pooled_comparison(
  5209|         policy, manifest, registry, segments_root,
  5210|         args.min_patterns, executed_utc,
  5211|         domain_filter=args.domain,
  5212|         focal_segment_ids=focal_filter,
  5213|     )
  5214| 
  5215|     union_inventory_rows = build_union_inventory_rows(
  5216|         manifest, registry, file_metadata, segments_root, executed_utc,
  5217|         domain_filter=args.domain,
  5218|     )
  5219|     reuse_distribution_rows = build_pattern_reuse_distribution_rows(
  5220|         union_inventory_rows, executed_utc
  5221|     )
  5222|     reuse_summary_by_domain_rows = build_pattern_reuse_summary_rows(
  5223|         reuse_distribution_rows, by_client=False
  5224|     )
  5225|     reuse_summary_by_client_rows = build_pattern_reuse_summary_rows(
  5226|         reuse_distribution_rows, by_client=True
  5227|     )
  5228|     matrix_outputs, fragmentation_rows, matrix_manifest_rows = build_explicit_matrix_outputs(
  5229|         summary_rows, pooled_rows, union_inventory_rows, executed_utc
  5230|     )
  5231| 
  5232|     # Write outputs
  5233|     if summary_rows:
  5234|         sort_summary_rows(summary_rows)
  5235|         out_dir.mkdir(parents=True, exist_ok=True)
  5236|         atomic_write_csv(out_dir / "cross_segment_summary.csv", SUMMARY_FIELDS, summary_rows)
  5237|         print(f"[compare] wrote {len(summary_rows)} rows → {out_dir / 'cross_segment_summary.csv'}")
  5238| 
  5239|     # Publish the streamed cross_segment_file_pairs.csv here, alongside the other
  5240|     # outputs, so a failure anywhere above (pooled/union/reuse/matrix computation)
  5241|     # leaves the previous run's file untouched instead of a fresh pairs file paired
  5242|     # with stale companions. Rows are in worker-completion order, not the fully
  5243|     # sorted order sort_pair_detail_rows() used to produce — confirmed with the
  5244|     # requester that nothing downstream depends on that ordering.
  5245|     if pair_detail_row_count:
  5246|         pair_detail_tmp_path.replace(out_dir / "cross_segment_file_pairs.csv")
  5247|         print(
  5248|             f"[compare] wrote {pair_detail_row_count} rows (streamed, unsorted) → "
  5249|             f"{out_dir / 'cross_segment_file_pairs.csv'}"
  5250|         )
  5251|     else:
  5252|         pair_detail_tmp_path.unlink(missing_ok=True)
  5253| 
  5254|     if governance_state_rows:
  5255|         governance_state_rows.sort(key=lambda r: (
  5256|             r["comparison_type"],
  5257|             r["segment_id_reference"],
  5258|             r["segment_id_target"],
  5259|             r["domain"],
  5260|             r["state"],
  5261|             r["join_hash"],
  5262|         ))
  5263|         governance_state_summary_rows.sort(key=lambda r: (
  5264|             r["comparison_type"],
  5265|             r["segment_id_reference"],
  5266|             r["segment_id_target"],
  5267|             r["domain"],
  5268|         ))
  5269|         out_dir.mkdir(parents=True, exist_ok=True)
  5270|         atomic_write_csv(
  5271|             out_dir / "cross_segment_governance_states.csv",
  5272|             GOVERNANCE_STATE_FIELDS,
  5273|             governance_state_rows,
  5274|         )
  5275|         atomic_write_csv(
  5276|             out_dir / "cross_segment_governance_state_summary.csv",
  5277|             GOVERNANCE_STATE_SUMMARY_FIELDS,
  5278|             governance_state_summary_rows,
  5279|         )
  5280|         print(
  5281|             f"[compare] governance states written: {len(governance_state_rows)} rows across "
  5282|             f"{governance_combo_count} domain/pair combinations"
  5283|         )
  5284|         print(
  5285|             f"[compare] wrote {len(governance_state_rows)} rows → "
  5286|             f"{out_dir / 'cross_segment_governance_states.csv'}"
  5287|         )
  5288|         print(
  5289|             f"[compare] wrote {len(governance_state_summary_rows)} rows → "
  5290|             f"{out_dir / 'cross_segment_governance_state_summary.csv'}"
  5291|         )
  5292| 
  5293|     if delta_rows:
  5294|         delta_rows.sort(key=lambda r: (
  5295|             r["comparison_type"],
  5296|             r["segment_id_reference"],
  5297|             r["segment_id_target"],
  5298|             r["domain"],
  5299|             -float(r["pct_files_in_target"] or "0"),
  5300|             r["join_hash"],
  5301|         ))
  5302|         out_dir.mkdir(parents=True, exist_ok=True)
  5303|         atomic_write_csv(out_dir / "cross_segment_delta.csv", DELTA_FIELDS, delta_rows)
  5304|         print(
  5305|             f"[compare] delta patterns written: {len(delta_rows)} rows across "
  5306|             f"{delta_combo_count} domain/pair combinations"
  5307|         )
  5308|         print(f"[compare] wrote {len(delta_rows)} rows → {out_dir / 'cross_segment_delta.csv'}")
  5309| 
  5310|     if pooled_rows:
  5311|         out_dir.mkdir(parents=True, exist_ok=True)
  5312|         atomic_write_csv(out_dir / "cross_segment_pooled.csv", POOLED_FIELDS, pooled_rows)
  5313|         print(f"[compare] wrote {len(pooled_rows)} rows → {out_dir / 'cross_segment_pooled.csv'}")
  5314| 
  5315|     if union_inventory_rows:
  5316|         out_dir.mkdir(parents=True, exist_ok=True)
  5317|         atomic_write_csv(
  5318|             out_dir / "cross_segment_union_inventory.csv",
  5319|             UNION_INVENTORY_FIELDS,
  5320|             union_inventory_rows,
  5321|         )
  5322|         print(
  5323|             f"[compare] wrote {len(union_inventory_rows)} rows → "
  5324|             f"{out_dir / 'cross_segment_union_inventory.csv'}"
  5325|         )
  5326| 
  5327|     if reuse_distribution_rows:
  5328|         out_dir.mkdir(parents=True, exist_ok=True)
  5329|         atomic_write_csv(
  5330|             out_dir / "pattern_reuse_distribution.csv",
  5331|             REUSE_DISTRIBUTION_FIELDS,
  5332|             reuse_distribution_rows,
  5333|         )
  5334|         atomic_write_csv(
  5335|             out_dir / "pattern_reuse_summary_by_domain.csv",
  5336|             REUSE_SUMMARY_FIELDS,
  5337|             reuse_summary_by_domain_rows,
  5338|         )
  5339|         atomic_write_csv(
  5340|             out_dir / "pattern_reuse_summary_by_client.csv",
  5341|             REUSE_SUMMARY_FIELDS,
  5342|             reuse_summary_by_client_rows,
  5343|         )
  5344|         print(
  5345|             f"[compare] wrote {len(reuse_distribution_rows)} rows → "
  5346|             f"{out_dir / 'pattern_reuse_distribution.csv'}"
  5347|         )
  5348| 
  5349|     if matrix_outputs or fragmentation_rows or matrix_manifest_rows:
  5350|         out_dir.mkdir(parents=True, exist_ok=True)
  5351|         for filename, rows in sorted(matrix_outputs.items()):
  5352|             atomic_write_csv(out_dir / filename, MATRIX_OUTPUT_FIELDS, rows)
  5353|             print(f"[compare] wrote {len(rows)} rows → {out_dir / filename}")
  5354|         atomic_write_csv(
  5355|             out_dir / "project_fragmentation_diagnostic.csv",
  5356|             FRAGMENTATION_DIAGNOSTIC_FIELDS,
  5357|             fragmentation_rows,
  5358|         )
  5359|         atomic_write_csv(
  5360|             out_dir / "matrix_output_manifest.csv",
  5361|             MATRIX_MANIFEST_FIELDS,
  5362|             matrix_manifest_rows,
  5363|         )
  5364|         print(f"[compare] wrote {len(matrix_manifest_rows)} rows → {out_dir / 'matrix_output_manifest.csv'}")
  5365| 
  5366|     if not summary_rows and not pooled_rows and not governance_state_rows and not union_inventory_rows:
  5367|         print("[compare] no comparison rows produced — check segment data and min-patterns threshold")
  5368| 
  5369|     comparison_registry_rows = build_comparison_registry_rows(
  5370|         completed_work_items, registry, executed_utc
  5371|     )
  5372|     atomic_write_csv(out_dir / "comparison_registry.csv", COMPARISON_REGISTRY_FIELDS, comparison_registry_rows)
  5373|     print(f"[compare] wrote {len(comparison_registry_rows)} rows → {out_dir / 'comparison_registry.csv'}")
  5374| 
  5375|     # Publish provenance last: a failed comparison must never advertise a new
  5376|     # identity policy beside an incomplete/previous evidence set.
  5377|     write_enterprise_policy_provenance(out_dir, policy)
  5378| 
  5379|     return 0
```
