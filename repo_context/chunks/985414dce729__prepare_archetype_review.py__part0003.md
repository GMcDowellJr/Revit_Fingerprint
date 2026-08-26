# Chunk of tools/archetype/prepare_archetype_review.py

- Source relative path: `tools/archetype/prepare_archetype_review.py`
- Chunk: 3 of 3
- Original line range: 901-1048
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 03bdf22e06a40e3b31dd69dea0931eb1695547b7a49d5dbbd3ada414575c6244
- Starts inside symbol: no
- Ends inside symbol: no

```
   901| def main() -> int:
   902|     ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
   903|     ap.add_argument("--repo-root", default=".", help="Repository root (code/config root; durable tool/config defaults live here)")
   904|     ap.add_argument("--assigned-root", default=None, help="Assigned/export root containing archetype_analysis/ and results/; omitted preserves legacy repo-local defaults")
   905|     ap.add_argument("--cluster-id", default=None, help="Target cluster_id from signal_clusters.json; if omitted, process all clusters")
   906|     ap.add_argument("--signal-clusters", default=None, help="Path to signal_clusters.json")
   907|     ap.add_argument("--cluster-classifications", default=None, help="Path to archetype_cluster_classifications.csv")
   908|     ap.add_argument("--archetype-classifications", default=None, help="Path to archetype_classifications.csv")
   909|     ap.add_argument("--validation-detail", default=None, help="Path to archetype_validation_detail.csv")
   910|     ap.add_argument("--records", default=None, help="Path to records.csv")
   911|     ap.add_argument("--file-metadata", default=None, help="Path to file_metadata.csv")
   912|     ap.add_argument("--identity-items-dir", default=None, help="Path to identity_items_by_domain/")
   913|     ap.add_argument("--bip-lookup", default=None, help="Path to bip_lookup.json")
   914|     ap.add_argument("--shared-param-names", default=None, help="Path to shared_param_names.json")
   915|     ap.add_argument("--vfd-category-map", default=None, help="Path to vfd_category_domain_map.json")
   916|     ap.add_argument("--out", default=None, help="Output directory; each cluster is written to <out>/review_<cluster_id>.csv")
   917|     ap.add_argument("--top-n", type=int, default=0, help="Limit to top N files per cluster; default 0 = all")
   918|     ap.add_argument("--dry-run", action="store_true")
   919|     ap.add_argument("--verbose", action="store_true", help="Print per-cluster verbose summaries and fallback diagnostics")
   920|     args = ap.parse_args()
   921| 
   922|     repo_root = Path(args.repo_root).resolve()
   923|     assigned_root = Path(args.assigned_root).resolve() if args.assigned_root else repo_root / "Fingerprint_Out"
   924|     analysis_dir = assigned_root / "archetype_analysis"
   925|     records_root = assigned_root / "results" if args.assigned_root else repo_root / "results"
   926|     log(STAGE, f"repo_root={repo_root}")
   927|     log(STAGE, f"assigned_root={assigned_root}")
   928| 
   929|     signal_clusters_path = Path(args.signal_clusters) if args.signal_clusters else analysis_dir / "signal_clusters.json"
   930|     cluster_classifications_path = Path(args.cluster_classifications) if args.cluster_classifications else analysis_dir / "archetype_cluster_classifications.csv"
   931|     archetype_classifications_path = Path(args.archetype_classifications) if args.archetype_classifications else analysis_dir / "archetype_classifications.csv"
   932|     validation_detail_path = Path(args.validation_detail) if args.validation_detail else analysis_dir / "archetype_validation_detail.csv"
   933|     records_path = Path(args.records) if args.records else records_root / "records" / "records.csv"
   934|     file_metadata_path = Path(args.file_metadata) if args.file_metadata else records_root / "records" / "file_metadata.csv"
   935|     identity_items_dir = Path(args.identity_items_dir) if args.identity_items_dir else records_root / "records" / "identity_items_by_domain"
   936|     bip_lookup_path = Path(args.bip_lookup) if args.bip_lookup else repo_root / "tools" / "archetype" / "bip_lookup.json"
   937|     shared_param_names_path = Path(args.shared_param_names) if args.shared_param_names else repo_root / "tools" / "archetype" / "shared_param_names.json"
   938|     vfd_category_map_path = Path(args.vfd_category_map) if args.vfd_category_map else repo_root / "tools" / "archetype" / "vfd_category_domain_map.json"
   939| 
   940|     out_dir = Path(args.out) if args.out else analysis_dir / "archetype_review"
   941|     if not out_dir.is_absolute():
   942|         out_dir = repo_root / out_dir
   943| 
   944|     # Stage 1: resolve target cluster(s).
   945|     if not signal_clusters_path.is_file():
   946|         log(STAGE, f"ERROR: signal_clusters.json not found at {signal_clusters_path}")
   947|         log(STAGE, "Run tools/archetype/cluster_archetype_signals.py first to generate it.")
   948|         return 1
   949| 
   950|     signal_clusters = read_json(signal_clusters_path, default={}) or {}
   951| 
   952|     if args.cluster_id:
   953|         cluster = _find_cluster(signal_clusters, args.cluster_id)
   954|         if cluster is None:
   955|             available = _all_cluster_ids(signal_clusters)
   956|             log(STAGE, f"ERROR: cluster_id={args.cluster_id!r} not found in {signal_clusters_path}")
   957|             log(STAGE, f"available cluster_ids ({len(available)}): {', '.join(available)}")
   958|             return 1
   959|         clusters = [cluster]
   960|         verbose = True
   961|     else:
   962|         clusters = _all_clusters(signal_clusters)
   963|         if not clusters:
   964|             log(STAGE, f"no clusters found in {signal_clusters_path}")
   965|             return 0
   966|         log(STAGE, f"--cluster-id not given; processing all {len(clusters)} clusters from {signal_clusters_path}")
   967|         verbose = args.verbose
   968| 
   969|     # Stage 2/3 inputs (shared across clusters).
   970|     cluster_classification_rows = read_csv_rows(cluster_classifications_path)
   971|     log(STAGE, f"loaded {len(cluster_classification_rows)} rows from {cluster_classifications_path}")
   972|     rows_by_cluster_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
   973|     for row in cluster_classification_rows:
   974|         cluster_id = row.get("cluster_id", "")
   975|         if cluster_id:
   976|             rows_by_cluster_id[cluster_id].append(row)
   977| 
   978|     validation_detail_rows = read_csv_rows(validation_detail_path)
   979|     log(STAGE, f"loaded {len(validation_detail_rows)} rows from {validation_detail_path}")
   980|     detail_rows_by_export: Dict[str, List[Dict[str, str]]] = defaultdict(list)
   981|     for row in validation_detail_rows:
   982|         export_run_id = row.get("export_run_id", "")
   983|         if export_run_id:
   984|             detail_rows_by_export[export_run_id].append(row)
   985| 
   986|     # archetype_id -> governance_question, for restricting Stage 3 detail
   987|     # rows to the cluster's own governance_question (see _build_cluster_context).
   988|     archetype_classification_rows = read_csv_rows(archetype_classifications_path)
   989|     log(STAGE, f"loaded {len(archetype_classification_rows)} rows from {archetype_classifications_path}")
   990|     curated_gq_map = _build_curated_gq_map(archetype_classification_rows)
   991|     archetype_ids_by_gq: Dict[str, Set[str]] = defaultdict(set)
   992|     for row in validation_detail_rows:
   993|         archetype_id = row.get("archetype_id", "")
   994|         if archetype_id:
   995|             archetype_ids_by_gq[_resolve_governance_question(archetype_id, curated_gq_map)].add(archetype_id)
   996| 
   997|     contexts: List[ClusterContext] = []
   998|     union_qualifying_files: Set[str] = set()
   999|     union_source_domains: Set[str] = set()
  1000|     for cluster in clusters:
  1001|         ctx = _build_cluster_context(cluster, rows_by_cluster_id, detail_rows_by_export, archetype_ids_by_gq)
  1002|         log(STAGE, f"cluster_id={ctx.cluster_id} cluster_label_stub={ctx.cluster_label_stub} n_signals={len(ctx.signal_ids)} qualifying_files={len(ctx.qualifying_files)} detail_rows={len(ctx.detail_by_file_signal)}")
  1003|         contexts.append(ctx)
  1004|         union_qualifying_files |= ctx.qualifying_files
  1005|         union_source_domains |= ctx.source_domains
  1006| 
  1007|     # Stage 4 (shared, single pass over records.csv).
  1008|     label_by_domain_sig, label_by_sig, label_by_record_pk, vfd_sig_to_record_pk = _load_label_lookup(records_path, union_qualifying_files, union_source_domains)
  1009| 
  1010|     # Stage 5 (shared).
  1011|     bip_lookup = read_json(bip_lookup_path, default={}) or {}
  1012|     shared_param_names = read_json(shared_param_names_path, default={}) or {}
  1013|     vfd_category_map = read_json(vfd_category_map_path, default={}) or {}
  1014|     log(STAGE, f"loaded bip_lookup ({len(bip_lookup)} entries) from {bip_lookup_path}")
  1015|     log(STAGE, f"loaded shared_param_names ({len(shared_param_names)} entries) from {shared_param_names_path}")
  1016|     vfd_resolution = _load_vfd_resolution(
  1017|         identity_items_dir, union_qualifying_files, union_source_domains,
  1018|         bip_lookup, shared_param_names, vfd_category_map,
  1019|     )
  1020| 
  1021|     # Stage 6 (shared).
  1022|     file_path_lookup = _load_file_path_lookup(file_metadata_path)
  1023| 
  1024|     # Stages 7/8 (per cluster).
  1025|     results: List[Dict[str, Any]] = []
  1026|     for ctx in contexts:
  1027|         result = _process_cluster(
  1028|             ctx, label_by_domain_sig, label_by_sig, label_by_record_pk, vfd_sig_to_record_pk, vfd_resolution, file_path_lookup,
  1029|             out_dir, args.top_n, args.dry_run, verbose=verbose,
  1030|         )
  1031|         results.append(result)
  1032| 
  1033|     _write_review_schedule_outputs(out_dir, results, args.dry_run)
  1034| 
  1035|     if not verbose:
  1036|         print(f"Processed {len(results)} clusters -> {out_dir}")
  1037|         for r in results:
  1038|             print(
  1039|                 f"  {r['cluster_id']:<50} total_files={r['total_files']:<5} "
  1040|                 f"all_fired={r['n_all_signals_fired']} ({r['pct_all']:.1f}%)  "
  1041|                 f"rows_written={r['n_rows']}"
  1042|             )
  1043| 
  1044|     return 0
  1045| 
  1046| 
  1047| if __name__ == "__main__":
  1048|     raise SystemExit(main())
```
