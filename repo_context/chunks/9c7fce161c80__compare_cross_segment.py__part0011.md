# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 11 of 13
- Original line range: 4748-5147
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: main
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: main

```
  4748| def main() -> int:
  4749|     ap = argparse.ArgumentParser(
  4750|         description="Cross-segment comparison — computes join_hash overlap metrics\n"
  4751|                     "across segment pairs discovered from the manifest hierarchy.",
  4752|         formatter_class=argparse.RawTextHelpFormatter,
  4753|     )
  4754|     ap.add_argument("--segments-root", required=True, metavar="DIR",
  4755|                     help="Base directory for resolving segment output_folder paths from run_registry.csv")
  4756|     ap.add_argument("--records-dir", required=True, metavar="DIR",
  4757|                     help="Directory containing segment_manifest.csv, run_registry.csv, and file_metadata.csv")
  4758|     ap.add_argument("--out-dir", required=True, metavar="DIR",
  4759|                     help="Output directory for cross_segment_summary.csv, cross_segment_file_pairs.csv, and cross_segment_pooled.csv")
  4760| 
  4761|     # Mode flags
  4762|     ap.add_argument("--within-segment", action="store_true",
  4763|                     help="Mode A: pairs child Template/Project/Container within same parent")
  4764|     ap.add_argument("--sibling-segments", action="store_true",
  4765|                     help="Mode B: sibling segments sharing same parent and same governance_role")
  4766|     ap.add_argument("--parent-siblings", action="store_true",
  4767|                     help="Mode C: level-2 segments with different governance_role under same level-1 parent")
  4768|     ap.add_argument("--within-project", action="store_true",
  4769|                     help="Mode D: file pairs within same project_label within a single segment")
  4770|     ap.add_argument("--governance-chain", action="store_true",
  4771|                     help="Mode E: directed governance pairs scoped by client_label and discipline_label")
  4772|     ap.add_argument("--cross-client", action="store_true",
  4773|                     help="Mode F: each client's client-level pooled Project vocabulary vs. every other client's, same unit_system")
  4774| 
  4775|     # Filters
  4776|     ap.add_argument("--domain", metavar="DOMAIN",
  4777|                     help="Restrict comparison to a single domain")
  4778|     ap.add_argument("--segment-a", metavar="SEGMENT_ID",
  4779|                     help="Restrict left side of pairs to this segment")
  4780|     ap.add_argument("--segment-b", metavar="SEGMENT_ID",
  4781|                     help="Restrict right side of pairs to this segment")
  4782|     ap.add_argument("--min-patterns", type=int, default=3, metavar="INT",
  4783|                     help="Skip domain/segment pairs with fewer than N join_hashes (default: 3)")
  4784|     ap.add_argument("--dry-run", action="store_true",
  4785|                     help="Print discovered pairs without computing; no output files written")
  4786|     ap.add_argument("--no-delta", action="store_true",
  4787|                     help="Skip delta pattern output (cross_segment_delta.csv); useful for large corpora")
  4788|     ap.add_argument("--workers", default="auto",
  4789|                     help="Max parallel pair×domain workers, or 'auto' to derive from "
  4790|                          "CPU count (default: auto)")
  4791|     ap.add_argument("--enterprise-policy", help="Deployment-local enterprise policy JSON")
  4792|     ap.add_argument("--enterprise-label", default=None, help="Enterprise label override (takes precedence over policy file)")
  4793| 
  4794|     args = ap.parse_args()
  4795|     policy = load_enterprise_policy(args.enterprise_policy, args.enterprise_label)
  4796|     args.workers = resolve_worker_count(args.workers)
  4797|     if args.workers < 1:
  4798|         sys.exit("[error] --workers must be >= 1")
  4799| 
  4800|     segments_root = Path(args.segments_root).resolve()
  4801|     records_dir = Path(args.records_dir).resolve()
  4802|     out_dir = Path(args.out_dir).resolve()
  4803| 
  4804|     # Default: all modes if none specified
  4805|     any_mode = any([
  4806|         args.within_segment, args.sibling_segments, args.parent_siblings,
  4807|         args.within_project, args.governance_chain, args.cross_client,
  4808|     ])
  4809|     if not any_mode:
  4810|         args.within_segment = args.sibling_segments = args.parent_siblings = True
  4811|         args.within_project = args.governance_chain = args.cross_client = True
  4812| 
  4813|     manifest = load_manifest(records_dir)
  4814|     registry = load_registry(records_dir)
  4815|     file_metadata = load_file_metadata(records_dir)
  4816|     membership_path_exists = (records_dir / "segment_membership.csv").exists()
  4817|     membership = load_membership(records_dir)
  4818| 
  4819|     stale_ancestor_warnings = detect_stale_ancestor_encoding(manifest)
  4820|     if stale_ancestor_warnings:
  4821|         print(
  4822|             f"[warn] {len(stale_ancestor_warnings)} segment(s) look like they carry "
  4823|             f"pre-D-028 ancestor_segment_ids data -- structural_ancestor lineage "
  4824|             f"exclusion may be incomplete until segment_manifest.csv is regenerated:",
  4825|             file=sys.stderr,
  4826|         )
  4827|         for w in stale_ancestor_warnings[:20]:
  4828|             print(f"[warn]   {w}", file=sys.stderr)
  4829|     # Validated on file EXISTENCE, not on `membership` being non-empty --
  4830|     # segment_membership.csv present but header-only/all-invalid-rows (e.g.
  4831|     # a write interrupted right after the header line) loads as an empty
  4832|     # dict indistinguishable from "file absent" otherwise, which would skip
  4833|     # this check entirely and silently disable population_containment with
  4834|     # no warning at all (Codex review finding on PR #423). validate_
  4835|     # membership_against_manifest()'s second pass (every non-zero-file_count
  4836|     # manifest segment must appear in membership) naturally catches this
  4837|     # case once actually invoked -- an empty membership dict fails that
  4838|     # check for every eligible segment.
  4839|     if membership_path_exists:
  4840|         membership_errors = validate_membership_against_manifest(manifest, membership)
  4841|         if membership_errors:
  4842|             print(
  4843|                 f"[warn] segment_membership.csv disagrees with segment_manifest.csv for "
  4844|                 f"{len(membership_errors)} segment(s) -- population_containment disabled "
  4845|                 f"for this run (structural_ancestor guard still applies). Re-run "
  4846|                 f"build_segment_manifest.py to regenerate a consistent set:",
  4847|                 file=sys.stderr,
  4848|             )
  4849|             for err in membership_errors:
  4850|                 print(f"[warn]   {err}", file=sys.stderr)
  4851|             membership = {}
  4852| 
  4853|     # structural_ancestor / population_containment (D-027): computed once up
  4854|     # front and threaded into discover_sibling_segments() below. ancestor_map
  4855|     # is cheap and always available (derived purely from manifest); the
  4856|     # containment_map additionally needs real population data (membership) --
  4857|     # when segment_membership.csv is absent (or fails validation above),
  4858|     # containment_map stays None and discover_sibling_segments() falls back
  4859|     # to the structural guard alone.
  4860|     ancestor_map = _build_ancestor_map(manifest)
  4861|     containment_map: Optional[Dict[str, Set[str]]] = None
  4862|     if membership:
  4863|         containment_thresholds = _compute_containment_thresholds(manifest, membership, ancestor_map)
  4864|         containment_map = _population_containment_map(manifest, membership, containment_thresholds)
  4865|         if not args.dry_run:
  4866|             thresholds_path = write_population_containment_thresholds(out_dir, containment_thresholds)
  4867|             print(f"[compare] population_containment thresholds written to {thresholds_path}")
  4868|     elif not args.dry_run:
  4869|         # A prior run in this same --out-dir may have written population_
  4870|         # containment_thresholds.csv when containment was available; this
  4871|         # run has it disabled (no segment_membership.csv, or it failed
  4872|         # validation above). Leaving that stale file in place would make
  4873|         # this run's output directory misrepresent a THIS-run artifact as
  4874|         # still describing THIS run's data, when population_containment
  4875|         # wasn't actually computed here at all (Codex review finding on
  4876|         # PR #423).
  4877|         stale_thresholds_path = out_dir / "population_containment_thresholds.csv"
  4878|         if stale_thresholds_path.exists():
  4879|             stale_thresholds_path.unlink()
  4880|             print(
  4881|                 f"[warn] removed stale {stale_thresholds_path} from a prior run -- "
  4882|                 f"population_containment is disabled for this run",
  4883|                 file=sys.stderr,
  4884|             )
  4885| 
  4886|     # Discover pairs
  4887|     pairs: List[ComparisonPair] = []
  4888|     if args.within_segment:
  4889|         pairs.extend(discover_within_segment(manifest))
  4890|     if args.sibling_segments:
  4891|         pairs.extend(discover_sibling_segments(policy, manifest, ancestor_map, containment_map))
  4892|     if args.parent_siblings:
  4893|         pairs.extend(discover_parent_siblings(manifest))
  4894|     if args.governance_chain:
  4895|         pairs.extend(discover_governance_chain(policy, manifest))
  4896|     if args.within_project:
  4897|         pairs.extend(discover_within_project(manifest, registry, file_metadata, segments_root))
  4898|     if args.cross_client:
  4899|         pairs.extend(discover_cross_client(policy, manifest))
  4900|         pairs.extend(discover_client_cross_bc(policy, manifest))
  4901| 
  4902|     pairs = deduplicate_pairs(pairs)
  4903| 
  4904|     # Filter by --segment-a / --segment-b. Must run BEFORE
  4905|     # drop_legacy_siblings_covered_by_peer_comparisons(): that drop is
  4906|     # order-independent (frozenset((a, b))), but discover_sibling_segments()
  4907|     # orders its pairs by sorted segment ID while discover_cross_client() orders
  4908|     # by sorted client label (bc_to_bc/discover_client_cross_bc() both order by
  4909|     # sorted segment ID too, matching sibling's own convention) -- the surviving
  4910|     # cross_client row can therefore be the reverse (b, a) of the sibling_projects
  4911|     # row it replaces. Since these filters are position-sensitive
  4912|     # (a == args.segment_a, b == args.segment_b), running the drop first could
  4913|     # remove the correctly-oriented sibling row and leave only a
  4914|     # reversed-orientation peer row that then fails the filter too, making a
  4915|     # scoped run silently report zero pairs for segments that do have a
  4916|     # comparison. Filtering here first means the drop only ever sees (and only
  4917|     # ever needs to reconcile) whichever orientation actually survived the
  4918|     # requested scope.
  4919|     if args.segment_a:
  4920|         pairs = [(a, b, ct) for a, b, ct in pairs if a == args.segment_a]
  4921|     if args.segment_b:
  4922|         pairs = [(a, b, ct) for a, b, ct in pairs if b == args.segment_b]
  4923| 
  4924|     pairs = drop_legacy_siblings_covered_by_peer_comparisons(pairs)
  4925| 
  4926|     if not pairs:
  4927|         print("[compare] no pairs discovered — check manifest hierarchy and mode flags")
  4928| 
  4929|     runnable_pairs = [
  4930|         (seg_a, seg_b, ctype)
  4931|         for seg_a, seg_b, ctype in pairs
  4932|         if segment_is_runnable(registry, seg_a)
  4933|         and (seg_a == seg_b or segment_is_runnable(registry, seg_b))
  4934|     ]
  4935| 
  4936|     # Build flat work list: one item per (pair × domain), limited to domains
  4937|     # present in either side of the pair so sparse corpora do not generate a
  4938|     # global-domain cross product of mostly-empty worker tasks. Computed
  4939|     # before the --dry-run branch so the preview reflects exactly the
  4940|     # (pair, domain) granularity a live run would recompute — including a
  4941|     # --domain filter, which only ever touches one domain per pair.
  4942|     work_items, _domains_by_segment, active_domain_filter = build_pair_domain_work_items(
  4943|         runnable_pairs, segments_root, registry, args.domain
  4944|     )
  4945| 
  4946|     # --dry-run: print table and exit
  4947|     if args.dry_run:
  4948|         comparison_registry = load_comparison_registry(out_dir)
  4949|         col_w = 36
  4950|         print(f"{'segment_a':<{col_w}}  {'segment_b':<{col_w}}  {'comparison_type':<28}  {'domain':<24}  {'staleness':<10}")
  4951|         print("-" * (col_w * 2 + 68))
  4952|         n_stale = 0
  4953|         for a, b, ctype, dom in work_items:
  4954|             la = manifest.get(a, {}).get("segment_label", a)
  4955|             lb = manifest.get(b, {}).get("segment_label", b)
  4956|             stale = comparison_is_stale(a, b, ctype, dom, registry, comparison_registry)
  4957|             if stale:
  4958|                 n_stale += 1
  4959|             staleness_label = "stale" if stale else "current"
  4960|             print(f"{la:<{col_w}}  {lb:<{col_w}}  {ctype:<28}  {dom:<24}  {staleness_label:<10}")
  4961|         print(
  4962|             f"\n[compare] {len(runnable_pairs)} pairs, {len(work_items)} pair-domain work items "
  4963|             f"({n_stale} stale, {len(work_items) - n_stale} current)"
  4964|         )
  4965|         return 0
  4966| 
  4967|     # Run comparisons
  4968|     executed_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
  4969|     summary_rows: List[Dict[str, str]] = []
  4970|     delta_rows: List[Dict[str, str]] = []
  4971|     governance_state_rows: List[Dict[str, str]] = []
  4972|     governance_state_summary_rows: List[Dict[str, str]] = []
  4973|     governance_combo_count = 0
  4974|     delta_combo_count = 0
  4975| 
  4976|     print(
  4977|         f"[compare] {len(runnable_pairs)} pairs × {len(active_domain_filter)} active domains = "
  4978|         f"{len(work_items)} pair-domain work items  workers={args.workers}"
  4979|     )
  4980| 
  4981|     n_complete = 0
  4982|     n_skipped = 0
  4983|     completed_work_items: List[Tuple[str, str, str, str]] = []
  4984| 
  4985|     # cross_segment_file_pairs.csv rows are streamed to a temp file as work items
  4986|     # complete rather than accumulated in memory — one row per matched file pair
  4987|     # within a domain comparison, easily millions of rows for a large corpus, which
  4988|     # was the dominant driver of multi-GB peak memory when held in a single Python
  4989|     # list for the whole run. Streamed rows land in worker-completion order rather
  4990|     # than the fully sorted order sort_pair_detail_rows() used to produce; nothing
  4991|     # downstream depends on that ordering. The temp file is only published (atomic
  4992|     # rename) later, in the "Write outputs" section below, alongside the other
  4993|     # outputs — not here — so a failure in the pooled/union/reuse/matrix steps
  4994|     # between here and there leaves the previous run's file untouched rather than
  4995|     # publishing a new pairs file paired with stale companion outputs.
  4996|     out_dir.mkdir(parents=True, exist_ok=True)
  4997|     pair_detail_tmp = NamedTemporaryFile(
  4998|         "w", encoding="utf-8", newline="", delete=False,
  4999|         dir=str(out_dir), suffix=".tmp",
  5000|     )
  5001|     pair_detail_writer = csv.DictWriter(pair_detail_tmp, fieldnames=PAIRS_FIELDS)
  5002|     pair_detail_writer.writeheader()
  5003|     pair_detail_row_count = 0
  5004|     pair_detail_tmp_path = Path(pair_detail_tmp.name)
  5005| 
  5006|     t0 = time.perf_counter()
  5007|     with ProcessPoolExecutor(max_workers=args.workers) as executor:
  5008|         future_to_item = {
  5009|             executor.submit(
  5010|                 _run_pair_domain,
  5011|                 policy, seg_a, seg_b, ctype, dom,
  5012|                 manifest, registry, file_metadata,
  5013|                 segments_root, args.min_patterns,
  5014|                 executed_utc, args.no_delta,
  5015|             ): (seg_a, seg_b, ctype, dom)
  5016|             for seg_a, seg_b, ctype, dom in work_items
  5017|         }
  5018|         for future in as_completed(future_to_item):
  5019|             seg_a, seg_b, ctype, domain = future_to_item[future]
  5020|             try:
  5021|                 result, pairs_out = future.result()
  5022|             except Exception as exc:
  5023|                 for pending in future_to_item:
  5024|                     if pending is not future:
  5025|                         pending.cancel()
  5026|                 raise RuntimeError(
  5027|                     f"pair=({seg_a}, {seg_b}) type={ctype} domain={domain} failed"
  5028|                 ) from exc
  5029| 
  5030|             if result is not None:
  5031|                 summary_rows.append(result)
  5032|                 for pair_row in pairs_out:
  5033|                     pair_row["_comparison_type"] = ctype
  5034|                     pair_detail_writer.writerow(
  5035|                         {name: pair_row.get(name, "") for name in PAIRS_FIELDS}
  5036|                     )
  5037|                 pair_detail_row_count += len(pairs_out)
  5038|                 n_complete += 1
  5039|                 n_p = result.get("n_pairs", "?")
  5040|                 print(
  5041|                     f"[compare] segment_a={seg_a} segment_b={seg_b} "
  5042|                     f"domain={domain} pairs={n_p}"
  5043|                 )
  5044| 
  5045|                 # Delta pattern output — directed pairs only, opt-out via --no-delta.
  5046|                 # Delta generation remains in the parent process so worker results stay
  5047|                 # limited to the existing (summary_row, detail_rows) contract.
  5048|                 #
  5049|                 # comparison_status == "blocked" (zero readable files on the
  5050|                 # reference side, or the target side) must be excluded here:
  5051|                 # an empty ref_union would make every target join_hash look
  5052|                 # like it's outside the reference, i.e. tgt_union - ref_union
  5053|                 # == tgt_union -- every target pattern gets misreported as
  5054|                 # locally-invented drift instead of "reference unknown, not
  5055|                 # locally drifted." A blocked row still exists in
  5056|                 # cross_segment_summary.csv (so the block itself is visible),
  5057|                 # it just can't source a trustworthy delta.
  5058|                 if (
  5059|                     not args.no_delta
  5060|                     and ctype in DELTA_DIRECTED_TYPES
  5061|                     and result.get("comparison_status") != "blocked"
  5062|                 ):
  5063|                     tgt_files = load_file_join_hashes(segments_root, registry, seg_b, domain)
  5064|                     tgt_files_used = load_file_join_hashes(
  5065|                         segments_root, registry, seg_b, domain, "used"
  5066|                     )
  5067|                     ref_files = load_file_join_hashes(segments_root, registry, seg_a, domain)
  5068|                     ref_union: Set[str] = set()
  5069|                     for jhs in ref_files.values():
  5070|                         ref_union |= jhs
  5071|                     tgt_union: Set[str] = set()
  5072|                     for jhs in tgt_files.values():
  5073|                         tgt_union |= jhs
  5074|                     delta_jhs = tgt_union - ref_union
  5075| 
  5076|                     if delta_jhs:
  5077|                         unit_system = manifest.get(seg_a, {}).get("unit_system", "")
  5078|                         container_set = get_role_jh_set(
  5079|                             "container", domain, unit_system, manifest, registry, segments_root,
  5080|                             exclude_segment_id=seg_b,
  5081|                         )
  5082|                         template_set = get_role_jh_set(
  5083|                             "template", domain, unit_system, manifest, registry, segments_root
  5084|                         )
  5085|                         pattern_labels = load_pattern_labels(
  5086|                             segments_root, registry, seg_b, domain
  5087|                         )
  5088|                         bnd_tgt_all = load_bundle_join_hash_set(
  5089|                             segments_root, registry, seg_b, domain, "all"
  5090|                         )
  5091|                         bnd_tgt_used = load_bundle_join_hash_set(
  5092|                             segments_root, registry, seg_b, domain, "used"
  5093|                         )
  5094|                         n_tgt_files = len(tgt_files)
  5095|                         crid = result.get("comparison_run_id", "")
  5096|                         ma = manifest.get(seg_a, {})
  5097|                         mb = manifest.get(seg_b, {})
  5098| 
  5099|                         for jh in delta_jhs:
  5100|                             n_files_in_tgt = sum(1 for jhs in tgt_files.values() if jh in jhs)
  5101|                             pct = n_files_in_tgt / n_tgt_files if n_tgt_files else 0.0
  5102|                             used_n_files_in_tgt = sum(
  5103|                                 1 for jhs in tgt_files_used.values() if jh in jhs
  5104|                             )
  5105|                             used_pct = used_n_files_in_tgt / n_tgt_files if n_tgt_files else 0.0
  5106|                             in_container = jh in container_set
  5107|                             in_template = jh in template_set
  5108|                             is_bnd_all = jh in bnd_tgt_all
  5109|                             is_bnd_used = jh in bnd_tgt_used
  5110|                             delta_rows.append({
  5111|                                 "comparison_run_id": crid,
  5112|                                 "segment_id_reference": seg_a,
  5113|                                 "segment_id_target": seg_b,
  5114|                                 "segment_label_reference": ma.get("segment_label", ""),
  5115|                                 "segment_label_target": mb.get("segment_label", ""),
  5116|                                 "comparison_type": ctype,
  5117|                                 "domain": domain,
  5118|                                 "join_hash": jh,
  5119|                                 "pattern_label": pattern_labels.get(jh, ""),
  5120|                                 "n_files_in_target": str(n_files_in_tgt),
  5121|                                 "pct_files_in_target": _fmt(pct),
  5122|                                 "in_any_container": "true" if in_container else "false",
  5123|                                 "in_any_template": "true" if in_template else "false",
  5124|                                 "used_pct_files_in_target": _fmt(used_pct),
  5125|                                 "is_bundle_member_all": "true" if is_bnd_all else "false",
  5126|                                 "is_bundle_member_used": "true" if is_bnd_used else "false",
  5127|                                 "delta_class": _classify_delta(
  5128|                                     in_container, in_template, is_bnd_all, is_bnd_used
  5129|                                 ),
  5130|                                 "executed_utc": executed_utc,
  5131|                             })
  5132|                         delta_combo_count += 1
  5133|             else:
  5134|                 n_skipped += 1
  5135| 
  5136|             produced_output = result is not None
  5137| 
  5138|             # Governance-state output is independent of legacy run_pair() summary
  5139|             # thresholds. Sparse or empty targets still need provided_but_missing
  5140|             # rows so missing downstream stock is visible.
  5141|             if ctype in GOVERNANCE_STATE_DIRECTED_TYPES:
  5142|                 crid = (
  5143|                     result.get("comparison_run_id", "")
  5144|                     if result is not None
  5145|                     else make_comparison_run_id(seg_a, seg_b, executed_utc, ctype)
  5146|                 )
  5147|                 state_rows, state_summary = build_governance_state_outputs(
```
