# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 11 of 17
- Original line range: 4392-4900
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: render_union_reuse_summary, _matrix_value_status_blocked, _manifest_bullets_for_matrix, _unordered_project_pairs, _render_portfolio_footprint_identity, _render_portfolio_density_similarity, _render_portfolio_density_similarity._shape_note, _render_portfolio_pool_containment, _render_portfolio_fragmentation, render_project_portfolio_section
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  4392| def render_union_reuse_summary(
  4393|     union_inventory_rows: list,
  4394|     reuse_distribution_rows: list,
  4395|     matrix_manifest_rows: list,
  4396|     reuse_by_client_rows: Optional[list] = None,
  4397| ) -> Optional[str]:
  4398|     reuse_by_client_rows = reuse_by_client_rows or []
  4399|     if (
  4400|         not union_inventory_rows and not reuse_distribution_rows
  4401|         and not matrix_manifest_rows and not reuse_by_client_rows
  4402|     ):
  4403|         return None
  4404| 
  4405|     lines = ["## Union Inventory Reuse Summary\n"]
  4406| 
  4407|     if reuse_distribution_rows:
  4408|         bucket_order = [
  4409|             "corpus_wide",
  4410|             "client_wide",
  4411|             "multi_project",
  4412|             "single_project",
  4413|             "emerging",
  4414|             "single_file",
  4415|             "unclassified",
  4416|         ]
  4417|         bucket_priority = {bucket: i for i, bucket in enumerate(bucket_order)}
  4418|         pattern_buckets = {}
  4419|         pattern_domains = {}
  4420|         domain_counts = defaultdict(lambda: {bucket: 0 for bucket in bucket_order})
  4421|         for row in reuse_distribution_rows:
  4422|             if row.get("classification_status") != "ok":
  4423|                 continue
  4424|             if row.get("inventory_status") != "ok":
  4425|                 continue
  4426|             domain = row.get("domain", "")
  4427|             join_hash = row.get("join_hash", "")
  4428|             if not join_hash:
  4429|                 continue
  4430|             bucket = row.get("reuse_bucket", "unclassified") or "unclassified"
  4431|             if bucket not in bucket_order:
  4432|                 bucket = "unclassified"
  4433|             pattern_key = (
  4434|                 row.get("view_scope", ""),
  4435|                 row.get("governance_role", ""),
  4436|                 row.get("discipline_label", ""),
  4437|                 row.get("unit_system", ""),
  4438|                 domain,
  4439|                 join_hash,
  4440|             )
  4441|             previous_bucket = pattern_buckets.get(pattern_key)
  4442|             if (
  4443|                 previous_bucket is None
  4444|                 or bucket_priority[bucket] < bucket_priority[previous_bucket]
  4445|             ):
  4446|                 pattern_buckets[pattern_key] = bucket
  4447|                 pattern_domains[pattern_key] = domain
  4448| 
  4449|         for pattern_key, bucket in pattern_buckets.items():
  4450|             domain_counts[pattern_domains[pattern_key]][bucket] += 1
  4451| 
  4452|         sorted_domains = sorted(
  4453|             domain_counts.items(),
  4454|             key=lambda item: (-item[1]["corpus_wide"], item[0]),
  4455|         )
  4456| 
  4457|         lines.append("**Reuse breadth summary**\n")
  4458|         lines.append("| domain | corpus_wide | client_wide | multi_project | single_project | emerging | single_file | unclassified |")
  4459|         lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
  4460|         for domain, counts in sorted_domains[:20]:
  4461|             lines.append(
  4462|                 f"| {domain} "
  4463|                 f"| {counts['corpus_wide']} "
  4464|                 f"| {counts['client_wide']} "
  4465|                 f"| {counts['multi_project']} "
  4466|                 f"| {counts['single_project']} "
  4467|                 f"| {counts['emerging']} "
  4468|                 f"| {counts['single_file']} "
  4469|                 f"| {counts['unclassified']} |"
  4470|             )
  4471|         if len(sorted_domains) > 20:
  4472|             lines.append(f"\nTable limited to 20 domains; {len(sorted_domains) - 20} domains not shown.")
  4473|         lines.append("")
  4474| 
  4475|     if reuse_by_client_rows:
  4476|         # Adoption-breadth signal: how many clients' patterns reach corpus-wide
  4477|         # reuse, per domain. Sourced from pattern_reuse_summary_by_client.csv's
  4478|         # own n_patterns under the "corpus_wide" bucket (bucket_basis
  4479|         # clients_in_corpus_domain) -- additive to, and independent of, the
  4480|         # distinct-pattern dedup table above (which never groups by client).
  4481|         # Do not touch that table's logic from here.
  4482|         #
  4483|         # A client needs at least _ADOPTION_BREADTH_MIN_PATTERNS corpus-wide
  4484|         # pattern-instances in a domain to count as "adopting" it here, not
  4485|         # just one -- a single near-universal pattern (a shipped-default line
  4486|         # style, text type, etc. present in nearly every template regardless
  4487|         # of any real governance decision) can trivially clear the producer's
  4488|         # per-pattern corpus_wide bucket threshold once, which made every
  4489|         # domain read as ~100%-breadth on real data and gave this table no
  4490|         # discriminating power. Raising the bar to >=2 requires genuine
  4491|         # multi-pattern convergence, not a single shared placeholder. This is
  4492|         # a narrative-side interpretation threshold only -- it does not
  4493|         # change reuse_bucket/bucket_basis classification, which stays the
  4494|         # producer's (compare_cross_segment.py's) call.
  4495|         _ADOPTION_BREADTH_MIN_PATTERNS = 2
  4496|         clients_seen_by_domain: dict = defaultdict(set)
  4497|         client_domain_corpus_wide_n: dict = defaultdict(int)
  4498|         corpus_wide_instances_by_domain: dict = defaultdict(int)
  4499|         for row in reuse_by_client_rows:
  4500|             if row.get("classification_status") != "ok":
  4501|                 continue
  4502|             client = row.get("client_label", "")
  4503|             domain = row.get("domain", "")
  4504|             if not client or not domain:
  4505|                 continue
  4506|             clients_seen_by_domain[domain].add(client)
  4507|             if row.get("reuse_bucket") == "corpus_wide":
  4508|                 n = int(row.get("n_patterns") or "0")
  4509|                 client_domain_corpus_wide_n[(domain, client)] += n
  4510|                 corpus_wide_instances_by_domain[domain] += n
  4511| 
  4512|         corpus_wide_clients_by_domain: dict = defaultdict(set)
  4513|         for (domain, client), n in client_domain_corpus_wide_n.items():
  4514|             if n >= _ADOPTION_BREADTH_MIN_PATTERNS:
  4515|                 corpus_wide_clients_by_domain[domain].add(client)
  4516| 
  4517|         adoption_domains = sorted(
  4518|             clients_seen_by_domain.keys(),
  4519|             key=lambda d: (-len(corpus_wide_clients_by_domain.get(d, set())), d),
  4520|         )
  4521|         if adoption_domains:
  4522|             lines.append("**Adoption breadth by domain (client reach)**\n")
  4523|             lines.append(
  4524|                 f"How many of a domain's clients have at least {_ADOPTION_BREADTH_MIN_PATTERNS} "
  4525|                 "corpus-wide-reused pattern-instances (n_patterns under the corpus_wide bucket, "
  4526|                 "basis clients_in_corpus_domain, summed per client) -- an additive breadth cut, "
  4527|                 "not a replacement for the distinct-pattern reuse table above. The pattern-"
  4528|                 "instances column is the domain's total corpus-wide n_patterns across all "
  4529|                 "clients regardless of this per-client threshold, so a domain reaching every "
  4530|                 "client on a small total (concentrated in one or two shared patterns) reads "
  4531|                 "differently from one reaching every client on a large total (broad "
  4532|                 "multi-pattern convergence).\n"
  4533|             )
  4534|             lines.append(
  4535|                 f"| domain | clients with >={_ADOPTION_BREADTH_MIN_PATTERNS} corpus-wide patterns "
  4536|                 "| clients seen | corpus-wide pattern-instances (all clients) |"
  4537|             )
  4538|             lines.append("|---|---:|---:|---:|")
  4539|             for domain in adoption_domains[:20]:
  4540|                 n_clients_cw = len(corpus_wide_clients_by_domain.get(domain, set()))
  4541|                 n_clients_seen = len(clients_seen_by_domain[domain])
  4542|                 lines.append(
  4543|                     f"| {domain} | {n_clients_cw} | {n_clients_seen} "
  4544|                     f"| {corpus_wide_instances_by_domain.get(domain, 0)} |"
  4545|                 )
  4546|             if len(adoption_domains) > 20:
  4547|                 lines.append(f"\nTable limited to 20 domains; {len(adoption_domains) - 20} domains not shown.")
  4548|             lines.append("")
  4549| 
  4550|     if matrix_manifest_rows:
  4551|         lines.append("**Matrix manifest metadata**\n")
  4552|         lines.append("Matrix availability is determined by each matrix CSV `value_status`; manifest rows are descriptive metadata.\n")
  4553|         for row in matrix_manifest_rows:
  4554|             interpretation = row.get("interpretation", "")
  4555|             if len(interpretation) > 120:
  4556|                 interpretation = interpretation[:120].rstrip()
  4557|             lines.append(
  4558|                 f"- {row.get('matrix_name', '')}: {row.get('metric', '')} ({interpretation})"
  4559|             )
  4560|         blocking_statuses = {
  4561|             "no_patterns",
  4562|             "missing_domain_patterns",
  4563|             "missing_membership_matrix",
  4564|             "used_view_unavailable",
  4565|         }
  4566|         blocked_domains = {
  4567|             row.get("domain", "")
  4568|             for row in union_inventory_rows
  4569|             if row.get("governance_role") == "Project"
  4570|             and row.get("inventory_status") in blocking_statuses
  4571|         }
  4572|         if blocked_domains:
  4573|             lines.append(f"- Project union inventory domains with blocking status: {len(blocked_domains)}")
  4574|     elif union_inventory_rows or reuse_distribution_rows:
  4575|         lines.append("Matrix manifest not provided; matrix availability unknown.")
  4576| 
  4577|     return "\n".join(lines)
  4578| 
  4579| 
  4580| def _matrix_value_status_blocked(status: str) -> bool:
  4581|     """Blocked/unavailable value_status values across the project matrix
  4582|     outputs (see MATRIX_OUTPUT_FIELDS / FRAGMENTATION_DIAGNOSTIC_FIELDS
  4583|     value_status in compare_cross_segment.py) -- "ok"/"diagnostic"/
  4584|     "synthetic_self_comparison" are the only non-blocked statuses emitted
  4585|     there.
  4586|     """
  4587|     return status not in ("ok", "diagnostic", "synthetic_self_comparison")
  4588| 
  4589| 
  4590| def _manifest_bullets_for_matrix(matrix_manifest_rows: list, matrix_name: str) -> list:
  4591|     """Mirror render_union_reuse_summary()'s matrix-manifest bullet rendering
  4592|     (matrix_name: metric (interpretation, truncated to 120 chars)), scoped to
  4593|     one matrix, for reuse across the Project Portfolio paragraphs below.
  4594|     """
  4595|     lines = []
  4596|     for row in matrix_manifest_rows:
  4597|         if row.get("matrix_name") != matrix_name:
  4598|             continue
  4599|         interpretation = row.get("interpretation", "")
  4600|         if len(interpretation) > 120:
  4601|             interpretation = interpretation[:120].rstrip()
  4602|         lines.append(f"- {row.get('matrix_name', '')}: {row.get('metric', '')} ({interpretation})")
  4603|     return lines
  4604| 
  4605| 
  4606| def _unordered_project_pairs(rows: list, *, view_scope: str, domain: str, metric: str) -> list:
  4607|     """Deduplicate a symmetric project x project matrix (compare_cross_segment.py's
  4608|     add_matrix loops emit both (a, b) and (b, a) rows) down to one row per
  4609|     unordered pair, excluding self-comparisons and non-ok cells.
  4610|     """
  4611|     seen: set = set()
  4612|     pairs = []
  4613|     for row in rows:
  4614|         if row.get("view_scope") != view_scope or row.get("domain") != domain:
  4615|             continue
  4616|         if row.get("metric") != metric or row.get("value_status") != "ok":
  4617|             continue
  4618|         if row.get("self_comparison") == "true":
  4619|             continue
  4620|         a, b = row.get("row_id", ""), row.get("column_id", "")
  4621|         if not a or not b or a == b:
  4622|             continue
  4623|         key = tuple(sorted((a, b)))
  4624|         if key in seen:
  4625|             continue
  4626|         seen.add(key)
  4627|         value = pf(row.get("value"))
  4628|         if value is None:
  4629|             continue
  4630|         pairs.append((key[0], key[1], value))
  4631|     return pairs
  4632| 
  4633| 
  4634| def _render_portfolio_footprint_identity(union_rows: list, matrix_manifest_rows: list, top_n: int = 5) -> list:
  4635|     lines = ["### Footprint identity\n"]
  4636|     if not union_rows:
  4637|         lines.append("`project_union_jaccard_matrix.csv` not provided; footprint-identity comparison unavailable this run.")
  4638|         return lines
  4639|     lines += _manifest_bullets_for_matrix(matrix_manifest_rows, "project_union_jaccard_matrix.csv")
  4640|     # Scoped to view_scope == "all" -- this paragraph only ever renders the
  4641|     # all-view ALL_DOMAINS pairs below, so an unavailable used-view cell (e.g.
  4642|     # a run where only all-view union was computed) must not inflate the
  4643|     # blocked count reported alongside the all-view table.
  4644|     blocked = sum(
  4645|         1 for r in union_rows
  4646|         if r.get("view_scope") == "all" and _matrix_value_status_blocked(r.get("value_status", ""))
  4647|     )
  4648|     if blocked:
  4649|         lines.append(f"- Blocked/unavailable footprint-identity cells (all-view): {blocked}")
  4650|     pairs = _unordered_project_pairs(union_rows, view_scope="all", domain="ALL_DOMAINS", metric="union_jaccard")
  4651|     if not pairs:
  4652|         lines.append("\nNo ok ALL_DOMAINS `union_jaccard` project pairs available (all-view).")
  4653|         return lines
  4654|     lines.append(
  4655|         f"\nSystem-level project footprint overlap (`union_jaccard`, ALL_DOMAINS, all-view); "
  4656|         f"{len(pairs)} project pairs compared.\n"
  4657|     )
  4658|     most = sorted(pairs, key=lambda p: (-p[2], p[0], p[1]))
  4659|     lines.append(f"**Most similar footprint ({min(top_n, len(most))} pairs)**")
  4660|     for a, b, v in most[:top_n]:
  4661|         lines.append(f"- {a} <-> {b}: {fmt(v)}")
  4662|     least = sorted(pairs, key=lambda p: (p[2], p[0], p[1]))
  4663|     lines.append(f"\n**Least similar footprint ({min(top_n, len(least))} pairs)**")
  4664|     for a, b, v in least[:top_n]:
  4665|         lines.append(f"- {a} <-> {b}: {fmt(v)}")
  4666|     return lines
  4667| 
  4668| 
  4669| def _render_portfolio_density_similarity(
  4670|     density_rows: list, union_rows: list, matrix_manifest_rows: list, top_n: int = 5
  4671| ) -> list:
  4672|     lines = ["### Density similarity\n"]
  4673|     if not density_rows:
  4674|         lines.append("`project_density_similarity_matrix.csv` not provided; density-similarity comparison unavailable this run.")
  4675|         return lines
  4676|     lines += _manifest_bullets_for_matrix(matrix_manifest_rows, "project_density_similarity_matrix.csv")
  4677|     # Scoped to view_scope == "all" -- see the matching comment in
  4678|     # _render_portfolio_footprint_identity().
  4679|     blocked = sum(
  4680|         1 for r in density_rows
  4681|         if r.get("view_scope") == "all" and _matrix_value_status_blocked(r.get("value_status", ""))
  4682|     )
  4683|     if blocked:
  4684|         lines.append(f"- Blocked/unavailable density-similarity cells (all-view): {blocked}")
  4685|     pairs = _unordered_project_pairs(density_rows, view_scope="all", domain="ALL_DOMAINS", metric="density_similarity")
  4686|     if not pairs:
  4687|         lines.append("\nNo ok ALL_DOMAINS `density_similarity` project pairs available (all-view).")
  4688|         return lines
  4689|     lines.append(
  4690|         "\n**Caveat: high density similarity with low footprint (`union_jaccard`) similarity "
  4691|         'means "same shape, different content" -- projects populate the same domains to a '
  4692|         "similar degree without holding the same canonical patterns. This is stated literally, "
  4693|         "not as a softened approximation.**\n"
  4694|     )
  4695|     union_index = {
  4696|         (a, b): v
  4697|         for a, b, v in (
  4698|             _unordered_project_pairs(union_rows, view_scope="all", domain="ALL_DOMAINS", metric="union_jaccard")
  4699|             if union_rows else []
  4700|         )
  4701|     }
  4702| 
  4703|     def _shape_note(a: str, b: str, density_v: float) -> str:
  4704|         uv = union_index.get((a, b))
  4705|         if uv is not None and density_v >= PORTFOLIO_SHAPE_DENSITY_MIN and uv < PORTFOLIO_SHAPE_UNION_JACCARD_MAX:
  4706|             return f" -- same shape, different content (union_jaccard={fmt(uv)})"
  4707|         return ""
  4708| 
  4709|     most = sorted(pairs, key=lambda p: (-p[2], p[0], p[1]))
  4710|     lines.append(f"**Most similar density ({min(top_n, len(most))} pairs)**")
  4711|     for a, b, v in most[:top_n]:
  4712|         lines.append(f"- {a} <-> {b}: {fmt(v)}{_shape_note(a, b, v)}")
  4713|     least = sorted(pairs, key=lambda p: (p[2], p[0], p[1]))
  4714|     lines.append(f"\n**Least similar density ({min(top_n, len(least))} pairs)**")
  4715|     for a, b, v in least[:top_n]:
  4716|         lines.append(f"- {a} <-> {b}: {fmt(v)}")
  4717|     if not union_rows:
  4718|         lines.append("\n`project_union_jaccard_matrix.csv` not provided; same-shape/different-content cross-check unavailable.")
  4719|     return lines
  4720| 
  4721| 
  4722| def _render_portfolio_pool_containment(pool_rows: list, matrix_manifest_rows: list, bottom_n: int = 5) -> list:
  4723|     lines = ["### Peer-pool containment\n"]
  4724|     if not pool_rows:
  4725|         lines.append("`project_pool_containment_similarity_matrix.csv` not provided; peer-pool containment unavailable this run.")
  4726|         return lines
  4727|     lines += _manifest_bullets_for_matrix(matrix_manifest_rows, "project_pool_containment_similarity_matrix.csv")
  4728|     # Scoped to view_scope == "all" -- this paragraph only ever aggregates the
  4729|     # all-view metric below (used-view rows exist in this file too, per
  4730|     # compare_cross_segment.py emitting one row per (all|used) x
  4731|     # (segment_id, domain, pool_scope)). An unavailable used-view cell (e.g. a
  4732|     # run where only all-view containment was computed) must not be counted
  4733|     # as a blocked all-view cell here -- see the matching comment in
  4734|     # _render_portfolio_footprint_identity().
  4735|     blocked = sum(
  4736|         1 for r in pool_rows
  4737|         if r.get("view_scope") == "all" and _matrix_value_status_blocked(r.get("value_status", ""))
  4738|     )
  4739|     if blocked:
  4740|         lines.append(f"- Blocked/unavailable peer-pool containment cells (all-view): {blocked}")
  4741| 
  4742|     # This matrix has no ALL_DOMAINS aggregate row in compare_cross_segment.py
  4743|     # (unlike the other three project matrices) -- rows are per
  4744|     # (focal_project, domain, pool_scope) only. Mean across a project's
  4745|     # available domains per (project, pool_scope) to get one outlier score
  4746|     # per project per peer-pool grain.
  4747|     sums: dict = defaultdict(float)
  4748|     counts: dict = defaultdict(int)
  4749|     for row in pool_rows:
  4750|         if row.get("view_scope") != "all" or row.get("value_status") != "ok":
  4751|             continue
  4752|         if row.get("metric") != "pool_containment_similarity":
  4753|             continue
  4754|         col = row.get("column_id", "")
  4755|         if not col.startswith("peer_pool:") or col.count(":") < 2:
  4756|             continue
  4757|         pool_scope = col.split(":", 2)[1]
  4758|         value = pf(row.get("value"))
  4759|         if value is None:
  4760|             continue
  4761|         key = (row.get("row_id", ""), pool_scope)
  4762|         sums[key] += value
  4763|         counts[key] += 1
  4764|     means = sorted(
  4765|         ((proj, scope, sums[(proj, scope)] / counts[(proj, scope)]) for proj, scope in counts),
  4766|         key=lambda t: (t[2], t[0], t[1]),
  4767|     )
  4768|     if not means:
  4769|         lines.append("\nNo ok all-view `pool_containment_similarity` rows available.")
  4770|         return lines
  4771|     lines.append(
  4772|         f"\nPer-project mean `pool_containment_similarity` across available domains (all-view), "
  4773|         f"by peer-pool grain ({len(means)} project/pool-grain combinations). Mean across domains "
  4774|         "because this matrix carries no ALL_DOMAINS aggregate row. Lowest scores below are the "
  4775|         "outliers -- projects whose systems least resemble their own peer pool.\n"
  4776|     )
  4777|     lines.append(f"**Lowest peer-pool containment ({min(bottom_n, len(means))})**")
  4778|     for proj, scope, v in means[:bottom_n]:
  4779|         lines.append(f"- {proj} (pool: {scope}): {fmt(v)}")
  4780|     return lines
  4781| 
  4782| 
  4783| def _render_portfolio_fragmentation(frag_rows: list, matrix_manifest_rows: list, top_n: int = 5) -> list:
  4784|     lines = ["### Fragmentation diagnostic\n"]
  4785|     if not frag_rows:
  4786|         lines.append("`project_fragmentation_diagnostic.csv` not provided; fragmentation diagnostic unavailable this run.")
  4787|         return lines
  4788|     lines += _manifest_bullets_for_matrix(matrix_manifest_rows, "project_fragmentation_diagnostic.csv")
  4789|     diagnostic_rows = [
  4790|         r for r in frag_rows if r.get("value_status") == "diagnostic" and r.get("view_scope") == "all"
  4791|     ]
  4792|     # Scoped to view_scope == "all" -- see the matching comment in
  4793|     # _render_portfolio_footprint_identity().
  4794|     unavailable = sum(
  4795|         1 for r in frag_rows
  4796|         if r.get("view_scope") == "all" and r.get("value_status") != "diagnostic"
  4797|     )
  4798|     if unavailable:
  4799|         lines.append(f"- Fragmentation-diagnostic cells not computable (all-view): {unavailable}")
  4800| 
  4801|     seen: set = set()
  4802|     pairs = []
  4803|     for r in diagnostic_rows:
  4804|         a, b = r.get("row_id", ""), r.get("column_id", "")
  4805|         if not a or not b or a == b:
  4806|             continue
  4807|         key = tuple(sorted((a, b)))
  4808|         if key in seen:
  4809|             continue
  4810|         seen.add(key)
  4811|         diag = pf(r.get("fragmentation_diagnostic"))
  4812|         if diag is None:
  4813|             continue
  4814|         pairs.append((key[0], key[1], diag, pf(r.get("footprint_similarity")), pf(r.get("exact_identity_overlap"))))
  4815|     if not pairs:
  4816|         lines.append("\nNo diagnostic ALL_DOMAINS fragmentation rows available (all-view).")
  4817|         return lines
  4818|     lines.append(
  4819|         "\n`fragmentation_diagnostic` = `footprint_similarity` (union_jaccard) minus "
  4820|         "`exact_identity_overlap` (mean file-pair jaccard -- this is where "
  4821|         "`project_mean_file_pair_jaccard_matrix.csv`'s signal is folded in, rather than "
  4822|         "rendering that matrix standalone; its other signal is already visible via the "
  4823|         "sibling_projects/cross_client rows elsewhere in this narrative). A large positive "
  4824|         "value means projects share broad structural footprint without matching exact "
  4825|         f"file-level identity. {len(pairs)} project pairs compared.\n"
  4826|     )
  4827|     most = sorted(pairs, key=lambda p: (-p[2], p[0], p[1]))
  4828|     lines.append(f"**Highest fragmentation divergence ({min(top_n, len(most))} pairs)**")
  4829|     for a, b, diag, foot, exact in most[:top_n]:
  4830|         lines.append(f"- {a} <-> {b}: {fmt(diag)} (footprint {fmt(foot)}, exact {fmt(exact)})")
  4831|     return lines
  4832| 
  4833| 
  4834| def render_project_portfolio_section(
  4835|     union_jaccard_rows: list,
  4836|     density_similarity_rows: list,
  4837|     pool_containment_rows: list,
  4838|     fragmentation_rows: list,
  4839|     matrix_manifest_rows: list,
  4840| ) -> Optional[str]:
  4841|     """Project x project portfolio-shape section.
  4842| 
  4843|     Deliberately outside assign_tier()/governance_domain_summary.csv -- project
  4844|     x project grain has no natural domain-tier slot. This matches the existing
  4845|     guardrail in docs/governance_generator_cross_compare_coverage.md ("Do not
  4846|     use matrix values to override domain governance tiers directly; they are
  4847|     project/portfolio diagnostics and should remain separate from domain-level
  4848|     cascade/state classifications"), not an oversight to fix later.
  4849| 
  4850|     Each paragraph degrades gracefully to a one-line not-provided note when its
  4851|     source file is absent; the whole section is omitted only when all four
  4852|     project/matrix inputs are absent.
  4853|     """
  4854|     if not union_jaccard_rows and not density_similarity_rows and not pool_containment_rows and not fragmentation_rows:
  4855|         return None
  4856| 
  4857|     lines = [
  4858|         "## Project Portfolio\n",
  4859|         "Project x project comparisons at portfolio grain. This section is intentionally "
  4860|         "kept separate from the domain governance tiers above and from "
  4861|         "`governance_domain_summary.csv` -- these matrices answer portfolio-shape questions, "
  4862|         "not domain-standard approval questions, and never override a domain tier.\n",
  4863|     ]
  4864|     lines += _render_portfolio_footprint_identity(union_jaccard_rows, matrix_manifest_rows)
  4865|     lines.append("")
  4866|     lines += _render_portfolio_density_similarity(density_similarity_rows, union_jaccard_rows, matrix_manifest_rows)
  4867|     lines.append("")
  4868|     lines += _render_portfolio_pool_containment(pool_containment_rows, matrix_manifest_rows)
  4869|     lines.append("")
  4870|     lines += _render_portfolio_fragmentation(fragmentation_rows, matrix_manifest_rows)
  4871|     return "\n".join(lines)
  4872| 
  4873| 
  4874| # ── Business Center Composition / Business Center Distribution ──────────────
  4875| # Relationship/topology layer, Deliverables 4-5. These render governance_bc_
  4876| # client_matrix.csv / governance_client_bc_matrix.csv (tools/governance_
  4877| # relationships.py) verbatim -- population COMPOSITION (project/file counts
  4878| # by client within a BC, and by BC within a client), computed once in that
  4879| # module. Neither function here recomputes percentage_of_bc/percentage_of_
  4880| # client; they only read and format the columns already on each row.
  4881| #
  4882| # This is deliberately a different question from two existing sections:
  4883| #   - render_bc_section() ("Business Center Analysis"): Template/Container
  4884| #     peer-alignment (bc_to_bc/enterprise_to_bc containment), not project
  4885| #     composition, and carries no client_label at all (see build_bc_summary()'s
  4886| #     own docstring: "No sector gating... does not apply to this summary at
  4887| #     all").
  4888| #   - render_project_portfolio_section()'s peer-pool containment paragraph:
  4889| #     BEHAVIORAL similarity (Jaccard/containment) at "project" grain -- but
  4890| #     that grain is a (client, discipline, unit_system) governance POPULATION
  4891| #     (see compare_cross_segment.py's _label_by_project_group(), keyed by
  4892| #     _matrix_group_id_from_values(role, client_label, discipline_label,
  4893| #     unit_system)), which can itself pool many physical projects together.
  4894| #     governance_relationships.csv's "project" grain is one physical project
  4895| #     (file_metadata.csv's project_label). These two "project" concepts are
  4896| #     NOT the same entity and are not row-for-row joinable -- a reader must
  4897| #     not assume a specific physical project's peer-pool outlier score maps
  4898| #     to this section's composition percentage for the same client. Stated
  4899| #     explicitly below rather than attempting a join that would silently
  4900| #     misrepresent two different grains as comparable.
```
