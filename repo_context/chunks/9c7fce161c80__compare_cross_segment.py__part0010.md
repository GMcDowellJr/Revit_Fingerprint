# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 10 of 13
- Original line range: 4257-4747
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: run_pooled_comparison, run_pooled_comparison._domains_for, run_pooled_comparison._emit_for_groups, _matrix_group_id_from_values, _matrix_group_id, _label_by_project_group, _jaccard_sets, _cosine_similarity, build_explicit_matrix_outputs, build_explicit_matrix_outputs.add_manifest, build_explicit_matrix_outputs.add_matrix, segment_is_runnable, build_pair_domain_work_items, sort_summary_rows, sort_pair_detail_rows, resolve_worker_count
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  4257| def run_pooled_comparison(
  4258|     policy: EnterprisePolicy,
  4259|     manifest: Dict[str, Dict[str, str]],
  4260|     registry: Dict[str, Dict[str, str]],
  4261|     segments_root: Path,
  4262|     min_patterns: int,
  4263|     executed_utc: str,
  4264|     domain_filter: Optional[str] = None,
  4265|     focal_segment_ids: Optional[Set[str]] = None,
  4266| ) -> List[Dict[str, str]]:
  4267|     """N-1 pooled comparison, across three independent pool grains.
  4268| 
  4269|     Each grain is a genuinely different pool with different membership, not
  4270|     a different view of the same pool (grid analogy: fix-row-vary-column vs.
  4271|     fix-column-vary-row):
  4272| 
  4273|       - parent_sibling: pool = sibling segments sharing the same
  4274|         (parent_segment_id, governance_role, unit_system) — the narrowest
  4275|         client+bc-together pool. This is the original/default pool grain.
  4276|       - bc: pool = segments sharing the same (business_center_label, role,
  4277|         unit_system), ignoring client_label — pools whichever clients happen
  4278|         to have work in that bc, to check bc-level consistency.
  4279|       - client: pool = segments sharing the same (client_label, role,
  4280|         unit_system), ignoring business_center_label — pools whichever bcs
  4281|         happen to have work for that client, to check client-level
  4282|         consistency.
  4283| 
  4284|     business_center_label is normalized via _bc_of() before bc-pool grouping
  4285|     (blank/NA spellings fold to blank; "0000"/"BC_0000" spelling variants
  4286|     canonicalize to the literal "0000" rather than folding to blank -- see
  4287|     _normalize_bc_label()).
  4288| 
  4289|     Emits one row per (segment_id, domain, pool_scope).
  4290|     """
  4291|     parent_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
  4292|     bc_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
  4293|     client_groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
  4294|     for sid, row in manifest.items():
  4295|         role = row.get("governance_role", "").strip().lower()
  4296|         us = row.get("unit_system", "").strip()
  4297|         rt = registry.get(sid, {}).get("run_type", "").strip().lower()
  4298|         if rt in ("skip", "registration"):
  4299|             continue
  4300|         if not role or not us:
  4301|             continue
  4302|         parent = row.get("parent_segment_id", "").strip()
  4303|         if parent:
  4304|             parent_groups[(parent, role, us)].append(sid)
  4305|         # pool_scope ("parent_sibling"|"bc"|"client") answers a different question
  4306|         # than scope_level ("enterprise"|"business_center"|"client_business_center"):
  4307|         # pool_scope is which axis this pool was GROUPED along, not where the
  4308|         # segment sits organizationally. They are not parallel/competing
  4309|         # classifications -- both are derived from the same normalized
  4310|         # business_center_label via _bc_of()/_normalize_bc_label(), so an
  4311|         # Enterprise segment (business_center_label == "0000") is never silently
  4312|         # excluded or mis-bucketed by either path. Do not attempt to collapse
  4313|         # pool_scope into scope_level; a segment's scope_level is fixed, but the
  4314|         # same segment can appear in a "bc" pool and a "client" pool depending on
  4315|         # which sibling group is being pooled against.
  4316|         bc = _bc_of(row)
  4317|         if bc:
  4318|             bc_groups[(bc, role, us)].append(sid)
  4319|         client = _client_of(row)
  4320|         if client:
  4321|             client_groups[(client, role, us)].append(sid)
  4322| 
  4323|     ancestor_map = _build_ancestor_map(manifest)
  4324| 
  4325|     rows: List[Dict[str, str]] = []
  4326| 
  4327|     # Memoized across every group/grain in this call -- the same segment_id
  4328|     # can appear as a member of several sibling groups (parent_sibling, bc,
  4329|     # client grains all draw from the same manifest), so without this a
  4330|     # large corpus would re-discover the same segment's domains repeatedly.
  4331|     domains_cache: Dict[str, Set[str]] = {}
  4332| 
  4333|     def _domains_for(sid: str) -> Set[str]:
  4334|         if sid not in domains_cache:
  4335|             domains_cache[sid] = discover_domains_for_segment(segments_root, registry, sid)
  4336|         return domains_cache[sid]
  4337| 
  4338|     def _emit_for_groups(
  4339|         groups: Dict[Tuple[str, str, str], List[str]], pool_scope: str
  4340|     ) -> None:
  4341|         sibling_groups = {k: v for k, v in groups.items() if len(v) >= 2}
  4342|         for key, members in sibling_groups.items():
  4343|             pool_key_str = "_".join(key)
  4344|             for focal_sid in members:
  4345|                 if focal_segment_ids is not None and focal_sid not in focal_segment_ids:
  4346|                     continue
  4347|                 # Exclude any member in the focal's own parent_segment_id
  4348|                 # lineage (ancestor or descendant) — a bc/client pool grain
  4349|                 # ignores parent_segment_id for grouping, so an ancestor
  4350|                 # roll-up and its own child can otherwise land in the same
  4351|                 # pool even though the roll-up's population already
  4352|                 # contains (some or all of) the child's own data.
  4353|                 pool_sids = [
  4354|                     s for s in members
  4355|                     if s != focal_sid and not _is_lineage_related(ancestor_map, focal_sid, s)
  4356|                 ]
  4357|                 if not pool_sids:
  4358|                     # Lineage filtering removed every candidate peer (e.g. a
  4359|                     # 2-member group where the other member is this focal's
  4360|                     # own ancestor/descendant) -- there is no pool to compare
  4361|                     # against, not an unreadable one. Emitting a
  4362|                     # comparison_status="blocked" row here would misrepresent
  4363|                     # "no eligible pool exists" as "the pool's inventory
  4364|                     # couldn't be read," inflating blocked counts with
  4365|                     # comparisons that were never eligible in the first
  4366|                     # place. Skip entirely, matching pre-blocked-row behavior
  4367|                     # for this case.
  4368|                     continue
  4369| 
  4370|                 # Union with the pool's own domains, not just the focal
  4371|                 # segment's -- otherwise a focal segment with zero inventory
  4372|                 # for a domain the pool has (n_files_focal=0, n_files_pool>0,
  4373|                 # the exact case _build_pooled_row()'s blocked-row path
  4374|                 # exists to report) never gets scheduled at all, since there
  4375|                 # would be no domain to iterate for it.
  4376|                 focal_domains = _domains_for(focal_sid)
  4377|                 for s in pool_sids:
  4378|                     focal_domains = focal_domains | _domains_for(s)
  4379|                 if domain_filter:
  4380|                     focal_domains = focal_domains & {domain_filter}
  4381| 
  4382|                 for domain in sorted(focal_domains):
  4383|                     pooled_row = _build_pooled_row(
  4384|                         policy, focal_sid, pool_sids, domain, manifest, registry,
  4385|                         segments_root, min_patterns, executed_utc,
  4386|                         pool_scope, pool_key_str,
  4387|                     )
  4388|                     if pooled_row is not None:
  4389|                         rows.append(pooled_row)
  4390| 
  4391|     _emit_for_groups(parent_groups, "parent_sibling")
  4392|     _emit_for_groups(bc_groups, "bc")
  4393|     _emit_for_groups(client_groups, "client")
  4394| 
  4395|     return rows
  4396| 
  4397| 
  4398| 
  4399| # ---------------------------------------------------------------------------
  4400| # Explicit matrix/reporting outputs
  4401| # ---------------------------------------------------------------------------
  4402| 
  4403| def _matrix_group_id_from_values(
  4404|     role: str,
  4405|     client: str,
  4406|     discipline: str,
  4407|     unit_system: str,
  4408| ) -> str:
  4409|     return "|".join([role, client, discipline, unit_system])
  4410| 
  4411| 
  4412| def _matrix_group_id(row: Dict[str, str]) -> str:
  4413|     return _matrix_group_id_from_values(
  4414|         row.get("governance_role", ""),
  4415|         row.get("client_label", ""),
  4416|         row.get("discipline_label", ""),
  4417|         row.get("unit_system", ""),
  4418|     )
  4419| 
  4420| 
  4421| def _label_by_project_group(summary_rows: List[Dict[str, str]]) -> Dict[str, str]:
  4422|     labels: Dict[str, Set[str]] = defaultdict(set)
  4423|     for row in summary_rows:
  4424|         for suffix in ("a", "b"):
  4425|             if _role_key(row.get(f"governance_role_{suffix}", "")) != "project":
  4426|                 continue
  4427|             group_id = _matrix_group_id_from_values(
  4428|                 row.get(f"governance_role_{suffix}", ""),
  4429|                 row.get(f"client_label_{suffix}", ""),
  4430|                 row.get(f"discipline_label_{suffix}", ""),
  4431|                 row.get("unit_system", ""),
  4432|             )
  4433|             label = row.get(f"segment_label_{suffix}", "").strip() or row.get(f"segment_id_{suffix}", "").strip()
  4434|             if group_id and label:
  4435|                 labels[group_id].add(label)
  4436|     return {group_id: next(iter(values)) for group_id, values in labels.items() if len(values) == 1}
  4437| 
  4438| 
  4439| def _jaccard_sets(a: Set[str], b: Set[str]) -> Optional[float]:
  4440|     union = a | b
  4441|     if not union:
  4442|         return None
  4443|     return len(a & b) / len(union)
  4444| 
  4445| 
  4446| def _cosine_similarity(a: Dict[str, float], b: Dict[str, float]) -> Optional[float]:
  4447|     keys = set(a) | set(b)
  4448|     if not keys:
  4449|         return None
  4450|     dot = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
  4451|     na = sum(a.get(k, 0.0) ** 2 for k in keys) ** 0.5
  4452|     nb = sum(b.get(k, 0.0) ** 2 for k in keys) ** 0.5
  4453|     if na == 0.0 or nb == 0.0:
  4454|         return None
  4455|     return dot / (na * nb)
  4456| 
  4457| 
  4458| def build_explicit_matrix_outputs(
  4459|     summary_rows: List[Dict[str, str]],
  4460|     pooled_rows: List[Dict[str, str]],
  4461|     union_inventory_rows: List[Dict[str, str]],
  4462|     executed_utc: str,
  4463| ) -> Tuple[Dict[str, List[Dict[str, str]]], List[Dict[str, str]], List[Dict[str, str]]]:
  4464|     """Build metric-specific matrix outputs with explicit semantics.
  4465| 
  4466|     Returns (matrix_rows_by_filename, fragmentation_rows, manifest_rows). Missing
  4467|     union inventory blocks union/density matrices by emitting unavailable-status
  4468|     rows in their named outputs rather than falling back to file-pair signals.
  4469|     """
  4470|     outputs: Dict[str, List[Dict[str, str]]] = defaultdict(list)
  4471|     manifests: List[Dict[str, str]] = []
  4472|     label_by_group = _label_by_project_group(summary_rows)
  4473| 
  4474|     def add_manifest(name: str, role: str, view: str, source: str, grain: str,
  4475|                      metric: str, identity: str, agg: str, interp: str, limits: str) -> None:
  4476|         manifests.append({
  4477|             "matrix_name": name, "governance_role": role, "view_scope": view,
  4478|             "source_file": source, "source_grain": grain, "metric": metric,
  4479|             "identity_unit": identity, "aggregation_method": agg,
  4480|             "interpretation": interp, "known_limitations": limits,
  4481|             "executed_utc": executed_utc,
  4482|         })
  4483| 
  4484|     def add_matrix(filename: str, row_id: str, col_id: str, view: str, domain: str,
  4485|                    metric: str, value: Optional[float], status: str, interp: str) -> None:
  4486|         outputs[filename].append({
  4487|             "matrix_name": filename, "row_id": row_id, "column_id": col_id,
  4488|             "view_scope": view, "domain": domain, "metric": metric,
  4489|             "value": _fmt(value) if isinstance(value, float) else "",
  4490|             "value_status": status, "self_comparison": _bool_str(row_id == col_id),
  4491|             "interpretation": interp, "executed_utc": executed_utc,
  4492|         })
  4493| 
  4494|     ok_union = [r for r in union_inventory_rows if r.get("inventory_status") == "ok" and r.get("join_hash", "").strip()]
  4495|     project_ok_union = [r for r in ok_union if _role_key(r.get("governance_role", "")) == "project"]
  4496|     if not union_inventory_rows or not ok_union or not project_ok_union:
  4497|         if not union_inventory_rows:
  4498|             status = "blocked_missing_union_inventory"
  4499|         elif not ok_union:
  4500|             status = "blocked_no_ok_union_inventory"
  4501|         else:
  4502|             status = "blocked_no_ok_project_union_inventory"
  4503|         for filename, metric in [
  4504|             ("project_union_jaccard_matrix.csv", "union_jaccard"),
  4505|             ("project_density_similarity_matrix.csv", "density_similarity"),
  4506|         ]:
  4507|             add_matrix(filename, "unavailable", "unavailable", "unavailable", "", metric, None, status,
  4508|                        "Union-derived matrix blocked because normalized project union inventory is unavailable.")
  4509|     else:
  4510|         by_group_view: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
  4511|         by_group_view_domain: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)
  4512|         for r in project_ok_union:
  4513|             raw_gid = _matrix_group_id(r)
  4514|             gid = label_by_group.get(raw_gid, raw_gid)
  4515|             view = r.get("view_scope", "")
  4516|             domain = r.get("domain", "")
  4517|             jh = r.get("join_hash", "")
  4518|             by_group_view[(gid, view)].add(jh)
  4519|             by_group_view_domain[(gid, view, domain)].add(jh)
  4520|         for view in sorted({v for _, v in by_group_view}):
  4521|             ids = sorted(g for g, v in by_group_view if v == view)
  4522|             for a in ids:
  4523|                 for b in ids:
  4524|                     value = 1.0 if a == b else _jaccard_sets(by_group_view[(a, view)], by_group_view[(b, view)])
  4525|                     add_matrix("project_union_jaccard_matrix.csv", a, b, view, "ALL_DOMAINS", "union_jaccard", value, "ok",
  4526|                                "Jaccard between normalized project-level join_hash unions; answers whether systems contain the same canonical patterns.")
  4527|         vectors: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(dict)
  4528|         for (gid, view, domain), jhs in by_group_view_domain.items():
  4529|             vectors[(gid, view)][domain] = float(len(jhs))
  4530|         for view in sorted({v for _, v in vectors}):
  4531|             ids = sorted(g for g, v in vectors if v == view)
  4532|             for a in ids:
  4533|                 for b in ids:
  4534|                     value = 1.0 if a == b else _cosine_similarity(vectors[(a, view)], vectors[(b, view)])
  4535|                     add_matrix("project_density_similarity_matrix.csv", a, b, view, "ALL_DOMAINS", "density_similarity", value, "ok",
  4536|                                "Cosine similarity of domain pattern-density vectors; absent domains are treated as zero occupancy by definition.")
  4537|     add_manifest("project_union_jaccard_matrix.csv", "Project", "all,used", "cross_segment_union_inventory.csv", "role/client/discipline/unit/domain/view/join_hash", "union_jaccard", "normalized join_hash", "Jaccard on system-level unions", "Do these project scopes contain/use the same canonical patterns?", "Requires PR 1 union inventory; not a file-to-file similarity score.")
  4538|     add_manifest("project_density_similarity_matrix.csv", "Project", "all,used", "cross_segment_union_inventory.csv", "role/client/discipline/unit/domain/view/join_hash", "density_similarity", "domain pattern count", "Cosine similarity over domain occupancy counts", "Are domains populated to similar degrees?", "Treats absent domains as zero occupancy; does not measure exact identity overlap.")
  4539| 
  4540|     # Existing file-pair mean Jaccard preserved under explicit name. Domain rows
  4541|     # remain available, and an explicit ALL_DOMAINS aggregate is added so
  4542|     # fragmentation diagnostics never collapse an arbitrary domain into an
  4543|     # all-domain union comparison.
  4544|     project_summary = [r for r in summary_rows if _role_key(r.get("governance_role_a", "")) == "project" and _role_key(r.get("governance_role_b", "")) == "project"]
  4545|     file_pair_values: Dict[Tuple[str, str, str], List[Tuple[str, float]]] = defaultdict(list)
  4546|     file_pair_ids_by_view: Dict[str, Set[str]] = defaultdict(set)
  4547|     file_pair_domains_by_id_view: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
  4548|     for r in sorted(project_summary, key=lambda x: (
  4549|         x.get("segment_label_a") or x.get("segment_id_a", ""),
  4550|         x.get("segment_label_b") or x.get("segment_id_b", ""),
  4551|         x.get("domain", ""),
  4552|     )):
  4553|         row_id = r.get("segment_label_a") or r.get("segment_id_a", "")
  4554|         col_id = r.get("segment_label_b") or r.get("segment_id_b", "")
  4555|         for view, col in [("all", "all_pairwise_jaccard_mean"), ("used", "used_pairwise_jaccard_mean")]:
  4556|             raw = r.get(col, "")
  4557|             value = float(raw) if raw else None
  4558|             status = "ok" if raw else "unavailable"
  4559|             add_matrix("project_mean_file_pair_jaccard_matrix.csv", row_id, col_id, view, r.get("domain", ""),
  4560|                        "mean_file_pair_jaccard", value, status,
  4561|                        "Mean of pairwise file Jaccard comparisons; answers whether individual files are typically similar across groups.")
  4562|             if row_id != col_id:
  4563|                 add_matrix("project_mean_file_pair_jaccard_matrix.csv", col_id, row_id, view, r.get("domain", ""),
  4564|                            "mean_file_pair_jaccard", value, status,
  4565|                            "Symmetric mean file-pair Jaccard cell mirrored from the observed unordered project pair.")
  4566|             if raw:
  4567|                 for a_id, b_id in [(row_id, col_id), (col_id, row_id)]:
  4568|                     file_pair_values[(a_id, b_id, view)].append((r.get("domain", ""), float(raw)))
  4569|                 file_pair_ids_by_view[view].update([row_id, col_id])
  4570|                 if r.get("domain", ""):
  4571|                     file_pair_domains_by_id_view[(row_id, view)].add(r.get("domain", ""))
  4572|                     file_pair_domains_by_id_view[(col_id, view)].add(r.get("domain", ""))
  4573|     for (row_id, col_id, view), values in sorted(file_pair_values.items()):
  4574|         if values:
  4575|             aggregate = sum(v for _domain, v in sorted(values)) / len(values)
  4576|             add_matrix("project_mean_file_pair_jaccard_matrix.csv", row_id, col_id, view, "ALL_DOMAINS",
  4577|                        "mean_file_pair_jaccard", aggregate, "ok",
  4578|                        "Mean of domain-level mean file-pair Jaccard values; aligned to all-domain union_jaccard for diagnostics.")
  4579|     existing_pair_keys = {
  4580|         (r["row_id"], r["column_id"], r["view_scope"], r["domain"])
  4581|         for r in outputs.get("project_mean_file_pair_jaccard_matrix.csv", [])
  4582|     }
  4583|     for view, ids in sorted(file_pair_ids_by_view.items()):
  4584|         for row_id in sorted(ids):
  4585|             observed_domains = file_pair_domains_by_id_view.get((row_id, view), set())
  4586|             for domain in sorted(observed_domains | ({"ALL_DOMAINS"} if observed_domains else set())):
  4587|                 key = (row_id, row_id, view, domain)
  4588|                 if key in existing_pair_keys:
  4589|                     continue
  4590|                 add_matrix("project_mean_file_pair_jaccard_matrix.csv", row_id, row_id, view, domain,
  4591|                            "mean_file_pair_jaccard", 1.0, "synthetic_self_comparison",
  4592|                            "Synthetic deterministic self-comparison cell for square matrix pivots; not an observed file-pair comparison.")
  4593|                 existing_pair_keys.add(key)
  4594|     add_manifest("project_mean_file_pair_jaccard_matrix.csv", "Project", "all,used", "cross_segment_summary.csv", "segment_pair/domain plus deterministic ALL_DOMAINS aggregate", "mean_file_pair_jaccard", "file join_hash set", "Mean of file-pair Jaccard values; ALL_DOMAINS is the mean across available domain means", "Are individual files typically similar across project groups?", "Not equivalent to union_jaccard; can diverge when file inventories are partitioned differently.")
  4595| 
  4596|     for r in pooled_rows:
  4597|         if _role_key(r.get("governance_role", "")) != "project":
  4598|             continue
  4599|         row_id = r.get("segment_label") or r.get("segment_id", "")
  4600|         # A project can now appear once per applicable pool_scope grain
  4601|         # (parent_sibling, bc, client — see run_pooled_comparison()). Fold
  4602|         # pool_scope into col_id so different grains for the same project
  4603|         # land on distinct matrix coordinates instead of colliding on
  4604|         # identical (row_id, col_id, view, domain) with different values.
  4605|         pool_scope = r.get("pool_scope", "") or "parent_sibling"
  4606|         col_id = f"peer_pool:{pool_scope}:{row_id}"
  4607|         for view, col in [("all", "all_containment_focal_in_pool"), ("used", "used_containment_focal_in_pool")]:
  4608|             raw = r.get(col, "")
  4609|             add_matrix("project_pool_containment_similarity_matrix.csv", row_id, col_id, view, r.get("domain", ""),
  4610|                        "pool_containment_similarity", float(raw) if raw else None, "ok" if raw else "unavailable",
  4611|                        "Focal-in-peer-pool containment; answers how much each system aligns with its peer pool.")
  4612|     add_manifest("project_pool_containment_similarity_matrix.csv", "Project", "all,used", "cross_segment_pooled.csv", "focal_segment/domain/peer_pool_scope", "pool_containment_similarity", "normalized join_hash", "Focal union contained in sibling pool union", "How much does each project system align with its peer pool?", "Peer pools derive only from existing manifest sibling grain; no new authority taxonomy is inferred. column_id encodes pool_scope (parent_sibling/bc/client) so a project's separate pool grains never share a matrix cell.")
  4613| 
  4614|     # Diagnostic: union footprint minus exact mean identity overlap, only when both inputs are available.
  4615|     union_index = {(r["row_id"], r["column_id"], r["view_scope"], r["domain"]): r for r in outputs.get("project_union_jaccard_matrix.csv", []) if r.get("value_status") == "ok" and r.get("domain") == "ALL_DOMAINS"}
  4616|     pair_index = {(r["row_id"], r["column_id"], r["view_scope"], r["domain"]): r for r in outputs.get("project_mean_file_pair_jaccard_matrix.csv", []) if r.get("value_status") == "ok" and r.get("domain") == "ALL_DOMAINS"}
  4617|     frag_rows: List[Dict[str, str]] = []
  4618|     for key in sorted(set(union_index) & set(pair_index)):
  4619|         u = float(union_index[key]["value"])
  4620|         p = float(pair_index[key]["value"])
  4621|         frag_rows.append({
  4622|             "matrix_name": "project_fragmentation_diagnostic.csv",
  4623|             "row_id": key[0], "column_id": key[1], "view_scope": key[2], "domain": key[3],
  4624|             "footprint_similarity": _fmt(u), "exact_identity_overlap": _fmt(p),
  4625|             "fragmentation_diagnostic": _fmt(u - p), "value_status": "diagnostic",
  4626|             "interpretation": "Diagnostic difference between union footprint similarity and mean exact file identity overlap; not a mathematically authoritative index.",
  4627|             "executed_utc": executed_utc,
  4628|         })
  4629|     if not frag_rows:
  4630|         frag_rows.append({
  4631|             "matrix_name": "project_fragmentation_diagnostic.csv", "row_id": "unavailable",
  4632|             "column_id": "unavailable", "view_scope": "unavailable", "domain": "ALL_DOMAINS",
  4633|             "footprint_similarity": "", "exact_identity_overlap": "", "fragmentation_diagnostic": "",
  4634|             "value_status": "unavailable_required_inputs", "interpretation": "Requires both union_jaccard and mean_file_pair_jaccard inputs.",
  4635|             "executed_utc": executed_utc,
  4636|         })
  4637|     add_manifest("project_fragmentation_diagnostic.csv", "Project", "all,used", "project_union_jaccard_matrix.csv + project_mean_file_pair_jaccard_matrix.csv", "matrix cell", "fragmentation_diagnostic", "normalized join_hash", "union_jaccard minus mean_file_pair_jaccard when both are available", "Highlights divergence between footprint overlap and exact per-file identity overlap.", "Diagnostic only; do not treat as an authoritative governance index.")
  4638| 
  4639|     for rows in outputs.values():
  4640|         rows.sort(key=lambda r: (r["matrix_name"], r["row_id"], r["column_id"], r["view_scope"], r["domain"], r["metric"]))
  4641|     manifests.sort(key=lambda r: (r["matrix_name"], r["governance_role"], r["view_scope"]))
  4642|     return dict(outputs), frag_rows, manifests
  4643| 
  4644| # ---------------------------------------------------------------------------
  4645| # Segment validation
  4646| # ---------------------------------------------------------------------------
  4647| 
  4648| def segment_is_runnable(
  4649|     registry: Dict[str, Dict[str, str]],
  4650|     segment_id: str,
  4651| ) -> bool:
  4652|     rec = registry.get(segment_id)
  4653|     if rec is None:
  4654|         return False
  4655|     rt = rec.get("run_type", "").strip().lower()
  4656|     if rt in ("skip", "registration"):
  4657|         print(
  4658|             f"[warn] segment={segment_id} has run_type={rt!r} — skipping",
  4659|             file=sys.stderr,
  4660|         )
  4661|     return True
  4662| 
  4663| 
  4664| 
  4665| def build_pair_domain_work_items(
  4666|     runnable_pairs: Sequence[ComparisonPair],
  4667|     segments_root: Path,
  4668|     registry: Dict[str, Dict[str, str]],
  4669|     requested_domain: Optional[str] = None,
  4670| ) -> Tuple[List[Tuple[str, str, str, str]], Dict[str, Set[str]], List[str]]:
  4671|     """Return runnable (pair × domain) work scoped to each pair's domain union.
  4672| 
  4673|     Domains are sparse in segmented corpora. Scheduling every pair against every
  4674|     globally active domain creates mostly-empty worker tasks and filesystem churn,
  4675|     so each pair is expanded only across domains present in either participating
  4676|     segment.
  4677|     """
  4678|     segment_ids = sorted({seg for pair in runnable_pairs for seg in (pair[0], pair[1])})
  4679|     domains_by_segment = {
  4680|         sid: discover_domains_for_segment(segments_root, registry, sid)
  4681|         for sid in segment_ids
  4682|     }
  4683| 
  4684|     active_domains: Set[str] = set()
  4685|     work_items: List[Tuple[str, str, str, str]] = []
  4686|     for seg_a, seg_b, ctype in runnable_pairs:
  4687|         pair_domains = domains_by_segment.get(seg_a, set()) | domains_by_segment.get(seg_b, set())
  4688|         if requested_domain:
  4689|             domains = [requested_domain] if requested_domain in pair_domains else []
  4690|         else:
  4691|             domains = sorted(pair_domains)
  4692|         active_domains.update(domains)
  4693|         for dom in domains:
  4694|             work_items.append((seg_a, seg_b, ctype, dom))
  4695| 
  4696|     return work_items, domains_by_segment, sorted(active_domains)
  4697| 
  4698| 
  4699| def sort_summary_rows(rows: List[Dict[str, str]]) -> None:
  4700|     rows.sort(key=lambda r: (
  4701|         r.get("comparison_type", ""),
  4702|         r.get("segment_id_a", ""),
  4703|         r.get("segment_id_b", ""),
  4704|         r.get("domain", ""),
  4705|     ))
  4706| 
  4707| 
  4708| def sort_pair_detail_rows(rows: List[Dict[str, str]]) -> None:
  4709|     rows.sort(key=lambda r: (
  4710|         r.get("_comparison_type", ""),
  4711|         r.get("segment_id_a", ""),
  4712|         r.get("segment_id_b", ""),
  4713|         r.get("domain", ""),
  4714|         r.get("project_label_a", ""),
  4715|         r.get("project_label_b", ""),
  4716|         r.get("export_run_id_a", ""),
  4717|         r.get("export_run_id_b", ""),
  4718|     ))
  4719| 
  4720| # ---------------------------------------------------------------------------
  4721| # CLI
  4722| # ---------------------------------------------------------------------------
  4723| 
  4724| # ProcessPoolExecutor raises ValueError when max_workers > 61 on Windows
  4725| # (WaitForMultipleObjects handle-count limit) — auto-detected counts must
  4726| # respect this cap there, or a default `--workers auto` run on a 64+-core
  4727| # Windows host fails outright.
  4728| _WIN32_MAX_WORKERS = 61
  4729| 
  4730| 
  4731| def resolve_worker_count(value: str, headroom: int = 2) -> int:
  4732|     """Resolve --workers, accepting either an int or the literal string 'auto'.
  4733| 
  4734|     'auto' derives a single-layer worker count from available logical cores
  4735|     minus headroom — this script's ProcessPoolExecutor is not nested inside
  4736|     another worker pool, so (unlike run_segment_orchestrator.py's bundle-stage
  4737|     subprocess) there is no second layer to coordinate against.
  4738|     """
  4739|     if str(value).strip().lower() == "auto":
  4740|         cpu_count = os.cpu_count()
  4741|         workers = max(1, cpu_count - headroom) if cpu_count else 4
  4742|         if sys.platform == "win32":
  4743|             workers = min(workers, _WIN32_MAX_WORKERS)
  4744|         return workers
  4745|     return int(value)
  4746| 
  4747| 
```
