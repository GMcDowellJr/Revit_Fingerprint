# Chunk of tools/archetype/discover_vfd_edges.py

- Source relative path: `tools/archetype/discover_vfd_edges.py`
- Chunk: 2 of 3
- Original line range: 487-947
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: parse_categories, normalize_param_name, parse_category_set, _resolve_target_domain_from_categories, _decompose_conflict_to_domains, _category_names_for_ids, _category_flags_for_ids, _find_verify_blocked_candidate, _validate_domain_has_identity_items, build_inventory_rows, build_inventory_rows.append_inventory_row, _category_map_domain_extracted, _candidate_category_details
- Source SHA-256: 95fe05c8009121c853de6753dc3020bdb0607b4cb260aeb1ac07496433793634
- Starts inside symbol: no
- Ends inside symbol: no

```
   487| def parse_categories(
   488|     raw: str,
   489|     category_map: Dict[str, Any],
   490|     warned_unparseable: Set[str],
   491|     recognized_distinct: Set[int],
   492|     unrecognized_distinct: Set[int],
   493| ) -> ParsedCategories:
   494|     parsed = parse_category_tokens(raw)
   495|     if parsed is None:
   496|         if raw not in warned_unparseable:
   497|             warn(f"unparseable vf.categories value {raw!r}; category scope will be empty for matching rows.")
   498|             warned_unparseable.add(raw)
   499|         parsed = []
   500| 
   501|     category_ids = sort_category_tokens(parsed)
   502|     if "-2000011" in category_map and "-2000011" in category_ids:
   503|         assert "-2000011" in category_map
   504|     names: List[str] = []
   505|     unrecognized: List[int] = []
   506|     has_unextracted = False
   507|     has_unverified = False
   508| 
   509|     for category_id in category_ids:
   510|         lookup_key = str(category_id).strip()
   511|         entry = category_map.get(lookup_key)
   512|         if entry is None:
   513|             unrecognized.append(int(lookup_key))
   514|             unrecognized_distinct.add(int(lookup_key))
   515|             continue
   516|         recognized_distinct.add(int(lookup_key))
   517|         name = category_entry_name(entry)
   518|         if name:
   519|             names.append(name)
   520|         if isinstance(entry, dict):
   521|             if entry.get("domain_extracted") is False:
   522|                 has_unextracted = True
   523|             if entry.get("_verify") is True:
   524|                 has_unverified = True
   525| 
   526|     return ParsedCategories(
   527|         category_set="|".join(category_ids),
   528|         category_ids=category_ids,
   529|         category_names="|".join(names),
   530|         unrecognized_category_ids="|".join(str(i) for i in unrecognized),
   531|         has_unextracted_domain=has_unextracted,
   532|         has_unverified_category_mapping=has_unverified,
   533|     )
   534| 
   535| 
   536| def normalize_param_name(name: str) -> str:
   537|     slug = []
   538|     previous_underscore = False
   539|     for ch in name.strip().lower():
   540|         if ch.isalnum():
   541|             slug.append(ch)
   542|             previous_underscore = False
   543|         elif ch.isspace() or ch == "_":
   544|             if not previous_underscore:
   545|                 slug.append("_")
   546|                 previous_underscore = True
   547|         # other punctuation is stripped, not replaced
   548|     return "".join(slug).strip("_")
   549| 
   550| 
   551| def parse_category_set(category_set: str) -> List[int]:
   552|     out: List[int] = []
   553|     for part in (category_set or "").split("|"):
   554|         if part:
   555|             out.append(int(part))
   556|     return out
   557| 
   558| 
   559| def _resolve_target_domain_from_categories(
   560|     category_ids: Sequence[int],
   561|     category_file_counts: Dict[str, int],
   562|     category_map: Dict[str, Any],
   563|     support_threshold: int,
   564| ) -> Tuple[Optional[str], str]:
   565|     qualifying = [
   566|         category_id for category_id in category_ids
   567|         if category_file_counts.get(str(category_id), 0) >= support_threshold
   568|     ]
   569|     if not qualifying:
   570|         return None, "category_map_no_signal"
   571| 
   572|     domains: Set[Optional[str]] = set()
   573|     for category_id in qualifying:
   574|         entry = category_map.get(str(category_id))
   575|         target: Optional[str] = None
   576|         if (
   577|             isinstance(entry, dict)
   578|             and entry.get("domain_extracted") is True
   579|             and entry.get("_verify") is not True
   580|         ):
   581|             target = entry.get("target_domain") or None
   582|         domains.add(target)
   583| 
   584|     if domains == {None}:
   585|         return None, "category_map_no_signal"
   586|     if len(domains) == 1:
   587|         return next(iter(domains)), "category_map_consensus"
   588|     return None, "category_map_conflict"
   589| 
   590| 
   591| def _decompose_conflict_to_domains(
   592|     category_ids: Sequence[int],
   593|     category_file_counts: Dict[str, int],
   594|     category_map: Dict[str, Any],
   595|     support_threshold: int,
   596| ) -> Dict[str, List[int]]:
   597|     """
   598|     For conflict cases where qualifying categories span multiple domains,
   599|     group category IDs by their resolved target_domain.
   600|     Only includes categories that:
   601|       - meet support_threshold file count
   602|       - have domain_extracted=True in the category map
   603|       - do NOT have _verify=True
   604|       - have a non-empty target_domain
   605|     Returns {target_domain: [category_ids]} — may be empty if no categories qualify.
   606|     """
   607|     domains: Dict[str, List[int]] = defaultdict(list)
   608|     for category_id in category_ids:
   609|         if category_file_counts.get(str(category_id), 0) < support_threshold:
   610|             continue
   611|         entry = category_map.get(str(category_id))
   612|         if not isinstance(entry, dict):
   613|             continue
   614|         if entry.get("domain_extracted") is not True or entry.get("_verify") is True:
   615|             continue
   616|         target_domain = entry.get("target_domain")
   617|         if target_domain in (None, ""):
   618|             continue
   619|         domains[str(target_domain)].append(category_id)
   620|     return {domain: sorted(cat_ids) for domain, cat_ids in sorted(domains.items())}
   621| 
   622| 
   623| def _category_names_for_ids(category_ids: Sequence[int], category_map: Dict[str, Any]) -> str:
   624|     names: List[str] = []
   625|     for category_id in category_ids:
   626|         entry = category_map.get(str(category_id))
   627|         if isinstance(entry, dict):
   628|             name = category_entry_name(entry)
   629|             if name:
   630|                 names.append(name)
   631|     return "|".join(names)
   632| 
   633| 
   634| def _category_flags_for_ids(category_ids: Sequence[int], category_map: Dict[str, Any]) -> Tuple[bool, bool]:
   635|     has_unextracted = False
   636|     has_unverified = False
   637|     for category_id in category_ids:
   638|         entry = category_map.get(str(category_id))
   639|         if not isinstance(entry, dict):
   640|             continue
   641|         if entry.get("domain_extracted") is False:
   642|             has_unextracted = True
   643|         if entry.get("_verify") is True:
   644|             has_unverified = True
   645|     return has_unextracted, has_unverified
   646| 
   647| 
   648| 
   649| def _find_verify_blocked_candidate(
   650|     category_ids: Sequence[int],
   651|     category_file_counts: Dict[str, int],
   652|     category_map: Dict[str, Any],
   653|     support_threshold: int,
   654| ) -> Tuple[str, str]:
   655|     """Return a category-map candidate blocked by explicit verification.
   656| 
   657|     This intentionally mirrors the qualifying-category portion of
   658|     _resolve_target_domain_from_categories() without changing that gate. It
   659|     only surfaces entries that category consensus would otherwise skip because
   660|     they are marked _verify=true.
   661|     """
   662|     qualifying = [
   663|         category_id for category_id in category_ids
   664|         if category_file_counts.get(str(category_id), 0) >= support_threshold
   665|     ]
   666|     if not qualifying:
   667|         return "", ""
   668| 
   669|     candidates: Set[str] = set()
   670|     for category_id in qualifying:
   671|         entry = category_map.get(str(category_id))
   672|         if (
   673|             isinstance(entry, dict)
   674|             and entry.get("domain_extracted") is True
   675|             and entry.get("_verify") is True
   676|         ):
   677|             target = entry.get("target_domain")
   678|             if target not in (None, ""):
   679|                 candidates.add(str(target))
   680| 
   681|     if not candidates:
   682|         return "", ""
   683|     return "|".join(sorted(candidates)), "category_map_verify_blocked"
   684| 
   685| 
   686| def _validate_domain_has_identity_items(target_domain: str, identity_items_dir: Path) -> bool:
   687|     shard_path = identity_items_dir / f"{target_domain}.csv"
   688|     if not shard_path.is_file():
   689|         return False
   690|     with shard_path.open("r", encoding="utf-8-sig", newline="") as f:
   691|         reader = csv.reader(f)
   692|         try:
   693|             next(reader)
   694|         except StopIteration:
   695|             return False
   696|         for _ in reader:
   697|             return True
   698|     return False
   699| 
   700| 
   701| def build_inventory_rows(
   702|     observations: Sequence[RawObservation],
   703|     resolved: Dict[str, ResolvedParam],
   704|     hints: Dict[str, Any],
   705|     category_map: Dict[str, Any],
   706|     support_min_files: int,
   707|     identity_items_dir: Optional[Path] = None,
   708| ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
   709|     warned_unparseable: Set[str] = set()
   710|     recognized_distinct: Set[int] = set()
   711|     unrecognized_distinct: Set[int] = set()
   712|     category_cache: Dict[str, ParsedCategories] = {}
   713|     groups: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
   714| 
   715|     for obs in observations:
   716|         param = resolved[obs.param_id]
   717|         if obs.categories_raw not in category_cache:
   718|             category_cache[obs.categories_raw] = parse_categories(
   719|                 obs.categories_raw, category_map, warned_unparseable, recognized_distinct, unrecognized_distinct,
   720|             )
   721|         cats = category_cache[obs.categories_raw]
   722|         key = (
   723|             obs.param_id,
   724|             param.param_kind,
   725|             param.param_name or "",
   726|             cats.category_set,
   727|         )
   728|         group = groups.setdefault(
   729|             key,
   730|             {
   731|                 "param_id": obs.param_id,
   732|                 "param_kind": param.param_kind,
   733|                 "param_name": param.param_name or "",
   734|                 "name_resolved": param.name_resolved,
   735|                 "category_set": cats.category_set,
   736|                 "category_ids": cats.category_ids,
   737|                 "category_names": cats.category_names,
   738|                 "unrecognized_category_ids": cats.unrecognized_category_ids,
   739|                 "has_unextracted_domain": cats.has_unextracted_domain,
   740|                 "has_unverified_category_mapping": cats.has_unverified_category_mapping,
   741|                 "export_run_ids": set(),
   742|                 "rule_count": 0,
   743|             },
   744|         )
   745|         group["export_run_ids"].add(obs.export_run_id)
   746|         group["rule_count"] += 1
   747| 
   748|     # Per-param category file support, aggregated across every category_set
   749|     # this param_id was observed under, for category-consensus resolution.
   750|     category_files_by_param: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
   751|     for group in groups.values():
   752|         for category_id in group["category_ids"]:
   753|             category_files_by_param[group["param_id"]][category_id].update(group["export_run_ids"])
   754|     category_file_counts_by_param: Dict[str, Dict[str, int]] = {
   755|         param_id: {cat_id: len(files) for cat_id, files in cat_counts.items()}
   756|         for param_id, cat_counts in category_files_by_param.items()
   757|     }
   758| 
   759|     rows: List[Dict[str, Any]] = []
   760|     for group in groups.values():
   761|         param_id = group["param_id"]
   762|         hint = infer_domain(param_id, group["param_name"] or None, hints)
   763|         target_domain = hint.target_domain or ""
   764|         target_domain_source = hint.source
   765|         target_domain_verified = bool(hint.verified)
   766| 
   767|         candidate_domain = ""
   768|         candidate_domain_blocked_reason = ""
   769|         category_ids_int = [int(cat_id) for cat_id in group["category_ids"]]
   770| 
   771|         def append_inventory_row(
   772|             row_target_domain: str,
   773|             row_target_domain_source: str,
   774|             row_target_domain_verified: bool,
   775|             row_category_ids: Sequence[int],
   776|             row_candidate_domain: str = "",
   777|             row_candidate_domain_blocked_reason: str = "",
   778|         ) -> None:
   779|             row_category_set = "|".join(str(category_id) for category_id in sorted(row_category_ids))
   780|             row_category_names = (
   781|                 _category_names_for_ids(row_category_ids, category_map)
   782|                 if row_category_ids
   783|                 else str(group["category_names"])
   784|             )
   785|             row_has_unextracted, row_has_unverified = (
   786|                 _category_flags_for_ids(row_category_ids, category_map)
   787|                 if row_category_ids
   788|                 else (bool(group["has_unextracted_domain"]), bool(group["has_unverified_category_mapping"]))
   789|             )
   790|             file_count = len(group["export_run_ids"])
   791|             meets_threshold = file_count >= support_min_files
   792|             name_resolved = bool(group["name_resolved"])
   793|             rows.append({
   794|                 "_export_run_ids": set(group["export_run_ids"]),
   795|                 "param_id": param_id,
   796|                 "param_kind": group["param_kind"],
   797|                 "param_name": group["param_name"],
   798|                 "name_resolved": bool_s(name_resolved),
   799|                 "target_domain": row_target_domain,
   800|                 "target_domain_source": row_target_domain_source,
   801|                 "target_domain_verified": bool_s(row_target_domain_verified),
   802|                 "category_set": row_category_set,
   803|                 "category_names": row_category_names,
   804|                 "unrecognized_category_ids": "" if row_category_ids else group["unrecognized_category_ids"],
   805|                 "has_unextracted_domain": bool_s(row_has_unextracted),
   806|                 "has_unverified_category_mapping": bool_s(row_has_unverified),
   807|                 "file_count": file_count,
   808|                 "rule_count": group["rule_count"],
   809|                 "meets_threshold": bool_s(meets_threshold),
   810|                 "requires_human_review": bool_s(row_target_domain == "" and name_resolved),
   811|                 "candidate_domain": row_candidate_domain,
   812|                 "candidate_domain_blocked_reason": row_candidate_domain_blocked_reason,
   813|             })
   814| 
   815|         if not target_domain:
   816|             consensus_domain, consensus_source = _resolve_target_domain_from_categories(
   817|                 category_ids_int,
   818|                 category_file_counts_by_param.get(param_id, {}),
   819|                 category_map,
   820|                 support_min_files,
   821|             )
   822|             if consensus_source == "category_map_consensus" and consensus_domain:
   823|                 consensus_verified = True
   824|                 if identity_items_dir is not None and identity_items_dir.is_dir():
   825|                     consensus_verified = _validate_domain_has_identity_items(consensus_domain, identity_items_dir)
   826|                 if consensus_verified:
   827|                     target_domain = consensus_domain
   828|                     target_domain_source = "category_map_consensus"
   829|                     target_domain_verified = True
   830|                 else:
   831|                     target_domain = ""
   832|                     target_domain_source = "unresolved"
   833|                     target_domain_verified = True
   834|                     candidate_domain = consensus_domain
   835|                     candidate_domain_blocked_reason = "identity_items_missing"
   836|             elif consensus_source == "category_map_conflict":
   837|                 domain_to_cats = _decompose_conflict_to_domains(
   838|                     category_ids_int,
   839|                     category_file_counts_by_param.get(param_id, {}),
   840|                     category_map,
   841|                     support_min_files,
   842|                 )
   843|                 if domain_to_cats:
   844|                     for resolved_domain, contributing_cat_ids in domain_to_cats.items():
   845|                         domain_verified = True
   846|                         if identity_items_dir is not None and identity_items_dir.is_dir():
   847|                             domain_verified = _validate_domain_has_identity_items(resolved_domain, identity_items_dir)
   848|                         if domain_verified:
   849|                             append_inventory_row(
   850|                                 resolved_domain,
   851|                                 "category_map_multi_domain",
   852|                                 True,
   853|                                 contributing_cat_ids,
   854|                             )
   855|                         else:
   856|                             append_inventory_row(
   857|                                 "",
   858|                                 "unresolved",
   859|                                 True,
   860|                                 contributing_cat_ids,
   861|                                 resolved_domain,
   862|                                 "identity_items_missing",
   863|                             )
   864|                     continue
   865|                 candidate_domain, candidate_domain_blocked_reason = _find_verify_blocked_candidate(
   866|                     category_ids_int,
   867|                     category_file_counts_by_param.get(param_id, {}),
   868|                     category_map,
   869|                     support_min_files,
   870|                 )
   871|                 target_domain = ""
   872|                 target_domain_source = "unresolved"
   873|                 target_domain_verified = True
   874|                 if not candidate_domain:
   875|                     candidate_domain_blocked_reason = "category_map_conflict_all_blocked"
   876|             elif consensus_source == "category_map_no_signal":
   877|                 candidate_domain, candidate_domain_blocked_reason = _find_verify_blocked_candidate(
   878|                     category_ids_int,
   879|                     category_file_counts_by_param.get(param_id, {}),
   880|                     category_map,
   881|                     support_min_files,
   882|                 )
   883|                 target_domain = ""
   884|                 target_domain_source = "unresolved"
   885|                 target_domain_verified = True
   886|             else:
   887|                 target_domain = ""
   888|                 target_domain_source = "unresolved"
   889|                 target_domain_verified = True
   890| 
   891|         append_inventory_row(
   892|             target_domain,
   893|             target_domain_source,
   894|             target_domain_verified,
   895|             category_ids_int,
   896|             candidate_domain,
   897|             candidate_domain_blocked_reason,
   898|         )
   899| 
   900|     rows.sort(key=lambda r: (r["param_kind"], r["param_id"], r["target_domain"], r["category_set"]))
   901|     stats = {
   902|         "recognized_distinct": recognized_distinct,
   903|         "unrecognized_distinct": unrecognized_distinct,
   904|     }
   905|     return rows, stats
   906| 
   907| 
   908| 
   909| def _category_map_domain_extracted(candidate_domain: str, category_map: Dict[str, Any]) -> str:
   910|     values: Set[str] = set()
   911|     for entry in category_map.values():
   912|         if not isinstance(entry, dict):
   913|             continue
   914|         if str(entry.get("target_domain") or "") != candidate_domain:
   915|             continue
   916|         value = entry.get("domain_extracted")
   917|         if isinstance(value, bool):
   918|             values.add(bool_s(value))
   919|     if not values:
   920|         return ""
   921|     if len(values) == 1:
   922|         return next(iter(values))
   923|     return "|".join(sorted(values))
   924| 
   925| 
   926| def _candidate_category_details(
   927|     candidate_domain: str,
   928|     category_set: str,
   929|     category_map: Dict[str, Any],
   930| ) -> Tuple[Set[str], Set[str]]:
   931|     category_ids: Set[str] = set()
   932|     category_names: Set[str] = set()
   933|     for category_id in str(category_set or "").split("|"):
   934|         if not category_id:
   935|             continue
   936|         entry = category_map.get(category_id)
   937|         if not isinstance(entry, dict):
   938|             continue
   939|         if str(entry.get("target_domain") or "") != candidate_domain:
   940|             continue
   941|         category_ids.add(category_id)
   942|         name = category_entry_name(entry)
   943|         if name:
   944|             category_names.add(name)
   945|     return category_ids, category_names
   946| 
   947| 
```
