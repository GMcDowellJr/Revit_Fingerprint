# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 8 of 13
- Original line range: 3311-3759
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: run_pair, _run_pair_domain
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  3311| def run_pair(
  3312|     policy: EnterprisePolicy,
  3313|     seg_a: str,
  3314|     seg_b: str,
  3315|     comparison_type: str,
  3316|     domain: str,
  3317|     manifest: Dict[str, Dict[str, str]],
  3318|     registry: Dict[str, Dict[str, str]],
  3319|     file_metadata: Dict[str, Dict[str, str]],
  3320|     segments_root: Path,
  3321|     min_patterns: int,
  3322|     executed_utc: str,
  3323| ) -> Tuple[Optional[Dict[str, str]], List[Dict[str, str]]]:
  3324|     """Return (summary_row_or_None, pair_detail_rows).
  3325| 
  3326|     All comparisons use file-level join_hash inventories from membership_matrix.csv.
  3327|     Bundle membership is added as post-hoc annotation after scores are computed.
  3328|     """
  3329|     is_directed = comparison_type in DIRECTED_TYPES
  3330|     is_within_project = comparison_type == "within_project"
  3331| 
  3332|     # For within_project: group by project_label within the single segment, then
  3333|     # aggregate all intra-project pairs into ONE summary row for (segment, domain).
  3334|     if is_within_project:
  3335|         all_files = load_file_join_hashes(segments_root, registry, seg_a, domain)
  3336|         all_files_used = load_file_join_hashes(segments_root, registry, seg_a, domain, "used")
  3337| 
  3338|         by_proj: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
  3339|         for eid, jhs in all_files.items():
  3340|             meta = file_metadata.get(eid, {})
  3341|             label = meta.get("project_label", "").strip()
  3342|             proj = eid if is_blank_or_na(label) else label
  3343|             by_proj[proj][eid] = jhs
  3344| 
  3345|         # Used-view project grouping (same labels, but used-view join_hash sets)
  3346|         by_proj_used: Dict[str, Dict[str, Set[str]]] = defaultdict(dict)
  3347|         for eid, jhs in all_files_used.items():
  3348|             meta = file_metadata.get(eid, {})
  3349|             label = meta.get("project_label", "").strip()
  3350|             proj = eid if is_blank_or_na(label) else label
  3351|             by_proj_used[proj][eid] = jhs
  3352| 
  3353|         PairRecord = Tuple[str, str, str, int, int, int, float, float, float]
  3354|         raw_pairs: List[PairRecord] = []  # (eid_a, eid_b, proj, na, nb, ns, j, c_ab, c_ba)
  3355|         participating_eids: Set[str] = set()
  3356| 
  3357|         for proj, proj_files in by_proj.items():
  3358|             if len(proj_files) < 2:
  3359|                 continue
  3360|             eids_sorted = sorted(proj_files.keys())
  3361|             for i in range(len(eids_sorted)):
  3362|                 for jj in range(i + 1, len(eids_sorted)):
  3363|                     eid_a2, eid_b2 = eids_sorted[i], eids_sorted[jj]
  3364|                     jhs_a2 = proj_files[eid_a2]
  3365|                     jhs_b2 = proj_files[eid_b2]
  3366|                     union = jhs_a2 | jhs_b2
  3367|                     j_val = len(jhs_a2 & jhs_b2) / len(union) if union else 0.0
  3368|                     c_ab = len(jhs_a2 & jhs_b2) / len(jhs_a2) if jhs_a2 else 0.0
  3369|                     c_ba = len(jhs_a2 & jhs_b2) / len(jhs_b2) if jhs_b2 else 0.0
  3370|                     raw_pairs.append((
  3371|                         eid_a2, eid_b2, proj,
  3372|                         len(jhs_a2), len(jhs_b2), len(jhs_a2 & jhs_b2),
  3373|                         j_val, c_ab, c_ba,
  3374|                     ))
  3375|                     participating_eids.add(eid_a2)
  3376|                     participating_eids.add(eid_b2)
  3377| 
  3378|         if not raw_pairs:
  3379|             return None, []
  3380| 
  3381|         jaccards = [p[6] for p in raw_pairs]
  3382|         total_jhs: Set[str] = set()
  3383|         for eid in participating_eids:
  3384|             total_jhs |= all_files.get(eid, set())
  3385| 
  3386|         if len(total_jhs) < min_patterns:
  3387|             return None, []
  3388| 
  3389|         from collections import Counter as _Counter
  3390|         jhs_file_count: Dict[str, int] = _Counter(
  3391|             jh for eid in participating_eids for jh in all_files.get(eid, set())
  3392|         )
  3393|         n_shared_jh = sum(1 for v in jhs_file_count.values() if v > 1)
  3394|         n_files = len(participating_eids)
  3395| 
  3396|         # Used-view intra-project pairs (indexed by (eid_a, eid_b) for join onto all-view)
  3397|         UsedRec = Tuple[int, float, float, float]  # (n_shared, jaccard, c_ab, c_ba)
  3398|         used_pair_index_wp: Dict[Tuple[str, str], UsedRec] = {}
  3399|         used_jaccards_wp: List[float] = []
  3400|         for proj, proj_files_used in by_proj_used.items():
  3401|             if len(proj_files_used) < 2:
  3402|                 continue
  3403|             eids_sorted_u = sorted(proj_files_used.keys())
  3404|             for i in range(len(eids_sorted_u)):
  3405|                 for jj in range(i + 1, len(eids_sorted_u)):
  3406|                     eu_a, eu_b = eids_sorted_u[i], eids_sorted_u[jj]
  3407|                     ju_a = proj_files_used[eu_a]
  3408|                     ju_b = proj_files_used[eu_b]
  3409|                     union_u = ju_a | ju_b
  3410|                     j_u = len(ju_a & ju_b) / len(union_u) if union_u else 0.0
  3411|                     cu_ab = len(ju_a & ju_b) / len(ju_a) if ju_a else 0.0
  3412|                     cu_ba = len(ju_a & ju_b) / len(ju_b) if ju_b else 0.0
  3413|                     used_pair_index_wp[(eu_a, eu_b)] = (len(ju_a & ju_b), j_u, cu_ab, cu_ba)
  3414|                     used_jaccards_wp.append(j_u)
  3415| 
  3416|         # Used-view shared join_hash count (patterns seen in >1 file under used view)
  3417|         used_jhs_file_count_wp: Dict[str, int] = _Counter(
  3418|             jh for eid in participating_eids for jh in all_files_used.get(eid, set())
  3419|         )
  3420|         used_n_shared_jh_wp = sum(1 for v in used_jhs_file_count_wp.values() if v > 1)
  3421| 
  3422|         # Bundle annotation on the shared set (dual-view)
  3423|         shared_jhs_wp: Set[str] = {jh for jh, cnt in jhs_file_count.items() if cnt > 1}
  3424|         bnd_a_wp_all = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "all")
  3425|         bnd_a_wp_used = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "used")
  3426|         n_both_wp_all, n_aonly_wp_all, n_bonly_wp_all = annotate_bundle_overlap(
  3427|             shared_jhs_wp, bnd_a_wp_all, bnd_a_wp_all
  3428|         )
  3429|         n_both_wp_used, n_aonly_wp_used, n_bonly_wp_used = annotate_bundle_overlap(
  3430|             shared_jhs_wp, bnd_a_wp_used, bnd_a_wp_used
  3431|         )
  3432| 
  3433|         metrics: Dict[str, str] = {
  3434|             "n_shared_join_hash": str(n_shared_jh),
  3435|             "all_pairwise_jaccard_mean": _mean(jaccards),
  3436|             "all_jaccard_p10": _fmt(_pct(jaccards, 10)) if jaccards else "",
  3437|             "all_jaccard_p90": _fmt(_pct(jaccards, 90)) if jaccards else "",
  3438|             "n_files_a": str(n_files),
  3439|             "n_files_b": str(n_files),
  3440|             "n_pairs": str(len(raw_pairs)),
  3441|         }
  3442| 
  3443|         crid = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
  3444|         all_has_bundles = "true" if bnd_a_wp_all else "false"
  3445|         used_has_bundles = "true" if bnd_a_wp_used else "false"
  3446|         n_unique_wp = len(total_jhs)
  3447| 
  3448|         summary_row = _build_summary_row(
  3449|             policy, crid, seg_a, seg_b, comparison_type, domain,
  3450|             manifest, metrics,
  3451|             n_patterns_a=n_unique_wp,
  3452|             n_patterns_b=n_unique_wp,
  3453|             n_unique_patterns_a=n_unique_wp,
  3454|             n_unique_patterns_b=n_unique_wp,
  3455|             all_has_bundles_a=all_has_bundles,
  3456|             all_has_bundles_b=all_has_bundles,
  3457|             all_n_shared_bundle_both=n_both_wp_all,
  3458|             all_n_shared_bundle_a_only=n_aonly_wp_all,
  3459|             all_n_shared_bundle_b_only=n_bonly_wp_all,
  3460|             used_has_bundles_a=used_has_bundles,
  3461|             used_has_bundles_b=used_has_bundles,
  3462|             used_n_shared_bundle_both=n_both_wp_used,
  3463|             used_n_shared_bundle_a_only=n_aonly_wp_used,
  3464|             used_n_shared_bundle_b_only=n_bonly_wp_used,
  3465|             used_n_shared_join_hash=str(used_n_shared_jh_wp),
  3466|             used_pairwise_jaccard_mean=_mean(used_jaccards_wp),
  3467|             used_jaccard_p10=_fmt(_pct(used_jaccards_wp, 10)) if used_jaccards_wp else "",
  3468|             used_jaccard_p90=_fmt(_pct(used_jaccards_wp, 90)) if used_jaccards_wp else "",
  3469|             executed_utc=executed_utc,
  3470|         )
  3471| 
  3472|         # Emit ALL pair rows (no suppression threshold)
  3473|         c_ab_list_wp = [p[7] for p in raw_pairs]
  3474|         c_ba_list_wp = [p[8] for p in raw_pairs]
  3475|         detail_rows: List[Dict[str, str]] = []
  3476|         used_c_ab_list_wp: List[float] = []
  3477|         used_c_ba_list_wp: List[float] = []
  3478|         for eid_a2, eid_b2, proj, na, nb, ns, j_val, c_ab, c_ba in raw_pairs:
  3479|             shared_pair: Set[str] = all_files.get(eid_a2, set()) & all_files.get(eid_b2, set())
  3480|             pb_all, pao_all, pbo_all = annotate_bundle_overlap(shared_pair, bnd_a_wp_all, bnd_a_wp_all)
  3481|             pb_used, pao_used, pbo_used = annotate_bundle_overlap(shared_pair, bnd_a_wp_used, bnd_a_wp_used)
  3482|             u_ns, u_j, u_cab, u_cba = used_pair_index_wp.get((eid_a2, eid_b2), (0, 0.0, 0.0, 0.0))
  3483|             used_c_ab_list_wp.append(u_cab)
  3484|             used_c_ba_list_wp.append(u_cba)
  3485|             detail_rows.append({
  3486|                 "comparison_run_id": crid,
  3487|                 "segment_id_a": seg_a,
  3488|                 "segment_id_b": seg_b,
  3489|                 "domain": domain,
  3490|                 "export_run_id_a": eid_a2,
  3491|                 "export_run_id_b": eid_b2,
  3492|                 "project_label_a": proj,
  3493|                 "project_label_b": proj,
  3494|                 "n_patterns_a": str(na),
  3495|                 "n_patterns_b": str(nb),
  3496|                 "n_shared": str(ns),
  3497|                 "all_jaccard": _fmt(j_val),
  3498|                 "all_containment_a_in_b": _fmt(c_ab),
  3499|                 "all_containment_b_in_a": _fmt(c_ba),
  3500|                 "used_n_shared": str(u_ns),
  3501|                 "used_jaccard": _fmt(u_j),
  3502|                 "used_containment_a_in_b": _fmt(u_cab),
  3503|                 "used_containment_b_in_a": _fmt(u_cba),
  3504|                 "all_n_shared_bundle_both": str(pb_all),
  3505|                 "all_n_shared_bundle_a_only": str(pao_all),
  3506|                 "all_n_shared_bundle_b_only": str(pbo_all),
  3507|                 "used_n_shared_bundle_both": str(pb_used),
  3508|                 "used_n_shared_bundle_a_only": str(pao_used),
  3509|                 "used_n_shared_bundle_b_only": str(pbo_used),
  3510|             })
  3511| 
  3512|         # Patch containment into summary metrics (mean/min over all pairs)
  3513|         summary_row["all_pairwise_containment_a_in_b_mean"] = _mean(c_ab_list_wp)
  3514|         summary_row["all_containment_a_in_b_min"] = _min(c_ab_list_wp)
  3515|         summary_row["all_pairwise_containment_b_in_a_mean"] = _mean(c_ba_list_wp)
  3516|         summary_row["all_containment_b_in_a_min"] = _min(c_ba_list_wp)
  3517|         summary_row["used_pairwise_containment_a_in_b_mean"] = _mean(used_c_ab_list_wp)
  3518|         summary_row["used_containment_a_in_b_min"] = _min(used_c_ab_list_wp)
  3519|         summary_row["used_pairwise_containment_b_in_a_mean"] = _mean(used_c_ba_list_wp)
  3520|         summary_row["used_containment_b_in_a_min"] = _min(used_c_ba_list_wp)
  3521|         summary_row["aggregation_method"] = "cartesian_file_pair_mean"
  3522|         summary_row.update(_cardinality_fields(n_files, n_files))
  3523| 
  3524|         return summary_row, detail_rows
  3525| 
  3526|     # Normal path — file-based, both all-view and used-view
  3527|     files_a = load_file_join_hashes(segments_root, registry, seg_a, domain)
  3528|     files_b = load_file_join_hashes(segments_root, registry, seg_b, domain)
  3529|     files_a_used = load_file_join_hashes(segments_root, registry, seg_a, domain, "used")
  3530|     files_b_used = load_file_join_hashes(segments_root, registry, seg_b, domain, "used")
  3531| 
  3532|     all_jhs_a: Set[str] = set()
  3533|     for jhs in files_a.values():
  3534|         all_jhs_a |= jhs
  3535|     all_jhs_b: Set[str] = set()
  3536|     for jhs in files_b.values():
  3537|         all_jhs_b |= jhs
  3538| 
  3539|     n_a = len(all_jhs_a)
  3540|     n_b = len(all_jhs_b)
  3541|     n_files_a_ct = len(files_a)
  3542|     n_files_b_ct = len(files_b)
  3543| 
  3544|     # Zero readable file inventory on either side is the only case that means
  3545|     # "don't trust this row at all" -- emit a real, schema-complete row marked
  3546|     # blocked instead of silently suppressing it. inventory_status_a/b
  3547|     # distinguishes a confirmed-empty domain (source read succeeded, zero
  3548|     # patterns) from a side that couldn't be read at all -- both have zero
  3549|     # files, but they are not the same fact.
  3550|     if n_files_a_ct == 0 or n_files_b_ct == 0:
  3551|         status_a, _ = _segment_domain_source_status(segments_root, registry, seg_a, domain)
  3552|         status_b, _ = _segment_domain_source_status(segments_root, registry, seg_b, domain)
  3553|         crid_blocked = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
  3554|         # has_bundles_* documents whether bundle analysis produced output for
  3555|         # each side -- availability metadata, not a similarity score -- so
  3556|         # it must be computed per side even when the comparison itself is
  3557|         # blocked. A populated side's bundles are real and available; only
  3558|         # the shared-overlap bucket counts are meaningless when one side has
  3559|         # zero files, so those stay at 0.
  3560|         bnd_a_all_blocked = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "all")
  3561|         bnd_b_all_blocked = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "all")
  3562|         bnd_a_used_blocked = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "used")
  3563|         bnd_b_used_blocked = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "used")
  3564|         blocked_row = _build_summary_row(
  3565|             policy, crid_blocked, seg_a, seg_b, comparison_type, domain,
  3566|             manifest, {"n_files_a": str(n_files_a_ct), "n_files_b": str(n_files_b_ct), "n_pairs": "0"},
  3567|             # n_a/n_b are the populated side's real pattern counts (a blocked
  3568|             # side is zero by definition, but the other side may not be) --
  3569|             # reporting them as 0 here would corrupt the raw inventory counts
  3570|             # a downstream reader needs to understand what was blocked.
  3571|             n_patterns_a=n_a, n_patterns_b=n_b,
  3572|             n_unique_patterns_a=n_a, n_unique_patterns_b=n_b,
  3573|             all_has_bundles_a="true" if bnd_a_all_blocked else "false",
  3574|             all_has_bundles_b="true" if bnd_b_all_blocked else "false",
  3575|             all_n_shared_bundle_both=0, all_n_shared_bundle_a_only=0, all_n_shared_bundle_b_only=0,
  3576|             used_has_bundles_a="true" if bnd_a_used_blocked else "false",
  3577|             used_has_bundles_b="true" if bnd_b_used_blocked else "false",
  3578|             used_n_shared_bundle_both=0, used_n_shared_bundle_a_only=0, used_n_shared_bundle_b_only=0,
  3579|             executed_utc=executed_utc,
  3580|         )
  3581|         blocked_row.update(_cardinality_fields(n_files_a_ct, n_files_b_ct))
  3582|         blocked_row["inventory_status_a"] = status_a
  3583|         blocked_row["inventory_status_b"] = status_b
  3584|         for key in (
  3585|             "all_union_jaccard", "all_union_containment_a_in_b", "all_union_containment_b_in_a",
  3586|             "used_union_jaccard", "used_union_containment_a_in_b", "used_union_containment_b_in_a",
  3587|             "all_a_file_mean_similarity_to_b_mean", "all_a_file_mean_similarity_to_b_min",
  3588|             "all_b_file_mean_similarity_to_a_mean", "all_b_file_mean_similarity_to_a_min",
  3589|             "reference_union_pattern_count", "reference_intersection_pattern_count", "reference_core_share",
  3590|         ):
  3591|             blocked_row[key] = ""
  3592|         if is_directed:
  3593|             blocked_row["reference_aggregation"] = "union"
  3594|             blocked_row["target_aggregation"] = "per_file_distribution"
  3595|             blocked_row["n_reference_files"] = str(n_files_a_ct)
  3596|         else:
  3597|             blocked_row["aggregation_method"] = "cartesian_file_pair_mean"
  3598|         return blocked_row, []
  3599| 
  3600|     if n_a < min_patterns or n_b < min_patterns:
  3601|         return None, []
  3602| 
  3603|     pair_rows: List[Dict[str, str]] = []
  3604| 
  3605|     # Load bundle sets for both views upfront
  3606|     bnd_a_all = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "all")
  3607|     bnd_b_all = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "all")
  3608|     bnd_a_used = load_bundle_join_hash_set(segments_root, registry, seg_a, domain, "used")
  3609|     bnd_b_used = load_bundle_join_hash_set(segments_root, registry, seg_b, domain, "used")
  3610| 
  3611|     # All-view metrics
  3612|     if is_directed:
  3613|         metrics = compare_directed_file(files_a, files_b)
  3614|         metrics_used = compare_directed_file(files_a_used, files_b_used)
  3615|     else:
  3616|         metrics, pair_rows_raw = compare_symmetric_file(files_a, files_b)
  3617|         metrics_used, pair_rows_used = compare_symmetric_file(files_a_used, files_b_used)
  3618|         # Index used-view rows by (eid_a, eid_b) for join
  3619|         used_row_index: Dict[Tuple[str, str], Dict[str, str]] = {
  3620|             (r["export_run_id_a"], r["export_run_id_b"]): r
  3621|             for r in pair_rows_used
  3622|         }
  3623|         # Emit ALL pair rows — no suppression threshold
  3624|         crid_pre = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
  3625|         for r in pair_rows_raw:
  3626|             eid_a2 = r.get("export_run_id_a", "")
  3627|             eid_b2 = r.get("export_run_id_b", "")
  3628|             shared_pair = files_a.get(eid_a2, set()) & files_b.get(eid_b2, set())
  3629|             pb_all, pao_all, pbo_all = annotate_bundle_overlap(shared_pair, bnd_a_all, bnd_b_all)
  3630|             pb_used, pao_used, pbo_used = annotate_bundle_overlap(shared_pair, bnd_a_used, bnd_b_used)
  3631|             ur = used_row_index.get((eid_a2, eid_b2), {})
  3632|             r.update({
  3633|                 "comparison_run_id": crid_pre,
  3634|                 "segment_id_a": seg_a,
  3635|                 "segment_id_b": seg_b,
  3636|                 "domain": domain,
  3637|                 "project_label_a": file_metadata.get(eid_a2, {}).get("project_label", ""),
  3638|                 "project_label_b": file_metadata.get(eid_b2, {}).get("project_label", ""),
  3639|                 "used_n_shared": ur.get("n_shared", "0"),
  3640|                 "used_jaccard": ur.get("all_jaccard", ""),
  3641|                 "used_containment_a_in_b": ur.get("all_containment_a_in_b", ""),
  3642|                 "used_containment_b_in_a": ur.get("all_containment_b_in_a", ""),
  3643|                 "all_n_shared_bundle_both": str(pb_all),
  3644|                 "all_n_shared_bundle_a_only": str(pao_all),
  3645|                 "all_n_shared_bundle_b_only": str(pbo_all),
  3646|                 "used_n_shared_bundle_both": str(pb_used),
  3647|                 "used_n_shared_bundle_a_only": str(pao_used),
  3648|                 "used_n_shared_bundle_b_only": str(pbo_used),
  3649|             })
  3650|         pair_rows = pair_rows_raw
  3651| 
  3652|     if not metrics:
  3653|         return None, []
  3654| 
  3655|     # Used-view population-grain shared count
  3656|     all_jhs_a_used: Set[str] = set()
  3657|     for jhs in files_a_used.values():
  3658|         all_jhs_a_used |= jhs
  3659|     all_jhs_b_used: Set[str] = set()
  3660|     for jhs in files_b_used.values():
  3661|         all_jhs_b_used |= jhs
  3662|     used_n_shared_jh = len(all_jhs_a_used & all_jhs_b_used)
  3663| 
  3664|     # Post-hoc bundle annotation on the population-grain shared set (dual-view)
  3665|     shared_jhs_norm = all_jhs_a & all_jhs_b
  3666|     n_both_all, n_aonly_all, n_bonly_all = annotate_bundle_overlap(shared_jhs_norm, bnd_a_all, bnd_b_all)
  3667|     n_both_used, n_aonly_used, n_bonly_used = annotate_bundle_overlap(shared_jhs_norm, bnd_a_used, bnd_b_used)
  3668| 
  3669|     all_has_bundles_a = "true" if bnd_a_all else "false"
  3670|     all_has_bundles_b = "true" if bnd_b_all else "false"
  3671|     used_has_bundles_a = "true" if bnd_a_used else "false"
  3672|     used_has_bundles_b = "true" if bnd_b_used else "false"
  3673| 
  3674|     crid = make_comparison_run_id(seg_a, seg_b, executed_utc, comparison_type)
  3675|     summary = _build_summary_row(
  3676|         policy, crid, seg_a, seg_b, comparison_type, domain,
  3677|         manifest, metrics,
  3678|         n_patterns_a=n_a,
  3679|         n_patterns_b=n_b,
  3680|         n_unique_patterns_a=n_a,
  3681|         n_unique_patterns_b=n_b,
  3682|         all_has_bundles_a=all_has_bundles_a,
  3683|         all_has_bundles_b=all_has_bundles_b,
  3684|         all_n_shared_bundle_both=n_both_all,
  3685|         all_n_shared_bundle_a_only=n_aonly_all,
  3686|         all_n_shared_bundle_b_only=n_bonly_all,
  3687|         used_has_bundles_a=used_has_bundles_a,
  3688|         used_has_bundles_b=used_has_bundles_b,
  3689|         used_n_shared_bundle_both=n_both_used,
  3690|         used_n_shared_bundle_a_only=n_aonly_used,
  3691|         used_n_shared_bundle_b_only=n_bonly_used,
  3692|         used_n_shared_join_hash=str(used_n_shared_jh),
  3693|         used_pairwise_jaccard_mean=metrics_used.get("all_pairwise_jaccard_mean", ""),
  3694|         used_jaccard_p10=metrics_used.get("all_jaccard_p10", ""),
  3695|         used_jaccard_p90=metrics_used.get("all_jaccard_p90", ""),
  3696|         used_pairwise_containment_a_in_b_mean=metrics_used.get("all_pairwise_containment_a_in_b_mean", ""),
  3697|         used_containment_a_in_b_min=metrics_used.get("all_containment_a_in_b_min", ""),
  3698|         used_pairwise_containment_b_in_a_mean=metrics_used.get("all_pairwise_containment_b_in_a_mean", ""),
  3699|         used_containment_b_in_a_min=metrics_used.get("all_containment_b_in_a_min", ""),
  3700|         executed_utc=executed_utc,
  3701|     )
  3702|     summary.update(_cardinality_fields(n_files_a_ct, n_files_b_ct))
  3703|     if is_directed:
  3704|         summary["reference_aggregation"] = "union"
  3705|         summary["target_aggregation"] = "per_file_distribution"
  3706|         summary["n_reference_files"] = metrics.get("n_reference_files", "")
  3707|         summary["reference_union_pattern_count"] = metrics.get("reference_union_pattern_count", "")
  3708|         summary["reference_intersection_pattern_count"] = metrics.get("reference_intersection_pattern_count", "")
  3709|         summary["reference_core_share"] = metrics.get("reference_core_share", "")
  3710|     else:
  3711|         summary["aggregation_method"] = "cartesian_file_pair_mean"
  3712|         all_union_jaccard, all_union_c_ab, all_union_c_ba = _union_similarity(all_jhs_a, all_jhs_b)
  3713|         used_union_jaccard, used_union_c_ab, used_union_c_ba = _union_similarity(all_jhs_a_used, all_jhs_b_used)
  3714|         summary["all_union_jaccard"] = all_union_jaccard
  3715|         summary["all_union_containment_a_in_b"] = all_union_c_ab
  3716|         summary["all_union_containment_b_in_a"] = all_union_c_ba
  3717|         summary["used_union_jaccard"] = used_union_jaccard
  3718|         summary["used_union_containment_a_in_b"] = used_union_c_ab
  3719|         summary["used_union_containment_b_in_a"] = used_union_c_ba
  3720|         summary["all_a_file_mean_similarity_to_b_mean"] = metrics.get("all_a_file_mean_similarity_to_b_mean", "")
  3721|         summary["all_a_file_mean_similarity_to_b_min"] = metrics.get("all_a_file_mean_similarity_to_b_min", "")
  3722|         summary["all_b_file_mean_similarity_to_a_mean"] = metrics.get("all_b_file_mean_similarity_to_a_mean", "")
  3723|         summary["all_b_file_mean_similarity_to_a_min"] = metrics.get("all_b_file_mean_similarity_to_a_min", "")
  3724|     for r in pair_rows:
  3725|         r["comparison_run_id"] = crid
  3726|     return summary, pair_rows
  3727| 
  3728| 
  3729| def _run_pair_domain(
  3730|     policy: EnterprisePolicy,
  3731|     seg_a: str,
  3732|     seg_b: str,
  3733|     comparison_type: str,
  3734|     domain: str,
  3735|     manifest: Dict[str, Dict[str, str]],
  3736|     registry: Dict[str, Dict[str, str]],
  3737|     file_metadata: Dict[str, Dict[str, str]],
  3738|     segments_root: Path,
  3739|     min_patterns: int,
  3740|     executed_utc: str,
  3741|     no_delta: bool,
  3742| ) -> Tuple[Optional[Dict[str, str]], List[Dict[str, str]]]:
  3743|     """Wrapper around run_pair for a single pair×domain. Returns (summary_row, detail_rows)."""
  3744|     _ = no_delta  # Accepted for future use; run_pair does not currently consume it.
  3745|     return run_pair(
  3746|         policy=policy,
  3747|         seg_a=seg_a,
  3748|         seg_b=seg_b,
  3749|         comparison_type=comparison_type,
  3750|         domain=domain,
  3751|         manifest=manifest,
  3752|         registry=registry,
  3753|         file_metadata=file_metadata,
  3754|         segments_root=segments_root,
  3755|         min_patterns=min_patterns,
  3756|         executed_utc=executed_utc,
  3757|     )
  3758| 
  3759| 
```
