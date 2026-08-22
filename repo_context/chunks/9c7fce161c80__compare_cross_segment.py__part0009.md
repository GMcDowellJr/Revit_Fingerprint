# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 9 of 13
- Original line range: 3760-4256
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _build_summary_row, build_governance_state_outputs, _build_pooled_row
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  3760| def _build_summary_row(
  3761|     policy: EnterprisePolicy,
  3762|     crid: str,
  3763|     seg_a: str,
  3764|     seg_b: str,
  3765|     comparison_type: str,
  3766|     domain: str,
  3767|     manifest: Dict[str, Dict[str, str]],
  3768|     metrics: Dict[str, str],
  3769|     n_patterns_a: int,
  3770|     n_patterns_b: int,
  3771|     n_unique_patterns_a: int,
  3772|     n_unique_patterns_b: int,
  3773|     all_has_bundles_a: str,
  3774|     all_has_bundles_b: str,
  3775|     all_n_shared_bundle_both: int,
  3776|     all_n_shared_bundle_a_only: int,
  3777|     all_n_shared_bundle_b_only: int,
  3778|     used_has_bundles_a: str,
  3779|     used_has_bundles_b: str,
  3780|     used_n_shared_bundle_both: int,
  3781|     used_n_shared_bundle_a_only: int,
  3782|     used_n_shared_bundle_b_only: int,
  3783|     executed_utc: str,
  3784|     used_n_shared_join_hash: str = "",
  3785|     used_pairwise_jaccard_mean: str = "",
  3786|     used_jaccard_p10: str = "",
  3787|     used_jaccard_p90: str = "",
  3788|     used_pairwise_containment_a_in_b_mean: str = "",
  3789|     used_containment_a_in_b_min: str = "",
  3790|     used_pairwise_containment_b_in_a_mean: str = "",
  3791|     used_containment_b_in_a_min: str = "",
  3792| ) -> Dict[str, str]:
  3793|     ma = manifest.get(seg_a, {})
  3794|     mb = manifest.get(seg_b, {})
  3795|     # A resolved redundant_single_child descendant (see _resolve_runnable_segment())
  3796|     # carries an override, stashed by _stash_scope_override(), of the ORIGINAL
  3797|     # (demoted) row's business_center_label/discipline_label/scope_level --
  3798|     # the broader population this comparison was actually matched under, as
  3799|     # opposed to the descendant's own narrower identity. segment_id_a/_b stay
  3800|     # the resolved descendant regardless (the only segment with real on-disk
  3801|     # data); only these display fields are affected. See Codex review finding
  3802|     # on PR #380.
  3803|     override_a = ma.get(_scope_override_key(comparison_type)) or {}
  3804|     override_b = mb.get(_scope_override_key(comparison_type)) or {}
  3805| 
  3806|     # signal_spread: raw containment-asymmetry measure (min-side minus max-side
  3807|     # containment share of the shared set); no interpretive banding applied here.
  3808|     _n_shared_ss = int(float(metrics.get("n_shared_join_hash") or 0))
  3809|     _n_a_ss = int(n_unique_patterns_a) if n_unique_patterns_a else 0
  3810|     _n_b_ss = int(n_unique_patterns_b) if n_unique_patterns_b else 0
  3811|     _min_ss = min(_n_a_ss, _n_b_ss)
  3812|     _max_ss = max(_n_a_ss, _n_b_ss)
  3813|     if _min_ss > 0:
  3814|         _signal_spread = (_n_shared_ss / _min_ss) - (_n_shared_ss / _max_ss if _max_ss > 0 else 0.0)
  3815|         _signal_spread_str = f"{_signal_spread:.4f}"
  3816|     else:
  3817|         _signal_spread_str = ""
  3818| 
  3819|     return {
  3820|         "comparison_run_id": crid,
  3821|         "segment_id_a": seg_a,
  3822|         "segment_id_b": seg_b,
  3823|         "segment_label_a": ma.get("segment_label", ""),
  3824|         "segment_label_b": mb.get("segment_label", ""),
  3825|         "governance_role_a": ma.get("governance_role", ""),
  3826|         "governance_role_b": mb.get("governance_role", ""),
  3827|         "client_label_a": ma.get("client_label", ""),
  3828|         "client_label_b": mb.get("client_label", ""),
  3829|         "business_center_label_a": override_a.get("business_center_label", _bc_of(ma)),
  3830|         "business_center_label_b": override_b.get("business_center_label", _bc_of(mb)),
  3831|         "scope_level_a": override_a.get("scope_level", _scope_level(ma, policy) or ""),
  3832|         "scope_level_b": override_b.get("scope_level", _scope_level(mb, policy) or ""),
  3833|         "discipline_label_a": override_a.get("discipline_label", ma.get("discipline_label", "")),
  3834|         "discipline_label_b": override_b.get("discipline_label", mb.get("discipline_label", "")),
  3835|         "unit_system": ma.get("unit_system", ""),
  3836|         "comparison_type": comparison_type,
  3837|         "domain": domain,
  3838|         "n_patterns_a": str(n_patterns_a),
  3839|         "n_patterns_b": str(n_patterns_b),
  3840|         "n_shared_join_hash": metrics.get("n_shared_join_hash", ""),
  3841|         "n_unique_patterns_a": str(n_unique_patterns_a),
  3842|         "n_unique_patterns_b": str(n_unique_patterns_b),
  3843|         "signal_spread": _signal_spread_str,
  3844|         "all_pairwise_containment_a_in_b_mean": metrics.get("all_pairwise_containment_a_in_b_mean", ""),
  3845|         "all_containment_a_in_b_min": metrics.get("all_containment_a_in_b_min", ""),
  3846|         "all_pairwise_containment_b_in_a_mean": metrics.get("all_pairwise_containment_b_in_a_mean", ""),
  3847|         "all_containment_b_in_a_min": metrics.get("all_containment_b_in_a_min", ""),
  3848|         "all_pairwise_jaccard_mean": metrics.get("all_pairwise_jaccard_mean", ""),
  3849|         "all_jaccard_p10": metrics.get("all_jaccard_p10", ""),
  3850|         "all_jaccard_p90": metrics.get("all_jaccard_p90", ""),
  3851|         "used_pairwise_jaccard_mean": used_pairwise_jaccard_mean,
  3852|         "used_jaccard_p10": used_jaccard_p10,
  3853|         "used_jaccard_p90": used_jaccard_p90,
  3854|         "used_pairwise_containment_a_in_b_mean": used_pairwise_containment_a_in_b_mean,
  3855|         "used_containment_a_in_b_min": used_containment_a_in_b_min,
  3856|         "used_pairwise_containment_b_in_a_mean": used_pairwise_containment_b_in_a_mean,
  3857|         "used_containment_b_in_a_min": used_containment_b_in_a_min,
  3858|         "used_n_shared_join_hash": used_n_shared_join_hash,
  3859|         "all_has_bundles_a": all_has_bundles_a,
  3860|         "all_has_bundles_b": all_has_bundles_b,
  3861|         "all_n_shared_bundle_both": str(all_n_shared_bundle_both),
  3862|         "all_n_shared_bundle_a_only": str(all_n_shared_bundle_a_only),
  3863|         "all_n_shared_bundle_b_only": str(all_n_shared_bundle_b_only),
  3864|         "used_has_bundles_a": used_has_bundles_a,
  3865|         "used_has_bundles_b": used_has_bundles_b,
  3866|         "used_n_shared_bundle_both": str(used_n_shared_bundle_both),
  3867|         "used_n_shared_bundle_a_only": str(used_n_shared_bundle_a_only),
  3868|         "used_n_shared_bundle_b_only": str(used_n_shared_bundle_b_only),
  3869|         "n_files_a": metrics.get("n_files_a", ""),
  3870|         "n_files_b": metrics.get("n_files_b", ""),
  3871|         "n_pairs": metrics.get("n_pairs", ""),
  3872|         "reference_usage_interpretable": _bool_str(_usage_interpretable_for_role(ma.get("governance_role", ""))),
  3873|         "target_usage_interpretable": _bool_str(_usage_interpretable_for_role(mb.get("governance_role", ""))),
  3874|         "recommended_primary_view": _recommended_primary_view(
  3875|             ma.get("governance_role", ""), mb.get("governance_role", ""), comparison_type
  3876|         ),
  3877|         "comparison_role_semantics": _comparison_role_semantics(
  3878|             ma.get("governance_role", ""), mb.get("governance_role", ""), comparison_type
  3879|         ),
  3880|         "executed_utc": executed_utc,
  3881|     }
  3882| 
  3883| 
  3884| def build_governance_state_outputs(
  3885|     policy: EnterprisePolicy,
  3886|     crid: str,
  3887|     seg_ref: str,
  3888|     seg_tgt: str,
  3889|     comparison_type: str,
  3890|     domain: str,
  3891|     manifest: Dict[str, Dict[str, str]],
  3892|     registry: Dict[str, Dict[str, str]],
  3893|     segments_root: Path,
  3894|     executed_utc: str,
  3895| ) -> Tuple[List[Dict[str, str]], Dict[str, str]]:
  3896|     """Emit directed governance rows over reference_all ∪ target_all.
  3897| 
  3898|     Reference all-view represents provisioned vocabulary. Target all-view is the
  3899|     configured downstream vocabulary. Target used-view is a governance/use signal
  3900|     only for Project targets; otherwise it is preserved as annotation.
  3901|     """
  3902|     ma = manifest.get(seg_ref, {})
  3903|     mb = manifest.get(seg_tgt, {})
  3904|     role_ref = ma.get("governance_role", "")
  3905|     role_tgt = mb.get("governance_role", "")
  3906|     unit_system = ma.get("unit_system", "")
  3907|     ref_usage_interpretable = _usage_interpretable_for_role(role_ref)
  3908|     tgt_usage_interpretable = _usage_interpretable_for_role(role_tgt)
  3909|     recommended_view = _recommended_primary_view(role_ref, role_tgt, comparison_type)
  3910| 
  3911|     ref_all = load_segment_join_hash_union(segments_root, registry, seg_ref, domain, "all")
  3912|     tgt_files_all = load_file_join_hashes(segments_root, registry, seg_tgt, domain, "all")
  3913|     tgt_files_used = load_file_join_hashes(segments_root, registry, seg_tgt, domain, "used")
  3914|     tgt_all: Set[str] = set()
  3915|     for jhs in tgt_files_all.values():
  3916|         tgt_all |= jhs
  3917|     tgt_used: Set[str] = set()
  3918|     for jhs in tgt_files_used.values():
  3919|         tgt_used |= jhs
  3920| 
  3921|     bnd_tgt_all = load_bundle_join_hash_set(segments_root, registry, seg_tgt, domain, "all")
  3922|     bnd_tgt_used = load_bundle_join_hash_set(segments_root, registry, seg_tgt, domain, "used")
  3923|     pattern_labels = load_pattern_labels(segments_root, registry, seg_tgt, domain)
  3924|     ref_labels = load_pattern_labels(segments_root, registry, seg_ref, domain)
  3925| 
  3926|     generic_set = get_role_jh_set("generic", domain, unit_system, manifest, registry, segments_root)
  3927|     template_set = get_role_jh_set("template", domain, unit_system, manifest, registry, segments_root)
  3928|     container_set = get_role_jh_set("container", domain, unit_system, manifest, registry, segments_root)
  3929| 
  3930|     n_tgt_all_files = len(tgt_files_all)
  3931|     n_tgt_used_files = len(tgt_files_used)
  3932|     rows: List[Dict[str, str]] = []
  3933|     state_counts: Dict[str, int] = defaultdict(int)
  3934| 
  3935|     for jh in sorted(ref_all | tgt_all):
  3936|         in_ref = jh in ref_all
  3937|         in_tgt_all = jh in tgt_all
  3938|         in_tgt_used = jh in tgt_used
  3939|         is_bnd_all = jh in bnd_tgt_all
  3940|         is_bnd_used = jh in bnd_tgt_used
  3941|         state = _classify_governance_state(
  3942|             in_ref, in_tgt_all, in_tgt_used, is_bnd_all, tgt_usage_interpretable
  3943|         )
  3944|         state_counts[state] += 1
  3945|         n_files_tgt_all = sum(1 for jhs in tgt_files_all.values() if jh in jhs)
  3946|         n_files_tgt_used = sum(1 for jhs in tgt_files_used.values() if jh in jhs)
  3947|         rows.append({
  3948|             "comparison_run_id": crid,
  3949|             "comparison_type": comparison_type,
  3950|             "segment_id_reference": seg_ref,
  3951|             "segment_id_target": seg_tgt,
  3952|             "segment_label_reference": ma.get("segment_label", ""),
  3953|             "segment_label_target": mb.get("segment_label", ""),
  3954|             "governance_role_reference": role_ref,
  3955|             "governance_role_target": role_tgt,
  3956|             "business_center_label_reference": _bc_of(ma),
  3957|             "business_center_label_target": _bc_of(mb),
  3958|             "unit_system": unit_system,
  3959|             "domain": domain,
  3960|             "join_hash": jh,
  3961|             "pattern_label": pattern_labels.get(jh, "") or ref_labels.get(jh, ""),
  3962|             "in_reference_all": _bool_str(in_ref),
  3963|             "in_target_all": _bool_str(in_tgt_all),
  3964|             "in_target_used": _bool_str(in_tgt_used),
  3965|             "state": state,
  3966|             "n_files_in_target_all": str(n_files_tgt_all),
  3967|             "pct_files_in_target_all": _fmt(n_files_tgt_all / n_tgt_all_files) if n_tgt_all_files else _fmt(0.0),
  3968|             "n_files_in_target_used": str(n_files_tgt_used),
  3969|             "pct_files_in_target_used": _fmt(n_files_tgt_used / n_tgt_used_files) if n_tgt_used_files else _fmt(0.0),
  3970|             "in_any_generic": _bool_str(jh in generic_set),
  3971|             "in_any_template": _bool_str(jh in template_set),
  3972|             "in_any_container": _bool_str(jh in container_set),
  3973|             "is_bundle_member_target_all": _bool_str(is_bnd_all),
  3974|             "is_bundle_member_target_used": _bool_str(is_bnd_used),
  3975|             "reference_usage_interpretable": _bool_str(ref_usage_interpretable),
  3976|             "target_usage_interpretable": _bool_str(tgt_usage_interpretable),
  3977|             "recommended_primary_view": recommended_view,
  3978|             "executed_utc": executed_utc,
  3979|         })
  3980| 
  3981|     ref_den = len(ref_all)
  3982|     tgt_all_den = len(tgt_all)
  3983|     tgt_used_den = len(tgt_used)
  3984|     provided_configured = len(ref_all & tgt_all)
  3985|     # Used-view governance summary metrics are active-delivery signals only for
  3986|     # Project targets. For Template/Generic/most Container targets, target_used is
  3987|     # retained as row-level annotation but not summarized as passive/active state.
  3988|     provided_used = len(ref_all & tgt_used) if tgt_usage_interpretable else 0
  3989|     provided_passive = len((ref_all & tgt_all) - tgt_used) if tgt_usage_interpretable else 0
  3990|     provided_missing = len(ref_all - tgt_all)
  3991|     local_active = len(tgt_used - ref_all) if tgt_usage_interpretable else 0
  3992| 
  3993|     summary = {
  3994|         "comparison_run_id": crid,
  3995|         "comparison_type": comparison_type,
  3996|         "segment_id_reference": seg_ref,
  3997|         "segment_id_target": seg_tgt,
  3998|         "segment_label_reference": ma.get("segment_label", ""),
  3999|         "segment_label_target": mb.get("segment_label", ""),
  4000|         "governance_role_reference": role_ref,
  4001|         "governance_role_target": role_tgt,
  4002|         "business_center_label_reference": _bc_of(ma),
  4003|         "business_center_label_target": _bc_of(mb),
  4004|         "unit_system": unit_system,
  4005|         "domain": domain,
  4006|         "reference_all_count": str(ref_den),
  4007|         "target_all_count": str(tgt_all_den),
  4008|         "target_used_count": str(tgt_used_den),
  4009|         "provided_to_configured_containment": _fmt(provided_configured / ref_den) if ref_den else "",
  4010|         "provided_to_used_containment": _fmt(provided_used / ref_den) if tgt_usage_interpretable and ref_den else "",
  4011|         "provided_passive_share": _fmt(provided_passive / ref_den) if tgt_usage_interpretable and ref_den else "",
  4012|         "provided_missing_share": _fmt(provided_missing / ref_den) if ref_den else "",
  4013|         "local_active_share": _fmt(local_active / tgt_used_den) if tgt_usage_interpretable and tgt_used_den else "",
  4014|         "provided_and_used_count": str(state_counts.get("provided_and_used", 0)),
  4015|         "provided_but_passive_count": str(state_counts.get("provided_but_passive", 0)),
  4016|         "provided_but_missing_count": str(state_counts.get("provided_but_missing", 0)),
  4017|         "local_active_count": str(state_counts.get("local_active", 0)),
  4018|         "local_passive_count": str(state_counts.get("local_passive", 0)),
  4019|         "local_unbundled_count": str(state_counts.get("local_unbundled", 0)),
  4020|         "provided_configured_count": str(state_counts.get("provided_configured", 0)),
  4021|         "local_configured_count": str(state_counts.get("local_configured", 0)),
  4022|         "provided_and_used_pct_of_reference_all": _fmt(state_counts.get("provided_and_used", 0) / ref_den) if tgt_usage_interpretable and ref_den else "",
  4023|         "provided_but_passive_pct_of_reference_all": _fmt(state_counts.get("provided_but_passive", 0) / ref_den) if tgt_usage_interpretable and ref_den else "",
  4024|         "provided_but_missing_pct_of_reference_all": _fmt(state_counts.get("provided_but_missing", 0) / ref_den) if ref_den else "",
  4025|         "local_active_pct_of_target_used": _fmt(state_counts.get("local_active", 0) / tgt_used_den) if tgt_usage_interpretable and tgt_used_den else "",
  4026|         "local_passive_pct_of_target_all": _fmt(state_counts.get("local_passive", 0) / tgt_all_den) if tgt_usage_interpretable and tgt_all_den else "",
  4027|         "local_unbundled_pct_of_target_all": _fmt(state_counts.get("local_unbundled", 0) / tgt_all_den) if tgt_all_den else "",
  4028|         "reference_usage_interpretable": _bool_str(ref_usage_interpretable),
  4029|         "target_usage_interpretable": _bool_str(tgt_usage_interpretable),
  4030|         "recommended_primary_view": recommended_view,
  4031|         "comparison_role_semantics": _comparison_role_semantics(role_ref, role_tgt, comparison_type),
  4032|         "executed_utc": executed_utc,
  4033|     }
  4034|     return rows, summary
  4035| 
  4036| 
  4037| # ---------------------------------------------------------------------------
  4038| # Pooled comparison
  4039| # ---------------------------------------------------------------------------
  4040| 
  4041| def _build_pooled_row(
  4042|     policy: EnterprisePolicy,
  4043|     focal_sid: str,
  4044|     pool_sids: List[str],
  4045|     domain: str,
  4046|     manifest: Dict[str, Dict[str, str]],
  4047|     registry: Dict[str, Dict[str, str]],
  4048|     segments_root: Path,
  4049|     min_patterns: int,
  4050|     executed_utc: str,
  4051|     pool_scope: str,
  4052|     pool_key_str: str,
  4053| ) -> Optional[Dict[str, str]]:
  4054|     """Compute one focal-vs-pool row. Shared across every pool_scope grain
  4055|     (parent_sibling, bc, client) — only pool membership and the reported
  4056|     pool_scope differ between grains; the containment/bundle math is
  4057|     identical."""
  4058|     focal_files = load_file_join_hashes(segments_root, registry, focal_sid, domain)
  4059|     focal_union: Set[str] = set()
  4060|     for jhs in focal_files.values():
  4061|         focal_union |= jhs
  4062| 
  4063|     # Aggregate pool files — key by (segment_id, export_run_id) so that
  4064|     # the same export_run_id appearing in two sibling segments is counted twice
  4065|     # rather than silently collapsed into one entry.
  4066|     pool_files_keyed: Dict[Tuple[str, str], Set[str]] = {}
  4067|     for pool_sid in pool_sids:
  4068|         pf = load_file_join_hashes(segments_root, registry, pool_sid, domain)
  4069|         for eid, jhs in pf.items():
  4070|             pool_files_keyed[(pool_sid, eid)] = jhs
  4071| 
  4072|     pool_union: Set[str] = set()
  4073|     for jhs in pool_files_keyed.values():
  4074|         pool_union |= jhs
  4075| 
  4076|     n_files_focal = len(focal_files)
  4077|     n_files_pool = len(pool_files_keyed)
  4078| 
  4079|     # Zero readable file inventory on either side -- emit a blocked row with
  4080|     # blank similarity fields (not a zero-valued one) instead of suppressing
  4081|     # it outright. See run_pair()'s equivalent short-circuit for rationale.
  4082|     if n_files_focal == 0 or n_files_pool == 0:
  4083|         mf_blocked = manifest.get(focal_sid, {})
  4084|         crid_blocked = make_comparison_run_id(focal_sid, f"pool_{pool_scope}_{pool_key_str}", executed_utc)
  4085|         # has_bundles_* is availability metadata (did bundle analysis
  4086|         # produce output for this side), not a similarity score -- compute
  4087|         # it per side even when blocked. The pool side is an aggregate of
  4088|         # every pool_sids member, same as the non-blocked path below; only
  4089|         # the shared-overlap bucket counts are meaningless when the focal
  4090|         # side has zero files, so those stay at 0.
  4091|         focal_bundle_all_blocked = load_bundle_join_hash_set(
  4092|             segments_root, registry, focal_sid, domain, "all"
  4093|         )
  4094|         focal_bundle_used_blocked = load_bundle_join_hash_set(
  4095|             segments_root, registry, focal_sid, domain, "used"
  4096|         )
  4097|         pool_bundle_all_blocked: Set[str] = set()
  4098|         pool_bundle_used_blocked: Set[str] = set()
  4099|         for pool_sid in pool_sids:
  4100|             pool_bundle_all_blocked |= load_bundle_join_hash_set(
  4101|                 segments_root, registry, pool_sid, domain, "all"
  4102|             )
  4103|             pool_bundle_used_blocked |= load_bundle_join_hash_set(
  4104|                 segments_root, registry, pool_sid, domain, "used"
  4105|             )
  4106|         blocked_row = {
  4107|             "comparison_run_id": crid_blocked,
  4108|             "segment_id": focal_sid,
  4109|             "segment_label": mf_blocked.get("segment_label", ""),
  4110|             "governance_role": mf_blocked.get("governance_role", ""),
  4111|             "client_label": mf_blocked.get("client_label", ""),
  4112|             "business_center_label": _bc_of(mf_blocked),
  4113|             "scope_level": _scope_level(mf_blocked, policy) or "",
  4114|             "unit_system": mf_blocked.get("unit_system", ""),
  4115|             "domain": domain,
  4116|             "pool_scope": pool_scope,
  4117|             "n_files_focal": str(n_files_focal),
  4118|             "n_files_pool": str(n_files_pool),
  4119|             "n_unique_patterns_focal": str(len(focal_union)),
  4120|             "n_unique_patterns_pool": str(len(pool_union)),
  4121|             "n_shared_join_hash": "",
  4122|             "signal_spread": "",
  4123|             "all_containment_focal_in_pool": "",
  4124|             "all_containment_pool_in_focal": "",
  4125|             "used_containment_focal_in_pool": "",
  4126|             "used_containment_pool_in_focal": "",
  4127|             "all_has_bundles_focal": "true" if focal_bundle_all_blocked else "false",
  4128|             "all_has_bundles_pool": "true" if pool_bundle_all_blocked else "false",
  4129|             "all_n_shared_bundle_both": "0",
  4130|             "all_n_shared_bundle_focal_only": "0",
  4131|             "all_n_shared_bundle_pool_only": "0",
  4132|             "used_has_bundles_focal": "true" if focal_bundle_used_blocked else "false",
  4133|             "used_has_bundles_pool": "true" if pool_bundle_used_blocked else "false",
  4134|             "used_n_shared_bundle_both": "0",
  4135|             "used_n_shared_bundle_focal_only": "0",
  4136|             "used_n_shared_bundle_pool_only": "0",
  4137|             "executed_utc": executed_utc,
  4138|         }
  4139|         blocked_row.update(_cardinality_fields(n_files_focal, n_files_pool))
  4140|         return blocked_row
  4141| 
  4142|     if len(focal_union) < min_patterns or len(pool_union) < min_patterns:
  4143|         return None
  4144| 
  4145|     shared = focal_union & pool_union
  4146|     n_shared = len(shared)
  4147|     n_focal_unique = len(focal_union)
  4148|     n_pool_unique = len(pool_union)
  4149| 
  4150|     c_focal_in_pool = n_shared / n_focal_unique if n_focal_unique else 0.0
  4151|     c_pool_in_focal = n_shared / n_pool_unique if n_pool_unique else 0.0
  4152| 
  4153|     # Used-view containment
  4154|     focal_files_used = load_file_join_hashes(
  4155|         segments_root, registry, focal_sid, domain, "used"
  4156|     )
  4157|     focal_union_used: Set[str] = set()
  4158|     for jhs in focal_files_used.values():
  4159|         focal_union_used |= jhs
  4160|     pool_files_used_keyed: Dict[Tuple[str, str], Set[str]] = {}
  4161|     for pool_sid in pool_sids:
  4162|         pf_u = load_file_join_hashes(
  4163|             segments_root, registry, pool_sid, domain, "used"
  4164|         )
  4165|         for eid, jhs in pf_u.items():
  4166|             pool_files_used_keyed[(pool_sid, eid)] = jhs
  4167|     pool_union_used: Set[str] = set()
  4168|     for jhs in pool_files_used_keyed.values():
  4169|         pool_union_used |= jhs
  4170|     shared_used = focal_union_used & pool_union_used
  4171|     used_c_focal_in_pool = (
  4172|         len(shared_used) / len(focal_union_used) if focal_union_used else 0.0
  4173|     )
  4174|     used_c_pool_in_focal = (
  4175|         len(shared_used) / len(pool_union_used) if pool_union_used else 0.0
  4176|     )
  4177| 
  4178|     # signal_spread: raw containment-asymmetry measure, same formula as
  4179|     # _build_summary_row; no interpretive banding applied here.
  4180|     _min_pu = min(n_focal_unique, n_pool_unique)
  4181|     _max_pu = max(n_focal_unique, n_pool_unique)
  4182|     if _min_pu > 0:
  4183|         _pooled_signal_spread = (n_shared / _min_pu) - (n_shared / _max_pu if _max_pu > 0 else 0.0)
  4184|         _pooled_signal_spread_str = f"{_pooled_signal_spread:.4f}"
  4185|     else:
  4186|         _pooled_signal_spread_str = ""
  4187| 
  4188|     # Bundle annotation — dual-view
  4189|     focal_bundle_all = load_bundle_join_hash_set(
  4190|         segments_root, registry, focal_sid, domain, "all"
  4191|     )
  4192|     focal_bundle_used = load_bundle_join_hash_set(
  4193|         segments_root, registry, focal_sid, domain, "used"
  4194|     )
  4195|     pool_bundle_all: Set[str] = set()
  4196|     pool_bundle_used: Set[str] = set()
  4197|     for pool_sid in pool_sids:
  4198|         pool_bundle_all |= load_bundle_join_hash_set(
  4199|             segments_root, registry, pool_sid, domain, "all"
  4200|         )
  4201|         pool_bundle_used |= load_bundle_join_hash_set(
  4202|             segments_root, registry, pool_sid, domain, "used"
  4203|         )
  4204| 
  4205|     all_has_bundles_focal = "true" if focal_bundle_all else "false"
  4206|     all_has_bundles_pool = "true" if pool_bundle_all else "false"
  4207|     used_has_bundles_focal = "true" if focal_bundle_used else "false"
  4208|     used_has_bundles_pool = "true" if pool_bundle_used else "false"
  4209| 
  4210|     n_both_all, n_focal_only_all, n_pool_only_all = annotate_bundle_overlap(
  4211|         shared, focal_bundle_all, pool_bundle_all
  4212|     )
  4213|     n_both_used, n_focal_only_used, n_pool_only_used = annotate_bundle_overlap(
  4214|         shared, focal_bundle_used, pool_bundle_used
  4215|     )
  4216| 
  4217|     mf = manifest.get(focal_sid, {})
  4218|     crid = make_comparison_run_id(focal_sid, f"pool_{pool_scope}_{pool_key_str}", executed_utc)
  4219| 
  4220|     row = {
  4221|         "comparison_run_id": crid,
  4222|         "segment_id": focal_sid,
  4223|         "segment_label": mf.get("segment_label", ""),
  4224|         "governance_role": mf.get("governance_role", ""),
  4225|         "client_label": mf.get("client_label", ""),
  4226|         "business_center_label": _bc_of(mf),
  4227|         "scope_level": _scope_level(mf, policy) or "",
  4228|         "unit_system": mf.get("unit_system", ""),
  4229|         "domain": domain,
  4230|         "pool_scope": pool_scope,
  4231|         "n_files_focal": str(n_files_focal),
  4232|         "n_files_pool": str(n_files_pool),
  4233|         "n_unique_patterns_focal": str(n_focal_unique),
  4234|         "n_unique_patterns_pool": str(n_pool_unique),
  4235|         "n_shared_join_hash": str(n_shared),
  4236|         "signal_spread": _pooled_signal_spread_str,
  4237|         "all_containment_focal_in_pool": _fmt(c_focal_in_pool),
  4238|         "all_containment_pool_in_focal": _fmt(c_pool_in_focal),
  4239|         "used_containment_focal_in_pool": _fmt(used_c_focal_in_pool),
  4240|         "used_containment_pool_in_focal": _fmt(used_c_pool_in_focal),
  4241|         "all_has_bundles_focal": all_has_bundles_focal,
  4242|         "all_has_bundles_pool": all_has_bundles_pool,
  4243|         "all_n_shared_bundle_both": str(n_both_all),
  4244|         "all_n_shared_bundle_focal_only": str(n_focal_only_all),
  4245|         "all_n_shared_bundle_pool_only": str(n_pool_only_all),
  4246|         "used_has_bundles_focal": used_has_bundles_focal,
  4247|         "used_has_bundles_pool": used_has_bundles_pool,
  4248|         "used_n_shared_bundle_both": str(n_both_used),
  4249|         "used_n_shared_bundle_focal_only": str(n_focal_only_used),
  4250|         "used_n_shared_bundle_pool_only": str(n_pool_only_used),
  4251|         "executed_utc": executed_utc,
  4252|     }
  4253|     row.update(_cardinality_fields(n_files_focal, n_files_pool))
  4254|     return row
  4255| 
  4256| 
```
