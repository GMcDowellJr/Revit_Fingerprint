# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 3 of 17
- Original line range: 788-1187
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_cascade
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: build_cascade

```
   788| def build_cascade(
   789|     summary_rows: list[dict],
   790|     sector_map: Optional[dict] = None,
   791|     segment_manifest: Optional[dict] = None,
   792| ) -> dict:
   793|     """
   794|     Returns domain-keyed dict with cascade scores from generic segments:
   795|       tc: template->container containment_a_in_b_mean
   796|       cp: container->project containment_a_in_b_mean
   797|       tp: template->project containment_a_in_b_mean
   798|       xc: cross-client Jaccard mean (clients whose sector is "healthcare" per
   799|         sector_map — see load_client_sectors())
   800|       wp_all: overall within-project Jaccard mean
   801|       wp_disc: {disc: mean_jaccard}
   802|       tw: within-template Jaccard
   803|       wp_p10: within-project Jaccard p10 (generic segment, all roles)
   804|       wp_p90: within-project Jaccard p90 (generic segment, all roles)
   805|       wp_p10_source: how the wp_p10/wp_p90 segment was obtained -- "enterprise" when
   806|         a==b and _is_unscoped_segment(r,"a") is directly true (the pre-existing
   807|         path), or "enterprise_resolved:<segment_id>" when the row's segment is not
   808|         itself unscoped but IS the segment build_segment_manifest.py's
   809|         redundant_single_child pass ultimately points the true unscoped root at
   810|         (same population_hash as the root -- see _resolve_runnable_segment() in
   811|         compare_cross_segment.py). Only computed when segment_manifest is supplied.
   812|         This is provenance only -- it does not change what score_reliability()
   813|         returns for the domain, since the resolved segment is population-identical
   814|         to the (never-discovered) root, not a narrower substitute.
   815|       gt/gc/gp: generic->template/container/project containment_a_in_b_mean, the
   816|         "enterprise" (target-unscoped) slice only (Group 2 — one level up the
   817|         cascade from tc/cp/tp; see CASCADE_GROUP2_TYPES)
   818|       gt_by_scope/gc_by_scope/gp_by_scope: {scope_label: mean_containment} for
   819|         EVERY target scope level compare_cross_segment.py emits (client/bc/
   820|         discipline and combinations thereof, plus "enterprise" itself) -- see
   821|         _target_scope_label(). Mirrors wp_disc's per-discipline breakdown pattern.
   822|       tc_by_scope/cp_by_scope/tp_by_scope: {scope_pair: mean_containment} for
   823|         EVERY (a-side scope, b-side scope) pair compare_cross_segment.py emits for
   824|         Group 1 (template_to_container/container_to_project/template_to_project),
   825|         keyed as f"{scope_a}::{scope_b}" (e.g. "enterprise::enterprise", "bc::bc") --
   826|         see _target_scope_label(). Unlike Group 2 (where only the target/b side is
   827|         classified, since the reference/a side is always gated to enterprise-only),
   828|         BOTH sides matter here, since neither side of a Group 1 pair is gated to a
   829|         fixed role population. tc/cp/tp themselves are UNCHANGED: still populated
   830|         only from the "enterprise::enterprise" pair (both sides pass
   831|         _is_unscoped_segment), matching today's behavior exactly. This mirrors
   832|         gt_by_scope/gc_by_scope/gp_by_scope's Option C precedent for the Group 1
   833|         gap documented in docs/governance_narrative_group1_scope_gap_investigation.md.
   834|       tc_by_scope_spread/cp_by_scope_spread/tp_by_scope_spread: {scope_pair:
   835|         (min, max)} for any scope_pair backed by >=2 rows -- lets detect_anomalies
   836|         flag a scope_pair whose pooled mean hides sharp disagreement between the
   837|         individual rows pooled into it, instead of only ever reporting the mean.
   838|         The varying dimension depends on which scope_pair fired: "bc::bc" pools
   839|         distinct business centers, but e.g. "client_bc::client_discipline" pools
   840|         rows that share the same client/bc and vary only by discipline -- see
   841|         detect_anomalies()'s note text, which is deliberately scope-neutral
   842|         rather than always saying "business-center."
   843|       ep/bp/eb/ec: enterprise->project / bc->project / enterprise->bc / enterprise->client
   844|         containment_a_in_b_mean (Group 3 — scope-level fan-out, captured but not yet
   845|         rendered/tiered/anomaly-detected; see CASCADE_GROUP3_TYPES)
   846|       cp_scoped/cp_scoped_pair: container_to_project rollup-gap fix. cp itself stays
   847|         gated to the "enterprise::enterprise" pair only (unchanged). But unlike
   848|         tc/tp, cp is essentially never populated in real corpora -- Project segments
   849|         are rarely fully unscoped -- while its non-enterprise evidence (cp_by_scope)
   850|         is real, sufficiently-populated signal that was previously computed and
   851|         then discarded before reaching governance_domain_summary.csv. cp_scoped is
   852|         the mean of the largest (most rows) scope_pair bucket in cp_by_scope that
   853|         (a) is not "enterprise::enterprise" (already covered by cp) and (b) is built
   854|         only from rows where n_files_a >= 5 and n_files_b >= 5 (the same threshold
   855|         compare_cross_segment.py used to compute as data_sufficient before that
   856|         field was removed in favor of downstream-owned interpretation).
   857|         cp_scoped_pair names which scope_pair that was. Both are None whenever cp
   858|         itself is non-None -- the scoped fallback only fires when there is no
   859|         enterprise-level evidence to report, so a reader can never mistake it for a
   860|         second, competing headline value. Populated from a SEPARATE accumulator
   861|         (cp_by_scope_suff), not a filtered view of cp_by_scope, so existing
   862|         cp_by_scope consumers (_has_group1_bc_pooled_evidence(),
   863|         render_group1_scope_section()) are unaffected.
   864|     """
   865|     tc = defaultdict(list)
   866|     cp = defaultdict(list)
   867|     tp = defaultdict(list)
   868|     xc = defaultdict(list)        # cross-client/sibling-project convergence, PRIMARY = used-view union
   869|     xc_all = defaultdict(list)    # same rows, all-view union (context: configured/inherited, not active practice)
   870|     wp_all = defaultdict(list)
   871|     wp_disc = defaultdict(lambda: defaultdict(list))
   872|     tw = defaultdict(list)
   873|     # p10/p90 from generic (all-role) within_project rows — most representative spread signal
   874|     wp_p10 = {}  # domain -> float
   875|     wp_p90 = {}
   876|     wp_p10_source = {}  # domain -> "enterprise" | "enterprise_resolved:<segment_id>"
   877|     # Cache of root_sid -> resolved segment_id (or None), so the same (unit_system,
   878|     # role) pair isn't re-resolved through the manifest for every domain's row.
   879|     _resolved_root_cache: dict = {}
   880|     # used-view cascade scores (dual-view schema only; fall back to all-view when absent)
   881|     tc_used = defaultdict(list)
   882|     cp_used = defaultdict(list)
   883|     tp_used = defaultdict(list)
   884|     wp_used = defaultdict(list)   # within-project used-view Jaccard
   885|     wp_used_p10 = {}
   886|     wp_used_p90 = {}
   887| 
   888|     # Group 1 bc-pooled fallback -- per-(a-scope, b-scope)-pair breakdown mirroring
   889|     # Group 2's Option C (gt_by_scope/etc.), see docs/governance_narrative_group1_scope_gap_investigation.md.
   890|     # tc/cp/tp themselves stay gated to the "enterprise::enterprise" pair only --
   891|     # unchanged from today.
   892|     tc_by_scope = defaultdict(lambda: defaultdict(list))
   893|     cp_by_scope = defaultdict(lambda: defaultdict(list))
   894|     tp_by_scope = defaultdict(lambda: defaultdict(list))
   895|     tc_used_by_scope = defaultdict(lambda: defaultdict(list))
   896|     cp_used_by_scope = defaultdict(lambda: defaultdict(list))
   897|     tp_used_by_scope = defaultdict(lambda: defaultdict(list))
   898| 
   899|     # container_to_project scoped fallback (governance_domain_summary.csv
   900|     # rollup gap fix) -- a SEPARATE accumulator from cp_by_scope above, not a
   901|     # filtered view of it, so _has_group1_bc_pooled_evidence()'s tier check and
   902|     # render_group1_scope_section()'s narrative (both existing cp_by_scope
   903|     # consumers) see byte-for-byte the same population as before this fix.
   904|     # Populated only from rows that are both (a) not the "enterprise::enterprise"
   905|     # pair (that evidence already surfaces via cp itself) and (b) pass
   906|     # n_files_a >= 5 and n_files_b >= 5 -- the same sufficiency threshold
   907|     # compare_cross_segment.py used to compute as data_sufficient for this
   908|     # comparison_type, which nothing in this rollup previously consulted.
   909|     cp_by_scope_suff = defaultdict(lambda: defaultdict(list))
   910| 
   911|     # Group 2 — generic->template/container/project containment (all-view + used-view).
   912|     # gt/gc/gp remain the "enterprise" scope-level slice only (Option A -- one clean
   913|     # number, matching tc/cp/tp; see the Scope decision comment on CASCADE_GROUP2_TYPES
   914|     # above), populated as a subset of the *_by_scope breakdowns below rather than a
   915|     # separately-gated accumulation, so the two can never drift apart.
   916|     gt = defaultdict(list)
   917|     gc = defaultdict(list)
   918|     gp = defaultdict(list)
   919|     gt_used = defaultdict(list)
   920|     gc_used = defaultdict(list)
   921|     gp_used = defaultdict(list)
   922|     # Option C — per-target-scope-level breakdown (mirrors wp_disc's per-discipline
   923|     # pattern): {dom: {scope_label: [values]}}. Captures the client-/bc-/discipline-
   924|     # scoped generic_to_* rows compare_cross_segment.py intentionally emits (see the
   925|     # Scope decision comment on CASCADE_GROUP2_TYPES above) instead of discarding
   926|     # them -- without blending them into the enterprise number itself.
   927|     gt_by_scope = defaultdict(lambda: defaultdict(list))
   928|     gc_by_scope = defaultdict(lambda: defaultdict(list))
   929|     gp_by_scope = defaultdict(lambda: defaultdict(list))
   930|     gt_used_by_scope = defaultdict(lambda: defaultdict(list))
   931|     gc_used_by_scope = defaultdict(lambda: defaultdict(list))
   932|     gp_used_by_scope = defaultdict(lambda: defaultdict(list))
   933| 
   934|     # Group 3 — scope-level fan-out containment (all-view + used-view). Captured only;
   935|     # not rendered/tiered/anomaly-detected in this pass (see CASCADE_GROUP3_TYPES above).
   936|     ep = defaultdict(list)
   937|     bp = defaultdict(list)
   938|     eb = defaultdict(list)
   939|     ec = defaultdict(list)
   940|     ep_used = defaultdict(list)
   941|     bp_used = defaultdict(list)
   942|     eb_used = defaultdict(list)
   943|     ec_used = defaultdict(list)
   944| 
   945|     # Group 3b — bc_to_bc peer comparison. Captured only; NOT rendered, tiered, or
   946|     # anomaly-detected in this pass (see CASCADE_GROUP3B_TYPES above). Keyed by
   947|     # the real, already-normalized (business_center_label_a, business_center_label_b)
   948|     # pair per domain -- not by scope shape -- because discover_governance_chain()
   949|     # already guarantees _bc_of(a) != _bc_of(b) at pair-discovery time (both sides
   950|     # are real, distinct business centers by construction: by_role_bc only admits
   951|     # rows with _scope_level(row) == "business_center", and the pairing loop itself
   952|     # skips any pair where _bc_of(a_row) == _bc_of(b_row)). A _group1_scope_pair()-
   953|     # style value-equality guard is therefore not needed at this layer; what IS
   954|     # still needed is preserving that per-pair identity here, since a-side/b-side
   955|     # here are symmetric peers, not a fixed reference/target -- pooling without the
   956|     # real pair key would conflate e.g. a (2270, Page) reading with a
   957|     # (2270, Vernon) reading under one shapeless bucket.
   958|     bb = defaultdict(lambda: defaultdict(list))
   959|     bb_used = defaultdict(lambda: defaultdict(list))
   960| 
   961|     # Per-BC breakout of Group 3's eb (enterprise_to_bc) capture, additive
   962|     # alongside the existing pooled eb[dom]/eb_used[dom] -- see
   963|     # build_bc_summary() / governance_bc_summary.csv. eb[dom]/eb_used[dom]
   964|     # stay untouched (byte-identical) for backward compatibility; this is a
   965|     # parallel accumulator keyed by the real target business_center_label
   966|     # (business_center_label_b -- enterprise_to_bc's a-side is always the
   967|     # enterprise reference, per discover_governance_chain()), not a
   968|     # replacement. Same pair-identity principle as bb/bb_used above, applied
   969|     # to an existing directed comparison_type instead of a new one.
   970|     eb_by_bc = defaultdict(lambda: defaultdict(list))
   971|     eb_used_by_bc = defaultdict(lambda: defaultdict(list))
   972| 
   973|     # Per-BC breakout of tc_by_scope["bc::bc"]. That pooled bucket already
   974|     # only contains genuine same-VALUE evidence (business_center_label_a ==
   975|     # business_center_label_b -- see _group1_scope_pair()'s docstring on why
   976|     # shape alone isn't enough), i.e. a real business center's own
   977|     # Template->Container reading -- but it pools EVERY business center's
   978|     # reading into one bucket, losing which specific BC each reading came
   979|     # from. tc_by_scope/tc_used_by_scope stay untouched; this is the same
   980|     # additive-parallel-accumulator fix as eb_by_bc above, applied to Group
   981|     # 1's scoped capture instead of Group 3's.
   982|     tc_bc_by_bc = defaultdict(lambda: defaultdict(list))
   983|     tc_used_bc_by_bc = defaultdict(lambda: defaultdict(list))
   984| 
   985|     seen_comparison_types: set = set()
   986|     sector_map = sector_map or {}
   987| 
   988|     for r in summary_rows:
   989|         ct = r["comparison_type"]
   990|         a, b = r["segment_id_a"], r["segment_id_b"]
   991|         dom = r["domain"]
   992|         seen_comparison_types.add(ct)
   993|         if dom in EXCLUDED_FROM_SCORING:
   994|             continue
   995| 
   996|         if ct == "template_to_container":
   997|             # Group 1 bc-pooled fallback: classify BOTH sides (unlike Group 2,
   998|             # neither side of a Group 1 pair is gated to a fixed role population)
   999|             # and bucket into every (scope_a, scope_b) pair observed, verifying
  1000|             # VALUE equality (not just shape equality) via _group1_scope_pair()
  1001|             # so a mismatched-value pair (e.g. BC_1 vs BC_2) never lands in the
  1002|             # same bucket as genuine same-value evidence. tc itself is promoted
  1003|             # only from "enterprise::enterprise" -- exactly the same condition
  1004|             # as today's _is_unscoped_segment(r,"a") and (r,"b") gate, since
  1005|             # _target_scope_label() returns "enterprise" iff
  1006|             # _is_unscoped_segment() is True for that side -- so tc is
  1007|             # byte-for-byte unchanged.
  1008|             scope_a, scope_b, scope_pair = _group1_scope_pair(r)
  1009|             v = pf(_col(r, "containment_a_in_b_mean"))
  1010|             if v is not None:
  1011|                 tc_by_scope[dom][scope_pair].append(v)
  1012|                 if scope_a == "enterprise" and scope_b == "enterprise":
  1013|                     tc[dom].append(v)
  1014|                 elif scope_pair == "bc::bc":
  1015|                     # Real value equality already verified by _group1_scope_pair()
  1016|                     # (scope_pair is "bc!cross::bc!cross" otherwise) -- side a's
  1017|                     # business_center_label equals side b's here by construction.
  1018|                     bc_label = r.get("business_center_label_a", "")
  1019|                     if bc_label:
  1020|                         tc_bc_by_bc[dom][bc_label].append(v)
  1021|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1022|             if vu is not None:
  1023|                 tc_used_by_scope[dom][scope_pair].append(vu)
  1024|                 if scope_a == "enterprise" and scope_b == "enterprise":
  1025|                     tc_used[dom].append(vu)
  1026|                 elif scope_pair == "bc::bc":
  1027|                     bc_label = r.get("business_center_label_a", "")
  1028|                     if bc_label:
  1029|                         tc_used_bc_by_bc[dom][bc_label].append(vu)
  1030| 
  1031|         elif ct == "container_to_project":
  1032|             scope_a, scope_b, scope_pair = _group1_scope_pair(r)
  1033|             v = pf(_col(r, "containment_a_in_b_mean"))
  1034|             if v is not None:
  1035|                 cp_by_scope[dom][scope_pair].append(v)
  1036|                 if scope_a == "enterprise" and scope_b == "enterprise":
  1037|                     cp[dom].append(v)
  1038|                 else:
  1039|                     # compare_cross_segment.py no longer emits data_sufficient;
  1040|                     # this is the same n_files_a/b >= 5 threshold it used to
  1041|                     # compute, now inlined at the point of use.
  1042|                     nfa = int(r["n_files_a"]) if r.get("n_files_a") else 0
  1043|                     nfb = int(r["n_files_b"]) if r.get("n_files_b") else 0
  1044|                     if nfa >= 5 and nfb >= 5:
  1045|                         cp_by_scope_suff[dom][scope_pair].append(v)
  1046|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1047|             if vu is not None:
  1048|                 cp_used_by_scope[dom][scope_pair].append(vu)
  1049|                 if scope_a == "enterprise" and scope_b == "enterprise":
  1050|                     cp_used[dom].append(vu)
  1051| 
  1052|         elif ct in ("template_to_project", "parent_sibling_roles"):
  1053|             scope_a, scope_b, scope_pair = _group1_scope_pair(r)
  1054|             v = pf(_col(r, "containment_a_in_b_mean"))
  1055|             if v is not None:
  1056|                 tp_by_scope[dom][scope_pair].append(v)
  1057|                 if scope_a == "enterprise" and scope_b == "enterprise":
  1058|                     tp[dom].append(v)
  1059|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1060|             if vu is not None:
  1061|                 tp_used_by_scope[dom][scope_pair].append(vu)
  1062|                 if scope_a == "enterprise" and scope_b == "enterprise":
  1063|                     tp_used[dom].append(vu)
  1064| 
  1065|         elif ct == "sibling_projects":
  1066|             ca = _pick(r, "client_label_a")
  1067|             cb = _pick(r, "client_label_b")
  1068|             # discover_sibling_segments() groups purely by (parent_segment_id,
  1069|             # governance_role, unit_system), so two DIFFERENTLY-scoped Project
  1070|             # segments under the SAME client (e.g. a client's discipline- or
  1071|             # collection-scoped siblings sharing a client-level parent) can pair
  1072|             # as sibling_projects with ca == cb -- a within-client comparison, not
  1073|             # cross-client convergence. The old segment_id-length==3 guard
  1074|             # incidentally excluded these (they render with >3 parts); requiring
  1075|             # distinct clients here is the direct, column-based replacement.
  1076|             if ca != cb and sector_map.get(ca) == "healthcare" and sector_map.get(cb) == "healthcare":
  1077|                 # Union metrics (see _recommended_primary_view() in compare_cross_segment.py):
  1078|                 # used-view is active practice for sibling_projects/cross_client, all-view is
  1079|                 # configured/inherited context -- v is PRIMARY (used), v_all is the secondary
  1080|                 # context value, not the other way around (opposite convention from tc/cp/tp).
  1081|                 v = pf(_col(r, "used_union_jaccard"))
  1082|                 if v is not None:
  1083|                     xc[dom].append(v)
  1084|                 v_all = pf(_col(r, "all_union_jaccard"))
  1085|                 if v_all is not None:
  1086|                     xc_all[dom].append(v_all)
  1087| 
  1088|         elif ct == "cross_client":
  1089|             # Purpose-built cross-client comparison (discover_cross_client() in
  1090|             # compare_cross_segment.py): each side is already that client's own
  1091|             # broadest (client-only-scoped) Project population, paired against
  1092|             # every OTHER client sharing the same unit_system -- no shared-parent
  1093|             # requirement (unlike sibling_projects). discover_cross_client()
  1094|             # itself has no hardcoded sector gate (it emits every client pair
  1095|             # into cross_segment_summary.csv, regardless of sector) -- but xc
  1096|             # is documented and consumed elsewhere as a healthcare-cohort
  1097|             # metric (see this function's own docstring, and the client-tier
  1098|             # "Non-comparable (different sector)" logic in
  1099|             # build_client_summary()), and sibling_projects's contribution to
  1100|             # this exact bucket is already gated to both-healthcare pairs. Gate
  1101|             # cross_client's contribution the same way for consistency, per
  1102|             # the original cross_client design note that sector filtering is
  1103|             # "left to downstream consumers" rather than baked into discovery.
  1104|             # ca != cb is defense-in-depth; discover_cross_client() only emits
  1105|             # distinct-client pairs by construction.
  1106|             ca = _pick(r, "client_label_a")
  1107|             cb = _pick(r, "client_label_b")
  1108|             if ca != cb and sector_map.get(ca) == "healthcare" and sector_map.get(cb) == "healthcare":
  1109|                 v = pf(_col(r, "used_union_jaccard"))
  1110|                 if v is not None:
  1111|                     xc[dom].append(v)
  1112|                 v_all = pf(_col(r, "all_union_jaccard"))
  1113|                 if v_all is not None:
  1114|                     xc_all[dom].append(v_all)
  1115| 
  1116|         elif ct == "within_project":
  1117|             # wp_all/wp_disc/wp_used are already a genuine all-view/used-view pair
  1118|             # (unlike xc above, which had no used companion before this change) --
  1119|             # swap the metric family from pairwise mean to union without changing
  1120|             # which side is "all" and which is "used"; passive_indicator's
  1121|             # (all - used) delta below depends on that assignment staying fixed.
  1122|             # Falls back to the pairwise mean when union is blank -- see
  1123|             # _col_union_or_pairwise()'s docstring: within_project rows never
  1124|             # get all_union_*/used_union_* from the producer at all.
  1125|             v = pf(_col_union_or_pairwise(r, "all_union_jaccard", "jaccard_mean"))
  1126|             disc = _pick(r, "discipline_label_a") or "all"
  1127|             if v is not None:
  1128|                 wp_disc[dom][disc].append(v)
  1129|                 wp_all[dom].append(v)
  1130|             vu = pf(_col_union_or_pairwise(r, "used_union_jaccard", "used_jaccard_mean"))
  1131|             if vu is not None:
  1132|                 wp_used[dom].append(vu)
  1133|             if a == b and r["governance_role_a"] == "Template" and v is not None:
  1134|                 tw[dom].append(v)
  1135|             # Capture p10/p90 for all-view and used-view from most inclusive generic segment.
  1136|             #
  1137|             # _is_unscoped_segment(r,"a") is the direct case: this row's own segment IS
  1138|             # the enterprise-wide root. Post business_center_label-promotion, that root is
  1139|             # frequently demoted to run_type="registration" by build_segment_manifest.py's
  1140|             # redundant_single_child pass whenever all its files sit in a single business
  1141|             # center (or client/discipline) -- and discover_within_project() in
  1142|             # compare_cross_segment.py (unlike discover_cross_client()/
  1143|             # discover_sibling_segments()/discover_parent_siblings(), fixed for the same
  1144|             # mechanism in PR #380) never resolves through the redundant chain, so no
  1145|             # within_project row for the root is ever emitted at all. When segment_manifest
  1146|             # is available, resolve the true root (f"{unit_system}|{role}") via
  1147|             # _resolve_runnable_segment() and accept this row as the substitute when it IS
  1148|             # that resolved segment -- guaranteed population_hash-identical to the root by
  1149|             # construction, so this is not narrower evidence, just a different segment_id
  1150|             # for the same population. wp_p10_source records which path fired, for
  1151|             # auditability only (score_reliability()'s meaning is unchanged either way).
  1152|             is_enterprise_root = _is_unscoped_segment(r, "a")
  1153|             is_resolved_enterprise_root = False
  1154|             if not is_enterprise_root and a == b and segment_manifest is not None:
  1155|                 root_sid = f"{r.get('unit_system', '')}|{r.get('governance_role_a', '')}"
  1156|                 if root_sid not in _resolved_root_cache:
  1157|                     _resolved_root_cache[root_sid] = _resolve_runnable_segment(segment_manifest, root_sid)
  1158|                 is_resolved_enterprise_root = _resolved_root_cache[root_sid] == a
  1159| 
  1160|             if a == b and (is_enterprise_root or is_resolved_enterprise_root):
  1161|                 n = int(r["n_files_a"]) if r.get("n_files_a") else 0
  1162|                 source = "enterprise" if is_enterprise_root else f"enterprise_resolved:{a}"
  1163|                 if _col(r, "jaccard_p10") and _col(r, "jaccard_p90"):
  1164|                     existing_n = wp_p10.get(dom + "_n", -1)
  1165|                     if n > existing_n:
  1166|                         wp_p10[dom] = pf(_col(r, "jaccard_p10"))
  1167|                         wp_p90[dom] = pf(_col(r, "jaccard_p90"))
  1168|                         wp_p10[dom + "_n"] = n
  1169|                         wp_p10_source[dom] = source
  1170|                 if _col(r, "used_jaccard_p10") and _col(r, "used_jaccard_p90"):
  1171|                     existing_n = wp_used_p10.get(dom + "_n", -1)
  1172|                     if n > existing_n:
  1173|                         wp_used_p10[dom] = pf(_col(r, "used_jaccard_p10"))
  1174|                         wp_used_p90[dom] = pf(_col(r, "used_jaccard_p90"))
  1175|                         wp_used_p10[dom + "_n"] = n
  1176| 
  1177|         # Group 2 — one level up the cascade from tc/cp/tp. The GENERIC (reference)
  1178|         # side must still be the one canonical enterprise-wide Generic population --
  1179|         # _is_unscoped_segment(r, "a") -- but the TARGET (Template/Container/Project)
  1180|         # side is bucketed by its own scope level (Option C) rather than gated to
  1181|         # broadest-only, since compare_cross_segment.py intentionally emits
  1182|         # generic_to_* rows for client-/bc-/discipline-scoped targets too (real
  1183|         # baseline-propagation evidence -- see the Scope decision comment on
  1184|         # CASCADE_GROUP2_TYPES above). gt/gc/gp (the enterprise slice) are populated
  1185|         # only when the target's own scope label is "enterprise", keeping today's
  1186|         # single clean number unchanged; every other scope level lands in
  1187|         # gt_by_scope/gc_by_scope/gp_by_scope instead of being discarded.
```
