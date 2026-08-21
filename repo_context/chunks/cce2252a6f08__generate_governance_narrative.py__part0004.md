# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 4 of 17
- Original line range: 1188-1517
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_cascade, build_cascade.mean_or_none, build_cascade._largest_scope_bucket
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: build_cascade
- Ends inside symbol: no

```
  1188|         elif ct == "generic_to_template" and _is_unscoped_segment(r, "a"):
  1189|             scope = _target_scope_label(r, "b")
  1190|             v = pf(_col(r, "containment_a_in_b_mean"))
  1191|             if v is not None:
  1192|                 gt_by_scope[dom][scope].append(v)
  1193|                 if scope == "enterprise":
  1194|                     gt[dom].append(v)
  1195|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1196|             if vu is not None:
  1197|                 gt_used_by_scope[dom][scope].append(vu)
  1198|                 if scope == "enterprise":
  1199|                     gt_used[dom].append(vu)
  1200| 
  1201|         elif ct == "generic_to_container" and _is_unscoped_segment(r, "a"):
  1202|             scope = _target_scope_label(r, "b")
  1203|             v = pf(_col(r, "containment_a_in_b_mean"))
  1204|             if v is not None:
  1205|                 gc_by_scope[dom][scope].append(v)
  1206|                 if scope == "enterprise":
  1207|                     gc[dom].append(v)
  1208|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1209|             if vu is not None:
  1210|                 gc_used_by_scope[dom][scope].append(vu)
  1211|                 if scope == "enterprise":
  1212|                     gc_used[dom].append(vu)
  1213| 
  1214|         elif ct == "generic_to_project" and _is_unscoped_segment(r, "a"):
  1215|             scope = _target_scope_label(r, "b")
  1216|             v = pf(_col(r, "containment_a_in_b_mean"))
  1217|             if v is not None:
  1218|                 gp_by_scope[dom][scope].append(v)
  1219|                 if scope == "enterprise":
  1220|                     gp[dom].append(v)
  1221|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1222|             if vu is not None:
  1223|                 gp_used_by_scope[dom][scope].append(vu)
  1224|                 if scope == "enterprise":
  1225|                     gp_used[dom].append(vu)
  1226| 
  1227|         # Group 3 — scope-level fan-out (enterprise/bc/client vs. Project, and
  1228|         # enterprise vs. bc/client). A different axis than the cascade stages above;
  1229|         # captured under new keys only. NOT rendered, tiered, or anomaly-detected in
  1230|         # this pass — pending a future business-center-section design decision.
  1231|         elif ct == "enterprise_to_project":
  1232|             v = pf(_col(r, "containment_a_in_b_mean"))
  1233|             if v is not None:
  1234|                 ep[dom].append(v)
  1235|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1236|             if vu is not None:
  1237|                 ep_used[dom].append(vu)
  1238| 
  1239|         elif ct == "bc_to_project":
  1240|             v = pf(_col(r, "containment_a_in_b_mean"))
  1241|             if v is not None:
  1242|                 bp[dom].append(v)
  1243|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1244|             if vu is not None:
  1245|                 bp_used[dom].append(vu)
  1246| 
  1247|         elif ct == "enterprise_to_bc":
  1248|             # b-side is always the real target business center here (a-side is
  1249|             # always the enterprise reference, per discover_governance_chain()).
  1250|             bc_label = r.get("business_center_label_b", "")
  1251|             v = pf(_col(r, "containment_a_in_b_mean"))
  1252|             if v is not None:
  1253|                 eb[dom].append(v)
  1254|                 if bc_label:
  1255|                     eb_by_bc[dom][bc_label].append(v)
  1256|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1257|             if vu is not None:
  1258|                 eb_used[dom].append(vu)
  1259|                 if bc_label:
  1260|                     eb_used_by_bc[dom][bc_label].append(vu)
  1261| 
  1262|         elif ct == "enterprise_to_client":
  1263|             v = pf(_col(r, "containment_a_in_b_mean"))
  1264|             if v is not None:
  1265|                 ec[dom].append(v)
  1266|             vu = pf(_col(r, "used_containment_a_in_b_mean"))
  1267|             if vu is not None:
  1268|                 ec_used[dom].append(vu)
  1269| 
  1270|         # Group 3b — bc_to_bc: symmetric peer comparison, not a directed reference->
  1271|         # target relationship, so (unlike Group 3's containment_a_in_b_mean) a
  1272|         # single containment_a_in_b reading would silently privilege whichever
  1273|         # business center's segment_id happened to sort first in discover_
  1274|         # governance_chain()'s combinations(sorted(sids), 2). Uses all_union_jaccard/
  1275|         # used_union_jaccard instead -- the population-similarity family
  1276|         # (compare_cross_segment.py's own module docstring: union metrics answer
  1277|         # "how similar are these two populations", directionless by construction --
  1278|         # exactly the peer-comparison question). No established precedent dictates
  1279|         # otherwise: sibling_templates/sibling_containers (the closer peer-comparison
  1280|         # analog to bc_to_bc than Group 1/3's directed types) are themselves
  1281|         # Group-4-excluded and unrendered anywhere in this file, and
  1282|         # compare_governance_populations.py's own same-role-peer bc_to_bc branch
  1283|         # computes Jaccard + containment side by side without designating either as
  1284|         # primary. bc_to_bc rows are symmetric (is_directed=False in
  1285|         # compare_cross_segment.py), so all_union_jaccard/used_union_jaccard are
  1286|         # unconditionally populated whenever the row isn't blocked.
  1287|         elif ct == "bc_to_bc":
  1288|             bc_a = r.get("business_center_label_a", "")
  1289|             bc_b = r.get("business_center_label_b", "")
  1290|             if bc_a and bc_b:
  1291|                 # Role-scoped key (PR2 fix): discover_governance_chain()'s
  1292|                 # by_role_bc grouping only ever pairs two BCs sharing the SAME
  1293|                 # role (governance_role_a == governance_role_b for every
  1294|                 # bc_to_bc row by construction), but a domain can independently
  1295|                 # apply to both Template and Container segments -- e.g. the
  1296|                 # same (bc_a, bc_b, domain) could have both a Template-role
  1297|                 # bc_to_bc row and a separate Container-role bc_to_bc row.
  1298|                 # Keying on (bc_a, bc_b) alone would silently average those two
  1299|                 # different-role readings together under one bucket; the role
  1300|                 # prefix keeps them apart, the same "shape isn't enough, keep
  1301|                 # real identity distinct" principle as bb's own bc-pair-identity
  1302|                 # comment above, one level more specific.
  1303|                 role = r.get("governance_role_a", "")
  1304|                 bc_pair = f"{role}::{bc_a}::{bc_b}"
  1305|                 v = pf(_col(r, "all_union_jaccard"))
  1306|                 if v is not None:
  1307|                     bb[dom][bc_pair].append(v)
  1308|                 vu = pf(_col(r, "used_union_jaccard"))
  1309|                 if vu is not None:
  1310|                     bb_used[dom][bc_pair].append(vu)
  1311| 
  1312|         # Group 4 — known, deliberately excluded from cascade (see
  1313|         # CASCADE_GROUP4_EXCLUDED_TYPES above for the reason behind each).
  1314|         elif ct in CASCADE_GROUP4_EXCLUDED_TYPES:
  1315|             pass
  1316| 
  1317|     # Coverage check: every comparison_type actually present in summary_rows must be
  1318|     # accounted for by name in one of the four groups above. This is the actual fix
  1319|     # for "future producer additions are invisible by default" (docs/
  1320|     # governance_narrative_scope_gap_audit.md A1) — an unrecognized type is a real
  1321|     # signal that either this dispatch or compare_cross_segment.py's vocabulary has
  1322|     # drifted, and must not be swallowed silently the way the old bare if/elif did.
  1323|     _known_comparison_types = (
  1324|         CASCADE_GROUP1_TYPES | CASCADE_GROUP2_TYPES | CASCADE_GROUP3_TYPES | CASCADE_GROUP3B_TYPES
  1325|         | set(CASCADE_GROUP4_EXCLUDED_TYPES.keys())
  1326|     )
  1327|     _warn_unrecognized_comparison_types(seen_comparison_types, _known_comparison_types, "build_cascade")
  1328| 
  1329|     # ── Bundle signal collection ──────────────────────────────────────────────
  1330|     # Dual-view schema (future):  all_n_shared_bundle_both / used_n_shared_bundle_both
  1331|     # Single-view schema (current): n_shared_bundle_both
  1332|     # We accumulate bundle_share = bundled_shared / total_shared for each view.
  1333|     # This measures what fraction of shared patterns are formally bundled (actively used).
  1334|     # For template_to_project rows only — that's where the governance signal lives.
  1335|     bundle_schema = detect_bundle_schema(summary_rows)
  1336| 
  1337|     # {domain: [bundle_share_all, ...]}  — fraction of shared patterns in all-view bundles
  1338|     bshare_all = defaultdict(list)
  1339|     # {domain: [bundle_share_used, ...]} — same for used-view bundles (dual schema only)
  1340|     bshare_used = defaultdict(list)
  1341|     # {domain: [passive_indicator, ...]} — drop from all to used, 0-1 (dual schema only)
  1342|     passive_indicator = defaultdict(list)
  1343| 
  1344|     for r in summary_rows:
  1345|         if r["comparison_type"] not in ("template_to_project", "parent_sibling_roles"):
  1346|             continue
  1347|         if not (_is_unscoped_segment(r, "a") and _is_unscoped_segment(r, "b")):
  1348|             continue
  1349|         dom = r["domain"]
  1350|         if dom in EXCLUDED_FROM_SCORING:
  1351|             continue
  1352|         ns = pf(_col(r, "n_shared_join_hash"))
  1353|         if not ns or ns == 0:
  1354|             continue
  1355| 
  1356|         if bundle_schema == "dual":
  1357|             nb_all = pf(_col(r, "all_n_shared_bundle_both"))
  1358|             nb_used = pf(_col(r, "used_n_shared_bundle_both"))
  1359|             if nb_all is not None:
  1360|                 share_all = nb_all / ns
  1361|                 bshare_all[dom].append(share_all)
  1362|             if nb_used is not None:
  1363|                 share_used = nb_used / ns
  1364|                 bshare_used[dom].append(share_used)
  1365|             if nb_all is not None and nb_used is not None and nb_all > 0:
  1366|                 passive_indicator[dom].append((nb_all - nb_used) / nb_all)
  1367|         elif bundle_schema == "single":
  1368|             nb = pf(_col(r, "all_n_shared_bundle_both"))
  1369|             if nb is not None:
  1370|                 bshare_all[dom].append(nb / ns)
  1371| 
  1372|     def mean_or_none(lst):
  1373|         return statistics.mean(lst) if lst else None
  1374| 
  1375|     def _largest_scope_bucket(buckets: dict):
  1376|         """Pick the (scope_pair, mean) backed by the most rows; deterministic
  1377|         tie-break on scope_pair name. (None, None) when buckets is empty.
  1378| 
  1379|         Used only for the container_to_project scoped fallback -- the fallback
  1380|         surfaces exactly one number, so ties must resolve the same way on every
  1381|         run rather than depend on dict/insertion order.
  1382|         """
  1383|         if not buckets:
  1384|             return None, None
  1385|         key = max(buckets, key=lambda k: (len(buckets[k]), k))
  1386|         return key, statistics.mean(buckets[key])
  1387| 
  1388|     result = {}
  1389|     all_domains = (
  1390|         set(tc) | set(cp) | set(tp) | set(xc) | set(wp_all) | set(tw)
  1391|         | set(gt) | set(gc) | set(gp)
  1392|         | set(gt_by_scope) | set(gc_by_scope) | set(gp_by_scope)
  1393|         | set(tc_by_scope) | set(cp_by_scope) | set(tp_by_scope)
  1394|         | set(ep) | set(bp) | set(eb) | set(ec)
  1395|         | set(bb)
  1396|     )
  1397|     for dom in all_domains:
  1398|         bs_all = mean_or_none(bshare_all[dom])
  1399|         bs_used = mean_or_none(bshare_used[dom])
  1400|         cp_mean = mean_or_none(cp[dom])
  1401|         # Only compute the scoped fallback when there's no enterprise::enterprise
  1402|         # evidence to report -- cp_scoped must never coexist with cp itself, or a
  1403|         # reader could mistake the fallback for a second, competing headline value.
  1404|         if cp_mean is None:
  1405|             cp_scoped_pair, cp_scoped_mean = _largest_scope_bucket(cp_by_scope_suff[dom])
  1406|         else:
  1407|             cp_scoped_pair, cp_scoped_mean = None, None
  1408| 
  1409|         # Passive inheritance indicator: prefer containment-based delta (more direct signal)
  1410|         # over bundle-density delta. All-view containment - used-view containment = passive floor.
  1411|         # Normalise by all-view score so 0 = no passive, 1 = all shared patterns are purgeable.
  1412|         tp_all_m = mean_or_none(tp[dom])
  1413|         tp_used_m = mean_or_none(tp_used[dom])
  1414|         if tp_all_m and tp_used_m is not None and tp_all_m > 0:
  1415|             pi_containment = (tp_all_m - tp_used_m) / tp_all_m
  1416|         else:
  1417|             pi_containment = None
  1418| 
  1419|         # Fall back to bundle-density delta if containment delta unavailable
  1420|         pi_bundle = mean_or_none(passive_indicator[dom])
  1421|         pi_mean = pi_containment if pi_containment is not None else pi_bundle
  1422| 
  1423|         result[dom] = {
  1424|             "tc": mean_or_none(tc[dom]),
  1425|             "cp": cp_mean,
  1426|             # Rollup-gap fix: best-available data_sufficient scoped bucket when
  1427|             # cp (enterprise::enterprise) is empty. Kept as its own pair of
  1428|             # fields rather than blended into "cp" so a reader can never mistake
  1429|             # scoped evidence for enterprise-level evidence (same posture as
  1430|             # _has_group1_bc_pooled_evidence()/TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE).
  1431|             "cp_scoped": cp_scoped_mean,
  1432|             "cp_scoped_pair": cp_scoped_pair,
  1433|             "tp": tp_all_m,
  1434|             "tp_used": tp_used_m,
  1435|             "tc_used": mean_or_none(tc_used[dom]),
  1436|             "cp_used": mean_or_none(cp_used[dom]),
  1437|             "xc": mean_or_none(xc[dom]),
  1438|             "xc_all": mean_or_none(xc_all[dom]),
  1439|             "wp_all": mean_or_none(wp_all[dom]),
  1440|             "wp_used": mean_or_none(wp_used[dom]),
  1441|             "wp_disc": {d: statistics.mean(v) for d, v in wp_disc[dom].items() if v},
  1442|             "tw": mean_or_none(tw[dom]),
  1443|             "wp_p10": wp_p10.get(dom),
  1444|             "wp_p90": wp_p90.get(dom),
  1445|             "wp_p10_source": wp_p10_source.get(dom, "none"),
  1446|             "wp_used_p10": wp_used_p10.get(dom),
  1447|             "wp_used_p90": wp_used_p90.get(dom),
  1448|             # Bundle/passive-inheritance signals
  1449|             "bundle_schema": bundle_schema,
  1450|             "bundle_share_all": bs_all,
  1451|             "bundle_share_used": bs_used,
  1452|             "passive_indicator": pi_mean,       # primary: containment delta, fallback: bundle delta
  1453|             "passive_indicator_method": "containment" if pi_containment is not None else ("bundle" if pi_bundle is not None else "none"),
  1454|             # Group 2 — generic->template/container/project containment (one level up
  1455|             # the cascade from tc/cp/tp)
  1456|             "gt": mean_or_none(gt[dom]),
  1457|             "gc": mean_or_none(gc[dom]),
  1458|             "gp": mean_or_none(gp[dom]),
  1459|             "gt_used": mean_or_none(gt_used[dom]),
  1460|             "gc_used": mean_or_none(gc_used[dom]),
  1461|             "gp_used": mean_or_none(gp_used[dom]),
  1462|             # Option C — per-target-scope-level breakdown, e.g. {"enterprise": 0.9,
  1463|             # "client": 0.4, "client_discipline": 0.3}. "enterprise" always equals
  1464|             # the "gt"/"gc"/"gp" value above (same source data); every other key is
  1465|             # scoped evidence that used to be silently discarded.
  1466|             "gt_by_scope": {s: statistics.mean(v) for s, v in gt_by_scope[dom].items() if v},
  1467|             "gc_by_scope": {s: statistics.mean(v) for s, v in gc_by_scope[dom].items() if v},
  1468|             "gp_by_scope": {s: statistics.mean(v) for s, v in gp_by_scope[dom].items() if v},
  1469|             "gt_used_by_scope": {s: statistics.mean(v) for s, v in gt_used_by_scope[dom].items() if v},
  1470|             "gc_used_by_scope": {s: statistics.mean(v) for s, v in gc_used_by_scope[dom].items() if v},
  1471|             "gp_used_by_scope": {s: statistics.mean(v) for s, v in gp_used_by_scope[dom].items() if v},
  1472|             # Group 1 bc-pooled fallback -- per-(scope_a, scope_b)-pair breakdown
  1473|             # mirroring Group 2's Option C above. "enterprise::enterprise" always
  1474|             # equals the tc/cp/tp value above (same source data); every other key
  1475|             # (typically "bc::bc") is scoped evidence that used to be silently
  1476|             # discarded. See docs/governance_narrative_group1_scope_gap_investigation.md.
  1477|             "tc_by_scope": {s: statistics.mean(v) for s, v in tc_by_scope[dom].items() if v},
  1478|             "cp_by_scope": {s: statistics.mean(v) for s, v in cp_by_scope[dom].items() if v},
  1479|             "tp_by_scope": {s: statistics.mean(v) for s, v in tp_by_scope[dom].items() if v},
  1480|             "tc_used_by_scope": {s: statistics.mean(v) for s, v in tc_used_by_scope[dom].items() if v},
  1481|             "cp_used_by_scope": {s: statistics.mean(v) for s, v in cp_used_by_scope[dom].items() if v},
  1482|             "tp_used_by_scope": {s: statistics.mean(v) for s, v in tp_used_by_scope[dom].items() if v},
  1483|             # Intra-bucket spread (min, max) for any scope_pair backed by >=2 rows --
  1484|             # lets detect_anomalies flag a pooled mean (typically "bc::bc") that hides
  1485|             # sharp per-business-center disagreement rather than genuine convergence.
  1486|             "tc_by_scope_spread": {s: (min(v), max(v)) for s, v in tc_by_scope[dom].items() if len(v) > 1},
  1487|             "cp_by_scope_spread": {s: (min(v), max(v)) for s, v in cp_by_scope[dom].items() if len(v) > 1},
  1488|             "tp_by_scope_spread": {s: (min(v), max(v)) for s, v in tp_by_scope[dom].items() if len(v) > 1},
  1489|             # Group 3 — scope-level fan-out containment. Captured only; NOT rendered,
  1490|             # tiered, or anomaly-detected in this pass — pending a future
  1491|             # business-center-section design decision (see
  1492|             # docs/governance_narrative_scope_gap_audit.md A1).
  1493|             "ep": mean_or_none(ep[dom]),
  1494|             "bp": mean_or_none(bp[dom]),
  1495|             "eb": mean_or_none(eb[dom]),
  1496|             "ec": mean_or_none(ec[dom]),
  1497|             "ep_used": mean_or_none(ep_used[dom]),
  1498|             "bp_used": mean_or_none(bp_used[dom]),
  1499|             "eb_used": mean_or_none(eb_used[dom]),
  1500|             "ec_used": mean_or_none(ec_used[dom]),
  1501|             # Group 3b — bc_to_bc peer comparison, {f"{bc_a}::{bc_b}": mean}. Captured
  1502|             # only; NOT rendered, tiered, or anomaly-detected in this pass (see
  1503|             # CASCADE_GROUP3B_TYPES above). No single enterprise-level scalar (unlike
  1504|             # ep/bp/eb/ec) because bc_to_bc has no enterprise-vs-scoped distinction --
  1505|             # every reading is already a scoped, real-BC-pair reading.
  1506|             "bb": {s: statistics.mean(v) for s, v in bb[dom].items() if v},
  1507|             "bb_used": {s: statistics.mean(v) for s, v in bb_used[dom].items() if v},
  1508|             # Per-BC breakouts (see build_bc_summary() / governance_bc_summary.csv).
  1509|             # Captured only here; not tiered/anomaly-detected/added to
  1510|             # governance_domain_summary.csv in this pass -- same posture as
  1511|             # eb/tc_by_scope themselves.
  1512|             "eb_by_bc": {s: statistics.mean(v) for s, v in eb_by_bc[dom].items() if v},
  1513|             "eb_used_by_bc": {s: statistics.mean(v) for s, v in eb_used_by_bc[dom].items() if v},
  1514|             "tc_bc_by_bc": {s: statistics.mean(v) for s, v in tc_bc_by_bc[dom].items() if v},
  1515|             "tc_used_bc_by_bc": {s: statistics.mean(v) for s, v in tc_used_bc_by_bc[dom].items() if v},
  1516|         }
  1517|     return result
```
