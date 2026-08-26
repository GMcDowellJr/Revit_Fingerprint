# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 2 of 13
- Original line range: 499-982
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _classify_delta, _bool_str, _role_key, _is_generic_role, _role_matches, _usage_interpretable_for_role, _recommended_primary_view, _comparison_role_semantics, _classify_governance_state, load_manifest, load_registry, load_file_metadata, load_membership, validate_membership_against_manifest, load_comparison_registry, _segment_status_complete, build_comparison_registry_rows, comparison_is_stale, segment_output_dir, bundle_analysis_dir, domain_patterns_path, pattern_presence_file_path, _load_export_run_ids_for_segment, discover_domains_for_segment
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
   499| def _classify_delta(
   500|     in_any_container: bool,
   501|     in_any_template: bool,
   502|     is_bundle_member_all: bool,
   503|     is_bundle_member_used: bool,
   504| ) -> str:
   505|     """Classify a delta pattern by origin and active-use status.
   506| 
   507|     Classes:
   508|       passive_inherited   — pattern came from governance (container/template) but is
   509|                             not actively used in the target; pure configuration bloat
   510|       active_inherited    — came from governance AND is actively used in the target;
   511|                             target intentionally extends the governance vocabulary
   512|       locally_custom_active  — not from governance context, actively used; target has
   513|                                 its own patterns it is rendering
   514|       locally_custom_passive — not from governance, in all-view bundle but not used;
   515|                                 locally defined orphan
   516|       locally_custom_unbundled — not from governance, not in any bundle analysis;
   517|                                   raw local definition with no bundle data
   518|     """
   519|     from_governance = in_any_container or in_any_template
   520|     if from_governance:
   521|         if is_bundle_member_used:
   522|             return "active_inherited"
   523|         return "passive_inherited"
   524|     if is_bundle_member_used:
   525|         return "locally_custom_active"
   526|     if is_bundle_member_all:
   527|         return "locally_custom_passive"
   528|     return "locally_custom_unbundled"
   529| 
   530| 
   531| # ---------------------------------------------------------------------------
   532| # Governance-state semantics
   533| # ---------------------------------------------------------------------------
   534| 
   535| def _bool_str(value: bool) -> str:
   536|     return "true" if value else "false"
   537| 
   538| 
   539| def _role_key(role: str) -> str:
   540|     return role.strip().lower().replace("_", "-")
   541| 
   542| 
   543| def _is_generic_role(role: str) -> bool:
   544|     return _role_key(role) in GENERIC_ROLE_KEYS
   545| 
   546| 
   547| def _role_matches(row_role: str, wanted_role: str) -> bool:
   548|     if wanted_role == "generic":
   549|         return _is_generic_role(row_role)
   550|     return _role_key(row_role) == wanted_role
   551| 
   552| 
   553| def _usage_interpretable_for_role(role: str) -> bool:
   554|     # Used/non-purgeable is a delivery signal for project targets. Standards-carrier
   555|     # roles can still have used-view files, but those values are annotations only.
   556|     return _role_key(role) == "project"
   557| 
   558| 
   559| def _recommended_primary_view(role_a: str, role_b: str, comparison_type: str) -> str:
   560|     if comparison_type in ("sibling_projects", "cross_client") or _role_key(role_b) == "project":
   561|         return "used"
   562|     return "all"
   563| 
   564| 
   565| def _comparison_role_semantics(role_a: str, role_b: str, comparison_type: str) -> str:
   566|     if comparison_type in GOVERNANCE_STATE_DIRECTED_TYPES:
   567|         if _usage_interpretable_for_role(role_b):
   568|             return "directed_governance: reference all-view provides vocabulary; project target used-view is active delivery"
   569|         return "directed_governance: reference and target are provided-vocabulary inventories; all-view is primary"
   570|     if comparison_type == "sibling_projects":
   571|         return "sibling_projects: used-view is active practice; all-view is configured/inherited context"
   572|     if comparison_type == "cross_client":
   573|         return "cross_client: used-view is active practice; all-view is configured/inherited context (same semantics as sibling_projects, across clients rather than within one)"
   574|     if comparison_type == "sibling_templates":
   575|         return "sibling_templates: all-view is primary; used-view must not be interpreted as bloat"
   576|     if comparison_type == "sibling_containers":
   577|         return "sibling_containers: all-view is primary unless an external subtype establishes delivery use semantics"
   578|     if _is_generic_role(role_a) and _is_generic_role(role_b):
   579|         return "sibling_generic: all-view is primary; used-view is not meaningful"
   580|     return "all-view is configured vocabulary; used-view is meaningful primarily for Project targets"
   581| 
   582| 
   583| def _classify_governance_state(
   584|     in_reference_all: bool,
   585|     in_target_all: bool,
   586|     in_target_used: bool,
   587|     is_bundle_member_target_all: bool,
   588|     target_usage_interpretable: bool,
   589| ) -> str:
   590|     if target_usage_interpretable:
   591|         if in_reference_all and in_target_used:
   592|             return "provided_and_used"
   593|         if in_reference_all and in_target_all and not in_target_used:
   594|             return "provided_but_passive"
   595|         if in_reference_all and not in_target_all:
   596|             return "provided_but_missing"
   597|         if not in_reference_all and in_target_used:
   598|             return "local_active"
   599|         if not in_reference_all and in_target_all and is_bundle_member_target_all:
   600|             return "local_passive"
   601|         return "local_unbundled"
   602| 
   603|     # For Template, Generic, and most Container targets, avoid usage-judgment labels:
   604|     # configured stock is inventory, not passive bloat. Keep target_used as annotation.
   605|     if in_reference_all and in_target_all:
   606|         return "provided_configured"
   607|     if in_reference_all and not in_target_all:
   608|         return "provided_but_missing"
   609|     if in_target_all and is_bundle_member_target_all:
   610|         return "local_configured"
   611|     return "local_unbundled"
   612| 
   613| 
   614| # ---------------------------------------------------------------------------
   615| # Data loading
   616| # ---------------------------------------------------------------------------
   617| 
   618| def load_manifest(records_dir: Path) -> Dict[str, Dict[str, str]]:
   619|     path = records_dir / "segment_manifest.csv"
   620|     if not path.exists():
   621|         sys.exit(f"[error] segment_manifest.csv not found at {path}")
   622|     manifest: Dict[str, Dict[str, str]] = {}
   623|     for row in read_csv_rows(path):
   624|         sid = row["segment_id"]
   625|         prior = manifest.get(sid)
   626|         if prior is not None and prior != row:
   627|             sys.exit(
   628|                 f"[error] Blocked: segment_manifest.csv has conflicting rows for "
   629|                 f"segment_id={sid!r} — this file must not be trusted as an "
   630|                 "authoritative hierarchy until the duplicate is resolved"
   631|             )
   632|         manifest[sid] = row
   633|     return manifest
   634| 
   635| 
   636| def load_registry(records_dir: Path) -> Dict[str, Dict[str, str]]:
   637|     path = records_dir / "run_registry.csv"
   638|     if not path.exists():
   639|         sys.exit(f"[error] run_registry.csv not found at {path}")
   640|     return {row["segment_id"]: row for row in read_csv_rows(path)}
   641| 
   642| 
   643| def load_file_metadata(records_dir: Path) -> Dict[str, Dict[str, str]]:
   644|     path = records_dir / "file_metadata.csv"
   645|     if not path.exists():
   646|         print(f"[warn] file_metadata.csv not found at {path}", file=sys.stderr)
   647|         return {}
   648|     return {row["export_run_id"]: row for row in read_csv_rows(path)}
   649| 
   650| 
   651| def load_membership(records_dir: Path) -> Dict[str, Set[str]]:
   652|     """Load segment_membership.csv into segment_id -> real export_run_id set.
   653| 
   654|     This is the ground-truth population source for `population_containment`
   655|     (see _population_containment_map()) -- unlike segment_manifest.csv's
   656|     file_count/population_hash columns, membership rows carry the actual
   657|     member export_run_ids, which is what a subset-or-equal check needs.
   658|     Optional: absent on older/partial output directories, in which case
   659|     population_containment is simply unavailable (callers treat an empty
   660|     map as "no containment data", not an error) — the structural_ancestor
   661|     guard still functions independently.
   662|     """
   663|     path = records_dir / "segment_membership.csv"
   664|     if not path.exists():
   665|         print(f"[warn] segment_membership.csv not found at {path}", file=sys.stderr)
   666|         return {}
   667|     membership: Dict[str, Set[str]] = defaultdict(set)
   668|     for row in read_csv_rows(path):
   669|         sid = row.get("segment_id", "").strip()
   670|         eid = row.get("export_run_id", "").strip()
   671|         if sid and eid:
   672|             membership[sid].add(eid)
   673|     return dict(membership)
   674| 
   675| 
   676| def validate_membership_against_manifest(
   677|     manifest: Dict[str, Dict[str, str]],
   678|     membership: Dict[str, Set[str]],
   679| ) -> List[str]:
   680|     """Return one error string per segment_id where segment_membership.csv
   681|     disagrees with segment_manifest.csv's file_count/population_hash, OR
   682|     where a manifest segment expected to have members has none at all.
   683| 
   684|     Same check tools/run_segment_orchestrator.py's own
   685|     validate_membership_against_manifest() performs (adapted here for
   686|     segment_id -> Set[str] membership instead of List[str]). Guards against a
   687|     stale or mismatched segment_membership.csv — e.g. build_segment_
   688|     manifest.py interrupted after replacing segment_manifest.csv but before
   689|     replacing segment_membership.csv — silently driving population_
   690|     containment()'s subset/materiality checks off a population that no
   691|     longer matches what segment_manifest.csv itself describes for that
   692|     segment_id, which could either wrongly suppress a valid comparison pair
   693|     or wrongly retain one that should have been excluded.
   694| 
   695|     Two passes: the first (over `membership`) catches count/hash mismatches
   696|     for segments the sidecar DOES have rows for; the second (over
   697|     `manifest`) catches a segment build_segment_manifest.py's own
   698|     _build_membership_rows() guarantees a membership row for (file_count > 0)
   699|     but that's entirely ABSENT from `membership` — a truncated/partially
   700|     written sidecar, not just a stale one. Missing this second pass would
   701|     silently exclude that segment from every population_containment check
   702|     instead of flagging the sidecar as untrustworthy (Codex review finding
   703|     on PR #423): _population_containment_map()/_compute_containment_
   704|     thresholds() only iterate segment_ids present in `membership`, so an
   705|     entirely-missing entry doesn't raise a count/hash mismatch on its own --
   706|     it just silently drops out of consideration.
   707|     """
   708|     errors: List[str] = []
   709|     for sid, eids in membership.items():
   710|         mrow = manifest.get(sid)
   711|         if mrow is None:
   712|             continue
   713|         expected_count = (mrow.get("file_count") or "").strip()
   714|         if expected_count and str(len(eids)) != expected_count:
   715|             errors.append(
   716|                 f"segment={sid}: segment_membership.csv has {len(eids)} export_run_id(s) "
   717|                 f"but segment_manifest.csv file_count={expected_count}"
   718|             )
   719|             continue
   720|         expected_hash = (mrow.get("population_hash") or "").strip()
   721|         if expected_hash:
   722|             actual_hash = hashlib.sha1("|".join(sorted(eids)).encode()).hexdigest()
   723|             if actual_hash != expected_hash:
   724|                 errors.append(
   725|                     f"segment={sid}: segment_membership.csv population_hash={actual_hash} "
   726|                     f"does not match segment_manifest.csv population_hash={expected_hash}"
   727|                 )
   728| 
   729|     for sid, mrow in manifest.items():
   730|         if sid in membership:
   731|             continue
   732|         expected_count = (mrow.get("file_count") or "").strip()
   733|         if expected_count and expected_count != "0":
   734|             errors.append(
   735|                 f"segment={sid}: segment_manifest.csv file_count={expected_count} but "
   736|                 f"segment_membership.csv has no export_run_id rows for this segment at all"
   737|             )
   738|     return errors
   739| 
   740| 
   741| # ---------------------------------------------------------------------------
   742| # Comparison staleness registry
   743| #
   744| # compare_cross_segment.py has no cached results of its own — every invocation
   745| # recomputes whatever (pair × domain) work items it is given, from whatever is
   746| # currently on disk under segments/. comparison_registry.csv exists purely to
   747| # let a run-plan preview (--dry-run below) tell a caller *which* pair×domain
   748| # comparisons are worth recomputing, by recording each side's
   749| # population_hash/last_run_utc (from run_registry.csv) at the moment that
   750| # specific (pair, domain) was last actually computed. It is not consulted to
   751| # skip computation — a live run always recomputes every work item it is given.
   752| #
   753| # Keyed on (segment_id_a, segment_id_b, comparison_type, domain) — matching
   754| # the actual work granularity (work_items from build_pair_domain_work_items),
   755| # not just the pair. A --domain-scoped invocation only recomputes one domain
   756| # per pair; stamping at pair granularity would mark every other domain for
   757| # that pair "current" without having recomputed it, hiding real staleness in
   758| # a later --dry-run.
   759| #
   760| # The file is a full snapshot of this invocation only — never merged with a
   761| # prior comparison_registry.csv — matching every other output this tool
   762| # writes (cross_segment_summary.csv etc. are always a full atomic_write_csv
   763| # replace, never a merge). A --domain/--segment-scoped run sharing the same
   764| # --out-dir as an earlier full run already destroys those other domains'
   765| # output rows; carrying their old registry stamp forward would falsely claim
   766| # they are still current. Only (pair, domain) work items that actually
   767| # produced a persisted output row this run are written.
   768| # ---------------------------------------------------------------------------
   769| 
   770| ComparisonRegistryKey = Tuple[str, str, str, str]  # (seg_a, seg_b, comparison_type, domain)
   771| 
   772| 
   773| def load_comparison_registry(out_dir: Path) -> Dict[ComparisonRegistryKey, Dict[str, str]]:
   774|     path = out_dir / "comparison_registry.csv"
   775|     if not path.exists():
   776|         return {}
   777|     result: Dict[ComparisonRegistryKey, Dict[str, str]] = {}
   778|     for row in read_csv_rows(path):
   779|         key = (
   780|             row.get("segment_id_a", ""), row.get("segment_id_b", ""),
   781|             row.get("comparison_type", ""), row.get("domain", ""),
   782|         )
   783|         result[key] = row
   784|     return result
   785| 
   786| 
   787| def _segment_status_complete(registry: Dict[str, Dict[str, str]], segment_id: str) -> bool:
   788|     return registry.get(segment_id, {}).get("status", "").strip().lower() == "complete"
   789| 
   790| 
   791| def build_comparison_registry_rows(
   792|     completed_work_items: Sequence[Tuple[str, str, str, str]],
   793|     registry: Dict[str, Dict[str, str]],
   794|     computed_utc: str,
   795| ) -> List[Dict[str, str]]:
   796|     """Return comparison_registry.csv rows: a fresh stamp for every (pair,
   797|     domain) that actually produced output this run (`completed_work_items`)
   798|     where both sides' run_registry.csv status is "complete".
   799| 
   800|     Deliberately no carryover of prior comparison_registry.csv rows: every
   801|     other output this tool writes (cross_segment_summary.csv,
   802|     cross_segment_file_pairs.csv, ...) is a full atomic_write_csv replace from
   803|     only this invocation's rows, not a merge — a --domain/--segment-scoped run
   804|     sharing the same --out-dir as an earlier full run already destroys those
   805|     other domains'/pairs' output rows. Carrying their old comparison_registry
   806|     stamp forward would claim they are still "current" when the data backing
   807|     that claim no longer exists on disk. comparison_registry.csv must mirror
   808|     the same full-snapshot-of-this-run semantics, so a scoped run correctly
   809|     makes every non-recomputed (pair, domain) report as stale (no recorded
   810|     stamp) on the next --dry-run — matching reality.
   811| 
   812|     Only work items that actually produced a persisted output row are
   813|     included — `run_pair()`/`_run_pair_domain()` returning None (e.g. a domain
   814|     below --min-patterns, or a within-project pair with no eligible file
   815|     pairs) must not get a fresh "current" stamp for output that was never
   816|     written.
   817| 
   818|     A (pair, domain) is also excluded if either side's registry status is not
   819|     "complete". build_segment_manifest.py updates population_hash to reflect
   820|     a segment's new file population immediately on manifest rebuild, resetting
   821|     status to "pending" (and clearing last_run_utc) until the orchestrator
   822|     actually re-runs that segment — but its output folder on disk still holds
   823|     the OLD population's results until then. A compare run in that window
   824|     reads the stale on-disk data yet would otherwise get stamped with the
   825|     segment's already-updated (new) population_hash, so once the segment
   826|     finally reaches "complete" with that same hash, a later --dry-run would
   827|     wrongly report the pair as already current."""
   828|     rows: List[Dict[str, str]] = []
   829|     for a, b, ctype, dom in completed_work_items:
   830|         if not (_segment_status_complete(registry, a) and _segment_status_complete(registry, b)):
   831|             continue
   832|         rec_a = registry.get(a, {})
   833|         rec_b = registry.get(b, {})
   834|         rows.append({
   835|             "segment_id_a": a,
   836|             "segment_id_b": b,
   837|             "comparison_type": ctype,
   838|             "domain": dom,
   839|             "population_hash_a": rec_a.get("population_hash", ""),
   840|             "population_hash_b": rec_b.get("population_hash", ""),
   841|             "last_run_utc_a": rec_a.get("last_run_utc", ""),
   842|             "last_run_utc_b": rec_b.get("last_run_utc", ""),
   843|             "conformance_reference_mode": rec_a.get("conformance_reference_mode", "") or "latest",
   844|             "computed_utc": computed_utc,
   845|         })
   846|     return rows
   847| 
   848| 
   849| def comparison_is_stale(
   850|     seg_a: str,
   851|     seg_b: str,
   852|     comparison_type: str,
   853|     domain: str,
   854|     registry: Dict[str, Dict[str, str]],
   855|     comparison_registry: Dict[ComparisonRegistryKey, Dict[str, str]],
   856| ) -> bool:
   857|     """True if this (pair, domain) has never been computed, or either side's
   858|     population_hash/last_run_utc has moved since it was last computed —
   859|     including a Template/Container reference re-running and producing new
   860|     bundle output with the target's own population unchanged."""
   861|     prior = comparison_registry.get((seg_a, seg_b, comparison_type, domain))
   862|     if prior is None:
   863|         return True
   864|     rec_a = registry.get(seg_a, {})
   865|     rec_b = registry.get(seg_b, {})
   866|     if prior.get("population_hash_a", "") != rec_a.get("population_hash", ""):
   867|         return True
   868|     if prior.get("population_hash_b", "") != rec_b.get("population_hash", ""):
   869|         return True
   870|     if prior.get("last_run_utc_a", "") != rec_a.get("last_run_utc", ""):
   871|         return True
   872|     if prior.get("last_run_utc_b", "") != rec_b.get("last_run_utc", ""):
   873|         return True
   874|     return False
   875| 
   876| 
   877| # ---------------------------------------------------------------------------
   878| # Path resolution
   879| # ---------------------------------------------------------------------------
   880| 
   881| def segment_output_dir(
   882|     segments_root: Path,
   883|     registry: Dict[str, Dict[str, str]],
   884|     segment_id: str,
   885| ) -> Optional[Path]:
   886|     rec = registry.get(segment_id)
   887|     if rec is None:
   888|         return None
   889|     folder = rec.get("output_folder", "").strip()
   890|     if not folder:
   891|         return None
   892|     return segments_root / folder
   893| 
   894| 
   895| def bundle_analysis_dir(seg_out: Path, domain: str, purge_view: str = "all") -> Path:
   896|     return seg_out / "results" / "bundle_analysis" / purge_view / domain
   897| 
   898| 
   899| def domain_patterns_path(seg_out: Path) -> Path:
   900|     return seg_out / "results" / "analysis" / "domain_patterns.csv"
   901| 
   902| 
   903| def pattern_presence_file_path(seg_out: Path) -> Path:
   904|     return seg_out / "results" / "analysis" / "pattern_presence_file.csv"
   905| 
   906| 
   907| def _load_export_run_ids_for_segment(seg_out: Path) -> List[str]:
   908|     ids_path = seg_out / "export_run_ids.txt"
   909|     if not ids_path.exists():
   910|         return []
   911|     return [
   912|         line.strip()
   913|         for line in ids_path.read_text(encoding="utf-8").splitlines()
   914|         if line.strip()
   915|     ]
   916| 
   917| 
   918| # ---------------------------------------------------------------------------
   919| # Domain discovery
   920| # ---------------------------------------------------------------------------
   921| 
   922| def discover_domains_for_segment(
   923|     segments_root: Path,
   924|     registry: Dict[str, Dict[str, str]],
   925|     segment_id: str,
   926| ) -> Set[str]:
   927|     seg_out = segment_output_dir(segments_root, registry, segment_id)
   928|     if seg_out is None:
   929|         return set()
   930|     # Always prefer bundle all-view discovery — it remains the domain authority
   931|     # source for bundle-producing segments. Generic/reference segments, however,
   932|     # are provided-vocabulary sources and may only have analysis CSVs. In that
   933|     # case, discover their domains from analysis outputs so they can participate
   934|     # in containment/provision comparisons.
   935|     ba_root = seg_out / "results" / "bundle_analysis" / "all"
   936|     domains: Set[str] = set()
   937|     if ba_root.exists():
   938|         domains = {
   939|             p.name.strip()
   940|             for p in ba_root.iterdir()
   941|             if p.is_dir() and p.name.strip()
   942|         }
   943|     if domains:
   944|         return domains
   945| 
   946|     dp_path = domain_patterns_path(seg_out)
   947|     if dp_path.exists():
   948|         domains = {
   949|             row.get("domain", "").strip()
   950|             for row in read_csv_rows(dp_path)
   951|             if row.get("domain", "").strip()
   952|         }
   953|     if domains:
   954|         return domains
   955| 
   956|     presence_path = pattern_presence_file_path(seg_out)
   957|     if presence_path.exists():
   958|         domains = {
   959|             row.get("domain", "").strip()
   960|             for row in read_csv_rows(presence_path)
   961|             if row.get("domain", "").strip()
   962|         }
   963|     return domains
   964| 
   965| 
   966| # ---------------------------------------------------------------------------
   967| # join_hash resolution cache
   968| # ---------------------------------------------------------------------------
   969| 
   970| # Cache: (segment_id, domain) -> {pattern_id: join_hash}
   971| _jh_cache: Dict[Tuple[str, str], Dict[str, str]] = {}
   972| 
   973| # Cache: (segment_id, domain) -> {join_hash: human_label}
   974| _pattern_label_cache: Dict[Tuple[str, str], Dict[str, str]] = {}
   975| 
   976| # Cache: (governance_role, domain, unit_system, exclude_segment_id) -> Set[join_hash]
   977| _role_jh_cache: Dict[Tuple[str, str, str, str], Set[str]] = {}
   978| 
   979| # Cache: (segment_id, domain, purge_view) -> Set[join_hash]  (bundle members only)
   980| _bundle_jh_cache: Dict[Tuple[str, str, str], Set[str]] = {}
   981| 
   982| 
```
