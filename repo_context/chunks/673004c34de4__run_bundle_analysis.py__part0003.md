# Chunk of tools/bundle_analysis/run_bundle_analysis.py

- Source relative path: `tools/bundle_analysis/run_bundle_analysis.py`
- Chunk: 3 of 3
- Original line range: 898-1202
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _validate_name_target_constraints, run_bundle_analysis_for_target, _parse_args, main
- Source SHA-256: f78aca08e1415706021084b7fd5c84367e1d0625c9c323e2394dc61b67f02fa2
- Starts inside symbol: no
- Ends inside symbol: no

```
   898| 
   899| 
   900| def _validate_name_target_constraints(
   901|     comparison_target: str,
   902|     purge_view: str,
   903|     compute_share_profile: bool,
   904|     compare: bool,
   905| ) -> None:
   906|     """Fail loudly (never guess, never silently fall back) when a caller asks the
   907|     name-projection target for a feature that has no defined name-projection equivalent
   908|     yet. See DECISIONS.md D-037."""
   909|     if comparison_target not in ("name", "both"):
   910|         return
   911|     if purge_view != "all":
   912|         raise SystemExit(
   913|             f"--comparison-target {comparison_target} only supports --purge-view all. "
   914|             "USED-view purgeability filtering has no defined name-projection equivalent "
   915|             "(latent_purgeable.csv is sig_hash-keyed; name-projection patterns key off "
   916|             "join_key_name_identity's join_hash instead) -- pass --purge-view all "
   917|             "explicitly, or run comparison_target=config separately for USED-view output. "
   918|             "See DECISIONS.md D-037."
   919|         )
   920|     if compute_share_profile:
   921|         raise SystemExit(
   922|             f"--comparison-target {comparison_target} does not support "
   923|             "--compute-share-profile. pattern_share_pct/is_dominant_pattern have no "
   924|             "name-projection equivalent (PR2's pattern_membership.csv carries neither "
   925|             "field). See DECISIONS.md D-037."
   926|         )
   927|     if compare:
   928|         raise SystemExit(
   929|             f"--comparison-target {comparison_target} does not support --compare. No "
   930|             "name-projection reference-bundle baseline is defined yet, and resolving that "
   931|             "gap is explicitly out of scope for this PR. See "
   932|             "DECISIONS.md D-037."
   933|         )
   934| 
   935| 
   936| def run_bundle_analysis_for_target(
   937|     analysis_dir: Path,
   938|     out_dir: Path,
   939|     comparison_target: str = "config",
   940|     name_key_patterns_dir: Optional[Path] = None,
   941|     domain: str = "",
   942|     min_support_count: int = 3,
   943|     min_support_pct: float = 0.0,
   944|     analysis_run_id: str = "",
   945|     discover_populations_flag: bool = True,
   946|     min_population_size: int = 0,
   947|     max_population_overlap: float = 0.20,
   948|     min_population_jaccard: float = 0.30,
   949|     discovery_support_pct: float = 0.10,
   950|     compare: bool = False,
   951|     compute_share_profile: bool = False,
   952|     roles: Optional[List[str]] = None,
   953|     metadata_file: Optional[Path] = None,
   954|     purge_view: Optional[str] = None,
   955|     latent_purgeable_file: Optional[Path] = None,
   956|     workers: int = 4,
   957| ) -> Dict[str, Dict[str, int]]:
   958|     """Run bundle analysis for one or both join-basis projections
   959|     (`--comparison-target {config,name,both}`, PR3), namespacing name-target output under
   960|     its own subdirectory so it can never collide with or overwrite config-target output.
   961| 
   962|     The `config` leg (default, and the only leg run when `comparison_target="config"`) is a
   963|     direct, argument-for-argument passthrough to `run_bundle_analysis()` writing to `out_dir`
   964|     exactly as before this function existed -- byte-identical by construction, not by
   965|     convention, since it is literally the same function call.
   966| 
   967|     `purge_view=None` (the default -- distinct from an explicit choice, so this can be
   968|     target-aware) resolves to `"both"` for `comparison_target="config"` (unchanged from
   969|     `run_bundle_analysis()`'s own default) and to `"all"` for `comparison_target` in
   970|     `{"name", "both"}`, since ALL is the only view name-target supports (PR #389 review: the
   971|     old flat `"both"` default made `--comparison-target name` fail out of the box even
   972|     though the caller never asked for anything but ALL). An *explicit* `--purge-view
   973|     used`/`both` under `comparison_target` in `{"name", "both"}` still raises via
   974|     `_validate_name_target_constraints()` -- only the unset-default case is target-aware.
   975| 
   976|     The `name` leg stages `Results_v21/name_key/patterns/name/` (PR2's output) into the
   977|     exact `analysis_dir` shape `run_bundle_analysis()` already expects (see
   978|     `name_projection_adapter.py`), forces `--purge-view all` /
   979|     `--compute-share-profile=False` / `--compare=False` (validated up front -- see
   980|     `_validate_name_target_constraints`), and writes a `bundle_provenance.csv` +
   981|     `domain_coverage.csv` + `README.md` declaring `comparison_target`, `coverage_class`, and
   982|     the analysis-side-reconstruction provenance note for every bundle produced.
   983| 
   984|     The name leg's final output lands at `out_dir/name_all` (per-domain step0-step7 output,
   985|     `bundle_provenance.csv`, `domain_coverage.csv`, `README.md` -- everything the internal
   986|     `out_dir/name/all` staging path produced, relocated as the last step of this branch).
   987|     `out_dir/name_all` is cleared *before* staging even starts, not only after a fresh
   988|     tree is produced -- if staging/mining/provenance raises partway through, a prior
   989|     successful run's `name_all/` must not survive untouched and look like current output
   990|     to Power BI (PR review, #391). This flat, single-path-segment location matches the
   991|     existing Power BI model's
   992|     `pPurgeView` folder-splice convention (`<segment>\\results\\bundle_analysis\\
   993|     <pPurgeView>\\*_combined.csv`) so a report author can point `pPurgeView` at `name_all`
   994|     exactly the way they already point it at `all`/`used` today -- see the PR3 BI-output-
   995|     compatibility brief. `tools/run_segment_orchestrator.py`'s BI-merge step reads/writes
   996|     `*_combined.csv` directly under `out_dir/name_all`, then calls
   997|     `name_projection_adapter.annotate_name_target_combined_files()` to add
   998|     `comparison_target`/`coverage_class`/`provenance_note` columns to each one.
   999|     """
  1000|     if comparison_target not in VALID_COMPARISON_TARGETS:
  1001|         raise ValueError(f"--comparison-target must be one of {sorted(VALID_COMPARISON_TARGETS)}, got {comparison_target!r}")
  1002|     if purge_view is None:
  1003|         purge_view = "all" if comparison_target in ("name", "both") else "both"
  1004|     _validate_name_target_constraints(comparison_target, purge_view, compute_share_profile, compare)
  1005| 
  1006|     targets = ["config"] if comparison_target == "config" else (["name"] if comparison_target == "name" else ["config", "name"])
  1007|     results: Dict[str, Dict[str, int]] = {}
  1008| 
  1009|     if "config" in targets:
  1010|         config_out_dir = out_dir if comparison_target == "config" else out_dir / "config"
  1011|         print(f"[run_multi_target] comparison_target=config out_dir={config_out_dir}")
  1012|         results["config"] = run_bundle_analysis(
  1013|             analysis_dir=analysis_dir,
  1014|             out_dir=config_out_dir,
  1015|             domain=domain,
  1016|             min_support_count=min_support_count,
  1017|             min_support_pct=min_support_pct,
  1018|             analysis_run_id=analysis_run_id,
  1019|             discover_populations_flag=discover_populations_flag,
  1020|             min_population_size=min_population_size,
  1021|             max_population_overlap=max_population_overlap,
  1022|             min_population_jaccard=min_population_jaccard,
  1023|             discovery_support_pct=discovery_support_pct,
  1024|             compare=compare,
  1025|             compute_share_profile=compute_share_profile,
  1026|             roles=roles,
  1027|             metadata_file=metadata_file,
  1028|             purge_view=purge_view,
  1029|             latent_purgeable_file=latent_purgeable_file,
  1030|             workers=workers,
  1031|         )
  1032| 
  1033|     if "name" in targets:
  1034|         resolved_name_patterns_dir = name_key_patterns_dir or DEFAULT_NAME_KEY_PATTERNS_DIR
  1035|         name_run_id = analysis_run_id or DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID
  1036|         name_out_dir = out_dir / "name"
  1037|         staging_dir = name_out_dir / "_staging_analysis_input"
  1038| 
  1039|         # Clear any previous run's BI-facing output before starting regeneration, not only
  1040|         # after a fresh name/all source has been produced. Without this, a failure during
  1041|         # staging/mining/provenance below (raised before the relocation step near the end
  1042|         # of this branch is ever reached) would leave a prior successful run's name_all/
  1043|         # completely untouched -- Power BI would silently keep reading stale combined
  1044|         # files from an old run even though this run is marked failed upstream (PR review,
  1045|         # #391). Matches the same "never leave a misleading stale artifact" rationale as
  1046|         # the orchestrator's own pre-clean of the internal bundle_analysis/name/ directory.
  1047|         name_all_dir = out_dir / "name_all"
  1048|         if name_all_dir.exists():
  1049|             retry_fs_op(shutil.rmtree, str(name_all_dir))
  1050| 
  1051|         # A details-only export (no sibling *.index.json) keeps its *.details.json name as
  1052|         # its canonical export_run_id -- normalize_export_run_id() can't tell that apart
  1053|         # from a split-export file's raw name by string shape alone, and blindly rewriting
  1054|         # it produces an id that matches nothing real (PR #390 review). file_metadata.csv's
  1055|         # own export_run_id column is the corpus's real id set, so when --metadata-file is
  1056|         # available it resolves this correctly; without it, staging falls back to the
  1057|         # original blind-rewrite behavior (unchanged for callers with no metadata file).
  1058|         known_export_run_ids = None
  1059|         if metadata_file is not None and Path(metadata_file).is_file():
  1060|             known_export_run_ids = {
  1061|                 (row.get("export_run_id", "") or "").strip()
  1062|                 for row in read_csv_rows(Path(metadata_file))
  1063|                 if (row.get("export_run_id", "") or "").strip()
  1064|             }
  1065| 
  1066|         stage_stats = stage_name_projection_analysis_dir(
  1067|             name_patterns_dir=resolved_name_patterns_dir,
  1068|             staging_dir=staging_dir,
  1069|             analysis_run_id=name_run_id,
  1070|             known_export_run_ids=known_export_run_ids,
  1071|         )
  1072|         print(f"[run_multi_target] comparison_target=name staged={stage_stats} out_dir={name_out_dir}")
  1073| 
  1074|         results["name"] = run_bundle_analysis(
  1075|             analysis_dir=staging_dir,
  1076|             out_dir=name_out_dir,
  1077|             domain=domain,
  1078|             min_support_count=min_support_count,
  1079|             min_support_pct=min_support_pct,
  1080|             analysis_run_id=name_run_id,
  1081|             discover_populations_flag=discover_populations_flag,
  1082|             min_population_size=min_population_size,
  1083|             max_population_overlap=max_population_overlap,
  1084|             min_population_jaccard=min_population_jaccard,
  1085|             discovery_support_pct=discovery_support_pct,
  1086|             compare=False,
  1087|             compute_share_profile=False,
  1088|             roles=roles,
  1089|             metadata_file=metadata_file,
  1090|             purge_view="all",
  1091|             latent_purgeable_file=None,
  1092|             workers=workers,
  1093|         )
  1094| 
  1095|         provenance_stats = emit_name_target_provenance(
  1096|             view_out_dir=name_out_dir,
  1097|             name_patterns_dir=resolved_name_patterns_dir,
  1098|             analysis_run_id=name_run_id,
  1099|         )
  1100|         print(f"[run_multi_target] comparison_target=name provenance={provenance_stats}")
  1101| 
  1102|         # Relocate the completed ALL-view output to a flat out_dir/name_all directory.
  1103|         # name_out_dir/"all" (this function's own internal staging/namespacing shape) is
  1104|         # two path segments; the Power BI model's pPurgeView parameter splices in a single
  1105|         # segment (`<segment>\results\bundle_analysis\<pPurgeView>\*_combined.csv`), so the
  1106|         # BI-facing output must land at out_dir/name_all, not out_dir/name/all.
  1107|         # name_all_dir was already cleared of any stale prior run above, before staging
  1108|         # even started -- re-checked here defensively in case anything unexpected
  1109|         # recreated it in between. Guarded on name_all_source existing so a caller that
  1110|         # mocks run_bundle_analysis / emit_name_target_provenance out (as some tests do)
  1111|         # doesn't hit a missing-directory error here. Every mutating call goes through
  1112|         # retry_fs_op() -- a segments root synced by OneDrive (or similar) can transiently
  1113|         # lock a file/folder this function just finished writing (WinError 5 "Access is
  1114|         # denied"), and this is dozens of small per-domain files written and immediately
  1115|         # relocated in one pass.
  1116|         name_all_source = name_out_dir / "all"
  1117|         if name_all_source.is_dir():
  1118|             if name_all_dir.exists():
  1119|                 retry_fs_op(shutil.rmtree, str(name_all_dir))
  1120|             retry_fs_op(shutil.move, str(name_all_source), str(name_all_dir))
  1121|             for extra in ("bundle_provenance.csv", "domain_coverage.csv", "README.md"):
  1122|                 src = name_out_dir / extra
  1123|                 if src.is_file():
  1124|                     retry_fs_op(shutil.move, str(src), str(name_all_dir / extra))
  1125| 
  1126|     return results
  1127| 
  1128| 
  1129| def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
  1130|     p = argparse.ArgumentParser(description="Run bundle analysis pipeline")
  1131|     p.add_argument("--analysis-dir", required=True, type=Path)
  1132|     p.add_argument("--out-dir", required=True, type=Path)
  1133|     p.add_argument(
  1134|         "--comparison-target", choices=sorted(VALID_COMPARISON_TARGETS), default="config",
  1135|         help="Which join-basis projection to run bundle analysis against: the existing "
  1136|              "configuration join_hash (config, default -- unchanged behavior/output), "
  1137|              "PR2's Canonical Name Identity Projection output (name, ALL view only), or "
  1138|              "both, namespaced separately under --out-dir/config and --out-dir/name_all.",
  1139|     )
  1140|     p.add_argument(
  1141|         "--name-key-patterns-dir", type=Path, default=None,
  1142|         help="Directory containing PR2's name-target domain_patterns.csv/"
  1143|              "pattern_membership.csv/domain_coverage.csv (default: "
  1144|              "Results_v21/name_key/patterns/name). Only used when --comparison-target is "
  1145|              "name or both.",
  1146|     )
  1147|     p.add_argument("--domain", default="")
  1148|     p.add_argument("--analysis-run-id", default="")
  1149|     p.add_argument("--min-support-count", type=int, default=3)
  1150|     p.add_argument("--min-support-pct", type=float, default=0.0)
  1151|     p.add_argument("--no-discover-populations", dest="discover_populations", action="store_false")
  1152|     p.set_defaults(discover_populations=True)
  1153|     p.add_argument("--min-population-size", type=int, default=0)
  1154|     p.add_argument("--max-population-overlap", type=float, default=0.20)
  1155|     p.add_argument("--min-population-jaccard", type=float, default=0.30)
  1156|     p.add_argument("--discovery-support-pct", type=float, default=0.10)
  1157|     p.add_argument("--compare", action="store_true")
  1158|     p.add_argument("--compute-share-profile", action="store_true")
  1159|     p.add_argument("--metadata-file", type=Path, default=None, help="Path to file_metadata.csv. Required when --roles is used.")
  1160|     p.add_argument("--roles", nargs="+", default=None, help="Governance roles: Project Template Generic Generic-Host Container, or alias template-group")
  1161|     p.add_argument(
  1162|         "--purge-view", choices=["all", "used", "both"], default=None,
  1163|         help="Default: both for --comparison-target config (unchanged); all for "
  1164|              "name/both, since ALL is the only view name-target supports. An explicit "
  1165|              "used/both under name/both still errors -- only the unset default is "
  1166|              "target-aware.",
  1167|     )
  1168|     p.add_argument("--latent-purgeable-file", type=Path, default=None, help="Path to latent_purgeable.csv")
  1169|     p.add_argument("--workers", type=int, default=4,
  1170|                    help="Max parallel domains for bundle analysis (default: 4)")
  1171|     return p.parse_args(argv)
  1172| 
  1173| 
  1174| def main(argv: Optional[List[str]] = None) -> int:
  1175|     args = _parse_args(argv)
  1176|     run_bundle_analysis_for_target(
  1177|         analysis_dir=args.analysis_dir,
  1178|         out_dir=args.out_dir,
  1179|         comparison_target=args.comparison_target,
  1180|         name_key_patterns_dir=args.name_key_patterns_dir,
  1181|         domain=args.domain,
  1182|         min_support_count=args.min_support_count,
  1183|         min_support_pct=args.min_support_pct,
  1184|         analysis_run_id=args.analysis_run_id,
  1185|         discover_populations_flag=args.discover_populations,
  1186|         min_population_size=args.min_population_size,
  1187|         max_population_overlap=args.max_population_overlap,
  1188|         min_population_jaccard=args.min_population_jaccard,
  1189|         discovery_support_pct=args.discovery_support_pct,
  1190|         compare=args.compare,
  1191|         compute_share_profile=args.compute_share_profile,
  1192|         roles=args.roles,
  1193|         metadata_file=args.metadata_file,
  1194|         purge_view=args.purge_view,
  1195|         latent_purgeable_file=args.latent_purgeable_file,
  1196|         workers=args.workers,
  1197|     )
  1198|     return 0
  1199| 
  1200| 
  1201| if __name__ == "__main__":
  1202|     raise SystemExit(main())
```
