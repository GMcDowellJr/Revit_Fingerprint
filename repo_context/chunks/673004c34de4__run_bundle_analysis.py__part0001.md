# Chunk of tools/bundle_analysis/run_bundle_analysis.py

- Source relative path: `tools/bundle_analysis/run_bundle_analysis.py`
- Chunk: 1 of 3
- Original line range: 1-353
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _view_out_dir, _ensure_latent_purgeable, _emit_meta_scatter_thresholds, _load_purgeable_only_set, _run_pipeline_once, _run_step2_to_step7
- Source SHA-256: f78aca08e1415706021084b7fd5c84367e1d0625c9c323e2394dc61b67f02fa2
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| from __future__ import annotations
     2| 
     3| import argparse
     4| from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
     5| import csv
     6| import shutil
     7| import subprocess
     8| import sys
     9| import time
    10| from pathlib import Path
    11| from typing import Dict, List, Optional, Set, Tuple
    12| 
    13| if __package__ in (None, ""):
    14|     _THIS_DIR = Path(__file__).resolve().parent
    15|     if str(_THIS_DIR) not in sys.path:
    16|         sys.path.insert(0, str(_THIS_DIR))
    17|     from common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id, retry_fs_op
    18|     from step0_discover_populations import discover_populations
    19|     from step1_membership_matrix import build_membership_matrix
    20|     from step2_find_bundles import find_bundles_for_domain
    21|     from step2b_bundle_share_profile import build_bundle_share_profile
    22|     from step3_build_dag import build_dag_for_domain
    23|     from step4_difference_sets import emit_stub as emit_step4
    24|     from step5_classify_patterns import emit_stub as emit_step5
    25|     from step6_classify_files import emit_stub as emit_step6
    26|     from step7_overlap_report import emit_stub as emit_step7
    27|     from reference_bundle import load_and_validate
    28|     from step_compare import run_compare_for_domain
    29|     from placeholder_exclusions import compute_placeholder_exclusions
    30|     from jenks_utils import jenks_breaks
    31|     from name_projection_adapter import (
    32|         DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
    33|         emit_name_target_provenance,
    34|         stage_name_projection_analysis_dir,
    35|     )
    36| else:
    37|     from .common import SCHEMA_VERSION, atomic_write_csv, read_csv_rows, resolve_analysis_run_id, retry_fs_op
    38|     from .step0_discover_populations import discover_populations
    39|     from .step1_membership_matrix import build_membership_matrix
    40|     from .step2_find_bundles import find_bundles_for_domain
    41|     from .step2b_bundle_share_profile import build_bundle_share_profile
    42|     from .step3_build_dag import build_dag_for_domain
    43|     from .step4_difference_sets import emit_stub as emit_step4
    44|     from .step5_classify_patterns import emit_stub as emit_step5
    45|     from .step6_classify_files import emit_stub as emit_step6
    46|     from .step7_overlap_report import emit_stub as emit_step7
    47|     from .reference_bundle import load_and_validate
    48|     from .step_compare import run_compare_for_domain
    49|     from .placeholder_exclusions import compute_placeholder_exclusions
    50|     from ..jenks_utils import jenks_breaks
    51|     from .name_projection_adapter import (
    52|         DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,
    53|         emit_name_target_provenance,
    54|         stage_name_projection_analysis_dir,
    55|     )
    56| 
    57| TIMING_FIELDNAMES = ["schema_version", "analysis_run_id", "domain", "population_id", "step", "seconds"]
    58| TIMING_STEPS = ("step1", "step2", "step2b", "step3", "step4", "step5", "step6", "step7")
    59| ROLE_GROUP_ALIASES = {
    60|     "template-group": ["Generic", "Generic-Host", "Template"],
    61| }
    62| VALID_ROLES = {"Project", "Template", "Generic", "Generic-Host", "Container"}
    63| VALID_COMPARISON_TARGETS = {"config", "name", "both"}
    64| DEFAULT_NAME_KEY_PATTERNS_DIR = Path("Results_v21/name_key/patterns/name")
    65| 
    66| 
    67| def _view_out_dir(out_dir: Path, purge_view: str) -> Path:
    68|     """Return out_dir/all or out_dir/used."""
    69|     return out_dir / purge_view
    70| 
    71| 
    72| def _ensure_latent_purgeable(latent_purgeable_file: Path, records_dir: Path) -> Path:
    73|     """Run compute_latent_purgeable.py if latent_purgeable.csv does not exist."""
    74|     if latent_purgeable_file.exists():
    75|         print(f"[run] latent_purgeable.csv found at {latent_purgeable_file}")
    76|         return latent_purgeable_file
    77| 
    78|     print(f"[run] latent_purgeable.csv not found — running compute_latent_purgeable.py ...")
    79|     cmd = [
    80|         sys.executable,
    81|         str(Path(__file__).parent.parent / "compute_latent_purgeable.py"),
    82|         "--records-dir", str(records_dir),
    83|         "--out-file", str(latent_purgeable_file),
    84|     ]
    85|     subprocess.run(cmd, check=True)
    86|     if not latent_purgeable_file.exists():
    87|         raise RuntimeError(
    88|             f"compute_latent_purgeable.py completed but {latent_purgeable_file} was not created"
    89|         )
    90|     print(f"[run] latent_purgeable.csv written to {latent_purgeable_file}")
    91|     return latent_purgeable_file
    92| 
    93| 
    94| def _emit_meta_scatter_thresholds(out_dir: Path, run_id: str, domain_filter: str = "") -> None:
    95|     if domain_filter:
    96|         return
    97| 
    98|     rows: List[Dict[str, str]] = []
    99|     for dom_dir in sorted([p for p in out_dir.iterdir() if p.is_dir() and not p.name.startswith("_")], key=lambda p: p.name.lower()):
   100|         bundle_files = sorted(dom_dir.rglob("bundles.csv"))
   101|         scope_files = sorted(dom_dir.rglob("scope_registry.csv"))
   102|         if not bundle_files or not scope_files:
   103|             continue
   104|         bundle_rows = []
   105|         for p in bundle_files:
   106|             bundle_rows.extend(read_csv_rows(p))
   107|         scope_rows = []
   108|         for p in scope_files:
   109|             scope_rows.extend(read_csv_rows(p))
   110|         run_bundles = [r for r in bundle_rows if r.get("analysis_run_id", "") == run_id]
   111|         run_scopes = [r for r in scope_rows if r.get("analysis_run_id", "") == run_id]
   112|         if not run_scopes:
   113|             continue
   114|         bundle_count = len(run_bundles)
   115|         population_files = sum(int(r.get("files_in_scope", "0") or "0") for r in run_scopes)
   116|         top_alignment = 0.0
   117|         for r in run_bundles:
   118|             try:
   119|                 files_present = int(r.get("files_present", "0") or "0")
   120|                 files_total = int(r.get("files_total", "0") or "0")
   121|             except ValueError:
   122|                 continue
   123|             if files_total > 0:
   124|                 top_alignment = max(top_alignment, files_present / files_total)
   125|         bundle_density = (bundle_count / population_files) if population_files > 0 else 0.0
   126|         rows.append({"domain": dom_dir.name, "b_alignment_rate": f"{top_alignment:.6f}", "bundle_density": f"{bundle_density:.6f}"})
   127| 
   128|     axis_map = {
   129|         "alignment_rate": [float(r["b_alignment_rate"]) for r in rows if float(r["b_alignment_rate"]) > 0.0 and float(r["bundle_density"]) > 0.0],
   130|         "bundle_density": [float(r["bundle_density"]) for r in rows if float(r["b_alignment_rate"]) > 0.0 and float(r["bundle_density"]) > 0.0],
   131|     }
   132|     out_rows: List[Dict[str, str]] = []
   133|     for axis, values in axis_map.items():
   134|         breaks = jenks_breaks(values, n_classes=2) if values else []
   135|         break_value = breaks[0] if breaks else 0.0
   136|         out_rows.append(
   137|             {
   138|                 "analysis_run_id": run_id,
   139|                 "axis": axis,
   140|                 "break_value": f"{break_value:.4f}",
   141|                 "n_domains": str(len(values)),
   142|                 "input_min": f"{(min(values) if values else 0.0):.4f}",
   143|                 "input_max": f"{(max(values) if values else 0.0):.4f}",
   144|             }
   145|         )
   146|     atomic_write_csv(
   147|         out_dir / "meta_scatter_thresholds.csv",
   148|         ["analysis_run_id", "axis", "break_value", "n_domains", "input_min", "input_max"],
   149|         out_rows,
   150|     )
   151| 
   152| 
   153| def _load_purgeable_only_set(
   154|     latent_purgeable_file: Path,
   155| ) -> Set[Tuple[str, str, str]]:
   156|     """Read latent_purgeable.csv once and return the purgeable_only set.
   157| 
   158|     purgeable_only = rows where latent_purgeable=true AND the same
   159|     (export_run_id, domain, sig_hash) triple never appears as latent_purgeable!=true.
   160|     Matches the logic in step1_membership_matrix.py exactly.
   161|     """
   162|     used_set: Set[Tuple[str, str, str]] = set()
   163|     excluded_set: Set[Tuple[str, str, str]] = set()
   164|     for row in read_csv_rows(latent_purgeable_file):
   165|         eid = row.get("export_run_id", "").strip()
   166|         dom = row.get("domain", "").strip()
   167|         sig = row.get("sig_hash", "").strip()
   168|         lp  = row.get("latent_purgeable", "").strip().lower()
   169|         if not (eid and dom and sig):
   170|             continue
   171|         if lp != "true":
   172|             used_set.add((eid, dom, sig))
   173|         else:
   174|             excluded_set.add((eid, dom, sig))
   175|     result = excluded_set - used_set
   176|     print(f"[run] purgeable_only_set loaded: {len(result)} entries from {latent_purgeable_file.name}")
   177|     return result
   178| 
   179| 
   180| def _run_pipeline_once(
   181|     analysis_dir: Path,
   182|     work_out_dir: Path,
   183|     domain: str,
   184|     run_id: str,
   185|     min_support_count: int,
   186|     min_support_pct: float,
   187|     compute_share_profile: bool = False,
   188|     population_id: Optional[str] = None,
   189|     analysis_run_id: str = "",
   190|     population_registry_dir: Optional[Path] = None,
   191|     scope_key_filter: Optional[str] = None,
   192|     allowed_export_run_ids: Optional[Set[str]] = None,
   193|     purge_view: str = "all",
   194|     latent_purgeable_file: Optional[Path] = None,
   195|     purgeable_only_set: Optional[Set[Tuple[str, str, str]]] = None,
   196| ) -> Dict[str, object]:
   197|     total_bundles = 0
   198|     total_edges = 0
   199|     total_files_no_bundle = 0
   200| 
   201|     t0 = time.time()
   202|     build_membership_matrix(
   203|         analysis_dir,
   204|         work_out_dir,
   205|         domain,
   206|         run_id,
   207|         population_id,
   208|         population_registry_dir,
   209|         scope_key_filter,
   210|         allowed_export_run_ids,
   211|         purge_view,
   212|         latent_purgeable_file,
   213|         purgeable_only_set=purgeable_only_set,
   214|     )
   215|     t1 = time.time() - t0
   216|     print(f"[run] domain={domain} step1_seconds={t1:.3f}")
   217| 
   218|     t0 = time.time()
   219|     step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
   220|     total_bundles += step2.get("bundles", 0)
   221|     t2 = time.time() - t0
   222|     print(f"[run] domain={domain} step2_seconds={t2:.3f}")
   223| 
   224|     t2b = 0.0
   225|     if compute_share_profile:
   226|         t0 = time.time()
   227|         build_bundle_share_profile(
   228|             analysis_dir=analysis_dir,
   229|             domain_out_dir=work_out_dir / domain,
   230|             domain=domain,
   231|             analysis_run_id=run_id,
   232|             scope_key=scope_key_filter,
   233|         )
   234|         t2b = time.time() - t0
   235|         print(f"[run] domain={domain} step2b_seconds={t2b:.3f}")
   236| 
   237|     t0 = time.time()
   238|     step3 = build_dag_for_domain(work_out_dir, domain)
   239|     total_edges += step3.get("edges", 0)
   240|     t3 = time.time() - t0
   241|     print(f"[run] domain={domain} step3_seconds={t3:.3f}")
   242| 
   243|     t0 = time.time()
   244|     emit_step4(work_out_dir, domain)
   245|     t4 = time.time() - t0
   246|     print(f"[run] domain={domain} step4_seconds={t4:.3f}")
   247| 
   248|     t0 = time.time()
   249|     emit_step5(work_out_dir, domain)
   250|     t5 = time.time() - t0
   251|     print(f"[run] domain={domain} step5_seconds={t5:.3f}")
   252| 
   253|     t0 = time.time()
   254|     step6 = emit_step6(work_out_dir, domain)
   255|     total_files_no_bundle += step6.get("files_no_bundle", 0)
   256|     t6 = time.time() - t0
   257|     print(f"[run] domain={domain} step6_seconds={t6:.3f}")
   258| 
   259|     t0 = time.time()
   260|     emit_step7(work_out_dir, domain)
   261|     t7 = time.time() - t0
   262|     print(f"[run] domain={domain} step7_seconds={t7:.3f}")
   263| 
   264|     total = t1 + t2 + t2b + t3 + t4 + t5 + t6 + t7
   265|     print(
   266|         f"[timing] summary domain={domain} population_id={population_id or 'none'} "
   267|         f"step1={t1:.2f} step2={t2:.2f} step2b={t2b:.2f} step3={t3:.2f} step4={t4:.2f} "
   268|         f"step5={t5:.2f} step6={t6:.2f} step7={t7:.2f} total={total:.2f}"
   269|     )
   270| 
   271|     return {
   272|         "total_bundles_found": total_bundles,
   273|         "total_dag_edges": total_edges,
   274|         "files_with_no_bundle_match": total_files_no_bundle,
   275|         "step_times": {
   276|             "step1": t1,
   277|             "step2": t2,
   278|             "step2b": t2b,
   279|             "step3": t3,
   280|             "step4": t4,
   281|             "step5": t5,
   282|             "step6": t6,
   283|             "step7": t7,
   284|         },
   285|     }
   286| 
   287| 
   288| def _run_step2_to_step7(
   289|     analysis_dir: Path,
   290|     work_out_dir: Path,
   291|     domain: str,
   292|     min_support_count: int,
   293|     min_support_pct: float,
   294|     run_id: str,
   295|     compute_share_profile: bool = False,
   296| ) -> Dict[str, object]:
   297|     total_bundles = 0
   298|     total_edges = 0
   299|     total_files_no_bundle = 0
   300| 
   301|     t0 = time.time()
   302|     step2 = find_bundles_for_domain(work_out_dir, domain, min_support_count, min_support_pct)
   303|     total_bundles += step2.get("bundles", 0)
   304|     t2 = time.time() - t0
   305|     print(f"[run] domain={domain} step2_seconds={t2:.3f}")
   306| 
   307|     t2b = 0.0
   308|     if compute_share_profile:
   309|         t0 = time.time()
   310|         build_bundle_share_profile(
   311|             analysis_dir=analysis_dir,
   312|             domain_out_dir=work_out_dir / domain,
   313|             domain=domain,
   314|             analysis_run_id=run_id,
   315|         )
   316|         t2b = time.time() - t0
   317|         print(f"[run] domain={domain} step2b_seconds={t2b:.3f}")
   318| 
   319|     t0 = time.time()
   320|     step3 = build_dag_for_domain(work_out_dir, domain)
   321|     total_edges += step3.get("edges", 0)
   322|     t3 = time.time() - t0
   323|     print(f"[run] domain={domain} step3_seconds={t3:.3f}")
   324| 
   325|     t0 = time.time()
   326|     emit_step4(work_out_dir, domain)
   327|     t4 = time.time() - t0
   328|     print(f"[run] domain={domain} step4_seconds={t4:.3f}")
   329| 
   330|     t0 = time.time()
   331|     emit_step5(work_out_dir, domain)
   332|     t5 = time.time() - t0
   333|     print(f"[run] domain={domain} step5_seconds={t5:.3f}")
   334| 
   335|     t0 = time.time()
   336|     step6 = emit_step6(work_out_dir, domain)
   337|     total_files_no_bundle += step6.get("files_no_bundle", 0)
   338|     t6 = time.time() - t0
   339|     print(f"[run] domain={domain} step6_seconds={t6:.3f}")
   340| 
   341|     t0 = time.time()
   342|     emit_step7(work_out_dir, domain)
   343|     t7 = time.time() - t0
   344|     print(f"[run] domain={domain} step7_seconds={t7:.3f}")
   345| 
   346|     return {
   347|         "total_bundles_found": total_bundles,
   348|         "total_dag_edges": total_edges,
   349|         "files_with_no_bundle_match": total_files_no_bundle,
   350|         "step_times": {"step2": t2, "step2b": t2b, "step3": t3, "step4": t4, "step5": t5, "step6": t6, "step7": t7},
   351|     }
   352| 
   353| 
```
