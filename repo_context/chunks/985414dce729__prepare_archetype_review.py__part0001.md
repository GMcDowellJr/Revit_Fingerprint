# Chunk of tools/archetype/prepare_archetype_review.py

- Source relative path: `tools/archetype/prepare_archetype_review.py`
- Chunk: 1 of 3
- Original line range: 1-520
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _find_cluster, _all_clusters, _all_cluster_ids, _resolve_param_name, _parse_category_ids, _resolve_category_name, _governance_question_from_cluster_id, _governance_question_from_archetype_id, _build_curated_gq_map, _resolve_governance_question, ClusterContext, ClusterContext.__init__, _build_cluster_context, _load_label_lookup, _load_vfd_resolution, _load_file_path_lookup, _is_named_element, _schedule_file_sort_key, _schedule_row_sort_key
- Source SHA-256: 03bdf22e06a40e3b31dd69dea0931eb1695547b7a49d5dbbd3ada414575c6244
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| #!/usr/bin/env python3
     2| """Build human-reviewable drill-down tables for archetype signal clusters.
     3| 
     4| Inputs:
     5|   - Fingerprint_Out/archetype_analysis/signal_clusters.json
     6|   - Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv
     7|   - Fingerprint_Out/archetype_analysis/archetype_classifications.csv
     8|   - Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv
     9|   - results/records/records.csv
    10|   - results/records/file_metadata.csv
    11|   - results/records/identity_items_by_domain/view_filter_definitions.csv (optional)
    12|   - tools/archetype/bip_lookup.json (optional)
    13|   - tools/archetype/shared_param_names.json (optional)
    14|   - tools/archetype/vfd_category_domain_map.json
    15| 
    16| Output:
    17|   - <out>/review_<cluster_id>.csv -- one file per cluster processed, all in
    18|     a single directory. <out> defaults to
    19|     Fingerprint_Out/archetype_analysis/archetype_review.
    20|   - <out>/archetype_review_schedule.csv -- manual file-open schedule rows,
    21|     using final review_<cluster_id>.csv files for cluster-level name diagnostics
    22|     and enriched candidate rows for Project-file selection when available.
    23|   - <out>/archetype_review_gaps.csv -- clusters with no usable
    24|     detail-backed review rows.
    25| 
    26| Processing:
    27|   For each target cluster, assemble one row per (export_run_id, signal_id)
    28|   carrying the file path, the human-readable Revit element name, and (for
    29|   view_filter_definitions-sourced signals) resolved parameter and category
    30|   names -- everything a reviewer needs to open a specific file and navigate
    31|   directly to the named filter.
    32| 
    33|   If --cluster-id is omitted, every cluster in signal_clusters.json is
    34|   processed and written to its own <out>/review_<cluster_id>.csv file
    35|   (all in the same directory); a condensed one-line-per-cluster summary is
    36|   printed unless --verbose is given. If --cluster-id is given, only that
    37|   cluster is processed and a verbose per-file summary is printed.
    38| 
    39|   Stage 1: Resolve target cluster(s) from signal_clusters.json (clusters are
    40|     keyed by governance_question; cluster_id is unique across the document).
    41|   Stage 2: Find qualifying files from archetype_cluster_classifications.csv
    42|     (rows where cluster_id == target).
    43|   Stage 3: Get (export_run_id, signal_id) -> sig_hash/source_domain from
    44|     archetype_validation_detail.csv, restricted to qualifying files, the
    45|     cluster's signal_ids (matched by edge_id; see Stage 3 below), and the
    46|     archetype_ids that belong to the cluster's governance_question (resolved
    47|     via archetype_classifications.csv / archetype_id naming, the same way
    48|     cluster_archetype_signals.py does). This prevents an edge_id that is
    49|     promoted under more than one governance question/archetype -- each with
    50|     its own join_hash filter -- from leaking unrelated detail rows into this
    51|     cluster's review.
    52|   Stage 4: Stream records.csv (once, across all clusters being processed) to
    53|     resolve sig_hash -> label_display (element_name).
    54|   Stage 5: For view_filter_definitions-sourced signals, resolve parameter
    55|     names (via vf.rule[*].param_ref.id + bip_lookup.json /
    56|     shared_param_names.json) and category names (via vf.categories +
    57|     vfd_category_domain_map.json) from the identity_items shard.
    58|   Stage 6: Resolve export_run_id -> file_path from file_metadata.csv.
    59|   Stage 7: Join everything, sort (templates first, most-signals-fired first,
    60|     all-signals-fired first, export_run_id as tiebreak), and optionally apply
    61|     --top-n by unique export_run_id when a positive value is provided.
    62|   Stage 8: Write <out>/review_<cluster_id>.csv and print a console summary.
    63|   Stage 9: Build archetype_review_schedule.csv and archetype_review_gaps.csv.
    64|     The schedule can select Project files for manual review even when they are
    65|     outside the top-N review sample; final review_<cluster_id>.csv files remain
    66|     the source of cluster-level name diagnostics.
    67| 
    68| Usage:
    69|     python tools/archetype/prepare_archetype_review.py \\
    70|         --repo-root . \\
    71|         [--cluster-id wall_graphics__cluster_003] \\
    72|         [--signal-clusters Fingerprint_Out/archetype_analysis/signal_clusters.json] \\
    73|         [--cluster-classifications Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv] \\
    74|         [--archetype-classifications Fingerprint_Out/archetype_analysis/archetype_classifications.csv] \\
    75|         [--validation-detail Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv] \\
    76|         [--records results/records/records.csv] \\
    77|         [--file-metadata results/records/file_metadata.csv] \\
    78|         [--identity-items-dir results/records/identity_items_by_domain] \\
    79|         [--bip-lookup tools/archetype/bip_lookup.json] \\
    80|         [--shared-param-names tools/archetype/shared_param_names.json] \\
    81|         [--vfd-category-map tools/archetype/vfd_category_domain_map.json] \\
    82|         [--out Fingerprint_Out/archetype_analysis/archetype_review] \\
    83|         [--top-n N] [--dry-run] [--verbose]
    84| """
    85| from __future__ import annotations
    86| 
    87| import argparse
    88| import csv
    89| import json
    90| import sys
    91| from collections import defaultdict
    92| from pathlib import Path
    93| from typing import Any, Dict, List, Optional, Set, Tuple
    94| 
    95| sys.path.insert(0, str(Path(__file__).resolve().parent))
    96| 
    97| from _common import (  # noqa: E402
    98|     log,
    99|     atomic_write_csv,
   100|     field_matches,
   101|     is_valid_item,
   102|     read_csv_rows,
   103|     read_json,
   104| )
   105| 
   106| STAGE = "prepare_archetype_review"
   107| 
   108| GOVERNANCE_ROLE_ORDER = {"Template": 0, "Container": 1, "Project": 2, "Generic": 3}
   109| FILE_LEVEL_SENTINEL_SIGNAL_ID = "(file-level sentinel — fired signals unresolved)"
   110| 
   111| OUT_FIELDS = [
   112|     "file_path",
   113|     "export_run_id",
   114|     "governance_role",
   115|     "discipline_label",
   116|     "unit_system",
   117|     "client_label",
   118|     "n_signals_fired",
   119|     "all_signals_fired",
   120|     "signal_id",
   121|     "source_domain",
   122|     "source_join_hash",
   123|     "element_name",
   124|     "sig_hash",
   125|     "param_names",
   126|     "category_names",
   127| ]
   128| 
   129| SCHEDULE_FIELDS = [
   130|     "cluster_id",
   131|     "cluster_label_stub",
   132|     "signal_id",
   133|     "representative_source",
   134|     "file_path",
   135|     "export_run_id",
   136|     "governance_role",
   137|     "n_signals_fired",
   138|     "all_signals_fired",
   139|     "source_domain",
   140|     "source_join_hash",
   141|     "element_name",
   142|     "sig_hash",
   143|     "param_names",
   144|     "category_names",
   145|     "review_rows",
   146|     "review_named_rows",
   147|     "schedule_rows",
   148|     "schedule_named_rows",
   149|     "cluster_has_named_review_examples",
   150|     "selected_file_has_named_examples",
   151|     "selected_file_is_in_review_sample",
   152|     "selected_file_name_status",
   153|     "selected_project_file_unresolved_but_cluster_has_named_examples",
   154|     "schedule_name_regression",
   155| ]
   156| 
   157| GAPS_FIELDS = [
   158|     "cluster_id",
   159|     "cluster_label_stub",
   160|     "reason",
   161|     "review_rows",
   162|     "review_named_rows",
   163|     "schedule_rows",
   164|     "schedule_named_rows",
   165|     "cluster_has_named_review_examples",
   166|     "selected_file_has_named_examples",
   167|     "selected_file_is_in_review_sample",
   168|     "selected_file_name_status",
   169|     "selected_project_file_unresolved_but_cluster_has_named_examples",
   170|     "schedule_name_regression",
   171| ]
   172| 
   173| _PATH_COLUMN_CANDIDATES = ("central_path", "central_path_norm")
   174| 
   175| _VFD_PARAM_REF_SOURCE_FIELD = "vf.rule[*].param_ref.id"
   176| _VFD_CATEGORIES_KEY = "vf.categories"
   177| 
   178| _MAX_CONSOLE_EXAMPLES = 5
   179| 
   180| 
   181| def _find_cluster(signal_clusters: Dict[str, Any], cluster_id: str) -> Optional[Dict[str, Any]]:
   182|     for c in _all_clusters(signal_clusters):
   183|         if c.get("cluster_id") == cluster_id:
   184|             return c
   185|     return None
   186| 
   187| 
   188| def _all_clusters(signal_clusters: Dict[str, Any]) -> List[Dict[str, Any]]:
   189|     clusters_by_gq = signal_clusters.get("clusters", {}) if isinstance(signal_clusters, dict) else {}
   190|     out: List[Dict[str, Any]] = []
   191|     for cluster_defs in clusters_by_gq.values():
   192|         for c in cluster_defs:
   193|             if c.get("cluster_id"):
   194|                 out.append(c)
   195|     out.sort(key=lambda c: c.get("cluster_id", ""))
   196|     return out
   197| 
   198| 
   199| def _all_cluster_ids(signal_clusters: Dict[str, Any]) -> List[str]:
   200|     return [c["cluster_id"] for c in _all_clusters(signal_clusters)]
   201| 
   202| 
   203| def _resolve_param_name(param_id: str, bip_lookup: Dict[str, Any], shared_param_names: Dict[str, Any]) -> str:
   204|     if param_id.startswith("bip:"):
   205|         name = bip_lookup.get(param_id) or bip_lookup.get(param_id[len("bip:"):])
   206|         return name or param_id
   207|     name = shared_param_names.get(param_id)
   208|     return name or param_id
   209| 
   210| 
   211| def _parse_category_ids(raw_value: str) -> List[str]:
   212|     """Parse a vf.categories value into an ordered list of category-id strings.
   213| 
   214|     Accepts both the historical comma-separated shape and a JSON-array shape
   215|     (see build_cross_domain_items.py._parse_vf_categories).
   216|     """
   217|     value = (raw_value or "").strip()
   218|     if not value:
   219|         return []
   220| 
   221|     comma_parts = [part.strip() for part in value.split(",")]
   222|     if comma_parts and all(part and part.lstrip("+-").isdigit() for part in comma_parts):
   223|         return comma_parts
   224| 
   225|     try:
   226|         data = json.loads(value)
   227|     except json.JSONDecodeError:
   228|         return []
   229|     if not isinstance(data, list):
   230|         return []
   231| 
   232|     out: List[str] = []
   233|     for item in data:
   234|         if isinstance(item, int):
   235|             out.append(str(item))
   236|         elif isinstance(item, str) and item.strip():
   237|             out.append(item.strip())
   238|     return out
   239| 
   240| 
   241| def _resolve_category_name(category_id: str, vfd_category_map: Dict[str, Any]) -> str:
   242|     entry = vfd_category_map.get(category_id)
   243|     if isinstance(entry, dict):
   244|         name = entry.get("name")
   245|         if name:
   246|             return str(name)
   247|     return f"{category_id}[?]"
   248| 
   249| 
   250| def _governance_question_from_cluster_id(cluster_id: str) -> str:
   251|     """Resolve governance_question from a cluster_id such as
   252|     view_filter_strategy__cluster_008 when older signal_clusters.json files do
   253|     not carry a governance_question field on each cluster object.
   254|     """
   255|     marker = "__cluster_"
   256|     if marker in cluster_id:
   257|         return cluster_id.split(marker, 1)[0]
   258|     return ""
   259| 
   260| 
   261| def _governance_question_from_archetype_id(archetype_id: str) -> str:
   262|     """archetype_id encodes governance_question as the second "__"-delimited
   263|     token, e.g. CANDIDATE__wall_graphics__... -> wall_graphics.
   264| 
   265|     Mirrors cluster_archetype_signals.py's
   266|     _governance_question_from_archetype_id().
   267|     """
   268|     parts = archetype_id.split("__")
   269|     return parts[1] if len(parts) > 1 else ""
   270| 
   271| 
   272| def _build_curated_gq_map(classification_rows: List[Dict[str, str]]) -> Dict[str, str]:
   273|     """archetype_id -> governance_question, from archetype_classifications.csv.
   274| 
   275|     Human curation can re-assign a promoted archetype to a different
   276|     governance_question without changing its (CANDIDATE-derived) archetype_id,
   277|     so this column is the source of truth wherever it is populated. Mirrors
   278|     cluster_archetype_signals.py's _build_curated_gq_map().
   279|     """
   280|     curated: Dict[str, str] = {}
   281|     for row in classification_rows:
   282|         archetype_id = row.get("archetype_id", "")
   283|         gq = row.get("governance_question", "")
   284|         if archetype_id and gq:
   285|             curated[archetype_id] = gq
   286|     return curated
   287| 
   288| 
   289| def _resolve_governance_question(archetype_id: str, curated_gq_map: Dict[str, str]) -> str:
   290|     return curated_gq_map.get(archetype_id) or _governance_question_from_archetype_id(archetype_id)
   291| 
   292| 
   293| class ClusterContext:
   294|     """Per-cluster Stage 2/3 results."""
   295| 
   296|     def __init__(self, cluster: Dict[str, Any]):
   297|         self.cluster_id: str = cluster.get("cluster_id", "")
   298|         self.governance_question: str = cluster.get("governance_question", "") or _governance_question_from_cluster_id(self.cluster_id)
   299|         self.signal_ids: List[str] = list(cluster.get("signal_ids", []) or [])
   300|         self.cluster_label_stub: str = cluster.get("cluster_label_stub", "")
   301|         self.classification_by_file: Dict[str, Dict[str, str]] = {}
   302|         self.detail_by_file_signal: Dict[Tuple[str, str, str], Dict[str, str]] = {}
   303|         self.qualifying_files: Set[str] = set()
   304|         self.source_domains: Set[str] = set()
   305| 
   306| 
   307| def _build_cluster_context(
   308|     cluster: Dict[str, Any],
   309|     rows_by_cluster_id: Dict[str, List[Dict[str, str]]],
   310|     detail_rows_by_export: Dict[str, List[Dict[str, str]]],
   311|     archetype_ids_by_gq: Dict[str, Set[str]],
   312| ) -> ClusterContext:
   313|     ctx = ClusterContext(cluster)
   314| 
   315|     # Stage 2: find qualifying files.
   316|     for row in rows_by_cluster_id.get(ctx.cluster_id, []):
   317|         export_run_id = row.get("export_run_id", "")
   318|         if export_run_id:
   319|             ctx.classification_by_file[export_run_id] = row
   320|     ctx.qualifying_files = set(ctx.classification_by_file.keys())
   321| 
   322|     # Stage 3: get sig_hashes per file per signal.
   323|     # signal_ids in signal_clusters.json are edge_id nodes (see
   324|     # cluster_archetype_signals.py Stage 1), while archetype_validation_detail.csv's
   325|     # signal_id column may be a curated, human-friendly id distinct from its
   326|     # edge_id. Membership in the cluster is therefore tested against edge_id;
   327|     # the curated signal_id is preserved as the row's display id.
   328|     #
   329|     # The same edge_id can be promoted under more than one governance
   330|     # question/archetype, each with its own join_hash filter, producing
   331|     # separate archetype_validation_detail.csv rows at the (export_run_id,
   332|     # archetype_id, signal_id) grain. Restrict to archetype_ids that belong
   333|     # to this cluster's governance_question so a shared edge_id doesn't pull
   334|     # in elements/signals from an unrelated archetype.
   335|     #
   336|     # A file can fire the same signal on multiple source records (one
   337|     # archetype_validation_detail.csv row per source_join_hash; see
   338|     # n_join_hashes_in_file). source_record_pk is preferred for the internal
   339|     # dedupe key when available because it is the most precise source-record
   340|     # identity; source_join_hash remains on the detail row and is written to
   341|     # review CSVs unchanged for reviewer forensics.
   342|     signal_id_set = set(ctx.signal_ids)
   343|     valid_archetype_ids = archetype_ids_by_gq.get(ctx.governance_question, set())
   344|     files_with_detail: Set[str] = set()
   345|     for export_run_id in ctx.qualifying_files:
   346|         for row in detail_rows_by_export.get(export_run_id, []):
   347|             edge_id = row.get("edge_id", "")
   348|             if edge_id not in signal_id_set:
   349|                 continue
   350|             if row.get("archetype_id", "") not in valid_archetype_ids:
   351|                 continue
   352|             signal_id = row.get("signal_id", "") or edge_id
   353|             source_join_hash = row.get("source_join_hash", "")
   354|             source_record_pk = row.get("source_record_pk", "")
   355|             key = (export_run_id, signal_id, source_record_pk or source_join_hash)
   356|             if key not in ctx.detail_by_file_signal:
   357|                 ctx.detail_by_file_signal[key] = row
   358|             files_with_detail.add(export_run_id)
   359| 
   360|     for export_run_id in sorted(ctx.qualifying_files - files_with_detail):
   361|         log(STAGE, f"WARNING: cluster_id={ctx.cluster_id}: qualifying file export_run_id={export_run_id} has no matching rows in archetype_validation_detail.csv")
   362| 
   363|     ctx.source_domains = {row.get("source_domain", "") for row in ctx.detail_by_file_signal.values() if row.get("source_domain")}
   364|     return ctx
   365| 
   366| 
   367| def _load_label_lookup(
   368|     records_path: Path,
   369|     qualifying_files: Set[str],
   370|     source_domains: Set[str],
   371| ) -> Tuple[
   372|     Dict[Tuple[str, str, str], Tuple[str, str]],
   373|     Dict[Tuple[str, str], Tuple[str, str]],
   374|     Dict[Tuple[str, str, str], Tuple[str, str]],
   375|     Dict[Tuple[str, str, str], str],
   376| ]:
   377|     """Stage 4: stream records.csv to resolve records to labels.
   378| 
   379|     Returns domain+sig_hash, legacy export+sig_hash, and record_pk keyed label
   380|     lookups, plus a VFD record_pk helper for param/category resolution.
   381|     """
   382|     label_by_domain_sig: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
   383|     label_by_sig: Dict[Tuple[str, str], Tuple[str, str]] = {}
   384|     label_by_record_pk: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
   385|     vfd_sig_to_record_pk: Dict[Tuple[str, str, str], str] = {}
   386|     if not records_path.is_file():
   387|         log(STAGE, f"WARNING: records file not found at {records_path}; element_name will be unresolved")
   388|         return label_by_domain_sig, label_by_sig, label_by_record_pk, vfd_sig_to_record_pk
   389| 
   390|     with records_path.open("r", encoding="utf-8-sig", newline="") as f:
   391|         reader = csv.DictReader(f)
   392|         for row in reader:
   393|             export_run_id = row.get("export_run_id", "")
   394|             domain = row.get("domain", "")
   395|             if export_run_id not in qualifying_files:
   396|                 continue
   397|             if source_domains and domain not in source_domains:
   398|                 continue
   399|             sig_hash = row.get("sig_hash", "")
   400|             label = (row.get("label_display", ""), row.get("label_quality", ""))
   401|             record_pk = row.get("record_pk", "")
   402|             if record_pk:
   403|                 label_by_record_pk[(export_run_id, domain, record_pk)] = label
   404|             if not sig_hash:
   405|                 continue
   406|             label_by_domain_sig[(export_run_id, domain, sig_hash)] = label
   407|             label_by_sig[(export_run_id, sig_hash)] = label
   408|             if domain == "view_filter_definitions":
   409|                 vfd_sig_to_record_pk[(export_run_id, domain, sig_hash)] = record_pk
   410| 
   411|     log(STAGE, f"resolved {len(label_by_domain_sig)} (export_run_id, domain, sig_hash) label rows from {records_path}")
   412|     return label_by_domain_sig, label_by_sig, label_by_record_pk, vfd_sig_to_record_pk
   413| 
   414| 
   415| def _load_vfd_resolution(
   416|     identity_items_dir: Path,
   417|     qualifying_files: Set[str],
   418|     source_domains: Set[str],
   419|     bip_lookup: Dict[str, Any],
   420|     shared_param_names: Dict[str, Any],
   421|     vfd_category_map: Dict[str, Any],
   422| ) -> Dict[Tuple[str, str], Tuple[str, str]]:
   423|     """Stage 5: resolve parameter names and category names (VFD only)."""
   424|     vfd_resolution: Dict[Tuple[str, str], Tuple[str, str]] = {}
   425|     if "view_filter_definitions" not in source_domains:
   426|         return vfd_resolution
   427| 
   428|     vfd_identity_items_path = identity_items_dir / "view_filter_definitions.csv"
   429|     vfd_identity_rows = read_csv_rows(vfd_identity_items_path)
   430|     if not vfd_identity_rows:
   431|         log(STAGE, f"WARNING: {vfd_identity_items_path} not found or empty; param_names/category_names will be empty for VFD rows")
   432|         return vfd_resolution
   433| 
   434|     grouped: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
   435|     for row in vfd_identity_rows:
   436|         export_run_id = row.get("export_run_id", "")
   437|         if export_run_id not in qualifying_files:
   438|             continue
   439|         grouped[(export_run_id, row.get("record_pk", ""))].append(row)
   440| 
   441|     for key, rows in grouped.items():
   442|         param_tokens: List[Tuple[str, str]] = []  # (item_key, item_value)
   443|         categories_raw: Optional[str] = None
   444|         for row in rows:
   445|             item_key = row.get("item_key", "")
   446|             item_value = row.get("item_value", "")
   447|             item_value_type = row.get("item_value_type", "")
   448|             if not is_valid_item(item_value, item_value_type):
   449|                 continue
   450|             if field_matches(item_key, _VFD_PARAM_REF_SOURCE_FIELD, "indexed"):
   451|                 param_tokens.append((item_key, item_value))
   452|             elif item_key == _VFD_CATEGORIES_KEY:
   453|                 categories_raw = item_value
   454| 
   455|         param_names = " | ".join(
   456|             _resolve_param_name(value, bip_lookup, shared_param_names)
   457|             for _, value in sorted(param_tokens)
   458|         )
   459|         category_names = " | ".join(
   460|             _resolve_category_name(cid, vfd_category_map)
   461|             for cid in _parse_category_ids(categories_raw or "")
   462|         )
   463|         vfd_resolution[key] = (param_names, category_names)
   464| 
   465|     log(STAGE, f"resolved param/category names for {len(vfd_resolution)} VFD records")
   466|     return vfd_resolution
   467| 
   468| 
   469| def _load_file_path_lookup(file_metadata_path: Path) -> Dict[str, str]:
   470|     """Stage 6: resolve export_run_id -> file_path."""
   471|     file_metadata_rows = read_csv_rows(file_metadata_path)
   472|     log(STAGE, f"loaded {len(file_metadata_rows)} rows from {file_metadata_path}")
   473| 
   474|     path_column = None
   475|     if file_metadata_rows:
   476|         header_keys = file_metadata_rows[0].keys()
   477|         for candidate in _PATH_COLUMN_CANDIDATES:
   478|             if candidate in header_keys:
   479|                 path_column = candidate
   480|                 break
   481|         if path_column is None:
   482|             log(STAGE, f"WARNING: no path column ({', '.join(_PATH_COLUMN_CANDIDATES)}) found in {file_metadata_path}; falling back to export_run_id")
   483| 
   484|     file_path_lookup: Dict[str, str] = {}
   485|     for row in file_metadata_rows:
   486|         export_run_id = row.get("export_run_id", "")
   487|         if not export_run_id:
   488|             continue
   489|         file_path_lookup[export_run_id] = row.get(path_column, "") if path_column else ""
   490|     return file_path_lookup
   491| 
   492| 
   493| def _is_named_element(element_name: str) -> bool:
   494|     value = (element_name or "").strip()
   495|     if not value or value == "_":
   496|         return False
   497|     return not value.lower().startswith("(unresolved")
   498| 
   499| 
   500| def _schedule_file_sort_key(rows_for_file: List[Dict[str, str]]) -> Tuple[int, int, int, str]:
   501|     role_order = {"Project": 0, "Template": 1, "Container": 2, "Generic": 3}
   502|     first = rows_for_file[0] if rows_for_file else {}
   503|     role_rank = role_order.get(first.get("governance_role", ""), 4)
   504|     max_signals = 0
   505|     any_all_signals = 0
   506|     for row in rows_for_file:
   507|         try:
   508|             max_signals = max(max_signals, int(row.get("n_signals_fired") or 0))
   509|         except (TypeError, ValueError):
   510|             pass
   511|         if row.get("all_signals_fired") == "true":
   512|             any_all_signals = 1
   513|     return (role_rank, -max_signals, -any_all_signals, first.get("export_run_id", ""))
   514| 
   515| 
   516| def _schedule_row_sort_key(row: Dict[str, str]) -> Tuple[str, int, str]:
   517|     named_rank = 0 if _is_named_element(row.get("element_name", "")) else 1
   518|     return (row.get("signal_id", ""), named_rank, row.get("source_join_hash", ""))
   519| 
   520| 
```
