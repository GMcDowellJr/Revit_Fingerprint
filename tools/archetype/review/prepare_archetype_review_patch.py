# ── Patch for tools/archetype/prepare_archetype_review.py ────────────────────
#
# Bug: when detail_by_file_signal is empty (e.g. VFD clusters whose
# archetype_validation_detail.csv rows don't join through to the cluster's
# qualifying files), _process_cluster emits a header-only CSV with no data rows.
#
# Fix: in _process_cluster, after building review_rows, add a fallback path that
# synthesises stub rows directly from ctx.classification_by_file when
# review_rows is empty but qualifying files exist.  Stub rows carry governance
# metadata (role, discipline, client, signals_fired counts) and mark
# element_name as "(unresolved — validation detail missing)" so downstream
# consumers can distinguish a genuine empty cluster from a resolution failure.
#
# Apply by replacing the section that builds review_rows in _process_cluster.
# The change is localised to that function; nothing else needs touching.
#
# ─────────────────────────────────────────────────────────────────────────────
# LOCATE the following block in _process_cluster (after the for-loop that
# builds review_rows from ctx.detail_by_file_signal):
# ─────────────────────────────────────────────────────────────────────────────

# BEFORE (existing code — ends just before "review_rows.sort(key=_sort_key)"):
#
#     review_rows: List[Dict[str, str]] = []
#     for (export_run_id, signal_id, source_join_hash), detail in ctx.detail_by_file_signal.items():
#         cls = ctx.classification_by_file.get(export_run_id, {})
#         ...
#         review_rows.append({ ... })
#
#     review_rows.sort(key=_sort_key)

# AFTER — insert the following fallback block immediately after the for-loop
# (before review_rows.sort):
#
#     # ── Fallback: detail join produced no rows ──────────────────────────────
#     # This happens when archetype_validation_detail.csv has no entries that
#     # match the cluster's signal_ids for the qualifying files — most commonly
#     # seen on VFD clusters where the detail rows don't propagate through the
#     # governance_question reclassification join.  Rather than silently writing
#     # a header-only CSV, emit one stub row per (file, signal_id) so the output
#     # is always non-empty and the element_name field makes the situation clear.
#     if not review_rows and ctx.classification_by_file:
#         if verbose:
#             log(
#                 STAGE,
#                 f"WARNING: cluster_id={ctx.cluster_id} detail_by_file_signal is empty "
#                 f"but {len(ctx.classification_by_file)} qualifying files exist; "
#                 f"emitting classification-only stub rows",
#             )
#         for export_run_id, cls in ctx.classification_by_file.items():
#             file_path = file_path_lookup.get(export_run_id) or export_run_id
#             for signal_id in ctx.signal_ids:
#                 review_rows.append({
#                     "file_path": file_path,
#                     "export_run_id": export_run_id,
#                     "governance_role": cls.get("governance_role", ""),
#                     "discipline_label": cls.get("discipline_label", ""),
#                     "unit_system": cls.get("unit_system", ""),
#                     "client_label": cls.get("client_label", ""),
#                     "n_signals_fired": cls.get("n_signals_fired", ""),
#                     "all_signals_fired": cls.get("all_signals_fired", ""),
#                     "signal_id": signal_id,
#                     "source_domain": "",
#                     "source_join_hash": "",
#                     "element_name": "(unresolved — validation detail missing)",
#                     "sig_hash": "",
#                     "param_names": "",
#                     "category_names": "",
#                 })
#     # ── end fallback ────────────────────────────────────────────────────────
#
#     review_rows.sort(key=_sort_key)

# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSIS NOTE
# ─────────────────────────────────────────────────────────────────────────────
# To investigate the root cause of the empty detail join, run with --verbose
# and look for the log line:
#
#   [prepare_archetype_review] cluster_id=<cid> ... detail_rows=0
#
# If detail_rows=0 for a cluster whose archetype_validation.csv shows
# n_files_classified > 0, the cause is one of:
#
#   1. archetype_validation_detail.csv was produced by Stage 4 before the
#      archetype_definitions.json governance_question corrections were in place.
#      Fix: re-run Stages 3 → 4 → 5 → prepare_archetype_review with the
#      corrected definitions file.
#
#   2. The archetype_ids in archetype_validation_detail.csv don't resolve to
#      the cluster's governance_question via curated_gq_map because
#      archetype_classifications.csv and archetype_validation_detail.csv are
#      out of sync (different Stage 3 runs).
#      Fix: ensure Stages 3, 4, and 5 are all run in sequence from the same
#      archetype_definitions.json without editing the definitions between runs.
#
#   3. The edge_id in archetype_validation_detail.csv doesn't match the
#      signal_ids in signal_clusters.json (e.g. due to edge aliasing in
#      reference_graph.json collapsing the canonical edge_id differently).
#      Fix: check _common.build_edge_aliases() — if the VFD edge is being
#      aliased to a different canonical id, update the cluster's signal_ids
#      accordingly or add the alias to the edge alias map.
# ─────────────────────────────────────────────────────────────────────────────
