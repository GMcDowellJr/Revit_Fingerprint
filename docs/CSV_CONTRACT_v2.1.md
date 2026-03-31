# CSV Contract v2.1 (Option 2 joinable split-analysis)

## Versioning
- `schema_version` is SemVer string (`2.1.0`).
- `tool_version` is SemVer string (supports build metadata, e.g. `0.0.0+<gitsha>`).
- `analysis_manifest.csv` policy versions are SemVer strings:
  - `join_key_policy_version`
  - `pattern_promotion_policy_version`
  - `authority_metric_version`
- `is_incremental_update` is integer `0`/`1`.

## Evidence vs derived boundary
- Deterministic evidence: fingerprint JSON exports and Phase0 CSVs.
- Derived outputs: analysis CSVs and split-analysis CSVs.

## Required split-analysis contract columns (Option 2)
Every split-analysis CSV must include:
1. `schema_version`
2. `analysis_run_id`
3. `domain`

File-grain split CSVs must additionally include:
4. `export_run_id`

`export_run_id` should not be added to non-file-grain outputs where it is not semantically meaningful
(for example, IDS policy tables or cluster/domain catalogs).

Trace/back-compat:
- `file_id` may remain as a trace column.
- `file_id_to_export_run_id.csv` is emitted for legacy bridges.

## Grains
- `split_records`: `analysis_run_id × export_run_id × domain × record_pk`
- `split_clusters`: `analysis_run_id × export_run_id × domain × cluster_id` (and/or `pattern_id`)
- `split_cluster_summary`: `analysis_run_id × domain × cluster_id` (and/or `pattern_id`)
- `split_coverage`: `analysis_run_id × domain × group_type × group_id`

## Pattern linkage
- Preferred linkage source is `record_pattern_membership.csv`.
- For cluster-level linkage, emit `split_cluster_to_pattern_map.csv` with:
  - `analysis_run_id,domain,cluster_id,pattern_id,mapping_quality,mapping_reason`

## `domain_patterns.csv` semantic grouping column
- `semantic_group` is an additive string column in `domain_patterns.csv`.
- Source of truth is `Results_v21/label_synthesis/label_semantic_groups.json`, keyed by `groups[domain][pattern_id]`.
- Emit behavior is nullable-by-design: when no cache entry exists, output empty string.
- Population is performed offline by `tools/label_synthesis/build_semantic_groups.py`; cache rows include `reviewed` for human curation state.
