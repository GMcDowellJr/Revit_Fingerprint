# Chunk of policies/domain_sig_hash_policies.json

- Source relative path: `policies/domain_sig_hash_policies.json`
- Chunk: 4 of 4
- Original line range: 1171-1239
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: f9574026a45f473063889e6bfa400737885177fa01bec6ddd2c3a96308a708dd
- Starts inside symbol: no
- Ends inside symbol: no

```
  1171|         "workset.name"
  1172|       ],
  1173|       "hash_alg": "md5_utf8_join_pipe",
  1174|       "minima": {
  1175|         "block_if_any_required_not_ok": true
  1176|       },
  1177|       "notes": [
  1178|         "Generated from contracts/domain_identity_keys_v2.json.",
  1179|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1180|       ],
  1181|       "required_items": [
  1182|         "workset.name",
  1183|         "workset.kind",
  1184|         "workset.is_default_workset"
  1185|       ],
  1186|       "sig_hash_schema": "worksets.sig_hash.v1"
  1187|     },
  1188|     "worksets_doc": {
  1189|       "allowed_item_prefixes": [],
  1190|       "allowed_items": [
  1191|         "worksets_doc.count_family_workset",
  1192|         "worksets_doc.count_other_workset",
  1193|         "worksets_doc.count_standard_workset",
  1194|         "worksets_doc.count_user_workset",
  1195|         "worksets_doc.count_view_workset",
  1196|         "worksets_doc.is_workshared"
  1197|       ],
  1198|       "hash_alg": "md5_utf8_join_pipe",
  1199|       "minima": {
  1200|         "block_if_any_required_not_ok": false
  1201|       },
  1202|       "notes": [
  1203|         "Generated from contracts/domain_identity_keys_v2.json.",
  1204|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1205|       ],
  1206|       "required_items": [],
  1207|       "sig_hash_schema": "worksets_doc.sig_hash.v1"
  1208|     },
  1209|     "browser_organization": {
  1210|       "allowed_item_prefixes": [],
  1211|       "allowed_items": [
  1212|         "bo.category",
  1213|         "bo.filter_has_value",
  1214|         "bo.sorting_order",
  1215|         "bo.sorting_parameter_id"
  1216|       ],
  1217|       "hash_alg": "md5_utf8_join_pipe",
  1218|       "minima": {
  1219|         "block_if_any_required_not_ok": true
  1220|       },
  1221|       "notes": [
  1222|         "Generated from contracts/domain_identity_keys_v2.json.",
  1223|         "sig_hash is computed post-extraction from canonical identity_basis.items.",
  1224|         "Hand-added, not via a full generate_sig_hash_policy.py re-run, matching the worksets/worksets_doc entries above -- the checked-in policy file has hand-curated notes for other domains that a wholesale regeneration would overwrite.",
  1225|         "bo.filter_has_value is semantic (drives sig_hash) but not required -- an unreadable read degrades the record rather than blocking it."
  1226|       ],
  1227|       "required_items": [
  1228|         "bo.category",
  1229|         "bo.sorting_order",
  1230|         "bo.sorting_parameter_id"
  1231|       ],
  1232|       "sig_hash_schema": "browser_organization.sig_hash.v1"
  1233|     }
  1234|   },
  1235|   "identity_item_schema": "identity_items.v1",
  1236|   "record_schema_version": "record.v2",
  1237|   "source_registry_version": "domain_identity_keys.v2",
  1238|   "version": "domain_sig_hash_policies.v1"
  1239| }
```
