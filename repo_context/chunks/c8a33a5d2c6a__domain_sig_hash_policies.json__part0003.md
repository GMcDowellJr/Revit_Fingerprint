# Chunk of policies/domain_sig_hash_policies.json

- Source relative path: `policies/domain_sig_hash_policies.json`
- Chunk: 3 of 4
- Original line range: 781-1180
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: f9574026a45f473063889e6bfa400737885177fa01bec6ddd2c3a96308a708dd
- Starts inside symbol: no
- Ends inside symbol: no

```
   781|       "sig_hash_schema": "phase_filters.sig_hash.v1"
   782|     },
   783|     "phases": {
   784|       "allowed_item_prefixes": [],
   785|       "allowed_items": [
   786|         "phase.seq",
   787|         "phase.name"
   788|       ],
   789|       "hash_alg": "md5_utf8_join_pipe",
   790|       "minima": {
   791|         "block_if_any_required_not_ok": true
   792|       },
   793|       "notes": [
   794|         "Generated from contracts/domain_identity_keys_v2.json.",
   795|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   796|       ],
   797|       "required_items": [
   798|         "phase.seq",
   799|         "phase.name"
   800|       ],
   801|       "sig_hash_schema": "phases.sig_hash.v1"
   802|     },
   803|     "roof_types": {
   804|       "allowed_item_prefixes": [],
   805|       "allowed_items": [
   806|         "rt.layer_count",
   807|         "rt.total_thickness_in",
   808|         "rt.stack_hash_loose",
   809|         "rt.total_layer_rows",
   810|         "rt.stack_hash_strict",
   811|         "rt.stack_hash_function_only",
   812|         "rt.coarse_fill_pattern_sig_hash",
   813|         "rt.type_name",
   814|         "rt.coarse_fill_color_rgb"
   815|       ],
   816|       "hash_alg": "md5_utf8_join_pipe",
   817|       "minima": {
   818|         "block_if_any_required_not_ok": true
   819|       },
   820|       "notes": [
   821|         "Generated from contracts/domain_identity_keys_v2.json.",
   822|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   823|       ],
   824|       "required_items": [
   825|         "rt.layer_count",
   826|         "rt.total_thickness_in",
   827|         "rt.stack_hash_loose"
   828|       ],
   829|       "sig_hash_schema": "roof_types.sig_hash.v1"
   830|     },
   831|     "text_types": {
   832|       "allowed_item_prefixes": [],
   833|       "allowed_items": [
   834|         "text_type.font",
   835|         "text_type.size_in",
   836|         "text_type.bold",
   837|         "text_type.italic",
   838|         "text_type.underline",
   839|         "text_type.color_rgb",
   840|         "text_type.width_factor",
   841|         "text_type.background",
   842|         "text_type.line_weight",
   843|         "text_type.show_border",
   844|         "text_type.leader_border_offset_in",
   845|         "text_type.tab_size_in"
   846|       ],
   847|       "hash_alg": "md5_utf8_join_pipe",
   848|       "minima": {
   849|         "block_if_any_required_not_ok": true
   850|       },
   851|       "notes": [
   852|         "Generated from contracts/domain_identity_keys_v2.json.",
   853|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   854|       ],
   855|       "required_items": [
   856|         "text_type.font",
   857|         "text_type.size_in",
   858|         "text_type.bold",
   859|         "text_type.italic",
   860|         "text_type.underline",
   861|         "text_type.color_rgb",
   862|         "text_type.width_factor"
   863|       ],
   864|       "sig_hash_schema": "text_types.sig_hash.v1"
   865|     },
   866|     "units": {
   867|       "allowed_item_prefixes": [],
   868|       "allowed_items": [
   869|         "units.spec",
   870|         "units.unit_type_id",
   871|         "units.symbol_type_id",
   872|         "units.accuracy",
   873|         "units.rounding_method",
   874|         "units.use_default",
   875|         "units.use_digit_grouping",
   876|         "units.use_plus_prefix",
   877|         "units.suppress_leading_zeros",
   878|         "units.suppress_spaces",
   879|         "units.suppress_trailing_zeros"
   880|       ],
   881|       "hash_alg": "md5_utf8_join_pipe",
   882|       "minima": {
   883|         "block_if_any_required_not_ok": true
   884|       },
   885|       "notes": [
   886|         "Generated from contracts/domain_identity_keys_v2.json.",
   887|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   888|       ],
   889|       "required_items": [
   890|         "units.spec",
   891|         "units.unit_type_id"
   892|       ],
   893|       "sig_hash_schema": "units.sig_hash.v1"
   894|     },
   895|     "units_doc": {
   896|       "allowed_item_prefixes": [],
   897|       "allowed_items": [
   898|         "units_doc.decimal_symbol",
   899|         "units_doc.digit_grouping_amount",
   900|         "units_doc.digit_grouping_symbol"
   901|       ],
   902|       "hash_alg": "md5_utf8_join_pipe",
   903|       "minima": {
   904|         "block_if_any_required_not_ok": false
   905|       },
   906|       "notes": [
   907|         "Generated from contracts/domain_identity_keys_v2.json.",
   908|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   909|       ],
   910|       "required_items": [],
   911|       "sig_hash_schema": "units_doc.sig_hash.v1"
   912|     },
   913|     "view_category_overrides_annotation": {
   914|       "allowed_item_prefixes": [],
   915|       "allowed_items": [
   916|         "vco.baseline_category_path",
   917|         "vco.baseline_sig_hash",
   918|         "vco.override_properties_hash",
   919|         "vco.projection.line_weight",
   920|         "vco.projection.color.rgb",
   921|         "vco.projection.pattern_ref.sig_hash",
   922|         "vco.projection.fill_pattern_ref.sig_hash",
   923|         "vco.projection.fill_color.rgb",
   924|         "vco.halftone",
   925|         "vco.transparency",
   926|         "vco.include_controlled",
   927|         "vco.vg_category_type",
   928|         "vco.context_type",
   929|         "vco.template_name",
   930|         "vco.template_element_id",
   931|         "vco.template_unique_id"
   932|       ],
   933|       "hash_alg": "md5_utf8_join_pipe",
   934|       "minima": {
   935|         "block_if_any_required_not_ok": true
   936|       },
   937|       "notes": [
   938|         "Generated from contracts/domain_identity_keys_v2.json.",
   939|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   940|       ],
   941|       "required_items": [
   942|         "vco.baseline_category_path",
   943|         "vco.baseline_sig_hash",
   944|         "vco.override_properties_hash"
   945|       ],
   946|       "sig_hash_schema": "view_category_overrides_annotation.sig_hash.v1"
   947|     },
   948|     "view_category_overrides_model": {
   949|       "allowed_item_prefixes": [],
   950|       "allowed_items": [
   951|         "vco.baseline_category_path",
   952|         "vco.baseline_sig_hash",
   953|         "vco.override_properties_hash",
   954|         "vco.projection.line_weight",
   955|         "vco.projection.color.rgb",
   956|         "vco.projection.pattern_ref.sig_hash",
   957|         "vco.projection.fill_pattern_ref.sig_hash",
   958|         "vco.projection.fill_color.rgb",
   959|         "vco.cut.line_weight",
   960|         "vco.cut.color.rgb",
   961|         "vco.cut.pattern_ref.sig_hash",
   962|         "vco.cut.fill_pattern_ref.sig_hash",
   963|         "vco.cut.fill_color.rgb",
   964|         "vco.halftone",
   965|         "vco.transparency",
   966|         "vco.include_controlled",
   967|         "vco.vg_category_type",
   968|         "vco.context_type",
   969|         "vco.template_name",
   970|         "vco.template_element_id",
   971|         "vco.template_unique_id"
   972|       ],
   973|       "hash_alg": "md5_utf8_join_pipe",
   974|       "minima": {
   975|         "block_if_any_required_not_ok": true
   976|       },
   977|       "notes": [
   978|         "Generated from contracts/domain_identity_keys_v2.json.",
   979|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   980|       ],
   981|       "required_items": [
   982|         "vco.baseline_category_path",
   983|         "vco.baseline_sig_hash",
   984|         "vco.override_properties_hash"
   985|       ],
   986|       "sig_hash_schema": "view_category_overrides_model.sig_hash.v1"
   987|     },
   988|     "view_filter_applications_view_templates": {
   989|       "allowed_item_prefixes": [],
   990|       "allowed_items": [
   991|         "vfa.stack_def_hash"
   992|       ],
   993|       "hash_alg": "md5_utf8_join_pipe",
   994|       "minima": {
   995|         "block_if_any_required_not_ok": true
   996|       },
   997|       "notes": [
   998|         "Generated from contracts/domain_identity_keys_v2.json.",
   999|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1000|       ],
  1001|       "required_items": [
  1002|         "vfa.stack_def_hash"
  1003|       ],
  1004|       "sig_hash_schema": "view_filter_applications_view_templates.sig_hash.v1"
  1005|     },
  1006|     "view_filter_definitions": {
  1007|       "allowed_item_prefixes": [
  1008|         "vf.rule["
  1009|       ],
  1010|       "allowed_items": [
  1011|         "vf.categories",
  1012|         "vf.logic_root",
  1013|         "vf.rule_count"
  1014|       ],
  1015|       "hash_alg": "md5_utf8_join_pipe",
  1016|       "minima": {
  1017|         "block_if_any_required_not_ok": true
  1018|       },
  1019|       "notes": [
  1020|         "sig_hash hashes raw vf.* items directly: matches extractor preimage exactly.",
  1021|         "vf.def_hash is excluded from allowed_items — it is a derived hash of the raw items",
  1022|         "and must not participate in sig_hash (would produce hash-of-hash, not hash-of-items).",
  1023|         "vf.categories, vf.logic_root, vf.rule_count plus all vf.rule[NNN].* items via prefix.",
  1024|         "vf.categories is always emitted by the extractor (q may be missing/unreadable but key present).",
  1025|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1026|       ],
  1027|       "required_items": [
  1028|         "vf.logic_root",
  1029|         "vf.rule_count"
  1030|       ],
  1031|       "sig_hash_schema": "view_filter_definitions.sig_hash.v2"
  1032|     },
  1033|     "view_templates_ceiling_plans": {
  1034|       "allowed_item_prefixes": [
  1035|         "view_template.sig."
  1036|       ],
  1037|       "allowed_items": [
  1038|         "view_template.def_hash"
  1039|       ],
  1040|       "hash_alg": "md5_utf8_join_pipe",
  1041|       "minima": {
  1042|         "block_if_any_required_not_ok": true
  1043|       },
  1044|       "notes": [
  1045|         "Generated from contracts/domain_identity_keys_v2.json.",
  1046|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1047|       ],
  1048|       "required_items": [
  1049|         "view_template.def_hash"
  1050|       ],
  1051|       "sig_hash_schema": "view_templates_ceiling_plans.sig_hash.v1"
  1052|     },
  1053|     "view_templates_elevations_sections_detail": {
  1054|       "allowed_item_prefixes": [
  1055|         "view_template.sig."
  1056|       ],
  1057|       "allowed_items": [
  1058|         "view_template.def_hash"
  1059|       ],
  1060|       "hash_alg": "md5_utf8_join_pipe",
  1061|       "minima": {
  1062|         "block_if_any_required_not_ok": true
  1063|       },
  1064|       "notes": [
  1065|         "Generated from contracts/domain_identity_keys_v2.json.",
  1066|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1067|       ],
  1068|       "required_items": [
  1069|         "view_template.def_hash"
  1070|       ],
  1071|       "sig_hash_schema": "view_templates_elevations_sections_detail.sig_hash.v1"
  1072|     },
  1073|     "view_templates_floor_structural_area_plans": {
  1074|       "allowed_item_prefixes": [
  1075|         "view_template.sig."
  1076|       ],
  1077|       "allowed_items": [
  1078|         "view_template.def_hash"
  1079|       ],
  1080|       "hash_alg": "md5_utf8_join_pipe",
  1081|       "minima": {
  1082|         "block_if_any_required_not_ok": true
  1083|       },
  1084|       "notes": [
  1085|         "Generated from contracts/domain_identity_keys_v2.json.",
  1086|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1087|       ],
  1088|       "required_items": [
  1089|         "view_template.def_hash"
  1090|       ],
  1091|       "sig_hash_schema": "view_templates_floor_structural_area_plans.sig_hash.v1"
  1092|     },
  1093|     "view_templates_renderings_drafting": {
  1094|       "allowed_item_prefixes": [
  1095|         "view_template.sig."
  1096|       ],
  1097|       "allowed_items": [
  1098|         "view_template.def_hash"
  1099|       ],
  1100|       "hash_alg": "md5_utf8_join_pipe",
  1101|       "minima": {
  1102|         "block_if_any_required_not_ok": true
  1103|       },
  1104|       "notes": [
  1105|         "Generated from contracts/domain_identity_keys_v2.json.",
  1106|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1107|       ],
  1108|       "required_items": [
  1109|         "view_template.def_hash"
  1110|       ],
  1111|       "sig_hash_schema": "view_templates_renderings_drafting.sig_hash.v1"
  1112|     },
  1113|     "view_templates_schedules": {
  1114|       "allowed_item_prefixes": [
  1115|         "view_template.sig."
  1116|       ],
  1117|       "allowed_items": [
  1118|         "view_template.def_hash"
  1119|       ],
  1120|       "hash_alg": "md5_utf8_join_pipe",
  1121|       "minima": {
  1122|         "block_if_any_required_not_ok": true
  1123|       },
  1124|       "notes": [
  1125|         "Generated from contracts/domain_identity_keys_v2.json.",
  1126|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1127|       ],
  1128|       "required_items": [
  1129|         "view_template.def_hash"
  1130|       ],
  1131|       "sig_hash_schema": "view_templates_schedules.sig_hash.v1"
  1132|     },
  1133|     "wall_types": {
  1134|       "allowed_item_prefixes": [],
  1135|       "allowed_items": [
  1136|         "wt.function",
  1137|         "wt.layer_count",
  1138|         "wt.total_thickness_in",
  1139|         "wt.stack_hash_loose",
  1140|         "wt.wraps_at_inserts",
  1141|         "wt.wraps_at_ends",
  1142|         "wt.kind",
  1143|         "wt.total_layer_rows",
  1144|         "wt.stack_hash_strict",
  1145|         "wt.stack_hash_function_only",
  1146|         "wt.coarse_fill_pattern_sig_hash",
  1147|         "wt.has_embedded_sweeps",
  1148|         "wt.type_name",
  1149|         "wt.coarse_fill_color_rgb"
  1150|       ],
  1151|       "hash_alg": "md5_utf8_join_pipe",
  1152|       "minima": {
  1153|         "block_if_any_required_not_ok": true
  1154|       },
  1155|       "notes": [
  1156|         "Generated from contracts/domain_identity_keys_v2.json.",
  1157|         "sig_hash is computed post-extraction from canonical identity_basis.items."
  1158|       ],
  1159|       "required_items": [
  1160|         "wt.layer_count",
  1161|         "wt.total_thickness_in",
  1162|         "wt.stack_hash_loose"
  1163|       ],
  1164|       "sig_hash_schema": "wall_types.sig_hash.v1"
  1165|     },
  1166|     "worksets": {
  1167|       "allowed_item_prefixes": [],
  1168|       "allowed_items": [
  1169|         "workset.is_default_workset",
  1170|         "workset.kind",
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
```
