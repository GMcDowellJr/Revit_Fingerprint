# Chunk of contracts/domain_identity_keys_v2.json

- Source relative path: `contracts/domain_identity_keys_v2.json`
- Chunk: 3 of 4
- Original line range: 781-1180
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 97b5e6834d04d3f21f92697078fa5f994471002da74d6487c4bc46e4cab7ad10
- Starts inside symbol: no
- Ends inside symbol: no

```
   781|         "lft.type_parameter_schema_hash is the primary behavioral anchor for sig_hash.",
   782|         "lft.shape_gate.category_id is allowed but excluded from sig/join: Revit-version-specific int.",
   783|         "lft.family_is_editable and lft.family_is_in_place are discrimination signals (loaded vs system).",
   784|         "lft.structural_material_type is read from the family's representative FamilySymbol (Area 12 probe: constant across a family's types, unique_value_count=1 in every sampled case).",
   785|         "lft.is_active is a per-symbol (type) property, not per-family, and was observed to vary within a family in the Area 12 probe corpus; it is aggregated across all of a family's types to true/false/partial rather than read off a single arbitrary type, so the family record stays independent of collect_types() ordering.",
   786|         "sig_hash = f(category + schema_hash + is_in_place + is_editable + counts + structural_material_type + is_active).",
   787|         "sig_hash_schema is pinned to loaded_family_types.sig_hash.v2 (not the generator's default loaded_family_types.sig_hash.v1) because Area 12 changed the sig_hash preimage composition -- tools/generate_sig_hash_policy.py's build_policy() defaults any domain without this override to '<domain>.sig_hash.v1', which would silently erase the version bump on the next regeneration and make post-Area-12 hashes appear compatible with the old preimage (PR review follow-up, same precedent as the identity domain's D-025 sig_hash_schema pin above).",
   788|         "join_hash discovery: schema_hash is the behavioral anchor; individual parameter value items",
   789|         "  (from parameter_rows) are future discovery work (see domain future-steps)."
   790|       ]
   791|     },
   792|     "materials": {
   793|       "domain_family": "materials",
   794|       "display_label": "Materials",
   795|       "allowed_keys": [
   796|         "material.graphics_sig_hash_v2",
   797|         "material.uid",
   798|         "material.class",
   799|         "material.keynote",
   800|         "material.name_class_hash",
   801|         "material.manufacturer",
   802|         "material.model"
   803|       ],
   804|       "required_keys": [
   805|         "material.graphics_sig_hash_v2",
   806|         "material.name_class_hash"
   807|       ],
   808|       "sig_hash_schema": "materials.sig_hash.v2",
   809|       "sig_hash_keys": [
   810|         "material.name_class_hash"
   811|       ],
   812|       "sig_hash_key_prefixes": [],
   813|       "minima": {
   814|         "block_if_any_required_not_ok": true
   815|       },
   816|       "notes": [
   817|         "sig_hash basis: md5(material.name | material.class). Answers semantic identity.",
   818|         "join_hash basis: md5(material.graphics_sig_hash_v2 | material.keynote). Answers governance equivalence.",
   819|         "material.graphics_sig_hash_v2 retained as identity_item for visual consolidation analysis.",
   820|         "material.uid retained for record traceability only — not a sig or join anchor.",
   821|         "material.name_class_hash = md5(name | class), emitted by migration script and extractor.",
   822|         "material.manufacturer and material.model are allowed optional discovery candidates emitted by the migration script when present.",
   823|         "Subset policy decision: recognized exception; materials sig_hash and join_hash use intentionally disjoint preimages."
   824|       ]
   825|     },
   826|     "object_styles_analytical": {
   827|       "domain_family": "object_styles",
   828|       "display_label": "Object Styles — Analytical",
   829|       "allowed_keys": [
   830|         "obj_style.row_key",
   831|         "obj_style.weight.projection",
   832|         "obj_style.color.rgb",
   833|         "obj_style.pattern_ref.sig_hash",
   834|         "obj_style.can_add_subcategory",
   835|         "obj_style.has_material_quantities",
   836|         "obj_style.is_cuttable",
   837|         "obj_style.parent_name"
   838|       ],
   839|       "sig_hash_keys": [
   840|         "obj_style.row_key",
   841|         "obj_style.weight.projection",
   842|         "obj_style.color.rgb",
   843|         "obj_style.pattern_ref.sig_hash"
   844|       ],
   845|       "required_keys": [
   846|         "obj_style.row_key"
   847|       ],
   848|       "minima": {
   849|         "block_if_any_required_not_ok": true
   850|       }
   851|     },
   852|     "object_styles_annotation": {
   853|       "domain_family": "object_styles",
   854|       "display_label": "Object Styles — Annotation",
   855|       "allowed_keys": [
   856|         "obj_style.row_key",
   857|         "obj_style.weight.projection",
   858|         "obj_style.color.rgb",
   859|         "obj_style.pattern_ref.sig_hash",
   860|         "obj_style.can_add_subcategory",
   861|         "obj_style.has_material_quantities",
   862|         "obj_style.is_cuttable",
   863|         "obj_style.parent_name"
   864|       ],
   865|       "sig_hash_keys": [
   866|         "obj_style.row_key",
   867|         "obj_style.weight.projection",
   868|         "obj_style.color.rgb",
   869|         "obj_style.pattern_ref.sig_hash"
   870|       ],
   871|       "required_keys": [
   872|         "obj_style.row_key"
   873|       ],
   874|       "minima": {
   875|         "block_if_any_required_not_ok": true
   876|       }
   877|     },
   878|     "object_styles_imported": {
   879|       "domain_family": "object_styles",
   880|       "allowed_keys": [
   881|         "obj_style.row_key",
   882|         "obj_style.weight.projection",
   883|         "obj_style.color.rgb",
   884|         "obj_style.pattern_ref.sig_hash",
   885|         "obj_style.can_add_subcategory",
   886|         "obj_style.has_material_quantities",
   887|         "obj_style.is_cuttable",
   888|         "obj_style.parent_name"
   889|       ],
   890|       "sig_hash_keys": [
   891|         "obj_style.row_key",
   892|         "obj_style.weight.projection",
   893|         "obj_style.color.rgb",
   894|         "obj_style.pattern_ref.sig_hash"
   895|       ],
   896|       "required_keys": [
   897|         "obj_style.row_key"
   898|       ],
   899|       "minima": {
   900|         "block_if_any_required_not_ok": true
   901|       }
   902|     },
   903|     "object_styles_model": {
   904|       "domain_family": "object_styles",
   905|       "display_label": "Object Styles — Model",
   906|       "allowed_keys": [
   907|         "obj_style.row_key",
   908|         "obj_style.weight.projection",
   909|         "obj_style.weight.cut",
   910|         "obj_style.color.rgb",
   911|         "obj_style.pattern_ref.sig_hash",
   912|         "obj_style.material_sig_hash",
   913|         "obj_style.can_add_subcategory",
   914|         "obj_style.has_material_quantities",
   915|         "obj_style.is_cuttable",
   916|         "obj_style.parent_name"
   917|       ],
   918|       "sig_hash_keys": [
   919|         "obj_style.row_key",
   920|         "obj_style.weight.projection",
   921|         "obj_style.weight.cut",
   922|         "obj_style.color.rgb",
   923|         "obj_style.pattern_ref.sig_hash",
   924|         "obj_style.material_sig_hash"
   925|       ],
   926|       "required_keys": [
   927|         "obj_style.row_key"
   928|       ],
   929|       "minima": {
   930|         "block_if_any_required_not_ok": true
   931|       }
   932|     },
   933|     "phase_filters": {
   934|       "domain_family": "phase_filters",
   935|       "display_label": "Phase Filters",
   936|       "allowed_keys": [
   937|         "phase_filter.demolished.presentation_id",
   938|         "phase_filter.existing.presentation_id",
   939|         "phase_filter.new.presentation_id",
   940|         "phase_filter.temporary.presentation_id"
   941|       ],
   942|       "allowed_key_prefixes": [],
   943|       "required_keys": [
   944|         "phase_filter.demolished.presentation_id",
   945|         "phase_filter.existing.presentation_id",
   946|         "phase_filter.new.presentation_id",
   947|         "phase_filter.temporary.presentation_id"
   948|       ],
   949|       "minima": {
   950|         "block_if_any_required_not_ok": true
   951|       },
   952|       "indexed_key_rules": {}
   953|     },
   954|     "phases": {
   955|       "domain_family": "phases",
   956|       "display_label": "Phases",
   957|       "allowed_keys": [
   958|         "phase.seq",
   959|         "phase.name"
   960|       ],
   961|       "allowed_key_prefixes": [],
   962|       "required_keys": [
   963|         "phase.seq",
   964|         "phase.name"
   965|       ],
   966|       "minima": {
   967|         "block_if_any_required_not_ok": true
   968|       },
   969|       "indexed_key_rules": {}
   970|     },
   971|     "roof_types": {
   972|       "domain_family": "compound_types",
   973|       "display_label": "Roof Types",
   974|       "allowed_keys": [
   975|         "rt.layer_count",
   976|         "rt.total_thickness_in",
   977|         "rt.stack_hash_loose",
   978|         "rt.total_layer_rows",
   979|         "rt.stack_hash_strict",
   980|         "rt.stack_hash_function_only",
   981|         "rt.coarse_fill_pattern_sig_hash",
   982|         "rt.type_name",
   983|         "rt.coarse_fill_color_rgb"
   984|       ],
   985|       "required_keys": [
   986|         "rt.layer_count",
   987|         "rt.total_thickness_in",
   988|         "rt.stack_hash_loose"
   989|       ],
   990|       "required": [
   991|         "rt.layer_count",
   992|         "rt.total_thickness_in",
   993|         "rt.stack_hash_loose"
   994|       ],
   995|       "optional": [
   996|         "rt.total_layer_rows",
   997|         "rt.stack_hash_strict",
   998|         "rt.stack_hash_function_only",
   999|         "rt.coarse_fill_pattern_sig_hash",
  1000|         "rt.type_name",
  1001|         "rt.coarse_fill_color_rgb"
  1002|       ],
  1003|       "minima": {
  1004|         "block_if_any_required_not_ok": true
  1005|       }
  1006|     },
  1007|     "text_types": {
  1008|       "domain_family": "text_types",
  1009|       "display_label": "Text Types",
  1010|       "allowed_keys": [
  1011|         "text_type.font",
  1012|         "text_type.size_in",
  1013|         "text_type.bold",
  1014|         "text_type.italic",
  1015|         "text_type.underline",
  1016|         "text_type.color_rgb",
  1017|         "text_type.width_factor",
  1018|         "text_type.background",
  1019|         "text_type.line_weight",
  1020|         "text_type.show_border",
  1021|         "text_type.leader_border_offset_in",
  1022|         "text_type.tab_size_in",
  1023|         "text_type.leader_arrowhead_sig_hash",
  1024|         "text_type.leader_arrowhead_uid",
  1025|         "text_type.leader_arrowhead_name"
  1026|       ],
  1027|       "allowed_key_prefixes": [],
  1028|       "sig_hash_keys": [
  1029|         "text_type.font",
  1030|         "text_type.size_in",
  1031|         "text_type.bold",
  1032|         "text_type.italic",
  1033|         "text_type.underline",
  1034|         "text_type.color_rgb",
  1035|         "text_type.width_factor",
  1036|         "text_type.background",
  1037|         "text_type.line_weight",
  1038|         "text_type.show_border",
  1039|         "text_type.leader_border_offset_in",
  1040|         "text_type.tab_size_in"
  1041|       ],
  1042|       "required_keys": [
  1043|         "text_type.font",
  1044|         "text_type.size_in",
  1045|         "text_type.bold",
  1046|         "text_type.italic",
  1047|         "text_type.underline",
  1048|         "text_type.color_rgb",
  1049|         "text_type.width_factor"
  1050|       ],
  1051|       "minima": {
  1052|         "block_if_any_required_not_ok": true
  1053|       },
  1054|       "indexed_key_rules": {},
  1055|       "notes": [
  1056|         "text_type.leader_arrowhead_sig_hash/_uid/_name are registered as allowed_keys (governed identity data, visible for join-key/pattern analysis) but deliberately excluded from sig_hash_keys: leader_arrowhead_uid is file-local (D-004 restricts UID use to element-backed identities), leader_arrowhead_name is cosmetic/presentation metadata, and promoting leader_arrowhead_sig_hash itself to hash-contributing is a separate future decision tied to the paused hash-composition discussion, not bundled into this registration change.",
  1057|         "Without this sig_hash_keys override, tools/generate_sig_hash_policy.py would default the sig_hash preimage to every allowed_keys entry on the next policy regeneration, silently pulling the UID/name fields into text_types sig_hash and making identical text types hash differently purely due to per-file arrowhead UID/name — see core/sig_hash_builder.py's build_sig_hash_from_policy, which hashes every policy-allowed item, not just required_keys."
  1058|       ]
  1059|     },
  1060|     "units": {
  1061|       "domain_family": "units",
  1062|       "display_label": "Units",
  1063|       "allowed_keys": [
  1064|         "units.spec",
  1065|         "units.unit_type_id",
  1066|         "units.symbol_type_id",
  1067|         "units.accuracy",
  1068|         "units.rounding_method",
  1069|         "units.use_default",
  1070|         "units.use_digit_grouping",
  1071|         "units.use_plus_prefix",
  1072|         "units.suppress_leading_zeros",
  1073|         "units.suppress_spaces",
  1074|         "units.suppress_trailing_zeros"
  1075|       ],
  1076|       "required_keys": [
  1077|         "units.spec",
  1078|         "units.unit_type_id"
  1079|       ],
  1080|       "minima": {
  1081|         "block_if_any_required_not_ok": true
  1082|       }
  1083|     },
  1084|     "units_doc": {
  1085|       "domain_family": "units",
  1086|       "display_label": "Units (Document Summary)",
  1087|       "allowed_keys": [
  1088|         "units_doc.decimal_symbol",
  1089|         "units_doc.digit_grouping_amount",
  1090|         "units_doc.digit_grouping_symbol"
  1091|       ],
  1092|       "required_keys": [],
  1093|       "minima": {
  1094|         "block_if_any_required_not_ok": false
  1095|       }
  1096|     },
  1097|     "view_category_overrides_annotation": {
  1098|       "domain_family": "view_category_overrides",
  1099|       "display_label": "View Category Overrides — Annotation",
  1100|       "allowed_keys": [
  1101|         "vco.baseline_category_path",
  1102|         "vco.baseline_sig_hash",
  1103|         "vco.override_properties_hash",
  1104|         "vco.projection.line_weight",
  1105|         "vco.projection.color.rgb",
  1106|         "vco.projection.pattern_ref.sig_hash",
  1107|         "vco.projection.fill_pattern_ref.sig_hash",
  1108|         "vco.projection.fill_color.rgb",
  1109|         "vco.halftone",
  1110|         "vco.transparency",
  1111|         "vco.include_controlled",
  1112|         "vco.vg_category_type",
  1113|         "vco.context_type",
  1114|         "vco.template_name",
  1115|         "vco.template_element_id",
  1116|         "vco.template_unique_id"
  1117|       ],
  1118|       "required_keys": [
  1119|         "vco.baseline_category_path",
  1120|         "vco.baseline_sig_hash",
  1121|         "vco.override_properties_hash"
  1122|       ],
  1123|       "minima": {
  1124|         "block_if_any_required_not_ok": true
  1125|       },
  1126|       "notes": [
  1127|         "Domain family: view_category_overrides. Annotation category partition.",
  1128|         "Cut properties not applicable to annotation categories — omitted from allowed_keys."
  1129|       ]
  1130|     },
  1131|     "view_category_overrides_model": {
  1132|       "domain_family": "view_category_overrides",
  1133|       "display_label": "View Category Overrides — Model",
  1134|       "allowed_keys": [
  1135|         "vco.baseline_category_path",
  1136|         "vco.baseline_sig_hash",
  1137|         "vco.override_properties_hash",
  1138|         "vco.projection.line_weight",
  1139|         "vco.projection.color.rgb",
  1140|         "vco.projection.pattern_ref.sig_hash",
  1141|         "vco.projection.fill_pattern_ref.sig_hash",
  1142|         "vco.projection.fill_color.rgb",
  1143|         "vco.cut.line_weight",
  1144|         "vco.cut.color.rgb",
  1145|         "vco.cut.pattern_ref.sig_hash",
  1146|         "vco.cut.fill_pattern_ref.sig_hash",
  1147|         "vco.cut.fill_color.rgb",
  1148|         "vco.halftone",
  1149|         "vco.transparency",
  1150|         "vco.include_controlled",
  1151|         "vco.vg_category_type",
  1152|         "vco.context_type",
  1153|         "vco.template_name",
  1154|         "vco.template_element_id",
  1155|         "vco.template_unique_id"
  1156|       ],
  1157|       "required_keys": [
  1158|         "vco.baseline_category_path",
  1159|         "vco.baseline_sig_hash",
  1160|         "vco.override_properties_hash"
  1161|       ],
  1162|       "minima": {
  1163|         "block_if_any_required_not_ok": true
  1164|       },
  1165|       "notes": [
  1166|         "Domain family: view_category_overrides. Model category partition.",
  1167|         "Both projection and cut properties applicable for model categories."
  1168|       ]
  1169|     },
  1170|     "view_filter_applications_view_templates": {
  1171|       "domain_family": "view_filter_applications_view_templates",
  1172|       "display_label": "View Filter Applications",
  1173|       "allowed_keys": [
  1174|         "vfa.stack_def_hash"
  1175|       ],
  1176|       "required_keys": [
  1177|         "vfa.stack_def_hash"
  1178|       ],
  1179|       "minima": {
  1180|         "block_if_any_required_not_ok": true
```
