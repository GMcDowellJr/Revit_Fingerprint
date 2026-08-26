# Chunk of policies/domain_sig_hash_policies.json

- Source relative path: `policies/domain_sig_hash_policies.json`
- Chunk: 2 of 4
- Original line range: 391-790
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: f9574026a45f473063889e6bfa400737885177fa01bec6ddd2c3a96308a708dd
- Starts inside symbol: no
- Ends inside symbol: no

```
   391|         "dim_type.top_indicator_as_prefix_suffix",
   392|         "dim_type.bottom_indicator_as_prefix_suffix",
   393|         "dim_type.text_orientation",
   394|         "dim_type.text_location",
   395|         "dim_type.symbol_name"
   396|       ],
   397|       "sig_hash_schema": "dimension_types_spot_elevation.sig_hash.v2"
   398|     },
   399|     "dimension_types_spot_slope": {
   400|       "allowed_item_prefixes": [],
   401|       "allowed_items": [
   402|         "dim_type.shape",
   403|         "dim_type.unit_format_id",
   404|         "dim_type.slope_direction",
   405|         "dim_type.leader_line_length",
   406|         "dim_type.leader_arrowhead_sig_hash",
   407|         "dim_type.text_font",
   408|         "dim_type.text_size_in",
   409|         "dim_type.text_bold",
   410|         "dim_type.text_italic",
   411|         "dim_type.text_underline",
   412|         "dim_type.text_width_factor",
   413|         "dim_type.text_background",
   414|         "dim_type.color_rgb",
   415|         "dim_type.line_weight",
   416|         "dim_type.suppress_spaces",
   417|         "dim_type.leader_arrowhead_line_weight",
   418|         "dim_type.leader_line_weight",
   419|         "dim_type.rotate_with_component",
   420|         "dim_type.text_offset_from_leader_in",
   421|         "dim_type.alternate_units",
   422|         "dim_type.alternate_units_prefix",
   423|         "dim_type.alternate_units_suffix"
   424|       ],
   425|       "hash_alg": "md5_utf8_join_pipe",
   426|       "minima": {
   427|         "block_if_any_required_not_ok": true
   428|       },
   429|       "notes": [
   430|         "Generated from contracts/domain_identity_keys_v2.json.",
   431|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   432|       ],
   433|       "required_items": [
   434|         "dim_type.shape",
   435|         "dim_type.unit_format_id",
   436|         "dim_type.slope_direction",
   437|         "dim_type.leader_line_length"
   438|       ],
   439|       "sig_hash_schema": "dimension_types_spot_slope.sig_hash.v2"
   440|     },
   441|     "fill_patterns_drafting": {
   442|       "allowed_item_prefixes": [
   443|         "fill_pattern.grid["
   444|       ],
   445|       "allowed_items": [
   446|         "fill_pattern.target",
   447|         "fill_pattern.grid_count",
   448|         "fill_pattern.grids_def_hash"
   449|       ],
   450|       "hash_alg": "md5_utf8_join_pipe",
   451|       "minima": {
   452|         "block_if_any_required_not_ok": true
   453|       },
   454|       "notes": [
   455|         "Generated from contracts/domain_identity_keys_v2.json.",
   456|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   457|       ],
   458|       "required_items": [
   459|         "fill_pattern.target",
   460|         "fill_pattern.grid_count",
   461|         "fill_pattern.grids_def_hash"
   462|       ],
   463|       "sig_hash_schema": "fill_patterns_drafting.sig_hash.v1"
   464|     },
   465|     "fill_patterns_model": {
   466|       "allowed_item_prefixes": [
   467|         "fill_pattern.grid["
   468|       ],
   469|       "allowed_items": [
   470|         "fill_pattern.target",
   471|         "fill_pattern.grid_count",
   472|         "fill_pattern.grids_def_hash"
   473|       ],
   474|       "hash_alg": "md5_utf8_join_pipe",
   475|       "minima": {
   476|         "block_if_any_required_not_ok": true
   477|       },
   478|       "notes": [
   479|         "Generated from contracts/domain_identity_keys_v2.json.",
   480|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   481|       ],
   482|       "required_items": [
   483|         "fill_pattern.target",
   484|         "fill_pattern.grid_count",
   485|         "fill_pattern.grids_def_hash"
   486|       ],
   487|       "sig_hash_schema": "fill_patterns_model.sig_hash.v1"
   488|     },
   489|     "floor_types": {
   490|       "allowed_item_prefixes": [],
   491|       "allowed_items": [
   492|         "ft.function",
   493|         "ft.layer_count",
   494|         "ft.total_thickness_in",
   495|         "ft.stack_hash_loose",
   496|         "ft.total_layer_rows",
   497|         "ft.stack_hash_strict",
   498|         "ft.stack_hash_function_only",
   499|         "ft.coarse_fill_pattern_sig_hash",
   500|         "ft.has_embedded_sweeps",
   501|         "ft.type_name",
   502|         "ft.coarse_fill_color_rgb"
   503|       ],
   504|       "hash_alg": "md5_utf8_join_pipe",
   505|       "minima": {
   506|         "block_if_any_required_not_ok": true
   507|       },
   508|       "notes": [
   509|         "Generated from contracts/domain_identity_keys_v2.json.",
   510|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   511|       ],
   512|       "required_items": [
   513|         "ft.layer_count",
   514|         "ft.total_thickness_in",
   515|         "ft.stack_hash_loose"
   516|       ],
   517|       "sig_hash_schema": "floor_types.sig_hash.v1"
   518|     },
   519|     "identity": {
   520|       "allowed_item_prefixes": [],
   521|       "allowed_items": [
   522|         "identity.is_workshared",
   523|         "identity.revit_version_number",
   524|         "identity.revit_version_name",
   525|         "identity.revit_build",
   526|         "project_info.name",
   527|         "project_info.number",
   528|         "project_info.status",
   529|         "project_info.address",
   530|         "project_info.issue_date",
   531|         "project_info.client_name",
   532|         "project_info.building_name",
   533|         "project_info.organization_name",
   534|         "project_info.organization_description",
   535|         "project_info.ifc_building_guid",
   536|         "project_info.ifc_project_guid",
   537|         "project_info.ifc_site_guid",
   538|         "project_info.business_center"
   539|       ],
   540|       "hash_alg": "md5_utf8_join_pipe",
   541|       "minima": {
   542|         "block_if_any_required_not_ok": true
   543|       },
   544|       "notes": [
   545|         "Generated from contracts/domain_identity_keys_v2.json.",
   546|         "sig_hash is computed post-extraction from canonical identity_basis.items.",
   547|         "project_info.* added by D-025 (identity domain expansion); hand-patched here rather than",
   548|         "via a full tools/generate_sig_hash_policy.py regen, which would also clobber unrelated",
   549|         "hand-tuned notes on other domains that have drifted from a strict mechanical regen."
   550|       ],
   551|       "required_items": [
   552|         "identity.is_workshared"
   553|       ],
   554|       "sig_hash_schema": "identity.sig_hash.v2"
   555|     },
   556|     "line_patterns": {
   557|       "allowed_item_prefixes": [],
   558|       "allowed_items": [
   559|         "line_pattern.segment_count",
   560|         "line_pattern.segments_def_hash"
   561|       ],
   562|       "hash_alg": "md5_utf8_join_pipe",
   563|       "minima": {
   564|         "block_if_any_required_not_ok": true
   565|       },
   566|       "notes": [
   567|         "sig_hash uses segment_count + segments_def_hash: matches extractor preimage exactly.",
   568|         "segments_norm_hash excluded from sig_hash — belongs in join_hash only (policy v3).",
   569|         "sig_hash distinguishes scale variants; join_hash collapses them for governance equivalence.",
   570|         "Cross-domain refs (obj_style.pattern_ref.sig_hash, line_style.pattern_ref.sig_hash) are written",
   571|         "at extraction time from ctx[line_pattern_uid_to_hash] which holds the extractor-computed sig_hash.",
   572|         "Post-extraction sig_hash recomputation must produce the same value for ctx refs to remain valid.",
   573|         "Indexed seg[NNN].* items are in identity_basis.items (canonical evidence) but NOT in the preimage;",
   574|         "allowed_item_prefixes is [] so the policy stage hashes only segment_count + segments_def_hash.",
   575|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   576|       ],
   577|       "required_items": [
   578|         "line_pattern.segment_count"
   579|       ],
   580|       "sig_hash_schema": "line_patterns.sig_hash.v2"
   581|     },
   582|     "line_styles": {
   583|       "allowed_item_prefixes": [],
   584|       "allowed_items": [
   585|         "line_style.weight.projection",
   586|         "line_style.color.rgb",
   587|         "line_style.weight.cut",
   588|         "line_style.pattern_ref.sig_hash"
   589|       ],
   590|       "hash_alg": "md5_utf8_join_pipe",
   591|       "minima": {
   592|         "block_if_any_required_not_ok": true
   593|       },
   594|       "notes": [
   595|         "Generated from contracts/domain_identity_keys_v2.json.",
   596|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   597|       ],
   598|       "required_items": [
   599|         "line_style.weight.projection",
   600|         "line_style.color.rgb",
   601|         "line_style.pattern_ref.sig_hash"
   602|       ],
   603|       "sig_hash_schema": "line_styles.sig_hash.v1"
   604|     },
   605|     "loaded_family_types": {
   606|       "allowed_item_prefixes": [],
   607|       "allowed_items": [
   608|         "lft.shape_gate.category",
   609|         "lft.shape_gate.category_id",
   610|         "lft.type_parameter_schema_hash",
   611|         "lft.type_parameter_count",
   612|         "lft.family_is_in_place",
   613|         "lft.family_is_editable",
   614|         "lft.family_symbol_count",
   615|         "lft.type_count",
   616|         "lft.structural_material_type",
   617|         "lft.is_active"
   618|       ],
   619|       "hash_alg": "md5_utf8_join_pipe",
   620|       "minima": {
   621|         "block_if_any_required_not_ok": true
   622|       },
   623|       "notes": [
   624|         "Generated from contracts/domain_identity_keys_v2.json.",
   625|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   626|       ],
   627|       "required_items": [
   628|         "lft.shape_gate.category",
   629|         "lft.type_parameter_schema_hash"
   630|       ],
   631|       "sig_hash_schema": "loaded_family_types.sig_hash.v2"
   632|     },
   633|     "materials": {
   634|       "allowed_item_prefixes": [],
   635|       "allowed_items": [
   636|         "material.name_class_hash"
   637|       ],
   638|       "hash_alg": "md5_utf8_join_pipe",
   639|       "minima": {
   640|         "block_if_any_required_not_ok": true
   641|       },
   642|       "notes": [
   643|         "Generated from contracts/domain_identity_keys_v2.json.",
   644|         "sig_hash_keys pinned to material.name_class_hash so the post-extraction",
   645|         "policy stage preserves the md5(name|class) identity basis instead of",
   646|         "falling back to allowed_keys (which would re-anchor on graphics_sig_hash_v2).",
   647|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   648|       ],
   649|       "required_items": [
   650|         "material.graphics_sig_hash_v2",
   651|         "material.name_class_hash"
   652|       ],
   653|       "sig_hash_schema": "materials.sig_hash.v2"
   654|     },
   655|     "object_styles_analytical": {
   656|       "allowed_item_prefixes": [],
   657|       "allowed_items": [
   658|         "obj_style.row_key",
   659|         "obj_style.weight.projection",
   660|         "obj_style.color.rgb",
   661|         "obj_style.pattern_ref.sig_hash"
   662|       ],
   663|       "hash_alg": "md5_utf8_join_pipe",
   664|       "minima": {
   665|         "block_if_any_required_not_ok": true
   666|       },
   667|       "notes": [
   668|         "Generated from contracts/domain_identity_keys_v2.json.",
   669|         "sig_hash is computed post-extraction from canonical identity_basis.items.",
   670|         "sig_hash_keys pinned in the registry to the pre-existing allowed_keys set so the Area 9",
   671|         "additions (can_add_subcategory/has_material_quantities/is_cuttable/parent_name) register",
   672|         "as identity_basis.items without widening the sig_hash preimage -- open design question,",
   673|         "see CHANGELOG.md/PR description."
   674|       ],
   675|       "required_items": [
   676|         "obj_style.row_key"
   677|       ],
   678|       "sig_hash_schema": "object_styles_analytical.sig_hash.v1"
   679|     },
   680|     "object_styles_annotation": {
   681|       "allowed_item_prefixes": [],
   682|       "allowed_items": [
   683|         "obj_style.row_key",
   684|         "obj_style.weight.projection",
   685|         "obj_style.color.rgb",
   686|         "obj_style.pattern_ref.sig_hash"
   687|       ],
   688|       "hash_alg": "md5_utf8_join_pipe",
   689|       "minima": {
   690|         "block_if_any_required_not_ok": true
   691|       },
   692|       "notes": [
   693|         "Generated from contracts/domain_identity_keys_v2.json.",
   694|         "sig_hash is computed post-extraction from canonical identity_basis.items.",
   695|         "sig_hash_keys pinned in the registry to the pre-existing allowed_keys set so the Area 9",
   696|         "additions (can_add_subcategory/has_material_quantities/is_cuttable/parent_name) register",
   697|         "as identity_basis.items without widening the sig_hash preimage -- open design question,",
   698|         "see CHANGELOG.md/PR description."
   699|       ],
   700|       "required_items": [
   701|         "obj_style.row_key"
   702|       ],
   703|       "sig_hash_schema": "object_styles_annotation.sig_hash.v1"
   704|     },
   705|     "object_styles_imported": {
   706|       "allowed_item_prefixes": [],
   707|       "allowed_items": [
   708|         "obj_style.row_key",
   709|         "obj_style.weight.projection",
   710|         "obj_style.color.rgb",
   711|         "obj_style.pattern_ref.sig_hash"
   712|       ],
   713|       "hash_alg": "md5_utf8_join_pipe",
   714|       "minima": {
   715|         "block_if_any_required_not_ok": true
   716|       },
   717|       "notes": [
   718|         "Generated from contracts/domain_identity_keys_v2.json.",
   719|         "sig_hash is computed post-extraction from canonical identity_basis.items.",
   720|         "sig_hash_keys pinned in the registry to the pre-existing allowed_keys set so the Area 9",
   721|         "additions (can_add_subcategory/has_material_quantities/is_cuttable/parent_name) register",
   722|         "as identity_basis.items without widening the sig_hash preimage -- open design question,",
   723|         "see CHANGELOG.md/PR description."
   724|       ],
   725|       "required_items": [
   726|         "obj_style.row_key"
   727|       ],
   728|       "sig_hash_schema": "object_styles_imported.sig_hash.v1"
   729|     },
   730|     "object_styles_model": {
   731|       "allowed_item_prefixes": [],
   732|       "allowed_items": [
   733|         "obj_style.row_key",
   734|         "obj_style.weight.projection",
   735|         "obj_style.weight.cut",
   736|         "obj_style.color.rgb",
   737|         "obj_style.pattern_ref.sig_hash",
   738|         "obj_style.material_sig_hash"
   739|       ],
   740|       "hash_alg": "md5_utf8_join_pipe",
   741|       "minima": {
   742|         "block_if_any_required_not_ok": true
   743|       },
   744|       "notes": [
   745|         "Generated from contracts/domain_identity_keys_v2.json.",
   746|         "sig_hash is computed post-extraction from canonical identity_basis.items.",
   747|         "sig_hash_keys pinned in the registry to the pre-existing allowed_keys set so the Area 9",
   748|         "additions (can_add_subcategory/has_material_quantities/is_cuttable/parent_name) register",
   749|         "as identity_basis.items without widening the sig_hash preimage -- open design question,",
   750|         "see CHANGELOG.md/PR description."
   751|       ],
   752|       "required_items": [
   753|         "obj_style.row_key"
   754|       ],
   755|       "sig_hash_schema": "object_styles_model.sig_hash.v1"
   756|     },
   757|     "phase_filters": {
   758|       "allowed_item_prefixes": [],
   759|       "allowed_items": [
   760|         "phase_filter.demolished.presentation_id",
   761|         "phase_filter.existing.presentation_id",
   762|         "phase_filter.new.presentation_id",
   763|         "phase_filter.temporary.presentation_id"
   764|       ],
   765|       "hash_alg": "md5_utf8_join_pipe",
   766|       "minima": {
   767|         "block_if_any_required_not_ok": true
   768|       },
   769|       "notes": [
   770|         "sig_hash uses all 4 phase status presentation_id items: matches extractor preimage exactly.",
   771|         "phase_filter.name excluded from allowed_items — it is cosmetic/coordination, not in semantic_keys.",
   772|         "Prior policy incorrectly included phase_filter.name in allowed_items.",
   773|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   774|       ],
   775|       "required_items": [
   776|         "phase_filter.demolished.presentation_id",
   777|         "phase_filter.existing.presentation_id",
   778|         "phase_filter.new.presentation_id",
   779|         "phase_filter.temporary.presentation_id"
   780|       ],
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
```
