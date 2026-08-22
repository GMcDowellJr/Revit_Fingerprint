# Chunk of contracts/domain_identity_keys_v2.json

- Source relative path: `contracts/domain_identity_keys_v2.json`
- Chunk: 2 of 4
- Original line range: 391-790
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 97b5e6834d04d3f21f92697078fa5f994471002da74d6487c4bc46e4cab7ad10
- Starts inside symbol: no
- Ends inside symbol: no

```
   391|       ],
   392|       "notes": [
   393|         "Area 7 §1 implements the previously-reserved (allowed-but-unimplemented) dim_type.leader_arrowhead_sig_hash via a shared core/dimension_type_helpers._read_leader_arrowhead() helper (also used by spot_elevation/spot_slope), and additionally registers dim_type.leader_arrowhead_uid/dim_type.leader_arrowhead_name -- same Area 10 precedent (text_type.leader_arrowhead_uid/_name) for file-local/cosmetic metadata that should be visible for join-key/pattern analysis but not hash-contributing.",
   394|         "sig_hash_keys is pinned to the pre-Area-7 allowed_keys plus every genuinely-new Area 7 key EXCEPT dim_type.leader_arrowhead_uid and dim_type.leader_arrowhead_name: leader_arrowhead_uid is file-local (D-004 restricts UID use to element-backed identities) and leader_arrowhead_name is cosmetic/presentation metadata, so both are excluded the same way Area 10 excluded text_type.leader_arrowhead_uid/_name. dim_type.leader_arrowhead_sig_hash itself is retained in sig_hash_keys (it was already an allowed_keys entry before this PR, and is content-derived from the referenced arrowhead, not file-local/cosmetic). Without this override, tools/generate_sig_hash_policy.py would default the sig_hash preimage to every allowed_keys entry including the uid/name pair on the next policy regeneration -- see core/sig_hash_builder.py's build_sig_hash_from_policy, which hashes every policy-allowed item, not just required_keys.",
   395|         "Area 7 §5/§6/§7 (alternate units, suppress_spaces, rotate_with_component, coordinate_base, text offsets) are non-required content-driven enrichment items, included in sig_hash_keys like any other identity item -- verified against three consistent runs from the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass.",
   396|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained."
   397|       ],
   398|       "sig_hash_schema": "dimension_types_spot_coordinate.sig_hash.v2"
   399|     },
   400|     "dimension_types_spot_elevation": {
   401|       "domain_family": "dimension_types",
   402|       "display_label": "Dimension Types — Spot Elevation",
   403|       "allowed_keys": [
   404|         "dim_type.shape",
   405|         "dim_type.unit_format_id",
   406|         "dim_type.elevation_indicator",
   407|         "dim_type.elevation_indicator_as_prefix_suffix",
   408|         "dim_type.top_indicator",
   409|         "dim_type.bottom_indicator",
   410|         "dim_type.top_indicator_as_prefix_suffix",
   411|         "dim_type.bottom_indicator_as_prefix_suffix",
   412|         "dim_type.text_orientation",
   413|         "dim_type.text_location",
   414|         "dim_type.symbol_name",
   415|         "dim_type.leader_arrowhead_sig_hash",
   416|         "dim_type.text_font",
   417|         "dim_type.text_size_in",
   418|         "dim_type.text_bold",
   419|         "dim_type.text_italic",
   420|         "dim_type.text_underline",
   421|         "dim_type.text_width_factor",
   422|         "dim_type.text_background",
   423|         "dim_type.color_rgb",
   424|         "dim_type.line_weight",
   425|         "dim_type.suppress_spaces",
   426|         "dim_type.leader_arrowhead_uid",
   427|         "dim_type.leader_arrowhead_name",
   428|         "dim_type.leader_arrowhead_line_weight",
   429|         "dim_type.leader_line_weight",
   430|         "dim_type.rotate_with_component",
   431|         "dim_type.elevation_base",
   432|         "dim_type.text_offset_from_leader_in",
   433|         "dim_type.text_offset_from_symbol_in",
   434|         "dim_type.alternate_units",
   435|         "dim_type.alternate_units_prefix",
   436|         "dim_type.alternate_units_suffix"
   437|       ],
   438|       "required_keys": [
   439|         "dim_type.shape",
   440|         "dim_type.unit_format_id",
   441|         "dim_type.elevation_indicator",
   442|         "dim_type.elevation_indicator_as_prefix_suffix",
   443|         "dim_type.top_indicator",
   444|         "dim_type.bottom_indicator",
   445|         "dim_type.top_indicator_as_prefix_suffix",
   446|         "dim_type.bottom_indicator_as_prefix_suffix",
   447|         "dim_type.text_orientation",
   448|         "dim_type.text_location",
   449|         "dim_type.symbol_name"
   450|       ],
   451|       "minima": {
   452|         "block_if_any_required_not_ok": true
   453|       },
   454|       "sig_hash_keys": [
   455|         "dim_type.shape",
   456|         "dim_type.unit_format_id",
   457|         "dim_type.elevation_indicator",
   458|         "dim_type.elevation_indicator_as_prefix_suffix",
   459|         "dim_type.top_indicator",
   460|         "dim_type.bottom_indicator",
   461|         "dim_type.top_indicator_as_prefix_suffix",
   462|         "dim_type.bottom_indicator_as_prefix_suffix",
   463|         "dim_type.text_orientation",
   464|         "dim_type.text_location",
   465|         "dim_type.symbol_name",
   466|         "dim_type.leader_arrowhead_sig_hash",
   467|         "dim_type.text_font",
   468|         "dim_type.text_size_in",
   469|         "dim_type.text_bold",
   470|         "dim_type.text_italic",
   471|         "dim_type.text_underline",
   472|         "dim_type.text_width_factor",
   473|         "dim_type.text_background",
   474|         "dim_type.color_rgb",
   475|         "dim_type.line_weight",
   476|         "dim_type.suppress_spaces",
   477|         "dim_type.leader_arrowhead_line_weight",
   478|         "dim_type.leader_line_weight",
   479|         "dim_type.rotate_with_component",
   480|         "dim_type.elevation_base",
   481|         "dim_type.text_offset_from_leader_in",
   482|         "dim_type.text_offset_from_symbol_in",
   483|         "dim_type.alternate_units",
   484|         "dim_type.alternate_units_prefix",
   485|         "dim_type.alternate_units_suffix"
   486|       ],
   487|       "notes": [
   488|         "Area 7 §1 implements the previously-reserved (allowed-but-unimplemented) dim_type.leader_arrowhead_sig_hash via a shared core/dimension_type_helpers._read_leader_arrowhead() helper (also used by spot_coordinate/spot_slope), and additionally registers dim_type.leader_arrowhead_uid/dim_type.leader_arrowhead_name -- same Area 10 precedent (text_type.leader_arrowhead_uid/_name) for file-local/cosmetic metadata that should be visible for join-key/pattern analysis but not hash-contributing.",
   489|         "sig_hash_keys is pinned to the pre-Area-7 allowed_keys plus every genuinely-new Area 7 key EXCEPT dim_type.leader_arrowhead_uid and dim_type.leader_arrowhead_name: leader_arrowhead_uid is file-local (D-004 restricts UID use to element-backed identities) and leader_arrowhead_name is cosmetic/presentation metadata, so both are excluded the same way Area 10 excluded text_type.leader_arrowhead_uid/_name. dim_type.leader_arrowhead_sig_hash itself is retained in sig_hash_keys (it was already an allowed_keys entry before this PR, and is content-derived from the referenced arrowhead, not file-local/cosmetic). Without this override, tools/generate_sig_hash_policy.py would default the sig_hash preimage to every allowed_keys entry including the uid/name pair on the next policy regeneration -- see core/sig_hash_builder.py's build_sig_hash_from_policy, which hashes every policy-allowed item, not just required_keys.",
   490|         "Area 7 §5/§6/§7 (alternate units, suppress_spaces, rotate_with_component, elevation_base, text offsets) are non-required content-driven enrichment items, included in sig_hash_keys like any other identity item -- verified against three consistent runs from the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass.",
   491|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained."
   492|       ],
   493|       "sig_hash_schema": "dimension_types_spot_elevation.sig_hash.v2"
   494|     },
   495|     "dimension_types_spot_slope": {
   496|       "domain_family": "dimension_types",
   497|       "display_label": "Dimension Types — Spot Slope",
   498|       "allowed_keys": [
   499|         "dim_type.shape",
   500|         "dim_type.unit_format_id",
   501|         "dim_type.slope_direction",
   502|         "dim_type.leader_line_length",
   503|         "dim_type.leader_arrowhead_sig_hash",
   504|         "dim_type.text_font",
   505|         "dim_type.text_size_in",
   506|         "dim_type.text_bold",
   507|         "dim_type.text_italic",
   508|         "dim_type.text_underline",
   509|         "dim_type.text_width_factor",
   510|         "dim_type.text_background",
   511|         "dim_type.color_rgb",
   512|         "dim_type.line_weight",
   513|         "dim_type.suppress_spaces",
   514|         "dim_type.leader_arrowhead_uid",
   515|         "dim_type.leader_arrowhead_name",
   516|         "dim_type.leader_arrowhead_line_weight",
   517|         "dim_type.leader_line_weight",
   518|         "dim_type.rotate_with_component",
   519|         "dim_type.text_offset_from_leader_in",
   520|         "dim_type.alternate_units",
   521|         "dim_type.alternate_units_prefix",
   522|         "dim_type.alternate_units_suffix"
   523|       ],
   524|       "required_keys": [
   525|         "dim_type.shape",
   526|         "dim_type.unit_format_id",
   527|         "dim_type.slope_direction",
   528|         "dim_type.leader_line_length"
   529|       ],
   530|       "minima": {
   531|         "block_if_any_required_not_ok": true
   532|       },
   533|       "sig_hash_keys": [
   534|         "dim_type.shape",
   535|         "dim_type.unit_format_id",
   536|         "dim_type.slope_direction",
   537|         "dim_type.leader_line_length",
   538|         "dim_type.leader_arrowhead_sig_hash",
   539|         "dim_type.text_font",
   540|         "dim_type.text_size_in",
   541|         "dim_type.text_bold",
   542|         "dim_type.text_italic",
   543|         "dim_type.text_underline",
   544|         "dim_type.text_width_factor",
   545|         "dim_type.text_background",
   546|         "dim_type.color_rgb",
   547|         "dim_type.line_weight",
   548|         "dim_type.suppress_spaces",
   549|         "dim_type.leader_arrowhead_line_weight",
   550|         "dim_type.leader_line_weight",
   551|         "dim_type.rotate_with_component",
   552|         "dim_type.text_offset_from_leader_in",
   553|         "dim_type.alternate_units",
   554|         "dim_type.alternate_units_prefix",
   555|         "dim_type.alternate_units_suffix"
   556|       ],
   557|       "notes": [
   558|         "Area 7 §1 implements the previously-reserved (allowed-but-unimplemented) dim_type.leader_arrowhead_sig_hash via a shared core/dimension_type_helpers._read_leader_arrowhead() helper (also used by spot_elevation/spot_coordinate), and additionally registers dim_type.leader_arrowhead_uid/dim_type.leader_arrowhead_name -- same Area 10 precedent (text_type.leader_arrowhead_uid/_name) for file-local/cosmetic metadata that should be visible for join-key/pattern analysis but not hash-contributing.",
   559|         "sig_hash_keys is pinned to the pre-Area-7 allowed_keys plus every genuinely-new Area 7 key EXCEPT dim_type.leader_arrowhead_uid and dim_type.leader_arrowhead_name: leader_arrowhead_uid is file-local (D-004 restricts UID use to element-backed identities) and leader_arrowhead_name is cosmetic/presentation metadata, so both are excluded the same way Area 10 excluded text_type.leader_arrowhead_uid/_name. dim_type.leader_arrowhead_sig_hash itself is retained in sig_hash_keys (it was already an allowed_keys entry before this PR, and is content-derived from the referenced arrowhead, not file-local/cosmetic). Without this override, tools/generate_sig_hash_policy.py would default the sig_hash preimage to every allowed_keys entry including the uid/name pair on the next policy regeneration -- see core/sig_hash_builder.py's build_sig_hash_from_policy, which hashes every policy-allowed item, not just required_keys.",
   560|         "Area 7 §5/§6/§7 (alternate units, suppress_spaces, rotate_with_component, text_offset_from_leader) are non-required content-driven enrichment items, included in sig_hash_keys like any other identity item -- verified against three consistent runs from the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass. Text Offset from Symbol and Coordinate/Elevation Base intentionally NOT added here: not observed on SpotSlope in probe data, consistent with Spot Slope having no Symbol field either.",
   561|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained."
   562|       ],
   563|       "sig_hash_schema": "dimension_types_spot_slope.sig_hash.v2"
   564|     },
   565|     "fill_patterns_drafting": {
   566|       "domain_family": "fill_patterns",
   567|       "display_label": "Fill Patterns — Drafting",
   568|       "allowed_keys": [
   569|         "fill_pattern.target",
   570|         "fill_pattern.grid_count",
   571|         "fill_pattern.grids_def_hash",
   572|         "fill_pattern.is_import"
   573|       ],
   574|       "allowed_key_prefixes": [
   575|         "fill_pattern.grid["
   576|       ],
   577|       "required_keys": [
   578|         "fill_pattern.target",
   579|         "fill_pattern.grid_count",
   580|         "fill_pattern.grids_def_hash"
   581|       ],
   582|       "minima": {
   583|         "block_if_any_required_not_ok": true
   584|       },
   585|       "indexed_key_rules": {
   586|         "fill_pattern.grid[i].angle": true,
   587|         "fill_pattern.grid[i].origin_u": true,
   588|         "fill_pattern.grid[i].origin_v": true,
   589|         "fill_pattern.grid[i].shift": true,
   590|         "fill_pattern.grid[i].offset": true
   591|       }
   592|     },
   593|     "fill_patterns_model": {
   594|       "domain_family": "fill_patterns",
   595|       "display_label": "Fill Patterns — Model",
   596|       "allowed_keys": [
   597|         "fill_pattern.target",
   598|         "fill_pattern.grid_count",
   599|         "fill_pattern.grids_def_hash",
   600|         "fill_pattern.is_import"
   601|       ],
   602|       "allowed_key_prefixes": [
   603|         "fill_pattern.grid["
   604|       ],
   605|       "required_keys": [
   606|         "fill_pattern.target",
   607|         "fill_pattern.grid_count",
   608|         "fill_pattern.grids_def_hash"
   609|       ],
   610|       "minima": {
   611|         "block_if_any_required_not_ok": true
   612|       },
   613|       "indexed_key_rules": {
   614|         "fill_pattern.grid[i].angle": true,
   615|         "fill_pattern.grid[i].origin_u": true,
   616|         "fill_pattern.grid[i].origin_v": true,
   617|         "fill_pattern.grid[i].shift": true,
   618|         "fill_pattern.grid[i].offset": true
   619|       }
   620|     },
   621|     "floor_types": {
   622|       "domain_family": "compound_types",
   623|       "display_label": "Floor Types",
   624|       "allowed_keys": [
   625|         "ft.function",
   626|         "ft.layer_count",
   627|         "ft.total_thickness_in",
   628|         "ft.stack_hash_loose",
   629|         "ft.total_layer_rows",
   630|         "ft.stack_hash_strict",
   631|         "ft.stack_hash_function_only",
   632|         "ft.coarse_fill_pattern_sig_hash",
   633|         "ft.has_embedded_sweeps",
   634|         "ft.type_name",
   635|         "ft.coarse_fill_color_rgb"
   636|       ],
   637|       "required_keys": [
   638|         "ft.layer_count",
   639|         "ft.total_thickness_in",
   640|         "ft.stack_hash_loose"
   641|       ],
   642|       "required": [
   643|         "ft.layer_count",
   644|         "ft.total_thickness_in",
   645|         "ft.stack_hash_loose"
   646|       ],
   647|       "optional": [
   648|         "ft.total_layer_rows",
   649|         "ft.stack_hash_strict",
   650|         "ft.stack_hash_function_only",
   651|         "ft.coarse_fill_pattern_sig_hash",
   652|         "ft.has_embedded_sweeps",
   653|         "ft.type_name",
   654|         "ft.coarse_fill_color_rgb"
   655|       ],
   656|       "minima": {
   657|         "block_if_any_required_not_ok": true
   658|       }
   659|     },
   660|     "identity": {
   661|       "domain_family": "identity",
   662|       "allowed_keys": [
   663|         "identity.is_workshared",
   664|         "identity.revit_version_number",
   665|         "identity.revit_version_name",
   666|         "identity.revit_build",
   667|         "project_info.name",
   668|         "project_info.number",
   669|         "project_info.status",
   670|         "project_info.address",
   671|         "project_info.issue_date",
   672|         "project_info.client_name",
   673|         "project_info.building_name",
   674|         "project_info.organization_name",
   675|         "project_info.organization_description",
   676|         "project_info.ifc_building_guid",
   677|         "project_info.ifc_project_guid",
   678|         "project_info.ifc_site_guid",
   679|         "project_info.business_center"
   680|       ],
   681|       "required_keys": [
   682|         "identity.is_workshared"
   683|       ],
   684|       "minima": {
   685|         "block_if_any_required_not_ok": true
   686|       },
   687|       "sig_hash_schema": "identity.sig_hash.v2",
   688|       "notes": [
   689|         "project_info.* keys (D-025) are ProjectInformation-sourced identity items, additive to the pre-existing worksharing/version/build core -- see domains/identity.py's project_info field tables.",
   690|         "project_info.name/number/status/address/issue_date/client_name/building_name/organization_name/organization_description/ifc_building_guid/ifc_project_guid/ifc_site_guid are all Revit built-ins (BuiltInParameter), expected on every project (q=unreadable if the Parameter object itself is absent, not q=missing). The IFC GUID fields were originally implemented as shared/custom LookupParameter reads before a PR review follow-up confirmed via tools/archetype/bip_lookup.json that they are real BuiltInParameter members (IFC_BUILDING_GUID/IFC_PROJECT_GUID/IFC_SITE_GUID).",
   691|         "project_info.business_center is a generic contract-registered deployment field. It is omitted when unconfigured and emitted only when project_info_shared_parameters supplies a deployment-owned display name and optional GUID; an absent configured definition is q=unsupported.not_applicable.",
   692|         "To register another deployment key, add its non-organization-specific project_info.* key to this allowed_keys list, review and version the identity signature policy, then deploy a mapping file. The core loader rejects mappings not present here before extraction. Names and GUIDs remain deployment-local.",
   693|         "All configured project_info.* items, including project_info.business_center and every quality state, intentionally participate in identity.sig_hash.v2. Enabling, disabling, or changing optional mappings therefore changes the preimage and is a compatibility/migration boundary; compare hashes only among runs using equivalent mapping policy.",
   694|         "sig_hash_schema is pinned to identity.sig_hash.v2 (not the generator's default identity.sig_hash.v1) because D-025 changed the sig_hash preimage composition -- tools/generate_sig_hash_policy.py's build_policy() defaults any domain without this override to '<domain>.sig_hash.v1', which would silently erase the version bump on the next regeneration and make post-D-025 hashes appear compatible with the old preimage (PR review follow-up)."
   695|       ]
   696|     },
   697|     "line_patterns": {
   698|       "domain_family": "line_patterns",
   699|       "display_label": "Line Patterns",
   700|       "allowed_keys": [
   701|         "line_pattern.segment_count",
   702|         "line_pattern.segments_def_hash"
   703|       ],
   704|       "allowed_key_prefixes": [
   705|         "line_pattern.seg["
   706|       ],
   707|       "required_keys": [
   708|         "line_pattern.segment_count"
   709|       ],
   710|       "sig_hash_schema": "line_patterns.sig_hash.v2",
   711|       "sig_hash_keys": [
   712|         "line_pattern.segment_count",
   713|         "line_pattern.segments_def_hash"
   714|       ],
   715|       "sig_hash_key_prefixes": [],
   716|       "minima": {
   717|         "block_if_any_required_not_ok": true
   718|       },
   719|       "indexed_key_rules": {
   720|         "line_pattern.seg[i].kind": true,
   721|         "line_pattern.seg[i].length": true
   722|       }
   723|     },
   724|     "line_styles": {
   725|       "domain_family": "line_styles",
   726|       "display_label": "Line Styles",
   727|       "allowed_keys": [
   728|         "line_style.weight.projection",
   729|         "line_style.color.rgb",
   730|         "line_style.weight.cut",
   731|         "line_style.pattern_ref.sig_hash",
   732|         "line_style.pattern_ref.synopsis"
   733|       ],
   734|       "required_keys": [
   735|         "line_style.weight.projection",
   736|         "line_style.color.rgb",
   737|         "line_style.pattern_ref.sig_hash"
   738|       ],
   739|       "minima": {
   740|         "block_if_any_required_not_ok": true
   741|       }
   742|     },
   743|     "loaded_family_types": {
   744|       "domain_family": "loaded_family_types",
   745|       "display_label": "Loaded Family Types",
   746|       "allowed_keys": [
   747|         "lft.shape_gate.category",
   748|         "lft.shape_gate.category_id",
   749|         "lft.type_parameter_schema_hash",
   750|         "lft.type_parameter_count",
   751|         "lft.family_is_in_place",
   752|         "lft.family_is_editable",
   753|         "lft.family_symbol_count",
   754|         "lft.type_count",
   755|         "lft.structural_material_type",
   756|         "lft.is_active"
   757|       ],
   758|       "required_keys": [
   759|         "lft.shape_gate.category",
   760|         "lft.type_parameter_schema_hash"
   761|       ],
   762|       "optional": [
   763|         "lft.type_parameter_count",
   764|         "lft.family_symbol_count",
   765|         "lft.family_is_in_place",
   766|         "lft.family_is_editable",
   767|         "lft.shape_gate.category_id",
   768|         "lft.type_count",
   769|         "lft.structural_material_type",
   770|         "lft.is_active"
   771|       ],
   772|       "minima": {
   773|         "block_if_any_required_not_ok": true
   774|       },
   775|       "sig_hash_schema": "loaded_family_types.sig_hash.v2",
   776|       "notes": [
   777|         "Records are emitted at family granularity (one record per Family element, not per FamilySymbol).",
   778|         "lft.family_name is label-only — excluded from identity_basis so sig_hash is name-independent.",
   779|         "lft.type_name removed: replaced by lft.type_count (count of collected types per family).",
   780|         "lft.family_symbol_count: API-reported total types; may differ from lft.type_count if types were filtered.",
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
```
