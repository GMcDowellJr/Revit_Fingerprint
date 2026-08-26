# Chunk of policies/domain_join_key_policies.json

- Source relative path: `policies/domain_join_key_policies.json`
- Chunk: 1 of 3
- Original line range: 1-400
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: c2197eb74a16316f88e902f8d66eca20eb7d7030b0f0e452980b93cd31d80964
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| {
     2|   "domains": {
     3|     "arrowheads": {
     4|       "join_key_schema": "arrowheads.join_key.v2",
     5|       "hash_alg": "md5_utf8_join_pipe",
     6|       "required_items": [
     7|         "arrowhead.style",
     8|         "arrowhead.tick_size_in"
     9|       ],
    10|       "optional_items": [
    11|         "arrowhead.width_angle_deg",
    12|         "arrowhead.fill_tick",
    13|         "arrowhead.arrow_closed",
    14|         "arrowhead.tick_mark_centered",
    15|         "arrowhead.heavy_end_pen_weight"
    16|       ],
    17|       "explicitly_excluded_items": [
    18|         "arrowhead.name",
    19|         "arrowhead.type_id"
    20|       ],
    21|       "shape_gating": {
    22|         "discriminator_key": "arrowhead.style",
    23|         "shape_requirements": {
    24|           "Arrow": {
    25|             "additional_required": [
    26|               "arrowhead.width_angle_deg",
    27|               "arrowhead.fill_tick",
    28|               "arrowhead.arrow_closed"
    29|             ],
    30|             "additional_optional": [],
    31|             "notes": "Arrow record class: arrow-specific geometry + fill/closed flags are applicable and required."
    32|           },
    33|           "Heavy end tick mark": {
    34|             "additional_required": [
    35|               "arrowhead.tick_mark_centered",
    36|               "arrowhead.heavy_end_pen_weight"
    37|             ],
    38|             "additional_optional": [],
    39|             "notes": "Heavy end tick mark record class: centered and pen weight are applicable and required."
    40|           }
    41|         },
    42|         "default_shape_behavior": "common_only"
    43|       },
    44|       "notes": [
    45|         "Schema v2: record class routing corrected. Dot/Diagonal/Box/Loop/Elevation Target/Datum triangle are size-only (common items only).",
    46|         "Arrow record class: arrowhead.width_angle_deg, arrowhead.fill_tick, arrowhead.arrow_closed.",
    47|         "Heavy end tick mark record class: arrowhead.tick_mark_centered, arrowhead.heavy_end_pen_weight.",
    48|         "Size-only record classes (Dot/Diagonal/Box/Loop/Elevation Target/Datum triangle): common items only (arrowhead.style + arrowhead.tick_size_in).",
    49|         "Style-specific fields moved from optional_items to explicitly_excluded_items at common level — they are never unconditionally applicable.",
    50|         "Definition-based identity only; no referential identifiers in joins."
    51|       ]
    52|     },
    53|     "ceiling_types": {
    54|       "join_key_schema": "ceiling_types.join_key.v1",
    55|       "hash_alg": "md5_utf8_join_pipe",
    56|       "required_items": [
    57|         "ct.layer_count",
    58|         "ct.total_thickness_in",
    59|         "ct.stack_hash_loose"
    60|       ],
    61|       "optional_items": [],
    62|       "explicitly_excluded_items": [
    63|         "ct.type_name",
    64|         "ct.coarse_fill_color_rgb"
    65|       ],
    66|       "notes": [
    67|         "Domain family: compound_types.",
    68|         "Compound Ceiling only — non-compound types blocked."
    69|       ]
    70|     },
    71|     "dimension_types_angular": {
    72|       "join_key_schema": "dimension_types_angular.join_key.v1",
    73|       "hash_alg": "md5_utf8_join_pipe",
    74|       "required_items": [
    75|         "dim_type.shape",
    76|         "dim_type.accuracy",
    77|         "dim_type.tick_mark_sig_hash",
    78|         "dim_type.witness_line_control",
    79|         "dim_type.unit_format_id",
    80|         "dim_type.rounding",
    81|         "dim_type.prefix",
    82|         "dim_type.suffix"
    83|       ],
    84|       "optional_items": [],
    85|       "explicitly_excluded_items": [
    86|         "dim_type.name",
    87|         "dim_type.tick_mark_uid",
    88|         "dim_attr.tick_mark_uid"
    89|       ],
    90|       "notes": [
    91|         "Domain family: dimension_types.",
    92|         "Contains: Angular only.",
    93|         "UI exposes Angular Dimension Style as a distinct system family.",
    94|         "Witness Line Control is active for Angular — required, not optional.",
    95|         "No record class gating needed."
    96|       ]
    97|     },
    98|     "dimension_types_diameter": {
    99|       "join_key_schema": "dimension_types_diameter.join_key.v1",
   100|       "hash_alg": "md5_utf8_join_pipe",
   101|       "required_items": [
   102|         "dim_type.shape",
   103|         "dim_type.accuracy",
   104|         "dim_type.tick_mark_sig_hash",
   105|         "dim_type.center_marks",
   106|         "dim_type.center_mark_size",
   107|         "dim_type.diameter_symbol_location",
   108|         "dim_type.diameter_symbol_text",
   109|         "dim_type.unit_format_id"
   110|       ],
   111|       "optional_items": [],
   112|       "explicitly_excluded_items": [
   113|         "dim_type.name",
   114|         "dim_type.tick_mark_uid",
   115|         "dim_attr.tick_mark_uid"
   116|       ],
   117|       "notes": [
   118|         "Domain family: dimension_types.",
   119|         "Contains: Diameter, DiameterLinked.",
   120|         "Diameter Symbol Location and Diameter Symbol Text are active fields in UI.",
   121|         "No record class gating needed."
   122|       ]
   123|     },
   124|     "dimension_types_linear": {
   125|       "join_key_schema": "dimension_types_linear.join_key.v1",
   126|       "hash_alg": "md5_utf8_join_pipe",
   127|       "required_items": [
   128|         "dim_type.shape",
   129|         "dim_type.accuracy",
   130|         "dim_type.tick_mark_sig_hash",
   131|         "dim_type.witness_line_control",
   132|         "dim_type.unit_format_id",
   133|         "dim_type.rounding",
   134|         "dim_type.prefix",
   135|         "dim_type.suffix"
   136|       ],
   137|       "optional_items": [],
   138|       "explicitly_excluded_items": [
   139|         "dim_type.name",
   140|         "dim_type.tick_mark_uid",
   141|         "dim_attr.tick_mark_uid"
   142|       ],
   143|       "notes": [
   144|         "Domain family: dimension_types.",
   145|         "Contains: Linear, LinearFixed, Angular, ArcLength shapes (all share witness_line_control).",
   146|         "No record class gating needed — homogeneous property structure across all four shapes.",
   147|         "witness_line_control is confirmed active for all four shapes in UI."
   148|       ]
   149|     },
   150|     "dimension_types_radial": {
   151|       "join_key_schema": "dimension_types_radial.join_key.v1",
   152|       "hash_alg": "md5_utf8_join_pipe",
   153|       "required_items": [
   154|         "dim_type.shape",
   155|         "dim_type.accuracy",
   156|         "dim_type.tick_mark_sig_hash",
   157|         "dim_type.center_marks",
   158|         "dim_type.center_mark_size",
   159|         "dim_type.radius_symbol_location",
   160|         "dim_type.radius_symbol_text",
   161|         "dim_type.unit_format_id"
   162|       ],
   163|       "optional_items": [],
   164|       "explicitly_excluded_items": [
   165|         "dim_type.name",
   166|         "dim_type.tick_mark_uid",
   167|         "dim_attr.tick_mark_uid"
   168|       ],
   169|       "notes": [
   170|         "Domain family: dimension_types.",
   171|         "Contains: Radial only.",
   172|         "Radius Symbol Location and Radius Symbol Text are active fields in UI.",
   173|         "No record class gating needed."
   174|       ]
   175|     },
   176|     "dimension_types_spot_coordinate": {
   177|       "join_key_schema": "dimension_types_spot_coordinate.join_key.v1",
   178|       "hash_alg": "md5_utf8_join_pipe",
   179|       "required_items": [
   180|         "dim_type.shape",
   181|         "dim_type.unit_format_id",
   182|         "dim_type.top_coordinate",
   183|         "dim_type.bottom_coordinate",
   184|         "dim_type.north_south_indicator",
   185|         "dim_type.east_west_indicator",
   186|         "dim_type.include_elevation",
   187|         "dim_type.elevation_indicator",
   188|         "dim_type.indicator_as_prefix_suffix",
   189|         "dim_type.text_orientation",
   190|         "dim_type.text_location",
   191|         "dim_type.symbol_name"
   192|       ],
   193|       "optional_items": [],
   194|       "explicitly_excluded_items": [
   195|         "dim_type.name",
   196|         "dim_type.tick_mark_uid",
   197|         "dim_attr.tick_mark_uid",
   198|         "dim_type.leader_arrowhead_uid",
   199|         "dim_type.leader_arrowhead_name"
   200|       ],
   201|       "notes": [
   202|         "Domain family: dimension_types.",
   203|         "Contains: SpotCoordinate only.",
   204|         "Entirely different primary units — coordinate indicator fields.",
   205|         "No record class gating needed."
   206|       ]
   207|     },
   208|     "dimension_types_spot_elevation": {
   209|       "join_key_schema": "dimension_types_spot_elevation.join_key.v1",
   210|       "hash_alg": "md5_utf8_join_pipe",
   211|       "required_items": [
   212|         "dim_type.shape",
   213|         "dim_type.unit_format_id",
   214|         "dim_type.elevation_indicator",
   215|         "dim_type.elevation_indicator_as_prefix_suffix",
   216|         "dim_type.top_indicator",
   217|         "dim_type.bottom_indicator",
   218|         "dim_type.top_indicator_as_prefix_suffix",
   219|         "dim_type.bottom_indicator_as_prefix_suffix",
   220|         "dim_type.text_orientation",
   221|         "dim_type.text_location",
   222|         "dim_type.symbol_name"
   223|       ],
   224|       "optional_items": [],
   225|       "explicitly_excluded_items": [
   226|         "dim_type.name",
   227|         "dim_type.tick_mark_uid",
   228|         "dim_attr.tick_mark_uid",
   229|         "dim_type.leader_arrowhead_uid",
   230|         "dim_type.leader_arrowhead_name"
   231|       ],
   232|       "notes": [
   233|         "Domain family: dimension_types.",
   234|         "Contains: SpotElevation, SpotElevationFixed.",
   235|         "Primary units shows indicator fields, no prefix/suffix, no witness line.",
   236|         "Leader Arrowhead replaces Tick Mark for spot types.",
   237|         "No record class gating needed."
   238|       ]
   239|     },
   240|     "dimension_types_spot_slope": {
   241|       "join_key_schema": "dimension_types_spot_slope.join_key.v1",
   242|       "hash_alg": "md5_utf8_join_pipe",
   243|       "required_items": [
   244|         "dim_type.shape",
   245|         "dim_type.unit_format_id",
   246|         "dim_type.slope_direction",
   247|         "dim_type.leader_line_length"
   248|       ],
   249|       "optional_items": [],
   250|       "explicitly_excluded_items": [
   251|         "dim_type.name",
   252|         "dim_type.tick_mark_uid",
   253|         "dim_attr.tick_mark_uid",
   254|         "dim_type.leader_arrowhead_uid",
   255|         "dim_type.leader_arrowhead_name"
   256|       ],
   257|       "notes": [
   258|         "Domain family: dimension_types.",
   259|         "Contains: SpotSlope only.",
   260|         "Minimal — Slope Direction and Leader Line Length active, no symbol field.",
   261|         "Rotate with Component is greyed (not applicable) — not extracted.",
   262|         "No record class gating needed."
   263|       ]
   264|     },
   265|     "fill_patterns_drafting": {
   266|       "join_key_schema": "fill_patterns_drafting.join_key.v1",
   267|       "hash_alg": "md5_utf8_join_pipe",
   268|       "required_items": [
   269|         "fill_pattern.target",
   270|         "fill_pattern.grid_count",
   271|         "fill_pattern.grids_def_hash"
   272|       ],
   273|       "optional_items": [],
   274|       "explicitly_excluded_items": [
   275|         "fill_pattern.name",
   276|         "fill_pattern.is_solid"
   277|       ],
   278|       "notes": [
   279|         "Domain family: fill_patterns.",
   280|         "Contains: FillPatternTarget.Drafting (target_id = 0), excluding solid fills.",
   281|         "Solid fills are system defaults, ungoverned — excluded from this domain.",
   282|         "grids_def_hash is the hash of the ordered grid definitions."
   283|       ]
   284|     },
   285|     "fill_patterns_model": {
   286|       "join_key_schema": "fill_patterns_model.join_key.v1",
   287|       "hash_alg": "md5_utf8_join_pipe",
   288|       "required_items": [
   289|         "fill_pattern.target",
   290|         "fill_pattern.grid_count",
   291|         "fill_pattern.grids_def_hash"
   292|       ],
   293|       "optional_items": [],
   294|       "explicitly_excluded_items": [
   295|         "fill_pattern.name",
   296|         "fill_pattern.is_solid"
   297|       ],
   298|       "notes": [
   299|         "Domain family: fill_patterns.",
   300|         "Contains: FillPatternTarget.Model (target_id = 1), excluding solid fills.",
   301|         "Solid fills excluded — system defaults, ungoverned."
   302|       ]
   303|     },
   304|     "floor_types": {
   305|       "join_key_schema": "floor_types.join_key.v1",
   306|       "hash_alg": "md5_utf8_join_pipe",
   307|       "required_items": [
   308|         "ft.function",
   309|         "ft.layer_count",
   310|         "ft.total_thickness_in",
   311|         "ft.stack_hash_loose"
   312|       ],
   313|       "optional_items": [],
   314|       "explicitly_excluded_items": [
   315|         "ft.type_name",
   316|         "ft.coarse_fill_color_rgb"
   317|       ],
   318|       "notes": [
   319|         "Domain family: compound_types.",
   320|         "stack_hash_loose includes deck_usage for structural deck layers."
   321|       ]
   322|     },
   323|     "identity": {
   324|       "join_key_schema": "identity.join_key.v1",
   325|       "hash_alg": "md5_utf8_join_pipe",
   326|       "required_items": [
   327|         "identity.is_workshared"
   328|       ],
   329|       "optional_items": [
   330|         "identity.revit_version_number",
   331|         "identity.revit_build",
   332|         "identity.revit_version_name"
   333|       ],
   334|       "explicitly_excluded_items": [
   335|         "identity.central_path",
   336|         "identity.central_path_norm",
   337|         "identity.filename",
   338|         "identity.project_title",
   339|         "project_info.name",
   340|         "project_info.number",
   341|         "project_info.status",
   342|         "project_info.address",
   343|         "project_info.issue_date",
   344|         "project_info.client_name",
   345|         "project_info.building_name",
   346|         "project_info.organization_name",
   347|         "project_info.organization_description",
   348|         "project_info.ifc_building_guid",
   349|         "project_info.ifc_project_guid",
   350|         "project_info.ifc_site_guid",
   351|         "project_info.business_center"
   352|       ],
   353|       "shape_gating": {
   354|         "discriminator_key": "identity.is_workshared",
   355|         "shape_requirements": {
   356|           "true": {
   357|             "additional_required": [
   358|               "identity.revit_version_number"
   359|             ],
   360|             "additional_optional": [],
   361|             "notes": "Workshared identity joins include version number as discriminator-gated required context."
   362|           }
   363|         },
   364|         "default_shape_behavior": "common_only"
   365|       },
   366|       "notes": [
   367|         "Pilot for canonical evidence superset + selectors in the identity domain.",
   368|         "join_hash uses policy-required items only; optional items remain in identity_basis.items for future exploration.",
   369|         "File-local identifiers remain excluded from canonical identity evidence and join-key hashing.",
   370|         "project_info.* (D-025) is human-entered/naming ProjectInformation metadata -- explicitly excluded from cross-project join-key matching for the same reason identity.project_title is, even though (unlike project_title) it is included in identity_basis.items/sig_hash."
   371|       ]
   372|     },
   373|     "line_patterns": {
   374|       "join_key_schema": "line_patterns.join_key.v3",
   375|       "hash_alg": "md5_utf8_join_pipe",
   376|       "required_items": [
   377|         "line_pattern.segments_norm_hash"
   378|       ],
   379|       "optional_items": [],
   380|       "explicitly_excluded_items": [
   381|         "line_pattern.uid",
   382|         "line_pattern.name",
   383|         "line_pattern.element_id",
   384|         "line_pattern.uid_or_namekey",
   385|         "line_pattern.segments_def_hash"
   386|       ],
   387|       "notes": [
   388|         "Upgraded from v2 (segments_def_hash) to v3 (segments_norm_hash) for scale-invariant identity.",
   389|         "segments_norm_hash encodes segment kind sequence + length ratios (length / non_dot_total).",
   390|         "Scale variants of the same structural type collapse to the same pattern.",
   391|         "Dot segments use relative epsilon (1% of non_dot_total) to preserve scale invariance.",
   392|         "segments_def_hash moved to explicitly_excluded — retains exact identity in identity_basis for forensics.",
   393|         "Names are labels only and must not participate in joins.",
   394|         "lp.is_import added to coordination_items in extractor v2.",
   395|         "sig_hash intentionally uses segments_def_hash (exact scale identity); see domain_sig_hash_policies.json.",
   396|         "join_hash (this policy) uses segments_norm_hash (governance equivalence). These are different questions."
   397|       ]
   398|     },
   399|     "line_styles": {
   400|       "join_key_schema": "line_styles.join_key.v1",
```
