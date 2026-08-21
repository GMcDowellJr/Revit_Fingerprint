# Chunk of policies/domain_sig_hash_policies.json

- Source relative path: `policies/domain_sig_hash_policies.json`
- Chunk: 1 of 4
- Original line range: 1-400
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: f9574026a45f473063889e6bfa400737885177fa01bec6ddd2c3a96308a708dd
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| {
     2|   "domains": {
     3|     "arrowheads": {
     4|       "allowed_item_prefixes": [],
     5|       "allowed_items": [
     6|         "arrowhead.style",
     7|         "arrowhead.arrow_style_raw_int",
     8|         "arrowhead.arrow_style_display",
     9|         "arrowhead.tick_size_in",
    10|         "arrowhead.width_angle_deg",
    11|         "arrowhead.fill_tick",
    12|         "arrowhead.arrow_closed",
    13|         "arrowhead.tick_mark_centered",
    14|         "arrowhead.heavy_end_pen_weight"
    15|       ],
    16|       "hash_alg": "md5_utf8_join_pipe",
    17|       "minima": {
    18|         "block_if_any_required_not_ok": true
    19|       },
    20|       "notes": [
    21|         "Generated from contracts/domain_identity_keys_v2.json.",
    22|         "sig_hash is computed post-extraction from canonical identity_basis.items."
    23|       ],
    24|       "required_items": [
    25|         "arrowhead.style",
    26|         "arrowhead.tick_size_in"
    27|       ],
    28|       "sig_hash_schema": "arrowheads.sig_hash.v1"
    29|     },
    30|     "ceiling_types": {
    31|       "allowed_item_prefixes": [],
    32|       "allowed_items": [
    33|         "ct.layer_count",
    34|         "ct.total_thickness_in",
    35|         "ct.stack_hash_loose",
    36|         "ct.total_layer_rows",
    37|         "ct.stack_hash_strict",
    38|         "ct.stack_hash_function_only",
    39|         "ct.coarse_fill_pattern_sig_hash",
    40|         "ct.type_name",
    41|         "ct.coarse_fill_color_rgb"
    42|       ],
    43|       "hash_alg": "md5_utf8_join_pipe",
    44|       "minima": {
    45|         "block_if_any_required_not_ok": true
    46|       },
    47|       "notes": [
    48|         "Generated from contracts/domain_identity_keys_v2.json.",
    49|         "sig_hash is computed post-extraction from canonical identity_basis.items."
    50|       ],
    51|       "required_items": [
    52|         "ct.layer_count",
    53|         "ct.total_thickness_in",
    54|         "ct.stack_hash_loose"
    55|       ],
    56|       "sig_hash_schema": "ceiling_types.sig_hash.v1"
    57|     },
    58|     "dimension_types_angular": {
    59|       "allowed_item_prefixes": [],
    60|       "allowed_items": [
    61|         "dim_type.shape",
    62|         "dim_type.accuracy",
    63|         "dim_type.tick_mark_sig_hash",
    64|         "dim_type.witness_line_control",
    65|         "dim_type.unit_format_id",
    66|         "dim_type.rounding",
    67|         "dim_type.prefix",
    68|         "dim_type.suffix",
    69|         "dim_type.text_font",
    70|         "dim_type.text_size_in",
    71|         "dim_type.text_bold",
    72|         "dim_type.text_italic",
    73|         "dim_type.text_underline",
    74|         "dim_type.text_width_factor",
    75|         "dim_type.text_background",
    76|         "dim_type.color_rgb",
    77|         "dim_type.line_weight",
    78|         "dim_type.suppress_spaces",
    79|         "dim_type.leader_tick_mark_sig_hash",
    80|         "dim_type.leader_type",
    81|         "dim_type.show_leader_when_text_moves",
    82|         "dim_type.tick_mark_line_weight",
    83|         "dim_type.witness_line_extension_in",
    84|         "dim_type.witness_line_gap_to_element_in",
    85|         "dim_type.witness_line_length_in",
    86|         "dim_type.equality_text",
    87|         "dim_type.equality_witness_display",
    88|         "dim_type.centerline_pattern_sig_hash",
    89|         "dim_type.centerline_symbol_name",
    90|         "dim_type.centerline_tick_mark_sig_hash",
    91|         "dim_type.interior_tick_mark_sig_hash",
    92|         "dim_type.interior_tick_mark_display",
    93|         "dim_type.text_offset_in",
    94|         "dim_type.alternate_units",
    95|         "dim_type.alternate_units_prefix",
    96|         "dim_type.alternate_units_suffix"
    97|       ],
    98|       "hash_alg": "md5_utf8_join_pipe",
    99|       "minima": {
   100|         "block_if_any_required_not_ok": true
   101|       },
   102|       "notes": [
   103|         "Generated from contracts/domain_identity_keys_v2.json.",
   104|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   105|       ],
   106|       "required_items": [
   107|         "dim_type.shape",
   108|         "dim_type.accuracy",
   109|         "dim_type.tick_mark_sig_hash",
   110|         "dim_type.witness_line_control",
   111|         "dim_type.unit_format_id",
   112|         "dim_type.rounding",
   113|         "dim_type.prefix",
   114|         "dim_type.suffix"
   115|       ],
   116|       "sig_hash_schema": "dimension_types_angular.sig_hash.v2"
   117|     },
   118|     "dimension_types_diameter": {
   119|       "allowed_item_prefixes": [],
   120|       "allowed_items": [
   121|         "dim_type.shape",
   122|         "dim_type.accuracy",
   123|         "dim_type.tick_mark_sig_hash",
   124|         "dim_type.center_marks",
   125|         "dim_type.center_mark_size",
   126|         "dim_type.diameter_symbol_location",
   127|         "dim_type.diameter_symbol_text",
   128|         "dim_type.unit_format_id",
   129|         "dim_type.text_font",
   130|         "dim_type.text_size_in",
   131|         "dim_type.text_bold",
   132|         "dim_type.text_italic",
   133|         "dim_type.text_underline",
   134|         "dim_type.text_width_factor",
   135|         "dim_type.text_background",
   136|         "dim_type.color_rgb",
   137|         "dim_type.line_weight",
   138|         "dim_type.suppress_spaces",
   139|         "dim_type.leader_tick_mark_sig_hash",
   140|         "dim_type.leader_type",
   141|         "dim_type.show_leader_when_text_moves",
   142|         "dim_type.tick_mark_line_weight",
   143|         "dim_type.text_offset_in",
   144|         "dim_type.alternate_units",
   145|         "dim_type.alternate_units_prefix",
   146|         "dim_type.alternate_units_suffix"
   147|       ],
   148|       "hash_alg": "md5_utf8_join_pipe",
   149|       "minima": {
   150|         "block_if_any_required_not_ok": true
   151|       },
   152|       "notes": [
   153|         "Generated from contracts/domain_identity_keys_v2.json.",
   154|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   155|       ],
   156|       "required_items": [
   157|         "dim_type.shape",
   158|         "dim_type.accuracy",
   159|         "dim_type.tick_mark_sig_hash",
   160|         "dim_type.center_marks",
   161|         "dim_type.center_mark_size",
   162|         "dim_type.diameter_symbol_location",
   163|         "dim_type.diameter_symbol_text",
   164|         "dim_type.unit_format_id"
   165|       ],
   166|       "sig_hash_schema": "dimension_types_diameter.sig_hash.v2"
   167|     },
   168|     "dimension_types_linear": {
   169|       "allowed_item_prefixes": [],
   170|       "allowed_items": [
   171|         "dim_type.shape",
   172|         "dim_type.accuracy",
   173|         "dim_type.tick_mark_sig_hash",
   174|         "dim_type.witness_line_control",
   175|         "dim_type.unit_format_id",
   176|         "dim_type.rounding",
   177|         "dim_type.prefix",
   178|         "dim_type.suffix",
   179|         "dim_type.text_font",
   180|         "dim_type.text_size_in",
   181|         "dim_type.text_bold",
   182|         "dim_type.text_italic",
   183|         "dim_type.text_underline",
   184|         "dim_type.text_width_factor",
   185|         "dim_type.text_background",
   186|         "dim_type.color_rgb",
   187|         "dim_type.line_weight",
   188|         "dim_type.suppress_spaces",
   189|         "dim_type.leader_tick_mark_sig_hash",
   190|         "dim_type.leader_type",
   191|         "dim_type.show_leader_when_text_moves",
   192|         "dim_type.tick_mark_line_weight",
   193|         "dim_type.witness_line_extension_in",
   194|         "dim_type.witness_line_gap_to_element_in",
   195|         "dim_type.witness_line_length_in",
   196|         "dim_type.witness_line_tick_mark_sig_hash",
   197|         "dim_type.equality_text",
   198|         "dim_type.equality_witness_display",
   199|         "dim_type.centerline_pattern_sig_hash",
   200|         "dim_type.centerline_symbol_name",
   201|         "dim_type.centerline_tick_mark_sig_hash",
   202|         "dim_type.interior_tick_mark_sig_hash",
   203|         "dim_type.interior_tick_mark_display",
   204|         "dim_type.text_offset_in",
   205|         "dim_type.dimension_string_type",
   206|         "dim_type.show_opening_height",
   207|         "dim_type.alternate_units",
   208|         "dim_type.alternate_units_prefix",
   209|         "dim_type.alternate_units_suffix"
   210|       ],
   211|       "hash_alg": "md5_utf8_join_pipe",
   212|       "minima": {
   213|         "block_if_any_required_not_ok": true
   214|       },
   215|       "notes": [
   216|         "Generated from contracts/domain_identity_keys_v2.json.",
   217|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   218|       ],
   219|       "required_items": [
   220|         "dim_type.shape",
   221|         "dim_type.accuracy",
   222|         "dim_type.tick_mark_sig_hash",
   223|         "dim_type.witness_line_control",
   224|         "dim_type.unit_format_id",
   225|         "dim_type.rounding",
   226|         "dim_type.prefix",
   227|         "dim_type.suffix"
   228|       ],
   229|       "sig_hash_schema": "dimension_types_linear.sig_hash.v2"
   230|     },
   231|     "dimension_types_radial": {
   232|       "allowed_item_prefixes": [],
   233|       "allowed_items": [
   234|         "dim_type.shape",
   235|         "dim_type.accuracy",
   236|         "dim_type.tick_mark_sig_hash",
   237|         "dim_type.center_marks",
   238|         "dim_type.center_mark_size",
   239|         "dim_type.radius_symbol_location",
   240|         "dim_type.radius_symbol_text",
   241|         "dim_type.unit_format_id",
   242|         "dim_type.text_font",
   243|         "dim_type.text_size_in",
   244|         "dim_type.text_bold",
   245|         "dim_type.text_italic",
   246|         "dim_type.text_underline",
   247|         "dim_type.text_width_factor",
   248|         "dim_type.text_background",
   249|         "dim_type.color_rgb",
   250|         "dim_type.line_weight",
   251|         "dim_type.suppress_spaces",
   252|         "dim_type.leader_tick_mark_sig_hash",
   253|         "dim_type.leader_type",
   254|         "dim_type.show_leader_when_text_moves",
   255|         "dim_type.tick_mark_line_weight",
   256|         "dim_type.text_offset_in",
   257|         "dim_type.alternate_units",
   258|         "dim_type.alternate_units_prefix",
   259|         "dim_type.alternate_units_suffix"
   260|       ],
   261|       "hash_alg": "md5_utf8_join_pipe",
   262|       "minima": {
   263|         "block_if_any_required_not_ok": true
   264|       },
   265|       "notes": [
   266|         "Generated from contracts/domain_identity_keys_v2.json.",
   267|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   268|       ],
   269|       "required_items": [
   270|         "dim_type.shape",
   271|         "dim_type.accuracy",
   272|         "dim_type.tick_mark_sig_hash",
   273|         "dim_type.center_marks",
   274|         "dim_type.center_mark_size",
   275|         "dim_type.radius_symbol_location",
   276|         "dim_type.radius_symbol_text",
   277|         "dim_type.unit_format_id"
   278|       ],
   279|       "sig_hash_schema": "dimension_types_radial.sig_hash.v2"
   280|     },
   281|     "dimension_types_spot_coordinate": {
   282|       "allowed_item_prefixes": [],
   283|       "allowed_items": [
   284|         "dim_type.shape",
   285|         "dim_type.unit_format_id",
   286|         "dim_type.top_coordinate",
   287|         "dim_type.bottom_coordinate",
   288|         "dim_type.north_south_indicator",
   289|         "dim_type.east_west_indicator",
   290|         "dim_type.include_elevation",
   291|         "dim_type.elevation_indicator",
   292|         "dim_type.indicator_as_prefix_suffix",
   293|         "dim_type.text_orientation",
   294|         "dim_type.text_location",
   295|         "dim_type.symbol_name",
   296|         "dim_type.leader_arrowhead_sig_hash",
   297|         "dim_type.text_font",
   298|         "dim_type.text_size_in",
   299|         "dim_type.text_bold",
   300|         "dim_type.text_italic",
   301|         "dim_type.text_underline",
   302|         "dim_type.text_width_factor",
   303|         "dim_type.text_background",
   304|         "dim_type.color_rgb",
   305|         "dim_type.line_weight",
   306|         "dim_type.suppress_spaces",
   307|         "dim_type.leader_arrowhead_line_weight",
   308|         "dim_type.leader_line_weight",
   309|         "dim_type.rotate_with_component",
   310|         "dim_type.coordinate_base",
   311|         "dim_type.text_offset_from_leader_in",
   312|         "dim_type.text_offset_from_symbol_in",
   313|         "dim_type.alternate_units",
   314|         "dim_type.alternate_units_prefix",
   315|         "dim_type.alternate_units_suffix"
   316|       ],
   317|       "hash_alg": "md5_utf8_join_pipe",
   318|       "minima": {
   319|         "block_if_any_required_not_ok": true
   320|       },
   321|       "notes": [
   322|         "Generated from contracts/domain_identity_keys_v2.json.",
   323|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   324|       ],
   325|       "required_items": [
   326|         "dim_type.shape",
   327|         "dim_type.unit_format_id",
   328|         "dim_type.top_coordinate",
   329|         "dim_type.bottom_coordinate",
   330|         "dim_type.north_south_indicator",
   331|         "dim_type.east_west_indicator",
   332|         "dim_type.include_elevation",
   333|         "dim_type.elevation_indicator",
   334|         "dim_type.indicator_as_prefix_suffix",
   335|         "dim_type.text_orientation",
   336|         "dim_type.text_location",
   337|         "dim_type.symbol_name"
   338|       ],
   339|       "sig_hash_schema": "dimension_types_spot_coordinate.sig_hash.v2"
   340|     },
   341|     "dimension_types_spot_elevation": {
   342|       "allowed_item_prefixes": [],
   343|       "allowed_items": [
   344|         "dim_type.shape",
   345|         "dim_type.unit_format_id",
   346|         "dim_type.elevation_indicator",
   347|         "dim_type.elevation_indicator_as_prefix_suffix",
   348|         "dim_type.top_indicator",
   349|         "dim_type.bottom_indicator",
   350|         "dim_type.top_indicator_as_prefix_suffix",
   351|         "dim_type.bottom_indicator_as_prefix_suffix",
   352|         "dim_type.text_orientation",
   353|         "dim_type.text_location",
   354|         "dim_type.symbol_name",
   355|         "dim_type.leader_arrowhead_sig_hash",
   356|         "dim_type.text_font",
   357|         "dim_type.text_size_in",
   358|         "dim_type.text_bold",
   359|         "dim_type.text_italic",
   360|         "dim_type.text_underline",
   361|         "dim_type.text_width_factor",
   362|         "dim_type.text_background",
   363|         "dim_type.color_rgb",
   364|         "dim_type.line_weight",
   365|         "dim_type.suppress_spaces",
   366|         "dim_type.leader_arrowhead_line_weight",
   367|         "dim_type.leader_line_weight",
   368|         "dim_type.rotate_with_component",
   369|         "dim_type.elevation_base",
   370|         "dim_type.text_offset_from_leader_in",
   371|         "dim_type.text_offset_from_symbol_in",
   372|         "dim_type.alternate_units",
   373|         "dim_type.alternate_units_prefix",
   374|         "dim_type.alternate_units_suffix"
   375|       ],
   376|       "hash_alg": "md5_utf8_join_pipe",
   377|       "minima": {
   378|         "block_if_any_required_not_ok": true
   379|       },
   380|       "notes": [
   381|         "Generated from contracts/domain_identity_keys_v2.json.",
   382|         "sig_hash is computed post-extraction from canonical identity_basis.items."
   383|       ],
   384|       "required_items": [
   385|         "dim_type.shape",
   386|         "dim_type.unit_format_id",
   387|         "dim_type.elevation_indicator",
   388|         "dim_type.elevation_indicator_as_prefix_suffix",
   389|         "dim_type.top_indicator",
   390|         "dim_type.bottom_indicator",
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
```
