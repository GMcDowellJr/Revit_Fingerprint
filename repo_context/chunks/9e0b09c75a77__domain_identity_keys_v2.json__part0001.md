# Chunk of contracts/domain_identity_keys_v2.json

- Source relative path: `contracts/domain_identity_keys_v2.json`
- Chunk: 1 of 4
- Original line range: 1-400
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 97b5e6834d04d3f21f92697078fa5f994471002da74d6487c4bc46e4cab7ad10
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| {
     2|   "version": "domain_identity_keys.v2",
     3|   "record_schema_version": "record.v2",
     4|   "identity_item_schema": "identity_items.v1",
     5|   "identity_quality_dominance_order": [
     6|     "none_blocked",
     7|     "incomplete_unreadable",
     8|     "incomplete_unsupported",
     9|     "incomplete_missing",
    10|     "complete"
    11|   ],
    12|   "banned_identity_value_substrings": [
    13|     "<MISSING>",
    14|     "<UNREADABLE>",
    15|     "<NOT_APPLICABLE>",
    16|     "<LP:UNMAPPED>"
    17|   ],
    18|   "domains": {
    19|     "arrowheads": {
    20|       "domain_family": "arrowheads",
    21|       "display_label": "Arrowheads",
    22|       "allowed_keys": [
    23|         "arrowhead.style",
    24|         "arrowhead.arrow_style_raw_int",
    25|         "arrowhead.arrow_style_display",
    26|         "arrowhead.tick_size_in",
    27|         "arrowhead.width_angle_deg",
    28|         "arrowhead.fill_tick",
    29|         "arrowhead.arrow_closed",
    30|         "arrowhead.tick_mark_centered",
    31|         "arrowhead.heavy_end_pen_weight"
    32|       ],
    33|       "required_keys": [
    34|         "arrowhead.style",
    35|         "arrowhead.tick_size_in"
    36|       ],
    37|       "minima": {
    38|         "block_if_any_required_not_ok": true
    39|       }
    40|     },
    41|     "ceiling_types": {
    42|       "domain_family": "compound_types",
    43|       "display_label": "Ceiling Types",
    44|       "allowed_keys": [
    45|         "ct.layer_count",
    46|         "ct.total_thickness_in",
    47|         "ct.stack_hash_loose",
    48|         "ct.total_layer_rows",
    49|         "ct.stack_hash_strict",
    50|         "ct.stack_hash_function_only",
    51|         "ct.coarse_fill_pattern_sig_hash",
    52|         "ct.type_name",
    53|         "ct.coarse_fill_color_rgb"
    54|       ],
    55|       "required_keys": [
    56|         "ct.layer_count",
    57|         "ct.total_thickness_in",
    58|         "ct.stack_hash_loose"
    59|       ],
    60|       "required": [
    61|         "ct.layer_count",
    62|         "ct.total_thickness_in",
    63|         "ct.stack_hash_loose"
    64|       ],
    65|       "optional": [
    66|         "ct.total_layer_rows",
    67|         "ct.stack_hash_strict",
    68|         "ct.stack_hash_function_only",
    69|         "ct.coarse_fill_pattern_sig_hash",
    70|         "ct.type_name",
    71|         "ct.coarse_fill_color_rgb"
    72|       ],
    73|       "minima": {
    74|         "block_if_any_required_not_ok": true
    75|       }
    76|     },
    77|     "dimension_types_angular": {
    78|       "domain_family": "dimension_types",
    79|       "display_label": "Dimension Types — Angular",
    80|       "allowed_keys": [
    81|         "dim_type.shape",
    82|         "dim_type.accuracy",
    83|         "dim_type.tick_mark_sig_hash",
    84|         "dim_type.witness_line_control",
    85|         "dim_type.unit_format_id",
    86|         "dim_type.rounding",
    87|         "dim_type.prefix",
    88|         "dim_type.suffix",
    89|         "dim_type.text_font",
    90|         "dim_type.text_size_in",
    91|         "dim_type.text_bold",
    92|         "dim_type.text_italic",
    93|         "dim_type.text_underline",
    94|         "dim_type.text_width_factor",
    95|         "dim_type.text_background",
    96|         "dim_type.color_rgb",
    97|         "dim_type.line_weight",
    98|         "dim_type.suppress_spaces",
    99|         "dim_type.leader_tick_mark_sig_hash",
   100|         "dim_type.leader_type",
   101|         "dim_type.show_leader_when_text_moves",
   102|         "dim_type.tick_mark_line_weight",
   103|         "dim_type.witness_line_extension_in",
   104|         "dim_type.witness_line_gap_to_element_in",
   105|         "dim_type.witness_line_length_in",
   106|         "dim_type.equality_text",
   107|         "dim_type.equality_witness_display",
   108|         "dim_type.centerline_pattern_sig_hash",
   109|         "dim_type.centerline_symbol_name",
   110|         "dim_type.centerline_tick_mark_sig_hash",
   111|         "dim_type.interior_tick_mark_sig_hash",
   112|         "dim_type.interior_tick_mark_display",
   113|         "dim_type.text_offset_in",
   114|         "dim_type.alternate_units",
   115|         "dim_type.alternate_units_prefix",
   116|         "dim_type.alternate_units_suffix"
   117|       ],
   118|       "required_keys": [
   119|         "dim_type.shape",
   120|         "dim_type.accuracy",
   121|         "dim_type.tick_mark_sig_hash",
   122|         "dim_type.witness_line_control",
   123|         "dim_type.unit_format_id",
   124|         "dim_type.rounding",
   125|         "dim_type.prefix",
   126|         "dim_type.suffix"
   127|       ],
   128|       "minima": {
   129|         "block_if_any_required_not_ok": true
   130|       },
   131|       "notes": [
   132|         "Area 7 (§2 leader config, §3 witness lines, §4 equality/centerline/tick weight, §5 alternate units, §6 suppress_spaces, §7 text offset) added dim_type.suppress_spaces through dim_type.alternate_units_suffix above as non-required enrichment identity items -- verified against three consistent runs from the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass. No sig_hash_keys override added for this domain: it had none before this change, so these new allowed_keys flow into the analysis-side sig_hash preimage the same way its pre-existing allowed_keys always have -- a content-driven addition, not an algorithm change.",
   133|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained.",
   134|         "PR #412 review fix: dim_type.centerline_pattern_name was renamed to dim_type.centerline_pattern_sig_hash and now resolves via core/dimension_type_helpers._read_line_pattern_ref_sig_hash() (the same ctx[\"line_pattern_uid_to_hash\"]/ctx[\"line_pattern_special_values\"] resolution domains/object_styles.py already uses), instead of a plain doc.GetElement() name lookup that returned missing for built-in pattern ids like the probe's -3000010 (\"Solid\"), collapsing it with \"no pattern\"."
   135|       ],
   136|       "sig_hash_schema": "dimension_types_angular.sig_hash.v2"
   137|     },
   138|     "dimension_types_diameter": {
   139|       "domain_family": "dimension_types",
   140|       "display_label": "Dimension Types — Diameter",
   141|       "allowed_keys": [
   142|         "dim_type.shape",
   143|         "dim_type.accuracy",
   144|         "dim_type.tick_mark_sig_hash",
   145|         "dim_type.center_marks",
   146|         "dim_type.center_mark_size",
   147|         "dim_type.diameter_symbol_location",
   148|         "dim_type.diameter_symbol_text",
   149|         "dim_type.unit_format_id",
   150|         "dim_type.text_font",
   151|         "dim_type.text_size_in",
   152|         "dim_type.text_bold",
   153|         "dim_type.text_italic",
   154|         "dim_type.text_underline",
   155|         "dim_type.text_width_factor",
   156|         "dim_type.text_background",
   157|         "dim_type.color_rgb",
   158|         "dim_type.line_weight",
   159|         "dim_type.suppress_spaces",
   160|         "dim_type.leader_tick_mark_sig_hash",
   161|         "dim_type.leader_type",
   162|         "dim_type.show_leader_when_text_moves",
   163|         "dim_type.tick_mark_line_weight",
   164|         "dim_type.text_offset_in",
   165|         "dim_type.alternate_units",
   166|         "dim_type.alternate_units_prefix",
   167|         "dim_type.alternate_units_suffix"
   168|       ],
   169|       "required_keys": [
   170|         "dim_type.shape",
   171|         "dim_type.accuracy",
   172|         "dim_type.tick_mark_sig_hash",
   173|         "dim_type.center_marks",
   174|         "dim_type.center_mark_size",
   175|         "dim_type.diameter_symbol_location",
   176|         "dim_type.diameter_symbol_text",
   177|         "dim_type.unit_format_id"
   178|       ],
   179|       "minima": {
   180|         "block_if_any_required_not_ok": true
   181|       },
   182|       "notes": [
   183|         "Area 7 (§2 leader config, §4c tick weight, §5 alternate units, §6 suppress_spaces, §7 text offset) added dim_type.suppress_spaces through dim_type.alternate_units_suffix above as non-required enrichment identity items -- Witness Lines/Equality/Centerline do NOT apply to Radial/Diameter per probe observed_on_shapes (Linear/Angular only), correcting an initial guess that they might. Verified against the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass. No sig_hash_keys override added: this domain had none before this change, so new allowed_keys flow into the analysis-side sig_hash preimage the same way its pre-existing allowed_keys always have.",
   184|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained."
   185|       ],
   186|       "sig_hash_schema": "dimension_types_diameter.sig_hash.v2"
   187|     },
   188|     "dimension_types_linear": {
   189|       "domain_family": "dimension_types",
   190|       "display_label": "Dimension Types — Linear",
   191|       "allowed_keys": [
   192|         "dim_type.shape",
   193|         "dim_type.accuracy",
   194|         "dim_type.tick_mark_sig_hash",
   195|         "dim_type.witness_line_control",
   196|         "dim_type.unit_format_id",
   197|         "dim_type.rounding",
   198|         "dim_type.prefix",
   199|         "dim_type.suffix",
   200|         "dim_type.text_font",
   201|         "dim_type.text_size_in",
   202|         "dim_type.text_bold",
   203|         "dim_type.text_italic",
   204|         "dim_type.text_underline",
   205|         "dim_type.text_width_factor",
   206|         "dim_type.text_background",
   207|         "dim_type.color_rgb",
   208|         "dim_type.line_weight",
   209|         "dim_type.suppress_spaces",
   210|         "dim_type.leader_tick_mark_sig_hash",
   211|         "dim_type.leader_type",
   212|         "dim_type.show_leader_when_text_moves",
   213|         "dim_type.tick_mark_line_weight",
   214|         "dim_type.witness_line_extension_in",
   215|         "dim_type.witness_line_gap_to_element_in",
   216|         "dim_type.witness_line_length_in",
   217|         "dim_type.witness_line_tick_mark_sig_hash",
   218|         "dim_type.equality_text",
   219|         "dim_type.equality_witness_display",
   220|         "dim_type.centerline_pattern_sig_hash",
   221|         "dim_type.centerline_symbol_name",
   222|         "dim_type.centerline_tick_mark_sig_hash",
   223|         "dim_type.interior_tick_mark_sig_hash",
   224|         "dim_type.interior_tick_mark_display",
   225|         "dim_type.text_offset_in",
   226|         "dim_type.dimension_string_type",
   227|         "dim_type.show_opening_height",
   228|         "dim_type.alternate_units",
   229|         "dim_type.alternate_units_prefix",
   230|         "dim_type.alternate_units_suffix"
   231|       ],
   232|       "required_keys": [
   233|         "dim_type.shape",
   234|         "dim_type.accuracy",
   235|         "dim_type.tick_mark_sig_hash",
   236|         "dim_type.witness_line_control",
   237|         "dim_type.unit_format_id",
   238|         "dim_type.rounding",
   239|         "dim_type.prefix",
   240|         "dim_type.suffix"
   241|       ],
   242|       "minima": {
   243|         "block_if_any_required_not_ok": true
   244|       },
   245|       "notes": [
   246|         "Area 7 (§2 leader config, §3 witness lines, §4 equality/centerline/tick weight, §5 alternate units, §6 suppress_spaces, §7 dimension string type/show opening height/text offset) added dim_type.suppress_spaces through dim_type.alternate_units_suffix above as non-required enrichment identity items. dim_type.witness_line_tick_mark_sig_hash, dim_type.dimension_string_type, and dim_type.show_opening_height are Linear-only (not Angular) per probe observed_on_shapes, confirmed consistent across all three approved external probe runs. Verified against the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass. No sig_hash_keys override added: this domain had none before this change, so new allowed_keys flow into the analysis-side sig_hash preimage the same way its pre-existing allowed_keys always have.",
   247|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained.",
   248|         "PR #412 review fix: dim_type.centerline_pattern_name was renamed to dim_type.centerline_pattern_sig_hash and now resolves via core/dimension_type_helpers._read_line_pattern_ref_sig_hash() (the same ctx[\"line_pattern_uid_to_hash\"]/ctx[\"line_pattern_special_values\"] resolution domains/object_styles.py already uses), instead of a plain doc.GetElement() name lookup that returned missing for built-in pattern ids like the probe's -3000010 (\"Solid\"), collapsing it with \"no pattern\"."
   249|       ],
   250|       "sig_hash_schema": "dimension_types_linear.sig_hash.v2"
   251|     },
   252|     "dimension_types_radial": {
   253|       "domain_family": "dimension_types",
   254|       "display_label": "Dimension Types — Radial",
   255|       "allowed_keys": [
   256|         "dim_type.shape",
   257|         "dim_type.accuracy",
   258|         "dim_type.tick_mark_sig_hash",
   259|         "dim_type.center_marks",
   260|         "dim_type.center_mark_size",
   261|         "dim_type.radius_symbol_location",
   262|         "dim_type.radius_symbol_text",
   263|         "dim_type.unit_format_id",
   264|         "dim_type.text_font",
   265|         "dim_type.text_size_in",
   266|         "dim_type.text_bold",
   267|         "dim_type.text_italic",
   268|         "dim_type.text_underline",
   269|         "dim_type.text_width_factor",
   270|         "dim_type.text_background",
   271|         "dim_type.color_rgb",
   272|         "dim_type.line_weight",
   273|         "dim_type.suppress_spaces",
   274|         "dim_type.leader_tick_mark_sig_hash",
   275|         "dim_type.leader_type",
   276|         "dim_type.show_leader_when_text_moves",
   277|         "dim_type.tick_mark_line_weight",
   278|         "dim_type.text_offset_in",
   279|         "dim_type.alternate_units",
   280|         "dim_type.alternate_units_prefix",
   281|         "dim_type.alternate_units_suffix"
   282|       ],
   283|       "required_keys": [
   284|         "dim_type.shape",
   285|         "dim_type.accuracy",
   286|         "dim_type.tick_mark_sig_hash",
   287|         "dim_type.center_marks",
   288|         "dim_type.center_mark_size",
   289|         "dim_type.radius_symbol_location",
   290|         "dim_type.radius_symbol_text",
   291|         "dim_type.unit_format_id"
   292|       ],
   293|       "minima": {
   294|         "block_if_any_required_not_ok": true
   295|       },
   296|       "notes": [
   297|         "Area 7 (§2 leader config, §4c tick weight, §5 alternate units, §6 suppress_spaces, §7 text offset) added dim_type.suppress_spaces through dim_type.alternate_units_suffix above as non-required enrichment identity items -- Witness Lines/Equality/Centerline do NOT apply to Radial/Diameter per probe observed_on_shapes (Linear/Angular only), correcting an initial guess that they might. Verified against the approved external probe dataset documented in tools/probes/Exports/README.md rather than a fresh live-Revit run in this pass. No sig_hash_keys override added: this domain had none before this change, so new allowed_keys flow into the analysis-side sig_hash preimage the same way its pre-existing allowed_keys always have.",
   298|         "PR #412 review fix: dim_type.alternate_units_format_id was dropped (required DimensionType.GetAlternateUnitsFormatOptions(), an accessor not confirmed to exist on the Revit surface this repo's probe data represents, which made every record degrade without ever capturing real data) -- dim_type.alternate_units/_prefix/_suffix are unaffected and retained."
   299|       ],
   300|       "sig_hash_schema": "dimension_types_radial.sig_hash.v2"
   301|     },
   302|     "dimension_types_spot_coordinate": {
   303|       "domain_family": "dimension_types",
   304|       "display_label": "Dimension Types — Spot Coordinate",
   305|       "allowed_keys": [
   306|         "dim_type.shape",
   307|         "dim_type.unit_format_id",
   308|         "dim_type.top_coordinate",
   309|         "dim_type.bottom_coordinate",
   310|         "dim_type.north_south_indicator",
   311|         "dim_type.east_west_indicator",
   312|         "dim_type.include_elevation",
   313|         "dim_type.elevation_indicator",
   314|         "dim_type.indicator_as_prefix_suffix",
   315|         "dim_type.text_orientation",
   316|         "dim_type.text_location",
   317|         "dim_type.symbol_name",
   318|         "dim_type.leader_arrowhead_sig_hash",
   319|         "dim_type.text_font",
   320|         "dim_type.text_size_in",
   321|         "dim_type.text_bold",
   322|         "dim_type.text_italic",
   323|         "dim_type.text_underline",
   324|         "dim_type.text_width_factor",
   325|         "dim_type.text_background",
   326|         "dim_type.color_rgb",
   327|         "dim_type.line_weight",
   328|         "dim_type.suppress_spaces",
   329|         "dim_type.leader_arrowhead_uid",
   330|         "dim_type.leader_arrowhead_name",
   331|         "dim_type.leader_arrowhead_line_weight",
   332|         "dim_type.leader_line_weight",
   333|         "dim_type.rotate_with_component",
   334|         "dim_type.coordinate_base",
   335|         "dim_type.text_offset_from_leader_in",
   336|         "dim_type.text_offset_from_symbol_in",
   337|         "dim_type.alternate_units",
   338|         "dim_type.alternate_units_prefix",
   339|         "dim_type.alternate_units_suffix"
   340|       ],
   341|       "required_keys": [
   342|         "dim_type.shape",
   343|         "dim_type.unit_format_id",
   344|         "dim_type.top_coordinate",
   345|         "dim_type.bottom_coordinate",
   346|         "dim_type.north_south_indicator",
   347|         "dim_type.east_west_indicator",
   348|         "dim_type.include_elevation",
   349|         "dim_type.elevation_indicator",
   350|         "dim_type.indicator_as_prefix_suffix",
   351|         "dim_type.text_orientation",
   352|         "dim_type.text_location",
   353|         "dim_type.symbol_name"
   354|       ],
   355|       "minima": {
   356|         "block_if_any_required_not_ok": true
   357|       },
   358|       "sig_hash_keys": [
   359|         "dim_type.shape",
   360|         "dim_type.unit_format_id",
   361|         "dim_type.top_coordinate",
   362|         "dim_type.bottom_coordinate",
   363|         "dim_type.north_south_indicator",
   364|         "dim_type.east_west_indicator",
   365|         "dim_type.include_elevation",
   366|         "dim_type.elevation_indicator",
   367|         "dim_type.indicator_as_prefix_suffix",
   368|         "dim_type.text_orientation",
   369|         "dim_type.text_location",
   370|         "dim_type.symbol_name",
   371|         "dim_type.leader_arrowhead_sig_hash",
   372|         "dim_type.text_font",
   373|         "dim_type.text_size_in",
   374|         "dim_type.text_bold",
   375|         "dim_type.text_italic",
   376|         "dim_type.text_underline",
   377|         "dim_type.text_width_factor",
   378|         "dim_type.text_background",
   379|         "dim_type.color_rgb",
   380|         "dim_type.line_weight",
   381|         "dim_type.suppress_spaces",
   382|         "dim_type.leader_arrowhead_line_weight",
   383|         "dim_type.leader_line_weight",
   384|         "dim_type.rotate_with_component",
   385|         "dim_type.coordinate_base",
   386|         "dim_type.text_offset_from_leader_in",
   387|         "dim_type.text_offset_from_symbol_in",
   388|         "dim_type.alternate_units",
   389|         "dim_type.alternate_units_prefix",
   390|         "dim_type.alternate_units_suffix"
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
```
