# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 4
- Original line range: 766-1335
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, generate_packet_from_request._maybe_file_expansion
- Source SHA-256: e5af5f07850e5e55e3c59afdede5ca2e2d0d048df70a923234802e541f9466b2
- Starts inside symbol: no
- Ends inside symbol: no

```
   766| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   767|                                   name_override: Optional[str] = None) -> tuple:
   768|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   769| 
   770|     error_message_or_None is set (and packet_path is None) for a
   771|     structurally invalid request, or when strict mode / an unresolvable
   772|     explicit-selector budget conflict blocks generation -- no partial
   773|     packet is written in either case.
   774|     """
   775|     try:
   776|         request_text = request_path.read_text(encoding="utf-8")
   777|     except OSError as exc:
   778|         return None, [], f"could not read request file {request_path}: {exc}"
   779| 
   780|     request_hash = sha256_text(request_text)
   781|     resolved, errors = parse_and_validate_request(request_text)
   782|     if errors:
   783|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   784| 
   785|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   786|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   787|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   788|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   789|     files_by_path = {r["relative_path"]: r for r in files_rows}
   790|     symbols_by_file: dict = {}
   791|     for r in symbols_rows:
   792|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   793| 
   794|     file_resolutions = resolve_files(resolved.files, files_by_path)
   795|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   796|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   797|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   798|         root, resolved.search_terms, resolved.search_as_regex, files_rows, resolved.max_files,
   799|     )
   800|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   801| 
   802|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   803|     if resolved.strict and unresolved_explicit:
   804|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   805|         return None, [_res_to_dict(r) for r in all_resolutions], (
   806|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   807|         )
   808| 
   809|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   810|     out: list = []
   811|     focus_files: list = []
   812| 
   813|     communities_by_file: dict = {}
   814|     if resolved.include_graphify:
   815|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   816|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   817|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   818|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   819|         )
   820|         for w in graphify_warnings_for_request:
   821|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   822| 
   823|     def note_focus_file(rel: str) -> bool:
   824|         if rel in focus_files:
   825|             return True
   826|         if len(focus_files) >= resolved.max_files:
   827|             return False
   828|         focus_files.append(rel)
   829|         return True
   830| 
   831|     # --- Reserve the fixed framing's budget cost up front ---
   832|     # The header, the selector-resolution report, and the footer are
   833|     # essential, always-rendered provenance -- not optional content -- so
   834|     # none of them can be dropped to make room. But charging their cost
   835|     # only after Tier-1/Tier-2 content had already spent against the full,
   836|     # unreserved budget (as a prior version did) let Tier-1/Tier-2 content
   837|     # spend as if framing were free: a request could "succeed" with
   838|     # Tier-1 content that, combined with the framing charged on top
   839|     # afterward, made the packet's *actual* rendered size exceed
   840|     # limits.max_estimated_tokens even though generation reported success.
   841|     # Reserving framing's cost first means a too-tight budget now
   842|     # correctly surfaces as an explicit_conflicts abort (an explicit
   843|     # selector's excerpt no longer fits once framing's real cost is
   844|     # subtracted) instead of a "successful" packet whose true size is
   845|     # larger than what was requested.
   846|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   847|     # Every entry below carries its own trailing "\n" -- header_lines is
   848|     # assembled with plain concatenation ("".join), not "\n".join(), so
   849|     # that the *number* of entries (which grows later: the "Estimated
   850|     # tokens used" line, then every resolution-report line) can never
   851|     # introduce an uncharged join-separator character. An earlier version
   852|     # relied on "\n".join(header_lines) for spacing, which silently added
   853|     # one character per entry that was never included in any budget.spend()
   854|     # call -- harmless for the fixed initial entries (accounted for
   855|     # correctly at the time header_text below was computed), but wrong
   856|     # once more entries were extended in afterward.
   857|     header_lines = [
   858|         "# Repo Context Packet (from packet_request.json)\n",
   859|         f"- Root: `{root.resolve().name}`\n",
   860|         f"- Question: {resolved.question}\n",
   861|         f"- schema_version: {resolved.schema_version}\n",
   862|         f"- Tool version: {TOOL_VERSION}\n",
   863|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)\n",
   864|     ]
   865|     if git_info.get("available"):
   866|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
   867|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)\n")
   868|     else:
   869|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)\n")
   870|     header_lines.append(
   871|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
   872|         f"max_hops={resolved.max_hops}\n"
   873|     )
   874|     header_text = "".join(header_lines)
   875| 
   876|     # Same fixed-framing reasoning as the header -- always rendered in
   877|     # full, reserved together with it (see below) so nothing else can
   878|     # spend against budget the footer will also need.
   879|     footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
   880|               "dispatch. See README.md in this output directory for full limitations._\n")
   881| 
   882|     # The "Estimated tokens used" summary line (appended at the very end,
   883|     # once Tier-1/Tier-2 are done) reports budget.chars_used itself -- its
   884|     # own exact text isn't knowable up front, but its *worst-case width*
   885|     # is: chars_used can never exceed budget.max_characters (every spend
   886|     # of variable content is gated by budget.allow() first), so building
   887|     # the placeholder with max_characters standing in for both the
   888|     # rounded-token and raw-char figures is guaranteed to be at least as
   889|     # wide as the real line will be. Reserving that placeholder now, atomically
   890|     # with the header and footer, means the real line (substituted in
   891|     # unchanged at the end, needing no separate spend) can never be the
   892|     # thing that pushes the packet over budget.
   893|     estimated_line_placeholder = (
   894|         f"- Estimated tokens used: ~{resolved.max_estimated_tokens} "
   895|         f"(chars_used={budget.max_characters}/{budget.max_characters})\n"
   896|     )
   897| 
   898|     # The "## Selector resolution report" section heading is, like the
   899|     # header/footer/summary line, always rendered in full regardless of
   900|     # request content (every valid request resolves at least one
   901|     # selector) -- reserve it in the same atomic check rather than as a
   902|     # separate, easy-to-forget budget.spend() of its own.
   903|     resolution_report_header = "## Selector resolution report\n"
   904| 
   905|     # Header, footer, the summary-line placeholder, and the resolution-
   906|     # report heading must be reserved *together*, in one atomic check,
   907|     # before anything else (including individual resolution-report
   908|     # entries below) is allowed to spend -- reserving the header alone
   909|     # first (an earlier version of this fix) still let a resolution-
   910|     # report entry's own budget.allow() check pass against a budget that
   911|     # hadn't yet accounted for the footer, so the footer's later
   912|     # unconditional spend pushed the total over the cap anyway. All four
   913|     # are mandatory and unshrinkable, so if they don't fit *together* in
   914|     # the requested budget, no amount of Tier-1/Tier-2 selector content
   915|     # could ever have fit either -- fail the request outright instead of
   916|     # writing a packet whose true size exceeds what was asked for.
   917|     framing_text = header_text + estimated_line_placeholder + resolution_report_header + footer
   918|     if not budget.allow(framing_text, len(header_lines) + 3):
   919|         return None, [_res_to_dict(r) for r in all_resolutions], (
   920|             f"limits.max_estimated_tokens ({resolved.max_estimated_tokens}) is too small to fit this packet's "
   921|             f"fixed framing (header + footer + packet-size summary line + resolution-report heading, before "
   922|             f"any selector content or individual resolution-report entries) alone; increase "
   923|             f"limits.max_estimated_tokens."
   924|         )
   925|     budget.spend(framing_text, len(header_lines) + 3)
   926| 
   927|     # The resolution report's *entries* scale with the *request* (a
   928|     # request naming hundreds of missing/ambiguous selectors could
   929|     # otherwise render an unbounded report regardless of
   930|     # limits.max_estimated_tokens) -- charge each against the same budget
   931|     # as everything else, with a count-of-omitted note rather than an
   932|     # unbounded listing. The full, untruncated report is always available
   933|     # in the accompanying packet_<name>.resolution.json sidecar. Computed
   934|     # here (reserved up front, alongside the header/footer) rather than
   935|     # after Tier-1/Tier-2 render, since it depends only on
   936|     # `all_resolutions` (already resolved above), not on anything
   937|     # Tier-1/Tier-2 produce.
   938|     # Same self-terminated-line convention as header_lines above -- every
   939|     # entry ends with its own "\n" so resolution_lines can be concatenated
   940|     # (not "\n".join()'d) into header_lines without an uncharged separator
   941|     # per entry. The section heading itself was already reserved above
   942|     # (as part of framing_text), so it's included here only for
   943|     # rendering, not spent a second time.
   944|     resolution_lines = [resolution_report_header]
   945|     omitted_selector_count = 0
   946|     for idx, r in enumerate(all_resolutions):
   947|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}\n"]
   948|         entry.extend(f"  - candidate: `{c}`\n" for c in r.candidates)
   949|         entry_text = "".join(entry)
   950|         if not budget.allow(entry_text, len(entry)):
   951|             omitted_selector_count = len(all_resolutions) - idx
   952|             break
   953|         resolution_lines.extend(entry)
   954|         budget.spend(entry_text, len(entry))
   955|     if omitted_selector_count:
   956|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
   957|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.\n")
   958|         if budget.allow(note, 1):
   959|             resolution_lines.append(note)
   960|             budget.spend(note, 1)
   961| 
   962|     # --- Tier 1: explicit selectors (never silently dropped) ---
   963|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   964|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   965|     # Freshness withholding and non-mandatory expansions (callers/callees/
   966|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   967|     # `budget` and must NOT abort generation.
   968|     #
   969|     # Two passes, deliberately in this order:
   970|     #   1. Render every explicit selector's own header/excerpt first (and
   971|     #      only once per distinct target -- two selectors naming the same
   972|     #      file/symbol/line render it a single time). None of this may be
   973|     #      pre-empted by expansion content.
   974|     #   2. Only once every explicit item has had its guaranteed shot at the
   975|     #      budget do optional expansions (callers/callees/imports/tests/
   976|     #      Graphify) spend whatever budget remains. Interleaving expansion
   977|     #      spend between explicit items (the previous structure) let one
   978|     #      selector's expansions manufacture a budget conflict for a later,
   979|     #      otherwise-fitting explicit selector.
   980|     explicit_conflicts: list = []
   981|     rendered_files: set = set()
   982|     rendered_symbols: set = set()
   983|     rendered_lines: set = set()
   984|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   985|     symbol_expansion_items: list = []  # [row]
   986| 
   987|     def _spend_header(header: str) -> bool:
   988|         if not budget.allow(header, 2):
   989|             return False
   990|         out.append(header)
   991|         budget.spend(header, 2)
   992|         return True
   993| 
   994|     for res in file_resolutions:
   995|         if res.status != "resolved":
   996|             continue
   997|         rel = res.requested
   998|         if rel in rendered_files:
   999|             # Duplicate selector for a file already attempted -- its first
  1000|             # occurrence's outcome (rendered, or a recorded conflict) is
  1001|             # final; re-attempting an identical selector would just repeat
  1002|             # the same header/budget work (or the same conflict message)
  1003|             # once per repeat, which is itself an unbounded-output shape
  1004|             # for a request naming the same selector many times over.
  1005|             continue
  1006|         rendered_files.add(rel)
  1007|         if not note_focus_file(rel):
  1008|             explicit_conflicts.append(
  1009|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
  1010|             )
  1011|             continue
  1012|         row = res.resolved_rows[0]
  1013|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
  1014|         if not _spend_header(header):
  1015|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
  1016|             continue
  1017|         # Render the mandatory excerpt (the actual content the selector asked
  1018|         # for) before the optional top-level-symbols inventory, so the
  1019|         # inventory can never spend the shared budget ahead of the excerpt
  1020|         # itself and force it into an "explicit_conflicts" abort.
  1021|         try:
  1022|             line_count = int(row.get("line_count") or 0)
  1023|         except ValueError:
  1024|             line_count = 0
  1025|         if line_count:
  1026|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
  1027|             if status == "too_large":
  1028|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
  1029|                 continue
  1030|         top_level = sorted(
  1031|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
  1032|             key=lambda r: int(r["start_line"]),
  1033|         )
  1034|         # The "Top-level symbols:" inventory itself is optional metadata,
  1035|         # same as this symbol's caller/callee/etc. expansions -- deferred
  1036|         # to the pass below (after every explicit file/symbol/line
  1037|         # selector has had its guaranteed shot at the budget), so file A's
  1038|         # inventory can never spend budget that file B's own explicit
  1039|         # excerpt needed.
  1040|         file_expansion_items.append((rel, top_level))
  1041| 
  1042|     for res in symbol_resolutions:
  1043|         if res.status != "resolved":
  1044|             continue
  1045|         row = res.resolved_rows[0]
  1046|         rel = row["relative_path"]
  1047|         symbol_key = (rel, row["qualified_name"])
  1048|         if symbol_key in rendered_symbols:
  1049|             continue  # duplicate selector; first occurrence's outcome is final
  1050|         rendered_symbols.add(symbol_key)
  1051|         if not note_focus_file(rel):
  1052|             explicit_conflicts.append(
  1053|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
  1054|                 f"limits.max_files ({resolved.max_files}) reached"
  1055|             )
  1056|             continue
  1057|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
  1058|         if not _spend_header(header):
  1059|             explicit_conflicts.append(
  1060|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
  1061|             )
  1062|             continue
  1063|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
  1064|                                         files_by_path.get(rel, {}).get("sha256", ""))
  1065|         if status == "too_large":
  1066|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
  1067|         symbol_expansion_items.append(row)
  1068| 
  1069|     for res in line_resolutions:
  1070|         if res.status != "resolved":
  1071|             continue
  1072|         info = res.resolved_rows[0]
  1073|         rel = info["file"]
  1074|         line_key = (rel, info["start"], info["end"])
  1075|         if line_key in rendered_lines:
  1076|             continue  # duplicate selector; first occurrence's outcome is final
  1077|         rendered_lines.add(line_key)
  1078|         if not note_focus_file(rel):
  1079|             explicit_conflicts.append(
  1080|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
  1081|             )
  1082|             continue
  1083|         # Only substitute a smaller enclosing symbol when it contains
  1084|         # *both* endpoints of the requested range -- a symbol containing
  1085|         # just the start line (the old check) could be smaller than the
  1086|         # actual request (e.g. lines 2-7 where line 2's enclosing function
  1087|         # ends at line 2), silently truncating the rendered excerpt to
  1088|         # that symbol's own bounds while the resolution report still
  1089|         # claimed the full range was resolved.
  1090|         enclosing = [
  1091|             r for r in symbols_by_file.get(rel, [])
  1092|             if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
  1093|             and r["symbol_type"] != "module"
  1094|         ]
  1095|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
  1096|         if not _spend_header(header):
  1097|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
  1098|             continue
  1099|         if enclosing:
  1100|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
  1101|             row = enclosing[0]
  1102|             enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
  1103|                                f"lines {row['start_line']}-{row['end_line']})\n")
  1104|             # Metadata, not the mandatory excerpt itself -- budgeted like
  1105|             # every other optional line, with a non-fatal skip if it
  1106|             # doesn't fit rather than silently rendering it unbudgeted.
  1107|             if budget.allow(enclosing_line, 1):
  1108|                 out.append(enclosing_line)
  1109|                 budget.spend(enclosing_line, 1)
  1110|             else:
  1111|                 budget.omissions.append(
  1112|                     f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
  1113|                 )
  1114|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
  1115|                                             files_by_path.get(rel, {}).get("sha256", ""))
  1116|         else:
  1117|             start, end = max(1, info["start"] - 10), info["end"] + 10
  1118|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
  1119|         if status == "too_large":
  1120|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
  1121| 
  1122|     if explicit_conflicts:
  1123|         # An explicit selection itself didn't fit -- either the token
  1124|         # budget or limits.max_files -- per contract this is a hard
  1125|         # conflict, not something to truncate silently.
  1126|         return None, [_res_to_dict(r) for r in all_resolutions], (
  1127|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
  1128|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
  1129|             f"relevant limit or narrow the request. Conflicts:\n"
  1130|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
  1131|         )
  1132| 
  1133|     # Every explicit selector fit -- now spend whatever budget remains on
  1134|     # optional metadata/expansions (callers/callees/imports/tests/
  1135|     # Graphify, plus each explicit file selector's own "Top-level
  1136|     # symbols:" inventory). Done only now, not interleaved with tier-1's
  1137|     # rendering above, so a symbol's expansions -- or one file's inventory
  1138|     # -- can never manufacture a budget conflict for a later explicit
  1139|     # selector.
  1140|     for rel, top_level in file_expansion_items:
  1141|         if top_level:
  1142|             header = "Top-level symbols:\n"
  1143|             if budget.allow(header, 1):
  1144|                 out.append(header)
  1145|                 budget.spend(header, 1)
  1146|                 for idx, r in enumerate(top_level):
  1147|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})\n"
  1148|                     if not budget.allow(line, 1):
  1149|                         budget.omissions.append(
  1150|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
  1151|                             f"listing (packet size limit reached); see python_symbols.csv."
  1152|                         )
  1153|                         break
  1154|                     out.append(line)
  1155|                     budget.spend(line, 1)
  1156|             else:
  1157|                 budget.omissions.append(
  1158|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
  1159|                     f"see python_symbols.csv."
  1160|                 )
  1161|     file_level_expansion_done: set = set()
  1162| 
  1163|     def _maybe_file_expansion(rel: str) -> None:
  1164|         # Once per file regardless of how many symbols in it get
  1165|         # expanded, and regardless of whether that file is reached via a
  1166|         # file selector's own top-level symbols or a distinct explicit
  1167|         # symbol selector in the same file.
  1168|         if rel in file_level_expansion_done:
  1169|             return
  1170|         file_level_expansion_done.add(rel)
  1171|         _file_expansion(rel, imports_rows, calls_rows, files_by_path, resolved, budget, out, note_focus_file,
  1172|                          communities_by_file)
  1173| 
  1174|     for rel, top_level in file_expansion_items:
  1175|         for r in top_level:
  1176|             _symbol_expansion(r, calls_rows, resolved, budget, out, note_focus_file)
  1177|         # File-level expansion (imports/related-tests/Graphify peers) must
  1178|         # run for every explicitly selected file, not just ones with
  1179|         # top-level symbols -- a symbol-free file (e.g. an __init__.py
  1180|         # re-export shim, or a plain config module) previously never got
  1181|         # its imports/related-tests/Graphify-peer expansion at all, since
  1182|         # this loop used to skip straight past it when `top_level` was
  1183|         # empty. Those expansions describe the file, not any symbol in
  1184|         # it, so they apply regardless of whether the file happens to
  1185|         # define any top-level symbols.
  1186|         _maybe_file_expansion(rel)
  1187|     for row in symbol_expansion_items:
  1188|         _symbol_expansion(row, calls_rows, resolved, budget, out, note_focus_file)
  1189|         _maybe_file_expansion(row["relative_path"])
  1190| 
  1191|     # --- Tier 2: exact search-term matches ---
  1192|     # Matches were already computed by resolve_search_terms() above (before
  1193|     # the strict-mode gate) -- reused here rather than re-scanning the
  1194|     # repository a second time.
  1195|     for term in resolved.search_terms:
  1196|         matches = search_matches_by_term.get(term, [])
  1197|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
  1198|         if term_status == "invalid":
  1199|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
  1200|             if not budget.allow(notice, 1):
  1201|                 # Unbounded per-term notices (e.g. a request with hundreds
  1202|                 # of invalid regex terms) would otherwise bypass
  1203|                 # limits.max_estimated_tokens entirely, same failure shape
  1204|                 # as the earlier unbudgeted resolution-report finding.
  1205|                 budget.omissions.append(
  1206|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
  1207|                     f"see the resolution report."
  1208|                 )
  1209|                 continue
  1210|             out.append(notice)
  1211|             budget.spend(notice, 1)
  1212|             continue
  1213|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
  1214|         if not budget.allow(header, 1):
  1215|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
  1216|             continue
  1217|         out.append(header); budget.spend(header, 1)
  1218|         max_files_note_emitted = False
  1219|         for rel, ln, text in matches:
  1220|             # Redact the full line before truncating it, not after -- a
  1221|             # secret-shaped value whose closing quote falls beyond
  1222|             # character 200 would otherwise have that quote cut off
  1223|             # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
  1224|             # backreference so redact_secrets() never matches and the
  1225|             # (truncated) secret prefix leaks into the packet.
  1226|             line_text = redact_secrets(text.strip())[:200]
  1227|             line = f"- `{rel}:{ln}` — `{line_text}`\n"
  1228|             # Check the budget *before* reserving a focus-file slot for
  1229|             # this match -- otherwise a match that ultimately doesn't fit
  1230|             # (and is never rendered) could still consume the one
  1231|             # remaining limits.max_files slot, starving a later, shorter
  1232|             # match from a different file that would have fit.
  1233|             if not budget.allow(line, 1):
  1234|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
  1235|                 break
  1236|             if not note_focus_file(rel):
  1237|                 # note_focus_file enforces limits.max_files against the
  1238|                 # *global* focus-file set shared across every selector/tier
  1239|                 # in this packet, not just this one search term -- so the
  1240|                 # cap holds even when different terms match different files.
  1241|                 # A match in a file beyond the cap doesn't mean every
  1242|                 # *later* match is unreachable too -- a later match may be
  1243|                 # in a file already in focus_files (e.g. the selected
  1244|                 # file), which costs no new slot. Skip this one match and
  1245|                 # keep checking the rest instead of abandoning the term.
  1246|                 if not max_files_note_emitted:
  1247|                     budget.omissions.append(
  1248|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
  1249|                     )
  1250|                     max_files_note_emitted = True
  1251|                 continue
  1252|             out.append(line); budget.spend(line, 1)
  1253|     if stale_search_files:
  1254|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
  1255|                                  f"skipped for search terms; re-run scan.")
  1256| 
  1257|     if budget.omissions:
  1258|         # The omissions *list* itself is unbounded in memory (a request
  1259|         # that triggers hundreds of distinct omission reasons -- e.g. many
  1260|         # invalid regex search terms -- can produce hundreds of entries),
  1261|         # so rendering it must be budgeted the same way the resolution
  1262|         # report is: otherwise this section alone could bypass
  1263|         # limits.max_estimated_tokens. The full list is always available,
  1264|         # unbudgeted, in the packet_*.resolution.json sidecar's
  1265|         # "omissions" field.
  1266|         header = "\n## Omitted / unresolved\n"
  1267|         if budget.allow(header, 1):
  1268|             out.append(header)
  1269|             budget.spend(header, 1)
  1270|             omitted_omissions = 0
  1271|             for idx, o in enumerate(budget.omissions):
  1272|                 line = f"- {o}\n"
  1273|                 if not budget.allow(line, 1):
  1274|                     omitted_omissions = len(budget.omissions) - idx
  1275|                     break
  1276|                 out.append(line)
  1277|                 budget.spend(line, 1)
  1278|             if omitted_omissions:
  1279|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
  1280|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1281|                         f"the complete list.\n")
  1282|                 if budget.allow(note, 1):
  1283|                     out.append(note)
  1284|                     budget.spend(note, 1)
  1285| 
  1286|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1287|     # resolution report" section built into the header below -- not
  1288|     # repeated here. header_lines/resolution_lines/footer were built and
  1289|     # charged against the budget up front, before Tier-1/Tier-2 rendering
  1290|     # -- see the "Reserve the fixed framing's budget cost up front"
  1291|     # comment above.)
  1292| 
  1293|     # Computed last so it reflects Tier-1/Tier-2's and the resolution
  1294|     # report's/footer's own budget spend too. Its width was already
  1295|     # reserved (as a worst-case placeholder) atomically with the header/
  1296|     # footer above, so this real line -- guaranteed no wider than that
  1297|     # placeholder, since chars_used can never exceed max_characters --
  1298|     # needs no separate budget.spend() of its own.
  1299|     header_lines.append(
  1300|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1301|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1302|     )
  1303|     header_lines.extend(resolution_lines)
  1304| 
  1305|     out.append(footer)
  1306| 
  1307|     # Plain concatenation, not "\n".join() -- every element of both lists
  1308|     # is self-terminated with its own "\n" (see the comments where
  1309|     # header_lines/resolution_lines/out entries are built), so no
  1310|     # separator character needs inserting between them. A join here would
  1311|     # silently add one uncharged character per element -- exactly the gap
  1312|     # that let a packet's true rendered size exceed limits.max_estimated_tokens
  1313|     # while the sidecar reported it fit.
  1314|     text = "".join(header_lines) + "".join(out) + "\n"
  1315| 
  1316|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1317|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1318|     atomic_write_text(packet_path, text)
  1319| 
  1320|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1321|     sidecar = {
  1322|         "tool_version": TOOL_VERSION,
  1323|         "schema_version": resolved.schema_version,
  1324|         "question": resolved.question,
  1325|         "request_file_sha256": request_hash,
  1326|         "git": git_info,
  1327|         "estimated_tokens_used": round(budget.chars_used / 4),
  1328|         "focus_files": focus_files,
  1329|         "omissions": budget.omissions,
  1330|         "resolution_report": resolution_report,
  1331|     }
  1332|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1333|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1334| 
  1335|     return packet_path, resolution_report, None
```
