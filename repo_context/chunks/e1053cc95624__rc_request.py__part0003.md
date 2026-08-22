# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 4
- Original line range: 758-1276
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, generate_packet_from_request._maybe_file_expansion
- Source SHA-256: da6b351bdc8071f0313b339e641c5fdb991fa445c5ab75e6b857884c94d04dea
- Starts inside symbol: no
- Ends inside symbol: no

```
   758| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   759|                                   name_override: Optional[str] = None) -> tuple:
   760|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   761| 
   762|     error_message_or_None is set (and packet_path is None) for a
   763|     structurally invalid request, or when strict mode / an unresolvable
   764|     explicit-selector budget conflict blocks generation -- no partial
   765|     packet is written in either case.
   766|     """
   767|     try:
   768|         request_text = request_path.read_text(encoding="utf-8")
   769|     except OSError as exc:
   770|         return None, [], f"could not read request file {request_path}: {exc}"
   771| 
   772|     request_hash = sha256_text(request_text)
   773|     resolved, errors = parse_and_validate_request(request_text)
   774|     if errors:
   775|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   776| 
   777|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   778|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   779|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   780|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   781|     files_by_path = {r["relative_path"]: r for r in files_rows}
   782|     symbols_by_file: dict = {}
   783|     for r in symbols_rows:
   784|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   785| 
   786|     file_resolutions = resolve_files(resolved.files, files_by_path)
   787|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   788|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   789|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   790|         root, resolved.search_terms, resolved.search_as_regex, files_rows, resolved.max_files,
   791|     )
   792|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   793| 
   794|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   795|     if resolved.strict and unresolved_explicit:
   796|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   797|         return None, [_res_to_dict(r) for r in all_resolutions], (
   798|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   799|         )
   800| 
   801|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   802|     out: list = []
   803|     focus_files: list = []
   804| 
   805|     communities_by_file: dict = {}
   806|     if resolved.include_graphify:
   807|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   808|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   809|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   810|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   811|         )
   812|         for w in graphify_warnings_for_request:
   813|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   814| 
   815|     def note_focus_file(rel: str) -> bool:
   816|         if rel in focus_files:
   817|             return True
   818|         if len(focus_files) >= resolved.max_files:
   819|             return False
   820|         focus_files.append(rel)
   821|         return True
   822| 
   823|     # --- Reserve the fixed framing's budget cost up front ---
   824|     # The header, the selector-resolution report, and the footer are
   825|     # essential, always-rendered provenance -- not optional content -- so
   826|     # none of them can be dropped to make room. But charging their cost
   827|     # only after Tier-1/Tier-2 content had already spent against the full,
   828|     # unreserved budget (as a prior version did) let Tier-1/Tier-2 content
   829|     # spend as if framing were free: a request could "succeed" with
   830|     # Tier-1 content that, combined with the framing charged on top
   831|     # afterward, made the packet's *actual* rendered size exceed
   832|     # limits.max_estimated_tokens even though generation reported success.
   833|     # Reserving framing's cost first means a too-tight budget now
   834|     # correctly surfaces as an explicit_conflicts abort (an explicit
   835|     # selector's excerpt no longer fits once framing's real cost is
   836|     # subtracted) instead of a "successful" packet whose true size is
   837|     # larger than what was requested.
   838|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   839|     header_lines = [
   840|         "# Repo Context Packet (from packet_request.json)\n",
   841|         f"- Root: `{root.resolve().name}`",
   842|         f"- Question: {resolved.question}",
   843|         f"- schema_version: {resolved.schema_version}",
   844|         f"- Tool version: {TOOL_VERSION}",
   845|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
   846|     ]
   847|     if git_info.get("available"):
   848|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
   849|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
   850|     else:
   851|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
   852|     header_lines.append(
   853|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
   854|         f"max_hops={resolved.max_hops}"
   855|     )
   856|     header_text = "\n".join(header_lines) + "\n"
   857| 
   858|     # Same fixed-framing reasoning as the header -- always rendered in
   859|     # full, reserved together with it (see below) so nothing else can
   860|     # spend against budget the footer will also need.
   861|     footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
   862|               "dispatch. See README.md in this output directory for full limitations._\n")
   863| 
   864|     # Header and footer must be reserved *together*, in one atomic check,
   865|     # before anything else (including the selector-resolution report
   866|     # below) is allowed to spend -- reserving the header alone first (an
   867|     # earlier version of this fix) still let a resolution-report entry's
   868|     # own budget.allow() check pass against a budget that hadn't yet
   869|     # accounted for the footer, so the footer's later unconditional spend
   870|     # pushed the total over the cap anyway. Both are mandatory and
   871|     # unshrinkable, so if they don't fit *together* in the requested
   872|     # budget, no amount of Tier-1/Tier-2 selector content could ever have
   873|     # fit either -- fail the request outright instead of writing a packet
   874|     # whose true size exceeds what was asked for.
   875|     framing_text = header_text + footer
   876|     if not budget.allow(framing_text, len(header_lines) + 1):
   877|         return None, [_res_to_dict(r) for r in all_resolutions], (
   878|             f"limits.max_estimated_tokens ({resolved.max_estimated_tokens}) is too small to fit this packet's "
   879|             f"fixed framing (header + footer, before any selector content or the selector-resolution report) "
   880|             f"alone; increase limits.max_estimated_tokens."
   881|         )
   882|     budget.spend(framing_text, len(header_lines) + 1)
   883| 
   884|     # The resolution report scales with the *request*, not the source
   885|     # repository (a request naming hundreds of missing/ambiguous
   886|     # selectors could otherwise render an unbounded report regardless of
   887|     # limits.max_estimated_tokens) -- charge it against the same budget as
   888|     # everything else, with a count-of-omitted note rather than an
   889|     # unbounded listing. The full, untruncated report is always available
   890|     # in the accompanying packet_<name>.resolution.json sidecar. Computed
   891|     # here (reserved up front, alongside the header/footer) rather than
   892|     # after Tier-1/Tier-2 render, since it depends only on
   893|     # `all_resolutions` (already resolved above), not on anything
   894|     # Tier-1/Tier-2 produce.
   895|     resolution_lines = ["## Selector resolution report\n"]
   896|     omitted_selector_count = 0
   897|     for idx, r in enumerate(all_resolutions):
   898|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
   899|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
   900|         entry_text = "\n".join(entry)
   901|         if not budget.allow(entry_text, len(entry)):
   902|             omitted_selector_count = len(all_resolutions) - idx
   903|             break
   904|         resolution_lines.extend(entry)
   905|         budget.spend(entry_text, len(entry))
   906|     if omitted_selector_count:
   907|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
   908|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
   909|         if budget.allow(note, 1):
   910|             resolution_lines.append(note)
   911|             budget.spend(note, 1)
   912|     resolution_lines.append("")
   913| 
   914|     # --- Tier 1: explicit selectors (never silently dropped) ---
   915|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   916|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   917|     # Freshness withholding and non-mandatory expansions (callers/callees/
   918|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   919|     # `budget` and must NOT abort generation.
   920|     #
   921|     # Two passes, deliberately in this order:
   922|     #   1. Render every explicit selector's own header/excerpt first (and
   923|     #      only once per distinct target -- two selectors naming the same
   924|     #      file/symbol/line render it a single time). None of this may be
   925|     #      pre-empted by expansion content.
   926|     #   2. Only once every explicit item has had its guaranteed shot at the
   927|     #      budget do optional expansions (callers/callees/imports/tests/
   928|     #      Graphify) spend whatever budget remains. Interleaving expansion
   929|     #      spend between explicit items (the previous structure) let one
   930|     #      selector's expansions manufacture a budget conflict for a later,
   931|     #      otherwise-fitting explicit selector.
   932|     explicit_conflicts: list = []
   933|     rendered_files: set = set()
   934|     rendered_symbols: set = set()
   935|     rendered_lines: set = set()
   936|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   937|     symbol_expansion_items: list = []  # [row]
   938| 
   939|     def _spend_header(header: str) -> bool:
   940|         if not budget.allow(header, 2):
   941|             return False
   942|         out.append(header)
   943|         budget.spend(header, 2)
   944|         return True
   945| 
   946|     for res in file_resolutions:
   947|         if res.status != "resolved":
   948|             continue
   949|         rel = res.requested
   950|         if rel in rendered_files:
   951|             # Duplicate selector for a file already attempted -- its first
   952|             # occurrence's outcome (rendered, or a recorded conflict) is
   953|             # final; re-attempting an identical selector would just repeat
   954|             # the same header/budget work (or the same conflict message)
   955|             # once per repeat, which is itself an unbounded-output shape
   956|             # for a request naming the same selector many times over.
   957|             continue
   958|         rendered_files.add(rel)
   959|         if not note_focus_file(rel):
   960|             explicit_conflicts.append(
   961|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   962|             )
   963|             continue
   964|         row = res.resolved_rows[0]
   965|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   966|         if not _spend_header(header):
   967|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   968|             continue
   969|         # Render the mandatory excerpt (the actual content the selector asked
   970|         # for) before the optional top-level-symbols inventory, so the
   971|         # inventory can never spend the shared budget ahead of the excerpt
   972|         # itself and force it into an "explicit_conflicts" abort.
   973|         try:
   974|             line_count = int(row.get("line_count") or 0)
   975|         except ValueError:
   976|             line_count = 0
   977|         if line_count:
   978|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   979|             if status == "too_large":
   980|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   981|                 continue
   982|         top_level = sorted(
   983|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   984|             key=lambda r: int(r["start_line"]),
   985|         )
   986|         # The "Top-level symbols:" inventory itself is optional metadata,
   987|         # same as this symbol's caller/callee/etc. expansions -- deferred
   988|         # to the pass below (after every explicit file/symbol/line
   989|         # selector has had its guaranteed shot at the budget), so file A's
   990|         # inventory can never spend budget that file B's own explicit
   991|         # excerpt needed.
   992|         file_expansion_items.append((rel, top_level))
   993| 
   994|     for res in symbol_resolutions:
   995|         if res.status != "resolved":
   996|             continue
   997|         row = res.resolved_rows[0]
   998|         rel = row["relative_path"]
   999|         symbol_key = (rel, row["qualified_name"])
  1000|         if symbol_key in rendered_symbols:
  1001|             continue  # duplicate selector; first occurrence's outcome is final
  1002|         rendered_symbols.add(symbol_key)
  1003|         if not note_focus_file(rel):
  1004|             explicit_conflicts.append(
  1005|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
  1006|                 f"limits.max_files ({resolved.max_files}) reached"
  1007|             )
  1008|             continue
  1009|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
  1010|         if not _spend_header(header):
  1011|             explicit_conflicts.append(
  1012|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
  1013|             )
  1014|             continue
  1015|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
  1016|                                         files_by_path.get(rel, {}).get("sha256", ""))
  1017|         if status == "too_large":
  1018|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
  1019|         symbol_expansion_items.append(row)
  1020| 
  1021|     for res in line_resolutions:
  1022|         if res.status != "resolved":
  1023|             continue
  1024|         info = res.resolved_rows[0]
  1025|         rel = info["file"]
  1026|         line_key = (rel, info["start"], info["end"])
  1027|         if line_key in rendered_lines:
  1028|             continue  # duplicate selector; first occurrence's outcome is final
  1029|         rendered_lines.add(line_key)
  1030|         if not note_focus_file(rel):
  1031|             explicit_conflicts.append(
  1032|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
  1033|             )
  1034|             continue
  1035|         # Only substitute a smaller enclosing symbol when it contains
  1036|         # *both* endpoints of the requested range -- a symbol containing
  1037|         # just the start line (the old check) could be smaller than the
  1038|         # actual request (e.g. lines 2-7 where line 2's enclosing function
  1039|         # ends at line 2), silently truncating the rendered excerpt to
  1040|         # that symbol's own bounds while the resolution report still
  1041|         # claimed the full range was resolved.
  1042|         enclosing = [
  1043|             r for r in symbols_by_file.get(rel, [])
  1044|             if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
  1045|             and r["symbol_type"] != "module"
  1046|         ]
  1047|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
  1048|         if not _spend_header(header):
  1049|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
  1050|             continue
  1051|         if enclosing:
  1052|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
  1053|             row = enclosing[0]
  1054|             enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
  1055|                                f"lines {row['start_line']}-{row['end_line']})\n")
  1056|             # Metadata, not the mandatory excerpt itself -- budgeted like
  1057|             # every other optional line, with a non-fatal skip if it
  1058|             # doesn't fit rather than silently rendering it unbudgeted.
  1059|             if budget.allow(enclosing_line, 1):
  1060|                 out.append(enclosing_line)
  1061|                 budget.spend(enclosing_line, 1)
  1062|             else:
  1063|                 budget.omissions.append(
  1064|                     f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
  1065|                 )
  1066|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
  1067|                                             files_by_path.get(rel, {}).get("sha256", ""))
  1068|         else:
  1069|             start, end = max(1, info["start"] - 10), info["end"] + 10
  1070|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
  1071|         if status == "too_large":
  1072|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
  1073| 
  1074|     if explicit_conflicts:
  1075|         # An explicit selection itself didn't fit -- either the token
  1076|         # budget or limits.max_files -- per contract this is a hard
  1077|         # conflict, not something to truncate silently.
  1078|         return None, [_res_to_dict(r) for r in all_resolutions], (
  1079|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
  1080|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
  1081|             f"relevant limit or narrow the request. Conflicts:\n"
  1082|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
  1083|         )
  1084| 
  1085|     # Every explicit selector fit -- now spend whatever budget remains on
  1086|     # optional metadata/expansions (callers/callees/imports/tests/
  1087|     # Graphify, plus each explicit file selector's own "Top-level
  1088|     # symbols:" inventory). Done only now, not interleaved with tier-1's
  1089|     # rendering above, so a symbol's expansions -- or one file's inventory
  1090|     # -- can never manufacture a budget conflict for a later explicit
  1091|     # selector.
  1092|     for rel, top_level in file_expansion_items:
  1093|         if top_level:
  1094|             header = "Top-level symbols:"
  1095|             if budget.allow(header, 1):
  1096|                 out.append(header)
  1097|                 budget.spend(header, 1)
  1098|                 for idx, r in enumerate(top_level):
  1099|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
  1100|                     if not budget.allow(line, 1):
  1101|                         budget.omissions.append(
  1102|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
  1103|                             f"listing (packet size limit reached); see python_symbols.csv."
  1104|                         )
  1105|                         break
  1106|                     out.append(line)
  1107|                     budget.spend(line, 1)
  1108|             else:
  1109|                 budget.omissions.append(
  1110|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
  1111|                     f"see python_symbols.csv."
  1112|                 )
  1113|     file_level_expansion_done: set = set()
  1114| 
  1115|     def _maybe_file_expansion(rel: str) -> None:
  1116|         # Once per file regardless of how many symbols in it get
  1117|         # expanded, and regardless of whether that file is reached via a
  1118|         # file selector's own top-level symbols or a distinct explicit
  1119|         # symbol selector in the same file.
  1120|         if rel in file_level_expansion_done:
  1121|             return
  1122|         file_level_expansion_done.add(rel)
  1123|         _file_expansion(rel, imports_rows, calls_rows, files_by_path, resolved, budget, out, note_focus_file,
  1124|                          communities_by_file)
  1125| 
  1126|     for rel, top_level in file_expansion_items:
  1127|         for r in top_level:
  1128|             _symbol_expansion(r, calls_rows, resolved, budget, out, note_focus_file)
  1129|         # File-level expansion (imports/related-tests/Graphify peers) must
  1130|         # run for every explicitly selected file, not just ones with
  1131|         # top-level symbols -- a symbol-free file (e.g. an __init__.py
  1132|         # re-export shim, or a plain config module) previously never got
  1133|         # its imports/related-tests/Graphify-peer expansion at all, since
  1134|         # this loop used to skip straight past it when `top_level` was
  1135|         # empty. Those expansions describe the file, not any symbol in
  1136|         # it, so they apply regardless of whether the file happens to
  1137|         # define any top-level symbols.
  1138|         _maybe_file_expansion(rel)
  1139|     for row in symbol_expansion_items:
  1140|         _symbol_expansion(row, calls_rows, resolved, budget, out, note_focus_file)
  1141|         _maybe_file_expansion(row["relative_path"])
  1142| 
  1143|     # --- Tier 2: exact search-term matches ---
  1144|     # Matches were already computed by resolve_search_terms() above (before
  1145|     # the strict-mode gate) -- reused here rather than re-scanning the
  1146|     # repository a second time.
  1147|     for term in resolved.search_terms:
  1148|         matches = search_matches_by_term.get(term, [])
  1149|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
  1150|         if term_status == "invalid":
  1151|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
  1152|             if not budget.allow(notice, 1):
  1153|                 # Unbounded per-term notices (e.g. a request with hundreds
  1154|                 # of invalid regex terms) would otherwise bypass
  1155|                 # limits.max_estimated_tokens entirely, same failure shape
  1156|                 # as the earlier unbudgeted resolution-report finding.
  1157|                 budget.omissions.append(
  1158|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
  1159|                     f"see the resolution report."
  1160|                 )
  1161|                 continue
  1162|             out.append(notice)
  1163|             budget.spend(notice, 1)
  1164|             continue
  1165|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
  1166|         if not budget.allow(header, 1):
  1167|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
  1168|             continue
  1169|         out.append(header); budget.spend(header, 1)
  1170|         max_files_note_emitted = False
  1171|         for rel, ln, text in matches:
  1172|             # Redact the full line before truncating it, not after -- a
  1173|             # secret-shaped value whose closing quote falls beyond
  1174|             # character 200 would otherwise have that quote cut off
  1175|             # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
  1176|             # backreference so redact_secrets() never matches and the
  1177|             # (truncated) secret prefix leaks into the packet.
  1178|             line_text = redact_secrets(text.strip())[:200]
  1179|             line = f"- `{rel}:{ln}` — `{line_text}`"
  1180|             # Check the budget *before* reserving a focus-file slot for
  1181|             # this match -- otherwise a match that ultimately doesn't fit
  1182|             # (and is never rendered) could still consume the one
  1183|             # remaining limits.max_files slot, starving a later, shorter
  1184|             # match from a different file that would have fit.
  1185|             if not budget.allow(line, 1):
  1186|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
  1187|                 break
  1188|             if not note_focus_file(rel):
  1189|                 # note_focus_file enforces limits.max_files against the
  1190|                 # *global* focus-file set shared across every selector/tier
  1191|                 # in this packet, not just this one search term -- so the
  1192|                 # cap holds even when different terms match different files.
  1193|                 # A match in a file beyond the cap doesn't mean every
  1194|                 # *later* match is unreachable too -- a later match may be
  1195|                 # in a file already in focus_files (e.g. the selected
  1196|                 # file), which costs no new slot. Skip this one match and
  1197|                 # keep checking the rest instead of abandoning the term.
  1198|                 if not max_files_note_emitted:
  1199|                     budget.omissions.append(
  1200|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
  1201|                     )
  1202|                     max_files_note_emitted = True
  1203|                 continue
  1204|             out.append(line); budget.spend(line, 1)
  1205|     if stale_search_files:
  1206|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
  1207|                                  f"skipped for search terms; re-run scan.")
  1208| 
  1209|     if budget.omissions:
  1210|         # The omissions *list* itself is unbounded in memory (a request
  1211|         # that triggers hundreds of distinct omission reasons -- e.g. many
  1212|         # invalid regex search terms -- can produce hundreds of entries),
  1213|         # so rendering it must be budgeted the same way the resolution
  1214|         # report is: otherwise this section alone could bypass
  1215|         # limits.max_estimated_tokens. The full list is always available,
  1216|         # unbudgeted, in the packet_*.resolution.json sidecar's
  1217|         # "omissions" field.
  1218|         header = "\n## Omitted / unresolved\n"
  1219|         if budget.allow(header, 1):
  1220|             out.append(header)
  1221|             budget.spend(header, 1)
  1222|             omitted_omissions = 0
  1223|             for idx, o in enumerate(budget.omissions):
  1224|                 line = f"- {o}"
  1225|                 if not budget.allow(line, 1):
  1226|                     omitted_omissions = len(budget.omissions) - idx
  1227|                     break
  1228|                 out.append(line)
  1229|                 budget.spend(line, 1)
  1230|             if omitted_omissions:
  1231|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
  1232|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1233|                         f"the complete list.")
  1234|                 if budget.allow(note, 1):
  1235|                     out.append(note)
  1236|                     budget.spend(note, 1)
  1237| 
  1238|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1239|     # resolution report" section built into the header below -- not
  1240|     # repeated here. header_lines/resolution_lines/footer were built and
  1241|     # charged against the budget up front, before Tier-1/Tier-2 rendering
  1242|     # -- see the "Reserve the fixed framing's budget cost up front"
  1243|     # comment above.)
  1244| 
  1245|     # Computed last so it reflects Tier-1/Tier-2's and the resolution
  1246|     # report's/footer's own budget spend too.
  1247|     header_lines.append(
  1248|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1249|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1250|     )
  1251|     header_lines.extend(resolution_lines)
  1252| 
  1253|     out.append(footer)
  1254| 
  1255|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1256| 
  1257|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1258|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1259|     atomic_write_text(packet_path, text)
  1260| 
  1261|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1262|     sidecar = {
  1263|         "tool_version": TOOL_VERSION,
  1264|         "schema_version": resolved.schema_version,
  1265|         "question": resolved.question,
  1266|         "request_file_sha256": request_hash,
  1267|         "git": git_info,
  1268|         "estimated_tokens_used": round(budget.chars_used / 4),
  1269|         "focus_files": focus_files,
  1270|         "omissions": budget.omissions,
  1271|         "resolution_report": resolution_report,
  1272|     }
  1273|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1274|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1275| 
  1276|     return packet_path, resolution_report, None
```
