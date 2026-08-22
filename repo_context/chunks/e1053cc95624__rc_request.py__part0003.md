# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 3
- Original line range: 716-1201
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, generate_packet_from_request._maybe_file_expansion, _res_to_dict
- Source SHA-256: 523991ef4ebd1a10bc2dd1b35f7956c36be078362fffaf1203e4cefd40337091
- Starts inside symbol: no
- Ends inside symbol: no

```
   716| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   717|                                   name_override: Optional[str] = None) -> tuple:
   718|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   719| 
   720|     error_message_or_None is set (and packet_path is None) for a
   721|     structurally invalid request, or when strict mode / an unresolvable
   722|     explicit-selector budget conflict blocks generation -- no partial
   723|     packet is written in either case.
   724|     """
   725|     try:
   726|         request_text = request_path.read_text(encoding="utf-8")
   727|     except OSError as exc:
   728|         return None, [], f"could not read request file {request_path}: {exc}"
   729| 
   730|     request_hash = sha256_text(request_text)
   731|     resolved, errors = parse_and_validate_request(request_text)
   732|     if errors:
   733|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   734| 
   735|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   736|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   737|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   738|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   739|     files_by_path = {r["relative_path"]: r for r in files_rows}
   740|     symbols_by_file: dict = {}
   741|     for r in symbols_rows:
   742|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   743| 
   744|     file_resolutions = resolve_files(resolved.files, files_by_path)
   745|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   746|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   747|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   748|         root, resolved.search_terms, resolved.search_as_regex, files_rows, resolved.max_files,
   749|     )
   750|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   751| 
   752|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   753|     if resolved.strict and unresolved_explicit:
   754|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   755|         return None, [_res_to_dict(r) for r in all_resolutions], (
   756|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   757|         )
   758| 
   759|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   760|     out: list = []
   761|     focus_files: list = []
   762| 
   763|     communities_by_file: dict = {}
   764|     if resolved.include_graphify:
   765|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   766|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   767|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   768|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   769|         )
   770|         for w in graphify_warnings_for_request:
   771|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   772| 
   773|     def note_focus_file(rel: str) -> bool:
   774|         if rel in focus_files:
   775|             return True
   776|         if len(focus_files) >= resolved.max_files:
   777|             return False
   778|         focus_files.append(rel)
   779|         return True
   780| 
   781|     # --- Tier 1: explicit selectors (never silently dropped) ---
   782|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   783|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   784|     # Freshness withholding and non-mandatory expansions (callers/callees/
   785|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   786|     # `budget` and must NOT abort generation.
   787|     #
   788|     # Two passes, deliberately in this order:
   789|     #   1. Render every explicit selector's own header/excerpt first (and
   790|     #      only once per distinct target -- two selectors naming the same
   791|     #      file/symbol/line render it a single time). None of this may be
   792|     #      pre-empted by expansion content.
   793|     #   2. Only once every explicit item has had its guaranteed shot at the
   794|     #      budget do optional expansions (callers/callees/imports/tests/
   795|     #      Graphify) spend whatever budget remains. Interleaving expansion
   796|     #      spend between explicit items (the previous structure) let one
   797|     #      selector's expansions manufacture a budget conflict for a later,
   798|     #      otherwise-fitting explicit selector.
   799|     explicit_conflicts: list = []
   800|     rendered_files: set = set()
   801|     rendered_symbols: set = set()
   802|     rendered_lines: set = set()
   803|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   804|     symbol_expansion_items: list = []  # [row]
   805| 
   806|     def _spend_header(header: str) -> bool:
   807|         if not budget.allow(header, 2):
   808|             return False
   809|         out.append(header)
   810|         budget.spend(header, 2)
   811|         return True
   812| 
   813|     for res in file_resolutions:
   814|         if res.status != "resolved":
   815|             continue
   816|         rel = res.requested
   817|         if rel in rendered_files:
   818|             # Duplicate selector for a file already attempted -- its first
   819|             # occurrence's outcome (rendered, or a recorded conflict) is
   820|             # final; re-attempting an identical selector would just repeat
   821|             # the same header/budget work (or the same conflict message)
   822|             # once per repeat, which is itself an unbounded-output shape
   823|             # for a request naming the same selector many times over.
   824|             continue
   825|         rendered_files.add(rel)
   826|         if not note_focus_file(rel):
   827|             explicit_conflicts.append(
   828|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   829|             )
   830|             continue
   831|         row = res.resolved_rows[0]
   832|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   833|         if not _spend_header(header):
   834|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   835|             continue
   836|         # Render the mandatory excerpt (the actual content the selector asked
   837|         # for) before the optional top-level-symbols inventory, so the
   838|         # inventory can never spend the shared budget ahead of the excerpt
   839|         # itself and force it into an "explicit_conflicts" abort.
   840|         try:
   841|             line_count = int(row.get("line_count") or 0)
   842|         except ValueError:
   843|             line_count = 0
   844|         if line_count:
   845|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   846|             if status == "too_large":
   847|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   848|                 continue
   849|         top_level = sorted(
   850|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   851|             key=lambda r: int(r["start_line"]),
   852|         )
   853|         # The "Top-level symbols:" inventory itself is optional metadata,
   854|         # same as this symbol's caller/callee/etc. expansions -- deferred
   855|         # to the pass below (after every explicit file/symbol/line
   856|         # selector has had its guaranteed shot at the budget), so file A's
   857|         # inventory can never spend budget that file B's own explicit
   858|         # excerpt needed.
   859|         file_expansion_items.append((rel, top_level))
   860| 
   861|     for res in symbol_resolutions:
   862|         if res.status != "resolved":
   863|             continue
   864|         row = res.resolved_rows[0]
   865|         rel = row["relative_path"]
   866|         symbol_key = (rel, row["qualified_name"])
   867|         if symbol_key in rendered_symbols:
   868|             continue  # duplicate selector; first occurrence's outcome is final
   869|         rendered_symbols.add(symbol_key)
   870|         if not note_focus_file(rel):
   871|             explicit_conflicts.append(
   872|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   873|                 f"limits.max_files ({resolved.max_files}) reached"
   874|             )
   875|             continue
   876|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   877|         if not _spend_header(header):
   878|             explicit_conflicts.append(
   879|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   880|             )
   881|             continue
   882|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   883|                                         files_by_path.get(rel, {}).get("sha256", ""))
   884|         if status == "too_large":
   885|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   886|         symbol_expansion_items.append(row)
   887| 
   888|     for res in line_resolutions:
   889|         if res.status != "resolved":
   890|             continue
   891|         info = res.resolved_rows[0]
   892|         rel = info["file"]
   893|         line_key = (rel, info["start"], info["end"])
   894|         if line_key in rendered_lines:
   895|             continue  # duplicate selector; first occurrence's outcome is final
   896|         rendered_lines.add(line_key)
   897|         if not note_focus_file(rel):
   898|             explicit_conflicts.append(
   899|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   900|             )
   901|             continue
   902|         # Only substitute a smaller enclosing symbol when it contains
   903|         # *both* endpoints of the requested range -- a symbol containing
   904|         # just the start line (the old check) could be smaller than the
   905|         # actual request (e.g. lines 2-7 where line 2's enclosing function
   906|         # ends at line 2), silently truncating the rendered excerpt to
   907|         # that symbol's own bounds while the resolution report still
   908|         # claimed the full range was resolved.
   909|         enclosing = [
   910|             r for r in symbols_by_file.get(rel, [])
   911|             if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
   912|             and r["symbol_type"] != "module"
   913|         ]
   914|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
   915|         if not _spend_header(header):
   916|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
   917|             continue
   918|         if enclosing:
   919|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
   920|             row = enclosing[0]
   921|             enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
   922|                                f"lines {row['start_line']}-{row['end_line']})\n")
   923|             # Metadata, not the mandatory excerpt itself -- budgeted like
   924|             # every other optional line, with a non-fatal skip if it
   925|             # doesn't fit rather than silently rendering it unbudgeted.
   926|             if budget.allow(enclosing_line, 1):
   927|                 out.append(enclosing_line)
   928|                 budget.spend(enclosing_line, 1)
   929|             else:
   930|                 budget.omissions.append(
   931|                     f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
   932|                 )
   933|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   934|                                             files_by_path.get(rel, {}).get("sha256", ""))
   935|         else:
   936|             start, end = max(1, info["start"] - 10), info["end"] + 10
   937|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
   938|         if status == "too_large":
   939|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
   940| 
   941|     if explicit_conflicts:
   942|         # An explicit selection itself didn't fit -- either the token
   943|         # budget or limits.max_files -- per contract this is a hard
   944|         # conflict, not something to truncate silently.
   945|         return None, [_res_to_dict(r) for r in all_resolutions], (
   946|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
   947|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
   948|             f"relevant limit or narrow the request. Conflicts:\n"
   949|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
   950|         )
   951| 
   952|     # Every explicit selector fit -- now spend whatever budget remains on
   953|     # optional metadata/expansions (callers/callees/imports/tests/
   954|     # Graphify, plus each explicit file selector's own "Top-level
   955|     # symbols:" inventory). Done only now, not interleaved with tier-1's
   956|     # rendering above, so a symbol's expansions -- or one file's inventory
   957|     # -- can never manufacture a budget conflict for a later explicit
   958|     # selector.
   959|     for rel, top_level in file_expansion_items:
   960|         if top_level:
   961|             header = "Top-level symbols:"
   962|             if budget.allow(header, 1):
   963|                 out.append(header)
   964|                 budget.spend(header, 1)
   965|                 for idx, r in enumerate(top_level):
   966|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
   967|                     if not budget.allow(line, 1):
   968|                         budget.omissions.append(
   969|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
   970|                             f"listing (packet size limit reached); see python_symbols.csv."
   971|                         )
   972|                         break
   973|                     out.append(line)
   974|                     budget.spend(line, 1)
   975|             else:
   976|                 budget.omissions.append(
   977|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
   978|                     f"see python_symbols.csv."
   979|                 )
   980|     file_level_expansion_done: set = set()
   981| 
   982|     def _maybe_file_expansion(rel: str) -> None:
   983|         # Once per file regardless of how many symbols in it get
   984|         # expanded, and regardless of whether that file is reached via a
   985|         # file selector's own top-level symbols or a distinct explicit
   986|         # symbol selector in the same file.
   987|         if rel in file_level_expansion_done:
   988|             return
   989|         file_level_expansion_done.add(rel)
   990|         _file_expansion(rel, imports_rows, calls_rows, files_by_path, resolved, budget, out, note_focus_file,
   991|                          communities_by_file)
   992| 
   993|     for rel, top_level in file_expansion_items:
   994|         if not top_level:
   995|             # Matches the prior trigger condition -- a file with no
   996|             # top-level symbols never had its expansions rendered either.
   997|             continue
   998|         for r in top_level:
   999|             _symbol_expansion(r, calls_rows, resolved, budget, out, note_focus_file)
  1000|         _maybe_file_expansion(rel)
  1001|     for row in symbol_expansion_items:
  1002|         _symbol_expansion(row, calls_rows, resolved, budget, out, note_focus_file)
  1003|         _maybe_file_expansion(row["relative_path"])
  1004| 
  1005|     # --- Tier 2: exact search-term matches ---
  1006|     # Matches were already computed by resolve_search_terms() above (before
  1007|     # the strict-mode gate) -- reused here rather than re-scanning the
  1008|     # repository a second time.
  1009|     for term in resolved.search_terms:
  1010|         matches = search_matches_by_term.get(term, [])
  1011|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
  1012|         if term_status == "invalid":
  1013|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
  1014|             if not budget.allow(notice, 1):
  1015|                 # Unbounded per-term notices (e.g. a request with hundreds
  1016|                 # of invalid regex terms) would otherwise bypass
  1017|                 # limits.max_estimated_tokens entirely, same failure shape
  1018|                 # as the earlier unbudgeted resolution-report finding.
  1019|                 budget.omissions.append(
  1020|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
  1021|                     f"see the resolution report."
  1022|                 )
  1023|                 continue
  1024|             out.append(notice)
  1025|             budget.spend(notice, 1)
  1026|             continue
  1027|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
  1028|         if not budget.allow(header, 1):
  1029|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
  1030|             continue
  1031|         out.append(header); budget.spend(header, 1)
  1032|         max_files_note_emitted = False
  1033|         for rel, ln, text in matches:
  1034|             # Redact the full line before truncating it, not after -- a
  1035|             # secret-shaped value whose closing quote falls beyond
  1036|             # character 200 would otherwise have that quote cut off
  1037|             # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
  1038|             # backreference so redact_secrets() never matches and the
  1039|             # (truncated) secret prefix leaks into the packet.
  1040|             line_text = redact_secrets(text.strip())[:200]
  1041|             line = f"- `{rel}:{ln}` — `{line_text}`"
  1042|             # Check the budget *before* reserving a focus-file slot for
  1043|             # this match -- otherwise a match that ultimately doesn't fit
  1044|             # (and is never rendered) could still consume the one
  1045|             # remaining limits.max_files slot, starving a later, shorter
  1046|             # match from a different file that would have fit.
  1047|             if not budget.allow(line, 1):
  1048|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
  1049|                 break
  1050|             if not note_focus_file(rel):
  1051|                 # note_focus_file enforces limits.max_files against the
  1052|                 # *global* focus-file set shared across every selector/tier
  1053|                 # in this packet, not just this one search term -- so the
  1054|                 # cap holds even when different terms match different files.
  1055|                 # A match in a file beyond the cap doesn't mean every
  1056|                 # *later* match is unreachable too -- a later match may be
  1057|                 # in a file already in focus_files (e.g. the selected
  1058|                 # file), which costs no new slot. Skip this one match and
  1059|                 # keep checking the rest instead of abandoning the term.
  1060|                 if not max_files_note_emitted:
  1061|                     budget.omissions.append(
  1062|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
  1063|                     )
  1064|                     max_files_note_emitted = True
  1065|                 continue
  1066|             out.append(line); budget.spend(line, 1)
  1067|     if stale_search_files:
  1068|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
  1069|                                  f"skipped for search terms; re-run scan.")
  1070| 
  1071|     if budget.omissions:
  1072|         # The omissions *list* itself is unbounded in memory (a request
  1073|         # that triggers hundreds of distinct omission reasons -- e.g. many
  1074|         # invalid regex search terms -- can produce hundreds of entries),
  1075|         # so rendering it must be budgeted the same way the resolution
  1076|         # report is: otherwise this section alone could bypass
  1077|         # limits.max_estimated_tokens. The full list is always available,
  1078|         # unbudgeted, in the packet_*.resolution.json sidecar's
  1079|         # "omissions" field.
  1080|         header = "\n## Omitted / unresolved\n"
  1081|         if budget.allow(header, 1):
  1082|             out.append(header)
  1083|             budget.spend(header, 1)
  1084|             omitted_omissions = 0
  1085|             for idx, o in enumerate(budget.omissions):
  1086|                 line = f"- {o}"
  1087|                 if not budget.allow(line, 1):
  1088|                     omitted_omissions = len(budget.omissions) - idx
  1089|                     break
  1090|                 out.append(line)
  1091|                 budget.spend(line, 1)
  1092|             if omitted_omissions:
  1093|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
  1094|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1095|                         f"the complete list.")
  1096|                 if budget.allow(note, 1):
  1097|                     out.append(note)
  1098|                     budget.spend(note, 1)
  1099| 
  1100|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1101|     # resolution report" section built into the header below -- not
  1102|     # repeated here.)
  1103| 
  1104|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
  1105|     header_lines = [
  1106|         "# Repo Context Packet (from packet_request.json)\n",
  1107|         f"- Root: `{root.resolve().name}`",
  1108|         f"- Question: {resolved.question}",
  1109|         f"- schema_version: {resolved.schema_version}",
  1110|         f"- Tool version: {TOOL_VERSION}",
  1111|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
  1112|     ]
  1113|     if git_info.get("available"):
  1114|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
  1115|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
  1116|     else:
  1117|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
  1118|     header_lines.append(
  1119|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
  1120|         f"max_hops={resolved.max_hops}"
  1121|     )
  1122|     # This fixed framing (title/root/question/provenance/limits) always
  1123|     # renders in full -- it's essential provenance, not optional content
  1124|     # -- but it must still be *charged* against the budget so "Estimated
  1125|     # tokens used" reflects the packet's true size. A `question` up to
  1126|     # MAX_QUESTION_LENGTH (4000 chars) alone could otherwise make the
  1127|     # actual packet many times larger than limits.max_estimated_tokens
  1128|     # while the sidecar reported a number near zero.
  1129|     budget.spend("\n".join(header_lines) + "\n", len(header_lines))
  1130| 
  1131|     # The resolution report scales with the *request*, not the source
  1132|     # repository (a request naming hundreds of missing/ambiguous
  1133|     # selectors could otherwise render an unbounded report regardless of
  1134|     # limits.max_estimated_tokens) -- charge it against the same budget
  1135|     # as everything else, with a count-of-omitted note rather than an
  1136|     # unbounded listing. The full, untruncated report is always available
  1137|     # in the accompanying packet_<name>.resolution.json sidecar.
  1138|     resolution_lines = ["## Selector resolution report\n"]
  1139|     omitted_selector_count = 0
  1140|     for idx, r in enumerate(all_resolutions):
  1141|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
  1142|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
  1143|         entry_text = "\n".join(entry)
  1144|         if not budget.allow(entry_text, len(entry)):
  1145|             omitted_selector_count = len(all_resolutions) - idx
  1146|             break
  1147|         resolution_lines.extend(entry)
  1148|         budget.spend(entry_text, len(entry))
  1149|     if omitted_selector_count:
  1150|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
  1151|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
  1152|         if budget.allow(note, 1):
  1153|             resolution_lines.append(note)
  1154|             budget.spend(note, 1)
  1155|     resolution_lines.append("")
  1156| 
  1157|     # Same fixed-framing reasoning as the header above -- always rendered
  1158|     # in full, but charged first so it's reflected in "Estimated tokens
  1159|     # used" below rather than silently riding along uncounted.
  1160|     footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
  1161|               "dispatch. See README.md in this output directory for full limitations._\n")
  1162|     budget.spend(footer, 1)
  1163| 
  1164|     # Computed last so it reflects the resolution report's and footer's own budget spend too.
  1165|     header_lines.append(
  1166|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1167|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1168|     )
  1169|     header_lines.extend(resolution_lines)
  1170| 
  1171|     out.append(footer)
  1172| 
  1173|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1174| 
  1175|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1176|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1177|     atomic_write_text(packet_path, text)
  1178| 
  1179|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1180|     sidecar = {
  1181|         "tool_version": TOOL_VERSION,
  1182|         "schema_version": resolved.schema_version,
  1183|         "question": resolved.question,
  1184|         "request_file_sha256": request_hash,
  1185|         "git": git_info,
  1186|         "estimated_tokens_used": round(budget.chars_used / 4),
  1187|         "focus_files": focus_files,
  1188|         "omissions": budget.omissions,
  1189|         "resolution_report": resolution_report,
  1190|     }
  1191|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1192|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1193| 
  1194|     return packet_path, resolution_report, None
  1195| 
  1196| 
  1197| def _res_to_dict(r: SelectorResolution) -> dict:
  1198|     return {
  1199|         "selector_type": r.selector_type, "requested": r.requested,
  1200|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1201|     }
```
