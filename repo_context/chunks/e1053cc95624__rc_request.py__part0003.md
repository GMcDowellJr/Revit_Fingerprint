# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 3
- Original line range: 699-1161
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, _res_to_dict
- Source SHA-256: 50a7a8ece86c108ece56ed514c39c4b261ea08176f2190cb9648cfeef747ed42
- Starts inside symbol: no
- Ends inside symbol: no

```
   699| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   700|                                   name_override: Optional[str] = None) -> tuple:
   701|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   702| 
   703|     error_message_or_None is set (and packet_path is None) for a
   704|     structurally invalid request, or when strict mode / an unresolvable
   705|     explicit-selector budget conflict blocks generation -- no partial
   706|     packet is written in either case.
   707|     """
   708|     try:
   709|         request_text = request_path.read_text(encoding="utf-8")
   710|     except OSError as exc:
   711|         return None, [], f"could not read request file {request_path}: {exc}"
   712| 
   713|     request_hash = sha256_text(request_text)
   714|     resolved, errors = parse_and_validate_request(request_text)
   715|     if errors:
   716|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   717| 
   718|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   719|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   720|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   721|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   722|     files_by_path = {r["relative_path"]: r for r in files_rows}
   723|     symbols_by_file: dict = {}
   724|     for r in symbols_rows:
   725|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   726| 
   727|     file_resolutions = resolve_files(resolved.files, files_by_path)
   728|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   729|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   730|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   731|         root, resolved.search_terms, resolved.search_as_regex, files_rows, resolved.max_files,
   732|     )
   733|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   734| 
   735|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   736|     if resolved.strict and unresolved_explicit:
   737|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   738|         return None, [_res_to_dict(r) for r in all_resolutions], (
   739|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   740|         )
   741| 
   742|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   743|     out: list = []
   744|     focus_files: list = []
   745| 
   746|     communities_by_file: dict = {}
   747|     if resolved.include_graphify:
   748|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   749|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   750|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   751|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   752|         )
   753|         for w in graphify_warnings_for_request:
   754|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   755| 
   756|     def note_focus_file(rel: str) -> bool:
   757|         if rel in focus_files:
   758|             return True
   759|         if len(focus_files) >= resolved.max_files:
   760|             return False
   761|         focus_files.append(rel)
   762|         return True
   763| 
   764|     # --- Tier 1: explicit selectors (never silently dropped) ---
   765|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   766|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   767|     # Freshness withholding and non-mandatory expansions (callers/callees/
   768|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   769|     # `budget` and must NOT abort generation.
   770|     #
   771|     # Two passes, deliberately in this order:
   772|     #   1. Render every explicit selector's own header/excerpt first (and
   773|     #      only once per distinct target -- two selectors naming the same
   774|     #      file/symbol/line render it a single time). None of this may be
   775|     #      pre-empted by expansion content.
   776|     #   2. Only once every explicit item has had its guaranteed shot at the
   777|     #      budget do optional expansions (callers/callees/imports/tests/
   778|     #      Graphify) spend whatever budget remains. Interleaving expansion
   779|     #      spend between explicit items (the previous structure) let one
   780|     #      selector's expansions manufacture a budget conflict for a later,
   781|     #      otherwise-fitting explicit selector.
   782|     explicit_conflicts: list = []
   783|     rendered_files: set = set()
   784|     rendered_symbols: set = set()
   785|     rendered_lines: set = set()
   786|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   787|     symbol_expansion_items: list = []  # [row]
   788| 
   789|     def _spend_header(header: str) -> bool:
   790|         if not budget.allow(header, 2):
   791|             return False
   792|         out.append(header)
   793|         budget.spend(header, 2)
   794|         return True
   795| 
   796|     for res in file_resolutions:
   797|         if res.status != "resolved":
   798|             continue
   799|         rel = res.requested
   800|         if rel in rendered_files:
   801|             # Duplicate selector for a file already attempted -- its first
   802|             # occurrence's outcome (rendered, or a recorded conflict) is
   803|             # final; re-attempting an identical selector would just repeat
   804|             # the same header/budget work (or the same conflict message)
   805|             # once per repeat, which is itself an unbounded-output shape
   806|             # for a request naming the same selector many times over.
   807|             continue
   808|         rendered_files.add(rel)
   809|         if not note_focus_file(rel):
   810|             explicit_conflicts.append(
   811|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   812|             )
   813|             continue
   814|         row = res.resolved_rows[0]
   815|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   816|         if not _spend_header(header):
   817|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   818|             continue
   819|         # Render the mandatory excerpt (the actual content the selector asked
   820|         # for) before the optional top-level-symbols inventory, so the
   821|         # inventory can never spend the shared budget ahead of the excerpt
   822|         # itself and force it into an "explicit_conflicts" abort.
   823|         try:
   824|             line_count = int(row.get("line_count") or 0)
   825|         except ValueError:
   826|             line_count = 0
   827|         if line_count:
   828|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   829|             if status == "too_large":
   830|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   831|                 continue
   832|         top_level = sorted(
   833|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   834|             key=lambda r: int(r["start_line"]),
   835|         )
   836|         # The "Top-level symbols:" inventory itself is optional metadata,
   837|         # same as this symbol's caller/callee/etc. expansions -- deferred
   838|         # to the pass below (after every explicit file/symbol/line
   839|         # selector has had its guaranteed shot at the budget), so file A's
   840|         # inventory can never spend budget that file B's own explicit
   841|         # excerpt needed.
   842|         file_expansion_items.append((rel, top_level))
   843| 
   844|     for res in symbol_resolutions:
   845|         if res.status != "resolved":
   846|             continue
   847|         row = res.resolved_rows[0]
   848|         rel = row["relative_path"]
   849|         symbol_key = (rel, row["qualified_name"])
   850|         if symbol_key in rendered_symbols:
   851|             continue  # duplicate selector; first occurrence's outcome is final
   852|         rendered_symbols.add(symbol_key)
   853|         if not note_focus_file(rel):
   854|             explicit_conflicts.append(
   855|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   856|                 f"limits.max_files ({resolved.max_files}) reached"
   857|             )
   858|             continue
   859|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   860|         if not _spend_header(header):
   861|             explicit_conflicts.append(
   862|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   863|             )
   864|             continue
   865|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   866|                                         files_by_path.get(rel, {}).get("sha256", ""))
   867|         if status == "too_large":
   868|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   869|         symbol_expansion_items.append(row)
   870| 
   871|     for res in line_resolutions:
   872|         if res.status != "resolved":
   873|             continue
   874|         info = res.resolved_rows[0]
   875|         rel = info["file"]
   876|         line_key = (rel, info["start"], info["end"])
   877|         if line_key in rendered_lines:
   878|             continue  # duplicate selector; first occurrence's outcome is final
   879|         rendered_lines.add(line_key)
   880|         if not note_focus_file(rel):
   881|             explicit_conflicts.append(
   882|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   883|             )
   884|             continue
   885|         # Only substitute a smaller enclosing symbol when it contains
   886|         # *both* endpoints of the requested range -- a symbol containing
   887|         # just the start line (the old check) could be smaller than the
   888|         # actual request (e.g. lines 2-7 where line 2's enclosing function
   889|         # ends at line 2), silently truncating the rendered excerpt to
   890|         # that symbol's own bounds while the resolution report still
   891|         # claimed the full range was resolved.
   892|         enclosing = [
   893|             r for r in symbols_by_file.get(rel, [])
   894|             if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
   895|             and r["symbol_type"] != "module"
   896|         ]
   897|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
   898|         if not _spend_header(header):
   899|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
   900|             continue
   901|         if enclosing:
   902|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
   903|             row = enclosing[0]
   904|             enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
   905|                                f"lines {row['start_line']}-{row['end_line']})\n")
   906|             # Metadata, not the mandatory excerpt itself -- budgeted like
   907|             # every other optional line, with a non-fatal skip if it
   908|             # doesn't fit rather than silently rendering it unbudgeted.
   909|             if budget.allow(enclosing_line, 1):
   910|                 out.append(enclosing_line)
   911|                 budget.spend(enclosing_line, 1)
   912|             else:
   913|                 budget.omissions.append(
   914|                     f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
   915|                 )
   916|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   917|                                             files_by_path.get(rel, {}).get("sha256", ""))
   918|         else:
   919|             start, end = max(1, info["start"] - 10), info["end"] + 10
   920|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
   921|         if status == "too_large":
   922|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
   923| 
   924|     if explicit_conflicts:
   925|         # An explicit selection itself didn't fit -- either the token
   926|         # budget or limits.max_files -- per contract this is a hard
   927|         # conflict, not something to truncate silently.
   928|         return None, [_res_to_dict(r) for r in all_resolutions], (
   929|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
   930|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
   931|             f"relevant limit or narrow the request. Conflicts:\n"
   932|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
   933|         )
   934| 
   935|     # Every explicit selector fit -- now spend whatever budget remains on
   936|     # optional metadata/expansions (callers/callees/imports/tests/
   937|     # Graphify, plus each explicit file selector's own "Top-level
   938|     # symbols:" inventory). Done only now, not interleaved with tier-1's
   939|     # rendering above, so a symbol's expansions -- or one file's inventory
   940|     # -- can never manufacture a budget conflict for a later explicit
   941|     # selector.
   942|     for rel, top_level in file_expansion_items:
   943|         if top_level:
   944|             header = "Top-level symbols:"
   945|             if budget.allow(header, 1):
   946|                 out.append(header)
   947|                 budget.spend(header, 1)
   948|                 for idx, r in enumerate(top_level):
   949|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
   950|                     if not budget.allow(line, 1):
   951|                         budget.omissions.append(
   952|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
   953|                             f"listing (packet size limit reached); see python_symbols.csv."
   954|                         )
   955|                         break
   956|                     out.append(line)
   957|                     budget.spend(line, 1)
   958|             else:
   959|                 budget.omissions.append(
   960|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
   961|                     f"see python_symbols.csv."
   962|                 )
   963|     for rel, top_level in file_expansion_items:
   964|         for r in top_level:
   965|             _symbol_expansion(r, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   966|                               communities_by_file)
   967|     for row in symbol_expansion_items:
   968|         _symbol_expansion(row, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   969|                           communities_by_file)
   970| 
   971|     # --- Tier 2: exact search-term matches ---
   972|     # Matches were already computed by resolve_search_terms() above (before
   973|     # the strict-mode gate) -- reused here rather than re-scanning the
   974|     # repository a second time.
   975|     for term in resolved.search_terms:
   976|         matches = search_matches_by_term.get(term, [])
   977|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
   978|         if term_status == "invalid":
   979|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
   980|             if not budget.allow(notice, 1):
   981|                 # Unbounded per-term notices (e.g. a request with hundreds
   982|                 # of invalid regex terms) would otherwise bypass
   983|                 # limits.max_estimated_tokens entirely, same failure shape
   984|                 # as the earlier unbudgeted resolution-report finding.
   985|                 budget.omissions.append(
   986|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
   987|                     f"see the resolution report."
   988|                 )
   989|                 continue
   990|             out.append(notice)
   991|             budget.spend(notice, 1)
   992|             continue
   993|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
   994|         if not budget.allow(header, 1):
   995|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
   996|             continue
   997|         out.append(header); budget.spend(header, 1)
   998|         max_files_note_emitted = False
   999|         for rel, ln, text in matches:
  1000|             line_text = redact_secrets(text.strip()[:200])
  1001|             line = f"- `{rel}:{ln}` — `{line_text}`"
  1002|             # Check the budget *before* reserving a focus-file slot for
  1003|             # this match -- otherwise a match that ultimately doesn't fit
  1004|             # (and is never rendered) could still consume the one
  1005|             # remaining limits.max_files slot, starving a later, shorter
  1006|             # match from a different file that would have fit.
  1007|             if not budget.allow(line, 1):
  1008|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
  1009|                 break
  1010|             if not note_focus_file(rel):
  1011|                 # note_focus_file enforces limits.max_files against the
  1012|                 # *global* focus-file set shared across every selector/tier
  1013|                 # in this packet, not just this one search term -- so the
  1014|                 # cap holds even when different terms match different files.
  1015|                 # A match in a file beyond the cap doesn't mean every
  1016|                 # *later* match is unreachable too -- a later match may be
  1017|                 # in a file already in focus_files (e.g. the selected
  1018|                 # file), which costs no new slot. Skip this one match and
  1019|                 # keep checking the rest instead of abandoning the term.
  1020|                 if not max_files_note_emitted:
  1021|                     budget.omissions.append(
  1022|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
  1023|                     )
  1024|                     max_files_note_emitted = True
  1025|                 continue
  1026|             out.append(line); budget.spend(line, 1)
  1027|     if stale_search_files:
  1028|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
  1029|                                  f"skipped for search terms; re-run scan.")
  1030| 
  1031|     if budget.omissions:
  1032|         # The omissions *list* itself is unbounded in memory (a request
  1033|         # that triggers hundreds of distinct omission reasons -- e.g. many
  1034|         # invalid regex search terms -- can produce hundreds of entries),
  1035|         # so rendering it must be budgeted the same way the resolution
  1036|         # report is: otherwise this section alone could bypass
  1037|         # limits.max_estimated_tokens. The full list is always available,
  1038|         # unbudgeted, in the packet_*.resolution.json sidecar's
  1039|         # "omissions" field.
  1040|         header = "\n## Omitted / unresolved\n"
  1041|         if budget.allow(header, 1):
  1042|             out.append(header)
  1043|             budget.spend(header, 1)
  1044|             omitted_omissions = 0
  1045|             for idx, o in enumerate(budget.omissions):
  1046|                 line = f"- {o}"
  1047|                 if not budget.allow(line, 1):
  1048|                     omitted_omissions = len(budget.omissions) - idx
  1049|                     break
  1050|                 out.append(line)
  1051|                 budget.spend(line, 1)
  1052|             if omitted_omissions:
  1053|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
  1054|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1055|                         f"the complete list.")
  1056|                 if budget.allow(note, 1):
  1057|                     out.append(note)
  1058|                     budget.spend(note, 1)
  1059| 
  1060|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1061|     # resolution report" section built into the header below -- not
  1062|     # repeated here.)
  1063| 
  1064|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
  1065|     header_lines = [
  1066|         "# Repo Context Packet (from packet_request.json)\n",
  1067|         f"- Root: `{root.resolve().name}`",
  1068|         f"- Question: {resolved.question}",
  1069|         f"- schema_version: {resolved.schema_version}",
  1070|         f"- Tool version: {TOOL_VERSION}",
  1071|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
  1072|     ]
  1073|     if git_info.get("available"):
  1074|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
  1075|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
  1076|     else:
  1077|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
  1078|     header_lines.append(
  1079|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
  1080|         f"max_hops={resolved.max_hops}"
  1081|     )
  1082|     # This fixed framing (title/root/question/provenance/limits) always
  1083|     # renders in full -- it's essential provenance, not optional content
  1084|     # -- but it must still be *charged* against the budget so "Estimated
  1085|     # tokens used" reflects the packet's true size. A `question` up to
  1086|     # MAX_QUESTION_LENGTH (4000 chars) alone could otherwise make the
  1087|     # actual packet many times larger than limits.max_estimated_tokens
  1088|     # while the sidecar reported a number near zero.
  1089|     budget.spend("\n".join(header_lines) + "\n", len(header_lines))
  1090| 
  1091|     # The resolution report scales with the *request*, not the source
  1092|     # repository (a request naming hundreds of missing/ambiguous
  1093|     # selectors could otherwise render an unbounded report regardless of
  1094|     # limits.max_estimated_tokens) -- charge it against the same budget
  1095|     # as everything else, with a count-of-omitted note rather than an
  1096|     # unbounded listing. The full, untruncated report is always available
  1097|     # in the accompanying packet_<name>.resolution.json sidecar.
  1098|     resolution_lines = ["## Selector resolution report\n"]
  1099|     omitted_selector_count = 0
  1100|     for idx, r in enumerate(all_resolutions):
  1101|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
  1102|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
  1103|         entry_text = "\n".join(entry)
  1104|         if not budget.allow(entry_text, len(entry)):
  1105|             omitted_selector_count = len(all_resolutions) - idx
  1106|             break
  1107|         resolution_lines.extend(entry)
  1108|         budget.spend(entry_text, len(entry))
  1109|     if omitted_selector_count:
  1110|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
  1111|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
  1112|         if budget.allow(note, 1):
  1113|             resolution_lines.append(note)
  1114|             budget.spend(note, 1)
  1115|     resolution_lines.append("")
  1116| 
  1117|     # Same fixed-framing reasoning as the header above -- always rendered
  1118|     # in full, but charged first so it's reflected in "Estimated tokens
  1119|     # used" below rather than silently riding along uncounted.
  1120|     footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
  1121|               "dispatch. See README.md in this output directory for full limitations._\n")
  1122|     budget.spend(footer, 1)
  1123| 
  1124|     # Computed last so it reflects the resolution report's and footer's own budget spend too.
  1125|     header_lines.append(
  1126|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1127|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1128|     )
  1129|     header_lines.extend(resolution_lines)
  1130| 
  1131|     out.append(footer)
  1132| 
  1133|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1134| 
  1135|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1136|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1137|     atomic_write_text(packet_path, text)
  1138| 
  1139|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1140|     sidecar = {
  1141|         "tool_version": TOOL_VERSION,
  1142|         "schema_version": resolved.schema_version,
  1143|         "question": resolved.question,
  1144|         "request_file_sha256": request_hash,
  1145|         "git": git_info,
  1146|         "estimated_tokens_used": round(budget.chars_used / 4),
  1147|         "focus_files": focus_files,
  1148|         "omissions": budget.omissions,
  1149|         "resolution_report": resolution_report,
  1150|     }
  1151|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1152|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1153| 
  1154|     return packet_path, resolution_report, None
  1155| 
  1156| 
  1157| def _res_to_dict(r: SelectorResolution) -> dict:
  1158|     return {
  1159|         "selector_type": r.selector_type, "requested": r.requested,
  1160|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1161|     }
```
