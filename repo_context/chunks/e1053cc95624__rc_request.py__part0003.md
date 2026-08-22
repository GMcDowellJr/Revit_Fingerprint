# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 4
- Original line range: 716-1234
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, generate_packet_from_request._maybe_file_expansion
- Source SHA-256: 82e6de1cc1d8a6782ab71ce65e4e91a7f422b049d64883de0a14e381c517b7c3
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
   781|     # --- Reserve the fixed framing's budget cost up front ---
   782|     # The header, the selector-resolution report, and the footer are
   783|     # essential, always-rendered provenance -- not optional content -- so
   784|     # none of them can be dropped to make room. But charging their cost
   785|     # only after Tier-1/Tier-2 content had already spent against the full,
   786|     # unreserved budget (as a prior version did) let Tier-1/Tier-2 content
   787|     # spend as if framing were free: a request could "succeed" with
   788|     # Tier-1 content that, combined with the framing charged on top
   789|     # afterward, made the packet's *actual* rendered size exceed
   790|     # limits.max_estimated_tokens even though generation reported success.
   791|     # Reserving framing's cost first means a too-tight budget now
   792|     # correctly surfaces as an explicit_conflicts abort (an explicit
   793|     # selector's excerpt no longer fits once framing's real cost is
   794|     # subtracted) instead of a "successful" packet whose true size is
   795|     # larger than what was requested.
   796|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   797|     header_lines = [
   798|         "# Repo Context Packet (from packet_request.json)\n",
   799|         f"- Root: `{root.resolve().name}`",
   800|         f"- Question: {resolved.question}",
   801|         f"- schema_version: {resolved.schema_version}",
   802|         f"- Tool version: {TOOL_VERSION}",
   803|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
   804|     ]
   805|     if git_info.get("available"):
   806|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
   807|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
   808|     else:
   809|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
   810|     header_lines.append(
   811|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
   812|         f"max_hops={resolved.max_hops}"
   813|     )
   814|     header_text = "\n".join(header_lines) + "\n"
   815| 
   816|     # Same fixed-framing reasoning as the header -- always rendered in
   817|     # full, reserved together with it (see below) so nothing else can
   818|     # spend against budget the footer will also need.
   819|     footer = ("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
   820|               "dispatch. See README.md in this output directory for full limitations._\n")
   821| 
   822|     # Header and footer must be reserved *together*, in one atomic check,
   823|     # before anything else (including the selector-resolution report
   824|     # below) is allowed to spend -- reserving the header alone first (an
   825|     # earlier version of this fix) still let a resolution-report entry's
   826|     # own budget.allow() check pass against a budget that hadn't yet
   827|     # accounted for the footer, so the footer's later unconditional spend
   828|     # pushed the total over the cap anyway. Both are mandatory and
   829|     # unshrinkable, so if they don't fit *together* in the requested
   830|     # budget, no amount of Tier-1/Tier-2 selector content could ever have
   831|     # fit either -- fail the request outright instead of writing a packet
   832|     # whose true size exceeds what was asked for.
   833|     framing_text = header_text + footer
   834|     if not budget.allow(framing_text, len(header_lines) + 1):
   835|         return None, [_res_to_dict(r) for r in all_resolutions], (
   836|             f"limits.max_estimated_tokens ({resolved.max_estimated_tokens}) is too small to fit this packet's "
   837|             f"fixed framing (header + footer, before any selector content or the selector-resolution report) "
   838|             f"alone; increase limits.max_estimated_tokens."
   839|         )
   840|     budget.spend(framing_text, len(header_lines) + 1)
   841| 
   842|     # The resolution report scales with the *request*, not the source
   843|     # repository (a request naming hundreds of missing/ambiguous
   844|     # selectors could otherwise render an unbounded report regardless of
   845|     # limits.max_estimated_tokens) -- charge it against the same budget as
   846|     # everything else, with a count-of-omitted note rather than an
   847|     # unbounded listing. The full, untruncated report is always available
   848|     # in the accompanying packet_<name>.resolution.json sidecar. Computed
   849|     # here (reserved up front, alongside the header/footer) rather than
   850|     # after Tier-1/Tier-2 render, since it depends only on
   851|     # `all_resolutions` (already resolved above), not on anything
   852|     # Tier-1/Tier-2 produce.
   853|     resolution_lines = ["## Selector resolution report\n"]
   854|     omitted_selector_count = 0
   855|     for idx, r in enumerate(all_resolutions):
   856|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
   857|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
   858|         entry_text = "\n".join(entry)
   859|         if not budget.allow(entry_text, len(entry)):
   860|             omitted_selector_count = len(all_resolutions) - idx
   861|             break
   862|         resolution_lines.extend(entry)
   863|         budget.spend(entry_text, len(entry))
   864|     if omitted_selector_count:
   865|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
   866|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
   867|         if budget.allow(note, 1):
   868|             resolution_lines.append(note)
   869|             budget.spend(note, 1)
   870|     resolution_lines.append("")
   871| 
   872|     # --- Tier 1: explicit selectors (never silently dropped) ---
   873|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   874|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   875|     # Freshness withholding and non-mandatory expansions (callers/callees/
   876|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   877|     # `budget` and must NOT abort generation.
   878|     #
   879|     # Two passes, deliberately in this order:
   880|     #   1. Render every explicit selector's own header/excerpt first (and
   881|     #      only once per distinct target -- two selectors naming the same
   882|     #      file/symbol/line render it a single time). None of this may be
   883|     #      pre-empted by expansion content.
   884|     #   2. Only once every explicit item has had its guaranteed shot at the
   885|     #      budget do optional expansions (callers/callees/imports/tests/
   886|     #      Graphify) spend whatever budget remains. Interleaving expansion
   887|     #      spend between explicit items (the previous structure) let one
   888|     #      selector's expansions manufacture a budget conflict for a later,
   889|     #      otherwise-fitting explicit selector.
   890|     explicit_conflicts: list = []
   891|     rendered_files: set = set()
   892|     rendered_symbols: set = set()
   893|     rendered_lines: set = set()
   894|     file_expansion_items: list = []    # [(rel, top_level_rows)]
   895|     symbol_expansion_items: list = []  # [row]
   896| 
   897|     def _spend_header(header: str) -> bool:
   898|         if not budget.allow(header, 2):
   899|             return False
   900|         out.append(header)
   901|         budget.spend(header, 2)
   902|         return True
   903| 
   904|     for res in file_resolutions:
   905|         if res.status != "resolved":
   906|             continue
   907|         rel = res.requested
   908|         if rel in rendered_files:
   909|             # Duplicate selector for a file already attempted -- its first
   910|             # occurrence's outcome (rendered, or a recorded conflict) is
   911|             # final; re-attempting an identical selector would just repeat
   912|             # the same header/budget work (or the same conflict message)
   913|             # once per repeat, which is itself an unbounded-output shape
   914|             # for a request naming the same selector many times over.
   915|             continue
   916|         rendered_files.add(rel)
   917|         if not note_focus_file(rel):
   918|             explicit_conflicts.append(
   919|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   920|             )
   921|             continue
   922|         row = res.resolved_rows[0]
   923|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   924|         if not _spend_header(header):
   925|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   926|             continue
   927|         # Render the mandatory excerpt (the actual content the selector asked
   928|         # for) before the optional top-level-symbols inventory, so the
   929|         # inventory can never spend the shared budget ahead of the excerpt
   930|         # itself and force it into an "explicit_conflicts" abort.
   931|         try:
   932|             line_count = int(row.get("line_count") or 0)
   933|         except ValueError:
   934|             line_count = 0
   935|         if line_count:
   936|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   937|             if status == "too_large":
   938|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   939|                 continue
   940|         top_level = sorted(
   941|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   942|             key=lambda r: int(r["start_line"]),
   943|         )
   944|         # The "Top-level symbols:" inventory itself is optional metadata,
   945|         # same as this symbol's caller/callee/etc. expansions -- deferred
   946|         # to the pass below (after every explicit file/symbol/line
   947|         # selector has had its guaranteed shot at the budget), so file A's
   948|         # inventory can never spend budget that file B's own explicit
   949|         # excerpt needed.
   950|         file_expansion_items.append((rel, top_level))
   951| 
   952|     for res in symbol_resolutions:
   953|         if res.status != "resolved":
   954|             continue
   955|         row = res.resolved_rows[0]
   956|         rel = row["relative_path"]
   957|         symbol_key = (rel, row["qualified_name"])
   958|         if symbol_key in rendered_symbols:
   959|             continue  # duplicate selector; first occurrence's outcome is final
   960|         rendered_symbols.add(symbol_key)
   961|         if not note_focus_file(rel):
   962|             explicit_conflicts.append(
   963|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   964|                 f"limits.max_files ({resolved.max_files}) reached"
   965|             )
   966|             continue
   967|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   968|         if not _spend_header(header):
   969|             explicit_conflicts.append(
   970|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   971|             )
   972|             continue
   973|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   974|                                         files_by_path.get(rel, {}).get("sha256", ""))
   975|         if status == "too_large":
   976|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   977|         symbol_expansion_items.append(row)
   978| 
   979|     for res in line_resolutions:
   980|         if res.status != "resolved":
   981|             continue
   982|         info = res.resolved_rows[0]
   983|         rel = info["file"]
   984|         line_key = (rel, info["start"], info["end"])
   985|         if line_key in rendered_lines:
   986|             continue  # duplicate selector; first occurrence's outcome is final
   987|         rendered_lines.add(line_key)
   988|         if not note_focus_file(rel):
   989|             explicit_conflicts.append(
   990|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   991|             )
   992|             continue
   993|         # Only substitute a smaller enclosing symbol when it contains
   994|         # *both* endpoints of the requested range -- a symbol containing
   995|         # just the start line (the old check) could be smaller than the
   996|         # actual request (e.g. lines 2-7 where line 2's enclosing function
   997|         # ends at line 2), silently truncating the rendered excerpt to
   998|         # that symbol's own bounds while the resolution report still
   999|         # claimed the full range was resolved.
  1000|         enclosing = [
  1001|             r for r in symbols_by_file.get(rel, [])
  1002|             if int(r["start_line"]) <= info["start"] and info["end"] <= int(r["end_line"])
  1003|             and r["symbol_type"] != "module"
  1004|         ]
  1005|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
  1006|         if not _spend_header(header):
  1007|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
  1008|             continue
  1009|         if enclosing:
  1010|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
  1011|             row = enclosing[0]
  1012|             enclosing_line = (f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
  1013|                                f"lines {row['start_line']}-{row['end_line']})\n")
  1014|             # Metadata, not the mandatory excerpt itself -- budgeted like
  1015|             # every other optional line, with a non-fatal skip if it
  1016|             # doesn't fit rather than silently rendering it unbudgeted.
  1017|             if budget.allow(enclosing_line, 1):
  1018|                 out.append(enclosing_line)
  1019|                 budget.spend(enclosing_line, 1)
  1020|             else:
  1021|                 budget.omissions.append(
  1022|                     f"Enclosing-symbol note for `{res.requested}` omitted (packet size limit reached)."
  1023|                 )
  1024|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
  1025|                                             files_by_path.get(rel, {}).get("sha256", ""))
  1026|         else:
  1027|             start, end = max(1, info["start"] - 10), info["end"] + 10
  1028|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
  1029|         if status == "too_large":
  1030|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
  1031| 
  1032|     if explicit_conflicts:
  1033|         # An explicit selection itself didn't fit -- either the token
  1034|         # budget or limits.max_files -- per contract this is a hard
  1035|         # conflict, not something to truncate silently.
  1036|         return None, [_res_to_dict(r) for r in all_resolutions], (
  1037|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
  1038|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
  1039|             f"relevant limit or narrow the request. Conflicts:\n"
  1040|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
  1041|         )
  1042| 
  1043|     # Every explicit selector fit -- now spend whatever budget remains on
  1044|     # optional metadata/expansions (callers/callees/imports/tests/
  1045|     # Graphify, plus each explicit file selector's own "Top-level
  1046|     # symbols:" inventory). Done only now, not interleaved with tier-1's
  1047|     # rendering above, so a symbol's expansions -- or one file's inventory
  1048|     # -- can never manufacture a budget conflict for a later explicit
  1049|     # selector.
  1050|     for rel, top_level in file_expansion_items:
  1051|         if top_level:
  1052|             header = "Top-level symbols:"
  1053|             if budget.allow(header, 1):
  1054|                 out.append(header)
  1055|                 budget.spend(header, 1)
  1056|                 for idx, r in enumerate(top_level):
  1057|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
  1058|                     if not budget.allow(line, 1):
  1059|                         budget.omissions.append(
  1060|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
  1061|                             f"listing (packet size limit reached); see python_symbols.csv."
  1062|                         )
  1063|                         break
  1064|                     out.append(line)
  1065|                     budget.spend(line, 1)
  1066|             else:
  1067|                 budget.omissions.append(
  1068|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
  1069|                     f"see python_symbols.csv."
  1070|                 )
  1071|     file_level_expansion_done: set = set()
  1072| 
  1073|     def _maybe_file_expansion(rel: str) -> None:
  1074|         # Once per file regardless of how many symbols in it get
  1075|         # expanded, and regardless of whether that file is reached via a
  1076|         # file selector's own top-level symbols or a distinct explicit
  1077|         # symbol selector in the same file.
  1078|         if rel in file_level_expansion_done:
  1079|             return
  1080|         file_level_expansion_done.add(rel)
  1081|         _file_expansion(rel, imports_rows, calls_rows, files_by_path, resolved, budget, out, note_focus_file,
  1082|                          communities_by_file)
  1083| 
  1084|     for rel, top_level in file_expansion_items:
  1085|         for r in top_level:
  1086|             _symbol_expansion(r, calls_rows, resolved, budget, out, note_focus_file)
  1087|         # File-level expansion (imports/related-tests/Graphify peers) must
  1088|         # run for every explicitly selected file, not just ones with
  1089|         # top-level symbols -- a symbol-free file (e.g. an __init__.py
  1090|         # re-export shim, or a plain config module) previously never got
  1091|         # its imports/related-tests/Graphify-peer expansion at all, since
  1092|         # this loop used to skip straight past it when `top_level` was
  1093|         # empty. Those expansions describe the file, not any symbol in
  1094|         # it, so they apply regardless of whether the file happens to
  1095|         # define any top-level symbols.
  1096|         _maybe_file_expansion(rel)
  1097|     for row in symbol_expansion_items:
  1098|         _symbol_expansion(row, calls_rows, resolved, budget, out, note_focus_file)
  1099|         _maybe_file_expansion(row["relative_path"])
  1100| 
  1101|     # --- Tier 2: exact search-term matches ---
  1102|     # Matches were already computed by resolve_search_terms() above (before
  1103|     # the strict-mode gate) -- reused here rather than re-scanning the
  1104|     # repository a second time.
  1105|     for term in resolved.search_terms:
  1106|         matches = search_matches_by_term.get(term, [])
  1107|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
  1108|         if term_status == "invalid":
  1109|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
  1110|             if not budget.allow(notice, 1):
  1111|                 # Unbounded per-term notices (e.g. a request with hundreds
  1112|                 # of invalid regex terms) would otherwise bypass
  1113|                 # limits.max_estimated_tokens entirely, same failure shape
  1114|                 # as the earlier unbudgeted resolution-report finding.
  1115|                 budget.omissions.append(
  1116|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
  1117|                     f"see the resolution report."
  1118|                 )
  1119|                 continue
  1120|             out.append(notice)
  1121|             budget.spend(notice, 1)
  1122|             continue
  1123|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
  1124|         if not budget.allow(header, 1):
  1125|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
  1126|             continue
  1127|         out.append(header); budget.spend(header, 1)
  1128|         max_files_note_emitted = False
  1129|         for rel, ln, text in matches:
  1130|             # Redact the full line before truncating it, not after -- a
  1131|             # secret-shaped value whose closing quote falls beyond
  1132|             # character 200 would otherwise have that quote cut off
  1133|             # first, breaking _SECRET_ASSIGNMENT_PATTERN's closing-quote
  1134|             # backreference so redact_secrets() never matches and the
  1135|             # (truncated) secret prefix leaks into the packet.
  1136|             line_text = redact_secrets(text.strip())[:200]
  1137|             line = f"- `{rel}:{ln}` — `{line_text}`"
  1138|             # Check the budget *before* reserving a focus-file slot for
  1139|             # this match -- otherwise a match that ultimately doesn't fit
  1140|             # (and is never rendered) could still consume the one
  1141|             # remaining limits.max_files slot, starving a later, shorter
  1142|             # match from a different file that would have fit.
  1143|             if not budget.allow(line, 1):
  1144|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
  1145|                 break
  1146|             if not note_focus_file(rel):
  1147|                 # note_focus_file enforces limits.max_files against the
  1148|                 # *global* focus-file set shared across every selector/tier
  1149|                 # in this packet, not just this one search term -- so the
  1150|                 # cap holds even when different terms match different files.
  1151|                 # A match in a file beyond the cap doesn't mean every
  1152|                 # *later* match is unreachable too -- a later match may be
  1153|                 # in a file already in focus_files (e.g. the selected
  1154|                 # file), which costs no new slot. Skip this one match and
  1155|                 # keep checking the rest instead of abandoning the term.
  1156|                 if not max_files_note_emitted:
  1157|                     budget.omissions.append(
  1158|                         f"Additional `{term}` match(es) beyond limits.max_files ({resolved.max_files}) omitted."
  1159|                     )
  1160|                     max_files_note_emitted = True
  1161|                 continue
  1162|             out.append(line); budget.spend(line, 1)
  1163|     if stale_search_files:
  1164|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
  1165|                                  f"skipped for search terms; re-run scan.")
  1166| 
  1167|     if budget.omissions:
  1168|         # The omissions *list* itself is unbounded in memory (a request
  1169|         # that triggers hundreds of distinct omission reasons -- e.g. many
  1170|         # invalid regex search terms -- can produce hundreds of entries),
  1171|         # so rendering it must be budgeted the same way the resolution
  1172|         # report is: otherwise this section alone could bypass
  1173|         # limits.max_estimated_tokens. The full list is always available,
  1174|         # unbudgeted, in the packet_*.resolution.json sidecar's
  1175|         # "omissions" field.
  1176|         header = "\n## Omitted / unresolved\n"
  1177|         if budget.allow(header, 1):
  1178|             out.append(header)
  1179|             budget.spend(header, 1)
  1180|             omitted_omissions = 0
  1181|             for idx, o in enumerate(budget.omissions):
  1182|                 line = f"- {o}"
  1183|                 if not budget.allow(line, 1):
  1184|                     omitted_omissions = len(budget.omissions) - idx
  1185|                     break
  1186|                 out.append(line)
  1187|                 budget.spend(line, 1)
  1188|             if omitted_omissions:
  1189|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
  1190|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
  1191|                         f"the complete list.")
  1192|                 if budget.allow(note, 1):
  1193|                     out.append(note)
  1194|                     budget.spend(note, 1)
  1195| 
  1196|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
  1197|     # resolution report" section built into the header below -- not
  1198|     # repeated here. header_lines/resolution_lines/footer were built and
  1199|     # charged against the budget up front, before Tier-1/Tier-2 rendering
  1200|     # -- see the "Reserve the fixed framing's budget cost up front"
  1201|     # comment above.)
  1202| 
  1203|     # Computed last so it reflects Tier-1/Tier-2's and the resolution
  1204|     # report's/footer's own budget spend too.
  1205|     header_lines.append(
  1206|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
  1207|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
  1208|     )
  1209|     header_lines.extend(resolution_lines)
  1210| 
  1211|     out.append(footer)
  1212| 
  1213|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1214| 
  1215|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1216|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1217|     atomic_write_text(packet_path, text)
  1218| 
  1219|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1220|     sidecar = {
  1221|         "tool_version": TOOL_VERSION,
  1222|         "schema_version": resolved.schema_version,
  1223|         "question": resolved.question,
  1224|         "request_file_sha256": request_hash,
  1225|         "git": git_info,
  1226|         "estimated_tokens_used": round(budget.chars_used / 4),
  1227|         "focus_files": focus_files,
  1228|         "omissions": budget.omissions,
  1229|         "resolution_report": resolution_report,
  1230|     }
  1231|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1232|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1233| 
  1234|     return packet_path, resolution_report, None
```
