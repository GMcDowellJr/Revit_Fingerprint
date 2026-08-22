# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 3 of 3
- Original line range: 620-1031
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: generate_packet_from_request, generate_packet_from_request.note_focus_file, generate_packet_from_request._spend_header, _res_to_dict
- Source SHA-256: f8fc322e94f1d42391838800f006205cb1179854a0162063d062fbcc18f13f91
- Starts inside symbol: no
- Ends inside symbol: no

```
   620| def generate_packet_from_request(root: Path, output_dir: Path, request_path: Path,
   621|                                   name_override: Optional[str] = None) -> tuple:
   622|     """Returns (packet_path_or_None, resolution_report, error_message_or_None).
   623| 
   624|     error_message_or_None is set (and packet_path is None) for a
   625|     structurally invalid request, or when strict mode / an unresolvable
   626|     explicit-selector budget conflict blocks generation -- no partial
   627|     packet is written in either case.
   628|     """
   629|     try:
   630|         request_text = request_path.read_text(encoding="utf-8")
   631|     except OSError as exc:
   632|         return None, [], f"could not read request file {request_path}: {exc}"
   633| 
   634|     request_hash = sha256_text(request_text)
   635|     resolved, errors = parse_and_validate_request(request_text)
   636|     if errors:
   637|         return None, [], "invalid packet_request.json:\n" + "\n".join(f"  - {e}" for e in errors)
   638| 
   639|     files_rows = _load_csv(output_dir / "file_inventory.csv")
   640|     symbols_rows = _load_csv(output_dir / "python_symbols.csv")
   641|     imports_rows = _load_csv(output_dir / "python_imports.csv")
   642|     calls_rows = _load_csv(output_dir / "python_calls.csv")
   643|     files_by_path = {r["relative_path"]: r for r in files_rows}
   644|     symbols_by_file: dict = {}
   645|     for r in symbols_rows:
   646|         symbols_by_file.setdefault(r["relative_path"], []).append(r)
   647| 
   648|     file_resolutions = resolve_files(resolved.files, files_by_path)
   649|     symbol_resolutions = resolve_symbols(resolved.symbols, symbols_rows)
   650|     line_resolutions = resolve_lines(resolved.lines, files_by_path)
   651|     search_resolutions, search_matches_by_term, stale_search_files = resolve_search_terms(
   652|         root, resolved.search_terms, resolved.search_as_regex, files_rows,
   653|     )
   654|     all_resolutions = file_resolutions + symbol_resolutions + line_resolutions + search_resolutions
   655| 
   656|     unresolved_explicit = [r for r in all_resolutions if r.status != "resolved"]
   657|     if resolved.strict and unresolved_explicit:
   658|         lines = [f"  - {r.selector_type} '{r.requested}': {r.status} — {r.detail}" for r in unresolved_explicit]
   659|         return None, [_res_to_dict(r) for r in all_resolutions], (
   660|             "strict mode: aborting because the following selector(s) did not resolve cleanly:\n" + "\n".join(lines)
   661|         )
   662| 
   663|     budget = Budget(max_lines=1_000_000, max_characters=resolved.max_estimated_tokens * 4)
   664|     out: list = []
   665|     focus_files: list = []
   666| 
   667|     communities_by_file: dict = {}
   668|     if resolved.include_graphify:
   669|         _git_for_graphify = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   670|         communities_by_file, graphify_warnings_for_request = rc_graphify.load_graphify_communities(
   671|             root, _git_for_graphify.get("commit") if _git_for_graphify.get("available") else None,
   672|             current_dirty=_git_for_graphify.get("dirty") if _git_for_graphify.get("available") else None,
   673|         )
   674|         for w in graphify_warnings_for_request:
   675|             budget.omissions.append(f"expansion.include_graphify was requested but unavailable: {w}")
   676| 
   677|     def note_focus_file(rel: str) -> bool:
   678|         if rel in focus_files:
   679|             return True
   680|         if len(focus_files) >= resolved.max_files:
   681|             return False
   682|         focus_files.append(rel)
   683|         return True
   684| 
   685|     # --- Tier 1: explicit selectors (never silently dropped) ---
   686|     # explicit_conflicts collects only "the explicit excerpt itself doesn't
   687|     # fit the budget" -- a hard, must-be-reported-not-truncated conflict.
   688|     # Freshness withholding and non-mandatory expansions (callers/callees/
   689|     # imports/tests) are still recorded as ordinary, non-fatal omissions on
   690|     # `budget` and must NOT abort generation.
   691|     #
   692|     # Two passes, deliberately in this order:
   693|     #   1. Render every explicit selector's own header/excerpt first (and
   694|     #      only once per distinct target -- two selectors naming the same
   695|     #      file/symbol/line render it a single time). None of this may be
   696|     #      pre-empted by expansion content.
   697|     #   2. Only once every explicit item has had its guaranteed shot at the
   698|     #      budget do optional expansions (callers/callees/imports/tests/
   699|     #      Graphify) spend whatever budget remains. Interleaving expansion
   700|     #      spend between explicit items (the previous structure) let one
   701|     #      selector's expansions manufacture a budget conflict for a later,
   702|     #      otherwise-fitting explicit selector.
   703|     explicit_conflicts: list = []
   704|     rendered_files: set = set()
   705|     rendered_symbols: set = set()
   706|     rendered_lines: set = set()
   707|     file_expansion_items: list = []    # [(top_level_rows)]
   708|     symbol_expansion_items: list = []  # [row]
   709| 
   710|     def _spend_header(header: str) -> bool:
   711|         if not budget.allow(header, 2):
   712|             return False
   713|         out.append(header)
   714|         budget.spend(header, 2)
   715|         return True
   716| 
   717|     for res in file_resolutions:
   718|         if res.status != "resolved":
   719|             continue
   720|         rel = res.requested
   721|         if rel in rendered_files:
   722|             # Duplicate selector for a file already attempted -- its first
   723|             # occurrence's outcome (rendered, or a recorded conflict) is
   724|             # final; re-attempting an identical selector would just repeat
   725|             # the same header/budget work (or the same conflict message)
   726|             # once per repeat, which is itself an unbounded-output shape
   727|             # for a request naming the same selector many times over.
   728|             continue
   729|         rendered_files.add(rel)
   730|         if not note_focus_file(rel):
   731|             explicit_conflicts.append(
   732|                 f"explicit file selector `{rel}` does not fit: limits.max_files ({resolved.max_files}) reached"
   733|             )
   734|             continue
   735|         row = res.resolved_rows[0]
   736|         header = _render_origin_header(f"File: `{rel}`", ["explicit_file_selector"])
   737|         if not _spend_header(header):
   738|             explicit_conflicts.append(f"explicit file selector `{rel}` does not fit (header alone exceeds budget)")
   739|             continue
   740|         # Render the mandatory excerpt (the actual content the selector asked
   741|         # for) before the optional top-level-symbols inventory, so the
   742|         # inventory can never spend the shared budget ahead of the excerpt
   743|         # itself and force it into an "explicit_conflicts" abort.
   744|         try:
   745|             line_count = int(row.get("line_count") or 0)
   746|         except ValueError:
   747|             line_count = 0
   748|         if line_count:
   749|             status = _render_excerpt_block(root, rel, 1, line_count, budget, out, row.get("sha256", ""))
   750|             if status == "too_large":
   751|                 explicit_conflicts.append(f"explicit file selector `{rel}` ({line_count} lines) does not fit")
   752|                 continue
   753|         top_level = sorted(
   754|             [r for r in symbols_by_file.get(rel, []) if r["parent_symbol"] == "<module>" and r["symbol_type"] != "module"],
   755|             key=lambda r: int(r["start_line"]),
   756|         )
   757|         if top_level:
   758|             header = "Top-level symbols:"
   759|             if budget.allow(header, 1):
   760|                 out.append(header)
   761|                 budget.spend(header, 1)
   762|                 for idx, r in enumerate(top_level):
   763|                     line = f"- `{r['qualified_name']}` ({r['symbol_type']}, lines {r['start_line']}-{r['end_line']})"
   764|                     if not budget.allow(line, 1):
   765|                         budget.omissions.append(
   766|                             f"{len(top_level) - idx} more top-level symbol(s) in `{rel}` omitted from the "
   767|                             f"listing (packet size limit reached); see python_symbols.csv."
   768|                         )
   769|                         break
   770|                     out.append(line)
   771|                     budget.spend(line, 1)
   772|             else:
   773|                 budget.omissions.append(
   774|                     f"Top-level symbol listing for `{rel}` omitted entirely (packet size limit reached); "
   775|                     f"see python_symbols.csv."
   776|                 )
   777|         file_expansion_items.append(top_level)
   778| 
   779|     for res in symbol_resolutions:
   780|         if res.status != "resolved":
   781|             continue
   782|         row = res.resolved_rows[0]
   783|         rel = row["relative_path"]
   784|         symbol_key = (rel, row["qualified_name"])
   785|         if symbol_key in rendered_symbols:
   786|             continue  # duplicate selector; first occurrence's outcome is final
   787|         rendered_symbols.add(symbol_key)
   788|         if not note_focus_file(rel):
   789|             explicit_conflicts.append(
   790|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit: "
   791|                 f"limits.max_files ({resolved.max_files}) reached"
   792|             )
   793|             continue
   794|         header = _render_origin_header(f"Symbol: `{row['qualified_name']}` — `{rel}`", ["explicit_symbol_selector"])
   795|         if not _spend_header(header):
   796|             explicit_conflicts.append(
   797|                 f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit (header alone exceeds budget)"
   798|             )
   799|             continue
   800|         status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   801|                                         files_by_path.get(rel, {}).get("sha256", ""))
   802|         if status == "too_large":
   803|             explicit_conflicts.append(f"explicit symbol selector `{row['qualified_name']}` in `{rel}` does not fit")
   804|         symbol_expansion_items.append(row)
   805| 
   806|     for res in line_resolutions:
   807|         if res.status != "resolved":
   808|             continue
   809|         info = res.resolved_rows[0]
   810|         rel = info["file"]
   811|         line_key = (rel, info["start"], info["end"])
   812|         if line_key in rendered_lines:
   813|             continue  # duplicate selector; first occurrence's outcome is final
   814|         rendered_lines.add(line_key)
   815|         if not note_focus_file(rel):
   816|             explicit_conflicts.append(
   817|                 f"explicit line selector `{res.requested}` does not fit: limits.max_files ({resolved.max_files}) reached"
   818|             )
   819|             continue
   820|         enclosing = [
   821|             r for r in symbols_by_file.get(rel, [])
   822|             if int(r["start_line"]) <= info["start"] <= int(r["end_line"]) and r["symbol_type"] != "module"
   823|         ]
   824|         header = _render_origin_header(f"Line selector: `{res.requested}`", ["explicit_line_selector"])
   825|         if not _spend_header(header):
   826|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit (header alone exceeds budget)")
   827|             continue
   828|         if enclosing:
   829|             enclosing.sort(key=lambda r: int(r["end_line"]) - int(r["start_line"]))
   830|             row = enclosing[0]
   831|             out.append(f"Enclosing symbol: `{row['qualified_name']}` ({row['symbol_type']}, "
   832|                        f"lines {row['start_line']}-{row['end_line']})\n")
   833|             status = _render_excerpt_block(root, rel, int(row["start_line"]), int(row["end_line"]), budget, out,
   834|                                             files_by_path.get(rel, {}).get("sha256", ""))
   835|         else:
   836|             start, end = max(1, info["start"] - 10), info["end"] + 10
   837|             status = _render_excerpt_block(root, rel, start, end, budget, out, files_by_path.get(rel, {}).get("sha256", ""))
   838|         if status == "too_large":
   839|             explicit_conflicts.append(f"explicit line selector `{res.requested}` does not fit")
   840| 
   841|     if explicit_conflicts:
   842|         # An explicit selection itself didn't fit -- either the token
   843|         # budget or limits.max_files -- per contract this is a hard
   844|         # conflict, not something to truncate silently.
   845|         return None, [_res_to_dict(r) for r in all_resolutions], (
   846|             "the requested explicit selector(s) do not fit within limits.max_estimated_tokens "
   847|             f"({resolved.max_estimated_tokens}) / limits.max_files ({resolved.max_files}); increase the "
   848|             f"relevant limit or narrow the request. Conflicts:\n"
   849|             + "\n".join(f"  - {o}" for o in explicit_conflicts)
   850|         )
   851| 
   852|     # Every explicit selector fit -- now spend whatever budget remains on
   853|     # optional expansions (callers/callees/imports/tests/Graphify). Done
   854|     # only now, not interleaved with tier-1's rendering above, so a
   855|     # symbol's expansions can never manufacture a budget conflict for a
   856|     # later explicit selector.
   857|     for top_level in file_expansion_items:
   858|         for r in top_level:
   859|             _symbol_expansion(r, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   860|                               communities_by_file)
   861|     for row in symbol_expansion_items:
   862|         _symbol_expansion(row, calls_rows, imports_rows, files_by_path, resolved, budget, out, note_focus_file,
   863|                           communities_by_file)
   864| 
   865|     # --- Tier 2: exact search-term matches ---
   866|     # Matches were already computed by resolve_search_terms() above (before
   867|     # the strict-mode gate) -- reused here rather than re-scanning the
   868|     # repository a second time.
   869|     for term in resolved.search_terms:
   870|         matches = search_matches_by_term.get(term, [])
   871|         term_status = next((r.status for r in search_resolutions if r.requested == term), None)
   872|         if term_status == "invalid":
   873|             notice = f"\n_Search term `{term}` is not a valid regex; skipped._\n"
   874|             if not budget.allow(notice, 1):
   875|                 # Unbounded per-term notices (e.g. a request with hundreds
   876|                 # of invalid regex terms) would otherwise bypass
   877|                 # limits.max_estimated_tokens entirely, same failure shape
   878|                 # as the earlier unbudgeted resolution-report finding.
   879|                 budget.omissions.append(
   880|                     f"Invalid-regex notice for `{term}` omitted (packet size limit reached); "
   881|                     f"see the resolution report."
   882|                 )
   883|                 continue
   884|             out.append(notice)
   885|             budget.spend(notice, 1)
   886|             continue
   887|         header = f"\n### Search: `{term}` ({len(matches)} match(es))\n_Included because: exact_search_match._\n"
   888|         if not budget.allow(header, 1):
   889|             budget.omissions.append(f"Search results for `{term}` omitted entirely (packet size limit reached).")
   890|             continue
   891|         out.append(header); budget.spend(header, 1)
   892|         for rel, ln, text in matches:
   893|             line_text = redact_secrets(text.strip()[:200])
   894|             line = f"- `{rel}:{ln}` — `{line_text}`"
   895|             # Check the budget *before* reserving a focus-file slot for
   896|             # this match -- otherwise a match that ultimately doesn't fit
   897|             # (and is never rendered) could still consume the one
   898|             # remaining limits.max_files slot, starving a later, shorter
   899|             # match from a different file that would have fit.
   900|             if not budget.allow(line, 1):
   901|                 budget.omissions.append(f"Additional `{term}` matches omitted (packet size limit reached).")
   902|                 break
   903|             if not note_focus_file(rel):
   904|                 # note_focus_file enforces limits.max_files against the
   905|                 # *global* focus-file set shared across every selector/tier
   906|                 # in this packet, not just this one search term -- so the
   907|                 # cap holds even when different terms match different files.
   908|                 budget.omissions.append(f"Additional `{term}` matches omitted beyond limits.max_files ({resolved.max_files}).")
   909|                 break
   910|             out.append(line); budget.spend(line, 1)
   911|     if stale_search_files:
   912|         budget.omissions.append(f"{stale_search_files} file(s) changed on disk since the last scan and were "
   913|                                  f"skipped for search terms; re-run scan.")
   914| 
   915|     if budget.omissions:
   916|         # The omissions *list* itself is unbounded in memory (a request
   917|         # that triggers hundreds of distinct omission reasons -- e.g. many
   918|         # invalid regex search terms -- can produce hundreds of entries),
   919|         # so rendering it must be budgeted the same way the resolution
   920|         # report is: otherwise this section alone could bypass
   921|         # limits.max_estimated_tokens. The full list is always available,
   922|         # unbudgeted, in the packet_*.resolution.json sidecar's
   923|         # "omissions" field.
   924|         header = "\n## Omitted / unresolved\n"
   925|         if budget.allow(header, 1):
   926|             out.append(header)
   927|             budget.spend(header, 1)
   928|             omitted_omissions = 0
   929|             for idx, o in enumerate(budget.omissions):
   930|                 line = f"- {o}"
   931|                 if not budget.allow(line, 1):
   932|                     omitted_omissions = len(budget.omissions) - idx
   933|                     break
   934|                 out.append(line)
   935|                 budget.spend(line, 1)
   936|             if omitted_omissions:
   937|                 note = (f"- ... and {omitted_omissions} more omission(s) not listed here (packet size limit "
   938|                         f"reached); see the accompanying packet_*.resolution.json's \"omissions\" field for "
   939|                         f"the complete list.")
   940|                 if budget.allow(note, 1):
   941|                     out.append(note)
   942|                     budget.spend(note, 1)
   943| 
   944|     # (Unresolved/ambiguous selectors are reported once, in the "Selector
   945|     # resolution report" section built into the header below -- not
   946|     # repeated here.)
   947| 
   948|     git_info = get_git_info(root, exclude_paths=generated_output_exclude_paths(root, output_dir))
   949|     header_lines = [
   950|         "# Repo Context Packet (from packet_request.json)\n",
   951|         f"- Root: `{root.resolve().name}`",
   952|         f"- Question: {resolved.question}",
   953|         f"- schema_version: {resolved.schema_version}",
   954|         f"- Tool version: {TOOL_VERSION}",
   955|         f"- Request file: `{request_path.name}` (sha256: `{request_hash[:16]}…`)",
   956|     ]
   957|     if git_info.get("available"):
   958|         dirty = "dirty" if git_info.get("dirty") else ("clean" if git_info.get("dirty") is False else "unknown")
   959|         header_lines.append(f"- Repository revision: `{git_info['commit']}` ({dirty} worktree)")
   960|     else:
   961|         header_lines.append("- Repository revision: not available (not a git repository, or git is not installed)")
   962|     header_lines.append(
   963|         f"- Limits: max_estimated_tokens={resolved.max_estimated_tokens}, max_files={resolved.max_files}, "
   964|         f"max_hops={resolved.max_hops}"
   965|     )
   966| 
   967|     # The resolution report scales with the *request*, not the source
   968|     # repository (a request naming hundreds of missing/ambiguous
   969|     # selectors could otherwise render an unbounded report regardless of
   970|     # limits.max_estimated_tokens) -- charge it against the same budget
   971|     # as everything else, with a count-of-omitted note rather than an
   972|     # unbounded listing. The full, untruncated report is always available
   973|     # in the accompanying packet_<name>.resolution.json sidecar.
   974|     resolution_lines = ["## Selector resolution report\n"]
   975|     omitted_selector_count = 0
   976|     for idx, r in enumerate(all_resolutions):
   977|         entry = [f"- {r.selector_type} `{r.requested}`: **{r.status}** — {r.detail}"]
   978|         entry.extend(f"  - candidate: `{c}`" for c in r.candidates)
   979|         entry_text = "\n".join(entry)
   980|         if not budget.allow(entry_text, len(entry)):
   981|             omitted_selector_count = len(all_resolutions) - idx
   982|             break
   983|         resolution_lines.extend(entry)
   984|         budget.spend(entry_text, len(entry))
   985|     if omitted_selector_count:
   986|         note = (f"- ... and {omitted_selector_count} more selector(s) omitted from this report (packet size "
   987|                 f"limit reached); see the accompanying packet_*.resolution.json for the complete report.")
   988|         if budget.allow(note, 1):
   989|             resolution_lines.append(note)
   990|             budget.spend(note, 1)
   991|     resolution_lines.append("")
   992| 
   993|     # Computed last so it reflects the resolution report's own budget spend too.
   994|     header_lines.append(
   995|         f"- Estimated tokens used: ~{round(budget.chars_used / 4)} "
   996|         f"(chars_used={budget.chars_used}/{budget.max_characters})\n"
   997|     )
   998|     header_lines.extend(resolution_lines)
   999| 
  1000|     out.append("\n_Static analysis only. Call/import relationships above are candidates, not proof of runtime "
  1001|                 "dispatch. See README.md in this output directory for full limitations._\n")
  1002| 
  1003|     text = "\n".join(header_lines) + "\n".join(out) + "\n"
  1004| 
  1005|     stem = sanitize_stem(name_override) if name_override else sanitize_stem(request_path.stem)
  1006|     packet_path = output_dir / "packets" / f"packet_{stem}.md"
  1007|     atomic_write_text(packet_path, text)
  1008| 
  1009|     resolution_report = [_res_to_dict(r) for r in all_resolutions]
  1010|     sidecar = {
  1011|         "tool_version": TOOL_VERSION,
  1012|         "schema_version": resolved.schema_version,
  1013|         "question": resolved.question,
  1014|         "request_file_sha256": request_hash,
  1015|         "git": git_info,
  1016|         "estimated_tokens_used": round(budget.chars_used / 4),
  1017|         "focus_files": focus_files,
  1018|         "omissions": budget.omissions,
  1019|         "resolution_report": resolution_report,
  1020|     }
  1021|     atomic_write_text(output_dir / "packets" / f"packet_{stem}.resolution.json",
  1022|                        json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
  1023| 
  1024|     return packet_path, resolution_report, None
  1025| 
  1026| 
  1027| def _res_to_dict(r: SelectorResolution) -> dict:
  1028|     return {
  1029|         "selector_type": r.selector_type, "requested": r.requested,
  1030|         "status": r.status, "detail": r.detail, "candidates": r.candidates,
  1031|     }
```
