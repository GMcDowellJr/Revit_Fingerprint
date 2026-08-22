# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 4
- Original line range: 521-757
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _render_origin_header, _render_excerpt_block, _symbol_expansion, _file_expansion
- Source SHA-256: da6b351bdc8071f0313b339e641c5fdb991fa445c5ab75e6b857884c94d04dea
- Starts inside symbol: no
- Ends inside symbol: no

```
   521| 
   522| 
   523| # --- Rendering ------------------------------------------------------------
   524| 
   525| def _render_origin_header(title: str, origins: list) -> str:
   526|     return f"\n### {title}\n_Included because: {', '.join(origins)}._\n"
   527| 
   528| 
   529| def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
   530|                            expected_sha256: str) -> str:
   531|     """Returns "rendered", "stale" (withheld -- source changed since scan),
   532|     "unavailable" (file missing/unreadable), or "too_large" (would not fit
   533|     within the remaining budget). Only "too_large" is a hard, must-not-be-
   534|     silently-dropped conflict for an *explicit* selector -- "stale" and
   535|     "unavailable" are reported as ordinary (non-fatal) omissions, matching
   536|     the direct --file/--symbol/--line packet path's existing behavior."""
   537|     if not _file_is_fresh(root, rel_path, expected_sha256):
   538|         msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
   539|                f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
   540|         if budget.allow(msg, 1):
   541|             out.append(msg)
   542|             budget.spend(msg, 1)
   543|         budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
   544|         return "stale"
   545|     lines = _iter_safe_lines(root, rel_path, start, end)
   546|     if lines is None:
   547|         out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
   548|         budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
   549|         return "unavailable"
   550|     # Stream the requested range rather than materializing it whole up
   551|     # front -- `end` can be an oversized file's real line_count (the
   552|     # scanner deliberately keeps files over MAX_TEXT_READ_BYTES in the
   553|     # inventory, with a real streamed-counted line_count, without ever
   554|     # reading them into memory whole -- see rc_scan.py), so an explicit
   555|     # file selector naming such a file could otherwise allocate hundreds
   556|     # of megabytes or more just to learn the excerpt is "too_large".
   557|     # Track the raw (pre-redaction) size as lines come in and bail out as
   558|     # soon as it clearly exceeds the remaining budget, without reading
   559|     # the rest of the file. redact_secrets() can grow a line slightly (a
   560|     # short matched secret replaced by the fixed-length placeholder), so
   561|     # this early check is deliberately against the raw size, not a
   562|     # substitute for the exact post-redaction budget.allow() check below
   563|     # -- it only ever *skips* reading further, never changes what content
   564|     # that does get read is judged against.
   565|     body_lines = []
   566|     remaining_chars = budget.max_characters - budget.chars_used
   567|     raw_chars = 0
   568|     too_large = False
   569|     try:
   570|         for ln, text in lines:
   571|             rendered = f"{ln:>6}| {text}"
   572|             body_lines.append(rendered)
   573|             raw_chars += len(rendered) + 1  # +1 for the joining newline
   574|             if raw_chars > remaining_chars:
   575|                 too_large = True
   576|                 break
   577|     finally:
   578|         lines.close()
   579|     if too_large:
   580|         return "too_large"
   581|     # Redact *before* the budget check, not after -- redact_secrets()
   582|     # replaces a matched secret with a placeholder that can be longer
   583|     # than the original text, so checking budget.allow() against the raw
   584|     # body and only redacting afterward let the actually-written content
   585|     # end up bigger than what was verified to fit.
   586|     body = redact_secrets("\n".join(body_lines))
   587|     if not budget.allow(body, len(body_lines)):
   588|         return "too_large"
   589|     out.append("```\n" + body + "\n```\n")
   590|     budget.spend(body, len(body_lines))
   591|     return "rendered"
   592| 
   593| 
   594| def _symbol_expansion(row: dict, calls_rows: list, req: ResolvedRequest, budget: Budget, out: list,
   595|                        note_focus_file) -> None:
   596|     """Callers/callees are inherently per-symbol (each symbol has its own
   597|     call graph neighborhood), unlike imports/related-tests/Graphify peers
   598|     below in _file_expansion(), which describe the containing file and
   599|     must not be repeated once per symbol in it."""
   600|     rel, qn = row["relative_path"], row["qualified_name"]
   601| 
   602|     if req.include_callers:
   603|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   604|         if callers:
   605|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   606|             if budget.allow(header, 1):
   607|                 out.append(header); budget.spend(header, 1)
   608|                 max_files_note_emitted = False
   609|                 for c in callers:
   610|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   611|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   612|                             f"[origin: caller_expansion]")
   613|                     # Budget-check before reserving a focus-file slot -- a
   614|                     # caller entry that ultimately doesn't fit must not
   615|                     # consume the slot on behalf of content that was never
   616|                     # actually rendered.
   617|                     if not budget.allow(line, 1):
   618|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   619|                         break
   620|                     if not note_focus_file(c["caller_file"]):
   621|                         # A file beyond limits.max_files doesn't mean every
   622|                         # *later* caller is unreachable too -- a later one
   623|                         # may be in a file already in focus_files, which
   624|                         # note_focus_file accepts for free. Skip this entry
   625|                         # and keep checking the rest instead of abandoning
   626|                         # the whole listing.
   627|                         if not max_files_note_emitted:
   628|                             budget.omissions.append(
   629|                                 f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   630|                             )
   631|                             max_files_note_emitted = True
   632|                         continue
   633|                     out.append(line); budget.spend(line, 1)
   634|             else:
   635|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   636| 
   637|     if req.include_callees:
   638|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   639|         if callees:
   640|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   641|             if budget.allow(header, 1):
   642|                 out.append(header); budget.spend(header, 1)
   643|                 max_files_note_emitted = False
   644|                 for c in callees:
   645|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   646|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   647|                             f"[origin: callee_expansion]")
   648|                     if not budget.allow(line, 1):
   649|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   650|                         break
   651|                     if not note_focus_file(c["candidate_file"]):
   652|                         if not max_files_note_emitted:
   653|                             budget.omissions.append(
   654|                                 f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   655|                             )
   656|                             max_files_note_emitted = True
   657|                         continue
   658|                     out.append(line); budget.spend(line, 1)
   659|             else:
   660|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   661| 
   662| 
   663| def _file_expansion(rel: str, imports_rows: list, calls_rows: list, files_by_path: dict,
   664|                      req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   665|                      communities_by_file: Optional[dict] = None) -> None:
   666|     """Internal imports, related tests, and Graphify community peers all
   667|     describe the *file*, not any one symbol in it -- unlike
   668|     callers/callees above, which are inherently per-symbol. Must be
   669|     called at most once per file regardless of how many of that file's
   670|     symbols are being expanded (see the caller: it used to call this
   671|     content once per top-level symbol, rendering identical "Internal
   672|     imports of X"/"Related tests for X"/"Graphify community peers of X"
   673|     sections over and over for a multi-symbol file)."""
   674|     if req.include_imports:
   675|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   676|         if file_imports:
   677|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   678|             if budget.allow(header, 1):
   679|                 out.append(header); budget.spend(header, 1)
   680|                 max_files_note_emitted = False
   681|                 for i in file_imports[:20]:
   682|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
   683|                     if not budget.allow(line, 1):
   684|                         break
   685|                     if not note_focus_file(i["resolved_file"]):
   686|                         if not max_files_note_emitted:
   687|                             budget.omissions.append(
   688|                                 f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   689|                             )
   690|                             max_files_note_emitted = True
   691|                         continue
   692|                     out.append(line); budget.spend(line, 1)
   693| 
   694|     if req.include_related_tests:
   695|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   696|         if tests:
   697|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   698|             if budget.allow(header, 1):
   699|                 out.append(header); budget.spend(header, 1)
   700|                 max_files_note_emitted = False
   701|                 for t in tests:
   702|                     line = f"- `{t}`"
   703|                     if not budget.allow(line, 1):
   704|                         break
   705|                     if not note_focus_file(t):
   706|                         # Route through the same global-focus-file gate as
   707|                         # every other tier -- a hard-coded high ceiling here
   708|                         # would let related-test expansion silently bypass
   709|                         # limits.max_files.
   710|                         if not max_files_note_emitted:
   711|                             budget.omissions.append(
   712|                                 f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   713|                             )
   714|                             max_files_note_emitted = True
   715|                         continue
   716|                     out.append(line); budget.spend(line, 1)
   717| 
   718|     if req.include_graphify and communities_by_file:
   719|         my_communities = communities_by_file.get(rel, [])
   720|         if my_communities:
   721|             comm_ids = {cid for cid, _ in my_communities}
   722|             peers = sorted(
   723|                 f for f, comms in communities_by_file.items()
   724|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   725|             )[:10]
   726|             if peers:
   727|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   728|                 if budget.allow(header, 1):
   729|                     out.append(header); budget.spend(header, 1)
   730|                     max_files_note_emitted = False
   731|                     for p in peers:
   732|                         line = f"- `{p}` [origin: graphify_expansion]"
   733|                         if not budget.allow(line, 1):
   734|                             budget.omissions.append(
   735|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   736|                             )
   737|                             break
   738|                         if not note_focus_file(p):
   739|                             # Route through the same global focus-file gate
   740|                             # as every other expansion tier -- otherwise a
   741|                             # Graphify peer could silently exceed
   742|                             # limits.max_files while the resolution
   743|                             # sidecar's focus_files list stayed under it.
   744|                             if not max_files_note_emitted:
   745|                                 budget.omissions.append(
   746|                                     f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
   747|                                     f"({req.max_files}) omitted."
   748|                                 )
   749|                                 max_files_note_emitted = True
   750|                             continue
   751|                         out.append(line); budget.spend(line, 1)
   752|                 else:
   753|                     budget.omissions.append(
   754|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   755|                     )
   756| 
   757| 
```
