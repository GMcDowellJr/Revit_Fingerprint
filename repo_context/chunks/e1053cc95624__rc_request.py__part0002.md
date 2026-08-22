# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 3
- Original line range: 515-698
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _render_excerpt_block, _symbol_expansion
- Source SHA-256: 50a7a8ece86c108ece56ed514c39c4b261ea08176f2190cb9648cfeef747ed42
- Starts inside symbol: no
- Ends inside symbol: no

```
   515| def _render_excerpt_block(root: Path, rel_path: str, start: int, end: int, budget: Budget, out: list,
   516|                            expected_sha256: str) -> str:
   517|     """Returns "rendered", "stale" (withheld -- source changed since scan),
   518|     "unavailable" (file missing/unreadable), or "too_large" (would not fit
   519|     within the remaining budget). Only "too_large" is a hard, must-not-be-
   520|     silently-dropped conflict for an *explicit* selector -- "stale" and
   521|     "unavailable" are reported as ordinary (non-fatal) omissions, matching
   522|     the direct --file/--symbol/--line packet path's existing behavior."""
   523|     if not _file_is_fresh(root, rel_path, expected_sha256):
   524|         msg = (f"_Source excerpt withheld: `{rel_path}` has changed on disk since the last `scan` "
   525|                f"(SHA-256 mismatch); re-run `scan` for an up-to-date packet._\n")
   526|         if budget.allow(msg, 1):
   527|             out.append(msg)
   528|             budget.spend(msg, 1)
   529|         budget.omissions.append(f"`{rel_path}` changed since the last scan; excerpt withheld. Re-run scan.")
   530|         return "stale"
   531|     excerpt = _safe_excerpt(root, rel_path, start, end)
   532|     if excerpt is None:
   533|         out.append("_Source excerpt unavailable (file missing or unreadable)._\n")
   534|         budget.omissions.append(f"`{rel_path}` excerpt unavailable (file missing or unreadable).")
   535|         return "unavailable"
   536|     body_lines = [f"{ln:>6}| {text}" for ln, text in excerpt]
   537|     # Redact *before* the budget check, not after -- redact_secrets()
   538|     # replaces a matched secret with a placeholder that can be longer
   539|     # than the original text, so checking budget.allow() against the raw
   540|     # body and only redacting afterward let the actually-written content
   541|     # end up bigger than what was verified to fit.
   542|     body = redact_secrets("\n".join(body_lines))
   543|     if not budget.allow(body, len(body_lines)):
   544|         return "too_large"
   545|     out.append("```\n" + body + "\n```\n")
   546|     budget.spend(body, len(body_lines))
   547|     return "rendered"
   548| 
   549| 
   550| def _symbol_expansion(row: dict, calls_rows: list, imports_rows: list, files_by_path: dict,
   551|                        req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   552|                        communities_by_file: Optional[dict] = None) -> None:
   553|     rel, qn = row["relative_path"], row["qualified_name"]
   554| 
   555|     if req.include_callers:
   556|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   557|         if callers:
   558|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   559|             if budget.allow(header, 1):
   560|                 out.append(header); budget.spend(header, 1)
   561|                 max_files_note_emitted = False
   562|                 for c in callers:
   563|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   564|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   565|                             f"[origin: caller_expansion]")
   566|                     # Budget-check before reserving a focus-file slot -- a
   567|                     # caller entry that ultimately doesn't fit must not
   568|                     # consume the slot on behalf of content that was never
   569|                     # actually rendered.
   570|                     if not budget.allow(line, 1):
   571|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   572|                         break
   573|                     if not note_focus_file(c["caller_file"]):
   574|                         # A file beyond limits.max_files doesn't mean every
   575|                         # *later* caller is unreachable too -- a later one
   576|                         # may be in a file already in focus_files, which
   577|                         # note_focus_file accepts for free. Skip this entry
   578|                         # and keep checking the rest instead of abandoning
   579|                         # the whole listing.
   580|                         if not max_files_note_emitted:
   581|                             budget.omissions.append(
   582|                                 f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   583|                             )
   584|                             max_files_note_emitted = True
   585|                         continue
   586|                     out.append(line); budget.spend(line, 1)
   587|             else:
   588|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   589| 
   590|     if req.include_callees:
   591|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   592|         if callees:
   593|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   594|             if budget.allow(header, 1):
   595|                 out.append(header); budget.spend(header, 1)
   596|                 max_files_note_emitted = False
   597|                 for c in callees:
   598|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   599|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   600|                             f"[origin: callee_expansion]")
   601|                     if not budget.allow(line, 1):
   602|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   603|                         break
   604|                     if not note_focus_file(c["candidate_file"]):
   605|                         if not max_files_note_emitted:
   606|                             budget.omissions.append(
   607|                                 f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   608|                             )
   609|                             max_files_note_emitted = True
   610|                         continue
   611|                     out.append(line); budget.spend(line, 1)
   612|             else:
   613|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   614| 
   615|     if req.include_imports:
   616|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   617|         if file_imports:
   618|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   619|             if budget.allow(header, 1):
   620|                 out.append(header); budget.spend(header, 1)
   621|                 max_files_note_emitted = False
   622|                 for i in file_imports[:20]:
   623|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
   624|                     if not budget.allow(line, 1):
   625|                         break
   626|                     if not note_focus_file(i["resolved_file"]):
   627|                         if not max_files_note_emitted:
   628|                             budget.omissions.append(
   629|                                 f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   630|                             )
   631|                             max_files_note_emitted = True
   632|                         continue
   633|                     out.append(line); budget.spend(line, 1)
   634| 
   635|     if req.include_related_tests:
   636|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   637|         if tests:
   638|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   639|             if budget.allow(header, 1):
   640|                 out.append(header); budget.spend(header, 1)
   641|                 max_files_note_emitted = False
   642|                 for t in tests:
   643|                     line = f"- `{t}`"
   644|                     if not budget.allow(line, 1):
   645|                         break
   646|                     if not note_focus_file(t):
   647|                         # Route through the same global-focus-file gate as
   648|                         # every other tier -- a hard-coded high ceiling here
   649|                         # would let related-test expansion silently bypass
   650|                         # limits.max_files.
   651|                         if not max_files_note_emitted:
   652|                             budget.omissions.append(
   653|                                 f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   654|                             )
   655|                             max_files_note_emitted = True
   656|                         continue
   657|                     out.append(line); budget.spend(line, 1)
   658| 
   659|     if req.include_graphify and communities_by_file:
   660|         my_communities = communities_by_file.get(rel, [])
   661|         if my_communities:
   662|             comm_ids = {cid for cid, _ in my_communities}
   663|             peers = sorted(
   664|                 f for f, comms in communities_by_file.items()
   665|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   666|             )[:10]
   667|             if peers:
   668|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   669|                 if budget.allow(header, 1):
   670|                     out.append(header); budget.spend(header, 1)
   671|                     max_files_note_emitted = False
   672|                     for p in peers:
   673|                         line = f"- `{p}` [origin: graphify_expansion]"
   674|                         if not budget.allow(line, 1):
   675|                             budget.omissions.append(
   676|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   677|                             )
   678|                             break
   679|                         if not note_focus_file(p):
   680|                             # Route through the same global focus-file gate
   681|                             # as every other expansion tier -- otherwise a
   682|                             # Graphify peer could silently exceed
   683|                             # limits.max_files while the resolution
   684|                             # sidecar's focus_files list stayed under it.
   685|                             if not max_files_note_emitted:
   686|                                 budget.omissions.append(
   687|                                     f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
   688|                                     f"({req.max_files}) omitted."
   689|                                 )
   690|                                 max_files_note_emitted = True
   691|                             continue
   692|                         out.append(line); budget.spend(line, 1)
   693|                 else:
   694|                     budget.omissions.append(
   695|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   696|                     )
   697| 
   698| 
```
