# Chunk of dev_tools/repo_context/rc_request.py

- Source relative path: `dev_tools/repo_context/rc_request.py`
- Chunk: 2 of 3
- Original line range: 513-661
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _symbol_expansion
- Source SHA-256: 9afae857c0fde058a6c80995f21f1d847647d7b327a1e3e9e5318ec40212b388
- Starts inside symbol: no
- Ends inside symbol: no

```
   513| def _symbol_expansion(row: dict, calls_rows: list, imports_rows: list, files_by_path: dict,
   514|                        req: ResolvedRequest, budget: Budget, out: list, note_focus_file,
   515|                        communities_by_file: Optional[dict] = None) -> None:
   516|     rel, qn = row["relative_path"], row["qualified_name"]
   517| 
   518|     if req.include_callers:
   519|         callers = [c for c in _bfs_callers(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   520|         if callers:
   521|             header = f"\nCallers of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   522|             if budget.allow(header, 1):
   523|                 out.append(header); budget.spend(header, 1)
   524|                 max_files_note_emitted = False
   525|                 for c in callers:
   526|                     line = (f"- `{c['caller_symbol']}` in `{c['caller_file']}`:{c['line']} "
   527|                             f"— `{c['call_expression']}` ({c['confidence']}: {c['explanation']}) "
   528|                             f"[origin: caller_expansion]")
   529|                     # Budget-check before reserving a focus-file slot -- a
   530|                     # caller entry that ultimately doesn't fit must not
   531|                     # consume the slot on behalf of content that was never
   532|                     # actually rendered.
   533|                     if not budget.allow(line, 1):
   534|                         budget.omissions.append(f"More callers of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   535|                         break
   536|                     if not note_focus_file(c["caller_file"]):
   537|                         # A file beyond limits.max_files doesn't mean every
   538|                         # *later* caller is unreachable too -- a later one
   539|                         # may be in a file already in focus_files, which
   540|                         # note_focus_file accepts for free. Skip this entry
   541|                         # and keep checking the rest instead of abandoning
   542|                         # the whole listing.
   543|                         if not max_files_note_emitted:
   544|                             budget.omissions.append(
   545|                                 f"Caller(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   546|                             )
   547|                             max_files_note_emitted = True
   548|                         continue
   549|                     out.append(line); budget.spend(line, 1)
   550|             else:
   551|                 budget.omissions.append(f"Callers listing for `{qn}` omitted entirely (packet size limit reached).")
   552| 
   553|     if req.include_callees:
   554|         callees = [c for c in _bfs_callees(rel, qn, calls_rows, req.max_hops) if c["confidence"] != "unresolved"]
   555|         if callees:
   556|             header = f"\nCallees of `{qn}` (statically resolved, max_hops={req.max_hops}):\n"
   557|             if budget.allow(header, 1):
   558|                 out.append(header); budget.spend(header, 1)
   559|                 max_files_note_emitted = False
   560|                 for c in callees:
   561|                     line = (f"- `{c['call_expression']}` at line {c['line']} -> `{c['candidate_symbol']}` "
   562|                             f"in `{c['candidate_file']}` ({c['confidence']}: {c['explanation']}) "
   563|                             f"[origin: callee_expansion]")
   564|                     if not budget.allow(line, 1):
   565|                         budget.omissions.append(f"More callees of `{qn}` omitted (packet size limit reached); see python_calls.csv.")
   566|                         break
   567|                     if not note_focus_file(c["candidate_file"]):
   568|                         if not max_files_note_emitted:
   569|                             budget.omissions.append(
   570|                                 f"Callee(s) of `{qn}` beyond limits.max_files ({req.max_files}) omitted."
   571|                             )
   572|                             max_files_note_emitted = True
   573|                         continue
   574|                     out.append(line); budget.spend(line, 1)
   575|             else:
   576|                 budget.omissions.append(f"Callees listing for `{qn}` omitted entirely (packet size limit reached).")
   577| 
   578|     if req.include_imports:
   579|         file_imports = [i for i in imports_rows if i["source_file"] == rel and i["resolved_file"]]
   580|         if file_imports:
   581|             header = f"\nInternal imports of `{rel}` (import_expansion):\n"
   582|             if budget.allow(header, 1):
   583|                 out.append(header); budget.spend(header, 1)
   584|                 max_files_note_emitted = False
   585|                 for i in file_imports[:20]:
   586|                     line = f"- line {i['line']}: `{i['imported_name'] or i['imported_module']}` -> `{i['resolved_file']}`"
   587|                     if not budget.allow(line, 1):
   588|                         break
   589|                     if not note_focus_file(i["resolved_file"]):
   590|                         if not max_files_note_emitted:
   591|                             budget.omissions.append(
   592|                                 f"Import(s) of `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   593|                             )
   594|                             max_files_note_emitted = True
   595|                         continue
   596|                     out.append(line); budget.spend(line, 1)
   597| 
   598|     if req.include_related_tests:
   599|         tests = _candidate_tests_for_file(rel, imports_rows, calls_rows, files_by_path)
   600|         if tests:
   601|             header = f"\nRelated tests for `{rel}` (related_test_expansion):\n"
   602|             if budget.allow(header, 1):
   603|                 out.append(header); budget.spend(header, 1)
   604|                 max_files_note_emitted = False
   605|                 for t in tests:
   606|                     line = f"- `{t}`"
   607|                     if not budget.allow(line, 1):
   608|                         break
   609|                     if not note_focus_file(t):
   610|                         # Route through the same global-focus-file gate as
   611|                         # every other tier -- a hard-coded high ceiling here
   612|                         # would let related-test expansion silently bypass
   613|                         # limits.max_files.
   614|                         if not max_files_note_emitted:
   615|                             budget.omissions.append(
   616|                                 f"Related test(s) for `{rel}` beyond limits.max_files ({req.max_files}) omitted."
   617|                             )
   618|                             max_files_note_emitted = True
   619|                         continue
   620|                     out.append(line); budget.spend(line, 1)
   621| 
   622|     if req.include_graphify and communities_by_file:
   623|         my_communities = communities_by_file.get(rel, [])
   624|         if my_communities:
   625|             comm_ids = {cid for cid, _ in my_communities}
   626|             peers = sorted(
   627|                 f for f, comms in communities_by_file.items()
   628|                 if f != rel and any(cid in comm_ids for cid, _ in comms)
   629|             )[:10]
   630|             if peers:
   631|                 header = f"\nGraphify community peers of `{rel}` ({rc_graphify.format_communities(my_communities)}):\n"
   632|                 if budget.allow(header, 1):
   633|                     out.append(header); budget.spend(header, 1)
   634|                     max_files_note_emitted = False
   635|                     for p in peers:
   636|                         line = f"- `{p}` [origin: graphify_expansion]"
   637|                         if not budget.allow(line, 1):
   638|                             budget.omissions.append(
   639|                                 f"More Graphify community peers of `{rel}` omitted (packet size limit reached)."
   640|                             )
   641|                             break
   642|                         if not note_focus_file(p):
   643|                             # Route through the same global focus-file gate
   644|                             # as every other expansion tier -- otherwise a
   645|                             # Graphify peer could silently exceed
   646|                             # limits.max_files while the resolution
   647|                             # sidecar's focus_files list stayed under it.
   648|                             if not max_files_note_emitted:
   649|                                 budget.omissions.append(
   650|                                     f"Graphify community peer(s) of `{rel}` beyond limits.max_files "
   651|                                     f"({req.max_files}) omitted."
   652|                                 )
   653|                                 max_files_note_emitted = True
   654|                             continue
   655|                         out.append(line); budget.spend(line, 1)
   656|                 else:
   657|                     budget.omissions.append(
   658|                         f"Graphify community peers listing for `{rel}` omitted entirely (packet size limit reached)."
   659|                     )
   660| 
   661| 
```
